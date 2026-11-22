/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicresources

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"

	"github.com/go-logr/logr"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	resourceslicetracker "k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

var _ fwk.SharedDRAManager = &DefaultDRAManager{}

// Package dynamicresources 提供动态资源分配（DRA）调度插件，
// 管理 ResourceClaim、ResourceSlice 与 DeviceClass。

// DefaultDRAManager 是 SharedDRAManager 的默认实现。
// 它从 API informer 获取 DRA 对象，并借助 AssumeCache 与在途分配映射
// 避免修改 ResourceClaim 时出现竞态。
type DefaultDRAManager struct {
	resourceClaimTracker  *claimTracker
	resourceSliceLister   *resourceSliceLister
	deviceClassLister     *deviceClassLister
	extendedResourceCache *extendedresourcecache.ExtendedResourceCache
}

// NewDRAManager 创建默认的 DRA 管理器，按特性门控决定是否启用扩展资源缓存。
func NewDRAManager(ctx context.Context, claimsCache *assumecache.AssumeCache, resourceSliceTracker *resourceslicetracker.Tracker, informerFactory informers.SharedInformerFactory) *DefaultDRAManager {
	logger := klog.FromContext(ctx)
	manager := &DefaultDRAManager{
		resourceClaimTracker: &claimTracker{
			cache:               claimsCache,
			inFlightAllocations: make(map[types.UID]inFlightAllocation),
			allocatedDevices:    newAllocatedDevices(logger),
			logger:              logger,
		},
		resourceSliceLister: &resourceSliceLister{tracker: resourceSliceTracker},
		deviceClassLister:   &deviceClassLister{classLister: informerFactory.Resource().V1().DeviceClasses().Lister()},
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
		manager.extendedResourceCache = extendedresourcecache.NewExtendedResourceCache(logger)
	}

	// 通过事件增量维护已分配设备，比在 PreFilter 中反复遍历整个列表更高效。
	manager.resourceClaimTracker.cache.AddEventHandler(manager.resourceClaimTracker.allocatedDevices.handlers())

	return manager
}

// ResourceClaims 返回 ResourceClaim 跟踪器。
func (s *DefaultDRAManager) ResourceClaims() fwk.ResourceClaimTracker {
	return s.resourceClaimTracker
}

// ResourceSlices 返回 ResourceSlice 查询器。
func (s *DefaultDRAManager) ResourceSlices() fwk.ResourceSliceLister {
	return s.resourceSliceLister
}

// DeviceClasses 返回 DeviceClass 查询器。
func (s *DefaultDRAManager) DeviceClasses() fwk.DeviceClassLister {
	return s.deviceClassLister
}

// DeviceClassResolver 始终返回有效的接口实现：特性关闭时包装的底层缓存为 nil。
// 这是安全的，因为 ExtendedResourceCache.GetDeviceClass 对 nil 接收者会返回 nil。
func (s *DefaultDRAManager) DeviceClassResolver() fwk.DeviceClassResolver {
	return s.extendedResourceCache
}

var _ fwk.ResourceSliceLister = &resourceSliceLister{}

// resourceSliceLister 基于资源切片 tracker 实现 ResourceSliceLister。
type resourceSliceLister struct {
	tracker *resourceslicetracker.Tracker
}

// ListWithDeviceTaintRules 返回应用了设备污点规则补丁后的 ResourceSlice 列表。
func (l *resourceSliceLister) ListWithDeviceTaintRules() ([]*resourceapi.ResourceSlice, error) {
	return l.tracker.ListPatchedResourceSlices()
}

var _ fwk.DeviceClassLister = &deviceClassLister{}

// deviceClassLister 基于 informer lister 实现 DeviceClassLister。
type deviceClassLister struct {
	classLister resourcelisters.DeviceClassLister
}

// Get 按名称返回单个 DeviceClass。
func (l *deviceClassLister) Get(className string) (*resourceapi.DeviceClass, error) {
	return l.classLister.Get(className)
}

// List 返回所有 DeviceClass。
func (l *deviceClassLister) List() ([]*resourceapi.DeviceClass, error) {
	return l.classLister.List(labels.Everything())
}

var _ fwk.ResourceClaimTracker = &claimTracker{}

// claimTracker 跟踪 ResourceClaim 的分配状态与在途分配。
type claimTracker struct {
	// cache 临时存放调度器已分配、但 apiserver 更新尚未被 informer 处理的较新 Claim 对象。
	// Claim 在 PreBind 阶段加入，在 informer 回调中（依据 assume cache 的版本比较）移除。
	//
	// 键由 cache.MetaNamespaceKeyFunc 生成，格式为 "<namespace>/<name>"。
	//
	// 这样可保证 Pod 调度周期开始重建资源用量时，不会复用已分配给这类 Claim 的资源。
	// 若改为跨调度周期跟踪分配状态，则：
	//   - 增加复杂度（需与 Claim 和 ResourceSlice 的 informer 事件仔细同步）
	//   - 加大与 cluster autoscaler 集成的难度（需要触发 informer 回调）
	cache *assumecache.AssumeCache
	// inFlightMutex 保护对 inFlightAllocations 的访问。
	inFlightMutex sync.RWMutex
	// inFlightAllocations 记录调度周期内已触发分配、但 PreBind 尚未写回状态的 Claim，
	// 键为 Claim UID。引用计数表示 PodGroup 中有多少正在调度的 Pod 共享该待定分配；
	// 组外需要该 Claim 的 Pod 会被视为暂时不可调度，等待 Claim 状态更新事件再入队。
	//
	// 该机制避免如下问题：
	//   - Pod A 为 Claim X 触发分配
	//   - Pod B 共享该 Claim，因 assume 认为已分配而被调度
	//   - Pod B 的 PreBind 先执行，更新 reservedFor 时因 Claim 实际未分配而失败
	//
	// 若允许 A 或 B 任一方写分配虽可规避顺序问题，却引入新问题：
	//   - A、B 如上被调度
	//   - A 的 PreBind 先执行但遇到临时 API 错误，从在途集合移除更新后的 Claim
	//   - B 的 PreBind 随后成功写入分配及自己的 reservedFor
	//   - assume cache 无法反映 Claim 已分配，导致其他 Claim 复用同一资源
	//
	// 对 PodGroup 中的 Pod，待定分配可被组内多个 Pod 共享；PreBind 阶段由第一个成功的 Pod 写入。
	// 通过引用计数保证只要还有一个组内 Pod 未取消预留，分配就保持 in-flight，避免上述竞态。
	inFlightAllocations map[types.UID]inFlightAllocation
	allocatedDevices    *allocatedDevices
	logger              klog.Logger
}

// inFlightAllocation 表示一次在途分配及其共享者数量。
type inFlightAllocation struct {
	claim   *resourceapi.ResourceClaim
	sharers int
}

// GetPendingAllocation 返回指定 Claim 的在途分配结果，不存在时返回 nil。
func (c *claimTracker) GetPendingAllocation(claimUID types.UID) *resourceapi.AllocationResult {
	c.inFlightMutex.RLock()
	defer c.inFlightMutex.RUnlock()

	inFlight, found := c.inFlightAllocations[claimUID]
	if !found || inFlight.claim == nil {
		return nil
	}
	return inFlight.claim.Status.Allocation
}

// SignalClaimPendingAllocation 登记一次在途分配；已有记录时只增加共享计数。
func (c *claimTracker) SignalClaimPendingAllocation(claimUID types.UID, allocatedClaim *resourceapi.ResourceClaim) error {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	inFlight, found := c.inFlightAllocations[claimUID]
	if found {
		inFlight.sharers++
		c.inFlightAllocations[claimUID] = inFlight

		claim := inFlight.claim
		c.logger.V(5).Info("Added share for in-flight claim", "claim", klog.KObj(claim), "uid", claimUID, "version", claim.ResourceVersion, "sharers", inFlight.sharers)
		return nil
	}

	// 检查 Claim 确实未分配：Pod 的 CycleState 可能已过期，期间该 Claim 可能已被分配。
	// 扩展资源 Claim 不可共享，已知其未分配，无需查 assume cache。
	if !isSpecialClaimName(allocatedClaim.Name) {
		assumedClaim, err := c.Get(allocatedClaim.Namespace, allocatedClaim.Name)
		if err != nil {
			return fmt.Errorf("look up assumed claim %s/%s, UID=%s: %w", allocatedClaim.Namespace, allocatedClaim.Name, claimUID, err)
		}
		if assumedClaim.UID == claimUID && assumedClaim.Status.Allocation != nil {
			c.logger.V(6).Info("Claim is already allocated, not creating in-flight", "claim", klog.KObj(assumedClaim), "uid", claimUID, "version", assumedClaim.ResourceVersion)
			return nil
		}
	}

	c.inFlightAllocations[claimUID] = inFlightAllocation{
		claim:   allocatedClaim,
		sharers: 1,
	}
	// 日志级别与 assume cache 中对应的记录保持一致。
	c.logger.V(5).Info("Added in-flight claim", "claim", klog.KObj(allocatedClaim), "uid", claimUID, "version", allocatedClaim.ResourceVersion)
	// 本实现没有返回错误的场景，但保留错误返回值便于其他实现扩展，
	// 例如处理 fake Claim 时可为无效分配返回错误。
	return nil
}

// MaybeRemoveClaimPendingAllocation 减少在途分配的共享计数，计数归零或强制删除时移除记录。
func (c *claimTracker) MaybeRemoveClaimPendingAllocation(claimUID types.UID, forceRemove bool) (deleted bool) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	inFlight, found := c.inFlightAllocations[claimUID]
	// assume cache 对此未记录日志，这里记一条便于排查重复删除。
	if !found {
		c.logger.V(5).Info("Redundant remove of in-flight claim, not found", "uid", claimUID)
		return false
	}
	claim := inFlight.claim

	if forceRemove || inFlight.sharers == 1 {
		delete(c.inFlightAllocations, claimUID)
		c.logger.V(5).Info("Removed in-flight claim", "claim", klog.KObj(claim), "uid", claimUID, "version", claim.ResourceVersion)
		return true
	}

	inFlight.sharers--
	c.inFlightAllocations[claimUID] = inFlight
	c.logger.V(5).Info("Claim is still shared by other pods, not removing in-flight claim", "claim", klog.KObj(claim), "uid", claimUID, "version", claim.ResourceVersion, "sharers", inFlight.sharers)
	return false
}

// Get 从 assume cache 按命名空间与名称获取 Claim。
func (c *claimTracker) Get(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
	obj, err := c.cache.Get(namespace + "/" + claimName)
	if err != nil {
		return nil, err
	}
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T for assumed object %s/%s", obj, namespace, claimName)
	}
	return claim, nil
}

// List 返回 assume cache 中的所有 Claim。
func (c *claimTracker) List() ([]*resourceapi.ResourceClaim, error) {
	var result []*resourceapi.ResourceClaim
	// 为 Claim 单独建索引收益不大，直接全量遍历。
	objs := c.cache.List(nil)
	for _, obj := range objs {
		claim, ok := obj.(*resourceapi.ResourceClaim)
		if ok {
			result = append(result, claim)
		}
	}
	return result, nil
}

// errClaimTrackerConcurrentModification 表示采集过程中状态被并发修改，调用方需重试。
//
// 罕见竞态：Claim 初次处于 in-flight 时，
//   - 从 cache 构建 allocated 集合（此时 Claim 不在 cache）
//   - 另一 goroutine 将其从 in-flight 移入 cache
//   - 随后检查 in-flight（Claim 已不在）
//     => 该 Claim 被漏掉
//
// 彻底修复需把 assume cache、allocatedDevices 与 in-flight map 置于同一把锁下，
// 改动较大且无法复用 assume cache。因此改为检查已分配设备集合是否变化，
// 直到某次采集期间无并发修改。
// Claim 不可能先出现在 cache 后又只存在于 in-flight，故无需反向复查。
var errClaimTrackerConcurrentModification = errors.New("conflicting concurrent modification")

// ListAllAllocatedDevices 返回集群中所有独占分配的设备 ID 集合。
func (c *claimTracker) ListAllAllocatedDevices() (a sets.Set[structured.DeviceID], err error) {
	c.logger.V(6).Info("Starting ListAllAllocatedDevices")
	defer func() {
		c.logger.V(6).Info("Finished ListAllAllocatedDevices", "allocatedDevices", logAllocatedDevices(c.logger, a), "err", err)
	}()

	// 先以 informer 视角的当前状态为基础构建集合。
	allocated, revision := c.allocatedDevices.Get()

	// 在途分配尚未写入缓存，也必须计入。
	for _, inFlight := range c.allInFlightAllocationsRLocked() {
		claim := inFlight.claim
		foreachAllocatedDevice(claim, func(deviceID structured.DeviceID) {
			c.logger.V(6).Info("Device is in flight for allocation", "device", deviceID, "claim", klog.KObj(claim))
			allocated.Insert(deviceID)
		}, false, func(structured.SharedDeviceID) {}, func(structured.DeviceConsumedCapacity) {})
	}

	if revision == c.allocatedDevices.Revision() {
		// 版本号未变，说明采集期间状态未被修改，结果有效。
		return allocated, nil
	}

	return nil, errClaimTrackerConcurrentModification
}

// GatherAllocatedState 采集集群中所有设备当前的分配状态，包括：
//   - AllocatedDevices：独占模式下已完整分配的设备 ID
//   - AllocatedSharedDeviceIDs：可消费容量模式下的共享设备 ID
//   - AggregatedCapacity：可消费容量模式下各设备已消费的容量
//
// 支持两种分配模型：
//  1. 传统独占模式（DRAConsumableCapacity 关闭）：设备只分配给单个 Claim，共享设备折算为基础设备 ID
//  2. 可消费容量模式（开启）：设备可按容量在多个 Claim 间共享
//
// 一致性保障：从 informer 缓存读取、纳入尚未持久化的在途分配，
// 并通过版本号检测并发修改。状态变化时返回 errClaimTrackerConcurrentModification 让调用方重试。
func (c *claimTracker) GatherAllocatedState() (s *structured.AllocatedState, err error) {
	c.logger.V(6).Info("Starting GatherAllocatedState")
	defer func() {
		var a sets.Set[structured.DeviceID]
		if s != nil {
			a = s.AllocatedDevices
		}
		c.logger.V(6).Info("Finished GatherAllocatedState", "allocatedDevices", logAllocatedDevices(c.logger, a), "err", err)
	}()

	// 先以 informer 视角的当前状态为基础构建集合。
	enabledConsumableCapacity := utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity)

	allocated, revision1 := c.allocatedDevices.Get()
	allocatedSharedDeviceIDs, revision2 := c.allocatedDevices.GetSharedDeviceIDs()
	aggregatedCapacity, revision3 := c.allocatedDevices.Capacities()

	if revision1 != revision2 || revision2 != revision3 {
		// 三个版本号不一致，读到的是中间状态，直接重试。
		return nil, errClaimTrackerConcurrentModification
	}

	if !enabledConsumableCapacity {
		// 特性关闭时回退到传统的独占分配模型，
		// 保持与原始 DRA 行为兼容：设备只能分配给单个 Claim。
		for sharedDeviceID := range allocatedSharedDeviceIDs {
			allocated.Insert(sharedDeviceID.GetDeviceID())
		}
		// 独占模型下不再需要共享设备与容量聚合数据。
		allocatedSharedDeviceIDs = sets.New[structured.SharedDeviceID]()
		aggregatedCapacity = make(schedulerapi.ConsumedCapacityCollection)
	}

	// 在途分配尚未写入缓存，也必须计入。
	for _, inFlight := range c.allInFlightAllocationsRLocked() {
		claim := inFlight.claim
		foreachAllocatedDevice(claim,
			func(deviceID structured.DeviceID) { // dedicatedDeviceCallback
				c.logger.V(6).Info("Device is in flight for allocation", "device", deviceID, "claim", klog.KObj(claim))
				allocated.Insert(deviceID)
			},
			enabledConsumableCapacity,
			func(sharedDeviceID structured.SharedDeviceID) { // sharedDeviceCallback
				c.logger.V(6).Info("Device is in flight for allocation", "shared device", sharedDeviceID, "claim", klog.KObj(claim))
				allocatedSharedDeviceIDs.Insert(sharedDeviceID)
			},
			func(capacity structured.DeviceConsumedCapacity) { // consumedCapacityCallback
				c.logger.V(6).Info("Device is in flight for allocation", "consumed capacity", capacity, "claim", klog.KObj(claim))
				aggregatedCapacity.Insert(capacity)
			})
	}
	if revision1 == c.allocatedDevices.Revision() {
		// 版本号未变，说明采集期间状态未被修改，结果有效。
		return &structured.AllocatedState{
			AllocatedDevices:         allocated,
			AllocatedSharedDeviceIDs: allocatedSharedDeviceIDs,
			AggregatedCapacity:       aggregatedCapacity,
		}, nil
	}

	return nil, errClaimTrackerConcurrentModification
}

// allInFlightAllocationsRLocked 返回遍历所有在途分配的迭代器，遍历期间持有读锁。
func (c *claimTracker) allInFlightAllocationsRLocked() iter.Seq2[types.UID, inFlightAllocation] {
	return func(yield func(types.UID, inFlightAllocation) bool) {
		c.inFlightMutex.RLock()
		defer c.inFlightMutex.RUnlock()
		for uid, inFlight := range c.inFlightAllocations {
			if !yield(uid, inFlight) {
				return
			}
		}
	}
}

// AssumeClaimAfterAPICall 在 API 调用成功后把 Claim 写入 assume cache。
func (c *claimTracker) AssumeClaimAfterAPICall(claim *resourceapi.ResourceClaim) error {
	return c.cache.Assume(claim)
}

// AssumedClaimRestore 撤销 assume cache 中的假设对象，回退到 informer 中的真实状态。
func (c *claimTracker) AssumedClaimRestore(namespace, claimName string) {
	c.cache.Restore(namespace + "/" + claimName)
}

// V(6) 级别只记录有限数量的设备，避免日志膨胀。
// 多数 E2E 测试（至少串行运行时）10 个设备足以覆盖全部，无需截断。
// maxDevicesLevel6 是 V(6) 日志中最多输出的设备数。
const maxDevicesLevel6 = 10

// logAllocatedDevices 返回一个结构化日志值，按日志详细程度输出不同量的已分配设备信息。
func logAllocatedDevices(logger klog.Logger, allocatedDevices sets.Set[structured.DeviceID]) any {
	// 必须在此处检查详细级别：调用方源码位置可能影响 -vmodule 匹配。
	helper, logger := logger.WithCallStackHelper()
	helper()

	// V <= 5 输出保持一致，从 V(6) 起才输出设备 ID。
	verbosity := 5
	for i := 7; i > verbosity; i-- {
		if loggerV := logger.V(i); loggerV.Enabled() {
			verbosity = i
			break
		}
	}

	return &allocatedDevicesLogger{verbosity, allocatedDevices}
}

// allocatedDevicesLogger 延迟生成已分配设备的日志内容。
type allocatedDevicesLogger struct {
	verbosity int
	devices   sets.Set[structured.DeviceID]
}

var _ logr.Marshaler = &allocatedDevicesLogger{}

// MarshalLog 按详细级别生成日志内容；低于 V(6) 时不输出设备信息。
func (a *allocatedDevicesLogger) MarshalLog() any {
	if a.verbosity < 6 {
		return nil
	}

	info := struct {
		Count   int      `json:"count"`
		Devices []string `json:"devices"`
	}{
		Count: len(a.devices),
	}
	ids := make([]string, 0, len(a.devices))
	for id := range a.devices {
		ids = append(ids, id.String())
	}
	slices.Sort(ids)
	if a.verbosity == 6 && len(ids) > maxDevicesLevel6 {
		truncated := make([]string, 0, maxDevicesLevel6+1)
		truncated = append(truncated, ids[:maxDevicesLevel6/2]...)
		truncated = append(truncated, "...")
		truncated = append(truncated, ids[len(ids)-maxDevicesLevel6/2:]...)
		ids = truncated
	}
	info.Devices = ids

	return info
}
