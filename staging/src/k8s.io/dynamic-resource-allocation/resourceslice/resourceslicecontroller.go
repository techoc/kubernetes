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

package resourceslice

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	cgocore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	draclient "k8s.io/dynamic-resource-allocation/client"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	// poolNameIndex 是 ResourceSlice 存储索引函数的名称，索引键为 ResourceSlice.Spec.Pool.Name。
	poolNameIndex = "poolName"

	// DefaultMutationCacheTTL 是变更缓存中新增条目的存活时长；因缓存中的对象可能已被他人删除而 informer 无法感知，故需设置 TTL 并在过期后重新检查 pool。
	DefaultMutationCacheTTL = time.Minute

	// DefaultSyncDelay 定义收到最新 informer 事件到再次同步之间的等待时长。
	// 既要等 informer 缓存追平以避免冗余删除，也要让运维能在等待期间看见并修正错误。
	DefaultSyncDelay = 30 * time.Second

	// resourceSliceIndexMinLength 是 ResourceSlice 名称中编码索引的最小长度；当最高索引需要更多字符时，所有切片统一使用更长前缀以保持字典序排序有效。
	resourceSliceIndexMinLength = 5

	// nameSeparator 用于分隔 ResourceSlice 名称的各个部分。
	nameSeparator = "-"
)

// Controller 将单个驱动的资源信息同步为 ResourceSlice 对象，支持节点本地资源与网络附加资源。
type Controller struct {
	// 控制器的基本字段：cancel 用于取消后台活动并携带原因，driverName 为 DRA 驱动名称，owner 为属主资源（可为 nil），resourceClient 与 coreClient 分别用于访问 ResourceSlice 与核心资源，wg 等待后台 goroutine 退出。
	cancel         func(cause error)
	driverName     string
	owner          *Owner
	resourceClient *draclient.Client
	coreClient     cgocore.CoreV1Interface
	wg             sync.WaitGroup
	// queue 是工作队列，以 pool 名称为键；sliceStore 为带变更跟踪的本地缓存，mutationCacheTTL 为其条目 TTL，syncDelay 为事件到同步的延迟，errorHandler 在发布出错时调用（默认仅记录日志）。
	queue            workqueue.TypedRateLimitingInterface[string]
	sliceStore       cache.MutationCache
	mutationCacheTTL time.Duration
	syncDelay        time.Duration
	errorHandler     func(ctx context.Context, err error, msg string)

	// lastAddByPool 记录每个 pool 最近一次创建 ResourceSlice 的时间，键为 pool 名称，使每个 syncPool 的时间戳独立。
	// 新增对象可能已被外部删除而 informer 无法感知，需在变更缓存 TTL 后复核。
	// 仅延迟同步不够：后续事件可能覆盖该延迟任务，导致过早同步且不再重试，因此记录时间并在每次同步时重新安排。
	lastAddByPool map[string]time.Time

	// 计数器，必须原子访问：numCreates 统计创建数，numUpdates 统计更新数，numDeletes 统计删除数。
	numCreates int64
	numUpdates int64
	numDeletes int64

	// mutex 保护 resources 等共享字段的并发访问。
	mutex sync.RWMutex

	// resources 是期望的资源状态；更新时整体替换指针，因此读取时只需读锁保护指针本身。
	resources *DriverResources

	// reconcilePoolWithName 是仅协调的 pool 名称，为空表示默认行为。
	reconcilePoolWithName string

	// usePoolNameFieldSelector 标记是否可在字段选择器中使用 spec.pool.name；List 与 Watch 可能并发访问，需原子读写。
	usePoolNameFieldSelector atomic.Bool
}

// +k8s:deepcopy-gen=true

// DriverResources 是控制器需要同步的全部资源的完整描述。
type DriverResources struct {
	// Pools 以 pool 名称为键，描述每个资源池。
	// 含 binding conditions 设备的 pool 最后尝试，其余按名称排序；名称可表达驱动偏好。
	Pools map[string]Pool
}

// +k8s:deepcopy-gen=true

// Pool 是属于同一资源池的设备集合。
type Pool struct {
	// NodeSelector 指定该 pool 的节点选择器，不可与其他节点选择字段同时设置。
	NodeSelector *v1.NodeSelector

	// AllNodes 表示是否将 slice 发布到所有节点，不可与其他节点选择字段同时设置。
	AllNodes bool

	// Generation 可由驱动留零，控制器会自动递增。
	Generation int64

	// Slices 是驱动希望为该 pool 发布的全部 ResourceSlice；驱动必须保证转换结果合法，包括设备数量上限。
	// 列表顺序决定分配优先级：索引前缀用于字典序 first-fit，优先的 slice 与设备应排在前面。
	// 从随机命名升级会删除并重建已有切片。命名为 [base16 索引]-[驱动名]-[owner 名（非 nil 时)]-。
	// 非法 slice 的 API Server 错误会记录到日志。正常管理的每个 pool 至少发布一个 slice，即使为空：
	// 空 pool 表示驱动存活但没有设备，完全没有 pool 则含义不同。
	Slices []Slice
}

// +k8s:deepcopy-gen=true

// Slice 会被控制器转换为一个 ResourceSlice。
type Slice struct {
	// Devices 列出该 slice 的全部设备，SharedCounters 列出可供设备引用的共享计数器集合，PerDeviceNodeSelection 表示是否按设备粒度选择节点。
	Devices                []resourceapi.Device
	SharedCounters         []resourceapi.CounterSet
	PerDeviceNodeSelection *bool
	// PartitionTypeAttribute 指定标识设备分区类型的属性名；DRAPartitionableDevicesType 关闭时 API Server 会丢弃该字段。
	// SkipNodeOperations 列出需要跳过的节点操作。
	PartitionTypeAttribute *resourceapi.FullyQualifiedName
	SkipNodeOperations     []resourceapi.SkipNodeOperation
}

// +k8s:deepcopy-gen=true

// Owner 描述 ResourceSlice 的属主资源，必须是集群级资源；Node 类型属主的 UID 可留空，由控制器自动查找。
// 没有属主也能发布，但卸载时无法依赖垃圾回收，必须手动删除剩余切片。
type Owner struct {
	// Owner 的字段：APIVersion 为属主资源的 API 版本，Kind 为类型，Name 为名称，UID 为 UID。
	APIVersion string
	Kind       string
	Name       string
	UID        types.UID
}

// StartController 构造并启动控制器。
func StartController(ctx context.Context, options Options) (*Controller, error) {
	logger := klog.FromContext(ctx)
	// 创建控制器实例，失败则包装错误返回。
	c, err := newController(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("create controller: %w", err)
	}

	logger.V(3).Info("Starting")
	// 登记后台 goroutine，便于 Stop 等待。
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer logger.V(3).Info("Stopping")
		// 在后台运行同步循环。
		c.run(ctx)
	}()
	return c, nil
}

// Options 是 StartController 的可选配置。
type Options struct {
	// DriverName 是必需的驱动名称，须为仅含小写字母的 DNS 子域，并应以厂商拥有的 DNS 域名结尾。
	DriverName string

	// KubeClient 用于读取 Node 与访问 ResourceSlice，必须提供。
	KubeClient kubernetes.Interface

	// Owner 指定可选的集群级属主；Node 属主时用 Name 标识管理对象，UID 可省略并由控制器自动查询。
	Owner *Owner

	// Resources 是初始期望的 slice 集合；nil 与空 DriverResources 等价。
	Resources *DriverResources

	// Queue 可覆盖默认的工作队列实现。
	Queue workqueue.TypedRateLimitingInterface[string]

	// MutationCacheTTL 可覆盖变更缓存的默认 TTL（一分钟）。
	MutationCacheTTL *time.Duration

	// SyncDelay 可覆盖事件到同步的默认延迟（30 秒）。
	SyncDelay *time.Duration

	// ErrorHandler 在发布出错时调用；处理器返回后控制器继续重试。可记录日志、替换非法切片或退出进程。
	// 字段丢弃错误会包装为 DroppedFieldsError，可用 errors.As 检查：
	//	var droppedFields *resourceslice.DroppedFieldsError
	//	if errors.As(err, &droppedFields) { ... do something with droppedFields ... }
	// 默认实现是 utilruntime.HandleErrorWithContext，仅记录日志。
	ErrorHandler func(ctx context.Context, err error, msg string)

	// ReconcilePoolWithName 只把 Spec.Pool.Name 匹配的切片入队；即使 owner 是 Node 也不设置 Spec.NodeName。
	// 切片通过 NodeSelector/AllNodes 对全集群可见，每个节点都会收到该驱动的全部切片，带来集群级性能开销。
	// 空字符串表示默认行为。
	ReconcilePoolWithName string
}

// DroppedFieldsError 表示 apiserver 丢弃了驱动期望发布的某些字段。
type DroppedFieldsError struct {
	// PoolName 为发生字段丢弃的 pool 名称，SliceIndex 为对应 slice 的索引，DesiredSlice 与 ActualSlice 分别为期望与实际对象，用于推断被丢弃的字段。
	PoolName                  string
	SliceIndex                int
	DesiredSlice, ActualSlice *resourceapi.ResourceSlice
}

// Error 实现 error 接口，给出字段被丢弃的错误信息及可能相关的已禁用特性。
func (err *DroppedFieldsError) Error() string {
	// 先推测哪些特性被禁用，用于错误信息。
	disabled := err.DisabledFeatures()
	// 若推测不出，则标记为 unknown。
	if len(disabled) == 0 {
		disabled = []string{"unknown"}
	}
	// 汇总 pool、slice 索引与疑似禁用的特性。
	return fmt.Sprintf("pool %q, slice #%d: some fields were dropped by the apiserver, probably because these features are disabled: %s", err.PoolName, err.SliceIndex, strings.Join(disabled, " "))
}

// DisabledFeatures 通过对比期望与实际对象，推断哪些特性被禁用导致字段丢失。
func (err *DroppedFieldsError) DisabledFeatures() []string {
	var disabled []string

	// 污点数量变少：DRADeviceTaints 可能被禁用。
	for i := 0; i < len(err.DesiredSlice.Spec.Devices) && i < len(err.ActualSlice.Spec.Devices); i++ {
		if len(err.DesiredSlice.Spec.Devices[i].Taints) > len(err.ActualSlice.Spec.Devices[i].Taints) {
			disabled = append(disabled, "DRADeviceTaints")
			break
		}
	}

	// 可分区设备字段：PerDeviceNodeSelection 或 SharedCounters 发生变化。
	if ptr.Deref(err.DesiredSlice.Spec.PerDeviceNodeSelection, false) && !ptr.Deref(err.ActualSlice.Spec.PerDeviceNodeSelection, false) ||
		len(err.DesiredSlice.Spec.SharedCounters) > len(err.ActualSlice.Spec.SharedCounters) {
		disabled = append(disabled, "DRAPartitionableDevices")
	} else {
		// 否则检查设备内的 ConsumesCounters 数量变化。
		for i := 0; i < len(err.DesiredSlice.Spec.Devices) && i < len(err.ActualSlice.Spec.Devices); i++ {
			if len(err.DesiredSlice.Spec.Devices[i].ConsumesCounters) != len(err.ActualSlice.Spec.Devices[i].ConsumesCounters) {
				disabled = append(disabled, "DRAPartitionableDevices")
				break
			}
		}
	}

	// BindingConditions 数量不一致：DRADeviceBindingConditions 可能被禁用。
	for i := 0; i < len(err.DesiredSlice.Spec.Devices) && i < len(err.ActualSlice.Spec.Devices); i++ {
		if len(err.DesiredSlice.Spec.Devices[i].BindingConditions) != len(err.ActualSlice.Spec.Devices[i].BindingConditions) ||
			len(err.DesiredSlice.Spec.Devices[i].BindingFailureConditions) != len(err.ActualSlice.Spec.Devices[i].BindingFailureConditions) {
			disabled = append(disabled, "DRADeviceBindingConditions")
			break
		}
	}

	// AllowMultipleAllocations 丢失：DRAConsumableCapacity 可能被禁用。
	for i := 0; i < len(err.DesiredSlice.Spec.Devices) && i < len(err.ActualSlice.Spec.Devices); i++ {
		if err.DesiredSlice.Spec.Devices[i].AllowMultipleAllocations != nil && err.ActualSlice.Spec.Devices[i].AllowMultipleAllocations == nil {
			disabled = append(disabled, "DRAConsumableCapacity")
			break
		}
	}

	// PartitionTypeAttribute 丢失：DRAPartitionableDevicesType 可能被禁用。
	if err.DesiredSlice.Spec.PartitionTypeAttribute != nil && err.ActualSlice.Spec.PartitionTypeAttribute == nil {
		disabled = append(disabled, "DRAPartitionableDevicesType")
	}

	// SkipNodeOperations 丢失：DRAOptionalNodeOperations 可能被禁用。
	if len(err.DesiredSlice.Spec.SkipNodeOperations) > 0 && len(err.ActualSlice.Spec.SkipNodeOperations) == 0 {
		disabled = append(disabled, "DRAOptionalNodeOperations")
	}

	// 计数器消费中的 CompatibilityGroups 变少：DRADeviceCompatibilityGroups 可能被禁用。
compatibilityGroups:
	for i := 0; i < len(err.DesiredSlice.Spec.Devices) && i < len(err.ActualSlice.Spec.Devices); i++ {
		desired := err.DesiredSlice.Spec.Devices[i].ConsumesCounters
		actual := err.ActualSlice.Spec.Devices[i].ConsumesCounters
		for j := 0; j < len(desired) && j < len(actual); j++ {
			if len(desired[j].CompatibilityGroups) > len(actual[j].CompatibilityGroups) {
				disabled = append(disabled, "DRADeviceCompatibilityGroups")
				break compatibilityGroups
			}
		}
	}

	// 返回所有推测出的已禁用特性。
	return disabled
}

// 编译期断言：DroppedFieldsError 实现 error 接口。
var _ error = &DroppedFieldsError{}

// Stop 取消所有后台活动，并阻塞直到控制器完全停止。
func (c *Controller) Stop() {
	// 允许对 nil 控制器调用 Stop。
	if c == nil {
		return
	}
	// 触发取消，通知所有后台 goroutine。
	c.cancel(errors.New("ResourceSlice controller was asked to stop"))
	// 等待后台 goroutine 退出。
	c.wg.Wait()
}

// Update 设置期望的资源状态；nil 有效且等价于空 DriverResources。控制器会做深拷贝，调用方可在返回后复用对象。
// 单 pool 协调模式只允许空 pool 列表，或恰好包含指定名称的一个 pool。
func (c *Controller) Update(resources *DriverResources) {
	// 加写锁保护 resources 与队列的更新。
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 先把所有旧 pool 入队，以便同步时删除不再需要的 slice。
	if c.resources != nil {
		for poolName := range c.resources.Pools {
			c.queue.Add(poolName)
		}
	}

	// nil 视为空资源集合。
	if resources == nil {
		c.resources = &DriverResources{}
	} else {
		// 单 pool 协调模式下校验 pool 数量是否合法。
		if c.reconcilePoolWithName != "" {
			_, ok := resources.Pools[c.reconcilePoolWithName]
			if (ok && len(resources.Pools) > 1) || !ok && len(resources.Pools) > 0 {
				c.errorHandler(context.Background(),
					fmt.Errorf("ReconcilePoolWithName=%q, but found %d pools; expected exactly one pool with this name", c.reconcilePoolWithName, len(resources.Pools)),
					"processing update DriverResources")
				return
			}
		}

		// 深拷贝后保存，并对污点时间做秒级取整。
		c.resources = resources.DeepCopy()
		roundTaintTimeAdded(c.resources)
	}

	// 再把所有新 pool 入队（可能与旧 pool 相同）。
	for poolName := range c.resources.Pools {
		c.queue.Add(poolName)
	}
}

// roundTaintTimeAdded 将所有污点时间取整到秒，因为 API 只能存储秒级精度，避免期望与实际对象出现语义差异。
func roundTaintTimeAdded(resources *DriverResources) {
	for _, pool := range resources.Pools {
		for _, slice := range pool.Slices {
			for _, device := range slice.Devices {
				for _, taint := range device.Taints {
					if taint.TimeAdded != nil {
						// 按秒取整，消除往返产生的精度差异。
						taint.TimeAdded.Time = taint.TimeAdded.Time.Round(time.Second)
					}
				}
			}
		}
	}
}

// GetStats 返回控制器各类操作的计数快照。
func (c *Controller) GetStats() Stats {
	s := Stats{
		// 原子读取各计数器，保证并发安全。
		NumCreates: atomic.LoadInt64(&c.numCreates),
		NumUpdates: atomic.LoadInt64(&c.numUpdates),
		NumDeletes: atomic.LoadInt64(&c.numDeletes),
	}
	return s
}

// Stats 汇总控制器创建、更新、删除 ResourceSlice 的次数。
type Stats struct {
	// NumCreates 统计创建的 ResourceSlice 数量。
	NumCreates int64
	// NumUpdates 统计更新的 ResourceSlice 数量。
	NumUpdates int64
	// NumDeletes 统计删除的 ResourceSlice 数量。
	NumDeletes int64
}

// newController 创建控制器（不启动）。
func newController(ctx context.Context, options Options) (*Controller, error) {
	// 校验必需参数：KubeClient 与驱动名。
	if options.KubeClient == nil {
		return nil, errors.New("KubeClient is nil")
	}
	if options.DriverName == "" {
		return nil, errors.New("DRA driver name is empty")
	}

	// 创建可携带取消原因的上下文。
	ctx, cancel := context.WithCancelCause(ctx)

	// 初始化控制器各字段，未显式指定的配置使用默认值。
	c := &Controller{
		cancel:                cancel,
		resourceClient:        draclient.New(options.KubeClient),
		coreClient:            options.KubeClient.CoreV1(),
		driverName:            options.DriverName,
		owner:                 options.Owner.DeepCopy(),
		queue:                 options.Queue,
		mutationCacheTTL:      ptr.Deref(options.MutationCacheTTL, DefaultMutationCacheTTL),
		syncDelay:             ptr.Deref(options.SyncDelay, DefaultSyncDelay),
		errorHandler:          options.ErrorHandler,
		lastAddByPool:         make(map[string]time.Time),
		reconcilePoolWithName: options.ReconcilePoolWithName,
	}
	// 未提供队列时使用默认的限速队列。
	if c.queue == nil {
		c.queue = workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "node_resource_slices"},
		)
	}
	// 未提供错误处理器时默认仅记录日志。
	if c.errorHandler == nil {
		c.errorHandler = func(ctx context.Context, err error, msg string) {
			utilruntime.HandleErrorWithContext(ctx, err, msg)
		}
	}
	// 仅单 pool 协调模式默认启用 poolName 字段选择器。
	c.usePoolNameFieldSelector.Store(c.reconcilePoolWithName != "")
	// 初始化 informer 并等待缓存同步。
	if err := c.initInformer(ctx); err != nil {
		return nil, err
	}

	// 设置初始期望状态并将相关 pool 入队。
	c.Update(options.Resources)

	return c, nil
}

// initInformer 初始化用于监听 ResourceSlice 变化的 informer，并等待其缓存完成同步。
func (c *Controller) initInformer(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	// 构造字段选择器：始终按驱动名过滤，节点本地资源额外按节点名过滤。
	fieldSelector := func(includePoolName bool) string {
		// 按驱动名、节点名（可选）与 pool 名（可选）构建选择器。
		selector := fields.Set{
			resourceapi.ResourceSliceSelectorDriver:   c.driverName,
			resourceapi.ResourceSliceSelectorNodeName: "",
		}
		// 仅节点属主且非单 pool 协调模式时，按节点名过滤。
		if c.owner != nil && c.owner.APIVersion == "v1" && c.owner.Kind == "Node" && c.reconcilePoolWithName == "" {
			selector[resourceapi.ResourceSliceSelectorNodeName] = c.owner.Name
		}
		// 单 pool 协调模式下额外按 pool 名过滤。
		if includePoolName {
			selector[resourceapi.ResourceSliceSelectorPoolName] = c.reconcilePoolWithName
		}

		return selector.String()
	}
	// 将字段选择器写入 ListOptions。
	tweakListOptions := func(options *metav1.ListOptions, includePoolName bool) {
		options.FieldSelector = fieldSelector(includePoolName)
	}
	// 建立按 pool 名索引的索引器。
	indexers := cache.Indexers{
		poolNameIndex: func(obj interface{}) ([]string, error) {
			slice, ok := obj.(*resourceapi.ResourceSlice)
			if !ok {
				return []string{}, nil
			}
			return []string{slice.Spec.Pool.Name}, nil
		},
	}
	// 创建共享 informer 并禁用周期性 resync：确定性同步不应因 resync 改变结果，否则只会把每个切片都报告为已更新。
	informer := cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			// List 回调：若服务端不支持 poolName 字段选择器则回退到客户端过滤。
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				// 读取是否启用 poolName 字段选择器。
				usePoolNameFieldSelector := c.usePoolNameFieldSelector.Load()
				tweakListOptions(&options, usePoolNameFieldSelector)
				slices, err := c.resourceClient.ResourceSlices().List(ctx, options)
				// 服务端拒绝该选择器时，关闭开关并重试。
				if err != nil && usePoolNameFieldSelector && isUnsupportedPoolNameFieldSelector(err) {
					c.usePoolNameFieldSelector.Store(false)
					usePoolNameFieldSelector = false
					tweakListOptions(&options, usePoolNameFieldSelector)
					slices, err = c.resourceClient.ResourceSlices().List(ctx, options)
				}
				// 记录 List 成功或失败的日志。
				if err == nil {
					logger.V(5).Info("Listed ResourceSlices", "resourceAPI", c.resourceClient.CurrentAPI(), "numSlices", len(slices.Items), "listMeta", slices.ListMeta)
				} else {
					logger.V(5).Info("Listed ResourceSlices", "resourceAPI", c.resourceClient.CurrentAPI(), "err", err)
				}

				// 回退到客户端过滤时，仅保留目标 pool 的 slice。
				if err == nil && c.reconcilePoolWithName != "" && !usePoolNameFieldSelector {
					retainSlicesByPoolName(slices, c.reconcilePoolWithName)
				}
				return slices, err
			},
			// Watch 回调：同样支持字段选择器回退与客户端过滤。
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				usePoolNameFieldSelector := c.usePoolNameFieldSelector.Load()
				tweakListOptions(&options, usePoolNameFieldSelector)
				w, err := c.resourceClient.ResourceSlices().Watch(ctx, options)
				if err != nil && usePoolNameFieldSelector && isUnsupportedPoolNameFieldSelector(err) {
					c.usePoolNameFieldSelector.Store(false)
					usePoolNameFieldSelector = false
					tweakListOptions(&options, usePoolNameFieldSelector)
					w, err = c.resourceClient.ResourceSlices().Watch(ctx, options)
				}
				// 记录 Watch 启动结果。
				logger.V(5).Info("Started watching ResourceSlices", "resourceAPI", c.resourceClient.CurrentAPI(), "err", err)

				// 用包装 watcher 在客户端按 pool 名过滤事件。
				if err == nil && c.reconcilePoolWithName != "" && !usePoolNameFieldSelector {
					return filterSliceWatchByPoolName(ctx, w, c.reconcilePoolWithName), nil
				}

				return w, err
			},
		}, c.resourceClient),
		&resourceapi.ResourceSlice{},
		0,
		indexers,
	)
	// 使用变更缓存跟踪新增与更新，避免 informer 尚未看到新建对象时重复创建同一组设备。
	// 参见 https://pkg.go.dev/k8s.io/client-go/tools/cache#MutationCache。
	c.sliceStore = cache.NewIntegerResourceVersionMutationCache(logger, informer.GetStore(), informer.GetIndexer(), c.mutationCacheTTL, true /* includeAdds */)
	// 注册事件处理器：增删改都会把对应 pool 延迟入队。
	handler, err := informer.AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		// 新增事件：登记对象并调度延迟同步。
		AddFunc: func(obj any) {
			slice, ok := obj.(*resourceapi.ResourceSlice)
			if !ok {
				return
			}
			logger.V(5).Info("ResourceSlice add", "slice", klog.KObj(slice))
			c.sliceStore.OnAddOrUpdate(slice)
			c.queue.AddAfter(slice.Spec.Pool.Name, c.syncDelay)
			logger.V(5).Info("Scheduled sync", "poolName", slice.Spec.Pool.Name, "at", time.Now().Add(c.syncDelay))
		},
		// 更新事件：登记新对象并调度同步；pool 名变化时两个 pool 都要同步。
		UpdateFunc: func(old, new any) {
			oldSlice, ok := old.(*resourceapi.ResourceSlice)
			if !ok {
				return
			}
			newSlice, ok := new.(*resourceapi.ResourceSlice)
			if !ok {
				return
			}
			if loggerV := logger.V(6); loggerV.Enabled() {
				loggerV.Info("ResourceSlice update", "slice", klog.KObj(newSlice), "diff", diff.Diff(oldSlice, newSlice))
			} else {
				logger.V(5).Info("ResourceSlice update", "slice", klog.KObj(newSlice))
			}
			c.sliceStore.OnAddOrUpdate(newSlice)
			c.queue.AddAfter(oldSlice.Spec.Pool.Name, c.syncDelay)
			logger.V(5).Info("Scheduled sync", "pool", oldSlice.Spec.Pool.Name, "at", time.Now().Add(c.syncDelay))
			if oldSlice.Spec.Pool.Name != newSlice.Spec.Pool.Name {
				c.queue.AddAfter(newSlice.Spec.Pool.Name, c.syncDelay)
				logger.V(5).Info("Scheduled sync", "poolName", newSlice.Spec.Pool.Name, "at", time.Now().Add(c.syncDelay))
			}
		},
		// 删除事件：处理墓碑对象，登记删除并调度同步。
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			slice, ok := obj.(*resourceapi.ResourceSlice)
			if !ok {
				return
			}
			logger.V(5).Info("ResourceSlice delete", "slice", klog.KObj(slice))
			c.sliceStore.OnDelete(slice)
			c.queue.AddAfter(slice.Spec.Pool.Name, c.syncDelay)
			logger.V(5).Info("Scheduled sync", "poolName", slice.Spec.Pool.Name, "at", time.Now().Add(c.syncDelay))
		},
	}, cache.HandlerOptions{Logger: &logger})
	// 事件处理器注册失败则包装错误返回。
	if err != nil {
		return fmt.Errorf("registering event handler on the ResourceSlice informer: %w", err)
	}
	// 启动 informer 并等待事件处理器完成初始同步。
	logger.V(3).Info("Starting ResourceSlice informer and waiting for it to sync")
	// 登记 informer goroutine。
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer logger.V(3).Info("ResourceSlice informer has stopped")
		// informer 退出时关闭工作队列。
		defer c.queue.ShutDown() // 执行到这里说明已被要求停止。
		// 运行 informer 直到上下文取消。
		informer.RunWithContext(ctx)
	}()
	// 轮询等待同步完成，期间响应上下文取消。
	for !handler.HasSynced() {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return fmt.Errorf("sync ResourceSlice informer: %w", context.Cause(ctx))
		}
	}
	// informer 已同步，可以开始处理工作项。
	logger.V(3).Info("ResourceSlice informer has synced")
	return nil
}

// run 在后台持续处理工作队列，直到队列关闭。
func (c *Controller) run(ctx context.Context) {
	// 循环处理下一个工作项，返回 false 表示队列已关闭。
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出一个 pool 并执行同步；出错时限速重试。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	// 从队列取出待同步的 pool 名称。
	poolName, shutdown := c.queue.Get()
	// 队列已关闭，停止处理。
	if shutdown {
		return false
	}
	// 无论成功与否都标记该项处理完成。
	defer c.queue.Done(poolName)
	logger := klog.FromContext(ctx)

	// 带上 poolName 日志字段执行同步。
	err := c.syncPool(klog.NewContext(ctx, klog.LoggerWithValues(logger, "poolName", poolName)), poolName)
	// 同步失败：上报错误并限速重试。
	if err != nil {
		c.errorHandler(ctx, err, "processing ResourceSlice objects")
		c.queue.AddRateLimited(poolName)

		return true
	}
	// 成功则清除该 pool 的重试退避记录。
	c.queue.Forget(poolName)
	return true
}

// syncPool 处理单个 pool；因只有一个 worker，除访问 c.resources 外无需加锁。
func (c *Controller) syncPool(ctx context.Context, poolName string) error {
	logger := klog.FromContext(ctx)
	// 记录同步开始时间，用于判断缓存条目是否已过期。
	start := time.Now()

	// 收集实际状态（缓存中的 slice）与期望状态（驱动提供的 resources）。
	var slices []*resourceapi.ResourceSlice
	// 按 pool 名从变更缓存中取出所有相关 slice。
	objs, err := c.sliceStore.ByIndex(poolNameIndex, poolName)
	// 读取缓存失败。
	if err != nil {
		return fmt.Errorf("retrieve ResourceSlice objects: %w", err)
	}
	for _, obj := range objs {
		if slice, ok := obj.(*resourceapi.ResourceSlice); ok {
			slices = append(slices, slice)
		}
	}
	var resources *DriverResources
	// 加读锁快照期望状态。
	c.mutex.RLock()
	resources = c.resources
	c.mutex.RUnlock()
	// 校验期望状态，错误仅上报而不重试。
	if err := validateDriverResources(resources); err != nil {
		c.errorHandler(ctx, err, "pool validation failed")
		// 校验失败只上报一次，不把 pool 重新入队导致无限重试。
		return nil
	}

	// 查找该 pool 的期望定义。
	pool, ok := resources.Pools[poolName]
	// pool 已不存在。
	if !ok {
		// 仍有残留 slice，需要全部删除。
		if len(slices) > 0 {
			logger.V(5).Info("Removing resource slices after pool removal")
			if err := c.removeSlices(ctx, slices); err != nil {
				return fmt.Errorf("remove slices: %w", err)
			}
		}
		return nil
	}

	// 若属主是 Node，确定 NodeName 并在 UID 缺失时查询并缓存它。
	var nodeName string
	// 仅 Node 属主需要此处理。
	if c.owner != nil && c.owner.APIVersion == "v1" && c.owner.Kind == "Node" {
		// 单 pool 协调模式不设置 NodeName。
		if c.reconcilePoolWithName == "" {
			nodeName = c.owner.Name
		}
		// UID 为空时查询 Node 对象，结果在控制器生命周期内缓存。
		if c.owner.UID == "" {
			// 查询失败则包装错误返回。
			node, err := c.coreClient.Nodes().Get(ctx, c.owner.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("retrieve node %q: %w", c.owner.Name, err)
			}
			c.owner.UID = node.UID
		}
	}

	// 找出当前所有 slice 中的最大 generation。
	var generation int64
	for _, slice := range slices {
		if slice.Spec.Pool.Generation > generation {
			generation = slice.Spec.Pool.Generation
		}
	}

	// 将现有 slice 分类：按索引匹配期望项，generation 落后或索引无效/重复的视为过期。
	currentSliceForDesiredSlice := make(map[int]*resourceapi.ResourceSlice, len(pool.Slices))
	obsoleteSlices := make([]*resourceapi.ResourceSlice, 0, len(slices))
	expectedPrefixLength := getIndexLength(len(pool.Slices))

	// 遍历现有 slice 进行分类。
	for _, slice := range slices {
		// generation 小于最高值的 slice 直接判为过期。
		if slice.Spec.Pool.Generation < generation {
			obsoleteSlices = append(obsoleteSlices, slice)
			continue
		}

		// 从名称前缀解码索引，失败则视为无法匹配。
		index, err := decodeIndex(slice.Name, expectedPrefixLength)
		if err != nil {
			logger.V(5).Info("Unmatched existing slice (invalid name)", "slice", klog.KObj(slice), "err", err)
			obsoleteSlices = append(obsoleteSlices, slice)
			continue
		}
		// 索引落在期望范围内时，首个匹配项作为当前 slice。
		if index >= 0 && index < len(pool.Slices) {
			if _, ok := currentSliceForDesiredSlice[index]; !ok {
				currentSliceForDesiredSlice[index] = slice
				logger.V(5).Info("Matched existing slice by index", "slice", klog.KObj(slice), "matchIndex", index)
				continue
			}
		}
		// 其余（重复或越界）归入过期列表。
		obsoleteSlices = append(obsoleteSlices, slice)
		logger.V(5).Info("Unmatched existing slice (obsolete)", "slice", klog.KObj(slice))
	}

	// 计算每个 slice 都必须设置的元数据，以及新增 slice 的数量。
	resourceSliceCount := len(pool.Slices)
	numMatchedSlices := len(currentSliceForDesiredSlice)
	numNewSlices := resourceSliceCount - numMatchedSlices
	desiredPool := resourceapi.ResourcePool{
		Name:               poolName,
		Generation:         generation, // 稍后可能会被更新。
		ResourceSliceCount: int64(resourceSliceCount),
	}

	// 逐项比较，找出内容发生变化、需要更新的 slice。
	changedDesiredSlices := sets.New[int]()
	// 比较元数据、节点选择器、设备、计数器等字段。
	for i, currentSlice := range currentSliceForDesiredSlice {
		// 注意：条目顺序变化也算差异，会触发更新。
		if !apiequality.Semantic.DeepEqual(&currentSlice.Spec.Pool, &desiredPool) ||
			!apiequality.Semantic.DeepEqual(currentSlice.Spec.NodeSelector, pool.NodeSelector) ||
			ptr.Deref(currentSlice.Spec.AllNodes, false) != pool.AllNodes ||
			!DevicesDeepEqual(currentSlice.Spec.Devices, pool.Slices[i].Devices) ||
			!apiequality.Semantic.DeepEqual(currentSlice.Spec.SharedCounters, pool.Slices[i].SharedCounters) ||
			!apiequality.Semantic.DeepEqual(currentSlice.Spec.PerDeviceNodeSelection, pool.Slices[i].PerDeviceNodeSelection) ||
			!apiequality.Semantic.DeepEqual(currentSlice.Spec.PartitionTypeAttribute, pool.Slices[i].PartitionTypeAttribute) ||
			!apiequality.Semantic.DeepEqual(currentSlice.Spec.SkipNodeOperations, pool.Slices[i].SkipNodeOperations) {
			changedDesiredSlices.Insert(i)
			logger.V(5).Info("Need to update slice", "slice", klog.KObj(currentSlice), "matchIndex", i)
		}
	}
	// 记录分类与比较结果。
	logger.V(5).Info("Completed comparison",
		"numObsolete", len(obsoleteSlices),
		"numMatchedSlices", len(currentSliceForDesiredSlice),
		"numChangedMatchedSlices", len(changedDesiredSlices),
		"numNewSlices", numNewSlices,
	)

	// 根据驱动请求与变更规模决定 generation 是否递增。
	bumpedGeneration := false
	// 三种情况：驱动指定更大值、最多一次更新可保持原值、否则递增。
	switch {
	// 驱动显式要求更大的 generation。
	case pool.Generation > generation:
		generation = pool.Generation
		bumpedGeneration = true
		logger.V(5).Info("Bumped generation to driver-provided generation", "generation", generation)
	// 没有新增且最多一次更新时保持 generation。
	case numNewSlices == 0 && len(changedDesiredSlices) <= 1:
		logger.V(5).Info("Kept generation because at most one update API call is necessary", "generation", generation)
	// 其他情况递增 generation，保证原子发布。
	default:
		generation++
		bumpedGeneration = true
		logger.V(5).Info("Bumped generation by one", "generation", generation)
	}
	// 将最终 generation 写入期望 pool 元数据。
	desiredPool.Generation = generation

	// 先删除过期 slice：即使新 slice 有问题，也应先移除驱动不再拥有的设备。
	if err := c.removeSlices(ctx, obsoleteSlices); err != nil {
		return fmt.Errorf("remove slices: %w", err)
	}

	// 更新内容变化或 generation 提升的现有 slice。
	for i, currentSlice := range currentSliceForDesiredSlice {
		// 未变化且 generation 未提升则跳过。
		if !changedDesiredSlices.Has(i) && !bumpedGeneration {
			continue
		}
		// 深拷贝后改写各字段，保留服务端填充的信息。
		slice := currentSlice.DeepCopy()
		slice.Spec.Pool = desiredPool
		// 无需设置 NodeName；若不一致，之前的列表就不会包含该 slice。
		slice.Spec.NodeSelector = pool.NodeSelector
		slice.Spec.AllNodes = refIfNotZero(pool.AllNodes)
		slice.Spec.SharedCounters = pool.Slices[i].SharedCounters
		slice.Spec.PerDeviceNodeSelection = pool.Slices[i].PerDeviceNodeSelection
		slice.Spec.PartitionTypeAttribute = pool.Slices[i].PartitionTypeAttribute
		slice.Spec.SkipNodeOperations = pool.Slices[i].SkipNodeOperations
		// 保留匹配设备的原有 TimeAdded，避免无谓变更。
		slice.Spec.Devices = copyTaintTimeAdded(slice.Spec.Devices, pool.Slices[i].Devices)

		// 调用 API 更新，失败则返回错误触发重试。
		actualSlice, err := c.resourceClient.ResourceSlices().Update(ctx, slice, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("update resource slice: %w", err)
		}
		logger.V(5).Info("Updated existing resource slice", "slice", klog.KObj(slice))
		// 更新成功后累加计数。
		atomic.AddInt64(&c.numUpdates, 1)
		// 将服务端实际结果登记到变更缓存，并对齐期望状态。
		c.sliceStored(ctx, "update ResourceSlice", poolName, pool, i, slice, actualSlice)
	}

	// 为尚无对应 slice 的期望项创建新对象。
	added := false
	// 遍历所有期望 slice。
	for i := 0; i < len(pool.Slices); i++ {
		// 已通过更新处理的跳过。
		if _, ok := currentSliceForDesiredSlice[i]; ok {
			continue
		}
		// 存在属主时设置 ownerReference。
		var ownerReferences []metav1.OwnerReference
		if c.owner != nil {
			ownerReferences = append(ownerReferences,
				metav1.OwnerReference{
					APIVersion: c.owner.APIVersion,
					Kind:       c.owner.Kind,
					Name:       c.owner.Name,
					UID:        c.owner.UID,
					Controller: ptr.To(true),
				},
			)
		}
		// 生成名称以索引的十六进制前缀、驱动名、属主名开头，API 服务器追加随机后缀保证唯一。
		generateName := encodeIndex(i, expectedPrefixLength) + nameSeparator + c.driverName + nameSeparator
		if c.owner != nil {
			generateName += c.owner.Name + nameSeparator
		}
		// 构造待创建的 ResourceSlice。
		slice := &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: ownerReferences,
				GenerateName:    generateName,
			},
			Spec: resourceapi.ResourceSliceSpec{
				Driver:                 c.driverName,
				Pool:                   desiredPool,
				NodeName:               refIfNotZero(nodeName),
				NodeSelector:           pool.NodeSelector,
				AllNodes:               refIfNotZero(pool.AllNodes),
				Devices:                pool.Slices[i].Devices,
				SharedCounters:         pool.Slices[i].SharedCounters,
				PerDeviceNodeSelection: pool.Slices[i].PerDeviceNodeSelection,
				PartitionTypeAttribute: pool.Slices[i].PartitionTypeAttribute,
				SkipNodeOperations:     pool.Slices[i].SkipNodeOperations,
			},
		}

		// 使用变更缓存避免因 informer 尚未看到新建对象而重复创建。
		// 调用 API 创建，失败则返回错误触发重试。
		actualSlice, err := c.resourceClient.ResourceSlices().Create(ctx, slice, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create resource slice: %w", err)
		}
		logger.V(5).Info("Created new resource slice", "slice", klog.KObj(actualSlice))
		// 创建成功后累加计数。
		atomic.AddInt64(&c.numCreates, 1)
		// 标记本次同步有新增，需记录时间以安排复核。
		added = true
		// 对齐期望状态与实际存储结果。
		c.sliceStored(ctx, "create ResourceSlice", poolName, pool, i, slice, actualSlice)
	}

	// 处理变更缓存的过期复核逻辑。TTL 复核与 informer 调度可能竞争，只要最终发生一次复核即可。
	// lastAdd 不早于插入变更缓存的时间，因此到期同步时缓存中不会仍有该新建对象。
	now := time.Now()
	// 有新增则记录时间。
	if added {
		c.lastAddByPool[poolName] = now
		logger.V(5).Info("Added slices")
		// 距上次新增已超过 TTL 则清理记录，不再复核。
	} else if lastAdd, ok := c.lastAddByPool[poolName]; ok && start.After(lastAdd.Add(c.mutationCacheTTL)) {
		delete(c.lastAddByPool, poolName)
		logger.V(5).Info("Done with re-syncing")
	}
	// 仍在 TTL 内则安排在过期时刻再次同步。
	if lastAdd, ok := c.lastAddByPool[poolName]; ok {
		// 计算复核时间并延迟入队。
		when := lastAdd.Add(c.mutationCacheTTL)
		c.queue.AddAfter(poolName, when.Sub(now))
		logger.V(5).Info("Scheduled re-sync", "at", when)
	}

	return nil
}

// removeSlices 删除给定的过期 slice，忽略已不存在的对象。
func (c *Controller) removeSlices(ctx context.Context, slices []*resourceapi.ResourceSlice) error {
	logger := klog.FromContext(ctx)

	// 逐项删除。
	for _, slice := range slices {
		// 通过 UID 与 ResourceVersion 前置条件避免误删同名的新对象。
		options := metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &slice.UID,
				ResourceVersion: &slice.ResourceVersion,
			},
		}
		// 刚删除后缓存可能尚未更新，重复删除会得到 NotFound，忽略即可。
		// MutationCache 不跟踪待删除对象；多一次删除比多一次创建危害更小。
		// 调用 API 删除。
		err := c.resourceClient.ResourceSlices().Delete(ctx, slice.Name, options)
		// 根据结果区分成功、NotFound 与其他错误。
		switch {
		case err == nil:
			// 删除成功，累加计数。
			logger.V(5).Info("Deleted obsolete resource slice", "slice", klog.KObj(slice), "deleteOptions", options)
			atomic.AddInt64(&c.numDeletes, 1)
		case apierrors.IsNotFound(err):
			// 已被删除，忽略。
			logger.V(5).Info("Resource slice was already deleted earlier", "slice", klog.KObj(slice))
		default:
			// 其他错误返回并触发重试。
			return fmt.Errorf("delete resource slice: %w", err)
		}
	}

	return nil
}

// sliceStored 在创建或更新后调用，用服务端返回的对象校正本地期望状态并登记到变更缓存。
func (c *Controller) sliceStored(ctx context.Context, msg string, poolName string, pool Pool, sliceIndex int, desiredSlice, actualSlice *resourceapi.ResourceSlice) {
	// 将实际对象登记进变更缓存，避免重复操作。
	c.sliceStore.Mutation(actualSlice)

	// 服务端可能补填 TimeAdded，先同步回期望对象以便后续比较。
	if copyServerDefaults(desiredSlice, actualSlice) {
		pool.Slices[sliceIndex].Devices = actualSlice.Spec.Devices
	}

	// 若字段被丢弃，调整后续同步使用的期望状态，使其与实际可存储内容一致，避免无限更新重试。
	if !apiequality.Semantic.DeepEqual(desiredSlice.Spec.PerDeviceNodeSelection, actualSlice.Spec.PerDeviceNodeSelection) ||
		!apiequality.Semantic.DeepEqual(desiredSlice.Spec.SharedCounters, actualSlice.Spec.SharedCounters) ||
		!apiequality.Semantic.DeepEqual(desiredSlice.Spec.PartitionTypeAttribute, actualSlice.Spec.PartitionTypeAttribute) ||
		!apiequality.Semantic.DeepEqual(desiredSlice.Spec.SkipNodeOperations, actualSlice.Spec.SkipNodeOperations) ||
		!apiequality.Semantic.DeepEqual(desiredSlice.Spec.Devices, actualSlice.Spec.Devices) {
		pool.Slices[sliceIndex].PerDeviceNodeSelection = actualSlice.Spec.PerDeviceNodeSelection
		pool.Slices[sliceIndex].SharedCounters = actualSlice.Spec.SharedCounters
		pool.Slices[sliceIndex].PartitionTypeAttribute = actualSlice.Spec.PartitionTypeAttribute
		pool.Slices[sliceIndex].SkipNodeOperations = actualSlice.Spec.SkipNodeOperations
		pool.Slices[sliceIndex].Devices = actualSlice.Spec.Devices

		// 构造字段丢弃错误并上报。
		err := &DroppedFieldsError{
			PoolName:     poolName,
			SliceIndex:   sliceIndex,
			DesiredSlice: desiredSlice.DeepCopy(),
			ActualSlice:  actualSlice.DeepCopy(),
		}
		c.errorHandler(ctx, err, msg)
	}
}

// copyServerDefaults 把服务端补填的 TimeAdded 拷贝回期望对象，返回是否发生了修改。
func copyServerDefaults(desiredSlice, actualSlice *resourceapi.ResourceSlice) bool {
	copied := false

	// 按位置遍历设备与污点，只补填缺失的 TimeAdded。
	for i := 0; i < len(desiredSlice.Spec.Devices) && i < len(actualSlice.Spec.Devices); i++ {
		for e := 0; e < len(desiredSlice.Spec.Devices[i].Taints) && e < len(actualSlice.Spec.Devices[i].Taints); e++ {
			if desiredSlice.Spec.Devices[i].Taints[e].TimeAdded == nil && actualSlice.Spec.Devices[i].Taints[e].TimeAdded != nil {
				// 首次修改时先深拷贝 Spec，避免污染共享对象。
				if !copied {
					desiredSlice.Spec = *desiredSlice.Spec.DeepCopy()
					copied = true
				}
				desiredSlice.Spec.Devices[i].Taints[e].TimeAdded = actualSlice.Spec.Devices[i].Taints[e].TimeAdded
			}
		}
	}
	return copied
}

// copyTaintTimeAdded 从 from 中为 to 的匹配设备补填缺失的 TimeAdded；两个输入切片只读。
func copyTaintTimeAdded(from, to []resourceapi.Device) []resourceapi.Device {
	// 克隆目标切片，保持输入只读。
	to = slices.Clone(to)
	// 按设备名查找匹配的旧设备。
	for i, toDevice := range to {
		index := slices.IndexFunc(from, func(fromDevice resourceapi.Device) bool {
			return fromDevice.Name == toDevice.Name
		})
		// 没有匹配设备则跳过。
		if index < 0 {
			continue
		}
		fromDevice := from[index]
		// 遍历目标设备的污点。
		for j, toTaint := range toDevice.Taints {
			// 已设置 TimeAdded 则跳过。
			if toTaint.TimeAdded != nil {
				continue
			}
			// 仅当其他字段完全一致时保留旧的 TimeAdded。
			index := slices.IndexFunc(fromDevice.Taints, func(fromTaint resourceapi.DeviceTaint) bool {
				return toTaint.Key == fromTaint.Key &&
					toTaint.Value == fromTaint.Value &&
					toTaint.Effect == fromTaint.Effect
			})
			// 没有匹配旧污点则跳过。
			if index < 0 {
				continue
			}
			// 修改前深拷贝整个设备（污点通常很少）。
			to[i] = *toDevice.DeepCopy()
			to[i].Taints[j].TimeAdded = fromDevice.Taints[index].TimeAdded
		}
	}
	return to
}

// DevicesDeepEqual 比较设备切片，语义类似 apiequality.Semantic.DeepEqual，但容忍 TimeAdded 为 nil 或秒级取整差异。
func DevicesDeepEqual(a, b []resourceapi.Device) bool {
	return devicesSemantic.DeepEqual(a, b)
}

// devicesSemantic 是在标准语义相等性副本上追加了设备污点比较规则的实例。
var devicesSemantic = func() conversion.Equalities {
	semantic := apiequality.Semantic.Copy()
	// 注册自定义比较函数，失败则直接 panic（发生在初始化期）。
	if err := semantic.AddFunc(deviceTaintEqual); err != nil {
		panic(err)
	}
	return semantic
}()

// deviceTaintEqual 比较两个设备污点，TimeAdded 相差在 1 秒内视为相等。
func deviceTaintEqual(a, b resourceapi.DeviceTaint) bool {
	// 两侧都有时间时比较差值，超过 1 秒判为不等。
	if a.TimeAdded != nil && b.TimeAdded != nil {
		delta := b.TimeAdded.Time.Sub(a.TimeAdded.Time)
		if delta < -time.Second || delta > time.Second {
			return false
		}
	}
	// 其余字段必须完全一致。
	return a.Key == b.Key &&
		a.Value == b.Value &&
		a.Effect == b.Effect
}

// refIfNotZero 对非零值返回其指针，零值返回 nil，便于写入可选指针字段。
func refIfNotZero[T comparable](t T) *T {
	var zero T
	// 零值返回 nil。
	if t == zero {
		return nil
	}
	return &t
}

// getIndexLength 计算用十六进制表示最大索引所需的最小前缀长度，并保证不小于 resourceSliceIndexMinLength。
func getIndexLength(numSlices int) int {
	hexLen := len(strconv.FormatInt(int64(numSlices-1), 16))

	return max(resourceSliceIndexMinLength, hexLen)
}

// decodeIndex 从名称中解析定长十六进制前缀作为索引，格式非法或溢出时返回 -1 与错误。
func decodeIndex(name string, expectedLength int) (int, error) {
	// 定位分隔符，其位置必须等于预期前缀长度。
	idx := strings.Index(name, nameSeparator)
	if idx != expectedLength {
		return -1, fmt.Errorf("invalid index length or missing separator")
	}
	prefix := name[:idx]

	// 按十六进制解析前缀，ParseInt 自动处理非法字符与溢出。
	res, err := strconv.ParseInt(prefix, 16, 0)
	if err != nil {
		return -1, fmt.Errorf("failed to parse index: %w", err)
	}

	return int(res), nil
}

// encodeIndex 将索引编码为定长的十六进制字符串，不足位补零。
func encodeIndex(index int, expectedLength int) string {
	return fmt.Sprintf("%0*x", expectedLength, index)
}

// retainSlicesByPoolName 在客户端从列表中移除不属于目标 pool 的 slice。
func retainSlicesByPoolName(sliceList *resourceapi.ResourceSliceList, poolName string) {
	// 原地过滤，仅保留目标 pool 的对象。
	sliceList.Items = slices.DeleteFunc(sliceList.Items, func(slice resourceapi.ResourceSlice) bool {
		return slice.Spec.Pool.Name != poolName
	})
}

// isUnsupportedPoolNameFieldSelector 判断错误是否表示服务端不支持 poolName 字段选择器。
// 客户端 List/Watch 过滤仅为向后兼容，未来可以移除。
func isUnsupportedPoolNameFieldSelector(err error) bool {
	return apierrors.IsBadRequest(err)
}

// filterSliceWatchByPoolName 包装 watch，仅转发目标 pool 的 ResourceSlice 事件，其他事件（如错误、书签）始终放行。
func filterSliceWatchByPoolName(ctx context.Context, w watch.Interface, poolName string) watch.Interface {
	// 用匹配函数过滤事件对象。
	return newWrapWatcher(ctx, w, func(event watch.Event) bool {
		// 非 ResourceSlice 事件（如错误事件）直接放行。
		resourceSlice, ok := event.Object.(*resourceapi.ResourceSlice)
		if !ok {
			// 例如 error 事件。
			return true
		}
		// 书签等事件的空对象直接放行。
		if resourceSlice == nil || (resourceSlice.Name == "" && resourceSlice.Spec.Pool.Name == "") {
			// 例如 bookmark 事件。
			return true
		}
		// 仅保留 pool 名匹配的事件。
		return resourceSlice.Spec.Pool.Name == poolName
	})
}
