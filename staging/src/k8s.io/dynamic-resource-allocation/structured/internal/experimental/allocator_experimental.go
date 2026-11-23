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

// Package experimental 提供 DRA（动态资源分配）实验版结构化分配器的核心实现。
// 本文件实现分配器主体：资源池收集、请求校验、约束匹配、回溯求解以及结果构造。
package experimental

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// 以下类型是内部包中对应定义的别名，便于本实验变体直接复用公共实现。
// DeviceClassLister：按名获取 DeviceClass 的接口。
// Features：控制本变体启用的 DRA 特性开关集合。
// DeviceID：由驱动名、资源池名和设备名唯一确定一台设备。
// Stats：对外暴露的分配器运行统计。
type DeviceClassLister = internal.DeviceClassLister
type Features = internal.Features
type DeviceID = internal.DeviceID
type Stats = internal.Stats

// MakeDeviceID 根据驱动名、资源池名与设备名构造 DeviceID。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return internal.MakeDeviceID(driver, pool, device)
}

// 以下类型是容量与共享分配相关内部定义的别名。
// SharedDeviceID：共享分配的设备份额标识。
// DeviceConsumedCapacity：单台设备已消耗的容量。
// ConsumedCapacityCollection：按设备 ID 聚合的已消耗容量集合。
// ConsumedCapacity：按限定名索引的容量消耗量。
// AllocatedState：集群中已完成分配的设备与容量状态。
type SharedDeviceID = internal.SharedDeviceID
type DeviceConsumedCapacity = internal.DeviceConsumedCapacity
type ConsumedCapacityCollection = internal.ConsumedCapacityCollection
type ConsumedCapacity = internal.ConsumedCapacity
type AllocatedState = internal.AllocatedState

// GenerateNewShareID 为共享分配生成一个新的份额 ID。
func GenerateNewShareID() *types.UID {
	return internal.GenerateShareID()
}

// NewConsumedCapacity 创建一个空的已消耗容量对象。
func NewConsumedCapacity() ConsumedCapacity {
	return internal.NewConsumedCapacity()
}

// NewDeviceConsumedCapacity 为指定设备构造其已消耗容量记录。
func NewDeviceConsumedCapacity(deviceID DeviceID,
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) DeviceConsumedCapacity {
	return internal.NewDeviceConsumedCapacity(deviceID, consumedCapacity)
}

// NewConsumedCapacityCollection 创建一个空的已消耗容量集合。
func NewConsumedCapacityCollection() ConsumedCapacityCollection {
	return internal.NewConsumedCapacityCollection()
}

// SupportedFeatures includes all additional features,
// making this the variant that is used when any of those
// are enabled.
// SupportedFeatures 列出本实验变体支持的全部附加特性；只要其中任一特性被启用，就应使用此变体。
// 字段：AdminAccess 允许管理访问；PrioritizedList 支持 firstAvailable 子请求；PartitionableDevices 支持计数器分区。
// DeviceTaints 支持设备污点与容忍；DeviceBindingAndStatus 支持绑定条件；ConsumableCapacity 支持容量消耗。
// FractionalCapacityRange 支持分数容量；ListTypeAttributes 支持列表类型属性；OptionalNodeOperations 支持可选节点操作。
// DerivedAttributes 支持 CEL 派生属性；CompatibilityGroups 支持兼容性组约束。
var SupportedFeatures = internal.Features{
	AdminAccess:             true,
	PrioritizedList:         true,
	PartitionableDevices:    true,
	DeviceTaints:            true,
	DeviceBindingAndStatus:  true,
	ConsumableCapacity:      true,
	FractionalCapacityRange: true,
	ListTypeAttributes:      true,
	OptionalNodeOperations:  true,
	DerivedAttributes:       true,
	CompatibilityGroups:     true,
}

// Allocator 是实验版结构化分配器，可被多次复用且并发安全；它惰性缓存池级信息以避免重复计算。
// 字段：features 为启用的特性集合；allocatedState 为已完成分配的状态；classLister 用于按名查询设备类。
// slicesOnNode 为按节点名索引的节点专属 slice；slicesShared 为不绑定节点的共享 slice；allSlices 为传入的全部 slice。
// celCache 缓存已编译的 CEL 表达式；availableCounters 缓存各池可用计数器；compatibilityGroupsBaseline 缓存各池兼容性组基线。
// mutex 保护两张缓存的并发访问；numAllocateOneInvocations 统计 allocateOne 的调用次数。
type Allocator struct {
	features       Features
	allocatedState AllocatedState
	classLister    DeviceClassLister
	slicesOnNode   map[string][]*resourceapi.ResourceSlice
	slicesShared   []*resourceapi.ResourceSlice
	allSlices      []*resourceapi.ResourceSlice
	celCache       *cel.Cache
	// availableCounters contains the available counters for each
	// resource pool. It acts as a cache that is updated the first time
	// the available counters are needed for each pool. The information
	// about each pool is never updated once set the first time.
	// This is computed bsed on information on the Allocator, so it will
	// be correct even for multiple usages of the Allocator.
	// The keys in the map are resource pool IDs (driver name and pool name).
	// The allocator might be accessed by different goroutines, so
	// access to this map must be synchronized.
	// availableCounters 按池 ID 缓存可用计数器，首次使用时计算且此后不再变化，访问需加锁。
	availableCounters map[PoolID]counterSets
	// compatibilityGroupsBaseline caches, per resource pool, the
	// compatibility-group intersection contributed by already-allocated devices
	// (their groups read live from the source ResourceSlice). Like
	// availableCounters it is computed lazily, never changes once set, and is
	// guarded by mutex. The keys in the map are resource pool IDs (driver name
	// and pool name), and the inner map keys are counter set names.
	// compatibilityGroupsBaseline 按池缓存已分配设备对各计数器集的兼容性组交集基线，惰性计算且不再变化，受 mutex 保护。
	compatibilityGroupsBaseline map[PoolID]map[string]compatibilityGroupIntersection
	mutex                       sync.RWMutex
	// numAllocateOneInvocations counts the number of times the allocateOne
	// function is called for the allocator. This is a measurement of the
	// amount of work the allocator had to do to allocate devices
	// for the claims.
	// numAllocateOneInvocations 统计 allocateOne 的调用次数，用于衡量分配所需工作量。
	numAllocateOneInvocations atomic.Int64
}

// 编译期断言：Allocator 实现了 internal.AllocatorExtended 接口。
var _ internal.AllocatorExtended = &Allocator{}

// NewAllocator returns an allocator for a certain set of claims or an error if
// some problem was detected which makes it impossible to allocate claims.
//
// The returned Allocator can be used multiple times and is thread-safe.
// NewAllocator 为给定的声明集构造分配器；若检测到导致无法分配的问题则返回错误。
// 返回的分配器可被多次使用且并发安全。
func NewAllocator(ctx context.Context,
	features Features,
	allocatedState AllocatedState,
	classLister DeviceClassLister,
	slices []*resourceapi.ResourceSlice,
	celCache *cel.Cache,
) (*Allocator, error) {
	// 按 slice.Spec.NodeName 把 ResourceSlice 分成节点专属与共享两类：节点名为空的归入共享列表。
	slicesOnNode := make(map[string][]*resourceapi.ResourceSlice)
	slicesShared := make([]*resourceapi.ResourceSlice, 0)
	for _, slice := range slices {
		// 读取 slice 的目标节点名；空字符串表示不绑定节点。
		nodeName := ptr.Deref(slice.Spec.NodeName, "")
		if nodeName == "" {
			slicesShared = append(slicesShared, slice)
		} else {
			slicesOnNode[nodeName] = append(slicesOnNode[nodeName], slice)
		}
	}
	// 构造并初始化 Allocator，预创建两张惰性缓存映射。
	a := &Allocator{
		features:          features,
		allocatedState:    allocatedState,
		classLister:       classLister,
		slicesOnNode:      slicesOnNode,
		slicesShared:      slicesShared,
		allSlices:         slices,
		celCache:          celCache,
		availableCounters: make(map[PoolID]counterSets),

		compatibilityGroupsBaseline: make(map[PoolID]map[string]compatibilityGroupIntersection),
	}
	return a, nil
}

// Channel 返回本分配器所属的实现通道，此处为实验版通道。
func (a *Allocator) Channel() internal.AllocatorChannel {
	return internal.Experimental
}

// Allocate 为给定节点上的一组 ResourceClaim 分配设备：先收集候选资源池、校验请求与约束，
// 再通过穷举回溯为每个请求选出满足全部条件的设备，最终生成 AllocationResult。
func (a *Allocator) Allocate(ctx context.Context, node *v1.Node, claims []*resourceapi.ResourceClaim) (finalResult []resourceapi.AllocationResult, finalErr error) {
	// 构造单次分配使用的 allocator 状态，集中保存本次分配的可变数据（匹配缓存、约束、已消耗计数器、请求数据与结果等）。
	alloc := &allocator{
		Allocator:              a,
		ctx:                    ctx, // all methods share the same a and thus ctx
		logger:                 klog.FromContext(ctx),
		node:                   node,
		claimsToAllocate:       claims,
		deviceMatchesRequest:   make(map[matchKey]bool),
		derivedAttributesCache: make(map[deviceExprKey]*resourceapi.DeviceAttribute),
		constraints:            make([][]constraint, len(claims)),
		consumedCounters:       make(map[PoolID]counterSets),

		consumedCompatibilityGroups: make(map[PoolID]map[string]compatibilityGroupIntersection),

		requestData:        make(map[requestIndices]requestData),
		result:             make([]internalAllocationResult, len(claims)),
		allocatingCapacity: NewConsumedCapacityCollection(),
	}
	// 汇总目标节点专属 slice 与共享 slice，作为本次分配可用的 slice 集合。
	slicesForNode := slices.Concat(alloc.slicesOnNode[node.Name], alloc.slicesShared)
	alloc.logger.V(5).Info("Starting allocation", "numClaims", len(alloc.claimsToAllocate), "numSlicesForNode", len(slicesForNode))
	// 记录分配开始与结束（含是否成功及错误）的日志。
	defer func() {
		alloc.logger.V(5).Info("Done with allocation", "success", len(finalResult) == len(alloc.claimsToAllocate), "err", finalErr)
	}()

	// 第一步：收集所有合格的资源池；失败则直接返回错误。
	// First determine all eligible pools.
	pools, err := GatherPools(ctx, slicesForNode, node, a.features, alloc.allSlices)
	if err != nil {
		return nil, fmt.Errorf("gather pool information: %w", err)
	}
	alloc.pools = pools
	alloc.logger.V(5).Info("Gathered pool information", "pools", logPools(alloc.logger, pools))

	// We allocate one claim after the other and for each claim, all of
	// its requests. For each individual device we pick one possible
	// candidate after the other, checking constraints as we go.
	// Each chosen candidate is marked as "in use" and the process
	// continues, recursively. This way, all requests get matched against
	// all candidates in all possible orders.
	//
	// The first full solution is chosen.
	//
	// In other words, this is an exhaustive search. This is okay because
	// it aborts early. Once scoring gets added, more intelligence may be
	// needed to avoid trying "equivalent" solutions (two identical
	// requests, two identical devices, two solutions that are the same in
	// practice).

	// 整体策略：逐个声明、逐个请求、逐个设备地穷举候选，边选边校验约束；选中即标记占用并递归，找到第一个完整解即返回。
	// This is where we sanity check that we can actually handle the claims
	// and their requests. For each claim we determine how many devices
	// need to be allocated. If not all can be stored in the result, the
	// claim cannot be allocated.
	// 第二步：校验所有声明与请求，并计算每个声明至少需要分配的设备数下限。
	minDevicesTotal := 0
	// 遍历每个声明。
	for claimIndex, claim := range alloc.claimsToAllocate {
		minDevicesPerClaim := 0

		// If we have any any request that wants "all" devices, we need to
		// figure out how much "all" is. If some pool is incomplete, we stop
		// here because allocation cannot succeed. Once we do scoring, we should
		// stop in all cases, not just when "all" devices are needed, because
		// pulling from an incomplete might not pick the best solution and it's
		// better to wait. This does not matter yet as long the incomplete pool
		// has some matching device.
		// 遍历声明中的请求；使用 firstAvailable 子请求时只能确定设备数下限。
		for requestIndex := range claim.Spec.Devices.Requests {
			request := &claim.Spec.Devices.Requests[requestIndex]
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			hasSubRequests := len(request.FirstAvailable) > 0

			// 若特性未启用但请求包含子请求，直接报错以避免意外行为。
			// Error out if the prioritizedList feature is not enabled and the request
			// has subrequests. This is to avoid surprising behavior for users.
			if !a.features.PrioritizedList && hasSubRequests {
				return nil, fmt.Errorf("claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name)
			}

			// 若特性未启用但请求（含子请求）携带容量需求，直接报错。
			// Error out if the consumableCapacity feature is not enabled
			// and the request contains capacity requests.
			if !a.features.ConsumableCapacity {
				if request.Exactly != nil && request.Exactly.Capacity != nil {
					return nil, fmt.Errorf("claim %s, request %s: has capacity requests, but the DRAConsumableCapacity feature is disabled",
						klog.KObj(claim), request.Name)
				}
				for _, subReq := range request.FirstAvailable {
					// Error out if any subrequest contains capacity requests.
					if subReq.Capacity != nil {
						return nil, fmt.Errorf("claim %s, subrequest %s: has capacity requests, but the DRAConsumableCapacity feature is disabled",
							klog.KObj(claim), subReq.Name)
					}
				}
			}

			// 计算该请求至少需要的设备数：有子请求时取各子请求最小值，否则取请求自身数量。
			var minDevicesPerRequest int
			if hasSubRequests {
				// Seeded high so the loop below can take the minimum.
				minDevicesPerRequest = math.MaxInt
				// A request with subrequests gets one entry per subrequest in alloc.requestData.
				// We can only predict a lower number of devices because it depends on which
				// subrequest gets chosen.
				// 对每个子请求执行校验并填充 requestData，取所需设备数的最小值。
				for i, subReq := range request.FirstAvailable {
					requestKey.subRequestIndex = i
					reqData, err := alloc.validateDeviceRequest(&deviceSubRequestAccessor{subRequest: &subReq},
						&exactDeviceRequestAccessor{request: request}, requestKey, pools)
					if err != nil {
						return nil, err
					}
					alloc.requestData[requestKey] = reqData
					minDevicesPerRequest = min(minDevicesPerRequest, reqData.numDevices)
				}
			} else {
				// 无子请求时，按普通请求校验并确定所需设备数。
				reqData, err := alloc.validateDeviceRequest(&exactDeviceRequestAccessor{request: request}, nil, requestKey, pools)
				if err != nil {
					return nil, err
				}
				alloc.requestData[requestKey] = reqData
				minDevicesPerRequest = reqData.numDevices
			}
			// 先与剩余容量比较再累加，避免大数量导致总数溢出。
			// Compare against the remaining capacity before adding, so a large count cannot overflow the total.
			if minDevicesPerRequest > resourceapi.AllocationResultsMaxSize-minDevicesPerClaim {
				return nil, fmt.Errorf("claim %s, request %s: adding %d devices exceeds the claim limit of %d, with %d already accounted for", klog.KObj(claim), request.Name, minDevicesPerRequest, resourceapi.AllocationResultsMaxSize, minDevicesPerClaim)
			}
			minDevicesPerClaim += minDevicesPerRequest
		}
		alloc.logger.V(6).Info("Checked claim", "claim", klog.KObj(claim), "minDevices", minDevicesPerClaim)

		// Only a lower bound: a subrequest may pick more, so allocateOne checks the limit again.
		// 以设备数下限预分配结果切片；子请求可能选用更多设备，allocateOne 中还会再次检查上限。
		alloc.result[claimIndex].devices = make([]internalDeviceResult, 0, minDevicesPerClaim)

		// Constraints are assumed to be monotonic: once a constraint returns
		// false, adding more devices will not cause it to return true. This
		// allows the search to stop early once a constraint returns false.
		// 第三步：为每个声明构造约束对象；约束被假定为单调的——一旦失败，继续添加设备也不会成功，可提前终止搜索。
		constraints := make([]constraint, len(claim.Spec.Devices.Constraints))
		// 按约束字段的类型分别构造 matchAttribute 或 distinctAttribute 约束；未知类型直接报错。
		for i, constraint := range claim.Spec.Devices.Constraints {
			switch {
			// 根据非空字段识别约束类型并构造对应实现。
			case constraint.MatchAttribute != nil:
				matchAttribute := resourceapi.FullyQualifiedName(*constraint.MatchAttribute)
				logger := alloc.logger
				if loggerV := alloc.logger.V(6); loggerV.Enabled() {
					logger = klog.LoggerWithName(logger, "matchAttributeConstraint")
					logger = klog.LoggerWithValues(logger, "matchAttribute", matchAttribute)
				}
				// matchAttribute 约束：所有相关设备必须共享相同的属性值。
				m := &matchAttributeConstraint{
					logger:            logger,
					requestNames:      sets.New(constraint.Requests...),
					attributeName:     matchAttribute,
					features:          a.features,
					attributeProvider: alloc,
				}
				constraints[i] = m
			case constraint.DistinctAttribute != nil:
				distinctAttribute := resourceapi.FullyQualifiedName(*constraint.DistinctAttribute)
				logger := alloc.logger
				if loggerV := alloc.logger.V(6); loggerV.Enabled() {
					logger = klog.LoggerWithName(logger, "distinctAttributeConstraint")
					logger = klog.LoggerWithValues(logger, "distinctAttribute", distinctAttribute)
				}
				// distinctAttribute 约束：所有相关设备的属性值必须互不相同。
				m := &distinctAttributeConstraint{
					logger:            logger,
					requestNames:      sets.New(constraint.Requests...),
					attributeName:     distinctAttribute,
					features:          a.features,
					attributeProvider: alloc,
				}
				constraints[i] = m
			// 未识别的约束类型，视为错误。
			default:
				// Unknown constraint type!
				return nil, fmt.Errorf("claim %s, constraint #%d: empty constraint (unsupported constraint type?)", klog.KObj(claim), i)
			}
		}
		// 保存该声明的约束列表并累加设备数下限。
		alloc.constraints[claimIndex] = constraints
		minDevicesTotal += minDevicesPerClaim
	}

	// Selecting a device for a request is independent of what has been
	// allocated already. Therefore the result of checking a request against
	// a device instance in the pool can be cached. The pointer to both
	// can serve as key because they are static for the duration of
	// the Allocate call and can be compared in Go.
	// 第四步：初始化设备/请求匹配缓存以及“正在分配的设备”集合，容量按设备总数下限预估。
	alloc.deviceMatchesRequest = make(map[matchKey]bool)

	// We can estimate the size based on what we need to allocate.
	// 预分配 allocatingDevices 映射，容量按设备总数下限估算。
	alloc.allocatingDevices = make(map[DeviceID]sets.Set[int], minDevicesTotal)

	alloc.logger.V(6).Info("Gathered information about devices", "numAllocatedDevices", len(alloc.allocatedState.AllocatedDevices), "minDevicesToBeAllocated", minDevicesTotal)

	// In practice, there aren't going to be many different CEL
	// expressions. Most likely, there is going to be handful of different
	// device classes that get used repeatedly. Different requests may all
	// use the same selector. Therefore compiling CEL expressions on demand
	// could be a useful performance enhancement. It's not implemented yet
	// because the key is more complex (just the string?) and the memory
	// for both key and cached content is larger than for device matches.
	//
	// We may also want to cache this in the shared [Allocator] instance,
	// which implies adding locking.

	// All errors get created such that they can be returned by Allocate
	// without further wrapping.
	// 第五步：从首个请求开始递归分配；errStop 表示已确定无法分配，返回空结果。
	done, err := alloc.allocateOne(deviceIndices{}, false, deviceLocation{})
	// 收到 errStop 表示分配确定无法完成，返回空结果与空错误。
	if errors.Is(err, errStop) {
		return nil, nil
	}
	// 其它错误直接向上返回。
	if err != nil {
		return nil, err
	}
	// 未找到解：若过程中遇到无效资源池则返回特定错误（其它节点仍可尝试），否则返回空结果。
	if !done {
		// If no devices could be allocated, but we found one or more
		// invalid pools, return an error here. We didn't do it during
		// allocation since there might be valid pools from which the
		// claims could be satisfied.
		// 仅在分配失败且存在无效池时，返回携带 ErrFailedAllocationOnNode 的错误。
		for _, pool := range pools {
			if pool.IsInvalid {
				// Not a fatal error, allocation on other nodes may proceed.
				// The error is only surfaced if allocation fails on all nodes.
				return nil, fmt.Errorf("invalid resource pools were encountered%w", internal.ErrFailedAllocationOnNode)
			}
		}
		return nil, nil
	}

	// 第六步：把内部分配结果转换为 API 的 AllocationResult，逐设备填充字段并深拷贝容量。
	result := make([]resourceapi.AllocationResult, len(alloc.result))
	for claimIndex, internalResult := range alloc.result {
		claim := alloc.claimsToAllocate[claimIndex]
		allocationResult := &result[claimIndex]
		allocationResult.Devices.Results = make([]resourceapi.DeviceRequestAllocationResult, len(internalResult.devices))
		for i, internal := range internalResult.devices {
			// 深拷贝已消耗容量，避免与内部状态共享底层 map。
			var consumedCapacity map[resourceapi.QualifiedName]resource.Quantity
			if internal.consumedCapacity != nil {
				consumedCapacity = make(map[resourceapi.QualifiedName]resource.Quantity, len(internal.consumedCapacity))
				for key, val := range internal.consumedCapacity {
					consumedCapacity[key] = val.DeepCopy()
				}
			}
			// 填充设备结果：请求名、驱动、池、设备、管理访问、容忍、份额 ID、容量及跳过节点操作标记。
			allocationResult.Devices.Results[i] = resourceapi.DeviceRequestAllocationResult{
				Request:          internal.requestName(),
				Driver:           internal.id.Driver.String(),
				Pool:             internal.id.Pool.String(),
				Device:           internal.id.Device.String(),
				AdminAccess:      internal.adminAccess,
				Tolerations:      internal.lookupRequest(claim).tolerations(),
				ShareID:          internal.shareID,
				ConsumedCapacity: consumedCapacity,
				// If OptionalNodeOperations feature is off, allocateDevice ensures
				// selected devices have SkipNodeOperations == nil.
				SkipNodeOperations: internal.slice.Spec.SkipNodeOperations,
			}
			// Performance optimization: skip the for loop if the feature is off.
			// Not needed for correctness because if the feature is off, the selected
			// device should not have binding conditions.
			// 仅在设备绑定与状态特性启用时携带绑定条件。
			if a.features.DeviceBindingAndStatus {
				allocationResult.Devices.Results[i].BindingConditions = internal.BindingConditions
				allocationResult.Devices.Results[i].BindingFailureConditions = internal.BindingFailureConditions
			}
		}

		// 填充设备类配置：每个类的配置只添加一次，多个使用该类的请求共享同一份配置。
		// Populate configs.

		// Each class config gets added only once.
		// We need to keep track of which class configs have already been added and at which position in the allocationResult.Devices.Config.
		// 记录每个设备类配置在结果切片中的起止位置，避免重复添加。
		type configRange struct{ start, end int }
		configIndexesForClass := make(map[string]configRange) // Key: class name / Value: position of the configs for the class in allocationResult.Devices.Config.
		// 遍历请求，定位其最终选中的类（子请求需取被选中的子请求的类）。
		for requestIndex := range claim.Spec.Devices.Requests {
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			requestData := alloc.requestData[requestKey]
			// 若请求来自子请求，先定位被选中的子请求以获得正确的类。
			if requestData.parentRequest != nil {
				// We need the class of the selected subrequest.
				requestKey.subRequestIndex = requestData.selectedSubRequestIndex
				requestData = alloc.requestData[requestKey]
			}

			class := requestData.class
			if class == nil {
				continue
			}
			// 若该类的配置已添加，只需把当前请求名追加到已有配置中。
			configIndexes, exists := configIndexesForClass[class.Name]
			if exists {
				// The configs for the class have already been added.
				// Just append the request name for the request class.
				for i := configIndexes.start; i < configIndexes.end; i++ {
					allocationResult.Devices.Config[i].Requests = append(allocationResult.Devices.Config[i].Requests, requestData.requestName())
				}
				continue
			}

			// Add all configs for the class once.
			// 首次遇到该类：把类的全部配置加入结果，并记录其位置区间。
			initialConfigLen := len(allocationResult.Devices.Config)
			for _, config := range class.Spec.Config {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClass,
					Requests:            []string{requestData.requestName()},
					DeviceConfiguration: config.DeviceConfiguration,
				})
			}
			configIndexesForClass[class.Name] = configRange{start: initialConfigLen, end: len(allocationResult.Devices.Config)}
		}
		// 处理声明级别的配置：Requests 为空表示应用于全部请求；非空则只对命中的请求生效。
		for _, config := range claim.Spec.Devices.Config {
			// If Requests are empty, it applies to all. So it can just be included.
			// 配置未指定 Requests，表示应用于所有请求，直接加入。
			if len(config.Requests) == 0 {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClaim,
					Requests:            config.Requests,
					DeviceConfiguration: config.DeviceConfiguration,
				})
				continue
			}

			// 配置指定了请求名：遍历请求（含子请求的全名）判断是否命中。
			for i, request := range claim.Spec.Devices.Requests {
				if slices.Contains(config.Requests, request.Name) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					break // We can stop checking after the first match as all requests in the config should be added.
				}

				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: i}
				requestData := alloc.requestData[requestKey]
				if requestData.parentRequest == nil {
					continue
				}

				// 对子请求使用“父请求/子请求”的全名进行匹配。
				subRequest := request.FirstAvailable[requestData.selectedSubRequestIndex]
				subRequestName := fmt.Sprintf("%s/%s", request.Name, subRequest.Name)
				if slices.Contains(config.Requests, subRequestName) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					break // We can stop checking after the first match as all requests in the config should be added.
				}
			}
		}

		// 若某配置实际应用于全部请求，清空其 Requests 以利用“空表示全部”的语义。
		// If a config applies to all requests, clear its Requests
		// field to take advantage of the "empty means all" semantic.
		for i := range allocationResult.Devices.Config {
			requests := allocationResult.Devices.Config[i].Requests
			if len(requests) == 0 {
				continue
			}
			appliesToAll := true
			for requestIndex, request := range claim.Spec.Devices.Requests {
				if slices.Contains(requests, request.Name) {
					continue
				}
				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
				requestData := alloc.requestData[requestKey]
				if requestData.parentRequest != nil {
					subRequest := request.FirstAvailable[requestData.selectedSubRequestIndex]
					subRequestName := fmt.Sprintf("%s/%s", request.Name, subRequest.Name)
					if slices.Contains(requests, subRequestName) {
						continue
					}
				}
				appliesToAll = false
				break
			}
			if appliesToAll {
				allocationResult.Devices.Config[i].Requests = nil
			}
		}

		// 第七步：根据所选设备的位置约束生成 NodeSelector。
		// Determine node selector.
		nodeSelector, err := alloc.createNodeSelector(internalResult.devices, node.Name)
		if err != nil {
			return nil, fmt.Errorf("create NodeSelector for claim %s: %w", claim.Name, err)
		}
		allocationResult.NodeSelector = nodeSelector
	}

	return result, nil
}

// GetStats 返回分配器的运行统计快照（当前为 allocateOne 的调用次数）。
func (a *Allocator) GetStats() Stats {
	s := Stats{
		NumAllocateOneInvocations: a.numAllocateOneInvocations.Load(),
	}
	return s
}

// validateDeviceRequest 校验单个请求（或子请求）的合法性：解析设备类、派生属性、所需设备数以及“all”模式的候选设备集合，并填充 requestData。
func (alloc *allocator) validateDeviceRequest(request requestAccessor, parentRequest requestAccessor, requestKey requestIndices, pools []*Pool) (requestData, error) {
	// 构造 requestData，记录请求索引与访问器（子请求同时记录父请求）。
	claim := alloc.claimsToAllocate[requestKey.claimIndex]
	requestData := requestData{
		indices:       requestKey,
		request:       request,
		parentRequest: parentRequest,
	}
	// 处理派生属性：请求声明了带 CEL 表达式的派生属性时，特性未启用则直接报错。
	derivedAttrs := request.derivedAttributes()
	if len(derivedAttrs) > 0 {
		// Explicitly reject if the feature gate is disabled to avoid silently
		// ignoring constraints. This can happen during a cluster upgrade if
		// the API server has the feature enabled but the scheduler does not.
		// Scheduling can succeed later if the scheduler is restarted with a
		// configuration that supports this feature.
		// 特性未启用时显式拒绝，避免静默忽略约束（例如集群升级导致服务端与调度器版本不一致）。
		if !alloc.features.DerivedAttributes {
			return requestData, fmt.Errorf("claim %s, request %s: cannot handle request with derived attributes, the feature is disabled", klog.KObj(claim), request.name())
		}
		// 将派生属性名映射到其 CEL 表达式，供后续属性解析使用。
		requestData.derivedAttributes = make(map[resourceapi.FullyQualifiedName]string, len(derivedAttrs))
		for _, attr := range derivedAttrs {
			requestData.derivedAttributes[attr.Name] = attr.Expression
		}
	}
	// 校验所有选择器均包含 CEL 表达式；未知选择器类型直接报错。
	for i, selector := range request.selectors() {
		if selector.CEL == nil {
			// Unknown future selector type!
			return requestData, fmt.Errorf("claim %s, request %s, selector #%d: CEL expression empty (unsupported selector type?)", klog.KObj(claim), request.name(), i)
		}
	}

	// 请求管理访问但特性未启用，报错。
	if !alloc.features.AdminAccess && request.hasAdminAccess() {
		return requestData, fmt.Errorf("claim %s, request %s: admin access is requested, but the feature is disabled", klog.KObj(claim), request.name())
	}

	// 请求必须指定设备类名，否则视为不支持的请求类型。
	// Should be set. If it isn't, something changed and we should refuse to proceed.
	if request.deviceClassName() == "" {
		return requestData, fmt.Errorf("claim %s, request %s: missing device class name (unsupported request type?)", klog.KObj(claim), request.name())
	}
	// 按类名获取 DeviceClass，失败则返回错误。
	class, err := alloc.classLister.Get(request.deviceClassName())
	if err != nil {
		return requestData, fmt.Errorf("claim %s, request %s: could not retrieve device class %s: %w", klog.KObj(claim), request.name(), request.deviceClassName(), err)
	}

	// Start collecting information about the request.
	// The class must be set and stored before calling isSelectable.
	// 必须先设置并保存 class，之后才能调用 isSelectable。
	requestData.class = class

	// 根据分配模式计算所需设备数：ExactCount 取显式数量，All 收集所有匹配设备。
	switch request.allocationMode() {
	// 精确数量模式：数量超出 int 上限时拒绝。
	case resourceapi.DeviceAllocationModeExactCount:
		numDevices := request.count()
		if numDevices > math.MaxInt {
			// Allowed by API validation, but doesn't make sense.
			return requestData, fmt.Errorf("claim %s, request %s: exact count %d is too large", klog.KObj(claim), request.name(), numDevices)
		}
		requestData.numDevices = int(numDevices)
	// “all”模式：遍历所有池中的 slice，收集所有可选且静态容量充足的设备。
	case resourceapi.DeviceAllocationModeAll:
		// If we have any any request that wants "all" devices, we need to
		// figure out how much "all" is. If some pool is incomplete, we stop
		// here because allocation cannot succeed. Once we do scoring, we should
		// stop in all cases, not just when "all" devices are needed, because
		// pulling from an incomplete might not pick the best solution and it's
		// better to wait. This does not matter yet as long the incomplete pool
		// has some matching device.
		// 预分配 allDevices 切片，并复用一个空的已消耗容量对象用于静态容量检查。
		requestData.allDevices = make([]deviceWithID, 0, resourceapi.AllocationResultsMaxSize)
		emptyConsumedCapacity := NewConsumedCapacity() // reusable: CmpRequestOverCapacity clones/reads only
		// 遍历每个资源池；池正在更新或无效时不能用于“all”请求。
		for _, pool := range pools {
			// 池不完整或无效时无法可靠确定“所有设备”，直接报错。
			if pool.IsIncomplete {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently being updated", klog.KObj(claim), request.name(), pool.PoolID)
			}
			if pool.IsInvalid {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently invalid", klog.KObj(claim), request.name(), pool.PoolID)
			}
			// 遍历池中 targeting 本节点的 slice 及其设备，筛选可选设备。
			for _, slice := range pool.DeviceSlicesTargetingNode {
				for deviceIndex := range slice.Spec.Devices {
					selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
					if err != nil {
						return requestData, err
					}
					if selectable {
						device := deviceWithID{
							id:     DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name},
							Device: &slice.Spec.Devices[deviceIndex],
							slice:  slice,
							pool:   pool,
						}
						// 若启用容量特性，先做静态容量检查；剩余容量在 allocateDevice 中还会复核。
						if alloc.features.ConsumableCapacity {
							// Static capacity only: remaining capacity is checked in allocateDevice
							// so capacity-blocked matching devices stay in allDevices and All fails.
							apiDevice := slice.Spec.Devices[deviceIndex]
							success, err := CmpRequestOverCapacity(emptyConsumedCapacity, requestData.request.capacities(),
								apiDevice.AllowMultipleAllocations, apiDevice.Capacity, emptyConsumedCapacity, alloc.features.FractionalCapacityRange)
							if err != nil {
								return requestData, fmt.Errorf("claim %s, request %s: checking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), apiDevice.Name, err)
							}
							// 设备静态容量不足，跳过。
							if !success {
								alloc.logger.V(7).Info("Device static capacity insufficient", "device", apiDevice)
								continue
							}
						}
						// 记录该设备为“all”请求的候选设备。
						requestData.allDevices = append(requestData.allDevices, device)
					}
				}
			}
		}
		// “all”请求所需设备数即筛选出的候选设备总数。
		requestData.numDevices = len(requestData.allDevices)
		alloc.logger.V(6).Info("Request for 'all' devices", "claim", klog.KObj(claim), "request", request.name(), "numDevicesPerRequest", requestData.numDevices)
	// 未知分配模式，报错。
	default:
		return requestData, fmt.Errorf("claim %s, request %s: unsupported count mode %s", klog.KObj(claim), request.name(), request.allocationMode())
	}
	return requestData, nil
}

// errStop is a special error that gets returned by allocateOne if it detects
// that allocation cannot succeed.
// errStop 是 allocateOne 在检测到分配无法成功时返回的特殊错误；Allocate 收到它时返回空结果。
var errStop = errors.New("stop allocation")

// errAllocationResultMaxSizeExceeded is a special error that gets return by
// allocatedOne when the number of allocated devices exceeds the max number
// allowed. This is checked by earlier invocations in the recursion and used
// to do more aggressive backtracking and avoid attempting allocations that
// we know can not succeed.
// errAllocationResultMaxSizeExceeded 表示已分配设备数超过上限；调用方据此更积极地回溯，避免尝试注定失败的路径。
var errAllocationResultMaxSizeExceeded = errors.New("allocation max size exceeded")

// allocator is used while an [Allocator.Allocate] is running. Only a single
// goroutine works with it, so there is no need for locking.
// allocator 是单次 [Allocator.Allocate] 执行期间使用的状态容器；仅由单个 goroutine 访问，无需加锁。
// 字段：ctx 为本次分配的上下文；logger 为日志器；node 为目标节点；claimsToAllocate 为待分配声明。
// pools 为合格资源池；deviceMatchesRequest 缓存设备/请求匹配结果；derivedAttributesCache 缓存派生属性求值结果。
// constraints 为每个声明保存约束；consumedCounters 记录各池已消耗计数器；consumedCompatibilityGroups 记录兼容性组滚动交集。
// requestData 为每个请求/子请求保存条目；allocatingDevices 记录本次尝试将新分配的设备；allocatingCapacity 记录将新增的容量消耗；result 保存各声明的内部结果。
type allocator struct {
	*Allocator
	ctx                  context.Context
	logger               klog.Logger
	node                 *v1.Node
	claimsToAllocate     []*resourceapi.ResourceClaim
	pools                []*Pool
	deviceMatchesRequest map[matchKey]bool
	// derivedAttributesCache caches evaluated CEL derived attributes for a
	// device. It is keyed by deviceExprKey (DeviceID + exact CEL expression)
	// to avoid re-evaluating identical expressions on the same device,
	// regardless of which derived attribute name or subrequest uses it.
	// derivedAttributesCache 以（设备 ID + 精确 CEL 表达式）为键缓存派生属性求值结果，避免同一设备上重复求值。
	derivedAttributesCache map[deviceExprKey]*resourceapi.DeviceAttribute
	constraints            [][]constraint // one list of constraints per claim
	// consumedCounters keeps track of the counters consumed by all devices
	// that are in the process of being allocated.
	// The keys in the map are resource pool IDs (driver name and pool name).
	// consumedCounters 记录当前正在分配过程中各资源池已被消耗的计数器，按池 ID 索引。
	consumedCounters map[PoolID]counterSets
	// consumedCompatibilityGroups tracks the rolling compatibility-group
	// intersection per counter set for each pool across the devices being
	// allocated in the current attempt. It is seeded from the already-allocated
	// peers (compatibilityGroupsBaseline) the first time a counter set is touched.
	// pool ID -> counter set name -> running intersection.
	// consumedCompatibilityGroups 记录当前分配尝试中各池、各计数器集的兼容性组滚动交集；首次触及某计数器集时以已分配设备的基线为种子。格式为：池 ID -> 计数器集名 -> 交集。
	consumedCompatibilityGroups map[PoolID]map[string]compatibilityGroupIntersection
	requestData                 map[requestIndices]requestData // one entry per request with no subrequests and one entry per subrequest
	// allocatingDevices tracks which devices will be newly allocated for a
	// particular attempt to find a solution. The map is indexed by device
	// and its values represent for which of a pod's claims the device will
	// be allocated.
	// Claims are identified by their index in claimsToAllocate.
	// allocatingDevices 记录本次求解尝试中哪些设备将被新分配；键为设备 ID，值为使用该设备的声明索引集合。
	allocatingDevices map[DeviceID]sets.Set[int]
	// allocatingCapacity tracks the amount of device capacity that will be newly allocated
	// for a particular attempt to find a solution.
	// The map is indexed by device ID, and each value represents the accumulated capacity
	// requested by all allocations targeting that device.
	// allocatingCapacity 记录本次求解尝试中各设备将新增的容量消耗。
	allocatingCapacity ConsumedCapacityCollection
	result             []internalAllocationResult
}

// counterSets is a map with the name of counter sets to the counters in
// the set.
// counterSets 描述计数器集合名到该集合内计数器（按计数器名索引）的映射。
type counterSets map[draapi.UniqueString]map[string]resourceapi.Counter

// matchKey identifies a device/request pair.
// matchKey 由设备 ID 与请求索引组成，唯一标识一个待判定的设备/请求对。
type matchKey struct {
	DeviceID
	requestIndices
}

// deviceExprKey identifies a specific CEL expression evaluated on a specific device.
// This allows caching the evaluated derived attribute so that identical CEL expressions
// are evaluated at most once per device, regardless of how many derived attributes or
// constraints reference them.
// deviceExprKey 由设备 ID 与 CEL 表达式字符串组成，用于缓存派生属性求值结果。
type deviceExprKey struct {
	DeviceID
	expr string
}

// requestIndices identifies one specific request
// or subrequest by three properties:
//
// - claimIndex: The index of the claim in the requestData map.
// - requestIndex: The index of the request in the claim.
// - subRequestIndex: The index of the subrequest in the parent request.
// requestIndices 通过声明索引、请求索引、子请求索引三元组唯一定位一个请求或子请求。
type requestIndices struct {
	claimIndex, requestIndex int
	subRequestIndex          int
}

// deviceIndices identifies one specific required device inside
// a request or subrequest of a certain claim.
// deviceIndices 在某个声明的请求（或子请求）内唯一定位所需的某一台设备。
// 字段：claimIndex 为声明在分配器中的索引；requestIndex 为请求在声明中的索引。
// subRequestIndex 为子请求在请求中的索引（非子请求时忽略）；deviceIndex 为该设备在该请求/子请求中的序号。
type deviceIndices struct {
	claimIndex      int // The index of the claim in the allocator.
	requestIndex    int // The index of the request in the claim.
	subRequestIndex int // The index of the subrequest within the request (ignored if subRequest is false).
	deviceIndex     int // The index of a device within a request or subrequest.
}

// deviceLocation identifies a device by its position in the allocator's pools.
// deviceLocation 用池、切片、设备三级下标定位设备在分配器池列表中的位置。
// 字段：poolIndex 为池在 pools 中的下标；sliceIndex 为切片在池的 DeviceSlicesTargetingNode 中的下标；deviceIndex 为设备在切片 Spec.Devices 中的下标。
type deviceLocation struct {
	poolIndex   int
	sliceIndex  int
	deviceIndex int
}

// requestData 保存单个请求（或子请求）在分配过程中的静态信息与决策结果。
// 字段：indices 为该请求的 requestIndices；request 为请求/子请求访问器（永不为 nil）；parentRequest 为子请求的父请求（非子请求为 nil）。
// class 为解析出的设备类；numDevices 为所需设备数（“all”模式为候选设备总数）；selectedSubRequestIndex 记录被选中的子请求下标。
// allDevices 为“all”模式预先确定的候选设备集合；derivedAttributes 将派生属性名映射到其 CEL 表达式。
type requestData struct {
	// indices holds the requestIndices for this request
	indices requestIndices
	// The request or subrequest which needs to be allocated.
	// Never nil.
	request requestAccessor
	// The parent of a subrequest, nil if not a subrequest.
	parentRequest requestAccessor
	class         *resourceapi.DeviceClass
	numDevices    int

	// selectedSubRequestIndex is set for the entry with requestIndices.subRequestIndex == 0.
	// It is the index of the subrequest which got picked during allocation.
	selectedSubRequestIndex int

	// pre-determined set of devices for allocating "all" devices
	allDevices []deviceWithID

	// derivedAttributes maps derived attribute name to its CEL expression
	derivedAttributes map[resourceapi.FullyQualifiedName]string
}

// requestName 返回请求名；子请求返回“父请求名/子请求名”的全名。
func (rd *requestData) requestName() string {
	if rd.parentRequest != nil {
		return fmt.Sprintf("%s/%s", rd.parentRequest.name(), rd.request.name())
	}
	return rd.request.name()
}

// deviceWithID 把设备 API 对象与其 ID、所属 slice 和池打包在一起，便于分配过程中传递。
// 字段：内嵌 *draapi.Device 提供设备定义；id 为唯一标识；slice 为发布该设备的 ResourceSlice；pool 为所属资源池。
type deviceWithID struct {
	*draapi.Device
	id    DeviceID
	slice *draapi.ResourceSlice
	pool  *Pool
}

// internalAllocationResult 保存单个声明的内部分配结果（尚未转换为 API 结构）；devices 为该声明已分配的设备列表。
type internalAllocationResult struct {
	devices []internalDeviceResult
}

// internalDeviceResult 保存单台已分配设备的内部结果信息。
// 字段：内嵌 *draapi.Device 提供设备定义（含绑定条件）；request 为请求名或子请求名；parentRequest 为父请求名（非子请求为空）。
// id 为设备标识；shareID 为共享份额 ID（非共享为 nil）；slice 为所属 ResourceSlice；consumedCapacity 为该份额消耗的容量；adminAccess 标记是否请求管理访问。
type internalDeviceResult struct {
	*draapi.Device
	request          string // name of the request (if no subrequests) or the subrequest
	parentRequest    string // name of the request which contains the subrequest, empty otherwise
	id               DeviceID
	shareID          *types.UID
	slice            *draapi.ResourceSlice
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity
	adminAccess      *bool
}

// requestName 返回该结果对应的请求名；子请求返回“父/子”全名。
func (idr internalDeviceResult) requestName() string {
	if idr.parentRequest == "" {
		return idr.request
	}
	return fmt.Sprintf("%s/%s", idr.parentRequest, idr.request)
}

// lookupRequest 根据结果中的请求名，在声明中查找对应的请求访问器（含子请求）；未找到返回 nil。
func (idr internalDeviceResult) lookupRequest(claim *resourceapi.ResourceClaim) requestAccessor {
	// 子请求使用父请求名定位外层请求，否则使用请求名本身。
	requestName := idr.request
	if idr.parentRequest != "" {
		requestName = idr.parentRequest
	}
	// 遍历声明的请求，找到匹配项后返回对应访问器。
	for i := range claim.Spec.Devices.Requests {
		request := &claim.Spec.Devices.Requests[i]
		if request.Name != requestName {
			continue
		}
		// 无父请求时无需检查子请求，直接返回精确请求访问器。
		if idr.parentRequest == "" {
			// No need to check sub-requests.
			return &exactDeviceRequestAccessor{request}
		}
		// 有父请求时继续在 FirstAvailable 中查找同名子请求。
		for j := range request.FirstAvailable {
			subRequest := &request.FirstAvailable[j]
			if subRequest.Name != idr.request {
				continue
			}
			return &deviceSubRequestAccessor{subRequest}
		}
	}
	// 未找到匹配请求，返回 nil。
	return nil
}

// constraint 定义声明级约束的行为：add 在分配设备时校验并记录状态，remove 撤销对应状态。
type constraint interface {
	// add is called whenever a device is about to be allocated. It must
	// check whether the device matches the constraint and if yes,
	// track that it is allocated.
	// add 在设备即将被分配时调用：检查设备是否满足约束，满足则记录该设备。
	add(request *requestData, device *draapi.Device, deviceID DeviceID) (bool, error)

	// For every successful add there is exactly one matching removed call
	// with the exact same parameters.
	// remove 撤销一次成功的 add；调用方保证参数与对应的 add 完全一致。
	remove(request *requestData, device *draapi.Device, deviceID DeviceID)
}

// deviceAttributeListAsSet is set-based representation of DeviceAttributeListType.
// This has the same structure as DeviceAttributeListType, but uses sets.Set instead of slices.
// deviceAttributeListAsSet 是列表类型设备属性的集合表示，结构与列表属性相同但使用 sets.Set 存储。
// 字段：intValue、boolValue、stringValue、versionValue 分别保存对应类型的取值集合。
type deviceAttributeListAsSet struct {
	intValue     sets.Set[int64]
	boolValue    sets.Set[bool]
	stringValue  sets.Set[string]
	versionValue sets.Set[string]
}

// intersection returns the new intersection set of two attribute sets as a new set.
// Returns nil if there is no intersection or if the types do not match.
// intersection 返回两个属性集合的交集；无交集或类型不匹配时返回 nil。
func (s *deviceAttributeListAsSet) intersection(other *deviceAttributeListAsSet) *deviceAttributeListAsSet {
	result := &deviceAttributeListAsSet{}

	// 仅当两侧类型一致时才求交；每个分支求交后若为空则返回 nil，否则保存交集。
	switch {
	case s.intValue != nil && other.intValue != nil:
		intersection := s.intValue.Intersection(other.intValue)
		if intersection.Len() == 0 {
			return nil
		}
		result.intValue = intersection
		return result
	case s.boolValue != nil && other.boolValue != nil:
		intersection := s.boolValue.Intersection(other.boolValue)
		if intersection.Len() == 0 {
			return nil
		}
		result.boolValue = intersection
		return result
	case s.stringValue != nil && other.stringValue != nil:
		intersection := s.stringValue.Intersection(other.stringValue)
		if intersection.Len() == 0 {
			return nil
		}
		result.stringValue = intersection
		return result
	case s.versionValue != nil && other.versionValue != nil:
		intersection := s.versionValue.Intersection(other.versionValue)
		if intersection.Len() == 0 {
			return nil
		}
		result.versionValue = intersection
		return result
	// 其它情况（类型不匹配）返回 nil。
	default:
		// Type mismatch
		return nil
	}
}

// attributeAsSet creates deviceAttributeListAsSet from DeviceAttribute.
// Both scalar and list attributes are converted to set representation.
// For scalar values, creates a single-element set.
// For list values, creates a set from all elements.
// Returns nil if the attribute type is unknown or empty.
// attributeAsSet 将设备属性转换为集合表示：列表属性取全部元素，标量属性转成单元素集合；类型未知或为空时返回 nil。
func attributeAsSet(attribute *resourceapi.DeviceAttribute) *deviceAttributeListAsSet {
	if attribute == nil {
		return nil
	}

	result := &deviceAttributeListAsSet{}

	// 优先处理列表值，再处理标量值。
	switch {
	case len(attribute.IntValues) > 0:
		result.intValue = sets.New(attribute.IntValues...)
		return result
	case len(attribute.BoolValues) > 0:
		result.boolValue = sets.New(attribute.BoolValues...)
		return result
	case len(attribute.StringValues) > 0:
		result.stringValue = sets.New(attribute.StringValues...)
		return result
	case len(attribute.VersionValues) > 0:
		result.versionValue = sets.New(attribute.VersionValues...)
		return result
	case attribute.IntValue != nil:
		result.intValue = sets.New(*attribute.IntValue)
		return result
	case attribute.BoolValue != nil:
		result.boolValue = sets.New(*attribute.BoolValue)
		return result
	case attribute.StringValue != nil:
		result.stringValue = sets.New(*attribute.StringValue)
		return result
	case attribute.VersionValue != nil:
		result.versionValue = sets.New(*attribute.VersionValue)
		return result
	// 未知属性类型，返回 nil。
	default:
		return nil
	}
}

// matchAttributeConstraint compares an attribute value across devices.
// All devices must share the same value. When the set of devices is
// empty, any device that has the attribute can be added. After that,
// only matching devices can be added.
//
// We don't need to track *which* devices are part of the set, only
// how many.
// matchAttributeConstraint 要求所有相关设备共享同一属性值；列表属性按集合交集非空判断，标量按精确相等判断。
// 字段：logger 为预置日志器；requestNames 为受影响的请求名集合（空表示全部）；attributeName 为属性全名。
// features 决定比较语义；attributeProvider 用于解析属性值；attribute 为标量模式的基准值。
// intersectionStack 保存列表模式下逐步收窄的交集（add 入栈、remove 出栈，回溯自动恢复）；numDevices 为已接纳设备数。
type matchAttributeConstraint struct {
	logger            klog.Logger // Includes name and attribute name, so no need to repeat in log messages.
	requestNames      sets.Set[string]
	attributeName     resourceapi.FullyQualifiedName
	features          Features
	attributeProvider attributeProvider

	// For scalar values (existing behavior)
	attribute *resourceapi.DeviceAttribute

	// For list values (when DRAListTypeAttributes feature gate is enabled).
	// intersectionStack[len-1] is the current intersection; each add() pushes and each remove() pops,
	// so backtracking always restores the intersection to its pre-add state.
	intersectionStack []*deviceAttributeListAsSet

	numDevices int
}

// add 检查设备是否满足“属性匹配”约束：首台设备确定基准，后续设备须与基准一致；满足则更新状态并返回成功。
func (m *matchAttributeConstraint) add(request *requestData, device *draapi.Device, deviceID DeviceID) (bool, error) {
	// 不受约束影响的请求，直接接纳该设备。
	if m.requestNames.Len() > 0 && !m.matches(request) {
		// Device not affected by constraint.
		m.logger.V(7).Info("Constraint does not apply to request", "request", request.requestName())
		return true, nil
	}

	// 解析属性值；解析失败直接返回错误。
	attribute, err := m.attributeProvider.lookupAttribute(request, device, deviceID, m.attributeName)
	if err != nil {
		return false, err
	}
	// 设备没有该属性，不满足约束。
	if attribute == nil {
		// Doesn't have the attribute.
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false, nil
	}

	// 第一台设备可直接入选，并据此初始化标量基准或列表交集。
	if m.numDevices == 0 {
		// The first device can always get picked.
		// Initialize either scalar attribute or list set based on the attribute type.
		// 列表模式：把首个属性转成集合并压栈作为初始交集。
		if m.features.ListTypeAttributes {
			// Convert attribute to set representation (both scalar and list)
			// 属性类型未知，无法转成集合，拒绝。
			first := attributeAsSet(attribute)
			if first == nil {
				m.logger.V(7).Info("Attribute type unknown")
				return false, nil
			}
			m.intersectionStack = append(m.intersectionStack, first)
		} else {
			// Scalar attribute: use existing behavior
			// 标量模式：直接记录首个属性值作为基准。
			m.attribute = attribute
		}
		m.numDevices = 1
		m.logger.V(7).Info("First in set")
		return true, nil
	}

	// 后续设备按集合或标量语义与基准比较。
	// Check if we are matching with set-based logic or scalar-based logic
	// 列表模式：与当前交集求交，交集为空则拒绝，否则压入收窄后的交集。
	if m.features.ListTypeAttributes {
		// Set-based matching: check if there is non-empty intersection
		// 属性类型未知，拒绝。
		newSet := attributeAsSet(attribute)
		if newSet == nil {
			m.logger.V(7).Info("Unknown attribute type")
			return false, nil
		}
		// 与当前交集无公共元素，拒绝。
		current := m.intersectionStack[m.numDevices-1]
		narrowed := current.intersection(newSet)
		if narrowed == nil {
			m.logger.V(7).Info("Attribute values have no common elements")
			return false, nil
		}
		// 交集非空：压栈保存收窄结果，便于回溯恢复。
		// Push a narrowed copy so remove() can pop back to the previous intersection.
		m.intersectionStack = append(m.intersectionStack, narrowed)
	} else {
		// 标量模式：按属性值类型逐一比较，与基准不同则拒绝。
		// Scalar matching: use existing behavior
		switch {
		case attribute.StringValue != nil:
			if m.attribute.StringValue == nil || *attribute.StringValue != *m.attribute.StringValue {
				m.logger.V(7).Info("String values different")
				return false, nil
			}
		case attribute.IntValue != nil:
			if m.attribute.IntValue == nil || *attribute.IntValue != *m.attribute.IntValue {
				m.logger.V(7).Info("Int values different")
				return false, nil
			}
		case attribute.BoolValue != nil:
			if m.attribute.BoolValue == nil || *attribute.BoolValue != *m.attribute.BoolValue {
				m.logger.V(7).Info("Bool values different")
				return false, nil
			}
		case attribute.VersionValue != nil:
			// semver 2.0.0 requires that version strings are in their
			// minimal form (in particular, no leading zeros). Therefore a
			// strict "exact equal" check can do a string comparison.
			if m.attribute.VersionValue == nil || *attribute.VersionValue != *m.attribute.VersionValue {
				m.logger.V(7).Info("Version values different")
				return false, nil
			}
		// 未知属性类型，拒绝。
		default:
			// Unknown value type, cannot match.
			m.logger.V(7).Info("Unknown attribute type")
			return false, nil
		}
	}

	// 满足约束，更新计数并返回成功。
	m.numDevices++
	m.logger.V(7).Info("Constraint satisfied by device", "device", deviceID, "numDevices", m.numDevices)
	return true, nil
}

// remove 撤销 add 的状态更新：设备计数减一，列表模式下弹出栈顶交集。
func (m *matchAttributeConstraint) remove(request *requestData, device *draapi.Device, deviceID DeviceID) {
	// 不受约束影响的请求无需回滚。
	if m.requestNames.Len() > 0 && !m.matches(request) {
		// Device not affected by constraint.
		return
	}

	// 计数减一；列表模式下同步弹出栈顶交集以恢复上一状态。
	m.numDevices--
	if m.features.ListTypeAttributes {
		// Pop the current intersection; the previous entry on the stack becomes current or the stack is empty.
		m.intersectionStack = m.intersectionStack[:m.numDevices]
	}
	m.logger.V(7).Info("Device removed from constraint set", "device", deviceID, "numDevices", m.numDevices)
}

// matches 判断请求是否受约束影响：子请求匹配父请求名或“父/子”全名。
func (m *matchAttributeConstraint) matches(request *requestData) bool {
	if request.parentRequest != nil {
		requestName := request.parentRequest.name()
		subRequestName := request.request.name()
		fullSubRequestName := fmt.Sprintf("%s/%s", requestName, subRequestName)
		return m.requestNames.Has(requestName) || m.requestNames.Has(fullSubRequestName)
	} else {
		return m.requestNames.Has(request.request.name())
	}
}

// lookupStaticAttribute performs a lookup of a static device attribute
// published directly by the driver on the candidate Device object.
// It handles fully-qualified domain name matching and fallback to
// unqualified attribute names if the driver domain matches.
// lookupStaticAttribute 从设备发布的静态属性中按全名查找；若属性域名与驱动名一致，也支持用非限定名查找。
func lookupStaticAttribute(device *draapi.Device, deviceID DeviceID, attributeName resourceapi.FullyQualifiedName) *resourceapi.DeviceAttribute {
	// 先按完全限定名精确匹配。
	// Fully-qualified match?
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName)]; ok {
		return &attr
	}
	// 查找域名分隔符；合法全名不应缺失该分隔符。
	index := strings.Index(string(attributeName), "/")
	if index < 0 {
		// Should not happen for a valid fully qualified name.
		return nil
	}

	// 域名与驱动名不一致且未精确命中，则属性不可用。
	if string(attributeName[0:index]) != deviceID.Driver.String() {
		// Not an attribute of the driver and not found above,
		// so it is not available.
		return nil
	}

	// 域名与驱动一致，用去掉域名后的 ID 再查一次。
	// Domain matches the driver, so let's check just the ID.
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName[index+1:])]; ok {
		return &attr
	}

	// 仍未找到，返回 nil。
	return nil
}

// lookupAttribute is the unified entry point for attribute resolution
// in the experimental allocator. It implements the lookup precedence:
//  1. If the attribute is defined as a derived attribute on the request,
//     it compiles/evaluates the CEL expression, caches it in the
//     request scope, and returns the synthesized virtual attribute.
//  2. Otherwise, it falls back to looking up the static attribute on
//     the Device object.
//
// lookupAttribute 是属性解析的统一入口：优先解析请求声明的派生属性（编译并求值 CEL，结果按设备与表达式缓存），否则回退到静态属性查找。
func (alloc *allocator) lookupAttribute(request *requestData, device *draapi.Device, deviceID DeviceID, attributeName resourceapi.FullyQualifiedName) (*resourceapi.DeviceAttribute, error) {
	// 若该属性在请求中被声明为派生属性，走 CEL 求值路径。
	if targetExpr, found := request.derivedAttributes[attributeName]; found {
		// 以（设备 ID + 表达式）为键查缓存，命中则直接返回。
		cacheKey := deviceExprKey{DeviceID: deviceID, expr: targetExpr}
		if attr, found := alloc.derivedAttributesCache[cacheKey]; found {
			return attr, nil
		}

		// 按需编译派生属性表达式，编译失败返回错误。
		expr := alloc.celCache.GetOrCompileDerivedAttribute(targetExpr)
		if expr.Error != nil {
			return nil, fmt.Errorf("compile derived attribute %s failed: %w", attributeName, expr.Error)
		}

		// 在设备上下文上求值派生属性，失败则返回错误。
		evaluatedAttr, _, err := expr.EvaluateDerivedAttribute(alloc.ctx, cel.Device{
			Driver:                   deviceID.Driver.String(),
			AllowMultipleAllocations: device.AllowMultipleAllocations,
			Attributes:               device.Attributes,
			Capacity:                 device.Capacity,
		})
		if err != nil {
			return nil, fmt.Errorf("evaluate derived attribute %s on device %s failed: %w", attributeName, deviceID, err)
		}

		// 缓存求值结果并返回。
		alloc.derivedAttributesCache[cacheKey] = evaluatedAttr
		return evaluatedAttr, nil
	}

	// 非派生属性：回退到静态属性查找。
	return lookupStaticAttribute(device, deviceID, attributeName), nil
}

// allocateOne iterates over all eligible devices (not in use, match selector,
// satisfy constraints) for a specific required device. It returns true if
// everything got allocated, an error if allocation needs to stop.
//
// allocateSubRequest is true when trying to allocate one particular subrequest.
// This allows the logic for subrequests to call allocateOne with the same
// device index without causing infinite recursion.
//
// startLocation provides the starting point when searching for the next device to
// allocate. Since we have a list of pools, each with a list of slices which again
// contains a list of devices, we have defined an order in which the devices will
// be attempted for allocation. Since the order of devices doesn't matter within a
// single request, we want to make sure we only attempt all combinations of devices,
// and not search through all permutations (since many of them will have the same set
// of devices and therefore be identical allocations). Note that the order of devices
// does matter across requests and claims. So by providing the startLocation, we
// make sure that the allocator only attempts allocations that follows the order
// of the available devices, thereby avoiding different permutations. For example,
// this means that for the devices [1, 2, 3], we will only attempt the following
// possible allocations [1], [2], [3], [1, 2], [1, 3], [2, 3], and [1, 2, 3].
//
// The null startLocation (= all indices zero) means that all devices are considered.
// The only situation where a non-null startLocation is used is when looking for the
// next device within the same request.
// allocateOne 为某个待分配设备遍历所有合格候选（未被占用、匹配选择器、满足约束），并递归为后续设备与请求求解；全部完成返回 true，确定无法完成返回错误或 false。
// startLocation 提供搜索起点，使同一请求内的设备按固定顺序枚举、避免重复排列；零值表示从所有设备开始。
func (alloc *allocator) allocateOne(r deviceIndices, allocateSubRequest bool, startLocation deviceLocation) (bool, error) {
	// 统计 allocateOne 的调用次数。
	alloc.numAllocateOneInvocations.Add(1)

	// 上下文已取消则中止分配并返回原因。
	if alloc.ctx.Err() != nil {
		return false, fmt.Errorf("filter operation aborted: %w", context.Cause(alloc.ctx))
	}

	// 所有声明均已处理完毕，找到完整解，成功返回。
	if r.claimIndex >= len(alloc.claimsToAllocate) {
		// Done! If we were doing scoring, we would compare the current allocation result
		// against the previous one, keep the best, and continue. Without scoring, we stop
		// and use the first solution.
		alloc.logger.V(6).Info("Allocation result found")
		return true, nil
	}

	// 取当前声明；若其请求已全部处理完，递归进入下一个声明。
	claim := alloc.claimsToAllocate[r.claimIndex]
	if r.requestIndex >= len(claim.Spec.Devices.Requests) {
		// Done with the claim, continue with the next one.
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex + 1}, false, deviceLocation{})
		// 超出数量上限不是致命错误：后续声明可能因选择不同的子请求而成功，故此处吞掉该错误。
		if errors.Is(err, errAllocationResultMaxSizeExceeded) {
			// We don't need to propagate this further because
			// this is not a fatal error. Retrying the claim under
			// different circumstances may succeed if it uses
			// subrequests and changing the allocation of some
			// prior claim enables allocating a subrequest here
			// which needs fewer devices.
			return false, nil
		}
		return success, err
	}

	// r.subRequestIndex 仅在递归链内非零；用当前索引取出 requestData。
	// r.subRequestIndex is zero unless the for loop below is in the
	// recursion chain.
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]

	// 子请求特殊处理：只需选中其中一个即可进入下一请求；首次遇到子请求时进入此分支。
	// Subrequests are special: we only need to allocate one of them, then
	// we can move on to the next request. We enter this for loop when
	// hitting the first subrequest, but not if we are already working on a
	// specific subrequest.
	// 尚未锁定具体子请求时，依次尝试每个子请求。
	if !allocateSubRequest && requestData.parentRequest != nil {
		// Keep track of whether all attempts to do allocation with the
		// subrequests results in the allocation result limit exceeded.
		// If so, there is no need to make attempts with other devices
		// in the previous request (if any), except when
		// it is a firstAvailable request where some sub-requests
		// need less devices than others.
		// 记录所有子请求是否都因超出数量上限而失败，以决定是否向上传递该错误。
		allAllocationExceeded := true
		// 按序尝试各子请求；索引超出范围说明全部尝试完毕。
		for subRequestIndex := 0; ; subRequestIndex++ {
			nextSubRequestKey := requestKey
			nextSubRequestKey.subRequestIndex = subRequestIndex
			// 已无更多子请求：若全部因超限失败则向上传播该错误，否则返回失败。
			if _, ok := alloc.requestData[nextSubRequestKey]; !ok {
				// Past the end of the subrequests without finding a solution -> give up.
				//
				// Return errAllocationResultMaxSizeExceeded if all
				// attempts for the subrequests failed to due to reaching
				// the max size limit. This would mean that there are no
				// solution that involves the previous request (if any).
				if allAllocationExceeded {
					return false, errAllocationResultMaxSizeExceeded
				}
				return false, nil
			}

			// 锁定当前子请求下标并递归分配（allocateSubRequest=true 防止无限递归）。
			r.subRequestIndex = subRequestIndex
			success, err := alloc.allocateOne(r, true /* prevent infinite recusion */, deviceLocation{})
			// If we reached the allocation result limit, we can try
			// with the next subrequest if there is one. It might request
			// fewer devices, so it might succeed.
			// 超限则尝试下一个子请求（它可能需要更少设备）。
			if errors.Is(err, errAllocationResultMaxSizeExceeded) {
				continue
			}
			// 非超限错误直接返回；同时标记并非全部子请求都因超限失败。
			// If we get here, at least one of the subrequests failed for a
			// different reason than errAllocationResultMaxSizeExceeded.
			allAllocationExceeded = false
			if err != nil {
				return false, err
			}
			// 子请求分配成功：记录被选中的子请求下标并返回成功。
			// If allocation with a subrequest succeeds, return without
			// attempting the remaining subrequests.
			if success {
				// Store the index of the selected subrequest
				requestData.selectedSubRequestIndex = subRequestIndex
				alloc.requestData[requestKey] = requestData
				return true, nil
			}
		}
		// This is unreachable, so no need to have a return statement here.
	}

	// 取当前请求访问器，并判断是否为“all”模式。
	// Look up the current request that we are attempting to satisfy. This can
	// be either a request or a subrequest.
	request := requestData.request
	doAllDevices := request.allocationMode() == resourceapi.DeviceAllocationModeAll

	// “all”模式但无候选设备，分配失败。
	// At least one device is required for 'All' allocation mode.
	if doAllDevices && len(requestData.allDevices) == 0 {
		alloc.logger.V(6).Info("Allocation for 'all' devices didn't succeed: no devices found", "claim", klog.KObj(claim), "request", requestData.request.name())
		return false, nil
	}

	// 当前请求所需设备已全部分配完，进入下一个请求；恢复 allocateSubRequest=false。
	// We already know how many devices per request are needed.
	if r.deviceIndex >= requestData.numDevices {
		// Done with request, continue with next one. We have completed the work for
		// the request or subrequest, so we can no longer be allocating devices for
		// a subrequest.
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex + 1}, false, deviceLocation{})
		// 向上传播超限错误：说明后续请求均无法在限制内分配，当前路径无解。
		// We want to propagate any errAllocationResultMaxSizeExceeded to the caller. If
		// that error is returned here, it means none of the requests/subrequests after this one
		// could be allocated while staying within the limit on the number of devices, so there
		// are no solution in the current request/subrequest that would work.
		return success, err
	}

	// 比较剩余需求与结果剩余容量，用比较而非相加防止溢出；超出则触发更积极的回溯。
	// Compare rather than add, so a large count cannot wrap the sum. deviceIndex
	// is below numDevices here, checked above, so remaining stays positive.
	remaining := requestData.numDevices - r.deviceIndex
	available := resourceapi.AllocationResultsMaxSize - len(alloc.result[r.claimIndex].devices)
	// 返回特殊错误，便于调用方识别并更积极地回溯。
	if remaining > available {
		// Return a special error so we can identify this situation in the
		// callers and do more aggressive backtracking.
		return false, errAllocationResultMaxSizeExceeded
	}

	alloc.logger.V(6).Info("Allocating one device", "currentClaim", r.claimIndex,
		"totalClaims", len(alloc.claimsToAllocate), "currentRequest", r.requestIndex,
		"currentSubRequest", r.subRequestIndex, "totalRequestsPerClaim", len(claim.Spec.Devices.Requests),
		"currentDevice", r.deviceIndex, "devicesPerRequest", requestData.numDevices, "allDevices", doAllDevices, "adminAccess", request.adminAccess(), "capacities", request.capacities())

	// “all”模式：候选设备已预先确定，只需逐个尝试占用。
	if doAllDevices {
		// For "all" devices we already know which ones we need. We
		// just need to check whether we can use them.
		// 尝试占用下一个候选设备；失败则不尝试其它组合，直接返回无解。
		deviceWithID := requestData.allDevices[r.deviceIndex]
		success, deallocate, err := alloc.allocateDevice(r, deviceWithID, true)
		if err != nil {
			return false, err
		}
		// “all”模式的设备顺序无关，某台不可用即整体无解。
		if !success {
			// The order in which we allocate "all" devices doesn't matter,
			// so we only try with the one which was up next. If we couldn't
			// get all of them, then there is no solution and we have to stop.
			return false, nil
		}
		// 递归分配下一台设备（只需一种组合，无需传递 startLocation）。
		// No need to propagate the startLocation here since we only try one combination of devices.
		done, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex, deviceIndex: r.deviceIndex + 1}, allocateSubRequest, deviceLocation{})
		// 后续失败或出错时回滚当前设备再返回。
		if err != nil || !done {
			// If we get an error or didn't complete, we need to backtrack. Depending
			// on the situation we might be able to retry, so we make sure we
			// deallocate.
			deallocate()
			return false, err
		}
		return done, nil
	}

	// 普通模式：按池、切片、设备的顺序遍历候选设备。
	// We need to find suitable devices.
	for poolIndex := startLocation.poolIndex; poolIndex < len(alloc.pools); poolIndex++ {
		pool := alloc.pools[poolIndex]
		// 跳过无效或不完整的池，但其它池中可能有可用设备，故不直接报错。
		// We don't allocate devices from invalid or incomplete pools, but
		// don't error out here since there might be available devices in other
		// pools.
		if pool.IsIncomplete || pool.IsInvalid {
			continue
		}
		// 仅当仍在起始池时才从 startLocation 指定的切片开始，否则从第一个切片开始。
		// We should start at the slice provided by startLocation unless we searched through
		// all devices in the previous pool and have started at the next pool. When this happens,
		//  we should start at the first slice in the pool.
		sliceStart := 0
		if poolIndex == startLocation.poolIndex {
			sliceStart = startLocation.sliceIndex
		}
		for sliceIndex := sliceStart; sliceIndex < len(pool.DeviceSlicesTargetingNode); sliceIndex++ {
			slice := pool.DeviceSlicesTargetingNode[sliceIndex]

			// 仅当仍在起始池与起始切片时才从 startLocation 指定的设备开始。
			// We should start at the device provided by startLocation unless we searched through
			// all devices in the previous slice and have started at the next slice. When this happens,
			//  we should start at the first device in the pool.
			deviceStart := 0
			if poolIndex == startLocation.poolIndex &&
				sliceIndex == startLocation.sliceIndex {
				deviceStart = startLocation.deviceIndex
			}
			// 遍历候选设备并构造其 ID。
			for deviceIndex := deviceStart; deviceIndex < len(slice.Spec.Devices); deviceIndex++ {
				deviceID := DeviceID{Driver: pool.Driver, Pool: pool.Pool, Device: slice.Spec.Devices[deviceIndex].Name}

				// 先做开销最小的“占用”检查：管理访问看同一声明内是否占用，普通请求看全局是否占用。
				// Checking for "in use" is cheap and thus gets done first.
				if request.adminAccess() && alloc.allocatingDeviceForClaim(deviceID, r.claimIndex) {
					alloc.logger.V(7).Info("Device in use in same claim", "device", deviceID)
					continue
				}
				if !request.adminAccess() && alloc.deviceInUse(deviceID) {
					alloc.logger.V(7).Info("Device in use", "device", deviceID)
					continue
				}

				// 再检查选择器匹配，结果带缓存。
				// Next check selectors.
				requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
				selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
				if err != nil {
					return false, err
				}
				if !selectable {
					alloc.logger.V(7).Info("Device not selectable", "device", deviceID)
					continue
				}
				// 校验容量是否满足请求。
				if alloc.features.ConsumableCapacity {
					// Next validate whether resource request over capacity
					device := slice.Spec.Devices[deviceIndex]
					success, err := alloc.CmpRequestOverCapacity(requestData.request, slice, device)
					if err != nil {
						return false, fmt.Errorf("claim %s, request %s: checking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), deviceID, err)
					}
					// 容量不足，跳过该设备。
					if !success {
						alloc.logger.V(7).Info("Device capacity not enough", "device", deviceID)
						continue
					}
				}

				// 通过前述检查后，构造候选设备并尝试占用（含计数器、约束等检查）。
				// Finally treat as allocated and move on to the next device.
				device := deviceWithID{
					id:     deviceID,
					Device: &slice.Spec.Devices[deviceIndex],
					slice:  slice,
					pool:   pool,
				}
				allocated, deallocate, err := alloc.allocateDevice(r, device, false)
				if err != nil {
					return false, err
				}
				// 被占用或违反约束，跳过。
				if !allocated {
					// In use or constraint violated...
					alloc.logger.V(7).Info("Device not usable", "device", deviceID)
					continue
				}
				// 构造下一台设备的索引与起始位置（从当前设备的下一个位置继续）。
				deviceKey := deviceIndices{
					claimIndex:      r.claimIndex,
					requestIndex:    r.requestIndex,
					subRequestIndex: r.subRequestIndex,
					deviceIndex:     r.deviceIndex + 1,
				}
				nextLocation := deviceLocation{
					poolIndex:   poolIndex,
					sliceIndex:  sliceIndex,
					deviceIndex: deviceIndex + 1,
				}
				// 递归分配同一请求的下一台设备；若找到解则立即返回。
				// This is the allocation attempt for the next device in the same request.
				// If allocateOne finds out that it is done with the request, it moves to
				// the next without setting a start location, so each request is free to try
				// all devices.
				done, err := alloc.allocateOne(deviceKey, allocateSubRequest, nextLocation)
				// If we found a solution, we can stop.
				if err == nil && done {
					return done, nil
				}

				// 未找到解：回滚临时占用，再继续尝试其它设备；若遇错误则向上返回。
				// Otherwise we didn't find a solution, and we need to deallocate
				// so the temporary allocation is correct for trying other devices.
				deallocate()

				if err != nil {
					// If we hit an error, we return. This might be that we reached
					// the allocation size limit, and if so, it will be caught further
					// up the stack and other subrequests will be attempted if there
					// are any.
					return false, err
				}
			}
		}
	}

	// 遍历完所有候选仍无解，返回失败。
	// If we get here without finding a solution, then there is none.
	return false, nil
}

// isSelectable checks whether a device satisfies the request and class selectors.
// isSelectable 检查设备是否满足请求的类选择器与请求选择器（含每设备节点选择），结果按（设备, 请求）缓存。
func (alloc *allocator) isSelectable(r requestIndices, requestData requestData, slice *draapi.ResourceSlice, deviceIndex int) (bool, error) {
	// 特性未启用时，带绑定条件的设备不可选。
	device := &slice.Spec.Devices[deviceIndex]
	if !alloc.features.DeviceBindingAndStatus &&
		len(device.BindingConditions) > 0 {
		// Devices with binding conditions are not supported, feature is off.
		return false, nil
	}

	// 先查匹配缓存，命中则直接返回。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name}
	matchKey := matchKey{DeviceID: deviceID, requestIndices: r}
	if matches, ok := alloc.deviceMatchesRequest[matchKey]; ok {
		// No need to check again.
		return matches, nil
	}

	// 先检查设备类上的选择器。
	if requestData.class != nil {
		match, err := alloc.selectorsMatch(r, device, deviceID, requestData.class, requestData.class.Spec.Selectors)
		if err != nil {
			return false, err
		}
		if !match {
			alloc.deviceMatchesRequest[matchKey] = false
			return false, nil
		}
	}

	// 再检查请求自身的选择器。
	request := requestData.request
	match, err := alloc.selectorsMatch(r, device, deviceID, nil, request.selectors())
	if err != nil {
		return false, err
	}
	if !match {
		alloc.deviceMatchesRequest[matchKey] = false
		return false, nil
	}

	// 若启用每设备节点选择，检查设备声明的节点/节点选择器是否匹配目标节点。
	if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
		matches, err := NodeMatches(alloc.node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
		if err != nil {
			return false, err
		}
		if !matches {
			alloc.deviceMatchesRequest[matchKey] = false
			return false, nil
		}
	}

	// 全部通过：缓存匹配结果并返回成功。
	alloc.deviceMatchesRequest[matchKey] = true
	return true, nil

}

// CmpRequestOverCapacity checks if the given device has sufficient remaining capacity
// to satisfy the resource request.
// Return true if success.
// CmpRequestOverCapacity 检查设备剩余容量（已考虑历史分配与本次正在分配的量）能否满足请求。
func (alloc *allocator) CmpRequestOverCapacity(request requestAccessor, slice *draapi.ResourceSlice, device draapi.Device) (bool, error) {
	// 取本次正在分配中该设备已累计消耗的容量，结合历史聚合容量进行比较。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: device.Name}
	allocatingCapacity := alloc.allocatingCapacity[deviceID]
	allowMultipleAllocations := device.AllowMultipleAllocations
	capacities := device.Capacity
	if allocatedCapacity, found := alloc.allocatedState.AggregatedCapacity[deviceID]; found {
		return CmpRequestOverCapacity(allocatedCapacity, request.capacities(), allowMultipleAllocations, capacities, allocatingCapacity, alloc.features.FractionalCapacityRange)
	}
	return CmpRequestOverCapacity(NewConsumedCapacity(), request.capacities(), allowMultipleAllocations, capacities, allocatingCapacity, alloc.features.FractionalCapacityRange)
}

// selectorsMatch 逐个求值 CEL 选择器：全部匹配返回 true；编译或运行时错误被包装为带类/请求信息的错误。
func (alloc *allocator) selectorsMatch(r requestIndices, device *draapi.Device, deviceID DeviceID, class *resourceapi.DeviceClass, selectors []resourceapi.DeviceSelector) (bool, error) {
	// 按需编译选择器表达式；编译失败通常意味着调度器版本落后于集群。
	for i, selector := range selectors {
		expr := alloc.celCache.GetOrCompile(selector.CEL.Expression)
		if expr.Error != nil {
			// Could happen if some future apiserver accepted some
			// future expression and then got downgraded. Normally
			// the "stored expression" mechanism prevents that, but
			// this code here might be more than one release older
			// than the cluster it runs in.
			// 编译错误根据是否来自设备类分别包装。
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d: CEL compile error: %w", class.Name, i, expr.Error)
			}
			return false, fmt.Errorf("claim %s: selector #%d: CEL compile error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, expr.Error)
		}

		// 将内部 Device 转换为 v1.Device 供 CEL 使用。
		// If this conversion turns out to be expensive, the CEL package could be converted
		// to use unique strings.
		var d resourceapi.Device
		if err := draapi.Convert_api_Device_To_v1_Device(device, &d, nil); err != nil {
			return false, fmt.Errorf("convert Device %s: %w", deviceID, err)
		}
		// 在设备上求值选择器，并记录 CEL 结果与开销。
		matches, details, err := expr.DeviceMatches(alloc.ctx, cel.Device{Driver: deviceID.Driver.String(), AllowMultipleAllocations: d.AllowMultipleAllocations, Attributes: d.Attributes, Capacity: d.Capacity})
		if class != nil {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "class", klog.KObj(class), "selector", i, "expression", selector.CEL.Expression, "matches", matches, "actualCost", ptr.Deref(details.ActualCost(), 0), "err", err)
		} else {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "claim", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), "selector", i, "expression", selector.CEL.Expression, "actualCost", ptr.Deref(details.ActualCost(), 0), "matches", matches, "err", err)
		}

		// 运行时错误经增强后包装返回。
		if err != nil {
			err = cel.EnhanceRuntimeError(err)
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d on device %s: CEL runtime error: %w", class.Name, i, deviceID, err)
			}
			return false, fmt.Errorf("claim %s: selector #%d on device %s: CEL runtime error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, deviceID, err)
		}
		// 任一选择器不匹配即返回 false。
		if !matches {
			return false, nil
		}
	}

	// 全部匹配，返回 true。
	// All of them match.
	return true, nil
}

// allocateDevice checks device availability and constraints for one
// candidate. The device must be selectable.
//
// r identifies the request for which allocation is attempted.
// The device is the candidate under consideration.
// If "must" is true, then it is okay (but not required) to return an error which describes in more detail why allocation wasn't possible.
// This is used when the caller wants exactly this device and will give up if it cannot allocate it.
//
// If that candidate works out okay, the shared state gets updated
// as if that candidate had been allocated. If allocation cannot continue later
// and must try something else, then the rollback function can be invoked to
// restore the previous state.
//
// The rollback function is only provided in case of a successful allocation
// (true and no error).
// allocateDevice 检查单个候选设备的可用性与约束；成功时更新共享状态并返回回滚闭包，失败时返回 false（must 为 true 时可返回详细错误）。调用方需保证设备已通过 isSelectable。
func (alloc *allocator) allocateDevice(r deviceIndices, device deviceWithID, must bool) (bool, func(), error) {
	// 取当前请求与请求数据，解析是否允许多次分配（共享）。
	claim := alloc.claimsToAllocate[r.claimIndex]
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]
	request := requestData.request
	allowMultipleAllocations := false
	if alloc.features.ConsumableCapacity {
		allowMultipleAllocations = device.AllowMultipleAllocations != nil && *device.AllowMultipleAllocations
	}
	// 不允许共享时，检查同一声明内或全局是否已被占用。
	if !allowMultipleAllocations && request.adminAccess() && alloc.allocatingDeviceForClaim(device.id, r.claimIndex) {
		alloc.logger.V(7).Info("Device in use in same claim", "device", device.id)
		return false, nil, nil
	}
	if !request.adminAccess() && alloc.deviceInUse(device.id) {
		alloc.logger.V(7).Info("Device in use", "device", device.id)
		return false, nil, nil
	}

	// 设备要求可选节点操作时，需特性启用且节点声明支持该能力。
	if len(device.slice.Spec.SkipNodeOperations) > 0 {
		if !alloc.features.OptionalNodeOperations {
			alloc.logger.V(7).Info("Device requires optional node operations, but the optional node operations feature is not enabled", "device", device.id)
			return false, nil, nil
		}
		if !slices.Contains(alloc.node.Status.DeclaredFeatures, draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate) {
			alloc.logger.V(7).Info("Device requires DRAOptionalNodeOperations but node lacks it", "device", device.id)
			return false, nil, nil
		}
	}

	// 消耗计数器的设备需要 partitionable devices 特性支持。
	// Devices that consume counters can not be allocated if the PartitionableDevices feature
	// is not enabled.
	if !alloc.features.PartitionableDevices && len(device.ConsumesCounters) > 0 {
		alloc.logger.V(7).Info("Device consumes counters, but the partitionable devices feature is not enabled", "device", device.id)
		return false, nil, nil
	}

	// 已在使用的可共享设备：其计数器已计入，不再重复计费。
	// Skip the counter check for an allow-multiple device in use: its counters are already
	// accounted for, so a further share must not charge them again.
	skipCounterCheck := allowMultipleAllocations && alloc.deviceCapacityInUse(device.id)

	// 记录父请求名，用于填充结果。
	var parentRequestName string
	if requestData.parentRequest != nil {
		parentRequestName = requestData.parentRequest.name()
	}

	// state records the mutations this call makes so rollbackDevice can undo
	// them. Every rejection path after the first mutation calls rollbackDevice
	// synchronously, and the success path returns a closure that calls the same
	// helper during backtracking, so both undo routes stay identical. Passing
	// the state by value to rollbackDevice keeps it (and the flags) on the
	// stack for the rejection paths; only the success closure escapes.
	// state 记录本次调用所做的全部状态变更，供拒绝路径同步回滚或成功路径返回的闭包在回溯时回滚。
	state := deviceRollbackState{
		previousNumResults: len(alloc.result[r.claimIndex].devices),
	}

	// 检查兼容性组约束：设备只能与已占用同一计数器集且兼容性组有交集的设备共同分配。
	// Enforce the DRADeviceCompatibilityGroups constraint: a device may only be
	// co-allocated with the other devices already drawing from a counter set when
	// their declared compatibility groups intersect. This is gated identically to
	// the counter check below so that additional shares of an already-allocated
	// device (skipCounterCheck) are not re-evaluated, and runs before it so that
	// a rejection returns before any counters have been reserved. While the
	// feature is disabled there is nothing to check: GatherPools already ignored
	// all slices with compatibility groups.
	// 启用特性且需要检查计数器时，先检查并消费兼容性组交集。
	if alloc.features.CompatibilityGroups && !skipCounterCheck && len(device.ConsumesCounters) > 0 {
		// 不兼容则直接拒绝（此时计数器尚未预留）。
		ok, restore := alloc.checkAndConsumeCompatibilityGroups(device)
		if !ok {
			alloc.logger.V(7).Info("Incompatible compatibility groups", "device", device.id)
			return false, nil, nil
		}
		// 保存回滚函数，供后续失败或回溯时恢复。
		state.restoreCompatibilityGroups = restore
	}

	// 校验计数器是否充足。
	// The API validation logic has checked the ConsumesCounters referred should exist inside SharedCounters.
	// state.countersReserved records whether checkAvailableCounters actually
	// reserved this device's counters. It is not the same as
	// len(device.ConsumesCounters) > 0, because skipCounterCheck can bypass the
	// reservation.
	// 需要检查时：验证计数器可用性，失败则回滚已消费的兼容性组并返回。
	if !skipCounterCheck && len(device.ConsumesCounters) > 0 {
		// If a device consumes counters from a counter set, verify that
		// there is sufficient counters available.
		ok, err := alloc.checkAvailableCounters(device)
		// 计数器不足或出错：同步回滚后返回。
		if err != nil || !ok {
			// Unwind the compatibility-group intersection consumed above now
			// that the device is rejected; nothing else has been mutated yet.
			alloc.rollbackDevice(r, device, &requestData, state)
			if err != nil {
				return false, nil, err
			}
			alloc.logger.V(7).Info("Insufficient counters", "device", device.id)
			return false, nil, nil
		}
		// 标记计数器已预留，便于回滚。
		state.countersReserved = true
	}

	// 检查设备污点是否被请求容忍；不被容忍则回滚并拒绝。
	// Might be tainted, in which case the taint has to be tolerated.
	// The check is skipped if the feature is disabled.
	if alloc.features.DeviceTaints && taintPreventsAllocation(device.Device, request) {
		alloc.rollbackDevice(r, device, &requestData, state)
		return false, nil, nil
	}

	// 检查声明级约束；任一约束不满足则回滚并拒绝。
	// It's available. Now check constraints.
	// 依次应用该声明的所有约束。
	for _, constraint := range alloc.constraints[r.claimIndex] {
		added, err := constraint.add(&requestData, device.Device, device.id)
		// 约束执行出错：回滚并返回错误。
		if err != nil {
			alloc.rollbackDevice(r, device, &requestData, state)
			return false, nil, err
		}
		// 约束不满足：回滚；must 模式下视为错误。
		if !added {
			alloc.rollbackDevice(r, device, &requestData, state)
			if must {
				// It does not make sense to declare a claim where a constraint prevents getting
				// all devices. Treat this as an error.
				return false, nil, fmt.Errorf("claim %s, request %s: cannot add device %s because a claim constraint would not be satisfied", klog.KObj(claim), request.name(), device.id)
			}
			return false, nil, nil
		}
		// 记录成功添加的约束数，便于回滚时逆向撤销。
		state.constraintsAdded++
	}

	// 所有约束满足：将设备标记为占用（管理访问或共享分配除外）并记录。
	// All constraints satisfied. Mark as in use (unless we do admin access or allow multiple allocations)
	// and record the result.
	alloc.logger.V(7).Info("Device allocated", "device", device.id)

	// 非共享分配：把当前声明索引加入占用集合。
	if !allowMultipleAllocations {
		if alloc.allocatingDevices[device.id] == nil {
			alloc.allocatingDevices[device.id] = make(sets.Set[int])
		}
		alloc.allocatingDevices[device.id].Insert(r.claimIndex)
		state.deviceMarked = true
	}

	// 处理容量：复核容量、计算共享份额的容量消耗并生成份额 ID。
	consumedCapacity := make(map[resourceapi.QualifiedName]resource.Quantity, 0)
	var shareID *types.UID
	// 再次确认容量充足，失败则回滚。
	if alloc.features.ConsumableCapacity {
		// Validate whether resource request over capacity
		success, err := alloc.CmpRequestOverCapacity(requestData.request, device.slice, *device.Device)
		if err != nil {
			// The overflow guard already ran during validation, so an error here is
			// unexpected. Roll back and surface it rather than swallowing it.
			alloc.rollbackDevice(r, device, &requestData, state)
			return false, nil, fmt.Errorf("claim %s, request %s: rechecking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), device.id, err)
		}
		if !success {
			alloc.logger.V(7).Info("Device capacity not enough", "device", device)
			alloc.rollbackDevice(r, device, &requestData, state)
			return false, nil, nil
		}

		// 共享分配：计算本次消耗的容量、生成份额 ID 并累加到总量。
		if allowMultipleAllocations {
			consumedCapacity, err = GetConsumedCapacityFromRequest(request.capacities(), device.Capacity, alloc.features.FractionalCapacityRange)
			if err != nil {
				alloc.rollbackDevice(r, device, &requestData, state)
				return false, nil, fmt.Errorf("claim %s, request %s: computing consumed capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), device.id, err)
			}
			// 为共享份额生成唯一 ID。
			shareID = GenerateNewShareID()
			alloc.logger.V(7).Info("Device capacity allocated", "device", device.id,
				"consumed capacity", klog.Format(consumedCapacity))
			// A prior share of this device may already hold the capacity entry.
			// That entry doubles as the "already shared" marker that lets a later
			// share skip the counter check, so record whether it predated this
			// share; rollback must not delete it while another share still needs it.
			// 记录该设备此前是否已有容量条目：该条目同时充当“已共享”标记，回滚时不能误删。
			_, state.capacityEntryExisted = alloc.allocatingCapacity[device.id]
			alloc.allocatingCapacity.Insert(NewDeviceConsumedCapacity(device.id, consumedCapacity))
			state.capacityInserted = true
			state.consumedCapacity = consumedCapacity
		}
	}

	// 构造内部结果并追加到当前声明的结果列表。
	result := internalDeviceResult{
		request:       request.name(),
		parentRequest: parentRequestName,
		id:            device.id,
		Device:        device.Device,
		slice:         device.slice,
		shareID:       shareID,
	}
	// 请求了管理访问时，在结果中标记。
	if request.adminAccess() {
		result.adminAccess = ptr.To(request.adminAccess())
	}
	// 有容量消耗时写入结果。
	if len(consumedCapacity) > 0 {
		result.consumedCapacity = consumedCapacity
	}
	// 记录结果并标记，便于回滚时截断。
	alloc.result[r.claimIndex].devices = append(alloc.result[r.claimIndex].devices, result)
	state.resultAdded = true

	// Only this success path builds an escaping closure, so backtracking can undo
	// a committed candidate. undo is a copy: capturing state directly would move it
	// and its flags to the heap on the rejection paths too, which never escape.
	// 仅成功路径创建逃逸的回滚闭包；拷贝 state 以避免拒绝路径也把它移到堆上。
	undo := state
	return true, func() {
		alloc.rollbackDevice(r, device, &requestData, undo)
	}, nil
}

// deviceRollbackState records the mutations allocateDevice makes for a single
// candidate so rollbackDevice can undo them, both when the candidate is rejected
// and when the backtracking search abandons a previously successful candidate.
// deviceRollbackState 记录 allocateDevice 对单个候选设备所做的状态变更，供 rollbackDevice 在拒绝或回溯时撤销。
// 字段：restoreCompatibilityGroups 恢复兼容性组交集；countersReserved 标记计数器是否已预留；constraintsAdded 记录成功添加的约束数。
// deviceMarked 标记是否加入占用集合；capacityInserted 与 capacityEntryExisted 记录容量条目的插入与原有状态。
// resultAdded 标记是否追加结果；previousNumResults 为追加前的切片长度；consumedCapacity 为本次消耗的容量。
type deviceRollbackState struct {
	// restoreCompatibilityGroups, when non-nil, reverts the rolling
	// compatibility-group intersection consumed for this device by
	// checkAndConsumeCompatibilityGroups.
	restoreCompatibilityGroups func()
	countersReserved           bool
	constraintsAdded           int
	deviceMarked               bool
	capacityInserted           bool
	capacityEntryExisted       bool
	resultAdded                bool
	previousNumResults         int
	consumedCapacity           map[resourceapi.QualifiedName]resource.Quantity
}

// rollbackDevice reverses the mutations recorded in state, in the opposite order
// they were applied. It is called synchronously on the rejection paths and, via
// the closure returned on success, during backtracking.
// rollbackDevice 按与施加相反的顺序撤销 state 记录的所有变更；在拒绝路径同步调用，在成功路径通过返回的闭包于回溯时调用。
func (alloc *allocator) rollbackDevice(r deviceIndices, device deviceWithID, requestData *requestData, state deviceRollbackState) {
	// 截断结果切片，撤销结果追加。
	if state.resultAdded {
		alloc.result[r.claimIndex].devices = alloc.result[r.claimIndex].devices[:state.previousNumResults]
	}
	// 移除容量消耗条目；若此前已有条目，移除后需恢复空条目以保留“已共享”标记。
	if state.capacityInserted {
		alloc.allocatingCapacity.Remove(NewDeviceConsumedCapacity(device.id, state.consumedCapacity))
		if state.capacityEntryExisted {
			// Remove drops the entry once it becomes empty, which also erases the
			// shared marker that the earlier share still relies on. Restore an empty
			// entry in that case so the earlier share stays accounted as shared.
			// Remove 会在条目变空时删除它，抹掉先前份额依赖的共享标记，因此需补回空条目。
			if _, found := alloc.allocatingCapacity[device.id]; !found {
				alloc.allocatingCapacity[device.id] = NewConsumedCapacity()
			}
		}
	}
	// 从占用集合中删除当前声明。
	if state.deviceMarked {
		alloc.allocatingDevices[device.id].Delete(r.claimIndex)
	}
	// 逆向撤销所有已添加的约束。
	for i := state.constraintsAdded - 1; i >= 0; i-- {
		alloc.constraints[r.claimIndex][i].remove(requestData, device.Device, device.id)
	}
	// 恢复兼容性组交集。
	if state.restoreCompatibilityGroups != nil {
		state.restoreCompatibilityGroups()
	}
	// 释放预留的计数器。
	if state.countersReserved {
		alloc.deallocateCountersForDevice(device)
	}
	// 仅完整分配过的候选才有结果，此处为真正的回溯释放。
	if state.resultAdded {
		// Only a fully allocated candidate recorded a result, so this is a real
		// deallocation during backtracking, not a rejection rollback.
		alloc.logger.V(7).Info("Device deallocated", "device", device.id)
	}
}

// taintPreventsAllocation 判断设备的污点是否阻止分配：仅 NoExecute 与 NoSchedule 生效，且需未被请求容忍。
func taintPreventsAllocation(device *draapi.Device, request requestAccessor) bool {
	// 遍历污点，只处理已知生效类型。
	for _, taint := range device.Taints {
		switch taint.Effect {
		// Only known effects prevent allocation, others (including None) are ignored.
		case resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule:
			// 污点未被容忍则阻止分配。
			if !taintTolerated(taint, request) {
				return true
			}
		}
	}
	return false
}

// taintTolerated 判断请求的容忍列表是否容忍给定污点。
func taintTolerated(taint resourceapi.DeviceTaint, request requestAccessor) bool {
	// 任一容忍匹配即返回 true。
	for _, toleration := range request.tolerations() {
		if resourceclaim.ToleratesTaint(toleration, taint) {
			return true
		}
	}
	return false
}

// checkAvailableCounters checks if there are enough counters available to allocate
// the specified device.
//
// Gets called only if the partitionable devices feature is enabled and the device
// consumes counters.
// checkAvailableCounters 检查池中是否有足够的计数器可供分配该设备；仅在 partitionable devices 特性启用且设备消耗计数器时调用。
func (alloc *allocator) checkAvailableCounters(device deviceWithID) (bool, error) {
	// 先读缓存：可用计数器按池惰性计算一次后不再变化。
	pool := device.pool
	poolID := pool.PoolID

	// Check first if the available counters for this pool have already been
	// calculated.
	alloc.mutex.RLock()
	availableCountersForPool, found := alloc.availableCounters[poolID]
	alloc.mutex.RUnlock()
	// If not, we need to do it now. But we store the result so it doesn't need
	// to be calculated again.
	// Since this is computed without holding the lock on the mutex, other goroutines
	// might also do this work. But the input will be the same to all of them, so
	// the result will also always be the same.
	// 未命中时计算；因计算不持锁，多个 goroutine 可能重复计算，但输入相同故结果一致。
	if !found {
		// 从池定义拷贝计数器，作为可用量的起点。
		availableCountersForPool = make(counterSets, len(pool.CounterSets))
		// 深拷贝计数器值，避免修改池定义。
		for _, counterSet := range pool.CounterSets {
			availableCountersForCounterSet := make(map[string]resourceapi.Counter, len(counterSet.Counters))
			for name, c := range counterSet.Counters {
				c.Value = c.Value.DeepCopy()
				availableCountersForCounterSet[name] = c
			}
			availableCountersForPool[counterSet.Name] = availableCountersForCounterSet
		}

		// 扣减已完成分配的设备所消耗的计数器；正在分配的设备由 consumedCounters 单独跟踪。
		// Update the data structure to reflect counters already consumed by allocated devices. This
		// only includes devices where the allocation process has completed, so this will never
		// change during the allocation process.
		// 遍历 targeting 与非 targeting 本节点的 slice。
		for _, resourceSlices := range [][]*draapi.ResourceSlice{pool.DeviceSlicesTargetingNode, pool.DeviceSlicesNotTargetingNode} {
			for _, slice := range resourceSlices {
				for _, device := range slice.Spec.Devices {
					deviceID := DeviceID{
						Driver: slice.Spec.Driver,
						Pool:   slice.Spec.Pool.Name,
						Device: device.Name,
					}
					// 未分配的设备不消耗计数器。
					// Devices that aren't allocated doesn't consume any counters, so we don't
					// need to consider them.
					if !internal.IsDeviceAllocated(deviceID, &alloc.allocatedState) {
						continue
					}
					// 从可用量中扣减该设备声明消耗的计数器。
					for _, deviceCounterConsumption := range device.ConsumesCounters {
						availableCountersForCounterSet := availableCountersForPool[deviceCounterConsumption.CounterSet]
						for name, c := range deviceCounterConsumption.Counters {
							existingCounter, ok := availableCountersForCounterSet[name]
							if !ok {
								// API 校验保证引用的计数器存在；此处防御性跳过。
								// the API validation logic has been added to make sure the counters referred should exist in counter sets.
								continue
							}
							// 可用量可为负，视为无可用计数器。
							// This can potentially result in negative available counters. That is fine,
							// we just treat it as no counters available.
							existingCounter.Value.Sub(c.Value)
							availableCountersForCounterSet[name] = existingCounter
						}
					}
					// Note that we don't include devices in the alloc.allocatingDevices here since
					// counters consumed by devices for the current claims are tracked in
					// alloc.consumedCounters
				}
			}
		}

		// 写入缓存，避免重复计算。
		// Set the available counters on the allocator so we don't have to
		// compute this again.
		alloc.mutex.Lock()
		alloc.availableCounters[poolID] = availableCountersForPool
		alloc.mutex.Unlock()
	}

	// 把当前设备消耗的计数器累加到本次分配的已消耗结构中。
	// Update the consumedCounters data structure with the counters consumed
	// by the current device.
	// 若该池尚无已消耗记录则初始化。
	consumedCountersForPool, found := alloc.consumedCounters[poolID]
	// If no devices in the allocating state have consumed any counters from the current
	// pool, initialize the data structure.
	if !found {
		consumedCountersForPool = make(counterSets)
		alloc.consumedCounters[poolID] = consumedCountersForPool
	}
	// 按计数器集与名称累加消耗量。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		consumedCountersForCounterSet, found := consumedCountersForPool[deviceCounterConsumption.CounterSet]
		if !found {
			consumedCountersForCounterSet = make(map[string]resourceapi.Counter)
			consumedCountersForPool[deviceCounterConsumption.CounterSet] = consumedCountersForCounterSet
		}
		// 首次出现时深拷贝，否则在已有值上累加。
		for name, c := range deviceCounterConsumption.Counters {
			consumedCounters, found := consumedCountersForCounterSet[name]
			if !found {
				c.Value = c.Value.DeepCopy()
				consumedCountersForCounterSet[name] = c
				continue
			}
			consumedCounters.Value.Add(c.Value)
			consumedCountersForCounterSet[name] = consumedCounters
		}
	}

	// 比较可用量与已消耗量，超限则回滚当前设备的消耗并返回失败。
	// Check that we didn't exceed the availability of any counters by allocating
	// the current device. If we did, the current set of devices doesn't work, so we
	// update the consumed counters to no longer reflect the current device.
	// 逐个计数器集、逐个计数器比较。
	for availableCounterSetName, availableCounters := range availableCountersForPool {
		consumedCounters := consumedCountersForPool[availableCounterSetName]
		for availableCounterName, availableCounter := range availableCounters {
			consumedCounter := consumedCounters[availableCounterName]
			// 可用量小于消耗量：释放计数器并返回 false。
			if availableCounter.Value.Cmp(consumedCounter.Value) < 0 {
				alloc.deallocateCountersForDevice(device)
				return false, nil
			}
		}
	}

	// 未超限，检查通过。
	return true, nil
}

// deviceInUse 判断设备是否已被分配，或正被本次分配中的任意声明占用。
func (alloc *allocator) deviceInUse(deviceID DeviceID) bool {
	return alloc.allocatedState.AllocatedDevices.Has(deviceID) || alloc.allocatingDeviceForAnyClaim(deviceID)
}

// deviceCapacityInUse 判断设备的容量是否已被历史分配或本次分配占用（含仅记录在共享设备 ID 中的情况）。
func (alloc *allocator) deviceCapacityInUse(deviceID DeviceID) bool {
	// 历史聚合容量中有记录即表示已使用。
	if _, found := alloc.allocatedState.AggregatedCapacity[deviceID]; found {
		return true
	}
	// 无按份额容量的历史共享分配只记录在 AllocatedSharedDeviceIDs 中。
	// A persisted allow-multiple allocation with no per-share capacity is recorded
	// only in AllocatedSharedDeviceIDs (with no AggregatedCapacity entry).
	for sharedDeviceID := range alloc.allocatedState.AllocatedSharedDeviceIDs {
		if sharedDeviceID.GetDeviceID() == deviceID {
			return true
		}
	}
	// 再检查本次分配中是否有声明正在使用该设备容量。
	return alloc.allocatingCapacityForAnyClaim(deviceID)
}

// allocatingDeviceForAnyClaim 判断本次分配中是否有任意声明正在占用该设备。
func (alloc *allocator) allocatingDeviceForAnyClaim(deviceID DeviceID) bool {
	return alloc.allocatingDevices[deviceID].Len() > 0
}

// allocatingDeviceForClaim 判断本次分配中指定声明是否正在占用该设备。
func (alloc *allocator) allocatingDeviceForClaim(deviceID DeviceID, claimIndex int) bool {
	return alloc.allocatingDevices[deviceID].Has(claimIndex)
}

// allocatingCapacityForAnyClaim 判断本次分配中是否有声明正在消耗该设备容量。
func (alloc *allocator) allocatingCapacityForAnyClaim(deviceID DeviceID) bool {
	_, found := alloc.allocatingCapacity[deviceID]
	return found
}

// deallocateCountersForDevice subtracts the consumed counters of the provided
// device from the consumedCounters data structure.
// deallocateCountersForDevice 从已消耗计数器结构中扣减该设备消耗的计数器。
func (alloc *allocator) deallocateCountersForDevice(device deviceWithID) {
	poolID := device.pool.PoolID

	// 按设备声明的消耗逐项扣减。
	consumedCountersForPool := alloc.consumedCounters[poolID]
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		counterSetName := deviceCounterConsumption.CounterSet
		consumedCounterSet := consumedCountersForPool[counterSetName]
		for name, c := range deviceCounterConsumption.Counters {
			consumedCounter := consumedCounterSet[name]
			consumedCounter.Value.Sub(c.Value)
			consumedCounterSet[name] = consumedCounter
		}
	}
}

// compatibilityGroupsBaselineForPool returns, per counter set in the pool, the
// compatibility-group intersection contributed by the devices that are already
// allocated (i.e. recorded in allocatedState). Both counter set membership and
// the declared group values are read live from the source ResourceSlice,
// exactly like checkAvailableCounters does for counters: an allocated device's
// consumption is assumed stable in the slice. A device that is allocated but no
// longer present in the slice contributes nothing, just as its counters do not.
//
// The result is computed once per pool and cached, mirroring availableCounters.
// compatibilityGroupsBaselineForPool 按池、按计数器集返回已分配设备贡献的兼容性组交集基线；与计数器一样从源 ResourceSlice 实时读取，按池惰性计算并缓存。
func (alloc *allocator) compatibilityGroupsBaselineForPool(pool *Pool) map[string]compatibilityGroupIntersection {
	// 先查缓存，命中即返回。
	poolID := pool.PoolID

	alloc.mutex.RLock()
	baseline, found := alloc.compatibilityGroupsBaseline[poolID]
	alloc.mutex.RUnlock()
	if found {
		return baseline
	}

	// 遍历两类 slice 中的设备，仅统计已分配设备对各计数器集的兼容性组交集。
	baseline = make(map[string]compatibilityGroupIntersection)
	for _, resourceSlices := range [][]*draapi.ResourceSlice{pool.DeviceSlicesTargetingNode, pool.DeviceSlicesNotTargetingNode} {
		// 逐设备、逐消耗记录累积交集。
		for _, slice := range resourceSlices {
			for _, device := range slice.Spec.Devices {
				deviceID := DeviceID{
					Driver: slice.Spec.Driver,
					Pool:   slice.Spec.Pool.Name,
					Device: device.Name,
				}
				// 未分配的设备不贡献兼容性组。
				if !internal.IsDeviceAllocated(deviceID, &alloc.allocatedState) {
					continue
				}
				// 把该设备声明的兼容性组并入对应计数器集的基线交集。
				for _, deviceCounterConsumption := range device.ConsumesCounters {
					counterSetName := deviceCounterConsumption.CounterSet.String()
					cur := baseline[counterSetName]
					baseline[counterSetName] = cur.add(compatibilityGroupSet(deviceCounterConsumption.CompatibilityGroups))
				}
			}
		}
	}

	// 写入缓存后返回。
	alloc.mutex.Lock()
	alloc.compatibilityGroupsBaseline[poolID] = baseline
	alloc.mutex.Unlock()
	return baseline
}

// checkAndConsumeCompatibilityGroups verifies that the candidate device can be
// co-allocated, for every counter set it consumes, with the devices already
// placed on that counter set (allocated peers plus in-flight allocations of the
// current attempt). If so it folds the candidate's groups into the rolling
// intersection and returns a rollback function that restores the previous state
// when the allocator backtracks. If not, it leaves the running state unchanged
// and returns false.
// checkAndConsumeCompatibilityGroups 检查候选设备在其消耗的每个计数器集上，能否与已分配及本次正在分配的设备共存；能则将设备的兼容性组并入滚动交集并返回回滚函数，不能则恢复原状并返回 false。
func (alloc *allocator) checkAndConsumeCompatibilityGroups(device deviceWithID) (bool, func()) {
	// 取该池的滚动交集映射（不存在则初始化）与已分配设备基线。
	poolID := device.pool.PoolID
	running, found := alloc.consumedCompatibilityGroups[poolID]
	if !found {
		running = make(map[string]compatibilityGroupIntersection)
		alloc.consumedCompatibilityGroups[poolID] = running
	}
	baseline := alloc.compatibilityGroupsBaselineForPool(device.pool)

	// 定义回滚记录与回滚函数：按逆序恢复被修改的计数器集条目。
	type savedEntry struct {
		counterSet string
		prev       compatibilityGroupIntersection
		existed    bool
	}
	var saved []savedEntry
	// 回滚函数按逆序恢复，防止同一计数器集重复记录时回滚出错。
	rollback := func() {
		// Restore in reverse order so repeated entries for the same counter set
		// (a device cannot have them, but be defensive) unwind correctly.
		for idx := len(saved) - 1; idx >= 0; idx-- {
			s := saved[idx]
			if s.existed {
				running[s.counterSet] = s.prev
			} else {
				delete(running, s.counterSet)
			}
		}
	}

	// 逐个计数器集检查并消费兼容性组。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		counterSetName := deviceCounterConsumption.CounterSet.String()
		// 首次触及该计数器集时，以已分配设备的基线交集为种子。
		cur, existed := running[counterSetName]
		if !existed {
			// Seed from the already-allocated peers for this counter set.
			cur = baseline[counterSetName]
		}
		// 若当前交集不接受该设备的兼容性组，则回滚并返回不兼容。
		groups := compatibilityGroupSet(deviceCounterConsumption.CompatibilityGroups)
		if !cur.admits(groups) {
			rollback()
			return false, nil
		}
		// 记录旧值以便回滚，并把设备的兼容性组并入交集。
		saved = append(saved, savedEntry{counterSet: counterSetName, prev: cur, existed: existed})
		running[counterSetName] = cur.add(groups)
	}

	// 全部通过：返回成功与回滚函数。
	return true, rollback
}

// createNodeSelector constructs a node selector for the allocation, if needed,
// otherwise it returns nil.
// createNodeSelector 根据所选设备的位置约束构造 NodeSelector；若设备可用于任意节点则返回 nil。
func (alloc *allocator) createNodeSelector(result []internalDeviceResult, targetNodeName string) (*v1.NodeSelector, error) {
	// 先准备一个只含一个 term 的 NodeSelector，后续追加各设备的节点要求。
	// Selector with one term. That term gets extended with additional
	// requirements from the different devices.
	ns := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{}},
	}

	// 遍历已分配设备，收集其节点限制。
	for i := range result {
		// 根据是否启用每设备节点选择，取设备或 slice 上的节点名与节点选择器。
		slice := result[i].slice
		var nodeName *string
		var nodeSelector *v1.NodeSelector
		if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			nodeName = result[i].NodeName
			nodeSelector = result[i].NodeSelector
		} else {
			nodeName = slice.Spec.NodeName
			nodeSelector = slice.Spec.NodeSelector
		}
		// 若任一设备绑定到具体节点，则整个分配必须限定到该节点。
		if nodeName != nil || result[i].BindsToNode {
			// At least one device is local to one node or binds to a node,
			// so we need to restrict the allocation to that node.
			return &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{targetNodeName},
					}},
				}},
			}, nil
		}
		// 合并设备上的节点选择器要求到统一的选择器中。
		if nodeSelector != nil {
			// 仅支持零个或一个 term 的选择器，多个 term 视为非法。
			switch len(nodeSelector.NodeSelectorTerms) {
			case 0:
				// Nothing?
			case 1:
				// 单个 term：把其 MatchFields 与 MatchExpressions 去重后加入。
				// Add all terms if they are not present already.
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchFields, &ns.NodeSelectorTerms[0].MatchFields)
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchExpressions, &ns.NodeSelectorTerms[0].MatchExpressions)
			default:
				// This shouldn't occur, validation must prevent creation of such slices.
				return nil, fmt.Errorf("unsupported ResourceSlice.NodeSelector with %d terms", len(nodeSelector.NodeSelectorTerms))
			}
		}
	}

	// 若收集到任何要求，返回构造好的选择器。
	if len(ns.NodeSelectorTerms[0].MatchFields) > 0 || len(ns.NodeSelectorTerms[0].MatchExpressions) > 0 {
		// We have a valid node selector.
		return ns, nil
	}

	// 无任何节点限制，返回 nil 表示可用于任意节点。
	// Available everywhere.
	return nil, nil
}

// requestAccessor is an interface for accessing either
// DeviceRequests or DeviceSubRequests. It lets most
// of the allocator code work with either DeviceRequests
// or DeviceSubRequests.
// requestAccessor 抽象 DeviceRequest 与 DeviceSubRequest 的公共访问接口，使分配器主体逻辑无需区分二者。
type requestAccessor interface {
	name() string
	deviceClassName() string
	allocationMode() resourceapi.DeviceAllocationMode
	count() int64
	adminAccess() bool
	hasAdminAccess() bool
	selectors() []resourceapi.DeviceSelector
	tolerations() []resourceapi.DeviceToleration
	capacities() *resourceapi.CapacityRequirements
	derivedAttributes() []resourceapi.DeviceDerivedAttribute
}

// exactDeviceRequestAccessor is an implementation of the
// requestAccessor interface for ExactDeviceRequests.
// exactDeviceRequestAccessor 是 requestAccessor 针对精确设备请求（DeviceRequest）的实现。
type exactDeviceRequestAccessor struct {
	request *resourceapi.DeviceRequest
}

// name 返回请求名。
func (d *exactDeviceRequestAccessor) name() string {
	return d.request.Name
}

// deviceClassName 返回请求指定的设备类名。
func (d *exactDeviceRequestAccessor) deviceClassName() string {
	return d.request.Exactly.DeviceClassName
}

// allocationMode 返回请求的分配模式。
func (d *exactDeviceRequestAccessor) allocationMode() resourceapi.DeviceAllocationMode {
	return d.request.Exactly.AllocationMode
}

// count 返回请求所需的设备数量。
func (d *exactDeviceRequestAccessor) count() int64 {
	return d.request.Exactly.Count
}

// adminAccess 返回是否请求管理访问（未设置时为 false）。
func (d *exactDeviceRequestAccessor) adminAccess() bool {
	return ptr.Deref(d.request.Exactly.AdminAccess, false)
}

// hasAdminAccess 返回请求是否显式设置了管理访问字段。
func (d *exactDeviceRequestAccessor) hasAdminAccess() bool {
	return d.request.Exactly.AdminAccess != nil
}

// derivedAttributes 返回请求声明的派生属性列表。
func (d *exactDeviceRequestAccessor) derivedAttributes() []resourceapi.DeviceDerivedAttribute {
	if d.request.Exactly != nil {
		return d.request.Exactly.DerivedAttributes
	}
	return nil
}

// selectors 返回请求的选择器列表。
func (d *exactDeviceRequestAccessor) selectors() []resourceapi.DeviceSelector {
	return d.request.Exactly.Selectors
}

// tolerations 返回请求的容忍列表。
func (d *exactDeviceRequestAccessor) tolerations() []resourceapi.DeviceToleration {
	return d.request.Exactly.Tolerations
}

// capacities 返回请求声明的容量需求。
func (d *exactDeviceRequestAccessor) capacities() *resourceapi.CapacityRequirements {
	return d.request.Exactly.Capacity
}

// deviceSubRequestAccessor is an implementation of the
// requestAccessor interface for DeviceSubRequests.
// deviceSubRequestAccessor 是 requestAccessor 针对子请求（DeviceSubRequest）的实现。
type deviceSubRequestAccessor struct {
	subRequest *resourceapi.DeviceSubRequest
}

// name 返回子请求名。
func (d *deviceSubRequestAccessor) name() string {
	return d.subRequest.Name
}

// deviceClassName 返回子请求指定的设备类名。
func (d *deviceSubRequestAccessor) deviceClassName() string {
	return d.subRequest.DeviceClassName
}

// allocationMode 返回子请求的分配模式。
func (d *deviceSubRequestAccessor) allocationMode() resourceapi.DeviceAllocationMode {
	return d.subRequest.AllocationMode
}

// count 返回子请求所需的设备数量。
func (d *deviceSubRequestAccessor) count() int64 {
	return d.subRequest.Count
}

// adminAccess 恒为 false：子请求不支持管理访问。
func (d *deviceSubRequestAccessor) adminAccess() bool {
	return false
}

// hasAdminAccess 恒为 false：子请求不支持管理访问。
func (d *deviceSubRequestAccessor) hasAdminAccess() bool {
	return false
}

// derivedAttributes 返回子请求声明的派生属性列表。
func (d *deviceSubRequestAccessor) derivedAttributes() []resourceapi.DeviceDerivedAttribute {
	return d.subRequest.DerivedAttributes
}

// selectors 返回子请求的选择器列表。
func (d *deviceSubRequestAccessor) selectors() []resourceapi.DeviceSelector {
	return d.subRequest.Selectors
}

// tolerations 返回子请求的容忍列表。
func (d *deviceSubRequestAccessor) tolerations() []resourceapi.DeviceToleration {
	return d.subRequest.Tolerations
}

// capacities 返回子请求声明的容量需求。
func (d *deviceSubRequestAccessor) capacities() *resourceapi.CapacityRequirements {
	return d.subRequest.Capacity
}

// addNewNodeSelectorRequirements 把 from 中尚未存在的要求追加到 to（按 Key、Operator、Values 判重）。
func addNewNodeSelectorRequirements(from []v1.NodeSelectorRequirement, to *[]v1.NodeSelectorRequirement) {
	// 逐条检查，仅追加尚未存在的要求。
	for _, requirement := range from {
		if !containsNodeSelectorRequirement(*to, requirement) {
			*to = append(*to, requirement)
		}
	}
}

// containsNodeSelectorRequirement 判断 requirements 中是否已存在与给定要求完全一致（Key、Operator、Values 均相同）的条目。
func containsNodeSelectorRequirement(requirements []v1.NodeSelectorRequirement, requirement v1.NodeSelectorRequirement) bool {
	// 将目标要求的 Values 转成集合便于比较；逐条比对已有要求。
	values := sets.New(requirement.Values...)
	for _, existingRequirement := range requirements {
		// Key 不同则跳过。
		if existingRequirement.Key != requirement.Key {
			continue
		}
		// Operator 不同则跳过。
		if existingRequirement.Operator != requirement.Operator {
			continue
		}
		// Values 集合不同则跳过。
		if !sets.New(existingRequirement.Values...).Equal(values) {
			continue
		}
		// 三项均一致，判定为已存在。
		return true
	}
	return false
}
