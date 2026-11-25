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

package incubating

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
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// 以下类型是 internal 包对应类型的别名：DeviceClassLister 按名称查询设备类；Features 描述启用的特性集合；
// DeviceID 由驱动名、池名与设备名唯一标识设备；Stats 承载分配器对外暴露的统计信息。
type DeviceClassLister = internal.DeviceClassLister
type Features = internal.Features
type DeviceID = internal.DeviceID
type Stats = internal.Stats

// MakeDeviceID 构造 DeviceID，委托给 internal 包实现。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return internal.MakeDeviceID(driver, pool, device)
}

// 以下类型是 internal 包对应类型的别名：SharedDeviceID 标识共享设备的一次份额；
// DeviceConsumedCapacity 记录单台设备已消耗容量；ConsumedCapacityCollection 按设备汇总容量；
// ConsumedCapacity 表示容量名到用量的映射；AllocatedState 保存已分配设备与容量状态。
type SharedDeviceID = internal.SharedDeviceID
type DeviceConsumedCapacity = internal.DeviceConsumedCapacity
type ConsumedCapacityCollection = internal.ConsumedCapacityCollection
type ConsumedCapacity = internal.ConsumedCapacity
type AllocatedState = internal.AllocatedState

// GenerateNewShareID 生成新的份额 ID，用于共享设备的多次独立分配。
func GenerateNewShareID() *types.UID {
	return internal.GenerateShareID()
}

// NewConsumedCapacity 创建一个空的已消耗容量集合。
func NewConsumedCapacity() ConsumedCapacity {
	return internal.NewConsumedCapacity()
}

// NewDeviceConsumedCapacity 用指定设备 ID 与用量映射构造 DeviceConsumedCapacity。
func NewDeviceConsumedCapacity(deviceID DeviceID,
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) DeviceConsumedCapacity {
	return internal.NewDeviceConsumedCapacity(deviceID, consumedCapacity)
}

// NewConsumedCapacityCollection 创建一个空的按设备汇总容量容器。
func NewConsumedCapacityCollection() ConsumedCapacityCollection {
	return internal.NewConsumedCapacityCollection()
}

// SupportedFeatures 列出 incubating 支持的附加特性；这些特性的任意组合都会选择本变体。
var SupportedFeatures = internal.Features{
	AdminAccess:             true,
	PrioritizedList:         true,
	PartitionableDevices:    true,
	DeviceTaints:            true,
	DeviceBindingAndStatus:  true,
	ConsumableCapacity:      true,
	FractionalCapacityRange: true,
}

// Allocator 是 incubating 变体的分配器，持有可跨多次 Allocate 复用的池信息与计数器缓存，
// 可被多个 goroutine 并发使用。
type Allocator struct {
	// 字段说明：features 为启用的特性；allocatedState 为历史已分配状态；classLister 查询设备类；
	// slicesOnNode/slicesShared 分别为节点本地与集群共享的切片；allSlices 为全部切片；celCache 缓存 CEL 表达式。
	features       Features
	allocatedState AllocatedState
	classLister    DeviceClassLister
	slicesOnNode   map[string][]*resourceapi.ResourceSlice
	slicesShared   []*resourceapi.ResourceSlice
	allSlices      []*resourceapi.ResourceSlice
	celCache       *cel.Cache
	// availableCounters 以 PoolID（驱动名+池名）为键，按池缓存可用计数器，首次需要时计算且之后不再更新。
	// 缓存基于 Allocator 的静态输入，因此跨多次 Allocate 调用仍有效；mutex 保护该映射的并发读写。
	availableCounters map[PoolID]counterSets
	mutex             sync.RWMutex
	// numAllocateOneInvocations 统计 allocateOne 调用次数，用于估算分配所需搜索工作量。
	numAllocateOneInvocations atomic.Int64
}

// 编译期断言：Allocator 实现 internal.AllocatorExtended 接口。
var _ internal.AllocatorExtended = &Allocator{}

// NewAllocator 根据特性、已分配状态、设备类查询器、切片与 CEL 缓存构造分配器；
// 若发现导致无法分配的问题则返回错误。返回的实例可重复使用且线程安全。
func NewAllocator(ctx context.Context,
	features Features,
	allocatedState AllocatedState,
	classLister DeviceClassLister,
	slices []*resourceapi.ResourceSlice,
	celCache *cel.Cache,
) (*Allocator, error) {
	// 按 NodeName 把切片划分为节点本地与集群共享两类，便于按目标节点筛选。
	slicesOnNode := make(map[string][]*resourceapi.ResourceSlice)
	slicesShared := make([]*resourceapi.ResourceSlice, 0)
	for _, slice := range slices {
		// NodeName 为空表示这是集群共享切片。
		nodeName := ptr.Deref(slice.Spec.NodeName, "")
		if nodeName == "" {
			slicesShared = append(slicesShared, slice)
		} else {
			slicesOnNode[nodeName] = append(slicesOnNode[nodeName], slice)
		}
	}
	// 初始化分配器；availableCounters 采用延迟计算，此处仅创建空映射。
	return &Allocator{
		features:          features,
		allocatedState:    allocatedState,
		classLister:       classLister,
		slicesOnNode:      slicesOnNode,
		slicesShared:      slicesShared,
		allSlices:         slices,
		celCache:          celCache,
		availableCounters: make(map[PoolID]counterSets),
	}, nil
}

// Channel 返回本分配器所属的实现通道标识（incubating）。
func (a *Allocator) Channel() internal.AllocatorChannel {
	return internal.Incubating
}

// Allocate 为目标节点上的一组 ResourceClaim 分配设备。
// 它先收集可用池并校验请求，再对每个请求递归穷举候选设备；找到第一个完整解后生成
// AllocationResult，其中包含设备结果、设备类与声明配置以及节点选择器。
func (a *Allocator) Allocate(ctx context.Context, node *v1.Node, claims []*resourceapi.ResourceClaim) (finalResult []resourceapi.AllocationResult, finalErr error) {
	// 每次调用创建独立的 allocator 状态，避免并发调用间相互干扰。
	alloc := &allocator{
		Allocator:            a,
		ctx:                  ctx, // 所有方法共享同一个 a，因此也共享 ctx
		logger:               klog.FromContext(ctx),
		node:                 node,
		claimsToAllocate:     claims,
		deviceMatchesRequest: make(map[matchKey]bool),
		constraints:          make([][]constraint, len(claims)),
		consumedCounters:     make(map[PoolID]counterSets),
		requestData:          make(map[requestIndices]requestData),
		result:               make([]internalAllocationResult, len(claims)),
		allocatingCapacity:   NewConsumedCapacityCollection(),
	}
	// 目标节点可见切片 = 节点本地切片 + 集群共享切片。
	slicesForNode := slices.Concat(alloc.slicesOnNode[node.Name], alloc.slicesShared)
	alloc.logger.V(5).Info("Starting allocation", "numClaims", len(alloc.claimsToAllocate), "numSlicesForNode", len(slicesForNode))
	// 记录分配结束日志；success 由返回结果数量是否等于声明数量判断。
	defer func() {
		alloc.logger.V(5).Info("Done with allocation", "success", len(finalResult) == len(alloc.claimsToAllocate), "err", finalErr)
	}()

	// 收集所有候选池；池不完整或无效会影响后续分配以及最终的错误上报。
	pools, err := GatherPools(ctx, slicesForNode, node, a.features, alloc.allSlices)
	if err != nil {
		return nil, fmt.Errorf("gather pool information: %w", err)
	}
	alloc.pools = pools
	alloc.logger.V(5).Info("Gathered pool information", "pools", logPools(alloc.logger, pools))

	// 逐个声明、请求与设备枚举候选，选中后标记占用并递归，从而覆盖所有可行顺序，找到第一个完整解即采用。
	// 当前无评分；未来引入评分后需比较等价解，并避免重复尝试实际相同的请求与设备组合。
	// 预校验每个请求：统计最少设备数，并检查特性开关与请求格式是否受支持。
	minDevicesTotal := 0
	for claimIndex, claim := range alloc.claimsToAllocate {
		minDevicesPerClaim := 0

		// 遍历请求；subRequestIndex 仅对子请求有意义。
		for requestIndex := range claim.Spec.Devices.Requests {
			request := &claim.Spec.Devices.Requests[requestIndex]
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			// firstAvailable 请求含多个子请求，实际设备数取决于最终选中的子请求。
			hasSubRequests := len(request.FirstAvailable) > 0

			// 未启用 DRAPrioritizedList 时，含子请求的声明无法处理。
			if !a.features.PrioritizedList && hasSubRequests {
				return nil, fmt.Errorf("claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name)
			}

			// 未启用 DRAConsumableCapacity 时，携带容量请求的声明无法处理。
			if !a.features.ConsumableCapacity {
				if request.Exactly != nil && request.Exactly.Capacity != nil {
					return nil, fmt.Errorf("claim %s, request %s: has capacity requests, but the DRAConsumableCapacity feature is disabled",
						klog.KObj(claim), request.Name)
				}
				// 逐个检查子请求中的容量字段。
				for _, subReq := range request.FirstAvailable {
					if subReq.Capacity != nil {
						return nil, fmt.Errorf("claim %s, subrequest %s: has capacity requests, but the DRAConsumableCapacity feature is disabled",
							klog.KObj(claim), subReq.Name)
					}
				}
			}

			// 按分配模式统计每个请求需要的设备数量。
			var minDevicesPerRequest int
			// 含子请求时先用 MaxInt 初始化，再取各子请求设备数的最小值。
			if hasSubRequests {
				minDevicesPerRequest = math.MaxInt
				// requestData 为每个子请求保存独立条目。
				// 遍历子请求并记录各自校验结果，取设备数最小者作为下界。
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
				// 无子请求时按精确请求校验一次。
				reqData, err := alloc.validateDeviceRequest(&exactDeviceRequestAccessor{request: request}, nil, requestKey, pools)
				if err != nil {
					return nil, err
				}
				alloc.requestData[requestKey] = reqData
				minDevicesPerRequest = reqData.numDevices
			}
			// 用差值与上限比较，避免设备总数溢出。
			if minDevicesPerRequest > resourceapi.AllocationResultsMaxSize-minDevicesPerClaim {
				return nil, fmt.Errorf("claim %s, request %s: adding %d devices exceeds the claim limit of %d, with %d already accounted for", klog.KObj(claim), request.Name, minDevicesPerRequest, resourceapi.AllocationResultsMaxSize, minDevicesPerClaim)
			}
			minDevicesPerClaim += minDevicesPerRequest
		}
		// 该声明至少需要的设备数；仅为下界，子请求可能分配更多。
		alloc.logger.V(6).Info("Checked claim", "claim", klog.KObj(claim), "minDevices", minDevicesPerClaim)

		// 结果切片按下界预留容量；后续分配会再次检查上限。
		alloc.result[claimIndex].devices = make([]internalDeviceResult, 0, minDevicesPerClaim)

		// 约束被假定为单调：一旦返回 false，继续添加设备也不会重新满足，故可提前剪枝。
		constraints := make([]constraint, len(claim.Spec.Devices.Constraints))
		// 把声明中的约束转换为内部约束对象。
		for i, constraint := range claim.Spec.Devices.Constraints {
			switch {
			// matchAttribute 约束：约束内所有设备在该属性上取值相同。
			case constraint.MatchAttribute != nil:
				matchAttribute := resourceapi.FullyQualifiedName(*constraint.MatchAttribute)
				logger := alloc.logger
				if loggerV := alloc.logger.V(6); loggerV.Enabled() {
					logger = klog.LoggerWithName(logger, "matchAttributeConstraint")
					logger = klog.LoggerWithValues(logger, "matchAttribute", matchAttribute)
				}
				m := &matchAttributeConstraint{
					logger:        logger,
					requestNames:  sets.New(constraint.Requests...),
					attributeName: matchAttribute,
				}
				constraints[i] = m
			// distinctAttribute 约束：约束内所有设备在该属性上取值互不相同。
			case constraint.DistinctAttribute != nil:
				distinctAttribute := resourceapi.FullyQualifiedName(*constraint.DistinctAttribute)
				logger := alloc.logger
				if loggerV := alloc.logger.V(6); loggerV.Enabled() {
					logger = klog.LoggerWithName(logger, "distinctAttributeConstraint")
					logger = klog.LoggerWithValues(logger, "distinctAttribute", distinctAttribute)
				}
				m := &distinctAttributeConstraint{
					logger:        logger,
					requestNames:  sets.New(constraint.Requests...),
					attributeName: distinctAttribute,
				}
				constraints[i] = m
			// 未知约束类型无法处理，直接报错。
			default:
				return nil, fmt.Errorf("claim %s, constraint #%d: empty constraint (unsupported constraint type?)", klog.KObj(claim), i)
			}
		}
		alloc.constraints[claimIndex] = constraints
		// 累加所有声明的设备数下界。
		minDevicesTotal += minDevicesPerClaim
	}

	// 设备与请求的匹配结果只依赖静态信息，可在本次 Allocate 内缓存复用。
	alloc.deviceMatchesRequest = make(map[matchKey]bool)

	// 按下界预估 allocatingDevices 映射的容量。
	alloc.allocatingDevices = make(map[DeviceID]sets.Set[int], minDevicesTotal)

	alloc.logger.V(6).Info("Gathered information about devices", "numAllocatedDevices", len(alloc.allocatedState.AllocatedDevices), "minDevicesToBeAllocated", minDevicesTotal)

	// 从第一个请求开始递归搜索。
	done, err := alloc.allocateOne(deviceIndices{}, false, deviceLocation{})
	// errStop 表示无需继续分配，返回空结果。
	if errors.Is(err, errStop) {
		return nil, nil
	}
	// 其他错误直接向上返回。
	if err != nil {
		return nil, err
	}
	// 未找到任何完整解。
	if !done {
		// 若遇到无效池，上报 ErrFailedAllocationOnNode，允许其他节点继续尝试分配。
		for _, pool := range pools {
			if pool.IsInvalid {
				return nil, fmt.Errorf("invalid resource pools were encountered%w", internal.ErrFailedAllocationOnNode)
			}
		}
		// 无无效池且确无解，返回空结果。
		return nil, nil
	}

	// 将内部结果转换为 API 层的 AllocationResult。
	result := make([]resourceapi.AllocationResult, len(alloc.result))
	for claimIndex, internalResult := range alloc.result {
		claim := alloc.claimsToAllocate[claimIndex]
		allocationResult := &result[claimIndex]
		// 逐设备填充请求名、驱动/池/设备 ID、管理员访问、容忍与份额 ID。
		allocationResult.Devices.Results = make([]resourceapi.DeviceRequestAllocationResult, len(internalResult.devices))
		for i, internal := range internalResult.devices {
			// 深拷贝已消耗容量，避免与其他结果共享底层 map。
			var consumedCapacity map[resourceapi.QualifiedName]resource.Quantity
			if internal.consumedCapacity != nil {
				consumedCapacity = make(map[resourceapi.QualifiedName]resource.Quantity, len(internal.consumedCapacity))
				for key, val := range internal.consumedCapacity {
					consumedCapacity[key] = val.DeepCopy()
				}
			}
			// 构造单条设备分配结果。
			allocationResult.Devices.Results[i] = resourceapi.DeviceRequestAllocationResult{
				Request:          internal.requestName(),
				Driver:           internal.id.Driver.String(),
				Pool:             internal.id.Pool.String(),
				Device:           internal.id.Device.String(),
				AdminAccess:      internal.adminAccess,
				Tolerations:      internal.lookupRequest(claim).tolerations(),
				ShareID:          internal.shareID,
				ConsumedCapacity: consumedCapacity,
			}
			// 特性关闭时选中设备不应带绑定条件，故跳过拷贝；不影响正确性。
			if a.features.DeviceBindingAndStatus {
				allocationResult.Devices.Results[i].BindingConditions = internal.BindingConditions
				allocationResult.Devices.Results[i].BindingFailureConditions = internal.BindingFailureConditions
			}
		}

		// 类配置只添加一次：configRange 记录类配置在 Devices.Config 中的起止下标，
		// configIndexesForClass 以类名为键保存该区间。
		type configRange struct{ start, end int }
		configIndexesForClass := make(map[string]configRange) // Key 为类名，Value 为该类的配置在 allocationResult.Devices.Config 中的位置。
		for requestIndex := range claim.Spec.Devices.Requests {
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			// 取出该请求的校验数据。
			requestData := alloc.requestData[requestKey]
			// 若为子请求，改用最终被选中的子请求所属的设备类。
			if requestData.parentRequest != nil {
				requestKey.subRequestIndex = requestData.selectedSubRequestIndex
				requestData = alloc.requestData[requestKey]
			}

			class := requestData.class
			if class == nil {
				continue
			}
			// 该类的配置已添加过，只需把当前请求名追加到已有配置项。
			configIndexes, exists := configIndexesForClass[class.Name]
			if exists {
				for i := configIndexes.start; i < configIndexes.end; i++ {
					allocationResult.Devices.Config[i].Requests = append(allocationResult.Devices.Config[i].Requests, requestData.requestName())
				}
				continue
			}

			// 首次遇到该类时，把其全部配置加入结果。
			initialConfigLen := len(allocationResult.Devices.Config)
			for _, config := range class.Spec.Config {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClass,
					Requests:            []string{requestData.requestName()},
					DeviceConfiguration: config.DeviceConfiguration,
				})
			}
			// 记录该类配置在结果中的下标区间。
			configIndexesForClass[class.Name] = configRange{start: initialConfigLen, end: len(allocationResult.Devices.Config)}
		}
		for _, config := range claim.Spec.Devices.Config {
			// Requests 为空表示作用于全部请求，直接加入结果。
			if len(config.Requests) == 0 {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClaim,
					Requests:            config.Requests,
					DeviceConfiguration: config.DeviceConfiguration,
				})
				continue
			}

			// 仅当配置列出的请求名命中当前遍历的请求时才加入。
			for i, request := range claim.Spec.Devices.Requests {
				if slices.Contains(config.Requests, request.Name) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					// 命中后停止遍历：该配置会整体加入。
					break // 首次匹配后即可停止检查，因为配置中的所有请求都应被加入。
				}

				// 子请求需使用“父请求/子请求”的全名进行匹配。
				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: i}
				requestData := alloc.requestData[requestKey]
				if requestData.parentRequest == nil {
					continue
				}

				subRequest := request.FirstAvailable[requestData.selectedSubRequestIndex]
				subRequestName := fmt.Sprintf("%s/%s", request.Name, subRequest.Name)
				// 命中子请求全名。
				if slices.Contains(config.Requests, subRequestName) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					// 同上，命中后停止遍历。
					break // 首次匹配后即可停止检查，因为配置中的所有请求都应被加入。
				}
			}
		}

		// 若某配置实际覆盖全部请求，清空其 Requests 以利用“空即全部”语义。
		for i := range allocationResult.Devices.Config {
			requests := allocationResult.Devices.Config[i].Requests
			if len(requests) == 0 {
				continue
			}
			// 检查是否存在任一请求未被该配置覆盖。
			appliesToAll := true
			for requestIndex, request := range claim.Spec.Devices.Requests {
				if slices.Contains(requests, request.Name) {
					continue
				}
				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
				requestData := alloc.requestData[requestKey]
				// 子请求场景下改用“父请求/子请求”全名判断。
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
			// 覆盖全部请求，清空 Requests。
			if appliesToAll {
				allocationResult.Devices.Config[i].Requests = nil
			}
		}

		// 根据设备结果计算并写入节点选择器。
		nodeSelector, err := alloc.createNodeSelector(internalResult.devices, node.Name)
		if err != nil {
			return nil, fmt.Errorf("create NodeSelector for claim %s: %w", claim.Name, err)
		}
		allocationResult.NodeSelector = nodeSelector
	}

	// 返回所有声明的分配结果。
	return result, nil
}

// GetStats 返回分配器统计信息。
func (a *Allocator) GetStats() Stats {
	// 构造统计结果。
	s := Stats{
		// 原子读取 allocateOne 的调用次数。
		NumAllocateOneInvocations: a.numAllocateOneInvocations.Load(),
	}
	return s
}

// validateDeviceRequest 校验单个（子）请求，收集其设备类、所需设备数以及 “all” 模式下的候选设备。
// requestKey 标明请求在声明中的位置；pools 用于 “all” 模式枚举设备。
func (alloc *allocator) validateDeviceRequest(request requestAccessor, parentRequest requestAccessor, requestKey requestIndices, pools []*Pool) (requestData, error) {
	// 当前处理的声明。
	claim := alloc.claimsToAllocate[requestKey.claimIndex]
	requestData := requestData{
		request:       request,
		parentRequest: parentRequest,
	}
	// 派生属性目前仅支持实验变体，incubating 出现即报错；待支持范围扩展后再实现。
	if len(request.derivedAttributes()) > 0 {
		return requestData, fmt.Errorf("claim %s, request %s: cannot handle request with derived attributes, the feature is disabled", klog.KObj(claim), request.name())
	}
	// 目前仅支持 CEL 选择器。
	for i, selector := range request.selectors() {
		if selector.CEL == nil {
			return requestData, fmt.Errorf("claim %s, request %s, selector #%d: CEL expression empty (unsupported selector type?)", klog.KObj(claim), request.name(), i)
		}
	}

	// 未启用 AdminAccess 特性时拒绝管理员访问请求。
	if !alloc.features.AdminAccess && request.hasAdminAccess() {
		return requestData, fmt.Errorf("claim %s, request %s: admin access is requested, but the feature is disabled", klog.KObj(claim), request.name())
	}

	// 请求必须指定设备类名。
	if request.deviceClassName() == "" {
		return requestData, fmt.Errorf("claim %s, request %s: missing device class name (unsupported request type?)", klog.KObj(claim), request.name())
	}
	// 从设备类查询器获取设备类，失败则终止。
	class, err := alloc.classLister.Get(request.deviceClassName())
	if err != nil {
		return requestData, fmt.Errorf("claim %s, request %s: could not retrieve device class %s: %w", klog.KObj(claim), request.name(), request.deviceClassName(), err)
	}

	// 必须先写入设备类，后续 isSelectable 才能使用。
	requestData.class = class

	// 按分配模式分别处理。
	switch request.allocationMode() {
	// ExactCount：校验数量并转换为 int。
	case resourceapi.DeviceAllocationModeExactCount:
		numDevices := request.count()
		// API 校验允许超大数值，但此处无法表示。
		if numDevices > math.MaxInt {
			return requestData, fmt.Errorf("claim %s, request %s: exact count %d is too large", klog.KObj(claim), request.name(), numDevices)
		}
		requestData.numDevices = int(numDevices)
	// All：预先枚举所有符合条件的设备。
	case resourceapi.DeviceAllocationModeAll:
		// “all” 模式要求池完整且有效，并只计入静态容量足够的设备。
		requestData.allDevices = make([]deviceWithID, 0, resourceapi.AllocationResultsMaxSize)
		// emptyConsumedCapacity 可复用：CmpRequestOverCapacity 只读取或克隆它。
		emptyConsumedCapacity := NewConsumedCapacity() // 可复用：CmpRequestOverCapacity 仅做克隆/读取
		// 遍历每个池。
		for _, pool := range pools {
			// 池不完整时无法枚举全部设备，直接报错。当前普通模式仍可尝试不完整池；
			// 未来引入评分后应一律停等，以免因可见设备有限而错过更优解。
			if pool.IsIncomplete {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently being updated", klog.KObj(claim), request.name(), pool.PoolID)
			}
			// 池无效时同样无法枚举全部设备。
			if pool.IsInvalid {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently invalid", klog.KObj(claim), request.name(), pool.PoolID)
			}
			// 遍历池中定向到目标节点的切片及其设备。
			for _, slice := range pool.DeviceSlicesTargetingNode {
				for deviceIndex := range slice.Spec.Devices {
					// 先判断设备是否被选择器接受。
					selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
					if err != nil {
						return requestData, err
					}
					if selectable {
						// 构造设备 ID 与内部设备表示。
						device := deviceWithID{
							id:     DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name},
							Device: &slice.Spec.Devices[deviceIndex],
							slice:  slice,
							pool:   pool,
						}
						// 只检查静态容量：剩余容量留到 allocateDevice 再查，故容量不足设备仍留在 allDevices 中，
						// 最终使 “all” 模式因无法分配全部设备而失败。
						if alloc.features.ConsumableCapacity {
							apiDevice := slice.Spec.Devices[deviceIndex]
							// 用空已消耗容量与请求容量比较设备静态容量。
							success, err := CmpRequestOverCapacity(emptyConsumedCapacity, requestData.request.capacities(),
								apiDevice.AllowMultipleAllocations, apiDevice.Capacity, emptyConsumedCapacity, alloc.features.FractionalCapacityRange)
							if err != nil {
								return requestData, fmt.Errorf("claim %s, request %s: checking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), apiDevice.Name, err)
							}
							// 静态容量不足则跳过该设备。
							if !success {
								alloc.logger.V(7).Info("Device static capacity insufficient", "device", apiDevice)
								continue
							}
						}
						// 记录匹配且静态容量足够的候选设备。
						requestData.allDevices = append(requestData.allDevices, device)
					}
				}
			}
		}
		// “all” 模式所需设备数即候选设备数量。
		requestData.numDevices = len(requestData.allDevices)
		alloc.logger.V(6).Info("Request for 'all' devices", "claim", klog.KObj(claim), "request", request.name(), "numDevicesPerRequest", requestData.numDevices)
	// 未知分配模式，无法处理。
	default:
		return requestData, fmt.Errorf("claim %s, request %s: unsupported count mode %s", klog.KObj(claim), request.name(), request.allocationMode())
	}
	return requestData, nil
}

// errStop 是内部哨兵错误，表示分配已被判定为不可能成功，调用方应返回空结果。
var errStop = errors.New("stop allocation")

// errAllocationResultMaxSizeExceeded 表示结果数量超出上限；上层据此更积极地回溯。
var errAllocationResultMaxSizeExceeded = errors.New("allocation max size exceeded")

// allocator 保存单次 Allocate 期间的可变状态；仅由单个 goroutine 访问，无需加锁。
// 字段说明：嵌入的 Allocator 提供池与缓存；ctx/logger/node 为上下文、日志器与目标节点；
// claimsToAllocate/pools 为待分配声明与候选池；deviceMatchesRequest 缓存匹配判定；
// constraints 为每个声明保存一组约束对象。
type allocator struct {
	*Allocator
	ctx                  context.Context
	logger               klog.Logger
	node                 *v1.Node
	claimsToAllocate     []*resourceapi.ResourceClaim
	pools                []*Pool
	deviceMatchesRequest map[matchKey]bool
	constraints          [][]constraint // 每个 claim 对应一个约束列表
	// consumedCounters 记录当前尝试中设备消耗的计数器，按池 ID 索引；
	// requestData 为每个（子）请求保存一份校验数据。
	consumedCounters map[PoolID]counterSets
	requestData      map[requestIndices]requestData // 无子请求的每个 request 一项，每个子请求一项
	// allocatingDevices 记录本次尝试中新分配的设备，值为使用它的声明下标集合；
	// allocatingCapacity 记录本次尝试中设备被占用的容量；result 保存每个声明的内部结果。
	allocatingDevices  map[DeviceID]sets.Set[int]
	allocatingCapacity ConsumedCapacityCollection
	result             []internalAllocationResult
}

// counterSets 的键为计数器集名称，值为集内计数器名到 Counter 的映射。
type counterSets map[draapi.UniqueString]map[string]resourceapi.Counter

// matchKey 由设备 ID 与请求下标组成，用作设备/请求匹配结果的缓存键。
type matchKey struct {
	DeviceID
	requestIndices
}

// requestIndices 用声明下标、请求下标与子请求下标唯一定位一个请求或子请求。
type requestIndices struct {
	claimIndex, requestIndex int
	subRequestIndex          int
}

// deviceIndices 定位某个请求或子请求内部待分配的第 N 台设备。
type deviceIndices struct {
	// 四个下标依次定位：声明在 claimsToAllocate 中的位置、请求在声明中的位置、
	// 子请求在父请求中的位置（subRequest 为 false 时忽略）以及设备在请求或子请求中的位置。
	claimIndex      int // claim 在分配器中的索引。
	requestIndex    int // request 在 claim 中的索引。
	subRequestIndex int // 子请求在 request 中的索引（subRequest 为 false 时忽略）。
	deviceIndex     int // 设备在 request 或子请求中的索引。
}

// deviceLocation 用池、切片、设备三级下标定位设备在池列表中的位置。
type deviceLocation struct {
	poolIndex   int
	sliceIndex  int
	deviceIndex int
}

// requestData 保存单个（子）请求经校验后得到的静态信息。
type requestData struct {
	// request 为需要分配的请求或子请求，永不为 nil。
	request requestAccessor
	// parentRequest 为子请求的父请求，普通请求为 nil；class 为设备类；numDevices 为该请求需要的设备数量。
	parentRequest requestAccessor
	class         *resourceapi.DeviceClass
	numDevices    int

	// selectedSubRequestIndex 是最终选中的子请求下标，仅设置在 subRequestIndex == 0 的条目上。
	selectedSubRequestIndex int

	// allDevices 为 “all” 模式预先确定的候选设备集合。
	allDevices []deviceWithID
}

// requestName 返回请求全名；子请求使用“父请求/子请求”形式。
func (rd *requestData) requestName() string {
	// 子请求拼接父请求名。
	if rd.parentRequest != nil {
		return fmt.Sprintf("%s/%s", rd.parentRequest.name(), rd.request.name())
	}
	// 普通请求直接返回自身名称。
	return rd.request.name()
}

// deviceWithID 把设备对象与它在切片、池中的位置及 ID 绑定在一起。
type deviceWithID struct {
	// 嵌入 draapi.Device；id 为设备标识；slice 为所属切片；pool 为所属池。
	*draapi.Device
	id    DeviceID
	slice *draapi.ResourceSlice
	pool  *Pool
}

// internalAllocationResult 保存单个声明的内部分配结果。
type internalAllocationResult struct {
	// devices 为该声明实际分配到的设备列表。
	devices []internalDeviceResult
}

// internalDeviceResult 是单台设备在分配结果中的内部表示。
type internalDeviceResult struct {
	// 嵌入 draapi.Device；request/parentRequest 为请求名与父请求名；id 为设备标识；
	// shareID 为共享份额 ID；slice 为所属切片；consumedCapacity 为消耗的容量；adminAccess 为管理员访问标记。
	*draapi.Device
	request          string // request（无子请求时）或子请求的名称
	parentRequest    string // 包含该子请求的 request 名称，否则为空
	id               DeviceID
	shareID          *types.UID
	slice            *draapi.ResourceSlice
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity
	adminAccess      *bool
}

// requestName 返回设备结果关联的请求全名，子请求带父请求前缀。
func (idr internalDeviceResult) requestName() string {
	// 无父请求时直接返回请求名。
	if idr.parentRequest == "" {
		return idr.request
	}
	// 子请求拼接为“父请求/子请求”。
	return fmt.Sprintf("%s/%s", idr.parentRequest, idr.request)
}

// lookupRequest 在声明中查找该设备结果对应的请求访问器。
func (idr internalDeviceResult) lookupRequest(claim *resourceapi.ResourceClaim) requestAccessor {
	// 子请求场景下先按父请求名定位。
	requestName := idr.request
	if idr.parentRequest != "" {
		requestName = idr.parentRequest
	}
	for i := range claim.Spec.Devices.Requests {
		request := &claim.Spec.Devices.Requests[i]
		// 名称不匹配则继续查找。
		if request.Name != requestName {
			continue
		}
		// 普通请求无需检查子请求。
		if idr.parentRequest == "" {
			return &exactDeviceRequestAccessor{request}
		}
		// 遍历子请求寻找与结果匹配的一项。
		for j := range request.FirstAvailable {
			subRequest := &request.FirstAvailable[j]
			// 子请求名不匹配则继续。
			if subRequest.Name != idr.request {
				continue
			}
			return &deviceSubRequestAccessor{subRequest}
		}
	}
	// 未找到匹配请求，返回 nil。
	return nil
}

// constraint 是声明级约束接口：add 在设备加入前校验并记录，remove 撤销一次成功的 add。
type constraint interface {
	// add 返回 false 表示设备违反约束，不能加入。
	add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool

	// remove 的参数必须与对应 add 完全一致，并严格一一配对。
	remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID)
}

// matchAttributeConstraint 实现 matchAttribute 约束：约束内所有设备在某属性上取值相同；
// 只需记录首台设备的属性值与已接受设备数量。
type matchAttributeConstraint struct {
	// logger 记录约束名与属性名；requestNames 为受约束的请求名集合；attributeName 为属性全限定名。
	logger        klog.Logger // 已包含名称与属性名，日志消息中无需重复。
	requestNames  sets.Set[string]
	attributeName resourceapi.FullyQualifiedName

	// attribute 为首台设备确立的基准属性值；numDevices 为已接受的设备数量。
	attribute  *resourceapi.DeviceAttribute
	numDevices int
}

// add 校验设备是否满足 matchAttribute 约束，满足时记录其属性值并增加计数。
func (m *matchAttributeConstraint) add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool {
	// 不受约束影响的请求直接通过。
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		m.logger.V(7).Info("Constraint does not apply to request", "request", requestName)
		return true
	}

	// 从设备上查找目标属性。
	attribute := lookupAttribute(device, deviceID, m.attributeName)
	// 缺少属性则不满足约束。
	if attribute == nil {
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false
	}

	// 首台设备可直接加入，并把它的属性值设为基准。
	if m.numDevices == 0 {
		m.attribute = attribute
		m.numDevices = 1
		m.logger.V(7).Info("First in set")
		return true
	}

	// 后续设备必须与基准值严格相等。
	switch {
	// 字符串值比较。
	case attribute.StringValue != nil:
		if m.attribute.StringValue == nil || *attribute.StringValue != *m.attribute.StringValue {
			m.logger.V(7).Info("String values different")
			return false
		}
	// 整数值比较。
	case attribute.IntValue != nil:
		if m.attribute.IntValue == nil || *attribute.IntValue != *m.attribute.IntValue {
			m.logger.V(7).Info("Int values different")
			return false
		}
	// 布尔值比较。
	case attribute.BoolValue != nil:
		if m.attribute.BoolValue == nil || *attribute.BoolValue != *m.attribute.BoolValue {
			m.logger.V(7).Info("Bool values different")
			return false
		}
	// 版本值比较：semver 2.0.0 要求最小形式，可直接做字符串相等比较。
	case attribute.VersionValue != nil:
		if m.attribute.VersionValue == nil || *attribute.VersionValue != *m.attribute.VersionValue {
			m.logger.V(7).Info("Version values different")
			return false
		}
	// 未知值类型无法匹配。
	default:
		m.logger.V(7).Info("Match attribute type unknown")
		return false
	}

	// 匹配成功，计数加一。
	m.numDevices++
	m.logger.V(7).Info("Constraint satisfied by device", "device", deviceID, "numDevices", m.numDevices)
	return true
}

// remove 撤销一次成功的 add：只减少计数，基准属性保持不变。
func (m *matchAttributeConstraint) remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) {
	// 不受约束影响的请求无需回滚。
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		return
	}

	m.numDevices--
	m.logger.V(7).Info("Device removed from constraint set", "device", deviceID, "numDevices", m.numDevices)
}

// matches 判断请求名或子请求全名是否落在约束的作用范围内。
func (m *matchAttributeConstraint) matches(requestName, subRequestName string) bool {
	// 普通请求按请求名匹配。
	if subRequestName == "" {
		return m.requestNames.Has(requestName)
	} else {
		// 子请求同时匹配请求名与“父请求/子请求”全名。
		fullSubRequestName := fmt.Sprintf("%s/%s", requestName, subRequestName)
		return m.requestNames.Has(requestName) || m.requestNames.Has(fullSubRequestName)
	}
}

// lookupAttribute 按全限定名查找设备属性：先尝试全名精确匹配；
// 若域名部分与设备驱动相同，再尝试仅用属性 ID 匹配。
func lookupAttribute(device *draapi.Device, deviceID DeviceID, attributeName resourceapi.FullyQualifiedName) *resourceapi.DeviceAttribute {
	// 先按全限定名精确查找。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName)]; ok {
		return &attr
	}
	// 全限定名应形如“域名/属性名”。
	index := strings.Index(string(attributeName), "/")
	// 无斜杠说明名称非法。
	if index < 0 {
		return nil
	}

	// 域名与设备驱动不一致，属性不存在。
	if string(attributeName[0:index]) != deviceID.Driver.String() {
		return nil
	}

	// 域名与驱动相同，用属性 ID 再查一次。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName[index+1:])]; ok {
		return &attr
	}

	return nil
}

// allocateOne 为当前待分配的单台设备递归尝试所有合格候选。
// 合格条件：未被占用、匹配选择器、满足约束且容量足够。startLocation 指定候选遍历起点，
// 用于避免同一请求内不同排列的重复搜索；同一请求内顺序无关，但跨请求与声明的顺序有意义。
// r.claimIndex 越界表示全部分配完成。
func (alloc *allocator) allocateOne(r deviceIndices, allocateSubRequest bool, startLocation deviceLocation) (bool, error) {
	// 统计递归调用次数。
	alloc.numAllocateOneInvocations.Add(1)

	// 及时响应上下文取消。
	if alloc.ctx.Err() != nil {
		return false, fmt.Errorf("filter operation aborted: %w", context.Cause(alloc.ctx))
	}

	// 所有声明处理完毕：无评分逻辑，直接采用首个完整解。
	if r.claimIndex >= len(alloc.claimsToAllocate) {
		alloc.logger.V(6).Info("Allocation result found")
		return true, nil
	}

	// 当前正在处理的声明。
	claim := alloc.claimsToAllocate[r.claimIndex]
	// 当前声明的所有请求已完成，进入下一声明。
	if r.requestIndex >= len(claim.Spec.Devices.Requests) {
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex + 1}, false, deviceLocation{})
		// 超限不是致命错误：更换前置分配后，子请求可能只需更少设备而成功。
		if errors.Is(err, errAllocationResultMaxSizeExceeded) {
			return false, nil
		}
		return success, err
	}

	// 根据递归层级定位当前（子）请求的校验数据。
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]

	// firstAvailable 请求只需选中一个子请求即可进入下一请求；此分支仅在遇到首个子请求时进入。
	if !allocateSubRequest && requestData.parentRequest != nil {
		// 记录是否所有子请求都因结果数量超限而失败；此时通常可停止尝试上一请求的其他设备组合。
		// 但 firstAvailable 的某些子请求可能需要更少设备，不能据此提前判定整个搜索无解。
		allAllocationExceeded := true
		// 依次尝试每个子请求。
		for subRequestIndex := 0; ; subRequestIndex++ {
			nextSubRequestKey := requestKey
			nextSubRequestKey.subRequestIndex = subRequestIndex
			// 下标越界表示所有子请求均已尝试完毕。
			if _, ok := alloc.requestData[nextSubRequestKey]; !ok {
				// 全部因超限失败时上抛该错误，以便上层更积极地回溯。
				if allAllocationExceeded {
					return false, errAllocationResultMaxSizeExceeded
				}
				return false, nil
			}

			// 传入 allocateSubRequest=true，防止对同一子请求无限递归。
			r.subRequestIndex = subRequestIndex
			success, err := alloc.allocateOne(r, true /* 防止无限递归 */, deviceLocation{})
			// 超限则尝试下一个可能需要更少设备的子请求。
			if errors.Is(err, errAllocationResultMaxSizeExceeded) {
				continue
			}
			// 出现非超限失败时清除标志。
			allAllocationExceeded = false
			if err != nil {
				return false, err
			}
			// 某个子请求成功即停止尝试其余子请求。
			if success {
				// 记录被选中的子请求下标。
				requestData.selectedSubRequestIndex = subRequestIndex
				alloc.requestData[requestKey] = requestData
				return true, nil
			}
		}
	}

	// 当前（子）请求及其分配模式。
	request := requestData.request
	// doAllDevices 表示“all”分配模式。
	doAllDevices := request.allocationMode() == resourceapi.DeviceAllocationModeAll

	// “all” 模式必须至少有一台候选设备。
	if doAllDevices && len(requestData.allDevices) == 0 {
		alloc.logger.V(6).Info("Allocation for 'all' devices didn't succeed: no devices found", "claim", klog.KObj(claim), "request", requestData.request.name())
		return false, nil
	}

	// r.deviceIndex 达到 numDevices 表示当前请求已完成。
	if r.deviceIndex >= requestData.numDevices {
		// 进入下一请求；此处不再沿用 startLocation。
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex + 1}, false, deviceLocation{})
		// 超限错误需要上抛，以便回溯到更早的分配决策。
		return success, err
	}

	// 用差值而非加法防止溢出。
	// 本请求还需要的设备数。
	remaining := requestData.numDevices - r.deviceIndex
	// 当前声明的结果列表还可容纳的设备数。
	available := resourceapi.AllocationResultsMaxSize - len(alloc.result[r.claimIndex].devices)
	// 剩余需求超过可用空间时触发激进回溯。
	if remaining > available {
		return false, errAllocationResultMaxSizeExceeded
	}

	// 记录当前分配进度，便于诊断。
	alloc.logger.V(6).Info("Allocating one device", "currentClaim", r.claimIndex,
		"totalClaims", len(alloc.claimsToAllocate), "currentRequest", r.requestIndex,
		"currentSubRequest", r.subRequestIndex, "totalRequestsPerClaim", len(claim.Spec.Devices.Requests),
		"currentDevice", r.deviceIndex, "devicesPerRequest", requestData.numDevices, "allDevices", doAllDevices, "adminAccess", request.adminAccess(), "capacities", request.capacities())

	// “all” 模式：候选设备已预先确定，只需逐台确认可用性。
	if doAllDevices {
		// 取下标的候选设备。
		deviceWithID := requestData.allDevices[r.deviceIndex]
		// must=true：调用方只要这台设备，失败时给出更详细的错误。
		success, deallocate, err := alloc.allocateDevice(r, deviceWithID, true)
		if err != nil {
			return false, err
		}
		// 分配顺序无关，当前设备失败即整体无解。
		if !success {
			return false, nil
		}
		// 只尝试一种组合，无需传递 startLocation。
		done, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex, deviceIndex: r.deviceIndex + 1}, allocateSubRequest, deviceLocation{})
		// 后续失败时需回退当前设备的分配。
		if err != nil || !done {
			deallocate()
			return false, err
		}
		return done, nil
	}

	// 非 “all” 模式：按池、切片、设备的顺序枚举候选。
	for poolIndex := startLocation.poolIndex; poolIndex < len(alloc.pools); poolIndex++ {
		// startLocation 决定起始搜索位置。
		pool := alloc.pools[poolIndex]
		// 跳过不完整或无效的池，但暂不报错，其他池可能仍有可用设备。
		if pool.IsIncomplete || pool.IsInvalid {
			continue
		}
		// 跨池后从新池的第一个切片开始。
		sliceStart := 0
		if poolIndex == startLocation.poolIndex {
			sliceStart = startLocation.sliceIndex
		}
		// 遍历切片。
		for sliceIndex := sliceStart; sliceIndex < len(pool.DeviceSlicesTargetingNode); sliceIndex++ {
			slice := pool.DeviceSlicesTargetingNode[sliceIndex]

			// 跨切片后从新切片的第一台设备开始。
			deviceStart := 0
			if poolIndex == startLocation.poolIndex &&
				sliceIndex == startLocation.sliceIndex {
				deviceStart = startLocation.deviceIndex
			}
			// 遍历切片内的设备。
			for deviceIndex := deviceStart; deviceIndex < len(slice.Spec.Devices); deviceIndex++ {
				// 构造设备 ID。
				deviceID := DeviceID{Driver: pool.Driver, Pool: pool.Pool, Device: slice.Spec.Devices[deviceIndex].Name}

				// “占用”检查开销小，优先执行。
				// 管理员访问要求同一声明内不重复使用同一设备。
				if request.adminAccess() && alloc.allocatingDeviceForClaim(deviceID, r.claimIndex) {
					alloc.logger.V(7).Info("Device in use in same claim", "device", deviceID)
					continue
				}
				// 普通请求要求设备未被任何声明占用。
				if !request.adminAccess() && alloc.deviceInUse(deviceID) {
					alloc.logger.V(7).Info("Device in use", "device", deviceID)
					continue
				}

				// 占用检查通过后检查选择器。
				requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
				// 匹配结果可缓存复用。
				selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
				if err != nil {
					return false, err
				}
				// 选择器不匹配则跳过。
				if !selectable {
					alloc.logger.V(7).Info("Device not selectable", "device", deviceID)
					continue
				}
				// 再检查容量是否足够。
				if alloc.features.ConsumableCapacity {
					device := slice.Spec.Devices[deviceIndex]
					// 比较请求容量与设备剩余容量。
					success, err := alloc.CmpRequestOverCapacity(requestData.request, slice, device)
					if err != nil {
						return false, fmt.Errorf("claim %s, request %s: checking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), deviceID, err)
					}
					// 容量不足则跳过。
					if !success {
						alloc.logger.V(7).Info("Device capacity not enough", "device", deviceID)
						continue
					}
				}

				// 全部检查通过后临时标记为已分配，继续递归。
				device := deviceWithID{
					id:     deviceID,
					Device: &slice.Spec.Devices[deviceIndex],
					slice:  slice,
					pool:   pool,
				}
				// 检查约束并更新临时状态。
				allocated, deallocate, err := alloc.allocateDevice(r, device, false)
				if err != nil {
					return false, err
				}
				// 设备被占用或违反约束。
				if !allocated {
					alloc.logger.V(7).Info("Device not usable", "device", deviceID)
					continue
				}
				// 下一台待分配设备的下标。
				deviceKey := deviceIndices{
					claimIndex:      r.claimIndex,
					requestIndex:    r.requestIndex,
					subRequestIndex: r.subRequestIndex,
					deviceIndex:     r.deviceIndex + 1,
				}
				// 下一台待分配设备的搜索起点。
				nextLocation := deviceLocation{
					poolIndex:   poolIndex,
					sliceIndex:  sliceIndex,
					deviceIndex: deviceIndex + 1,
				}
				// 递归分配本请求的下一台设备。
				done, err := alloc.allocateOne(deviceKey, allocateSubRequest, nextLocation)
				// 找到完整解立即返回。
				if err == nil && done {
					return done, nil
				}

				// 未找到解，撤销临时分配，以便尝试其他设备。
				deallocate()

				// 遇到错误则上抛，交由上层针对超限等情况处理。
				if err != nil {
					return false, err
				}
			}
		}
	}

	// 所有候选都失败，当前请求无解。
	return false, nil
}

// isSelectable 检查设备是否被特性开关、设备类选择器、请求选择器与节点选择规则接受；
// 结果按 matchKey 缓存以避免重复 CEL 求值。
func (alloc *allocator) isSelectable(r requestIndices, requestData requestData, slice *draapi.ResourceSlice, deviceIndex int) (bool, error) {
	// 当前待检查的设备。
	device := &slice.Spec.Devices[deviceIndex]
	// 特性关闭时不支持带绑定条件的设备。
	if !alloc.features.DeviceBindingAndStatus &&
		len(device.BindingConditions) > 0 {
		return false, nil
	}

	// 构造匹配缓存键。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name}
	matchKey := matchKey{DeviceID: deviceID, requestIndices: r}
	// 命中缓存直接返回。
	if matches, ok := alloc.deviceMatchesRequest[matchKey]; ok {
		return matches, nil
	}

	// 先检查设备类上的选择器。
	if requestData.class != nil {
		match, err := alloc.selectorsMatch(r, device, deviceID, requestData.class, requestData.class.Spec.Selectors)
		if err != nil {
			return false, err
		}
		// 类选择器不匹配。
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
	// 请求选择器不匹配。
	if !match {
		alloc.deviceMatchesRequest[matchKey] = false
		return false, nil
	}

	// 最后检查每设备节点选择规则。
	if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
		matches, err := NodeMatches(alloc.node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
		if err != nil {
			return false, err
		}
		// 节点不匹配。
		if !matches {
			alloc.deviceMatchesRequest[matchKey] = false
			return false, nil
		}
	}

	// 全部通过，缓存匹配结果。
	alloc.deviceMatchesRequest[matchKey] = true
	return true, nil

}

// CmpRequestOverCapacity 比较请求容量与设备剩余容量，同时考虑历史已分配容量与本次尝试占用的容量。
func (alloc *allocator) CmpRequestOverCapacity(request requestAccessor, slice *draapi.ResourceSlice, device draapi.Device) (bool, error) {
	// 构造设备 ID。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: device.Name}
	// 本次尝试中该设备已被请求的容量。
	allocatingCapacity := alloc.allocatingCapacity[deviceID]
	allowMultipleAllocations := device.AllowMultipleAllocations
	capacities := device.Capacity
	// 优先使用历史已聚合容量；否则按空容量处理。
	if allocatedCapacity, found := alloc.allocatedState.AggregatedCapacity[deviceID]; found {
		return CmpRequestOverCapacity(allocatedCapacity, request.capacities(), allowMultipleAllocations, capacities, allocatingCapacity, alloc.features.FractionalCapacityRange)
	}
	return CmpRequestOverCapacity(NewConsumedCapacity(), request.capacities(), allowMultipleAllocations, capacities, allocatingCapacity, alloc.features.FractionalCapacityRange)
}

// selectorsMatch 依次求值给定的 CEL 选择器，全部匹配返回 true。
// class 非空时，错误信息与日志按设备类上下文包装。
func (alloc *allocator) selectorsMatch(r requestIndices, device *draapi.Device, deviceID DeviceID, class *resourceapi.DeviceClass, selectors []resourceapi.DeviceSelector) (bool, error) {
	// 逐个选择器求值。
	for i, selector := range selectors {
		// 从缓存获取已编译表达式。
		expr := alloc.celCache.GetOrCompile(selector.CEL.Expression)
		// 新版本 API server 接受表达式后若集群降级，旧代码可能无法编译。
		// stored expression 通常可避免此情况，但运行代码仍可能比集群旧多个版本。
		if expr.Error != nil {
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d: CEL compile error: %w", class.Name, i, expr.Error)
			}
			return false, fmt.Errorf("claim %s: selector #%d: CEL compile error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, expr.Error)
		}

		// 把内部 Device 转换为 API Device 供 CEL 求值。
		// 转换结果。
		var d resourceapi.Device
		// 转换失败则终止。
		if err := draapi.Convert_api_Device_To_v1_Device(device, &d, nil); err != nil {
			return false, fmt.Errorf("convert Device %s: %w", deviceID, err)
		}
		// 求值选择器并记录实际成本。
		matches, details, err := expr.DeviceMatches(alloc.ctx, cel.Device{Driver: deviceID.Driver.String(), AllowMultipleAllocations: d.AllowMultipleAllocations, Attributes: d.Attributes, Capacity: d.Capacity})
		// 按设备类或请求的上下文记录日志。
		if class != nil {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "class", klog.KObj(class), "selector", i, "expression", selector.CEL.Expression, "matches", matches, "actualCost", ptr.Deref(details.ActualCost(), 0), "err", err)
		} else {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "claim", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), "selector", i, "expression", selector.CEL.Expression, "actualCost", ptr.Deref(details.ActualCost(), 0), "matches", matches, "err", err)
		}

		// 运行时错误经增强后上抛。
		if err != nil {
			err = cel.EnhanceRuntimeError(err)
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d on device %s: CEL runtime error: %w", class.Name, i, deviceID, err)
			}
			return false, fmt.Errorf("claim %s: selector #%d on device %s: CEL runtime error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, deviceID, err)
		}
		// 任一选择器不匹配即整体失败。
		if !matches {
			return false, nil
		}
	}

	// 全部匹配。
	return true, nil
}

// allocateDevice 校验单个候选设备是否可用：占用状态、计数器、污点、约束与容量。
// 仅分配成功且无错误时返回回滚闭包，供后续回溯恢复状态；must 为真时约束冲突会作为错误返回。
func (alloc *allocator) allocateDevice(r deviceIndices, device deviceWithID, must bool) (bool, func(), error) {
	// 当前声明与请求数据。
	claim := alloc.claimsToAllocate[r.claimIndex]
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]
	request := requestData.request
	// 是否允许多次分配（共享设备容量）。
	allowMultipleAllocations := false
	// 仅在特性启用时读取 AllowMultipleAllocations。
	if alloc.features.ConsumableCapacity {
		allowMultipleAllocations = device.AllowMultipleAllocations != nil && *device.AllowMultipleAllocations
	}
	// 独占设备：同一声明内不可重复使用。
	if !allowMultipleAllocations && request.adminAccess() && alloc.allocatingDeviceForClaim(device.id, r.claimIndex) {
		alloc.logger.V(7).Info("Device in use in same claim", "device", device.id)
		return false, nil, nil
	}
	// 普通请求：设备不可被任何声明占用。
	if !request.adminAccess() && alloc.deviceInUse(device.id) {
		alloc.logger.V(7).Info("Device in use", "device", device.id)
		return false, nil, nil
	}

	// 未启用 PartitionableDevices 时禁止分配消耗计数器的设备。
	if !alloc.features.PartitionableDevices && len(device.ConsumesCounters) > 0 {
		alloc.logger.V(7).Info("Device consumes counters, but the partitionable devices feature is not enabled", "device", device.id)
		return false, nil, nil
	}

	// 已在用的多实例设备不重复扣减计数器，其计数器已被计入。
	// 判断本次是否跳过计数器检查。
	skipCounterCheck := allowMultipleAllocations && alloc.deviceCapacityInUse(device.id)

	// 记录是否真正预留了计数器，供回滚使用。
	// 初始化计数器预留标志。
	countersReserved := false
	// 需要时校验计数器余量。
	if !skipCounterCheck && len(device.ConsumesCounters) > 0 {
		// 校验设备声明的计数器消耗是否可满足。
		// 校验并预留计数器。
		ok, err := alloc.checkAvailableCounters(device)
		if err != nil {
			return false, nil, err
		}
		// 计数器不足则放弃该候选。
		if !ok {
			alloc.logger.V(7).Info("Insufficient counters", "device", device.id)
			return false, nil, nil
		}
		// 预留成功。
		countersReserved = true
	}

	// 解析请求名与子请求名，供约束校验使用。
	var parentRequestName string
	var baseRequestName string
	var subRequestName string
	// 普通请求没有父请求名。
	if requestData.parentRequest == nil {
		baseRequestName = requestData.request.name()
	} else {
		// 子请求同时记录父请求名与自身名称。
		parentRequestName = requestData.parentRequest.name()
		baseRequestName = parentRequestName
		subRequestName = requestData.request.name()
	}

	// state 记录本次状态修改：拒绝路径同步调用 rollbackDevice，成功路径返回同一撤销逻辑的闭包供回溯使用。
	state := deviceRollbackState{
		countersReserved:   countersReserved,
		previousNumResults: len(alloc.result[r.claimIndex].devices),
	}

	// 特性开启时检查设备污点，未被容忍则回滚并拒绝；特性关闭时跳过检查。
	if alloc.features.DeviceTaints && taintPreventsAllocation(device.Device, request) {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
		return false, nil, nil
	}

	// 设备可用，开始检查声明级约束。
	// 逐个校验约束。
	for _, constraint := range alloc.constraints[r.claimIndex] {
		// 约束不满足则回滚已做修改。
		if !constraint.add(baseRequestName, subRequestName, device.Device, device.id) {
			alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
			// 必须拿到该设备时，约束冲突视为错误。
			if must {
				return false, nil, fmt.Errorf("claim %s, request %s: cannot add device %s because a claim constraint would not be satisfied", klog.KObj(claim), request.name(), device.id)
			}
			return false, nil, nil
		}
		// 记录已成功添加的约束数量。
		state.constraintsAdded++
	}

	// 约束全部满足，标记为占用并记录结果。
	alloc.logger.V(7).Info("Device allocated", "device", device.id)

	// 独占设备在本次尝试中登记占用。
	if !allowMultipleAllocations {
		// 首次使用时创建集合。
		if alloc.allocatingDevices[device.id] == nil {
			alloc.allocatingDevices[device.id] = make(sets.Set[int])
		}
		// 记录当前声明占用该设备。
		alloc.allocatingDevices[device.id].Insert(r.claimIndex)
		// 标记需要回滚设备占用。
		state.deviceMarked = true
	}

	// 准备容量与份额信息。
	consumedCapacity := make(map[resourceapi.QualifiedName]resource.Quantity, 0)
	var shareID *types.UID
	// 启用可消耗容量时再次校验容量。
	if alloc.features.ConsumableCapacity {
		// 二次校验容量，防止与前面的检查不一致。
		success, err := alloc.CmpRequestOverCapacity(requestData.request, device.slice, *device.Device)
		// 校验出错则回滚并上抛。
		if err != nil {
			alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
			return false, nil, fmt.Errorf("claim %s, request %s: rechecking capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), device.id, err)
		}
		// 容量不足则回滚。
		if !success {
			alloc.logger.V(7).Info("Device capacity not enough", "device", device)
			alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
			return false, nil, nil
		}

		// 多实例设备：计算本次消耗的容量并生成份额 ID。
		if allowMultipleAllocations {
			// 由请求容量与设备容量计算实际消耗。
			consumedCapacity, err = GetConsumedCapacityFromRequest(request.capacities(), device.Capacity, alloc.features.FractionalCapacityRange)
			// 计算失败则回滚。
			if err != nil {
				alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
				return false, nil, fmt.Errorf("claim %s, request %s: computing consumed capacity for device %s: %w", klog.KObj(claim), requestData.request.name(), device.id, err)
			}
			// 生成唯一份额 ID。
			shareID = GenerateNewShareID()
			// 记录本次容量分配。
			alloc.logger.V(7).Info("Device capacity allocated", "device", device.id,
				"consumed capacity", klog.Format(consumedCapacity))
			// 记录容量条目是否已存在：该条目兼作“已共享”标记，回滚时不能误删。
			// 判断此前是否已有容量条目。
			_, state.capacityEntryExisted = alloc.allocatingCapacity[device.id]
			// 累加本次消耗的容量。
			alloc.allocatingCapacity.Insert(NewDeviceConsumedCapacity(device.id, consumedCapacity))
			// 标记已插入容量。
			state.capacityInserted = true
			// 保存消耗容量供回滚使用。
			state.consumedCapacity = consumedCapacity
		}
	}

	// 构造内部设备结果。
	result := internalDeviceResult{
		request:       request.name(),
		parentRequest: parentRequestName,
		id:            device.id,
		Device:        device.Device,
		slice:         device.slice,
		shareID:       shareID,
	}
	// 记录管理员访问标记。
	if request.adminAccess() {
		result.adminAccess = ptr.To(request.adminAccess())
	}
	// 有容量消耗时写入结果。
	if len(consumedCapacity) > 0 {
		result.consumedCapacity = consumedCapacity
	}
	// 追加到当前声明的结果列表。
	alloc.result[r.claimIndex].devices = append(alloc.result[r.claimIndex].devices, result)
	// 标记结果已追加。
	state.resultAdded = true

	// 复制状态，避免其他路径把状态逃逸到堆。
	undo := state
	// 返回成功标记与回滚闭包。
	return true, func() {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, undo)
	}, nil
}

// 字段说明：countersReserved/constraintsAdded/deviceMarked 分别记录计数器预留、约束添加与设备占用是否已发生；
// capacityInserted/capacityEntryExisted 记录容量条目变化；resultAdded/previousNumResults 记录结果追加；
// consumedCapacity 保存本次消耗的容量。
type deviceRollbackState struct {
	countersReserved     bool
	constraintsAdded     int
	deviceMarked         bool
	capacityInserted     bool
	capacityEntryExisted bool
	resultAdded          bool
	previousNumResults   int
	consumedCapacity     map[resourceapi.QualifiedName]resource.Quantity
}

// rollbackDevice 按与施加相反的顺序撤销 state 中的修改，同时服务于失败回滚与回溯。
func (alloc *allocator) rollbackDevice(r deviceIndices, device deviceWithID, baseRequestName, subRequestName string, state deviceRollbackState) {
	// 撤销结果追加。
	if state.resultAdded {
		alloc.result[r.claimIndex].devices = alloc.result[r.claimIndex].devices[:state.previousNumResults]
	}
	// 撤销容量累加。
	if state.capacityInserted {
		alloc.allocatingCapacity.Remove(NewDeviceConsumedCapacity(device.id, state.consumedCapacity))
		// 若此前已有条目，需恢复空条目以保留“已共享”标记。
		if state.capacityEntryExisted {
			// 仅在条目被删除后恢复。
			if _, found := alloc.allocatingCapacity[device.id]; !found {
				alloc.allocatingCapacity[device.id] = NewConsumedCapacity()
			}
		}
	}
	// 撤销设备占用标记。
	if state.deviceMarked {
		alloc.allocatingDevices[device.id].Delete(r.claimIndex)
	}
	// 逆序撤销约束。
	for i := state.constraintsAdded - 1; i >= 0; i-- {
		alloc.constraints[r.claimIndex][i].remove(baseRequestName, subRequestName, device.Device, device.id)
	}
	// 撤销计数器预留。
	if state.countersReserved {
		alloc.deallocateCountersForDevice(device)
	}
	// 只有完整分配过才记录日志。
	if state.resultAdded {
		alloc.logger.V(7).Info("Device deallocated", "device", device.id)
	}
}

// taintPreventsAllocation 判断是否存在未被容忍且效果为 NoExecute/NoSchedule 的污点。
func taintPreventsAllocation(device *draapi.Device, request requestAccessor) bool {
	// 遍历设备污点。
	for _, taint := range device.Taints {
		// 仅已知效果会阻止分配。
		switch taint.Effect {
		// NoExecute 与 NoSchedule 需要检查容忍。
		case resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule:
			// 未被容忍则阻止分配。
			if !taintTolerated(taint, request) {
				return true
			}
		}
	}
	return false
}

// taintTolerated 判断请求的容忍列表是否覆盖给定污点。
func taintTolerated(taint resourceapi.DeviceTaint, request requestAccessor) bool {
	// 逐个检查容忍。
	for _, toleration := range request.tolerations() {
		// 命中即视为容忍。
		if resourceclaim.ToleratesTaint(toleration, taint) {
			return true
		}
	}
	return false
}

// checkAvailableCounters 检查池内可用计数器是否足够当前设备消耗；成功后预留计数器。
// 仅在分区特性启用且设备消耗计数器时调用。
func (alloc *allocator) checkAvailableCounters(device deviceWithID) (bool, error) {
	// 设备所属池与池 ID。
	pool := device.pool
	poolID := pool.PoolID

	// 先查缓存的可用计数器。
	// 读锁保护缓存读取。
	alloc.mutex.RLock()
	availableCountersForPool, found := alloc.availableCounters[poolID]
	alloc.mutex.RUnlock()
	// 未命中则即时计算；无锁计算可能被多个 goroutine 重复执行，但输入相同故结果一致。
	if !found {
		// 首次使用：创建计数器集映射，并从池定义深拷贝计数器。
		availableCountersForPool = make(counterSets, len(pool.CounterSets))
		// 遍历池中的计数器集。
		for _, counterSet := range pool.CounterSets {
			// 初始化集内计数器映射。
			availableCountersForCounterSet := make(map[string]resourceapi.Counter, len(counterSet.Counters))
			// 遍历集内计数器。
			for name, c := range counterSet.Counters {
				// 深拷贝计数器值，避免修改池定义。
				c.Value = c.Value.DeepCopy()
				availableCountersForCounterSet[name] = c
			}
			// 写入计数器集。
			availableCountersForPool[counterSet.Name] = availableCountersForCounterSet
		}

		// 同时考虑定向到目标节点与未定向的切片。
		for _, resourceSlices := range [][]*draapi.ResourceSlice{pool.DeviceSlicesTargetingNode, pool.DeviceSlicesNotTargetingNode} {
			// 遍历切片与设备。
			for _, slice := range resourceSlices {
				for _, device := range slice.Spec.Devices {
					// 构造设备 ID。
					deviceID := DeviceID{
						Driver: slice.Spec.Driver,
						Pool:   slice.Spec.Pool.Name,
						Device: device.Name,
					}
					// 未分配的设备不消耗计数器。
					if !internal.IsDeviceAllocated(deviceID, &alloc.allocatedState) {
						continue
					}
					// 逐项扣除该设备已消耗的计数器。
					for _, deviceCounterConsumption := range device.ConsumesCounters {
						availableCountersForCounterSet := availableCountersForPool[deviceCounterConsumption.CounterSet]
						for name, c := range deviceCounterConsumption.Counters {
							// 查找集内同名计数器。
							existingCounter, ok := availableCountersForCounterSet[name]
							if !ok {
								continue
							}
							// 允许出现负值，表示没有可用计数器。
							existingCounter.Value.Sub(c.Value)
							availableCountersForCounterSet[name] = existingCounter
						}
					}
				}
			}
		}

		// 缓存计算结果，避免重复计算。
		// 写锁保护缓存写入。
		alloc.mutex.Lock()
		alloc.availableCounters[poolID] = availableCountersForPool
		alloc.mutex.Unlock()
	}

	// 将当前设备的消耗累加到 consumedCounters。
	// 取当前池对应的消耗计数器集合。
	consumedCountersForPool, found := alloc.consumedCounters[poolID]
	// 首次使用时初始化。
	if !found {
		consumedCountersForPool = make(counterSets)
		alloc.consumedCounters[poolID] = consumedCountersForPool
	}
	// 遍历设备声明的计数器消耗。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		// 首次使用该计数器集时初始化。
		consumedCountersForCounterSet, found := consumedCountersForPool[deviceCounterConsumption.CounterSet]
		if !found {
			consumedCountersForCounterSet = make(map[string]resourceapi.Counter)
			consumedCountersForPool[deviceCounterConsumption.CounterSet] = consumedCountersForCounterSet
		}
		// 逐项累加计数器消耗。
		for name, c := range deviceCounterConsumption.Counters {
			consumedCounters, found := consumedCountersForCounterSet[name]
			// 首次出现时深拷贝。
			if !found {
				c.Value = c.Value.DeepCopy()
				consumedCountersForCounterSet[name] = c
				continue
			}
			// 已存在则相加。
			consumedCounters.Value.Add(c.Value)
			consumedCountersForCounterSet[name] = consumedCounters
		}
	}

	// 检查是否超出可用量。
	for availableCounterSetName, availableCounters := range availableCountersForPool {
		consumedCounters := consumedCountersForPool[availableCounterSetName]
		// 遍历所有可用计数器集。
		for availableCounterName, availableCounter := range availableCounters {
			consumedCounter := consumedCounters[availableCounterName]
			// 消耗超过可用量则回滚预留。
			if availableCounter.Value.Cmp(consumedCounter.Value) < 0 {
				alloc.deallocateCountersForDevice(device)
				return false, nil
			}
		}
	}

	// 余量充足，预留成功。
	return true, nil
}

// deviceInUse 判断设备是否已被历史分配或本次尝试占用。
func (alloc *allocator) deviceInUse(deviceID DeviceID) bool {
	// 历史已分配或本次尝试中任一声明占用，均视为在用。
	return alloc.allocatedState.AllocatedDevices.Has(deviceID) || alloc.allocatingDeviceForAnyClaim(deviceID)
}

// deviceCapacityInUse 判断设备是否已有容量被占用（历史分配或本次尝试）。
func (alloc *allocator) deviceCapacityInUse(deviceID DeviceID) bool {
	// 已聚合容量说明设备已被使用。
	if _, found := alloc.allocatedState.AggregatedCapacity[deviceID]; found {
		return true
	}
	// 兼容旧数据：无每份额容量的多实例分配只记录在共享设备 ID 集合中。
	// 检查共享设备 ID 集合。
	for sharedDeviceID := range alloc.allocatedState.AllocatedSharedDeviceIDs {
		if sharedDeviceID.GetDeviceID() == deviceID {
			return true
		}
	}
	// 检查本次尝试中登记的容量占用。
	return alloc.allocatingCapacityForAnyClaim(deviceID)
}

// allocatingDeviceForAnyClaim 判断设备是否被本次尝试中的任一声明占用。
func (alloc *allocator) allocatingDeviceForAnyClaim(deviceID DeviceID) bool {
	return alloc.allocatingDevices[deviceID].Len() > 0
}

// allocatingDeviceForClaim 判断设备是否被本次尝试中的指定声明占用。
func (alloc *allocator) allocatingDeviceForClaim(deviceID DeviceID, claimIndex int) bool {
	return alloc.allocatingDevices[deviceID].Has(claimIndex)
}

// allocatingCapacityForAnyClaim 判断本次尝试中是否在该设备上登记了容量占用。
func (alloc *allocator) allocatingCapacityForAnyClaim(deviceID DeviceID) bool {
	_, found := alloc.allocatingCapacity[deviceID]
	return found
}

// deallocateCountersForDevice 从 consumedCounters 中扣除指定设备的计数器消耗。
func (alloc *allocator) deallocateCountersForDevice(device deviceWithID) {
	poolID := device.pool.PoolID

	// 取设备所属池的消耗计数器集合。
	consumedCountersForPool := alloc.consumedCounters[poolID]
	// 遍历设备的计数器消耗声明。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		counterSetName := deviceCounterConsumption.CounterSet
		consumedCounterSet := consumedCountersForPool[counterSetName]
		// 逐个计数器扣减。
		for name, c := range deviceCounterConsumption.Counters {
			consumedCounter := consumedCounterSet[name]
			consumedCounter.Value.Sub(c.Value)
			consumedCounterSet[name] = consumedCounter
		}
	}
}

// createNodeSelector 根据已分配设备的节点约束构造节点选择器；无需约束时返回 nil。
func (alloc *allocator) createNodeSelector(result []internalDeviceResult, targetNodeName string) (*v1.NodeSelector, error) {
	// 先构造单 term 选择器，后续按需追加要求。
	// 初始选择器。
	ns := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{}},
	}

	// 遍历所有分配到的设备。
	for i := range result {
		// 设备所属切片。
		slice := result[i].slice
		// 待收集的节点名与节点选择器。
		var nodeName *string
		var nodeSelector *v1.NodeSelector
		// 每设备节点选择时取设备自身的配置。
		if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			// 设备级节点名。
			nodeName = result[i].NodeName
			// 设备级节点选择器。
			nodeSelector = result[i].NodeSelector
		} else {
			// 否则使用切片级配置。
			// 切片级节点名。
			nodeName = slice.Spec.NodeName
			// 切片级节点选择器。
			nodeSelector = slice.Spec.NodeSelector
		}
		// 设备本地于某节点或绑定到节点时，直接把分配限定到目标节点。
		if nodeName != nil || result[i].BindsToNode {
			// 返回按 metadata.name 限定的节点选择器。
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
		// 合并切片级选择器中的要求。
		if nodeSelector != nil {
			// 根据 term 数量决定处理方式。
			switch len(nodeSelector.NodeSelectorTerms) {
			// 0 个 term：无需处理。
			case 0:
			// 1 个 term：追加 MatchFields 与 MatchExpressions。
			case 1:
				// 追加字段要求。
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchFields, &ns.NodeSelectorTerms[0].MatchFields)
				// 追加表达式要求。
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchExpressions, &ns.NodeSelectorTerms[0].MatchExpressions)
			// 多个 term 不受支持，校验本应阻止创建此类切片。
			default:
				// 报错。
				return nil, fmt.Errorf("unsupported ResourceSlice.NodeSelector with %d terms", len(nodeSelector.NodeSelectorTerms))
			}
		}
	}

	// 有任一要求时返回选择器。
	if len(ns.NodeSelectorTerms[0].MatchFields) > 0 || len(ns.NodeSelectorTerms[0].MatchExpressions) > 0 {
		return ns, nil
	}

	// 无要求表示设备可用于任意节点。
	return nil, nil
}

// requestAccessor 统一访问 DeviceRequest 与 DeviceSubRequest，使分配逻辑无需区分二者。
type requestAccessor interface {
	// name 返回请求或子请求的名称。
	name() string
	// deviceClassName 返回设备类名。
	deviceClassName() string
	// allocationMode 返回设备分配模式。
	allocationMode() resourceapi.DeviceAllocationMode
	// count 返回请求的设备数量。
	count() int64
	// adminAccess 返回是否实际启用了管理员访问。
	adminAccess() bool
	// hasAdminAccess 返回是否显式设置了管理员访问字段。
	hasAdminAccess() bool
	// selectors 返回 CEL 设备选择器列表。
	selectors() []resourceapi.DeviceSelector
	// tolerations 返回设备污点容忍列表。
	tolerations() []resourceapi.DeviceToleration
	// capacities 返回容量需求。
	capacities() *resourceapi.CapacityRequirements
	// derivedAttributes 返回派生属性列表。
	derivedAttributes() []resourceapi.DeviceDerivedAttribute
}

// exactDeviceRequestAccessor 将 DeviceRequest 适配为 requestAccessor。
type exactDeviceRequestAccessor struct {
	// 被适配的精确设备请求。
	request *resourceapi.DeviceRequest
}

// name 返回请求名。
func (d *exactDeviceRequestAccessor) name() string {
	return d.request.Name
}

// deviceClassName 返回 Exactly.DeviceClassName。
func (d *exactDeviceRequestAccessor) deviceClassName() string {
	return d.request.Exactly.DeviceClassName
}

// allocationMode 返回 Exactly.AllocationMode。
func (d *exactDeviceRequestAccessor) allocationMode() resourceapi.DeviceAllocationMode {
	return d.request.Exactly.AllocationMode
}

// count 返回 Exactly.Count。
func (d *exactDeviceRequestAccessor) count() int64 {
	return d.request.Exactly.Count
}

// adminAccess 返回管理员访问标志，未设置视为 false。
func (d *exactDeviceRequestAccessor) adminAccess() bool {
	return ptr.Deref(d.request.Exactly.AdminAccess, false)
}

// hasAdminAccess 判断管理员访问字段是否被显式设置。
func (d *exactDeviceRequestAccessor) hasAdminAccess() bool {
	return d.request.Exactly.AdminAccess != nil
}

// derivedAttributes 返回 Exactly.DerivedAttributes。
func (d *exactDeviceRequestAccessor) derivedAttributes() []resourceapi.DeviceDerivedAttribute {
	// Exactly 为空时返回 nil。
	if d.request.Exactly != nil {
		return d.request.Exactly.DerivedAttributes
	}
	return nil
}

// selectors 返回 Exactly.Selectors。
func (d *exactDeviceRequestAccessor) selectors() []resourceapi.DeviceSelector {
	return d.request.Exactly.Selectors
}

// tolerations 返回 Exactly.Tolerations。
func (d *exactDeviceRequestAccessor) tolerations() []resourceapi.DeviceToleration {
	return d.request.Exactly.Tolerations
}

// capacities 返回 Exactly.Capacity。
func (d *exactDeviceRequestAccessor) capacities() *resourceapi.CapacityRequirements {
	return d.request.Exactly.Capacity
}

// deviceSubRequestAccessor 将 DeviceSubRequest 适配为 requestAccessor。
type deviceSubRequestAccessor struct {
	// 被适配的子请求。
	subRequest *resourceapi.DeviceSubRequest
}

// name 返回子请求名。
func (d *deviceSubRequestAccessor) name() string {
	return d.subRequest.Name
}

// deviceClassName 返回子请求的设备类名。
func (d *deviceSubRequestAccessor) deviceClassName() string {
	return d.subRequest.DeviceClassName
}

// allocationMode 返回子请求的分配模式。
func (d *deviceSubRequestAccessor) allocationMode() resourceapi.DeviceAllocationMode {
	return d.subRequest.AllocationMode
}

// count 返回子请求的设备数量。
func (d *deviceSubRequestAccessor) count() int64 {
	return d.subRequest.Count
}

// adminAccess 恒为 false：子请求不支持管理员访问。
func (d *deviceSubRequestAccessor) adminAccess() bool {
	return false
}

// hasAdminAccess 恒为 false。
func (d *deviceSubRequestAccessor) hasAdminAccess() bool {
	return false
}

// derivedAttributes 返回子请求的派生属性。
func (d *deviceSubRequestAccessor) derivedAttributes() []resourceapi.DeviceDerivedAttribute {
	return d.subRequest.DerivedAttributes
}

// selectors 返回子请求的选择器。
func (d *deviceSubRequestAccessor) selectors() []resourceapi.DeviceSelector {
	return d.subRequest.Selectors
}

// tolerations 返回子请求的容忍列表。
func (d *deviceSubRequestAccessor) tolerations() []resourceapi.DeviceToleration {
	return d.subRequest.Tolerations
}

// capacities 返回子请求的容量需求。
func (d *deviceSubRequestAccessor) capacities() *resourceapi.CapacityRequirements {
	return d.subRequest.Capacity
}

// addNewNodeSelectorRequirements 将 from 中尚未存在的要求追加到 to。
func addNewNodeSelectorRequirements(from []v1.NodeSelectorRequirement, to *[]v1.NodeSelectorRequirement) {
	// 逐个检查要求。
	for _, requirement := range from {
		// 不重复才追加。
		if !containsNodeSelectorRequirement(*to, requirement) {
			*to = append(*to, requirement)
		}
	}
}

// containsNodeSelectorRequirement 判断要求是否已存在：键、操作符与值集合均相同才算重复。
func containsNodeSelectorRequirement(requirements []v1.NodeSelectorRequirement, requirement v1.NodeSelectorRequirement) bool {
	// 值按集合比较，忽略顺序与重复。
	values := sets.New(requirement.Values...)
	// 逐个比较已有要求。
	for _, existingRequirement := range requirements {
		// 键不同则跳过。
		if existingRequirement.Key != requirement.Key {
			continue
		}
		// 操作符不同则跳过。
		if existingRequirement.Operator != requirement.Operator {
			continue
		}
		// 值集合不同则跳过。
		if !sets.New(existingRequirement.Values...).Equal(values) {
			continue
		}
		// 三项均相同，判定为已存在。
		return true
	}
	return false
}
