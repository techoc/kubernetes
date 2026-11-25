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

package stable

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
	"k8s.io/apimachinery/pkg/util/sets"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// DeviceClassLister、Features、DeviceID、Stats 与 AllocatedState 均为 internal 包类型的别名。
type DeviceClassLister = internal.DeviceClassLister
type Features = internal.Features
type DeviceID = internal.DeviceID
type Stats = internal.Stats
type AllocatedState = internal.AllocatedState

// MakeDeviceID 根据驱动名、池名与设备名构造 DeviceID，实际逻辑委托给 internal 包。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return internal.MakeDeviceID(driver, pool, device)
}

// SupportedFeatures 列出 stable 支持的附加特性；这些特性的任意组合都会选择本变体。
var SupportedFeatures = internal.Features{
	AdminAccess:          true,
	PrioritizedList:      true,
	PartitionableDevices: true,
	DeviceTaints:         true,
}

// Allocator 是稳定版分配器的顶层对象，可被多次、并发使用；每次 Allocate 会创建独立的临时状态。
type Allocator struct {
	// features 为启用的特性集合。
	// allocatedDevices 为已经完成分配的设备集合。
	// classLister 用于按名称查询设备类。
	// slices 为候选资源切片列表。
	// celCache 复用已编译的 CEL 表达式。
	features         Features
	allocatedDevices sets.Set[DeviceID]
	classLister      DeviceClassLister
	slices           []*resourceapi.ResourceSlice
	celCache         *cel.Cache
	// availableCounters 以 PoolID（驱动名+池名）为键，按池缓存可用计数器，首次使用时计算且之后不再更新。
	// 缓存基于 Allocator 的静态输入，因此跨多次 Allocate 调用仍有效；mutex 保护其并发访问。
	availableCounters map[PoolID]counterSets
	mutex             sync.RWMutex
	// numAllocateOneInvocations 统计 allocateOne 的调用次数，用于衡量分配工作量。
	numAllocateOneInvocations atomic.Int64
}

// 确保 Allocator 实现 internal.AllocatorExtended 接口。
var _ internal.AllocatorExtended = &Allocator{}

// NewAllocator 创建可在多次分配间复用且线程安全的分配器；输入存在导致无法分配的问题时返回错误。
func NewAllocator(ctx context.Context,
	features Features,
	allocatedDevices sets.Set[DeviceID],
	classLister DeviceClassLister,
	slices []*resourceapi.ResourceSlice,
	celCache *cel.Cache,
) (*Allocator, error) {
	// 初始化分配器并创建可用计数器缓存。
	return &Allocator{
		features:          features,
		allocatedDevices:  allocatedDevices,
		classLister:       classLister,
		slices:            slices,
		celCache:          celCache,
		availableCounters: make(map[PoolID]counterSets),
	}, nil
}

// Channel 返回分配器所属的发布通道，稳定版为 internal.Stable。
func (a *Allocator) Channel() internal.AllocatorChannel {
	return internal.Stable
}

// Allocate 为一组资源声明分配设备，返回每个声明对应的分配结果。
// 先收集节点可用池中的候选设备，再对每个请求穷举候选并检查约束，取第一个完整解。
// 所有节点均无解时返回错误或空结果，由调用方汇总。
func (a *Allocator) Allocate(ctx context.Context, node *v1.Node, claims []*resourceapi.ResourceClaim) (finalResult []resourceapi.AllocationResult, finalErr error) {
	// 每次调用创建独立的 allocator，避免跨调用共享临时状态。
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
	}
	// 记录分配开始时的声明数与切片数。
	alloc.logger.V(5).Info("Starting allocation", "numClaims", len(alloc.claimsToAllocate), "numSlices", len(alloc.slices))
	// 用 defer 统一记录分配结束与结果。
	defer func() {
		alloc.logger.V(5).Info("Done with allocation", "success", len(finalResult) == len(alloc.claimsToAllocate), "err", finalErr)
	}()

	// 先收集当前节点可用的资源池，失败则直接返回错误。
	pools, err := GatherPools(ctx, alloc.slices, node, a.features)
	if err != nil {
		return nil, fmt.Errorf("gather pool information: %w", err)
	}
	// 将池信息存入本次分配状态。
	alloc.pools = pools
	alloc.logger.V(5).Info("Gathered pool information", "pools", logPools(alloc.logger, pools))

	// 逐个声明、请求与设备枚举候选，选中后标记占用并递归，从而覆盖所有可行顺序，找到第一个完整解即采用。
	// 当前无评分；未来引入评分后需比较等价解，并避免重复尝试实际相同的请求与设备组合。
	// 统计每个声明所需的最小设备数，并校验请求能否被处理；总数用于预分配。
	minDevicesTotal := 0
	// 逐个声明校验。
	for claimIndex, claim := range alloc.claimsToAllocate {
		minDevicesPerClaim := 0

		// 遍历声明中的每个设备请求。
		for requestIndex := range claim.Spec.Devices.Requests {
			request := &claim.Spec.Devices.Requests[requestIndex]
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			// 判断是否存在 firstAvailable 子请求。
			hasSubRequests := len(request.FirstAvailable) > 0

			// 未启用 prioritizedList 特性时拒绝带子请求的声明。
			if !a.features.PrioritizedList && hasSubRequests {
				return nil, fmt.Errorf("claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name)
			}

			// 当前请求所需的最小设备数。
			var minDevicesPerRequest int
			// 带子请求时取各子请求中的最小值。
			if hasSubRequests {
				// 初始化为最大值，便于求最小。
				minDevicesPerRequest = math.MaxInt
				// 遍历子请求并逐个校验。
				for i, subReq := range request.FirstAvailable {
					requestKey.subRequestIndex = i
					// 同时传入精确请求访问器作为父请求上下文。
					reqData, err := alloc.validateDeviceRequest(&deviceSubRequestAccessor{subRequest: &subReq},
						&exactDeviceRequestAccessor{request: request}, requestKey, pools)
					// 校验失败直接返回。
					if err != nil {
						return nil, err
					}
					// 保存子请求的校验结果。
					alloc.requestData[requestKey] = reqData
					// 取更小的设备数。
					minDevicesPerRequest = min(minDevicesPerRequest, reqData.numDevices)
				}
			} else {
				// 不带子请求的普通请求按精确请求校验。
				reqData, err := alloc.validateDeviceRequest(&exactDeviceRequestAccessor{request: request}, nil, requestKey, pools)
				if err != nil {
					return nil, err
				}
				// 保存请求的校验结果。
				alloc.requestData[requestKey] = reqData
				minDevicesPerRequest = reqData.numDevices
			}
			// 先比较剩余容量再累加，避免大数值造成溢出。
			if minDevicesPerRequest > resourceapi.AllocationResultsMaxSize-minDevicesPerClaim {
				return nil, fmt.Errorf("claim %s, request %s: adding %d devices exceeds the claim limit of %d, with %d already accounted for", klog.KObj(claim), request.Name, minDevicesPerRequest, resourceapi.AllocationResultsMaxSize, minDevicesPerClaim)
			}
			// 累加到当前声明。
			minDevicesPerClaim += minDevicesPerRequest
		}
		// 记录声明校验结果。
		alloc.logger.V(6).Info("Checked claim", "claim", klog.KObj(claim), "minDevices", minDevicesPerClaim)

		// 为结果切片预留容量；这只是下界，allocateOne 中会再次检查上限。
		alloc.result[claimIndex].devices = make([]internalDeviceResult, 0, minDevicesPerClaim)

		// 约束一旦返回 false 即剪枝，前提是其具有单调性。
		constraints := make([]constraint, len(claim.Spec.Devices.Constraints))
		// 逐个构造约束对象。
		for i, constraint := range claim.Spec.Devices.Constraints {
			// 按约束类型分派。
			switch {
			// 属性匹配约束。
			case constraint.MatchAttribute != nil:
				matchAttribute := resourceapi.FullyQualifiedName(*constraint.MatchAttribute)
				logger := alloc.logger
				if loggerV := alloc.logger.V(6); loggerV.Enabled() {
					logger = klog.LoggerWithName(logger, "matchAttributeConstraint")
					logger = klog.LoggerWithValues(logger, "matchAttribute", matchAttribute)
				}
				// 记录受影响的请求名与待匹配的属性名。
				m := &matchAttributeConstraint{
					logger:        logger,
					requestNames:  sets.New(constraint.Requests...),
					attributeName: matchAttribute,
				}
				// 保存约束对象。
				constraints[i] = m
			// 未知约束类型，无法处理。
			default:
				return nil, fmt.Errorf("claim %s, constraint #%d: empty constraint (unsupported constraint type?)", klog.KObj(claim), i)
			}
		}
		// 将约束列表存入本次分配状态。
		alloc.constraints[claimIndex] = constraints
		// 累计到全局最小设备数。
		minDevicesTotal += minDevicesPerClaim
	}

	// 设备与请求的匹配结果可安全缓存，因为它们在单次 Allocate 期间不变。
	alloc.deviceMatchesRequest = make(map[matchKey]bool)

	// 按最小设备数预分配正在分配的设备表。
	alloc.allocatingDevices = make(map[DeviceID]sets.Set[int], minDevicesTotal)

	// 记录已分配设备与待分配设备数。
	alloc.logger.V(5).Info("Gathered information about devices", "allocatedDevices", logAllocatedDevices(alloc.logger, alloc.allocatedDevices), "minDevicesToBeAllocated", minDevicesTotal)

	// 所有错误均已包装好，可直接由 Allocate 返回；从第一个声明开始递归分配。
	done, err := alloc.allocateOne(deviceIndices{}, false)
	// 提前终止信号表示无解但不算错误。
	if errors.Is(err, errStop) {
		// 无解时返回空结果。
		return nil, nil
	}
	// 其他错误直接返回。
	if err != nil {
		return nil, err
	}
	// 未找到完整解。
	if !done {
		// 若存在无效池，返回非致命错误以便其他节点继续尝试。
		for _, pool := range pools {
			if pool.IsInvalid {
				// 该错误仅在所有节点都失败时才会被汇总上报。
				return nil, fmt.Errorf("invalid resource pools were encountered%w", internal.ErrFailedAllocationOnNode)
			}
		}
		// 没有无效池时按无解返回空结果。
		return nil, nil
	}

	// 将内部结果转换为 API 分配结果。
	result := make([]resourceapi.AllocationResult, len(alloc.result))
	// 逐个声明转换。
	for claimIndex, internalResult := range alloc.result {
		claim := alloc.claimsToAllocate[claimIndex]
		allocationResult := &result[claimIndex]
		// 转换设备结果列表。
		allocationResult.Devices.Results = make([]resourceapi.DeviceRequestAllocationResult, len(internalResult.devices))
		// 逐设备填充请求名、驱动、池、设备名与权限信息。
		for i, internal := range internalResult.devices {
			allocationResult.Devices.Results[i] = resourceapi.DeviceRequestAllocationResult{
				Request:     internal.requestName(),
				Driver:      internal.id.Driver.String(),
				Pool:        internal.id.Pool.String(),
				Device:      internal.id.Device.String(),
				AdminAccess: internal.adminAccess,
				Tolerations: internal.lookupRequest(claim).tolerations(),
			}
		}

		// 填充设备配置；每个设备类的配置只添加一次，并记录其在结果中的位置。
		// configRange 记录某个设备类的配置在结果切片中的区间。
		type configRange struct{ start, end int }
		// 以类名为键记录已添加配置的区间。
		configIndexesForClass := make(map[string]configRange) // Key 为类名，Value 为该类的配置在 allocationResult.Devices.Config 中的位置。
		// 遍历请求以收集类配置。
		for requestIndex := range claim.Spec.Devices.Requests {
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			requestData := alloc.requestData[requestKey]
			// 子请求需使用被选中的子请求所属的设备类。
			if requestData.parentRequest != nil {
				// 定位到被选中的子请求。
				requestKey.subRequestIndex = requestData.selectedSubRequestIndex
				requestData = alloc.requestData[requestKey]
			}

			class := requestData.class
			// 没有设备类则跳过。
			if class == nil {
				continue
			}
			// 检查该类的配置是否已添加。
			configIndexes, exists := configIndexesForClass[class.Name]
			// 已添加则只需把当前请求名追加到现有配置。
			if exists {
				for i := configIndexes.start; i < configIndexes.end; i++ {
					allocationResult.Devices.Config[i].Requests = append(allocationResult.Devices.Config[i].Requests, requestData.requestName())
				}
				continue
			}

			// 首次遇到该类，添加其全部配置并记录添加前的长度。
			initialConfigLen := len(allocationResult.Devices.Config)
			// 逐条添加类配置。
			for _, config := range class.Spec.Config {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClass,
					Requests:            []string{requestData.requestName()},
					DeviceConfiguration: config.DeviceConfiguration,
				})
			}
			// 记录该类配置在结果中的位置。
			configIndexesForClass[class.Name] = configRange{start: initialConfigLen, end: len(allocationResult.Devices.Config)}
		}
		// 处理声明级别的配置。
		for _, config := range claim.Spec.Devices.Config {
			// 未指定请求的声明配置适用于所有请求，直接加入。
			if len(config.Requests) == 0 {
				allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
					Source:              resourceapi.AllocationConfigSourceClaim,
					Requests:            config.Requests,
					DeviceConfiguration: config.DeviceConfiguration,
				})
				continue
			}

			// 检查配置中列出的请求是否在本声明中。
			for i, request := range claim.Spec.Devices.Requests {
				// 请求名匹配则添加配置。
				if slices.Contains(config.Requests, request.Name) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					break // 首次匹配后即可停止检查，因为配置中的所有请求都应被加入。
				}

				// 检查请求数据以确定是否被选中的子请求引用。
				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: i}
				requestData := alloc.requestData[requestKey]
				// 非父请求跳过。
				if requestData.parentRequest == nil {
					continue
				}

				// 构造完整的子请求名，即「父请求/子请求」。
				subRequest := request.FirstAvailable[requestData.selectedSubRequestIndex]
				subRequestName := fmt.Sprintf("%s/%s", request.Name, subRequest.Name)
				// 子请求名匹配则添加配置。
				if slices.Contains(config.Requests, subRequestName) {
					allocationResult.Devices.Config = append(allocationResult.Devices.Config, resourceapi.DeviceAllocationConfiguration{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            config.Requests,
						DeviceConfiguration: config.DeviceConfiguration,
					})
					break // 首次匹配后即可停止检查，因为配置中的所有请求都应被加入。
				}
			}
		}

		// 若配置实际适用于所有请求，则清空 Requests 以利用「空表示全部」的语义。
		for i := range allocationResult.Devices.Config {
			requests := allocationResult.Devices.Config[i].Requests
			if len(requests) == 0 {
				continue
			}
			// 假设适用于所有请求，发现例外则置为 false。
			appliesToAll := true
			// 逐个请求核对。
			for requestIndex, request := range claim.Spec.Devices.Requests {
				// 请求名在列表中则继续。
				if slices.Contains(requests, request.Name) {
					continue
				}
				requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
				requestData := alloc.requestData[requestKey]
				// 子请求需要同时检查完整名称。
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
			// 适用于所有请求时清空列表。
			if appliesToAll {
				allocationResult.Devices.Config[i].Requests = nil
			}
		}

		// 根据已分配设备生成节点选择器，失败则返回带上下文的错误。
		nodeSelector, err := alloc.createNodeSelector(internalResult.devices)
		if err != nil {
			return nil, fmt.Errorf("create NodeSelector for claim %s: %w", claim.Name, err)
		}
		// 写入节点选择器。
		allocationResult.NodeSelector = nodeSelector
	}

	// 返回全部声明的分配结果。
	return result, nil
}

// GetStats 返回分配器统计信息，当前仅包含 allocateOne 的调用次数。
func (a *Allocator) GetStats() Stats {
	s := Stats{
		NumAllocateOneInvocations: a.numAllocateOneInvocations.Load(),
	}
	return s
}

// validateDeviceRequest 校验设备请求并返回预计算的请求数据：设备类、所需设备数，以及 all 模式下的候选设备。
func (alloc *allocator) validateDeviceRequest(request requestAccessor, parentRequest requestAccessor, requestKey requestIndices, pools []*Pool) (requestData, error) {
	// 取得请求所属的声明。
	claim := alloc.claimsToAllocate[requestKey.claimIndex]
	// 初始化请求数据，保存请求与父请求访问器。
	requestData := requestData{
		request:       request,
		parentRequest: parentRequest,
	}
	// 派生属性目前仅支持实验变体，stable 出现即报错；待支持范围扩展后再实现。
	if len(request.derivedAttributes()) > 0 {
		return requestData, fmt.Errorf("claim %s, request %s: cannot handle request with derived attributes, the feature is disabled", klog.KObj(claim), request.name())
	}
	// 校验所有选择器都包含 CEL 表达式。
	for i, selector := range request.selectors() {
		// 未知的选择器类型。
		if selector.CEL == nil {
			return requestData, fmt.Errorf("claim %s, request %s, selector #%d: CEL expression empty (unsupported selector type?)", klog.KObj(claim), request.name(), i)
		}
	}

	// 未启用 admin access 特性时拒绝相应请求。
	if !alloc.features.AdminAccess && request.hasAdminAccess() {
		return requestData, fmt.Errorf("claim %s, request %s: admin access is requested, but the feature is disabled", klog.KObj(claim), request.name())
	}

	// 设备类名必须存在，否则视为不支持的请求。
	if request.deviceClassName() == "" {
		return requestData, fmt.Errorf("claim %s, request %s: missing device class name (unsupported request type?)", klog.KObj(claim), request.name())
	}
	// 按名称获取设备类。
	class, err := alloc.classLister.Get(request.deviceClassName())
	// 获取失败返回带上下文的错误。
	if err != nil {
		return requestData, fmt.Errorf("claim %s, request %s: could not retrieve device class %s: %w", klog.KObj(claim), request.name(), request.deviceClassName(), err)
	}

	// 类必须在调用 isSelectable 之前设置并保存。
	requestData.class = class

	// 按分配模式计算所需设备数。
	switch request.allocationMode() {
	// 精确数量模式。
	case resourceapi.DeviceAllocationModeExactCount:
		numDevices := request.count()
		// 数量超出 int 范围，无法处理。
		if numDevices > math.MaxInt {
			return requestData, fmt.Errorf("claim %s, request %s: exact count %d is too large", klog.KObj(claim), request.name(), numDevices)
		}
		// 记录精确数量。
		requestData.numDevices = int(numDevices)
	// all 模式：收集所有可选择的设备。
	case resourceapi.DeviceAllocationModeAll:
		// 预分配候选设备列表。
		requestData.allDevices = make([]deviceWithID, 0, resourceapi.AllocationResultsMaxSize)
		// 遍历所有资源池。
		for _, pool := range pools {
			// all 请求必须确定全部设备；不完整的池无法保证无遗漏，因此分配必然失败。
			// 当前普通模式仍可尝试不完整池；未来引入评分后应一律停等，以免因可见设备有限而错过更优解。
			if pool.IsIncomplete {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently being updated", klog.KObj(claim), request.name(), pool.PoolID)
			}
			// 非法的池同样无法支持。
			if pool.IsInvalid {
				return requestData, fmt.Errorf("claim %s, request %s: asks for all devices, but resource pool %s is currently invalid", klog.KObj(claim), request.name(), pool.PoolID)
			}

			// 遍历面向当前节点的设备切片。
			for _, slice := range pool.DeviceSlicesTargetingNode {
				// 逐个检查设备是否可选。
				for deviceIndex := range slice.Spec.Devices {
					// 调用可选择性检查。
					selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
					if err != nil {
						return requestData, err
					}
					// 可选设备加入候选列表。
					if selectable {
						device := deviceWithID{
							id:     DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name},
							Device: &slice.Spec.Devices[deviceIndex],
							slice:  slice,
							pool:   pool,
						}
						requestData.allDevices = append(requestData.allDevices, device)
					}
				}
			}
		}
		// 数量为候选设备的实际个数。
		requestData.numDevices = len(requestData.allDevices)
		// 记录 all 请求的结果。
		alloc.logger.V(6).Info("Request for 'all' devices", "claim", klog.KObj(claim), "request", request.name(), "numDevicesPerRequest", requestData.numDevices)
	// 不支持的分配模式。
	default:
		return requestData, fmt.Errorf("claim %s, request %s: unsupported count mode %s", klog.KObj(claim), request.name(), request.allocationMode())
	}
	return requestData, nil
}

// errStop 是 allocateOne 判定分配无法成功时返回的提前终止信号。
var errStop = errors.New("stop allocation")

// errAllocationResultMaxSizeExceeded 表示设备数超过上限，用于触发更积极的回溯。
var errAllocationResultMaxSizeExceeded = errors.New("allocation max size exceeded")

// allocator 仅在单次 Allocate 期间由单 goroutine 使用，因此无需加锁。
type allocator struct {
	// 嵌入的顶层分配器，提供共享配置与缓存。
	*Allocator
	// ctx 为本次分配的上下文。
	// logger 为本次分配使用的日志记录器。
	// node 为目标节点。
	// claimsToAllocate 为待分配的声明列表。
	// pools 为当前节点可用的资源池。
	// deviceMatchesRequest 为设备与请求的匹配结果缓存。
	// constraints 为每个声明对应的约束列表。
	ctx                  context.Context
	logger               klog.Logger
	node                 *v1.Node
	claimsToAllocate     []*resourceapi.ResourceClaim
	pools                []*Pool
	deviceMatchesRequest map[matchKey]bool
	constraints          [][]constraint // 每个 claim 对应一个约束列表
	// consumedCounters 记录本次分配过程中所有正在分配设备消耗的计数器。
	// requestData 为每个请求或子请求对应的数据。
	consumedCounters map[PoolID]counterSets
	requestData      map[requestIndices]requestData // 无子请求的每个 request 一项，每个子请求一项
	// allocatingDevices 记录某次尝试中将被新分配的设备及其对应的声明索引。
	// result 为每个声明的内部分配结果。
	allocatingDevices map[DeviceID]sets.Set[int]
	result            []internalAllocationResult
}

// counterSets 以计数器集合名为键，映射到该集合内计数器名到计数器的映射。
type counterSets map[draapi.UniqueString]map[string]resourceapi.Counter

// matchKey 由设备 ID 与请求索引组成，唯一标识一次设备/请求匹配。
type matchKey struct {
	// 设备 ID。
	DeviceID
	// 请求索引。
	requestIndices
}

// requestIndices 通过声明索引、请求索引与子请求索引唯一定位一个请求或子请求。
type requestIndices struct {
	// claimIndex 与 requestIndex 为声明与请求在各自列表中的索引。
	// subRequestIndex 为子请求在父请求中的索引。
	claimIndex, requestIndex int
	subRequestIndex          int
}

// deviceIndices 定位某个声明的请求或子请求中所需的一个具体设备。
type deviceIndices struct {
	// claimIndex 为声明在分配器中的索引。
	// requestIndex 为请求在声明中的索引。
	// subRequestIndex 为子请求在请求中的索引，非子请求时忽略。
	// deviceIndex 为设备在请求或子请求中的索引。
	claimIndex      int // claim 在分配器中的索引。
	requestIndex    int // request 在 claim 中的索引。
	subRequestIndex int // 子请求在 request 中的索引（subRequest 为 false 时忽略）。
	deviceIndex     int // 设备在 request 或子请求中的索引。
}

// requestData 保存单个请求或子请求在校验阶段得到的元数据。
type requestData struct {
	// 需要分配的请求或子请求，永不为 nil。
	request requestAccessor
	// parentRequest 为子请求的父请求，非子请求时为 nil。
	// class 为请求引用的设备类。
	// numDevices 为该请求需要的设备数量。
	parentRequest requestAccessor
	class         *resourceapi.DeviceClass
	numDevices    int

	// 被选中的子请求索引，仅在 subRequestIndex 为 0 的条目上设置。
	selectedSubRequestIndex int

	// all 模式下预先确定的候选设备集合。
	allDevices []deviceWithID
}

// requestName 返回请求的完整名称；子请求以「父请求/子请求」形式表示。
func (rd *requestData) requestName() string {
	if rd.parentRequest != nil {
		return fmt.Sprintf("%s/%s", rd.parentRequest.name(), rd.request.name())
	}
	return rd.request.name()
}

// deviceWithID 将内部设备对象与其 ID、所属切片和池绑定在一起。
type deviceWithID struct {
	// 嵌入的 *draapi.Device 为内部设备对象。
	// id 为设备 ID。
	// slice 为设备所在的资源切片。
	// pool 为设备所属的资源池。
	*draapi.Device
	id    DeviceID
	slice *draapi.ResourceSlice
	pool  *Pool
}

// internalAllocationResult 是单个声明的内部分配结果。
type internalAllocationResult struct {
	// 该声明分配到的设备列表。
	devices []internalDeviceResult
}

// internalDeviceResult 记录单个设备的分配细节。
type internalDeviceResult struct {
	// 嵌入的 *draapi.Device 为内部设备对象。
	// request 为请求名，无子请求时为请求名，有子请求时为子请求名。
	// parentRequest 为包含子请求的父请求名，无子请求时为空。
	// id 为设备 ID。
	// slice 为设备所在的资源切片。
	// adminAccess 表示是否申请了管理员访问权限。
	*draapi.Device
	request       string // request（无子请求时）或子请求的名称
	parentRequest string // 包含该子请求的 request 名称，否则为空
	id            DeviceID
	slice         *draapi.ResourceSlice
	adminAccess   *bool
}

// requestName 返回完整的请求名，子请求以「父请求/子请求」形式表示。
func (idr internalDeviceResult) requestName() string {
	if idr.parentRequest == "" {
		return idr.request
	}
	return fmt.Sprintf("%s/%s", idr.parentRequest, idr.request)
}

// lookupRequest 在声明中查找本设备结果对应的请求访问器，找不到时返回 nil。
func (idr internalDeviceResult) lookupRequest(claim *resourceapi.ResourceClaim) requestAccessor {
	// 子请求需使用父请求名进行查找。
	requestName := idr.request
	if idr.parentRequest != "" {
		requestName = idr.parentRequest
	}
	// 遍历声明的请求。
	for i := range claim.Spec.Devices.Requests {
		request := &claim.Spec.Devices.Requests[i]
		// 名称不匹配则跳过。
		if request.Name != requestName {
			continue
		}
		// 无子请求时直接返回精确请求访问器。
		if idr.parentRequest == "" {
			return &exactDeviceRequestAccessor{request}
		}
		// 有子请求时继续查找具体子请求。
		for j := range request.FirstAvailable {
			subRequest := &request.FirstAvailable[j]
			// 子请求名不匹配则跳过。
			if subRequest.Name != idr.request {
				continue
			}
			// 找到子请求，返回其访问器。
			return &deviceSubRequestAccessor{subRequest}
		}
	}
	// 未找到对应请求。
	return nil
}

// constraint 表示声明级别的设备约束，add 记录设备加入，remove 撤销加入。
type constraint interface {
	// add 在设备即将分配时调用，判断设备是否满足约束并在满足时记录。
	add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool

	// remove 与成功的 add 一一对应，参数完全相同，用于撤销记录。
	remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID)
}

// matchAttributeConstraint 要求一组设备的同名属性值完全一致，只需记录已加入的设备数量。
type matchAttributeConstraint struct {
	// logger 为已附加名称与属性名的日志记录器。
	// requestNames 为受该约束影响的请求名集合。
	// attributeName 为待比较的属性名。
	logger        klog.Logger // 已包含名称与属性名，日志消息中无需重复。
	requestNames  sets.Set[string]
	attributeName resourceapi.FullyQualifiedName

	// attribute 为第一台设备确定的基准属性值。
	// numDevices 为已加入约束的设备数量。
	attribute  *resourceapi.DeviceAttribute
	numDevices int
}

// add 检查设备属性是否与第一台设备一致，一致则计入约束集合。
func (m *matchAttributeConstraint) add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool {
	// 不受约束影响的请求直接视为满足。
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		m.logger.V(7).Info("Constraint does not apply to request", "request", requestName)
		return true
	}

	// 查找设备上的目标属性。
	attribute := lookupAttribute(device, deviceID, m.attributeName)
	// 设备没有该属性，不满足约束。
	if attribute == nil {
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false
	}

	// 第一台设备可作为基准。
	if m.numDevices == 0 {
		// 记录基准属性值。
		m.attribute = attribute
		m.numDevices = 1
		m.logger.V(7).Info("First in set")
		return true
	}

	// 按属性值的类型分别比较。
	switch {
	// 字符串类型需值相等。
	case attribute.StringValue != nil:
		if m.attribute.StringValue == nil || *attribute.StringValue != *m.attribute.StringValue {
			m.logger.V(7).Info("String values different")
			return false
		}
	// 整数类型需值相等。
	case attribute.IntValue != nil:
		if m.attribute.IntValue == nil || *attribute.IntValue != *m.attribute.IntValue {
			m.logger.V(7).Info("Int values different")
			return false
		}
	// 布尔类型需值相等。
	case attribute.BoolValue != nil:
		if m.attribute.BoolValue == nil || *attribute.BoolValue != *m.attribute.BoolValue {
			m.logger.V(7).Info("Bool values different")
			return false
		}
	// semver 2.0.0 要求最小规范形式（尤其禁止前导零），因此版本值可直接做字符串相等比较。
	case attribute.VersionValue != nil:
		if m.attribute.VersionValue == nil || *attribute.VersionValue != *m.attribute.VersionValue {
			m.logger.V(7).Info("Version values different")
			return false
		}
	// 未知类型无法匹配。
	default:
		m.logger.V(7).Info("Match attribute type unknown")
		return false
	}

	// 属性一致，计入集合。
	m.numDevices++
	m.logger.V(7).Info("Constraint satisfied by device", "device", deviceID, "numDevices", m.numDevices)
	return true
}

// remove 将设备从约束集合中移除，仅对受影响请求生效。
func (m *matchAttributeConstraint) remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) {
	// 不受约束影响的请求无需撤销。
	if m.requestNames.Len() > 0 && !m.matches(requestName, subRequestName) {
		return
	}

	// 递减计数。
	m.numDevices--
	m.logger.V(7).Info("Device removed from constraint set", "device", deviceID, "numDevices", m.numDevices)
}

// matches 判断请求或子请求是否在约束作用的请求名集合中。
func (m *matchAttributeConstraint) matches(requestName, subRequestName string) bool {
	// 普通请求直接按名称匹配。
	if subRequestName == "" {
		return m.requestNames.Has(requestName)
	} else {
		// 子请求同时匹配父请求名与完整名称。
		fullSubRequestName := fmt.Sprintf("%s/%s", requestName, subRequestName)
		return m.requestNames.Has(requestName) || m.requestNames.Has(fullSubRequestName)
	}
}

// lookupAttribute 在设备上查找属性：优先完全限定名，其次用设备驱动名作为域名匹配短名。
func lookupAttribute(device *draapi.Device, deviceID DeviceID, attributeName resourceapi.FullyQualifiedName) *resourceapi.DeviceAttribute {
	// 先尝试完全限定名。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName)]; ok {
		return &attr
	}
	// 查找域名与属性名的分隔符。
	index := strings.Index(string(attributeName), "/")
	// 非法完全限定名，无法匹配。
	if index < 0 {
		return nil
	}

	// 域名与驱动名不一致，属性不可用。
	if string(attributeName[0:index]) != deviceID.Driver.String() {
		return nil
	}

	// 域名匹配驱动名，用属性短名再次查找。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName[index+1:])]; ok {
		return &attr
	}

	return nil
}

// allocateOne 为当前所需设备枚举所有可用候选；allocateSubRequest 为 true 表示正在尝试某个特定子请求，用于避免递归死循环。
// 找到完整解返回 true，判定无法成功时返回 errStop 或其他错误。
func (alloc *allocator) allocateOne(r deviceIndices, allocateSubRequest bool) (bool, error) {
	// 统计调用次数。
	alloc.numAllocateOneInvocations.Add(1)

	// 上下文取消则中止分配。
	if alloc.ctx.Err() != nil {
		return false, fmt.Errorf("filter operation aborted: %w", context.Cause(alloc.ctx))
	}

	// 所有声明已分配完毕，找到完整解。
	if r.claimIndex >= len(alloc.claimsToAllocate) {
		// 当前无评分机制，直接使用第一个解。
		alloc.logger.V(6).Info("Allocation result found")
		return true, nil
	}

	// 取得当前声明。
	claim := alloc.claimsToAllocate[r.claimIndex]
	// 当前声明处理完毕，进入下一个声明。
	if r.requestIndex >= len(claim.Spec.Devices.Requests) {
		// 递归处理下一个声明。
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex + 1}, false)
		// 上限错误不是致命错误，换其他设备重试可能成功。
		if errors.Is(err, errAllocationResultMaxSizeExceeded) {
			return false, nil
		}
		return success, err
	}

	// 当前请求或子请求的数据来自 requestData。
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]

	// 子请求只需选中一个即可进入下一请求；尚未选定时遍历所有子请求。
	if !allocateSubRequest && requestData.parentRequest != nil {
		// 记录是否所有子请求都因超出上限而失败；此时可停止尝试上一请求的其他设备组合。
		// 但 firstAvailable 的某些子请求可能需要更少设备，不能据此提前判定整个搜索无解。
		allAllocationExceeded := true
		// 依次尝试每个子请求。
		for subRequestIndex := 0; ; subRequestIndex++ {
			// 构造下一个子请求的索引。
			nextSubRequestKey := requestKey
			nextSubRequestKey.subRequestIndex = subRequestIndex
			// 已遍历完所有子请求。
			if _, ok := alloc.requestData[nextSubRequestKey]; !ok {
				// 全部因上限失败，向上传递该错误以触发更积极回溯。
				if allAllocationExceeded {
					return false, errAllocationResultMaxSizeExceeded
				}
				return false, nil
			}

			// 设置当前要尝试的子请求索引。
			r.subRequestIndex = subRequestIndex
			// 尝试该子请求，传入 true 防止再次进入子请求循环。
			success, err := alloc.allocateOne(r, true /* 防止无限递归 */)
			// 上限错误可尝试下一个可能更省设备的子请求。
			if errors.Is(err, errAllocationResultMaxSizeExceeded) {
				continue
			}
			// 至少有一个子请求因其他原因失败。
			allAllocationExceeded = false
			if err != nil {
				return false, err
			}
			// 子请求分配成功，记录其索引。
			if success {
				// 保存选中的子请求。
				requestData.selectedSubRequestIndex = subRequestIndex
				alloc.requestData[requestKey] = requestData
				return true, nil
			}
		}
	}

	// 取得当前要满足的请求，可能是子请求。
	request := requestData.request
	// 是否为 all 模式。
	doAllDevices := request.allocationMode() == resourceapi.DeviceAllocationModeAll

	// all 模式至少要有一台设备，没有候选则无解。
	if doAllDevices && len(requestData.allDevices) == 0 {
		alloc.logger.V(6).Info("Allocation for 'all' devices didn't succeed: no devices found", "claim", klog.KObj(claim), "request", requestData.request.name())
		return false, nil
	}

	// 当前请求所需设备已分配完，进入下一请求。
	if r.deviceIndex >= requestData.numDevices {
		// 进入下一个请求。
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex + 1}, false)
		return success, err
	}

	// 计算还需分配的设备数与结果剩余容量。
	remaining := requestData.numDevices - r.deviceIndex
	// 结果列表剩余容量。
	available := resourceapi.AllocationResultsMaxSize - len(alloc.result[r.claimIndex].devices)
	// 容量不足，触发回溯。
	if remaining > available {
		// 返回特殊错误以便上层更积极回溯。
		return false, errAllocationResultMaxSizeExceeded
	}

	// 记录正在分配一台设备的上下文。
	alloc.logger.V(6).Info("Allocating one device", "currentClaim", r.claimIndex, "totalClaims", len(alloc.claimsToAllocate), "currentRequest", r.requestIndex, "currentSubRequest", r.subRequestIndex, "totalRequestsPerClaim", len(claim.Spec.Devices.Requests), "currentDevice", r.deviceIndex, "devicesPerRequest", requestData.numDevices, "allDevices", doAllDevices, "adminAccess", request.adminAccess())
	// all 模式下候选设备已确定，只需校验可用性。
	if doAllDevices {
		// 取下一个候选设备。
		deviceWithID := requestData.allDevices[r.deviceIndex]
		// must 为 true：约束不满足时视为错误。
		success, deallocate, err := alloc.allocateDevice(r, deviceWithID, true)
		if err != nil {
			return false, err
		}
		// all 设备的顺序无关，当前设备不可用即无解。
		if !success {
			return false, nil
		}
		// 递归分配下一台设备。
		done, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex, deviceIndex: r.deviceIndex + 1}, allocateSubRequest)
		// 失败或出错时回滚。
		if err != nil || !done {
			// 撤销该设备的临时分配。
			deallocate()
			return false, err
		}
		return done, nil
	}

	// 普通模式需要遍历所有池寻找合适设备。
	for _, pool := range alloc.pools {
		// 无效或不完整的池不参与分配，但此时不报错。
		if pool.IsIncomplete || pool.IsInvalid {
			continue
		}
		// 遍历面向当前节点的切片。
		for _, slice := range pool.DeviceSlicesTargetingNode {
			// 遍历设备候选。
			for deviceIndex := range slice.Spec.Devices {
				deviceID := DeviceID{Driver: pool.Driver, Pool: pool.Pool, Device: slice.Spec.Devices[deviceIndex].Name}

				// 优先做开销最小的使用中检查：admin access 只需避免同一声明内重复分配。
				if request.adminAccess() && alloc.allocatingDeviceForClaim(deviceID, r.claimIndex) {
					alloc.logger.V(7).Info("Device in use in same claim", "device", deviceID)
					continue
				}
				// 普通请求要求设备全局空闲。
				if !request.adminAccess() && alloc.deviceInUse(deviceID) {
					alloc.logger.V(7).Info("Device in use", "device", deviceID)
					continue
				}

				requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
				// 然后检查选择器是否满足类与请求的要求。
				selectable, err := alloc.isSelectable(requestKey, requestData, slice, deviceIndex)
				if err != nil {
					return false, err
				}
				// 不满足选择器则跳过。
				if !selectable {
					alloc.logger.V(7).Info("Device not selectable", "device", deviceID)
					continue
				}

				// 构造候选设备对象。
				device := deviceWithID{
					id:     deviceID,
					Device: &slice.Spec.Devices[deviceIndex],
					slice:  slice,
					pool:   pool,
				}
				// 检查计数器、污点与约束并暂存分配。
				allocated, deallocate, err := alloc.allocateDevice(r, device, false)
				if err != nil {
					return false, err
				}
				// 设备被占用或约束冲突，跳过。
				if !allocated {
					alloc.logger.V(7).Info("Device not usable", "device", deviceID)
					continue
				}
				// 构造下一台设备的索引。
				deviceKey := deviceIndices{
					claimIndex:      r.claimIndex,
					requestIndex:    r.requestIndex,
					subRequestIndex: r.subRequestIndex,
					deviceIndex:     r.deviceIndex + 1,
				}
				// 递归分配下一台设备。
				done, err := alloc.allocateOne(deviceKey, allocateSubRequest)
				if err == nil && done {
					// 找到解则保留当前分配并返回。
					return done, nil
				}

				// 未找到解，撤销临时分配后再尝试其他设备。
				deallocate()

				// 遇到错误如超出上限，回滚后返回，由上层处理。
				if err != nil {
					return false, err
				}
			}
		}
	}

	// 遍历完所有候选仍无解。
	return false, nil
}

// isSelectable 检查设备是否满足请求与设备类的选择器以及节点选择条件，并缓存匹配结果。
func (alloc *allocator) isSelectable(r requestIndices, requestData requestData, slice *draapi.ResourceSlice, deviceIndex int) (bool, error) {
	// 取得设备对象。
	device := &slice.Spec.Devices[deviceIndex]
	// 特性关闭时不支持带 bindingConditions 的设备。
	if !alloc.features.DeviceBindingAndStatus &&
		len(device.BindingConditions) > 0 {
		return false, nil
	}

	// 构造设备 ID。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name}
	// 构造缓存键。
	matchKey := matchKey{DeviceID: deviceID, requestIndices: r}
	// 命中缓存直接返回。
	if matches, ok := alloc.deviceMatchesRequest[matchKey]; ok {
		return matches, nil
	}

	// 先检查设备类的选择器。
	if requestData.class != nil {
		// 所有类选择器都必须匹配。
		match, err := alloc.selectorsMatch(r, device, deviceID, requestData.class, requestData.class.Spec.Selectors)
		if err != nil {
			return false, err
		}
		// 类选择器不匹配，缓存 false。
		if !match {
			alloc.deviceMatchesRequest[matchKey] = false
			return false, nil
		}
	}

	// 再检查请求自身的选择器。
	request := requestData.request
	// 所有请求选择器都必须匹配。
	match, err := alloc.selectorsMatch(r, device, deviceID, nil, request.selectors())
	if err != nil {
		return false, err
	}
	if !match {
		alloc.deviceMatchesRequest[matchKey] = false
		return false, nil
	}

	// 按设备选择节点时，还需校验设备自身的节点选择条件。
	if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
		// 校验设备与当前节点的匹配。
		matches, err := NodeMatches(alloc.node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
		if err != nil {
			return false, err
		}
		if !matches {
			alloc.deviceMatchesRequest[matchKey] = false
			return false, nil
		}
	}

	// 全部条件满足，缓存 true。
	alloc.deviceMatchesRequest[matchKey] = true
	return true, nil

}

// selectorsMatch 依次求值选择器中的 CEL 表达式，全部匹配才返回 true。
func (alloc *allocator) selectorsMatch(r requestIndices, device *draapi.Device, deviceID DeviceID, class *resourceapi.DeviceClass, selectors []resourceapi.DeviceSelector) (bool, error) {
	// 逐个检查选择器。
	for i, selector := range selectors {
		// 从缓存获取或编译表达式。
		expr := alloc.celCache.GetOrCompile(selector.CEL.Expression)
		// 新版本 API server 接受表达式后若集群降级，旧代码可能无法编译。
		// stored expression 通常可避免此情况，但运行代码仍可能比集群旧多个版本。
		if expr.Error != nil {
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d: CEL compile error: %w", class.Name, i, expr.Error)
			}
			return false, fmt.Errorf("claim %s: selector #%d: CEL compile error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, expr.Error)
		}

		// 将内部设备转换为 v1 表示供 CEL 使用。
		var d resourceapi.Device
		// 转换失败则返回错误。
		if err := draapi.Convert_api_Device_To_v1_Device(device, &d, nil); err != nil {
			return false, fmt.Errorf("convert Device %s: %w", deviceID, err)
		}
		// 执行 CEL 匹配。
		matches, details, err := expr.DeviceMatches(alloc.ctx, cel.Device{Driver: deviceID.Driver.String(), Attributes: d.Attributes, Capacity: d.Capacity})
		// 按来源即设备类或声明，记录不同的日志上下文。
		if class != nil {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "class", klog.KObj(class), "selector", i, "expression", selector.CEL.Expression, "matches", matches, "actualCost", ptr.Deref(details.ActualCost(), 0), "err", err)
		} else {
			alloc.logger.V(7).Info("CEL result", "device", deviceID, "claim", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), "selector", i, "expression", selector.CEL.Expression, "actualCost", ptr.Deref(details.ActualCost(), 0), "matches", matches, "err", err)
		}

		// 运行时错误包装后返回。
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

	// 所有选择器都匹配。
	return true, nil
}

// allocateDevice 检查候选设备的占用、计数器、污点与约束；成功时更新共享状态并返回回滚闭包，
// 后续搜索失败时可用该闭包恢复状态。仅分配成功且无错误时返回闭包。
// must 为 true 时，约束不满足会作为错误返回。
func (alloc *allocator) allocateDevice(r deviceIndices, device deviceWithID, must bool) (bool, func(), error) {
	// 取得声明、请求数据与请求访问器。
	claim := alloc.claimsToAllocate[r.claimIndex]
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]
	request := requestData.request
	// admin access 避免同一声明内重复分配。
	if request.adminAccess() && alloc.allocatingDeviceForClaim(device.id, r.claimIndex) {
		alloc.logger.V(7).Info("Device in use in same claim", "device", device.id)
		return false, nil, nil
	}
	// 普通请求要求设备未被占用。
	if !request.adminAccess() && alloc.deviceInUse(device.id) {
		alloc.logger.V(7).Info("Device in use", "device", device.id)
		return false, nil, nil
	}

	// 分区特性关闭时不允许分配消费计数器的设备。
	if !alloc.features.PartitionableDevices && len(device.ConsumesCounters) > 0 {
		alloc.logger.V(7).Info("Device consumes counters, but the partitionable devices feature is not enabled", "device", device.id)
		return false, nil, nil
	}

	// 记录是否已预留计数器，确保回滚只释放已预留部分。
	countersReserved := false
	// 消费计数器时先校验可用性。
	if len(device.ConsumesCounters) > 0 {
		// 校验并预留计数器。
		ok, err := alloc.checkAvailableCounters(device)
		if err != nil {
			return false, nil, err
		}
		// 计数器不足，设备不可用。
		if !ok {
			alloc.logger.V(7).Info("Insufficient counters", "device", device.id)
			return false, nil, nil
		}
		countersReserved = true
	}

	// 计算请求名称，区分普通请求与子请求。
	var parentRequestName string
	var baseRequestName string
	var subRequestName string
	if requestData.parentRequest == nil {
		baseRequestName = requestData.request.name()
	} else {
		// 子请求需同时记录父请求名。
		parentRequestName = requestData.parentRequest.name()
		baseRequestName = parentRequestName
		subRequestName = requestData.request.name()
	}

	// 用 state 记录本次调用对共享状态的修改，供回滚使用。
	state := deviceRollbackState{
		countersReserved:   countersReserved,
		previousNumResults: len(alloc.result[r.claimIndex].devices),
	}

	// 特性开启时检查设备污点，未被容忍则回滚并拒绝；特性关闭时跳过检查。
	if alloc.features.DeviceTaints && taintPreventsAllocation(device.Device, request) {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
		return false, nil, nil
	}

	// 设备可用，依次应用声明的所有约束。
	for _, constraint := range alloc.constraints[r.claimIndex] {
		// 约束不满足时回滚。
		if !constraint.add(baseRequestName, subRequestName, device.Device, device.id) {
			alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
			// all 模式下约束冲突视为错误。
			if must {
				return false, nil, fmt.Errorf("claim %s, request %s: cannot add device %s because a claim constraint would not be satisfied", klog.KObj(claim), request.name(), device.id)
			}
			return false, nil, nil
		}
		// 记录已成功添加的约束数量。
		state.constraintsAdded++
	}

	alloc.logger.V(7).Info("Device allocated", "device", device.id)

	// 所有约束满足，首次使用时初始化正在分配的设备集合。
	if alloc.allocatingDevices[device.id] == nil {
		alloc.allocatingDevices[device.id] = make(sets.Set[int])
	}
	// 标记设备正在为该声明分配。
	alloc.allocatingDevices[device.id].Insert(r.claimIndex)
	// 记录已标记。
	state.deviceMarked = true

	// 构造内部设备结果。
	result := internalDeviceResult{
		request:       request.name(),
		parentRequest: parentRequestName,
		id:            device.id,
		Device:        device.Device,
		slice:         device.slice,
	}
	// 记录管理员访问权限。
	if request.adminAccess() {
		result.adminAccess = ptr.To(request.adminAccess())
	}
	// 追加到声明的分配结果并记录。
	alloc.result[r.claimIndex].devices = append(alloc.result[r.claimIndex].devices, result)
	// 记录已添加结果。
	state.resultAdded = true

	// 成功路径返回回滚闭包，回溯时撤销该候选；复制状态以避免逃逸。
	undo := state
	// 返回成功标识与回滚函数。
	return true, func() {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, undo)
	}, nil
}

// deviceRollbackState 记录 allocateDevice 对单个候选所做的全部状态修改，以便撤销。
type deviceRollbackState struct {
	// countersReserved 表示是否已预留计数器。
	// constraintsAdded 为已成功添加的约束数量。
	// deviceMarked 表示是否已将设备标记为使用中。
	// resultAdded 表示是否已向结果列表追加设备。
	// previousNumResults 为追加前的结果列表长度。
	countersReserved   bool
	constraintsAdded   int
	deviceMarked       bool
	resultAdded        bool
	previousNumResults int
}

// rollbackDevice 按与施加相反的顺序撤销 state 中记录的所有修改。
func (alloc *allocator) rollbackDevice(r deviceIndices, device deviceWithID, baseRequestName, subRequestName string, state deviceRollbackState) {
	// 截断结果列表，撤销设备结果。
	if state.resultAdded {
		alloc.result[r.claimIndex].devices = alloc.result[r.claimIndex].devices[:state.previousNumResults]
	}
	// 清除设备的使用标记。
	if state.deviceMarked {
		alloc.allocatingDevices[device.id].Delete(r.claimIndex)
	}
	// 按逆序移除已添加的约束。
	for i := state.constraintsAdded - 1; i >= 0; i-- {
		alloc.constraints[r.claimIndex][i].remove(baseRequestName, subRequestName, device.Device, device.id)
	}
	// 释放已预留的计数器。
	if state.countersReserved {
		alloc.deallocateCountersForDevice(device)
	}
	// 只有完整分配的候选才记录结果，此处为真正的回溯撤销。
	if state.resultAdded {
		alloc.logger.V(7).Info("Device deallocated", "device", device.id)
	}
}

// taintPreventsAllocation 判断设备污点是否阻止分配：仅已知 effect 且未被容忍时阻止。
func taintPreventsAllocation(device *draapi.Device, request requestAccessor) bool {
	// 遍历设备污点。
	for _, taint := range device.Taints {
		// 只处理已知 effect。
		switch taint.Effect {
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
	// 逐个检查容忍项。
	for _, toleration := range request.tolerations() {
		// 任一容忍项匹配即可容忍。
		if resourceclaim.ToleratesTaint(toleration, taint) {
			return true
		}
	}
	return false
}

// checkAvailableCounters 校验并预留设备消耗的计数器，必要时首次计算池的可用计数器。
func (alloc *allocator) checkAvailableCounters(device deviceWithID) (bool, error) {
	// 取得设备所属池及其 ID。
	pool := device.pool
	poolID := pool.PoolID

	// 读锁保护缓存访问。
	alloc.mutex.RLock()
	availableCountersForPool, found := alloc.availableCounters[poolID]
	alloc.mutex.RUnlock()
	// 未命中时在锁外计算；各 goroutine 输入相同，结果也相同。
	if !found {
		// 按集合数量预分配。
		availableCountersForPool = make(counterSets, len(pool.CounterSets))
		// 深拷贝每个计数器的值，避免修改原始数据。
		for _, counterSet := range pool.CounterSets {
			availableCountersForCounterSet := make(map[string]resourceapi.Counter, len(counterSet.Counters))
			for name, c := range counterSet.Counters {
				// 深拷贝数量值。
				c.Value = c.Value.DeepCopy()
				availableCountersForCounterSet[name] = c
			}
			availableCountersForPool[counterSet.Name] = availableCountersForCounterSet
		}

		// 同时处理面向与不面向当前节点的切片。
		for _, resourceSlices := range [][]*draapi.ResourceSlice{pool.DeviceSlicesTargetingNode, pool.DeviceSlicesNotTargetingNode} {
			for _, slice := range resourceSlices {
				for _, device := range slice.Spec.Devices {
					// 构造设备 ID 以查询是否已分配。
					deviceID := DeviceID{
						Driver: slice.Spec.Driver,
						Pool:   slice.Spec.Pool.Name,
						Device: device.Name,
					}
					// 未分配的设备不消费计数器，无需考虑。
					if !alloc.allocatedDevices.Has(deviceID) {
						continue
					}
					// 逐项扣除已分配设备消费的计数器。
					for _, deviceCounterConsumption := range device.ConsumesCounters {
						availableCountersForCounterSet := availableCountersForPool[deviceCounterConsumption.CounterSet]
						for name, c := range deviceCounterConsumption.Counters {
							// 查找对应计数器。
							existingCounter, ok := availableCountersForCounterSet[name]
							// 引用不存在的计数器则跳过，API 校验应已保证存在。
							if !ok {
								continue
							}
							// 扣除消费量，可能出现负值，视为无可用计数器。
							existingCounter.Value.Sub(c.Value)
							availableCountersForCounterSet[name] = existingCounter
						}
					}
				}
			}
		}

		// 写锁保护缓存更新。
		alloc.mutex.Lock()
		alloc.availableCounters[poolID] = availableCountersForPool
		alloc.mutex.Unlock()
	}

	// 将当前设备消费的计数器累加到 consumedCounters。
	consumedCountersForPool, found := alloc.consumedCounters[poolID]
	// 首次消费该池计数器，初始化映射。
	if !found {
		consumedCountersForPool = make(counterSets)
		alloc.consumedCounters[poolID] = consumedCountersForPool
	}
	// 按集合与计数器名累加。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		consumedCountersForCounterSet, found := consumedCountersForPool[deviceCounterConsumption.CounterSet]
		if !found {
			consumedCountersForCounterSet = make(map[string]resourceapi.Counter)
			consumedCountersForPool[deviceCounterConsumption.CounterSet] = consumedCountersForCounterSet
		}
		// 逐计数器处理。
		for name, c := range deviceCounterConsumption.Counters {
			// 首次出现时深拷贝，否则累加。
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

	// 逐一比较可用量与已消费量，超限则回滚。
	for availableCounterSetName, availableCounters := range availableCountersForPool {
		consumedCounters := consumedCountersForPool[availableCounterSetName]
		for availableCounterName, availableCounter := range availableCounters {
			consumedCounter := consumedCounters[availableCounterName]
			// 消费量超过可用量，撤销本次预留。
			if availableCounter.Value.Cmp(consumedCounter.Value) < 0 {
				// 回滚计数器消费。
				alloc.deallocateCountersForDevice(device)
				return false, nil
			}
		}
	}

	// 计数器充足，设备可用。
	return true, nil
}

// deviceInUse 判断设备是否已被分配或正在被任意声明分配。
func (alloc *allocator) deviceInUse(deviceID DeviceID) bool {
	return alloc.allocatedDevices.Has(deviceID) || alloc.allocatingDeviceForAnyClaim(deviceID)
}

// allocatingDeviceForAnyClaim 判断设备是否正在被任意声明分配。
func (alloc *allocator) allocatingDeviceForAnyClaim(deviceID DeviceID) bool {
	return alloc.allocatingDevices[deviceID].Len() > 0
}

// allocatingDeviceForClaim 判断设备是否正在被指定声明分配。
func (alloc *allocator) allocatingDeviceForClaim(deviceID DeviceID, claimIndex int) bool {
	return alloc.allocatingDevices[deviceID].Has(claimIndex)
}

// deallocateCountersForDevice 从 consumedCounters 中扣除指定设备消费的计数器。
func (alloc *allocator) deallocateCountersForDevice(device deviceWithID) {
	// 取得设备所属池的已消费计数器。
	poolID := device.pool.PoolID

	consumedCountersForPool := alloc.consumedCounters[poolID]
	// 按集合与计数器名逐项扣减。
	for _, deviceCounterConsumption := range device.ConsumesCounters {
		counterSetName := deviceCounterConsumption.CounterSet
		consumedCounterSet := consumedCountersForPool[counterSetName]
		for name, c := range deviceCounterConsumption.Counters {
			// 取出当前消费量并扣减。
			consumedCounter := consumedCounterSet[name]
			consumedCounter.Value.Sub(c.Value)
			consumedCounterSet[name] = consumedCounter
		}
	}
}

// createNodeSelector 根据已分配设备的位置约束生成节点选择器，设备处处可用时返回 nil。
func (alloc *allocator) createNodeSelector(result []internalDeviceResult) (*v1.NodeSelector, error) {
	// 构造只包含一个 term 的选择器，后续逐步追加要求。
	ns := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{}},
	}

	// 遍历所有已分配设备。
	for i := range result {
		slice := result[i].slice
		var nodeName *string
		var nodeSelector *v1.NodeSelector
		// 按设备选择节点时取设备自身的节点信息，否则取切片的。
		if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			nodeName = result[i].NodeName
			nodeSelector = result[i].NodeSelector
		} else {
			nodeName = slice.Spec.NodeName
			nodeSelector = slice.Spec.NodeSelector
		}
		// 存在节点名时，分配被限制到该节点。
		if nodeName != nil {
			// 生成按 metadata.name 精确匹配的节点选择器。
			return &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{
					MatchFields: []v1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{*nodeName},
					}},
				}},
			}, nil
		}
		// 节点选择器要求需合并到结果中。
		if nodeSelector != nil {
			// 只支持零个或一个 term。
			switch len(nodeSelector.NodeSelectorTerms) {
			case 0:
			// 合并唯一 term 中的要求，避免重复。
			case 1:
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchFields, &ns.NodeSelectorTerms[0].MatchFields)
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchExpressions, &ns.NodeSelectorTerms[0].MatchExpressions)
			// 多个 term 非法，API 校验应已阻止。
			default:
				return nil, fmt.Errorf("unsupported ResourceSlice.NodeSelector with %d terms", len(nodeSelector.NodeSelectorTerms))
			}
		}
	}

	// 收集到有效要求时返回选择器。
	if len(ns.NodeSelectorTerms[0].MatchFields) > 0 || len(ns.NodeSelectorTerms[0].MatchExpressions) > 0 {
		return ns, nil
	}

	// 没有任何位置约束，设备处处可用。
	return nil, nil
}

// requestAccessor 统一 DeviceRequest 与 DeviceSubRequest 的访问方式，使分配逻辑无需区分两者。
type requestAccessor interface {
	// name 返回请求名称。
	name() string
	// deviceClassName 返回引用的设备类名。
	deviceClassName() string
	// allocationMode 返回分配模式。
	allocationMode() resourceapi.DeviceAllocationMode
	// count 返回请求的设备数量。
	count() int64
	// adminAccess 返回是否实际启用管理员访问。
	adminAccess() bool
	// hasAdminAccess 返回是否显式设置了管理员访问。
	hasAdminAccess() bool
	// selectors 返回设备选择器列表。
	selectors() []resourceapi.DeviceSelector
	// tolerations 返回设备容忍列表。
	tolerations() []resourceapi.DeviceToleration
	// derivedAttributes 返回派生属性列表。
	derivedAttributes() []resourceapi.DeviceDerivedAttribute
}

// exactDeviceRequestAccessor 是 requestAccessor 针对 DeviceRequest 的实现。
type exactDeviceRequestAccessor struct {
	// 底层设备请求。
	request *resourceapi.DeviceRequest
}

// name 返回请求的 Name。
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

// adminAccess 返回是否启用管理员访问，未设置时为 false。
func (d *exactDeviceRequestAccessor) adminAccess() bool {
	return ptr.Deref(d.request.Exactly.AdminAccess, false)
}

// hasAdminAccess 返回是否显式设置管理员访问。
func (d *exactDeviceRequestAccessor) hasAdminAccess() bool {
	return d.request.Exactly.AdminAccess != nil
}

// derivedAttributes 返回派生属性，Exactly 为空时返回 nil。
func (d *exactDeviceRequestAccessor) derivedAttributes() []resourceapi.DeviceDerivedAttribute {
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

// deviceSubRequestAccessor 是 requestAccessor 针对 DeviceSubRequest 的实现。
type deviceSubRequestAccessor struct {
	// 底层设备子请求。
	subRequest *resourceapi.DeviceSubRequest
}

// name 返回子请求的 Name。
func (d *deviceSubRequestAccessor) name() string {
	return d.subRequest.Name
}

// deviceClassName 返回子请求的 DeviceClassName。
func (d *deviceSubRequestAccessor) deviceClassName() string {
	return d.subRequest.DeviceClassName
}

// allocationMode 返回子请求的 AllocationMode。
func (d *deviceSubRequestAccessor) allocationMode() resourceapi.DeviceAllocationMode {
	return d.subRequest.AllocationMode
}

// count 返回子请求的 Count。
func (d *deviceSubRequestAccessor) count() int64 {
	return d.subRequest.Count
}

// adminAccess 对子请求恒为 false，子请求不支持管理员访问。
func (d *deviceSubRequestAccessor) adminAccess() bool {
	return false
}

// hasAdminAccess 对子请求恒为 false。
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

// addNewNodeSelectorRequirements 将 from 中尚未存在的节点选择器要求追加到 to。
func addNewNodeSelectorRequirements(from []v1.NodeSelectorRequirement, to *[]v1.NodeSelectorRequirement) {
	// 逐条检查并追加。
	for _, requirement := range from {
		// 已存在的跳过。
		if !containsNodeSelectorRequirement(*to, requirement) {
			*to = append(*to, requirement)
		}
	}
}

// containsNodeSelectorRequirement 判断要求列表中是否已存在键、操作符与值集合完全相同的要求。
func containsNodeSelectorRequirement(requirements []v1.NodeSelectorRequirement, requirement v1.NodeSelectorRequirement) bool {
	// 将值列表转为集合便于比较。
	values := sets.New(requirement.Values...)
	// 遍历现有要求。
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
		// 找到完全相同的要求。
		return true
	}
	return false
}
