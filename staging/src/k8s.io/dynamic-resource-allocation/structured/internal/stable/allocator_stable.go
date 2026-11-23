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

// SupportedFeatures includes all additional features,
// making this the variant that is used when any of those
// are enabled.
// SupportedFeatures 声明本变体支持的所有特性，任一特性启用时即采用此变体。
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
	// availableCounters contains the available counters for each
	// resource pool. It acts as a cache that is updated the first time
	// the available counters are needed for each pool. The information
	// about each pool is never updated once set the first time.
	// This is computed bsed on information on the Allocator, so it will
	// be correct even for multiple usages of the Allocator.
	// The keys in the map are resource pool IDs (driver name and pool name).
	// The allocator might be accessed by different goroutines, so
	// access to this map must be synchronized.
	// availableCounters 按池缓存可用计数器，首次使用时计算，之后不再更新。
	// mutex 保护 availableCounters 的并发访问。
	availableCounters map[PoolID]counterSets
	mutex             sync.RWMutex
	// numAllocateOneInvocations counts the number of times the allocateOne
	// function is called for the allocator. This is a measurement of the
	// amount of work the allocator had to do to allocate devices
	// for the claims.
	// numAllocateOneInvocations 统计 allocateOne 的调用次数，用于衡量分配工作量。
	numAllocateOneInvocations atomic.Int64
}

// 确保 Allocator 实现 internal.AllocatorExtended 接口。
var _ internal.AllocatorExtended = &Allocator{}

// NewAllocator returns an allocator for a certain set of claims or an error if
// some problem was detected which makes it impossible to allocate claims.
//
// The returned Allocator can be used multiple times and is thread-safe.
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
		ctx:                  ctx, // all methods share the same a and thus ctx
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

	// First determine all eligible pools.
	// 先收集当前节点可用的资源池，失败则直接返回错误。
	pools, err := GatherPools(ctx, alloc.slices, node, a.features)
	if err != nil {
		return nil, fmt.Errorf("gather pool information: %w", err)
	}
	// 将池信息存入本次分配状态。
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

	// This is where we sanity check that we can actually handle the claims
	// and their requests. For each claim we determine how many devices
	// need to be allocated. If not all can be stored in the result, the
	// claim cannot be allocated.
	// 统计每个声明所需的最小设备数，并校验请求能否被处理；总数用于预分配。
	minDevicesTotal := 0
	// 逐个声明校验。
	for claimIndex, claim := range alloc.claimsToAllocate {
		minDevicesPerClaim := 0

		// If we have any any request that wants "all" devices, we need to
		// figure out how much "all" is. If some pool is incomplete, we stop
		// here because allocation cannot succeed. Once we do scoring, we should
		// stop in all cases, not just when "all" devices are needed, because
		// pulling from an incomplete might not pick the best solution and it's
		// better to wait. This does not matter yet as long the incomplete pool
		// has some matching device.
		// 遍历声明中的每个设备请求。
		for requestIndex := range claim.Spec.Devices.Requests {
			request := &claim.Spec.Devices.Requests[requestIndex]
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			// 判断是否存在 firstAvailable 子请求。
			hasSubRequests := len(request.FirstAvailable) > 0

			// Error out if the prioritizedList feature is not enabled and the request
			// has subrequests. This is to avoid surprising behavior for users.
			// 未启用 prioritizedList 特性时拒绝带子请求的声明。
			if !a.features.PrioritizedList && hasSubRequests {
				return nil, fmt.Errorf("claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name)
			}

			// 当前请求所需的最小设备数。
			var minDevicesPerRequest int
			// 带子请求时取各子请求中的最小值。
			if hasSubRequests {
				// Seeded high so the loop below can take the minimum.
				// 初始化为最大值，便于求最小。
				minDevicesPerRequest = math.MaxInt
				// A request with subrequests gets one entry per subrequest in alloc.requestData.
				// We can only predict a lower number of devices because it depends on which
				// subrequest gets chosen.
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
			// Compare against the remaining capacity before adding, so a large count cannot overflow the total.
			// 先比较剩余容量再累加，避免大数值造成溢出。
			if minDevicesPerRequest > resourceapi.AllocationResultsMaxSize-minDevicesPerClaim {
				return nil, fmt.Errorf("claim %s, request %s: adding %d devices exceeds the claim limit of %d, with %d already accounted for", klog.KObj(claim), request.Name, minDevicesPerRequest, resourceapi.AllocationResultsMaxSize, minDevicesPerClaim)
			}
			// 累加到当前声明。
			minDevicesPerClaim += minDevicesPerRequest
		}
		// 记录声明校验结果。
		alloc.logger.V(6).Info("Checked claim", "claim", klog.KObj(claim), "minDevices", minDevicesPerClaim)

		// Only a lower bound: a subrequest may pick more, so allocateOne checks the limit again.
		// 为结果切片预留容量；这只是下界，allocateOne 中会再次检查上限。
		alloc.result[claimIndex].devices = make([]internalDeviceResult, 0, minDevicesPerClaim)

		// Constraints are assumed to be monotonic: once a constraint returns
		// false, adding more devices will not cause it to return true. This
		// allows the search to stop early once a constraint returns false.
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
				// Unknown constraint type!
				return nil, fmt.Errorf("claim %s, constraint #%d: empty constraint (unsupported constraint type?)", klog.KObj(claim), i)
			}
		}
		// 将约束列表存入本次分配状态。
		alloc.constraints[claimIndex] = constraints
		// 累计到全局最小设备数。
		minDevicesTotal += minDevicesPerClaim
	}

	// Selecting a device for a request is independent of what has been
	// allocated already. Therefore the result of checking a request against
	// a device instance in the pool can be cached. The pointer to both
	// can serve as key because they are static for the duration of
	// the Allocate call and can be compared in Go.
	// 设备与请求的匹配结果可安全缓存，因为它们在单次 Allocate 期间不变。
	alloc.deviceMatchesRequest = make(map[matchKey]bool)

	// We can estimate the size based on what we need to allocate.
	// 按最小设备数预分配正在分配的设备表。
	alloc.allocatingDevices = make(map[DeviceID]sets.Set[int], minDevicesTotal)

	// 记录已分配设备与待分配设备数。
	alloc.logger.V(5).Info("Gathered information about devices", "allocatedDevices", logAllocatedDevices(alloc.logger, alloc.allocatedDevices), "minDevicesToBeAllocated", minDevicesTotal)

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
		// If no devices could be allocated, but we found one or more
		// invalid pools, return an error here. We didn't do it during
		// allocation since there might be valid pools from which the
		// claims could be satisfied.
		// 若存在无效池，返回非致命错误以便其他节点继续尝试。
		for _, pool := range pools {
			if pool.IsInvalid {
				// Not a fatal error, allocation on other nodes may proceed.
				// The error is only surfaced if allocation fails on all nodes.
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

		// Populate configs.

		// Each class config gets added only once.
		// We need to keep track of which class configs have already been added and at which position in the allocationResult.Devices.Config.
		// 填充设备配置；每个设备类的配置只添加一次，并记录其在结果中的位置。
		// configRange 记录某个设备类的配置在结果切片中的区间。
		type configRange struct{ start, end int }
		// 以类名为键记录已添加配置的区间。
		configIndexesForClass := make(map[string]configRange) // Key: class name / Value: position of the configs for the class in allocationResult.Devices.Config.
		// 遍历请求以收集类配置。
		for requestIndex := range claim.Spec.Devices.Requests {
			requestKey := requestIndices{claimIndex: claimIndex, requestIndex: requestIndex}
			requestData := alloc.requestData[requestKey]
			// 子请求需使用被选中的子请求所属的设备类。
			if requestData.parentRequest != nil {
				// We need the class of the selected subrequest.
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
				// The configs for the class have already been added.
				// Just append the request name for the request class.
				for i := configIndexes.start; i < configIndexes.end; i++ {
					allocationResult.Devices.Config[i].Requests = append(allocationResult.Devices.Config[i].Requests, requestData.requestName())
				}
				continue
			}

			// Add all configs for the class once.
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
			// If Requests are empty, it applies to all. So it can just be included.
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
					break // We can stop checking after the first match as all requests in the config should be added.
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
					break // We can stop checking after the first match as all requests in the config should be added.
				}
			}
		}

		// If a config applies to all requests, clear its Requests
		// field to take advantage of the "empty means all" semantic.
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

		// Determine node selector.
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
	// The DerivedAttributes feature is currently an alpha-only feature and is
	// not supported in the stable variant. Implementation will be added
	// once the feature's support is extended to this variant.
	// 稳定变体不支持派生属性，出现即报错。
	if len(request.derivedAttributes()) > 0 {
		return requestData, fmt.Errorf("claim %s, request %s: cannot handle request with derived attributes, the feature is disabled", klog.KObj(claim), request.name())
	}
	// 校验所有选择器都包含 CEL 表达式。
	for i, selector := range request.selectors() {
		// 未知的选择器类型。
		if selector.CEL == nil {
			// Unknown future selector type!
			return requestData, fmt.Errorf("claim %s, request %s, selector #%d: CEL expression empty (unsupported selector type?)", klog.KObj(claim), request.name(), i)
		}
	}

	// 未启用 admin access 特性时拒绝相应请求。
	if !alloc.features.AdminAccess && request.hasAdminAccess() {
		return requestData, fmt.Errorf("claim %s, request %s: admin access is requested, but the feature is disabled", klog.KObj(claim), request.name())
	}

	// Should be set. If it isn't, something changed and we should refuse to proceed.
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

	// Start collecting information about the request.
	// The class must be set and stored before calling isSelectable.
	// 类必须在调用 isSelectable 之前设置并保存。
	requestData.class = class

	// 按分配模式计算所需设备数。
	switch request.allocationMode() {
	// 精确数量模式。
	case resourceapi.DeviceAllocationModeExactCount:
		numDevices := request.count()
		// 数量超出 int 范围，无法处理。
		if numDevices > math.MaxInt {
			// Allowed by API validation, but doesn't make sense.
			return requestData, fmt.Errorf("claim %s, request %s: exact count %d is too large", klog.KObj(claim), request.name(), numDevices)
		}
		// 记录精确数量。
		requestData.numDevices = int(numDevices)
	// all 模式：收集所有可选择的设备。
	case resourceapi.DeviceAllocationModeAll:
		// If we have any any request that wants "all" devices, we need to
		// figure out how much "all" is. If some pool is incomplete, we stop
		// here because allocation cannot succeed. Once we do scoring, we should
		// stop in all cases, not just when "all" devices are needed, because
		// pulling from an incomplete might not pick the best solution and it's
		// better to wait. This does not matter yet as long the incomplete pool
		// has some matching device.
		// 预分配候选设备列表。
		requestData.allDevices = make([]deviceWithID, 0, resourceapi.AllocationResultsMaxSize)
		// 遍历所有资源池。
		for _, pool := range pools {
			// 不完整的池无法支持 all 请求。
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

// errStop is a special error that gets returned by allocateOne if it detects
// that allocation cannot succeed.
// errStop 是 allocateOne 判定分配无法成功时返回的提前终止信号。
var errStop = errors.New("stop allocation")

// errAllocationResultMaxSizeExceeded is a special error that gets return by
// allocatedOne when the number of allocated devices exceeds the max number
// allowed. This is checked by earlier invocations in the recursion and used
// to do more aggressive backtracking and avoid attempting allocations that
// we know can not succeed.
// errAllocationResultMaxSizeExceeded 表示设备数超过上限，用于触发更积极的回溯。
var errAllocationResultMaxSizeExceeded = errors.New("allocation max size exceeded")

// allocator is used while an [Allocator.Allocate] is running. Only a single
// goroutine works with it, so there is no need for locking.
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
	constraints          [][]constraint // one list of constraints per claim
	// consumedCounters keeps track of the counters consumed by all devices
	// that are in the process of being allocated.
	// The keys in the map are resource pool IDs (driver name and pool name).
	// consumedCounters 记录本次分配过程中所有正在分配设备消耗的计数器。
	// requestData 为每个请求或子请求对应的数据。
	consumedCounters map[PoolID]counterSets
	requestData      map[requestIndices]requestData // one entry per request with no subrequests and one entry per subrequest
	// allocatingDevices tracks which devices will be newly allocated for a
	// particular attempt to find a solution. The map is indexed by device
	// and its values represent for which of a pod's claims the device will
	// be allocated.
	// Claims are identified by their index in claimsToAllocate.
	// allocatingDevices 记录某次尝试中将被新分配的设备及其对应的声明索引。
	// result 为每个声明的内部分配结果。
	allocatingDevices map[DeviceID]sets.Set[int]
	result            []internalAllocationResult
}

// counterSets is a map with the name of counter sets to the counters in
// the set.
// counterSets 以计数器集合名为键，映射到该集合内计数器名到计数器的映射。
type counterSets map[draapi.UniqueString]map[string]resourceapi.Counter

// matchKey identifies a device/request pair.
// matchKey 由设备 ID 与请求索引组成，唯一标识一次设备/请求匹配。
type matchKey struct {
	// 设备 ID。
	DeviceID
	// 请求索引。
	requestIndices
}

// requestIndices identifies one specific request
// or subrequest by three properties:
//
// - claimIndex: The index of the claim in the requestData map.
// - requestIndex: The index of the request in the claim.
// - subRequestIndex: The index of the subrequest in the parent request.
// requestIndices 通过声明索引、请求索引与子请求索引唯一定位一个请求或子请求。
type requestIndices struct {
	// claimIndex 与 requestIndex 为声明与请求在各自列表中的索引。
	// subRequestIndex 为子请求在父请求中的索引。
	claimIndex, requestIndex int
	subRequestIndex          int
}

// deviceIndices identifies one specific required device inside
// a request or subrequest of a certain claim.
// deviceIndices 定位某个声明的请求或子请求中所需的一个具体设备。
type deviceIndices struct {
	// claimIndex 为声明在分配器中的索引。
	// requestIndex 为请求在声明中的索引。
	// subRequestIndex 为子请求在请求中的索引，非子请求时忽略。
	// deviceIndex 为设备在请求或子请求中的索引。
	claimIndex      int // The index of the claim in the allocator.
	requestIndex    int // The index of the request in the claim.
	subRequestIndex int // The index of the subrequest within the request (ignored if subRequest is false).
	deviceIndex     int // The index of a device within a request or subrequest.
}

// requestData 保存单个请求或子请求在校验阶段得到的元数据。
type requestData struct {
	// The request or subrequest which needs to be allocated.
	// Never nil.
	// 需要分配的请求或子请求，永不为 nil。
	request requestAccessor
	// The parent of a subrequest, nil if not a subrequest.
	// parentRequest 为子请求的父请求，非子请求时为 nil。
	// class 为请求引用的设备类。
	// numDevices 为该请求需要的设备数量。
	parentRequest requestAccessor
	class         *resourceapi.DeviceClass
	numDevices    int

	// selectedSubRequestIndex is set for the entry with requestIndices.subRequestIndex == 0.
	// It is the index of the subrequest which got picked during allocation.
	// 被选中的子请求索引，仅在 subRequestIndex 为 0 的条目上设置。
	selectedSubRequestIndex int

	// pre-determined set of devices for allocating "all" devices
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
	request       string // name of the request (if no subrequests) or the subrequest
	parentRequest string // name of the request which contains the subrequest, empty otherwise
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
			// No need to check sub-requests.
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
	// add is called whenever a device is about to be allocated. It must
	// check whether the device matches the constraint and if yes,
	// track that it is allocated.
	// add 在设备即将分配时调用，判断设备是否满足约束并在满足时记录。
	add(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID) bool

	// For every successful add there is exactly one matching removed call
	// with the exact same parameters.
	// remove 与成功的 add 一一对应，参数完全相同，用于撤销记录。
	remove(requestName, subRequestName string, device *draapi.Device, deviceID DeviceID)
}

// matchAttributeConstraint compares an attribute value across devices.
// All devices must share the same value. When the set of devices is
// empty, any device that has the attribute can be added. After that,
// only matching devices can be added.
//
// We don't need to track *which* devices are part of the set, only
// how many.
// matchAttributeConstraint 要求一组设备的同名属性值完全一致，只需记录已加入的设备数量。
type matchAttributeConstraint struct {
	// logger 为已附加名称与属性名的日志记录器。
	// requestNames 为受该约束影响的请求名集合。
	// attributeName 为待比较的属性名。
	logger        klog.Logger // Includes name and attribute name, so no need to repeat in log messages.
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
		// Device not affected by constraint.
		m.logger.V(7).Info("Constraint does not apply to request", "request", requestName)
		return true
	}

	// 查找设备上的目标属性。
	attribute := lookupAttribute(device, deviceID, m.attributeName)
	// 设备没有该属性，不满足约束。
	if attribute == nil {
		// Doesn't have the attribute.
		m.logger.V(7).Info("Constraint not satisfied, attribute not set")
		return false
	}

	// 第一台设备可作为基准。
	if m.numDevices == 0 {
		// The first device can always get picked.
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
	// 版本类型可用字符串严格比较，因为 semver 要求规范形式。
	case attribute.VersionValue != nil:
		// semver 2.0.0 requires that version strings are in their
		// minimal form (in particular, no leading zeros). Therefore a
		// strict "exact equal" check can do a string comparison.
		if m.attribute.VersionValue == nil || *attribute.VersionValue != *m.attribute.VersionValue {
			m.logger.V(7).Info("Version values different")
			return false
		}
	// 未知类型无法匹配。
	default:
		// Unknown value type, cannot match.
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
		// Device not affected by constraint.
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
	// Fully-qualified match?
	// 先尝试完全限定名。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName)]; ok {
		return &attr
	}
	// 查找域名与属性名的分隔符。
	index := strings.Index(string(attributeName), "/")
	// 非法完全限定名，无法匹配。
	if index < 0 {
		// Should not happen for a valid fully qualified name.
		return nil
	}

	// 域名与驱动名不一致，属性不可用。
	if string(attributeName[0:index]) != deviceID.Driver.String() {
		// Not an attribute of the driver and not found above,
		// so it is not available.
		return nil
	}

	// Domain matches the driver, so let's check just the ID.
	// 域名匹配驱动名，用属性短名再次查找。
	if attr, ok := device.Attributes[resourceapi.QualifiedName(attributeName[index+1:])]; ok {
		return &attr
	}

	return nil
}

// allocateOne iterates over all eligible devices (not in use, match selector,
// satisfy constraints) for a specific required device. It returns true if
// everything got allocated, an error if allocation needs to stop.
//
// allocateSubRequest is true when trying to allocate one particular subrequest.
// This allows the logic for subrequests to call allocateOne with the same
// device index without causing infinite recursion.
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
		// Done! If we were doing scoring, we would compare the current allocation result
		// against the previous one, keep the best, and continue. Without scoring, we stop
		// and use the first solution.
		// 当前无评分机制，直接使用第一个解。
		alloc.logger.V(6).Info("Allocation result found")
		return true, nil
	}

	// 取得当前声明。
	claim := alloc.claimsToAllocate[r.claimIndex]
	// 当前声明处理完毕，进入下一个声明。
	if r.requestIndex >= len(claim.Spec.Devices.Requests) {
		// Done with the claim, continue with the next one.
		// 递归处理下一个声明。
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex + 1}, false)
		// 上限错误不是致命错误，换其他设备重试可能成功。
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

	// r.subRequestIndex is zero unless the for loop below is in the
	// recursion chain.
	// 当前请求或子请求的数据来自 requestData。
	requestKey := requestIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex}
	requestData := alloc.requestData[requestKey]

	// Subrequests are special: we only need to allocate one of them, then
	// we can move on to the next request. We enter this for loop when
	// hitting the first subrequest, but not if we are already working on a
	// specific subrequest.
	// 子请求只需选中一个即可进入下一请求；尚未选定时遍历所有子请求。
	if !allocateSubRequest && requestData.parentRequest != nil {
		// Keep track of whether all attempts to do allocation with the
		// subrequests results in the allocation result limit exceeded.
		// If so, there is no need to make attempts with other devices
		// in the previous request (if any), except when
		// it is a firstAvailable request where some sub-requests
		// need less devices than others.
		// 记录是否所有子请求都因超出上限而失败。
		allAllocationExceeded := true
		// 依次尝试每个子请求。
		for subRequestIndex := 0; ; subRequestIndex++ {
			// 构造下一个子请求的索引。
			nextSubRequestKey := requestKey
			nextSubRequestKey.subRequestIndex = subRequestIndex
			// 已遍历完所有子请求。
			if _, ok := alloc.requestData[nextSubRequestKey]; !ok {
				// Past the end of the subrequests without finding a solution -> give up.
				//
				// Return errAllocationResultMaxSizeExceeded if all
				// attempts for the subrequests failed to due to reaching
				// the max size limit. This would mean that there are no
				// solution that involves the previous request (if any).
				// 全部因上限失败，向上传递该错误以触发更积极回溯。
				if allAllocationExceeded {
					return false, errAllocationResultMaxSizeExceeded
				}
				return false, nil
			}

			// 设置当前要尝试的子请求索引。
			r.subRequestIndex = subRequestIndex
			// 尝试该子请求，传入 true 防止再次进入子请求循环。
			success, err := alloc.allocateOne(r, true /* prevent infinite recusion */)
			// If we reached the allocation result limit, we can try
			// with the next subrequest if there is one. It might request
			// fewer devices, so it might succeed.
			// 上限错误可尝试下一个可能更省设备的子请求。
			if errors.Is(err, errAllocationResultMaxSizeExceeded) {
				continue
			}
			// If we get here, at least one of the subrequests failed for a
			// different reason than errAllocationResultMaxSizeExceeded.
			// 至少有一个子请求因其他原因失败。
			allAllocationExceeded = false
			if err != nil {
				return false, err
			}
			// If allocation with a subrequest succeeds, return without
			// attempting the remaining subrequests.
			// 子请求分配成功，记录其索引。
			if success {
				// Store the index of the selected subrequest
				// 保存选中的子请求。
				requestData.selectedSubRequestIndex = subRequestIndex
				alloc.requestData[requestKey] = requestData
				return true, nil
			}
		}
		// This is unreachable, so no need to have a return statement here.
	}

	// Look up the current request that we are attempting to satisfy. This can
	// be either a request or a subrequest.
	// 取得当前要满足的请求，可能是子请求。
	request := requestData.request
	// 是否为 all 模式。
	doAllDevices := request.allocationMode() == resourceapi.DeviceAllocationModeAll

	// At least one device is required for 'All' allocation mode.
	// all 模式至少要有一台设备，没有候选则无解。
	if doAllDevices && len(requestData.allDevices) == 0 {
		alloc.logger.V(6).Info("Allocation for 'all' devices didn't succeed: no devices found", "claim", klog.KObj(claim), "request", requestData.request.name())
		return false, nil
	}

	// We already know how many devices per request are needed.
	// 当前请求所需设备已分配完，进入下一请求。
	if r.deviceIndex >= requestData.numDevices {
		// Done with request, continue with next one. We have completed the work for
		// the request or subrequest, so we can no longer be allocating devices for
		// a subrequest.
		// 进入下一个请求。
		success, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex + 1}, false)
		// We want to propagate any errAllocationResultMaxSizeExceeded to the caller. If
		// that error is returned here, it means none of the requests/subrequests after this one
		// could be allocated while staying within the limit on the number of devices, so there
		// are no solution in the current request/subrequest that would work.
		return success, err
	}

	// Compare rather than add, so a large count cannot wrap the sum. deviceIndex
	// is below numDevices here, checked above, so remaining stays positive.
	// 计算还需分配的设备数与结果剩余容量。
	remaining := requestData.numDevices - r.deviceIndex
	// 结果列表剩余容量。
	available := resourceapi.AllocationResultsMaxSize - len(alloc.result[r.claimIndex].devices)
	// 容量不足，触发回溯。
	if remaining > available {
		// Return a special error so we can identify this situation in the
		// callers and do more aggressive backtracking.
		// 返回特殊错误以便上层更积极回溯。
		return false, errAllocationResultMaxSizeExceeded
	}

	// 记录正在分配一台设备的上下文。
	alloc.logger.V(6).Info("Allocating one device", "currentClaim", r.claimIndex, "totalClaims", len(alloc.claimsToAllocate), "currentRequest", r.requestIndex, "currentSubRequest", r.subRequestIndex, "totalRequestsPerClaim", len(claim.Spec.Devices.Requests), "currentDevice", r.deviceIndex, "devicesPerRequest", requestData.numDevices, "allDevices", doAllDevices, "adminAccess", request.adminAccess())
	// all 模式下候选设备已确定，只需校验可用性。
	if doAllDevices {
		// For "all" devices we already know which ones we need. We
		// just need to check whether we can use them.
		// 取下一个候选设备。
		deviceWithID := requestData.allDevices[r.deviceIndex]
		// must 为 true：约束不满足时视为错误。
		success, deallocate, err := alloc.allocateDevice(r, deviceWithID, true)
		if err != nil {
			return false, err
		}
		// all 设备的顺序无关，当前设备不可用即无解。
		if !success {
			// The order in which we allocate "all" devices doesn't matter,
			// so we only try with the one which was up next. If we couldn't
			// get all of them, then there is no solution and we have to stop.
			return false, nil
		}
		// 递归分配下一台设备。
		done, err := alloc.allocateOne(deviceIndices{claimIndex: r.claimIndex, requestIndex: r.requestIndex, subRequestIndex: r.subRequestIndex, deviceIndex: r.deviceIndex + 1}, allocateSubRequest)
		// 失败或出错时回滚。
		if err != nil || !done {
			// If we get an error or didn't complete, we need to backtrack. Depending
			// on the situation we might be able to retry, so we make sure we
			// deallocate.
			// 撤销该设备的临时分配。
			deallocate()
			return false, err
		}
		return done, nil
	}

	// We need to find suitable devices.
	// 普通模式需要遍历所有池寻找合适设备。
	for _, pool := range alloc.pools {
		// We don't allocate devices from invalid or incomplete pools, but
		// don't error out here since there might be available devices in other
		// pools.
		// 无效或不完整的池不参与分配，但此时不报错。
		if pool.IsIncomplete || pool.IsInvalid {
			continue
		}
		// 遍历面向当前节点的切片。
		for _, slice := range pool.DeviceSlicesTargetingNode {
			// 遍历设备候选。
			for deviceIndex := range slice.Spec.Devices {
				deviceID := DeviceID{Driver: pool.Driver, Pool: pool.Pool, Device: slice.Spec.Devices[deviceIndex].Name}

				// Checking for "in use" is cheap and thus gets done first.
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

				// Next check selectors.
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

				// Finally treat as allocated and move on to the next device.
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
					// In use or constraint violated...
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
				// If we found a solution, we can stop.
				if err == nil && done {
					// 找到解则保留当前分配并返回。
					return done, nil
				}

				// Otherwise we didn't find a solution, and we need to deallocate
				// so the temporary allocation is correct for trying other devices.
				// 未找到解，撤销临时分配后再尝试其他设备。
				deallocate()

				// 遇到错误如超出上限，回滚后返回，由上层处理。
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

	// If we get here without finding a solution, then there is none.
	// 遍历完所有候选仍无解。
	return false, nil
}

// isSelectable checks whether a device satisfies the request and class selectors.
// isSelectable 检查设备是否满足请求与设备类的选择器以及节点选择条件，并缓存匹配结果。
func (alloc *allocator) isSelectable(r requestIndices, requestData requestData, slice *draapi.ResourceSlice, deviceIndex int) (bool, error) {
	// 取得设备对象。
	device := &slice.Spec.Devices[deviceIndex]
	// 特性关闭时不支持带 bindingConditions 的设备。
	if !alloc.features.DeviceBindingAndStatus &&
		len(device.BindingConditions) > 0 {
		// Devices with binding conditions are not supported, feature is off.
		return false, nil
	}

	// 构造设备 ID。
	deviceID := DeviceID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name, Device: slice.Spec.Devices[deviceIndex].Name}
	// 构造缓存键。
	matchKey := matchKey{DeviceID: deviceID, requestIndices: r}
	// 命中缓存直接返回。
	if matches, ok := alloc.deviceMatchesRequest[matchKey]; ok {
		// No need to check again.
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
		// 编译错误通常是新集群表达式与旧版本不兼容所致。
		if expr.Error != nil {
			// Could happen if some future apiserver accepted some
			// future expression and then got downgraded. Normally
			// the "stored expression" mechanism prevents that, but
			// this code here might be more than one release older
			// than the cluster it runs in.
			if class != nil {
				return false, fmt.Errorf("class %s: selector #%d: CEL compile error: %w", class.Name, i, expr.Error)
			}
			return false, fmt.Errorf("claim %s: selector #%d: CEL compile error: %w", klog.KObj(alloc.claimsToAllocate[r.claimIndex]), i, expr.Error)
		}

		// If this conversion turns out to be expensive, the CEL package could be converted
		// to use unique strings.
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

	// All of them match.
	// 所有选择器都匹配。
	return true, nil
}

// allocateDevice checks device availability and constraints for one
// candidate. The device must be selectable.
//
// If that candidate works out okay, the shared state gets updated
// as if that candidate had been allocated. If allocation cannot continue later
// and must try something else, then the rollback function can be invoked to
// restore the previous state.
//
// The rollback function is only provided in case of a successful allocation
// (true and no error).
// allocateDevice 检查候选设备的占用、计数器、污点与约束；成功时更新共享状态并返回回滚闭包。
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

	// Devices that consume counters can not be allocated if the PartitionableDevices feature
	// is not enabled.
	// 分区特性关闭时不允许分配消费计数器的设备。
	if !alloc.features.PartitionableDevices && len(device.ConsumesCounters) > 0 {
		alloc.logger.V(7).Info("Device consumes counters, but the partitionable devices feature is not enabled", "device", device.id)
		return false, nil, nil
	}

	// The API validation logic has checked the ConsumesCounters referred should exist inside SharedCounters.
	// countersReserved records whether checkAvailableCounters actually reserved
	// this device's counters, so the rollback below only releases what was taken.
	// 记录是否已预留计数器，确保回滚只释放已预留部分。
	countersReserved := false
	// 消费计数器时先校验可用性。
	if len(device.ConsumesCounters) > 0 {
		// If a device consumes counters from a counter set, verify that
		// there is sufficient counters available.
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

	// state records the mutations this call makes so rollbackDevice can undo
	// them. Every rejection path after a successful counter reservation calls
	// rollbackDevice synchronously, and the success path returns a closure that
	// calls the same helper during backtracking, so both undo routes stay
	// identical. Passing the state by value to rollbackDevice keeps it (and the flags) on the
	// stack for the rejection paths; only the success closure escapes.
	// 用 state 记录本次调用对共享状态的修改，供回滚使用。
	state := deviceRollbackState{
		countersReserved:   countersReserved,
		previousNumResults: len(alloc.result[r.claimIndex].devices),
	}

	// Might be tainted, in which case the taint has to be tolerated.
	// The check is skipped if the feature is disabled.
	// 设备可能带污点，需被容忍才能分配；未被容忍则回滚并拒绝。
	if alloc.features.DeviceTaints && taintPreventsAllocation(device.Device, request) {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
		return false, nil, nil
	}

	// It's available. Now check constraints.
	// 设备可用，依次应用声明的所有约束。
	for _, constraint := range alloc.constraints[r.claimIndex] {
		// 约束不满足时回滚。
		if !constraint.add(baseRequestName, subRequestName, device.Device, device.id) {
			alloc.rollbackDevice(r, device, baseRequestName, subRequestName, state)
			// all 模式下约束冲突视为错误。
			if must {
				// It does not make sense to declare a claim where a constraint prevents getting
				// all devices. Treat this as an error.
				return false, nil, fmt.Errorf("claim %s, request %s: cannot add device %s because a claim constraint would not be satisfied", klog.KObj(claim), request.name(), device.id)
			}
			return false, nil, nil
		}
		// 记录已成功添加的约束数量。
		state.constraintsAdded++
	}

	// All constraints satisfied. Mark as in use (unless we do admin access)
	// and record the result.
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

	// Only this success path builds an escaping closure, so backtracking can undo
	// a committed candidate. undo is a copy: capturing state directly would move it
	// and its flags to the heap on the rejection paths too, which never escape.
	// 成功路径返回回滚闭包，回溯时撤销该候选；复制状态以避免逃逸。
	undo := state
	// 返回成功标识与回滚函数。
	return true, func() {
		alloc.rollbackDevice(r, device, baseRequestName, subRequestName, undo)
	}, nil
}

// deviceRollbackState records the mutations allocateDevice makes for a single
// candidate so rollbackDevice can undo them, both when the candidate is rejected
// and when the backtracking search abandons a previously successful candidate.
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

// rollbackDevice reverses the mutations recorded in state, in the opposite order
// they were applied. It is called synchronously on the rejection paths and, via
// the closure returned on success, during backtracking.
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
		// Only a fully allocated candidate recorded a result, so this is a real
		// deallocation during backtracking, not a rejection rollback.
		alloc.logger.V(7).Info("Device deallocated", "device", device.id)
	}
}

// taintPreventsAllocation 判断设备污点是否阻止分配：仅已知 effect 且未被容忍时阻止。
func taintPreventsAllocation(device *draapi.Device, request requestAccessor) bool {
	// 遍历设备污点。
	for _, taint := range device.Taints {
		// 只处理已知 effect。
		switch taint.Effect {
		// Only known effects prevent allocation, others (including None) are ignored.
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

// checkAvailableCounters checks if there are enough counters available to allocate
// the specified device.
//
// Gets called only if the partitionable devices feature is enabled and the device
// consumes counters.
// checkAvailableCounters 校验并预留设备消耗的计数器，必要时首次计算池的可用计数器。
func (alloc *allocator) checkAvailableCounters(device deviceWithID) (bool, error) {
	// 取得设备所属池及其 ID。
	pool := device.pool
	poolID := pool.PoolID

	// Check first if the available counters for this pool have already been
	// calculated.
	// 读锁保护缓存访问。
	alloc.mutex.RLock()
	availableCountersForPool, found := alloc.availableCounters[poolID]
	alloc.mutex.RUnlock()
	// If not, we need to do it now. But we store the result so it doesn't need
	// to be calculated again.
	// Since this is computed without holding the lock on the mutex, other goroutines
	// might also do this work. But the input will be the same to all of them, so
	// the result will also always be the same.
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

		// Update the data structure to reflect counters already consumed by allocated devices. This
		// only includes devices where the allocation process has completed, so this will never
		// change during the allocation process.
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
					// Devices that aren't allocated doesn't consume any counters, so we don't
					// need to consider them.
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
								// the API validation logic has been added to make sure the counters referred should exist in counter sets.
								continue
							}
							// This can potentially result in negative available counters. That is fine,
							// we just treat it as no counters available.
							// 扣除消费量，可能出现负值，视为无可用计数器。
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

		// Set the available counters on the allocator so we don't have to
		// compute this again.
		// 写锁保护缓存更新。
		alloc.mutex.Lock()
		alloc.availableCounters[poolID] = availableCountersForPool
		alloc.mutex.Unlock()
	}

	// Update the consumedCounters data structure with the counters consumed
	// by the current device.
	// 将当前设备消费的计数器累加到 consumedCounters。
	consumedCountersForPool, found := alloc.consumedCounters[poolID]
	// If no devices in the allocating state have consumed any counters from the current
	// pool, initialize the data structure.
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

	// Check that we didn't exceed the availability of any counters by allocating
	// the current device. If we did, the current set of devices doesn't work, so we
	// update the consumed counters to no longer reflect the current device.
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

// deallocateCountersForDevice subtracts the consumed counters of the provided
// device from the consumedCounters data structure.
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

// createNodeSelector constructs a node selector for the allocation, if needed,
// otherwise it returns nil.
// createNodeSelector 根据已分配设备的位置约束生成节点选择器，设备处处可用时返回 nil。
func (alloc *allocator) createNodeSelector(result []internalDeviceResult) (*v1.NodeSelector, error) {
	// Selector with one term. That term gets extended with additional
	// requirements from the different devices.
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
			// At least one device is local to one node. This
			// restricts the allocation to that node.
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
				// Nothing?
			// 合并唯一 term 中的要求，避免重复。
			case 1:
				// Add all terms if they are not present already.
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchFields, &ns.NodeSelectorTerms[0].MatchFields)
				addNewNodeSelectorRequirements(nodeSelector.NodeSelectorTerms[0].MatchExpressions, &ns.NodeSelectorTerms[0].MatchExpressions)
			// 多个 term 非法，API 校验应已阻止。
			default:
				// This shouldn't occur, validation must prevent creation of such slices.
				return nil, fmt.Errorf("unsupported ResourceSlice.NodeSelector with %d terms", len(nodeSelector.NodeSelectorTerms))
			}
		}
	}

	// 收集到有效要求时返回选择器。
	if len(ns.NodeSelectorTerms[0].MatchFields) > 0 || len(ns.NodeSelectorTerms[0].MatchExpressions) > 0 {
		// We have a valid node selector.
		return ns, nil
	}

	// Available everywhere.
	// 没有任何位置约束，设备处处可用。
	return nil, nil
}

// requestAccessor is an interface for accessing either
// DeviceRequests or DeviceSubRequests. It lets most
// of the allocator code work with either DeviceRequests
// or DeviceSubRequests.
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

// exactDeviceRequestAccessor is an implementation of the
// requestAccessor interface for ExactDeviceRequests.
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

// deviceSubRequestAccessor is an implementation of the
// requestAccessor interface for DeviceSubRequests.
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
