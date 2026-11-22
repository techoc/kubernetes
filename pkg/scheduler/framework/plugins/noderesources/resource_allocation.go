/*
Copyright 2017 The Kubernetes Authors.

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

package noderesources

import (
	"context"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	resourceapi "k8s.io/api/resource/v1"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// scorer 是 resourceAllocationScorer 的装饰函数：传入评分策略，返回对应的打分器实例。
type scorer func(strategy *config.ScoringStrategy) *resourceAllocationScorer

// DRACaches 保存 DRA（动态资源分配）相关计算所用的各类缓存。
type DRACaches struct {
	// celCache 缓存设备类选择器中已编译的 CEL 表达式，避免重复编译。
	celCache *cel.Cache
	// 缓存 DeviceMatches 的结果，避免代价高昂的重复求值。
	deviceMatchCache sync.Map // map[deviceMatchCacheKey]bool
	// 缓存 NodeMatches 的结果，避免代价高昂的重复节点选择器求值。
	nodeMatchCache sync.Map // map[nodeMatchCacheKey]bool
}

// resourceAllocationScorer 封装计算资源分配得分所需的信息。
type resourceAllocationScorer struct {
	Name                                          string
	enableInPlacePodVerticalScaling               bool
	enablePodLevelResources                       bool
	enableDRAExtendedResource                     bool
	enableInPlacePodLevelResourcesVerticalScaling bool
	// 决定 cpu 与 memory 使用 Requested 还是 NonZeroRequested：
	// 后者忽略默认请求，只统计显式设置的值。
	useRequested bool
	scorer       func(requested, allocated, allocatable []int64) int64
	resources    []config.ResourceSpec
	draFeatures  structured.Features
	draManager   fwk.SharedDRAManager
	// DRA 相关计算所用的缓存。
	DRACaches
}

// buildNodeMatchCacheKey 为节点匹配结果构造字符串缓存键。
// 用字符串键比在 sync.Map 中使用结构体键快得多。
func buildNodeMatchCacheKey(nodeName string, nodeNameToMatch string, allNodesMatch bool, nodeSelectorHash string) string {
	// 预先分配足够容量，避免写入过程中反复扩容。
	var b strings.Builder
	b.Grow(len(nodeName) + len(nodeNameToMatch) + len(nodeSelectorHash) + 4)

	b.WriteString(nodeName)
	b.WriteByte('|')
	b.WriteString(nodeNameToMatch)
	b.WriteByte('|')
	if allNodesMatch {
		b.WriteByte('1')
	} else {
		b.WriteByte('0')
	}
	b.WriteByte('|')
	b.WriteString(nodeSelectorHash)

	return b.String()
}

// buildDeviceMatchCacheKey 为设备匹配结果构造字符串缓存键。
// 用字符串键比在 sync.Map 中使用结构体键快得多。
// 以竖线分隔拼接 expression|driver|poolName|deviceName。
func buildDeviceMatchCacheKey(expression string, driver string, poolName string, deviceName string) string {
	// 预先分配足够容量，避免写入过程中反复扩容。
	var b strings.Builder
	b.Grow(len(expression) + len(driver) + len(poolName) + len(deviceName) + 3)

	b.WriteString(expression)
	b.WriteByte('|')
	b.WriteString(driver)
	b.WriteByte('|')
	b.WriteString(poolName)
	b.WriteByte('|')
	b.WriteString(deviceName)

	return b.String()
}

// nodeMatches 是 structured.NodeMatches 的带缓存包装。
func (r *resourceAllocationScorer) nodeMatches(node *v1.Node, nodeNameToMatch string, allNodesMatch bool, nodeSelector *v1.NodeSelector) (bool, error) {

	var nodeName string
	if node != nil {
		nodeName = node.Name
	}

	nodeSelectorStr := nodeSelector.String()
	key := buildNodeMatchCacheKey(nodeName, nodeNameToMatch, allNodesMatch, nodeSelectorStr)

	// 先查缓存：相同节点与选择器的匹配结果可以复用。
	if matches, ok := r.nodeMatchCache.Load(key); ok {
		return matches.(bool), nil
	}

	// 缓存未命中，调用原始函数实际计算。
	matches, err := structured.NodeMatches(r.draFeatures, node, nodeNameToMatch, allNodesMatch, nodeSelector)

	// 缓存结果（即便出错也缓存，避免反复失败）。
	if err == nil {
		r.nodeMatchCache.Store(key, matches)
	}

	return matches, err
}

// score 使用 `scorer` 函数计算得分。
func (r *resourceAllocationScorer) score(
	ctx context.Context,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
	podRequests []int64,
	draPreScoreState *draPreScoreState,
) (int64, *fwk.Status) {
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	// 未配置任何评分资源，无从打分。
	if len(r.resources) == 0 {
		return 0, fwk.NewStatus(fwk.Error, "resources not found")
	}

	requested, allocated, allocatable := r.calculateNodeAllocatableRequest(ctx, nodeInfo, podRequests, draPreScoreState)

	score := r.scorer(requested, allocated, allocatable)

	if loggerV := logger.V(10); loggerV.Enabled() { // 序列化这些 map 开销较大，仅在 V(10) 开启时执行。
		loggerV.Info("Listed internal info for allocatable resources, requested resources and score", "pod",
			klog.KObj(pod), "node", klog.KObj(node), "resourceAllocationScorer", r.Name,
			"allocatableResource", allocatable, "requestedResource", requested, "resourceScore", score,
		)
	}

	return score, nil
}

// calculateNodeAllocatableRequest 汇总节点上各资源的可分配量、已分配量与（含待调度 Pod 的）请求量。
func (r *resourceAllocationScorer) calculateNodeAllocatableRequest(
	ctx context.Context,
	nodeInfo fwk.NodeInfo,
	podRequests []int64,
	draPreScoreState *draPreScoreState,
) (requested []int64, allocated []int64, allocatable []int64) {
	requested = make([]int64, len(r.resources))
	allocated = make([]int64, len(r.resources))
	allocatable = make([]int64, len(r.resources))
	for i := range r.resources {
		resource := v1.ResourceName(r.resources[i].Name)
		// 对扩展资源，若 Pod 未请求，则保持该条目为零，
		// 以此跳过该资源的评分。
		if podRequests[i] == 0 && schedutil.IsScalarResourceName(resource) {
			continue
		}
		nodeAllocatable, nodeAllocated := r.calculateResourceAllocatableRequest(ctx, nodeInfo, resource, draPreScoreState)
		// 仅当可分配量非零时才填充扩展资源条目，避免零值干扰评分。
		if nodeAllocatable == 0 {
			continue
		}
		allocatable[i] = nodeAllocatable
		allocated[i] = nodeAllocated
		// 请求量 = 节点已分配量 + 待调度 Pod 的请求量，体现调度后的资源占用。
		requested[i] = allocated[i] + podRequests[i]
	}
	return requested, allocated, allocatable
}

// calculateResourceAllocatableRequest 返回两个值：
// - 第一个：节点上该资源的可分配量。
// - 第二个：节点上该资源已请求（聚合）的量。
func (r *resourceAllocationScorer) calculateResourceAllocatableRequest(
	ctx context.Context,
	nodeInfo fwk.NodeInfo,
	resource v1.ResourceName,
	draPreScoreState *draPreScoreState,
) (allocatable int64, allocated int64) {
	requested := nodeInfo.GetNonZeroRequested()
	if r.useRequested {
		requested = nodeInfo.GetRequested()
	}

	switch resource {
	case v1.ResourceCPU:
		return nodeInfo.GetAllocatable().GetMilliCPU(), requested.GetMilliCPU()
	case v1.ResourceMemory:
		return nodeInfo.GetAllocatable().GetMemory(), requested.GetMemory()
	case v1.ResourceEphemeralStorage:
		// 临时存储始终使用完整的 Requested（含默认值），与设计保持一致。
		return nodeInfo.GetAllocatable().GetEphemeralStorage(), nodeInfo.GetRequested().GetEphemeralStorage()
	default:
		allocatable, exists := nodeInfo.GetAllocatable().GetScalarResources()[resource]
		if allocatable == 0 && r.enableDRAExtendedResource && draPreScoreState != nil {
			// 可分配量为 0 说明该资源不由 device plugin 提供，
			// 尝试按 DRA 支持的扩展资源重新计算可分配量与请求量。
			allocatable, allocated := r.calculateDRAExtendedResourceAllocatableRequest(ctx, nodeInfo.Node(), resource, draPreScoreState)
			if allocatable > 0 {
				return allocatable, allocated
			}
		}
		if exists {
			return allocatable, nodeInfo.GetRequested().GetScalarResources()[resource]
		}
	}
	// 节点无该资源信息，跳过并在 V(10) 下记录原因。
	klog.FromContext(ctx).V(10).Info("Requested resource is omitted for node score calculation", "resourceName", resource)
	return 0, 0
}

// calculatePodResourceRequest 返回 Pod 的非零请求总量。
// 若 Pod 定义了 Overhead，会将其计入结果。
func (r *resourceAllocationScorer) calculatePodResourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {

	opts := resourcehelper.PodResourcesOptions{
		UseStatusResources: r.enableInPlacePodVerticalScaling,
		InPlacePodLevelResourcesVerticalScalingEnabled: r.enableInPlacePodLevelResourcesVerticalScaling,
		// 仅当 PodLevelResources 特性开启时才统计 Pod 级资源。
		SkipPodLevelResources: !r.enablePodLevelResources,
	}

	if !r.useRequested {
		// 不以默认请求参与评分，这里显式补上 CPU/内存默认值。
		opts.NonMissingContainerRequests = v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI),
		}
	}

	requests := resourcehelper.PodRequests(pod, opts)

	quantity := requests[resourceName]
	if resourceName == v1.ResourceCPU {
		return quantity.MilliValue()
	}
	return quantity.Value()
}

// calculatePodResourceRequestList 逐个计算 Pod 在给定资源列表上的请求量，保持与资源列表相同的顺序。
func (r *resourceAllocationScorer) calculatePodResourceRequestList(pod *v1.Pod, resources []config.ResourceSpec) []int64 {
	podRequests := make([]int64, len(resources))
	for i := range resources {
		podRequests[i] = r.calculatePodResourceRequest(pod, v1.ResourceName(resources[i].Name))
	}
	return podRequests
}

// isBestEffortPod 判断给定请求列表中是否全为零（即 BestEffort Pod）。
func (r *resourceAllocationScorer) isBestEffortPod(podRequests []int64) bool {
	for _, request := range podRequests {
		if request != 0 {
			return false
		}
	}
	return true
}

// getDRAPreScoredParams 返回用于 DRA 扩展资源评分的 DRA 已分配状态与资源切片。
func getDRAPreScoredParams(draManager fwk.SharedDRAManager, resources []config.ResourceSpec) (*draPreScoreState, *fwk.Status) {
	anyBackedByDRA := false
	for _, resource := range resources {
		resourceName := v1.ResourceName(resource.Name)
		if !schedutil.IsDRAExtendedResourceName(resourceName) {
			continue
		}
		deviceClass := draManager.DeviceClassResolver().GetDeviceClass(resourceName)
		if deviceClass != nil {
			anyBackedByDRA = true
			break
		}
	}
	// 没有任何 DRA 支持的资源时，返回 DRA 数据没有意义。
	if !anyBackedByDRA {
		return nil, nil
	}

	allocatedState, err := draManager.ResourceClaims().GatherAllocatedState()
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	resourceSlices, err := draManager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	return &draPreScoreState{
		allocatedState: allocatedState,
		resourceSlices: resourceSlices,
	}, nil
}

// calculateDRAExtendedResourceAllocatableRequest 计算由 DRA 支持的扩展资源的可分配量与已分配量。
func (r *resourceAllocationScorer) calculateDRAExtendedResourceAllocatableRequest(
	ctx context.Context,
	node *v1.Node,
	resource v1.ResourceName,
	draPreScoreState *draPreScoreState,
) (int64, int64) {
	logger := klog.FromContext(ctx)
	deviceClass := r.draManager.DeviceClassResolver().GetDeviceClass(resource)
	if deviceClass == nil {
		// 该资源并非 DRA 支持，无法按 DRA 路径计算。
		logger.V(7).Info("Extended resource not found in device class mapping", "resource", resource)
		return 0, 0
	}

	capacity, allocated, err := r.calculateDRAResourceTotals(ctx, node, deviceClass, draPreScoreState.allocatedState, draPreScoreState.resourceSlices)
	if err != nil {
		logger.Error(err, "Failed to calculate DRA resource capacity and allocated", "node", node.Name, "resource", resource, "deviceClass", deviceClass.Name)
		return 0, 0
	}

	logger.V(7).Info("DRA extended resource calculation", "node", node.Name, "resource", resource, "deviceClass", deviceClass.Name, "capacity", capacity, "allocated", allocated)
	return capacity, allocated
}

// calculateDRAResourceTotals 计算给定节点上匹配指定设备类的设备总容量与已分配总数。
// 它向 DRA manager 查询资源切片与已分配设备，按设备类与驱动过滤设备，返回计数。
// 若无法获取资源信息或节点匹配失败，则返回错误。
//
// 参数：
//
//	ctx         - 用于取消与设置截止时间的上下文
//	node        - 待评估设备资源的节点
//	deviceClass - 用于过滤设备的设备类
//
// 返回：
//
//	totalCapacity  - 节点上匹配该设备类的设备总数
//	totalAllocated - 匹配设备集合中当前已分配的设备数
//	error          - 处理过程中遇到的任意错误
func (r *resourceAllocationScorer) calculateDRAResourceTotals(ctx context.Context, node *v1.Node, deviceClass *resourceapi.DeviceClass, allocatedState *structured.AllocatedState, resourceSlices []*resourceapi.ResourceSlice,
) (int64, int64, error) {
	var totalCapacity, totalAllocated int64
	nodeName := node.Name

	for _, slice := range resourceSlices {
		// 提前过滤：先判断切片是否适用于本节点，避免无谓的逐设备求值。
		perDeviceNodeSelection := ptr.Deref(slice.Spec.PerDeviceNodeSelection, false)

		var devices []resourceapi.Device

		if perDeviceNodeSelection {
			// 逐设备节点选择：需要逐个设备判断是否属于本节点。
			devices = make([]resourceapi.Device, 0, len(slice.Spec.Devices))
			for _, device := range slice.Spec.Devices {
				deviceNodeName := ptr.Deref(device.NodeName, "")
				deviceAllNodes := ptr.Deref(device.AllNodes, false)

				// 快路径：先检查 AllNodes 或节点名精确匹配，避免构造选择器。
				if deviceAllNodes || (deviceNodeName != "" && deviceNodeName == nodeName) {
					devices = append(devices, device)
					continue
				}

				// 慢路径：仅当存在节点选择器时才进行匹配求值。
				if device.NodeSelector != nil {
					deviceMatches, err := r.nodeMatches(node, deviceNodeName, deviceAllNodes, device.NodeSelector)
					if err != nil {
						return 0, 0, err
					}
					if deviceMatches {
						devices = append(devices, device)
					}
				}
			}
		} else {
			// 切片级节点选择：整个切片共享同一节点约束。
			sliceNodeName := ptr.Deref(slice.Spec.NodeName, "")
			sliceAllNodes := ptr.Deref(slice.Spec.AllNodes, false)

			// 快路径：先检查 AllNodes 或节点名精确匹配，避免构造选择器。
			if !sliceAllNodes && sliceNodeName != nodeName && slice.Spec.NodeSelector != nil {
				// 节点名不直接匹配，需要求值节点选择器。
				matches, err := r.nodeMatches(node, sliceNodeName, sliceAllNodes, slice.Spec.NodeSelector)
				if err != nil {
					return 0, 0, err
				}
				if !matches {
					continue // 该切片不适用于本节点，跳过。
				}
			} else if !sliceAllNodes && sliceNodeName != "" && sliceNodeName != nodeName {
				// 指定了节点名但不匹配，跳过。
				continue
			}

			devices = slice.Spec.Devices
		}

		// 快路径：设备类没有选择器时无需逐设备做 CEL 求值。
		if len(deviceClass.Spec.Selectors) == 0 {
			driver := slice.Spec.Driver
			pool := slice.Spec.Pool.Name
			for _, device := range devices {
				totalCapacity++
				deviceID := structured.MakeDeviceID(driver, pool, device.Name)
				if structured.IsDeviceAllocated(deviceID, allocatedState) {
					totalAllocated++
				}
			}
		} else {
			// 慢路径：逐设备检查设备类选择器。
			driver := slice.Spec.Driver
			pool := slice.Spec.Pool.Name
			for _, device := range devices {
				matches, err := r.deviceMatchesClass(ctx, device, deviceClass, driver, pool)
				if err != nil {
					return 0, 0, err
				}
				if matches {
					totalCapacity++
					deviceID := structured.MakeDeviceID(driver, pool, device.Name)
					if structured.IsDeviceAllocated(deviceID, allocatedState) {
						totalAllocated++
					}
				}
			}
		}
	}

	return totalCapacity, totalAllocated, nil
}

// deviceMatchesClass 检查设备是否匹配设备类的选择器。
// 注意：调用方应确保设备类已设置 ExtendedResourceName 并只传入用于扩展资源评分的 DRA 资源，
// 本方法假定该前提成立。
func (r *resourceAllocationScorer) deviceMatchesClass(ctx context.Context, device resourceapi.Device, deviceClass *resourceapi.DeviceClass, driver string, poolName string) (bool, error) {
	// 未定义选择器时，所有设备都视为匹配。
	if len(deviceClass.Spec.Selectors) == 0 {
		return true, nil
	}

	// 仅在确有需要（遇到首个未缓存的 CEL 选择器）时才惰性构造 CEL 设备。
	var celDevice cel.Device
	celDeviceCreated := false

	// 所有选择器都匹配才算设备匹配。
	for _, selector := range deviceClass.Spec.Selectors {
		if selector.CEL == nil {
			continue
		}

		key := buildDeviceMatchCacheKey(selector.CEL.Expression, driver, poolName, device.Name)

		// 先查缓存，命中则复用上次的求值结果。
		if matches, ok := r.deviceMatchCache.Load(key); ok {
			if !matches.(bool) {
				return false, nil
			}
			continue // 该选择器已匹配，继续检查下一个。
		}

		// 缓存未命中，需要实际求值 CEL 表达式。
		// 若尚未构造 CEL 设备，此时再构造，避免无谓开销。
		if !celDeviceCreated {
			celDevice = cel.Device{
				Driver:     driver,
				Attributes: device.Attributes,
				Capacity:   device.Capacity,
			}
			celDeviceCreated = true
		}

		// 复用缓存的 CEL 编译结果，避免重复编译带来的性能损耗。
		result := r.celCache.GetOrCompile(selector.CEL.Expression)
		if result.Error != nil {
			return false, result.Error
		}

		matches, _, err := result.DeviceMatches(ctx, celDevice)
		if err != nil || !matches {
			return false, err
		}

		// 缓存结果，供后续相同表达式与设备复用。
		r.deviceMatchCache.Store(key, matches)
	}

	return true, nil
}

// scorePlacement 计算给定放置（placement）的资源分配得分。
func (r *resourceAllocationScorer) scorePlacement(
	ctx context.Context,
	podGroupInfo fwk.PodGroupInfo,
	podGroupAssignments *fwk.PodGroupAssignments,
) (int64, *fwk.Status) {
	requested := make([]int64, len(r.resources))
	// 汇总该放置中 Pod 组各 Pod 的请求量。
	for _, assignment := range podGroupAssignments.ProposedAssignments {
		pod := assignment.GetPod()
		podRequests := r.calculatePodResourceRequestList(pod, r.resources)
		for i := range len(r.resources) {
			requested[i] += podRequests[i]
		}
	}
	allocatable := make([]int64, len(r.resources))
	allocated := make([]int64, len(r.resources))
	// 汇总该放置中所有节点的资源情况。
	for _, node := range podGroupAssignments.Nodes {
		_, nodeAllocated, nodeAllocatable := r.calculateNodeAllocatableRequest(ctx, node, requested, nil)
		for i := range r.resources {
			allocatable[i] += nodeAllocatable[i]
			allocated[i] += nodeAllocated[i]
			// requested 同时包含节点上已有 Pod 的请求量
			// 以及本次为该放置调度的 Pod 组请求量。
			requested[i] += nodeAllocated[i]
		}
	}

	score := r.scorer(requested, allocated, allocatable)

	logger := klog.FromContext(ctx)
	if loggerV := logger.V(10); loggerV.Enabled() { // 序列化这些 map 开销较大，仅在 V(10) 开启时执行。
		loggerV.Info("Listed internal info for placement's allocatable resources, requested resources and placement score", "podGroup",
			klog.KRef(podGroupInfo.GetNamespace(), podGroupInfo.GetName()), "placement", podGroupAssignments.Name, "resourceAllocationScorer", r.Name,
			"allocatableResource", allocatable, "requestedResource", requested, "resourceScore", score,
		)
	}

	return score, nil
}
