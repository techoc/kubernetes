/*
Copyright 2025 The Kubernetes Authors.

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
	"sort"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// ExtractPodNodeAllocatableResourceClaimStatus 从 CycleState 中取出 Pod 在指定节点上的 nodeAllocatable 声明状态。
func ExtractPodNodeAllocatableResourceClaimStatus(logger klog.Logger, state fwk.CycleState, nodeName string) []v1.NodeAllocatableResourceClaimStatus {
	s, err := state.Read(names.DynamicResources)
	if err != nil {
		// 插件未运行或没有对应状态，直接返回空。
		return nil
	}

	draState, ok := s.(*stateData)
	if !ok {
		logger.Error(errors.New("invalid DynamicResources state type"), "Failed to cast CycleState data")
		return nil
	}

	if nodeAlloc, exists := draState.nodeAllocations[nodeName]; exists {
		return nodeAlloc.nodeAllocatableResourceClaimStatuses
	}

	return nil
}

// calculateAndCheckNodeAllocatableResources 计算 Pod 对节点可分配资源（如 CPU、内存）的总需求，
// 既包括容器自身的请求，也包括 DRA 声明带来的额外资源，然后检查总需求是否不超过节点剩余容量。
func (pl *DynamicResources) calculateAndCheckNodeAllocatableResources(ctx context.Context, state *stateData, pod *v1.Pod, nodeInfo fwk.NodeInfo, allocations map[types.UID]*resourceapi.AllocationResult) ([]v1.NodeAllocatableResourceClaimStatus, *fwk.Status) {
	logger := klog.FromContext(ctx)

	nodeAllocatableClaims := []*resourceapi.ResourceClaim{}

	var err error

	nodeSlices, err := filterSlicesForNode(pl.draManager, nodeInfo.Node())
	if err != nil {
		logger.Error(err, "Failed to list resource slices")
		return nil, statusError(logger, err)
	}

	for _, claim := range state.claims.all() {
		alloc := claim.Status.Allocation
		if a, ok := allocations[claim.UID]; ok {
			alloc = a
		}
		if alloc != nil && len(alloc.Devices.Results) > 0 {
			for _, result := range alloc.Devices.Results {
				device, err := getDeviceFromSlices(nodeSlices, &result, nodeInfo.Node())
				if err != nil {
					logger.Error(err, "Failed to get device from manager", "claim", klog.KObj(claim), "device", result.Device, "pool", result.Pool, "driver", result.Driver)
					continue
				}
				if device != nil && len(device.NodeAllocatableResources) > 0 {
					nodeAllocatableClaims = append(nodeAllocatableClaims, claim)
					break
				}
			}
		}
	}

	if len(nodeAllocatableClaims) == 0 {
		return nil, nil // 没有节点可分配资源需要检查
	}

	totalPodDemand, nodeAllocatableClaimStatus, status := pl.getPodNodeAllocatableResourceFootprint(logger, pod, allocations, nodeAllocatableClaims, nodeSlices, nodeInfo.Node())
	if status != nil {
		logger.V(5).Info("calculateAndCheckNodeAllocatableResources: getPodNodeAllocatableResourceFootprint failed", "status", status)
		return nil, status
	}

	if status := pl.nodeFitsResources(nodeInfo, totalPodDemand); status != nil {
		return nil, status
	}
	logger.V(5).Info("Pod fits on node ( including DRA Node Allocatable Resources)", "pod", klog.KObj(pod), "node", nodeInfo.Node().Name)
	return nodeAllocatableClaimStatus, nil
}

// getDeviceFromManager 从 DRA manager 的全部资源切片中查找分配结果对应的设备。
func getDeviceFromManager(draManager fwk.SharedDRAManager, result *resourceapi.DeviceRequestAllocationResult) (*resourceapi.Device, error) {
	slices, err := draManager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		return nil, fmt.Errorf("listing resource slices: %w", err)
	}
	return getDeviceFromSlices(slices, result, nil)
}

// filterSlicesForNode 过滤出与给定节点相关的资源切片：
// 节点专属、所有节点可用、节点选择器匹配，或按设备单独选择节点的切片。
func filterSlicesForNode(draManager fwk.SharedDRAManager, node *v1.Node) ([]*resourceapi.ResourceSlice, error) {
	slices, err := draManager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		return nil, fmt.Errorf("listing resource slices: %w", err)
	}

	if node == nil {
		return slices, nil
	}
	var nodeSlices []*resourceapi.ResourceSlice
	for _, slice := range slices {
		if slice.Spec.NodeName != nil && *slice.Spec.NodeName == node.Name {
			nodeSlices = append(nodeSlices, slice)
		} else if slice.Spec.AllNodes != nil && *slice.Spec.AllNodes {
			nodeSlices = append(nodeSlices, slice)
		} else if slice.Spec.NodeSelector != nil {
			selector, err := nodeaffinity.NewNodeSelector(slice.Spec.NodeSelector)
			if err == nil && selector.Match(node) {
				nodeSlices = append(nodeSlices, slice)
			}
		} else if slice.Spec.PerDeviceNodeSelection != nil && *slice.Spec.PerDeviceNodeSelection {
			nodeSlices = append(nodeSlices, slice)
		}
	}
	return nodeSlices, nil
}

// deviceMatchesNode 判断单个设备是否可被调度到指定节点上。
func deviceMatchesNode(device *resourceapi.Device, node *v1.Node) bool {
	if node == nil {
		return true
	}
	if device.NodeName != nil && *device.NodeName == node.Name {
		return true
	}
	if device.AllNodes != nil && *device.AllNodes {
		return true
	}
	if device.NodeSelector != nil {
		selector, err := nodeaffinity.NewNodeSelector(device.NodeSelector)
		if err == nil && selector.Match(node) {
			return true
		}
	}
	return false
}

// getDeviceFromSlices 在资源切片中按驱动、池和设备名定位分配结果对应的设备。
func getDeviceFromSlices(slices []*resourceapi.ResourceSlice, result *resourceapi.DeviceRequestAllocationResult, node *v1.Node) (*resourceapi.Device, error) {
	for _, slice := range slices {
		if slice.Spec.Driver == result.Driver && slice.Spec.Pool.Name == result.Pool {
			for i := range slice.Spec.Devices {
				if slice.Spec.Devices[i].Name == result.Device {
					device := &slice.Spec.Devices[i]
					if slice.Spec.PerDeviceNodeSelection != nil && *slice.Spec.PerDeviceNodeSelection {
						if !deviceMatchesNode(device, node) {
							return nil, fmt.Errorf("device %s does not match node", result.Device)
						}
					}
					return device, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("device %s not found in pool %s for driver %s", result.Device, result.Pool, result.Driver)
}

// addDeviceMapping 计算设备直接映射的资源量并累加到总量映射中。
func addDeviceMapping(
	resourceName v1.ResourceName,
	mappingMap *resourceapi.NodeAllocatableMapping,
	result *resourceapi.DeviceRequestAllocationResult,
	key v1.ObjectReference,
	totalResources map[v1.ResourceName]resource.Quantity,
) error {
	if mappingMap.CapacityKey != nil && *mappingMap.CapacityKey != "" {
		capacityKey := *mappingMap.CapacityKey
		if result.ConsumedCapacity == nil {
			return fmt.Errorf("claim %s/%s, device %s: ConsumedCapacity is nil, but Capacity key '%s' is set in NodeAllocatableResources for resource %s", key.Namespace, key.Name, result.Device, capacityKey, resourceName)
		}
		if consumed, exists := result.ConsumedCapacity[capacityKey]; exists {
			// 若不存在该 capacityKey，说明此映射与当前分配无关，无需计入。
			consumedQuantity := consumed.DeepCopy()
			quantityOne := resource.MustParse("1")
			if mappingMap.CapacityMultiplier != nil && !mappingMap.CapacityMultiplier.Equal(quantityOne) {
				multiplier := mappingMap.CapacityMultiplier.DeepCopy()
				qDec := consumedQuantity.AsDec()
				qDec.Mul(qDec, multiplier.AsDec())
				consumedQuantity = *resource.NewDecimalQuantity(*qDec, consumedQuantity.Format)
			}
			current := totalResources[resourceName]
			current.Add(consumedQuantity)
			totalResources[resourceName] = current
		}
		return nil
	}

	// 同一设备的映射中 DeviceMultiplier 和 CapacityKey 不会同时设置（API 校验保证），
	// 因此这两条路径可以安全地视为互斥。
	if mappingMap.DeviceMultiplier != nil {
		current := totalResources[resourceName]
		current.Add(mappingMap.DeviceMultiplier.DeepCopy())
		totalResources[resourceName] = current
	}
	return nil
}

// addDeviceOverhead 计算设备带来的额外开销资源并累加到总量映射中。
func addDeviceOverhead(
	resourceName v1.ResourceName,
	newOverhead *resourceapi.NodeAllocatableOverhead,
	totalResources map[v1.ResourceName]v1.NodeAllocatableOverheadResources,
) {
	if newOverhead.PerPod == nil && newOverhead.PerContainer == nil {
		return
	}

	current, exists := totalResources[resourceName]
	if !exists {
		current = v1.NodeAllocatableOverheadResources{Name: resourceName}
	}

	if newOverhead.PerPod != nil {
		if current.PerPod == nil {
			q := newOverhead.PerPod.DeepCopy()
			current.PerPod = &q
		} else {
			current.PerPod.Add(*newOverhead.PerPod)
		}
	}
	if newOverhead.PerContainer != nil {
		if current.PerContainer == nil {
			q := newOverhead.PerContainer.DeepCopy()
			current.PerContainer = &q
		} else {
			current.PerContainer.Add(*newOverhead.PerContainer)
		}
	}

	totalResources[resourceName] = current
}

// buildNodeAllocatableDRAInfo 处理 Pod 的节点可分配资源分配情况，
// 把 DRA 声明中分配到的设备和数量转换为 v1.NodeAllocatableResourceClaimStatus 列表。
func (pl *DynamicResources) buildNodeAllocatableDRAInfo(pod *v1.Pod, nodeAllocatableClaimAllocations map[v1.ObjectReference]*resourceapi.AllocationResult, claimNametoUID map[string]types.UID, slices []*resourceapi.ResourceSlice, node *v1.Node) ([]v1.NodeAllocatableResourceClaimStatus, error) {
	if len(nodeAllocatableClaimAllocations) == 0 {
		return []v1.NodeAllocatableResourceClaimStatus{}, nil
	}

	claimToStatus := make(map[types.UID]v1.NodeAllocatableResourceClaimStatus)

	for key, alloc := range nodeAllocatableClaimAllocations {
		totalDirectMappedResourcesPerClaim := make(map[v1.ResourceName]resource.Quantity)
		totalOverheadResourcesPerClaim := make(map[v1.ResourceName]v1.NodeAllocatableOverheadResources)

		for _, result := range alloc.Devices.Results {
			device, err := getDeviceFromSlices(slices, &result, node)
			if err != nil {
				return nil, fmt.Errorf("claim %s/%s, device %s, driver %s: %w", key.Namespace, key.Name, result.Device, result.Driver, err)
			}
			if device == nil || device.NodeAllocatableResources == nil {
				continue
			}

			for resourceName, resourceMap := range device.NodeAllocatableResources {
				if resourceMap.Mapping != nil {
					if err := addDeviceMapping(resourceName, resourceMap.Mapping, &result, key, totalDirectMappedResourcesPerClaim); err != nil {
						return nil, err
					}
				}
				if resourceMap.Overhead != nil {
					addDeviceOverhead(resourceName, resourceMap.Overhead, totalOverheadResourcesPerClaim)
				}
			}
		}

		if len(totalDirectMappedResourcesPerClaim) > 0 || len(totalOverheadResourcesPerClaim) > 0 {
			status := v1.NodeAllocatableResourceClaimStatus{
				ResourceClaimName: key.Name,
				Containers:        []string{},
				Mapping:           []v1.NodeAllocatableMappedResources{},
				Overhead:          []v1.NodeAllocatableOverheadResources{},
			}

			for name, quantity := range totalDirectMappedResourcesPerClaim {
				q := quantity.DeepCopy()
				status.Mapping = append(status.Mapping, v1.NodeAllocatableMappedResources{
					Name:     name,
					Quantity: &q,
				})
			}
			for _, overhead := range totalOverheadResourcesPerClaim {
				status.Overhead = append(status.Overhead, overhead)
			}

			sort.Slice(status.Mapping, func(i, j int) bool {
				return status.Mapping[i].Name < status.Mapping[j].Name
			})
			sort.Slice(status.Overhead, func(i, j int) bool {
				return status.Overhead[i].Name < status.Overhead[j].Name
			})
			claimToStatus[key.UID] = status
		}
	}

	for _, containers := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, container := range containers {
			for _, podClaim := range container.Resources.Claims {
				if claimUID, ok := claimNametoUID[podClaim.Name]; ok {
					if nodeAllocatableClaimStatus, ok := claimToStatus[claimUID]; ok {
						nodeAllocatableClaimStatus.Containers = append(nodeAllocatableClaimStatus.Containers, container.Name)
						claimToStatus[claimUID] = nodeAllocatableClaimStatus
					}
				}
			}
		}
	}

	nodeAllocatableClaimInfoList := make([]v1.NodeAllocatableResourceClaimStatus, 0, len(claimToStatus))
	for _, status := range claimToStatus {
		nodeAllocatableClaimInfoList = append(nodeAllocatableClaimInfoList, status)
	}

	// 排序以保证输出稳定可复现。
	sort.Slice(nodeAllocatableClaimInfoList, func(i, j int) bool {
		return nodeAllocatableClaimInfoList[i].ResourceClaimName < nodeAllocatableClaimInfoList[j].ResourceClaimName
	})

	return nodeAllocatableClaimInfoList, nil
}

// validateNodeAllocatableDRAClaimSharing 确保节点可分配的 DRA 声明不会被同一节点上的其他 Pod 复用。
func (pl *DynamicResources) validateNodeAllocatableDRAClaimSharing(pod *v1.Pod, claim *resourceapi.ResourceClaim, state *stateData, podGroupState *podGroupStateData) *fwk.Status {
	if claim == nil {
		return nil
	}

	if !state.claimHasNodeAllocatableMappedDevice[claim.UID] {
		// 仅含开销的声明不涉及设备映射，允许共享。
		return nil
	}

	// 声明若已被其他 Pod 或 PodGroup 预留，直接判定不可调度。
	if len(claim.Status.ReservedFor) > 0 && !resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node allocatable resource claim %s has a mapped device and cannot be shared across pods", claim.Name))
	}

	// 同一 PodGroup 中若已有其他 Pod 在本轮调度中预留/分配了该声明，同样禁止共享。
	if podGroupState != nil {
		if pendingAllocPodUIDs, ok := podGroupState.pendingAllocations[claim.UID]; ok {
			for uid := range pendingAllocPodUIDs {
				if uid != pod.UID {
					return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node allocatable resource claim %s has a mapped device and cannot be shared across pods", claim.Name))
				}
			}
		}
	}

	return nil
}

// validatePodLevelResourcesCoverDRA 检查两项约束：
// 1. Pod 级 requests 不小于所有容器 requests 加上 DRA 资源后的总和；
// 2. Pod 级 limits 不小于每个容器各自的 limits 加上 DRA 资源后的值。
// 校验模式与 pkg/apis/core/validation/validation.go 中的 validatePodResourceConsistency() 一致。
func (pl *DynamicResources) validatePodLevelResourcesCoverDRA(pod *v1.Pod) *fwk.Status {
	if !pl.fts.EnablePodLevelResources || pod.Spec.Resources == nil {
		return nil
	}
	if len(pod.Status.NodeAllocatableResourceClaimStatuses) == 0 {
		return nil
	}

	if pod.Spec.Resources.Requests != nil {
		// 计算容器请求 + DRA + 开销的总和，注意排除 Pod 级资源本身以免重复计算。
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    true,
			UseDRANodeAllocatableResourceClaimStatus: true,
		}
		requestWithoutPodLevel := resourcehelper.AggregateContainerRequests(pod, opts)

		// 对设置了 Pod 级请求的资源，检查容器与 DRA 的合计是否超出 Pod 级预算。
		for resName, podLevelReq := range pod.Spec.Resources.Requests {
			if !resourcehelper.IsSupportedPodLevelResource(resName) {
				continue
			}
			val, ok := requestWithoutPodLevel[resName]
			if !ok {
				continue
			}
			if val.Cmp(podLevelReq) > 0 {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("pod level request for %s is insufficient to cover the aggregated container and node-allocatable DRA requests", resName))
			}
		}
	}

	if pod.Spec.Resources.Limits != nil {
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    true,
			UseDRANodeAllocatableResourceClaimStatus: true,
		}
		limitsWithoutPodLevel := resourcehelper.AggregateContainerLimits(pod, opts)

		// Pod 级大页资源限制必须不小于容器级大页限制与 DRA 限制之和。
		for resourceName, ctrLims := range limitsWithoutPodLevel {
			if !v1helper.IsHugePageResourceName(resourceName) {
				continue
			}

			podLevelResLimit, hasLimit := pod.Spec.Resources.Limits[resourceName]
			if !hasLimit {
				continue
			}

			if ctrLims.Cmp(podLevelResLimit) > 0 {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("pod level limit for %s is insufficient to cover the aggregated container and node-allocatable DRA limits", resourceName))
			}
		}

		// 每个容器的限制加上 DRA 开销后，都不得超过 Pod 级限制。
		containerDRAAllocations := make(map[string]v1.ResourceList, len(pod.Spec.Containers))
		for _, ctr := range pod.Spec.Containers {
			containerDRAAllocations[ctr.Name] = resourcehelper.GetContainerDRAAllocations(pod, ctr.Name)
		}

		for _, ctr := range pod.Spec.Containers {
			for resourceName, ctrLimit := range ctr.Resources.Limits {
				if v1helper.IsHugePageResourceName(resourceName) {
					continue
				}

				// 未设置该资源的 Pod 级限制时无需检查。
				podLevelResLimit, exists := pod.Spec.Resources.Limits[resourceName]
				if !exists {
					continue
				}

				draResAllocation := containerDRAAllocations[ctr.Name][resourceName]
				effectiveLimit := ctrLimit.DeepCopy()
				effectiveLimit.Add(draResAllocation)

				if effectiveLimit.Cmp(podLevelResLimit) > 0 {
					return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("pod level limit for %s is insufficient to cover the limit and DRA overhead for container %s", resourceName, ctr.Name))
				}
			}
		}
	}

	return nil
}

// getPodNodeAllocatableResourceFootprint 计算 Pod 对节点可分配资源的总需求。
func (pl *DynamicResources) getPodNodeAllocatableResourceFootprint(logger klog.Logger, pod *v1.Pod, allocations map[types.UID]*resourceapi.AllocationResult, nodeAllocatableClaims []*resourceapi.ResourceClaim, slices []*resourceapi.ResourceSlice, node *v1.Node) (*framework.Resource, []v1.NodeAllocatableResourceClaimStatus, *fwk.Status) {
	nodeAllocatableDRAAllocations := make(map[v1.ObjectReference]*resourceapi.AllocationResult)
	// 先纳入已经完成分配的声明。
	for _, claim := range nodeAllocatableClaims {
		key := v1.ObjectReference{
			Namespace: claim.Namespace,
			Name:      claim.Name,
			UID:       claim.UID,
		}
		if claim.Status.Allocation != nil {
			nodeAllocatableDRAAllocations[key] = claim.Status.Allocation
		}
		if alloc, ok := allocations[claim.UID]; ok {
			nodeAllocatableDRAAllocations[key] = alloc
		}
	}

	claimNametoUID := make(map[string]types.UID)
	if err := pl.foreachPodResourceClaim(pod, func(name string, claim *resourceapi.ResourceClaim) {
		claimNametoUID[name] = claim.UID
	}); err != nil {
		return nil, nil, statusError(logger, fmt.Errorf("processing pod resource claims: %w", err))
	}

	nodeAllocatableStatus, err := pl.buildNodeAllocatableDRAInfo(pod, nodeAllocatableDRAAllocations, claimNametoUID, slices, node)
	if err != nil {
		return nil, nil, statusError(logger, err)
	}

	// 计算最终用于节点适配性判断的总需求。
	optsTotal := resourcehelper.PodResourcesOptions{
		SkipPodLevelResources:                    !pl.fts.EnablePodLevelResources,
		UseDRANodeAllocatableResourceClaimStatus: true,
	}
	// 浅拷贝即可，这里只读取容器和 Pod spec 中的资源信息来计算资源占用。
	podCopy := *pod
	podCopy.Status.NodeAllocatableResourceClaimStatuses = nodeAllocatableStatus
	totalPodDemandRes := resourcehelper.PodRequests(&podCopy, optsTotal)

	// API 层的校验只检查 Spec 内的容器请求，无法覆盖设备分配后才确定的 DRA 资源，
	// 因此这里额外做一次校验。
	if status := pl.validatePodLevelResourcesCoverDRA(&podCopy); status != nil {
		return nil, nil, status
	}

	totalPodDemand := framework.NewResource(totalPodDemandRes)
	logger.V(5).Info("Total Pod Demand After DRA", "pod", klog.KObj(pod), "demand", totalPodDemand)

	return totalPodDemand, nodeAllocatableStatus, nil
}

// insufficientResource 描述导致 Pod 无法适配节点的资源类型及具体情况。
type insufficientResource struct {
	resourceName v1.ResourceName
	// 显式保存原因字符串，避免对常见资源动态格式化消息，
	// 这在集群自动扩缩容模拟中开销较大。
	reason    string
	requested int64
	used      int64
	capacity  int64
	// unresolvable 表示通过抢占也无法让该节点适配该 Pod，
	// 依据是节点总容量与 Pod 请求量的比较。
	unresolvable bool
}

// nodeFitsResources 检查节点是否有足够容量满足 Pod 的节点可分配资源需求。
// TODO(pravk03): Unify this with the fitsRequest() in pkg/scheduler/framework/plugins/noderesources/fit.go.
func (pl *DynamicResources) nodeFitsResources(nodeInfo fwk.NodeInfo, podRequest *framework.Resource) *fwk.Status {
	insufficientResources := make([]insufficientResource, 0, 4)

	if podRequest.MilliCPU > 0 && podRequest.MilliCPU > (nodeInfo.GetAllocatable().GetMilliCPU()-nodeInfo.GetRequested().GetMilliCPU()) {
		insufficientResources = append(insufficientResources, insufficientResource{
			resourceName: v1.ResourceCPU,
			reason:       "Insufficient cpu",
			requested:    podRequest.MilliCPU,
			used:         nodeInfo.GetRequested().GetMilliCPU(),
			capacity:     nodeInfo.GetAllocatable().GetMilliCPU(),
			unresolvable: podRequest.MilliCPU > nodeInfo.GetAllocatable().GetMilliCPU(),
		})
	}
	if podRequest.Memory > 0 && podRequest.Memory > (nodeInfo.GetAllocatable().GetMemory()-nodeInfo.GetRequested().GetMemory()) {
		insufficientResources = append(insufficientResources, insufficientResource{
			resourceName: v1.ResourceMemory,
			reason:       "Insufficient memory",
			requested:    podRequest.Memory,
			used:         nodeInfo.GetRequested().GetMemory(),
			capacity:     nodeInfo.GetAllocatable().GetMemory(),
			unresolvable: podRequest.Memory > nodeInfo.GetAllocatable().GetMemory(),
		})
	}
	if podRequest.EphemeralStorage > 0 &&
		podRequest.EphemeralStorage > (nodeInfo.GetAllocatable().GetEphemeralStorage()-nodeInfo.GetRequested().GetEphemeralStorage()) {
		insufficientResources = append(insufficientResources, insufficientResource{
			resourceName: v1.ResourceEphemeralStorage,
			reason:       "Insufficient ephemeral-storage",
			requested:    podRequest.EphemeralStorage,
			used:         nodeInfo.GetRequested().GetEphemeralStorage(),
			capacity:     nodeInfo.GetAllocatable().GetEphemeralStorage(),
			unresolvable: podRequest.GetEphemeralStorage() > nodeInfo.GetAllocatable().GetEphemeralStorage(),
		})
	}

	for resName, reqQuant := range podRequest.ScalarResources {
		if v1helper.IsHugePageResourceName(resName) {
			nodeCapacity := nodeInfo.GetAllocatable().GetScalarResources()[resName]
			nodeRequested := nodeInfo.GetRequested().GetScalarResources()[resName]
			available := nodeCapacity - nodeRequested

			if reqQuant > available {
				insufficientResources = append(insufficientResources, insufficientResource{
					resourceName: resName,
					reason:       fmt.Sprintf("Insufficient %s", resName),
					requested:    reqQuant,
					used:         nodeRequested,
					capacity:     nodeCapacity,
					unresolvable: reqQuant > nodeCapacity,
				})
			}
		}
	}

	if len(insufficientResources) > 0 {
		failureReasons := make([]string, 0, len(insufficientResources))
		statusCode := fwk.Unschedulable
		for i := range insufficientResources {
			failureReasons = append(failureReasons, insufficientResources[i].reason)
			if insufficientResources[i].unresolvable {
				statusCode = fwk.UnschedulableAndUnresolvable
			}
		}
		return fwk.NewStatus(statusCode, failureReasons...)
	}
	return nil
}

// patchNodeAllocatableResourceClaimStatus 把计算出的节点可分配资源声明状态写回 Pod status。
func (pl *DynamicResources) patchNodeAllocatableResourceClaimStatus(ctx context.Context, pod *v1.Pod, nodeAllocatableClaimStatus []v1.NodeAllocatableResourceClaimStatus) *fwk.Status {

	if len(nodeAllocatableClaimStatus) == 0 {
		return nil
	}
	logger := klog.FromContext(ctx)

	// 传入的 Pod 来自调度器缓存，在 assume 阶段已填充该状态但尚未持久化。
	// PatchPodStatus 在前后状态相同时会跳过打补丁，所以这里先清空 baseStatus 中的该字段，
	// 强制生成一次补丁把状态写入 apiserver。
	baseStatus := pod.Status.DeepCopy()
	if !apiequality.Semantic.DeepEqual(baseStatus.NodeAllocatableResourceClaimStatuses, nodeAllocatableClaimStatus) {
		logger.V(5).Info("NodeAllocatableResourceClaimStatuses difference: assumed pod status does not match calculated status", "pod", klog.KObj(pod))
		return statusError(logger, errors.New("assumed pod status does not match calculated status to be patched"))
	}
	baseStatus.NodeAllocatableResourceClaimStatuses = nil

	targetStatus := pod.Status.DeepCopy()

	targetStatus.NodeAllocatableResourceClaimStatuses = nodeAllocatableClaimStatus
	if err := schedutil.PatchPodStatus(ctx, pl.clientset, pod.Name, pod.Namespace, baseStatus, targetStatus); err != nil {
		return statusError(logger, fmt.Errorf("updating pod %s/%s NodeAllocatableResourceClaimStatuses: %w", pod.Namespace, pod.Name, err))
	}
	logger.V(5).Info("Patched pod status with NodeAllocatableResourceClaimStatuses", "pod", klog.KObj(pod), "status", targetStatus.NodeAllocatableResourceClaimStatuses)

	return nil
}

// clearNodeAllocatableResourceClaimStatus 在 Unreserve 时清除 Pod status 中的节点可分配资源声明状态。
func (pl *DynamicResources) clearNodeAllocatableResourceClaimStatus(ctx context.Context, pod *v1.Pod) {
	if len(pod.Status.NodeAllocatableResourceClaimStatuses) == 0 {
		return
	}

	logger := klog.FromContext(ctx)
	logger.V(5).Info("Clearing NodeAllocatableResourceClaimStatuses on Unreserve", "pod", klog.KObj(pod))

	// 显式空列表可区分“主动清空”和旧客户端因不认识该字段而省略它。
	// PatchPodStatus 因该字段带 omitempty 标签，无法保留这种区别，故直接构造原始 patch。
	//
	// patch 中带上 uid 作为前置条件，防止原 Pod 被删除并以同名重建后，
	// 补丁误应用到另一个 Pod 对象上。
	patch := fmt.Appendf(nil, `{"metadata":{"uid":%q},"status":{"nodeAllocatableResourceClaimStatuses":[]}}`, pod.UID)
	if _, err := pl.clientset.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{}, "status"); err != nil {
		logger.Error(err, "Failed to clear NodeAllocatableResourceClaimStatuses on Unreserve", "pod", klog.KObj(pod))
	} else {
		logger.V(5).Info("Cleared NodeAllocatableResourceClaimStatuses", "pod", klog.KObj(pod))
	}
}
