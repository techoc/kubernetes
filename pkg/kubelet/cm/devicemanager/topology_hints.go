/*
Copyright 2019 The Kubernetes Authors.

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

package devicemanager

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// GetTopologyHints 实现 TopologyManager 的 HintProvider 接口，确保在创建每个容器的
// 拓扑感知提示时咨询 Device Manager。
func (m *ManagerImpl) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// 生成拓扑提示前先回收孤立的设备资源，避免陈旧分配影响可用设备的判断。
	m.UpdateAllocatedDevices(logger)

	// 遍历所有设备资源，分别为其生成拓扑提示。
	deviceHints := make(map[string][]topologymanager.TopologyHint)
	accumulatedResourceRequests := m.getContainerDeviceRequest(container)

	// 读取设备分配状态前加锁，与分配/回收流程互斥。
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for resource, requested := range accumulatedResourceRequests {
		// 只考虑真正包含拓扑信息的设备；没有拓扑信息的资源返回 nil 提示。
		if aligned := m.deviceHasTopologyAlignment(resource); !aligned {
			logger.Info("Resource does not have a topology preference", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name, "request", requested)
			deviceHints[resource] = nil
			continue
		}

		// 如果容器已经有设备分配，则直接基于现有分配重新生成相同的提示，
		// 避免重新选择设备；典型场景是 kubelet 重启之后。
		allocated := m.podDevices.containerDevices(string(pod.UID), container.Name, resource)
		if allocated.Len() > 0 {
			// 已分配数量与请求不一致时返回空提示，上层会按无法满足处理。
			if allocated.Len() != requested {
				logger.Info("Resource already allocated to pod with different number than request", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name, "request", requested, "allocated", allocated.Len())
				deviceHints[resource] = []topologymanager.TopologyHint{}
				continue
			}
			logger.Info("Regenerating TopologyHints for resource already allocated to pod", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name)
			deviceHints[resource] = m.generateDeviceTopologyHints(logger, resource, allocated, sets.Set[string]{}, requested)
			continue
		}

		// 获取可用于生成拓扑提示的设备列表（健康设备减去已分配设备，再并入可复用设备）。
		available := m.getAvailableDevices(resource)
		reusable := m.devicesToReuse[string(pod.UID)][resource]
		// 可用设备（含可复用设备）不足以满足请求时返回空提示。
		if available.Union(reusable).Len() < requested {
			logger.Info("Unable to generate topology hints: requested number of devices unavailable", "resourceName", resource, "pod", klog.KObj(pod), "containerName", container.Name, "request", requested, "available", available.Union(reusable).Len())
			deviceHints[resource] = []topologymanager.TopologyHint{}
			continue
		}

		// 根据当前请求数量与可用设备列表，为该资源生成拓扑提示。
		deviceHints[resource] = m.generateDeviceTopologyHints(logger, resource, available, reusable, requested)
	}

	return deviceHints
}

// GetPodTopologyHints 实现 topologymanager.HintProvider 接口，确保在创建 Pod 级
// 拓扑感知提示时咨询 Device Manager。
func (m *ManagerImpl) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// Garbage collect any stranded device resources before providing TopologyHints
	m.UpdateAllocatedDevices(logger)

	deviceHints := make(map[string][]topologymanager.TopologyHint)
	accumulatedResourceRequests := m.getPodDeviceRequest(pod)

	m.mutex.Lock()
	defer m.mutex.Unlock()
	for resource, requested := range accumulatedResourceRequests {
		// Only consider devices that actually contain topology information.
		if aligned := m.deviceHasTopologyAlignment(resource); !aligned {
			logger.Info("Resource does not have a topology preference", "resourceName", resource, "pod", klog.KObj(pod), "request", requested)
			deviceHints[resource] = nil
			continue
		}

		// 如果 Pod 已经有设备分配，则直接基于现有分配重新生成相同的提示，
		// 避免重新选择设备；典型场景是 kubelet 重启之后。
		allocated := m.podDevices.podDevices(string(pod.UID), resource)
		if allocated.Len() > 0 {
			// 已分配数量与请求不一致时返回空提示，上层会按无法满足处理。
			if allocated.Len() != requested {
				logger.Info("Resource already allocated to pod with different number than request", "resourceName", resource, "pod", klog.KObj(pod), "request", requested, "allocated", allocated.Len())
				deviceHints[resource] = []topologymanager.TopologyHint{}
				continue
			}
			logger.Info("Regenerating TopologyHints for resource already allocated to pod", "resourceName", resource, "pod", klog.KObj(pod), "allocated", allocated.Len())
			deviceHints[resource] = m.generateDeviceTopologyHints(logger, resource, allocated, sets.Set[string]{}, requested)
			continue
		}

		// Get the list of available devices, for which TopologyHints should be generated.
		available := m.getAvailableDevices(resource)
		// 可用设备不足以满足请求时返回空提示。
		if available.Len() < requested {
			logger.Info("Unable to generate topology hints: requested number of devices unavailable", "resourceName", resource, "pod", klog.KObj(pod), "request", requested, "available", available.Len())
			deviceHints[resource] = []topologymanager.TopologyHint{}
			continue
		}

		// Generate TopologyHints for this resource given the current
		// request size and the list of available devices.
		deviceHints[resource] = m.generateDeviceTopologyHints(logger, resource, available, sets.Set[string]{}, requested)
	}

	return deviceHints
}

// deviceHasTopologyAlignment 判断给定资源的设备是否携带 NUMA 拓扑信息；
// 只要有任一设备报告了 NUMA 节点，就认为需要拓扑对齐。
func (m *ManagerImpl) deviceHasTopologyAlignment(resource string) bool {
	// 只要任一设备带有非空 NUMA 节点信息，就认为该资源关心拓扑对齐。
	for _, device := range m.allDevices[resource] {
		if device.Topology != nil && len(device.Topology.Nodes) > 0 {
			return true
		}
	}
	return false
}

// getAvailableDevices 返回该资源当前可用的设备集合。
func (m *ManagerImpl) getAvailableDevices(resource string) sets.Set[string] {
	// 从健康设备列表中剔除正在使用的设备。
	return m.healthyDevices[resource].Difference(m.allocatedDevices[resource])
}

// generateDeviceTopologyHints 为指定资源枚举 NUMA 节点组合，生成满足请求的拓扑提示，
// 并把掩码最小的提示标记为首选。
func (m *ManagerImpl) generateDeviceTopologyHints(logger klog.Logger, resource string, available sets.Set[string], reusable sets.Set[string], request int) []topologymanager.TopologyHint {
	// 把位掩码枚举范围收窄到真正承载该资源设备的 NUMA 节点。在某些平台上，
	// 操作系统会暴露大量不承载设备的 NUMA 节点（例如带 36 个 NUMA 节点的
	// NVIDIA GB200，其中多数只承载 GPU HBM）；遍历全部机器 NUMA 节点会枚举
	// O(2^n) 个子集。限制在承载设备的节点上可把 n 降到真正相关的节点数。
	// 由于无设备的节点不会贡献 devicesInMask，排除它们不会改变 minAffinitySize
	// 或 Preferred 标记的计算结果。
	numaNodes := m.deviceNUMANodes(logger, resource)
	// 没有任何设备承载 NUMA 信息时，退化为使用机器全部 NUMA 节点。
	if len(numaNodes) == 0 {
		numaNodes = m.numaNodes
	}

	// 把 minAffinitySize 初始化为参与枚举的 NUMA 节点数；
	// 随着满足条件的掩码出现，它会逐步收窄。
	minAffinitySize := len(numaNodes)

	// 遍历 NUMA 节点的所有组合，并据此构建提示。
	hints := []topologymanager.TopologyHint{}
	bitmask.IterateBitMasks(numaNodes, func(mask bitmask.BitMask) {
		// 首先按当前请求数量更新 minAffinitySize。
		devicesInMask := 0
		for _, device := range m.allDevices[resource] {
			if mask.AnySet(m.getNUMANodeIds(device.Topology)) {
				devicesInMask++
			}
		}
		if devicesInMask >= request && mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// 然后检查所有可复用设备是否都包含在该掩码中。
		numMatching := 0
		for d := range reusable {
			// 设备没有拓扑信息时跳过。
			if m.allDevices[resource][d].Topology == nil {
				continue
			}
			// 若掩码不包含该设备的 NUMA 节点，则直接放弃这个掩码。
			if !mask.AnySet(m.getNUMANodeIds(m.allDevices[resource][d].Topology)) {
				return
			}
			numMatching++
		}

		// 最后检查当前 NUMA 节点组合上是否还有足够的可用设备满足请求。
		for d := range available {
			if mask.AnySet(m.getNUMANodeIds(m.allDevices[resource][d].Topology)) {
				numMatching++
			}
		}

		// 设备数量不足时继续尝试下一个组合。
		if numMatching < request {
			return
		}

		// 否则基于该 NUMA 掩码创建新提示并加入列表；第一轮遍历时
		// 所有提示的 Preferred 都先置为 false。
		hints = append(hints, topologymanager.TopologyHint{
			NUMANodeAffinity: mask,
			Preferred:        false,
		})
	})

	// 再遍历一遍所有提示，根据亲和掩码中置一的位数与 minAffinitySize 的比较结果
	// 更新 Preferred 字段；只有位数等于最小值的提示才被视为首选。
	for i := range hints {
		if hints[i].NUMANodeAffinity.Count() == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

// deviceNUMANodes 返回该资源至少承载一个设备的 NUMA 节点 ID 有序列表。
// 返回值保证是 m.numaNodes 的子集：设备插件上报但 cadvisor 未知的 NUMA ID
// 会被记录日志并丢弃。调用方必须持有 m.mutex。
func (m *ManagerImpl) deviceNUMANodes(logger klog.Logger, resource string) []int {
	nodesWithDevices := sets.New[int]()
	for _, device := range m.allDevices[resource] {
		nodesWithDevices.Insert(m.getNUMANodeIds(device.Topology)...)
	}

	knownNodes := sets.New[int](m.numaNodes...)
	// 与 cadvisor 已知节点求差集，识别设备插件上报的未知 NUMA ID。
	unknown := nodesWithDevices.Difference(knownNodes)
	if unknown.Len() > 0 {
		logger.Info("Ignoring NUMA node IDs reported by device plugin that are unknown to cadvisor",
			"resource", resource, "unknownNodes", sets.List(unknown), "knownNodes", m.numaNodes)
		nodesWithDevices = nodesWithDevices.Intersection(knownNodes)
	}

	return sets.List(nodesWithDevices)
}

// getNUMANodeIds 提取设备拓扑信息中的 NUMA 节点 ID 列表；拓扑为空时返回 nil。
func (m *ManagerImpl) getNUMANodeIds(topology *pluginapi.TopologyInfo) []int {
	if topology == nil {
		return nil
	}
	var ids []int
	for _, n := range topology.Nodes {
		ids = append(ids, int(n.ID))
	}
	return ids
}

// getPodDeviceRequest 汇总 Pod 级别各设备插件资源的请求数量。
func (m *ManagerImpl) getPodDeviceRequest(pod *v1.Pod) map[string]int {
	// 对设备插件资源而言 requests 等于 limits，因此直接从 limits 取值。
	limits := resource.PodLimits(pod, resource.PodResourcesOptions{
		ExcludeOverhead: true,
	})
	podRequests := make(map[string]int)
	for resourceName, quantity := range limits {
		// 忽略非设备插件管理的资源。
		if !m.isDevicePluginResource(string(resourceName)) {
			continue
		}
		podRequests[string(resourceName)] = int(quantity.Value())
	}
	return podRequests
}

// getContainerDeviceRequest 汇总容器级别各设备插件资源的请求数量。
func (m *ManagerImpl) getContainerDeviceRequest(container *v1.Container) map[string]int {
	containerRequests := make(map[string]int)
	for resourceObj, requestedObj := range container.Resources.Limits {
		resource := string(resourceObj)
		requested := int(requestedObj.Value())
		// 忽略非设备插件管理的资源。
		if !m.isDevicePluginResource(resource) {
			continue
		}
		containerRequests[resource] = requested
	}
	return containerRequests
}
