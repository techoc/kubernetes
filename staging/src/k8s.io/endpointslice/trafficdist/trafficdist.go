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

// trafficdist 包负责根据 Service 的 trafficDistribution 字段协调 EndpointSlice 中 Endpoint 的拓扑提示（hints）。
package trafficdist

import (
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// closeTrafficDistribution 收集所有启用“就近偏好”启发式的 trafficDistribution 取值。
var closeTrafficDistribution = sets.New(
	corev1.ServiceTrafficDistributionPreferClose,
	corev1.ServiceTrafficDistributionPreferSameZone,
	corev1.ServiceTrafficDistributionPreferSameNode,
)

// ReconcileHints 为给定的 EndpointSlice 协调（重写）其中 Endpoint 的 hints。
// slicesUnchanged 中的对象不会被原地修改：需要更新时会先深拷贝再放入 slicesToUpdate。
func ReconcileHints(trafficDistribution *string, slicesToCreate, slicesToUpdate, slicesUnchanged []*discoveryv1.EndpointSlice) ([]*discoveryv1.EndpointSlice, []*discoveryv1.EndpointSlice, []*discoveryv1.EndpointSlice) {
	// 默认使用集群范围路由策略，对应的启发式会清除所有 hints。
	var h heuristic = &defaultHeuristic{}

	// 仅三种“就近”策略启用 preferCloseHeuristic；其中只有 PreferSameNode 需要生成节点级 hints。
	if trafficDistribution != nil && closeTrafficDistribution.Has(*trafficDistribution) {
		h = &preferCloseHeuristic{*trafficDistribution == corev1.ServiceTrafficDistributionPreferSameNode}
	}

	// 用原地过滤的方式把 hints 缺失或错误的切片从 unchanged 移到 update：j 指向下一个保留位置。
	// 技巧说明：https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	j := 0
	for _, slice := range slicesUnchanged {
		// 先判定该切片是否因 hints 不正确而需要重新生成。
		if h.needsUpdate(slice) {
			// 深拷贝后再加入待更新列表，避免修改 informer 缓存中的原始对象。
			slicesToUpdate = append(slicesToUpdate, slice.DeepCopy())
		} else {
			// 仍无需变化，原地保留该切片并推进写入位置。
			slicesUnchanged[j] = slice
			j++
		}
	}
	// 截断切片，只保留真正未发生变化的部分。
	slicesUnchanged = slicesUnchanged[:j]

	// 对所有新建或待更新的切片统一写入正确的 hints。
	for _, slice := range slicesToCreate {
		h.update(slice)
	}
	for _, slice := range slicesToUpdate {
		h.update(slice)
	}

	// 返回三组切片，调用方据此决定后续创建、更新或无需操作的资源。
	return slicesToCreate, slicesToUpdate, slicesUnchanged
}

// heuristic 定义 hints 协调的两个步骤：判断是否需要更新，以及执行更新。
type heuristic interface {
	// needsUpdate 判断切片中的 hints 是否缺失、错误或过期，需要重新生成。
	needsUpdate(*discoveryv1.EndpointSlice) bool
	// update 将切片中 Endpoint 的 hints 改写为当前策略期望的值。
	update(*discoveryv1.EndpointSlice)
}

// endpointReady 判断 Endpoint 的 Ready 条件是否被显式设置为 true。
func endpointReady(endpoint discoveryv1.Endpoint) bool {
	// Ready 为 nil（状态未知）时视为未就绪。
	return endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
}

// defaultHeuristic 表示集群范围路由，因此会移除 EndpointSlice 中出现的所有 hints。
type defaultHeuristic struct {
}

// needsUpdate 判断切片中是否存在带有 hints 的 Endpoint；只要有一个就需要清理。
func (defaultHeuristic) needsUpdate(slice *discoveryv1.EndpointSlice) bool {
	if slice == nil {
		// 空切片无需处理。
		return false
	}
	for _, endpoint := range slice.Endpoints {
		// 只要残留任何 hints（zone 或 node）就认为需要更新。
		if endpoint.Hints != nil {
			return true
		}
	}
	return false
}

// update 清除所有 Endpoint 的 hints，实现集群范围（cluster-wide）路由。
func (defaultHeuristic) update(slice *discoveryv1.EndpointSlice) {
	for i := range slice.Endpoints {
		// 直接置 nil，表示不限定任何拓扑区域。
		slice.Endpoints[i].Hints = nil
	}
}

// preferCloseHeuristic 实现 PreferClose/PreferSameZone 与 PreferSameNode 三种就近分发策略。
type preferCloseHeuristic struct {
	// generateNodeHints 控制是否同时生成节点级提示（ForNodes）；仅 PreferSameNode 为 true。
	generateNodeHints bool
}

// needsUpdate 判断就绪 Endpoint 的 hints 是否缺失、不正确或残留过期值。
func (h preferCloseHeuristic) needsUpdate(slice *discoveryv1.EndpointSlice) bool {
	if slice == nil {
		// 空切片无需处理。
		return false
	}
	for _, endpoint := range slice.Endpoints {
		// 未就绪的 Endpoint 不参与就近分发，忽略其 hints。
		if !endpointReady(endpoint) {
			continue
		}

		if endpoint.Zone != nil {
			// 有 zone 信息，期望恰好一个与之同名的 ForZones 提示。
			if endpoint.Hints == nil || len(endpoint.Hints.ForZones) != 1 || endpoint.Hints.ForZones[0].Name != *endpoint.Zone {
				// 缺失、数量不符或名称不匹配，都需要修正。
				return true
			}
		} else {
			// 没有 zone 信息，不应存在任何 ForZones 提示。
			if endpoint.Hints != nil && len(endpoint.Hints.ForZones) > 0 {
				// 残留旧的 zone 提示，需要清除。
				return true
			}
		}

		if endpoint.NodeName != nil && h.generateNodeHints {
			// 需要节点级提示时，期望恰好一个与 NodeName 同名的 ForNodes 提示。
			if endpoint.Hints == nil || len(endpoint.Hints.ForNodes) != 1 || endpoint.Hints.ForNodes[0].Name != *endpoint.NodeName {
				// 缺失、数量不符或名称不匹配，都需要修正。
				return true
			}
		} else {
			// 不需要节点级提示时，不应存在任何 ForNodes 提示。
			if endpoint.Hints != nil && len(endpoint.Hints.ForNodes) > 0 {
				// 残留旧的节点提示，需要清除。
				return true
			}
		}
	}
	return false
}

// update 为所有就绪 Endpoint 写入与其 zone（以及可选 node）一致的拓扑提示。
func (h preferCloseHeuristic) update(slice *discoveryv1.EndpointSlice) {
	for i, endpoint := range slice.Endpoints {
		// 未就绪 Endpoint 不参与就近分发，跳过。
		if !endpointReady(endpoint) {
			continue
		}

		// 先按拓扑信息构造两类提示列表，最后统一写回。
		var forZones []discoveryv1.ForZone
		var forNodes []discoveryv1.ForNode
		if endpoint.Zone != nil {
			// 每个 Endpoint 只生成一个同 zone 提示。
			forZones = []discoveryv1.ForZone{{Name: *endpoint.Zone}}
		}
		if endpoint.NodeName != nil && h.generateNodeHints {
			// 仅 PreferSameNode 场景才附加同 node 提示。
			forNodes = []discoveryv1.ForNode{{Name: *endpoint.NodeName}}
		}

		if forZones != nil || forNodes != nil {
			// 至少有一类提示时写入完整的 Hints 结构。
			slice.Endpoints[i].Hints = &discoveryv1.EndpointHints{
				ForZones: forZones,
				ForNodes: forNodes,
			}
		} else {
			// 无任何拓扑信息时清空 hints，回退到集群范围路由。
			slice.Endpoints[i].Hints = nil
		}
	}
}
