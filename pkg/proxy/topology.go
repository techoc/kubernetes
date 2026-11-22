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

package proxy

import (
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

// CategorizeEndpoints 把一个 Service 的 Endpoint 按流量策略和拓扑位置归类，返回：
//
//   - 该 Service 可用的 Cluster 策略 Endpoint（必要时考虑拓扑）；若 Service 从不使用 Cluster 策略则为 nil。
//
//   - 该 Service 可用的 Local 策略 Endpoint；若 Service 从不使用 Local 策略则为 nil。
//
//   - 从本节点可达的全部 Endpoint（前两个列表的并集；若与其中之一完全相同，则直接复用该列表，
//     避免额外分配）。
//
//   - 该 Service 在整个集群范围内是否存在任何可用 Endpoint 的标志（即使 allReachableEndpoints 为空，
//     该标志也可能为真，例如 Endpoint 存在于其他节点上）。
//
// “可用 Endpoint” 默认指 Ready 端点；若没有 Ready 端点，则退而使用 Serving-Terminating 端点，
// Cluster 与 Local 两个策略分别独立判断。
//
// 注意：NodeTopologyConfig.handleNodeEvent（pkg/proxy/config）在通知 proxier 之前会过滤拓扑标签。
// 如果这里改为关注其他 Endpoint 类型或标签，必须同步更新 NodeTopologyConfig 中的过滤逻辑。
func CategorizeEndpoints(endpoints []Endpoint, svcInfo ServicePort, nodeName string, topologyLabels map[string]string) (clusterEndpoints, localEndpoints, allReachableEndpoints []Endpoint, hasAnyEndpoints bool) {
	if len(endpoints) == 0 {
		// 没有 Endpoint 时无需分类，直接返回全零值（含 hasAnyEndpoints=false）。
		return
	}

	var topologyMode string
	var useServingTerminatingEndpoints bool

	if svcInfo.UsesClusterEndpoints() {
		// 先根据 Endpoint 提示和本节点拓扑确定分发模式，再据此过滤 Cluster 端点。
		zone := topologyLabels[v1.LabelTopologyZone]
		topologyMode = topologyModeFromHints(svcInfo, endpoints, nodeName, zone)
		clusterEndpoints = filterEndpoints(endpoints, func(ep Endpoint) bool {
			if !ep.IsReady() {
				return false
			}
			if !availableForTopology(ep, topologyMode, nodeName, zone) {
				return false
			}
			return true
		})

		// 没有任何端点通过过滤时，改用正在终止但仍可服务的端点再试一次。
		// 满足拓扑约束的 Ready 端点为空时回退到正在终止但仍在服务的端点，避免流量被完全丢弃。
		if len(clusterEndpoints) == 0 {
			clusterEndpoints = filterEndpoints(endpoints, func(ep Endpoint) bool {
				if ep.IsServing() && ep.IsTerminating() {
					return true
				}

				return false
			})
		}

		// 集群内只要存在 Ready 端点，Cluster 过滤结果必不为空，可据此认定“集群内可用”。
		if len(clusterEndpoints) > 0 {
			hasAnyEndpoints = true
		}
	}

	// 服务不使用 Local 策略时，可达端点就是 Cluster 端点；直接复用切片，省一次分配。
	if !svcInfo.UsesLocalEndpoints() {
		allReachableEndpoints = clusterEndpoints
		return
	}

	// 预扫描一次：判定 Local 策略应使用哪类端点（Ready 优先，否则用 Serving-Terminating），
	// 同时确认集群内是否任何位置存在可用端点（包括远端节点上的端点）。
	var hasLocalReadyEndpoints, hasLocalServingTerminatingEndpoints bool
	for _, ep := range endpoints {
		if ep.IsReady() {
			hasAnyEndpoints = true
			if ep.IsLocal() {
				hasLocalReadyEndpoints = true
			}
		} else if ep.IsServing() && ep.IsTerminating() {
			hasAnyEndpoints = true
			if ep.IsLocal() {
				hasLocalServingTerminatingEndpoints = true
			}
		}
	}

	// Ready 本地端点优先；只有完全没有时才回退到本节点上正在终止的端点。
	if hasLocalReadyEndpoints {
		localEndpoints = filterEndpoints(endpoints, func(ep Endpoint) bool {
			return ep.IsLocal() && ep.IsReady()
		})
	} else if hasLocalServingTerminatingEndpoints {
		useServingTerminatingEndpoints = true
		localEndpoints = filterEndpoints(endpoints, func(ep Endpoint) bool {
			return ep.IsLocal() && ep.IsServing() && ep.IsTerminating()
		})
	}

	// 只使用 Local 策略时，可达端点就是 Local 端点。
	if !svcInfo.UsesClusterEndpoints() {
		allReachableEndpoints = localEndpoints
		return
	}

	// 两个集合存在包含关系时直接复用 Cluster 列表，避免为 allReachableEndpoints 再分配内存。
	if topologyMode == "" && !useServingTerminatingEndpoints {
		allReachableEndpoints = clusterEndpoints
		return
	}

	// Cluster 列表含有远端端点，Local 列表可能含有因拓扑过滤或终止状态而未进入 Cluster 列表的本节点端点，
	// 两者互不包含，因此必须用字符串键做并集合并（同时天然去重）。
	endpointsMap := make(map[string]Endpoint, len(clusterEndpoints)+len(localEndpoints))
	for _, ep := range clusterEndpoints {
		endpointsMap[ep.String()] = ep
	}
	for _, ep := range localEndpoints {
		endpointsMap[ep.String()] = ep
	}
	allReachableEndpoints = make([]Endpoint, 0, len(endpointsMap))
	for _, ep := range endpointsMap {
		allReachableEndpoints = append(allReachableEndpoints, ep)
	}

	return
}

// topologyModeFromHints 根据 Endpoint 提示决定拓扑模式（""、"PreferSameZone" 或 "PreferSameNode"）：
//   - 若启用 PreferSameTrafficDistribution 特性门控，且所有 Ready 端点都有节点提示，
//     并且至少一个端点提示了本节点，则返回 "PreferSameNode"。
//   - 否则，若所有 Ready 端点都有区域提示，且至少一个端点提示了本节点所在区域，
//     则返回 "PreferSameZone"。
//   - 其他情况返回 ""（即不按拓扑分发，使用默认策略）。
//
// 只有“所有” Ready 端点都带同类提示时才启用同节点/同区域模式，避免部分端点无提示时把流量全压到少数端点。
func topologyModeFromHints(svcInfo ServicePort, endpoints []Endpoint, nodeName, zone string) string {
	hasReadyEndpoints := false
	hasEndpointForNode := false
	allEndpointsHaveNodeHints := true
	hasEndpointForZone := false
	allEndpointsHaveZoneHints := true
	// 只统计 Ready 端点：未就绪端点的提示不应影响模式判定。
	for _, endpoint := range endpoints {
		if !endpoint.IsReady() {
			continue
		}
		hasReadyEndpoints = true

		if endpoint.NodeHints().Len() == 0 {
			allEndpointsHaveNodeHints = false
		} else if endpoint.NodeHints().Has(nodeName) {
			hasEndpointForNode = true
		}

		if endpoint.ZoneHints().Len() == 0 {
			allEndpointsHaveZoneHints = false
		} else if endpoint.ZoneHints().Has(zone) {
			hasEndpointForZone = true
		}
	}

	// 没有 Ready 端点就没有提示可依据，退化为默认分发（后续会用 Serving-Terminating 端点）。
	if !hasReadyEndpoints {
		return ""
	}

	// 特性门控关闭时完全跳过同节点逻辑；即便开启，只要有一个端点缺节点提示也不能启用该模式。
	if utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution) {
		if allEndpointsHaveNodeHints {
			if hasEndpointForNode {
				return v1.ServiceTrafficDistributionPreferSameNode
			}
			klog.V(2).InfoS("Ignoring same-node topology hints for service since no hints were provided for node", "service", svcInfo, "node", nodeName)
		} else {
			klog.V(7).InfoS("Ignoring same-node topology hints for service since one or more endpoints is missing a node hint", "service", svcInfo)
		}
	}
	// 同理：区域提示也必须覆盖全部 Ready 端点且命中本节点区域，才启用同区域模式。
	if allEndpointsHaveZoneHints {
		if hasEndpointForZone {
			return v1.ServiceTrafficDistributionPreferSameZone
		}
		if zone == "" {
			klog.V(2).InfoS("Ignoring same-zone topology hints for service since node is missing label", "service", svcInfo, "label", v1.LabelTopologyZone)
		} else {
			klog.V(2).InfoS("Ignoring same-zone topology hints for service since no hints were provided for zone", "service", svcInfo, "zone", zone)
		}
	} else {
		klog.V(7).InfoS("Ignoring same-zone topology hints for service since one or more endpoints is missing a zone hint", "service", svcInfo.String())
	}

	return ""
}

// availableForTopology 判断在给定拓扑模式下，某个 Endpoint 是否可供本节点使用。
// 这里不做回退：回退逻辑发生在“选择模式”时（topologyModeFromHints），而不是应用模式时，
// 否则同一批端点会出现部分过滤、部分不过滤的不一致结果。
func availableForTopology(endpoint Endpoint, topologyMode, nodeName, zone string) bool {
	// 默认模式接受所有端点；同节点/同区域模式要求 Endpoint 的提示集合命中本节点/本区域。
	switch topologyMode {
	case "":
		return true
	case v1.ServiceTrafficDistributionPreferSameNode:
		return endpoint.NodeHints().Has(nodeName)
	case v1.ServiceTrafficDistributionPreferSameZone:
		return endpoint.ZoneHints().Has(zone)
	default:
		return false
	}
}

// filterEndpoints 按 predicate 过滤 Endpoint 列表，预先分配足够容量以避免逐个追加时的多次扩容。
func filterEndpoints(endpoints []Endpoint, predicate func(Endpoint) bool) []Endpoint {
	filteredEndpoints := make([]Endpoint, 0, len(endpoints))

	for _, ep := range endpoints {
		if predicate(ep) {
			filteredEndpoints = append(filteredEndpoints, ep)
		}
	}

	return filteredEndpoints
}
