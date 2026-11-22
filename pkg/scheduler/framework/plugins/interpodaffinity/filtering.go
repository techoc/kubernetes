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

package interpodaffinity

import (
	"context"
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// preFilterStateKey 是 CycleState 中存放过滤阶段预计算数据的键。
	// 用插件名作前缀可降低与其他插件的键冲突概率。
	preFilterStateKey = "PreFilter" + Name

	// ErrReasonExistingAntiAffinityRulesNotMatch 用于已有 Pod 的反亲和规则拒绝待调度 Pod 的场景。
	ErrReasonExistingAntiAffinityRulesNotMatch = "node(s) didn't satisfy existing pods anti-affinity rules"
	// ErrReasonAffinityRulesNotMatch 用于亲和规则不满足的场景。
	ErrReasonAffinityRulesNotMatch = "node(s) didn't match pod affinity rules"
	// ErrReasonAntiAffinityRulesNotMatch 用于反亲和规则冲突的场景。
	ErrReasonAntiAffinityRulesNotMatch = "node(s) didn't match pod anti-affinity rules"
)

// preFilterState 在 PreFilter 阶段计算，供 Filter 阶段使用。
//
// InterPodAffinityHostnameFastPath 优化概览：
// 该特性针对仅使用 kubernetes.io/hostname 拓扑的硬性（反）亲和规则，
// 优化 PreFilter 与 Filter 的性能。
//
// 一般情况下，集群中任意位置的已有 Pod 都可能影响待调度 Pod 能否落到某个节点
// （例如要求同 zone），因此标准算法在 PreFilter 阶段构建全集群拓扑映射，
// Filter 时再遍历候选节点的每个标签做哈希查找。
//
// 当规则只使用 kubernetes.io/hostname 时，只关心该节点本地的 Pod。
// 将主机域规则单独拆出后，可完全跳过 PreFilter 的拓扑图构建与 Filter 的节点标签遍历。
//
// 具体做法：
//
//  1. 已有 Pod 的反亲和：
//     已有 Pod 的主机域反亲和项不加入全局拓扑图（existingClusterWideAntiAffinityCounts），
//     Filter 中直接对候选节点上的 Pod 做本地检查。
//
//  2. 待调度 Pod 的反亲和：
//     同理，待调度 Pod 自身的主机域反亲和项也不构建全局映射，
//     推迟到 Filter 阶段做廉价的本地检查。
//
//  3. 待调度 Pod 的亲和：
//     亲和更复杂：需要找到一个同时满足所有亲和项的 Pod。
//     - 混合拓扑：只要有一个非主机域亲和项，就必须构建全局簇映射来跨拓扑域跟踪匹配。
//     - 纯主机域：全部亲和项都是主机域时，完全跳过全局映射，只在本地 Filter 阶段验证。
//
//  4. 自亲和回退（matchingHostScopedAffinityPodsCount）：
//     若没有其他 Pod 匹配亲和规则，但待调度 Pod 自身满足规则，也应允许调度。
//     通常全局映射为空即可判定无人匹配；跳过构建映射后则失去该全局视角。
//     因此改用一个计数器（matchingHostScopedAffinityPodsCount）跟踪是否有任何匹配 Pod，
//     只要它仍为 0 就允许自亲和 Pod 调度。
//
//  5. PreFilterExtensions（抢占 / AddPod / RemovePod）：
//     抢占模拟驱逐 Pod 时通过 AddPod/RemovePod 增量更新状态，避免从头重建。
//     纯主机域规则下，增删模拟仅需增减计数器（matchingHostScopedAffinityPodsCount），
//     无需更新全局拓扑图。

type preFilterState struct {
	// existingClusterWideAntiAffinityCounts 记录拓扑对到已有 Pod 数量的映射，
	// 这些 Pod 的反亲和项匹配待调度 Pod，即待调度 Pod 违反了它们的规则。
	// 启用 InterPodAffinityHostnameFastPath 时只统计非主机域项（主机域项在 Filter 本地检查）；
	// 关闭时统计所有项。
	existingClusterWideAntiAffinityCounts topologyToMatchedTermCount
	// clusterWideAffinityCounts 记录拓扑对到匹配待调度 Pod 亲和项的已有 Pod 数量。
	// 启用快速路径时只跟踪 clusterWideAffinityTerms 中的项：存在非主机域项时包含全部亲和项，否则为空；
	// 关闭时跟踪所有亲和项。
	clusterWideAffinityCounts topologyToMatchedTermCount
	// clusterWideAntiAffinityCounts 记录拓扑对到匹配待调度 Pod 反亲和项的已有 Pod 数量。
	// 启用快速路径时只跟踪非主机域项；关闭时跟踪所有反亲和项。
	clusterWideAntiAffinityCounts topologyToMatchedTermCount
	// podInfo 是待调度 Pod 的信息。
	podInfo fwk.PodInfo
	// namespaceLabels 是待调度 Pod 所在命名空间的标签副本。
	namespaceLabels labels.Set

	// hostScopedAffinityTerms 是待调度 Pod 中将在 Filter 本地检查的亲和项。
	// 启用快速路径时，仅当所有亲和项都是主机域时才填充；关闭时始终为空。
	hostScopedAffinityTerms []fwk.AffinityTerm
	// hostScopedAntiAffinityTerms 是待调度 Pod 中将在 Filter 本地检查的反亲和项。
	// 启用快速路径时包含所有主机域反亲和项；关闭时始终为空。
	hostScopedAntiAffinityTerms []fwk.AffinityTerm
	// clusterWideAffinityTerms 是需要全集群扫描的亲和项。
	// 启用快速路径时：只要存在非主机域项，就填入待调度 Pod 的所有亲和项；纯主机域时为空。
	// 关闭时：无论作用域，包含所有亲和项。
	clusterWideAffinityTerms []fwk.AffinityTerm
	// clusterWideAntiAffinityTerms 是需要全集群扫描的反亲和项。
	// 启用快速路径时只包含非主机域反亲和项；关闭时包含所有反亲和项。
	clusterWideAntiAffinityTerms []fwk.AffinityTerm
	// matchingHostScopedAffinityPodsCount 是集群中匹配待调度 Pod 主机域亲和项的 Pod 总数。
	// 仅当所有亲和项都是主机域时用于优化检查。
	// 快速路径关闭时该值恒为 0。
	matchingHostScopedAffinityPodsCount int64
	// enableInterPodAffinityHostnameFastPath 表示是否启用了对应的特性门控。
	enableInterPodAffinityHostnameFastPath bool
}

// Clone 返回 preFilterState 的副本，供框架在分支节点时复制状态。
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}

	copy := preFilterState{}
	copy.clusterWideAffinityCounts = s.clusterWideAffinityCounts.clone()
	copy.clusterWideAntiAffinityCounts = s.clusterWideAntiAffinityCounts.clone()
	copy.existingClusterWideAntiAffinityCounts = s.existingClusterWideAntiAffinityCounts.clone()
	// podInfo 在调度周期内不变，直接共享引用即可。
	copy.podInfo = s.podInfo
	copy.namespaceLabels = s.namespaceLabels
	copy.clusterWideAffinityTerms = s.clusterWideAffinityTerms
	copy.clusterWideAntiAffinityTerms = s.clusterWideAntiAffinityTerms
	copy.hostScopedAffinityTerms = s.hostScopedAffinityTerms
	copy.hostScopedAntiAffinityTerms = s.hostScopedAntiAffinityTerms
	copy.matchingHostScopedAffinityPodsCount = s.matchingHostScopedAffinityPodsCount
	copy.enableInterPodAffinityHostnameFastPath = s.enableInterPodAffinityHostnameFastPath
	return &copy
}

// updateWithPod 按给定 Pod 的（反）亲和匹配情况增量更新 preFilterState 计数器，
// multiplier 取 +1 表示添加、-1 表示移除，用于抢占模拟。
func (s *preFilterState) updateWithPod(pInfo fwk.PodInfo, node *v1.Node, multiplier int64) {
	if s == nil {
		return
	}

	// 处理已有 Pod 的反亲和项，判断待调度 Pod 是否违反这些规则。
	if s.enableInterPodAffinityHostnameFastPath {
		// 已有 Pod 的亲和项未按作用域预分类，因此在这里逐项检查拓扑键。
		for _, t := range pInfo.GetRequiredAntiAffinityTerms() {
			if t.TopologyKey != v1.LabelHostname {
				if t.Matches(s.podInfo.GetPod(), s.namespaceLabels) {
					s.existingClusterWideAntiAffinityCounts.update(node, t.TopologyKey, multiplier)
				}
			}
		}
	} else {
		s.existingClusterWideAntiAffinityCounts.updateWithAntiAffinityTerms(pInfo.GetRequiredAntiAffinityTerms(), s.podInfo.GetPod(), s.namespaceLabels, node, multiplier)
	}
	// 处理待调度 Pod 自身的亲和与反亲和项。
	if s.enableInterPodAffinityHostnameFastPath {
		// 只需评估 s.clusterWideAffinityTerms 与 s.clusterWideAntiAffinityTerms 中需要全局计算的项。
		// 待调度 Pod 的项已在 PreFilter 中按作用域预处理，无需再手动检查拓扑键。
		s.clusterWideAffinityCounts.updateWithAffinityTerms(s.clusterWideAffinityTerms, pInfo.GetPod(), node, multiplier)
		// 待调度 Pod 的 namespaceSelector 已合并进 namespaces，无需再查命名空间标签，故传 nil。
		s.clusterWideAntiAffinityCounts.updateWithAntiAffinityTerms(s.clusterWideAntiAffinityTerms, pInfo.GetPod(), nil, node, multiplier)

		if len(s.hostScopedAffinityTerms) > 0 {
			// hostScopedAffinityTerms 只在所有亲和项都是主机域时填充。
			//
			// 插件维护一个全局匹配 Pod 计数器，仅在纯主机域亲和项场景下更新。
			// 计数为 0 表示没有已有 Pod 满足规则，允许自亲和组中的第一个 Pod 通过检查。
			// 若存在非主机域项，则不依赖该计数，而改查 len(clusterWideAffinityCounts) == 0。
			// multiplier（+1/-1）在抢占等模拟场景中调整计数。
			if podMatchesAllAffinityTerms(s.hostScopedAffinityTerms, pInfo.GetPod()) {
				s.matchingHostScopedAffinityPodsCount += multiplier
			}
		}
	} else {
		s.clusterWideAffinityCounts.updateWithAffinityTerms(s.podInfo.GetRequiredAffinityTerms(), pInfo.GetPod(), node, multiplier)
		// 待调度 Pod 的 namespaceSelector 已合并进 namespaces，无需再查命名空间标签，故传 nil。
		s.clusterWideAntiAffinityCounts.updateWithAntiAffinityTerms(s.podInfo.GetRequiredAntiAffinityTerms(), pInfo.GetPod(), nil, node, multiplier)
	}
}

// classifyTermsBasedOnScope 将待调度 Pod 的（反）亲和项按作用域分组：
//   - clusterWideAffinityTerms：需要全集群范围评估的亲和项
//   - clusterWideAntiAffinityTerms：需要全集群范围评估的反亲和项
//   - hostScopedAffinityTerms：可在节点本地评估的亲和项，跳过昂贵的全局扫描
//
// 注意同一项可能同时出现在多个列表中。
//
// 对于亲和：节点（或其所在 zone 等更广拓扑）上已有匹配 Pod 时节点才可用。
// 算法要在该拓扑中找到一个同时满足所有亲和项的 Pod，
// 因此主机域与非主机域项不能独立评估，必须一起留在 clusterWideAffinityTerms 中。
//
// 对于反亲和：只需任一约束被违反即拒绝节点。
// 主机域反亲和总是可以本地检查，无需全局扫描；
// 例如同时有主机名与 zone 反亲和规则时，若违反主机名规则可立即拒绝节点，不必再查 zone 规则。
//
// 若 Pod 只有主机域亲和与反亲和项，两个 clusterWide 列表均为空，
// 此时 getIncomingAffinityAntiAffinityCounts 会提前返回，跳过遍历全集群的并行循环。
func (s *preFilterState) classifyTermsBasedOnScope() {
	if s.allHostScoped() {
		s.hostScopedAffinityTerms = s.podInfo.GetRequiredAffinityTerms()
	} else {
		s.clusterWideAffinityTerms = s.podInfo.GetRequiredAffinityTerms()
	}

	for _, t := range s.podInfo.GetRequiredAntiAffinityTerms() {
		if t.TopologyKey == v1.LabelHostname {
			s.hostScopedAntiAffinityTerms = append(s.hostScopedAntiAffinityTerms, t)
		} else {
			s.clusterWideAntiAffinityTerms = append(s.clusterWideAntiAffinityTerms, t)
		}
	}
}

// allHostScoped 判断是否可跳过昂贵的全局拓扑扫描：
// 所有亲和项都是主机域时，只需检查本节点 Pod，速度更快。
func (s *preFilterState) allHostScoped() bool {
	for _, t := range s.podInfo.GetRequiredAffinityTerms() {
		if t.TopologyKey != v1.LabelHostname {
			return false
		}
	}
	return true
}

// topologyPair 表示拓扑键与对应取值组成的一对，作为计数映射的键。
type topologyPair struct {
	key   string
	value string
}

// topologyToMatchedTermCount 记录各拓扑对上匹配项的 Pod 数量。
type topologyToMatchedTermCount map[topologyPair]int64

// merge 将另一个计数映射累加到当前映射中。
func (m topologyToMatchedTermCount) merge(toMerge topologyToMatchedTermCount) {
	for pair, count := range toMerge {
		m[pair] += count
	}
}

// mergeWithList 将列表形式的计数累加到当前映射中。
func (m topologyToMatchedTermCount) mergeWithList(toMerge topologyToMatchedTermCountList) {
	for _, tmtc := range toMerge {
		m[tmtc.topologyPair] += tmtc.count
	}
}

// clone 返回计数映射的浅拷贝。
func (m topologyToMatchedTermCount) clone() topologyToMatchedTermCount {
	copy := make(topologyToMatchedTermCount, len(m))
	copy.merge(m)
	return copy
}

// update 按节点上 tk 拓扑键的标签值增减对应拓扑对的计数，
// 计数归零时删除该项以节省内存。
func (m topologyToMatchedTermCount) update(node *v1.Node, tk string, value int64) {
	if tv, ok := node.Labels[tk]; ok {
		pair := topologyPair{key: tk, value: tv}
		m[pair] += value
		// value 可能为负，计数归零后删除条目避免保留无效键。
		if m[pair] == 0 {
			delete(m, pair)
		}
	}
}

// updateWithAffinityTerms 在 targetPod 匹配所有亲和项时，
// 逐项按指定 value 更新计数映射。
func (m topologyToMatchedTermCount) updateWithAffinityTerms(
	terms []fwk.AffinityTerm, pod *v1.Pod, node *v1.Node, value int64) {
	if podMatchesAllAffinityTerms(terms, pod) {
		for _, t := range terms {
			m.update(node, t.TopologyKey, value)
		}
	}
}

// updateWithAntiAffinityTerms 对所有匹配 targetPod 的反亲和项，
// 按指定 value 更新计数映射。
func (m topologyToMatchedTermCount) updateWithAntiAffinityTerms(terms []fwk.AffinityTerm, pod *v1.Pod, nsLabels labels.Set, node *v1.Node, value int64) {
	// 逐项检查反亲和是否匹配。
	for _, t := range terms {
		if t.Matches(pod, nsLabels) {
			m.update(node, t.TopologyKey, value)
		}
	}
}

// topologyToMatchedTermCountList 是计数映射的切片形式，
// PreFilter 中合并切片比遍历 map 更快。
type topologyToMatchedTermCountList []topologyPairCount

// topologyPairCount 是切片形式的一个拓扑对计数条目。
type topologyPairCount struct {
	topologyPair topologyPair
	count        int64
}

// recordMatch 按节点上 tk 拓扑键的标签值向切片追加一条匹配记录。
func (m *topologyToMatchedTermCountList) recordMatch(node *v1.Node, tk string, value int64) {
	if tv, ok := node.Labels[tk]; ok {
		pair := topologyPair{key: tk, value: tv}
		*m = append(*m, topologyPairCount{
			topologyPair: pair,
			count:        value,
		})
	}
}

// appendWithAffinityTerms 在 targetPod 匹配所有亲和项时，
// 逐项向列表追加指定值。
func (m *topologyToMatchedTermCountList) appendWithAffinityTerms(
	terms []fwk.AffinityTerm, pod *v1.Pod, node *v1.Node, value int64) {
	if podMatchesAllAffinityTerms(terms, pod) {
		for _, t := range terms {
			m.recordMatch(node, t.TopologyKey, value)
		}
	}
}

// appendWithAntiAffinityTerms 对所有匹配 targetPod 的反亲和项，
// 逐项向列表追加指定值。
func (m *topologyToMatchedTermCountList) appendWithAntiAffinityTerms(terms []fwk.AffinityTerm, pod *v1.Pod, nsLabels labels.Set, node *v1.Node, value int64) {
	// 逐项检查反亲和是否匹配。
	for _, t := range terms {
		if t.Matches(pod, nsLabels) {
			m.recordMatch(node, t.TopologyKey, value)
		}
	}
}

// podMatchesAllAffinityTerms 在项列表非空且 Pod 匹配所有项时返回 true。
// 用于亲和检查；反亲和请用 podMatchesAnyAffinityTerms。
func podMatchesAllAffinityTerms(terms []fwk.AffinityTerm, pod *v1.Pod) bool {
	if len(terms) == 0 {
		return false
	}
	for _, t := range terms {
		// 待调度 Pod 的 namespaceSelector 已合并进 Namespaces，无需显式传入命名空间标签。
		if !t.Matches(pod, nil) {
			return false
		}
	}
	return true
}

// podMatchesAnyAffinityTerms 在 Pod 匹配任一项时返回 true，用于反亲和检查。
func podMatchesAnyAffinityTerms(terms []fwk.AffinityTerm, pod *v1.Pod) bool {
	for _, t := range terms {
		if t.Matches(pod, nil) {
			return true
		}
	}
	return false
}

// getExistingAntiAffinityCounts 并发扫描节点，评估已有 Pod 的反亲和项是否匹配待调度 Pod，
// 统计会产生调度冲突的情况。返回两个值：
//
//  1. 拓扑对到匹配 Pod 数量的映射：启用快速路径时只统计非主机域项
//     （主机域项在 Filter 中动态检查），关闭时统计所有匹配项。
//
//  2. hasHostScopedAntiAffinity：是否存在主机域反亲和项。启用快速路径时
//     为 true 表示 Filter 需要做节点本地检查；关闭时恒为 false。
//
// 若该布尔值为 false 且计数映射为空，Filter 阶段可直接跳过。
func (pl *InterPodAffinity) getExistingAntiAffinityCounts(ctx context.Context, incomingPod *v1.Pod, nsLabels labels.Set, nodes []fwk.NodeInfo) (topologyToMatchedTermCount, bool) {
	antiAffinityCountsList := make([]topologyToMatchedTermCountList, len(nodes))
	index := int32(-1)
	var hasHostScopedAntiAffinity atomic.Bool
	processNode := func(i int) {
		nodeInfo := nodes[i]
		node := nodeInfo.Node()

		clusterWideAntiAffinityCounts := make(topologyToMatchedTermCountList, 0)
		hasHostScoped := false
		for _, existingPod := range nodeInfo.GetPodsWithRequiredAntiAffinity() {
			for _, term := range existingPod.GetRequiredAntiAffinityTerms() {
				if pl.enableInterPodAffinityHostnameFastPath && term.TopologyKey == v1.LabelHostname {
					hasHostScoped = true
				} else if term.Matches(incomingPod, nsLabels) {
					clusterWideAntiAffinityCounts.recordMatch(node, term.TopologyKey, 1)
				}
			}
		}
		if hasHostScoped && !hasHostScopedAntiAffinity.Load() {
			hasHostScopedAntiAffinity.Store(true)
		}
		if len(clusterWideAntiAffinityCounts) != 0 {
			antiAffinityCountsList[atomic.AddInt32(&index, 1)] = clusterWideAntiAffinityCounts
		}
	}
	pl.parallelizer.Until(ctx, len(nodes), processNode, pl.Name())

	result := make(topologyToMatchedTermCount)
	// 此场景下传统 for 循环比 for range 略快。
	for i := 0; i <= int(index); i++ {
		result.mergeWithList(antiAffinityCountsList[i])
	}

	return result, hasHostScopedAntiAffinity.Load()
}

// getIncomingAffinityAntiAffinityCounts 并发扫描所有节点，
// 统计匹配待调度 Pod 亲和与反亲和项的已有 Pod 数量。
// 返回两张（拓扑 -> 匹配数）映射：亲和匹配与反亲和冲突。
// 启用快速路径时，调用方只传入需要全局评估的项（见 classifyTermsBasedOnScope）。
func (pl *InterPodAffinity) getIncomingAffinityAntiAffinityCounts(ctx context.Context, affinityTerms, antiAffinityTerms []fwk.AffinityTerm, allNodes []fwk.NodeInfo) (topologyToMatchedTermCount, topologyToMatchedTermCount) {
	clusterWideAffinityCounts := make(topologyToMatchedTermCount)
	clusterWideAntiAffinityCounts := make(topologyToMatchedTermCount)
	if len(affinityTerms) == 0 && len(antiAffinityTerms) == 0 {
		return clusterWideAffinityCounts, clusterWideAntiAffinityCounts
	}

	affinityCountsList := make([]topologyToMatchedTermCountList, len(allNodes))
	antiAffinityCountsList := make([]topologyToMatchedTermCountList, len(allNodes))
	index := int32(-1)
	processNode := func(i int) {
		nodeInfo := allNodes[i]
		node := nodeInfo.Node()

		affinity := make(topologyToMatchedTermCountList, 0)
		antiAffinity := make(topologyToMatchedTermCountList, 0)
		for _, existingPod := range nodeInfo.GetPods() {
			affinity.appendWithAffinityTerms(affinityTerms, existingPod.GetPod(), node, 1)
			// 待调度 Pod 的 namespaceSelector 已合并进 namespaces，无需查已有 Pod 的命名空间标签。
			antiAffinity.appendWithAntiAffinityTerms(antiAffinityTerms, existingPod.GetPod(), nil, node, 1)
		}

		if len(affinity) > 0 || len(antiAffinity) > 0 {
			k := atomic.AddInt32(&index, 1)
			affinityCountsList[k] = affinity
			antiAffinityCountsList[k] = antiAffinity
		}
	}
	pl.parallelizer.Until(ctx, len(allNodes), processNode, pl.Name())

	for i := 0; i <= int(index); i++ {
		clusterWideAffinityCounts.mergeWithList(affinityCountsList[i])
		clusterWideAntiAffinityCounts.mergeWithList(antiAffinityCountsList[i])
	}

	return clusterWideAffinityCounts, clusterWideAntiAffinityCounts
}

// PreFilter 在预过滤扩展点执行，为逐节点的 Filter 阶段预计算状态：
// 将待调度 Pod 与集群中已有 Pod 比对，构建（拓扑对 -> 匹配数）映射以强制执行亲和/反亲和。
//
// 关闭 InterPodAffinityHostnameFastPath 时：
//  1. 已有 Pod 的反亲和：只查询含硬性反亲和 Pod 的节点子集，
//     遍历这些节点上的 Pod 填充 existingClusterWideAntiAffinityCounts。
//  2. 待调度 Pod 的约束：遍历全部节点与 Pod，填充 clusterWideAntiAffinityCounts 与
//     clusterWideAffinityCounts 两张全局映射。
//
// 开启快速路径时，通过忽略 kubernetes.io/hostname 项来缩小映射规模：
//  1. 已有 Pod 的反亲和：只查询含非主机域反亲和 Pod 的节点，构建全局映射；
//     并对比节点数组长度判断 Filter 阶段是否还需做主机域检查。
//  2. 待调度 Pod 的约束：按作用域分类，只为非主机域项构建全局映射。
//  3. 自亲和回退：全部亲和项都是主机域时不建映射，
//     改为并行遍历全部节点统计匹配 Pod 数到 matchingHostScopedAffinityPodsCount。
//
// 若没有构建任何全局映射、无需主机域检查且待调度 Pod 无约束，
// 则返回 fwk.Skip 让 Filter 阶段完全跳过该插件。
func (pl *InterPodAffinity) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, allNodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	s := &preFilterState{
		enableInterPodAffinityHostnameFastPath: pl.enableInterPodAffinityHostnameFastPath,
	}
	var err error
	if s.podInfo, err = framework.NewPodInfo(pod); err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("parsing pod: %+v", err))
	}

	requiredAffinityTerms := s.podInfo.GetRequiredAffinityTerms()
	for i := range requiredAffinityTerms {
		if err := pl.mergeAffinityTermNamespacesIfNotEmpty(&requiredAffinityTerms[i]); err != nil {
			return nil, fwk.AsStatus(err)
		}
	}
	requiredAntiAffinityTerms := s.podInfo.GetRequiredAntiAffinityTerms()
	for i := range requiredAntiAffinityTerms {
		if err := pl.mergeAffinityTermNamespacesIfNotEmpty(&requiredAntiAffinityTerms[i]); err != nil {
			return nil, fwk.AsStatus(err)
		}
	}

	logger := klog.FromContext(ctx)
	s.namespaceLabels = GetNamespaceLabelsSnapshot(logger, pod.Namespace, pl.nsLister)

	var mustEvaluateHostScopedAntiAffinity bool
	if s.enableInterPodAffinityHostnameFastPath {
		var nodesWithNonHostScopedAntiAffinityPods []fwk.NodeInfo
		var nodesWithAnyRequiredAntiAffinityPods []fwk.NodeInfo
		if nodesWithNonHostScopedAntiAffinityPods, err = pl.sharedLister.NodeInfos().HavePodsWithRequiredNonHostScopedAntiAffinityList(); err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("failed to list NodeInfos with pods with required anti-affinity terms where topologyKey != kubernetes.io/hostname: %w", err))
		}
		if nodesWithAnyRequiredAntiAffinityPods, err = pl.sharedLister.NodeInfos().HavePodsWithRequiredAntiAffinityList(); err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("failed to list NodeInfos with pods with affinity: %w", err))
		}

		s.classifyTermsBasedOnScope()

		var nonHostScopedNodesContainHostScopedTerms bool
		s.existingClusterWideAntiAffinityCounts, nonHostScopedNodesContainHostScopedTerms = pl.getExistingAntiAffinityCounts(ctx, pod, s.namespaceLabels, nodesWithNonHostScopedAntiAffinityPods)
		nodeExistsWithPodsWithOnlyHostScopedAntiAffinityTerms := len(nodesWithAnyRequiredAntiAffinityPods) > len(nodesWithNonHostScopedAntiAffinityPods)
		mustEvaluateHostScopedAntiAffinity = nonHostScopedNodesContainHostScopedTerms || nodeExistsWithPodsWithOnlyHostScopedAntiAffinityTerms

		s.clusterWideAffinityCounts, s.clusterWideAntiAffinityCounts = pl.getIncomingAffinityAntiAffinityCounts(ctx, s.clusterWideAffinityTerms, s.clusterWideAntiAffinityTerms, allNodes)

		// 检查集群中是否存在匹配主机域亲和项的 Pod，用于自亲和回退判定。
		if len(s.hostScopedAffinityTerms) > 0 {
			s.matchingHostScopedAffinityPodsCount = pl.countMatchingHostScopedAffinityPodsGlobally(ctx, allNodes, s)
		}
	} else {
		var nodesWithRequiredAntiAffinityPods []fwk.NodeInfo
		if nodesWithRequiredAntiAffinityPods, err = pl.sharedLister.NodeInfos().HavePodsWithRequiredAntiAffinityList(); err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("failed to list NodeInfos with pods with affinity: %w", err))
		}
		s.existingClusterWideAntiAffinityCounts, _ = pl.getExistingAntiAffinityCounts(ctx, pod, s.namespaceLabels, nodesWithRequiredAntiAffinityPods)
		s.clusterWideAffinityCounts, s.clusterWideAntiAffinityCounts = pl.getIncomingAffinityAntiAffinityCounts(ctx, s.podInfo.GetRequiredAffinityTerms(), s.podInfo.GetRequiredAntiAffinityTerms(), allNodes)
		s.clusterWideAffinityTerms = s.podInfo.GetRequiredAffinityTerms()
		s.clusterWideAntiAffinityTerms = s.podInfo.GetRequiredAntiAffinityTerms()
	}

	incomingPodHasNoTerms := len(s.podInfo.GetRequiredAffinityTerms()) == 0 && len(s.podInfo.GetRequiredAntiAffinityTerms()) == 0
	if len(s.existingClusterWideAntiAffinityCounts) == 0 && incomingPodHasNoTerms && !mustEvaluateHostScopedAntiAffinity {
		return nil, fwk.NewStatus(fwk.Skip)
	}

	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}

// countMatchingHostScopedAffinityPodsGlobally 并行统计全集群中匹配主机域亲和项的 Pod 数量。
func (pl *InterPodAffinity) countMatchingHostScopedAffinityPodsGlobally(ctx context.Context, allNodes []fwk.NodeInfo, s *preFilterState) int64 {
	var count int64
	countMatchingPodsOnNode := func(i int) {
		nodeInfo := allNodes[i]
		var localCount int64
		for _, existingPod := range nodeInfo.GetPods() {
			if podMatchesAllAffinityTerms(s.hostScopedAffinityTerms, existingPod.GetPod()) {
				localCount++
			}
		}
		if localCount > 0 {
			atomic.AddInt64(&count, localCount)
		}
	}
	pl.parallelizer.Until(ctx, len(allNodes), countMatchingPodsOnNode, pl.Name())
	return count
}

// PreFilterExtensions 返回 PreFilter 扩展（AddPod/RemovePod）。
func (pl *InterPodAffinity) PreFilterExtensions() fwk.PreFilterExtensions {
	return pl
}

// AddPod 在抢占模拟中添加 Pod 时增量更新预计算状态。
func (pl *InterPodAffinity) AddPod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToAdd, nodeInfo.Node(), 1)
	return nil
}

// RemovePod 在抢占模拟中移除 Pod 时增量更新预计算状态。
func (pl *InterPodAffinity) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToRemove, nodeInfo.Node(), -1)
	return nil
}

// getPreFilterState 从 CycleState 读取本插件的预计算状态。
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// 读不到状态通常意味着 PreFilter 未被调用。
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to interpodaffinity.state error", c)
	}
	return s, nil
}

// satisfyExistingPodsAntiAffinity 检查将 Pod 调度到该节点是否违反已有 Pod 声明的反亲和规则。
func satisfyExistingPodsAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.existingClusterWideAntiAffinityCounts) > 0 {
		// 遍历节点标签对应的拓扑对，找出受待调度 Pod 反亲和项影响的匹配。
		for topologyKey, topologyValue := range nodeInfo.Node().Labels {
			tp := topologyPair{key: topologyKey, value: topologyValue}
			if state.existingClusterWideAntiAffinityCounts[tp] > 0 {
				return false
			}
		}
	}

	// 对主机域反亲和做节点本地检查。
	if state.enableInterPodAffinityHostnameFastPath {
		// 节点必须带有 kubernetes.io/hostname 标签，主机域项才可能匹配。
		if _, ok := nodeInfo.Node().Labels[v1.LabelHostname]; ok {
			for _, existingPodInfo := range nodeInfo.GetPodsWithRequiredAntiAffinity() {
				for _, term := range existingPodInfo.GetRequiredAntiAffinityTerms() {
					if term.TopologyKey == v1.LabelHostname {
						if term.Matches(state.podInfo.GetPod(), state.namespaceLabels) {
							return false
						}
					}
				}
			}
		}
	}

	return true
}

// nodeTopologyConflictsWithAntiAffinity 检查节点的更广拓扑域中是否存在与待调度 Pod
// 反亲和规则冲突的 Pod（仅限非 kubernetes.io/hostname 拓扑），
// 依据 PreFilter 预计算的 clusterWideAntiAffinityCounts。
func nodeTopologyConflictsWithAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.clusterWideAntiAffinityCounts) > 0 {
		for _, term := range state.clusterWideAntiAffinityTerms {
			if topologyValue, ok := nodeInfo.Node().Labels[term.TopologyKey]; ok {
				tp := topologyPair{key: term.TopologyKey, value: topologyValue}
				if state.clusterWideAntiAffinityCounts[tp] > 0 {
					return true
				}
			}
		}
	}
	return false
}

// nodePodsConflictWithAntiAffinity 判断目标节点上是否已有与待调度 Pod 主机域反亲和规则冲突的 Pod。
// 用于 topologyKey 为 kubernetes.io/hostname 的反亲和项；
// 由于为每个节点预计算全局计数代价过高，这里基于节点 PodInfo 列表动态匹配。
func nodePodsConflictWithAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.hostScopedAntiAffinityTerms) == 0 {
		return false
	}
	if _, ok := nodeInfo.Node().Labels[v1.LabelHostname]; !ok {
		// 节点必须带有 kubernetes.io/hostname 标签，主机域项才可能匹配。
		return false
	}

	for _, existingPod := range nodeInfo.GetPods() {
		for _, term := range state.hostScopedAntiAffinityTerms {
			if term.Matches(existingPod.GetPod(), nil) {
				return true
			}
		}
	}
	return false
}

// satisfyPodAntiAffinity 检查节点上已有 Pod 是否满足待调度 Pod 的反亲和规则。
// 仅在快速路径特性关闭时使用。
func satisfyPodAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.clusterWideAntiAffinityCounts) > 0 {
		for _, term := range state.podInfo.GetRequiredAntiAffinityTerms() {
			if topologyValue, ok := nodeInfo.Node().Labels[term.TopologyKey]; ok {
				tp := topologyPair{key: term.TopologyKey, value: topologyValue}
				if state.clusterWideAntiAffinityCounts[tp] > 0 {
					return false
				}
			}
		}
	}
	return true
}

// nodePodsSatisfyAffinity 快速检查目标节点上是否已有满足亲和规则的 Pod（或满足自亲和）。
// 仅当所有亲和项都是 kubernetes.io/hostname 拓扑时使用，跳过全局检查；
// 为每个节点预计算全局计数代价过高，因此基于本地 PodInfo 动态匹配。
func nodePodsSatisfyAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.hostScopedAffinityTerms) == 0 {
		return true
	}
	if _, ok := nodeInfo.Node().Labels[v1.LabelHostname]; !ok {
		// 节点必须带有 kubernetes.io/hostname 标签，主机域项才可能匹配。
		return false
	}

	for _, existingPod := range nodeInfo.GetPods() {
		if podMatchesAllAffinityTerms(state.hostScopedAffinityTerms, existingPod.GetPod()) {
			return true
		}
	}
	// 未找到匹配的已有 Pod，检查是否满足自亲和。
	return satisfiesSelfAffinityCheck(state, nodeInfo)
}

// nodeTopologyMatchesAffinity 检查节点拓扑域中是否有匹配亲和项的 Pod（或满足自亲和）。
// 仅当存在至少一个非 kubernetes.io/hostname 亲和项时使用，此时所有亲和项都在这里评估。
// 依据 PreFilter 预计算的 clusterWideAffinityCounts。
func nodeTopologyMatchesAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.clusterWideAffinityTerms) > 0 {
		podsExist := true
		for _, term := range state.clusterWideAffinityTerms {
			if topologyValue, ok := nodeInfo.Node().Labels[term.TopologyKey]; ok {
				tp := topologyPair{key: term.TopologyKey, value: topologyValue}
				if state.clusterWideAffinityCounts[tp] <= 0 {
					podsExist = false
				}
			} else {
				// 即使是自亲和场景，节点也必须具备所有拓扑标签，否则不满足约束。
				return false
			}
		}

		if !podsExist {
			return len(state.clusterWideAffinityCounts) == 0 && satisfiesSelfAffinityCheck(state, nodeInfo)
		}
		return true
	}
	return true
}

// 该 Pod 可能是一组自亲和 Pod 中的第一个。为避免其永久 Pending，
// 当集群中没有其他 Pod 匹配其命名空间与选择器、且自身满足亲和项、
// 节点具备所需全部拓扑标签时，允许通过亲和检查。
func satisfiesSelfAffinityCheck(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if state.enableInterPodAffinityHostnameFastPath {
		if state.matchingHostScopedAffinityPodsCount <= 0 && podMatchesAllAffinityTerms(state.podInfo.GetRequiredAffinityTerms(), state.podInfo.GetPod()) {
			for _, term := range state.podInfo.GetRequiredAffinityTerms() {
				if _, ok := nodeInfo.Node().Labels[term.TopologyKey]; !ok {
					return false
				}
			}
			return true
		}
		return false
	}

	// 特性关闭时，拓扑已在 nodeTopologyMatchesAffinity 中完整评估；
	// 调用方会检查 len(state.clusterWideAffinityCounts) == 0。
	if podMatchesAllAffinityTerms(state.podInfo.GetRequiredAffinityTerms(), state.podInfo.GetPod()) {
		return true
	}
	return false
}

// Filter 在过滤扩展点判断 Pod 能否调度到指定节点。
// 借助 PreFilter 预计算的映射做 O(1) 拓扑检查，主机域项则在节点本地动态评估，避免遍历全集群。
//
// 关闭 InterPodAffinityHostnameFastPath 时：
//  1. 亲和：用 clusterWideAffinityCounts 检查节点拓扑域中的匹配 Pod；
//     全局无匹配且 Pod 满足自亲和时允许调度。
//  2. 待调度 Pod 的反亲和：用 clusterWideAntiAffinityCounts 检查拓扑冲突。
//  3. 已有 Pod 的反亲和：用 existingClusterWideAntiAffinityCounts 检查是否拒绝该 Pod。
//
// 开启快速路径时，全局拓扑检查与节点本地 Pod 检查彻底分离：
//  1. 亲和：纯主机域项时动态遍历节点 Pod 找匹配；全集群无匹配且满足自亲和则允许调度；
//     否则用 clusterWideAffinityCounts 检查拓扑匹配。
//  2. 待调度 Pod 的反亲和：非主机域项查全局计数，主机域项本地动态检查。
//  3. 已有 Pod 的反亲和：全局计数检查拓扑拒绝，并本地遍历有主机域反亲和的节点 Pod。
func (pl *InterPodAffinity) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}

	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	if state.enableInterPodAffinityHostnameFastPath {
		if len(state.hostScopedAffinityTerms) > 0 {
			if !nodePodsSatisfyAffinity(state, nodeInfo) {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonAffinityRulesNotMatch)
			}
		} else if len(state.clusterWideAffinityTerms) > 0 {
			if !nodeTopologyMatchesAffinity(state, nodeInfo) {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonAffinityRulesNotMatch)
			}
		}
	} else {
		if !nodeTopologyMatchesAffinity(state, nodeInfo) {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonAffinityRulesNotMatch)
		}
	}

	if state.enableInterPodAffinityHostnameFastPath {
		if nodeTopologyConflictsWithAntiAffinity(state, nodeInfo) || nodePodsConflictWithAntiAffinity(state, nodeInfo) {
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonAntiAffinityRulesNotMatch)
		}
	} else {
		if !satisfyPodAntiAffinity(state, nodeInfo) {
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonAntiAffinityRulesNotMatch)
		}
	}

	if !satisfyExistingPodsAntiAffinity(state, nodeInfo) {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonExistingAntiAffinityRulesNotMatch)
	}

	return nil
}
