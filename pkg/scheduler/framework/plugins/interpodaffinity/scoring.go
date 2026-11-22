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
	"math"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// preScoreStateKey 是 CycleState 中存放 InterPodAffinity 打分预计算数据的键。
const preScoreStateKey = "PreScore" + Name

// scoreMap 按拓扑键、拓扑值累计亲和性得分。
type scoreMap map[string]map[string]int64

// preScoreState 在 PreScore 阶段计算，供 Score 阶段使用。
type preScoreState struct {
	topologyScore scoreMap
	podInfo       fwk.PodInfo
	// 复制待调度 Pod 的命名空间标签，避免遍历期间因外部变更看到不一致的数据。
	namespaceLabels labels.Set
}

// Clone 实现必需的 Clone 接口。本状态在读取后不再修改，因此直接复用同一实例即可。
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// processTerm 若 Pod 匹配该亲和性条目，就在条目拓扑键对应的拓扑值上累加权重。
// multiplier 为 1 表示亲和，-1 表示反亲和。
func (m scoreMap) processTerm(term *fwk.AffinityTerm, weight int32, pod *v1.Pod, nsLabels labels.Set, node *v1.Node, multiplier int32) {
	if term.Matches(pod, nsLabels) {
		if tpValue, tpValueExist := node.Labels[term.TopologyKey]; tpValueExist {
			if m[term.TopologyKey] == nil {
				m[term.TopologyKey] = make(map[string]int64)
			}
			m[term.TopologyKey][tpValue] += int64(weight * multiplier)
		}
	}
}

// processTerms 逐个处理带权重的亲和性条目。
func (m scoreMap) processTerms(terms []fwk.WeightedAffinityTerm, pod *v1.Pod, nsLabels labels.Set, node *v1.Node, multiplier int32) {
	for _, term := range terms {
		m.processTerm(&term.AffinityTerm, term.Weight, pod, nsLabels, node, multiplier)
	}
}

// append 把另一张得分表按拓扑键与值合并进当前表。
func (m scoreMap) append(other scoreMap) {
	for topology, oScores := range other {
		scores := m[topology]
		if scores == nil {
			m[topology] = oScores
			continue
		}
		for k, v := range oScores {
			scores[k] += v
		}
	}
}

// processExistingPod 汇总一个已存在 Pod 对待调度 Pod 的双向亲和性/反亲和性得分。
func (pl *InterPodAffinity) processExistingPod(
	state *preScoreState,
	existingPod fwk.PodInfo,
	existingPodNodeInfo fwk.NodeInfo,
	incomingPod *v1.Pod,
	topoScore scoreMap,
) {
	existingPodNode := existingPodNodeInfo.Node()
	if len(existingPodNode.Labels) == 0 {
		return
	}

	// 待调度 Pod 的软亲和条目若匹配已存在 Pod，就在该 Pod 所在节点的拓扑值上加对应权重。
	// 待调度 Pod 的条目已把 namespaceSelector 合并进 namespaces，因此这里无需查命名空间标签。
	topoScore.processTerms(state.podInfo.GetPreferredAffinityTerms(), existingPod.GetPod(), nil, existingPodNode, 1)

	// 软反亲和同理，权重取负值表示惩罚。
	// 条目已合并 namespaceSelector，这里同样不需要命名空间标签。
	topoScore.processTerms(state.podInfo.GetPreferredAntiAffinityTerms(), existingPod.GetPod(), nil, existingPodNode, -1)

	// 已存在 Pod 的硬亲和条目若匹配待调度 Pod，按配置的固定权重加分。
	if pl.args.HardPodAffinityWeight > 0 && len(existingPodNode.Labels) != 0 {
		for _, t := range existingPod.GetRequiredAffinityTerms() {
			topoScore.processTerm(&t, pl.args.HardPodAffinityWeight, incomingPod, state.namespaceLabels, existingPodNode, 1)
		}
	}

	// 反向处理：已存在 Pod 的软亲和条目若匹配待调度 Pod，按其权重加分。
	topoScore.processTerms(existingPod.GetPreferredAffinityTerms(), incomingPod, state.namespaceLabels, existingPodNode, 1)

	// 反向的软反亲和条目同样按负权重扣分。
	topoScore.processTerms(existingPod.GetPreferredAntiAffinityTerms(), incomingPod, state.namespaceLabels, existingPodNode, -1)
}

// PreScore 构建并写入供 Score 与 NormalizeScore 使用的 CycleState。
func (pl *InterPodAffinity) PreScore(
	pCtx context.Context,
	cycleState fwk.CycleState,
	pod *v1.Pod,
	nodes []fwk.NodeInfo,
) *fwk.Status {

	if pl.sharedLister == nil {
		return fwk.NewStatus(fwk.Error, "empty shared lister in InterPodAffinity PreScore")
	}

	affinity := pod.Spec.Affinity
	hasPreferredAffinityConstraints := affinity != nil && affinity.PodAffinity != nil && len(affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0
	hasPreferredAntiAffinityConstraints := affinity != nil && affinity.PodAntiAffinity != nil && len(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0
	hasConstraints := hasPreferredAffinityConstraints || hasPreferredAntiAffinityConstraints

	// 待调度 Pod 自身没有亲和性约束时，可选择跳过对现有 Pod 偏好的计算。
	if pl.args.IgnorePreferredTermsOfExistingPods && !hasConstraints {
		return fwk.NewStatus(fwk.Skip)
	}

	// 待调度 Pod 没有软偏好时，只需处理承载了带亲和性 Pod 的节点，缩小扫描范围。
	var allNodes []fwk.NodeInfo
	var err error
	if hasConstraints {
		allNodes, err = pl.sharedLister.NodeInfos().List()
		if err != nil {
			return fwk.AsStatus(fmt.Errorf("failed to get all nodes from shared lister: %w", err))
		}
	} else {
		allNodes, err = pl.sharedLister.NodeInfos().HavePodsWithAffinityList()
		if err != nil {
			return fwk.AsStatus(fmt.Errorf("failed to get pods with affinity list: %w", err))
		}
	}

	state := &preScoreState{
		topologyScore: make(map[string]map[string]int64),
	}

	if state.podInfo, err = framework.NewPodInfo(pod); err != nil {
		// 理论上不会走到这里，PreFilter 已拦截这类错误。
		return fwk.AsStatus(fmt.Errorf("failed to parse pod: %w", err))
	}

	preferredAffinityTerms := state.podInfo.GetPreferredAffinityTerms()
	for i := range preferredAffinityTerms {
		if err := pl.mergeAffinityTermNamespacesIfNotEmpty(&preferredAffinityTerms[i].AffinityTerm); err != nil {
			return fwk.AsStatus(fmt.Errorf("updating PreferredAffinityTerms: %w", err))
		}
	}
	preferredAntiAffinityTerms := state.podInfo.GetPreferredAntiAffinityTerms()
	for i := range preferredAntiAffinityTerms {
		if err := pl.mergeAffinityTermNamespacesIfNotEmpty(&preferredAntiAffinityTerms[i].AffinityTerm); err != nil {
			return fwk.AsStatus(fmt.Errorf("updating PreferredAntiAffinityTerms: %w", err))
		}
	}
	logger := klog.FromContext(pCtx)
	state.namespaceLabels = GetNamespaceLabelsSnapshot(logger, pod.Namespace, pl.nsLister)

	topoScores := make([]scoreMap, len(allNodes))
	index := int32(-1)
	processNode := func(i int) {
		nodeInfo := allNodes[i]

		// 待调度 Pod 有软偏好时，任何现有 Pod 都可能匹配其条目，必须全量处理。
		podsToProcess := nodeInfo.GetPodsWithAffinity()
		if hasConstraints {
			podsToProcess = nodeInfo.GetPods()
		}

		topoScore := make(scoreMap)
		for _, existingPod := range podsToProcess {
			pl.processExistingPod(state, existingPod, nodeInfo, pod, topoScore)
		}
		if len(topoScore) > 0 {
			topoScores[atomic.AddInt32(&index, 1)] = topoScore
		}
	}
	pl.parallelizer.Until(pCtx, len(allNodes), processNode, pl.Name())

	if index == -1 {
		return fwk.NewStatus(fwk.Skip)
	}

	for i := 0; i <= int(index); i++ {
		state.topologyScore.append(topoScores[i])
	}

	cycleState.Write(preScoreStateKey, state)
	return nil
}

// getPreScoreState 从 CycleState 中读取 PreScore 阶段写入的状态。
func getPreScoreState(cycleState fwk.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to interpodaffinity.preScoreState error", c)
	}
	return s, nil
}

// Score 在 Score 扩展点调用，返回节点标签匹配的所有拓扑值上的权重之和。
// 该分数会在 NormalizeScore 中归一化；亲和性为正分，反亲和性为负分。
func (pl *InterPodAffinity) Score(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	node := nodeInfo.Node()

	s, err := getPreScoreState(cycleState)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}
	var score int64
	for tpKey, tpValues := range s.topologyScore {
		if v, exist := node.Labels[tpKey]; exist {
			score += tpValues[v]
		}
	}

	return score, nil
}

// NormalizeScore 将各节点的原始分数线性归一化到 [0, MaxNodeScore]。
func (pl *InterPodAffinity) NormalizeScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	s, err := getPreScoreState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if len(s.topologyScore) == 0 {
		return nil
	}

	var minCount int64 = math.MaxInt64
	var maxCount int64 = math.MinInt64
	for i := range scores {
		score := scores[i].Score
		if score > maxCount {
			maxCount = score
		}
		if score < minCount {
			minCount = score
		}
	}

	maxMinDiff := maxCount - minCount
	for i := range scores {
		fScore := float64(0)
		if maxMinDiff > 0 {
			fScore = float64(fwk.MaxNodeScore) * (float64(scores[i].Score-minCount) / float64(maxMinDiff))
		}

		scores[i].Score = int64(fScore)
	}

	return nil
}

// ScoreExtensions 返回该 Score 插件的扩展接口实现。
func (pl *InterPodAffinity) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}
