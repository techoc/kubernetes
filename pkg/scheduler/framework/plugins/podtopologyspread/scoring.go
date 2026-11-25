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

package podtopologyspread

import (
	"context"
	"fmt"
	"k8s.io/klog/v2"
	"math"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	fwk "k8s.io/kube-scheduler/framework"
)

const preScoreStateKey = "PreScore" + Name
const invalidScore = -1

// preScoreState 在 PreScore 阶段计算，在 Score/NormalizeScore 阶段使用。
// 字段首字母大写是为了测试中方便比较。
type preScoreState struct {
	Constraints []topologySpreadConstraint
	// IgnoredNodes 是缺少某些约束中 topologyKey 的节点名集合，这些节点不参与打分。
	IgnoredNodes sets.Set[string]
	// TopologyValueToPodCounts 按约束下标索引，每个 map 以拓扑值为键、匹配 Pod 数为值。
	TopologyValueToPodCounts []map[string]*int64
	// TopologyNormalizingWeight 是每个拓扑计数的权重。
	// 它让小规模拓扑中的 Pod 数不被大规模拓扑稀释。
	TopologyNormalizingWeight []float64
}

// Clone 实现必需的 Clone 接口。该状态不会被修改，因此无需真正复制数据。
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// initPreScoreState 遍历 filteredNodes，剔除缺少所需 topologyKey 的节点，并初始化：
// 1) s.TopologyValueToPodCounts：按拓扑值统计匹配 Pod 数的计数器；
// 2) s.IgnoredNodes：不参与打分的节点集合；
// 3) s.TopologyNormalizingWeight：依据每个拓扑的值数量计算各约束的归一化权重。
func (pl *PodTopologySpread) initPreScoreState(s *preScoreState, pod *v1.Pod, filteredNodes []fwk.NodeInfo, requireAllTopologies bool) error {
	var err error
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		s.Constraints, err = pl.filterTopologySpreadConstraints(
			pod.Spec.TopologySpreadConstraints,
			pod.Labels,
			v1.ScheduleAnyway,
		)
		if err != nil {
			return fmt.Errorf("obtaining pod's soft topology spread constraints: %w", err)
		}
	} else {
		s.Constraints, err = pl.buildDefaultConstraints(pod, v1.ScheduleAnyway)
		if err != nil {
			return fmt.Errorf("setting default soft topology spread constraints: %w", err)
		}
	}
	if len(s.Constraints) == 0 {
		return nil
	}
	s.TopologyValueToPodCounts = make([]map[string]*int64, len(s.Constraints))
	for i := 0; i < len(s.Constraints); i++ {
		s.TopologyValueToPodCounts[i] = make(map[string]*int64)
	}
	topoSize := make([]int, len(s.Constraints))
	for _, node := range filteredNodes {
		if requireAllTopologies && !nodeLabelsMatchSpreadConstraints(node.Node().Labels, s.Constraints) {
			// 缺少必需拓扑标签的节点在后续打分时直接忽略。
			s.IgnoredNodes.Insert(node.Node().Name)
			continue
		}
		for i, constraint := range s.Constraints {
			// hostname 维度的计数在 Score 阶段按当前节点计算，这里跳过。
			if constraint.TopologyKey == v1.LabelHostname {
				continue
			}
			value := node.Node().Labels[constraint.TopologyKey]
			if s.TopologyValueToPodCounts[i][value] == nil {
				s.TopologyValueToPodCounts[i][value] = new(int64)
				topoSize[i]++
			}
		}
	}

	s.TopologyNormalizingWeight = make([]float64, len(s.Constraints))
	for i, c := range s.Constraints {
		sz := topoSize[i]
		if c.TopologyKey == v1.LabelHostname {
			sz = len(filteredNodes) - len(s.IgnoredNodes)
		}
		s.TopologyNormalizingWeight[i] = topologyNormalizingWeight(sz)
	}
	return nil
}

// PreScore 构建并写入供 Score 和 NormalizeScore 使用的周期状态。
func (pl *PodTopologySpread) PreScore(
	ctx context.Context,
	cycleState fwk.CycleState,
	pod *v1.Pod,
	filteredNodes []fwk.NodeInfo,
) *fwk.Status {

	allNodes, err := pl.sharedLister.NodeInfos().List()
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("getting all nodes: %w", err))
	}

	if len(allNodes) == 0 {
		// 没有节点可打分，直接跳过。
		return fwk.NewStatus(fwk.Skip)
	}

	logger := klog.FromContext(ctx)
	state := &preScoreState{
		IgnoredNodes: sets.New[string](),
	}
	// 只有显式配置约束或非系统默认规则时才要求节点具备全部拓扑标签。
	// 这样缺少 zone 标签的节点仍可参与 hostname 维度的打散。
	requireAllTopologies := len(pod.Spec.TopologySpreadConstraints) > 0 || !pl.systemDefaulted
	err = pl.initPreScoreState(state, pod, filteredNodes, requireAllTopologies)
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("calculating preScoreState: %w", err))
	}

	// 新 Pod 没有软打散约束时无需打分，跳过。
	if len(state.Constraints) == 0 {
		return fwk.NewStatus(fwk.Skip)
	}

	// 为兼容旧行为，忽略解析错误。
	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	processAllNode := func(n int) {
		nodeInfo := allNodes[n]
		node := nodeInfo.Node()

		if !pl.enableNodeInclusionPolicyInPodTopologySpread {
			// 旧逻辑要求节点必须满足 Pod 的 NodeSelector/NodeAffinity 才计入。
			if match, _ := requiredNodeAffinity.Match(node); !match {
				return
			}
		}

		// 需要时，节点必须具备所有拓扑标签才参与统计。
		if requireAllTopologies && !nodeLabelsMatchSpreadConstraints(node.Labels, state.Constraints) {
			return
		}

		for i, c := range state.Constraints {
			if pl.enableNodeInclusionPolicyInPodTopologySpread &&
				!c.matchNodeInclusionPolicies(logger, pod, node, requiredNodeAffinity,
					pl.enableTaintTolerationComparisonOperators) {
				continue
			}

			value := node.Labels[c.TopologyKey]
			// 该拓扑值不在候选节点集合中时无需统计。
			// hostname 维度的每节点计数同样留到 Score 阶段处理。
			tpCount := state.TopologyValueToPodCounts[i][value]
			if tpCount == nil {
				continue
			}
			count := countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace)
			atomic.AddInt64(tpCount, int64(count))
		}
	}
	pl.parallelizer.Until(ctx, len(allNodes), processAllNode, pl.Name())

	cycleState.Write(preScoreStateKey, state)
	return nil
}

// Score 在 Score 扩展点被调用。
// 这里返回的分数是各拓扑域匹配 Pod 数的合计，稍后由 NormalizeScore 归一化。
func (pl *PodTopologySpread) Score(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	node := nodeInfo.Node()
	s, err := getPreScoreState(cycleState)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}

	// 被忽略的节点不参与打分，返回 0。
	if s.IgnoredNodes.Has(node.Name) {
		return 0, nil
	}

	// 对每个存在的拓扑值，当前节点获得对应的匹配 Pod 数作为得分贡献，最后求和。
	var score float64
	for i, c := range s.Constraints {
		if tpVal, ok := node.Labels[c.TopologyKey]; ok {
			var cnt int64
			if c.TopologyKey == v1.LabelHostname {
				cnt = int64(countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace))
			} else {
				cnt = *s.TopologyValueToPodCounts[i][tpVal]
			}
			score += scoreForCount(cnt, c.MaxSkew, s.TopologyNormalizingWeight[i])
		}
	}
	return int64(math.Round(score)), nil
}

// NormalizeScore 在所有节点打分后调用，把原始分数线性映射到 [0, MaxNodeScore]。
// 分数越低表示节点上的 Pod 分布越均衡，因此映射时做反向处理。
func (pl *PodTopologySpread) NormalizeScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	s, err := getPreScoreState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s == nil {
		return nil
	}

	// 先求有效节点分数的最小值和最大值，同时把被忽略节点标记为 invalidScore。
	var minScore int64 = math.MaxInt64
	var maxScore int64
	for i, score := range scores {
		// 必须先排除被忽略节点，否则其中的 0 分会污染最小值。
		if s.IgnoredNodes.Has(score.Name) {
			scores[i].Score = invalidScore
			continue
		}
		if score.Score < minScore {
			minScore = score.Score
		}
		if score.Score > maxScore {
			maxScore = score.Score
		}
	}

	for i := range scores {
		if scores[i].Score == invalidScore {
			scores[i].Score = 0
			continue
		}
		// 全部为 0 分说明分布完全均衡，给所有节点满分。
		if maxScore == 0 {
			scores[i].Score = fwk.MaxNodeScore
			continue
		}
		s := scores[i].Score
		scores[i].Score = fwk.MaxNodeScore * (maxScore + minScore - s) / maxScore
	}
	return nil
}

// ScoreExtensions 返回 Score 插件自身的扩展接口实现。
func (pl *PodTopologySpread) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

// getPreScoreState 从 CycleState 读取 PreScore 阶段写入的打散状态。
func getPreScoreState(cycleState fwk.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to podtopologyspread.preScoreState error", c)
	}
	return s, nil
}

// topologyNormalizingWeight 根据拓扑包含的值数量计算归一化权重。
// 通过 Filter 的节点至少构成 1 个值，k8s 最多支持约 5k 节点，因此结果落在 [1.09, 8.52]。
//
// 注意：当没有节点具备所需拓扑时 size 可能为 0，但此时所有节点都得 0 分，权重无关紧要。
func topologyNormalizingWeight(size int) float64 {
	return math.Log(float64(size + 2))
}

// scoreForCount 根据拓扑域内匹配 Pod 数、约束的 maxSkew 和拓扑权重计算分数。
// 加上 `maxSkew-1` 是为了缩小各拓扑域之间的差异，控制评分对倾斜的敏感度。
func scoreForCount(cnt int64, maxSkew int32, tpWeight float64) float64 {
	return float64(cnt)*tpWeight + float64(maxSkew-1)
}
