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
	"maps"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

const preFilterStateKey = "PreFilter" + Name

// preFilterState 在 PreFilter 阶段计算，在 Filter 阶段使用。
// 它用 CriticalPaths 和 TpValueToMatchNum 两部分表示：
// (1) 每条 spread 约束上匹配 Pod 数最少的关键路径；
// (2) 每条 spread 约束上各拓扑值匹配的 Pod 数。
// nil 的 preFilterState 表示尚未设置（即 PreFilter 未被调用）；
// 空的 preFilterState 对象是合法状态，表示 PreFilter 已调用但没有约束。
// 字段导出仅为便于测试中直接比较。
type preFilterState struct {
	Constraints []topologySpreadConstraint
	// CriticalPaths 按约束下标索引。
	// 每项只记录 2 条关键路径而非全部，因为 Filter 只需要全局最小值。
	// CriticalPaths[i][0].MatchNum 始终是最小匹配数。
	// CriticalPaths[i][1].MatchNum 始终大于等于 CriticalPaths[i][0].MatchNum，
	// 但不保证是第二小的匹配数。
	CriticalPaths []*criticalPaths
	// TpValueToMatchNum 按约束下标索引。
	// 每个 map 以拓扑值为键、匹配 Pod 数为值。
	TpValueToMatchNum []map[string]int
}

// minMatchNum 返回用于计算倾斜度的全局最小值，并将 MinDomains 纳入考量。
func (s *preFilterState) minMatchNum(constraintID int, minDomains int32) (int, error) {
	paths := s.CriticalPaths[constraintID]

	minMatchNum := paths[0].MatchNum
	domainsNum := len(s.TpValueToMatchNum[constraintID])

	if domainsNum < int(minDomains) {
		// 具有匹配拓扑键的合格域少于 minDomains 时，跳过该约束，
		// 因此把全局最小值当作 0，避免误判为倾斜超标。
		minMatchNum = 0
	}

	return minMatchNum, nil
}

// Clone 复制给定状态，避免抢占过程修改到其他节点的计算基准。
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}
	copy := preFilterState{
		// Constraints 只读且不可变，直接共享即可。
		Constraints:       s.Constraints,
		CriticalPaths:     make([]*criticalPaths, len(s.CriticalPaths)),
		TpValueToMatchNum: make([]map[string]int, len(s.TpValueToMatchNum)),
	}
	for i, paths := range s.CriticalPaths {
		copy.CriticalPaths[i] = &criticalPaths{paths[0], paths[1]}
	}
	for i, tpMap := range s.TpValueToMatchNum {
		// 拓扑计数随 Pod 的增删而变化，必须逐 map 复制。
		copy.TpValueToMatchNum[i] = maps.Clone(tpMap)
	}
	return &copy
}

// 注意：`[2]criticalPath` 之所以够用，依赖当前抢占算法的实现，具体有两点前提：
// 事实 1：抢占只发生在同一个节点上，而非跨多个节点抢占 Pod。
// 事实 2：每个节点在自己的抢占周期里使用独立的 preFilterState 副本进行评估。
// 若将来改为“跨任意节点的任意 Pod”这类更复杂的算法，需要重新审视该结构。
// 字段导出仅为便于测试中直接比较。
type criticalPaths [2]struct {
	// TopologyValue 是条目对应的拓扑值。
	TopologyValue string
	// MatchNum 是匹配的 Pod 数。
	MatchNum int
}

// newCriticalPaths 创建初始状态，两个槽位都用 math.MaxInt32 占位，
// 保证任何真实匹配数都能进入前两名。
func newCriticalPaths() *criticalPaths {
	return &criticalPaths{{MatchNum: math.MaxInt32}, {MatchNum: math.MaxInt32}}
}

// update 用拓扑值 tpVal 的新匹配数 num 维护最小的两个槽位，
// 使 paths[0] 始终是最小值——Filter 判倾斜只需要这个全局最小值。
func (p *criticalPaths) update(tpVal string, num int) {
	// 先确认 tpVal 是否已记录，避免把同一拓扑值占满两个槽位。
	i := -1
	if tpVal == p[0].TopologyValue {
		i = 0
	} else if tpVal == p[1].TopologyValue {
		i = 1
	}

	if i >= 0 {
		// tpVal 已存在：更新计数后可能需要交换，维持 paths[0] 最小。
		p[i].MatchNum = num
		if p[0].MatchNum > p[1].MatchNum {
			// 交换 paths[0] 与 paths[1]，始终把最小值放在 paths[0]。
			p[0], p[1] = p[1], p[0]
		}
	} else {
		// tpVal 不存在：只有比现有最小值更小或介于两者之间才值得记录。
		if num < p[0].MatchNum {
			// 原最小值降级为第二小，新值占据 paths[0]。
			p[1] = p[0]
			// 更新 paths[0]。
			p[0].TopologyValue, p[0].MatchNum = tpVal, num
		} else if num < p[1].MatchNum {
			// 新值不足以成为最小值，但小于第二小值，更新 paths[1]。
			p[1].TopologyValue, p[1].MatchNum = tpVal, num
		}
	}
}

// PreFilter 在 prefilter 扩展点调用，预计算 Filter 所需的 spread 状态。
func (pl *PodTopologySpread) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// 原地垂直扩缩容且扩容被延迟的 Pod 不参与 spread 计算。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	s, err := pl.calPreFilterState(ctx, pod, nodes)
	if err != nil {
		return nil, fwk.AsStatus(err)
	} else if s != nil && len(s.Constraints) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}

	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}

// PreFilterExtensions 返回 prefilter 扩展，即 Pod 增删时同步更新状态的钩子。
func (pl *PodTopologySpread) PreFilterExtensions() fwk.PreFilterExtensions {
	return pl
}

// AddPod 根据 cycleState 中的预计算数据，模拟加入一个 Pod 后的状态变化。
func (pl *PodTopologySpread) AddPod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)

	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	// delta=1 表示增加一个匹配 Pod，并同步刷新关键路径。
	pl.updateWithPod(logger, s, podInfoToAdd.GetPod(), podToSchedule, nodeInfo.Node(), 1)
	return nil
}

// RemovePod 根据 cycleState 中的预计算数据，模拟移除一个 Pod 后的状态变化。
func (pl *PodTopologySpread) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	// delta=-1 表示移除一个匹配 Pod；抢占模拟依赖此增量更新。
	pl.updateWithPod(logger, s, podInfoToRemove.GetPod(), podToSchedule, nodeInfo.Node(), -1)
	return nil
}

// updateWithPod 按 delta 增减 Pod 对拓扑计数的贡献，并刷新对应约束的关键路径。
// 命名空间不同或节点不匹配 spread 约束时无需处理。
func (pl *PodTopologySpread) updateWithPod(logger klog.Logger, s *preFilterState, updatedPod, preemptorPod *v1.Pod, node *v1.Node, delta int) {
	if s == nil || updatedPod.Namespace != preemptorPod.Namespace || node == nil {
		return
	}
	if !nodeLabelsMatchSpreadConstraints(node.Labels, s.Constraints) {
		return
	}

	requiredSchedulingTerm := nodeaffinity.GetRequiredNodeAffinity(preemptorPod)
	if !pl.enableNodeInclusionPolicyInPodTopologySpread {
		// 仅对通过节点过滤的节点应用 spread。
		// 为保持向后兼容，忽略解析错误。
		if match, _ := requiredSchedulingTerm.Match(node); !match {
			return
		}
	}

	podLabelSet := labels.Set(updatedPod.Labels)
	for i, constraint := range s.Constraints {
		if !constraint.Selector.Matches(podLabelSet) {
			// Pod 不匹配选择器，不计入该约束。
			continue
		}

		if pl.enableNodeInclusionPolicyInPodTopologySpread &&
			!constraint.matchNodeInclusionPolicies(logger, preemptorPod, node, requiredSchedulingTerm, pl.enableTaintTolerationComparisonOperators) {
			continue
		}

		v := node.Labels[constraint.TopologyKey]
		// 增量更新拓扑计数，再用新计数刷新关键路径，供后续 Filter 复算倾斜度。
		s.TpValueToMatchNum[i][v] += delta
		s.CriticalPaths[i].update(v, s.TpValueToMatchNum[i][v])
	}
}

// getPreFilterState 从 cycleState 中取出预计算好的 preFilterState。
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState 不存在，说明 PreFilter 可能未被调用。
		return nil, fmt.Errorf("reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to podtopologyspread.preFilterState error", c)
	}
	return s, nil
}

// topologyCount 记录单个节点上某条约束在某个拓扑值下的匹配 Pod 数。
type topologyCount struct {
	topologyValue string
	constraintID  int
	count         int
}

// calPreFilterState 计算 preFilterState，描述 Pod 在各拓扑上的分布情况。
func (pl *PodTopologySpread) calPreFilterState(ctx context.Context, pod *v1.Pod, allNodes []fwk.NodeInfo) (*preFilterState, error) {
	constraints, err := pl.getConstraints(pod)
	if err != nil {
		return nil, fmt.Errorf("get constraints from pod: %w", err)
	}
	if len(constraints) == 0 {
		return &preFilterState{}, nil
	}

	logger := klog.FromContext(ctx)
	s := preFilterState{
		Constraints:       constraints,
		CriticalPaths:     make([]*criticalPaths, len(constraints)),
		TpValueToMatchNum: make([]map[string]int, len(constraints)),
	}
	for i := 0; i < len(constraints); i++ {
		s.TpValueToMatchNum[i] = make(map[string]int, sizeHeuristic(len(allNodes), constraints[i]))
	}

	// 先按节点并行统计，再串行汇总，避免多 goroutine 写同一个 map。
	tpCountsByNode := make([][]topologyCount, len(allNodes))
	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	processNode := func(n int) {
		nodeInfo := allNodes[n]
		node := nodeInfo.Node()

		if !pl.enableNodeInclusionPolicyInPodTopologySpread {
			// 仅对通过节点过滤的节点应用 spread。
			// 为保持向后兼容，忽略解析错误。
			if match, _ := requiredNodeAffinity.Match(node); !match {
				return
			}
		}

		// 确保当前节点标签包含所有约束的 topologyKey，否则无法统计对应域。
		if !nodeLabelsMatchSpreadConstraints(node.Labels, constraints) {
			return
		}

		tpCounts := make([]topologyCount, 0, len(constraints))
		for i, c := range constraints {
			if pl.enableNodeInclusionPolicyInPodTopologySpread &&
				!c.matchNodeInclusionPolicies(logger, pod, node, requiredNodeAffinity, pl.enableTaintTolerationComparisonOperators) {
				continue
			}

			value := node.Labels[c.TopologyKey]
			count := countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace)
			tpCounts = append(tpCounts, topologyCount{
				topologyValue: value,
				constraintID:  i,
				count:         count,
			})
		}
		tpCountsByNode[n] = tpCounts
	}
	pl.parallelizer.Until(ctx, len(allNodes), processNode, pl.Name())

	for _, tpCounts := range tpCountsByNode {
		// tpCounts 可能不含全部约束（部分被跳过），因此必须用 constraintID 而非切片下标。
		for _, tpCount := range tpCounts {
			s.TpValueToMatchNum[tpCount.constraintID][tpCount.topologyValue] += tpCount.count
		}
	}

	// 汇总后为每条约束计算最小匹配数，供 Filter 计算倾斜度。
	for i := 0; i < len(constraints); i++ {
		s.CriticalPaths[i] = newCriticalPaths()

		for value, num := range s.TpValueToMatchNum[i] {
			s.CriticalPaths[i].update(value, num)
		}
	}

	return &s, nil
}

// Filter 在 filter 扩展点调用，判断节点是否满足 Pod 的拓扑分布约束。
func (pl *PodTopologySpread) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// 原地垂直扩缩容且扩容被延迟的 Pod 不受 spread 约束限制。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	node := nodeInfo.Node()

	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	// 注意：空的 preFilterState 是合法状态，表示所有待调度 Pod 都被允许。
	if len(s.Constraints) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)
	podLabelSet := labels.Set(pod.Labels)
	for i, c := range s.Constraints {
		tpKey := c.TopologyKey
		tpVal, ok := node.Labels[tpKey]
		if !ok {
			logger.V(5).Info("Node doesn't have required topology label for spread constraint", "node", klog.KObj(node), "topologyKey", tpKey)
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeLabelNotMatch)
		}

		// 判定标准：
		// '现有匹配数' + '自身是否匹配（1 或 0）' - '全局最小值' <= 'maxSkew'。
		minMatchNum, err := s.minMatchNum(i, c.MinDomains)
		if err != nil {
			logger.Error(err, "Internal error occurred while retrieving value precalculated in PreFilter", "topologyKey", tpKey, "paths", s.CriticalPaths[i])
			continue
		}

		selfMatchNum := 0
		// Pod 自身若匹配选择器，落在该拓扑值上会额外贡献 1 个匹配。
		if c.Selector.Matches(podLabelSet) {
			selfMatchNum = 1
		}

		matchNum := s.TpValueToMatchNum[i][tpVal]
		// 倾斜度超过 maxSkew 即拒绝，防止把 Pod 堆到已经过载的拓扑域。
		skew := matchNum + selfMatchNum - minMatchNum
		if skew > int(c.MaxSkew) {
			logger.V(5).Info("Node failed spreadConstraint: matchNum + selfMatchNum - minMatchNum > maxSkew", "node", klog.KObj(node), "topologyKey", tpKey, "matchNum", matchNum, "selfMatchNum", selfMatchNum, "minMatchNum", minMatchNum, "maxSkew", c.MaxSkew)
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonConstraintsNotMatch)
		}
	}

	return nil
}

// sizeHeuristic 为预分配 TpValueToMatchNum 提供容量提示。
// hostname 维度每个节点的拓扑值唯一，按节点数预分配可减少扩容开销。
func sizeHeuristic(nodes int, constraint topologySpreadConstraint) int {
	if constraint.TopologyKey == v1.LabelHostname {
		return nodes
	}
	return 0
}
