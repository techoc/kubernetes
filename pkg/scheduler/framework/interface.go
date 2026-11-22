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

// 本文件定义调度框架（scheduling framework）的插件接口。

package framework

import (
	"context"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

// NodeToStatus 记录传入 Pod 在哪些节点上不可调度，以及对应的状态。
type NodeToStatus struct {
	// nodeToStatus 保存各节点的具体状态。
	nodeToStatus map[string]*fwk.Status
	// absentNodesStatus 是 nodeToStatus 中未出现的所有节点的统一状态。
	// 默认为 UnschedulableAndUnresolvable。
	absentNodesStatus *fwk.Status
}

// NewDefaultNodeToStatus 创建空的 NodeToStatus。
// absentNodesStatus 默认为 UnschedulableAndUnresolvable。
func NewDefaultNodeToStatus() *NodeToStatus {
	return NewNodeToStatus(make(map[string]*fwk.Status), fwk.NewStatus(fwk.UnschedulableAndUnresolvable))
}

// NewNodeToStatus 用给定的 nodeToStatus 和 absentNodesStatus 创建 NodeToStatus。
func NewNodeToStatus(nodeToStatus map[string]*fwk.Status, absentNodesStatus *fwk.Status) *NodeToStatus {
	return &NodeToStatus{
		nodeToStatus:      nodeToStatus,
		absentNodesStatus: absentNodesStatus,
	}
}

// Get 返回指定节点的状态；节点不在 map 中时返回 absentNodesStatus。
func (m *NodeToStatus) Get(nodeName string) *fwk.Status {
	if status, ok := m.nodeToStatus[nodeName]; ok {
		return status
	}
	return m.absentNodesStatus
}

// Set 设置指定节点的状态。
func (m *NodeToStatus) Set(nodeName string, status *fwk.Status) {
	m.nodeToStatus[nodeName] = status
}

// Len 返回 nodeToStatus map 的长度，不包含未显式记录的节点。
func (m *NodeToStatus) Len() int {
	return len(m.nodeToStatus)
}

// AbsentNodesStatus 返回 absentNodesStatus 的值。
func (m *NodeToStatus) AbsentNodesStatus() *fwk.Status {
	return m.absentNodesStatus
}

// SetAbsentNodesStatus 设置 absentNodesStatus 的值。
func (m *NodeToStatus) SetAbsentNodesStatus(status *fwk.Status) {
	m.absentNodesStatus = status
}

// ForEachExplicitNode 对每个显式设置过状态的节点调用 fn。
// 注意：它只对注册过状态的节点调用，不一定覆盖所有现存节点。
// 例如 PreFilter 拒绝所有节点时，调度器不会给每个节点都设置失败状态，
// 而是把失败状态统一放到 AbsentNodesStatus 中。
// 使用方应通过 AbsentNodesStatus() 取回该状态，并把其余节点视为被它拒绝。
func (m *NodeToStatus) ForEachExplicitNode(fn func(nodeName string, status *fwk.Status)) {
	for nodeName, status := range m.nodeToStatus {
		fn(nodeName, status)
	}
}

// NodesForStatusCode 返回状态匹配给定 code 的节点的 NodeInfo 列表。
// 如果 absentNodesStatus 匹配该 code，则通过 nodeLister 取出所有节点，再用 Get 过滤；
// 如果不匹配，则只遍历 nodeToStatus 中的节点，并逐个用 nodeLister.Get 取 NodeInfo。
func (m *NodeToStatus) NodesForStatusCode(nodeLister fwk.NodeInfoLister, code fwk.Code) ([]fwk.NodeInfo, error) {
	var resultNodes []fwk.NodeInfo

	if m.AbsentNodesStatus().Code() == code {
		allNodes, err := nodeLister.List()
		if err != nil {
			return nil, err
		}
		if m.Len() == 0 {
			// 所有节点都未显式记录，且状态匹配，直接全部返回。
			return allNodes, nil
		}
		// 从所有节点中筛出未记录或状态匹配的节点。
		for _, node := range allNodes {
			nodeName := node.Node().Name
			if status := m.Get(nodeName); status.Code() == code {
				resultNodes = append(resultNodes, node)
			}
		}
		return resultNodes, nil
	}

	m.ForEachExplicitNode(func(nodeName string, status *fwk.Status) {
		if status.Code() == code {
			if nodeInfo, err := nodeLister.Get(nodeName); err == nil {
				resultNodes = append(resultNodes, nodeInfo)
			}
		}
	})

	return resultNodes, nil
}

// PodsToActivateKey 是存放待激活 Pod 的保留状态键。
// 如果暂存的 Pod 出现在 unschedulableEntities 或 backoffQ 中，会分两阶段激活（移入 activeQ）：
//   - 调度周期成功结束时（激活后会从 `PodsToActivate` 中清除）
//   - 绑定周期成功结束时
var PodsToActivateKey fwk.StateKey = "kubernetes.io/pods-to-activate"

// PodsToActivate 保存待激活的 Pod。
type PodsToActivate struct {
	sync.Mutex
	// Map 以带命名空间的 Pod 名为键，Pod 对象为值。
	Map map[string]*v1.Pod
}

// Clone 直接返回同一份状态，不做拷贝。
func (s *PodsToActivate) Clone() fwk.StateData {
	return s
}

// NewPodsToActivate 创建 PodsToActivate 对象。
func NewPodsToActivate() *PodsToActivate {
	return &PodsToActivate{Map: make(map[string]*v1.Pod)}
}

// SortedScoredNodes 是调度返回的已打分节点列表。
type SortedScoredNodes interface {
	Pop() fwk.NodePluginScores
	Len() int
	// UnorderedList 按堆内部顺序返回所有节点，不保证按分数排序。
	UnorderedList() []fwk.NodePluginScores
}

// PlacementFeasiblePlugin 在 PodGroup 中每个 Pod 评估后调用。
// 用于判断 PodGroup 是可调度、可能可调度，还是无论剩余 Pod 结果如何都不可调度。
type PlacementFeasiblePlugin interface {
	fwk.Plugin

	// PlacementFeasible 在 PodGroup 中每个 Pod 评估后调用。
	// placementProgress 提供插件判断放置可行性时可能额外需要的信息。
	// 当前部分放置无法调度、但继续评估更多 Pod 后可能可调度时返回 Wait。
	// 当前放置确定无法调度时返回 Unschedulable，调度器会放弃该放置且不再评估剩余 Pod，但该放置仍可被抢占。
	// 当前部分放置可以调度时返回 Success；一旦返回 Success，后续 Pod 也应继续返回 Success。
	PlacementFeasible(ctx context.Context, placementCycleState fwk.PlacementCycleState, podGroupInfo fwk.PodGroupInfo, placementProgress PlacementProgress) *fwk.Status
}

// PlacementProgress 保存 PlacementFeasiblePlugin 判断放置可行性时可能需要的额外信息。
type PlacementProgress struct {
	// Remaining 是当前调度周期中尚未评估的子项数量；对 PodGroup 而言即未调度的 Pod 数。
	Remaining int
	// Scheduled 是当前 PodGroup 调度周期中，针对特定（组合）PodGroup 与放置已经完成的子项数量。
	// 对 PodGroup 而言，包含当前周期中已分配或已假设（assume）的 Pod。
	Scheduled int
}

// Framework 管理调度框架中使用的插件集合。
// 配置好的插件会在调度上下文的指定扩展点被调用。
type Framework interface {
	fwk.Handle

	// PreEnqueuePlugins 返回已注册的 preEnqueue 插件。
	PreEnqueuePlugins() []fwk.PreEnqueuePlugin

	// EnqueueExtensions 返回已注册的入队扩展。
	EnqueueExtensions() []fwk.EnqueueExtensions

	// QueueSortFunc 返回调度队列中 Pod 的排序函数。
	QueueSortFunc() fwk.LessFunc

	// SignPod 为 Pod 生成调度签名（如果可行）。签名相同的两个 Pod，
	// 在任意节点集合上应得到相同的可行性与分数，即使其中一个已被分配。
	// 如果某些插件无法生成签名，Pod 就是「不可签名的」，会禁用结果缓存与 gang 调度优化。
	// 参见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/5598-opportunistic-batching
	SignPod(ctx context.Context, pod *v1.Pod) fwk.PodSignature

	// RunPreFilterPlugins 运行配置的一组 PreFilter 插件。
	// 任一插件返回非 Success 状态时，返回的 *fwk.Status 的 code 非成功，调度周期随即中止。
	// 同时返回 PreFilterResult，可能影响下游评估哪些节点、评估多少节点。
	// 第三个返回值包含通过 PreFilterResult 拒绝部分或全部节点的 PreFilter 插件。
	// 注意：如果插件是通过非 Success 状态而非 PreFilterResult 拒绝 Pod，这里不包含任何插件。
	RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) (*fwk.PreFilterResult, *fwk.Status, sets.Set[string])

	// RunPostFilterPlugins 运行配置的一组 PostFilter 插件。
	// PostFilter 插件可以是纯信息型的（应配置为先执行并返回 Unschedulable），
	// 也可以尝试改变集群状态，让 Pod 在未来的调度周期中变得可调度。
	RunPostFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, filteredNodeStatusMap fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status)

	// GetNodeHint 返回给定 Pod 的「节点提示」：当批处理代码判断本周期可复用上一周期的信息时，
	// 给出的节点名。如果批处理代码无法提供提示，返回 ""。
	// 参见 git.k8s.io/enhancements/keps/sig-scheduling/5598-opportunistic-batching
	GetNodeHint(ctx context.Context, pod *v1.Pod, signature fwk.PodSignature, state fwk.CycleState, cycleCount int64) string

	// StoreScheduleResults 保存节点排序与过滤后的结果。
	StoreScheduleResults(ctx context.Context, signature fwk.PodSignature, hintedNode, chosenNode string, otherNodes SortedScoredNodes, cycleCount int64)

	// RunRawScorePlugins 只对单个节点运行各打分插件的 Score() 阶段，
	// 不执行 NormalizeScore 与权重计算，返回归一化前的分数。
	RunRawScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) ([]fwk.PluginScore, *fwk.Status)

	// NormalizeScores 对给定节点运行 NormalizeScore() 并应用权重，以 RawScores 为输入，
	// 原地更新每个 NodePluginScores 的 Scores 与 TotalScore。
	NormalizeScores(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores []fwk.NodePluginScores) *fwk.Status

	// RunPlacementGeneratePlugins 运行配置的一组 PlacementGenerate 插件。
	// 返回所有生成 Placement 的合并列表。
	RunPlacementGeneratePlugins(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, nodes []fwk.NodeInfo) ([]*fwk.Placement, *fwk.Status)

	// RunPreBindPlugins 运行配置的一组 PreBind 插件。
	// 任一插件返回非 Success 状态时，返回的 *fwk.Status 的 code 非成功。
	// 若状态码为 "Unschedulable"，视为调度检查失败，否则视为内部错误；无论哪种情况 Pod 都不会被绑定。
	RunPreBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status

	// RunPreBindPreFlights 运行 PreBind 插件中配置的 PreBindPreFlight 函数。
	// 任一插件返回非 skip 状态时立即返回。
	RunPreBindPreFlights(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status

	// RunPostBindPlugins 运行配置的一组 PostBind 插件。
	RunPostBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string)

	// RunPermitPlugins 运行配置的一组 Permit 插件。
	// 任一插件返回 "Success" 或 "Wait" 之外的状态时，不再运行其余插件并返回错误。
	// 否则，任一插件返回 "Wait" 时，本函数构造 pluginsWaitTime 并返回 code 为 "Wait" 的状态。
	// 本函数自身不会创建 waiting pod 对象，调用方需调用 AddWaitingPod 完成。
	RunPermitPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (pluginsWaitTime map[string]time.Duration, status *fwk.Status)

	// RunPlacementFeasiblePlugins 运行实现了 PlacementFeasible 接口的 Permit 插件集合。
	// 所有插件返回 Success 时结果为 Success。
	// 其他合法状态只有 Wait 和 Unschedulable。
	// 任一插件返回非法状态时，结果为 Error，剩余插件不再调用。
	// 否则，至少一个插件返回 Unschedulable 时，剩余插件不再调用，结果为 Unschedulable；该放置仍可被抢占。
	// 否则，至少一个插件返回 Wait 时，剩余插件仍会继续调用，结果为 Wait。
	RunPlacementFeasiblePlugins(ctx context.Context, placementCycleState fwk.PlacementCycleState, podGroupInfo fwk.PodGroupInfo, placementProgress PlacementProgress) *fwk.Status

	// AddWaitingPod 创建 waiting pod 实例并加入框架。
	// 参数是 RunPermitPlugins 返回的 pluginsWaitTime。
	// Pod 最短等待时间取 Permit 插件返回值中的最小值。
	// 只在 RunPermitPlugins 返回 Wait 且即将执行 WaitOnPermit 时调用。
	AddWaitingPod(pod *v1.Pod, pluginsWaitTime map[string]time.Duration)

	// WillWaitOnPermit 通过检查 Pod 是否为 waiting pod，判断它是否会在 permit 处等待。
	WillWaitOnPermit(ctx context.Context, pod *v1.Pod) bool

	// WaitOnPermit 若 Pod 是 waiting pod，则阻塞直到它被拒绝或放行。
	WaitOnPermit(ctx context.Context, pod *v1.Pod) *fwk.Status

	// RunBindPlugins 运行配置的一组 Bind 插件。Bind 插件可自行决定是否处理该 Pod。
	// 如果选择跳过绑定，应返回 code=5("skip")；否则返回 "Error" 或 "Success"。
	// 如果没有任何插件处理绑定，RunBindPlugins 返回 code=5("skip")。
	RunBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status

	// RunPlacementScorePlugins 运行配置的 Placement 打分插件集合。
	// 返回每个插件的分数以及每个 Placement 的总分。
	// 同时返回 *Status：任一插件返回非成功状态时该值非成功。
	// 每个 PlacementCycleState 传给相同下标的 PodGroupAssignments 对应的 ScorePlacement。
	RunPlacementScorePlugins(ctx context.Context, state fwk.PodGroupCycleState, podGroupInfo fwk.PodGroupInfo, placements []*fwk.PodGroupAssignments, placementStates []fwk.PlacementCycleState) (ns []fwk.PlacementPluginScores, status *fwk.Status)

	// RunPodGroupPostFilterPlugins 运行配置的 PodGroupPostFilter 插件集合。
	RunPodGroupPostFilterPlugins(ctx context.Context, state *CycleState, podGroupInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status)

	// HasFilterPlugins 是否至少定义了一个 Filter 插件。
	HasFilterPlugins() bool

	// HasPostFilterPlugins 是否至少定义了一个 PostFilter 插件。
	HasPostFilterPlugins() bool

	// HasScorePlugins 是否至少定义了一个 Score 插件。
	HasScorePlugins() bool

	// PodGroupPostFilterPlugins 返回已注册的 PodGroupPostFilter 插件。
	PodGroupPostFilterPlugins() []fwk.PodGroupPostFilterPlugin

	// ListPlugins 返回扩展点名称到已配置插件列表的映射。
	ListPlugins() *config.Plugins

	// PercentageOfNodesToScore 返回该 profile 的 percentageOfNodesToScore。
	PercentageOfNodesToScore() *int32

	// SetPodNominator 设置 PodNominator。
	SetPodNominator(nominator fwk.PodNominator)
	// SetPodActivator 设置 PodActivator。
	SetPodActivator(activator fwk.PodActivator)
	// SetAPICacher 设置 APICacher。
	SetAPICacher(apiCacher fwk.APICacher)

	// Close 依次调用各插件的 Close 方法。
	Close() error
}

// NewPostFilterResultWithNominatedNode 创建指定提名节点的 PostFilterResult。
func NewPostFilterResultWithNominatedNode(name string) *fwk.PostFilterResult {
	return &fwk.PostFilterResult{
		NominatingInfo: &fwk.NominatingInfo{
			NominatedNodeName: name,
			NominatingMode:    fwk.ModeOverride,
		},
	}
}
