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

package nodeaffinity

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// NodeAffinity 检查 Pod 的 nodeSelector/nodeAffinity 是否与节点标签匹配，并据此过滤和打分。
type NodeAffinity struct {
	handle                                             fwk.Handle
	addedNodeSelector                                  *nodeaffinity.NodeSelector
	addedPrefSchedTerms                                *nodeaffinity.PreferredSchedulingTerms
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &NodeAffinity{}
var _ fwk.FilterPlugin = &NodeAffinity{}
var _ fwk.PreScorePlugin = &NodeAffinity{}
var _ fwk.ScorePlugin = &NodeAffinity{}
var _ fwk.EnqueueExtensions = &NodeAffinity{}
var _ fwk.SignPlugin = &NodeAffinity{}

const (
	// Name 是该插件在插件注册表和配置文件中使用的名称。
	Name = names.NodeAffinity

	// preScoreStateKey 是 CycleState 中存放打分用预计算数据的键。
	preScoreStateKey = "PreScore" + Name

	// preFilterStateKey 是 CycleState 中存放过滤用预计算数据的键。
	preFilterStateKey = "PreFilter" + Name

	// ErrReasonPod 是 Pod 自身 nodeAffinity/nodeSelector 不匹配时的原因。
	ErrReasonPod = "node(s) didn't match Pod's node affinity/selector"

	// errReasonEnforced 是调度器强制添加的 nodeAffinity 不匹配时的原因。
	errReasonEnforced = "node(s) didn't match scheduler-enforced node affinity"

	// errReasonConflict 是 Pod 亲和性规则内部冲突时的原因。
	errReasonConflict = "pod affinity terms conflict"
)

// Name 返回插件名称，用于日志等场景。
func (pl *NodeAffinity) Name() string {
	return Name
}

// SignPod 生成包含 Pod 节点亲和性与 nodeSelector 的签名片段，
// 因为过滤和打分都依赖这两项配置。
func (pl *NodeAffinity) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	aff, err := fwk.NodeAffinitySigner(pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	return []fwk.SignFragment{
		{Key: fwk.NodeAffinitySignerName, Value: aff},
		{Key: fwk.NodeSelectorSignerName, Value: pod.Spec.NodeSelector},
	}, nil
}

// preFilterState 保存 PreFilter 阶段计算好的必需节点亲和性规则。
type preFilterState struct {
	requiredNodeSelectorAndAffinity nodeaffinity.RequiredNodeAffinity
}

// Clone 直接返回同一状态：它不受 Pod 增删影响，无需复制。
func (s *preFilterState) Clone() fwk.StateData {
	return s
}

// EventsToRegister 返回可能让此前被本插件判定不可调度的 Pod 变为可调度的事件。
func (pl *NodeAffinity) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
	}, nil
}

// isSchedulableAfterNodeChange 在节点发生变化时调用，
// 判断该变化是否让此前不可调度的 Pod 变得可调度。
func (pl *NodeAffinity) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pl.addedNodeSelector != nil && !pl.addedNodeSelector.Match(modifiedNode) {
		logger.V(4).Info("added or modified node didn't match scheduler-enforced node affinity and this event won't make the Pod schedulable", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.QueueSkip, nil
	}

	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	isMatched, err := requiredNodeAffinity.Match(modifiedNode)
	if err != nil {
		return fwk.Queue, err
	}
	if !isMatched {
		logger.V(5).Info("node was created or updated, but the pod's NodeAffinity doesn't match", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.QueueSkip, nil
	}
	// 新节点创建且匹配 Pod 的亲和性条件，此前被阻塞的 Pod 现在可以调度了。
	if originalNode == nil {
		logger.V(5).Info("node was created, and matches with the pod's NodeAffinity", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}
	// 到此可以确定是更新操作，只需关注由不匹配变为匹配的情况
	// （即节点上新增了必需的亲和性标签）。
	wasMatched, err := requiredNodeAffinity.Match(originalNode)
	if err != nil {
		return fwk.Queue, err
	}
	if wasMatched {
		logger.V(5).Info("node updated, but the pod's NodeAffinity hasn't changed", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.QueueSkip, nil
	}
	logger.V(5).Info("node was updated and the pod's NodeAffinity changed to matched", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.Queue, nil
}

// PreFilter 构建并写入 Filter 阶段使用的 cycle state，
// 同时尝试把元数据名匹配收敛为具体的节点名列表，缩小后续过滤范围。
func (pl *NodeAffinity) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// 延迟扩容的 Pod 由抢占流程处理，这里跳过亲和性过滤。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	affinity := pod.Spec.Affinity
	noNodeAffinity := (affinity == nil ||
		affinity.NodeAffinity == nil ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil)
	if noNodeAffinity && pl.addedNodeSelector == nil && pod.Spec.NodeSelector == nil {
		// Pod 没有任何节点亲和性约束，跳过过滤。
		return nil, fwk.NewStatus(fwk.Skip)
	}

	state := &preFilterState{requiredNodeSelectorAndAffinity: nodeaffinity.GetRequiredNodeAffinity(pod)}
	cycleState.Write(preFilterStateKey, state)

	if noNodeAffinity || len(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		return nil, nil
	}

	// 检查是否有针对具体节点名（metadata.name）的亲和性，若有则返回候选节点集合。
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	var nodeNames sets.Set[string]
	for _, t := range terms {
		var termNodeNames sets.Set[string]
		for _, r := range t.MatchFields {
			if r.Key == metav1.ObjectNameField && r.Operator == v1.NodeSelectorOpIn {
				// 同一 term 内的 MatchFields 是与关系，需要取节点集合的交集。
				s := sets.New(r.Values...)
				if termNodeNames == nil {
					termNodeNames = s
				} else {
					termNodeNames = termNodeNames.Intersection(s)
				}
			}
		}
		if termNodeNames == nil {
			// 该 term 没有 node.Name 约束；由于各 term 之间是或关系，
			// 此 term 可匹配任意节点，无需进一步收敛。
			return nil, nil
		}
		nodeNames = nodeNames.Union(termNodeNames)
	}
	// nodeNames 非 nil 但长度为 0，说明各 term 的 node.Name 约束相互冲突，
	// Pod 无法匹配任何节点。
	if nodeNames != nil && len(nodeNames) == 0 {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonConflict)
	} else if len(nodeNames) > 0 {
		return &fwk.PreFilterResult{NodeNames: nodeNames}, nil
	}
	return nil, nil

}

// PreFilterExtensions 对本插件非必需：状态不依赖 Pod 的增删。
func (pl *NodeAffinity) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 检查节点是否匹配 Pod 的 .spec.affinity.nodeAffinity 以及调度器强制添加的亲和性。
func (pl *NodeAffinity) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	node := nodeInfo.Node()

	if pl.addedNodeSelector != nil && !pl.addedNodeSelector.Match(node) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonEnforced)
	}

	s, err := getPreFilterState(state)
	if err != nil {
		// PreFilter 被禁用时没有可用状态，这里现场计算必需亲和性作为兜底。
		s = &preFilterState{requiredNodeSelectorAndAffinity: nodeaffinity.GetRequiredNodeAffinity(pod)}
	}

	// 为向后兼容忽略解析错误：历史上这类表达式不匹配节点，保持该行为。
	match, _ := s.requiredNodeSelectorAndAffinity.Match(node)
	if !match {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonPod)
	}

	return nil
}

// preScoreState 在 PreScore 阶段计算，供 Score 阶段使用。
type preScoreState struct {
	preferredNodeAffinity *nodeaffinity.PreferredSchedulingTerms
}

// Clone 实现必需的 Clone 接口；数据不可变，直接共享即可。
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// PreScore 构建并写入 Score 和 NormalizeScore 使用的 cycle state。
func (pl *NodeAffinity) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	preferredNodeAffinity, err := getPodPreferredNodeAffinity(pod)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if preferredNodeAffinity == nil && pl.addedPrefSchedTerms == nil {
		// Pod 没有优先亲和性，打分环节无需处理。
		return fwk.NewStatus(fwk.Skip)
	}
	state := &preScoreState{
		preferredNodeAffinity: preferredNodeAffinity,
	}
	cycleState.Write(preScoreStateKey, state)
	return nil
}

// Score 返回所有匹配该节点的优先亲和性项权重之和。
// 这些项来自 Pod 的 .spec.affinity.nodeAffinity 和插件配置的默认亲和性。
func (pl *NodeAffinity) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	node := nodeInfo.Node()

	var count int64
	if pl.addedPrefSchedTerms != nil {
		count += pl.addedPrefSchedTerms.Score(node)
	}

	s, err := getPreScoreState(state)
	if err != nil {
		// PreScore 被禁用时现场计算优先亲和性作为兜底。
		preferredNodeAffinity, err := getPodPreferredNodeAffinity(pod)
		if err != nil {
			return 0, fwk.AsStatus(err)
		}
		s = &preScoreState{
			preferredNodeAffinity: preferredNodeAffinity,
		}
	}

	if s.preferredNodeAffinity != nil {
		count += s.preferredNodeAffinity.Score(node)
	}

	return count, nil
}

// NormalizeScore 在所有节点打分后调用，把分数归一化到满分值。
func (pl *NodeAffinity) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	return helper.DefaultNormalizeScore(fwk.MaxNodeScore, false, scores)
}

// ScoreExtensions 返回打分插件的扩展，即本插件自身（实现了 NormalizeScore）。
func (pl *NodeAffinity) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

// New 初始化并返回一个 NodeAffinity 插件实例。
func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, err := getArgs(plArgs)
	if err != nil {
		return nil, err
	}
	pl := &NodeAffinity{
		handle: h,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}
	if args.AddedAffinity != nil {
		if ns := args.AddedAffinity.RequiredDuringSchedulingIgnoredDuringExecution; ns != nil {
			pl.addedNodeSelector, err = nodeaffinity.NewNodeSelector(ns)
			if err != nil {
				return nil, fmt.Errorf("parsing addedAffinity.requiredDuringSchedulingIgnoredDuringExecution: %w", err)
			}
		}
		// TODO: parse requiredDuringSchedulingRequiredDuringExecution when it gets added to the API.
		// 仅当配置了优先亲和性项时才构造对应的匹配器。
		if terms := args.AddedAffinity.PreferredDuringSchedulingIgnoredDuringExecution; len(terms) != 0 {
			pl.addedPrefSchedTerms, err = nodeaffinity.NewPreferredSchedulingTerms(terms)
			if err != nil {
				return nil, fmt.Errorf("parsing addedAffinity.preferredDuringSchedulingIgnoredDuringExecution: %w", err)
			}
		}
	}
	return pl, nil
}

// getArgs 校验并取出 NodeAffinity 插件配置参数。
func getArgs(obj runtime.Object) (config.NodeAffinityArgs, error) {
	ptr, ok := obj.(*config.NodeAffinityArgs)
	if !ok {
		return config.NodeAffinityArgs{}, fmt.Errorf("args are not of type NodeAffinityArgs, got %T", obj)
	}
	return *ptr, validation.ValidateNodeAffinityArgs(nil, ptr)
}

// getPodPreferredNodeAffinity 解析 Pod 的优先节点亲和性；未配置时返回 nil。
func getPodPreferredNodeAffinity(pod *v1.Pod) (*nodeaffinity.PreferredSchedulingTerms, error) {
	affinity := pod.Spec.Affinity
	if affinity != nil && affinity.NodeAffinity != nil && affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
		return nodeaffinity.NewPreferredSchedulingTerms(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	}
	return nil, nil
}

// getPreScoreState 从 CycleState 中读取并校验 PreScore 阶段保存的状态。
func getPreScoreState(cycleState fwk.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid PreScore state, got type %T", c)
	}
	return s, nil
}

// getPreFilterState 从 CycleState 中读取并校验 PreFilter 阶段保存的状态。
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %v", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("invalid PreFilter state, got type %T", c)
	}
	return s, nil
}
