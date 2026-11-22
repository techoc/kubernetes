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

// Package tainttoleration 提供污点与容忍相关的调度插件。
//
// Filter 阶段拒绝 Pod 不能容忍的 NoSchedule/NoExecute 污点；
// Score 阶段按 Pod 不能容忍的 PreferNoSchedule 污点数量降分，
// 让调度器尽量避开这些节点。
package tainttoleration

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/resource"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// TaintToleration 检查 Pod 是否容忍节点的污点。
type TaintToleration struct {
	handle                                             fwk.Handle
	enableTaintTolerationComparisonOperators           bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &TaintToleration{}
var _ fwk.FilterPlugin = &TaintToleration{}
var _ fwk.PreScorePlugin = &TaintToleration{}
var _ fwk.ScorePlugin = &TaintToleration{}
var _ fwk.EnqueueExtensions = &TaintToleration{}
var _ fwk.SignPlugin = &TaintToleration{}

const (
	// Name 是插件在注册表和配置中使用的名称。
	Name = names.TaintToleration
	// preScoreStateKey 是 CycleState 中存放评分预计算数据的键。
	preScoreStateKey = "PreScore" + Name
	// ErrReasonNotMatch 是不匹配时 Filter 返回的状态原因。
	ErrReasonNotMatch = "node(s) had taints that the pod didn't tolerate"
)

// Name 返回插件名称，用于日志等场景。
func (pl *TaintToleration) Name() string {
	return Name
}

// SignPod 基于 Pod 的容忍声明生成签名片段。
// 可行性检查与打分都依赖同一份容忍配置，因此用签名标识它。
func (pl *TaintToleration) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.TolerationsSignerName, Value: fwk.TolerationsSigner(pod)},
	}, nil
}

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 变为可调度的事件。
// 节点污点变化和 Pod 容忍变化是两类直接的触发源。
func (pl *TaintToleration) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration}, QueueingHintFn: pl.isSchedulableAfterTargetPodTolerationChange},
	}, nil
}

// isSchedulableAfterNodeChange 对所有节点事件调用，判断变更是否让此前不可调度的 Pod 变为可调度。
// 只有从「存在无法容忍的污点」变为「不存在」时才需要重新入队。
func (pl *TaintToleration) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	// 新增节点没有旧状态，保守地认为之前存在无法容忍的污点。
	wasUntolerated := true
	if originalNode != nil {
		_, wasUntolerated = v1helper.FindMatchingUntoleratedTaint(logger, originalNode.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), pl.enableTaintTolerationComparisonOperators)
	}

	_, isUntolerated := v1helper.FindMatchingUntoleratedTaint(logger, modifiedNode.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), pl.enableTaintTolerationComparisonOperators)

	if wasUntolerated && !isUntolerated {
		logger.V(5).Info("node was created or updated, and this may make the Pod rejected by TaintToleration plugin in the previous scheduling cycle schedulable", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}

	logger.V(5).Info("node was created or updated, but it doesn't change the TaintToleration plugin's decision", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.QueueSkip, nil
}

// PreFilter 在 prefilter 扩展点调用。
// 延迟调整大小的 Pod 需要跳过本插件，避免调度与原地扩缩容的抢占逻辑冲突。
func (pl *TaintToleration) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

// PreFilterExtensions 返回 nil：本插件没有需要逐节点处理的 prefilter 扩展。
func (pl *TaintToleration) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 在 filter 扩展点调用，拒绝 Pod 无法容忍的节点污点。
// 节点中只要有一个 DoNotSchedule 级别的污点未被容忍，就直接判定不可调度。
func (pl *TaintToleration) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	taint, isUntolerated := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations,
		helper.DoNotScheduleTaintsFilterFunc(),
		pl.enableTaintTolerationComparisonOperators)
	if !isUntolerated {
		return nil
	}

	logger.V(4).Info("node had untolerated taints", "node", klog.KObj(node), "pod", klog.KObj(pod), "untoleratedTaint", taint)
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)")
}

// preScoreState 在 PreScore 阶段计算，供 Score 阶段复用。
type preScoreState struct {
	tolerationsPreferNoSchedule []v1.Toleration
}

// Clone 实现 StateData 的 Clone 接口。
// 该状态在打分期间只读，因此直接返回原指针，避免无谓拷贝。
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// getAllTolerationPreferNoSchedule 收集所有 Effect 为 PreferNoSchedule 或未指定 Effect 的容忍。
// 未指定 Effect 的容忍对所有效果生效，自然也包括 PreferNoSchedule。
func getAllTolerationPreferNoSchedule(tolerations []v1.Toleration) (tolerationList []v1.Toleration) {
	for _, toleration := range tolerations {
		// 空 Effect 表示匹配所有效果，包含 PreferNoSchedule，需要一并收集。
		if len(toleration.Effect) == 0 || toleration.Effect == v1.TaintEffectPreferNoSchedule {
			tolerationList = append(tolerationList, toleration)
		}
	}
	return
}

// PreScore 构建并写入 CycleState，供 Score 与 NormalizeScore 使用。
func (pl *TaintToleration) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	tolerationsPreferNoSchedule := getAllTolerationPreferNoSchedule(pod.Spec.Tolerations)
	state := &preScoreState{
		tolerationsPreferNoSchedule: tolerationsPreferNoSchedule,
	}
	cycleState.Write(preScoreStateKey, state)
	return nil
}

// getPreScoreState 从 CycleState 读取本插件的预计算状态。
func getPreScoreState(cycleState fwk.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to tainttoleration.preScoreState error", c)
	}
	return s, nil
}

// countIntolerableTaintsPreferNoSchedule 统计节点上 Pod 无法容忍的 PreferNoSchedule 污点数量。
func (pl *TaintToleration) countIntolerableTaintsPreferNoSchedule(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration) (intolerableTaints int) {
	for _, taint := range taints {
		// 只检查 PreferNoSchedule 级别的污点。
		if taint.Effect != v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !v1helper.TolerationsTolerateTaint(logger, tolerations, &taint, pl.enableTaintTolerationComparisonOperators) {
			intolerableTaints++
		}
	}
	return
}

// Score 在 score 扩展点调用，返回无法容忍的 PreferNoSchedule 污点数量。
func (pl *TaintToleration) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	logger := klog.FromContext(ctx)

	node := nodeInfo.Node()

	s, err := getPreScoreState(state)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}

	score := int64(pl.countIntolerableTaintsPreferNoSchedule(logger, node.Spec.Taints, s.tolerationsPreferNoSchedule))
	return score, nil
}

// NormalizeScore 在所有节点打分完成后调用，把分数归一化到 0-100。
func (pl *TaintToleration) NormalizeScore(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	return helper.DefaultNormalizeScore(fwk.MaxNodeScore, true, scores)
}

// ScoreExtensions 返回 Score 插件的扩展接口，即插件自身。
func (pl *TaintToleration) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

// New 初始化插件并返回。
func New(_ context.Context, _ runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &TaintToleration{
		handle:                                   h,
		enableTaintTolerationComparisonOperators: fts.EnableTaintTolerationComparisonOperators,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}, nil
}

// isSchedulableAfterTargetPodTolerationChange 在 Pod 的容忍发生变化时调用。
func (pl *TaintToleration) isSchedulableAfterTargetPodTolerationChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	// 更新的 Pod 就是那个此前不可调度的 Pod。
	logger.V(5).Info("a new toleration is added for the unschedulable Pod, and it may make it schedulable", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}
