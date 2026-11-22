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

package nodeunschedulable

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/resource"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// NodeUnschedulable 插件过滤掉设置了 `node.Spec.Unschedulable=true` 的节点，
// 除非 Pod 容忍 `{key=node.kubernetes.io/unschedulable, effect:NoSchedule}` 污点。
type NodeUnschedulable struct {
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	enableTaintTolerationComparisonOperators           bool
}

var _ fwk.PreFilterPlugin = &NodeUnschedulable{}
var _ fwk.FilterPlugin = &NodeUnschedulable{}
var _ fwk.EnqueueExtensions = &NodeUnschedulable{}
var _ fwk.SignPlugin = &NodeUnschedulable{}

// Name 是该插件在插件注册表和配置中使用的名称。
const Name = names.NodeUnschedulable

const (
	// ErrReasonUnknownCondition 用于 `NodeUnknownCondition` 判定错误的原因字符串。
	ErrReasonUnknownCondition = "node(s) had unknown conditions"
	// ErrReasonUnschedulable 用于 `NodeUnschedulable` 判定错误的原因字符串。
	ErrReasonUnschedulable = "node(s) were unschedulable"
)

// EventsToRegister 返回可能让此前被本插件判为不可调度的 Pod 重新变为可调度的事件。
func (pl *NodeUnschedulable) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 启用 QueueingHint 后不再使用 preCheck，因此无需注册 UpdateNodeLabel 事件。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
		// 启用 QueueingHint 特性后，调度队列借助 Pod/Update QueueingHint
		// 判断 Pod 的更新是否让它变得可调度。
		// https://github.com/kubernetes/kubernetes/pull/122234
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration}, QueueingHintFn: pl.isSchedulableAfterTargetPodTolerationChange},
	}, nil
}

// isSchedulableAfterTargetPodTolerationChange 在 Pod 的 toleration 发生变化时被调用。
func (pl *NodeUnschedulable) isSchedulableAfterTargetPodTolerationChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, modifiedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	// 无需检查 oldPod 的 toleration 是否已覆盖该污点，原因：
	// - 污点只能添加，不能修改或删除。
	// - 若 Pod 原本就有该 toleration，它一开始就不会被本插件拒绝；
	//   既然曾被拒绝，说明它此刻还没有该 toleration。
	if v1helper.TolerationsTolerateTaint(logger, modifiedPod.Spec.Tolerations, &v1.Taint{
		Key:    v1.TaintNodeUnschedulable,
		Effect: v1.TaintEffectNoSchedule,
	}, pl.enableTaintTolerationComparisonOperators) {
		// 这次更新使 Pod 能够容忍 unschedulable 污点。
		logger.V(5).Info("a new toleration is added for the unschedulable Pod, and it may make it schedulable", "pod", klog.KObj(modifiedPod))
		return fwk.Queue, nil
	}

	logger.V(5).Info("a new toleration is added for the unschedulable Pod, but it's an unrelated toleration", "pod", klog.KObj(modifiedPod))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterNodeChange 在 informer 上报的所有 Node 事件上被调用，
// 检查该变更是否让此前不可调度的 Pod 变为可调度。
func (pl *NodeUnschedulable) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	// 以下情况需要重新将该 Pod 入队：
	// 1. 节点从不可调度更新为可调度。
	// 2. 新增节点且该节点可调度。
	if (originalNode != nil && originalNode.Spec.Unschedulable && !modifiedNode.Spec.Unschedulable) ||
		(originalNode == nil && !modifiedNode.Spec.Unschedulable) {
		logger.V(5).Info("node was created or updated, pod may be schedulable now", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}

	logger.V(5).Info("node was created or updated, but it doesn't make this pod schedulable", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.QueueSkip, nil
}

// Name 返回插件名称，用于日志等场景。
func (pl *NodeUnschedulable) Name() string {
	return Name
}

// 基于 Pod 的 toleration 生成用于可行性与打分的签名片段。
func (pl *NodeUnschedulable) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.TolerationsSignerName, Value: fwk.TolerationsSigner(pod)},
	}, nil
}

// PreFilter 在 prefilter 扩展点被调用。
func (pl *NodeUnschedulable) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		// 延迟调整大小的 Pod 由抢占流程单独处理，跳过以免干扰后续扩展点。
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

// 本插件不提供 PreFilterExtensions。
func (pl *NodeUnschedulable) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 在 filter 扩展点被调用。
func (pl *NodeUnschedulable) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		// 同上：延迟调整大小的 Pod 不在本插件里被过滤掉。
		return nil
	}
	node := nodeInfo.Node()

	if !node.Spec.Unschedulable {
		return nil
	}

	logger := klog.FromContext(ctx)
	// 如果 Pod 容忍 unschedulable 污点，也就等同于容忍 `node.Spec.Unschedulable`。
	podToleratesUnschedulable := v1helper.TolerationsTolerateTaint(logger, pod.Spec.Tolerations, &v1.Taint{
		Key:    v1.TaintNodeUnschedulable,
		Effect: v1.TaintEffectNoSchedule,
	}, pl.enableTaintTolerationComparisonOperators)
	if !podToleratesUnschedulable {
		// 这是 Pod 自身属性，换节点也解决不了，标记为不可解以避免无谓的重试。
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonUnschedulable)
	}

	return nil
}

// New 初始化并返回一个新的插件实例。
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &NodeUnschedulable{
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
		enableTaintTolerationComparisonOperators:           fts.EnableTaintTolerationComparisonOperators,
	}, nil
}
