/*
Copyright The Kubernetes Authors.

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

package deferredpodscheduling

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// DeferredPodScheduling 处理“延迟扩容”Pod 的调度插件：这类 Pod 扩容后暂时放不下时，
// 允许它留在 unschedulable 队列中等待 Kubelet 实际执行扩容，再由抢占机制腾出空间。
type DeferredPodScheduling struct {
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &DeferredPodScheduling{}
var _ fwk.FilterPlugin = &DeferredPodScheduling{}
var _ fwk.EnqueueExtensions = &DeferredPodScheduling{}
var _ fwk.PermitPlugin = &DeferredPodScheduling{}

const (
	// Name 是本插件在调度器框架中的注册名。
	Name = names.DeferredPodScheduling
	// ErrReasonNodeDisablesResizePreemption 是节点禁用扩容抢占时的过滤失败原因。
	ErrReasonNodeDisablesResizePreemption = "node had resize preemption disabled"
)

// Name 返回插件名。
func (pl *DeferredPodScheduling) Name() string {
	return Name
}

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 重新变为可调度的集群事件。
func (pl *DeferredPodScheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodePreemptionPolicy},
			QueueingHintFn: pl.isSchedulableAfterNodeChange,
		},
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			QueueingHintFn: pl.isSchedulableAfterNodeAdd,
		},
	}, nil
}

// isSchedulableAfterNodeChange 判断节点抢占策略变化后，Pod 是否可能变得可调度。
func (pl *DeferredPodScheduling) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return fwk.QueueSkip, nil
	}
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		return fwk.Queue, fmt.Errorf("got object of type %T, want *v1.Node", oldObj)
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		return fwk.Queue, fmt.Errorf("got object of type %T, want *v1.Node", newObj)
	}

	if pod.Spec.NodeName != newNode.Name {
		return fwk.QueueSkip, nil
	}

	oldNodeDisabled := oldNode.Spec.PodPreemptionPolicy != nil && len(oldNode.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0
	newNodeDisabled := newNode.Spec.PodPreemptionPolicy != nil && len(newNode.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0

	// 抢占策略从禁用变为启用后，Pod 可能重新变得可调度，值得重新入队。
	if oldNodeDisabled && !newNodeDisabled {
		logger.V(5).Info("Node preemption policy changed from disabled to enabled, queueing pod", "pod", klog.KObj(pod), "node", newNode.Name)
		return fwk.Queue, nil
	}

	return fwk.QueueSkip, nil
}

// isSchedulableAfterNodeAdd 判断新增节点后，Pod 是否可能变得可调度。
func (pl *DeferredPodScheduling) isSchedulableAfterNodeAdd(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return fwk.QueueSkip, nil
	}
	node, ok := newObj.(*v1.Node)
	if !ok {
		return fwk.Queue, fmt.Errorf("got object of type %T, want *v1.Node", newObj)
	}

	if pod.Spec.NodeName != node.Name {
		return fwk.QueueSkip, nil
	}

	// 新增节点若启用了扩容抢占，Pod 可能重新变得可调度。
	if node.Spec.PodPreemptionPolicy == nil || len(node.Spec.PodPreemptionPolicy.DisableResizePreemption) == 0 {
		logger.V(5).Info("Node added with preemption enabled, queueing pod", "pod", klog.KObj(pod), "node", node.Name)
		return fwk.Queue, nil
	}

	return fwk.QueueSkip, nil
}

// PreFilter 在非延迟扩容 Pod 上跳过本插件，避免对普通 Pod 产生开销。
func (pl *DeferredPodScheduling) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

// PreFilterExtensions 返回 nil，本插件不使用 PreFilter 的扩展点。
func (pl *DeferredPodScheduling) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 过滤掉禁用了扩容抢占的节点，以及 Pod 已绑定到其他节点的候选节点。
func (pl *DeferredPodScheduling) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Error, "node not found")
	}

	if pod.Spec.NodeName != "" && pod.Spec.NodeName != node.Name {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod assigned to different node")
	}

	if node.Spec.PodPreemptionPolicy != nil && len(node.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0 {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeDisablesResizePreemption)
	}
	return nil
}

// Permit 在 permit 扩展点被调用。
// 即使延迟扩容的 Pod 已经能放进节点，也拒绝放行，让它继续留在 Unschedulable 队列中，
// 直到 Kubelet 真正完成扩容动作后再重新调度。
func (pl *DeferredPodScheduling) Permit(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(p) {
		return nil, 0
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod resize fits, waiting for Kubelet actuation"), 0
}

// New 根据启用的特性创建 DeferredPodScheduling 插件实例。
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &DeferredPodScheduling{
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}, nil
}
