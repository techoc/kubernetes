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

package nodeports

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// NodePorts 是一个插件，检查节点上是否有空闲端口供 Pod 请求的宿主端口使用。
type NodePorts struct {
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &NodePorts{}
var _ fwk.FilterPlugin = &NodePorts{}
var _ fwk.EnqueueExtensions = &NodePorts{}
var _ fwk.SignPlugin = &NodePorts{}

const (
	// Name 是插件在插件注册表和配置中使用的名字。
	Name = names.NodePorts

	// preFilterStateKey 是 CycleState 中存放 NodePorts 预计算数据的键。
	// 以插件名做前缀，可以避免与其他插件的键冲突。
	preFilterStateKey = "PreFilter" + Name

	// ErrReason 是节点端口不可用时的失败原因。
	ErrReason = "node(s) didn't have free ports for the requested pod ports"
)

// preFilterState 保存 Pod 请求的宿主端口列表，由 PreFilter 计算并在 Filter 中复用。
type preFilterState []v1.ContainerPort

// Clone 返回 prefilter 状态本身。
func (s preFilterState) Clone() fwk.StateData {
	// 该状态不受 Pod 增删影响，所以无需深拷贝，直接共享即可。
	return s
}

// Name 返回插件名，用于日志等场景。
func (pl *NodePorts) Name() string {
	return Name
}

// SignPod 根据容器的宿主端口生成签名片段。
// 节点可行性判断与调度都以容器的宿主端口为依据。
func (pl *NodePorts) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.HostPortsSignerName, Value: fwk.HostPortsSigner(pod)},
	}, nil
}

// PreFilter 在 prefilter 扩展点被调用，预先算出 Pod 需要的宿主端口。
func (pl *NodePorts) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// 原地扩容被推迟时端口可能变化，此时跳过本插件，交给后续抢占流程处理。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	s := util.GetHostPorts(pod)
	// Pod 没有请求宿主端口时无需参与过滤，直接跳过。
	if len(s) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, preFilterState(s))
	return nil, nil
}

// PreFilterExtensions 返回 nil：本插件不需要增量更新 prefilter 状态。
func (pl *NodePorts) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// getPreFilterState 从 CycleState 读取 PreFilter 计算好的宿主端口列表。
func getPreFilterState(cycleState fwk.CycleState) (preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// 读不到说明 PreFilter 没被调用，或者状态被其他插件清掉了。
		return nil, fmt.Errorf("reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to nodeports.preFilterState error", c)
	}
	return s, nil
}

// EventsToRegister 返回可能让被本插件判定为不可调度的 Pod 重新变得可调度的事件。
func (pl *NodePorts) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// `spec.containers[*].ports` 是不可变字段，Pod 更新不会改变端口占用，故忽略更新事件。
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		// 这里不需要 QueueingHintFn：节点添加时会带退避重试，效果等同于 Queue。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
	}, nil
}

// isSchedulableAfterAssignedPodDeleted 在已调度 Pod 被删除时调用，判断这次删除是否让
// 之前因端口冲突而不可调度的 Pod 变得可调度。
func (pl *NodePorts) isSchedulableAfterAssignedPodDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedPod, _, err := util.As[*v1.Pod](oldObj, nil)
	if err != nil {
		return fwk.Queue, err
	}

	// 未调度的 Pod 没有占用任何节点端口，删除它不会释放端口。
	if deletedPod.Spec.NodeName == "" && deletedPod.Status.NominatedNodeName == "" {
		logger.V(4).Info("the deleted pod is unscheduled and it doesn't make the target pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
		return fwk.QueueSkip, nil
	}

	// 被删除的 Pod 不占用宿主端口时，同样没有端口可释放。
	ports := util.GetHostPorts(deletedPod)
	if len(ports) == 0 {
		return fwk.QueueSkip, nil
	}

	// 如果目标 Pod 和被删 Pod 的端口完全不重叠，删掉后者不会解除任何冲突。
	portsInUse := make(fwk.HostPortInfo, len(ports))
	for _, p := range ports {
		portsInUse.Add(p.HostIP, string(p.Protocol), p.HostPort)
	}
	if fitsPorts(util.GetHostPorts(pod), portsInUse) {
		logger.V(4).Info("the deleted pod and the target pod don't have any common port(s), returning QueueSkip as deleting this Pod won't make the Pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
		return fwk.QueueSkip, nil
	}

	logger.V(4).Info("the deleted pod and the target pod have any common port(s), returning Queue as deleting this Pod may make the Pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
	return fwk.Queue, nil
}

// Filter 在 filter 扩展点被调用，检查节点上已占用的端口是否与 Pod 请求的端口冲突。
func (pl *NodePorts) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// 原地扩容被推迟时端口尚未确定，放行由抢占流程统一处理。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	wantPorts, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	fits := fitsPorts(wantPorts, nodeInfo.GetUsedPorts())
	if !fits {
		return fwk.NewStatus(fwk.Unschedulable, ErrReason)
	}

	return nil
}

// Fits 检查 Pod 请求的宿主端口与节点上已占用端口是否冲突。
// 无冲突返回 true（即 Pod 可以调度到该节点），否则返回 false。
func Fits(pod *v1.Pod, nodeInfo fwk.NodeInfo) bool {
	return fitsPorts(util.GetHostPorts(pod), nodeInfo.GetUsedPorts())
}

// fitsPorts 逐个检查 wantPorts 是否与 portsInUse 冲突，全部不冲突才返回 true。
func fitsPorts(wantPorts []v1.ContainerPort, portsInUse fwk.HostPortInfo) bool {
	// 任一端口冲突即判定该节点不可用，不必继续检查剩余端口。
	for _, cp := range wantPorts {
		if portsInUse.CheckConflict(cp.HostIP, string(cp.Protocol), cp.HostPort) {
			return false
		}
	}
	return true
}

// New 根据 feature gate 初始化并返回一个 NodePorts 插件实例。
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &NodePorts{enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption}, nil
}
