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

package nodename

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// NodeName 是一个检查 Pod 的 spec.nodeName 是否与当前节点匹配的插件。
type NodeName struct{}

var _ fwk.PreFilterPlugin = &NodeName{}
var _ fwk.FilterPlugin = &NodeName{}
var _ fwk.EnqueueExtensions = &NodeName{}
var _ fwk.SignPlugin = &NodeName{}

const (
	// Name 是插件在插件注册表和配置中使用的名字。
	Name = names.NodeName

	// ErrReason 是节点名不匹配时返回的原因。
	ErrReason = "node(s) didn't match the requested node name"
)

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 变为可调度的事件。
func (pl *NodeName) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 这里不需要 QueueingHintFn：该事件发生时 Pod 的调度总会带退避重试。
		// （效果等同于 Queue）
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
	}, nil
}

// Name 返回插件名称，用于日志等场景。
func (pl *NodeName) Name() string {
	return Name
}

// SignPod 根据 NodeName 字段生成签名片段。
// 节点的可行性与打分都取决于 NodeName 字段。
func (pl *NodeName) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	return []fwk.SignFragment{
		{Key: fwk.NodeNameSignerName, Value: pod.Spec.NodeName},
	}, nil
}

// PreFilter 在 prefilter 扩展点调用。
func (pl *NodeName) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pod.Spec.NodeName != "" {
		// Pod 已经在 spec.nodeName 里指定了目标节点（例如延迟调整大小的已分配 Pod，或静态节点分配）。
		// 只在该节点上评估：其他节点会被判为 UnschedulableAndUnresolvable，因而不会成为抢占目标。
		return &fwk.PreFilterResult{
			NodeNames: sets.New(pod.Spec.NodeName),
		}, nil
	}
	return nil, nil
}

// PreFilterExtensions 返回 prefilter 扩展；本插件没有，返回 nil。
func (pl *NodeName) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 在 filter 扩展点调用。
func (pl *NodeName) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {

	if !Fits(pod, nodeInfo) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReason)
	}
	return nil
}

// Fits 真正检查 Pod 是否适配该节点。
func Fits(pod *v1.Pod, nodeInfo fwk.NodeInfo) bool {
	return len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName == nodeInfo.Node().Name
}

// New 初始化并返回一个新的插件实例。
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, _ feature.Features) (fwk.Plugin, error) {
	return &NodeName{}, nil
}
