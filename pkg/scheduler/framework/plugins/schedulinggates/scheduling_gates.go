/*
Copyright 2022 The Kubernetes Authors.

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

package schedulinggates

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// Name 是插件在插件注册表和配置中使用的名字。
const Name = names.SchedulingGates

// SchedulingGates 检查 Pod 是否带有 .spec.schedulingGates。
type SchedulingGates struct{}

var _ fwk.PreEnqueuePlugin = &SchedulingGates{}
var _ fwk.EnqueueExtensions = &SchedulingGates{}

// Name 返回插件名称，用于日志等场景。
func (pl *SchedulingGates) Name() string {
	return Name
}

// PreEnqueue 在入队前检查 Pod 是否仍被调度门禁拦住；
// 只要还有门禁未移除，就把 Pod 判为不可调度且不可通过抢占解决。
func (pl *SchedulingGates) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	if len(p.Spec.SchedulingGates) == 0 {
		return nil
	}
	gates := make([]string, 0, len(p.Spec.SchedulingGates))
	for _, gate := range p.Spec.SchedulingGates {
		gates = append(gates, gate.Name)
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for scheduling gates: %v", gates))
}

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 变为可调度的事件。
func (pl *SchedulingGates) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 门禁移除后 Pod 就有可能变得可调度
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodSchedulingGatesEliminated}, QueueingHintFn: pl.isSchedulableAfterUpdateTargetPodSchedulingGatesEliminated},
	}, nil
}

// New 初始化并返回一个新的插件实例。
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, _ feature.Features) (fwk.Plugin, error) {
	return &SchedulingGates{}, nil
}

// isSchedulableAfterUpdateTargetPodSchedulingGatesEliminated 在目标 Pod 的门禁被移除后返回入队提示。
// 该事件只由门禁移除触发，直接入队即可。
func (pl *SchedulingGates) isSchedulableAfterUpdateTargetPodSchedulingGatesEliminated(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	return fwk.Queue, nil
}
