/*
Copyright 2017 The Kubernetes Authors.

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

package cpumanager

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/utils/cpuset"
)

// nonePolicy 是不做任何 CPU 绑定的策略实现。
type nonePolicy struct {
}

// 编译期断言：nonePolicy 实现了 Policy 接口。
var _ Policy = &nonePolicy{}

// PolicyNone 是 none 策略的名称。
const PolicyNone policyName = "none"

// NewNonePolicy 返回不做任何操作的 CPU Manager 策略。
func NewNonePolicy(cpuPolicyOptions map[string]string) (Policy, error) {
	// none 策略不接受任何选项，非空即报错。
	if len(cpuPolicyOptions) > 0 {
		return nil, fmt.Errorf("None policy: received unsupported options=%v", cpuPolicyOptions)
	}
	return &nonePolicy{}, nil
}

// Name 返回策略名称。
func (p *nonePolicy) Name() string {
	return string(PolicyNone)
}

// Start 记录调用并返回成功。
func (p *nonePolicy) Start(logger klog.Logger, s state.State) error {
	logger.Info("Start")
	return nil
}

// Allocate 不做任何 CPU 分配。
func (p *nonePolicy) Allocate(_ klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) error {
	return nil
}

// RemoveContainer 不做任何清理。
func (p *nonePolicy) RemoveContainer(_ klog.Logger, s state.State, podUID string, containerName string) error {
	return nil
}

// GetTopologyHints 返回 nil，表示不提供拓扑提示。
func (p *nonePolicy) GetTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// GetPodTopologyHints 返回 nil，表示不提供 Pod 级拓扑提示。
func (p *nonePolicy) GetPodTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// AllocatePod 不做任何 Pod 级 CPU 分配。
func (p *nonePolicy) AllocatePod(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) error {
	return nil
}

// 可分配 CPU 指可以独占分配给满足独占条件（Guaranteed QoS 且 CPU 请求为整数）的 Pod 的 CPU。
// 「CPU 可分配性」只对 static 策略有意义，即工作负载能够独占核心的场景。
// 因此这里返回空集合：none 策略下没有 CPU 符合上述可分配定义。
func (p *nonePolicy) GetAllocatableCPUs(m state.State) cpuset.CPUSet {
	return cpuset.New()
}
