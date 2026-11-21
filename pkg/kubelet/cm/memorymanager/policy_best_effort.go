/*
Copyright 2024 The Kubernetes Authors.

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

package memorymanager

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	v1 "k8s.io/api/core/v1"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// 在 Windows 上希望复用 StaticPolicy 的内存拓扑提示计算逻辑，
// 但与 Linux 不同，Windows API 无法直接绑定或保证 NUMA 节点
// （Windows 调度器会使用离所分配 CPU 最近的 NUMA 节点，因此 NUMA 绑定只能尽力而为）。
// 因此不允许用户通过 kubelet 配置为内存管理器指定 "StaticPolicy"，
// 而是使用 "BestEffort" 策略：它复用 StaticPolicy 的逻辑以减少重复代码。
const policyTypeBestEffort policyType = "BestEffort"

// bestEffortPolicy 是 BestEffort 策略的实现。
type bestEffortPolicy struct {
	static *staticPolicy
	// affinity 与静态策略使用同一个 Store，但以 AuthoritativeStore 类型保存，
	// 以便判断容器的提示是否具有权威性（Windows 上容器跟随 CPU Manager 的
	// NUMA 决策），从而禁止把提示扩展到额外的 NUMA 节点。
	affinity topologymanager.AuthoritativeStore
}

// 编译期断言：bestEffortPolicy 实现了 Policy 接口。
var _ Policy = &bestEffortPolicy{}

// NewPolicyBestEffort 创建 BestEffort 内存策略，内部复用静态策略。
func NewPolicyBestEffort(logger klog.Logger, machineInfo *cadvisorapi.MachineInfo, reserved systemReservedMemory, affinity topologymanager.AuthoritativeStore) (Policy, error) {
	// 复用静态策略的拓扑提示与分配逻辑。
	p, err := NewPolicyStatic(logger, machineInfo, reserved, affinity)

	if err != nil {
		return nil, err
	}

	return &bestEffortPolicy{
		static:   p.(*staticPolicy),
		affinity: affinity,
	}, nil
}

// Name 返回策略名称。
func (p *bestEffortPolicy) Name() string {
	return string(policyTypeBestEffort)
}

// Start 委托给内部静态策略。
func (p *bestEffortPolicy) Start(logger klog.Logger, s state.State) error {
	return p.static.Start(logger, s)
}

// Allocate 委托静态策略完成内存分配。
func (p *bestEffortPolicy) Allocate(ctx context.Context, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) (rerr error) {
	// Windows 上，当容器的亲和性提示具有权威性（容器拥有独占 CPU）时，
	// 容器会跟随 CPU Manager 的 NUMA 决策。此时静态策略不得把提示扩展到
	// 额外的 NUMA 节点（内存位置跟随 CPU 亲和性），因此通过 skipExtend 告知。
	// 否则静态策略按常规扩展提示。
	// 提示具有权威性时禁止扩展 NUMA 节点。
	p.static.skipExtend = p.affinity.IsHintAuthoritative(string(pod.UID), container.Name)
	return p.static.Allocate(ctx, s, pod, container, operation)
}

// RemoveContainer 委托静态策略移除容器状态。
func (p *bestEffortPolicy) RemoveContainer(logger klog.Logger, s state.State, podUID string, containerName string) {
	p.static.RemoveContainer(logger, s, podUID, containerName)
}

// GetPodTopologyHints 在 Windows 上不支持 Pod 级资源，返回 nil。
func (p *bestEffortPolicy) GetPodTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// Windows 不支持 Pod 级资源。
	return nil
}

// GetTopologyHints 委托静态策略计算容器级拓扑提示。
func (p *bestEffortPolicy) GetTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return p.static.GetTopologyHints(logger, s, pod, container, operation)
}

// GetAllocatableMemory 委托静态策略返回可分配内存。
func (p *bestEffortPolicy) GetAllocatableMemory(s state.State) []state.Block {
	return p.static.GetAllocatableMemory(s)
}

// AllocatePod 在 Windows 上不支持 Pod 级资源，直接返回成功。
func (p *bestEffortPolicy) AllocatePod(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) error {
	// Windows 不支持 Pod 级资源。
	return nil
}
