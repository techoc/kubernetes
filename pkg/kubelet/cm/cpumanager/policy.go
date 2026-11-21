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
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/utils/cpuset"
)

// Policy 实现 Pod 容器到 CPU 的分配逻辑。
type Policy interface {
	// Name 返回策略名称。
	Name() string
	// Start 初始化策略并加载状态。
	Start(logger klog.Logger, s state.State) error
	// Allocate 调用是幂等的。
	Allocate(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error
	// RemoveContainer 调用是幂等的。
	RemoveContainer(logger klog.Logger, s state.State, podUID string, containerName string) error
	// GetTopologyHints 实现 topologymanager.HintProvider 接口，
	// 用于与其他资源控制器共同实现 NUMA 感知的资源对齐。
	GetTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint
	// GetPodTopologyHints 实现 topologymanager.HintProvider 接口，
	// 用于与其他资源控制器共同实现 Pod 级的 NUMA 感知资源对齐。
	GetPodTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint
	// AllocatePod 用于触发为 Pod 分配 CPU。
	AllocatePod(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) error
	// GetAllocatableCPUs 返回可供分配的全部 CPU 集合。
	GetAllocatableCPUs(m state.State) cpuset.CPUSet
}
