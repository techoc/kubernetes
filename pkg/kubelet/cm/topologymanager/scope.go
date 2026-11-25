/*
Copyright 2020 The Kubernetes Authors.

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

package topologymanager

import (
	"context"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

const (
	// 按容器划分的 TopologyManagerScope。
	ContainerTopologyScope = "container"
	// 按 Pod 划分的 TopologyManagerScope。
	PodTopologyScope = "pod"
	// 拓扑策略为 none 时的 TopologyManagerScope。
	NoneTopologyScope = "none"
)

// podTopologyHints 保存 PodUID -> 容器名 -> TopologyHint 的映射。
type podTopologyHints map[string]map[string]TopologyHint

// Scope 是 Topology Manager 的作用域接口。
type Scope interface {
	// 返回作用域名称。
	Name() string
	// 返回当前拓扑策略。
	GetPolicy() Policy
	// 执行 Pod 准入并返回准入结果。
	Admit(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult
	// 向 manager 添加 hint provider，表示做拓扑决策时需要征询它。
	AddHintProvider(logger klog.Logger, h HintProvider)
	// 把 Pod 加入 manager 以便跟踪。
	AddContainer(pod *v1.Pod, container *v1.Container, containerID string)
	// 从 manager 跟踪中移除 Pod。
	RemoveContainer(logger klog.Logger, containerID string) error
	// 保存 Pod 拓扑 hint 的接口。
	Store
}

// scope 是各作用域共用的基础实现，mutex 保护 hint 映射与 podMap。
type scope struct {
	mutex sync.Mutex
	name  string
	// Pod 内各容器的 TopologyHint 映射，按 PodUID -> 容器名索引。
	podTopologyHints podTopologyHints
	// 注册到 manager 的组件列表。
	hintProviders []HintProvider
	// Topology Manager 策略。
	policy Policy
	// (PodUID, 容器名) 到 ContainerID 的映射，用于增删 PodTopologyHints。
	podMap containermap.ContainerMap
}

// Name 返回作用域名称。
func (s *scope) Name() string {
	return s.name
}

// getTopologyHints 在锁保护下读取指定容器的 hint。
func (s *scope) getTopologyHints(podUID string, containerName string) TopologyHint {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.podTopologyHints[podUID][containerName]
}

// setTopologyHints 在锁保护下写入 hint；必要时初始化内层 map。
func (s *scope) setTopologyHints(podUID string, containerName string, th TopologyHint) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.podTopologyHints[podUID] == nil {
		s.podTopologyHints[podUID] = make(map[string]TopologyHint)
	}
	s.podTopologyHints[podUID][containerName] = th
}

// GetAffinity 实现 Store 接口，返回已保存的 hint。
func (s *scope) GetAffinity(_ klog.Logger, podUID string, containerName string) TopologyHint {
	return s.getTopologyHints(podUID, containerName)
}

// GetPolicy 返回当前拓扑策略。
func (s *scope) GetPolicy() Policy {
	return s.policy
}

// AddHintProvider 注册 hint provider。
func (s *scope) AddHintProvider(_ klog.Logger, h HintProvider) {
	s.hintProviders = append(s.hintProviders, h)
}

// 该函数更适合放在 topologymanager 中，但 topologymanager 已不再跟踪映射。
// AddContainer 记录 Pod、容器与 ContainerID 的映射。
func (s *scope) AddContainer(pod *v1.Pod, container *v1.Container, containerID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.podMap.Add(string(pod.UID), container.Name, containerID)
}

// 同上：映射跟踪保留在 scope 中。
// RemoveContainer 移除容器映射，并在确认没有新实例复用同一 Pod/容器时清理 hint。
func (s *scope) RemoveContainer(logger klog.Logger, containerID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	logger.Info("RemoveContainer", "containerID", containerID)
	// 获取与待移除 containerID 关联的 podUID 和 containerName，并将其移除。
	// 找不到映射说明容器从未登记，直接返回成功。
	podUIDString, containerName, err := s.podMap.GetContainerRef(containerID)
	if err != nil {
		return nil
	}
	s.podMap.RemoveByContainerID(containerID)

	// 容器重启后，同一 Pod/容器名可能已关联到新的 ContainerID；
	// 只有在没有新映射时才删除对应的 TopologyHints，避免误删新实例的 hint。
	if _, err := s.podMap.GetContainerID(podUIDString, containerName); err != nil {
		delete(s.podTopologyHints[podUIDString], containerName)
		if len(s.podTopologyHints[podUIDString]) == 0 {
			delete(s.podTopologyHints, podUIDString)
		}
	}

	return nil
}

// admitPolicyNone 在策略为 none 时仅执行资源分配，不做亲和性检查。
func (s *scope) admitPolicyNone(ctx context.Context, pod *v1.Pod, operation lifecycle.Operation) lifecycle.PodAdmitResult {
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		err := s.allocateAlignedResources(ctx, pod, &container, operation)
		if err != nil {
			return admission.GetPodAdmitResult(err)
		}
	}
	return admission.GetPodAdmitResult(nil)
}

// 该函数更适合放在 topologymanager 中，但 topologymanager 已不再跟踪 provider。
// allocateAlignedResources 依次调用所有 hint provider 完成容器级资源分配。
func (s *scope) allocateAlignedResources(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	for _, provider := range s.hintProviders {
		err := provider.Allocate(ctx, pod, container, operation)
		if err != nil {
			return err
		}
	}
	return nil
}

// allocatePodAlignedResources 依次调用所有 hint provider 完成 Pod 级资源分配。
func (s *scope) allocatePodAlignedResources(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error {
	for _, provider := range s.hintProviders {
		err := provider.AllocatePod(logger, pod, operation)
		if err != nil {
			return err
		}
	}
	return nil
}
