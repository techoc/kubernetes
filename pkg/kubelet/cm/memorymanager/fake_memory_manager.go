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

package memorymanager

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
)

// fakeManager 是供测试使用的内存管理器实现，只记录调用不做实际分配。
type fakeManager struct {
	// state 保存测试使用的内存状态。
	state state.State
}

// Start 记录调用并返回成功。
func (m *fakeManager) Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error {
	logger := klog.FromContext(ctx)
	logger.Info("Start()")
	return nil
}

// Policy 返回 none 策略，表示不做内存绑定。
func (m *fakeManager) Policy(logger klog.Logger) Policy {
	logger.Info("Policy()")
	return NewPolicyNone(logger)
}

// Allocate 记录分配请求并返回成功。
func (m *fakeManager) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	logger.Info("Allocate")
	return nil
}

// AddContainer 记录新增容器的调用。
func (m *fakeManager) AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	logger.Info("Add container", "pod", klog.KObj(pod), "containerName", container.Name, "containerID", containerID)
}

// GetMemoryNUMANodes 返回 nil，表示没有绑定的 NUMA 节点。
func (m *fakeManager) GetMemoryNUMANodes(logger klog.Logger, pod *v1.Pod, container *v1.Container) sets.Set[int] {
	logger.Info("Get MemoryNUMANodes", "pod", klog.KObj(pod), "containerName", container.Name)
	return nil
}

// RemoveContainer 记录删除容器的调用并返回成功。
func (m *fakeManager) RemoveContainer(logger klog.Logger, containerID string) error {
	logger.Info("RemoveContainer", "containerID", containerID)
	return nil
}

// GetTopologyHints 返回空的容器级拓扑提示。
func (m *fakeManager) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger.Info("GetTopologyHints", "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	return map[string][]topologymanager.TopologyHint{}
}

// GetPodTopologyHints 返回空的 Pod 级拓扑提示。
func (m *fakeManager) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger.Info("GetPodTopologyHints", "pod", klog.KObj(pod), "operation", operation)
	return map[string][]topologymanager.TopologyHint{}
}

// AllocatePod 记录 Pod 级分配请求并返回成功。
func (m *fakeManager) AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error {
	logger.Info("AllocatePod", "pod", klog.KObj(pod), "operation", operation)
	return nil
}

// State 返回内部状态读取器。
func (m *fakeManager) State() state.Reader {
	return m.state
}

// GetAllocatableMemory 返回每个 NUMA 节点的可分配内存量。
func (m *fakeManager) GetAllocatableMemory(logger klog.Logger) []state.Block {
	logger.Info("Get Allocatable Memory")
	return []state.Block{}
}

// GetMemory 返回容器从各 NUMA 节点分配到的内存。
func (m *fakeManager) GetMemory(logger klog.Logger, podUID, containerName string) []state.Block {
	logger = klog.LoggerWithValues(logger, "podUID", podUID, "containerName", containerName)
	logger.Info("Get Memory")
	return []state.Block{}
}

// GetPodMemory 返回 Pod 从各 NUMA 节点分配到的内存。
func (m *fakeManager) GetPodMemory(podUID string) []state.Block {
	logger := klog.LoggerWithValues(klog.TODO(), "podUID", podUID)
	logger.Info("Get Pod Memory")
	return []state.Block{}
}

// GetResourceIsolationLevel 默认按容器级别隔离资源。
func (m *fakeManager) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	return cmqos.ResourceIsolationContainer
}

// NewFakeManager 创建空的测试用内存管理器。
func NewFakeManager(logger klog.Logger) Manager {
	logger = klog.LoggerWithName(logger, "memory-mgr.fake")
	return &fakeManager{
		state: state.NewMemoryState(logger),
	}
}
