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
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/utils/cpuset"
)

// fakeManager 是供测试使用的 CPU Manager 实现，只记录调用不做实际分配。
type fakeManager struct {
	// logger 用于输出方法调用日志。
	logger klog.Logger
	// state 保存测试使用的内存状态。
	state state.State
}

// Start 记录调用并返回成功。
func (m *fakeManager) Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error {
	logger := klog.FromContext(ctx)
	logger.Info("Start()")
	return nil
}

// Policy 返回 none 策略，表示不做 CPU 绑定。
func (m *fakeManager) Policy() Policy {
	m.logger.Info("Policy()")
	// 忽略错误：none 策略构造不会失败。
	pol, _ := NewNonePolicy(nil)
	return pol
}

// Allocate 记录分配请求并返回成功。
func (m *fakeManager) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	logger := klog.FromContext(ctx)
	logger.Info("Allocate", "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)
	return nil
}

// AddContainer 记录新增容器的调用。
func (m *fakeManager) AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	logger.Info("AddContainer", "pod", klog.KObj(pod), "containerName", container.Name, "containerID", containerID)
}

// RemoveContainer 记录删除容器的调用并返回成功。
func (m *fakeManager) RemoveContainer(logger klog.Logger, containerID string) error {
	logger.Info("RemoveContainer", "containerID", containerID)
	return nil
}

// GetTopologyHints 返回空的容器级拓扑提示。
func (m *fakeManager) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger.Info("Get container topology hints", "operation", operation)
	return map[string][]topologymanager.TopologyHint{}
}

// GetPodTopologyHints 返回空的 Pod 级拓扑提示。
func (m *fakeManager) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger.Info("Get pod topology hints", "operation", operation)
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

// GetExclusiveCPUs 返回空的独占 CPU 集合。
func (m *fakeManager) GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet {
	m.logger.Info("GetExclusiveCPUs", "podUID", podUID, "containerName", containerName)
	return cpuset.New()
}

// GetPodCPUs 返回空的 Pod CPU 集合。
func (m *fakeManager) GetPodCPUs(podUID string) cpuset.CPUSet {
	m.logger.Info("GetPodCPUs", "podUID", podUID)
	return cpuset.New()
}

// GetAllocatableCPUs 返回空的可分配 CPU 集合。
func (m *fakeManager) GetAllocatableCPUs() cpuset.CPUSet {
	m.logger.Info("Get Allocatable CPUs")
	return cpuset.New()
}

// GetCPUAffinity 返回空的 CPU 亲和性集合。
func (m *fakeManager) GetCPUAffinity(podUID, containerName string) cpuset.CPUSet {
	m.logger.Info("GetCPUAffinity", "podUID", podUID, "containerName", containerName)
	return cpuset.New()
}

// GetAllCPUs 返回空的 CPU 集合。
func (m *fakeManager) GetAllCPUs() cpuset.CPUSet {
	m.logger.Info("GetAllCPUs")
	return cpuset.New()
}

// GetResourceIsolationLevel 默认按容器级别隔离资源。
func (m *fakeManager) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	return cmqos.ResourceIsolationContainer
}

// NewFakeManager 创建空的测试用 CPU Manager。
func NewFakeManager(logger klog.Logger) Manager {
	logger = klog.LoggerWithName(logger, "cpu.fake")
	return &fakeManager{
		logger: logger,
		state:  state.NewMemoryState(logger),
	}
}
