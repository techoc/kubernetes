/*
Copyright 2015 The Kubernetes Authors.

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

package cm

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	internalapi "k8s.io/cri-api/pkg/apis"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// containerManagerStub 是 ContainerManager 的空实现，主要用于测试与不需要容器管理器的场景。
type containerManagerStub struct {
	shouldResetExtendedResourceCapacity bool
	extendedPluginResources             v1.ResourceList
	memoryManager                       memorymanager.Manager
	cpuManager                          cpumanager.Manager
}

// 编译期断言：containerManagerStub 必须实现 ContainerManager 接口。
var _ ContainerManager = &containerManagerStub{}

// Start 启动 stub 容器管理器，仅初始化假的 CPU/内存管理器。
func (cm *containerManagerStub) Start(ctx context.Context, _ *v1.Node, _ ActivePodsFunc, _ GetNodeFunc, _ config.SourcesReady, _ status.PodStatusProvider, _ internalapi.RuntimeService, _ bool) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Starting stub container manager")
	cm.memoryManager = memorymanager.NewFakeManager(logger)
	cm.cpuManager = cpumanager.NewFakeManager(logger)
	return nil
}

// SystemCgroupsLimit 返回空的系统 cgroup 资源限制。
func (cm *containerManagerStub) SystemCgroupsLimit() v1.ResourceList {
	return v1.ResourceList{}
}

// GetNodeConfig 返回空的节点配置。
func (cm *containerManagerStub) GetNodeConfig() NodeConfig {
	return NodeConfig{}
}

// GetMountedSubsystems 返回空的 cgroup 子系统集合。
func (cm *containerManagerStub) GetMountedSubsystems() *CgroupSubsystems {
	return &CgroupSubsystems{}
}

// GetQOSContainersInfo 返回空的 QoS 容器信息。
func (cm *containerManagerStub) GetQOSContainersInfo() QOSContainersInfo {
	return QOSContainersInfo{}
}

// UpdateQOSCgroups 是空操作。
func (cm *containerManagerStub) UpdateQOSCgroups(logger klog.Logger) error {
	return nil
}

// Status 返回空的容器管理器状态。
func (cm *containerManagerStub) Status() Status {
	return Status{}
}

// GetNodeAllocatableReservation 返回 nil，表示没有预留资源。
func (cm *containerManagerStub) GetNodeAllocatableReservation() v1.ResourceList {
	return nil
}

// GetCapacity 返回节点容量；未开启本地存储容量隔离时返回空列表。
func (cm *containerManagerStub) GetCapacity(_ klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList {
	if !localStorageCapacityIsolation {
		return v1.ResourceList{}
	}
	c := v1.ResourceList{
		v1.ResourceEphemeralStorage: *resource.NewQuantity(
			int64(0),
			resource.BinarySI),
	}
	return c
}

// GetPluginRegistrationHandlers 返回 nil，stub 不注册任何设备插件处理器。
func (cm *containerManagerStub) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	return nil
}

// GetHealthCheckers 返回空的健康检查器列表。
func (cm *containerManagerStub) GetHealthCheckers() []healthz.HealthChecker {
	return []healthz.HealthChecker{}
}

// GetDevicePluginResourceCapacity 返回扩展资源容量，stub 中容量与可分配量相同。
func (cm *containerManagerStub) GetDevicePluginResourceCapacity(_ klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
	return cm.extendedPluginResources, cm.extendedPluginResources, []string{}
}

// GetPodCgroupConfig 未实现，始终返回错误。
func (m *podContainerManagerStub) GetPodCgroupConfig(_ *v1.Pod, _ v1.ResourceName) (*ResourceConfig, error) {
	return nil, fmt.Errorf("not implemented")
}

// SetPodCgroupConfig 未实现，始终返回错误。
func (m *podContainerManagerStub) SetPodCgroupConfig(logger klog.Logger, pod *v1.Pod, resourceConfig *ResourceConfig) error {
	return fmt.Errorf("not implemented")
}

// NewPodContainerManager 返回空的 Pod 容器管理器实现。
func (cm *containerManagerStub) NewPodContainerManager() PodContainerManager {
	return &podContainerManagerStub{}
}

// GetResources 返回空的容器运行选项。
func (cm *containerManagerStub) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	return &kubecontainer.RunContainerOptions{}, nil
}

// UpdatePluginResources 是空操作。
func (cm *containerManagerStub) UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	return nil
}

// InternalContainerLifecycle 返回使用假 CPU/内存/拓扑管理器实现的容器生命周期钩子。
func (cm *containerManagerStub) InternalContainerLifecycle(logger klog.Logger) InternalContainerLifecycle {
	return &internalContainerLifecycleImpl{cm.cpuManager, cm.memoryManager, topologymanager.NewFakeManager(logger)}
}

// GetPodCgroupRoot 返回空字符串。
func (cm *containerManagerStub) GetPodCgroupRoot() string {
	return ""
}

// GetDevices 返回 nil，stub 不管理设备。
func (cm *containerManagerStub) GetDevices(_, _ string) []*podresourcesapi.ContainerDevices {
	return nil
}

// GetAllocatableDevices 返回 nil。
func (cm *containerManagerStub) GetAllocatableDevices(logger klog.Logger) []*podresourcesapi.ContainerDevices {
	return nil
}

// ShouldResetExtendedResourceCapacity 返回构造时配置的标志。
func (cm *containerManagerStub) ShouldResetExtendedResourceCapacity() bool {
	return cm.shouldResetExtendedResourceCapacity
}

// GetAllocateResourcesPodAdmitHandler 返回假的拓扑管理器作为准入处理器。
func (cm *containerManagerStub) GetAllocateResourcesPodAdmitHandler(logger klog.Logger) lifecycle.PodAdmitHandler {
	return topologymanager.NewFakeManager(logger)
}

// UpdateAllocatedDevices 是空操作。
func (cm *containerManagerStub) UpdateAllocatedDevices(_ klog.Logger) {
	return
}

// GetCPUs 返回 nil，stub 不分配 CPU。
func (cm *containerManagerStub) GetCPUs(pod *v1.Pod, container *v1.Container) []int64 {
	return nil
}

// GetPodCPUs 返回 nil。
func (cm *containerManagerStub) GetPodCPUs(_ string) []int64 {
	return nil
}

// GetAllocatableCPUs 返回 nil。
func (cm *containerManagerStub) GetAllocatableCPUs() []int64 {
	return nil
}

// GetMemory 返回 nil，stub 不分配内存。
func (cm *containerManagerStub) GetMemory(_ klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetPodMemory 返回 nil。
func (cm *containerManagerStub) GetPodMemory(_ klog.Logger, _ string) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetAllocatableMemory 返回 nil。
func (cm *containerManagerStub) GetAllocatableMemory(_ klog.Logger) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetDynamicResources 返回 nil，stub 不管理动态资源。
func (cm *containerManagerStub) GetDynamicResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	return nil
}

// GetNodeAllocatableAbsolute 返回 nil。
func (cm *containerManagerStub) GetNodeAllocatableAbsolute() v1.ResourceList {
	return nil
}

// PrepareDynamicResources 是空操作。
func (cm *containerManagerStub) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// UnprepareDynamicResources 是空操作。
func (cm *containerManagerStub) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// PodMightNeedToUnprepareResources 始终返回 false。
func (cm *containerManagerStub) PodMightNeedToUnprepareResources(UID types.UID) bool {
	return false
}

// UpdateAllocatedResourcesStatus 是空操作。
func (cm *containerManagerStub) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {
}

// Updates 返回 nil，stub 不产生资源更新事件。
func (cm *containerManagerStub) Updates() <-chan resourceupdates.Update {
	return nil
}

// PodHasExclusiveCPUs 始终返回 false。
func (cm *containerManagerStub) PodHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod) bool {
	return false
}

// ContainerHasExclusiveCPUs 始终返回 false。
func (cm *containerManagerStub) ContainerHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) bool {
	return false
}

// NewStubContainerManager 创建默认的 stub 容器管理器。
func NewStubContainerManager() ContainerManager {
	return &containerManagerStub{shouldResetExtendedResourceCapacity: false}
}

// NewStubContainerManagerWithExtendedResource 创建可配置是否重置扩展资源容量的 stub 容器管理器。
func NewStubContainerManagerWithExtendedResource(shouldResetExtendedResourceCapacity bool) ContainerManager {
	return &containerManagerStub{shouldResetExtendedResourceCapacity: shouldResetExtendedResourceCapacity}
}

// NewStubContainerManagerWithDevicePluginResource 创建带设备插件扩展资源的 stub 容器管理器。
func NewStubContainerManagerWithDevicePluginResource(extendedPluginResources v1.ResourceList) ContainerManager {
	return &containerManagerStub{
		shouldResetExtendedResourceCapacity: false,
		extendedPluginResources:             extendedPluginResources,
	}
}
