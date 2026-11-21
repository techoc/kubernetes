/*
Copyright 2021 The Kubernetes Authors.

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
	"sync"

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

// FakeContainerManager 是 ContainerManager 的测试实现，
// 在 CalledFunctions 中记录被调用的方法名。
type FakeContainerManager struct {
	sync.Mutex
	CalledFunctions                     []string
	PodContainerManager                 *FakePodContainerManager
	shouldResetExtendedResourceCapacity bool
	nodeConfig                          NodeConfig
	cpuManager                          cpumanager.Manager
	memoryManager                       memorymanager.Manager
}

// 编译期断言：FakeContainerManager 实现了 ContainerManager 接口。
var _ ContainerManager = &FakeContainerManager{}

// NewFakeContainerManager 创建带 fake Pod/CPU/内存管理器的测试实例。
func NewFakeContainerManager(logger klog.Logger) *FakeContainerManager {
	return &FakeContainerManager{
		PodContainerManager: NewFakePodContainerManager(),
		cpuManager:          cpumanager.NewFakeManager(logger),
		memoryManager:       memorymanager.NewFakeManager(logger),
	}
}

// NewFakeContainerManagerWithNodeConfig 创建并注入指定 NodeConfig 的测试实例。
func NewFakeContainerManagerWithNodeConfig(nodeConfig NodeConfig) *FakeContainerManager {
	return &FakeContainerManager{
		PodContainerManager: NewFakePodContainerManager(),
		nodeConfig:          nodeConfig,
	}
}

// Start 记录调用并返回 nil。
func (cm *FakeContainerManager) Start(_ context.Context, _ *v1.Node, _ ActivePodsFunc, _ GetNodeFunc, _ config.SourcesReady, _ status.PodStatusProvider, _ internalapi.RuntimeService, _ bool) error {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "Start")
	return nil
}

// SystemCgroupsLimit 返回空资源列表。
func (cm *FakeContainerManager) SystemCgroupsLimit() v1.ResourceList {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "SystemCgroupsLimit")
	return v1.ResourceList{}
}

// GetNodeConfig 返回创建时注入的 NodeConfig。
func (cm *FakeContainerManager) GetNodeConfig() NodeConfig {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetNodeConfig")
	return cm.nodeConfig
}

// GetMountedSubsystems 返回空的 cgroup 子系统集合。
func (cm *FakeContainerManager) GetMountedSubsystems() *CgroupSubsystems {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetMountedSubsystems")
	return &CgroupSubsystems{}
}

// GetQOSContainersInfo 返回空 QoS cgroup 信息。
func (cm *FakeContainerManager) GetQOSContainersInfo() QOSContainersInfo {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "QOSContainersInfo")
	return QOSContainersInfo{}
}

// UpdateQOSCgroups 记录调用并返回 nil。
func (cm *FakeContainerManager) UpdateQOSCgroups(logger klog.Logger) error {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "UpdateQOSCgroups")
	return nil
}

// Status 返回空状态。
func (cm *FakeContainerManager) Status() Status {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "Status")
	return Status{}
}

// GetNodeAllocatableReservation 返回 nil，表示没有预留。
func (cm *FakeContainerManager) GetNodeAllocatableReservation() v1.ResourceList {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetNodeAllocatableReservation")
	return nil
}

// GetCapacity 在启用本地存储隔离时返回 0 的 ephemeral-storage，否则返回空列表。
func (cm *FakeContainerManager) GetCapacity(_ klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetCapacity")
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

// GetPluginRegistrationHandlers 返回 nil。
func (cm *FakeContainerManager) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPluginRegistrationHandlers")
	return nil
}

// GetHealthCheckers 返回空的健康检查器列表。
func (cm *FakeContainerManager) GetHealthCheckers() []healthz.HealthChecker {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPluginRegistrationServerChecker")
	return []healthz.HealthChecker{}
}

// GetDevicePluginResourceCapacity 返回空的容量、可分配量和设备列表。
func (cm *FakeContainerManager) GetDevicePluginResourceCapacity(_ klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetDevicePluginResourceCapacity")
	return nil, nil, []string{}
}

// NewPodContainerManager 返回 fake Pod cgroup 管理器。
func (cm *FakeContainerManager) NewPodContainerManager() PodContainerManager {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "PodContainerManager")
	return cm.PodContainerManager
}

// GetResources 返回空的容器运行选项。
func (cm *FakeContainerManager) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetResources")
	return &kubecontainer.RunContainerOptions{}, nil
}

// UpdatePluginResources 记录调用并返回 nil。
func (cm *FakeContainerManager) UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "UpdatePluginResources")
	return nil
}

// InternalContainerLifecycle 返回基于 fake CPU/内存/拓扑管理器的实现。
func (cm *FakeContainerManager) InternalContainerLifecycle(logger klog.Logger) InternalContainerLifecycle {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "InternalContainerLifecycle")
	return &internalContainerLifecycleImpl{cm.cpuManager, cm.memoryManager, topologymanager.NewFakeManager(logger)}
}

// GetPodCgroupRoot 返回空路径。
func (cm *FakeContainerManager) GetPodCgroupRoot() string {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPodCgroupRoot")
	return ""
}

// GetDevices 返回 nil。
func (cm *FakeContainerManager) GetDevices(_, _ string) []*podresourcesapi.ContainerDevices {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetDevices")
	return nil
}

// GetAllocatableDevices 返回 nil。
func (cm *FakeContainerManager) GetAllocatableDevices(_ klog.Logger) []*podresourcesapi.ContainerDevices {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetAllocatableDevices")
	return nil
}

// ShouldResetExtendedResourceCapacity 返回创建时设置的标志。
func (cm *FakeContainerManager) ShouldResetExtendedResourceCapacity() bool {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "ShouldResetExtendedResourceCapacity")
	return cm.shouldResetExtendedResourceCapacity
}

// GetAllocateResourcesPodAdmitHandler 返回 fake 拓扑管理器作为准入处理器。
func (cm *FakeContainerManager) GetAllocateResourcesPodAdmitHandler(logger klog.Logger) lifecycle.PodAdmitHandler {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetAllocateResourcesPodAdmitHandler")
	return topologymanager.NewFakeManager(logger)
}

// UpdateAllocatedDevices 记录调用，无实际动作。
func (cm *FakeContainerManager) UpdateAllocatedDevices(_ klog.Logger) {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "UpdateAllocatedDevices")
	return
}

// GetCPUs 返回 nil。
func (cm *FakeContainerManager) GetCPUs(pod *v1.Pod, container *v1.Container) []int64 {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetCPUs")
	return nil
}

// GetPodCPUs 返回 nil。
func (cm *FakeContainerManager) GetPodCPUs(_ string) []int64 {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPodCPUs")
	return nil
}

// GetAllocatableCPUs 返回 nil。
func (cm *FakeContainerManager) GetAllocatableCPUs() []int64 {
	cm.Lock()
	defer cm.Unlock()
	return nil
}

// GetMemory 返回 nil。
func (cm *FakeContainerManager) GetMemory(_ klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.ContainerMemory {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetMemory")
	return nil
}

// GetPodMemory 返回 nil。
func (cm *FakeContainerManager) GetPodMemory(_ klog.Logger, _ string) []*podresourcesapi.ContainerMemory {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPodMemory")
	return nil
}

// GetAllocatableMemory 返回 nil。
func (cm *FakeContainerManager) GetAllocatableMemory(_ klog.Logger) []*podresourcesapi.ContainerMemory {
	cm.Lock()
	defer cm.Unlock()
	return nil
}

// GetDynamicResources 返回 nil。
func (cm *FakeContainerManager) GetDynamicResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	return nil
}

// GetNodeAllocatableAbsolute 返回固定的 4 CPU、4Gi 内存和 40 个 Pod。
func (cm *FakeContainerManager) GetNodeAllocatableAbsolute() v1.ResourceList {
	cm.Lock()
	defer cm.Unlock()
	return v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("4Gi"),
		v1.ResourcePods:   *resource.NewQuantity(40, resource.DecimalSI),
	}
}

// PrepareDynamicResources 为空操作。
func (cm *FakeContainerManager) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// UnprepareDynamicResources 为空操作。
func (cm *FakeContainerManager) UnprepareDynamicResources(context.Context, *v1.Pod) error {
	return nil
}

// PodMightNeedToUnprepareResources 始终返回 false。
func (cm *FakeContainerManager) PodMightNeedToUnprepareResources(UID types.UID) bool {
	return false
}

// UpdateAllocatedResourcesStatus 为空操作。
func (cm *FakeContainerManager) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {
}

// Updates 返回 nil channel。
func (cm *FakeContainerManager) Updates() <-chan resourceupdates.Update {
	return nil
}

// PodHasExclusiveCPUs 始终返回 false。
func (cm *FakeContainerManager) PodHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod) bool {
	return false
}

// ContainerHasExclusiveCPUs 始终返回 false。
func (cm *FakeContainerManager) ContainerHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) bool {
	return false
}
