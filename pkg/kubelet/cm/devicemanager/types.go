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

package devicemanager

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Manager 管理节点上运行的所有设备插件。
type Manager interface {
	// Start 启动设备插件注册服务。
	Start(logger klog.Logger, activePods ActivePodsFunc, sourcesReady config.SourcesReady, initialContainers containermap.ContainerMap, initialContainerRunningSet sets.Set[string]) error

	// Allocate 为 Pod 中的容器配置并分配设备。
	// 根据请求的设备资源，Allocate 会与设备插件通信完成准备工作，
	// 并取得使用设备所需的运行时设置（环境变量、挂载点和设备文件）。
	Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error

	// UpdatePluginResources 根据已分配给 Pod 的设备更新节点资源。
	// 传入的节点对象用于让设备管理器更新节点容量，反映当前可用设备。
	UpdatePluginResources(node *schedulerframework.NodeInfo, attrs *lifecycle.PodAdmitAttributes) error

	// Stop 停止设备管理器。
	Stop(logger klog.Logger) error

	// GetDeviceRunContainerOptions 检查是否缓存了传入 <pod, container> 的设备信息，
	// 找到时返回其 DeviceRunContainerOptions；没有缓存时返回空结构。
	GetDeviceRunContainerOptions(ctx context.Context, pod *v1.Pod, container *v1.Container) (*DeviceRunContainerOptions, error)

	// GetCapacity 返回节点上已注册设备插件的可用容量、可分配资源以及未激活的资源。
	GetCapacity(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string)

	// GetWatcherHandler 返回设备管理器使用的插件处理器。
	GetWatcherHandler() cache.PluginHandler
	// GetHealthChecker 返回设备管理器的健康检查器。
	GetHealthChecker() healthz.HealthChecker

	// GetDevices 返回已分配给 Pod 与容器的设备信息。
	GetDevices(podUID, containerName string) ResourceDeviceInstances

	// UpdateAllocatedResourcesStatus 更新 Pod 已分配资源的状态。
	UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus)

	// GetAllocatableDevices 返回管理器已知的所有设备信息。
	GetAllocatableDevices(logger klog.Logger) ResourceDeviceInstances

	// ShouldResetExtendedResourceCapacity 根据 checkpoint 文件是否存在决定是否重置扩展资源。
	// checkpoint 文件缺失通常说明节点已被重建。
	ShouldResetExtendedResourceCapacity() bool

	// GetTopologyHints 表明设备管理器实现了 TopologyManager 的 HintProvider 接口，
	// 参与拓扑感知的资源对齐决策。
	GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// GetTopologyHints 表明设备管理器实现了 TopologyManager 的 HintProvider 接口，
	// 参与 Pod 级别的拓扑感知资源对齐决策。
	GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// AllocatePod 触发向 Pod 分配资源。
	AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error

	// UpdateAllocatedDevices 释放绑定到已终止 Pod 的设备。
	UpdateAllocatedDevices(logger klog.Logger)

	// Updates 返回设备状态变化时接收更新的通道。
	Updates() <-chan resourceupdates.Update
}

// DeviceRunContainerOptions 保存容器使用已分配设备所需的运行时设置。
type DeviceRunContainerOptions struct {
	// 环境变量列表。
	Envs []kubecontainer.EnvVar
	// 容器需要挂载的路径。
	Mounts []kubecontainer.Mount
	// 映射到容器内的宿主设备。
	Devices []kubecontainer.DeviceInfo
	// 容器的注解。
	Annotations []kubecontainer.Annotation
	// 容器使用的 CDI 设备。
	CDIDevices []kubecontainer.CDIDevice
}

// TODO: evaluate whether we need this error definition.
const (
	// errEndpointStopped 是插件 endpoint 已停止时的错误格式。
	errEndpointStopped = "endpoint %v has been stopped"
)

// endpointStopGracePeriod 是插件故障导致 endpoint 停止后的宽限期。
// 在此期间设备管理器会保留已停止的 endpoint，以覆盖容量变更生效前的时间差。
const endpointStopGracePeriod = time.Duration(5) * time.Minute

// kubeletDeviceManagerCheckpoint 是设备插件 checkpoint 的文件名。
const kubeletDeviceManagerCheckpoint = "kubelet_internal_checkpoint"
