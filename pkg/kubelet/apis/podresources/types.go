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

//go:generate mockery
package podresources

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

// DevicesProvider 提供给定容器使用的设备信息。
type DevicesProvider interface {
	// UpdateAllocatedDevices 释放已绑定到终止 Pod 的设备。
	UpdateAllocatedDevices(logger klog.Logger)
	// GetDevices 返回分配给 Pod 与容器的设备信息。
	GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices
	// GetAllocatableDevices 返回 manager 已知的所有设备信息。
	GetAllocatableDevices(logger klog.Logger) []*podresourcesapi.ContainerDevices
}

// PodsProvider 提供节点已准入的 Pod 信息。
type PodsProvider interface {
	// GetActivePods 返回当前活跃的 Pod 列表。
	GetActivePods() []*v1.Pod
	// GetPods 返回节点上的所有 Pod。
	GetPods() []*v1.Pod
	// GetPodByName 按命名空间与名称查找 Pod。
	GetPodByName(namespace, name string) (*v1.Pod, bool)
}

// CPUsProvider 提供给定容器使用的 CPU 信息。
type CPUsProvider interface {
	// GetCPUs 返回分配给容器的 CPU 信息。
	GetCPUs(pod *v1.Pod, container *v1.Container) []int64
	// GetPodCPUs 返回分配给 Pod 的 CPU 信息。
	GetPodCPUs(podUID string) []int64
	// GetAllocatableCPUs 返回节点上可分配的 CPU。
	GetAllocatableCPUs() []int64
}

// MemoryProvider 提供容器与 Pod 的内存分配信息。
type MemoryProvider interface {
	// GetMemory 返回分配给容器的内存信息。
	GetMemory(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.ContainerMemory
	// GetPodMemory 返回分配给 Pod 的内存信息。
	GetPodMemory(logger klog.Logger, podUID string) []*podresourcesapi.ContainerMemory
	// GetAllocatableMemory 返回节点上可分配的内存。
	GetAllocatableMemory(logger klog.Logger) []*podresourcesapi.ContainerMemory
}

// DynamicResourcesProvider 提供动态资源分配信息。
type DynamicResourcesProvider interface {
	// GetDynamicResources 返回分配给 Pod 与容器的动态资源信息。
	GetDynamicResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource
}

// PodResourcesProviders 汇总 PodResources API 需要的各类 provider。
type PodResourcesProviders struct {
	Pods             PodsProvider
	Devices          DevicesProvider
	Cpus             CPUsProvider
	Memory           MemoryProvider
	DynamicResources DynamicResourcesProvider
}
