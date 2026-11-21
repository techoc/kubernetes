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

package cm

import (
	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

// InternalContainerLifecycle 定义容器创建前后与停止后的内部钩子，
// 供 CPU、内存、拓扑管理器介入容器生命周期。
type InternalContainerLifecycle interface {
	// PreCreateContainer 在容器创建前修改运行时容器配置（如 cpuset）。
	PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error
	// PreStartContainer 在容器启动前把容器注册到各资源管理器。
	PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error
	// PostStopContainer 在容器停止后从各资源管理器清理状态。
	PostStopContainer(logger klog.Logger, containerID string) error
}

// internalContainerLifecycleImpl 实现 InternalContainerLifecycle 接口。
type internalContainerLifecycleImpl struct {
	cpuManager      cpumanager.Manager
	memoryManager   memorymanager.Manager
	topologyManager topologymanager.Manager
}

// PreStartContainer 把容器注册到 CPU、内存与拓扑管理器，
// 使后续资源分配能关联到具体容器 ID。
func (i *internalContainerLifecycleImpl) PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error {
	if i.cpuManager != nil {
		i.cpuManager.AddContainer(logger, pod, container, containerID)
	}

	if i.memoryManager != nil {
		i.memoryManager.AddContainer(logger, pod, container, containerID)
	}

	i.topologyManager.AddContainer(logger, pod, container, containerID)

	return nil
}

// PostStopContainer 从拓扑管理器移除容器，释放其拓扑分配记录。
func (i *internalContainerLifecycleImpl) PostStopContainer(logger klog.Logger, containerID string) error {
	return i.topologyManager.RemoveContainer(logger, containerID)
}
