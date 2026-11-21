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

package state

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// PodResourceInfo 保存 Pod 内容器的资源需求。
type PodResourceInfo struct {
	// ContainerResources 是容器名到其资源需求的映射。
	ContainerResources map[string]v1.ResourceRequirements

	// PodLevelResources 表示作用于整个 Pod 的资源需求（若存在）。
	PodLevelResources *v1.ResourceRequirements

	// EmptyDirVolumeLimits 是 emptyDir 卷名到其容量限制的映射（若存在）。
	EmptyDirVolumeLimits map[string]*resource.Quantity
}

// PodResourceInfoMap 是 Pod UID 到 PodResourceInfo 的映射，
// 跟踪每个 Pod 内所有容器的资源需求。
type PodResourceInfoMap map[types.UID]PodResourceInfo

// Clone 返回 PodResourceInfoMap 的深拷贝。
func (pr PodResourceInfoMap) Clone() PodResourceInfoMap {
	// 深拷贝 Pod 级资源、容器资源与 emptyDir 限制。
	prCopy := make(PodResourceInfoMap)
	for podUID, podInfo := range pr {
		newPodInfo := PodResourceInfo{
			ContainerResources: make(map[string]v1.ResourceRequirements),
			PodLevelResources:  podInfo.PodLevelResources.DeepCopy(),
		}
		for containerName, containerInfo := range podInfo.ContainerResources {
			newPodInfo.ContainerResources[containerName] = *containerInfo.DeepCopy()
		}
		if podInfo.EmptyDirVolumeLimits != nil {
			newPodInfo.EmptyDirVolumeLimits = make(map[string]*resource.Quantity)
			for volumeName, volumeLimit := range podInfo.EmptyDirVolumeLimits {
				if volumeLimit == nil {
					newPodInfo.EmptyDirVolumeLimits[volumeName] = nil
				} else {
					vl := volumeLimit.DeepCopy()
					newPodInfo.EmptyDirVolumeLimits[volumeName] = &vl
				}
			}
		}
		prCopy[podUID] = newPodInfo
	}
	return prCopy
}

// Reader 提供读取当前 Pod 资源状态的方法。
type Reader interface {
	// GetContainerResources 返回指定容器的资源需求。
	GetContainerResources(podUID types.UID, containerName string) (v1.ResourceRequirements, bool)
	// GetPodResourceInfoMap 返回全部 Pod 的资源信息。
	GetPodResourceInfoMap() PodResourceInfoMap
	// GetPodResourceInfo 返回指定 Pod 的资源信息。
	GetPodResourceInfo(podUID types.UID) (PodResourceInfo, bool)
	// GetPodLevelResources 返回指定 Pod 的 Pod 级资源需求。
	GetPodLevelResources(podUID types.UID) (*v1.ResourceRequirements, bool)
	// GetEmptyDirVolumeLimit 返回指定 emptyDir 卷的容量限制。
	GetEmptyDirVolumeLimit(podUID types.UID, volumeName string) (*resource.Quantity, bool)
}

// writer 提供写入 Pod 资源状态的方法。
type writer interface {
	// SetContainerResources 设置指定容器的资源需求。
	SetContainerResources(logger klog.Logger, podUID types.UID, containerName string, resources v1.ResourceRequirements) error
	// SetPodResourceInfo 设置指定 Pod 的资源信息。
	SetPodResourceInfo(logger klog.Logger, podUID types.UID, resourceInfo PodResourceInfo) error
	// SetPodLevelResources 设置指定 Pod 的 Pod 级资源需求。
	SetPodLevelResources(logger klog.Logger, podUID types.UID, alloc *v1.ResourceRequirements) error
	// SetEmptyDirVolumeLimit 设置指定 emptyDir 卷的容量限制。
	SetEmptyDirVolumeLimit(podUID types.UID, volumeName string, limit *resource.Quantity) error
	// RemovePod 删除指定 Pod 的资源状态。
	RemovePod(logger klog.Logger, podUID types.UID) error
	// RemoveOrphanedPods 删除不在 remainingPods 中的 Pod 状态。
	RemoveOrphanedPods(remainingPods sets.Set[types.UID])
}

// State 提供跟踪和设置 Pod 资源的完整接口。
type State interface {
	Reader
	writer
}
