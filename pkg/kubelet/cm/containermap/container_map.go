/*
Copyright 2019 The Kubernetes Authors.

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

package containermap

import (
	"fmt"
	"maps"
)

// cmItem 是 (podUID, containerName) 二元组。
type cmItem struct {
	// podUID 是 Pod 的 UID。
	podUID string
	// containerName 是容器名称。
	containerName string
}

// ContainerMap 建立 containerID 到 (podUID, containerName) 的映射。
type ContainerMap map[string]cmItem

// NewContainerMap 创建空的 ContainerMap。
func NewContainerMap() ContainerMap {
	return make(ContainerMap)
}

// Clone 返回 ContainerMap 的深拷贝。
func (cm ContainerMap) Clone() ContainerMap {
	return maps.Clone(cm)
}

// Add 添加 containerID 到 (podUID, containerName) 的映射。
func (cm ContainerMap) Add(podUID, containerName, containerID string) {
	cm[containerID] = cmItem{
		podUID:        podUID,
		containerName: containerName,
	}
}

// RemoveByContainerID 按容器 ID 删除映射。
func (cm ContainerMap) RemoveByContainerID(containerID string) {
	delete(cm, containerID)
}

// RemoveByContainerRef 按 Pod UID 与容器名删除映射。
func (cm ContainerMap) RemoveByContainerRef(podUID, containerName string) {
	containerID, err := cm.GetContainerID(podUID, containerName)
	// 找不到映射时视为已删除，无需报错。
	if err == nil {
		cm.RemoveByContainerID(containerID)
	}
}

// GetContainerID 按 Pod UID 与容器名反查容器 ID。
func (cm ContainerMap) GetContainerID(podUID, containerName string) (string, error) {
	// 需要反查，遍历所有条目。
	for key, val := range cm {
		if val.podUID == podUID && val.containerName == containerName {
			return key, nil
		}
	}
	return "", fmt.Errorf("container %s not in ContainerMap for pod %s", containerName, podUID)
}

// GetContainerRef 按容器 ID 获取 (podUID, containerName)。
func (cm ContainerMap) GetContainerRef(containerID string) (string, string, error) {
	if _, exists := cm[containerID]; !exists {
		return "", "", fmt.Errorf("containerID %s not in ContainerMap", containerID)
	}
	return cm[containerID].podUID, cm[containerID].containerName, nil
}

// Visit 遍历容器映射中的全部条目。
func (cm ContainerMap) Visit(visitor func(podUID, containerName, containerID string)) {
	for k, v := range cm {
		visitor(v.podUID, v.containerName, k)
	}
}
