/*
Copyright 2016 The Kubernetes Authors.

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

// Package cache 实现挂载/卸载控制器使用的数据结构，
// 用于跟踪卷、卷所挂载到的节点，以及引用这些卷的 Pod。
package cache

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// DesiredStateOfWorld 定义挂载/卸载控制器期望状态缓存支持的一组线程安全操作。
// 该缓存以 节点->卷->Pod 的层级组织：节点是控制器管理的所有节点，
// 卷是应挂载到指定节点的所有卷，Pod 是引用该卷并被调度到该节点的 Pod。
// 注意：这与 kubelet 卷管理器实现的 DesiredStateOfWorld 不同。
// 两者跟踪的对象不同，这里包含的是挂载/卸载控制器特有的状态。
type DesiredStateOfWorld interface {
	// AddNode 将给定节点加入挂载/卸载控制器管理的节点列表。
	// 如果节点已存在，则不做任何操作。
	AddNode(nodeName k8stypes.NodeName)

	// AddPod 将给定 Pod 加入引用指定卷并被调度到指定节点的 Pod 列表。
	// 成功时返回由 volumeSpec 生成的唯一卷名 volumeName。
	// 如果 Pod 已存在于该卷下，则不做任何操作。
	// 如果 volumeSpec 对应的卷插件不支持挂载，则返回错误。
	// 如果应挂载到指定节点的卷列表中不存在名为 volumeName 的卷，则隐式添加该卷。
	// 如果挂载/卸载控制器管理的节点列表中不存在名为 nodeName 的节点，则返回错误。
	AddPod(podName types.UniquePodName, pod *v1.Pod, volumeSpec *volume.Spec, nodeName k8stypes.NodeName) (v1.UniqueVolumeName, error)

	// DeleteNode 从挂载/卸载控制器管理的节点列表中移除给定节点。
	// 如果节点不存在，则不做任何操作。
	// 如果节点存在但仍有一个或多个子卷，则返回错误。
	DeleteNode(nodeName k8stypes.NodeName) error

	// DeletePod 从引用指定卷并被调度到指定节点的 Pod 列表中移除给定 Pod。
	// 如果引用该卷且调度到该节点的 Pod 列表中不存在该 Pod，则不做任何操作。
	// 如果挂载/卸载控制器管理的节点中不存在名为 nodeName 的节点，则不做任何操作。
	// 如果指定节点下不存在名为 volumeName 的卷，则不做任何操作。
	// 如果删除 Pod 后该卷不再包含任何其他子 Pod，则同时删除该卷。
	DeletePod(podName types.UniquePodName, volumeName v1.UniqueVolumeName, nodeName k8stypes.NodeName)

	// NodeExists 如果指定名称的节点存在于挂载/卸载控制器管理的节点列表中，则返回 true。
	NodeExists(nodeName k8stypes.NodeName) bool

	// VolumeExists 如果指定名称的卷存在于应由挂载/卸载控制器挂载到指定节点的卷列表中，则返回 true。
	VolumeExists(volumeName v1.UniqueVolumeName, nodeName k8stypes.NodeName) bool

	// GetVolumesToAttach 根据当前期望状态生成并返回待挂载卷列表及其目标节点。
	GetVolumesToAttach() []VolumeToAttach

	// GetPodToAdd 根据当前期望状态生成并返回 Pod 映射。
	GetPodToAdd() map[types.UniquePodName]PodToAdd

	// SetMultiAttachError 将多重挂载错误标记为已上报，避免同一错误重复产生大量事件。
	SetMultiAttachError(v1.UniqueVolumeName, k8stypes.NodeName)

	// GetVolumePodsOnNodes 返回在给定节点上需要指定卷的 Pod 列表（"命名空间/名称"）。
	GetVolumePodsOnNodes(nodes []k8stypes.NodeName, volumeName v1.UniqueVolumeName) []*v1.Pod
}

// VolumeToAttach 表示应挂载到某个节点的卷。
type VolumeToAttach struct {
	operationexecutor.VolumeToAttach
}

// PodToAdd 表示引用底层卷并被调度到底层节点的 Pod。
type PodToAdd struct {
	// Pod 包含 Pod 的 API 对象。
	Pod *v1.Pod

	// VolumeName 包含该卷的唯一标识符。
	VolumeName v1.UniqueVolumeName

	// NodeName 包含该节点的名称。
	NodeName k8stypes.NodeName
}

// NewDesiredStateOfWorld 返回一个新的 DesiredStateOfWorld 实例。
func NewDesiredStateOfWorld(volumePluginMgr *volume.VolumePluginMgr) DesiredStateOfWorld {
	return &desiredStateOfWorld{
		nodesManaged:    make(map[k8stypes.NodeName]nodeManaged),
		volumePluginMgr: volumePluginMgr,
	}
}

// desiredStateOfWorld 是 DesiredStateOfWorld 的线程安全实现。
type desiredStateOfWorld struct {
	// nodesManaged 保存挂载/卸载控制器管理的节点集合。
	// 键为节点名称，值为包含节点详细信息的 node 对象。
	nodesManaged map[k8stypes.NodeName]nodeManaged
	// volumePluginMgr 是用于创建卷插件对象的卷插件管理器。
	volumePluginMgr *volume.VolumePluginMgr
	sync.RWMutex
}

// nodeManaged 表示由挂载/卸载控制器管理的一个节点。
type nodeManaged struct {
	// nodeName 包含该节点的名称。
	nodeName k8stypes.NodeName

	// volumesToAttach 保存应挂载到该节点的卷集合。
	// 键为卷名称，值为包含卷详细信息的 volumeToAttach 对象。
	volumesToAttach map[v1.UniqueVolumeName]volumeToAttach
}

// volumeToAttach 表示应挂载到某个节点的卷。
type volumeToAttach struct {
	// multiAttachErrorReported 表示该卷的多重挂载错误是否已上报。
	// 用于避免对同一卷重复上报该错误。
	multiAttachErrorReported bool

	// volumeName 包含该卷的唯一标识符。
	volumeName v1.UniqueVolumeName

	// spec 是该卷的卷规格。
	// 用于生成卷插件对象，并传递给挂载/卸载方法。
	spec *volume.Spec

	// scheduledPods 保存引用该卷并被调度到对应节点的 Pod 集合。
	// 键为 Pod 名称，值为包含 Pod 详细信息的 pod 对象。
	scheduledPods map[types.UniquePodName]pod
}

// pod 表示引用底层卷并被调度到底层节点的 Pod。
type pod struct {
	// podName 包含该 Pod 的唯一标识符。
	podName types.UniquePodName

	// podObj 包含 Pod 的 API 对象。
	podObj *v1.Pod
}

// AddNode 将给定节点加入控制器管理的节点集合；节点已存在时不做任何操作。
func (dsw *desiredStateOfWorld) AddNode(nodeName k8stypes.NodeName) {
	dsw.Lock()
	defer dsw.Unlock()

	if _, nodeExists := dsw.nodesManaged[nodeName]; !nodeExists {
		dsw.nodesManaged[nodeName] = nodeManaged{
			nodeName:        nodeName,
			volumesToAttach: make(map[v1.UniqueVolumeName]volumeToAttach),
		}
	}
}

// AddPod 将 Pod 加入引用指定卷且调度到指定节点的列表，必要时隐式创建卷条目。
func (dsw *desiredStateOfWorld) AddPod(
	podName types.UniquePodName,
	podToAdd *v1.Pod,
	volumeSpec *volume.Spec,
	nodeName k8stypes.NodeName) (v1.UniqueVolumeName, error) {
	dsw.Lock()
	defer dsw.Unlock()

	nodeObj, nodeExists := dsw.nodesManaged[nodeName]
	if !nodeExists {
		return "", fmt.Errorf(
			"no node with the name %q exists in the list of managed nodes",
			nodeName)
	}

	attachableVolumePlugin, err := dsw.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || attachableVolumePlugin == nil {
		// 没有对应插件时给出明确错误，避免后续操作空指针。
		if attachableVolumePlugin == nil {
			err = fmt.Errorf("plugin do not support attachment")
		}
		return "", fmt.Errorf(
			"failed to get AttachablePlugin from volumeSpec for volume %q err=%v",
			volumeSpec.Name(),
			err)
	}

	volumeName, err := util.GetUniqueVolumeNameFromSpec(
		attachableVolumePlugin, volumeSpec)
	if err != nil {
		return "", fmt.Errorf(
			"failed to get UniqueVolumeName from volumeSpec for plugin=%q and volume=%q err=%v",
			attachableVolumePlugin.GetPluginName(),
			volumeSpec.Name(),
			err)
	}

	volumeObj, volumeExists := nodeObj.volumesToAttach[volumeName]
	if !volumeExists {
		// 卷不存在时隐式创建条目，因为 Pod 已声明对该卷的引用。
		volumeObj = volumeToAttach{
			multiAttachErrorReported: false,
			volumeName:               volumeName,
			spec:                     volumeSpec,
			scheduledPods:            make(map[types.UniquePodName]pod),
		}
		dsw.nodesManaged[nodeName].volumesToAttach[volumeName] = volumeObj
	}
	if _, podExists := volumeObj.scheduledPods[podName]; !podExists {
		dsw.nodesManaged[nodeName].volumesToAttach[volumeName].scheduledPods[podName] =
			pod{
				podName: podName,
				podObj:  podToAdd,
			}
	}

	return volumeName, nil
}

// DeleteNode 从管理列表中移除节点；节点不存在时为空操作，仍有卷挂载时返回错误。
func (dsw *desiredStateOfWorld) DeleteNode(nodeName k8stypes.NodeName) error {
	dsw.Lock()
	defer dsw.Unlock()

	nodeObj, nodeExists := dsw.nodesManaged[nodeName]
	if !nodeExists {
		return nil
	}

	if len(nodeObj.volumesToAttach) > 0 {
		// 节点上仍有卷时删除会导致挂载状态悬空，拒绝删除。
		return fmt.Errorf(
			"failed to delete node %q from list of nodes managed by attach/detach controller--the node still contains %v volumes in its list of volumes to attach",
			nodeName,
			len(nodeObj.volumesToAttach))
	}

	delete(
		dsw.nodesManaged,
		nodeName)
	return nil
}

// DeletePod 从卷的 Pod 列表中移除 Pod；若该卷不再有子 Pod 则一并删除卷条目。
func (dsw *desiredStateOfWorld) DeletePod(
	podName types.UniquePodName,
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName) {
	dsw.Lock()
	defer dsw.Unlock()

	nodeObj, nodeExists := dsw.nodesManaged[nodeName]
	if !nodeExists {
		return
	}

	volumeObj, volumeExists := nodeObj.volumesToAttach[volumeName]
	if !volumeExists {
		return
	}
	if _, podExists := volumeObj.scheduledPods[podName]; !podExists {
		return
	}

	delete(
		dsw.nodesManaged[nodeName].volumesToAttach[volumeName].scheduledPods,
		podName)

	if len(volumeObj.scheduledPods) == 0 {
		// 没有 Pod 引用该卷后无需继续挂载，清理卷条目避免残留状态。
		delete(
			dsw.nodesManaged[nodeName].volumesToAttach,
			volumeName)
	}
}

// NodeExists 判断指定节点是否在控制器管理的节点列表中。
func (dsw *desiredStateOfWorld) NodeExists(nodeName k8stypes.NodeName) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	_, nodeExists := dsw.nodesManaged[nodeName]
	return nodeExists
}

// VolumeExists 判断指定卷是否在应挂载到指定节点的卷列表中。
func (dsw *desiredStateOfWorld) VolumeExists(
	volumeName v1.UniqueVolumeName, nodeName k8stypes.NodeName) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	nodeObj, nodeExists := dsw.nodesManaged[nodeName]
	if nodeExists {
		if _, volumeExists := nodeObj.volumesToAttach[volumeName]; volumeExists {
			return true
		}
	}

	return false
}

// SetMultiAttachError 标记指定卷的多重挂载错误已上报，防止重复产生事件。
func (dsw *desiredStateOfWorld) SetMultiAttachError(
	volumeName v1.UniqueVolumeName,
	nodeName k8stypes.NodeName) {
	dsw.Lock()
	defer dsw.Unlock()

	nodeObj, nodeExists := dsw.nodesManaged[nodeName]
	if nodeExists {
		if volumeObj, volumeExists := nodeObj.volumesToAttach[volumeName]; volumeExists {
			volumeObj.multiAttachErrorReported = true
			// 结构体是值类型，修改后需写回 map 才能生效。
			dsw.nodesManaged[nodeName].volumesToAttach[volumeName] = volumeObj
		}
	}
}

// GetVolumesToAttach 返回当前期望状态下所有待挂载的卷及其目标节点信息。
func (dsw *desiredStateOfWorld) GetVolumesToAttach() []VolumeToAttach {
	dsw.RLock()
	defer dsw.RUnlock()

	volumesToAttach := make([]VolumeToAttach, 0 /* len */, len(dsw.nodesManaged) /* cap */)
	for nodeName, nodeObj := range dsw.nodesManaged {
		for volumeName, volumeObj := range nodeObj.volumesToAttach {
			volumesToAttach = append(volumesToAttach,
				VolumeToAttach{
					VolumeToAttach: operationexecutor.VolumeToAttach{
						MultiAttachErrorReported: volumeObj.multiAttachErrorReported,
						VolumeName:               volumeName,
						VolumeSpec:               volumeObj.spec,
						NodeName:                 nodeName,
						ScheduledPods:            getPodsFromMap(volumeObj.scheduledPods),
					}})
		}
	}

	return volumesToAttach
}

// getPodsFromMap 从 Pod 映射中构造 v1.Pod 对象列表。
func getPodsFromMap(podMap map[types.UniquePodName]pod) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(podMap))
	for _, pod := range podMap {
		pods = append(pods, pod.podObj)
	}
	return pods
}

// GetPodToAdd 返回当前期望状态下按唯一 Pod 名称索引的待处理 Pod 映射。
func (dsw *desiredStateOfWorld) GetPodToAdd() map[types.UniquePodName]PodToAdd {
	dsw.RLock()
	defer dsw.RUnlock()

	pods := make(map[types.UniquePodName]PodToAdd)
	for nodeName, nodeObj := range dsw.nodesManaged {
		for volumeName, volumeObj := range nodeObj.volumesToAttach {
			for podUID, pod := range volumeObj.scheduledPods {
				pods[podUID] = PodToAdd{
					Pod:        pod.podObj,
					VolumeName: volumeName,
					NodeName:   nodeName,
				}
			}
		}
	}
	return pods
}

// GetVolumePodsOnNodes 返回在给定节点上需要指定卷的 Pod 列表。
func (dsw *desiredStateOfWorld) GetVolumePodsOnNodes(nodes []k8stypes.NodeName, volumeName v1.UniqueVolumeName) []*v1.Pod {
	dsw.RLock()
	defer dsw.RUnlock()

	pods := []*v1.Pod{}
	for _, nodeName := range nodes {
		node, ok := dsw.nodesManaged[nodeName]
		if !ok {
			continue
		}
		volume, ok := node.volumesToAttach[volumeName]
		if !ok {
			continue
		}
		for _, pod := range volume.scheduledPods {
			pods = append(pods, pod.podObj)
		}
	}
	return pods
}
