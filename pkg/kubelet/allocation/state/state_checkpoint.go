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
	"fmt"
	"path"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
)

// 编译期断言：stateCheckpoint 必须实现 State 接口。
var _ State = &stateCheckpoint{}

// stateCheckpoint 在内存状态之上增加 checkpoint 持久化，保证 kubelet 重启后能恢复资源分配。
type stateCheckpoint struct {
	mux               sync.RWMutex
	cache             State
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
	lastChecksum      checksum.Checksum
}

// NewStateCheckpoint 创建带 checkpoint 后端的 State，用于跟踪 Pod 资源信息。
func NewStateCheckpoint(logger klog.Logger, stateDir, checkpointName string) (State, error) {
	checkpointManager, err := checkpointmanager.NewCheckpointManager(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager for pod resource information tracking: %w", err)
	}

	// 从磁盘恢复上次保存的状态；checkpoint 不存在时返回空状态。
	pra, checksum, err := restoreState(logger, checkpointManager, checkpointName)
	if err != nil {
		//lint:ignore ST1005 user-facing error message
		// 恢复失败时要求用户 drain 节点并删除 checkpoint，避免带着不一致的状态启动。
		return nil, fmt.Errorf("could not restore state from checkpoint: %w, please drain this node and delete pod resource information checkpoint file %q before restarting Kubelet",
			err, path.Join(stateDir, checkpointName))
	}

	stateCheckpoint := &stateCheckpoint{
		cache:             NewStateMemory(logger, pra),
		checkpointManager: checkpointManager,
		checkpointName:    checkpointName,
		lastChecksum:      checksum,
	}
	return stateCheckpoint, nil
}

// 从 checkpoint 恢复状态；checkpoint 不存在时按空状态处理。
func restoreState(logger klog.Logger, checkpointManager checkpointmanager.CheckpointManager, checkpointName string) (PodResourceInfoMap, checksum.Checksum, error) {
	checkpoint := &Checkpoint{}
	if err := checkpointManager.GetCheckpoint(checkpointName, checkpoint); err != nil {
		// checkpoint 不存在属于正常情况，返回空状态即可。
		if err == errors.ErrCheckpointNotFound {
			return nil, 0, nil
		}
		return nil, 0, err
	}

	praInfo, err := checkpoint.GetPodResourceCheckpointInfo()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get pod resource information: %w", err)
	}

	logger.V(2).Info("State checkpoint: restored pod resource state from checkpoint")
	return praInfo.Entries, checkpoint.Checksum, nil
}

// 将状态保存到 checkpoint；调用方负责加锁。
func (sc *stateCheckpoint) storeState(logger klog.Logger) error {
	resourceInfo := sc.cache.GetPodResourceInfoMap()

	checkpoint, err := NewCheckpoint(&PodResourceCheckpointInfo{
		Entries: resourceInfo,
	})
	if err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}
	if checkpoint.Checksum == sc.lastChecksum {
		// checkpoint 未变化时无需重复写盘。
		return nil
	}
	err = sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint)
	if err != nil {
		logger.Error(err, "Failed to save pod resource information checkpoint")
		return err
	}
	sc.lastChecksum = checkpoint.Checksum
	return nil
}

// GetContainerResources 返回 Pod 容器当前的资源信息。
func (sc *stateCheckpoint) GetContainerResources(podUID types.UID, containerName string) (v1.ResourceRequirements, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetContainerResources(podUID, containerName)
}

// GetPodLevelResources 返回 Pod 级当前的资源信息。
func (sc *stateCheckpoint) GetPodLevelResources(podUID types.UID) (*v1.ResourceRequirements, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetPodLevelResources(podUID)
}

// GetEmptyDirVolumeLimit 返回 emptyDir 卷当前的容量限制信息。
func (sc *stateCheckpoint) GetEmptyDirVolumeLimit(podUID types.UID, volumeName string) (*resource.Quantity, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetEmptyDirVolumeLimit(podUID, volumeName)
}

// GetPodResourceInfoMap 返回当前全部 Pod 的资源信息映射。
func (sc *stateCheckpoint) GetPodResourceInfoMap() PodResourceInfoMap {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetPodResourceInfoMap()
}

// GetPodResourceInfo 返回指定 Pod 的资源信息。
func (sc *stateCheckpoint) GetPodResourceInfo(podUID types.UID) (PodResourceInfo, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetPodResourceInfo(podUID)
}

// SetContainerResources 设置 Pod 容器的资源信息，并持久化到 checkpoint。
func (sc *stateCheckpoint) SetContainerResources(logger klog.Logger, podUID types.UID, containerName string, resources v1.ResourceRequirements) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	err := sc.cache.SetContainerResources(logger, podUID, containerName, resources)
	if err != nil {
		return err
	}
	return sc.storeState(logger)
}

// SetPodLevelResources 设置 Pod 级资源信息，并持久化到 checkpoint。
func (sc *stateCheckpoint) SetPodLevelResources(logger klog.Logger, podUID types.UID, resInfo *v1.ResourceRequirements) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	err := sc.cache.SetPodLevelResources(logger, podUID, resInfo)
	if err != nil {
		return err
	}
	return sc.storeState(logger)
}

// SetEmptyDirVolumeLimit 设置 Pod 的 emptyDir 卷容量限制，并持久化到 checkpoint。
func (sc *stateCheckpoint) SetEmptyDirVolumeLimit(podUID types.UID, volumeName string, limit *resource.Quantity) error {
	logger := klog.TODO()
	sc.mux.Lock()
	defer sc.mux.Unlock()
	err := sc.cache.SetEmptyDirVolumeLimit(podUID, volumeName, limit)
	if err != nil {
		return err
	}
	return sc.storeState(logger)
}

// SetPodResourceInfo 设置 Pod 资源信息，并持久化到 checkpoint。
func (sc *stateCheckpoint) SetPodResourceInfo(logger klog.Logger, podUID types.UID, resourceInfo PodResourceInfo) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	err := sc.cache.SetPodResourceInfo(logger, podUID, resourceInfo)
	if err != nil {
		return err
	}
	return sc.storeState(logger)
}

// RemovePod 删除指定 Pod 的资源信息。
func (sc *stateCheckpoint) RemovePod(logger klog.Logger, podUID types.UID) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	// 删除 Pod 时跳过写 checkpoint：保留已删除 Pod 的记录没有副作用，
	// 这些记录最终会被 RemoveOrphanedPods 清理；下一次非删除类更新时
	// 删除操作会随之落盘。
	return sc.cache.RemovePod(logger, podUID)
}

// RemoveOrphanedPods 从内存状态中移除不再存在的 Pod。
func (sc *stateCheckpoint) RemoveOrphanedPods(remainingPods sets.Set[types.UID]) {
	sc.cache.RemoveOrphanedPods(remainingPods)
	// 不更新已存储的状态：如果缓存在写盘前 kubelet 重启，
	// 下次调用本方法时孤儿 Pod 仍会被移除。
}

// noopStateCheckpoint 是空实现，用于不需要持久化的场景。
type noopStateCheckpoint struct{}

// NewNoopStateCheckpoint 创建一个不做任何持久化的空 State。
func NewNoopStateCheckpoint() State {
	return &noopStateCheckpoint{}
}

// GetContainerResources 始终返回空资源与 false。
func (sc *noopStateCheckpoint) GetContainerResources(_ types.UID, _ string) (v1.ResourceRequirements, bool) {
	return v1.ResourceRequirements{}, false
}

// GetPodLevelResources 始终返回 nil 与 false。
func (sc *noopStateCheckpoint) GetPodLevelResources(_ types.UID) (*v1.ResourceRequirements, bool) {
	return nil, false
}

// GetEmptyDirVolumeLimit 始终返回 nil 与 false。
func (sc *noopStateCheckpoint) GetEmptyDirVolumeLimit(_ types.UID, _ string) (*resource.Quantity, bool) {
	return nil, false
}

// GetPodResourceInfoMap 始终返回 nil。
func (sc *noopStateCheckpoint) GetPodResourceInfoMap() PodResourceInfoMap {
	return nil
}

// GetPodResourceInfo 始终返回空信息与 false。
func (sc *noopStateCheckpoint) GetPodResourceInfo(_ types.UID) (PodResourceInfo, bool) {
	return PodResourceInfo{}, false
}

// SetContainerResources 是空操作。
func (sc *noopStateCheckpoint) SetContainerResources(_ klog.Logger, _ types.UID, _ string, _ v1.ResourceRequirements) error {
	return nil
}

// SetPodLevelResources 是空操作。
func (sc *noopStateCheckpoint) SetPodLevelResources(_ klog.Logger, _ types.UID, _ *v1.ResourceRequirements) error {
	return nil
}

// SetEmptyDirVolumeLimit 是空操作。
func (sc *noopStateCheckpoint) SetEmptyDirVolumeLimit(_ types.UID, _ string, _ *resource.Quantity) error {
	return nil
}

// SetPodResourceInfo 是空操作。
func (sc *noopStateCheckpoint) SetPodResourceInfo(_ klog.Logger, _ types.UID, _ PodResourceInfo) error {
	return nil
}

// RemovePod 是空操作。
func (sc *noopStateCheckpoint) RemovePod(_ klog.Logger, _ types.UID) error {
	return nil
}

// RemoveOrphanedPods 是空操作。
func (sc *noopStateCheckpoint) RemoveOrphanedPods(_ sets.Set[types.UID]) {}
