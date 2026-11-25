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

package state

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	cperrors "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
)

var _ State = &stateCheckpoint{}

// stateCheckpoint 在内存状态缓存之上增加 checkpoint 持久化能力。
type stateCheckpoint struct {
	sync.RWMutex
	logger            klog.Logger
	cache             State
	policyName        string
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
}

// NewCheckpointState 创建带 checkpoint 后端的状态对象，用于持久化内存与 Pod 的分配关系。
func NewCheckpointState(logger klog.Logger, stateDir, checkpointName, policyName string) (State, error) {
	logger = klog.LoggerWithName(logger, "Memory Manager state checkpoint")
	// checkpoint 文件位于 stateDir 下，由 checkpointmanager 负责原子写入。
	checkpointManager, err := checkpointmanager.NewCheckpointManager(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %v", err)
	}
	stateCheckpoint := &stateCheckpoint{
		logger:            logger,
		cache:             NewMemoryState(logger),
		policyName:        policyName,
		checkpointManager: checkpointManager,
		checkpointName:    checkpointName,
	}

	// 启动时恢复状态；失败时要求用户 drain 节点并删除损坏的 checkpoint，
	// 避免带着不一致的分配状态继续运行。
	if err := stateCheckpoint.restoreState(); err != nil {
		//nolint:staticcheck // ST1005 user-facing error message
		return nil, fmt.Errorf("could not restore state from checkpoint: %v, please drain this node and delete the memory manager checkpoint file %q before restarting Kubelet",
			err, filepath.Join(stateDir, checkpointName))
	}

	return stateCheckpoint, nil
}

// migrateV1CheckpointToV2Checkpoint 把 v1 格式的 checkpoint 转换为 v2。
func (sc *stateCheckpoint) migrateV1CheckpointToV2Checkpoint(src *MemoryManagerCheckpointV1, dst *MemoryManagerCheckpointV2) {
	dst.PolicyName = src.PolicyName
	dst.MachineState = src.MachineState.Clone()
	dst.Entries = src.Entries.Clone()
}

// migrateV2CheckpointToV3Checkpoint 把 v2 格式的 checkpoint 转换为 v3。
func (sc *stateCheckpoint) migrateV2CheckpointToV3Checkpoint(src *MemoryManagerCheckpointV2, dst *MemoryManagerCheckpointV3) {
	dst.CheckpointData.PolicyName = src.PolicyName
	dst.CheckpointData.MachineState = src.MachineState.Clone()
	dst.CheckpointData.Entries = src.Entries.Clone()
	dst.CheckpointData.PodEntries = src.PodEntries.Clone()
}

// restoreState 从 checkpoint 恢复状态；checkpoint 不存在时创建初始文件。
func (sc *stateCheckpoint) restoreState() error {
	sc.Lock()
	defer sc.Unlock()
	cp, err := sc.loadAndMigrateCheckpointV3()
	if err != nil {
		if errors.Is(err, cperrors.ErrCheckpointNotFound) {
			return sc.storeState()
		}
		return err
	}

	// checkpoint 中的策略必须与当前配置一致，否则无法安全恢复。
	if sc.policyName != cp.CheckpointData.PolicyName {
		return fmt.Errorf("[memorymanager] configured policy %q differs from state checkpoint policy %q", sc.policyName, cp.CheckpointData.PolicyName)
	}
	// 恢复机器状态和容器级分配；Pod 级分配仅在特性开启时恢复。
	sc.cache.SetMachineState(cp.CheckpointData.MachineState)
	sc.cache.SetMemoryAssignments(cp.CheckpointData.Entries)
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		sc.cache.SetPodMemoryAssignments(cp.CheckpointData.PodEntries)
	}

	sc.logger.V(2).Info("State checkpoint: restored state from checkpoint")

	return nil
}

// loadAndMigrateCheckpointV3 加载最新格式的 checkpoint，必要时从旧版本迁移。
func (sc *stateCheckpoint) loadAndMigrateCheckpointV3() (*MemoryManagerCheckpointV3, error) {
	// 优先按 v3 加载。
	checkpointV3 := newMemoryManagerCheckpointV3()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV3)
	if err == nil {
		return checkpointV3, nil
	}
	if errors.Is(err, cperrors.ErrCheckpointNotFound) {
		return nil, err
	}

	if len(checkpointV3.Data) > 0 || checkpointV3.DataChecksum != 0 {
		// v3 数据已存在却读取失败，说明 checkpoint 损坏；此时不能回退旧版本，直接报错。
		err = fmt.Errorf("could not load v3 checkpoint: %w", err)
		sc.logger.Error(err, "not falling back to previous versions")
		return nil, err
	}

	// 记录 v3 加载失败并回退到 v2。
	sc.logger.Info("could not load v3 checkpoint for memory manager, falling back to v2", "err", err)

	// 按 v2 加载。
	checkpointV2, err := sc.loadAndMigrateCheckpointV2()
	if err != nil {
		return nil, err
	}

	// 重置 v3 对象，清除上次加载尝试可能残留的数据。
	checkpointV3 = newMemoryManagerCheckpointV3()

	// v2 加载成功，迁移为 v3 后返回。
	sc.logger.Info("migrating memory manager checkpoint from v2 to v3")
	sc.migrateV2CheckpointToV3Checkpoint(checkpointV2, checkpointV3)

	return checkpointV3, nil
}

// loadAndMigrateCheckpointV2 加载 v2 checkpoint，必要时从 v1 迁移。
func (sc *stateCheckpoint) loadAndMigrateCheckpointV2() (*MemoryManagerCheckpointV2, error) {
	// 尝试按 V2 加载。
	checkpointV2 := newMemoryManagerCheckpointV2()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV2)
	if err == nil {
		return checkpointV2, nil
	}

	// 记录 v2 加载失败并回退到 v1。
	if errors.Is(err, cperrors.CorruptCheckpointError{}) {
		sc.logger.Error(err, "V2 checkpoint for memory manager is corrupt, falling back to V1")
	} else {
		sc.logger.Info("could not load V2 checkpoint for memory manager, falling back to V1", "err", err)
	}

	// 按 v1 加载。
	checkpointV1, err := sc.loadCheckpointV1()
	if err != nil {
		// 找不到时返回 ErrCheckpointNotFound，否则返回损坏错误。
		return nil, err
	}

	// 重置 v2 对象，清除上次加载尝试可能残留的数据。
	checkpointV2 = newMemoryManagerCheckpointV2()

	// v1 加载成功，迁移为 v2 后返回。
	sc.logger.Info("migrating memory manager checkpoint from v1 to v2")
	sc.migrateV1CheckpointToV2Checkpoint(checkpointV1, checkpointV2)

	return checkpointV2, nil
}

// loadCheckpointV1 按最旧的 v1 格式读取 checkpoint。
func (sc *stateCheckpoint) loadCheckpointV1() (*MemoryManagerCheckpointV1, error) {
	checkpointV1 := newMemoryManagerCheckpointV1()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV1)
	if err != nil {
		return nil, err
	}
	return checkpointV1, nil
}

// storeState 把当前状态写入 checkpoint；调用方负责加锁。
func (sc *stateCheckpoint) storeState() error {
	checkpoint := newMemoryManagerCheckpoint()
	checkpoint.CheckpointData.PolicyName = sc.policyName
	checkpoint.CheckpointData.MachineState = sc.cache.GetMachineState()
	checkpoint.CheckpointData.Entries = sc.cache.GetMemoryAssignments()

	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		checkpoint.CheckpointData.PodEntries = sc.cache.GetPodMemoryAssignments()
	}

	err := sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint)
	if err != nil {
		sc.logger.Error(err, "Could not save checkpoint")
		return err
	}
	return nil
}

// GetMachineState 返回状态中保存的 NUMA 节点内存映射。
func (sc *stateCheckpoint) GetMachineState() NUMANodeMap {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMachineState()
}

// GetMemoryBlocks 返回指定容器的内存块分配。
func (sc *stateCheckpoint) GetMemoryBlocks(podUID string, containerName string) []Block {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMemoryBlocks(podUID, containerName)
}

// GetMemoryAssignments 返回全部容器级内存分配。
func (sc *stateCheckpoint) GetMemoryAssignments() ContainerMemoryAssignments {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMemoryAssignments()
}

// GetPodMemoryBlocks 返回指定 Pod 的内存块分配。
func (sc *stateCheckpoint) GetPodMemoryBlocks(podUID string) []Block {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetPodMemoryBlocks(podUID)
}

// GetPodMemoryAssignments 返回全部 Pod 级内存分配。
func (sc *stateCheckpoint) GetPodMemoryAssignments() PodMemoryAssignments {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetPodMemoryAssignments()
}

// SetMachineState 保存 NUMA 节点内存映射并持久化。
func (sc *stateCheckpoint) SetMachineState(memoryMap NUMANodeMap) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMachineState(memoryMap)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// SetMemoryBlocks 保存指定容器的内存块分配并持久化。
func (sc *stateCheckpoint) SetMemoryBlocks(podUID string, containerName string, blocks []Block) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMemoryBlocks(podUID, containerName, blocks)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID, "containerName", containerName)
	}
}

// SetMemoryAssignments 用给定参数整体替换容器级内存分配并持久化。
func (sc *stateCheckpoint) SetMemoryAssignments(assignments ContainerMemoryAssignments) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetMemoryAssignments(assignments)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// SetPodMemoryAssignments 整体替换 Pod 级内存分配并持久化。
func (sc *stateCheckpoint) SetPodMemoryAssignments(assignments PodMemoryAssignments) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetPodMemoryAssignments(assignments)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// SetPodMemoryBlocks 保存指定 Pod 的内存块分配并持久化。
func (sc *stateCheckpoint) SetPodMemoryBlocks(podUID string, blocks []Block) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.SetPodMemoryBlocks(podUID, blocks)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID)
	}
}

// Delete 从容器级分配中删除对应条目并持久化。
func (sc *stateCheckpoint) Delete(podUID string, containerName string) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.Delete(podUID, containerName)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID, "containerName", containerName)
	}
}

// DeletePod 删除指定 Pod 的 Pod 级分配，不影响容器级分配。
func (sc *stateCheckpoint) DeletePod(podUID string) {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.DeletePod(podUID)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID)
	}
}

// ClearState 清空机器状态和所有内存分配。
func (sc *stateCheckpoint) ClearState() {
	sc.Lock()
	defer sc.Unlock()

	sc.cache.ClearState()
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}
