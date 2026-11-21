/*
Copyright 2018 The Kubernetes Authors.

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
	"maps"
	"path/filepath"
	"sync"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	cperrors "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/utils/cpuset"
)

// 编译期断言：stateCheckpoint 实现了 State 接口。
var _ State = &stateCheckpoint{}

// stateCheckpoint 是基于 checkpoint 后端的 CPU 管理器状态实现。
// 它把内存中的 State 作为缓存，每次状态变化都会同步落盘，
// 从而保证 kubelet 重启后能恢复已有的 CPU 分配结果。
type stateCheckpoint struct {
	mux               sync.RWMutex
	logger            klog.Logger
	policyName        string
	cache             State
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
	initialContainers containermap.ContainerMap
}

// NewCheckpointState 创建一个基于 checkpoint 后端的 State，用于跟踪 CPU 与 pod 的分配关系。
// 函数会初始化 checkpoint 管理器并从已有 checkpoint 恢复状态；恢复失败时返回的错误
// 会提示 drain 节点、删除对应 checkpoint 文件后再重启 kubelet。
func NewCheckpointState(logger klog.Logger, stateDir, checkpointName, policyName string, initialContainers containermap.ContainerMap) (State, error) {
	// checkpointmanager 目前无法携带上下文，且本对象需要持有 checkpoint，
	// 因此在外层统一使用开销相对较高的 WithName 来标注日志来源是合理的。
	logger = klog.LoggerWithName(logger, "CPUManager state checkpoint")
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
		initialContainers: initialContainers,
	}

	if err := stateCheckpoint.restoreState(); err != nil {
		//nolint:staticcheck // ST1005 user-facing error message
		return nil, fmt.Errorf("could not restore state from checkpoint: %v, please drain this node and delete the CPU manager checkpoint file %q before restarting Kubelet",
			err, filepath.Join(stateDir, checkpointName))
	}

	return stateCheckpoint, nil
}

// migrateV2CheckpointToV3Checkpoint 把 v2 格式的 checkpoint 转换为 v3 格式。
func (sc *stateCheckpoint) migrateV2CheckpointToV3Checkpoint(src *CPUManagerCheckpointV2, dst *CPUManagerCheckpointV3) {
	if src.PolicyName != "" {
		dst.PolicyName = src.PolicyName
	}
	if src.DefaultCPUSet != "" {
		dst.DefaultCPUSet = src.DefaultCPUSet
	}
	if len(src.Entries) > 0 {
		dst.Entries = make(map[string]map[string]string, len(src.Entries))
		for podUID, containerEntries := range src.Entries {
			dst.Entries[podUID] = make(map[string]string, len(containerEntries))
			maps.Copy(dst.Entries[podUID], containerEntries)
		}
	}
}

// migrateV3CheckpointToV4Checkpoint 把 v3 格式的 checkpoint 转换为 v4 格式。
func (sc *stateCheckpoint) migrateV3CheckpointToV4Checkpoint(src *CPUManagerCheckpointV3, dst *CPUManagerCheckpointV4) {
	if src.PolicyName != "" {
		dst.CheckpointData.PolicyName = src.PolicyName
	}
	if src.DefaultCPUSet != "" {
		dst.CheckpointData.DefaultCPUSet = src.DefaultCPUSet
	}
	if len(src.Entries) > 0 {
		dst.CheckpointData.Entries = make(map[string]map[string]string, len(src.Entries))
		for podUID, containerEntries := range src.Entries {
			dst.CheckpointData.Entries[podUID] = make(map[string]string, len(containerEntries))
			maps.Copy(dst.CheckpointData.Entries[podUID], containerEntries)
		}
	}
	if len(src.PodEntries) > 0 {
		// 克隆 CPUSet，使迁移后的 checkpoint 与源数据解耦，避免共享底层状态。
		dst.CheckpointData.PodEntries = make(PodCPUAssignments, len(src.PodEntries))
		for podUID, podEntries := range src.PodEntries {
			dst.CheckpointData.PodEntries[podUID] = PodEntry{CPUSet: podEntries.CPUSet.Clone()}
		}
	}
}

// restoreState 从 checkpoint 恢复状态；若 checkpoint 不存在，则创建一份初始状态。
func (sc *stateCheckpoint) restoreState() error {
	// 恢复过程会读写共享缓存，必须持有写锁，避免与并发的读写操作竞争。
	sc.mux.Lock()
	defer sc.mux.Unlock()

	cp, err := sc.loadAndMigrateCheckpointV4()
	if err != nil {
		if errors.Is(err, cperrors.ErrCheckpointNotFound) {
			// 首次启动没有 checkpoint 属于正常情况，直接写入一份空状态即可。
			return sc.storeState()
		}
		return err
	}

	var tmpDefaultCPUSet cpuset.CPUSet
	var tmpContainerCPUSet cpuset.CPUSet
	var tmpContainerBaselineCPUSet cpuset.CPUSet
	tmpAssignments := ContainerCPUAssignments{}
	tmpBaselines := ContainerCPUBaselines{}

	// 策略不一致意味着配置已被修改，沿用旧 checkpoint 会导致分配结果与当前策略不符，必须直接报错。
	if sc.policyName != cp.CheckpointData.PolicyName {
		return fmt.Errorf("configured policy %q differs from state checkpoint policy %q", sc.policyName, cp.CheckpointData.PolicyName)
	}
	if tmpDefaultCPUSet, err = cpuset.Parse(cp.CheckpointData.DefaultCPUSet); err != nil {
		return fmt.Errorf("could not parse default CPU set %q: %w", cp.CheckpointData.DefaultCPUSet, err)
	}
	for pod := range cp.CheckpointData.Entries {
		tmpAssignments[pod] = make(map[string]cpuset.CPUSet, len(cp.CheckpointData.Entries[pod]))
		for container, cpuString := range cp.CheckpointData.Entries[pod] {
			if tmpContainerCPUSet, err = cpuset.Parse(cpuString); err != nil {
				return fmt.Errorf("could not parse cpuset %q for container %q in pod %q: %w", cpuString, container, pod, err)
			}
			tmpAssignments[pod][container] = tmpContainerCPUSet
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		if len(cp.CheckpointData.Baselines) > 0 {
			// 从 checkpoint 数据中恢复各容器的 Baseline。
			for pod := range cp.CheckpointData.Baselines {
				tmpBaselines[pod] = make(map[string]ContainerCPUBaseline, len(cp.CheckpointData.Baselines[pod]))
				for container, cpuBaselines := range cp.CheckpointData.Baselines[pod] {
					if tmpContainerBaselineCPUSet, err = cpuset.Parse(cpuBaselines.Baseline); err != nil {
						return fmt.Errorf("could not parse Baseline cpuset %q for container %q in pod %q: %w", cpuBaselines.Baseline, container, pod, err)
					}
					tmpBaselines[pod][container] = ContainerCPUBaseline{Baseline: tmpContainerBaselineCPUSet}
				}
			}
		} else {
			// 只有未启用原地垂直扩缩容（独占 CPU）的 kubelet 才会不设置 Baselines。
			// 此时从未发生过 resize，两组分配天然一致，因此可以安全地用 Entries 充当 Baselines。
			for pod := range tmpAssignments {
				tmpBaselines[pod] = make(map[string]ContainerCPUBaseline, len(tmpAssignments[pod]))
				for container, entries := range tmpAssignments[pod] {
					tmpBaselines[pod][container] = ContainerCPUBaseline{Baseline: entries.Clone()}
				}
			}
		}
	}

	// 恢复结果写入内存缓存，供后续 CPU 分配逻辑读取。
	sc.cache.SetDefaultCPUSet(tmpDefaultCPUSet)
	sc.cache.SetCPUAssignments(tmpAssignments)
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		sc.cache.SetCPUBaselines(tmpBaselines)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		sc.cache.SetPodCPUAssignments(cp.CheckpointData.PodEntries)
	}

	sc.logger.V(2).Info("restored state from checkpoint")
	sc.logger.V(2).Info("defaultCPUSet", "defaultCpuSet", tmpDefaultCPUSet.String())

	return nil
}

// loadAndMigrateCheckpointV4 加载最新的 v4 checkpoint；若不存在或无法解析，则按需从更老版本迁移。
func (sc *stateCheckpoint) loadAndMigrateCheckpointV4() (*CPUManagerCheckpointV4, error) {
	sc.logger.Info("trying to load v4 CPU manager checkpoint")
	checkpointV4 := newCPUManagerCheckpointV4()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV4)
	if err == nil {
		return checkpointV4, nil
	}
	if errors.Is(err, cperrors.ErrCheckpointNotFound) {
		return nil, err
	}

	if len(checkpointV4.Data) > 0 || checkpointV4.DataChecksum != 0 {
		// v4 checkpoint 已写入部分数据，说明文件损坏：不能回退到更老版本，直接报错，
		// 避免用与磁盘不一致的状态启动。
		err = fmt.Errorf("could not load v4 checkpoint: %w", err)
		sc.logger.Error(err, "not falling back to previous versions")
		return nil, err
	}

	// v4 解析失败且没有残留数据，说明可能是旧版本文件，记录错误后回退到 v3。
	sc.logger.Info("could not load v4 checkpoint, falling back to v3", "err", err)

	// 尝试以 v3 格式加载。
	checkpointV3, err := sc.loadAndMigrateCheckpointV3()
	if err != nil {
		return nil, err
	}

	// 重新构造空的 v4 结构，丢弃上次加载尝试可能残留的数据。
	checkpointV4 = newCPUManagerCheckpointV4()

	// 已加载 v3，现在将其迁移为 v4。
	sc.logger.Info("migrating CPU manager checkpoint from v3 to v4")
	sc.migrateV3CheckpointToV4Checkpoint(checkpointV3, checkpointV4)

	return checkpointV4, nil
}

// loadAndMigrateCheckpointV3 加载 v3 格式的 checkpoint，并按需从更老的版本迁移。
func (sc *stateCheckpoint) loadAndMigrateCheckpointV3() (*CPUManagerCheckpointV3, error) {
	sc.logger.Info("trying to load v3 CPU manager checkpoint")
	checkpointV3 := newCPUManagerCheckpointV3()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV3)
	if err == nil {
		return checkpointV3, nil
	}
	if errors.Is(err, cperrors.ErrCheckpointNotFound) {
		return nil, err
	}

	// v3 解析失败，记录错误后回退到 v2。
	sc.logger.Info("could not load v3 checkpoint, falling back to v2", "err", err)

	// 尝试以 v2 格式加载。
	checkpointV2, err := sc.loadCheckpointV2()
	if err != nil {
		return nil, err
	}

	// 重新构造空的 v3 结构，丢弃上次加载尝试可能残留的数据。
	checkpointV3 = newCPUManagerCheckpointV3()

	// 已加载 v2，现在将其迁移为 v3。
	sc.logger.Info("migrating CPU manager checkpoint from v2 to v3")
	sc.migrateV2CheckpointToV3Checkpoint(checkpointV2, checkpointV3)

	return checkpointV3, nil
}

// loadCheckpointV2 加载 v2 格式的 checkpoint。
// 这是当前支持的最老版本，因此不再尝试从 v1 迁移。
func (sc *stateCheckpoint) loadCheckpointV2() (*CPUManagerCheckpointV2, error) {
	sc.logger.Info("trying to load v2 cpu manager checkpoint")
	checkpointV2 := newCPUManagerCheckpointV2()
	err := sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpointV2)
	if err == nil {
		return checkpointV2, nil
	}

	// 所有尝试均失败，返回最后得到的错误。
	return nil, err
}

// storeState 把当前状态写入 checkpoint；调用方需自行持有锁。
func (sc *stateCheckpoint) storeState() error {
	checkpoint := newCPUManagerCheckpoint()
	checkpoint.CheckpointData.PolicyName = sc.policyName
	checkpoint.CheckpointData.DefaultCPUSet = sc.cache.GetDefaultCPUSet().String()
	assignments := sc.cache.GetCPUAssignments()
	for pod := range assignments {
		// 按容器数预分配容量，避免写入过程中反复扩容。
		checkpoint.CheckpointData.Entries[pod] = make(map[string]string, len(assignments[pod]))
		for container, cset := range assignments[pod] {
			checkpoint.CheckpointData.Entries[pod][container] = cset.String()
		}
	}
	// 启用原地垂直扩缩容（独占 CPU）时，Baselines 需要一并持久化，供 resize 时对比。
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		baselines := sc.cache.GetCPUBaselines()
		for pod := range baselines {
			checkpoint.CheckpointData.Baselines[pod] = make(map[string]ContainerCPUs, len(baselines[pod]))
			for container, cpuAllocation := range baselines[pod] {
				checkpoint.CheckpointData.Baselines[pod][container] = ContainerCPUs{Baseline: cpuAllocation.Baseline.String()}
			}
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		podAssignments := sc.cache.GetPodCPUAssignments()
		maps.Copy(checkpoint.CheckpointData.PodEntries, podAssignments)
	}

	err := sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint)
	if err != nil {
		sc.logger.Error(err, "Failed to save checkpoint")
		return err
	}
	return nil
}

// GetCPUSet 返回指定 pod 容器当前的 CPU 集合，并返回是否找到。
func (sc *stateCheckpoint) GetCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	res, ok := sc.cache.GetCPUSet(podUID, containerName)
	return res, ok
}

// GetDefaultCPUSet 返回默认 CPU 集合。
func (sc *stateCheckpoint) GetDefaultCPUSet() cpuset.CPUSet {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetDefaultCPUSet()
}

// GetCPUSetOrDefault 返回指定 pod 容器当前的 CPU 集合；若尚未单独分配，则返回默认 CPU 集合。
func (sc *stateCheckpoint) GetCPUSetOrDefault(podUID string, containerName string) cpuset.CPUSet {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetCPUSetOrDefault(podUID, containerName)
}

// GetCPUAssignments 返回当前所有 pod 的容器级 CPU 分配结果。
func (sc *stateCheckpoint) GetCPUAssignments() ContainerCPUAssignments {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetCPUAssignments()
}

// GetPodCPUAssignments 返回当前所有 pod 的 pod 级别 CPU 分配结果。
func (sc *stateCheckpoint) GetPodCPUAssignments() PodCPUAssignments {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetPodCPUAssignments()
}

// SetPodCPUAssignments 设置 pod 级别的 CPU 分配结果，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetPodCPUAssignments(assignments PodCPUAssignments) {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	sc.cache.SetPodCPUAssignments(assignments)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// GetPodCPUSet 返回指定 pod 的 pod 级别 CPU 集合，并返回是否找到。
func (sc *stateCheckpoint) GetPodCPUSet(podUID string) (cpuset.CPUSet, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()
	return sc.cache.GetPodCPUSet(podUID)
}

// SetCPUSet 设置指定 pod 容器的 CPU 集合，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetCPUSet(podUID string, containerName string, cset cpuset.CPUSet) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.SetCPUSet(podUID, containerName, cset)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID, "containerName", containerName)
	}
}

// SetDefaultCPUSet 设置默认 CPU 集合，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetDefaultCPUSet(cset cpuset.CPUSet) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.SetDefaultCPUSet(cset)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// SetCPUAssignments 设置容器级 CPU 分配结果，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetCPUAssignments(a ContainerCPUAssignments) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.SetCPUAssignments(a)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// SetPodCPUSet 设置指定 pod 的 pod 级别 CPU 集合，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetPodCPUSet(podUID string, cset cpuset.CPUSet) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.SetPodCPUSet(podUID, cset)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID)
	}
}

// Delete 删除指定 pod 容器的分配记录，并同步持久化到 checkpoint。
func (sc *stateCheckpoint) Delete(podUID string, containerName string) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.Delete(podUID, containerName)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID, "containerName", containerName)
	}
}

// DeletePod 删除指定 pod 的 pod 级别 CPU 分配记录。
// 该操作不影响容器级别的分配。
func (sc *stateCheckpoint) DeletePod(podUID string) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.DeletePod(podUID)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint", "podUID", podUID)
	}
}

// ClearState 清空当前状态，并把清空后的状态保存到 checkpoint。
func (sc *stateCheckpoint) ClearState() {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.ClearState()
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}

// GetBaselineCPUSet 返回容器在准入时记录、且在任何 resize 发生之前的 CPU 分配（Baseline）。
func (sc *stateCheckpoint) GetBaselineCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetBaselineCPUSet(podUID, containerName)
}

// GetCPUBaselines 返回所有容器在准入时记录、且在任何 resize 发生之前的 CPU 分配（Baseline）。
func (sc *stateCheckpoint) GetCPUBaselines() ContainerCPUBaselines {
	sc.mux.RLock()
	defer sc.mux.RUnlock()

	return sc.cache.GetCPUBaselines()
}

// SetCPUBaselines 保存所有容器在准入时记录、且在任何 resize 发生之前的 CPU 分配（Baseline），并同步持久化到 checkpoint。
func (sc *stateCheckpoint) SetCPUBaselines(a ContainerCPUBaselines) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	sc.cache.SetCPUBaselines(a)
	err := sc.storeState()
	if err != nil {
		sc.logger.Error(err, "Failed to store state to checkpoint")
	}
}
