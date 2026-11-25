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

package kuberuntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// containerGC 是容器与沙箱垃圾回收的管理器。
type containerGC struct {
	client           internalapi.RuntimeService
	manager          *kubeGenericRuntimeManager
	podStateProvider podStateProvider
	tracer           trace.Tracer
}

// newContainerGC 创建一个新的 containerGC。
func newContainerGC(client internalapi.RuntimeService, podStateProvider podStateProvider, manager *kubeGenericRuntimeManager, tracer trace.Tracer) *containerGC {
	return &containerGC{
		client:           client,
		manager:          manager,
		podStateProvider: podStateProvider,
		tracer:           tracer,
	}
}

// containerGCInfo 是参与 GC 评估的容器的内部信息。
type containerGCInfo struct {
	// 容器 ID。
	id string
	// 容器名称。
	name string
	// 容器的创建时间。
	createTime time.Time
	// 为 true 表示容器处于未知状态，垃圾回收器应在删除前先尝试停止它。
	unknown bool
}

// sandboxGCInfo 是参与 GC 评估的沙箱的内部信息。
type sandboxGCInfo struct {
	// 沙箱 ID。
	id string
	// 沙箱的创建时间。
	createTime time.Time
	// 为 true 表示沙箱处于就绪状态或仍包含容器。
	active bool
}

// evictUnit 以 (Pod UID, 容器名) 为粒度作为回收单元。
type evictUnit struct {
	// Pod 的 UID。
	uid types.UID
	// Pod 中容器的名称。
	name string
}

// containersByEvictUnit 按回收单元分组保存容器信息。
type containersByEvictUnit map[evictUnit][]containerGCInfo

// sandboxesByPodUID 按 Pod UID 分组保存沙箱信息。
type sandboxesByPodUID map[types.UID][]sandboxGCInfo

// NumContainers 返回该映射中的容器总数。
func (cu containersByEvictUnit) NumContainers() int {
	num := 0
	for key := range cu {
		num += len(cu[key])
	}
	return num
}

// NumEvictUnits 返回该映射中的回收单元数量。
func (cu containersByEvictUnit) NumEvictUnits() int {
	return len(cu)
}

// byCreated 按创建时间从新到旧排序容器信息。
type byCreated []containerGCInfo

// Len 返回待排序容器的数量。
func (a byCreated) Len() int { return len(a) }

// Swap 交换两个元素的位置。
func (a byCreated) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 定义排序规则：创建时间更晚的排在前面。
func (a byCreated) Less(i, j int) bool { return a[i].createTime.After(a[j].createTime) }

// sandboxByCreated 按创建时间从新到旧排序沙箱信息。
type sandboxByCreated []sandboxGCInfo

// Len 返回待排序沙箱的数量。
func (a sandboxByCreated) Len() int { return len(a) }

// Swap 交换两个元素的位置。
func (a sandboxByCreated) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 定义排序规则：创建时间更晚的排在前面。
func (a sandboxByCreated) Less(i, j int) bool { return a[i].createTime.After(a[j].createTime) }

// enforceMaxContainersPerEvictUnit 对每个回收单元执行 MaxPerPodContainer 限制。
func (cgc *containerGC) enforceMaxContainersPerEvictUnit(ctx context.Context, evictUnits containersByEvictUnit, MaxContainers int) {
	for key := range evictUnits {
		toRemove := len(evictUnits[key]) - MaxContainers

		if toRemove > 0 {
			evictUnits[key] = cgc.removeOldestN(ctx, evictUnits[key], toRemove)
		}
	}
}

// removeOldestN 删除最旧的 toRemove 个容器，并返回剩余的容器切片。
func (cgc *containerGC) removeOldestN(ctx context.Context, containers []containerGCInfo, toRemove int) []containerGCInfo {
	logger := klog.FromContext(ctx)
	// 按从旧到新（从后到前）的顺序删除。
	numToKeep := len(containers) - toRemove
	if numToKeep > 0 {
		sort.Sort(byCreated(containers))
	}
	for i := len(containers) - 1; i >= numToKeep; i-- {
		if containers[i].unknown {
			// 处于未知状态的容器可能仍在运行，删除前应先尝试停止它。
			id := kubecontainer.ContainerID{
				Type: cgc.manager.runtimeName,
				ID:   containers[i].id,
			}
			message := "Container is in unknown state, try killing it before removal"
			if err := cgc.manager.killContainer(ctx, nil, id, containers[i].name, message, reasonUnknown, nil, nil); err != nil {
				logger.Error(err, "Failed to stop container", "containerID", containers[i].id)
				continue
			}
		}
		if err := cgc.manager.removeContainer(ctx, containers[i].id, false); err != nil {
			logger.Error(err, "Failed to remove container", "containerID", containers[i].id)
		}
	}

	// 假定容器已被删除并据此返回剩余部分，避免回收过于激进。
	return containers[:numToKeep]
}

// removeOldestNSandboxes 删除最旧的 toRemove 个非活跃沙箱。
func (cgc *containerGC) removeOldestNSandboxes(ctx context.Context, sandboxes []sandboxGCInfo, toRemove int) {
	numToKeep := len(sandboxes) - toRemove
	if numToKeep > 0 {
		sort.Sort(sandboxByCreated(sandboxes))
	}
	// 按从旧到新（从后到前）的顺序删除。
	for i := len(sandboxes) - 1; i >= numToKeep; i-- {
		if !sandboxes[i].active {
			cgc.removeSandbox(ctx, sandboxes[i].id)
		}
	}
}

// removeSandbox 按 sandboxID 删除沙箱。
func (cgc *containerGC) removeSandbox(ctx context.Context, sandboxID string) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Removing sandbox", "sandboxID", sandboxID)
	// 正常情况下 kubelet 在 GC 之前已经调用过 StopPodSandbox。
	// 为防范少数未调用的情况，删除前先尝试停止沙箱。
	if err := cgc.client.StopPodSandbox(ctx, sandboxID); err != nil {
		logger.Error(err, "Failed to stop sandbox before removing", "sandboxID", sandboxID)
		return
	}
	if err := cgc.client.RemovePodSandbox(ctx, sandboxID); err != nil {
		logger.Error(err, "Failed to remove sandbox", "sandboxID", sandboxID)
	}
}

// evictableContainers 获取所有可回收的容器：未在运行且创建时间超过 MinAge。
func (cgc *containerGC) evictableContainers(ctx context.Context, minAge time.Duration) (containersByEvictUnit, error) {
	containers, err := cgc.manager.getContainers(ctx, listOptions{})
	if err != nil {
		return containersByEvictUnit{}, err
	}

	evictUnits := make(containersByEvictUnit)
	newestGCTime := time.Now().Add(-minAge)
	for _, container := range containers {
		// 剔除运行中的容器。
		if container.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}

		// 创建时间晚于 MinAge 边界的容器尚未“成熟”，本轮不回收。
		createdAt := time.Unix(0, container.CreatedAt)
		if newestGCTime.Before(createdAt) {
			continue
		}

		labeledInfo := getContainerInfoFromLabels(ctx, container.Labels)
		containerInfo := containerGCInfo{
			id:         container.Id,
			name:       container.Metadata.Name,
			createTime: createdAt,
			unknown:    container.State == runtimeapi.ContainerState_CONTAINER_UNKNOWN,
		}
		key := evictUnit{
			uid:  labeledInfo.PodUID,
			name: containerInfo.name,
		}
		evictUnits[key] = append(evictUnits[key], containerInfo)
	}

	return evictUnits, nil
}

// evictContainers 回收所有可回收的容器。
func (cgc *containerGC) evictContainers(ctx context.Context, gcPolicy kubecontainer.GCPolicy, allSourcesReady bool, evictNonDeletedPods bool) error {
	// 先按回收单元对可回收容器分组。
	evictUnits, err := cgc.evictableContainers(ctx, gcPolicy.MinAge)
	if err != nil {
		return err
	}

	// 所有配置源就绪时，删除已消失 Pod 的全部容器。
	if allSourcesReady {
		for key, unit := range evictUnits {
			if cgc.podStateProvider.ShouldPodContentBeRemoved(key.uid) || (evictNonDeletedPods && cgc.podStateProvider.ShouldPodRuntimeBeRemoved(key.uid)) {
				cgc.removeOldestN(ctx, unit, len(unit)) // 移除全部。
				delete(evictUnits, key)
			}
		}
	}

	// 执行每个回收单元的容器数量上限。
	if gcPolicy.MaxPerPodContainer >= 0 {
		cgc.enforceMaxContainersPerEvictUnit(ctx, evictUnits, gcPolicy.MaxPerPodContainer)
	}

	// 执行容器总数上限。
	if gcPolicy.MaxContainers >= 0 && evictUnits.NumContainers() > gcPolicy.MaxContainers {
		// 为每个回收单元保留相同数量的容器（至少 1 个）。
		numContainersPerEvictUnit := gcPolicy.MaxContainers / evictUnits.NumEvictUnits()
		if numContainersPerEvictUnit < 1 {
			numContainersPerEvictUnit = 1
		}
		cgc.enforceMaxContainersPerEvictUnit(ctx, evictUnits, numContainersPerEvictUnit)

		// 若仍需回收，则从最旧的容器开始。
		numContainers := evictUnits.NumContainers()
		if numContainers > gcPolicy.MaxContainers {
			flattened := make([]containerGCInfo, 0, numContainers)
			for key := range evictUnits {
				flattened = append(flattened, evictUnits[key]...)
			}
			sort.Sort(byCreated(flattened))

			cgc.removeOldestN(ctx, flattened, numContainers-gcPolicy.MaxContainers)
		}
	}
	return nil
}

// evictSandboxes 回收所有可回收的沙箱。可回收沙箱必须同时满足：
//  1. 不处于就绪状态；
//  2. 不包含任何容器；
//  3. 属于已不存在（即已被删除）的 Pod，或不是该 Pod 最近创建的沙箱。
func (cgc *containerGC) evictSandboxes(ctx context.Context, evictNonDeletedPods bool) error {
	containers, err := cgc.manager.getContainers(ctx, listOptions{})
	if err != nil {
		return err
	}

	sandboxes, err := cgc.manager.getSandboxes(ctx, listOptions{})
	if err != nil {
		return err
	}

	// 收集所有容器引用的 PodSandboxId，用于判断沙箱是否仍被使用。
	sandboxIDs := sets.New[string]()
	for _, container := range containers {
		sandboxIDs.Insert(container.PodSandboxId)
	}

	sandboxesByPod := make(sandboxesByPodUID, len(sandboxes))
	for _, sandbox := range sandboxes {
		podUID := types.UID(sandbox.Metadata.Uid)
		sandboxInfo := sandboxGCInfo{
			id:         sandbox.Id,
			createTime: time.Unix(0, sandbox.CreatedAt),
		}

		// 就绪的沙箱以及仍包含容器的沙箱都标记为活跃。
		if sandbox.State == runtimeapi.PodSandboxState_SANDBOX_READY || sandboxIDs.Has(sandbox.Id) {
			sandboxInfo.active = true
		}

		sandboxesByPod[podUID] = append(sandboxesByPod[podUID], sandboxInfo)
	}

	for podUID, sandboxes := range sandboxesByPod {
		if cgc.podStateProvider.ShouldPodContentBeRemoved(podUID) || (evictNonDeletedPods && cgc.podStateProvider.ShouldPodRuntimeBeRemoved(podUID)) {
			// Pod 已被删除时回收其全部可回收沙箱。
			// 注意：若已存在活跃沙箱，最新的已死沙箱也会被回收。
			cgc.removeOldestNSandboxes(ctx, sandboxes, len(sandboxes))
		} else {
			// Pod 仍存在时至少保留最新的一个沙箱。
			cgc.removeOldestNSandboxes(ctx, sandboxes, len(sandboxes)-1)
		}
	}
	return nil
}

// evictPodLogsDirectories 回收所有可回收的 Pod 日志目录：
// 不存在对应 Pod 的日志目录即可回收。
func (cgc *containerGC) evictPodLogsDirectories(ctx context.Context, allSourcesReady bool) error {
	logger := klog.FromContext(ctx)
	osInterface := cgc.manager.osInterface
	podLogsDirectory := cgc.manager.podLogsDirectory
	if allSourcesReady {
		// 仅当所有配置源就绪时才删除 Pod 日志目录，避免误删仍在使用的目录。
		dirs, err := osInterface.ReadDir(podLogsDirectory)
		if err != nil {
			return fmt.Errorf("failed to read podLogsDirectory %q: %w", podLogsDirectory, err)
		}
		for _, dir := range dirs {
			name := dir.Name()
			podUID := parsePodUIDFromLogsDirectory(name)
			if !cgc.podStateProvider.ShouldPodContentBeRemoved(podUID) {
				continue
			}
			logger.V(4).Info("Removing pod logs", "podUID", podUID)
			err := osInterface.RemoveAll(filepath.Join(podLogsDirectory, name))
			if err != nil {
				logger.Error(err, "Failed to remove pod logs directory", "path", name)
			}
		}
	}

	// 清理失效的容器日志符号链接。
	// TODO(random-liu): 集群日志支持 CRI 容器日志路径后，删除这段兼容逻辑。
	logSymlinks, _ := osInterface.Glob(filepath.Join(legacyContainerLogsDir, fmt.Sprintf("*.%s", legacyLogSuffix)))
	for _, logSymlink := range logSymlinks {
		if _, err := osInterface.Stat(logSymlink); os.IsNotExist(err) {
			if containerID, err := getContainerIDFromLegacyLogSymlink(logSymlink); err == nil {
				resp, err := cgc.manager.runtimeService.ContainerStatus(ctx, containerID, false)
				if err != nil {
					// TODO: 待 https://github.com/kubernetes/kubernetes/issues/63336
					// 解决后，应对“容器不存在（已被删除）”的情况做单独处理。
					logger.Info("Error getting ContainerStatus for containerID", "containerID", containerID, "err", err)
				} else {
					status := resp.GetStatus()
					if status == nil {
						logger.V(4).Info("Container status is nil")
						continue
					}
					if status.State != runtimeapi.ContainerState_CONTAINER_EXITED {
						// 容器日志轮转的流程如下（参见 containerLogManager#rotateLatestLog）：
						//
						// 1. 将当前日志重命名为带时间戳的轮转文件（fmt.Sprintf("%s.%s", log, timestamp)）；
						// 2. 重新打开容器日志；
						// 3. 若第 2 步失败，把轮转文件改回原日志名。
						//
						// 在第 1 步与第 2 步之间、第 1 步与第 3 步之间存在短暂且不确定的
						// 日志文件缺失窗口，此时符号链接可能被误判为失效。
						// 参见 https://github.com/kubernetes/kubernetes/issues/52172
						//
						// 因此只对已死容器删除失效的符号链接。
						logger.V(5).Info("Container is still running, not removing symlink", "containerID", containerID, "path", logSymlink)
						continue
					}
				}
			} else {
				logger.V(4).Info("Unable to obtain container ID", "err", err)
			}
			err := osInterface.Remove(logSymlink)
			if err != nil {
				logger.Error(err, "Failed to remove container log dead symlink", "path", logSymlink)
			} else {
				logger.V(4).Info("Removed symlink", "path", logSymlink)
			}
		}
	}
	return nil
}

// GarbageCollect 按指定的容器 GC 策略删除已死容器。
// 注意 GC 策略不作用于沙箱：沙箱只在未就绪且不含任何容器时才会被删除。
//
// GarbageCollect 包含以下步骤：
// * 找出非活跃且创建时间超过 gcPolicy.MinAge 的可回收容器；
// * 按 gcPolicy.MaxPerPodContainer 删除每个 Pod 中最旧的已死容器；
// * 按 gcPolicy.MaxContainers 删除最旧的已死容器；
// * 找出未就绪且不含容器的可回收沙箱；
// * 删除可回收沙箱。
func (cgc *containerGC) GarbageCollect(ctx context.Context, gcPolicy kubecontainer.GCPolicy, allSourcesReady bool, evictNonDeletedPods bool) error {
	ctx, otelSpan := cgc.tracer.Start(ctx, "Containers/GarbageCollect")
	defer otelSpan.End()
	errors := []error{}
	// 回收可回收的容器。
	if err := cgc.evictContainers(ctx, gcPolicy, allSourcesReady, evictNonDeletedPods); err != nil {
		errors = append(errors, err)
	}

	// 回收不含容器的沙箱。
	if err := cgc.evictSandboxes(ctx, evictNonDeletedPods); err != nil {
		errors = append(errors, err)
	}

	// 回收 Pod 沙箱的日志目录。
	if err := cgc.evictPodLogsDirectories(ctx, allSourcesReady); err != nil {
		errors = append(errors, err)
	}
	// 聚合各步骤错误，尽量让后续步骤仍然执行。
	return utilerrors.NewAggregate(errors)
}
