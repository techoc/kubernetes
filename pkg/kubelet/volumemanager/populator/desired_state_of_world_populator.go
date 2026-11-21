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

// Package populator 实现一组接口，负责让缓存状态与“真实状态”保持同步。
package populator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// DesiredStateOfWorldPopulator 周期性遍历活跃 Pod，确保带卷的 Pod
// 都存在于期望状态缓存中；同时校验缓存中的 Pod 是否仍然存在，
// 不存在则将其移除。
type DesiredStateOfWorldPopulator interface {
	// Run 启动填充循环：先等到配置源就绪并完成首轮填充，之后持续调谐。
	Run(ctx context.Context, sourcesReady config.SourcesReady)

	// ReprocessPod 把指定 Pod 在 processedPods 中标记为未处理，强制重新处理。
	// Pod 更新后需要重新挂载卷（例如 Downward API 卷依赖该行为刷新内容）。
	ReprocessPod(podName volumetypes.UniquePodName)

	// HasAddedPods 表示在配置源全部就绪之后，填充器是否至少完整遍历过一次
	// 活跃 Pod 列表并把它们加入期望状态缓存。就绪之前不会返回 true，
	// 因为此时活跃 Pod 列表可能严重不完整。
	HasAddedPods() bool
}

// PodStateProvider 用于判断 Pod 是否即将终止。
type PodStateProvider interface {
	// ShouldPodContainersBeTerminating 判断 Pod 的容器是否正在终止。
	ShouldPodContainersBeTerminating(types.UID) bool
	// ShouldPodRuntimeBeRemoved 判断 Pod 的运行时是否已被彻底移除。
	ShouldPodRuntimeBeRemoved(types.UID) bool
}

// PodManager 是填充器观察 kubelet 实际状态所需的方法子集。
// 方法语义见 kubelet/pod.Manager。
type PodManager interface {
	// GetPodByUID 按 UID 返回 Pod；第二个返回值表示是否存在。
	GetPodByUID(types.UID) (*v1.Pod, bool)
	// GetPods 返回节点上已知的全部 Pod。
	GetPods() []*v1.Pod
}

// NewDesiredStateOfWorldPopulator 返回新的 DesiredStateOfWorldPopulator。
//
// kubeClient - 用于从 API server 获取 PV 与 PVC 对象
// loopSleepDuration - 填充循环相邻两次执行之间的休眠时间
//
// podManager - kubelet 的 PodManager，是节点上 Pod 真实状态的来源
//
// desiredStateOfWorld - 待填充的缓存
func NewDesiredStateOfWorldPopulator(
	kubeClient clientset.Interface,
	loopSleepDuration time.Duration,
	podManager PodManager,
	podStateProvider PodStateProvider,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	csiMigratedPluginManager csimigration.PluginManager,
	intreeToCSITranslator csimigration.InTreeToCSITranslator,
	volumePluginMgr *volume.VolumePluginMgr) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		kubeClient:          kubeClient,
		loopSleepDuration:   loopSleepDuration,
		podManager:          podManager,
		podStateProvider:    podStateProvider,
		desiredStateOfWorld: desiredStateOfWorld,
		actualStateOfWorld:  actualStateOfWorld,
		pods: processedPods{
			processedPods: make(map[volumetypes.UniquePodName]bool)},
		hasAddedPods:             false,
		hasAddedPodsLock:         sync.RWMutex{},
		csiMigratedPluginManager: csiMigratedPluginManager,
		intreeToCSITranslator:    intreeToCSITranslator,
		volumePluginMgr:          volumePluginMgr,
	}
}

// desiredStateOfWorldPopulator 是 DesiredStateOfWorldPopulator 的实现。
type desiredStateOfWorldPopulator struct {
	// kubeClient 用于从 API server 获取 PV/PVC。
	kubeClient clientset.Interface
	// loopSleepDuration 是两次填充循环之间的休眠时间。
	loopSleepDuration time.Duration
	// podManager 提供节点上 Pod 的真实列表。
	podManager PodManager
	// podStateProvider 判断 Pod 是否正在终止或运行时已移除。
	podStateProvider PodStateProvider
	// desiredStateOfWorld 是本填充器要维护的期望状态缓存。
	desiredStateOfWorld cache.DesiredStateOfWorld
	// actualStateOfWorld 用于查询卷的实际挂载状态。
	actualStateOfWorld cache.ActualStateOfWorld
	// pods 记录每个 Pod 是否已被完整处理。
	pods processedPods
	// hasAddedPods 表示是否至少完成过一轮初始填充。
	hasAddedPods bool
	// hasAddedPodsLock 保护 hasAddedPods，读多写少。
	hasAddedPodsLock sync.RWMutex
	// csiMigratedPluginManager 判断卷插件是否可迁移到 CSI。
	csiMigratedPluginManager csimigration.PluginManager
	// intreeToCSITranslator 把树内卷规格转换为 CSI 规格。
	intreeToCSITranslator csimigration.InTreeToCSITranslator
	// volumePluginMgr 用于查找卷插件。
	volumePluginMgr *volume.VolumePluginMgr
}

// processedPods 并发安全地记录 Pod 是否已被填充器完整处理。
type processedPods struct {
	// processedPods 的 value 为 true 表示已处理，false 表示需要重新处理。
	processedPods map[volumetypes.UniquePodName]bool
	// RWMutex 保护 processedPods，填充循环与 Pod 同步会并发访问。
	sync.RWMutex
}

func (dswp *desiredStateOfWorldPopulator) Run(ctx context.Context, sourcesReady config.SourcesReady) {
	// 等待配置源全部就绪后启动的那一轮循环完成，再据此设置 hasAddedPods。
	logger := klog.FromContext(ctx)
	logger.Info("Desired state populator starts to run")
	// 在配置源就绪前持续执行填充循环，就绪后完成当前轮再退出轮询。
	_ = wait.PollUntilContextCancel(ctx, dswp.loopSleepDuration, false, func(ctx context.Context) (bool, error) {
		done := sourcesReady.AllReady()
		dswp.populatorLoop(ctx)
		return done, nil
	})
	// 首轮完成后标记已添加 Pod，供卷管理器判断是否可以开始工作。
	dswp.hasAddedPodsLock.Lock()
	if !dswp.hasAddedPods {
		logger.Info("Finished populating initial desired state of world")
		dswp.hasAddedPods = true
	}
	dswp.hasAddedPodsLock.Unlock()
	// 之后按固定间隔持续调谐。
	wait.UntilWithContext(ctx, dswp.populatorLoop, dswp.loopSleepDuration)
}

// ReprocessPod 把 Pod 标记为未处理，使下一轮循环重新处理其卷。
func (dswp *desiredStateOfWorldPopulator) ReprocessPod(
	podName volumetypes.UniquePodName) {
	dswp.markPodProcessingFailed(podName)
}

// HasAddedPods 返回是否已完成至少一轮初始填充。
func (dswp *desiredStateOfWorldPopulator) HasAddedPods() bool {
	dswp.hasAddedPodsLock.RLock()
	defer dswp.hasAddedPodsLock.RUnlock()
	return dswp.hasAddedPods
}

// populatorLoop 执行一轮填充：先添加新 Pod 的卷，再删除已消失 Pod 的卷。
func (dswp *desiredStateOfWorldPopulator) populatorLoop(ctx context.Context) {
	logger := klog.FromContext(ctx)
	dswp.findAndAddNewPods(ctx)
	dswp.findAndRemoveDeletedPods(logger)
}

// 遍历所有 Pod，把应当存在但尚未加入期望状态的卷添加进去。
func (dswp *desiredStateOfWorldPopulator) findAndAddNewPods(ctx context.Context) {
	for _, pod := range dswp.podManager.GetPods() {
		// 调谐阶段不再为正在终止的 Pod 添加卷，避免与容器启动竞争。
		if dswp.hasAddedPods && dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
			// 不能启动容器的 Pod 也不应重新添加卷。
			continue
		}

		if !dswp.hasAddedPods && dswp.podStateProvider.ShouldPodRuntimeBeRemoved(pod.UID) {
			// kubelet 重启后，只要运行时可能仍有容器在跑，就需要把 Pod 加回期望状态。
			continue
		}

		dswp.processPodVolumes(ctx, pod)
	}
}

// 遍历期望状态中的所有 Pod，不再存在的从缓存中移除。
func (dswp *desiredStateOfWorldPopulator) findAndRemoveDeletedPods(logger klog.Logger) {
	podsFromCache := make(map[volumetypes.UniquePodName]struct{})
	for _, volumeToMount := range dswp.desiredStateOfWorld.GetVolumesToMount() {
		podsFromCache[volumetypes.UniquePodName(volumeToMount.Pod.UID)] = struct{}{}
		pod, podExists := dswp.podManager.GetPodByUID(volumeToMount.Pod.UID)
		if podExists {

			// 检查该卷的可挂载属性是否发生变化。
			if volumeToMount.PluginIsAttachable {
				attachableVolumePlugin, err := dswp.volumePluginMgr.FindAttachablePluginBySpec(volumeToMount.VolumeSpec)
				// 只有这种组合才说明插件确实不可挂载。
				if err == nil && attachableVolumePlugin == nil {
					// 目前 CSI 插件不会同时满足可挂载与不可设备挂载，
					// 因此属性变化后 uniqueVolumeName 保持不变。
					dswp.desiredStateOfWorld.MarkVolumeAttachability(volumeToMount.VolumeName, false)
					logger.Info("Volume changes from attachable to non-attachable", "volumeName", volumeToMount.VolumeName)
					continue
				}
			}

			// 仍在运行且预期存在的 Pod 跳过删除。
			if !dswp.podStateProvider.ShouldPodRuntimeBeRemoved(pod.UID) {
				continue
			}
		}

		// Pod 从 kubelet PodManager 删除后不要立刻从卷管理器删除，
		// 必须先通过 podStateProvider 确认其中所有容器都已终止。
		if !dswp.podStateProvider.ShouldPodRuntimeBeRemoved(volumeToMount.Pod.UID) {
			logger.V(4).Info("Pod still has one or more containers in the non-exited state and will not be removed from desired state", "pod", klog.KObj(volumeToMount.Pod))
			continue
		}
		var volumeToMountSpecName string
		if volumeToMount.VolumeSpec != nil {
			volumeToMountSpecName = volumeToMount.VolumeSpec.Name()
		}
		removed := dswp.actualStateOfWorld.PodRemovedFromVolume(volumeToMount.PodName, volumeToMount.VolumeName)
		if removed && podExists {
			logger.V(4).Info("Actual state does not yet have volume mount information and pod still exists in pod manager, skip removing volume from desired state", "pod", klog.KObj(volumeToMount.Pod), "podUID", volumeToMount.Pod.UID, "volumeName", volumeToMountSpecName)
			continue
		}
		logger.V(4).Info("Removing volume from desired state", "pod", klog.KObj(volumeToMount.Pod), "podUID", volumeToMount.Pod.UID, "volumeName", volumeToMountSpecName)
		dswp.desiredStateOfWorld.DeletePodFromVolume(
			volumeToMount.PodName, volumeToMount.VolumeName)
		dswp.deleteProcessedPod(volumeToMount.PodName)
	}

	// 清理 processedPods 中没有对应期望状态的孤儿记录。
	dswp.pods.Lock()
	orphanedPods := make([]volumetypes.UniquePodName, 0, len(dswp.pods.processedPods))
	for k := range dswp.pods.processedPods {
		if _, ok := podsFromCache[k]; !ok {
			orphanedPods = append(orphanedPods, k)
		}
	}
	dswp.pods.Unlock()
	// 只有 Pod 已不存在且运行时确实移除后，才删除该记录。
	for _, orphanedPod := range orphanedPods {
		uid := types.UID(orphanedPod)
		_, podExists := dswp.podManager.GetPodByUID(uid)
		if !podExists && dswp.podStateProvider.ShouldPodRuntimeBeRemoved(uid) {
			dswp.deleteProcessedPod(orphanedPod)
		}
	}

	// 清理已删除 Pod 上残留的卷处理错误。
	podsWithError := dswp.desiredStateOfWorld.GetPodsWithErrors()
	for _, podName := range podsWithError {
		if _, podExists := dswp.podManager.GetPodByUID(types.UID(podName)); !podExists {
			dswp.desiredStateOfWorld.PopPodErrors(podName)
		}
	}
}

// processPodVolumes 处理给定 Pod 的卷，并加入期望状态。
func (dswp *desiredStateOfWorldPopulator) processPodVolumes(ctx context.Context, pod *v1.Pod) {
	// 防御性检查：Pod 可能在入队后被删除。
	if pod == nil {
		return
	}

	logger := klog.FromContext(ctx)
	uniquePodName := util.GetUniquePodName(pod)
	// 已完整处理且未被要求重新处理时直接跳过。
	if dswp.podPreviouslyProcessed(uniquePodName) {
		return
	}

	// 只有所有卷都成功加入，才把 Pod 标记为已处理。
	allVolumesAdded := true
	collectSELinuxOptions := utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod)
	mounts, devices, seLinuxContainerContexts := util.GetPodVolumeNames(pod, collectSELinuxOptions)

	// 逐个处理 Pod 中定义的卷。
	for _, podVolume := range pod.Spec.Volumes {
		if !mounts.Has(podVolume.Name) && !devices.Has(podVolume.Name) {
			// 未在 mount/device 中出现的卷不参与挂载。
			logger.V(4).Info("Skipping unused volume", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
			continue
		}

		pvc, volumeSpec, volumeGIDValue, err :=
			dswp.createVolumeSpec(ctx, podVolume, pod, mounts, devices)
		// 记录错误并继续处理其余卷，单卷失败不应阻断整个 Pod。
		if err != nil {
			logger.Error(err, "Error processing volume", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
			dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
			allVolumesAdded = false
			continue
		}

		// 把卷加入期望状态。
		uniqueVolumeName, err := dswp.desiredStateOfWorld.AddPodToVolume(
			logger, uniquePodName, pod, volumeSpec, podVolume.Name, volumeGIDValue, seLinuxContainerContexts[podVolume.Name])
		if err != nil {
			logger.Error(err, "Failed to add volume to desiredStateOfWorld", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "volumeSpecName", volumeSpec.Name())
			dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
			allVolumesAdded = false
			continue
		}

		logger.V(4).Info("Added volume to desired state", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "volumeSpecName", volumeSpec.Name())
		dswp.checkVolumeFSResize(logger, pod, podVolume, pvc, volumeSpec, uniqueVolumeName)
	}

	// 部分卷添加失败时，不能把 Pod 标记为已完整处理。
	if allVolumesAdded {
		dswp.markPodProcessed(uniquePodName)
		// 新 Pod 已完成同步，重新挂载需要刷新的卷（例如 Downward API）。
		dswp.actualStateOfWorld.MarkRemountRequired(logger, uniquePodName)
		// 本轮全部成功，清除该 Pod 之前记录的错误。
		dswp.desiredStateOfWorld.PopPodErrors(uniquePodName)
	} else if dswp.podHasBeenSeenOnce(uniquePodName) {
		// 对至少处理过一次的 Pod，即使本轮有卷失败，也标记为已处理，
		// 避免以极高频率反复重试；卷管理器会在 SyncPod 触发 ReprocessPod() 时
		// 重新处理该 Pod。
		dswp.markPodProcessed(uniquePodName)
	}

}

// checkVolumeFSResize 记录 Pod 所挂载卷的期望 PVC 大小，
// 用于与 pvc.Status.Capacity 中的实际大小比较，并在需要时触发节点侧扩容。
func (dswp *desiredStateOfWorldPopulator) checkVolumeFSResize(
	logger klog.Logger,
	pod *v1.Pod,
	podVolume v1.Volume,
	pvc *v1.PersistentVolumeClaim,
	volumeSpec *volume.Spec,
	uniqueVolumeName v1.UniqueVolumeName) {

	// 卷规格没有 PV、启用了 InlineVolumeSpecForCSIMigration，或 PVC 为空时
	// 无法扩容，直接跳过。
	if volumeSpec.PersistentVolume == nil || volumeSpec.InlineVolumeSpecForCSIMigration || pvc == nil {
		// 只有 PVC 支持扩容操作。
		return
	}

	// volumeSpec.ReadOnly 决定卷挂载时能否格式化，也决定离线扩容行为，
	// 因此这里必须使用同一标志；该值来自 Pod.spec.volumes.persistentVolumeClaim.readOnly。
	if volumeSpec.ReadOnly {
		// 只读卷不做扩容。
		logger.V(5).Info("Skip file system resize check for the volume, as the volume is mounted as readonly", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
		return
	}

	// 把 PV 期望容量与 PVC 实际状态容量都深拷贝，避免后续修改共享对象。
	pvCap := volumeSpec.PersistentVolume.Spec.Capacity.Storage().DeepCopy()
	pvcStatusCap := pvc.Status.Capacity.Storage().DeepCopy()
	dswp.desiredStateOfWorld.UpdatePersistentVolumeSize(uniqueVolumeName, pvCap)
	logger.V(5).Info("NodeExpandVolume updating size", "actualSize", pvcStatusCap.String(), "desiredSize", pvCap.String(), "volumeName", uniqueVolumeName)
	// kubelet 重启后实际状态可能被重建，这里确保 claimSize 被校正为准确值。
	dswp.actualStateOfWorld.InitializeClaimSize(logger, uniqueVolumeName, pvcStatusCap)
}

// podPreviouslyProcessed 返回该 Pod 的卷是否已被填充器处理/重新处理过；
// 否则说明还需要重新处理。
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()

	return dswp.pods.processedPods[podName]
}

// markPodProcessingFailed 把 Pod 标记为 false，表示处理失败、需要重试。
func (dswp *desiredStateOfWorldPopulator) markPodProcessingFailed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	dswp.pods.processedPods[podName] = false
	dswp.pods.Unlock()
}

// podHasBeenSeenOnce 返回填充器是否至少见过该 Pod 一次。
func (dswp *desiredStateOfWorldPopulator) podHasBeenSeenOnce(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	_, exist := dswp.pods.processedPods[podName]
	dswp.pods.RUnlock()
	return exist
}

// markPodProcessed 记录该 Pod 的卷已被填充器完整处理。
func (dswp *desiredStateOfWorldPopulator) markPodProcessed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	dswp.pods.processedPods[podName] = true
}

// deleteProcessedPod 从 processedPods 中删除该 Pod 的记录。
func (dswp *desiredStateOfWorldPopulator) deleteProcessedPod(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	delete(dswp.pods.processedPods, podName)
}

// createVolumeSpec 为指定卷创建并返回可变的 volume.Spec 对象；
// 需要时会解引用 PVC 获取 PV。当前无法取得卷时返回错误。
func (dswp *desiredStateOfWorldPopulator) createVolumeSpec(
	ctx context.Context, podVolume v1.Volume, pod *v1.Pod, mounts, devices sets.Set[string]) (*v1.PersistentVolumeClaim, *volume.Spec, string, error) {
	logger := klog.FromContext(ctx)
	// PVC 显式引用与通用临时卷都按 PVC 处理。
	pvcSource := podVolume.VolumeSource.PersistentVolumeClaim
	isEphemeral := pvcSource == nil && podVolume.VolumeSource.Ephemeral != nil
	if isEphemeral {
		// 通用临时内联卷与 PVC 引用走同一条路径，
		// 额外约束（下方检查）是 PVC 必须由该 Pod 拥有。
		pvcSource = &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: ephemeral.VolumeClaimName(pod, &podVolume),
		}
	}
	// PVC 路径需要先从 API server 取回 PVC 与绑定 PV。
	if pvcSource != nil {
		logger.V(5).Info("Found PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName))
		// 如果是 PVC，取回 claim 后面的真实 PV。
		pvc, err := dswp.getPVCExtractPV(
			ctx, pod.Namespace, pvcSource.ClaimName)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %s/%s: %v",
				pod.Namespace,
				pvcSource.ClaimName,
				err)
		}
		// 临时卷必须确认 PVC 确实属于该 Pod，防止误用其他 Pod 的卷。
		if isEphemeral {
			if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
				return nil, nil, "", err
			}
		}
		pvName, pvcUID := pvc.Spec.VolumeName, pvc.UID
		logger.V(5).Info("Found bound PV for PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName), "PVCUID", pvcUID, "PVName", pvName)
		// 获取实际的 PV 对象。
		volumeSpec, volumeGIDValue, err :=
			dswp.getPVSpec(ctx, pvName, pvcSource.ReadOnly, pvcUID)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %s/%s: %v",
				pod.Namespace,
				pvcSource.ClaimName,
				err)
		}
		logger.V(5).Info("Extracted volumeSpec from bound PV and PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName), "PVCUID", pvcUID, "PVName", pvName, "volumeSpecName", volumeSpec.Name())
		migratable, err := dswp.csiMigratedPluginManager.IsMigratable(volumeSpec)
		if err != nil {
			return nil, nil, "", err
		}
		if migratable {
			volumeSpec, err = csimigration.TranslateInTreeSpecToCSI(logger, volumeSpec, pod.Namespace, dswp.intreeToCSITranslator)
			if err != nil {
				return nil, nil, "", err
			}
		}

		volumeMode, err := util.GetVolumeMode(volumeSpec)
		if err != nil {
			return nil, nil, "", err
		}
		// 容器使用 volumeMounts 时，PVC 的 volumeMode 必须是 Filesystem。
		if mounts.Has(podVolume.Name) && volumeMode != v1.PersistentVolumeFilesystem {
			return nil, nil, "", fmt.Errorf(
				"volume %s has volumeMode %s, but is specified in volumeMounts",
				podVolume.Name,
				volumeMode)
		}
		// 容器使用 volumeDevices 时，PVC 的 volumeMode 必须是 Block。
		if devices.Has(podVolume.Name) && volumeMode != v1.PersistentVolumeBlock {
			return nil, nil, "", fmt.Errorf(
				"volume %s has volumeMode %s, but is specified in volumeDevices",
				podVolume.Name,
				volumeMode)
		}
		return pvc, volumeSpec, volumeGIDValue, nil
	}

	// 不返回原始卷对象，避免源对象随后被修改而影响缓存。
	clonedPodVolume := podVolume.DeepCopy()

	spec := volume.NewSpecFromVolume(clonedPodVolume)
	migratable, err := dswp.csiMigratedPluginManager.IsMigratable(spec)
	if err != nil {
		return nil, nil, "", err
	}
	if migratable {
		spec, err = csimigration.TranslateInTreeSpecToCSI(logger, spec, pod.Namespace, dswp.intreeToCSITranslator)
		if err != nil {
			return nil, nil, "", err
		}
	}
	return nil, spec, "", nil
}

// getPVCExtractPV 从 API server 获取指定 namespace/name 的 PVC，
// 检查其是否正在删除，并提取其指向的 PV 名称。
// PVC 的 phase 不是 Bound 时返回错误。
func (dswp *desiredStateOfWorldPopulator) getPVCExtractPV(
	ctx context.Context, namespace string, claimName string) (*v1.PersistentVolumeClaim, error) {
	pvc, err :=
		dswp.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil || pvc == nil {
		return nil, fmt.Errorf("failed to fetch PVC from API server: %v", err)
	}

	// 使用正在删除且未受 kubernetes.io/pvc-protection 保护的 PVC 的 Pod 不应启动。
	//
	// 1) 旧版本 kubelet 没有该检查时，最坏结果是这类 Pod 被调度；
	//    1.8 及更早版本就是这种行为，用户对此不至于意外。
	//    只有在调度器调度 Pod 的同时用户删除其 PVC 的罕见场景才会发生。
	//
	// 2) 在这里检查 kubernetes.io/pvc-protection，可以避免 kubelet 重启
	//    重建期望状态缓存时影响已在运行的 Pod。
	//    该检查是安全的：在 Pod 删除前，PVC 会一直停留在 Terminating 状态。
	if pvc.ObjectMeta.DeletionTimestamp != nil && !slices.Contains(pvc.Finalizers, util.PVCProtectionFinalizer) {
		return nil, errors.New("PVC is being deleted")
	}

	if pvc.Status.Phase != v1.ClaimBound {
		return nil, errors.New("PVC is not bound")
	}
	if pvc.Spec.VolumeName == "" {
		return nil, errors.New("PVC has empty pvc.Spec.VolumeName")
	}

	return pvc, nil
}

// getPVSpec 从 API server 获取指定名称的 PV，并返回表示它的 volume.Spec。
// 获取 PV 失败时返回错误。
func (dswp *desiredStateOfWorldPopulator) getPVSpec(
	ctx context.Context,
	name string,
	pvcReadOnly bool,
	expectedClaimUID types.UID) (*volume.Spec, string, error) {
	pv, err := dswp.kubeClient.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
	if err != nil || pv == nil {
		return nil, "", fmt.Errorf(
			"failed to fetch PV %s from API server: %v", name, err)
	}

	// 没有 ClaimRef 说明 PV 尚未绑定到任何 PVC。
	if pv.Spec.ClaimRef == nil {
		return nil, "", fmt.Errorf(
			"found PV object %s but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim",
			name)
	}

	// 校验绑定关系，防止使用已重新绑定到其他 PVC 的 PV。
	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, "", fmt.Errorf(
			"found PV object %s but its pv.Spec.ClaimRef.UID %s does not point to claim.UID %s",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}

	volumeGIDValue := getPVVolumeGidAnnotationValue(pv)
	return volume.NewSpecFromPersistentVolume(pv, pvcReadOnly), volumeGIDValue, nil
}

// getPVVolumeGidAnnotationValue 读取 PV 上记录的卷 GID 注解值。
func getPVVolumeGidAnnotationValue(pv *v1.PersistentVolume) string {
	if volumeGid, ok := pv.Annotations[util.VolumeGidAnnotationKey]; ok {
		return volumeGid
	}

	return ""
}
