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

// Package populator 提供相关接口的实现，负责监控并维持期望状态
// 缓存与 informer 中“真实状态”的一致性。
package populator

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/util"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volutil "k8s.io/kubernetes/pkg/volume/util"
)

// DesiredStateOfWorldPopulator 定期检查期望状态中的 Pod
// 是否仍然存在，不存在则将其移除；同时遍历活跃 Pod 列表，确保带有卷的 Pod
// 都存在于该缓存中，从而使实际卷挂载状态与 informer 中的真实状态保持一致。
type DesiredStateOfWorldPopulator interface {
	// Run 启动填充循环，直到 ctx 被取消。
	Run(ctx context.Context)
}

// NewDesiredStateOfWorldPopulator 创建 DesiredStateOfWorldPopulator 实例。
// loopSleepDuration 是两轮填充循环之间的休眠时长。
// podLister 提供本节点上 Pod 的真实来源（通过 informer 缓存）。
// desiredStateOfWorld 是待填充的目标缓存。
// listPodsRetryDuration 限定重新 List 全部 Pod 的最小间隔，避免频繁全量列举。
func NewDesiredStateOfWorldPopulator(
	loopSleepDuration time.Duration,
	listPodsRetryDuration time.Duration,
	podLister corelisters.PodLister,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	volumePluginMgr *volume.VolumePluginMgr,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pvLister corelisters.PersistentVolumeLister,
	csiMigratedPluginManager csimigration.PluginManager,
	intreeToCSITranslator csimigration.InTreeToCSITranslator) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		loopSleepDuration:        loopSleepDuration,
		listPodsRetryDuration:    listPodsRetryDuration,
		podLister:                podLister,
		desiredStateOfWorld:      desiredStateOfWorld,
		volumePluginMgr:          volumePluginMgr,
		pvcLister:                pvcLister,
		pvLister:                 pvLister,
		csiMigratedPluginManager: csiMigratedPluginManager,
		intreeToCSITranslator:    intreeToCSITranslator,
	}
}

// desiredStateOfWorldPopulator 是 DesiredStateOfWorldPopulator 的具体实现，
// 通过 informer lister 周期性地对账缓存中的 Pod 挂载意图。
type desiredStateOfWorldPopulator struct {
	loopSleepDuration        time.Duration
	podLister                corelisters.PodLister
	desiredStateOfWorld      cache.DesiredStateOfWorld
	volumePluginMgr          *volume.VolumePluginMgr
	pvcLister                corelisters.PersistentVolumeClaimLister
	pvLister                 corelisters.PersistentVolumeLister
	listPodsRetryDuration    time.Duration
	timeOfLastListPods       time.Time
	csiMigratedPluginManager csimigration.PluginManager
	intreeToCSITranslator    csimigration.InTreeToCSITranslator
}

// Run 周期性执行填充循环，直到 ctx 被取消。
func (dswp *desiredStateOfWorldPopulator) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, dswp.populatorLoopFunc(ctx), dswp.loopSleepDuration)
}

// populatorLoopFunc 返回每轮循环执行的函数：先清理缓存中已删除或失配的 Pod，
// 再按 listPodsRetryDuration 节流，只有达到重试时间才全量列举并补齐活跃 Pod，
// 避免每轮都做开销较大的 List 操作。
func (dswp *desiredStateOfWorldPopulator) populatorLoopFunc(ctx context.Context) func(ctx context.Context) {
	return func(ctx context.Context) {
		logger := klog.FromContext(ctx)
		dswp.findAndRemoveDeletedPods(logger)

		// findAndAddActivePods 的调用独立于主填充循环做节流：
		// 全量 List Pod 代价较高，未到重试时间就跳过，防止频繁列举拖慢控制器。
		if time.Since(dswp.timeOfLastListPods) < dswp.listPodsRetryDuration {
			logger.V(5).Info(
				"Skipping findAndAddActivePods(). Not permitted until the retry time is reached",
				"retryTime", dswp.timeOfLastListPods.Add(dswp.listPodsRetryDuration),
				"retryDuration", dswp.listPodsRetryDuration)

			return
		}
		dswp.findAndAddActivePods(logger)
	}
}

// findAndRemoveDeletedPods 遍历期望状态中的所有 Pod，
// 若某 Pod 在 informer 中已不存在，或其 UID 与缓存中的记录不匹配（同名 Pod 被重建），
// 则将其从缓存移除；此外，当卷从可挂载变为不可挂载时，也要清理相关挂载记录。
func (dswp *desiredStateOfWorldPopulator) findAndRemoveDeletedPods(logger klog.Logger) {
	for dswPodUID, dswPodToAdd := range dswp.desiredStateOfWorld.GetPodToAdd() {
		dswPodKey, err := kcache.MetaNamespaceKeyFunc(dswPodToAdd.Pod)
		if err != nil {
			logger.Error(err, "MetaNamespaceKeyFunc failed for pod", "podName", dswPodKey, "podUID", dswPodUID)
			continue
		}

		// 用 namespace/name 键从 pod informer 中取出最新的 Pod 对象，
		// 以确认缓存中的记录是否仍然有效。
		namespace, name, err := kcache.SplitMetaNamespaceKey(dswPodKey)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error splitting dswPodKey %q: %v", dswPodKey, err))
			continue
		}
		informerPod, err := dswp.podLister.Pods(namespace).Get(name)
		switch {
		case errors.IsNotFound(err):
			// Pod 在 informer 中已不存在，落到下面的逻辑将其从缓存中删除。
		case err != nil:
			logger.Error(err, "podLister Get failed for pod", "podName", dswPodKey, "podUID", dswPodUID)
			continue
		default:
			volumeActionFlag := util.DetermineVolumeAction(
				informerPod,
				true /* 默认卷操作 */)

			if volumeActionFlag {
				informerPodUID := volutil.GetUniquePodName(informerPod)
				// 即便同名 Pod 仍存在，也要比对 UID：UID 不同说明是重建的 Pod，
				// 旧的挂载意图必须删除，否则会基于过期的 Pod 做挂载决策。
				if informerPodUID == dswPodUID {
					logger.V(10).Info("Verified podfrom dsw exists in pod informer", "podName", dswPodKey, "podUID", dswPodUID)
					continue
				}
			}
		}

		// 走到这里说明 Pod 已不存在，或与 informer 中的 UID 不匹配，
		// 因此必须从期望状态中清除。
		logger.V(1).Info("Removing pod from dsw because it does not exist in pod informer", "podName", dswPodKey, "podUID", dswPodUID)
		dswp.desiredStateOfWorld.DeletePod(dswPodUID, dswPodToAdd.VolumeName, dswPodToAdd.NodeName)
	}

	// 检查已有卷的可挂载属性是否发生变化，例如 CSIDriver 更新导致卷不可挂载。
	for _, volumeToAttach := range dswp.desiredStateOfWorld.GetVolumesToAttach() {
		// 对 CSI 卷，IsAttachableVolume 会读取 CSIDriver 对象；但结果来自本地
		// lister 缓存而非 API Server，因此这里不算昂贵调用。
		volumeAttachable := volutil.IsAttachableVolume(volumeToAttach.VolumeSpec, dswp.volumePluginMgr)
		if !volumeAttachable {
			logger.Info("Volume changes from attachable to non-attachable", "volumeName", volumeToAttach.VolumeName)
			for _, scheduledPod := range volumeToAttach.ScheduledPods {
				podUID := volutil.GetUniquePodName(scheduledPod)
				dswp.desiredStateOfWorld.DeletePod(podUID, volumeToAttach.VolumeName, volumeToAttach.NodeName)
				logger.V(4).Info("Removing podUID and volume on node from desired state of world"+
					" because of the change of volume attachability", "node", klog.KRef("", string(volumeToAttach.NodeName)), "podUID", podUID, "volumeName", volumeToAttach.VolumeName)
			}
		}
	}
}

// findAndAddActivePods 全量列举 informer 中的 Pod，跳过已终止的 Pod，
// 处理其余 Pod 的卷并补齐到期望状态缓存。
// 同时记录列举时间，供上层按 listPodsRetryDuration 做节流。
func (dswp *desiredStateOfWorldPopulator) findAndAddActivePods(logger klog.Logger) {
	pods, err := dswp.podLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "PodLister List failed")
		return
	}
	dswp.timeOfLastListPods = time.Now()

	for _, pod := range pods {
		if volutil.IsPodTerminated(pod, pod.Status) {
			// 已终止的 Pod 不再需要挂载卷，直接跳过，避免残留无效的挂载意图。
			continue
		}
		util.ProcessPodVolumes(logger, pod, true,
			dswp.desiredStateOfWorld, dswp.volumePluginMgr, dswp.pvcLister, dswp.pvLister, dswp.csiMigratedPluginManager, dswp.intreeToCSITranslator)

	}

}
