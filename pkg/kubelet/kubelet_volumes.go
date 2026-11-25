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

package kubelet

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/util/removeall"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// ListVolumesForPod 返回指定 Pod 已挂载卷的映射，
// 键是 OuterVolumeSpecName（即 pod.Spec.Volumes[x].Name）。
func (kl *Kubelet) ListVolumesForPod(podUID types.UID) (map[string]volume.Volume, bool) {
	volumesToReturn := make(map[string]volume.Volume)
	podVolumes := kl.volumeManager.GetMountedVolumesForPod(
		volumetypes.UniquePodName(podUID))
	for outerVolumeSpecName, volume := range podVolumes {
		// TODO: 从 reconciler 的同步状态恢复卷对象时，volume.Mounter 可能为 nil。
		// PR 33616 将修复恢复卷状态时创建 Mounter 对象的问题。
		if volume.Mounter == nil {
			continue
		}
		volumesToReturn[outerVolumeSpecName] = volume.Mounter
	}

	return volumesToReturn, len(volumesToReturn) > 0
}

// ListBlockVolumesForPod 返回指定 Pod 已挂载块设备卷的映射，
// 键是 OuterVolumeSpecName（即 pod.Spec.Volumes[x].Name）。
func (kl *Kubelet) ListBlockVolumesForPod(podUID types.UID) (map[string]volume.BlockVolume, bool) {
	volumesToReturn := make(map[string]volume.BlockVolume)
	podVolumes := kl.volumeManager.GetMountedVolumesForPod(
		volumetypes.UniquePodName(podUID))
	for outerVolumeSpecName, volume := range podVolumes {
		// TODO: 从 reconciler 的同步状态恢复卷对象时，volume.Mounter 可能为 nil。
		// PR 33616 将修复恢复卷状态时创建 Mounter 对象的问题。
		// 块设备卷缺少 BlockVolumeMapper 时同样跳过。
		if volume.BlockVolumeMapper == nil {
			continue
		}
		volumesToReturn[outerVolumeSpecName] = volume.BlockVolumeMapper
	}

	return volumesToReturn, len(volumesToReturn) > 0
}

// podVolumesExist 向卷管理器查询：只要指定 Pod 的任一卷仍处于挂载状态
// 或状态不确定，就返回 true。
func (kl *Kubelet) podVolumesExist(logger klog.Logger, podUID types.UID) bool {
	// 卷管理器认为可能存在挂载时直接返回 true，采取保守策略。
	if kl.volumeManager.HasPossiblyMountedVolumesForPod(volumetypes.UniquePodName(podUID)) {
		return true
	}
	// TODO: 这里检查 Pod 卷路径及其挂载状态。若检查出错，podVolumesExist 会返回 true，
	// 表示卷可能仍存在，需要进一步确认。flexvolume 等插件可能没有挂载点，参见 issue #61229。
	volumePaths, err := kl.getMountedVolumePathListFromDisk(logger, podUID)
	if err != nil {
		// 检查失败时宁可保留，避免误删仍在使用的卷。
		logger.Error(err, "Pod found, but error occurred during checking mounted volumes from disk", "podUID", podUID)
		return true
	}
	// 磁盘上仍有挂载路径，说明卷尚未清理干净。
	if len(volumePaths) > 0 {
		logger.V(4).Info("Pod found, but volumes are still mounted on disk", "podUID", podUID, "paths", volumePaths)
		return true
	}

	return false
}

// newVolumeMounterFromPlugins 根据卷规格、Pod 与卷选项查找插件并创建 Mounter，
// 返回可用的 Mounter 或错误。
func (kl *Kubelet) newVolumeMounterFromPlugins(logger klog.Logger, spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	plugin, err := kl.volumePluginMgr.FindPluginBySpec(spec)
	if err != nil {
		return nil, fmt.Errorf("can't use volume plugins for %s: %v", spec.Name(), err)
	}
	physicalMounter, err := plugin.NewMounter(spec, pod)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate mounter for volume: %s using plugin: %s with a root cause: %v", spec.Name(), plugin.GetPluginName(), err)
	}
	logger.V(10).Info("Using volume plugin for mount", "volumePluginName", plugin.GetPluginName(), "volumeName", spec.Name())
	return physicalMounter, nil
}

// removeOrphanedPodVolumeDirs 尝试删除 Pod 的卷目录及其子目录。正常情况下调用时
// 不应残留文件，因此这里实际执行的是递归 rmdir 而非 RemoveAll，确保只删除空目录
// 和曾被用作挂载点的文件，而不删除挂载点里的内容。
func (kl *Kubelet) removeOrphanedPodVolumeDirs(logger klog.Logger, uid types.UID) []error {
	orphanVolumeErrors := []error{}

	// 若仍有卷目录，逐个尝试 rmdir（仅删除空目录）。
	volumePaths, err := kl.getPodVolumePathListFromDisk(logger, uid)
	if err != nil {
		// 读取目录失败时记录错误并停止后续清理。
		orphanVolumeErrors = append(orphanVolumeErrors, fmt.Errorf("orphaned pod %q found, but error occurred during reading volume dir from disk: %v", uid, err))
		return orphanVolumeErrors
	}
	if len(volumePaths) > 0 {
		for _, volumePath := range volumePaths {
			if err := syscall.Rmdir(volumePath); err != nil {
				orphanVolumeErrors = append(orphanVolumeErrors, fmt.Errorf("orphaned pod %q found, but failed to rmdir() volume at path %v: %v", uid, volumePath, err))
			} else {
				logger.Info("Cleaned up orphaned volume from pod", "podUID", uid, "path", volumePath)
			}
		}
	}

	// 清理残留的 volume-subpath 挂载点。
	subpathVolumePaths, err := kl.getPodVolumeSubpathListFromDisk(logger, uid)
	if err != nil {
		orphanVolumeErrors = append(orphanVolumeErrors, fmt.Errorf("orphaned pod %q found, but error occurred during reading of volume-subpaths dir from disk: %v", uid, err))
		return orphanVolumeErrors
	}
	if len(subpathVolumePaths) > 0 {
		for _, subpathVolumePath := range subpathVolumePaths {
			// 这里同时删除文件和空目录，因为 subpath 可能是文件或目录的 bind mount。
			if err := os.Remove(subpathVolumePath); err != nil {
				orphanVolumeErrors = append(orphanVolumeErrors, fmt.Errorf("orphaned pod %q found, but failed to remove subpath at path %v: %w", uid, subpathVolumePath, err))
			} else {
				logger.Info("Cleaned up orphaned volume subpath from pod", "podUID", uid, "path", subpathVolumePath)
			}
		}
	}

	// 删除剩余子目录以及 volumes 目录本身；遇到普通文件则失败。
	podVolDir := kl.getPodVolumesDir(uid)
	if err := removeall.RemoveDirsOneFilesystem(kl.mounter, podVolDir); err != nil {
		orphanVolumeErrors = append(orphanVolumeErrors, fmt.Errorf("orphaned pod %q found, but error occurred when trying to remove the volumes dir: %v", uid, err))
	} else {
		logger.Info("Cleaned up orphaned pod volumes dir", "podUID", uid, "path", podVolDir)
	}

	return orphanVolumeErrors
}

// cleanupOrphanedPodDirs 删除不应再运行且没有容器的 Pod 的卷。
// 该函数运行在主循环中，因此这里只汇总日志输出。
func (kl *Kubelet) cleanupOrphanedPodDirs(logger klog.Logger, pods []*v1.Pod, runningPods []*kubecontainer.Pod) error {
	// 汇总所有已知 Pod（包括正在运行的容器），用于识别磁盘上的孤儿目录。
	allPods := sets.New[string]()
	for _, pod := range pods {
		allPods.Insert(string(pod.UID))
	}
	for _, pod := range runningPods {
		allPods.Insert(string(pod.ID))
	}

	// 从磁盘枚举 kubelet 记录过的 Pod UID。
	found, err := kl.listPodsFromDisk()
	if err != nil {
		return err
	}

	orphanRemovalErrors := []error{}
	orphanVolumeErrors := []error{}
	var totalPods, errorPods int

	for _, uid := range found {
		// 跳过仍然存在的 Pod。
		if allPods.Has(string(uid)) {
			continue
		}

		totalPods++

		// 若卷尚未卸载/解挂，不要删除目录，否则可能导致数据损坏。
		// TODO: getMountedVolumePathListFromDisk() 的调用可能与
		// kl.getPodVolumePathListFromDisk() 重复，能否清理掉？
		if podVolumesExist := kl.podVolumesExist(logger, uid); podVolumesExist {
			errorPods++
			logger.V(3).Info("Orphaned pod found, but volumes are not cleaned up", "podUID", uid)
			continue
		}

		// 尝试删除 Pod 的卷目录及其子目录。
		podVolumeErrors := kl.removeOrphanedPodVolumeDirs(logger, uid)
		if len(podVolumeErrors) > 0 {
			errorPods++
			orphanVolumeErrors = append(orphanVolumeErrors, podVolumeErrors...)
			// 并非所有卷都已删除，暂时不要清理 Pod 目录：可能仍有挂载点或文件残留，
			// 会导致后面的 Pod 目录删除失败。所有删除操作的错误都已记录，
			// 这里不再重复追加。
			continue
		}

		// 对 Pod 目录下剩余的子目录调用 RemoveAllOneFilesystem。
		podDir := kl.getPodDir(uid)
		podSubdirs, err := os.ReadDir(podDir)
		if err != nil {
			errorPods++
			logger.Error(err, "Could not read directory", "path", podDir)
			orphanRemovalErrors = append(orphanRemovalErrors, fmt.Errorf("orphaned pod %q found, but error occurred during reading the pod dir from disk: %v", uid, err))
			continue
		}

		var cleanupFailed bool
		for _, podSubdir := range podSubdirs {
			podSubdirName := podSubdir.Name()
			podSubdirPath := filepath.Join(podDir, podSubdirName)
			// 绝不要对 volumes 目录调用 RemoveAllOneFilesystem，某些情况下会导致数据丢失；
			// 该目录本应由 removeOrphanedPodVolumeDirs 删除。
			if podSubdirName == "volumes" {
				cleanupFailed = true
				err := fmt.Errorf("volumes subdir was found after it was removed")
				logger.Error(err, "Orphaned pod found, but failed to remove volumes subdir", "podUID", uid, "path", podSubdirPath)
				continue
			}
			if err := removeall.RemoveAllOneFilesystem(kl.mounter, podSubdirPath); err != nil {
				cleanupFailed = true
				logger.Error(err, "Failed to remove orphaned pod subdir", "podUID", uid, "path", podSubdirPath)
				orphanRemovalErrors = append(orphanRemovalErrors, fmt.Errorf("orphaned pod %q found, but error occurred when trying to remove subdir %q: %v", uid, podSubdirPath, err))
			}
		}

		// 删除 Pod 目录；如果上面都成功，此时目录应为空。
		logger.V(3).Info("Orphaned pod found, removing", "podUID", uid)
		if err := syscall.Rmdir(podDir); err != nil {
			cleanupFailed = true
			logger.Error(err, "Failed to remove orphaned pod dir", "podUID", uid)
			orphanRemovalErrors = append(orphanRemovalErrors, fmt.Errorf("orphaned pod %q found, but error occurred when trying to remove the pod directory: %v", uid, err))
		}
		// 任一子目录清理失败都标记为清理失败，最后据此统计错误 Pod 数。
		if cleanupFailed {
			errorPods++
		}
	}

	// logSpew 汇总同类错误：只打印一条错误，详细错误在更高日志级别输出。
	logSpew := func(errs []error) {
		if len(errs) > 0 {
			logger.Error(errs[0], "There were many similar errors. Turn up verbosity to see them.", "numErrs", len(errs))
			for _, err := range errs {
				logger.V(5).Info("Orphan pod", "err", err)
			}
		}
	}
	logSpew(orphanVolumeErrors)
	logSpew(orphanRemovalErrors)
	// 记录本次清理的孤儿 Pod 总数与失败数。
	metrics.OrphanPodCleanedVolumes.Set(float64(totalPods))
	metrics.OrphanPodCleanedVolumesErrors.Set(float64(errorPods))
	return utilerrors.NewAggregate(orphanRemovalErrors)
}
