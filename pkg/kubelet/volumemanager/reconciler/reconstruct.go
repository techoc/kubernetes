/*
Copyright 2022 The Kubernetes Authors.

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

package reconciler

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
)

// readyToUnmount 在 reconciler 可以开始卸载卷时返回 true。
func (rc *reconciler) readyToUnmount() bool {
	// kubelet 启动时，磁盘上的所有卷都会以“不确定”状态加入 ASW。
	// 只有 DSW 完全填充后才允许卸载，避免卸载尚未进入 DSW 的卷。
	if !rc.populatorHasAddedPods() {
		return false
	}

	// 只有 ASW 中的设备路径已从 node.status 修正后才允许卸载，
	// 避免使用错误的 devicePath 调用卸载。
	if len(rc.volumesNeedUpdateFromNodeStatus) != 0 {
		return false
	}
	return true
}

// reconstructVolumes 通过扫描磁盘上所有 Pod 的卷目录来重建实际状态。
// 对于不支持重建或重建失败的卷，会放入 volumesFailedReconstruction，
// 待 DesiredStateOfWorld 填充后再清理。
func (rc *reconciler) reconstructVolumes(logger klog.Logger) {
	// 通过读取 Pod 目录获取卷信息。
	podVolumes, err := getVolumesFromPodDir(logger, rc.kubeletPodsDir)
	if err != nil {
		logger.Error(err, "Cannot get volumes from disk, skip sync states for volume reconstruction")
		return
	}
	reconstructedVolumes := make(map[v1.UniqueVolumeName]*globalVolumeInfo)
	reconstructedVolumeNames := []v1.UniqueVolumeName{}
	for _, volume := range podVolumes {
		if rc.actualStateOfWorld.VolumeExistsWithSpecName(volume.podName, volume.volumeSpecName) {
			logger.V(4).Info("Volume exists in actual state, skip cleaning up mounts", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName)
			// 已存在于实际状态中，无需重建。
			continue
		}
		reconstructedVolume, err := rc.reconstructVolume(volume)
		if err != nil {
			logger.Info("Could not construct volume information", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName, "err", err)
			// 无法重建该卷；待 DSW 完全填充后检查，若已成为孤儿则强制卸载。
			rc.volumesFailedReconstruction = append(rc.volumesFailedReconstruction, volume)
			continue
		}
		logger.V(4).Info("Adding reconstructed volume to actual state and node status", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName)
		gvl := &globalVolumeInfo{
			volumeName:        reconstructedVolume.volumeName,
			volumeSpec:        reconstructedVolume.volumeSpec,
			devicePath:        reconstructedVolume.devicePath,
			deviceMounter:     reconstructedVolume.deviceMounter,
			blockVolumeMapper: reconstructedVolume.blockVolumeMapper,
			mounter:           reconstructedVolume.mounter,
		}
		// 同一个卷可能被多个 Pod 使用，复用已有的 globalVolumeInfo。
		if cachedInfo, ok := reconstructedVolumes[reconstructedVolume.volumeName]; ok {
			gvl = cachedInfo
		}
		gvl.addPodVolume(reconstructedVolume)

		reconstructedVolumeNames = append(reconstructedVolumeNames, reconstructedVolume.volumeName)
		reconstructedVolumes[reconstructedVolume.volumeName] = gvl
	}

	if len(reconstructedVolumes) > 0 {
		// 把重建出的卷加入实际状态。
		rc.updateStates(logger, reconstructedVolumes)

		// 记录待从 node.status.volumesAttached 更新的设备路径。
		rc.volumesNeedUpdateFromNodeStatus = reconstructedVolumeNames
	}
	logger.V(2).Info("Volume reconstruction finished")
}

// updateStates 把重建出的卷以“不确定”状态写入实际状态缓存。
func (rc *reconciler) updateStates(logger klog.Logger, reconstructedVolumes map[v1.UniqueVolumeName]*globalVolumeInfo) {
	for _, gvl := range reconstructedVolumes {
		err := rc.actualStateOfWorld.AddAttachUncertainReconstructedVolume(
			//TODO: the devicePath might not be correct for some volume plugins: see issue #54108
			logger, gvl.volumeName, gvl.volumeSpec, rc.nodeName, gvl.devicePath)
		if err != nil {
			logger.Error(err, "Could not add volume information to actual state of world", "volumeName", gvl.volumeName)
			continue
		}
		var seLinuxMountContext string
		// 同一卷的所有 Pod 都标记为挂载不确定。
		for _, volume := range gvl.podVolumes {
			markVolumeOpts := operationexecutor.MarkVolumeOpts{
				PodName:             volume.podName,
				PodUID:              types.UID(volume.podName),
				VolumeName:          volume.volumeName,
				Mounter:             volume.mounter,
				BlockVolumeMapper:   volume.blockVolumeMapper,
				VolumeGIDVolume:     volume.volumeGIDValue,
				VolumeSpec:          volume.volumeSpec,
				VolumeMountState:    operationexecutor.VolumeMountUncertain,
				SELinuxMountContext: volume.seLinuxMountContext,
			}

			_, err = rc.actualStateOfWorld.CheckAndMarkVolumeAsUncertainViaReconstruction(markVolumeOpts)
			if err != nil {
				logger.Error(err, "Could not add pod to volume information to actual state of world", "pod", klog.KObj(volume.pod))
				continue
			}
			seLinuxMountContext = volume.seLinuxMountContext
			logger.V(2).Info("Volume is marked as uncertain and added into the actual state", "pod", klog.KObj(volume.pod), "podName", volume.podName, "volumeName", volume.volumeName, "seLinuxMountContext", volume.seLinuxMountContext)
		}
		// 若卷有需要挂载的设备，则把设备状态也标记为不确定。
		if gvl.deviceMounter != nil || gvl.blockVolumeMapper != nil {
			deviceMountPath, err := getDeviceMountPath(gvl)
			if err != nil {
				logger.Error(err, "Could not find device mount path for volume", "volumeName", gvl.volumeName)
				continue
			}
			err = rc.actualStateOfWorld.MarkDeviceAsUncertain(gvl.volumeName, gvl.devicePath, deviceMountPath, seLinuxMountContext)
			if err != nil {
				logger.Error(err, "Could not mark device is uncertain to actual state of world", "volumeName", gvl.volumeName, "deviceMountPath", deviceMountPath)
				continue
			}
			logger.V(2).Info("Volume is marked device as uncertain and added into the actual state", "volumeName", gvl.volumeName, "deviceMountPath", deviceMountPath)
		}
	}
}

// cleanOrphanVolumes 尝试清理所有重建失败的卷。
func (rc *reconciler) cleanOrphanVolumes(logger klog.Logger) {
	if len(rc.volumesFailedReconstruction) == 0 {
		return
	}

	for _, volume := range rc.volumesFailedReconstruction {
		if rc.desiredStateOfWorld.VolumeExistsWithSpecName(volume.podName, volume.volumeSpecName) {
			// 仍有 Pod 需要该卷，不要清理；等待 reconcile() 调用 SetUp 并在 ASW 中重建它。
			logger.V(4).Info("Volume exists in desired state, skip cleaning up mounts", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName)
			continue
		}
		logger.Info("Cleaning up mounts for volume that could not be reconstructed", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName)
		rc.cleanupMounts(logger, volume)
	}

	logger.V(2).Info("Orphan volume cleanup finished")
	// 清理是一次性操作，完成后清空缓存。
	rc.volumesFailedReconstruction = make([]podVolume, 0)
}

// updateReconstructedFromNodeStatus 尝试从 node.Status.VolumesAttached 填充重建卷的
// 设备路径。这只能在连接 apiserver 之后进行，因此不能放在 reconstructVolumes() 里。
func (rc *reconciler) updateReconstructedFromNodeStatus(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating reconstructed devicePaths")

	if rc.kubeClient == nil {
		// kubelet 处于 standalone 模式时跳过从 Node 对象重建设备路径；
		// 这种 kubelet 不会挂载可附加卷或 Secret / ConfigMap。
		logger.V(2).Info("Skipped reconstruction of DevicePaths from node.status in standalone mode")
		rc.volumesNeedUpdateFromNodeStatus = nil
		return
	}

	node, fetchErr := rc.kubeClient.CoreV1().Nodes().Get(ctx, string(rc.nodeName), metav1.GetOptions{})
	if fetchErr != nil {
		// 在 kubelet 首次成功读取自身状态之前，这里可能每秒重复几次。
		logger.V(4).Info("Failed to get Node status to reconstruct device paths", "err", fetchErr)
		return
	}

	for _, volumeID := range rc.volumesNeedUpdateFromNodeStatus {
		attachable := false
		for _, attachedVolume := range node.Status.VolumesAttached {
			// 在 node.status 中找到对应卷时更新设备路径，并标记为可附加。
			if volumeID != attachedVolume.Name {
				continue
			}
			rc.actualStateOfWorld.UpdateReconstructedDevicePath(volumeID, attachedVolume.DevicePath)
			attachable = true
			logger.V(4).Info("Updated devicePath from node status for volume", "volumeName", attachedVolume.Name, "path", attachedVolume.DevicePath)
		}
		rc.actualStateOfWorld.UpdateReconstructedVolumeAttachability(volumeID, attachable)
	}

	logger.V(2).Info("DevicePaths of reconstructed volumes updated")
	rc.volumesNeedUpdateFromNodeStatus = nil
}
