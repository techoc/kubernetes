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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/metrics"
	volumepkg "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	utilpath "k8s.io/utils/path"
	utilstrings "k8s.io/utils/strings"
)

// 这些接口断言用于在保持结构体私有的同时，
// 让结构化日志能正确序列化它们。
var _ logr.Marshaler = podVolume{}
var _ logr.Marshaler = reconstructedVolume{}
var _ logr.Marshaler = globalVolumeInfo{}

// podVolume 描述从 Pod 目录扫描出的一个卷挂载点信息。
type podVolume struct {
	podName        volumetypes.UniquePodName
	volumeSpecName string
	volumePath     string
	pluginName     string
	volumeMode     v1.PersistentVolumeMode
}

// MarshalLog 实现 logr.Marshaler，以结构化字段输出 podVolume。
func (p podVolume) MarshalLog() interface{} {
	return struct {
		PodName        string `json:"podName"`
		VolumeSpecName string `json:"volumeSpecName"`
		VolumePath     string `json:"volumePath"`
		PluginName     string `json:"pluginName"`
		VolumeMode     string `json:"volumeMode"`
	}{
		PodName:        string(p.podName),
		VolumeSpecName: p.volumeSpecName,
		VolumePath:     p.volumePath,
		PluginName:     p.pluginName,
		VolumeMode:     string(p.volumeMode),
	}
}

// reconstructedVolume 描述从磁盘状态重建出来的卷及其所属 Pod 信息。
type reconstructedVolume struct {
	volumeName          v1.UniqueVolumeName
	podName             volumetypes.UniquePodName
	volumeSpec          *volumepkg.Spec
	pod                 *v1.Pod
	volumeGIDValue      string
	devicePath          string
	mounter             volumepkg.Mounter
	deviceMounter       volumepkg.DeviceMounter
	blockVolumeMapper   volumepkg.BlockVolumeMapper
	seLinuxMountContext string
}

// MarshalLog 实现 logr.Marshaler，以结构化字段输出 reconstructedVolume。
func (rv reconstructedVolume) MarshalLog() interface{} {
	return struct {
		VolumeName          string `json:"volumeName"`
		PodName             string `json:"podName"`
		VolumeSpecName      string `json:"volumeSpecName"`
		PodUID              string `json:"podUID"`
		VolumeGIDValue      string `json:"volumeGIDValue"`
		DevicePath          string `json:"devicePath"`
		SeLinuxMountContext string `json:"seLinuxMountContext"`
	}{
		VolumeName:          string(rv.volumeName),
		PodName:             string(rv.podName),
		VolumeSpecName:      rv.volumeSpec.Name(),
		PodUID:              string(rv.pod.UID),
		VolumeGIDValue:      rv.volumeGIDValue,
		DevicePath:          rv.devicePath,
		SeLinuxMountContext: rv.seLinuxMountContext,
	}
}

// globalVolumeInfo 保存某个卷的重建信息，
// 以及使用该卷的每个 Pod 对应的卷信息。
type globalVolumeInfo struct {
	volumeName        v1.UniqueVolumeName
	volumeSpec        *volumepkg.Spec
	devicePath        string
	mounter           volumepkg.Mounter
	deviceMounter     volumepkg.DeviceMounter
	blockVolumeMapper volumepkg.BlockVolumeMapper
	podVolumes        map[volumetypes.UniquePodName]*reconstructedVolume
}

// MarshalLog 实现 logr.Marshaler，以结构化字段输出 globalVolumeInfo。
func (gvi globalVolumeInfo) MarshalLog() interface{} {
	podVolumes := make(map[volumetypes.UniquePodName]v1.UniqueVolumeName)
	for podName, volume := range gvi.podVolumes {
		podVolumes[podName] = volume.volumeName
	}

	return struct {
		VolumeName     string                                            `json:"volumeName"`
		VolumeSpecName string                                            `json:"volumeSpecName"`
		DevicePath     string                                            `json:"devicePath"`
		PodVolumes     map[volumetypes.UniquePodName]v1.UniqueVolumeName `json:"podVolumes"`
	}{
		VolumeName:     string(gvi.volumeName),
		VolumeSpecName: gvi.volumeSpec.Name(),
		DevicePath:     gvi.devicePath,
		PodVolumes:     podVolumes,
	}
}

// updateLastSyncTime 记录最近一次状态同步的时间。
func (rc *reconciler) updateLastSyncTime() {
	rc.timeOfLastSyncLock.Lock()
	defer rc.timeOfLastSyncLock.Unlock()
	rc.timeOfLastSync = time.Now()
}

// StatesHasBeenSynced 返回是否至少完成过一次状态同步。
func (rc *reconciler) StatesHasBeenSynced() bool {
	rc.timeOfLastSyncLock.Lock()
	defer rc.timeOfLastSyncLock.Unlock()
	return !rc.timeOfLastSync.IsZero()
}

// addPodVolume 把某个 Pod 的重建卷信息加入全局卷信息中。
func (gvi *globalVolumeInfo) addPodVolume(rcv *reconstructedVolume) {
	if gvi.podVolumes == nil {
		gvi.podVolumes = map[volumetypes.UniquePodName]*reconstructedVolume{}
	}
	gvi.podVolumes[rcv.podName] = rcv
}

// cleanupMounts 清理无法在期望状态中找到对应信息的卷挂载点。
func (rc *reconciler) cleanupMounts(logger klog.Logger, volume podVolume) {
	logger.V(2).Info("Reconciler sync states: could not find volume information in desired state, clean up the mount points", "podName", volume.podName, "volumeSpecName", volume.volumeSpecName)
	mountedVolume := operationexecutor.MountedVolume{
		PodName: volume.podName,
		// VolumeName 本应由 `GetUniqueVolumeNameFromSpec` 或
		// `GetUniqueVolumeNameFromSpecWithPod` 生成；但清理挂载点时 ASW 中没有卷信息，
		// 因此这里填入什么值都无关紧要。
		VolumeName:          v1.UniqueVolumeName(volume.volumeSpecName),
		InnerVolumeSpecName: volume.volumeSpecName,
		PluginName:          volume.pluginName,
		PodUID:              types.UID(volume.podName),
	}
	metrics.ForceCleanedFailedVolumeOperationsTotal.Inc()
	// TODO: 目前 cleanupMounts 只执行 UnmountVolume 操作。后续会改为在同一个流程中
	// 同时卸载卷与设备。
	err := rc.operationExecutor.UnmountVolume(mountedVolume, rc.actualStateOfWorld, rc.kubeletPodsDir)
	if err != nil {
		metrics.ForceCleanedFailedVolumeOperationsErrorsTotal.Inc()
		logger.Error(err, mountedVolume.GenerateErrorDetailed("volumeHandler.UnmountVolumeHandler for UnmountVolume failed", err).Error())
		return
	}
}

// getDeviceMountPath 返回设备挂载路径：
// 块卷使用 BlockVolumeMapper，文件系统卷使用 DeviceMounter。
func getDeviceMountPath(gvi *globalVolumeInfo) (string, error) {
	if gvi.blockVolumeMapper != nil {
		// 块卷返回其全局映射路径。
		return gvi.blockVolumeMapper.GetGlobalMapPath(gvi.volumeSpec)
	} else if gvi.deviceMounter != nil {
		// 文件系统卷在插件实现 DeviceMounter 时返回其设备挂载路径。
		return gvi.deviceMounter.GetDeviceMountPath(gvi.volumeSpec)
	} else {
		return "", fmt.Errorf("blockVolumeMapper or deviceMounter required")
	}
}

// getVolumesFromPodDir 扫描给定 Pod 目录下的卷目录，
// 返回包含 Pod UID、卷插件名、挂载路径与卷规格名的 Pod 卷信息列表。
func getVolumesFromPodDir(logger klog.Logger, podDir string) ([]podVolume, error) {
	podsDirInfo, err := os.ReadDir(podDir)
	if err != nil {
		return nil, err
	}
	volumes := []podVolume{}
	for i := range podsDirInfo {
		if !podsDirInfo[i].IsDir() {
			continue
		}
		podName := podsDirInfo[i].Name()
		podDir := filepath.Join(podDir, podName)

		// 收集文件系统卷信息。
		// 例如：/pods/{podUid}/volumes/{escapeQualifiedPluginName}/{volumeName}
		volumesDirs := map[v1.PersistentVolumeMode]string{
			v1.PersistentVolumeFilesystem: filepath.Join(podDir, kubeletconfig.DefaultKubeletVolumesDirName),
		}
		// 收集块设备卷信息。
		// 例如：/pods/{podUid}/volumeDevices/{escapeQualifiedPluginName}/{volumeName}
		volumesDirs[v1.PersistentVolumeBlock] = filepath.Join(podDir, kubeletconfig.DefaultKubeletVolumeDevicesDirName)

		for volumeMode, volumesDir := range volumesDirs {
			var volumesDirInfo []fs.DirEntry
			if volumesDirInfo, err = os.ReadDir(volumesDir); err != nil {
				// 对应 volumeMode 的目录可能不存在，直接跳过即可。
				continue
			}
			for _, volumeDir := range volumesDirInfo {
				pluginName := volumeDir.Name()
				volumePluginPath := filepath.Join(volumesDir, pluginName)
				volumePluginDirs, err := utilpath.ReadDirNoStat(volumePluginPath)
				if err != nil {
					logger.Error(err, "Could not read volume plugin directory", "volumePluginPath", volumePluginPath)
					continue
				}
				unescapePluginName := utilstrings.UnescapeQualifiedName(pluginName)
				for _, volumeName := range volumePluginDirs {
					volumePath := filepath.Join(volumePluginPath, volumeName)
					logger.V(5).Info("Volume path from volume plugin directory", "podName", podName, "volumePath", volumePath)
					volumes = append(volumes, podVolume{
						podName:        volumetypes.UniquePodName(podName),
						volumeSpecName: volumeName,
						volumePath:     volumePath,
						pluginName:     unescapePluginName,
						volumeMode:     volumeMode,
					})
				}
			}
		}
	}
	for _, volume := range volumes {
		logger.V(4).Info("Get volume from pod directory", "path", podDir, "volume", volume)
	}
	return volumes, nil
}

// reconstructVolume 通过读取 Pod 的卷目录重建卷的数据结构。
func (rc *reconciler) reconstructVolume(volume podVolume) (rvolume *reconstructedVolume, rerr error) {
	// 记录重建操作总数，并在失败时累计错误数。
	metrics.ReconstructVolumeOperationsTotal.Inc()
	defer func() {
		if rerr != nil {
			metrics.ReconstructVolumeOperationsErrorsTotal.Inc()
		}
	}()

	// 查找并初始化卷插件。
	plugin, err := rc.volumePluginMgr.FindPluginByName(volume.pluginName)
	if err != nil {
		return nil, err
	}

	// 构造仅含 UID 的最小 Pod 对象，供插件接口使用。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(volume.podName),
		},
	}
	mapperPlugin, err := rc.volumePluginMgr.FindMapperPluginByName(volume.pluginName)
	if err != nil {
		return nil, err
	}
	if volume.volumeMode == v1.PersistentVolumeBlock && mapperPlugin == nil {
		return nil, fmt.Errorf("could not find block volume plugin %q (spec.Name: %q) pod %q (UID: %q)", volume.pluginName, volume.volumeSpecName, volume.podName, pod.UID)
	}

	reconstructed, err := rc.operationExecutor.ReconstructVolumeOperation(
		volume.volumeMode,
		plugin,
		mapperPlugin,
		pod.UID,
		volume.podName,
		volume.volumeSpecName,
		volume.volumePath,
		volume.pluginName)
	if err != nil {
		return nil, err
	}
	volumeSpec := reconstructed.Spec
	if volumeSpec == nil {
		return nil, fmt.Errorf("failed to reconstruct volume for plugin %q (spec.Name: %q) pod %q (UID: %q): got nil", volume.pluginName, volume.volumeSpecName, volume.podName, pod.UID)
	}

	// 这里必须按卷规格（而不是插件名）查找插件，才能正确重建临时卷类型。
	// 按规格查找能判断卷是否真的可挂载（即是否拥有 PV）；
	// 而按插件名查找只能判断插件是否支持可挂载卷。
	deviceMountablePlugin, err := rc.volumePluginMgr.FindDeviceMountablePluginBySpec(volumeSpec)
	if err != nil {
		return nil, err
	}

	// 使用哪种唯一卷名取决于卷是否可挂载/可设备挂载
	//（needsNameFromSpec = true）。
	needsNameFromSpec := deviceMountablePlugin != nil
	if !needsNameFromSpec {
		// 仅在必要时才检查卷的可挂载性，避免对 CSI 卷调用
		// FindAttachablePluginBySpec——该调用需要连接 API server，
		// 而 kubelet 启动阶段 API server 可能尚不可用。
		// 所有 CSI 卷都是可设备挂载的，因此不会走到这里。
		attachablePlugin, err := rc.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
		if err != nil {
			return nil, err
		}
		needsNameFromSpec = attachablePlugin != nil
	}

	var uniqueVolumeName v1.UniqueVolumeName
	if needsNameFromSpec {
		uniqueVolumeName, err = util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
		if err != nil {
			return nil, err
		}
	} else {
		uniqueVolumeName = util.GetUniqueVolumeNameFromSpecWithPod(volume.podName, plugin, volumeSpec)
	}

	var volumeMapper volumepkg.BlockVolumeMapper
	var volumeMounter volumepkg.Mounter
	var deviceMounter volumepkg.DeviceMounter

	if volume.volumeMode == v1.PersistentVolumeBlock {
		var newMapperErr error
		volumeMapper, newMapperErr = mapperPlugin.NewBlockVolumeMapper(
			volumeSpec,
			pod)
		if newMapperErr != nil {
			return nil, fmt.Errorf(
				"reconstructVolume.NewBlockVolumeMapper failed for volume %q (spec.Name: %q) pod %q (UID: %q) with: %v",
				uniqueVolumeName,
				volumeSpec.Name(),
				volume.podName,
				pod.UID,
				newMapperErr)
		}
	} else {
		var err error
		volumeMounter, err = plugin.NewMounter(volumeSpec, pod)
		if err != nil {
			return nil, fmt.Errorf(
				"reconstructVolume.NewMounter failed for volume %q (spec.Name: %q) pod %q (UID: %q) with: %v",
				uniqueVolumeName,
				volumeSpec.Name(),
				volume.podName,
				pod.UID,
				err)
		}
		if deviceMountablePlugin != nil {
			deviceMounter, err = deviceMountablePlugin.NewDeviceMounter()
			if err != nil {
				return nil, fmt.Errorf("reconstructVolume.NewDeviceMounter failed for volume %q (spec.Name: %q) pod %q (UID: %q) with: %v",
					uniqueVolumeName,
					volumeSpec.Name(),
					volume.podName,
					pod.UID,
					err)
			}
		}
	}

	reconstructedVolume := &reconstructedVolume{
		volumeName:     uniqueVolumeName,
		podName:        volume.podName,
		volumeSpec:     volumeSpec,
		pod:            pod,
		deviceMounter:  deviceMounter,
		volumeGIDValue: "",
		// devicePath 会在 updateStates() 中通过节点状态的 VolumesAttached 数据更新。
		// TODO: 直接从卷挂载路径获取设备路径。
		devicePath:          "",
		mounter:             volumeMounter,
		blockVolumeMapper:   volumeMapper,
		seLinuxMountContext: reconstructed.SELinuxMountContext,
	}
	return reconstructedVolume, nil
}
