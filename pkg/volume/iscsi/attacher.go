/*
Copyright 2017 The Kubernetes Authors.

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

package iscsi

import (
	"fmt"
	"os"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"k8s.io/utils/keymutex"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// iscsiAttacher 实现 iSCSI 卷的挂载相关接口，并通过 targetLocks 串行化对同一 target 的操作。
type iscsiAttacher struct {
	host        volume.VolumeHost
	targetLocks keymutex.KeyMutex
	manager     diskManager
}

var _ volume.Attacher = &iscsiAttacher{}

var _ volume.DeviceMounter = &iscsiAttacher{}

var _ volume.AttachableVolumePlugin = &iscsiPlugin{}

var _ volume.DeviceMountableVolumePlugin = &iscsiPlugin{}

// NewAttacher 创建一个 iscsiAttacher 实例。
func (plugin *iscsiPlugin) NewAttacher() (volume.Attacher, error) {
	return &iscsiAttacher{
		host:        plugin.host,
		targetLocks: plugin.targetLocks,
		manager:     &ISCSIUtil{},
	}, nil
}

// VerifyExhaustedResource 检查卷规格是否耗尽资源；iSCSI 无需检查，始终返回 false。
func (plugin *iscsiPlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return false
}

// NewDeviceMounter 复用 NewAttacher，因为 iSCSI 的 attach 与设备挂载由同一实现完成。
func (plugin *iscsiPlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.NewAttacher()
}

// GetDeviceMountRefs 返回设备挂载点的引用（其他挂载点）列表。
func (plugin *iscsiPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mounter.GetMountRefs(deviceMountPath)
}

// Attach 为空实现：iSCSI 卷由 kubelet 端通过 WaitForAttach 完成 attach。
func (attacher *iscsiAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	return "", nil
}

// VolumesAreAttached 假定所有给定卷均已连接（iSCSI 由 kubelet 自行管理连接状态）。
func (attacher *iscsiAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	volumesAttachedCheck := make(map[*volume.Spec]bool)
	for _, spec := range specs {
		volumesAttachedCheck[spec] = true
	}

	return volumesAttachedCheck, nil
}

// WaitForAttach 等待 iSCSI 设备出现并返回其路径，实际连接由 AttachDisk 完成。
func (attacher *iscsiAttacher) WaitForAttach(spec *volume.Spec, devicePath string, pod *v1.Pod, timeout time.Duration) (string, error) {
	mounter, err := volumeSpecToMounter(spec, attacher.host, attacher.targetLocks, pod)
	if err != nil {
		klog.Warningf("failed to get iscsi mounter: %v", err)
		return "", err
	}
	return attacher.manager.AttachDisk(*mounter)
}

// GetDeviceMountPath 返回 iSCSI 设备的全局挂载路径。
func (attacher *iscsiAttacher) GetDeviceMountPath(
	spec *volume.Spec) (string, error) {
	mounter, err := volumeSpecToMounter(spec, attacher.host, attacher.targetLocks, nil)
	if err != nil {
		klog.Warningf("failed to get iscsi mounter: %v", err)
		return "", err
	}
	if mounter.InitiatorName != "" {
		// 新的 iface 名称约定为 <target portal>:<volume name>。
		mounter.Iface = mounter.Portals[0] + ":" + mounter.VolName
	}
	return attacher.manager.MakeGlobalPDName(*mounter.iscsiDisk), nil
}

// MountDevice 在必要时格式化并将 iSCSI 设备挂载到 deviceMountPath。
func (attacher *iscsiAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, mountArgs volume.DeviceMounterArgs) error {
	mounter := attacher.host.GetMounter()
	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}
	readOnly, fsType, err := getISCSIVolumeInfo(spec)
	if err != nil {
		return err
	}

	options := []string{}
	if readOnly {
		options = append(options, "ro")
	}
	if mountArgs.SELinuxLabel != "" {
		options = volumeutil.AddSELinuxMountOption(options, mountArgs.SELinuxLabel)
	}
	if notMnt {
		diskMounter := &mount.SafeFormatAndMount{Interface: mounter, Exec: utilexec.New()}
		mountOptions := volumeutil.MountOptionFromSpec(spec, options...)
		err = diskMounter.FormatAndMount(devicePath, deviceMountPath, fsType, mountOptions)
		if err != nil {
			os.Remove(deviceMountPath)
			return err
		}
	}
	return nil
}

// iscsiDetacher 实现 iSCSI 卷的卸载与断开接口。
type iscsiDetacher struct {
	host    volume.VolumeHost
	mounter mount.Interface
	manager diskManager
	plugin  *iscsiPlugin
}

var _ volume.Detacher = &iscsiDetacher{}

var _ volume.DeviceUnmounter = &iscsiDetacher{}

// NewDetacher 创建一个 iscsiDetacher 实例。
func (plugin *iscsiPlugin) NewDetacher() (volume.Detacher, error) {
	return &iscsiDetacher{
		host:    plugin.host,
		mounter: plugin.host.GetMounter(),
		manager: &ISCSIUtil{},
		plugin:  plugin,
	}, nil
}

// NewDeviceUnmounter 复用 NewDetacher，因为 iSCSI 的 detach 与设备卸载由同一实现完成。
func (plugin *iscsiPlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.NewDetacher()
}

// Detach 为空实现：iSCSI 卷的实际断开在 UnmountDevice 中完成。
func (detacher *iscsiDetacher) Detach(volumeName string, nodeName types.NodeName) error {
	return nil
}

// UnmountDevice 断开 iSCSI 磁盘并清理设备挂载目录。
func (detacher *iscsiDetacher) UnmountDevice(deviceMountPath string) error {
	unMounter := volumeSpecToUnmounter(detacher.mounter, detacher.host, detacher.plugin)
	err := detacher.manager.DetachDisk(*unMounter, deviceMountPath)
	if err != nil {
		return fmt.Errorf("iscsi: failed to detach disk: %s\nError: %v", deviceMountPath, err)
	}
	klog.V(4).Infof("iscsi: %q is unmounted, deleting the directory", deviceMountPath)
	err = os.RemoveAll(deviceMountPath)
	if err != nil {
		return fmt.Errorf("iscsi: failed to delete the directory: %s\nError: %v", deviceMountPath, err)
	}
	klog.V(4).Infof("iscsi: successfully detached disk: %s", deviceMountPath)
	return nil
}

// CanAttach 表示 iSCSI 卷始终可 attach。
func (plugin *iscsiPlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return true, nil
}

// CanDeviceMount 表示 iSCSI 卷始终可进行设备挂载。
func (plugin *iscsiPlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// volumeSpecToMounter 将卷规格转换为 iscsiDiskMounter；pod 非空时读取其命名空间下的 secret。
func volumeSpecToMounter(spec *volume.Spec, host volume.VolumeHost, targetLocks keymutex.KeyMutex, pod *v1.Pod) (*iscsiDiskMounter, error) {
	var secret map[string]string
	readOnly, fsType, err := getISCSIVolumeInfo(spec)
	if err != nil {
		return nil, err
	}
	var podUID types.UID
	if pod != nil {
		secret, err = createSecretMap(spec, &iscsiPlugin{host: host, targetLocks: targetLocks}, pod.Namespace)
		if err != nil {
			return nil, err
		}
		podUID = pod.UID
	}
	iscsiDisk, err := createISCSIDisk(spec,
		podUID,
		&iscsiPlugin{host: host, targetLocks: targetLocks},
		&ISCSIUtil{},
		secret,
	)
	if err != nil {
		return nil, err
	}
	exec := utilexec.New()

	volumeMode, err := volumeutil.GetVolumeMode(spec)
	if err != nil {
		return nil, err
	}

	klog.V(5).Infof("iscsi: VolumeSpecToMounter volumeMode %s", volumeMode)
	return &iscsiDiskMounter{
		iscsiDisk:  iscsiDisk,
		fsType:     fsType,
		volumeMode: volumeMode,
		readOnly:   readOnly,
		mounter:    &mount.SafeFormatAndMount{Interface: host.GetMounter(), Exec: exec},
		exec:       exec,
		deviceUtil: volumeutil.NewDeviceHandler(volumeutil.NewIOHandler()),
	}, nil
}

// volumeSpecToUnmounter 构造用于断开 iSCSI 磁盘的 iscsiDiskUnmounter。
func volumeSpecToUnmounter(mounter mount.Interface, host volume.VolumeHost, plugin *iscsiPlugin) *iscsiDiskUnmounter {
	return &iscsiDiskUnmounter{
		iscsiDisk: &iscsiDisk{
			plugin: plugin,
		},
		mounter:    mounter,
		exec:       utilexec.New(),
		deviceUtil: volumeutil.NewDeviceHandler(volumeutil.NewIOHandler()),
	}
}
