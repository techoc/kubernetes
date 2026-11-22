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

package fc

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

// fcAttacher 实现 FC 卷的挂载与设备挂载接口。
type fcAttacher struct {
	host    volume.VolumeHost
	manager diskManager
}

var _ volume.Attacher = &fcAttacher{}

var _ volume.DeviceMounter = &fcAttacher{}

var _ volume.AttachableVolumePlugin = &fcPlugin{}

var _ volume.DeviceMountableVolumePlugin = &fcPlugin{}

// NewAttacher 创建 FC 卷的挂载器。
func (plugin *fcPlugin) NewAttacher() (volume.Attacher, error) {
	return &fcAttacher{
		host:    plugin.host,
		manager: &fcUtil{},
	}, nil
}

// NewDeviceMounter 创建设备挂载器；FC 与卷挂载共用同一实现。
func (plugin *fcPlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.NewAttacher()
}

// GetDeviceMountRefs 返回设备挂载点的引用列表。
func (plugin *fcPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mounter.GetMountRefs(deviceMountPath)
}

// Attach 空实现：FC 卷在 WaitForAttach 阶段才完成实际挂载。
func (attacher *fcAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	return "", nil
}

// VolumesAreAttached 假设所有卷均已挂载；FC 的挂载状态在节点侧检查。
func (attacher *fcAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	volumesAttachedCheck := make(map[*volume.Spec]bool)
	for _, spec := range specs {
		volumesAttachedCheck[spec] = true
	}

	return volumesAttachedCheck, nil
}

// VerifyExhaustedResource 返回 false：FC 无需检查 exhausted 资源状态。
func (plugin *fcPlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return false
}

// WaitForAttach 根据卷规格找到并挂载 FC 磁盘，返回设备路径。
func (attacher *fcAttacher) WaitForAttach(spec *volume.Spec, devicePath string, _ *v1.Pod, timeout time.Duration) (string, error) {
	mounter, err := volumeSpecToMounter(spec, attacher.host)
	if err != nil {
		klog.Warningf("failed to get fc mounter: %v", err)
		return "", err
	}
	return attacher.manager.AttachDisk(*mounter)
}

// GetDeviceMountPath 返回 FC 磁盘的全局挂载路径。
func (attacher *fcAttacher) GetDeviceMountPath(
	spec *volume.Spec) (string, error) {
	mounter, err := volumeSpecToMounter(spec, attacher.host)
	if err != nil {
		klog.Warningf("failed to get fc mounter: %v", err)
		return "", err
	}

	return attacher.manager.MakeGlobalPDName(*mounter.fcDisk), nil
}

// MountDevice 在全局挂载路径上格式化并挂载 FC 设备；已挂载则跳过。
func (attacher *fcAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, mountArgs volume.DeviceMounterArgs) error {
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

	volumeSource, readOnly, err := getVolumeSource(spec)
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
		diskMounter := &mount.SafeFormatAndMount{Interface: mounter, Exec: exec.New()}
		mountOptions := volumeutil.MountOptionFromSpec(spec, options...)
		err = diskMounter.FormatAndMount(devicePath, deviceMountPath, volumeSource.FSType, mountOptions)
		if err != nil {
			os.Remove(deviceMountPath)
			return err
		}
	}
	return nil
}

// fcDetacher 实现 FC 卷的卸载与设备卸载接口。
type fcDetacher struct {
	mounter mount.Interface
	manager diskManager
	host    volume.VolumeHost
}

var _ volume.Detacher = &fcDetacher{}

var _ volume.DeviceUnmounter = &fcDetacher{}

// NewDetacher 创建 FC 卷的卸载器。
func (plugin *fcPlugin) NewDetacher() (volume.Detacher, error) {
	return &fcDetacher{
		mounter: plugin.host.GetMounter(),
		manager: &fcUtil{},
		host:    plugin.host,
	}, nil
}

// NewDeviceUnmounter 创建设备卸载器；FC 与卷卸载共用同一实现。
func (plugin *fcPlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.NewDetacher()
}

// Detach 空实现：FC 卷在 UnmountDevice 阶段完成分离。
func (detacher *fcDetacher) Detach(volumeName string, nodeName types.NodeName) error {
	return nil
}

// UnmountDevice 卸载全局挂载路径并分离 FC 磁盘。
func (detacher *fcDetacher) UnmountDevice(deviceMountPath string) error {
	// 先取设备名，供后续 DetachDisk 使用。
	devName, _, err := mount.GetDeviceNameFromMount(detacher.mounter, deviceMountPath)
	if err != nil {
		klog.Errorf("fc: failed to get device from mnt: %s\nError: %v", deviceMountPath, err)
		return err
	}
	// 卸载 deviceMountPath（即 globalPDPath）。
	err = mount.CleanupMountPoint(deviceMountPath, detacher.mounter, false)
	if err != nil {
		return fmt.Errorf("fc: failed to unmount: %s\nError: %v", deviceMountPath, err)
	}
	// 若 deviceMountPath 本就不是挂载点，GetDeviceNameFromMount 返回空字符串；
	// 此时无需（也不应）调用 DetachDisk，否则会报错。
	if devName == "" {
		return nil
	}

	unMounter := volumeSpecToUnmounter(detacher.mounter)
	// 设备已卸载；若此时重试 UnmountDevice，GetDeviceNameFromMount 找不到挂载点，
	// 也就不会再进入下面的 DetachDisk，因此这里自行实现重试。
	// 例如刚卸载时 DetachDisk 可能因 "device is busy" 无法刷新多路径设备。
	// 在 6 分钟强制分离超时内，2 分钟重试窗口足够。
	var detachError error
	err = wait.PollImmediate(10*time.Second, 2*time.Minute, func() (bool, error) {
		detachError = detacher.manager.DetachDisk(*unMounter, devName)
		if detachError != nil {
			klog.V(4).Infof("fc: failed to detach disk %s (%s): %v", devName, deviceMountPath, detachError)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("fc: failed to detach disk: %s: %v", devName, detachError)
	}

	klog.V(2).Infof("fc: successfully detached disk: %s", devName)
	return nil
}

// CanAttach 返回 true：FC 插件始终允许挂载。
func (plugin *fcPlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return true, nil
}

// CanDeviceMount 返回 true：FC 插件始终允许设备挂载。
func (plugin *fcPlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// volumeSpecToMounter 从卷规格构造 FC 磁盘挂载器，兼容 lun 与 WWID 两种标识方式。
func volumeSpecToMounter(spec *volume.Spec, host volume.VolumeHost) (*fcDiskMounter, error) {
	fc, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}
	var lun string
	var wwids []string
	if fc.Lun != nil && len(fc.TargetWWNs) != 0 {
		lun = strconv.Itoa(int(*fc.Lun))
	} else if len(fc.WWIDs) != 0 {
		for _, wwid := range fc.WWIDs {
			wwids = append(wwids, strings.Replace(wwid, " ", "_", -1))
		}
	} else {
		return nil, fmt.Errorf("fc: no fc disk information found. failed to make a new mounter")
	}
	fcDisk := &fcDisk{
		plugin: &fcPlugin{
			host: host,
		},
		wwns:  fc.TargetWWNs,
		lun:   lun,
		wwids: wwids,
		io:    &osIOHandler{},
	}

	volumeMode, err := volumeutil.GetVolumeMode(spec)
	if err != nil {
		return nil, err
	}

	klog.V(5).Infof("fc: volumeSpecToMounter volumeMode %s", volumeMode)
	return &fcDiskMounter{
		fcDisk:       fcDisk,
		fsType:       fc.FSType,
		volumeMode:   volumeMode,
		readOnly:     readOnly,
		mounter:      mount.NewSafeFormatAndMount(host.GetMounter(), exec.New()),
		deviceUtil:   volumeutil.NewDeviceHandler(volumeutil.NewIOHandler()),
		mountOptions: volumeutil.MountOptionFromSpec(spec),
	}, nil
}

// volumeSpecToUnmounter 构造 FC 磁盘卸载器。
func volumeSpecToUnmounter(mounter mount.Interface) *fcDiskUnmounter {
	return &fcDiskUnmounter{
		fcDisk: &fcDisk{
			io: &osIOHandler{},
		},
		mounter:    mounter,
		deviceUtil: volumeutil.NewDeviceHandler(volumeutil.NewIOHandler()),
		exec:       exec.New(),
	}
}
