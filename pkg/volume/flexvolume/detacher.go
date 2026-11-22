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

package flexvolume

import (
	"fmt"
	"os"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

// flexVolumeDetacher 实现 flexVolume 插件的卸载设备与解挂载逻辑。
type flexVolumeDetacher struct {
	plugin *flexVolumeAttachablePlugin
}

var _ volume.Detacher = &flexVolumeDetacher{}

var _ volume.DeviceUnmounter = &flexVolumeDetacher{}

// Detach 是 volume.Detacher 接口的一部分。
func (d *flexVolumeDetacher) Detach(volumeName string, hostName types.NodeName) error {

	call := d.plugin.NewDriverCall(detachCmd)
	call.Append(volumeName)
	call.Append(string(hostName))

	_, err := call.Run()
	// 驱动不支持 detach 时退回默认实现，保证向后兼容。
	if isCmdNotSupportedErr(err) {
		return (*detacherDefaults)(d).Detach(volumeName, hostName)
	}
	return err
}

// UnmountDevice 是 volume.Detacher 接口的一部分。
func (d *flexVolumeDetacher) UnmountDevice(deviceMountPath string) error {

	pathExists, pathErr := mount.PathExists(deviceMountPath)
	if !pathExists {
		klog.Warningf("Unmount skipped because path does not exist: %v", deviceMountPath)
		return nil
	}
	if pathErr != nil && !mount.IsCorruptedMnt(pathErr) {
		return fmt.Errorf("error checking path: %w", pathErr)
	}

	notmnt, err := isNotMounted(d.plugin.host.GetMounter(), deviceMountPath)
	if err != nil {
		if mount.IsCorruptedMnt(err) {
			// 挂载点损坏时按“已挂载”处理，避免跳过必要的清理。
			notmnt = false // 损坏错误视为仍处于挂载状态。
		} else {
			return err
		}
	}

	if notmnt {
		klog.Warningf("Path %v already unmounted", deviceMountPath)
	} else {
		call := d.plugin.NewDriverCall(unmountDeviceCmd)
		call.Append(deviceMountPath)

		_, err := call.Run()
		if isCmdNotSupportedErr(err) {
			err = (*detacherDefaults)(d).UnmountDevice(deviceMountPath)
		}
		if err != nil {
			return err
		}
	}

	// Flexvolume 驱动可能已自行删除目录，此时无需再删。
	if pathExists, pathErr := mount.PathExists(deviceMountPath); pathErr != nil {
		return fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		return nil
	}
	return os.Remove(deviceMountPath)
}
