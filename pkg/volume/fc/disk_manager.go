/*
Copyright 2015 The Kubernetes Authors.

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
	"os"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// diskManager 抽象 FC 磁盘操作，便于测试时替换。
type diskManager interface {
	MakeGlobalPDName(disk fcDisk) string
	MakeGlobalVDPDName(disk fcDisk) string
	// 将磁盘连接到 kubelet 所在主机。
	AttachDisk(b fcDiskMounter) (string, error)
	// 从 kubelet 所在主机断开磁盘。
	DetachDisk(disk fcDiskUnmounter, devicePath string) error
	// 从 kubelet 所在主机断开块设备磁盘。
	DetachBlockFCDisk(disk fcDiskUnmapper, mntPath, devicePath string) error
}

// diskSetUp 将 FC 磁盘上的文件系统挂载到 volPath。
func diskSetUp(manager diskManager, b fcDiskMounter, volPath string, mounter mount.Interface, fsGroup *int64, fsGroupChangePolicy *v1.PodFSGroupChangePolicy) error {
	globalPDPath := manager.MakeGlobalPDName(*b.fcDisk)
	noMnt, err := mounter.IsLikelyNotMountPoint(volPath)

	if err != nil && !os.IsNotExist(err) {
		klog.Errorf("cannot validate mountpoint: %s", volPath)
		return err
	}
	if !noMnt {
		return nil
	}
	if err := os.MkdirAll(volPath, 0750); err != nil {
		klog.Errorf("failed to mkdir:%s", volPath)
		return err
	}
	// 通过 bind mount 完整路径，使同一磁盘可在多处重复挂载。
	options := []string{"bind"}
	if b.readOnly {
		options = append(options, "ro")
	}
	mountOptions := util.JoinMountOptions(options, b.mountOptions)
	err = mounter.MountSensitiveWithoutSystemd(globalPDPath, volPath, "", mountOptions, nil)
	if err != nil {
		klog.Errorf("Failed to bind mount: source:%s, target:%s, err:%v", globalPDPath, volPath, err)
		noMnt, mntErr := b.mounter.IsLikelyNotMountPoint(volPath)
		if mntErr != nil {
			klog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
			return err
		}
		if !noMnt {
			if mntErr = b.mounter.Unmount(volPath); mntErr != nil {
				klog.Errorf("Failed to unmount: %v", mntErr)
				return err
			}
			noMnt, mntErr = b.mounter.IsLikelyNotMountPoint(volPath)
			if mntErr != nil {
				klog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
				return err
			}
			if !noMnt {
				// 卸载未生效，留待下一个 sync 循环重试。
				klog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", volPath)
				return err
			}
		}
		os.Remove(volPath)

		return err
	}

	if !b.readOnly {
		ownershipChanger := volume.NewVolumeOwnership(&b, volPath, fsGroup, fsGroupChangePolicy, util.FSGroupCompleteHook(b.plugin, nil))
		// TODO: 正确处理此处返回的错误。
		_ = ownershipChanger.ChangePermissions()
	}

	return nil
}
