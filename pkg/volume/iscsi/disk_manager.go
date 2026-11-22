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

package iscsi

import (
	"os"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// diskManager 抽象磁盘操作方法，便于测试时替换。
type diskManager interface {
	MakeGlobalPDName(disk iscsiDisk) string
	MakeGlobalVDPDName(disk iscsiDisk) string
	// 将磁盘附加到 kubelet 所在宿主机。
	AttachDisk(b iscsiDiskMounter) (string, error)
	// 从 kubelet 所在宿主机分离磁盘。
	DetachDisk(disk iscsiDiskUnmounter, mntPath string) error
	// 从 kubelet 所在宿主机分离块设备。
	DetachBlockISCSIDisk(disk iscsiDiskUnmapper, mntPath string) error
}

// diskSetUp 将全局挂载路径 bind mount 到 Pod 卷目录。
// globalPDPath 形如 /var/lib/kubelet/plugins/kubernetes.io/iscsi/{ifaceName}/{portal-some_iqn-lun-lun_id}
// volPath 形如 /var/lib/kubelet/pods/{podUID}/volumes/kubernetes.io~iscsi/{volumeName}
func diskSetUp(manager diskManager, b iscsiDiskMounter, volPath string, mounter mount.Interface, mounterArgs volume.MounterArgs) error {
	fsGroup := mounterArgs.FsGroup
	fsGroupChangePolicy := mounterArgs.FSGroupChangePolicy
	notMnt, err := mounter.IsLikelyNotMountPoint(volPath)
	if err != nil && !os.IsNotExist(err) {
		klog.Errorf("cannot validate mountpoint: %s", volPath)
		return err
	}
	if !notMnt {
		return nil
	}

	if err := os.MkdirAll(volPath, 0750); err != nil {
		klog.Errorf("failed to mkdir:%s", volPath)
		return err
	}
	// 使用 bind mount，使同一磁盘可在多个位置重复挂载。
	options := []string{"bind"}
	if b.readOnly {
		options = append(options, "ro")
	}
	if b.iscsiDisk.InitiatorName != "" {
		// 新 iface 名格式为 <目标 portal>:<卷名>。
		b.iscsiDisk.Iface = b.iscsiDisk.Portals[0] + ":" + b.iscsiDisk.VolName
	}
	globalPDPath := manager.MakeGlobalPDName(*b.iscsiDisk)
	mountOptions := util.JoinMountOptions(b.mountOptions, options)
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
				// 下一次同步循环大概率会重试。
				klog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", volPath)
				return err
			}
		}
		os.Remove(volPath)
		return err
	}

	if !b.readOnly {
		// 监控属主变更进度需要较大重构，此处暂时忽略错误。
		ownershipChanger := volume.NewVolumeOwnership(&b, volPath, fsGroup, fsGroupChangePolicy, util.FSGroupCompleteHook(b.plugin, nil))
		_ = ownershipChanger.ChangePermissions()
	}

	return nil
}
