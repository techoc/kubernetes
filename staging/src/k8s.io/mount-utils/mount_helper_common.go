/*
Copyright 2018 The Kubernetes Authors.

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

package mount

import (
	"fmt"
	"os"
	"time"

	"k8s.io/klog/v2"
)

// CleanupMountPoint unmounts the given path and deletes the remaining directory
// if successful. If extensiveMountPointCheck is true IsNotMountPoint will be
// called instead of IsLikelyNotMountPoint. IsNotMountPoint is more expensive
// but properly handles bind mounts within the same fs.
// 路径不存在直接返回成功，让卸载幂等；损坏的挂载点仍要继续清理。
func CleanupMountPoint(mountPath string, mounter Interface, extensiveMountPointCheck bool) error {
	pathExists, pathErr := PathExists(mountPath)
	if !pathExists && pathErr == nil {
		klog.Warningf("Mount cleanup skipped because path does not exist: %v", mountPath)
		return nil
	}
	corruptedMnt := IsCorruptedMnt(pathErr)
	if pathErr != nil && !corruptedMnt {
		return fmt.Errorf("Error checking path: %v", pathErr)
	}
	return doCleanupMountPoint(mountPath, mounter, extensiveMountPointCheck, corruptedMnt)
}

// CleanupMountWithForce 在普通卸载可能卡住时使用，超时后改用强制卸载。
// 与 CleanupMountPoint 逻辑一致，区别只在卸载调用的入口。
func CleanupMountWithForce(mountPath string, mounter MounterForceUnmounter, extensiveMountPointCheck bool, umountTimeout time.Duration) error {
	pathExists, pathErr := PathExists(mountPath)
	if !pathExists && pathErr == nil {
		klog.Warningf("Mount cleanup skipped because path does not exist: %v", mountPath)
		return nil
	}
	corruptedMnt := IsCorruptedMnt(pathErr)
	if pathErr != nil && !corruptedMnt {
		return fmt.Errorf("Error checking path: %v", pathErr)
	}

	if corruptedMnt || mounter.CanSafelySkipMountPointCheck() {
		klog.V(4).Infof("unmounting %q (corruptedMount: %t, mounterCanSkipMountPointChecks: %t)",
			mountPath, corruptedMnt, mounter.CanSafelySkipMountPointCheck())
		if err := mounter.UnmountWithForce(mountPath, umountTimeout); err != nil {
			return err
		}
		return removePath(mountPath)
	}

	notMnt, err := removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// if mountPath is not a mount point, it's just been removed or there was an error
	if err != nil || notMnt {
		return err
	}

	klog.V(4).Infof("%q is a mountpoint, unmounting", mountPath)
	if err := mounter.UnmountWithForce(mountPath, umountTimeout); err != nil {
		return err
	}

	notMnt, err = removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// if mountPath is not a mount point, it's either just been removed or there was an error
	if notMnt {
		return err
	}
	// mountPath is still a mount point
	return fmt.Errorf("failed to cleanup mount point %v", mountPath)
}

// doCleanupMountPoint unmounts the given path and
// deletes the remaining directory if successful.
// if extensiveMountPointCheck is true
// IsNotMountPoint will be called instead of IsLikelyNotMountPoint.
// IsNotMountPoint is more expensive but properly handles bind mounts within the same fs.
// if corruptedMnt is true, it means that the mountPath is a corrupted mountpoint, and the mount point check
// will be skipped. The mount point check will also be skipped if the mounter supports it.
// 损坏挂载点无法再做检查，只能直接卸载；检查失败时宁可保留目录也不误删。
func doCleanupMountPoint(mountPath string, mounter Interface, extensiveMountPointCheck bool, corruptedMnt bool) error {
	if corruptedMnt || mounter.CanSafelySkipMountPointCheck() {
		klog.V(4).Infof("unmounting %q (corruptedMount: %t, mounterCanSkipMountPointChecks: %t)",
			mountPath, corruptedMnt, mounter.CanSafelySkipMountPointCheck())
		if err := mounter.Unmount(mountPath); err != nil {
			return err
		}
		return removePath(mountPath)
	}

	notMnt, err := removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// if mountPath is not a mount point, it's just been removed or there was an error
	if err != nil || notMnt {
		return err
	}

	klog.V(4).Infof("%q is a mountpoint, unmounting", mountPath)
	if err := mounter.Unmount(mountPath); err != nil {
		return err
	}

	notMnt, err = removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// if mountPath is not a mount point, it's either just been removed or there was an error
	if notMnt {
		return err
	}
	// mountPath is still a mount point
	return fmt.Errorf("failed to cleanup mount point %v", mountPath)
}

// removePathIfNotMountPoint verifies if given mountPath is a mount point if not it attempts
// to remove the directory. Returns true and nil if directory was not a mount point and removed.
// 只在确认不是挂载点时才删目录，防止误删挂载在非空目录上的数据。
func removePathIfNotMountPoint(mountPath string, mounter Interface, extensiveMountPointCheck bool) (bool, error) {
	var notMnt bool
	var err error

	if extensiveMountPointCheck {
		notMnt, err = IsNotMountPoint(mounter, mountPath)
	} else {
		notMnt, err = mounter.IsLikelyNotMountPoint(mountPath)
	}

	if err != nil {
		if os.IsNotExist(err) {
			klog.V(4).Infof("%q does not exist", mountPath)
			return true, nil
		}
		return notMnt, err
	}

	if notMnt {
		klog.V(4).Infof("%q is not a mountpoint, deleting", mountPath)
		return notMnt, os.Remove(mountPath)
	}
	return notMnt, nil
}

// removePath attempts to remove the directory. Returns nil if the directory was removed or does not exist.
// 已不存在视为成功，让多次清理调用保持幂等。
func removePath(mountPath string) error {
	klog.V(4).Infof("Deleting path %q", mountPath)
	err := os.Remove(mountPath)
	if os.IsNotExist(err) {
		klog.V(4).Infof("%q does not exist", mountPath)
		return nil
	}
	return err
}
