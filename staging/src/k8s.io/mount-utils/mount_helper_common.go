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

// CleanupMountPoint 卸载指定路径，并在成功后删除剩余目录；路径不存在时视为成功，保证幂等。
// extensiveMountPointCheck 为 true 时使用成本更高但能正确处理同一文件系统内绑定挂载的 IsNotMountPoint；
// 否则使用 IsLikelyNotMountPoint。损坏的挂载点仍需继续清理。
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
// 路径存在性、损坏挂载和挂载点判定沿用 CleanupMountPoint 的规则，仅卸载入口不同。
// corruptedMnt 为 true 或 mounter 允许跳过检查时直接强制卸载，否则先确认路径仍是挂载点。
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
	// 出错或已确认不是挂载点时，目录要么已删除，要么应保留给调用方处理。
	if err != nil || notMnt {
		return err
	}

	klog.V(4).Infof("%q is a mountpoint, unmounting", mountPath)
	if err := mounter.UnmountWithForce(mountPath, umountTimeout); err != nil {
		return err
	}

	notMnt, err = removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// 卸载后路径可能已被移除，也可能检查出错；仅确认仍是挂载点时才继续报错。
	if notMnt {
		return err
	}
	// 仍是挂载点，说明清理未完成。
	return fmt.Errorf("failed to cleanup mount point %v", mountPath)
}

// doCleanupMountPoint 卸载指定路径，并在成功后删除剩余目录。
// extensiveMountPointCheck 为 true 时使用 IsNotMountPoint，它能正确处理同一文件系统内的绑定挂载，但成本更高；
// 否则使用 IsLikelyNotMountPoint。corruptedMnt 表示损坏的挂载点，此时跳过检查；
// mounter 声明可以安全跳过检查时也同样直接卸载。
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
	// 出错或已确认不是挂载点时，目录要么已删除，要么应保留给调用方处理。
	if err != nil || notMnt {
		return err
	}

	klog.V(4).Infof("%q is a mountpoint, unmounting", mountPath)
	if err := mounter.Unmount(mountPath); err != nil {
		return err
	}

	notMnt, err = removePathIfNotMountPoint(mountPath, mounter, extensiveMountPointCheck)
	// 卸载后路径可能已被移除，也可能检查出错；仅确认仍是挂载点时才继续报错。
	if notMnt {
		return err
	}
	// 仍是挂载点，说明清理未完成。
	return fmt.Errorf("failed to cleanup mount point %v", mountPath)
}

// removePathIfNotMountPoint 验证给定路径是否为挂载点，仅在确认不是挂载点时尝试删除目录。
// 目录原本不是挂载点且已删除时返回 true 和 nil；出错时保留目录并返回错误，避免误删数据。
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

// removePath 尝试删除目录；目录已删除或不存在时返回 nil。
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
