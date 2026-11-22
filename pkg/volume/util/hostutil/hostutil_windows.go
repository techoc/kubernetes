//go:build windows

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

package hostutil

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilpath "k8s.io/utils/path"
)

// HostUtil 在 Windows 平台上实现 HostUtils。
type HostUtil struct{}

// NewHostUtil 返回一个在 Windows 平台上实现 HostUtils 的结构体。
func NewHostUtil() *HostUtil {
	return &HostUtil{}
}

// GetDeviceNameFromMount 根据挂载点查找对应的设备。
func (hu *HostUtil) GetDeviceNameFromMount(mounter mount.Interface, mountPath, pluginMountDir string) (string, error) {
	return getDeviceNameFromMount(mounter, mountPath, pluginMountDir)
}

// getDeviceNameFromMount 查找挂载路径引用所匹配的插件挂载目录下的设备（盘符）名。
// 若没有任何挂载路径引用匹配，则返回给定 mountPath 的末级目录名作为卷名。
func getDeviceNameFromMount(mounter mount.Interface, mountPath, pluginMountDir string) (string, error) {
	refs, err := mounter.GetMountRefs(mountPath)
	if err != nil {
		klog.V(4).Infof("GetMountRefs failed for mount path %q: %v", mountPath, err)
		return "", err
	}
	if len(refs) == 0 {
		return "", fmt.Errorf("directory %s is not mounted", mountPath)
	}
	basemountPath := mount.NormalizeWindowsPath(pluginMountDir)
	for _, ref := range refs {
		if strings.Contains(ref, basemountPath) {
			volumeID, err := filepath.Rel(mount.NormalizeWindowsPath(basemountPath), ref)
			if err != nil {
				klog.Errorf("Failed to get volume id from mount %s - %v", mountPath, err)
				return "", err
			}
			return volumeID, nil
		}
	}

	return filepath.Base(mountPath), nil
}

// DeviceOpened 判断设备是否被其他地方占用。Windows 上始终返回未占用。
func (hu *HostUtil) DeviceOpened(pathname string) (bool, error) {
	return false, nil
}

// PathIsDevice 判断给定路径是否是设备。Windows 上始终返回否。
func (hu *HostUtil) PathIsDevice(pathname string) (bool, error) {
	return false, nil
}

// MakeRShared 检查给定路径是否位于以 'rshared' 方式传播的挂载上。
// Windows 不支持该传播模式，这里为空实现。
func (hu *HostUtil) MakeRShared(path string) error {
	return nil
}

// isSystemCannotAccessErr 判断错误是否为 Windows 的“无法访问文件”系统错误。
func isSystemCannotAccessErr(err error) bool {
	if fserr, ok := err.(*fs.PathError); ok {
		errno, ok := fserr.Err.(syscall.Errno)
		return ok && errno == windows.ERROR_CANT_ACCESS_FILE
	}

	return false
}

// GetFileType 检查路径是否为 socket/块设备/字符设备。
func (hu *(HostUtil)) GetFileType(pathname string) (FileType, error) {
	filetype, err := getFileType(pathname)

	return filetype, err
}

// PathExists 检查路径是否存在。
func (hu *HostUtil) PathExists(pathname string) (bool, error) {
	return utilpath.Exists(utilpath.CheckFollowSymlink, pathname)
}

// EvalHostSymlinks 返回解析符号链接后的路径。
func (hu *HostUtil) EvalHostSymlinks(pathname string) (string, error) {
	return filepath.EvalSymlinks(pathname)
}

// GetOwner 返回给定路径的用户和组的整数 ID。
// 注意在 Windows 上始终返回 0：该平台并不设置组，参见 SetVolumeOwnership 的实现。
func (hu *HostUtil) GetOwner(pathname string) (int64, int64, error) {
	return -1, -1, nil
}

// GetSELinuxSupport 返回是否支持 SELinux。Windows 不支持 SELinux。
func (hu *HostUtil) GetSELinuxSupport(pathname string) (bool, error) {
	return false, nil
}

// GetMode 返回路径的权限位。
func (hu *HostUtil) GetMode(pathname string) (os.FileMode, error) {
	info, err := os.Stat(pathname)
	if err != nil {
		return 0, err
	}
	return info.Mode(), nil
}

// GetSELinuxMountContext 返回给定挂载点上 -o context=XYZ 挂载选项的值。
func (hu *HostUtil) GetSELinuxMountContext(pathname string) (string, error) {
	return "", nil
}
