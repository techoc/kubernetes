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

// Package volumepathhandler 提供块设备卷的映射路径管理：创建/删除符号链接或绑定挂载，
// 以及查询映射关系。全局映射路径供 kubelet 管理，Pod 映射路径供容器运行时使用。
package volumepathhandler

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	"k8s.io/apimachinery/pkg/types"
)

const (
	losetupPath = "losetup"
	// ErrDeviceNotFound 表示未找到对应 loop 设备时返回的标记字符串。
	ErrDeviceNotFound = "device not found"
)

// BlockVolumePathHandler 定义块设备卷映射路径相关操作的接口。
type BlockVolumePathHandler interface {
	// MapDevice 在指定映射路径下创建指向块设备的符号链接或绑定挂载
	MapDevice(devicePath string, mapPath string, linkName string, bindMount bool) error
	// UnmapDevice 删除指定映射路径下块设备对应的符号链接或绑定挂载
	UnmapDevice(mapPath string, linkName string, bindMount bool) error
	// RemoveMapPath 删除指定映射路径下的文件或目录
	RemoveMapPath(mapPath string) error
	// IsSymlinkExist 判断指定符号链接是否存在
	IsSymlinkExist(mapPath string) (bool, error)
	// IsDeviceBindMountExist 判断指定的绑定挂载是否存在
	IsDeviceBindMountExist(mapPath string) (bool, error)
	// GetDeviceBindMountRefs 在全局映射路径下搜索绑定挂载
	GetDeviceBindMountRefs(devPath string, mapPath string) ([]string, error)
	// FindGlobalMapPathUUIDFromPod 根据 Pod 映射路径的符号链接，
	// 在 globalMapPath 下找到对应的 {pod uuid} 符号链接，返回带 Pod UUID 的全局映射路径。
	FindGlobalMapPathUUIDFromPod(pluginDir, mapPath string, podUID types.UID) (string, error)
	// AttachFileDevice 将普通文件路径映射为已附加的块设备。
	AttachFileDevice(path string) (string, error)
	// DetachFileDevice 将已附加的块设备从文件解除映射。
	DetachFileDevice(path string) error
	// GetLoopDevice 返回与给定路径关联的 loop 设备的完整路径。
	GetLoopDevice(path string) (string, error)
}

// NewBlockVolumePathHandler 返回一个新的 BlockVolumePathHandler 实例。
func NewBlockVolumePathHandler() BlockVolumePathHandler {
	var volumePathHandler VolumePathHandler
	return volumePathHandler
}

// VolumePathHandler 是块设备卷路径相关操作的实现类型。
type VolumePathHandler struct {
}

// MapDevice 在指定映射路径下创建指向块设备的符号链接或绑定挂载。
func (v VolumePathHandler) MapDevice(devicePath string, mapPath string, linkName string, bindMount bool) error {
	// 全局映射路径示例：
	//   globalMapPath/linkName: plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{podUid}
	//   linkName: {podUid}
	//
	// Pod 设备映射路径示例：
	//   podDeviceMapPath/linkName: pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/{volumeName}
	//   linkName: {volumeName}
	if len(devicePath) == 0 {
		return fmt.Errorf("failed to map device to map path. devicePath is empty")
	}
	if len(mapPath) == 0 {
		return fmt.Errorf("failed to map device to map path. mapPath is empty")
	}
	if !filepath.IsAbs(mapPath) {
		return fmt.Errorf("the map path should be absolute: map path: %s", mapPath)
	}
	klog.V(5).Infof("MapDevice: devicePath %s", devicePath)
	klog.V(5).Infof("MapDevice: mapPath %s", mapPath)
	klog.V(5).Infof("MapDevice: linkName %s", linkName)

	// 检查并创建映射路径。
	_, err := os.Stat(mapPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot validate map path: %s: %v", mapPath, err)
	}
	if err = os.MkdirAll(mapPath, 0750); err != nil {
		return fmt.Errorf("failed to mkdir %s: %v", mapPath, err)
	}

	if bindMount {
		return mapBindMountDevice(devicePath, mapPath, linkName)
	}
	return mapSymlinkDevice(devicePath, mapPath, linkName)
}

func mapBindMountDevice(devicePath string, mapPath string, linkName string) error {
	// 检查绑定挂载是否已存在。
	linkPath := filepath.Join(mapPath, string(linkName))

	file, err := os.Stat(linkPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat file %s: %v", linkPath, err)
		}

		// 创建作为挂载点的占位文件。
		newFile, err := os.OpenFile(linkPath, os.O_CREATE|os.O_RDWR, 0750)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", linkPath, err)
		}
		if err := newFile.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %v", linkPath, err)
		}
	} else {
		// 设备文件已存在，说明挂载已生效；普通文件则仅告警并继续尝试挂载。
		// TODO: 需要检查该设备文件是否真的是预期的绑定挂载
		if file.Mode()&os.ModeDevice == os.ModeDevice {
			klog.Warningf("Map skipped because bind mount already exists on the path: %v", linkPath)
			return nil
		}

		klog.Warningf("File %s already exists but is not mounted, skip creating file", linkPath)
	}

	// 将设备绑定挂载到占位文件上。
	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	if err := mounter.MountSensitiveWithoutSystemd(devicePath, linkPath, "" /* fsType */, []string{"bind"}, nil); err != nil {
		return fmt.Errorf("failed to bind mount devicePath: %s to linkPath %s: %v", devicePath, linkPath, err)
	}

	return nil
}

func mapSymlinkDevice(devicePath string, mapPath string, linkName string) error {
	// 先移除旧符号链接（或文件）再创建新的。
	// 节点重启后旧符号链接可能已失效，必须重建。
	linkPath := filepath.Join(mapPath, string(linkName))
	if err := os.Remove(linkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file %s: %v", linkPath, err)
	}
	return os.Symlink(devicePath, linkPath)
}

// UnmapDevice 删除指定映射路径下块设备对应的符号链接或绑定挂载。
func (v VolumePathHandler) UnmapDevice(mapPath string, linkName string, bindMount bool) error {
	if len(mapPath) == 0 {
		return fmt.Errorf("failed to unmap device from map path. mapPath is empty")
	}
	klog.V(5).Infof("UnmapDevice: mapPath %s", mapPath)
	klog.V(5).Infof("UnmapDevice: linkName %s", linkName)

	if bindMount {
		return unmapBindMountDevice(v, mapPath, linkName)
	}
	return unmapSymlinkDevice(v, mapPath, linkName)
}

func unmapBindMountDevice(v VolumePathHandler, mapPath string, linkName string) error {
	// 检查绑定挂载是否存在。
	linkPath := filepath.Join(mapPath, string(linkName))
	if isMountExist, checkErr := v.IsDeviceBindMountExist(linkPath); checkErr != nil {
		return checkErr
	} else if !isMountExist {
		klog.Warningf("Unmap skipped because bind mount does not exist on the path: %v", linkPath)

		// 挂载可能已消失，但占位文件还在，需要清理。
		if _, err := os.Stat(linkPath); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to check if path %s exists: %v", linkPath, err)
			}
			// linkPath 已被移除。
			return nil
		}
		// 删除残留文件。
		if err := os.Remove(linkPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove file %s: %v", linkPath, err)
		}
		return nil
	}

	// 先卸载，再删除挂载点文件。
	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	if err := mounter.Unmount(linkPath); err != nil {
		return fmt.Errorf("failed to unmount linkPath %s: %v", linkPath, err)
	}

	// 删除文件。
	if err := os.Remove(linkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file %s: %v", linkPath, err)
	}

	return nil
}

func unmapSymlinkDevice(v VolumePathHandler, mapPath string, linkName string) error {
	// 先检查符号链接是否存在，避免对不存在的路径执行删除。
	linkPath := filepath.Join(mapPath, string(linkName))
	if islinkExist, checkErr := v.IsSymlinkExist(linkPath); checkErr != nil {
		return checkErr
	} else if !islinkExist {
		klog.Warningf("Unmap skipped because symlink does not exist on the path: %v", linkPath)
		return nil
	}
	return os.Remove(linkPath)
}

// RemoveMapPath 删除指定映射路径下的文件或目录。
func (v VolumePathHandler) RemoveMapPath(mapPath string) error {
	if len(mapPath) == 0 {
		return fmt.Errorf("failed to remove map path. mapPath is empty")
	}
	klog.V(5).Infof("RemoveMapPath: mapPath %s", mapPath)
	err := os.RemoveAll(mapPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove directory %s: %v", mapPath, err)
	}
	return nil
}

// IsSymlinkExist 判断指定文件是否存在且为符号链接（Windows 上还包括不规则文件）。
// 文件不存在或存在但不是符号链接时返回 false 且无错误；其他情况返回 false 和 Lstat 错误。
func (v VolumePathHandler) IsSymlinkExist(mapPath string) (bool, error) {
	fi, err := os.Lstat(mapPath)
	if err != nil {
		// 文件不存在，返回 false 且无错误。
		if os.IsNotExist(err) {
			return false, nil
		}
		// 返回 Lstat 错误。
		return false, fmt.Errorf("failed to Lstat file %s: %v", mapPath, err)
	}
	// 文件存在且为符号链接，返回 true 且无错误。
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		return true, nil
	}
	// go1.23 行为变更：Windows 上部分符号链接被识别为不规则文件。
	// go1.23 behavior change: https://github.com/golang/go/issues/63703#issuecomment-2535941458
	if (runtime.GOOS == "windows") && (fi.Mode()&os.ModeIrregular != 0) {
		return true, nil
	}
	// 文件存在但不是符号链接，返回 false 且无错误。
	return false, nil
}

// IsDeviceBindMountExist 判断指定文件是否存在且为设备文件。
// 文件不存在或存在但不是设备时返回 false 且无错误；其他情况返回 false 和 Lstat 错误。
func (v VolumePathHandler) IsDeviceBindMountExist(mapPath string) (bool, error) {
	fi, err := os.Lstat(mapPath)
	if err != nil {
		// 文件不存在，返回 false 且无错误。
		if os.IsNotExist(err) {
			return false, nil
		}

		// 返回 Lstat 错误。
		return false, fmt.Errorf("failed to Lstat file %s: %v", mapPath, err)
	}
	// 文件存在且为设备，返回 true 且无错误。
	if fi.Mode()&os.ModeDevice == os.ModeDevice {
		return true, nil
	}
	// 文件存在但不是设备，返回 false 且无错误。
	return false, nil
}

// GetDeviceBindMountRefs 在全局映射路径下搜索绑定挂载，返回其路径列表。
func (v VolumePathHandler) GetDeviceBindMountRefs(devPath string, mapPath string) ([]string, error) {
	var refs []string
	files, err := os.ReadDir(mapPath)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		// 只关心设备文件，普通文件不是绑定挂载。
		if file.Type()&os.ModeDevice != os.ModeDevice {
			continue
		}
		filename := file.Name()
		// TODO: 可能需要检查该文件是否确实链接到 devPath
		refs = append(refs, filepath.Join(mapPath, filename))
	}
	klog.V(5).Infof("GetDeviceBindMountRefs: refs %v", refs)
	return refs, nil
}
