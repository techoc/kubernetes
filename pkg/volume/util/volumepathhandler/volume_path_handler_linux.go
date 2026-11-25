//go:build linux

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

package volumepathhandler

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// AttachFileDevice 将普通文件作为已附加的块设备暴露出来。
func (v VolumePathHandler) AttachFileDevice(path string) (string, error) {
	blockDevicePath, err := v.GetLoopDevice(path)
	if err != nil && err.Error() != ErrDeviceNotFound {
		return "", fmt.Errorf("GetLoopDevice failed for path %s: %v", path, err)
	}

	// 若该路径尚无对应的 loop 设备，则创建一个。
	if blockDevicePath == "" {
		klog.V(4).Infof("Creating device for path: %s", path)
		blockDevicePath, err = makeLoopDevice(path)
		if err != nil {
			return "", fmt.Errorf("makeLoopDevice failed for path %s: %v", path, err)
		}
	}
	return blockDevicePath, nil
}

// DetachFileDevice 将已附加块设备对应的路径从块设备上分离。
func (v VolumePathHandler) DetachFileDevice(path string) error {
	loopPath, err := v.GetLoopDevice(path)
	if err != nil {
		if err.Error() == ErrDeviceNotFound {
			klog.Warningf("couldn't find loopback device which takes file descriptor lock. Skip detaching device. device path: %q", path)
		} else {
			return fmt.Errorf("GetLoopDevice failed for path %s: %v", path, err)
		}
	} else {
		if len(loopPath) != 0 {
			err = removeLoopDevice(loopPath)
			if err != nil {
				return fmt.Errorf("removeLoopDevice failed for path %s: %v", path, err)
			}
		}
	}
	return nil
}

// GetLoopDevice 返回与给定路径关联的 loop 设备的完整路径。
func (v VolumePathHandler) GetLoopDevice(path string) (string, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return "", errors.New(ErrDeviceNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("not attachable: %v", err)
	}

	return getLoopDeviceFromSysfs(path)
}

// makeLoopDevice 通过 losetup 为指定文件创建 loop 设备。
func makeLoopDevice(path string) (string, error) {
	args := []string{"-f", path}
	cmd := exec.Command(losetupPath, args...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		klog.V(2).Infof("Failed device create command for path: %s %v %s", path, err, out)
		return "", fmt.Errorf("losetup %s failed: %v", strings.Join(args, " "), err)
	}

	return getLoopDeviceFromSysfs(path)
}

// removeLoopDevice 删除指定的 loop 设备。
func removeLoopDevice(device string) error {
	args := []string{"-d", device}
	cmd := exec.Command(losetupPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, err := os.Stat(device); os.IsNotExist(err) {
			return nil
		}
		klog.V(2).Infof("Failed to remove loopback device: %s: %v %s", device, err, out)
		return fmt.Errorf("losetup -d %s failed: %v", device, err)
	}
	return nil
}

// getLoopDeviceFromSysfs 通过 "/sys/block/loop*/loop/backing_file" 从 sysfs
// 查找与给定文件对应的 loop 设备。
func getLoopDeviceFromSysfs(path string) (string, error) {
	// 解析符号链接得到真实路径。
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate path %s: %s", path, err)
	}

	devices, err := filepath.Glob("/sys/block/loop*")
	if err != nil {
		return "", fmt.Errorf("failed to list loop devices in sysfs: %s", err)
	}

	for _, device := range devices {
		backingFile := fmt.Sprintf("%s/loop/backing_file", device)

		// 该文件内容是对应 backing 文件的绝对路径。
		data, err := os.ReadFile(backingFile)
		if err != nil {
			continue
		}

		// 返回第一个匹配项。
		backingFilePath := cleanBackingFilePath(string(data))
		if backingFilePath == path || backingFilePath == realPath {
			return fmt.Sprintf("/dev/%s", filepath.Base(device)), nil
		}
	}

	return "", errors.New(ErrDeviceNotFound)
}

// cleanBackingFilePath 去除不属于 backing 文件路径的尾部子串。
func cleanBackingFilePath(path string) string {
	// 块设备被删除时，路径会带有 "(deleted)" 后缀。
	path = strings.TrimSpace(path)
	path = strings.TrimSuffix(path, "(deleted)")
	return strings.TrimSpace(path)
}

// FindGlobalMapPathUUIDFromPod 在 globalMapPath 下查找与 mapPath 符号链接对应的
// {pod uuid} bind mount，并返回带有 Pod UUID 的全局映射路径。
// （全局映射路径与 Pod 设备映射路径的细节见 pkg/volume/volume.go。）
// 例如 mapPath 符号链接：pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/{volumeName} -> /dev/sdX
//
//	globalMapPath/{pod uuid} 绑定挂载：plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid} -> /dev/sdX
func (v VolumePathHandler) FindGlobalMapPathUUIDFromPod(pluginDir, mapPath string, podUID types.UID) (string, error) {
	var globalMapPathUUID string
	// 在插件目录下查找以 Pod UUID 命名的符号链接。
	err := filepath.Walk(pluginDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if (fi.Mode()&os.ModeDevice == os.ModeDevice) && (fi.Name() == string(podUID)) {
			klog.V(5).Infof("FindGlobalMapPathFromPod: path %s, mapPath %s", path, mapPath)
			if res, err := compareBindMountAndSymlinks(path, mapPath); err == nil && res {
				globalMapPathUUID = path
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("FindGlobalMapPathUUIDFromPod failed: %v", err)
	}
	klog.V(5).Infof("FindGlobalMapPathFromPod: globalMapPathUUID %s", globalMapPathUUID)
	// 返回的路径包含全局映射路径 + {pod uuid}。
	return globalMapPathUUID, nil
}

// compareBindMountAndSymlinks 判断全局路径（bind mount）与 Pod 路径（符号链接）
// 是否指向同一个设备；检查过程出错时返回错误。
func compareBindMountAndSymlinks(global, pod string) (bool, error) {
	// 判断 bind mount 与符号链接是否指向同一设备，需要比较二者的
	// 设备号（major/minor）是否相同。

	// 获取全局路径的设备号。
	devNumGlobal, err := getDeviceMajorMinor(global)
	if err != nil {
		return false, fmt.Errorf("getDeviceMajorMinor failed for path %s: %v", global, err)
	}

	// 从 Pod 路径读取符号链接指向的设备。
	devPod, err := os.Readlink(pod)
	if err != nil {
		return false, fmt.Errorf("failed to readlink path %s: %v", pod, err)
	}
	// 获取 Pod 路径符号链接所指设备的设备号。
	devNumPod, err := getDeviceMajorMinor(devPod)
	if err != nil {
		return false, fmt.Errorf("getDeviceMajorMinor failed for path %s: %v", devPod, err)
	}
	klog.V(5).Infof("CompareBindMountAndSymlinks: devNumGlobal %s, devNumPod %s", devNumGlobal, devNumPod)

	// 设备号相同即认为指向同一设备。
	if devNumGlobal == devNumPod {
		return true, nil
	}
	return false, nil
}

// getDeviceMajorMinor 返回路径对应设备的设备号，格式为 major:minor（十六进制）。
// 例如：fc:10
func getDeviceMajorMinor(path string) (string, error) {
	var stat unix.Stat_t

	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("failed to stat path %s: %v", path, err)
	}

	devNumber := uint64(stat.Rdev)
	major := unix.Major(devNumber)
	minor := unix.Minor(devNumber)

	return fmt.Sprintf("%x:%x", major, minor), nil
}
