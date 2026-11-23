//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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
	"strconv"
	"strings"

	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

const (
	blockDev = "blockdev"
)

// ResizeFs Provides support for resizing file systems
// 通过外部命令扩容，便于在测试中替换执行器；exec 可注入假实现用于测试。
type ResizeFs struct {
	exec utilexec.Interface
}

// NewResizeFs returns new instance of resizer
// 生产代码传入真实执行器，测试中传入 fake。
func NewResizeFs(exec utilexec.Interface) *ResizeFs {
	return &ResizeFs{exec: exec}
}

// Resize perform resize of file system
// 按探测到的文件系统类型选择工具；未格式化时不扩容，交给 mkfs 使用整盘。
func (resizefs *ResizeFs) Resize(devicePath string, deviceMountPath string) (bool, error) {
	format, err := getDiskFormat(resizefs.exec, devicePath)
	if err != nil {
		formatErr := fmt.Errorf("ResizeFS.Resize - error checking format for device %s: %v", devicePath, err)
		return false, formatErr
	}

	// If disk has no format, there is no need to resize the disk because mkfs.*
	// by default will use whole disk anyways.
	if format == "" {
		return false, nil
	}

	klog.V(3).Infof("ResizeFS.Resize - Expanding mounted volume %s", devicePath)
	switch format {
	case "ext3", "ext4":
		return resizefs.extResize(devicePath)
	case "xfs":
		return resizefs.xfsResize(deviceMountPath)
	case "btrfs":
		return resizefs.btrfsResize(deviceMountPath)
	}
	return false, fmt.Errorf("ResizeFS.Resize - resize of format %s is not supported for device %s mounted at %s", format, devicePath, deviceMountPath)
}

// extResize 对 ext3/ext4 扩容；resize2fs 接受设备路径而非挂载点。
func (resizefs *ResizeFs) extResize(devicePath string) (bool, error) {
	output, err := resizefs.exec.Command("resize2fs", devicePath).CombinedOutput()
	if err == nil {
		klog.V(2).Infof("Device %s resized successfully", devicePath)
		return true, nil
	}

	resizeError := fmt.Errorf("resize of device %s failed: %v. resize2fs output: %s", devicePath, err, string(output))
	return false, resizeError
}

// xfsResize 接受挂载点；xfs_growfs 依赖已挂载的文件系统，不能传块设备。
func (resizefs *ResizeFs) xfsResize(deviceMountPath string) (bool, error) {
	args := []string{"-d", deviceMountPath}
	output, err := resizefs.exec.Command("xfs_growfs", args...).CombinedOutput()

	if err == nil {
		klog.V(2).Infof("Device %s resized successfully", deviceMountPath)
		return true, nil
	}

	resizeError := fmt.Errorf("resize of device %s failed: %v. xfs_growfs output: %s", deviceMountPath, err, string(output))
	return false, resizeError
}

// btrfsResize 同样针对挂载点操作，resize max 让文件系统占满可用空间。
func (resizefs *ResizeFs) btrfsResize(deviceMountPath string) (bool, error) {
	args := []string{"filesystem", "resize", "max", deviceMountPath}
	output, err := resizefs.exec.Command("btrfs", args...).CombinedOutput()

	if err == nil {
		klog.V(2).Infof("Device %s resized successfully", deviceMountPath)
		return true, nil
	}

	resizeError := fmt.Errorf("resize of device %s failed: %v. btrfs output: %s", deviceMountPath, err, string(output))
	return false, resizeError
}

// NeedResize 先排除只读和未格式化的设备，再按文件系统类型判断是否需要扩容。
func (resizefs *ResizeFs) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	// Do nothing if device is mounted as readonly
	readonly, err := resizefs.getDeviceRO(devicePath)
	if err != nil {
		return false, err
	}

	if readonly {
		klog.V(3).Infof("ResizeFs.needResize - no resize possible since filesystem %s is readonly", devicePath)
		return false, nil
	}

	format, err := getDiskFormat(resizefs.exec, devicePath)
	if err != nil {
		formatErr := fmt.Errorf("ResizeFS.Resize - error checking format for device %s: %v", devicePath, err)
		return false, formatErr
	}

	// If disk has no format, there is no need to resize the disk because mkfs.*
	// by default will use whole disk anyways.
	if format == "" {
		return false, nil
	}

	switch format {
	case "ext3", "ext4", "xfs":
		// For ext3/ext4/xfs, recommendation received from linux filesystem folks is to let
		// resize2fs/xfs_growfs do the check for us. So we will not do any check here.
		return true, nil
	case "btrfs":
		deviceSize, err := resizefs.getDeviceSize(devicePath)
		if err != nil {
			return false, err
		}
		blockSize, fsSize, err := resizefs.getBtrfsSize(devicePath)
		klog.V(5).Infof("Btrfs size: filesystem size=%d, block size=%d, err=%v", fsSize, blockSize, err)
		if err != nil {
			return false, err
		}
		if deviceSize <= fsSize+blockSize {
			return false, nil
		}
		return true, nil
	default:
		return false, fmt.Errorf("could not parse fs info of given filesystem format: %s. Supported fs types are: xfs, ext3, ext4", format)
	}
}

// getDeviceSize 读取块设备字节数；命令输出非数字时视为探测失败。
func (resizefs *ResizeFs) getDeviceSize(devicePath string) (uint64, error) {
	output, err := resizefs.exec.Command(blockDev, "--getsize64", devicePath).CombinedOutput()
	outStr := strings.TrimSpace(string(output))
	if err != nil {
		return 0, fmt.Errorf("failed to read size of device %s: %s: %s", devicePath, err, outStr)
	}
	size, err := strconv.ParseUint(outStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size of device %s %s: %s", devicePath, outStr, err)
	}
	return size, nil
}

// getBtrfsSize 从 superblock dump 提取块大小与总字节数；缺任一项都无法判断扩容空间。
func (resizefs *ResizeFs) getBtrfsSize(devicePath string) (uint64, uint64, error) {
	output, err := resizefs.exec.Command("btrfs", "inspect-internal", "dump-super", "-f", devicePath).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read size of filesystem on %s: %s: %s", devicePath, err, string(output))
	}

	blockSize, totalBytes, _ := resizefs.parseBtrfsInfoOutput(string(output), "sectorsize", "total_bytes")

	if blockSize == 0 {
		return 0, 0, fmt.Errorf("could not find block size of device %s", devicePath)
	}
	if totalBytes == 0 {
		return 0, 0, fmt.Errorf("could not find total size of device %s", devicePath)
	}
	return blockSize, totalBytes, nil
}

// parseBtrfsInfoOutput 逐行按空白切分，忽略格式不符的行以兼容不同 btrfs 版本输出。
func (resizefs *ResizeFs) parseBtrfsInfoOutput(cmdOutput string, blockSizeKey string, totalBytesKey string) (uint64, uint64, error) {
	lines := strings.Split(cmdOutput, "\n")
	var blockSize, blockCount uint64
	var err error

	for _, line := range lines {
		tokens := strings.Fields(line)
		if len(tokens) != 2 {
			continue
		}
		key, value := strings.ToLower(strings.TrimSpace(tokens[0])), strings.ToLower(strings.TrimSpace(tokens[1]))

		if key == blockSizeKey {
			blockSize, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to parse block size %s: %s", value, err)
			}
		}
		if key == totalBytesKey {
			blockCount, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to parse total size %s: %s", value, err)
			}
		}
	}
	return blockSize, blockCount, err
}

// getDeviceRO 读取 blockdev 的只读位；只读设备扩容会失败，需提前拦截。
func (resizefs *ResizeFs) getDeviceRO(devicePath string) (bool, error) {
	output, err := resizefs.exec.Command(blockDev, "--getro", devicePath).CombinedOutput()
	outStr := strings.TrimSpace(string(output))
	if err != nil {
		return false, fmt.Errorf("failed to get readonly bit from device %s: %w: %s", devicePath, err, outStr)
	}
	switch outStr {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("failed readonly device check. Expected 1 or 0, got '%s'", outStr)
	}
}
