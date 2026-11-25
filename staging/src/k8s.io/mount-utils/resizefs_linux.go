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

// ResizeFs 通过外部命令扩容文件系统。
// exec 可注入假实现，便于测试命令选择、参数和失败路径。
type ResizeFs struct {
	exec utilexec.Interface
}

// NewResizeFs 创建新的扩容器实例。
// 生产代码传入真实执行器，测试中传入假实现。
func NewResizeFs(exec utilexec.Interface) *ResizeFs {
	return &ResizeFs{exec: exec}
}

// Resize 执行文件系统扩容，成功返回 true；未格式化或无需扩容返回 false。
// 按探测到的文件系统类型选择工具；未格式化时不扩容，因为 mkfs.* 默认会使用整盘。
func (resizefs *ResizeFs) Resize(devicePath string, deviceMountPath string) (bool, error) {
	format, err := getDiskFormat(resizefs.exec, devicePath)
	if err != nil {
		formatErr := fmt.Errorf("ResizeFS.Resize - error checking format for device %s: %v", devicePath, err)
		return false, formatErr
	}

	// 磁盘尚未格式化，无需扩容；mkfs.* 默认会使用整盘。
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

// extResize 对 ext3/ext4 扩容，成功返回 true；resize2fs 接受设备路径而非挂载点。
func (resizefs *ResizeFs) extResize(devicePath string) (bool, error) {
	output, err := resizefs.exec.Command("resize2fs", devicePath).CombinedOutput()
	if err == nil {
		klog.V(2).Infof("Device %s resized successfully", devicePath)
		return true, nil
	}

	resizeError := fmt.Errorf("resize of device %s failed: %v. resize2fs output: %s", devicePath, err, string(output))
	return false, resizeError
}

// xfsResize 对 XFS 扩容，成功返回 true；xfs_growfs 接受挂载点，不能传块设备。
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

// btrfsResize 对 Btrfs 扩容，成功返回 true；resize max 让文件系统占满可用空间。
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

// NeedResize 判断文件系统是否需要扩容；只读、未格式化或大小已满足时返回 false。
// ext3/ext4/xfs 交由扩容工具最终判断；btrfs 比较设备与文件系统大小，并留出一个块的余量。
func (resizefs *ResizeFs) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	// 块设备为只读时不执行扩容。
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

	// 磁盘尚未格式化，无需扩容；mkfs.* 默认会使用整盘。
	if format == "" {
		return false, nil
	}

	switch format {
	case "ext3", "ext4", "xfs":
		// Linux 文件系统维护者建议让 resize2fs/xfs_growfs 自行判断，这里不重复检查。
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
			// 设备至多比文件系统多一个块时不扩容，避免无实际收益的调用。
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

// getBtrfsSize 从 superblock dump 提取块大小与总字节数，并返回 (块大小, 总字节数, 错误)。
// 缺任一项都无法判断扩容空间，因此按零值处理为错误。
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

// parseBtrfsInfoOutput 从 btrfs 命令输出中提取指定键的数值，返回 (块大小, 总字节数, 错误)。
// 逐行按空白切分，忽略格式不符的行以兼容不同 btrfs 版本输出。
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

// getDeviceRO 读取 blockdev 的只读位，返回 (是否只读, 错误)。
// 只读设备扩容会失败，需提前拦截；输出既不是 0 也不是 1 时视为探测失败。
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
