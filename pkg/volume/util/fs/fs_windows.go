//go:build windows

/*
Copyright 2014 The Kubernetes Authors.

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

package fs

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows 下查询磁盘空间所需的 kernel32 动态链接库入口。
var (
	modkernel32            = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpaceEx = modkernel32.NewProc("GetDiskFreeSpaceExW")
)

// UsageInfo 记录路径的磁盘用量，Inodes 在 Windows 上不可用（恒为 0）。
type UsageInfo struct {
	Bytes  int64
	Inodes int64
}

// Info 返回 path 所在文件系统的信息：
// 可用字节数、总容量、已用字节数、总 inode 数、空闲 inode 数、已用 inode 数及错误。
// Windows 不提供 inode 统计，后三项恒为 0。
func Info(path string) (int64, int64, int64, int64, int64, int64, error) {
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes int64
	var err error

	// Linux 的等价调用可作用于文件，但 Windows 的该 syscall 对文件会失败，
	// 返回错误码 "The directory name is invalid"（#99173）。
	// https://docs.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-
	// 因此统一取父目录路径，覆盖所有使用场景。
	path = filepath.Dir(path)
	ret, _, err := syscall.Syscall6(
		procGetDiskFreeSpaceEx.Addr(),
		4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
		0,
		0,
	)
	if ret == 0 {
		return 0, 0, 0, 0, 0, 0, err
	}

	return freeBytesAvailable, totalNumberOfBytes, totalNumberOfBytes - freeBytesAvailable, 0, 0, 0, nil
}

// DiskUsage 计算指定路径（含子目录）占用的磁盘字节数。
func DiskUsage(path string) (UsageInfo, error) {
	var usage UsageInfo
	info, err := os.Lstat(path)
	if err != nil {
		return usage, err
	}

	usage.Bytes, err = diskUsage(path, info)
	return usage, err
}

// diskUsage 递归累加路径占用字节数；符号链接不追踪，避免重复计数或环。
func diskUsage(currPath string, info os.FileInfo) (int64, error) {
	var size int64

	if info.Mode()&os.ModeSymlink != 0 {
		return size, nil
	}

	// go1.23 行为变更：Windows 上不规则文件的 Size() 不再可靠，跳过此类文件。
	// https://github.com/golang/go/issues/63703#issuecomment-2535941458
	if info.Mode()&os.ModeIrregular != 0 {
		return size, nil
	}

	size += info.Size()

	if !info.IsDir() {
		return size, nil
	}

	dir, err := os.Open(currPath)
	if err != nil {
		return size, err
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		return size, err
	}

	for _, file := range files {
		if file.IsDir() {
			s, err := diskUsage(filepath.Join(currPath, file.Name()), file)
			if err != nil {
				return size, err
			}
			size += s
		} else {
			size += file.Size()
		}
	}
	return size, nil
}
