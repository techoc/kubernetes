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

package hostutil

import (
	"fmt"
	"os"
)

// FileType 枚举已知的文件类型集合。
type FileType string

const (
	// FileTypeBlockDev 表示块设备类型。
	FileTypeBlockDev FileType = "BlockDevice"
	// FileTypeCharDev 表示字符设备类型。
	FileTypeCharDev FileType = "CharDevice"
	// FileTypeDirectory 表示目录类型。
	FileTypeDirectory FileType = "Directory"
	// FileTypeFile 表示普通文件类型。
	FileTypeFile FileType = "File"
	// FileTypeSocket 表示套接字类型。
	FileTypeSocket FileType = "Socket"
	// FileTypeUnknown 表示未知的文件类型。
	FileTypeUnknown FileType = ""
)

var (
	errUnknownFileType = fmt.Errorf("only recognise file, directory, socket, block device and character device")
)

// HostUtils 定义与宿主机路径交互的一组方法。
type HostUtils interface {
	// DeviceOpened 判断设备（如 /dev/sdc）是否在系统别处仍被使用，即是否仍处于挂载状态。
	DeviceOpened(pathname string) (bool, error)
	// PathIsDevice 判断给定路径是否为设备。
	PathIsDevice(pathname string) (bool, error)
	// MakeRShared 检查给定路径所在挂载点是否具有 rshared 传播属性，若没有则以 rshared 方式 bind mount 该路径。
	MakeRShared(path string) error
	// GetFileType 判断路径是文件、目录、套接字还是块/字符设备。
	GetFileType(pathname string) (FileType, error)
	// PathExists 判断给定路径是否已存在。
	// 除“文件不存在”以外的错误都会作为 error 返回。
	PathExists(pathname string) (bool, error)
	// EvalHostSymlinks 返回解析符号链接之后的路径。
	EvalHostSymlinks(pathname string) (string, error)
	// GetOwner 返回给定路径的用户和组的整数 ID。
	GetOwner(pathname string) (int64, int64, error)
	// GetSELinuxSupport 判断给定路径所在挂载点是否支持 SELinux。
	GetSELinuxSupport(pathname string) (bool, error)
	// GetMode 返回路径的权限位。
	GetMode(pathname string) (os.FileMode, error)
	// GetSELinuxMountContext 返回给定挂载点上 -o context=XYZ 挂载选项的值。
	GetSELinuxMountContext(pathname string) (string, error)
}

// 编译期检查，确保 HostUtil 实现满足 HostUtils 接口。
var _ HostUtils = &HostUtil{}

// getFileType 检查路径是文件、目录、套接字还是块/字符设备。
func getFileType(pathname string) (FileType, error) {
	var pathType FileType
	info, err := os.Stat(pathname)
	if os.IsNotExist(err) {
		return pathType, fmt.Errorf("path %q does not exist", pathname)
	}
	// 路径存在但 os.Stat 仍报错，直接向上传递该错误。
	if err != nil {
		return pathType, err
	}

	// 位运算判断是否设置了目标 mode 位，设备类型可能叠加多个标志位。
	isSpecificMode := func(mode, targetMode os.FileMode) bool {
		return mode&targetMode == targetMode
	}

	mode := info.Mode()
	if mode.IsDir() {
		return FileTypeDirectory, nil
	} else if mode.IsRegular() {
		return FileTypeFile, nil
	} else if isSpecificMode(mode, os.ModeSocket) {
		return FileTypeSocket, nil
	} else if isSpecificMode(mode, os.ModeDevice) {
		if isSpecificMode(mode, os.ModeCharDevice) {
			return FileTypeCharDev, nil
		}
		return FileTypeBlockDev, nil
	}

	return pathType, errUnknownFileType
}
