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

package hostutil

import (
	"errors"
	"os"
	"sync"

	"k8s.io/mount-utils"
)

// FakeHostUtil 是供测试使用的 HostUtils 假实现。
type FakeHostUtil struct {
	MountPoints []mount.MountPoint
	Filesystem  map[string]FileType

	mutex sync.Mutex
}

// NewFakeHostUtil 返回一个实现 HostUtils 接口的测试用结构体。
// TODO: 目前没有调用方初始化 MountPoints。检查是否仍有调用方使用，以及该字段是否仍需保留。
func NewFakeHostUtil(fs map[string]FileType) *FakeHostUtil {
	return &FakeHostUtil{
		Filesystem: fs,
	}
}

// 编译期检查，确保 FakeHostUtil 实现了 HostUtils 接口。
var _ HostUtils = &FakeHostUtil{}

// DeviceOpened 通过检查内存挂载点表中是否列出 pathname，判断对应块设备是否已被占用。
func (hu *FakeHostUtil) DeviceOpened(pathname string) (bool, error) {
	hu.mutex.Lock()
	defer hu.mutex.Unlock()

	for _, mp := range hu.MountPoints {
		if mp.Device == pathname {
			return true, nil
		}
	}
	return false, nil
}

// PathIsDevice 在测试中始终返回 true。
func (hu *FakeHostUtil) PathIsDevice(pathname string) (bool, error) {
	return true, nil
}

// GetDeviceNameFromMount 根据挂载点查找卷 ID。
func (hu *FakeHostUtil) GetDeviceNameFromMount(mounter mount.Interface, mountPath, pluginMountDir string) (string, error) {
	return getDeviceNameFromMount(mounter, mountPath, pluginMountDir)
}

// MakeRShared 检查 path 是否共享，必要时以 rshared 方式 bind mount；测试中为空操作。
func (hu *FakeHostUtil) MakeRShared(path string) error {
	return nil
}

// GetFileType 检查文件/目录/socket/块设备/字符设备；未指定的路径默认返回 Directory。
func (hu *FakeHostUtil) GetFileType(pathname string) (FileType, error) {
	if t, ok := hu.Filesystem[pathname]; ok {
		return t, nil
	}
	return FileType("Directory"), nil
}

// PathExists 检查 pathname 是否存在。
func (hu *FakeHostUtil) PathExists(pathname string) (bool, error) {
	if _, ok := hu.Filesystem[pathname]; ok {
		return true, nil
	}
	return false, nil
}

// EvalHostSymlinks 返回解析符号链接后的路径；测试中直接原样返回。
func (hu *FakeHostUtil) EvalHostSymlinks(pathname string) (string, error) {
	return pathname, nil
}

// GetOwner 返回给定路径的用户和组 ID；测试中未实现，返回错误。
func (hu *FakeHostUtil) GetOwner(pathname string) (int64, int64, error) {
	return -1, -1, errors.New("GetOwner not implemented")
}

// GetSELinuxSupport 检查 pathname 所在挂载是否支持 SELinux；测试中未实现，始终返回 false。
func (hu *FakeHostUtil) GetSELinuxSupport(pathname string) (bool, error) {
	return false, nil
}

// GetMode 返回 pathname 的权限；测试中未实现，返回错误。
func (hu *FakeHostUtil) GetMode(pathname string) (os.FileMode, error) {
	return 0, errors.New("not implemented")
}

// GetSELinuxMountContext 返回给定挂载点上 -o context=XYZ 挂载选项的值。
func (hu *FakeHostUtil) GetSELinuxMountContext(pathname string) (string, error) {
	// 假装操作系统不支持 SELinux。
	return "", nil
}
