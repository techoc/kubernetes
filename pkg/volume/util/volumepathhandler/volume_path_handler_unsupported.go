//go:build !linux

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
	"fmt"

	"k8s.io/apimachinery/pkg/types"
)

// AttachFileDevice 接收普通文件路径，将其作为已附加的块设备提供。
// 非 Linux 构建下不支持，始终返回错误。
func (v VolumePathHandler) AttachFileDevice(path string) (string, error) {
	return "", fmt.Errorf("AttachFileDevice not supported for this build.")
}

// DetachFileDevice 接收已附加块设备的路径，将其从块设备分离。
// 非 Linux 构建下不支持，始终返回错误。
func (v VolumePathHandler) DetachFileDevice(path string) error {
	return fmt.Errorf("DetachFileDevice not supported for this build.")
}

// GetLoopDevice 返回与给定路径关联的回环设备的完整路径。
// 非 Linux 构建下不支持，始终返回错误。
func (v VolumePathHandler) GetLoopDevice(path string) (string, error) {
	return "", fmt.Errorf("GetLoopDevice not supported for this build.")
}

// FindGlobalMapPathUUIDFromPod 在 globalMapPath 下查找与 map path 符号链接对应的
// {pod uuid} 绑定挂载，返回带 pod uuid 的全局映射路径。
// 非 Linux 构建下不支持，始终返回错误。
func (v VolumePathHandler) FindGlobalMapPathUUIDFromPod(pluginDir, mapPath string, podUID types.UID) (string, error) {
	return "", fmt.Errorf("FindGlobalMapPathUUIDFromPod not supported for this build.")
}
