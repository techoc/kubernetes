//go:build !linux

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

	utilexec "k8s.io/utils/exec"
)

// ResizeFs Provides support for resizing file systems
// 本文件为不支持扩容的平台提供桩实现，保证接口在各平台都能编译通过。
type ResizeFs struct {
	exec utilexec.Interface
}

// NewResizeFs returns new instance of resizer
// 保留 exec 参数以维持跨平台一致的构造函数签名，尽管此实现用不到它。
func NewResizeFs(exec utilexec.Interface) *ResizeFs {
	return &ResizeFs{exec: exec}
}

// Resize perform resize of file system
// 明确报错而不是静默成功，让调用方立即知道该平台不支持扩容。
func (resizefs *ResizeFs) Resize(devicePath string, deviceMountPath string) (bool, error) {
	return false, fmt.Errorf("Resize is not supported for this build")
}

// NeedResize check whether mounted volume needs resize
// 与 Resize 一致：不做猜测，直接返回不支持的错误。
func (resizefs *ResizeFs) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	return false, fmt.Errorf("NeedResize is not supported for this build")
}
