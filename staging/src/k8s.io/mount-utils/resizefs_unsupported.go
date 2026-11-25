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

// ResizeFs 在非 Linux 平台提供文件系统扩容的桩实现。
// 本文件保证接口在各平台都能编译通过，但不执行实际扩容。
type ResizeFs struct {
	exec utilexec.Interface
}

// NewResizeFs 创建扩容器实例。
// 保留 exec 参数以维持跨平台一致的构造函数签名，尽管此实现用不到它。
func NewResizeFs(exec utilexec.Interface) *ResizeFs {
	return &ResizeFs{exec: exec}
}

// Resize 尝试扩容文件系统。
// 非 Linux 平台明确报错而不是静默成功，让调用方立即知道该平台不支持扩容。
func (resizefs *ResizeFs) Resize(devicePath string, deviceMountPath string) (bool, error) {
	return false, fmt.Errorf("Resize is not supported for this build")
}

// NeedResize 判断已挂载卷是否需要扩容。
// 与 Resize 一致：不做猜测，直接返回不支持的错误。
func (resizefs *ResizeFs) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	return false, fmt.Errorf("NeedResize is not supported for this build")
}
