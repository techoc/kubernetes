//go:build !linux

/*
Copyright 2016 The Kubernetes Authors.

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

package procfs

import (
	"fmt"
	"syscall"
)

// ProcFS 是非 Linux 平台上的空实现，仅用于满足 ProcFSInterface 的编译需要。
type ProcFS struct{}

// NewProcFS 返回非 Linux 平台上的 ProcFSInterface 实现。
func NewProcFS() ProcFSInterface {
	return &ProcFS{}
}

// GetFullContainerName 根据容器的根进程 ID 获取容器名称。
// 非 Linux 构建中不支持此操作，始终返回错误。
func (pfs *ProcFS) GetFullContainerName(pid int) (string, error) {
	return "", fmt.Errorf("GetFullContainerName is unsupported in this build")
}

// PKill 用正则表达式查找进程并向每个匹配的进程发送指定信号。
// 非 Linux 构建中不支持此操作，始终返回错误。
func PKill(name string, sig syscall.Signal) error {
	return fmt.Errorf("PKill is unsupported in this build")
}

// PidOf 按指定名称（精确匹配）查找进程并返回其 PID。
// 非 Linux 构建中不支持此操作，始终返回错误。
func PidOf(name string) ([]int, error) {
	return []int{}, fmt.Errorf("PidOf is unsupported in this build")
}
