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

// Package procfs 提供通过 /proc 查询容器进程信息的接口。
package procfs

// ProcFSInterface 抽象了从进程 ID 获取容器名称的能力，
// 便于在不同平台上提供各自的实现。
type ProcFSInterface interface {
	// GetFullContainerName 根据容器的根进程 ID 获取容器名称。
	GetFullContainerName(pid int) (string, error)
}
