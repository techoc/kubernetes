/*
Copyright 2019 The Kubernetes Authors.

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

package subpath

import "os"

// Interface 定义所有 subpath 实现者必须实现的方法集合。
type Interface interface {
	// CleanSubPaths 清理 PrepareSafeSubpath 在指定 Pod 卷目录中创建的 bind mount。
	CleanSubPaths(poodDir string, volumeName string) error

	// PrepareSafeSubpath 完成准备 subPath 所需的全部工作，确保它 1) 位于给定 volumePath 内，
	// 2) 在本次调用之后不再变化。
	//
	// newHostPath 是准备好的 subPath 位置，运行容器时应使用它替代 hostName。
	// cleanupAction 是在容器运行或启动失败时要执行的清理动作。
	//
	// 启动带该 subpath 的容器后必须立即调用 cleanupAction；Pod 结束时则必须调用
	// Interface.CleanSubPaths。
	PrepareSafeSubpath(subPath Subpath) (newHostPath string, cleanupAction func(), err error)

	// SafeMakeDir 在给定 base 目录下创建子目录，并确保所创建的目录不会通过
	// 滥用符号链接逃逸出 base 目录。注意：该函数只保证目录创建在 base 之下的
	// 某处，仅此而已。例如目录已存在时，它可能通过符号链接指向 base 之外。
	// 当待创建目录位于用户可控的卷内时应使用该方法，防止用户借助符号链接
	// 逃出卷并在其他位置创建目录。
	SafeMakeDir(subdir string, base string, perm os.FileMode) error
}

// Subpath 定义 subpath 的属性。
type Subpath struct {
	// 该容器的 VolumeMount 索引。
	VolumeMountIndex int

	// 主机上 subpath 目录的完整路径。
	Path string

	// 卷名称，必须是合法的目录名。
	VolumeName string

	// 卷路径的完整路径。
	VolumePath string

	// Pod 目录路径，包含 Pod UID。
	PodDir string

	// 容器名称。
	ContainerName string
}

// 编译期检查：确保所有 subpath 接口实现者都满足契约。
var _ Interface = &subpath{}
var _ Interface = &FakeSubpath{}

// FakeSubpath 是用于测试的 subpath 实现。
type FakeSubpath struct{}

// PrepareSafeSubpath 是 PrepareSafeSubpath 的假实现，始终返回 newHostPath == subPath.Path。
func (fs *FakeSubpath) PrepareSafeSubpath(subPath Subpath) (newHostPath string, cleanupAction func(), err error) {
	return subPath.Path, nil, nil
}

// CleanSubPaths 是 CleanSubPaths 的假实现，为空操作。
func (fs *FakeSubpath) CleanSubPaths(podDir string, volumeName string) error {
	return nil
}

// SafeMakeDir 是 SafeMakeDir 的假实现，为空操作。
func (fs *FakeSubpath) SafeMakeDir(pathname string, base string, perm os.FileMode) error {
	return nil
}
