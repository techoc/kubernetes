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

package util

import (
	"path/filepath"

	libcontainerutils "k8s.io/kubernetes/third_party/forked/libcontainer/utils"

	libcontainercgroups "github.com/opencontainers/cgroups"
)

const (
	// CgroupRoot 是 cgroup 的挂载根路径。
	CgroupRoot = "/sys/fs/cgroup"
)

// GetPids 返回指定 cgroup 中的 PID。
// 逻辑派生自 opencontainers/runc/libcontainer/cgroup/fs.Manager.GetPids()。
func GetPids(cgroupPath string) ([]int, error) {
	dir := ""

	// cgroup v2 使用统一层级，路径直接相对 CgroupRoot。
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		path, err := filepath.Rel("/", cgroupPath)
		if err != nil {
			return nil, err
		}
		dir = filepath.Join(CgroupRoot, path)
	} else {
		var err error
		// cgroup v1 需要按 devices 子系统定位实际挂载路径。
		dir, err = getCgroupV1Path(cgroupPath)
		if err != nil {
			return nil, err
		}
	}
	return libcontainercgroups.GetPids(dir)
}

// getCgroupV1Path 返回指定 cgroup 在 "devices" 子系统下的文件路径。
// cgroupPath 是 cgroup 层级中的路径。
func getCgroupV1Path(cgroupPath string) (string, error) {
	cgroupPath = libcontainerutils.CleanPath(cgroupPath)

	mnt, root, err := libcontainercgroups.FindCgroupMountpointAndRoot(cgroupPath, "devices")
	// 未挂载该子系统时无需继续构造路径。
	if err != nil {
		return "", err
	}

	// 若 cgroup 名称/路径是绝对路径，则不再相对 init 进程的 cgroup 解析。
	// 绝对路径直接拼接，不再相对 init 进程的 cgroup。
	if filepath.IsAbs(cgroupPath) {
		// 子系统有时会一起挂载，例如 'cpu,cpuacct'。
		return filepath.Join(root, mnt, cgroupPath), nil
	}

	parentPath, err := getCgroupV1ParentPath(mnt, root)
	if err != nil {
		return "", err
	}

	return filepath.Join(parentPath, cgroupPath), nil
}

// getCgroupV1ParentPath 返回当前 cgroup 的父路径，用于解析相对 cgroup 路径。
func getCgroupV1ParentPath(mountpoint, root string) (string, error) {
	// 使用 GetOwnCgroup 而不是 GetInitCgroupDir：
	// 创建进程可能位于容器内并与宿主共享 PID 命名空间，
	// /proc/1/cgroup 可能指向完全不同的 cgroup 世界。
	initPath, err := libcontainercgroups.GetOwnCgroup("devices")
	if err != nil {
		return "", err
	}
	// 嵌套容器场景需要这样处理：/proc/self/cgroup 中看到的是宿主路径，
	// 这些路径在容器内并不存在。
	relDir, err := filepath.Rel(root, initPath)
	if err != nil {
		return "", err
	}
	return filepath.Join(mountpoint, relDir), nil
}
