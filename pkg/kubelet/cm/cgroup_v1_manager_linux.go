/*
Copyright 2024 The Kubernetes Authors.

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

package cm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	libcontainercgroups "github.com/opencontainers/cgroups"
	"github.com/opencontainers/cgroups/fscommon"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// cgroupv1MemLimitFile 是 cgroup v1 中记录内存上限的控制文件。
const cgroupv1MemLimitFile string = "memory.limit_in_bytes"

// cgroupV1impl 实现 cgroup v1 的 CgroupManager 接口。
// 它是无状态对象，可创建、更新或删除任意数量的 cgroup，
// 底层依赖 runc/libcontainer 的 cgroup 管理器。
type cgroupV1impl struct {
	cgroupCommon
}

// NewCgroupV1Manager 基于 cgroup v1 子系统信息构造 CgroupManager。
func NewCgroupV1Manager(logger klog.Logger, cs *CgroupSubsystems, cgroupDriver string) CgroupManager {
	return &cgroupV1impl{
		cgroupCommon: newCgroupCommon(logger, cs, cgroupDriver),
	}
}

// Version 返回宿主机上 cgroup 实现的版本号。
func (c *cgroupV1impl) Version() int {
	return 1
}

// Validate 检查指定 cgroup 在所有关注的子系统下是否都已存在。
func (c *cgroupV1impl) Validate(name CgroupName) error {
	// 获取该 cgroup 在系统上的所有 cgroup 路径。
	cgroupPaths := c.buildCgroupPaths(name)

	// runc 不认识的额外控制组会干扰 kubelet 的存在性检查。
	// 理想情况下 runc 应提供按已知控制组集合限定范围的 Exists() 逻辑，
	// 相关讨论见 https://github.com/opencontainers/runc/issues/1440，
	// 该问题解决后即可删除这段代码。
	// 只检查 runc 认识的控制器，避免未知挂载点导致误判。
	allowlistControllers := sets.New[string]("cpu", "cpuacct", "cpuset", "memory", "systemd", "pids")

	// hugetlb 仅在宿主挂载了对应控制器时才纳入检查。
	if _, ok := c.subsystems.MountPoints["hugetlb"]; ok {
		allowlistControllers.Insert("hugetlb")
	}
	var missingPaths []string
	// 任一关注子系统的路径缺失，就认为该 cgroup 不存在。
	for controller, path := range cgroupPaths {
		// 忽略不关心的挂载点。
		if !allowlistControllers.Has(controller) {
			continue
		}
		if !libcontainercgroups.PathExists(path) {
			missingPaths = append(missingPaths, path)
		}
	}

	if len(missingPaths) > 0 {
		return fmt.Errorf("cgroup %q has some missing paths: %v", name, strings.Join(missingPaths, ", "))
	}

	return nil
}

// Exists 判断所有子系统 cgroup 是否都已存在。
func (c *cgroupV1impl) Exists(name CgroupName) bool {
	return c.Validate(name) == nil
}

// MemoryUsage 从 cgroupfs 读取指定 cgroup 的当前内存用量。
func (c *cgroupV1impl) MemoryUsage(name CgroupName) (int64, error) {
	var path, file string
	// 内存用量只能从 memory 控制器的挂载点读取。
	mp, ok := c.subsystems.MountPoints["memory"]
	if !ok { // 不应发生。
		return -1, errors.New("no cgroup v1 mountpoint for memory controller found")
	}
	path = mp + "/" + c.Name(name)
	file = "memory.usage_in_bytes"
	val, err := fscommon.GetCgroupParamUint(path, file)
	return int64(val), err
}

// GetCgroupConfig 读取指定资源类型在 cgroup 上生效的配置值。
func (c *cgroupV1impl) GetCgroupConfig(name CgroupName, resource v1.ResourceName) (*ResourceConfig, error) {
	cgroupPaths := c.buildCgroupPaths(name)
	cgroupResourcePath, found := cgroupPaths[string(resource)]
	if !found {
		return nil, fmt.Errorf("failed to build %v cgroup fs path for cgroup %v", resource, name)
	}
	// 目前只支持读取 CPU 与内存两类资源。
	switch resource {
	case v1.ResourceCPU:
		return c.getCgroupCPUConfig(cgroupResourcePath)
	case v1.ResourceMemory:
		return c.getCgroupMemoryConfig(cgroupResourcePath)
	}
	return nil, fmt.Errorf("unsupported resource %v for cgroup %v", resource, name)
}

// getCgroupCPUConfig 读取 cpu.cfs_quota_us、cpu.cfs_period_us 和 cpu.shares。
func (c *cgroupV1impl) getCgroupCPUConfig(cgroupPath string) (*ResourceConfig, error) {
	cpuQuotaStr, errQ := fscommon.GetCgroupParamString(cgroupPath, "cpu.cfs_quota_us")
	if errQ != nil {
		return nil, fmt.Errorf("failed to read CPU quota for cgroup %v: %w", cgroupPath, errQ)
	}
	// quota 可能为 -1（表示不限制），因此必须按有符号整数解析。
	cpuQuota, errInt := strconv.ParseInt(cpuQuotaStr, 10, 64)
	if errInt != nil {
		return nil, fmt.Errorf("failed to convert CPU quota as integer for cgroup %v: %w", cgroupPath, errInt)
	}
	cpuPeriod, errP := fscommon.GetCgroupParamUint(cgroupPath, "cpu.cfs_period_us")
	if errP != nil {
		return nil, fmt.Errorf("failed to read CPU period for cgroup %v: %w", cgroupPath, errP)
	}
	cpuShares, errS := fscommon.GetCgroupParamUint(cgroupPath, "cpu.shares")
	if errS != nil {
		return nil, fmt.Errorf("failed to read CPU shares for cgroup %v: %w", cgroupPath, errS)
	}
	return &ResourceConfig{CPUShares: &cpuShares, CPUQuota: &cpuQuota, CPUPeriod: &cpuPeriod}, nil
}

// getCgroupMemoryConfig 复用通用逻辑读取 cgroup v1 的内存上限。
func (c *cgroupV1impl) getCgroupMemoryConfig(cgroupPath string) (*ResourceConfig, error) {
	return readCgroupMemoryConfig(cgroupPath, cgroupv1MemLimitFile)
}
