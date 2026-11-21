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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/opencontainers/cgroups/fscommon"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	cmutil "k8s.io/kubernetes/pkg/kubelet/cm/util"
)

// cgroup v2 关键文件的文件名。
const (
	cgroupv2MemLimitFile  = "memory.max"
	cgroupv2CpuMaxFile    = "cpu.max"
	cgroupv2CpuWeightFile = "cpu.weight"
)

// cgroupV2impl 实现 cgroup v2 的 CgroupManager 接口。
// 它是无状态对象，可用于更新、创建或删除任意数量的 cgroup；
// 依赖 runc/libcontainer 的 cgroup manager。
type cgroupV2impl struct {
	cgroupCommon
}

// NewCgroupV2Manager 创建 cgroup v2 管理器。
func NewCgroupV2Manager(logger klog.Logger, cs *CgroupSubsystems, cgroupDriver string) CgroupManager {
	return &cgroupV2impl{
		cgroupCommon: newCgroupCommon(logger, cs, cgroupDriver),
	}
}

// Version 返回宿主机上 cgroup 实现的版本。
func (c *cgroupV2impl) Version() int {
	return 2
}

// Validate 检查所有子系统 cgroup 是否有效。
func (c *cgroupV2impl) Validate(name CgroupName) error {
	cgroupPath := c.buildCgroupUnifiedPath(name)
	// kubelet 需要的控制器必须在 cgroup.controllers 中全部启用。
	neededControllers := getSupportedUnifiedControllers()
	enabledControllers, err := readUnifiedControllers(cgroupPath)
	if err != nil {
		return fmt.Errorf("could not read controllers for cgroup %q: %w", name, err)
	}
	// 缺少任一控制器都视为无效。
	difference := neededControllers.Difference(enabledControllers)
	if difference.Len() > 0 {
		return fmt.Errorf("cgroup %q has some missing controllers: %v", name, strings.Join(sets.List(difference), ", "))
	}
	return nil
}

// Exists 检查所有子系统 cgroup 是否已存在。
func (c *cgroupV2impl) Exists(name CgroupName) bool {
	return c.Validate(name) == nil
}

// MemoryUsage 返回指定 cgroup 的当前内存使用量（读取 cgroupfs）。
func (c *cgroupV2impl) MemoryUsage(name CgroupName) (int64, error) {
	var path, file string
	path = c.buildCgroupUnifiedPath(name)
	// memory.current 是 cgroup v2 的当前内存使用量。
	file = "memory.current"
	val, err := fscommon.GetCgroupParamUint(path, file)
	return int64(val), err
}

// GetCgroupConfig 返回指定资源类型应用到 cgroup 的配置值。
func (c *cgroupV2impl) GetCgroupConfig(name CgroupName, resource v1.ResourceName) (*ResourceConfig, error) {
	// 先按资源找到对应 cgroup 路径，再读取具体配置。
	cgroupPaths := c.buildCgroupPaths(name)
	cgroupResourcePath, found := cgroupPaths[string(resource)]
	if !found {
		return nil, fmt.Errorf("failed to build %v cgroup fs path for cgroup %v", resource, name)
	}
	switch resource {
	case v1.ResourceCPU:
		return c.getCgroupCPUConfig(cgroupResourcePath)
	case v1.ResourceMemory:
		return c.getCgroupMemoryConfig(cgroupResourcePath)
	}
	return nil, fmt.Errorf("unsupported resource %v for cgroup %v", resource, name)
}

// getCgroupCPUConfig 从 cpu.max 与 cpu.weight 解析 CPU 配置。
func (c *cgroupV2impl) getCgroupCPUConfig(cgroupPath string) (*ResourceConfig, error) {
	var cpuLimitStr, cpuPeriodStr string
	cpuLimitAndPeriod, err := fscommon.GetCgroupParamString(cgroupPath, cgroupv2CpuMaxFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s file for cgroup %v: %w", cgroupv2CpuMaxFile, cgroupPath, err)
	}
	// cpu.max 的格式为 "<quota> <period>"。
	numItems, errScan := fmt.Sscanf(cpuLimitAndPeriod, "%s %s", &cpuLimitStr, &cpuPeriodStr)
	if errScan != nil || numItems != 2 {
		return nil, fmt.Errorf("failed to correctly parse content of %s file ('%s') for cgroup %v: %w",
			cgroupv2CpuMaxFile, cpuLimitAndPeriod, cgroupPath, errScan)
	}
	// "max" 表示不限制，对应 -1。
	cpuLimit := int64(-1)
	if cpuLimitStr != Cgroup2MaxCpuLimit {
		cpuLimit, err = strconv.ParseInt(cpuLimitStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert CPU limit as integer for cgroup %v: %w", cgroupPath, err)
		}
	}
	cpuPeriod, errPeriod := strconv.ParseUint(cpuPeriodStr, 10, 64)
	if errPeriod != nil {
		return nil, fmt.Errorf("failed to convert CPU period as integer for cgroup %v: %w", cgroupPath, errPeriod)
	}
	cpuWeight, errWeight := fscommon.GetCgroupParamUint(cgroupPath, cgroupv2CpuWeightFile)
	if errWeight != nil {
		return nil, fmt.Errorf("failed to read CPU weight for cgroup %v: %w", cgroupPath, errWeight)
	}
	// 把 v2 的 cpu.weight 换算为 v1 的 cpu.shares。
	cpuShares := cpuWeightToCPUShares(cpuWeight)
	return &ResourceConfig{CPUShares: &cpuShares, CPUQuota: &cpuLimit, CPUPeriod: &cpuPeriod}, nil
}

// getCgroupMemoryConfig 从 memory.max 解析内存限制。
func (c *cgroupV2impl) getCgroupMemoryConfig(cgroupPath string) (*ResourceConfig, error) {
	return readCgroupMemoryConfig(cgroupPath, cgroupv2MemLimitFile)
}

// getSupportedUnifiedControllers 返回 cgroup v2 下支持的控制器集合。
func getSupportedUnifiedControllers() sets.Set[string] {
	// 这是 kubelet 使用的控制器集合。
	supportedControllers := sets.New("cpu", "cpuset", "memory", "hugetlb", "pids")
	// 缓存根 cgroup 中实际存在的控制器，避免重复读取。
	availableRootControllersOnce.Do(func() {
		var err error
		availableRootControllers, err = readUnifiedControllers(cmutil.CgroupRoot)
		if err != nil {
			// 根 cgroup 不可读说明环境异常，直接 panic 以便尽早暴露。
			panic(fmt.Errorf("cannot read cgroup controllers at %s", cmutil.CgroupRoot))
		}
	})
	// 返回 kubelet 与内核共同支持的控制器。
	return supportedControllers.Intersection(availableRootControllers)
}

// readUnifiedControllers 读取指定 cgroup 中可用的控制器。
func readUnifiedControllers(path string) (sets.Set[string], error) {
	controllersFileContent, err := os.ReadFile(filepath.Join(path, "cgroup.controllers"))
	if err != nil {
		return nil, err
	}
	controllers := strings.Fields(string(controllersFileContent))
	return sets.New(controllers...), nil
}

// buildCgroupUnifiedPath 构造指定名称的统一 cgroup 路径。
func (c *cgroupV2impl) buildCgroupUnifiedPath(name CgroupName) string {
	cgroupFsAdaptedName := c.Name(name)
	return path.Join(cmutil.CgroupRoot, cgroupFsAdaptedName)
}

// cpuWeightToCPUShares 把 cgroup v2 的 cpu.weight 转换为 cgroup v1 的 cpu.shares。
// 换算公式参见 KEP 2254。
func cpuWeightToCPUShares(cpuWeight uint64) uint64 {
	return uint64((((cpuWeight - 1) * 262142) / 9999) + 2)
}
