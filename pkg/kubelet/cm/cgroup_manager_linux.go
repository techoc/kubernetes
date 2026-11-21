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

package cm

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	libcontainercgroups "github.com/opencontainers/cgroups"
	"github.com/opencontainers/cgroups/fscommon"
	libcontainercgroupmanager "github.com/opencontainers/cgroups/manager"
	cgroupsystemd "github.com/opencontainers/cgroups/systemd"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

const (
	// systemdSuffix 是 systemd 使用的 cgroup 名称后缀。
	systemdSuffix string = ".slice"
	// Cgroup2MemoryMin 是 cgroup v2 的 memory.min。
	Cgroup2MemoryMin string = "memory.min"
	// Cgroup2MemoryLow 是 cgroup v2 的 memory.low。
	Cgroup2MemoryLow string = "memory.low"
	// Cgroup2MemoryHigh 是 cgroup v2 的 memory.high。
	Cgroup2MemoryHigh string = "memory.high"
	// Cgroup2MaxCpuLimit 表示 cgroup v2 中不限制 CPU 的字面值。
	Cgroup2MaxCpuLimit string = "max"
	// Cgroup2MaxSwapFilename 是 cgroup v2 的 swap 上限文件名。
	Cgroup2MaxSwapFilename string = "memory.swap.max"
)

// RootCgroupName 是表示根 cgroup 的空名称。
var RootCgroupName = CgroupName([]string{})

// NewCgroupName 组合出新的 cgroup 名称。
// 使用 RootCgroupName 作为基准即可从根开始。
// 该函数会对名称中的非法字符做基本检查。
func NewCgroupName(base CgroupName, components ...string) CgroupName {
	for _, component := range components {
		// 禁止内部名称中使用 "_"。在把内部名称映射到 systemd cgroup 驱动时，
		// 需要把 "-" 重映射为 "_"，因此禁止 "_" 以保证映射始终可逆。
		if strings.Contains(component, "/") || strings.Contains(component, "_") {
			panic(fmt.Errorf("invalid character in component [%q] of CgroupName", component))
		}
	}
	return CgroupName(append(append([]string{}, base...), components...))
}

// escapeSystemdCgroupName 将内部名称中的 "-" 转义为 systemd 使用的 "_"。
func escapeSystemdCgroupName(part string) string {
	return strings.Replace(part, "-", "_", -1)
}

// unescapeSystemdCgroupName 将 systemd 名称中的 "_" 还原为内部的 "-"。
func unescapeSystemdCgroupName(part string) string {
	return strings.Replace(part, "_", "-", -1)
}

// ToSystemd 将内部 cgroup 名称转换为 systemd 名称。
// 例如 {"kubepods", "burstable", "pod1234-abcd-5678-efgh"} 会变成
// "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1234_abcd_5678_efgh.slice"。
// 该函数总是把 systemd 名称展开成 cgroupfs 形式。若只需要最后一段，
// 可对其调用 path.Base(...) 丢弃其余部分。
func (cgroupName CgroupName) ToSystemd() string {
	if len(cgroupName) == 0 || (len(cgroupName) == 1 && cgroupName[0] == "") {
		// 空名称表示根 cgroup。
		return "/"
	}
	newparts := []string{}
	for _, part := range cgroupName {
		part = escapeSystemdCgroupName(part)
		newparts = append(newparts, part)
	}

	result, err := cgroupsystemd.ExpandSlice(strings.Join(newparts, "-") + systemdSuffix)
	if err != nil {
		// 理论上不应发生，展开失败说明名称非法。
		panic(fmt.Errorf("error converting cgroup name [%v] to systemd format: %v", cgroupName, err))
	}
	return result
}

// ParseSystemdToCgroupName 将 systemd 形式的 cgroup 名称解析为内部名称。
func ParseSystemdToCgroupName(name string) CgroupName {
	driverName := path.Base(name)
	driverName = strings.TrimSuffix(driverName, systemdSuffix)
	parts := strings.Split(driverName, "-")
	result := []string{}
	for _, part := range parts {
		result = append(result, unescapeSystemdCgroupName(part))
	}
	return CgroupName(result)
}

// ToCgroupfs 将内部 cgroup 名称转换为 cgroupfs 路径。
func (cgroupName CgroupName) ToCgroupfs() string {
	return "/" + path.Join(cgroupName...)
}

// ParseCgroupfsToCgroupName 将 cgroupfs 路径解析为内部 cgroup 名称。
func ParseCgroupfsToCgroupName(name string) CgroupName {
	components := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(components) == 1 && components[0] == "" {
		components = []string{}
	}
	return CgroupName(components)
}

// IsSystemdStyleName 判断给定名称是否为 systemd 风格（以 .slice 结尾）。
func IsSystemdStyleName(name string) bool {
	return strings.HasSuffix(name, systemdSuffix)
}

// CgroupSubsystems 保存已挂载的 cgroup 子系统信息。
type CgroupSubsystems struct {
	// cgroup 子系统挂载点。
	// 例如："/sys/fs/cgroup/cpu" -> ["cpu", "cpuacct"]
	Mounts []libcontainercgroups.Mount

	// cgroup 子系统到其挂载位置的映射。
	// 例如："cpu" -> "/sys/fs/cgroup/cpu"
	MountPoints map[string]string
}

// cgroupCommon 实现 cgroup v1 与 v2 共有的通用逻辑，
// 避免在 v1 和 v2 的具体实现中重复代码。
type cgroupCommon struct {
	// subsystems 保存节点上所有已挂载 cgroup 子系统的信息。
	subsystems *CgroupSubsystems

	// useSystemd 表示是否应使用 systemd cgroup 管理器。
	useSystemd bool
}

// 确保 cgroupV1impl 和 cgroupV2impl 实现了 CgroupManager 接口。
var _ CgroupManager = &cgroupV1impl{}
var _ CgroupManager = &cgroupV2impl{}

// NewCgroupManager 是返回 CgroupManager 的工厂方法。
func NewCgroupManager(logger klog.Logger, cs *CgroupSubsystems, cgroupDriver string) CgroupManager {
	// 根据宿主机是否启用 cgroup v2 统一模式选择对应实现。
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		return NewCgroupV2Manager(logger, cs, cgroupDriver)
	}
	return NewCgroupV1Manager(logger, cs, cgroupDriver)
}

// newCgroupCommon 构造 cgroup v1/v2 共用的基础结构。
func newCgroupCommon(logger klog.Logger, cs *CgroupSubsystems, cgroupDriver string) cgroupCommon {
	return cgroupCommon{
		subsystems: cs,
		useSystemd: cgroupDriver == "systemd",
	}
}

// Name 把 cgroup 转换为 cgroupfs 形式的驱动相关值。
// 即使使用 systemd 驱动，也总是返回合法的 cgroupfs 路径！
func (m *cgroupCommon) Name(name CgroupName) string {
	if m.useSystemd {
		return name.ToSystemd()
	}
	return name.ToCgroupfs()
}

// CgroupName 把宿主机上的字面 cgroupfs 名称转换为内部标识符。
func (m *cgroupCommon) CgroupName(name string) CgroupName {
	if m.useSystemd {
		return ParseSystemdToCgroupName(name)
	}
	return ParseCgroupfsToCgroupName(name)
}

// buildCgroupPaths 为指定名称构建各 cgroup 子系统下的路径。
func (m *cgroupCommon) buildCgroupPaths(name CgroupName) map[string]string {
	cgroupFsAdaptedName := m.Name(name)
	cgroupPaths := make(map[string]string, len(m.subsystems.MountPoints))
	for key, val := range m.subsystems.MountPoints {
		cgroupPaths[key] = path.Join(val, cgroupFsAdaptedName)
	}
	return cgroupPaths
}

// libctCgroupConfig 把 CgroupConfig 转换为 libcontainer 的 cgroup 配置。
func (m *cgroupCommon) libctCgroupConfig(logger klog.Logger, in *CgroupConfig, needResources bool) *libcontainercgroups.Cgroup {
	config := &libcontainercgroups.Cgroup{
		Systemd: m.useSystemd,
	}
	if needResources {
		config.Resources = m.toResources(logger, in.ResourceParameters)
	} else {
		// 不需要资源参数时使用空资源，避免误写控制器配置。
		config.Resources = &libcontainercgroups.Resources{}
	}

	if !config.Systemd {
		// 对 fs cgroup 管理器，可以设置 Path，也可以设置 Name 和 Parent；
		// 设置 Path 更简单。
		config.Path = in.Name.ToCgroupfs()

		return config
	}

	// 对 systemd 而言，必须设置 Name 和 Parent，因为与 systemd 交互时需要使用它们。
	// Path 是可选的，可由 Name 和 Parent 推导出来。

	// TODO(filbranden): This logic belongs in libcontainer/cgroup/systemd instead.
	// It should take a libcontainerconfigs.Cgroup.Path field (rather than Name and Parent)
	// and split it appropriately, using essentially the logic below.
	// This was done for cgroupfs in opencontainers/runc#497 but a counterpart
	// for systemd was never introduced.
	dir, base := path.Split(in.Name.ToSystemd())
	if dir == "/" {
		// 根目录在 systemd 中对应 "-.slice"。
		dir = "-.slice"
	} else {
		dir = path.Base(dir)
	}
	config.Parent = dir
	config.Name = base

	return config
}

// Destroy 销毁指定的 cgroup。
func (m *cgroupCommon) Destroy(logger klog.Logger, cgroupConfig *CgroupConfig) error {

	start := time.Now()
	defer func() {
		metrics.CgroupManagerDuration.WithLabelValues("destroy").Observe(metrics.SinceInSeconds(start))
	}()

	libcontainerCgroupConfig := m.libctCgroupConfig(logger, cgroupConfig, false)
	manager, err := libcontainercgroupmanager.New(libcontainerCgroupConfig)
	if err != nil {
		return err
	}

	// 使用 libcontainer 管理器的 Destroy() 方法删除 cgroup。
	if err = manager.Destroy(); err != nil {
		return fmt.Errorf("unable to destroy cgroup paths for cgroup %v : %v", cgroupConfig.Name, err)
	}

	return nil
}

// SetCgroupConfig 将资源参数应用到指定 cgroup。
func (m *cgroupCommon) SetCgroupConfig(logger klog.Logger, name CgroupName, resourceConfig *ResourceConfig) error {
	containerConfig := &CgroupConfig{
		Name:               name,
		ResourceParameters: resourceConfig,
	}

	return m.Update(logger, containerConfig)
}

// getCPUWeight 把 [2, 262144] 范围内的 cpu.shares 转换为 [1, 10000] 范围内的 cpu.weight。
func getCPUWeight(cpuShares *uint64) uint64 {
	if cpuShares == nil {
		// 未设置 shares 时返回 0，表示不写入 weight。
		return 0
	}
	if *cpuShares >= 262144 {
		// 超过上限时饱和到最大值。
		return 10000
	}
	return 1 + ((*cpuShares-2)*9999)/262142
}

var (
	// availableRootControllersOnce 保证根 cgroup 可用控制器的探测只执行一次。
	availableRootControllersOnce sync.Once
	// availableRootControllers 缓存探测到的根 cgroup 控制器集合。
	availableRootControllers sets.Set[string]
)

// toResources 把 ResourceConfig 转换为 libcontainer 的资源结构。
func (m *cgroupCommon) toResources(logger klog.Logger, resourceConfig *ResourceConfig) *libcontainercgroups.Resources {
	resources := &libcontainercgroups.Resources{
		// kubelet 不通过该路径管理设备和冻结，跳过相关操作。
		SkipDevices:     true,
		SkipFreezeOnSet: true,
	}
	if resourceConfig == nil {
		return resources
	}
	if resourceConfig.Memory != nil {
		resources.Memory = *resourceConfig.Memory
	}
	if resourceConfig.CPUShares != nil {
		// cgroup v2 使用 cpu.weight，需要把 shares 换算为权重。
		if libcontainercgroups.IsCgroup2UnifiedMode() {
			resources.CpuWeight = getCPUWeight(resourceConfig.CPUShares)
		} else {
			resources.CpuShares = *resourceConfig.CPUShares
		}
	}
	if resourceConfig.CPUQuota != nil {
		resources.CpuQuota = *resourceConfig.CPUQuota
	}
	if resourceConfig.CPUPeriod != nil {
		resources.CpuPeriod = *resourceConfig.CPUPeriod
	}
	if resourceConfig.PidsLimit != nil {
		resources.PidsLimit = resourceConfig.PidsLimit
	}
	if !resourceConfig.CPUSet.IsEmpty() {
		resources.CpusetCpus = resourceConfig.CPUSet.String()
	}

	m.maybeSetHugetlb(logger, resourceConfig, resources)

	// 在 cgroup v2 上，所有资源最好都通过 Unified 设置。
	// 对 memory.max 没有区别，但对 cpu 控制器等而言，可以直接给出正确的值，
	// 而无需依赖 OCI 运行时执行的换算。
	if resourceConfig.Unified != nil && libcontainercgroups.IsCgroup2UnifiedMode() {
		resources.Unified = make(map[string]string)
		for k, v := range resourceConfig.Unified {
			resources.Unified[k] = v
		}
	}
	return resources
}

// maybeSetHugetlb 在子系统支持 hugetlb 时填充大页限制。
func (m *cgroupCommon) maybeSetHugetlb(logger klog.Logger, resourceConfig *ResourceConfig, resources *libcontainercgroups.Resources) {
	// 检查是否支持 hugetlb。
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		if !getSupportedUnifiedControllers().Has("hugetlb") {
			logger.V(6).Info("Optional subsystem not supported: hugetlb")
			return
		}
	} else if _, ok := m.subsystems.MountPoints["hugetlb"]; !ok {
		logger.V(6).Info("Optional subsystem not supported: hugetlb")
		return
	}

	// 为每个显式列出的页大小设置对应限制。
	pageSizes := sets.New[string]()
	for pageSize, limit := range resourceConfig.HugePageLimit {
		sizeString, err := v1helper.HugePageUnitSizeFromByteSize(pageSize)
		if err != nil {
			logger.Info("Invalid pageSize", "err", err)
			continue
		}
		resources.HugetlbLimit = append(resources.HugetlbLimit, &libcontainercgroups.HugepageLimit{
			Pagesize: sizeString,
			Limit:    uint64(limit),
		})
		pageSizes.Insert(sizeString)
	}
	// 未列出的页大小统一限制为 0，避免沿用旧值。
	for _, pageSize := range libcontainercgroups.HugePageSizes() {
		if pageSizes.Has(pageSize) {
			continue
		}
		resources.HugetlbLimit = append(resources.HugetlbLimit, &libcontainercgroups.HugepageLimit{
			Pagesize: pageSize,
			Limit:    uint64(0),
		})
	}
}

// Update 使用指定的 Cgroup 配置更新 cgroup。
func (m *cgroupCommon) Update(logger klog.Logger, cgroupConfig *CgroupConfig) error {
	start := time.Now()
	defer func() {
		metrics.CgroupManagerDuration.WithLabelValues("update").Observe(metrics.SinceInSeconds(start))
	}()

	libcontainerCgroupConfig := m.libctCgroupConfig(logger, cgroupConfig, true)
	manager, err := libcontainercgroupmanager.New(libcontainerCgroupConfig)
	if err != nil {
		return fmt.Errorf("failed to create cgroup manager: %v", err)
	}
	return manager.Set(libcontainerCgroupConfig.Resources)
}

// Create 创建指定的 cgroup。
func (m *cgroupCommon) Create(logger klog.Logger, cgroupConfig *CgroupConfig) error {
	start := time.Now()
	defer func() {
		metrics.CgroupManagerDuration.WithLabelValues("create").Observe(metrics.SinceInSeconds(start))
	}()

	libcontainerCgroupConfig := m.libctCgroupConfig(logger, cgroupConfig, true)
	manager, err := libcontainercgroupmanager.New(libcontainerCgroupConfig)
	if err != nil {
		return err
	}

	// Apply(-1) 是一个 hack，用于为每个资源子系统创建 cgroup 目录。
	// 函数 [cgroups.Manager.apply()] 会把 cgroup 配置应用到指定 pid 的进程：
	// 它为每个子系统创建 cgroup 文件，并把 pid 写入 tasks 文件。
	// 这里借此创建所有必需的 cgroup 文件，但不把任何“真实” pid 挂入 cgroup。
	if err := manager.Apply(-1); err != nil {
		return err
	}

	// 在 apply 之后再调用 set 可能令人费解，但 runc 采用类似模式，
	// 这样做是为了确保 cpu quota 被正确设置。
	if err := manager.Set(libcontainerCgroupConfig.Resources); err != nil {
		utilruntime.HandleError(fmt.Errorf("cgroup manager.Set failed: %w", err))
	}

	return nil
}

// Pids 扫描所有子系统，查找与指定 cgroup 关联的进程。
func (m *cgroupCommon) Pids(logger klog.Logger, name CgroupName) []int {
	// 需要驱动相关的名称。
	cgroupFsName := m.Name(name)

	// 收集需要终止的进程。
	pidsToKill := sets.New[int]()
	var pids []int
	for _, val := range m.subsystems.MountPoints {
		dir := path.Join(val, cgroupFsName)
		_, err := os.Stat(dir)
		if os.IsNotExist(err) {
			// 子系统中的 Pod cgroup 已被删除，跳过。
			continue
		}
		// 获取仍计入该 Pod cgroup 的 pid。
		pids, err = getCgroupProcs(dir)
		if err != nil {
			continue
		}
		pidsToKill.Insert(pids...)

		// 遍历 Pod cgroup 目录中的每个文件与目录。
		visitor := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				logger.V(4).Info("Cgroup manager encountered error scanning cgroup path", "path", path, "err", err)
				return filepath.SkipDir
			}
			if !info.IsDir() {
				return nil
			}
			pids, err = getCgroupProcs(path)
			if err != nil {
				logger.V(4).Info("Cgroup manager encountered error getting procs for cgroup path", "path", path, "err", err)
				return filepath.SkipDir
			}
			pidsToKill.Insert(pids...)
			return nil
		}
		// 遍历 Pod cgroup 目录，检查尚未被 GC 的容器 cgroup，
		// 收集这些残留容器下挂载的进程。
		if err = filepath.Walk(dir, visitor); err != nil {
			logger.V(4).Info("Cgroup manager encountered error scanning pids for directory", "path", dir, "err", err)
		}
	}
	return sets.List(pidsToKill)
}

// ReduceCPULimits 将 cgroup 的 cpu shares 降到最低可能值。
func (m *cgroupCommon) ReduceCPULimits(logger klog.Logger, cgroupName CgroupName) error {
	// 把 cgroup 的 CpuShares 设为最低值。
	minimumCPUShares := uint64(MinShares)
	resources := &ResourceConfig{
		CPUShares: &minimumCPUShares,
	}
	containerConfig := &CgroupConfig{
		Name:               cgroupName,
		ResourceParameters: resources,
	}
	return m.Update(logger, containerConfig)
}

// readCgroupMemoryConfig 从指定 cgroup 的内存限制文件中读取限制值。
func readCgroupMemoryConfig(cgroupPath string, memLimitFile string) (*ResourceConfig, error) {
	memLimit, err := fscommon.GetCgroupParamUint(cgroupPath, memLimitFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s for cgroup %v: %v", memLimitFile, cgroupPath, err)
	}
	mLim := int64(memLimit)
	//TODO(vinaykul,InPlacePodVerticalScaling): Add memory request support
	return &ResourceConfig{Memory: &mLim}, nil

}
