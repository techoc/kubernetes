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
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"

	libcontainercgroups "github.com/opencontainers/cgroups"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/util"
)

// ApplyPodLevelMemoryHigh 按 KEP-2570 的公式为 pod 级 cgroup 设置 memory.high。
// 仅在启用 PodLevelResources 且 pod 声明了内存上限（pod 级或全部容器）时生效；
// 对 request 等于 limit 的 Guaranteed 类 pod 直接跳过。
func ApplyPodLevelMemoryHigh(pod *v1.Pod, rc *ResourceConfig, throttlingFactor float64) {
	// 未启用 pod 级资源或 pod 未声明 pod 级资源时无需处理，直接返回避免误写 cgroup。
	podLevelResourcesEnabled := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources)
	if !podLevelResourcesEnabled || !resourcehelper.IsPodLevelResourcesSet(pod) {
		return
	}
	reqs := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	limits := resourcehelper.PodLimits(pod, resourcehelper.PodResourcesOptions{})
	// 只有全部容器都声明了内存上限才视为"声明了内存上限"，否则无法推导节流区间。
	memoryLimitsDeclared := true
	for c := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers|podutil.Containers) {
		if c.Resources.Limits.Memory().IsZero() {
			memoryLimitsDeclared = false
		}
	}
	if !pod.Spec.Resources.Limits.Memory().IsZero() {
		memoryLimitsDeclared = true
	}
	if !memoryLimitsDeclared {
		return
	}
	memoryRequest := int64(0)
	memoryLimit := int64(0)
	if req, found := reqs[v1.ResourceMemory]; found {
		memoryRequest = req.Value()
	}
	if lim, found := limits[v1.ResourceMemory]; found {
		memoryLimit = lim.Value()
	}
	// 推导结果为空字符串（如 Guaranteed 类 pod）时不写入，避免产生无效的 memory.high 配置。
	if val := memoryHighForPod(memoryRequest, memoryLimit, throttlingFactor); val != "" {
		if rc.Unified == nil {
			rc.Unified = map[string]string{}
		}
		rc.Unified[Cgroup2MemoryHigh] = val
	}
}

// memoryHighForPod 按 KEP-2570 公式推导 pod 级 memory.high 的字节数，
// 结果按页大小向下对齐；不需要设置时返回空字符串。
func memoryHighForPod(memoryRequest, memoryLimit int64, throttlingFactor float64) string {
	// request 等于 limit 的 pod 属于 Guaranteed 语义，没有可节流的弹性区间，无需设置 memory.high；
	// 未声明 limit 时同样无法推导。
	if (memoryRequest == memoryLimit && memoryRequest != 0) || memoryLimit == 0 {
		return ""
	}
	// 按页大小向下对齐，避免写入内核无法按页对齐而忽略的非法值。
	pageSize := int64(os.Getpagesize())
	memoryHigh := int64(math.Floor(
		float64(memoryRequest)+
			(float64(memoryLimit)-float64(memoryRequest))*throttlingFactor)/float64(pageSize)) * pageSize
	if memoryHigh > 0 && memoryHigh > memoryRequest {
		return strconv.FormatInt(memoryHigh, 10)
	}
	return ""
}

// CFS 相关的内核限制与秒/毫核换算基数。
const (
	// 以下限制值来自内核定义：
	// https://github.com/torvalds/linux/blob/0bddd227f3dc55975e2b8dfa7fc6f959b062a2c7/kernel/sched/sched.h#L427-L428
	// MinShares 是内核允许的最小 CFS shares，MaxShares 是最大值。
	MinShares = 2
	MaxShares = 262144

	// 每个 CPU 对应的 shares 数量，以及毫核与核之间的换算基数。
	SharesPerCPU  = 1024
	MilliCPUToCPU = 1000

	// 100000 微秒等价于 100ms，即默认的 CFS 调度周期。
	QuotaPeriod = 100000
	// 1000 微秒等价于 1ms，是内核允许的最小 CFS quota 值，
	// 定义位置：
	// https://github.com/torvalds/linux/blob/cac03ac368fabff0122853de2422d4e17a32de08/kernel/sched/core.c#L10546
	MinQuotaPeriod = 1000

	// 由 MilliCPUToQuota 换算的逆运算得到：MinQuotaPeriod * MilliCPUToCPU / QuotaPeriod。
	// 表示能换算出的最小毫核值，低于该值的配额没有实际意义。
	MinMilliCPULimit = 10
)

// MilliCPUToQuota 把毫核（milliCPU）换算为 CFS quota 和 period 值。
// 输入参数与结果都以微秒为单位。
func MilliCPUToQuota(milliCPU int64, period int64) (quota int64) {
	// CFS quota 由两个值来衡量：
	//  - cfs_period_us=100ms（测量用量的时间窗口，由 period 指定）
	//  - cfs_quota=20ms（一个周期内允许使用的 CPU 时间）
	// 因此上例中单个 CPU 只能使用 20%；多 CPU 环境下按同等比例放大即可。
	// 详见 https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt

	// 毫核为 0 表示不做 CPU 限制，返回零值 quota。
	if milliCPU == 0 {
		return
	}

	// 未启用 CPUCFSQuotaPeriod 时无法自定义 period，只能退回默认的 100ms 调度周期。
	if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CPUCFSQuotaPeriod) {
		period = QuotaPeriod
	}

	// 然后把毫核换算为在一个 period 上归一化的 quota 值。
	quota = (milliCPU * period) / MilliCPUToCPU

	// quota 至少要为 1ms，否则内核会忽略过小的配额，导致实际限制失效。
	if quota < MinQuotaPeriod {
		quota = MinQuotaPeriod
	}
	return
}

// MilliCPUToShares 把毫核（milliCPU）换算为 CFS shares。
func MilliCPUToShares(milliCPU int64) uint64 {
	if milliCPU == 0 {
		// Docker 把 0 毫核转换为未设置，对应内核默认值 1024。
		// 这里返回 MinShares（2）以真正匹配内核对 0 毫核的默认处理。
		return MinShares
	}
	// 概念上等价于 (milliCPU / MilliCPUToCPU) * SharesPerCPU，
	// 改写为先乘后除以减少整数除法带来的舍入误差。
	shares := (milliCPU * SharesPerCPU) / MilliCPUToCPU
	if shares < MinShares {
		return MinShares
	}
	if shares > MaxShares {
		return MaxShares
	}
	return uint64(shares)
}

// HugePageLimits 把 API 的 ResourceList 表示转换为映射，
// key 为大页页大小（字节），value 为该页大小的大页上限（字节）。
func HugePageLimits(resourceList v1.ResourceList) map[int64]int64 {
	hugePageLimits := map[int64]int64{}
	for k, v := range resourceList {
		if v1helper.IsHugePageResourceName(k) {
			pageSize, _ := v1helper.HugePageSizeFromResourceName(k)
			// 同页大小的大页资源可能声明多次，需要累加得到该页大小的总上限。
			if value, exists := hugePageLimits[pageSize.Value()]; exists {
				hugePageLimits[pageSize.Value()] = value + v.Value()
			} else {
				hugePageLimits[pageSize.Value()] = v.Value()
			}
		}
	}
	return hugePageLimits
}

// ResourceConfigForPod 依据输入 pod 的请求与上限，输出其 cgroup 资源配置，
// 包括 CPU shares、quota、period、内存上限与大页限制。
func ResourceConfigForPod(allocatedPod *v1.Pod, enforceCPULimits bool, cpuPeriod uint64, enforceMemoryQoS bool, memoryReservationPolicy kubeletconfig.MemoryReservationPolicy) *ResourceConfig {
	podLevelResourcesEnabled := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources)
	draNodeAllocatableEnabled := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRANodeAllocatableResources)
	// 汇总请求与上限。
	reqs := resourcehelper.PodRequests(allocatedPod, resourcehelper.PodResourcesOptions{
		// PodLevelResources 启用时 SkipPodLevelResources 为 false，把 pod 级资源计入请求。
		SkipPodLevelResources:                    !podLevelResourcesEnabled,
		UseStatusResources:                       false,
		UseDRANodeAllocatableResourceClaimStatus: draNodeAllocatableEnabled,
	})
	// 记录每个资源是否声明了上限，决定是否下发对应的 cgroup 限制。
	memoryLimitsDeclared := true
	cpuLimitsDeclared := true

	limits := resourcehelper.PodLimits(allocatedPod, resourcehelper.PodResourcesOptions{
		// PodLevelResources 启用时 SkipPodLevelResources 为 false，把 pod 级资源计入上限。
		SkipPodLevelResources:                    !podLevelResourcesEnabled,
		UseDRANodeAllocatableResourceClaimStatus: draNodeAllocatableEnabled,
	})

	for c := range podutil.ContainerIter(&allocatedPod.Spec, podutil.InitContainers|podutil.Containers) {
		if c.Resources.Limits.Cpu().IsZero() {
			cpuLimitsDeclared = false
		}
		if c.Resources.Limits.Memory().IsZero() {
			memoryLimitsDeclared = false
		}
	}

	if podLevelResourcesEnabled && resourcehelper.IsPodLevelResourcesSet(allocatedPod) {
		if !allocatedPod.Spec.Resources.Limits.Cpu().IsZero() {
			cpuLimitsDeclared = true
		}

		if !allocatedPod.Spec.Resources.Limits.Memory().IsZero() {
			memoryLimitsDeclared = true
		}
	}
	// 把大页页大小（字节）映射为对应的大页上限（字节）。
	hugePageLimits := HugePageLimits(reqs)

	cpuRequests := int64(0)
	cpuLimits := int64(0)
	memoryLimits := int64(0)
	if request, found := reqs[v1.ResourceCPU]; found {
		cpuRequests = request.MilliValue()
	}
	if limit, found := limits[v1.ResourceCPU]; found {
		cpuLimits = limit.MilliValue()
	}
	if limit, found := limits[v1.ResourceMemory]; found {
		memoryLimits = limit.Value()
	}

	// 把请求/上限换算为 CFS 的 shares 与 quota 值。
	cpuShares := MilliCPUToShares(cpuRequests)
	cpuQuota := MilliCPUToQuota(cpuLimits, int64(cpuPeriod))

	// 关闭 CPU 配额强制时，用 -1 表示不设置 quota（内核按无限制处理）。
	if !enforceCPULimits {
		cpuQuota = int64(-1)
	}

	// 依据请求与上限判定 pod 的 QoS 等级。
	qosClass := v1qos.GetPodQOS(allocatedPod)

	// 依据 QoS 等级构建结果：Guaranteed 下发全部限制，Burstable 仅在声明了上限时下发，
	// BestEffort 只保留最小 shares。
	result := &ResourceConfig{}
	if qosClass == v1.PodQOSGuaranteed {
		result.CPUShares = &cpuShares
		result.CPUQuota = &cpuQuota
		result.CPUPeriod = &cpuPeriod
		result.Memory = &memoryLimits
	} else if qosClass == v1.PodQOSBurstable {
		result.CPUShares = &cpuShares
		if cpuLimitsDeclared {
			result.CPUQuota = &cpuQuota
			result.CPUPeriod = &cpuPeriod
		}
		if memoryLimitsDeclared {
			result.Memory = &memoryLimits
		}
	} else {
		shares := uint64(MinShares)
		result.CPUShares = &shares
	}
	result.HugePageLimit = hugePageLimits

	if enforceMemoryQoS && memoryReservationPolicy == kubeletconfig.TieredReservationMemoryReservationPolicy {
		memoryRequest := int64(0)
		if request, found := reqs[v1.ResourceMemory]; found {
			memoryRequest = request.Value()
		}
		if memoryRequest > 0 {
			// Guaranteed pod 写入 memory.min（硬保护，内核不会回收）；
			// Burstable pod 写入 memory.low（软保护，内核只是优先不回收）。
			cgroupKey := Cgroup2MemoryLow
			if qosClass == v1.PodQOSGuaranteed {
				cgroupKey = Cgroup2MemoryMin
			}
			result.Unified = map[string]string{
				cgroupKey: strconv.FormatInt(memoryRequest, 10),
			}
		}
	}

	return result
}

// getCgroupSubsystemsV1 返回已挂载的 cgroup v1 子系统信息。
func getCgroupSubsystemsV1() (*CgroupSubsystems, error) {
	// 获取所有 cgroup 挂载点。
	allCgroups, err := libcontainercgroups.GetCgroupMounts(true)
	if err != nil {
		return &CgroupSubsystems{}, err
	}
	if len(allCgroups) == 0 {
		return &CgroupSubsystems{}, fmt.Errorf("failed to find cgroup mounts")
	}
	mountPoints := make(map[string]string, len(allCgroups))
	for _, mount := range allCgroups {
		// 挂载点选择策略的演进：BEFORE 阶段每个子系统随机选挂载点；
		// NOW 阶段更确定：统一选用路径最短的挂载点；
		// FUTURE 阶段的选择规则将在文档中明确。
		// 参考问题：https://github.com/kubernetes/kubernetes/issues/95488。

		// 同一子系统可能挂载在多个位置，选最短路径保证结果确定且可复现。
		for _, subsystem := range mount.Subsystems {
			previous := mountPoints[subsystem]
			if previous == "" || len(mount.Mountpoint) < len(previous) {
				mountPoints[subsystem] = mount.Mountpoint
			}
		}
	}
	return &CgroupSubsystems{
		Mounts:      allCgroups,
		MountPoints: mountPoints,
	}, nil
}

// getCgroupSubsystemsV2 返回已启用的 cgroup v2 子系统信息。
func getCgroupSubsystemsV2() (*CgroupSubsystems, error) {
	controllers, err := libcontainercgroups.GetAllSubsystems()
	if err != nil {
		return nil, err
	}

	mounts := []libcontainercgroups.Mount{}
	// cgroup v2 使用统一层级，所有控制器都挂载在同一根路径下，逐一构造挂载信息即可。
	mountPoints := make(map[string]string, len(controllers))
	for _, controller := range controllers {
		mountPoints[controller] = util.CgroupRoot
		m := libcontainercgroups.Mount{
			Mountpoint: util.CgroupRoot,
			Root:       util.CgroupRoot,
			Subsystems: []string{controller},
		}
		mounts = append(mounts, m)
	}

	return &CgroupSubsystems{
		Mounts:      mounts,
		MountPoints: mountPoints,
	}, nil
}

// GetCgroupSubsystems 返回已挂载的 cgroup 子系统信息，按当前内核使用 cgroup v1 或 v2 分派。
func GetCgroupSubsystems() (*CgroupSubsystems, error) {
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		return getCgroupSubsystemsV2()
	}

	return getCgroupSubsystemsV1()
}

// getCgroupProcs 以 cgroup 目录名为参数，读取该目录的 procs 文件并返回其中的 tgid 列表；
// procs 文件不存在时返回空列表。
func getCgroupProcs(dir string) ([]int, error) {
	procsFile := filepath.Join(dir, "cgroup.procs")
	f, err := os.Open(procsFile)
	if err != nil {
		if os.IsNotExist(err) {
			// procs 文件不存在说明该目录下没有归属任何进程，视为空列表。
			return []int{}, nil
		}
		return nil, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	out := []int{}
	for s.Scan() {
		if t := s.Text(); t != "" {
			pid, err := strconv.Atoi(t)
			if err != nil {
				return nil, fmt.Errorf("unexpected line in %v; could not convert to pid: %v", procsFile, err)
			}
			out = append(out, pid)
		}
	}
	return out, nil
}

// GetPodCgroupNameSuffix 返回 pod CgroupName 标识的最后一个元素，即由 pod UID 构成的后缀。
func GetPodCgroupNameSuffix(podUID types.UID) string {
	return podCgroupNamePrefix + string(podUID)
}

// NodeAllocatableRoot 返回节点 allocatable cgroup 的字面路径，按 QoS 分组和驱动类型输出对应格式。
func NodeAllocatableRoot(cgroupRoot string, cgroupsPerQOS bool, cgroupDriver string) string {
	nodeAllocatableRoot := ParseCgroupfsToCgroupName(cgroupRoot)
	if cgroupsPerQOS {
		nodeAllocatableRoot = NewCgroupName(nodeAllocatableRoot, defaultNodeAllocatableCgroupName)
	}
	if cgroupDriver == "systemd" {
		return nodeAllocatableRoot.ToSystemd()
	}
	return nodeAllocatableRoot.ToCgroupfs()
}

// GetKubeletContainer 返回 kubelet 自身将使用的 cgroup：
// 未显式配置时从当前进程所在的 cgroup 推导，否则直接使用传入值。
func GetKubeletContainer(logger klog.Logger, kubeletCgroups string) (string, error) {
	if kubeletCgroups == "" {
		cont, err := getContainer(logger, os.Getpid())
		if err != nil {
			return "", err
		}
		return cont, nil
	}
	return kubeletCgroups, nil
}

// CPURequestsFromConfig 根据资源配置中的 CPUShares 反推 CPU 请求（毫核），无效时返回 nil。
func CPURequestsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	var cpuRequest *resource.Quantity
	// 只有配置了有效 shares 才反推请求，避免把 0 或非法值转换为无意义的请求。
	if podConfig != nil && *podConfig.CPUShares > 0 {
		milliCPU := SharesToMilliCPU(int64(*podConfig.CPUShares))
		if milliCPU > 0 {
			cpuRequest = resource.NewMilliQuantity(milliCPU, resource.DecimalSI)
		}
	}

	return cpuRequest
}

// CPUSharesEqualAfterV2RoundTrip 判断 readbackShares 是否等于把 allocatedShares 经过
// cgroup v2 换算路径（cpu.shares -> cpu.weight -> cpu.shares）往返转换后的结果。
//
// cgroup v2 的 cpu.weight 粒度较粗，写入 shares 再读回可能是有损的。
// 调用方可以据此把有损的读回值视为与最初分配的 shares 等价。
func CPUSharesEqualAfterV2RoundTrip(allocatedShares, readbackShares uint64) bool {
	return cpuWeightToCPUShares(getCPUWeight(&allocatedShares)) == readbackShares
}

// CPULimitsFromConfig 根据资源配置中的 CPUQuota 和 CPUPeriod 反推 CPU 上限（毫核），无效时返回 nil。
func CPULimitsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	var cpuLimit *resource.Quantity

	if podConfig != nil && *podConfig.CPUPeriod > 0 {
		milliCPU := QuotaToMilliCPU(*podConfig.CPUQuota, int64(*podConfig.CPUPeriod))
		if milliCPU > 0 {
			cpuLimit = resource.NewMilliQuantity(milliCPU, resource.DecimalSI)
		}
	}

	return cpuLimit
}

// MemoryLimitsFromConfig 根据资源配置中的内存上限反推内存请求量，无效时返回 nil。
func MemoryLimitsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	var memLimit *resource.Quantity

	if podConfig != nil && *podConfig.Memory > int64(0) {
		memLimit = resource.NewQuantity(*podConfig.Memory, resource.BinarySI)
	}
	return memLimit
}

// SharesToMilliCPU 把 cpu.shares 换算为毫核值。
func SharesToMilliCPU(shares int64) int64 {
	milliCPU := int64(0)
	// 低于内核最小 shares 的值没有意义，直接返回 0 表示无法换算。
	if shares >= int64(MinShares) {
		milliCPU = int64(math.Ceil(float64(shares*MilliCPUToCPU) / float64(SharesPerCPU)))
	}
	return milliCPU
}

// QuotaToMilliCPU 把 cpu.cfs_quota_us 和 cpu.cfs_period_us 换算为毫核值。
func QuotaToMilliCPU(quota int64, period int64) int64 {
	// quota 为 -1 表示内核不限制 CPU，换算结果按 0 毫核处理。
	if quota == -1 {
		return int64(0)
	}
	return (quota * MilliCPUToCPU) / period
}
