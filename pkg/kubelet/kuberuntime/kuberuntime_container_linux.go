//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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

package kuberuntime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	cadvisorv1 "github.com/google/cadvisor/lib/model"
	libcontainercgroups "github.com/opencontainers/cgroups"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	kubeapiqos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/qos"
	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/utils/ptr"
)

var defaultPageSize = int64(os.Getpagesize())

// applyPlatformSpecificContainerConfig 向 runtimeapi.ContainerConfig 应用平台相关的配置。
func (m *kubeGenericRuntimeManager) applyPlatformSpecificContainerConfig(ctx context.Context, config *runtimeapi.ContainerConfig, container *v1.Container, pod *v1.Pod, uid *int64, username string, nsTarget *kubecontainer.ContainerID) error {
	enforceMemoryQoS := false
	// 在 MemoryQoS 开启且使用 cgroups v2 时设置 memory.min 与 memory.high
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) &&
		isCgroup2UnifiedMode() {
		enforceMemoryQoS = true
	}
	cl, err := m.generateLinuxContainerConfig(ctx, container, pod, uid, username, nsTarget, enforceMemoryQoS)
	if err != nil {
		return err
	}
	config.Linux = cl

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.UserNamespacesSupport) {
		if cl.SecurityContext.NamespaceOptions.UsernsOptions != nil {
			for _, mount := range config.Mounts {
				mount.UidMappings = cl.SecurityContext.NamespaceOptions.UsernsOptions.Uids
				mount.GidMappings = cl.SecurityContext.NamespaceOptions.UsernsOptions.Gids
			}
		}
	}
	return nil
}

// generateLinuxContainerConfig 为 kubelet runtime v1 生成 Linux 容器配置。
func (m *kubeGenericRuntimeManager) generateLinuxContainerConfig(ctx context.Context, container *v1.Container, pod *v1.Pod, uid *int64, username string, nsTarget *kubecontainer.ContainerID, enforceMemoryQoS bool) (*runtimeapi.LinuxContainerConfig, error) {
	sc, err := m.determineEffectiveSecurityContext(ctx, pod, container, uid, username)
	if err != nil {
		return nil, err
	}
	lc := &runtimeapi.LinuxContainerConfig{
		Resources:       m.generateLinuxContainerResources(ctx, pod, container, enforceMemoryQoS),
		SecurityContext: sc,
	}

	if nsTarget != nil && lc.SecurityContext.NamespaceOptions.Pid == runtimeapi.NamespaceMode_CONTAINER {
		lc.SecurityContext.NamespaceOptions.Pid = runtimeapi.NamespaceMode_TARGET
		lc.SecurityContext.NamespaceOptions.TargetId = nsTarget.ID
	}

	return lc, nil
}

// getCPULimit 返回用于计算 Linux 容器资源的容器 CPU 上限。
func getCPULimit(pod *v1.Pod, container *v1.Container, draAllocations v1.ResourceList) *resource.Quantity {
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		// 容器级 CPU 上限未设置时，依赖 Linux 资源上限配置的组件使用 Pod 级上限参与计算。
		if container.Resources.Limits.Cpu().IsZero() {
			return pod.Spec.Resources.Limits.Cpu()
		}
	}
	limit := container.Resources.Limits.Cpu()
	draCPU, exists := draAllocations[v1.ResourceCPU]
	if limit.IsZero() || !exists || draCPU.IsZero() {
		return limit
	}
	// 仅当容器 spec 中显式设置了上限时，才把 DRA 分配值累加进去；
	// 否则保留现有默认行为：设置了 Pod 级上限则用之，否则为不限量。
	if origContainer := getContainerSpec(pod, container.Name); origContainer != nil && origContainer.Resources.Limits.Cpu().IsZero() {
		return limit
	}
	q := limit.DeepCopy()
	q.Add(draCPU)
	return &q
}

// getMemoryLimit 返回用于计算 Linux 容器资源的容器内存上限。
func getMemoryLimit(pod *v1.Pod, container *v1.Container, draAllocations v1.ResourceList) *resource.Quantity {
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		// 容器级内存上限未设置时，依赖 Linux 资源上限配置的组件使用 Pod 级上限参与计算。
		if container.Resources.Limits.Memory().IsZero() {
			return pod.Spec.Resources.Limits.Memory()
		}
	}
	limit := container.Resources.Limits.Memory()
	draMemory, exists := draAllocations[v1.ResourceMemory]
	if limit.IsZero() || !exists || draMemory.IsZero() {
		return limit
	}
	// 仅当容器 spec 中显式设置了上限时，才把 DRA 分配值累加进去；
	// 否则保留现有默认行为：设置了 Pod 级上限则用之，否则为不限量。
	if origContainer := getContainerSpec(pod, container.Name); origContainer != nil && origContainer.Resources.Limits.Memory().IsZero() {
		return limit
	}
	q := limit.DeepCopy()
	q.Add(draMemory)
	return &q
}

// getContainerSpec 返回 Pod spec 中指定名称的容器，依次查找普通容器与 init 容器。
func getContainerSpec(pod *v1.Pod, containerName string) *v1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == containerName {
			return &pod.Spec.Containers[i]
		}
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == containerName {
			return &pod.Spec.InitContainers[i]
		}
	}
	return nil
}

// generateLinuxContainerResources 生成运行时所需的 Linux 容器资源配置。
func (m *kubeGenericRuntimeManager) generateLinuxContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container, enforceMemoryQoS bool) *runtimeapi.LinuxContainerResources {
	logger := klog.FromContext(ctx)
	// 设置 Linux 容器资源
	var cpuRequest *resource.Quantity
	if _, cpuRequestExists := container.Resources.Requests[v1.ResourceCPU]; cpuRequestExists {
		cpuRequest = container.Resources.Requests.Cpu()
	}

	draAllocations := make(v1.ResourceList)
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRANodeAllocatableResources) {
		draAllocations = resourcehelper.GetContainerDRAAllocations(pod, container.Name)
	}
	memoryLimit := getMemoryLimit(pod, container, draAllocations)
	cpuLimit := getCPULimit(pod, container, draAllocations)

	// 未指定请求值时，默认取不含 DRA 分配的 CPU 上限，
	// 避免 calculateLinuxResources 回退到被 DRA 放大后的 cpuLimit。
	if cpuRequest == nil {
		cpuRequest = getCPULimit(pod, container, nil)
	}

	// 若 Pod 独占 CPU 且该容器请求的是整数 CPU，则不启用 CFS quota
	disableCPUQuota := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DisableCPUQuotaWithExclusiveCPUs) && m.containerManager.ContainerHasExclusiveCPUs(logger, pod, container)
	logger.V(5).Info("Enforcing CFS quota", "pod", klog.KObj(pod), "unlimited", disableCPUQuota)
	lcr := m.calculateLinuxResources(cpuRequest, cpuLimit, memoryLimit, disableCPUQuota)

	lcr.OomScoreAdj = int64(qos.GetContainerOOMScoreAdjust(pod, container,
		int64(m.machineInfo.MemoryCapacity)))

	lcr.HugepageLimits = GetHugepageLimitsFromResources(ctx, pod, container.Resources, draAllocations)

	// 为容器配置 swap。
	m.configureContainerSwapResources(ctx, lcr, pod, container)

	// 设置 memory.min 与 memory.high 以启用 MemoryQoS
	if enforceMemoryQoS {
		unified := map[string]string{}
		memoryRequest := container.Resources.Requests.Memory().Value()
		memoryLimitSpec := container.Resources.Limits.Memory().Value()
		if memoryRequest != 0 && m.memoryReservationPolicy == kubeletconfiginternal.TieredReservationMemoryReservationPolicy {
			// Guaranteed Pod 设置 memory.min（硬性保护），Burstable Pod 设置 memory.low（软性保护）。
			if kubeapiqos.GetPodQOS(pod) == v1.PodQOSGuaranteed {
				unified[cm.Cgroup2MemoryMin] = strconv.FormatInt(memoryRequest, 10)
			} else {
				unified[cm.Cgroup2MemoryLow] = strconv.FormatInt(memoryRequest, 10)
			}
		} else {
			unified[cm.Cgroup2MemoryMin] = "0"
			unified[cm.Cgroup2MemoryLow] = "0"
		}
		// PodLevelResources 生效且容器没有内存上限时，跳过容器级 memory.high：
		// 节流由 Pod cgroup 的 memory.high 按层级处理（内核在 try_charge_memcg 中向上遍历祖先）。
		skipContainerMemoryHigh := memoryLimitSpec == 0 &&
			utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) &&
			resourcehelper.IsPodLevelResourcesSet(pod) &&
			!pod.Spec.Resources.Limits.Memory().IsZero()
		if m.memoryThrottlingFactor != nil && !skipContainerMemoryHigh && (memoryRequest != memoryLimitSpec || memoryRequest == 0) {
			memoryLimitVal := memoryLimitSpec
			if _, exists := draAllocations[v1.ResourceMemory]; exists && memoryLimit != nil && memoryLimitSpec != 0 {
				// 上面计算出的 memoryLimit 已包含 DRA 分配。
				// 用被 DRA 放大后的内存上限计算节流阈值（memory.high），
				// 避免 Burstable Pod 在其标准 Spec 上限处被过早节流。
				// 但 QoS 分类仍严格基于纯 spec 的请求与上限，不考虑 DRA。
				// 对于 DRA 节点可分配资源，继续执行：
				// 1. Guaranteed Pod 跳过设置 memory.high；
				// 2. Besteffort Pod 基于节点内存容量设置 memory.high。
				memoryLimitVal = memoryLimit.Value()
			}
			// K8s v1.27 该特性 Alpha 阶段起，容器 cgroup 的 memory.high 公式有所调整，
			// 按如下公式设置：
			// `memory.high=floor[(requests.memory + memory throttling factor * (limits.memory or node allocatable memory - requests.memory))/pageSize] * pageSize`
			// 详见：https://git.k8s.io/enhancements/keps/sig-node/2570-memory-qos
			memoryHigh := int64(0)
			if memoryLimitVal != 0 {
				memoryHigh = int64(math.Floor(
					float64(memoryRequest)+
						(float64(memoryLimitVal)-float64(memoryRequest))*float64(*m.memoryThrottlingFactor))/float64(defaultPageSize)) * defaultPageSize
			} else {
				allocatable := m.getNodeAllocatable()
				allocatableMemory, ok := allocatable[v1.ResourceMemory]
				if ok && allocatableMemory.Value() > 0 {
					memoryHigh = int64(math.Floor(
						float64(memoryRequest)+
							(float64(allocatableMemory.Value())-float64(memoryRequest))*(*m.memoryThrottlingFactor))/float64(defaultPageSize)) * defaultPageSize
				}
			}
			if memoryHigh != 0 && memoryHigh > memoryRequest {
				unified[cm.Cgroup2MemoryHigh] = strconv.FormatInt(memoryHigh, 10)
			}
		}
		if len(unified) > 0 {
			if lcr.Unified == nil {
				lcr.Unified = unified
			} else {
				for k, v := range unified {
					lcr.Unified[k] = v
				}
			}
			logger.V(4).Info("MemoryQoS config for container", "pod", klog.KObj(pod), "containerName", container.Name, "unified", unified)
		}
	} else if isCgroup2UnifiedMode() {
		// MemoryQoS 关闭时，显式将 memory.high 重置为 "max"，
		// 以便 InPlacePodResize 清除在开启 MemoryQoS 期间创建的容器上残留的节流上限。
		if lcr.Unified == nil {
			lcr.Unified = map[string]string{}
		}
		lcr.Unified[cm.Cgroup2MemoryHigh] = "max"
	}

	return lcr
}

// configureContainerSwapResources 为指定的（Linux）容器配置 swap 资源。
// 仅在 swap cgroup 控制器可用且 NodeSwap 特性门控开启时才进行配置。
func (m *kubeGenericRuntimeManager) configureContainerSwapResources(ctx context.Context, lcr *runtimeapi.LinuxContainerResources, pod *v1.Pod, container *v1.Container) {
	if !m.getSwapControllerAvailable() {
		return
	}

	swapConfigurationHelper := newSwapConfigurationHelper(*m.machineInfo, m.getSwapControllerAvailable)
	// 注意（ehashman）：该行为由 opencontainers 运行时规范定义：
	// https://github.com/opencontainers/runtime-spec/blob/1c3f411f041711bbeecf35ff7e93461ea6789220/config-linux.md#memory
	switch m.GetContainerSwapBehavior(pod, container) {
	case types.NoSwap:
		swapConfigurationHelper.ConfigureNoSwap(ctx, lcr)
	case types.LimitedSwap:
		swapConfigurationHelper.ConfigureLimitedSwap(ctx, lcr, pod, container)
	default:
		swapConfigurationHelper.ConfigureNoSwap(ctx, lcr)
	}
}

// GetContainerSwapBehavior 结合启用 swap 的各项前提条件，判断容器应配置的 swap 行为。
func (m *kubeGenericRuntimeManager) GetContainerSwapBehavior(pod *v1.Pod, container *v1.Container) types.SwapBehavior {
	c := types.SwapBehavior(m.memorySwapBehavior)
	if c == types.LimitedSwap {
		if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.NodeSwap) || !m.getSwapControllerAvailable() {
			return types.NoSwap
		}

		if !isCgroup2UnifiedMode() {
			return types.NoSwap
		}

		if types.IsCriticalPod(pod) {
			return types.NoSwap
		}
		podQos := kubeapiqos.GetPodQOS(pod)
		containerDoesNotRequestMemory := container.Resources.Requests.Memory().IsZero() && container.Resources.Limits.Memory().IsZero()
		memoryRequestEqualsToLimit := container.Resources.Requests.Memory().Cmp(*container.Resources.Limits.Memory()) == 0
		if podQos != v1.PodQOSBurstable || containerDoesNotRequestMemory || memoryRequestEqualsToLimit {
			return types.NoSwap
		}
		return c
	}
	return types.NoSwap
}

// generateContainerResources 生成平台相关的（Linux）容器资源配置。
func (m *kubeGenericRuntimeManager) generateContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container) *runtimeapi.ContainerResources {
	enforceMemoryQoS := false
	// 在 MemoryQoS 开启且使用 cgroups v2 时设置 memory.min 与 memory.high
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) &&
		isCgroup2UnifiedMode() {
		enforceMemoryQoS = true
	}
	return &runtimeapi.ContainerResources{
		Linux: m.generateLinuxContainerResources(ctx, pod, container, enforceMemoryQoS),
	}
}

// generateUpdatePodSandboxResourcesRequest 生成平台相关的（Linux）PodSandbox 资源更新请求。
func (m *kubeGenericRuntimeManager) generateUpdatePodSandboxResourcesRequest(sandboxID string, pod *v1.Pod, podResources *cm.ResourceConfig) *runtimeapi.UpdatePodSandboxResourcesRequest {

	podResourcesWithoutOverhead := subtractOverheadFromResourceConfig(podResources, pod)
	return &runtimeapi.UpdatePodSandboxResourcesRequest{
		PodSandboxId: sandboxID,
		Overhead:     m.convertOverheadToLinuxResources(pod),
		Resources:    convertResourceConfigToLinuxContainerResources(podResourcesWithoutOverhead),
	}
}

// calculateLinuxResources 根据给定的 CPU 与内存资源请求、上限生成 linuxContainerResources。
func (m *kubeGenericRuntimeManager) calculateLinuxResources(cpuRequest, cpuLimit, memoryLimit *resource.Quantity, disableCPUQuota bool) *runtimeapi.LinuxContainerResources {
	resources := runtimeapi.LinuxContainerResources{}
	var cpuShares int64

	memLimit := memoryLimit.Value()

	// 请求未指定但上限已指定时，请求默认取上限。API server 对新容器已这样做，
	// 此处在 Kubelet 重复该逻辑，以兼容已有集群中的存量容器。
	if cpuRequest == nil && cpuLimit != nil {
		cpuShares = int64(cm.MilliCPUToShares(cpuLimit.MilliValue()))
	} else {
		// cpuRequest.Amount 为 nil 时，MilliCPUToShares 返回最小的 CPU shares 值。
		cpuShares = int64(cm.MilliCPUToShares(cpuRequest.MilliValue()))
	}
	resources.CpuShares = cpuShares
	if memLimit != 0 {
		resources.MemoryLimitInBytes = memLimit
	}

	if m.cpuCFSQuota {
		// cpuLimit.Amount 为 nil 时，返回允许充分使用 CPU 资源的合适默认值。
		cpuPeriod := int64(cm.QuotaPeriod)
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CPUCFSQuotaPeriod) {
			// cpuCFSQuotaPeriod 以 time.Duration 提供，需要转换为内核使用的微秒数。
			cpuPeriod = int64(m.cpuCFSQuotaPeriod.Duration / time.Microsecond)
		}
		cpuQuota := cm.MilliCPUToQuota(cpuLimit.MilliValue(), cpuPeriod)
		resources.CpuQuota = cpuQuota
		if disableCPUQuota {
			resources.CpuQuota = int64(-1)
		}
		resources.CpuPeriod = cpuPeriod
	}

	// runc 的 unified 模式依赖 cgroup v2
	if isCgroup2UnifiedMode() && !ptr.Deref(m.singleProcessOOMKill, true) {
		resources.Unified = map[string]string{
			// 请求内核在 OOM 时杀死容器 cgroup 内的所有进程；
			// 详见 https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html 中 memory.oom.group 的说明。
			"memory.oom.group": "1",
		}
	}
	return &resources
}

// GetHugepageLimitsFromResources 返回资源中各大页（hugepage）的上限。
func GetHugepageLimitsFromResources(ctx context.Context, pod *v1.Pod, containerResources v1.ResourceRequirements, draAllocations v1.ResourceList) []*runtimeapi.HugepageLimit {
	var hugepageLimits []*runtimeapi.HugepageLimit

	// 每种页大小先默认上限为 0。
	for _, pageSize := range libcontainercgroups.HugePageSizes() {
		hugepageLimits = append(hugepageLimits, &runtimeapi.HugepageLimit{
			PageSize: pageSize,
			Limit:    uint64(0),
		})
	}

	requiredHugepageLimits := map[string]uint64{}

	// Pod 级设置了大页上限而容器级未设置时，容器 cgroup 沿用 Pod 级上限。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		for limitName, limitAmount := range pod.Spec.Resources.Limits {
			readAndDefineRequiredHugepageLimit(ctx, requiredHugepageLimits, limitName, limitAmount)
		}
	}

	// Pod 级与容器级都设置了大页时，容器级优先，容器 cgroup 反映容器级上限。
	for resourceObj, amountObj := range containerResources.Limits {
		readAndDefineRequiredHugepageLimit(ctx, requiredHugepageLimits, resourceObj, amountObj)
	}

	// 即使容器 spec 未设置大页上限，也要应用 DRA 的大页分配。
	// 与 CPU/内存（默认不限量）不同，大页在 spec 未指定时默认为 0（禁止分配），
	// 因此必须显式把上限设为 DRA 分配值，才能在缺少 Pod 级上限时允许容器进程分配大页。
	for resourceObj, amountObj := range draAllocations {
		if v1helper.IsHugePageResourceName(resourceObj) {
			pageSize, err := v1helper.HugePageSizeFromResourceName(resourceObj)
			if err != nil {
				continue
			}
			sizeString, err := v1helper.HugePageUnitSizeFromByteSize(pageSize.Value())
			if err != nil {
				continue
			}

			// 判断该大页资源是否设置了 Pod 级上限。
			podLevelLimitsSet := false
			if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources) && pod.Spec.Resources != nil {
				_, podLevelLimitsSet = pod.Spec.Resources.Limits[resourceObj]
			}
			_, containerSpecLimitsSet := containerResources.Limits[resourceObj]

			if containerSpecLimitsSet {
				// 情况 A：容器上限显式指定 -> 在其基础上叠加 DRA 值。
				requiredHugepageLimits[sizeString] += uint64(amountObj.Value())
			} else if !podLevelLimitsSet {
				// 情况 B：该大页无 Pod 级上限且容器上限未设置 -> 上限取 DRA 分配值。
				requiredHugepageLimits[sizeString] = uint64(amountObj.Value())
			}
			// 情况 C：该大页设置了 Pod 级上限但容器上限未设置 -> 保留已写入 requiredHugepageLimits 的 Pod 级上限。
		}
	}

	for _, hugepageLimit := range hugepageLimits {
		if limit, exists := requiredHugepageLimits[hugepageLimit.PageSize]; exists {
			hugepageLimit.Limit = limit
		}
	}

	return hugepageLimits
}

// readAndDefineRequiredHugepageLimit 把资源中的大页项解析为页大小字符串并写入汇总表，非大页资源直接忽略。
func readAndDefineRequiredHugepageLimit(ctx context.Context, requiredHugepageLimits map[string]uint64, resourceObj v1.ResourceName, amountObj resource.Quantity) {
	logger := klog.FromContext(ctx)

	if !v1helper.IsHugePageResourceName(resourceObj) {
		return
	}

	pageSize, err := v1helper.HugePageSizeFromResourceName(resourceObj)
	if err != nil {
		logger.Info("Failed to get hugepage size from resource", "object", resourceObj, "err", err)
		return
	}

	sizeString, err := v1helper.HugePageUnitSizeFromByteSize(pageSize.Value())
	if err != nil {
		logger.Info("Size is invalid", "object", resourceObj, "err", err)
		return
	}
	requiredHugepageLimits[sizeString] = uint64(amountObj.Value())
}

// toKubeContainerResources 将运行时状态中的容器资源转换为 kubecontainer.ContainerResources；
// 无任何有效字段时返回 nil。
func toKubeContainerResources(statusResources *runtimeapi.ContainerResources) *kubecontainer.ContainerResources {
	var cStatusResources *kubecontainer.ContainerResources
	runtimeStatusResources := statusResources.GetLinux()
	if runtimeStatusResources != nil {
		var cpuLimit, memLimit, cpuRequest *resource.Quantity
		if runtimeStatusResources.CpuPeriod > 0 {
			milliCPU := cm.QuotaToMilliCPU(runtimeStatusResources.CpuQuota, runtimeStatusResources.CpuPeriod)
			if milliCPU > 0 {
				cpuLimit = resource.NewMilliQuantity(milliCPU, resource.DecimalSI)
			}
		}
		if runtimeStatusResources.CpuShares > 0 {
			milliCPU := cm.SharesToMilliCPU(runtimeStatusResources.CpuShares)
			if milliCPU > 0 {
				cpuRequest = resource.NewMilliQuantity(milliCPU, resource.DecimalSI)
			}
		}
		if runtimeStatusResources.MemoryLimitInBytes > 0 {
			memLimit = resource.NewQuantity(runtimeStatusResources.MemoryLimitInBytes, resource.BinarySI)
		}
		if cpuLimit != nil || memLimit != nil || cpuRequest != nil {
			cStatusResources = &kubecontainer.ContainerResources{
				CPULimit:    cpuLimit,
				CPURequest:  cpuRequest,
				MemoryLimit: memLimit,
			}
		}
	}
	return cStatusResources
}

// 说明：这里将函数声明为变量，是为了在单元测试中替换为 mock 函数来模拟 cgroup 版本；
// 否则 cgroup 版本将完全取决于测试运行的环境。
var isCgroup2UnifiedMode = libcontainercgroups.IsCgroup2UnifiedMode

// isMemoryQoSEnforced 报告 MemoryQoS 是否在 cgroup v2 上生效。
func (m *kubeGenericRuntimeManager) isMemoryQoSEnforced() bool {
	return utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) && isCgroup2UnifiedMode()
}

// applyPodLevelMemoryHigh 设置 Pod 级 memory.high，由内核按层级强制生效。
func (m *kubeGenericRuntimeManager) applyPodLevelMemoryHigh(pod *v1.Pod, rc *cm.ResourceConfig) {
	if m.memoryThrottlingFactor != nil {
		cm.ApplyPodLevelMemoryHigh(pod, rc, *m.memoryThrottlingFactor)
	}
}

// checkSwapControllerAvailability 检查 swap 控制器是否可用，可用返回 true，否则返回 false。
func checkSwapControllerAvailability(ctx context.Context) bool {
	// 参考 https://github.com/containerd/containerd/pull/7838/
	logger := klog.FromContext(ctx)
	const warn = "Failed to detect the availability of the swap controller, assuming not available"
	p := "/sys/fs/cgroup/memory/memory.memsw.limit_in_bytes"
	if isCgroup2UnifiedMode() {
		// cgroup 根目录下没有 memory.swap.max，因此检查 /sys/fs/cgroup/<SELF>/memory.swap.max
		cm, err := libcontainercgroups.ParseCgroupFile("/proc/self/cgroup")
		if err != nil {
			logger.V(5).Info(warn, "err", fmt.Errorf("failed to parse /proc/self/cgroup: %w", err))
			return false
		}
		// cgroup v2 统一层级下没有按控制器划分的 cgroup 路径，
		// 因此上面 ParseCgroupFile 返回的 cm map 只有一个元素：
		// key 为空字符串（""），value 是该 <pid> 所在的 cgroup 路径。
		p = filepath.Join("/sys/fs/cgroup", cm[""], "memory.swap.max")
	}
	if _, err := os.Stat(p); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.V(5).Info(warn, "err", err)
		}
		return false
	}

	return true
}

// initSwapControllerAvailabilityCheck 返回一个基于 sync.OnceValue 惰性初始化的
// swap 控制器可用性检查函数。
func initSwapControllerAvailabilityCheck(ctx context.Context) func() bool {
	return sync.OnceValue(func() bool {
		return checkSwapControllerAvailability(ctx)
	})
}

// swapConfigurationHelper 封装 swap 配置的计算与下发，依赖机器信息与控制器可用性检查。
type swapConfigurationHelper struct {
	machineInfo                cadvisorv1.MachineInfo
	getSwapControllerAvailable func() bool
}

// newSwapConfigurationHelper 构造 swapConfigurationHelper。
func newSwapConfigurationHelper(machineInfo cadvisorv1.MachineInfo, getSwapControllerAvailable func() bool) *swapConfigurationHelper {
	return &swapConfigurationHelper{
		machineInfo:                machineInfo,
		getSwapControllerAvailable: getSwapControllerAvailable,
	}
}

// ConfigureLimitedSwap 为 Burstable Pod 配置有限 swap：按比例计算 swap 上限，
// 计算失败时降级为不启用 swap。
func (m swapConfigurationHelper) ConfigureLimitedSwap(ctx context.Context, lcr *runtimeapi.LinuxContainerResources, pod *v1.Pod, container *v1.Container) {
	logger := klog.FromContext(ctx)
	containerMemoryRequest := container.Resources.Requests.Memory()
	swapLimit, err := calcSwapForBurstablePods(containerMemoryRequest.Value(), int64(m.machineInfo.MemoryCapacity), int64(m.machineInfo.SwapCapacity))
	if err != nil {
		logger.Error(err, "Cannot calculate swap allocation amount; disallowing swap")
		m.ConfigureNoSwap(ctx, lcr)
		return
	}

	m.configureSwap(ctx, lcr, swapLimit)
}

// ConfigureNoSwap 配置为不启用 swap：cgroup v1 下把 swap 上限对齐内存上限，cgroup v2 下显式置 0。
func (m swapConfigurationHelper) ConfigureNoSwap(ctx context.Context, lcr *runtimeapi.LinuxContainerResources) {
	if !isCgroup2UnifiedMode() {
		if m.getSwapControllerAvailable() {
			// memorySwapLimit 是允许的内存+swap 总量；与内存上限相等意味着超出内存上限的 swap 配额为 0，
			// 但仍可能发生部分 swap。注意内存上限为 0 时，内存 swap 上限会被忽略。
			lcr.MemorySwapLimitInBytes = lcr.MemoryLimitInBytes
		}
		return
	}

	m.configureSwap(ctx, lcr, 0)
}

// configureSwap 在 cgroup v2 下把 swap 上限写入 lcr.Unified；cgroup v1 环境直接报错返回。
func (m swapConfigurationHelper) configureSwap(ctx context.Context, lcr *runtimeapi.LinuxContainerResources, swapMemory int64) {
	logger := klog.FromContext(ctx)
	if !isCgroup2UnifiedMode() {
		logger.Error(fmt.Errorf("swap configuration is not supported with cgroup v1"), "Swap configuration under cgroup v1 is unexpected")
		return
	}

	if lcr.Unified == nil {
		lcr.Unified = map[string]string{}
	}

	lcr.Unified[cm.Cgroup2MaxSwapFilename] = fmt.Sprintf("%d", swapMemory)
}

// swap 上限按 (<容器内存请求>/<节点总内存>)*<Pod 可用 swap 总量> 计算。
// 详见 KEP：https://kep.k8s.io/2400
func calcSwapForBurstablePods(containerMemoryRequest, nodeTotalMemory, totalPodsSwapAvailable int64) (int64, error) {
	if nodeTotalMemory <= 0 {
		return 0, fmt.Errorf("total node memory is 0")
	}
	if containerMemoryRequest > nodeTotalMemory {
		return 0, fmt.Errorf("container request %d is larger than total node memory %d", containerMemoryRequest, nodeTotalMemory)
	}

	containerMemoryProportion := float64(containerMemoryRequest) / float64(nodeTotalMemory)
	swapAllocation := containerMemoryProportion * float64(totalPodsSwapAvailable)

	return int64(swapAllocation), nil
}

// toKubeContainerUser 将运行时返回的容器用户信息转换为 kubecontainer.ContainerUser，nil 输入返回 nil。
func toKubeContainerUser(statusUser *runtimeapi.ContainerUser) *kubecontainer.ContainerUser {
	if statusUser == nil {
		return nil
	}

	user := &kubecontainer.ContainerUser{}
	if statusUser.GetLinux() != nil {
		user.Linux = &kubecontainer.LinuxContainerUser{
			UID:                statusUser.GetLinux().GetUid(),
			GID:                statusUser.GetLinux().GetGid(),
			SupplementalGroups: statusUser.GetLinux().GetSupplementalGroups(),
		}
	}

	return user
}
