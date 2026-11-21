/*
Copyright 2017 The Kubernetes Authors.

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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/wait"

	units "github.com/docker/go-units"
	libcontainercgroups "github.com/opencontainers/cgroups"
	utilfeature "k8s.io/apiserver/pkg/util/feature"

	"k8s.io/component-helpers/resource"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
)

// qos cgroup 管理器相关的常量定义。
const (
	// periodicQOSCgroupUpdateInterval 是 qos cgroup 管理器周期性更新
	// QoS 级别 cgroup 资源约束的时间间隔。
	periodicQOSCgroupUpdateInterval = 1 * time.Minute
)

// QOSContainerManager 管理 QoS 级别（Guaranteed、Burstable、BestEffort）cgroup 的接口。
type QOSContainerManager interface {
	Start(context.Context, func() v1.ResourceList, ActivePodsFunc) error
	GetQOSContainersInfo() QOSContainersInfo
	UpdateCgroups(logger klog.Logger) error
}

// qosContainerManagerImpl 是 QOSContainerManager 的默认实现：
// 为 Burstable 与 BestEffort 两类 QoS 创建顶层 cgroup，并周期性同步资源约束。
type qosContainerManagerImpl struct {
	sync.Mutex
	qosContainersInfo       QOSContainersInfo
	subsystems              *CgroupSubsystems
	cgroupManager           CgroupManager
	activePods              ActivePodsFunc
	getNodeAllocatable      func() v1.ResourceList
	cgroupRoot              CgroupName
	qosReserved             map[v1.ResourceName]int64
	memoryReservationPolicy kubeletconfig.MemoryReservationPolicy
}

// NewQOSContainerManager 创建 QoS 容器管理器实例。
// 节点配置未开启 CgroupsPerQOS 时，返回不做任何实际操作的 noop 实现。
func NewQOSContainerManager(subsystems *CgroupSubsystems, cgroupRoot CgroupName, nodeConfig NodeConfig, cgroupManager CgroupManager) (QOSContainerManager, error) {
	if !nodeConfig.CgroupsPerQOS {
		// 未按 QoS 划分 cgroup，无需创建顶层容器，直接用空实现即可。
		return &qosContainerManagerNoop{
			cgroupRoot: cgroupRoot,
		}, nil
	}

	return &qosContainerManagerImpl{
		subsystems:              subsystems,
		cgroupManager:           cgroupManager,
		cgroupRoot:              cgroupRoot,
		qosReserved:             nodeConfig.QOSReserved,
		memoryReservationPolicy: nodeConfig.MemoryReservationPolicy,
	}, nil
}

// GetQOSContainersInfo 返回已创建的 QoS 级别 cgroup 名称信息。
func (m *qosContainerManagerImpl) GetQOSContainersInfo() QOSContainersInfo {
	return m.qosContainersInfo
}

// Start 校验并创建各 QoS 级别的顶层 cgroup，保存回调函数，
// 然后启动周期性任务，持续把 QoS 资源约束同步到 cgroup 上。
func (m *qosContainerManagerImpl) Start(ctx context.Context, getNodeAllocatable func() v1.ResourceList, activePods ActivePodsFunc) error {
	logger := klog.FromContext(ctx)
	cm := m.cgroupManager
	rootContainer := m.cgroupRoot

	if err := cm.Validate(rootContainer); err != nil {
		return fmt.Errorf("error validating root container %v : %w", rootContainer, err)
	}

	// 只为 Burstable 和 BestEffort 两类 QoS 创建顶层 cgroup，
	// Guaranteed pod 直接使用根容器所在的 cgroup。
	qosClasses := map[v1.PodQOSClass]CgroupName{
		v1.PodQOSBurstable:  NewCgroupName(rootContainer, strings.ToLower(string(v1.PodQOSBurstable))),
		v1.PodQOSBestEffort: NewCgroupName(rootContainer, strings.ToLower(string(v1.PodQOSBestEffort))),
	}

	// 为两个 QoS 类分别创建顶层容器。
	for qosClass, containerName := range qosClasses {
		resourceParameters := &ResourceConfig{}
		// BestEffort 的最小份额是静态配置的，不随 pod 变化。
		if qosClass == v1.PodQOSBestEffort {
			minShares := uint64(MinShares)
			resourceParameters.CPUShares = &minShares
		}

		// MemoryQoS 未开启或策略为 None 时，在启动阶段重置残留的内存保护设置。
		// 周期性循环只为 TieredReservation 运行（其取值随 pod 增减而变化）；
		// 其余策略切换都属于需要重启 kubelet 的配置变更，在启动时处理一次即可。
		if libcontainercgroups.IsCgroup2UnifiedMode() {
			if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) ||
				m.memoryReservationPolicy != kubeletconfig.TieredReservationMemoryReservationPolicy {
				if qosClass == v1.PodQOSBurstable {
					resourceParameters.Unified = map[string]string{Cgroup2MemoryLow: "0"}
				}
			}
		}

		// containerConfig 保存该 cgroup 的配置规格。
		containerConfig := &CgroupConfig{
			Name:               containerName,
			ResourceParameters: resourceParameters,
		}

		// 对每一种枚举出的巨页大小，QoS 层级都保持不设上限。
		m.setHugePagesUnbounded(containerConfig)

		// 先检查目标 cgroup 是否已经存在。
		if !cm.Exists(containerName) {
			if err := cm.Create(logger, containerConfig); err != nil {
				return fmt.Errorf("failed to create top level %v QOS cgroup : %v", qosClass, err)
			}
		} else {
			// 已存在时主动更新配置，确保实际状态与期望一致，而不是假定状态正确。
			if err := cm.Update(logger, containerConfig); err != nil {
				return fmt.Errorf("failed to update top level %v QOS cgroup : %v", qosClass, err)
			}
		}
	}
	// 上面的循环不覆盖根 cgroup（kubepods.slice），
	// 因此这里在启动时单独重置其残留的内存保护设置。
	if libcontainercgroups.IsCgroup2UnifiedMode() {
		if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) ||
			m.memoryReservationPolicy != kubeletconfig.TieredReservationMemoryReservationPolicy {
			rootConfig := &CgroupConfig{
				Name: rootContainer,
				ResourceParameters: &ResourceConfig{
					Unified: map[string]string{
						Cgroup2MemoryMin: "0",
						Cgroup2MemoryLow: "0",
					},
				},
			}
			if err := cm.Update(logger, rootConfig); err != nil {
				logger.Error(err, "Failed to reset memory protection on root cgroup")
			}
		}
	}

	// 记录顶层 QoS 容器的名称，供后续更新与查询使用。
	m.qosContainersInfo = QOSContainersInfo{
		Guaranteed: rootContainer,
		Burstable:  qosClasses[v1.PodQOSBurstable],
		BestEffort: qosClasses[v1.PodQOSBestEffort],
	}
	m.getNodeAllocatable = getNodeAllocatable
	m.activePods = activePods

	// 启动后立即同步一次，之后按固定间隔周期同步，
	// 保证期望状态与实际 cgroup 状态持续一致。
	go wait.Until(func() {
		err := m.UpdateCgroups(logger)
		if err != nil {
			logger.Info("Failed to reserve QoS requests", "err", err)
		}
	}, periodicQOSCgroupUpdateInterval, wait.NeverStop)

	return nil
}

// setHugePagesUnbounded 把各类巨页的配额设为极大值，
// 使 QoS 层级对 hugetlb 资源实际上不设限制。
func (m *qosContainerManagerImpl) setHugePagesUnbounded(cgroupConfig *CgroupConfig) error {
	hugePageLimit := map[int64]int64{}
	for _, pageSize := range libcontainercgroups.HugePageSizes() {
		pageSizeBytes, err := units.RAMInBytes(pageSize)
		if err != nil {
			return err
		}
		// 用 1<<62 作为"无上限"的占位值：既满足 cgroup 对上限的约束，又不会限制实际使用。
		hugePageLimit[pageSizeBytes] = int64(1 << 62)
	}
	cgroupConfig.ResourceParameters.HugePageLimit = hugePageLimit
	return nil
}

// setHugePagesConfig 为给定的一组 QoS 层级 cgroup 统一设置巨页不设上限。
func (m *qosContainerManagerImpl) setHugePagesConfig(configs map[v1.PodQOSClass]*CgroupConfig) error {
	for _, v := range configs {
		if err := m.setHugePagesUnbounded(v); err != nil {
			return err
		}
	}
	return nil
}

// setCPUCgroupConfig 根据当前活跃 pod 的 CPU 请求，计算并设置各 QoS 层级的 CPU 份额：
// BestEffort 固定为最小份额，Burstable 按其全部 pod 的 CPU 请求之和换算。
func (m *qosContainerManagerImpl) setCPUCgroupConfig(configs map[v1.PodQOSClass]*CgroupConfig) error {
	pods := m.activePods()
	burstablePodCPURequest := int64(0)
	// 复用同一个 ResourceList 作为缓存，避免为每个 pod 重复分配内存。
	reuseReqs := make(v1.ResourceList, 4)
	for i := range pods {
		pod := pods[i]
		qosClass := v1qos.GetPodQOS(pod)
		if qosClass != v1.PodQOSBurstable {
			// 只关心 Burstable 这一 QoS 层级，其余层级不参与份额计算。
			continue
		}
		draNodeAllocatableEnabled := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRANodeAllocatableResources)
		req := resource.PodRequests(pod, resource.PodResourcesOptions{
			Reuse: reuseReqs,
			// PodLevelResources 特性开启时，不应跳过 pod 级资源。
			SkipPodLevelResources:                    !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResources),
			UseDRANodeAllocatableResourceClaimStatus: draNodeAllocatableEnabled,
		})
		if request, found := req[v1.ResourceCPU]; found {
			burstablePodCPURequest += request.MilliValue()
		}
	}

	// 保证 BestEffort 始终使用 2 个份额。
	bestEffortCPUShares := uint64(MinShares)
	configs[v1.PodQOSBestEffort].ResourceParameters.CPUShares = &bestEffortCPUShares

	// 按当前观测到的请求总量换算 Burstable 的份额，使其随 pod 变化。
	burstableCPUShares := MilliCPUToShares(burstablePodCPURequest)
	configs[v1.PodQOSBurstable].ResourceParameters.CPUShares = &burstableCPUShares
	return nil
}

// getQoSMemoryRequests 汇总并返回 Guaranteed 与 Burstable 两类 QoS 下所有 pod 的内存请求。
func (m *qosContainerManagerImpl) getQoSMemoryRequests() map[v1.PodQOSClass]int64 {
	qosMemoryRequests := map[v1.PodQOSClass]int64{
		v1.PodQOSGuaranteed: 0,
		v1.PodQOSBurstable:  0,
	}

	// 汇总每个 QoS 类下各 pod 的内存限额。
	pods := m.activePods()
	reuseReqs := make(v1.ResourceList, 4)
	for _, pod := range pods {
		podMemoryRequest := int64(0)
		qosClass := v1qos.GetPodQOS(pod)
		if qosClass == v1.PodQOSBestEffort {
			// BestEffort pod 不设置限额，无需纳入统计。
			continue
		}
		draNodeAllocatableEnabled := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRANodeAllocatableResources)
		req := resource.PodRequests(pod, resource.PodResourcesOptions{
			Reuse:                                    reuseReqs,
			UseDRANodeAllocatableResourceClaimStatus: draNodeAllocatableEnabled,
		})
		if request, found := req[v1.ResourceMemory]; found {
			podMemoryRequest += request.Value()
		}
		qosMemoryRequests[qosClass] += podMemoryRequest
	}

	return qosMemoryRequests
}

// setMemoryReserve 汇总某 QoS 类下所有 pod 的内存限额，
// 计算出各 QoS 类的内存上限，并把这些上限写入对应 QoS 类的 CgroupConfig。
func (m *qosContainerManagerImpl) setMemoryReserve(logger klog.Logger, configs map[v1.PodQOSClass]*CgroupConfig, percentReserve int64) {
	qosMemoryRequests := m.getQoSMemoryRequests()

	resources := m.getNodeAllocatable()
	allocatableResource, ok := resources[v1.ResourceMemory]
	if !ok {
		logger.V(2).Info("Allocatable memory value could not be determined, not setting QoS memory limits")
		return
	}
	allocatable := allocatableResource.Value()
	if allocatable == 0 {
		logger.V(2).Info("Allocatable memory reported as 0, might be in standalone mode, not setting QoS memory limits")
		return
	}

	for qos, limits := range qosMemoryRequests {
		logger.V(2).Info("QoS pod memory limit", "qos", qos, "limits", limits, "percentReserve", percentReserve)
	}

	// 依次扣减：先按 Guaranteed 的保留比例占用可分配内存，
	// 再从剩余部分按 Burstable 的保留比例扣减，得到 BestEffort 的上限。
	burstableLimit := allocatable - (qosMemoryRequests[v1.PodQOSGuaranteed] * percentReserve / 100)
	bestEffortLimit := burstableLimit - (qosMemoryRequests[v1.PodQOSBurstable] * percentReserve / 100)
	configs[v1.PodQOSBurstable].ResourceParameters.Memory = &burstableLimit
	configs[v1.PodQOSBestEffort].ResourceParameters.Memory = &bestEffortLimit
}

// retrySetMemoryReserve 检查首次 Update() 之后 QoS cgroup 是否超出目标限额，
// 若超出则把限额调整为当前用量，阻止内存继续增长。
func (m *qosContainerManagerImpl) retrySetMemoryReserve(logger klog.Logger, configs map[v1.PodQOSClass]*CgroupConfig, percentReserve int64) {
	// 不可回收的内存用量可能已经超过目标限额，
	// 把限额压到当前用量附近，对 cgroup 形成压力，阻止其进一步增长。
	for qos, config := range configs {
		usage, err := m.cgroupManager.MemoryUsage(config.Name)
		if err != nil {
			logger.V(2).Info("Failed to get resource stats", "err", err)
			return
		}

		// 无法直接判断首次 Update() 是否成功，这里通过"用量是否超过目标限额"来推断：
		// 若超过，则认为首次设置失败，把限额改为当前用量后重试；否则保持原配置不变。
		if configs[qos].ResourceParameters.Memory != nil && usage > *configs[qos].ResourceParameters.Memory {
			configs[qos].ResourceParameters.Memory = &usage
		}
	}
}

// setMemoryQoS 为 QoS 层级 cgroup 设置 cgroup v2 的内存保护。
// Guaranteed pod 使用 memory.min（硬保护），Burstable pod 使用 memory.low（软保护）。
func (m *qosContainerManagerImpl) setMemoryQoS(logger klog.Logger, configs map[v1.PodQOSClass]*CgroupConfig) {
	setUnified := func(qos v1.PodQOSClass, key string, value int64) {
		if configs[qos].ResourceParameters.Unified == nil {
			configs[qos].ResourceParameters.Unified = make(map[string]string)
		}
		configs[qos].ResourceParameters.Unified[key] = strconv.FormatInt(value, 10)
		logger.V(4).Info("MemoryQoS config for qos", "qos", qos, "key", key, "value", value)
	}

	// 生产环境中唯一的调用方已限定为 TieredReservation 策略，此分支实际不可达，
	// 保留它只是为了覆盖单元测试场景。
	if m.memoryReservationPolicy != kubeletconfig.TieredReservationMemoryReservationPolicy {
		setUnified(v1.PodQOSGuaranteed, Cgroup2MemoryMin, 0)
		setUnified(v1.PodQOSGuaranteed, Cgroup2MemoryLow, 0)
		setUnified(v1.PodQOSBurstable, Cgroup2MemoryLow, 0)
		kubeletmetrics.MemoryQoSNodeMemoryMinBytes.Set(0)
		kubeletmetrics.MemoryQoSNodeMemoryLowBytes.Set(0)
		return
	}

	qosMemoryRequests := m.getQoSMemoryRequests()

	burstableRequests := qosMemoryRequests[v1.PodQOSBurstable]
	guaranteedRequests := qosMemoryRequests[v1.PodQOSGuaranteed]

	kubeletmetrics.MemoryQoSNodeMemoryMinBytes.Set(float64(guaranteedRequests))
	kubeletmetrics.MemoryQoSNodeMemoryLowBytes.Set(float64(burstableRequests))

	// 根 cgroup（kubepods.slice）需要覆盖两条保护链的祖先层级；
	// 这里以 Guaranteed 作为映射键，实际指向根 cgroup，而非逐 pod 的配置。
	setUnified(v1.PodQOSGuaranteed, Cgroup2MemoryMin, guaranteedRequests+burstableRequests)
	setUnified(v1.PodQOSGuaranteed, Cgroup2MemoryLow, burstableRequests)

	setUnified(v1.PodQOSBurstable, Cgroup2MemoryLow, burstableRequests)
}

// UpdateCgroups 同步各 QoS 层级 cgroup 的配置：更新 CPU 份额与巨页上限，
// 按策略更新内存保护与内存保留限额，最后把配置写入 cgroup。
func (m *qosContainerManagerImpl) UpdateCgroups(logger klog.Logger) error {
	// 整个同步过程会读写共享状态，必须串行化，避免与并发更新竞争。
	m.Lock()
	defer m.Unlock()

	qosConfigs := map[v1.PodQOSClass]*CgroupConfig{
		v1.PodQOSGuaranteed: {
			Name:               m.qosContainersInfo.Guaranteed,
			ResourceParameters: &ResourceConfig{},
		},
		v1.PodQOSBurstable: {
			Name:               m.qosContainersInfo.Burstable,
			ResourceParameters: &ResourceConfig{},
		},
		v1.PodQOSBestEffort: {
			Name:               m.qosContainersInfo.BestEffort,
			ResourceParameters: &ResourceConfig{},
		},
	}

	// 更新 QoS 层级 cgroup 的 CPU 份额设置。
	if err := m.setCPUCgroupConfig(qosConfigs); err != nil {
		return err
	}

	// 更新 QoS 层级 cgroup 的巨页设置，确保其始终保持不设上限。
	if err := m.setHugePagesConfig(qosConfigs); err != nil {
		return err
	}

	// 仅当 TieredReservation 策略生效时，才周期性更新 cgroup v2 的 memory.min/memory.low，
	// 因为这些取值随 pod 增减而变化。None 策略与特性关闭时的回退场景已在启动时处理，
	// 避免不必要的 systemd SetUnitProperties 调用——它会顺带重置其他 cgroup 属性。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) && libcontainercgroups.IsCgroup2UnifiedMode() &&
		m.memoryReservationPolicy == kubeletconfig.TieredReservationMemoryReservationPolicy {
		m.setMemoryQoS(logger, qosConfigs)
	}

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.QOSReserved) {
		for resource, percentReserve := range m.qosReserved {
			switch resource {
			case v1.ResourceMemory:
				m.setMemoryReserve(logger, qosConfigs, percentReserve)
			}
		}

		updateSuccess := true
		for _, config := range qosConfigs {
			err := m.cgroupManager.Update(logger, config)
			if err != nil {
				updateSuccess = false
			}
		}
		if updateSuccess {
			logger.V(4).Info("Updated QoS cgroup configuration")
			return nil
		}

		// 若某类资源能够通过调整 ResourceConfig 提高成功率，在这里先做一次调整；
		// 否则下一轮 Update() 会带着相同的值再次被调用。
		for resource, percentReserve := range m.qosReserved {
			switch resource {
			case v1.ResourceMemory:
				m.retrySetMemoryReserve(logger, qosConfigs, percentReserve)
			}
		}
	}

	for _, config := range qosConfigs {
		err := m.cgroupManager.Update(logger, config)
		if err != nil {
			logger.Error(err, "Failed to update QoS cgroup configuration")
			return err
		}
	}

	logger.V(4).Info("Updated QoS cgroup configuration")
	return nil
}

// qosContainerManagerNoop 是未开启 CgroupsPerQOS 时使用的空实现，所有操作都不做实际处理。
type qosContainerManagerNoop struct {
	cgroupRoot CgroupName
}

// 编译期断言：qosContainerManagerNoop 实现了 QOSContainerManager 接口。
var _ QOSContainerManager = &qosContainerManagerNoop{}

// GetQOSContainersInfo 返回空的 QoS 容器信息。
func (m *qosContainerManagerNoop) GetQOSContainersInfo() QOSContainersInfo {
	return QOSContainersInfo{}
}

// Start 不创建任何 cgroup，直接返回成功。
func (m *qosContainerManagerNoop) Start(_ context.Context, _ func() v1.ResourceList, _ ActivePodsFunc) error {
	return nil
}

// UpdateCgroups 不更新任何 cgroup，直接返回成功。
func (m *qosContainerManagerNoop) UpdateCgroups(logger klog.Logger) error {
	return nil
}
