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
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	libcontainercgroups "github.com/opencontainers/cgroups"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

const (
	// podCgroupNamePrefix 是 Pod cgroup 名称的前缀，后接 Pod UID。
	podCgroupNamePrefix = "pod"
)

// podContainerManagerImpl 实现 podContainerManager 接口。
// 这是通用实现：当 QoS cgroup 层级启用时，可在 Pod 级别管理 cgroup。
type podContainerManagerImpl struct {
	// qosContainersInfo 保存顶层 QoS 容器的绝对路径。
	qosContainersInfo QOSContainersInfo
	// subsystems 保存已挂载的 cgroup 子系统。
	subsystems *CgroupSubsystems
	// cgroupManager 是负责管理所有 Pod cgroup 的管理器。
	cgroupManager CgroupManager
	// podPidsLimit 是单个 Pod 中允许的最大 PID 数量。
	podPidsLimit int64
	// enforceCPULimits 控制是否启用 CFS 配额。
	enforceCPULimits bool
	// cpuCFSQuotaPeriod 是节点级别的 CFS 周期值（cfs_period_us，单位微秒）。
	cpuCFSQuotaPeriod uint64
	// podContainerManager 是运行在节点上的 ContainerManager。
	podContainerManager ContainerManager
	// memoryReservationPolicy 控制内存预留的保护策略。
	memoryReservationPolicy kubeletconfig.MemoryReservationPolicy
	// memoryThrottlingFactor 用于计算 Pod 级别的 memory.high。
	memoryThrottlingFactor *float64
}

// 确保 podContainerManagerImpl 实现了 PodContainerManager 接口。
var _ PodContainerManager = &podContainerManagerImpl{}

// Exists 检查 Pod 的 cgroup 是否已存在。
func (m *podContainerManagerImpl) Exists(pod *v1.Pod) bool {
	podContainerName, _ := m.GetPodContainerName(pod)
	return m.cgroupManager.Exists(podContainerName)
}

// EnsureExists 确保 Pod 的 cgroup 存在（在 QoS cgroup 层级启用时）；
// 若 Pod 级别的容器尚不存在则创建它。
func (m *podContainerManagerImpl) EnsureExists(logger klog.Logger, pod *v1.Pod) error {
	// 先检查 cgroup 是否已存在，避免重复创建。
	alreadyExists := m.Exists(pod)
	if !alreadyExists {
		enforceCPULimits := m.enforceCPULimits
		// 使用独占 CPU 的 Pod 无需 CFS 配额限制，否则会与其独占语义冲突。
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DisableCPUQuotaWithExclusiveCPUs) && m.podContainerManager.PodHasExclusiveCPUs(logger, pod) {
			logger.V(2).Info("Disabled CFS quota", "pod", klog.KObj(pod))
			enforceCPULimits = false
		}
		// 仅当 MemoryQoS 门控开启且系统使用 cgroup v2 时才启用内存 QoS。
		enforceMemoryQoS := false
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) &&
			libcontainercgroups.IsCgroup2UnifiedMode() {
			enforceMemoryQoS = true
		}
		// 创建 Pod 级别的 cgroup。
		podContainerName, _ := m.GetPodContainerName(pod)
		containerConfig := &CgroupConfig{
			Name:               podContainerName,
			ResourceParameters: ResourceConfigForPod(pod, enforceCPULimits, m.cpuCFSQuotaPeriod, enforceMemoryQoS, m.memoryReservationPolicy),
		}
		if m.podPidsLimit > 0 {
			containerConfig.ResourceParameters.PidsLimit = &m.podPidsLimit
		}
		if enforceMemoryQoS {
			m.applyPodLevelMemoryHigh(pod, containerConfig.ResourceParameters)
			logger.V(4).Info("MemoryQoS config for pod", "pod", klog.KObj(pod), "unified", containerConfig.ResourceParameters.Unified)
		}
		if err := m.cgroupManager.Create(logger, containerConfig); err != nil {
			return fmt.Errorf("failed to create container for %v : %v", podContainerName, err)
		}

	}
	return nil
}

// applyPodLevelMemoryHigh 在 Pod cgroup 上设置 memory.high。
// 内核会沿层级逐级执行 memory.high（try_charge_memcg 会遍历祖先节点），
// 因此只需在 Pod 级别设置即可限制 Pod 内所有容器，无需为每个容器单独设置。
func (m *podContainerManagerImpl) applyPodLevelMemoryHigh(pod *v1.Pod, rc *ResourceConfig) {
	if m.memoryThrottlingFactor != nil {
		ApplyPodLevelMemoryHigh(pod, rc, *m.memoryThrottlingFactor)
	}
}

// GetPodContainerName 返回 Pod 的 CgroupName 标识及其在主机上的字面 cgroupfs 形式。
func (m *podContainerManagerImpl) GetPodContainerName(pod *v1.Pod) (CgroupName, string) {
	podQOS := v1qos.GetPodQOS(pod)
	// 根据 Pod 的 QoS 等级选择对应的父级 QoS 容器。
	var parentContainer CgroupName
	switch podQOS {
	case v1.PodQOSGuaranteed:
		parentContainer = m.qosContainersInfo.Guaranteed
	case v1.PodQOSBurstable:
		parentContainer = m.qosContainersInfo.Burstable
	case v1.PodQOSBestEffort:
		parentContainer = m.qosContainersInfo.BestEffort
	}
	podContainer := GetPodCgroupNameSuffix(pod.UID)

	// 计算 cgroup 的绝对路径。
	cgroupName := NewCgroupName(parentContainer, podContainer)
	// 获取字面 cgroupfs 名称。
	cgroupfsName := m.cgroupManager.Name(cgroupName)

	return cgroupName, cgroupfsName
}

// GetPodCgroupMemoryUsage 返回 Pod cgroup 的当前内存使用量（字节）。
func (m *podContainerManagerImpl) GetPodCgroupMemoryUsage(pod *v1.Pod) (uint64, error) {
	podCgroupName, _ := m.GetPodContainerName(pod)
	memUsage, err := m.cgroupManager.MemoryUsage(podCgroupName)
	if err != nil {
		return 0, err
	}
	return uint64(memUsage), nil
}

// GetPodCgroupConfig 返回 Pod cgroup 在指定资源上的当前配置。
func (m *podContainerManagerImpl) GetPodCgroupConfig(pod *v1.Pod, resource v1.ResourceName) (*ResourceConfig, error) {
	podCgroupName, _ := m.GetPodContainerName(pod)
	return m.cgroupManager.GetCgroupConfig(podCgroupName, resource)
}

// SetPodCgroupConfig 更新 Pod cgroup 的资源限制配置。
func (m *podContainerManagerImpl) SetPodCgroupConfig(logger klog.Logger, pod *v1.Pod, resourceConfig *ResourceConfig) error {
	podCgroupName, _ := m.GetPodContainerName(pod)
	return m.cgroupManager.SetCgroupConfig(logger, podCgroupName, resourceConfig)
}

// killOnePid 向指定 PID 发送 kill 信号。
func (m *podContainerManagerImpl) killOnePid(logger klog.Logger, pid int) error {
	// 在 POSIX 系统上 os.FindProcess 不会返回错误。
	// 参考 https://go-review.googlesource.com/c/go/+/19093
	p, _ := os.FindProcess(pid)
	if err := p.Kill(); err != nil {
		// 进程已经退出属于正常情况。
		if errors.Is(err, os.ErrProcessDone) {
			logger.V(3).Info("Process no longer exists", "pid", pid)
			return nil
		}
		return err
	}
	return nil
}

// tryKillingCgroupProcesses 扫描整个 cgroup 目录，
// 杀掉所有直接或间接挂在 Pod cgroup 下的进程。
func (m *podContainerManagerImpl) tryKillingCgroupProcesses(logger klog.Logger, podCgroup CgroupName) error {
	pidsToKill := m.cgroupManager.Pids(logger, podCgroup)
	// 已终止的 Pod cgroup 中没有 PID 时直接返回。
	if len(pidsToKill) == 0 {
		return nil
	}

	var errlist []error
	// os.Kill 可能频繁出错，因此对全部 PID 重试多次。
	removed := map[int]bool{}
	for i := 0; i < 5; i++ {
		if i != 0 {
			logger.V(3).Info("Attempt failed to kill all unwanted process from cgroup, retrying", "attempt", i, "cgroupName", podCgroup)
		}
		errlist = []error{}
		for _, pid := range pidsToKill {
			if _, ok := removed[pid]; ok {
				continue
			}
			logger.V(3).Info("Attempting to kill process from cgroup", "pid", pid, "cgroupName", podCgroup)
			if err := m.killOnePid(logger, pid); err != nil {
				logger.V(3).Info("Failed to kill process from cgroup", "pid", pid, "cgroupName", podCgroup, "err", err)
				errlist = append(errlist, err)
			} else {
				removed[pid] = true
			}
		}
		if len(errlist) == 0 {
			logger.V(3).Info("Successfully killed all unwanted processes from cgroup", "cgroupName", podCgroup)
			return nil
		}
	}
	return utilerrors.NewAggregate(errlist)
}

// Destroy 销毁 Pod 容器的 cgroup 路径。
func (m *podContainerManagerImpl) Destroy(logger klog.Logger, podCgroup CgroupName) error {
	// 先杀掉 Pod cgroup 下的所有进程，否则 cgroup 无法删除。
	if err := m.tryKillingCgroupProcesses(logger, podCgroup); err != nil {
		logger.Info("Failed to kill all the processes attached to cgroup", "cgroupName", podCgroup, "err", err)
		return fmt.Errorf("failed to kill all the processes attached to the %v cgroups : %v", podCgroup, err)
	}

	// 进程清理完成后才能安全删除 Pod cgroup。
	containerConfig := &CgroupConfig{
		Name:               podCgroup,
		ResourceParameters: &ResourceConfig{},
	}
	if err := m.cgroupManager.Destroy(logger, containerConfig); err != nil {
		logger.Info("Failed to delete cgroup paths", "cgroupName", podCgroup, "err", err)
		return fmt.Errorf("failed to delete cgroup paths for %v : %v", podCgroup, err)
	}
	return nil
}

// ReduceCPULimits 将 CPU CFS 配额降到最小份额。
func (m *podContainerManagerImpl) ReduceCPULimits(logger klog.Logger, podCgroup CgroupName) error {
	return m.cgroupManager.ReduceCPULimits(logger, podCgroup)
}

// IsPodCgroup 判断字面 cgroupfs 名称是否对应一个 Pod，并返回其 UID。
func (m *podContainerManagerImpl) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	// 把字面 cgroupfs 形式转换为驱动内部使用的名称。
	cgroupName := m.cgroupManager.CgroupName(cgroupfs)
	qosContainersList := [3]CgroupName{m.qosContainersInfo.BestEffort, m.qosContainersInfo.Burstable, m.qosContainersInfo.Guaranteed}
	basePath := ""
	for _, qosContainerName := range qosContainersList {
		// Pod cgroup 是 QoS 节点的直接子节点，据此判断是否匹配。
		if len(cgroupName) == len(qosContainerName)+1 {
			basePath = cgroupName[len(qosContainerName)]
		}
	}
	if basePath == "" {
		return false, types.UID("")
	}
	if !strings.HasPrefix(basePath, podCgroupNamePrefix) {
		return false, types.UID("")
	}
	parts := strings.Split(basePath, podCgroupNamePrefix)
	if len(parts) != 2 {
		return false, types.UID("")
	}
	return true, types.UID(parts[1])
}

// GetAllPodsFromCgroups 扫描所有 Pod cgroup 子系统，
// 返回 cgroup 挂载点上仍然存在的 Pod 列表。
func (m *podContainerManagerImpl) GetAllPodsFromCgroups(logger klog.Logger) (map[types.UID]CgroupName, error) {
	// 保存磁盘上发现的所有 Pod。
	foundPods := make(map[types.UID]CgroupName)
	qosContainersList := [3]CgroupName{m.qosContainersInfo.BestEffort, m.qosContainersInfo.Burstable, m.qosContainersInfo.Guaranteed}
	// 遍历所有子系统挂载点及其下的各 QoS cgroup 目录。
	// 只要 Pod cgroup 在任一子系统挂载中存在，就会尝试删除它。
	for _, val := range m.subsystems.MountPoints {
		for _, qosContainerName := range qosContainersList {
			// 获取子系统 QoS cgroup 的绝对名称。
			qcConversion := m.cgroupManager.Name(qosContainerName)
			qc := path.Join(val, qcConversion)
			dirInfo, err := os.ReadDir(qc)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("failed to read the cgroup directory %v : %v", qc, err)
			}
			for i := range dirInfo {
				// 不是目录则跳过。
				if !dirInfo[i].IsDir() {
					continue
				}
				// 把具体的 cgroupfs 名称转换回内部标识。
				// 这是为了兼容 systemd 环境的路径编码：systemd 会在每一段中编码路径，
				// 因此必须传入完整路径才能正确解码。
				cgroupfsPath := path.Join(qcConversion, dirInfo[i].Name())
				internalPath := m.cgroupManager.CgroupName(cgroupfsPath)
				// 这里只关心转换后路径的最后一段，因为正是当前读取的这一层
				// 决定它是否是 Pod。
				basePath := internalPath[len(internalPath)-1]
				if !strings.Contains(basePath, podCgroupNamePrefix) {
					continue
				}
				// 再按 pod 前缀切分名称以提取 UID。
				parts := strings.Split(basePath, podCgroupNamePrefix)
				// 缺少 UID 说明该 cgroup 不符合 pod<uid> 形式，记录日志并跳过。
				if len(parts) != 2 {
					logger.Info("Pod cgroup manager ignored unexpected cgroup because it is not a pod", "path", cgroupfsPath)
					continue
				}
				podUID := parts[1]
				foundPods[types.UID(podUID)] = internalPath
			}
		}
	}
	return foundPods, nil
}

// podContainerManagerNoop 实现 podContainerManager 接口，但几乎不做任何事。
// 当 QoS cgroup 层级未启用时使用该实现：
// 此时 Exists() 始终返回 true，因为 cgroupRoot 被假定总是存在。
type podContainerManagerNoop struct {
	cgroupRoot CgroupName
}

// 确保 podContainerManagerNoop 实现了 PodContainerManager 接口。
var _ PodContainerManager = &podContainerManagerNoop{}

// Exists 在 no-op 实现中始终返回 true。
func (m *podContainerManagerNoop) Exists(_ *v1.Pod) bool {
	return true
}

// EnsureExists 在 no-op 实现中不执行任何操作。
func (m *podContainerManagerNoop) EnsureExists(_ klog.Logger, _ *v1.Pod) error {
	return nil
}

// GetPodContainerName 返回 cgroupRoot 作为 Pod 容器名。
func (m *podContainerManagerNoop) GetPodContainerName(_ *v1.Pod) (CgroupName, string) {
	return m.cgroupRoot, ""
}

// GetPodContainerNameForDriver 在 no-op 实现中返回空字符串。
func (m *podContainerManagerNoop) GetPodContainerNameForDriver(_ *v1.Pod) string {
	return ""
}

// Destroy 销毁 Pod 容器的 cgroup 路径。
func (m *podContainerManagerNoop) Destroy(_ klog.Logger, _ CgroupName) error {
	return nil
}

// ReduceCPULimits 在 no-op 实现中不执行任何操作。
func (m *podContainerManagerNoop) ReduceCPULimits(_ klog.Logger, _ CgroupName) error {
	return nil
}

// GetAllPodsFromCgroups 在 no-op 实现中返回 nil。
func (m *podContainerManagerNoop) GetAllPodsFromCgroups(_ klog.Logger) (map[types.UID]CgroupName, error) {
	return nil, nil
}

// IsPodCgroup 在 no-op 实现中始终返回 false。
func (m *podContainerManagerNoop) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	return false, types.UID("")
}

// GetPodCgroupMemoryUsage 在 no-op 实现中返回 0。
func (m *podContainerManagerNoop) GetPodCgroupMemoryUsage(_ *v1.Pod) (uint64, error) {
	return 0, nil
}

// GetPodCgroupConfig 在 no-op 实现中返回 nil。
func (m *podContainerManagerNoop) GetPodCgroupConfig(_ *v1.Pod, _ v1.ResourceName) (*ResourceConfig, error) {
	return nil, nil
}

// SetPodCgroupConfig 在 no-op 实现中不执行任何操作。
func (m *podContainerManagerNoop) SetPodCgroupConfig(_ klog.Logger, _ *v1.Pod, _ *ResourceConfig) error {
	return nil
}
