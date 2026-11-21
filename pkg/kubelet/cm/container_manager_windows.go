//go:build windows

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

// containerManagerImpl 是 Windows 上的容器管理器实现。
// 目前仅实现了 GetNodeAllocatableReservation() 与 GetCapacity() 等部分能力，
// 其余方法多为空实现或直接返回零值。

package cm

import (
	"context"
	"fmt"
	"sync"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	pluginwatcherapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// containerManagerImpl 保存 Windows 容器管理器所需的状态与依赖。
type containerManagerImpl struct {
	// 本节点的资源容量。
	capacity v1.ResourceList
	// cAdvisor 接口。
	cadvisorInterface cadvisor.Interface
	// 本节点的配置。
	nodeConfig NodeConfig
	// 用于导出与分配设备插件上报资源的接口。
	deviceManager devicemanager.Manager
	// 拓扑资源协调接口。
	topologyManager topologymanager.Manager
	cpuManager      cpumanager.Manager
	memoryManager   memorymanager.Manager
	nodeInfo        *v1.Node
	sync.RWMutex
}

// Start 启动 Windows 容器管理器：初始化容量信息并启动 CPU、内存与设备管理器。
func (cm *containerManagerImpl) Start(ctx context.Context, node *v1.Node,
	activePods ActivePodsFunc,
	getNode GetNodeFunc,
	sourcesReady config.SourcesReady,
	podStatusProvider status.PodStatusProvider,
	runtimeService internalapi.RuntimeService,
	localStorageCapacityIsolation bool) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Starting Windows container manager")

	cm.nodeInfo = node

	// 启用本地存储容量隔离时，用根文件系统信息补充临时存储容量。
	if localStorageCapacityIsolation {
		rootfs, err := cm.cadvisorInterface.RootFsInfo()
		if err != nil {
			return fmt.Errorf("failed to get rootfs info: %v", err)
		}
		for rName, rCap := range cadvisor.EphemeralStorageCapacityFromFsInfo(rootfs) {
			cm.capacity[rName] = rCap
		}
	}

	// 从运行时收集容器映射与运行中容器集合，供各管理器初始化使用。
	containerMap, containerRunningSet := buildContainerMapAndRunningSetFromRuntime(ctx, runtimeService)

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WindowsCPUAndMemoryAffinity) {
		err := cm.cpuManager.Start(ctx, cpumanager.ActivePodsFunc(activePods), sourcesReady, podStatusProvider, runtimeService, containerMap.Clone())
		if err != nil {
			return fmt.Errorf("start cpu manager error: %v", err)
		}

		// 初始化内存管理器。
		err = cm.memoryManager.Start(ctx, memorymanager.ActivePodsFunc(activePods), sourcesReady, podStatusProvider, runtimeService, containerMap.Clone())
		if err != nil {
			return fmt.Errorf("start memory manager error: %v", err)
		}
	}

	// 启动设备管理器。
	if err := cm.deviceManager.Start(logger, devicemanager.ActivePodsFunc(activePods), sourcesReady, containerMap.Clone(), containerRunningSet); err != nil {
		return err
	}

	return nil
}

// NewContainerManager 创建 Windows 容器管理器。
func NewContainerManager(ctx context.Context, mountUtil mount.Interface, cadvisorInterface cadvisor.Interface, nodeConfig NodeConfig, failSwapOn bool, recorder record.EventRecorder, kubeClient clientset.Interface) (ContainerManager, error) {
	logger := klog.FromContext(ctx)
	// 在逻辑初始化 cAdvisor 之前调用 `MachineInfo` 是安全的，
	// 因为机器信息在创建 cAdvisor 对象时就已计算并缓存。
	// 但 `RootFsInfo` 与 `ImagesFsInfo` 此时不可用，需等管理器启动后再获取。
	machineInfo, err := cadvisorInterface.MachineInfo(logger)
	if err != nil {
		return nil, err
	}
	capacity := cadvisor.CapacityFromMachineInfo(machineInfo)

	cm := &containerManagerImpl{
		capacity:          capacity,
		nodeConfig:        nodeConfig,
		cadvisorInterface: cadvisorInterface,
	}

	// 默认使用 fake 实现；仅当 WindowsCPUAndMemoryAffinity 门控开启时才替换为真实管理器。
	cm.topologyManager = topologymanager.NewFakeManager(logger)
	cm.cpuManager = cpumanager.NewFakeManager(logger)
	cm.memoryManager = memorymanager.NewFakeManager(logger)

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WindowsCPUAndMemoryAffinity) {
		logger.Info("Creating topology manager")
		cm.topologyManager, err = topologymanager.NewManager(
			logger,
			machineInfo.Topology,
			nodeConfig.TopologyManagerPolicy,
			nodeConfig.TopologyManagerScope,
			nodeConfig.TopologyManagerPolicyOptions)
		if err != nil {
			logger.Error(err, "Failed to initialize topology manager")
			return nil, err
		}

		logger.Info("Creating cpu manager")
		cm.cpuManager, err = cpumanager.NewManager(
			logger,
			nodeConfig.CPUManagerPolicy,
			nodeConfig.CPUManagerPolicyOptions,
			nodeConfig.CPUManagerReconcilePeriod,
			machineInfo,
			nodeConfig.NodeAllocatableConfig.ReservedSystemCPUs,
			cm.GetNodeAllocatableReservation(),
			nodeConfig.KubeletRootDir,
			cm.topologyManager,
		)
		if err != nil {
			logger.Error(err, "Failed to initialize cpu manager")
			return nil, err
		}
		cm.topologyManager.AddHintProvider(logger, cm.cpuManager)

		logger.Info("Creating memory manager")
		// Windows 上的内存亲和性是尽力而为的，没有类似 cpuset.mems 的机制
		// 直接把内存绑定到 NUMA 节点。因此采用“内存跟随 CPU”的尽力而为设计：
		// 包装拓扑管理器的存储，使内存管理器的 GetAffinity 返回容器独占 CPU
		// 所属的 NUMA 节点。这依赖上面的 CPU 管理器已注册为提示提供者，
		// 从而保证其 Allocate 先执行、CPU 已提交后再读取亲和性。
		memoryAffinity := newCPUFollowingStore(cm.topologyManager, cm.cpuManager, machineInfo)
		cm.memoryManager, err = memorymanager.NewManager(
			logger,
			nodeConfig.MemoryManagerPolicy,
			machineInfo,
			cm.GetNodeAllocatableReservation(),
			nodeConfig.MemoryManagerReservedMemory,
			nodeConfig.KubeletRootDir,
			memoryAffinity,
		)
		if err != nil {
			logger.Error(err, "Failed to initialize memory manager")
			return nil, err
		}
		cm.topologyManager.AddHintProvider(logger, cm.memoryManager)
	}

	// 创建设备插件管理器，并注册为拓扑提示提供者。
	logger.Info("Creating device plugin manager")
	cm.deviceManager, err = devicemanager.NewManagerImpl(logger, nil, cm.topologyManager)
	if err != nil {
		return nil, err
	}
	cm.topologyManager.AddHintProvider(logger, cm.deviceManager)

	return cm, nil
}

// SystemCgroupsLimit 在 Windows 上返回空资源列表（不支持 system cgroups）。
func (cm *containerManagerImpl) SystemCgroupsLimit() v1.ResourceList {
	return v1.ResourceList{}
}

// GetNodeConfig 返回节点的容器管理器配置。
func (cm *containerManagerImpl) GetNodeConfig() NodeConfig {
	cm.RLock()
	defer cm.RUnlock()
	return cm.nodeConfig
}

// GetMountedSubsystems 在 Windows 上返回空的 cgroup 子系统信息。
func (cm *containerManagerImpl) GetMountedSubsystems() *CgroupSubsystems {
	return &CgroupSubsystems{}
}

// GetQOSContainersInfo 在 Windows 上返回空的 QoS 容器信息。
func (cm *containerManagerImpl) GetQOSContainersInfo() QOSContainersInfo {
	return QOSContainersInfo{}
}

// UpdateQOSCgroups 在 Windows 上为空操作。
func (cm *containerManagerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	return nil
}

// Status 在 Windows 上返回空的管理器状态。
func (cm *containerManagerImpl) Status() Status {
	return Status{}
}

// GetNodeAllocatableReservation 返回节点为系统、Kubernetes 组件与驱逐预留的资源总量。
func (cm *containerManagerImpl) GetNodeAllocatableReservation() v1.ResourceList {
	evictionReservation := hardEvictionReservation(cm.nodeConfig.HardEvictionThresholds, cm.capacity)
	result := make(v1.ResourceList)
	for k := range cm.capacity {
		value := resource.NewQuantity(0, resource.DecimalSI)
		if cm.nodeConfig.SystemReserved != nil {
			value.Add(cm.nodeConfig.SystemReserved[k])
		}
		if cm.nodeConfig.KubeReserved != nil {
			value.Add(cm.nodeConfig.KubeReserved[k])
		}
		if evictionReservation != nil {
			value.Add(evictionReservation[k])
		}
		if !value.IsZero() {
			result[k] = *value
		}
	}
	return result
}

// GetCapacity 返回节点的资源容量。
func (cm *containerManagerImpl) GetCapacity(_ klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList {
	return cm.capacity
}

// GetPluginRegistrationHandlers 返回设备插件注册处理器；Windows 不支持 DRA。
func (cm *containerManagerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	// Windows 不支持 DRA，仅支持设备插件。
	return map[string]cache.PluginHandler{pluginwatcherapi.DevicePlugin: cm.deviceManager.GetWatcherHandler()}
}

// GetHealthCheckers 返回设备管理器的健康检查器。
func (cm *containerManagerImpl) GetHealthCheckers() []healthz.HealthChecker {
	return []healthz.HealthChecker{cm.deviceManager.GetHealthChecker()}
}

// GetDevicePluginResourceCapacity 返回设备插件上报的资源容量与可分配量。
func (cm *containerManagerImpl) GetDevicePluginResourceCapacity(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
	return cm.deviceManager.GetCapacity(logger)
}

// NewPodContainerManager 返回 Windows 上的 Pod 容器管理器空实现。
func (cm *containerManagerImpl) NewPodContainerManager() PodContainerManager {
	return &podContainerManagerStub{}
}

// GetResources 返回容器运行所需的设备、挂载、环境变量与注解。
func (cm *containerManagerImpl) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	opts := &kubecontainer.RunContainerOptions{}
	// Allocate 应已在 predicateAdmitHandler.Admit() 中调用过，
	// 这里只需从缓存状态中读取设备运行时信息。
	devOpts, err := cm.deviceManager.GetDeviceRunContainerOptions(ctx, pod, container)
	if err != nil {
		return nil, err
	} else if devOpts == nil {
		return opts, nil
	}
	opts.Devices = append(opts.Devices, devOpts.Devices...)
	opts.Mounts = append(opts.Mounts, devOpts.Mounts...)
	opts.Envs = append(opts.Envs, devOpts.Envs...)
	opts.Annotations = append(opts.Annotations, devOpts.Annotations...)
	return opts, nil
}

// UpdateAllocatedResourcesStatus 更新 Pod 中已分配资源的状态（Windows 目前仅支持设备插件）。
func (cm *containerManagerImpl) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {
	// 目前仅支持设备插件。

	cm.deviceManager.UpdateAllocatedResourcesStatus(logger, pod, status)

	// TODO(SergeyKanzhelev, https://kep.k8s.io/4680): DRA 支持 Windows 后补充 DRA 资源支持。
}

// Updates 返回资源更新事件通道；Windows 目前仅转发设备插件更新。
func (cm *containerManagerImpl) Updates() <-chan resourceupdates.Update {
	// TODO(SergeyKanzhelev, https://kep.k8s.io/4680): 补充 DRA 资源支持，目前仅转发设备插件更新。
	return cm.deviceManager.Updates()
}

// UpdatePluginResources 将设备插件资源更新到 NodeInfo 中。
func (cm *containerManagerImpl) UpdatePluginResources(node *schedulerframework.NodeInfo, attrs *lifecycle.PodAdmitAttributes) error {
	return cm.deviceManager.UpdatePluginResources(node, attrs)
}

// InternalContainerLifecycle 返回内部容器生命周期实现。
func (cm *containerManagerImpl) InternalContainerLifecycle(_ klog.Logger) InternalContainerLifecycle {
	return &internalContainerLifecycleImpl{cm.cpuManager, cm.memoryManager, cm.topologyManager}
}

// GetPodCgroupRoot 在 Windows 上返回空字符串（无 cgroup 根路径）。
func (cm *containerManagerImpl) GetPodCgroupRoot() string {
	return ""
}

// GetDevices 返回指定容器分配到的设备。
func (cm *containerManagerImpl) GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices {
	return containerDevicesFromResourceDeviceInstances(cm.deviceManager.GetDevices(podUID, containerName))
}

// GetAllocatableDevices 在 Windows 上暂不支持，返回 nil。
func (cm *containerManagerImpl) GetAllocatableDevices(_ klog.Logger) []*podresourcesapi.ContainerDevices {
	return nil
}

// ShouldResetExtendedResourceCapacity 返回是否需要重置扩展资源容量。
func (cm *containerManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	return cm.deviceManager.ShouldResetExtendedResourceCapacity()
}

// GetAllocateResourcesPodAdmitHandler 返回资源分配准入处理器（拓扑管理器）。
func (cm *containerManagerImpl) GetAllocateResourcesPodAdmitHandler(_ klog.Logger) lifecycle.PodAdmitHandler {
	return cm.topologyManager
}

// UpdateAllocatedDevices 在 Windows 上为空操作。
func (cm *containerManagerImpl) UpdateAllocatedDevices(_ klog.Logger) {
	return
}

// GetCPUs 返回指定容器独占的 CPU 列表。
func (cm *containerManagerImpl) GetCPUs(pod *v1.Pod, container *v1.Container) []int64 {
	// 未开启 WindowsCPUAndMemoryAffinity 时不提供 CPU 亲和性信息。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WindowsCPUAndMemoryAffinity) {
		if cm.cpuManager != nil {
			return int64Slice(cm.cpuManager.GetExclusiveCPUs(string(pod.UID), container.Name).UnsortedList())
		}
		return []int64{}
	}
	return nil
}

// GetPodCPUs 在 Windows 上暂不支持，返回 nil。
func (cm *containerManagerImpl) GetPodCPUs(podUID string) []int64 {
	return nil
}

// GetAllocatableCPUs 返回节点可分配的 CPU 列表。
// GetAllocatableCPUs 返回节点可分配的 CPU 列表。
func (cm *containerManagerImpl) GetAllocatableCPUs() []int64 {
	// 未开启 WindowsCPUAndMemoryAffinity 时不提供可分配 CPU 信息。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WindowsCPUAndMemoryAffinity) {
		if cm.cpuManager != nil {
			return int64Slice(cm.cpuManager.GetAllocatableCPUs().UnsortedList())
		}
		return []int64{}
	}
	return nil
}

// GetMemory 在 Windows 上暂不支持，返回 nil。
func (cm *containerManagerImpl) GetMemory(_ klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetPodMemory 在 Windows 上暂不支持，返回 nil。
func (cm *containerManagerImpl) GetPodMemory(_ klog.Logger, _ string) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetAllocatableMemory 在 Windows 上暂不支持，返回 nil。
func (cm *containerManagerImpl) GetAllocatableMemory(_ klog.Logger) []*podresourcesapi.ContainerMemory {
	return nil
}

// GetNodeAllocatableAbsolute 在 Windows 上返回 nil。
func (cm *containerManagerImpl) GetNodeAllocatableAbsolute() v1.ResourceList {
	return nil
}

// GetDynamicResources 在 Windows 上暂不支持 DRA，返回 nil。
func (cm *containerManagerImpl) GetDynamicResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	return nil
}

// PrepareDynamicResources 在 Windows 上为空操作。
func (cm *containerManagerImpl) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// UnprepareDynamicResources 在 Windows 上为空操作。
func (cm *containerManagerImpl) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// PodMightNeedToUnprepareResources 在 Windows 上始终返回 false。
func (cm *containerManagerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	return false
}

// PodHasExclusiveCPUs 判断 Pod 是否使用了独占 CPU。
func (cm *containerManagerImpl) PodHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod) bool {
	return podHasExclusiveCPUs(logger, cm.cpuManager, pod)
}

// ContainerHasExclusiveCPUs 判断容器是否使用了独占 CPU。
func (cm *containerManagerImpl) ContainerHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) bool {
	return containerHasExclusiveCPUs(logger, cm.cpuManager, pod, container)
}
