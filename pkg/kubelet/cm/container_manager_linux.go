//go:build linux

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

// Package cm 实现 kubelet 的容器管理器：管理 cgroup 层级、系统容器、CPU/内存/设备/DRA 子管理器以及节点可分配资源。
package cm

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/opencontainers/cgroups"
	"github.com/opencontainers/cgroups/manager"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilpath "k8s.io/utils/path"

	inuserns "github.com/moby/sys/userns"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	resourcehelper "k8s.io/component-helpers/resource"
	internalapi "k8s.io/cri-api/pkg/apis"
	pluginwatcherapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager"
	memorymanagerstate "k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	cmutil "k8s.io/kubernetes/pkg/kubelet/cm/util"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/stats/pidlimit"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/kubelet/util/swap"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/util/oom"
)

// systemContainer 表示由 kubelet 跟踪的非用户容器（如系统预留 cgroup）。
type systemContainer struct {
	// name 是容器的绝对名称（cgroup 路径）。
	name string

	// cpuMillicores 是该容器的 CPU 限制（毫核）。
	cpuMillicores int64

	// ensureStateFunc 周期性保证容器状态符合预期。
	// 参数 m 是指定容器的 cgroup 管理器。
	ensureStateFunc func(m cgroups.Manager) error

	// manager 是外部容器的 cgroup 管理器。
	manager cgroups.Manager
}

// newSystemCgroups 为指定 cgroup 名称创建系统容器对象。
func newSystemCgroups(containerName string) (*systemContainer, error) {
	manager, err := createManager(containerName)
	if err != nil {
		return nil, err
	}
	return &systemContainer{
		name:    containerName,
		manager: manager,
	}, nil
}

// containerManagerImpl 是 ContainerManager 的 Linux 实现。
type containerManagerImpl struct {
	sync.RWMutex
	cadvisorInterface cadvisor.Interface
	mountUtil         mount.Interface
	NodeConfig
	status Status
	// systemContainers 是 kubelet 管理的系统容器列表。
	systemContainers []*systemContainer
	// periodicTasks 是周期性执行的任务列表。
	periodicTasks []func()
	// subsystems 保存所有已挂载的 cgroup 子系统。
	subsystems *CgroupSubsystems
	nodeInfo   *v1.Node
	// cgroupManager 提供 cgroup 管理接口。
	cgroupManager CgroupManager
	// capacity 是节点容量（不含内部资源）。
	capacity v1.ResourceList
	// internalCapacity 是包含内部资源（如 PID）的节点容量。
	internalCapacity v1.ResourceList
	// cgroupRoot 是 kubelet 放置所有 Pod 的 cgroupfs 绝对路径，
	// 其中包含用于强制 Node Allocatable 的顶层 cgroup。
	cgroupRoot CgroupName
	// recorder 用于上报事件。
	recorder record.EventRecorder
	// qosContainerManager 管理 QoS 层级 cgroup。
	qosContainerManager QOSContainerManager
	// deviceManager 负责设备插件上报设备的导出与分配。
	deviceManager devicemanager.Manager
	// cpuManager 负责 CPU 亲和性管理。
	cpuManager cpumanager.Manager
	// memoryManager 负责内存亲和性管理。
	memoryManager memorymanager.Manager
	// topologyManager 负责拓扑资源的协调。
	topologyManager topologymanager.Manager
	// draManager 是动态资源分配（DRA）的实现。
	draManager *dra.Manager
	// kubeClient 是访问 API server 的客户端；standalone 模式下可能为 nil。
	kubeClient clientset.Interface
	// resourceUpdates 汇总各子管理器的资源更新通知。
	resourceUpdates chan resourceupdates.Update
}

// features 记录系统验证发现的能力特性。
type features struct {
	cpuHardcapping bool
}

var _ ContainerManager = &containerManagerImpl{}

// validateSystemRequirements 检查必需的 cgroup 子系统是否已挂载。
// 目前要求 cpu、cpuacct、cpuset、memory 四个子系统。
// CPU 配额（cfs_period/quota）属于软性要求，缺失只影响硬限制能力。
func validateSystemRequirements(logger klog.Logger, mountUtil mount.Interface) (features, error) {
	const (
		cgroupMountType = "cgroup"
		localErr        = "system validation failed"
	)
	var (
		cpuMountPoint string
		f             features
	)
	mountPoints, err := mountUtil.List()
	if err != nil {
		return f, fmt.Errorf("%s - %v", localErr, err)
	}

	if cgroups.IsCgroup2UnifiedMode() {
		f.cpuHardcapping = true
		return f, nil
	}

	expectedCgroups := sets.New("cpu", "cpuacct", "cpuset", "memory")
	for _, mountPoint := range mountPoints {
		if mountPoint.Type == cgroupMountType {
			for _, opt := range mountPoint.Opts {
				if expectedCgroups.Has(opt) {
					expectedCgroups.Delete(opt)
				}
				if opt == "cpu" {
					cpuMountPoint = mountPoint.Path
				}
			}
		}
	}

	if expectedCgroups.Len() > 0 {
		return f, fmt.Errorf("%s - Following Cgroup subsystem not mounted: %v", localErr, sets.List(expectedCgroups))
	}

	// CPU cgroup 已确认挂载，因此可以直接检查配额文件。
	// 两个文件都存在才认为支持 CPU 硬限制。
	periodExists, err := utilpath.Exists(utilpath.CheckFollowSymlink, path.Join(cpuMountPoint, "cpu.cfs_period_us"))
	if err != nil {
		logger.Error(err, "Failed to detect if CPU cgroup cpu.cfs_period_us is available")
	}
	quotaExists, err := utilpath.Exists(utilpath.CheckFollowSymlink, path.Join(cpuMountPoint, "cpu.cfs_quota_us"))
	if err != nil {
		logger.Error(err, "Failed to detect if CPU cgroup cpu.cfs_quota_us is available")
	}
	if quotaExists && periodExists {
		f.cpuHardcapping = true
	}
	return f, nil
}

// TODO(vmarmol): 为系统容器增加资源限制。
// NewContainerManager 创建 Linux 容器管理器，并初始化各子管理器。
// 容器名为空表示不启用对应的系统容器。
func NewContainerManager(ctx context.Context, mountUtil mount.Interface, cadvisorInterface cadvisor.Interface, nodeConfig NodeConfig, failSwapOn bool, recorder record.EventRecorder, kubeClient clientset.Interface) (ContainerManager, error) {
	logger := klog.FromContext(ctx)

	subsystems, err := GetCgroupSubsystems()
	if err != nil {
		return nil, fmt.Errorf("failed to get mounted cgroup subsystems: %v", err)
	}

	isSwapOn, err := swap.IsSwapOn()
	if err != nil {
		return nil, fmt.Errorf("failed to determine if swap is on: %w", err)
	}

	// swap 开启时按配置决定失败或告警：tmpfs 的 noswap 选项缺失会使内存卷可能落盘。
	if isSwapOn {
		if failSwapOn {
			return nil, fmt.Errorf("running with swap on is not supported, please disable swap or set --fail-swap-on flag to false")
		}

		if !swap.IsTmpfsNoswapOptionSupported(mountUtil, nodeConfig.KubeletRootDir) {
			nodeRef := nodeRefFromNode(string(nodeConfig.NodeName))
			recorder.Event(nodeRef, v1.EventTypeWarning, events.PossibleMemoryBackedVolumesOnDisk,
				"The tmpfs noswap option is not supported. Memory-backed volumes (e.g. secrets, emptyDirs, etc.) "+
					"might be swapped to disk and should no longer be considered secure.",
			)
		}
	}

	// internalCapacity 在节点容量基础上补充 PID 等内部资源。
	var internalCapacity = v1.ResourceList{}
	// 此处可在 cAdvisor 逻辑初始化前调用 MachineInfo：
	// 机器信息在 cAdvisor 创建时已计算并缓存。
	// 但 RootFsInfo/ImagesFsInfo 此时不可用，稍后在 Start 中获取。
	machineInfo, err := cadvisorInterface.MachineInfo(logger)
	if err != nil {
		return nil, err
	}
	capacity := cadvisor.CapacityFromMachineInfo(machineInfo)
	for k, v := range capacity {
		internalCapacity[k] = v
	}
	pidlimits, err := pidlimit.Stats()
	if err == nil && pidlimits != nil && pidlimits.MaxPID != nil {
		internalCapacity[pidlimit.PIDs] = *resource.NewQuantity(
			int64(*pidlimits.MaxPID),
			resource.DecimalSI)
	}

	// 把 cgroupfs 路径形式的 CgroupRoot 转为内部 CgroupName。
	cgroupRoot := ParseCgroupfsToCgroupName(nodeConfig.CgroupRoot)
	cgroupManager := NewCgroupManager(logger, subsystems, nodeConfig.CgroupDriver)
	nodeConfig.CgroupVersion = cgroupManager.Version()
	// 启用 QoS 层级时，必须显式配置并校验 cgroup-root 存在。
	if nodeConfig.CgroupsPerQOS {
		// 虽然默认是 /，这里仍显式检查以防回归。
		if nodeConfig.CgroupRoot == "" {
			return nil, fmt.Errorf("invalid configuration: cgroups-per-qos was specified and cgroup-root was not specified. To enable the QoS cgroup hierarchy you need to specify a valid cgroup-root")
		}

		// 需要确认每个子系统下该 cgroup root 都存在；
		// 校验时固定使用 cgroupfs 驱动，
		// 因为输入本身就是 cgroupfs 格式，避免名称转换干扰。
		// 这是保证校验结果准确的关键。
		if err := cgroupManager.Validate(cgroupRoot); err != nil {
			return nil, fmt.Errorf("invalid configuration: %w", err)
		}
		logger.Info("Container manager verified user specified cgroup-root exists", "cgroupRoot", cgroupRoot)
		// 把 Node Allocatable 的顶层 cgroup 纳入 cgroup-root，
		// 这样各子模块无需理解 Node Allocatable 概念。
		cgroupRoot = NewCgroupName(cgroupRoot, defaultNodeAllocatableCgroupName)
	}
	logger.Info("Creating Container Manager object based on Node Config", "nodeConfig", nodeConfig)

	qosContainerManager, err := NewQOSContainerManager(subsystems, cgroupRoot, nodeConfig, cgroupManager)
	if err != nil {
		return nil, err
	}

	cm := &containerManagerImpl{
		cadvisorInterface:   cadvisorInterface,
		mountUtil:           mountUtil,
		NodeConfig:          nodeConfig,
		subsystems:          subsystems,
		cgroupManager:       cgroupManager,
		capacity:            capacity,
		internalCapacity:    internalCapacity,
		cgroupRoot:          cgroupRoot,
		recorder:            recorder,
		qosContainerManager: qosContainerManager,
	}

	cm.topologyManager, err = topologymanager.NewManager(
		logger,
		machineInfo.Topology,
		nodeConfig.TopologyManagerPolicy,
		nodeConfig.TopologyManagerScope,
		nodeConfig.TopologyManagerPolicyOptions,
	)

	if err != nil {
		return nil, err
	}

	logger.Info("Creating device plugin manager")
	cm.deviceManager, err = devicemanager.NewManagerImpl(logger, machineInfo.Topology, cm.topologyManager)
	if err != nil {
		return nil, err
	}
	cm.topologyManager.AddHintProvider(logger, cm.deviceManager)

	// DRA 管理器按特性门控创建，注册指标采集器；Start 时再注入活跃 Pod 等依赖。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) {
		logger.Info("Creating Dynamic Resource Allocation (DRA) manager")
		cm.draManager, err = dra.NewManager(logger, kubeClient, nodeConfig.KubeletRootDir)
		if err != nil {
			return nil, err
		}
		metrics.RegisterCollectors(cm.draManager.NewMetricsCollector())
	}
	cm.kubeClient = kubeClient

	// CPU 与内存管理器都注册为拓扑提示提供者，供 TopologyManager 协调。
	// CPU 与内存管理器需要容器映射快照，以恢复已分配状态。
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

	cm.memoryManager, err = memorymanager.NewManager(
		logger,
		nodeConfig.MemoryManagerPolicy,
		machineInfo,
		cm.GetNodeAllocatableReservation(),
		nodeConfig.MemoryManagerReservedMemory,
		nodeConfig.KubeletRootDir,
		cm.topologyManager,
	)
	if err != nil {
		logger.Error(err, "Failed to initialize memory manager")
		return nil, err
	}
	cm.topologyManager.AddHintProvider(logger, cm.memoryManager)

	// 所有子管理器的资源更新汇总到单一通道，
	// 由 kubelet 主同步循环消费。
	cm.resourceUpdates = make(chan resourceupdates.Update, 10)

	// 为每个子管理器启动 goroutine，把更新扇入到汇总通道。
	// 这样 kubelet 只需监听一个通道。
	var wg sync.WaitGroup
	sources := map[string]<-chan resourceupdates.Update{}
	if cm.deviceManager != nil {
		sources["deviceManager"] = cm.deviceManager.Updates()
	}
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) && cm.draManager != nil {
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.ResourceHealthStatus) {
			sources["draManager"] = cm.draManager.Updates()
		}
	}

	for name, ch := range sources {
		wg.Add(1)
		go func(name string, c <-chan resourceupdates.Update) {
			defer wg.Done()
			for v := range c {
				logger.V(4).Info("Container Manager: forwarding resource update", "source", name, "pods", v.PodUIDs)
				cm.resourceUpdates <- v
			}
		}(name, ch)
	}

	go func() {
		wg.Wait()
		close(cm.resourceUpdates)
	}()

	return cm, nil
}

// NewPodContainerManager 返回 Pod 容器管理器。
// 启用 QoS cgroup 时返回真实实现，
// 否则返回不做任何事的空实现。
func (cm *containerManagerImpl) NewPodContainerManager() PodContainerManager {
	if cm.NodeConfig.CgroupsPerQOS {
		return &podContainerManagerImpl{
			qosContainersInfo: cm.GetQOSContainersInfo(),
			subsystems:        cm.subsystems,
			cgroupManager:     cm.cgroupManager,
			podPidsLimit:      cm.PodPidsLimit,
			enforceCPULimits:  cm.EnforceCPULimits,
			// cpuCFSQuotaPeriod 单位为微秒，而 NodeConfig 中是纳秒，需换算。
			// 除以 time.Microsecond 即得到微秒值。
			cpuCFSQuotaPeriod:       uint64(cm.CPUCFSQuotaPeriod / time.Microsecond),
			podContainerManager:     cm,
			memoryReservationPolicy: cm.MemoryReservationPolicy,
			memoryThrottlingFactor:  cm.MemoryThrottlingFactor,
		}
	}
	return &podContainerManagerNoop{
		cgroupRoot: cm.cgroupRoot,
	}
}

// PodHasExclusiveCPUs 判断 Pod 是否独占 CPU。
func (cm *containerManagerImpl) PodHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod) bool {
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		// Pod 级资源模式下，所有容器都必须使用容器级隔离才视为独占。
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			if cm.cpuManager.GetResourceIsolationLevel(pod, &container) != cmqos.ResourceIsolationContainer {
				return false
			}
		}

		logger.V(4).Info("Pod has pinned cpus", "podName", pod.Name)
		return true
	}

	return podHasExclusiveCPUs(logger, cm.cpuManager, pod)
}

// ContainerHasExclusiveCPUs 判断单个容器是否独占 CPU。
func (cm *containerManagerImpl) ContainerHasExclusiveCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) bool {
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResourceManagers) {
		if cm.cpuManager.GetResourceIsolationLevel(pod, container) != cmqos.ResourceIsolationContainer {
			return false
		}

		logger.V(4).Info("Container has pinned cpus", "podName", pod.Name, "containerName", container.Name)
		return true
	}

	return containerHasExclusiveCPUs(logger, cm.cpuManager, pod, container)
}

// InternalContainerLifecycle 返回内部容器生命周期实现，组合 CPU、内存与拓扑管理器。
func (cm *containerManagerImpl) InternalContainerLifecycle(_ klog.Logger) InternalContainerLifecycle {
	return &internalContainerLifecycleImpl{cm.cpuManager, cm.memoryManager, cm.topologyManager}
}

// createManager 为指定容器名创建 cgroup 管理器。
func createManager(containerName string) (cgroups.Manager, error) {
	// 系统容器直接挂在根 cgroup 下，且不管理设备权限。
	cg := &cgroups.Cgroup{
		Parent: "/",
		Name:   containerName,
		Resources: &cgroups.Resources{
			SkipDevices: true,
		},
		Systemd: false,
	}

	return manager.New(cg)
}

// KernelTunableBehavior 定义内核参数不符合预期时的处理方式。
type KernelTunableBehavior string

const (
	// 内核参数取值：warn 仅告警，error 报错，modify 主动修改。
	KernelTunableWarn   KernelTunableBehavior = "warn"
	KernelTunableError  KernelTunableBehavior = "error"
	KernelTunableModify KernelTunableBehavior = "modify"
)

// setupKernelTunables 校验内核参数是否符合预期，
// 并按指定策略选择告警、报错或直接修改。
func setupKernelTunables(logger klog.Logger, option KernelTunableBehavior) error {
	desiredState := map[string]int{
		utilsysctl.VMOvercommitMemory: utilsysctl.VMOvercommitMemoryAlways,
		utilsysctl.VMPanicOnOOM:       utilsysctl.VMPanicOnOOMInvokeOOMKiller,
		utilsysctl.KernelPanic:        utilsysctl.KernelPanicRebootTimeout,
		utilsysctl.KernelPanicOnOops:  utilsysctl.KernelPanicOnOopsAlways,
		utilsysctl.RootMaxKeys:        utilsysctl.RootMaxKeysSetting,
		utilsysctl.RootMaxBytes:       utilsysctl.RootMaxBytesSetting,
	}

	// 期望值来自 kubelet 对节点稳定性的要求。
	sysctl := utilsysctl.New()

	errList := []error{}
	for flag, expectedValue := range desiredState {
		val, err := sysctl.GetSysctl(flag)
		if err != nil {
			errList = append(errList, err)
			continue
		}
		if val == expectedValue {
			continue
		}

		switch option {
		case KernelTunableError:
			errList = append(errList, fmt.Errorf("invalid kernel flag: %v, expected value: %v, actual value: %v", flag, expectedValue, val))
		case KernelTunableWarn:
			logger.V(2).Info("Invalid kernel flag", "flag", flag, "expectedValue", expectedValue, "actualValue", val)
		// 修改失败时，若运行在用户命名空间且特性开启，可忽略该错误。
		case KernelTunableModify:
			logger.V(2).Info("Updating kernel flag", "flag", flag, "expectedValue", expectedValue, "actualValue", val)
			err = sysctl.SetSysctl(flag, expectedValue)
			if err != nil {
				if inuserns.RunningInUserNS() {
					if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.KubeletInUserNamespace) {
						logger.V(2).Info("Updating kernel flag failed (running in UserNS, ignoring)", "flag", flag, "err", err)
						continue
					}
					logger.Error(err, "Updating kernel flag failed (Hint: enable KubeletInUserNamespace feature flag to ignore the error)", "flag", flag)
				}
				errList = append(errList, err)
			}
		}
	}
	return utilerrors.NewAggregate(errList)
}

// setupNode 完成节点级初始化：校验系统要求、设置内核参数、创建 QoS 与系统 cgroup。
func (cm *containerManagerImpl) setupNode(ctx context.Context, activePods ActivePodsFunc) error {
	logger := klog.FromContext(ctx)

	f, err := validateSystemRequirements(logger, cm.mountUtil)
	if err != nil {
		return err
	}
	// 不支持 CPU 硬限制只作为软性状态上报，不阻断启动。
	if !f.cpuHardcapping {
		cm.status.SoftRequirements = fmt.Errorf("CPU hardcapping unsupported")
	}
	b := KernelTunableModify
	if cm.GetNodeConfig().ProtectKernelDefaults {
		b = KernelTunableError
	}
	if err := setupKernelTunables(logger, b); err != nil {
		return err
	}

	// 仅在启用 cgroups-per-qos 时创建顶层 QoS 容器。
	if cm.NodeConfig.CgroupsPerQOS {
		if err := cm.createNodeAllocatableCgroups(logger); err != nil {
			return err
		}
		err = cm.qosContainerManager.Start(ctx, cm.GetNodeAllocatableAbsolute, activePods)
		if err != nil {
			return fmt.Errorf("failed to initialize top level QOS containers: %v", err)
		}
	}

	// 按配置强制 Node Allocatable 限制。
	if err := cm.enforceNodeAllocatableCgroups(logger); err != nil {
		return err
	}

	systemContainers := []*systemContainer{}

	// 配置了 SystemCgroupsName 时创建系统容器，把系统进程从根 cgroup 隔离出去。
	if cm.SystemCgroupsName != "" {
		if cm.SystemCgroupsName == "/" {
			return fmt.Errorf("system container cannot be root (\"/\")")
		}
		cont, err := newSystemCgroups(cm.SystemCgroupsName)
		if err != nil {
			return err
		}
		// 周期性把非内核进程移入系统容器，避免其消耗未预留资源。
		cont.ensureStateFunc = func(manager cgroups.Manager) error {
			return ensureSystemCgroups(logger, "/", manager)
		}
		systemContainers = append(systemContainers, cont)
	}

	if cm.KubeletCgroupsName != "" {
		cont, err := newSystemCgroups(cm.KubeletCgroupsName)
		if err != nil {
			return err
		}

		// kubelet 自身进程需要放入指定 cgroup 并设置 OOM 分数。
		cont.ensureStateFunc = func(_ cgroups.Manager) error {
			return ensureProcessInContainerWithOOMScore(logger, os.Getpid(), int(cm.KubeletOOMScoreAdj), cont.manager)
		}
		systemContainers = append(systemContainers, cont)
	} else {
		// 未配置 kubelet cgroup 时，周期性探测其实际 cgroup 并记录。
		cm.periodicTasks = append(cm.periodicTasks, func() {
			if err := ensureProcessInContainerWithOOMScore(logger, os.Getpid(), int(cm.KubeletOOMScoreAdj), nil); err != nil {
				logger.Error(err, "Failed to ensure process in container with oom score")
				return
			}
			cont, err := getContainer(logger, os.Getpid())
			if err != nil {
				logger.Error(err, "Failed to find cgroups of kubelet")
				return
			}
			cm.Lock()
			defer cm.Unlock()

			cm.KubeletCgroupsName = cont
		})
	}

	cm.systemContainers = systemContainers
	return nil
}

// GetNodeConfig 返回节点配置副本，读锁保护。
func (cm *containerManagerImpl) GetNodeConfig() NodeConfig {
	cm.RLock()
	defer cm.RUnlock()
	return cm.NodeConfig
}

// GetPodCgroupRoot 返回包含所有 Pod 的 cgroupfs 路径。
func (cm *containerManagerImpl) GetPodCgroupRoot() string {
	return cm.cgroupManager.Name(cm.cgroupRoot)
}

// GetMountedSubsystems 返回已挂载的 cgroup 子系统。
func (cm *containerManagerImpl) GetMountedSubsystems() *CgroupSubsystems {
	return cm.subsystems
}

// GetQOSContainersInfo 返回各 QoS 层级的 cgroup 信息。
func (cm *containerManagerImpl) GetQOSContainersInfo() QOSContainersInfo {
	return cm.qosContainerManager.GetQOSContainersInfo()
}

// UpdateQOSCgroups 更新 QoS cgroup 的资源参数。
func (cm *containerManagerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	return cm.qosContainerManager.UpdateCgroups(logger)
}

// Status 返回容器管理器状态。
func (cm *containerManagerImpl) Status() Status {
	cm.RLock()
	defer cm.RUnlock()
	return cm.status
}

// Start 启动容器管理器及其所有子管理器。
func (cm *containerManagerImpl) Start(ctx context.Context, node *v1.Node,
	activePods ActivePodsFunc,
	getNode GetNodeFunc,
	sourcesReady config.SourcesReady,
	podStatusProvider status.PodStatusProvider,
	runtimeService internalapi.RuntimeService,
	localStorageCapacityIsolation bool) error {
	logger := klog.FromContext(ctx)

	containerMap, containerRunningSet := buildContainerMapAndRunningSetFromRuntime(ctx, runtimeService)

	// DRA 管理器需要活跃 Pod、节点获取函数与配置源就绪状态。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) {
		err := cm.draManager.Start(ctx, dra.ActivePodsFunc(activePods), dra.GetNodeFunc(getNode), sourcesReady)
		if err != nil {
			return fmt.Errorf("start dra manager error: %w", err)
		}
	}

	// CPU 管理器启动时恢复已分配状态并开始对账。
	err := cm.cpuManager.Start(ctx, cpumanager.ActivePodsFunc(activePods), sourcesReady, podStatusProvider, runtimeService, containerMap.Clone())
	if err != nil {
		return fmt.Errorf("start cpu manager error: %w", err)
	}

	// 内存管理器与 CPU 管理器使用同一份容器映射副本。
	err = cm.memoryManager.Start(ctx, memorymanager.ActivePodsFunc(activePods), sourcesReady, podStatusProvider, runtimeService, containerMap.Clone())
	if err != nil {
		return fmt.Errorf("start memory manager error: %w", err)
	}

	// 缓存节点信息（容量与可分配量）。
	// 后续状态上报直接复用该对象。
	cm.nodeInfo = node

	if localStorageCapacityIsolation {
		rootfs, err := cm.cadvisorInterface.RootFsInfo()
		if err != nil {
			return fmt.Errorf("failed to get rootfs info: %v", err)
		}
		cm.Lock()
		for rName, rCap := range cadvisor.EphemeralStorageCapacityFromFsInfo(rootfs) {
			cm.capacity[rName] = rCap
		}
		cm.Unlock()
	}

	// 校验 Node Allocatable 配置，避免预留量超过容量。
	if err := cm.validateNodeAllocatable(); err != nil {
		return err
	}

	// 执行节点级 cgroup 与内核参数初始化。
	if err := cm.setupNode(ctx, activePods); err != nil {
		return err
	}

	// 没有状态保证函数时不启动后台线程，节省资源。
	hasEnsureStateFuncs := false
	for _, cont := range cm.systemContainers {
		if cont.ensureStateFunc != nil {
			hasEnsureStateFuncs = true
			break
		}
	}
	if hasEnsureStateFuncs {
		// 每分钟重新确保一次系统容器状态。
		go wait.Until(func() {
			for _, cont := range cm.systemContainers {
				if cont.ensureStateFunc != nil {
					if err := cont.ensureStateFunc(cont.manager); err != nil {
						logger.Info("Failed to ensure state", "containerName", cont.name, "err", err)
					}
				}
			}
		}, time.Minute, wait.NeverStop)

	}

	if len(cm.periodicTasks) > 0 {
		go wait.Until(func() {
			for _, task := range cm.periodicTasks {
				if task != nil {
					task()
				}
			}
		}, 5*time.Minute, wait.NeverStop)
	}

	// 设备管理器最后启动，它依赖前面初始化好的拓扑信息。
	if err := cm.deviceManager.Start(klog.FromContext(ctx), devicemanager.ActivePodsFunc(activePods), sourcesReady, containerMap.Clone(), containerRunningSet); err != nil {
		return err
	}

	return nil
}

// GetPluginRegistrationHandlers 返回设备插件与 DRA 插件的注册处理器。
func (cm *containerManagerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	res := map[string]cache.PluginHandler{
		pluginwatcherapi.DevicePlugin: cm.deviceManager.GetWatcherHandler(),
	}

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) {
		res[pluginwatcherapi.DRAPlugin] = cm.draManager.GetWatcherHandler()
	}

	return res
}

// GetHealthCheckers 返回设备插件的健康检查器。
func (cm *containerManagerImpl) GetHealthCheckers() []healthz.HealthChecker {
	return []healthz.HealthChecker{cm.deviceManager.GetHealthChecker()}
}

// TODO: 把 GetResources 逻辑迁移到 PodContainerManager。
// GetResources 汇总容器运行所需的 CDI 设备、设备节点、挂载与环境变量。
func (cm *containerManagerImpl) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	logger := klog.FromContext(ctx)
	opts := &kubecontainer.RunContainerOptions{}
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) {
		resOpts, err := cm.draManager.GetResources(pod, container)
		if err != nil {
			return nil, err
		}
		logger.V(5).Info("Determined CDI devices for pod", "pod", klog.KObj(pod), "cdiDevices", resOpts.CDIDevices)
		opts.CDIDevices = append(opts.CDIDevices, resOpts.CDIDevices...)
	}
	// 设备分配应已在准入阶段完成，这里只读取缓存。
	// 因此不会触发新的分配。
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
	opts.CDIDevices = append(opts.CDIDevices, devOpts.CDIDevices...)
	return opts, nil
}

// UpdatePluginResources 在准入阶段修正 NodeInfo 的设备资源。
func (cm *containerManagerImpl) UpdatePluginResources(node *schedulerframework.NodeInfo, attrs *lifecycle.PodAdmitAttributes) error {
	return cm.deviceManager.UpdatePluginResources(node, attrs)
}

// GetAllocateResourcesPodAdmitHandler 返回负责资源分配的准入处理器（即拓扑管理器）。
func (cm *containerManagerImpl) GetAllocateResourcesPodAdmitHandler(_ klog.Logger) lifecycle.PodAdmitHandler {
	return cm.topologyManager
}

// SystemCgroupsLimit 返回所有系统容器的 CPU 限制总和。
func (cm *containerManagerImpl) SystemCgroupsLimit() v1.ResourceList {
	cpuLimit := int64(0)

	// 汇总所有系统容器的 CPU 限制。
	for _, cont := range cm.systemContainers {
		cpuLimit += cont.cpuMillicores
	}

	return v1.ResourceList{
		v1.ResourceCPU: *resource.NewMilliQuantity(
			cpuLimit,
			resource.DecimalSI),
	}
}

// isProcessRunningInHost 判断进程是否运行在宿主机 PID 命名空间。
func isProcessRunningInHost(logger klog.Logger, pid int) (bool, error) {
	// 通过比较 /proc/1 与目标进程的 pid 命名空间判断。
	// 通过比较 /proc/1 与目标进程的 pid 命名空间判断。
	initPidNs, err := os.Readlink("/proc/1/ns/pid")
	if err != nil {
		return false, fmt.Errorf("failed to find pid namespace of init process")
	}
	logger.V(10).Info("Found init PID namespace", "namespace", initPidNs)
	processPidNs, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/pid", pid))
	if err != nil {
		return false, fmt.Errorf("failed to find pid namespace of process %d", pid)
	}
	logger.V(10).Info("Process info", "pid", pid, "namespace", processPidNs)
	return initPidNs == processPidNs, nil
}

// ensureProcessInContainerWithOOMScore 确保进程位于指定 cgroup 并设置 OOM 分数。
func ensureProcessInContainerWithOOMScore(logger klog.Logger, pid int, oomScoreAdj int, manager cgroups.Manager) error {
	if runningInHost, err := isProcessRunningInHost(logger, pid); err != nil {
		// 无法判断上下文时宁可不动，避免误移动 docker daemon 等关键进程。
		return err
	} else if !runningInHost {
		// 进程已在容器内运行，不应再调整。
		logger.V(2).Info("PID is not running in the host namespace", "pid", pid)
		return nil
	}

	var errs []error
	if manager != nil {
		cont, err := getContainer(logger, pid)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to find container of PID %d: %v", pid, err))
		}

		name := ""
		cgroups, err := manager.GetCgroups()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get cgroups for %d: %v", pid, err))
		} else {
			name = cgroups.Name
		}

		if cont != name {
			err = manager.Apply(pid)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to move PID %d (in %q) to %q: %v", pid, cont, name, err))
			}
		}
	}

	// 除 cgroup 归属外，还要设置 OOM 分数以影响内核的杀进程选择。
	oomAdjuster := oom.NewOOMAdjuster()
	logger.V(5).Info("Attempting to apply oom_score_adj to process", "oomScoreAdj", oomScoreAdj, "pid", pid)
	if err := oomAdjuster.ApplyOOMScoreAdj(pid, oomScoreAdj); err != nil {
		logger.V(3).Info("Failed to apply oom_score_adj to process", "oomScoreAdj", oomScoreAdj, "pid", pid, "err", err)
		errs = append(errs, fmt.Errorf("failed to apply oom score %d to PID %d: %v", oomScoreAdj, pid, err))
	}
	return utilerrors.NewAggregate(errs)
}

// getContainer 返回指定 PID 所属的 cgroup。
// 要求 memory 与 cpu 使用统一层级。
// systemd 环境下使用 name=systemd cgroup。
func getContainer(logger klog.Logger, pid int) (string, error) {
	cgs, err := cgroups.ParseCgroupFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}

	if cgroups.IsCgroup2UnifiedMode() {
		c, found := cgs[""]
		if !found {
			return "", cgroups.NewNotFoundError("unified")
		}
		return c, nil
	}

	cpu, found := cgs["cpu"]
	if !found {
		return "", cgroups.NewNotFoundError("cpu")
	}
	memory, found := cgs["memory"]
	if !found {
		return "", cgroups.NewNotFoundError("memory")
	}

	// 该 cgroup 用于资源统计，必须保证 cpu 与 memory 层级一致。
	if cpu != memory {
		return "", fmt.Errorf("cpu and memory cgroup hierarchy not unified.  cpu: %s, memory: %s", cpu, memory)
	}

	// systemd 下每个 PID 都在统一层级中，但 CPU/内存记账默认关闭；
	// 用户可通过 system.conf 或单元配置开启记账。
	// 未开启时只告警而不报错，以免影响本地开发。
	// 这种情况下统计可能落在最近的祖先 cgroup 上。
	// 因此 kubelet 可能拿不到自身的 CPU/内存统计。
	// 例如用户会话 cgroup 与实际记账 cgroup 可能不同。
	// 这是 systemd 环境下的已知行为。
	// 告警足以提示用户，无需中断启动。
	// 容器运行时同理。
	if systemd, found := cgs["name=systemd"]; found {
		if systemd != cpu {
			logger.Info("CPUAccounting not enabled for process", "pid", pid)
		}
		if systemd != memory {
			logger.Info("MemoryAccounting not enabled for process", "pid", pid)
		}
		return systemd, nil
	}

	return cpu, nil
}

// ensureSystemCgroups 创建系统容器，并把所有非内核线程与 PID 1 之外的进程移入其中。
// 这样系统预留资源才能真正生效。
//
// 内核线程保留在根 cgroup，避免其执行与待定义的 /system 配额绑定，
// 从而产生优先级反转。
func ensureSystemCgroups(logger klog.Logger, rootCgroupPath string, manager cgroups.Manager) error {
	// 把非内核 PID 移入系统容器。
	// 只保留最后一次尝试的错误，避免重试噪音。
	var finalErr error
	for i := 0; i <= 10; i++ {
		allPids, err := cmutil.GetPids(rootCgroupPath)
		if err != nil {
			finalErr = fmt.Errorf("failed to list PIDs for root: %v", err)
			continue
		}

		// 排除内核线程与受保护 PID（PID 1 等）。
		pids := make([]int, 0, len(allPids))
		for _, pid := range allPids {
			if pid == 1 || isKernelPid(pid) {
				continue
			}

			pids = append(pids, pid)
		}

		// 没有剩余非内核 PID 即完成。
		if len(pids) == 0 {
			return nil
		}

		logger.V(3).Info("Moving non-kernel processes", "pids", pids)
		for _, pid := range pids {
			err := manager.Apply(pid)
			if err != nil {
				name := ""
				cgroups, err := manager.GetCgroups()
				if err == nil {
					name = cgroups.Name
				}

				finalErr = fmt.Errorf("failed to move PID %d into the system container %q: %v", pid, name, err)
			}
		}

	}

	return finalErr
}

// isKernelPid 判断指定 PID 是否为内核线程。
func isKernelPid(pid int) bool {
	// 内核线程没有可执行文件链接。
	_, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	return err != nil && os.IsNotExist(err)
}

// GetCapacity 返回节点容量（cpu、memory、ephemeral-storage、huge-pages 等）。
// 目前仅在查询临时存储时调用。
func (cm *containerManagerImpl) GetCapacity(logger klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList {
	cm.RLock()
	defer cm.RUnlock()
	if localStorageCapacityIsolation {
		// 若尚未缓存临时存储容量，则直接从 cAdvisor 获取。
		if _, ok := cm.capacity[v1.ResourceEphemeralStorage]; !ok {
			// 这是 Start 之前的兜底路径。
			if cm.cadvisorInterface != nil {
				rootfs, err := cm.cadvisorInterface.RootFsInfo()
				if err != nil {
					logger.Error(err, "Unable to get rootfs data from cAdvisor interface")
					// 获取失败时返回不含临时存储的容量，保证调用方仍可工作。
					return cm.capacity
				}
				// 不修改 cm.capacity，而是构造副本后追加临时存储容量。
				// 这样缓存保持原样。
				capacityWithEphemeralStorage := v1.ResourceList{}
				for rName, rQuant := range cm.capacity {
					capacityWithEphemeralStorage[rName] = rQuant
				}
				capacityWithEphemeralStorage[v1.ResourceEphemeralStorage] = cadvisor.EphemeralStorageCapacityFromFsInfo(rootfs)[v1.ResourceEphemeralStorage]
				return capacityWithEphemeralStorage
			}
		}
	}
	return cm.capacity
}

// GetDevicePluginResourceCapacity 返回设备插件资源容量、可分配量与失效资源。
func (cm *containerManagerImpl) GetDevicePluginResourceCapacity(logger klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
	return cm.deviceManager.GetCapacity(logger)
}

// GetDevices 返回指定容器使用的设备列表。
func (cm *containerManagerImpl) GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices {
	return containerDevicesFromResourceDeviceInstances(cm.deviceManager.GetDevices(podUID, containerName))
}

// GetAllocatableDevices 返回所有可分配设备。
func (cm *containerManagerImpl) GetAllocatableDevices(logger klog.Logger) []*podresourcesapi.ContainerDevices {
	return containerDevicesFromResourceDeviceInstances(cm.deviceManager.GetAllocatableDevices(logger))
}

// GetCPUs 返回容器独占的 CPU 列表。
func (cm *containerManagerImpl) GetCPUs(pod *v1.Pod, container *v1.Container) []int64 {
	if cm.cpuManager != nil {
		// 只有容器级隔离才报告容器 CPU；
		// Pod 级或宿主机级共享池的 CPU 在 Pod 级上报。
		// 这里返回空，避免重复。
		// 这是 Pod 级资源管理的前提。
		if cm.cpuManager.GetResourceIsolationLevel(pod, container) != cmqos.ResourceIsolationContainer {
			return []int64{}
		}
		return int64Slice(cm.cpuManager.GetExclusiveCPUs(string(pod.UID), container.Name).UnsortedList())
	}
	return []int64{}
}

// GetPodCPUs 返回 Pod 级独占的 CPU 列表。
func (cm *containerManagerImpl) GetPodCPUs(podUID string) []int64 {
	if cm.cpuManager == nil {
		return []int64{}
	}
	return int64Slice(cm.cpuManager.GetPodCPUs(podUID).UnsortedList())
}

// GetAllocatableCPUs 返回所有可分配的 CPU。
func (cm *containerManagerImpl) GetAllocatableCPUs() []int64 {
	if cm.cpuManager != nil {
		return int64Slice(cm.cpuManager.GetAllocatableCPUs().UnsortedList())
	}
	return []int64{}
}

// GetMemory 返回容器独占的内存块。
func (cm *containerManagerImpl) GetMemory(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.ContainerMemory {
	if cm.memoryManager == nil {
		return []*podresourcesapi.ContainerMemory{}
	}

	// 与 CPU 类似，只有容器级隔离才报告容器内存。
	// 共享池内存在 Pod 级上报。
	// 否则返回空。
	// 这样避免同一资源被重复统计。
	if cm.memoryManager.GetResourceIsolationLevel(pod, container) != cmqos.ResourceIsolationContainer {
		return []*podresourcesapi.ContainerMemory{}
	}

	return containerMemoryFromBlock(cm.memoryManager.GetMemory(logger, string(pod.UID), container.Name))
}

// GetAllocatableMemory 返回所有可分配内存块。
func (cm *containerManagerImpl) GetAllocatableMemory(logger klog.Logger) []*podresourcesapi.ContainerMemory {
	if cm.memoryManager == nil {
		return []*podresourcesapi.ContainerMemory{}
	}
	return containerMemoryFromBlock(cm.memoryManager.GetAllocatableMemory(logger))
}

// GetPodMemory 返回 Pod 级独占的内存块。
func (cm *containerManagerImpl) GetPodMemory(logger klog.Logger, podUID string) []*podresourcesapi.ContainerMemory {
	if cm.memoryManager == nil {
		return []*podresourcesapi.ContainerMemory{}
	}

	return containerMemoryFromBlock(cm.memoryManager.GetPodMemory(podUID))
}

// GetDynamicResources 返回容器通过 DRA 获得的动态资源。
func (cm *containerManagerImpl) GetDynamicResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) {
		return []*podresourcesapi.DynamicResource{}
	}

	// 按 claim 组织结果，每个 claim 下再按驱动与设备展开。
	var containerDynamicResources []*podresourcesapi.DynamicResource
	containerClaimInfos, err := cm.draManager.GetContainerClaimInfos(pod, container)
	if err != nil {
		logger.Error(err, "Unable to get container claim info state")
		return []*podresourcesapi.DynamicResource{}
	}
	for _, containerClaimInfo := range containerClaimInfos {
		var claimResources []*podresourcesapi.ClaimResource
		for driverName, driverState := range containerClaimInfo.DriverState {
			var cdiDevices []*podresourcesapi.CDIDevice
			for _, device := range driverState.Devices {
				for _, cdiDeviceID := range device.CDIDeviceIDs {
					cdiDevices = append(cdiDevices, &podresourcesapi.CDIDevice{Name: cdiDeviceID})
				}
				resources := &podresourcesapi.ClaimResource{
					CdiDevices: cdiDevices,
					DriverName: driverName,
					PoolName:   device.PoolName,
					DeviceName: device.DeviceName,
					ShareId:    (*string)(device.ShareID),
				}
				claimResources = append(claimResources, resources)
			}
		}
		containerDynamicResource := podresourcesapi.DynamicResource{
			ClaimName:      containerClaimInfo.ClaimName,
			ClaimNamespace: containerClaimInfo.Namespace,
			ClaimResources: claimResources,
		}
		containerDynamicResources = append(containerDynamicResources, &containerDynamicResource)
	}
	return containerDynamicResources
}

// ShouldResetExtendedResourceCapacity 判断是否需要清零扩展资源容量。
func (cm *containerManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	return cm.deviceManager.ShouldResetExtendedResourceCapacity()
}

// UpdateAllocatedDevices 释放已终止 Pod 占用的设备。
func (cm *containerManagerImpl) UpdateAllocatedDevices(logger klog.Logger) {
	cm.deviceManager.UpdateAllocatedDevices(logger)
}

// containerMemoryFromBlock 把内存管理器返回的内存块转换为 podresources API 结构。
func containerMemoryFromBlock(blocks []memorymanagerstate.Block) []*podresourcesapi.ContainerMemory {
	var containerMemories []*podresourcesapi.ContainerMemory

	for _, b := range blocks {
		containerMemory := podresourcesapi.ContainerMemory{
			MemoryType: string(b.Type),
			Size:       b.Size,
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{},
			},
		}

		for _, numaNodeID := range b.NUMAAffinity {
			containerMemory.Topology.Nodes = append(containerMemory.Topology.Nodes, &podresourcesapi.NUMANode{ID: int64(numaNodeID)})
		}

		containerMemories = append(containerMemories, &containerMemory)
	}

	return containerMemories
}

// PrepareDynamicResources 准备 Pod 的 DRA 资源。
func (cm *containerManagerImpl) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return cm.draManager.PrepareResources(ctx, pod)
}

// UnprepareDynamicResources 释放 Pod 的 DRA 资源。
func (cm *containerManagerImpl) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return cm.draManager.UnprepareResources(ctx, pod)
}

// PodMightNeedToUnprepareResources 判断 Pod 是否可能需要释放 DRA 资源。
func (cm *containerManagerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	return cm.draManager.PodMightNeedToUnprepareResources(UID)
}

// UpdateAllocatedResourcesStatus 更新 Pod 状态中的设备与 DRA 资源健康信息。
func (cm *containerManagerImpl) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {

	// 目前仅设备插件与（开启特性后的）DRA 支持资源健康上报。
	cm.deviceManager.UpdateAllocatedResourcesStatus(logger, pod, status)

	// DRA 健康状态还额外受 ResourceHealthStatus 门控。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DynamicResourceAllocation) && cm.draManager != nil {
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.ResourceHealthStatus) {
			cm.draManager.UpdateAllocatedResourcesStatus(logger, pod, status)
		}
	}
}

// Updates 返回汇总后的资源更新通道。
func (cm *containerManagerImpl) Updates() <-chan resourceupdates.Update {
	return cm.resourceUpdates
}
