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

package cpumanager

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/utils/cpuset"
)

// ActivePodsFunc 返回需要参与 CPU 调谐的 Pod 列表。
type ActivePodsFunc func() []*v1.Pod

// runtimeService 是 CPU manager 更新容器资源时依赖的运行时方法子集。
type runtimeService interface {
	UpdateContainerResources(ctx context.Context, id string, resources *runtimeapi.ContainerResources) error
}

// policyName 表示 CPU manager 所选策略的名称。
type policyName string

// cpuManagerStateFileName 是 cpu manager 持久化状态的检查点文件名。
const cpuManagerStateFileName = "cpu_manager_state"

// Manager 接口提供 Kubelet 管理 Pod CPU 的方法。
type Manager interface {
	// Start 在 Kubelet 初始化时调用。
	// 接收 Context 是因为它可能启动 reconcileState 辅助流程，
	// 而该流程更新容器状态时需要 Context。
	Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error

	// 触发为容器分配 CPU。必须在调用该容器的 AddContainer() 之前
	// （例如 Pod 准入阶段）完成。
	Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error

	// AddContainer 记录容器 ID 到 Pod UID 和容器名的映射，
	// 删除容器时用该映射回收 CPU 分配。
	AddContainer(logger klog.Logger, p *v1.Pod, c *v1.Container, containerID string)

	// RemoveContainer 在 Kubelet 决定杀掉或删除容器后调用。
	// 调用后 CPU manager 不再尝试调谐该容器，其独占 CPU 会被释放。
	RemoveContainer(logger klog.Logger, containerID string) error

	// State 返回内部 CPU manager 状态的只读视图。
	State() state.Reader

	// GetTopologyHints 实现 topologymanager.HintProvider 接口，
	// 用于在本控制器与其他资源控制器之间达成 NUMA 感知的资源对齐。
	GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// GetExclusiveCPUs 实现 podresources.CPUsProvider 接口，
	// 返回容器独占分配的 CPU。
	GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet

	// GetPodCPUs 实现 podresources.CPUsProvider 接口，
	// 返回分配给整个 Pod 的 CPU 集合。
	GetPodCPUs(podUID string) cpuset.CPUSet

	// GetPodTopologyHints 实现 topologymanager.HintProvider 接口，
	// 用于按 Pod 在本控制器与其他资源控制器之间达成 NUMA 感知的资源对齐。
	GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// AllocatePod 触发为整个 Pod 分配 CPU。
	AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error

	// GetAllocatableCPUs 返回可供分配的全部 CPU 集合。
	GetAllocatableCPUs() cpuset.CPUSet

	// GetCPUAffinity 返回容器可用的 CPU 集合，既包含共享池中的 CPU，
	// 也包含独占分配的 CPU。
	GetCPUAffinity(podUID, containerName string) cpuset.CPUSet

	// GetAllCPUs 返回 cpumanager 通过硬件发现获知的全部 CPU，
	// 对应节点的 CPU 容量。
	GetAllCPUs() cpuset.CPUSet

	// GetResourceIsolationLevel 返回容器的资源隔离级别。
	GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel
}

// manager 是 Manager 接口的默认实现，用互斥锁保护策略与状态。
type manager struct {
	sync.Mutex
	policy Policy

	// reconcilePeriod 是两次 reconcileState 调用之间的间隔。
	reconcilePeriod time.Duration

	// state 让可插拔的 CPU 分配策略共享同一份状态表示，供系统检查与调谐。
	state state.State

	// lastUpdateState 保存每个容器最近一次更新时的状态。
	lastUpdateState state.State

	// containerRuntime 是调用 UpdateContainerResources() 所需的容器运行时服务接口。
	containerRuntime runtimeService

	// activePods 返回节点上的活跃 Pod，调谐循环据此更新所有容器。
	activePods ActivePodsFunc

	// podStatusProvider 提供 Pod 状态及其容器 ID。
	podStatusProvider status.PodStatusProvider

	// containerMap 保存所有容器的 (pod, container) -> containerID 映射。
	containerMap containermap.ContainerMap

	topology *topology.CPUTopology

	nodeAllocatableReservation v1.ResourceList

	// sourcesReady 表示 kubelet 配置源（如 apiserver 更新）是否就绪；
	// 只有就绪后才能从检查点状态中清理非活跃 Pod。
	sourcesReady config.SourcesReady

	// stateFileDirectory 是检查点状态文件所在目录。
	stateFileDirectory string

	// allCPUs 是系统报告的在线 CPU 集合。
	allCPUs cpuset.CPUSet

	// allocatableCPUs 是系统报告的在线 CPU 中扣除预留后可分配的集合。
	allocatableCPUs cpuset.CPUSet
}

// 编译期断言：manager 必须实现 Manager 接口。
var _ Manager = &manager{}

// sourcesReadyStub 是始终认为所有配置源已就绪的桩实现。
type sourcesReadyStub struct{}

// AddSource 忽略数据源注册，桩实现不需要跟踪来源。
func (s *sourcesReadyStub) AddSource(source string) {}

// AllReady 恒返回 true，表示配置源已全部就绪。
func (s *sourcesReadyStub) AllReady() bool { return true }

// NewManager 根据给定策略创建新的 CPU manager。
func NewManager(logger klog.Logger, cpuPolicyName string, cpuPolicyOptions map[string]string, reconcilePeriod time.Duration, machineInfo *cadvisorapi.MachineInfo, specificCPUs cpuset.CPUSet, nodeAllocatableReservation v1.ResourceList, stateFileDirectory string, affinity topologymanager.Store) (Manager, error) {
	var topo *topology.CPUTopology
	var policy Policy
	var err error

	// 先探测 CPU 拓扑，静态策略需要用它划分独占 CPU 池。
	topo, err = topology.Discover(logger, machineInfo)
	if err != nil {
		return nil, err
	}

	switch policyName(cpuPolicyName) {

	// none 策略不分配独占 CPU，只维护共享池。
	case PolicyNone:
		policy, err = NewNonePolicy(cpuPolicyOptions)
		if err != nil {
			return nil, fmt.Errorf("new none policy error: %w", err)
		}

	// 静态策略要求显式预留 CPU，并据此计算可分配池。
	case PolicyStatic:
		logger.Info("Detected CPU topology", "topology", topo)

		reservedCPUs, ok := nodeAllocatableReservation[v1.ResourceCPU]
		if !ok {
			// 缺少预留信息时静态策略无法初始化。
			return nil, fmt.Errorf("[cpumanager] unable to determine reserved CPU resources for static policy")
		}
		if reservedCPUs.IsZero() {
			// 静态策略要求预留量非零；预留为零会让共享池被完全占满，
			// 届时要么破坏独占性保证，要么只能驱逐至少有一个零 CPU 容器的 Pod。
			// 详见 policy_static.go 中的说明。
			return nil, fmt.Errorf("[cpumanager] the static policy requires systemreserved.cpu + kubereserved.cpu to be greater than zero")
		}

		// 预留量向上取整，因为小数 CPU 无法被独占分配。
		reservedCPUsFloat := float64(reservedCPUs.MilliValue()) / 1000
		numReservedCPUs := int(math.Ceil(reservedCPUsFloat))
		policy, err = NewStaticPolicy(logger, topo, numReservedCPUs, specificCPUs, affinity, cpuPolicyOptions)
		if err != nil {
			return nil, fmt.Errorf("new static policy error: %w", err)
		}

	default:
		return nil, fmt.Errorf("unknown policy: \"%s\"", cpuPolicyName)
	}

	// manager 的 state 在 Start() 中初始化，这里先填充其余依赖。
	manager := &manager{
		policy:                     policy,
		reconcilePeriod:            reconcilePeriod,
		lastUpdateState:            state.NewMemoryState(logger),
		topology:                   topo,
		nodeAllocatableReservation: nodeAllocatableReservation,
		stateFileDirectory:         stateFileDirectory,
		allCPUs:                    topo.CPUDetails.CPUs(),
	}
	// Start() 调用前先使用桩实现，避免访问未初始化的字段。
	manager.sourcesReady = &sourcesReadyStub{}
	return manager, nil
}

// Start 初始化状态检查点、启动策略，并在非 none 策略下启动周期性调谐循环。
func (m *manager) Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting", "policy", m.policy.Name())
	logger.Info("Reconciling", "reconcilePeriod", m.reconcilePeriod)
	m.sourcesReady = sourcesReady
	m.activePods = activePods
	m.podStatusProvider = podStatusProvider
	m.containerRuntime = containerRuntime
	m.containerMap = initialContainers

	// 检查点状态是 CPU 分配的唯一持久化来源，加载失败必须中止启动。
	stateImpl, err := state.NewCheckpointState(logger, m.stateFileDirectory, cpuManagerStateFileName, m.policy.Name(), m.containerMap)
	if err != nil {
		logger.Error(err, "Could not initialize checkpoint manager, please drain node and remove policy state file")
		return err
	}
	m.state = stateImpl

	// 由策略校验并预热状态，例如静态策略会核对 CPU 拓扑兼容性。
	err = m.policy.Start(logger, m.state)
	if err != nil {
		logger.Error(err, "Policy start error")
		return err
	}

	logger.V(4).Info("CPU manager started", "policy", m.policy.Name())

	// 缓存可分配 CPU，后续查询无需重复计算。
	m.allocatableCPUs = m.policy.GetAllocatableCPUs(m.state)

	// none 策略没有独占分配，无需周期性调谐。
	if m.policy.Name() == string(PolicyNone) {
		return nil
	}
	// 后台周期调谐，确保容器的实际 CPU 集合与已发放的独占 CPU 一致。
	go wait.Until(func() { m.reconcileState(ctx) }, m.reconcilePeriod, wait.NeverStop)
	return nil
}

// Allocate 先回收陈旧状态，再在锁内委托策略为容器分配 CPU。
func (m *manager) Allocate(ctx context.Context, p *v1.Pod, c *v1.Container, operation lifecycle.Operation) error {
	logger := klog.FromContext(ctx)

	// 分配前回收已失效的资源，避免把 CPU 分配给已删除的容器。
	m.removeStaleState(logger)

	m.Lock()
	defer m.Unlock()

	// 交给具体策略决定该容器是否需要独占 CPU。
	err := m.policy.Allocate(logger, m.state, p, c, operation)
	if err != nil {
		logger.Error(err, "policy error")
		return err
	}

	return nil
}

// AllocatePod 先回收陈旧状态，再委托策略为整个 Pod 分配 CPU。
func (m *manager) AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error {
	// 分配前回收已失效的资源，避免把 CPU 分配给已删除的容器。
	m.removeStaleState(logger)

	m.Lock()
	defer m.Unlock()

	// 交给具体策略决定该 Pod 是否需要独占 CPU。
	if err := m.policy.AllocatePod(logger, m.state, pod, operation); err != nil {
		logger.Error(err, "AllocatePod error", "pod", klog.KObj(pod))
		return err
	}
	return nil
}

// AddContainer 把容器记录到 containerMap，并同步 lastUpdateState 中的 CPU 集合。
func (m *manager) AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	m.Lock()
	defer m.Unlock()
	if cset, exists := m.state.GetCPUSet(string(pod.UID), container.Name); exists {
		// 同步当前集合，调谐时据此判断容器是否已是最新分配。
		m.lastUpdateState.SetCPUSet(string(pod.UID), container.Name, cset)
	}
	m.containerMap.Add(string(pod.UID), container.Name, containerID)
	logger.V(4).Info("Added Container", "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "containerID", containerID)
}

// RemoveContainer 按容器 ID 查找并回收对应的 CPU 分配。
func (m *manager) RemoveContainer(logger klog.Logger, containerID string) error {
	m.Lock()
	defer m.Unlock()

	err := m.policyRemoveContainerByID(logger, containerID)
	if err != nil {
		logger.Error(err, "RemoveContainer error")
		return err
	}

	return nil
}

// policyRemoveContainerByID 通过 containerMap 找到容器引用并调用策略移除。
func (m *manager) policyRemoveContainerByID(logger klog.Logger, containerID string) error {
	podUID, containerName, err := m.containerMap.GetContainerRef(containerID)
	// 查不到说明容器从未被记录或已清理，直接忽略即可。
	if err != nil {
		return nil
	}

	// 只有策略成功释放后，才清理本地簿记，避免状态不一致。
	err = m.policy.RemoveContainer(logger, m.state, podUID, containerName)
	if err == nil {
		m.lastUpdateState.Delete(podUID, containerName)
		m.containerMap.RemoveByContainerID(containerID)
	}

	return err
}

// policyRemoveContainerByRef 按 Pod UID 与容器名调用策略移除，并清理簿记。
func (m *manager) policyRemoveContainerByRef(logger klog.Logger, podUID string, containerName string) error {
	// 策略成功移除后同步删除 lastUpdateState 与 containerMap 中的记录。
	err := m.policy.RemoveContainer(logger, m.state, podUID, containerName)
	if err == nil {
		m.lastUpdateState.Delete(podUID, containerName)
		m.containerMap.RemoveByContainerRef(podUID, containerName)
	}

	return err
}

// State 返回内部状态，调用方只能通过 state.Reader 读取。
func (m *manager) State() state.Reader {
	return m.state
}

// GetTopologyHints 先回收陈旧状态，再委托当前策略计算容器的拓扑提示。
func (m *manager) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// 提供拓扑提示前先回收失效资源，避免把已释放的 CPU 算进可用池。
	m.removeStaleState(logger)
	// 具体提示由当前生效策略给出。
	return m.policy.GetTopologyHints(logger, m.state, pod, container, operation)
}

// GetPodTopologyHints 先回收陈旧状态，再委托当前策略计算 Pod 级别的拓扑提示。
func (m *manager) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// 提供拓扑提示前先回收失效资源，避免把已释放的 CPU 算进可用池。
	m.removeStaleState(logger)
	// 具体提示由当前生效策略给出。
	return m.policy.GetPodTopologyHints(logger, m.state, pod, operation)
}

// GetAllocatableCPUs 返回可分配 CPU 的副本，防止调用方修改内部状态。
func (m *manager) GetAllocatableCPUs() cpuset.CPUSet {
	return m.allocatableCPUs.Clone()
}

// GetAllCPUs 返回全部在线 CPU 的副本，防止调用方修改内部状态。
func (m *manager) GetAllCPUs() cpuset.CPUSet {
	return m.allCPUs.Clone()
}

// reconciledContainer 标识一次调谐中处理过的容器。
type reconciledContainer struct {
	// podName 是 Pod 名称，仅用于日志与结果报告。
	podName string
	// containerName 是容器名。
	containerName string
	// containerID 是容器运行时 ID，未找到时为空。
	containerID string
}

// removeStaleState 删除不属于任何活跃 Pod 的容器状态。
func (m *manager) removeStaleState(rootLogger klog.Logger) {
	// 只有所有配置源就绪后才清理陈旧状态，
	// 否则下面 activePods() 返回的列表可能不完整，误删仍在运行的容器。
	if !m.sourcesReady.AllReady() {
		return
	}

	// 加锁保证执行期间不会有新容器抢占 CPU，
	// 否则可能误删 AddContainer() 异步新增的状态。
	m.Lock()
	defer m.Unlock()

	// 获取活跃 Pod 列表。
	activePods := m.activePods()

	// 汇总所有活跃 Pod 中容器的 (podUID, containerName) 对。
	activeContainers := make(map[string]map[string]struct{})
	for _, pod := range activePods {
		activeContainers[string(pod.UID)] = make(map[string]struct{})
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			activeContainers[string(pod.UID)][container.Name] = struct{}{}
		}
	}

	// 遍历状态，删除不在活跃容器集合中的记录。
	// 这些记录对应的容器已不存在。
	assignments := m.state.GetCPUAssignments()
	for podUID := range assignments {
		for containerName := range assignments[podUID] {
			logger := klog.LoggerWithValues(rootLogger, "podUID", podUID, "containerName", containerName)

			if _, ok := activeContainers[podUID][containerName]; ok {
				logger.V(5).Info("container still active")
				continue
			}

			logger.V(2).Info("removing container")
			err := m.policyRemoveContainerByRef(logger, podUID, containerName)
			if err != nil {
				logger.Error(err, "failed to remove container")
			}
		}
	}

	m.containerMap.Visit(func(podUID, containerName, containerID string) {
		logger := klog.LoggerWithValues(rootLogger, "podUID", podUID, "containerName", containerName)
		if _, ok := activeContainers[podUID][containerName]; ok {
			logger.V(5).Info("containerMap: container still active")
			return
		}
		logger.V(2).Info("containerMap: removing container")
		err := m.policyRemoveContainerByRef(logger, podUID, containerName)
		if err != nil {
			logger.Error(err, "containerMap: failed to remove container")
		}
	})
}

// reconcileState 让所有活跃容器的 CPU 集合与 CPU manager 状态保持一致，
// 并返回成功与失败的容器列表。
func (m *manager) reconcileState(ctx context.Context) (success []reconciledContainer, failure []reconciledContainer) {
	success = []reconciledContainer{}
	failure = []reconciledContainer{}

	rootLogger := klog.FromContext(ctx)

	m.removeStaleState(rootLogger)
	for _, pod := range m.activePods() {
		podLogger := klog.LoggerWithValues(rootLogger, "pod", klog.KObj(pod))

		pstatus, ok := m.podStatusProvider.GetPodStatus(pod.UID)
		if !ok {
			podLogger.V(5).Info("skipping pod; status not found")
			failure = append(failure, reconciledContainer{pod.Name, "", ""})
			continue
		}

		allContainers := pod.Spec.InitContainers
		allContainers = append(allContainers, pod.Spec.Containers...)
		for _, container := range allContainers {
			logger := klog.LoggerWithValues(podLogger, "containerName", container.Name)

			containerID, err := findContainerIDByName(&pstatus, container.Name)
			if err != nil {
				logger.V(5).Info("skipping container; ID not found in pod status", "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			cstatus, err := findContainerStatusByName(&pstatus, container.Name)
			if err != nil {
				logger.V(5).Info("skipping container; container status not found in pod status", "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			if cstatus.State.Waiting != nil ||
				(cstatus.State.Waiting == nil && cstatus.State.Running == nil && cstatus.State.Terminated == nil) {
				logger.V(4).Info("skipping container; container still in the waiting state", "err", err)
				failure = append(failure, reconciledContainer{pod.Name, container.Name, ""})
				continue
			}

			m.Lock()
			if cstatus.State.Terminated != nil {
				// 容器已终止，但不能在这里调用 RemoveContainer()，
				// 否则可能删掉正等待重启的容器的独占 CPU 集合，
				// 使其丢失此前获得的独占 CPU；因此这里只跳过并保留状态。
				_, _, err := m.containerMap.GetContainerRef(containerID)
				if err == nil {
					logger.V(4).Info("ignoring terminated container", "containerID", containerID)
				}
				m.Unlock()
				continue
			}

			// 走到这里说明容器处于运行状态。
			// 幂等地补写 containerMap，kubelet 重启后该映射可能缺失。
			m.containerMap.Add(string(pod.UID), container.Name, containerID)

			cset := m.state.GetCPUSetOrDefault(string(pod.UID), container.Name)
			baselineCPUs, hasBaselineCPUs := m.state.GetBaselineCPUSet(string(pod.UID), container.Name)
			m.Unlock()

			if cset.IsEmpty() {
				// 注意：测试之外不应出现空 CPU 集合。
				logger.V(2).Info("ReconcileState: skipping container; empty cpuset assigned")
				failure = append(failure, reconciledContainer{pod.Name, container.Name, containerID})
				continue
			}

			if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.InPlacePodVerticalScalingExclusiveCPUs) {
				// 这里的逻辑之所以不够直观，是因为 GetCPUSetOrDefault API 的约束，
				// 该 API 后续应单独重构。容器只有在首次准入时就获得独占 CPU 分配，
				// 才会有 baseline CPUs；baseline 反映当时承诺的 CPU，之后的扩缩容都不会改动它。
				// 这种情况下 GetCPUSetOrDefault 必然返回独占集合而不是默认共享集合。
				// 更清晰的方案是废弃 GetCPUSetOrDefault，改用同时返回独占集合与共享集合的新 API。
				if hasBaselineCPUs && !baselineCPUs.Equals(cset) {
					logger.V(4).Info("Current container CPU assignments differ from CPU assignments recorded at admission time",
						"currentCPUSet", cset, "baselineCPUSet", baselineCPUs)
				}
			}

			lcset := m.lastUpdateState.GetCPUSetOrDefault(string(pod.UID), container.Name)
			if !cset.Equals(lcset) {
				logger.V(5).Info("updating container", "containerID", containerID, "cpuSet", cset)
				err = m.updateContainerCPUSet(ctx, containerID, cset)
				if err != nil {
					logger.Error(err, "failed to update container", "containerID", containerID, "cpuSet", cset)
					failure = append(failure, reconciledContainer{pod.Name, container.Name, containerID})
					continue
				}
				m.lastUpdateState.SetCPUSet(string(pod.UID), container.Name, cset)
			}
			success = append(success, reconciledContainer{pod.Name, container.Name, containerID})
		}
	}
	return success, failure
}

// findContainerIDByName 在 Pod 状态中查找指定容器的运行时 ID。
func findContainerIDByName(status *v1.PodStatus, name string) (string, error) {
	allStatuses := status.InitContainerStatuses
	allStatuses = append(allStatuses, status.ContainerStatuses...)
	for _, container := range allStatuses {
		if container.Name == name && container.ContainerID != "" {
			cid := &kubecontainer.ContainerID{}
			// 状态中的 ID 可能带运行时前缀（如 docker://），ParseString 会剥离它。
			err := cid.ParseString(container.ContainerID)
			if err != nil {
				return "", err
			}
			return cid.ID, nil
		}
	}
	return "", fmt.Errorf("unable to find ID for container with name %v in pod status (it may not be running)", name)
}

// findContainerStatusByName 在 Pod 状态中查找指定容器的状态。
func findContainerStatusByName(status *v1.PodStatus, name string) (*v1.ContainerStatus, error) {
	for _, containerStatus := range append(status.InitContainerStatuses, status.ContainerStatuses...) {
		if containerStatus.Name == name {
			return &containerStatus, nil
		}
	}
	return nil, fmt.Errorf("unable to find status for container with name %v in pod status (it may not be running)", name)
}

// GetExclusiveCPUs 返回容器独占的 CPU；不存在分配时返回空集合。
func (m *manager) GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet {
	if result, ok := m.state.GetCPUSet(podUID, containerName); ok {
		return result
	}
	return cpuset.New()
}

// GetPodCPUs 返回 Pod 的 CPU 集合；不存在分配时返回空集合。
func (m *manager) GetPodCPUs(podUID string) cpuset.CPUSet {
	if result, ok := m.state.GetPodCPUSet(podUID); ok {
		return result
	}
	return cpuset.New()
}

// GetCPUAffinity 返回容器的 CPU 亲和集合，无独占分配时回退到默认集合。
func (m *manager) GetCPUAffinity(podUID, containerName string) cpuset.CPUSet {
	return m.state.GetCPUSetOrDefault(podUID, containerName)
}

// resourcesQualifyForExclusiveCPUs 判断容器是否满足独占 CPU 的条件：
// 等价 Guaranteed QoS，且 CPU limit 为整数（不含毫核）。
func resourcesQualifyForExclusiveCPUs(container *v1.Container) bool {
	if !cmqos.IsContainerEquivalentQOSGuaranteed(container) {
		return false
	}

	// Value()*1000 == MilliValue() 说明没有小数 CPU，可做整数独占分配。
	cpuLimit := container.Resources.Limits[v1.ResourceCPU]
	return cpuLimit.Value()*1000 == cpuLimit.MilliValue()
}

// GetResourceIsolationLevel 根据是否存在独占 CPU 分配以及 Pod 级资源特性
// 判断隔离级别。
func (m *manager) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	if _, ok := m.state.GetCPUSet(string(pod.UID), container.Name); !ok {
		return cmqos.ResourceIsolationHost
	}

	// 开启 Pod 级资源管理且容器自身不独占 CPU 时，隔离边界退到 Pod 级。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) && !resourcesQualifyForExclusiveCPUs(container) {
		return cmqos.ResourceIsolationPod
	}

	return cmqos.ResourceIsolationContainer
}
