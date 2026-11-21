/*
Copyright 2020 The Kubernetes Authors.

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

package memorymanager

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
)

// memoryManagerStateFileName 是内存管理器保存其状态文件的文件名。
const memoryManagerStateFileName = "memory_manager_state"

// ActivePodsFunc 是一个函数类型，用于返回节点上当前活跃的 Pod 列表。
type ActivePodsFunc func() []*v1.Pod

// runtimeService 是与容器运行时交互的接口，负责向运行时更新容器资源。
type runtimeService interface {
	UpdateContainerResources(ctx context.Context, id string, resources *runtimeapi.ContainerResources) error
}

// sourcesReadyStub 是 config.SourcesReady 的桩实现，表示配置源始终处于就绪状态。
type sourcesReadyStub struct{}

// AddSource 是空实现：桩实现不跟踪任何配置源，直接忽略新添加的源。
func (s *sourcesReadyStub) AddSource(source string) {}

// AllReady 始终返回 true，表示配置源已全部就绪，可以据此清理不再活跃的 Pod 状态。
func (s *sourcesReadyStub) AllReady() bool { return true }

// Manager 接口提供 Kubelet 管理 Pod 内存所需的方法。
type Manager interface {
	// Start 在 Kubelet 初始化阶段被调用，用于加载策略与持久化状态。
	Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error

	// AddContainer 记录容器 ID 到 Pod UID 及容器名的映射。
	// 该映射用于在容器被移除时释放其内存分配。
	AddContainer(logger klog.Logger, p *v1.Pod, c *v1.Container, containerID string)

	// Allocate 在 Pod 准入阶段预先分配内存资源。
	// 必须在为某容器调用 AddContainer() 之前的某个时刻调用，例如在 Pod 准入时。
	Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error

	// RemoveContainer 在 Kubelet 决定杀死或删除容器之后调用。
	// 调用后，分配给该容器的内存会被释放。
	RemoveContainer(logger klog.Logger, containerID string) error

	// State 返回内部内存管理器状态的只读接口。
	State() state.Reader

	// GetTopologyHints 实现 topologymanager.HintProvider 接口，
	// 被拓扑管理器查询以在本控制器与其他资源控制器之间实现 NUMA 感知的资源对齐。
	GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// GetPodTopologyHints 实现 topologymanager.HintProvider 接口，
	// 被拓扑管理器查询以在本控制器与其他资源控制器之间实现 NUMA 感知的资源对齐。
	GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint

	// AllocatePod 用于触发为整个 Pod 分配内存。
	AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error

	// GetMemoryNUMANodes 返回用于分配该容器内存的 NUMA 节点集合。
	GetMemoryNUMANodes(logger klog.Logger, pod *v1.Pod, container *v1.Container) sets.Set[int]

	// GetAllocatableMemory 返回每个 NUMA 节点的可分配内存量。
	GetAllocatableMemory(logger klog.Logger) []state.Block

	// GetMemory 返回某容器从各 NUMA 节点分配到的内存。
	GetMemory(logger klog.Logger, podUID, containerName string) []state.Block

	// GetPodMemory 返回某个 Pod 从各 NUMA 节点分配到的内存。
	GetPodMemory(podUID string) []state.Block

	// GetResourceIsolationLevel 返回该容器的资源隔离级别。
	GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel
}

// manager 是 Manager 接口的具体实现，负责按 NUMA 拓扑分配内存并维护分配状态。
type manager struct {
	sync.Mutex
	policy Policy

	// state 保存 Guaranteed Pod 的内存分配信息，以便 Kubelet 重启后恢复。
	state state.State

	// containerRuntime 是容器运行时服务接口，用于对容器发起 UpdateContainerResources() 调用。
	containerRuntime runtimeService

	// activePods 用于列出节点上的活跃 Pod，
	// 以便在调用 removeStaleState 时能更新所有容器。
	activePods ActivePodsFunc

	// podStatusProvider 提供获取 Pod 状态及其容器 ID 的方法。
	podStatusProvider status.PodStatusProvider

	// containerMap 提供 (pod, container) -> containerID 的映射，
	// 覆盖 Pod 的所有容器。
	containerMap containermap.ContainerMap

	// sourcesReady 表示 Kubelet 配置源（如 apiserver 更新）的就绪情况。
	// 用它来判断何时可以从 checkpoint 状态中清理不再活跃的 Pod。
	sourcesReady config.SourcesReady

	// stateFileDirectory 是存放 checkpoint 状态文件的目录。
	stateFileDirectory string

	// allocatableMemory 保存每个 NUMA 节点的可分配内存。
	allocatableMemory []state.Block
}

// 编译期断言：确保 manager 完整实现了 Manager 接口。
var _ Manager = &manager{}

// NewManager 返回内存管理器的新实例。
// 根据 policyName 选择策略（none、static 或 best-effort），并做必要的平台校验与保留内存计算。
func NewManager(logger klog.Logger, policyName string, machineInfo *cadvisorapi.MachineInfo, nodeAllocatableReservation v1.ResourceList, reservedMemory []kubeletconfig.MemoryReservation, stateFileDirectory string, affinity topologymanager.Store) (Manager, error) {
	var policy Policy

	// 按策略类型构造对应实现；不同策略对平台与输入配置的要求不同。
	switch policyType(policyName) {

	case policyTypeNone:
		policy = NewPolicyNone(logger)

	case PolicyTypeStatic:
		// static 策略依赖 cgroup 与 NUMA 拓扑，Windows 上尚不支持。
		if runtime.GOOS == "windows" {
			return nil, fmt.Errorf("policy %q is not available on Windows", PolicyTypeStatic)
		}

		systemReserved, err := getSystemReservedMemory(machineInfo, nodeAllocatableReservation, reservedMemory)
		if err != nil {
			return nil, err
		}

		policy, err = NewPolicyStatic(logger, machineInfo, systemReserved, affinity)
		if err != nil {
			return nil, err
		}

	case policyTypeBestEffort:
		if runtime.GOOS == "windows" {
			systemReserved, err := getSystemReservedMemory(machineInfo, nodeAllocatableReservation, reservedMemory)
			if err != nil {
				return nil, err
			}
			// Windows 的 BestEffort 策略需要跟随 CPU 管理器的 NUMA 决策，
			// 该决策通过 affinity Store 读取，因此要求更完整的
			// AuthoritativeStore 接口（由 Windows 的 cpuFollowingStore 实现），
			// 仅有普通的 Store 接口不足以提供所需信息。
			authStore, ok := affinity.(topologymanager.AuthoritativeStore)
			if !ok {
				return nil, fmt.Errorf("policy %q requires an AuthoritativeStore affinity", policyTypeBestEffort)
			}
			policy, err = NewPolicyBestEffort(logger, machineInfo, systemReserved, authStore)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("policy %q is not available for platform %q", policyTypeBestEffort, runtime.GOOS)
		}

	default:
		return nil, fmt.Errorf("unknown policy: %q", policyName)
	}

	manager := &manager{
		policy:             policy,
		stateFileDirectory: stateFileDirectory,
	}
	// 在 Start 被真正调用前先用桩实现占位，避免 sourcesReady 为 nil 导致崩溃。
	manager.sourcesReady = &sourcesReadyStub{}
	return manager, nil
}

// Start 在 Kubelet 下启动内存管理器，并调用所选策略的启动逻辑。
func (m *manager) Start(ctx context.Context, activePods ActivePodsFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, containerRuntime runtimeService, initialContainers containermap.ContainerMap) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting memorymanager", "policy", m.policy.Name())
	m.sourcesReady = sourcesReady
	m.activePods = activePods
	m.podStatusProvider = podStatusProvider
	m.containerRuntime = containerRuntime
	m.containerMap = initialContainers

	// 从 checkpoint 恢复状态，保证 Kubelet 重启后仍能识别已分配的 CPU/内存亲和关系。
	stateImpl, err := state.NewCheckpointState(logger, m.stateFileDirectory, memoryManagerStateFileName, m.policy.Name())
	if err != nil {
		logger.Error(err, "Could not initialize checkpoint manager, please drain node and remove policy state file")
		return err
	}
	m.state = stateImpl

	err = m.policy.Start(logger, m.state)
	if err != nil {
		logger.Error(err, "Policy start error")
		return err
	}

	// 策略启动后一次性计算并缓存可分配内存，供后续查询复用。
	m.allocatableMemory = m.policy.GetAllocatableMemory(m.state)

	logger.V(4).Info("memorymanager started", "policy", m.policy.Name())
	return nil
}

// AddContainer 将 Guaranteed Pod 申请的内存记录到状态中，并根据拓扑管理器的决策设置内存亲和性。
func (m *manager) AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	m.Lock()
	defer m.Unlock()

	m.containerMap.Add(string(pod.UID), container.Name, containerID)

	// 每个 init 容器都会在下一个容器启动前运行至完成，
	// 因此可以安全地移除对之前已启动的 init 容器的引用，
	// 从而释放它们占用的内存供其他 Pod 使用。
	// 如果当前添加的正好是 init 容器本身，则跳过删除，等到下一个容器被添加时再清理。
	for _, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == container.Name {
			break
		}

		// 可重启的 init 容器会伴随 Pod 整个生命周期持续运行，
		// 因此不能从内存管理器状态中移除。
		if podutil.IsRestartableInitContainer(&initContainer) {
			continue
		}

		m.policyRemoveContainerByRef(logger, string(pod.UID), initContainer.Name)
	}
}

// GetMemoryNUMANodes 返回用于分配该容器内存的 NUMA 节点集合。
func (m *manager) GetMemoryNUMANodes(logger klog.Logger, pod *v1.Pod, container *v1.Container) sets.Set[int] {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "containerName", container.Name)

	// 汇总 Allocate() 阶段分配给该容器的内存块的 NUMA 亲和性。
	numaNodes := sets.New[int]()
	for _, block := range m.state.GetMemoryBlocks(string(pod.UID), container.Name) {
		for _, nodeID := range block.NUMAAffinity {
			// 大页与普通内存可能固定到同一 NUMA 节点，用集合去重避免重复统计。
			numaNodes.Insert(nodeID)
		}
	}

	if numaNodes.Len() == 0 {
		logger.V(5).Info("NUMA nodes not available for allocation")
		return nil
	}

	logger.Info("Memory affinity", "numaNodes", numaNodes)
	return numaNodes
}

// Allocate 在 Pod 准入阶段预先分配内存资源。
func (m *manager) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	logger := klog.FromContext(ctx)
	// 先回收已不活跃容器的资源，避免陈旧状态导致分配决策出错。
	m.removeStaleState(logger)

	m.Lock()
	defer m.Unlock()

	// 下放到策略层，按需为该容器分配内存。
	if err := m.policy.Allocate(ctx, m.state, pod, container, operation); err != nil {
		logger.Error(err, "Allocate error", "pod", klog.KObj(pod), "containerName", container.Name)
		return err
	}
	return nil
}

// AllocatePod 触发为整个 Pod 分配内存。
func (m *manager) AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error {
	// 先回收陈旧状态，确保 Pod 级分配基于最新的活跃资源视图。
	m.removeStaleState(logger)

	m.Lock()
	defer m.Unlock()

	// 下放到策略层执行 Pod 级内存分配。
	if err := m.policy.AllocatePod(logger, m.state, pod, operation); err != nil {
		logger.Error(err, "AllocatePod error", "pod", klog.KObj(pod))
		return err
	}
	return nil
}

// RemoveContainer 从状态中移除该容器。
func (m *manager) RemoveContainer(logger klog.Logger, containerID string) error {
	logger = klog.LoggerWithValues(logger, "containerID", containerID)

	m.Lock()
	defer m.Unlock()

	// 若此处出错，说明 container map 中本就不存在该容器条目，当作空操作处理。
	podUID, containerName, err := m.containerMap.GetContainerRef(containerID)
	if err != nil {
		logger.Error(err, "Failed to get container from container map")
		return nil
	}

	m.policyRemoveContainerByRef(logger, podUID, containerName)

	return nil
}

// State 返回管理器的状态。
func (m *manager) State() state.Reader {
	return m.state
}

// GetPodTopologyHints 返回给拓扑管理器使用的 Pod 级拓扑提示。
func (m *manager) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// 在提供拓扑提示前先回收滞留资源，避免提示基于过期状态。
	m.removeStaleState(logger)
	// 委托给当前生效的策略。
	return m.policy.GetPodTopologyHints(logger, m.state, pod, operation)
}

// GetTopologyHints 返回给拓扑管理器使用的容器级拓扑提示。
func (m *manager) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	// 在提供拓扑提示前先回收滞留资源，避免提示基于过期状态。
	m.removeStaleState(logger)
	// 委托给当前生效的策略。
	return m.policy.GetTopologyHints(logger, m.state, pod, container, operation)
}

// removeStaleState 清理不再属于任何活跃 Pod 的容器状态。
// TODO: move the method to the upper level, to re-use it under the CPU and memory managers
func (m *manager) removeStaleState(logger klog.Logger) {

	// 只有所有配置源都就绪时才尝试清理陈旧状态，
	// 这能确保下面的 m.activePods() 返回真实的活跃 Pod 列表。
	if !m.sourcesReady.AllReady() {
		return
	}

	// 加锁确保执行期间不会有新容器抢占内存块；
	// 否则可能误删异步 AddContainer() 刚添加的状态。
	m.Lock()
	defer m.Unlock()

	// 获取活跃 Pod 列表。
	activePods := m.activePods()

	// 为所有活跃 Pod 中的所有容器构建 (podUID, containerName) 对集合。
	activeContainers := make(map[string]map[string]struct{})
	for _, pod := range activePods {
		activeContainers[string(pod.UID)] = make(map[string]struct{})
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			activeContainers[string(pod.UID)][container.Name] = struct{}{}
		}
	}

	// 遍历内存管理器状态，移除不在上述 activeContainers 列表中的容器状态。
	assignments := m.state.GetMemoryAssignments()
	for podUID := range assignments {
		for containerName := range assignments[podUID] {
			if _, ok := activeContainers[podUID][containerName]; !ok {
				logger.V(2).Info("RemoveStaleState removing state", "podUID", podUID, "containerName", containerName)
				m.policyRemoveContainerByRef(logger, podUID, containerName)
			}
		}
	}

	// container map 中也可能残留已消失容器的引用，一并清理。
	m.containerMap.Visit(func(podUID, containerName, containerID string) {
		if _, ok := activeContainers[podUID][containerName]; !ok {
			logger.V(2).Info("RemoveStaleState removing state", "podUID", podUID, "containerName", containerName)
			m.policyRemoveContainerByRef(logger, podUID, containerName)
		}
	})
}

// policyRemoveContainerByRef 按 Pod UID 与容器名同时清理策略状态和容器映射。
func (m *manager) policyRemoveContainerByRef(logger klog.Logger, podUID string, containerName string) {
	m.policy.RemoveContainer(logger, m.state, podUID, containerName)
	m.containerMap.RemoveByContainerRef(podUID, containerName)
}

// getTotalMemoryTypeReserved 汇总所有 NUMA 节点上各类内存资源（内存、大页）的保留量。
func getTotalMemoryTypeReserved(machineInfo *cadvisorapi.MachineInfo, reservedMemory []kubeletconfig.MemoryReservation) (map[v1.ResourceName]resource.Quantity, error) {
	totalMemoryType := map[v1.ResourceName]resource.Quantity{}

	// 先记录机器上实际存在的 NUMA 节点，用于校验保留配置的合法性。
	numaNodes := map[int]bool{}
	for _, numaNode := range machineInfo.Topology {
		numaNodes[numaNode.Id] = true
	}

	for _, reservation := range reservedMemory {
		if !numaNodes[int(reservation.NumaNode)] {
			return nil, fmt.Errorf("the reserved memory configuration references a NUMA node %d that does not exist on this machine", reservation.NumaNode)
		}

		for resourceName, q := range reservation.Limits {
			if value, ok := totalMemoryType[resourceName]; ok {
				q.Add(value)
			}
			totalMemoryType[resourceName] = q
		}
	}

	return totalMemoryType, nil
}

// validateReservedMemory 校验按 NUMA 节点配置的保留内存总量与 Node Allocatable 计算出的值一致。
func validateReservedMemory(machineInfo *cadvisorapi.MachineInfo, nodeAllocatableReservation v1.ResourceList, reservedMemory []kubeletconfig.MemoryReservation) error {
	totalMemoryType, err := getTotalMemoryTypeReserved(machineInfo, reservedMemory)
	if err != nil {
		return err
	}

	// 收集所有需要参与比对的资源类型：既包含保留配置中的类型，
	// 也包含 Node Allocatable 中的内存和大页类型，取两者并集。
	commonMemoryTypeSet := make(map[v1.ResourceName]bool)
	for resourceType := range totalMemoryType {
		commonMemoryTypeSet[resourceType] = true
	}

	for resourceType := range nodeAllocatableReservation {
		if !(corev1helper.IsHugePageResourceName(resourceType) || resourceType == v1.ResourceMemory) {
			continue
		}
		commonMemoryTypeSet[resourceType] = true
	}

	for resourceType := range commonMemoryTypeSet {
		nodeAllocatableMemory := resource.NewQuantity(0, resource.DecimalSI)
		if memValue, set := nodeAllocatableReservation[resourceType]; set {
			nodeAllocatableMemory.Add(memValue)
		}

		reservedMemory := resource.NewQuantity(0, resource.DecimalSI)
		if memValue, set := totalMemoryType[resourceType]; set {
			reservedMemory.Add(memValue)
		}

		// 两者必须完全相等，否则说明保留配置与 Node Allocatable 特性计算不一致。
		if !(*nodeAllocatableMemory).Equal(*reservedMemory) {
			return fmt.Errorf("the total amount %q of type %q is not equal to the value %q determined by Node Allocatable feature", reservedMemory.String(), resourceType, nodeAllocatableMemory.String())
		}
	}

	return nil
}

// convertReserved 把按 NUMA 节点配置的保留内存转换为以节点 ID 为键的映射。
func convertReserved(machineInfo *cadvisorapi.MachineInfo, reservedMemory []kubeletconfig.MemoryReservation) (systemReservedMemory, error) {
	reservedMemoryConverted := make(map[int]map[v1.ResourceName]uint64)
	for _, node := range machineInfo.Topology {
		reservedMemoryConverted[node.Id] = make(map[v1.ResourceName]uint64)
	}

	for _, reservation := range reservedMemory {
		for resourceName, q := range reservation.Limits {
			// 策略层使用 uint64 表示内存字节数，需要通过 Quantity 显式转换。
			val, success := q.AsInt64()
			if !success {
				return nil, fmt.Errorf("could not covert a variable of type Quantity to int64")
			}
			reservedMemoryConverted[int(reservation.NumaNode)][resourceName] = uint64(val)
		}
	}

	return reservedMemoryConverted, nil
}

// getSystemReservedMemory 校验并转换系统保留内存配置，返回按 NUMA 节点划分的保留内存。
func getSystemReservedMemory(machineInfo *cadvisorapi.MachineInfo, nodeAllocatableReservation v1.ResourceList, reservedMemory []kubeletconfig.MemoryReservation) (systemReservedMemory, error) {
	// 先校验配置与 Node Allocatable 的一致性，避免后续用错误的保留量初始化策略。
	if err := validateReservedMemory(machineInfo, nodeAllocatableReservation, reservedMemory); err != nil {
		return nil, err
	}

	reservedMemoryConverted, err := convertReserved(machineInfo, reservedMemory)
	if err != nil {
		return nil, err
	}

	return reservedMemoryConverted, nil
}

// GetAllocatableMemory 返回每个 NUMA 节点的可分配内存量。
func (m *manager) GetAllocatableMemory(_ klog.Logger) []state.Block {
	return m.allocatableMemory
}

// GetMemory 返回某容器从各 NUMA 节点分配到的内存。
func (m *manager) GetMemory(_ klog.Logger, podUID, containerName string) []state.Block {
	return m.state.GetMemoryBlocks(podUID, containerName)
}

// GetPodMemory 返回某个 Pod 从各 NUMA 节点分配到的内存。
func (m *manager) GetPodMemory(podUID string) []state.Block {
	return m.state.GetPodMemoryBlocks(podUID)
}

// GetResourceIsolationLevel 返回该容器的资源隔离级别。
func (m *manager) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	// 没有分配任何内存块，说明容器未绑定 NUMA 内存，仅属于主机级隔离。
	if len(m.state.GetMemoryBlocks(string(pod.UID), container.Name)) == 0 {
		return cmqos.ResourceIsolationHost
	}

	// 启用 Pod 级资源且 Pod 设置了资源、而容器本身不等价于 Guaranteed 时，
	// 隔离边界应提升到 Pod 级。
	if feature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) && !cmqos.IsContainerEquivalentQOSGuaranteed(container) {
		return cmqos.ResourceIsolationPod
	}

	return cmqos.ResourceIsolationContainer
}
