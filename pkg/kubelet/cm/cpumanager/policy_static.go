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

// Package cpumanager 实现 kubelet 的 CPU 管理器，包含 static 与 none 两种策略，负责独占 CPU 的分配与回收。
package cpumanager

import (
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/cpuset"
)

const (
	// PolicyStatic 是 static 策略的名称。
	// 在 1.21 及更早的兼容模式下，策略选项会被忽略，
	// 以保持向后兼容行为。
	PolicyStatic policyName = "static"
	// ErrorSMTAlignment 是 SMTAlignmentError 的类型标识，用于准入失败原因。
	ErrorSMTAlignment = "SMTAlignmentError"
)

// SMTAlignmentError 表示因 SMT（超线程）对齐要求无法满足而产生的错误。
type SMTAlignmentError struct {
	RequestedCPUs         int
	CpusPerCore           int
	AvailablePhysicalCPUs int
	CausedByPhysicalCPUs  bool
}

// Error 返回可读的 SMT 对齐错误信息。
func (e SMTAlignmentError) Error() string {
	if e.CausedByPhysicalCPUs {
		return fmt.Sprintf("SMT Alignment Error: not enough free physical CPUs: available physical CPUs = %d, requested CPUs = %d, CPUs per core = %d", e.AvailablePhysicalCPUs, e.RequestedCPUs, e.CpusPerCore)
	}
	return fmt.Sprintf("SMT Alignment Error: requested %d cpus not multiple cpus per core = %d", e.RequestedCPUs, e.CpusPerCore)
}

// Type 返回该错误的类型名，供准入控制填充失败原因。
func (e SMTAlignmentError) Type() string {
	return ErrorSMTAlignment
}

// staticPolicy 在容器主进程启动后不再改变其独占 CPU 分配。
//
// 仅当 Pod 为 Guaranteed 且 CPU request 为正整数时，才会为容器分配独占 CPU。
//
// 该策略维护以下逻辑 CPU 集合：
//
//   - SHARED：Burstable、BestEffort 以及非整数 Guaranteed 容器运行于此。
//     初始为系统全部 CPU；独占分配创建/销毁时相应收缩与扩张。
//     该集合以默认 CPU 集的形式存储在 state 中。
//
//   - RESERVED：SHARED 的子集，不可用于独占分配。成员在 kubelet 生命周期内固定，
//     大小为 ceil(systemreserved.cpu + kubereserved.cpu)。预留 CPU 按 cAdvisor 报告的拓扑，
//     从编号最小的物理核开始选取。
//
//   - ASSIGNABLE：等于 SHARED - RESERVED，独占 CPU 从此池分配。
//
//   - EXCLUSIVE ALLOCATIONS：独占分配给单个容器的 CPU 集，以显式分配记录保存在 state 中。
//
// 独占分配创建后，默认 cpuset 也会同步更新；CPU 管理器的周期性对账会把最新 cpuset 写回 cgroupfs。
// 因此独占容器中的应用需容忍最多一个对账周期内的 CPU 共享——这是 static 策略的已知权衡。
type staticPolicy struct {
	// topology 是 CPU socket/NUMA 拓扑信息。
	topology *topology.CPUTopology
	// reservedCPUs 是不可用于独占分配的 CPU 集合。
	reservedCPUs cpuset.CPUSet
	// reservedPhysicalCPUs 是 reservedCPUs 的超集，还包括这些 CPU 在同一物理核上的兄弟线程。
	// 注意：若预留集合一开始就包含完整物理核，该集合与 reservedCPUs 相同。
	// 这是 FullPhysicalCPUsOnly 计算可用核数的基础。
	reservedPhysicalCPUs cpuset.CPUSet
	// affinity 用于获取 TopologyManager 给出的容器亲和性提示。
	affinity topologymanager.Store
	// cpusToReuse 记录 Pod 内可跨分配复用的 CPU（主要来自 init 容器）。
	cpusToReuse map[string]cpuset.CPUSet
	// options 用于微调策略行为。
	options StaticPolicyOptions
	// cpuGroupSize 会被多次使用，且运行期不应变化；CPU 管理器本身也不支持运行时拓扑变化。
	cpuGroupSize int
}

// 确保 staticPolicy 实现 Policy 接口。
var _ Policy = &staticPolicy{}

// NewStaticPolicy 创建 static 策略：独占容器在启动后不再改变 CPU 分配。
// 创建时会校验选项并计算预留 CPU 集合。
func NewStaticPolicy(logger klog.Logger, topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, affinity topologymanager.Store, cpuPolicyOptions map[string]string) (Policy, error) {
	opts, err := NewStaticPolicyOptions(cpuPolicyOptions)
	if err != nil {
		return nil, err
	}
	err = ValidateStaticPolicyOptions(opts, topology, affinity)
	if err != nil {
		return nil, err
	}

	cpuGroupSize := topology.CPUsPerCore()
	logger.Info("created with configuration", "options", opts, "cpuGroupSize", cpuGroupSize)

	policy := &staticPolicy{
		topology:     topology,
		affinity:     affinity,
		cpusToReuse:  make(map[string]cpuset.CPUSet),
		options:      opts,
		cpuGroupSize: cpuGroupSize,
	}

	allCPUs := topology.CPUDetails.CPUs()
	var reserved cpuset.CPUSet
	if reservedCPUs.Size() > 0 {
		reserved = reservedCPUs
	} else {
		// 未显式指定预留 CPU 时，按拓扑从编号较小的核开始选取，保证结果稳定可预测。
		// takeByTopology 会返回与低编号核关联的 CPU。
		//
		// 例如 8 个 CPU 且开启超线程时，numReservedCPUs=2 会得到 {0,4}，
		// 即同一物理核的两个线程。
		reserved, _ = policy.takeByTopology(logger, allCPUs, numReservedCPUs)
	}

	if reserved.Size() != numReservedCPUs {
		err := fmt.Errorf("[cpumanager] unable to reserve the required amount of CPUs (size of %s did not equal %d)", reserved, numReservedCPUs)
		return nil, err
	}

	// 计算预留 CPU 的完整物理核集合，供 SMT 对齐判断使用。
	var reservedPhysicalCPUs cpuset.CPUSet
	for _, cpu := range reserved.UnsortedList() {
		core, err := topology.CPUCoreID(cpu)
		if err != nil {
			return nil, fmt.Errorf("[cpumanager] unable to build the reserved physical CPUs from the reserved set: %w", err)
		}
		reservedPhysicalCPUs = reservedPhysicalCPUs.Union(topology.CPUDetails.CPUsInCores(core))
	}

	logger.Info("Reserved CPUs not available for exclusive assignment", "reservedSize", reserved.Size(), "reserved", reserved, "reservedPhysicalCPUs", reservedPhysicalCPUs)
	policy.reservedCPUs = reserved
	policy.reservedPhysicalCPUs = reservedPhysicalCPUs

	return policy, nil
}

// Name 返回策略名。
func (p *staticPolicy) Name() string {
	return string(PolicyStatic)
}

// Start 校验并初始化策略状态，随后初始化指标。
func (p *staticPolicy) Start(logger klog.Logger, s state.State) error {
	if err := p.validateState(logger, s); err != nil {
		logger.Error(err, "invalid state, please drain node and remove policy state file")
		return err
	}
	p.initializeMetrics(logger, s)
	return nil
}

// validateState 校验 checkpoint 中的状态与当前拓扑、预留配置是否一致。
func (p *staticPolicy) validateState(logger klog.Logger, s state.State) error {
	tmpAssignments := s.GetCPUAssignments()
	tmpDefaultCPUset := s.GetDefaultCPUSet()

	allCPUs := p.topology.CPUDetails.CPUs()
	if p.options.StrictCPUReservation {
		allCPUs = allCPUs.Difference(p.reservedCPUs)
	}

	// 存在分配时默认 cpuset 不能为空，否则状态不一致。
	if tmpDefaultCPUset.IsEmpty() {
		if len(tmpAssignments) != 0 {
			return fmt.Errorf("default cpuset cannot be empty")
		}
		// 空状态视为首次启动，用全部 CPU 初始化默认集合。
		s.SetDefaultCPUSet(allCPUs)
		logger.Info("Static policy initialized", "defaultCPUSet", allCPUs)
		return nil
	}

	// 状态已从文件恢复，需要校验其一致性。
	// 1. 预留 CPU 与默认 cpuset 的关系必须符合当前配置：
	//    - 系统预留量变化可能导致部分容器无法启动；也可能是用户手工修改了状态文件。
	if p.options.StrictCPUReservation {
		if !p.reservedCPUs.Intersection(tmpDefaultCPUset).IsEmpty() {
			return fmt.Errorf("some of strictly reserved cpus: %q are present in defaultCpuSet: %q",
				p.reservedCPUs.Intersection(tmpDefaultCPUset).String(), tmpDefaultCPUset.String())
		}
	} else {
		if !p.reservedCPUs.Intersection(tmpDefaultCPUset).Equals(p.reservedCPUs) {
			return fmt.Errorf("not all reserved cpus: %q are present in defaultCpuSet: %q",
				p.reservedCPUs.String(), tmpDefaultCPUset.String())
		}
	}

	// 2. 检查默认集合与各容器分配是否重叠。
	for pod := range tmpAssignments {
		for container, cset := range tmpAssignments[pod] {
			// 默认集合中的 CPU 不应出现在任何独占分配中。
			if !tmpDefaultCPUset.Intersection(cset).IsEmpty() {
				return fmt.Errorf("pod: %s, container: %s cpuset: %q overlaps with default cpuset %q",
					pod, container, cset.String(), tmpDefaultCPUset.String())
			}
		}
	}

	// 3. 状态写入后可用 CPU 集合可能变化（例如离线 CPU），这会导致后续分配不存在的 CPU。
	// 因此需要确认启动时拓扑与状态中记录的 CPU 完全一致；不一致时要求用户 drain 节点并删除状态文件。
	// 校验范围包括默认集合、容器分配与 Pod 级分配，三者并集必须等于当前全部 CPU。
	totalKnownCPUs := tmpDefaultCPUset.Clone()
	tmpCPUSets := []cpuset.CPUSet{}
	for pod := range tmpAssignments {
		for _, cset := range tmpAssignments[pod] {
			tmpCPUSets = append(tmpCPUSets, cset)
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		for _, podAssigments := range s.GetPodCPUAssignments() {
			tmpCPUSets = append(tmpCPUSets, podAssigments.CPUSet)
		}
	}
	totalKnownCPUs = totalKnownCPUs.Union(tmpCPUSets...)
	if !totalKnownCPUs.Equals(allCPUs) {
		return fmt.Errorf("current set of available CPUs %q doesn't match with CPUs in state %q",
			allCPUs.String(), totalKnownCPUs.String())

	}

	return nil
}

// GetAllocatableCPUs 返回可供分配的全部 CPU（已扣除预留）。
func (p *staticPolicy) GetAllocatableCPUs(s state.State) cpuset.CPUSet {
	return p.topology.CPUDetails.CPUs().Difference(p.reservedCPUs)
}

// GetAvailableCPUs 返回未分配且非预留的 CPU。
func (p *staticPolicy) GetAvailableCPUs(s state.State) cpuset.CPUSet {
	return s.GetDefaultCPUSet().Difference(p.reservedCPUs)
}

// updateCPUsToReuse 维护 Pod 内可复用的 CPU 集合。
func (p *staticPolicy) updateCPUsToReuse(pod *v1.Pod, container *v1.Container, cset cpuset.CPUSet) {
	// 只保留当前 Pod 的条目，避免旧 Pod 数据无限累积。
	for podUID := range p.cpusToReuse {
		if podUID != string(pod.UID) {
			delete(p.cpusToReuse, podUID)
		}
	}
	// 首次为该 Pod 分配时创建空集合。
	if _, ok := p.cpusToReuse[string(pod.UID)]; !ok {
		p.cpusToReuse[string(pod.UID)] = cpuset.New()
	}
	// init 容器的 CPU 在结束后可供后续容器复用。
	// 因此把其 cpuset 并入复用集合。
	for _, initContainer := range pod.Spec.InitContainers {
		if container.Name == initContainer.Name {
			if podutil.IsRestartableInitContainer(&initContainer) {
				// 重启式 init 容器会与普通容器同时运行，其 CPU 不能复用，否则会冲突。
				// 因此直接跳出，不加入复用集合。
				break
			}
			p.cpusToReuse[string(pod.UID)] = p.cpusToReuse[string(pod.UID)].Union(cset)
			return
		}
	}
	// 普通容器会长期占用其 CPU，需从复用集合中移除。
	// 这样后续容器不会拿到已被占用的 CPU。
	p.cpusToReuse[string(pod.UID)] = p.cpusToReuse[string(pod.UID)].Difference(cset)
}

// validatePodScopeResources 检查“共享池为空”的非法配置：当独占容器的 CPU 请求耗尽整个 Pod 预算时，
// 需要共享池的容器将没有可用 CPU，从而拿到空 cpuset，回落到节点共享池并破坏 NUMA 亲和性。
// 因此必须在分配前显式拒绝。
func (p *staticPolicy) validatePodScopeResources(logger klog.Logger, pod *v1.Pod) error {
	podTotalCPUs := p.podGuaranteedCPUs(logger, pod)

	hasSharedLongRunningContainers := false
	// 只统计与 Pod 同生命周期的容器（普通容器与重启式 init 容器），其独占 CPU 会持续占用 Pod 预算。
	// 标准 init 容器只在启动阶段运行，不计入长期占用。
	sumOfLongRunningExclusiveCPUs := 0

	// 标准 init 容器需要共享池时，必须确保池非空。
	for _, container := range pod.Spec.InitContainers {
		guaranteedCPUs := p.guaranteedCPUs(logger, pod, &container)

		if !podutil.IsRestartableInitContainer(&container) {
			if guaranteedCPUs == 0 && sumOfLongRunningExclusiveCPUs >= podTotalCPUs {
				// 若 Pod 预算已被 sidecar 占满，标准 init 容器将无法启动；提前拒绝以避免非法 cgroup。
				return fmt.Errorf("pod rejected, pod has shared init containers but no cpus available for them")
			}
		} else {
			// 重启式 init 容器与普通容器同等看待，参与长期占用统计。
			if guaranteedCPUs == 0 {
				hasSharedLongRunningContainers = true
			} else {
				sumOfLongRunningExclusiveCPUs += guaranteedCPUs
			}
		}
	}

	// 再检查并发运行的容器（普通容器与重启式 init 容器）。
	for _, container := range pod.Spec.Containers {
		guaranteedCPUs := p.guaranteedCPUs(logger, pod, &container)

		if guaranteedCPUs == 0 {
			hasSharedLongRunningContainers = true
		} else {
			sumOfLongRunningExclusiveCPUs += guaranteedCPUs
		}
	}

	// 若存在需要共享池的容器，而独占请求已占满预算，则拒绝，否则共享容器会拿到空 cpuset。
	if hasSharedLongRunningContainers && sumOfLongRunningExclusiveCPUs >= podTotalCPUs {
		return fmt.Errorf("pod rejected, sum of exclusive container cpu requests equals pod budget, leaving no cpus for shared containers")
	}

	return nil
}

// AllocatePod 是 Pod 级资源分配的入口，由 TopologyManager 的 Pod 级准入处理器对每个 Pod 调用一次。
// 先为整个 Pod 分配一个 CPU “气泡”，再在容器之间划分该气泡。
func (p *staticPolicy) AllocatePod(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) (rerr error) {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "podUID", pod.UID, "operation", operation)
	logger.V(4).Info("AllocatePod called for pod-level managed pod")

	// static 策略只支持 Add 操作，其他操作直接跳过。
	switch operation {
	case lifecycle.AddOperation:
		return p.allocatePodForAdd(logger, s, pod)
	default:
		logger.V(2).Info("CPU Manager pod-level resource allocation skipped, operation not supported by the static CPU manager policy")
		return nil
	}
}

// allocatePodForAdd 执行 Pod 级 CPU 分配：计算总量、校验、分配气泡并划分给容器。
func (p *staticPolicy) allocatePodForAdd(logger klog.Logger, s state.State, pod *v1.Pod) (rerr error) {
	podUID := string(pod.UID)
	// 1. 计算 Pod 需要的 CPU 总数，已考虑 init 容器复用。
	totalPodCPUs := p.podGuaranteedCPUs(logger, pod)
	if totalPodCPUs == 0 {
		// 无独占 CPU 需求，使用默认共享池即可。
		return nil
	}
	logger.V(4).Info("Calculated total pod CPUs", "totalCPUs", totalPodCPUs)

	defer func() {
		if rerr != nil {
			metrics.CPUManagerPinningErrorsTotal.Inc()
			metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerPod).Inc()
		}
	}()

	// 2. 再次校验共享池为空的情况。提示生成阶段已检查过，但这里必须重复检查：
	// best-effort/none 策略下即使没有提示也会准入 Pod，只有在这里拒绝才能防止创建非法 cgroup。
	if err := p.validatePodScopeResources(logger, pod); err != nil {
		return admission.NewEmptyPodSharedPoolError(err)
	}

	// 3. 开启 FullPhysicalCPUsOnly 时强制 SMT 对齐。
	if err := p.enforceSMTAlignment(s, totalPodCPUs); err != nil {
		return err
	}

	// 4. 按 TopologyManager 提示为 Pod 分配整个 CPU 气泡。
	hint := p.affinity.GetAffinity(logger, podUID, append(pod.Spec.InitContainers, pod.Spec.Containers...)[0].Name)
	podAllocation, err := p.allocateCPUs(logger, s, totalPodCPUs, hint.NUMANodeAffinity, cpuset.New())
	if err != nil {
		logger.Error(err, "Unable to allocate CPUs for pod", "totalPodCPUs", totalPodCPUs)
		return err
	}
	p.updateMetricsOnAllocate(logger, s, podAllocation)
	logger.V(4).Info("Allocated pod-level CPU bubble", "allocation", podAllocation.CPUs)

	// 先把 Pod 级分配写入状态，供容器划分使用。
	s.SetPodCPUSet(podUID, podAllocation.CPUs)

	// 5. 在容器间划分气泡，并正确处理 init 容器的 CPU 复用。
	exclusiveCPUs := make(map[string]cpuset.CPUSet)
	sidecarCPUs := cpuset.New()

	// 先处理 init 容器：可用池是整个气泡减去 sidecar 已占部分。
	for _, c := range pod.Spec.InitContainers {
		if numCPUs := p.guaranteedCPUs(logger, pod, &c); numCPUs > 0 {
			metrics.CPUManagerPinningRequestsTotal.Inc()
			// 标准 init 容器结束后其 CPU 可被后续容器复用。
			runnablePool := podAllocation.CPUs.Difference(sidecarCPUs)
			cset, err := p.takeByTopology(logger, runnablePool, numCPUs)
			if err != nil {
				return err
			}
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerExclusivePod).Inc()
			exclusiveCPUs[c.Name] = cset

			// 重启式 init 容器与普通容器并发运行，因此永久占用。
			if podutil.IsRestartableInitContainer(&c) {
				sidecarCPUs = sidecarCPUs.Union(cset)
			}
		} else {
			// 无独占需求的 sidecar 使用 Pod 共享池。
			if podutil.IsRestartableInitContainer(&c) {
				continue
			}

			// 标准 init 容器可访问整个 Pod 池（扣除 sidecar），因为它们运行期间其他普通容器尚未启动。
			runnablePool := podAllocation.CPUs.Difference(sidecarCPUs)
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerSharedPod).Inc()
			exclusiveCPUs[c.Name] = runnablePool
		}
	}

	// 标准 init 容器结束后，整个气泡（扣除 sidecar）重新可用，无需继承 init 容器的复杂记账。
	podSharedPool := podAllocation.CPUs.Difference(sidecarCPUs)

	// 再为普通容器从剩余池中分配，并逐个扣减。
	for _, c := range pod.Spec.Containers {
		if numCPUs := p.guaranteedCPUs(logger, pod, &c); numCPUs > 0 {
			metrics.CPUManagerPinningRequestsTotal.Inc()
			cset, err := p.takeByTopology(logger, podSharedPool, numCPUs)
			if err != nil {
				return err
			}
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerExclusivePod).Inc()
			exclusiveCPUs[c.Name] = cset
			// 已分配的 CPU 必须从共享池中扣除。
			podSharedPool = podSharedPool.Difference(cset)
		}
	}

	logger.V(4).Info("Partitioned pod-level CPU allocation", "exclusiveCPUs", podCPUAllocationToString(exclusiveCPUs), "podSharedPool", podSharedPool)

	// 6. 保存所有容器分配；无独占需求的容器记录为共享池。
	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if cset, isExclusive := exclusiveCPUs[c.Name]; isExclusive {
			s.SetCPUSet(podUID, c.Name, cset)
		} else {
			s.SetCPUSet(podUID, c.Name, podSharedPool)
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerSharedPod).Inc()
		}

		metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerPod).Inc()
	}

	return nil
}

// podCPUAllocationToString 把容器到 CPU 集合的映射格式化为紧凑字符串，便于日志输出。
func podCPUAllocationToString(alloc map[string]cpuset.CPUSet) string {
	if len(alloc) == 0 {
		return ""
	}
	var sb strings.Builder
	for name, cset := range alloc {
		fmt.Fprintf(&sb, ",%s={%s}", name, cset)
	}
	// 能走到这里说明至少有一个分配，去掉开头的逗号是安全的。
	repr := sb.String()[1:]
	return "<" + repr + ">"
}

// enforceSMTAlignment 校验 CPU 请求是否满足 FullPhysicalCPUsOnly 的 SMT 对齐要求。
func (p *staticPolicy) enforceSMTAlignment(s state.State, numCPUs int) error {
	if !p.options.FullPhysicalCPUsOnly {
		return nil
	}

	if (numCPUs % p.cpuGroupSize) != 0 {
		// 开启严格 SMT 对齐后，Guaranteed Pod 的 CPU 请求必须是每物理核线程数的整数倍，
		// 否则 Pod 会以 SMTAlignmentError 失败。分配以物理核为单位，因此不会出现只分配单个线程的情况；
		// 调度器负责把工作负载放到物理核足够的节点上。
		return SMTAlignmentError{
			RequestedCPUs:        numCPUs,
			CpusPerCore:          p.cpuGroupSize,
			CausedByPhysicalCPUs: false,
		}
	}

	availablePhysicalCPUs := s.GetDefaultCPUSet().Difference(p.reservedPhysicalCPUs).Size()

	// 允许预留非兄弟线程的 CPU，但此时分配器可能退化到单核分配，破坏 FullPhysicalCPUsOnly 的保证。
	// 因此计算可用 CPU 时把预留 CPU 的兄弟线程也视为不可用，使按 SMT 粒度计算的可用量正确。
	if numCPUs > availablePhysicalCPUs {
		return SMTAlignmentError{
			RequestedCPUs:         numCPUs,
			CpusPerCore:           p.cpuGroupSize,
			AvailablePhysicalCPUs: availablePhysicalCPUs,
			CausedByPhysicalCPUs:  true,
		}
	}
	return nil
}

// Allocate 为容器分配独占 CPU；只支持 Add 操作。
func (p *staticPolicy) Allocate(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) (rerr error) {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "operation", operation)
	logger.Info("Allocate start") // V=0 用于向后兼容。
	defer logger.V(2).Info("Allocate end")

	switch operation {
	case lifecycle.AddOperation:
		return p.allocateForAdd(logger, s, pod, container)
	default:
		logger.V(2).Info("CPU Manager container-level resource allocation skipped, operation not supported by the static CPU manager policy")
		return nil
	}
}

// allocateForAdd 执行容器级独占 CPU 分配。
func (p *staticPolicy) allocateForAdd(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container) (rerr error) {
	numCPUs := p.guaranteedCPUs(logger, pod, container)
	if numCPUs == 0 {
		// 无独占需求，使用默认共享池。
		return nil
	}

	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(2).Info("CPU Manager allocation skipped, pod is using pod-level resources but the PodLevelResourceManagers feature gate is not enabled", "pod", klog.KObj(pod), "podUID", pod.UID)
		return nil
	}

	// 容器需要独占 CPU，进入分配流程。
	metrics.CPUManagerPinningRequestsTotal.Inc()
	defer func() {
		if rerr != nil {
			metrics.CPUManagerPinningErrorsTotal.Inc()
			metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerNode).Inc()
			if p.options.FullPhysicalCPUsOnly {
				metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedPhysicalCPU).Inc()
			}
			return
		}
		// TODO: 将该指标更新移入 updateMetricsOnAllocate。
		if p.options.FullPhysicalCPUsOnly {
			// 只在确定分配了对齐资源时累加成功计数。
			metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedPhysicalCPU).Inc()
		}
	}()

	// 先检查已有分配、再做 SMT 对齐校验，这样即使当前策略更严格，已运行的容器也不会被打断。
	// 例如 kubelet 从 full-pcpus-only=false 重启为 true 时，已有的非对齐分配仍会被接受。
	if cset, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
		p.updateCPUsToReuse(pod, container, cset)
		logger.Info("Static policy: container already present in state, skipping")
		return nil
	}

	if err := p.enforceSMTAlignment(s, numCPUs); err != nil {
		return err
	}

	// 从 TopologyManager 获取所有提示提供者对齐后的亲和性。
	hint := p.affinity.GetAffinity(logger, string(pod.UID), container.Name)
	logger.Info("Topology Affinity", "affinity", hint)

	// 按提示中的 NUMA 亲和性分配 CPU。
	cpuAllocation, err := p.allocateCPUs(logger, s, numCPUs, hint.NUMANodeAffinity, p.cpusToReuse[string(pod.UID)])
	if err != nil {
		logger.Error(err, "Unable to allocate CPUs", "numCPUs", numCPUs)
		return err
	}

	s.SetCPUSet(string(pod.UID), container.Name, cpuAllocation.CPUs)
	p.updateCPUsToReuse(pod, container, cpuAllocation.CPUs)
	p.updateMetricsOnAllocate(logger, s, cpuAllocation)
	metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerNode).Inc()
	metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerCPU, metrics.ResourceManagerExclusiveNode).Inc()

	logger.V(4).Info("Allocated exclusive CPUs", "cpuset", cpuAllocation.CPUs.String())
	return nil
}

// getAssignedCPUsOfSiblings 返回同一 Pod 内除指定容器外其他容器已分配的 CPU 集合。
func getAssignedCPUsOfSiblings(s state.State, podUID string, containerName string) cpuset.CPUSet {
	assignments := s.GetCPUAssignments()
	cset := cpuset.New()
	for name, cpus := range assignments[podUID] {
		if containerName == name {
			continue
		}
		cset = cset.Union(cpus)
	}
	return cset
}

// RemoveContainer 释放容器占用的 CPU；Pod 级分配在最后一个容器移除时整体释放。
func (p *staticPolicy) RemoveContainer(logger klog.Logger, s state.State, podUID string, containerName string) error {
	logger = klog.LoggerWithValues(logger, "podUID", podUID, "containerName", containerName)
	logger.Info("RemoveContainer start") // 用于向后兼容。
	defer logger.V(4).Info("RemoveContainer start")
	toRelease, ok := s.GetCPUSet(podUID, containerName)
	if !ok {
		return nil
	}
	s.Delete(podUID, containerName)

	// Pod 级分配模式下，CPU 属于整个 Pod，而非单个容器。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		if podCPUSet, isPodLevel := s.GetPodCPUSet(podUID); isPodLevel {
			// 最后一个容器移除时才释放整个 Pod 的 CPU 集合。
			if len(s.GetCPUAssignments()[podUID]) == 0 {
				updatedCPUSets := s.GetDefaultCPUSet().Union(podCPUSet)
				s.SetDefaultCPUSet(updatedCPUSets)
				s.DeletePod(podUID) // 清理该 Pod 的所有状态。
				p.updateMetricsOnRelease(logger, s, podCPUSet)
				logger.Info("Released pod-level CPUs", "defaultCPUSet", updatedCPUSets)
			}
			// 还有其他容器时不能释放，否则会破坏仍在运行的容器；等最后一个容器移除时再整体释放。
			return nil
		}
	}

	cpusInUse := getAssignedCPUsOfSiblings(s, podUID, containerName)
	// 只释放未被同 Pod 其他容器占用的 CPU，避免重复归还。
	toRelease = toRelease.Difference(cpusInUse)
	updatedCPUs := s.GetDefaultCPUSet().Union(toRelease)
	s.SetDefaultCPUSet(updatedCPUs)
	p.updateMetricsOnRelease(logger, s, toRelease)
	logger.Info("RemoveContainer end", "defaultCPUSet", updatedCPUs)
	return nil
}

// allocateCPUs 在可用与可复用 CPU 中按亲和性分配指定数量的 CPU。
func (p *staticPolicy) allocateCPUs(logger klog.Logger, s state.State, numCPUs int, numaAffinity bitmask.BitMask, reusableCPUs cpuset.CPUSet) (topology.Allocation, error) {
	logger.Info("AllocateCPUs", "numCPUs", numCPUs, "socket", numaAffinity)

	allocatableCPUs := p.GetAvailableCPUs(s).Union(reusableCPUs)

	// 优先从亲和掩码覆盖的 CPU 中分配，以满足 NUMA 对齐。
	result := topology.EmptyAllocation()
	if numaAffinity != nil {
		alignedCPUs := p.getAlignedCPUs(numaAffinity, allocatableCPUs)

		numAlignedToAlloc := alignedCPUs.Size()
		if numCPUs < numAlignedToAlloc {
			numAlignedToAlloc = numCPUs
		}

		allocatedCPUs, err := p.takeByTopology(logger, alignedCPUs, numAlignedToAlloc)
		if err != nil {
			return topology.EmptyAllocation(), err
		}

		result.CPUs = result.CPUs.Union(allocatedCPUs)
	}

	// 对齐 CPU 不足时，从剩余可用 CPU 中补齐。
	remainingCPUs, err := p.takeByTopology(logger, allocatableCPUs.Difference(result.CPUs), numCPUs-result.CPUs.Size())
	if err != nil {
		return topology.EmptyAllocation(), err
	}
	result.CPUs = result.CPUs.Union(remainingCPUs)
	result.Aligned = p.topology.CheckAlignment(result.CPUs)

	// 已分配的 CPU 必须从共享池中移除。
	s.SetDefaultCPUSet(s.GetDefaultCPUSet().Difference(result.CPUs))

	logger.Info("AllocateCPUs", "result", result.String())
	return result, nil
}

// isIntegralCPUAmount 判断 CPU 数量是否为整数（不含毫核小数）。
func isIntegralCPUAmount(cpuQuantity resource.Quantity) bool {
	return cpuQuantity.Value()*1000 == cpuQuantity.MilliValue()
}

// guaranteedCPUs 返回容器可独占的 CPU 数；不满足条件时返回 0。
func (p *staticPolicy) guaranteedCPUs(logger klog.Logger, pod *v1.Pod, container *v1.Container) int {
	qos := v1qos.GetPodQOS(pod)
	if qos != v1.PodQOSGuaranteed {
		logger.V(5).Info("Exclusive CPU allocation skipped, pod QoS is not guaranteed", "qos", qos)
		return 0
	}

	cpuQuantity := container.Resources.Requests[v1.ResourceCPU]

	// Pod 级资源模式下，容器必须 request==limit（CPU 与内存）才视为 Guaranteed，否则不能分配独占 CPU。
	// 这与容器级 QoS 的判定标准一致。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) && !cmqos.IsContainerEquivalentQOSGuaranteed(container) {
		logger.V(5).Info("Exclusive CPU allocation skipped, container is not eligible, request and limit are not equal", "pod", klog.KObj(pod), "containerName", container.Name)
		return 0
	}

	cpuValue := cpuQuantity.Value()
	if !isIntegralCPUAmount(cpuQuantity) {
		logger.V(5).Info("Exclusive CPU allocation skipped, container requested non-integral CPUs", "pod", klog.KObj(pod), "containerName", container.Name, "cpu", cpuValue)
		return 0
	}
	// 对于 CPU 数小于 21 亿的系统，转为 int 是安全的：语言规范保证 int 至少 32 位。
	// 参见 https://golang.org/ref/spec#Numeric_types。
	return int(cpuValue)
}

// podGuaranteedCPUs 返回 Pod 级需要独占的 CPU 总数。
func (p *staticPolicy) podGuaranteedCPUs(logger klog.Logger, pod *v1.Pod) int {
	// 设置了 Pod 级资源时直接使用 Pod 的请求值。
	// 该检查很重要：GetPodTopologyHints 会在 AllocatePod 的特性门控检查之前调用，因此这里必须自行判断。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		qos := v1qos.GetPodQOS(pod)
		if qos != v1.PodQOSGuaranteed {
			return 0
		}

		if cpuQuantity, ok := pod.Spec.Resources.Requests[v1.ResourceCPU]; ok {
			// 只有整数 CPU 请求才参与独占分配。
			if isIntegralCPUAmount(cpuQuantity) {
				return int(cpuQuantity.Value())
			}
		}
		return 0
	}

	// init 阶段所需 CPU 取“标准 init 最大值”与“重启式 init 累加”的较大者。
	requestedByInitContainers := 0
	requestedByRestartableInitContainers := 0
	for _, container := range pod.Spec.InitContainers {
		if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
			continue
		}
		requestedCPU := p.guaranteedCPUs(logger, pod, &container)
		// 计算规则见 KEP-753 sidecar 容器的资源计算说明：重启式 init 容器与普通容器并发运行，因此累加。
		if podutil.IsRestartableInitContainer(&container) {
			requestedByRestartableInitContainers += requestedCPU
		} else if requestedByRestartableInitContainers+requestedCPU > requestedByInitContainers {
			requestedByInitContainers = requestedByRestartableInitContainers + requestedCPU
		}
	}

	// 普通容器的 CPU 请求直接累加。
	requestedByAppContainers := 0
	for _, container := range pod.Spec.Containers {
		if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
			continue
		}
		requestedByAppContainers += p.guaranteedCPUs(logger, pod, &container)
	}

	requestedByLongRunningContainers := requestedByAppContainers + requestedByRestartableInitContainers
	if requestedByInitContainers > requestedByLongRunningContainers {
		return requestedByInitContainers
	}
	return requestedByLongRunningContainers
}

// takeByTopology 按策略选项选择 NUMA 分布/紧凑算法分配 CPU。
func (p *staticPolicy) takeByTopology(logger klog.Logger, availableCPUs cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	cpuSortingStrategy := CPUSortingStrategyPacked
	if p.options.DistributeCPUsAcrossCores {
		cpuSortingStrategy = CPUSortingStrategySpread
	}

	if p.options.DistributeCPUsAcrossNUMA {
		cpuGroupSize := 1
		if p.options.FullPhysicalCPUsOnly {
			cpuGroupSize = p.cpuGroupSize
		}
		return takeByTopologyNUMADistributed(logger, p.topology, availableCPUs, numCPUs, cpuGroupSize, cpuSortingStrategy, p.options.AlignBySocket)
	}

	return takeByTopologyNUMAPacked(logger, p.topology, availableCPUs, numCPUs, cpuSortingStrategy, p.options.PreferAlignByUncoreCacheOption)
}

// GetTopologyHints 为容器生成 CPU 拓扑提示。
func (p *staticPolicy) GetTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "operation", operation)
	switch operation {
	case lifecycle.AddOperation:
		return p.getTopologyHintsForAdd(logger, s, pod, container)
	default:
		logger.V(2).Info("CPU Manager container-level hint generation skipped, operation not supported by the static CPU manager policy")
		return nil
	}
}

// getTopologyHintsForAdd 实现容器级提示生成。
func (p *staticPolicy) getTopologyHintsForAdd(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {

	// 计算需要独占的 CPU 数量。
	requested := p.guaranteedCPUs(logger, pod, container)

	// 数量非整数或容器不属于 Guaranteed 时返回 nil 提示，TopologyManager 会忽略该资源。
	// 等价于 TopologyHints[NUMANodeAffinity: nil, Preferred: true]。
	if requested == 0 {
		return nil
	}

	// Pod 级资源与门控不匹配时返回 nil，让 Pod 无需对齐即可准入，避免阻塞调度。
	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(3).Info("CPU Manager hint generation skipped, pod is using pod-level resources but the PodLevelResourceManagers feature gate is not enabled")
		return nil
	}

	// 已有分配时（如 kubelet 重启后）重新生成相同的提示，保证重启不会改变已运行容器的亲和性。
	if allocated, exists := s.GetCPUSet(string(pod.UID), container.Name); exists {
		if allocated.Size() != requested {
			logger.Info("CPUs already allocated to container with different number than request", "requestedSize", requested, "allocatedSize", allocated.Size())
			// 空提示表示无法满足（Preferred: false），除 best-effort 外 TopologyManager 会拒绝该 Pod。
			return map[string][]topologymanager.TopologyHint{
				string(v1.ResourceCPU): {},
			}
		}
		logger.Info("Regenerating TopologyHints for CPUs already allocated")
		return map[string][]topologymanager.TopologyHint{
			string(v1.ResourceCPU): p.generateCPUTopologyHints(allocated, cpuset.New(), requested),
		}
	}

	// 获取当前可用 CPU。
	available := p.GetAvailableCPUs(s)

	// 获取可复用 CPU（来自已结束的 init 容器）；新建 Pod 该集合为空。
	// 复用集合只包含本 Pod 内可继承的 CPU。
	reusable := p.cpusToReuse[string(pod.UID)]

	// 生成提示并返回。
	cpuHints := p.generateCPUTopologyHints(available, reusable, requested)
	logger.Info("TopologyHints generated", "cpuHints", cpuHints)

	return map[string][]topologymanager.TopologyHint{
		string(v1.ResourceCPU): cpuHints,
	}
}

// GetPodTopologyHints 为 Pod 级分配生成 CPU 拓扑提示。
func (p *staticPolicy) GetPodTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "podUID", pod.UID, "operation", operation)
	switch operation {
	case lifecycle.AddOperation:
		return p.getPodTopologyHintsForAdd(logger, s, pod)
	default:
		logger.V(2).Info("CPU Manager pod hint generation skipped, operation not supported by the static CPU manager policy")
		return nil
	}
}

// getPodTopologyHintsForAdd 实现 Pod 级提示生成。
func (p *staticPolicy) getPodTopologyHintsForAdd(logger klog.Logger, s state.State, pod *v1.Pod) map[string][]topologymanager.TopologyHint {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "podUID", pod.UID)

	// 计算 Pod 需要独占的 CPU 总数。
	requested := p.podGuaranteedCPUs(logger, pod)

	// 数量非整数或 Pod 非 Guaranteed 时无需对齐，TopologyManager 会忽略该资源。
	// 等价于 TopologyHints[NUMANodeAffinity: nil, Preferred: true]。
	if requested == 0 {
		return nil
	}

	// Pod 级资源与门控不匹配时返回 nil，让 Pod 无需对齐即可准入。
	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(3).Info("CPU Manager pod hint generation skipped, pod is using pod-level resources but the PodLevelResourceManagers feature gate is not enabled", "podUID", pod.UID)
		return nil
	}

	// 若 Pod 中存在需要共享池的容器，必须确保共享池非空；否则生成空提示，让 TopologyManager 拒绝该 Pod。
	// 这样可以避免创建空 cpuset，提示生成与分配阶段都会做该检查。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		if err := p.validatePodScopeResources(logger, pod); err != nil {
			logger.V(2).Info("Invalid pod spec. Sum of exclusive container requests equals pod budget, leaving no CPUs for shared containers")
			return map[string][]topologymanager.TopologyHint{
				string(v1.ResourceCPU): {},
			}
		}
	}

	assignedCPUs := cpuset.New()
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		logger_ := klog.LoggerWithValues(logger, "containerName", container.Name)

		requestedByContainer := p.guaranteedCPUs(logger, pod, &container)
		// 已有分配时重新生成相同提示，保持重启前后一致；分配数不一致则返回空提示。
		if allocated, exists := s.GetCPUSet(string(pod.UID), container.Name); exists {
			if allocated.Size() != requestedByContainer {
				logger_.Info("CPUs already allocated to container with different number than request", "allocatedSize", requested, "requestedByContainer", requestedByContainer, "allocatedSize", allocated.Size())
				// 空提示表示无法满足（Preferred: false），除 best-effort 外会拒绝该 Pod。
				return map[string][]topologymanager.TopologyHint{
					string(v1.ResourceCPU): {},
				}
			}
			// 汇总本 Pod 内已分配的 CPU，避免重复分配。
			assignedCPUs = assignedCPUs.Union(allocated)
		}
	}
	if assignedCPUs.Size() == requested {
		logger.Info("Regenerating TopologyHints for CPUs already allocated")
		return map[string][]topologymanager.TopologyHint{
			string(v1.ResourceCPU): p.generateCPUTopologyHints(assignedCPUs, cpuset.New(), requested),
		}
	}

	// 获取当前可用 CPU。
	available := p.GetAvailableCPUs(s)

	// 获取可复用 CPU（来自已结束的 init 容器）；新建 Pod 该集合为空。
	// 复用集合只包含本 Pod 内可继承的 CPU。
	reusable := p.cpusToReuse[string(pod.UID)]

	// 已分配的 CPU 必须计入提示，保证结果包含它们。
	reusable = reusable.Union(assignedCPUs)

	// 生成提示并返回。
	cpuHints := p.generateCPUTopologyHints(available, reusable, requested)
	logger.Info("TopologyHints generated", "cpuHints", cpuHints)

	return map[string][]topologymanager.TopologyHint{
		string(v1.ResourceCPU): cpuHints,
	}
}

// generateCPUTopologyHints 根据可用 CPU 与请求数量生成拓扑提示。
//
// 提示覆盖所有 NUMA 节点组合，其中位数等于最小可满足组合的提示标记为 Preferred，其余为 false。
// 这样 TopologyManager 能优先选择最紧凑的 NUMA 组合。
func (p *staticPolicy) generateCPUTopologyHints(availableCPUs cpuset.CPUSet, reusableCPUs cpuset.CPUSet, request int) []topologymanager.TopologyHint {
	// 初始最小亲和性大小为全部 NUMA 节点数。
	minAffinitySize := p.topology.CPUDetails.NUMANodes().Size()

	// 遍历所有 NUMA 节点位掩码组合。
	hints := []topologymanager.TopologyHint{}
	bitmask.IterateBitMasks(p.topology.CPUDetails.NUMANodes().List(), func(mask bitmask.BitMask) {
		// 先根据当前请求更新最小亲和性大小。
		cpusInMask := p.topology.CPUDetails.CPUsInNUMANodes(mask.GetBits()...).Size()
		if cpusInMask >= request && mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// 再检查该掩码内是否有足够的可用与可复用 CPU；可复用 CPU 必须全部落在掩码内。
		numMatching := 0
		for _, c := range reusableCPUs.List() {
			// 可复用 CPU 不在掩码内时直接跳过该组合。
			if !mask.IsSet(p.topology.CPUDetails[c].NUMANodeID) {
				return
			}
			numMatching++
		}

		// 最后统计可用 CPU 是否足够；两者之和达到请求量才算可行。
		for _, c := range availableCPUs.List() {
			if mask.IsSet(p.topology.CPUDetails[c].NUMANodeID) {
				numMatching++
			}
		}

		// 不足则尝试下一个组合。
		if numMatching < request {
			return
		}

		// 可行则先以 Preferred=false 加入，后续统一标记。
		hints = append(hints, topologymanager.TopologyHint{
			NUMANodeAffinity: mask,
			Preferred:        false,
		})
	})

	// 第二轮按亲和性大小与 socket 对齐情况标记 Preferred：
	// 只有位数等于最小亲和性大小的提示才优先；开启 AlignBySocket 时还要求 socket 对齐。
	for i := range hints {
		if p.options.AlignBySocket && p.isHintSocketAligned(hints[i], minAffinitySize) {
			hints[i].Preferred = true
			continue
		}
		if hints[i].NUMANodeAffinity.Count() == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

// isHintSocketAligned 判断提示中的 NUMA 节点是否按 socket 对齐。
func (p *staticPolicy) isHintSocketAligned(hint topologymanager.TopologyHint, minAffinitySize int) bool {
	numaNodesBitMask := hint.NUMANodeAffinity.GetBits()
	numaNodesPerSocket := p.topology.NumNUMANodes / p.topology.NumSockets
	if numaNodesPerSocket == 0 {
		return false
	}
	// minSockets 是满足分配所需的最少 socket 数。
	// 提示跨越的 socket 数等于该值时才认为对齐。
	minSockets := (minAffinitySize + numaNodesPerSocket - 1) / numaNodesPerSocket
	return p.topology.CPUDetails.SocketsInNUMANodes(numaNodesBitMask...).Size() == minSockets
}

// getAlignedCPUs 根据 NUMA 亲和掩码与策略选项返回对齐的 CPU 集合。
func (p *staticPolicy) getAlignedCPUs(numaAffinity bitmask.BitMask, allocatableCPUs cpuset.CPUSet) cpuset.CPUSet {
	alignedCPUs := cpuset.New()
	numaBits := numaAffinity.GetBits()

	// 开启 align-by-socket 时，把 NUMA 提示扩展为 socket 范围，
	// 优先从整个 socket 中分配，避免过早跨 socket 拆分。
	if p.options.AlignBySocket {
		socketBits := p.topology.CPUDetails.SocketsInNUMANodes(numaBits...).UnsortedList()
		for _, socketID := range socketBits {
			alignedCPUs = alignedCPUs.Union(allocatableCPUs.Intersection(p.topology.CPUDetails.CPUsInSockets(socketID)))
		}
		return alignedCPUs
	}

	for _, numaNodeID := range numaBits {
		alignedCPUs = alignedCPUs.Union(allocatableCPUs.Intersection(p.topology.CPUDetails.CPUsInNUMANodes(numaNodeID)))
	}

	return alignedCPUs
}

// initializeMetrics 初始化 CPU 管理相关指标，确保标签值存在。
func (p *staticPolicy) initializeMetrics(logger klog.Logger, s state.State) {
	metrics.CPUManagerSharedPoolSizeMilliCores.Set(float64(p.GetAvailableCPUs(s).Size() * 1000))
	metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedPhysicalCPU).Add(0)
	metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedPhysicalCPU).Add(0)
	metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedUncoreCache).Add(0)
	totalAssignedCPUs := getTotalAssignedExclusiveCPUs(s)
	metrics.CPUManagerExclusiveCPUsAllocationCount.Set(float64(totalAssignedCPUs.Size()))
	updateAllocationPerNUMAMetric(logger, p.topology, totalAssignedCPUs)
}

// updateMetricsOnAllocate 在分配后更新共享池、独占数与 NUMA 分布指标。
func (p *staticPolicy) updateMetricsOnAllocate(logger klog.Logger, s state.State, cpuAlloc topology.Allocation) {
	ncpus := cpuAlloc.CPUs.Size()
	metrics.CPUManagerExclusiveCPUsAllocationCount.Add(float64(ncpus))
	metrics.CPUManagerSharedPoolSizeMilliCores.Add(float64(-ncpus * 1000))
	if cpuAlloc.Aligned.UncoreCache {
		metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedUncoreCache).Inc()
	}
	totalAssignedCPUs := getTotalAssignedExclusiveCPUs(s)
	updateAllocationPerNUMAMetric(logger, p.topology, totalAssignedCPUs)
}

// updateMetricsOnRelease 在释放后更新共享池、独占数与 NUMA 分布指标。
func (p *staticPolicy) updateMetricsOnRelease(logger klog.Logger, s state.State, cset cpuset.CPUSet) {
	ncpus := cset.Size()
	metrics.CPUManagerExclusiveCPUsAllocationCount.Add(float64(-ncpus))
	metrics.CPUManagerSharedPoolSizeMilliCores.Add(float64(ncpus * 1000))
	totalAssignedCPUs := getTotalAssignedExclusiveCPUs(s)
	updateAllocationPerNUMAMetric(logger, p.topology, totalAssignedCPUs.Difference(cset))
}

// getTotalAssignedExclusiveCPUs 汇总状态中所有容器已独占的 CPU。
func getTotalAssignedExclusiveCPUs(s state.State) cpuset.CPUSet {
	totalAssignedCPUs := cpuset.New()
	for _, assignment := range s.GetCPUAssignments() {
		for _, cset := range assignment {
			totalAssignedCPUs = totalAssignedCPUs.Union(cset)
		}
	}
	return totalAssignedCPUs
}

// updateAllocationPerNUMAMetric 按 NUMA 节点统计已分配 CPU 数并写入指标。
func updateAllocationPerNUMAMetric(logger klog.Logger, topo *topology.CPUTopology, allocatedCPUs cpuset.CPUSet) {
	numaCount := make(map[int]int)

	// 按 NUMA 节点计数。
	for _, cpuID := range allocatedCPUs.UnsortedList() {
		numaNode, err := topo.CPUNUMANodeID(cpuID)
		if err != nil {
			// 注意：这里只记录错误。CPU 集合已预先计算校验过，用户无法篡改，因此该错误几乎不会发生。
			logger.Error(err, "Unable to determine NUMA node", "cpuID", cpuID)
		}
		numaCount[numaNode]++
	}

	// 写入各 NUMA 节点的指标值。
	for numaNode, count := range numaCount {
		metrics.CPUManagerAllocationPerNUMA.WithLabelValues(strconv.Itoa(numaNode)).Set(float64(count))
	}
}
