/*
Copyright 2019 The Kubernetes Authors.

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

package topologymanager

import (
	"context"
	"fmt"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

const (
	// defaultMaxAllowableNUMANodes 指定 TopologyManager 在底层机器上支持的最大 NUMA 节点数。
	//
	// 目前，NUMA 节点数超过该值时，枚举所有可能的 NUMAAffinity 掩码并生成提示会导致
	// 状态爆炸。因此，如果机器上的 NUMA 节点数超过该值且 TopologyManager 已启用，
	// 将返回错误并拒绝加载 TopologyManager。
	defaultMaxAllowableNUMANodes = 8
	// ErrorTopologyAffinity 是 TopologyAffinityError 错误的类型标识。
	ErrorTopologyAffinity = "TopologyAffinityError"
)

// TopologyAffinityError 表示资源对齐错误。
type TopologyAffinityError struct{}

// NewTopologyAffinityError 返回一个新的 TopologyAffinityError。
func NewTopologyAffinityError() *TopologyAffinityError {
	return &TopologyAffinityError{}
}

// Error 实现 error 接口，返回拓扑局部性分配失败信息。
func (e TopologyAffinityError) Error() string {
	return "Resources cannot be allocated with Topology locality"
}

// Type 返回错误类型标识，便于上层按类别识别拓扑亲和性错误。
func (e TopologyAffinityError) Type() string {
	return ErrorTopologyAffinity
}

// PodLevelTopologyAffinityError 表示 Pod 级别的资源对齐错误。
type PodLevelTopologyAffinityError struct {
	message string
}

// NewPodLevelTopologyAffinityError 创建带自定义消息的 Pod 级拓扑亲和性错误。
func NewPodLevelTopologyAffinityError(message string) *PodLevelTopologyAffinityError {
	return &PodLevelTopologyAffinityError{message: message}
}

// Error 返回错误信息：若构造时提供了自定义消息则附加在固定前缀之后，
// 否则使用默认的 Pod 范围错误文案。
func (e *PodLevelTopologyAffinityError) Error() string {
	if e.message != "" {
		return "Pod Scope " + e.message
	}
	return "Pod Scope Resources cannot be allocated with Topology locality"
}

// Type 返回错误类型标识，与容器级错误共用 ErrorTopologyAffinity。
func (e *PodLevelTopologyAffinityError) Type() string {
	return ErrorTopologyAffinity
}

// Manager 接口为 Kubelet 提供管理 Pod 拓扑提示的方法。
type Manager interface {
	// PodAdmitHandler 由 Manager 实现，使其能够参与 Pod 准入流程。
	lifecycle.PodAdmitHandler
	// AddHintProvider 向 Manager 注册提示提供者，表示生成拓扑提示时需要征求其意见。
	AddHintProvider(logger klog.Logger, h HintProvider)
	// AddContainer 将 Pod 加入 Manager 进行跟踪。
	AddContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string)
	// RemoveContainer 将容器从 Manager 的跟踪中移除。
	RemoveContainer(logger klog.Logger, containerID string) error
	// Store 是存取 Pod 拓扑提示的接口。
	Store
}

// manager 是 Manager 接口的实现，将提示收集、存储与准入判断委托给所选作用域。
type manager struct {
	// scope 表示拓扑管理的作用域（容器级或 Pod 级）。
	scope Scope
}

// HintProvider 是希望协作实现 NUMA 局部性意义上全局最优具体资源对齐的组件所实现的接口。
type HintProvider interface {
	// GetTopologyHints 返回资源名到可能的具体分配列表的映射，以 NUMA 局部性提示表示。
	// 每个提示可标记为“首选”，并指明假设分配涉及的 NUMA 节点集合。拓扑管理器会为
	// 每个提示提供者调用该函数并合并结果，产生共识的“最佳”提示；随后提示提供者
	// 可以查询拓扑管理器来影响实际的资源分配。
	GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]TopologyHint
	// GetPodTopologyHints 返回 Pod 级资源名到可能的 NUMA 局部性提示列表的映射。
	GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]TopologyHint
	// AllocatePod 用于触发向 Pod 分配资源。
	AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error
	// Allocate 在收集完所有提示、聚合后的提示可通过 Store.GetAffinity() 获取之后，
	// 触发提示提供者执行资源分配。
	Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error
}

// Store 接口允许提示提供者查询 Pod 的亲和性。
type Store interface {
	// GetAffinity 返回指定 Pod 和容器的拓扑亲和性提示。
	GetAffinity(logger klog.Logger, podUID string, containerName string) TopologyHint
	// GetPolicy 返回当前使用的拓扑管理策略。
	GetPolicy() Policy
	// Name 返回作用域的名称。
	Name() string
}

// AuthoritativeStore 是 Store 的可选扩展，用于报告容器的亲和性提示是否具有权威性。
// 当 IsHintAuthoritative 返回 true 时，调用方必须原样使用该提示，不得将其扩展到
// 更多 NUMA 节点。
//
// 该机制用于 Windows：Windows 没有 cpuset.mems，内存放置必须跟随 CPU 管理器的
// NUMA 决策。亲和性 Store 被包装成持有“跟随 CPU”知识的组件，因此 CPU 与内存管理器
// 之间的耦合经由 Store 传递，而不是让两个管理器直接相互依赖。
type AuthoritativeStore interface {
	Store
	// IsHintAuthoritative 报告指定容器的亲和性提示是否具有权威性。
	IsHintAuthoritative(podUID, containerName string) bool
}

// TopologyHint 是包含容器 NUMANodeAffinity（NUMA 节点亲和性）的结构体。
type TopologyHint struct {
	NUMANodeAffinity bitmask.BitMask
	// Preferred 为 true 表示 NUMANodeAffinity 编码的是该容器的首选分配；否则为 false。
	Preferred bool
}

// IsEqual 判断两个 TopologyHint 是否相等。
func (th *TopologyHint) IsEqual(topologyHint TopologyHint) bool {
	if th.Preferred == topologyHint.Preferred {
		// 掩码为 nil 时无法调用其方法，直接比较两者的 nil 状态是否一致。
		if th.NUMANodeAffinity == nil || topologyHint.NUMANodeAffinity == nil {
			return th.NUMANodeAffinity == topologyHint.NUMANodeAffinity
		}
		return th.NUMANodeAffinity.IsEqual(topologyHint.NUMANodeAffinity)
	}
	return false
}

// LessThan 判断 TopologyHint a 是否优于（小于）TopologyHint b：
// 即 a 是首选提示而 b 不是，或者 a 的 NUMANodeAffinity 比 b 的更窄。
func (th *TopologyHint) LessThan(other TopologyHint) bool {
	if th.Preferred != other.Preferred {
		return th.Preferred
	}
	return th.NUMANodeAffinity.IsNarrowerThan(other.NUMANodeAffinity)
}

// 编译期断言：manager 必须实现 Manager 接口。
var _ Manager = &manager{}

// NewManager 根据给定的策略和作用域创建新的 TopologyManager。
func NewManager(logger klog.Logger, topology []cadvisorapi.Node, topologyPolicyName string, topologyScopeName string, topologyPolicyOptions map[string]string) (Manager, error) {
	// 策略为 none 时作用域没有意义，直接短路返回，避免不必要的 NUMA 发现与校验。
	if topologyPolicyName == PolicyNone {
		logger.Info("Creating topology manager with none policy")
		return &manager{scope: NewNoneScope()}, nil
	}

	// 解析策略选项，选项非法时直接失败，避免带着错误配置继续初始化。
	opts, err := NewPolicyOptions(logger, topologyPolicyOptions)
	if err != nil {
		return nil, err
	}

	logger.Info("Creating topology manager with policy per scope", "topologyPolicyName", topologyPolicyName, "topologyScopeName", topologyScopeName, "topologyPolicyOptions", opts)

	// 发现 NUMA 拓扑信息，失败时无法生成正确的拓扑提示。
	numaInfo, err := NewNUMAInfo(topology, opts)
	if err != nil {
		return nil, fmt.Errorf("cannot discover NUMA topology: %w", err)
	}

	// 校验 NUMA 节点数上限，防止提示枚举出现状态爆炸。
	if topologyPolicyName != PolicyNone && len(numaInfo.Nodes) > opts.MaxAllowableNUMANodes {
		return nil, fmt.Errorf("unsupported on machines with more than %v NUMA Nodes", opts.MaxAllowableNUMANodes)
	}

	// 按策略名实例化具体策略实现。
	var policy Policy
	switch topologyPolicyName {

	case PolicyBestEffort:
		policy = NewBestEffortPolicy(numaInfo, opts)

	case PolicyRestricted:
		policy = NewRestrictedPolicy(numaInfo, opts)

	case PolicySingleNumaNode:
		policy = NewSingleNumaNodePolicy(numaInfo, opts)

	default:
		return nil, fmt.Errorf("unknown policy: \"%s\"", topologyPolicyName)
	}

	// 按作用域名实例化容器级或 Pod 级作用域。
	var scope Scope
	switch topologyScopeName {

	case ContainerTopologyScope:
		scope = NewContainerScope(policy)

	case PodTopologyScope:
		scope = NewPodScope(policy)

	default:
		return nil, fmt.Errorf("unknown scope: \"%s\"", topologyScopeName)
	}

	manager := &manager{
		scope: scope,
	}

	// 预初始化指标，确保各标签组合在首次观测前就以 0 值存在。
	manager.initializeMetrics()

	return manager, nil
}

// initializeMetrics 预创建所有指标标签组合，确保它们在首次观测前就以 0 值暴露。
func (m *manager) initializeMetrics() {
	// 通过 Add(0) 触发指标向量创建，避免首次真实观测前指标缺失。
	metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Add(0)
	metrics.ContainerAlignedComputeResources.WithLabelValues(metrics.AlignScopePod, metrics.AlignedNUMANode).Add(0)
	metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopeContainer, metrics.AlignedNUMANode).Add(0)
	metrics.ContainerAlignedComputeResourcesFailure.WithLabelValues(metrics.AlignScopePod, metrics.AlignedNUMANode).Add(0)
}

// GetAffinity 将亲和性查询转发给底层作用域。
func (m *manager) GetAffinity(logger klog.Logger, podUID string, containerName string) TopologyHint {
	return m.scope.GetAffinity(logger, podUID, containerName)
}

// GetPolicy 返回底层作用域使用的策略。
func (m *manager) GetPolicy() Policy {
	return m.scope.GetPolicy()
}

// Name 返回底层作用域的名称。
func (m *manager) Name() string {
	return m.scope.Name()
}

// AddHintProvider 将提示提供者注册到底层作用域。
func (m *manager) AddHintProvider(logger klog.Logger, h HintProvider) {
	m.scope.AddHintProvider(logger, h)
}

// AddContainer 将容器登记到底层作用域以便跟踪其拓扑提示。
func (m *manager) AddContainer(_ klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) {
	m.scope.AddContainer(pod, container, containerID)
}

// RemoveContainer 从底层作用域中移除容器跟踪。
func (m *manager) RemoveContainer(logger klog.Logger, containerID string) error {
	return m.scope.RemoveContainer(logger, containerID)
}

// Admit 执行拓扑管理器准入检查：由作用域判断 Pod 的资源请求能否满足拓扑局部性，
// 并记录准入请求计数与耗时指标。
func (m *manager) Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	logger := klog.FromContext(ctx)
	// 输出准入检查日志，仅在 V(4) 级别开启时产生。
	logger.V(4).Info("Topology manager admission check", "pod", klog.KObj(attrs.Pod))
	// 统计准入检查次数，便于观察拓扑管理器给准入路径带来的开销。
	metrics.TopologyManagerAdmissionRequestsTotal.Inc()

	// 记录起始时间，用于观测一次准入检查的耗时。
	startTime := time.Now()
	podAdmitResult := m.scope.Admit(ctx, attrs.Pod, attrs.Operation)
	metrics.TopologyManagerAdmissionDuration.Observe(float64(time.Since(startTime).Milliseconds()))

	// 以详细日志输出准入结果，便于排查资源对齐失败。
	logger.V(4).Info("Pod Admit Result", "Message", podAdmitResult.Message, "pod", klog.KObj(attrs.Pod))
	return podAdmitResult
}
