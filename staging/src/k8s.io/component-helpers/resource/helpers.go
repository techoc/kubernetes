/*
Copyright 2024 The Kubernetes Authors.

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

// Package resource 提供 Pod 级资源（requests/limits）计算的公共辅助函数。
//
// 这里集中了调度与准入环节判定 Pod 资源占用的统一口径：普通容器、init 容器与
// 可重启 sidecar 的叠加规则；原地垂直扩容下 spec/allocated/actuated 多口径取最大值；
// 以及 Pod 级资源和 DRA 节点可分配资源的处理。该计算属于 API 的一部分，修改需按 API 变更评审。
package resource

import (
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ContainerType 表示容器类型，用位掩码区分普通容器与 init 容器，便于按类型过滤。
type ContainerType int

const (
	// Containers 表示普通容器（spec.containers 中的容器）。
	Containers ContainerType = 1 << iota
	// InitContainers 表示初始化容器（spec.initContainers 中的容器）。
	InitContainers
)

// PodResourcesOptions 控制 PodRequests 和 PodLimits 的计算行为。
//
// 这些开关对应多个正在推进中的特性（原地垂直扩容、Pod 级资源、DRA 节点可分配资源）。
// 把它们集中在一个选项结构里，而不是拆成多个函数，是为了让同一套计算逻辑能在不同特性门控组合下
// 复用；调用方必须保证各开关与当前集群实际启用的特性一致，否则计算结果会失准。
type PodResourcesOptions struct {
	// Reuse 若非空，则作为累加结果的容器复用，并由 PodRequests/PodLimits 返回。
	// 复用前原有内容会被清空，因此传入后不能再依赖其中的旧值；这样可在调度热路径减少内存分配。
	Reuse v1.ResourceList
	// UseStatusResources 表示计算 Pod 资源时是否考虑 PodStatus 上报的资源。
	// 若未启用 InPlacePodVerticalScaling 特性，该值必须为 false，否则状态字段缺失时结果会失真。
	UseStatusResources bool
	// InPlacePodLevelResourcesVerticalScalingEnabled 表示计算时是否考虑 PodStatus 上报的 Pod 级资源。
	// 若未启用 InPlacePodLevelResourcesVerticalScaling 特性，该值必须为 false。
	InPlacePodLevelResourcesVerticalScalingEnabled bool
	// ExcludeOverhead 控制是否从计算中排除 Pod 自身开销（spec.overhead）。
	ExcludeOverhead bool
	// NonMissingContainerRequests 为指定资源提供缺省的容器级 requests 值：
	// 仅当容器未显式写出该资源时填入；即使显式写成 0 也不视为缺失，不会被覆盖。
	NonMissingContainerRequests v1.ResourceList
	// SkipPodLevelResources 控制是否跳过 Pod 级资源。若 PodSpec 中未设置 Pod 级资源，
	// 无论该值如何都会跳过。
	SkipPodLevelResources bool
	// SkipContainerLevelResources 控制是否跳过容器级资源，只按 Pod 级资源计算。
	// 与 SkipPodLevelResources 配合可分别考察两种口径。
	SkipContainerLevelResources bool
	// UseDRANodeAllocatableResourceClaimStatus 控制是否使用 PodStatus 中的节点可分配资源 claim 信息
	// 来计算 Pod 的有效资源请求。对应 DRA 节点可分配资源特性，默认关闭。
	UseDRANodeAllocatableResourceClaimStatus bool
}

// supportedPodLevelResources 是受 Pod 级资源管理支持的“固定名称”资源集合（CPU 与内存）。
// hugepages 的资源名带尺寸后缀（如 hugepages-2Mi），无法逐一枚举，故不放入集合，
// 而是由 IsSupportedPodLevelResource 用前缀匹配判断，SupportedPodLevelResources 返回时再补上该前缀。
var supportedPodLevelResources = sets.New(v1.ResourceCPU, v1.ResourceMemory)

// SupportedPodLevelResources 返回当前支持 Pod 级管理的资源集合。
// 每次返回克隆，避免调用方修改这个包级变量，进而影响到其他调用方。
// 除 CPU/内存外还会加入 hugepages 前缀，调用方可用前缀匹配判断具体尺寸的资源。
func SupportedPodLevelResources() sets.Set[v1.ResourceName] {
	return supportedPodLevelResources.Clone().Insert(v1.ResourceHugePagesPrefix)
}

// IsSupportedPodLevelResource 判断给定资源是否受 PodLevelResources 特性的 Pod 级资源管理支持。
// CPU、内存走集合精确匹配；hugepages 类资源名带尺寸后缀，用前缀匹配判断，
// 因此 hugepages-2Mi 这类名称也返回 true。
func IsSupportedPodLevelResource(name v1.ResourceName) bool {
	return supportedPodLevelResources.Has(name) || strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}

// IsPodLevelResourcesSet 判断 Pod 是否设置了 Pod 级资源。
// Requests 或 Limits 中只要有一个包含受支持的资源即返回 true。
// 注意：本函数需与 k8s.io/kubernetes/pkg/apis/core/helper.IsPodLevelResourcesSet 保持一致。
//
// 这里不把“非空”简单等同于 len>0，而是逐个检查资源名是否属于受支持的 Pod 级资源。
// 因为 API 对象里可能存在尚未被本特性支持的资源条目，把这类值算进来会导致本不应生效的
// Pod 级资源逻辑被误触发。先做长度快路径，再逐项过滤，避免为绝大多数为空的对象做无用遍历。
func IsPodLevelResourcesSet(pod *v1.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if (len(pod.Spec.Resources.Requests) + len(pod.Spec.Resources.Limits)) == 0 {
		return false
	}

	for resourceName := range pod.Spec.Resources.Requests {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	for resourceName := range pod.Spec.Resources.Limits {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	return false
}

// IsPodLevelRequestsSet 判断 Pod 是否设置了 Pod 级 requests。
// 仅当 pod.Spec.Resources.Requests 中存在受支持的资源名时返回 true；
// 与 IsPodLevelResourcesSet 一样会过滤掉本特性尚未支持的资源条目。
func IsPodLevelRequestsSet(pod *v1.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if len(pod.Spec.Resources.Requests) == 0 {
		return false
	}

	for resourceName := range pod.Spec.Resources.Requests {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	return false
}

// IsPodLevelLimitsSet 判断 Pod 是否设置了 Pod 级 limits。
// 仅当 pod.Spec.Resources.Limits 非空且至少包含一个受支持的 Pod 级资源名时返回 true。
func IsPodLevelLimitsSet(pod *v1.Pod) bool {
	if pod.Spec.Resources == nil {
		return false
	}

	if len(pod.Spec.Resources.Limits) == 0 {
		return false
	}

	for resourceName := range pod.Spec.Resources.Limits {
		if IsSupportedPodLevelResource(resourceName) {
			return true
		}
	}

	return false
}

// PodRequests 按给定的 PodResourcesOptions 计算 Pod 的总 requests。
// 计算顺序：先按需聚合容器级 requests，再用 Pod 级 requests 覆盖同名资源，最后叠加 pod overhead。
// 若启用了 PodLevelResources 特性且设置了 Pod 级资源，则以 Pod 级值为准；
// 原地垂直扩容场景下还会结合 PodStatus 取 spec/allocated/actuated 的有效值。
// opts.ExcludeOverhead 为 false 时会包含 spec.overhead。
// 该计算属于 API 的一部分，修改需按 API 变更评审。
func PodRequests(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	reqs := v1.ResourceList{}
	if !opts.SkipContainerLevelResources {
		reqs = AggregateContainerRequests(pod, opts)
	}

	if !opts.SkipPodLevelResources && IsPodLevelRequestsSet(pod) {
		effectiveReqs := pod.Spec.Resources.Requests
		if opts.InPlacePodLevelResourcesVerticalScalingEnabled && opts.UseStatusResources && pod.Status.Resources != nil {
			effectiveReqs = effectivePodLevelResources(pod, pod.Spec.Resources.Requests, pod.Status.Resources.Requests, pod.Status.AllocatedResources)
		}

		applyPodLevelResources(reqs, effectiveReqs)
	}

	// 除非显式排除，否则把运行 Pod 自身的开销累加到 requests 总和。
	if !opts.ExcludeOverhead && pod.Spec.Overhead != nil {
		addResourceList(reqs, pod.Spec.Overhead)
	}

	return reqs
}

// applyPodLevelResources 用 Pod 级有效资源覆盖结果列表中的同名资源。
// 注意是“覆盖”而非累加：Pod 级值表达的是整个 Pod 的总量，已包含容器部分，
// 若再相加会重复计数；非受支持的资源名则原样保留容器级结果。
func applyPodLevelResources(result, effectiveResources v1.ResourceList) {
	for resourceName, quantity := range effectiveResources {
		if IsSupportedPodLevelResource(resourceName) {
			result[resourceName] = quantity
		}
	}
}

// effectivePodLevelResources 取 spec 与各状态视图中的最大值作为真正生效的 Pod 级资源。
// 原地扩容不可行时丢弃 spec：此时容器实际运行的是上次生效的分配值，
// 用 spec 计算会夸大 Pod 的资源占用，甚至把根本没生效的扩容算进调度 footprint。
func effectivePodLevelResources(pod *v1.Pod, spec v1.ResourceList, statuses ...v1.ResourceList) v1.ResourceList {
	if IsPodResizeInfeasible(pod) {
		spec = nil
	}
	return max(spec, statuses...)
}

// containerSpecRequests 只关心 spec 中的请求值。函数签名与 aggregateContainerResourcesByFn 的
// 回调类型一致，因此可以忽略状态和扩容可行性参数；用一个统一签名是为了让同一套聚合代码
// 能在 spec/allocated/actuated 三种取值策略之间切换。
func containerSpecRequests(container *v1.Container, _ *v1.ContainerStatus, _ bool) v1.ResourceList {
	return container.Resources.Requests
}

// containerAllocatedRequests 取容器已被实际分配的资源（AllocatedResources）。
// 扩容不可行时连分配值也不可信，返回 nil 让上层 max 只考虑 actuated 值。
func containerAllocatedRequests(container *v1.Container, containerStatus *v1.ContainerStatus, isResizeInfeasible bool) v1.ResourceList {
	if containerStatus != nil && containerStatus.AllocatedResources != nil {
		return containerStatus.AllocatedResources
	}
	if isResizeInfeasible {
		return nil
	}
	return container.Resources.Requests
}

// containerActuatedRequests 取 kubelet 已落到 cgroup 上的真实请求值。
// 依次尝试 actuated -> allocated -> spec：状态字段可能因为特性门控或版本原因缺失，
// 逐级回退保证在任意组合下都能拿到一个可用值；扩容不可行时同样丢弃 spec。
func containerActuatedRequests(container *v1.Container, containerStatus *v1.ContainerStatus, isResizeInfeasible bool) v1.ResourceList {
	if containerStatus != nil && containerStatus.Resources != nil && containerStatus.Resources.Requests != nil {
		return containerStatus.Resources.Requests
	}
	if containerStatus != nil && containerStatus.AllocatedResources != nil {
		return containerStatus.AllocatedResources
	}
	if isResizeInfeasible {
		return nil
	}
	return container.Resources.Requests
}

// findContainerStatus 同时在普通容器和 init 容器的状态里按名字查找。
// 重启型 sidecar 的状态放在 InitContainerStatuses 中，只查普通容器会漏掉它们，
// 导致原地扩容场景下的资源值回退到 spec。
func findContainerStatus(pod *v1.Pod, name string) *v1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == name {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == name {
			return &pod.Status.InitContainerStatuses[i]
		}
	}
	return nil
}

// isRestartableInitContainer 判断 init 容器是否设置了 restartPolicy: Always。
// 这类容器即 sidecar，会在主容器运行期间持续存活，因此其资源需要计入 Pod 的最终占用，
// 而普通 init 容器执行完即退出，只影响启动阶段的峰值。
func isRestartableInitContainer(container *v1.Container) bool {
	return container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways
}

// aggregateContainerResourcesByFn 用回调指定的取值策略聚合所有容器的资源。
// spec、allocated、actuated 三种口径的容器遍历与 sidecar 叠加规则完全相同，
// 把差异抽成回调可以避免复制三份容易漂移的实现。
func aggregateContainerResourcesByFn(pod *v1.Pod, opts PodResourcesOptions, getResourceList func(container *v1.Container, containerStatus *v1.ContainerStatus, isResizeInfeasible bool) v1.ResourceList) v1.ResourceList {
	// 仅在使用状态资源时才需要判断扩容可行性；纯 spec 口径下该值恒为 false。
	var isResizeInfeasible bool
	if opts.UseStatusResources {
		isResizeInfeasible = IsPodResizeInfeasible(pod)
	}
	result := v1.ResourceList{}
	// 普通容器长期运行，资源直接累加。
	for _, container := range pod.Spec.Containers {
		var cs *v1.ContainerStatus
		if opts.UseStatusResources {
			cs = findContainerStatus(pod, container.Name)
		}
		containerResources := getResourceList(&container, cs, isResizeInfeasible)
		if len(opts.NonMissingContainerRequests) > 0 {
			containerResources = applyNonMissing(containerResources, opts.NonMissingContainerRequests)
		}
		addResourceList(result, containerResources)
	}

	restartableInitContainerResources := v1.ResourceList{}
	initContainerResources := v1.ResourceList{}
	// init 容器决定各资源需求的“最小值”（即启动阶段的峰值）。
	// 设第 i 个 init 容器执行时的资源需求为 InitContainerUse(i)，则：
	// InitContainerUse(i) = 下标小于 i 的可重启 init 容器资源之和 + 第 i 个 init 容器的资源。
	// 详见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/753-sidecar-containers#exposing-pod-resource-requirements 。
	//
	// 重启型 init 容器（sidecar）在整个 Pod 生命周期内持续运行，所以其资源
	// 既计入每个后续 init 阶段的峰值，也直接计入 Pod 的最终占用；普通 init 容器只在
	// 自己运行的那一阶段起作用，因此用 max 取各阶段峰值而不是累加。
	for _, container := range pod.Spec.InitContainers {
		var cs *v1.ContainerStatus
		if opts.UseStatusResources {
			cs = findContainerStatus(pod, container.Name)
		}
		containerResources := getResourceList(&container, cs, isResizeInfeasible)
		if len(opts.NonMissingContainerRequests) > 0 {
			containerResources = applyNonMissing(containerResources, opts.NonMissingContainerRequests)
		}
		// 判断是否为可重启 init 容器（sidecar）。
		if isRestartableInitContainer(&container) {
			// sidecar 在整个生命周期内都在运行，因此直接计入 Pod 的最终资源占用。
			addResourceList(result, containerResources)

			// 同时累计已遇到的 sidecar 资源，供后续 init 阶段的峰值计算使用。
			addResourceList(restartableInitContainerResources, containerResources)
			containerResources = restartableInitContainerResources
		} else {
			// 普通 init 容器只在自身阶段运行，但其启动期间已启动的 sidecar 也在消耗资源，
			// 因此该阶段峰值是“自身资源 + 此前所有 sidecar 资源”。
			combinedResources := v1.ResourceList{}
			addResourceList(combinedResources, containerResources)
			addResourceList(combinedResources, restartableInitContainerResources)
			containerResources = combinedResources
		}
		// 逐阶段取最大值，得到启动过程中任一时刻的资源峰值。
		maxResourceList(initContainerResources, containerResources)
	}
	// 最终占用 = 普通容器之和 + sidecar 之和（已在 result 中）与启动峰值二者取最大值。
	maxResourceList(result, initContainerResources)
	return result
}

// AggregateContainerRequests 计算 Pod 中所有容器的 requests 总和。
// 算法遵循 sidecar 容器 KEP 中定义的公式：普通容器累加，可重启 init 容器（sidecar）既计入
// 最终占用也计入各启动阶段峰值，普通 init 容器只取各阶段最大值。
// 详见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/753-sidecar-containers#resources-calculation-for-scheduling-and-pod-admission 。
//
// 当 opts.UseStatusResources 为 true 时，会同时考虑 spec、已分配（allocated）与实际生效（actuated）
// 三种口径并取最大值，以避免扩容过程中低估 Pod 的资源占用；扩容不可行时忽略 spec。
func AggregateContainerRequests(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	// 允许复用调用方传入的 map 是为了在调度热路径里减少分配；
	// 代价是复用前会清空原有内容，因此调用方交出 Reuse 后不能再依赖其旧值。
	reqs := reuseOrClearResourceList(opts.Reuse)
	if !opts.UseStatusResources {
		addResourceList(reqs, aggregateContainerResourcesByFn(pod, opts, containerSpecRequests))
		addDRANodeAllocatableClaimResources(reqs, pod, opts)
	} else {
		isResizeInfeasible := IsPodResizeInfeasible(pod)
		specReqs := aggregateContainerResourcesByFn(pod, opts, containerSpecRequests)
		addDRANodeAllocatableClaimResources(specReqs, pod, opts)
		// 需要同时掌握 allocated（已分配）和 actuated（实际生效）两个口径：
		// 前者反映 kubelet 承诺的资源，后者反映 cgroup 真实使用量。扩容不可行时
		// 只比较后两者，避免把 spec 中无法落地的目标值算进 Pod footprint。
		var allocatedReqs, actuatedReqs v1.ResourceList
		// Pod 级状态字段一旦被填充，就已经是所有容器的聚合值，可直接取用；
		// 未填充时（例如 Pod 刚创建或特性门控未开启）才回退到逐容器状态聚合。
		// 待 InPlacePodLevelResourcesVerticalScaling 与 InPlacePodVerticalScaling GA、门控移除后，
		// 容器级回退将变得多余：创建时 max(spec, actuated, allocated) 自然等于 spec。
		if opts.InPlacePodLevelResourcesVerticalScalingEnabled && pod.Status.AllocatedResources != nil && pod.Status.Resources != nil && pod.Status.Resources.Requests != nil {
			allocatedReqs = pod.Status.AllocatedResources
			actuatedReqs = pod.Status.Resources.Requests
			// 此处不再向 allocatedReqs/actuatedReqs 额外加 DRA 值，因为：
			// 1. kubelet 在设置 pod.Status.AllocatedResources 时已包含 DRA 分配；
			// 2. kubelet 配置 Pod 级 cgroup 时也已包含 DRA，而 pod.Status.Resources.Requests
			//    正是从 cgroup 设置读出的，天然带有 DRA 值，重复相加会造成高估。
		} else {
			// 回退路径中需要手动把 DRA 分配加到 allocatedReqs，因为 kubelet 不会把 DRA
			// 写进 pod.Status.ContainerStatuses[].AllocatedResources，不加会低报资源。
			// 这是临时方案，待所有节点上 InPlacePodLevelResourcesVerticalScaling 达到 Beta/GA、
			// 原生包含 DRA 的 Pod 级状态字段始终可用后即可移除。
			allocatedReqs = aggregateContainerResourcesByFn(pod, opts, containerAllocatedRequests)
			addDRANodeAllocatableClaimResources(allocatedReqs, pod, opts)
			actuatedReqs = aggregateContainerResourcesByFn(pod, opts, containerActuatedRequests)
		}

		// 取各口径的最大值而不是某个单一来源：扩容过程中 spec、状态、cgroup 之间
		// 天然存在时序差，取最大值能保证任何时刻的 Pod footprint 都不会被低估。
		if isResizeInfeasible {
			addResourceList(reqs, max(actuatedReqs, allocatedReqs))
		} else {
			addResourceList(reqs, max(specReqs, actuatedReqs, allocatedReqs))
		}
	}

	return reqs
}

// IsPodResizeInfeasible 判断 Pod 的 PodResizePending 条件是否处于 infeasible（不可行）状态。
// 不可行表示 kubelet 无法落地 spec 中的资源变更，此时计算资源占用必须以状态值为准。
func IsPodResizeInfeasible(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodResizePending {
			return condition.Reason == v1.PodReasonInfeasible
		}
	}
	return false
}

// IsPodResizeDeferred 判断 Pod 的 PodResizePending 条件是否处于 deferred（延迟）状态。
// 延迟表示扩容暂时无法执行但仍有希望，与 infeasible 的“彻底不可行”语义不同。
func IsPodResizeDeferred(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodResizePending {
			return condition.Reason == v1.PodReasonDeferred
		}
	}
	return false
}

// applyNonMissing 返回给定资源列表的拷贝，并用 nonMissing 中的值补齐其中缺失的资源。
//
// 只补“没写”的资源，不覆盖显式写出的零值：显式 0 是用户表达“不请求该资源”的方式，
// 与缺省语义完全不同。这里返回拷贝而不是原地修改，是因为传入的 ResourceList 可能来自
// PodSpec，直接改写会污染调用方的对象。
func applyNonMissing(reqs v1.ResourceList, nonMissing v1.ResourceList) v1.ResourceList {
	cp := v1.ResourceList{}
	for k, v := range reqs {
		cp[k] = v.DeepCopy()
	}

	for k, v := range nonMissing {
		if _, found := reqs[k]; !found {
			rk := cp[k]
			rk.Add(v)
			cp[k] = rk
		}
	}
	return cp
}

// containerSpecLimits 只取容器 spec 中声明的 limits，忽略状态与扩容可行性参数。
func containerSpecLimits(c *v1.Container, _ *v1.ContainerStatus, _ bool) v1.ResourceList {
	return c.Resources.Limits
}

// containerActuatedLimits 取 kubelet 实际生效的容器 limits。
// 优先使用 containerStatus.Resources.Limits；状态缺失时回退到 spec，
// 但若扩容不可行则丢弃 spec，避免把无法落地的目标值当作实际限制。
func containerActuatedLimits(c *v1.Container, cs *v1.ContainerStatus, isResizeInfeasible bool) v1.ResourceList {
	if cs != nil && cs.Resources != nil && cs.Resources.Limits != nil {
		return cs.Resources.Limits
	}
	if isResizeInfeasible {
		return nil
	}
	return c.Resources.Limits
}

// PodLimits 按给定的 PodResourcesOptions 计算 Pod 的总 limits。
// 先聚合容器级 limits，再用 Pod 级 limits 覆盖同名资源；opts.ExcludeOverhead 为 false 时，
// 只对已显式设置且非零的 limits 叠加 pod overhead（未设置 limit 表示不受限，不应制造人为上界）。
// 该计算属于 API 的一部分，修改需按 API 变更评审。
func PodLimits(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	// 实际复用逻辑在 AggregateContainerLimits 中完成：传入的 Reuse map 会被清空后重新累加。
	limits := AggregateContainerLimits(pod, opts)
	if !opts.SkipPodLevelResources && IsPodLevelResourcesSet(pod) {
		effectiveLims := pod.Spec.Resources.Limits
		if opts.InPlacePodLevelResourcesVerticalScalingEnabled && opts.UseStatusResources && pod.Status.Resources != nil {
			effectiveLims = effectivePodLevelResources(pod, pod.Spec.Resources.Limits, pod.Status.Resources.Limits)
		}
		applyPodLevelResources(limits, effectiveLims)
	}

	// 除非显式排除，否则对非零 limits 叠加 Pod 自身开销。
	// 和 requests 不同，limits 里只有“显式设置了非零限制”的资源才叠加 overhead。
	// 未设置 limit 的资源表示不受限，给它加上 overhead 会凭空制造出一个人为的上界。
	if !opts.ExcludeOverhead && pod.Spec.Overhead != nil {
		for name, quantity := range pod.Spec.Overhead {
			if value, ok := limits[name]; ok && !value.IsZero() {
				value.Add(quantity)
				limits[name] = value
			}
		}
	}

	return limits
}

// AggregateContainerLimits 计算 Pod 中所有容器的 limits 总和。
// 与 AggregateContainerRequests 使用同一套 sidecar 叠加公式（见同一 KEP 链接），
// 但 limits 没有 NonMissingContainerRequests 补齐策略：未声明 limit 即表示不受限。
// 使用状态资源时会比较 spec 与 actuated（实际生效）并取最大值；扩容不可行时只取 actuated。
func AggregateContainerLimits(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	// NonMissingContainerRequests 是 requests 独有的补齐策略，套用到 limits 上会
	// 凭空生成原本不存在的限制。这里先清空，避免调用方复用同一个 opts 时误用。
	opts.NonMissingContainerRequests = nil
	// 复用调用方传入的 map 以减少分配；复用前会清空其原有内容。
	limits := reuseOrClearResourceList(opts.Reuse)
	if !opts.UseStatusResources {
		addResourceList(limits, aggregateContainerResourcesByFn(pod, opts, containerSpecLimits))
		limits = addDRANodeAllocatableLimits(limits, pod, opts)
	} else {
		isResizeInfeasible := IsPodResizeInfeasible(pod)
		specLimits := aggregateContainerResourcesByFn(pod, opts, containerSpecLimits)
		specLimits = addDRANodeAllocatableLimits(specLimits, pod, opts)
		var actuatedLimits v1.ResourceList
		// Pod 级状态字段已填充时，其中的 limits 已是所有容器的聚合值，可直接取用；
		// 未填充时（刚创建或门控未开启）才回退到逐容器状态聚合。
		// 待相关特性 GA、门控移除后，容器级回退将变得多余：创建时 max(spec, actuated)
		// 自然等于 spec。
		if opts.InPlacePodLevelResourcesVerticalScalingEnabled && pod.Status.Resources != nil && pod.Status.Resources.Limits != nil {
			actuatedLimits = pod.Status.Resources.Limits
			// kubelet 在填充 Pod 级 limits 时已包含 DRA 值；该字段来自 cgroup 设置，
			// 因此这里不能再重复累加，否则会高估。
		} else {
			actuatedLimits = aggregateContainerResourcesByFn(pod, opts, containerActuatedLimits)
			// kubelet 更新容器 limits 时已考虑 DRA 值，这里同样不再重复累加。
		}

		if isResizeInfeasible {
			addResourceList(limits, actuatedLimits)
		} else {
			addResourceList(limits, max(specLimits, actuatedLimits))
		}
	}
	return limits
}

// addResourceList 把 newList 中的资源按名称累加到 list 中。
// 首次出现的资源会深拷贝后写入，避免与输入列表共享 Quantity 的内部存储；
// 后续同名资源则直接相加。list 会被原地修改。
func addResourceList(list, newList v1.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

// maxResourceList 逐资源名取 list 与 newList 中的较大值，并原地写回 list。
// newList 中不存在的资源名不做处理；Quantity 通过 Cmp 比较大小。
func maxResourceList(list, newList v1.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok || quantity.Cmp(value) > 0 {
			list[name] = quantity.DeepCopy()
		}
	}
}

// max 对每个资源名返回 a、b... 中的最大值，仅在无法就地累加到已有列表时使用。
// 先深拷贝输入 map 再求最大值，因为结果是新建的列表，不能借用任何输入 map 的存储。
// 这也是它与 maxResourceList 的分工：后者原地更新，用于已有累加结果的场景。
func max(a v1.ResourceList, b ...v1.ResourceList) v1.ResourceList {
	var result v1.ResourceList
	if a != nil {
		result = a.DeepCopy()
	} else {
		result = v1.ResourceList{}
	}
	for _, other := range b {
		maxResourceList(result, other)
	}
	return result
}

// reuseOrClearResourceList 在资源计算的内层中复用 map，避免频繁分配。
// 传入 nil 时新建一个容量为 4 的 map；非 nil 时清空所有键后原样返回。
// 调用方必须清楚“旧值全部丢失”这一语义。
func reuseOrClearResourceList(reuse v1.ResourceList) v1.ResourceList {
	if reuse == nil {
		return make(v1.ResourceList, 4)
	}
	for k := range reuse {
		delete(reuse, k)
	}
	return reuse
}

// GetContainerDRAAllocations 汇总分配给指定容器的所有 DRA 资源分配。
// 只统计 claimStatus.Containers 中包含 containerName 的 claim，避免共享资源被重复计入。
// 开销字段区分 PerPod 与 PerContainer：前者每个 claim 只计一次，后者只计入当前容器的份额；
// 忽略任何一种都会低估节点可分配资源的占用。
func GetContainerDRAAllocations(pod *v1.Pod, containerName string) v1.ResourceList {
	draAllocations := make(v1.ResourceList)
	for _, claimStatus := range pod.Status.NodeAllocatableResourceClaimStatuses {
		if !slices.Contains(claimStatus.Containers, containerName) {
			continue
		}
		// 累加 Mapping（资源映射）部分。
		// 只处理名字出现在 Containers 列表里的 claim，确保同一份共享资源
		// 不会被重复计入不同容器；这里的 Containers 是 kubelet 实际使用该 claim 的容器集合。
		for _, mapping := range claimStatus.Mapping {
			if mapping.Quantity != nil {
				q := draAllocations[mapping.Name]
				q.Add(*mapping.Quantity)
				draAllocations[mapping.Name] = q
			}
		}

		// 累加开销资源：PerPod 部分每个 claim 计一次，PerContainer 部分计当前容器的份额。
		for _, overhead := range claimStatus.Overhead {
			var quantity resource.Quantity
			if overhead.PerPod != nil {
				quantity.Add(*overhead.PerPod)
			}
			if overhead.PerContainer != nil {
				quantity.Add(*overhead.PerContainer)
			}
			q := draAllocations[overhead.Name]
			q.Add(quantity)
			draAllocations[overhead.Name] = q
		}
	}
	return draAllocations
}

// addDRANodeAllocatableLimits 把 DRA 节点可分配资源累加到 spec limits 中。
// 只有 spec 中显式声明了 limit 的资源才累加，否则 kubelet 不会为该资源设置 limit（视为不受限），
// 加上 DRA 值会凭空制造上界。hugepages 是例外：它严格不可超卖，因此无论 spec 是否声明都要累加。
func addDRANodeAllocatableLimits(specLimits v1.ResourceList, pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	draResources := v1.ResourceList{}
	addDRANodeAllocatableClaimResources(draResources, pod, opts)
	for name, quantity := range draResources {
		val, declared := specLimits[name]
		// 只有 spec 显式声明 limit 时才累加 DRA 值：未声明时 kubelet 不设置 limit（不受限），
		// 人为加上界会改变语义。hugepages 严格不可超卖，是例外，始终累加。
		if declared || strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix) {
			q := val
			q.Add(quantity)
			specLimits[name] = q
		}
	}
	return specLimits
}

// addDRANodeAllocatableClaimResources 把 DRA claim 的资源累加到 Pod 的资源列表中。
// 这个开关默认关闭，因为相关状态字段是 alpha 特性；条件里同时检查开关和非空状态，
// 避免为绝大多数没有使用 DRA 的 Pod 做无谓的循环。
func addDRANodeAllocatableClaimResources(resources v1.ResourceList, pod *v1.Pod, opts PodResourcesOptions) {
	if opts.UseDRANodeAllocatableResourceClaimStatus && len(pod.Status.NodeAllocatableResourceClaimStatuses) > 0 {
		for _, claimStatus := range pod.Status.NodeAllocatableResourceClaimStatuses {
			// TODO(pravk03): 处理 init 容器引用的 claim，并据此计算启动阶段峰值。
			// 目前任何 DRA 分配都无条件计入 Pod footprint，不区分 init 容器阶段。
			for _, mapping := range claimStatus.Mapping {
				if mapping.Quantity != nil {
					q := resources[mapping.Name]
					q.Add(*mapping.Quantity)
					resources[mapping.Name] = q
				}
			}
			for _, overhead := range claimStatus.Overhead {
				var quantity resource.Quantity
				if overhead.PerPod != nil {
					quantity.Add(*overhead.PerPod)
				}
				if overhead.PerContainer != nil && len(claimStatus.Containers) > 0 {
					varOverhead := overhead.PerContainer.DeepCopy()
					varOverhead.Mul(int64(len(claimStatus.Containers)))
					quantity.Add(varOverhead)
				}
				q := resources[overhead.Name]
				q.Add(quantity)
				resources[overhead.Name] = q
			}
		}
	}
}
