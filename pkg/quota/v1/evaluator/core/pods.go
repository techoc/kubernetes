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

package core

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/utils/clock"

	resourcehelper "k8s.io/component-helpers/resource"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// podObjectCountName 是 Pod 对象计数配额的资源名（count/pods）。
// 对象计数配额跟踪存储中的所有对象，与 Pod 生命周期无关。
var podObjectCountName = generic.ObjectCountQuotaResourceNameFor(corev1.SchemeGroupVersion.WithResource("pods").GroupResource())

// resourceRequestsDeviceClassPrefix 是隐式扩展资源的前缀，用于 DRA 设备类配额
var resourceRequestsDeviceClassPrefix = corev1.DefaultResourceRequestsPrefix + resourceapi.ResourceDeviceClassPrefix

// podResources 是 Pod 相关的配额资源列表。
// 包含对象计数、CPU/内存/存储的请求和限制、以及 Pod 数量。
var podResources = []corev1.ResourceName{
	podObjectCountName,
	corev1.ResourceCPU,
	corev1.ResourceMemory,
	corev1.ResourceEphemeralStorage,
	corev1.ResourceRequestsCPU,
	corev1.ResourceRequestsMemory,
	corev1.ResourceRequestsEphemeralStorage,
	corev1.ResourceLimitsCPU,
	corev1.ResourceLimitsMemory,
	corev1.ResourceLimitsEphemeralStorage,
	corev1.ResourcePods,
}

// podResourcePrefixes 是 Pod 相关配额资源的前缀列表。
// 用于匹配 HugePages 等具有特定前缀的扩展资源。
var podResourcePrefixes = []string{
	corev1.ResourceHugePagesPrefix,
	corev1.ResourceRequestsHugePagesPrefix,
}

// requestedResourcePrefixes 是 Pod 的 Resources.Requests/Limits 中可能声明的资源前缀。
// 用于识别需要在配额中特殊处理的资源类型（如 HugePages）。
var requestedResourcePrefixes = []string{
	corev1.ResourceHugePagesPrefix,
}

// maskResourceWithPrefix 为资源名添加前缀。
// 例如：hugepages-XXX -> requests.hugepages-XXX
func maskResourceWithPrefix(resource corev1.ResourceName, prefix string) corev1.ResourceName {
	return corev1.ResourceName(fmt.Sprintf("%s%s", prefix, string(resource)))
}

// isExtendedResourceNameForQuota 判断扩展资源名是否具有配额相关的前缀。
// 扩展资源不支持超卖，因此只允许 "requests.resourceName" 格式的配额对象。
// 同时也支持隐式扩展资源格式 "requests.deviceclass.resource.kubernetes.io/device-class-name"。
func isExtendedResourceNameForQuota(name corev1.ResourceName) bool {
	// As overcommit is not supported by extended resources for now,
	// quota objects in format of "requests.resourceName" is allowed
	nonNative := !helper.IsNativeResource(name)
	// allow the implicit extended resource name in the format of
	// requests.deviceclass.resource.kubernetes.io/device-class-name
	implicitExtendedResource := strings.HasPrefix(string(name), resourceRequestsDeviceClassPrefix)
	// name starts with 'requests.'
	isQuotaRequest := strings.HasPrefix(string(name), corev1.DefaultResourceRequestsPrefix)

	return (nonNative || implicitExtendedResource) && isQuotaRequest
}

// validationSet 是配额验证时检查的资源集合。
// 历史遗留问题：如果配额跟踪 CPU 或内存，则 Pod 必须显式设置这些值。
// 这是一个设计缺陷，将资源验证与配额执行耦合在一起。
// 不要向此列表添加更多资源！
var validationSet = sets.NewString(
	string(corev1.ResourceCPU),
	string(corev1.ResourceMemory),
	string(corev1.ResourceRequestsCPU),
	string(corev1.ResourceRequestsMemory),
	string(corev1.ResourceLimitsCPU),
	string(corev1.ResourceLimitsMemory),
)

// NewPodEvaluator 创建 Pod 配额评估器。
// clock 参数用于测试时注入模拟时钟。
func NewPodEvaluator(f quota.ListerForResourceFunc, clock clock.Clock) quota.Evaluator {
	listFuncByNamespace := generic.ListResourceUsingListerFunc(f, corev1.SchemeGroupVersion.WithResource("pods"))
	podEvaluator := &podEvaluator{listFuncByNamespace: listFuncByNamespace, clock: clock}
	return podEvaluator
}

// podEvaluator 实现 Pod 配额使用量的计算。
type podEvaluator struct {
	// listFuncByNamespace 按命名空间列出 Pod
	listFuncByNamespace generic.ListFuncByNamespace
	// clock 用于时间追踪，判断 Pod 是否超过删除宽限期
	clock clock.Clock
}

// Constraints 验证 Pod 是否设置了所有必需的资源。
// 如果配额中配置了 CPU/内存，则每个容器必须显式声明对应的 requests。
// 这是一个向后兼容要求，新资源不应重复此模式。
// 此外还验证资源值的有效性（requests < limits）。
func (p *podEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	pod, err := toExternalPodOrError(item)
	if err != nil {
		return err
	}

	// 旧版本要求：如果配额启用了 CPU/内存，则每个容器必须显式声明 requests。
	// 这是一个设计缺陷，将资源验证与配额执行耦合。启用 PodLevelResources 特性后，
	// 容器级资源不再是必须的，因为可以在 Pod 级别设置资源。
	// 此时跳过容器级请求缺失的检查。
	if feature.DefaultFeatureGate.Enabled(features.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		return nil
	}

	// 向后兼容要求：如果配额了 CPU 或内存，每个容器必须显式请求该资源。
	// 这是一个历史错误，在 QoS 定义之前就耦合了验证和资源计数。
	// 现在 QoS 已定义，不再对其他资源重复此错误。
	// 只检查 validationSet 中的资源（CPU/内存及其 requests/limits）。
	requiredSet := quota.ToSet(required).Intersection(validationSet)
	missingSetResourceToContainerNames := make(map[string]sets.String)
	for i := range pod.Spec.Containers {
		enforcePodContainerConstraints(&pod.Spec.Containers[i], requiredSet, missingSetResourceToContainerNames)
	}
	for i := range pod.Spec.InitContainers {
		enforcePodContainerConstraints(&pod.Spec.InitContainers[i], requiredSet, missingSetResourceToContainerNames)
	}
	if len(missingSetResourceToContainerNames) == 0 {
		return nil
	}
	var resources = sets.NewString()
	for resource := range missingSetResourceToContainerNames {
		resources.Insert(resource)
	}
	var errorMessages = make([]string, 0, len(missingSetResourceToContainerNames))
	for _, resource := range resources.List() {
		errorMessages = append(errorMessages, fmt.Sprintf("%s for: %s", resource, strings.Join(missingSetResourceToContainerNames[resource].List(), ",")))
	}
	return fmt.Errorf("must specify %s", strings.Join(errorMessages, "; "))
}

// GroupResource 返回此评估器跟踪的 GroupResource（Pod）。
func (p *podEvaluator) GroupResource() schema.GroupResource {
	return corev1.SchemeGroupVersion.WithResource("pods").GroupResource()
}

// Handles 判断评估器是否应该处理指定的 admission 操作。
// 处理 Create 操作、resize subresource 的 Update 操作，
// 以及 Pod 的 terminating 状态发生变化时的 Update 操作。
func (p *podEvaluator) Handles(a admission.Attributes) bool {
	op := a.GetOperation()
	switch a.GetSubresource() {
	case "":
		if op == admission.Update {
			pod, err1 := toExternalPodOrError(a.GetObject())
			oldPod, err2 := toExternalPodOrError(a.GetOldObject())
			if err1 != nil || err2 != nil {
				return false
			}
			// 当 Pod 的 terminating 状态发生变化时，需要重新计算配额
			// 因为 terminating 和非 terminating Pod 可能属于不同的配额 scope
			if IsTerminating(oldPod) != IsTerminating(pod) {
				return true
			}
		}
		return op == admission.Create
	case "resize":
		// resize subresource 的 Update 操作会改变资源请求，需要重新评估配额
		return op == admission.Update
	default:
		return false
	}
}

// Matches 判断给定的 ResourceQuota 是否与输入项（Pod）匹配。
// 检查资源匹配和 scope 匹配两个条件。
func (p *podEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	return generic.Matches(resourceQuota, item, p.MatchingResources, podMatchesScopeFunc)
}

// MatchingResources 从输入的资源列表中返回与 Pod 配额相关的资源子集。
// 包括固定资源（podResources）、前缀匹配资源（如 HugePages）和扩展资源。
func (p *podEvaluator) MatchingResources(input []corev1.ResourceName) []corev1.ResourceName {
	result := quota.Intersection(input, podResources)
	for _, resource := range input {
		// 匹配具有特定前缀的资源，如 HugePages
		if quota.ContainsPrefix(podResourcePrefixes, resource) {
			result = append(result, resource)
		}
		// 匹配扩展资源（如 DRA 设备）
		if isExtendedResourceNameForQuota(resource) {
			result = append(result, resource)
		}
	}

	return result
}

// MatchingScopes 返回 Pod 匹配的 scope selector 列表。
// 用于确定 Pod 属于哪些配额 scope（如 BestEffort、Terminating 等）。
func (p *podEvaluator) MatchingScopes(item runtime.Object, scopeSelectors []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	matchedScopes := []corev1.ScopedResourceSelectorRequirement{}
	for _, selector := range scopeSelectors {
		match, err := podMatchesScopeFunc(selector, item)
		if err != nil {
			return []corev1.ScopedResourceSelectorRequirement{}, fmt.Errorf("error on matching scope %v: %v", selector, err)
		}
		if match {
			matchedScopes = append(matchedScopes, selector)
		}
	}
	return matchedScopes, nil
}

// UncoveredQuotaScopes 返回有限制但没有对应配额覆盖的 scope 列表。
// 用于检测配置了 scope 限制但没有对应 ResourceQuota 覆盖的情况。
func (p *podEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	uncoveredScopes := []corev1.ScopedResourceSelectorRequirement{}
	for _, selector := range limitedScopes {
		isCovered := false
		for _, matchedScopeSelector := range matchedQuotaScopes {
			if matchedScopeSelector.ScopeName == selector.ScopeName {
				isCovered = true
				break
			}
		}

		if !isCovered {
			uncoveredScopes = append(uncoveredScopes, selector)
		}
	}
	return uncoveredScopes, nil
}

// Usage 计算单个 Pod 的配额使用量。
// 委托给 PodUsageFunc 实现。
func (p *podEvaluator) Usage(item runtime.Object) (corev1.ResourceList, error) {
	// delegate to normal usage
	return PodUsageFunc(item, p.clock)
}

// UsageStats 计算命名空间内所有 Pod 的聚合配额使用统计。
// 包括硬限制、当前使用量和已使用用量。
func (p *podEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	return generic.CalculateUsageStats(options, p.listFuncByNamespace, podMatchesScopeFunc, p.Usage)
}

// verifies we implement the required interface.
var _ quota.Evaluator = &podEvaluator{}

// enforcePodContainerConstraints 检查容器中未设置的必需资源，并添加到 missingSet。
// 比较容器的 requests/limits 与 requiredSet，找出缺失的资源。
func enforcePodContainerConstraints(container *corev1.Container, requiredSet sets.String, missingSetResourceToContainerNames map[string]sets.String) {
	requests := container.Resources.Requests
	limits := container.Resources.Limits
	containerUsage := podComputeUsageHelper(requests, limits)
	containerSet := quota.ToSet(quota.ResourceNames(containerUsage))
	if !containerSet.Equal(requiredSet) {
		if difference := requiredSet.Difference(containerSet); difference.Len() != 0 {
			for _, diff := range difference.List() {
				if _, ok := missingSetResourceToContainerNames[diff]; !ok {
					missingSetResourceToContainerNames[diff] = sets.NewString(container.Name)
				} else {
					missingSetResourceToContainerNames[diff].Insert(container.Name)
				}
			}
		}
	}
}

// podComputeUsageHelper 根据 requests 和 limits 汇总 Pod 的计算资源配额使用量。
// 为每种资源类型（CPU、内存、存储、HugePages、扩展资源）计算对应的配额计数。
func podComputeUsageHelper(requests corev1.ResourceList, limits corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	// 每个 Pod 计数为 1
	result[corev1.ResourcePods] = resource.MustParse("1")
	if request, found := requests[corev1.ResourceCPU]; found {
		result[corev1.ResourceCPU] = request
		result[corev1.ResourceRequestsCPU] = request
	}
	if limit, found := limits[corev1.ResourceCPU]; found {
		result[corev1.ResourceLimitsCPU] = limit
	}
	if request, found := requests[corev1.ResourceMemory]; found {
		result[corev1.ResourceMemory] = request
		result[corev1.ResourceRequestsMemory] = request
	}
	if limit, found := limits[corev1.ResourceMemory]; found {
		result[corev1.ResourceLimitsMemory] = limit
	}
	if request, found := requests[corev1.ResourceEphemeralStorage]; found {
		result[corev1.ResourceEphemeralStorage] = request
		result[corev1.ResourceRequestsEphemeralStorage] = request
	}
	if limit, found := limits[corev1.ResourceEphemeralStorage]; found {
		result[corev1.ResourceLimitsEphemeralStorage] = limit
	}
	// 遍历所有请求的资源，处理特殊前缀和扩展资源
	for resource, request := range requests {
		// 处理具有特定前缀的资源，如 HugePages
		if quota.ContainsPrefix(requestedResourcePrefixes, resource) {
			result[resource] = request
			result[maskResourceWithPrefix(resource, corev1.DefaultResourceRequestsPrefix)] = request
		}
		// 处理 DRA 扩展资源，只允许 "requests.resourceName" 格式
		if schedutil.IsDRAExtendedResourceName(resource) {
			// only quota objects in format of "requests.resourceName" is allowed for extended resource.
			result[maskResourceWithPrefix(resource, corev1.DefaultResourceRequestsPrefix)] = request
		}
	}

	return result
}

// toExternalPodOrError 将 runtime.Object 转换为外部 Pod 对象。
// 支持内部 api.Pod 和外部 v1.Pod 两种类型。
func toExternalPodOrError(obj runtime.Object) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case *api.Pod:
		if err := k8s_api_v1.Convert_core_Pod_To_v1_Pod(t, pod, nil); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("expect *api.Pod or *v1.Pod, got %v", t)
	}
	return pod, nil
}

// podMatchesScopeFunc 判断 Pod 是否匹配给定的 scope selector。
// 支持的 scope 包括：Terminating、NotTerminating、BestEffort、NotBestEffort、
// PriorityClass 和 CrossNamespacePodAffinity。
func podMatchesScopeFunc(selector corev1.ScopedResourceSelectorRequirement, object runtime.Object) (bool, error) {
	pod, err := toExternalPodOrError(object)
	if err != nil {
		return false, err
	}
	switch selector.ScopeName {
	case corev1.ResourceQuotaScopeTerminating:
		return IsTerminating(pod), nil
	case corev1.ResourceQuotaScopeNotTerminating:
		return !IsTerminating(pod), nil
	case corev1.ResourceQuotaScopeBestEffort:
		return isBestEffort(pod), nil
	case corev1.ResourceQuotaScopeNotBestEffort:
		return !isBestEffort(pod), nil
	case corev1.ResourceQuotaScopePriorityClass:
		// Exists 操作符只需检查 Pod 是否有 PriorityClassName，无需解析 selector
		if selector.Operator == corev1.ScopeSelectorOpExists {
			// This is just checking for existence of a priorityClass on the pod,
			// no need to take the overhead of selector parsing/evaluation.
			return len(pod.Spec.PriorityClassName) != 0, nil
		}
		return podMatchesSelector(pod, selector)
	case corev1.ResourceQuotaScopeCrossNamespacePodAffinity:
		// 检查 Pod 是否使用了跨命名空间的 Pod 亲和性/反亲和性
		return usesCrossNamespacePodAffinity(pod), nil
	}
	return false, nil
}

// PodUsageFunc 计算 Pod 的配额使用量。
// 只有当 Pod 满足以下条件时才会计入配额：
//   - Pod 未处于终止状态（Failed 或 Succeeded）
//   - Pod 未被标记删除或 grace period 未过期
//
// 对象计数配额（count/pods）始终计算，无论 Pod 状态如何。
// 计算资源配额（如 CPU、内存）只跟踪活跃 Pod。
func PodUsageFunc(obj runtime.Object, clock clock.Clock) (corev1.ResourceList, error) {
	pod, err := toExternalPodOrError(obj)
	if err != nil {
		return corev1.ResourceList{}, err
	}

	// 对象计数配额始终计算，即使 Pod 已到达生命周期终点。
	// 因为对象计数配额跟踪存储中的所有对象。
	// "pods" 资源只跟踪未到达终止状态的 Pod，
	// 而 "count/pods" 跟踪所有状态的 Pod。
	result := corev1.ResourceList{
		podObjectCountName: *(resource.NewQuantity(1, resource.DecimalSI)),
	}

	// 按照惯例，不计算已到达生命周期终点的 Pod 的计算资源配额。
	// "pods" 资源被视为计算资源，因为它与生命周期绑定。
	if !QuotaV1Pod(pod, clock) {
		return result, nil
	}

	// 配置 Pod 资源计算选项
	opts := resourcehelper.PodResourcesOptions{
		// 如果启用了原地垂直扩缩，使用 Status.Resources 而非 Spec.Resources
		UseStatusResources: feature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		// 启用 PodLevelResources 时，不跳过 Pod 级资源
		SkipPodLevelResources: !feature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	}
	requests := resourcehelper.PodRequests(pod, opts)
	limits := resourcehelper.PodLimits(pod, opts)

	result = quota.Add(result, podComputeUsageHelper(requests, limits))
	return result, nil
}

// isBestEffort 判断 Pod 是否为 BestEffort QoS 级别。
// BestEffort Pod 未设置任何资源请求或限制。
func isBestEffort(pod *corev1.Pod) bool {
	return qos.GetPodQOS(pod) == corev1.PodQOSBestEffort
}

// IsTerminating 判断 Pod 是否处于终止状态。
// 设置了 ActiveDeadlineSeconds 的 Pod 被视为终止 Pod（如 Job 创建的 Pod）。
func IsTerminating(pod *corev1.Pod) bool {
	if pod.Spec.ActiveDeadlineSeconds != nil && *pod.Spec.ActiveDeadlineSeconds >= int64(0) {
		return true
	}
	return false
}

// podMatchesSelector 检查 Pod 的 PriorityClass 是否匹配给定的 label selector。
func podMatchesSelector(pod *corev1.Pod, selector corev1.ScopedResourceSelectorRequirement) (bool, error) {
	labelSelector, err := helper.ScopedResourceSelectorRequirementsAsSelector(selector)
	if err != nil {
		return false, fmt.Errorf("failed to parse and convert selector: %v", err)
	}
	var m map[string]string
	if len(pod.Spec.PriorityClassName) != 0 {
		m = map[string]string{string(corev1.ResourceQuotaScopePriorityClass): pod.Spec.PriorityClassName}
	}
	if labelSelector.Matches(labels.Set(m)) {
		return true, nil
	}
	return false, nil
}

// crossNamespacePodAffinityTerm 判断亲和性项是否指定了跨命名空间的选择器。
// 设置了 Namespaces 或 NamespaceSelector 表示跨命名空间。
func crossNamespacePodAffinityTerm(term *corev1.PodAffinityTerm) bool {
	return len(term.Namespaces) != 0 || term.NamespaceSelector != nil
}

// crossNamespacePodAffinityTerms 检查亲和性项列表中是否有跨命名空间的项。
func crossNamespacePodAffinityTerms(terms []corev1.PodAffinityTerm) bool {
	for _, t := range terms {
		if crossNamespacePodAffinityTerm(&t) {
			return true
		}
	}
	return false
}

// crossNamespaceWeightedPodAffinityTerms 检查加权亲和性项列表中是否有跨命名空间的项。
func crossNamespaceWeightedPodAffinityTerms(terms []corev1.WeightedPodAffinityTerm) bool {
	for _, t := range terms {
		if crossNamespacePodAffinityTerm(&t.PodAffinityTerm) {
			return true
		}
	}
	return false
}

// usesCrossNamespacePodAffinity 检查 Pod 是否使用了跨命名空间的亲和性或反亲和性。
// 包括 Required 和 Preferred 两种类型。
func usesCrossNamespacePodAffinity(pod *corev1.Pod) bool {
	if pod == nil || pod.Spec.Affinity == nil {
		return false
	}

	affinity := pod.Spec.Affinity.PodAffinity
	if affinity != nil {
		if crossNamespacePodAffinityTerms(affinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
		if crossNamespaceWeightedPodAffinityTerms(affinity.PreferredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
	}

	antiAffinity := pod.Spec.Affinity.PodAntiAffinity
	if antiAffinity != nil {
		if crossNamespacePodAffinityTerms(antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
		if crossNamespaceWeightedPodAffinityTerms(antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) {
			return true
		}
	}

	return false
}

// QuotaV1Pod 判断 Pod 是否应该计入配额跟踪。
// 只有非终止状态且未超过删除宽限期的 Pod 才计入配额。
//
// 如果 Pod 卡住无法终止（如节点丢失），不将其计入配额，
// 避免阻止用户创建新 Pod 来提供服务。
func QuotaV1Pod(pod *corev1.Pod, clock clock.Clock) bool {
	// 终止状态的 Pod 不计入计算资源配额
	if corev1.PodFailed == pod.Status.Phase || corev1.PodSucceeded == pod.Status.Phase {
		return false
	}
	// 如果 Pod 卡住无法终止（如节点丢失），不将其计入配额。
	// 否则用户可能因为配额耗尽而无法创建新 Pod 来提供服务。
	// 检查是否超过了删除时间 + grace period
	if pod.DeletionTimestamp != nil && pod.DeletionGracePeriodSeconds != nil {
		now := clock.Now()
		deletionTime := pod.DeletionTimestamp.Time
		gracePeriod := time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second
		if now.After(deletionTime.Add(gracePeriod)) {
			return false
		}
	}
	return true
}
