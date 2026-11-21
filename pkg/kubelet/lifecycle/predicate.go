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

package lifecycle

import (
	"context"
	"fmt"
	"runtime"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

const (
	// PodOSSelectorNodeLabelDoesNotMatch 表示 Pod 被节点拒绝准入，原因是 Pod 针对
	// kubernetes.io/os 标签的节点选择器与节点标签不匹配。
	PodOSSelectorNodeLabelDoesNotMatch = "PodOSSelectorNodeLabelDoesNotMatch"

	// PodOSNotSupported 表示 Pod 被节点拒绝准入，原因是 Pod 的 OS 字段与节点操作系统不一致。
	PodOSNotSupported = "PodOSNotSupported"

	// InvalidNodeInfo 表示 Pod 被节点拒绝准入，原因是 kubelet 无法获取节点信息。
	InvalidNodeInfo = "InvalidNodeInfo"

	// InitContainerRestartPolicyForbidden 表示 Pod 被节点拒绝准入，原因是其部分
	// 初始化容器使用了 Always 之外的重启策略。
	InitContainerRestartPolicyForbidden = "InitContainerRestartPolicyForbidden"

	// SupplementalGroupsPolicyNotSupported 表示 Pod 被节点拒绝准入，原因是节点
	// 不支持 Pod 声明的 SupplementalGroupsPolicy。
	SupplementalGroupsPolicyNotSupported = "SupplementalGroupsPolicyNotSupported"

	// UnexpectedAdmissionError 表示 Pod 被节点拒绝准入，原因是准入过程中出现了
	// 无法归类的错误。
	UnexpectedAdmissionError = "UnexpectedAdmissionError"

	// UnknownReason 表示 Pod 被节点拒绝准入，原因是某个断言失败但无法确定具体原因。
	UnknownReason = "UnknownReason"

	// UnexpectedPredicateFailureType 表示 Pod 被节点拒绝准入，原因是断言返回了
	// InsufficientResourceError 或 PredicateFailureError 之外的原因对象。
	UnexpectedPredicateFailureType = "UnexpectedPredicateFailureType"

	// InsufficientResourcePrefix 是 kubelet 因资源不足拒绝 Pod 时准入原因的前缀。
	InsufficientResourcePrefix = "OutOf"

	// 以下原因表示节点因资源不足而拒绝 Pod 准入。
	OutOfCPU              = "OutOfcpu"
	OutOfMemory           = "OutOfmemory"
	OutOfEphemeralStorage = "OutOfephemeral-storage"
	OutOfPods             = "OutOfpods"
)

// getNodeAnyWayFuncType 是获取节点信息的函数类型；useCache 为 false 时需绕过缓存同步获取。
type getNodeAnyWayFuncType func(ctx context.Context, useCache bool) (*v1.Node, error)

// pluginResourceUpdateFuncType 负责把设备插件上报的资源更新到 NodeInfo 中。
type pluginResourceUpdateFuncType func(*schedulerframework.NodeInfo, *PodAdmitAttributes) error

// AdmissionFailureHandler 定义 Pod 准入失败后的处理方式，
// 以便在资源不足等场景下进行优雅的降级或抢占处理。
type AdmissionFailureHandler interface {
	HandleAdmissionFailure(ctx context.Context, admitPod *v1.Pod, failureReasons []PredicateFailureReason, operation Operation) ([]PredicateFailureReason, error)
}

// predicateAdmitHandler 通过调度器断言（predicate）判断 Pod 能否被本节点准入。
type predicateAdmitHandler struct {
	getNodeAnyWayFunc        getNodeAnyWayFuncType
	pluginResourceUpdateFunc pluginResourceUpdateFuncType
	admissionFailureHandler  AdmissionFailureHandler
}

var _ PodAdmitHandler = &predicateAdmitHandler{}

// NewPredicateAdmitHandler 返回一个 PodAdmitHandler，从断言（predicate）的角度
// 评估 Pod 是否可以被准入。
func NewPredicateAdmitHandler(getNodeAnyWayFunc getNodeAnyWayFuncType, admissionFailureHandler AdmissionFailureHandler, pluginResourceUpdateFunc pluginResourceUpdateFuncType) PodAdmitHandler {
	return &predicateAdmitHandler{
		getNodeAnyWayFunc,
		pluginResourceUpdateFunc,
		admissionFailureHandler,
	}
}

// Admit 判断 Pod 是否满足准入条件，包括 OS 匹配、资源充足与污点容忍等。
func (w *predicateAdmitHandler) Admit(ctx context.Context, attrs *PodAdmitAttributes) PodAdmitResult {
	logger := klog.FromContext(ctx)
	// 优先使用缓存的节点信息，避免每次准入都产生 API 调用开销。
	node, err := w.getNodeAnyWayFunc(ctx, true)
	if err != nil {
		logger.Error(err, "Cannot get Node info")
		return PodAdmitResult{
			Admit:   false,
			Reason:  InvalidNodeInfo,
			Message: "Kubelet cannot get node info.",
		}
	}
	admitPod := attrs.Pod

	// 先执行抢占也无济于事的检查，避免触发无意义的 Pod 驱逐。
	if rejectPodAdmissionBasedOnOSSelector(admitPod, node) {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodOSSelectorNodeLabelDoesNotMatch,
			Message: "Failed to admit pod as the `kubernetes.io/os` label doesn't match node label",
		}
	}
	if rejectPodAdmissionBasedOnOSField(admitPod) {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodOSNotSupported,
			Message: "Failed to admit pod as the OS field doesn't match node OS",
		}
	}

	if rejectPodAdmissionBasedOnSupplementalGroupsPolicy(admitPod, node) {
		message := fmt.Sprintf("SupplementalGroupsPolicy=%s is not supported in this node", v1.SupplementalGroupsPolicyStrict)
		logger.Info("Failed to admit pod", "pod", klog.KObj(admitPod), "message", message)
		return PodAdmitResult{
			Admit:   false,
			Reason:  SupplementalGroupsPolicyNotSupported,
			Message: message,
		}
	}

	// 构造包含节点上已有 Pod 的 NodeInfo，供后续资源与亲和性断言使用。
	pods := attrs.OtherPods
	nodeInfo := schedulerframework.NewNodeInfo(pods...)
	nodeInfo.SetNode(node)

	// 确保节点上的插件资源（如设备插件上报的资源）足以满足 Pod 的请求。
	if err = w.pluginResourceUpdateFunc(nodeInfo, attrs); err != nil {
		message := fmt.Sprintf("Update plugin resources failed due to %v, which is unexpected.", err)
		logger.Info("Failed to admit pod", "pod", klog.KObj(admitPod), "message", message)
		return PodAdmitResult{
			Admit:   false,
			Reason:  UnexpectedAdmissionError,
			Message: message,
		}
	}

	// 从 Pod 副本中移除节点信息里不存在的扩展资源请求。这样做是为了支持
	// 集群级资源（节点并不知晓的扩展资源）以及由 DRA 支撑的扩展资源。
	//
	// 注意：如果 Pod 被手动绑定到某节点（例如静态 Pod），而它要求的节点级
	// 扩展资源在该节点上并不存在，kubelet 将不会按预期拒绝准入。该问题
	// 后续将通过 Resource Class API 解决。
	podWithoutMissingExtendedResources := removeMissingExtendedResources(admitPod, nodeInfo)

	// 执行一组 kubelet 关心的过滤断言，无任何失败原因即视为可调度。
	reasons := w.generalFilter(ctx, podWithoutMissingExtendedResources, nodeInfo)
	fit := len(reasons) == 0
	if !fit {
		// 准入失败时交给失败处理器（如抢占逻辑）尝试恢复。
		reasons, err = w.admissionFailureHandler.HandleAdmissionFailure(ctx, admitPod, reasons, attrs.Operation)
		fit = len(reasons) == 0 && err == nil
		if err != nil {
			message := fmt.Sprintf("Unexpected error while attempting to recover from admission failure: %v", err)
			logger.Info("Failed to admit pod, unexpected error while attempting to recover from admission failure", "pod", klog.KObj(admitPod), "err", err)
			return PodAdmitResult{
				Admit:   fit,
				Reason:  UnexpectedAdmissionError,
				Message: message,
			}
		}
	}
	if !fit {
		var reason string
		var message string
		if len(reasons) == 0 {
			message = fmt.Sprint("GeneralPredicates failed due to unknown reason, which is unexpected.")
			logger.Info("Failed to admit pod: GeneralPredicates failed due to unknown reason, which is unexpected", "pod", klog.KObj(admitPod))
			return PodAdmitResult{
				Admit:   fit,
				Reason:  UnknownReason,
				Message: message,
			}
		}
		// 存在多个失败断言时只取第一个作为拒绝原因，避免信息过载。
		r := reasons[0]
		switch re := r.(type) {
		case *PredicateFailureError:
			reason = re.PredicateName
			message = re.Error()
			logger.V(2).Info("Predicate failed on Pod", "pod", klog.KObj(admitPod), "err", message)
		case *InsufficientResourceError:
			switch re.ResourceName {
			case v1.ResourceCPU:
				reason = OutOfCPU
			case v1.ResourceMemory:
				reason = OutOfMemory
			case v1.ResourceEphemeralStorage:
				reason = OutOfEphemeralStorage
			case v1.ResourcePods:
				reason = OutOfPods
			default:
				reason = fmt.Sprintf("%s%s", InsufficientResourcePrefix, re.ResourceName)
			}
			message = re.Error()
			logger.V(2).Info("Predicate failed on Pod", "pod", klog.KObj(admitPod), "err", message)
		default:
			reason = UnexpectedPredicateFailureType
			message = fmt.Sprintf("GeneralPredicates failed due to %v, which is unexpected.", r)
			logger.Info("Failed to admit pod", "pod", klog.KObj(admitPod), "err", message)
		}
		return PodAdmitResult{
			Admit:   fit,
			Reason:  reason,
			Message: message,
		}
	}
	return PodAdmitResult{
		Admit: true,
	}
}

// generalFilter 执行一组 kubelet 关心的过滤断言；若失败原因仅涉及节点亲和性，
// 则绕过缓存重新获取节点信息后重试，以规避节点标签尚未同步带来的误判。
func (w *predicateAdmitHandler) generalFilter(ctx context.Context, pod *v1.Pod, nodeInfo *schedulerframework.NodeInfo) []PredicateFailureReason {
	logger := klog.FromContext(ctx)

	reasons := generalFilter(logger, pod, nodeInfo)
	for _, r := range reasons {
		if r.GetReason() != nodeaffinity.ErrReasonPod {
			return reasons
		}
	}
	if len(reasons) > 0 {
		// 若唯一失败原因是节点亲和性标签，则同步拉取最新节点信息后重试，
		// 因为节点标签可能尚未通过周期性状态同步刷新到本地缓存。
		node, err := w.getNodeAnyWayFunc(ctx, false)
		if err != nil {
			logger.Error(err, "Failed to synchronously fetch node info")
			return reasons
		}
		nodeInfo.SetNode(node)
		reasons = generalFilter(logger, pod, nodeInfo)
	}

	return reasons
}

// rejectPodAdmissionBasedOnOSSelector 在 Pod 的节点选择器与节点 OS 标签不匹配时拒绝准入。
// 若发现不匹配，依赖每 10 秒一次的 kubelet 状态同步来更新节点标签。
func rejectPodAdmissionBasedOnOSSelector(pod *v1.Pod, node *v1.Node) bool {
	labels := node.Labels
	osName, osLabelExists := labels[v1.LabelOSStable]
	if !osLabelExists || osName != runtime.GOOS {
		if len(labels) == 0 {
			labels = make(map[string]string)
		}
		labels[v1.LabelOSStable] = runtime.GOOS
	}
	podLabelSelector, podOSLabelExists := pod.Labels[v1.LabelOSStable]
	if !podOSLabelExists {
		// 未设置 OS 标签选择器时保持原有行为，不做拒绝。
		return false
	} else if podOSLabelExists && podLabelSelector != labels[v1.LabelOSStable] {
		return true
	}
	return false
}

// rejectPodAdmissionBasedOnOSField 在 Pod 的 OS 字段与 runtime.GOOS 不一致时拒绝准入。
// TODO: 当 Kubernetes 支持 LCOW（Pod OS 可能与节点 OS 不同）后，应放宽此限制。
func rejectPodAdmissionBasedOnOSField(pod *v1.Pod) bool {
	if pod.Spec.OS == nil {
		return false
	}
	// Pod OS 与 runtime.GOOS 不一致时拒绝准入。
	return string(pod.Spec.OS.Name) != runtime.GOOS
}

// rejectPodAdmissionBasedOnSupplementalGroupsPolicy 仅在以下条件同时满足时拒绝 Pod：
// - 该特性处于 Beta 或更高阶段，且 Pod 中设置了 SupplementalPolicy=Strict；
// - 但节点并不支持该特性。
//
// 注意：当该特性仍处于 Alpha 阶段（或尚未发布的模拟版本）时，出于向后兼容
// 应允许准入。
func rejectPodAdmissionBasedOnSupplementalGroupsPolicy(pod *v1.Pod, node *v1.Node) bool {
	admit, reject := false, true // just for readability

	inUse := (pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.SupplementalGroupsPolicy != nil)
	if !inUse {
		return admit
	}

	isBetaOrAbove := false
	if featureSpec, ok := utilfeature.DefaultMutableFeatureGate.GetAll()[features.SupplementalGroupsPolicy]; ok {
		isBetaOrAbove = (featureSpec.PreRelease == featuregate.Beta) || (featureSpec.PreRelease == featuregate.GA)
	}

	if !isBetaOrAbove {
		return admit
	}

	featureSupportedOnNode := ptr.Deref(
		ptr.Deref(node.Status.Features, v1.NodeFeatures{SupplementalGroupsPolicy: ptr.To(false)}).SupplementalGroupsPolicy,
		false,
	)
	effectivePolicy := ptr.Deref(
		pod.Spec.SecurityContext.SupplementalGroupsPolicy,
		v1.SupplementalGroupsPolicyMerge,
	)

	if effectivePolicy == v1.SupplementalGroupsPolicyStrict && !featureSupportedOnNode {
		return reject
	}

	return admit
}

// removeMissingExtendedResources 返回 Pod 的副本，其中节点上不存在的扩展资源请求被剔除；
// 由 DRA 支撑的扩展资源同样被剔除，避免调度器的资源断言误判。
func removeMissingExtendedResources(pod *v1.Pod, nodeInfo *schedulerframework.NodeInfo) *v1.Pod {
	// 判断某个扩展资源请求是否由 DRA（动态资源分配）支撑。
	isResourceBackedByDRA := func(resourceName v1.ResourceName, containerName string) bool {
		if !utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
			return false
		}
		if pod.Status.ExtendedResourceClaimStatus == nil {
			return false
		}
		for _, resourceMapping := range pod.Status.ExtendedResourceClaimStatus.RequestMappings {
			if v1.ResourceName(resourceMapping.ResourceName) == resourceName && resourceMapping.ContainerName == containerName {
				return true
			}
		}
		return false
	}
	filterExtendedResources := func(containers []v1.Container) {
		for i, c := range containers {
			// 只处理 Requests 而不处理 Limits：结果 Pod 后续会传给
			// PodFitsResources 断言，而该断言并不使用 Limits。
			filteredResources := make(v1.ResourceList)
			for rName, rQuant := range c.Resources.Requests {
				if schedutil.IsDRAExtendedResourceName(rName) && isResourceBackedByDRA(rName, c.Name) {
					continue
				}
				if v1helper.IsExtendedResourceName(rName) {
					if _, found := nodeInfo.Allocatable.ScalarResources[rName]; !found {
						continue
					}
				}
				filteredResources[rName] = rQuant
			}
			containers[i].Resources.Requests = filteredResources
		}
	}
	// 深拷贝后再过滤，避免修改调用方持有的原始 Pod 对象。
	podCopy := pod.DeepCopy()
	filterExtendedResources(podCopy.Spec.Containers)
	filterExtendedResources(podCopy.Spec.InitContainers)
	return podCopy
}

// InsufficientResourceError 描述因某类资源达到上限而导致的不适配失败。
type InsufficientResourceError struct {
	ResourceName v1.ResourceName
	Requested    int64
	Used         int64
	Capacity     int64
}

// Error 返回资源不足错误的可读描述。
func (e *InsufficientResourceError) Error() string {
	return fmt.Sprintf("Node didn't have enough resource: %s, requested: %d, used: %d, capacity: %d",
		e.ResourceName, e.Requested, e.Used, e.Capacity)
}

// PredicateFailureReason 接口表示某个断言失败的原因。
type PredicateFailureReason interface {
	GetReason() string
}

// GetReason 返回资源不足错误的原因描述。
func (e *InsufficientResourceError) GetReason() string {
	return fmt.Sprintf("Insufficient %v", e.ResourceName)
}

// GetInsufficientAmount 返回该错误对应的资源缺口数量。
func (e *InsufficientResourceError) GetInsufficientAmount() int64 {
	return e.Requested - (e.Capacity - e.Used)
}

// PredicateFailureError 描述断言失败的错误。
type PredicateFailureError struct {
	PredicateName string
	PredicateDesc string
}

// Error 返回断言失败错误的可读描述。
func (e *PredicateFailureError) Error() string {
	return fmt.Sprintf("Predicate %s failed: %s", e.PredicateName, e.PredicateDesc)
}

// GetReason 返回断言失败错误的原因描述。
func (e *PredicateFailureError) GetReason() string {
	return e.PredicateDesc
}

// generalFilter 执行一组 kubelet 关心的过滤断言，汇总资源不足与断言失败两类原因。
func generalFilter(logger klog.Logger, pod *v1.Pod, nodeInfo *schedulerframework.NodeInfo) []PredicateFailureReason {
	// 复用调度器的准入检查逻辑，保证 kubelet 与调度器对可调度性的判断一致。
	admissionResults := scheduler.AdmissionCheck(pod, nodeInfo, true)
	var reasons []PredicateFailureReason
	for _, r := range admissionResults {
		if r.InsufficientResource != nil {
			reasons = append(reasons, &InsufficientResourceError{
				ResourceName: r.InsufficientResource.ResourceName,
				Requested:    r.InsufficientResource.Requested,
				Used:         r.InsufficientResource.Used,
				Capacity:     r.InsufficientResource.Capacity,
			})
		} else {
			reasons = append(reasons, &PredicateFailureError{r.Name, r.Reason})
		}
	}

	// 检查污点与容忍，静态 Pod 例外：它们由节点自身创建，不应被节点污点阻挡。
	if !types.IsStaticPod(pod) {
		_, isUntolerated := corev1.FindMatchingUntoleratedTaint(logger, nodeInfo.Node().Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			// kubelet 只关心 NoExecute 类型的污点，其他效果由调度器处理。
			return t.Effect == v1.TaintEffectNoExecute
		}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
		if isUntolerated {
			reasons = append(reasons, &PredicateFailureError{tainttoleration.Name, tainttoleration.ErrReasonNotMatch})
		}
	}

	return reasons
}
