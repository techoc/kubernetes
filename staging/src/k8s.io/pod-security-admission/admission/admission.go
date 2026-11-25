/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"k8s.io/klog/v2"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	admissionapi "k8s.io/pod-security-admission/admission/api"
	"k8s.io/pod-security-admission/admission/api/validation"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/metrics"
	"k8s.io/pod-security-admission/policy"
)

// 兜底上限与耗时预算：预检既有 Pod 既要给出有用警告，又不能显著拖慢命名空间更新。
const (
	defaultNamespaceMaxPodsToCheck  = 3000
	defaultNamespacePodCheckTimeout = 1 * time.Second
)

// Admission 实现 PodSecurity 准入控制器的核心逻辑，聚合配置、评估器与外部依赖；
// 依赖字段可替换，便于测试隔离。
type Admission struct {
	Configuration *admissionapi.PodSecurityConfiguration

	// Evaluator 按等级与版本获取检查项，由调用方注入以支持独立测试。
	Evaluator policy.Evaluator

	// Metrics 记录决策与豁免；缺失时拒绝启动，避免静默失去观测能力。
	Metrics metrics.Recorder

	// PodSpecExtractor 从控制器对象中抽取 PodSpec，以复用 Pod 检查逻辑。
	PodSpecExtractor PodSpecExtractor

	// NamespaceGetter 与 PodLister 只读取命名空间标签和既有 Pod，无需写权限。
	NamespaceGetter NamespaceGetter
	PodLister       PodLister

	// 未打标签的命名空间走这套默认策略，由 CompleteConfiguration 派生，避免每次请求重复解析。
	defaultPolicy api.Policy

	// 预检既有 Pod 的预算，由 CompleteConfiguration 设为默认值；校验阶段据此拒绝未初始化实例。
	namespaceMaxPodsToCheck  int
	namespacePodCheckTimeout time.Duration
}

// NamespaceGetter 只暴露命名空间读取接口，便于测试注入假实现并避免误用写权限。
type NamespaceGetter interface {
	// GetNamespace 返回指定命名空间。
	GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error)
}

// PodLister 仅用于命名空间收紧策略时预检既有 Pod，返回的切片会被本地重排。
type PodLister interface {
	// ListPods 列取指定命名空间中的 Pod。
	ListPods(ctx context.Context, namespace string) ([]*corev1.Pod, error)
}

// PodSpecExtractor 从内嵌 PodSpec 的控制器对象中抽取模板；可扩展以检查自定义 Pod 控制器 CRD。
type PodSpecExtractor interface {
	// HasPodSpec 判断资源类型是否可能包含可抽取的 PodSpec。
	HasPodSpec(schema.GroupResource) bool
	// ExtractPodSpec 返回待评估的元数据与 PodSpec；无 PodSpec 时返回三个 nil。
	// 抽取失败只记入审计信息，不阻断控制器对象准入，也不直接返回给用户；
	// 实际 Pod 创建时会再次执行强制策略。
	ExtractPodSpec(runtime.Object) (*metav1.ObjectMeta, *corev1.PodSpec, error)
}

// 用集合而非切片判定资源类型，HasPodSpec 可 O(1) 查询；顺序无关，遍历时再排序。
var defaultPodSpecResources = map[schema.GroupResource]bool{
	corev1.Resource("pods"):                   true,
	corev1.Resource("replicationcontrollers"): true,
	corev1.Resource("podtemplates"):           true,
	appsv1.Resource("replicasets"):            true,
	appsv1.Resource("deployments"):            true,
	appsv1.Resource("statefulsets"):           true,
	appsv1.Resource("daemonsets"):             true,
	batchv1.Resource("jobs"):                  true,
	batchv1.Resource("cronjobs"):              true,
}

// DefaultPodSpecExtractor 只覆盖已知内建控制器；CRD 由调用方扩展，避免误判未知类型。
type DefaultPodSpecExtractor struct{}

// HasPodSpec 判断资源是否属于内建 Pod 或 Pod 控制器白名单。
func (DefaultPodSpecExtractor) HasPodSpec(gr schema.GroupResource) bool {
	return defaultPodSpecResources[gr]
}

// ExtractPodSpec 抽取内建 Pod 或控制器模板；未知资源类型返回错误。
func (DefaultPodSpecExtractor) ExtractPodSpec(obj runtime.Object) (*metav1.ObjectMeta, *corev1.PodSpec, error) {
	// 用类型断言而非 reflect，未知类型直接报错，避免把无关对象误当 Pod 模板。
	switch o := obj.(type) {
	case *corev1.Pod:
		return &o.ObjectMeta, &o.Spec, nil
	case *corev1.PodTemplate:
		return extractPodSpecFromTemplate(&o.Template)
	case *corev1.ReplicationController:
		return extractPodSpecFromTemplate(o.Spec.Template)
	case *appsv1.ReplicaSet:
		return extractPodSpecFromTemplate(&o.Spec.Template)
	case *appsv1.Deployment:
		return extractPodSpecFromTemplate(&o.Spec.Template)
	case *appsv1.DaemonSet:
		return extractPodSpecFromTemplate(&o.Spec.Template)
	case *appsv1.StatefulSet:
		return extractPodSpecFromTemplate(&o.Spec.Template)
	case *batchv1.Job:
		return extractPodSpecFromTemplate(&o.Spec.Template)
	case *batchv1.CronJob:
		return extractPodSpecFromTemplate(&o.Spec.JobTemplate.Spec.Template)
	default:
		return nil, nil, fmt.Errorf("unexpected object type: %s", obj.GetObjectKind().GroupVersionKind().String())
	}
}

// PodSpecResources 返回可检查的内建资源类型，供调用方构造 webhook 规则；
// map 遍历顺序不稳定，调用方应先排序。
func (DefaultPodSpecExtractor) PodSpecResources() []schema.GroupResource {
	retval := make([]schema.GroupResource, 0, len(defaultPodSpecResources))
	for r := range defaultPodSpecResources {
		retval = append(retval, r)
	}
	return retval
}

// 部分控制器允许空的 Pod 模板，此时返回全 nil 表示无需检查。
func extractPodSpecFromTemplate(template *corev1.PodTemplateSpec) (*metav1.ObjectMeta, *corev1.PodSpec, error) {
	if template == nil {
		return nil, nil, nil
	}
	return &template.ObjectMeta, &template.Spec, nil
}

// CompleteConfiguration 补齐默认值与派生字段，必须先于 ValidateConfiguration 调用。
func (a *Admission) CompleteConfiguration() error {
	if a.Configuration != nil {
		if p, err := admissionapi.ToPolicy(a.Configuration.Defaults); err != nil {
			return err
		} else {
			a.defaultPolicy = p
		}
	}
	a.namespaceMaxPodsToCheck = defaultNamespaceMaxPodsToCheck
	a.namespacePodCheckTimeout = defaultNamespacePodCheckTimeout

	if a.PodSpecExtractor == nil {
		a.PodSpecExtractor = &DefaultPodSpecExtractor{}
	}

	return nil
}

// ValidateConfiguration 校验必需字段与取值，并确认派生字段和配置一致，以发现初始化顺序错误。
func (a *Admission) ValidateConfiguration() error {
	if a.Configuration == nil {
		return fmt.Errorf("configuration required")
	} else if errs := validation.ValidatePodSecurityConfiguration(a.Configuration); len(errs) > 0 {
		return errs.ToAggregate()
	} else {
		if p, err := admissionapi.ToPolicy(a.Configuration.Defaults); err != nil {
			return err
		} else if !reflect.DeepEqual(p, a.defaultPolicy) {
			return fmt.Errorf("default policy does not match; CompleteConfiguration() was not called before ValidateConfiguration()")
		}
	}
	if a.namespaceMaxPodsToCheck == 0 || a.namespacePodCheckTimeout == 0 {
		return fmt.Errorf("namespace configuration not set; CompleteConfiguration() was not called before ValidateConfiguration()")
	}
	if a.Metrics == nil {
		return fmt.Errorf("Metrics recorder required")
	}
	if a.PodSpecExtractor == nil {
		return fmt.Errorf("PodSpecExtractor required")
	}
	if a.Evaluator == nil {
		return fmt.Errorf("Evaluator required")
	}
	if a.NamespaceGetter == nil {
		return fmt.Errorf("NamespaceGetter required")
	}
	if a.PodLister == nil {
		return fmt.Errorf("PodLister required")
	}
	return nil
}

// 预解析资源标识，避免每次请求重复构造 GroupResource 字符串。
var (
	namespacesResource = corev1.Resource("namespaces")
	podsResource       = corev1.Resource("pods")
)

// Validate 按资源类型分派准入请求：命名空间改标签时预检，Pod 强制执行，其余对象只审计与警告。
// 属性中的对象应为相关的外部 v1 类型；返回响应可能被共享，调用方不得修改。
func (a *Admission) Validate(ctx context.Context, attrs api.Attributes) *admissionv1.AdmissionResponse {
	var response *admissionv1.AdmissionResponse
	switch attrs.GetResource().GroupResource() {
	case namespacesResource:
		response = a.ValidateNamespace(ctx, attrs)
	case podsResource:
		response = a.ValidatePod(ctx, attrs)
	default:
		response = a.ValidatePodController(ctx, attrs)
	}
	return response
}

// ValidateNamespace 校验命名空间创建或更新请求中的 PodSecurity 标签；
// 更新 enforce 策略时预检既有 Pod。返回响应可能被共享，调用方不得修改。
// 收紧 enforce 时只警告不拒绝：存量 Pod 无法就地修改，强堵会阻止命名空间更新。
func (a *Admission) ValidateNamespace(ctx context.Context, attrs api.Attributes) *admissionv1.AdmissionResponse {
	// 子资源不承载待检查的对象，直接放行。
	if attrs.GetSubresource() != "" {
		return sharedAllowedResponse
	}
	obj, err := attrs.GetObject()
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to decode object")
		return errorResponse(err, &apierrors.NewBadRequest("failed to decode object").ErrStatus)
	}
	namespace, ok := obj.(*corev1.Namespace)
	if !ok {
		klog.FromContext(ctx).Info("failed to assert namespace type", "type", reflect.TypeOf(obj))
		return errorResponse(nil, &apierrors.NewBadRequest("failed to decode namespace").ErrStatus)
	}

	newPolicy, newErrs := a.PolicyToEvaluate(namespace.Labels)

	switch attrs.GetOperation() {
	case admissionv1.Create:
		// 创建时标签必须合法。
		if len(newErrs) > 0 {
			return invalidResponse(attrs, newErrs)
		}
		if a.exemptNamespace(attrs.GetNamespace()) {
			if warning := a.exemptNamespaceWarning(namespace.Name, newPolicy, namespace.Labels); warning != "" {
				response := allowedResponse()
				response.Warnings = append(response.Warnings, warning)
				return response
			}
		}
		return sharedAllowedResponse

	case admissionv1.Update:
		// 更新时先读取旧标签，用于判断策略是否变化。
		oldObj, err := attrs.GetOldObject()
		if err != nil {
			klog.FromContext(ctx).Error(err, "failed to decode old object")
			return errorResponse(err, &apierrors.NewBadRequest("failed to decode  old object").ErrStatus)
		}
		oldNamespace, ok := oldObj.(*corev1.Namespace)
		if !ok {
			klog.FromContext(ctx).Info("failed to assert old namespace type", "type", reflect.TypeOf(oldObj))
			return errorResponse(nil, &apierrors.NewBadRequest("failed to decode  old namespace").ErrStatus)
		}
		oldPolicy, oldErrs := a.PolicyToEvaluate(oldNamespace.Labels)

		// 仅当标签发生新变化时拒绝非法值；保持原有错误不阻断无关更新。
		if len(newErrs) > 0 && (len(oldErrs) == 0 || !reflect.DeepEqual(newErrs, oldErrs)) {
			return invalidResponse(attrs, newErrs)
		}

		// 以下情况跳过存量 Pod 预检：
		// * enforce 策略未变化；
		// * 新 enforce 策略为 privileged；
		// * 版本相同且等级放宽；
		// * 命名空间已豁免。
		if newPolicy.Enforce == oldPolicy.Enforce {
			return sharedAllowedResponse
		}
		if newPolicy.Enforce.Level == api.LevelPrivileged {
			return sharedAllowedResponse
		}
		if newPolicy.Enforce.Version == oldPolicy.Enforce.Version &&
			api.CompareLevels(newPolicy.Enforce.Level, oldPolicy.Enforce.Level) < 1 {
			return sharedAllowedResponse
		}
		if a.exemptNamespace(attrs.GetNamespace()) {
			if warning := a.exemptNamespaceWarning(namespace.Name, newPolicy, namespace.Labels); warning != "" {
				response := allowedResponse()
				response.Warnings = append(response.Warnings, warning)
				return response
			}
			return sharedAllowedResponse
		}
		response := allowedResponse()
		response.Warnings = a.EvaluatePodsInNamespace(ctx, namespace.Name, newPolicy.Enforce)
		return response

	default:
		return sharedAllowedResponse
	}
}

// ignoredPodSubresources 列出不执行 Pod 校验的子资源。
// 其余子资源按 v1.Pod 检查，使版本错配时对可能修改 PodSpec 的未知子资源采取拒绝策略。
var ignoredPodSubresources = map[string]bool{
	"exec":        true,
	"attach":      true,
	"binding":     true,
	"eviction":    true,
	"log":         true,
	"portforward": true,
	"proxy":       true,
	"status":      true,
}

// ValidatePod 按命名空间有效策略校验 Pod 创建或更新；返回响应可能被共享，调用方不得修改。
// 更新时仅策略相关字段变化才重新评估，避免无关更新被误拒。
func (a *Admission) ValidatePod(ctx context.Context, attrs api.Attributes) *admissionv1.AdmissionResponse {
	// 忽略的子资源不执行 Pod 校验。
	if ignoredPodSubresources[attrs.GetSubresource()] {
		return sharedAllowedResponse
	}
	// 命名空间或用户命中豁免时跳过策略检查。
	if a.exemptNamespace(attrs.GetNamespace()) {
		a.Metrics.RecordExemption(attrs)
		return sharedAllowedByNamespaceExemptionResponse
	}

	if a.exemptUser(attrs.GetUserName()) {
		a.Metrics.RecordExemption(attrs)
		return sharedAllowedByUserExemptionResponse
	}

	// 三种模式均为 privileged 时无需评估。
	namespace, err := a.NamespaceGetter.GetNamespace(ctx, attrs.GetNamespace())
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to fetch pod namespace", "namespace", attrs.GetNamespace())
		a.Metrics.RecordError(true, attrs)
		return errorResponse(err, &apierrors.NewInternalError(fmt.Errorf("failed to lookup namespace %q", attrs.GetNamespace())).ErrStatus)
	}
	nsPolicy, nsPolicyErrs := a.PolicyToEvaluate(namespace.Labels)
	if len(nsPolicyErrs) == 0 && nsPolicy.FullyPrivileged() {
		a.Metrics.RecordEvaluation(metrics.DecisionAllow, nsPolicy.Enforce, metrics.ModeEnforce, attrs)
		return sharedAllowedPrivilegedResponse
	}

	obj, err := attrs.GetObject()
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to decode object")
		a.Metrics.RecordError(true, attrs)
		return errorResponse(err, &apierrors.NewBadRequest("failed to decode object").ErrStatus)
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.FromContext(ctx).Info("failed to assert pod type", "type", reflect.TypeOf(obj))
		a.Metrics.RecordError(true, attrs)
		return errorResponse(nil, &apierrors.NewBadRequest("failed to decode pod").ErrStatus)
	}
	if attrs.GetOperation() == admissionv1.Update {
		oldObj, err := attrs.GetOldObject()
		if err != nil {
			klog.FromContext(ctx).Error(err, "failed to decode old object")
			a.Metrics.RecordError(true, attrs)
			return errorResponse(err, &apierrors.NewBadRequest("failed to decode old object").ErrStatus)
		}
		oldPod, ok := oldObj.(*corev1.Pod)
		if !ok {
			klog.FromContext(ctx).Info("failed to assert old pod type", "type", reflect.TypeOf(oldObj))
			a.Metrics.RecordError(true, attrs)
			return errorResponse(nil, &apierrors.NewBadRequest("failed to decode old pod").ErrStatus)
		}
		if !isSignificantPodUpdate(pod, oldPod) {
			// 策略相关字段未变化，直接放行更新。
			return sharedAllowedResponse
		}
	}
	return a.EvaluatePod(ctx, nsPolicy, nsPolicyErrs.ToAggregate(), &pod.ObjectMeta, &pod.Spec, attrs, true)
}

// ValidatePodController 按命名空间有效策略检查 Pod 控制器模板；返回响应可能被共享，调用方不得修改。
// 控制器请求只审计与警告、不强制拒绝，实际 Pod 创建时会再次强制执行检查。
func (a *Admission) ValidatePodController(ctx context.Context, attrs api.Attributes) *admissionv1.AdmissionResponse {
	// 子资源不承载待检查的对象，直接放行。
	if attrs.GetSubresource() != "" {
		return sharedAllowedResponse
	}
	// 命名空间或用户命中豁免时跳过策略检查。
	if a.exemptNamespace(attrs.GetNamespace()) {
		a.Metrics.RecordExemption(attrs)
		return sharedAllowedByNamespaceExemptionResponse
	}

	if a.exemptUser(attrs.GetUserName()) {
		a.Metrics.RecordExemption(attrs)
		return sharedAllowedByUserExemptionResponse
	}

	// audit 与 warn 均为 privileged 时无需评估模板。
	namespace, err := a.NamespaceGetter.GetNamespace(ctx, attrs.GetNamespace())
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to fetch pod namespace", "namespace", attrs.GetNamespace())
		a.Metrics.RecordError(true, attrs)
		response := allowedResponse()
		response.AuditAnnotations = map[string]string{
			"error": fmt.Sprintf("failed to lookup namespace %q: %v", attrs.GetNamespace(), err),
		}
		return response
	}
	nsPolicy, nsPolicyErrs := a.PolicyToEvaluate(namespace.Labels)
	if len(nsPolicyErrs) == 0 && nsPolicy.Warn.Level == api.LevelPrivileged && nsPolicy.Audit.Level == api.LevelPrivileged {
		return sharedAllowedResponse
	}

	obj, err := attrs.GetObject()
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to decode object")
		a.Metrics.RecordError(true, attrs)
		response := allowedResponse()
		response.AuditAnnotations = map[string]string{
			"error": fmt.Sprintf("failed to decode object: %v", err),
		}
		return response
	}
	podMetadata, podSpec, err := a.PodSpecExtractor.ExtractPodSpec(obj)
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to extract pod spec")
		a.Metrics.RecordError(true, attrs)
		response := allowedResponse()
		response.AuditAnnotations = map[string]string{
			"error": fmt.Sprintf("failed to extract pod template: %v", err),
		}
		return response
	}
	if podMetadata == nil && podSpec == nil {
		// 可选 PodSpec 的控制器未提供模板时跳过校验。
		return sharedAllowedResponse
	}
	return a.EvaluatePod(ctx, nsPolicy, nsPolicyErrs.ToAggregate(), podMetadata, podSpec, attrs, false)
}

// EvaluatePod 按给定策略评估 Pod 或类 Pod 对象；仅 enforce 为 true 时执行强制检查。
// 返回响应可能被共享，调用方不得修改。
// enforce、audit、warn 同等级同版本时复用缓存，避免重复检查。
func (a *Admission) EvaluatePod(ctx context.Context, nsPolicy api.Policy, nsPolicyErr error, podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec, attrs api.Attributes, enforce bool) *admissionv1.AdmissionResponse {
	logger := klog.FromContext(ctx)
	// RuntimeClass 命中豁免时跳过策略检查。
	if a.exemptRuntimeClass(podSpec.RuntimeClassName) {
		a.Metrics.RecordExemption(attrs)
		return sharedAllowedByRuntimeClassExemptionResponse
	}

	auditAnnotations := map[string]string{}
	if nsPolicyErr != nil {
		logger.V(2).Info("failed to parse PodSecurity namespace labels", "err", nsPolicyErr)
		auditAnnotations["error"] = fmt.Sprintf("Failed to parse policy: %v", nsPolicyErr)
		a.Metrics.RecordError(false, attrs)
	}

	if klogV := logger.V(5); klogV.Enabled() {
		klogV.Info("PodSecurity evaluation", "policy", fmt.Sprintf("%v", nsPolicy), "op", attrs.GetOperation(), "resource", attrs.GetResource(), "namespace", attrs.GetNamespace(), "name", attrs.GetName())
	}
	cachedResults := make(map[api.LevelVersion]policy.AggregateCheckResult)
	response := allowedResponse()
	if enforce {
		auditAnnotations[api.EnforcedPolicyAnnotationKey] = nsPolicy.Enforce.String()

		result := policy.AggregateCheckResults(a.Evaluator.EvaluatePod(nsPolicy.Enforce, podMetadata, podSpec))
		if !result.Allowed {
			response = forbiddenResponse(attrs, fmt.Errorf(
				"violates PodSecurity %q: %s",
				nsPolicy.Enforce.String(),
				result.ForbiddenDetail(),
			))
			a.Metrics.RecordEvaluation(metrics.DecisionDeny, nsPolicy.Enforce, metrics.ModeEnforce, attrs)
		} else {
			a.Metrics.RecordEvaluation(metrics.DecisionAllow, nsPolicy.Enforce, metrics.ModeEnforce, attrs)
		}
		cachedResults[nsPolicy.Enforce] = result
	}

	// audit 与 enforce 的等级和版本相同时复用强制检查结果。

	auditResult, ok := cachedResults[nsPolicy.Audit]
	if !ok {
		auditResult = policy.AggregateCheckResults(a.Evaluator.EvaluatePod(nsPolicy.Audit, podMetadata, podSpec))
		cachedResults[nsPolicy.Audit] = auditResult
	}
	if !auditResult.Allowed {
		auditAnnotations[api.AuditViolationsAnnotationKey] = fmt.Sprintf(
			"would violate PodSecurity %q: %s",
			nsPolicy.Audit.String(),
			auditResult.ForbiddenDetail(),
		)
		a.Metrics.RecordEvaluation(metrics.DecisionDeny, nsPolicy.Audit, metrics.ModeAudit, attrs)
	}

	// 请求已因错误拒绝时不再追加警告。
	if response.Allowed {
		// warn 与 audit 或 enforce 的等级和版本相同时复用已有结果。
		warnResult, ok := cachedResults[nsPolicy.Warn]
		if !ok {
			warnResult = policy.AggregateCheckResults(a.Evaluator.EvaluatePod(nsPolicy.Warn, podMetadata, podSpec))
		}
		if !warnResult.Allowed {
			// TODO: 改进面向用户的警告文案。
			response.Warnings = append(response.Warnings, fmt.Sprintf(
				"would violate PodSecurity %q: %s",
				nsPolicy.Warn.String(),
				warnResult.ForbiddenDetail(),
			))
			a.Metrics.RecordEvaluation(metrics.DecisionDeny, nsPolicy.Warn, metrics.ModeWarn, attrs)
		}
	}

	response.AuditAnnotations = auditAnnotations
	return response
}

// podCount 汇总相同警告涉及的 Pod；保留字典序最小的名字，确保输出稳定。
type podCount struct {
	// podName 是该警告对应的字典序最小 Pod 名。
	podName string

	// podCount 是命中该警告的 Pod 总数。
	podCount int
}

// EvaluatePodsInNamespace 在命名空间收紧 enforce 时预检存量 Pod，返回违规警告。
// 超时或触及上限时只检查部分 Pod，避免无关的命名空间更新被无限期阻塞。
func (a *Admission) EvaluatePodsInNamespace(ctx context.Context, namespace string, enforce api.LevelVersion) []string {
	// 先使用配置的超时预算。
	timeout := a.namespacePodCheckTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeRemaining := time.Until(deadline) / 2 // 最多占用请求剩余时间的一半。
		if timeout > timeRemaining {
			timeout = timeRemaining
		}
	}
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	pods, err := a.PodLister.ListPods(ctx, namespace)
	if err != nil {
		klog.FromContext(ctx).Error(err, "failed to list pods", "namespace", namespace)
		return []string{"failed to list pods while checking new PodSecurity enforce level"}
	}

	var (
		warnings []string

		podWarnings        []string
		podWarningsToCount = make(map[string]podCount)
		prioritizedPods    = a.prioritizePods(pods)
	)

	totalPods := len(prioritizedPods)
	if len(prioritizedPods) > a.namespaceMaxPodsToCheck {
		prioritizedPods = prioritizedPods[0:a.namespaceMaxPodsToCheck]
	}

	checkedPods := len(prioritizedPods)
	for i, pod := range prioritizedPods {
		r := policy.AggregateCheckResults(a.Evaluator.EvaluatePod(enforce, &pod.ObjectMeta, &pod.Spec))
		if !r.Allowed {
			warning := r.ForbiddenReason()
			c, seen := podWarningsToCount[warning]
			if !seen {
				c.podName = pod.Name
				podWarnings = append(podWarnings, warning)
			} else if pod.Name < c.podName {
				c.podName = pod.Name
			}
			c.podCount++
			podWarningsToCount[warning] = c
		}
		if err := ctx.Err(); err != nil { // 超时或上下文取消。
			checkedPods = i + 1
			break
		}
	}

	if checkedPods < totalPods {
		warnings = append(warnings, fmt.Sprintf("new PodSecurity enforce level only checked against the first %d of %d existing pods", checkedPods, totalPods))
	}

	if len(podWarnings) > 0 {
		warnings = append(warnings, fmt.Sprintf("existing pods in namespace %q violate the new PodSecurity enforce level %q", namespace, enforce.String()))
	}

	// 为警告补充 Pod 名。
	decoratePodWarnings(podWarningsToCount, podWarnings)
	// 固定警告顺序，便于比较与排查。
	sort.Strings(podWarnings)

	return append(warnings, podWarnings...)
}

// decoratePodWarnings 按受影响 Pod 数量补充名字与数量，便于定位违规来源。
func decoratePodWarnings(podWarningsToCount map[string]podCount, warnings []string) {
	for i, warning := range warnings {
		c := podWarningsToCount[warning]
		switch c.podCount {
		case 0:
			// 无对应计数时保留原警告。
		case 1:
			warnings[i] = fmt.Sprintf("%s: %s", c.podName, warning)
		case 2:
			warnings[i] = fmt.Sprintf("%s (and 1 other pod): %s", c.podName, warning)
		default:
			warnings[i] = fmt.Sprintf("%s (and %d other pods): %s", c.podName, c.podCount-1, warning)
		}
	}
}

// PolicyToEvaluate 用已解析的默认策略补齐命名空间标签，确保校验阶段与 Pod 预检看到同一份策略。
func (a *Admission) PolicyToEvaluate(labels map[string]string) (api.Policy, field.ErrorList) {
	return api.PolicyToEvaluate(labels, a.defaultPolicy)
}

// isSignificantPodUpdate 判断 Pod 更新是否需要重新执行策略评估。
// 截至 1.21，相关的可变字段包括镜像与注解：
// * https://github.com/kubernetes/kubernetes/blob/release-1.21/pkg/apis/core/validation/validation.go#L3947-L3949
func isSignificantPodUpdate(pod, oldPod *corev1.Pod) bool {
	// TODO: 改为仅允许明确的更新类型。
	if len(pod.Spec.Containers) != len(oldPod.Spec.Containers) {
		return true
	}
	if len(pod.Spec.InitContainers) != len(oldPod.Spec.InitContainers) {
		return true
	}
	for i := 0; i < len(pod.Spec.Containers); i++ {
		if isSignificantContainerUpdate(&pod.Spec.Containers[i], &oldPod.Spec.Containers[i]) {
			return true
		}
	}
	for i := 0; i < len(pod.Spec.InitContainers); i++ {
		if isSignificantContainerUpdate(&pod.Spec.InitContainers[i], &oldPod.Spec.InitContainers[i]) {
			return true
		}
	}
	for _, c := range pod.Spec.EphemeralContainers {
		var oldC *corev1.Container
		for i, oc := range oldPod.Spec.EphemeralContainers {
			if oc.Name == c.Name {
				oldC = (*corev1.Container)(&oldPod.Spec.EphemeralContainers[i].EphemeralContainerCommon)
				break
			}
		}
		if oldC == nil {
			return true // 新增临时容器。
		}
		if isSignificantContainerUpdate((*corev1.Container)(&c.EphemeralContainerCommon), oldC) {
			return true
		}
	}
	return false
}

// isSignificantContainerUpdate 判断容器更新是否需要重新执行策略评估。
func isSignificantContainerUpdate(container, oldContainer *corev1.Container) bool {
	return container.Image != oldContainer.Image
}

// 豁免名单通常很短，线性查找足够；新配置变更时可再考虑换成集合。
func (a *Admission) exemptNamespace(namespace string) bool {
	if len(namespace) == 0 {
		return false
	}
	// TODO: 考虑优化为 O(1) 查找。
	return containsString(namespace, a.Configuration.Exemptions.Namespaces)
}

// 与命名空间豁免独立判断，任一命中即跳过策略检查。
func (a *Admission) exemptUser(username string) bool {
	if len(username) == 0 {
		return false
	}
	// TODO: 考虑优化为 O(1) 查找。
	return containsString(username, a.Configuration.Exemptions.Usernames)
}

// 未指定 runtimeClass 时按默认运行时处理，不进入豁免名单匹配。
func (a *Admission) exemptRuntimeClass(runtimeClass *string) bool {
	if runtimeClass == nil || len(*runtimeClass) == 0 {
		return false
	}
	// TODO: 考虑优化为 O(1) 查找。
	return containsString(*runtimeClass, a.Configuration.Exemptions.RuntimeClasses)
}

// prioritizePods 过滤豁免 Pod，并优先排列不同控制器的 Pod；输入切片会原地修改，调用方不应复用。
// 同控制器副本先检查一份，可在有限预算内覆盖更多控制器。
func (a *Admission) prioritizePods(pods []*corev1.Pod) []*corev1.Pod {
	// 原地构造优先列表，避免二次分配。
	prioritizedPods := pods[:0]
	// 同一控制器的额外副本暂存，待不同控制器检查完后追加。
	var duplicateReplicatedPods []*corev1.Pod
	evaluatedControllers := make(map[types.UID]bool)
	for _, pod := range pods {
		// RuntimeClass 命中豁免时跳过策略检查。
		if a.exemptRuntimeClass(pod.Spec.RuntimeClassName) {
			continue
		}
		// 同一控制器已有代表 Pod 时，将副本放入延迟列表。
		podOwnerControllerRef := metav1.GetControllerOfNoCopy(pod)
		if podOwnerControllerRef == nil {
			prioritizedPods = append(prioritizedPods, pod)
			continue
		}
		if evaluatedControllers[podOwnerControllerRef.UID] {
			duplicateReplicatedPods = append(duplicateReplicatedPods, pod)
			continue
		}
		prioritizedPods = append(prioritizedPods, pod)
		evaluatedControllers[podOwnerControllerRef.UID] = true
	}
	return append(prioritizedPods, duplicateReplicatedPods...)
}

// 豁免名单共用这个小工具，避免为每个字段重复写循环。
func containsString(needle string, haystack []string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// exemptNamespaceWarning 在豁免命名空间显式设置非 privileged 策略时返回警告，提示该策略不会生效。
func (a *Admission) exemptNamespaceWarning(exemptNamespace string, policy api.Policy, nsLabels map[string]string) string {
	if policy.FullyPrivileged() || policy.Equivalent(&a.defaultPolicy) {
		return ""
	}

	// 仅输出显式设置的非 privileged 模式，保持警告简洁并避免泄露无关标签。
	sb := strings.Builder{}
	_, hasEnforceLevel := nsLabels[api.EnforceLevelLabel]
	_, hasEnforceVersion := nsLabels[api.EnforceVersionLabel]
	if policy.Enforce.Level != api.LevelPrivileged && (hasEnforceLevel || hasEnforceVersion) {
		sb.WriteString("enforce=")
		sb.WriteString(policy.Enforce.String())
	}
	_, hasAuditLevel := nsLabels[api.AuditLevelLabel]
	_, hasAuditVersion := nsLabels[api.AuditVersionLabel]
	if policy.Audit.Level != api.LevelPrivileged && (hasAuditLevel || hasAuditVersion) {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("audit=")
		sb.WriteString(policy.Audit.String())
	}
	_, hasWarnLevel := nsLabels[api.WarnLevelLabel]
	_, hasWarnVersion := nsLabels[api.WarnVersionLabel]
	if policy.Warn.Level != api.LevelPrivileged && (hasWarnLevel || hasWarnVersion) {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("warn=")
		sb.WriteString(policy.Warn.String())
	}

	return fmt.Sprintf("namespace %q is exempt from Pod Security, and the policy (%s) will be ignored",
		exemptNamespace, sb.String())
}
