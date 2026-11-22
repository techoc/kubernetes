/*
Copyright 2014 The Kubernetes Authors.

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

package limitranger

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitailizer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/lru"

	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
)

const (
	limitRangerAnnotation = "kubernetes.io/limit-ranger"
	// PluginName 是该准入插件的名称。
	PluginName = "LimitRanger"
)

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewLimitRanger(&DefaultLimitRangerActions{})
	})
}

// LimitRanger 按命名空间对资源用量实施 LimitRange 限制。
type LimitRanger struct {
	*admission.Handler
	client  kubernetes.Interface
	actions LimitRangerActions
	lister  corev1listers.LimitRangeLister

	// liveLookupCache 缓存近期的实时查询结果，摊薄重复查询失败的开销。
	// 缓存未命中或无结果时回查存储，可应对缓存滞后；
	// 记录结果后，相同命名空间的重复请求就不必频繁回查。
	liveLookupCache *lru.Cache
	group           singleflight.Group
	liveTTL         time.Duration
}

var _ admission.MutationInterface = &LimitRanger{}
var _ admission.ValidationInterface = &LimitRanger{}

var _ genericadmissioninitailizer.WantsExternalKubeInformerFactory = &LimitRanger{}
var _ genericadmissioninitailizer.WantsExternalKubeClientSet = &LimitRanger{}

// liveLookupEntry 是一次实时查询的结果及其过期时间。
type liveLookupEntry struct {
	expiry time.Time
	items  []*corev1.LimitRange
}

// SetExternalKubeInformerFactory 注册 informer factory，并等待 LimitRange 缓存同步完成。
func (l *LimitRanger) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	limitRangeInformer := f.Core().V1().LimitRanges()
	l.SetReadyFunc(limitRangeInformer.Informer().HasSynced)
	l.lister = limitRangeInformer.Lister()
}

// SetExternalKubeClientSet 注入 Kubernetes 客户端，供缓存未命中时实时查询。
func (l *LimitRanger) SetExternalKubeClientSet(client kubernetes.Interface) {
	l.client = client
}

// ValidateInitialization 确认 lister 与客户端均已注入。
func (l *LimitRanger) ValidateInitialization() error {
	if l.lister == nil {
		return fmt.Errorf("missing limitRange lister")
	}
	if l.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}

// Admit 对资源应用 LimitRange 中定义的默认值修改。
func (l *LimitRanger) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return l.runLimitFunc(a, l.actions.MutateLimit)
}

// Validate 校验资源是否满足命名空间中所有适用的 LimitRange 约束。
func (l *LimitRanger) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return l.runLimitFunc(a, l.actions.ValidateLimit)
}

// runLimitFunc 对请求涉及的每个适用 LimitRange 执行给定的修改或校验函数。
func (l *LimitRanger) runLimitFunc(a admission.Attributes, limitFn func(limitRange *corev1.LimitRange, kind string, obj runtime.Object) error) (err error) {
	if !l.actions.SupportsAttributes(a) {
		return nil
	}

	// 正在删除的对象无需再限制，直接放行。
	oldObj := a.GetOldObject()
	if oldObj != nil {
		oldAccessor, err := meta.Accessor(oldObj)
		if err != nil {
			return admission.NewForbidden(a, err)
		}
		if oldAccessor.GetDeletionTimestamp() != nil {
			return nil
		}
	}

	items, err := l.GetLimitRanges(a)
	if err != nil {
		return err
	}

	// 确保满足每条 LimitRange 规定的最小值/最大值。
	for i := range items {
		limitRange := items[i]

		if !l.actions.SupportsLimit(limitRange) {
			continue
		}

		err = limitFn(limitRange, a.GetResource().Resource, a.GetObject())
		if err != nil {
			return admission.NewForbidden(a, err)
		}
	}
	return nil
}

// GetLimitRanges 优先返回索引缓存中的 LimitRange；缓存为空时实时查询并写入短期缓存。
func (l *LimitRanger) GetLimitRanges(a admission.Attributes) ([]*corev1.LimitRange, error) {
	items, err := l.lister.LimitRanges(a.GetNamespace()).List(labels.Everything())
	if err != nil {
		return nil, admission.NewForbidden(a, fmt.Errorf("unable to %s %v at this time because there was an error enforcing limit ranges", a.GetOperation(), a.GetResource()))
	}

	// 索引缓存为空时先查实时查询 LRU，未命中则回查存储并预热缓存。
	if len(items) == 0 {
		lruItemObj, ok := l.liveLookupCache.Get(a.GetNamespace())
		if !ok || lruItemObj.(liveLookupEntry).expiry.Before(time.Now()) {
			// Fixed: #22422
			// 使用 singleflight 合并同一命名空间的并发查询，避免缓存击穿。
			lruItemObj, err, _ = l.group.Do(a.GetNamespace(), func() (interface{}, error) {
				liveList, err := l.client.CoreV1().LimitRanges(a.GetNamespace()).List(context.TODO(), metav1.ListOptions{})
				if err != nil {
					return nil, admission.NewForbidden(a, err)
				}
				newEntry := liveLookupEntry{expiry: time.Now().Add(l.liveTTL)}
				for i := range liveList.Items {
					newEntry.items = append(newEntry.items, &liveList.Items[i])
				}
				l.liveLookupCache.Add(a.GetNamespace(), newEntry)
				return newEntry, nil
			})
			if err != nil {
				return nil, err
			}
		}
		lruEntry := lruItemObj.(liveLookupEntry)

		items = append(items, lruEntry.items...)

	}

	return items, nil
}

// NewLimitRanger 创建使用给定 actions 执行限制检查的 LimitRanger。
func NewLimitRanger(actions LimitRangerActions) (*LimitRanger, error) {
	liveLookupCache := lru.New(10000)

	if actions == nil {
		actions = &DefaultLimitRangerActions{}
	}

	return &LimitRanger{
		Handler:         admission.NewHandler(admission.Create, admission.Update),
		actions:         actions,
		liveLookupCache: liveLookupCache,
		liveTTL:         time.Duration(30 * time.Second),
	}, nil
}

// defaultContainerResourceRequirements 从 LimitRange 中提取容器的默认资源需求。
// Limits 取 LimitRange 的 default，Requests 取 defaultRequest。
func defaultContainerResourceRequirements(limitRange *corev1.LimitRange) api.ResourceRequirements {
	requirements := api.ResourceRequirements{}
	requirements.Requests = api.ResourceList{}
	requirements.Limits = api.ResourceList{}

	for i := range limitRange.Spec.Limits {
		limit := limitRange.Spec.Limits[i]
		if limit.Type == corev1.LimitTypeContainer {
			for k, v := range limit.DefaultRequest {
				requirements.Requests[api.ResourceName(k)] = v.DeepCopy()
			}
			for k, v := range limit.Default {
				requirements.Limits[api.ResourceName(k)] = v.DeepCopy()
			}
		}
	}
	return requirements
}

// mergeContainerResources 用默认值补全容器未显式设置的资源，并记录被设置的资源用于注解。
func mergeContainerResources(container *api.Container, defaultRequirements *api.ResourceRequirements, annotationPrefix string, annotations []string) []string {
	setRequests := []string{}
	setLimits := []string{}
	if container.Resources.Limits == nil {
		container.Resources.Limits = api.ResourceList{}
	}
	if container.Resources.Requests == nil {
		container.Resources.Requests = api.ResourceList{}
	}
	for k, v := range defaultRequirements.Limits {
		_, found := container.Resources.Limits[k]
		if !found {
			container.Resources.Limits[k] = v.DeepCopy()
			setLimits = append(setLimits, string(k))
		}
	}
	for k, v := range defaultRequirements.Requests {
		_, found := container.Resources.Requests[k]
		if !found {
			container.Resources.Requests[k] = v.DeepCopy()
			setRequests = append(setRequests, string(k))
		}
	}
	if len(setRequests) > 0 {
		sort.Strings(setRequests)
		a := strings.Join(setRequests, ", ") + fmt.Sprintf(" request for %s %s", annotationPrefix, container.Name)
		annotations = append(annotations, a)
	}
	if len(setLimits) > 0 {
		sort.Strings(setLimits)
		a := strings.Join(setLimits, ", ") + fmt.Sprintf(" limit for %s %s", annotationPrefix, container.Name)
		annotations = append(annotations, a)
	}
	return annotations
}

// mergePodResourceRequirements 对 Pod 的所有容器（含 init 容器）应用默认资源，
// 并在 Pod 上注解哪些资源被自动设置。
func mergePodResourceRequirements(pod *api.Pod, defaultRequirements *api.ResourceRequirements) {
	annotations := []string{}

	for i := range pod.Spec.Containers {
		annotations = mergeContainerResources(&pod.Spec.Containers[i], defaultRequirements, "container", annotations)
	}

	for i := range pod.Spec.InitContainers {
		annotations = mergeContainerResources(&pod.Spec.InitContainers[i], defaultRequirements, "init container", annotations)
	}

	if len(annotations) > 0 {
		if pod.ObjectMeta.Annotations == nil {
			pod.ObjectMeta.Annotations = make(map[string]string)
		}
		val := "LimitRanger plugin set: " + strings.Join(annotations, "; ")
		pod.ObjectMeta.Annotations[limitRangerAnnotation] = val
	}
}

// requestLimitEnforcedValues 将请求值、限制值和约束值统一到相同精度，便于比较。
// 数值不溢出时优先使用毫级精度以减少舍入误差。
func requestLimitEnforcedValues(requestQuantity, limitQuantity, enforcedQuantity resource.Quantity) (request, limit, enforced int64) {
	request = requestQuantity.Value()
	limit = limitQuantity.Value()
	enforced = enforcedQuantity.Value()
	// 数值不溢出时使用毫级精度做更精确的比较。
	if request <= resource.MaxMilliValue && limit <= resource.MaxMilliValue && enforced <= resource.MaxMilliValue {
		request = requestQuantity.MilliValue()
		limit = limitQuantity.MilliValue()
		enforced = enforcedQuantity.MilliValue()
	}
	return
}

// minConstraint 校验指定资源的 request（以及存在的 limit）不低于最小值。
func minConstraint(limitType string, resourceName string, enforced resource.Quantity, request api.ResourceList, limit api.ResourceList) error {
	req, reqExists := request[api.ResourceName(resourceName)]
	lim, limExists := limit[api.ResourceName(resourceName)]
	observedReqValue, observedLimValue, enforcedValue := requestLimitEnforcedValues(req, lim, enforced)

	if !reqExists {
		return fmt.Errorf("minimum %s usage per %s is %s.  No request is specified", resourceName, limitType, enforced.String())
	}
	if observedReqValue < enforcedValue {
		return fmt.Errorf("minimum %s usage per %s is %s, but request is %s", resourceName, limitType, enforced.String(), req.String())
	}
	if limExists && (observedLimValue < enforcedValue) {
		return fmt.Errorf("minimum %s usage per %s is %s, but limit is %s", resourceName, limitType, enforced.String(), lim.String())
	}
	return nil
}

// maxRequestConstraint 校验指定资源的 request 不超过最大值。
// 当 LimitType 不识别 limit 值（如 PVC）时使用。
func maxRequestConstraint(limitType string, resourceName string, enforced resource.Quantity, request api.ResourceList) error {
	req, reqExists := request[api.ResourceName(resourceName)]
	observedReqValue, _, enforcedValue := requestLimitEnforcedValues(req, resource.Quantity{}, enforced)

	if !reqExists {
		return fmt.Errorf("maximum %s usage per %s is %s.  No request is specified", resourceName, limitType, enforced.String())
	}
	if observedReqValue > enforcedValue {
		return fmt.Errorf("maximum %s usage per %s is %s, but request is %s", resourceName, limitType, enforced.String(), req.String())
	}
	return nil
}

// maxConstraint 校验指定资源的 limit（以及存在的 request）不超过最大值。
func maxConstraint(limitType string, resourceName string, enforced resource.Quantity, request api.ResourceList, limit api.ResourceList) error {
	req, reqExists := request[api.ResourceName(resourceName)]
	lim, limExists := limit[api.ResourceName(resourceName)]
	observedReqValue, observedLimValue, enforcedValue := requestLimitEnforcedValues(req, lim, enforced)

	if !limExists {
		return fmt.Errorf("maximum %s usage per %s is %s.  No limit is specified", resourceName, limitType, enforced.String())
	}
	if observedLimValue > enforcedValue {
		return fmt.Errorf("maximum %s usage per %s is %s, but limit is %s", resourceName, limitType, enforced.String(), lim.String())
	}
	if reqExists && (observedReqValue > enforcedValue) {
		return fmt.Errorf("maximum %s usage per %s is %s, but request is %s", resourceName, limitType, enforced.String(), req.String())
	}
	return nil
}

// limitRequestRatioConstraint 校验指定资源的 limit/request 比值不超过上限。
func limitRequestRatioConstraint(limitType string, resourceName string, enforced resource.Quantity, request api.ResourceList, limit api.ResourceList) error {
	req, reqExists := request[api.ResourceName(resourceName)]
	lim, limExists := limit[api.ResourceName(resourceName)]
	observedReqValue, observedLimValue, _ := requestLimitEnforcedValues(req, lim, enforced)

	if !reqExists || (observedReqValue == int64(0)) {
		return fmt.Errorf("%s max limit to request ratio per %s is %s, but no request is specified or request is 0", resourceName, limitType, enforced.String())
	}
	if !limExists || (observedLimValue == int64(0)) {
		return fmt.Errorf("%s max limit to request ratio per %s is %s, but no limit is specified or limit is 0", resourceName, limitType, enforced.String())
	}

	observedRatio := float64(observedLimValue) / float64(observedReqValue)
	displayObservedRatio := observedRatio
	maxLimitRequestRatio := float64(enforced.Value())
	if enforced.Value() <= resource.MaxMilliValue {
		observedRatio = observedRatio * 1000
		maxLimitRequestRatio = float64(enforced.MilliValue())
	}

	if observedRatio > maxLimitRequestRatio {
		return fmt.Errorf("%s max limit to request ratio per %s is %s, but provided ratio is %f", resourceName, limitType, enforced.String(), displayObservedRatio)
	}

	return nil
}

// DefaultLimitRangerActions 是 LimitRangerActions 的默认实现。
type DefaultLimitRangerActions struct{}

// 确保 DefaultLimitRangerActions 实现 LimitRangerActions 接口。
var _ LimitRangerActions = &DefaultLimitRangerActions{}

// MutateLimit 按 LimitRange 的约束处理新对象的资源需求；
// 对未指定的资源应用 LimitRange 中列举的默认值。
func (d *DefaultLimitRangerActions) MutateLimit(limitRange *corev1.LimitRange, resourceName string, obj runtime.Object) error {
	switch resourceName {
	case "pods":
		return PodMutateLimitFunc(limitRange, obj.(*api.Pod))
	}
	return nil
}

// ValidateLimit 校验新对象的资源需求是否满足 LimitRange 中列举的约束。
func (d *DefaultLimitRangerActions) ValidateLimit(limitRange *corev1.LimitRange, resourceName string, obj runtime.Object) error {
	switch resourceName {
	case "pods":
		return PodValidateLimitFunc(limitRange, obj.(*api.Pod))
	case "persistentvolumeclaims":
		return PersistentVolumeClaimValidateLimitFunc(limitRange, obj.(*api.PersistentVolumeClaim))
	}
	return nil
}

// SupportsAttributes 只接受 Pod 与 PVC 请求（含 Pod 的 resize 子资源），
// 忽略其他资源和子资源。
func (d *DefaultLimitRangerActions) SupportsAttributes(a admission.Attributes) bool {
	// Pod 原地使用 resize 子资源修改容器资源，需要纳入限制检查。
	if a.GetSubresource() == "resize" && a.GetKind().GroupKind() == api.Kind("Pod") && a.GetOperation() == admission.Update {
		return true
	}

	// 其他子资源一律不支持。
	if a.GetSubresource() != "" {
		return false
	}

	// 容器的 resources 字段创建后不可变，因此 Pod 更新时无需再做 limitrange 修改与校验；
	// 强行修改也只会在 Pod 校验阶段因不可变字段而失败。
	if a.GetKind().GroupKind() == api.Kind("Pod") && a.GetOperation() == admission.Update {
		return false
	}

	return a.GetKind().GroupKind() == api.Kind("Pod") || a.GetKind().GroupKind() == api.Kind("PersistentVolumeClaim")
}

// SupportsLimit 对默认实现始终返回 true，即不跳过任何 LimitRange。
func (d *DefaultLimitRangerActions) SupportsLimit(limitRange *corev1.LimitRange) bool {
	return true
}

// PersistentVolumeClaimValidateLimitFunc 校验 PVC 的存储请求是否满足 LimitRange 限制。
// 存储量由用户通过 pvc.Spec.Resources.Requests 指定，管理员通过 LimitRange 设置上下限。
// 由于存储是 PVC 的必填项，不会为其设置默认值；所有限制仅作用于 Requests。
func PersistentVolumeClaimValidateLimitFunc(limitRange *corev1.LimitRange, pvc *api.PersistentVolumeClaim) error {
	var errs []error
	for i := range limitRange.Spec.Limits {
		limit := limitRange.Spec.Limits[i]
		limitType := limit.Type
		if limitType == corev1.LimitTypePersistentVolumeClaim {
			for k, v := range limit.Min {
				// PVC 的 Limits 不是用户输入，按空列表传入。
				if err := minConstraint(string(limitType), string(k), v, pvc.Spec.Resources.Requests, api.ResourceList{}); err != nil {
					errs = append(errs, err)
				}
			}
			for k, v := range limit.Max {
				// 最大值应针对用户的实际请求进行校验。
				if err := maxRequestConstraint(string(limitType), string(k), v, pvc.Spec.Resources.Requests); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

// PodMutateLimitFunc 按 LimitRange 为 Pod 补全默认资源配置。
func PodMutateLimitFunc(limitRange *corev1.LimitRange, pod *api.Pod) error {
	defaultResources := defaultContainerResourceRequirements(limitRange)
	mergePodResourceRequirements(pod, &defaultResources)
	return nil
}

// PodValidateLimitFunc 校验 Pod 的资源需求是否满足 LimitRange 的 min/max/比例约束。
func PodValidateLimitFunc(limitRange *corev1.LimitRange, pod *api.Pod) error {
	var errs []error

	for i := range limitRange.Spec.Limits {
		limit := limitRange.Spec.Limits[i]
		limitType := limit.Type
		// 校验普通容器和 init 容器的资源限制。
		if limitType == corev1.LimitTypeContainer {
			for j := range pod.Spec.Containers {
				container := &pod.Spec.Containers[j]
				for k, v := range limit.Min {
					if err := minConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
				for k, v := range limit.Max {
					if err := maxConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
				for k, v := range limit.MaxLimitRequestRatio {
					if err := limitRequestRatioConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
			}
			for j := range pod.Spec.InitContainers {
				container := &pod.Spec.InitContainers[j]
				for k, v := range limit.Min {
					if err := minConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
				for k, v := range limit.Max {
					if err := maxConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
				for k, v := range limit.MaxLimitRequestRatio {
					if err := limitRequestRatioConstraint(string(limitType), string(k), v, container.Resources.Requests, container.Resources.Limits); err != nil {
						errs = append(errs, err)
					}
				}
			}
		}

		// Pod 级限制同样作用于 init 容器。
		if limitType == corev1.LimitTypePod {
			opts := podResourcesOptions{
				PodLevelResourcesEnabled: feature.DefaultFeatureGate.Enabled(features.PodLevelResources),
			}
			podRequests := podRequests(pod, opts)
			podLimits := podLimits(pod, opts)
			for k, v := range limit.Min {
				if err := minConstraint(string(limitType), string(k), v, podRequests, podLimits); err != nil {
					errs = append(errs, err)
				}
			}
			for k, v := range limit.Max {
				if err := maxConstraint(string(limitType), string(k), v, podRequests, podLimits); err != nil {
					errs = append(errs, err)
				}
			}
			for k, v := range limit.MaxLimitRequestRatio {
				if err := limitRequestRatioConstraint(string(limitType), string(k), v, podRequests, podLimits); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

// podResourcesOptions 控制 Pod 级资源的计算方式。
type podResourcesOptions struct {
	// PodLevelResourcesEnabled 表示 PodLevelResources 特性门控已启用。
	PodLevelResourcesEnabled bool
}

// podRequests 是 pkg/api/v1/resource/PodRequests 针对 core 版本 Pod 的简化实现；
// 上游计算方式若有变更，这里需同步。
// NOTE: We do not want to check status resources here, only the spec. This is equivalent to setting
// UseStatusResources=false in the common helper.
// TODO: Maybe we can consider doing a partial conversion of the pod to a v1
// type and then using the pkg/api/v1/resource/PodRequests.
// TODO(ndixita): PodRequests method exists in
// staging/src/k8s.io/component-helpers/resource/helpers.go. Refactor the code to
// avoid duplicating podRequests method.
func podRequests(pod *api.Pod, opts podResourcesOptions) api.ResourceList {
	reqs := api.ResourceList{}

	for _, container := range pod.Spec.Containers {
		containerReqs := container.Resources.Requests
		addResourceList(reqs, containerReqs)
	}

	restartableInitCotnainerReqs := api.ResourceList{}
	initContainerReqs := api.ResourceList{}
	// init 容器决定资源需求的下界。
	// Note: In-place resize is not allowed for InitContainers, so no need to check for ResizeStatus value
	for _, container := range pod.Spec.InitContainers {
		containerReqs := container.Resources.Requests

		if container.RestartPolicy != nil && *container.RestartPolicy == api.ContainerRestartPolicyAlways {
			// sidecar 会一直运行，其请求计入 Pod 总需求。
			addResourceList(reqs, containerReqs)

			// 累计所有可重启 init 容器的资源。
			addResourceList(restartableInitCotnainerReqs, containerReqs)
			containerReqs = restartableInitCotnainerReqs
		} else {
			tmp := api.ResourceList{}
			addResourceList(tmp, containerReqs)
			addResourceList(tmp, restartableInitCotnainerReqs)
			containerReqs = tmp
		}

		maxResourceList(initContainerReqs, containerReqs)
	}

	maxResourceList(reqs, initContainerReqs)

	// PodLevelResources 启用后，Pod 级声明的资源优先于容器请求聚合值。
	if opts.PodLevelResourcesEnabled && pod.Spec.Resources != nil {
		for resourceName, quantity := range pod.Spec.Resources.Requests {
			if isSupportedPodLevelResource(resourceName) {
				// 用 Pod 级请求覆盖聚合值。
				reqs[resourceName] = quantity
			}
		}
	}

	return reqs
}

// podLimits 是 pkg/api/v1/resource/PodLimits 针对 core 版本 Pod 的简化实现；
// 上游计算方式若有变更，这里需同步。
// NOTE: We do not want to check status resources here, only the spec. This is equivalent to setting
// UseStatusResources=false in the common helper.
// TODO: Maybe we can consider doing a partial conversion of the pod to a v1
// type and then using the pkg/api/v1/resource/PodLimits.
// TODO(ndixita): PodLimits method exists in
// staging/src/k8s.io/component-helpers/resource/helpers.go. Refactor the code to
// avoid duplicating podLimits method.
func podLimits(pod *api.Pod, opts podResourcesOptions) api.ResourceList {
	limits := api.ResourceList{}

	for _, container := range pod.Spec.Containers {
		addResourceList(limits, container.Resources.Limits)
	}

	restartableInitContainerLimits := api.ResourceList{}
	initContainerLimits := api.ResourceList{}
	// init 容器决定资源限制的下界。
	for _, container := range pod.Spec.InitContainers {
		containerLimits := container.Resources.Limits
		// 判断 init 容器是否为 sidecar。
		if container.RestartPolicy != nil && *container.RestartPolicy == api.ContainerRestartPolicyAlways {
			addResourceList(limits, containerLimits)

			// 累计所有可重启 init 容器的资源。
			addResourceList(restartableInitContainerLimits, containerLimits)
			containerLimits = restartableInitContainerLimits
		} else {
			tmp := api.ResourceList{}
			addResourceList(tmp, containerLimits)
			addResourceList(tmp, restartableInitContainerLimits)
			containerLimits = tmp
		}
		maxResourceList(initContainerLimits, containerLimits)
	}

	maxResourceList(limits, initContainerLimits)

	// PodLevelResources 启用后，Pod 级声明的资源优先于容器限制聚合值。
	if opts.PodLevelResourcesEnabled && pod.Spec.Resources != nil {
		for resourceName, quantity := range pod.Spec.Resources.Limits {
			if isSupportedPodLevelResource(resourceName) {
				// 用 Pod 级限制覆盖聚合值。
				limits[resourceName] = quantity
			}
		}
	}

	return limits
}

var supportedPodLevelResources = sets.New(api.ResourceCPU, api.ResourceMemory)

// isSupportedPodLevelResource 判断指定资源是否受 PodLevelResources 特性支持。
// isSupportedPodLevelResource method exists in
// staging/src/k8s.io/component-helpers/resource/helpers.go.
// isSupportedPodLevelResource is added here to avoid conversion of v1.
// Pod to api.Pod.
// TODO(ndixita): Find alternatives to avoid duplicating the code.
func isSupportedPodLevelResource(name api.ResourceName) bool {
	return supportedPodLevelResources.Has(name)
}

// addResourceList 将 newList 中的资源累加到 list 中。
func addResourceList(list, newList api.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

// maxResourceList 对 newList 中的每种资源取 list 与新值的较大者。
func maxResourceList(list, newList api.ResourceList) {
	for name, quantity := range newList {
		if value, ok := list[name]; !ok || quantity.Cmp(value) > 0 {
			list[name] = quantity.DeepCopy()
		}
	}
}
