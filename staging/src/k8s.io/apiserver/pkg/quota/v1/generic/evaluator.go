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

package generic

import (
	"fmt"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// InformerForResourceFunc 知道如何为指定资源创建 informer。
type InformerForResourceFunc func(schema.GroupVersionResource) (informers.GenericInformer, error)

// ListerFuncForResourceFunc 根据 informer 创建函数生成资源 lister。
// informer 同步前，lister 会返回错误。
//
// 在缓存同步前返回错误而非空列表，防止配额计算把“尚未知”误判为“使用量为 0”。
func ListerFuncForResourceFunc(f InformerForResourceFunc) quota.ListerForResourceFunc {
	return func(gvr schema.GroupVersionResource) (cache.GenericLister, error) {
		informer, err := f(gvr)
		if err != nil {
			return nil, err
		}
		return &protectedLister{
			hasSynced:   cachedHasSynced(informer.Informer().HasSynced),
			notReadyErr: fmt.Errorf("%v not yet synced", gvr),
			delegate:    informer.Lister(),
		}, nil
	}
}

// cachedHasSynced 返回一个函数：调用 hasSynced，首次得到 true 后便永久返回 true。
// 缓存结果是因为 HasSynced 可能涉及锁；一旦为 true 便不可逆，可安全记忆化。
func cachedHasSynced(hasSynced func() bool) func() bool {
	cache := &atomic.Bool{}
	cache.Store(false)
	return func() bool {
		if cache.Load() {
			// 已同步则直接短路。
			return true
		}
		if hasSynced() {
			// 记住已完成同步。
			cache.Store(true)
			return true
		}
		return false
	}
}

// protectedLister 在未同步时返回 notReadyError，否则委托给 delegate。
type protectedLister struct {
	hasSynced   func() bool
	notReadyErr error
	delegate    cache.GenericLister
}

// List 在缓存未同步时返回 notReadyErr。
func (p *protectedLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	if !p.hasSynced() {
		return nil, p.notReadyErr
	}
	return p.delegate.List(selector)
}

// Get 在缓存未同步时返回 notReadyErr。
func (p *protectedLister) Get(name string) (runtime.Object, error) {
	if !p.hasSynced() {
		return nil, p.notReadyErr
	}
	return p.delegate.Get(name)
}

// ByNamespace 返回同样受同步状态保护的命名空间视图。
func (p *protectedLister) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return &protectedNamespaceLister{p.hasSynced, p.notReadyErr, p.delegate.ByNamespace(namespace)}
}

// protectedNamespaceLister 在未同步时返回 notReadyError，否则委托给 delegate。
type protectedNamespaceLister struct {
	hasSynced   func() bool
	notReadyErr error
	delegate    cache.GenericNamespaceLister
}

// List 在缓存未同步时返回 notReadyErr。
func (p *protectedNamespaceLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	if !p.hasSynced() {
		return nil, p.notReadyErr
	}
	return p.delegate.List(selector)
}

// Get 在缓存未同步时返回 notReadyErr。
func (p *protectedNamespaceLister) Get(name string) (runtime.Object, error) {
	if !p.hasSynced() {
		return nil, p.notReadyErr
	}
	return p.delegate.Get(name)
}

// ListResourceUsingListerFunc 基于共享 informer 工厂，为指定资源创建列表函数。
func ListResourceUsingListerFunc(l quota.ListerForResourceFunc, resource schema.GroupVersionResource) ListFuncByNamespace {
	return func(namespace string) ([]runtime.Object, error) {
		lister, err := l(resource)
		if err != nil {
			return nil, err
		}
		return lister.ByNamespace(namespace).List(labels.Everything())
	}
}

// ObjectCountQuotaResourceNameFor 返回指定组资源的对象计数配额名称。
//
// 核心组使用 count/<resource>，其他组使用 count/<resource>.<group>，
// 与历史配额名称保持兼容。
func ObjectCountQuotaResourceNameFor(groupResource schema.GroupResource) corev1.ResourceName {
	if len(groupResource.Group) == 0 {
		return corev1.ResourceName("count/" + groupResource.Resource)
	}
	return corev1.ResourceName("count/" + groupResource.Resource + "." + groupResource.Group)
}

// ListFuncByNamespace 知道如何列出指定命名空间中的资源。
type ListFuncByNamespace func(namespace string) ([]runtime.Object, error)

// MatchesScopeFunc 知道如何判断对象是否匹配某个作用域。
type MatchesScopeFunc func(scope corev1.ScopedResourceSelectorRequirement, object runtime.Object) (bool, error)

// UsageFunc 知道如何计量对象关联的资源用量。
type UsageFunc func(object runtime.Object) (corev1.ResourceList, error)

// MatchingResourceNamesFunc 返回输入资源名列表中匹配的资源名。
type MatchingResourceNamesFunc func(input []corev1.ResourceName) []corev1.ResourceName

// MatchesNoScopeFunc 在所有匹配检查中均返回 false。
// 用于不区分作用域的资源（如对象计数），表示没有任何 scope 命中。
func MatchesNoScopeFunc(scope corev1.ScopedResourceSelectorRequirement, object runtime.Object) (bool, error) {
	return false, nil
}

// Matches 在配额匹配指定对象时返回 true。
//
// 匹配需同时满足：资源名有交集，且所有 scope 选择器均命中（多个 scope 之间是 AND）。
func Matches(
	resourceQuota *corev1.ResourceQuota, item runtime.Object,
	matchFunc MatchingResourceNamesFunc, scopeFunc MatchesScopeFunc) (bool, error) {
	if resourceQuota == nil {
		return false, fmt.Errorf("expected non-nil quota")
	}
	// 确认至少有一种资源与配额匹配。
	matchResource := len(matchFunc(quota.ResourceNames(resourceQuota.Status.Hard))) > 0
	// 默认没有作用域限制，视为全部匹配。
	matchScope := true
	for _, scope := range getScopeSelectorsFromQuota(resourceQuota) {
		innerMatch, err := scopeFunc(scope, item)
		if err != nil {
			return false, err
		}
		matchScope = matchScope && innerMatch
	}
	return matchResource && matchScope, nil
}

// getScopeSelectorsFromQuota 把旧式 Scopes 与新的 ScopeSelector 统一为选择器列表；
// 旧式 scope 等价于 Exists 操作符。
func getScopeSelectorsFromQuota(quota *corev1.ResourceQuota) []corev1.ScopedResourceSelectorRequirement {
	selectors := []corev1.ScopedResourceSelectorRequirement{}
	for _, scope := range quota.Spec.Scopes {
		selectors = append(selectors, corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists})
	}
	if quota.Spec.ScopeSelector != nil {
		selectors = append(selectors, quota.Spec.ScopeSelector.MatchExpressions...)
	}
	return selectors
}

// CalculateUsageStats 是计算汇总用量的工具函数。
//
// 注意：scopeFunc 返回错误时当前实现直接返回部分结果且不返回错误
// （保留既有行为以兼容调用方），调用方需自行判断结果完整性。
func CalculateUsageStats(options quota.UsageStatsOptions,
	listFunc ListFuncByNamespace,
	scopeFunc MatchesScopeFunc,
	usageFunc UsageFunc) (quota.UsageStats, error) {
	// 默认将每个受跟踪资源的用量设为零。
	result := quota.UsageStats{Used: corev1.ResourceList{}}
	for _, resourceName := range options.Resources {
		result.Used[resourceName] = resource.Quantity{Format: resource.DecimalSI}
	}
	items, err := listFunc(options.Namespace)
	if err != nil {
		return result, fmt.Errorf("failed to list content: %v", err)
	}
	for _, item := range items {
		// 确认对象匹配全部作用域。
		matchesScopes := true
		for _, scope := range options.Scopes {
			innerMatch, err := scopeFunc(corev1.ScopedResourceSelectorRequirement{ScopeName: scope, Operator: corev1.ScopeSelectorOpExists}, item)
			if err != nil {
				return result, nil
			}
			if !innerMatch {
				matchesScopes = false
			}
		}
		if options.ScopeSelector != nil {
			for _, selector := range options.ScopeSelector.MatchExpressions {
				innerMatch, err := scopeFunc(selector, item)
				if err != nil {
					return result, nil
				}
				matchesScopes = matchesScopes && innerMatch
			}
		}
		// 仅在匹配时计入用量。
		if matchesScopes {
			usage, err := usageFunc(item)
			if err != nil {
				return result, err
			}
			result.Used = quota.Add(result.Used, usage)
		}
	}
	return result, nil
}

// objectCountEvaluator 实现 quota.Evaluator，按指定列表函数返回的对象数量计量用量。
//
// 按对象数量计量配额，只关心 Create 且非子资源的请求。
type objectCountEvaluator struct {
	// 此评估器跟踪的组资源，用于构造通用对象计数配额名称。
	groupResource schema.GroupResource
	// 按命名空间列出资源的函数。
	// TODO：未来改为使用动态客户端。
	listFuncByNamespace ListFuncByNamespace
	// 通用计数在配额中关联的资源名称。
	resourceNames []corev1.ResourceName
}

// Constraints 在配置的资源名不在必需集合中时返回错误。
func (o *objectCountEvaluator) Constraints(required []corev1.ResourceName, item runtime.Object) error {
	// 对象计数无需额外约束。
	return nil
}

// Handles 在对象计数评估器需要跟踪指定属性时返回 true。
func (o *objectCountEvaluator) Handles(a admission.Attributes) bool {
	if a.GetSubresource() != "" {
		return false
	}
	operation := a.GetOperation()
	return operation == admission.Create
}

// Matches 在评估器使用给定输入对象匹配指定配额时返回 true。
func (o *objectCountEvaluator) Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error) {
	return Matches(resourceQuota, item, o.MatchingResources, MatchesNoScopeFunc)
}

// MatchingResources 返回与输入资源列表中相匹配的资源集合。
func (o *objectCountEvaluator) MatchingResources(input []corev1.ResourceName) []corev1.ResourceName {
	return quota.Intersection(input, o.resourceNames)
}

// MatchingScopes 返回与输入对象匹配的输入作用域子集。
func (o *objectCountEvaluator) MatchingScopes(item runtime.Object, scopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// UncoveredQuotaScopes 返回受配置限制、但没有对应覆盖配额作用域的作用域。
func (o *objectCountEvaluator) UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error) {
	return []corev1.ScopedResourceSelectorRequirement{}, nil
}

// Usage 返回指定对象的资源用量。
func (o *objectCountEvaluator) Usage(object runtime.Object) (corev1.ResourceList, error) {
	quantity := resource.NewQuantity(1, resource.DecimalSI)
	resourceList := corev1.ResourceList{}
	for _, resourceName := range o.resourceNames {
		resourceList[resourceName] = *quantity
	}
	return resourceList, nil
}

// GroupResource 返回此评估器跟踪的组资源。
func (o *objectCountEvaluator) GroupResource() schema.GroupResource {
	return o.groupResource
}

// UsageStats 计算指定对象的汇总用量。
func (o *objectCountEvaluator) UsageStats(options quota.UsageStatsOptions) (quota.UsageStats, error) {
	return CalculateUsageStats(options, o.listFuncByNamespace, MatchesNoScopeFunc, o.Usage)
}

// 在编译期验证接口实现。
var _ quota.Evaluator = &objectCountEvaluator{}

// NewObjectCountEvaluator 返回执行通用对象配额计数的评估器。
// 为兼容旧配额名称，可传入可选别名；除非需要向后兼容，否则不应使用别名。
func NewObjectCountEvaluator(
	groupResource schema.GroupResource, listFuncByNamespace ListFuncByNamespace,
	alias corev1.ResourceName) quota.Evaluator {

	// 主名称总是存在；alias 仅用于兼容旧配额名称，两者共享同一使用量。
	resourceNames := []corev1.ResourceName{ObjectCountQuotaResourceNameFor(groupResource)}
	if len(alias) > 0 {
		resourceNames = append(resourceNames, alias)
	}

	return &objectCountEvaluator{
		groupResource:       groupResource,
		listFuncByNamespace: listFuncByNamespace,
		resourceNames:       resourceNames,
	}
}
