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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/tools/cache"
)

// UsageStatsOptions 是描述用量统计方式的选项结构体。
// UsageStatsOptions 描述统计口径：命名空间、作用域与资源集合共同决定哪些对象
// 会被计入用量。
type UsageStatsOptions struct {
	// 统计所在的命名空间。
	Namespace string
	// 被计入的对象必须匹配的作用域。
	Scopes []corev1.ResourceQuotaScope
	// 计入测量的资源集合。
	Resources     []corev1.ResourceName
	ScopeSelector *corev1.ScopeSelector
}

// UsageStats 是系统中已观察资源用量的测量结果。
type UsageStats struct {
	// 资源到已用数量的映射。
	Used corev1.ResourceList
}

// Evaluator 知道如何评估特定组资源的配额用量。
// Evaluator 是每种资源类型（如 pods、services）的用量计算插件。
type Evaluator interface {
	// Constraints 确保对象的每个必需资源都存在。
	Constraints(required []corev1.ResourceName, item runtime.Object) error
	// GroupResource 返回此评估器能够处理的组资源。
	GroupResource() schema.GroupResource
	// Handles 判断指定操作是否可能影响配额。返回 true 时，准入控制必须执行配额处理；
	// 返回 false 时，可以安全地忽略该操作的配额。
	Handles(operation admission.Attributes) bool
	// Matches 在指定配额匹配输入对象时返回 true。
	Matches(resourceQuota *corev1.ResourceQuota, item runtime.Object) (bool, error)
	// MatchingScopes 返回给定作用域列表中与输入对象匹配的作用域。
	MatchingScopes(item runtime.Object, scopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error)
	// UncoveredQuotaScopes 返回受配置限制、但没有对应覆盖配额作用域的作用域。
	UncoveredQuotaScopes(limitedScopes []corev1.ScopedResourceSelectorRequirement, matchedQuotaScopes []corev1.ScopedResourceSelectorRequirement) ([]corev1.ScopedResourceSelectorRequirement, error)
	// MatchingResources 返回输入资源列表中与评估器匹配的资源。
	MatchingResources(input []corev1.ResourceName) []corev1.ResourceName
	// Usage 返回指定对象的资源用量。
	Usage(item runtime.Object) (corev1.ResourceList, error)
	// UsageStats 计算所有对象的最新观察用量。
	UsageStats(options UsageStatsOptions) (UsageStats, error)
}

// Configuration 定义配额系统的配置方式。
// Configuration 提供准入控制器所需的静态配置：忽略资源与评估器集合。
type Configuration interface {
	// IgnoredResources 返回配额忽略的资源。
	IgnoredResources() map[schema.GroupResource]struct{}
	// Evaluators 返回用于配额评估的评估器。
	Evaluators() []Evaluator
}

// Registry 维护评估器列表。
// Registry 把 GroupResource 映射到 Evaluator；实现需保证并发安全。
type Registry interface {
	// Add 将评估器加入注册表。
	Add(e Evaluator)
	// Remove 从注册表移除评估器。
	Remove(e Evaluator)
	// Get 按组资源获取评估器。
	Get(gr schema.GroupResource) Evaluator
	// List 返回注册表中的评估器。
	List() []Evaluator
}

// ListerForResourceFunc 知道如何获取指定资源的 lister。
// 评估器借助它从 informer 缓存读取对象，用于批量计算已用量。
type ListerForResourceFunc func(schema.GroupVersionResource) (cache.GenericLister, error)
