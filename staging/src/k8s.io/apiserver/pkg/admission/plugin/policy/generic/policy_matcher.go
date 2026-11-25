/*
Copyright 2022 The Kubernetes Authors.

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
	"context"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
)

// PolicyMatcher 负责把 ValidatingAdmissionPolicy / Binding 的匹配条件与请求属性做比对。
type PolicyMatcher interface {
	admission.InitializationValidator

	// DefinitionMatches 判断策略定义是否匹配给定的准入资源请求。
	// 同时返回解析出的资源与类型，便于后续在命中资源上做精确匹配。
	DefinitionMatches(a admission.Attributes, o admission.ObjectInterfaces, definition PolicyAccessor) (bool, schema.GroupVersionResource, schema.GroupVersionKind, error)

	// BindingMatches 判断策略绑定是否匹配给定的准入资源请求。
	BindingMatches(a admission.Attributes, o admission.ObjectInterfaces, binding BindingAccessor) (bool, error)

	// GetNamespace 按给定名称查找 Namespace 资源。name 可以为空，
	// 此时 GetNamespace 必须返回 nil 与 NotFound。
	GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error)
}

// errNilSelector 是选择器未初始化的统一报错模板。
// 选择器为 nil 通常意味着 API 默认化缺失或对象来自非标准构造路径，
// 继续匹配会得到错误结果，因此直接失败而非当作“匹配全部”。
var errNilSelector = "a nil %s selector was passed, please ensure selectors are initialized properly"

// matcher 是 PolicyMatcher 的实现，把匹配委托给共享的 matching.Matcher。
type matcher struct {
	Matcher *matching.Matcher
}

// NewPolicyMatcher 用底层 Matcher 构造 PolicyMatcher。
func NewPolicyMatcher(m *matching.Matcher) PolicyMatcher {
	return &matcher{
		Matcher: m,
	}
}

// ValidateInitialization 检查底层 Matcher 是否已正确初始化。
func (c *matcher) ValidateInitialization() error {
	return c.Matcher.ValidateInitialization()
}

// DefinitionMatches 返回该策略定义是否匹配给定的准入资源请求。
func (c *matcher) DefinitionMatches(a admission.Attributes, o admission.ObjectInterfaces, definition PolicyAccessor) (bool, schema.GroupVersionResource, schema.GroupVersionKind, error) {
	constraints := definition.GetMatchConstraints()
	// matchConstraints 是必填字段，缺失视为策略非法而不是“匹配所有”。
	if constraints == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, fmt.Errorf("policy contained no match constraints, a required field")
	}
	if constraints.NamespaceSelector == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, fmt.Errorf(errNilSelector, "namespace")
	}
	if constraints.ObjectSelector == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, fmt.Errorf(errNilSelector, "object")
	}

	criteria := matchCriteria{constraints: constraints}
	return c.Matcher.Matches(a, o, &criteria)
}

// BindingMatches 返回该策略绑定是否匹配给定的准入资源请求。
func (c *matcher) BindingMatches(a admission.Attributes, o admission.ObjectInterfaces, binding BindingAccessor) (bool, error) {
	matchResources := binding.GetMatchResources()
	// 与定义不同，绑定未设置 matchResources 表示匹配全部，这是 API 允许的语义。
	if matchResources == nil {
		return true, nil
	}
	if matchResources.NamespaceSelector == nil {
		return false, fmt.Errorf(errNilSelector, "namespace")
	}
	if matchResources.ObjectSelector == nil {
		return false, fmt.Errorf(errNilSelector, "object")
	}

	criteria := matchCriteria{constraints: matchResources}
	isMatch, _, _, err := c.Matcher.Matches(a, o, &criteria)
	return isMatch, err
}

// GetNamespace 按名称查找 Namespace；name 为空时必须返回 NotFound 而非 nil。
func (c *matcher) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return c.Matcher.GetNamespace(ctx, name)
}

// 编译期断言：matchCriteria 满足 matching.MatchCriteria。
var _ matching.MatchCriteria = &matchCriteria{}

// matchCriteria 把 MatchResources 适配成 matching.MatchCriteria。
// 用适配器而非直接改 API 类型，避免 API 包反向依赖匹配实现。
type matchCriteria struct {
	constraints *admissionregistrationv1.MatchResources
}

// GetParsedNamespaceSelector 返回转换为 labels.Selector 的命名空间选择器。
func (m *matchCriteria) GetParsedNamespaceSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(m.constraints.NamespaceSelector)
}

// GetParsedObjectSelector 返回转换为 labels.Selector 的对象选择器。
func (m *matchCriteria) GetParsedObjectSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(m.constraints.ObjectSelector)
}

// GetMatchResources 返回底层匹配条件（按值复制，避免调用方误改共享对象）。
func (m *matchCriteria) GetMatchResources() admissionregistrationv1.MatchResources {
	return *m.constraints
}
