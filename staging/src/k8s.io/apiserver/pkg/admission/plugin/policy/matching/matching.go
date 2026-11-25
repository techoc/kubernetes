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

// Package matching 把准入策略（ValidatingAdmissionPolicy 等）的 matchConstraints
// 与请求属性进行匹配，判断某条策略是否适用于当前请求。
package matching

import (
	"context"
	"fmt"

	v1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/namespace"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/object"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules"
)

// MatchCriteria 是 Matcher 所需的策略侧输入：命名空间选择器、对象选择器以及
// 资源匹配规则。
type MatchCriteria interface {
	namespace.NamespaceSelectorProvider
	object.ObjectSelectorProvider

	GetMatchResources() v1.MatchResources
}

// Matcher 判断请求是否满足 matchCriteria。
type Matcher struct {
	namespaceMatcher *namespace.Matcher
	objectMatcher    *object.Matcher
}

// GetNamespace 暴露底层 namespace matcher 的查询能力，供需要 namespace 对象的
// 策略参数解析等场景使用。
func (m *Matcher) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return m.namespaceMatcher.GetNamespace(ctx, name)
}

// NewMatcher 用所需的依赖初始化 matcher。
func NewMatcher(
	namespaceLister listersv1.NamespaceLister,
	client kubernetes.Interface,
) *Matcher {
	return &Matcher{
		namespaceMatcher: &namespace.Matcher{
			NamespaceLister: namespaceLister,
			Client:          client,
		},
		objectMatcher: &object.Matcher{},
	}
}

// ValidateInitialization 在使用前校验 matcher 是否已就绪。
func (m *Matcher) ValidateInitialization() error {
	if err := m.namespaceMatcher.Validate(); err != nil {
		return fmt.Errorf("namespaceMatcher is not properly setup: %v", err)
	}
	return nil
}

// Matches 判断请求是否命中给定策略，并返回匹配到的等价资源与 Kind。
//
// 求值顺序是刻意的：先做廉价的 selector 检查，再做资源规则匹配，最后才把
// selector 的错误上报。这样，若请求本就不适用于该策略，即使读取 namespace
// 失败也不会让整个准入链报错——错误只应影响真正会应用该策略的请求。
func (m *Matcher) Matches(attr admission.Attributes, o admission.ObjectInterfaces, criteria MatchCriteria) (bool, schema.GroupVersionResource, schema.GroupVersionKind, error) {
	matches, matchNsErr := m.namespaceMatcher.MatchNamespaceSelector(criteria, attr)
	// 对不适用于当前请求的策略，即使 err 属于意外场景，也不应在此返回错误。
	if !matches && matchNsErr == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	matches, matchObjErr := m.objectMatcher.MatchObjectSelector(criteria, attr)
	// 对不适用于当前请求的策略，即使 err 属于意外场景，也不应在此返回错误。
	if !matches && matchObjErr == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	matchResources := criteria.GetMatchResources()
	matchPolicy := matchResources.MatchPolicy
	// 排除规则优先于包含规则：命中任一 exclude 即视为不适用。
	if isExcluded, _, _, err := matchesResourceRules(matchResources.ExcludeResourceRules, matchPolicy, attr, o); isExcluded || err != nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, err
	}

	var (
		isMatch       bool
		matchResource schema.GroupVersionResource
		matchKind     schema.GroupVersionKind
		matchErr      error
	)
	if len(matchResources.ResourceRules) == 0 {
		// 未配置 resourceRules 表示匹配所有资源。
		isMatch = true
		matchKind = attr.GetKind()
		matchResource = attr.GetResource()
	} else {
		isMatch, matchResource, matchKind, matchErr = matchesResourceRules(matchResources.ResourceRules, matchPolicy, attr, o)
	}
	if matchErr != nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, matchErr
	}
	if !isMatch {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	// 至此已确认该策略适用于当前请求；selector 错误此时才需要返回。
	if matchNsErr != nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, matchNsErr
	}
	if matchObjErr != nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, matchObjErr
	}

	return true, matchResource, matchKind, nil
}

// matchesResourceRules 按规则逐条匹配请求；若 matchPolicy 允许且直接匹配失败，
// 再尝试把请求资源映射到等价资源（例如 Deployment 与 ReplicaSet 的 scale 子资源）
// 重新匹配。
func matchesResourceRules(namedRules []v1.NamedRuleWithOperations, matchPolicy *v1.MatchPolicyType, attr admission.Attributes, o admission.ObjectInterfaces) (bool, schema.GroupVersionResource, schema.GroupVersionKind, error) {
	matchKind := attr.GetKind()
	matchResource := attr.GetResource()

	for _, namedRule := range namedRules {
		rule := v1.RuleWithOperations(namedRule.RuleWithOperations)
		ruleMatcher := rules.Matcher{
			Rule: rule,
			Attr: attr,
		}
		if !ruleMatcher.Matches() {
			continue
		}
		// 名称列表为空表示始终匹配。
		if len(namedRule.ResourceNames) == 0 {
			return true, matchResource, matchKind, nil
		}
		// TODO: 若用户依赖 API server 生成名称，GetName() 可能返回空字符串……
		// 需要确定这种边界情况的处理方式。
		name := attr.GetName()
		for _, matchedName := range namedRule.ResourceNames {
			if name == matchedName {
				return true, matchResource, matchKind, nil
			}
		}
	}

	// nil 视为 Exact：策略未显式声明时不做等价资源扩展；
	// 注意默认走 fuzzy 匹配是由 API 设置的。
	if matchPolicy == nil || *matchPolicy == v1.Exact {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	// Equivalent 策略：用等价资源重放规则匹配，并返回实际匹配到的资源与 Kind。
	attrWithOverride := &attrWithResourceOverride{Attributes: attr}
	equivalents := o.GetEquivalentResourceMapper().EquivalentResourcesFor(attr.GetResource(), attr.GetSubresource())
	for _, namedRule := range namedRules {
		for _, equivalent := range equivalents {
			if equivalent == attr.GetResource() {
				// 原始资源此前已经检查过。
				continue
			}
			attrWithOverride.resource = equivalent
			rule := v1.RuleWithOperations(namedRule.RuleWithOperations)
			m := rules.Matcher{
				Rule: rule,
				Attr: attrWithOverride,
			}
			if !m.Matches() {
				continue
			}
			matchKind = o.GetEquivalentResourceMapper().KindFor(equivalent, attr.GetSubresource())
			// 无法确定 Kind 时不能构造 AdmissionRequest，必须显式失败而不是猜测。
			if matchKind.Empty() {
				return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, fmt.Errorf("unable to convert to %v: unknown kind", equivalent)
			}
			// 名称列表为空表示始终匹配。
			if len(namedRule.ResourceNames) == 0 {
				return true, equivalent, matchKind, nil
			}

			// TODO: 若用户依赖 API server 生成名称，GetName() 可能返回空字符串……
			// 需要确定这种边界情况的处理方式。
			name := attr.GetName()
			for _, matchedName := range namedRule.ResourceNames {
				if name == matchedName {
					return true, equivalent, matchKind, nil
				}
			}
		}
	}
	return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
}

// attrWithResourceOverride 在保持其余属性不变的前提下替换 GetResource 的返回值，
// 用于等价资源匹配。
type attrWithResourceOverride struct {
	admission.Attributes
	resource schema.GroupVersionResource
}

func (a *attrWithResourceOverride) GetResource() schema.GroupVersionResource { return a.resource }
