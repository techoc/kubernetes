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

// Matcher decides if a request matches against matchCriteria
type Matcher struct {
	namespaceMatcher *namespace.Matcher
	objectMatcher    *object.Matcher
}

// GetNamespace 暴露底层 namespace matcher 的查询能力，供需要 namespace 对象的
// 策略参数解析等场景使用。
func (m *Matcher) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return m.namespaceMatcher.GetNamespace(ctx, name)
}

// NewMatcher initialize the matcher with dependencies requires
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

// ValidateInitialization verify if the matcher is ready before use
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
	// Should not return an error here for policy which do not apply to the request, even if err is an unexpected scenario.
	if !matches && matchNsErr == nil {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	matches, matchObjErr := m.objectMatcher.MatchObjectSelector(criteria, attr)
	// Should not return an error here for policy which do not apply to the request, even if err is an unexpected scenario.
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

	// now that we know this applies to this request otherwise, if there were selector errors, return them
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
		// an empty name list always matches
		if len(namedRule.ResourceNames) == 0 {
			return true, matchResource, matchKind, nil
		}
		// TODO: GetName() can return an empty string if the user is relying on
		// the API server to generate the name... figure out what to do for this edge case
		name := attr.GetName()
		for _, matchedName := range namedRule.ResourceNames {
			if name == matchedName {
				return true, matchResource, matchKind, nil
			}
		}
	}

	// if match policy is undefined or exact, don't perform fuzzy matching
	// note that defaulting to fuzzy matching is set by the API
	// nil 视为 Exact：策略未显式声明时不做等价资源扩展。
	if matchPolicy == nil || *matchPolicy == v1.Exact {
		return false, schema.GroupVersionResource{}, schema.GroupVersionKind{}, nil
	}

	// Equivalent 策略：用等价资源重放规则匹配，并返回实际匹配到的资源与 Kind。
	attrWithOverride := &attrWithResourceOverride{Attributes: attr}
	equivalents := o.GetEquivalentResourceMapper().EquivalentResourcesFor(attr.GetResource(), attr.GetSubresource())
	for _, namedRule := range namedRules {
		for _, equivalent := range equivalents {
			if equivalent == attr.GetResource() {
				// we have already checked the original resource
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
			// an empty name list always matches
			if len(namedRule.ResourceNames) == 0 {
				return true, equivalent, matchKind, nil
			}

			// TODO: GetName() can return an empty string if the user is relying on
			// the API server to generate the name... figure out what to do for this edge case
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
