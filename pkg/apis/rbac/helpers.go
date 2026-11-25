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

package rbac

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ResourceMatches 返回 rule.Resources 的匹配结果。
func ResourceMatches(rule *PolicyRule, combinedRequestedResource, requestedSubresource string) bool {
	for _, ruleResource := range rule.Resources {
		// 如果允许全部资源，则直接匹配
		if ruleResource == ResourceAll {
			return true
		}
		// 资源名完全一致时直接命中。
		if ruleResource == combinedRequestedResource {
			return true
		}

		// 还支持 "*/subresource" 形式：对任意资源的该子资源授权。
		// 请求不涉及子资源时无需继续尝试这种形式。
		if len(requestedSubresource) == 0 {
			continue
		}
		// 规则不是 "*/subresource" 格式（长度不符或缺少前缀/后缀）时不匹配。
		if len(ruleResource) == len(requestedSubresource)+2 &&
			strings.HasPrefix(ruleResource, "*/") &&
			strings.HasSuffix(ruleResource, requestedSubresource) {
			return true

		}
	}

	return false
}

// SubjectsStrings 返回 users、groups、serviceaccounts 及未知类型的主体，用于展示。
func SubjectsStrings(subjects []Subject) ([]string, []string, []string, []string) {
	users := []string{}
	groups := []string{}
	sas := []string{}
	others := []string{}

	for _, subject := range subjects {
		switch subject.Kind {
		case ServiceAccountKind:
			sas = append(sas, fmt.Sprintf("%s/%s", subject.Namespace, subject.Name))

		case UserKind:
			users = append(users, subject.Name)

		case GroupKind:
			groups = append(groups, subject.Name)

		default:
			others = append(others, fmt.Sprintf("%s/%s/%s", subject.Kind, subject.Namespace, subject.Name))
		}
	}

	return users, groups, sas, others
}

func (r PolicyRule) String() string {
	return "PolicyRule" + r.CompactString()
}

// CompactString 返回规则的紧凑字符串表示，用于权限提升（escalation）相关的错误信息。
func (r PolicyRule) CompactString() string {
	formatStringParts := []string{}
	formatArgs := []interface{}{}
	if len(r.APIGroups) > 0 {
		formatStringParts = append(formatStringParts, "APIGroups:%q")
		formatArgs = append(formatArgs, r.APIGroups)
	}
	if len(r.Resources) > 0 {
		formatStringParts = append(formatStringParts, "Resources:%q")
		formatArgs = append(formatArgs, r.Resources)
	}
	if len(r.NonResourceURLs) > 0 {
		formatStringParts = append(formatStringParts, "NonResourceURLs:%q")
		formatArgs = append(formatArgs, r.NonResourceURLs)
	}
	if len(r.ResourceNames) > 0 {
		formatStringParts = append(formatStringParts, "ResourceNames:%q")
		formatArgs = append(formatArgs, r.ResourceNames)
	}
	if len(r.Verbs) > 0 {
		formatStringParts = append(formatStringParts, "Verbs:%q")
		formatArgs = append(formatArgs, r.Verbs)
	}
	formatString := "{" + strings.Join(formatStringParts, ", ") + "}"
	return fmt.Sprintf(formatString, formatArgs...)
}

// PolicyRuleBuilder 允许挂接方法，这在普通 API 类型上是不允许的。
// 我们用它在代码中构造规则：比字面量写法更紧凑，
// 并且允许在构造过程中执行一些基本检查。
// +k8s:deepcopy-gen=false
type PolicyRuleBuilder struct {
	PolicyRule PolicyRule
}

// NewRule 返回由输入 verbs 构造的新 PolicyRule。
func NewRule(verbs ...string) *PolicyRuleBuilder {
	return &PolicyRuleBuilder{
		PolicyRule: PolicyRule{Verbs: sets.NewString(verbs...).List()},
	}
}

// Groups 合并 PolicyRule.APIGroups 与输入的 groups。
func (r *PolicyRuleBuilder) Groups(groups ...string) *PolicyRuleBuilder {
	r.PolicyRule.APIGroups = combine(r.PolicyRule.APIGroups, groups)
	return r
}

// Resources 合并 PolicyRule.Resources 与输入的 resources。
func (r *PolicyRuleBuilder) Resources(resources ...string) *PolicyRuleBuilder {
	r.PolicyRule.Resources = combine(r.PolicyRule.Resources, resources)
	return r
}

// Names 合并 PolicyRule.ResourceNames 与输入的 names。
func (r *PolicyRuleBuilder) Names(names ...string) *PolicyRuleBuilder {
	r.PolicyRule.ResourceNames = combine(r.PolicyRule.ResourceNames, names)
	return r
}

// URLs 合并 PolicyRule.NonResourceURLs 与输入的 urls。
func (r *PolicyRuleBuilder) URLs(urls ...string) *PolicyRuleBuilder {
	r.PolicyRule.NonResourceURLs = combine(r.PolicyRule.NonResourceURLs, urls)
	return r
}

// RuleOrDie 调用 binding 方法，若出错则 panic。
func (r *PolicyRuleBuilder) RuleOrDie() PolicyRule {
	ret, err := r.Rule()
	if err != nil {
		panic(err)
	}
	return ret
}

func combine(s1, s2 []string) []string {
	s := sets.NewString(s1...)
	s.Insert(s2...)
	return s.List()
}

// Rule 返回 PolicyRule 及错误。
func (r *PolicyRuleBuilder) Rule() (PolicyRule, error) {
	if len(r.PolicyRule.Verbs) == 0 {
		return PolicyRule{}, fmt.Errorf("verbs are required: %#v", r.PolicyRule)
	}

	switch {
	case len(r.PolicyRule.NonResourceURLs) > 0:
		if len(r.PolicyRule.APIGroups) != 0 || len(r.PolicyRule.Resources) != 0 || len(r.PolicyRule.ResourceNames) != 0 {
			return PolicyRule{}, fmt.Errorf("non-resource rule may not have apiGroups, resources, or resourceNames: %#v", r.PolicyRule)
		}
	case len(r.PolicyRule.Resources) > 0:
		// 资源规则不能带有 nonResourceURLs

		if len(r.PolicyRule.APIGroups) == 0 {
			// 这是一个常见错误。
			return PolicyRule{}, fmt.Errorf("resource rule must have apiGroups: %#v", r.PolicyRule)
		}
		// 如果设置了 resource names，则 verb 不得为 list、watch、create 或 deletecollection。
		// 由于 verbs 在很大程度上是不透明的，我们不希望意外阻止诸如 "impersonate" 之类的用法，因此
		// 这里采用黑名单列出常见错误，而不是用白名单列出允许的选项。
		if len(r.PolicyRule.ResourceNames) != 0 {
			illegalVerbs := []string{}
			for _, verb := range r.PolicyRule.Verbs {
				switch verb {
				case "list", "watch", "create", "deletecollection":
					illegalVerbs = append(illegalVerbs, verb)
				}
			}
			if len(illegalVerbs) > 0 {
				return PolicyRule{}, fmt.Errorf("verbs %v do not have names available: %#v", illegalVerbs, r.PolicyRule)
			}
		}

	default:
		return PolicyRule{}, fmt.Errorf("a rule must have either nonResourceURLs or resources: %#v", r.PolicyRule)
	}

	return r.PolicyRule, nil
}

// ClusterRoleBindingBuilder 允许挂接方法，这在 API 类型上是不允许的。
// 我们用它在代码中构造 binding，比字面量写法更紧凑。
// +k8s:deepcopy-gen=false
type ClusterRoleBindingBuilder struct {
	ClusterRoleBinding ClusterRoleBinding
}

// NewClusterBinding 创建一个 ClusterRoleBinding builder，可用于定义
// 集群角色 binding 的主体。在调用 `Binding*` 方法之前，
// 必须至少调用 `Groups`、`Users` 或 `SAs` 方法中的一个。
func NewClusterBinding(clusterRoleName string) *ClusterRoleBindingBuilder {
	return &ClusterRoleBindingBuilder{
		ClusterRoleBinding: ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName},
			RoleRef: RoleRef{
				APIGroup: GroupName,
				Kind:     "ClusterRole",
				Name:     clusterRoleName,
			},
		},
	}
}

// Groups 将指定的 groups 添加为 ClusterRoleBinding 的主体。
func (r *ClusterRoleBindingBuilder) Groups(groups ...string) *ClusterRoleBindingBuilder {
	for _, group := range groups {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, Subject{Kind: GroupKind, APIGroup: GroupName, Name: group})
	}
	return r
}

// Users 将指定的 users 添加为 ClusterRoleBinding 的主体。
func (r *ClusterRoleBindingBuilder) Users(users ...string) *ClusterRoleBindingBuilder {
	for _, user := range users {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, Subject{Kind: UserKind, APIGroup: GroupName, Name: user})
	}
	return r
}

// SAs 将指定的 service account 添加为 ClusterRoleBinding 的主体。
func (r *ClusterRoleBindingBuilder) SAs(namespace string, serviceAccountNames ...string) *ClusterRoleBindingBuilder {
	for _, saName := range serviceAccountNames {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, Subject{Kind: ServiceAccountKind, Namespace: namespace, Name: saName})
	}
	return r
}

// BindingOrDie 调用 binding 方法，若出错则 panic。
func (r *ClusterRoleBindingBuilder) BindingOrDie() ClusterRoleBinding {
	ret, err := r.Binding()
	if err != nil {
		panic(err)
	}
	return ret
}

// Binding 从 builder 对象构建并返回 ClusterRoleBinding API 对象。
func (r *ClusterRoleBindingBuilder) Binding() (ClusterRoleBinding, error) {
	if len(r.ClusterRoleBinding.Subjects) == 0 {
		return ClusterRoleBinding{}, fmt.Errorf("subjects are required: %#v", r.ClusterRoleBinding)
	}

	return r.ClusterRoleBinding, nil
}

// RoleBindingBuilder 允许挂接方法，与上面的
// ClusterRoleBindingBuilder 类似。
// +k8s:deepcopy-gen=false
type RoleBindingBuilder struct {
	RoleBinding RoleBinding
}

// NewRoleBinding 创建一个 RoleBinding builder，可用于定义
// 角色 binding 的主体。在调用 `Binding*` 方法之前，
// 必须至少调用 `Groups`、`Users` 或 `SAs` 方法中的一个。
func NewRoleBinding(roleName, namespace string) *RoleBindingBuilder {
	return &RoleBindingBuilder{
		RoleBinding: RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      roleName,
				Namespace: namespace,
			},
			RoleRef: RoleRef{
				APIGroup: GroupName,
				Kind:     "Role",
				Name:     roleName,
			},
		},
	}
}

// NewRoleBindingForClusterRole 创建一个 RoleBinding builder，可用于定义
// 集群角色 binding 的主体。在调用 `Binding*` 方法之前，
// 必须至少调用 `Groups`、`Users` 或 `SAs` 方法中的一个。
func NewRoleBindingForClusterRole(roleName, namespace string) *RoleBindingBuilder {
	return &RoleBindingBuilder{
		RoleBinding: RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      roleName,
				Namespace: namespace,
			},
			RoleRef: RoleRef{
				APIGroup: GroupName,
				Kind:     "ClusterRole",
				Name:     roleName,
			},
		},
	}
}

// Groups 将指定的 groups 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) Groups(groups ...string) *RoleBindingBuilder {
	for _, group := range groups {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, Subject{Kind: GroupKind, APIGroup: GroupName, Name: group})
	}
	return r
}

// Users 将指定的 users 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) Users(users ...string) *RoleBindingBuilder {
	for _, user := range users {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, Subject{Kind: UserKind, APIGroup: GroupName, Name: user})
	}
	return r
}

// SAs 将指定的 service accounts 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) SAs(namespace string, serviceAccountNames ...string) *RoleBindingBuilder {
	for _, saName := range serviceAccountNames {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, Subject{Kind: ServiceAccountKind, Namespace: namespace, Name: saName})
	}
	return r
}

// BindingOrDie 调用 binding 方法，若出错则 panic。
func (r *RoleBindingBuilder) BindingOrDie() RoleBinding {
	ret, err := r.Binding()
	if err != nil {
		panic(err)
	}
	return ret
}

// Binding 从 builder 对象构建并返回 RoleBinding API 对象。
func (r *RoleBindingBuilder) Binding() (RoleBinding, error) {
	if len(r.RoleBinding.Subjects) == 0 {
		return RoleBinding{}, fmt.Errorf("subjects are required: %#v", r.RoleBinding)
	}

	return r.RoleBinding, nil
}

// SortableRuleSlice 是 PolicyRule 的切片。
type SortableRuleSlice []PolicyRule

func (s SortableRuleSlice) Len() int      { return len(s) }
func (s SortableRuleSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortableRuleSlice) Less(i, j int) bool {
	return strings.Compare(s[i].String(), s[j].String()) < 0
}
