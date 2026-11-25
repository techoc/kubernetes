/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"

	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// +k8s:deepcopy-gen=false

// PolicyRuleBuilder 允许挂接方法，这在普通 API 类型上是不允许的。
// 我们用它在代码中构造规则：比字面量写法更紧凑，
// 并且允许在构造过程中执行一些基本检查。
type PolicyRuleBuilder struct {
	PolicyRule rbacv1.PolicyRule `protobuf:"bytes,1,opt,name=policyRule"`
}

func NewRule(verbs ...string) *PolicyRuleBuilder {
	return &PolicyRuleBuilder{
		PolicyRule: rbacv1.PolicyRule{Verbs: sets.NewString(verbs...).List()},
	}
}

func (r *PolicyRuleBuilder) Groups(groups ...string) *PolicyRuleBuilder {
	r.PolicyRule.APIGroups = combine(r.PolicyRule.APIGroups, groups)
	return r
}

func (r *PolicyRuleBuilder) Resources(resources ...string) *PolicyRuleBuilder {
	r.PolicyRule.Resources = combine(r.PolicyRule.Resources, resources)
	return r
}

func (r *PolicyRuleBuilder) Names(names ...string) *PolicyRuleBuilder {
	r.PolicyRule.ResourceNames = combine(r.PolicyRule.ResourceNames, names)
	return r
}

func (r *PolicyRuleBuilder) URLs(urls ...string) *PolicyRuleBuilder {
	r.PolicyRule.NonResourceURLs = combine(r.PolicyRule.NonResourceURLs, urls)
	return r
}

func combine(s1, s2 []string) []string {
	s := sets.NewString(s1...)
	s.Insert(s2...)
	return s.List()
}

func (r *PolicyRuleBuilder) RuleOrDie() rbacv1.PolicyRule {
	ret, err := r.Rule()
	if err != nil {
		panic(err)
	}
	return ret
}

func (r *PolicyRuleBuilder) Rule() (rbacv1.PolicyRule, error) {
	if len(r.PolicyRule.Verbs) == 0 {
		return rbacv1.PolicyRule{}, fmt.Errorf("verbs are required: %#v", r.PolicyRule)
	}

	switch {
	case len(r.PolicyRule.NonResourceURLs) > 0:
		if len(r.PolicyRule.APIGroups) != 0 || len(r.PolicyRule.Resources) != 0 || len(r.PolicyRule.ResourceNames) != 0 {
			return rbacv1.PolicyRule{}, fmt.Errorf("non-resource rule may not have apiGroups, resources, or resourceNames: %#v", r.PolicyRule)
		}
	case len(r.PolicyRule.Resources) > 0:
		if len(r.PolicyRule.NonResourceURLs) != 0 {
			return rbacv1.PolicyRule{}, fmt.Errorf("resource rule may not have nonResourceURLs: %#v", r.PolicyRule)
		}
		if len(r.PolicyRule.APIGroups) == 0 {
			// 这是一个常见错误。
			return rbacv1.PolicyRule{}, fmt.Errorf("resource rule must have apiGroups: %#v", r.PolicyRule)
		}
	default:
		return rbacv1.PolicyRule{}, fmt.Errorf("a rule must have either nonResourceURLs or resources: %#v", r.PolicyRule)
	}

	sort.Strings(r.PolicyRule.Resources)
	sort.Strings(r.PolicyRule.ResourceNames)
	sort.Strings(r.PolicyRule.APIGroups)
	sort.Strings(r.PolicyRule.NonResourceURLs)
	sort.Strings(r.PolicyRule.Verbs)
	return r.PolicyRule, nil
}

// +k8s:deepcopy-gen=false

// ClusterRoleBindingBuilder 允许挂接方法，这在 API 类型上是不允许的。
// 我们用它在代码中构造 binding，比字面量写法更紧凑。
type ClusterRoleBindingBuilder struct {
	ClusterRoleBinding rbacv1.ClusterRoleBinding `protobuf:"bytes,1,opt,name=clusterRoleBinding"`
}

func NewClusterBinding(clusterRoleName string) *ClusterRoleBindingBuilder {
	return &ClusterRoleBindingBuilder{
		ClusterRoleBinding: rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: GroupName,
				Kind:     "ClusterRole",
				Name:     clusterRoleName,
			},
		},
	}
}

func (r *ClusterRoleBindingBuilder) Groups(groups ...string) *ClusterRoleBindingBuilder {
	for _, group := range groups {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1.Subject{APIGroup: rbacv1.GroupName, Kind: rbacv1.GroupKind, Name: group})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) Users(users ...string) *ClusterRoleBindingBuilder {
	for _, user := range users {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1.Subject{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) SAs(namespace string, serviceAccountNames ...string) *ClusterRoleBindingBuilder {
	for _, saName := range serviceAccountNames {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: namespace, Name: saName})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) BindingOrDie() rbacv1.ClusterRoleBinding {
	ret, err := r.Binding()
	if err != nil {
		panic(err)
	}
	return ret
}

func (r *ClusterRoleBindingBuilder) Binding() (rbacv1.ClusterRoleBinding, error) {
	if len(r.ClusterRoleBinding.Subjects) == 0 {
		return rbacv1.ClusterRoleBinding{}, fmt.Errorf("subjects are required: %#v", r.ClusterRoleBinding)
	}

	return r.ClusterRoleBinding, nil
}

// +k8s:deepcopy-gen=false

// RoleBindingBuilder 允许挂接方法，与上面的
// ClusterRoleBindingBuilder 类似。
type RoleBindingBuilder struct {
	RoleBinding rbacv1.RoleBinding
}

// NewRoleBinding 创建一个 RoleBinding builder，可用于定义
// 角色 binding 的主体。在调用 `Binding*` 方法之前，
// 必须至少调用 `Groups`、`Users` 或 `SAs` 方法中的一个。
func NewRoleBinding(roleName, namespace string) *RoleBindingBuilder {
	return &RoleBindingBuilder{
		RoleBinding: rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      roleName,
				Namespace: namespace,
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: GroupName,
				Kind:     "Role",
				Name:     roleName,
			},
		},
	}
}

// Groups 将指定的 groups 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) Groups(groups ...string) *RoleBindingBuilder {
	for _, group := range groups {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, rbacv1.Subject{Kind: rbacv1.GroupKind, APIGroup: GroupName, Name: group})
	}
	return r
}

// Users 将指定的 users 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) Users(users ...string) *RoleBindingBuilder {
	for _, user := range users {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: GroupName, Name: user})
	}
	return r
}

// SAs 将指定的 service accounts 添加为 RoleBinding 的主体。
func (r *RoleBindingBuilder) SAs(namespace string, serviceAccountNames ...string) *RoleBindingBuilder {
	for _, saName := range serviceAccountNames {
		r.RoleBinding.Subjects = append(r.RoleBinding.Subjects, rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: namespace, Name: saName})
	}
	return r
}

// BindingOrDie 调用 binding 方法，若出错则 panic。
func (r *RoleBindingBuilder) BindingOrDie() rbacv1.RoleBinding {
	ret, err := r.Binding()
	if err != nil {
		panic(err)
	}
	return ret
}

// Binding 从 builder 对象构建并返回 RoleBinding API 对象。
func (r *RoleBindingBuilder) Binding() (rbacv1.RoleBinding, error) {
	if len(r.RoleBinding.Subjects) == 0 {
		return rbacv1.RoleBinding{}, fmt.Errorf("subjects are required: %#v", r.RoleBinding)
	}

	return r.RoleBinding, nil
}
