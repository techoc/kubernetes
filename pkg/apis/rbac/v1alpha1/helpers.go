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

package v1alpha1

import (
	"fmt"

	rbacv1alpha1 "k8s.io/api/rbac/v1alpha1"
)

// PolicyRuleBuilder 允许挂接方法，这在普通 API 类型上是不允许的。
// 用它可以在代码中构造 rule：比逐字写字面量更紧凑，还能在构造时做基本校验。
type PolicyRuleBuilder struct {
	PolicyRule rbacv1alpha1.PolicyRule `protobuf:"bytes,1,opt,name=policyRule"`
}

func (r *PolicyRuleBuilder) Groups(groups ...string) *PolicyRuleBuilder {
	r.PolicyRule.APIGroups = append(r.PolicyRule.APIGroups, groups...)
	return r
}

func (r *PolicyRuleBuilder) Resources(resources ...string) *PolicyRuleBuilder {
	r.PolicyRule.Resources = append(r.PolicyRule.Resources, resources...)
	return r
}

func (r *PolicyRuleBuilder) Names(names ...string) *PolicyRuleBuilder {
	r.PolicyRule.ResourceNames = append(r.PolicyRule.ResourceNames, names...)
	return r
}

func (r *PolicyRuleBuilder) URLs(urls ...string) *PolicyRuleBuilder {
	r.PolicyRule.NonResourceURLs = append(r.PolicyRule.NonResourceURLs, urls...)
	return r
}

func (r *PolicyRuleBuilder) RuleOrDie() rbacv1alpha1.PolicyRule {
	ret, err := r.Rule()
	if err != nil {
		panic(err)
	}
	return ret
}

func (r *PolicyRuleBuilder) Rule() (rbacv1alpha1.PolicyRule, error) {
	if len(r.PolicyRule.Verbs) == 0 {
		return rbacv1alpha1.PolicyRule{}, fmt.Errorf("verbs are required: %#v", r.PolicyRule)
	}

	switch {
	case len(r.PolicyRule.NonResourceURLs) > 0:
		if len(r.PolicyRule.APIGroups) != 0 || len(r.PolicyRule.Resources) != 0 || len(r.PolicyRule.ResourceNames) != 0 {
			return rbacv1alpha1.PolicyRule{}, fmt.Errorf("non-resource rule may not have apiGroups, resources, or resourceNames: %#v", r.PolicyRule)
		}
	case len(r.PolicyRule.Resources) > 0:
		if len(r.PolicyRule.NonResourceURLs) != 0 {
			return rbacv1alpha1.PolicyRule{}, fmt.Errorf("resource rule may not have nonResourceURLs: %#v", r.PolicyRule)
		}
		if len(r.PolicyRule.APIGroups) == 0 {
			// 这是一个常见错误。
			return rbacv1alpha1.PolicyRule{}, fmt.Errorf("resource rule must have apiGroups: %#v", r.PolicyRule)
		}
	default:
		return rbacv1alpha1.PolicyRule{}, fmt.Errorf("a rule must have either nonResourceURLs or resources: %#v", r.PolicyRule)
	}

	return r.PolicyRule, nil
}

// ClusterRoleBindingBuilder 让我们能给绑定挂方法，API 类型本身不允许这样做。
// 用它可以在代码中构造绑定，比写字面量更紧凑。
type ClusterRoleBindingBuilder struct {
	ClusterRoleBinding rbacv1alpha1.ClusterRoleBinding `protobuf:"bytes,1,opt,name=clusterRoleBinding"`
}

func (r *ClusterRoleBindingBuilder) Groups(groups ...string) *ClusterRoleBindingBuilder {
	for _, group := range groups {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1alpha1.Subject{Kind: rbacv1alpha1.GroupKind, Name: group})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) Users(users ...string) *ClusterRoleBindingBuilder {
	for _, user := range users {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1alpha1.Subject{Kind: rbacv1alpha1.UserKind, Name: user})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) SAs(namespace string, serviceAccountNames ...string) *ClusterRoleBindingBuilder {
	for _, saName := range serviceAccountNames {
		r.ClusterRoleBinding.Subjects = append(r.ClusterRoleBinding.Subjects, rbacv1alpha1.Subject{Kind: rbacv1alpha1.ServiceAccountKind, Namespace: namespace, Name: saName})
	}
	return r
}

func (r *ClusterRoleBindingBuilder) BindingOrDie() rbacv1alpha1.ClusterRoleBinding {
	ret, err := r.Binding()
	if err != nil {
		panic(err)
	}
	return ret
}

func (r *ClusterRoleBindingBuilder) Binding() (rbacv1alpha1.ClusterRoleBinding, error) {
	if len(r.ClusterRoleBinding.Subjects) == 0 {
		return rbacv1alpha1.ClusterRoleBinding{}, fmt.Errorf("subjects are required: %#v", r.ClusterRoleBinding)
	}

	return r.ClusterRoleBinding, nil
}
