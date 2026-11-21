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

package v1alpha1_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	v1alpha1 "k8s.io/api/admissionregistration/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	_ "k8s.io/kubernetes/pkg/apis/admissionregistration/install"
)

// TestDefaultAdmissionPolicy 验证 v1alpha1 版本验证/变更准入策略经过默认化后，
// 失败策略、匹配资源策略和选择器、规则作用域均被正确填充。
func TestDefaultAdmissionPolicy(t *testing.T) {
	fail := v1alpha1.Fail
	never := v1alpha1.NeverReinvocationPolicy
	equivalent := v1alpha1.Equivalent
	allScopes := v1alpha1.AllScopes

	tests := []struct {
		name     string
		original runtime.Object
		expected runtime.Object
	}{
		{
			name: "ValidatingAdmissionPolicy",
			original: &v1alpha1.ValidatingAdmissionPolicy{
				Spec: v1alpha1.ValidatingAdmissionPolicySpec{
					MatchConstraints: &v1alpha1.MatchResources{},
				},
			},
			expected: &v1alpha1.ValidatingAdmissionPolicy{
				Spec: v1alpha1.ValidatingAdmissionPolicySpec{
					MatchConstraints: &v1alpha1.MatchResources{
						MatchPolicy:       &equivalent,
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
					},
					FailurePolicy: &fail,
				},
			},
		},
		{
			name: "ValidatingAdmissionPolicyBinding",
			original: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					MatchResources: &v1alpha1.MatchResources{},
				},
			},
			expected: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					MatchResources: &v1alpha1.MatchResources{
						MatchPolicy:       &equivalent,
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
					},
				},
			},
		},
		{
			name: "scope=*",
			original: &v1alpha1.ValidatingAdmissionPolicy{
				Spec: v1alpha1.ValidatingAdmissionPolicySpec{
					MatchConstraints: &v1alpha1.MatchResources{
						ResourceRules: []v1alpha1.NamedRuleWithOperations{{}},
					},
				},
			},
			expected: &v1alpha1.ValidatingAdmissionPolicy{
				Spec: v1alpha1.ValidatingAdmissionPolicySpec{
					MatchConstraints: &v1alpha1.MatchResources{
						MatchPolicy:       &equivalent,
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						ResourceRules: []v1alpha1.NamedRuleWithOperations{
							{
								RuleWithOperations: v1alpha1.RuleWithOperations{
									Rule: v1alpha1.Rule{
										Scope: &allScopes, // defaulted
									},
								},
							},
						},
					},
					FailurePolicy: &fail,
				},
			},
		},
		{
			name: "MutatingAdmissionPolicy",
			original: &v1alpha1.MutatingAdmissionPolicy{
				Spec: v1alpha1.MutatingAdmissionPolicySpec{
					MatchConstraints:   &v1alpha1.MatchResources{},
					ReinvocationPolicy: never,
					Mutations: []v1alpha1.Mutation{
						{
							PatchType: v1alpha1.PatchTypeApplyConfiguration,
							ApplyConfiguration: &v1alpha1.ApplyConfiguration{
								Expression: "fake string",
							},
						},
					},
				},
			},
			expected: &v1alpha1.MutatingAdmissionPolicy{
				Spec: v1alpha1.MutatingAdmissionPolicySpec{
					MatchConstraints: &v1alpha1.MatchResources{
						MatchPolicy:       &equivalent,
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
					},
					FailurePolicy:      &fail,
					ReinvocationPolicy: never,
					Mutations: []v1alpha1.Mutation{
						{
							PatchType: v1alpha1.PatchTypeApplyConfiguration,
							ApplyConfiguration: &v1alpha1.ApplyConfiguration{
								Expression: "fake string",
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := test.original
			expected := test.expected
			legacyscheme.Scheme.Default(original)
			if !apiequality.Semantic.DeepEqual(original, expected) {
				t.Error(cmp.Diff(expected, original))
			}
		})
	}
}

// TestDefaultAdmissionPolicyBinding 验证 v1alpha1 版本策略绑定经过默认化后，
// 参数引用的 parameterNotFoundAction 与匹配资源策略被正确填充。
func TestDefaultAdmissionPolicyBinding(t *testing.T) {
	denyAction := v1alpha1.DenyAction
	equivalent := v1alpha1.Equivalent

	tests := []struct {
		name     string
		original runtime.Object
		expected runtime.Object
	}{
		{
			name: "ValidatingAdmissionPolicyBinding.ParamRef.ParameterNotFoundAction",
			original: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					ParamRef: &v1alpha1.ParamRef{},
				},
			},
			expected: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					ParamRef: &v1alpha1.ParamRef{
						ParameterNotFoundAction: &denyAction,
					},
				},
			},
		},
		{
			name: "ValidatingAdmissionPolicyBinding.MatchResources",
			original: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					MatchResources: &v1alpha1.MatchResources{},
				},
			},
			expected: &v1alpha1.ValidatingAdmissionPolicyBinding{
				Spec: v1alpha1.ValidatingAdmissionPolicyBindingSpec{
					MatchResources: &v1alpha1.MatchResources{
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						MatchPolicy:       &equivalent,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := test.original
			expected := test.expected
			legacyscheme.Scheme.Default(original)
			if !apiequality.Semantic.DeepEqual(original, expected) {
				t.Error(cmp.Diff(expected, original))
			}
		})
	}
}
