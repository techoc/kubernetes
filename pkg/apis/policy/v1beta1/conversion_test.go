/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta1

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// TestConversion 通过 scheme 校验 v1beta1 与内部 PodDisruptionBudget 的双向转换。
func TestConversion(t *testing.T) {
	testcases := []struct {
		Name      string
		In        runtime.Object
		Out       runtime.Object
		ExpectOut runtime.Object
		ExpectErr string
	}{
		{
			// 空 selector 在内部表示中会补 Exists 条件，避免误选全部 Pod。
			Name: "v1beta1 to internal with empty selector",
			In: &v1beta1.PodDisruptionBudget{
				Spec: v1beta1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{},
				},
			},
			Out: &policy.PodDisruptionBudget{},
			ExpectOut: &policy.PodDisruptionBudget{
				Spec: policy.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match",
								Operator: metav1.LabelSelectorOpExists,
							},
						},
					},
				},
			},
		},
		{
			// nil selector 表示不选择任何对象，转换后应保持为 nil。
			Name:      "v1beta1 to internal with nil selector",
			In:        &v1beta1.PodDisruptionBudget{},
			Out:       &policy.PodDisruptionBudget{},
			ExpectOut: &policy.PodDisruptionBudget{},
		},
		{
			// 已有的 MatchLabels 应原样保留。
			Name: "v1 to internal with existing selector",
			In: &v1beta1.PodDisruptionBudget{
				Spec: v1beta1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			Out: &policy.PodDisruptionBudget{},
			ExpectOut: &policy.PodDisruptionBudget{
				Spec: policy.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		},
		{
			// 内部表示中的 DoesNotExist 条件在转回 v1beta1 时应被丢弃。
			Name: "v1beta1 to internal with existing pdb selector",
			In: &v1beta1.PodDisruptionBudget{
				Spec: v1beta1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match",
								Operator: metav1.LabelSelectorOpDoesNotExist,
							},
						},
					},
				},
			},
			Out: &policy.PodDisruptionBudget{},
			ExpectOut: &policy.PodDisruptionBudget{
				Spec: policy.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
						MatchExpressions: []metav1.LabelSelectorRequirement{},
					},
				},
			},
		},
		{
			// 反向转换时同样会补出 Exists 条件。
			Name: "internal to v1beta1 with empty selector",
			In: &policy.PodDisruptionBudget{
				Spec: policy.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{},
				},
			},
			Out: &v1beta1.PodDisruptionBudget{},
			ExpectOut: &v1beta1.PodDisruptionBudget{
				Spec: v1beta1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match",
								Operator: metav1.LabelSelectorOpDoesNotExist,
							},
						},
					},
				},
			},
		},
		{
			// nil selector 双向转换后仍应为 nil。
			Name:      "internal to v1beta1 with nil selector",
			In:        &policy.PodDisruptionBudget{},
			Out:       &v1beta1.PodDisruptionBudget{},
			ExpectOut: &v1beta1.PodDisruptionBudget{},
		},
		{
			// 已有标签的双向转换必须保持相等。
			Name: "internal to v1beta1 with existing selector",
			In: &policy.PodDisruptionBudget{
				Spec: policy.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			Out: &v1beta1.PodDisruptionBudget{},
			ExpectOut: &v1beta1.PodDisruptionBudget{
				Spec: v1beta1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		},
	}

	// 同时注册内部与外部类型，scheme.Convert 才能找到转换路径。
	scheme := runtime.NewScheme()
	if err := policy.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 逐条转换并断言预期错误与结果。
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			err := scheme.Convert(tc.In, tc.Out, nil)
			if err != nil {
				if len(tc.ExpectErr) == 0 {
					t.Fatalf("unexpected error %v", err)
				}
				if !strings.Contains(err.Error(), tc.ExpectErr) {
					t.Fatalf("expected error %s, got %v", tc.ExpectErr, err)
				}
				return
			}
			if len(tc.ExpectErr) > 0 {
				t.Fatalf("expected error %s, got none", tc.ExpectErr)
			}
			if !reflect.DeepEqual(tc.Out, tc.ExpectOut) {
				t.Fatalf("unexpected result:\n %s", cmp.Diff(tc.ExpectOut, tc.Out))
			}
		})
	}
}
