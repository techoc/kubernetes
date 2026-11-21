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

package validation

import (
	"fmt"
	"testing"
	"time"

	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// TestValidatePodDisruptionBudgetSpec 验证同时设置 minAvailable 与 maxUnavailable 的 PDB 配置会被拒绝。
// 这两个字段语义互斥——一个声明最少需保留的 Pod，另一个声明最多可中断的 Pod，同时设置会产生冲突。
func TestValidatePodDisruptionBudgetSpec(t *testing.T) {
	minAvailable := intstr.FromString("0%")
	maxUnavailable := intstr.FromString("10%")

	spec := policy.PodDisruptionBudgetSpec{
		MinAvailable:   &minAvailable,
		MaxUnavailable: &maxUnavailable,
	}
	errs := ValidatePodDisruptionBudgetSpec(spec, PodDisruptionBudgetValidationOptions{true}, field.NewPath("foo"))
	// 期望校验失败：两个互斥字段同时存在时不应返回空错误列表。
	if len(errs) == 0 {
		t.Errorf("unexpected success for %v", spec)
	}
}

// TestValidateMinAvailablePodDisruptionBudgetSpec 验证 minAvailable 的合法与非法取值。
// 百分比须为 0~100 的整数百分比，整数形式须非负；小数百分比、非数字字符串及负数均应被拒绝。
func TestValidateMinAvailablePodDisruptionBudgetSpec(t *testing.T) {
	successCases := []intstr.IntOrString{
		intstr.FromString("0%"),
		intstr.FromString("1%"),
		intstr.FromString("100%"),
		intstr.FromInt32(0),
		intstr.FromInt32(1),
		intstr.FromInt32(100),
	}
	// 逐个验证合法取值，任何预期成功却报错的用例都说明校验过严。
	for _, c := range successCases {
		spec := policy.PodDisruptionBudgetSpec{
			MinAvailable: &c,
		}
		errs := ValidatePodDisruptionBudgetSpec(spec, PodDisruptionBudgetValidationOptions{true}, field.NewPath("foo"))
		if len(errs) != 0 {
			t.Errorf("unexpected failure %v for %v", errs, spec)
		}
	}

	failureCases := []intstr.IntOrString{
		intstr.FromString("1.1%"),
		intstr.FromString("nope"),
		intstr.FromString("-1%"),
		intstr.FromString("101%"),
		intstr.FromInt32(-1),
	}
	// 逐个验证非法取值，防止校验过松放过无效配置。
	for _, c := range failureCases {
		spec := policy.PodDisruptionBudgetSpec{
			MinAvailable: &c,
		}
		errs := ValidatePodDisruptionBudgetSpec(spec, PodDisruptionBudgetValidationOptions{true}, field.NewPath("foo"))
		if len(errs) == 0 {
			t.Errorf("unexpected success for %v", spec)
		}
	}
}

// TestValidateMinAvailablePodAndMaxUnavailableDisruptionBudgetSpec 验证百分比形式的 minAvailable
// 与整数形式的 maxUnavailable 同样互斥，确保无论取值类型如何，双字段配置都会被拒绝。
func TestValidateMinAvailablePodAndMaxUnavailableDisruptionBudgetSpec(t *testing.T) {
	c1 := intstr.FromString("10%")
	c2 := intstr.FromInt32(1)

	spec := policy.PodDisruptionBudgetSpec{
		MinAvailable:   &c1,
		MaxUnavailable: &c2,
	}
	errs := ValidatePodDisruptionBudgetSpec(spec, PodDisruptionBudgetValidationOptions{true}, field.NewPath("foo"))
	// 期望校验失败：百分比与整数形式同样互斥，双字段配置应被拒绝。
	if len(errs) == 0 {
		t.Errorf("unexpected success for %v", spec)
	}
}

// TestValidateUnhealthyPodEvictionPolicyDisruptionBudgetSpec 验证 unhealthyPodEvictionPolicy 字段的校验。
// 未设置（nil）或使用合法枚举值应通过；空字符串和未知枚举值应被拒绝。
func TestValidateUnhealthyPodEvictionPolicyDisruptionBudgetSpec(t *testing.T) {
	c1 := intstr.FromString("10%")
	alwaysAllowPolicy := policy.AlwaysAllow
	invalidPolicy := policy.UnhealthyPodEvictionPolicyType("Invalid")

	testCases := []struct {
		name      string
		pdbSpec   policy.PodDisruptionBudgetSpec
		expectErr bool
	}{{
		// nil 表示不启用该策略，属于合法状态。
		name: "valid nil UnhealthyPodEvictionPolicy",
		pdbSpec: policy.PodDisruptionBudgetSpec{
			MinAvailable:               &c1,
			UnhealthyPodEvictionPolicy: nil,
		},
		expectErr: false,
	}, {
		// AlwaysAllow 是合法的枚举值，应被接受。
		name: "valid UnhealthyPodEvictionPolicy",
		pdbSpec: policy.PodDisruptionBudgetSpec{
			MinAvailable:               &c1,
			UnhealthyPodEvictionPolicy: &alwaysAllowPolicy,
		},
		expectErr: false,
	}, {
		// 空字符串不是合法策略，等价于未指定但显式给出时应报错。
		name: "empty UnhealthyPodEvictionPolicy",
		pdbSpec: policy.PodDisruptionBudgetSpec{
			MinAvailable:               &c1,
			UnhealthyPodEvictionPolicy: new(policy.UnhealthyPodEvictionPolicyType),
		},
		expectErr: true,
	}, {
		// 未知枚举值应被拒绝，防止非法策略进入存储。
		name: "invalid UnhealthyPodEvictionPolicy",
		pdbSpec: policy.PodDisruptionBudgetSpec{
			MinAvailable:               &c1,
			UnhealthyPodEvictionPolicy: &invalidPolicy,
		},
		expectErr: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			errs := ValidatePodDisruptionBudgetSpec(tc.pdbSpec, PodDisruptionBudgetValidationOptions{true}, field.NewPath("foo"))
			if len(errs) == 0 && tc.expectErr {
				t.Errorf("unexpected success for %v", tc.pdbSpec)
			}
			if len(errs) != 0 && !tc.expectErr {
				t.Errorf("unexpected failure for %v", tc.pdbSpec)
			}
		})
	}
}

// TestValidatePodDisruptionBudgetStatus 验证 PDB status 字段的校验，并按 API 版本区分预期。
// internal 版本对负数计数字段和重复 Condition 有更严格的要求，v1beta1 为兼容旧行为有所放宽。
func TestValidatePodDisruptionBudgetStatus(t *testing.T) {
	const expectNoErrors = false
	const expectErrors = true
	testCases := []struct {
		name                string
		pdbStatus           policy.PodDisruptionBudgetStatus
		expectErrForVersion map[schema.GroupVersion]bool
	}{{
		name: "DisruptionsAllowed: 10",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			DisruptionsAllowed: 10,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectNoErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		name: "CurrentHealthy: 5",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			CurrentHealthy: 5,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectNoErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		name: "DesiredHealthy: 3",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			DesiredHealthy: 3,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectNoErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		name: "ExpectedPods: 2",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			ExpectedPods: 2,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectNoErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 负数 DisruptionsAllowed 在 internal 版本非法，v1beta1 为兼容旧数据不报错。
		name: "DisruptionsAllowed: -10",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			DisruptionsAllowed: -10,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 负数 CurrentHealthy 同样只在 internal 版本被拒绝。
		name: "CurrentHealthy: -5",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			CurrentHealthy: -5,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 负数 DesiredHealthy 同样只在 internal 版本被拒绝。
		name: "DesiredHealthy: -3",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			DesiredHealthy: -3,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 负数 ExpectedPods 同样只在 internal 版本被拒绝。
		name: "ExpectedPods: -2",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			ExpectedPods: -2,
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 带有完整字段（含 Reason/Message/ObservedGeneration）的 Condition 应通过校验。
		name: "Conditions valid",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			Conditions: []metav1.Condition{{
				Type:   policyv1beta1.DisruptionAllowedCondition,
				Status: metav1.ConditionTrue,
				LastTransitionTime: metav1.Time{
					Time: time.Now().Add(-5 * time.Minute),
				},
				Reason:             policyv1beta1.SufficientPodsReason,
				Message:            "message",
				ObservedGeneration: 3,
			}},
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectNoErrors,
			policyv1beta1.SchemeGroupVersion: expectNoErrors,
		},
	}, {
		// 两个 Type 相同的 Condition 违反唯一性约束，两个版本都应报错。
		name: "Conditions not valid",
		pdbStatus: policy.PodDisruptionBudgetStatus{
			Conditions: []metav1.Condition{{
				Type:   policyv1beta1.DisruptionAllowedCondition,
				Status: metav1.ConditionTrue,
			}, {
				Type:   policyv1beta1.DisruptionAllowedCondition,
				Status: metav1.ConditionFalse,
			}},
		},
		expectErrForVersion: map[schema.GroupVersion]bool{
			policy.SchemeGroupVersion:        expectErrors,
			policyv1beta1.SchemeGroupVersion: expectErrors,
		},
	}}

	for _, tc := range testCases {
		for apiVersion, expectErrors := range tc.expectErrForVersion {
			t.Run(fmt.Sprintf("apiVersion: %s, %s", apiVersion.String(), tc.name), func(t *testing.T) {
				errors := ValidatePodDisruptionBudgetStatusUpdate(tc.pdbStatus, policy.PodDisruptionBudgetStatus{},
					field.NewPath("status"), apiVersion)
				errCount := len(errors)

				if errCount > 0 && !expectErrors {
					t.Errorf("unexpected failure %v for %v", errors, tc.pdbStatus)
				}

				if errCount == 0 && expectErrors {
					t.Errorf("expected errors but didn't one for %v", tc.pdbStatus)
				}
			})
		}
	}
}

// TestIsValidSysctlPattern 验证 sysctl 白名单模式的格式规则。
// 模式允许小写字母、数字、点、连字符、下划线以及作为通配符的 *，
// 但不允许空串、大写字母、非 ASCII 字符、连续分隔符、不合法的通配符位置或超长名称。
func TestIsValidSysctlPattern(t *testing.T) {
	valid := []string{
		"a.b.c.d",
		"a",
		"a_b",
		"a-b",
		"abc",
		"abc.def",
		"*",
		"a.*",
		"*",
		"abc*",
		"a.abc*",
		"a.b.*",
		"a/b/c/d",
		"a/*",
		"a/b/*",
		"a.b/c*",
		"a.b/c.d",
		"a/b.c/d",
	}
	invalid := []string{
		"",
		"ä",
		"a_",
		"_",
		"_a",
		"_a._b",
		"__",
		"-",
		".",
		"a.",
		".a",
		"a.b.",
		"a*.b",
		"a*b",
		"*a",
		"Abc",
		"/",
		"a/",
		"/a",
		"a*/b",
		func(n int) string {
			x := make([]byte, n)
			for i := range x {
				x[i] = byte('a')
			}
			return string(x)
		}(256),
	}
	// 合法模式集合中若出现校验失败，说明合法语法被误拒。
	for _, s := range valid {
		if !IsValidSysctlPattern(s) {
			t.Errorf("%q expected to be a valid sysctl pattern", s)
		}
	}
	// 非法模式集合中若出现校验通过，说明非法语法被放过。
	for _, s := range invalid {
		if IsValidSysctlPattern(s) {
			t.Errorf("%q expected to be an invalid sysctl pattern", s)
		}
	}
}
