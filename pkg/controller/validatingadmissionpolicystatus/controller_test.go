/*
Copyright 2023 The Kubernetes Authors.

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

package validatingadmissionpolicystatus

import (
	"context"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kubernetes/pkg/generated/openapi"
)

// TestTypeChecking 验证控制器会对策略表达式做类型检查，并把每个表达式的告警
// 连同其字段路径写入 status；同时覆盖无告警、类型不匹配及未知字段等场景。
func TestTypeChecking(t *testing.T) {
	for _, tc := range []struct {
		name           string
		policy         *admissionregistrationv1.ValidatingAdmissionPolicy
		assertFieldRef func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) // 校验 warning.fieldRef
		assertWarnings func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) // 校验 warning.warning
	}{
		{
			name: "deployment with correct expression",
			policy: withGVRMatch([]string{"apps"}, []string{"v1"}, []string{"deployments"}, withValidations([]admissionregistrationv1.Validation{
				{
					Expression: "object.spec.replicas > 1",
				},
			}, makePolicy("replicated-deployment"))),
			assertFieldRef: toHaveLengthOf(0),
			assertWarnings: toHaveLengthOf(0),
		},
		{
			name: "deployment with type confusion",
			policy: withGVRMatch([]string{"apps"}, []string{"v1"}, []string{"deployments"}, withValidations([]admissionregistrationv1.Validation{
				{
					Expression: "object.spec.replicas < 100", // 该表达式能通过类型检查
				},
				{
					Expression: "object.spec.replicas > '1'", // '1' 应为 int
				},
			}, makePolicy("confused-deployment"))),
			assertFieldRef: toBe("spec.validations[1].expression"),
			assertWarnings: toHaveSubstring(`found no matching overload for '_>_' applied to '(int, string)'`),
		},
		{
			name: "two expressions different type checking errors",
			policy: withGVRMatch([]string{"apps"}, []string{"v1"}, []string{"deployments"}, withValidations([]admissionregistrationv1.Validation{
				{
					Expression: "object.spec.nonExistingFirst > 1",
				},
				{
					Expression: "object.spec.replicas > '1'", // '1' 应为 int
				},
			}, makePolicy("confused-deployment"))),
			assertFieldRef: toBe("spec.validations[0].expression", "spec.validations[1].expression"),
			assertWarnings: toHaveSubstring(
				"undefined field 'nonExistingFirst'",
				`found no matching overload for '_>_' applied to '(int, string)'`,
			),
		},
		{
			name: "one expression, two warnings",
			policy: withGVRMatch([]string{"apps"}, []string{"v1"}, []string{"deployments"}, withValidations([]admissionregistrationv1.Validation{
				{
					Expression: "object.spec.replicas < 100", // 该表达式能通过类型检查
				},
				{
					Expression: "object.spec.replicas > '1' && object.spec.nonExisting == 1",
				},
			}, makePolicy("confused-deployment"))),
			assertFieldRef: toBe("spec.validations[1].expression"),
			assertWarnings: toHaveMultipleSubstrings([]string{"undefined field 'nonExisting'", `found no matching overload for '_>_' applied to '(int, string)'`}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			policy := tc.policy.DeepCopy()
			policy.ObjectMeta.Generation = 1 // fake 存储不会自动设置 Generation
			client := fake.NewSimpleClientset(policy)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			typeChecker := &validatingadmissionpolicy.TypeChecker{
				SchemaResolver: resolver.NewDefinitionsSchemaResolver(openapi.GetOpenAPIDefinitions, scheme.Scheme),
				RestMapper:     testrestmapper.TestOnlyStaticRESTMapper(scheme.Scheme),
			}
			controller, err := NewController(
				informerFactory.Admissionregistration().V1().ValidatingAdmissionPolicies(),
				client.AdmissionregistrationV1().ValidatingAdmissionPolicies(),
				typeChecker,
			)
			if err != nil {
				t.Fatalf("cannot create controller: %v", err)
			}
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())
			go controller.Run(ctx, 1)
			err = wait.PollUntilContextCancel(ctx, time.Second, false, func(ctx context.Context) (done bool, err error) {
				name := policy.Name
				// 轮询等待 TypeChecking 被写入：该字段非空才意味着控制器已完成类型检查，
				// 过早断言会读到空状态导致假失败。
				updated, err := client.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					// API 调用失败属于真实错误，交给轮询机制终止，避免无限等待。
					return false, err
				}
				if updated.Status.TypeChecking != nil {
					policy = updated
					return true, nil
				}
				// 状态尚未就绪不是错误，继续下一轮轮询。
				return false, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			// 分别校验字段路径和告警内容，二者共同决定用户能否定位并修复表达式问题。
			tc.assertFieldRef(policy.Status.TypeChecking.ExpressionWarnings, t)
			tc.assertWarnings(policy.Status.TypeChecking.ExpressionWarnings, t)
			if err != nil {
				t.Fatalf("failed to initialize controller: %v", err)
			}
		})
	}

}

// toBe 生成断言函数，按给定顺序逐一匹配告警的 FieldRef。
// 先校验数量再逐个比对，避免索引越界并区分“多报”与“少报”问题。
func toBe(expected ...string) func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
	return func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
		// 数量不符时后续逐项比较已无意义，直接终止该子测试。
		if len(expected) != len(warnings) {
			t.Fatalf("mismatched length, expect %d, got %d", len(expected), len(warnings))
		}
		for i := range expected {
			if expected[i] != warnings[i].FieldRef {
				t.Errorf("expected %q but got %q", expected[i], warnings[i].FieldRef)
			}
		}
	}
}

// toHaveSubstring 生成断言函数，逐条检查告警文本是否包含预期子串。
// 用子串而非全等匹配，是因为编译器错误文案可能随版本调整，只需关心关键语义。
func toHaveSubstring(substrings ...string) func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
	return func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
		if len(substrings) != len(warnings) {
			t.Fatalf("mismatched length, expect %d, got %d", len(substrings), len(warnings))
		}
		for i := range substrings {
			if !strings.Contains(warnings[i].Warning, substrings[i]) {
				t.Errorf("missing expected substring %q in %v", substrings[i], warnings[i])
			}
		}
	}
}

// toHaveMultipleSubstrings 生成断言函数，检查单条告警中同时包含多个预期子串。
// 用于一个表达式触发多个类型错误的场景，确保错误没有被合并丢弃。
func toHaveMultipleSubstrings(substrings ...[]string) func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
	return func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
		if len(substrings) != len(warnings) {
			t.Fatalf("mismatched length, expect %d, got %d", len(substrings), len(warnings))
		}
		for i, expectedSubstrings := range substrings {
			for _, s := range expectedSubstrings {
				if !strings.Contains(warnings[i].Warning, s) {
					t.Errorf("missing expected substring %q in %v", substrings[i], warnings[i])
				}
			}
		}
	}
}

// toHaveLengthOf 生成断言函数，校验告警条数恰好为 n。
// 用于无告警场景，防止控制器产生误报。
func toHaveLengthOf(n int) func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
	return func(warnings []admissionregistrationv1.ExpressionWarning, t *testing.T) {
		if n != len(warnings) {
			t.Fatalf("mismatched length, expect %d, got %d", n, len(warnings))
		}
	}
}

// withGVRMatch 为策略设置按 Group/Version/Resource 匹配的约束，
// 覆盖全部操作，使类型检查能解析到目标资源的 OpenAPI schema。
func withGVRMatch(groups []string, versions []string, resources []string, policy *admissionregistrationv1.ValidatingAdmissionPolicy) *admissionregistrationv1.ValidatingAdmissionPolicy {
	policy.Spec.MatchConstraints = &admissionregistrationv1.MatchResources{
		ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
			{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{
						"*",
					},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   groups,
						APIVersions: versions,
						Resources:   resources,
					},
				},
			},
		},
	}
	return policy
}

// withValidations 将待检查的 CEL 表达式列表写入策略的 Spec.Validations。
func withValidations(validations []admissionregistrationv1.Validation, policy *admissionregistrationv1.ValidatingAdmissionPolicy) *admissionregistrationv1.ValidatingAdmissionPolicy {
	policy.Spec.Validations = validations
	return policy
}

// makePolicy 创建一个仅带名称的最小策略对象，后续由各用例按需填充字段。
func makePolicy(name string) *admissionregistrationv1.ValidatingAdmissionPolicy {
	return &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}
