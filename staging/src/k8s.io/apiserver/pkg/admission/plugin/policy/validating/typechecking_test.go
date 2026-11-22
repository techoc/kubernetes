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

package validating

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// scheme 注册测试所需的 API 类型，供 RESTMapper 推断 GVK。
var (
	scheme *runtime.Scheme = func() *runtime.Scheme {
		res := runtime.NewScheme()
		if err := v1.AddToScheme(res); err != nil {
			panic(err)
		}

		if err := fake.AddToScheme(res); err != nil {
			panic(err)
		}

		return res
	}()
)

// must3 用于测试中“确定成功”的三返回值调用，失败直接 panic 以缩短样板代码。
func must3[T any, I any](val T, _ I, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

// TestExtractTypeNames 验证从策略的 matchConstraints 推导出需要类型检查的 GVK 列表。
//
// 关键规则：通配资源（*、pods/*）无法静态确定类型，返回 nil 表示跳过检查；
// 结果必须按 GVK 排序并去重，保证检查结果稳定。
func TestExtractTypeNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policy   *v1.ValidatingAdmissionPolicy
		expected []schema.GroupVersionKind // must be sorted
	}{
		{
			name:     "empty",
			policy:   &v1.ValidatingAdmissionPolicy{},
			expected: nil,
		},
		{
			name: "specific",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"v1"},
								Resources:   []string{"deployments"},
							},
						},
					},
				}},
			}},
			expected: []schema.GroupVersionKind{{
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			}},
		},
		{
			name: "multiple",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"v1"},
								Resources:   []string{"deployments"},
							},
						},
					}, {
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{""},
								APIVersions: []string{"v1"},
								Resources:   []string{"pods"},
							},
						},
					},
				}},
			}},
			expected: []schema.GroupVersionKind{
				{
					Version: "v1",
					Kind:    "Pod",
				}, {
					Group:   "apps",
					Version: "v1",
					Kind:    "Deployment",
				}},
		},
		{
			name: "all resources",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"v1"},
								Resources:   []string{"*"},
							},
						},
					},
				}},
			}},
			expected: nil,
		},
		{
			name: "sub resources",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"v1"},
								Resources:   []string{"pods/*"},
							},
						},
					},
				}},
			}},
			expected: nil,
		},
		{
			name: "mixtures",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"v1"},
								Resources:   []string{"deployments"},
							},
						},
					},
					{
						RuleWithOperations: v1.RuleWithOperations{
							Rule: v1.Rule{
								APIGroups:   []string{"apps"},
								APIVersions: []string{"*"},
								Resources:   []string{"deployments"},
							},
						},
					},
				}},
			}},
			expected: []schema.GroupVersionKind{{
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typeChecker := buildTypeChecker(nil)
			got := typeChecker.typesToCheck(tc.policy)
			if !reflect.DeepEqual(tc.expected, got) {
				t.Errorf("expected %v but got %v", tc.expected, got)
			}
		})
	}
}

// TestTypeCheck 验证 CEL 表达式对匹配资源 schema 的类型检查，产出带 fieldRef 的告警。
//
// 覆盖：未解析到 schema 时静默跳过、未定义字段、类型不匹配、params 引用、
// messageExpression、authorizer 调用、additionalProperties、以及 variables 的
// 类型推导与“变量自身错误不在类型检查阶段报告”的约定。
func TestTypeCheck(t *testing.T) {
	// deploymentPolicy 是基准策略：校验表达式访问 object.foo，匹配 apps/v1 deployments。
	deploymentPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "object.foo == 'bar'",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}
	noTypeSchemaPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "true",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}

	deploymentPolicyWithBadMessageExpression := deploymentPolicy.DeepCopy()
	// messageExpression 期望返回字符串，这里刻意返回 int，用于验证它也被类型检查。
	deploymentPolicyWithBadMessageExpression.Spec.Validations[0].MessageExpression = "object.foo + 114514" // confusion

	multiExpressionPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "object.foo == 'bar'",
			},
			{
				Expression: "object.bar == 'foo'",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}
	paramsRefPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		ParamKind: &v1.ParamKind{
			APIVersion: "v1",
			Kind:       "DoesNotMatter",
		},
		Validations: []v1.Validation{
			{
				Expression: "object.foo == params.bar",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}
	authorizerPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "authorizer.group('').resource('endpoints').check('create').allowed()",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}
	authorizerInvalidPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "authorizer.allowed()",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
		}},
	}}

	reproducerPolicy := &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
		Validations: []v1.Validation{
			{
				Expression: "has(object.spec.text)",
			},
		},
		MatchConstraints: &v1.MatchResources{ResourceRules: []v1.NamedRuleWithOperations{
			{
				RuleWithOperations: v1.RuleWithOperations{
					Rule: v1.Rule{
						APIGroups:   []string{"example.com"},
						APIVersions: []string{"v1alpha1"},
						Resources:   []string{"reproducers"},
					},
				},
			},
		}},
	}}

	for _, tc := range []struct {
		name           string
		schemaToReturn *spec.Schema
		policy         *v1.ValidatingAdmissionPolicy
		assertions     []assertionFunc
	}{
		{
			name:       "empty",
			policy:     &v1.ValidatingAdmissionPolicy{},
			assertions: []assertionFunc{toBeEmpty},
		},
		{
			name:           "unresolved schema",
			policy:         deploymentPolicy,
			schemaToReturn: nil,
			// 无法解析 schema 时无法做类型检查，必须静默放过而不是报错阻塞策略创建。
			assertions: []assertionFunc{toBeEmpty},
		},
		{
			name:   "passed check",
			policy: deploymentPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{toBeEmpty},
		},
		{
			name:   "undefined field",
			policy: deploymentPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"bar": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveFieldRef("spec.validations[0].expression"),
				toContain(`undefined field 'foo'`),
			},
		},
		{
			name:   "field type mismatch",
			policy: deploymentPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.Int64Property(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveFieldRef("spec.validations[0].expression"),
				toContain(`found no matching overload`),
			},
		},
		{
			name:   "params",
			policy: paramsRefPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveFieldRef("spec.validations[0].expression"),
				toContain(`undefined field 'bar'`),
			},
		},
		{
			name: "params with untyped schema",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				ParamKind: &v1.ParamKind{
					APIVersion: "v1",
					Kind:       "Config",
				},
				Validations: []v1.Validation{
					{
						Expression: "params != null",
					},
				},
				MatchConstraints: deploymentPolicy.Spec.MatchConstraints,
			}},
			schemaToReturn: &spec.Schema{},
			assertions:     []assertionFunc{toBeEmpty},
		},
		{
			name:   "multiple expressions",
			policy: multiExpressionPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{
				// 只有第二条表达式出错，fieldRef 必须精确指向它，便于用户定位。
				toHaveFieldRef("spec.validations[1].expression"), // expressions[0] is okay, [1] is wrong
				toHaveLengthOf(1),
			},
		},
		{
			name:   "message expressions",
			policy: deploymentPolicyWithBadMessageExpression,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveFieldRef("spec.validations[0].messageExpression"),
				toHaveLengthOf(1),
			},
		},
		{
			name:   "authorizer",
			policy: authorizerPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{toBeEmpty},
		},
		{
			name:   "authorizer invalid",
			policy: authorizerInvalidPolicy,
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.StringProperty(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveFieldRef("spec.validations[0].expression"),
				toHaveLengthOf(1),
				toContain("found no matching overload for 'allowed' applied to 'kubernetes.authorization.Authorizer"),
			},
		},
		{
			name:   "additionalProperties: true",
			policy: reproducerPolicy,
			// additionalProperties 为 true 时任意字段名都合法，不应产生 undefined field 告警。
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"spec": {
							SchemaProps: spec.SchemaProps{
								Type: []string{"object"},
								Properties: map[string]spec.Schema{
									"text": *spec.StringProperty(),
								},
							},
						},
						"status": {
							SchemaProps: spec.SchemaProps{
								Type: []string{"object"},
								Properties: map[string]spec.Schema{
									"problematicProperty": {
										SchemaProps: spec.SchemaProps{
											Type:                 []string{"object"},
											AdditionalProperties: &spec.SchemaOrBool{Allows: true},
										},
									},
								},
							},
						},
					},
				},
			},
			assertions: []assertionFunc{toBeEmpty},
		},
		{
			name:           "schema without type",
			policy:         noTypeSchemaPolicy,
			schemaToReturn: &spec.Schema{},
			assertions:     []assertionFunc{toBeEmpty},
		},
		{
			name: "variables valid",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				Variables: []v1.Variable{
					{
						Name:       "works",
						Expression: "true",
					},
				},
				Validations: []v1.Validation{
					{
						Expression: "variables.works",
					},
				},
				MatchConstraints: deploymentPolicy.Spec.MatchConstraints,
			},
			},
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.Int64Property(),
					},
				},
			},
			assertions: []assertionFunc{toHaveLengthOf(0)},
		},
		{
			name: "variables missing field",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				Variables: []v1.Variable{
					{
						Name:       "works",
						Expression: "true",
					},
				},
				Validations: []v1.Validation{
					{
						Expression: "variables.nonExisting",
					},
				},
				MatchConstraints: deploymentPolicy.Spec.MatchConstraints,
			},
			},
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.Int64Property(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveLengthOf(1),
				toHaveFieldRef("spec.validations[0].expression"),
				toContain("undefined field 'nonExisting'"),
			},
		},
		{
			name: "variables field wrong type",
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				Variables: []v1.Variable{
					{
						Name:       "name",
						Expression: "'something'",
					},
				},
				Validations: []v1.Validation{
					{
						Expression: "variables.name == object.foo", // foo is int64
					},
				},
				MatchConstraints: deploymentPolicy.Spec.MatchConstraints,
			},
			},
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.Int64Property(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveLengthOf(1),
				toHaveFieldRef("spec.validations[0].expression"),
				toContain("found no matching overload for '_==_' applied to '(string, int)"),
			},
		},
		{
			name: "error in variables, not reported during type checking.",
			// 变量表达式自身的类型错误在策略编译阶段单独报告，这里只检查
			// 校验表达式对变量推导类型的兼容性，避免同一问题重复告警。
			policy: &v1.ValidatingAdmissionPolicy{Spec: v1.ValidatingAdmissionPolicySpec{
				Variables: []v1.Variable{
					{
						Name:       "name",
						Expression: "object.foo == 'str'",
					},
				},
				Validations: []v1.Validation{
					{
						Expression: "variables.name == object.foo", // foo is int64
					},
				},
				MatchConstraints: deploymentPolicy.Spec.MatchConstraints,
			},
			},
			schemaToReturn: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"foo": *spec.Int64Property(),
					},
				},
			},
			assertions: []assertionFunc{
				toHaveLengthOf(0),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typeChecker := buildTypeChecker(tc.schemaToReturn)
			warnings := typeChecker.Check(tc.policy)
			for _, a := range tc.assertions {
				a(warnings, t)
			}
		})
	}
}

// buildTypeChecker 构造一个使用假 schema 解析器的类型检查器；
// RESTMapper 只注册测试涉及的 Pod 与 Deployment。
func buildTypeChecker(schemaToReturn *spec.Schema) *TypeChecker {
	restMapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{
			Group:   "",
			Version: "v1",
		},
	})
	restMapper.Add(must3(scheme.ObjectKinds(&corev1.Pod{}))[0], meta.RESTScopeRoot)
	restMapper.Add(must3(scheme.ObjectKinds(&appsv1.Deployment{}))[0], meta.RESTScopeRoot)

	return &TypeChecker{
		SchemaResolver: &fakeSchemaResolver{schemaToReturn: schemaToReturn},
		RestMapper:     restMapper,
	}
}

// fakeSchemaResolver 返回固定 schema；未设置时模拟“schema 不存在”。
type fakeSchemaResolver struct {
	schemaToReturn *spec.Schema
}

// ResolveSchema 实现 resolver.SchemaResolver。
func (r *fakeSchemaResolver) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	if r.schemaToReturn == nil {
		return nil, fmt.Errorf("cannot resolve for %v: %w", gvk, resolver.ErrSchemaNotFound)
	}
	return r.schemaToReturn, nil
}

// toBeEmpty 断言没有产生任何告警。
func toBeEmpty(warnings []v1.ExpressionWarning, t *testing.T) {
	if len(warnings) != 0 {
		t.Fatalf("expected empty but got %v", warnings)
	}
}

// toContain 断言每条告警文本都包含指定子串。
func toContain(substring string) func(warnings []v1.ExpressionWarning, t *testing.T) {
	return func(warnings []v1.ExpressionWarning, t *testing.T) {
		if len(warnings) == 0 {
			t.Errorf("expected containing %q but got empty", substring)
		}
		for i, w := range warnings {
			if !strings.Contains(w.Warning, substring) {
				t.Errorf("warning %d does not contain %q, got %v", i, substring, w)
			}
		}
	}
}

// toHaveLengthOf 断言告警数量。
func toHaveLengthOf(expected int) func(warnings []v1.ExpressionWarning, t *testing.T) {
	return func(warnings []v1.ExpressionWarning, t *testing.T) {
		got := len(warnings)
		if expected != got {
			t.Errorf("expect warnings to have length of %d, but got %d", expected, got)
		}
	}
}

// toHaveFieldRef 断言告警按顺序指向指定的字段路径。
func toHaveFieldRef(paths ...string) func(warnings []v1.ExpressionWarning, t *testing.T) {
	return func(warnings []v1.ExpressionWarning, t *testing.T) {
		if len(paths) != len(warnings) {
			t.Errorf("expect warnings to have length of %d, but got %d", len(paths), len(warnings))
		}
		for i := range paths {
			if paths[i] != warnings[i].FieldRef {
				t.Errorf("wrong fieldRef at %d, expected %q but got %q", i, paths[i], warnings[i].FieldRef)
			}
		}
	}
}

// assertionFunc 是对类型检查告警的断言。
type assertionFunc func(warnings []v1.ExpressionWarning, t *testing.T)
