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

package cel

import (
	"context"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// testVariable 是 NamedExpressionAccessor 的最小实现，用于在测试中
// 声明一个组合变量及其 CEL 表达式。返回类型固定为 dyn，便于覆盖任意值。
type testVariable struct {
	name       string
	expression string
}

// GetExpression 返回该变量的 CEL 表达式。
func (t *testVariable) GetExpression() string {
	return t.expression
}

// ReturnTypes 返回 dyn，允许变量求值为任意类型。
func (t *testVariable) ReturnTypes() []*cel.Type {
	return []*cel.Type{cel.AnyType}
}

// GetName 返回变量在 variables 对象中的字段名。
func (t *testVariable) GetName() string {
	return t.name
}

// TestCompositedPolicies 覆盖组合编译器（composited variable）的主要语义：
// 变量可被主表达式引用；编译期错误在 CompileAndStoreVariables 阶段暴露；
// 运行期错误来自按需求值；以及成本预算对惰性求值的约束。
func TestCompositedPolicies(t *testing.T) {
	cases := []struct {
		name                 string
		variables            []NamedExpressionAccessor
		expression           string
		attributes           admission.Attributes
		expectedResult       any
		expectErr            bool
		expectedErrorMessage string
		runtimeCostBudget    int64
	}{
		{
			// 最基础场景：主表达式通过 variables.name 读取组合变量。
			name: "simple",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "name",
					expression: "object.metadata.name",
				},
			},
			attributes:     endpointCreateAttributes(),
			expression:     "variables.name == 'endpoints1'",
			expectedResult: true,
		},
		{
			// 组合变量本身无法编译时，应在存储阶段直接报错，而不是等到求值。
			name: "early compile error",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "name",
					expression: "1 == '1'", // won't compile
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "variables.name == 'endpoints1'",
			expectErr:            true,
			expectedErrorMessage: `found no matching overload for '_==_' applied to '(int, string)'`,
		},
		{
			// 表达式可编译但运行期越界：错误需指明是哪个组合变量失败。
			name: "delayed eval error",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "count",
					expression: "object.subsets[114514].addresses.size()", // array index out of bound
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "variables.count == 810",
			expectErr:            true,
			expectedErrorMessage: `composited variable "count" fails to evaluate: index out of bounds: 114514`,
		},
		{
			// 预算够求值单个变量但不够整条表达式，验证惰性求值也受总预算约束。
			name: "out of budget during lazy evaluation",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "name",
					expression: "object.metadata.name", // cost = 3
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "variables.name == 'endpoints1'", // cost = 3
			expectedResult:       true,
			runtimeCostBudget:    4, // enough for main variable but not for entire expression
			expectErr:            true,
			expectedErrorMessage: "running out of cost budget",
		},
		{
			// 同一变量被引用两次也只计一次成本：结果被缓存，这是预算模型的关键不变量。
			name: "lazy evaluation, budget counts only once",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "name",
					expression: "object.metadata.name", // cost = 3
				},
			},
			attributes:        endpointCreateAttributes(),
			expression:        "variables.name == 'endpoints1' && variables.name == 'endpoints1' ", // cost = 7
			expectedResult:    true,
			runtimeCostBudget: 10, // enough for one lazy evaluation but not two, should pass
		},
		{
			name: "single boolean variable in expression",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "fortuneTelling",
					expression: "true",
				},
			},
			attributes:     endpointCreateAttributes(),
			expression:     "variables.fortuneTelling",
			expectedResult: true,
		},
		{
			name: "variable of a list",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "list",
					expression: "[1, 2, 3, 4]",
				},
			},
			attributes:     endpointCreateAttributes(),
			expression:     "variables.list.sum() == 10",
			expectedResult: true,
		},
		{
			name: "variable of a map",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "dict",
					expression: `{"foo": "bar"}`,
				},
			},
			attributes:     endpointCreateAttributes(),
			expression:     "variables.dict['foo'].contains('bar')",
			expectedResult: true,
		},
		{
			// 类型不匹配（list 当 map 用、string 与 int 比较）应当编译期失败。
			name: "variable of a list but confused as a map",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "list",
					expression: "[1, 2, 3, 4]",
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "variables.list['invalid'] == 'invalid'",
			expectErr:            true,
			expectedErrorMessage: "found no matching overload for '_[_]' applied to '(list(int), string)'",
		},
		{
			name: "list of strings, but element is confused as an integer",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "list",
					expression: "['1', '2', '3', '4']",
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "variables.list[0] == 1",
			expectErr:            true,
			expectedErrorMessage: "found no matching overload for '_==_' applied to '(string, int)'",
		},
		{
			// 整体超出运行期预算：错误文案由上层固定，供指标归类依赖。
			name: "exceeds cost budget",
			variables: []NamedExpressionAccessor{
				&testVariable{
					name:       "dict",
					expression: "'abc 123 def 123'.split(' ')",
				},
			},
			attributes:           endpointCreateAttributes(),
			expression:           "size(variables.dict) > 0",
			expectErr:            true,
			expectedErrorMessage: "validation failed due to running out of cost budget, no further validation rules will be run",
			runtimeCostBudget:    5,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个用例使用全新编译器，避免变量在用例之间泄漏。
			compiler, err := NewCompositedCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
			if err != nil {
				t.Fatal(err)
			}
			compiler.CompileAndStoreVariables(tc.variables, OptionalVariableDeclarations{HasParams: false, HasAuthorizer: false}, environment.NewExpressions)
			validations := []ExpressionAccessor{&testCondition{Expression: tc.expression}}
			f := compiler.CompileCondition(validations, OptionalVariableDeclarations{HasParams: false, HasAuthorizer: false}, environment.NewExpressions)
			versionedAttr, err := admission.NewVersionedAttributes(tc.attributes, tc.attributes.GetKind(), newObjectInterfacesForTest())
			if err != nil {
				t.Fatal(err)
			}
			optionalVars := OptionalVariableBindings{}
			// 用例未指定预算时使用全局默认值，避免测试与生产预算漂移。
			costBudget := tc.runtimeCostBudget
			if costBudget == 0 {
				costBudget = celconfig.RuntimeCELCostBudget
			}
			result, _, err := f.ForInput(context.Background(), versionedAttr, CreateAdmissionRequest(versionedAttr.Attributes, v1.GroupVersionResource(tc.attributes.GetResource()), v1.GroupVersionKind(versionedAttr.VersionedKind)), optionalVars, nil, costBudget)
			if !tc.expectErr && err != nil {
				t.Fatalf("failed evaluation: %v", err)
			}
			if !tc.expectErr && len(result) == 0 {
				t.Fatal("unexpected empty result")
			}
			if err == nil {
				err = result[0].Error
			}
			if tc.expectErr {
				if err == nil {
					t.Fatal("unexpected no error")
				}
				if !strings.Contains(err.Error(), tc.expectedErrorMessage) {
					t.Errorf("expected error to contain %q but got %s", tc.expectedErrorMessage, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("failed validation: %v", result[0].Error)
			}
			if tc.expectedResult != result[0].EvalResult.Value() {
				t.Errorf("wrong result: expected %v but got %v", tc.expectedResult, result)
			}

		})
	}
}

// TestCompilerIsolation verifies that each call to NewCompositedCompiler
// creates an isolated environment where variables from one compiler
// do not leak into another.
//
// 隔离性很重要：不同策略/配置可以复用同一基础环境，但组合变量必须
// 各自独立，否则一个来源的变量会污染另一来源的求值。
func TestCompilerIsolation(t *testing.T) {
	baseEnv := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion())

	// Create first compiler with variable "foo"
	compiler1, err := NewCompositedCompiler(baseEnv)
	if err != nil {
		t.Fatal(err)
	}
	vars1 := []NamedExpressionAccessor{
		&testVariable{name: "foo", expression: "'bar'"},
	}
	compiler1.CompileAndStoreVariables(vars1, OptionalVariableDeclarations{}, environment.StoredExpressions)

	// Create second compiler with variable "baz"
	compiler2, err := NewCompositedCompiler(baseEnv)
	if err != nil {
		t.Fatal(err)
	}
	vars2 := []NamedExpressionAccessor{
		&testVariable{name: "baz", expression: "'qux'"},
	}
	compiler2.CompileAndStoreVariables(vars2, OptionalVariableDeclarations{}, environment.StoredExpressions)

	// Verify isolation: compiler1 should not have "baz", compiler2 should not have "foo"
	if _, ok := compiler1.state.mapType.Fields["baz"]; ok {
		t.Error("compiler1 should not have variable 'baz' from compiler2")
	}
	if _, ok := compiler2.state.mapType.Fields["foo"]; ok {
		t.Error("compiler2 should not have variable 'foo' from compiler1")
	}

	// Verify each compiler has its own variable
	if _, ok := compiler1.state.mapType.Fields["foo"]; !ok {
		t.Error("compiler1 should have variable 'foo'")
	}
	if _, ok := compiler2.state.mapType.Fields["baz"]; !ok {
		t.Error("compiler2 should have variable 'baz'")
	}
}
