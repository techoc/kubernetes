/*
Copyright 2024 The Kubernetes Authors.

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

package mutation

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/mutation/dynamic"
)

// TestTypeResolver 验证 DynamicTypeResolver 能把 CEL 的对象字面量
// （Object{...} / Object.spec{...} / JSONPatch{...}）解析成对应的动态值，
// 并在编译期捕获字段类型错误与未声明类型。
func TestTypeResolver(t *testing.T) {
	for _, tc := range []struct {
		name               string
		expression         string
		expectedValue      any
		expectCompileError string
	}{
		{
			// 非对象字面量走普通求值路径，不受类型解析器影响。
			name:          "not an object",
			expression:    `string(114514)`,
			expectedValue: "114514",
		},
		{
			name:          "empty",
			expression:    "Object{}",
			expectedValue: map[string]any{},
		},
		{
			// 嵌套对象字面量；整数统一映射为 int64，与 JSON 数值语义一致。
			name:       "Object.spec",
			expression: "Object{spec: Object.spec{replicas: 3}}",
			expectedValue: map[string]any{
				"spec": map[string]any{
					// 整数映射为 int64
					"replicas": int64(3),
				},
			},
		},
		{
			// 列表字面量不需要类型解析器新增 Path 代码，
			// 与对象字面量相比。
			// 该用例用于记录“支持的语法”。
			//
			// 列表字面量不需要类型解析器新增路径逻辑，但仍需保证嵌套元素能正确解析。
			name: "Object.spec.template.containers",
			expression: `Object{
				spec: Object.spec{
					template: Object.spec.template{
						containers: [
							Object.spec.template.containers.item{
								name: "nginx",
								image: "nginx",
								args: ["-g"]
							}
						]
					}
				}
			}`,
			expectedValue: map[string]any{
				"spec": map[string]any{
					"template": map[string]any{
						"containers": []any{
							map[string]any{
								"name":  "nginx",
								"image": "nginx",
								"args":  []any{"-g"},
							},
						},
					},
				},
			},
		},
		{
			name: "list of ints",
			expression: `Object{
				intList: [1, 2, 3]
			}`,
			expectedValue: map[string]any{
				"intList": []any{int64(1), int64(2), int64(3)},
			},
		},
		{
			name: "map string-to-string",
			expression: `Object{
				annotations: {"foo": "bar"}
			}`,
			expectedValue: map[string]any{
				"annotations": map[string]any{
					"foo": "bar",
				},
			},
		},
		{
			name: "field access",
			expression: `Object{
				intList: [1, 2, 3]
			}.intList.sum()`,
			expectedValue: int64(6),
		},
		{
			name:          "equality check",
			expression:    "Object{spec: Object.spec{replicas: 3}} == Object{spec: Object.spec{replicas: 1 + 2}}",
			expectedValue: true,
		},
		{
			name:       "invalid type",
			expression: "Invalid{}",
			// 未声明的类型必须在编译期失败，而不是运行期产生不可预期的对象。
			expectCompileError: "undeclared reference to 'Invalid'",
		},
		{
			name:          "logic around JSONPatch",
			expression:    `true ? JSONPatch{op: "add", path: "/spec/replicas", value: 3} : JSONPatch{op: "remove", path: "/spec/replicas"}`,
			expectedValue: &JSONPatchVal{Op: "add", Path: "/spec/replicas", Val: types.Int(3)},
		},
		{
			// JSONPatch 支持 add/remove/replace/move/copy/test 六种操作；
			// 值语义由 JSONPatchVal 承载，后续会被转换为真正的 JSON Patch。
			name:          "JSONPatch add",
			expression:    `JSONPatch{op: "add", path: "/spec/replicas", value: 3}`,
			expectedValue: &JSONPatchVal{Op: "add", Path: "/spec/replicas", Val: types.Int(3)},
		},
		{
			name:          "JSONPatch remove",
			expression:    `JSONPatch{op: "remove", path: "/spec/replicas"}`,
			expectedValue: &JSONPatchVal{Op: "remove", Path: "/spec/replicas"},
		},
		{
			name:          "JSONPatch replace",
			expression:    `JSONPatch{op: "replace", path: "/spec/replicas", value: 3}`,
			expectedValue: &JSONPatchVal{Op: "replace", Path: "/spec/replicas", Val: types.Int(3)},
		},
		{
			name:          "JSONPatch move",
			expression:    `JSONPatch{op: "move", from: "/spec/replicas", path: "/spec/replicas"}`,
			expectedValue: &JSONPatchVal{Op: "move", From: "/spec/replicas", Path: "/spec/replicas"},
		},
		{
			name:          "JSONPatch copy",
			expression:    `JSONPatch{op: "copy", from: "/spec/replicas", path: "/spec/replicas"}`,
			expectedValue: &JSONPatchVal{Op: "copy", From: "/spec/replicas", Path: "/spec/replicas"},
		},
		{
			name:          "JSONPatch test",
			expression:    `JSONPatch{op: "test", path: "/spec/replicas", value: 3}`,
			expectedValue: &JSONPatchVal{Op: "test", Path: "/spec/replicas", Val: types.Int(3)},
		},
		{
			// op 取值不在枚举内不会在编译期报错，但后续应用 patch 时会失败；
			// 这是有意的分层：语法/类型与运行时语义分别校验。
			name:          "JSONPatch invalid op",
			expression:    `JSONPatch{op: "invalid", path: "/spec/replicas", value: 3}`,
			expectedValue: &JSONPatchVal{Op: "invalid", Path: "/spec/replicas", Val: types.Int(3)},
			// 无错误，因为编译期不检查值的语义。
		},
		{
			name:          "JSONPatch missing path",
			expression:    `JSONPatch{op: "add", value: 3}`,
			expectedValue: &JSONPatchVal{Op: "add", Val: types.Int(3)},
			// 无错误，因为编译期不检查值的语义。
		},
		{
			name:          "JSONPatch missing value",
			expression:    `JSONPatch{op: "add", path: "/spec/replicas"}`,
			expectedValue: &JSONPatchVal{Op: "add", Path: "/spec/replicas"},
			// 无错误，因为编译期不检查值的语义。
		},
		{
			name:               "JSONPatch invalid value",
			expression:         `JSONPatch{op: "add", path: "/spec/replicas", value: Invalid{}}`,
			expectCompileError: "undeclared reference to 'Invalid'",
		},
		{
			name:               "JSONPatch invalid path",
			expression:         `JSONPatch{op: "add", path: Invalid{}, value: 3}`,
			expectCompileError: "undeclared reference to 'Invalid'",
		},
		{
			name:               "JSONPatch invalid from",
			expression:         `JSONPatch{op: "move", from: Invalid{}, path: "/spec/replicas"}`,
			expectCompileError: "undeclared reference to 'Invalid'",
		},
		{
			// 字段类型不匹配必须编译期失败，避免把错误类型写入对象。
			name:               "JSONPatch invalid op type",
			expression:         `JSONPatch{op: 1, path: "/spec/replicas", value: 3}`,
			expectCompileError: "expected type of field 'op' is 'string' but provided type is 'int'",
		},
		{
			name:               "JSONPatch invalid path type",
			expression:         `JSONPatch{op: "add", path: 1, value: 3}`,
			expectCompileError: "expected type of field 'path' is 'string' but provided type is 'int'",
		},
		{
			name:               "JSONPatch invalid from type",
			expression:         `JSONPatch{op: "move", from: 1, path: "/spec/replicas"}`,
			expectCompileError: "expected type of field 'from' is 'string' but provided type is 'int'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := common.ResolverEnvOption(&DynamicTypeResolver{})
			env := mustCreateEnv(t, option)
			ast, issues := env.Compile(tc.expression)
			if len(tc.expectCompileError) > 0 {
				if issues == nil {
					t.Fatalf("expected error %v but got no error", tc.expectCompileError)
				}
				if !strings.Contains(issues.String(), tc.expectCompileError) {
					t.Fatalf("expected error %v but got %v", tc.expectCompileError, issues.String())
				}
				return
			}

			if issues != nil {
				t.Fatalf("unexpected issues during compilation: %v", issues)
			}
			program, err := env.Program(ast)
			if err != nil {
				t.Fatalf("unexpected error while creating program: %v", err)
			}
			r, _, err := program.Eval(map[string]any{})
			if err != nil {
				t.Fatalf("unexpected error during evaluation: %v", err)
			}
			if v := r.Value(); !reflect.DeepEqual(v, tc.expectedValue) {
				t.Errorf("expected %#v but got %#v", tc.expectedValue, v)
			}
		})
	}
}

// TestCELOptional 探索性测试，演示 CEL optional 库如何满足 mutation 库的用例。
// 满足 mutation 库所需的使用场景。
//
// 说明 optional 语义在“对象字面量”与“map 索引”两种语法下的差异，
// 这些差异会直接影响策略作者能否安全地表达“字段缺失时保持不设置”。
func TestCELOptional(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		expression           string
		expectedVal          ref.Val
		expectedCompileError string
	}{
		{
			name: "construct existing field with none, empty object",
			expression: `Object{
				?existing: optional.none()
			}`,
			expectedVal: dynamic.NewObjectVal(types.NewObjectType("Object"), map[string]ref.Val{
				// "existing" 字段未设置。
			}),
		},
		{
			name:        "object of zero value, ofNonZeroValue",
			expression:  `Object{?spec: optional.ofNonZeroValue(Object.spec{?replicas: Object{}.?replicas})}`,
			expectedVal: dynamic.NewObjectVal(types.NewObjectType("Object"), map[string]ref.Val{
				// "existing" 字段未设置。
			}),
		},
		{
			name:        "access existing field, return none",
			expression:  `Object{}.?existing`,
			expectedVal: types.OptionalNone,
		},
		{
			name:        "map non-existing field, return none",
			expression:  `{"foo": 1}[?"bar"]`,
			expectedVal: types.OptionalNone,
		},
		{
			name:        "map existing field, return actual value",
			expression:  `{"foo": 1}[?"foo"]`,
			expectedVal: types.OptionalOf(types.Int(1)),
		},
		{
			// map 的行为与 Object 不同。
			//
			// 引自
			// https://github.com/google/cel-spec/blob/master/doc/langdef.md#field-selection
			//
			// 要测试字段是否存在，可使用布尔宏 has(e.f)。
			//
			// 1. 若 e 求值为 map，has(e.f) 表示字符串 f 是否是 map 中的 key
			//   （注意 f 在语法上必须是标识符）。
			//
			// 对 map 使用 has 时，存在性判断基于 key，而不是对象字段元数据。
			name: "has on a map, de-sugared, non-existing field, returns false",
			// has 宏仅支持点号访问语法。
			expression:  `has({"foo": 1}.bar)`,
			expectedVal: types.False,
		},
		{
			name: "has on a map, de-sugared, existing field, returns true",
			// has 宏仅支持点号访问语法。
			expression:  `has({"foo": 1}.foo)`,
			expectedVal: types.True,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := common.ResolverEnvOption(&DynamicTypeResolver{})
			env := mustCreateEnvWithOptional(t, option)
			ast, issues := env.Compile(tc.expression)
			if len(tc.expectedCompileError) > 0 {
				if issues == nil {
					t.Fatalf("expected error %v but got no error", tc.expectedCompileError)
				}
				if !strings.Contains(issues.String(), tc.expectedCompileError) {
					t.Fatalf("expected error %v but got %v", tc.expectedCompileError, issues.String())
				}
				return
			}
			if issues != nil {
				t.Fatalf("unexpected issues during compilation: %v", issues)
			}
			program, err := env.Program(ast)
			if err != nil {
				t.Fatalf("unexpected error while creating program: %v", err)
			}
			r, _, err := program.Eval(map[string]any{})
			if err != nil {
				t.Fatalf("unexpected error during evaluation: %v", err)
			}
			if equals := tc.expectedVal.Equal(r); equals.Value() != true {
				t.Errorf("expected %#+v but got %#+v", tc.expectedVal, r)
			}
		})
	}
}

// mustCreateEnv 用给定选项创建默认测试环境，
// 环境创建失败时直接终止测试。
//
// 使用 StoredExpressions 环境，模拟策略被写入 API 后再次编译的情形。
func mustCreateEnv(t testing.TB, envOptions ...cel.EnvOption) *cel.Env {
	envSet, err := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()).
		Extend(environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 0), // 始终启用。仅用于测试。
			EnvOptions:        envOptions,
		})
	if err != nil {
		t.Fatalf("fail to create env set: %v", err)
	}
	env, err := envSet.Env(environment.StoredExpressions)
	if err != nil {
		t.Fatalf("fail to setup env: %v", env)
	}
	return env
}

// mustCreateEnvWithOptional 用给定选项创建默认测试环境，
// 并以默认配置设置 optional 库。
// 环境创建失败时直接终止测试。
func mustCreateEnvWithOptional(t testing.TB, envOptions ...cel.EnvOption) *cel.Env {
	return mustCreateEnv(t, append(envOptions, cel.OptionalTypes())...)
}
