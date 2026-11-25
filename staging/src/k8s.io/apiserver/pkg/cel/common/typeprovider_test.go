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

package common

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/mutation/dynamic"
)

// TestTypeProvider 验证 ResolverEnvOption 注入的类型解析器只影响对象构造，
// 不干扰普通表达式的求值。
// 关键不变量是"非对象表达式必须原样通过"——类型解析器若过度介入，
// 会把算术等基础表达式也改写，造成难以排查的行为变化。
func TestTypeProvider(t *testing.T) {
	for _, tc := range []struct {
		name               string
		expression         string
		expectedValue      any
		expectCompileError string
	}{
		{
			name:          "not an object",
			expression:    `2 * 31 * 1847`,
			expectedValue: int64(114514), // 类型解析器不应介入。
			// 此处正是回归保护点：解析器不得改写非对象字面量。
		},
		{
			name:          "empty",
			expression:    "Test{}",
			expectedValue: map[string]any{},
		},
		{
			name:          "simple",
			expression:    "Test{x: 1}",
			expectedValue: map[string]any{"x": int64(1)},
		},
		{
			name:               "invalid type",
			expression:         "Objectfoo{}",
			expectCompileError: "undeclared reference to 'Objectfoo'",
			// 未注册的类型名必须编译期报错，而不是运行期变成 dyn。
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := ResolverEnvOption(&mockTypeResolver{})
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
			if v := r.Value(); !reflect.DeepEqual(tc.expectedValue, v) {
				t.Errorf("expected %v but got %v", tc.expectedValue, v)
			}
		})
	}
}

// mockTypeResolver 是 DynamicTypeResolver 的 mock 实现，
// 允许对象包含任意字段。
// 只识别 "Test" 一个类型名，其余一律拒绝，用来验证未知类型
// 在编译期被拦截。
type mockTypeResolver struct {
}

func (m *mockTypeResolver) Resolve(name string) (ResolvedType, bool) {
	if name == "Test" {
		return newMockResolvedType(m, name), true
	}
	return nil, false
}

// mockResolvedType 是 ResolvedType 的 mock 实现，可包含任意字段。
// Field 对任意字段名都返回 dyn 类型，模拟"无 schema 的开放对象"，
// 使测试无需构造真实 OpenAPI 文档。
type mockResolvedType struct {
	objectType *types.Type
	resolver   TypeResolver
}

func newMockResolvedType(resolver TypeResolver, name string) *mockResolvedType {
	objectType := types.NewObjectType(name)
	return &mockResolvedType{
		objectType: objectType,
		resolver:   resolver,
	}
}

func (m *mockResolvedType) HasTrait(trait int) bool {
	return m.objectType.HasTrait(trait)
}

func (m *mockResolvedType) TypeName() string {
	return m.objectType.TypeName()
}

func (m *mockResolvedType) Type() *types.Type {
	return m.objectType
}

func (m *mockResolvedType) TypeType() *types.Type {
	return types.NewTypeTypeWithParam(m.objectType)
}

func (m *mockResolvedType) Field(name string) (*types.FieldType, bool) {
	// 返回 dyn 而非具体类型，保证任意字段访问都能通过编译。
	return &types.FieldType{
		Type: types.DynType,
		IsSet: func(target any) bool {
			return true
		},
		GetFrom: func(target any) (any, error) {
			return nil, nil
		},
	}, true
}

func (m *mockResolvedType) FieldNames() ([]string, bool) {
	// 返回 true 表示"字段集合开放"，CEL 不会对未知字段报错。
	return nil, true
}

// Val 把字面量字段包装为动态对象，使求值结果表现为 map。
func (m *mockResolvedType) Val(fields map[string]ref.Val) ref.Val {
	return dynamic.NewObjectVal(m.objectType, fields)
}

// mustCreateEnv 用给定选项创建用于测试的默认环境；若环境设置失败则使测试致命失败。
// 以 1.30 作为引入版本，与生产代码中类型解析选项的启用版本一致，
// 确保测试覆盖真实的特性开关边界。
func mustCreateEnv(t testing.TB, envOptions ...cel.EnvOption) *cel.Env {
	envSet, err := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()).
		Extend(environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 30),
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
