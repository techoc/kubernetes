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
	"reflect"
	"strings"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	apiservercel "k8s.io/apiserver/pkg/cel"
)

// TestCompileClaimsExpression 验证三种针对 claims 环境的访问器都能成功编译：
// ClaimMappingExpression、ClaimValidationCondition 与 ExtraMappingExpression。
func TestCompileClaimsExpression(t *testing.T) {
	testCases := []struct {
		name                string
		expressionAccessors []ExpressionAccessor
	}{
		{
			name: "valid ClaimMappingCondition",
			// 映射表达式只要求能求值，不限制返回类型。
			expressionAccessors: []ExpressionAccessor{
				&ClaimMappingExpression{
					Expression: "claims.foo",
				},
			},
		},
		{
			name: "valid ClaimValidationCondition",
			// 校验条件会被当作布尔断言使用，因此必须能编译为 bool。
			expressionAccessors: []ExpressionAccessor{
				&ClaimValidationCondition{
					Expression: "claims.foo == 'bar'",
				},
			},
		},
		{
			name: "valid ExtraMapppingCondition",
			// 额外映射同样允许任意类型的返回值。
			expressionAccessors: []ExpressionAccessor{
				&ExtraMappingExpression{
					Expression: "claims.foo",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, expressionAccessor := range tc.expressionAccessors {
				// 编译器按访问器类型选择 claims 声明环境，这里只关心是否报错。
				_, err := NewDefaultCompiler().CompileClaimsExpression(expressionAccessor)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestCompileUserExpression 验证针对 user 环境的表达式可以编译。
// user 环境中以 user.username 等字段为根。
func TestCompileUserExpression(t *testing.T) {
	testCases := []struct {
		name                string
		expressionAccessors []ExpressionAccessor
	}{
		{
			name: "valid UserValidationCondition",
			expressionAccessors: []ExpressionAccessor{
				&ExtraMappingExpression{
					Expression: "user.username == 'bar'",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, expressionAccessor := range tc.expressionAccessors {
				// 使用 user 声明环境编译，成功的标志就是 err 为 nil。
				_, err := NewDefaultCompiler().CompileUserExpression(expressionAccessor)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestCompileClaimsExpressionError 验证 claims 环境下的典型编译错误：
// 非 bool 的校验条件、在 claims 环境误用 user，以及对 map 做非法算术。
func TestCompileClaimsExpressionError(t *testing.T) {
	testCases := []struct {
		name                string
		expressionAccessors []ExpressionAccessor
		wantErr             string
	}{
		{
			name: "invalid ClaimValidationCondition",
			// 校验条件必须返回 bool，而 claims.foo 是 map 值。
			expressionAccessors: []ExpressionAccessor{
				&ClaimValidationCondition{
					Expression: "claims.foo",
				},
			},
			wantErr: "must evaluate to bool",
		},
		{
			name: "UserValidationCondition with wrong env",
			expressionAccessors: []ExpressionAccessor{
				&UserValidationCondition{
					Expression: "user.username == 'foo'",
				},
			},
			// user 未在 claims 声明环境中定义，因此是编译错误。
			wantErr: `compilation failed: ERROR: <input>:1:1: undeclared reference to 'user' (in container '')`,
		},
		{
			name: "invalid ClaimMappingCondition",
			expressionAccessors: []ExpressionAccessor{
				&ClaimMappingExpression{
					Expression: "claims + 1",
				},
			},
			// map 与 int 之间没有 `+` 重载，编译器应报告无匹配重载。
			wantErr: `compilation failed: ERROR: <input>:1:8: found no matching overload for '_+_' applied to '(map(string, any), int)'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, expressionAccessor := range tc.expressionAccessors {
				// 校验条件必须返回 bool，而 claims.foo 是 map 值。
				_, err := NewDefaultCompiler().CompileClaimsExpression(expressionAccessor)
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error to contain %q but got %q", tc.wantErr, err.Error())
				}
			}
		})
	}
}

// TestCompileUserExpressionError 验证 user 环境下的编译错误：
// 非 bool 校验条件、误用 claims 字段以及访问未声明字段。
func TestCompileUserExpressionError(t *testing.T) {
	testCases := []struct {
		name                string
		expressionAccessors []ExpressionAccessor
		wantErr             string
	}{
		{
			name: "invalid UserValidationCondition",
			// 校验条件必须返回 bool，而 user.username 是 string。
			expressionAccessors: []ExpressionAccessor{
				&UserValidationCondition{
					Expression: "user.username",
				},
			},
			wantErr: "must evaluate to bool",
		},
		{
			name: "ClamMappingCondition with wrong env",
			// claims 未在 user 声明环境中定义。
			expressionAccessors: []ExpressionAccessor{
				&ClaimMappingExpression{
					Expression: "claims.foo",
				},
			},
			wantErr: `compilation failed: ERROR: <input>:1:1: undeclared reference to 'claims' (in container '')`,
		},
		{
			name: "ExtraMappingCondition with wrong env",
			// claims 未在 user 声明环境中定义。
			expressionAccessors: []ExpressionAccessor{
				&ExtraMappingExpression{
					Expression: "claims.foo",
				},
			},
			wantErr: `compilation failed: ERROR: <input>:1:1: undeclared reference to 'claims' (in container '')`,
		},
		{
			name: "ClaimValidationCondition with wrong env",
			// claims 未在 user 声明环境中定义。
			expressionAccessors: []ExpressionAccessor{
				&ClaimValidationCondition{
					Expression: "claims.foo == 'bar'",
				},
			},
			wantErr: `compilation failed: ERROR: <input>:1:1: undeclared reference to 'claims' (in container '')`,
		},
		{
			name: "UserValidationCondition expression with unknown field",
			// user 的声明类型中没有 unknown 字段。
			expressionAccessors: []ExpressionAccessor{
				&UserValidationCondition{
					Expression: "user.unknown == 'foo'",
				},
			},
			wantErr: `compilation failed: ERROR: <input>:1:5: undefined field 'unknown'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, expressionAccessor := range tc.expressionAccessors {
				// 断言存在错误且错误文本包含预期片段；CEL 报错细节可能随版本变化，故用 Contains。
				_, err := NewDefaultCompiler().CompileUserExpression(expressionAccessor)
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error to contain %q but got %q", tc.wantErr, err.Error())
				}
			}
		})
	}
}

// TestBuildUserType 验证 buildUserType 生成的声明类型与 authenticationv1.UserInfo
// 的字段数量、json 名称及 CEL 类型逐一对应。
func TestBuildUserType(t *testing.T) {
	userDeclType := buildUserType()
	userType := reflect.TypeOf(authenticationv1.UserInfo{})

	if len(userDeclType.Fields) != userType.NumField() {
		// 字段数不一致通常意味着新增字段后忘了同步 CEL 声明类型。
		t.Errorf("expected %d fields, got %d", userType.NumField(), len(userDeclType.Fields))
	}

	for i := 0; i < userType.NumField(); i++ {
		field := userType.Field(i)
		jsonTagParts := strings.Split(field.Tag.Get("json"), ",")
		if len(jsonTagParts) < 1 {
			t.Fatal("expected json tag to be present")
		}
		fieldName := jsonTagParts[0]

		declField, ok := userDeclType.Fields[fieldName]
		if !ok {
			t.Errorf("expected field %q to be present", field.Name)
		}
		// CEL 类型也必须一致，避免表达式在运行时出现类型不匹配。
		if nativeTypeToCELType(t, field.Type).CelType().Equal(declField.Type.CelType()).Value() != true {
			t.Errorf("expected field %q to have type %v, got %v", field.Name, field.Type, declField.Type)
		}
	}
}

// nativeTypeToCELType 把测试涉及的 Go 类型映射为 CEL 声明类型；遇到未覆盖类型直接失败。
func nativeTypeToCELType(t *testing.T, nativeType reflect.Type) *apiservercel.DeclType {
	switch nativeType {
	case reflect.TypeOf(""):
		return apiservercel.StringType
	case reflect.TypeOf([]string{}):
		return apiservercel.NewListType(apiservercel.StringType, -1)
	case reflect.TypeOf(map[string]authenticationv1.ExtraValue{}):
		return apiservercel.NewMapType(apiservercel.StringType, apiservercel.AnyType, -1)
	default:
		// 新增字段类型时需要在此补充映射，否则无法比较 CEL 类型。
		t.Fatalf("unsupported type %v", nativeType)
	}
	return nil
}
