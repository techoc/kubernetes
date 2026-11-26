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

// 本文件测试动态类型（schema 未知时）的对象值实现：
// optional 字段的转换语义、类型名与字段路径的一致性校验，以及转换为原生 map 的递归逻辑。

package dynamic

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// TestOptional 验证 CEL optional 值在转换为原生表示时的行为：
// optional.of(x) 展开为 x，optional.none() 当前只能表现为 nil。
func TestOptional(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fields   map[string]ref.Val
		expected map[string]any
	}{
		{
			// 注意零值也要保留：optional.of(0) 与“字段缺失”语义不同，不能在此处被丢弃。
			name: "present",
			fields: map[string]ref.Val{
				"zero": types.OptionalOf(types.IntZero),
			},
			expected: map[string]any{
				"zero": int64(0),
			},
		},
		{
			// 已知局限：转换层无法区分 optional.none() 与显式 null，
			// 需要在转换前先过滤掉 optional.none()。
			name: "none",
			fields: map[string]ref.Val{
				"absent": types.OptionalNone,
			},
			expected: map[string]any{
				// 目前无法与普通的 null 区分。
				// 转换前需要先过滤掉 optional.none()。
				"absent": nil,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &ObjectVal{
				objectType: nil, // safe in this test, otherwise put a mock
				fields:     tc.fields,
			}
			converted := v.Value()
			if !reflect.DeepEqual(tc.expected, converted) {
				t.Errorf("wrong result, expected %v but got %v", tc.expected, converted)
			}
		})
	}
}

// TestCheckTypeNamesMatchFieldPathNames 验证类型名必须由字段路径推导：
// 嵌套对象、map 值、list 元素都要满足 <父类型>.<字段名> 的命名规则。
// 这是防止表达式引用任意类型、把 patch 写到错误路径上的关键校验。
func TestCheckTypeNamesMatchFieldPathNames(t *testing.T) {
	for _, tc := range []struct {
		name        string
		obj         *ObjectVal
		expectError string
	}{
		{
			// 正常路径：struct 字段、map 值与 list 元素都使用与路径一致的类型名。
			name: "valid",
			obj: &ObjectVal{
				objectType: types.NewObjectType("Object"),
				fields: map[string]ref.Val{
					"spec": &ObjectVal{
						objectType: types.NewObjectType("Object.spec"),
						fields: map[string]ref.Val{
							"replicas": types.Int(100),
							"m": types.NewRefValMap(nil, map[ref.Val]ref.Val{
								types.String("k1"): &ObjectVal{
									objectType: types.NewObjectType("Object.spec.m"),
								},
							}),
							"l": types.NewRefValList(nil, []ref.Val{
								&ObjectVal{
									objectType: types.NewObjectType("Object.spec.l"),
								},
							}),
						},
					},
				},
			},
		},
		{
			// 子对象类型名与字段名不匹配（Object.spec 出现在 invalid 字段下）。
			name: "invalid struct field",
			obj: &ObjectVal{
				objectType: types.NewObjectType("Object"),
				fields: map[string]ref.Val{"invalid": &ObjectVal{
					objectType: types.NewObjectType("Object.spec"),
					fields:     map[string]ref.Val{"replicas": types.Int(100)},
				}},
			},
			expectError: "unexpected type name \"Object.spec\", expected \"Object.invalid\"",
		},
		{
			// map 的条目类型同样受约束，否则可借 map 绕过字段路径检查。
			name: "invalid map field",
			obj: &ObjectVal{
				objectType: types.NewObjectType("Object"),
				fields: map[string]ref.Val{
					"spec": &ObjectVal{
						objectType: types.NewObjectType("Object.spec"),
						fields: map[string]ref.Val{
							"replicas": types.Int(100),
							"m": types.NewRefValMap(nil, map[ref.Val]ref.Val{
								types.String("k1"): &ObjectVal{
									objectType: types.NewObjectType("Object.spec.invalid"),
								},
							}),
						},
					},
				},
			},
			expectError: "unexpected type name \"Object.spec.invalid\", expected \"Object.spec.m\"",
		},
		{
			// list 元素类型同样受约束。
			name: "invalid list field",
			obj: &ObjectVal{
				objectType: types.NewObjectType("Object"),
				fields: map[string]ref.Val{
					"spec": &ObjectVal{
						objectType: types.NewObjectType("Object.spec"),
						fields: map[string]ref.Val{
							"replicas": types.Int(100),
							"l": types.NewRefValList(nil, []ref.Val{
								&ObjectVal{
									objectType: types.NewObjectType("Object.spec.invalid"),
								},
							}),
						},
					},
				},
			},
			expectError: "unexpected type name \"Object.spec.invalid\", expected \"Object.spec.l\"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.obj.CheckTypeNamesMatchFieldPathNames()
			if tc.expectError == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error")
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("expected error to contain %v, got %v", tc.expectError, err)
				}
			}
		})
	}
}

// TestConvertField 验证 ObjectVal 到 map[string]any 的递归转换：
// 覆盖 list/map 中混合标量与嵌套对象、带/不带 type adapter 的两种构造方式，
// 以及 map 键非字符串、list 元素不可转换等错误路径。
func TestConvertField(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fields      map[string]ref.Val
		expected    map[string]any
		expectError string
	}{
		{
			// 不带 type adapter 的构造方式：元素为原生 CEL 值，需直接转换。
			name: "list of primitives and object",
			fields: map[string]ref.Val{
				"list": types.NewRefValList(nil, []ref.Val{
					types.String("hello"),
					&ObjectVal{
						objectType: types.NewObjectType("MyType"),
						fields: map[string]ref.Val{
							"foo": types.String("bar"),
						},
					},
				}),
			},
			expected: map[string]any{
				"list": []any{
					"hello",
					map[string]any{"foo": "bar"},
				},
			},
		},
		{
			// 带 type adapter 的路径应产生与上面完全相同的结果，二者不可有行为差异。
			name: "list of primitives and object (traits.Lister)",
			fields: map[string]ref.Val{
				"list": types.NewRefValList(types.DefaultTypeAdapter, []ref.Val{
					types.String("hello"),
					&ObjectVal{
						objectType: types.NewObjectType("MyType"),
						fields: map[string]ref.Val{
							"foo": types.String("bar"),
						},
					},
				}),
			},
			expected: map[string]any{
				"list": []any{
					"hello",
					map[string]any{"foo": "bar"},
				},
			},
		},
		{
			// map 值中的嵌套对象需递归转换，整型统一转成 int64 以匹配 JSON 数值语义。
			name: "map of primitives and object",
			fields: map[string]ref.Val{
				"map": types.NewRefValMap(nil, map[ref.Val]ref.Val{
					types.String("key1"): types.Int(42),
					types.String("key2"): &ObjectVal{
						objectType: types.NewObjectType("MyType"),
						fields: map[string]ref.Val{
							"foo": types.String("bar"),
						},
					},
				}),
			},
			expected: map[string]any{
				"map": map[string]any{
					"key1": int64(42),
					"key2": map[string]any{"foo": "bar"},
				},
			},
		},
		{
			// 与 list 相同：带 type adapter 的 map 结果必须与不带时一致。
			name: "map of primitives and object (traits.Mapper)",
			fields: map[string]ref.Val{
				"map": types.NewRefValMap(types.DefaultTypeAdapter, map[ref.Val]ref.Val{
					types.String("key1"): types.Int(42),
					types.String("key2"): &ObjectVal{
						objectType: types.NewObjectType("MyType"),
						fields: map[string]ref.Val{
							"foo": types.String("bar"),
						},
					},
				}),
			},
			expected: map[string]any{
				"map": map[string]any{
					"key1": int64(42),
					"key2": map[string]any{"foo": "bar"},
				},
			},
		},
		{
			// 非字符串键无法表示为 JSON 对象键，必须报错而不是静默丢弃。
			name: "invalid map key type",
			fields: map[string]ref.Val{
				"badMap": types.NewRefValMap(nil, map[ref.Val]ref.Val{
					types.Int(100): types.String("value"),
				}),
			},
			expectError: "not string",
		},
		{
			// list 元素内部的 map 键非法时，错误应向上冒泡到调用方。
			name: "invalid list element error",
			fields: map[string]ref.Val{
				"badList": types.DefaultTypeAdapter.NativeToValue([]any{
					map[int]string{200: "value"},
				}),
			},
			expectError: "not string",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &ObjectVal{
				objectType: types.NewObjectType("Root"),
				fields:     tc.fields,
			}
			converted, err := v.ConvertToNative(reflect.TypeFor[map[string]any]())
			if tc.expectError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectError)
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("expected error containing %q, got %v", tc.expectError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(tc.expected, converted) {
					t.Errorf("wrong result\nexpected: %#v\ngot:      %#v", tc.expected, converted)
				}
			}
		})
	}
}
