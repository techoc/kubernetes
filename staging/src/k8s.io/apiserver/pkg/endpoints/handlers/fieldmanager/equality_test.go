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

package fieldmanager

import "testing"

// TestEqualIgnoringFieldValueAtPath 验证 field manager 的相等性判断会忽略
// 指定路径上的差异（这里是 metadata.managedFields），同时对其余字段保持
// 严格比较；并验证该判断满足交换律。
func TestEqualIgnoringFieldValueAtPath(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]any
		want bool
	}{
		{
			name: "identical objects",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			want: true,
		},
		{
			name: "different metadata label value",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "prod"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			want: false,
		},
		{
			name: "different spec value",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value1",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value2",
				},
			},
			want: false,
		},
		{
			name: "extra spec field",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field":      "value1",
					"otherField": "other",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value1",
				},
			},
			want: false,
		},
		{
			name: "different spec field",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field": "value1",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{},
				},
				"spec": map[string]any{
					"field":      "value1",
					"otherField": "other",
				},
			},
			want: false,
		},
		{
			// managedFields 由 apiserver 自己维护，不同 manager 的条目差异
			// 不应导致对象被判为“已变更”。
			name: "different managed fields should be ignored",
			a: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{map[string]any{"manager": "client1"}},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			b: map[string]any{
				"metadata": map[string]any{
					"labels":        map[string]any{"env": "dev"},
					"managedFields": []any{map[string]any{"manager": "client2"}},
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			want: true,
		},
		{
			// 路径类型不符合预期（metadata 不是 map）时，无法定位要忽略的
			// 字段，实现选择“忽略该差异”而不是报错，避免因异常结构阻断
			// 正常的 update 流程。
			name: "wrong type for metadata result in differences being ignored",
			a: map[string]any{
				"metadata": []any{
					"something",
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			b: map[string]any{
				"metadata": []any{
					"something else",
				},
				"spec": map[string]any{
					"field": "value",
				},
			},
			want: true,
		},
	}

	path := []string{"metadata", "managedFields"}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := equalIgnoringValueAtPath(c.a, c.b, path)
			if actual != c.want {
				t.Error("Expected equality check to return ", c.want, ", but got ", actual)
			}
			// Check that equality is commutative
			// 交换参数顺序必须得到相同结论，否则调用方的行为会依赖参数
			// 顺序，属于隐蔽的正确性 bug。
			actual = equalIgnoringValueAtPath(c.b, c.a, path)
			if actual != c.want {
				t.Error("Expected equality check to return ", c.want, ", but got ", actual)
			}
		})
	}
}
