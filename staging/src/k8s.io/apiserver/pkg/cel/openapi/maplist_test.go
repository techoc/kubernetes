/*
Copyright 2022 The Kubernetes Authors.

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

package openapi

import (
	"reflect"
	"testing"

	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestMapList 验证 MakeMapList 在 CEL 侧对 listType=map 的数组做按键查找
// 的语义：只有同时具备 x-kubernetes-list-type: map 与 list-map-keys 的
// schema 才建立键索引；查询键必须能由各 map-key 字段唯一确定；重复键
// 保留首个元素；非 map 元素或非标量键值被忽略而非报错，因为 CEL 求值
// 需要在部分数据损坏时仍保持确定性行为。
func TestMapList(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sts           *spec.Schema
		items         []interface{}
		warmUpQueries []interface{}
		query         interface{}
		expected      interface{}
	}{
		{
			name: "default list type",
			// 未声明 list type 时视为普通数组，不做 map 查找。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				}},
			query:    map[string]interface{}{},
			expected: nil,
		},
		{
			name: "non list type",
			// 非数组类型不适用该逻辑。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"map"},
				}},
			query:    map[string]interface{}{},
			expected: nil,
		},
		{
			name: "non-map list type",
			// listType=set 不是 map，没有键查找语义。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType: listTypeSet,
				}}},
			query:    map[string]interface{}{},
			expected: nil,
		},
		{
			name: "no keys",
			// 缺少 list-map-keys 无法建立索引。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				}},
			query:    map[string]interface{}{},
			expected: nil,
		},
		{
			name: "single key",
			// 单键查找命中第二个元素；查询中多余字段不参与键计算。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"k"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"k":  "a",
					"v1": "a",
				},
				map[string]interface{}{
					"k":  "b",
					"v1": "b",
				},
			},
			query: map[string]interface{}{
				"k":  "b",
				"v1": "B",
			},
			expected: map[string]interface{}{
				"k":  "b",
				"v1": "b",
			},
		},
		{
			name: "single key ignoring non-map query",
			// 查询不是 map 时返回 nil，而不是 panic 或误匹配。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				}},
			items: []interface{}{
				map[string]interface{}{
					"k":  "a",
					"v1": "a",
				},
			},
			query:    42,
			expected: nil,
		},
		{
			name: "single key ignoring unkeyable query",
			// 键字段值不是标量（此处为 map）时无法构成索引键。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"k"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"k":  "a",
					"v1": "a",
				},
			},
			query: map[string]interface{}{
				"k": map[string]interface{}{
					"keys": "must",
					"be":   "scalars",
				},
				"v1": "A",
			},
			expected: nil,
		},
		{
			name: "ignores item of invalid type",
			// 数组中的非 map 元素不参与索引，但不应阻止其他元素匹配。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"k"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"k":  "a",
					"v1": "a",
				},
				5,
			},
			query: map[string]interface{}{
				"k":  "a",
				"v1": "A",
			},
			expected: map[string]interface{}{
				"k":  "a",
				"v1": "a",
			},
		},
		{
			name: "keep first entry when duplicated keys are encountered",
			// listType=map 要求键唯一，但损坏数据可能重复；保留首个元素
			// 使求值结果确定，与 API server 合并语义一致。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"k"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"k":  "a",
					"v1": "a",
				},
				map[string]interface{}{
					"k":  "a",
					"v1": "b",
				},
			},
			query: map[string]interface{}{
				"k":  "a",
				"v1": "A",
			},
			expected: map[string]interface{}{
				"k":  "a",
				"v1": "a",
			},
		},
		{
			name: "keep first entry when duplicated multi-keys are encountered",
			// 复合键同样保留首个；warmUpQueries 先以另一组键查询，验证
			// 索引缓存不会污染后续查找结果。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"k1", "k2"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"k1": "a",
					"k2": "b",
					"v1": "a",
				},
				map[string]interface{}{
					"k1": "a",
					"k2": "b",
					"v1": "b",
				},
				map[string]interface{}{
					"k1": "x",
					"k2": "y",
					"v1": "z",
				},
			},
			warmUpQueries: []interface{}{
				map[string]interface{}{
					"k1": "x",
					"k2": "y",
				},
			},
			query: map[string]interface{}{
				"k1": "a",
				"k2": "b",
			},
			expected: map[string]interface{}{
				"k1": "a",
				"k2": "b",
				"v1": "a",
			},
		},
		{
			name: "multiple keys with defaults ignores item with nil value for key",
			// 键字段为 nil 的元素无法参与索引（首个元素 kb=nil 被跳过），
			// 第二个元素以完整键值命中。各类型的默认值仅用于表明 schema
			// 允许这些键类型（bool/float/int/string）。
			sts: &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: []string{"array"},
					Properties: map[string]spec.Schema{
						"kb": {SchemaProps: spec.SchemaProps{
							Default: true,
						}},
						"kf": {SchemaProps: spec.SchemaProps{
							Default: 2.0,
						}},
						"ki": {SchemaProps: spec.SchemaProps{
							Default: int64(64),
						}},
						"ks": {
							SchemaProps: spec.SchemaProps{
								Default: "hello",
							}},
					},
				},
				VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
					extListType:    listTypeMap,
					extListMapKeys: []any{"kb", "kf", "ki", "ks"},
				}}},
			items: []interface{}{
				map[string]interface{}{
					"kb": nil,
					"kf": float64(2.0),
					"ki": int64(42),
					"ks": "hello",
					"v1": "a",
				},
				map[string]interface{}{
					"kb": false,
					"kf": float64(2.0),
					"ki": int64(42),
					"ks": "hello",
					"v1": "b",
				},
			},
			query: map[string]interface{}{
				"kb": false,
				"kf": float64(2.0),
				"ki": int64(42),
				"ks": "hello",
				"v1": "B",
			},
			expected: map[string]interface{}{
				"kb": false,
				"kf": float64(2.0),
				"ki": int64(42),
				"ks": "hello",
				"v1": "b",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapList := MakeMapList(tc.sts, tc.items)
			// 预热查询用于验证索引构建后对后续任意键的查找都稳定。
			for _, warmUp := range tc.warmUpQueries {
				mapList.Get(warmUp)
			}
			actual := mapList.Get(tc.query)
			if !reflect.DeepEqual(tc.expected, actual) {
				t.Errorf("got: %v, expected %v", actual, tc.expected)
			}
		})
	}
}
