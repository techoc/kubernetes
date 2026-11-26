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

package common_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestCase 描述一条"按 keypath 遍历关联树"的用例及其期望结果。
type TestCase struct {
	Name string

	// 遍历后旧值的期望值；为 nil 表示遍历应当失败。
	OldValue interface{}

	// 遍历后新值的期望值；为 nil 表示遍历应当失败。
	NewValue interface{}

	// OldValue 与 NewValue 是否应视为相等。
	// 默认用 reflect.DeepEqual 比较两者。若两者并非 DeepEqual 但语义相等
	// （例如 map 列表顺序不同），可在此覆盖为 true。
	// 例如 map 列表顺序变化。
	ExpectEqual bool

	// 提供给关联对象的 schema。
	Schema common.Schema

	// 为取到目标值而要遍历的字段名与索引数组。
	KeyPath []interface{}

	// 遍历的起点根对象。
	RootObject    interface{}
	RootOldObject interface{}
}

// Run 执行一次遍历并校验三件事：新值、旧值、相等性判定。
// 遍历到不可关联节点时 NewValue/OldValue 都必须期望为 nil。
func (c TestCase) Run() error {
	// 构造关联对象。
	correlatedObject := common.NewCorrelatedObject(c.RootObject, c.RootOldObject, c.Schema)

	// 遍历关联对象。
	var err error
	for _, key := range c.KeyPath {
		if correlatedObject == nil {
			break
		}

		switch k := key.(type) {
		case string:
			correlatedObject = correlatedObject.Key(k)
		case int:
			correlatedObject = correlatedObject.Index(k)
		default:
			return errors.New("key must be a string or int")
		}
		if err != nil {
			return err
		}
	}

	if correlatedObject == nil {
		if c.OldValue != nil || c.NewValue != nil {
			return fmt.Errorf("expected non-nil value, got nil")
		}
	} else {
		// 校验关联对象是否具有期望的值。
		if !reflect.DeepEqual(correlatedObject.Value, c.NewValue) {
			return fmt.Errorf("expected value %v, got %v", c.NewValue, correlatedObject.Value)
		}
		if !reflect.DeepEqual(correlatedObject.OldValue, c.OldValue) {
			return fmt.Errorf("expected old value %v, got %v", c.OldValue, correlatedObject.OldValue)
		}

		// 校验关联对象与期望值是否被视为相等。
		if (c.ExpectEqual || reflect.DeepEqual(correlatedObject.Value, correlatedObject.OldValue)) != correlatedObject.CachedDeepEqual() {
			return fmt.Errorf("expected equal, got not equal")
		}
	}

	return nil
}

// 解码给定 YAML 创建 *spec.Schema Schema；出错时 panic。
// YAML 形式让测试用例可以贴近真实的 OpenAPI 片段；解码失败说明用例本身写错，故 panic。
func mustSchema(source string) *openapi.Schema {
	d := yaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	res := &spec.Schema{}
	if err := d.Decode(res); err != nil {
		panic(err)
	}
	return &openapi.Schema{Schema: res}
}

// 解码给定 YAML 创建 *unstructured；出错时 panic。
func mustUnstructured(source string) interface{} {
	d := yaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	var res interface{}
	if err := d.Decode(&res); err != nil {
		panic(err)
	}
	return res
}

// TestCorrelation 验证按 keypath 遍历关联树的结果。
//
// 用例验证：按给定 schema 从根对象沿 keypath 遍历，得到的新旧值是否符合预期。
//
// 若 new 或 old 为 nil，则验证遍历是否因字段路径不可关联而失败。
//
// 同时确认 CachedDeepEqual 的输出与新旧值的 reflect.DeepEqual 结果一致。
//
// 用例围绕 x-kubernetes-list-type 的语义展开：map 类型按 map-key 跨顺序关联，
// atomic/set 不可逐项关联，只能整体比较。
func TestCorrelation(t *testing.T) {
	cases := []TestCase{
		{
			// 最基本的情形：普通对象字段按名称一一配对。
			Name:          "Basic Key",
			RootObject:    mustUnstructured(`a: b`),
			RootOldObject: mustUnstructured(`a: b`),
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{"a"},
			NewValue: "b",
			OldValue: "b",
		},
		{
			// atomic 列表（未声明 list-type 时的默认）不按元素关联，期望遍历失败。
			Name:          "Atomic Array not correlatable",
			RootObject:    mustUnstructured(`[a, b]`),
			RootOldObject: mustUnstructured(`[a, b]`),
			Schema: mustSchema(`
                items:
                  type: string
            `),
			KeyPath: []interface{}{1},
		},
		{
			// 新对象新增的键在旧对象中不存在，无法配对，遍历失败。
			Name: "Added Key Not In Old Object",
			RootObject: mustUnstructured(`
                a: b
                c: d
            `),
			RootOldObject: mustUnstructured(`
                a: b
            `),
			Schema: mustSchema(`
                properties:
                  a: { type: string }
                  c: { type: string }
            `),
			KeyPath: []interface{}{"c"},
		},
		{
			Name: "Added Index Not In Old Object",
			RootObject: mustUnstructured(`
                - a
                - b
                - c
            `),
			RootOldObject: mustUnstructured(`
                - a
                - b
            `),
			Schema: mustSchema(`
                items:
                    type: string
            `),
			KeyPath: []interface{}{2},
		},
		{
			Name: "Changed Index In Old Object not correlatable",
			RootObject: []interface{}{
				"a",
				"b",
			},
			RootOldObject: []interface{}{
				"a",
				"oldB",
			},
			Schema: mustSchema(`
                items:
                    type: string
            `),
			KeyPath: []interface{}{1},
		},
		{
			Name: "Changed Index In Nested Old Object",
			RootObject: []interface{}{
				"a",
				"b",
			},
			RootOldObject: []interface{}{
				"a",
				"oldB",
			},
			Schema: mustSchema(`
                items:
                    type: string
            `),
			KeyPath:  []interface{}{},
			NewValue: []interface{}{"a", "b"},
			OldValue: []interface{}{"a", "oldB"},
		},
		{
			Name: "Changed Key In Old Object",
			RootObject: map[string]interface{}{
				"a": "b",
			},
			RootOldObject: map[string]interface{}{
				"a": "oldB",
			},
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{"a"},
			NewValue: "b",
			OldValue: "oldB",
		},
		{
			Name: "Replaced Key In Old Object",
			RootObject: map[string]interface{}{
				"a": "b",
			},
			RootOldObject: map[string]interface{}{
				"b": "a",
			},
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{},
			NewValue: map[string]interface{}{"a": "b"},
			OldValue: map[string]interface{}{"b": "a"},
		},
		{
			Name: "Added Key In Old Object",
			RootObject: map[string]interface{}{
				"a": "b",
			},
			RootOldObject: map[string]interface{}{},
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{},
			NewValue: map[string]interface{}{"a": "b"},
			OldValue: map[string]interface{}{},
		},
		{
			// 类型从列表变为 map：不相等，但根节点本身仍可比较。
			Name: "Changed list to map",
			RootObject: map[string]interface{}{
				"a": "b",
			},
			RootOldObject: []interface{}{"a", "b"},
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{},
			NewValue: map[string]interface{}{"a": "b"},
			OldValue: []interface{}{"a", "b"},
		},
		{
			Name: "Changed string to map",
			RootObject: map[string]interface{}{
				"a": "b",
			},
			RootOldObject: "a string",
			Schema: mustSchema(`
                properties:
                  a: { type: string }
            `),
			KeyPath:  []interface{}{},
			NewValue: map[string]interface{}{"a": "b"},
			OldValue: "a string",
		},
		{
			// map 类型列表：新列表第 0 项应按 bar=baz 匹配到旧列表第 1 项，
			// 从而拿到正确的旧值 bazValue——这是棘轮校验的核心场景。
			Name: "Map list type",
			RootObject: mustUnstructured(`
                foo:
                - bar: baz
                  val: newBazValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - bar: fizz
                  val: fizzValue
                - bar: baz
                  val: bazValue
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: array
                    items:
                      type: object
                      properties:
                        bar:
                          type: string
                        val:
                          type: string
                    x-kubernetes-list-type: map
                    x-kubernetes-list-map-keys:
                      - bar
            `),
			KeyPath:  []interface{}{"foo", 0, "val"},
			NewValue: "newBazValue",
			OldValue: "bazValue",
		},
		{
			// 即使元素含 map-key 字段，list-type 为 atomic 时也不得逐项关联。
			Name: "Atomic list item should not correlate",
			RootObject: mustUnstructured(`
                foo:
                - bar: baz
                  val: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - bar: fizz
                  val: fizzValue
                - bar: baz
                  val: barValue
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: array
                    items:
                      type: object
                      properties:
                        bar:
                          type: string
                        val:
                          type: string
                    x-kubernetes-list-type: atomic
            `),
			KeyPath: []interface{}{"foo", 0, "val"},
		},
		{
			// map-key 匹配发生在内层对象内部，校验关联需递归到元素内部。
			Name: "Map used inside of map list type should correlate",
			RootObject: mustUnstructured(`
                foo:
                - key: keyValue
                  bar:
                    baz: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - key: otherKeyValue
                  bar:
                    baz: otherOldValue
                - key: altKeyValue
                  bar:
                    baz: altOldValue
                - key: keyValue
                  bar:
                    baz: oldValue
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: array
                    items:
                      type: object
                      properties:
                        key:
                          type: string
                        bar:
                          type: object
                          properties:
                            baz:
                              type: string
                    x-kubernetes-list-type: map
                    x-kubernetes-list-map-keys:
                      - key
            `),
			KeyPath:  []interface{}{"foo", 0, "bar", "baz"},
			NewValue: "newValue",
			OldValue: "oldValue",
		},
		{
			Name: "Map used inside another map should correlate",
			RootObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                    key: otherKeyValue
                    bar:
                        baz: otherOldValue
                altFoo:
                    key: altKeyValue
                    bar:
                        baz: altOldValue
                otherFoo:
                    key: keyValue
                    bar:
                        baz: oldValue
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: object
                    properties:
                      key:
                        type: string
                      bar:
                        type: object
                        properties:
                          baz:
                            type: string
            `),
			KeyPath:  []interface{}{"foo", "bar"},
			NewValue: map[string]interface{}{"baz": "newValue"},
			OldValue: map[string]interface{}{"baz": "otherOldValue"},
		},
		{
			Name: "Nested map equal to old",
			RootObject: mustUnstructured(`
                foo:
                    key: newKeyValue
                    bar:
                        baz: value
            `),
			RootOldObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: value
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: object
                    properties:
                      key:
                        type: string
                      bar:
                        type: object
                        properties:
                          baz:
                            type: string
            `),
			KeyPath:  []interface{}{"foo", "bar"},
			NewValue: map[string]interface{}{"baz": "value"},
			OldValue: map[string]interface{}{"baz": "value"},
		},
		{
			// 列表顺序变化但 map-key 相同，语义上应视为未变更，故显式覆盖 ExpectEqual。
			Name: "Re-ordered list considered equal to old value due to map keys",
			RootObject: mustUnstructured(`
                foo:
                - key: keyValue
                  bar:
                    baz: value
                - key: altKeyValue
                  bar:
                    baz: altValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - key: altKeyValue
                  bar:
                    baz: altValue
                - key: keyValue
                  bar:
                    baz: value
            `),
			Schema: mustSchema(`
                properties:
                  foo:
                    type: array
                    items:
                      type: object
                      properties:
                        key:
                          type: string
                        bar:
                          type: object
                          properties:
                            baz:
                              type: string
                    x-kubernetes-list-type: map
                    x-kubernetes-list-map-keys:
                      - key
            `),
			KeyPath: []interface{}{"foo"},
			NewValue: mustUnstructured(`
                - key: keyValue
                  bar:
                    baz: value
                - key: altKeyValue
                  bar:
                    baz: altValue
            `),
			OldValue: mustUnstructured(`
                - key: altKeyValue
                  bar:
                    baz: altValue
                - key: keyValue
                  bar:
                    baz: value
            `),
			ExpectEqual: true,
		},
		{
			// 未在 properties 中声明的键通过 additionalProperties 的 schema 关联，
			// 否则自由键 map 无法做棘轮校验。
			Name: "Correlate unknown string key via additional properties",
			RootObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                    key: otherKeyValue
                    bar:
                        baz: otherOldValue
            `),
			Schema: mustSchema(`
                properties:
                    foo:
                        type: object
                        additionalProperties:
                            properties:
                                baz:
                                    type: string
            `),
			KeyPath:  []interface{}{"foo", "bar", "baz"},
			NewValue: "newValue",
			OldValue: "otherOldValue",
		},
		{
			Name: "Changed map value",
			RootObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: oldValue
            `),
			Schema: mustSchema(`
                properties:
                    foo:
                        type: object
                        properties:
                            key:
                                type: string
                            bar:
                                type: object
                                properties:
                                    baz:
                                        type: string
            `),
			KeyPath: []interface{}{"foo", "bar"},
			NewValue: mustUnstructured(`
                baz: newValue
            `),
			OldValue: mustUnstructured(`
                baz: oldValue
            `),
		},
		{
			Name: "Changed nested map value",
			RootObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: newValue
            `),
			RootOldObject: mustUnstructured(`
                foo:
                    key: keyValue
                    bar:
                        baz: oldValue
            `),
			Schema: mustSchema(`
                properties:
                    foo:
                        type: object
                        properties:
                            key:
                                type: string
                            bar:
                                type: object
                                properties:
                                    baz:
                                        type: string
            `),
			KeyPath: []interface{}{"foo"},
			NewValue: mustUnstructured(`
                key: keyValue    
                bar:
                  baz: newValue
            `),
			OldValue: mustUnstructured(`
                key: keyValue    
                bar:
                  baz: oldValue
            `),
		},
		{
			// set 类型整体不变时应判为相等（走整体比较路径）。
			Name: "unchanged list type set with atomic map values",
			Schema: mustSchema(`
                properties:
                    foo:
                        type: array
                        items:
                            type: object
                            x-kubernetes-map-type: atomic
                            properties:
                                key:
                                    type: string
                                bar:
                                    type: string
                        x-kubernetes-list-type: set
            `),
			RootObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
			KeyPath: []interface{}{"foo"},
			NewValue: mustUnstructured(`
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
			OldValue: mustUnstructured(`
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
		},
		{
			// set 类型有元素变化时应判为不相等。
			Name: "changed list type set with atomic map values",
			Schema: mustSchema(`
                properties:
                    foo:
                        type: array
                        items:
                            type: object
                            x-kubernetes-map-type: atomic
                            properties:
                                key:
                                    type: string
                                bar:
                                    type: string
                        x-kubernetes-list-type: set
            `),
			RootObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: newValue2
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
			KeyPath: []interface{}{"foo"},
			NewValue: mustUnstructured(`
                - key: key1
                  bar: value1
                - key: key2
                  bar: newValue2
            `),
			OldValue: mustUnstructured(`
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
		},
		{
			// 印证 set 的不可关联性：按索引下钻应得到 nil。
			Name: "elements of list type set with atomic map values are not correlated",
			Schema: mustSchema(`
                properties:
                    foo:
                        type: array
                        items:
                            type: object
                            x-kubernetes-map-type: atomic
                            properties:
                                key:
                                    type: string
                                bar:
                                    type: string
                        x-kubernetes-list-type: set
            `),
			RootObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: newValue2
            `),
			RootOldObject: mustUnstructured(`
                foo:
                - key: key1
                  bar: value1
                - key: key2
                  bar: value2
            `),
			KeyPath:  []interface{}{"foo", 0, "key"},
			NewValue: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
