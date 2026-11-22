/*
Copyright The Kubernetes Authors.

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

// 本文件直接测试 valuesreflect.go 中用于实现
// x-kubernetes-list-type=map 语义（Add 时按 key 合并、Equal 时忽略顺序）的
// Go map key 构造过程。values_test.go 中的 TestListAdd 通过 CEL 求值做端到端覆盖，
// 这里的测试则断言 key 的具体形态——包括超过 3 个 key 属性时使用的
// fmt %#v 序列化字符串形式——使构造结果直接可见于断言中。
//
// 存在两个 key 构造函数，两者的 key 永不互相比较：
//
//   - typedMapList.toMapKey 通过反射按未转义的 JSON 属性名读取结构体字段，
//     并保留字段声明的 Go 类型（例如 int32 字段贡献 int32）。
//     指针字段会被解引用，因此按值而非按地址比较。
//
//   - refValMapKey 通过 CEL accessor 按转义后的属性名读取 key 值。
//     accessor 返回 CEL 规范化后的值（所有整数为 int64、具名字符串类型为 string、
//     指针已解引用），正是这一点让同一逻辑元素在 typed struct、unstructured map
//     与 CEL 字面量 map 三种表示下得到相同的 key。
//
// This file directly tests construction of the golang map keys used to implement
// x-kubernetes-list-type=map semantics (merge on Add, order-insensitive Equal) in
// valuesreflect.go. TestListAdd in values_test.go covers the same dimensions end-to-end
// through CEL expression evaluation; the tests here assert the exact keys produced —
// including the fmt %#v serialized string form used for more than 3 key props — so that the
// constructed keys are directly visible in the test expectations.
//
// Two key constructors exist, and keys from one are never compared with keys from the other:
//
//   - typedMapList.toMapKey reads struct fields reflectively by unescaped JSON property name
//     and preserves the declared Go types of the fields (e.g. an int32 key field contributes
//     an int32). Pointer fields are dereferenced so keys compare by value, not by address.
//
//   - refValMapKey reads key values through CEL accessors by escaped property name. The
//     accessors return CEL-normalized values (all ints as int64, named string types as string,
//     pointers dereferenced), which is what makes keys agree across typed struct, unstructured
//     map and CEL literal map representations of the same logical element.

import (
	"reflect"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// keyTestEntry 覆盖 key 构造中会遇到的各种字段形态：
// 普通值、int32、需转义的 CEL 保留字、指针、以及不可比较的 []byte。
type keyTestEntry struct {
	Name     string  `json:"name"`
	Port     int32   `json:"port"`
	Protocol string  `json:"protocol"`
	Extra    string  `json:"extra"`
	If       string  `json:"if"` // CEL reserved word, escapes to __if__
	PtrName  *string `json:"ptrName"`
	PtrPort  *int32  `json:"ptrPort"`
	Raw      []byte  `json:"raw"` // not comparable in go
}

// keyTestWideEntry 提供 4 个 key 属性，用于触发序列化 key 的路径，
// 并验证值中空白不会造成 key 碰撞。
type keyTestWideEntry struct {
	K1 string `json:"k1"`
	K2 string `json:"k2"`
	K3 string `json:"k3"`
	K4 string `json:"k4"`
}

// TestTypedMapListToMapKey 验证 typedMapList.toMapKey 生成的 key：
// 1~3 个属性使用数组 key（保留 Go 类型），4 个及以上退化为 %#v 序列化字符串；
// 指针按被指向的值比较；不可比较值同样走序列化；非法输入返回 *types.Err。
func TestTypedMapListToMapKey(t *testing.T) {
	cases := []struct {
		name                string
		keyProps            []string // unescaped JSON names
		element             interface{}
		want                interface{}
		wantErr             bool
		sameKeyElement      interface{} // If set, test that this key is considered equivalent to keyProps (useful for testing pointer keys)
		differentKeyElement interface{} // If set, test that this key is considered non-equivalent to keyProps (provides a negative test when testing pointer keys)
	}{
		{
			name:     "one key prop",
			keyProps: []string{"name"},
			element:  keyTestEntry{Name: "a"},
			want:     "a",
		},
		{
			name:     "two key props preserve declared go types",
			keyProps: []string{"name", "port"},
			element:  keyTestEntry{Name: "a", Port: 8080},
			want:     [2]interface{}{"a", int32(8080)},
		},
		{
			name:     "three key props",
			keyProps: []string{"name", "port", "protocol"},
			element:  keyTestEntry{Name: "a", Port: 8080, Protocol: "TCP"},
			want:     [3]interface{}{"a", int32(8080), "TCP"},
		},
		{
			name:     "four key props serialize to a string",
			keyProps: []string{"name", "port", "protocol", "extra"},
			element:  keyTestEntry{Name: "a", Port: 8080, Protocol: "TCP", Extra: "x"},
			want:     `[]interface {}{"a", 8080, "TCP", "x"}`,
		},
		{
			name:                "serialized keys keep whitespace boundaries distinct",
			keyProps:            []string{"k1", "k2", "k3", "k4"},
			element:             keyTestWideEntry{K1: "a b", K2: "c", K3: "d", K4: "e"},
			want:                `[]interface {}{"a b", "c", "d", "e"}`,
			differentKeyElement: keyTestWideEntry{K1: "a", K2: "b c", K3: "d", K4: "e"},
		},
		{
			name:     "key prop that requires escaping in CEL is unescaped here",
			keyProps: []string{"if"},
			element:  keyTestEntry{If: "cond"},
			want:     "cond",
		},
		{
			name:           "pointer key prop is dereferenced",
			keyProps:       []string{"ptrName"},
			element:        keyTestEntry{PtrName: new("a")},
			want:           "a",
			sameKeyElement: keyTestEntry{PtrName: new("a")},
		},
		{
			name:                "two pointer key props compare by pointed-to values",
			keyProps:            []string{"ptrName", "ptrPort"},
			element:             keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8080))},
			want:                [2]interface{}{"a", int32(8080)},
			sameKeyElement:      keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8080))},
			differentKeyElement: keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8081))},
		},
		{
			name:     "nil pointer key props contribute nil",
			keyProps: []string{"ptrName", "ptrPort"},
			element:  keyTestEntry{},
			want:     [2]interface{}{nil, nil},
		},
		{
			name:           "serialized keys contain pointed-to values, not addresses",
			keyProps:       []string{"ptrName", "ptrPort", "name", "extra"},
			element:        keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8080)), Name: "n", Extra: "x"},
			want:           `[]interface {}{"a", 8080, "n", "x"}`,
			sameKeyElement: keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8080)), Name: "n", Extra: "x"},
		},
		{
			name:     "non-comparable key value is serialized",
			keyProps: []string{"raw"},
			element:  keyTestEntry{Raw: []byte("ab")},
			want:     `[]byte{0x61, 0x62}`,
		},
		{
			name:     "non-struct element returns an error key",
			keyProps: []string{"name"},
			element:  "not a struct",
			wantErr:  true,
		},
		{
			name:     "unknown key prop returns an error key",
			keyProps: []string{"nosuch"},
			element:  keyTestEntry{Name: "a"},
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := &typedMapList{
				typedList:       typedList{value: reflect.ValueOf([]interface{}{tc.element})},
				keyProps:        tc.keyProps,
				escapedKeyProps: escapeKeyProps(tc.keyProps),
			}
			got := list.toMapKey(reflect.ValueOf(tc.element))
			if tc.wantErr {
				// 错误以 CEL 的 *types.Err 形式返回，作为 key 仍可用于相等比较。
				if _, isErr := got.(*types.Err); !isErr {
					t.Errorf("toMapKey() = %#v (%T), want *types.Err", got, got)
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("toMapKey() = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
			// 指针 key 的等价性：不同地址、相同值必须得到同一个 key（可直接 == 比较）。
			if tc.sameKeyElement != nil {
				same := list.toMapKey(reflect.ValueOf(tc.sameKeyElement))
				if got != same {
					t.Errorf("elements with equal key values produced distinct keys: %#v != %#v", got, same)
				}
			}
			// 反向验证：值不同必须得到不同 key，防止 key 构造丢失字段。
			if tc.differentKeyElement != nil {
				different := list.toMapKey(reflect.ValueOf(tc.differentKeyElement))
				if got == different {
					t.Errorf("elements with different key values produced the same key: %#v", got)
				}
			}
		})
	}
}

// TestRefValMapKey 验证 refValMapKey 通过 CEL accessor 构造 key，
// 并断言同一逻辑元素在 typed struct 与 CEL 原生 map 两种表示下得到相同 key——
// 这是 list-type=map 在混合类型输入下正确合并的前提。
func TestRefValMapKey(t *testing.T) {
	// typedStructOf 构造一个无 schema 的 typedStruct，使属性查找按 Go 结构体字段进行。
	typedStructOf := func(v interface{}) ref.Val {
		return &typedStruct{
			value:      reflect.ValueOf(v),
			propSchema: func(key string) (Schema, bool) { return nil, true },
		}
	}

	cases := []struct {
		name             string
		escapedKeyProps  []string // escaped CEL identifiers
		element          interface{}
		want             interface{}
		equivalentCELMap map[string]interface{} // If set, is the same logical map.
	}{
		{
			name:             "one key prop",
			escapedKeyProps:  []string{"name"},
			element:          keyTestEntry{Name: "a"},
			want:             "a",
			equivalentCELMap: map[string]interface{}{"name": "a"},
		},
		{
			name:             "int key props normalize to int64 across representations",
			escapedKeyProps:  []string{"name", "port"},
			element:          keyTestEntry{Name: "a", Port: 8080}, // int32 field
			want:             [2]interface{}{"a", int64(8080)},
			equivalentCELMap: map[string]interface{}{"name": "a", "port": 8080}, // go int
		},
		{
			name:             "reserved word key prop is looked up by escaped name",
			escapedKeyProps:  []string{"__if__"},
			element:          keyTestEntry{If: "cond"},
			want:             "cond",
			equivalentCELMap: map[string]interface{}{"__if__": "cond"},
		},
		{
			name:            "three key props",
			escapedKeyProps: []string{"name", "port", "protocol"},
			element:         keyTestEntry{Name: "a", Port: 8080, Protocol: "TCP"},
			want:            [3]interface{}{"a", int64(8080), "TCP"},
		},
		{
			name:             "four key props serialize to a string",
			escapedKeyProps:  []string{"name", "port", "protocol", "extra"},
			element:          keyTestEntry{Name: "a", Port: 8080, Protocol: "TCP", Extra: "x"},
			want:             `[]interface {}{"a", 8080, "TCP", "x"}`,
			equivalentCELMap: map[string]interface{}{"name": "a", "port": 8080, "protocol": "TCP", "extra": "x"},
		},
		{
			name:             "serialized map key with whitespace in values",
			escapedKeyProps:  []string{"k1", "k2", "k3", "k4"},
			element:          keyTestWideEntry{K1: "a b", K2: "c", K3: "d", K4: "e"},
			want:             `[]interface {}{"a b", "c", "d", "e"}`,
			equivalentCELMap: map[string]interface{}{"k1": "a b", "k2": "c", "k3": "d", "k4": "e"},
		},
		{
			name:            "serialized map key with shifted whitespace boundary is distinct",
			escapedKeyProps: []string{"k1", "k2", "k3", "k4"},
			element:         keyTestWideEntry{K1: "a", K2: "b c", K3: "d", K4: "e"},
			want:            `[]interface {}{"a", "b c", "d", "e"}`,
		},
		{
			name:             "pointer key props are dereferenced and normalized",
			escapedKeyProps:  []string{"ptrName", "ptrPort"},
			element:          keyTestEntry{PtrName: new("a"), PtrPort: new(int32(8080))},
			want:             [2]interface{}{"a", int64(8080)},
			equivalentCELMap: map[string]interface{}{"ptrName": "a", "ptrPort": 8080},
		},
		{
			name:            "unset pointer key props contribute nil",
			escapedKeyProps: []string{"ptrName", "ptrPort"},
			element:         keyTestEntry{},
			want:            [2]interface{}{nil, nil},
		},
		{
			name:            "unresolvable key prop contributes nil",
			escapedKeyProps: []string{"nosuch"},
			element:         keyTestEntry{Name: "a"},
			want:            nil,
		},
		{
			name:            "non-comparable key value is serialized",
			escapedKeyProps: []string{"raw"},
			element:         keyTestEntry{Raw: []byte("ab")},
			want:            `[]byte{0x61, 0x62}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := refValMapKey(typedStructOf(tc.element), tc.escapedKeyProps)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("refValMapKey() = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
			// 跨表示一致性：把同一逻辑元素写成 CEL map，key 必须完全相同。
			if tc.equivalentCELMap != nil {
				celMapKey := refValMapKey(types.DefaultTypeAdapter.NativeToValue(tc.equivalentCELMap), tc.escapedKeyProps)
				if got != celMapKey {
					t.Errorf("typed element key %#v differs from equivalent CEL map key %#v", got, celMapKey)
				}
			}
		})
	}
}
