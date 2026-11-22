/*
Copyright 2014 The Kubernetes Authors.

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

package queryparams_test

import (
	"net/url"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion/queryparams"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

// namedString / namedBool 验证自定义字符串/布尔类型按底层类型处理。
type namedString string
type namedBool bool

// bar 验证基本类型、omitempty、未打 tag 字段的行为；未打 tag 的字段应被忽略。
type bar struct {
	Float1   float32 `json:"float1"`
	Float2   float64 `json:"float2"`
	Int1     int64   `json:"int1,omitempty"`
	Int2     int32   `json:"int2,omitempty"`
	Int3     int16   `json:"int3,omitempty"`
	Str1     string  `json:"str1,omitempty"`
	Ignored  int
	Ignored2 string
}

func (obj *bar) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// foo 验证嵌套结构体、切片、map、命名类型与自定义类型的组合。
type foo struct {
	Str       string            `json:"str"`
	Integer   int               `json:"integer,omitempty"`
	Slice     []string          `json:"slice,omitempty"`
	Boolean   bool              `json:"boolean,omitempty"`
	NamedStr  namedString       `json:"namedStr,omitempty"`
	NamedBool namedBool         `json:"namedBool,omitempty"`
	Foobar    bar               `json:"foobar,omitempty"`
	Testmap   map[string]string `json:"testmap,omitempty"`
}

func (obj *foo) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// baz 验证指针字段：nil 且无 omitempty 时输出空串，nil 且有 omitempty 时省略。
type baz struct {
	Ptr  *int  `json:"ptr"`
	Bptr *bool `json:"bptr,omitempty"`
}

func (obj *baz) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// childStructs tests some of the types we serialize to query params for log API calls
// notably, the nested time struct
//
// 覆盖指针/非指针的 metav1.Time 以及自定义 marshaller 的 nil 处理。
type childStructs struct {
	Container      string       `json:"container,omitempty"`
	Follow         bool         `json:"follow,omitempty"`
	Previous       bool         `json:"previous,omitempty"`
	SinceSeconds   *int64       `json:"sinceSeconds,omitempty"`
	TailLines      *int64       `json:"tailLines,omitempty"`
	SinceTime      *metav1.Time `json:"sinceTime,omitempty"`
	EmptyTime      *metav1.Time `json:"emptyTime"`
	NonPointerTime metav1.Time  `json:"nonPointerTime"`
}

func (obj *childStructs) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// validateResult 双向比较 actual/expected：实际结果不得多键、不得缺键、值必须一致。
func validateResult(t *testing.T, input interface{}, actual, expected url.Values) {
	// 复制一份 expected，避免在比较过程中修改调用方的数据。
	local := url.Values{}
	for k, v := range expected {
		local[k] = v
	}
	for k, v := range actual {
		if ev, ok := local[k]; !ok || !reflect.DeepEqual(ev, v) {
			if !ok {
				t.Errorf("%#v: actual value key %s not found in expected map", input, k)
			} else {
				t.Errorf("%#v: values don't match: actual: %#v, expected: %#v", input, v, ev)
			}
		}
		delete(local, k)
	}
	if len(local) > 0 {
		t.Errorf("%#v: expected map has keys that were not found in actual map: %#v", input, local)
	}
}

// TestConvert 覆盖 queryparams.Convert 的主要编码规则：omitempty、未打 tag
// 字段忽略、内嵌结构体展开、命名类型、指针 nil 语义以及自定义 marshaller。
func TestConvert(t *testing.T) {
	sinceSeconds := int64(123)
	tailLines := int64(0)
	sinceTime := metav1.Date(2000, 1, 1, 12, 34, 56, 0, time.UTC)

	tests := []struct {
		input    interface{}
		expected url.Values
	}{
		{
			input: &foo{
				Str: "hello",
			},
			expected: url.Values{"str": {"hello"}},
		},
		{
			input: &foo{
				Str:     "test string",
				Slice:   []string{"one", "two", "three"},
				Integer: 234,
				Boolean: true,
			},
			expected: url.Values{"str": {"test string"}, "slice": {"one", "two", "three"}, "integer": {"234"}, "boolean": {"true"}},
		},
		{
			input: &foo{
				Str:       "named types",
				NamedStr:  "value1",
				NamedBool: true,
			},
			expected: url.Values{"str": {"named types"}, "namedStr": {"value1"}, "namedBool": {"true"}},
		},
		{
			input: &foo{
				Str: "don't ignore embedded struct",
				Foobar: bar{
					Float1: 5.0,
				},
			},
			expected: url.Values{"str": {"don't ignore embedded struct"}, "float1": {"5"}, "float2": {"0"}},
		},
		{
			// Ignore untagged fields
			// 未打 json tag 的字段不进入 query 参数。
			input: &bar{
				Float1:   23.5,
				Float2:   100.7,
				Int1:     1,
				Int2:     2,
				Int3:     3,
				Ignored:  1,
				Ignored2: "ignored",
			},
			expected: url.Values{"float1": {"23.5"}, "float2": {"100.7"}, "int1": {"1"}, "int2": {"2"}, "int3": {"3"}},
		},
		{
			// include fields that are not tagged omitempty
			input: &foo{
				NamedStr: "named str",
			},
			expected: url.Values{"str": {""}, "namedStr": {"named str"}},
		},
		{
			input: &baz{
				Ptr:  ptr.To(5),
				Bptr: ptr.To(true),
			},
			expected: url.Values{"ptr": {"5"}, "bptr": {"true"}},
		},
		{
			input: &baz{
				Bptr: ptr.To(true),
			},
			expected: url.Values{"ptr": {""}, "bptr": {"true"}},
		},
		{
			input: &baz{
				Ptr: ptr.To(5),
			},
			expected: url.Values{"ptr": {"5"}},
		},
		{
			input: &childStructs{
				Container:      "mycontainer",
				Follow:         true,
				Previous:       true,
				SinceSeconds:   &sinceSeconds,
				TailLines:      nil,
				SinceTime:      &sinceTime, // test a custom marshaller
				EmptyTime:      nil,        // test a nil custom marshaller without omitempty
				NonPointerTime: sinceTime,
			},
			expected: url.Values{"container": {"mycontainer"}, "follow": {"true"}, "previous": {"true"}, "sinceSeconds": {"123"}, "sinceTime": {"2000-01-01T12:34:56Z"}, "emptyTime": {""}, "nonPointerTime": {"2000-01-01T12:34:56Z"}},
		},
		{
			input: &childStructs{
				Container:      "mycontainer",
				Follow:         true,
				Previous:       true,
				SinceSeconds:   &sinceSeconds,
				TailLines:      &tailLines,
				SinceTime:      nil, // test a nil custom marshaller with omitempty
				NonPointerTime: sinceTime,
			},
			expected: url.Values{"container": {"mycontainer"}, "follow": {"true"}, "previous": {"true"}, "sinceSeconds": {"123"}, "tailLines": {"0"}, "emptyTime": {""}, "nonPointerTime": {"2000-01-01T12:34:56Z"}},
		},
	}

	for _, test := range tests {
		result, err := queryparams.Convert(test.input)
		if err != nil {
			t.Errorf("Unexpected error while converting %#v: %v", test.input, err)
		}
		validateResult(t, test.input, result, test.expected)
	}
}
