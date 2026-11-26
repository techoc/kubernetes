/*
Copyright 2015 The Kubernetes Authors.

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

package json_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"
	"k8s.io/apimachinery/pkg/util/diff"

	"github.com/google/go-cmp/cmp"
)

// testDecodable 是解码测试的核心目标类型：包含带 tag/不带 tag 的字段、
// 嵌套结构体与 interface{} 字段，用来验证 GVK 推断与字段匹配规则。
type testDecodable struct {
	metav1.TypeMeta `json:""`

	Other     string
	Value     int           `json:"value"`
	Spec      DecodableSpec `json:"spec"`
	Interface interface{}   `json:"interface"`
}

// DecodableSpec 有 15 个字段。
// 中文补充：字段数量较多，用来验证编码器对“大结构体”的字段匹配是否区分大小写。
type DecodableSpec struct {
	A int `json:"A"`
	B int `json:"B"`
	C int `json:"C"`
	D int `json:"D"`
	E int `json:"E"`
	F int `json:"F"`
	G int `json:"G"`
	H int `json:"h"`
	I int `json:"i"`
	J int `json:"j"`
	K int `json:"k"`
	L int `json:"l"`
	M int `json:"m"`
	N int `json:"n"`
	O int `json:"o"`
}

// DeepCopyObject 实现 runtime.Object；nil 接收者返回 nil，与生成代码的惯例一致。
func (d *testDecodable) DeepCopyObject() runtime.Object {
	if d == nil {
		return nil
	}
	out := new(testDecodable)
	d.DeepCopyInto(out)
	return out
}

// DeepCopyInto 逐字段复制；interface{} 字段直接赋引用即可满足测试需要。
func (d *testDecodable) DeepCopyInto(out *testDecodable) {
	*out = *d
	out.Other = d.Other
	out.Value = d.Value
	out.Spec = d.Spec
	out.Interface = d.Interface
	return
}

// testDecodeCoercion 覆盖严格解码时各种 JSON 类型到 Go 类型的“强制转换”行为
// （null、空字符串、空数组/对象、整数与浮点数）。
type testDecodeCoercion struct {
	metav1.TypeMeta `json:""`

	Bool bool `json:"bool"`

	Int   int `json:"int"`
	Int32 int `json:"int32"`
	Int64 int `json:"int64"`

	Float32 float32 `json:"float32"`
	Float64 float64 `json:"float64"`

	String string `json:"string"`

	Struct testDecodable `json:"struct"`

	Array []string          `json:"array"`
	Map   map[string]string `json:"map"`
}

// DeepCopyObject 实现 runtime.Object。
func (d *testDecodeCoercion) DeepCopyObject() runtime.Object {
	if d == nil {
		return nil
	}
	out := new(testDecodeCoercion)
	d.DeepCopyInto(out)
	return out
}

// DeepCopyInto 直接复制值；该类型没有需要深拷贝的引用字段。
func (d *testDecodeCoercion) DeepCopyInto(out *testDecodeCoercion) {
	*out = *d
	return
}

// TestDecode 是 JSON/YAML 解码行为的总表驱动测试，覆盖：
// GVK 缺失与补全、unstructured 与结构化目标、creater/typer 错误传播、
// 大小写敏感匹配、严格模式下的未知字段/重复字段、以及类型强制转换。
func TestDecode(t *testing.T) {
	type testCase struct {
		creater runtime.ObjectCreater
		typer   runtime.ObjectTyper
		yaml    bool
		pretty  bool
		strict  bool

		data       []byte
		defaultGVK *schema.GroupVersionKind
		into       runtime.Object

		errFn          func(error) bool
		expectedObject runtime.Object
		expectedGVK    *schema.GroupVersionKind
	}

	testCases := []testCase{
		// 缺少 metadata，未提供 into，带类型的 creator
		{
			data: []byte("{}"),

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data: []byte("{}"),

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data: []byte(`{"kind":"Foo"}`),

			expectedGVK: &schema.GroupVersionKind{Kind: "Foo"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'apiVersion' is missing in") },
		},
		{
			data: []byte(`{"kind":"Foo"}`),

			expectedGVK: &schema.GroupVersionKind{Kind: "Foo"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'apiVersion' is missing in") },
			strict:      true,
		},

		{
			data: []byte(`{"apiVersion":"foo/v1"}`),

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data: []byte(`{"apiVersion":"foo/v1"}`),

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:    []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &testDecodable{}},

			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "/v1", Kind: "Foo"}},
			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
		},
		{
			data:    []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &testDecodable{}},

			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "/v1", Kind: "Foo"}},
			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			strict:         true,
		},

		// 缺少 metadata，提供 unstructured into
		{
			data:  []byte("{}"),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:  []byte("{}"),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:  []byte(`{"kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK:    &schema.GroupVersionKind{Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Foo"}},
			// TODO(109023): 这里应当报错；目前 unstructured 解码只要求 kind，不要求 apiVersion。
		},
		{
			data:  []byte(`{"kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK:    &schema.GroupVersionKind{Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Foo"}},
			strict:         true,
			// TODO(109023): 这里应当报错；目前 unstructured 解码只要求 kind，不要求 apiVersion。
		},

		{
			data:  []byte(`{"apiVersion":"foo/v1"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:  []byte(`{"apiVersion":"foo/v1"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:  []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
		},
		{
			data:  []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
			strict:         true,
		},

		// 缺少 metadata，unstructured into 中自带 metadata
		{
			data:  []byte("{}"),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:  []byte("{}"),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:  []byte(`{"kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK:    &schema.GroupVersionKind{Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Foo"}},
			// TODO(109023): 这里应当报错；目前 unstructured 解码只要求 kind，不要求 apiVersion。
		},
		{
			data:  []byte(`{"kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK:    &schema.GroupVersionKind{Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"kind": "Foo"}},
			strict:         true,
			// TODO(109023): 这里应当报错；目前 unstructured 解码只要求 kind，不要求 apiVersion。
		},

		{
			data:  []byte(`{"apiVersion":"foo/v1"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:  []byte(`{"apiVersion":"foo/v1"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:  []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
		},
		{
			data:  []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			into:  &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "into/v1", "kind": "Into"}},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
			strict:         true,
		},

		// 缺少 metadata，未提供 into，unstructured creator
		{
			data:    []byte("{}"),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:    []byte("{}"),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:    []byte(`{"kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{Kind: "Foo"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'apiVersion' is missing in") },
		},
		{
			data:    []byte(`{"kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{Kind: "Foo"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'apiVersion' is missing in") },
			strict:      true,
		},

		{
			data:    []byte(`{"apiVersion":"foo/v1"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
		},
		{
			data:    []byte(`{"apiVersion":"foo/v1"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK: &schema.GroupVersionKind{Group: "foo", Version: "v1"},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'Kind' is missing in") },
			strict:      true,
		},

		{
			data:    []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
		},
		{
			data:    []byte(`{"apiVersion":"/v1","kind":"Foo"}`),
			typer:   &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			creater: &mockCreater{obj: &unstructured.Unstructured{}},

			expectedGVK:    &schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Foo"},
			expectedObject: &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "/v1", "kind": "Foo"}},
			strict:         true,
		},

		// creator 返回错误
		{
			data:       []byte("{}"),
			defaultGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:    &mockCreater{err: fmt.Errorf("fake error")},

			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn:       func(err error) bool { return err.Error() == "fake error" },
		},
		{
			data:       []byte("{}"),
			defaultGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:    &mockCreater{err: fmt.Errorf("fake error")},

			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn:       func(err error) bool { return err.Error() == "fake error" },
		},
		// 带类型的 creator
		{
			data:           []byte("{}"),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
		},
		{
			data:           []byte("{}"),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			strict:         true,
		},

		// 只有 version 没有 group 时不做默认值填充
		{
			data:           []byte(`{"apiVersion":"blah"}`),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "", Version: "blah"},
		},
		// 只有 group 没有 version 时会填充默认值
		{
			data:           []byte(`{"apiVersion":"other/"}`),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "other/"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
		},
		// 已提供 group version，kind 会被填充默认值
		{
			data:           []byte(`{"apiVersion":"other1/blah1"}`),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "other1/blah1"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other1", Version: "blah1"},
		},
		// gvk 齐全时不做任何默认值填充
		{
			data:           []byte(`{"kind":"Test","apiVersion":"other/blah"}`),
			defaultGVK:     &schema.GroupVersionKind{Kind: "Test1", Group: "other1", Version: "blah1"},
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
		},
		// 数据中未提供 kind 时，用 into 的 kind 填充 defaultGVK
		{
			data:           []byte(`{"apiVersion":"b1/c1"}`),
			into:           &testDecodable{TypeMeta: metav1.TypeMeta{Kind: "a3", APIVersion: "b1/c1"}},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "a3", Group: "b1", Version: "c1"}},
			defaultGVK:     nil,
			creater:        &mockCreater{obj: &testDecodable{}},
			expectedObject: &testDecodable{TypeMeta: metav1.TypeMeta{Kind: "a3", APIVersion: "b1/c1"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "a3", Group: "b1", Version: "c1"},
		},

		// 接受 runtime.Unknown 作为 into，并绕过 creator
		{
			data: []byte(`{}`),
			into: &runtime.Unknown{},

			expectedGVK: &schema.GroupVersionKind{},
			expectedObject: &runtime.Unknown{
				Raw:         []byte(`{}`),
				ContentType: runtime.ContentTypeJSON,
			},
		},
		{
			data: []byte(`{"test":"object"}`),
			into: &runtime.Unknown{},

			expectedGVK: &schema.GroupVersionKind{},
			expectedObject: &runtime.Unknown{
				Raw:         []byte(`{"test":"object"}`),
				ContentType: runtime.ContentTypeJSON,
			},
		},
		{
			data:        []byte(`{"test":"object"}`),
			into:        &runtime.Unknown{},
			defaultGVK:  &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &runtime.Unknown{
				TypeMeta:    runtime.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Raw:         []byte(`{"test":"object"}`),
				ContentType: runtime.ContentTypeJSON,
			},
		},

		// 未注册的对象可以直接解码到 into
		{
			data:        []byte(`{"kind":"Test","apiVersion":"other/blah","value":1,"Other":"test"}`),
			into:        &testDecodable{},
			typer:       &mockTyper{err: runtime.NewNotRegisteredErrForKind("mock", schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"})},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Other:    "test",
				Value:    1,
			},
		},
		// 已注册类型按 into 对象的 kind 填充默认值
		{
			data:        []byte(`{"value":1,"Other":"test"}`),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				Other: "test",
				Value: 1,
			},
		},
		// 已注册类型即使没有 version 也会按 into 的 kind 填充默认值，但会返回错误
		{
			data:        []byte(`{"value":1,"Other":"test"}`),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: ""}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: ""},
			errFn:       func(err error) bool { return strings.Contains(err.Error(), "Object 'apiVersion' is missing in") },
			expectedObject: &testDecodable{
				Other: "test",
				Value: 1,
			},
		},
		// 非法数字应报错
		{
			data:        []byte(`{"kind":"Test","apiVersion":"other/blah","interface":1e1000}`),
			creater:     &mockCreater{obj: &testDecodable{}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `json: cannot unmarshal number 1e1000 into Go struct field testDecodable.interface of type float64`)
			},
		},
		// 反序列化区分大小写
		{
			// "VaLue" 本应是 "value"
			data:        []byte(`{"kind":"Test","apiVersion":"other/blah","VaLue":1,"Other":"test"}`),
			into:        &testDecodable{},
			typer:       &mockTyper{err: runtime.NewNotRegisteredErrForKind("mock", schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"})},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Other:    "test",
			},
		},
		// 大结构体的反序列化同样区分大小写。
		{
			// "b" 本应是 "B"，"I" 本应是 "i"
			data:        []byte(`{"kind":"Test","apiVersion":"other/blah","spec": {"A": 1, "b": 2, "h": 3, "I": 4}}`),
			into:        &testDecodable{},
			typer:       &mockTyper{err: runtime.NewNotRegisteredErrForKind("mock", schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"})},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Spec:     DecodableSpec{A: 1, H: 3},
			},
		},
		// 严格 JSON 反序列化遇到未知字段应报错。
		{
			data:        []byte(`{"unknown": 1}`),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `unknown field "unknown"`)
			},
			strict: true,
		},
		// 严格 YAML 反序列化遇到未知字段应报错。
		{
			data:        []byte("unknown: 1\n"),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `unknown field "unknown"`)
			},
			yaml:   true,
			strict: true,
		},
		// 严格 JSON 反序列化遇到重复字段应报错。
		{
			data:        []byte(`{"value":1,"value":1}`),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `duplicate field "value"`)
			},
			strict: true,
		},
		// 严格 YAML 反序列化遇到重复字段应报错。
		{
			data: []byte("value: 1\n" +
				"value: 1\n"),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `"value" already set in map`)
			},
			yaml:   true,
			strict: true,
		},
		// 严格 JSON 反序列化 unstructured 时遇到重复字段应报错。
		{
			data:        []byte(`{"kind":"Custom","value":1,"value":1}`),
			into:        &unstructured.Unstructured{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Custom"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `duplicate field "value"`)
			},
			strict: true,
		},
		// 严格 YAML 反序列化 unstructured 时遇到重复字段应报错。
		{
			data: []byte("kind: Custom\n" +
				"value: 1\n" +
				"value: 1\n"),
			into:        &unstructured.Unstructured{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Custom"},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `"value" already set in map`)
			},
			yaml:   true,
			strict: true,
		},
		// 严格 JSON 直接解码到未注册对象。
		{
			data:        []byte(`{"kind":"Test","apiVersion":"other/blah","value":1,"Other":"test"}`),
			into:        &testDecodable{},
			typer:       &mockTyper{err: runtime.NewNotRegisteredErrForKind("mock", schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"})},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Other:    "test",
				Value:    1,
			},
			strict: true,
		},
		// 严格 YAML 直接解码到未注册对象。
		{
			data: []byte("kind: Test\n" +
				"apiVersion: other/blah\n" +
				"value: 1\n" +
				"Other: test\n"),
			into:        &testDecodable{},
			typer:       &mockTyper{err: runtime.NewNotRegisteredErrForKind("mock", schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"})},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				TypeMeta: metav1.TypeMeta{APIVersion: "other/blah", Kind: "Test"},
				Other:    "test",
				Value:    1,
			},
			yaml:   true,
			strict: true,
		},
		// 无 GVK 的合法严格 JSON 解码。
		{
			data:        []byte(`{"value":1234}`),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				Value: 1234,
			},
			strict: true,
		},
		// 无 GVK 的合法严格 YAML 解码。
		{
			data:        []byte("value: 1234\n"),
			into:        &testDecodable{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodable{
				Value: 1234,
			},
			yaml:   true,
			strict: true,
		},
		// 非法严格 JSON，应产生 JSON 解析错误：
		{
			data:  []byte("foo"),
			into:  &unstructured.Unstructured{},
			typer: &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `json parse error: invalid character 'o'`)
			},
			strict: true,
		},
		// 空 JSON 严格解码，应产生缺少 kind 的错误
		{
			data:        []byte("{}"),
			into:        &unstructured.Unstructured{},
			typer:       &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK: &schema.GroupVersionKind{},
			errFn: func(err error) bool {
				return strings.Contains(err.Error(), `Object 'Kind' is missing`)
			},
			strict: true,
		},
		// 从 null 强制转换
		{
			data:           []byte(`{"bool":null,"int":null,"int32":null,"int64":null,"float32":null,"float64":null,"string":null,"array":null,"map":null,"struct":null}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{},
			strict:         true,
		},
		{
			data:           []byte(`{"bool":null,"int":null,"int32":null,"int64":null,"float32":null,"float64":null,"string":null,"array":null,"map":null,"struct":null}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{},
			yaml:           true,
			strict:         true,
		},
		// 从 string 强制转换
		{
			data:           []byte(`{"string":""}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{},
			strict:         true,
		},
		{
			data:           []byte(`{"string":""}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{},
			yaml:           true,
			strict:         true,
		},
		// 从 array 强制转换
		{
			data:           []byte(`{"array":[]}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Array: []string{}},
			strict:         true,
		},
		{
			data:           []byte(`{"array":[]}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Array: []string{}},
			yaml:           true,
			strict:         true,
		},
		// 从 map 强制转换
		{
			data:           []byte(`{"map":{},"struct":{}}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Map: map[string]string{}},
			strict:         true,
		},
		{
			data:           []byte(`{"map":{},"struct":{}}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Map: map[string]string{}},
			yaml:           true,
			strict:         true,
		},
		// 从 int 强制转换
		{
			data:           []byte(`{"int":1,"int32":1,"int64":1,"float32":1,"float64":1}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Int: 1, Int32: 1, Int64: 1, Float32: 1, Float64: 1},
			strict:         true,
		},
		{
			data:           []byte(`{"int":1,"int32":1,"int64":1,"float32":1,"float64":1}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Int: 1, Int32: 1, Int64: 1, Float32: 1, Float64: 1},
			yaml:           true,
			strict:         true,
		},
		// 从 float 强制转换
		{
			data:           []byte(`{"float32":1.0,"float64":1.0}`),
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Float32: 1, Float64: 1},
			strict:         true,
		},
		{
			data:           []byte(`{"int":1.0,"int32":1.0,"int64":1.0,"float32":1.0,"float64":1.0}`), // floating point gets dropped in yaml -> json step
			into:           &testDecodeCoercion{},
			typer:          &mockTyper{gvk: &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"}},
			expectedGVK:    &schema.GroupVersionKind{Kind: "Test", Group: "other", Version: "blah"},
			expectedObject: &testDecodeCoercion{Int: 1, Int32: 1, Int64: 1, Float32: 1, Float64: 1},
			yaml:           true,
			strict:         true,
		},
	}

	logTestCase := func(t *testing.T, tc testCase) {
		t.Logf("data=%s\n\tinto=%T, yaml=%v, strict=%v", string(tc.data), tc.into, tc.yaml, tc.strict)
	}

	// 每个用例独立构造序列化器，避免 yaml/strict 选项相互污染。
	for i, test := range testCases {
		var s runtime.Serializer
		if test.yaml {
			s = json.NewSerializerWithOptions(json.DefaultMetaFactory, test.creater, test.typer, json.SerializerOptions{Yaml: test.yaml, Pretty: false, Strict: test.strict})
		} else {
			s = json.NewSerializerWithOptions(json.DefaultMetaFactory, test.creater, test.typer, json.SerializerOptions{Yaml: test.yaml, Pretty: test.pretty, Strict: test.strict})
		}
		obj, gvk, err := s.Decode([]byte(test.data), test.defaultGVK, test.into)

		if !reflect.DeepEqual(test.expectedGVK, gvk) {
			logTestCase(t, test)
			t.Errorf("%d: unexpected GVK: %v", i, gvk)
		}

		switch {
		case err == nil && test.errFn != nil:
			logTestCase(t, test)
			t.Errorf("%d: failed: not getting the expected error", i)
			continue
		case err != nil && test.errFn == nil:
			logTestCase(t, test)
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil:
			if !test.errFn(err) {
				logTestCase(t, test)
				t.Errorf("%d: failed: %v", i, err)
			}
			// 严格解码错误属于“部分成功”：对象可能已被填充，因此允许非 nil 返回。
			if !runtime.IsStrictDecodingError(err) && obj != nil {
				logTestCase(t, test)
				t.Errorf("%d: should have returned nil object", i)
			}
			continue
		}

		// 传入 into 时，解码器应复用并返回同一对象，而不是新建实例。
		if test.into != nil && test.into != obj {
			logTestCase(t, test)
			t.Errorf("%d: expected into to be returned: %v", i, obj)
			continue
		}

		if !reflect.DeepEqual(test.expectedObject, obj) {
			logTestCase(t, test)
			t.Errorf("%d: unexpected object:\n%s", i, diff.ObjectGoPrintSideBySide(test.expectedObject, obj))
		}
	}
}

// TestCacheableObject 复用 runtime/testing 的通用用例，验证 JSON 序列化器
// 满足 CacheableObject 的缓存/编码约定。
func TestCacheableObject(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "group", Version: "version", Kind: "MockCacheableObject"}
	creater := &mockCreater{obj: &runtimetesting.MockCacheableObject{}}
	typer := &mockTyper{gvk: &gvk}
	serializer := json.NewSerializerWithOptions(json.DefaultMetaFactory, creater, typer, json.SerializerOptions{})

	runtimetesting.CacheableObjectTest(t, serializer)
}

// mockCreater 是 runtime.ObjectCreater 的替身：记录请求的 GVK 并返回预置对象或错误。
type mockCreater struct {
	apiVersion string
	kind       string
	err        error
	obj        runtime.Object
}

// New 记录调用参数并返回配置的结果，便于断言解码器是否按 GVK 请求对象。
func (c *mockCreater) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	c.apiVersion, c.kind = kind.GroupVersion().String(), kind.Kind
	return c.obj, c.err
}

// mockTyper 是 runtime.ObjectTyper 的替身：固定返回一个 GVK（或错误）。
type mockTyper struct {
	gvk *schema.GroupVersionKind
	err error
}

// ObjectKinds 返回预置 GVK；gvk 为 nil 时返回 nil 列表与配置的错误。
func (t *mockTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	if t.gvk == nil {
		return nil, false, t.err
	}
	return []schema.GroupVersionKind{*t.gvk}, false, t.err
}

// Recognizes 固定返回 false：测试用例通过 ObjectKinds 的错误来控制“未注册”场景。
func (t *mockTyper) Recognizes(_ schema.GroupVersionKind) bool {
	return false
}

// testEncodableDuplicateTag 的两个字段使用相同 json tag，验证编码器会全部忽略而不是报错。
type testEncodableDuplicateTag struct {
	metav1.TypeMeta `json:""`

	A1 int `json:"a"`
	A2 int `json:"a"` //nolint:govet // This is intentional to test that the encoder will not encode two map entries with the same key.
}

// DeepCopyObject 故意 panic：编码测试不涉及深拷贝。
func (testEncodableDuplicateTag) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// testEncodableTagMatchesUntaggedName 验证“有 tag 的字段优先于同名的无 tag 字段”。
type testEncodableTagMatchesUntaggedName struct {
	metav1.TypeMeta `json:""`

	A       int
	TaggedA int `json:"A"`
}

// DeepCopyObject 故意 panic：编码测试不涉及深拷贝。
func (testEncodableTagMatchesUntaggedName) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// staticTextMarshaler 的所有取值都序列化为同一个字符串 "static"，
// 用来构造“键不相等但序列化后重复”的 map。
type staticTextMarshaler int

// MarshalText 恒定返回 "static"，使不同的 key 在 JSON 中变成同一个属性名。
func (staticTextMarshaler) MarshalText() ([]byte, error) {
	return []byte("static"), nil
}

// testEncodableMap 是可用作 JSON 对象键的 map 类型，用于测试键的 TextMarshaler 行为。
type testEncodableMap[K comparable] map[K]interface{}

// GetObjectKind 故意 panic：该类型仅用于编码测试。
func (testEncodableMap[K]) GetObjectKind() schema.ObjectKind {
	panic("unimplemented")
}

// DeepCopyObject 故意 panic：该类型仅用于编码测试。
func (testEncodableMap[K]) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// TestEncode 验证编码器遵循 encoding/json 的字段可见性规则：
// 同层存在 tag 字段时只编码 tag 字段；重复 tag 的字段全部忽略；
// 以及 TextMarshaler 键可能产生重复属性名（不报错）。
func TestEncode(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   runtime.Object
		want []byte
	}{
		// JSON 在决定序列化哪些字段时，会对 Go 的结构体字段可见性规则做修正。
		// 若最浅层（即按常规 Go 规则会被选中的层）存在多个同名字段，则适用以下额外规则：

		// 1) 这些字段中若有带 JSON tag 的，则只考虑带 tag 的字段，
		//    even if there are multiple untagged fields that would otherwise conflict.
		{
			name: "only tagged field is considered if any are tagged",
			in: &testEncodableTagMatchesUntaggedName{
				A:       1,
				TaggedA: 2,
			},
			want: []byte("{\"A\":2}\n"),
		},
		// 2) 若（按第一条规则筛选后）只剩一个字段，
		//    that is selected.
		// 3) 否则存在多个字段，全部忽略且不报错。
		{
			name: "all duplicate fields are ignored",
			in:   &testEncodableDuplicateTag{},
			want: []byte("{}\n"),
		},
		{
			name: "text marshaler keys can compare inequal but serialize to duplicates",
			in: testEncodableMap[staticTextMarshaler]{
				staticTextMarshaler(1): nil,
				staticTextMarshaler(2): nil,
			},
			want: []byte("{\"static\":null,\"static\":null}\n"),
		},
		{
			name: "time.Time keys can compare inequal but serialize to duplicates because time.Time implements TextMarshaler",
			in: testEncodableMap[time.Time]{
				time.Date(2222, 11, 30, 23, 59, 58, 57, time.UTC):              nil,
				time.Date(2222, 11, 30, 23, 59, 58, 57, time.FixedZone("", 0)): nil,
			},
			want: []byte("{\"2222-11-30T23:59:58.000000057Z\":null,\"2222-11-30T23:59:58.000000057Z\":null}\n"),
		},
		{
			name: "metav1.Time keys can compare inequal but serialize to duplicates because metav1.Time embeds time.Time which implements TextMarshaler",
			in: testEncodableMap[metav1.Time]{
				metav1.Date(2222, 11, 30, 23, 59, 58, 57, time.UTC):              nil,
				metav1.Date(2222, 11, 30, 23, 59, 58, 57, time.FixedZone("", 0)): nil,
			},
			want: []byte("{\"2222-11-30T23:59:58.000000057Z\":null,\"2222-11-30T23:59:58.000000057Z\":null}\n"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dst bytes.Buffer
			s := json.NewSerializerWithOptions(json.DefaultMetaFactory, nil, nil, json.SerializerOptions{})
			if err := s.Encode(tc.in, &dst); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, dst.Bytes()); diff != "" {
				t.Errorf("unexpected output:\n%s", diff)
			}
		})
	}
}

// TestRoundtripUnstructuredFloat64 说明：把没有小数部分的 float64 编码为 JSON
// 再解码到 interface{}，可能得到具体类型为 int64 的值。原因有二：一是 JSON 中
// 无小数的 float64 与整数的编码完全相同；二是解码到 interface{} 时，只要数字
// 能被 int64 精确表示，没有小数点的 JSON 数字就会被解成 int64。
//
// 中文补充：超出 int64 精确范围的大数仍保持 float64，这解释了为何只有部分值发生类型变化。
func TestRoundtripUnstructuredFractionlessFloat64(t *testing.T) {
	s := json.NewSerializerWithOptions(json.DefaultMetaFactory, runtime.NewScheme(), runtime.NewScheme(), json.SerializerOptions{})

	initial := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion":                    "v1",
		"kind":                          "Test",
		"with-fraction":                 float64(1.5),
		"without-fraction":              float64(1),
		"without-fraction-big-positive": float64(9223372036854776000),
		"without-fraction-big-negative": float64(-9223372036854776000),
	}}

	var buf bytes.Buffer
	if err := s.Encode(initial, &buf); err != nil {
		t.Fatal(err)
	}

	final := &unstructured.Unstructured{}
	got, _, err := s.Decode(buf.Bytes(), nil, final)
	if err != nil {
		t.Fatal(err)
	}
	if got != final {
		t.Fatalf("expected Decode to return target Unstructured object but got: %v", got)
	}

	expected := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion":                    "v1",
		"kind":                          "Test",
		"with-fraction":                 float64(1.5),
		"without-fraction":              int64(1), // 注意具体类型发生了变化
		"without-fraction-big-positive": float64(9223372036854776000),
		"without-fraction-big-negative": float64(-9223372036854776000),
	}}

	if diff := cmp.Diff(expected, final); diff != "" {
		t.Fatalf("unexpected diff:\n%s", diff)
	}
}
