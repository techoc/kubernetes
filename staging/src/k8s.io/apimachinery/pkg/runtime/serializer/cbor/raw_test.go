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

package cbor

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/google/go-cmp/cmp"
)

// TestTranscodeRawTypes 验证 transcodeRawTypes 能沿结构体字段、切片、数组、map
// 和接口递归找到 RawExtension/FieldsV1，并把其中的 CBOR 负载转成 JSON。
//
// 关键边界：
//   - 只有可寻址的值才能原地修改，因此 map 值、非指针的结构体/数组元素保持原样；
//   - nil 接口与 nil 指针必须原样保留，不能被"修复"成空对象；
//   - 嵌入字段与 fmt.Stringer 接口字段也要被递归处理。
func TestTranscodeRawTypes(t *testing.T) {
	for _, tc := range []struct {
		In  interface{}
		Out interface{}
	}{
		{
			In:  nil,
			Out: nil,
		},
		{
			// 0xd9d9f707 是 CBOR 编码的整数 7，转码后变成 JSON 文本 "7"。
			In:  &runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}},
			Out: &runtime.RawExtension{Raw: []byte(`7`)},
		},
		{
			// 空 Raw 无需转码。
			In:  &runtime.RawExtension{},
			Out: &runtime.RawExtension{},
		},
		{
			// 值类型不可寻址，无法原地写回，保持原字节。
			In:  runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}},
			Out: runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}, // not addressable
		},
		{
			In:  &[...]runtime.RawExtension{{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}, {Raw: []byte{0xd9, 0xd9, 0xf7, 0x08}}, {Raw: []byte{0xd9, 0xd9, 0xf7, 0x09}}},
			Out: &[...]runtime.RawExtension{{Raw: []byte(`7`)}, {Raw: []byte(`8`)}, {Raw: []byte(`9`)}},
		},
		{
			// 数组长度为零也要能正常处理，不 panic。
			In:  &[0]runtime.RawExtension{},
			Out: &[0]runtime.RawExtension{},
		},
		{
			// 普通切片元素可寻址，能被就地转码。
			In:  &[]runtime.RawExtension{{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}, {Raw: []byte{0xd9, 0xd9, 0xf7, 0x08}}, {Raw: []byte{0xd9, 0xd9, 0xf7, 0x09}}},
			Out: &[]runtime.RawExtension{{Raw: []byte(`7`)}, {Raw: []byte(`8`)}, {Raw: []byte(`9`)}},
		},
		{
			In:  &[]runtime.RawExtension{},
			Out: &[]runtime.RawExtension{},
		},
		{
			// 无关类型原样返回。
			In:  &[]string{"foo"},
			Out: &[]string{"foo"},
		},
		{
			// 类型化的 nil 指针保持 nil，不能解引用。
			In:  (*runtime.RawExtension)(nil),
			Out: (*runtime.RawExtension)(nil),
		},
		{
			// 接口字段中的指针可寻址，可被转码。
			In:  &struct{ I fmt.Stringer }{I: &runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}},
			Out: &struct{ I fmt.Stringer }{I: &runtime.RawExtension{Raw: []byte(`7`)}},
		},
		{
			In:  &struct{ I fmt.Stringer }{I: nil},
			Out: &struct{ I fmt.Stringer }{I: nil},
		},
		{
			In:  &struct{ I int64 }{I: 7},
			Out: &struct{ I int64 }{I: 7},
		},
		{
			In: &struct {
				E runtime.RawExtension
				I int64
			}{E: runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}, I: 7},
			Out: &struct {
				E runtime.RawExtension
				I int64
			}{E: runtime.RawExtension{Raw: []byte(`7`)}, I: 7},
		},
		{
			// 嵌入字段（匿名字段）同样会被遍历。
			In: &struct {
				runtime.RawExtension
			}{RawExtension: runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}},
			Out: &struct {
				runtime.RawExtension
			}{RawExtension: runtime.RawExtension{Raw: []byte(`7`)}},
		},
		{
			// map 的值不可寻址，因此整体保持原样。
			In:  &map[string]string{"hello": "world"},
			Out: &map[string]string{"hello": "world"},
		},
		{
			In:  &map[string]runtime.RawExtension{"hello": {Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}},
			Out: &map[string]runtime.RawExtension{"hello": {Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}}, // not addressable
		},
		{
			// 但 map 值是切片时，切片元素又可寻址了，可以被转码。
			In:  &map[string][]runtime.RawExtension{"hello": {{Raw: []byte{0xd9, 0xd9, 0xf7, 0x07}}}},
			Out: &map[string][]runtime.RawExtension{"hello": {{Raw: []byte(`7`)}}},
		},
		{
			// FieldsV1 内部以字符串保存 JSON，0xa0 是 CBOR 空 map，转成 "{}"。
			In:  metav1.NewFieldsV1(string([]byte{0xa0})),
			Out: metav1.NewFieldsV1(`{}`),
		},
		{
			In:  &metav1.FieldsV1{},
			Out: &metav1.FieldsV1{},
		},
		{
			In:  *metav1.NewFieldsV1(string([]byte{0xa0})),
			Out: *metav1.NewFieldsV1(string([]byte{0xa0})), // not addressable
		},
		{
			// 0xf6 是 CBOR 的 null，转成 JSON 的 "null"。
			In:  &[...]metav1.FieldsV1{*metav1.NewFieldsV1(string([]byte{0xa0})), *metav1.NewFieldsV1(string([]byte{0xf6}))},
			Out: &[...]metav1.FieldsV1{*metav1.NewFieldsV1(`{}`), *metav1.NewFieldsV1(`null`)},
		},
		{
			In:  &[0]metav1.FieldsV1{},
			Out: &[0]metav1.FieldsV1{},
		},
		{
			In:  &[]metav1.FieldsV1{*metav1.NewFieldsV1(string([]byte{0xa0})), *metav1.NewFieldsV1(string([]byte{0xf6}))},
			Out: &[]metav1.FieldsV1{*metav1.NewFieldsV1(`{}`), *metav1.NewFieldsV1(`null`)},
		},
		{
			In:  &[]metav1.FieldsV1{},
			Out: &[]metav1.FieldsV1{},
		},
		{
			In:  (*metav1.FieldsV1)(nil),
			Out: (*metav1.FieldsV1)(nil),
		},
		{
			In:  &struct{ I fmt.Stringer }{I: metav1.NewFieldsV1(string([]byte{0xa0}))},
			Out: &struct{ I fmt.Stringer }{I: metav1.NewFieldsV1(`{}`)},
		},
		{
			In: &struct {
				E metav1.FieldsV1
				I int64
			}{E: *metav1.NewFieldsV1(string([]byte{0xa0})), I: 7},
			Out: &struct {
				E metav1.FieldsV1
				I int64
			}{E: *metav1.NewFieldsV1(`{}`), I: 7},
		},
		{
			In: &struct {
				metav1.FieldsV1
			}{FieldsV1: *metav1.NewFieldsV1(string([]byte{0xa0}))},
			Out: &struct {
				metav1.FieldsV1
			}{FieldsV1: *metav1.NewFieldsV1(`{}`)},
		},
		{
			In:  &map[string]metav1.FieldsV1{"hello": *metav1.NewFieldsV1(string([]byte{0xa0}))},
			Out: &map[string]metav1.FieldsV1{"hello": *metav1.NewFieldsV1(string([]byte{0xa0}))}, // not addressable
		},
		{
			In:  &map[string][]metav1.FieldsV1{"hello": {*metav1.NewFieldsV1(string([]byte{0xa0}))}},
			Out: &map[string][]metav1.FieldsV1{"hello": {*metav1.NewFieldsV1(`{}`)}},
		},
	} {
		t.Run(fmt.Sprintf("%#v", tc.In), func(t *testing.T) {
			// 转码是原地修改，直接对 tc.In 断言最终状态。
			if err := transcodeRawTypes(tc.In); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(tc.Out, tc.In); diff != "" {
				t.Errorf("unexpected diff:\n%s", diff)
			}
		})
	}
}
