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

// 本包测试聚焦 runtime.Serializer 实现的正确性。Go 值与 CBOR 字节之间编解码的具体行为
// 由 ./internal/modes 包测试——该包同时被本 Serializer 与 ./direct 包中的
// Marshal/Unmarshal 函数使用。

package cbor

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"

	"github.com/google/go-cmp/cmp"
)

// TestRecognizesData 验证 CBOR 识别只依赖 self-described tag 前缀 0xd9d9f7：
// 前缀不完整或不匹配时返回 false，且 unknown 始终为 false（不猜测其它格式）。
func TestRecognizesData(t *testing.T) {
	for _, tc := range []struct {
		in         []byte
		recognizes bool
	}{
		{
			in:         nil,
			recognizes: false,
		},
		{
			in:         []byte{},
			recognizes: false,
		},
		{
			in:         []byte{0xd9},
			recognizes: false,
		},
		{
			in:         []byte{0xd9, 0xd9},
			recognizes: false,
		},
		{
			in:         []byte{0xd9, 0xd9, 0xf7},
			recognizes: true,
		},
		{
			in:         []byte{0xff, 0xff, 0xff},
			recognizes: false,
		},
		{
			in:         []byte{0xd9, 0xd9, 0xf7, 0x01, 0x02, 0x03},
			recognizes: true,
		},
		{
			in:         []byte{0xff, 0xff, 0xff, 0x01, 0x02, 0x03},
			recognizes: false,
		},
	} {
		t.Run(hex.EncodeToString(tc.in), func(t *testing.T) {
			s := NewSerializer(nil, nil)
			recognizes, unknown, err := s.RecognizesData(tc.in)
			if recognizes != tc.recognizes {
				t.Errorf("expected recognized to be %t, got %t", tc.recognizes, recognizes)
			}
			if unknown {
				t.Error("expected unknown to be false, got true")
			}
			if err != nil {
				t.Errorf("expected nil error, got: %v", err)
			}
		})
	}
}

// stubWriter 返回预设的写入结果，用于模拟底层 writer 的短写/错误。
type stubWriter struct {
	n   int
	err error
}

// Write 忽略输入，返回预设的字节数与错误。
func (w stubWriter) Write([]byte) (int, error) {
	return w.n, w.err
}

// anyObject 包装任意具体值，使其可以被编码或解码（测试 CBOR 编组器本身）。
type anyObject struct {
	Value interface{}
}

// GetObjectKind 返回空 ObjectKind，满足 runtime.Object 接口。
func (p anyObject) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (anyObject) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// MarshalCBOR 用确定性编码模式编组内部值。
func (p anyObject) MarshalCBOR() ([]byte, error) {
	return modes.Encode.Marshal(p.Value)
}

// UnmarshalCBOR 用解码模式把字节解码进内部值。
func (p *anyObject) UnmarshalCBOR(in []byte) error {
	return modes.Decode.Unmarshal(in, &p.Value)
}

// structWithRawFields 覆盖四种“原始字节”字段形态（值/指针 × FieldsV1/RawExtension），
// 用于验证 transcode 选项是否能把它们转成 JSON。
type structWithRawFields struct {
	FieldsV1            metav1.FieldsV1       `json:"f"`
	FieldsV1Pointer     *metav1.FieldsV1      `json:"fp"`
	RawExtension        runtime.RawExtension  `json:"r"`
	RawExtensionPointer *runtime.RawExtension `json:"rp"`
}

// GetObjectKind 返回空 ObjectKind。
func (structWithRawFields) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (structWithRawFields) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// structWithEmbeddedMetas 模拟真实 API 对象：内嵌 TypeMeta 与 ObjectMeta。
type structWithEmbeddedMetas struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (structWithEmbeddedMetas) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// TestEncode 验证 Encode 的输出前缀、流式列表编码、TextMarshaler 处理，
// 以及底层 writer 出错时的错误传播。每个用例自带 writer 与错误断言。
func TestEncode(t *testing.T) {
	for _, tc := range []struct {
		name           string
		in             runtime.Object
		assertOnWriter func() (io.Writer, func(*testing.T))
		assertOnError  func(*testing.T, error)
	}{
		{
			name: "io error writing self described cbor tag",
			assertOnWriter: func() (io.Writer, func(*testing.T)) {
				return stubWriter{err: io.ErrShortWrite}, func(*testing.T) {}
			},
			assertOnError: func(t *testing.T, err error) {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Errorf("expected io.ErrShortWrite, got: %v", err)
				}
			},
		},
		{
			name: "output enclosed by self-described CBOR tag",
			in:   anyObject{},
			assertOnWriter: func() (io.Writer, func(*testing.T)) {
				var b bytes.Buffer
				return &b, func(t *testing.T) {
					if !bytes.HasPrefix(b.Bytes(), []byte{0xd9, 0xd9, 0xf7}) {
						t.Errorf("expected output to have prefix 0xd9d9f7: 0x%x", b.Bytes())
					}
				}
			},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name: "unstructuredlist",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"apiVersion": "v",
					"kind":       "kList",
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"foo": int64(1)}},
					{Object: map[string]interface{}{"foo": int64(2)}},
				},
			},
			assertOnWriter: func() (io.Writer, func(t *testing.T)) {
				var b bytes.Buffer
				return &b, func(t *testing.T) {
					// {'kind': 'kList', 'items': [{'foo': 1}, {'foo': 2}], 'apiVersion': 'v'}
					if diff := cmp.Diff(b.Bytes(), []byte("\xd9\xd9\xf7\xa3\x44kind\x45kList\x45items\x82\xa1\x43foo\x01\xa1\x43foo\x02\x4aapiVersion\x41v")); diff != "" {
						t.Errorf("unexpected diff:\n%s", diff)
					}
				}
			},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name: "text marshaler",
			in:   &textMarshalerObject{},
			assertOnWriter: func() (io.Writer, func(*testing.T)) {
				var b bytes.Buffer
				return &b, func(t *testing.T) {
					if diff := cmp.Diff(b.Bytes(), []byte{0xd9, 0xd9, 0xf7, 0x64, 't', 'e', 's', 't'}); diff != "" {
						t.Errorf("unexpected diff:\n%s", diff)
					}
				}
			},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name: "text marshaler within unstructured content",
			in: &unstructured.Unstructured{
				Object: map[string]interface{}{"": textMarshalerObject{}},
			},
			assertOnWriter: func() (io.Writer, func(*testing.T)) {
				var b bytes.Buffer
				return &b, func(t *testing.T) {
					if diff := cmp.Diff(b.Bytes(), []byte{0xd9, 0xd9, 0xf7, 0xa1, 0x40, 0x64, 't', 'e', 's', 't'}); diff != "" {
						t.Errorf("unexpected diff:\n%s", diff)
					}
				}
			},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSerializer(nil, nil)
			w, assertOnWriter := tc.assertOnWriter()
			err := s.Encode(tc.in, w)
			tc.assertOnError(t, err)
			assertOnWriter(t)
		})
	}
}

// TestDecode 是解码路径的主测试：覆盖 GVK 推断、strict/transcode 选项、
// 预分配对象、不支持的 typer/creater，以及完整列表解码等场景。
func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		options       []Option
		data          []byte
		gvk           *schema.GroupVersionKind
		metaFactory   metaFactory
		typer         runtime.ObjectTyper
		creater       runtime.ObjectCreater
		into          runtime.Object
		expectedObj   runtime.Object
		expectedGVK   *schema.GroupVersionKind
		assertOnError func(*testing.T, error)
	}{
		{
			name:        "self-described cbor tag accepted",
			data:        []byte("\xd9\xd9\xf7\xa3\x4aapiVersion\x41v\x44kind\x41k\x48metadata\xa1\x44name\x43foo"), // 55799({'apiVersion': 'v', 'kind': 'k', 'metadata': {'name': 'foo'}})
			gvk:         &schema.GroupVersionKind{},
			metaFactory: &defaultMetaFactory{},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Version: "v", Kind: "k"}}},
			into:        &metav1.PartialObjectMetadata{},
			expectedObj: &metav1.PartialObjectMetadata{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v", Kind: "k"},
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "k"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "error determining gvk",
			metaFactory: stubMetaFactory{err: errors.New("test")},
			assertOnError: func(t *testing.T, err error) {
				if err == nil || err.Error() != "test" {
					t.Errorf("expected error \"test\", got: %v", err)
				}
			},
		},
		{
			name:        "typer does not recognize into",
			gvk:         &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       notRegisteredTyper{},
			into:        &anyObject{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsNotRegisteredError(err) {
					t.Errorf("expected NotRegisteredError, got: %v", err)
				}
			},
		},
		{
			name:        "gvk from type of into",
			data:        []byte{0xf6},
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &anyObject{},
			expectedObj: &anyObject{},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "raw types transcoded",
			data:        []byte{0xa4, 0x41, 'f', 0xa1, 0x41, 'a', 0x01, 0x42, 'f', 'p', 0xa1, 0x41, 'z', 0x02, 0x41, 'r', 0xa1, 0x41, 'b', 0x03, 0x42, 'r', 'p', 0xa1, 0x41, 'y', 0x04},
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &structWithRawFields{},
			expectedObj: &structWithRawFields{
				FieldsV1:            *metav1.NewFieldsV1(`{"a":1}`),
				FieldsV1Pointer:     metav1.NewFieldsV1(`{"z":2}`),
				RawExtension:        runtime.RawExtension{Raw: []byte(`{"b":3}`)},
				RawExtensionPointer: &runtime.RawExtension{Raw: []byte(`{"y":4}`)},
			},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "raw types not transcoded",
			options:     []Option{Transcode(false)},
			data:        []byte{0xa4, 0x41, 'f', 0xa1, 0x41, 'a', 0x01, 0x42, 'f', 'p', 0xa1, 0x41, 'z', 0x02, 0x41, 'r', 0xa1, 0x41, 'b', 0x03, 0x42, 'r', 'p', 0xa1, 0x41, 'y', 0x04},
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &structWithRawFields{},
			expectedObj: &structWithRawFields{
				FieldsV1:        *metav1.NewFieldsV1(string([]byte{0xa1, 0x41, 'a', 0x01})),
				FieldsV1Pointer: metav1.NewFieldsV1(string([]byte{0xa1, 0x41, 'z', 0x02})),
				// RawExtension 的 UnmarshalCBOR 会保证结果中存在 self-described CBOR tag，
				// 从而在区分 CBOR 与 JSON/Protobuf 时永不产生歧义。FieldsV1 不需要同样处理：
				// 首字节已足以区分合法的 JSON 编码与合法的 CBOR 编码。
				RawExtension:        runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0xa1, 0x41, 'b', 0x03}},
				RawExtensionPointer: &runtime.RawExtension{Raw: []byte{0xd9, 0xd9, 0xf7, 0xa1, 0x41, 'y', 0x04}},
			},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "object with embedded typemeta and objectmeta",
			data:        []byte("\xa2\x48metadata\xa1\x44name\x43foo\x44spec\xa0"), // {"metadata": {"name": "foo"}}
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &structWithEmbeddedMetas{},
			expectedObj: &structWithEmbeddedMetas{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "strict mode strict error",
			options:     []Option{Strict(true)},
			data:        []byte{0xa1, 0x61, 'z', 0x01}, // {'z': 1}
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &metav1.PartialObjectMetadata{},
			expectedObj: &metav1.PartialObjectMetadata{},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsStrictDecodingError(err) {
					t.Errorf("expected StrictDecodingError, got: %v", err)
				}
			},
		},
		{
			name:        "no strict mode no strict error",
			data:        []byte{0xa1, 0x61, 'z', 0x01}, // {'z': 1}
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{gvks: []schema.GroupVersionKind{{Group: "x", Version: "y", Kind: "z"}}},
			into:        &metav1.PartialObjectMetadata{},
			expectedObj: &metav1.PartialObjectMetadata{},
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "unknown error from typer on into",
			gvk:         &schema.GroupVersionKind{},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			typer:       stubTyper{err: errors.New("test")},
			into:        &anyObject{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{},
			assertOnError: func(t *testing.T, err error) {
				if err == nil || err.Error() != "test" {
					t.Errorf("expected error \"test\", got: %v", err)
				}
			},
		},
		{
			name:        "missing kind",
			gvk:         &schema.GroupVersionKind{Version: "v"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Version: "v"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingKind(err) {
					t.Errorf("expected MissingKind, got: %v", err)
				}
			},
		},
		{
			name:        "missing version",
			gvk:         &schema.GroupVersionKind{Kind: "k"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Kind: "k"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingVersion(err) {
					t.Errorf("expected MissingVersion, got: %v", err)
				}
			},
		},
		{
			name:        "creater error",
			gvk:         &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			creater:     stubCreater{err: errors.New("test")},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if err == nil || err.Error() != "test" {
					t.Errorf("expected error \"test\", got: %v", err)
				}
			},
		},
		{
			name:        "unmarshal error",
			data:        nil, // EOF
			gvk:         &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			creater:     stubCreater{obj: &anyObject{}},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if !errors.Is(err, io.EOF) {
					t.Errorf("expected EOF, got: %v", err)
				}
			},
		},
		{
			name:        "strict mode unmarshal error",
			options:     []Option{Strict(true)},
			data:        nil, // EOF
			gvk:         &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			creater:     stubCreater{obj: &anyObject{}},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if !errors.Is(err, io.EOF) {
					t.Errorf("expected EOF, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructured unmarshal error",
			data:        nil, // EOF
			gvk:         &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			metaFactory: stubMetaFactory{gvk: &schema.GroupVersionKind{}},
			into:        &unstructured.Unstructured{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Group: "x", Version: "y", Kind: "z"},
			assertOnError: func(t *testing.T, err error) {
				if !errors.Is(err, io.EOF) {
					t.Errorf("expected EOF, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructured missing kind",
			data:        []byte("\xa1\x6aapiVersion\x61v"),
			into:        &unstructured.Unstructured{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Version: "v"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingKind(err) {
					t.Errorf("expected MissingKind, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructured missing version",
			data:        []byte("\xa1\x64kind\x61k"),
			into:        &unstructured.Unstructured{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Kind: "k"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingVersion(err) {
					t.Errorf("expected MissingVersion, got: %v", err)
				}
			},
		},
		{
			name: "into unstructured",
			data: []byte("\xa2\x6aapiVersion\x61v\x64kind\x61k"),
			into: &unstructured.Unstructured{},
			expectedObj: &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v",
				"kind":       "k",
			}},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "k"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "using unstructured creater",
			data:        []byte("\xa2\x6aapiVersion\x61v\x64kind\x61k"),
			metaFactory: &defaultMetaFactory{},
			creater:     stubCreater{obj: &unstructured.Unstructured{}},
			expectedObj: &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v",
				"kind":       "k",
			}},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "k"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructuredlist missing kind",
			data:        []byte("\xa1\x6aapiVersion\x61v"),
			into:        &unstructured.UnstructuredList{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Version: "v"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingKind(err) {
					t.Errorf("expected MissingKind, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructuredlist missing version",
			data:        []byte("\xa1\x64kind\x65kList"),
			into:        &unstructured.UnstructuredList{},
			expectedObj: nil,
			expectedGVK: &schema.GroupVersionKind{Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingVersion(err) {
					t.Errorf("expected MissingVersion, got: %v", err)
				}
			},
		},
		{
			name: "into unstructuredlist empty",
			data: []byte("\xa2\x6aapiVersion\x61v\x64kind\x65kList"),
			into: &unstructured.UnstructuredList{},
			expectedObj: &unstructured.UnstructuredList{Object: map[string]interface{}{
				"apiVersion": "v",
				"kind":       "kList",
			}},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name: "into unstructuredlist nonempty",
			data: []byte("\xa3\x6aapiVersion\x61v\x64kind\x65kList\x65items\x82\xa1\x63foo\x01\xa1\x63foo\x02"), // {"apiVersion": "v", "kind": "kList", "items": [{"foo": 1}, {"foo": 2}]}
			into: &unstructured.UnstructuredList{},
			expectedObj: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"apiVersion": "v",
					"kind":       "kList",
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"apiVersion": "v", "kind": "k", "foo": int64(1)}},
					{Object: map[string]interface{}{"apiVersion": "v", "kind": "k", "foo": int64(2)}},
				},
			},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name: "into unstructuredlist item gvk present",
			data: []byte("\xa3\x6aapiVersion\x61v\x64kind\x65kList\x65items\x81\xa2\x6aapiVersion\x62vv\x64kind\x62kk"), // {"apiVersion": "v", "kind": "kList", "items": [{"apiVersion": "vv", "kind": "kk"}]}
			into: &unstructured.UnstructuredList{},
			expectedObj: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"apiVersion": "v",
					"kind":       "kList",
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"apiVersion": "vv", "kind": "kk"}},
				},
			},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructuredlist item missing kind",
			data:        []byte("\xa3\x6aapiVersion\x61v\x64kind\x65kList\x65items\x81\xa1\x6aapiVersion\x62vv"), // {"apiVersion": "v", "kind": "kList", "items": [{"apiVersion": "vv"}]}
			metaFactory: &defaultMetaFactory{},
			into:        &unstructured.UnstructuredList{},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingKind(err) {
					t.Errorf("expected MissingVersion, got: %v", err)
				}
			},
		},
		{
			name:        "into unstructuredlist item missing version",
			data:        []byte("\xa3\x6aapiVersion\x61v\x64kind\x65kList\x65items\x81\xa1\x64kind\x62kk"), // {"apiVersion": "v", "kind": "kList", "items": [{"kind": "kk"}]}
			metaFactory: &defaultMetaFactory{},
			into:        &unstructured.UnstructuredList{},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if !runtime.IsMissingVersion(err) {
					t.Errorf("expected MissingVersion, got: %v", err)
				}
			},
		},
		{
			name:        "using unstructuredlist creater",
			data:        []byte("\xa2\x6aapiVersion\x61v\x64kind\x65kList"),
			metaFactory: &defaultMetaFactory{},
			creater:     stubCreater{obj: &unstructured.UnstructuredList{}},
			expectedObj: &unstructured.UnstructuredList{Object: map[string]interface{}{
				"apiVersion": "v",
				"kind":       "kList",
			}},
			expectedGVK: &schema.GroupVersionKind{Version: "v", Kind: "kList"},
			assertOnError: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSerializer(tc.metaFactory, tc.creater, tc.typer, tc.options...)

			actualObj, actualGVK, err := s.Decode(tc.data, tc.gvk, tc.into)
			tc.assertOnError(t, err)

			if !reflect.DeepEqual(tc.expectedObj, actualObj) {
				t.Error(cmp.Diff(tc.expectedObj, actualObj))
			}

			if diff := cmp.Diff(tc.expectedGVK, actualGVK); diff != "" {
				t.Error(diff)
			}
		})
	}
}

// textMarshalerObject 实现 encoding.TextMarshaler，用于验证 CBOR 编码是否遵循文本编组接口。
type textMarshalerObject struct{}

// GetObjectKind 返回空 ObjectKind。
func (p textMarshalerObject) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (textMarshalerObject) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// MarshalText 返回固定文本，供编码测试断言。
func (textMarshalerObject) MarshalText() ([]byte, error) {
	return []byte("test"), nil
}

// TestMetaFactoryInterpret 验证默认 metaFactory 的 GVK 推断：空输入报错，
// 含 apiVersion/kind 的 CBOR map 能被正确拆解。
func TestMetaFactoryInterpret(t *testing.T) {
	mf := &defaultMetaFactory{}
	_, err := mf.Interpret(nil)
	if err == nil {
		t.Error("expected non-nil error")
	}
	gvk, err := mf.Interpret([]byte("\xa2\x6aapiVersion\x63a/b\x64kind\x61c"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff(&schema.GroupVersionKind{Group: "a", Version: "b", Kind: "c"}, gvk); diff != "" {
		t.Error(diff)
	}
}

// stubTyper 返回预设的 GVK 列表与错误，用于隔离 Decode 的 typer 依赖。
type stubTyper struct {
	gvks        []schema.GroupVersionKind
	unversioned bool
	err         error
}

// ObjectKinds 直接返回预设结果。
func (t stubTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	return t.gvks, t.unversioned, t.err
}

// Recognizes 恒返回 false。
func (stubTyper) Recognizes(schema.GroupVersionKind) bool {
	return false
}

// stubCreater 返回预设对象或错误。
type stubCreater struct {
	obj runtime.Object
	err error
}

// New 直接返回预设结果。
func (c stubCreater) New(gvk schema.GroupVersionKind) (runtime.Object, error) {
	return c.obj, c.err
}

// notRegisteredTyper 模拟“对象类型未注册”的场景。
type notRegisteredTyper struct{}

// ObjectKinds 始终返回 NotRegisteredError。
func (notRegisteredTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	return nil, false, runtime.NewNotRegisteredErrForType("test", reflect.TypeOf(obj))
}

// Recognizes 恒返回 false。
func (notRegisteredTyper) Recognizes(schema.GroupVersionKind) bool {
	return false
}

// stubMetaFactory 返回预设 GVK 或错误。
type stubMetaFactory struct {
	gvk *schema.GroupVersionKind
	err error
}

// Interpret 直接返回预设结果。
func (mf stubMetaFactory) Interpret([]byte) (*schema.GroupVersionKind, error) {
	return mf.gvk, mf.err
}

// oneMapField 只包含一个 map 字段，用于测试非确定性编码时 map 键顺序被打乱。
type oneMapField struct {
	metav1.TypeMeta `json:""`
	Map             map[string]interface{} `json:"map"`
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (o oneMapField) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// GetObjectKind 不实现，仅在测试中用作占位。
func (o oneMapField) GetObjectKind() schema.ObjectKind {
	panic("unimplemented")
}

// eightStringFields 含 8 个字符串字段，用于测试非确定性编码时结构体字段顺序被打乱。
type eightStringFields struct {
	metav1.TypeMeta `json:""`
	A               string `json:"1"`
	B               string `json:"2"`
	C               string `json:"3"`
	D               string `json:"4"`
	E               string `json:"5"`
	F               string `json:"6"`
	G               string `json:"7"`
	H               string `json:"8"`
}

// DeepCopyObject 不实现，仅在测试中用作占位。
func (o eightStringFields) DeepCopyObject() runtime.Object {
	panic("unimplemented")
}

// GetObjectKind 不实现，仅在测试中用作占位。
func (o eightStringFields) GetObjectKind() schema.ObjectKind {
	panic("unimplemented")
}

// TestEncodeNondeterministic 验证反复用 EncodeNondeterministic 编码多字段结构体/map 时，
// 输出字节不会每次都完全相同。该模式下 CBOR map 的元素顺序会被有意打乱，
// 以防应用无意中依赖编码的确定性。注意各排列出现的概率不一定相等。
func TestEncodeNondeterministic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input runtime.Object
	}{
		{
			name: "map",
			input: func() runtime.Object {
				m := map[string]interface{}{}
				for i := 1; i <= 8; i++ {
					m[strconv.Itoa(i)] = strconv.Itoa(i)

				}
				return oneMapField{Map: m}
			}(),
		},
		{
			name: "struct",
			input: eightStringFields{
				TypeMeta: metav1.TypeMeta{},
				A:        "1",
				B:        "2",
				C:        "3",
				D:        "4",
				E:        "5",
				F:        "6",
				G:        "7",
				H:        "8",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			e := NewSerializer(nil, nil)

			if err := e.EncodeNondeterministic(tc.input, &b); err != nil {
				t.Fatal(err)
			}
			first := b.String()

			const Trials = 128
			for trial := 0; trial < Trials; trial++ {
				b.Reset()
				if err := e.EncodeNondeterministic(tc.input, &b); err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal([]byte(first), b.Bytes()) {
					return
				}
			}
			t.Fatalf("nondeterministic encode produced the same bytes on %d consecutive calls: %s", Trials, first)
		})
	}

}
