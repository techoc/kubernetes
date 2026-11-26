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

package test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

// testObject 是最小化的 runtime.Object：只维护 GVK，用于隔离序列化器与具体类型。
type testObject struct {
	gvk schema.GroupVersionKind
}

// GetObjectKind 返回自身，满足 runtime.Object 的 ObjectKind 接口。
func (d *testObject) GetObjectKind() schema.ObjectKind { return d }

// SetGroupVersionKind 设置 GVK，使编码测试能控制写入 wire 的 apiVersion/kind。
func (d *testObject) SetGroupVersionKind(gvk schema.GroupVersionKind) { d.gvk = gvk }

// GroupVersionKind 返回当前 GVK。
func (d *testObject) GroupVersionKind() schema.GroupVersionKind { return d.gvk }

// DeepCopyObject 故意 panic：这些替身不参与需要深拷贝的路径。
func (d *testObject) DeepCopyObject() runtime.Object {
	panic("testObject does not support DeepCopy")
}

// testMarshalable 通过 Marshal() 返回预置数据，模拟走“慢路径”的 protobuf 对象。
type testMarshalable struct {
	testObject
	data []byte
	err  error
}

// Marshal 直接返回预置的字节与错误，便于验证序列化器的错误传播。
func (d *testMarshalable) Marshal() ([]byte, error) {
	return d.data, d.err
}

// DeepCopyObject 故意 panic，理由同 testObject。
func (d *testMarshalable) DeepCopyObject() runtime.Object {
	panic("testMarshalable does not support DeepCopy")
}

// testBufferedMarshalable 实现 MarshalTo/Size，模拟 protobuf “快路径”；
// Marshal 固定报错，确保序列化器确实走了缓冲区写入路径。
type testBufferedMarshalable struct {
	testObject
	data []byte
	err  error
}

// Marshal 始终返回错误，用来证明编码器优先使用 MarshalTo 而非 Marshal。
func (d *testBufferedMarshalable) Marshal() ([]byte, error) {
	return nil, fmt.Errorf("not invokable")
}

// MarshalTo 把预置数据复制到目标缓冲区，返回写入长度。
func (d *testBufferedMarshalable) MarshalTo(data []byte) (int, error) {
	copy(data, d.data)
	return len(d.data), d.err
}

// Size 返回序列化后的字节数，供编码器预分配缓冲区。
func (d *testBufferedMarshalable) Size() int {
	return len(d.data)
}

// DeepCopyObject 故意 panic，理由同 testObject。
func (d *testBufferedMarshalable) DeepCopyObject() runtime.Object {
	panic("testBufferedMarshalable does not support DeepCopy")
}

// TestRecognize 验证 protobuf 识别依据是 4 字节前缀 "k8s\0"：
// 长度不足、前缀不符或第 4 字节非 0 的数据都不能被识别。
func TestRecognize(t *testing.T) {
	s := protobuf.NewSerializer(nil, nil)
	ignores := [][]byte{
		nil,
		{},
		[]byte("k8s"),
		{0x6b, 0x38, 0x73, 0x01},
	}
	for i, data := range ignores {
		if ok, _, err := s.RecognizesData(data); err != nil || ok {
			t.Errorf("%d: should not recognize data: %v", i, err)
		}
	}
	recognizes := [][]byte{
		{0x6b, 0x38, 0x73, 0x00},
		{0x6b, 0x38, 0x73, 0x00, 0x01},
	}
	for i, data := range recognizes {
		if ok, _, err := s.RecognizesData(data); err != nil || !ok {
			t.Errorf("%d: should recognize data: %v", i, err)
		}
	}
}

// TestEncode 验证 protobuf 编码的完整 wire 格式（前缀 + Unknown 信封 + 原始数据），
// 并对比 MarshalTo“快路径”与 Marshal“慢路径”的输出必须一致；不可 marshal 的对象
// 与底层错误需原样上抛。
func TestEncode(t *testing.T) {
	obj1 := &testMarshalable{testObject: testObject{}, data: []byte{}}
	wire1 := []byte{
		0x6b, 0x38, 0x73, 0x00, // prefix
		0x0a, 0x04,
		0x0a, 0x00, // apiversion
		0x12, 0x00, // kind
		0x12, 0x00, // data
		0x1a, 0x00, // content-type
		0x22, 0x00, // content-encoding
	}
	obj2 := &testMarshalable{
		testObject: testObject{gvk: schema.GroupVersionKind{Kind: "test", Group: "other", Version: "version"}},
		data:       []byte{0x01, 0x02, 0x03},
	}
	wire2 := []byte{
		0x6b, 0x38, 0x73, 0x00, // prefix
		0x0a, 0x15,
		0x0a, 0x0d, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // apiversion
		0x12, 0x04, 0x74, 0x65, 0x73, 0x74, // kind
		0x12, 0x03, 0x01, 0x02, 0x03, // data
		0x1a, 0x00, // content-type
		0x22, 0x00, // content-encoding
	}

	err1 := fmt.Errorf("a test error")

	testCases := []struct {
		obj   runtime.Object
		data  []byte
		errFn func(error) bool
	}{
		{
			obj:   &testObject{},
			errFn: protobuf.IsNotMarshalable,
		},
		{
			obj:  obj1,
			data: wire1,
		},
		{
			obj:   &testMarshalable{testObject: obj1.testObject, err: err1},
			errFn: func(err error) bool { return err == err1 },
		},
		{
			// 若此测试失败，说明 "快路径" 与 "慢路径" 的序列化结果不一致
			obj:  &testBufferedMarshalable{testObject: obj1.testObject, data: obj1.data},
			data: wire1,
		},
		{
			obj:  obj2,
			data: wire2,
		},
		{
			// 若此测试失败，说明 "快路径" 与 "慢路径" 的序列化结果不一致
			obj:  &testBufferedMarshalable{testObject: obj2.testObject, data: obj2.data},
			data: wire2,
		},
		{
			obj:   &testBufferedMarshalable{testObject: obj1.testObject, err: err1},
			errFn: func(err error) bool { return err == err1 },
		},
	}

	for i, test := range testCases {
		s := protobuf.NewSerializer(nil, nil)
		data, err := runtime.Encode(s, test.obj)

		switch {
		case err == nil && test.errFn != nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil && test.errFn == nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil:
			if !test.errFn(err) {
				t.Errorf("%d: failed: %v", i, err)
			}
			if data != nil {
				t.Errorf("%d: should not have returned nil data", i)
			}
			continue
		}

		if test.data != nil && !bytes.Equal(test.data, data) {
			t.Errorf("%d: unexpected data:\n%s", i, hex.Dump(data))
			continue
		}

		if ok, _, err := s.RecognizesData(data); !ok || err != nil {
			t.Errorf("%d: did not recognize data generated by call: %v", i, err)
		}
	}
}

// TestProtobufDecode 验证解码到 runtime.Unknown：空数据、非 protobuf 前缀都会报错；
// 带前缀的数据会把原始字节与 ContentType 填入 Unknown。
func TestProtobufDecode(t *testing.T) {
	wire1 := []byte{
		0x6b, 0x38, 0x73, 0x00, // prefix
		0x0a, 0x04,
		0x0a, 0x00, // apiversion
		0x12, 0x00, // kind
		0x12, 0x00, // data
		0x1a, 0x00, // content-type
		0x22, 0x00, // content-encoding
	}
	wire2 := []byte{
		0x6b, 0x38, 0x73, 0x00, // prefix
		0x0a, 0x15,
		0x0a, 0x0d, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x2f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // apiversion
		0x12, 0x04, 0x74, 0x65, 0x73, 0x74, // kind
		0x12, 0x07, 0x6b, 0x38, 0x73, 0x00, 0x01, 0x02, 0x03, // data
		0x1a, 0x00, // content-type
		0x22, 0x00, // content-encoding
	}

	testCases := []struct {
		obj   runtime.Object
		data  []byte
		errFn func(error) bool
	}{
		{
			obj:   &runtime.Unknown{},
			errFn: func(err error) bool { return err.Error() == "empty data" },
		},
		{
			data:  []byte{0x6b},
			errFn: func(err error) bool { return strings.Contains(err.Error(), "does not appear to be a protobuf message") },
		},
		{
			obj: &runtime.Unknown{
				Raw: []byte{},
			},
			data: wire1,
		},
		{
			obj: &runtime.Unknown{
				TypeMeta: runtime.TypeMeta{
					APIVersion: "other/version",
					Kind:       "test",
				},
				// 前缀与内容匹配，因此设置了 content type
				ContentType: runtime.ContentTypeProtobuf,
				Raw:         []byte{0x6b, 0x38, 0x73, 0x00, 0x01, 0x02, 0x03},
			},
			data: wire2,
		},
	}

	for i, test := range testCases {
		s := protobuf.NewSerializer(nil, nil)
		unk := &runtime.Unknown{}
		err := runtime.DecodeInto(s, test.data, unk)

		switch {
		case err == nil && test.errFn != nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil && test.errFn == nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil:
			if !test.errFn(err) {
				t.Errorf("%d: failed: %v", i, err)
			}
			continue
		}

		if !reflect.DeepEqual(unk, test.obj) {
			t.Errorf("%d: unexpected object:\n%#v", i, unk)
			continue
		}
	}
}

// TestDecodeObjects 验证完整对象解码：先校验 NestedMarshalTo 生成的 wire 与 Marshal 一致，
// 再加上前缀后用带 scheme 的序列化器解码，结果应与原 Carp 对象语义相等。
func TestDecodeObjects(t *testing.T) {
	obj1 := &v1.Carp{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cool",
		},
		Spec: v1.CarpSpec{
			Hostname: "coolhost",
		},
	}
	obj1wire, err := obj1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	wire1, err := (&runtime.Unknown{
		TypeMeta: runtime.TypeMeta{Kind: "Carp", APIVersion: "v1"},
		Raw:      obj1wire,
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}

	unk2 := &runtime.Unknown{
		TypeMeta: runtime.TypeMeta{Kind: "Carp", APIVersion: "v1"},
	}
	wire2 := make([]byte, len(wire1)*2)
	n, err := unk2.NestedMarshalTo(wire2, obj1, uint64(obj1.Size()))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(wire1) || !bytes.Equal(wire1, wire2[:n]) {
		t.Fatalf("unexpected wire:\n%s", hex.Dump(wire2[:n]))
	}

	wire1 = append([]byte{0x6b, 0x38, 0x73, 0x00}, wire1...)

	// 解码结果需要带上 GVK，因为 protobuf 解码不会像 JSON 那样从 body 内推断类型。
	obj1WithKind := obj1.DeepCopyObject()
	obj1WithKind.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Carp"})
	testCases := []struct {
		obj   runtime.Object
		data  []byte
		errFn func(error) bool
	}{
		{
			obj:  obj1WithKind,
			data: wire1,
		},
	}
	scheme := runtime.NewScheme()
	for i, test := range testCases {
		scheme.AddKnownTypes(schema.GroupVersion{Version: "v1"}, &v1.Carp{})
		require.NoError(t, v1.AddToScheme(scheme))
		s := protobuf.NewSerializer(scheme, scheme)
		obj, err := runtime.Decode(s, test.data)

		switch {
		case err == nil && test.errFn != nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil && test.errFn == nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil:
			if !test.errFn(err) {
				t.Errorf("%d: failed: %v", i, err)
			}
			if obj != nil {
				t.Errorf("%d: should not have returned an object", i)
			}
			continue
		}

		if !apiequality.Semantic.DeepEqual(obj, test.obj) {
			t.Errorf("%d: unexpected object:\n%s", i, cmp.Diff(test.obj, obj))
			continue
		}
	}
}
