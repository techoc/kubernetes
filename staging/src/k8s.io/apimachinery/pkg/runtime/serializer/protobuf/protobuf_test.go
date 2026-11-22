/*
Copyright 2019 The Kubernetes Authors.

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

package protobuf

import (
	"bytes"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"
)

// TestCacheableObject 用 protobuf 与 raw 两种 serializer 分别跑通用 CacheableObjectTest，
// 确认它们的缓存标识（Identifier）与序列化结果满足缓存的契约。
func TestCacheableObject(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "group", Version: "version", Kind: "MockCacheableObject"}
	creater := &mockCreater{obj: &runtimetesting.MockCacheableObject{}}
	typer := &mockTyper{gvk: &gvk}

	encoders := []runtime.Encoder{
		NewSerializer(creater, typer),
		NewRawSerializer(creater, typer),
	}

	for _, encoder := range encoders {
		runtimetesting.CacheableObjectTest(t, encoder)
	}
}

// mockCreater 记录 serializer 请求创建的类型，并返回预设对象。
type mockCreater struct {
	apiVersion string
	kind       string
	err        error
	obj        runtime.Object
}

// New 记录请求的 GVK 并返回预设对象（或预设错误）。
func (c *mockCreater) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	c.apiVersion, c.kind = kind.GroupVersion().String(), kind.Kind
	return c.obj, c.err
}

// mockTyper 返回固定的 GVK，用于让 serializer 无需真实 scheme 即可确定类型。
type mockTyper struct {
	gvk *schema.GroupVersionKind
	err error
}

// ObjectKinds 返回预设的单一 GVK；gvk 为 nil 时返回预设错误。
func (t *mockTyper) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	if t.gvk == nil {
		return nil, false, t.err
	}
	return []schema.GroupVersionKind{*t.gvk}, false, t.err
}

// Recognizes 恒返回 false：该 mock 仅用于输出，不参与类型识别。
func (t *mockTyper) Recognizes(_ schema.GroupVersionKind) bool {
	return false
}

// TestSerializerEncodeWithAllocator 验证 EncodeWithAllocator 与 Encode 的输出完全一致，
// 并且只调用一次 Allocate——这是为减少序列化分配而引入新路径时必须守住的兼容性约束。
func TestSerializerEncodeWithAllocator(t *testing.T) {
	testCases := []struct {
		name string
		obj  runtime.Object
	}{
		{
			name: "encode a bufferedMarshaller obj",
			obj: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{APIVersion: "group/version", Kind: "Carp"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "name",
					Namespace: "namespace",
				},
				Spec: testapigroupv1.CarpSpec{
					Subdomain: "carp.k8s.io",
				},
			},
		},

		{
			name: "encode a runtime.Unknown obj",
			obj:  &runtime.Unknown{TypeMeta: runtime.TypeMeta{APIVersion: "group/version", Kind: "Unknown"}, Raw: []byte("hello world")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target := NewSerializer(nil, nil)

			writer := &bytes.Buffer{}
			if err := target.Encode(tc.obj, writer); err != nil {
				t.Fatal(err)
			}

			writer2 := &bytes.Buffer{}
			alloc := &testAllocator{}
			if err := target.EncodeWithAllocator(tc.obj, writer2, alloc); err != nil {
				t.Fatal(err)
			}
			// 只允许分配一次缓冲区；调用方传入 allocator 的目的就是避免重复分配。
			if alloc.allocateCount != 1 {
				t.Fatalf("expected the Allocate method to be called exactly 1 but it was executed: %v times ", alloc.allocateCount)
			}

			// 为确保新方法对旧方法兼容，序列化数据必须完全一致。
			// 这里不测解码，因为“往返”已在其它地方对所有已知类型覆盖。
			if !reflect.DeepEqual(writer.Bytes(), writer2.Bytes()) {
				t.Fatal("data mismatch, data serialized with the Encode method is different than serialized with the EncodeWithAllocator method")
			}
		})
	}
}

// TestRawSerializerEncodeWithAllocator 验证 RawSerializer 走 EncodeWithAllocator 时
// 输出与 Encode 相同且只分配一次；raw serializer 不封装 envelope，是这一路径的关键差异点。
func TestRawSerializerEncodeWithAllocator(t *testing.T) {
	testCases := []struct {
		name string
		obj  runtime.Object
	}{
		{
			name: "encode a bufferedReverseMarshaller obj",
			obj: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{APIVersion: "group/version", Kind: "Carp"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "name",
					Namespace: "namespace",
				},
				Spec: testapigroupv1.CarpSpec{
					Subdomain: "carp.k8s.io",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &bytes.Buffer{}
			target := NewRawSerializer(nil, nil)

			if err := target.Encode(tc.obj, writer); err != nil {
				t.Fatal(err)
			}

			writer2 := &bytes.Buffer{}
			alloc := &testAllocator{}
			if err := target.EncodeWithAllocator(tc.obj, writer2, alloc); err != nil {
				t.Fatal(err)
			}
			// 只允许分配一次缓冲区，与 EncodeWithAllocator 的约定一致。
			if alloc.allocateCount != 1 {
				t.Fatalf("expected the Allocate method to be called exactly 1 but it was executed: %v times ", alloc.allocateCount)
			}

			// 为确保新方法对旧方法兼容，序列化数据必须完全一致。
			// 这里不测解码，因为“往返”已在其它地方对所有已知类型覆盖。
			if !reflect.DeepEqual(writer.Bytes(), writer2.Bytes()) {
				t.Fatal("data mismatch, data serialized with the Encode method is different than serialized with the EncodeWithAllocator method")
			}
		})
	}
}

// testAllocator 记录 Allocate 被调用的次数，供测试断言分配路径只走一次。
type testAllocator struct {
	buf           []byte
	allocateCount int
}

// Allocate 分配 n 字节并累加计数。
func (ta *testAllocator) Allocate(n uint64) []byte {
	ta.buf = make([]byte, n)
	ta.allocateCount++
	return ta.buf
}
