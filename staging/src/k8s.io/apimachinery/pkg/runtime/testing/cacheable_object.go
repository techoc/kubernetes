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

package testing

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// noncacheableTestObject 实现 json.Marshaler 与 proto.Marshaler 接口，
// 用于测试目的。
// +k8s:deepcopy-gen=false
type noncacheableTestObject struct {
	gvk schema.GroupVersionKind
}

// MarshalJSON 实现 json.Marshaler 接口。
func (*noncacheableTestObject) MarshalJSON() ([]byte, error) {
	return []byte("\"json-result\""), nil
}

// Marshal 实现 proto.Marshaler 接口。
func (*noncacheableTestObject) Marshal() ([]byte, error) {
	return []byte("\"proto-result\""), nil
}

// DeepCopyObject 实现 runtime.Object 接口（测试桩，调用即 panic）。
func (*noncacheableTestObject) DeepCopyObject() runtime.Object {
	panic("DeepCopy unimplemented for noncacheableTestObject")
}

// GetObjectKind 实现 runtime.Object 接口。
func (o *noncacheableTestObject) GetObjectKind() schema.ObjectKind {
	return o
}

// GroupVersionKind 实现 schema.ObjectKind 接口。
func (o *noncacheableTestObject) GroupVersionKind() schema.GroupVersionKind {
	return o.gvk
}

// SetGroupVersionKind 实现 schema.ObjectKind 接口。
func (o *noncacheableTestObject) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	o.gvk = gvk
}

var _ runtime.CacheableObject = &MockCacheableObject{}

// MockCacheableObject 用于测试 CacheableObject 接口。
// 它记录编码器传入的 Identifier，并可按配置执行真实编码或返回预置结果。
// +k8s:deepcopy-gen=false
type MockCacheableObject struct {
	gvk schema.GroupVersionKind

	t *testing.T

	// runEncode 为 true 时调用真实的 encode 回调，否则直接写出 expectedResult。
	runEncode bool
	// returnSelf 控制 GetObject 返回自身还是 noncacheableTestObject。
	returnSelf     bool
	expectedResult string
	expectedError  error

	// intercepted 记录每次 CacheEncode 收到的 Identifier，供断言使用。
	intercepted []runtime.Identifier
}

// DeepCopyObject 实现 runtime.Object 接口（测试桩，调用即 panic）。
func (m *MockCacheableObject) DeepCopyObject() runtime.Object {
	panic("DeepCopy unimplemented for MockCacheableObject")
}

// GetObjectKind 实现 runtime.Object 接口。
func (m *MockCacheableObject) GetObjectKind() schema.ObjectKind {
	return m
}

// GroupVersionKind 实现 schema.ObjectKind 接口。
func (m *MockCacheableObject) GroupVersionKind() schema.GroupVersionKind {
	return m.gvk
}

// SetGroupVersionKind 实现 schema.ObjectKind 接口。
func (m *MockCacheableObject) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	m.gvk = gvk
}

// Marshal 实现 proto.Marshaler 接口。
// 提供该实现是为了避免 protobuf 序列化器报错。
func (*MockCacheableObject) Marshal() ([]byte, error) {
	return []byte("\"proto-result\""), nil
}

// CacheEncode 实现 runtime.CacheableObject 接口。
// 先记录 identifier，再根据 runEncode 决定委托真实编码还是写出预置结果。
func (m *MockCacheableObject) CacheEncode(id runtime.Identifier, encode func(runtime.Object, io.Writer) error, w io.Writer) error {
	m.intercepted = append(m.intercepted, id)
	if m.runEncode {
		return encode(m.GetObject(), w)
	}
	if _, err := w.Write([]byte(m.expectedResult)); err != nil {
		m.t.Errorf("couldn't write to io.Writer: %v", err)
	}
	return m.expectedError
}

// GetObject 实现 runtime.CacheableObject 接口。
func (m *MockCacheableObject) GetObject() runtime.Object {
	if m.returnSelf {
		return m
	}
	gvk := schema.GroupVersionKind{Group: "group", Version: "version", Kind: "noncacheableTestObject"}
	return &noncacheableTestObject{gvk: gvk}
}

// interceptedCalls 返回已记录的 CacheEncode identifier 列表。
func (m *MockCacheableObject) interceptedCalls() []runtime.Identifier {
	return m.intercepted
}

// testBuffer 包装 io.Writer，用于断言编码器确实通过 CacheEncode 路径写入，
// 而不是绕过缓存直接序列化对象。
type testBuffer struct {
	writer io.Writer
	t      *testing.T
	object *MockCacheableObject
}

// Write 实现 io.Writer 接口。
func (b *testBuffer) Write(p []byte) (int, error) {
	// 写入任何字节之前，检查 <object> 是否已经拦截过 CacheEncode 调用。
	if len(b.object.interceptedCalls()) == 0 {
		b.t.Errorf("writing to buffer without handling MockCacheableObject")
	}
	return b.writer.Write(p)
}

// CacheableObjectTest 是一个应对每个 runtime.Encoder 实现运行的测试，
// 用于检查该编码器是否正确支持 CacheableObject。
// 用例覆盖：委托真实编码、委托且 GetObject 返回自身、命中缓存成功、命中缓存失败。
func CacheableObjectTest(t *testing.T, e runtime.Encoder) {
	gvk1 := schema.GroupVersionKind{Group: "group", Version: "version1", Kind: "MockCacheableObject"}

	testCases := []struct {
		desc           string
		runEncode      bool
		returnSelf     bool
		expectedResult string
		expectedError  error
	}{
		{
			desc:      "delegate",
			runEncode: true,
		},
		{
			desc:       "delegate return self",
			runEncode:  true,
			returnSelf: true,
		},
		{
			desc:           "cached success",
			runEncode:      false,
			expectedResult: "result",
			expectedError:  nil,
		},
		{
			desc:           "cached failure",
			runEncode:      false,
			expectedResult: "",
			expectedError:  fmt.Errorf("encoding error"),
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			obj := &MockCacheableObject{
				gvk:            gvk1,
				t:              t,
				runEncode:      test.runEncode,
				returnSelf:     test.returnSelf,
				expectedResult: test.expectedResult,
				expectedError:  test.expectedError,
			}
			buffer := bytes.NewBuffer(nil)
			w := &testBuffer{
				writer: buffer,
				t:      t,
				object: obj,
			}

			if err := e.Encode(obj, w); err != test.expectedError {
				t.Errorf("unexpected error: %v, expected: %v", err, test.expectedError)
			}
			if !test.runEncode {
				if result := buffer.String(); result != test.expectedResult {
					t.Errorf("unexpected result: %s, expected: %s", result, test.expectedResult)
				}
			}
			intercepted := obj.interceptedCalls()
			if len(intercepted) != 1 {
				t.Fatalf("unexpected number of intercepted calls: %v", intercepted)
			}
			if intercepted[0] != e.Identifier() {
				t.Errorf("unexpected intercepted call: %v, expected: %v", intercepted, e.Identifier())
			}
		})
	}
}
