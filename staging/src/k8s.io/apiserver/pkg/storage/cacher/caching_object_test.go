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

package cacher

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// mockEncoder 记录被调用的次数并可返回预设错误，用于验证缓存是否真的生效。
type mockEncoder struct {
	identifier     runtime.Identifier
	expectedResult string
	expectedError  error

	// callsNumber 用原子操作累加，因为并发测试会多 goroutine 同时调用。
	callsNumber int32
}

// newMockEncoder 构造一个 mock 编码器。
func newMockEncoder(id, result string, err error) *mockEncoder {
	return &mockEncoder{
		identifier:     runtime.Identifier(id),
		expectedResult: result,
		expectedError:  err,
	}
}

// encode 按预设结果写入；一旦被调用多次，callsNumber 会暴露缓存失效。
func (e *mockEncoder) encode(_ runtime.Object, w io.Writer) error {
	atomic.AddInt32(&e.callsNumber, 1)
	if e.expectedError != nil {
		return e.expectedError
	}
	_, err := w.Write([]byte(e.expectedResult))
	return err
}

// TestCachingObject 验证 CacheEncode 对每个 identifier 只编码一次（含返回错误的情况），
// 并原样透传编码结果与错误。
func TestCachingObject(t *testing.T) {
	object, err := newCachingObject(&v1.Pod{})
	if err != nil {
		t.Fatalf("couldn't create cachingObject: %v", err)
	}

	encoders := []*mockEncoder{
		newMockEncoder("1", "result1", nil),
		newMockEncoder("2", "", fmt.Errorf("mock error")),
		newMockEncoder("3", "result3", nil),
	}

	for _, encoder := range encoders {
		buffer := bytes.NewBuffer(nil)
		err := object.CacheEncode(encoder.identifier, encoder.encode, buffer)
		if a, e := err, encoder.expectedError; e != a {
			t.Errorf("%s: unexpected error: %v, expected: %v", encoder.identifier, a, e)
		}
		if a, e := buffer.String(), encoder.expectedResult; e != a {
			t.Errorf("%s: unexpected result: %s, expected: %s", encoder.identifier, a, e)
		}
	}
	for _, encoder := range encoders {
		if encoder.callsNumber != 1 {
			t.Errorf("%s: unexpected number of encode() calls: %d", encoder.identifier, encoder.callsNumber)
		}
	}
}

// TestCachingObjectFieldAccessors 验证 setter 修改的字段对后续编码（包括 GetObject
// 返回的对象）都可见；所有 accessor 共用同一份逻辑，这里仅以 namespace 为代表。
func TestCachingObjectFieldAccessors(t *testing.T) {
	object, err := newCachingObject(&v1.Pod{})
	if err != nil {
		t.Fatalf("couldn't create cachingObject: %v", err)
	}

	// Given accessors for all fields implement the same logic,
	// we are choosing an arbitrary one to test.
	namespace := "namespace"
	object.SetNamespace(namespace)

	encodeNamespace := func(obj runtime.Object, w io.Writer) error {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			t.Fatalf("failed to get accessor for %#v: %v", obj, err)
		}
		_, err = w.Write([]byte(accessor.GetNamespace()))
		return err
	}
	buffer := bytes.NewBuffer(nil)
	if err := object.CacheEncode("", encodeNamespace, buffer); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if a, e := buffer.String(), namespace; a != e {
		t.Errorf("unexpected serialization: %s, expected: %s", a, e)
	}

	// GetObject should also set namespace.
	buffer.Reset()
	if err := encodeNamespace(object.GetObject(), buffer); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if a, e := buffer.String(), namespace; a != e {
		t.Errorf("unexpected serialization: %s, expected: %s", a, e)
	}
}

// TestCachingObjectRaces 用大量 goroutine 并发读写，配合 -race 检测缓存与 accessor
// 的同步问题；同时断言每个 encoder 仍只被调用一次。
func TestCachingObjectRaces(t *testing.T) {
	object, err := newCachingObject(&v1.Pod{})
	if err != nil {
		t.Fatalf("couldn't create cachingObject: %v", err)
	}

	encoders := []*mockEncoder{}
	for i := 0; i < 10; i++ {
		encoder := newMockEncoder(fmt.Sprintf("%d", i), "result", nil)
		encoders = append(encoders, encoder)
	}

	numWorkers := 1000
	wg := &sync.WaitGroup{}
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			object.SetNamespace("namespace")
			buffer := bytes.NewBuffer(nil)
			for _, encoder := range encoders {
				buffer.Reset()
				if err := object.CacheEncode(encoder.identifier, encoder.encode, buffer); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if callsNumber := atomic.LoadInt32(&encoder.callsNumber); callsNumber != 1 {
					t.Errorf("unexpected number of serializations: %d", callsNumber)
				}
			}
			accessor, err := meta.Accessor(object.GetObject())
			if err != nil {
				t.Errorf("failed to get accessor: %v", err)
				return
			}
			if namespace := accessor.GetNamespace(); namespace != "namespace" {
				t.Errorf("unexpected namespace: %s", namespace)
			}
		}()
	}
	wg.Wait()
}

// TestCachingObjectLazyDeepCopy 验证深拷贝是惰性的：从未修改时不拷贝，
// 设置相同值也不拷贝，只有真正改变时才拷贝。
func TestCachingObjectLazyDeepCopy(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "name",
			ResourceVersion: "123",
		},
	}
	object, err := newCachingObject(pod)
	if err != nil {
		t.Fatalf("couldn't create cachingObject: %v", err)
	}

	if object.deepCopied != false {
		t.Errorf("object deep-copied without the need")
	}

	object.SetResourceVersion("123")
	if object.deepCopied != false {
		t.Errorf("object deep-copied on no-op change")
	}
	object.SetResourceVersion("234")
	if object.deepCopied != true {
		t.Errorf("object not deep-copied on change")
	}
}
