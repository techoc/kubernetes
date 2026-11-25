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

package generic

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/tools/cache"
)

// TestCachedHasSynced 验证 cachedHasSynced 在结果为 false 时每次都重新调用委托函数，
// 一旦变为 true 就永久缓存，避免在缓存已同步后持续付出检查成本。
func TestCachedHasSynced(t *testing.T) {

	called := 0
	result := false
	cachedFunc := cachedHasSynced(func() bool {
		called++
		return result
	})

	if cachedFunc() {
		t.Fatal("expected false")
	}
	if called != 1 {
		t.Fatalf("expected called=1, got %d", called)
	}

	if cachedFunc() {
		t.Fatal("expected false")
	}
	if called != 2 {
		t.Fatalf("expected called=2, got %d", called)
	}

	result = true
	if !cachedFunc() {
		t.Fatal("expected true")
	}
	if called != 3 {
		t.Fatalf("expected called=3, got %d", called)
	}

	if !cachedFunc() {
		t.Fatal("expected true")
	}
	if called != 3 {
		// 返回 true 后不再调用底层函数。
		t.Fatalf("expected called=3, got %d", called)
	}
}

// TestProtectedLister 验证未同步时 protectedLister 直接返回 notReadyErr 且不触碰
// 底层 lister；同步后才把 List/Get 委托下去。这是一个启动阶段的保护：
// informer 缓存未就绪时读取会导致配额计算使用不完整数据。
func TestProtectedLister(t *testing.T) {

	hasSynced := false
	notReadyErr := errors.New("not ready")
	fake := &fakeLister{}
	l := &protectedLister{
		hasSynced:   func() bool { return hasSynced },
		notReadyErr: notReadyErr,
		delegate:    fake,
	}
	if _, err := l.List(nil); err != notReadyErr {
		t.Fatalf("expected %v, got %v", notReadyErr, err)
	}
	if _, err := l.Get(""); err != notReadyErr {
		t.Fatalf("expected %v, got %v", notReadyErr, err)
	}
	if fake.called != 0 {
		t.Fatalf("expected called=0, got %d", fake.called)
	}
	fake.called = 0

	hasSynced = true

	if _, err := l.List(nil); err != errFakeLister {
		t.Fatalf("expected %v, got %v", errFakeLister, err)
	}
	if _, err := l.Get(""); err != errFakeLister {
		t.Fatalf("expected %v, got %v", errFakeLister, err)
	}
	if fake.called != 2 {
		t.Fatalf("expected called=2, got %d", fake.called)
	}
	fake.called = 0

	hasSynced = false

	if _, err := l.List(nil); err != notReadyErr {
		t.Fatalf("expected %v, got %v", notReadyErr, err)
	}
	if _, err := l.Get(""); err != notReadyErr {
		t.Fatalf("expected %v, got %v", notReadyErr, err)
	}
	if fake.called != 0 {
		t.Fatalf("expected called=2, got %d", fake.called)
	}
}

var errFakeLister = errors.New("errFakeLister")

// fakeLister 记录调用次数并固定返回 errFakeLister。
type fakeLister struct {
	called int
}

// List 记录一次调用并返回预设错误。
func (f *fakeLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	f.called++
	return nil, errFakeLister
}

// Get 记录一次调用并返回预设错误。
func (f *fakeLister) Get(name string) (runtime.Object, error) {
	f.called++
	return nil, errFakeLister
}

// ByNamespace 未实现，不应被对象计数评估器调用。
func (f *fakeLister) ByNamespace(namespace string) cache.GenericNamespaceLister {
	panic("not implemented")
}

// TestObjectCountEvaluatorHandles 验证对象计数评估器只处理顶层的 Create：
// Update/Delete/Connect 与子资源请求都不计入 count/<resource> 配额，
// 否则用量会被重复计算。
func TestObjectCountEvaluatorHandles(t *testing.T) {
	evaluator := objectCountEvaluator{}
	testCases := []struct {
		name  string
		attrs admission.Attributes
		want  bool
	}{
		{
			name:  "create",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "", admission.Create, nil, false, nil),
			want:  true,
		},
		{
			name:  "update",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "", admission.Update, nil, false, nil),
			want:  false,
		},
		{
			name:  "delete",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "", admission.Delete, nil, false, nil),
			want:  false,
		},
		{
			name:  "connect",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "", admission.Connect, nil, false, nil),
			want:  false,
		},
		{
			name:  "create-subresource",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "subresource", admission.Create, nil, false, nil),
			want:  false,
		},
		{
			name:  "update-subresource",
			attrs: admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{Group: "core", Version: "v1", Kind: "Pod"}, "", "", schema.GroupVersionResource{Group: "core", Version: "v1", Resource: "pods"}, "subresource", admission.Update, nil, false, nil),
			want:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := evaluator.Handles(tc.attrs)

			if tc.want != actual {
				t.Errorf("%s expected:\n%v\n, actual:\n%v", tc.name, tc.want, actual)
			}
		})
	}
}
