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

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// fakeReadinessStorage 只实现 ReadinessCheck，用固定错误模拟存储就绪状态。
type fakeReadinessStorage struct {
	result error
}

// New 返回 nil：测试不关心对象创建。
func (s *fakeReadinessStorage) New() runtime.Object { return nil }

// Destroy 无操作。
func (s *fakeReadinessStorage) Destroy() {}

// ReadinessCheck 返回预设错误；nil 表示就绪。
func (s *fakeReadinessStorage) ReadinessCheck() error { return s.result }

// testGVR 生成第 index 个测试用 GVR，保证各存储的注册 key 唯一。
func testGVR(index int) metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    "group",
		Version:  "version",
		Resource: fmt.Sprintf("resource-%d", index),
	}
}

// TestStorageReadinessHook 验证 check 的聚合语义：任一存储未就绪则整体不就绪，
// 只有全部存储就绪后 check 才通过。
func TestStorageReadinessHook(t *testing.T) {
	h := NewStorageReadinessHook(time.Second)

	numChecks := 5
	storages := make([]*fakeReadinessStorage, numChecks)
	for i := 0; i < numChecks; i++ {
		storages[i] = &fakeReadinessStorage{
			result: fmt.Errorf("failed"),
		}
		h.RegisterStorage(testGVR(i), storages[i])
	}

	// 逐个把存储标记为就绪，每次都检查整体结果：
	// 只要仍有未就绪项就必须失败。
	for i := 0; i < numChecks; i++ {
		if ok := h.check(); ok {
			t.Errorf("%d: unexpected check pass", i)
		}
		storages[i].result = nil
	}
	if ok := h.check(); !ok {
		t.Errorf("unexpected check failure")
	}
}

// TestStorageReadinessHookTimeout 验证 Hook 在超时后返回，而不是无限等待存储就绪；
// 对 post-start hook 而言，超时属于可接受结果，因此不应返回错误。
func TestStorageReadinessHookTimeout(t *testing.T) {
	h := NewStorageReadinessHook(time.Second)

	storage := &fakeReadinessStorage{
		result: fmt.Errorf("failed"),
	}
	h.RegisterStorage(testGVR(0), storage)

	ctx := context.Background()
	hookCtx := PostStartHookContext{
		LoopbackClientConfig: nil,
		Context:              ctx,
	}
	if err := h.Hook(hookCtx); err != nil {
		t.Errorf("unexpected hook failure on timeout")
	}
}
