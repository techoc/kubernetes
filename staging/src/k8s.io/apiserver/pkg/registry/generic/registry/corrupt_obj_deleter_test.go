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

// 本文件验证 CorruptObjectDeleter 的安全边界：只有显式设置
// IgnoreStoreReadErrorWithClusterBreakingPotential 时才允许删除无法解码的对象，
// 且必须绕过调用方的准入校验、改用内置的删除校验，避免不可读对象被任意删除。

package registry

import (
	"context"
	"fmt"
	"path"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/apis/example"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/utils/ptr"
)

// fakeStorage 是 storage.Interface 的测试替身：拦截 Delete、记录参数并返回预设错误。
// 默认模拟 etcd3 处理损坏对象的流程——校验在损坏检查之后、删除之前执行，校验失败即中止删除。
// 设置 bypassValidator 后，则模拟不调用校验器就直接删除的故障后端。
type fakeStorage struct {
	storage.Interface
	deleteErr       error
	bypassValidator bool

	deleteCalled            bool
	key                     string
	expectTransformOrDecode bool
	preconditions           *storage.Preconditions
	cachedExistingObject    runtime.Object
}

// Delete 记录调用参数并按配置模拟存储层行为。
// 正常路径会把 nil 对象传给 deleteValidation：损坏对象无法解码，校验器必须能处理 nil。
func (s *fakeStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, deleteValidation storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	s.deleteCalled = true
	s.key = key
	s.expectTransformOrDecode = opts.ExpectTransformOrDecodeError
	s.preconditions = preconditions
	s.cachedExistingObject = cachedExistingObject
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if s.bypassValidator {
		return nil
	}
	// 损坏对象无法解码，传给校验器的是 nil。
	return deleteValidation(ctx, nil)
}

// TestCorruptObjectDeleterDelete 覆盖 IgnoreStoreReadErrorWithClusterBreakingPotential
// 的取值、存储层返回的错误类型、dry-run 以及后端未调用校验器（模拟 bug）等组合。
func TestCorruptObjectDeleterDelete(t *testing.T) {
	for _, test := range []struct {
		name               string
		deleteErr          error
		bypassValidator    bool
		opts               *metav1.DeleteOptions
		expectDeleteCalled bool
		wantDeleted        bool
		wantErr            func(error) bool
	}{
		{
			// 未显式开启“忽略读错误”时必须失败关闭，绝不触碰存储层。
			name:    "options nil, should return internal error",
			opts:    nil,
			wantErr: errors.IsInternalError,
		},
		{
			name:    "options empty, should return internal error",
			opts:    &metav1.DeleteOptions{},
			wantErr: errors.IsInternalError,
		},
		{
			// 显式 false 与未设置等价：都拒绝删除。
			name:    "option false, should return internal error",
			opts:    &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(false)},
			wantErr: errors.IsInternalError,
		},
		{
			// 开启后对象实际可解码：这是正常删除路径，存储层返回的 InvalidObj
			// 说明对象状态不满足删除条件，应映射为 Conflict 而非内部错误。
			name:               "option true, object decodable, store returns InvalidObj",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true)},
			deleteErr:          storage.NewInvalidObjError("/pods/foo", "object is decodable"),
			expectDeleteCalled: true,
			wantErr:            errors.IsConflict,
		},
		{
			// 核心用例：对象损坏、删除成功，返回 deleted=true。
			name:               "option true, object not decodable, delete succeeds",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true)},
			deleteErr:          nil,
			expectDeleteCalled: true,
			wantDeleted:        true,
			wantErr:            func(err error) bool { return err == nil },
		},
		{
			name:               "option true, object not found, store returns NotFound",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true)},
			deleteErr:          storage.NewKeyNotFoundError("/pods/foo", 0),
			expectDeleteCalled: true,
			wantErr:            errors.IsNotFound,
		},
		{
			// dry-run 与真实删除走同一校验路径，但不改变存储状态；
			// 这里验证“允许删除”的判定结果与真实删除一致。
			name:               "option true, dry run, object not decodable, store honors the validator",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: new(true), DryRun: []string{"All"}},
			expectDeleteCalled: true,
			wantDeleted:        true,
			wantErr:            func(err error) bool { return err == nil },
		},
		{
			name:               "option true, dry run, object decodable, store returns InvalidObj",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: new(true), DryRun: []string{"All"}},
			deleteErr:          storage.NewInvalidObjError("/pods/foo", "object is decodable"),
			expectDeleteCalled: true,
			wantErr:            errors.IsConflict,
		},
		{
			name:               "option true, dry run, object not found, store returns NotFound",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: new(true), DryRun: []string{"All"}},
			deleteErr:          storage.NewKeyNotFoundError("/pods/foo", 0),
			expectDeleteCalled: true,
			wantErr:            errors.IsNotFound,
		},
		{
			// 后端未调用校验器就删除（bug 场景）必须被识别为内部错误，
			// 否则会绕过安全校验静默删除对象。
			name:               "option true, dry run, store deletes without invoking the validator",
			opts:               &metav1.DeleteOptions{IgnoreStoreReadErrorWithClusterBreakingPotential: new(true), DryRun: []string{"All"}},
			bypassValidator:    true,
			expectDeleteCalled: true,
			wantErr:            errors.IsInternalError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), "test")
			fs := &fakeStorage{deleteErr: test.deleteErr, bypassValidator: test.bypassValidator}
			const podPrefix = "/pods/"
			store := &Store{
				NewFunc:                   func() runtime.Object { return &example.Pod{} },
				DefaultQualifiedResource:  schema.GroupResource{Resource: "pods"},
				SingularQualifiedResource: schema.GroupResource{Resource: "pod"},
				KeyRootFunc:               func(ctx context.Context) string { return podPrefix },
				KeyFunc: func(ctx context.Context, name string) (string, error) {
					if _, ok := genericapirequest.NamespaceFrom(ctx); !ok {
						return "", fmt.Errorf("namespace is required")
					}
					return path.Join(podPrefix, name), nil
				},
				Storage: DryRunnableStorage{Storage: fs},
			}
			deleter := NewCorruptObjectDeleter(store)

			// 不安全删除器必须忽略调用方传入的准入校验，改用自带的校验器：
			// 普通删除全部放行，dry-run 则固定失败。若删除器重新接回外部准入，
			// 这个回调会让测试失败。
			obj, deleted, err := deleter.Delete(ctx, "foo", func(context.Context, runtime.Object) error {
				t.Fatal("caller-provided admission was invoked")
				return nil
			}, test.opts)

			if obj != nil {
				t.Errorf("Expected nil object, but got %v", obj)
			}
			if !test.wantErr(err) {
				t.Errorf("Unexpected error: %v", err)
			}
			if test.wantDeleted != deleted {
				t.Errorf("Expected deleted to be %t, but got %t", test.wantDeleted, deleted)
			}
			if test.expectDeleteCalled != fs.deleteCalled {
				t.Errorf("Expected storage Delete called=%t, but got %t", test.expectDeleteCalled, fs.deleteCalled)
			}
			if fs.deleteCalled {
				// 删除调用的参数同样是安全契约的一部分：
				// 固定 key、必须要求 transform/decode 错误可忽略、禁止前置条件与缓存对象。
				if want, got := "/pods/foo", fs.key; want != got {
					t.Errorf("Expected storage Delete to be called with key %q, but got %q", want, got)
				}
				if !fs.expectTransformOrDecode {
					t.Error("Expected storage Delete to be called with ExpectTransformOrDecodeError=true")
				}
				if fs.preconditions != nil {
					t.Error("Expected storage Delete to be called with nil preconditions")
				}
				if fs.cachedExistingObject != nil {
					t.Error("Expected storage Delete to be called with nil cachedExistingObject")
				}
			}
		})
	}
}
