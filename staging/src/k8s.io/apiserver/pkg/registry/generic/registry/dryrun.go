/*
Copyright 2018 The Kubernetes Authors.

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

package registry

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// DryRunnableStorage 包装 storage.Interface，在 dryRun 请求下跳过真实写入：
// 读取、校验与更新函数照常执行，但结果只写回内存对象，不落到后端存储。
// 这样 dry-run 请求仍会经历与真实请求相同的准入、校验和错误路径，
// 同时保证存储与 ResourceVersion 不被改变。
type DryRunnableStorage struct {
	Storage storage.Interface
	Codec   runtime.Codec
}

// Versioner 透传底层存储的版本器。
func (s *DryRunnableStorage) Versioner() storage.Versioner {
	return s.Storage.Versioner()
}

// Create 在 dryRun 时只检查 key 是否已存在并把 obj 拷贝到 out，不写入存储。
func (s *DryRunnableStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64, dryRun bool) error {
	if dryRun {
		// 模拟真实 Create 的 AlreadyExists 语义：已存在则返回冲突错误。
		if err := s.Storage.Get(ctx, key, storage.GetOptions{}, out); err == nil {
			return storage.NewKeyExistsError(key, 0)
		}
		return s.copyInto(obj, out)
	}
	return s.Storage.Create(ctx, key, obj, out, ttl)
}

// Delete 在 dryRun 时读取对象、检查前置条件并执行删除校验，但不真正删除。
func (s *DryRunnableStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, deleteValidation storage.ValidateObjectFunc, dryRun bool, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if dryRun {
		if err := s.Storage.Get(ctx, key, storage.GetOptions{}, out); err != nil {
			return err
		}
		if err := preconditions.Check(key, out); err != nil {
			return err
		}
		return deleteValidation(ctx, out)
	}
	return s.Storage.Delete(ctx, key, out, preconditions, deleteValidation, cachedExistingObject, opts)
}

// Watch 直接透传：watch 本身不修改数据，无需区分 dry-run。
func (s *DryRunnableStorage) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.Storage.Watch(ctx, key, opts)
}

// Get 直接透传。
func (s *DryRunnableStorage) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	return s.Storage.Get(ctx, key, opts, objPtr)
}

// GetList 直接透传。
func (s *DryRunnableStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return s.Storage.GetList(ctx, key, opts, listObj)
}

// GuaranteedUpdate 在 dryRun 时模拟完整更新流程：读取当前对象、检查前置条件、
// 调用 tryUpdate 得到新对象，最后只把结果拷贝到 destination，不持久化。
func (s *DryRunnableStorage) GuaranteedUpdate(
	ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
	preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, dryRun bool, cachedExistingObject runtime.Object) error {
	if dryRun {
		var current runtime.Object
		v, err := conversion.EnforcePtr(destination)
		if err != nil {
			return fmt.Errorf("unable to convert output object to pointer: %v", err)
		}
		// 按 destination 的实际类型新建一个空对象来承载当前值；
		// unstructured 需要走 NewEmptyInstance 才能保留正确的类型信息。
		if u, ok := v.Addr().Interface().(runtime.Unstructured); ok {
			current = u.NewEmptyInstance()
		} else {
			current = reflect.New(v.Type()).Interface().(runtime.Object)
		}

		err = s.Storage.Get(ctx, key, storage.GetOptions{IgnoreNotFound: ignoreNotFound}, current)
		if err != nil {
			return err
		}
		err = preconditions.Check(key, current)
		if err != nil {
			return err
		}
		rev, err := s.Versioner().ObjectResourceVersion(current)
		if err != nil {
			return err
		}
		// 以当前 ResourceVersion 调用 tryUpdate，使 dry-run 暴露真实的冲突与校验错误。
		updated, _, err := tryUpdate(current, storage.ResponseMeta{ResourceVersion: rev})
		if err != nil {
			return err
		}
		return s.copyInto(updated, destination)
	}
	return s.Storage.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject)
}

// Stats 直接透传。
func (s *DryRunnableStorage) Stats(ctx context.Context) (storage.Stats, error) {
	return s.Storage.Stats(ctx)
}

// copyInto 通过 Codec 编码再解码实现深拷贝，确保 in 与 out 不共享底层数据，
// 也避免 dry-run 的返回值意外持有存储缓存中的对象。
func (s *DryRunnableStorage) copyInto(in, out runtime.Object) error {
	var data []byte

	data, err := runtime.Encode(s.Codec, in)
	if err != nil {
		return err
	}
	_, _, err = s.Codec.Decode(data, nil, out)
	return err
}
