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

package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	api "k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/registry/core/rangeallocation"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
)

var (
	errorUnableToAllocate = errors.New("unable to allocate")
)

// Etcd 为内存 Snapshottable 分配器提供 etcd 持久化外壳，每次修改都通过 GuaranteedUpdate 保存快照并处理资源版本冲突。
// Etcd 将底层分配器封装为可持久化到 etcd 的 service.Allocator。
// TODO：支持在一次持久化更新中尝试批量分配多个资源。
// TODO：拆分存储键空间，以降低并发更新冲突。
// TODO：研究无需先读取对象即可执行 CAS 更新的可行性。
type Etcd struct {
	lock sync.Mutex

	alloc   allocator.Snapshottable
	storage storage.Interface
	last    string

	baseKey  string
	resource schema.GroupResource

	destroyFn func()
}

// Etcd implements allocator.Interface and rangeallocation.RangeRegistry
var _ allocator.Interface = &Etcd{}
var _ rangeallocation.RangeRegistry = &Etcd{}

// NewEtcd 创建由 etcd 支撑的分配器；每次分配状态变更后，都会持久化对应快照。
func NewEtcd(alloc allocator.Snapshottable, baseKey string, config *storagebackend.ConfigForResource) (*Etcd, error) {
	// 除非使用 storage.Watch 方法，否则 newFunc 和 newListFunc 可以留空。
	storage, d, err := generic.NewRawStorage(config, nil, nil, baseKey)
	if err != nil {
		return nil, err
	}

	var once sync.Once
	return &Etcd{
		alloc:     alloc,
		storage:   storage,
		baseKey:   baseKey,
		resource:  config.GroupResource,
		destroyFn: func() { once.Do(d) },
	}, nil
}

// Allocate 尝试分配指定资源，并将更新后的快照持久化到 etcd。
func (e *Etcd) Allocate(offset int) (bool, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	err := e.tryUpdate(func() error {
		ok, err := e.alloc.Allocate(offset)
		if err != nil {
			return err
		}
		if !ok {
			return errorUnableToAllocate
		}
		return nil
	})
	if err != nil {
		if err == errorUnableToAllocate {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// AllocateNext 尝试自动分配下一个资源，并将更新后的快照持久化到 etcd。
func (e *Etcd) AllocateNext() (int, bool, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	var offset int
	var ok bool
	var err error

	err = e.tryUpdate(func() error {
		// 在此处更新分配偏移量。
		offset, ok, err = e.alloc.AllocateNext()
		if err != nil {
			return err
		}
		if !ok {
			return errorUnableToAllocate
		}
		return nil
	})

	if err != nil {
		if err == errorUnableToAllocate {
			return offset, false, nil
		}
		return offset, false, err
	}
	return offset, true, nil
}

// Release 尝试释放指定资源，并持久化释放后的快照。
func (e *Etcd) Release(item int) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.tryUpdate(func() error {
		return e.alloc.Release(item)
	})

}

func (e *Etcd) ForEach(fn func(int)) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.alloc.ForEach(fn)
}

// tryUpdate performs a read-update to persist the latest snapshot state of allocation.
// tryUpdate 在 etcd 的 GuaranteedUpdate 回调内先检查快照 ResourceVersion；本地版本过期时先 Restore，再执行分配并写回新快照。
func (e *Etcd) tryUpdate(fn func() error) error {
	err := e.storage.GuaranteedUpdate(context.TODO(), e.baseKey, &api.RangeAllocation{}, true, nil,
		storage.SimpleUpdate(func(input runtime.Object) (output runtime.Object, err error) {
			existing := input.(*api.RangeAllocation)
			if len(existing.ResourceVersion) == 0 {
				return nil, fmt.Errorf("cannot allocate resources of type %s at this time", e.resource.String())
			}
			if existing.ResourceVersion != e.last {
				if err := e.alloc.Restore(existing.Range, existing.Data); err != nil {
					return nil, err
				}
			}
			if err := fn(); err != nil {
				return nil, err
			}
			e.last = existing.ResourceVersion
			rangeSpec, data := e.alloc.Snapshot()
			existing.Range = rangeSpec
			existing.Data = data
			return existing, nil
		}),
		nil,
	)
	return storeerr.InterpretUpdateError(err, e.resource, "")
}

// Get 从 etcd 读取当前 RangeAllocation。若键不存在，返回对象的 ResourceVersion 为空。
func (e *Etcd) Get() (*api.RangeAllocation, error) {
	existing := &api.RangeAllocation{}
	if err := e.storage.Get(context.TODO(), e.baseKey, storage.GetOptions{IgnoreNotFound: true}, existing); err != nil {
		return nil, storeerr.InterpretGetError(err, e.resource, "")
	}
	return existing, nil
}

// CreateOrUpdate 使用传入的分配状态创建或更新 etcd 中的 RangeAllocation。
// CreateOrUpdate 用资源版本前置条件初始化/替换 RangeAllocation，并在持久化成功后同步恢复本地分配器。
func (e *Etcd) CreateOrUpdate(snapshot *api.RangeAllocation) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	last := ""
	err := e.storage.GuaranteedUpdate(context.TODO(), e.baseKey, &api.RangeAllocation{}, true, nil,
		storage.SimpleUpdate(func(input runtime.Object) (output runtime.Object, err error) {
			existing := input.(*api.RangeAllocation)
			switch {
			case len(snapshot.ResourceVersion) != 0 && len(existing.ResourceVersion) != 0:
				if snapshot.ResourceVersion != existing.ResourceVersion {
					return nil, apierrors.NewConflict(e.resource, "", fmt.Errorf("the provided resource version does not match"))
				}
			case len(existing.ResourceVersion) != 0:
				return nil, apierrors.NewConflict(e.resource, "", fmt.Errorf("another caller has already initialized the resource"))
			}
			last = snapshot.ResourceVersion
			return snapshot, nil
		}),
		nil,
	)
	if err != nil {
		return storeerr.InterpretUpdateError(err, e.resource, "")
	}
	err = e.alloc.Restore(snapshot.Range, snapshot.Data)
	if err == nil {
		e.last = last
	}
	return err
}

// Has 实现 allocator.Interface 的 Has 方法。
func (e *Etcd) Has(item int) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.alloc.Has(item)
}

// Free 实现 allocator.Interface 的 Free 方法。
func (e *Etcd) Free() int {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.alloc.Free()
}

// Destroy 实现 allocator.Interface 的 Destroy 方法。
func (e *Etcd) Destroy() {
	e.destroyFn()
}
