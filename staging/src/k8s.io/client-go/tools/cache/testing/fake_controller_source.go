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

// Package framework provides fake list/watch sources used by cache and reflector tests.
//
// Package framework 提供用于测试的假数据源：它同时实现 List 与 Watch，
// 可模拟资源版本、事件丢失、连接中断与过期（ResourceExpired）等场景。
package framework

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// NewFakeControllerSource 创建通用数据源；100 的缓冲区在测试规模下足够，
// 满时阻塞发送端（WaitIfChannelFull）以模拟背压。
func NewFakeControllerSource() *FakeControllerSource {
	return &FakeControllerSource{
		Items:       map[nnu]runtime.Object{},
		Broadcaster: watch.NewBroadcaster(100, watch.WaitIfChannelFull),
	}
}

// NewFakePVControllerSource 创建只返回 PersistentVolumeList 的假数据源。
func NewFakePVControllerSource() *FakePVControllerSource {
	return &FakePVControllerSource{
		FakeControllerSource{
			Items:       map[nnu]runtime.Object{},
			Broadcaster: watch.NewBroadcaster(100, watch.WaitIfChannelFull),
		}}
}

// NewFakePVCControllerSource 创建只返回 PersistentVolumeClaimList 的假数据源。
func NewFakePVCControllerSource() *FakePVCControllerSource {
	return &FakePVCControllerSource{
		FakeControllerSource{
			Items:       map[nnu]runtime.Object{},
			Broadcaster: watch.NewBroadcaster(100, watch.WaitIfChannelFull),
		}}
}

// FakeControllerSource implements listing/watching for testing.
//
// 所有字段由 lock 保护；changes 保留每个 resourceVersion 对应的事件，
// 使从任意较新 RV 开始的 watch 都能重放。
type FakeControllerSource struct {
	lock        sync.RWMutex
	Items       map[nnu]runtime.Object
	changes     []watch.Event // one change per resourceVersion
	Broadcaster *watch.Broadcaster
	lastRV      int

	// Set this to simulate an error on List()
	ListError error
}

// FakePVControllerSource 复用通用逻辑，仅定制 List 返回的类型。
type FakePVControllerSource struct {
	FakeControllerSource
}

// FakePVCControllerSource 复用通用逻辑，仅定制 List 返回的类型。
type FakePVCControllerSource struct {
	FakeControllerSource
}

// namespace, name, uid to be used as a key.
// 键包含 UID：同名对象删除后重建会被视为不同对象。
type nnu struct {
	namespace, name string
	uid             types.UID
}

// ResetWatch simulates connection problems; creates a new Broadcaster and flushes
// the change queue so that clients have to re-list and watch.
//
// 清空 changes 后，旧 RV 的 watch 会收到 ResourceExpired，正是测试 Reflector
// 重新 list 的路径。
func (f *FakeControllerSource) ResetWatch() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Broadcaster.Shutdown()
	f.Broadcaster = watch.NewBroadcaster(100, watch.WaitIfChannelFull)
	f.changes = []watch.Event{}
}

// Add adds an object to the set and sends an add event to watchers.
// obj's ResourceVersion is set.
func (f *FakeControllerSource) Add(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Added, Object: obj}, 1)
}

// Modify updates an object in the set and sends a modified event to watchers.
// obj's ResourceVersion is set.
func (f *FakeControllerSource) Modify(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Modified, Object: obj}, 1)
}

// Delete deletes an object from the set and sends a delete event to watchers.
// obj's ResourceVersion is set.
func (f *FakeControllerSource) Delete(lastValue runtime.Object) {
	f.Change(watch.Event{Type: watch.Deleted, Object: lastValue}, 1)
}

// AddDropWatch adds an object to the set but forgets to send an add event to
// watchers.
// obj's ResourceVersion is set.
//
// watchProbability 为 0，用于模拟事件丢失：对象已进入存储但 watch 客户端看不到。
func (f *FakeControllerSource) AddDropWatch(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Added, Object: obj}, 0)
}

// ModifyDropWatch updates an object in the set but forgets to send a modify
// event to watchers.
// obj's ResourceVersion is set.
func (f *FakeControllerSource) ModifyDropWatch(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Modified, Object: obj}, 0)
}

// DeleteDropWatch deletes an object from the set but forgets to send a delete
// event to watchers.
// obj's ResourceVersion is set.
func (f *FakeControllerSource) DeleteDropWatch(lastValue runtime.Object) {
	f.Change(watch.Event{Type: watch.Deleted, Object: lastValue}, 0)
}

// key 生成对象在 Items 中的索引。
func (f *FakeControllerSource) key(accessor metav1.Object) nnu {
	return nnu{accessor.GetNamespace(), accessor.GetName(), accessor.GetUID()}
}

// Change records the given event (setting the object's resource version) and
// sends a watch event with the specified probability.
//
// 先更新存储与 changes 再广播，保证 watch 客户端收到事件时状态已可见；
// watchProbability 用于刻意丢弃事件，以测试客户端的重新同步逻辑。
func (f *FakeControllerSource) Change(e watch.Event, watchProbability float64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	accessor, err := meta.Accessor(e.Object)
	if err != nil {
		panic(err) // this is test code only
	}

	f.lastRV += 1
	accessor.SetResourceVersion(strconv.Itoa(f.lastRV))
	f.changes = append(f.changes, e)
	key := f.key(accessor)
	switch e.Type {
	case watch.Added, watch.Modified:
		f.Items[key] = e.Object
	case watch.Deleted:
		delete(f.Items, key)
	}

	if rand.Float64() < watchProbability {
		f.Broadcaster.Action(e.Type, e.Object)
	}
}

// getListItemsLocked 调用方必须持有读锁；返回深拷贝避免客户端修改内部状态。
func (f *FakeControllerSource) getListItemsLocked() ([]runtime.Object, error) {
	list := make([]runtime.Object, 0, len(f.Items))
	for _, obj := range f.Items {
		// Must make a copy to allow clients to modify the object.
		// Otherwise, if they make a change and write it back, they
		// will inadvertently change our canonical copy (in
		// addition to racing with other clients).
		list = append(list, obj.DeepCopyObject())
	}
	return list, nil
}

// List returns a list object, with its resource version set.
func (f *FakeControllerSource) List(options metav1.ListOptions) (runtime.Object, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	if f.ListError != nil {
		return nil, f.ListError
	}

	list, err := f.getListItemsLocked()
	if err != nil {
		return nil, err
	}
	listObj := &v1.List{}
	if err := meta.SetList(listObj, list); err != nil {
		return nil, err
	}
	listAccessor, err := meta.ListAccessor(listObj)
	if err != nil {
		return nil, err
	}
	// list 的 RV 必须是当前最新值，否则 watch 会从错误的起点开始。
	listAccessor.SetResourceVersion(strconv.Itoa(f.lastRV))
	return listObj, nil
}

// List returns a list object, with its resource version set.
func (f *FakePVControllerSource) List(options metav1.ListOptions) (runtime.Object, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list, err := f.FakeControllerSource.getListItemsLocked()
	if err != nil {
		return nil, err
	}
	listObj := &v1.PersistentVolumeList{}
	if err := meta.SetList(listObj, list); err != nil {
		return nil, err
	}
	listAccessor, err := meta.ListAccessor(listObj)
	if err != nil {
		return nil, err
	}
	listAccessor.SetResourceVersion(strconv.Itoa(f.lastRV))
	return listObj, nil
}

// List returns a list object, with its resource version set.
func (f *FakePVCControllerSource) List(options metav1.ListOptions) (runtime.Object, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list, err := f.FakeControllerSource.getListItemsLocked()
	if err != nil {
		return nil, err
	}
	listObj := &v1.PersistentVolumeClaimList{}
	if err := meta.SetList(listObj, list); err != nil {
		return nil, err
	}
	listAccessor, err := meta.ListAccessor(listObj)
	if err != nil {
		return nil, err
	}
	listAccessor.SetResourceVersion(strconv.Itoa(f.lastRV))
	return listObj, nil
}

// Watch returns a watch, which will be pre-populated with all changes
// after resourceVersion.
//
// 三种情况：RV 落后则重放尚未被 flush 的事件；RV 过旧返回 ResourceExpired；
// RV 在未来则直接报错（真实 apiserver 也不会接受）。
func (f *FakeControllerSource) Watch(options metav1.ListOptions) (watch.Interface, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	rc, err := strconv.Atoi(options.ResourceVersion)
	if err != nil {
		return nil, err
	}
	if rc < f.lastRV {
		// if the change queue was flushed...
		if len(f.changes) == 0 {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", rc, f.lastRV))
		}

		// get the RV of the oldest object in the change queue
		oldestRV, err := meta.NewAccessor().ResourceVersion(f.changes[0].Object)
		if err != nil {
			panic(err)
		}
		oldestRC, err := strconv.Atoi(oldestRV)
		if err != nil {
			panic(err)
		}
		if rc < oldestRC {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", rc, oldestRC))
		}

		changes := []watch.Event{}
		for _, c := range f.changes[rc-oldestRC+1:] {
			// Must make a copy to allow clients to modify the
			// object.  Otherwise, if they make a change and write
			// it back, they will inadvertently change the our
			// canonical copy (in addition to racing with other
			// clients).
			changes = append(changes, watch.Event{Type: c.Type, Object: c.Object.DeepCopyObject()})
		}
		return f.Broadcaster.WatchWithPrefix(changes)
	} else if rc > f.lastRV {
		return nil, errors.New("resource version in the future not supported by this fake")
	}
	return f.Broadcaster.Watch()
}

// Shutdown closes the underlying broadcaster, waiting for events to be
// delivered. It's an error to call any method after calling shutdown. This is
// enforced by Shutdown() leaving f locked.
func (f *FakeControllerSource) Shutdown() {
	f.lock.Lock() // Purposely no unlock.
	f.Broadcaster.Shutdown()
}
