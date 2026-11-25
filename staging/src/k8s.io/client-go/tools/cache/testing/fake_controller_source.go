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

// Package framework 提供 cache 与 reflector 测试使用的假 list/watch 数据源：
// 它同时实现 List 与 Watch，可模拟资源版本、事件丢失、连接中断与过期
// （ResourceExpired）等场景。
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

// FakeControllerSource 实现了用于测试的 list/watch。
//
// 所有字段由 lock 保护；changes 保留每个 resourceVersion 对应的事件，
// 使从任意较新 RV 开始的 watch 都能重放。
type FakeControllerSource struct {
	lock        sync.RWMutex
	Items       map[nnu]runtime.Object
	changes     []watch.Event // 每个 resourceVersion 对应一个变更
	Broadcaster *watch.Broadcaster
	lastRV      int

	// 置位该字段可模拟 List() 返回错误
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

// namespace、name、uid 共同构成键。
// 键包含 UID：同名对象删除后重建会被视为不同对象。
type nnu struct {
	namespace, name string
	uid             types.UID
}

// ResetWatch 模拟连接问题：创建新的 Broadcaster 并清空 change 队列，
// 迫使客户端重新执行 list 和 watch。
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

// Add 把对象加入集合并向 watchers 发送 add 事件，会设置 obj 的 ResourceVersion。
func (f *FakeControllerSource) Add(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Added, Object: obj}, 1)
}

// Modify 更新集合中的对象并向 watchers 发送 modified 事件，会设置 obj 的 ResourceVersion。
func (f *FakeControllerSource) Modify(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Modified, Object: obj}, 1)
}

// Delete 从集合中删除对象并向 watchers 发送 delete 事件，会设置 obj 的 ResourceVersion。
func (f *FakeControllerSource) Delete(lastValue runtime.Object) {
	f.Change(watch.Event{Type: watch.Deleted, Object: lastValue}, 1)
}

// AddDropWatch 把对象加入集合，但故意不向 watchers 发送 add 事件，
// 会设置 obj 的 ResourceVersion。
//
// watchProbability 为 0，用于模拟事件丢失：对象已进入存储但 watch 客户端看不到。
func (f *FakeControllerSource) AddDropWatch(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Added, Object: obj}, 0)
}

// ModifyDropWatch 更新集合中的对象，但故意不向 watchers 发送 modify 事件，
// 会设置 obj 的 ResourceVersion。
func (f *FakeControllerSource) ModifyDropWatch(obj runtime.Object) {
	f.Change(watch.Event{Type: watch.Modified, Object: obj}, 0)
}

// DeleteDropWatch 从集合中删除对象，但故意不向 watchers 发送 delete 事件，
// 会设置 obj 的 ResourceVersion。
func (f *FakeControllerSource) DeleteDropWatch(lastValue runtime.Object) {
	f.Change(watch.Event{Type: watch.Deleted, Object: lastValue}, 0)
}

// key 生成对象在 Items 中的索引。
func (f *FakeControllerSource) key(accessor metav1.Object) nnu {
	return nnu{accessor.GetNamespace(), accessor.GetName(), accessor.GetUID()}
}

// Change 记录给定事件（同时设置对象的 resource version），并按指定概率发送 watch 事件。
//
// 先更新存储与 changes 再广播，保证 watch 客户端收到事件时状态已可见；
// watchProbability 用于刻意丢弃事件，以测试客户端的重新同步逻辑。
func (f *FakeControllerSource) Change(e watch.Event, watchProbability float64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	accessor, err := meta.Accessor(e.Object)
	if err != nil {
		panic(err) // 这里只是测试代码
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
		// 必须做一份拷贝，允许客户端修改对象。否则客户端改动后写回时，
		// 会意外改掉我们的权威副本（还与其他客户端存在数据竞争）。
		list = append(list, obj.DeepCopyObject())
	}
	return list, nil
}

// List 返回一个 list 对象，并设置其 resource version。
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

// List 返回一个 list 对象，并设置其 resource version。
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

// List 返回一个 list 对象，并设置其 resource version。
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

// Watch 返回一个 watch，其中预先填充 resourceVersion 之后的全部变更。
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
		// 如果 change 队列已被清空……
		if len(f.changes) == 0 {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", rc, f.lastRV))
		}

		// 取 change 队列中最老对象的 RV
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
			// 必须做一份拷贝，允许客户端修改对象。否则客户端改动后写回时，
			// 会意外改掉我们的权威副本（还与其他客户端存在数据竞争）。
			changes = append(changes, watch.Event{Type: c.Type, Object: c.Object.DeepCopyObject()})
		}
		return f.Broadcaster.WatchWithPrefix(changes)
	} else if rc > f.lastRV {
		return nil, errors.New("resource version in the future not supported by this fake")
	}
	return f.Broadcaster.Watch()
}

// Shutdown 关闭底层 broadcaster，并等待事件投递完成。调用 Shutdown 之后再调用
// 任何方法都是错误的——Shutdown() 故意不释放锁来强制这一点。
func (f *FakeControllerSource) Shutdown() {
	f.lock.Lock() // 故意不加 unlock。
	f.Broadcaster.Shutdown()
}
