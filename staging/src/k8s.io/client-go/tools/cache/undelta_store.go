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

package cache

// UndeltaStore 监听增量更新，并在每次变更后推送完整状态。
// 它实现了 Store 接口，因此可以接收 Reflector 镜像出来的对象流。
// 每当收到任何全量（Store.Replace）或增量（Store.Add、Store.Update、Store.Delete）
// 变更，它就调用 PushFunc 发送完整状态。它是线程安全的：保证每个变更
// （Add、Update、Replace、Delete）都会触发一次 PushFunc 调用，但 PushFunc
// 有时也可能带着相同的值被调用两次。PushFunc 自身应当是线程安全的。
//
// 用途：把 Reflector 的增量事件转换为“每次变更后的完整快照”推送。内嵌 Store 负责
// 存储，UndeltaStore 只在其上叠加快照回调。
type UndeltaStore struct {
	Store
	// PushFunc 接收完整状态切片；实现必须并发安全，且不得长期持有该切片（Store.List
	// 每次返回新切片，但仍建议只读）。
	PushFunc func([]interface{})
}

// 断言 UndeltaStore 实现了 Store 接口。
var _ Store = &UndeltaStore{}

// Add 把对象插入存储并调用 PushFunc 推送完整状态。
// 关于线程安全：底层 Store 实现（cache.cache）的所有方法都持有锁。
// 在下面这些函数里，锁会在 {Add,Delete,等} 与 List 之间被释放并重新获取，
// 因此可能出现下面这种情况，导致两次完全相同的 PushFunc 调用。
// time            thread 1                  thread 2
// 0               UndeltaStore.Add(a)
// 1                                         UndeltaStore.Add(b)
// 2               Store.Add(a)
// 3                                         Store.Add(b)
// 4               Store.List() -> [a,b]
// 5                                         Store.List() -> [a,b]
//
// 因此 PushFunc 可能收到重复快照，消费端需要按快照内容自行去重或幂等处理。
func (u *UndeltaStore) Add(obj interface{}) error {
	if err := u.Store.Add(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Update 把缓存中条目的状态更新为最新值，并调用 PushFunc 推送完整状态。
func (u *UndeltaStore) Update(obj interface{}) error {
	if err := u.Store.Update(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Delete 从缓存中移除条目，并调用 PushFunc 推送完整状态。
func (u *UndeltaStore) Delete(obj interface{}) error {
	if err := u.Store.Delete(obj); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// Replace 会删除当前存储的内容，改用给定的列表。
// 调用此函数后 u 接管该列表的所有权，调用方不应再引用它。
// 替换完成后会调用 PushFunc 推送新内容的完整状态。
//
// 典型调用方是 Reflector 的全量 List 阶段。
func (u *UndeltaStore) Replace(list []interface{}, resourceVersion string) error {
	if err := u.Store.Replace(list, resourceVersion); err != nil {
		return err
	}
	u.PushFunc(u.Store.List())
	return nil
}

// NewUndeltaStore 返回一个基于 Store 实现的 UndeltaStore。
func NewUndeltaStore(pushFunc func([]interface{}), keyFunc KeyFunc) *UndeltaStore {
	return &UndeltaStore{
		Store:    NewStore(keyFunc),
		PushFunc: pushFunc,
	}
}
