/*
Copyright 2017 The Kubernetes Authors.

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

// This file implements a heap data structure.

package cache

import (
	"container/heap"
	"fmt"
	"sync"
)

const (
	closedMsg = "heap is closed"
)

// LessFunc is used to compare two objects in the heap.
type LessFunc func(interface{}, interface{}) bool

// heapItem 是堆中一个对象的包装，index 记录其 key 在 queue 中的位置。
//
// 中文：index 由 Swap/Push/Pop 维护，供 heap.Fix/Remove 直接定位元素，避免线性查找。
type heapItem struct {
	obj   interface{} // The object which is stored in the heap.
	index int         // The index of the object's key in the Heap.queue.
}

// itemKeyValue 是传给 heap.Push 的载荷。
type itemKeyValue struct {
	key string
	obj interface{}
}

// heapData is an internal struct that implements the standard heap interface
// and keeps the data stored in the heap.
//
// 中文说明：items 与 queue 必须始终互为镜像——queue 中每个 key 在 items 有对应项，
// 反之亦然。任何破坏该不变量的操作都会使 heap.Fix/Remove 定位错误。
type heapData struct {
	// items is a map from key of the objects to the objects and their index.
	// We depend on the property that items in the map are in the queue and vice versa.
	items map[string]*heapItem
	// queue implements a heap data structure and keeps the order of elements
	// according to the heap invariant. The queue keeps the keys of objects stored
	// in "items".
	queue []string

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc
	// lessFunc is used to compare two objects in the heap.
	lessFunc LessFunc
}

var (
	_ = heap.Interface(&heapData{}) // heapData is a standard heap
)

// Less compares two objects and returns true if the first one should go
// in front of the second one in the heap.
//
// 中文：遇到缺失的索引或 item 时保守返回 false，保证比较不 panic（标准库不会传入越界索引）。
func (h *heapData) Less(i, j int) bool {
	if i > len(h.queue) || j > len(h.queue) {
		return false
	}
	itemi, ok := h.items[h.queue[i]]
	if !ok {
		return false
	}
	itemj, ok := h.items[h.queue[j]]
	if !ok {
		return false
	}
	return h.lessFunc(itemi.obj, itemj.obj)
}

// Len returns the number of items in the Heap.
func (h *heapData) Len() int { return len(h.queue) }

// Swap implements swapping of two elements in the heap. This is a part of standard
// heap interface and should never be called directly.
//
// 中文：交换 queue 后必须同步更新两个 heapItem.index，否则 Fix/Remove 会定位到错误元素。
func (h *heapData) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

// Push is supposed to be called by heap.Push only.
func (h *heapData) Push(kv interface{}) {
	keyValue := kv.(*itemKeyValue)
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

// Pop is supposed to be called by heap.Pop only.
//
// 中文：标准 heap.Pop 会先把根与末尾交换再调用本方法，因此这里直接移除末尾元素。
func (h *heapData) Pop() interface{} {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[0 : len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		// This is an error
		return nil
	}
	delete(h.items, key)
	return item.obj
}

// Heap is a thread-safe producer/consumer queue that implements a heap data structure.
// It can be used to implement priority queues and similar data structures.
//
// 中文说明：Pop 在队列为空时阻塞，由 Add/BulkAdd 的 Broadcast 唤醒，由 Close 唤醒并
// 返回错误。所有公开方法均可在并发下安全调用。
type Heap struct {
	lock sync.RWMutex
	cond sync.Cond

	// data stores objects and has a queue that keeps their ordering according
	// to the heap invariant.
	data *heapData

	// closed indicates that the queue is closed.
	// It is mainly used to let Pop() exit its control loop while waiting for an item.
	closed bool
}

// Close the Heap and signals condition variables that may be waiting to pop
// items from the heap.
//
// 中文：只置位并唤醒等待者，不丢弃已有元素；关闭后 Add 等写操作返回错误。
func (h *Heap) Close() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.closed = true
	h.cond.Broadcast()
}

// Add inserts an item, and puts it in the queue. The item is updated if it
// already exists.
//
// 中文说明：key 已存在时替换对象并调用 heap.Fix 重新调整位置，而不是重新插入，
// 从而保证同一 key 在堆中至多出现一次（例如只保留每个资源的最新变更）。
func (h *Heap) Add(obj interface{}) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return fmt.Errorf(closedMsg)
	}
	if _, exists := h.data.items[key]; exists {
		h.data.items[key].obj = obj
		heap.Fix(h.data, h.data.items[key].index)
	} else {
		h.addIfNotPresentLocked(key, obj)
	}
	h.cond.Broadcast()
	return nil
}

// BulkAdd adds all the items in the list to the queue and then signals the condition
// variable. It is useful when the caller would like to add all of the items
// to the queue before consumer starts processing them.
//
// 中文说明：整个批次在同一临界区内完成，最后只 Broadcast 一次，避免消费者在批次
// 中途被唤醒而看到不完整的集合。
func (h *Heap) BulkAdd(list []interface{}) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return fmt.Errorf(closedMsg)
	}
	for _, obj := range list {
		key, err := h.data.keyFunc(obj)
		if err != nil {
			return KeyError{obj, err}
		}
		if _, exists := h.data.items[key]; exists {
			h.data.items[key].obj = obj
			heap.Fix(h.data, h.data.items[key].index)
		} else {
			h.addIfNotPresentLocked(key, obj)
		}
	}
	h.cond.Broadcast()
	return nil
}

// AddIfNotPresent inserts an item, and puts it in the queue. If an item with
// the key is present in the map, no changes is made to the item.
//
// This is useful in a single producer/consumer scenario so that the consumer can
// safely retry items without contending with the producer and potentially enqueueing
// stale items.
//
// 中文说明：消费者处理失败后可用它重试入队，若生产者已放入更新版本则保留新版，
// 避免旧对象覆盖新对象。
func (h *Heap) AddIfNotPresent(obj interface{}) error {
	id, err := h.data.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return fmt.Errorf(closedMsg)
	}
	h.addIfNotPresentLocked(id, obj)
	h.cond.Broadcast()
	return nil
}

// addIfNotPresentLocked assumes the lock is already held and adds the provided
// item to the queue if it does not already exist.
func (h *Heap) addIfNotPresentLocked(key string, obj interface{}) {
	if _, exists := h.data.items[key]; exists {
		return
	}
	heap.Push(h.data, &itemKeyValue{key, obj})
}

// Update is the same as Add in this implementation. When the item does not
// exist, it is added.
//
// 中文：仅为满足 Queue 接口而保留的别名。
func (h *Heap) Update(obj interface{}) error {
	return h.Add(obj)
}

// Delete removes an item.
//
// 中文：通过 heapItem.index 直接定位并 heap.Remove，元素不存在时返回错误以便调用方感知。
func (h *Heap) Delete(obj interface{}) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if item, ok := h.data.items[key]; ok {
		heap.Remove(h.data, item.index)
		return nil
	}
	return fmt.Errorf("object not found")
}

// Pop waits until an item is ready. If multiple items are
// ready, they are returned in the order given by Heap.data.lessFunc.
//
// 中文说明：等待在 cond 上进行，期间持有写锁；Close 会广播唤醒并让本方法返回错误，
// 因此调用方可以安全地阻塞在 Pop 上。
func (h *Heap) Pop() (interface{}, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for len(h.data.queue) == 0 {
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
		// When Close() is called, the h.closed is set and the condition is broadcast,
		// which causes this loop to continue and return from the Pop().
		if h.closed {
			return nil, fmt.Errorf("heap is closed")
		}
		h.cond.Wait()
	}
	obj := heap.Pop(h.data)
	if obj == nil {
		// 中文：queue 非空却取不到对象，说明 items/queue 不变量已被破坏。
		return nil, fmt.Errorf("object was removed from heap data")
	}

	return obj, nil
}

// List returns a list of all the items.
//
// 中文：直接遍历 map，返回顺序不确定；需要顺序时请使用 Pop 或自行排序。
func (h *Heap) List() []interface{} {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]interface{}, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently in the Heap.
//
// 中文：顺序不确定，仅用于成员检查或调试。
func (h *Heap) ListKeys() []string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]string, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}
	return list
}

// Get returns the requested item, or sets exists=false.
//
// 中文：先由 keyFunc 求 key，再走 GetByKey；keyFunc 报错时以 KeyError 返回。
func (h *Heap) Get(obj interface{}) (interface{}, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return h.GetByKey(key)
}

// GetByKey returns the requested item, or sets exists=false.
//
// 中文：只读锁，允许多个读者并发查询。
func (h *Heap) GetByKey(key string) (interface{}, bool, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	item, exists := h.data.items[key]
	if !exists {
		return nil, false, nil
	}
	return item.obj, true, nil
}

// IsClosed returns true if the queue is closed.
func (h *Heap) IsClosed() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.closed
}

// NewHeap returns a Heap which can be used to queue up items to process.
//
// 中文：cond.L 必须指向堆自身的锁，否则 Wait/Broadcast 的同步语义不正确。
func NewHeap(keyFn KeyFunc, lessFn LessFunc) *Heap {
	h := &Heap{
		data: &heapData{
			items:    map[string]*heapItem{},
			queue:    []string{},
			keyFunc:  keyFn,
			lessFunc: lessFn,
		},
	}
	h.cond.L = &h.lock
	return h
}
