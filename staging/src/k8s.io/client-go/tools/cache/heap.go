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

// 本文件实现堆（heap）数据结构。

package cache

import (
	"container/heap"
	"fmt"
	"sync"
)

const (
	closedMsg = "heap is closed"
)

// LessFunc 用于比较堆中的两个对象。
type LessFunc func(interface{}, interface{}) bool

// heapItem 是堆中一个对象的包装，index 记录其 key 在 queue 中的位置。
//
// index 由 Swap/Push/Pop 维护，供 heap.Fix/Remove 直接定位元素，避免线性查找。
type heapItem struct {
	obj   interface{} // 存储在堆中的对象。
	index int         // 对象 key 在 Heap.queue 中的下标。
}

// itemKeyValue 是传给 heap.Push 的载荷。
type itemKeyValue struct {
	key string
	obj interface{}
}

// heapData 是实现标准 heap 接口的内部结构，保存堆中的数据。
//
// items 与 queue 必须始终互为镜像——queue 中每个 key 在 items 中都有对应项，
// 反之亦然。任何破坏该不变量的操作都会使 heap.Fix/Remove 定位错误。
type heapData struct {
	// items 是从对象 key 到对象及其下标的 map。
	// 依赖不变量：map 中的条目与 queue 中的条目一一对应。
	items map[string]*heapItem
	// queue 实现堆数据结构，按堆不变量维持元素顺序，
	// 保存的是 "items" 中对象的 key。
	queue []string

	// keyFunc 生成入队/出队使用的 key，必须是确定性的。
	keyFunc KeyFunc
	// lessFunc 用于比较堆中的两个对象。
	lessFunc LessFunc
}

var (
	_ = heap.Interface(&heapData{}) // heapData 是标准的堆
)

// Less 比较两个对象，若第一个应排在第二个之前则返回 true。
//
// 遇到缺失的索引或 item 时保守返回 false，保证比较不 panic（标准库不会传入越界索引）。
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

// Len 返回 Heap 中的条目数。
func (h *heapData) Len() int { return len(h.queue) }

// Swap 实现堆中两个元素的交换。这是标准 heap 接口的一部分，不应直接调用。
//
// 交换 queue 后必须同步更新两个 heapItem.index，否则 Fix/Remove 会定位到错误元素。
func (h *heapData) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

// Push 只应由 heap.Push 调用。
func (h *heapData) Push(kv interface{}) {
	keyValue := kv.(*itemKeyValue)
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

// Pop 只应由 heap.Pop 调用。
//
// 标准 heap.Pop 会先把根与末尾交换再调用本方法，因此这里直接移除末尾元素。
func (h *heapData) Pop() interface{} {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[0 : len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		// 走到这里说明出了错误
		return nil
	}
	delete(h.items, key)
	return item.obj
}

// Heap 是实现堆数据结构的线程安全生产者/消费者队列，
// 可用于实现优先级队列等数据结构。
//
// Pop 在队列为空时阻塞，由 Add/BulkAdd 的 Broadcast 唤醒，由 Close 唤醒并
// 返回错误。所有公开方法均可在并发下安全调用。
type Heap struct {
	lock sync.RWMutex
	cond sync.Cond

	// data 保存对象，并用 queue 按堆不变量维持它们的顺序。
	data *heapData

	// closed 表示队列已关闭。
	// 主要用于让 Pop() 在等待条目时退出控制循环。
	closed bool
}

// Close 关闭 Heap，并通知可能正在等待从堆中取元素的条件变量。
//
// 只置位并唤醒等待者，不丢弃已有元素；关闭后 Add 等写操作返回错误。
func (h *Heap) Close() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.closed = true
	h.cond.Broadcast()
}

// Add 插入条目并放入队列；若条目已存在则更新它。
//
// key 已存在时替换对象并调用 heap.Fix 重新调整位置，而不是重新插入，
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

// BulkAdd 把列表中的全部条目加入队列，然后通知条件变量。
// 适合调用方希望在消费者开始处理前先把所有条目放入队列的场景。
//
// 整个批次在同一临界区内完成，最后只 Broadcast 一次，避免消费者在批次
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

// AddIfNotPresent 插入条目并放入队列；若 map 中已有相同 key 的条目则不做修改。
//
// 适用于单生产者/单消费者场景：消费者可以安全地重试条目，而不会与生产者竞争
// 并可能把过期条目重新入队。
//
// 消费者处理失败后可用它重试入队，若生产者已放入更新版本则保留新版，
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

// addIfNotPresentLocked 假定调用方已持有锁，在条目不存在时把它加入队列。
func (h *Heap) addIfNotPresentLocked(key string, obj interface{}) {
	if _, exists := h.data.items[key]; exists {
		return
	}
	heap.Push(h.data, &itemKeyValue{key, obj})
}

// 在该实现中 Update 与 Add 相同：条目不存在时会被添加。
//
// 仅为满足 Queue 接口而保留的别名。
func (h *Heap) Update(obj interface{}) error {
	return h.Add(obj)
}

// Delete 移除一个条目。
//
// 通过 heapItem.index 直接定位并调用 heap.Remove，元素不存在时返回错误以便调用方感知。
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

// Pop 等待直到有条目可用。若多个条目同时可用，按 Heap.data.lessFunc 定义的顺序返回。
//
// 等待在 cond 上进行，期间持有写锁；Close 会广播唤醒并让本方法返回错误，
// 因此调用方可以安全地阻塞在 Pop 上。
func (h *Heap) Pop() (interface{}, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for len(h.data.queue) == 0 {
		// 队列为空时，Pop() 会阻塞直到有新条目入队。
		// 调用 Close() 后 h.closed 被置位且条件被广播，
		// 使该循环继续执行并从 Pop() 返回。
		if h.closed {
			return nil, fmt.Errorf("heap is closed")
		}
		h.cond.Wait()
	}
	obj := heap.Pop(h.data)
	if obj == nil {
		// queue 非空却取不到对象，说明 items/queue 不变量已被破坏。
		return nil, fmt.Errorf("object was removed from heap data")
	}

	return obj, nil
}

// List 返回全部条目的列表。
//
// 直接遍历 map，返回顺序不确定；需要顺序时请使用 Pop 或自行排序。
func (h *Heap) List() []interface{} {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]interface{}, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

// ListKeys 返回当前 Heap 中所有对象的 key。
//
// 顺序不确定，仅用于成员检查或调试。
func (h *Heap) ListKeys() []string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]string, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}
	return list
}

// Get 返回请求的条目；不存在时 exists=false。
//
// 先由 keyFunc 求 key，再走 GetByKey；keyFunc 报错时以 KeyError 返回。
func (h *Heap) Get(obj interface{}) (interface{}, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return h.GetByKey(key)
}

// GetByKey 返回请求的条目；不存在时 exists=false。
//
// 只读锁，允许多个读者并发查询。
func (h *Heap) GetByKey(key string) (interface{}, bool, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	item, exists := h.data.items[key]
	if !exists {
		return nil, false, nil
	}
	return item.obj, true, nil
}

// IsClosed 返回队列是否已关闭。
func (h *Heap) IsClosed() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.closed
}

// NewHeap 返回一个可用于排队待处理条目的 Heap。
//
// cond.L 必须指向堆自身的锁，否则 Wait/Broadcast 的同步语义不正确。
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
