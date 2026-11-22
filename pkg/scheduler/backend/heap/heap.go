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

// 以下是泛型堆的实现。堆本身不做同步，同步职责交给调用方 SchedulingQueue，
// 这样堆操作保持简单，也避免在热点路径上重复加锁。

// Package heap 实现调度队列使用的泛型堆：按 LessFunc 排序，并按 KeyFunc 维护键到位置的索引以支持 O(1) 查找与删除。
package heap

import (
	"container/heap"
	"fmt"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// Item 是堆中元素必须实现的接口，提供指标采集所需的信息。
type Item interface {
	// Size 返回元素大小（如 Pod 数），用于指标统计。
	Size() int
	// Type 返回元素类型（pod 或 podgroup），用于区分指标维度。
	Type() fwk.EntityKeyType
}

// KeyFunc 从对象提取唯一键，用于元素的身份识别与查找。
type KeyFunc[T Item] func(obj T) string

// heapItem 是堆内部存储条目，同时保存对象和它的键。
type heapItem[T Item] struct {
	// obj 是实际存储的对象。
	obj T
	// key 是对象的唯一键，用于查找与删除。
	key string
}

// data 实现标准库 heap.Interface，并维护键到队列下标的索引，保证查找/删除为 O(1)。
type data[T Item] struct {
	// queue 是按堆序排列的元素指针切片。
	queue []*heapItem[T]
	// keyIndex 记录每个键在 queue 中的下标，避免按值线性查找。
	keyIndex map[string]int

	// keyFunc 生成入队/出队使用的键，必须确定性：同一对象每次都应得到相同键，
	// 否则索引会与队列内容失配。
	keyFunc KeyFunc[T]
	// lessFunc 定义堆中两个对象的优先级比较。
	lessFunc LessFunc[T]
}

var (
	_ = heap.Interface(&data[Item]{}) // 编译时断言 data 实现了标准 heap 接口
)

// Less 比较下标 i、j 处的对象，第一个应排在前面时返回 true。
func (h *data[T]) Less(i, j int) bool {
	return h.lessFunc(h.queue[i].obj, h.queue[j].obj)
}

// Len 返回堆中元素数量。
func (h *data[T]) Len() int { return len(h.queue) }

// Swap 交换两个元素，属于标准 heap 接口的一部分，不应直接调用。
// 交换后同步更新 keyIndex，否则后续查找会拿到错误下标。
func (h *data[T]) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	h.keyIndex[h.queue[i].key] = i
	h.keyIndex[h.queue[j].key] = j
}

// Push 向堆尾追加元素，只应由 container/heap.Push 调用。
func (h *data[T]) Push(x interface{}) {
	item := x.(*heapItem[T])
	h.keyIndex[item.key] = len(h.queue)
	h.queue = append(h.queue, item)
}

// Pop 移除堆尾元素，只应由 container/heap.Pop 调用。
func (h *data[T]) Pop() interface{} {
	n := len(h.queue)
	if n == 0 {
		return nil
	}
	item := h.queue[n-1]
	h.queue[n-1] = nil // 切断引用，避免被移除的对象因切片底层数组而滞留内存。
	h.queue = h.queue[:n-1]
	delete(h.keyIndex, item.key)
	return item.obj
}

// Peek 返回堆顶元素但不移除；堆为空时返回零值和 false。
func (h *data[T]) Peek() (T, bool) {
	if len(h.queue) > 0 {
		return h.queue[0].obj, true
	}
	return *new(T), false
}

// Heap 是基于堆结构的生产者/消费者队列，可用于实现优先队列等数据结构。
type Heap[T Item] struct {
	// data 保存对象，并以堆序维护它们的顺序。
	data *data[T]
	// metricRecorder 在元素增删时更新计数；为 nil 时不记录指标。
	metricRecorder metrics.MetricRecorder
}

// AddOrUpdate 插入对象；若相同键的对象已存在则更新它并按新的优先级重新调整堆。
func (h *Heap[T]) AddOrUpdate(obj T) {
	key := h.data.keyFunc(obj)
	if idx, exists := h.data.keyIndex[key]; exists {
		if h.metricRecorder != nil {
			// 新对象大小可能变化，需要用旧对象与新对象的差值更新指标。
			h.metricRecorder.Update(h.data.queue[idx].obj, obj)
		}
		h.data.queue[idx].obj = obj
		heap.Fix(h.data, idx)
	} else {
		heap.Push(h.data, &heapItem[T]{obj: obj, key: key})
		if h.metricRecorder != nil {
			h.metricRecorder.Add(obj)
		}
	}
}

// Delete 删除与 obj 同键的元素并返回被删除的对象；不存在时返回零值。
func (h *Heap[T]) Delete(obj T) T {
	key := h.data.keyFunc(obj)
	if idx, ok := h.data.keyIndex[key]; ok {
		removed := heap.Remove(h.data, idx).(T)
		if h.metricRecorder != nil {
			// 用堆中实际被删的对象记录指标，传入的 obj 可能只是查找用的壳，大小不准。
			h.metricRecorder.Remove(removed)
		}
		return removed
	}
	var zero T
	return zero
}

// Peek 返回堆顶元素但不移除；堆为空时返回零值和 false。
func (h *Heap[T]) Peek() (T, bool) {
	return h.data.Peek()
}

// Pop 返回并移除堆顶元素；堆为空时返回错误。
func (h *Heap[T]) Pop() (T, error) {
	if h.data.Len() == 0 {
		return *new(T), fmt.Errorf("heap is empty")
	}
	obj := heap.Pop(h.data)
	typedObj := obj.(T)
	if h.metricRecorder != nil {
		h.metricRecorder.Remove(typedObj)
	}
	return typedObj, nil
}

// Get 按键查找对象；未找到时返回零值和 false。
func (h *Heap[T]) Get(obj T) (T, bool) {
	key := h.data.keyFunc(obj)
	return h.GetByKey(key)
}

// GetByKey 按键直接查找对象；未找到时返回零值和 false。
func (h *Heap[T]) GetByKey(key string) (T, bool) {
	idx, exists := h.data.keyIndex[key]
	if !exists {
		return *new(T), false
	}
	return h.data.queue[idx].obj, true
}

// Has 判断给定键的对象是否在堆中。
func (h *Heap[T]) Has(obj T) bool {
	key := h.data.keyFunc(obj)
	_, ok := h.data.keyIndex[key]
	return ok
}

// List 返回堆中所有元素的快照，顺序为堆内部顺序，并非有序序列。
func (h *Heap[T]) List() []T {
	list := make([]T, 0, len(h.data.queue))
	for _, item := range h.data.queue {
		list = append(list, item.obj)
	}
	return list
}

// Len 返回堆中元素数量。
func (h *Heap[T]) Len() int {
	return len(h.data.queue)
}

// New 创建一个不记录指标的堆，用于按优先级排队处理元素。
func New[T Item](keyFn KeyFunc[T], lessFn LessFunc[T]) *Heap[T] {
	return NewWithRecorder(keyFn, lessFn, nil)
}

// NewWithRecorder 创建一个带指标记录器的堆；metricRecorder 为 nil 时不记录。
func NewWithRecorder[T Item](keyFn KeyFunc[T], lessFn LessFunc[T], metricRecorder metrics.MetricRecorder) *Heap[T] {
	return &Heap[T]{
		data: &data[T]{
			queue:    []*heapItem[T]{},
			keyIndex: map[string]int{},
			keyFunc:  keyFn,
			lessFunc: lessFn,
		},
		metricRecorder: metricRecorder,
	}
}

// LessFunc 比较两个元素，排序时第一个应排在第二个前面则返回 true。
type LessFunc[T Item] func(item1, item2 T) bool
