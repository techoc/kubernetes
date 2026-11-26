/*
Copyright 2021 The Kubernetes Authors.

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

package queueset

import (
	"container/list"
)

// removeFromFIFOFunc 从列表中移除指定元素（若存在）。
//
// 运行时复杂度为 O(1)。
// 返回被移除的元素（若确有移除），
// 否则为 `nil`。
//
// 幂等：重复调用只会成功一次，返回 nil 表示该请求已被别的路径移除。
type removeFromFIFOFunc func() *request

// walkFunc 按最旧 -> 最新的顺序
// 对列表中每个请求调用。
// ok：walkFunc 返回 false 时立即停止迭代。
// walkFunc 可以从 fifo 中移除给定请求，
// 但不得以任何其他方式修改 fifo。
//
// 只允许删除当前元素，是因为 Walk 依赖元素链表指针继续遍历，插入或重排会使遍历失效。
type walkFunc func(*request) (ok bool)

// 内部接口，用于抽象
// 维护请求的底层列表的实现细节。
//
// 注意：fifo（包括 Enqueue 返回的 removeFromFIFOFunc）
// 不支持多个 goroutine 并发使用。
//
// 抽象成接口是为了便于在测试中替换实现；生产实现是下面的 requestFIFO。
type fifo interface {
	// Enqueue 把指定请求入队，并返回一个 removeFromFIFOFunc
	// 函数，可用于把该请求从列表中移除。
	//
	Enqueue(*request) removeFromFIFOFunc

	// Dequeue 取出列表中最旧的请求。
	Dequeue() (*request, bool)

	// Peek 返回最旧的请求但不移除它。
	Peek() (*request, bool)

	// Length 返回列表中的请求数。
	Length() int

	// QueueSum 返回本队列所有请求聚合的
	// 初始 seats、最终 seats 与附加延迟之和。
	QueueSum() queueSum

	// Walk 按最旧 -> 最新的顺序遍历列表，
	// 并按该顺序对每个请求执行指定的 walkFunc。
	//
	// 若指定的 walkFunc 返回 false，Walk 函数
	// 立即停止遍历并返回。
	Walk(walkFunc)
}

// FIFO 链表实现不支持多个 goroutine 并发使用。
// goroutine 并发使用。
//
// 内嵌 container/list 以复用 O(1) 的删除；sum 是增量维护的聚合值，
// 避免每次调度都遍历整条队列（队列可能很长）。
type requestFIFO struct {
	*list.List

	sum queueSum
}

func newRequestFIFO() fifo {
	return &requestFIFO{
		List: list.New(),
	}
}

func (l *requestFIFO) Length() int {
	return l.Len()
}

func (l *requestFIFO) QueueSum() queueSum {
	return l.sum
}

func (l *requestFIFO) Enqueue(req *request) removeFromFIFOFunc {
	e := l.PushBack(req)
	addToQueueSum(&l.sum, req)

	// 把元素置空而不是仅 Remove，防止闭包被多次调用时重复扣减聚合值。
	return func() *request {
		if e.Value == nil {
			return nil
		}
		l.Remove(e)
		e.Value = nil
		deductFromQueueSum(&l.sum, req)
		return req
	}
}

func (l *requestFIFO) Dequeue() (*request, bool) {
	return l.getFirst(true)
}

func (l *requestFIFO) Peek() (*request, bool) {
	return l.getFirst(false)
}

// getFirst 实现 Dequeue 与 Peek 的共用逻辑；只在成功取出时扣减聚合值。
func (l *requestFIFO) getFirst(remove bool) (*request, bool) {
	e := l.Front()
	if e == nil {
		return nil, false
	}

	if remove {
		defer func() {
			l.Remove(e)
			e.Value = nil
		}()
	}

	request, ok := e.Value.(*request)
	if remove && ok {
		deductFromQueueSum(&l.sum, request)
	}
	return request, ok
}

func (l *requestFIFO) Walk(f walkFunc) {
	var next *list.Element
	for current := l.Front(); current != nil; current = next {
		next = current.Next() // f is allowed to remove current
		if r, ok := current.Value.(*request); ok {
			if !f(r) {
				return
			}
		}
	}
}

func addToQueueSum(sum *queueSum, req *request) {
	sum.InitialSeatsSum += req.InitialSeats()
	sum.MaxSeatsSum += req.MaxSeats()
	sum.TotalWorkSum += req.totalWork()
}

// deductFromQueueSum 必须与 addToQueueSum 使用的取值函数完全一致，
// 否则请求进出队列后聚合值会漂移。
func deductFromQueueSum(sum *queueSum, req *request) {
	sum.InitialSeatsSum -= req.InitialSeats()
	sum.MaxSeatsSum -= req.MaxSeats()
	sum.TotalWorkSum -= req.totalWork()
}
