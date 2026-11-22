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

// removeFromFIFOFunc removes a designated element from the list
// if that element is in the list.
// The complexity of the runtime cost is O(1).
// The returned value is the element removed, if indeed one was removed,
// otherwise `nil`.
//
// 幂等：重复调用只会成功一次，返回 nil 表示该请求已被别的路径移除。
type removeFromFIFOFunc func() *request

// walkFunc is called for each request in the list in the
// oldest -> newest order.
// ok: if walkFunc returns false then the iteration stops immediately.
// walkFunc may remove the given request from the fifo,
// but may not mutate the fifo in any othe way.
//
// 只允许删除当前元素，是因为 Walk 依赖元素链表指针继续遍历，插入或重排会使遍历失效。
type walkFunc func(*request) (ok bool)

// Internal interface to abstract out the implementation details
// of the underlying list used to maintain the requests.
//
// Note that a fifo, including the removeFromFIFOFuncs returned from Enqueue,
// is not safe for concurrent use by multiple goroutines.
//
// 抽象成接口是为了便于在测试中替换实现；生产实现是下面的 requestFIFO。
type fifo interface {
	// Enqueue enqueues the specified request into the list and
	// returns a removeFromFIFOFunc function that can be used to remove the
	// request from the list
	Enqueue(*request) removeFromFIFOFunc

	// Dequeue pulls out the oldest request from the list.
	Dequeue() (*request, bool)

	// Peek returns the oldest request without removing it.
	Peek() (*request, bool)

	// Length returns the number of requests in the list.
	Length() int

	// QueueSum returns the sum of initial seats, final seats, and
	// additional latency aggregated from all requests in this queue.
	QueueSum() queueSum

	// Walk iterates through the list in order of oldest -> newest
	// and executes the specified walkFunc for each request in that order.
	//
	// if the specified walkFunc returns false the Walk function
	// stops the walk an returns immediately.
	Walk(walkFunc)
}

// the FIFO list implementation is not safe for concurrent use by multiple
// goroutines.
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
