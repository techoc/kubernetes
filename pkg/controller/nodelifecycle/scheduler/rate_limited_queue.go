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

package scheduler

import (
	"container/heap"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

const (
	// NodeHealthUpdateRetry 控制写入节点健康状态更新时的重试次数。
	NodeHealthUpdateRetry = 5
	// NodeEvictionPeriod 控制 NodeController 尝试从无响应节点驱逐 Pod 的频率。
	NodeEvictionPeriod = 100 * time.Millisecond
	// EvictionRateLimiterBurst 是所有驱逐速率限制器的突发值。
	EvictionRateLimiterBurst = 1
)

// TimedValue 表示应在指定时间处理的值。
type TimedValue struct {
	Value string
	// UID 可以是任何有助于识别该值的标识。
	UID       interface{}
	AddedAt   time.Time
	ProcessAt time.Time
}

// now 便于在测试中替换时间来源。
var now = time.Now

// TimedQueue 是一个优先堆，ProcessAt 最小的元素位于队首。
type TimedQueue []*TimedValue

// Len 返回队列长度。
func (h TimedQueue) Len() int { return len(h) }

// Less 在 queue[i] 的处理时间早于 queue[j] 时返回 true。
func (h TimedQueue) Less(i, j int) bool { return h[i].ProcessAt.Before(h[j].ProcessAt) }

// Swap 交换下标 i 与 j 处的元素。
func (h TimedQueue) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push 向队列添加一个 TimedValue。
func (h *TimedQueue) Push(x interface{}) {
	*h = append(*h, x.(*TimedValue))
}

// Pop 移除并返回 ProcessAt 最小的元素。
func (h *TimedQueue) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// UniqueQueue 是 FIFO 队列，额外保证任一元素被移除之前不会重复入队。
type UniqueQueue struct {
	lock  sync.Mutex
	queue TimedQueue
	set   sets.Set[string]
}

// Add 在值未被添加过、或已被 Remove 显式移除后，将其加入队列；返回是否真的新增。
func (q *UniqueQueue) Add(value TimedValue) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	// set 记录了队列中所有尚未被 Get/Remove 取出的值，避免重复入队。
	if q.set.Has(value.Value) {
		return false
	}
	heap.Push(&q.queue, &value)
	q.set.Insert(value.Value)
	return true
}

// Replace 在值已存在时替换队列中的元素，否则不做任何操作；返回是否找到并替换。
func (q *UniqueQueue) Replace(value TimedValue) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	for i := range q.queue {
		if q.queue[i].Value != value.Value {
			continue
		}
		heap.Remove(&q.queue, i)
		heap.Push(&q.queue, &value)
		return true
	}
	return false
}

// RemoveFromQueue 从队列中移除值，但保留在 set 中，因此不会被再次添加；返回是否有元素被移除。
func (q *UniqueQueue) RemoveFromQueue(value string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.set.Has(value) {
		return false
	}
	for i, val := range q.queue {
		if val.Value == value {
			heap.Remove(&q.queue, i)
			return true
		}
	}
	return false
}

// Remove 从队列中移除值，使 Get() 不再返回它，并允许该值后续再次添加。
// 值不存在时不做事并返回 false。
func (q *UniqueQueue) Remove(value string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	if !q.set.Has(value) {
		return false
	}
	q.set.Delete(value)
	for i, val := range q.queue {
		if val.Value == value {
			heap.Remove(&q.queue, i)
			return true
		}
	}
	return true
}

// Get 返回最早添加且尚未被取出的值。
func (q *UniqueQueue) Get() (TimedValue, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.queue) == 0 {
		return TimedValue{}, false
	}
	result := heap.Pop(&q.queue).(*TimedValue)
	q.set.Delete(result.Value)
	return *result, true
}

// Head 返回最早添加且尚未被取出的值，但不将其从队列移除。
func (q *UniqueQueue) Head() (TimedValue, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.queue) == 0 {
		return TimedValue{}, false
	}
	result := q.queue[0]
	return *result, true
}

// Clear 清空队列中的所有元素以及用于去重的 set。
func (q *UniqueQueue) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.queue.Len() > 0 {
		q.queue = make(TimedQueue, 0)
	}
	if len(q.set) > 0 {
		q.set = sets.New[string]()
	}
}

// RateLimitedTimedQueue 是按预期下次执行时间排序的唯一元素优先队列，并带有速率限制。
type RateLimitedTimedQueue struct {
	queue       UniqueQueue
	limiterLock sync.Mutex
	limiter     flowcontrol.RateLimiter
}

// NewRateLimitedTimedQueue 创建一个使用给定 RateLimiter 控制执行速率的队列。
func NewRateLimitedTimedQueue(limiter flowcontrol.RateLimiter) *RateLimitedTimedQueue {
	return &RateLimitedTimedQueue{
		queue: UniqueQueue{
			queue: TimedQueue{},
			set:   sets.New[string](),
		},
		limiter: limiter,
	}
}

// ActionFunc 处理一个 TimedValue：返回 false 表示该项需要重试，
// 同时可给出重试前的最小等待时间。
type ActionFunc func(TimedValue) (bool, time.Duration)

// Try 按速率限制处理队列：当 RateLimiter 拒绝动作时提前结束本轮，
// 否则将处理失败（fn 返回 false）的元素按等待时间重新入队。
// 每个值在 fn 返回 true 时只处理一次，否则按指定间隔重试。
// 除非显式调用 Remove（NodeController 在节点恢复 Ready 时通过 cancelPodEviction 完成），
// 同一个值不会被重复处理。TODO: 为已从集群移除的所有节点设计合理的垃圾回收机制。
func (q *RateLimitedTimedQueue) Try(logger klog.Logger, fn ActionFunc) {
	val, ok := q.queue.Head()
	q.limiterLock.Lock()
	defer q.limiterLock.Unlock()
	for ok {
		// rate limit the queue checking
		if !q.limiter.TryAccept() {
			logger.V(10).Info("Try rate limited", "value", val)
			// 速率受限，留到下一轮 Try 再处理。
			break
		}

		now := now()
		if now.Before(val.ProcessAt) {
			break
		}

		if ok, wait := fn(val); !ok {
			// 加 1ns 保证下次处理时间严格晚于当前时刻，避免同一轮无限重试。
			val.ProcessAt = now.Add(wait + 1)
			q.queue.Replace(val)
		} else {
			q.queue.RemoveFromQueue(val.Value)
		}
		val, ok = q.queue.Head()
	}
}

// Add 将值加入队列等待处理。若相同值（按值比较）已存在且未被移除，不会重复添加。
func (q *RateLimitedTimedQueue) Add(value string, uid interface{}) bool {
	now := now()
	return q.queue.Add(TimedValue{
		Value:     value,
		UID:       uid,
		AddedAt:   now,
		ProcessAt: now,
	})
}

// Remove 从驱逐器中移除节点，在再次添加之前不会处理该节点。
func (q *RateLimitedTimedQueue) Remove(value string) bool {
	return q.queue.Remove(value)
}

// Clear 清空队列中的所有元素。
func (q *RateLimitedTimedQueue) Clear() {
	q.queue.Clear()
}

// SwapLimiter 在 QPS 变化时安全地替换当前速率限制器。
func (q *RateLimitedTimedQueue) SwapLimiter(newQPS float32) {
	q.limiterLock.Lock()
	defer q.limiterLock.Unlock()
	if q.limiter.QPS() == newQPS {
		return
	}
	var newLimiter flowcontrol.RateLimiter
	if newQPS <= 0 {
		newLimiter = flowcontrol.NewFakeNeverRateLimiter()
	} else {
		newLimiter = flowcontrol.NewTokenBucketRateLimiter(newQPS, EvictionRateLimiterBurst)

		// 若当前限制器处于等待状态，则消耗掉新限制器的一个令牌，使 Burst 为 1 时行为平滑。
		// TODO: 确定是否需要支持更高的 Burst 值，并据此设计令牌转移逻辑：保留
		// - 饱和度（已用令牌百分比）
		// - 已用令牌数
		// - 可用令牌数
		// - 其他指标
		if q.limiter.TryAccept() == false {
			newLimiter.TryAccept()
		}
	}
	q.limiter.Stop()
	q.limiter = newLimiter
}
