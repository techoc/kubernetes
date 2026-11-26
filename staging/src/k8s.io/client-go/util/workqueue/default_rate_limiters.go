/*
Copyright 2016 The Kubernetes Authors.

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

package workqueue

import (
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Deprecated: RateLimiter is deprecated, use TypedRateLimiter instead.
//
// 非泛型版本等价于 TypedRateLimiter[any]，仅用于兼容旧调用方。
type RateLimiter TypedRateLimiter[any]

// TypedRateLimiter 决定一个条目应等待多久再重试。
//
// 实现需要按条目记录失败次数，因此必须并发安全；Forget 是释放记录的
// 唯一入口，长期不调用会导致内存持续增长。
type TypedRateLimiter[T comparable] interface {
	// When 接收条目并决定它应等待多久。
	When(item T) time.Duration
	// Forget 表示条目已结束重试（无论成功或失败），此后不再追踪。
	Forget(item T)
	// NumRequeues 返回该条目累计的失败次数。
	NumRequeues(item T) int
}

// DefaultControllerRateLimiter 是工作队列默认限流器的无参构造函数，同时提供整体（令牌桶）与条目级（指数退避）限流。
//
// Deprecated: Use DefaultTypedControllerRateLimiter instead.
func DefaultControllerRateLimiter() RateLimiter {
	return DefaultTypedControllerRateLimiter[any]()
}

// DefaultTypedControllerRateLimiter 是工作队列默认限流器的无参构造函数，同时提供整体（令牌桶）与条目级（指数退避）限流。
func DefaultTypedControllerRateLimiter[T comparable]() TypedRateLimiter[T] {
	return NewTypedMaxOfRateLimiter(
		NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second),
		// 10 QPS、桶容量 100：只控制整体重试速度，不针对单个条目。
		&TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// Deprecated: BucketRateLimiter is deprecated, use TypedBucketRateLimiter instead.
type BucketRateLimiter = TypedBucketRateLimiter[any]

// TypedBucketRateLimiter 把标准令牌桶适配为工作队列的限流器接口。
//
// 令牌桶只限制整体重试速率，不跟踪单个条目；因此 NumRequeues 恒为 0，
// 需要条目级退避时应与指数退避限流器组合使用。
type TypedBucketRateLimiter[T comparable] struct {
	*rate.Limiter
}

var _ RateLimiter = &BucketRateLimiter{}

// When 预留一个令牌并返回需要等待的时间；桶空时延迟会自然增长。
func (r *TypedBucketRateLimiter[T]) When(item T) time.Duration {
	return r.Limiter.Reserve().Delay()
}

// NumRequeues 对令牌桶没有意义，恒返回 0。
func (r *TypedBucketRateLimiter[T]) NumRequeues(item T) int {
	return 0
}

// Forget 是空操作：令牌桶不按条目记录状态。
func (r *TypedBucketRateLimiter[T]) Forget(item T) {
}

// Deprecated: ItemExponentialFailureRateLimiter is deprecated, use TypedItemExponentialFailureRateLimiter instead.
type ItemExponentialFailureRateLimiter = TypedItemExponentialFailureRateLimiter[any]

// TypedItemExponentialFailureRateLimiter 按 baseDelay*2^失败次数 计算延迟，失败次数上限与过期清理由调用方负责。
type TypedItemExponentialFailureRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]int

	baseDelay time.Duration
	maxDelay  time.Duration
}

var _ RateLimiter = &ItemExponentialFailureRateLimiter{}

// Deprecated: NewItemExponentialFailureRateLimiter is deprecated, use NewTypedItemExponentialFailureRateLimiter instead.
func NewItemExponentialFailureRateLimiter(baseDelay time.Duration, maxDelay time.Duration) RateLimiter {
	return NewTypedItemExponentialFailureRateLimiter[any](baseDelay, maxDelay)
}

// NewTypedItemExponentialFailureRateLimiter 构造指数退避限流器：
// 第 n 次失败等待 baseDelay * 2^n，封顶 maxDelay。
func NewTypedItemExponentialFailureRateLimiter[T comparable](baseDelay time.Duration, maxDelay time.Duration) TypedRateLimiter[T] {
	return &TypedItemExponentialFailureRateLimiter[T]{
		failures:  map[T]int{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

// Deprecated: DefaultItemBasedRateLimiter is deprecated, use DefaultTypedItemBasedRateLimiter instead.
func DefaultItemBasedRateLimiter() RateLimiter {
	return DefaultTypedItemBasedRateLimiter[any]()
}

// DefaultTypedItemBasedRateLimiter 返回毫秒起步、封顶约 16 分钟的纯条目级退避。
func DefaultTypedItemBasedRateLimiter[T comparable]() TypedRateLimiter[T] {
	return NewTypedItemExponentialFailureRateLimiter[T](time.Millisecond, 1000*time.Second)
}

func (r *TypedItemExponentialFailureRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	// 先取当前失败次数用于计算，再把计数加一：首次失败按 2^0 退避。
	exp := r.failures[item]
	r.failures[item] = r.failures[item] + 1

	// 退避值设有上限，避免计算结果溢出。
	// 用浮点计算后再判断溢出，避免整数移位在极端次数下回绕成负值。
	backoff := float64(r.baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
	if backoff > math.MaxInt64 {
		return r.maxDelay
	}

	calculated := time.Duration(backoff)
	if calculated > r.maxDelay {
		return r.maxDelay
	}

	return calculated
}

// NumRequeues 返回该条目累计的失败次数（含尚未 Forget 的历史）。
func (r *TypedItemExponentialFailureRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

// Forget 清除条目的失败记录，使下次 When 重新从 baseDelay 起算。
func (r *TypedItemExponentialFailureRateLimiter[T]) Forget(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// ItemFastSlowRateLimiter 前若干次快速重试，之后转为慢速重试。
// Deprecated: Use TypedItemFastSlowRateLimiter instead.
type ItemFastSlowRateLimiter = TypedItemFastSlowRateLimiter[any]

// TypedItemFastSlowRateLimiter 前若干次快速重试，之后转为慢速重试。
type TypedItemFastSlowRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]int

	maxFastAttempts int
	fastDelay       time.Duration
	slowDelay       time.Duration
}

var _ RateLimiter = &ItemFastSlowRateLimiter{}

// Deprecated: NewItemFastSlowRateLimiter is deprecated, use NewTypedItemFastSlowRateLimiter instead.
func NewItemFastSlowRateLimiter(fastDelay, slowDelay time.Duration, maxFastAttempts int) RateLimiter {
	return NewTypedItemFastSlowRateLimiter[any](fastDelay, slowDelay, maxFastAttempts)
}

// NewTypedItemFastSlowRateLimiter 构造“先快后慢”限流器：前 maxFastAttempts
// 次失败使用 fastDelay，之后一律 slowDelay。
func NewTypedItemFastSlowRateLimiter[T comparable](fastDelay, slowDelay time.Duration, maxFastAttempts int) TypedRateLimiter[T] {
	return &TypedItemFastSlowRateLimiter[T]{
		failures:        map[T]int{},
		fastDelay:       fastDelay,
		slowDelay:       slowDelay,
		maxFastAttempts: maxFastAttempts,
	}
}

// When 先自增失败次数再判断阈值，使“第 maxFastAttempts 次”仍算快速重试。
func (r *TypedItemFastSlowRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	r.failures[item] = r.failures[item] + 1

	if r.failures[item] <= r.maxFastAttempts {
		return r.fastDelay
	}

	return r.slowDelay
}

func (r *TypedItemFastSlowRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

func (r *TypedItemFastSlowRateLimiter[T]) Forget(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// MaxOfRateLimiter 调用所有限流器并返回最坏（最长）的等待时间。与令牌桶组合时，若某些条目被单独延迟较久，看起来可能突破突发上限。
//
// Deprecated: Use TypedMaxOfRateLimiter instead.
type MaxOfRateLimiter = TypedMaxOfRateLimiter[any]

// TypedMaxOfRateLimiter 调用所有限流器并返回最坏（最长）的等待时间。与令牌桶组合时，若某些条目被单独延迟较久，看起来可能突破突发上限。
type TypedMaxOfRateLimiter[T comparable] struct {
	limiters []TypedRateLimiter[T]
}

// When 返回所有子限流器中的最大延迟，即“最坏情况”重试间隔。
func (r *TypedMaxOfRateLimiter[T]) When(item T) time.Duration {
	ret := time.Duration(0)
	for _, limiter := range r.limiters {
		curr := limiter.When(item)
		if curr > ret {
			ret = curr
		}
	}

	return ret
}

// Deprecated: NewMaxOfRateLimiter is deprecated, use NewTypedMaxOfRateLimiter instead.
func NewMaxOfRateLimiter(limiters ...TypedRateLimiter[any]) RateLimiter {
	return NewTypedMaxOfRateLimiter(limiters...)
}

// NewTypedMaxOfRateLimiter 组合多个限流器，任一限流器要求等待都必须等待。
func NewTypedMaxOfRateLimiter[T comparable](limiters ...TypedRateLimiter[T]) TypedRateLimiter[T] {
	return &TypedMaxOfRateLimiter[T]{limiters: limiters}
}

// NumRequeues 返回所有子限流器中的最大重试次数。
func (r *TypedMaxOfRateLimiter[T]) NumRequeues(item T) int {
	ret := 0
	for _, limiter := range r.limiters {
		curr := limiter.NumRequeues(item)
		if curr > ret {
			ret = curr
		}
	}

	return ret
}

// Forget 通知所有子限流器清除该条目的记录。
func (r *TypedMaxOfRateLimiter[T]) Forget(item T) {
	for _, limiter := range r.limiters {
		limiter.Forget(item)
	}
}

// WithMaxWaitRateLimiter 设置 maxDelay，避免等待过长。
// Deprecated: Use TypedWithMaxWaitRateLimiter instead.
type WithMaxWaitRateLimiter = TypedWithMaxWaitRateLimiter[any]

// TypedWithMaxWaitRateLimiter 设置 maxDelay，避免等待过长。
type TypedWithMaxWaitRateLimiter[T comparable] struct {
	limiter  TypedRateLimiter[T]
	maxDelay time.Duration
}

// Deprecated: NewWithMaxWaitRateLimiter is deprecated, use NewTypedWithMaxWaitRateLimiter instead.
func NewWithMaxWaitRateLimiter(limiter RateLimiter, maxDelay time.Duration) RateLimiter {
	return NewTypedWithMaxWaitRateLimiter[any](limiter, maxDelay)
}

// NewTypedWithMaxWaitRateLimiter 包装限流器并给延迟设置上限。
//
// 用于防止指数退避在长时间故障时把重试间隔拉到不可接受的程度。
func NewTypedWithMaxWaitRateLimiter[T comparable](limiter TypedRateLimiter[T], maxDelay time.Duration) TypedRateLimiter[T] {
	return &TypedWithMaxWaitRateLimiter[T]{limiter: limiter, maxDelay: maxDelay}
}

// When 返回被 maxDelay 截断后的延迟。
func (w TypedWithMaxWaitRateLimiter[T]) When(item T) time.Duration {
	delay := w.limiter.When(item)
	if delay > w.maxDelay {
		return w.maxDelay
	}

	return delay
}

// Forget 透传给被包装的限流器。
func (w TypedWithMaxWaitRateLimiter[T]) Forget(item T) {
	w.limiter.Forget(item)
}

// NumRequeues 透传给被包装的限流器。
func (w TypedWithMaxWaitRateLimiter[T]) NumRequeues(item T) int {
	return w.limiter.NumRequeues(item)
}
