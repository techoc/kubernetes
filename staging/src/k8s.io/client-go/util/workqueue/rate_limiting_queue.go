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

import "k8s.io/utils/clock"

// RateLimitingInterface 对入队条目做限速。
//
// Deprecated: Use TypedRateLimitingInterface instead.
type RateLimitingInterface TypedRateLimitingInterface[any]

// TypedRateLimitingInterface 对入队条目做限速。
//
// 它在延迟队列基础上叠加限流器：重试延迟由 rateLimiter.When 决定，
// 调用方成功后必须 Forget，否则失败计数会一直保留。
type TypedRateLimitingInterface[T comparable] interface {
	TypedDelayingInterface[T]

	// AddRateLimited 按限速器允许的时间把条目加入队列。
	AddRateLimited(item T)

	// Forget 表示条目不再重试。无论永久失败还是成功，都会停止限速器对该条目的
	// 跟踪。这只会清除 `rateLimiter`，仍需自行调用队列的 `Done`。
	Forget(item T)

	// NumRequeues 返回条目被重新入队的次数。
	NumRequeues(item T) int
}

// RateLimitingQueueConfig 指定自定义 RateLimitingInterface 的可选配置。
//
// Deprecated: Use TypedRateLimitingQueueConfig instead.
type RateLimitingQueueConfig = TypedRateLimitingQueueConfig[any]

// TypedRateLimitingQueueConfig 指定自定义 TypedRateLimitingInterface 的可选配置。
type TypedRateLimitingQueueConfig[T comparable] struct {
	// 队列名称；未命名时不会注册 metrics。
	Name string

	// MetricsProvider 可选，用于为该队列指定 metrics provider，而非使用全局 provider。
	MetricsProvider MetricsProvider

	// Clock 可注入真实或假的时钟，便于测试。
	Clock clock.WithTicker

	// DelayingQueue 可注入自定义延迟队列，替代默认延迟队列。
	DelayingQueue TypedDelayingInterface[T]
}

// NewRateLimitingQueue 创建支持限速入队的工作队列。
// 记得调用 Forget！否则可能永久跟踪失败记录。
// NewRateLimitingQueue 不产生 metrics。若需使用 MetricsProvider，请改用
// NewRateLimitingQueueWithConfig 并指定名字。
//
// Deprecated: Use NewTypedRateLimitingQueue instead.
func NewRateLimitingQueue(rateLimiter RateLimiter) RateLimitingInterface {
	return NewRateLimitingQueueWithConfig(rateLimiter, RateLimitingQueueConfig{})
}

// NewTypedRateLimitingQueue 创建支持限速入队的工作队列。
// 记得调用 Forget！否则可能永久跟踪失败记录。
// NewTypedRateLimitingQueue 不产生 metrics。若需使用 MetricsProvider，请改用
// NewTypedRateLimitingQueueWithConfig 并指定名字。
func NewTypedRateLimitingQueue[T comparable](rateLimiter TypedRateLimiter[T]) TypedRateLimitingInterface[T] {
	return NewTypedRateLimitingQueueWithConfig(rateLimiter, TypedRateLimitingQueueConfig[T]{})
}

// NewRateLimitingQueueWithConfig 创建支持限速入队的工作队列，可自定义各项属性。
// 记得调用 Forget！否则可能永久跟踪失败记录。
//
// Deprecated: Use NewTypedRateLimitingQueueWithConfig instead.
func NewRateLimitingQueueWithConfig(rateLimiter RateLimiter, config RateLimitingQueueConfig) RateLimitingInterface {
	return NewTypedRateLimitingQueueWithConfig(rateLimiter, config)
}

// NewTypedRateLimitingQueueWithConfig 创建支持限速入队的工作队列，可自定义各项属性。
// 记得调用 Forget！否则可能永久跟踪失败记录。
func NewTypedRateLimitingQueueWithConfig[T comparable](rateLimiter TypedRateLimiter[T], config TypedRateLimitingQueueConfig[T]) TypedRateLimitingInterface[T] {
	// 未注入时钟/延迟队列时使用默认实现，并把名称与指标配置透传下去。
	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}

	if config.DelayingQueue == nil {
		config.DelayingQueue = NewTypedDelayingQueueWithConfig(TypedDelayingQueueConfig[T]{
			Name:            config.Name,
			MetricsProvider: config.MetricsProvider,
			Clock:           config.Clock,
		})
	}

	return &rateLimitingType[T]{
		TypedDelayingInterface: config.DelayingQueue,
		rateLimiter:            rateLimiter,
	}
}

// NewNamedRateLimitingQueue 创建支持限速入队的带名字工作队列。
// Deprecated: Use NewRateLimitingQueueWithConfig instead.
func NewNamedRateLimitingQueue(rateLimiter RateLimiter, name string) RateLimitingInterface {
	return NewRateLimitingQueueWithConfig(rateLimiter, RateLimitingQueueConfig{
		Name: name,
	})
}

// NewRateLimitingQueueWithDelayingInterface 创建支持限速入队的工作队列，可注入
// 自定义延迟队列。
// Deprecated: Use NewRateLimitingQueueWithConfig instead.
func NewRateLimitingQueueWithDelayingInterface(di DelayingInterface, rateLimiter RateLimiter) RateLimitingInterface {
	return NewRateLimitingQueueWithConfig(rateLimiter, RateLimitingQueueConfig{
		DelayingQueue: di,
	})
}

// rateLimitingType 包装 Interface，提供限速重新入队能力。
type rateLimitingType[T comparable] struct {
	TypedDelayingInterface[T]

	rateLimiter TypedRateLimiter[T]
}

// AddRateLimited 按限速器允许的时间调用 AddAfter 入队。
//
// 注意 When 会同时推进该条目的失败计数，因此每次重试调用一次即可。
func (q *rateLimitingType[T]) AddRateLimited(item T) {
	q.TypedDelayingInterface.AddAfter(item, q.rateLimiter.When(item))
}

// NumRequeues 返回限流器记录的该条目重试次数。
func (q *rateLimitingType[T]) NumRequeues(item T) int {
	return q.rateLimiter.NumRequeues(item)
}

// Forget 清除限流器中的失败记录；底层队列的 Done 仍需单独调用。
func (q *rateLimitingType[T]) Forget(item T) {
	q.rateLimiter.Forget(item)
}
