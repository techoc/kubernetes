/*
Copyright 2023 The Kubernetes Authors.

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

package waitgroup

import (
	"context"
	"fmt"
	"sync"
)

// RateLimiter 抽象出 RateLimitedSafeWaitGroup 使用的限速器。
// 实现必须是线程安全的：Done 会在释放内部锁之后并发调用它。
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// RateLimiterFactoryFunc 由 RateLimitedSafeWaitGroup 调用来创建限速器实例，
// 该限速器用于限制在途请求归还（Done）的速率。'count' 是预计会对该 wait group
// 调用 'Done' 的在途请求数。
type RateLimiterFactoryFunc func(count int) (RateLimiter, context.Context, context.CancelFunc)

// RateLimitedSafeWaitGroup 在 SafeWaitGroup 的基础上，对进入等待状态后发生的
// Done 施加限速，用于平滑关闭一批请求。首次使用后不得复制。
type RateLimitedSafeWaitGroup struct {
	wg sync.WaitGroup
	// 一旦 Wait 开始，之后所有 Done 调用都会被该限速器限速。
	limiter RateLimiter
	stopCtx context.Context

	mu sync.Mutex
	// wait 表示 Wait 是否已被调用；为 true 时任何正增量的 Add 都会返回错误。
	wait bool
	// 当前仍在使用该 wait group 的请求数。
	count int
}

// Add 增加 delta（可以为负），与 sync.WaitGroup 类似。
// 如果在 Wait 之后发生正增量的 Add，会返回错误，从而阻止不安全的 Add。
func (wg *RateLimitedSafeWaitGroup) Add(delta int) error {
	wg.mu.Lock()
	defer wg.mu.Unlock()

	// Wait 之后不允许再新增请求：否则 Wait 可能在请求尚未结束时提前返回。
	if wg.wait && delta > 0 {
		return fmt.Errorf("add with positive delta after Wait is forbidden")
	}
	wg.wg.Add(delta)
	wg.count += delta
	return nil
}

// Done 递减 WaitGroup 计数；仅当 wait group 处于等待模式时才应用限速。
func (wg *RateLimitedSafeWaitGroup) Done() {
	var limiter RateLimiter
	func() {
		wg.mu.Lock()
		defer wg.mu.Unlock()

		wg.count -= 1
		if wg.wait {
			// 只在持锁期间取出限速器，实际限速在锁外执行，避免持锁等待拖慢其它 Done。
			limiter = wg.limiter
		}
	}()

	// 先归还 WaitGroup 计数，再限速：这样 Wait 的硬性超时保护不会被限速拖住。
	defer wg.wg.Done()
	if limiter != nil {
		limiter.Wait(wg.stopCtx)
	}
}

// Wait 阻塞直到 WaitGroup 计数归零或达到硬性超时。
// 它返回调用 Wait 时刻记录的活跃请求数，以及 Wait 即将返回前已排空的请求数
// （即刚刚通过该 wait group 完成 Done 的数量）。
// 理想情况下两者相等，表示所有使用该 wait group 的请求都已释放。
func (wg *RateLimitedSafeWaitGroup) Wait(limiterFactory RateLimiterFactoryFunc) (int, int, error) {
	if limiterFactory == nil {
		return 0, 0, fmt.Errorf("rate limiter factory must be specified")
	}

	var cancel context.CancelFunc
	var countNow, countAfter int
	func() {
		wg.mu.Lock()
		defer wg.mu.Unlock()

		// 用当前在途数创建限速器，并在持锁期间切换到等待模式，保证之后进入的 Done 都能看到限速器。
		wg.limiter, wg.stopCtx, cancel = limiterFactory(wg.count)
		countNow = wg.count
		wg.wait = true
	}()

	defer cancel()
	// 必须有硬性超时兜底：万一请求迟迟不调用 Done，等待也不能无限持续。
	waitDoneCh := make(chan struct{})
	go func() {
		defer close(waitDoneCh)
		wg.wg.Wait()
	}()

	var err error
	select {
	case <-wg.stopCtx.Done():
		err = wg.stopCtx.Err()
	case <-waitDoneCh:
	}

	// 在返回前再读一次计数，作为“排空”数量；此时请求可能仍在陆续 Done。
	func() {
		wg.mu.Lock()
		defer wg.mu.Unlock()

		countAfter = wg.count
	}()
	return countNow, countAfter, err
}
