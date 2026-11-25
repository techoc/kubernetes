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
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestRateLimitedSafeWaitGroup 验证 RateLimitedSafeWaitGroup 的完整等待流程：
// Wait 之前计数可正常增减；进入 Waiting 后禁止 Add；限流器工厂收到当前计数；
// 等待期间限流器按计数被调用；结束后计数归零且 context 被取消一次。
func TestRateLimitedSafeWaitGroup(t *testing.T) {
	// 统计限流器 Wait 的调用次数，覆盖等待前与等待中两个阶段。
	limiter := &limiterWrapper{}

	// 期望使用工厂返回的 context，并统计其 cancel 调用次数。
	var cancelInvoked int
	factory := &factory{
		limiter: limiter,
		grace:   2 * time.Second,
		ctx:     context.Background(),
		cancel: func() {
			cancelInvoked++
		},
	}
	target := &rateLimitedSafeWaitGroupWrapper{
		RateLimitedSafeWaitGroup: &RateLimitedSafeWaitGroup{limiter: limiter},
	}

	// 两组请求：n1 在调用 Wait 之前完成；n2 在调用 Wait 之后仍在进行中。
	n1, n2 := 100, 101

	// 用于确认 n1 全部完成。
	n1DoneWG := sync.WaitGroup{}

	// 用于确认 n2 都调用了 Add 但都未调用 Done——此时可安全触发 Wait。
	n2BeforeWaitWG := sync.WaitGroup{}
	// 用于确认 n2 全部调用 Done 并结束。
	n2DoneWG := sync.WaitGroup{}

	startCh, blockedCh := make(chan struct{}), make(chan struct{})
	n1DoneWG.Add(n1)
	for i := 0; i < n1; i++ {
		go func() {
			defer n1DoneWG.Done()
			<-startCh

			target.Add(1)
			//立即结束使用 waitgroup。
			target.Done()
		}()
	}

	n2BeforeWaitWG.Add(n2)
	n2DoneWG.Add(n2)
	for i := 0; i < n2; i++ {
		go func() {
			func() {
				defer n2BeforeWaitWG.Done()
				<-startCh

				target.Add(1)
			}()

			func() {
				defer n2DoneWG.Done()
				//等待测试指示 n2 中的请求
				//结束使用 waitgroup。
				<-blockedCh

				target.Done()
			}()
		}()
	}

	// 初始计数为 0。
	if count := target.Count(); count != 0 {
		t.Errorf("expected count to be zero, but got: %d", count)
	}
	// 放行所有请求。
	close(startCh)
	// 等待 n1 全部完成。
	n1DoneWG.Wait()

	// n1 完成后计数应归零，且此时不应有任何限流器调用。
	if invoked := limiter.invoked(); invoked != 0 {
		t.Errorf("expected no call to rate limiter before Wait is called, but got: %d", invoked)
	}

	// 确认 n2 都已 Add 且尚未 Done。
	n2BeforeWaitWG.Wait()

	// 此时计数应等于 n2。
	if count := target.Count(); count != n2 {
		t.Errorf("expected count to be: %d, but got: %d", n2, count)
	}

	// 现在把 waitgroup 标记为 Waiting，并异步等待其排空。
	waitDoneCh := make(chan waitResult)
	go func() {
		factory.grace = 2 * time.Second
		before, after, err := target.Wait(factory.NewRateLimiter)
		waitDoneCh <- waitResult{before: before, after: after, err: err}
	}()

	// 轮询等待进入 Waiting 状态，消除异步切换带来的竞态抖动。
	var waitingGot bool
	wait.PollImmediate(500*time.Millisecond, wait.ForeverTestTimeout, func() (done bool, err error) {
		if waiting := target.Waiting(); waiting {
			waitingGot = true
			return true, nil
		}
		return false, nil
	})
	// 确认已进入 Waiting 模式。
	if !waitingGot {
		t.Errorf("expected to be in waiting")
	}

	// Waiting 模式下不允许再 Add，应报错。
	if err := target.Add(1); err == nil ||
		!strings.Contains(err.Error(), "add with positive delta after Wait is forbidden") {
		t.Errorf("expected Add to return error while in waiting mode: %v", err)
	}

	// 确认传给限流器工厂的计数等于当前在途请求数。
	if factory.countGot != n2 {
		t.Errorf("expected count passed to factory to be: %d, but got: %d", n2, factory.countGot)
	}

	// 放行 n2，让它们调用 Done 以排空计数。
	close(blockedCh)
	n2DoneWG.Wait()

	if invoked := limiter.invoked(); invoked != n2 {
		t.Errorf("expected rate limiter to be called %d times, but got: %d", n2, invoked)
	}

	waitResult := <-waitDoneCh
	if count := target.Count(); count != 0 {
		t.Errorf("expected count to be zero, but got: %d", count)
	}
	if waitResult.before != n2 {
		t.Errorf("expected count before Wait to be: %d, but got: %d", n2, waitResult.before)
	}
	if waitResult.after != 0 {
		t.Errorf("expected count after Wait to be zero, but got: %d", waitResult.after)
	}
	if cancelInvoked != 1 {
		t.Errorf("expected context cancel to be invoked once, but got: %d", cancelInvoked)
	}
}

// TestRateLimitedSafeWaitGroupWithHardTimeout 验证预先取消的 context 会让 Wait 立即返回
// context.Canceled，且不改变现有计数。
func TestRateLimitedSafeWaitGroupWithHardTimeout(t *testing.T) {
	target := &rateLimitedSafeWaitGroupWrapper{
		RateLimitedSafeWaitGroup: &RateLimitedSafeWaitGroup{},
	}
	n := 10
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			target.Add(1)
		}()
	}

	wg.Wait()
	if count := target.Count(); count != n {
		t.Errorf("expected count to be: %d, but got: %d", n, count)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activeAt, activeNow, err := target.Wait(func(count int) (RateLimiter, context.Context, context.CancelFunc) {
		return nil, ctx, cancel
	})
	if activeAt != n {
		t.Errorf("expected active at Wait to be: %d, but got: %d", n, activeAt)
	}
	if activeNow != n {
		t.Errorf("expected active after Wait to be: %d, but got: %d", n, activeNow)
	}
	if err != context.Canceled {
		t.Errorf("expected error: %v, but got: %v", context.Canceled, err)
	}
}

// TestRateLimitedSafeWaitGroupWithBurstOfOne 验证 burst=1 的限流器下，
// Wait 能在宽限期（允许 2 倍余量）内排空全部在途请求，适配 CPU 饥饿的测试环境。
func TestRateLimitedSafeWaitGroupWithBurstOfOne(t *testing.T) {
	target := &rateLimitedSafeWaitGroupWrapper{
		RateLimitedSafeWaitGroup: &RateLimitedSafeWaitGroup{},
	}
	n := 200
	grace := 5 * time.Second
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			target.Add(1)
		}()
	}
	wg.Wait()

	waitingCh := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			<-waitingCh
			target.Done()
		}()
	}
	defer wg.Wait()

	now := time.Now()
	t.Logf("Wait starting, N=%d, grace: %s, at: %s", n, grace, now)
	activeAt, activeNow, err := target.Wait(func(count int) (RateLimiter, context.Context, context.CancelFunc) {
		defer close(waitingCh)
		// 上下文中不设截止时间，Wait 会一直等待；这里只测量排空请求所需的时间。
		return rate.NewLimiter(rate.Limit(n/int(grace.Seconds())), 1), context.Background(), func() {}
	})
	took := time.Since(now)
	t.Logf("Wait finished, count(before): %d, count(after): %d, took: %s, err: %v", activeAt, activeNow, took, err)

	// CPU 饥饿环境下 goroutine 可能无法准时结束，因此放宽到 2 倍宽限期。
	if took > 2*grace {
		t.Errorf("expected Wait to take: %s, but it took: %s", grace, took)
	}
}

// waitResult 记录 Wait 返回的等待前/后计数与错误。
type waitResult struct {
	before, after int
	err           error
}

// rateLimitedSafeWaitGroupWrapper 暴露内部计数与等待状态，供测试断言。
type rateLimitedSafeWaitGroupWrapper struct {
	*RateLimitedSafeWaitGroup
}

// 仅供测试使用。
// Count 在加锁后读取当前在途计数。
func (wg *rateLimitedSafeWaitGroupWrapper) Count() int {
	wg.mu.Lock()
	defer wg.mu.Unlock()

	return wg.count
}

// Waiting 在加锁后读取 Waiting 标志。
func (wg *rateLimitedSafeWaitGroupWrapper) Waiting() bool {
	wg.mu.Lock()
	defer wg.mu.Unlock()

	return wg.wait
}

// limiterWrapper 包装限流器并统计 Wait 调用次数；调用次数受互斥锁保护。
type limiterWrapper struct {
	delegate RateLimiter
	lock     sync.Mutex
	invokedN int
}

// invoked 返回累计的 Wait 调用次数。
func (w *limiterWrapper) invoked() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.invokedN
}

// Wait 先记录一次调用，再委托给底层限流器。
func (w *limiterWrapper) Wait(ctx context.Context) error {
	w.lock.Lock()
	w.invokedN++
	w.lock.Unlock()

	if w.delegate != nil {
		w.delegate.Wait(ctx)
	}
	return nil
}

// factory 是限流器工厂：记录收到的计数，并按宽限期构造限流速率。
type factory struct {
	limiter  *limiterWrapper
	grace    time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	countGot int
}

// NewRateLimiter 按 count 与宽限期计算每秒速率，burst 固定为 20。
func (f *factory) NewRateLimiter(count int) (RateLimiter, context.Context, context.CancelFunc) {
	f.countGot = count
	f.limiter.delegate = rate.NewLimiter(rate.Limit(count/int(f.grace.Seconds())), 20)
	return f.limiter, f.ctx, f.cancel
}
