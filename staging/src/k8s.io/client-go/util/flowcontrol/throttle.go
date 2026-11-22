/*
Copyright 2014 The Kubernetes Authors.

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

// Package flowcontrol 提供客户端侧的限流与退避工具。
// RateLimiter 基于令牌桶实现平滑 QPS 限制，并支持突发；Clock 抽象使测试无需真实等待。
package flowcontrol

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/utils/clock"
)

// PassiveRateLimiter 是不可阻塞的限流器：只能探测当前是否有令牌，
// 适用于“能拿就拿、拿不到就跳过”的场景（如 metrics 上报）。
type PassiveRateLimiter interface {
	// TryAccept returns true if a token is taken immediately. Otherwise,
	// it returns false.
	TryAccept() bool
	// Stop stops the rate limiter, subsequent calls to CanAccept will return false
	Stop()
	// QPS returns QPS of this rate limiter
	QPS() float32
}

// RateLimiter 在 PassiveRateLimiter 之上增加阻塞等待能力，
// 是 REST 客户端默认使用的接口。
type RateLimiter interface {
	PassiveRateLimiter
	// Accept returns once a token becomes available.
	Accept()
	// Wait returns nil if a token is taken before the Context is done.
	Wait(ctx context.Context) error
}

// tokenBucketPassiveRateLimiter 用 golang.org/x/time/rate 实现令牌桶，
// 但只暴露非阻塞接口；clock 用于让 TryAccept 使用可注入的时间。
type tokenBucketPassiveRateLimiter struct {
	limiter *rate.Limiter
	qps     float32
	clock   clock.PassiveClock
}

// tokenBucketRateLimiter 额外持有可 Sleep 的时钟，用于 Accept 的阻塞等待。
type tokenBucketRateLimiter struct {
	tokenBucketPassiveRateLimiter
	clock Clock
}

// NewTokenBucketRateLimiter creates a rate limiter which implements a token bucket approach.
// The rate limiter allows bursts of up to 'burst' to exceed the QPS, while still maintaining a
// smoothed qps rate of 'qps'.
// The bucket is initially filled with 'burst' tokens, and refills at a rate of 'qps'.
// The maximum number of tokens in the bucket is capped at 'burst'.
func NewTokenBucketRateLimiter(qps float32, burst int) RateLimiter {
	limiter := rate.NewLimiter(rate.Limit(qps), burst)
	return newTokenBucketRateLimiterWithClock(limiter, clock.RealClock{}, qps)
}

// NewTokenBucketPassiveRateLimiter is similar to NewTokenBucketRateLimiter except that it returns
// a PassiveRateLimiter which does not have Accept() and Wait() methods.
func NewTokenBucketPassiveRateLimiter(qps float32, burst int) PassiveRateLimiter {
	limiter := rate.NewLimiter(rate.Limit(qps), burst)
	return newTokenBucketRateLimiterWithPassiveClock(limiter, clock.RealClock{}, qps)
}

// An injectable, mockable clock interface.
type Clock interface {
	clock.PassiveClock
	Sleep(time.Duration)
}

var _ Clock = (*clock.RealClock)(nil)

// NewTokenBucketRateLimiterWithClock is identical to NewTokenBucketRateLimiter
// but allows an injectable clock, for testing.
func NewTokenBucketRateLimiterWithClock(qps float32, burst int, c Clock) RateLimiter {
	limiter := rate.NewLimiter(rate.Limit(qps), burst)
	return newTokenBucketRateLimiterWithClock(limiter, c, qps)
}

// NewTokenBucketPassiveRateLimiterWithClock is similar to NewTokenBucketRateLimiterWithClock
// except that it returns a PassiveRateLimiter which does not have Accept() and Wait() methods
// and uses a PassiveClock.
func NewTokenBucketPassiveRateLimiterWithClock(qps float32, burst int, c clock.PassiveClock) PassiveRateLimiter {
	limiter := rate.NewLimiter(rate.Limit(qps), burst)
	return newTokenBucketRateLimiterWithPassiveClock(limiter, c, qps)
}

func newTokenBucketRateLimiterWithClock(limiter *rate.Limiter, c Clock, qps float32) *tokenBucketRateLimiter {
	return &tokenBucketRateLimiter{
		tokenBucketPassiveRateLimiter: *newTokenBucketRateLimiterWithPassiveClock(limiter, c, qps),
		clock:                         c,
	}
}

func newTokenBucketRateLimiterWithPassiveClock(limiter *rate.Limiter, c clock.PassiveClock, qps float32) *tokenBucketPassiveRateLimiter {
	return &tokenBucketPassiveRateLimiter{
		limiter: limiter,
		qps:     qps,
		clock:   c,
	}
}

func (tbprl *tokenBucketPassiveRateLimiter) Stop() {
}

func (tbprl *tokenBucketPassiveRateLimiter) QPS() float32 {
	return tbprl.qps
}

// TryAccept 以注入时钟的当前时间尝试取一个令牌，不阻塞。
func (tbprl *tokenBucketPassiveRateLimiter) TryAccept() bool {
	return tbprl.limiter.AllowN(tbprl.clock.Now(), 1)
}

// Accept will block until a token becomes available
//
// 通过 ReserveN 计算需要等待的时长再 Sleep，而不是忙等或依赖真实时间，
// 这样 fake clock 可以在测试中瞬间推进。
func (tbrl *tokenBucketRateLimiter) Accept() {
	now := tbrl.clock.Now()
	tbrl.clock.Sleep(tbrl.limiter.ReserveN(now, 1).DelayFrom(now))
}

// Wait 直接使用 rate.Limiter 的阻塞等待，并响应 context 取消。
func (tbrl *tokenBucketRateLimiter) Wait(ctx context.Context) error {
	return tbrl.limiter.Wait(ctx)
}

// fakeAlwaysRateLimiter 永远放行，供测试或无需限流的场景使用。
type fakeAlwaysRateLimiter struct{}

func NewFakeAlwaysRateLimiter() RateLimiter {
	return &fakeAlwaysRateLimiter{}
}

func (t *fakeAlwaysRateLimiter) TryAccept() bool {
	return true
}

func (t *fakeAlwaysRateLimiter) Stop() {}

func (t *fakeAlwaysRateLimiter) Accept() {}

func (t *fakeAlwaysRateLimiter) QPS() float32 {
	return 1
}

func (t *fakeAlwaysRateLimiter) Wait(ctx context.Context) error {
	return nil
}

// fakeNeverRateLimiter 永不放行：Accept 会一直阻塞到 Stop 被调用，
// 用来模拟“完全限流”并验证调用方是否会正确退出。
type fakeNeverRateLimiter struct {
	wg sync.WaitGroup
}

func NewFakeNeverRateLimiter() RateLimiter {
	rl := fakeNeverRateLimiter{}
	rl.wg.Add(1)
	return &rl
}

func (t *fakeNeverRateLimiter) TryAccept() bool {
	return false
}

func (t *fakeNeverRateLimiter) Stop() {
	t.wg.Done()
}

func (t *fakeNeverRateLimiter) Accept() {
	t.wg.Wait()
}

func (t *fakeNeverRateLimiter) QPS() float32 {
	return 1
}

func (t *fakeNeverRateLimiter) Wait(ctx context.Context) error {
	return errors.New("can not be accept")
}

var (
	_ RateLimiter = (*tokenBucketRateLimiter)(nil)
	_ RateLimiter = (*fakeAlwaysRateLimiter)(nil)
	_ RateLimiter = (*fakeNeverRateLimiter)(nil)
)

var _ PassiveRateLimiter = (*tokenBucketPassiveRateLimiter)(nil)
