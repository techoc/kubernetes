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

package flowcontrol

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestMultithreadedThrottling 用 10 个 goroutine 争抢令牌，验证在并发情况下
// 限速依然是全局的：100 QPS、burst=1 时取满 100 个令牌至少需要约 1 秒。
func TestMultithreadedThrottling(t *testing.T) {
	// 100 QPS、无突发的令牌桶。
	r := NewTokenBucketRateLimiter(100, 1)

	// taken 容量设为 100，配合 default 分支实现“收集满即停止”的 lambda。
	// 收集 100 个令牌的 channel。
	taken := make(chan bool, 100)

	// 启动多个 goroutine 争抢令牌。
	startCh := make(chan bool)
	endCh := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			// 等待起跑信号。
			<-startCh
			for {
				// 取一个令牌。
				r.Accept()
				select {
				// 尝试放入已取令牌 channel。
				case taken <- true:
					continue
				// channel 已满时发通知并返回。
				default:
					endCh <- true
					return
				}
			}
		}()
	}

	// 记录墙钟时间。
	startTime := time.Now()
	// 先耗尽初始容量，确保后续令牌都来自补充。
	r.Accept()
	// 启动并发争抢。
	close(startCh)
	// 等待收满 100 个令牌的信号。
	<-endCh
	// 记录墙钟时间。
	endTime := time.Now()

	// 容忍 1% 的时钟偏差。
	if duration := endTime.Sub(startTime); duration < (time.Second * 99 / 100) {
		// 无论怎样，都不应在不到 1 秒内从桶中取到 100 个令牌。
		t.Errorf("Expected it to take at least 1 second to get 100 tokens, took %v", duration)
	} else {
		t.Logf("Took %v to get 100 tokens", duration)
	}
}

// TestBasicThrottle 验证 burst 上限：桶容量为 3 时前 3 次非阻塞获取成功，第 4 次失败。
func TestBasicThrottle(t *testing.T) {
	r := NewTokenBucketRateLimiter(1, 3)
	for i := 0; i < 3; i++ {
		if !r.TryAccept() {
			t.Error("unexpected false accept")
		}
	}
	if r.TryAccept() {
		t.Error("unexpected true accept")
	}
}

// TestIncrementThrottle 验证令牌会按速率补充：耗尽后再等待足够时间即可重新获取。
func TestIncrementThrottle(t *testing.T) {
	r := NewTokenBucketRateLimiter(1, 1)
	if !r.TryAccept() {
		t.Error("unexpected false accept")
	}
	if r.TryAccept() {
		t.Error("unexpected true accept")
	}

	// 等待令牌补充。
	time.Sleep(2 * time.Second)

	if !r.TryAccept() {
		t.Error("unexpected false accept")
	}
}

// TestThrottle 验证阻塞式 Accept 的实际耗时：先瞬时消耗 burst=5 个令牌，
// 其余 11 个按 10 QPS 补充，因此总耗时不可能少于 1 秒。
func TestThrottle(t *testing.T) {
	r := NewTokenBucketRateLimiter(10, 5)

	// 前 5 个令牌应立即取到，剩余 11 个按 0.1s/个至少需要 1 秒。
	expectedFinish := time.Now().Add(time.Second * 1)
	for i := 0; i < 16; i++ {
		r.Accept()
	}
	if time.Now().Before(expectedFinish) {
		t.Error("rate limit was not respected, finished too early")
	}
}

// TestAlwaysFake 验证“永远放行”的假限速器既立即接受，也不会在阻塞式 Accept 上卡住。
func TestAlwaysFake(t *testing.T) {
	rl := NewFakeAlwaysRateLimiter()
	if !rl.TryAccept() {
		t.Error("TryAccept in AlwaysFake should return true.")
	}
	// 若此处阻塞，测试就会超时。
	rl.Accept()
}

// TestNeverFake 验证“永远拒绝”的假限速器：TryAccept 恒为 false，Accept 阻塞，
// 直到调用 Stop 才解除阻塞——这模拟了限速器被关闭时的唤醒语义。
func TestNeverFake(t *testing.T) {
	rl := NewFakeNeverRateLimiter()
	if rl.TryAccept() {
		t.Error("TryAccept in NeverFake should return false.")
	}

	finished := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		rl.Accept()
		finished = true
		wg.Done()
	}()

	// 等待一段时间，确认它始终没有结束。
	time.Sleep(time.Second)
	if finished {
		t.Error("Accept should block forever in NeverFake.")
	}

	rl.Stop()
	wg.Wait()
	if !finished {
		t.Error("Stop should make Accept unblock in NeverFake.")
	}
}

// TestWait 验证 Wait 会遵守 context：令牌可以等到时返回 nil，等不到时返回 context 错误。
func TestWait(t *testing.T) {
	r := NewTokenBucketRateLimiter(0.0001, 1)

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn()
	if err := r.Wait(ctx); err != nil {
		t.Errorf("unexpected wait failed, err: %v", err)
	}

	ctx2, cancelFn2 := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn2()
	if err := r.Wait(ctx2); err == nil {
		t.Errorf("unexpected wait success")
	} else {
		t.Log(fmt.Sprintf("wait err: %v", err))
	}
}

// fakeClock 是可手动推进的极简时钟，让令牌补充过程无需真实等待。
type fakeClock struct {
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (fc *fakeClock) Now() time.Time {
	return fc.now
}

func (fc *fakeClock) Sleep(d time.Duration) {
	fc.now = fc.now.Add(d)
}

func (fc *fakeClock) Since(ts time.Time) time.Duration {
	return time.Since(ts)
}

// TestRatePrecisionBug 是针对 x/time/rate 历史精度问题的回归测试：某些 QPS 在长时间
// 累积后会出现浮点误差，导致本应可用的令牌被误判为不足。这里用极高的时间精度、
// 反复推进 120 秒来确保问题不会复现。
func TestRatePrecisionBug(t *testing.T) {
	// golang.org/x/time/rate 曾有精度问题，这里验证其不再复现（至少已知形式）。该用例专门设计成在 14 秒后触发问题。
	qps := float32(time.Second) / float32(1031425*time.Microsecond)
	clock := newFakeClock()
	tb := NewTokenBucketRateLimiterWithClock(qps, 1, clock)

	for i := 0; i < 60; i++ {
		if !tb.TryAccept() {
			t.Fatalf("failed after %d seconds", i*2)
		}
		clock.Sleep(2 * time.Second)
	}
}
