/*
Copyright 2017 The Kubernetes Authors.

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

package runner

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"
)

// receiver 用于记录被管理函数的调用次数和返回值。
type receiver struct {
	counter atomic.Int32
	// counterCh 用于通知 F() 已完成，并发送最新的调用计数。
	// 使用无缓冲 channel，使 F() 中的发送操作保持阻塞。
	counterCh chan int
	resultMu  sync.RWMutex
	result    error
}

// F 递增调用计数、通知测试并返回可配置的返回值。
func (r *receiver) F() error {
	newCount := r.counter.Add(1)
	// 阻塞式发送：F() 会停在这里，直到测试从 counterCh 读取。
	r.counterCh <- int(newCount)
	r.resultMu.RLock()
	defer r.resultMu.RUnlock()
	return r.result
}

// newReceiver 创建一个用于测试的 receiver。
func newReceiver() *receiver {
	return &receiver{
		counterCh: make(chan int),
	}
}

// calls 返回调用计数的 channel，供测试等待函数执行。
func (r *receiver) calls() <-chan int {
	return r.counterCh
}

// setReturnValue 设置 F() 下次执行时返回的错误。
func (r *receiver) setReturnValue(err error) {
	r.resultMu.Lock()
	defer r.resultMu.Unlock()
	r.result = err
}

// assertCalls 等待被管理函数被调用，并断言总调用次数等于 expectedCalls。
// 若等待超时或调用次数不符，则让测试失败。
func assertCalls(t *testing.T, r *receiver, expectedCalls int) {
	t.Helper()
	select {
	case calls := <-r.calls():
		if calls != expectedCalls {
			t.Fatalf("expected %d calls, but got %d", expectedCalls, calls)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for function execution (expected %d calls, got %d)", expectedCalls, r.counter.Load())
	}
}

// assertNoCalls 等待 100 毫秒，并断言这段时间内被管理函数未被调用。
// 若检测到调用，则让测试失败。
func assertNoCalls(t *testing.T, r *receiver) {
	t.Helper()
	select {
	case calls := <-r.calls():
		t.Fatalf("unexpected function execution detected (call count: %d)", calls)
	case <-time.After(100 * time.Millisecond):
	}
}

// Test_BoundedFrequencyRunner 使用假时钟验证限频器的最小/最大间隔行为。
func Test_BoundedFrequencyRunner(t *testing.T) {
	var minInterval = 1 * time.Second
	var retryInterval = 5 * time.Second
	var maxInterval = 10 * time.Second
	obj := newReceiver()
	fakeClock := clock.NewFakeClock(time.Now())
	runner := construct("test-runner", obj.F, minInterval, retryInterval, maxInterval, fakeClock)
	stop := make(chan struct{})
	defer close(stop)

	go runner.Loop(stop)

	// 第一次调用应立即执行。
	// rel=0ms（相对时间，保持原标记以便对照）
	runner.Run()
	assertCalls(t, obj, 1)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}
	// 尚未达到最小间隔，延迟执行不应触发。
	fakeClock.Step(500 * time.Millisecond) // rel=500ms
	runner.Run()
	assertNoCalls(t, obj)

	// 尚未达到最小间隔，延迟执行不应触发。
	fakeClock.Step(499 * time.Millisecond) // rel=999ms
	runner.Run()
	assertNoCalls(t, obj)

	// 达到最小间隔，执行之前排队的运行。
	fakeClock.Step(1 * time.Millisecond) // rel=1000ms
	assertCalls(t, obj, 2)

	runner.Run()
	assertNoCalls(t, obj)

	// 尚未达到最小间隔，延迟执行不应触发。
	fakeClock.Step(1 * time.Millisecond) // rel=1ms
	runner.Run()
	assertNoCalls(t, obj)

	// 没有新的 Run()，推进时间也不会提前触发。
	fakeClock.Step(998 * time.Millisecond) // rel=999ms
	assertNoCalls(t, obj)

	// 达到最小间隔，执行之前排队的运行。
	fakeClock.Step(1 * time.Millisecond) // rel=1000ms
	assertCalls(t, obj, 3)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}
	// 时间流逝但没有待执行请求，不应触发运行。
	fakeClock.Step(1 * time.Second) // rel=1000ms
	assertNoCalls(t, obj)

	// 达到最大间隔后，即使没有 Run() 也应执行。
	fakeClock.Step(maxInterval) // rel=10000ms
	assertCalls(t, obj, 4)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}
	// 尚未达到最小间隔，延迟执行不应触发。
	fakeClock.Step(1 * time.Millisecond) // rel=1ms
	runner.Run()
	assertNoCalls(t, obj)

	// 推进到最小间隔到期。
	fakeClock.Step(999 * time.Millisecond) // rel=1000ms
	assertCalls(t, obj, 5)
}

// Test_BoundedFrequencyRunnerRetry 使用假时钟验证失败后的重试退避行为。
func Test_BoundedFrequencyRunnerRetry(t *testing.T) {
	var minInterval = 1 * time.Second
	var retryInterval = 5 * time.Second
	var maxInterval = 10 * time.Second
	obj := newReceiver()
	fakeClock := clock.NewFakeClock(time.Now())
	runner := construct("test-runner", obj.F, minInterval, retryInterval, maxInterval, fakeClock)
	stop := make(chan struct{})
	defer close(stop)

	go runner.Loop(stop)

	// 首次立即执行并返回错误，排队一次重试。
	// rel=0ms（相对时间，保持原标记以便对照）
	obj.setReturnValue(fmt.Errorf("sync error"))
	runner.Run()
	assertCalls(t, obj, 1)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// 下一次执行将成功。
	obj.setReturnValue(nil)
	assertNoCalls(t, obj)

	// 最小间隔内没有新的 Run()，不应执行。
	// 尚无待执行请求，不应有调用。
	fakeClock.Step(minInterval) // rel=1000ms
	assertNoCalls(t, obj)

	// 达到重试间隔后，重试被执行。
	fakeClock.Step(4 * time.Second) // rel=5000ms
	assertCalls(t, obj, 2)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// 最小间隔内再次触发错误，重试延后。
	fakeClock.Step(499 * time.Millisecond) // rel=499ms
	obj.setReturnValue(fmt.Errorf("sync error"))
	runner.Run()
	assertNoCalls(t, obj)

	// 达到最小间隔后执行排队的运行，并由其返回值再排队重试。
	fakeClock.Step(501 * time.Millisecond) // rel=1000ms
	assertCalls(t, obj, 3)

	// 下一次执行将成功。
	obj.setReturnValue(nil)
	assertNoCalls(t, obj)

	// 等待最小间隔结束。
	fakeClock.Step(time.Second) // rel=1000ms
	assertNoCalls(t, obj)

	// 主动运行一次成功，取消之前排队的重试。
	runner.Run()
	assertCalls(t, obj, 4)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// 已成功运行，重试被取消。
	fakeClock.Step(4 * time.Second)
	assertNoCalls(t, obj)

	// 新的失败会重新排队重试。
	obj.setReturnValue(fmt.Errorf("sync error"))
	runner.Run()
	assertCalls(t, obj, 5)
	for fakeClock.Waiters() != 2 { // 等待重试定时器重置
		time.Sleep(1 * time.Millisecond)
	}

	// 下一次执行将成功。
	obj.setReturnValue(nil)
	assertNoCalls(t, obj)

	// 最小间隔内再次 Run()，调用会被延迟。
	fakeClock.Step(100 * time.Millisecond) // rel=100ms
	runner.Run()
	assertNoCalls(t, obj)

	// 达到最小间隔后执行延迟的运行。
	fakeClock.Step(900 * time.Millisecond) // rel=1000ms
	assertCalls(t, obj, 6)
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// 已成功运行，重试被取消。
	fakeClock.Step(4 * time.Second) // rel=4s since run, 5s since RetryAfter
	assertNoCalls(t, obj)

	// 距离上次运行还未到最大间隔，不应执行。
	fakeClock.Step(5 * time.Second) // rel=9s since run, 10s since RetryAfter
	assertNoCalls(t, obj)

	fakeClock.Step(time.Second) // rel=10s since run
	assertCalls(t, obj, 7)
}

// Test_BoundedFrequencyRunnerRetryShorterThanMinInterval 验证重试间隔小于最小间隔时不会提前执行。
func Test_BoundedFrequencyRunnerRetryShorterThanMinInterval(t *testing.T) {
	var minInterval = 5 * time.Second
	// 注意：这里重试间隔故意短于最小间隔。
	var retryInterval = 1 * time.Second // Shorter than minInterval
	var maxInterval = 10 * time.Second
	obj := newReceiver()
	fakeClock := clock.NewFakeClock(time.Now())
	runner := construct("test-runner-short-retry", obj.F, minInterval, retryInterval, maxInterval, fakeClock)
	stop := make(chan struct{})
	defer close(stop)

	go runner.Loop(stop)

	// 首次执行失败，排队一次重试。
	// rel=0s（相对时间，保持原标记以便对照）
	obj.setReturnValue(fmt.Errorf("sync error"))
	runner.Run()
	assertCalls(t, obj, 1)
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}

	// 下一次执行将成功。
	obj.setReturnValue(nil)
	assertNoCalls(t, obj)

	// 超过重试间隔但最小间隔未到，不应执行。
	// rel=1s
	fakeClock.Step(retryInterval)
	// 最小间隔未到，所以仍不应执行。
	assertNoCalls(t, obj) // Still shouldn't run because minInterval hasn't passed since run 1 finished.

	// 推进到最小间隔到期前一毫秒。
	// rel=4.999s（保持原标记以便对照）
	fakeClock.Step(minInterval - retryInterval - 1*time.Millisecond)
	assertNoCalls(t, obj)

	// 最小间隔到期，之前排队的重试触发执行。
	// rel=5s（保持原标记以便对照）
	fakeClock.Step(1 * time.Millisecond)
	// 由早先的重试触发，且已满足最小间隔。
	assertCalls(t, obj, 2) // Run happens now, triggered by the earlier retry, respecting minInterval.
	// 等待定时器重置完成，再推进时间。
	for fakeClock.Waiters() != 2 {
		time.Sleep(1 * time.Millisecond)
	}
	// 没有 Run() 或重试，最大间隔到期后执行一次。
	fakeClock.Step(maxInterval) // rel=10s since run 2
	assertCalls(t, obj, 3)
}

// TestBoundedFrequencyRunner_Run_RunsAgainAfterMinInterval_RealClock 用真实时钟验证最小间隔后可再次运行。
func TestBoundedFrequencyRunner_Run_RunsAgainAfterMinInterval_RealClock(t *testing.T) {
	// 真实时钟测试采用较短的间隔以控制测试耗时。
	var minInterval = 500 * time.Millisecond
	var retryInterval = 800 * time.Millisecond
	var maxInterval = 1500 * time.Millisecond
	obj := newReceiver()
	runner := NewBoundedFrequencyRunner("test-runner", obj.F, minInterval, retryInterval, maxInterval)

	stopCh := make(chan struct{})
	defer close(stopCh)
	go runner.Loop(stopCh)

	// 第一次运行。
	runner.Run() // First run
	assertCalls(t, obj, 1)

	time.Sleep(2 * minInterval)
	assertNoCalls(t, obj)

	// 第二次运行。
	runner.Run() // Second run
	assertCalls(t, obj, 2)
}

// TestBoundedFrequencyRunner_Run_DoesNotRunBeforeMinInterval_RealClock 用真实时钟验证最小间隔内不会重复运行。
func TestBoundedFrequencyRunner_Run_DoesNotRunBeforeMinInterval_RealClock(t *testing.T) {
	// 真实时钟测试采用较短的间隔以控制测试耗时。
	var minInterval = 500 * time.Millisecond
	var retryInterval = 800 * time.Millisecond
	var maxInterval = 1500 * time.Millisecond
	obj := newReceiver()
	runner := NewBoundedFrequencyRunner("test-runner", obj.F, minInterval, retryInterval, maxInterval)

	stopCh := make(chan struct{})
	defer close(stopCh)
	go runner.Loop(stopCh)

	// 第一次运行。
	runner.Run() // First run
	assertCalls(t, obj, 1)

	time.Sleep(minInterval / 4)
	runner.Run()
	assertNoCalls(t, obj)
}

// TestBoundedFrequencyRunner_RunAfterMaxInterval_RealClock 用真实时钟验证最大间隔到期会自动运行。
func TestBoundedFrequencyRunner_RunAfterMaxInterval_RealClock(t *testing.T) {
	// 真实时钟测试采用较短的间隔以控制测试耗时。
	var minInterval = 100 * time.Millisecond
	var retryInterval = 200 * time.Millisecond
	var maxInterval = 500 * time.Millisecond
	obj := newReceiver()
	runner := NewBoundedFrequencyRunner("test-runner", obj.F, minInterval, retryInterval, maxInterval)

	stopCh := make(chan struct{})
	defer close(stopCh)
	go runner.Loop(stopCh)

	// 没有显式 Run()，等待最大间隔到期自动执行。
	assertNoCalls(t, obj)

	time.Sleep(maxInterval)
	assertCalls(t, obj, 1)
}

// Test_BoundedFrequencyRunnerRetry_RealClock 用真实时钟验证失败后的重试周期。
func Test_BoundedFrequencyRunnerRetry_RealClock(t *testing.T) {
	// 真实时钟测试采用较短的间隔以控制测试耗时。
	var minInterval = 100 * time.Millisecond
	var retryInterval = 500 * time.Millisecond
	var maxInterval = 10 * time.Second

	obj := newReceiver()
	// 使用真实时钟的构造函数。
	runner := NewBoundedFrequencyRunner("test-runner-real-clock", obj.F, minInterval, retryInterval, maxInterval)

	stopCh := make(chan struct{})
	defer close(stopCh)
	go runner.Loop(stopCh)

	// 通过日志标记首次重试的触发点。
	t.Log("Triggering first retry")
	// 首次执行失败，排队一次重试。
	// rel=0s（相对时间，保持原标记以便对照）
	obj.setReturnValue(fmt.Errorf("sync error"))
	runner.Run()
	assertCalls(t, obj, 1)

	// 尚未到达重试间隔，不应执行。
	time.Sleep(retryInterval / 4)
	assertNoCalls(t, obj)

	// 超过重试间隔后应触发重试。
	time.Sleep(retryInterval) // Wait past retryInterval
	assertCalls(t, obj, 2)

	// 下一个周期相对于本批次首次 Retry 计算。
	time.Sleep(retryInterval)
	assertCalls(t, obj, 3)

	time.Sleep(retryInterval / 8)
	assertNoCalls(t, obj)

	// 超过新的重试间隔后再次触发。
	time.Sleep(retryInterval) // Wait past the new retryInterval
	assertCalls(t, obj, 4)
}
