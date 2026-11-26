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

package flowcontrol

import (
	"fmt"
	"sync"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// TestDroppedRequestsTracker 验证丢弃请求对 Retry-After 的递增与衰减。
//
// 以假时钟按秒推进，精确断言每秒记录若干丢弃请求后 retryAfter 的
// 变化；同一秒内多次丢弃只应提升一次，避免瞬时抖动放大惩罚。
func TestDroppedRequestsTracker(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	tracker := newDroppedRequestsTracker(fakeClock.Now)

	// 下表按时间列出：
	// - 经过的秒数（自初始时间起算）
	// - 当前秒将记录为丢弃的请求数
	steps := []struct {
		secondsElapsed int
		// droppedRequests 是 secondsElapsed 之后要丢弃的请求数。
		droppedRequests int
		// retryAfter 是所有丢弃请求通过 RecordDroppedRequest
		// 记录后的期望 retryAfter。
		retryAfter int64
	}{
		{secondsElapsed: 0, droppedRequests: 5, retryAfter: 1},
		{secondsElapsed: 1, droppedRequests: 11, retryAfter: 2},
		// 即便短时间内多次丢弃请求，也不应立即提高 retryAfter。
		{secondsElapsed: 2, droppedRequests: 1, retryAfter: 2},
		{secondsElapsed: 3, droppedRequests: 11, retryAfter: 4},
		{secondsElapsed: 4, droppedRequests: 1, retryAfter: 4},
		{secondsElapsed: 7, droppedRequests: 1, retryAfter: 8},
		{secondsElapsed: 11, droppedRequests: 1, retryAfter: 8},
		{secondsElapsed: 15, droppedRequests: 1, retryAfter: 7},
		{secondsElapsed: 17, droppedRequests: 1, retryAfter: 6},
		{secondsElapsed: 21, droppedRequests: 14, retryAfter: 5},
		{secondsElapsed: 22, droppedRequests: 1, retryAfter: 10},
	}

	for i, step := range steps {
		secondsToAdvance := step.secondsElapsed
		if i > 0 {
			secondsToAdvance -= steps[i-1].secondsElapsed
		}
		fakeClock.Step(time.Duration(secondsToAdvance) * time.Second)

		// 记录所有丢弃的请求并重新计算 retryAfter。
		for r := 0; r < step.droppedRequests; r++ {
			tracker.RecordDroppedRequest("pl")
		}
		if retryAfter := tracker.GetRetryAfter("pl"); retryAfter != step.retryAfter {
			t.Errorf("Unexpected retryAfter: %v, expected: %v", retryAfter, step.retryAfter)
		}

	}
}

// TestDroppedRequestsTrackerPLIndependent 验证不同优先级（PL）之间互不影响。
//
// 一个 PL 的丢弃不应连带惩罚其他 PL，否则单个高优先级流量会拖慢
// 整个 apiserver；同时验证未出现过的 PL 返回基线值。
func TestDroppedRequestsTrackerPLIndependent(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	tracker := newDroppedRequestsTracker(fakeClock.Now)

	// 在多个 PL 中各报告一次丢弃请求。
	// 验证下一秒 RetryAfter 没有提高。
	for i := 0; i < 10; i++ {
		tracker.RecordDroppedRequest(fmt.Sprintf("pl-%d", i))
	}
	fakeClock.Step(time.Second)
	for i := 0; i < 10; i++ {
		tracker.RecordDroppedRequest(fmt.Sprintf("pl-%d", i))
		retryAfter := tracker.GetRetryAfter(fmt.Sprintf("pl-%d", i))
		if retryAfter != 1 {
			t.Errorf("Unexpected retryAfter for pl-%d: %v", i, retryAfter)
		}
	}

	// 在单个 PL 上记录少量丢弃请求。
	// 验证只有该 PL 的 RetryAfter 提高。
	for i := 0; i < 5; i++ {
		tracker.RecordDroppedRequest("pl-0")
	}
	fakeClock.Step(time.Second)
	for i := 0; i < 10; i++ {
		tracker.RecordDroppedRequest(fmt.Sprintf("pl-%d", i))
		retryAfter := tracker.GetRetryAfter(fmt.Sprintf("pl-%d", i))
		switch i {
		case 0:
			if retryAfter != 2 {
				t.Errorf("Unexpected retryAfter for pl-0: %v", retryAfter)
			}
		default:
			if retryAfter != 1 {
				t.Errorf("Unexpected retryAfter for pl-%d: %v", i, retryAfter)
			}
		}
	}
	// 同时验证没有丢弃请求的 PL。
	if retryAfter := tracker.GetRetryAfter("other-pl"); retryAfter != 1 {
		t.Errorf("Unexpected retryAfter for other-pl: %v", retryAfter)
	}
}

// BenchmarkDroppedRequestsTracker 衡量多 PL 并发记录丢弃请求的开销。
//
// tracker 位于请求拒绝路径上，任何锁竞争都会直接放大过载影响，
// 因此用并发基准守住性能。
func BenchmarkDroppedRequestsTracker(b *testing.B) {
	b.StopTimer()

	fakeClock := testingclock.NewFakeClock(time.Now())
	tracker := newDroppedRequestsTracker(fakeClock.Now)

	startCh := make(chan struct{})
	wg := sync.WaitGroup{}
	numPLs := 5
	// 为所有 `numPLs` 优先级创建 b.N 个 worker，
	// 各自每 100ms 尝试记录一次丢弃请求，
	// 并带随机抖动。
	for i := 0; i < numPLs; i++ {
		plName := fmt.Sprintf("priority-level-%d", i)
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh

				for a := 0; a < 5; a++ {
					tracker.RecordDroppedRequest(plName)
					time.Sleep(25 * time.Millisecond)
				}
			}()
		}
	}
	// 推进时间的 goroutine。
	stopCh := make(chan struct{})
	timeWg := sync.WaitGroup{}
	timeWg.Add(1)
	go func() {
		defer timeWg.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(25 * time.Millisecond):
				fakeClock.Step(time.Second)
			}
		}
	}()

	b.StartTimer()
	close(startCh)
	wg.Wait()

	close(stopCh)
	timeWg.Wait()
}
