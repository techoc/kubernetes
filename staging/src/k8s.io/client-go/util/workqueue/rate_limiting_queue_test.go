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
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// TestRateLimitingQueue 验证 AddRateLimited 的退避计算：指数退避按 item 独立累计
// （1ms、2ms…），Forget 后重新计数，NumRequeues 反映失败次数。
// 这里直接替换内部 delaying queue 以读取 readyAt，从而不依赖真实计时。
func TestRateLimitingQueue(t *testing.T) {
	// 下限 1ms、上限 1s，便于用 fake clock 精确断言前几次退避。
	limiter := NewItemExponentialFailureRateLimiter(1*time.Millisecond, 1*time.Second)
	queue := NewRateLimitingQueue(limiter).(*rateLimitingType[any])
	fakeClock := testingclock.NewFakeClock(time.Now())
	delayingQueue := &delayingType[any]{
		TypedInterface:  New(),
		clock:           fakeClock,
		heartbeat:       fakeClock.NewTicker(maxWait),
		stopCh:          make(chan struct{}),
		waitingForAddCh: make(chan *waitFor[any], 1000),
		metrics:         newRetryMetrics("", nil),
	}
	queue.TypedDelayingInterface = delayingQueue

	queue.AddRateLimited("one")
	waitEntry := <-delayingQueue.waitingForAddCh
	// 首次失败：退避 1ms。
	if e, a := 1*time.Millisecond, waitEntry.readyAt.Sub(fakeClock.Now()); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	queue.AddRateLimited("one")
	waitEntry = <-delayingQueue.waitingForAddCh
	// 第二次失败：同一 item 的退避翻倍为 2ms。
	if e, a := 2*time.Millisecond, waitEntry.readyAt.Sub(fakeClock.Now()); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, queue.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 不同 item 的计数彼此独立："two" 从 1ms 重新开始。
	queue.AddRateLimited("two")
	waitEntry = <-delayingQueue.waitingForAddCh
	if e, a := 1*time.Millisecond, waitEntry.readyAt.Sub(fakeClock.Now()); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	queue.AddRateLimited("two")
	waitEntry = <-delayingQueue.waitingForAddCh
	if e, a := 2*time.Millisecond, waitEntry.readyAt.Sub(fakeClock.Now()); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// Forget 清除失败计数，使下次失败重新从 1ms 开始。
	queue.Forget("one")
	if e, a := 0, queue.NumRequeues("one"); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	queue.AddRateLimited("one")
	waitEntry = <-delayingQueue.waitingForAddCh
	if e, a := 1*time.Millisecond, waitEntry.readyAt.Sub(fakeClock.Now()); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

}
