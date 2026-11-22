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

package progress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

var (
	pollPeriod      = time.Millisecond
	minimalNoChange = 20 * time.Millisecond
	pollTimeout     = 5 * time.Second
)

// TestConditionalProgressRequester 验证条件式进度请求器只在有 watcher 等待时
// 才周期性向 etcd 发送 progress 请求，并在 stopCh 关闭后停止发送。
// 使用假时钟 + 轮询断言，避免依赖真实时间产生偶发失败。
func TestConditionalProgressRequester(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	clock := testingclock.NewFakeClock(time.Now())
	pr := newTestConditionalProgressRequester(clock)
	stopCh := make(chan struct{})
	go pr.Run(stopCh)
	var wantRequestsSent int32
	var requestsSent int32

	// 等待 Run 内部创建 ticker，否则后续 Step 无法推进循环。
	logger.Info("Wait for ticker to be created")
	for !clock.HasWaiters() {
		time.Sleep(pollPeriod)
	}

	// 无 waiter 时，时间推进也不应产生 progress 请求。
	logger.Info("No progress requests if no-one is waiting")
	clock.Step(progressRequestPeriod * 2)

	if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
		requestsSent = pr.progressRequestsSentCount.Load()
		return requestsSent == wantRequestsSent
	}); err != nil {
		t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
	}

	// 增加 waiter 后，下一个周期应发出请求。
	logger.Info("Adding waiters allows progress request to be sent")
	pr.Add()
	wantRequestsSent++
	if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
		requestsSent = pr.progressRequestsSentCount.Load()
		return requestsSent == wantRequestsSent
	}); err != nil {
		t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
	}

	// 有 waiter 时应每个周期持续发送。
	logger.Info("Periodically request progress to be sent every period")
	for wantRequestsSent < 10 {
		clock.Step(progressRequestPeriod)
		wantRequestsSent++

		if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
			requestsSent = pr.progressRequestsSentCount.Load()
			return requestsSent == wantRequestsSent
		}); err != nil {
			t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
		}
	}
	pr.Remove()

	// 移除所有 waiter 后应停止发送。
	logger.Info("No progress requests if no-one is waiting")
	clock.Step(progressRequestPeriod * 2)
	if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
		requestsSent = pr.progressRequestsSentCount.Load()
		return requestsSent == wantRequestsSent
	}); err != nil {
		t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
	}

	// Run 退出后即使新增 waiter 也不应再发送，验证 stopCh 的终止语义。
	logger.Info("No progress after stopping")
	close(stopCh)
	if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
		requestsSent = pr.progressRequestsSentCount.Load()
		return requestsSent == wantRequestsSent
	}); err != nil {
		t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
	}
	pr.Add()
	clock.Step(progressRequestPeriod * 2)
	if err := pollConditionNoChange(pollPeriod, minimalNoChange, pollTimeout, func() bool {
		requestsSent = pr.progressRequestsSentCount.Load()
		return requestsSent == wantRequestsSent
	}); err != nil {
		t.Fatalf("Failed to wait progress requests, err: %s, want: %d , got %d", err, wantRequestsSent, requestsSent)
	}
}

// newTestConditionalProgressRequester 构造带计数功能的测试替身，
// 用 RequestWatchProgress 统计实际发出的 progress 请求数。
func newTestConditionalProgressRequester(clock clock.WithTicker) *testConditionalProgressRequester {
	pr := &testConditionalProgressRequester{}
	pr.ConditionalProgressRequester = NewConditionalProgressRequester(pr.RequestWatchProgress, clock, nil)
	return pr
}

// testConditionalProgressRequester 嵌入真实实现并记录回调次数。
type testConditionalProgressRequester struct {
	*ConditionalProgressRequester
	progressRequestsSentCount atomic.Int32
}

// RequestWatchProgress 是对外请求的桩，只累加计数并立即成功返回。
func (pr *testConditionalProgressRequester) RequestWatchProgress(ctx context.Context) error {
	pr.progressRequestsSentCount.Add(1)
	return nil
}

// pollConditionNoChange 等待 condition 连续 stable/interval+1 次为真才返回成功。
// “连续稳定”而非“一次为真”，是为了排除 goroutine 尚未消费时钟推进的竞态。
func pollConditionNoChange(interval, stable, timeout time.Duration, condition func() bool) error {
	passCounter := 0
	requiredNumberOfPasses := int(stable/interval) + 1
	return wait.Poll(interval, timeout, func() (done bool, err error) {
		if condition() {
			passCounter++
		} else {
			passCounter = 0
		}
		return passCounter >= requiredNumberOfPasses, nil
	})
}
