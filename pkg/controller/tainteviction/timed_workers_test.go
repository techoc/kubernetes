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

package tainteviction

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/klog/v2/ktesting"
	testingclock "k8s.io/utils/clock/testing"
)

// TestExecute 验证立即触发的工作项都会被处理。
// 使用工作队列的同步执行语义：每个 AddWork 对应一次回调，计数应为工作项数量。
func TestExecute(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	testVal := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(5)
	queue := CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		atomic.AddInt32(&testVal, 1)
		wg.Done()
		return nil
	})
	now := time.Now()
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, now)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, now)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, now)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, now)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, now)
	wg.Wait()
	lastVal := atomic.LoadInt32(&testVal)
	if lastVal != 5 {
		t.Errorf("Expected testVal = 5, got %v", lastVal)
	}
}

// TestExecuteDelayed 验证延迟工作项的去重语义：
// 对相同 key 重复添加不会累积多次执行，时间推进后每个 key 只触发一次回调。
// 使用假时钟避免测试真的等待 10 秒。
func TestExecuteDelayed(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	testVal := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(5)
	queue := CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		atomic.AddInt32(&testVal, 1)
		wg.Done()
		return nil
	})
	now := time.Now()
	then := now.Add(10 * time.Second)
	fakeClock := testingclock.NewFakeClock(now)
	queue.clock = fakeClock
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	fakeClock.Step(11 * time.Second)
	wg.Wait()
	lastVal := atomic.LoadInt32(&testVal)
	if lastVal != 5 {
		t.Errorf("Expected testVal = 5, got %v", lastVal)
	}
}

// TestCancel 验证取消工作项后该 key 不再触发回调。
// 取消必须在假时钟推进之前完成，才能证明是取消而非超时导致的跳过。
func TestCancel(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testVal := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(3)
	queue := CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		atomic.AddInt32(&testVal, 1)
		wg.Done()
		return nil
	})
	now := time.Now()
	then := now.Add(10 * time.Second)
	fakeClock := testingclock.NewFakeClock(now)
	queue.clock = fakeClock
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	queue.CancelWork(logger, NewWorkArgs("2", "2").KeyFromWorkArgs())
	queue.CancelWork(logger, NewWorkArgs("4", "4").KeyFromWorkArgs())
	fakeClock.Step(11 * time.Second)
	wg.Wait()
	lastVal := atomic.LoadInt32(&testVal)
	if lastVal != 3 {
		t.Errorf("Expected testVal = 3, got %v", lastVal)
	}
}

// TestCancelAndRead 验证被取消的 key 再次添加后可以重新生效：
// 取消只影响当时排队的工作项，不应永久屏蔽相同 key 的后续调度。
func TestCancelAndRead(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testVal := int32(0)
	wg := sync.WaitGroup{}
	wg.Add(4)
	queue := CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		atomic.AddInt32(&testVal, 1)
		wg.Done()
		return nil
	})
	now := time.Now()
	then := now.Add(10 * time.Second)
	fakeClock := testingclock.NewFakeClock(now)
	queue.clock = fakeClock
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	queue.AddWork(ctx, NewWorkArgs("1", "1"), now, then)
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	queue.AddWork(ctx, NewWorkArgs("3", "3"), now, then)
	queue.AddWork(ctx, NewWorkArgs("4", "4"), now, then)
	queue.AddWork(ctx, NewWorkArgs("5", "5"), now, then)
	queue.CancelWork(logger, NewWorkArgs("2", "2").KeyFromWorkArgs())
	queue.CancelWork(logger, NewWorkArgs("4", "4").KeyFromWorkArgs())
	queue.AddWork(ctx, NewWorkArgs("2", "2"), now, then)
	fakeClock.Step(11 * time.Second)
	wg.Wait()
	lastVal := atomic.LoadInt32(&testVal)
	if lastVal != 4 {
		t.Errorf("Expected testVal = 4, got %v", lastVal)
	}
}
