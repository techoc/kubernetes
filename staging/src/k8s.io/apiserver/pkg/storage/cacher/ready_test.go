/*
Copyright 2022 The Kubernetes Authors.

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

package cacher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// Test_newReady 验证 ready.wait 在 setReady 之前会阻塞，之后全部等待者成功返回。
func Test_newReady(t *testing.T) {
	errCh := make(chan error, 10)
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	ready.setError(nil)
	// 创建 10 个等待 ready 的 goroutine
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- ready.wait(context.Background())
		}()
	}
	select {
	case <-time.After(1 * time.Second):
	case <-errCh:
		t.Errorf("ready should be blocking")
	}
	ready.setReady()
	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("unexpected error on channel %d", i)
		}
	}
}

// Test_newReadySetIdempotent 验证 setError/setReady 的幂等性与 generation 递增规则：
// 重复调用不应改变状态，只有 pending->ready 的跃迁才递增 generation。
func Test_newReadySetIdempotent(t *testing.T) {
	errCh := make(chan error, 10)
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	ready.setError(nil)
	ready.setError(nil)
	ready.setError(nil)
	if generation, _, err := ready.checkAndReadGeneration(); generation != 0 || err == nil {
		t.Errorf("unexpected state: generation=%v ready=%v", generation, err)
	}
	ready.setReady()
	if generation, _, err := ready.checkAndReadGeneration(); generation != 1 || err != nil {
		t.Errorf("unexpected state: generation=%v ready=%v", generation, err)
	}
	ready.setReady()
	ready.setReady()
	if generation, _, err := ready.checkAndReadGeneration(); generation != 1 || err != nil {
		t.Errorf("unexpected state: generation=%v ready=%v", generation, err)
	}
	ready.setError(nil)
	// 创建 10 个等待 ready 与 stop 的 goroutine
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- ready.wait(context.Background())
		}()
	}
	select {
	case <-time.After(1 * time.Second):
	case <-errCh:
		t.Errorf("ready should be blocking")
	}
	ready.setReady()
	if generation, _, err := ready.checkAndReadGeneration(); generation != 2 || err != nil {
		t.Errorf("unexpected state: generation=%v ready=%v", generation, err)
	}
	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("unexpected error on channel %d", i)
		}
	}
}

// Test_newReadyRacy 随机执行所有可能的状态转换。
// 必须在开启 race detector 的情况下运行。
// 该用例用于让 -race 覆盖 ready 的全部状态转换路径。
func Test_newReadyRacy(t *testing.T) {
	concurrency := 1000
	errCh := make(chan error, concurrency)
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	ready.setError(nil)

	wg := sync.WaitGroup{}
	wg.Add(2 * concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			errCh <- ready.wait(context.Background())
		}()
		go func() {
			defer wg.Done()
			ready.setError(nil)
		}()
		go func() {
			defer wg.Done()
			ready.setReady()
		}()
	}
	// 最后一个必须设为 true。
	wg.Wait()
	ready.setReady()

	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("unexpected error %v on channel %d", err, i)
		}
	}
}

// Test_newReadyStop 验证 stop 会让所有等待者以错误返回，而不是永久阻塞。
func Test_newReadyStop(t *testing.T) {
	errCh := make(chan error, 10)
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	ready.setError(nil)
	// 创建 10 个等待 ready 与 stop 的 goroutine
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- ready.wait(context.Background())
		}()
	}
	select {
	case <-time.After(1 * time.Second):
	case <-errCh:
		t.Errorf("ready should be blocking")
	}
	ready.stop()
	for i := 0; i < 10; i++ {
		if err := <-errCh; err == nil {
			t.Errorf("unexpected success on channel %d", i)
		}
	}
}

// Test_newReadyCheck 验证非阻塞 check 在初始、就绪、停止三种状态下的返回。
func Test_newReadyCheck(t *testing.T) {
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	// 初始状态为 false
	if _, err := ready.check(); err == nil {
		t.Errorf("unexpected ready state %v", err)
	}
	ready.setReady()
	if _, err := ready.check(); err != nil {
		t.Errorf("unexpected ready state %v", err)
	}
	// stop 将 ready 置为 false
	ready.stop()
	if _, err := ready.check(); err == nil {
		t.Errorf("unexpected ready state %v", err)
	}
	// 已 stop 后不能再置为 true
	ready.setReady()
	if _, err := ready.check(); err == nil {
		t.Errorf("unexpected ready state %v", err)
	}
	err := ready.wait(context.Background())
	if err == nil {
		t.Errorf("expected error waiting on a stopped state")
	}
}

// Test_newReadyCancelPending 验证等待者可被 context 取消唤醒，避免请求无法中止。
func Test_newReadyCancelPending(t *testing.T) {
	errCh := make(chan error, 10)
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	ready.setError(nil)
	ctx, cancel := context.WithCancel(context.Background())
	// 创建 10 个卡在 pending 上的 goroutine
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- ready.wait(ctx)
		}()
	}
	select {
	case <-time.After(1 * time.Second):
	case <-errCh:
		t.Errorf("ready should be blocking")
	}
	cancel()
	for i := 0; i < 10; i++ {
		if err := <-errCh; err == nil {
			t.Errorf("unexpected success on channel %d", i)
		}
	}
}

// Test_newReadyStateChangeTimestamp 验证 check 返回的“距上次状态变更的时长”随时钟推进而重置。
func Test_newReadyStateChangeTimestamp(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	fakeClock.SetTime(time.Now())

	ready := newReady(fakeClock)
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, time.Minute)

	ready.setReady()
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, time.Minute)
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, 2*time.Minute)

	ready.setError(nil)
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, time.Minute)
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, 2*time.Minute)

	ready.setReady()
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, time.Minute)

	ready.stop()
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, time.Minute)
	fakeClock.Step(time.Minute)
	checkReadyTransitionTime(t, ready, 2*time.Minute)
}

// checkReadyTransitionTime 断言 ready 报告的距上次状态变更时长。
func checkReadyTransitionTime(t *testing.T, r *ready, expectedLastStateChangeDuration time.Duration) {
	if lastStateChangeDuration, _ := r.check(); lastStateChangeDuration != expectedLastStateChangeDuration {
		t.Errorf("unexpected last state change duration: %v, expected: %v", lastStateChangeDuration, expectedLastStateChangeDuration)
	}
}

// TestReadyError 验证未就绪时错误文案包含底层错误原因，便于定位存储问题。
func TestReadyError(t *testing.T) {
	ready := newReady(testingclock.NewFakeClock(time.Now()))
	_, _, err := ready.checkAndReadGeneration()
	if err == nil || err.Error() != "storage is (re)initializing" {
		t.Errorf("Unexpected error when unready, got %q", err)
	}
	ready.setError(errors.New("etcd is down"))
	_, _, err = ready.checkAndReadGeneration()
	if err == nil || err.Error() != "storage is (re)initializing: etcd is down" {
		t.Errorf("Unexpected error when unready, got %q", err)
	}
	ready.setError(nil)
	_, _, err = ready.checkAndReadGeneration()
	if err == nil || err.Error() != "storage is (re)initializing" {
		t.Errorf("Unexpected error when unready, got %q", err)
	}
}
