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

package wait

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// TestUntil 验证 Until 在停止通道已关闭时不调用回调，在运行中关闭后能正常退出。
func TestUntil(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	Until(func() {
		t.Fatal("should not have been invoked")
	}, 0, ch)

	ch = make(chan struct{})
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		Until(func() {
			called <- struct{}{}
		}, 0, ch)
		close(called)
	}()
	<-called
	close(ch)
	<-called
	wg.Wait()
}

// TestUntilWithContext 验证 UntilWithContext 通过 context 取消驱动退出。
func TestUntilWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	UntilWithContext(ctx, func(context.Context) {
		t.Fatal("should not have been invoked")
	}, 0)

	ctx, cancel = context.WithCancel(context.TODO())
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		UntilWithContext(ctx, func(context.Context) {
			called <- struct{}{}
		}, 0)
		close(called)
	}()
	<-called
	cancel()
	<-called
	wg.Wait()
}

// TestNonSlidingUntil 验证非滑动窗口版本的 Until 的启动与退出行为。
func TestNonSlidingUntil(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	NonSlidingUntil(func() {
		t.Fatal("should not have been invoked")
	}, 0, ch)

	ch = make(chan struct{})
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		NonSlidingUntil(func() {
			called <- struct{}{}
		}, 0, ch)
		close(called)
	}()
	<-called
	close(ch)
	<-called
	wg.Wait()
}

// TestNonSlidingUntilWithContext 验证非滑动版本的 UntilWithContext。
func TestNonSlidingUntilWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	NonSlidingUntilWithContext(ctx, func(context.Context) {
		t.Fatal("should not have been invoked")
	}, 0)

	ctx, cancel = context.WithCancel(context.TODO())
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		NonSlidingUntilWithContext(ctx, func(context.Context) {
			called <- struct{}{}
		}, 0)
		close(called)
	}()
	<-called
	cancel()
	<-called
	wg.Wait()
}

// TestUntilReturnsImmediately 验证回调内部关闭停止通道时，Until 立即返回而不等下一次间隔。
func TestUntilReturnsImmediately(t *testing.T) {
	now := time.Now()
	ch := make(chan struct{})
	var attempts int
	Until(func() {
		attempts++
		if attempts > 1 {
			t.Fatalf("invoked after close of channel")
		}
		close(ch)
	}, 30*time.Second, ch)
	if now.Add(25 * time.Second).Before(time.Now()) {
		t.Errorf("Until did not return immediately when the stop chan was closed inside the func")
	}
}

// TestJitterUntil 验证抖动循环在停止通道关闭后立即返回、在运行中关闭后正常退出。
func TestJitterUntil(t *testing.T) {
	ch := make(chan struct{})
	//channel 关闭时 JitterUntil 不再调用函数 f，
	//并立即返回。
	// 停止通道已关闭时 JitterUntil 不调用回调并立即返回。
	close(ch)
	JitterUntil(func() {
		t.Fatal("should not have been invoked")
	}, 0, 1.0, true, ch)

	ch = make(chan struct{})
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		JitterUntil(func() {
			called <- struct{}{}
		}, 0, 1.0, true, ch)
		close(called)
	}()
	<-called
	close(ch)
	<-called
	wg.Wait()
}

// TestJitterUntilWithContext 验证抖动循环的 context 取消路径。
func TestJitterUntilWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	JitterUntilWithContext(ctx, func(context.Context) {
		t.Fatal("should not have been invoked")
	}, 0, 1.0, true)

	ctx, cancel = context.WithCancel(context.TODO())
	called := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		JitterUntilWithContext(ctx, func(context.Context) {
			called <- struct{}{}
		}, 0, 1.0, true)
		close(called)
	}()
	<-called
	cancel()
	<-called
	wg.Wait()
}

// TestJitterUntilReturnsImmediately 验证回调内关闭通道时抖动循环立即返回。
func TestJitterUntilReturnsImmediately(t *testing.T) {
	now := time.Now()
	ch := make(chan struct{})
	JitterUntil(func() {
		close(ch)
	}, 30*time.Second, 1.0, true, ch)
	if now.Add(25 * time.Second).Before(time.Now()) {
		t.Errorf("JitterUntil did not return immediately when the stop chan was closed inside the func")
	}
}

// TestJitterUntilRecoversPanic 验证回调 panic 会被运行时 panic 处理器恢复，
// 循环继续执行而不是让进程崩溃。测试临时替换全局 crash 配置并在结束后恢复。
func TestJitterUntilRecoversPanic(t *testing.T) {
	// 保存并恢复全局 crash 处理器，避免影响其他测试。
	originalReallyCrash := runtime.ReallyCrash
	originalHandlers := runtime.PanicHandlers
	defer func() {
		runtime.ReallyCrash = originalReallyCrash
		runtime.PanicHandlers = originalHandlers
	}()

	called := 0
	handled := 0

	// 挂载自定义 panic 处理器，确认回调 panic 时它会被调用。
	runtime.ReallyCrash = false
	runtime.PanicHandlers = []func(context.Context, interface{}){
		func(_ context.Context, p interface{}) {
			handled++
		},
	}

	ch := make(chan struct{})
	JitterUntil(func() {
		called++
		if called > 2 {
			close(ch)
			return
		}
		panic("TestJitterUntilRecoversPanic")
	}, time.Millisecond, 1.0, true, ch)

	if called != 3 {
		t.Errorf("Expected panic recovers")
	}
}

// TestJitterUntilNegativeFactor 验证负抖动因子退化为固定间隔，关闭通道后能及时返回。
func TestJitterUntilNegativeFactor(t *testing.T) {
	now := time.Now()
	ch := make(chan struct{})
	called := make(chan struct{})
	received := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		JitterUntil(func() {
			called <- struct{}{}
			<-received
		}, time.Second, -30.0, true, ch)
	}()
	//第一轮循环
	<-called
	received <- struct{}{}
	//第二轮循环
	<-called
	close(ch)
	received <- struct{}{}

	//耗时最多为 2 秒加少量开销，而不是 3 秒。
	if now.Add(3 * time.Second).Before(time.Now()) {
		t.Errorf("JitterUntil did not returned after predefined period with negative jitter factor when the stop chan was closed inside the func")
	}
	wg.Wait()
}

// TestExponentialBackoff 验证指数退避的各终止路径：
// 默认零步数立即超时、步数耗尽、条件成功、错误传播与多步后成功。
func TestExponentialBackoff(t *testing.T) {
	//立即退出
	// 零步数时直接返回超时。
	i := 0
	err := ExponentialBackoff(Backoff{Factor: 1.0}, func() (bool, error) {
		i++
		return false, nil
	})
	if err != ErrWaitTimeout || i != 0 {
		t.Errorf("unexpected error: %v", err)
	}

	opts := Backoff{Factor: 1.0, Steps: 3}

	//最多等待 steps 次
	// 最多执行 Steps 次后以超时结束。
	i = 0
	err = ExponentialBackoff(opts, func() (bool, error) {
		i++
		return false, nil
	})
	if err != ErrWaitTimeout || i != opts.Steps {
		t.Errorf("unexpected error: %v", err)
	}

	//立即返回
	// 条件首次返回 true 时立即成功返回。
	i = 0
	err = ExponentialBackoff(opts, func() (bool, error) {
		i++
		return true, nil
	})
	if err != nil || i != 1 {
		t.Errorf("unexpected error: %v", err)
	}

	//出错时立即返回
	// 条件返回错误时立即把该错误透传给调用方。
	testErr := fmt.Errorf("some other error")
	err = ExponentialBackoff(opts, func() (bool, error) {
		return false, testErr
	})
	if err != testErr {
		t.Errorf("unexpected error: %v", err)
	}

	//调用多次
	// 多次调用后条件成功，验证步进计数正确。
	i = 1
	err = ExponentialBackoff(opts, func() (bool, error) {
		if i < opts.Steps {
			i++
			return false, nil
		}
		return true, nil
	})
	if err != nil || i != opts.Steps {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestPoller 验证内部 poller 产生的 tick 数量有上限（最多 3 次）。
func TestPoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := poller(time.Millisecond, 2*time.Millisecond)
	ch := w(ctx)
	count := 0
DRAIN:
	for {
		select {
		case _, open := <-ch:
			if !open {
				break DRAIN
			}
			count++
		case <-time.After(ForeverTestTimeout):
			t.Errorf("unexpected timeout after poll")
		}
	}
	if count > 3 {
		t.Errorf("expected up to three values, got %d", count)
	}
}

// fakePoller 是可计数的假 tick 源：限制触发次数并记录已触发次数，
// 测试可等待其 goroutine 结束。
type fakePoller struct {
	max  int
	used int32 //通过原子操作访问
	wg   sync.WaitGroup
}

// fakeTicker 返回一个 waitFunc：在独立 goroutine 中最多产生 max 次 tick，
// done 关闭时提前退出；结束时调用 doneFunc 以便测试同步。
func fakeTicker(max int, used *int32, doneFunc func()) waitFunc {
	return func(done <-chan struct{}) <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			defer doneFunc()
			defer close(ch)
			for i := 0; i < max; i++ {
				select {
				case ch <- struct{}{}:
				case <-done:
					return
				}
				if used != nil {
					atomic.AddInt32(used, 1)
				}
			}
		}()
		return ch
	}
}

// GetwaitFunc 返回一个与 fakePoller 生命周期绑定的 waitFunc 并登记到 wg。
func (fp *fakePoller) GetwaitFunc() waitFunc {
	fp.wg.Add(1)
	return fakeTicker(fp.max, &fp.used, fp.wg.Done)
}

// TestPoll 验证 poll 在一次 tick 后停止：条件只调用一次、tick 也只消费一次。
func TestPoll(t *testing.T) {
	invocations := 0
	f := ConditionWithContextFunc(func(ctx context.Context) (bool, error) {
		invocations++
		return true, nil
	})
	fp := fakePoller{max: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := poll(ctx, false, fp.GetwaitFunc().WithContext(), f); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	fp.wg.Wait()
	if invocations != 1 {
		t.Errorf("Expected exactly one invocation, got %d", invocations)
	}
	used := atomic.LoadInt32(&fp.used)
	if used != 1 {
		t.Errorf("Expected exactly one tick, got %d", used)
	}
}

// TestPollError 验证条件函数返回错误时立即传播，不再等待后续 tick。
func TestPollError(t *testing.T) {
	expectedError := errors.New("Expected error")
	f := ConditionFunc(func() (bool, error) {
		return false, expectedError
	})
	fp := fakePoller{max: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := poll(ctx, false, fp.GetwaitFunc().WithContext(), f.WithContext()); err == nil || err != expectedError {
		t.Fatalf("Expected error %v, got none %v", expectedError, err)
	}
	fp.wg.Wait()
	used := atomic.LoadInt32(&fp.used)
	if used != 1 {
		t.Errorf("Expected exactly one tick, got %d", used)
	}
}

// TestPollImmediate 验证 immediate 模式先调用一次条件，且完全不消耗 tick。
func TestPollImmediate(t *testing.T) {
	invocations := 0
	f := ConditionFunc(func() (bool, error) {
		invocations++
		return true, nil
	})
	fp := fakePoller{max: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := poll(ctx, true, fp.GetwaitFunc().WithContext(), f.WithContext()); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	// immediate 模式不调用 waitFunc，因此无需等待 fp.wg。
	if invocations != 1 {
		t.Errorf("Expected exactly one invocation, got %d", invocations)
	}
	used := atomic.LoadInt32(&fp.used)
	if used != 0 {
		t.Errorf("Expected exactly zero ticks, got %d", used)
	}
}

// TestPollImmediateError 验证 immediate 模式下条件返回的错误立即传播。
func TestPollImmediateError(t *testing.T) {
	expectedError := errors.New("Expected error")
	f := ConditionFunc(func() (bool, error) {
		return false, expectedError
	})
	fp := fakePoller{max: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := poll(ctx, true, fp.GetwaitFunc().WithContext(), f.WithContext()); err == nil || err != expectedError {
		t.Fatalf("Expected error %v, got none %v", expectedError, err)
	}
	// immediate 模式不调用 waitFunc，因此无需等待 fp.wg。
	used := atomic.LoadInt32(&fp.used)
	if used != 0 {
		t.Errorf("Expected exactly zero ticks, got %d", used)
	}
}

// TestPollForever 验证无限轮询：条件持续返回 false 时不断触发，
// 一旦条件返回 true 便收敛，且其后的通知次数受控。
func TestPollForever(t *testing.T) {
	ch := make(chan struct{})
	errc := make(chan error, 1)
	done := make(chan struct{}, 1)
	complete := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		f := ConditionFunc(func() (bool, error) {
			ch <- struct{}{}
			select {
			case <-done:
				return true, nil
			default:
			}
			return false, nil
		})

		if err := PollInfinite(time.Microsecond, f); err != nil {
			errc <- fmt.Errorf("unexpected error %v", err)
		}

		close(ch)
		complete <- struct{}{}
	}()

	//确保条件已打开。
	// 确认条件已被首次调用（通道可读）。
	<-ch

	//确保 channel 发送事件。
	// 至少在若干次轮询间隔内持续收到通知。
	for i := 0; i < 10; i++ {
		select {
		case _, open := <-ch:
			if !open {
				if len(errc) != 0 {
					t.Fatalf("did not expect channel to be closed, %v", <-errc)
				}
				t.Fatal("did not expect channel to be closed")
			}
		case <-time.After(ForeverTestTimeout):
			t.Fatalf("channel did not return at least once within the poll interval")
		}
	}

	//从条件函数返回后最多发送一个轮询通知。
	// 条件返回 true 后最多再收到一次通知，随后通道应关闭。
	done <- struct{}{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			_, open := <-ch
			if !open {
				return
			}
		}
		t.Error("expected closed channel after two iterations")
	}()
	<-complete

	if len(errc) != 0 {
		t.Fatal(<-errc)
	}
	wg.Wait()
}

// Test_waitFor 验证 waitFor 的调用次数约定：成功路径只调用一次；
// 超时路径在等待结束后会额外调用一次条件。
func Test_waitFor(t *testing.T) {
	var invocations int
	testCases := map[string]struct {
		F       ConditionFunc
		Ticks   int
		Invoked int
		Err     bool
	}{
		"invoked once": {
			ConditionFunc(func() (bool, error) {
				invocations++
				return true, nil
			}),
			2,
			1,
			false,
		},
		"invoked and returns a timeout": {
			ConditionFunc(func() (bool, error) {
				invocations++
				return false, nil
			}),
			2,
			3, //waitFor() 的约定要求等待结束时再调用一次 func。
			// waitFor 的约定：等待结束时条件会再被调用一次。
			true,
		},
		"returns immediately on error": {
			ConditionFunc(func() (bool, error) {
				invocations++
				return false, errors.New("test")
			}),
			2,
			1,
			true,
		},
	}
	for k, c := range testCases {
		invocations = 0
		ticker := fakeTicker(c.Ticks, nil, func() {})
		err := func() error {
			done := make(chan struct{})
			defer close(done)
			ctx := ContextForChannel(done)
			return waitForWithContext(ctx, ticker.WithContext(), c.F.WithContext())
		}()
		switch {
		case c.Err && err == nil:
			t.Errorf("%s: Expected error, got nil", k)
			continue
		case !c.Err && err != nil:
			t.Errorf("%s: Expected no error, got: %#v", k, err)
			continue
		}
		if invocations != c.Invoked {
			t.Errorf("%s: Expected %d invocations, got %d", k, c.Invoked, invocations)
		}
	}
}

// Test_waitForWithEarlyClosing_waitFunc 测试 waitFunc 提前关闭 channel 时 WaitFor 的行为；
// 应始终返回 ErrWaitTimeout。
// 验证 waitFunc 提前关闭通道时，waitFor 仍立即返回 ErrWaitTimeout。
func Test_waitForWithEarlyClosing_waitFunc(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	ctx := ContextForChannel(stopCh)
	start := time.Now()
	err := waitForWithContext(ctx, func(ctx context.Context) <-chan struct{} {
		c := make(chan struct{})
		close(c)
		return c
	}, func(_ context.Context) (bool, error) {
		return false, nil
	})
	duration := time.Since(start)

	// waitFor 应立即返回，因此耗时接近 0。
	if duration >= ForeverTestTimeout/2 {
		t.Errorf("expected short timeout duration")
	}
	if err != ErrWaitTimeout {
		t.Errorf("expected ErrWaitTimeout from WaitFunc")
	}
}

// 验证 waitFor 收到已关闭的通道时也立即返回 ErrWaitTimeout。
func Test_waitForWithClosedChannel(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh)
	c := make(chan struct{})
	defer close(c)
	ctx := ContextForChannel(stopCh)

	start := time.Now()
	err := waitForWithContext(ctx, func(_ context.Context) <-chan struct{} {
		return c
	}, func(_ context.Context) (bool, error) {
		return false, nil
	})
	duration := time.Since(start)
	// waitFor 应立即返回，耗时接近 0。
	if duration >= ForeverTestTimeout/2 {
		t.Errorf("expected short timeout duration")
	}
	// 轮询间隔为 ForeverTestTimeout，因此最终应报 ErrWaitTimeout。
	if err != ErrWaitTimeout {
		t.Errorf("expected ErrWaitTimeout from WaitFunc")
	}
}

// 验证条件返回 true 后，waitForWithContext 会取消它传给 waitFunc 的 context，
// 使轮询 goroutine 得以退出。
func Test_waitForWithContextCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitFn := poller(time.Millisecond, ForeverTestTimeout)

	var ctxPassedToWait context.Context
	waitForWithContext(ctx, func(ctx context.Context) <-chan struct{} {
		ctxPassedToWait = ctx
		return waitFn(ctx)
	}, func(ctx context.Context) (bool, error) {
		time.Sleep(10 * time.Millisecond)
		return true, nil
	})
	// waitForWithContext 返回后，其创建的轮询 goroutine 应已退出（context 已被取消）。
	if ctxPassedToWait.Err() != context.Canceled {
		t.Errorf("expected the context passed to waitForWithContext to be closed with: %v, but got: %v", context.Canceled, ctxPassedToWait.Err())
	}
}

// TestPollUntil 验证 PollUntil 在 stopCh 关闭后结束轮询。
func TestPollUntil(t *testing.T) {
	stopCh := make(chan struct{})
	called := make(chan bool)
	pollDone := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		PollUntil(time.Microsecond, ConditionFunc(func() (bool, error) {
			called <- true
			return false, nil
		}), stopCh)

		close(pollDone)
	}()

	//确保只调用一次。
	// 确认条件至少被调用一次。
	<-called
	//这应触发 "done"。
	// 关闭 stopCh 触发退出。
	close(stopCh)

	go func() {
		//必要时释放条件函数。
		// 排空 called，避免条件函数因发送阻塞而无法退出。
		for range called {
		}
	}()

	//确保轮询已结束。
	// 等待轮询真正结束。
	<-pollDone
	close(called)
	wg.Wait()
}

// TestBackoff_Step 验证 Backoff.Step 的步进、封顶与抖动范围。
// 用固定随机种子多轮运行，避免抖动采样导致偶发失败。
func TestBackoff_Step(t *testing.T) {
	tests := []struct {
		initial *Backoff
		want    []time.Duration
	}{
		{initial: nil, want: []time.Duration{0, 0, 0, 0}},
		{initial: &Backoff{Duration: time.Second, Steps: -1}, want: []time.Duration{time.Second, time.Second, time.Second}},
		{initial: &Backoff{Duration: time.Second, Steps: 0}, want: []time.Duration{time.Second, time.Second, time.Second}},
		{initial: &Backoff{Duration: time.Second, Steps: 1}, want: []time.Duration{time.Second, time.Second, time.Second}},
		{initial: &Backoff{Duration: time.Second, Factor: 1.0, Steps: 1}, want: []time.Duration{time.Second, time.Second, time.Second}},
		{initial: &Backoff{Duration: time.Second, Factor: 2, Steps: 3}, want: []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}},
		{initial: &Backoff{Duration: time.Second, Factor: 2, Steps: 3, Cap: 3 * time.Second}, want: []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second}},
		{initial: &Backoff{Duration: time.Second, Factor: 2, Steps: 2, Cap: 3 * time.Second, Jitter: 0.5}, want: []time.Duration{2 * time.Second, 3 * time.Second, 3 * time.Second}},
		{initial: &Backoff{Duration: time.Second, Factor: 2, Steps: 6, Jitter: 4}, want: []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second}},
	}
	for seed := int64(0); seed < 5; seed++ {
		for _, tt := range tests {
			var initial *Backoff
			if tt.initial != nil {
				copied := *tt.initial
				initial = &copied
			} else {
				initial = nil
			}
			t.Run(fmt.Sprintf("%#v seed=%d", initial, seed), func(t *testing.T) {
				jitterRand = rand.New(rand.NewSource(seed)).Float64
				for i := 0; i < len(tt.want); i++ {
					got := initial.Step()
					t.Logf("[%d]=%s", i, got)
					if initial != nil && initial.Jitter > 0 {
						if got == tt.want[i] {
							//随机情况下极不可能发生。
							t.Errorf("Backoff.Step(%d) = %v, no jitter", i, got)
							continue
						}
						diff := float64(tt.want[i]-got) / float64(tt.want[i])
						if diff > initial.Jitter {
							t.Errorf("Backoff.Step(%d) = %v, want %v, outside range", i, got, tt.want)
							continue
						}
					} else {
						if got != tt.want[i] {
							t.Errorf("Backoff.Step(%d) = %v, want %v", i, got, tt.want)
							continue
						}
					}
				}
			})
		}
	}
}

// TestContextForChannel 验证关闭父通道会取消所有由它派生的子 context。
func TestContextForChannel(t *testing.T) {
	var wg sync.WaitGroup
	parentCh := make(chan struct{})
	done := make(chan struct{})

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := ContextForChannel(parentCh)
			<-ctx.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	//关闭父 channel 应取消所有子 context。
	close(parentCh)

	select {
	case <-done:
	case <-time.After(ForeverTestTimeout):
		t.Errorf("unexpected timeout waiting for parent to cancel child contexts")
	}
}

// TestExponentialBackoffManagerGetNextBackoff 验证指数退避管理器按倍数增长直至封顶，
// 且静默时间超过重置间隔后回到初始值。
func TestExponentialBackoffManagerGetNextBackoff(t *testing.T) {
	fc := testingclock.NewFakeClock(time.Now())
	backoff := NewExponentialBackoffManager(1, 10, 10, 2.0, 0.0, fc)
	durations := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for i := 0; i < len(durations); i++ {
		generatedBackoff := backoff.(*exponentialBackoffManagerImpl).getNextBackoff()
		if generatedBackoff != durations[i] {
			t.Errorf("unexpected %d-th backoff: %d, expecting %d", i, generatedBackoff, durations[i])
		}
	}

	fc.Step(11)
	resetDuration := backoff.(*exponentialBackoffManagerImpl).getNextBackoff()
	if resetDuration != 1 {
		t.Errorf("after reset, backoff should be 1, but got %d", resetDuration)
	}
}

// TestJitteredBackoffManagerGetNextBackoff 验证正抖动落在 [base, base+jitter] 区间，
// 负抖动退化为固定退避值。
func TestJitteredBackoffManagerGetNextBackoff(t *testing.T) {
	// 正抖动：结果在基础值与基础值加抖动之间。
	backoffMgr := NewJitteredBackoffManager(1, 1, testingclock.NewFakeClock(time.Now()))
	for i := 0; i < 5; i++ {
		backoff := backoffMgr.(*jitteredBackoffManagerImpl).getNextBackoff()
		if backoff < 1 || backoff > 2 {
			t.Errorf("backoff out of range: %d", backoff)
		}
	}

	// 负抖动应退化为固定退避。
	backoffMgr = NewJitteredBackoffManager(1, -1, testingclock.NewFakeClock(time.Now()))
	backoff := backoffMgr.(*jitteredBackoffManagerImpl).getNextBackoff()
	if backoff != 1 {
		t.Errorf("backoff should be 1, but got %d", backoff)
	}
}

// TestJitterBackoffManagerWithRealClock 用真实时钟验证抖动退避的实际等待时间不低于基础值。
func TestJitterBackoffManagerWithRealClock(t *testing.T) {
	backoffMgr := NewJitteredBackoffManager(1*time.Millisecond, 0, &clock.RealClock{})
	for i := 0; i < 5; i++ {
		start := time.Now()
		<-backoffMgr.Backoff().C()
		passed := time.Since(start)
		if passed < 1*time.Millisecond {
			t.Errorf("backoff should be at least 1ms, but got %s", passed.String())
		}
	}
}

// TestExponentialBackoffManagerWithRealClock 用真实时钟验证指数退避的实际等待时间序列。
func TestExponentialBackoffManagerWithRealClock(t *testing.T) {
	//退避至少为 1ms、2ms、4ms、8ms、10ms、10ms、10ms。
	// 期望的退避序列（毫秒）：1, 2, 4, 8, 10, 10, 10。
	durationFactors := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	backoffMgr := NewExponentialBackoffManager(1*time.Millisecond, 10*time.Millisecond, 1*time.Hour, 2.0, 0.0, &clock.RealClock{})

	for i := range durationFactors {
		start := time.Now()
		<-backoffMgr.Backoff().C()
		passed := time.Since(start)
		if passed < durationFactors[i]*time.Millisecond {
			t.Errorf("backoff should be at least %d ms, but got %s", durationFactors[i], passed.String())
		}
	}
}

// TestBackoffDelayWithResetExponential 验证 DelayWithReset 生成的退避序列
// 以及静默超过重置间隔后回到初始时长。
func TestBackoffDelayWithResetExponential(t *testing.T) {
	fc := testingclock.NewFakeClock(time.Now())
	backoff := Backoff{Duration: 1, Cap: 10, Factor: 2.0, Jitter: 0.0, Steps: 10}.DelayWithReset(fc, 10)
	durations := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for i := 0; i < len(durations); i++ {
		generatedBackoff := backoff()
		if generatedBackoff != durations[i] {
			t.Errorf("unexpected %d-th backoff: %d, expecting %d", i, generatedBackoff, durations[i])
		}
	}

	fc.Step(11)
	resetDuration := backoff()
	if resetDuration != 1 {
		t.Errorf("after reset, backoff should be 1, but got %d", resetDuration)
	}
}

// TestBackoffDelayWithResetEmpty 验证重置间隔为 0 时每次都立即重置到初始时长。
func TestBackoffDelayWithResetEmpty(t *testing.T) {
	fc := testingclock.NewFakeClock(time.Now())
	backoff := Backoff{Duration: 1, Cap: 10, Factor: 2.0, Jitter: 0.0, Steps: 10}.DelayWithReset(fc, 0)
	//resetInterval 为 0，因此立即重置为初始时长。
	durations := []time.Duration{1, 1, 1, 1, 1, 1, 1}
	for i := 0; i < len(durations); i++ {
		generatedBackoff := backoff()
		if generatedBackoff != durations[i] {
			t.Errorf("unexpected %d-th backoff: %d, expecting %d", i, generatedBackoff, durations[i])
		}
	}

	fc.Step(11)
	resetDuration := backoff()
	if resetDuration != 1 {
		t.Errorf("after reset, backoff should be 1, but got %d", resetDuration)
	}
}

// TestBackoffDelayWithResetJitter 验证 DelayWithReset 的抖动行为：
// 正抖动在区间内随机，负抖动退化为固定值。
func TestBackoffDelayWithResetJitter(t *testing.T) {
	// 正抖动。
	backoff := Backoff{Duration: 1, Jitter: 1}.DelayWithReset(testingclock.NewFakeClock(time.Now()), 0)
	for i := 0; i < 5; i++ {
		value := backoff()
		if value < 1 || value > 2 {
			t.Errorf("backoff out of range: %d", value)
		}
	}

	// 负抖动应退化为固定退避。
	backoff = Backoff{Duration: 1, Jitter: -1}.DelayWithReset(testingclock.NewFakeClock(time.Now()), 0)
	value := backoff()
	if value != 1 {
		t.Errorf("backoff should be 1, but got %d", value)
	}
}

// TestBackoffDelayWithResetWithRealClockJitter 用真实时钟验证固定退避的实际等待时间。
func TestBackoffDelayWithResetWithRealClockJitter(t *testing.T) {
	backoff := Backoff{Duration: 1 * time.Millisecond, Jitter: 0}.DelayWithReset(&clock.RealClock{}, 0)
	for i := 0; i < 5; i++ {
		start := time.Now()
		<-RealTimer(backoff()).C()
		passed := time.Since(start)
		if passed < 1*time.Millisecond {
			t.Errorf("backoff should be at least 1ms, but got %s", passed.String())
		}
	}
}

// TestBackoffDelayWithResetWithRealClockExponential 用真实时钟验证指数退避的等待时间序列。
func TestBackoffDelayWithResetWithRealClockExponential(t *testing.T) {
	//退避至少为 1ms、2ms、4ms、8ms、10ms、10ms、10ms。
	// 期望的退避序列（毫秒）：1, 2, 4, 8, 10, 10, 10。
	durationFactors := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	backoff := Backoff{Duration: 1 * time.Millisecond, Cap: 10 * time.Millisecond, Factor: 2.0, Jitter: 0.0, Steps: 10}.DelayWithReset(&clock.RealClock{}, 1*time.Hour)

	for i := range durationFactors {
		start := time.Now()
		<-RealTimer(backoff()).C()
		passed := time.Since(start)
		if passed < durationFactors[i]*time.Millisecond {
			t.Errorf("backoff should be at least %d ms, but got %s", durationFactors[i], passed.String())
		}
	}
}

// defaultContext 返回一个可取消的普通上下文。
func defaultContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// cancelledContext 返回一个已立即取消的上下文。
func cancelledContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}

// deadlinedContext 返回一个已到达截止时间的上下文，用于验证超时路径。
func deadlinedContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	for ctx.Err() != context.DeadlineExceeded {
		time.Sleep(501 * time.Microsecond)
	}
	return ctx, cancel
}

// TestExponentialBackoffWithContext 验证带 context 的指数退避在各场景下的尝试次数与错误：
// 步数耗尽、条件成功/失败、上下文取消/超时，以及在条件内取消上下文。
func TestExponentialBackoffWithContext(t *testing.T) {
	defaultCallback := func(_ int) (bool, error) {
		return false, nil
	}

	conditionErr := errors.New("condition failed")

	tests := []struct {
		name               string
		steps              int
		zeroDuration       bool
		context            func() (context.Context, context.CancelFunc)
		callback           func(calls int) (bool, error)
		cancelContextAfter int
		attemptsExpected   int
		errExpected        error
	}{
		{
			name:             "no attempts expected with zero backoff steps",
			steps:            0,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:             "condition returns false with single backoff step",
			steps:            1,
			callback:         defaultCallback,
			attemptsExpected: 1,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:  "condition returns true with single backoff step",
			steps: 1,
			callback: func(_ int) (bool, error) {
				return true, nil
			},
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:             "condition always returns false with multiple backoff steps",
			steps:            5,
			callback:         defaultCallback,
			attemptsExpected: 5,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:  "condition returns true after certain attempts with multiple backoff steps",
			steps: 5,
			callback: func(attempts int) (bool, error) {
				if attempts == 3 {
					return true, nil
				}
				return false, nil
			},
			attemptsExpected: 3,
			errExpected:      nil,
		},
		{
			name:  "condition returns error no further attempts expected",
			steps: 5,
			callback: func(_ int) (bool, error) {
				return true, conditionErr
			},
			attemptsExpected: 1,
			errExpected:      conditionErr,
		},
		{
			name:             "context already canceled no attempts expected",
			steps:            5,
			context:          cancelledContext,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      context.Canceled,
		},
		{
			name:             "context at deadline no attempts expected",
			steps:            5,
			context:          deadlinedContext,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      context.DeadlineExceeded,
		},
		{
			name:             "no attempts expected with zero backoff steps",
			steps:            0,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:             "condition returns false with single backoff step",
			steps:            1,
			callback:         defaultCallback,
			attemptsExpected: 1,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:  "condition returns true with single backoff step",
			steps: 1,
			callback: func(_ int) (bool, error) {
				return true, nil
			},
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:               "condition always returns false with multiple backoff steps but is cancelled at step 4",
			steps:              5,
			callback:           defaultCallback,
			attemptsExpected:   4,
			cancelContextAfter: 4,
			errExpected:        context.Canceled,
		},
		{
			name:         "condition returns true after certain attempts with multiple backoff steps and zero duration",
			steps:        5,
			zeroDuration: true,
			callback: func(attempts int) (bool, error) {
				if attempts == 3 {
					return true, nil
				}
				return false, nil
			},
			attemptsExpected: 3,
			errExpected:      nil,
		},
		{
			name:  "condition returns error no further attempts expected",
			steps: 5,
			callback: func(_ int) (bool, error) {
				return true, conditionErr
			},
			attemptsExpected: 1,
			errExpected:      conditionErr,
		},
		{
			name:             "context already canceled no attempts expected",
			steps:            5,
			context:          cancelledContext,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      context.Canceled,
		},
		{
			name:             "context at deadline no attempts expected",
			steps:            5,
			context:          deadlinedContext,
			callback:         defaultCallback,
			attemptsExpected: 0,
			errExpected:      context.DeadlineExceeded,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backoff := Backoff{
				Duration: 1 * time.Microsecond,
				Factor:   1.0,
				Steps:    test.steps,
			}
			if test.zeroDuration {
				backoff.Duration = 0
			}

			contextFn := test.context
			if contextFn == nil {
				contextFn = defaultContext
			}
			ctx, cancel := contextFn()
			defer cancel()

			attempts := 0
			err := ExponentialBackoffWithContext(ctx, backoff, func(_ context.Context) (bool, error) {
				attempts++
				defer func() {
					if test.cancelContextAfter > 0 && test.cancelContextAfter == attempts {
						cancel()
					}
				}()
				return test.callback(attempts)
			})

			if test.errExpected != err {
				t.Errorf("expected error: %v but got: %v", test.errExpected, err)
			}

			if test.attemptsExpected != attempts {
				t.Errorf("expected attempts count: %d but got: %d", test.attemptsExpected, attempts)
			}
		})
	}
}

// BenchmarkExponentialBackoffWithContext 测量零时长退避下每次调用的开销。
func BenchmarkExponentialBackoffWithContext(b *testing.B) {
	backoff := Backoff{
		Duration: 0,
		Factor:   0,
		Steps:    101,
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attempts := 0
		if err := ExponentialBackoffWithContext(ctx, backoff, func(_ context.Context) (bool, error) {
			attempts++
			return attempts >= 100, nil
		}); err != nil {
			b.Fatalf("unexpected err: %v", err)
		}
	}
}

// TestPollImmediateUntilWithContext 验证首次立即执行、随后轮询的语义：
// 错误/成功都会终止重试，上下文取消则返回 ErrWaitTimeout（保持向后兼容）。
func TestPollImmediateUntilWithContext(t *testing.T) {
	fakeErr := errors.New("my error")
	tests := []struct {
		name                         string
		condition                    func(int) ConditionWithContextFunc
		context                      func() (context.Context, context.CancelFunc)
		cancelContextAfterNthAttempt int
		errExpected                  error
		attemptsExpected             int
	}{
		{
			name: "condition throws error on immediate attempt, no retry is attempted",
			condition: func(int) ConditionWithContextFunc {
				return func(context.Context) (done bool, err error) {
					return false, fakeErr
				}
			},
			errExpected:      fakeErr,
			attemptsExpected: 1,
		},
		{
			name: "condition returns done=true on immediate attempt, no retry is attempted",
			condition: func(int) ConditionWithContextFunc {
				return func(context.Context) (done bool, err error) {
					return true, nil
				}
			},
			errExpected:      nil,
			attemptsExpected: 1,
		},
		{
			name: "condition returns done=false on immediate attempt, context is already cancelled, no retry is attempted",
			condition: func(int) ConditionWithContextFunc {
				return func(context.Context) (done bool, err error) {
					return false, nil
				}
			},
			context:          cancelledContext,
			errExpected:      ErrWaitTimeout, //此处本应为 context.Canceled，但会破坏假定所有错误都是 ErrWaitTimeout 的调用方。
			attemptsExpected: 1,
		},
		{
			name: "condition returns done=false on immediate attempt, context is not cancelled, retry is attempted",
			condition: func(attempts int) ConditionWithContextFunc {
				return func(context.Context) (done bool, err error) {
					//让前三次尝试失败，最后一次成功。
					if attempts <= 3 {
						return false, nil
					}
					return true, nil
				}
			},
			errExpected:      nil,
			attemptsExpected: 4,
		},
		{
			name: "condition always returns done=false, context gets cancelled after N attempts",
			condition: func(attempts int) ConditionWithContextFunc {
				return func(ctx context.Context) (done bool, err error) {
					return false, nil
				}
			},
			cancelContextAfterNthAttempt: 4,
			errExpected:                  ErrWaitTimeout, //此处本应为 context.Canceled，但此方法不能改变该行为。
			attemptsExpected:             4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contextFn := test.context
			if contextFn == nil {
				contextFn = defaultContext
			}
			ctx, cancel := contextFn()
			defer cancel()

			var attempts int
			conditionWrapper := func(ctx context.Context) (done bool, err error) {
				attempts++
				defer func() {
					if test.cancelContextAfterNthAttempt == attempts {
						cancel()
					}
				}()

				c := test.condition(attempts)
				return c(ctx)
			}

			err := PollImmediateUntilWithContext(ctx, time.Millisecond, conditionWrapper)
			if test.errExpected != err {
				t.Errorf("Expected error: %v, but got: %v", test.errExpected, err)
			}
			if test.attemptsExpected != attempts {
				t.Errorf("Expected ConditionFunc to be invoked: %d times, but got: %d", test.attemptsExpected, attempts)
			}
		})
	}
}

// Test_waitForWithContext 验证 waitForWithContext 在各终止路径下的尝试次数与错误，
// 包括成功、超时、条件错误与上下文取消。
func Test_waitForWithContext(t *testing.T) {
	fakeErr := errors.New("fake error")
	tests := []struct {
		name             string
		context          func() (context.Context, context.CancelFunc)
		condition        ConditionWithContextFunc
		waitFunc         func() waitFunc
		attemptsExpected int
		errExpected      error
	}{
		{
			name:    "condition returns done=true on first attempt, no retry is attempted",
			context: defaultContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return true, nil
			}),
			waitFunc:         func() waitFunc { return fakeTicker(2, nil, func() {}) },
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:    "condition always returns done=false, timeout error expected",
			context: defaultContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc { return fakeTicker(2, nil, func() {}) },
			//waitForWithContext() 的约定要求等待结束时再调用一次 func。
			attemptsExpected: 3,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:    "condition returns an error on first attempt, the error is returned",
			context: defaultContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, fakeErr
			}),
			waitFunc:         func() waitFunc { return fakeTicker(2, nil, func() {}) },
			attemptsExpected: 1,
			errExpected:      fakeErr,
		},
		{
			name:    "context is cancelled, context cancelled error expected",
			context: cancelledContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					//此 channel 不产生 tick。
					return ch
				}
			},
			attemptsExpected: 0,
			errExpected:      ErrWaitTimeout,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts int
			conditionWrapper := func(ctx context.Context) (done bool, err error) {
				attempts++
				return test.condition(ctx)
			}

			ticker := test.waitFunc()
			err := func() error {
				contextFn := test.context
				if contextFn == nil {
					contextFn = defaultContext
				}
				ctx, cancel := contextFn()
				defer cancel()

				return waitForWithContext(ctx, ticker.WithContext(), conditionWrapper)
			}()

			if test.errExpected != err {
				t.Errorf("Expected error: %v, but got: %v", test.errExpected, err)
			}
			if test.attemptsExpected != attempts {
				t.Errorf("Expected %d invocations, got %d", test.attemptsExpected, attempts)
			}
		})
	}
}

// Test_poll 验证 poll 在 immediate、上下文取消、tick 关闭等场景下的行为与错误约定。
func Test_poll(t *testing.T) {
	fakeErr := errors.New("fake error")
	tests := []struct {
		name               string
		context            func() (context.Context, context.CancelFunc)
		immediate          bool
		waitFunc           func() waitFunc
		condition          ConditionWithContextFunc
		cancelContextAfter int
		attemptsExpected   int
		errExpected        error
	}{
		{
			name:      "immediate is true, condition returns an error",
			immediate: true,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, fakeErr
			}),
			waitFunc:         nil,
			attemptsExpected: 1,
			errExpected:      fakeErr,
		},
		{
			name:      "immediate is true, condition returns true",
			immediate: true,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return true, nil
			}),
			waitFunc:         nil,
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:      "immediate is true, context is cancelled, condition return false",
			immediate: true,
			context:   cancelledContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc:         nil,
			attemptsExpected: 1,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:      "immediate is false, context is cancelled",
			immediate: false,
			context:   cancelledContext,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc:         nil,
			attemptsExpected: 0,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:      "immediate is false, condition returns an error",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, fakeErr
			}),
			waitFunc:         func() waitFunc { return fakeTicker(5, nil, func() {}) },
			attemptsExpected: 1,
			errExpected:      fakeErr,
		},
		{
			name:      "immediate is false, condition returns true",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return true, nil
			}),
			waitFunc:         func() waitFunc { return fakeTicker(5, nil, func() {}) },
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:      "immediate is false, ticker channel is closed, condition returns true",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return true, nil
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					close(ch)
					return ch
				}
			},
			attemptsExpected: 1,
			errExpected:      nil,
		},
		{
			name:      "immediate is false, ticker channel is closed, condition returns error",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, fakeErr
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					close(ch)
					return ch
				}
			},
			attemptsExpected: 1,
			errExpected:      fakeErr,
		},
		{
			name:      "immediate is false, ticker channel is closed, condition returns false",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					close(ch)
					return ch
				}
			},
			attemptsExpected: 1,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:      "condition always returns false, timeout error expected",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc { return fakeTicker(2, nil, func() {}) },
			//waitForWithContext() 的约定要求等待结束时再调用一次 func。
			attemptsExpected: 3,
			errExpected:      ErrWaitTimeout,
		},
		{
			name:      "context is cancelled after N attempts, timeout error expected",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					//仅产生两次 tick。
					go func() {
						ch <- struct{}{}
						ch <- struct{}{}
					}()
					return ch
				}
			},
			cancelContextAfter: 2,
			attemptsExpected:   2,
			errExpected:        ErrWaitTimeout,
		},
		{
			name:      "context is cancelled after N attempts, context error not expected (legacy behavior)",
			immediate: false,
			condition: ConditionWithContextFunc(func(context.Context) (bool, error) {
				return false, nil
			}),
			waitFunc: func() waitFunc {
				return func(done <-chan struct{}) <-chan struct{} {
					ch := make(chan struct{})
					//仅产生两次 tick。
					go func() {
						ch <- struct{}{}
						ch <- struct{}{}
					}()
					return ch
				}
			},
			cancelContextAfter: 2,
			attemptsExpected:   2,
			errExpected:        ErrWaitTimeout,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts int
			ticker := waitFunc(func(done <-chan struct{}) <-chan struct{} {
				return nil
			})
			if test.waitFunc != nil {
				ticker = test.waitFunc()
			}
			err := func() error {
				contextFn := test.context
				if contextFn == nil {
					contextFn = defaultContext
				}
				ctx, cancel := contextFn()
				defer cancel()

				conditionWrapper := func(ctx context.Context) (done bool, err error) {
					attempts++

					defer func() {
						if test.cancelContextAfter == attempts {
							cancel()
						}
					}()

					return test.condition(ctx)
				}

				return poll(ctx, test.immediate, ticker.WithContext(), conditionWrapper)
			}()

			if test.errExpected != err {
				t.Errorf("Expected error: %v, but got: %v", test.errExpected, err)
			}
			if test.attemptsExpected != attempts {
				t.Errorf("Expected %d invocations, got %d", test.attemptsExpected, attempts)
			}
		})
	}
}

// Benchmark_poll 测量 poll 在 immediate 模式、零轮询间隔下的开销。
func Benchmark_poll(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attempts := 0
		if err := poll(ctx, true, poller(time.Microsecond, 0), func(_ context.Context) (bool, error) {
			attempts++
			return attempts >= 100, nil
		}); err != nil {
			b.Fatalf("unexpected err: %v", err)
		}
	}
}
