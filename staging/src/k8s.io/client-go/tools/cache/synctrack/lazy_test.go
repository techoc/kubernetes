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

package synctrack_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/tools/cache/synctrack"
)

func TestLazy(t *testing.T) {
	var reality int64
	var z synctrack.Lazy[int64]

	z.Evaluate = func() (int64, error) {
		return atomic.LoadInt64(&reality), nil
	}

	var wg sync.WaitGroup

	// 中文：10 个 goroutine 并发递增“真实值”并调用 Notify/Get，验证并发下不会返回
	// 比调用 Notify 时更旧的值（时间不能倒流）。
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(delay time.Duration) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				t.Helper()
				set := atomic.AddInt64(&reality, 1)
				z.Notify()
				got, err := z.Get()
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if got < set {
					t.Errorf("time went backwards. %v vs %v", got, set)
				}
				time.Sleep(delay)
			}
		}((1 + time.Duration(i%3)) * time.Microsecond)
	}

	wg.Wait()
}

// TestLazyThroughput 验证 Notify 合并效果：Evaluate 耗时 11ms，每 10ms 通知一次，
// 100 个并发 Get 的总等待时间应远小于逐个串行的累积值，说明多次通知被合并为少量求值。
func TestLazyThroughput(t *testing.T) {
	var reality int64
	var z synctrack.Lazy[int64]
	var totalWait int64
	z.Evaluate = func() (int64, error) {
		got := atomic.LoadInt64(&reality)
		// 中文：刻意拉长求值时间，使并发等待者重叠，从而暴露是否发生合并。
		time.Sleep(11 * time.Millisecond)
		return got, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		notifies := 0
		tt := time.NewTicker(10 * time.Millisecond)
		for {
			<-tt.C
			atomic.AddInt64(&reality, 1)
			z.Notify()
			notifies++
			if notifies >= 100 {
				tt.Stop()
				return
			}
			wg.Add(1)
			go func() {
				t.Helper()
				defer wg.Done()
				start := time.Now()
				z.Get()
				d := time.Since(start)
				atomic.AddInt64(&totalWait, int64(d))
			}()
		}
	}()

	wg.Wait()

	twd := time.Duration(totalWait)

	// 中文：阈值 3s 留足 CI 抖动余量；若未合并，等待会线性累加到约 1.1s 以上乃至更久。
	if twd > 3*time.Second {
		t.Errorf("total wait was: %v; par would be ~1s", twd)
	}

}

// sequence is for controlling the order various lines of code execute in.
// Replaces a bunch of time.Sleep() calls that would certainly be flaky.
//
// 中文说明：每个 WaitGroup 代表一个执行步骤的闸门。Step(n) 等待第 n 步被放行，
// 然后放行第 n+1 步，从而在不使用 sleep 的情况下确定性地编排并发顺序。
type sequence []sync.WaitGroup

// newSequence 创建 n 个初始均阻塞的步骤闸门。
func newSequence(n int) sequence {
	s := make(sequence, n)
	for i := range s {
		s[i].Add(1)
	}
	return s
}

// Start 放行第 0 步。
func (s sequence) Start() { s[0].Done() }

// Step 等待第 n 步，并放行第 n+1 步（若存在）。
func (s sequence) Step(n int) {
	s[n].Wait()
	if n+1 < len(s) {
		s[n+1].Done()
	}
}

// asyncGet runs a goroutine to do the get so it doesn't block.
//
// 中文：Get 在 pre 步骤放行后开始，完成后放行 post 步骤；返回一个阻塞式取值函数，
// 调用它即等待 goroutine 结束并拿到结果。
func asyncGet[T any](t *testing.T, seq sequence, z *synctrack.Lazy[T], pre, post int) func() T {
	var wg sync.WaitGroup
	var val T
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Helper()
		var err error
		seq.Step(pre)
		val, err = z.Get()
		seq.Step(post)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()
	return func() T { wg.Wait(); return val }
}

// TestLazySlowEval 覆盖“先发起的求值比后发起的更晚完成”的竞态。
//
// 中文说明：预期结果是不把较旧的求值结果写入缓存，也不为纠正它而额外调用 Evaluate；
// 后发起的较新结果应胜出，且所有 Get 最终拿到相同（较新）的值。
func TestLazySlowEval(t *testing.T) {
	// This tests the case where the first invocation of eval finishes
	// after a subseqent invocation. The old value should not be put into
	// the cache and returned. Nor should eval be called an extra time to
	// correct the old value having been placed into the cache.

	seq := newSequence(10)

	var getCount int64
	var z synctrack.Lazy[int64]

	z.Evaluate = func() (int64, error) {
		count := atomic.AddInt64(&getCount, 1)
		if count == 1 {
			// 中文：第一次求值故意跨过第二次求值的完成点，制造乱序完成。
			seq.Step(1)
			seq.Step(6)
		} else if count > 2 {
			t.Helper()
			t.Errorf("Eval called extra times. count=%v", count)
		} else {
			seq.Step(4)
		}
		return time.Now().UnixNano(), nil
	}

	seq.Start()

	getA := asyncGet(t, seq, &z, 0, 7)

	seq.Step(2)
	z.Notify()

	getB := asyncGet(t, seq, &z, 3, 5)

	getC := asyncGet(t, seq, &z, 8, 9)

	a, b, c := getA(), getB(), getC()
	// 中文：a 晚于 b 完成，保证测试条件成立。
	if a < b {
		t.Errorf("failed to create the test condition")
	}
	// 中文：b 与 c 都应来自较新的求值结果；若 c 等于较旧的 a 则说明旧值被错误缓存。
	if b != c && c == a {
		t.Errorf("wrong value was cached")
	}
}

// TestLazySlowEval2 覆盖“先发起的求值先完成”的竞态：旧值应被后续结果覆盖，
// 且不应额外调用 Evaluate 去修正。
func TestLazySlowEval2(t *testing.T) {
	// This tests the case where the first invocation of eval finishes
	// before a subseqent invocation. The old value should be overwritten.
	// Eval should not be called an extra time to correct the wrong value
	// having been placed into the cache.

	seq := newSequence(11)

	var getCount int64
	var z synctrack.Lazy[int64]

	z.Evaluate = func() (int64, error) {
		count := atomic.AddInt64(&getCount, 1)
		if count == 1 {
			// 中文：第一次求值在第二次开始前完成。
			seq.Step(1)
			seq.Step(5)
		} else if count > 2 {
			t.Helper()
			t.Errorf("Eval called extra times. count=%v", count)
		} else {
			seq.Step(4)
			seq.Step(7)
		}
		return time.Now().UnixNano(), nil
	}

	seq.Start()

	getA := asyncGet(t, seq, &z, 0, 6)

	seq.Step(2)

	z.Notify()

	getB := asyncGet(t, seq, &z, 3, 8)

	getC := asyncGet(t, seq, &z, 9, 10)

	a, b, c := getA(), getB(), getC()
	// 中文：a 先于 b 完成，保证测试条件成立（a 是旧值）。
	if a > b {
		t.Errorf("failed to create the test condition")
	}
	if b != c && c == a {
		t.Errorf("wrong value was cached")
	}
}

// TestLazyOnlyOnce 验证并发 Get 只触发一次 Evaluate（即请求合并）。
func TestLazyOnlyOnce(t *testing.T) {
	// This demonstrates that multiple Gets don't cause multiple Evaluates.

	seq := newSequence(8)

	var getCount int64
	var z synctrack.Lazy[int64]

	z.Evaluate = func() (int64, error) {
		count := atomic.AddInt64(&getCount, 1)
		if count == 1 {
			// 中文：保持求值进行中，使后续多个 Get 都落在同一次求值上。
			seq.Step(1)
			seq.Step(4)
		} else if count > 1 {
			t.Helper()
			t.Errorf("Eval called extra times. count=%v", count)
		}
		return time.Now().UnixNano(), nil
	}

	seq.Start()

	z.Notify()

	getA := asyncGet(t, seq, &z, 0, 5)
	getB := asyncGet(t, seq, &z, 2, 6)
	getC := asyncGet(t, seq, &z, 3, 7)

	a, b, c := getA(), getB(), getC()
	if a > b {
		t.Errorf("failed to create the test condition")
	}
	if b != c && c == a {
		t.Errorf("wrong value was cached")
	}
}

// TestLazyError 验证求值错误不会被缓存：无需 Notify，下一次 Get 会重新求值。
func TestLazyError(t *testing.T) {
	var succeed bool
	var z synctrack.Lazy[bool]
	z.Evaluate = func() (bool, error) {
		if succeed {
			return true, nil
		} else {
			return false, errors.New("deliberate fail")
		}
	}

	if _, err := z.Get(); err == nil {
		t.Fatalf("expected error")
	}
	// Note: no notify, proving the error was not cached
	succeed = true
	if _, err := z.Get(); err != nil {
		t.Fatalf("unexpected error")
	}
}
