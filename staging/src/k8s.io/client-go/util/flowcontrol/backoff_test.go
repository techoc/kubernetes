/*
Copyright 2015 The Kubernetes Authors.

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
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// TestSlowBackoff 验证退避时长的指数增长与上限：1s、2s、4s……直到 50s
// 封顶，随后 Reset 清零。
func TestSlowBackoff(t *testing.T) {
	id := "_idSlow"
	tc := testingclock.NewFakeClock(time.Now())
	step := time.Second
	maxDuration := 50 * step

	b := NewFakeBackOff(step, maxDuration, tc)
	cases := []time.Duration{0, 1, 2, 4, 8, 16, 32, 50, 50, 50}
	for ix, c := range cases {
		tc.Step(step)
		w := b.Get(id)
		if w != c*step {
			t.Errorf("input: '%d': expected %s, got %s", ix, c*step, w)
		}
		b.Next(id, tc.Now())
	}

	// 确认 Reset 会清零退避。
	// 即使已经达到高水位，Reset 也应立即把退避清零。
	b.Next(id, tc.Now())
	b.Reset(id)
	if b.Get(id) != 0 {
		t.Errorf("Reset didn't clear the backoff.")
	}

}

// TestBackoffReset 验证长时间（> 2*maxDuration）没有新失败后，
// IsInBackOffSince 会认为退避已过期，相当于自动重置。
func TestBackoffReset(t *testing.T) {
	id := "_idReset"
	tc := testingclock.NewFakeClock(time.Now())
	step := time.Second
	maxDuration := step * 5
	b := NewFakeBackOff(step, maxDuration, tc)
	startTime := tc.Now()

	// 推进到退避值等于 maxDuration。
	for i := 0; i <= int(maxDuration/step); i++ {
		tc.Step(step)
		b.Next(id, tc.Now())
	}

	// 退避应被 maxDuration 封顶。
	if !b.IsInBackOffSince(id, tc.Now()) {
		t.Errorf("expected to be in Backoff got %s", b.Get(id))
	}

	lastUpdate := tc.Now()
	tc.Step(2*maxDuration + step) // time += 11s, 11 > 2*maxDuration
	if b.IsInBackOffSince(id, lastUpdate) {
		t.Errorf("expected to not be in Backoff after reset (start=%s, now=%s, lastUpdate=%s), got %s", startTime, tc.Now(), lastUpdate, b.Get(id))
	}
}

// TestBackoffHighWaterMark 验证超过过期窗口后再次失败时，退避不会继续
// 增长，而是停留在高水位 maxDuration。
func TestBackoffHighWaterMark(t *testing.T) {
	id := "_idHiWaterMark"
	tc := testingclock.NewFakeClock(time.Now())
	step := time.Second
	maxDuration := 5 * step
	b := NewFakeBackOff(step, maxDuration, tc)

	// 推进到退避值等于 maxDuration。
	for i := 0; i <= int(maxDuration/step); i++ {
		tc.Step(step)
		b.Next(id, tc.Now())
	}

	// 退避高水位在 2*maxDuration 后过期。
	tc.Step(maxDuration + step)
	b.Next(id, tc.Now())

	if b.Get(id) != maxDuration {
		t.Errorf("expected Backoff to stay at high watermark %s got %s", maxDuration, b.Get(id))
	}
}

// TestBackoffGC 验证 GC 的过期判定：未超过 2*maxDuration 时保留条目，
// 超过后才删除，防止 map 无界增长。
func TestBackoffGC(t *testing.T) {
	id := "_idGC"
	tc := testingclock.NewFakeClock(time.Now())
	step := time.Second
	maxDuration := 5 * step

	b := NewFakeBackOff(step, maxDuration, tc)

	for i := 0; i <= int(maxDuration/step); i++ {
		tc.Step(step)
		b.Next(id, tc.Now())
	}
	lastUpdate := tc.Now()
	tc.Step(maxDuration + step)
	b.GC()
	_, found := b.perItemBackoff[id]
	if !found {
		t.Errorf("expected GC to skip entry, elapsed time=%s maxDuration=%s", tc.Since(lastUpdate), maxDuration)
	}

	tc.Step(maxDuration + step)
	b.GC()
	r, found := b.perItemBackoff[id]
	if found {
		t.Errorf("expected GC of entry after %s got entry %v", tc.Since(lastUpdate), r)
	}
}

// TestAlternateBackoffGC 验证自定义 HasExpiredFunc 可改变 GC 的过期阈值，
// 而不影响默认的 2*maxDuration 行为。
func TestAlternateBackoffGC(t *testing.T) {
	cases := []struct {
		name           string
		hasExpiredFunc func(time.Time, time.Time, time.Duration) bool
		maxDuration    time.Duration
		nonExpiredTime time.Duration
		expiredTime    time.Duration
	}{
		{
			// 默认过期规则：超过 2*maxDuration 才回收。
			name:           "default GC",
			maxDuration:    time.Duration(50 * time.Second),
			nonExpiredTime: time.Duration(5 * time.Second),
			expiredTime:    time.Duration(101 * time.Second),
		},
		{
			// 自定义 200s 阈值，远大于默认的 2*50s。
			name: "GC later than 2*maxDuration",
			hasExpiredFunc: func(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool {
				return eventTime.Sub(lastUpdate) >= 200*time.Second
			},
			maxDuration:    time.Duration(50 * time.Second),
			nonExpiredTime: time.Duration(101 * time.Second),
			expiredTime:    time.Duration(501 * time.Second),
		},
	}

	for _, tt := range cases {
		clock := testingclock.NewFakeClock(time.Now())
		base := time.Second
		maxDuration := tt.maxDuration
		id := tt.name

		b := NewFakeBackOff(base, maxDuration, clock)
		if tt.hasExpiredFunc != nil {
			b.HasExpiredFunc = tt.hasExpiredFunc
		}

		// 初始化退避。
		lastUpdate := clock.Now()
		b.Next(id, lastUpdate)

		// 推进到仍在 GC 过期窗口内的时刻。
		clock.Step(tt.nonExpiredTime)
		b.GC()

		// 确认该条目未被 GC。
		_, found := b.perItemBackoff[id]
		if !found {
			t.Errorf("[%s] expected GC to skip entry, elapsed time=%s", tt.name, clock.Since(lastUpdate))
		}

		// 推进到超出 GC 过期窗口的时刻。
		clock.Step(tt.expiredTime)
		b.GC()
		r, found := b.perItemBackoff[id]
		if found {
			t.Errorf("[%s] expected GC of entry after %s got entry %v", tt.name, clock.Since(lastUpdate), r)
		}

	}
}

// TestIsInBackOffSinceUpdate 用一张时间表验证 IsInBackOffSinceUpdate 的
// 语义：只有当距离上次更新时间小于当前退避时长时才算“仍在退避中”，
// 并检查对应的退避值（指数增长、10s 封顶、过期后归零）。
func TestIsInBackOffSinceUpdate(t *testing.T) {
	id := "_idIsInBackOffSinceUpdate"
	tc := testingclock.NewFakeClock(time.Now())
	step := time.Second
	maxDuration := 10 * step
	b := NewFakeBackOff(step, maxDuration, tc)
	startTime := tc.Now()

	cases := []struct {
		tick      time.Duration
		inBackOff bool
		value     int
	}{
		{tick: 0, inBackOff: false, value: 0},
		{tick: 1, inBackOff: false, value: 1},
		{tick: 2, inBackOff: true, value: 2},
		{tick: 3, inBackOff: false, value: 2},
		{tick: 4, inBackOff: true, value: 4},
		{tick: 5, inBackOff: true, value: 4},
		{tick: 6, inBackOff: true, value: 4},
		{tick: 7, inBackOff: false, value: 4},
		{tick: 8, inBackOff: true, value: 8},
		{tick: 9, inBackOff: true, value: 8},
		{tick: 10, inBackOff: true, value: 8},
		{tick: 11, inBackOff: true, value: 8},
		{tick: 12, inBackOff: true, value: 8},
		{tick: 13, inBackOff: true, value: 8},
		{tick: 14, inBackOff: true, value: 8},
		{tick: 15, inBackOff: false, value: 8},
		{tick: 16, inBackOff: true, value: 10},
		{tick: 17, inBackOff: true, value: 10},
		{tick: 18, inBackOff: true, value: 10},
		{tick: 19, inBackOff: true, value: 10},
		{tick: 20, inBackOff: true, value: 10},
		{tick: 21, inBackOff: true, value: 10},
		{tick: 22, inBackOff: true, value: 10},
		{tick: 23, inBackOff: true, value: 10},
		{tick: 24, inBackOff: true, value: 10},
		{tick: 25, inBackOff: false, value: 10},
		{tick: 26, inBackOff: true, value: 10},
		{tick: 27, inBackOff: true, value: 10},
		{tick: 28, inBackOff: true, value: 10},
		{tick: 29, inBackOff: true, value: 10},
		{tick: 30, inBackOff: true, value: 10},
		{tick: 31, inBackOff: true, value: 10},
		{tick: 32, inBackOff: true, value: 10},
		{tick: 33, inBackOff: true, value: 10},
		{tick: 34, inBackOff: true, value: 10},
		{tick: 35, inBackOff: false, value: 10},
		{tick: 56, inBackOff: false, value: 0},
		{tick: 57, inBackOff: false, value: 1},
	}

	for _, c := range cases {
		tc.SetTime(startTime.Add(c.tick * step))
		if c.inBackOff != b.IsInBackOffSinceUpdate(id, tc.Now()) {
			t.Errorf("expected IsInBackOffSinceUpdate %v got %v at tick %s", c.inBackOff, b.IsInBackOffSinceUpdate(id, tc.Now()), c.tick*step)
		}

		if c.inBackOff && (time.Duration(c.value)*step != b.Get(id)) {
			t.Errorf("expected backoff value=%s got %s at tick %s", time.Duration(c.value)*step, b.Get(id), c.tick*step)
		}

		if !c.inBackOff {
			// 只有不在退避中时才推进一次失败计数，模拟下一次重试。
			b.Next(id, tc.Now())
		}
	}
}

// TestBackoffWithJitter 验证启用抖动后，每次退避值落在
// (prev, prev*(1+maxJitterFactor)] 区间内，或已封顶为 maxDuration。
func TestBackoffWithJitter(t *testing.T) {
	id := "_idJitter"
	tc := testingclock.NewFakeClock(time.Now())

	// 测试设置：下面给出 11 轮迭代，先列出抖动因子恒为 0 时的预期延迟序列：
	// 100ms  200ms  400ms  800ms  1.6s  3.2s  06.4s  12.8s  25.6s  51.2s  1m42s
	// 再列出抖动因子恒为 0.1（最大值）时的序列：
	// 110ms  231ms  485ms  1.0s   2.1s  4.4s  09.4s  19.8s  41.6s  1m27s  2m6s
	//
	// 在该配置下，即使一直取到抖动因子 0，也必然到达最大延迟，从而覆盖启用抖动时判断是否已达上限的代码路径。
	initial := 100 * time.Millisecond
	maxDuration := time.Minute
	maxJitterFactor := 0.1
	attempts := 10

	b := NewFakeBackOffWithJitter(initial, maxDuration, tc, maxJitterFactor)

	assert := func(t *testing.T, factor int, prevDelayGot, curDelayGot time.Duration) {
		// 期望区间为 (prev*factor, prev*factor + prev*maxJitterFactor]；
		// 达到 maxDuration 时允许直接等于上限。
		low := time.Duration((float64(prevDelayGot) * float64(factor)))
		high := low + time.Duration(maxJitterFactor*float64(prevDelayGot))
		if !((curDelayGot > low && curDelayGot <= high) || curDelayGot == maxDuration) {
			t.Errorf("jittered delay not within range: (%s - %s], but got %s", low, high, curDelayGot)
		}
	}

	delays := make([]time.Duration, 0)
	next := func() time.Duration {
		tc.Step(initial)
		b.Next(id, tc.Now())

		delay := b.Get(id)
		delays = append(delays, delay)
		return delay
	}

	if got := b.Get(id); got != 0 {
		t.Errorf("expected a zero wait durtion, but got: %s", got)
	}

	delayGot := next()
	assert(t, 1, initial, delayGot)

	prevDelayGot := delayGot
	for i := 0; i < attempts; i++ {
		delayGot = next()
		assert(t, 2, prevDelayGot, delayGot)

		prevDelayGot = delayGot
	}

	t.Logf("exponentially backed off jittered delays: %v", delays)
}

// TestAlternateHasExpiredFunc 验证自定义 HasExpiredFunc 控制“何时重置退避”：
// 分别覆盖默认阈值和更短的 8s 阈值。
func TestAlternateHasExpiredFunc(t *testing.T) {
	cases := []struct {
		name           string
		hasExpiredFunc func(time.Time, time.Time, time.Duration) bool
		maxDuration    time.Duration
		nonExpiredTime time.Duration
		expiredTime    time.Duration
	}{
		{
			// 默认规则下 5s 尚未过期。
			name:           "default expiration",
			maxDuration:    time.Duration(50 * time.Second),
			nonExpiredTime: time.Duration(5 * time.Second),
			expiredTime:    time.Duration(101 * time.Second),
		},
		{
			// 自定义 8s 过期：超过后 Next 应把退避重置为初始值。
			name: "expires faster than maxDuration",
			hasExpiredFunc: func(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool {
				return eventTime.Sub(lastUpdate) >= 8*time.Second
			},
			maxDuration:    time.Duration(50 * time.Second),
			nonExpiredTime: time.Duration(5 * time.Second),
			expiredTime:    time.Duration(9 * time.Second),
		},
	}

	for _, tt := range cases {
		clock := testingclock.NewFakeClock(time.Now())
		base := time.Second
		maxDuration := tt.maxDuration
		id := tt.name

		b := NewFakeBackOff(base, maxDuration, clock)

		if tt.hasExpiredFunc != nil {
			b.HasExpiredFunc = tt.hasExpiredFunc
		}
		// 初始化退避。
		b.Next(id, clock.Now())

		// 推进到仍在过期窗口内的时刻。
		clock.Step(tt.nonExpiredTime)
		b.Next(id, clock.Now())

		// 确认已进入退避。
		w := b.Get(id)
		if w < base*2 {
			t.Errorf("case %v: backoff object has not incremented like expected: want %s, got %s", tt.name, base*2, w)
		}

		// 推进到超出过期窗口的时刻。
		clock.Step(tt.expiredTime)
		b.Next(id, clock.Now())

		// 确认退避已重置为初始值。
		w = b.Get(id)
		if w != base {
			t.Errorf("case %v: hasexpired value: expected %s (backoff to be reset to initial), got %s", tt.name, base, w)
		}

		clock.SetTime(time.Now())
		b.Reset(id)
	}
}
