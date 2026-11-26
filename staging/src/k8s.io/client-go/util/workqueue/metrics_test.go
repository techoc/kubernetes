/*
Copyright 2018 The Kubernetes Authors.

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
	"sync"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// testMetrics 统计队列的 add/get/done 次数，并通过 updateCalled 通知 unfinished-work 更新发生。
type testMetrics struct {
	added, gotten, finished int64

	updateCalled chan<- struct{}
}

func (m *testMetrics) add(item any)          { m.added++ }
func (m *testMetrics) get(item any)          { m.gotten++ }
func (m *testMetrics) done(item any)         { m.finished++ }
func (m *testMetrics) updateUnfinishedWork() { m.updateCalled <- struct{}{} }

// TestMetricShutdown 验证队列关闭后不会再触发 unfinished-work 指标更新，避免 goroutine 泄漏。
func TestMetricShutdown(t *testing.T) {
	ch := make(chan struct{})
	m := &testMetrics{
		updateCalled: ch,
	}
	c := testingclock.NewFakeClock(time.Now())
	q := newQueue[any](c, DefaultQueue[any](), m, time.Millisecond)
	for !c.HasWaiters() {
		// 等待 goroutine 调用 NewTicker()。
		time.Sleep(time.Millisecond)
	}

	c.Step(time.Millisecond)
	<-ch
	q.ShutDown()

	// ShutDown 之后即使时钟大幅前进，也不应再有更新被投递。
	c.Step(time.Hour)
	select {
	default:
		return
	case <-ch:
		t.Errorf("Unexpected update after shutdown was called.")
	}
}

// testMetric 是一个线程安全的通用指标替身，记录 inc/dec/set/observe 调用结果。
type testMetric struct {
	inc int64
	dec int64
	set float64

	observedValue float64
	observedCount int

	notifyCh chan<- struct{}

	lock sync.Mutex
}

func (m *testMetric) Inc() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.inc++
	m.notify()
}

func (m *testMetric) Dec() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.dec++
	m.notify()
}

func (m *testMetric) Set(f float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.set = f
	m.notify()
}

func (m *testMetric) Observe(f float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.observedValue = f
	m.observedCount++
	m.notify()
}

// gaugeValue 优先返回最近一次 Set 的值；没有 Set 过则退化为 inc-dec 的净计数。
func (m *testMetric) gaugeValue() float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.set != 0 {
		return m.set
	}
	return float64(m.inc - m.dec)
}

func (m *testMetric) observationValue() float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.observedValue
}

func (m *testMetric) observationCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.observedCount
}

// notify 在设置了通知通道时发送信号，让测试在指标更新后再读取，避免竞态。
func (m *testMetric) notify() {
	if m.notifyCh != nil {
		m.notifyCh <- struct{}{}
	}
}

// testMetricsProvider 为每类指标返回固定的 testMetric，便于断言。
type testMetricsProvider struct {
	depth      testMetric
	adds       testMetric
	latency    testMetric
	duration   testMetric
	unfinished testMetric
	longest    testMetric
	retries    testMetric
}

func (m *testMetricsProvider) NewDepthMetric(name string) GaugeMetric {
	return &m.depth
}

func (m *testMetricsProvider) NewAddsMetric(name string) CounterMetric {
	return &m.adds
}

func (m *testMetricsProvider) NewLatencyMetric(name string) HistogramMetric {
	return &m.latency
}

func (m *testMetricsProvider) NewWorkDurationMetric(name string) HistogramMetric {
	return &m.duration
}

func (m *testMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) SettableGaugeMetric {
	return &m.unfinished
}

func (m *testMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) SettableGaugeMetric {
	return &m.longest
}

func (m *testMetricsProvider) NewRetriesMetric(name string) CounterMetric {
	return &m.retries
}

// TestMetrics 验证队列在 add/get/done 全流程中上报的各指标：adds、depth、latency、
// work duration、unfinished work 与 longest running processor。使用假时钟精确控制时间推进。
func TestMetrics(t *testing.T) {
	mp := testMetricsProvider{}
	t0 := time.Unix(0, 0)
	c := testingclock.NewFakeClock(t0)
	config := QueueConfig{
		Name:            "test",
		Clock:           c,
		MetricsProvider: &mp,
	}
	q := newQueueWithConfig[any](config, time.Millisecond)
	defer q.ShutDown()
	for !c.HasWaiters() {
		// 等待 goroutine 调用 NewTicker()。
		time.Sleep(time.Millisecond)
	}

	q.Add("foo")
	if e, a := 1.0, mp.adds.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	if e, a := 1.0, mp.depth.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	c.Step(50 * time.Microsecond)

	// 开始处理。
	i, _ := q.Get()
	if i != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	if e, a := 5e-05, mp.latency.observationValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 1, mp.latency.observationCount(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 处理中再次加入；同一条目多次加入会
	// de-duped.
	q.Add(i)
	q.Add(i)
	q.Add(i)
	q.Add(i)
	q.Add(i)
	if e, a := 2.0, mp.adds.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	// 队列中还剩一个条目。
	if e, a := 1.0, mp.depth.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	c.Step(25 * time.Microsecond)

	// Finish it up
	q.Done(i)

	if e, a := 2.5e-05, mp.duration.observationValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 1, mp.duration.observationCount(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 队列中还剩一个条目。
	if e, a := 1.0, mp.depth.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 它应重新出现在队列中。
	i, _ = q.Get()
	if i != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	if e, a := 2.5e-05, mp.latency.observationValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, mp.latency.observationCount(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 用 channel 确保不会在指标设置前查看它。
	// 已被设置。
	ch := make(chan struct{}, 1)
	longestCh := make(chan struct{}, 1)
	mp.unfinished.notifyCh = ch
	mp.longest.notifyCh = longestCh
	c.Step(time.Millisecond)
	<-ch
	mp.unfinished.notifyCh = nil
	if e, a := .001, mp.unfinished.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	<-longestCh
	mp.longest.notifyCh = nil
	if e, a := .001, mp.longest.gaugeValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 处理完最后一个条目。
	q.Done(i)
	if e, a := .001, mp.duration.observationValue(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := 2, mp.duration.observationCount(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}
