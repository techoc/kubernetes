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

package metrics

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	testclock "k8s.io/utils/clock/testing"
)

const (
	ddtRangeCentiPeriods  = 300
	ddtOffsetCentiPeriods = 50
	numIterations         = 100
)

var errMetricNotFound = errors.New("not found")

// TestTimingRatioHistogramVecElementSimple 验证 Checked 构造函数在标签合法时
// 能正常记录，并与 exerciseTimingRatioHistogram 的预期计数一致。
func TestTimingRatioHistogramVecElementSimple(t *testing.T) {
	testHistogramName := "vec_element_simple"
	t0 := time.Now()
	clk := testclock.NewFakePassiveClock(t0)
	buckets := []float64{0, 1}
	vec := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{Name: testHistogramName, Buckets: buckets},
		"alabel",
	)
	toRegister := vec.metrics()
	registry := compbasemetrics.NewKubeRegistry()
	for _, reg := range toRegister {
		registry.MustRegister(reg)
	}
	tro, err := vec.NewForLabelValuesChecked(0, 1, []string{"avalue"})
	if err != nil {
		t.Error(err)
	}
	exerciseTimingRatioHistogram(t, testHistogramName, t0, clk, registry, tro)
}

// TestTimingRatioHistogramVecElementSafeEarly 验证在注册之前创建 Safe 元素也能工作；
// 这类元素底层使用懒注册的 observer。
func TestTimingRatioHistogramVecElementSafeEarly(t *testing.T) {
	testHistogramName := "vec_element_safe_early"
	t0 := time.Now()
	clk := testclock.NewFakePassiveClock(t0)
	buckets := []float64{0, 1}
	vec := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{Name: testHistogramName, Buckets: buckets},
		"alabel",
	)
	tro := vec.NewForLabelValuesSafe(0, 1, []string{"avalue"})
	toRegister := vec.metrics()
	registry := compbasemetrics.NewKubeRegistry()
	for _, reg := range toRegister {
		registry.MustRegister(reg)
	}
	exerciseTimingRatioHistogram(t, testHistogramName, t0, clk, registry, tro)
}

// TestTimingRatioHistogramVecElementSafeLate 验证注册之后再创建 Safe 元素的行为，
// 与 Early 变体共同覆盖两种初始化顺序。
func TestTimingRatioHistogramVecElementSafeLate(t *testing.T) {
	testHistogramName := "vec_element_safe_late"
	t0 := time.Now()
	clk := testclock.NewFakePassiveClock(t0)
	buckets := []float64{0, 1}
	vec := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{Name: testHistogramName, Buckets: buckets},
		"alabel",
	)
	toRegister := vec.metrics()
	registry := compbasemetrics.NewKubeRegistry()
	for _, reg := range toRegister {
		registry.MustRegister(reg)
	}
	tro := vec.NewForLabelValuesSafe(0, 1, []string{"avalue"})
	exerciseTimingRatioHistogram(t, testHistogramName, t0, clk, registry, tro)
}

// exerciseTimingRatioHistogram 对 RatioedObserver 做粗略的行为测试。
// 使用假时钟，反复给它加上一个从"略为负"到"略大于两毫秒"
// 的范围内均匀随机选取的时间量。
//
// 包含负增量是因为真实单调时钟读数中确实观察到过
// 微小的负变化（见 issue #96459），我们要测试它
// 是否被正确处理。设计上的容忍方式是：在时钟净向前
// 前进之前，假装时钟没有变化。
// 测试会检查每一步 observer 中的计数是否正确，
// 预期在每一纳秒结束时产生一次观测。
//
// 预期在每一纳秒结束时产生一次观测。
//
// 测试刻意包含小幅负向时间跳变，模拟问题 #96459 中观测到的单调时钟回退；
// 设计约定是忽略回退、只按净前进量记数，因此期望计数始终基于不回退的 dt。
func exerciseTimingRatioHistogram(t *testing.T, histogramName string, t0 time.Time, clk *testclock.FakePassiveClock, registry compbasemetrics.KubeRegistry, tro RatioedGauge) {
	samplingPeriod := time.Nanosecond
	steppingPeriod := time.Millisecond
	tro.Set(1)
	// `dt` 是自测试开始以来假时间中累计被接纳的差值。
	// "被接纳"意味着它永不允许减小，这与对负的单调时钟变化
	// 的容忍设计一致。
	//
	var dt time.Duration
	// `t1` 是当前的假时间
	t1 := t0.Add(dt)
	klog.Infof("Expect about %v warnings about time going backwards; this is fake time deliberately misbehaving.", (numIterations*ddtOffsetCentiPeriods)/ddtRangeCentiPeriods)
	t.Logf("t0=%s", t0)
	for i := 0; i < numIterations; i++ {
		// `ddt` 是假时间的下一个步长
		ddt := time.Duration(rand.Intn(ddtRangeCentiPeriods)-ddtOffsetCentiPeriods) * steppingPeriod / 100
		t1 = t1.Add(ddt)
		diff := t1.Sub(t0)
		if diff > dt {
			dt = diff
		}
		clk.SetTime(t1)
		tro.Set(1)
		// 采样周期为 1ns，因此期望样本数等于累计前进的纳秒数。
		expectedCount := int64(dt / samplingPeriod)
		actualCount, err := getHistogramCount(registry, histogramName)
		if err != nil && !(err == errMetricNotFound && expectedCount == 0) {
			t.Fatalf("For t0=%s, t1=%s, failed to getHistogramCount: %#+v", t0, t1, err)
		}
		t.Logf("For i=%d, ddt=%s, t1=%s, diff=%s, dt=%s, count=%d", i, ddt, t1, diff, dt, actualCount)
		if expectedCount != actualCount {
			t.Errorf("For i=%d, t0=%s, ddt=%s, t1=%s, expectedCount=%d, actualCount=%d", i, t0, ddt, t1, expectedCount, actualCount)
		}
	}
}

/* getHistogramCount returns the count of the named histogram or an error (if any) */
func getHistogramCount(registry compbasemetrics.KubeRegistry, metricName string) (int64, error) {
	mfs, err := registry.Gather()
	if err != nil {
		return 0, fmt.Errorf("failed to gather metrics: %w", err)
	}
	for _, mf := range mfs {
		thisName := mf.GetName()
		if thisName != metricName {
			continue
		}
		metric := mf.GetMetric()[0]
		hist := metric.GetHistogram()
		if hist == nil {
			return 0, errors.New("dto.Metric has nil Histogram")
		}
		if hist.SampleCount == nil {
			return 0, errors.New("dto.Histogram has nil SampleCount")
		}
		return int64(*hist.SampleCount), nil
	}
	return 0, errMetricNotFound
}

// BenchmarkTimingRatioHistogram 测量单实例 TimingRatioHistogram 在时间推进下的 Set 开销。
func BenchmarkTimingRatioHistogram(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	clk := testclock.NewFakePassiveClock(now)
	th := NewTestableTimingRatioHistogram(clk.Now,
		&TimingRatioHistogramOpts{
			compbasemetrics.TimingHistogramOpts{
				Namespace: "testns",
				Subsystem: "testsubsys",
				Name:      "testhist",
				Help:      "Me",
				Buckets:   []float64{1, 2, 4, 8, 16, 32},
			},
			1})
	registry := compbasemetrics.NewKubeRegistry()
	registry.MustRegister(th)
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		delta := (i % 6) + 1
		now = now.Add(time.Duration(delta) * time.Millisecond)
		clk.SetTime(now)
		th.Set(float64(x))
		x = (x + i) % 60
	}
}

// BenchmarkTimingRatioHistogramVecElementSimple 测量已校验标签的 Vec 元素 Set 开销。
func BenchmarkTimingRatioHistogramVecElementSimple(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	clk := testclock.NewFakePassiveClock(now)
	thv := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{
			Namespace: "testns",
			Subsystem: "testsubsys",
			Name:      "testhist",
			Help:      "Me",
			Buckets:   []float64{1, 2, 4, 8, 16, 32},
		},
		"labelname")
	registry := compbasemetrics.NewKubeRegistry()
	registry.MustRegister(thv.metrics()...)
	th, err := thv.NewForLabelValuesChecked(0, 3, []string{"labelvalue"})
	if err != nil {
		b.Error(err)
	}
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		delta := (i % 6) + 1
		now = now.Add(time.Duration(delta) * time.Millisecond)
		clk.SetTime(now)
		th.Set(float64(x))
		x = (x + i) % 60
	}
}

// BenchmarkTimingRatioHistogramVecElementSafeEarly 测量注册前创建的 Safe 元素开销。
func BenchmarkTimingRatioHistogramVecElementSafeEarly(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	clk := testclock.NewFakePassiveClock(now)
	thv := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{
			Namespace: "testns",
			Subsystem: "testsubsys",
			Name:      "testhist",
			Help:      "Me",
			Buckets:   []float64{1, 2, 4, 8, 16, 32},
		},
		"labelname")
	th := thv.NewForLabelValuesSafe(0, 3, []string{"labelvalue"})
	registry := compbasemetrics.NewKubeRegistry()
	registry.MustRegister(thv.metrics()...)
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		delta := (i % 6) + 1
		now = now.Add(time.Duration(delta) * time.Millisecond)
		clk.SetTime(now)
		th.Set(float64(x))
		x = (x + i) % 60
	}
}

// BenchmarkTimingRatioHistogramVecElementSafeLate 测量注册后创建 Safe 元素的延迟路径开销。
func BenchmarkTimingRatioHistogramVecElementSafeLate(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	clk := testclock.NewFakePassiveClock(now)
	thv := NewTestableTimingRatioHistogramVec(clk.Now,
		&compbasemetrics.TimingHistogramOpts{
			Namespace: "testns",
			Subsystem: "testsubsys",
			Name:      "testhist",
			Help:      "Me",
			Buckets:   []float64{1, 2, 4, 8, 16, 32},
		},
		"labelname")
	registry := compbasemetrics.NewKubeRegistry()
	registry.MustRegister(thv.metrics()...)
	th := thv.NewForLabelValuesSafe(0, 3, []string{"labelvalue"})
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		delta := (i % 6) + 1
		now = now.Add(time.Duration(delta) * time.Millisecond)
		clk.SetTime(now)
		th.Set(float64(x))
		x = (x + i) % 60
	}
}
