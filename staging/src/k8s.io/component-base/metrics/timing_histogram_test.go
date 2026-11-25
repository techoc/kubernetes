/*
Copyright 2019 The Kubernetes Authors.

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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
	testclock "k8s.io/utils/clock/testing"
)

// TestTimingHistogram 针对单一 TimingHistogram，验证不同稳定性/弃用状态下
// 指标是否暴露、Help 前缀与弃用标注，以及 Set/Add 对计数与积分的计算。
func TestTimingHistogram(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*TimingHistogramOpts
		expectedMetricCount int
		expectedHelp        string
	}{
		// 未弃用指标
		{
			desc: "ALPHA metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: ALPHA,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   13,
			},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [ALPHA] histogram help message",
		},
		{
			desc: "BETA metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: BETA,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   17,
			},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [BETA] histogram help message",
		},
		{
			desc: "STABLE metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: STABLE,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   19,
			},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [STABLE] histogram help message",
		},
		// 已弃用指标
		{
			desc: "ALPHA metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      3,
			},
			expectedMetricCount: 0,
			expectedHelp:        "histogram help message",
		},
		{
			desc: "BETA metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      11,
			},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [BETA] (Deprecated since 1.15.0) histogram help message",
		},
		{
			desc: "STABLE metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      23,
			},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [STABLE] (Deprecated since 1.15.0) histogram help message",
		},
		// 隐藏指标
		{
			desc: "ALPHA metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.14.0",
				Buckets:           DefBuckets,
				InitialValue:      5,
			},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
		{
			desc: "BETA metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.14.0",
				Buckets:           DefBuckets,
				InitialValue:      7,
			},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
		{
			desc: "STABLE metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "histogram help message",
				DeprecatedVersion: "1.12.0",
				Buckets:           DefBuckets,
				InitialValue:      9,
			},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			t0 := time.Now()
			// 假时钟让“经过时间”可控，从而精确断言加权观测的计数与总和。
			clk := testclock.NewFakePassiveClock(t0)
			c := NewTestableTimingHistogram(clk.Now, test.TimingHistogramOpts)
			registry.MustRegister(c)

			metricChan := make(chan prometheus.Metric)
			go func() {
				c.Collect(metricChan)
				close(metricChan)
			}()
			m1 := <-metricChan
			gm1, ok := m1.(GaugeMetric)
			if !ok || gm1 != c.PrometheusTimingHistogram {
				t.Error("Unexpected metric", m1, c.PrometheusTimingHistogram)
			}
			// TimingHistogram 每次 Collect 只应产出一个指标。
			m2, ok := <-metricChan
			if ok {
				t.Error("Unexpected second metric", m2)
			}

			ms, err := registry.Gather()
			assert.Lenf(t, ms, test.expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), test.expectedMetricCount)
			require.NoError(t, err, "Gather failed %v", err)

			for _, metric := range ms {
				assert.Equalf(t, test.expectedHelp, metric.GetHelp(), "Got %s as help message, want %s", metric.GetHelp(), test.expectedHelp)
			}

			// 三段不同持续时间/值：计数为各段纳秒数之和，总和为“值×时长”的积分。
			v0 := test.TimingHistogramOpts.InitialValue
			dt1 := time.Nanosecond
			t1 := t0.Add(dt1)
			clk.SetTime(t1)
			var v1 float64 = 10
			c.Set(v1)
			dt2 := time.Hour
			t2 := t1.Add(dt2)
			clk.SetTime(t2)
			var v2 float64 = 1e6
			c.Add(v2 - v1)
			dt3 := time.Microsecond
			t3 := t2.Add(dt3)
			clk.SetTime(t3)
			c.Set(0)
			expectedCount := uint64(dt1 + dt2 + dt3)
			expectedSum := float64(dt1)*v0 + float64(dt2)*v1 + float64(dt3)*v2
			ms, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range ms {
				t.Logf("Considering metric family %s", mf.GetName())
				for _, m := range mf.GetMetric() {
					assert.Equalf(t, expectedCount, m.GetHistogram().GetSampleCount(), "Got %v, want %v as the sample count of metric %s", m.GetHistogram().GetSampleCount(), expectedCount, m.String())
					assert.InDeltaf(t, expectedSum, m.GetHistogram().GetSampleSum(), 0.01, "Got %v, want %v as the sample sum of metric %s", m.GetHistogram().GetSampleSum(), expectedSum, m.String())
				}
			}
		})
	}
}

// TestTimingHistogramVec 验证带标签的 TimingHistogramVec：
// 指标暴露/Help 规则与单值版本一致，并检查不同标签子指标的积分独立累计。
func TestTimingHistogramVec(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*TimingHistogramOpts
		labels              []string
		expectedMetricCount int
		expectedHelp        string
	}{
		// 未弃用指标
		{
			desc: "ALPHA metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: ALPHA,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   5,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [ALPHA] histogram help message",
		},
		{
			desc: "BETA metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: BETA,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   7,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [BETA] histogram help message",
		},
		{
			desc: "STABLE metric non deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: STABLE,
				Help:           "histogram help message",
				Buckets:        DefBuckets,
				InitialValue:   9,
			},
			labels: []string{"label_a", "label_b"},

			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [STABLE] histogram help message",
		},
		// 已弃用指标
		{
			desc: "ALPHA metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      13,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "histogram help message",
		},
		{
			desc: "BETA metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      11,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [BETA] (Deprecated since 1.15.0) histogram help message",
		},
		{
			desc: "STABLE metric deprecated",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "histogram help message",
				DeprecatedVersion: "1.15.0",
				Buckets:           DefBuckets,
				InitialValue:      17,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "EXPERIMENTAL: [STABLE] (Deprecated since 1.15.0) histogram help message",
		},
		// 隐藏指标
		{
			desc: "ALPHA metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.14.0",
				Buckets:           DefBuckets,
				InitialValue:      42,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
		{
			desc: "BETA metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "histogram help message",
				DeprecatedVersion: "1.14.0",
				Buckets:           DefBuckets,
				InitialValue:      19,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
		{
			desc: "STABLE metric hidden",
			TimingHistogramOpts: &TimingHistogramOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "histogram help message",
				DeprecatedVersion: "1.12.0",
				Buckets:           DefBuckets,
				InitialValue:      23,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "EXPERIMENTAL: histogram help message",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			t0 := time.Now()
			clk := testclock.NewFakePassiveClock(t0)
			c := NewTestableTimingHistogramVec(clk.Now, test.TimingHistogramOpts, test.labels)
			registry.MustRegister(c)
			var v0 float64 = 3
			cm1, err := c.WithLabelValuesChecked("1", "2")
			if err != nil {
				t.Error(err)
			}
			cm1.Set(v0)

			if test.expectedMetricCount > 0 {
				// 只用 Checked 版本，确保注册后才能取得真实子指标。
				metricChan := make(chan prometheus.Metric, 2)
				c.Collect(metricChan)
				close(metricChan)
				m1 := <-metricChan
				if m1 != cm1.(prometheus.Metric) {
					t.Error("Unexpected metric", m1, cm1)
				}
				m2, ok := <-metricChan
				if ok {
					t.Error("Unexpected second metric", m2)
				}
			}

			ms, err := registry.Gather()
			assert.Lenf(t, ms, test.expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), test.expectedMetricCount)
			require.NoError(t, err, "Gather failed %v", err)
			for _, metric := range ms {
				if metric.GetHelp() != test.expectedHelp {
					assert.Equalf(t, test.expectedHelp, metric.GetHelp(), "Got %s as help message, want %s", metric.GetHelp(), test.expectedHelp)
				}
			}

			// 操作指标并验证其仍能正常累计。
			c.WithLabelValues("1", "3").Set(v0)
			c.WithLabelValues("2", "3").Set(v0)
			dt1 := time.Nanosecond
			t1 := t0.Add(dt1)
			clk.SetTime(t1)
			c.WithLabelValues("1", "2").Add(5.0)
			c.WithLabelValues("1", "3").Add(5.0)
			c.WithLabelValues("2", "3").Add(5.0)
			ms, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range ms {
				t.Logf("Considering metric family %s", mf.String())
				assert.Lenf(t, mf.GetMetric(), 3, "Got %v metrics, wanted 3 as the count for family %#+v", len(mf.GetMetric()), mf)
				for _, m := range mf.GetMetric() {
					expectedCount := uint64(dt1)
					expectedSum := float64(dt1) * v0
					assert.Equalf(t, expectedCount, m.GetHistogram().GetSampleCount(), "Got %v, expected histogram sample count to equal %d for metric %s", m.GetHistogram().GetSampleCount(), expectedCount, m.String())
					assert.InDeltaf(t, expectedSum, m.GetHistogram().GetSampleSum(), 0.01, "Got %v, expected histogram sample sum to equal %v for metric %s", m.GetHistogram().GetSampleSum(), expectedSum, m.String())
				}
			}
		})
	}
}

// TestTimingHistogramWithLabelValueAllowList 验证标签值允许列表：
// 未列出的值会被替换为 "unexpected"，从而限制指标基数。
func TestTimingHistogramWithLabelValueAllowList(t *testing.T) {
	labelAllowValues := map[string]string{
		"namespace_subsystem_metric_allowlist_test,label_a": "allowed",
	}
	labels := []string{"label_a", "label_b"}
	opts := &TimingHistogramOpts{
		Namespace:    "namespace",
		Name:         "metric_allowlist_test",
		Subsystem:    "subsystem",
		InitialValue: 7,
	}
	var tests = []struct {
		desc               string
		labelValues        [][]string
		expectMetricValues map[string]uint64
	}{
		{
			desc:        "Test no unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"allowed", "b2"}},
			expectMetricValues: map[string]uint64{
				"allowed b1": 1.0,
				"allowed b2": 1.0,
			},
		},
		{
			desc:        "Test unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"not_allowed", "b1"}},
			expectMetricValues: map[string]uint64{
				"allowed b1":    1.0,
				"unexpected b1": 1.0,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			labelValueAllowLists = map[string]*MetricLabelAllowList{}

			registry := newKubeRegistry(apimachineryversion.Info{
				Major:      "1",
				Minor:      "15",
				GitVersion: "v1.15.0-alpha-1.12345",
			})
			t0 := time.Now()
			clk := testclock.NewFakePassiveClock(t0)
			c := NewTestableTimingHistogramVec(clk.Now, opts, labels)
			registry.MustRegister(c)
			SetLabelAllowList(labelAllowValues)
			var v0 float64 = 13
			for _, lv := range test.labelValues {
				c.WithLabelValues(lv...).Set(v0)
			}

			dt1 := 3 * time.Hour
			t1 := t0.Add(dt1)
			clk.SetTime(t1)

			for _, lv := range test.labelValues {
				c.WithLabelValues(lv...).Add(1.0)
			}
			mfs, err := registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range mfs {
				if *mf.Name != BuildFQName(opts.Namespace, opts.Subsystem, opts.Name) {
					continue
				}
				mfMetric := mf.GetMetric()
				t.Logf("Consider metric family %s", mf.GetName())

				for _, m := range mfMetric {
					var aValue, bValue string
					for _, l := range m.Label {
						if *l.Name == "label_a" {
							aValue = *l.Value
						}
						if *l.Name == "label_b" {
							bValue = *l.Value
						}
					}
					labelValuePair := aValue + " " + bValue
					expectedCount, ok := test.expectMetricValues[labelValuePair]
					assert.True(t, ok, "Got unexpected label values, lable_a is %v, label_b is %v", aValue, bValue)
					expectedSum := float64(dt1) * v0 * float64(expectedCount)
					expectedCount *= uint64(dt1)
					actualCount := m.GetHistogram().GetSampleCount()
					actualSum := m.GetHistogram().GetSampleSum()
					assert.Equalf(t, expectedCount, actualCount, "Got %v, wanted %v as the count while setting label_a to %v and label b to %v", actualCount, expectedCount, aValue, bValue)
					assert.InDeltaf(t, expectedSum, actualSum, 0.01, "Got %v, wanted %v as the sum while setting label_a to %v and label b to %v", actualSum, expectedSum, aValue, bValue)
				}
			}
		})
	}
}

// BenchmarkTimingHistogram 测量单值 TimingHistogram 的 Set/积分开销。
func BenchmarkTimingHistogram(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	th := NewTestableTimingHistogram(func() time.Time { return now }, &TimingHistogramOpts{
		Namespace:    "testns",
		Subsystem:    "testsubsys",
		Name:         "testhist",
		Help:         "Me",
		Buckets:      []float64{1, 2, 4, 8, 16},
		InitialValue: 3,
	})
	registry := NewKubeRegistry()
	registry.MustRegister(th)
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(time.Duration(31-x) * time.Microsecond)
		th.Set(float64(x))
		x = (x + i) % 23
	}
}

// BenchmarkTimingHistogramVecEltCached 测量已缓存子指标的写入路径。
func BenchmarkTimingHistogramVecEltCached(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	hv := NewTestableTimingHistogramVec(func() time.Time { return now }, &TimingHistogramOpts{
		Namespace:    "testns",
		Subsystem:    "testsubsys",
		Name:         "testhist",
		Help:         "Me",
		Buckets:      []float64{1, 2, 4, 8, 16},
		InitialValue: 3,
	},
		[]string{"label1", "label2"})
	registry := NewKubeRegistry()
	registry.MustRegister(hv)
	th, err := hv.WithLabelValuesChecked("v1", "v2")
	if err != nil {
		b.Error(err)
	}
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(time.Duration(31-x) * time.Microsecond)
		th.Set(float64(x))
		x = (x + i) % 23
	}
}

// BenchmarkTimingHistogramVecEltFetched 测量每次 WithLabelValues 查找再写入的路径，
// 用于对比缓存的收益。
func BenchmarkTimingHistogramVecEltFetched(b *testing.B) {
	b.StopTimer()
	now := time.Now()
	hv := NewTestableTimingHistogramVec(func() time.Time { return now }, &TimingHistogramOpts{
		Namespace:    "testns",
		Subsystem:    "testsubsys",
		Name:         "testhist",
		Help:         "Me",
		Buckets:      []float64{1, 2, 4, 8, 16},
		InitialValue: 3,
	},
		[]string{"label1", "label2"})
	registry := NewKubeRegistry()
	registry.MustRegister(hv)
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(time.Duration(31-x) * time.Microsecond)
		hv.WithLabelValues("v1", "v2").Set(float64(x))
		x = (x + i) % 60
	}
}

// TestUnregisteredVec 验证未注册向量返回 noop 与 ErrIsNotRegistered，
// 而不是创建驻留内存的永久子指标。
func TestUnregisteredVec(t *testing.T) {
	hv := NewTestableTimingHistogramVec(time.Now, &TimingHistogramOpts{
		Namespace:    "testns",
		Subsystem:    "testsubsys",
		Name:         "testhist",
		Help:         "Me",
		Buckets:      []float64{1, 2, 4, 8, 16},
		InitialValue: 3,
	},
		[]string{"label1", "label2"})
	gauge, err := hv.WithLabelValuesChecked("v1", "v2")
	if gauge != noop {
		t.Errorf("Expected noop but got %#+v", gauge)
	}
	if !ErrIsNotRegistered(err) {
		t.Errorf("Expected errNotRegistered but got err=%v", err)
	}
}

// TestBadValues 验证标签数量与向量定义不符时返回普通错误（非 ErrIsNotRegistered），
// 且不产生真实子指标。
func TestBadValues(t *testing.T) {
	hv := NewTestableTimingHistogramVec(time.Now, &TimingHistogramOpts{
		Namespace:    "testns",
		Subsystem:    "testsubsys",
		Name:         "testhist",
		Help:         "Me",
		Buckets:      []float64{1, 2, 4, 8, 16},
		InitialValue: 3,
	},
		[]string{"label1", "label2"})
	registry := NewKubeRegistry()
	registry.MustRegister(hv)
	gauge, err := hv.WithLabelValuesChecked("v1")
	if gauge != noop {
		t.Errorf("Expected noop but got %#+v", gauge)
	}
	if err == nil {
		t.Error("Expected an error but got nil")
	}
	if ErrIsNotRegistered(err) {
		t.Error("Expected an error other than errNotRegistered but got that one")
	}
}
