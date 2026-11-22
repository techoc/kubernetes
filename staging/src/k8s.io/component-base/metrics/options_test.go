/*
Copyright 2021 The Kubernetes Authors.

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
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// TestEnableHiddenMetrics 验证 SetShowHidden 前后，已过弃用宽限期的 STABLE 指标
// 从不可采集变为可采集，并同步更新 hidden_metrics_total。
// 覆盖 Register 与 MustRegister 两条注册路径。
func TestEnableHiddenMetrics(t *testing.T) {
	resetMetricsGlobalState(t)

	currentVersion := apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.1-alpha-1.12345",
	}

	var tests = []struct {
		name           string
		fqName         string
		counter        *Counter
		mustRegister   bool
		expectedMetric string
	}{
		{
			name:   "hide by register",
			fqName: "hidden_metric_register",
			counter: NewCounter(&CounterOpts{
				Name:              "hidden_metric_register",
				Help:              "counter help",
				StabilityLevel:    STABLE,
				DeprecatedVersion: "1.14.0",
			}),
			mustRegister: false,
			expectedMetric: `
				# HELP hidden_metrics_total [BETA] The count of hidden metrics.
                # TYPE hidden_metrics_total counter
                hidden_metrics_total 1
				# HELP hidden_metric_register [STABLE] (Deprecated since 1.14.0) counter help
				# TYPE hidden_metric_register counter
				hidden_metric_register 1
				`,
		},
		{
			name:   "hide by must register",
			fqName: "hidden_metric_must_register",
			counter: NewCounter(&CounterOpts{
				Name:              "hidden_metric_must_register",
				Help:              "counter help",
				StabilityLevel:    STABLE,
				DeprecatedVersion: "1.14.0",
			}),
			mustRegister: true,
			expectedMetric: `
				# HELP hidden_metric_must_register [STABLE] (Deprecated since 1.14.0) counter help
				# TYPE hidden_metric_must_register counter
				hidden_metric_must_register 1
				# HELP hidden_metrics_total [BETA] The count of hidden metrics.
                # TYPE hidden_metrics_total counter
                hidden_metrics_total 2
				`,
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			registry := newKubeRegistry(currentVersion)
			registry.MustRegister(hiddenMetricsTotal)
			if tc.mustRegister {
				registry.MustRegister(tc.counter)
			} else {
				_ = registry.Register(tc.counter)
			}

			// 隐藏的 counter 尚未初始化，Inc 是空操作，不会出现在采集结果中。
			tc.counter.Inc() // no-ops, because counter hasn't been initialized
			if err := testutil.GatherAndCompare(registry, strings.NewReader(""), tc.fqName); err != nil {
				t.Fatal(err)
			}

			SetShowHidden()
			// showHidden 是全局一次性状态，测试结束必须复位，否则影响其他用例。
			defer func() {
				showHiddenOnce = sync.Once{}
				showHidden.Store(false)
			}()

			tc.counter.Inc()
			if err := testutil.GatherAndCompare(registry, strings.NewReader(tc.expectedMetric), tc.fqName, hiddenMetricsTotal.Name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestEnableHiddenStableCollector 验证自定义 StableCollector 中隐藏的指标在
// SetShowHidden 后重新注册：全部隐藏与部分隐藏两种组合。
func TestEnableHiddenStableCollector(t *testing.T) {
	var currentVersion = apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.0-alpha-1.12345",
	}
	var normal = NewDesc("test_enable_hidden_custom_metric_normal", "this is a normal metric", []string{"name"}, nil, STABLE, "")
	var hiddenA = NewDesc("test_enable_hidden_custom_metric_hidden_a", "this is the hidden metric A", []string{"name"}, nil, STABLE, "1.14.0")
	var hiddenB = NewDesc("test_enable_hidden_custom_metric_hidden_b", "this is the hidden metric B", []string{"name"}, nil, STABLE, "1.14.0")

	var tests = []struct {
		name                      string
		descriptors               []*Desc
		metricNames               []string
		expectMetricsBeforeEnable string
		expectMetricsAfterEnable  string
	}{
		{
			name:        "all hidden",
			descriptors: []*Desc{hiddenA, hiddenB},
			metricNames: []string{"test_enable_hidden_custom_metric_hidden_a",
				"test_enable_hidden_custom_metric_hidden_b"},
			expectMetricsBeforeEnable: "",
			expectMetricsAfterEnable: `
        		# HELP test_enable_hidden_custom_metric_hidden_a [STABLE] (Deprecated since 1.14.0) this is the hidden metric A
        		# TYPE test_enable_hidden_custom_metric_hidden_a gauge
        		test_enable_hidden_custom_metric_hidden_a{name="value"} 1
        		# HELP test_enable_hidden_custom_metric_hidden_b [STABLE] (Deprecated since 1.14.0) this is the hidden metric B
        		# TYPE test_enable_hidden_custom_metric_hidden_b gauge
        		test_enable_hidden_custom_metric_hidden_b{name="value"} 1
			`,
		},
		{
			name:        "partial hidden",
			descriptors: []*Desc{normal, hiddenA, hiddenB},
			metricNames: []string{"test_enable_hidden_custom_metric_normal",
				"test_enable_hidden_custom_metric_hidden_a",
				"test_enable_hidden_custom_metric_hidden_b"},
			expectMetricsBeforeEnable: `
        		# HELP test_enable_hidden_custom_metric_normal [STABLE] this is a normal metric
        		# TYPE test_enable_hidden_custom_metric_normal gauge
        		test_enable_hidden_custom_metric_normal{name="value"} 1
			`,
			expectMetricsAfterEnable: `
        		# HELP test_enable_hidden_custom_metric_normal [STABLE] this is a normal metric
        		# TYPE test_enable_hidden_custom_metric_normal gauge
        		test_enable_hidden_custom_metric_normal{name="value"} 1
        		# HELP test_enable_hidden_custom_metric_hidden_a [STABLE] (Deprecated since 1.14.0) this is the hidden metric A
        		# TYPE test_enable_hidden_custom_metric_hidden_a gauge
        		test_enable_hidden_custom_metric_hidden_a{name="value"} 1
        		# HELP test_enable_hidden_custom_metric_hidden_b [STABLE] (Deprecated since 1.14.0) this is the hidden metric B
        		# TYPE test_enable_hidden_custom_metric_hidden_b gauge
        		test_enable_hidden_custom_metric_hidden_b{name="value"} 1
			`,
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			registry := newKubeRegistry(currentVersion)
			customCollector := newTestCustomCollector(tc.descriptors...)
			registry.CustomMustRegister(customCollector)

			if err := testutil.GatherAndCompare(registry, strings.NewReader(tc.expectMetricsBeforeEnable), tc.metricNames...); err != nil {
				t.Fatalf("before enable test failed: %v", err)
			}

			SetShowHidden()
			defer func() {
				showHiddenOnce = sync.Once{}
				showHidden.Store(false)
			}()

			if err := testutil.GatherAndCompare(registry, strings.NewReader(tc.expectMetricsAfterEnable), tc.metricNames...); err != nil {
				t.Fatalf("after enable test failed: %v", err)
			}

			// refresh descriptors to share with cases.
			// Desc 在用例间共享，重新启用后需清掉隐藏标记，避免污染下一个用例。
			for _, d := range tc.descriptors {
				d.ClearState()
			}
		})
	}
}

// TestShowHiddenMetric 验证在 showHidden 置位后才注册的指标也会立即可见，
// 说明可见性判定发生在注册时而非创建时。
func TestShowHiddenMetric(t *testing.T) {
	resetMetricsGlobalState(t)

	registry := newKubeRegistry(apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	})

	expectedMetricCount := 0
	registry.MustRegister(alphaHiddenCounter)

	ms, err := registry.Gather()
	require.NoError(t, err, "Gather failed %v", err)
	assert.Lenf(t, ms, expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), expectedMetricCount)

	showHidden.Store(true)
	registry.MustRegister(NewCounter(
		&CounterOpts{
			Namespace:         "some_namespace",
			Name:              "test_alpha_show_hidden_counter",
			Subsystem:         "subsystem",
			StabilityLevel:    ALPHA,
			Help:              "counter help",
			DeprecatedVersion: "1.14.0",
		},
	))
	expectedMetricCount = 1

	ms, err = registry.Gather()
	require.NoError(t, err, "Gather failed %v", err)
	assert.Lenf(t, ms, expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), expectedMetricCount)
}

// TestDisabledMetrics 验证 --disabled-metrics：重复项去重后 disabled_metrics_total 为 1，
// 被禁用的指标不进入采集结果，而其他指标不受影响。
func TestDisabledMetrics(t *testing.T) {
	resetMetricsGlobalState(t)

	o := NewOptions()
	// 重复项用于验证内部按集合去重。
	o.DisabledMetrics = []string{"should_be_disabled", "should_be_disabled"} // should be deduplicated (disabled_metrics_total == 1)
	currentVersion := apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.1-alpha-1.12345",
	}
	registry := newKubeRegistry(currentVersion)
	registry.MustRegister(disabledMetricsTotal)
	o.Apply()
	disabledMetric := NewCounterVec(&CounterOpts{
		Name: "should_be_disabled",
		Help: "this metric should be disabled",
	}, []string{"label"})
	// gauges cannot be reset
	enabledMetric := NewGauge(&GaugeOpts{
		Name: "should_be_enabled",
		Help: "this metric should not be disabled",
	})

	registry.MustRegister(disabledMetric)
	registry.MustRegister(enabledMetric)
	disabledMetric.WithLabelValues("one").Inc()
	disabledMetric.WithLabelValues("two").Inc()
	disabledMetric.WithLabelValues("two").Inc()
	enabledMetric.Inc()

	enabledMetricOutput := `# HELP disabled_metrics_total [BETA] The count of disabled metrics.
		# TYPE disabled_metrics_total counter
        disabled_metrics_total 1
        # HELP should_be_enabled [ALPHA] this metric should not be disabled
        # TYPE should_be_enabled gauge
        should_be_enabled 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(enabledMetricOutput), "should_be_disabled", "should_be_enabled", disabledMetricsTotal.Name); err != nil {
		t.Fatal(err)
	}
}
