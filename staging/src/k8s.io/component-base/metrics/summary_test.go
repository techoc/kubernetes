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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// TestSummary 验证 Summary 的注册与采集：未弃用指标带稳定性前缀，
// 弃用指标按级别决定是否隐藏，并确认 Observe 后样本数正确。
func TestSummary(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*SummaryOpts
		expectedMetricCount int
		expectedHelp        string
	}{
		// 未弃用指标：Help 只带稳定性前缀。
		{
			desc: "ALPHA metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				Help:           "summary help message",
				StabilityLevel: ALPHA,
			},
			expectedMetricCount: 1,
			expectedHelp:        "[ALPHA] summary help message",
		},
		{
			desc: "BETA metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				Help:           "summary help message",
				StabilityLevel: BETA,
			},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] summary help message",
		},
		{
			desc: "STABLE metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				Help:           "summary help message",
				StabilityLevel: STABLE,
			},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] summary help message",
		},
		// 弃用指标：Help 追加弃用版本；ALPHA 弃用会被隐藏，其余仍暴露。
		{
			desc: "ALPHA metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
				StabilityLevel:    ALPHA,
			},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "BETA metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
				StabilityLevel:    BETA,
			},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] (Deprecated since 1.15.0) summary help message",
		},
		{
			desc: "STABLE metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
				StabilityLevel:    STABLE,
			},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] (Deprecated since 1.15.0) summary help message",
		},
		// 隐藏指标：不再产生任何输出，Help 不会被标注。
		{
			desc: "ALPHA metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "summary help message",
				DeprecatedVersion: "1.14.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "BETA metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "summary help message",
				DeprecatedVersion: "1.14.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "STABLE metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "summary help message",
				DeprecatedVersion: "1.12.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			c := NewSummary(test.SummaryOpts)
			registry.MustRegister(c)

			ms, err := registry.Gather()
			assert.Lenf(t, ms, test.expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), test.expectedMetricCount)
			require.NoError(t, err, "Gather failed %v", err)

			for _, metric := range ms {
				assert.Equalf(t, test.expectedHelp, metric.GetHelp(), "Got %s as help message, want %s", metric.GetHelp(), test.expectedHelp)
			}

			// 每次 Observe 都应使样本数递增，验证 summary 真的在统计。
			c.Observe(1)
			c.Observe(2)
			c.Observe(3)
			c.Observe(1.5)
			expected := 4
			ms, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range ms {
				for _, m := range mf.GetMetric() {
					assert.Equalf(t, expected, int(m.GetSummary().GetSampleCount()), "Got %v, want %v as the sample count", m.GetHistogram().GetSampleCount(), expected)
				}
			}
		})
	}
}

// TestSummaryVec 验证 SummaryVec 按标签值创建独立成员，
// 并复用与 Summary 相同的稳定性/弃用/隐藏规则。
func TestSummaryVec(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*SummaryOpts
		labels              []string
		expectedMetricCount int
		expectedHelp        string
	}{
		// 未弃用指标：Help 只带稳定性前缀。
		{
			desc: "ALPHA metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: ALPHA,
				Help:           "summary help message",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "[ALPHA] summary help message",
		},
		{
			desc: "BETA metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: BETA,
				Help:           "summary help message",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] summary help message",
		},
		{
			desc: "STABLE metric non deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: STABLE,
				Help:           "summary help message",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] summary help message",
		},
		// 弃用指标：Help 追加弃用版本；ALPHA 弃用会被隐藏，其余仍暴露。
		{
			desc: "ALPHA metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "BETA metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
				StabilityLevel:    BETA,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] (Deprecated since 1.15.0) summary help message",
		},
		{
			desc: "STABLE metric deprecated",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "summary help message",
				DeprecatedVersion: "1.15.0",
				StabilityLevel:    STABLE,
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] (Deprecated since 1.15.0) summary help message",
		},
		// 隐藏指标：不再产生任何输出，Help 不会被标注。
		{
			desc: "ALPHA metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "summary help message",
				DeprecatedVersion: "1.14.0",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "BETA metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "summary help message",
				DeprecatedVersion: "1.14.0",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
		{
			desc: "STABLE metric hidden",
			SummaryOpts: &SummaryOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "summary help message",
				DeprecatedVersion: "1.12.0",
			},
			labels:              []string{"label_a", "label_b"},
			expectedMetricCount: 0,
			expectedHelp:        "summary help message",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			c := NewSummaryVec(test.SummaryOpts, test.labels)
			registry.MustRegister(c)
			c.WithLabelValues("1", "2").Observe(1.0)
			ms, err := registry.Gather()
			assert.Lenf(t, ms, test.expectedMetricCount, "Got %v metrics, Want: %v metrics", len(ms), test.expectedMetricCount)
			require.NoError(t, err, "Gather failed %v", err)

			for _, metric := range ms {
				assert.Equalf(t, test.expectedHelp, metric.GetHelp(), "Got %s as help message, want %s", metric.GetHelp(), test.expectedHelp)
			}

			// 每个新的标签组合都会创建一个成员，已存在的组合复用。
			c.WithLabelValues("1", "3").Observe(1.0)
			c.WithLabelValues("2", "3").Observe(1.0)
			ms, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range ms {
				assert.Lenf(t, mf.GetMetric(), 3, "Got %v metrics, wanted 2 as the count", len(mf.GetMetric()))
				for _, m := range mf.GetMetric() {
					assert.Equalf(t, uint64(1), m.GetSummary().GetSampleCount(), "Got %v metrics, wanted 1 as the summary sample count", m.GetSummary().GetSampleCount())
				}
			}
		})
	}
}

// TestSummaryWithLabelValueAllowList 验证标签 allow list：允许值保持原样，
// 越界值被替换为 "unexpected"，从而把高基数标签收敛到有限集合。
func TestSummaryWithLabelValueAllowList(t *testing.T) {
	labelAllowValues := map[string]string{
		"namespace_subsystem_metric_allowlist_test,label_a": "allowed",
	}
	labels := []string{"label_a", "label_b"}
	opts := &SummaryOpts{
		Namespace: "namespace",
		Name:      "metric_allowlist_test",
		Subsystem: "subsystem",
	}
	var tests = []struct {
		desc               string
		labelValues        [][]string
		expectMetricValues map[string]int
	}{
		{
			desc:        "Test no unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"allowed", "b2"}},
			expectMetricValues: map[string]int{
				"allowed b1": 1,
				"allowed b2": 1,
			},
		},
		{
			desc:        "Test unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"not_allowed", "b1"}},
			expectMetricValues: map[string]int{
				"allowed b1":    1,
				"unexpected b1": 1,
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
			c := NewSummaryVec(opts, labels)
			registry.MustRegister(c)
			SetLabelAllowList(labelAllowValues)

			for _, lv := range test.labelValues {
				c.WithLabelValues(lv...).Observe(1.0)
			}
			mfs, err := registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range mfs {
				if *mf.Name != BuildFQName(opts.Namespace, opts.Subsystem, opts.Name) {
					continue
				}
				mfMetric := mf.GetMetric()

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
					expectedValue, ok := test.expectMetricValues[labelValuePair]
					assert.True(t, ok, "Got unexpected label values, lable_a is %v, label_b is %v", aValue, bValue)
					actualValue := int(m.GetSummary().GetSampleCount())
					assert.Equalf(t, expectedValue, actualValue, "Got %v, wanted %v as the count while setting label_a to %v and label b to %v", actualValue, expectedValue, aValue, bValue)
				}
			}
		})
	}
}
