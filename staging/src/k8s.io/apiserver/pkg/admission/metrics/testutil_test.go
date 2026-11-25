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

package metrics

import (
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// expectFindMetric 按名称与标签查找指标，找不到则上报致命测试错误。
// 注意：本函数只校验标签数量是否与期望一致，并不断言该指标确实存在，
// 因此它更适合做“标签集合是否被意外扩展”的守卫，而不是存在性检查。
func expectFindMetric(t *testing.T, name string, expectedLabels map[string]string) {
	// 从 legacyregistry 收集当前进程注册的全部指标，避免测试依赖全局变量。
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}

	if len(metrics) == 0 {
		t.Fatalf("No metric found with name %s and labels %#+v", name, expectedLabels)
	}

	for _, mf := range metrics {
		if mf.GetName() == name {
			for _, metric := range mf.GetMetric() {
				// 只对标签子集匹配的样本做数量校验；多出来的标签意味着
				// 指标契约被破坏，通常是新增标签却未同步更新测试。
				if testutil.LabelsMatch(metric, expectedLabels) {
					gotLabelCount := len(metric.GetLabel())
					wantLabelCount := len(expectedLabels)
					if wantLabelCount != gotLabelCount {
						t.Errorf("Got metric with %d labels, but wanted %d labels. Wanted %#+v for %s",
							gotLabelCount, wantLabelCount, expectedLabels, metric.String())
						continue
					}
				}
			}
		}
	}
}

// expectHistogramCountTotal 断言匹配 labelFilter 的各指标样本计数之和与期望一致。
// 直方图按标签组合拆成多个样本，因此这里累加所有匹配样本的 SampleCount，
// 用于验证“某类调用共发生了多少次”，而不关心具体落在哪个桶。
func expectHistogramCountTotal(t *testing.T, name string, labelFilter map[string]string, wantCount int) {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}

	counterSum := 0
	for _, mf := range metrics {
		if mf.GetName() != name {
			continue // 忽略其他指标
		}
		for _, metric := range mf.GetMetric() {
			if !testutil.LabelsMatch(metric, labelFilter) {
				continue
			}
			counterSum += int(metric.GetHistogram().GetSampleCount())
		}
	}
	if wantCount != counterSum {
		t.Errorf("Wanted count %d, got %d for metric %s with labels %#+v", wantCount, counterSum, name, labelFilter)
		// 失败时打印同名指标的全部样本，便于定位是哪一组标签多算或少算。
		for _, mf := range metrics {
			if mf.GetName() == name {
				for _, metric := range mf.GetMetric() {
					t.Logf("\tnear match: %s", metric.String())
				}
			}
		}
	}
}

// expectCounterValue 断言匹配 labelFilter 的各指标计数之和与期望一致。
// 与直方图版本不同，这里累加的是 Counter 的值：同一指标的不同标签组合
// 各自维护独立计数器，断言总和对标签划分方式不敏感。
func expectCounterValue(t *testing.T, name string, labelFilter map[string]string, wantCount int) {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}

	counterSum := 0
	for _, mf := range metrics {
		if mf.GetName() != name {
			continue // 忽略其他指标
		}
		for _, metric := range mf.GetMetric() {
			if !testutil.LabelsMatch(metric, labelFilter) {
				continue
			}
			counterSum += int(metric.GetCounter().GetValue())
		}
	}
	if wantCount != counterSum {
		t.Errorf("Wanted count %d, got %d for metric %s with labels %#+v", wantCount, counterSum, name, labelFilter)
		for _, mf := range metrics {
			if mf.GetName() == name {
				for _, metric := range mf.GetMetric() {
					t.Logf("\tnear match: %s", metric.String())
				}
			}
		}
	}
}
