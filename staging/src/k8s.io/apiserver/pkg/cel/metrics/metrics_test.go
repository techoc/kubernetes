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
	"math"
	"testing"
	"time"

	"k8s.io/component-base/metrics/legacyregistry"
)

// TestObserveCompilation 确认 CEL 编译耗时被记录进直方图：一次 2 秒的观测应使
// 计数为 1、样本和为 2。使用 legacyregistry.Reset 隔离全局注册表，避免测试间串扰。
func TestObserveCompilation(t *testing.T) {
	defer legacyregistry.Reset()
	Metrics.ObserveCompilation(2 * time.Second)
	c, s := gatherHistogram(t, "apiserver_cel_compilation_duration_seconds")
	if c != 1 {
		t.Errorf("unexpected count: %v", c)
	}
	if math.Abs(s-2.0) > 1e-7 {
		t.Fatalf("incorrect sum: %v", s)
	}
}

// TestObserveEvaluation 与上面类似，验证求值阶段耗时指标。
func TestObserveEvaluation(t *testing.T) {
	defer legacyregistry.Reset()
	Metrics.ObserveEvaluation(2 * time.Second)
	c, s := gatherHistogram(t, "apiserver_cel_evaluation_duration_seconds")
	if c != 1 {
		t.Errorf("unexpected count: %v", c)
	}
	if math.Abs(s-2.0) > 1e-7 {
		t.Fatalf("incorrect sum: %v", s)
	}
}

// gatherHistogram 从默认注册表汇总指定直方图的样本数与样本和。
// 汇总所有 Metric 是因为直方图可能带标签，产生多条序列；测试里只关心总量。
func gatherHistogram(t *testing.T, name string) (count uint64, sum float64) {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}
	for _, mf := range metrics {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				count += h.GetSampleCount()
				sum += h.GetSampleSum()
			}
			return
		}
	}
	t.Fatalf("metric not found: %v", name)
	return 0, 0
}
