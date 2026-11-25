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

package testutil

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// TB 是 *testing.T 与 *testing.B 共用的最小接口，便于辅助函数同时服务测试和基准测试。
type TB interface {
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// MetricFamily 是类型别名，便于测试编写收集器时无需直接导入 Prometheus
// （https://github.com/kubernetes/kubernetes/issues/99876）。
type MetricFamily = dto.MetricFamily

// GathererFunc 是类型别名，便于测试以函数形式编写收集器时无需直接导入 Prometheus
// （https://github.com/kubernetes/kubernetes/issues/99876）。
type GathererFunc = prometheus.GathererFunc

// CollectAndCompare 将传入的 Collector 注册到新建的严格注册表，并从该注册表收集指标，
// 后续处理与 GatherAndCompare 相同。
//
// 先 lint 再比较：格式不规范的指标即使数值正确也会让测试失败，以便尽早发现。
func CollectAndCompare(c metrics.Collector, expected io.Reader, metricNames ...string) error {
	lintProblems, err := testutil.CollectAndLint(c, metricNames...)
	if err != nil {
		return err
	}
	if err := GetLintError(lintProblems); err != nil {
		return err
	}

	return testutil.CollectAndCompare(c, expected, metricNames...)
}

// GatherAndCompare 从传入的 Gatherer 获取全部指标，并与 Reader 中 Prometheus 文本
// 展示格式的预期输出比较；若指定 metricNames，则只比较这些名称的指标。
//
// 同样先执行 lint，保证测试使用的期望文本符合 Prometheus 命名与 HELP 规范。
func GatherAndCompare(g metrics.Gatherer, expected io.Reader, metricNames ...string) error {
	lintProblems, err := testutil.GatherAndLint(g, metricNames...)
	if err != nil {
		return err
	}
	if err := GetLintError(lintProblems); err != nil {
		return err
	}

	return testutil.GatherAndCompare(g, expected, metricNames...)
}

// CustomCollectAndCompare 将传入的 StableCollector 注册到新建的严格注册表，
// 并从该注册表收集指标，后续处理与 GatherAndCompare 相同。
func CustomCollectAndCompare(c metrics.StableCollector, expected io.Reader, metricNames ...string) error {
	// 使用新注册表隔离，避免与其他测试共享的全局指标互相干扰。
	registry := metrics.NewKubeRegistry()
	registry.CustomMustRegister(c)

	return GatherAndCompare(registry, expected, metricNames...)
}

// ScrapeAndCompare 调用远程 exporter 的端点，将其返回的纯文本指标与 expected 中的预期输出比较。
// 若 metricNames 非空，则只比较指定名称的指标。
func ScrapeAndCompare(url string, expected io.Reader, metricNames ...string) error {
	return testutil.ScrapeAndCompare(url, expected, metricNames...)
}

// NewFakeKubeRegistry 创建一个以输入版本作为构建版本的模拟 KubeRegistry。
// 仅应用于测试，尤其是测试已弃用指标；版本格式为 major.minor.patch，如 1.18.0。
func NewFakeKubeRegistry(ver string) metrics.KubeRegistry {
	// 临时替换 BuildVersion，创建后立即恢复，避免影响并发创建的注册表。
	backup := metrics.BuildVersion
	defer func() {
		metrics.BuildVersion = backup
	}()

	metrics.BuildVersion = func() apimachineryversion.Info {
		return apimachineryversion.Info{
			GitVersion: fmt.Sprintf("v%s-alpha+1.12345", ver),
		}
	}

	return metrics.NewKubeRegistry()
}

// AssertVectorCount 在全局默认注册表中统计指定指标、匹配标签的 counter 值之和，
// 用于断言“某个标签组合下的事件次数”。
func AssertVectorCount(t TB, name string, labelFilter map[string]string, wantCount int) {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}

	counterSum := 0
	for _, mf := range metrics {
		if mf.GetName() != name {
			continue // 忽略其他指标。
		}
		for _, metric := range mf.GetMetric() {
			if !LabelsMatch(metric, labelFilter) {
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

// AssertHistogramTotalCount 与 AssertVectorCount 类似，但累加直方图的 SampleCount。
func AssertHistogramTotalCount(t TB, name string, labelFilter map[string]string, wantCount int) {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %s", err)
	}
	counterSum := 0
	for _, mf := range metrics {
		if mf.GetName() != name {
			continue // 忽略其他指标。
		}
		for _, metric := range mf.GetMetric() {
			if !LabelsMatch(metric, labelFilter) {
				continue
			}
			counterSum += int(metric.GetHistogram().GetSampleCount())
		}
	}
	if wantCount != counterSum {
		t.Errorf("Wanted count %d, got %d for metric %s with labels %#+v", wantCount, counterSum, name, labelFilter)
		for _, mf := range metrics {
			if mf.GetName() == name {
				for _, metric := range mf.GetMetric() {
					t.Logf("\tnear match: %s\n", metric.String())
				}
			}
		}
	}
}

// AssertHasNativeHistogram 验证具有指定标签的原生直方图指标存在且数据有效。
func AssertHasNativeHistogram(t TB, mf *dto.MetricFamily, labelFilter map[string]string) {
	if mf.GetType() != dto.MetricType_HISTOGRAM {
		t.Errorf("metric %q is not a histogram", mf.GetName())
		return
	}

	for _, m := range mf.GetMetric() {
		if !LabelsMatch(m, labelFilter) {
			continue
		}
		h := m.GetHistogram()
		if h == nil {
			continue
		}

		if h.Schema == nil {
			// Schema 为 nil 表示该序列仍是经典直方图，原生桶未启用。
			t.Errorf("expected native histogram data to be present for metric %q with labels %v", mf.GetName(), labelFilter)
		}

		// 存在观测值时，PositiveSpan 应包含正时长的数据
		if h.GetSampleCount() > 0 && len(h.GetPositiveSpan()) == 0 {
			t.Errorf("expected PositiveSpan data for histogram with observations")
		}

		return
	}
	t.Errorf("metric %q with labels %v not found", mf.GetName(), labelFilter)
}

// ScrapeMetricsProto 使用 protobuf 格式从 URL 抓取指标，返回以指标名称为键的指标族映射。
// 原生直方图只有在 protobuf 格式中才能完整表达，因此必须使用此格式。
func ScrapeMetricsProto(url string, client *http.Client) (map[string]*dto.MetricFamily, error) {
	if client == nil {
		return nil, fmt.Errorf("http client is required")
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	// 文本格式无法完整表达原生直方图的 span/schema，因此必须协商 protobuf。
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeProtoDelim)))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// 解析 protobuf 格式
	decoder := expfmt.NewDecoder(resp.Body, expfmt.ResponseFormat(resp.Header))
	result := make(map[string]*dto.MetricFamily)
	for {
		var mf dto.MetricFamily
		if err := decoder.Decode(&mf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode metric: %w", err)
		}
		result[mf.GetName()] = &mf
	}
	return result, nil
}
