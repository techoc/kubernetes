/*
Copyright The Kubernetes Authors.

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
	"testing"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
)

// TestRequestsProcessed 验证处理请求计数器按驱动名称正确累加：
// 相同驱动两次请求计为 2，另一驱动计为 1，标签之间互不混淆。
func TestRequestsProcessed(t *testing.T) {
	registry := metrics.NewKubeRegistry()
	// 每次测试使用独立注册表并在结束时重置，避免与其他用例共享指标状态。
	defer registry.Reset()
	registry.MustRegister(RequestsProcessed)

	RequestsProcessed.WithLabelValues("driver-a.example.com").Inc()
	RequestsProcessed.WithLabelValues("driver-a.example.com").Inc()
	RequestsProcessed.WithLabelValues("driver-b.example.com").Inc()

	// 期望输出直接以 Prometheus 文本格式给出，
	// 便于逐字核对指标名、标签和数值。
	want := `# HELP resourcepoolstatusrequest_controller_requests_processed_total [ALPHA] Total number of ResourcePoolStatusRequests processed
# TYPE resourcepoolstatusrequest_controller_requests_processed_total counter
resourcepoolstatusrequest_controller_requests_processed_total{driver_name="driver-a.example.com"} 2
resourcepoolstatusrequest_controller_requests_processed_total{driver_name="driver-b.example.com"} 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(want), "resourcepoolstatusrequest_controller_requests_processed_total"); err != nil {
		t.Errorf("unexpected metric output: %v", err)
	}
}

// TestRequestProcessingErrors 验证处理错误计数器按驱动名称统计。
func TestRequestProcessingErrors(t *testing.T) {
	registry := metrics.NewKubeRegistry()
	defer registry.Reset()
	registry.MustRegister(RequestProcessingErrors)

	RequestProcessingErrors.WithLabelValues("driver-a.example.com").Inc()

	want := `# HELP resourcepoolstatusrequest_controller_request_processing_errors_total [ALPHA] Total number of errors encountered while processing ResourcePoolStatusRequests
# TYPE resourcepoolstatusrequest_controller_request_processing_errors_total counter
resourcepoolstatusrequest_controller_request_processing_errors_total{driver_name="driver-a.example.com"} 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(want), "resourcepoolstatusrequest_controller_request_processing_errors_total"); err != nil {
		t.Errorf("unexpected metric output: %v", err)
	}
}

// TestRequestProcessingDuration 验证处理耗时直方图按驱动名称记录样本数；
// 耗时相关字段由 gatherWithoutDurations 去除，只断言稳定的 _count 行。
func TestRequestProcessingDuration(t *testing.T) {
	registry := metrics.NewKubeRegistry()
	defer registry.Reset()
	registry.MustRegister(RequestProcessingDuration)

	RequestProcessingDuration.WithLabelValues("driver-a.example.com").Observe(0.5)
	RequestProcessingDuration.WithLabelValues("driver-b.example.com").Observe(1.0)

	// SampleSum 与 Bucket 依赖实际观测值，
	// 去掉它们才能让断言只关注计数器式的 _count。
	// (SampleSum and Bucket), verifying only _count lines.
	want := `# HELP resourcepoolstatusrequest_controller_request_processing_duration_seconds [ALPHA] Time taken to process a ResourcePoolStatusRequest
# TYPE resourcepoolstatusrequest_controller_request_processing_duration_seconds histogram
resourcepoolstatusrequest_controller_request_processing_duration_seconds_count{driver_name="driver-a.example.com"} 1
resourcepoolstatusrequest_controller_request_processing_duration_seconds_count{driver_name="driver-b.example.com"} 1
`
	if err := testutil.GatherAndCompare(gatherWithoutDurations(registry), strings.NewReader(want), "resourcepoolstatusrequest_controller_request_processing_duration_seconds"); err != nil {
		t.Errorf("unexpected metric output: %v", err)
	}
}

// gatherWithoutDurations 包装注册表采集结果，
// 从直方图中剥离依赖具体观测值的 SampleSum 与 Bucket 字段，
// 使断言只能验证各标签下样本数量（_count）。
func gatherWithoutDurations(registry metrics.KubeRegistry) testutil.GathererFunc {
	return func() ([]*testutil.MetricFamily, error) {
		got, err := registry.Gather()
		for _, mf := range got {
			for _, m := range mf.Metric {
				if m.Histogram == nil {
					continue
				}
				m.Histogram.SampleSum = nil
				m.Histogram.Bucket = nil
			}
		}
		return got, err
	}
}

// TestRegister 验证 Register 可以重复调用而不 panic，
// 因为控制器初始化路径可能在同进程中多次触发注册。
func TestRegister(t *testing.T) {
	// 重复注册依赖实现中的 sync.Once 保护，不应抛出重复注册错误。
	Register()
	Register()
}
