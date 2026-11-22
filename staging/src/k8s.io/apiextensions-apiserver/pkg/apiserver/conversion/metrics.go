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

package conversion

import (
	"context"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	latencyBuckets = metrics.ExponentialBuckets(0.001, 2, 15)
)

// converterMetricFactory 保存所有 CRD converter 的指标。
type converterMetricFactory struct {
	// 从 converter 名称到其指标的映射。这样可以为特定 converter（例如 'webhook'）
	// 复用同一个指标再次创建 converterMetric。
	durations   map[string]*metrics.HistogramVec
	factoryLock sync.Mutex
}

func newConverterMetricFactory() *converterMetricFactory {
	return &converterMetricFactory{durations: map[string]*metrics.HistogramVec{}, factoryLock: sync.Mutex{}}
}

var _ crConverterInterface = &converterMetric{}

type converterMetric struct {
	delegate  crConverterInterface
	latencies *metrics.HistogramVec
	crdName   string
}

func (c *converterMetricFactory) addMetrics(crdName string, converter crConverterInterface) (crConverterInterface, error) {
	c.factoryLock.Lock()
	defer c.factoryLock.Unlock()
	metric, exists := c.durations["webhook"]
	if !exists {
		metric = metrics.NewHistogramVec(
			&metrics.HistogramOpts{
				Name:           "apiserver_crd_conversion_webhook_duration_seconds",
				Help:           "CRD webhook conversion duration in seconds",
				Buckets:        latencyBuckets,
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"crd_name", "from_version", "to_version", "succeeded"})
		err := legacyregistry.Register(metric)
		if err != nil {
			return nil, err
		}
		c.durations["webhook"] = metric
	}
	return &converterMetric{latencies: metric, delegate: converter, crdName: crdName}, nil
}

// Convert 调用被包装的转换器，并记录耗时指标。
func (m *converterMetric) Convert(in runtime.Object, targetGV schema.GroupVersion) (runtime.Object, error) {
	start := time.Now()
	obj, err := m.delegate.Convert(in, targetGV)
	fromVersion := in.GetObjectKind().GroupVersionKind().Version
	toVersion := targetGV.Version

	// 仅当版本确实不同时才记录这次观测
	if fromVersion != toVersion {
		m.latencies.WithLabelValues(
			m.crdName, fromVersion, toVersion, strconv.FormatBool(err == nil)).Observe(time.Since(start).Seconds())
	}
	return obj, err
}

// ConversionWebhookErrorType 标识转换 webhook 的失败类型，用于指标中的 failure_type 标签。
type ConversionWebhookErrorType string

const (
	// ConversionWebhookCallFailure 表示调用 webhook 失败。
	ConversionWebhookCallFailure ConversionWebhookErrorType = "conversion_webhook_call_failure"
	// ConversionWebhookMalformedResponseFailure 表示 webhook 返回的响应格式错误。
	ConversionWebhookMalformedResponseFailure ConversionWebhookErrorType = "conversion_webhook_malformed_response_failure"
	// ConversionWebhookPartialResponseFailure 表示 webhook 只转换了部分对象。
	ConversionWebhookPartialResponseFailure ConversionWebhookErrorType = "conversion_webhook_partial_response_failure"
	// ConversionWebhookInvalidConvertedObjectFailure 表示转换后的对象不合法。
	ConversionWebhookInvalidConvertedObjectFailure ConversionWebhookErrorType = "conversion_webhook_invalid_converted_object_failure"
	// ConversionWebhookNoObjectsReturnedFailure 表示 webhook 没有返回任何对象。
	ConversionWebhookNoObjectsReturnedFailure ConversionWebhookErrorType = "conversion_webhook_no_objects_returned_failure"
)

// Metrics 是全局的 webhook 转换指标集合。
var (
	Metrics   = newConversionWebhookMetrics()
	namespace = "apiserver"
)

// ConversionWebhookMetrics 用 prometheus 指标度量 webhook 转换。
type ConversionWebhookMetrics struct {
	conversionWebhookRequest *metrics.CounterVec
	conversionWebhookLatency *metrics.HistogramVec
}

func newConversionWebhookMetrics() *ConversionWebhookMetrics {
	conversionWebhookRequest := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "conversion_webhook_request_total",
			Namespace:      namespace,
			Help:           "Counter for conversion webhook requests with success/failure and failure error type",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result", "failure_type"})

	conversionWebhookLatency := metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Name:      "conversion_webhook_duration_seconds",
			Namespace: namespace,
			Help:      "Conversion webhook request latency",
			// 从 5 毫秒到 60 秒的多个桶
			Buckets:        []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 45, 60},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result", "failure_type"},
	)

	legacyregistry.MustRegister(conversionWebhookRequest)
	legacyregistry.MustRegister(conversionWebhookLatency)

	return &ConversionWebhookMetrics{conversionWebhookRequest: conversionWebhookRequest, conversionWebhookLatency: conversionWebhookLatency}
}

// 观测成功的请求。
func (m *ConversionWebhookMetrics) ObserveConversionWebhookSuccess(ctx context.Context, elapsed time.Duration) {
	result := "success"
	m.conversionWebhookRequest.WithContext(ctx).WithLabelValues(result, "").Inc()
	m.observe(ctx, elapsed, result, "")
}

// 观测失败请求并记录失败类型。
func (m *ConversionWebhookMetrics) ObserveConversionWebhookFailure(ctx context.Context, elapsed time.Duration, errorType ConversionWebhookErrorType) {
	result := "failure"
	m.conversionWebhookRequest.WithContext(ctx).WithLabelValues(result, string(errorType)).Inc()
	m.observe(ctx, elapsed, result, errorType)
}

// 观测延迟。
func (m *ConversionWebhookMetrics) observe(ctx context.Context, elapsed time.Duration, result string, errorType ConversionWebhookErrorType) {
	elapsedSeconds := elapsed.Seconds()
	m.conversionWebhookLatency.WithContext(ctx).WithLabelValues(result, string(errorType)).Observe(elapsedSeconds)
}
