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

package value

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "storage"
)

/*
 * 默认情况下，以下所有指标定义为
 * ALPHA 稳定性级别 https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * 提升指标的稳定性级别是组件所有者的责任，因为这需要
 * 依照指标稳定性策略，明确承诺在多个版本中支持该指标。
 *
 */
// 以上指标默认均为 ALPHA 级别：升级稳定性等级需要组件 owner 明确承诺
// 跨多个版本的支持，因此这里不做隐式升级。
var (
	// transformerLatencies 记录转换耗时；桶从 5µs（进程内 AES）指数增长，
	// 覆盖到 KMS 外部调用可能出现的数百毫秒。
	transformerLatencies = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "transformation_duration_seconds",
			Help:      "Latencies in seconds of value transformation operations.",
			// 进程内转换（如 AES CBC）耗时约 20 微秒。但若涉及
			// 外部 KMS，延迟可能上升到数百毫秒。
			Buckets:        metrics.ExponentialBuckets(5e-6, 2, 25),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"transformation_type", "transformer_prefix"},
	)

	// transformerOperationsTotal 按资源、方向、转换器与状态统计转换次数；
	// from_storage 对应解密、to_storage 对应加密，可用于告警。
	transformerOperationsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "transformation_operations_total",
			Help:           "Total number of transformations. Successful transformation will have a status 'OK' and a varied status string when the transformation fails. The status, resource, and transformation_type fields can be used for alerting purposes. For example, you can monitor for encryption/decryption failures using the transformation_type (e.g., from_storage for decryption and to_storage for encryption). Additionally, these fields can be used to ensure that the correct transformers are applied to each resource.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "transformation_type", "transformer_prefix", "status"},
	)

	// envelopeTransformationCacheMissTotal 统计 KEK 解密缓存未命中（即需要访问 KMS）。
	envelopeTransformationCacheMissTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "envelope_transformation_cache_misses_total",
			Help:           "Total number of cache misses while accessing key decryption key(KEK).",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// dataKeyGenerationLatencies 记录 DEK 生成耗时。
	dataKeyGenerationLatencies = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "data_key_generation_duration_seconds",
			Help:           "Latencies in seconds of data encryption key(DEK) generation operations.",
			Buckets:        metrics.ExponentialBuckets(5e-6, 2, 14),
			StabilityLevel: metrics.ALPHA,
		},
	)

	// dataKeyGenerationFailuresTotal 统计 DEK 生成失败次数。
	dataKeyGenerationFailuresTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "data_key_generation_failures_total",
			Help:           "Total number of failed data encryption key(DEK) generation operations.",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

// registerMetrics 保证注册只发生一次。
var registerMetrics sync.Once

// RegisterMetrics 注册本包全部指标；可重复调用。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(transformerLatencies)
		legacyregistry.MustRegister(transformerOperationsTotal)
		legacyregistry.MustRegister(envelopeTransformationCacheMissTotal)
		legacyregistry.MustRegister(dataKeyGenerationLatencies)
		legacyregistry.MustRegister(dataKeyGenerationFailuresTotal)
	})
}

// RecordTransformation 记录一次 TransformFromStorage/TransformToStorage 的
// 次数与耗时；仅成功时才记录耗时直方图，失败耗时对容量规划无意义。
// 注意 transformation_failures_total 指标已弃用，请改用 transformation_operations_total。
func RecordTransformation(resource, transformationType, transformerPrefix string, elapsed time.Duration, err error) {
	transformerOperationsTotal.WithLabelValues(resource, transformationType, transformerPrefix, getErrorCode(err)).Inc()

	if err == nil {
		transformerLatencies.WithLabelValues(transformationType, transformerPrefix).Observe(elapsed.Seconds())
	}
}

// RecordCacheMiss 记录一次 KEK 缓存未命中；调用方在不得不访问 KMS 解密 KEK 时调用。
func RecordCacheMiss() {
	envelopeTransformationCacheMissTotal.Inc()
}

// RecordDataKeyGeneration 记录 DEK 生成结果：失败只计数，成功才记录耗时。
func RecordDataKeyGeneration(start time.Time, err error) {
	if err != nil {
		dataKeyGenerationFailuresTotal.Inc()
		return
	}

	dataKeyGenerationLatencies.Observe(sinceInSeconds(start))
}

// sinceInSeconds 返回从指定起点至今的秒数。
func sinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// gRPCError 抽象出可提取 gRPC 状态码的错误。
type gRPCError interface {
	GRPCStatus() *status.Status
}

// getErrorCode 把错误映射为稳定的标签值，避免高基数。
// 非 gRPC 错误统一归为 "unknown-non-grpc"：说明失败发生在调用 gRPC 之前。
func getErrorCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}

	// 处理被 fmt.Errorf 及类似方式包装的错误
	var s gRPCError
	if errors.As(err, &s) {
		return s.GRPCStatus().Code().String()
	}

	// 这不是 gRPC 错误。操作一定在 gRPC
	// 方法被调用前就失败了，否则会得到 gRPC 错误。
	return "unknown-non-grpc"
}
