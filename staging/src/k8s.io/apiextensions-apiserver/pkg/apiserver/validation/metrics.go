/*
Copyright 2023 The Kubernetes Authors.

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

// Package validation 实现 CRD 自定义资源的结构化与 OpenAPI schema 校验。
package validation

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiextensions_apiserver"
	subsystem = "validation"
)

// ValidationMetrics 抽象校验相关指标，便于测试中打桩替换。
type ValidationMetrics interface {
	// ObserveRatchetingTime 记录一次棘轮校验（ratcheting）耗时。
	ObserveRatchetingTime(d time.Duration)
}

// Metrics 是供 handler 链使用的全局校验指标实现。
var Metrics ValidationMetrics = &validationMetrics{
	RatchetingTime: metrics.NewHistogram(&metrics.HistogramOpts{
		Namespace:      namespace,
		Subsystem:      subsystem,
		Name:           "ratcheting_seconds",
		Help:           "Time for comparison of old to new for the purposes of CRDValidationRatcheting during an UPDATE in seconds.",
		StabilityLevel: metrics.ALPHA,
		// 从 0.01ms 开始，最后一个桶为 [~2.5s, +Inf)
		Buckets: metrics.ExponentialBuckets(0.00001, 4, 10),
	}),
}

var registerMetricsOnce sync.Once

// RegisterMetrics 将校验指标注册到 legacy registry。
// 它由 handler 链安装校验指标时调用，而非在 init() 中调用，
// 以确保 NativeHistograms 等指标 feature gate 在这些直方图创建前已经生效。
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(Metrics.(*validationMetrics).RatchetingTime)
	})
}

type validationMetrics struct {
	RatchetingTime *metrics.Histogram
}

// ObserveRatchetingTime 记录棘轮校验所花费的时间。
func (m *validationMetrics) ObserveRatchetingTime(d time.Duration) {
	m.RatchetingTime.Observe(d.Seconds())
}

// Reset 重置指标，仅供测试使用。若指标无法重新注册则 panic。
// 返回所有被重置的指标。
func (m *validationMetrics) Reset() []metrics.Registerable {
	m.RatchetingTime = metrics.NewHistogram(m.RatchetingTime.HistogramOpts)
	return []metrics.Registerable{m.RatchetingTime}
}
