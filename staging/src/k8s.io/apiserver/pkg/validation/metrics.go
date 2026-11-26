/*
Copyright 2025 The Kubernetes Authors.

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

package validation

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver" // Keep it consistent; apiserver is handling it
	subsystem = "validation"
)

// ValidationMetrics 是 validation 指标的接口。
// 用接口而非全局计数器，便于测试替换并避免各包直接依赖具体实现。
type ValidationMetrics interface {
	IncDeclarativeValidationMismatchMetric(validationIdentifier string)
	IncDeclarativeValidationPanicMetric(validationIdentifier string)
	IncDuplicateValidationErrorMetric()
	Reset()
}

// validationMetricsInstance 是生产实现。
// 同时维护“总量”和“按 validation identifier 拆分”的两个计数器：
// 前者用于长期趋势（BETA），后者用于定位具体规则（ALPHA，新增标签较灵活）。
var validationMetricsInstance = &validationMetrics{
	DeclarativeValidationMismatchCounter: metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "declarative_validation_mismatch_total",
			Help:           "Number of times declarative validation results differed from handwritten validation results for core types.",
			StabilityLevel: metrics.BETA,
		},
	),
	DeclarativeValidationMismatchCounterVector: metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "declarative_validation_parity_discrepancies_total",
			Help:           "Number of discrepancies between declarative and handwritten validation, broken down by validation identifier.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"validation_identifier"},
	),
	DeclarativeValidationPanicCounter: metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "declarative_validation_panic_total",
			Help:           "Number of times declarative validation has panicked during validation.",
			StabilityLevel: metrics.BETA,
		},
	),
	DeclarativeValidationPanicCounterVector: metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "declarative_validation_panics_total",
			Help:           "Number of panics in declarative validation, broken down by validation identifier.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"validation_identifier"},
	),
	DuplicateValidationErrorCounter: metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "duplicate_validation_error_total",
			Help:           "Number of duplicate validation errors during validation.",
			StabilityLevel: metrics.INTERNAL,
		},
	),
}

// Metrics 提供对 validation 指标的访问。
// 包级可变变量而非常量：测试可以通过替换它注入假指标实现。
var Metrics ValidationMetrics = validationMetricsInstance

// init 在导入时注册指标；相对于显式 Register 调用，这保证任何使用
// Metrics 的组件都不会忘记注册。
func init() {
	legacyregistry.MustRegister(validationMetricsInstance.DeclarativeValidationMismatchCounter)
	legacyregistry.MustRegister(validationMetricsInstance.DeclarativeValidationMismatchCounterVector)
	legacyregistry.MustRegister(validationMetricsInstance.DeclarativeValidationPanicCounter)
	legacyregistry.MustRegister(validationMetricsInstance.DeclarativeValidationPanicCounterVector)
	legacyregistry.MustRegister(validationMetricsInstance.DuplicateValidationErrorCounter)
}

// validationMetrics 是 ValidationMetrics 的生产实现，持有全部底层计数器。
type validationMetrics struct {
	DeclarativeValidationMismatchCounter       *metrics.Counter
	DeclarativeValidationMismatchCounterVector *metrics.CounterVec
	DeclarativeValidationPanicCounter          *metrics.Counter
	DeclarativeValidationPanicCounterVector    *metrics.CounterVec
	DuplicateValidationErrorCounter            *metrics.Counter
}

// 重置校验相关指标。
func (m *validationMetrics) Reset() {
	m.DeclarativeValidationMismatchCounter.Reset()
	m.DeclarativeValidationMismatchCounterVector.Reset()
	m.DeclarativeValidationPanicCounter.Reset()
	m.DeclarativeValidationPanicCounterVector.Reset()
	m.DuplicateValidationErrorCounter.Reset()
}

// IncDeclarativeValidationMismatchMetric 递增 declarative_validation_mismatch_total 指标的计数，
// 同时更新聚合计数与按标识拆分计数，二者始终成对增长。
func (m *validationMetrics) IncDeclarativeValidationMismatchMetric(validationIdentifier string) {
	m.DeclarativeValidationMismatchCounter.Inc()
	m.DeclarativeValidationMismatchCounterVector.WithLabelValues(validationIdentifier).Inc()
}

// 递增 declarative_validation_panic_total 计数器。
func (m *validationMetrics) IncDeclarativeValidationPanicMetric(validationIdentifier string) {
	m.DeclarativeValidationPanicCounter.Inc()
	m.DeclarativeValidationPanicCounterVector.WithLabelValues(validationIdentifier).Inc()
}

// 递增 duplicate_validation_error_total 计数器。
func (m *validationMetrics) IncDuplicateValidationErrorMetric() {
	m.DuplicateValidationErrorCounter.Inc()
}

// ResetValidationMetricsInstance 重置全局指标实例，供测试清理状态。
func ResetValidationMetricsInstance() {
	validationMetricsInstance.Reset()
}
