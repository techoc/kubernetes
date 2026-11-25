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

// Package metrics 提供准入链路的 Prometheus 指标：整段准入、单个准入控制器、
// 单个 webhook 的耗时与结果，以及 matchCondition 求值的错误、排除与耗时。
package metrics

import (
	"context"
	"strconv"
	"time"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// WebhookRejectionErrorType 定义了 webhook 拒绝请求时发生的不同错误类型。
//
// 区分“webhook 正常拒绝”与“调用 webhook 失败/服务端内部错误”，
// 因为二者对运维的含义完全不同：前者是策略生效，后者是链路故障。
type WebhookRejectionErrorType string

const (
	namespace = "apiserver"
	subsystem = "admission"

	// WebhookRejectionCallingWebhookError 标识导致 webhook 准入拒绝请求的
	// 调用 webhook 错误。
	//
	// 调用 webhook 本身出错（超时、网络、证书等）导致的拒绝。
	WebhookRejectionCallingWebhookError WebhookRejectionErrorType = "calling_webhook_error"
	// WebhookRejectionAPIServerInternalError 标识导致 webhook 准入拒绝请求的
	// apiserver 内部错误。
	//
	// apiserver 侧内部错误导致的拒绝。
	WebhookRejectionAPIServerInternalError WebhookRejectionErrorType = "apiserver_internal_error"
	// WebhookRejectionNoError 标识 webhook 正常地拒绝了请求。
	//
	// webhook 正常返回拒绝，链路本身没有错误。
	WebhookRejectionNoError WebhookRejectionErrorType = "no_error"
)

var (
	// summary 指标保留 5 小时窗口，用于观察较长时间内的分位数而不无限增长内存。
	latencySummaryMaxAge = 5 * time.Hour

	// Metrics 提供对所有准入指标的访问入口。
	//
	// 全局单例：指标注册只能发生一次，否则 legacyregistry 会因重复注册而 panic。
	Metrics = newAdmissionMetrics()
)

// ObserverFunc 是一个用于上报指标的函数。
//
// extraLabels 用于在通用装饰器里追加标签，如插件名。
type ObserverFunc func(ctx context.Context, elapsed time.Duration, rejected bool, attr admission.Attributes, stepType string, extraLabels ...string)

const (
	kindWebhook  = "webhook"
	kindPolicy   = "policy"
	stepValidate = "validate"
	stepAdmit    = "admit"
)

// WithControllerMetrics 是具名准入处理器的装饰器。
//
// 用内置控制器的名字作为额外标签，把耗时归因到具体插件。
func WithControllerMetrics(i admission.Interface, name string) admission.Interface {
	return WithMetrics(i, Metrics.ObserveAdmissionController, name)
}

// WithStepMetrics 是整个准入阶段（即 admit 或 validation.admission 步骤）的装饰器。
//
// 用于统计整段准入流程的耗时，不归因到单个插件。
func WithStepMetrics(i admission.Interface) admission.Interface {
	return WithMetrics(i, Metrics.ObserveAdmissionStep)
}

// WithMetrics 使用通用的 observer 函数为准入处理器添加指标装饰。
func WithMetrics(i admission.Interface, observer ObserverFunc, extraLabels ...string) admission.Interface {
	return &pluginHandlerWithMetrics{
		Interface:   i,
		observer:    observer,
		extraLabels: extraLabels,
	}
}

// pluginHandlerWithMetrics 为准入处理器添加指标装饰。
//
// 内嵌原接口以保留其全部方法，只覆写 Admit/Validate 以插入计时。
type pluginHandlerWithMetrics struct {
	admission.Interface
	observer    ObserverFunc
	extraLabels []string
}

// Admit 执行可变（mutating）准入检查并上报指标。
//
// 若被装饰的插件不实现 MutationInterface 则直接放行，这样同一个装饰器可安全包裹任意插件。
func (p pluginHandlerWithMetrics) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	mutatingHandler, ok := p.Interface.(admission.MutationInterface)
	if !ok {
		return nil
	}

	start := time.Now()
	err := mutatingHandler.Admit(ctx, a, o)
	// 只要返回错误即计为 rejected；调用方随后决定是 fail-open 还是拒绝请求。
	p.observer(ctx, time.Since(start), err != nil, a, stepAdmit, p.extraLabels...)
	return err
}

// Validate 执行非可变准入检查并上报指标。
func (p pluginHandlerWithMetrics) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	validatingHandler, ok := p.Interface.(admission.ValidationInterface)
	if !ok {
		return nil
	}

	start := time.Now()
	err := validatingHandler.Validate(ctx, a, o)
	p.observer(ctx, time.Since(start), err != nil, a, stepValidate, p.extraLabels...)
	return err
}

// AdmissionMetrics 使用 prometheus 指标为准入流程埋点。
type AdmissionMetrics struct {
	step                            *metricSet
	controller                      *metricSet
	webhook                         *metricSet
	webhookRejection                *metrics.CounterVec
	webhookFailOpen                 *metrics.CounterVec
	webhookRequest                  *metrics.CounterVec
	matchConditionEvalErrors        *metrics.CounterVec
	matchConditionExclusions        *metrics.CounterVec
	matchConditionEvaluationSeconds *metricSet
}

// newAdmissionMetrics 创建一个新的 AdmissionMetrics，使用默认的指标名称。
func newAdmissionMetrics() *AdmissionMetrics {
	// 准入流程各步骤的指标。整个准入流程被拆分为一系列步骤，
	// 每个步骤由不同的 type 标签值标识。
	// 使用从 5 毫秒到 2.5 秒的桶。
	step := &metricSet{
		latencies: metrics.NewHistogramVec(
			&metrics.HistogramOpts{
				Namespace:      namespace,
				Subsystem:      subsystem,
				Name:           "step_admission_duration_seconds",
				Help:           "Admission sub-step latency histogram in seconds, broken out for each operation and API resource and step type (validate or admit).",
				Buckets:        []float64{0.005, 0.025, 0.1, 0.5, 1.0, 2.5},
				StabilityLevel: metrics.STABLE,
			},
			[]string{"type", "operation", "rejected"},
		),

		latenciesSummary: metrics.NewSummaryVec(
			&metrics.SummaryOpts{
				Namespace:      namespace,
				Subsystem:      subsystem,
				Name:           "step_admission_duration_seconds_summary",
				Help:           "Admission sub-step latency summary in seconds, broken out for each operation and API resource and step type (validate or admit).",
				MaxAge:         latencySummaryMaxAge,
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"type", "operation", "rejected"},
		),
	}

	// 内置准入控制器的指标。每个准入控制器以名称标识。
	// 使用从 5 毫秒到 2.5 秒的桶。
	controller := &metricSet{
		latencies: metrics.NewHistogramVec(
			&metrics.HistogramOpts{
				Namespace:      namespace,
				Subsystem:      subsystem,
				Name:           "controller_admission_duration_seconds",
				Help:           "Admission controller latency histogram in seconds, identified by name and broken out for each operation and API resource and type (validate or admit).",
				Buckets:        []float64{0.005, 0.025, 0.1, 0.5, 1.0, 2.5},
				StabilityLevel: metrics.STABLE,
			},
			[]string{"name", "type", "operation", "rejected"},
		),

		latenciesSummary: nil,
	}

	// 准入 webhook 的指标。每个 webhook 以名称标识。
	// 使用从 5 毫秒到 2.5 秒的桶（准入 webhook 默认超时为 30 秒）。
	webhook := &metricSet{
		latencies: metrics.NewHistogramVec(
			&metrics.HistogramOpts{
				Namespace:      namespace,
				Subsystem:      subsystem,
				Name:           "webhook_admission_duration_seconds",
				Help:           "Admission webhook latency histogram in seconds, identified by name and broken out for each operation and API resource and type (validate or admit).",
				Buckets:        []float64{0.005, 0.025, 0.1, 0.5, 1.0, 2.5, 10, 25},
				StabilityLevel: metrics.STABLE,
			},
			[]string{"name", "type", "operation", "rejected"},
		),

		latenciesSummary: nil,
	}

	webhookRejection := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_rejection_count",
			Help:           "Admission webhook rejection count, identified by name and broken out for each admission type (validating or admit) and operation. Additional labels specify an error type (calling_webhook_error or apiserver_internal_error if an error occurred; no_error otherwise) and optionally a non-zero rejection code if the webhook rejects the request with an HTTP status code (honored by the apiserver when the code is greater or equal to 400). Codes greater than 600 are truncated to 600, to keep the metrics cardinality bounded.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"name", "type", "operation", "error_type", "rejection_code"})

	webhookFailOpen := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_fail_open_count",
			Help:           "Admission webhook fail open count, identified by name and broken out for each admission type (validating or admit).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"name", "type"})

	webhookRequest := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_request_total",
			Help:           "Admission webhook request total, identified by name and broken out for each admission type (validating or admit) and operation. Additional labels specify whether the request was rejected or not and an HTTP status code. Codes greater than 600 are truncated to 600, to keep the metrics cardinality bounded.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"name", "type", "operation", "code", "rejected"})

	matchConditionEvalError := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "match_condition_evaluation_errors_total",
			Help:           "Admission match condition evaluation errors count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"name", "kind", "type", "operation"})

	matchConditionExclusions := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "match_condition_exclusions_total",
			Help:           "Admission match condition evaluation exclusions count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"name", "kind", "type", "operation"})

	matchConditionEvaluationSeconds := &metricSet{
		latencies: metrics.NewHistogramVec(
			&metrics.HistogramOpts{
				Namespace:      namespace,
				Subsystem:      subsystem,
				Name:           "match_condition_evaluation_seconds",
				Help:           "Admission match condition evaluation time in seconds, identified by name and broken out for each kind containing matchConditions (webhook or policy), operation and type (validate or admit).",
				Buckets:        []float64{0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25},
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"name", "kind", "type", "operation"},
		),
		latenciesSummary: nil,
	}

	step.mustRegister()
	controller.mustRegister()
	webhook.mustRegister()
	matchConditionEvaluationSeconds.mustRegister()
	legacyregistry.MustRegister(webhookRejection)
	legacyregistry.MustRegister(webhookFailOpen)
	legacyregistry.MustRegister(webhookRequest)
	legacyregistry.MustRegister(matchConditionEvalError)
	legacyregistry.MustRegister(matchConditionExclusions)
	return &AdmissionMetrics{step: step, controller: controller, webhook: webhook, webhookRejection: webhookRejection, webhookFailOpen: webhookFailOpen, webhookRequest: webhookRequest, matchConditionEvalErrors: matchConditionEvalError, matchConditionExclusions: matchConditionExclusions, matchConditionEvaluationSeconds: matchConditionEvaluationSeconds}
}

// reset 仅重置耗时类指标，供测试在用例之间隔离数据。
func (m *AdmissionMetrics) reset() {
	m.step.reset()
	m.controller.reset()
	m.webhook.reset()
}

// ObserveAdmissionStep 记录以步骤类型标识的准入步骤的相关指标。
func (m *AdmissionMetrics) ObserveAdmissionStep(ctx context.Context, elapsed time.Duration, rejected bool, attr admission.Attributes, stepType string, extraLabels ...string) {
	// 标签顺序必须与注册时声明的顺序一致：type、operation、rejected。
	m.step.observe(ctx, elapsed, append(extraLabels, stepType, string(attr.GetOperation()), strconv.FormatBool(rejected))...)
}

// ObserveAdmissionController 记录以插件处理器名称标识的内置准入控制器的相关指标。
func (m *AdmissionMetrics) ObserveAdmissionController(ctx context.Context, elapsed time.Duration, rejected bool, attr admission.Attributes, stepType string, extraLabels ...string) {
	m.controller.observe(ctx, elapsed, append(extraLabels, stepType, string(attr.GetOperation()), strconv.FormatBool(rejected))...)
}

// ObserveWebhook 记录准入 webhook 的相关指标。
func (m *AdmissionMetrics) ObserveWebhook(ctx context.Context, name string, elapsed time.Duration, rejected bool, attr admission.Attributes, stepType string, code int) {
	// webhook 可能返回任意状态码，这里将大于 600 的状态码截断，以避免指标基数失控。
	if code > 600 {
		code = 600
	}
	m.webhookRequest.WithContext(ctx).WithLabelValues(name, stepType, string(attr.GetOperation()), strconv.Itoa(code), strconv.FormatBool(rejected)).Inc()
	m.webhook.observe(ctx, elapsed, name, stepType, string(attr.GetOperation()), strconv.FormatBool(rejected))
}

// ObserveWebhookRejection 记录准入 webhook 拒绝事件的相关指标。
func (m *AdmissionMetrics) ObserveWebhookRejection(ctx context.Context, name, stepType, operation string, errorType WebhookRejectionErrorType, rejectionCode int) {
	// 将大于 600 的状态码截断，以控制指标基数。
	// 只有行为异常的 webhook 服务器才会返回这类状态码。
	if rejectionCode > 600 {
		rejectionCode = 600
	}
	m.webhookRejection.WithContext(ctx).WithLabelValues(name, stepType, operation, string(errorType), strconv.Itoa(rejectionCode)).Inc()
}

// WebhookRejectionGathererForTest 暴露准入 webhook 拒绝指标，供单元测试访问。
func (m *AdmissionMetrics) WebhookRejectionGathererForTest() *metrics.CounterVec {
	return m.webhookRejection
}

// ObserveWebhookFailOpen 记录 fail open（失败放行）的 validating 或 mutating webhook。
func (m *AdmissionMetrics) ObserveWebhookFailOpen(ctx context.Context, name, stepType string) {
	m.webhookFailOpen.WithContext(ctx).WithLabelValues(name, stepType).Inc()
}

// ObserveMatchConditionEvalError 记录因 match conditions 求值出错而未被调用的
//
// 注意：matchCondition 求值出错时默认策略是“跳过该 webhook”，因此这里统计的是被跳过的次数，
// 与 ObserveMatchConditionExclusion 区分开，便于定位是配置写错还是条件本身命中。
func (m *AdmissionMetrics) ObserveMatchConditionEvalError(ctx context.Context, name, kind, stepType, operation string) {
	m.matchConditionEvalErrors.WithContext(ctx).WithLabelValues(name, kind, stepType, operation).Inc()
}

// ObserveMatchConditionExclusion 记录因命中 match conditions 而未被调用的
func (m *AdmissionMetrics) ObserveMatchConditionExclusion(ctx context.Context, name, kind, stepType, operation string) {
	m.matchConditionExclusions.WithContext(ctx).WithLabelValues(name, kind, stepType, operation).Inc()
}

// ObserveMatchConditionEvaluationTime 记录 match condition 求值过程的耗时。
func (m *AdmissionMetrics) ObserveMatchConditionEvaluationTime(ctx context.Context, elapsed time.Duration, name, kind, stepType, operation string) {
	m.matchConditionEvaluationSeconds.observe(ctx, elapsed, name, kind, stepType, operation)
}

// metricSet 把同一组标签的 histogram 与可选 summary 打包，便于统一注册、重置与观测。
type metricSet struct {
	latencies        *metrics.HistogramVec
	latenciesSummary *metrics.SummaryVec
}

// MustRegister 注册 metricSet 中的所有 prometheus 指标。
func (m *metricSet) mustRegister() {
	legacyregistry.MustRegister(m.latencies)
	if m.latenciesSummary != nil {
		legacyregistry.MustRegister(m.latenciesSummary)
	}
}

// Reset 重置 metricSet 中的所有 prometheus 指标。
func (m *metricSet) reset() {
	m.latencies.Reset()
	if m.latenciesSummary != nil {
		m.latenciesSummary.Reset()
	}
}

// Observe 将一次观测到的准入事件记录到 metricSet 的所有指标中。
func (m *metricSet) observe(ctx context.Context, elapsed time.Duration, labels ...string) {
	elapsedSeconds := elapsed.Seconds()
	m.latencies.WithContext(ctx).WithLabelValues(labels...).Observe(elapsedSeconds)
	if m.latenciesSummary != nil {
		m.latenciesSummary.WithContext(ctx).WithLabelValues(labels...).Observe(elapsedSeconds)
	}
}
