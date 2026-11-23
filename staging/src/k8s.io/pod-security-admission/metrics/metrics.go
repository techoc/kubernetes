/*
Copyright 2021 The Kubernetes Authors.

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

// 本文件把准入结果映射到固定标签的 Prometheus 计数器，供审计与告警使用。

import (
	"strconv"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics"
	"k8s.io/pod-security-admission/api"
)

const (
	ModeAudit     = "audit"
	ModeEnforce   = "enforce"
	ModeWarn      = "warn"
	DecisionAllow = "allow" // Policy evaluated, request allowed
	DecisionDeny  = "deny"  // Policy evaluated, request denied
)

// Decision 与 Mode 作为独立类型，避免调用方把任意字符串传进录制接口污染指标标签。
type Decision string
type Mode string

// Recorder 抽象出指标上报，测试里可以换成假实现而不依赖 Prometheus registry。
type Recorder interface {
	RecordEvaluation(Decision, api.LevelVersion, Mode, api.Attributes)
	RecordExemption(api.Attributes)
	RecordError(fatal bool, attrs api.Attributes)
}

// apiVersion 用来判断策略版本是“未来版本”，防止指标基数随 API 升级无限增长。
type PrometheusRecorder struct {
	apiVersion api.Version

	evaluationsCounter *evaluationsCounter
	exemptionsCounter  *exemptionsCounter
	errorsCounter      *metrics.CounterVec
}

var _ Recorder = &PrometheusRecorder{}

func NewPrometheusRecorder(version api.Version) *PrometheusRecorder {
	errorsCounter := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "pod_security_errors_total",
			Help:           "Number of errors preventing normal evaluation. Non-fatal errors may result in the latest restricted profile being used for evaluation.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"fatal", "request_operation", "resource", "subresource"},
	)

	return &PrometheusRecorder{
		apiVersion:         version,
		evaluationsCounter: newEvaluationsCounter(),
		exemptionsCounter:  newExemptionsCounter(),
		errorsCounter:      errorsCounter,
	}
}

// MustRegister 让调用方决定注册到哪个 registry；注册失败由传入函数决定如何处理。
func (r *PrometheusRecorder) MustRegister(registerFunc func(...metrics.Registerable)) {
	registerFunc(r.evaluationsCounter)
	registerFunc(r.exemptionsCounter)
	registerFunc(r.errorsCounter)
}

// Reset 主要用于测试：多个用例共享 registry 时需要清掉上一轮的计数。
func (r *PrometheusRecorder) Reset() {
	r.evaluationsCounter.Reset()
	r.exemptionsCounter.Reset()
	r.errorsCounter.Reset()
}

func (r *PrometheusRecorder) RecordEvaluation(decision Decision, policy api.LevelVersion, evalMode Mode, attrs api.Attributes) {
	var version string
	// privileged 没有版本差异，和 latest 归到同一个标签，避免产生无意义的版本序列。
	if policy.Version.Latest() || policy.Level == api.LevelPrivileged { // Privileged is always effectively latest.
		version = "latest"
	} else {
		// 超出当前 API 版本的策略统一记为 future，防止每次发版都新增一组时间序列。
		if !r.apiVersion.Older(policy.Version) {
			version = policy.Version.String()
		} else {
			version = "future"
		}
	}

	r.evaluationsCounter.CachedInc(evaluationsLabels{
		decision:    string(decision),
		level:       string(policy.Level),
		version:     version,
		mode:        string(evalMode),
		operation:   operationLabel(attrs.GetOperation()),
		resource:    resourceLabel(attrs.GetResource()),
		subresource: attrs.GetSubresource(),
	})
}

func (r *PrometheusRecorder) RecordExemption(attrs api.Attributes) {
	r.exemptionsCounter.CachedInc(exemptionsLabels{
		operation:   operationLabel(attrs.GetOperation()),
		resource:    resourceLabel(attrs.GetResource()),
		subresource: attrs.GetSubresource(),
	})
}

func (r *PrometheusRecorder) RecordError(fatal bool, attrs api.Attributes) {
	r.errorsCounter.WithLabelValues(
		strconv.FormatBool(fatal),
		operationLabel(attrs.GetOperation()),
		resourceLabel(attrs.GetResource()),
		attrs.GetSubresource(),
	).Inc()
}

var (
	podResource       = corev1.Resource("pods")
	namespaceResource = corev1.Resource("namespaces")
)

func resourceLabel(resource schema.GroupVersionResource) string {
	switch resource.GroupResource() {
	case podResource:
		return "pod"
	case namespaceResource:
		return "namespace"
	default:
		// Assume any other resource is a valid input to pod-security, and therefore a controller.
		// 用固定值兜底，避免把任意 CRD 的 group/resource 原样当标签导致基数爆炸。
		return "controller"
	}
}

func operationLabel(op admissionv1.Operation) string {
	switch op {
	case admissionv1.Create:
		return "create"
	case admissionv1.Update:
		return "update"
	default:
		// This is a slower operation, but never used in the default implementation.
		// 保留通用分支供非默认实现使用；标签值统一小写，保持命名一致。
		return strings.ToLower(string(op))
	}
}

type evaluationsLabels struct {
	decision    string
	level       string
	version     string
	mode        string
	operation   string
	resource    string
	subresource string
}

func (l *evaluationsLabels) labels() []string {
	return []string{l.decision, l.level, l.version, l.mode, l.operation, l.resource, l.subresource}
}

type exemptionsLabels struct {
	operation   string
	resource    string
	subresource string
}

func (l *exemptionsLabels) labels() []string {
	return []string{l.operation, l.resource, l.subresource}
}

// 缓存热路径标签对应的 CounterMetric，免去每次评估都做字符串拼接和 map 查找。
type evaluationsCounter struct {
	*metrics.CounterVec

	cache     map[evaluationsLabels]metrics.CounterMetric
	cacheLock sync.RWMutex
}

func newEvaluationsCounter() *evaluationsCounter {
	return &evaluationsCounter{
		CounterVec: metrics.NewCounterVec(
			&metrics.CounterOpts{
				Name:           "pod_security_evaluations_total",
				Help:           "Number of policy evaluations that occurred, not counting ignored or exempt requests.",
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"decision", "policy_level", "policy_version", "mode", "request_operation", "resource", "subresource"},
		),
		cache: make(map[evaluationsLabels]metrics.CounterMetric),
	}
}

// 读锁覆盖 Inc 全过程：Reset 会清空 map，不能在锁外持有指针。
func (c *evaluationsCounter) CachedInc(l evaluationsLabels) {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	if cachedCounter, ok := c.cache[l]; ok {
		cachedCounter.Inc()
	} else {
		c.CounterVec.WithLabelValues(l.labels()...).Inc()
	}
}

func (c *evaluationsCounter) Create(version *semver.Version) bool {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	if c.CounterVec.Create(version) {
		c.populateCache()
		return true
	} else {
		return false
	}
}

func (c *evaluationsCounter) Reset() {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	c.CounterVec.Reset()
	c.populateCache()
}

// 只缓存默认实现真正常用的标签组合，其余走慢路径即可。
func (c *evaluationsCounter) populateCache() {
	labelsToCache := []evaluationsLabels{
		{decision: "allow", level: "privileged", version: "latest", mode: "enforce", operation: "create", resource: "pod", subresource: ""},
		{decision: "allow", level: "privileged", version: "latest", mode: "enforce", operation: "update", resource: "pod", subresource: ""},
	}
	for _, l := range labelsToCache {
		c.cache[l] = c.CounterVec.WithLabelValues(l.labels()...)
	}
}

// 与 evaluationsCounter 同理：豁免路径同样高频，值得单独缓存计数句柄。
type exemptionsCounter struct {
	*metrics.CounterVec

	cache     map[exemptionsLabels]metrics.CounterMetric
	cacheLock sync.RWMutex
}

func newExemptionsCounter() *exemptionsCounter {
	return &exemptionsCounter{
		CounterVec: metrics.NewCounterVec(
			&metrics.CounterOpts{
				Name:           "pod_security_exemptions_total",
				Help:           "Number of exempt requests, not counting ignored or out of scope requests.",
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"request_operation", "resource", "subresource"},
		),
		cache: make(map[exemptionsLabels]metrics.CounterMetric),
	}
}

// 读锁保证 Reset 重新填充缓存前不会有旧指针被使用。
func (c *exemptionsCounter) CachedInc(l exemptionsLabels) {
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	if cachedCounter, ok := c.cache[l]; ok {
		cachedCounter.Inc()
	} else {
		c.CounterVec.WithLabelValues(l.labels()...).Inc()
	}
}

func (c *exemptionsCounter) Create(version *semver.Version) bool {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	if c.CounterVec.Create(version) {
		c.populateCache()
		return true
	} else {
		return false
	}
}

func (c *exemptionsCounter) Reset() {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	c.CounterVec.Reset()
	c.populateCache()
}

// 默认实现只产生 pod/controller 的 create/update 豁免，缓存这四种组合足够。
func (c *exemptionsCounter) populateCache() {
	labelsToCache := []exemptionsLabels{
		{operation: "create", resource: "pod", subresource: ""},
		{operation: "update", resource: "pod", subresource: ""},
		{operation: "create", resource: "controller", subresource: ""},
		{operation: "update", resource: "controller", subresource: ""},
	}
	for _, l := range labelsToCache {
		c.cache[l] = c.CounterVec.WithLabelValues(l.labels()...)
	}
}
