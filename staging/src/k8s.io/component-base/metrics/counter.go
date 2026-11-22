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

package metrics

import (
	"context"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"

	dto "github.com/prometheus/client_model/go"
)

// Counter is our internal representation for our wrapping struct around prometheus
// counters. Counter implements both kubeCollector and CounterMetric.
// Counter 是对 prometheus Counter 的包装，同时实现 kubeCollector 与 CounterMetric。
// 指标在首次注册前是惰性 noop，未注册时不会真正计数，也不会泄露到内存中。
type Counter struct {
	CounterMetric
	*CounterOpts
	lazyMetric
	selfCollector
}

// The implementation of the Metric interface is expected by testutil.GetCounterMetricValue.
var _ Metric = &Counter{}

// exemplarCounterMetric holds a context to extract exemplar labels from, and a counter metric to attach them to. It implements the metricWithExemplar interface.
// exemplarCounterMetric 把 context 中的 trace/span 信息作为 exemplar 附加到计数上。
type exemplarCounterMetric struct {
	ctx      context.Context
	delegate CounterMetric
}

// NewCounter returns an object which satisfies the kubeCollector and CounterMetric interfaces.
// However, the object returned will not measure anything unless the collector is first
// registered, since the metric is lazily instantiated.
func NewCounter(opts *CounterOpts) *Counter {
	opts.StabilityLevel.setDefaults()

	kc := &Counter{
		CounterOpts: opts,
		lazyMetric:  lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	// 先挂 noop 实现：未注册前调用 Add/Inc 不产生任何开销。
	kc.setPrometheusCounter(noop)
	kc.lazyInit(kc, BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return kc
}

// Desc 返回底层 prometheus Desc。
func (c *Counter) Desc() *prometheus.Desc {
	return c.metric.Desc()
}

// Write 把当前值写出到 dto.Metric，供 testutil 等工具读取。
func (c *Counter) Write(to *dto.Metric) error {
	return c.metric.Write(to)
}

// Reset resets the underlying prometheus Counter to start counting from 0 again
// Reset 将底层计数器清零；未创建时直接返回，避免无意义地替换 noop。
func (c *Counter) Reset() {
	if !c.IsCreated() {
		return
	}
	c.setPrometheusCounter(prometheus.NewCounter(c.CounterOpts.toPromCounterOpts()))
}

// setPrometheusCounter sets the underlying CounterMetric object, i.e. the thing that does the measurement.
// setPrometheusCounter 同时更新 selfCollector，使 Describe/Collect 指向同一个实现。
func (c *Counter) setPrometheusCounter(counter prometheus.Counter) {
	c.CounterMetric = counter
	c.initSelfCollection(counter)
}

// DeprecatedVersion returns a pointer to the Version or nil
// DeprecatedVersion 解析并返回弃用版本；未配置时返回 nil。
func (c *Counter) DeprecatedVersion() *semver.Version {
	return parseSemver(c.CounterOpts.DeprecatedVersion)
}

// initializeMetric invocation creates the actual underlying Counter. Until this method is called
// the underlying counter is a no-op.
// initializeMetric 在注册时创建真正的 prometheus Counter，并在 Help 中标注稳定性级别。
func (c *Counter) initializeMetric() {
	c.CounterOpts.annotateStabilityLevel()
	// 这里才真正创建底层 prometheus counter。
	c.setPrometheusCounter(prometheus.NewCounter(c.CounterOpts.toPromCounterOpts()))
}

// initializeDeprecatedMetric invocation creates the actual (but deprecated) Counter. Until this method
// is called the underlying counter is a no-op.
// initializeDeprecatedMetric 为已弃用指标创建 counters，并在 Help 中追加弃用说明。
func (c *Counter) initializeDeprecatedMetric() {
	c.CounterOpts.markDeprecated()
	c.initializeMetric()
}

// WithContext allows the normal Counter metric to pass in context.
// WithContext 返回带 context 的包装，用于把 trace 信息作为 exemplar 上报。
func (c *Counter) WithContext(ctx context.Context) CounterMetric {
	return &exemplarCounterMetric{ctx: ctx, delegate: c.CounterMetric}
}

// Add attaches an exemplar to the metric and then calls the delegate.
// Add 先检查 context 中是否有已采样的 span；有则附带 trace_id/span_id 作为 exemplar，
// 否则退化为普通 Add，避免抽样数据缺失时影响指标本身。
func (e *exemplarCounterMetric) Add(v float64) {
	if m, ok := e.delegate.(prometheus.ExemplarAdder); ok {
		maybeSpanCtx := trace.SpanContextFromContext(e.ctx)
		if maybeSpanCtx.IsValid() && maybeSpanCtx.IsSampled() {
			exemplarLabels := prometheus.Labels{
				"trace_id": maybeSpanCtx.TraceID().String(),
				"span_id":  maybeSpanCtx.SpanID().String(),
			}
			m.AddWithExemplar(v, exemplarLabels)
			return
		}
	}

	e.delegate.Add(v)
}

// Inc attaches an exemplar to the metric and then calls the delegate.
// Inc 是 Add(1) 的便捷写法。
func (e *exemplarCounterMetric) Inc() {
	e.Add(1)
}

// CounterVec is the internal representation of our wrapping struct around prometheus
// counterVecs. CounterVec implements both kubeCollector and CounterVecMetric.
// CounterVec 是对 prometheus CounterVec 的包装；注册前成员提取返回固定 noop。
type CounterVec struct {
	*prometheus.CounterVec
	*CounterOpts
	lazyMetric
	originalLabels []string
}

var _ kubeCollector = &CounterVec{}

// TODO: make this true: var _ CounterVecMetric = &CounterVec{}

// NewCounterVec returns an object which satisfies the kubeCollector and (almost) CounterVecMetric interfaces.
// However, the object returned will not measure anything unless the collector is first
// registered, since the metric is lazily instantiated, and only members extracted after
// registration will actually measure anything.
func NewCounterVec(opts *CounterOpts, labels []string) *CounterVec {
	opts.StabilityLevel.setDefaults()

	fqName := BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	cv := &CounterVec{
		CounterVec:     noopCounterVec,
		CounterOpts:    opts,
		originalLabels: labels,
		lazyMetric:     lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	cv.lazyInit(cv, fqName)
	return cv
}

// DeprecatedVersion returns a pointer to the Version or nil
func (v *CounterVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.CounterOpts.DeprecatedVersion)

}

// initializeMetric invocation creates the actual underlying CounterVec. Until this method is called
// the underlying counterVec is a no-op.
func (v *CounterVec) initializeMetric() {
	v.CounterOpts.annotateStabilityLevel()
	v.CounterVec = prometheus.NewCounterVec(v.CounterOpts.toPromCounterOpts(), v.originalLabels)
}

// initializeDeprecatedMetric invocation creates the actual (but deprecated) CounterVec. Until this method is called
// the underlying counterVec is a no-op.
func (v *CounterVec) initializeDeprecatedMetric() {
	v.CounterOpts.markDeprecated()
	v.initializeMetric()
}

// Default Prometheus Vec behavior is that member extraction results in creation of a new element
// if one with the unique label values is not found in the underlying stored metricMap.
// This means  that if this function is called but the underlying metric is not registered
// (which means it will never be exposed externally nor consumed), the metric will exist in memory
// for perpetuity (i.e. throughout application lifecycle).
//
// For reference: https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/counter.go#L179-L197
//
// In contrast, the Vec behavior in this package is that member extraction before registration
// returns a permanent noop object.

// WithLabelValues returns the Counter for the given slice of label
// values (same order as the VariableLabels in Desc). If that combination of
// label values is accessed for the first time, a new Counter is created IFF the counterVec
// has been registered to a metrics registry.
// 未注册时返回共享的 noop；注册后才会真正创建成员，并按 allow list 约束标签值。
func (v *CounterVec) WithLabelValues(lvs ...string) CounterMetric {
	if !v.IsCreated() {
		return noop // 返回 noop counter
	}

	// 首次使用时从全局 allow list 初始化，避免每次调用都加读锁。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 把不在允许列表中的标签值替换为 "unexpected"，限制基数。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainToAllowedList(v.originalLabels, lvs)
	}

	return v.CounterVec.WithLabelValues(lvs...)
}

// With returns the Counter for the given Labels map (the label names
// must match those of the VariableLabels in Desc). If that label map is
// accessed for the first time, a new Counter is created IFF the counterVec has
// been registered to a metrics registry.
func (v *CounterVec) With(labels map[string]string) CounterMetric {
	if !v.IsCreated() {
		return noop // 返回 noop counter
	}

	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// map 形式的标签同样要收敛到允许值，并在越界时计数。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}

	return v.CounterVec.With(labels)
}

// Delete deletes the metric where the variable labels are the same as those
// passed in as labels. It returns true if a metric was deleted.
//
// It is not an error if the number and names of the Labels are inconsistent
// with those of the VariableLabels in Desc. However, such inconsistent Labels
// can never match an actual metric, so the method will always return false in
// that case.
func (v *CounterVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 未创建指标，自然也没有删除任何成员
	}
	return v.CounterVec.Delete(labels)
}

// Reset deletes all metrics in this vector.
func (v *CounterVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.CounterVec.Reset()
}

// ResetLabelAllowLists resets the label allow list for the CounterVec.
// NOTE: This should only be used in test.
func (v *CounterVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext returns wrapped CounterVec with context
// WithContext 返回带 context 的 vec 包装，子指标的 Observe/Add 会携带 exemplar。
func (v *CounterVec) WithContext(ctx context.Context) *CounterVecWithContext {
	return &CounterVecWithContext{
		ctx:        ctx,
		CounterVec: v,
	}
}

// CounterVecWithContext is the wrapper of CounterVec with context.
type CounterVecWithContext struct {
	*CounterVec
	ctx context.Context
}

// WithLabelValues is the wrapper of CounterVec.WithLabelValues.
func (vc *CounterVecWithContext) WithLabelValues(lvs ...string) CounterMetric {
	return vc.CounterVec.WithLabelValues(lvs...)
}

// With is the wrapper of CounterVec.With.
func (vc *CounterVecWithContext) With(labels map[string]string) CounterMetric {
	return vc.CounterVec.With(labels)
}
