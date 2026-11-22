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
	"time"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// Histogram is our internal representation for our wrapping struct around prometheus
// histograms. Summary implements both kubeCollector and ObserverMetric
// Histogram 是对 prometheus Histogram 的包装，同时实现 kubeCollector 与 ObserverMetric。
// 注册前为 noop，首次注册时才真正创建底层直方图。
type Histogram struct {
	ObserverMetric
	*HistogramOpts
	lazyMetric
	selfCollector
}

// exemplarHistogramMetric holds a context to extract exemplar labels from, and a historgram metric to attach them to. It implements the metricWithExemplar interface.
type exemplarHistogramMetric struct {
	ctx      context.Context
	delegate ObserverMetric
}

// exemplarHistogramVec 把带 context 的 vec 成员与具体 observer 绑定，用于附加 exemplar。
type exemplarHistogramVec struct {
	*HistogramVecWithContext
	observer prometheus.Observer
}

// Observe attaches an exemplar to the metric and then calls the delegate.
// Observe 在 context 中存在已采样 span 时附上 trace_id/span_id，否则普通观测。
func (e *exemplarHistogramMetric) Observe(v float64) {
	if m, ok := e.delegate.(prometheus.ExemplarObserver); ok {
		maybeSpanCtx := trace.SpanContextFromContext(e.ctx)
		if maybeSpanCtx.IsValid() && maybeSpanCtx.IsSampled() {
			exemplarLabels := prometheus.Labels{
				"trace_id": maybeSpanCtx.TraceID().String(),
				"span_id":  maybeSpanCtx.SpanID().String(),
			}
			m.ObserveWithExemplar(v, exemplarLabels)
			return
		}
	}

	e.delegate.Observe(v)
}

// NewHistogram returns an object which is Histogram-like. However, nothing
// will be measured until the histogram is registered somewhere.
func NewHistogram(opts *HistogramOpts) *Histogram {
	opts.StabilityLevel.setDefaults()

	h := &Histogram{
		HistogramOpts: opts,
		lazyMetric:    lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	h.setPrometheusHistogram(noopMetric{})
	h.lazyInit(h, BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return h
}

// setPrometheusHistogram sets the underlying KubeGauge object, i.e. the thing that does the measurement.
func (h *Histogram) setPrometheusHistogram(histogram prometheus.Histogram) {
	h.ObserverMetric = histogram
	h.initSelfCollection(histogram)
}

// DeprecatedVersion returns a pointer to the Version or nil
func (h *Histogram) DeprecatedVersion() *semver.Version {
	return parseSemver(h.HistogramOpts.DeprecatedVersion)
}

// initializeMetric invokes the actual prometheus.Histogram object instantiation
// and stores a reference to it
func (h *Histogram) initializeMetric() {
	h.HistogramOpts.annotateStabilityLevel()
	// 这里才真正创建底层 prometheus histogram。
	h.setPrometheusHistogram(prometheus.NewHistogram(h.HistogramOpts.toPromHistogramOpts()))
}

// initializeDeprecatedMetric invokes the actual prometheus.Histogram object instantiation
// but modifies the Help description prior to object instantiation.
func (h *Histogram) initializeDeprecatedMetric() {
	h.HistogramOpts.markDeprecated()
	h.initializeMetric()
}

// WithContext allows the normal Histogram metric to pass in context. The context is no-op now.
// WithContext 返回带 context 的包装，观测时可携带 trace exemplar。
func (h *Histogram) WithContext(ctx context.Context) ObserverMetric {
	return &exemplarHistogramMetric{ctx: ctx, delegate: h.ObserverMetric}
}

// HistogramVec is the internal representation of our wrapping struct around prometheus
// histogramVecs.
type HistogramVec struct {
	*prometheus.HistogramVec
	*HistogramOpts
	lazyMetric
	originalLabels []string
}

// NewHistogramVec returns an object which satisfies kubeCollector and wraps the
// prometheus.HistogramVec object. However, the object returned will not measure
// anything unless the collector is first registered, since the metric is lazily instantiated,
// and only members extracted after
// registration will actually measure anything.

func NewHistogramVec(opts *HistogramOpts, labels []string) *HistogramVec {
	opts.StabilityLevel.setDefaults()

	fqName := BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	v := &HistogramVec{
		HistogramVec:   noopHistogramVec,
		HistogramOpts:  opts,
		originalLabels: labels,
		lazyMetric:     lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	v.lazyInit(v, fqName)
	return v
}

// DeprecatedVersion returns a pointer to the Version or nil
func (v *HistogramVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.HistogramOpts.DeprecatedVersion)
}

// initializeMetric 创建真正的 prometheus HistogramVec。
func (v *HistogramVec) initializeMetric() {
	v.HistogramOpts.annotateStabilityLevel()
	v.HistogramVec = prometheus.NewHistogramVec(v.HistogramOpts.toPromHistogramOpts(), v.originalLabels)
}

// initializeDeprecatedMetric 为弃用指标创建 HistogramVec，先标注弃用版本。
func (v *HistogramVec) initializeDeprecatedMetric() {
	v.HistogramOpts.markDeprecated()
	v.initializeMetric()
}

// Default Prometheus Vec behavior is that member extraction results in creation of a new element
// if one with the unique label values is not found in the underlying stored metricMap.
// This means  that if this function is called but the underlying metric is not registered
// (which means it will never be exposed externally nor consumed), the metric will exist in memory
// for perpetuity (i.e. throughout application lifecycle).
//
// For reference: https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/histogram.go#L460-L470
//
// In contrast, the Vec behavior in this package is that member extraction before registration
// returns a permanent noop object.

// WithLabelValues returns the ObserverMetric for the given slice of label
// values (same order as the VariableLabels in Desc). If that combination of
// label values is accessed for the first time, a new ObserverMetric is created IFF the HistogramVec
// has been registered to a metrics registry.
func (v *HistogramVec) WithLabelValues(lvs ...string) ObserverMetric {
	if !v.IsCreated() {
		return noop
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
	return v.HistogramVec.WithLabelValues(lvs...)
}

// With returns the ObserverMetric for the given Labels map (the label names
// must match those of the VariableLabels in Desc). If that label map is
// accessed for the first time, a new ObserverMetric is created IFF the HistogramVec has
// been registered to a metrics registry.
func (v *HistogramVec) With(labels map[string]string) ObserverMetric {
	if !v.IsCreated() {
		return noop
	}

	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// map 形式的标签同样收敛到允许值。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}

	return v.HistogramVec.With(labels)
}

// ObserveSince returns a function that observes the duration since the given start time.
// This is intended to be used with defer to measure the time elapsed since a given point:
//
//	start := time.Now()
//	defer metricVec.ObserveSince(start, "label1", "label2")()
//
// The returned function must be called (typically via defer) to record the observation.
// This pattern avoids the common pitfall where arguments to deferred functions are evaluated
// immediately, which would result in incorrect timing measurements.
func (v *HistogramVec) ObserveSince(start time.Time, lvs ...string) func() {
	// 真正的耗时在返回函数被调用时计算，而不是在 defer 时；否则测到的只是零。
	return func() {
		v.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
	}
}

// Delete deletes the metric where the variable labels are the same as those
// passed in as labels. It returns true if a metric was deleted.
//
// It is not an error if the number and names of the Labels are inconsistent
// with those of the VariableLabels in Desc. However, such inconsistent Labels
// can never match an actual metric, so the method will always return false in
// that case.
func (v *HistogramVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 未创建指标，自然也没有删除任何成员
	}
	return v.HistogramVec.Delete(labels)
}

// Reset deletes all metrics in this vector.
func (v *HistogramVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.HistogramVec.Reset()
}

// ResetLabelAllowLists resets the label allow list for the HistogramVec.
// NOTE: This should only be used in test.
func (v *HistogramVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext returns wrapped HistogramVec with context
func (v *HistogramVec) WithContext(ctx context.Context) *HistogramVecWithContext {
	return &HistogramVecWithContext{
		ctx:          ctx,
		HistogramVec: v,
	}
}

// HistogramVecWithContext is the wrapper of HistogramVec with context.
type HistogramVecWithContext struct {
	*HistogramVec
	ctx context.Context
}

// Observe 观测一次，并在有已采样 span 时附加 exemplar。
func (h *exemplarHistogramVec) Observe(v float64) {
	h.withExemplar(v)
}

// withExemplar 在 observer 支持 exemplar 且 span 已采样时带 trace 观察，
// 否则退回普通 Observe。
func (h *exemplarHistogramVec) withExemplar(v float64) {
	if m, ok := h.observer.(prometheus.ExemplarObserver); ok {
		maybeSpanCtx := trace.SpanContextFromContext(h.HistogramVecWithContext.ctx)
		if maybeSpanCtx.IsValid() && maybeSpanCtx.IsSampled() {
			m.ObserveWithExemplar(v, prometheus.Labels{
				"trace_id": maybeSpanCtx.TraceID().String(),
				"span_id":  maybeSpanCtx.SpanID().String(),
			})
			return
		}
	}

	h.observer.Observe(v)
}

// WithLabelValues is the wrapper of HistogramVec.WithLabelValues.
func (vc *HistogramVecWithContext) WithLabelValues(lvs ...string) *exemplarHistogramVec {
	return &exemplarHistogramVec{
		HistogramVecWithContext: vc,
		observer:                vc.HistogramVec.WithLabelValues(lvs...),
	}
}

// With is the wrapper of HistogramVec.With.
func (vc *HistogramVecWithContext) With(labels map[string]string) *exemplarHistogramVec {
	return &exemplarHistogramVec{
		HistogramVecWithContext: vc,
		observer:                vc.HistogramVec.With(labels),
	}
}

// ObserveSince is the wrapper of HistogramVec.ObserveSince.
func (vc *HistogramVecWithContext) ObserveSince(start time.Time, lvs ...string) func() {
	return vc.HistogramVec.ObserveSince(start, lvs...)
}
