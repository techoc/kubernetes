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

// Histogram 是对 prometheus.Histogram 的内部包装，同时实现 kubeCollector 与 ObserverMetric。
// 注册前为 noop，首次注册时才真正创建底层直方图。
type Histogram struct {
	ObserverMetric
	*HistogramOpts
	lazyMetric
	selfCollector
}

// exemplarHistogramMetric 保存用于提取 exemplar 标签的 context，以及承载 exemplar 的直方图；实现 metricWithExemplar 接口。
type exemplarHistogramMetric struct {
	ctx      context.Context
	delegate ObserverMetric
}

// exemplarHistogramVec 把带 context 的 vec 成员与具体 observer 绑定，用于附加 exemplar。
type exemplarHistogramVec struct {
	*HistogramVecWithContext
	observer prometheus.Observer
}

// Observe 在 context 中存在已采样 span 时附上 trace_id/span_id，否则执行普通观测。
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

// NewHistogram 返回一个类似 Histogram 的对象；注册前不会记录任何观测。
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

// setPrometheusHistogram 设置执行实际观测的底层 prometheus.Histogram。
func (h *Histogram) setPrometheusHistogram(histogram prometheus.Histogram) {
	h.ObserverMetric = histogram
	h.initSelfCollection(histogram)
}

// DeprecatedVersion 返回已解析的弃用版本；未设置时返回 nil。
func (h *Histogram) DeprecatedVersion() *semver.Version {
	return parseSemver(h.HistogramOpts.DeprecatedVersion)
}

// initializeMetric 创建并保存底层 prometheus.Histogram。
func (h *Histogram) initializeMetric() {
	h.HistogramOpts.annotateStabilityLevel()
	// 这里才真正创建底层 prometheus histogram。
	h.setPrometheusHistogram(prometheus.NewHistogram(h.HistogramOpts.toPromHistogramOpts()))
}

// initializeDeprecatedMetric 先修改 Help 中的弃用说明，再创建 prometheus.Histogram。
func (h *Histogram) initializeDeprecatedMetric() {
	h.HistogramOpts.markDeprecated()
	h.initializeMetric()
}

// WithContext 返回带 context 的包装，观测时可携带 trace exemplar。
func (h *Histogram) WithContext(ctx context.Context) ObserverMetric {
	return &exemplarHistogramMetric{ctx: ctx, delegate: h.ObserverMetric}
}

// HistogramVec 是对 prometheus.HistogramVec 的内部包装。
type HistogramVec struct {
	*prometheus.HistogramVec
	*HistogramOpts
	lazyMetric
	originalLabels []string
}

// NewHistogramVec 返回实现 kubeCollector 的 prometheus.HistogramVec 包装。
// 指标采用懒创建；只有注册后获取的成员才会记录观测。

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

// DeprecatedVersion 返回已解析的弃用版本；未设置时返回 nil。
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

// Prometheus Vec 默认会在提取成员时，为底层 metricMap 中不存在的标签组合创建元素。
// 若指标尚未注册，该元素永远不会对外暴露或被消费，却会一直占用内存。
//
// 参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/histogram.go#L460-L470
//
// 本包的 Vec 在注册前提取成员时只返回永久的 noop 对象。

// WithLabelValues 返回给定标签值序列对应的 ObserverMetric，顺序须与 Desc 中的
// VariableLabels 一致。首次访问该组合时，仅当 HistogramVec 已注册才创建成员。
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

// With 返回给定 Labels 映射对应的 ObserverMetric，标签名须与 Desc 中的
// VariableLabels 一致。首次访问该组合时，仅当 HistogramVec 已注册才创建成员。
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

// ObserveSince 返回一个记录自 start 起经过时长的函数，可配合 defer 测量耗时：
//
//	start := time.Now()
//	defer metricVec.ObserveSince(start, "label1", "label2")()
//
// 必须调用返回的函数（通常通过 defer）才能记录观测。
// 这种写法避免 defer 的参数立即求值，导致耗时测量不正确。
func (v *HistogramVec) ObserveSince(start time.Time, lvs ...string) func() {
	// 真正的耗时在返回函数被调用时计算，而不是在 defer 时；否则测到的只是零。
	return func() {
		v.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
	}
}

// Delete 删除可变标签与给定 Labels 匹配的成员，删除成功时返回 true。
//
// Labels 的数量或名称与 Desc 中的 VariableLabels 不一致不会报错，
// 但这样的 Labels 无法匹配实际成员，因此始终返回 false。
func (v *HistogramVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 未创建指标，自然也没有删除任何成员
	}
	return v.HistogramVec.Delete(labels)
}

// Reset 删除该 vector 中的所有成员。
func (v *HistogramVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.HistogramVec.Reset()
}

// ResetLabelAllowLists 重置 HistogramVec 的标签允许列表。
// 注意：仅供测试使用。
func (v *HistogramVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext 返回带 context 的 HistogramVec 包装。
func (v *HistogramVec) WithContext(ctx context.Context) *HistogramVecWithContext {
	return &HistogramVecWithContext{
		ctx:          ctx,
		HistogramVec: v,
	}
}

// HistogramVecWithContext 是带 context 的 HistogramVec 包装。
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

// WithLabelValues 包装 HistogramVec.WithLabelValues，并保留 context 以附加 exemplar。
func (vc *HistogramVecWithContext) WithLabelValues(lvs ...string) *exemplarHistogramVec {
	return &exemplarHistogramVec{
		HistogramVecWithContext: vc,
		observer:                vc.HistogramVec.WithLabelValues(lvs...),
	}
}

// With 包装 HistogramVec.With，并保留 context 以附加 exemplar。
func (vc *HistogramVecWithContext) With(labels map[string]string) *exemplarHistogramVec {
	return &exemplarHistogramVec{
		HistogramVecWithContext: vc,
		observer:                vc.HistogramVec.With(labels),
	}
}

// ObserveSince 包装 HistogramVec.ObserveSince，返回记录经过时长的函数。
func (vc *HistogramVecWithContext) ObserveSince(start time.Time, lvs ...string) func() {
	return vc.HistogramVec.ObserveSince(start, lvs...)
}
