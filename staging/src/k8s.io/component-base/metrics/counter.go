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

// Counter 包装 prometheus Counter，同时实现 kubeCollector 与 CounterMetric。
// 首次注册前使用惰性 noop：未注册时不会真正计数，也不会驻留内存。
type Counter struct {
	CounterMetric
	*CounterOpts
	lazyMetric
	selfCollector
}

// testutil.GetCounterMetricValue 要求实现 Metric 接口。
var _ Metric = &Counter{}

// exemplarCounterMetric 保存用于提取 exemplar 标签的上下文及目标计数指标，并实现 metricWithExemplar。
type exemplarCounterMetric struct {
	ctx      context.Context
	delegate CounterMetric
}

// NewCounter 返回实现 kubeCollector 与 CounterMetric 的对象。
// 指标惰性创建，因此注册前不会记录任何数据。
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

// Reset 将底层 prometheus Counter 清零；尚未创建时直接返回，避免无意义地替换 noop。
func (c *Counter) Reset() {
	if !c.IsCreated() {
		return
	}
	c.setPrometheusCounter(prometheus.NewCounter(c.CounterOpts.toPromCounterOpts()))
}

// setPrometheusCounter 设置实际负责计数的 CounterMetric，并同步更新 selfCollector，使 Describe/Collect 使用同一实现。
func (c *Counter) setPrometheusCounter(counter prometheus.Counter) {
	c.CounterMetric = counter
	c.initSelfCollection(counter)
}

// DeprecatedVersion 解析并返回弃用版本；未配置时返回 nil。
func (c *Counter) DeprecatedVersion() *semver.Version {
	return parseSemver(c.CounterOpts.DeprecatedVersion)
}

// initializeMetric 在注册时创建真正的 prometheus Counter，并在 Help 中标注稳定性级别。
func (c *Counter) initializeMetric() {
	c.CounterOpts.annotateStabilityLevel()
	// 此处才创建底层 prometheus counter。
	c.setPrometheusCounter(prometheus.NewCounter(c.CounterOpts.toPromCounterOpts()))
}

// initializeDeprecatedMetric 创建已弃用的 prometheus Counter，并在 Help 中追加弃用说明。
func (c *Counter) initializeDeprecatedMetric() {
	c.CounterOpts.markDeprecated()
	c.initializeMetric()
}

// WithContext 返回带上下文的包装，用于把 trace 信息作为 exemplar 上报。
func (c *Counter) WithContext(ctx context.Context) CounterMetric {
	return &exemplarCounterMetric{ctx: ctx, delegate: c.CounterMetric}
}

// Add 先检查上下文中的 span 是否已采样；是则附带 trace_id/span_id 作为 exemplar，否则按普通 Add 处理，避免 exemplar 缺失影响计数。
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

// Inc 先附带 exemplar，再委托底层指标执行 Add(1)。
func (e *exemplarCounterMetric) Inc() {
	e.Add(1)
}

// CounterVec 包装 prometheus CounterVec，同时实现 kubeCollector 与 CounterVecMetric。
// 注册前提取成员会返回固定 noop。
type CounterVec struct {
	*prometheus.CounterVec
	*CounterOpts
	lazyMetric
	originalLabels []string
}

var _ kubeCollector = &CounterVec{}

// TODO: 使以下断言成立：var _ CounterVecMetric = &CounterVec{}

// NewCounterVec 返回实现 kubeCollector 及大部分 CounterVecMetric 方法的对象。
// 指标惰性创建，且只有注册后提取的成员才会真正记录数据。
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

// DeprecatedVersion 解析并返回弃用版本；未配置时返回 nil。
func (v *CounterVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.CounterOpts.DeprecatedVersion)

}

// initializeMetric 在注册时创建真正的 CounterVec；创建前底层 counterVec 为 noop。
func (v *CounterVec) initializeMetric() {
	v.CounterOpts.annotateStabilityLevel()
	v.CounterVec = prometheus.NewCounterVec(v.CounterOpts.toPromCounterOpts(), v.originalLabels)
}

// initializeDeprecatedMetric 创建已弃用的 CounterVec，并在 Help 中追加弃用说明。
func (v *CounterVec) initializeDeprecatedMetric() {
	v.CounterOpts.markDeprecated()
	v.initializeMetric()
}

// prometheus Vec 默认会在底层 metricMap 中找不到对应标签值时创建新成员。
// 因此，若在注册前提取成员，该指标虽不会对外暴露或消费，却会永久驻留内存，直到应用结束。
//
// 参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/counter.go#L179-L197
//
// 相比之下，本包在注册前提取成员时返回永久 noop 对象。

// WithLabelValues 按标签值顺序返回 Counter，顺序应与 Desc 中的 VariableLabels 一致。
// 首次访问某组标签值时，只有 counterVec 已注册才会创建新 Counter。
// 未注册时返回共享 noop；注册后按 allow list 约束标签值。
func (v *CounterVec) WithLabelValues(lvs ...string) CounterMetric {
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

	return v.CounterVec.WithLabelValues(lvs...)
}

// With 按标签 map 返回 Counter，标签名必须与 Desc 中的 VariableLabels 一致。
// 首次访问该标签 map 时，只有 counterVec 已注册才会创建新 Counter。
// 未注册时返回共享 noop；注册后按允许列表约束标签值。
func (v *CounterVec) With(labels map[string]string) CounterMetric {
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

	// map 形式的标签同样要收敛到允许值，并在越界时计数。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}

	return v.CounterVec.With(labels)
}

// Delete 删除变量标签与给定标签 map 相匹配的指标，删除成功时返回 true。
// 标签数量或名称与 Desc 中的 VariableLabels 不一致不算错误，但不可能匹配实际指标，因此始终返回 false。
func (v *CounterVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 尚未创建指标，因此也未删除任何成员
	}
	return v.CounterVec.Delete(labels)
}

// Reset 删除向量中的所有指标。
func (v *CounterVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.CounterVec.Reset()
}

// ResetLabelAllowLists 重置 CounterVec 的标签允许列表。
// 注意：仅供测试使用。
func (v *CounterVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext 返回带上下文的 CounterVec 包装，使子指标的 Add 携带 exemplar。
func (v *CounterVec) WithContext(ctx context.Context) *CounterVecWithContext {
	return &CounterVecWithContext{
		ctx:        ctx,
		CounterVec: v,
	}
}

// CounterVecWithContext 是带上下文的 CounterVec 包装。
type CounterVecWithContext struct {
	*CounterVec
	ctx context.Context
}

// WithLabelValues 是 CounterVec.WithLabelValues 的包装。
func (vc *CounterVecWithContext) WithLabelValues(lvs ...string) CounterMetric {
	return vc.CounterVec.WithLabelValues(lvs...)
}

// With 是 CounterVec.With 的包装。
func (vc *CounterVecWithContext) With(labels map[string]string) CounterMetric {
	return vc.CounterVec.With(labels)
}
