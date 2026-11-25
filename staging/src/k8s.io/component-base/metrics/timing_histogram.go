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
	promext "k8s.io/component-base/metrics/prometheusextension"
)

// PrometheusTimingHistogram 从包装器提升底层计时直方图，仅暴露 GaugeMetric。
// timing histogram 记录“当前值 × 持续时长”的积分，而非离散观测。
type PrometheusTimingHistogram interface {
	GaugeMetric
}

// TimingHistogram 是计时直方图的内部包装结构，实现 kubeCollector 和 GaugeMetric。
type TimingHistogram struct {
	PrometheusTimingHistogram
	*TimingHistogramOpts
	nowFunc func() time.Time
	lazyMetric
	selfCollector
}

var _ GaugeMetric = &TimingHistogram{}
var _ Registerable = &TimingHistogram{}
var _ kubeCollector = &TimingHistogram{}

// NewTimingHistogram 创建类似 TimingHistogram 的对象，注册前不会进行测量。
// 它使用真实时钟；测试需注入时钟时请用 NewTestableTimingHistogram。
func NewTimingHistogram(opts *TimingHistogramOpts) *TimingHistogram {
	return NewTestableTimingHistogram(time.Now, opts)
}

// NewTestableTimingHistogram 允许注入时钟，以精确构造“某值持续了多久”的场景，
// 避免测试依赖真实时间而变得不稳定。
func NewTestableTimingHistogram(nowFunc func() time.Time, opts *TimingHistogramOpts) *TimingHistogram {
	opts.StabilityLevel.setDefaults()

	h := &TimingHistogram{
		TimingHistogramOpts: opts,
		nowFunc:             nowFunc,
		lazyMetric:          lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	h.setPrometheusHistogram(noopMetric{})
	h.lazyInit(h, BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return h
}

// setPrometheusHistogram 设置执行实际测量的底层计时直方图。
func (h *TimingHistogram) setPrometheusHistogram(histogram promext.TimingHistogram) {
	h.PrometheusTimingHistogram = histogram
	h.initSelfCollection(histogram)
}

// DeprecatedVersion 解析 DeprecatedVersion 字符串；未设置时返回 nil，表示未弃用。
func (h *TimingHistogram) DeprecatedVersion() *semver.Version {
	return parseSemver(h.TimingHistogramOpts.DeprecatedVersion)
}

// initializeMetric 创建底层 prometheus 计时直方图并保存引用。
// 创建失败通常是桶配置非法；与标准 Histogram 保持一致，直接 panic。
func (h *TimingHistogram) initializeMetric() {
	h.TimingHistogramOpts.annotateStabilityLevel()
	// 这里实际创建底层 prometheus gauge。
	histogram, err := promext.NewTestableTimingHistogram(h.nowFunc, h.TimingHistogramOpts.toPromHistogramOpts())
	if err != nil {
		panic(err) // 与普通直方图的处理方式一致
	}
	h.setPrometheusHistogram(histogram)
}

// initializeDeprecatedMetric 先修改 Help 中的弃用说明，再创建底层 prometheus 计时直方图。
func (h *TimingHistogram) initializeDeprecatedMetric() {
	h.TimingHistogramOpts.markDeprecated()
	h.initializeMetric()
}

// WithContext 允许普通 TimingHistogram 指标传入 context；目前 context 不产生实际作用。
func (h *TimingHistogram) WithContext(ctx context.Context) GaugeMetric {
	return h.PrometheusTimingHistogram
}

// TimingHistogramVec 是 prometheus TimingHistogramVec 的内部包装结构。
type TimingHistogramVec struct {
	*promext.TimingHistogramVec
	*TimingHistogramOpts
	nowFunc func() time.Time
	lazyMetric
	originalLabels []string
}

var _ GaugeVecMetric = &TimingHistogramVec{}
var _ Registerable = &TimingHistogramVec{}
var _ kubeCollector = &TimingHistogramVec{}

// NewTimingHistogramVec 创建实现 kubeCollector、Registerable 和 GaugeVecMetric
// 接口的包装对象，内部封装 promext.TimingHistogramVec。
// 未注册时底层是 noopTimingHistogramVec；隐藏指标注册后仍不会对外暴露。
func NewTimingHistogramVec(opts *TimingHistogramOpts, labels []string) *TimingHistogramVec {
	return NewTestableTimingHistogramVec(time.Now, opts, labels)
}

// NewTestableTimingHistogramVec 创建允许注入时钟的 TimingHistogramVec。
func NewTestableTimingHistogramVec(nowFunc func() time.Time, opts *TimingHistogramOpts, labels []string) *TimingHistogramVec {
	opts.StabilityLevel.setDefaults()

	fqName := BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	v := &TimingHistogramVec{
		TimingHistogramVec:  noopTimingHistogramVec,
		TimingHistogramOpts: opts,
		nowFunc:             nowFunc,
		originalLabels:      labels,
		lazyMetric:          lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	v.lazyInit(v, fqName)
	return v
}

// DeprecatedVersion 解析 DeprecatedVersion 字符串；未设置时返回 nil，表示未弃用。
func (v *TimingHistogramVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.TimingHistogramOpts.DeprecatedVersion)
}

// initializeMetric 在注册时用注入的时钟创建真实 vec。
func (v *TimingHistogramVec) initializeMetric() {
	v.TimingHistogramOpts.annotateStabilityLevel()
	v.TimingHistogramVec = promext.NewTestableTimingHistogramVec(v.nowFunc, v.TimingHistogramOpts.toPromHistogramOpts(), v.originalLabels...)
}

// initializeDeprecatedMetric 先给 Help 加弃用标记再创建。
func (v *TimingHistogramVec) initializeDeprecatedMetric() {
	v.TimingHistogramOpts.markDeprecated()
	v.initializeMetric()
}

// WithLabelValuesChecked 按位置标签返回子 gauge：
// 未注册时返回 noop 和可通过 ErrIsNotRegistered 识别的错误；
// 隐藏向量返回 noop 和 nil；标签语法错误返回 noop 和相应错误；
// 其他情况返回对应子指标和 nil。
func (v *TimingHistogramVec) WithLabelValuesChecked(lvs ...string) (GaugeMetric, error) {
	if !v.IsCreated() {
		if v.IsHidden() {
			return noop, nil
		}
		return noop, errNotRegistered
	}

	// 尚未初始化时，加载标签值允许列表。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 将位置标签值限制在允许集合内。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainToAllowedList(v.originalLabels, lvs)
	}
	ops, err := v.TimingHistogramVec.GetMetricWithLabelValues(lvs...)
	if err != nil {
		return noop, err
	}
	return ops.(GaugeMetric), err
}

// WithLabelValues 调用 WithLabelValuesChecked：
// 忽略可通过 ErrIsNotRegistered 识别的错误并返回 noop，其他错误触发 panic。
func (v *TimingHistogramVec) WithLabelValues(lvs ...string) GaugeMetric {
	ans, err := v.WithLabelValuesChecked(lvs...)
	if err == nil || ErrIsNotRegistered(err) {
		return ans
	}
	panic(err)
}

// WithChecked 按 map 标签返回子 gauge：
// 未注册时返回 noop 和可通过 ErrIsNotRegistered 识别的错误；
// 隐藏向量返回 noop 和 nil；标签语法错误返回 noop 和相应错误；
// 其他情况返回对应子指标和 nil。
func (v *TimingHistogramVec) WithChecked(labels map[string]string) (GaugeMetric, error) {
	if !v.IsCreated() {
		if v.IsHidden() {
			return noop, nil
		}
		return noop, errNotRegistered
	}

	// 尚未初始化时，加载标签值允许列表。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 将 map 标签值限制在允许集合内。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}
	ops, err := v.TimingHistogramVec.GetMetricWith(labels)
	if err != nil {
		return noop, err
	}
	return ops.(GaugeMetric), err
}

// With 调用 WithChecked：
// 忽略可通过 ErrIsNotRegistered 识别的错误并返回 noop，其他错误触发 panic。
func (v *TimingHistogramVec) With(labels map[string]string) GaugeMetric {
	ans, err := v.WithChecked(labels)
	if err == nil || ErrIsNotRegistered(err) {
		return ans
	}
	panic(err)
}

// Delete 删除可变标签与给定标签匹配的指标，删除成功时返回 true。
// 标签数量或名称与 Desc 中的 VariableLabels 不一致不会报错，但无法匹配任何指标，
// 因此始终返回 false。
func (v *TimingHistogramVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 指标尚未创建，因此未删除任何与给定标签匹配的指标
	}
	return v.TimingHistogramVec.Delete(labels)
}

// Reset 删除此向量中的所有指标。
func (v *TimingHistogramVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.TimingHistogramVec.Reset()
}

// ResetLabelAllowLists 重置 TimingHistogramVec 的标签值允许列表。
// 注意：仅应在测试中使用。
func (v *TimingHistogramVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// InterfaceWithContext 返回携带 context 的 TimingHistogramVec 包装器。
// 方法名带 Interface 是为了避免与内嵌类型的 WithContext 冲突；当前 context 只保存不使用。
func (v *TimingHistogramVec) InterfaceWithContext(ctx context.Context) GaugeVecMetric {
	return &TimingHistogramVecWithContext{
		ctx:                ctx,
		TimingHistogramVec: v,
	}
}

// TimingHistogramVecWithContext 是携带 context 的 TimingHistogramVec 包装器。
// 当前 context 会被忽略。
type TimingHistogramVecWithContext struct {
	*TimingHistogramVec
	ctx context.Context
}
