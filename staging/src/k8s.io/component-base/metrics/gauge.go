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

	"k8s.io/component-base/version"
)

// Gauge 包装 prometheus Gauge，同时实现 kubeCollector 与 GaugeMetric。
// 注册前使用共享 noop，注册时才替换为真正的 prometheus.Gauge。
type Gauge struct {
	GaugeMetric
	*GaugeOpts
	lazyMetric
	selfCollector
}

var _ GaugeMetric = &Gauge{}
var _ Registerable = &Gauge{}
var _ kubeCollector = &Gauge{}

// NewGauge 返回实现 kubeCollector、Registerable 与 GaugeMetric 的对象。
// 指标惰性创建，因此注册前不会记录任何数据。
func NewGauge(opts *GaugeOpts) *Gauge {
	// 空 stability level 默认为 ALPHA，避免未声明稳定性时被当作 STABLE。
	opts.StabilityLevel.setDefaults()

	kc := &Gauge{
		GaugeOpts:  opts,
		lazyMetric: lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	kc.setPrometheusGauge(noop)
	kc.lazyInit(kc, BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return kc
}

// setPrometheusGauge 设置实际负责测量的 GaugeMetric，并同步更新 selfCollector。
func (g *Gauge) setPrometheusGauge(gauge prometheus.Gauge) {
	g.GaugeMetric = gauge
	g.initSelfCollection(gauge)
}

// DeprecatedVersion 解析并返回弃用版本；未配置时返回 nil。
func (g *Gauge) DeprecatedVersion() *semver.Version {
	return parseSemver(g.GaugeOpts.DeprecatedVersion)
}

// initializeMetric 在注册时创建真正的 Gauge；创建前底层 gauge 为 noop。
func (g *Gauge) initializeMetric() {
	// 稳定性与弃用信息通过 Help 前缀暴露，必须在构造 prometheus 描述符前附加。
	g.GaugeOpts.annotateStabilityLevel()
	// 此处创建底层 prometheus gauge。
	g.setPrometheusGauge(prometheus.NewGauge(g.GaugeOpts.toPromGaugeOpts()))
}

// initializeDeprecatedMetric 创建已弃用的 Gauge，并在 Help 中追加弃用说明。
func (g *Gauge) initializeDeprecatedMetric() {
	// 先标记弃用，使 Help 带上弃用版本，再按普通路径创建。
	g.GaugeOpts.markDeprecated()
	g.initializeMetric()
}

// WithContext 接收上下文并返回原 GaugeMetric；当前 gauge 操作不使用上下文。
func (g *Gauge) WithContext(ctx context.Context) GaugeMetric {
	return g.GaugeMetric
}

// GaugeVec 包装 prometheus GaugeVec，同时实现 kubeCollector 与 GaugeVecMetric。
type GaugeVec struct {
	*prometheus.GaugeVec
	*GaugeOpts
	lazyMetric
	originalLabels []string
}

var _ GaugeVecMetric = &GaugeVec{}
var _ Registerable = &GaugeVec{}
var _ kubeCollector = &GaugeVec{}

// NewGaugeVec 返回实现 kubeCollector、Registerable 与 GaugeVecMetric 的对象。
// 指标惰性创建，且只有注册后提取的成员才会真正记录数据。
func NewGaugeVec(opts *GaugeOpts, labels []string) *GaugeVec {
	// 与 Gauge 相同，未声明稳定性时按 ALPHA 处理。
	opts.StabilityLevel.setDefaults()

	fqName := BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	cv := &GaugeVec{
		// 初始用 noop 占位；注册前取子指标不会创建永久驻留的内存对象。
		GaugeVec:       noopGaugeVec,
		GaugeOpts:      opts,
		originalLabels: labels,
		lazyMetric:     lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	cv.lazyInit(cv, fqName)
	return cv
}

// DeprecatedVersion 解析并返回弃用版本；未配置时返回 nil。
func (v *GaugeVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.GaugeOpts.DeprecatedVersion)
}

// initializeMetric 在注册时创建真正的 GaugeVec；创建前底层 gaugeVec 为 noop。
func (v *GaugeVec) initializeMetric() {
	v.GaugeOpts.annotateStabilityLevel()
	v.GaugeVec = prometheus.NewGaugeVec(v.GaugeOpts.toPromGaugeOpts(), v.originalLabels)
}

// initializeDeprecatedMetric 创建已弃用的 GaugeVec，并在 Help 中追加弃用说明。
func (v *GaugeVec) initializeDeprecatedMetric() {
	v.GaugeOpts.markDeprecated()
	v.initializeMetric()
}

// WithLabelValuesChecked 按标签值顺序返回子 gauge，并检查注册状态及标签是否合法。
// 隐藏指标返回 noop 且无错误；未注册返回 noop 和 ErrIsNotRegistered。
func (v *GaugeVec) WithLabelValuesChecked(lvs ...string) (GaugeMetric, error) {
	if !v.IsCreated() {
		if v.IsHidden() {
			return noop, nil
		}
		return noop, errNotRegistered
	}

	// 允许列表可能在指标创建后才加载，因此用 Once 延迟绑定。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 将允许列表之外的标签值替换为 "unexpected"，以限制指标基数。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainToAllowedList(v.originalLabels, lvs)
	}

	return v.GetMetricWithLabelValues(lvs...)
}

// DeleteLabelValuesChecked 仅在指标已创建时删除指定标签值的子指标。
// 隐藏或未注册向量均视为无可删成员，分别返回 false、nil 或 false、ErrIsNotRegistered。
func (v *GaugeVec) DeleteLabelValuesChecked(lvs ...string) (bool, error) {
	if !v.IsCreated() {
		if v.IsHidden() {
			return false, nil
		}
		return false, errNotRegistered
	}

	return v.GaugeVec.DeleteLabelValues(lvs...), nil
}

// prometheus Vec 默认会在底层 metricMap 中找不到对应标签值时创建新成员。
// 因此，若在注册前提取成员，该指标虽不会对外暴露或消费，却会永久驻留内存，直到应用结束。
//
// 参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/gauge.go#L190-L208
//
// 相比之下，本包在注册前提取成员时返回永久 noop 对象。

// WithLabelValues 按标签值顺序返回 GaugeMetric，顺序应与 Desc 中的 VariableLabels 一致。
// 首次访问某组标签值时，只有 gaugeVec 已注册才会创建新指标。
func (v *GaugeVec) WithLabelValues(lvs ...string) GaugeMetric {
	ans, err := v.WithLabelValuesChecked(lvs...)
	// 未注册不算错误（返回 noop），其它错误（如标签数量不符）则 panic。
	if err == nil || ErrIsNotRegistered(err) {
		return ans
	}
	panic(err)
}

// DeleteLabelValues 删除指定标签值的子指标；隐藏或未注册时返回 false。
func (v *GaugeVec) DeleteLabelValues(lvs ...string) bool {
	ans, err := v.DeleteLabelValuesChecked(lvs...)
	if err == nil || ErrIsNotRegistered(err) {
		return ans
	}
	panic(err)
}

// WithChecked 与 WithLabelValuesChecked 类似，但使用完整的标签 map。
func (v *GaugeVec) WithChecked(labels map[string]string) (GaugeMetric, error) {
	if !v.IsCreated() {
		if v.IsHidden() {
			return noop, nil
		}
		return noop, errNotRegistered
	}

	// 若尚未初始化，则延迟绑定标签允许列表。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 将标签 map 约束到允许值。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}

	return v.GetMetricWith(labels)
}

// With 按标签 map 返回 GaugeMetric，标签名必须与 Desc 中的 VariableLabels 一致。
// 首次访问该标签 map 时，只有 gaugeVec 已注册才会创建新指标。
func (v *GaugeVec) With(labels map[string]string) GaugeMetric {
	ans, err := v.WithChecked(labels)
	if err == nil || ErrIsNotRegistered(err) {
		return ans
	}
	panic(err)
}

// Delete 删除变量标签与给定标签 map 相匹配的指标，删除成功时返回 true。
// 标签数量或名称与 Desc 中的 VariableLabels 不一致不算错误，但不可能匹配实际指标，因此始终返回 false。
func (v *GaugeVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 尚未创建指标，因此未删除任何成员
	}
	return v.GaugeVec.Delete(labels)
}

// Reset 删除向量中的所有指标。
func (v *GaugeVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.GaugeVec.Reset()
}

// ResetLabelAllowLists 重置 GaugeVec 的标签允许列表。
// 注意：仅供测试使用。
func (v *GaugeVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

func newGaugeFunc(opts *GaugeOpts, function func() float64, v semver.Version) GaugeFunc {
	g := NewGauge(opts)

	// 隐藏的 GaugeFunc 返回 nil，调用方据此避免注册一个永远不产出的 collector。
	if !g.Create(&v) {
		return nil
	}

	return prometheus.NewGaugeFunc(g.GaugeOpts.toPromGaugeOpts(), function)
}

// NewGaugeFunc 根据 GaugeOpts 创建 GaugeFunc，其值在 Write 时调用给定函数获得。
// 指标采集可能并发进行；若多个 Write 并发调用该函数（例如直接向 prometheus 注册 GaugeFunc），函数必须并发安全。
func NewGaugeFunc(opts *GaugeOpts, function func() float64) GaugeFunc {
	v := parseVersion(version.Get())

	return newGaugeFunc(opts, function, v)
}

// WithContext 返回带上下文的 GaugeVec 包装。
func (v *GaugeVec) WithContext(ctx context.Context) *GaugeVecWithContext {
	return &GaugeVecWithContext{
		ctx:      ctx,
		GaugeVec: v,
	}
}

// InterfaceWithContext 返回带上下文的 GaugeVecMetric。
func (v *GaugeVec) InterfaceWithContext(ctx context.Context) GaugeVecMetric {
	return v.WithContext(ctx)
}

// GaugeVecWithContext 是带上下文的 GaugeVec 包装。
// 目前 context 仅被保存而不参与指标操作，保留它是为了 API 一致性。
type GaugeVecWithContext struct {
	*GaugeVec
	ctx context.Context
}

// WithLabelValues 包装 GaugeVec.WithLabelValues。
func (vc *GaugeVecWithContext) WithLabelValues(lvs ...string) GaugeMetric {
	return vc.GaugeVec.WithLabelValues(lvs...)
}

// With 包装 GaugeVec.With。
func (vc *GaugeVecWithContext) With(labels map[string]string) GaugeMetric {
	return vc.GaugeVec.With(labels)
}
