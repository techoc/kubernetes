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
)

const (
	// DefAgeBuckets 是 Prometheus 默认的 Summary 年龄分桶。
	// DefBufCap 是 Prometheus 默认的 Summary 缓冲区容量。
	// DefMaxAge 是 Prometheus 默认的 Summary 最大保留时长。
	DefAgeBuckets = prometheus.DefAgeBuckets
	DefBufCap     = prometheus.DefBufCap
	DefMaxAge     = prometheus.DefMaxAge
)

// Summary 是对 prometheus.Summary 的内部包装，实现 kubeCollector 与 ObserverMetric。
//
// Deprecated: 根据指标重构 KEP 弃用。
type Summary struct {
	ObserverMetric
	*SummaryOpts
	lazyMetric
	selfCollector
}

// NewSummary 返回一个类似 Summary 的对象；注册前不会记录任何观测。
//
// Deprecated: 根据指标重构 KEP 弃用。
//
// 构造阶段先把底层实现设为 noop；只有注册后 lazyMetric 才会创建真正的
// Prometheus Summary，这样未注册的指标不会占用内存。
func NewSummary(opts *SummaryOpts) *Summary {
	opts.StabilityLevel.setDefaults()

	s := &Summary{
		SummaryOpts: opts,
		lazyMetric:  lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	s.setPrometheusSummary(noopMetric{})
	s.lazyInit(s, BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return s
}

// setPrometheusSummary 设置执行实际观测的底层 prometheus.Summary。
func (s *Summary) setPrometheusSummary(summary prometheus.Summary) {
	s.ObserverMetric = summary
	s.initSelfCollection(summary)
}

// DeprecatedVersion 返回已解析的弃用版本；未设置时返回 nil。
func (s *Summary) DeprecatedVersion() *semver.Version {
	return parseSemver(s.SummaryOpts.DeprecatedVersion)
}

// initializeMetric 创建并保存底层 prometheus.Summary。
//
// 注册时调用：先给 Help 打上稳定性前缀，再创建底层对象。
func (s *Summary) initializeMetric() {
	s.SummaryOpts.annotateStabilityLevel()
	// 这里才创建底层 prometheus.Summary。
	s.setPrometheusSummary(prometheus.NewSummary(s.SummaryOpts.toPromSummaryOpts()))
}

// initializeDeprecatedMetric 先修改 Help 中的弃用说明，再创建 prometheus.Summary。
//
// 弃用指标的 Help 会追加 "(Deprecated since ...)"，便于使用者从 /metrics 直接发现。
func (s *Summary) initializeDeprecatedMetric() {
	s.SummaryOpts.markDeprecated()
	s.initializeMetric()
}

// WithContext 接受供普通 Summary 指标使用的 context；当前实现忽略该参数。
func (s *Summary) WithContext(ctx context.Context) ObserverMetric {
	return s.ObserverMetric
}

// SummaryVec 是对 prometheus.SummaryVec 的内部包装。
//
// Deprecated: 根据指标重构 KEP 弃用。
type SummaryVec struct {
	*prometheus.SummaryVec
	*SummaryOpts
	lazyMetric
	originalLabels []string
}

// NewSummaryVec 返回实现 kubeCollector 的 prometheus.SummaryVec 包装。
// 指标采用懒创建；只有注册后获取的成员才会记录观测。
//
// Deprecated: 根据指标重构 KEP 弃用。
//
// 注册前通过 WithLabelValues 拿到的只是永久 noop，不会在内存中累积子指标。
func NewSummaryVec(opts *SummaryOpts, labels []string) *SummaryVec {
	opts.StabilityLevel.setDefaults()

	fqName := BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	v := &SummaryVec{
		SummaryOpts:    opts,
		originalLabels: labels,
		lazyMetric:     lazyMetric{stabilityLevel: opts.StabilityLevel},
	}
	v.lazyInit(v, fqName)
	return v
}

// DeprecatedVersion 返回已解析的弃用版本；未设置时返回 nil。
func (v *SummaryVec) DeprecatedVersion() *semver.Version {
	return parseSemver(v.SummaryOpts.DeprecatedVersion)
}

// initializeMetric 在注册时创建真正的 SummaryVec。
func (v *SummaryVec) initializeMetric() {
	v.SummaryOpts.annotateStabilityLevel()
	v.SummaryVec = prometheus.NewSummaryVec(v.SummaryOpts.toPromSummaryOpts(), v.originalLabels)
}

// initializeDeprecatedMetric 先标记弃用再创建，使 Help 带上弃用说明。
func (v *SummaryVec) initializeDeprecatedMetric() {
	v.SummaryOpts.markDeprecated()
	v.initializeMetric()
}

// Prometheus Vec 默认会在提取成员时，为底层 metricMap 中不存在的标签组合创建元素。
// 若指标尚未注册，该元素永远不会对外暴露或被消费，却会一直占用内存。
//
// 参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/histogram.go#L460-L470
//
// 本包的 Vec 在注册前提取成员时只返回永久的 noop 对象。

// WithLabelValues 返回给定标签值序列对应的 ObserverMetric，顺序须与 Desc 中的
// VariableLabels 一致。首次访问该组合时，仅当 SummaryVec 已注册才创建成员。
//
// 未注册时返回 noop；注册后才按 allow list 约束 label 值并创建真实子指标。
func (v *SummaryVec) WithLabelValues(lvs ...string) ObserverMetric {
	if !v.IsCreated() {
		return noop
	}

	// 首次调用时在读锁下从全局快照初始化标签允许列表。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// 将标签值约束为允许值。
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainToAllowedList(v.originalLabels, lvs)
	}
	return v.SummaryVec.WithLabelValues(lvs...)
}

// With 返回给定 Labels 映射对应的 ObserverMetric，标签名须与 Desc 中的
// VariableLabels 一致。首次访问该组合时，仅当 SummaryVec 已注册才创建成员。
func (v *SummaryVec) With(labels map[string]string) ObserverMetric {
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

	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainLabelMap(labels)
	}
	return v.SummaryVec.With(labels)
}

// Delete 删除可变标签与给定 Labels 匹配的成员，删除成功时返回 true。
//
// Labels 的数量或名称与 Desc 中的 VariableLabels 不一致不会报错，
// 但这样的 Labels 无法匹配实际成员，因此始终返回 false。
func (v *SummaryVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // 指标尚未创建，因此未删除任何匹配成员
	}
	return v.SummaryVec.Delete(labels)
}

// Reset 删除该 vector 中的所有成员。
func (v *SummaryVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.SummaryVec.Reset()
}

// ResetLabelAllowLists 重置 SummaryVec 的标签允许列表。
// 注意：仅供测试使用。
func (v *SummaryVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext 返回带 context 的 SummaryVec 包装。
//
// 当前实现只保存 ctx，为将来接入 exemplar 预留接口。
func (v *SummaryVec) WithContext(ctx context.Context) *SummaryVecWithContext {
	return &SummaryVecWithContext{
		ctx:        ctx,
		SummaryVec: v,
	}
}

// SummaryVecWithContext 是带 context 的 SummaryVec 包装。
type SummaryVecWithContext struct {
	*SummaryVec
	ctx context.Context
}

// WithLabelValues 包装 SummaryVec.WithLabelValues，并保留 context。
func (vc *SummaryVecWithContext) WithLabelValues(lvs ...string) ObserverMetric {
	return vc.SummaryVec.WithLabelValues(lvs...)
}

// With 包装 SummaryVec.With，并保留 context。
func (vc *SummaryVecWithContext) With(labels map[string]string) ObserverMetric {
	return vc.SummaryVec.With(labels)
}
