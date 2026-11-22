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
	// 以下常量直接转发 Prometheus 的默认值，供调用方在构造 SummaryOpts 时引用。
	DefAgeBuckets = prometheus.DefAgeBuckets
	DefBufCap     = prometheus.DefBufCap
	DefMaxAge     = prometheus.DefMaxAge
)

// Summary is our internal representation for our wrapping struct around prometheus
// summaries. Summary implements both kubeCollector and ObserverMetric
//
// Deprecated: as per the metrics overhaul KEP
type Summary struct {
	ObserverMetric
	*SummaryOpts
	lazyMetric
	selfCollector
}

// NewSummary returns an object which is Summary-like. However, nothing
// will be measured until the summary is registered somewhere.
//
// Deprecated: as per the metrics overhaul KEP
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

// setPrometheusSummary sets the underlying KubeGauge object, i.e. the thing that does the measurement.
func (s *Summary) setPrometheusSummary(summary prometheus.Summary) {
	s.ObserverMetric = summary
	s.initSelfCollection(summary)
}

// DeprecatedVersion returns a pointer to the Version or nil
func (s *Summary) DeprecatedVersion() *semver.Version {
	return parseSemver(s.SummaryOpts.DeprecatedVersion)
}

// initializeMetric invokes the actual prometheus.Summary object instantiation
// and stores a reference to it
//
// 注册时调用：先给 Help 打上稳定性前缀，再创建底层对象。
func (s *Summary) initializeMetric() {
	s.SummaryOpts.annotateStabilityLevel()
	// this actually creates the underlying prometheus gauge.
	s.setPrometheusSummary(prometheus.NewSummary(s.SummaryOpts.toPromSummaryOpts()))
}

// initializeDeprecatedMetric invokes the actual prometheus.Summary object instantiation
// but modifies the Help description prior to object instantiation.
//
// 弃用指标的 Help 会追加 "(Deprecated since ...)"，便于使用者从 /metrics 直接发现。
func (s *Summary) initializeDeprecatedMetric() {
	s.SummaryOpts.markDeprecated()
	s.initializeMetric()
}

// WithContext allows the normal Summary metric to pass in context. The context is no-op now.
func (s *Summary) WithContext(ctx context.Context) ObserverMetric {
	return s.ObserverMetric
}

// SummaryVec is the internal representation of our wrapping struct around prometheus
// summaryVecs.
//
// Deprecated: as per the metrics overhaul KEP
type SummaryVec struct {
	*prometheus.SummaryVec
	*SummaryOpts
	lazyMetric
	originalLabels []string
}

// NewSummaryVec returns an object which satisfies kubeCollector and wraps the
// prometheus.SummaryVec object. However, the object returned will not measure
// anything unless the collector is first registered, since the metric is lazily instantiated,
// and only members extracted after
// registration will actually measure anything.
//
// Deprecated: as per the metrics overhaul KEP
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

// DeprecatedVersion returns a pointer to the Version or nil
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
// label values is accessed for the first time, a new ObserverMetric is created IFF the summaryVec
// has been registered to a metrics registry.
//
// 未注册时返回 noop；注册后才按 allow list 约束 label 值并创建真实子指标。
func (v *SummaryVec) WithLabelValues(lvs ...string) ObserverMetric {
	if !v.IsCreated() {
		return noop
	}

	// Initialize label allow lists if not already initialized
	// 只读取全局 allow list 快照，加读锁即可。
	v.initializeLabelAllowListsOnce.Do(func() {
		allowListLock.RLock()
		if allowList, ok := labelValueAllowLists[v.FQName()]; ok {
			v.LabelValueAllowLists = allowList
		}
		allowListLock.RUnlock()
	})

	// Constrain label values to allowed values
	if v.LabelValueAllowLists != nil {
		v.LabelValueAllowLists.ConstrainToAllowedList(v.originalLabels, lvs)
	}
	return v.SummaryVec.WithLabelValues(lvs...)
}

// With returns the ObserverMetric for the given Labels map (the label names
// must match those of the VariableLabels in Desc). If that label map is
// accessed for the first time, a new ObserverMetric is created IFF the summaryVec has
// been registered to a metrics registry.
//
// With 与 WithLabelValues 语义相同，只是以 map 形式传参。
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

// Delete deletes the metric where the variable labels are the same as those
// passed in as labels. It returns true if a metric was deleted.
//
// It is not an error if the number and names of the Labels are inconsistent
// with those of the VariableLabels in Desc. However, such inconsistent Labels
// can never match an actual metric, so the method will always return false in
// that case.
func (v *SummaryVec) Delete(labels map[string]string) bool {
	if !v.IsCreated() {
		return false // since we haven't created the metric, we haven't deleted a metric with the passed in values
	}
	return v.SummaryVec.Delete(labels)
}

// Reset deletes all metrics in this vector.
func (v *SummaryVec) Reset() {
	if !v.IsCreated() {
		return
	}

	v.SummaryVec.Reset()
}

// ResetLabelAllowLists resets the label allow list for the SummaryVec.
// NOTE: This should only be used in test.
func (v *SummaryVec) ResetLabelAllowLists() {
	v.initializeLabelAllowListsOnce = sync.Once{}
	v.LabelValueAllowLists = nil
}

// WithContext returns wrapped SummaryVec with context
//
// 当前实现只保存 ctx，为将来接入 exemplar 预留接口。
func (v *SummaryVec) WithContext(ctx context.Context) *SummaryVecWithContext {
	return &SummaryVecWithContext{
		ctx:        ctx,
		SummaryVec: v,
	}
}

// SummaryVecWithContext is the wrapper of SummaryVec with context.
type SummaryVecWithContext struct {
	*SummaryVec
	ctx context.Context
}

// WithLabelValues is the wrapper of SummaryVec.WithLabelValues.
func (vc *SummaryVecWithContext) WithLabelValues(lvs ...string) ObserverMetric {
	return vc.SummaryVec.WithLabelValues(lvs...)
}

// With is the wrapper of SummaryVec.With.
func (vc *SummaryVecWithContext) With(labels map[string]string) ObserverMetric {
	return vc.SummaryVec.With(labels)
}
