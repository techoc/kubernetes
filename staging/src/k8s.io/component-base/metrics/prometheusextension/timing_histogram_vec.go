/*
Copyright 2022 The Kubernetes Authors.

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

package prometheusextension

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// GaugeVecOps 是一组共享同一 Desc、按变量标签区分的 Gauge 的操作接口。
//
// 该接口与 prometheus.GaugeVec 的方法集对齐，使 TimingHistogramVec 能像普通 GaugeVec
// 一样被使用（每次采样返回快照型 Gauge）。
type GaugeVecOps interface {
	// GetMetricWith 按完整标签集合获取指标。
	GetMetricWith(prometheus.Labels) (GaugeOps, error)
	// GetMetricWithLabelValues 按位置标签值获取指标。
	GetMetricWithLabelValues(lvs ...string) (GaugeOps, error)
	// With 获取指标，出错时 panic。
	With(prometheus.Labels) GaugeOps
	// WithLabelValues 按位置标签值获取指标，出错时 panic。
	WithLabelValues(...string) GaugeOps
	// CurryWith 预绑定部分标签并返回新的向量接口。
	CurryWith(prometheus.Labels) (GaugeVecOps, error)
	// MustCurryWith 是 CurryWith 的 panic 版本。
	MustCurryWith(prometheus.Labels) GaugeVecOps
}

// TimingHistogramVec 是一组共享同一 Desc、按变量标签区分的 TimingHistogram。
// 嵌入式 MetricVec 负责标签组合到指标的映射与并发安全。
type TimingHistogramVec struct {
	*prometheus.MetricVec
}

var _ GaugeVecOps = &TimingHistogramVec{}
var _ prometheus.Collector = &TimingHistogramVec{}

// NewTimingHistogramVec 使用系统时钟创建向量。
func NewTimingHistogramVec(opts TimingHistogramOpts, labelNames ...string) *TimingHistogramVec {
	return NewTestableTimingHistogramVec(time.Now, opts, labelNames...)
}

// NewTestableTimingHistogramVec 允许注入时钟，便于测试过期窗口与时间序列行为。
func NewTestableTimingHistogramVec(nowFunc func() time.Time, opts TimingHistogramOpts, labelNames ...string) *TimingHistogramVec {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		wrapTimingHelp(opts.Help),
		labelNames,
		opts.ConstLabels,
	)
	return &TimingHistogramVec{
		MetricVec: prometheus.NewMetricVec(desc, func(lvs ...string) prometheus.Metric {
			metric, err := newTimingHistogram(nowFunc, desc, opts, lvs...)
			if err != nil {
				// 与 prometheus.newHistogram 行为一致：标签数不匹配属于编程错误，直接 panic。
				panic(err) // 与 prometheus.newHistogram 的行为一致
			}
			return metric
		}),
	}
}

// GetMetricWith 按完整标签集合获取指标；不存在的标签组合会按需创建。
func (hv *TimingHistogramVec) GetMetricWith(labels prometheus.Labels) (GaugeOps, error) {
	metric, err := hv.MetricVec.GetMetricWith(labels)
	if metric != nil {
		return metric.(GaugeOps), err
	}
	return nil, err
}

// GetMetricWithLabelValues 是 GetMetricWith 的位置参数版本。
func (hv *TimingHistogramVec) GetMetricWithLabelValues(lvs ...string) (GaugeOps, error) {
	metric, err := hv.MetricVec.GetMetricWithLabelValues(lvs...)
	if metric != nil {
		return metric.(GaugeOps), err
	}
	return nil, err
}

// With 获取指标并在出错时 panic，适合标签静态已知的热路径。
func (hv *TimingHistogramVec) With(labels prometheus.Labels) GaugeOps {
	h, err := hv.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return h
}

// WithLabelValues 是 With 的位置参数版本。
func (hv *TimingHistogramVec) WithLabelValues(lvs ...string) GaugeOps {
	h, err := hv.GetMetricWithLabelValues(lvs...)
	if err != nil {
		panic(err)
	}
	return h
}

// CurryWith 预绑定部分标签，返回新的向量视图而不修改原向量。
func (hv *TimingHistogramVec) CurryWith(labels prometheus.Labels) (GaugeVecOps, error) {
	vec, err := hv.MetricVec.CurryWith(labels)
	if vec != nil {
		return &TimingHistogramVec{MetricVec: vec}, err
	}
	return nil, err
}

// MustCurryWith 是 CurryWith 的 panic 版本。
func (hv *TimingHistogramVec) MustCurryWith(labels prometheus.Labels) GaugeVecOps {
	vec, err := hv.CurryWith(labels)
	if err != nil {
		panic(err)
	}
	return vec
}
