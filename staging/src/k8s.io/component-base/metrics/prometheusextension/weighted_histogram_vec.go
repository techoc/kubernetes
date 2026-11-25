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
	"github.com/prometheus/client_golang/prometheus"
)

// WeightedObserverVec 是一组共享同一 Desc、按变量标签区分的 WeightedObserver。
// 与 prometheus.HistogramVec 的区别仅在于 ObserveWithWeight 接受带权重的样本。
type WeightedObserverVec interface {
	// GetMetricWith 按完整标签集合获取带权重观察器。
	GetMetricWith(prometheus.Labels) (WeightedObserver, error)
	// GetMetricWithLabelValues 按位置标签值获取带权重观察器。
	GetMetricWithLabelValues(lvs ...string) (WeightedObserver, error)
	// With 获取带权重观察器，出错时 panic。
	With(prometheus.Labels) WeightedObserver
	// WithLabelValues 按位置标签值获取带权重观察器，出错时 panic。
	WithLabelValues(...string) WeightedObserver
	// CurryWith 预绑定部分标签并返回新的向量接口。
	CurryWith(prometheus.Labels) (WeightedObserverVec, error)
	// MustCurryWith 是 CurryWith 的 panic 版本。
	MustCurryWith(prometheus.Labels) WeightedObserverVec
}

// WeightedHistogramVec 实现 WeightedObserverVec。
type WeightedHistogramVec struct {
	*prometheus.MetricVec
}

var _ WeightedObserverVec = &WeightedHistogramVec{}
var _ prometheus.Collector = &WeightedHistogramVec{}

// NewWeightedHistogramVec 构造带可变标签的加权直方图集合。
// 借助 prometheus.NewMetricVec 的标签校验与缓存，子指标在首次访问时惰性创建。
func NewWeightedHistogramVec(opts WeightedHistogramOpts, labelNames ...string) *WeightedHistogramVec {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		wrapWeightedHelp(opts.Help),
		labelNames,
		opts.ConstLabels,
	)
	return &WeightedHistogramVec{
		MetricVec: prometheus.NewMetricVec(desc, func(lvs ...string) prometheus.Metric {
			metric, err := newWeightedHistogram(desc, opts, lvs...)
			if err != nil {
				panic(err) // 与 prometheus.newHistogram 的行为一致
			}
			return metric
		}),
	}
}

// GetMetricWith 按完整标签集获取子指标；标签不匹配时返回错误。
func (hv *WeightedHistogramVec) GetMetricWith(labels prometheus.Labels) (WeightedObserver, error) {
	metric, err := hv.MetricVec.GetMetricWith(labels)
	if metric != nil {
		return metric.(WeightedObserver), err
	}
	return nil, err
}

// GetMetricWithLabelValues 按位置标签值获取子指标。
func (hv *WeightedHistogramVec) GetMetricWithLabelValues(lvs ...string) (WeightedObserver, error) {
	metric, err := hv.MetricVec.GetMetricWithLabelValues(lvs...)
	if metric != nil {
		return metric.(WeightedObserver), err
	}
	return nil, err
}

// With 是 GetMetricWith 的 panic 版本，适用于标签集合已知正确的调用点。
func (hv *WeightedHistogramVec) With(labels prometheus.Labels) WeightedObserver {
	h, err := hv.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return h
}

// WithLabelValues 是 GetMetricWithLabelValues 的 panic 版本。
func (hv *WeightedHistogramVec) WithLabelValues(lvs ...string) WeightedObserver {
	h, err := hv.GetMetricWithLabelValues(lvs...)
	if err != nil {
		panic(err)
	}
	return h
}

// CurryWith 预绑定部分标签，返回新的向量视图（共享底层子指标缓存）。
func (hv *WeightedHistogramVec) CurryWith(labels prometheus.Labels) (WeightedObserverVec, error) {
	vec, err := hv.MetricVec.CurryWith(labels)
	if vec != nil {
		return &WeightedHistogramVec{MetricVec: vec}, err
	}
	return nil, err
}

// MustCurryWith 是 CurryWith 的 panic 版本。
func (hv *WeightedHistogramVec) MustCurryWith(labels prometheus.Labels) WeightedObserverVec {
	vec, err := hv.CurryWith(labels)
	if err != nil {
		panic(err)
	}
	return vec
}
