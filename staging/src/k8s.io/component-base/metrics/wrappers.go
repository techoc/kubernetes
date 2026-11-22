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
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// This file contains a series of interfaces which we explicitly define for
// integrating with prometheus. We redefine the interfaces explicitly here
// so that we can prevent breakage if methods are ever added to prometheus
// variants of them.

// Collector defines a subset of prometheus.Collector interface methods
// 只固定这里用到的方法，避免 prometheus 上游新增方法时破坏兼容性。
type Collector interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}

// Metric defines a subset of prometheus.Metric interface methods
type Metric interface {
	Desc() *prometheus.Desc
	Write(*dto.Metric) error
}

// CounterMetric is a Metric that represents a single numerical value that only ever
// goes up. That implies that it cannot be used to count items whose number can
// also go down, e.g. the number of currently running goroutines. Those
// "counters" are represented by Gauges.

// CounterMetric is an interface which defines a subset of the interface provided by prometheus.Counter
// 只暴露单调递增操作，避免调用方误用 Set/Dec 破坏 counter 语义。
type CounterMetric interface {
	Inc()
	Add(float64)
}

// CounterVecMetric is an interface which prometheus.CounterVec satisfies.
// 允许按标签取子 counter；未注册时返回 noop 而非 panic。
type CounterVecMetric interface {
	WithLabelValues(...string) CounterMetric
	With(prometheus.Labels) CounterMetric
}

// GaugeMetric is an interface which defines a subset of the interface provided by prometheus.Gauge
// 包含 Write，便于测试直接读取 DTO 而不经过完整 Gather。
type GaugeMetric interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	Write(out *dto.Metric) error
	SetToCurrentTime()
}

// GaugeVecMetric is a collection of Gauges that differ only in label values.
// 与 prometheus 默认行为不同：注册前取子指标返回 noop，避免未注册指标永久驻留内存。
type GaugeVecMetric interface {
	// Default Prometheus Vec behavior is that member extraction results in creation of a new element
	// if one with the unique label values is not found in the underlying stored metricMap.
	// This means  that if this function is called but the underlying metric is not registered
	// (which means it will never be exposed externally nor consumed), the metric would exist in memory
	// for perpetuity (i.e. throughout application lifecycle).
	//
	// For reference: https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/gauge.go#L190-L208
	//
	// In contrast, the Vec behavior in this package is that member extraction before registration
	// returns a permanent noop object.

	// WithLabelValuesChecked, if called before this vector has been registered in
	// at least one registry, will return a noop gauge and
	// an error that passes ErrIsNotRegistered.
	// If called on a hidden vector,
	// will return a noop gauge and a nil error.
	// If called with a syntactic problem in the labels, will
	// return a noop gauge and an error about the labels.
	// If none of the above apply, this method will return
	// the appropriate vector member and a nil error.
	WithLabelValuesChecked(labelValues ...string) (GaugeMetric, error)

	// WithLabelValues calls WithLabelValuesChecked
	// and handles errors as follows.
	// An error that passes ErrIsNotRegistered is ignored
	// and the noop gauge is returned;
	// all other errors cause a panic.
	WithLabelValues(labelValues ...string) GaugeMetric

	// WithChecked, if called before this vector has been registered in
	// at least one registry, will return a noop gauge and
	// an error that passes ErrIsNotRegistered.
	// If called on a hidden vector,
	// will return a noop gauge and a nil error.
	// If called with a syntactic problem in the labels, will
	// return a noop gauge and an error about the labels.
	// If none of the above apply, this method will return
	// the appropriate vector member and a nil error.
	WithChecked(labels map[string]string) (GaugeMetric, error)

	// With calls WithChecked and handles errors as follows.
	// An error that passes ErrIsNotRegistered is ignored
	// and the noop gauge is returned;
	// all other errors cause a panic.
	With(labels map[string]string) GaugeMetric

	// Delete asserts that the vec should have no member for the given label set.
	// The returned bool indicates whether there was a change.
	// The return will certainly be `false` if the given label set has the wrong
	// set of label names.
	Delete(map[string]string) bool

	// Reset removes all the members
	Reset()
}

// ObserverMetric captures individual observations.
type ObserverMetric interface {
	Observe(float64)
}

// PromRegistry is an interface which implements a subset of prometheus.Registerer and
// prometheus.Gatherer interfaces
// 同时提供注册与抓取能力，便于用单一对象替换 prometheus 默认 registry。
type PromRegistry interface {
	Register(prometheus.Collector) error
	MustRegister(...prometheus.Collector)
	Unregister(prometheus.Collector) bool
	Gather() ([]*dto.MetricFamily, error)
}

// Gatherer is the interface for the part of a registry in charge of gathering
// the collected metrics into a number of MetricFamilies.
type Gatherer interface {
	prometheus.Gatherer
}

// Registerer is the interface for the part of a registry in charge of registering
// the collected metrics.
type Registerer interface {
	prometheus.Registerer
}

// GaugeFunc is a Gauge whose value is determined at collect time by calling a
// provided function.
//
// To create GaugeFunc instances, use NewGaugeFunc.
// 同时实现 Metric 与 Collector，使它可以像普通 gauge 一样被读取或被注册。
type GaugeFunc interface {
	Metric
	Collector
}

// ErrIsNotRegistered 判断错误是否表示“向量尚未注册”。
// 调用方据此区分可忽略的未注册状态与真正的标签错误。
func ErrIsNotRegistered(err error) bool {
	return err == errNotRegistered
}

// errNotRegistered 作为哨兵值比较，调用方不应依赖其字符串内容。
var errNotRegistered = errors.New("metric vec is not registered yet")
