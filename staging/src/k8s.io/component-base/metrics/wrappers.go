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

// 本文件显式定义与 prometheus 集成所需的接口子集，
// 避免上游接口新增方法时破坏兼容性。

// Collector 定义 prometheus.Collector 接口中所需的方法子集。
type Collector interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}

// Metric 定义 prometheus.Metric 接口中所需的方法子集。
type Metric interface {
	Desc() *prometheus.Desc
	Write(*dto.Metric) error
}

// CounterMetric 定义 prometheus.Counter 的接口子集，表示只增不减的单个数值。
// 它不能统计当前运行 goroutine 数等可减小的量，这类量应使用 Gauge。
// 接口只暴露递增操作，避免调用方误用 Set/Dec 破坏 counter 语义。
type CounterMetric interface {
	Inc()
	Add(float64)
}

// CounterVecMetric 是 prometheus.CounterVec 实现的接口。
// 允许按标签取子 counter；未注册时返回 noop 而非 panic。
type CounterVecMetric interface {
	WithLabelValues(...string) CounterMetric
	With(prometheus.Labels) CounterMetric
}

// GaugeMetric 定义 prometheus.Gauge 的接口子集。
// 包含 Write，便于测试直接读取 DTO 而不经过完整 Gather。
type GaugeMetric interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	Write(out *dto.Metric) error
	SetToCurrentTime()
}

// GaugeVecMetric 是仅标签值不同的一组 Gauge。
type GaugeVecMetric interface {
	// prometheus 默认在取不到匹配标签值时创建新成员；若指标从未注册，
	// 该成员会永久驻留内存。本包在注册前返回固定 noop 对象，避免此问题。
	// 参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/gauge.go#L190-L208

	// WithLabelValuesChecked 按位置标签返回子 gauge：
	// 未注册时返回 noop 和可通过 ErrIsNotRegistered 识别的错误；
	// 隐藏向量返回 noop 和 nil；标签语法错误返回 noop 和相应错误；
	// 其他情况返回对应子指标和 nil。
	WithLabelValuesChecked(labelValues ...string) (GaugeMetric, error)

	// WithLabelValues 调用 WithLabelValuesChecked：
	// 忽略可通过 ErrIsNotRegistered 识别的错误并返回 noop，其他错误触发 panic。
	WithLabelValues(labelValues ...string) GaugeMetric

	// WithChecked 按 map 标签返回子 gauge：
	// 未注册时返回 noop 和可通过 ErrIsNotRegistered 识别的错误；
	// 隐藏向量返回 noop 和 nil；标签语法错误返回 noop 和相应错误；
	// 其他情况返回对应子指标和 nil。
	WithChecked(labels map[string]string) (GaugeMetric, error)

	// With 调用 WithChecked：
	// 忽略可通过 ErrIsNotRegistered 识别的错误并返回 noop，其他错误触发 panic。
	With(labels map[string]string) GaugeMetric

	// Delete 要求向量中不存在给定标签集的成员，并返回是否有变化。
	// 标签名称集不正确时必定返回 `false`。
	Delete(map[string]string) bool

	// Reset 移除所有成员。
	Reset()
}

// ObserverMetric 记录单次观测值。
type ObserverMetric interface {
	Observe(float64)
}

// PromRegistry 实现 prometheus.Registerer 和 prometheus.Gatherer 的接口子集，
// 同时提供注册与抓取能力。
type PromRegistry interface {
	Register(prometheus.Collector) error
	MustRegister(...prometheus.Collector)
	Unregister(prometheus.Collector) bool
	Gather() ([]*dto.MetricFamily, error)
}

// Gatherer 是 registry 中负责将已采集指标汇总为多个 MetricFamily 的接口。
type Gatherer interface {
	prometheus.Gatherer
}

// Registerer 是 registry 中负责注册已采集指标的接口。
type Registerer interface {
	prometheus.Registerer
}

// GaugeFunc 是采集时调用给定函数来确定值的 Gauge。
// 使用 NewGaugeFunc 创建实例；它同时实现 Metric 与 Collector，可像普通 gauge 一样读取或注册。
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
