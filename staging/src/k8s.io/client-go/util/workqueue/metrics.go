/*
Copyright 2016 The Kubernetes Authors.

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

package workqueue

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// 本文件提供设置指标后端（如 Prometheus）的抽象层。
//
// 本文件定义 workqueue 的指标抽象层：队列实现只依赖这里的最小接口，
// 具体的指标后端（如 Prometheus）由 MetricsProvider 注入，从而避免核心逻辑依赖具体监控库。

// queueMetrics 是队列内部使用的指标钩子；nil 实现被视为“未启用指标”。
type queueMetrics[T comparable] interface {
	add(item T)
	get(item T)
	done(item T)
	updateUnfinishedWork()
}

// GaugeMetric 表示可任意增减的单个数值。
type GaugeMetric interface {
	Inc()
	Dec()
}

// SettableGaugeMetric 表示可任意增减的单个数值，与 GaugeMetric 分开是为了向后兼容。
//
// 之所以与 GaugeMetric 分开，是因为 Set 语义无法从旧接口平滑添加（向后兼容）。
type SettableGaugeMetric interface {
	Set(float64)
}

// CounterMetric 表示只增不减的单个数值。
type CounterMetric interface {
	Inc()
}

// SummaryMetric 记录单次观测值。
type SummaryMetric interface {
	Observe(float64)
}

// HistogramMetric 按区间统计观测值。
type HistogramMetric interface {
	Observe(float64)
}

// noopMetric 实现全部指标接口，用于“指标关闭”场景，避免调用处判空。
type noopMetric struct{}

func (noopMetric) Inc()            {}
func (noopMetric) Dec()            {}
func (noopMetric) Set(float64)     {}
func (noopMetric) Observe(float64) {}

// defaultQueueMetrics 要求调用方在更新指标前先加锁。
//
// defaultQueueMetrics 记录单个队列的即时指标。调用方（队列）必须在持有自身锁时调用，
// 因此这里内部不做同步；addTimes / processingStartTimes 只在锁内读写。
type defaultQueueMetrics[T comparable] struct {
	clock clock.Clock

	// 队列当前深度。
	depth GaugeMetric
	// 队列累计入队次数。
	adds CounterMetric
	// 条目在队列中的停留时长。
	latency HistogramMetric
	// 处理队列中一个条目的耗时。
	workDuration HistogramMetric
	// addTimes 记录条目首次入队时间，用于计算排队时延；重复入队不会刷新。
	addTimes map[T]time.Time
	// processingStartTimes 记录条目开始处理的时间，用于计算处理时长与未完成工作。
	processingStartTimes map[T]time.Time

	// 当前 worker 已工作多久。
	unfinishedWorkSeconds   SettableGaugeMetric
	longestRunningProcessor SettableGaugeMetric
}

// add 记录一次入队：增加计数与深度，并在首次入队时启动时延计时。
func (m *defaultQueueMetrics[T]) add(item T) {
	if m == nil {
		return
	}

	m.adds.Inc()
	m.depth.Inc()
	if _, exists := m.addTimes[item]; !exists {
		m.addTimes[item] = m.clock.Now()
	}
}

// get 记录一次出队：深度减一，记录处理开始时间；
// 若存在首次入队时间则观测排队时延并清除，避免重复观测。
func (m *defaultQueueMetrics[T]) get(item T) {
	if m == nil {
		return
	}

	m.depth.Dec()
	m.processingStartTimes[item] = m.clock.Now()
	if startTime, exists := m.addTimes[item]; exists {
		m.latency.Observe(m.sinceInSeconds(startTime))
		delete(m.addTimes, item)
	}
}

// done 记录一次处理完成：观测处理时长并清除开始时间。
// 条目若被重新入队，会在 add 中重新计时。
func (m *defaultQueueMetrics[T]) done(item T) {
	if m == nil {
		return
	}

	if startTime, exists := m.processingStartTimes[item]; exists {
		m.workDuration.Observe(m.sinceInSeconds(startTime))
		delete(m.processingStartTimes, item)
	}
}

// updateUnfinishedWork 汇总当前所有在处理条目的等待时间：
// 总计值与最老条目的耗时。用两个 gauge 而非 summary，
// 是因为 Prometheus summary 难以在进程重启以外重置。
func (m *defaultQueueMetrics[T]) updateUnfinishedWork() {
	// 这里用 summary 更合适，但 Prometheus 似乎没有体面的方式来重置 summary 指标。
	var total float64
	var oldest float64
	for _, t := range m.processingStartTimes {
		age := m.sinceInSeconds(t)
		total += age
		if age > oldest {
			oldest = age
		}
	}
	m.unfinishedWorkSeconds.Set(total)
	m.longestRunningProcessor.Set(oldest)
}

// noMetrics 是 queueMetrics 的空实现，用于未启用指标的队列。
type noMetrics[T any] struct{}

func (noMetrics[T]) add(item T)            {}
func (noMetrics[T]) get(item T)            {}
func (noMetrics[T]) done(item T)           {}
func (noMetrics[T]) updateUnfinishedWork() {}

// 返回自给定起始时刻以来的秒数。
func (m *defaultQueueMetrics[T]) sinceInSeconds(start time.Time) float64 {
	return m.clock.Since(start).Seconds()
}

// retryMetrics 记录队列重试次数。
type retryMetrics interface {
	retry()
}

type defaultRetryMetrics struct {
	retries CounterMetric
}

func (m *defaultRetryMetrics) retry() {
	if m == nil {
		return
	}

	m.retries.Inc()
}

// MetricsProvider 提供队列使用的各类指标。
//
// MetricsProvider 由调用方实现（例如 controller-runtime 或 Prometheus 适配器），
// 每个方法以队列名作为标签区分不同队列。
type MetricsProvider interface {
	NewDepthMetric(name string) GaugeMetric
	NewAddsMetric(name string) CounterMetric
	NewLatencyMetric(name string) HistogramMetric
	NewWorkDurationMetric(name string) HistogramMetric
	NewUnfinishedWorkSecondsMetric(name string) SettableGaugeMetric
	NewLongestRunningProcessorSecondsMetric(name string) SettableGaugeMetric
	NewRetriesMetric(name string) CounterMetric
}

// noopMetricsProvider 返回全部为空的指标实现，是默认的全局 provider。
type noopMetricsProvider struct{}

func (_ noopMetricsProvider) NewDepthMetric(name string) GaugeMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewAddsMetric(name string) CounterMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewLatencyMetric(name string) HistogramMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewWorkDurationMetric(name string) HistogramMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) SettableGaugeMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) SettableGaugeMetric {
	return noopMetric{}
}

func (_ noopMetricsProvider) NewRetriesMetric(name string) CounterMetric {
	return noopMetric{}
}

// globalMetricsProvider 在 SetProvider 之前默认为 noop，保证未显式配置时不产生开销。
var globalMetricsProvider MetricsProvider = noopMetricsProvider{}

// setGlobalMetricsProviderOnce 保证全局 provider 只被设置一次，避免并发竞争。
var setGlobalMetricsProviderOnce sync.Once

// newQueueMetrics 为指定队列构造指标收集器；名称为空或 provider 为 noop 时返回空实现，
// 从而在未启用指标时完全跳过统计开销。
func newQueueMetrics[T comparable](mp MetricsProvider, name string, clock clock.Clock) queueMetrics[T] {
	if len(name) == 0 || mp == (noopMetricsProvider{}) {
		return noMetrics[T]{}
	}
	return &defaultQueueMetrics[T]{
		clock:                   clock,
		depth:                   mp.NewDepthMetric(name),
		adds:                    mp.NewAddsMetric(name),
		latency:                 mp.NewLatencyMetric(name),
		workDuration:            mp.NewWorkDurationMetric(name),
		unfinishedWorkSeconds:   mp.NewUnfinishedWorkSecondsMetric(name),
		longestRunningProcessor: mp.NewLongestRunningProcessorSecondsMetric(name),
		addTimes:                map[T]time.Time{},
		processingStartTimes:    map[T]time.Time{},
	}
}

// newRetryMetrics 构造重试指标；未提供 provider 时回退到全局 provider，
// 名称为空则返回 nil（调用处视为未启用，方法为 nil 安全的 no-op）。
func newRetryMetrics(name string, provider MetricsProvider) retryMetrics {
	var ret *defaultRetryMetrics
	if len(name) == 0 {
		return ret
	}

	if provider == nil {
		provider = globalMetricsProvider
	}

	return &defaultRetryMetrics{
		retries: provider.NewRetriesMetric(name),
	}
}

// SetProvider 为之后创建的所有工作队列设置指标后端，仅首次调用生效。
func SetProvider(metricsProvider MetricsProvider) {
	setGlobalMetricsProviderOnce.Do(func() {
		globalMetricsProvider = metricsProvider
	})
}
