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
	"time"

	"k8s.io/component-base/metrics"
	fwk "k8s.io/kube-scheduler/framework"
)

// Entity 表示被记录进指标的实体，可以是单个 Pod 或 PodGroup。
type Entity interface {
	// Size 返回实体包含的 Pod 数量（单个 Pod 为 1，PodGroup 为其包含的 Pod 数）。
	Size() int
	// Type 返回实体类型（如 "pod" 或 "podgroup"）。
	Type() fwk.EntityKeyType
}

// MetricRecorder 通过 Add、Remove、Update、Clear 维护队列中的实体指标。
type MetricRecorder interface {
	// Add 记录新增一个实体。
	Add(entity Entity)
	// Remove 记录移除一个实体。
	Remove(entity Entity)
	// Update 记录实体的更新（Pod 数变化）。
	Update(oldEntity, newEntity Entity)
	// Clear 将已记录的指标重置为零。
	Clear()
}

var _ MetricRecorder = &QueuedEntitiesRecorder{}

// QueuedEntitiesRecorder 是 MetricRecorder 的实现，
// 分别维护 Pod 数量 Gauge 与按实体类型划分的 Gauge。
type QueuedEntitiesRecorder struct {
	pods     metrics.GaugeMetric
	entities func(entityType string) metrics.GaugeMetric
}

// NewActiveEntitiesRecorder 创建记录活跃 Pod 与活跃实体数量的指标记录器。
func NewActiveEntitiesRecorder() *QueuedEntitiesRecorder {
	return &QueuedEntitiesRecorder{
		pods:     ActivePods(),
		entities: ActiveEntities,
	}
}

// NewUnschedulableEntitiesRecorder 创建记录不可调度 Pod 与实体数量的指标记录器。
func NewUnschedulableEntitiesRecorder() *QueuedEntitiesRecorder {
	return &QueuedEntitiesRecorder{
		pods:     UnschedulablePods(),
		entities: UnschedulableEntities,
	}
}

// NewBackoffEntitiesRecorder 创建记录退避中 Pod 与实体数量的指标记录器。
func NewBackoffEntitiesRecorder() *QueuedEntitiesRecorder {
	return &QueuedEntitiesRecorder{
		pods:     BackoffPods(),
		entities: BackoffEntities,
	}
}

// NewGatedEntitiesRecorder 创建记录被 gating 的 Pod 与实体数量的指标记录器。
func NewGatedEntitiesRecorder() *QueuedEntitiesRecorder {
	return &QueuedEntitiesRecorder{
		pods:     GatedPods(),
		entities: GatedEntities,
	}
}

// EntityToLabel 将实体类型转换为指标使用的标签字符串；
// 未知类型返回 false，调用方据此跳过计数。
func EntityToLabel(entity Entity) (string, bool) {
	if entity == nil {
		return "", false
	}

	switch entity.Type() {
	case fwk.PodKeyType:
		return Pod, true
	case fwk.PodGroupKeyType:
		return PodGroup, true
	}

	return "", false
}

// Add 记录新增实体：按 Pod 数量累加 Pod 计数，并按实体类型递增实体计数。
func (r *QueuedEntitiesRecorder) Add(entity Entity) {
	r.pods.Add(float64(entity.Size()))
	if label, ok := EntityToLabel(entity); ok {
		r.entities(label).Inc()
	}
}

// Remove 记录移除实体：按 Pod 数量扣减 Pod 计数，并按实体类型递减实体计数。
func (r *QueuedEntitiesRecorder) Remove(entity Entity) {
	r.pods.Add(-float64(entity.Size()))
	if label, ok := EntityToLabel(entity); ok {
		r.entities(label).Dec()
	}
}

// Update 记录实体更新，只调整 Pod 数量指标。
// 实体类型不会改变，且一个实体始终对应一个对象，因此无需改动实体计数。
func (r *QueuedEntitiesRecorder) Update(oldEntity, newEntity Entity) {
	diff := newEntity.Size() - oldEntity.Size()
	r.pods.Add(float64(diff))
}

// Clear 将 Pod 数量与各类实体计数全部重置为 0。
func (r *QueuedEntitiesRecorder) Clear() {
	r.pods.Set(float64(0))
	r.entities(Pod).Set(float64(0))
	r.entities(PodGroup).Set(float64(0))
}

// histogramVecMetric 是主框架线程与指标记录 goroutine 之间通过缓冲通道传递的直方图数据。
type histogramVecMetric struct {
	metric      *metrics.HistogramVec
	labelValues []string
	value       float64
}

type gaugeVecMetric struct {
	metric      *metrics.GaugeVec
	labelValues []string
	valueToAdd  float64
}

type gaugeVecMetricKey struct {
	metricName string
	labelValue string
}

type counterVecMetric struct {
	metric      *metrics.CounterVec
	labelValues []string
	value       float64
}

// MetricAsyncRecorder 在独立 goroutine 中上报指标，避免指标写入阻塞调度关键路径。
type MetricAsyncRecorder struct {
	// bufferCh 是指标上报前的缓冲通道；缓冲区满时新指标会被丢弃。
	bufferCh chan *histogramVecMetric
	// 缓冲区达到 bufferSize 后，新来的指标会被丢弃，而非阻塞调用方。
	bufferSize int
	// 指标刷新的时间间隔。
	interval time.Duration

	// aggregatedInflightEventMetric 仅用于异步记录 InFlightEvents：
	// 按 gaugeVecMetricKey 聚合变更量，每隔 interval 刷新到 Prometheus。
	// 故意不加锁：假设调用方（队列）在更新在途事件前已持有锁。
	aggregatedInflightEventMetric              map[gaugeVecMetricKey]int
	aggregatedInflightEventMetricLastFlushTime time.Time
	aggregatedInflightEventMetricBufferCh      chan *gaugeVecMetric

	// counterBufferCh 是异步计数器增量的缓冲通道。
	counterBufferCh chan *counterVecMetric

	// stopCh 用于停止定期刷新指标的 goroutine。
	stopCh <-chan struct{}
	// IsStoppedCh 在刷新 goroutine 退出时关闭；仅用于测试中确认指标已停止写入，便于采集校验。
	IsStoppedCh chan struct{}
}

// NewMetricsAsyncRecorder 创建异步指标记录器并启动其刷新 goroutine。
// bufferSize 为各通道的缓冲大小，interval 为刷新周期，stopCh 关闭时停止刷新。
func NewMetricsAsyncRecorder(bufferSize int, interval time.Duration, stopCh <-chan struct{}) *MetricAsyncRecorder {
	recorder := &MetricAsyncRecorder{
		bufferCh:                      make(chan *histogramVecMetric, bufferSize),
		counterBufferCh:               make(chan *counterVecMetric, bufferSize),
		bufferSize:                    bufferSize,
		interval:                      interval,
		stopCh:                        stopCh,
		aggregatedInflightEventMetric: make(map[gaugeVecMetricKey]int),
		aggregatedInflightEventMetricLastFlushTime: time.Now(),
		aggregatedInflightEventMetricBufferCh:      make(chan *gaugeVecMetric, bufferSize),
		IsStoppedCh:                                make(chan struct{}),
	}
	go recorder.run()
	return recorder
}

// ObserveFrameworkExtensionPointDurationAsync 记录 framework_extension_point_duration_seconds，
// 实际写入 Prometheus 由后台 goroutine 异步完成。
func (r *MetricAsyncRecorder) ObserveFrameworkExtensionPointDurationAsync(extensionPoint, status, profileName string, value float64) {
	r.observeMetricAsync(FrameworkExtensionPointDuration, value, extensionPoint, status, profileName)
}

// ObservePluginDurationAsync 记录 plugin_execution_duration_seconds，
// 实际写入 Prometheus 由后台 goroutine 异步完成。
func (r *MetricAsyncRecorder) ObservePluginDurationAsync(extensionPoint, pluginName, status string, value float64) {
	r.observeMetricAsync(PluginExecutionDuration, value, pluginName, extensionPoint, status)
}

// ObserveQueueingHintDurationAsync 记录 queueing_hint_execution_duration_seconds，
// 实际写入 Prometheus 由后台 goroutine 异步完成。
func (r *MetricAsyncRecorder) ObserveQueueingHintDurationAsync(pluginName, event, hint string, value float64) {
	r.observeMetricAsync(queueingHintExecutionDuration, value, pluginName, event, hint)
}

// ObserveCounterAsync 异步累加计数器指标。
// 缓冲区满时直接丢弃，不影响主流程。
func (r *MetricAsyncRecorder) ObserveCounterAsync(m *metrics.CounterVec, value float64, labelValues ...string) {
	newMetric := &counterVecMetric{
		metric:      m,
		labelValues: labelValues,
		value:       value,
	}
	select {
	case r.counterBufferCh <- newMetric:
	default:
	}
}

// ObserveInFlightEventsAsync 记录 in_flight_events 指标的增量。
//
// 该函数不是并发安全的：出于性能考虑故意不给 map 加锁，
// 依赖调用方（队列）在更新在途事件前持有锁。
func (r *MetricAsyncRecorder) ObserveInFlightEventsAsync(eventLabel string, valueToAdd float64, forceFlush bool) {
	r.aggregatedInflightEventMetric[gaugeVecMetricKey{metricName: InFlightEvents.Name, labelValue: eventLabel}] += int(valueToAdd)

	// 仅在达到刷新周期时才把聚合值送入通道，由 run() 写入 Prometheus。
	// 刷新逻辑放在这里而非 FlushMetrics，是为了避免给该 map 加锁。
	if forceFlush || time.Since(r.aggregatedInflightEventMetricLastFlushTime) > r.interval {
		for key, value := range r.aggregatedInflightEventMetric {
			newMetric := &gaugeVecMetric{
				metric:      InFlightEvents,
				labelValues: []string{key.labelValue},
				valueToAdd:  float64(value),
			}
			select {
			case r.aggregatedInflightEventMetricBufferCh <- newMetric:
			default:
			}
		}
		r.aggregatedInflightEventMetricLastFlushTime = time.Now()
		// 重置聚合值，进入下一个统计周期。
		r.aggregatedInflightEventMetric = make(map[gaugeVecMetricKey]int)
	}
}

// observeMetricAsync 将直方图观测值异步写入缓冲通道，缓冲区满时丢弃。
func (r *MetricAsyncRecorder) observeMetricAsync(m *metrics.HistogramVec, value float64, labelsValues ...string) {
	newMetric := &histogramVecMetric{
		metric:      m,
		labelValues: labelsValues,
		value:       value,
	}
	select {
	case r.bufferCh <- newMetric:
	default:
	}
}

// run 每隔 interval 将缓冲的指标刷新到 Prometheus，
// 收到 stopCh 信号时关闭 IsStoppedCh 并退出。
func (r *MetricAsyncRecorder) run() {
	for {
		r.FlushMetrics()
		select {
		case <-r.stopCh:
			close(r.IsStoppedCh)
			return
		case <-time.After(r.interval):
		}
	}
}

// FlushMetrics 最多读取 bufferSize 条指标以清空缓冲通道。
func (r *MetricAsyncRecorder) FlushMetrics() {
	for i := 0; i < r.bufferSize; i++ {
		select {
		case m := <-r.bufferCh:
			m.metric.WithLabelValues(m.labelValues...).Observe(m.value)
		case m := <-r.counterBufferCh:
			m.metric.WithLabelValues(m.labelValues...).Add(m.value)
		default:
			// 缓冲区已空。
		}

		select {
		case m := <-r.aggregatedInflightEventMetricBufferCh:
			m.metric.WithLabelValues(m.labelValues...).Add(m.valueToAdd)
		default:
			// 缓冲区已空。
		}
	}
}
