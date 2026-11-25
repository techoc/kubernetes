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
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// GaugeOps 是 prometheus.Gauge 中与埋点代码相关的方法子集。
// Prometheus 已从 histogram 和 summary 中拆分出 Observer 接口，这里也采用类似的拆分方式。
type GaugeOps interface {
	// Set 与 Gauge.Set 相同。
	Set(float64)
	// Inc 与 Gauge.Inc 相同。
	Inc()
	// Dec 与 Gauge.Dec 相同。
	Dec()
	// Add 与 Gauge.Add 相同。
	Add(float64)
	// Sub 与 Gauge.Sub 相同。
	Sub(float64)

	// SetToCurrentTime 与 Gauge.SetToCurrentTime 相同。
	SetToCurrentTime()
}

// TimingHistogram 跟踪一个 float64 变量在各桶定义的值区间中停留的时长，时间以纳秒计。
// 直方图的总和是从创建时刻起，变量值随时间（纳秒）积分的结果。
// 典型用途是按 bucket 统计“处于某值区间的持续时间”，而不是单次观测次数。
type TimingHistogram interface {
	prometheus.Metric
	prometheus.Collector
	GaugeOps
}

// TimingHistogramOpts 是 TimingHistogram 构造函数的参数。
type TimingHistogramOpts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Help        string
	ConstLabels prometheus.Labels

	// Buckets 定义观测值累计到哪些桶。切片中的每个元素都是一个桶的上界（含该边界），
	// 取值必须严格递增。无需添加上界为 +Inf 的桶，默认值为 prometheus.DefBuckets。
	Buckets []float64

	// InitialValue 是变量的初始值。
	// 指标从该值开始，自创建起按它累计“时间×值”的积分。
	InitialValue float64
}

// NewTimingHistogram 创建一个新的 TimingHistogram。
func NewTimingHistogram(opts TimingHistogramOpts) (TimingHistogram, error) {
	// 生产实现直接使用系统时钟；测试可通过 NewTestableTimingHistogram 注入假时钟。
	return NewTestableTimingHistogram(time.Now, opts)
}

// NewTestableTimingHistogram 创建一个使用可模拟时钟的 TimingHistogram。
func NewTestableTimingHistogram(nowFunc func() time.Time, opts TimingHistogramOpts) (TimingHistogram, error) {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		wrapTimingHelp(opts.Help),
		nil,
		opts.ConstLabels,
	)
	return newTimingHistogram(nowFunc, desc, opts)
}

// wrapTimingHelp 在 Help 前加 EXPERIMENTAL 前缀，提示消费者该指标语义可能变化。
func wrapTimingHelp(given string) string {
	return "EXPERIMENTAL: " + given
}

// newTimingHistogram 把变量标签展开为常量标签后构造底层加权直方图。
// TimingHistogram 没有可变标签，因此这里主要是把位置标签纳入 Desc。
func newTimingHistogram(nowFunc func() time.Time, desc *prometheus.Desc, opts TimingHistogramOpts, variableLabelValues ...string) (TimingHistogram, error) {
	allLabelsM := prometheus.Labels{}
	allLabelsS := prometheus.MakeLabelPairs(desc, variableLabelValues)
	for _, pair := range allLabelsS {
		// MakeLabelPairs 理论上不会返回 nil，防御性检查避免后续解引用 panic。
		if pair == nil || pair.Name == nil || pair.Value == nil {
			return nil, errors.New("prometheus.MakeLabelPairs returned a nil")
		}
		allLabelsM[*pair.Name] = *pair.Value
	}
	weighted, err := newWeightedHistogram(desc, WeightedHistogramOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Help:        opts.Help,
		ConstLabels: allLabelsM,
		Buckets:     opts.Buckets,
	}, variableLabelValues...)
	if err != nil {
		return nil, err
	}
	return &timingHistogram{
		nowFunc:  nowFunc,
		weighted: weighted,
		// 创建时刻即开始计时：初始值从此刻起参与积分。
		lastSetTime: nowFunc(),
		value:       opts.InitialValue,
	}, nil
}

// timingHistogram 把 gauge 操作转换为“值×持续时间”的加权观测。
type timingHistogram struct {
	nowFunc  func() time.Time
	weighted *weightedHistogram

	// 以下字段只能在持有 weighted 的锁时访问。

	lastSetTime time.Time // 记录上次设置值的时间
	value       float64
}

var _ TimingHistogram = &timingHistogram{}

// Set 将当前值设置为 newValue，并累计此前时段的积分。
func (th *timingHistogram) Set(newValue float64) {
	th.update(func(float64) float64 { return newValue })
}

// Inc 在当前值基础上加一，并按持续时间计入积分。
func (th *timingHistogram) Inc() {
	th.update(func(oldValue float64) float64 { return oldValue + 1 })
}

// Dec 在当前值基础上减一。
func (th *timingHistogram) Dec() {
	th.update(func(oldValue float64) float64 { return oldValue - 1 })
}

// Add 给当前值加上 delta。
func (th *timingHistogram) Add(delta float64) {
	th.update(func(oldValue float64) float64 { return oldValue + delta })
}

// Sub 从当前值减去 delta。
func (th *timingHistogram) Sub(delta float64) {
	th.update(func(oldValue float64) float64 { return oldValue - delta })
}

// SetToCurrentTime 把值设为 Unix 秒数，语义与 prometheus.Gauge 一致。
func (th *timingHistogram) SetToCurrentTime() {
	th.update(func(oldValue float64) float64 { return th.nowFunc().Sub(time.Unix(0, 0)).Seconds() })
}

// update 是所有写操作的公共路径：先按“旧值 × 经过时间”累加观测，
// 再修改值。时间和值必须一起在锁内更新，否则积分会丢失或重复计时段。
func (th *timingHistogram) update(updateFn func(float64) float64) {
	th.weighted.lock.Lock()
	defer th.weighted.lock.Unlock()
	now := th.nowFunc()
	delta := now.Sub(th.lastSetTime)
	value := th.value
	// 时钟可能回拨或精度不足；只有正的时间差才产生观测。
	if delta > 0 {
		th.weighted.observeWithWeightLocked(value, uint64(delta))
		th.lastSetTime = now
	}
	th.value = updateFn(value)
}

// Desc 返回底层加权直方图的描述符。
func (th *timingHistogram) Desc() *prometheus.Desc {
	return th.weighted.Desc()
}

// Write 在写出前把“最后一次更新至今”的时段也计入，避免抓取间隔内的积分丢失。
func (th *timingHistogram) Write(dest *dto.Metric) error {
	th.Add(0) // 计入自上次更新以来经过的时间
	return th.weighted.Write(dest)
}

// Describe 报告唯一的描述符。
func (th *timingHistogram) Describe(ch chan<- *prometheus.Desc) {
	ch <- th.weighted.Desc()
}

// Collect 把自身作为单个指标发送。
func (th *timingHistogram) Collect(ch chan<- prometheus.Metric) {
	ch <- th
}
