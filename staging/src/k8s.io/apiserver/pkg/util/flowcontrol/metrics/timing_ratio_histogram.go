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

// Package metrics 提供 APF 使用的特殊指标类型：可分别设置分子与分母的
// “比值直方图”，用于表达并发度/利用率这类会随时间波动的比例。
package metrics

import (
	"context"
	"sync"
	"time"

	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
)

// TimingRatioHistogram 本质上是一个比值 gauge，客户端可独立控制分子与分母。
// 被抓取时，它产生一份对每一纳秒末尾采样的比值直方图。
//
// `*TimingRatioHistogram` 同时实现 Registerable 与 RatioedGauge。
type TimingRatioHistogram struct {
	// 该实现基于 TimingHistogram，
	// 额外除以一个偶尔调整的分母。

	// Registerable 是可注册相关的部分。
	// 即底层 TimingHistogram 的可注册能力。
	compbasemetrics.Registerable

	// timingRatioHistogramInner 实现 RatioedGauge 部分。
	timingRatioHistogramInner
}

// TimingRatioHistogramOpts 是 TimingRatioHistogram 的构造参数。
// `TimingHistogramOpts.InitialValue` 是初始分子。
type TimingRatioHistogramOpts struct {
	compbasemetrics.TimingHistogramOpts
	InitialDenominator float64
}

// timingRatioHistogramInner 实现埋点部分
//
// 分子分母的更新必须原子完成，否则并发 Set/SetDenominator 会产生
// 与任一真实状态都不对应的中间比值。
type timingRatioHistogramInner struct {
	nowFunc         func() time.Time
	getGaugeOfRatio func() Gauge
	sync.Mutex
	// 仅在持有 mutex 时访问
	numerator, denominator float64
}

var _ RatioedGauge = &timingRatioHistogramInner{}
var _ RatioedGauge = &TimingRatioHistogram{}
var _ compbasemetrics.Registerable = &TimingRatioHistogram{}

// NewTimingHistogram 返回一个类 TimingHistogram 的对象。但在至少注册到一个 registry 之前，
// 不会测量任何数据。
func NewTimingRatioHistogram(opts *TimingRatioHistogramOpts) *TimingRatioHistogram {
	return NewTestableTimingRatioHistogram(time.Now, opts)
}

// NewTestableTimingHistogram 额外支持注入时钟。
//
// 构造时把 InitialValue 视为分子并除以初始分母，保证底层直方图一开始
// 就持有“比值”而非原始分子。
func NewTestableTimingRatioHistogram(nowFunc func() time.Time, opts *TimingRatioHistogramOpts) *TimingRatioHistogram {
	//nolint:govet // copylocks: assignment copies lock value to ratioedOpts: k8s.io/component-base/metrics.TimingHistogramOpts contains sync.Once contains sync.Mutex
	ratioedOpts := opts.TimingHistogramOpts
	ratioedOpts.InitialValue /= opts.InitialDenominator
	th := compbasemetrics.NewTestableTimingHistogram(nowFunc, &ratioedOpts)
	return &TimingRatioHistogram{
		Registerable: th,
		timingRatioHistogramInner: timingRatioHistogramInner{
			nowFunc:         nowFunc,
			getGaugeOfRatio: func() Gauge { return th },
			numerator:       opts.InitialValue,
			denominator:     opts.InitialDenominator,
		}}
}

func (trh *timingRatioHistogramInner) Set(numerator float64) {
	trh.Lock()
	defer trh.Unlock()
	trh.numerator = numerator
	ratio := numerator / trh.denominator
	trh.getGaugeOfRatio().Set(ratio)
}

func (trh *timingRatioHistogramInner) Add(deltaNumerator float64) {
	trh.Lock()
	defer trh.Unlock()
	numerator := trh.numerator + deltaNumerator
	trh.numerator = numerator
	ratio := numerator / trh.denominator
	trh.getGaugeOfRatio().Set(ratio)
}

func (trh *timingRatioHistogramInner) Sub(deltaNumerator float64) {
	trh.Add(-deltaNumerator)
}

func (trh *timingRatioHistogramInner) Inc() {
	trh.Add(1)
}

func (trh *timingRatioHistogramInner) Dec() {
	trh.Add(-1)
}

func (trh *timingRatioHistogramInner) SetToCurrentTime() {
	trh.Set(float64(trh.nowFunc().Sub(time.Unix(0, 0))))
}

// SetDenominator 更新分母并立即重算比值；分母变化本身也代表系统容量
// 调整，需要反映到指标上。
func (trh *timingRatioHistogramInner) SetDenominator(denominator float64) {
	trh.Lock()
	defer trh.Unlock()
	trh.denominator = denominator
	ratio := trh.numerator / denominator
	trh.getGaugeOfRatio().Set(ratio)
}

// WithContext 让普通 TimingHistogram 指标可以传入 context。
// 当前阶段 context 不起任何作用。
func (trh *timingRatioHistogramInner) WithContext(ctx context.Context) RatioedGauge {
	return trh
}

// TimingRatioHistogramVec 是一组 TimingRatioHistogram，彼此仅在标签值上不同。
// TimingRatioHistogramVec 是一组 TimingRatioHistogram，彼此仅在标签值上不同。
// `*TimingRatioHistogramVec` 同时实现 Registerable 与 RatioedGaugeVec。
type TimingRatioHistogramVec struct {
	// 只提升 Registerable 方法
	compbasemetrics.Registerable
	// delegate 是存放比值的 TimingHistogram。
	delegate compbasemetrics.GaugeVecMetric
}

var _ RatioedGaugeVec = &TimingRatioHistogramVec{}
var _ compbasemetrics.Registerable = &TimingRatioHistogramVec{}

// NewTimingHistogramVec 构造一个新 vector。
// `opts.InitialValue` 是初始比值，但这一设定只在极短时间内有效：
// 之后 NewForLabelValuesSafe 会根据给定的初始分子与分母
// 重新设置比值。
// 因此在成员构造期间有极短一段时间，底层 TimingHistogram 拿到的是初始分子而非
// 初始比值（两者都为 0 时显然无所谓）。
//
// 注意在注册 vector 之前取出成员会遇到的困难。
func NewTimingRatioHistogramVec(opts *compbasemetrics.TimingHistogramOpts, labelNames ...string) *TimingRatioHistogramVec {
	return NewTestableTimingRatioHistogramVec(time.Now, opts, labelNames...)
}

// NewTestableTimingHistogramVec 额外支持注入时钟。
func NewTestableTimingRatioHistogramVec(nowFunc func() time.Time, opts *compbasemetrics.TimingHistogramOpts, labelNames ...string) *TimingRatioHistogramVec {
	delegate := compbasemetrics.NewTestableTimingHistogramVec(nowFunc, opts, labelNames)
	return &TimingRatioHistogramVec{
		Registerable: delegate,
		delegate:     delegate,
	}
}

func (v *TimingRatioHistogramVec) metrics() Registerables {
	return Registerables{v}
}

// 若该 vec 未隐藏且尚未注册，或 labelValues 存在语法问题，
// NewForLabelValuesChecked 返回错误。
//
// 返回的 gauge 直接绑定到具体的 vector member，后续更新无需再次查找。
func (v *TimingRatioHistogramVec) NewForLabelValuesChecked(initialNumerator, initialDenominator float64, labelValues []string) (RatioedGauge, error) {
	underMember, err := v.delegate.WithLabelValuesChecked(labelValues...)
	if err != nil {
		return noopRatioed{}, err
	}
	underMember.Set(initialNumerator / initialDenominator)
	return &timingRatioHistogramInner{
		getGaugeOfRatio: func() Gauge { return underMember },
		numerator:       initialNumerator,
		denominator:     initialDenominator,
	}, nil
}

// NewForLabelValuesSafe 在 NewForLabelValuesChecked 不返回错误时与其行为一致。
// 当非安全版本因 vector 尚未注册而返回错误时，安全版本返回一个对象，
// 它的方法在每次调用时查找对应的 vector 成员（注册后便不再是 noop）。
// 在其它错误情形下，这里返回的对象是 noop。
func (v *TimingRatioHistogramVec) NewForLabelValuesSafe(initialNumerator, initialDenominator float64, labelValues []string) RatioedGauge {
	tro, err := v.NewForLabelValuesChecked(initialNumerator, initialDenominator, labelValues)
	if err == nil {
		klog.V(3).InfoS("TimingRatioHistogramVec.NewForLabelValuesSafe hit the efficient case", "fqName", v.FQName(), "labelValues", labelValues)
		return tro
	}
	if !compbasemetrics.ErrIsNotRegistered(err) {
		klog.ErrorS(err, "Failed to extract TimingRatioHistogramVec member, using noop instead", "vectorname", v.FQName(), "labelValues", labelValues)
		return tro
	}
	klog.V(3).InfoS("TimingRatioHistogramVec.NewForLabelValuesSafe hit the inefficient case", "fqName", v.FQName(), "labelValues", labelValues)
	// 此时已知 v.NewForLabelValuesChecked(..) 返回永久 noop，
	// 这正是我们要避免使用的。因此自己实现 gauge，
	// 在每次 Set 时获取元素。
	return &timingRatioHistogramInner{
		getGaugeOfRatio: func() Gauge { return v.delegate.WithLabelValues(labelValues...) },
		numerator:       initialNumerator,
		denominator:     initialDenominator,
	}
}

// noopRatioed 是无效标签/未注册场景下的占位实现，所有操作静默丢弃，
// 保证调用方无需到处判空。
type noopRatioed struct{}

func (noopRatioed) Set(float64)            {}
func (noopRatioed) Add(float64)            {}
func (noopRatioed) Sub(float64)            {}
func (noopRatioed) Inc()                   {}
func (noopRatioed) Dec()                   {}
func (noopRatioed) SetToCurrentTime()      {}
func (noopRatioed) SetDenominator(float64) {}

// Reset 清空所有 label 的成员，主要用于测试。
func (v *TimingRatioHistogramVec) Reset() {
	v.delegate.Reset()
}
