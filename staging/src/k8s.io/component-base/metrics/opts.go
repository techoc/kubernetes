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
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	internalmetrics "k8s.io/component-base/metrics/internal"
	promext "k8s.io/component-base/metrics/prometheusextension"
)

var (
	// labelValueAllowLists 以 "metric,label" 为键保存允许值集合；
	// 并发读写由 allowListLock 保护。
	labelValueAllowLists = map[string]*MetricLabelAllowList{}
	allowListLock        sync.RWMutex
)

// ResetLabelValueAllowLists 重置标签值允许列表。
// 注意：仅应在测试中使用。
func ResetLabelValueAllowLists() {
	allowListLock.Lock()
	defer allowListLock.Unlock()
	labelValueAllowLists = map[string]*MetricLabelAllowList{}
}

// KubeOpts 是 prometheus.Opts 的超集结构体。
// 特意不嵌入 prometheus.Opts，以保持调用方熟悉的结构体初始化方式。
// Name 必须设为非空字符串；只有指标确实已弃用时才设置 DeprecatedVersion。
// deprecateOnce/annotateOnce 保证 Help 文本只被改写一次，即使多次创建同名指标。
type KubeOpts struct {
	Namespace                     string
	Subsystem                     string
	Name                          string
	Help                          string
	ConstLabels                   map[string]string
	DeprecatedVersion             string
	deprecateOnce                 sync.Once
	annotateOnce                  sync.Once
	StabilityLevel                StabilityLevel
	initializeLabelAllowListsOnce sync.Once
	LabelValueAllowLists          *MetricLabelAllowList
}

// BuildFQName 用 "_" 连接三个名称组成部分，并忽略空部分。
// 无论其他部分如何，name 为空时始终返回空字符串。
// 本库的指标实现用它从 Opts 中的名称生成完整指标名；
// 调用方仅在实现自己的 Metric 或直接通过 NewDesc 创建 Desc 时才需要使用它。
func BuildFQName(namespace, subsystem, name string) string {
	return prometheus.BuildFQName(namespace, subsystem, name)
}

// StabilityLevel 表示已定义指标的 API 稳定性保证。
type StabilityLevel string

const (
	// INTERNAL 指标没有稳定性保证，标签可随意增删，指标也可随时删除。
	INTERNAL StabilityLevel = "INTERNAL"
	// ALPHA 指标没有稳定性保证，标签可随意增删，指标也可随时删除。
	ALPHA StabilityLevel = "ALPHA"
	// BETA 指标遵循控制平面指标稳定性 KEP 规定的弃用策略。
	BETA StabilityLevel = "BETA"
	// STABLE 指标保证不被修改，移除遵循控制平面指标稳定性 KEP 规定的弃用策略。
	STABLE StabilityLevel = "STABLE"
)

// setDefaults 在未声明稳定性时采用 ALPHA，避免指标被误当作稳定接口。
func (sl *StabilityLevel) setDefaults() {
	switch *sl {
	case "":
		*sl = ALPHA
	default:
		// 已设置稳定性级别，无需处理。
	}
}

// CounterOpts 是 KubeOpts 的别名，字段含义见 KubeOpts。
type CounterOpts KubeOpts

// markDeprecated 在指标描述中添加弃用说明。
//
// 使用 sync.Once 防止同一 Opts 被重复创建时 Help 前缀叠加。
func (o *CounterOpts) markDeprecated() {
	o.deprecateOnce.Do(func() {
		o.Help = fmt.Sprintf("(Deprecated since %v) %v", o.DeprecatedVersion, o.Help)
	})
}

// annotateStabilityLevel 在指标 Help 中添加稳定性级别。
//
// 稳定性级别会出现在 /metrics 的 HELP 中，便于告警规则与升级评估直接识别。
func (o *CounterOpts) annotateStabilityLevel() {
	o.annotateOnce.Do(func() {
		o.Help = fmt.Sprintf("[%v] %v", o.StabilityLevel, o.Help)
	})
}

// 将选项便捷转换为对应的 prometheus 配置；待有完善的标签抽象后再扩展。
func (o *CounterOpts) toPromCounterOpts() prometheus.CounterOpts {
	return prometheus.CounterOpts{
		Namespace:   o.Namespace,
		Subsystem:   o.Subsystem,
		Name:        o.Name,
		Help:        o.Help,
		ConstLabels: o.ConstLabels,
	}
}

// GaugeOpts 是 KubeOpts 的别名，字段含义见 KubeOpts。
type GaugeOpts KubeOpts

// markDeprecated 在指标描述中添加弃用说明。
//
// 与 CounterOpts 相同，Once 保证 Help 只改写一次。
func (o *GaugeOpts) markDeprecated() {
	o.deprecateOnce.Do(func() {
		o.Help = fmt.Sprintf("(Deprecated since %v) %v", o.DeprecatedVersion, o.Help)
	})
}

// annotateStabilityLevel 在指标 Help 中添加稳定性级别。
func (o *GaugeOpts) annotateStabilityLevel() {
	o.annotateOnce.Do(func() {
		o.Help = fmt.Sprintf("[%v] %v", o.StabilityLevel, o.Help)
	})
}

// 将选项便捷转换为对应的 prometheus 配置；待有完善的标签抽象后再扩展。
func (o *GaugeOpts) toPromGaugeOpts() prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace:   o.Namespace,
		Subsystem:   o.Subsystem,
		Name:        o.Name,
		Help:        o.Help,
		ConstLabels: o.ConstLabels,
	}
}

// HistogramOpts 汇总创建 Histogram 指标的选项。
// Name 必须设为非空字符串；其他字段可保留零值，但建议设置 Help。
// Buckets 为经典直方图边界；启用原生直方图时两者会同时暴露。
type HistogramOpts struct {
	Namespace                     string
	Subsystem                     string
	Name                          string
	Help                          string
	ConstLabels                   map[string]string
	Buckets                       []float64
	DeprecatedVersion             string
	deprecateOnce                 sync.Once
	annotateOnce                  sync.Once
	StabilityLevel                StabilityLevel
	initializeLabelAllowListsOnce sync.Once
	LabelValueAllowLists          *MetricLabelAllowList
}

// markDeprecated 在指标描述中添加弃用说明。
//
// 与 CounterOpts 一致，Once 保证 Help 只改写一次。
func (o *HistogramOpts) markDeprecated() {
	o.deprecateOnce.Do(func() {
		o.Help = fmt.Sprintf("(Deprecated since %v) %v", o.DeprecatedVersion, o.Help)
	})
}

// annotateStabilityLevel 在指标 Help 中添加稳定性级别。
func (o *HistogramOpts) annotateStabilityLevel() {
	o.annotateOnce.Do(func() {
		o.Help = fmt.Sprintf("[%v] %v", o.StabilityLevel, o.Help)
	})
}

// 将选项便捷转换为对应的 prometheus 配置；待有完善的标签抽象后再扩展。
func (o *HistogramOpts) toPromHistogramOpts() prometheus.HistogramOpts {
	opts := prometheus.HistogramOpts{
		Namespace:   o.Namespace,
		Subsystem:   o.Subsystem,
		Name:        o.Name,
		Help:        o.Help,
		ConstLabels: o.ConstLabels,
		Buckets:     o.Buckets,
	}

	// 启用原生直方图时配置指数桶参数，同时以经典和原生格式暴露指标。
	// 保留经典 Buckets 以兼容旧采集器，并附加远端存储可用的原生直方图参数。
	if internalmetrics.NativeHistogramsEnabled() {
		nativeOpts := internalmetrics.NativeHistogramConfig()
		opts.NativeHistogramBucketFactor = nativeOpts.BucketFactor
		opts.NativeHistogramMaxBucketNumber = nativeOpts.MaxBucketNumber
	}

	return opts
}

// TimingHistogramOpts 汇总创建 TimingHistogram 指标的选项。
// Name 必须设为非空字符串；其他字段可保留零值，但建议设置 Help。
// InitialValue 用于预置未观测时的初始值，避免依赖该指标的告警在启动瞬间误触发。
type TimingHistogramOpts struct {
	Namespace                     string
	Subsystem                     string
	Name                          string
	Help                          string
	ConstLabels                   map[string]string
	Buckets                       []float64
	InitialValue                  float64
	DeprecatedVersion             string
	deprecateOnce                 sync.Once
	annotateOnce                  sync.Once
	StabilityLevel                StabilityLevel
	initializeLabelAllowListsOnce sync.Once
	LabelValueAllowLists          *MetricLabelAllowList
}

// markDeprecated 在指标描述中添加弃用说明。
func (o *TimingHistogramOpts) markDeprecated() {
	o.deprecateOnce.Do(func() {
		o.Help = fmt.Sprintf("(Deprecated since %v) %v", o.DeprecatedVersion, o.Help)
	})
}

// annotateStabilityLevel 在指标 Help 中添加稳定性级别。
func (o *TimingHistogramOpts) annotateStabilityLevel() {
	o.annotateOnce.Do(func() {
		o.Help = fmt.Sprintf("[%v] %v", o.StabilityLevel, o.Help)
	})
}

// 将选项便捷转换为对应的 prometheus 配置；待有完善的标签抽象后再扩展。
func (o *TimingHistogramOpts) toPromHistogramOpts() promext.TimingHistogramOpts {
	return promext.TimingHistogramOpts{
		Namespace:    o.Namespace,
		Subsystem:    o.Subsystem,
		Name:         o.Name,
		Help:         o.Help,
		ConstLabels:  o.ConstLabels,
		Buckets:      o.Buckets,
		InitialValue: o.InitialValue,
	}
}

// SummaryOpts 汇总创建 Summary 指标的选项。
// Name 必须设为非空字符串；其他字段可保留零值，但建议设置 Help，
// 并显式指定 Objectives，因为库的默认值将在 v0.10 中改变。
// Objectives 为空时使用 defObjectives，以维持旧版 quantile 行为。
type SummaryOpts struct {
	Namespace                     string
	Subsystem                     string
	Name                          string
	Help                          string
	ConstLabels                   map[string]string
	Objectives                    map[float64]float64
	MaxAge                        time.Duration
	AgeBuckets                    uint32
	BufCap                        uint32
	DeprecatedVersion             string
	deprecateOnce                 sync.Once
	annotateOnce                  sync.Once
	StabilityLevel                StabilityLevel
	initializeLabelAllowListsOnce sync.Once
	LabelValueAllowLists          *MetricLabelAllowList
}

// markDeprecated 在指标描述中添加弃用说明。
//
// 与 CounterOpts 一致，Once 保证 Help 只改写一次。
func (o *SummaryOpts) markDeprecated() {
	o.deprecateOnce.Do(func() {
		o.Help = fmt.Sprintf("(Deprecated since %v) %v", o.DeprecatedVersion, o.Help)
	})
}

// annotateStabilityLevel 在指标 Help 中添加稳定性级别。
func (o *SummaryOpts) annotateStabilityLevel() {
	o.annotateOnce.Do(func() {
		o.Help = fmt.Sprintf("[%v] %v", o.StabilityLevel, o.Help)
	})
}

// Deprecated: 从库的 v1.0.0 起，DefObjectives 将不再作为默认 objectives；
// 此后默认 Summary 将不包含 quantile。
var (
	// defObjectives 是 prometheus v1 之前的默认 quantile，保留以维持兼容。
	defObjectives = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
)

// 将选项便捷转换为对应的 prometheus 配置；待有完善的标签抽象后再扩展。
func (o *SummaryOpts) toPromSummaryOpts() prometheus.SummaryOpts {
	// 为保持向后兼容，沿用 prometheus v1 之前的 quantile 默认行为。
	objectives := o.Objectives
	if objectives == nil {
		objectives = defObjectives
	}
	return prometheus.SummaryOpts{
		Namespace:   o.Namespace,
		Subsystem:   o.Subsystem,
		Name:        o.Name,
		Help:        o.Help,
		ConstLabels: o.ConstLabels,
		Objectives:  objectives,
		MaxAge:      o.MaxAge,
		AgeBuckets:  o.AgeBuckets,
		BufCap:      o.BufCap,
	}
}
