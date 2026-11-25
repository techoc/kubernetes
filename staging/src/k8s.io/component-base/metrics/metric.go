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
	"sync"
	"sync/atomic"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	promext "k8s.io/component-base/metrics/prometheusextension"
	"k8s.io/klog/v2"
)

/*
kubeCollector 扩展 prometheus.Collector，允许自定义注册流程：将指标初始化推迟到 Create()，
并根据指标是否已弃用，委托调用底层指标的 initializeMetric 或 initializeDeprecatedMetric。
*/
// 中文补充：把“是否创建、是否弃用、是否隐藏”的决策推迟到注册时，注册表才能拿到版本。
type kubeCollector interface {
	Collector
	lazyKubeMetric
	DeprecatedVersion() *semver.Version
	// 每个 collector 指标都需提供弃用与非弃用两种初始化函数，因为实例化会推迟到实际注册时。
	initializeMetric()
	initializeDeprecatedMetric()
}

/*
lazyKubeMetric 定义指标注册接口，要求实现者延迟实例化指标，直到显式调用 Create()。
*/
type lazyKubeMetric interface {
	Create(*semver.Version) bool
	IsCreated() bool
	IsHidden() bool
	IsDeprecated() bool
}

/*
lazyMetric 实现 lazyKubeMetric，等待注册时才实例化。将其作为匿名字段嵌入实现 kubeCollector
的结构体即可获得延迟注册行为；调用方必须以该 kubeCollector 自身为参数调用 lazyInit。
*/
type lazyMetric struct {
	fqName string
	// 弃用/隐藏状态用 atomic.Bool，因为 Enabled 风格的读取在运行期无锁并发。
	isDeprecated atomic.Bool
	isHidden     atomic.Bool
	isCreated    bool
	createLock   sync.RWMutex
	// createOnce/markDeprecationOnce 保证 Create 与弃用判定只执行一次（ClearState 后可重置）。
	markDeprecationOnce sync.Once
	createOnce          sync.Once
	self                kubeCollector
	stabilityLevel      StabilityLevel
}

// IsCreated 报告指标是否已经实例化。
func (r *lazyMetric) IsCreated() bool {
	r.createLock.RLock()
	defer r.createLock.RUnlock()
	return r.isCreated
}

// lazyInit 为 lazyMetric 保存允许其延迟初始化的 kubeCollector 引用，应在创建该对象的工厂函数中调用。
func (r *lazyMetric) lazyInit(self kubeCollector, fqName string) {
	r.fqName = fqName
	r.self = self
}

// preprocessMetric 判断延迟指标是否应被隐藏。Version 参数是当前运行二进制的版本。
// 满足以下任一条件时指标会被隐藏：
//  1. 指标已弃用且超过宽限期，即弃用时间已超过一个发布周期。
//  2. 指标通过 CLI 标志被手动禁用。
//
// 注意：通过 CLI 标志禁用指标的优先级高于弃用，会覆盖显式禁用指标的 show-hidden-metrics 设置。
func (r *lazyMetric) preprocessMetric(currentVersion semver.Version) {
	disabledMetricsLock.RLock()
	defer disabledMetricsLock.RUnlock()
	// 禁用指标的优先级高于展示隐藏指标。
	if _, ok := disabledMetrics[r.fqName]; ok {
		r.isHidden.Store(true)
		return
	}
	deprecatedVersion := r.self.DeprecatedVersion()
	if deprecatedVersion == nil {
		return
	}
	r.markDeprecationOnce.Do(func() {
		r.isDeprecated.Store(isDeprecated(currentVersion, *deprecatedVersion))

		if shouldHide(r.stabilityLevel, &currentVersion, deprecatedVersion) {
			if ShouldShowHidden() {
				klog.Warningf("Hidden metrics (%s) have been manually overridden, showing this very deprecated metric.", r.fqName)
				return
			}
			// TODO(RainbowMango): 暂时移除此日志。https://github.com/kubernetes/kubernetes/issues/85369
			// klog.Warningf("This metric has been deprecated for more than one release, hiding.")
			r.isHidden.Store(true)
		}
	})
}

func (r *lazyMetric) IsHidden() bool {
	return r.isHidden.Load()
}

// IsDeprecated 报告指标是否已进入弃用期（仍可能对外暴露）。
func (r *lazyMetric) IsDeprecated() bool {
	return r.isDeprecated.Load()
}

// Create 强制完成此前延迟的指标初始化，并判定指标是否已弃用或隐藏；应隐藏时不执行创建。
// 若指标已经创建，则不再操作并返回 true。
func (r *lazyMetric) Create(version *semver.Version) bool {
	// version 为 nil 表示调用方不关心弃用隐藏，只做实例化。
	if version != nil {
		r.preprocessMetric(*version)
	}
	// 已决定隐藏的指标不创建。
	if r.IsHidden() {
		return false
	}

	// 双重检查 + Once：并发注册时只初始化一次底层指标。
	r.createOnce.Do(func() {
		r.createLock.Lock()
		defer r.createLock.Unlock()
		r.isCreated = true
		if r.IsDeprecated() {
			r.self.initializeDeprecatedMetric()
		} else {
			r.self.initializeMetric()
		}
	})
	// 记录注册指标数，按稳定性与弃用版本打标签，便于观察指标治理进度。
	sl := r.stabilityLevel
	deprecatedV := r.self.DeprecatedVersion()
	dv := ""
	if deprecatedV != nil {
		dv = deprecatedV.String()
	}
	registeredMetricsTotal.WithLabelValues(string(sl), dv).Inc()
	return r.IsCreated()
}

// ClearState 清除 Create 设置的所有状态，以便重新注册隐藏指标。
func (r *lazyMetric) ClearState() {
	r.createLock.Lock()
	defer r.createLock.Unlock()

	// 复位所有一次性标志，使指标可以按新的版本重新判定与实例化。
	r.isDeprecated.Store(false)
	r.isHidden.Store(false)
	r.isCreated = false
	r.markDeprecationOnce = sync.Once{}
	r.createOnce = sync.Once{}
}

// FQName 返回 collector 的完整限定指标名。
func (r *lazyMetric) FQName() string {
	return r.fqName
}

/*
以下代码直接取自 prometheus 代码库：已实现 Metric 接口的结构体可通过该便捷结构体自动满足
Collector 接口。
参考：https://github.com/prometheus/client_golang/blob/v0.9.2/prometheus/collector.go#L98-L120
*/
// selfCollector 让实现 Metric 的结构体自动获得 Collector 行为：Describe/Collect 都转发给自身。
type selfCollector struct {
	metric prometheus.Metric
}

func (c *selfCollector) initSelfCollection(m prometheus.Metric) {
	c.metric = m
}

// Describe 转发到被包装指标的描述符。
func (c *selfCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.metric.Desc()
}

// Collect 把自身作为唯一指标发送。
func (c *selfCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- c.metric
}

// 这些零值 vec 只作为占位，解引用会 panic，因此只会被类型断言后从不实际使用。
var noopCounterVec = &prometheus.CounterVec{}
var noopHistogramVec = &prometheus.HistogramVec{}
var noopTimingHistogramVec = &promext.TimingHistogramVec{}
var noopGaugeVec = &prometheus.GaugeVec{}

// 所有空操作统一使用该便捷结构体。
var noop = &noopMetric{}

// noopMetric 实现所有指标操作的空版本，未注册或隐藏指标返回它以避免 nil 检查。
type noopMetric struct{}

func (noopMetric) Inc()                              {}
func (noopMetric) Add(float64)                       {}
func (noopMetric) Dec()                              {}
func (noopMetric) Set(float64)                       {}
func (noopMetric) Sub(float64)                       {}
func (noopMetric) Observe(float64)                   {}
func (noopMetric) ObserveWithWeight(float64, uint64) {}
func (noopMetric) SetToCurrentTime()                 {}
func (noopMetric) Desc() *prometheus.Desc            { return nil }
func (noopMetric) Write(*dto.Metric) error           { return nil }
func (noopMetric) Describe(chan<- *prometheus.Desc)  {}
func (noopMetric) Collect(chan<- prometheus.Metric)  {}
