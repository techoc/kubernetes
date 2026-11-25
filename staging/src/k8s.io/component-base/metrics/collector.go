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

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// StableCollector 扩展 prometheus.Collector，允许自定义注册流程，尤其适用于自定义 collector。
// 在注册时由 registry 调用 Create，按当前版本决定描述符是注册、隐藏还是忽略。
type StableCollector interface {
	prometheus.Collector

	// DescribeWithStability 将该 StableCollector 能收集的全部 metrics.Desc 超集发送到给定通道。
	// 实现者需在 Create 前上报全部描述符，registry 才能统一做稳定性筛选。
	DescribeWithStability(chan<- *Desc)

	// CollectWithStability 通过给定通道发送每个收集到的 metrics.Metric。
	CollectWithStability(chan<- Metric)

	// Create 初始化所有 Desc，供 registry 调用。
	Create(version *semver.Version, self StableCollector) bool

	// ClearState 清除 Create 设置的所有状态。
	ClearState()

	// HiddenMetrics 返回以 fqName 表示的隐藏指标列表。
	HiddenMetrics() []string
}

// BaseStableCollector 实现 StableCollector 的几乎所有方法，为自定义 collector 提供便捷基类。
// 建议实现自定义 collector 时继承 BaseStableCollector。
type BaseStableCollector struct {
	descriptors  map[string]*Desc // 按 <fqName, Desc> 保存 DescribeWithStability() 收集到的全部描述符。
	registerable map[string]*Desc // 按 <fqName, Desc> 保存可注册描述符，是 descriptors 的子集。
	hidden       map[string]*Desc // 按 <fqName, Desc> 保存隐藏描述符，是 descriptors 的子集。
	// self 指回外层 collector，用于回调其 DescribeWithStability/CollectWithStability。
	self StableCollector
}

// DescribeWithStability 将全部描述符发送到给定通道，自定义 collector 应重写此方法。
func (bsc *BaseStableCollector) DescribeWithStability(ch chan<- *Desc) {
	panic(fmt.Errorf("custom collector should over-write DescribeWithStability method"))
}

// Describe 将全部描述符发送到给定通道，供 prometheus registry 调用。
// 只发送已通过稳定性筛选的描述符，隐藏指标不会出现在这里。
func (bsc *BaseStableCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range bsc.registerable {
		ch <- d.toPrometheusDesc()
	}
}

// CollectWithStability 将全部指标发送到给定通道，自定义 collector 应重写此方法。
func (bsc *BaseStableCollector) CollectWithStability(ch chan<- Metric) {
	panic(fmt.Errorf("custom collector should over-write CollectWithStability method"))
}

// Collect 由 Prometheus registry 在收集指标时调用。
// 用 goroutine 桥接回调式接口与通道式 Collect，同时过滤隐藏指标（nil）。
func (bsc *BaseStableCollector) Collect(ch chan<- prometheus.Metric) {
	mch := make(chan Metric)

	go func() {
		bsc.self.CollectWithStability(mch)
		close(mch)
	}()

	for m := range mch {
		// nil Metric 通常表示隐藏指标。
		if m == nil {
			continue
		}

		ch <- prometheus.Metric(m)
	}
}

// add 登记描述符；名称缺失或重复都是编程错误，直接 panic。
func (bsc *BaseStableCollector) add(d *Desc) {
	if len(d.fqName) == 0 {
		panic("nameless metrics will be not allowed")
	}

	if bsc.descriptors == nil {
		bsc.descriptors = make(map[string]*Desc)
	}

	if _, exist := bsc.descriptors[d.fqName]; exist {
		panic(fmt.Sprintf("duplicate metrics (%s) will be not allowed", d.fqName))
	}

	bsc.descriptors[d.fqName] = d
}

// init 供 registry 调用。
// 通过通道收集实现方上报的描述符，允许实现方在独立 goroutine 中生产。
func (bsc *BaseStableCollector) init(self StableCollector) {
	bsc.self = self

	dch := make(chan *Desc)

	// 收集自定义实现方可能上报的全部描述符。
	go func() {
		bsc.self.DescribeWithStability(dch)
		close(dch)
	}()

	for d := range dch {
		bsc.add(d)
	}
}

// trackRegistrableDescriptor 记录将对外暴露的描述符。
func (bsc *BaseStableCollector) trackRegistrableDescriptor(d *Desc) {
	if bsc.registerable == nil {
		bsc.registerable = make(map[string]*Desc)
	}

	bsc.registerable[d.fqName] = d
}

// trackHiddenDescriptor 记录被隐藏的描述符，供 HiddenMetrics 查询。
func (bsc *BaseStableCollector) trackHiddenDescriptor(d *Desc) {
	if bsc.hidden == nil {
		bsc.hidden = make(map[string]*Desc)
	}

	bsc.hidden[d.fqName] = d
}

// Create 供 registry 调用；只要至少一个指标未被隐藏就返回 true，否则返回 false，表示 registry 将忽略整个 collector。
func (bsc *BaseStableCollector) Create(version *semver.Version, self StableCollector) bool {
	bsc.init(self)

	// 对每个描述符执行稳定性判定，分入可注册或隐藏两类。
	for _, d := range bsc.descriptors {
		d.create(version)
		if d.IsHidden() {
			bsc.trackHiddenDescriptor(d)
		} else {
			bsc.trackRegistrableDescriptor(d)
		}
	}

	// 全部指标都被隐藏时整体忽略该 collector，避免空壳注册。
	if len(bsc.registerable) > 0 {
		return true
	}

	return false
}

// ClearState 清除 Create 设置的所有状态，以便重新注册隐藏指标。
func (bsc *BaseStableCollector) ClearState() {
	for _, d := range bsc.descriptors {
		d.ClearState()
	}

	// 重置所有缓存，使下一次 Create 能按新版本重新判定。
	bsc.descriptors = nil
	bsc.registerable = nil
	bsc.hidden = nil
	bsc.self = nil
}

// HiddenMetrics 返回以 fqName 表示的隐藏指标列表。
func (bsc *BaseStableCollector) HiddenMetrics() (fqNames []string) {
	// 对 map 做 range 顺序不确定，调用方若需稳定顺序应自行排序。
	for i := range bsc.hidden {
		fqNames = append(fqNames, bsc.hidden[i].fqName)
	}
	return
}

// 检查 BaseStableCollector 是否实现所需接口。
var _ StableCollector = &BaseStableCollector{}
