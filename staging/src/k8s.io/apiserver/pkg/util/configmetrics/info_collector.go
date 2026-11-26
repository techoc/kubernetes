/*
Copyright 2025 The Kubernetes Authors.

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

// Package configmetrics 以指标形式暴露配置内容哈希，便于判断多个 apiserver
// 实例是否加载了相同配置。
package configmetrics

import (
	"slices"
	"sync/atomic"

	"k8s.io/component-base/metrics"
	"k8s.io/utils/ptr"
)

// HashProvider 是获取当前配置哈希值的接口。抽象出接口是为了让指标采集与哈希计算解耦，测试可注入固定值。
type HashProvider interface {
	GetCurrentHashes() []string
	SetHashes(hashes ...string)
}

// AtomicHashProvider 用指向切片的单个原子指针实现 HashProvider。配置热加载时写、
// 指标采集时读，用原子指针避免读写锁；哈希列表很小，整体替换比逐项同步更简单。
type AtomicHashProvider struct {
	hashes *atomic.Pointer[[]string]
}

// NewAtomicHashProvider 创建一个原子哈希提供者。
func NewAtomicHashProvider() *AtomicHashProvider {
	p := &AtomicHashProvider{
		hashes: &atomic.Pointer[[]string]{},
	}
	// 用空切片初始化：预置空切片使首次 Load 不会得到 nil，减少调用方的判空分支。
	p.hashes.Store(ptr.To([]string{}))
	return p
}

// GetCurrentHashes 返回哈希列表的副本。
//
// 返回副本而非内部切片，防止调用方修改后与 SetHashes 产生数据竞争。
func (h *AtomicHashProvider) GetCurrentHashes() []string {
	hashesPtr := h.hashes.Load()
	if hashesPtr == nil {
		// 理论上不会发生，兜底处理。
		return []string{}
	}
	// 返回副本，避免外部修改内部状态。
	hashes := *hashesPtr
	return slices.Clone(hashes)
}

// SetHashes 原子替换哈希列表。
//
// 先克隆入参再存储，避免调用方之后复用底层数组造成别名问题。
func (h *AtomicHashProvider) SetHashes(hashes ...string) {
	hashCopy := slices.Clone(hashes)
	h.hashes.Store(&hashCopy)
}

// NewConfigInfoCustomCollector 为配置哈希 info 指标创建自定义采集器。它按需读取指标，
// 从而免去状态管理和锁。采集时才读取哈希，指标始终反映最新配置，无需在配置变更时维护指标状态。
func NewConfigInfoCustomCollector(desc *metrics.Desc, hashProvider HashProvider) metrics.StableCollector {
	return &configInfoCustomCollector{
		desc:         desc,
		hashProvider: hashProvider,
	}
}

// configInfoCustomCollector 按需生成 info 型指标。
type configInfoCustomCollector struct {
	metrics.BaseStableCollector
	desc         *metrics.Desc
	hashProvider HashProvider
}

var _ metrics.StableCollector = &configInfoCustomCollector{}

// DescribeWithStability 声明该 collector 唯一产出的指标描述。
func (c *configInfoCustomCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- c.desc
}

// CollectWithStability 用当前哈希作为标签值输出值为 1 的 gauge。
//
// 无哈希时不输出样本，避免产生一个无意义的空标签序列。
func (c *configInfoCustomCollector) CollectWithStability(ch chan<- metrics.Metric) {
	hashes := c.hashProvider.GetCurrentHashes()
	if len(hashes) == 0 {
		return
	}

	ch <- metrics.NewLazyConstMetric(c.desc, metrics.GaugeValue, 1, hashes...)
}
