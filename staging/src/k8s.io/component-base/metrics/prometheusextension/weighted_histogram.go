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

// Package prometheusextension 提供 Prometheus 客户端尚未包含的实验性指标扩展：
// 支持带权重观测的 histogram，以及按“值×持续时间”积分的 timing histogram。
// 这些实现带有 EXPERIMENTAL 前缀，API 不保证稳定。
package prometheusextension

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// WeightedHistogram 扩展了普通 Histogram，使每次观测都带有一个权重。
// 对于给定的 x 和 N，一次 ObserveWithWeight(x, N) 等价于 N 次 ObserveWithWeight(x, 1)。
// 尽管实现采取了措施减小误差，加权总和仍可能因浮点运算而略有差异。
// 所有观测权重都为 1 时，语义与标准 Histogram 一致。
type WeightedHistogram interface {
	prometheus.Metric
	prometheus.Collector
	WeightedObserver
}

// WeightedObserver 扩展了 Observer 接口，使观测值可以带有权重。
type WeightedObserver interface {
	// ObserveWithWeight 以指定权重记录给定值。
	ObserveWithWeight(value float64, weight uint64)
}

// WeightedHistogramOpts 与普通 Histogram 的选项类型相同。
//
// 直接复用标准 HistogramOpts，避免维护一份会漂移的平行配置。
type WeightedHistogramOpts = prometheus.HistogramOpts

// NewWeightedHistogram 创建一个新的 WeightedHistogram。
//
// 返回值实现 prometheus.Metric 与 Collector，可直接交给 registry 注册。
func NewWeightedHistogram(opts WeightedHistogramOpts) (WeightedHistogram, error) {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		wrapWeightedHelp(opts.Help),
		nil,
		opts.ConstLabels,
	)
	return newWeightedHistogram(desc, opts)
}

// wrapWeightedHelp 在 Help 前加 EXPERIMENTAL 标记，
// 让使用者在 /metrics 输出里一眼看出该指标不属于稳定契约。
func wrapWeightedHelp(given string) string {
	return "EXPERIMENTAL: " + given
}

func newWeightedHistogram(desc *prometheus.Desc, opts WeightedHistogramOpts, variableLabelValues ...string) (*weightedHistogram, error) {
	// 未显式给出桶时沿用 Prometheus 默认桶，保持与标准 Histogram 一致的体验。
	if len(opts.Buckets) == 0 {
		opts.Buckets = prometheus.DefBuckets
	}

	for i, upperBound := range opts.Buckets {
		if i < len(opts.Buckets)-1 {
			// 桶上界必须严格递增，否则 sort.SearchFloat64s 的行为无定义。
			if upperBound >= opts.Buckets[i+1] {
				return nil, fmt.Errorf(
					"histogram buckets must be in increasing order: %f >= %f",
					upperBound, opts.Buckets[i+1],
				)
			}
		} else {
			if math.IsInf(upperBound, +1) {
				// +Inf 桶是隐式的，这里将其移除。
				opts.Buckets = opts.Buckets[:i]
			}
		}
	}
	upperBounds := make([]float64, len(opts.Buckets))
	copy(upperBounds, opts.Buckets)

	return &weightedHistogram{
		desc:                desc,
		variableLabelValues: variableLabelValues,
		upperBounds:         upperBounds,
		buckets:             make([]uint64, len(upperBounds)+1),
		hotCount:            initialHotCount,
	}, nil
}

type weightedHistogram struct {
	desc                *prometheus.Desc
	variableLabelValues []string
	upperBounds         []float64 // 不包含 +Inf

	lock sync.Mutex // 保护以下所有字段

	// buckets 比 upperBounds 多一个元素。
	// 对于 0 <= idx < len(upperBounds)，buckets[idx] 累计值不超过 upperBounds[idx]，
	// 且大于上一桶上界（第一桶无下界）的观测权重。
	// buckets[len(upperBounds)] 累计未落入其他桶的观测权重。
	buckets []uint64

	// sumHot + sumCold 是 value * weight 的加权总和。
	// 为避免在单个 float64 中累加时丢失精度，采用分层求和：连续增量先加入 sumHot，
	// 再定期比较 sumHot 与 sumCold 的量级；比例足够大时，将 sumHot 并入 sumCold。
	sumHot  float64
	sumCold float64

	transferThreshold float64 // 等于 math.Abs(sumCold) / 2^26，约为 float64 精度位数的一半

	// hotCount 用于决定何时考虑将 sumHot 并入 sumCold。
	// 它从 initialHotCount 向上计数到零。
	hotCount int
}

// initialHotCount 是判断是否将 sumHot 并入 sumCold 前，累加项数的相反数。
// 其绝对值只需大到让额外浮点运算只发生在少数调用中即可。
//
// 每累加 15 次才判断一次是否把 sumHot 并入 sumCold，
// 目的是让高精度合并的开销只出现在少数调用中。
const initialHotCount = -15

var _ WeightedHistogram = &weightedHistogram{}
var _ prometheus.Metric = &weightedHistogram{}
var _ prometheus.Collector = &weightedHistogram{}

// ObserveWithWeight 在锁外完成桶定位（upperBounds 只读），
// 再在锁内更新计数与和，减少锁的持有时间。
func (sh *weightedHistogram) ObserveWithWeight(value float64, weight uint64) {
	idx := sort.SearchFloat64s(sh.upperBounds, value)
	sh.lock.Lock()
	defer sh.lock.Unlock()
	sh.updateLocked(idx, value, weight)
}

// observeWithWeightLocked 供已持锁的调用路径复用，避免重复定位。
func (sh *weightedHistogram) observeWithWeightLocked(value float64, weight uint64) {
	idx := sort.SearchFloat64s(sh.upperBounds, value)
	sh.updateLocked(idx, value, weight)
}

// updateLocked 在持锁状态下累加桶计数与加权和。
// 需要通过 sumHot/sumCold 分层求和来控制 float64 的精度损失。
func (sh *weightedHistogram) updateLocked(idx int, value float64, weight uint64) {
	sh.buckets[idx] += weight
	newSumHot := sh.sumHot + float64(weight)*value
	sh.hotCount++
	if sh.hotCount >= 0 {
		sh.hotCount = initialHotCount
		// 仅当 sumHot 相对 sumCold 足够大（不至于被舍入吞掉）时才合并。
		if math.Abs(newSumHot) > sh.transferThreshold {
			newSumCold := sh.sumCold + newSumHot
			sh.sumCold = newSumCold
			sh.transferThreshold = math.Abs(newSumCold / 67108864)
			sh.sumHot = 0
			return
		}
	}
	sh.sumHot = newSumHot
}

// Desc 实现 prometheus.Metric。
func (sh *weightedHistogram) Desc() *prometheus.Desc {
	return sh.desc
}

// Write 把累计值转换为标准 Histogram 的 dto 表示；
// 快照在锁内完成，避免抓取过程中与写入相互干扰。
func (sh *weightedHistogram) Write(dest *dto.Metric) error {
	count, sum, buckets := func() (uint64, float64, map[float64]uint64) {
		sh.lock.Lock()
		defer sh.lock.Unlock()
		nBounds := len(sh.upperBounds)
		buckets := make(map[float64]uint64, nBounds)
		var count uint64
		for idx, upperBound := range sh.upperBounds {
			count += sh.buckets[idx]
			buckets[upperBound] = count
		}
		count += sh.buckets[nBounds]
		return count, sh.sumHot + sh.sumCold, buckets
	}()
	metric, err := prometheus.NewConstHistogram(sh.desc, count, sum, buckets, sh.variableLabelValues...)
	if err != nil {
		return err
	}
	return metric.Write(dest)
}

// Describe 实现 prometheus.Collector。
func (sh *weightedHistogram) Describe(ch chan<- *prometheus.Desc) {
	ch <- sh.desc
}

// Collect 把自身作为唯一指标发出。
func (sh *weightedHistogram) Collect(ch chan<- prometheus.Metric) {
	ch <- sh
}
