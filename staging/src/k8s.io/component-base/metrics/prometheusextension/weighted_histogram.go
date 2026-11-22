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

// WeightedHistogram generalizes Histogram: each observation has
// an associated _weight_. For a given `x` and `N`,
// `1` call on `ObserveWithWeight(x, N)` has the same meaning as
// `N` calls on `ObserveWithWeight(x, 1)`.
// The weighted sum might differ slightly due to the use of
// floating point, although the implementation takes some steps
// to mitigate that.
// If every weight were 1,
// this would be the same as the existing Histogram abstraction.
//
// WeightedHistogram 在普通 Histogram 上加了一个权重：一次
// ObserveWithWeight(x, N) 等价于 N 次 ObserveWithWeight(x, 1)。
// 所有观测权重都为 1 时，语义与标准 Histogram 一致。
type WeightedHistogram interface {
	prometheus.Metric
	prometheus.Collector
	WeightedObserver
}

// WeightedObserver generalizes the Observer interface.
type WeightedObserver interface {
	// Set the variable to the given value with the given weight.
	ObserveWithWeight(value float64, weight uint64)
}

// WeightedHistogramOpts is the same as for an ordinary Histogram
//
// 直接复用标准 HistogramOpts，避免维护一份会漂移的平行配置。
type WeightedHistogramOpts = prometheus.HistogramOpts

// NewWeightedHistogram creates a new WeightedHistogram
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
				// The +Inf bucket is implicit. Remove it here.
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
	upperBounds         []float64 // exclusive of +Inf

	lock sync.Mutex // applies to all the following

	// buckets is longer by one than upperBounds.
	// For 0 <= idx < len(upperBounds), buckets[idx] holds the
	// accumulated time.Duration that value has been <=
	// upperBounds[idx] but not <= upperBounds[idx-1].
	// buckets[len(upperBounds)] holds the accumulated
	// time.Duration when value fit in no other bucket.
	buckets []uint64

	// sumHot + sumCold is the weighted sum of value.
	// Rather than risk loss of precision in one
	// float64, we do this sum hierarchically.  Many successive
	// increments are added into sumHot; once in a while
	// the magnitude of sumHot is compared to the magnitude
	// of sumCold and, if the ratio is high enough,
	// sumHot is transferred into sumCold.
	sumHot  float64
	sumCold float64

	transferThreshold float64 // = math.Abs(sumCold) / 2^26 (that's about half of the bits of precision in a float64)

	// hotCount is used to decide when to consider dumping sumHot into sumCold.
	// hotCount counts upward from initialHotCount to zero.
	hotCount int
}

// initialHotCount is the negative of the number of terms
// that are summed into sumHot before considering whether
// to transfer to sumCold.  This only has to be big enough
// to make the extra floating point operations occur in a
// distinct minority of cases.
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
