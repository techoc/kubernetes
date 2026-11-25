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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Float64Slice 是按绝对值排序的 float64 切片。
//
// 按绝对值排序，使浮点求和结果与顺序无关且可重复（正负抵消对误差影响最小）。
type Float64Slice []float64

// Len 返回切片长度。
func (fs Float64Slice) Len() int { return len(fs) }

// Less 比较绝对值而非原值，减少累加时的相消误差。
func (fs Float64Slice) Less(i, j int) bool { return math.Abs(fs[i]) < math.Abs(fs[j]) }

// Swap 交换两个元素的位置。
func (fs Float64Slice) Swap(i, j int) { fs[i], fs[j] = fs[j], fs[i] }

// weightedHistogramSpecFunc 返回一个 WeightedHistogram 及其预期桶上界。
// 同一函数的每次调用都返回同一个直方图。
type weightedHistogramSpecFunc func() (wh WeightedObserver, upperBounds []float64)

// exerciseWeightedHistograms 测试一组 WeightedHistogram。每个直方图由返回它的函数提供，
// 以便验证 Vec 函数对相同输入返回相同结果。
// 对于有 N 个上界的直方图，测试使用 2N+1 个值：各上界及相邻上界的中点，
// 并在最低上界下方和最高上界上方各扩展一个值。第 J 个值调用 J*m1 次 ObserveWithWeight，
// m1 的选取应使多次触发 sumHot 向 sumCold 的合并判断。
// 各直方图的调用交错执行，以检查彼此之间没有干扰。
//
// 期望值按“值与桶上界的关系”独立计算，再与实测的桶计数、样本数、样本和比较；
// 样本和允许 1e-13 的相对误差（见 issue 120112）。
func exerciseWeightedHistograms(t *testing.T, whSpecs ...weightedHistogramSpecFunc) {
	var whos []weightedHistogramObs
	expectations := []whExerciseExpectation{}
	// 创建 ObserveWithWeight 调用的预期结果与执行计划
	for whIdx, whSpec := range whSpecs {
		wh, upperBounds := whSpec()
		numUBs := len(upperBounds)
		numWhos := numUBs*2 + 1
		multSum := (numWhos * (numWhos + 1)) / 2
		m1 := (-10 * initialHotCount) / multSum
		terms := Float64Slice{}
		ee := whExerciseExpectation{wh: wh,
			upperBounds: upperBounds,
			buckets:     make([]uint64, numUBs),
		}
		addWHOs := func(val float64, weight uint64, mult, idx int) {
			multipliedWeight := weight * uint64(mult)
			terms = append(terms, val*float64(multipliedWeight))
			t.Logf("For WH %d, adding obs val=%v, weight=%v, mult=%d, idx=%d", whIdx, val, weight, mult, idx)
			for i := 0; i < mult; i++ {
				whos = append(whos, weightedHistogramObs{whSpec, val, weight})
			}
			for j := idx; j < numUBs; j++ {
				ee.buckets[j] += multipliedWeight
			}
			ee.count += multipliedWeight
		}
		// 交错调用多次，覆盖 sumHot 向 sumCold 溢出（spill）的路径。
		for idx, ub := range upperBounds {
			var val float64
			if idx > 0 {
				val = (upperBounds[idx-1] + ub) / 2
			} else if numUBs > 1 {
				val = (3*ub - upperBounds[1]) / 2
			} else {
				val = ub - 1
			}
			addWHOs(val, (1 << rand.Intn(40)), (2*idx+1)*m1, idx)
			addWHOs(ub, (1 << rand.Intn(40)), (2*idx+2)*m1, idx)
		}
		val := upperBounds[numUBs-1] + 1
		if numUBs > 1 {
			val = (3*upperBounds[numUBs-1] - upperBounds[numUBs-2]) / 2
		}
		addWHOs(val, 1+uint64(rand.Intn(1000000)), (2*numUBs+1)*m1, numUBs)
		sort.Sort(terms)
		for _, term := range terms {
			ee.sum += term
		}
		t.Logf("At idx=%v, adding expectation of buckets=%#+v, upperBounds=%#+v, sum=%v, count=%v", whIdx, ee.buckets, ee.upperBounds, ee.sum, ee.count)
		expectations = append(expectations, ee)
	}
	// 按随机顺序执行计划中的 ObserveWithWeight 调用
	for len(whos) > 0 {
		var wi weightedHistogramObs
		whos, wi = whosPick(whos)
		wh, _ := wi.whSpec()
		wh.ObserveWithWeight(wi.val, wi.weight)
		// t.Logf("ObserveWithWeight(%v, %v) => %#+v", wi.val, wi.weight, wh)
	}
	// 检查实际结果是否符合预期
	for idx, ee := range expectations {
		wh := ee.wh
		whAsMetric := wh.(prometheus.Metric)
		var metric dto.Metric
		whAsMetric.Write(&metric)
		actualHist := metric.GetHistogram()
		if actualHist == nil {
			t.Errorf("At idx=%d, Write produced nil Histogram", idx)
		}
		actualCount := actualHist.GetSampleCount()
		if actualCount != ee.count {
			t.Errorf("At idx=%d, expected count %v but got %v", idx, ee.count, actualCount)

		}
		actualBuckets := actualHist.GetBucket()
		if len(ee.buckets) != len(actualBuckets) {
			t.Errorf("At idx=%d, expected %v buckets but got %v", idx, len(ee.buckets), len(actualBuckets))
		}
		for j := 0; j < len(ee.buckets) && j < len(actualBuckets); j++ {
			actualUB := actualBuckets[j].GetUpperBound()
			// 桶计数是累积值，因此每次观测都要加到 idx 及其之后的所有桶。
			actualCount := actualBuckets[j].GetCumulativeCount()
			if ee.upperBounds[j] != actualUB {
				t.Errorf("At idx=%d, bucket %d, expected upper bound %v but got %v, err=%v", idx, j, ee.upperBounds[j], actualUB, actualUB-ee.upperBounds[j])
			}
			if ee.buckets[j] != actualCount {
				t.Errorf("At idx=%d, bucket %d expected count %d but got %d", idx, j, ee.buckets[j], actualCount)
			}
		}
		actualSum := actualHist.GetSampleSum()
		num := math.Abs(actualSum - ee.sum)
		den := math.Max(math.Abs(actualSum), math.Abs(ee.sum))
		relErr := num / den
		// issue 120112 报告的最高相对误差为 9.55994394104272e-14
		if relErr > 1e-13 {
			t.Errorf("At idx=%d, expected sum %v but got %v, err=%v, relativeErr=%v", idx, ee.sum, actualSum, actualSum-ee.sum, relErr)
		}
	}
}

// weightedHistogramObs 描述一次 WeightedHistogram.ObserveWithWeight 调用。
type weightedHistogramObs struct {
	whSpec weightedHistogramSpecFunc
	val    float64
	weight uint64
}

// whExerciseExpectation 保存测试 WeightedHistogram 时的预期结果。
type whExerciseExpectation struct {
	wh          WeightedObserver
	upperBounds []float64
	buckets     []uint64
	sum         float64
	count       uint64
}

// whosPick 随机取出一条待执行的观测，用末尾元素填补空位，避免删除导致搬移。
func whosPick(whos []weightedHistogramObs) ([]weightedHistogramObs, weightedHistogramObs) {
	n := len(whos)
	if n < 2 {
		return whos[:0], whos[0]
	}
	idx := rand.Intn(n)
	ans := whos[idx]
	whos[idx] = whos[n-1]
	return whos[:n-1], ans
}

// TestOneWeightedHistogram 覆盖固定桶数与随机桶上界两种场景；
// 随机用例使用递增上界，贴近真实直方图的构造方式。
func TestOneWeightedHistogram(t *testing.T) {
	// 先测试固定字面量用例
	for _, testCase := range []struct {
		name        string
		upperBounds []float64
	}{
		{"one bucket", []float64{0.07}},
		{"two buckets", []float64{0.07, 0.13}},
		{"three buckets", []float64{0.07, 0.13, 1e6}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			wh, err := NewWeightedHistogram(WeightedHistogramOpts{
				Namespace: "testns",
				Subsystem: "testsubsys",
				Name:      "testhist",
				Help:      "Me",
				Buckets:   testCase.upperBounds,
			})
			if err != nil {
				t.Error(err)
			}
			exerciseWeightedHistograms(t, func() (WeightedObserver, []float64) { return wh, testCase.upperBounds })
		})
	}
	// 再测试随机用例
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("random_case_%d", i)
		t.Run(name, func(t *testing.T) {
			nBounds := rand.Intn(10) + 1
			ubs := []float64{}
			var bound float64
			for j := 0; j < nBounds; j++ {
				bound += rand.Float64()
				ubs = append(ubs, bound)
			}
			wh, err := NewWeightedHistogram(WeightedHistogramOpts{
				Namespace:   "testns",
				Subsystem:   "testsubsys",
				Name:        name,
				Help:        "Me",
				Buckets:     ubs,
				ConstLabels: prometheus.Labels{"k0": "v0"},
			})
			if err != nil {
				t.Error(err)
			}
			exerciseWeightedHistograms(t, func() (WeightedObserver, []float64) { return wh, ubs })
		})
	}
}

// TestWeightedHistogramVec 对两个使用不同标签集的向量交错观测，
// 验证 WithLabelValues 与 With 返回的序列互不干扰。
func TestWeightedHistogramVec(t *testing.T) {
	ubs1 := []float64{0.07, 1.3, 1e6}
	vec1 := NewWeightedHistogramVec(WeightedHistogramOpts{
		Namespace:   "testns",
		Subsystem:   "testsubsys",
		Name:        "vec1",
		Help:        "Me",
		Buckets:     ubs1,
		ConstLabels: prometheus.Labels{"k0": "v0"},
	}, "k1", "k2")
	gen1 := func(lvs ...string) func() (WeightedObserver, []float64) {
		return func() (WeightedObserver, []float64) { return vec1.WithLabelValues(lvs...), ubs1 }
	}
	ubs2 := []float64{-0.03, 0.71, 1e9}
	vec2 := NewWeightedHistogramVec(WeightedHistogramOpts{
		Namespace:   "testns",
		Subsystem:   "testsubsys",
		Name:        "vec2",
		Help:        "Me",
		Buckets:     ubs2,
		ConstLabels: prometheus.Labels{"j0": "u0"},
	}, "j1", "j2")
	gen2 := func(lvs ...string) func() (WeightedObserver, []float64) {
		varLabels := prometheus.Labels{}
		varLabels["j1"] = lvs[0]
		varLabels["j2"] = lvs[1]
		return func() (WeightedObserver, []float64) { return vec2.With(varLabels), ubs2 }
	}
	exerciseWeightedHistograms(t,
		gen1("v11", "v21"),
		gen1("v12", "v21"),
		gen1("v12", "v22"),
		gen2("a", "b"),
		gen2("a", "c"),
		gen2("b", "c"),
	)
}

// BenchmarkWeightedHistogram 测量带权观测的写入开销。
func BenchmarkWeightedHistogram(b *testing.B) {
	b.StopTimer()
	wh, err := NewWeightedHistogram(WeightedHistogramOpts{
		Namespace: "testns",
		Subsystem: "testsubsys",
		Name:      "testhist",
		Help:      "Me",
		Buckets:   []float64{1, 2, 4, 8, 16},
	})
	if err != nil {
		b.Error(err)
	}
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		wh.ObserveWithWeight(float64(x), uint64(i)%32+1)
		x = (x + i) % 20
	}
}

// BenchmarkHistogram 是对照组：标准 prometheus 直方图的写入开销，
// 用于评估 WeightedHistogram 的额外成本。
func BenchmarkHistogram(b *testing.B) {
	b.StopTimer()
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "testns",
		Subsystem: "testsubsys",
		Name:      "testhist",
		Help:      "Me",
		Buckets:   []float64{1, 2, 4, 8, 16},
	})
	var x int
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		hist.Observe(float64(x))
		x = (x + i) % 20
	}
}
