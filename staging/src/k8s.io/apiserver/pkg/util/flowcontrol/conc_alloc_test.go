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

package flowcontrol

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// 浮点精度误差
const fpSlack = 1e-10

// TestConcAlloc 用大量随机用例测试 computeConcurrencyAllocation。
//
// 用固定种子跑一万次随机用例：分配算法涉及多段线性规划，手写用例很难覆盖边界，
// 随机化配合不变量断言更可靠；固定种子保证失败可复现。
func TestConcAlloc(t *testing.T) {
	rands := rand.New(rand.NewSource(1234567890))
	for i := 0; i < 10000; i++ {
		test1ConcAlloc(t, rands)
	}
}

// test1ConcAlloc 构造一组随机上限/下限/目标值，然后验证分配结果满足三条不变量：
// 总和等于需求、越界项被夹到边界、未越界项与公平比例成正比。
func test1ConcAlloc(t *testing.T, rands *rand.Rand) {
	probLen := ([]int{0, 1, 2, 3, 4, 6, 9})[rands.Intn(7)]
	classes := make([]allocProblemItem, probLen)
	var lowSum, highSum float64
	var requiredSum int
	var requiredSumF float64
	style := "empty"
	if probLen > 0 {
		// 三种构造风格分别制造"总量由下限决定""由上限决定""由目标值决定"的情形。
		switch rands.Intn(20) {
		case 0:
			style = "bound from below"
			requiredSum = rands.Intn(probLen * 3)
			requiredSumF = float64(requiredSum)
			partition64(rands, probLen, requiredSumF, func(j int, x float64) {
				classes[j].lowerBound = x
				classes[j].target = x + 2*rands.Float64()
				classes[j].upperBound = x + 3*rands.Float64()
				lowSum += classes[j].lowerBound
				highSum += classes[j].upperBound
			})
		case 1:
			style = "bound from above"
			requiredSum = rands.Intn(probLen*3) + 1
			requiredSumF = float64(requiredSum)
			partition64(rands, probLen, requiredSumF, func(j int, x float64) {
				classes[j].upperBound = x
				classes[j].lowerBound = x * math.Max(0, 1.25*rands.Float64()-1)
				classes[j].target = classes[j].lowerBound + rands.Float64()
				lowSum += classes[j].lowerBound
				highSum += classes[j].upperBound
			})
		default:
			style = "not-set-by-bounds"
			for j := 0; j < probLen; j++ {
				x := math.Max(0, rands.Float64()*5-1)
				classes[j].lowerBound = x
				classes[j].target = x + 2*rands.Float64()
				classes[j].upperBound = x + 3*rands.Float64()
				lowSum += classes[j].lowerBound
				highSum += classes[j].upperBound
			}
			requiredSumF = math.Round(float64(lowSum + (highSum-lowSum)*rands.Float64()))
			requiredSum = int(requiredSumF)
		}
	}
	// 以一定概率混入 target 为 0 的类，验证零目标类不会抢走分配份额。
	for rands.Float64() < 0.25 {
		// 添加一个目标为 0 的 category
		classes = append(classes, allocProblemItem{target: 0, upperBound: rands.Float64() + 0.00001})
		highSum += classes[probLen].upperBound
		if probLen > 1 {
			m := rands.Intn(probLen)
			classes[m], classes[probLen] = classes[probLen], classes[m]
		}
		probLen = len(classes)
	}
	allocs, fairProp, err := computeConcurrencyAllocation(requiredSum, classes)
	var actualSumF float64
	for _, item := range allocs {
		actualSumF += item
	}
	// 需求总量落在 [lowSum, highSum] 之外时，算法应当报错而不是硬凑一个解。
	expectErr := lowSum-requiredSumF > fpSlack || requiredSumF-highSum > fpSlack
	if err != nil {
		if expectErr {
			t.Logf("For requiredSum=%v, %s classes=%#+v expected error and got %#+v", requiredSum, style, classes, err)
			return
		}
		t.Fatalf("For requiredSum=%v, %s classes=%#+v got unexpected error %#+v", requiredSum, style, classes, err)
	}
	if expectErr {
		t.Fatalf("Expected error from requiredSum=%v, %s classes=%#+v but got solution %v, %v instead", requiredSum, style, classes, allocs, fairProp)
	}
	rd := f64RelDiff(requiredSumF, actualSumF)
	if rd > fpSlack {
		t.Fatalf("For requiredSum=%v, %s classes=%#+v got solution %v, %v which has sum %v", requiredSum, style, classes, allocs, fairProp, actualSumF)
	}
	// 逐项核对：触及边界的项必须精确等于边界，其余项必须等于 fairProp*target。
	for idx, item := range classes {
		target := math.Max(item.target, MinTarget)
		alloc := fairProp * target
		if alloc <= item.lowerBound {
			if allocs[idx] != item.lowerBound {
				t.Fatalf("For requiredSum=%v, %s classes=%#+v got solution %v, %v in which item %d should be its lower bound but is not", requiredSum, style, classes, allocs, fairProp, idx)
			}
		} else if alloc >= item.upperBound {
			if allocs[idx] != item.upperBound {
				t.Fatalf("For requiredSum=%v, %s classes=%#+v got solution %v, %v in which item %d should be its upper bound but is not", requiredSum, style, classes, allocs, fairProp, idx)
			}
		} else if f64RelDiff(alloc, allocs[idx]) > fpSlack {
			t.Fatalf("For requiredSum=%v, %s classes=%#+v got solution %v, %v in which item %d got alloc %v should be %v (which is proportional to its target) but is not", requiredSum, style, classes, allocs, fairProp, idx, allocs[idx], alloc)
		}
	}
	t.Logf("For requiredSum=%v, %s classes=%#+v got solution %v, %v", requiredSum, style, classes, allocs, fairProp)
}

// partition64 的 consume 被调用 n 次，传入 [0,n) 的整数与和为 x 的浮点数
//
// 通过对 n-1 个随机切点排序得到递增序列，相邻差即为各份额；保证份额之和精确为 x
// （最后一个用 1-last 补齐），从而让随机用例满足总量约束。
func partition64(rands *rand.Rand, n int, x float64, consume func(int, float64)) {
	if n <= 0 {
		return
	}
	divs := make([]float64, n-1)
	for idx := range divs {
		divs[idx] = float64(rands.Float64())
	}
	sort.Float64s(divs)
	var last float64
	for idx, div := range divs {
		div32 := float64(div)
		delta := div32 - last
		consume(idx, delta*x)
		last = div32
	}
	consume(n-1, (1-last)*x)
}

// f64RelDiff 计算相对误差；分母为 0 时返回 0，避免 0/0 产生 NaN 让比较永远失败。
func f64RelDiff(a, b float64) float64 {
	den := math.Max(math.Abs(a), math.Abs(b))
	if den == 0 {
		return 0
	}
	return math.Abs(a-b) / den
}
