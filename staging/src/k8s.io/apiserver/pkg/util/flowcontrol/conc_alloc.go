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
	"errors"
	"fmt"
	"math"
	"sort"
)

// allocProblemItem 是 computeConcurrencyAllocation 需要为其分配并发额度的一个类别。
type allocProblemItem struct {
	target     float64
	lowerBound float64
	upperBound float64
}

// relativeAllocItem 与 allocProblemItem 类似，但 target 避免为零，
// 且上下界都除以 target，使比较可以统一按“公平比例”进行。
type relativeAllocItem struct {
	target             float64
	relativeLowerBound float64
	relativeUpperBound float64
}

// relativeAllocProblem 汇总所有类别，并保存按界升序排序的结果。
// 对 J <= K，ascendingIndices[J] 指向的界不大于 ascendingIndices[K] 指向的界。
// ascendingIndices[J] = 2*N + 0 表示 items[N] 的下界；
// ascendingIndices[J] = 2*N + 1 表示 items[N] 的上界。
type relativeAllocProblem struct {
	items            []relativeAllocItem
	ascendingIndices []int
}

// initIndices 填充并排序 ascendingIndices。
func (rap *relativeAllocProblem) initIndices() *relativeAllocProblem {
	rap.ascendingIndices = make([]int, len(rap.items)*2)
	for idx := 0; idx < len(rap.ascendingIndices); idx++ {
		rap.ascendingIndices[idx] = idx
	}
	sort.Sort(rap)
	return rap
}

// getItemIndex 把 packed index 拆成类别下标，并报告它是否为下界。
func (rap *relativeAllocProblem) getItemIndex(idx int) (int, bool) {
	packedIndex := rap.ascendingIndices[idx]
	itemIndex := packedIndex / 2
	return itemIndex, packedIndex == itemIndex*2
}

// decode 返回 ascendingIndices[J] 关联的界值、类别下标，
// 以及该界是否为该类别下界的布尔值。
func (rap *relativeAllocProblem) decode(idx int) (float64, int, bool) {
	itemIdx, lower := rap.getItemIndex(idx)
	if lower {
		return rap.items[itemIdx].relativeLowerBound, itemIdx, lower
	}
	return rap.items[itemIdx].relativeUpperBound, itemIdx, lower
}

// getProportion 返回第 idx 个排序位置上的界值。
func (rap *relativeAllocProblem) getProportion(idx int) float64 {
	prop, _, _ := rap.decode(idx)
	return prop
}

// Len 实现 sort.Interface：每个类别有两个界，因此长度是类别的两倍。
func (rap *relativeAllocProblem) Len() int { return len(rap.items) * 2 }

// Less 按界值升序比较。
func (rap *relativeAllocProblem) Less(i, j int) bool {
	return rap.getProportion(i) < rap.getProportion(j)
}

// Swap 交换排序索引，从而间接对界排序而不移动 items。
func (rap *relativeAllocProblem) Swap(i, j int) {
	rap.ascendingIndices[i], rap.ascendingIndices[j] = rap.ascendingIndices[j], rap.ascendingIndices[i]
}

// minMax 记录扫描一组数值时看到的最小值与最大值。
type minMax struct {
	min float64
	max float64
}

// note 纳入一个新的数值。
func (mm *minMax) note(x float64) {
	mm.min = math.Min(mm.min, x)
	mm.max = math.Max(mm.max, x)
}

// MinTarget 是 target 的下限：更小的 target 会被提升到该值，避免除以零。
const MinTarget = 0.001

// epsilon 是浮点比较的容差。
const epsilon = 0.0000001

// computeConcurrencyAllocation 返回唯一的一组 `allocs []float64` 及关联的 `fairProp float64`，
// 在浮点精度允许的范围内同时满足以下性质（无法满足时返回解释原因的错误）：
// `allocs` 之和等于 `requiredSum`。
// 对 [0, len(classes)) 中的每个 J：
//  1. `classes[J].lowerBound <= allocs[J] <= classes[J].upperBound`，且
//  2. 下列之一恰好成立：
//     2a. `allocs[J] == fairProp * classes[J].target`，
//     2b. `allocs[J] == classes[J].lowerBound && classes[J].lowerBound > fairProp * classes[J].target`，或
//     2c. `allocs[J] == classes[J].upperBound && classes[J].upperBound < fairProp * classes[J].target`。
//
// 每个 allocProblemItem 必须满足 `target >= lowerBound >= 0` 且 `upperBound >= lowerBound`。
// 小于 MinTarget 的 target 会被当作 MinTarget 处理。
//
// 直观理解：先按 target 比例分配；触及上下界的类别被钉在界上，
// 剩余额度再在未触界的类别之间按比例重新分配。
func computeConcurrencyAllocation(requiredSum int, classes []allocProblemItem) ([]float64, float64, error) {
	if requiredSum < 0 {
		return nil, 0, errors.New("negative sums are not supported")
	}
	requiredSumF := float64(requiredSum)
	var lowSum, highSum, targetSum float64
	// 以 MaxFloat32 初始化，保证第一次 note 就能更新为真实值。
	ubRange := minMax{min: float64(math.MaxFloat32)}
	lbRange := minMax{min: float64(math.MaxFloat32)}
	relativeItems := make([]relativeAllocItem, len(classes))
	for idx, item := range classes {
		target := item.target
		if item.lowerBound < 0 {
			return nil, 0, fmt.Errorf("lower bound %d is %v but negative lower bounds are not allowed", idx, item.lowerBound)
		}
		if target < item.lowerBound {
			return nil, 0, fmt.Errorf("target %d is %v, which is below its lower bound of %v", idx, target, item.lowerBound)
		}
		if item.upperBound < item.lowerBound {
			return nil, 0, fmt.Errorf("upper bound %d is %v but should not be less than the lower bound %v", idx, item.upperBound, item.lowerBound)
		}
		if target < MinTarget {
			// 调整为非零值，避免后续除以零
			target = MinTarget
		}
		lowSum += item.lowerBound
		highSum += item.upperBound
		targetSum += target
		relativeItem := relativeAllocItem{
			target:             target,
			relativeLowerBound: item.lowerBound / target,
			relativeUpperBound: item.upperBound / target,
		}
		ubRange.note(relativeItem.relativeUpperBound)
		lbRange.note(relativeItem.relativeLowerBound)
		relativeItems[idx] = relativeItem
	}
	// 每个类别的下界不得超过其 target，否则相对下界会大于 1，问题无解。
	if lbRange.max > 1 {
		return nil, 0, fmt.Errorf("lbRange.max-1=%v, which is impossible because lbRange.max can not be greater than 1", lbRange.max-1)
	}
	// 所有下界之和已超过所需总量：无解。
	if lowSum-requiredSumF > epsilon {
		return nil, 0, fmt.Errorf("lower bounds sum to %v, which is higher than the required sum of %v", lowSum, requiredSum)
	}
	// 所有上界之和仍低于所需总量：无解。
	if requiredSumF-highSum > epsilon {
		return nil, 0, fmt.Errorf("upper bounds sum to %v, which is lower than the required sum of %v", highSum, requiredSum)
	}
	ans := make([]float64, len(classes))
	if requiredSum == 0 {
		return ans, 0, nil
	}
	// 下界之和恰好等于所需总量：没有腾挪空间，所有类别取下界。
	if lowSum-requiredSumF > -epsilon {
		for idx, item := range classes {
			ans[idx] = item.lowerBound
		}
		return ans, lbRange.min, nil
	}
	// 上界之和恰好等于所需总量：所有类别取上界。
	if requiredSumF-highSum > -epsilon {
		for idx, item := range classes {
			ans[idx] = item.upperBound
		}
		return ans, ubRange.max, nil
	}
	// 此时解是 [lbRange.min, ubRange.max] 内唯一的 fairProp。
	// 先看解是否不触及任何界。
	fairProp := requiredSumF / targetSum
	if lbRange.max <= fairProp && fairProp <= ubRange.min { // 没有界生效
		for idx := range classes {
			ans[idx] = relativeItems[idx].target * fairProp
		}
		return ans, fairProp, nil
	}
	// 有界生效。
	// 通过对界排序，从 lbRange.min 开始逐步提高 fairProp 来求解释。
	rap := (&relativeAllocProblem{items: relativeItems}).initIndices()
	sumSoFar := lowSum
	fairProp = lbRange.min
	var sensitiveTargetSum, deltaSensitiveTargetSum float64
	var numSensitiveClasses, deltaSensitiveClasses int
	var nextIdx int
	// `nextIdx` 是下一个要考察的 `rap` 下标。
	// `sumSoFar` 是若当前 fairProp 即为解时各 alloc 之和；
	// 若当前 fairProp 就是答案，则 `sumSoFar == requiredSum`。
	// 否则 fairProp 的下一次增量会改变 `numSensitiveClasses` 个类别的分配，
	// 这些类别的 target 之和为 `sensitiveTargetSum`；其余类别已有界生效且继续生效。
	// nextIdx 的最近一次推进要求把 `deltaSensitiveClasses` 加到 `numSensitiveClasses`，
	// 把 `deltaSensitiveTargetSum` 加到 `sensitiveTargetSum`。
	for sumSoFar < requiredSumF {
		// 可能有多个界等于当前 fairProp；全部找出来，因为它们都影响下一次 fairProp 变化。
		// 把 nextBound 设为下一个不等于 fairProp 的界，并把 nextIdx 推进到该界。
		var nextBound float64
		for {
			sensitiveTargetSum += deltaSensitiveTargetSum
			numSensitiveClasses += deltaSensitiveClasses
			if nextIdx >= rap.Len() {
				return nil, 0, fmt.Errorf("impossible: ran out of bounds to consider in bound-constrained problem")
			}
			var itemIdx int
			var lower bool
			nextBound, itemIdx, lower = rap.decode(nextIdx)
			// 下界被触及后该类别进入敏感集合；上界被触及后离开敏感集合。
			if lower {
				deltaSensitiveClasses = 1
				deltaSensitiveTargetSum = rap.items[itemIdx].target
			} else {
				deltaSensitiveClasses = -1
				deltaSensitiveTargetSum = -rap.items[itemIdx].target
			}
			nextIdx++
			if nextBound > fairProp {
				break
			}
		}
		// fairProp 可以增大到 nextBound 而不经过任何中间界。
		if numSensitiveClasses == 0 {
			// 下一段 fairProp 区间没有类别受影响，直接跳过
			fairProp = nextBound
			continue
		}
		// 看 fairProp 能否在到达下一个界之前就满足总和。
		deltaFairProp := (requiredSumF - sumSoFar) / sensitiveTargetSum
		nextProp := fairProp + deltaFairProp
		if nextProp <= nextBound {
			fairProp = nextProp
			break
		}
		// 不能，fairProp 必须超过 nextBound
		sumSoFar += (nextBound - fairProp) * sensitiveTargetSum
		fairProp = nextBound
	}
	// 最终分配 = 按 fairProp 比例分配，再夹到各自的上下界内。
	for idx, item := range classes {
		ans[idx] = math.Max(item.lowerBound, math.Min(item.upperBound, fairProp*relativeItems[idx].target))
	}
	return ans, fairProp, nil
}
