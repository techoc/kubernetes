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

package topologymanager

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

// Policy 是 Topology Manager 的准入策略接口。
type Policy interface {
	// 返回策略名称。
	Name() string
	// 根据各 hint provider 的输入合并出 TopologyHint，
	// 并给出基于提示和策略类型的准入结果。
	Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool)
}

// IsAlignmentGuaranteed 在策略保证“计算资源要么分配在同一 NUMA 边界内、要么直接失败”时返回 true。
func IsAlignmentGuaranteed(p Policy) bool {
	// 目前该判断几乎与策略名一一对应，暂不新增字段，直接复用名称。
	return p.Name() == PolicySingleNumaNode
}

// mergePermutation 对一个提示排列做亲和性掩码按位与，合并成单个提示；
// 排列中所有提示都为首选时，结果才是首选。
func mergePermutation(defaultAffinity bitmask.BitMask, permutation []TopologyHint) TopologyHint {
	// 收集排列中每个提示的 NUMA 亲和性，同时判断是否存在非首选分配。
	preferred := true
	var numaAffinities []bitmask.BitMask
	for _, hint := range permutation {
		// 只处理真正设置了 NUMA 亲和性的提示。
		if hint.NUMANodeAffinity != nil {
			numaAffinities = append(numaAffinities, hint.NUMANodeAffinity)
			// 所有亲和性掩码一致时才算首选。
			if !hint.NUMANodeAffinity.IsEqual(numaAffinities[0]) {
				preferred = false
			}
		}
		// 任一提示非首选，合并结果就非首选。
		if !hint.Preferred {
			preferred = false
		}
	}

	// 用按位与合并所有亲和性掩码，得到各 provider 的共同可用 NUMA 集合。
	mergedAffinity := bitmask.And(defaultAffinity, numaAffinities...)
	// 用合并后的掩码和上面的首选判断构造结果。
	return TopologyHint{mergedAffinity, preferred}
}

// filterProvidersHints 把各 provider 的提示整理成二维列表；
// 没有拓扑偏好的 provider 用 (nil, true) 占位，表示任意 NUMA 均可。
func filterProvidersHints(logger klog.Logger, providersHints []map[string][]TopologyHint) [][]TopologyHint {
	// 遍历所有 hint provider 并累积其提示；provider 未提供提示时，
	// 视为对拓扑感知分配没有偏好。
	var allProviderHints [][]TopologyHint
	for _, hints := range providersHints {
		// hints 为 nil 时插入一个首选的“任意 NUMA”提示。
		if len(hints) == 0 {
			logger.Info("Hint Provider has no preference for NUMA affinity with any resource")
			allProviderHints = append(allProviderHints, []TopologyHint{{nil, true}})
			continue
		}

		// 否则按资源类型逐个累积提示。
		for resource := range hints {
			if hints[resource] == nil {
				logger.Info("Hint Provider has no preference for NUMA affinity with resource", "resource", resource)
				allProviderHints = append(allProviderHints, []TopologyHint{{nil, true}})
				continue
			}

			if len(hints[resource]) == 0 {
				logger.Info("Hint Provider has no possible NUMA affinities for resource", "resource", resource)
				allProviderHints = append(allProviderHints, []TopologyHint{{nil, false}})
				continue
			}

			allProviderHints = append(allProviderHints, hints[resource])
		}
	}
	return allProviderHints
}

// narrowestHint 返回亲和性掩码最窄（NUMA 数量最少）的非 nil 提示。
func narrowestHint(hints []TopologyHint) *TopologyHint {
	if len(hints) == 0 {
		return nil
	}
	var narrowestHint *TopologyHint
	for i := range hints {
		if hints[i].NUMANodeAffinity == nil {
			continue
		}
		if narrowestHint == nil {
			narrowestHint = &hints[i]
		}
		if hints[i].NUMANodeAffinity.IsNarrowerThan(narrowestHint.NUMANodeAffinity) {
			narrowestHint = &hints[i]
		}
	}
	return narrowestHint
}

// maxOfMinAffinityCounts 取各 provider 最窄提示中亲和性数量的最大值，
// 作为比较非首选提示时的目标基准。
func maxOfMinAffinityCounts(filteredHints [][]TopologyHint) int {
	maxOfMinCount := 0
	for _, resourceHints := range filteredHints {
		narrowestHint := narrowestHint(resourceHints)
		if narrowestHint == nil {
			continue
		}
		if narrowestHint.NUMANodeAffinity.Count() > maxOfMinCount {
			maxOfMinCount = narrowestHint.NUMANodeAffinity.Count()
		}
	}
	return maxOfMinCount
}

// HintMerger 把多个 provider 的拓扑提示合并为最终结果。
type HintMerger struct {
	NUMAInfo *NUMAInfo
	Hints    [][]TopologyHint
	// BestNonPreferredAffinityCount 用于在非首选提示中做选择：取所有 provider
	// 最窄亲和性数量的最大值。也就是说，优先选择覆盖“需求最宽的 provider”
	// 所需全部 NUMA 节点的掩码。
	BestNonPreferredAffinityCount int
	CompareNUMAAffinityMasks      func(candidate *TopologyHint, current *TopologyHint) (best *TopologyHint)
}

// NewHintMerger 构造合并器，并注入按策略选择的亲和性比较函数。
func NewHintMerger(numaInfo *NUMAInfo, hints [][]TopologyHint, policyName string, opts PolicyOptions) HintMerger {
	compareNumaAffinityMasks := func(current, candidate *TopologyHint) *TopologyHint {
		// 掩码相同时保留当前提示，避免无意义的替换。
		if candidate.NUMANodeAffinity.IsEqual(current.NUMANodeAffinity) {
			return current
		}

		// 否则按策略选项比较：single-numa-node 必须取最窄；
		// 其他策略在开启 PreferClosestNUMA 时优先取最近的 NUMA。
		var best bitmask.BitMask
		if (policyName != PolicySingleNumaNode) && opts.PreferClosestNUMA {
			best = numaInfo.Closest(current.NUMANodeAffinity, candidate.NUMANodeAffinity)
		} else {
			best = numaInfo.Narrowest(current.NUMANodeAffinity, candidate.NUMANodeAffinity)
		}
		if best.IsEqual(current.NUMANodeAffinity) {
			return current
		}
		return candidate
	}

	// 非首选提示的比较基准在构造时一次算好，避免每次比较重复计算。
	merger := HintMerger{
		NUMAInfo:                      numaInfo,
		Hints:                         hints,
		BestNonPreferredAffinityCount: maxOfMinAffinityCounts(hints),
		CompareNUMAAffinityMasks:      compareNumaAffinityMasks,
	}

	return merger
}

// compare 在当前最优提示和候选提示之间做选择，是合并逻辑的核心。
func (m HintMerger) compare(current *TopologyHint, candidate *TopologyHint) *TopologyHint {
	// 只有亲和性数量大于 0 的候选才有资格替换当前最优提示。
	if candidate.NUMANodeAffinity.Count() == 0 {
		return current
	}

	// 还没有最优提示时，候选直接成为最优提示。
	if current == nil {
		return candidate
	}

	// 首选提示永远优先于非首选提示。
	if !current.Preferred && candidate.Preferred {
		return candidate
	}

	// 当前提示已首选而候选非首选时，无论掩码如何都不替换。
	if current.Preferred && !candidate.Preferred {
		return current
	}

	// 两者都是首选时，只比较亲和性掩码的优劣。
	if current.Preferred && candidate.Preferred {
		return m.CompareNUMAAffinityMasks(current, candidate)

	}

	// 剩余情况是两者都非首选：希望亲和性数量尽量接近但不大于
	// BestNonPreferredAffinityCount，按当前值分三种情况：
	// 1. 当前值大于目标值：换成更合适的候选（比较掩码）；
	// 2. 当前值等于目标值：只有候选也等于目标值且更合适时才替换；
	// 3. 当前值小于目标值：候选超过目标值则保留当前值；
	//    候选等于目标值则立即采用；两者都低于目标值时选择更接近目标值的，
	//    数量相等时再比较亲和性掩码。

	// Case 1
	if current.NUMANodeAffinity.Count() > m.BestNonPreferredAffinityCount {
		return m.CompareNUMAAffinityMasks(current, candidate)
	}
	// Case 2
	if current.NUMANodeAffinity.Count() == m.BestNonPreferredAffinityCount {
		if candidate.NUMANodeAffinity.Count() != m.BestNonPreferredAffinityCount {
			return current
		}
		return m.CompareNUMAAffinityMasks(current, candidate)
	}
	// Case 3a
	if candidate.NUMANodeAffinity.Count() > m.BestNonPreferredAffinityCount {
		return current
	}
	// Case 3b
	if candidate.NUMANodeAffinity.Count() == m.BestNonPreferredAffinityCount {
		return candidate
	}

	// Case 3ca
	if candidate.NUMANodeAffinity.Count() > current.NUMANodeAffinity.Count() {
		return candidate
	}
	// Case 3cb
	if candidate.NUMANodeAffinity.Count() < current.NUMANodeAffinity.Count() {
		return current
	}

	// Case 3cc
	return m.CompareNUMAAffinityMasks(current, candidate)

}

// Merge 遍历所有提示排列，逐一合并并与当前最优结果比较。
func (m HintMerger) Merge() TopologyHint {
	defaultAffinity := m.NUMAInfo.DefaultAffinityMask()

	var bestHint *TopologyHint
	iterateAllProviderTopologyHints(m.Hints, func(permutation []TopologyHint) {
		// 把当前排列合并成一个提示，并判断是否为非首选分配。
		mergedHint := mergePermutation(defaultAffinity, permutation)

		// 候选与当前最优比较，必要时更新最优提示。
		bestHint = m.compare(bestHint, &mergedHint)
	})

	// 所有排列都无法产生有效提示时，退化为默认亲和性的非首选结果。
	if bestHint == nil {
		bestHint = &TopologyHint{defaultAffinity, false}
	}

	return *bestHint
}

// 遍历 allProviderHints 中所有提示的组合。
// 递归实现：对 allProviderHints[i] 的每个提示向下展开，
// 每形成一个完整排列就调用一次 callback。等价于多重循环：
//
// for i := 0; i < len(providerHints[0]); i++
//
//	for j := 0; j < len(providerHints[1]); j++
//	    for k := 0; k < len(providerHints[2]); k++
//	        ...
//	        for z := 0; z < len(providerHints[-1]); z++
//	            permutation := []TopologyHint{
//	                providerHints[0][i],
//	                providerHints[1][j],
//	                providerHints[2][k],
//	                ...
//	                providerHints[-1][z]
//	            }
//	            callback(permutation)
func iterateAllProviderTopologyHints(allProviderHints [][]TopologyHint, callback func([]TopologyHint)) {
	// 内部递归辅助函数，负责累积当前排列。
	var iterate func(i int, accum []TopologyHint)
	iterate = func(i int, accum []TopologyHint) {
		// 递归终止：所有 provider 都已选择，得到一个完整排列。
		if i == len(allProviderHints) {
			callback(accum)
			return
		}

		// 遍历 provider i 的所有提示，递归拼接后续 provider 的提示。
		for j := range allProviderHints[i] {
			iterate(i+1, append(accum, allProviderHints[i][j]))
		}
	}
	iterate(0, []TopologyHint{})
}
