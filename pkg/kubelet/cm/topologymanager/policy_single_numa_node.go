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

import "k8s.io/klog/v2"

// singleNumaNodePolicy 要求最终 hint 落在单个 NUMA 节点上。
type singleNumaNodePolicy struct {
	// numaInfo 描述底层机器上可用的 NUMA 节点列表及其相互距离。
	numaInfo *NUMAInfo
	opts     PolicyOptions
}

var _ Policy = &singleNumaNodePolicy{}

// PolicySingleNumaNode 是 single-numa-node 策略的名称。
const PolicySingleNumaNode string = "single-numa-node"

// NewSingleNumaNodePolicy 返回 single-numa-node 策略实例。
func NewSingleNumaNodePolicy(numaInfo *NUMAInfo, opts PolicyOptions) Policy {
	return &singleNumaNodePolicy{numaInfo: numaInfo, opts: opts}
}

// Name 返回策略名称。
func (p *singleNumaNodePolicy) Name() string {
	return PolicySingleNumaNode
}

// canAdmitPodResult 仅在 hint 为 preferred 时允许准入。
func (p *singleNumaNodePolicy) canAdmitPodResult(hint *TopologyHint) bool {
	return hint.Preferred
}

// filterSingleNumaHints 只保留 preferred 的无拓扑偏好 hint 与恰好一个 NUMA 节点的 hint。
func filterSingleNumaHints(allResourcesHints [][]TopologyHint) [][]TopologyHint {
	var filteredResourcesHints [][]TopologyHint
	for _, oneResourceHints := range allResourcesHints {
		var filtered []TopologyHint
		for _, hint := range oneResourceHints {
			if hint.NUMANodeAffinity == nil && hint.Preferred {
				filtered = append(filtered, hint)
			}
			if hint.NUMANodeAffinity != nil && hint.NUMANodeAffinity.Count() == 1 && hint.Preferred {
				filtered = append(filtered, hint)
			}
		}
		filteredResourcesHints = append(filteredResourcesHints, filtered)
	}
	return filteredResourcesHints
}

// Merge 只保留单 NUMA 节点与无拓扑偏好的 hint，再合并并决定是否准入。
func (p *singleNumaNodePolicy) Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(logger, providersHints)
	// 只保留无拓扑偏好与单 NUMA 节点的 hint。
	singleNumaHints := filterSingleNumaHints(filteredHints)

	merger := NewHintMerger(p.numaInfo, singleNumaHints, p.Name(), p.opts)
	bestHint := merger.Merge()

	if bestHint.NUMANodeAffinity.IsEqual(p.numaInfo.DefaultAffinityMask()) {
		bestHint = TopologyHint{nil, bestHint.Preferred}
	}

	admit := p.canAdmitPodResult(&bestHint)
	return bestHint, admit
}
