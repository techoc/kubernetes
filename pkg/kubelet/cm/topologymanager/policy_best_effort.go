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

// bestEffortPolicy 不强制 preferred hint，任何合并结果都可准入。
type bestEffortPolicy struct {
	// numaInfo 描述底层机器上可用的 NUMA 节点列表及其相互距离。
	numaInfo *NUMAInfo
	opts     PolicyOptions
}

var _ Policy = &bestEffortPolicy{}

// PolicyBestEffort 是 best-effort 策略的名称。
const PolicyBestEffort string = "best-effort"

// NewBestEffortPolicy 返回 best-effort 策略实例。
func NewBestEffortPolicy(numaInfo *NUMAInfo, opts PolicyOptions) Policy {
	return &bestEffortPolicy{numaInfo: numaInfo, opts: opts}
}

// Name 返回策略名称。
func (p *bestEffortPolicy) Name() string {
	return PolicyBestEffort
}

// canAdmitPodResult 对 best-effort 策略始终返回 true。
func (p *bestEffortPolicy) canAdmitPodResult(hint *TopologyHint) bool {
	return true
}

// Merge 过滤各 provider 的 hint 并合并，best-effort 下总能准入。
func (p *bestEffortPolicy) Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(logger, providersHints)
	merger := NewHintMerger(p.numaInfo, filteredHints, p.Name(), p.opts)
	bestHint := merger.Merge()
	admit := p.canAdmitPodResult(&bestHint)
	return bestHint, admit
}
