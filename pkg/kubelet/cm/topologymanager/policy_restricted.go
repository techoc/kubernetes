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

// restrictedPolicy 要求最佳 hint 必须是 preferred 才允许准入。
type restrictedPolicy struct {
	bestEffortPolicy
}

var _ Policy = &restrictedPolicy{}

// PolicyRestricted 是 restricted 策略的名称。
const PolicyRestricted string = "restricted"

// NewRestrictedPolicy 返回 restricted 策略实例。
func NewRestrictedPolicy(numaInfo *NUMAInfo, opts PolicyOptions) Policy {
	return &restrictedPolicy{bestEffortPolicy{numaInfo: numaInfo, opts: opts}}
}

// Name 返回策略名称。
func (p *restrictedPolicy) Name() string {
	return PolicyRestricted
}

// canAdmitPodResult 仅在 hint 为 preferred 时允许准入。
func (p *restrictedPolicy) canAdmitPodResult(hint *TopologyHint) bool {
	return hint.Preferred
}

// Merge 过滤各 provider 的 hint，合并后按 restricted 规则决定是否准入。
func (p *restrictedPolicy) Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(logger, providersHints)
	merger := NewHintMerger(p.numaInfo, filteredHints, p.Name(), p.opts)
	bestHint := merger.Merge()
	admit := p.canAdmitPodResult(&bestHint)
	return bestHint, admit
}
