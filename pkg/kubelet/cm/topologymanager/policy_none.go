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

// nonePolicy 是不做任何拓扑约束的策略，任何 hint 都会被接受。
type nonePolicy struct{}

var _ Policy = &nonePolicy{}

// PolicyNone 是 none 策略的名称。
const PolicyNone string = "none"

// NewNonePolicy 返回 none 策略实例。
func NewNonePolicy() Policy {
	return &nonePolicy{}
}

// Name 返回策略名称。
func (p *nonePolicy) Name() string {
	return PolicyNone
}

// canAdmitPodResult 对 none 策略始终返回 true。
func (p *nonePolicy) canAdmitPodResult(hint *TopologyHint) bool {
	return true
}

// Merge 对 none 策略直接返回空 hint 并允许准入。
func (p *nonePolicy) Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	return TopologyHint{}, p.canAdmitPodResult(nil)
}
