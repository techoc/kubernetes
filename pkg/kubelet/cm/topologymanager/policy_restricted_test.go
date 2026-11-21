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
	"testing"
)

// TestPolicyRestrictedName 验证 restricted 策略的名称固定为 "restricted"。
func TestPolicyRestrictedName(t *testing.T) {
	tcases := []struct {
		name     string
		expected string
	}{
		{
			name:     "New Restricted Policy",
			expected: "restricted",
		},
	}
	// 使用两节点 NUMA 拓扑构造策略。
	numaInfo := commonNUMAInfoTwoNodes()
	for _, tc := range tcases {
		policy := &restrictedPolicy{bestEffortPolicy{numaInfo: numaInfo, opts: PolicyOptions{}}}
		if policy.Name() != tc.expected {
			t.Errorf("Expected Policy Name to be %s, got %s", tc.expected, policy.Name())
		}
	}
}

// TestPolicyRestrictedCanAdmitPodResult 验证 restricted 策略仅在所有提示均为 Preferred 时准入。
func TestPolicyRestrictedCanAdmitPodResult(t *testing.T) {
	tcases := []struct {
		name     string
		hint     TopologyHint
		expected bool
	}{
		{
			name:     "Preferred is set to false in topology hints",
			hint:     TopologyHint{nil, false},
			expected: false,
		},
		{
			name:     "Preferred is set to true in topology hints",
			hint:     TopologyHint{nil, true},
			expected: true,
		},
	}

	for _, tc := range tcases {
		// 使用两节点 NUMA 拓扑构造策略。
		numaInfo := commonNUMAInfoTwoNodes()
		policy := &restrictedPolicy{bestEffortPolicy{numaInfo: numaInfo}}
		result := policy.canAdmitPodResult(&tc.hint)

		if result != tc.expected {
			t.Errorf("Expected result to be %t, got %t", tc.expected, result)
		}
	}
}

// TestPolicyRestrictedMerge 验证 restricted 策略的提示合并逻辑。
func TestPolicyRestrictedMerge(t *testing.T) {
	numaInfo := commonNUMAInfoFourNodes()
	policy := &restrictedPolicy{bestEffortPolicy{numaInfo: numaInfo}}

	// 合并通用用例与 restricted 策略特有的用例。
	tcases := commonPolicyMergeTestCases(numaInfo.Nodes)
	tcases = append(tcases, policy.mergeTestCases(numaInfo.Nodes)...)
	tcases = append(tcases, policy.mergeTestCasesNoPolicies(numaInfo.Nodes)...)

	testPolicyMerge(policy, tcases, t)
}

// TestPolicyRestrictedMergeClosestNUMA 验证启用 PreferClosestNUMA 时 restricted 策略的合并结果。
func TestPolicyRestrictedMergeClosestNUMA(t *testing.T) {
	numaInfo := commonNUMAInfoEightNodes()
	policy := &restrictedPolicy{bestEffortPolicy{numaInfo: numaInfo, opts: PolicyOptions{PreferClosestNUMA: true}}}

	// 合并通用用例与启用最近 NUMA 偏好时的专用用例。
	tcases := commonPolicyMergeTestCases(numaInfo.Nodes)
	tcases = append(tcases, policy.mergeTestCases(numaInfo.Nodes)...)
	tcases = append(tcases, policy.mergeTestCasesClosestNUMA(numaInfo.Nodes)...)

	testPolicyMerge(policy, tcases, t)
}
