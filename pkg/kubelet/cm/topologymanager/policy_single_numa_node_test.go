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
	"reflect"
	"testing"
)

// TestPolicySingleNumaNodeCanAdmitPodResult 验证 Preferred=false 的 hint
// 不能被单 NUMA 节点策略接纳。
func TestPolicySingleNumaNodeCanAdmitPodResult(t *testing.T) {
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
	}
	// 使用双节点拓扑构造策略。
	numaInfo := commonNUMAInfoTwoNodes()

	// 逐用例构造策略并校验准入结果。
	for _, tc := range tcases {
		policy := singleNumaNodePolicy{numaInfo: numaInfo, opts: PolicyOptions{}}
		result := policy.canAdmitPodResult(&tc.hint)

		if result != tc.expected {
			t.Errorf("Expected result to be %t, got %t", tc.expected, result)
		}
	}
}

// TestPolicySingleNumaNodeFilterHints 验证过滤规则：
// 空资源与 nil 掩码 hint 会被剔除，保留可用 hint。
func TestPolicySingleNumaNodeFilterHints(t *testing.T) {
	tcases := []struct {
		name              string
		allResources      [][]TopologyHint
		expectedResources [][]TopologyHint
	}{
		{
			name:              "filter empty resources",
			allResources:      [][]TopologyHint{},
			expectedResources: [][]TopologyHint(nil),
		},
		{
			name: "filter hints with nil socket mask 1/2",
			allResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: nil, Preferred: false},
				},
				{
					{NUMANodeAffinity: nil, Preferred: true},
				},
			},
			expectedResources: [][]TopologyHint{
				[]TopologyHint(nil),
				{
					{NUMANodeAffinity: nil, Preferred: true},
				},
			},
		},
		{
			name: "filter hints with nil socket mask 2/2",
			allResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
					{NUMANodeAffinity: nil, Preferred: false},
				},
				{
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
					{NUMANodeAffinity: nil, Preferred: true},
				},
			},
			expectedResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
				},
				{
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
					{NUMANodeAffinity: nil, Preferred: true},
				},
			},
		},
		{
			name: "filter hints with empty resource socket mask",
			allResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
					{NUMANodeAffinity: nil, Preferred: false},
				},
				{},
			},
			expectedResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
				},
				[]TopologyHint(nil),
			},
		},
		{
			name: "filter hints with wide sockemask",
			allResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
					{NUMANodeAffinity: NewTestBitMask(1, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(0, 1, 2), Preferred: false},
					{NUMANodeAffinity: nil, Preferred: false},
				},
				{
					{NUMANodeAffinity: NewTestBitMask(1, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(0, 1, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(0, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(3), Preferred: false},
				},
				{
					{NUMANodeAffinity: NewTestBitMask(1, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(0, 1, 2), Preferred: false},
					{NUMANodeAffinity: NewTestBitMask(0, 2), Preferred: false},
				},
			},
			expectedResources: [][]TopologyHint{
				{
					{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
					{NUMANodeAffinity: NewTestBitMask(1), Preferred: true},
				},
				[]TopologyHint(nil),
				[]TopologyHint(nil),
			},
		},
	}

	// 逐用例比较过滤结果。
	for _, tc := range tcases {
		// 调用被测过滤函数并深比较结果。
		actual := filterSingleNumaHints(tc.allResources)
		if !reflect.DeepEqual(tc.expectedResources, actual) {
			t.Errorf("Test Case: %s", tc.name)
			t.Errorf("Expected result to be %v, got %v", tc.expectedResources, actual)
		}
	}
}

// TestPolicySingleNumaNodeMerge 验证单 NUMA 节点策略的 hint 合并逻辑。
func TestPolicySingleNumaNodeMerge(t *testing.T) {
	numaInfo := commonNUMAInfoFourNodes()
	policy := singleNumaNodePolicy{numaInfo: numaInfo, opts: PolicyOptions{}}

	// 合并通用用例与策略特有的用例后统一执行。
	tcases := commonPolicyMergeTestCases(numaInfo.Nodes)
	tcases = append(tcases, policy.mergeTestCases(numaInfo.Nodes)...)

	// 复用通用合并测试框架。
	testPolicyMerge(&policy, tcases, t)
}
