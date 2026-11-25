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

package deletion

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestUpdateConditions 验证 updateConditions 对现有 Condition 的更新语义：
// 未知类型保持原样，已知类型被新结果覆盖，缺失的已知类型按成功状态补齐。
func TestUpdateConditions(t *testing.T) {
	// 表格字段说明：newConditions 为本轮新观测到的失败/状态，startingStatus 为更新前的状态，
	// expecteds 为期望的最终 Condition 集合。
	tests := []struct {
		name string

		newConditions  []v1.NamespaceCondition
		startingStatus *v1.NamespaceStatus

		expecteds []v1.NamespaceCondition
	}{
		// 未知类型的 Condition 不在控制器管理范围内，必须原样保留；
		// 其余已知类型即使没有新事件，也要补上"成功"状态。
		{
			name: "leave unknown",

			newConditions: []v1.NamespaceCondition{},
			startingStatus: &v1.NamespaceStatus{
				Conditions: []v1.NamespaceCondition{
					{Type: "unknown", Status: v1.ConditionTrue},
				},
			},
			expecteds: []v1.NamespaceCondition{
				{Type: "unknown", Status: v1.ConditionTrue},
				*newSuccessfulCondition(v1.NamespaceDeletionDiscoveryFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionGVParsingFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionContentFailure),
				*newSuccessfulCondition(v1.NamespaceContentRemaining),
				*newSuccessfulCondition(v1.NamespaceFinalizersRemaining),
			},
		},
		// 旧的失败状态在收到新的成功结果后应被覆盖，避免失败信息一直残留。
		{
			name: "replace with success",

			newConditions: []v1.NamespaceCondition{},
			startingStatus: &v1.NamespaceStatus{
				Conditions: []v1.NamespaceCondition{
					{Type: v1.NamespaceDeletionDiscoveryFailure, Status: v1.ConditionTrue},
				},
			},
			expecteds: []v1.NamespaceCondition{
				*newSuccessfulCondition(v1.NamespaceDeletionDiscoveryFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionGVParsingFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionContentFailure),
				*newSuccessfulCondition(v1.NamespaceContentRemaining),
				*newSuccessfulCondition(v1.NamespaceFinalizersRemaining),
			},
		},
		// Condition 的排列顺序由起始状态决定，不应因更新而被重排。
		{
			name: "leave different order",

			newConditions: []v1.NamespaceCondition{},
			startingStatus: &v1.NamespaceStatus{
				Conditions: []v1.NamespaceCondition{
					{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionTrue},
					{Type: v1.NamespaceDeletionDiscoveryFailure, Status: v1.ConditionTrue},
				},
			},
			expecteds: []v1.NamespaceCondition{
				*newSuccessfulCondition(v1.NamespaceDeletionGVParsingFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionDiscoveryFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionContentFailure),
				*newSuccessfulCondition(v1.NamespaceContentRemaining),
				*newSuccessfulCondition(v1.NamespaceFinalizersRemaining),
			},
		},
		// 新的失败结果应覆盖同类型的旧结果，并保留其 Reason 与 Message。
		{
			name: "overwrite with failure",

			newConditions: []v1.NamespaceCondition{
				{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionTrue, Reason: "foo", Message: "bar"},
			},
			startingStatus: &v1.NamespaceStatus{
				Conditions: []v1.NamespaceCondition{
					{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionFalse},
					{Type: v1.NamespaceDeletionDiscoveryFailure, Status: v1.ConditionTrue},
				},
			},
			expecteds: []v1.NamespaceCondition{
				{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionTrue, Reason: "foo", Message: "bar"},
				*newSuccessfulCondition(v1.NamespaceDeletionDiscoveryFailure),
				*newSuccessfulCondition(v1.NamespaceDeletionContentFailure),
				*newSuccessfulCondition(v1.NamespaceContentRemaining),
				*newSuccessfulCondition(v1.NamespaceFinalizersRemaining),
			},
		},
		// 起始状态中不存在的新失败类型应被追加到结果中。
		{
			name: "write new failure",

			newConditions: []v1.NamespaceCondition{
				{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionTrue, Reason: "foo", Message: "bar"},
			},
			startingStatus: &v1.NamespaceStatus{
				Conditions: []v1.NamespaceCondition{
					{Type: v1.NamespaceDeletionDiscoveryFailure, Status: v1.ConditionTrue},
				},
			},
			expecteds: []v1.NamespaceCondition{
				*newSuccessfulCondition(v1.NamespaceDeletionDiscoveryFailure),
				{Type: v1.NamespaceDeletionGVParsingFailure, Status: v1.ConditionTrue, Reason: "foo", Message: "bar"},
				*newSuccessfulCondition(v1.NamespaceDeletionContentFailure),
				*newSuccessfulCondition(v1.NamespaceContentRemaining),
				*newSuccessfulCondition(v1.NamespaceFinalizersRemaining),
			},
		},
	}

	// updateConditions 原地修改 startingStatus.Conditions，因此直接读取该字段作为实际结果。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			updateConditions(test.startingStatus, test.newConditions)

			// 先比较数量，防止随后按索引访问时越界。
			actuals := test.startingStatus.Conditions
			if len(actuals) != len(test.expecteds) {
				t.Fatal(actuals)
			}
			// 逐项比较时忽略 LastTransitionTime：它由实现按当前时间生成，
			// 测试中无法预知，因此用实际值覆盖期望值后再做深度比较。
			for i := range actuals {
				actual := actuals[i]
				expected := test.expecteds[i]
				// 时间戳由实现生成，不属于本测试关注的语义，比较前对齐。
				expected.LastTransitionTime = actual.LastTransitionTime
				if !reflect.DeepEqual(expected, actual) {
					t.Error(actual)
				}
			}
		})
	}
}

// TestProcessContentTotals 验证 ProcessContentTotals 能按剩余资源与 finalizer 统计，
// 生成对应的 ContentRemaining / FinalizersRemaining Condition。
func TestProcessContentTotals(t *testing.T) {
	tests := []struct {
		name string

		contentTotals allGVRDeletionMetadata
		expecteds     []v1.NamespaceCondition
	}{
		// 没有任何剩余项时不应产生任何 Condition。
		{
			name: "nothing",

			contentTotals: allGVRDeletionMetadata{
				gvrToNumRemaining:        map[schema.GroupVersionResource]int{},
				finalizersToNumRemaining: map[string]int{},
			},
			expecteds: []v1.NamespaceCondition{},
		},
		// 仅有资源剩余时，只生成 NamespaceContentRemaining，消息中按 GVR 列出数量。
		{
			name: "just remaining",

			contentTotals: allGVRDeletionMetadata{
				gvrToNumRemaining: map[schema.GroupVersionResource]int{
					{Group: "apps.k8s.io", Resource: "daemonsets"}:  5,
					{Group: "apps.k8s.io", Resource: "deployments"}: 5,
				},
				finalizersToNumRemaining: map[string]int{},
			},
			expecteds: []v1.NamespaceCondition{
				{Type: v1.NamespaceContentRemaining, Status: v1.ConditionTrue, Reason: "SomeResourcesRemain", Message: `Some resources are remaining: daemonsets.apps.k8s.io has 5 resource instances, deployments.apps.k8s.io has 5 resource instances`},
			},
		},
		{
			// 仅有 finalizer 剩余：理论上不应出现（资源先于 finalizer 被清理），但实现仍需能处理。
			name: "just finalizers", // 理论上不应出现

			contentTotals: allGVRDeletionMetadata{
				gvrToNumRemaining: map[schema.GroupVersionResource]int{},
				finalizersToNumRemaining: map[string]int{
					"service-catalog": 6,
					"kubedb":          5,
				},
			},
			expecteds: []v1.NamespaceCondition{
				{Type: v1.NamespaceFinalizersRemaining, Status: v1.ConditionTrue, Reason: "SomeFinalizersRemain", Message: `Some content in the namespace has finalizers remaining: kubedb in 5 resource instances, service-catalog in 6 resource instances`},
			},
		},
		// 资源与 finalizer 同时剩余时，应同时生成两个 Condition，分别反映各自计数。
		{
			name: "both",

			contentTotals: allGVRDeletionMetadata{
				gvrToNumRemaining: map[schema.GroupVersionResource]int{
					{Group: "apps.k8s.io", Resource: "daemonsets"}:  5,
					{Group: "apps.k8s.io", Resource: "deployments"}: 5,
				},
				finalizersToNumRemaining: map[string]int{
					"service-catalog": 6,
					"kubedb":          5,
				},
			},
			expecteds: []v1.NamespaceCondition{
				{Type: v1.NamespaceContentRemaining, Status: v1.ConditionTrue, Reason: "SomeResourcesRemain", Message: `Some resources are remaining: daemonsets.apps.k8s.io has 5 resource instances, deployments.apps.k8s.io has 5 resource instances`},
				{Type: v1.NamespaceFinalizersRemaining, Status: v1.ConditionTrue, Reason: "SomeFinalizersRemain", Message: `Some content in the namespace has finalizers remaining: kubedb in 5 resource instances, service-catalog in 6 resource instances`},
			},
		},
	}

	// 同样忽略由实现生成的时间戳，只比较 Condition 的业务语义。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 每次子测试使用全新的 updater，避免前一行用例的状态污染本次结果。
			u := namespaceConditionUpdater{}

			u.ProcessContentTotals(test.contentTotals)

			actuals := u.newConditions
			if len(actuals) != len(test.expecteds) {
				t.Fatal(actuals)
			}
			for i := range actuals {
				actual := actuals[i]
				expected := test.expecteds[i]
				expected.LastTransitionTime = actual.LastTransitionTime
				if !reflect.DeepEqual(expected, actual) {
					t.Error(actual)
				}
			}
		})
	}
}
