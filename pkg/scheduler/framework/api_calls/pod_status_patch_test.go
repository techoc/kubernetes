/*
Copyright 2025 The Kubernetes Authors.

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

package apicalls

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
)

// TestPodStatusPatchCall_IsNoOp 验证 IsNoOp 只在目标条件与提名节点都和 Pod 现状一致时返回 true。
// 无实际变更时跳过 patch 可减少不必要的 API 写入。
func TestPodStatusPatchCall_IsNoOp(t *testing.T) {
	// 基准 Pod 已提名 node-a 且 PodScheduled=False，后续用例复用它。
	podWithNode := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "uid",
		},
		Status: v1.PodStatus{
			NominatedNodeName: "node-a",
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionFalse,
				},
			},
		},
	}

	// conditions 为待写入状态，nominatingInfo 为提名信息，want 表示是否无需 patch。
	tests := []struct {
		name           string
		pod            *v1.Pod
		conditions     []*v1.PodCondition
		nominatingInfo *fwk.NominatingInfo
		want           bool
	}{
		{
			name:           "No-op when condition and node name match",
			pod:            podWithNode,
			conditions:     []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			want:           true,
		},
		{
			name:           "Not no-op when condition is different",
			pod:            podWithNode,
			conditions:     []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			want:           false,
		},
		{
			name:           "Not no-op when nominated node name is different",
			pod:            podWithNode,
			conditions:     []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeOverride},
			want:           false,
		},
		{
			name:           "No-op when condition is nil and node name matches",
			pod:            podWithNode,
			conditions:     nil,
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			want:           true,
		},
		{
			name:           "Not no-op when condition is nil but node name differs",
			pod:            podWithNode,
			conditions:     nil,
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeOverride},
			want:           false,
		},
		{
			name:           "Not no-op when any of multiple conditions differ",
			pod:            podWithNode,
			conditions:     []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}, {Type: v1.PodInitialized, Status: v1.ConditionTrue}},
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			want:           false,
		},
		{
			name: "No-op when multiple conditions all match",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "uid"},
				Status: v1.PodStatus{
					NominatedNodeName: "node-a",
					Conditions: []v1.PodCondition{
						{Type: v1.PodScheduled, Status: v1.ConditionFalse},
						{Type: v1.PodInitialized, Status: v1.ConditionTrue},
					},
				},
			},
			conditions:     []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}, {Type: v1.PodInitialized, Status: v1.ConditionTrue}},
			nominatingInfo: &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			want:           true,
		},
	}
	// 覆盖条件匹配/不匹配、提名节点匹配/不匹配、nil 条件和多条件等组合。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			call := NewPodStatusPatchCall(test.pod, test.conditions, test.nominatingInfo)
			// IsNoOp 只做判定，不会改动 Pod 或调用 API。
			if got := call.IsNoOp(); got != test.want {
				t.Errorf("Expected IsNoOp() to return %v, but got %v", test.want, got)
			}
		})
	}
}

// TestPodStatusPatchCall_Merge 验证合并旧调用时的覆盖规则。
// 新调用已有的字段优先；新调用缺失的字段从旧调用补齐，避免丢掉调度结果。
func TestPodStatusPatchCall_Merge(t *testing.T) {
	// 仅用于构造调用对象，Merge 本身不依赖 Pod 的具体状态。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "uid",
		},
	}

	// 旧调用有值、新调用为空：应从旧调用补上提名节点和条件。
	t.Run("Merges nominating info and condition from the old call", func(t *testing.T) {
		oldCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			&fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
		)
		newCall := NewPodStatusPatchCall(pod, nil, &fwk.NominatingInfo{NominatingMode: fwk.ModeNoop})

		if err := newCall.Merge(oldCall); err != nil {
			t.Fatalf("Unexpected error returned by Merge(): %v", err)
		}
		// 新调用显式给出 Noop 模式，但其提名节点为空，故仍应沿用旧值。
		if newCall.nominatingInfo.NominatedNodeName != "node-a" {
			t.Errorf("Expected NominatedNodeName to be node-a, but got: %v", newCall.nominatingInfo.NominatedNodeName)
		}
		if len(newCall.newConditions) == 0 || newCall.newConditions[0].Type != v1.PodScheduled {
			t.Errorf("Expected PodScheduled condition, but got: %v", newCall.newConditions)
		}
	})

	// 新调用已有值：不能被旧调用覆盖，否则新一次调度的结果会被回退。
	t.Run("Doesn't overwrite nominating info and condition of a new call", func(t *testing.T) {
		oldCall := NewPodStatusPatchCall(pod, nil, &fwk.NominatingInfo{NominatingMode: fwk.ModeNoop})
		newCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			&fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeOverride})

		if err := newCall.Merge(oldCall); err != nil {
			t.Fatalf("Unexpected error returned by Merge(): %v", err)
		}
		if newCall.nominatingInfo.NominatedNodeName != "node-b" {
			t.Errorf("Expected NominatedNodeName to be node-b, but got: %v", newCall.nominatingInfo.NominatedNodeName)
		}
		if len(newCall.newConditions) == 0 || newCall.newConditions[0].Type != v1.PodScheduled {
			t.Errorf("Expected PodScheduled condition, but got: %v", newCall.newConditions)
		}
	})

	// 新旧对同一条件给出不同值：新值必须胜出。
	t.Run("Overwrite nominating info and condition of a old call", func(t *testing.T) {
		oldCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}}, nil)
		newCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}, {Type: v1.PodInitialized, Status: v1.ConditionTrue}}, nil)

		if err := newCall.Merge(oldCall); err != nil {
			t.Fatalf("Unexpected error returned by Merge(): %v", err)
		}
		expectedConditions := []*v1.PodCondition{
			{Type: v1.PodScheduled, Status: v1.ConditionTrue},
			{Type: v1.PodInitialized, Status: v1.ConditionTrue},
		}
		// 比较完整条件列表，确认旧调用的 PodScheduled 没有覆盖新值。
		if diff := cmp.Diff(expectedConditions, newCall.newConditions); diff != "" {
			t.Errorf("Unexpected conditions after merging (-want,+got):\n%s", diff)
		}
	})

	// 新旧条件类型不重叠：两者都应保留，顺序为新条件在前。
	t.Run("Merges condition from old call that new call doesn't have", func(t *testing.T) {
		oldCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			&fwk.NominatingInfo{NominatingMode: fwk.ModeNoop},
		)
		newCall := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodInitialized, Status: v1.ConditionTrue}},
			&fwk.NominatingInfo{NominatingMode: fwk.ModeNoop})

		if err := newCall.Merge(oldCall); err != nil {
			t.Fatalf("Unexpected error returned by Merge(): %v", err)
		}
		expectedConditions := []*v1.PodCondition{
			{Type: v1.PodInitialized, Status: v1.ConditionTrue},
			{Type: v1.PodScheduled, Status: v1.ConditionFalse},
		}
		if diff := cmp.Diff(expectedConditions, newCall.newConditions); diff != "" {
			t.Errorf("Unexpected conditions after merging (-want,+got):\n%s", diff)
		}
	})
}

// TestPodStatusPatchCall_Sync 验证 Sync 在执行前后对内部状态和返回 Pod 的更新差异。
// 执行前同步内部状态；执行后固定内部状态，只把最新字段写入返回的 Pod。
func TestPodStatusPatchCall_Sync(t *testing.T) {
	// 基准 Pod 状态，后续的 updatedPod 在其副本上改动提名节点。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "uid",
		},
		Status: v1.PodStatus{
			NominatedNodeName: "node-a",
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionFalse,
				},
			},
		},
	}

	// 未执行时：内部状态取自最新 Pod，返回对象叠加本次 patch 的目标值。
	t.Run("Syncs the status before execution and updates the pod", func(t *testing.T) {
		call := NewPodStatusPatchCall(pod, nil,
			&fwk.NominatingInfo{NominatedNodeName: "node-c", NominatingMode: fwk.ModeOverride})

		updatedPod := pod.DeepCopy()
		updatedPod.Status.NominatedNodeName = "node-b"

		syncedObj, err := call.Sync(updatedPod)
		if err != nil {
			t.Fatalf("Unexpected error returned by Sync(): %v", err)
		}
		// 执行前同步：内部状态应跟随 updatedPod 变成 node-b。
		if call.podStatus.NominatedNodeName != "node-b" {
			t.Errorf("Expected podStatus NominatedNodeName to be node-b, but got: %v", call.podStatus.NominatedNodeName)
		}
		// 返回对象应保留 patch 中指定的 node-c。
		syncedPod := syncedObj.(*v1.Pod)
		if syncedPod.Status.NominatedNodeName != "node-c" {
			t.Errorf("Expected synced pod's NominatedNodeName to be node-c, but got: %v", syncedPod.Status.NominatedNodeName)
		}
	})

	// 已执行时：内部状态保持在 patch 生成时的快照，避免后续同步推翻已提交结果。
	t.Run("Doesn't sync internal status during or after execution, but updates the pod", func(t *testing.T) {
		call := NewPodStatusPatchCall(pod, nil,
			&fwk.NominatingInfo{NominatedNodeName: "node-c", NominatingMode: fwk.ModeOverride})
		// 直接置位模拟 Execute 已完成，仅考察 Sync 的分支行为。
		call.executed = true

		updatedPod := pod.DeepCopy()
		updatedPod.Status.NominatedNodeName = "node-b"

		syncedObj, err := call.Sync(updatedPod)
		if err != nil {
			t.Fatalf("Unexpected error returned by Sync(): %v", err)
		}
		// 执行后不同步：内部状态仍是最初的 node-a，而非 updatedPod 的 node-b。
		if call.podStatus.NominatedNodeName != "node-a" {
			t.Errorf("Expected podStatus NominatedNodeName to be node-a, but got: %v", call.podStatus.NominatedNodeName)
		}
		// 返回对象照常应用 patch 中的 node-c。
		syncedPod := syncedObj.(*v1.Pod)
		if syncedPod.Status.NominatedNodeName != "node-c" {
			t.Errorf("Expected synced pod's NominatedNodeName to be node-c, but got: %v", syncedPod.Status.NominatedNodeName)
		}
	})
}

// TestSyncStatus 验证 syncStatus 在何种条件下原地更新 PodStatus，以及提名模式对更新的影响。
// ModeNoop 下提名节点不参与更新，只有 Condition 变化才会触发写入。
func TestSyncStatus(t *testing.T) {
	// expectUpdated 表示函数是否报告发生了变更；其余 expected 字段描述最终状态。
	tests := []struct {
		name                  string
		oldPodStatus          *v1.PodStatus
		conditions            []*v1.PodCondition
		nominatingInfo        *fwk.NominatingInfo
		expectUpdated         bool
		expectedNominatedNode string
		expectedConditions    []v1.PodCondition
	}{
		{
			name: "No changes needed - empty conditions and same node name",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-a",
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodScheduled,
						Status: v1.ConditionFalse,
					},
				},
			},
			conditions:            nil,
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride},
			expectUpdated:         false,
			expectedNominatedNode: "node-a",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
		},
		{
			name: "Condition needs update",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-a",
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodScheduled,
						Status: v1.ConditionFalse,
					},
				},
			},
			conditions:            []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeNoop},
			expectUpdated:         true,
			expectedNominatedNode: "node-a",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
		},
		{
			name: "Nominated node name needs update",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-a",
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodScheduled,
						Status: v1.ConditionFalse,
					},
				},
			},
			conditions:            nil,
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeOverride},
			expectUpdated:         true,
			expectedNominatedNode: "node-b",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
		},
		{
			name: "Both condition and node name need update",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-a",
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodScheduled,
						Status: v1.ConditionFalse,
					},
				},
			},
			conditions:            []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeOverride},
			expectUpdated:         true,
			expectedNominatedNode: "node-b",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
		},
		{
			name: "Multiple conditions need update",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-original",
			},
			conditions: []*v1.PodCondition{
				{Type: v1.PodScheduled, Status: v1.ConditionTrue},
				{Type: v1.ContainersReady, Status: v1.ConditionFalse},
			},
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeNoop},
			expectUpdated:         true,
			expectedNominatedNode: "node-original",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}, {Type: v1.ContainersReady, Status: v1.ConditionFalse}},
		},
		{
			name: "No-op nominating mode with different node name",
			oldPodStatus: &v1.PodStatus{
				NominatedNodeName: "node-a",
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodScheduled,
						Status: v1.ConditionFalse,
					},
				},
			},
			conditions:            nil,
			nominatingInfo:        &fwk.NominatingInfo{NominatedNodeName: "node-b", NominatingMode: fwk.ModeNoop},
			expectUpdated:         false,
			expectedNominatedNode: "node-a",
			expectedConditions:    []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
		},
	}

	// 覆盖无需更新、条件更新、提名节点更新及 ModeNoop 忽略提名节点等场景。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 深拷贝避免用例间共享状态，确保断言的是单次调用的结果。
			podStatus := test.oldPodStatus.DeepCopy()
			result := syncStatus(podStatus, test.conditions, test.nominatingInfo)
			if result != test.expectUpdated {
				t.Errorf("Expected syncStatus to return %v, but got %v", test.expectUpdated, result)
			}
			if podStatus.NominatedNodeName != test.expectedNominatedNode {
				t.Errorf("Expected NominatedNodeName to be %s, but got %s", test.expectedNominatedNode, podStatus.NominatedNodeName)
			}
			// 时间戳由同步逻辑自动生成，比较条件内容时忽略它们。
			if diff := cmp.Diff(test.expectedConditions, podStatus.Conditions, cmpopts.IgnoreFields(v1.PodCondition{}, "LastTransitionTime", "LastProbeTime")); diff != "" {
				t.Errorf("Unexpected conditions (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPodStatusPatchCall_Execute 验证 Execute 是否按需发起 patch 并标记已执行。
// 无实际变更时不应产生 API 调用，避免无谓的写压力。
func TestPodStatusPatchCall_Execute(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// Pod 带命名空间和名称，fake client 才能定位到具体资源。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "uid",
			Name:      "pod",
			Namespace: "ns",
		},
		Status: v1.PodStatus{
			NominatedNodeName: "node-a",
		},
	}

	// 有变更时：应调用 patch API 并置位 executed。
	t.Run("Successful patch", func(t *testing.T) {
		client := fake.NewClientset()
		patched := false
		// 拦截 patch 动作，用标志位记录是否真的发起过 API 调用。
		client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
			patched = true
			return true, nil, nil
		})

		call := NewPodStatusPatchCall(pod, []*v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionFalse}},
			&fwk.NominatingInfo{NominatingMode: fwk.ModeNoop})
		if err := call.Execute(ctx, client); err != nil {
			t.Fatalf("Unexpected error returned by Execute(): %v", err)
		}
		// 假客户端不会真的写对象，只需验证动作是否被触发。
		if !patched {
			t.Error("Expected patch API to be called")
		}
		// executed 影响后续 Sync 的行为，必须正确置位。
		if !call.executed {
			t.Error("Expected 'executed' flag to be set during execution")
		}
	})

	// 提名节点已与 Pod 一致且无新条件：这是 no-op，不应产生 patch。
	t.Run("Skip API call if patch is not needed", func(t *testing.T) {
		client := fake.NewClientset()
		patched := false
		client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
			patched = true
			return true, nil, nil
		})

		noOpCall := NewPodStatusPatchCall(pod, nil,
			&fwk.NominatingInfo{NominatedNodeName: "node-a", NominatingMode: fwk.ModeOverride})
		if err := noOpCall.Execute(ctx, client); err != nil {
			t.Fatalf("Unexpected error returned by Execute(): %v", err)
		}
		// 没有实际变更时直接返回，patch 标志必须保持为 false。
		if patched {
			t.Error("Expected patch API not to be called if the call is no-op")
		}
	})
}
