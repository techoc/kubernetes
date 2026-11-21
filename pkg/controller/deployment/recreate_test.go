/*
Copyright 2016 The Kubernetes Authors.

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

package deployment

import (
	"fmt"
	"testing"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
)

// TestScaleDownOldReplicaSets 验证 Recreate 策略下旧 ReplicaSet 会被缩容到 0。
// 用 fake client 预置"期望状态"的 RS，再传入原始 RS 调用缩容，断言内存对象被改写。
func TestScaleDownOldReplicaSets(t *testing.T) {
	tests := []struct {
		oldRSSizes []int32 // 各旧 RS 的初始副本数
		d          *apps.Deployment
	}{
		{
			oldRSSizes: []int32{3},
			d:          newDeployment("foo", 3, nil, nil, nil, map[string]string{"foo": "bar"}),
		},
	}

	for i := range tests {
		t.Logf("running scenario %d", i)
		test := tests[i]

		var oldRSs []*apps.ReplicaSet
		var expected []runtime.Object

		for n, size := range test.oldRSSizes {
			rs := newReplicaSet(test.d, fmt.Sprintf("%s-%d", test.d.Name, n), size)
			oldRSs = append(oldRSs, rs)

			// 期望状态是副本数归零的 RS 深拷贝，避免与输入共享指针。
			rsCopy := rs.DeepCopy()

			zero := int32(0)
			rsCopy.Spec.Replicas = &zero
			expected = append(expected, rsCopy)

			// 自检：原始与期望副本数必须不同，否则测试无法区分缩容是否发生。
			if *(oldRSs[n].Spec.Replicas) == *(expected[n].(*apps.ReplicaSet).Spec.Replicas) {
				t.Errorf("broken test - original and expected RS have the same size")
			}
		}

		kc := fake.NewSimpleClientset(expected...)
		informers := informers.NewSharedInformerFactory(kc, controller.NoResyncPeriodFunc())
		_, ctx := ktesting.NewTestContext(t)
		c, err := NewDeploymentController(ctx, informers.Apps().V1().Deployments(), informers.Apps().V1().ReplicaSets(), informers.Core().V1().Pods(), kc)
		if err != nil {
			t.Fatalf("error creating Deployment controller: %v", err)
		}
		// 事件记录器仅用于避免空指针，不关心事件内容。
		c.eventRecorder = &record.FakeRecorder{}

		c.scaleDownOldReplicaSetsForRecreate(ctx, oldRSs, test.d)
		for j := range oldRSs {
			rs := oldRSs[j]

			// 断言缩容直接作用于传入的 RS 对象。
			if *rs.Spec.Replicas != 0 {
				t.Errorf("rs %q has non-zero replicas", rs.Name)
			}
		}
	}
}

// TestOldPodsRunning 验证"旧 Pod 是否仍在运行"的判定，覆盖无旧 RS、
// 非零 status 副本、终态/未知/Pending/Running 相位等场景。
func TestOldPodsRunning(t *testing.T) {
	tests := []struct {
		name string

		newRS  *apps.ReplicaSet
		oldRSs []*apps.ReplicaSet
		// podMap 按 RS UID 索引其 Pod；空列表表示 Pod 已消失但 API 对象可能尚在。
		podMap map[types.UID][]*v1.Pod

		hasOldPodsRunning bool
	}{
		{
			// 没有旧 RS 时无需等待
			name:              "no old RSs",
			hasOldPodsRunning: false,
		},
		{
			// Pod 存在即视为仍在运行
			name:              "old RSs with running pods",
			oldRSs:            []*apps.ReplicaSet{rsWithUID("some-uid"), rsWithUID("other-uid")},
			podMap:            podMapWithUIDs([]string{"some-uid", "other-uid"}),
			hasOldPodsRunning: true,
		},
		{
			// 无 Pod 对象但 status 副本非零：信息可能滞后，保守认为仍在运行
			name:              "old RSs without pods but with non-zero status replicas",
			oldRSs:            []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 1, nil)},
			hasOldPodsRunning: true,
		},
		{
			// 无 Pod 且 status 副本为零：可以安全推进
			name:              "old RSs without pods or non-zero status replicas",
			oldRSs:            []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			hasOldPodsRunning: false,
		},
		{
			// Failed/Succeeded 是终态，不应阻塞
			name:   "old RSs with zero status replicas but pods in terminal state are present",
			oldRSs: []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			podMap: map[types.UID][]*v1.Pod{
				"uid-1": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodFailed,
						},
					},
					{
						Status: v1.PodStatus{
							Phase: v1.PodSucceeded,
						},
					},
				},
			},
			hasOldPodsRunning: false,
		},
		{
			// Unknown 相位不能证明 Pod 已终止，保守等待
			name:   "old RSs with zero status replicas but pod in unknown phase present",
			oldRSs: []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			podMap: map[types.UID][]*v1.Pod{
				"uid-1": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodUnknown,
						},
					},
				},
			},
			hasOldPodsRunning: true,
		},
		{
			// Pending 的 Pod 可能尚未运行但即将启动，需等待
			name:   "old RSs with zero status replicas with pending pod present",
			oldRSs: []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			podMap: map[types.UID][]*v1.Pod{
				"uid-1": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodPending,
						},
					},
				},
			},
			hasOldPodsRunning: true,
		},
		{
			// Running 明确表示仍在运行
			name:   "old RSs with zero status replicas with running pod present",
			oldRSs: []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			podMap: map[types.UID][]*v1.Pod{
				"uid-1": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodRunning,
						},
					},
				},
			},
			hasOldPodsRunning: true,
		},
		{
			// 混合场景：只要有任何非终态 Pod（哪怕 UID 对应空列表）就需等待
			name:   "old RSs with zero status replicas but pods in terminal state and pending are present",
			oldRSs: []*apps.ReplicaSet{newRSWithStatus("rs-1", 0, 0, nil)},
			podMap: map[types.UID][]*v1.Pod{
				"uid-1": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodFailed,
						},
					},
					{
						Status: v1.PodStatus{
							Phase: v1.PodSucceeded,
						},
					},
				},
				"uid-2": {},
				"uid-3": {
					{
						Status: v1.PodStatus{
							Phase: v1.PodPending,
						},
					},
				},
			},
			hasOldPodsRunning: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if expected, got := test.hasOldPodsRunning, oldPodsRunning(test.newRS, test.oldRSs, test.podMap); expected != got {
				t.Errorf("%s: expected %t, got %t", test.name, expected, got)
			}
		})
	}
}

// rsWithUID 构造带指定 UID 的 ReplicaSet，用于按 UID 关联 Pod。
func rsWithUID(uid string) *apps.ReplicaSet {
	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	rs := newReplicaSet(d, fmt.Sprintf("foo-%s", uid), 0)
	rs.UID = types.UID(uid)
	return rs
}

// podMapWithUIDs 为每个 UID 生成包含两个占位 Pod 的映射。
func podMapWithUIDs(uids []string) map[types.UID][]*v1.Pod {
	podMap := make(map[types.UID][]*v1.Pod)
	for _, uid := range uids {
		podMap[types.UID(uid)] = []*v1.Pod{
			{ /* supposedly a pod */ },
			{ /* supposedly another pod pod */ },
		}
	}
	return podMap
}
