/*
Copyright 2017 The Kubernetes Authors.

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

// 修改本文件时，也应同步修改 ReplicationController 中对应的测试。

package replicaset

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// TestCalculateStatus 验证 calculateStatus 在各种场景下的状态汇总：
// 标签是否完整决定 FullyLabeledReplicas，Pod 就绪状态决定 ReadyReplicas，
// MinReadySeconds 决定 AvailableReplicas，以及 DeploymentReplicaSetTerminatingReplicas
// 特性开关对 TerminatingReplicas 的影响（含 ReplicationController 不上报该字段的例外）。
func TestCalculateStatus(t *testing.T) {
	labelMap := map[string]string{"name": "foo"}
	fullLabelMap := map[string]string{"name": "foo", "type": "production"}
	notFullyLabelledRS := newReplicaSet(1, labelMap)
	// 副本数设为 2，便于在状态条件测试中构造 diff < 0 和 diff > 0 两种情形。
	fullyLabelledRS := newReplicaSet(2, fullLabelMap)
	longMinReadySeconds := 3600
	longMinReadySecondsRS := newReplicaSet(1, fullLabelMap)
	longMinReadySecondsRS.Spec.MinReadySeconds = int32(longMinReadySeconds)

	// 用删除时间戳模拟处于终止中的 Pod，以验证 TerminatingReplicas 的统计。
	asTerminating := func(pod *v1.Pod) *v1.Pod {
		pod.DeletionTimestamp = ptr.To(meta.Now())
		return pod
	}

	clock := clocktesting.NewFakePassiveClock(time.Now())

	rsStatusTests := []struct {
		// name 是子测试名称。
		name string
		// enableDeploymentReplicaSetTerminatingReplicas 控制特性开关的取值。
		enableDeploymentReplicaSetTerminatingReplicas bool
		// replicaset 是待计算状态的 ReplicaSet。
		replicaset *apps.ReplicaSet
		// activePods 是活跃的 Pod 列表。
		activePods []*v1.Pod
		// terminatingPods 是处于终止中的 Pod 列表。
		terminatingPods []*v1.Pod
		// controllerFeatures 可覆盖默认的控制器特性配置，nil 表示使用默认值。
		controllerFeatures *ReplicaSetControllerFeatures
		// expectedReplicaSetStatus 是期望的状态汇总结果。
		expectedReplicaSetStatus apps.ReplicaSetStatus
	}{
		{
			"1 fully labelled pod",
			false,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 not fully labelled pod", // Pod 缺少 type 标签，FullyLabeledReplicas 应为 0。
			false,
			notFullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", notFullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 0,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
		{
			"2 fully labelled pods",
			false,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
				newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             2,
				FullyLabeledReplicas: 2,
				ReadyReplicas:        2,
				AvailableReplicas:    2,
				TerminatingReplicas:  nil,
			},
		},
		{
			"2 fully labelled pods with DeploymentReplicaSetTerminatingReplicas",
			true,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
				newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             2,
				FullyLabeledReplicas: 2,
				ReadyReplicas:        2,
				AvailableReplicas:    2,
				TerminatingReplicas:  ptr.To[int32](0),
			},
		},
		{
			"2 not fully labelled pods",
			false,
			notFullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", notFullyLabelledRS, v1.PodRunning, nil, true),
				newPod("pod2", notFullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             2,
				FullyLabeledReplicas: 0,
				ReadyReplicas:        2,
				AvailableReplicas:    2,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 fully labelled pod, 1 not fully labelled pod",
			false,
			notFullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", notFullyLabelledRS, v1.PodRunning, nil, true),
				newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             2,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        2,
				AvailableReplicas:    2,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 non-ready pod", // Pending Pod 不计入 ReadyReplicas 与 AvailableReplicas。
			false,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodPending, nil, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        0,
				AvailableReplicas:    0,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 ready but non-available pod", // 刚就绪，尚未满足 MinReadySeconds，因此 Available 为 0。
			false,
			longMinReadySecondsRS,
			[]*v1.Pod{
				newPod("pod1", longMinReadySecondsRS, v1.PodRunning, &meta.Time{Time: clock.Now()}, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    0,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 available pod with minReadySeconds", // 就绪时间已超过 MinReadySeconds，可计入 Available。
			false,
			longMinReadySecondsRS,
			[]*v1.Pod{
				newPod("pod1", longMinReadySecondsRS, v1.PodRunning, &meta.Time{Time: clock.Now().Add(-time.Duration(longMinReadySeconds) * time.Second)}, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 available pod for a long time with minReadySeconds", // 就绪时间远超 MinReadySeconds，同样可用。
			false,
			longMinReadySecondsRS,
			[]*v1.Pod{
				newPod("pod1", longMinReadySecondsRS, v1.PodRunning, &meta.Time{Time: clock.Now().Add(2 * -time.Duration(longMinReadySeconds) * time.Second)}, true),
			},
			nil,
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 fully labelled pod and 1 terminating without DeploymentReplicaSetTerminatingReplicas", // 特性关闭时不上报 TerminatingReplicas。
			false,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			[]*v1.Pod{
				asTerminating(newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true)),
			},
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
		{
			"1 fully labelled pods and 2 terminating with DeploymentReplicaSetTerminatingReplicas", // 特性开启时统计两个终止中的 Pod。
			true,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			[]*v1.Pod{
				asTerminating(newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true)),
				asTerminating(newPod("pod3", fullyLabelledRS, v1.PodRunning, nil, true)),
			},
			nil,
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  ptr.To[int32](2),
			},
		},
		{
			"1 fully labelled pods and 2 terminating with DeploymentReplicaSetTerminatingReplicas (ReplicationController)", // RC 显式关闭该字段，即使特性开启也不上报。
			true,
			fullyLabelledRS,
			[]*v1.Pod{
				newPod("pod1", fullyLabelledRS, v1.PodRunning, nil, true),
			},
			[]*v1.Pod{
				asTerminating(newPod("pod2", fullyLabelledRS, v1.PodRunning, nil, true)),
				asTerminating(newPod("pod3", fullyLabelledRS, v1.PodRunning, nil, true)),
			},
			&ReplicaSetControllerFeatures{
				EnableStatusTerminatingReplicas: false,
			},
			apps.ReplicaSetStatus{
				Replicas:             1,
				FullyLabeledReplicas: 1,
				ReadyReplicas:        1,
				AvailableReplicas:    1,
				TerminatingReplicas:  nil,
			},
		},
	}

	for _, test := range rsStatusTests {
		t.Run(test.name, func(t *testing.T) {
			// 每个子测试单独设置特性开关，避免不同用例之间互相影响。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeploymentReplicaSetTerminatingReplicas, test.enableDeploymentReplicaSetTerminatingReplicas)

			// 用例未显式指定时，使用 ReplicaSet 控制器的默认特性配置。
			controllerFeatures := ptr.Deref(test.controllerFeatures, DefaultReplicaSetControllerFeatures())
			replicaSetStatus := calculateStatus(test.replicaset, test.activePods, test.terminatingPods, nil, controllerFeatures, clock.Now())
			if !reflect.DeepEqual(replicaSetStatus, test.expectedReplicaSetStatus) {
				t.Errorf("unexpected replicaset status: expected %v, got %v", test.expectedReplicaSetStatus, replicaSetStatus)
			}
		})
	}
}

// TestCalculateStatusConditions 验证 calculateStatus 对 ReplicaFailure 条件的处理：
// manageReplicas 出错且当前无失败条件时，按副本数差异方向设置 FailedCreate/FailedDelete 原因；
// 已有失败条件时保留原条件，错误消失后清除该条件。
func TestCalculateStatusConditions(t *testing.T) {
	labelMap := map[string]string{"name": "foo"}
	rs := newReplicaSet(2, labelMap)
	replicaFailureRS := newReplicaSet(10, labelMap)
	replicaFailureRS.Status.Conditions = []apps.ReplicaSetCondition{
		{
			Type:   apps.ReplicaSetReplicaFailure,
			Status: v1.ConditionTrue,
		},
	}

	rsStatusConditionTests := []struct {
		// name 是子测试名称。
		name string
		// replicaset 是待计算状态的 ReplicaSet。
		replicaset *apps.ReplicaSet
		// activePods 是活跃的 Pod 列表。
		activePods []*v1.Pod
		// manageReplicasErr 模拟 manageReplicas 的出错情况。
		manageReplicasErr error
		// expectedReplicaSetConditions 是期望的状态条件列表。
		expectedReplicaSetConditions []apps.ReplicaSetCondition
	}{

		{
			"manageReplicasErr != nil && failureCond == nil, diff < 0", // 副本不足，失败原因应为 FailedCreate。
			rs,
			[]*v1.Pod{
				newPod("pod1", rs, v1.PodRunning, nil, true),
			},
			fmt.Errorf("fake manageReplicasErr"),
			[]apps.ReplicaSetCondition{
				{
					Type:    apps.ReplicaSetReplicaFailure,
					Status:  v1.ConditionTrue,
					Reason:  "FailedCreate",
					Message: "fake manageReplicasErr",
				},
			},
		},
		{
			"manageReplicasErr != nil && failureCond == nil, diff > 0", // 副本过剩，失败原因应为 FailedDelete。
			rs,
			[]*v1.Pod{
				newPod("pod1", rs, v1.PodRunning, nil, true),
				newPod("pod2", rs, v1.PodRunning, nil, true),
				newPod("pod3", rs, v1.PodRunning, nil, true),
			},
			fmt.Errorf("fake manageReplicasErr"),
			[]apps.ReplicaSetCondition{
				{
					Type:    apps.ReplicaSetReplicaFailure,
					Status:  v1.ConditionTrue,
					Reason:  "FailedDelete",
					Message: "fake manageReplicasErr",
				},
			},
		},
		{
			"manageReplicasErr == nil && failureCond != nil", // 错误已消失，应清除旧的失败条件。
			replicaFailureRS,
			[]*v1.Pod{
				newPod("pod1", replicaFailureRS, v1.PodRunning, nil, true),
			},
			nil,
			nil,
		},
		{
			"manageReplicasErr != nil && failureCond != nil", // 已有失败条件时保留原条件，不覆盖其 Reason/Message。
			replicaFailureRS,
			[]*v1.Pod{
				newPod("pod1", replicaFailureRS, v1.PodRunning, nil, true),
			},
			fmt.Errorf("fake manageReplicasErr"),
			[]apps.ReplicaSetCondition{
				{
					Type:   apps.ReplicaSetReplicaFailure,
					Status: v1.ConditionTrue,
				},
			},
		},
		{
			"manageReplicasErr == nil && failureCond == nil", // 一切正常，不应产生任何条件。
			rs,
			[]*v1.Pod{
				newPod("pod1", rs, v1.PodRunning, nil, true),
			},
			nil,
			nil,
		},
	}

	for _, test := range rsStatusConditionTests {
		t.Run(test.name, func(t *testing.T) {
			replicaSetStatus := calculateStatus(test.replicaset, test.activePods, nil, test.manageReplicasErr, DefaultReplicaSetControllerFeatures(), time.Now())
			// 所有用例最多只有一个条件；LastTransitionTime 由实现生成，校验时对齐以专注其他字段。
			if len(replicaSetStatus.Conditions) > 0 {
				test.expectedReplicaSetConditions[0].LastTransitionTime = replicaSetStatus.Conditions[0].LastTransitionTime
			}
			if !reflect.DeepEqual(replicaSetStatus.Conditions, test.expectedReplicaSetConditions) {
				t.Errorf("unexpected replicaset status: expected %v, got %v", test.expectedReplicaSetConditions, replicaSetStatus.Conditions)
			}
		})
	}
}
