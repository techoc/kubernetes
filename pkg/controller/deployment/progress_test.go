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

package deployment

import (
	"context"
	"math"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
)

// newDeploymentStatus 构造仅包含副本计数三类字段的 DeploymentStatus 测试值。
func newDeploymentStatus(replicas, updatedReplicas, availableReplicas int32) apps.DeploymentStatus {
	return apps.DeploymentStatus{
		Replicas:          replicas,
		UpdatedReplicas:   updatedReplicas,
		AvailableReplicas: availableReplicas,
	}
}

// currentDeployment 构造带有进度期限、副本数与条件的 Deployment 测试对象。
// 调用方假定返回的 Deployment 一定是被控制器观察（observed）的对象，此处无需额外验证。
func currentDeployment(pds *int32, replicas, statusReplicas, updatedReplicas, availableReplicas int32, conditions []apps.DeploymentCondition) *apps.Deployment {
	d := &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "progress-test",
		},
		Spec: apps.DeploymentSpec{
			ProgressDeadlineSeconds: pds,
			Replicas:                &replicas,
			Strategy: apps.DeploymentStrategy{
				Type: apps.RecreateDeploymentStrategyType,
			},
		},
		Status: newDeploymentStatus(statusReplicas, updatedReplicas, availableReplicas),
	}
	d.Status.Conditions = conditions
	return d
}

// newRSWithAvailable 构造具有指定可用副本数的 ReplicaSet 测试对象。
func newRSWithAvailable(name string, specReplicas, statusReplicas, availableReplicas int32) *apps.ReplicaSet {
	rs := rs(name, specReplicas, nil, metav1.Time{})
	rs.Status = apps.ReplicaSetStatus{
		Replicas:          statusReplicas,
		AvailableReplicas: availableReplicas,
	}
	return rs
}

// TestRequeueStuckDeployment 验证 requeueStuckDeployment 对进度卡住的 Deployment 计算出的
// 重新入队时间：期限、已完成/已失败对象返回 -1，卡住对象返回剩余秒数。
func TestRequeueStuckDeployment(t *testing.T) {
	pds := int32(60)
	infinite := int32(math.MaxInt32)
	failed := []apps.DeploymentCondition{
		{
			Type:   apps.DeploymentProgressing,
			Status: v1.ConditionFalse,
			Reason: util.TimedOutReason,
		},
	}
	stuck := []apps.DeploymentCondition{
		{
			Type:           apps.DeploymentProgressing,
			Status:         v1.ConditionTrue,
			LastUpdateTime: metav1.Date(2017, 2, 15, 18, 49, 00, 00, time.UTC),
		},
	}

	tests := []struct {
		// name 是用例名；d 是 Deployment；status 是当前状态；
		// nowFn 覆盖当前时间；expected 是期望的重入队间隔（-1 表示无需重入队）。
		name     string
		d        *apps.Deployment
		status   apps.DeploymentStatus
		nowFn    func() time.Time
		expected time.Duration
	}{
		{
			// 未设置进度期限时不做超时判断，无需重新入队。
			name:     "nil progressDeadlineSeconds specified",
			d:        currentDeployment(nil, 4, 3, 3, 2, nil),
			status:   newDeploymentStatus(3, 3, 2),
			expected: time.Duration(-1),
		},
		{
			// MaxInt32 表示无限期，同样不进行超时判断。
			name:     "infinite progressDeadlineSeconds specified",
			d:        currentDeployment(&infinite, 4, 3, 3, 2, nil),
			status:   newDeploymentStatus(3, 3, 2),
			expected: time.Duration(-1),
		},
		{
			// 没有 Progressing 条件就没有计时起点，不计算剩余时间。
			name:     "no progressing condition found",
			d:        currentDeployment(&pds, 4, 3, 3, 2, nil),
			status:   newDeploymentStatus(3, 3, 2),
			expected: time.Duration(-1),
		},
		{
			// 已完成的 Deployment 不再需要重新入队。
			name:     "complete deployment does not need to be requeued",
			d:        currentDeployment(&pds, 3, 3, 3, 3, nil),
			status:   newDeploymentStatus(3, 3, 3),
			expected: time.Duration(-1),
		},
		{
			// 已因超时标记为失败的 Deployment 不再重复入队。
			name:     "already failed deployment does not need to be requeued",
			d:        currentDeployment(&pds, 3, 3, 3, 0, failed),
			status:   newDeploymentStatus(3, 3, 0),
			expected: time.Duration(-1),
		},
		{
			// 距离期限还有 30 秒，应返回该剩余间隔。
			name:     "stuck deployment - 30s",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 49, 30, 00, time.UTC).Time },
			expected: 30 * time.Second,
		},
		{
			// 距离期限还有 1 秒，应返回该剩余间隔。
			name:     "stuck deployment - 1s",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 49, 59, 00, time.UTC).Time },
			expected: 1 * time.Second,
		},
		{
			// 不足 1 秒时向下取整为 0，表示立即入队。
			name:     "failed deployment - less than a second => now",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 49, 59, 1, time.UTC).Time },
			expected: time.Duration(0),
		},
		{
			// 恰好到达期限，立即入队。
			name:     "failed deployment - now",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 50, 00, 00, time.UTC).Time },
			expected: time.Duration(0),
		},
		{
			// 已超过期限，不再计算负间隔，立即入队。
			name:     "failed deployment - 1s after deadline",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 50, 01, 00, time.UTC).Time },
			expected: time.Duration(0),
		},
		{
			// 超时较久同样立即入队，由控制器标记为失败。
			name:     "failed deployment - 60s after deadline",
			d:        currentDeployment(&pds, 3, 3, 3, 1, stuck),
			status:   newDeploymentStatus(3, 3, 1),
			nowFn:    func() time.Time { return metav1.Date(2017, 2, 15, 18, 51, 00, 00, time.UTC).Time },
			expected: time.Duration(0),
		},
	}

	// 构造带有工作队列的控制器，以便 enqueueDeployment 能够被真实调用。
	dc := &DeploymentController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "doesnt_matter",
			},
		),
	}
	dc.enqueueDeployment = dc.enqueue

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// nowFn 是包级变量，仅对显式指定的用例覆盖，以便控制时间边界。
			if test.nowFn != nil {
				nowFn = test.nowFn
			}
			_, ctx := ktesting.NewTestContext(t)
			// 子测试结束后取消 context，避免资源泄漏影响后续用例。
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			got := dc.requeueStuckDeployment(ctx, test.d, test.status)
			if got != test.expected {
				t.Errorf("%s: got duration: %v, expected duration: %v", test.name, got, test.expected)
			}
		})
	}
}

// TestSyncRolloutStatus 验证 syncRolloutStatus 在不同部署阶段对 Progressing 条件的维护：
// 无期限时不估算进度、仅一个 RS 时沿用条件、完成时更新时间戳、超时则标记失败。
func TestSyncRolloutStatus(t *testing.T) {
	pds := int32(60)
	testTime := metav1.Date(2017, 2, 15, 18, 49, 00, 00, time.UTC)
	failedTimedOut := apps.DeploymentCondition{
		Type:   apps.DeploymentProgressing,
		Status: v1.ConditionFalse,
		Reason: util.TimedOutReason,
	}
	newRSAvailable := apps.DeploymentCondition{
		Type:               apps.DeploymentProgressing,
		Status:             v1.ConditionTrue,
		Reason:             util.NewRSAvailableReason,
		LastUpdateTime:     testTime,
		LastTransitionTime: testTime,
	}
	replicaSetUpdated := apps.DeploymentCondition{
		Type:               apps.DeploymentProgressing,
		Status:             v1.ConditionTrue,
		Reason:             util.ReplicaSetUpdatedReason,
		LastUpdateTime:     testTime,
		LastTransitionTime: testTime,
	}

	tests := []struct {
		// name 是用例名；d 是 Deployment；allRSs 是全部 ReplicaSet；newRS 是最新 ReplicaSet。
		// conditionType/status/reason 是期望的条件字段；
		// lastUpdate/lastTransition 为期望的时间戳，零值表示不校验该字段。
		name            string
		d               *apps.Deployment
		allRSs          []*apps.ReplicaSet
		newRS           *apps.ReplicaSet
		conditionType   apps.DeploymentConditionType
		conditionStatus v1.ConditionStatus
		conditionReason string
		lastUpdate      metav1.Time
		lastTransition  metav1.Time
	}{
		{
			// 无进度期限时应移除 Progressing 条件，且不估算进度。
			name:   "General: remove Progressing condition and do not estimate progress if deployment has no Progress Deadline",
			d:      currentDeployment(nil, 3, 2, 2, 2, []apps.DeploymentCondition{replicaSetUpdated}),
			allRSs: []*apps.ReplicaSet{newRSWithAvailable("bar", 0, 1, 1)},
			newRS:  newRSWithAvailable("foo", 3, 2, 2),
		},
		{
			// 只有一个活跃 RS 时无需估算进度，沿用已有的 NewRSAvailable 条件。
			name:            "General: do not estimate progress of deployment with only one active ReplicaSet",
			d:               currentDeployment(&pds, 3, 3, 3, 3, []apps.DeploymentCondition{newRSAvailable}),
			allRSs:          []*apps.ReplicaSet{newRSWithAvailable("bar", 3, 3, 3)},
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.NewRSAvailableReason,
			lastUpdate:      testTime,
			lastTransition:  testTime,
		},
		{
			// 条件状态未变时不应刷新 lastTransitionTime。
			name:            "DeploymentProgressing: dont update lastTransitionTime if deployment already has Progressing=True",
			d:               currentDeployment(&pds, 3, 2, 2, 2, []apps.DeploymentCondition{replicaSetUpdated}),
			allRSs:          []*apps.ReplicaSet{newRSWithAvailable("bar", 0, 1, 1)},
			newRS:           newRSWithAvailable("foo", 3, 2, 2),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.ReplicaSetUpdatedReason,
			lastTransition:  testTime,
		},
		{
			// 从 False 转为 True 属于状态转换，应更新全部条件字段。
			name:            "DeploymentProgressing: update everything if deployment has Progressing=False",
			d:               currentDeployment(&pds, 3, 2, 2, 2, []apps.DeploymentCondition{failedTimedOut}),
			allRSs:          []*apps.ReplicaSet{newRSWithAvailable("bar", 0, 1, 1)},
			newRS:           newRSWithAvailable("foo", 3, 2, 2),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.ReplicaSetUpdatedReason,
		},
		{
			// 条件不存在时应新建 Progressing 条件。
			name:            "DeploymentProgressing: create Progressing condition if it does not exist",
			d:               currentDeployment(&pds, 3, 2, 2, 2, []apps.DeploymentCondition{}),
			allRSs:          []*apps.ReplicaSet{newRSWithAvailable("bar", 0, 1, 1)},
			newRS:           newRSWithAvailable("foo", 3, 2, 2),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.ReplicaSetUpdatedReason,
		},
		{
			// 部署完成但已是 Progressing=True 时，不刷新转换时间。
			name:            "DeploymentComplete: dont update lastTransitionTime if deployment already has Progressing=True",
			d:               currentDeployment(&pds, 3, 3, 3, 3, []apps.DeploymentCondition{replicaSetUpdated}),
			allRSs:          []*apps.ReplicaSet{},
			newRS:           newRSWithAvailable("foo", 3, 3, 3),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.NewRSAvailableReason,
			lastTransition:  testTime,
		},
		{
			// 从失败状态转为完成时，应更新全部条件字段。
			name:            "DeploymentComplete: update everything if deployment has Progressing=False",
			d:               currentDeployment(&pds, 3, 3, 3, 3, []apps.DeploymentCondition{failedTimedOut}),
			allRSs:          []*apps.ReplicaSet{},
			newRS:           newRSWithAvailable("foo", 3, 3, 3),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.NewRSAvailableReason,
		},
		{
			// 完成态下若没有 Progressing 条件，需要补建。
			name:            "DeploymentComplete: create Progressing condition if it does not exist",
			d:               currentDeployment(&pds, 3, 3, 3, 3, []apps.DeploymentCondition{}),
			allRSs:          []*apps.ReplicaSet{},
			newRS:           newRSWithAvailable("foo", 3, 3, 3),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.NewRSAvailableReason,
		},
		{
			// newRS 为 nil 时不应发生空指针解引用。
			name:            "DeploymentComplete: defend against NPE when newRS=nil",
			d:               currentDeployment(&pds, 0, 3, 3, 3, []apps.DeploymentCondition{replicaSetUpdated}),
			allRSs:          []*apps.ReplicaSet{newRSWithAvailable("foo", 0, 0, 0)},
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionTrue,
			conditionReason: util.NewRSAvailableReason,
		},
		{
			// 超过进度期限时应把 Progressing 标记为 False（TimedOut）。
			name:            "DeploymentTimedOut: update status if rollout exceeds Progress Deadline",
			d:               currentDeployment(&pds, 3, 2, 2, 2, []apps.DeploymentCondition{replicaSetUpdated}),
			allRSs:          []*apps.ReplicaSet{},
			newRS:           newRSWithAvailable("foo", 3, 2, 2),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionFalse,
			conditionReason: util.TimedOutReason,
		},
		{
			// 已有超时条件时不重复更新，保留原有时间戳。
			name:            "DeploymentTimedOut: do not update status if deployment has existing timedOut condition",
			d:               currentDeployment(&pds, 3, 2, 2, 2, []apps.DeploymentCondition{failedTimedOut}),
			allRSs:          []*apps.ReplicaSet{},
			newRS:           newRSWithAvailable("foo", 3, 2, 2),
			conditionType:   apps.DeploymentProgressing,
			conditionStatus: v1.ConditionFalse,
			conditionReason: util.TimedOutReason,
			lastUpdate:      testTime,
			lastTransition:  testTime,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := fake.Clientset{}
			dc := &DeploymentController{
				client: &fake,
			}

			// syncRolloutStatus 假定 newRS 也包含在 allRSs 中，需显式补齐。
			if test.newRS != nil {
				test.allRSs = append(test.allRSs, test.newRS)
			}
			_, ctx := ktesting.NewTestContext(t)
			err := dc.syncRolloutStatus(ctx, test.allRSs, test.newRS, test.d)
			if err != nil {
				t.Error(err)
			}

			newCond := util.GetDeploymentCondition(test.d.Status, test.conditionType)
			// 按条件是否存在、状态/原因是否匹配、时间戳是否被改动依次校验。
			switch {
			// 期望无条件的场景仅限无进度期限或无限期；其余情况缺失条件即报错。
			case newCond == nil:
				if test.d.Spec.ProgressDeadlineSeconds != nil && *test.d.Spec.ProgressDeadlineSeconds != math.MaxInt32 {
					t.Errorf("%s: expected deployment condition: %s", test.name, test.conditionType)
				}
			case newCond.Status != test.conditionStatus || newCond.Reason != test.conditionReason:
				t.Errorf("%s: DeploymentProgressing has status %s with reason %s. Expected %s with %s.", test.name, newCond.Status, newCond.Reason, test.conditionStatus, test.conditionReason)
			case !test.lastUpdate.IsZero() && test.lastUpdate != testTime:
				t.Errorf("%s: LastUpdateTime was changed to %s but expected %s;", test.name, test.lastUpdate, testTime)
			case !test.lastTransition.IsZero() && test.lastTransition != testTime:
				t.Errorf("%s: LastTransitionTime was changed to %s but expected %s;", test.name, test.lastTransition, testTime)
			}
		})
	}
}
