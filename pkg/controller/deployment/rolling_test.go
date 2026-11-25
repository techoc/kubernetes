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
	"context"
	"testing"

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
)

// TestDeploymentController_reconcileNewReplicaSet 验证滚动更新时新 ReplicaSet 的扩缩容判断：
// 依据 maxSurge 与当前副本数决定是否扩容，以及副本数超出目标时是否缩回。
func TestDeploymentController_reconcileNewReplicaSet(t *testing.T) {
	tests := []struct {
		deploymentReplicas  int32
		maxSurge            intstr.IntOrString
		oldReplicas         int32
		newReplicas         int32
		scaleExpected       bool
		expectedNewReplicas int32
	}{
		{
			// 没有 surge 余量时不扩容。
			deploymentReplicas: 10,
			maxSurge:           intstr.FromInt32(0),
			oldReplicas:        10,
			newReplicas:        0,
			scaleExpected:      false,
		},
		{
			deploymentReplicas:  10,
			maxSurge:            intstr.FromInt32(2),
			oldReplicas:         10,
			newReplicas:         0,
			scaleExpected:       true,
			expectedNewReplicas: 2,
		},
		{
			deploymentReplicas:  10,
			maxSurge:            intstr.FromInt32(2),
			oldReplicas:         5,
			newReplicas:         0,
			scaleExpected:       true,
			expectedNewReplicas: 7,
		},
		{
			deploymentReplicas: 10,
			maxSurge:           intstr.FromInt32(2),
			oldReplicas:        10,
			newReplicas:        2,
			scaleExpected:      false,
		},
		{
			// 新副本数超过期望值时缩回目标副本数。
			deploymentReplicas:  10,
			maxSurge:            intstr.FromInt32(2),
			oldReplicas:         2,
			newReplicas:         11,
			scaleExpected:       true,
			expectedNewReplicas: 10,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Logf("executing scenario %d", i)
		newRS := rs("foo-v2", test.newReplicas, nil, noTimestamp)
		oldRS := rs("foo-v2", test.oldReplicas, nil, noTimestamp)
		allRSs := []*apps.ReplicaSet{newRS, oldRS}
		maxUnavailable := intstr.FromInt32(0)
		deployment := newDeployment("foo", test.deploymentReplicas, nil, &test.maxSurge, &maxUnavailable, map[string]string{"foo": "bar"})
		fake := fake.Clientset{}
		controller := &DeploymentController{
			client:        &fake,
			eventRecorder: &record.FakeRecorder{},
		}
		_, ctx := ktesting.NewTestContext(t)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		scaled, err := controller.reconcileNewReplicaSet(ctx, allRSs, newRS, deployment)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		// 期望不扩缩容时，返回值和 fake 客户端动作都必须为空，防止偷偷发起写请求。
		if !test.scaleExpected {
			if scaled || len(fake.Actions()) > 0 {
				t.Errorf("unexpected scaling: %v", fake.Actions())
			}
			continue
		}
		if test.scaleExpected && !scaled {
			t.Errorf("expected scaling to occur")
			continue
		}
		if len(fake.Actions()) != 1 {
			t.Errorf("expected 1 action during scale, got: %v", fake.Actions())
			continue
		}
		updated := fake.Actions()[0].(core.UpdateAction).GetObject().(*apps.ReplicaSet)
		if e, a := test.expectedNewReplicas, *(updated.Spec.Replicas); e != a {
			t.Errorf("expected update to %d replicas, got %d", e, a)
		}
	}
}

// TestDeploymentController_reconcileOldReplicaSets 验证滚动更新时旧 ReplicaSet 的缩容判断，
// 覆盖按 maxUnavailable 缩容、清理不健康副本，以及新副本不可用时停止进一步缩容的场景。
func TestDeploymentController_reconcileOldReplicaSets(t *testing.T) {
	tests := []struct {
		deploymentReplicas  int32
		maxUnavailable      intstr.IntOrString
		oldReplicas         int32
		newReplicas         int32
		readyPodsFromOldRS  int
		readyPodsFromNewRS  int
		scaleExpected       bool
		expectedOldReplicas int32
	}{
		{
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(0),
			oldReplicas:         10,
			newReplicas:         0,
			readyPodsFromOldRS:  10,
			readyPodsFromNewRS:  0,
			scaleExpected:       true,
			expectedOldReplicas: 9,
		},
		{
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(2),
			oldReplicas:         10,
			newReplicas:         0,
			readyPodsFromOldRS:  10,
			readyPodsFromNewRS:  0,
			scaleExpected:       true,
			expectedOldReplicas: 8,
		},
		{ // 期望清理旧 ReplicaSet 中不健康的副本
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(2),
			oldReplicas:         10,
			newReplicas:         0,
			readyPodsFromOldRS:  8,
			readyPodsFromNewRS:  0,
			scaleExpected:       true,
			expectedOldReplicas: 8,
		},
		{ // 期望清理 1 个不健康副本，同时缩掉 1 个就绪 Pod
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(2),
			oldReplicas:         10,
			newReplicas:         0,
			readyPodsFromOldRS:  9,
			readyPodsFromNewRS:  0,
			scaleExpected:       true,
			expectedOldReplicas: 8,
		},
		{ // 新 ReplicaSet 中存在不可用 Pod 时，不应继续推进旧 ReplicaSet 的缩容
			deploymentReplicas: 10,
			maxUnavailable:     intstr.FromInt32(2),
			oldReplicas:        8,
			newReplicas:        2,
			readyPodsFromOldRS: 8,
			readyPodsFromNewRS: 0,
			scaleExpected:      false,
		},
	}
	for i := range tests {
		test := tests[i]
		t.Logf("executing scenario %d", i)

		newSelector := map[string]string{"foo": "new"}
		oldSelector := map[string]string{"foo": "old"}
		newRS := rs("foo-new", test.newReplicas, newSelector, noTimestamp)
		// 把就绪副本数写入 Status，模拟控制器从状态中读取的可用副本信息。
		newRS.Status.AvailableReplicas = int32(test.readyPodsFromNewRS)
		oldRS := rs("foo-old", test.oldReplicas, oldSelector, noTimestamp)
		oldRS.Status.AvailableReplicas = int32(test.readyPodsFromOldRS)
		oldRSs := []*apps.ReplicaSet{oldRS}
		allRSs := []*apps.ReplicaSet{oldRS, newRS}
		maxSurge := intstr.FromInt32(0)
		deployment := newDeployment("foo", test.deploymentReplicas, nil, &maxSurge, &test.maxUnavailable, newSelector)
		fakeClientset := fake.Clientset{}
		controller := &DeploymentController{
			client:        &fakeClientset,
			eventRecorder: &record.FakeRecorder{},
		}
		_, ctx := ktesting.NewTestContext(t)
		scaled, err := controller.reconcileOldReplicaSets(ctx, allRSs, oldRSs, newRS, deployment)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		// 缩容标记是直接返回值，不依赖 fake 客户端动作，这里只验证决策结果。
		if !test.scaleExpected && scaled {
			t.Errorf("unexpected scaling: %v", fakeClientset.Actions())
		}
		if test.scaleExpected && !scaled {
			t.Errorf("expected scaling to occur")
			continue
		}
		continue
	}
}

// TestDeploymentController_cleanupUnhealthyReplicas 验证按允许的清理上限缩容旧 ReplicaSet 中的不健康副本，
// 确保实际清理数不超过 maxCleanupCount，也不超过真实的不健康副本数。
func TestDeploymentController_cleanupUnhealthyReplicas(t *testing.T) {
	tests := []struct {
		oldReplicas          int32
		readyPods            int
		unHealthyPods        int
		maxCleanupCount      int
		cleanupCountExpected int
	}{
		{
			oldReplicas:          10,
			readyPods:            8,
			unHealthyPods:        2,
			maxCleanupCount:      1,
			cleanupCountExpected: 1,
		},
		{
			oldReplicas:          10,
			readyPods:            8,
			unHealthyPods:        2,
			maxCleanupCount:      3,
			cleanupCountExpected: 2,
		},
		{
			oldReplicas:          10,
			readyPods:            8,
			unHealthyPods:        2,
			maxCleanupCount:      0,
			cleanupCountExpected: 0,
		},
		{
			oldReplicas:          10,
			readyPods:            10,
			unHealthyPods:        0,
			maxCleanupCount:      3,
			cleanupCountExpected: 0,
		},
	}

	for i, test := range tests {
		t.Logf("executing scenario %d", i)
		oldRS := rs("foo-v2", test.oldReplicas, nil, noTimestamp)
		oldRS.Status.AvailableReplicas = int32(test.readyPods)
		oldRSs := []*apps.ReplicaSet{oldRS}
		maxSurge := intstr.FromInt32(2)
		maxUnavailable := intstr.FromInt32(2)
		deployment := newDeployment("foo", 10, nil, &maxSurge, &maxUnavailable, nil)
		fakeClientset := fake.Clientset{}

		controller := &DeploymentController{
			client:        &fakeClientset,
			eventRecorder: &record.FakeRecorder{},
		}
		_, ctx := ktesting.NewTestContext(t)
		_, cleanupCount, err := controller.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, int32(test.maxCleanupCount))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		if int(cleanupCount) != test.cleanupCountExpected {
			t.Errorf("expected %v unhealthy replicas been cleaned up, got %v", test.cleanupCountExpected, cleanupCount)
			continue
		}
	}
}

// TestDeploymentController_scaleDownOldReplicaSetsForRollingUpdate 验证滚动更新时旧 ReplicaSet 的缩容上限与边界条件。
func TestDeploymentController_scaleDownOldReplicaSetsForRollingUpdate(t *testing.T) {
	tests := []struct {
		deploymentReplicas  int32
		maxUnavailable      intstr.IntOrString
		readyPods           int
		oldReplicas         int32
		scaleExpected       bool
		expectedOldReplicas int32
	}{
		{
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(0),
			readyPods:           10,
			oldReplicas:         10,
			scaleExpected:       true,
			expectedOldReplicas: 9,
		},
		{
			deploymentReplicas:  10,
			maxUnavailable:      intstr.FromInt32(2),
			readyPods:           10,
			oldReplicas:         10,
			scaleExpected:       true,
			expectedOldReplicas: 8,
		},
		{
			deploymentReplicas: 10,
			maxUnavailable:     intstr.FromInt32(2),
			readyPods:          8,
			oldReplicas:        10,
			scaleExpected:      false,
		},
		{
			deploymentReplicas: 10,
			maxUnavailable:     intstr.FromInt32(2),
			readyPods:          10,
			oldReplicas:        0,
			scaleExpected:      false,
		},
		{
			deploymentReplicas: 10,
			maxUnavailable:     intstr.FromInt32(2),
			readyPods:          1,
			oldReplicas:        10,
			scaleExpected:      false,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Logf("executing scenario %d", i)
		oldRS := rs("foo-v2", test.oldReplicas, nil, noTimestamp)
		oldRS.Status.AvailableReplicas = int32(test.readyPods)
		allRSs := []*apps.ReplicaSet{oldRS}
		oldRSs := []*apps.ReplicaSet{oldRS}
		maxSurge := intstr.FromInt32(0)
		deployment := newDeployment("foo", test.deploymentReplicas, nil, &maxSurge, &test.maxUnavailable, map[string]string{"foo": "bar"})
		fakeClientset := fake.Clientset{}
		controller := &DeploymentController{
			client:        &fakeClientset,
			eventRecorder: &record.FakeRecorder{},
		}
		_, ctx := ktesting.NewTestContext(t)
		scaled, err := controller.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, deployment)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		if !test.scaleExpected {
			if scaled != 0 {
				t.Errorf("unexpected scaling: %v", fakeClientset.Actions())
			}
			continue
		}
		if test.scaleExpected && scaled == 0 {
			t.Errorf("expected scaling to occur; actions: %v", fakeClientset.Actions())
			continue
		}
		// 操作记录中同时包含 list 和 update，提取 update 操作进行校验。
		var updateAction core.UpdateAction
		for _, action := range fakeClientset.Actions() {
			switch a := action.(type) {
			case core.UpdateAction:
				if updateAction != nil {
					t.Errorf("expected only 1 update action; had %v and found %v", updateAction, a)
				} else {
					updateAction = a
				}
			}
		}
		if updateAction == nil {
			t.Errorf("expected an update action")
			continue
		}
		updated := updateAction.GetObject().(*apps.ReplicaSet)
		if e, a := test.expectedOldReplicas, *(updated.Spec.Replicas); e != a {
			t.Errorf("expected update to %d replicas, got %d", e, a)
		}
	}
}
