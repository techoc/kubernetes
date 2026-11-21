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
	"math"
	"strings"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	testclient "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/utils/ptr"
)

// TestScale 验证 Deployment 扩容与缩容时的副本分配：
// 包括按比例缩放、余数分配、失败更新以及 maxSurge 变化等场景。
func TestScale(t *testing.T) {
	newTimestamp := metav1.Date(2016, 5, 20, 2, 0, 0, 0, time.UTC)
	oldTimestamp := metav1.Date(2016, 5, 20, 1, 0, 0, 0, time.UTC)
	olderTimestamp := metav1.Date(2016, 5, 20, 0, 0, 0, 0, time.UTC)

	// updatedTemplate 生成一个模板发生改变的 Deployment，用于验证模板变更时的缩放。
	var updatedTemplate = func(replicas int32) *apps.Deployment {
		d := newDeployment("foo", replicas, nil, nil, nil, map[string]string{"foo": "bar"})
		d.Spec.Template.Labels["another"] = "label"
		return d
	}

	tests := []struct {
		// name 为测试用例名称。
		name string
		// deployment 为缩放后的目标 Deployment。
		deployment *apps.Deployment
		// oldDeployment 为缩放前的 Deployment，用于读取 maxSurge 等旧配置。
		oldDeployment *apps.Deployment

		// newRS 为最新的 ReplicaSet。
		newRS *apps.ReplicaSet
		// oldRSs 为历史 ReplicaSet 列表。
		oldRSs []*apps.ReplicaSet

		// expectedNew 为新 ReplicaSet 的期望副本数。
		expectedNew *apps.ReplicaSet
		// expectedOld 为历史 ReplicaSet 的期望副本数。
		expectedOld []*apps.ReplicaSet
		// wasntUpdated 标记预期不会被更新的 ReplicaSet，校验时跳过。
		wasntUpdated map[string]bool

		// desiredReplicasAnnotations 为指定 ReplicaSet 设置期望副本注解。
		desiredReplicasAnnotations map[string]int32
	}{
		{
			name:          "normal scaling event: 10 -> 12",
			deployment:    newDeployment("foo", 12, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 10, nil, nil, nil, nil),

			newRS:  rs("foo-v1", 10, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{},

			expectedNew: rs("foo-v1", 12, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{},
		},
		{
			name:          "normal scaling event: 10 -> 5",
			deployment:    newDeployment("foo", 5, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 10, nil, nil, nil, nil),

			newRS:  rs("foo-v1", 10, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{},

			expectedNew: rs("foo-v1", 5, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{},
		},
		{
			name:          "proportional scaling: 5 -> 10",
			deployment:    newDeployment("foo", 10, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 5, nil, nil, nil, nil),

			newRS:  rs("foo-v2", 2, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 3, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 4, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 6, nil, oldTimestamp)},
		},
		{
			name:          "proportional scaling: 5 -> 3",
			deployment:    newDeployment("foo", 3, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 5, nil, nil, nil, nil),

			newRS:  rs("foo-v2", 2, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 3, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 1, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 2, nil, oldTimestamp)},
		},
		{
			name:          "proportional scaling: 9 -> 4",
			deployment:    newDeployment("foo", 4, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 9, nil, nil, nil, nil),

			newRS:  rs("foo-v2", 8, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 1, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 4, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 0, nil, oldTimestamp)},
		},
		{
			name:          "proportional scaling: 7 -> 10",
			deployment:    newDeployment("foo", 10, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 7, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 2, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 3, nil, oldTimestamp), rs("foo-v1", 2, nil, olderTimestamp)},

			expectedNew: rs("foo-v3", 3, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 4, nil, oldTimestamp), rs("foo-v1", 3, nil, olderTimestamp)},
		},
		{
			name:          "proportional scaling: 13 -> 8",
			deployment:    newDeployment("foo", 8, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 13, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 2, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 8, nil, oldTimestamp), rs("foo-v1", 3, nil, olderTimestamp)},

			expectedNew: rs("foo-v3", 1, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 5, nil, oldTimestamp), rs("foo-v1", 2, nil, olderTimestamp)},
		},
		// 余数优先分配给最新的 ReplicaSet。
		{
			name:          "leftover distribution: 3 -> 4",
			deployment:    newDeployment("foo", 4, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 3, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 1, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 1, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},

			expectedNew: rs("foo-v3", 2, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 1, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},
		},
		// 余数优先从最旧的 ReplicaSet 扣除。
		{
			name:          "leftover distribution: 3 -> 2",
			deployment:    newDeployment("foo", 2, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 3, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 1, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 1, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},

			expectedNew: rs("foo-v3", 1, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 1, nil, oldTimestamp), rs("foo-v1", 0, nil, olderTimestamp)},
		},
		// 没有新 ReplicaSet 时，优先扩容最新的历史 ReplicaSet。
		{
			name:          "proportional scaling (no new rs): 4 -> 5",
			deployment:    newDeployment("foo", 5, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 4, nil, nil, nil, nil),

			newRS:  nil,
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 2, nil, oldTimestamp), rs("foo-v1", 2, nil, olderTimestamp)},

			expectedNew: nil,
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 3, nil, oldTimestamp), rs("foo-v1", 2, nil, olderTimestamp)},
		},
		// 缩容到零。
		{
			name:          "proportional scaling: 6 -> 0",
			deployment:    newDeployment("foo", 0, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 6, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 3, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 2, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},

			expectedNew: rs("foo-v3", 0, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 0, nil, oldTimestamp), rs("foo-v1", 0, nil, olderTimestamp)},
		},
		// 从零扩容。
		{
			name:          "proportional scaling: 0 -> 6",
			deployment:    newDeployment("foo", 6, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 6, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 0, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 0, nil, oldTimestamp), rs("foo-v1", 0, nil, olderTimestamp)},

			expectedNew:  rs("foo-v3", 6, nil, newTimestamp),
			expectedOld:  []*apps.ReplicaSet{rs("foo-v2", 0, nil, oldTimestamp), rs("foo-v1", 0, nil, olderTimestamp)},
			wasntUpdated: map[string]bool{"foo-v2": true, "foo-v1": true},
		},
		// 场景：deployment.spec.replicas 为 3，且三个 ReplicaSet 各有 1 个副本；
		// Deployment 被扩到 5，foo-v3 和 foo-v2 应各增加 1 个副本，但 foo-v2 更新失败；
		// 此时期望副本数应记录在注解中以便后续重试。
		{
			name:          "failed rs update",
			deployment:    newDeployment("foo", 5, nil, nil, nil, nil),
			oldDeployment: newDeployment("foo", 5, nil, nil, nil, nil),

			newRS:  rs("foo-v3", 2, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 1, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},

			expectedNew:  rs("foo-v3", 2, nil, newTimestamp),
			expectedOld:  []*apps.ReplicaSet{rs("foo-v2", 2, nil, oldTimestamp), rs("foo-v1", 1, nil, olderTimestamp)},
			wasntUpdated: map[string]bool{"foo-v3": true, "foo-v1": true},

			desiredReplicasAnnotations: map[string]int32{"foo-v2": int32(3)},
		},
		{
			name:          "deployment with surge pods",
			deployment:    newDeployment("foo", 20, nil, ptr.To(intstr.FromInt32(2)), nil, nil),
			oldDeployment: newDeployment("foo", 10, nil, ptr.To(intstr.FromInt32(2)), nil, nil),

			newRS:  rs("foo-v2", 6, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 6, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 11, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 11, nil, oldTimestamp)},
		},
		{
			name:          "change both surge and size",
			deployment:    newDeployment("foo", 50, nil, ptr.To(intstr.FromInt32(6)), nil, nil),
			oldDeployment: newDeployment("foo", 10, nil, ptr.To(intstr.FromInt32(3)), nil, nil),

			newRS:  rs("foo-v2", 5, nil, newTimestamp),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 8, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 22, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 34, nil, oldTimestamp)},
		},
		{
			name:          "change both size and template",
			deployment:    updatedTemplate(14),
			oldDeployment: newDeployment("foo", 10, nil, nil, nil, map[string]string{"foo": "bar"}),

			newRS:  nil,
			oldRSs: []*apps.ReplicaSet{rs("foo-v2", 7, nil, newTimestamp), rs("foo-v1", 3, nil, oldTimestamp)},

			expectedNew: nil,
			expectedOld: []*apps.ReplicaSet{rs("foo-v2", 10, nil, newTimestamp), rs("foo-v1", 4, nil, oldTimestamp)},
		},
		{
			name:          "saturated but broken new replica set does not affect old pods",
			deployment:    newDeployment("foo", 2, nil, ptr.To(intstr.FromInt32(1)), ptr.To(intstr.FromInt32(1)), nil),
			oldDeployment: newDeployment("foo", 2, nil, ptr.To(intstr.FromInt32(1)), ptr.To(intstr.FromInt32(1)), nil),

			newRS: func() *apps.ReplicaSet {
				rs := rs("foo-v2", 2, nil, newTimestamp)
				rs.Status.AvailableReplicas = 0
				return rs
			}(),
			oldRSs: []*apps.ReplicaSet{rs("foo-v1", 1, nil, oldTimestamp)},

			expectedNew: rs("foo-v2", 2, nil, newTimestamp),
			expectedOld: []*apps.ReplicaSet{rs("foo-v1", 1, nil, oldTimestamp)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_ = olderTimestamp
			t.Log(test.name)
			// 用假客户端记录 scale 产生的 UPDATE 动作。
			fake := fake.Clientset{}
			dc := &DeploymentController{
				client:        &fake,
				eventRecorder: &record.FakeRecorder{},
			}

			// 预先设置期望副本注解，模拟真实 controller 在缩放前写入的状态；
			// scale 逻辑会从这些注解读取上次的目标副本数。
			if test.newRS != nil {
				desiredReplicas := *(test.oldDeployment.Spec.Replicas)
				if desired, ok := test.desiredReplicasAnnotations[test.newRS.Name]; ok {
					desiredReplicas = desired
				}
				deploymentutil.SetReplicasAnnotations(test.newRS, desiredReplicas, desiredReplicas+deploymentutil.MaxSurge(*test.oldDeployment))
			}
			for i := range test.oldRSs {
				rs := test.oldRSs[i]
				// 用例允许传入 nil 表示对应位置没有 ReplicaSet，跳过即可。
				if rs == nil {
					continue
				}
				desiredReplicas := *(test.oldDeployment.Spec.Replicas)
				if desired, ok := test.desiredReplicasAnnotations[rs.Name]; ok {
					desiredReplicas = desired
				}
				deploymentutil.SetReplicasAnnotations(rs, desiredReplicas, desiredReplicas+deploymentutil.MaxSurge(*test.oldDeployment))
			}

			_, ctx := ktesting.NewTestContext(t)

			if err := dc.scale(ctx, test.deployment, test.newRS, test.oldRSs); err != nil {
				t.Errorf("%s: unexpected error: %v", test.name, err)
				return
			}

			// nameToSize 汇总 scale 之后各 ReplicaSet 的副本数
			// 对预期未被更新的 ReplicaSet 跳过校验，
			// 因为客户端不会收到对应的 UPDATE 动作。
			nameToSize := make(map[string]int32)
			if test.newRS != nil {
				nameToSize[test.newRS.Name] = *(test.newRS.Spec.Replicas)
			}
			for i := range test.oldRSs {
				rs := test.oldRSs[i]
				nameToSize[rs.Name] = *(rs.Spec.Replicas)
			}
			// 从 UPDATE 动作中提取更新后的副本数，覆盖初始记录
			for _, action := range fake.Actions() {
				rs := action.(testclient.UpdateAction).GetObject().(*apps.ReplicaSet)
				if !test.wasntUpdated[rs.Name] {
					nameToSize[rs.Name] = *(rs.Spec.Replicas)
				}
			}

			if test.expectedNew != nil && test.newRS != nil && *(test.expectedNew.Spec.Replicas) != nameToSize[test.newRS.Name] {
				t.Errorf("%s: expected new replicas: %d, got: %d", test.name, *(test.expectedNew.Spec.Replicas), nameToSize[test.newRS.Name])
				return
			}
			if len(test.expectedOld) != len(test.oldRSs) {
				t.Errorf("%s: expected %d old replica sets, got %d", test.name, len(test.expectedOld), len(test.oldRSs))
				return
			}
			for n := range test.oldRSs {
				rs := test.oldRSs[n]
				expected := test.expectedOld[n]
				if *(expected.Spec.Replicas) != nameToSize[rs.Name] {
					t.Errorf("%s: expected old (%s) replicas: %d, got: %d", test.name, rs.Name, *(expected.Spec.Replicas), nameToSize[rs.Name])
				}
			}
		})
	}
}

// TestDeploymentController_cleanupDeployment 验证清理逻辑会删除超出历史限制的旧 ReplicaSet，
// 同时只清理副本数已归零且尚未开始删除的对象。
func TestDeploymentController_cleanupDeployment(t *testing.T) {
	selector := map[string]string{"foo": "bar"}
	alreadyDeleted := newRSWithStatus("foo-1", 0, 0, selector)
	now := metav1.Now()
	alreadyDeleted.DeletionTimestamp = &now

	tests := []struct {
		// oldRSs 为待清理的历史 ReplicaSet。
		oldRSs []*apps.ReplicaSet
		// revisionHistoryLimit 为保留的历史版本数量。
		revisionHistoryLimit int32
		// expectedDeletions 为期望的删除动作数量。
		expectedDeletions int
	}{
		{
			oldRSs: []*apps.ReplicaSet{
				newRSWithStatus("foo-1", 0, 0, selector),
				newRSWithStatus("foo-2", 0, 0, selector),
				newRSWithStatus("foo-3", 0, 0, selector),
			},
			revisionHistoryLimit: 1,
			expectedDeletions:    2,
		},
		{
			// 只删除 Spec.Replicas 与 Status.Replicas 都为 0 的 ReplicaSet。
			oldRSs: []*apps.ReplicaSet{
				newRSWithStatus("foo-1", 0, 0, selector),
				newRSWithStatus("foo-2", 0, 1, selector),
				newRSWithStatus("foo-3", 1, 0, selector),
				newRSWithStatus("foo-4", 1, 1, selector),
			},
			revisionHistoryLimit: 0,
			expectedDeletions:    1,
		},

		{
			oldRSs: []*apps.ReplicaSet{
				newRSWithStatus("foo-1", 0, 0, selector),
				newRSWithStatus("foo-2", 0, 0, selector),
			},
			revisionHistoryLimit: 0,
			expectedDeletions:    2,
		},
		{
			oldRSs: []*apps.ReplicaSet{
				newRSWithStatus("foo-1", 1, 1, selector),
				newRSWithStatus("foo-2", 1, 1, selector),
			},
			revisionHistoryLimit: 0,
			expectedDeletions:    0,
		},
		{
			oldRSs: []*apps.ReplicaSet{
				alreadyDeleted,
			},
			revisionHistoryLimit: 0,
			expectedDeletions:    0,
		},
		{
			// 历史版本上限无限大时不应删除任何 ReplicaSet。
			oldRSs: []*apps.ReplicaSet{
				newRSWithStatus("foo-1", 0, 0, selector),
				newRSWithStatus("foo-2", 0, 0, selector),
				newRSWithStatus("foo-3", 0, 0, selector),
			},
			revisionHistoryLimit: math.MaxInt32,
			expectedDeletions:    0,
		},
	}

	for i := range tests {
		test := tests[i]
		t.Logf("scenario %d", i)

		_, ctx := ktesting.NewTestContext(t)

		fake := &fake.Clientset{}
		informers := informers.NewSharedInformerFactory(fake, controller.NoResyncPeriodFunc())
		controller, err := NewDeploymentController(ctx, informers.Apps().V1().Deployments(), informers.Apps().V1().ReplicaSets(), informers.Core().V1().Pods(), fake)
		if err != nil {
			t.Fatalf("error creating Deployment controller: %v", err)
		}

		controller.eventRecorder = &record.FakeRecorder{}
		// lister 的同步探针直接置为 ready，避免为单测启动完整 informer 同步流程。
		controller.dListerSynced = alwaysReady
		controller.rsListerSynced = alwaysReady
		controller.podListerSynced = alwaysReady
		// 把用例中的历史 RS 灌入 informer 索引，cleanupDeployment 依赖 lister 读取它们。
		for _, rs := range test.oldRSs {
			informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
		}

		stopCh := make(chan struct{})
		defer close(stopCh)
		informers.Start(stopCh)
		informers.WaitForCacheSync(stopCh)

		t.Logf(" &test.revisionHistoryLimit: %d", test.revisionHistoryLimit)
		d := newDeployment("foo", 1, &test.revisionHistoryLimit, nil, nil, map[string]string{"foo": "bar"})
		controller.cleanupDeployment(ctx, test.oldRSs, d)

		gotDeletions := 0
		for _, action := range fake.Actions() {
			if action.GetVerb() == "delete" {
				gotDeletions++
			}
		}
		if gotDeletions != test.expectedDeletions {
			t.Errorf("expect %v old replica sets been deleted, but got %v", test.expectedDeletions, gotDeletions)
			continue
		}
	}
}

// TestDeploymentController_cleanupDeploymentOrder 验证清理顺序：
// 优先按修订版本号删除旧 ReplicaSet，缺失修订版本或仍有副本的对象会被跳过。
func TestDeploymentController_cleanupDeploymentOrder(t *testing.T) {
	selector := map[string]string{"foo": "bar"}
	now := metav1.Now()
	duration := time.Minute

	// newRSWithRevisionAndCreationTimestamp 构造带创建时间和修订版本注解的 ReplicaSet。
	newRSWithRevisionAndCreationTimestamp := func(name string, replicas int32, selector map[string]string, timestamp time.Time, revision string) *apps.ReplicaSet {
		rs := rs(name, replicas, selector, metav1.NewTime(timestamp))
		if revision != "" {
			rs.Annotations = map[string]string{
				deploymentutil.RevisionAnnotation: revision,
			}
		}
		rs.Status = apps.ReplicaSetStatus{
			Replicas: int32(replicas),
		}
		return rs
	}

	// 所有用例中创建时间顺序固定为 rs1 < rs2 < rs3 < rs4，以便单独验证修订版本的影响。
	tests := []struct {
		// oldRSs 为待清理的历史 ReplicaSet。
		oldRSs []*apps.ReplicaSet
		// revisionHistoryLimit 为保留的历史版本数量。
		revisionHistoryLimit int32
		// expectedDeletedRSs 为期望被删除的 ReplicaSet 名称集合。
		expectedDeletedRSs sets.String
	}{
		{
			// 修订版本顺序 rs1 < rs2，应删除 rs1
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 0, selector, now.Add(-1*duration), "1"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, "2"),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString("foo-1"),
		},
		{
			// 修订版本顺序 rs2 < rs1，应删除 rs2
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 0, selector, now.Add(-1*duration), "2"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, "1"),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString("foo-2"),
		},
		{
			// rs1 有修订版本而 rs2 没有，应删除 rs2
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 0, selector, now.Add(-1*duration), "1"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, ""),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString("foo-2"),
		},
		{
			// rs1 没有修订版本而 rs2 有，应删除 rs1
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 0, selector, now.Add(-1*duration), ""),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, "2"),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString("foo-1"),
		},
		{
			// 修订版本 rs1 < rs2 < rs3，但 rs1 仍有副本，应删除 rs2
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 1, selector, now.Add(-1*duration), "1"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, "2"),
				newRSWithRevisionAndCreationTimestamp("foo-3", 0, selector, now.Add(duration), "3"),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString("foo-2"),
		},
		{
			// 修订版本 rs1 < rs2 < rs3，rs1 和 rs2 都有副本，不应删除
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 1, selector, now.Add(-1*duration), "1"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 1, selector, now.Time, "2"),
				newRSWithRevisionAndCreationTimestamp("foo-3", 0, selector, now.Add(duration), "3"),
			},
			revisionHistoryLimit: 1,
			expectedDeletedRSs:   sets.NewString(),
		},
		{
			// 修订版本 rs2 < rs4 < rs1 < rs3，应删除 rs2 和 rs4
			oldRSs: []*apps.ReplicaSet{
				newRSWithRevisionAndCreationTimestamp("foo-1", 0, selector, now.Add(-1*duration), "3"),
				newRSWithRevisionAndCreationTimestamp("foo-2", 0, selector, now.Time, "1"),
				newRSWithRevisionAndCreationTimestamp("foo-3", 0, selector, now.Add(duration), "4"),
				newRSWithRevisionAndCreationTimestamp("foo-4", 0, selector, now.Add(2*duration), "2"),
			},
			revisionHistoryLimit: 2,
			expectedDeletedRSs:   sets.NewString("foo-2", "foo-4"),
		},
	}

	for i := range tests {
		test := tests[i]
		t.Logf("scenario %d", i)

		_, ctx := ktesting.NewTestContext(t)

		fake := &fake.Clientset{}
		informers := informers.NewSharedInformerFactory(fake, controller.NoResyncPeriodFunc())
		controller, err := NewDeploymentController(ctx, informers.Apps().V1().Deployments(), informers.Apps().V1().ReplicaSets(), informers.Core().V1().Pods(), fake)
		if err != nil {
			t.Fatalf("error creating Deployment controller: %v", err)
		}

		controller.eventRecorder = &record.FakeRecorder{}
		// lister 的同步探针直接置为 ready，避免为单测启动完整 informer 同步流程。
		controller.dListerSynced = alwaysReady
		controller.rsListerSynced = alwaysReady
		controller.podListerSynced = alwaysReady
		// 把用例中的历史 RS 灌入 informer 索引，cleanupDeployment 依赖 lister 读取它们。
		for _, rs := range test.oldRSs {
			informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
		}

		stopCh := make(chan struct{})
		defer close(stopCh)
		informers.Start(stopCh)

		d := newDeployment("foo", 1, &test.revisionHistoryLimit, nil, nil, map[string]string{"foo": "bar"})
		controller.cleanupDeployment(ctx, test.oldRSs, d)

		deletedRSs := sets.String{}
		for _, action := range fake.Actions() {
			deleteAction, ok := action.(testclient.DeleteActionImpl)
			if !ok {
				t.Logf("Found not-delete action with verb %v. Ignoring.", action.GetVerb())
				continue
			}

			if deleteAction.GetResource().Resource != "replicasets" {
				continue
			}

			deletedRSs.Insert(deleteAction.GetName())
		}
		t.Logf("&test.revisionHistoryLimit: %d, &test.deletedReplicaSets: %v", test.revisionHistoryLimit, deletedRSs)

		if !test.expectedDeletedRSs.Equal(deletedRSs) {
			t.Errorf("expect to delete old replica sets %v, but got %v", test.expectedDeletedRSs, deletedRSs)
			continue
		}
	}
}

// TestDeploymentController_generateReplicaSetName 验证新 ReplicaSet 的名称生成：
// Deployment 名称会被截断以给哈希后缀留出空间，且总长度不超过 DNS 子域限制。
func TestDeploymentController_generateReplicaSetName(t *testing.T) {
	tests := []struct {
		// name 为测试用例名称。
		name string
		// deploymentName 为输入的 Deployment 名称。
		deploymentName string
		// wantDeploymentPortion 为期望名称中 Deployment 部分的内容。
		wantDeploymentPortion string
	}{
		{
			name:                  "short name",
			deploymentName:        "my-deployment",
			wantDeploymentPortion: "my-deployment",
		},
		{
			name:                  "very long name truncated",
			deploymentName:        strings.Repeat("a", 250),
			wantDeploymentPortion: strings.Repeat("a", 242),
		},
		{
			name:                  "very long name not truncated",
			deploymentName:        strings.Repeat("a", 242),
			wantDeploymentPortion: strings.Repeat("a", 242),
		},
	}

	for _, test := range tests {
		_, ctx := ktesting.NewTestContext(t)

		fake := &fake.Clientset{}
		informers := informers.NewSharedInformerFactory(fake, controller.NoResyncPeriodFunc())
		controller, err := NewDeploymentController(ctx, informers.Apps().V1().Deployments(), informers.Apps().V1().ReplicaSets(), informers.Core().V1().Pods(), fake)
		if err != nil {
			t.Fatalf("error creating Deployment controller: %v", err)
		}

		controller.eventRecorder = &record.FakeRecorder{}
		controller.dListerSynced = alwaysReady
		controller.rsListerSynced = alwaysReady
		controller.podListerSynced = alwaysReady

		stopCh := make(chan struct{})
		defer close(stopCh)
		informers.Start(stopCh)

		d := newDeployment(test.deploymentName, 1, nil, nil, nil, map[string]string{"foo": "bar"})

		if _, err := controller.getNewReplicaSet(ctx, d, []*apps.ReplicaSet{}, []*apps.ReplicaSet{}, true); err != nil {
			t.Errorf("failed to create new ReplicaSet: %v", err)
			return
		}

		// 生成的 RS 名称无法直接调用内部函数得到，只能从假客户端记录的 CREATE 动作中反推。
		rsName := ""
		for _, action := range fake.Actions() {
			if createAction, ok := action.(testclient.CreateAction); ok {
				if createdRS, ok := createAction.GetObject().(*apps.ReplicaSet); ok {
					if createdRS.Name != "" {
						rsName = createdRS.Name
						break
					}
				}
			}
		}

		if len(rsName) > validation.DNS1123SubdomainMaxLength {
			t.Errorf("ReplicaSet name length %d, want <= %d", len(rsName), validation.DNS1123SubdomainMaxLength)
		}

		parts := strings.Split(rsName, "-")
		if len(parts) < 2 {
			t.Errorf("ReplicaSet name should contain at least one hyphen separator")
		}

		deploymentPortion := strings.Join(parts[:len(parts)-1], "-")
		if len(test.deploymentName) <= 242 {
			if len(deploymentPortion) != len(test.deploymentName) {
				t.Errorf("Deployment name portion should be %d chars, got %d", len(test.deploymentName), len(deploymentPortion))
			}
		} else {
			if len(deploymentPortion) != 242 {
				t.Errorf("Truncated deployment name should be 242 chars, got %d", len(deploymentPortion))
			}
		}

		if deploymentPortion != test.wantDeploymentPortion {
			t.Errorf("Deployment name portion mismatch: got %q, want %q", deploymentPortion, test.wantDeploymentPortion)
		}
	}
}

// TestGenerateReplicaSetName 验证 generateReplicaSetName 在各种名称与哈希长度下的拼接结果。
func TestGenerateReplicaSetName(t *testing.T) {
	tests := []struct {
		// name 为测试用例名称。
		name string
		// deploymentName 为输入的 Deployment 名称。
		deploymentName string
		// hash 为输入的 pod-template-hash。
		hash string
		// want 为期望生成的 ReplicaSet 名称。
		want string
	}{
		{
			name:           "short name",
			deploymentName: "my-deployment",
			hash:           "abcde12345",
			want:           "my-deployment-abcde12345",
		},
		{
			name:           "maximum length without truncating",
			deploymentName: strings.Repeat("a", 242),
			hash:           "abcde12345",
			want:           strings.Repeat("a", 242) + "-abcde12345",
		},
		{
			name:           "very long deployment name is truncated",
			deploymentName: strings.Repeat("b", 250),
			hash:           "abcde12345",
			want:           strings.Repeat("b", 242) + "-abcde12345",
		},
		{
			name:           "very long hash is not truncated",
			deploymentName: strings.Repeat("d", 252),
			hash:           strings.Repeat("h", 252),
			want:           strings.Repeat("d", 252) + "-" + strings.Repeat("h", 252),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := generateReplicaSetName(test.deploymentName, test.hash)

			if got != test.want {
				t.Errorf("generateReplicaSetName(%q, %q) = %q, want %q", test.deploymentName, test.hash, got, test.want)
			}
		})
	}
}
