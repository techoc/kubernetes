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

package statefulset

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/onsi/gomega"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// parentKind 是 ControllerRevision 所属父对象的 GVK。
var parentKind = apps.SchemeGroupVersion.WithKind("StatefulSet")

// alwaysReady 作为 informer 同步检查，测试中缓存视为已同步。
func alwaysReady() bool { return true }

// TestStatefulSetControllerCreates 验证控制器扩容后状态中的副本数正确。
func TestStatefulSetControllerCreates(t *testing.T) {
	set := newStatefulSet(3)
	logger, ctx := ktesting.NewTestContext(t)
	ssc, spc, om, _ := newFakeStatefulSetController(ctx, set)
	if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 3 {
		t.Errorf("set.Status.Replicas = %v; want 3", set.Status.Replicas)
	}
}

// TestStatefulSetControllerDeletes 验证控制器缩容到零后状态中的副本数正确。
func TestStatefulSetControllerDeletes(t *testing.T) {
	set := newStatefulSet(3)
	logger, ctx := ktesting.NewTestContext(t)
	ssc, spc, om, _ := newFakeStatefulSetController(ctx, set)
	if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 3 {
		t.Errorf("set.Status.Replicas = %v; want 3", set.Status.Replicas)
	}
	*set.Spec.Replicas = 0
	if err := scaleDownStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn down StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 0 {
		t.Errorf("set.Status.Replicas = %v; want 0", set.Status.Replicas)
	}
}

// TestStatefulSetControllerRespectsTermination 验证终止中的 Pod 不被重建或计入副本数。
func TestStatefulSetControllerRespectsTermination(t *testing.T) {
	set := newStatefulSet(3)
	logger, ctx := ktesting.NewTestContext(t)
	ssc, spc, om, _ := newFakeStatefulSetController(ctx, set)
	if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 3 {
		t.Errorf("set.Status.Replicas = %v; want 3", set.Status.Replicas)
	}
	// 终止中的 Pod 仍在 lister 中，恢复时不能被当作缺失副本重建。
	_, err := om.addTerminatingPod(set, 3)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.addTerminatingPod(set, 4)
	if err != nil {
		t.Error(err)
	}
	ssc.syncStatefulSet(ctx, set, pods)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if len(pods) != 5 {
		t.Error("StatefulSet does not respect termination")
	}
	sort.Sort(ascendingOrdinal(pods))
	spc.DeleteStatefulPod(set, pods[3])
	spc.DeleteStatefulPod(set, pods[4])
	*set.Spec.Replicas = 0
	if err := scaleDownStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn down StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 0 {
		t.Errorf("set.Status.Replicas = %v; want 0", set.Status.Replicas)
	}
}

// TestStatefulSetControllerBlocksScaling 验证有 Pod 终止中时扩容被阻塞，清理后恢复。
func TestStatefulSetControllerBlocksScaling(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	set := newStatefulSet(3)
	ssc, spc, om, _ := newFakeStatefulSetController(ctx, set)
	if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
		t.Errorf("Failed to turn up StatefulSet : %s", err)
	}
	if obj, _, err := om.setsIndexer.Get(set); err != nil {
		t.Error(err)
	} else {
		set = obj.(*apps.StatefulSet)
	}
	if set.Status.Replicas != 3 {
		t.Errorf("set.Status.Replicas = %v; want 3", set.Status.Replicas)
	}
	*set.Spec.Replicas = 5
	// 递增 resourceVersion 以模拟 API server 的真实更新，否则事件过滤可能吞掉变更。
	fakeResourceVersion(set)
	om.setsIndexer.Update(set)
	_, err := om.setPodTerminated(set, 0)
	if err != nil {
		t.Error("Failed to set pod terminated at ordinal 0")
	}
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Error(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if len(pods) != 3 {
		t.Error("StatefulSet does not block scaling")
	}
	sort.Sort(ascendingOrdinal(pods))
	spc.DeleteStatefulPod(set, pods[0])
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)
	pods, err = om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Error(err)
	}
	if len(pods) != 3 {
		t.Error("StatefulSet does not resume when terminated Pod is removed")
	}
}

// TestStatefulSetControllerDeletionTimestamp 验证已标记删除的 StatefulSet 不再创建 Pod。
func TestStatefulSetControllerDeletionTimestamp(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	set := newStatefulSet(3)
	set.DeletionTimestamp = new(metav1.Time)
	ssc, _, om, _ := newFakeStatefulSetController(ctx, set)

	om.setsIndexer.Add(set)

	// 主动触发同步，确认带删除时间戳的 StatefulSet 不会创建任何 Pod。
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(pods), 0; got != want {
		t.Errorf("len(pods) = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerDeletionTimestampRace 验证客户端与缓存对删除状态判断不一致时不收养孤儿资源。
func TestStatefulSetControllerDeletionTimestampRace(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	set := newStatefulSet(3)
	// 裸客户端认为对象已标记删除。
	set.DeletionTimestamp = new(metav1.Time)
	ssc, _, om, ssh := newFakeStatefulSetController(ctx, set)

	// lister（缓存）中对象仍未标记删除，制造竞态窗口。
	set2 := *set
	set2.DeletionTimestamp = nil
	om.setsIndexer.Add(&set2)

	// 竞态复核时存在匹配的孤儿 Pod 和 Revision。
	pod := newStatefulSetPod(set, 1)
	pod.OwnerReferences = nil
	om.podsIndexer.Add(pod)
	set.Status.CollisionCount = new(int32)
	revision, err := newRevision(set, 1, set.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	revision.OwnerReferences = nil
	_, err = ssh.CreateControllerRevision(set, revision, set.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}

	// 主动触发同步：孤儿 Pod 与 Revision 应留在原地。
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)

	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(pods), 1; got != want {
		t.Errorf("len(pods) = %v, want %v", got, want)
	}

	// 不应给孤儿 Pod 添加 ownerReference。
	for _, pod := range pods {
		if len(pod.OwnerReferences) > 0 {
			t.Errorf("unexpected pod owner references: %v", pod.OwnerReferences)
		}
	}

	// 同理，孤儿 Revision 也不应被收养。
	revisions, err := ssh.ListControllerRevisions(set, parentKind, selector)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(revisions), 1; got != want {
		t.Errorf("len(revisions) = %v, want %v", got, want)
	}
	for _, revision := range revisions {
		if len(revision.OwnerReferences) > 0 {
			t.Errorf("unexpected revision owner references: %v", revision.OwnerReferences)
		}
	}
}

// TestStatefulSetControllerAddPod 验证 Pod 添加事件会把所属 StatefulSet 入队。
func TestStatefulSetControllerAddPod(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	pod1 := newStatefulSetPod(set1, 0)
	pod2 := newStatefulSetPod(set2, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)

	// addPod 应根据 Pod 的 ownerReference 找到所属 Set 并入队其 key。
	ssc.addPod(logger, pod1)
	key, done := ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set1); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}
	ssc.queue.Done(key)

	ssc.addPod(logger, pod2)
	key, done = ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set2); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}
	ssc.queue.Done(key)
}

// TestStatefulSetControllerAddPodOrphan 验证孤儿 Pod 会入队所有标签匹配的 StatefulSet。
func TestStatefulSetControllerAddPodOrphan(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	set3 := newStatefulSet(3)
	set3.Name = "foo3"
	set3.Spec.Selector.MatchLabels = map[string]string{"foo3": "bar"}
	pod := newStatefulSetPod(set1, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)
	om.setsIndexer.Add(set3)

	// 去掉 owner 引用后，靠标签匹配定位候选 Set：set1/set2 命中，set3 标签不同不入队。
	pod.OwnerReferences = nil
	ssc.addPod(logger, pod)
	if got, want := ssc.queue.Len(), 2; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerAddPodNoSet 验证没有匹配 StatefulSet 的 Pod 不入队。
func TestStatefulSetControllerAddPodNoSet(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, _, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	// 没有 Set 索引时 addPod 无法推断归属，队列必须保持为空。
	ssc.addPod(logger, pod)
	ssc.queue.ShutDown()
	key, _ := ssc.queue.Get()
	if key != "" {
		t.Errorf("StatefulSet enqueued key for Pod with no Set %s", key)
	}
}

// TestStatefulSetControllerUpdatePod 验证 Pod 更新事件会把所属 StatefulSet 入队。
func TestStatefulSetControllerUpdatePod(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod1 := newStatefulSetPod(set1, 0)
	pod2 := newStatefulSetPod(set2, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)

	// 递增 resourceVersion 模拟真实更新，否则 updatePod 会因版本相同而忽略事件。
	prev := *pod1
	fakeResourceVersion(pod1)
	ssc.updatePod(logger, &prev, pod1)
	key, done := ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set1); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}

	prev = *pod2
	fakeResourceVersion(pod2)
	ssc.updatePod(logger, &prev, pod2)
	key, done = ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set2); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}
}

// TestStatefulSetControllerUpdatePodWithNoSet 验证无匹配 StatefulSet 的 Pod 更新不入队。
func TestStatefulSetControllerUpdatePodWithNoSet(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, _, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	prev := *pod
	fakeResourceVersion(pod)
	// 没有已索引的 Set，更新事件也无法产生可入队的归属。
	ssc.updatePod(logger, &prev, pod)
	ssc.queue.ShutDown()
	key, _ := ssc.queue.Get()
	if key != "" {
		t.Errorf("StatefulSet enqueued key for Pod with no Set %s", key)
	}
}

// TestStatefulSetControllerUpdatePodWithSameVersion 验证 resourceVersion 未变化的更新被忽略。
func TestStatefulSetControllerUpdatePodWithSameVersion(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	om.setsIndexer.Add(set)
	// 新旧对象 resourceVersion 相同意味着没有实质变更，应被忽略避免无谓同步。
	ssc.updatePod(logger, pod, pod)
	ssc.queue.ShutDown()
	key, _ := ssc.queue.Get()
	if key != "" {
		t.Errorf("StatefulSet enqueued key for Pod with no Set %s", key)
	}
}

// TestStatefulSetControllerUpdatePodOrphanWithNewLabels 验证孤儿 Pod 标签变化后新旧匹配的 Set 都入队。
func TestStatefulSetControllerUpdatePodOrphanWithNewLabels(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	pod.OwnerReferences = nil
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	om.setsIndexer.Add(set)
	om.setsIndexer.Add(set2)
	// 孤儿 Pod 改标签后，旧标签与新标签匹配的 Set 都应被入队以重新评估归属。
	clone := *pod
	clone.Labels = map[string]string{"foo2": "bar2"}
	fakeResourceVersion(&clone)
	ssc.updatePod(logger, &clone, pod)
	if got, want := ssc.queue.Len(), 2; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerUpdatePodChangeControllerRef 验证 controller 引用变更时旧、新 Set 都入队。
func TestStatefulSetControllerUpdatePodChangeControllerRef(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod := newStatefulSetPod(set, 0)
	pod2 := newStatefulSetPod(set2, 0)
	om.setsIndexer.Add(set)
	om.setsIndexer.Add(set2)
	// controller 引用切换时，旧属主与新属主都要入队：前者释放 Pod，后者接管。
	clone := *pod
	clone.OwnerReferences = pod2.OwnerReferences
	fakeResourceVersion(&clone)
	ssc.updatePod(logger, &clone, pod)
	if got, want := ssc.queue.Len(), 2; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerUpdatePodRelease 验证 Pod 被释放时旧属主和标签匹配的 Set 都入队。
func TestStatefulSetControllerUpdatePodRelease(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod := newStatefulSetPod(set, 0)
	om.setsIndexer.Add(set)
	om.setsIndexer.Add(set2)
	// Pod 被释放时，旧属主需入队处理孤儿，标签匹配的 Set 也需入队尝试收养。
	clone := *pod
	clone.OwnerReferences = nil
	fakeResourceVersion(&clone)
	ssc.updatePod(logger, pod, &clone)
	if got, want := ssc.queue.Len(), 2; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerDeletePod 验证 Pod 删除事件把所属 StatefulSet 入队。
func TestStatefulSetControllerDeletePod(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod1 := newStatefulSetPod(set1, 0)
	pod2 := newStatefulSetPod(set2, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)

	// 删除事件同样通过 ownerReference 把所属 Set 入队，触发副本数补齐。
	ssc.deletePod(logger, pod1)
	key, done := ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set1); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}

	ssc.deletePod(logger, pod2)
	key, done = ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set2); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}
}

// TestStatefulSetControllerDeletePodOrphan 验证孤儿 Pod 删除不会入队任何 StatefulSet。
func TestStatefulSetControllerDeletePodOrphan(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	pod1 := newStatefulSetPod(set1, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)

	// 孤儿 Pod 没有属主可通知，删除不应产生任何入队。
	pod1.OwnerReferences = nil
	ssc.deletePod(logger, pod1)
	if got, want := ssc.queue.Len(), 0; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}
}

// TestStatefulSetControllerDeletePodTombstone 验证墓碑形式的删除事件仍能入队所属 StatefulSet。
func TestStatefulSetControllerDeletePodTombstone(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set := newStatefulSet(3)
	pod := newStatefulSetPod(set, 0)
	om.setsIndexer.Add(set)
	// informer 可能只给出墓碑，处理器仍需从 Obj 中还原 Pod 归属并入队。
	tombstoneKey, _ := controller.KeyFunc(pod)
	tombstone := cache.DeletedFinalStateUnknown{Key: tombstoneKey, Obj: pod}
	ssc.deletePod(logger, tombstone)
	key, done := ssc.queue.Get()
	if key == "" || done {
		t.Error("failed to enqueue StatefulSet")
	} else if expectedKey, _ := controller.KeyFunc(set); expectedKey != key {
		t.Errorf("expected StatefulSet key %s found %s", expectedKey, key)
	}
}

// TestStatefulSetControllerGetStatefulSetsForPod 验证能找出所有标签匹配给定 Pod 的 StatefulSet。
func TestStatefulSetControllerGetStatefulSetsForPod(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx)
	set1 := newStatefulSet(3)
	set2 := newStatefulSet(3)
	set2.Name = "foo2"
	// 按 Pod 标签反查候选 Set，两个标签相同的 Set 都应命中。
	pod := newStatefulSetPod(set1, 0)
	om.setsIndexer.Add(set1)
	om.setsIndexer.Add(set2)
	om.podsIndexer.Add(pod)
	sets := ssc.getStatefulSetsForPod(logger, pod)
	if got, want := len(sets), 2; got != want {
		t.Errorf("len(sets) = %v, want %v", got, want)
	}
}

// TestGetPodsForStatefulSetAdopt 验证匹配标签与名称的孤儿 Pod 会被收养，其余被忽略。
func TestGetPodsForStatefulSetAdopt(t *testing.T) {
	set := newStatefulSet(5)
	// pod1 有属主且名称、标签都匹配，是最普通的保留场景。
	pod1 := newStatefulSetPod(set, 1)
	// pod2 是标签和名称都匹配的孤儿，应被收养。
	pod2 := newStatefulSetPod(set, 2)
	pod2.OwnerReferences = nil
	// pod3 标签不匹配。
	pod3 := newStatefulSetPod(set, 3)
	pod3.OwnerReferences = nil
	pod3.Labels = nil
	// pod4 名称不匹配。
	pod4 := newStatefulSetPod(set, 4)
	pod4.OwnerReferences = nil
	pod4.Name = "x" + pod4.Name

	_, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx, set, pod1, pod2, pod3, pod4)

	om.podsIndexer.Add(pod1)
	om.podsIndexer.Add(pod2)
	om.podsIndexer.Add(pod3)
	om.podsIndexer.Add(pod4)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := ssc.getPodsForStatefulSet(context.TODO(), set, selector)
	if err != nil {
		t.Fatalf("getPodsForStatefulSet() error: %v", err)
	}
	got := sets.NewString()
	for _, pod := range pods {
		got.Insert(pod.Name)
	}
	// 只有 pod1 和 pod2 应被认领，pod3/pod4 被忽略。
	want := sets.NewString(pod1.Name, pod2.Name)
	if !got.Equal(want) {
		t.Errorf("getPodsForStatefulSet() = %v, want %v", got, want)
	}
}

// TestAdoptOrphanRevisions 验证孤儿 ControllerRevision 被收养并设置 ownerReference。
func TestAdoptOrphanRevisions(t *testing.T) {
	ss1 := newStatefulSetWithLabels(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss1Rev1, err := history.NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1.Spec.Template.Annotations = make(map[string]string)
	ss1.Spec.Template.Annotations["ss1"] = "ss1"
	ss1Rev2, err := history.NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	// 清空 ownerReference 制造孤儿 Revision，等待 adoptOrphanRevisions 收养。
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}

	_, ctx := ktesting.NewTestContext(t)
	ssc, _, om, _ := newFakeStatefulSetController(ctx, ss1, ss1Rev1, ss1Rev2)

	om.revisionsIndexer.Add(ss1Rev1)
	om.revisionsIndexer.Add(ss1Rev2)

	// 收养会 patch Revision 的 ownerReference，fake client 下由 control 层处理。
	err = ssc.adoptOrphanRevisions(context.TODO(), ss1)
	if err != nil {
		t.Errorf("adoptOrphanRevisions() error: %v", err)
	}

	if revisions, err := ssc.control.ListRevisions(ss1); err != nil {
		t.Errorf("ListRevisions() error: %v", err)
	} else {
		var adopted bool
		for i := range revisions {
			if revisions[i].Name == ss1Rev2.Name && metav1.GetControllerOf(revisions[i]) != nil {
				adopted = true
			}
		}
		if !adopted {
			t.Error("adoptOrphanRevisions() not adopt orphan revisions")
		}
	}
}

// TestGetPodsForStatefulSetRelease 验证名称或标签不再匹配的已属主 Pod 被释放。
func TestGetPodsForStatefulSetRelease(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	set := newStatefulSet(3)
	ssc, _, om, _ := newFakeStatefulSetController(ctx, set)
	pod1 := newStatefulSetPod(set, 1)
	// pod2 虽归本 Set 所有，但名称不匹配。
	pod2 := newStatefulSetPod(set, 2)
	pod2.Name = "x" + pod2.Name
	// pod3 归本 Set 所有，但标签不匹配。
	pod3 := newStatefulSetPod(set, 3)
	pod3.Labels = nil
	// pod4 是不匹配的孤儿，不在收养范围。
	pod4 := newStatefulSetPod(set, 4)
	pod4.OwnerReferences = nil
	pod4.Labels = nil

	om.podsIndexer.Add(pod1)
	om.podsIndexer.Add(pod2)
	om.podsIndexer.Add(pod3)
	om.podsIndexer.Add(pod4)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := ssc.getPodsForStatefulSet(context.TODO(), set, selector)
	if err != nil {
		t.Fatalf("getPodsForStatefulSet() error: %v", err)
	}
	got := sets.NewString()
	for _, pod := range pods {
		got.Insert(pod.Name)
	}

	// 只保留 pod1：pod2/pod3 应被释放，pod4 直接忽略。
	want := sets.NewString(pod1.Name)
	if !got.Equal(want) {
		t.Errorf("getPodsForStatefulSet() = %v, want %v", got, want)
	}
}

// TestOrphanedPodsWithPVCDeletePolicy 验证不同 PVC 保留策略下孤儿 Pod 的 PVC ownerReference 归属。
func TestOrphanedPodsWithPVCDeletePolicy(t *testing.T) {
	testFn := func(t *testing.T, scaledownPolicy, deletionPolicy apps.PersistentVolumeClaimRetentionPolicyType) {
		set := newStatefulSet(4)
		*set.Spec.Replicas = 2
		set.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled = scaledownPolicy
		set.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted = deletionPolicy
		logger, ctx := ktesting.NewTestContext(t)
		ssc, _, om, _ := newFakeStatefulSetController(ctx, set)
		om.setsIndexer.Add(set)

		pods := []*v1.Pod{}
		pods = append(pods, newStatefulSetPod(set, 0))
		// pod1 是孤儿，但仍会被 controller 重新认领。
		pods = append(pods, newStatefulSetPod(set, 1))
		pods[1].OwnerReferences = nil
		// pod2 名称不匹配，其 PVC 不应被归属。
		pods = append(pods, newStatefulSetPod(set, 2))
		pods[2].Name = "x" + pods[2].Name

		// fake client 不会应用 strategic merge patch，需手动应用到本地 Pod 对象。
		ssc.kubeClient.(*fake.Clientset).PrependReactor("patch", "pods", func(action core.Action) (bool, runtime.Object, error) {
			patch := action.(core.PatchAction).GetPatch()
			target := action.(core.PatchAction).GetName()
			var pod *v1.Pod
			for _, p := range pods {
				if p.Name == target {
					pod = p
					break
				}
			}
			if pod == nil {
				t.Fatalf("Can't find patch target %s", target)
			}
			original, err := json.Marshal(pod)
			if err != nil {
				t.Fatalf("failed to marshal original pod %s: %v", pod.Name, err)
			}
			updated, err := strategicpatch.StrategicMergePatch(original, patch, v1.Pod{})
			if err != nil {
				t.Fatalf("failed to apply strategic merge patch %q on node %s: %v", patch, pod.Name, err)
			}
			if err := json.Unmarshal(updated, pod); err != nil {
				t.Fatalf("failed to unmarshal updated pod %s: %v", pod.Name, err)
			}

			return true, pod, nil
		})

		for _, pod := range pods {
			om.podsIndexer.Add(pod)
			claims := getPersistentVolumeClaims(set, pod)
			for _, claim := range claims {
				if err := om.CreateClaim(&claim, set); err != nil {
					t.Errorf("Failed to create claim %s: %v", claim.Name, err)
				}
			}
		}

		for i := range pods {
			if _, err := om.setPodReadyCondition(set, i, true, time.Now()); err != nil {
				t.Errorf("%d: %v", i, err)
			}
			if _, err := om.setPodRunning(set, i); err != nil {
				t.Errorf("%d: %v", i, err)
			}
		}

		// 先同步认领孤儿 Pod，再缩容：这样被缩容删除的 PVC 才会按策略获得正确的 ownerRef。
		ssc.enqueueStatefulSet(logger, set)
		fakeWorker(ssc)
		// 缩容到零：被引发的 PVC 删除应按策略决定 ownerRef 归属。
		*set.Spec.Replicas = 0
		ssc.enqueueStatefulSet(logger, set)
		fakeWorker(ssc)

		hasNamedOwnerRef := func(claim *v1.PersistentVolumeClaim, name string) bool {
			for _, ownerRef := range claim.GetOwnerReferences() {
				if ownerRef.Name == name {
					return true
				}
			}
			return false
		}
		verifyOwnerRefs := func(claim *v1.PersistentVolumeClaim, condemned bool) {
			podName := getClaimPodName(set, claim)
			const retain = apps.RetainPersistentVolumeClaimRetentionPolicyType
			const delete = apps.DeletePersistentVolumeClaimRetentionPolicyType
			switch {
			case scaledownPolicy == retain && deletionPolicy == retain:
				if hasNamedOwnerRef(claim, podName) || hasNamedOwnerRef(claim, set.Name) {
					t.Errorf("bad claim ownerRefs: %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			case scaledownPolicy == retain && deletionPolicy == delete:
				if hasNamedOwnerRef(claim, podName) || !hasNamedOwnerRef(claim, set.Name) {
					t.Errorf("bad claim ownerRefs: %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			case scaledownPolicy == delete && deletionPolicy == retain:
				if hasNamedOwnerRef(claim, podName) != condemned || hasNamedOwnerRef(claim, set.Name) {
					t.Errorf("bad claim ownerRefs: %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			case scaledownPolicy == delete && deletionPolicy == delete:
				if hasNamedOwnerRef(claim, podName) != condemned || !hasNamedOwnerRef(claim, set.Name) {
					t.Errorf("bad claim ownerRefs: %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			}
		}

		claims, _ := om.claimsLister.PersistentVolumeClaims(set.Namespace).List(labels.Everything())
		if len(claims) != len(pods) {
			t.Errorf("Unexpected number of claims: %d", len(claims))
		}
		for _, claim := range claims {
			// 只有 pod0 和重新认领的 pod1 的 PVC 可能带 ownerRef；pod2 名称不匹配应无引用。
			switch claim.Name {
			case "datadir-foo-0", "datadir-foo-1":
				verifyOwnerRefs(claim, false)
			case "datadir-foo-2":
				if hasNamedOwnerRef(claim, getClaimPodName(set, claim)) || hasNamedOwnerRef(claim, set.Name) {
					t.Errorf("unexpected ownerRefs for %s: %v", claim.Name, claim.GetOwnerReferences())
				}
			default:
				t.Errorf("Unexpected claim %s", claim.Name)
			}
		}
	}
	policies := []apps.PersistentVolumeClaimRetentionPolicyType{
		apps.RetainPersistentVolumeClaimRetentionPolicyType,
		apps.DeletePersistentVolumeClaimRetentionPolicyType,
	}
	// 遍历两种策略的笛卡尔积，覆盖缩容与删除两个时机的所有组合。
	for _, scaledownPolicy := range policies {
		for _, deletionPolicy := range policies {
			testName := fmt.Sprintf("ScaleDown:%s/SetDeletion:%s", scaledownPolicy, deletionPolicy)
			t.Run(testName, func(t *testing.T) { testFn(t, scaledownPolicy, deletionPolicy) })
		}
	}
}

// TestStaleOwnerRefOnScaleup 验证 PVC 存在过期 owner 引用时扩容受阻，清理后才能完成。
func TestStaleOwnerRefOnScaleup(t *testing.T) {
	for _, policy := range []*apps.StatefulSetPersistentVolumeClaimRetentionPolicy{
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.RetainPersistentVolumeClaimRetentionPolicyType,
		},
		{
			WhenScaled:  apps.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: apps.DeletePersistentVolumeClaimRetentionPolicyType,
		},
	} {
		onPolicy := func(msg string, args ...interface{}) string {
			return fmt.Sprintf(fmt.Sprintf("(%s) %s", policy, msg), args...)
		}
		// 仅当缩容策略为 Delete 时才会给 PVC 打上 Pod ownerRef，才能制造 stale 引用。
		set := newStatefulSet(3)
		set.Spec.PersistentVolumeClaimRetentionPolicy = policy
		logger, ctx := ktesting.NewTestContext(t)
		ssc, spc, om, _ := newFakeStatefulSetController(ctx, set)
		if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
			t.Error(onPolicy("Failed to turn up StatefulSet : %s", err))
		}
		var err error
		if set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name); err != nil {
			t.Error(onPolicy("Could not get scaled up set: %v", err))
		}
		if set.Status.Replicas != 3 {
			t.Error(onPolicy("set.Status.Replicas = %v; want 3", set.Status.Replicas))
		}
		*set.Spec.Replicas = 2
		if err := scaleDownStatefulSetController(logger, set, ssc, spc, om); err != nil {
			t.Error(onPolicy("Failed to scale down StatefulSet : msg, %s", err))
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Error(onPolicy("Could not get scaled down StatefulSet: %v", err))
		}
		if set.Status.Replicas != 2 {
			t.Error(onPolicy("Failed to scale statefulset to 2 replicas"))
		}

		var claim *v1.PersistentVolumeClaim
		claim, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).Get("datadir-foo-2")
		if err != nil {
			t.Error(onPolicy("Could not find expected pvc datadir-foo-2"))
		}
		refs := claim.GetOwnerReferences()
		if len(refs) != 1 {
			t.Error(onPolicy("Expected only one refs: %v", refs))
		}
		// 篡改 Pod ownerReference 的 UID，制造 stale 引用。
		for i := range refs {
			if refs[i].Name == "foo-2" {
				refs[i].UID = "stale"
				break
			}
		}
		claim.SetOwnerReferences(refs)
		if err = om.claimsIndexer.Update(claim); err != nil {
			t.Error(onPolicy("Could not update claim with new owner ref: %v", err))
		}

		*set.Spec.Replicas = 3
		// stale PVC 未删除前扩容不能完成：限制 10 次迭代后删除 PVC 再验证恢复。
		if err := scaleUpStatefulSetControllerBounded(logger, set, ssc, spc, om, 10); err != nil {
			t.Error(onPolicy("Failed attempt to scale StatefulSet back up: %v", err))
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Error(onPolicy("Could not get scaled down StatefulSet: %v", err))
		}
		if set.Status.Replicas != 2 {
			t.Error(onPolicy("Expected set to stay at two replicas"))
		}

		claim, err = om.claimsLister.PersistentVolumeClaims(set.Namespace).Get("datadir-foo-2")
		if err != nil {
			t.Error(onPolicy("Could not find expected pvc datadir-foo-2"))
		}
		refs = claim.GetOwnerReferences()
		if len(refs) != 1 {
			t.Error(onPolicy("Unexpected change to condemned pvc ownerRefs: %v", refs))
		}
		foundPodRef := false
		for i := range refs {
			if refs[i].UID == "stale" {
				foundPodRef = true
				break
			}
		}
		if !foundPodRef {
			t.Error(onPolicy("Claim ref unexpectedly changed: %v", refs))
		}
		if err = om.claimsIndexer.Delete(claim); err != nil {
			t.Error(onPolicy("Could not delete stale pvc: %v", err))
		}

		if err := scaleUpStatefulSetController(logger, set, ssc, spc, om); err != nil {
			t.Error(onPolicy("Failed to scale StatefulSet back up: %v", err))
		}
		set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name)
		if err != nil {
			t.Error(onPolicy("Could not get scaled down StatefulSet: %v", err))
		}
		if set.Status.Replicas != 3 {
			t.Error(onPolicy("Failed to scale set back up once PVC was deleted"))
		}
	}
}

// TestStatefulSetAvailabilityCheck 验证 minReadySeconds 对 availableReplicas 的影响及到期重排。
func TestStatefulSetAvailabilityCheck(t *testing.T) {
	tCtx := ktesting.Init(t)

	set := setMinReadySeconds(newStatefulSet(4), int32(5)) // 5 秒
	set = setupPodManagementPolicy(apps.ParallelPodManagement, set)
	ssc, _, om, _ := newFakeStatefulSetController(tCtx, set)
	if err := om.setsIndexer.Add(set); err != nil {
		t.Fatalf("could not add set to the cache: %v", err)
	}
	now := time.Now()

	pods := []*v1.Pod{}
	pods = append(pods, newStatefulSetPod(set, 0))
	pods = append(pods, newStatefulSetPod(set, 1))
	// 刚就绪，尚未满足 minReadySeconds=5s。
	pods[1].Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: now}}}
	pods = append(pods, newStatefulSetPod(set, 2))
	// 就绪 2 秒，仍未满足 5 秒门槛。
	pods[2].Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: now.Add(-2 * time.Second)}}}
	pods = append(pods, newStatefulSetPod(set, 3))
	// 就绪 4.3 秒，还差 0.7 秒才满足门槛，用于验证自动重排。
	pods[3].Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: now.Add(-4300 * time.Millisecond)}}}

	for i, pod := range pods {
		if err := om.podsIndexer.Add(pod); err != nil {
			t.Fatalf("could not add pod to the cache %d: %v", i, err)
		}
		var err error
		if pods, err = om.setPodRunning(set, i); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
	}
	err := ssc.syncStatefulSet(tCtx, set, pods)
	if err != nil {
		t.Fatal(err)
	}

	if set, err = om.setsLister.StatefulSets(set.Namespace).Get(set.Name); err != nil {
		t.Fatalf("Could not get StatefulSet: %v", err)
	}

	// pod0 未就绪，其余三个中至少两个未满足 minReadySeconds，故均不算可用。
	if set.Status.ReadyReplicas != 3 {
		t.Errorf("Expected updated StatefulSet to contain ready replicas %v, got %v instead",
			3, set.Status.ReadyReplicas)
	}
	if set.Status.AvailableReplicas != 0 {
		t.Errorf("Expected updated StatefulSet to contain available replicas %v, got %v instead",
			0, set.Status.AvailableReplicas)
	}

	if got, want := ssc.queue.Len(), 0; got != want {
		t.Errorf("queue.Len() = %v, want %v", got, want)
	}

	// 700ms 后应重新入队以重算 availableReplicas；额外 200ms 留给测试调度。
	tCtx.Eventually(ssc.queue.Len).WithTimeout(900*time.Millisecond).
		WithPolling(10*time.Millisecond).
		Should(gomega.Equal(1), " StatefulSet should be re-queued to recompute .status.availableReplicas")
}

// newFakeStatefulSetController 搭建使用 fake client 与 informer 的控制器测试环境。
func newFakeStatefulSetController(ctx context.Context, initialObjects ...runtime.Object) (*StatefulSetController, *StatefulPodControl, *fakeObjectManager, history.Interface) {
	client := fake.NewSimpleClientset(initialObjects...)
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	om := newFakeObjectManager(informerFactory)
	spc := NewStatefulPodControlFromManager(om, &noopRecorder{})
	ssu := newFakeStatefulSetStatusUpdater(informerFactory.Apps().V1().StatefulSets())
	ssc := NewStatefulSetController(
		ctx,
		informerFactory.Core().V1().Pods(),
		informerFactory.Apps().V1().StatefulSets(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Apps().V1().ControllerRevisions(),
		client,
	)
	ssh := history.NewFakeHistory(informerFactory.Apps().V1().ControllerRevisions())
	// 测试不启动 informer，直接把同步检查标记为已完成。
	ssc.podListerSynced = alwaysReady
	ssc.setListerSynced = alwaysReady
	ssc.control = NewDefaultStatefulSetControl(spc, ssu, ssh)

	return ssc, spc, om, ssh
}

// fakeWorker 取出队列中的一项并同步一次，代替真实 worker 循环。
func fakeWorker(ssc *StatefulSetController) {
	if obj, done := ssc.queue.Get(); !done {
		_ = ssc.sync(context.TODO(), obj)
		ssc.queue.Done(obj)
	}
}

// getPodAtOrdinal 按序号排序后返回指定位置的 Pod，越界返回 nil。
func getPodAtOrdinal(pods []*v1.Pod, ordinal int) *v1.Pod {
	// 防止越界索引，调用方据此判断序号处是否已有 Pod。
	if 0 > ordinal || ordinal >= len(pods) {
		return nil
	}
	// lister 返回顺序不保证，先按序号排序才能正确定位。
	sort.Sort(ascendingOrdinal(pods))
	return pods[ordinal]
}

// scaleUpStatefulSetController 反复模拟 Pod 生命周期事件，直到 StatefulSet 扩容到目标副本数。
func scaleUpStatefulSetController(logger klog.Logger, set *apps.StatefulSet, ssc *StatefulSetController, spc *StatefulPodControl, om *fakeObjectManager) error {
	return scaleUpStatefulSetControllerBounded(logger, set, ssc, spc, om, -1)
}

// scaleUpStatefulSetControllerBounded 与 scaleUpStatefulSetController 相同，但限制模拟迭代次数。
func scaleUpStatefulSetControllerBounded(logger klog.Logger, set *apps.StatefulSet, ssc *StatefulSetController, spc *StatefulPodControl, om *fakeObjectManager, maxIterations int) error {
	om.setsIndexer.Add(set)
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	iterations := 0
	// 每次迭代推进一步生命周期：Pending -> Running -> Ready，并同步触发对应事件。
	for (maxIterations < 0 || iterations < maxIterations) && set.Status.ReadyReplicas < *set.Spec.Replicas {
		iterations++
		pods, err := om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}
		ord := len(pods) - 1
		if pods, err = om.setPodPending(set, ord); err != nil {
			return err
		}
		pod := getPodAtOrdinal(pods, ord)
		ssc.addPod(logger, pod)
		fakeWorker(ssc)
		pod = getPodAtOrdinal(pods, ord)
		prev := *pod
		if pods, err = om.setPodRunning(set, ord); err != nil {
			return err
		}
		pod = getPodAtOrdinal(pods, ord)
		ssc.updatePod(logger, &prev, pod)
		fakeWorker(ssc)
		pod = getPodAtOrdinal(pods, ord)
		prev = *pod
		if pods, err = om.setPodReadyCondition(set, ord, true, time.Now()); err != nil {
			return err
		}
		pod = getPodAtOrdinal(pods, ord)
		ssc.updatePod(logger, &prev, pod)
		fakeWorker(ssc)
		if err := assertMonotonicInvariants(set, om); err != nil {
			return err
		}
		obj, _, err := om.setsIndexer.Get(set)
		if err != nil {
			return err
		}
		set = obj.(*apps.StatefulSet)

	}
	return assertMonotonicInvariants(set, om)
}

// scaleDownStatefulSetController 模拟终止并删除最高序号 Pod，直到缩容到目标副本数。
func scaleDownStatefulSetController(logger klog.Logger, set *apps.StatefulSet, ssc *StatefulSetController, spc *StatefulPodControl, om *fakeObjectManager) error {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := om.podsLister.Pods(set.Namespace).List(selector)
	if err != nil {
		return err
	}
	ord := len(pods) - 1
	pod := getPodAtOrdinal(pods, ord)
	prev := *pod
	fakeResourceVersion(set)
	om.setsIndexer.Add(set)
	ssc.enqueueStatefulSet(logger, set)
	fakeWorker(ssc)
	pods, err = om.addTerminatingPod(set, ord)
	if err != nil {
		return err
	}
	pod = getPodAtOrdinal(pods, ord)
	ssc.updatePod(logger, &prev, pod)
	fakeWorker(ssc)
	spc.DeleteStatefulPod(set, pod)
	ssc.deletePod(logger, pod)
	fakeWorker(ssc)
	for set.Status.Replicas > *set.Spec.Replicas {
		pods, err = om.podsLister.Pods(set.Namespace).List(selector)
		if err != nil {
			return err
		}

		ord := len(pods)
		pods, err = om.addTerminatingPod(set, ord)
		if err != nil {
			return err
		}
		pod = getPodAtOrdinal(pods, ord)
		ssc.updatePod(logger, &prev, pod)
		fakeWorker(ssc)
		spc.DeleteStatefulPod(set, pod)
		ssc.deletePod(logger, pod)
		fakeWorker(ssc)
		obj, _, err := om.setsIndexer.Get(set)
		if err != nil {
			return err
		}
		set = obj.(*apps.StatefulSet)

	}
	return assertMonotonicInvariants(set, om)
}

// rawTemplate 将 Pod 模板序列化为 ControllerRevision 使用的 RawExtension。
func rawTemplate(template *v1.PodTemplateSpec) runtime.RawExtension {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(template); err != nil {
		panic(err)
	}
	return runtime.RawExtension{Raw: buf.Bytes()}
}
