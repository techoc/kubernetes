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

package cache

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// TestPodGroupState_AssumeForget 验证 Pod 在 PodGroup 状态中 assume 与 forget 的完整流转：
// addPod 后进入未调度集合，assumePod 后进入 AssumedPods，forgetPod 后再退回未调度集合。
func TestPodGroupState_AssumeForget(t *testing.T) {
	pgs := newPodGroupState()
	pod := st.MakePod().Namespace("ns1").Name("p1").UID("p1").PodGroupName("pg1").Obj()

	pgs.addPod(pod)
	// 刚加入的 Pod 不应处于 assumed 状态。
	if pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("AssumedPods should be initially empty")
	}
	// 刚加入的 Pod 应处于未调度状态，等待调度。
	if !pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("Pod should be initially in UnscheduledPods")
	}

	pgs.assumePod(pod)
	// assume 后应进入 AssumedPods，表示该 Pod 已被预留给 gang。
	if !pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("Pod should be in AssumedPods after AssumePod")
	}
	// assume 后应从 UnscheduledPods 移除，避免被重复调度。
	if pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("UnscheduledPods should be empty after AssumePod")
	}

	pgs.forgetPod(pod.UID)
	// forget 后应退出 AssumedPods，释放预留。
	if pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("Pod should not be in AssumedPods after ForgetPod")
	}
	// forget 后应重新进入 UnscheduledPods，等待重新调度。
	if !pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("Pod should be in UnscheduledPods after ForgetPod")
	}
}

// TestPodGroupState_Clone 验证 snapshot 得到的快照与原始状态隔离：
// 克隆生成时状态一致，但此后双向修改互不影响。
func TestPodGroupState_Clone(t *testing.T) {
	pgs := newPodGroupState()

	pod1 := st.MakePod().Namespace("ns1").Name("p1").UID("p1").
		PodGroupName("pg").Obj()
	pod2 := st.MakePod().Namespace("ns1").Name("p2").UID("p2").
		PodGroupName("pg").Obj()

	// 加入两个 Pod 并 assume 其一，覆盖未调度与 assumed 两种子状态的克隆。
	pgs.addPod(pod1)
	pgs.addPod(pod2)
	pgs.assumePod(pod2)

	snap := pgs.snapshot()

	// 克隆应保留相同的 generation，便于检测其后的变更。
	if snap.generation != pgs.generation {
		t.Errorf("expected clone generation %d, got %d", pgs.generation, snap.generation)
	}

	// 克隆应包含全部 Pod。
	if !snap.AllPods().Has(pod1.UID) || !snap.AllPods().Has(pod2.UID) {
		t.Error("expected both pods in clone's AllPods")
	}

	// 克隆应保留 pod1 的未调度状态。
	if _, ok := snap.UnscheduledPods()[pod1.Name]; !ok {
		t.Error("expected pod1 in clone's UnscheduledPods")
	}

	// 克隆应保留 pod2 的 assumed 状态。
	if !snap.AssumedPods().Has(pod2.UID) {
		t.Error("expected pod2 in clone's AssumedPods")
	}

	// 修改克隆不应影响原始状态：验证底层集合是独立拷贝的。
	snap.assumePod(pod1)
	if _, ok := pgs.assumedPods[pod1.UID]; ok {
		t.Error("mutation to clone should not affect original's assumedPods")
	}

	// 反向验证：修改原始状态也不应影响已生成的快照。
	pod3 := st.MakePod().Namespace("ns1").Name("p3").UID("p3").
		PodGroupName("pg").Obj()
	pgs.addPod(pod3)
	if snap.AllPods().Has(pod3.UID) {
		t.Error("mutation to original should not affect clone's AllPods")
	}
}

// TestPodGroupState_PodCounts 验证 PodGroup 的 Pod 计数：
// AllPodsCount 统计全部已知 Pod，ScheduledPodsCount 统计 assumed 与已分配 Pod 之和。
func TestPodGroupState_PodCounts(t *testing.T) {
	pgs := newPodGroupState()
	pod1 := st.MakePod().Namespace("ns1").Name("p1").UID("p1").
		PodGroupName("pg1").Obj()
	pod2 := st.MakePod().Namespace("ns1").Name("p2").UID("p2").
		PodGroupName("pg1").Obj()
	pod3 := st.MakePod().Namespace("ns1").Name("p3").UID("p3").Node("node1").
		PodGroupName("pg1").Obj()
	pod4 := st.MakePod().Namespace("ns1").Name("p4").UID("p4").
		PodGroupName("pg1").Obj()

	// 空状态两项计数都应为 0。
	if count := pgs.AllPodsCount(); count != 0 {
		t.Errorf("Expected AllPodsCount to be 0, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 0 {
		t.Errorf("Expected ScheduledPodsCount to be 0, got %d", count)
	}

	// pod3 已带 NodeName，加入后应计入已调度，其余两个未调度。
	pgs.addPod(pod1)
	pgs.addPod(pod2)
	pgs.addPod(pod3)

	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 1 {
		t.Errorf("Expected ScheduledPodsCount to be 1, got %d", count)
	}

	// assume 未调度 Pod 会将其移入 assumed，已调度计数加一，但总数不变。
	pgs.assumePod(pod1)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}

	// 对已分配的 Pod 执行 assume 不应重复计数。
	pgs.assumePod(pod3)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}

	// assume 不在状态中的 Pod 会被忽略，计数不变。
	pgs.assumePod(pod4)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}

	// forget 已分配的 Pod 会被重新归入 assigned，已调度计数不变。
	pgs.forgetPod(pod3.UID)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}

	// forget assumed 状态的 Pod 会退回未调度集合，已调度计数减一。
	pgs.forgetPod(pod1.UID)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 1 {
		t.Errorf("Expected ScheduledPodsCount to be 1, got %d", count)
	}

	// 重复 forget 非 assumed 的 Pod 不应改变计数。
	pgs.forgetPod(pod1.UID)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 1 {
		t.Errorf("Expected ScheduledPodsCount to be 1, got %d", count)
	}

	// 重新 assume 未调度 Pod 会回到 assumed，已调度计数加一。
	pgs.assumePod(pod2)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}

	// forget 不在状态中的 Pod 同样被忽略，计数不变。
	pgs.forgetPod(pod4.UID)
	if count := pgs.AllPodsCount(); count != 3 {
		t.Errorf("Expected AllPodsCount to be 3, got %d", count)
	}
	if count := pgs.ScheduledPodsCount(); count != 2 {
		t.Errorf("Expected ScheduledPodsCount to be 2, got %d", count)
	}
}

// TestPodGroupState_ScheduledPods 验证 ScheduledPods 返回当前所有已 assume 或已分配的 Pod，
// 且快照在生成后被冻结，不受原始状态后续变更的影响。
func TestPodGroupState_ScheduledPods(t *testing.T) {

	pgs := newPodGroupState()
	unscheduledPod := st.MakePod().Namespace("ns").Name("p1").UID("p1").
		PodGroupName("pg").Obj()
	assumedPod := st.MakePod().Namespace("ns").Name("p2").UID("p2").
		PodGroupName("pg").Obj()
	assignedPod := st.MakePod().Namespace("ns").Name("p3").UID("p3").Node("node1").
		PodGroupName("pg").Obj()

	pgs.addPod(assignedPod)
	pgs.addPod(unscheduledPod)
	pgs.addPod(assumedPod)

	// 模拟调度器在节点上 assume Pod：深拷贝后设置 NodeName，
	// 确保原始 assumedPod 仍是未带节点名的版本。
	assumedPodWithNodeName := assumedPod.DeepCopy()
	assumedPodWithNodeName.Spec.NodeName = "node2"

	pgs.assumePod(assumedPodWithNodeName)
	scheduledPods := pgs.ScheduledPods()

	// 先生成快照，再原状态 assume 另一个 Pod，用于验证快照结果不变。
	snapshot := pgs.snapshot()
	pgs.assumePod(unscheduledPod)
	snapshotScheduledPods := snapshot.ScheduledPods()

	// 期望只含已分配的 pod3 与 assume 时带节点名的 pod2，不含未调度的 pod1。
	expectedScheduledPods := []*v1.Pod{assignedPod, assumedPodWithNodeName}

	if diff := cmp.Diff(expectedScheduledPods, scheduledPods); diff != "" {
		t.Errorf("unexpected ScheduledPods result (-want,+got):\n%s", diff)
	}

	if diff := cmp.Diff(expectedScheduledPods, snapshotScheduledPods); diff != "" {
		t.Errorf("unexpected snapshot ScheduledPods result (-want,+got):\n%s", diff)
	}
}

// TestCompositePodGroupState_Children 验证 CompositePodGroup 的子对象集合：
// 初始为空，addChild 后能取回全部子元素，覆盖 PodGroup 与 CompositePodGroup 两种键类型。
func TestCompositePodGroupState_Children(t *testing.T) {
	cpgs := newCompositePodGroupState()

	// 1. 初始状态：子元素应为空。
	if children := cpgs.GetChildren(); len(children) != 0 {
		t.Errorf("Expected no children initially, got %v", children)
	}

	// 2. 添加子元素并验证 GetChildren。
	childKey1 := fwk.PodGroupKey("ns1", "child1")
	childKey2 := fwk.CompositePodGroupKey("ns1", "child2")
	cpgs.addChild(childKey1)
	cpgs.addChild(childKey2)

	children := cpgs.GetChildren()
	var childrenStrs []string
	for _, child := range children {
		childrenStrs = append(childrenStrs, child.String())
	}
	// 子元素来自 set，遍历顺序不稳定，排序后才能与期望值稳定比较。
	sort.Strings(childrenStrs)
	// 期望同时包含两种不同类型的子键。
	expectedChildren := []string{"compositepodgroup/ns1/child2", "podgroup/ns1/child1"}
	if diff := cmp.Diff(expectedChildren, childrenStrs); diff != "" {
		t.Errorf("Unexpected children result (-want,+got):\n%s", diff)
	}
}
