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

package daemon

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/daemon/util"
	testingclock "k8s.io/utils/clock/testing"
)

// TestDaemonSetUpdatesPods 验证 RollingUpdate 策略下按 maxUnavailable 分批滚动更新 Pod。
// 先为 5 个节点各创建一个 Pod 并标记为就绪；修改镜像后，每轮同步先删除至多 maxUnavailable 个旧 Pod，
// 再创建等量新 Pod，直到全部 Pod 都使用新模板。各轮之间用 clearExpectations 隔离期望计数，
// 并用 markPodsReady 模拟新 Pod 就绪，最后断言不再产生任何变更。
func TestDaemonSetUpdatesPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	maxUnavailable := 2
	addNodes(manager.nodeStore, 0, 5, nil)
	manager.dsStore.Add(ds)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	markPodsReady(podControl.podStore)

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	intStr := intstr.FromInt32(int32(maxUnavailable))
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	manager.dsStore.Update(ds)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 1, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)
}

// TestDaemonSetUpdatesPodsWithMaxSurge 验证由 maxSurge 主导的滚动更新：
// 新 Pod 可以先超额创建，随后旧 Pod 才被删除；
// 用 5%maxSurge 覆盖不能整除的情况，保证最后一批只创建余数个 Pod。
func TestDaemonSetUpdatesPodsWithMaxSurge(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	addNodes(manager.nodeStore, 0, 5, nil)
	manager.dsStore.Add(ds)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	markPodsReady(podControl.podStore)

	// 此更新策略下由 surge（超额创建数）主导更新节奏。
	maxSurge := 2
	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(int32(maxSurge)))
	manager.dsStore.Update(ds)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, 0, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 5%maxSurge, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 5%maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
}

// TestDaemonSetUpdatesPodsNotMatchTainstWithMaxSurge 验证移除污点容忍后的滚动更新行为：
// DaemonSet 最初容忍 control-plane 污点，因而能在全部 5 个节点上运行；
// 去掉容忍后，新 Pod 无法调度到带污点的节点，只能在其余节点上超额创建，
// 而污点节点上的旧 Pod 会一直保留，最终同步不再产生变更。
func TestDaemonSetUpdatesPodsNotMatchTainstWithMaxSurge(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	ds := newDaemonSet("foo")
	maxSurge := 1
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt(maxSurge))
	tolerations := []v1.Toleration{
		{Key: "node-role.kubernetes.io/control-plane", Operator: v1.TolerationOpExists},
	}
	setDaemonSetToleration(ds, tolerations)
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}

	// 添加五个节点，并给其中一个打上 control-plane 污点。
	addNodes(manager.nodeStore, 0, 5, nil)
	taints := []v1.Taint{
		{Key: "node-role.kubernetes.io/control-plane", Effect: v1.TaintEffectNoSchedule},
	}
	node := newNode("node-0", nil)
	setNodeTaint(node, taints)
	err = manager.nodeStore.Update(node)
	if err != nil {
		t.Fatal(err)
	}

	// 此时 DaemonSet 仍容忍污点，五个节点上都应创建出 Pod。
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	markPodsReady(podControl.podStore)

	// 去掉容忍后，污点节点上的旧 Pod 无法被新 Pod 取代。
	ds.Spec.Template.Spec.Tolerations = nil
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, 1, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxSurge, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	markPodsReady(podControl.podStore)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxSurge, 0)
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
}

// TestDaemonSetUpdatesWhenNewPodIsNotReady 验证新 Pod 一直未就绪时更新会停在 maxUnavailable 上限：
// 删除 3 个旧 Pod 并创建 3 个新 Pod 后，不可用数已达上限，后续同步不再继续更新。
func TestDaemonSetUpdatesWhenNewPodIsNotReady(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	maxUnavailable := 3
	addNodes(manager.nodeStore, 0, 5, nil)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	markPodsReady(podControl.podStore)

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	intStr := intstr.FromInt32(int32(maxUnavailable))
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	// 这里不把新 Pod 标记为就绪，使 numUnavailable 恰好等于 maxUnavailable，从而阻塞后续更新。
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)
}

// TestDaemonSetUpdatesSomeOldPodsNotReady 验证部分旧 Pod 未就绪时仍按 maxUnavailable 控制更新，
// 且连续两次模板更新后，仍保持就绪的旧 Pod 不会被反复重建。
func TestDaemonSetUpdatesSomeOldPodsNotReady(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	maxUnavailable := 2
	addNodes(manager.nodeStore, 0, 5, nil)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)
	markPodsReady(podControl.podStore)

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	intStr := intstr.FromInt(maxUnavailable)
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	// 旧 Pod 全部可用，本轮应更新 2 个。
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	// 再次更新模板，仍只删除并创建 2 个 Pod，另外 3 个可用的旧 Pod 不应被触碰。

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar3"
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	readyCount := 0
	expectedReadyCount := 5 - maxUnavailable
	for _, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		n, condition := podutil.GetPodCondition(&pod.Status, v1.PodReady)
		if n != -1 && condition.Status == v1.ConditionTrue {
			readyCount++
		}
	}

	if readyCount != expectedReadyCount {
		t.Fatalf("Expected %d old ready pods, but found %d", expectedReadyCount, readyCount)
	}
}

// TestDaemonSetUpdatesSaveOldHealthyPods 验证滚动更新不会反复重建仍然健康的旧 Pod：
// 第一次以 maxUnavailable=10 更新后记录下 10 个保持就绪的旧 Pod；
// 之后多次以随机 maxUnavailable 继续更新，这些旧 Pod 应始终存在；
// 最后把 maxUnavailable 提高到 11，才允许其中 1 个被更新，剩余就绪 Pod 数为 9。
func TestDaemonSetUpdatesSaveOldHealthyPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	addNodes(manager.nodeStore, 0, 20, nil)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 20, 0, 0)
	markPodsReady(podControl.podStore)

	t.Logf("first update to get 10 old pods which should never be touched")
	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	maxUnavailable := 10
	intStr := intstr.FromInt(maxUnavailable)
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	// 记录当前仍处于就绪状态的 Pod，后续断言它们不会被更新。
	oldReadyPods := sets.Set[string]{}
	for _, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		if podutil.IsPodReady(pod) {
			oldReadyPods.Insert(pod.Name)
		}
	}

	for i := 0; i < 10; i++ {
		maxUnavailable := rand.Intn(10)
		t.Logf("%d iteration, maxUnavailable=%d", i+1, maxUnavailable)
		intStr = intstr.FromInt(maxUnavailable)
		ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
		ds.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("foo2/bar3-%d", i)
		err = manager.dsStore.Update(ds)
		if err != nil {
			t.Fatal(err)
		}

		// 只有那 10 个不可用的 Pod 会被更新，就绪的旧 Pod 不在可选范围内。
		clearExpectations(t, manager, ds, podControl)
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 10, 0)

		clearExpectations(t, manager, ds, podControl)
		expectSyncDaemonSets(t, manager, ds, podControl, 10, 0, 0)

		clearExpectations(t, manager, ds, podControl)
		expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
		clearExpectations(t, manager, ds, podControl)

		// 确认此前的就绪 Pod 在随机更新后仍然存在。
		readyPods := sets.Set[string]{}
		t.Logf("looking for old ready pods: %s", strings.Join(oldReadyPods.UnsortedList(), ", "))
		for _, obj := range podControl.podStore.List() {
			pod := obj.(*v1.Pod)
			if podutil.IsPodReady(pod) {
				readyPods.Insert(pod.Name)
			}
		}
		if !readyPods.HasAll(oldReadyPods.UnsortedList()...) {
			t.Errorf("pods have changed in %d-th iteration: %s", i,
				strings.Join(oldReadyPods.Difference(readyPods).UnsortedList(), ", "))
		}
	}

	maxUnavailable = 11
	intStr = intstr.FromInt(maxUnavailable)
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar4"
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	// maxUnavailable 超过旧 Pod 数后，就绪 Pod 会继续被保留，最终只剩 9 个就绪。
	readyPods := sets.Set[string]{}
	for _, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		if podutil.IsPodReady(pod) {
			readyPods.Insert(pod.Name)
		}
	}
	if readyPods.Len() != 9 {
		t.Errorf("readyPods are different than expected, should be 9 but is %s", strings.Join(readyPods.UnsortedList(), ", "))
	}
}

// TestDaemonSetUpdatesAllOldNotReadyPodsAndNewNotReadyPods 验证混合状态下只替换不可用的旧 Pod：
// 先完成 100 个节点的一轮滚动更新，再人为把 88 个新 Pod 标记为未就绪，
// 并把其中 12 个改回旧 hash（其中 2 个未就绪）；
// 同步应只删除这 2 个不可用的旧 Pod 并重建，10 个仍可用的旧 Pod 不受影响。
func TestDaemonSetUpdatesAllOldNotReadyPodsAndNewNotReadyPods(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	addNodes(manager.nodeStore, 0, 100, nil)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 100, 0, 0)
	markPodsReady(podControl.podStore)
	var hash1 string
	// 此时 100 个 Pod 都属于该 DaemonSet；记下当前 hash，后面用它把部分 Pod 伪装成旧版本。
	for _, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		hash1 = pod.Labels[apps.ControllerRevisionHashLabelKey]
		break
	}

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	maxUnavailable := 10
	intStr := intstr.FromInt(maxUnavailable)
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}
	// 每轮最多更新 10 个，因此需要 10 轮才能完成 100 个节点的滚动更新。
	for i := 0; i < 10; i++ {
		clearExpectations(t, manager, ds, podControl)
		expectSyncDaemonSets(t, manager, ds, podControl, 0, maxUnavailable, 0)

		clearExpectations(t, manager, ds, podControl)
		expectSyncDaemonSets(t, manager, ds, podControl, maxUnavailable, 0, 0)
		// 必须把新 Pod 标记为就绪，否则后续轮次会因不可用数超限而无法继续。
		markPodsReady(podControl.podStore)
	}

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	// 构造如下混合状态：maxUnavailable 为 10，88 个新 Pod 不可用，2 个旧 Pod 不可用，10 个旧 Pod 可用。
	oldUnavailablePods := sets.Set[string]{}
	for i, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		// 把后 90 个 Pod 标记为未就绪。
		if i >= 10 {
			condition := v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionFalse}
			podutil.UpdatePodCondition(&pod.Status, &condition)
		}
		// 把前 12 个 Pod 改回旧 hash。
		if i < 12 {
			pod.Labels[apps.ControllerRevisionHashLabelKey] = hash1
			// 记录其中 2 个未就绪的旧 Pod，稍后验证它们确实被替换。
			if i >= 10 {
				oldUnavailablePods.Insert(pod.Name)
			}
		}
	}

	clearExpectations(t, manager, ds, podControl)
	t.Logf("expect 2 old pods deletion in 1st iteration")
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 2, 0)

	clearExpectations(t, manager, ds, podControl)
	t.Logf("expect 2 new pods creation in 2nd iteration")
	expectSyncDaemonSets(t, manager, ds, podControl, 2, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	t.Logf("expect no modifications in 3rd iteration")
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)

	// 确认之前记录的不可用旧 Pod 已经全部被替换。
	t.Logf("Looking for old pods %s", strings.Join(oldUnavailablePods.UnsortedList(), ", "))
	notUpdatedOldPods := sets.Set[string]{}
	for _, obj := range podControl.podStore.List() {
		pod := obj.(*v1.Pod)
		if oldUnavailablePods.Has(pod.Name) {
			notUpdatedOldPods.Insert(pod.Name)
		}
	}
	if notUpdatedOldPods.Len() > 0 {
		t.Fatalf("found not updated old pods: %s", strings.Join(notUpdatedOldPods.UnsortedList(), ", "))
	}
}

// TestDaemonSetUpdatesAllOldPodsNotReady 验证所有旧 Pod 都不可用时会被一次性全部删除再重建：
// 即使 maxUnavailable 只有 3，删除数量仍可超过该值，因为旧 Pod 已不可用、不会加剧服务中断。
func TestDaemonSetUpdatesAllOldPodsNotReady(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	maxUnavailable := 3
	addNodes(manager.nodeStore, 0, 5, nil)
	err = manager.dsStore.Add(ds)
	if err != nil {
		t.Fatal(err)
	}
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	intStr := intstr.FromInt32(int32(maxUnavailable))
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	err = manager.dsStore.Update(ds)
	if err != nil {
		t.Fatal(err)
	}

	// 旧 Pod 全部不可用，因此不受 maxUnavailable 限制，应全部删除。
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 5, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)
}

// TestDaemonSetUpdatesAllOldPodsNotReadyMaxSurge 验证 maxSurge 模式下旧 Pod 全部不可用时的处理，
// 并结合 minReadySeconds 检查新 Pod 的就绪时间：
// 先超额创建新 Pod，待其就绪后删除旧 Pod；随后再次更新模板，
// 通过移动假时钟逐步让新 Pod 满足最短就绪时间，验证删除旧 Pod 的节奏正确。
func TestDaemonSetUpdatesAllOldPodsNotReadyMaxSurge(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	addNodes(manager.nodeStore, 0, 5, nil)
	manager.dsStore.Add(ds)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

	maxSurge := 3
	ds.Spec.Template.Spec.Containers[0].Image = "foo2/bar2"
	ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(int32(maxSurge)))
	manager.dsStore.Update(ds)

	// 旧 Pod 都不可用，因此直接超额创建新 Pod。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(100, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

	// 推进假时钟等待新 Pod 就绪，随后旧 Pod 应被删除。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(200, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 5, 0)

	setPodReadiness(t, manager, true, 5, func(_ *v1.Pod) bool { return true })
	ds.Spec.MinReadySeconds = 15
	ds.Spec.Template.Spec.Containers[0].Image = "foo3/bar3"
	manager.dsStore.Update(ds)

	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(300, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 3, 0, 0)

	hash, err := currentDSHash(manager, ds)
	if err != nil {
		t.Fatal(err)
	}
	currentPods := podsByNodeMatchingHash(manager, hash)
	// 在时间点 300 将两个新 Pod 标记为就绪。
	setPodReadiness(t, manager, true, 2, func(pod *v1.Pod) bool {
		return pod.Labels[apps.ControllerRevisionHashLabelKey] == hash
	})
	// 再把某个尚未创建新 Pod 的节点上的旧 Pod 标记为未就绪，制造补位需求。
	setPodReadiness(t, manager, false, 1, func(pod *v1.Pod) bool {
		nodeName, err := util.GetTargetNodeName(pod)
		if err != nil {
			t.Fatal(err)
		}
		return pod.Labels[apps.ControllerRevisionHashLabelKey] != hash && len(currentPods[nodeName]) == 0
	})

	// 此时新 Pod 还未满足 minReadySeconds，删除旧 Pod 释放的名额仍需新建一个 Pod 补上。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(310, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 1, 0, 0)

	// 推进到新 Pod 满足最短就绪时间后，旧 Pod 才会被删除。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(320, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 1, 3, 0)

	// 在时间点 320 将所有新 Pod 标记为就绪。
	currentPods = podsByNodeMatchingHash(manager, hash)
	setPodReadiness(t, manager, true, 3, func(pod *v1.Pod) bool {
		return pod.Labels[apps.ControllerRevisionHashLabelKey] == hash
	})

	// 剩余旧 Pod 现在可以被安全删除。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(340, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 2, 0)

	// 此时升级完成，不应再有任何变更。
	manager.failedPodsBackoff.Clock = testingclock.NewFakeClock(time.Unix(350, 0))
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
}

// podsByNodeMatchingHash 按节点聚合具有指定 ControllerRevision hash 的 Pod 名称，
// 方便测试断言某个节点上是否已经存在新版本的 Pod。
func podsByNodeMatchingHash(dsc *daemonSetsController, hash string) map[string][]string {
	byNode := make(map[string][]string)
	for _, obj := range dsc.podStore.List() {
		pod := obj.(*v1.Pod)
		if pod.Labels[apps.ControllerRevisionHashLabelKey] != hash {
			continue
		}
		nodeName, err := util.GetTargetNodeName(pod)
		if err != nil {
			panic(err)
		}
		byNode[nodeName] = append(byNode[nodeName], pod.Name)
	}
	return byNode
}

// setPodReadiness 将最多 count 个满足 fn 的 Pod 设为指定的就绪状态：
// 跳过正在删除以及状态已符合的 Pod；由于 UpdatePodCondition 内部直接调用 time.Now()，
// 这里把 LastTransitionTime 改写为控制器假时钟的时间，以便稳定验证 minReadySeconds 行为。
func setPodReadiness(t *testing.T, dsc *daemonSetsController, ready bool, count int, fn func(*v1.Pod) bool) {
	t.Helper()
	logger, _ := ktesting.NewTestContext(t)
	for _, obj := range dsc.podStore.List() {
		if count <= 0 {
			break
		}
		pod := obj.(*v1.Pod)
		if pod.DeletionTimestamp != nil {
			continue
		}
		if podutil.IsPodReady(pod) == ready {
			continue
		}
		if !fn(pod) {
			continue
		}
		condition := v1.PodCondition{Type: v1.PodReady}
		if ready {
			condition.Status = v1.ConditionTrue
		} else {
			condition.Status = v1.ConditionFalse
		}
		if !podutil.UpdatePodCondition(&pod.Status, &condition) {
			t.Fatal("failed to update pod")
		}
		// TODO: workaround UpdatePodCondition calling time.Now() directly
		setCondition := podutil.GetPodReadyCondition(pod.Status)
		setCondition.LastTransitionTime.Time = dsc.failedPodsBackoff.Clock.Now()
		logger.Info("marked pod ready", "pod", pod.Name, "ready", ready)
		count--
	}
	if count > 0 {
		t.Fatalf("could not mark %d pods ready=%t", count, ready)
	}
}

// currentDSHash 构造 DaemonSet 的控制器Revision历史，并返回当前版本标签中的唯一 hash。
func currentDSHash(dsc *daemonSetsController, ds *apps.DaemonSet) (string, error) {
	// 构造 DaemonSet 的历史版本，随后从中取出当前版本的 hash。
	cur, _, err := dsc.constructHistory(context.TODO(), ds)
	if err != nil {
		return "", err
	}
	return cur.Labels[apps.DefaultDaemonSetUniqueLabelKey], nil

}

// TestDaemonSetUpdatesNoTemplateChanged 验证仅更新滚动策略而 Pod 模板未变化时不会删除或创建任何 Pod。
func TestDaemonSetUpdatesNoTemplateChanged(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ds := newDaemonSet("foo")
	manager, podControl, _, err := newTestController(ctx, ds)
	if err != nil {
		t.Fatalf("error creating DaemonSets controller: %v", err)
	}
	maxUnavailable := 3
	addNodes(manager.nodeStore, 0, 5, nil)
	manager.dsStore.Add(ds)
	expectSyncDaemonSets(t, manager, ds, podControl, 5, 0, 0)

	ds.Spec.UpdateStrategy.Type = apps.RollingUpdateDaemonSetStrategyType
	intStr := intstr.FromInt32(int32(maxUnavailable))
	ds.Spec.UpdateStrategy.RollingUpdate = &apps.RollingUpdateDaemonSet{MaxUnavailable: &intStr}
	manager.dsStore.Update(ds)

	// 模板未变，因此不应删除任何 Pod。
	clearExpectations(t, manager, ds, podControl)
	expectSyncDaemonSets(t, manager, ds, podControl, 0, 0, 0)
	clearExpectations(t, manager, ds, podControl)
}

// newUpdateSurge 构造一个 RollingUpdate 策略：maxUnavailable 为 0，maxSurge 由参数指定。
func newUpdateSurge(value intstr.IntOrString) apps.DaemonSetUpdateStrategy {
	zero := intstr.FromInt32(0)
	return apps.DaemonSetUpdateStrategy{
		Type: apps.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &apps.RollingUpdateDaemonSet{
			MaxUnavailable: &zero,
			MaxSurge:       &value,
		},
	}
}

// newUpdateUnavailable 构造一个 RollingUpdate 策略，仅指定 maxUnavailable。
func newUpdateUnavailable(value intstr.IntOrString) apps.DaemonSetUpdateStrategy {
	return apps.DaemonSetUpdateStrategy{
		Type: apps.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &apps.RollingUpdateDaemonSet{
			MaxUnavailable: &value,
		},
	}
}

// TestGetUnavailableNumbers 表驱动验证 updatedDesiredNodeCounts 对 maxSurge、maxUnavailable、
// 期望调度节点数和空节点数的计算：覆盖无节点、节点就绪、空节点、百分比配置以及 Pod 正在删除等场景。
func TestGetUnavailableNumbers(t *testing.T) {
	cases := []struct {
		name                   string
		ManagerFunc            func(ctx context.Context) *daemonSetsController
		ds                     *apps.DaemonSet
		nodeToPods             map[string][]*v1.Pod
		maxSurge               int
		maxUnavailable         int
		desiredNumberScheduled int
		emptyNodes             int
		Err                    error
	}{
		{
			name: "No nodes",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateUnavailable(intstr.FromInt32(0))
				return ds
			}(),
			nodeToPods:     make(map[string][]*v1.Pod),
			maxUnavailable: 0,
			emptyNodes:     0,
		},
		{
			name: "Two nodes with ready pods",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateUnavailable(intstr.FromInt32(1))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				pod1 := newPod("pod-1", "node-1", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				markPodReady(pod1)
				mapping["node-0"] = []*v1.Pod{pod0}
				mapping["node-1"] = []*v1.Pod{pod1}
				return mapping
			}(),
			maxUnavailable:         1,
			desiredNumberScheduled: 2,
			emptyNodes:             0,
		},
		{
			name: "Two nodes, one node without pods",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateUnavailable(intstr.FromInt32(0))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				mapping["node-0"] = []*v1.Pod{pod0}
				return mapping
			}(),
			maxUnavailable:         1,
			desiredNumberScheduled: 2,
			emptyNodes:             1,
		},
		{
			name: "Two nodes, one node without pods, surge",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromInt32(0))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				mapping["node-0"] = []*v1.Pod{pod0}
				return mapping
			}(),
			maxUnavailable:         1,
			desiredNumberScheduled: 2,
			emptyNodes:             1,
		},
		{
			name: "Two nodes with pods, MaxUnavailable in percents",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateUnavailable(intstr.FromString("50%"))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				pod1 := newPod("pod-1", "node-1", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				markPodReady(pod1)
				mapping["node-0"] = []*v1.Pod{pod0}
				mapping["node-1"] = []*v1.Pod{pod1}
				return mapping
			}(),
			maxUnavailable:         1,
			desiredNumberScheduled: 2,
			emptyNodes:             0,
		},
		{
			name: "Two nodes with pods, MaxUnavailable in percents, surge",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromString("50%"))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				pod1 := newPod("pod-1", "node-1", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				markPodReady(pod1)
				mapping["node-0"] = []*v1.Pod{pod0}
				mapping["node-1"] = []*v1.Pod{pod1}
				return mapping
			}(),
			maxSurge:               1,
			maxUnavailable:         0,
			desiredNumberScheduled: 2,
			emptyNodes:             0,
		},
		{
			name: "Two nodes with pods, MaxUnavailable is 100%, surge",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 2, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateSurge(intstr.FromString("100%"))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				pod1 := newPod("pod-1", "node-1", simpleDaemonSetLabel, nil)
				markPodReady(pod0)
				markPodReady(pod1)
				mapping["node-0"] = []*v1.Pod{pod0}
				mapping["node-1"] = []*v1.Pod{pod1}
				return mapping
			}(),
			maxSurge:               2,
			maxUnavailable:         0,
			desiredNumberScheduled: 2,
			emptyNodes:             0,
		},
		{
			name: "Two nodes with pods, MaxUnavailable in percents, pod terminating",
			ManagerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				addNodes(manager.nodeStore, 0, 3, nil)
				return manager
			},
			ds: func() *apps.DaemonSet {
				ds := newDaemonSet("x")
				ds.Spec.UpdateStrategy = newUpdateUnavailable(intstr.FromString("50%"))
				return ds
			}(),
			nodeToPods: func() map[string][]*v1.Pod {
				mapping := make(map[string][]*v1.Pod)
				pod0 := newPod("pod-0", "node-0", simpleDaemonSetLabel, nil)
				pod1 := newPod("pod-1", "node-1", simpleDaemonSetLabel, nil)
				now := metav1.Now()
				markPodReady(pod0)
				markPodReady(pod1)
				pod1.DeletionTimestamp = &now
				mapping["node-0"] = []*v1.Pod{pod0}
				mapping["node-1"] = []*v1.Pod{pod1}
				return mapping
			}(),
			maxUnavailable:         2,
			desiredNumberScheduled: 3,
			emptyNodes:             1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			manager := c.ManagerFunc(ctx)
			manager.dsStore.Add(c.ds)
			nodeList, err := manager.nodeLister.List(labels.Everything())
			if err != nil {
				t.Fatalf("error listing nodes: %v", err)
			}
			maxSurge, maxUnavailable, desiredNumberScheduled, err := manager.updatedDesiredNodeCounts(ctx, c.ds, nodeList, c.nodeToPods)
			if err != nil && c.Err != nil {
				if c.Err != err {
					t.Fatalf("Expected error: %v but got: %v", c.Err, err)
				}
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if maxSurge != c.maxSurge || maxUnavailable != c.maxUnavailable || desiredNumberScheduled != c.desiredNumberScheduled {
				t.Errorf("Wrong values. maxSurge: %d, expected %d, maxUnavailable: %d, expected: %d, desiredNumberScheduled: %d, expected: %d", maxSurge, c.maxSurge, maxUnavailable, c.maxUnavailable, desiredNumberScheduled, c.desiredNumberScheduled)
			}
			var emptyNodes int
			for _, pods := range c.nodeToPods {
				if len(pods) == 0 {
					emptyNodes++
				}
			}
			if emptyNodes != c.emptyNodes {
				t.Errorf("expected numEmpty to be %d, was %d", c.emptyNodes, emptyNodes)
			}
		})
	}
}

// TestControlledHistories 验证 controlledHistories 只返回由该 DaemonSet 所有、且位于同一命名空间的
// ControllerRevision：不同命名空间的孤立 Revision 不应被纳入，即使标签相同。
func TestControlledHistories(t *testing.T) {
	ds1 := newDaemonSet("ds1")
	crOfDs1 := newControllerRevision(ds1.GetName()+"-x1", ds1.GetNamespace(), ds1.Spec.Template.Labels,
		[]metav1.OwnerReference{*metav1.NewControllerRef(ds1, controllerKind)})
	orphanCrInSameNsWithDs1 := newControllerRevision(ds1.GetName()+"-x2", ds1.GetNamespace(), ds1.Spec.Template.Labels, nil)
	orphanCrNotInSameNsWithDs1 := newControllerRevision(ds1.GetName()+"-x3", ds1.GetNamespace()+"-other", ds1.Spec.Template.Labels, nil)
	cases := []struct {
		name                      string
		managerFunc               func(ctx context.Context) *daemonSetsController
		historyCRAll              []*apps.ControllerRevision
		expectControllerRevisions []*apps.ControllerRevision
	}{
		{
			name: "controller revision in the same namespace",
			managerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx, ds1, crOfDs1, orphanCrInSameNsWithDs1)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				manager.dsStore.Add(ds1)
				return manager
			},
			historyCRAll:              []*apps.ControllerRevision{crOfDs1, orphanCrInSameNsWithDs1},
			expectControllerRevisions: []*apps.ControllerRevision{crOfDs1, orphanCrInSameNsWithDs1},
		},
		{
			name: "Skip adopting the controller revision in namespace other than the one in which DS lives",
			managerFunc: func(ctx context.Context) *daemonSetsController {
				manager, _, _, err := newTestController(ctx, ds1, orphanCrNotInSameNsWithDs1)
				if err != nil {
					t.Fatalf("error creating DaemonSets controller: %v", err)
				}
				manager.dsStore.Add(ds1)
				return manager
			},
			historyCRAll:              []*apps.ControllerRevision{orphanCrNotInSameNsWithDs1},
			expectControllerRevisions: []*apps.ControllerRevision{},
		},
	}
	for _, c := range cases {
		_, ctx := ktesting.NewTestContext(t)
		manager := c.managerFunc(ctx)
		for _, h := range c.historyCRAll {
			manager.historyStore.Add(h)
		}
		crList, err := manager.controlledHistories(context.TODO(), ds1)
		if err != nil {
			t.Fatalf("Test case: %s. Unexpected error: %v", c.name, err)
		}
		if len(crList) != len(c.expectControllerRevisions) {
			t.Errorf("Test case: %s, expect controllerrevision count %d but got:%d",
				c.name, len(c.expectControllerRevisions), len(crList))
		} else {
			// 逐个检查返回的 Revision 是否都在期望集合中。
			for _, cr := range crList {
				found := false
				for _, expectCr := range c.expectControllerRevisions {
					if reflect.DeepEqual(cr, expectCr) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Test case: %s, controllerrevision %v not expected",
						c.name, cr)
				}
			}
			t.Logf("Test case: %s done", c.name)
		}
	}
}
