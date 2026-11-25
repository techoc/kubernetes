/*
Copyright The Kubernetes Authors.

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

package podgroupprotection

import (
	"context"
	"slices"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/ptr"
)

const (
	// 测试共用的命名空间。
	defaultNS = "default"
	// 测试共用的 PodGroup 名称。
	defaultPGName = "my-podgroup"
	// 测试共用的 PodGroup UID，用于构造稳定的对象引用。
	defaultPGUID = "pg-uid-1"
)

// podGroup 构造一个不带 finalizer、未进入删除流程的默认 PodGroup。
func podGroup() *schedulingv1beta1.PodGroup {
	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultPGName,
			Namespace: defaultNS,
			UID:       defaultPGUID,
		},
	}
}

// withFinalizer 给 PodGroup 加上保护 finalizer，使其成为控制器的处理对象。
func withFinalizer(pg *schedulingv1beta1.PodGroup) *schedulingv1beta1.PodGroup {
	pg.Finalizers = append(pg.Finalizers, scheduling.PodGroupProtectionFinalizer)
	return pg
}

// deletedPodGroup 给 PodGroup 设置删除时间戳，模拟用户已发出删除请求。
func deletedPodGroup(pg *schedulingv1beta1.PodGroup) *schedulingv1beta1.PodGroup {
	pg.DeletionTimestamp = &metav1.Time{}
	return pg
}

// podForPG 构造一个已调度到 node-1、处于 Running 且引用指定 PodGroup 的 Pod。
func podForPG(name string, pgName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultNS,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
			SchedulingGroup: &v1.PodSchedulingGroup{
				PodGroupName: ptr.To(pgName),
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
}

// terminatedPod 把 Pod 的相位改为给定的终态，用于模拟运行结束的 Pod。
func terminatedPod(pod *v1.Pod, phase v1.PodPhase) *v1.Pod {
	pod.Status.Phase = phase
	return pod
}

// unscheduledPod 清空 Pod 的 NodeName，模拟尚未被调度的 Pod（不计入活跃 Pod）。
func unscheduledPod(pod *v1.Pod) *v1.Pod {
	pod.Spec.NodeName = ""
	return pod
}

// podWithoutSchedulingGroup 构造没有 SchedulingGroup 字段的 Pod，用于验证控制器忽略它。
func podWithoutSchedulingGroup(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultNS,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
}

// TestIsPodTerminated 验证只有 Succeeded 与 Failed 被视为终态，
// Running 与 Pending 都不是终态——这直接决定 finalizer 能否被移除。
func TestIsPodTerminated(t *testing.T) {
	tests := map[string]struct {
		// phase 是待判断的 Pod 相位。
		phase v1.PodPhase
		// want 是预期的终态判定结果。
		want bool
	}{
		// Running 与 Pending 都是非终态，必须保留对 PodGroup 的保护。
		"running": {phase: v1.PodRunning, want: false},
		"pending": {phase: v1.PodPending, want: false},
		// 成功与失败都是终态，允许释放 finalizer。
		"succeeded": {phase: v1.PodSucceeded, want: true},
		"failed":    {phase: v1.PodFailed, want: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := &v1.Pod{Status: v1.PodStatus{Phase: tc.phase}}
			if got := isPodTerminated(pod); got != tc.want {
				t.Errorf("isPodTerminated(%v) = %v, want %v", tc.phase, got, tc.want)
			}
		})
	}
}

// TestGetPod 验证 getPod 能同时处理普通 Pod、删除 tombstone 以及非 Pod 对象，
// 保证 informer 的 DeletedFinalStateUnknown 事件不会丢失。
func TestGetPod(t *testing.T) {
	tests := map[string]struct {
		// obj 是传给 getPod 的 informer 事件对象。
		obj interface{}
		// want 表示期望是否解析出非空 Pod。
		want bool
	}{
		"nil": {
			obj:  nil,
			want: false,
		},
		"pod": {
			obj:  &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}},
			want: true,
		},
		// tombstone 中包装的是 Pod，应当被正确解包。
		"tombstone with pod": {
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/p",
				Obj: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}},
			},
			want: true,
		},
		// tombstone 中包装的不是 Pod，必须返回 nil 而非误判。
		"tombstone with non-pod": {
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/p",
				Obj: &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "p"}},
			},
			want: false,
		},
		"non-pod object": {
			obj:  &v1.ConfigMap{},
			want: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := getPod(tc.obj)
			if (got != nil) != tc.want {
				t.Errorf("parsePod() returned pod=%v, want non-nil=%v", got, tc.want)
			}
		})
	}
}

// TestHandlePodChange 验证 Pod 事件何时需要把对应 PodGroup 入队。
// 关注删除/终态事件、UID 变化与去重语义：workqueue 会合并同一 key。
func TestHandlePodChange(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	tests := map[string]struct {
		// old 与 new 是 handlePodChange 收到的前后对象（nil 表示创建或删除）。
		old interface{}
		new interface{}
		// wantSize 是处理后队列中预期的条目数，体现是否入队及去重结果。
		wantSize int
	}{
		// Pod 被删除且引用 PodGroup，需入队以重新评估是否还有活跃 Pod。
		"deleted pod with schedulingGroup enqueues": {
			old:      podForPG("pod-1", defaultPGName),
			new:      nil,
			wantSize: 1,
		},
		// Pod 进入终态等价于"不再活跃"，仍需入队更新 PodGroup 保护状态。
		"terminated pod enqueues": {
			old:      podForPG("pod-1", defaultPGName),
			new:      terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded),
			wantSize: 1,
		},
		// 未调度的 Pod 不占用资源，新增时不改变活跃 Pod 计数，无需入队。
		"unscheduled new pod does not enqueue": {
			old:      nil,
			new:      unscheduledPod(podForPG("pod-1", defaultPGName)),
			wantSize: 0,
		},
		// 已调度的 Running Pod 是新增活跃 Pod，但本控制器只在 Pod 减少时移除 finalizer，故无需入队。
		"scheduled running pod does not enqueue": {
			old:      nil,
			new:      podForPG("pod-1", defaultPGName),
			wantSize: 0,
		},
		// 删除不带 SchedulingGroup 的 Pod 无法关联到 PodGroup，无需入队。
		"deleted pod without schedulingGroup does not enqueue": {
			old:      podWithoutSchedulingGroup("pod-1"),
			new:      nil,
			wantSize: 0,
		},
		// PodGroupName 为 nil 时同样无法关联到具体 PodGroup。
		"deleted pod with nil podGroupName does not enqueue": {
			old: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: defaultNS},
				Spec: v1.PodSpec{
					SchedulingGroup: &v1.PodSchedulingGroup{PodGroupName: nil},
				},
			},
			new:      nil,
			wantSize: 0,
		},
		// UID 不同但新旧 Pod 引用同一 PodGroup，两个入口产生相同 key，workqueue 去重后只剩一条。
		"UID mismatch with terminated new pod, same PodGroup, deduplicates enqueue": {
			old: func() *v1.Pod {
				p := podForPG("pod-1", defaultPGName)
				p.UID = "old-uid"
				return p
			}(),
			new: func() interface{} {
				p := terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded)
				p.UID = "new-uid"
				return p
			}(),
			wantSize: 1,
		},
		// UID 不同且引用不同 PodGroup，会产生两个不同的 key，应各自入队。
		"UID mismatch with terminated new pod referencing different PodGroup enqueues both": {
			old: func() *v1.Pod {
				p := podForPG("pod-1", defaultPGName)
				p.UID = "old-uid"
				return p
			}(),
			new: func() interface{} {
				p := terminatedPod(podForPG("pod-1", "other-pg"), v1.PodFailed)
				p.UID = "new-uid"
				return p
			}(),
			wantSize: 2,
		},
		// 新 Pod 非终态时只有旧 Pod 的删除需要入队，新 Pod 不产生额外条目。
		"UID mismatch on update with non-terminated new pod enqueues only old pod": {
			old: func() *v1.Pod {
				p := podForPG("pod-1", defaultPGName)
				p.UID = "old-uid"
				return p
			}(),
			new: func() interface{} {
				p := unscheduledPod(podForPG("pod-1", defaultPGName))
				p.UID = "new-uid"
				return p
			}(),
			wantSize: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// 使用真实 workqueue 才能验证去重行为（Len 会因相同 key 合并而小于事件数）。
			c := &Controller{
				queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
			}
			defer c.queue.ShutDown()
			c.handlePodChange(logger, tc.old, tc.new)

			if c.queue.Len() != tc.wantSize {
				t.Errorf("queue size = %d, want %d", c.queue.Len(), tc.wantSize)
			}
		})
	}
}

// TestPodGroupProtectionController 是端到端用例：启动真实 informer 与控制器，
// 通过 fake client 注入对象，轮询验证 finalizer 的最终状态。
func TestPodGroupProtectionController(t *testing.T) {
	tests := []struct {
		name string
		// 控制器启动前预置到 fake client 中的对象。
		initialObjects []runtime.Object
		// 控制器启动后要删除的 Pod 名称（位于 defaultNS）。
		podToDelete string
		// 控制器处理完成后，PodGroup 上是否应当仍有保护 finalizer。
		expectFinalizer bool
	}{
		{
			// 新 PodGroup 不加 finalizer 是 admission 插件的职责，本控制器不应越权添加。
			name:            "new PodGroup without finalizer, no action (admission plugin handles it)",
			initialObjects:  []runtime.Object{podGroup()},
			expectFinalizer: false,
		},
		{
			// 即使存在活跃 Pod，新建 PodGroup 的 finalizer 仍由 admission 插件处理。
			name:            "new PodGroup without finalizer, active pod exists, then no action (admission plugin handles it)",
			initialObjects:  []runtime.Object{podGroup(), podForPG("pod-1", defaultPGName)},
			expectFinalizer: false,
		},
		{
			// 已有 finalizer 且未被删除时保持现状。
			name:            "PodGroup with finalizer, not being deleted, then no action",
			initialObjects:  []runtime.Object{withFinalizer(podGroup())},
			expectFinalizer: true,
		},
		{
			// 待删除且无活跃 Pod，finalizer 应被移除，让删除得以完成。
			name:            "deleted PodGroup with finalizer, no active pods, then finalizer is removed",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup()))},
			expectFinalizer: false,
		},
		{
			// 仍有活跃 Pod 时必须保留 finalizer，阻止 PodGroup 被提前删除。
			name:            "deleted PodGroup with finalizer, active pod exists, then finalizer is kept",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup())), podForPG("pod-1", defaultPGName)},
			expectFinalizer: true,
		},
		{
			// 只剩终态 Pod 等同于没有活跃 Pod，finalizer 可以移除。
			name:            "deleted PodGroup with finalizer, only terminated pods, then finalizer is removed",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup())), terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded)},
			expectFinalizer: false,
		},
		{
			// 活跃与终态 Pod 混合时，只要有一个活跃 Pod 就保留 finalizer。
			name: "deleted PodGroup with finalizer, mix of active and terminated pods, then finalizer is kept",
			initialObjects: []runtime.Object{
				deletedPodGroup(withFinalizer(podGroup())),
				podForPG("pod-active", defaultPGName),
				terminatedPod(podForPG("pod-done", defaultPGName), v1.PodSucceeded),
			},
			expectFinalizer: true,
		},
		{
			// 删除中但没有 finalizer 的对象不是本控制器的处理候选。
			name:            "PodGroup without finalizer, already deleted, then no action (not a deletion candidate)",
			initialObjects:  []runtime.Object{deletedPodGroup(podGroup())},
			expectFinalizer: false,
		},
		{
			// 不能给正在删除的对象补加 finalizer，否则会阻塞删除。
			name:            "PodGroup without finalizer, already deleted, active pods exist, then no action (should not add finalizer to deleting object)",
			initialObjects:  []runtime.Object{deletedPodGroup(podGroup()), podForPG("pod-1", defaultPGName)},
			expectFinalizer: false,
		},
		{
			// 删除最后一个活跃 Pod 后，应触发 PodGroup 的 finalizer 移除。
			name:            "pod deleted, PodGroup being deleted with finalizer, was last active pod, finalizer is removed",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup())), podForPG("pod-1", defaultPGName)},
			podToDelete:     "pod-1",
			expectFinalizer: false,
		},
		{
			// Pod 以 Succeeded 终态结束，同样释放对 PodGroup 的保护。
			name:            "pod terminated succeeded, PodGroup being deleted with finalizer, was last active pod, then finalizer is removed",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup())), terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded)},
			expectFinalizer: false,
		},
		{
			// Pod 以 Failed 终态结束，同样释放对 PodGroup 的保护。
			name:            "pod terminated failed, PodGroup being deleted with finalizer, was last active pod, then finalizer is removed",
			initialObjects:  []runtime.Object{deletedPodGroup(withFinalizer(podGroup())), terminatedPod(podForPG("pod-1", defaultPGName), v1.PodFailed)},
			expectFinalizer: false,
		},
		{
			// 未调度的 Pod 不构成活跃 Pod，且新建 PodGroup 不加 finalizer。
			name:            "new unscheduled pod, PodGroup without finalizer, then no action (admission plugin handles it)",
			initialObjects:  []runtime.Object{podGroup(), unscheduledPod(podForPG("pod-1", defaultPGName))},
			expectFinalizer: false,
		},
		{
			// 没有 SchedulingGroup 的 Pod 无法关联到 PodGroup，不影响其 finalizer。
			name:            "pod without schedulingGroup -> no PodGroup action",
			initialObjects:  []runtime.Object{withFinalizer(podGroup()), podWithoutSchedulingGroup("pod-1")},
			expectFinalizer: true,
		},
		{
			// 终态 Pod 引用不存在的 PodGroup 时控制器应优雅处理，不能报错卡住。
			name:            "terminated pod references non-existent PodGroup, controller handles gracefully",
			initialObjects:  []runtime.Object{terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded)},
			expectFinalizer: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)

			client := fake.NewClientset(test.initialObjects...)
			informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
			pgInformer := informerFactory.Scheduling().V1beta1().PodGroups()
			podInformer := informerFactory.Core().V1().Pods()

			ctrl, err := NewPodGroupProtectionController(klog.FromContext(ctx), pgInformer, podInformer, client)
			if err != nil {
				t.Fatalf("unexpected error creating controller: %v", err)
			}

			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())
			go ctrl.Run(ctx, 1)

			// 为降低 flakiness，先创建一个"预热" Pod 确保 watch 通道已建立：
			// 它在 LIST（WaitForCacheSync）之后创建，informer 必然通过 WATCH 看到它。
			syncPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sync-pod", Namespace: defaultNS}}
			_, err = client.CoreV1().Pods(defaultNS).Create(ctx, syncPod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("creating sync pod: %v", err)
			}

			// 等待 informer 缓存真正看到预热 Pod，再执行后续删除，避免事件竞争。
			err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
				_, err = podInformer.Lister().Pods(defaultNS).Get(syncPod.Name)
				return err == nil, nil
			})
			if err != nil {
				t.Fatalf("timed out waiting for informer to see the sync pod: %v", err)
			}

			// 需要删 Pod 的场景才执行删除，确保事件由控制器实时处理。
			if test.podToDelete != "" {
				if err := client.CoreV1().Pods(defaultNS).Delete(ctx, test.podToDelete, metav1.DeleteOptions{}); err != nil {
					t.Fatalf("deleting pod: %v", err)
				}
			}

			// 轮询最终状态：PodGroup 被删除或 finalizer 状态符合预期即可。
			// 用轮询而非固定 sleep，既稳定又能尽快通过。
			if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
				pg, err := client.SchedulingV1beta1().PodGroups(defaultNS).Get(ctx, defaultPGName, metav1.GetOptions{})
				// NotFound 说明 PodGroup 已被删除，等价于"无 finalizer"。
				if apierrors.IsNotFound(err) {
					return !test.expectFinalizer, nil
				}
				if err != nil {
					return false, err
				}
				hasFinalizer := slices.Contains(pg.Finalizers, scheduling.PodGroupProtectionFinalizer)
				return hasFinalizer == test.expectFinalizer, nil
			}); err != nil {
				t.Fatalf("timed out waiting for expected finalizer state (want present=%v): %v", test.expectFinalizer, err)
			}
		})
	}
}

// TestActivePodSchedulingGroupIndexer 验证索引只收录"活跃且引用 PodGroup"的 Pod：
// 无 SchedulingGroup、PodGroupName 为 nil 以及终态 Pod 都应被排除。
func TestActivePodSchedulingGroupIndexer(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := addActivePodSchedulingGroupIndexer(indexer); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pod1 := podForPG("pod-1", "pg-a")
	_ = indexer.Add(pod1)

	pod2 := podWithoutSchedulingGroup("pod-2")
	_ = indexer.Add(pod2)

	pod3 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: defaultNS, UID: "pod-3-uid"},
		Spec: v1.PodSpec{
			SchedulingGroup: &v1.PodSchedulingGroup{PodGroupName: nil},
		},
	}
	_ = indexer.Add(pod3)

	// 终态 Pod 不应进入活跃 Pod 索引。
	pod4 := terminatedPod(podForPG("pod-4", "pg-a"), v1.PodSucceeded)
	_ = indexer.Add(pod4)

	objs, err := indexer.ByIndex(activePodSchedulingGroupIndex, defaultNS+"/pg-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 active pod for pg-a, got %d", len(objs))
	}
	if objs[0].(*v1.Pod).Name != "pod-1" {
		t.Errorf("expected pod-1, got %s", objs[0].(*v1.Pod).Name)
	}

	// 查询不存在的 PodGroup 应返回空列表。
	objs, err = indexer.ByIndex(activePodSchedulingGroupIndex, defaultNS+"/nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(objs) != 0 {
		t.Errorf("expected 0 pods for nonexistent PodGroup, got %d", len(objs))
	}
}

// TestHasActivePods 验证 hasActivePods 通过索引准确判断 PodGroup 是否还有活跃 Pod，
// 这是决定是否移除 finalizer 的核心依据。
func TestHasActivePods(t *testing.T) {
	tests := map[string]struct {
		// pods 是预置到索引器中的 Pod 列表。
		pods []runtime.Object
		// want 是期望是否存在活跃 Pod。
		want bool
	}{
		// 没有任何 Pod 时显然没有活跃 Pod。
		"no pods": {
			pods: nil,
			want: false,
		},
		// 引用目标 PodGroup 的运行中 Pod 应被识别为活跃。
		"active pod referencing PodGroup": {
			pods: []runtime.Object{
				podForPG("pod-1", defaultPGName),
			},
			want: true,
		},
		// 只有终态 Pod（Succeeded/Failed）时不算活跃。
		"only terminated pods": {
			pods: []runtime.Object{
				terminatedPod(podForPG("pod-1", defaultPGName), v1.PodSucceeded),
				terminatedPod(podForPG("pod-2", defaultPGName), v1.PodFailed),
			},
			want: false,
		},
		// 活跃与终态混合时，只要有一个活跃 Pod 就返回 true。
		"mix of active and terminated": {
			pods: []runtime.Object{
				podForPG("pod-active", defaultPGName),
				terminatedPod(podForPG("pod-done", defaultPGName), v1.PodSucceeded),
			},
			want: true,
		},
		// 引用其他 PodGroup 的 Pod 不应计入当前 PodGroup。
		"pods referencing different PodGroup": {
			pods: []runtime.Object{
				podForPG("pod-1", "other-pg"),
			},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := addActivePodSchedulingGroupIndexer(indexer); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, obj := range tc.pods {
				_ = indexer.Add(obj)
			}

			ctrl := &Controller{
				podIndexer: indexer,
			}
			pg := podGroup()
			got, err := ctrl.hasActivePods(context.Background(), pg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("hasActivePods() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestHandlePodGroupUpdate 验证只有"正在删除且带 finalizer"的 PodGroup 才会入队，
// 其他更新无需控制器介入。
func TestHandlePodGroupUpdate(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	tests := map[string]struct {
		// pg 是传入 handlePodGroupUpdate 的 PodGroup；wantSize 是期望的队列长度。
		pg *schedulingv1beta1.PodGroup
		// 同样借助真实 workqueue 验证入队次数。
		wantSize int
	}{
		// 没有 finalizer 且未删除的 PodGroup 不入队。
		"PodGroup without finalizer, not deleting/not enqueued": {
			pg:       podGroup(),
			wantSize: 0,
		},
		// 处于删除候选状态（有 finalizer 且有删除时间戳）才需要入队。
		"PodGroup is deletion candidate -> enqueued": {
			pg:       deletedPodGroup(withFinalizer(podGroup())),
			wantSize: 1,
		},
		// 有 finalizer 但未删除时保持现状，无需入队。
		"PodGroup has finalizer, not deleting -> not enqueued": {
			pg:       withFinalizer(podGroup()),
			wantSize: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := &Controller{
				queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
			}
			defer c.queue.ShutDown()
			c.handlePodGroupUpdate(logger, tc.pg)
			if c.queue.Len() != tc.wantSize {
				t.Errorf("queue size = %d, want %d", c.queue.Len(), tc.wantSize)
			}
		})
	}
}
