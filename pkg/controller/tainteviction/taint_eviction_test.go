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

package tainteviction

import (
	"context"
	"fmt"
	goruntime "runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller/testutil"
)

// 短暂等待时间，用于让控制器有机会处理事件，避免断言过早。
var timeForControllerToProgressForSanityCheck = 20 * time.Millisecond

// getPodsAssignedToNode 返回一个查询函数，通过 field selector 从 fake client 中
// 列出调度到指定节点的所有 Pod，模拟 TaintManager 对 Pod 的按节点查询。
func getPodsAssignedToNode(ctx context.Context, c *fake.Clientset) GetPodsByNodeNameFunc {
	return func(nodeName string) ([]*corev1.Pod, error) {
		selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
		pods, err := c.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			return []*corev1.Pod{}, fmt.Errorf("failed to get Pods assigned to node %v", nodeName)
		}
		rPods := make([]*corev1.Pod, len(pods.Items))
		for i := range pods.Items {
			rPods[i] = &pods.Items[i]
		}
		return rPods, nil
	}
}

// createNoExecuteTaint 按索引生成带/testTaint<index> 键值的 NoExecute 污点，
// 并记录当前时间作为 TimeAdded。
func createNoExecuteTaint(index int) corev1.Taint {
	now := metav1.Now()
	return corev1.Taint{
		Key:       "testTaint" + fmt.Sprintf("%v", index),
		Value:     "test" + fmt.Sprintf("%v", index),
		Effect:    corev1.TaintEffectNoExecute,
		TimeAdded: &now,
	}
}

// addToleration 给 Pod 添加一个匹配指定索引污点的容忍；
// duration 为负数表示无限期容忍，否则为带 TolerationSeconds 的有限期容忍。
func addToleration(pod *corev1.Pod, index int, duration int64) *corev1.Pod {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if duration < 0 {
		pod.Spec.Tolerations = []corev1.Toleration{{Key: "testTaint" + fmt.Sprintf("%v", index), Value: "test" + fmt.Sprintf("%v", index), Effect: corev1.TaintEffectNoExecute}}

	} else {
		pod.Spec.Tolerations = []corev1.Toleration{{Key: "testTaint" + fmt.Sprintf("%v", index), Value: "test" + fmt.Sprintf("%v", index), Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &duration}}
	}
	return pod
}

// addTaintsToNode 为节点设置由索引列表生成的 NoExecute 污点，
// 返回节点本身，便于在测试中链式构造节点对象。
func addTaintsToNode(node *corev1.Node, key, value string, indices []int) *corev1.Node {
	taints := []corev1.Taint{}
	for _, index := range indices {
		taints = append(taints, createNoExecuteTaint(index))
	}
	node.Spec.Taints = taints
	return node
}

// alwaysReady 是测试用的同步检查函数，始终返回 true。
var alwaysReady = func() bool { return true }

// setupNewController 用 fake client 构造一个测试用 Controller，
// 注入 informer 索引器与按节点查询 Pod 的函数，并让 lister 的同步检查始终返回就绪，
// 从而在测试中无需启动完整的 informer。
func setupNewController(ctx context.Context, fakeClientSet *fake.Clientset) (*Controller, cache.Indexer, cache.Indexer) {
	informerFactory := informers.NewSharedInformerFactory(fakeClientSet, 0)
	podIndexer := informerFactory.Core().V1().Pods().Informer().GetIndexer()
	nodeIndexer := informerFactory.Core().V1().Nodes().Informer().GetIndexer()
	mgr, _ := New(ctx, fakeClientSet, informerFactory.Core().V1().Pods(), informerFactory.Core().V1().Nodes(), "taint-eviction-controller")
	mgr.podListerSynced = alwaysReady
	mgr.nodeListerSynced = alwaysReady
	mgr.getPodsAssignedToNode = getPodsAssignedToNode(ctx, fakeClientSet)
	return mgr, podIndexer, nodeIndexer
}

// timestampedPod 记录一组预期在同一时间点被删除的 Pod 名称，
// timestamp 表示从测试开始到删除的预期时长。
type timestampedPod struct {
	// names 为预期在该时间点被删除的 Pod 名称列表。
	names []string
	// timestamp 为预期的驱逐时间点。
	timestamp time.Duration
}

// durationSlice 是 timestampedPod 的切片，实现 sort.Interface 以便按时间排序。
type durationSlice []timestampedPod

// Len/Swap/Less 实现 sort.Interface，使 durationSlice 能按时间戳升序排序。
func (a durationSlice) Len() int           { return len(a) }
func (a durationSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a durationSlice) Less(i, j int) bool { return a[i].timestamp < a[j].timestamp }

// TestFilterNoExecuteTaints 验证 getNoExecuteTaints 只保留 NoExecute 类型的污点，
// 忽略 NoSchedule 等其他效果。
func TestFilterNoExecuteTaints(t *testing.T) {
	taints := []corev1.Taint{
		{
			Key:    "one",
			Value:  "one",
			Effect: corev1.TaintEffectNoExecute,
		},
		{
			Key:    "two",
			Value:  "two",
			Effect: corev1.TaintEffectNoSchedule,
		},
	}
	taints = getNoExecuteTaints(taints)
	if len(taints) != 1 || taints[0].Key != "one" {
		t.Errorf("Filtering doesn't work. Got %v", taints)
	}
}

// TestCreatePod 验证 Pod 创建事件的处理：只有调度到带 NoExecute 污点节点
// 且未能容忍该污点的 Pod 才会被标记并删除；未调度或在无污点节点上的 Pod 应被忽略。
func TestCreatePod(t *testing.T) {
	testCases := []struct {
		description  string
		pod          *corev1.Pod
		taintedNodes map[string][]corev1.Taint
		expectPatch  bool
		expectDelete bool
	}{
		{
			description:  "not scheduled - ignore",
			pod:          testutil.NewPod("pod1", ""),
			taintedNodes: map[string][]corev1.Taint{},
			expectDelete: false,
		},
		{
			description:  "scheduled on untainted Node",
			pod:          testutil.NewPod("pod1", "node1"),
			taintedNodes: map[string][]corev1.Taint{},
			expectDelete: false,
		},
		{
			description: "schedule on tainted Node",
			pod:         testutil.NewPod("pod1", "node1"),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "schedule on tainted Node with finite toleration",
			pod:         addToleration(testutil.NewPod("pod1", "node1"), 1, 100),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectDelete: false,
		},
		{
			description: "schedule on tainted Node with infinite toleration",
			pod:         addToleration(testutil.NewPod("pod1", "node1"), 1, -1),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectDelete: false,
		},
		{
			description: "schedule on tainted Node with infinite invalid toleration",
			pod:         addToleration(testutil.NewPod("pod1", "node1"), 2, -1),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectPatch:  true,
			expectDelete: true,
		},
	}

	for _, item := range testCases {
		// 为每个用例启动控制器，将 Pod 加入索引并触发 PodUpdated 事件，
		// 然后验证是否按预期产生 patch/delete 动作。
		t.Run(item.description, func(t *testing.T) {
			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewSimpleClientset(&corev1.PodList{Items: []corev1.Pod{*item.pod}})
			controller, podIndexer, _ := setupNewController(ctx, fakeClientset)
			controller.recorder = testutil.NewFakeRecorder()
			controller.taintedNodes = item.taintedNodes

			wg.Go(func() {
				controller.Run(ctx)
			})

			podIndexer.Add(item.pod)
			controller.PodUpdated(nil, item.pod)

			verifyPodActions(t, item.description, fakeClientset, item.expectPatch, item.expectDelete)

			cancel()
		})
	}
}

// TestDeletePod 验证 Pod 删除事件的健壮性：删除一个已调度到带污点节点的 Pod 时，
// 控制器不应 panic；通过短暂等待让控制器有机会处理该事件。
func TestDeletePod(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeClientset := fake.NewSimpleClientset()
	controller, _, _ := setupNewController(ctx, fakeClientset)
	controller.recorder = testutil.NewFakeRecorder()
	wg.Go(func() {
		controller.Run(ctx)
	})
	controller.taintedNodes = map[string][]corev1.Taint{
		"node1": {createNoExecuteTaint(1)},
	}
	controller.PodUpdated(testutil.NewPod("pod1", "node1"), nil)
	// 稍作等待，确认删除事件不会引发 panic。
	time.Sleep(timeForControllerToProgressForSanityCheck)
}

// TestUpdatePod 验证 Pod 更新事件的处理：调度到带污点节点、容忍被移除或容忍被延长时，
// 控制器是否正确安排或取消驱逐。
func TestUpdatePod(t *testing.T) {
	testCases := []struct {
		description               string
		prevPod                   *corev1.Pod
		awaitForScheduledEviction bool
		newPod                    *corev1.Pod
		taintedNodes              map[string][]corev1.Taint
		expectPatch               bool
		expectDelete              bool
		skipOnWindows             bool
	}{
		{
			description: "scheduling onto tainted Node",
			prevPod:     testutil.NewPod("pod1", ""),
			newPod:      testutil.NewPod("pod1", "node1"),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "scheduling onto tainted Node with toleration",
			prevPod:     addToleration(testutil.NewPod("pod1", ""), 1, -1),
			newPod:      addToleration(testutil.NewPod("pod1", "node1"), 1, -1),
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectDelete: false,
		},
		{
			description:               "removing toleration",
			prevPod:                   addToleration(testutil.NewPod("pod1", "node1"), 1, 100),
			newPod:                    testutil.NewPod("pod1", "node1"),
			awaitForScheduledEviction: true,
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description:               "lengthening toleration shouldn't work",
			prevPod:                   addToleration(testutil.NewPod("pod1", "node1"), 1, 1),
			newPod:                    addToleration(testutil.NewPod("pod1", "node1"), 1, 100),
			awaitForScheduledEviction: true,
			taintedNodes: map[string][]corev1.Taint{
				"node1": {createNoExecuteTaint(1)},
			},
			expectPatch:   true,
			expectDelete:  true,
			skipOnWindows: true,
		},
	}

	for _, item := range testCases {
		// 逐一运行用例；Windows 上部分时间相关用例存在偶发失败，需跳过。
		t.Run(item.description, func(t *testing.T) {
			if item.skipOnWindows && goruntime.GOOS == "windows" {
				// TODO: 待该偶发失败的测试修复后移除此跳过逻辑。
				t.Skip("Skip flaking test on Windows.")
			}

			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewSimpleClientset(&corev1.PodList{Items: []corev1.Pod{*item.prevPod}})
			controller, podIndexer, _ := setupNewController(context.TODO(), fakeClientset)
			controller.recorder = testutil.NewFakeRecorder()
			controller.taintedNodes = item.taintedNodes

			wg.Go(func() {
				controller.Run(ctx)
			})

			podIndexer.Add(item.prevPod)
			controller.PodUpdated(nil, item.prevPod)

			// 对于移除容忍或延长容忍的用例，先等待驱逐任务进入队列，
			// 再执行 Pod 更新，避免竞态导致断言时机过早。
			if item.awaitForScheduledEviction {
				nsName := types.NamespacedName{Namespace: item.prevPod.Namespace, Name: item.prevPod.Name}
				err := wait.PollImmediate(time.Millisecond*10, time.Second, func() (bool, error) {
					scheduledEviction := controller.taintEvictionQueue.GetWorkerUnsafe(nsName.String())
					return scheduledEviction != nil, nil
				})
				if err != nil {
					t.Fatalf("Failed to await for scheduled eviction: %q", err)
				}
			}

			podIndexer.Update(item.newPod)
			controller.PodUpdated(item.prevPod, item.newPod)

			verifyPodActions(t, item.description, fakeClientset, item.expectPatch, item.expectDelete)
			cancel()
		})
	}
}

// TestCreateNode 验证新建节点事件的处理：若节点带有 NoExecute 污点，
// 则已调度到该节点且不容忍污点的 Pod 应被驱逐；容忍的 Pod 则保留。
func TestCreateNode(t *testing.T) {
	testCases := []struct {
		description  string
		pods         []corev1.Pod
		node         *corev1.Node
		expectPatch  bool
		expectDelete bool
	}{
		{
			description: "Creating Node matching already assigned Pod",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			node:         testutil.NewNode("node1"),
			expectPatch:  false,
			expectDelete: false,
		},
		{
			description: "Creating tainted Node matching already assigned Pod",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			node:         addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "Creating tainted Node matching already assigned tolerating Pod",
			pods: []corev1.Pod{
				*addToleration(testutil.NewPod("pod1", "node1"), 1, -1),
			},
			node:         addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  false,
			expectDelete: false,
		},
	}

	for _, item := range testCases {
		t.Run(item.description, func(t *testing.T) {
			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewClientset(&corev1.PodList{Items: item.pods})
			controller, _, nodeIndexer := setupNewController(ctx, fakeClientset)
			if err := nodeIndexer.Add(item.node); err != nil {
				t.Fatalf("Failed to add node %q: %v", item.node.GetName(), err)
			}
			controller.recorder = testutil.NewFakeRecorder()

			wg.Go(func() {
				controller.Run(ctx)
			})

			controller.NodeUpdated(nil, item.node)

			verifyPodActions(t, item.description, fakeClientset, item.expectPatch, item.expectDelete)

			cancel()
		})
	}
}

// TestDeleteNode 验证节点删除后，控制器会从 taintedNodes 缓存中移除该节点，
// 通过轮询等待缓存清空来确认事件已被处理。
func TestDeleteNode(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeClientset := fake.NewSimpleClientset()
	controller, _, _ := setupNewController(ctx, fakeClientset)
	controller.recorder = testutil.NewFakeRecorder()
	controller.taintedNodes = map[string][]corev1.Taint{
		"node1": {createNoExecuteTaint(1)},
	}

	wg.Go(func() {
		controller.Run(ctx)
	})

	controller.NodeUpdated(testutil.NewNode("node1"), nil)

	// 轮询等待控制器处理完节点删除，清空 taintedNodes 缓存。
	err := wait.PollImmediate(10*time.Millisecond, time.Second, func() (bool, error) {
		controller.taintedNodesLock.Lock()
		defer controller.taintedNodesLock.Unlock()
		_, ok := controller.taintedNodes["node1"]
		return !ok, nil
	})
	if err != nil {
		t.Errorf("Failed to await for processing node deleted: %q", err)
	}
}

// TestUpdateNode 验证节点更新事件的处理：添加污点会驱逐不容忍的 Pod，
// 容忍污点的 Pod 不会被驱逐，而污点被移除后也不应触发驱逐。
// 表中还覆盖多个容忍中第一个到期即驱逐的场景。
func TestUpdateNode(t *testing.T) {
	testCases := []struct {
		description     string
		pods            []corev1.Pod
		oldNode         *corev1.Node
		newNode         *corev1.Node
		expectPatch     bool
		expectDelete    bool
		additionalSleep time.Duration
	}{
		{
			description: "Added taint, expect node patched and deleted",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "Added tolerated taint",
			pods: []corev1.Pod{
				*addToleration(testutil.NewPod("pod1", "node1"), 1, 100),
			},
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectDelete: false,
		},
		{
			description: "Only one added taint tolerated",
			pods: []corev1.Pod{
				*addToleration(testutil.NewPod("pod1", "node1"), 1, 100),
			},
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1, 2}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "Taint removed",
			pods: []corev1.Pod{
				*addToleration(testutil.NewPod("pod1", "node1"), 1, 1),
			},
			oldNode:         addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			newNode:         testutil.NewNode("node1"),
			expectDelete:    false,
			additionalSleep: 1500 * time.Millisecond,
		},
		{
			description: "Pod with multiple tolerations are evicted when first one runs out",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pod1",
					},
					Spec: corev1.PodSpec{
						NodeName: "node1",
						Tolerations: []corev1.Toleration{
							{Key: "testTaint1", Value: "test1", Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &[]int64{1}[0]},
							{Key: "testTaint2", Value: "test2", Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &[]int64{100}[0]},
						},
					},
					Status: corev1.PodStatus{
						Conditions: []corev1.PodCondition{
							{
								Type:   corev1.PodReady,
								Status: corev1.ConditionTrue,
							},
						},
					},
				},
			},
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1, 2}),
			expectPatch:  true,
			expectDelete: true,
		},
	}

	for _, item := range testCases {
		t.Run(item.description, func(t *testing.T) {
			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewSimpleClientset(&corev1.PodList{Items: item.pods})
			controller, _, nodeIndexer := setupNewController(ctx, fakeClientset)
			nodeIndexer.Add(item.newNode)
			controller.recorder = testutil.NewFakeRecorder()

			wg.Go(func() {
				controller.Run(ctx)
			})

			controller.NodeUpdated(item.oldNode, item.newNode)

			if item.additionalSleep > 0 {
				time.Sleep(item.additionalSleep)
			}

			verifyPodActions(t, item.description, fakeClientset, item.expectPatch, item.expectDelete)
		})
	}
}

// TestUpdateNodeWithMultipleTaints 验证节点污点变化时 Pod 排队状态如何更新：
// 无限期容忍的污点不应触发驱逐；一旦出现无法无限期容忍的污点，Pod 应被排入驱逐队列；
// 当该污点被移除后应取消排队，且整个过程中不应产生实际的删除动作。
func TestUpdateNodeWithMultipleTaints(t *testing.T) {
	taint1 := createNoExecuteTaint(1)
	taint2 := createNoExecuteTaint(2)

	minute := int64(60)
	pod := testutil.NewPod("pod1", "node1")
	pod.Spec.Tolerations = []corev1.Toleration{
		{Key: taint1.Key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		{Key: taint2.Key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &minute},
	}
	podNamespacedName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}

	untaintedNode := testutil.NewNode("node1")

	doubleTaintedNode := testutil.NewNode("node1")
	doubleTaintedNode.Spec.Taints = []corev1.Taint{taint1, taint2}

	singleTaintedNode := testutil.NewNode("node1")
	singleTaintedNode.Spec.Taints = []corev1.Taint{taint1}

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeClientset := fake.NewSimpleClientset(pod)
	controller, _, nodeIndexer := setupNewController(ctx, fakeClientset)
	controller.recorder = testutil.NewFakeRecorder()

	wg.Go(func() {
		controller.Run(ctx)
	})

	// 节点无污点：Pod 不应被排入驱逐队列。
	nodeIndexer.Add(untaintedNode)
	controller.handleNodeUpdate(ctx, nodeUpdateItem{"node1"})
	// 验证 Pod 没有被排入驱逐队列。
	if controller.taintEvictionQueue.GetWorkerUnsafe(podNamespacedName.String()) != nil {
		t.Fatalf("pod queued for deletion with no taints")
	}

	// 无污点 -> 添加无限期容忍的污点：Pod 不应被驱逐。
	nodeIndexer.Update(singleTaintedNode)
	controller.handleNodeUpdate(ctx, nodeUpdateItem{"node1"})
	// 验证 Pod 没有被排入驱逐队列。
	if controller.taintEvictionQueue.GetWorkerUnsafe(podNamespacedName.String()) != nil {
		t.Fatalf("pod queued for deletion with permanently tolerated taint")
	}

	// 无限期容忍 -> 再添加一个仅临时容忍的污点：Pod 应被排入驱逐队列。
	nodeIndexer.Update(doubleTaintedNode)
	controller.handleNodeUpdate(ctx, nodeUpdateItem{"node1"})
	// 验证 Pod 已被排入驱逐队列。
	if controller.taintEvictionQueue.GetWorkerUnsafe(podNamespacedName.String()) == nil {
		t.Fatalf("pod not queued for deletion after addition of temporarily tolerated taint")
	}

	// 临时容忍 -> 回到仅无限期容忍的污点：应取消驱逐排队。
	nodeIndexer.Update(singleTaintedNode)
	controller.handleNodeUpdate(ctx, nodeUpdateItem{"node1"})
	// 验证 Pod 没有被排入驱逐队列。
	if controller.taintEvictionQueue.GetWorkerUnsafe(podNamespacedName.String()) != nil {
		t.Fatalf("pod queued for deletion after removal of temporarily tolerated taint")
	}

	// 最终验证整个过程中没有产生对 Pod 的实际删除操作。
	for _, action := range fakeClientset.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			t.Error("Unexpected deletion")
		}
	}
}

// TestUpdateNodeWithMultiplePods 验证节点被加上污点后，多个 Pod 按各自容忍时长在正确时间被驱逐：
// 无容忍或容忍不匹配的 Pod 立即删除，有限期容忍的 Pod 在容忍结束后删除，无限期容忍的 Pod 不删除。
func TestUpdateNodeWithMultiplePods(t *testing.T) {
	testCases := []struct {
		description         string
		pods                []corev1.Pod
		oldNode             *corev1.Node
		newNode             *corev1.Node
		expectedDeleteTimes durationSlice
	}{
		{
			description: "Pods with different toleration times are evicted appropriately",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
				*addToleration(testutil.NewPod("pod2", "node1"), 1, 1),
				*addToleration(testutil.NewPod("pod3", "node1"), 1, -1),
			},
			oldNode: testutil.NewNode("node1"),
			newNode: addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectedDeleteTimes: durationSlice{
				{[]string{"pod1"}, 0},
				{[]string{"pod2"}, time.Second},
			},
		},
		{
			description: "Evict all pods not matching all taints instantly",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
				*addToleration(testutil.NewPod("pod2", "node1"), 1, 1),
				*addToleration(testutil.NewPod("pod3", "node1"), 1, -1),
			},
			oldNode: testutil.NewNode("node1"),
			newNode: addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1, 2}),
			expectedDeleteTimes: durationSlice{
				{[]string{"pod1", "pod2", "pod3"}, 0},
			},
		},
	}

	for _, item := range testCases {
		t.Run(item.description, func(t *testing.T) {
			t.Logf("Starting testcase %q", item.description)

			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewSimpleClientset(&corev1.PodList{Items: item.pods})
			// 期望的删除时间按时间戳排序，便于逐批检查。
			sort.Sort(item.expectedDeleteTimes)
			controller, _, nodeIndexer := setupNewController(ctx, fakeClientset)
			nodeIndexer.Add(item.newNode)
			controller.recorder = testutil.NewFakeRecorder()

			wg.Go(func() {
				controller.Run(ctx)
			})

			controller.NodeUpdated(item.oldNode, item.newNode)

			startedAt := time.Now()
			for i := range item.expectedDeleteTimes {
				if i == 0 || item.expectedDeleteTimes[i-1].timestamp != item.expectedDeleteTimes[i].timestamp {
					// 计算一段宽限期，给控制器留出处理更新的时间。
					// 上面用例中的时间间隔取得足够大，以避免测试出现偶发失败。
					var increment time.Duration
					if i == len(item.expectedDeleteTimes)-1 || item.expectedDeleteTimes[i+1].timestamp == item.expectedDeleteTimes[i].timestamp {
						increment = 500 * time.Millisecond
					} else {
						increment = ((item.expectedDeleteTimes[i+1].timestamp - item.expectedDeleteTimes[i].timestamp) / time.Duration(2))
					}

					sleepTime := item.expectedDeleteTimes[i].timestamp - time.Since(startedAt) + increment
					if sleepTime < 0 {
						sleepTime = 0
					}
					t.Logf("Sleeping for %v", sleepTime)
					time.Sleep(sleepTime)
				}

				for delay, podName := range item.expectedDeleteTimes[i].names {
					deleted := false
					for _, action := range fakeClientset.Actions() {
						deleteAction, ok := action.(clienttesting.DeleteActionImpl)
						if !ok {
							t.Logf("Found not-delete action with verb %v. Ignoring.", action.GetVerb())
							continue
						}
						if deleteAction.GetResource().Resource != "pods" {
							continue
						}
						if podName == deleteAction.GetName() {
							deleted = true
						}
					}
					if !deleted {
						t.Errorf("Failed to deleted pod %v after %v", podName, delay)
					}
				}
				for _, action := range fakeClientset.Actions() {
					deleteAction, ok := action.(clienttesting.DeleteActionImpl)
					if !ok {
						t.Logf("Found not-delete action with verb %v. Ignoring.", action.GetVerb())
						continue
					}
					if deleteAction.GetResource().Resource != "pods" {
						continue
					}
					deletedPodName := deleteAction.GetName()
					expected := false
					for _, podName := range item.expectedDeleteTimes[i].names {
						if podName == deletedPodName {
							expected = true
						}
					}
					if !expected {
						t.Errorf("Pod %v was deleted even though it shouldn't have", deletedPodName)
					}
				}
				// 每批检查后清空动作记录，避免影响下一批的判断。
				fakeClientset.ClearActions()
			}
		})
	}
}

// TestGetMinTolerationTime 验证 getMinTolerationTime 对容忍时长的计算：
// 空列表返回 0，存在无限期容忍返回 -1，多个有限期容忍返回其中最短值。
func TestGetMinTolerationTime(t *testing.T) {
	one := int64(1)
	two := int64(2)
	oneSec := 1 * time.Second

	tests := []struct {
		tolerations []corev1.Toleration
		expected    time.Duration
	}{
		{
			tolerations: []corev1.Toleration{},
			expected:    0,
		},
		{
			tolerations: []corev1.Toleration{
				{
					TolerationSeconds: nil,
				},
			},
			expected: -1,
		},
		{
			tolerations: []corev1.Toleration{
				{
					TolerationSeconds: &one,
				},
				{
					TolerationSeconds: &two,
				},
			},
			expected: oneSec,
		},

		{
			tolerations: []corev1.Toleration{
				{
					TolerationSeconds: &one,
				},
				{
					TolerationSeconds: nil,
				},
			},
			expected: oneSec,
		},
		{
			tolerations: []corev1.Toleration{
				{
					TolerationSeconds: nil,
				},
				{
					TolerationSeconds: &one,
				},
			},
			expected: oneSec,
		},
	}

	// 注意语义：空容忍列表返回 0；包含无限期容忍（TolerationSeconds 为 nil）时返回 -1；
	// 多个有限期容忍取最短的那个。
	for _, test := range tests {
		got := getMinTolerationTime(test.tolerations)
		if got != test.expected {
			t.Errorf("Incorrect min toleration time: got %v, expected %v", got, test.expected)
		}
	}
}

// TestEventualConsistency 验证：即使 getPodsAssignedToNode 返回的数据不完整
// （例如由于 watch 延迟），TaintManager 最终也会协调并驱逐剩余的 Pod。
// TestUpdatePods 已部分覆盖该场景，但鉴于这是 TaintManager 的重要性质，
// 有必要提供一个显式的测试。
// TestEventualConsistency 表驱动验证最终一致性：节点更新先处理已知 Pod，
// 随后到达的 Pod 更新也需被处理，确保不会因数据延迟而漏掉驱逐。
func TestEventualConsistency(t *testing.T) {
	testCases := []struct {
		description  string
		pods         []corev1.Pod
		prevPod      *corev1.Pod
		newPod       *corev1.Pod
		oldNode      *corev1.Node
		newNode      *corev1.Node
		expectPatch  bool
		expectDelete bool
	}{
		{
			description: "existing pod2 scheduled onto tainted Node",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			prevPod:      testutil.NewPod("pod2", ""),
			newPod:       testutil.NewPod("pod2", "node1"),
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "existing pod2 with taint toleration scheduled onto tainted Node",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			prevPod:      addToleration(testutil.NewPod("pod2", ""), 1, 100),
			newPod:       addToleration(testutil.NewPod("pod2", "node1"), 1, 100),
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "new pod2 created on tainted Node",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			prevPod:      nil,
			newPod:       testutil.NewPod("pod2", "node1"),
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
		{
			description: "new pod2 with tait toleration created on tainted Node",
			pods: []corev1.Pod{
				*testutil.NewPod("pod1", "node1"),
			},
			prevPod:      nil,
			newPod:       addToleration(testutil.NewPod("pod2", "node1"), 1, 100),
			oldNode:      testutil.NewNode("node1"),
			newNode:      addTaintsToNode(testutil.NewNode("node1"), "testTaint1", "taint1", []int{1}),
			expectPatch:  true,
			expectDelete: true,
		},
	}

	for _, item := range testCases {
		t.Run(item.description, func(t *testing.T) {
			var wg sync.WaitGroup
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeClientset := fake.NewSimpleClientset(&corev1.PodList{Items: item.pods})
			controller, podIndexer, nodeIndexer := setupNewController(ctx, fakeClientset)
			nodeIndexer.Add(item.newNode)
			controller.recorder = testutil.NewFakeRecorder()

			wg.Go(func() {
				controller.Run(ctx)
			})

			if item.prevPod != nil {
				podIndexer.Add(item.prevPod)
				controller.PodUpdated(nil, item.prevPod)
			}

			// 首先模拟一次 NodeUpdate：此时控制器还不知道 pod2，
			// 因此只应当删除已纳入视野的 pod1。
			controller.NodeUpdated(item.oldNode, item.newNode)

			verifyPodActions(t, item.description, fakeClientset, item.expectPatch, item.expectDelete)
			fakeClientset.ClearActions()

			// 随后 pod2 的延迟更新才到达 TaintManager，此时它也应被驱逐。
			podIndexer.Update(item.newPod)
			controller.PodUpdated(item.prevPod, item.newPod)
			// 稍作等待，让控制器处理延迟到达的 Pod 更新。
			time.Sleep(timeForControllerToProgressForSanityCheck)
		})
	}
}

// verifyPodActions 轮询 fake client 的动作记录，直到同时满足期望的 patch/delete 组合。
// 使用 wait.Poll 留出控制器处理时间；超时后再逐项断言失败原因，便于定位。
func verifyPodActions(t *testing.T, description string, fakeClientset *fake.Clientset, expectPatch, expectDelete bool) {
	t.Helper()
	podPatched := false
	podDeleted := false
	// 使用 Poll 而不是 PollImmediate，是为了给控制器留出处理时间，
	// 使期望的动作很可能已经被发出后再检查，避免由于竞态造成误判。
	err := wait.Poll(10*time.Millisecond, 5*time.Second, func() (bool, error) {
		for _, action := range fakeClientset.Actions() {
			if action.GetVerb() == "patch" && action.GetResource().Resource == "pods" {
				podPatched = true
			}
			if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
				podDeleted = true
			}
		}
		return podPatched == expectPatch && podDeleted == expectDelete, nil
	})
	if err != nil {
		t.Errorf("Failed waiting for the expected actions: %q", err)
	}
	if podPatched != expectPatch {
		t.Errorf("[%v]Unexpected test result. Expected patch %v, got %v", description, expectPatch, podPatched)
	}
	if podDeleted != expectDelete {
		t.Errorf("[%v]Unexpected test result. Expected delete %v, got %v", description, expectDelete, podDeleted)
	}
}

// TestPodDeletionEvent 校验控制器输出的 Pod 删除相关事件。
// TestPodDeletionEvent 验证 Pod 驱逐相关事件的输出符合预期。
// 通过 cmp.FilterPath 忽略时间戳与事件名称等运行时字段，仅比较事件的核心内容。
func TestPodDeletionEvent(t *testing.T) {
	f := func(path cmp.Path) bool {
		switch path.String() {
		// 这些字段在运行时会变化，比较时需忽略。
		case "LastTimestamp", "FirstTimestamp", "ObjectMeta.Name":
			return true
		}
		return false
	}

	// 验证 emitPodDeletionEvent 生成的标记驱逐事件内容正确。
	t.Run("emitPodDeletionEvent", func(t *testing.T) {
		controller := &Controller{}
		recorder := testutil.NewFakeRecorder()
		controller.recorder = recorder
		controller.emitPodDeletionEvent(types.NamespacedName{
			Name:      "test",
			Namespace: "test",
		})
		want := []*corev1.Event{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test",
				},
				InvolvedObject: corev1.ObjectReference{
					Kind:       "Pod",
					APIVersion: "v1",
					Namespace:  "test",
					Name:       "test",
				},
				Reason:  "TaintManagerEviction",
				Type:    "Normal",
				Count:   1,
				Message: "Marking for deletion Pod test/test",
				Source:  corev1.EventSource{Component: "nodeControllerTest"},
			},
		}
		if diff := cmp.Diff(want, recorder.Events, cmp.FilterPath(f, cmp.Ignore())); len(diff) > 0 {
			t.Errorf("emitPodDeletionEvent() returned data (-want,+got):\n%s", diff)
		}
	})

	// 验证 emitCancelPodDeletionEvent 生成的取消驱逐事件内容正确。
	t.Run("emitCancelPodDeletionEvent", func(t *testing.T) {
		controller := &Controller{}
		recorder := testutil.NewFakeRecorder()
		controller.recorder = recorder
		controller.emitCancelPodDeletionEvent(types.NamespacedName{
			Name:      "test",
			Namespace: "test",
		})
		want := []*corev1.Event{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test",
				},
				InvolvedObject: corev1.ObjectReference{
					Kind:       "Pod",
					APIVersion: "v1",
					Namespace:  "test",
					Name:       "test",
				},
				Reason:  "TaintManagerEviction",
				Type:    "Normal",
				Count:   1,
				Message: "Cancelling deletion of Pod test/test",
				Source:  corev1.EventSource{Component: "nodeControllerTest"},
			},
		}
		if diff := cmp.Diff(want, recorder.Events, cmp.FilterPath(f, cmp.Ignore())); len(diff) > 0 {
			t.Errorf("emitPodDeletionEvent() returned data (-want,+got):\n%s", diff)
		}
	})
}
