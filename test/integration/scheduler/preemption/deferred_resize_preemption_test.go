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

package preemption

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	configv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/utils/ptr"
)

func TestDeferredResizePodPreemption(t *testing.T) {
	// 启用相关 feature gate，否则原地垂直扩缩容抢占不会生效
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.InPlacePodVerticalScaling:                    true,
		features.InPlacePodVerticalScalingSchedulerPreemption: true,
	})

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
		}},
	})

	tests := []struct {
		name                 string
		nodeCapacityCPU      string
		nodeCapacityMem      string
		nodePreemptionPolicy *v1.NodePodPreemptionPolicy
		existingPods         []*v1.Pod
		preemptorConfig      *testutils.PausePodConfig
		expectEvictedNames   []string
	}{
		{
			name:            "preempt single low priority pod",
			nodeCapacityCPU: "300m",
			nodeCapacityMem: "300",
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-1",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
					},
				}),
			},
			preemptorConfig: &testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
				},
			},
			expectEvictedNames: []string{"victim-1"},
		},
		{
			name:            "preempt multiple low priority pods",
			nodeCapacityCPU: "300m",
			nodeCapacityMem: "300",
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-1",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(50, resource.DecimalSI)},
					},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-2",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(50, resource.DecimalSI)},
					},
				}),
			},
			preemptorConfig: &testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
				},
			},
			expectEvictedNames: []string{"victim-1", "victim-2"},
		},
		{
			name:            "no preemption when preemptor policy is PreemptNever",
			nodeCapacityCPU: "300m",
			nodeCapacityMem: "300",
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-1",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
					},
				}),
			},
			preemptorConfig: &testutils.PausePodConfig{
				Name:             "preemptor-pod",
				Priority:         &highPriority,
				PreemptionPolicy: ptr.To(v1.PreemptNever),
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
				},
			},
			expectEvictedNames: nil,
		},
		{
			name:            "parking strategy when deferred resize fits on node",
			nodeCapacityCPU: "300m",
			nodeCapacityMem: "300",
			existingPods:    nil, // 无需抢占即可放下
			preemptorConfig: &testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
				},
			},
			expectEvictedNames: nil,
		},
		{
			name:            "node-level preemption policy disables preemption",
			nodeCapacityCPU: "300m",
			nodeCapacityMem: "300",
			nodePreemptionPolicy: &v1.NodePodPreemptionPolicy{
				DisableResizePreemption: []string{"test-operator"},
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:     "victim-1",
					Priority: &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(200, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
					},
				}),
			},
			preemptorConfig: &testutils.PausePodConfig{
				Name:     "preemptor-pod",
				Priority: &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
				},
			},
			expectEvictedNames: nil,
		},
	}

	// 所有表格用例共用一套 API server 和 scheduler，避免重复启动开销
	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "def-preempt", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...),
	)
	defer testCtx.SchedulerCloseFn()
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs := testCtx.ClientSet

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeName := fmt.Sprintf("preempt-node-%d", idx)

			// 创建带指定容量的节点
			nodeRes := map[v1.ResourceName]string{
				v1.ResourcePods:   "32",
				v1.ResourceCPU:    tt.nodeCapacityCPU,
				v1.ResourceMemory: tt.nodeCapacityMem,
			}
			nodeObject := st.MakeNode().Name(nodeName).Capacity(nodeRes).Label("node", nodeName).Obj()
			if tt.nodePreemptionPolicy != nil {
				nodeObject.Spec.PodPreemptionPolicy = tt.nodePreemptionPolicy
			}
			if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject, metav1.CreateOptions{}); err != nil {
				t.Fatalf("Failed to create node: %v", err)
			}
			defer func() {
				_ = cs.CoreV1().Nodes().Delete(testCtx.Ctx, nodeName, metav1.DeleteOptions{})
			}()

			// 在 nodeName 上运行已有 Pod（作为潜在被抢占者）
			var pods []*v1.Pod
			for _, p := range tt.existingPods {
				p.Namespace = testCtx.NS.Name
				p.Spec.NodeName = nodeName
				runningPod, err := runPausePod(cs, p)
				if err != nil {
					t.Fatalf("Failed running pause pod %v: %v", p.Name, err)
				}
				pods = append(pods, runningPod)
			}

			// 创建已调度到 nodeName 的抢占者（扩缩容中的 Pod）
			tt.preemptorConfig.NodeName = nodeName
			tt.preemptorConfig.Namespace = testCtx.NS.Name
			preemptorPod := initPausePod(tt.preemptorConfig)
			preemptorPod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, preemptorPod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create preemptor pod: %v", err)
			}

			// 伪造 Pod 正在运行且扩缩容被延迟的状态：
			// 已分配 100m/100，而 Spec 请求为 300m/100
			preemptorPod.Status.Phase = v1.PodRunning
			preemptorPod.Status.Conditions = []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionTrue,
				},
				{
					Type:   v1.PodResizePending,
					Status: v1.ConditionTrue,
					Reason: v1.PodReasonDeferred,
				},
			}
			preemptorPod.Status.ContainerStatuses = []v1.ContainerStatus{
				{
					Name: preemptorPod.Name,
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
					},
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI),
						},
					},
				},
			}
			preemptorPod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, preemptorPod, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to update status of preemptor pod: %v", err)
			}

			// 等待预期的驱逐发生（如有），或超时
			if len(tt.expectEvictedNames) > 0 {
				for _, name := range tt.expectEvictedNames {
					err = wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, 10*time.Second, false,
						podIsGettingEvicted(cs, testCtx.NS.Name, name))
					if err != nil {
						t.Errorf("Expected pod %q to be evicted/deleted, but it was not", name)
					}
				}
			} else {
				// 观察一段时间，确认没有发生驱逐
				time.Sleep(500 * time.Millisecond)
				for _, p := range pods {
					livePod, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, p.Name, metav1.GetOptions{})
					if err == nil && livePod.DeletionTimestamp != nil {
						t.Errorf("Expected pod %q NOT to be evicted/deleted, but its DeletionTimestamp is set", p.Name)
					}
				}
			}

			// 延迟扩缩容的 Pod 不应设置 NominatedNodeName
			updatedPreemptor, err := cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, preemptorPod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Failed to get updated preemptor pod: %v", err)
			}
			if updatedPreemptor.Status.NominatedNodeName != "" {
				t.Errorf("Expected NominatedNodeName to remain empty, but got %q", updatedPreemptor.Status.NominatedNodeName)
			}

			// 验证 Pod 仍停留在调度队列中等待重试
			queue := testCtx.Scheduler.SchedulingQueue
			_, found := queue.GetPod(preemptorPod.Name, preemptorPod.Namespace, nil)
			if !found {
				t.Errorf("Expected preemptor pod to be found in scheduling queue")
			}
			// 验证 PodScheduled 仍为 True，保留运行中状态
			var gotScheduledStatus v1.ConditionStatus
			for _, cond := range updatedPreemptor.Status.Conditions {
				if cond.Type == v1.PodScheduled {
					gotScheduledStatus = cond.Status
				}
			}
			if gotScheduledStatus != v1.ConditionTrue {
				t.Errorf("Expected PodScheduled condition status to remain True, got %v", gotScheduledStatus)
			}

			// 清理本用例创建的 Pod
			pods = append(pods, preemptorPod)
			testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
		})
	}
}

func setUpPreemptionTestWithContext(t *testing.T, testCtx *testutils.TestContext, idx int, nodeCPUCapacity, otherPodCPURequest, deferredPodCPURequest, deferredPodCPUAllocated string, disableResizePreemption bool) (*v1.Pod, *v1.Pod, *v1.Pod) {
	cs := testCtx.ClientSet

	nodeName1 := fmt.Sprintf("hint-node1-%d", idx)
	nodeName2 := fmt.Sprintf("hint-node2-%d", idx)

	// 创建测试目标节点 node1
	nodeObject1 := st.MakeNode().Name(nodeName1).Capacity(map[v1.ResourceName]string{
		v1.ResourcePods: "32",
		v1.ResourceCPU:  nodeCPUCapacity,
	}).Obj()
	if disableResizePreemption {
		nodeObject1.Spec.PodPreemptionPolicy = &v1.NodePodPreemptionPolicy{
			DisableResizePreemption: []string{"test-policy"},
		}
	}
	if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject1, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}

	// 创建无关节点 node2，用于验证队列提示只关注相关节点
	nodeObject2 := st.MakeNode().Name(nodeName2).Capacity(map[v1.ResourceName]string{
		v1.ResourcePods: "32",
		v1.ResourceCPU:  "500m",
	}).Obj()
	if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}

	// 在 node1 上运行 other-pod，占据部分 CPU
	other := initPausePod(&testutils.PausePodConfig{
		Name:     fmt.Sprintf("other-pod-%d", idx),
		NodeName: nodeName1,
		Priority: &highPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse(otherPodCPURequest)},
		},
	})
	other.Namespace = testCtx.NS.Name
	other, err := runPausePod(cs, other)
	if err != nil {
		t.Fatalf("Failed to run other pod: %v", err)
	}

	// 在无关节点 node2 上运行 irrelevant-pod
	irrelevant := initPausePod(&testutils.PausePodConfig{
		Name:     fmt.Sprintf("irrelevant-pod-%d", idx),
		NodeName: nodeName2,
		Priority: &highPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
		},
	})
	irrelevant.Namespace = testCtx.NS.Name
	irrelevant, err = runPausePod(cs, irrelevant)
	if err != nil {
		t.Fatalf("Failed to run irrelevant pod: %v", err)
	}

	// 创建扩缩容被延迟的 Pod
	pod := initPausePod(&testutils.PausePodConfig{
		Name:     fmt.Sprintf("deferred-pod-%d", idx),
		NodeName: nodeName1,
		Priority: &lowPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse(deferredPodCPURequest)},
		},
	})
	pod.Namespace = testCtx.NS.Name
	pod, err = cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	pod.Status.Phase = v1.PodRunning
	pod.Status.Conditions = []v1.PodCondition{
		{
			Type:   v1.PodScheduled,
			Status: v1.ConditionTrue,
		},
		{
			Type:   v1.PodResizePending,
			Status: v1.ConditionTrue,
			Reason: v1.PodReasonDeferred,
		},
	}
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{
			Name: pod.Name,
			AllocatedResources: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse(deferredPodCPUAllocated),
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse(deferredPodCPUAllocated),
				},
			},
		},
	}
	pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 等待延迟扩缩容的 Pod 调度失败并进入 Unschedulable 队列
	queue := testCtx.Scheduler.SchedulingQueue
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, 5*time.Second, false, func(context.Context) (bool, error) {
		unsched := queue.UnschedulablePods()
		for _, p := range unsched {
			if p.Name == pod.Name {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Expected deferred pod to be parked in Unschedulable queue, but it was not found")
	}

	return pod, other, irrelevant
}

func TestDeferredResizeQueueingHints(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.InPlacePodVerticalScaling:                    true,
		features.InPlacePodVerticalScalingSchedulerPreemption: true,
	})

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
		}},
	})

	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "def-q-hint", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...),
	)
	defer testCtx.SchedulerCloseFn()
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	tests := []struct {
		name               string
		nodeCPUCapacity    string
		otherPodCPURequest string
		deferredPodRequest string
		expectIncrement    bool
		trigger            func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error
	}{
		{
			name:            "irrelevant pod scale down ignores queue",
			expectIncrement: false,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				p, err := cs.CoreV1().Pods(ns).Get(ctx, irrelevant.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				p.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}
				_, err = cs.CoreV1().Pods(ns).UpdateResize(ctx, p.Name, p, metav1.UpdateOptions{})
				return err
			},
		},
		{
			name:            "assigned pod scale down on same node wakes queue",
			expectIncrement: true,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				p, err := cs.CoreV1().Pods(ns).Get(ctx, other.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				p.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}
				_, err = cs.CoreV1().Pods(ns).UpdateResize(ctx, p.Name, p, metav1.UpdateOptions{})
				return err
			},
		},
		{
			name:            "irrelevant pod deletion ignores queue",
			expectIncrement: false,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				return cs.CoreV1().Pods(ns).Delete(ctx, irrelevant.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
			},
		},
		{
			name:            "assigned pod deletion on same node wakes queue",
			expectIncrement: true,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				return cs.CoreV1().Pods(ns).Delete(ctx, other.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
			},
		},
		{
			name:            "non-resource label change ignores queue",
			expectIncrement: false,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				p, err := cs.CoreV1().Pods(ns).Get(ctx, other.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if p.Labels == nil {
					p.Labels = make(map[string]string)
				}
				p.Labels["updated-by-test"] = "true"
				_, err = cs.CoreV1().Pods(ns).Update(ctx, p, metav1.UpdateOptions{})
				return err
			},
		},
		{
			name:            "assigned node capacity increase wakes queue",
			expectIncrement: true,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				nodeName := pod.Spec.NodeName
				n, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				n.Status.Capacity[v1.ResourceCPU] = *resource.NewMilliQuantity(1000, resource.DecimalSI)
				n.Status.Allocatable[v1.ResourceCPU] = *resource.NewMilliQuantity(1000, resource.DecimalSI)
				_, err = cs.CoreV1().Nodes().UpdateStatus(ctx, n, metav1.UpdateOptions{})
				return err
			},
		},
		{
			name:            "target pod spec scale down wakes queue",
			expectIncrement: true,
			trigger: func(ctx context.Context, cs clientset.Interface, ns string, pod, other, irrelevant *v1.Pod) error {
				p, err := cs.CoreV1().Pods(ns).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				p.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(80, resource.DecimalSI)}
				_, err = cs.CoreV1().Pods(ns).UpdateResize(ctx, p.Name, p, metav1.UpdateOptions{})
				return err
			},
		},
	}

	for idx, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeCap := "200m"
			if tc.nodeCPUCapacity != "" {
				nodeCap = tc.nodeCPUCapacity
			}
			otherReq := "200m"
			if tc.otherPodCPURequest != "" {
				otherReq = tc.otherPodCPURequest
			}
			defReq := "100m"
			if tc.deferredPodRequest != "" {
				defReq = tc.deferredPodRequest
			}
			pod, other, irrelevant := setUpPreemptionTestWithContext(t, testCtx, idx, nodeCap, otherReq, defReq, "50m", false)

			cs := testCtx.ClientSet
			queue := testCtx.Scheduler.SchedulingQueue

			queuedPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
			if !found {
				t.Fatalf("Pod not found in queue")
			}
			initialAttempts := queuedPod.Attempts

			if err := tc.trigger(testCtx.Ctx, cs, testCtx.NS.Name, pod, other, irrelevant); err != nil {
				t.Fatalf("Failed triggering event: %v", err)
			}

			if tc.expectIncrement {
				err := wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, 5*time.Second, false, func(context.Context) (bool, error) {
					qPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
					return found && qPod.Attempts > initialAttempts, nil
				})
				if err != nil {
					t.Fatalf("Expected queue attempts to increment after trigger")
				}
			} else {
				time.Sleep(300 * time.Millisecond)
				qPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
				if found && qPod.Attempts > initialAttempts {
					t.Fatalf("Expected attempts not to increment, but went from %d to %d", initialAttempts, qPod.Attempts)
				}
			}

			testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod, other, irrelevant})
		})
	}
}

func TestDeferredResizeNodePreemptionPolicy(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.InPlacePodVerticalScaling:                    true,
		features.InPlacePodVerticalScalingSchedulerPreemption: true,
	})

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
		}},
	})

	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "def-policy", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...),
	)
	defer testCtx.SchedulerCloseFn()
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	t.Run("disabling node preemption policy does not wake queue", func(t *testing.T) {
		pod, other, irrelevant := setUpPreemptionTestWithContext(t, testCtx, 0, "200m", "200m", "100m", "50m", false)

		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue

		queuedPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
		if !found {
			t.Fatalf("Expected pod to be in queue initially")
		}
		initialAttempts := queuedPod.Attempts

		// 在 Pod 所在节点上禁用抢占
		nodeName := pod.Spec.NodeName
		n, err := cs.CoreV1().Nodes().Get(testCtx.Ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get node: %v", err)
		}
		n.Spec.PodPreemptionPolicy = &v1.NodePodPreemptionPolicy{
			DisableResizePreemption: []string{"test-policy"},
		}
		if _, err := cs.CoreV1().Nodes().Update(testCtx.Ctx, n, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("Failed to update node to disable preemption: %v", err)
		}

		// 禁用抢占后不应产生队列提示，重试次数不变
		time.Sleep(300 * time.Millisecond)
		qPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
		if !found || qPod.Attempts > initialAttempts {
			t.Fatalf("Expected attempts not to increment, but went from %d to %d (or pod was lost)", initialAttempts, qPod.Attempts)
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod, other, irrelevant})
	})

	t.Run("enabling node preemption policy wakes queue", func(t *testing.T) {
		pod, other, irrelevant := setUpPreemptionTestWithContext(t, testCtx, 1, "200m", "200m", "100m", "50m", true)

		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue

		queuedPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
		if !found {
			t.Fatalf("Expected pod to still be in queue")
		}
		initialAttempts := queuedPod.Attempts

		// 清除 DisableResizePreemption，重新启用节点抢占
		nodeName := pod.Spec.NodeName
		n, err := cs.CoreV1().Nodes().Get(testCtx.Ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get node: %v", err)
		}
		n.Spec.PodPreemptionPolicy = nil
		if _, err := cs.CoreV1().Nodes().Update(testCtx.Ctx, n, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("Failed to update node to enable preemption: %v", err)
		}

		// 重新启用后应触发重试，attempts 增加
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 50*time.Millisecond, 5*time.Second, false, func(context.Context) (bool, error) {
			qPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
			return found && qPod.Attempts > initialAttempts, nil
		})
		if err != nil {
			t.Fatalf("Expected attempts to increment after re-enabling node preemption policy")
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod, other, irrelevant})
	})

	t.Run("irrelevant node preemption policy change ignores queue", func(t *testing.T) {
		pod, other, irrelevant := setUpPreemptionTestWithContext(t, testCtx, 2, "200m", "200m", "100m", "50m", false)

		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue

		queuedPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
		if !found {
			t.Fatalf("Expected pod to be in queue initially")
		}
		initialAttempts := queuedPod.Attempts

		// 修改无关节点 node2 的策略，不应影响 Pod 的调度队列
		nodeName2 := fmt.Sprintf("hint-node2-%d", 2)
		n, err := cs.CoreV1().Nodes().Get(testCtx.Ctx, nodeName2, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get node2: %v", err)
		}
		n.Spec.PodPreemptionPolicy = &v1.NodePodPreemptionPolicy{
			DisableResizePreemption: []string{"test-policy"},
		}
		if _, err := cs.CoreV1().Nodes().Update(testCtx.Ctx, n, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("Failed to update node2: %v", err)
		}

		time.Sleep(300 * time.Millisecond)
		qPod, found := queue.GetPod(pod.Name, pod.Namespace, nil)
		if !found || qPod.Attempts > initialAttempts {
			t.Fatalf("Expected attempts not to increment and pod to remain in queue for irrelevant node update")
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod, other, irrelevant})
	})
}

func TestDeferredResizeQueueingHandlers(t *testing.T) {
	t.Run("AddPod handler enqueues existing deferred pod on scheduler startup", func(t *testing.T) {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.InPlacePodVerticalScaling:                    true,
			features.InPlacePodVerticalScalingSchedulerPreemption: true,
		})

		cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
			Profiles: []configv1.KubeSchedulerProfile{{
				SchedulerName: ptr.To(v1.DefaultSchedulerName),
			}},
		})

		// 先只启动 API server，暂不启动 scheduler，以验证启动时的 AddPod 处理
		testCtx := testutils.InitTestAPIServer(t, "deferred-handlers-test", nil)

		cs := testCtx.ClientSet

		// 创建 node1
		nodeObject := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourcePods: "32",
			v1.ResourceCPU:  "200m",
		}).Obj()
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// 创建 Pod 并伪造延迟扩缩容状态
		pod := initPausePod(&testutils.PausePodConfig{
			Name:     "deferred-pod",
			NodeName: "node1",
			Priority: &lowPriority,
			Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
			},
		})
		pod.Namespace = testCtx.NS.Name
		pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonDeferred,
			},
		}
		pod.Status.ContainerStatuses = []v1.ContainerStatus{
			{
				Name: pod.Name,
				AllocatedResources: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
				},
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
					},
				},
			},
		}
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status: %v", err)
		}

		// 此时再启动 scheduler，验证其注册 handler 时会列出并入队已有 Pod
		testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithProfiles(cfg.Profiles...))
		defer testCtx.SchedulerCloseFn()
		testutils.SyncSchedulerInformerFactory(testCtx)

		// 验证 AddPod handler 在 scheduler 启动时立即将该 Pod 入队
		queue := testCtx.Scheduler.SchedulingQueue
		var found bool
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			_, found = queue.GetPod(pod.Name, pod.Namespace, nil)
			return found, nil
		})
		if err != nil || !found {
			t.Fatalf("Expected pod to be queued automatically by AddPod handler on scheduler startup, err: %v, found: %t", err, found)
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod})
	})

	t.Run("UpdatePod handler enqueues pod when it transitions to deferred resize", func(t *testing.T) {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.InPlacePodVerticalScaling:                    true,
			features.InPlacePodVerticalScalingSchedulerPreemption: true,
		})

		cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
			Profiles: []configv1.KubeSchedulerProfile{{
				SchedulerName: ptr.To(v1.DefaultSchedulerName),
			}},
		})

		testCtx := testutils.InitTestSchedulerWithOptions(t,
			testutils.InitTestAPIServer(t, "deferred-handlers-test", nil),
			0,
			scheduler.WithProfiles(cfg.Profiles...),
		)
		defer testCtx.SchedulerCloseFn()
		testutils.SyncSchedulerInformerFactory(testCtx)

		// 只启动 informer 与队列，不启动调度循环，以隔离 handler 行为
		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue

		// 创建 node1
		nodeObject := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourcePods: "32",
			v1.ResourceCPU:  "200m",
		}).Obj()
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// 先在 node1 上创建一个普通运行中的 Pod（无延迟条件）
		pod := initPausePod(&testutils.PausePodConfig{
			Name:     "deferred-pod",
			NodeName: "node1",
			Priority: &lowPriority,
			Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
			},
		})
		pod.Namespace = testCtx.NS.Name
		pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
		}
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status: %v", err)
		}

		// 普通 Pod 不应进入调度队列
		time.Sleep(300 * time.Millisecond)
		if _, found := queue.GetPod(pod.Name, pod.Namespace, nil); found {
			t.Fatalf("Expected pod to not be in scheduling queue initially")
		}

		// 更新 status，加上延迟扩缩容条件
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get pod: %v", err)
		}
		pod.Status.Conditions = append(pod.Status.Conditions, v1.PodCondition{
			Type:   v1.PodResizePending,
			Status: v1.ConditionTrue,
			Reason: v1.PodReasonDeferred,
		})
		pod.Status.ContainerStatuses = []v1.ContainerStatus{
			{
				Name: pod.Name,
				AllocatedResources: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
				},
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
					},
				},
			},
		}
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to transition status: %v", err)
		}

		// 验证状态转换后 UpdatePod handler 自动将 Pod 入队
		var found bool
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			_, found = queue.GetPod(pod.Name, pod.Namespace, nil)
			return found, nil
		})
		if err != nil || !found {
			t.Fatalf("Expected pod to be enqueued automatically by UpdatePod handler on deferred status transition")
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod})
	})

	t.Run("UpdatePod handler removes pod from queue when deferred condition is cleared", func(t *testing.T) {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.InPlacePodVerticalScaling:                    true,
			features.InPlacePodVerticalScalingSchedulerPreemption: true,
		})

		cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
			Profiles: []configv1.KubeSchedulerProfile{{
				SchedulerName: ptr.To(v1.DefaultSchedulerName),
			}},
		})

		testCtx := testutils.InitTestSchedulerWithOptions(t,
			testutils.InitTestAPIServer(t, "deferred-handlers-test", nil),
			0,
			scheduler.WithProfiles(cfg.Profiles...),
		)
		defer testCtx.SchedulerCloseFn()
		testutils.SyncSchedulerInformerFactory(testCtx)

		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue

		nodeObject := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourcePods: "32",
			v1.ResourceCPU:  "200m",
		}).Obj()
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		pod := initPausePod(&testutils.PausePodConfig{
			Name:     "deferred-pod",
			NodeName: "node1",
			Priority: &lowPriority,
			Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
			},
		})
		pod.Namespace = testCtx.NS.Name
		pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonDeferred,
			},
		}
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status: %v", err)
		}

		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			_, found := queue.GetPod(pod.Name, pod.Namespace, nil)
			return found, nil
		})
		if err != nil {
			t.Fatalf("Expected pod to be in queue after transitioning to deferred")
		}

		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get pod: %v", err)
		}
		pod.Status.Conditions = []v1.PodCondition{
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
		}
		if _, err := cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("Failed to update status clearing deferred condition: %v", err)
		}

		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			_, found := queue.GetPod(pod.Name, pod.Namespace, nil)
			return !found, nil
		})
		if err != nil {
			t.Fatalf("Expected pod to be removed from queue after deferred condition cleared")
		}

		testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{pod})
	})

	t.Run("DeletePod handler removes pod from both cache and scheduling queue when deleted", func(t *testing.T) {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.InPlacePodVerticalScaling:                    true,
			features.InPlacePodVerticalScalingSchedulerPreemption: true,
		})

		cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
			Profiles: []configv1.KubeSchedulerProfile{{
				SchedulerName: ptr.To(v1.DefaultSchedulerName),
			}},
		})

		testCtx := testutils.InitTestSchedulerWithOptions(t,
			testutils.InitTestAPIServer(t, "def-del", nil),
			0,
			scheduler.WithProfiles(cfg.Profiles...),
		)
		defer testCtx.SchedulerCloseFn()
		testutils.SyncSchedulerInformerFactory(testCtx)

		cs := testCtx.ClientSet
		queue := testCtx.Scheduler.SchedulingQueue
		cache := testCtx.Scheduler.Cache

		// 创建 node1
		nodeObject := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourcePods: "32",
			v1.ResourceCPU:  "200m",
		}).Obj()
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, nodeObject, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// 创建运行在 node1 上、扩缩容被延迟的 Pod
		pod := initPausePod(&testutils.PausePodConfig{
			Name:     "deferred-pod-del",
			NodeName: "node1",
			Priority: &lowPriority,
			Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
			},
		})
		pod.Namespace = testCtx.NS.Name
		pod, err := cs.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodResizePending,
				Status: v1.ConditionTrue,
				Reason: v1.PodReasonDeferred,
			},
		}
		pod.Status.ContainerStatuses = []v1.ContainerStatus{
			{
				Name: pod.Name,
				AllocatedResources: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
				},
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI),
					},
				},
			},
		}
		pod, err = cs.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Failed to update status: %v", err)
		}

		// 等待 Pod 同时出现在调度队列和 scheduler cache 中
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
			_, foundInQueue := queue.GetPod(pod.Name, pod.Namespace, nil)
			_, errCache := cache.GetPod(pod)
			return foundInQueue && errCache == nil, nil
		})
		if err != nil {
			t.Fatalf("Expected pod to be in both cache and queue initially: %v", err)
		}

		// 删除 Pod
		if err := cs.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)}); err != nil {
			t.Fatalf("Failed to delete pod: %v", err)
		}

		// 验证 DeletePod handler 将 Pod 从队列和缓存中一并移除
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
			_, foundInQueue := queue.GetPod(pod.Name, pod.Namespace, nil)
			_, errCache := cache.GetPod(pod)
			return !foundInQueue && errCache != nil, nil
		})
		if err != nil {
			t.Fatalf("Expected pod to be removed from both cache and queue after deletion: %v", err)
		}
	})
}
