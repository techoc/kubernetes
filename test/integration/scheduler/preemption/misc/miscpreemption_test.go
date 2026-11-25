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

package miscpreemption

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumerestrictions"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/plugin/pkg/admission/priority"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/utils/ptr"
)

// 从 testutils 导入
var (
	initPausePod                    = testutils.InitPausePod
	createNode                      = testutils.CreateNode
	createPausePod                  = testutils.CreatePausePod
	runPausePod                     = testutils.RunPausePod
	initTest                        = testutils.InitTestSchedulerWithNS
	initTestDisablePreemption       = testutils.InitTestDisablePreemption
	initDisruptionController        = testutils.InitDisruptionController
	waitCachedPodsStable            = testutils.WaitCachedPodsStable
	podIsGettingEvicted             = testutils.PodIsGettingEvicted
	podUnschedulable                = testutils.PodUnschedulable
	waitForPDBsStable               = testutils.WaitForPDBsStable
	waitForPodToScheduleWithTimeout = testutils.WaitForPodToScheduleWithTimeout
	waitForPodUnschedulable         = testutils.WaitForPodUnschedulable
)

var lowPriority, mediumPriority, highPriority = int32(100), int32(200), int32(300)

// TestNonPreemption 测试调度器的 PriorityClass 的 NonPreempt 选项是否按预期工作。
func TestNonPreemption(t *testing.T) {
	var preemptNever = v1.PreemptNever
	// 初始化调度器。
	testCtx := initTest(t, "non-preemption")
	cs := testCtx.ClientSet
	tests := []struct {
		name             string
		PreemptionPolicy *v1.PreemptionPolicy
	}{
		{
			name:             "pod preemption will happen",
			PreemptionPolicy: nil,
		},
		{
			name:             "pod preemption will not happen",
			PreemptionPolicy: &preemptNever,
		},
	}
	victim := initPausePod(&testutils.PausePodConfig{
		Name:      "victim-pod",
		Namespace: testCtx.NS.Name,
		Priority:  &lowPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(400, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
		},
	})

	preemptor := initPausePod(&testutils.PausePodConfig{
		Name:      "preemptor-pod",
		Namespace: testCtx.NS.Name,
		Priority:  &highPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
		},
	})

	// 创建一个带有一些资源的节点
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	_, err := createNode(testCtx.ClientSet, st.MakeNode().Name("node1").Capacity(nodeRes).Obj())
	if err != nil {
		t.Fatalf("Error creating nodes: %v", err)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s (Async preemption enabled: %v)", test.name, asyncPreemptionEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, asyncPreemptionEnabled)

				defer testutils.CleanupPods(testCtx.Ctx, cs, t, []*v1.Pod{preemptor, victim})
				preemptor.Spec.PreemptionPolicy = test.PreemptionPolicy
				victimPod, err := createPausePod(cs, victim)
				if err != nil {
					t.Fatalf("Error while creating victim: %v", err)
				}
				if err := waitForPodToScheduleWithTimeout(testCtx.Ctx, cs, victimPod, 5*time.Second); err != nil {
					t.Fatalf("victim %v should be become scheduled", victimPod.Name)
				}

				preemptorPod, err := createPausePod(cs, preemptor)
				if err != nil {
					t.Fatalf("Error while creating preemptor: %v", err)
				}

				err = testutils.WaitForNominatedNodeNameWithTimeout(testCtx.Ctx, cs, preemptorPod, 5*time.Second)
				// test.PreemptionPolicy == nil 表示期望抢占者获得提名。
				expect := test.PreemptionPolicy == nil
				// err == nil 表示抢占者确实获得了提名。
				got := err == nil
				if got != expect {
					t.Errorf("Expect preemptor to be nominated=%v, but got=%v", expect, got)
				}
			})
		}
	}
}

// TestDisablePreemption 测试调度器禁用 Pod 抢占的功能是否按预期工作。
func TestDisablePreemption(t *testing.T) {
	// 初始化调度器，并禁用抢占。
	testCtx := initTestDisablePreemption(t, "disable-preemption")
	cs := testCtx.ClientSet

	tests := []struct {
		name         string
		existingPods []*v1.Pod
		pod          *v1.Pod
	}{
		{
			name: "pod preemption will not happen",
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "victim-pod",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(400, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
		},
	}

	// 创建一个带有一些资源的节点
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	_, err := createNode(testCtx.ClientSet, st.MakeNode().Name("node1").Capacity(nodeRes).Obj())
	if err != nil {
		t.Fatalf("Error creating nodes: %v", err)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s (Async preemption enabled: %v)", test.name, asyncPreemptionEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, asyncPreemptionEnabled)

				pods := make([]*v1.Pod, len(test.existingPods))
				// 创建并运行 existingPods。
				for i, p := range test.existingPods {
					pods[i], err = runPausePod(cs, p)
					if err != nil {
						t.Fatalf("Test [%v]: Error running pause pod: %v", test.name, err)
					}
				}
				// 创建该 Pod。
				preemptor, err := createPausePod(cs, test.pod)
				if err != nil {
					t.Errorf("Error while creating high priority pod: %v", err)
				}
				// 确保抢占者保持不可调度状态。
				if err := waitForPodUnschedulable(testCtx.Ctx, cs, preemptor); err != nil {
					t.Errorf("Preemptor %v should not become scheduled", preemptor.Name)
				}

				// 确保抢占者不会获得提名。
				if err := testutils.WaitForNominatedNodeNameWithTimeout(testCtx.Ctx, cs, preemptor, 5*time.Second); err == nil {
					t.Errorf("Preemptor %v should not be nominated", preemptor.Name)
				}

				// 清理
				pods = append(pods, preemptor)
				testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
			})
		}
	}
}

// 该测试验证系统关键优先级会被自动创建并正确解析。
func TestPodPriorityResolution(t *testing.T) {
	admission := priority.NewPlugin()
	testCtx := testutils.InitTestScheduler(t, testutils.InitTestAPIServer(t, "preemption", admission))
	cs := testCtx.ClientSet

	// 为控制器构建 clientset 和 informer。
	externalClientConfig := restclient.CopyConfig(testCtx.KubeConfig)
	externalClientConfig.QPS = -1
	externalClientset := clientset.NewForConfigOrDie(externalClientConfig)
	externalInformers := informers.NewSharedInformerFactory(externalClientset, time.Second)
	admission.SetExternalKubeClientSet(externalClientset)
	admission.SetExternalKubeInformerFactory(externalInformers)

	// 等待所有控制器完成同步
	testutils.SyncSchedulerInformerFactory(testCtx)
	externalInformers.Start(testCtx.Ctx.Done())
	externalInformers.WaitForCacheSync(testCtx.Ctx.Done())

	// 运行所有控制器
	go testCtx.Scheduler.Run(testCtx.Ctx)

	tests := []struct {
		Name             string
		PriorityClass    string
		Pod              *v1.Pod
		ExpectedPriority int32
		ExpectedError    error
	}{
		{
			Name:             "SystemNodeCritical priority class",
			PriorityClass:    scheduling.SystemNodeCritical,
			ExpectedPriority: scheduling.SystemCriticalPriority + 1000,
			Pod: initPausePod(&testutils.PausePodConfig{
				Name:              fmt.Sprintf("pod1-%v", scheduling.SystemNodeCritical),
				Namespace:         metav1.NamespaceSystem,
				PriorityClassName: scheduling.SystemNodeCritical,
			}),
		},
		{
			Name:             "SystemClusterCritical priority class",
			PriorityClass:    scheduling.SystemClusterCritical,
			ExpectedPriority: scheduling.SystemCriticalPriority,
			Pod: initPausePod(&testutils.PausePodConfig{
				Name:              fmt.Sprintf("pod2-%v", scheduling.SystemClusterCritical),
				Namespace:         metav1.NamespaceSystem,
				PriorityClassName: scheduling.SystemClusterCritical,
			}),
		},
		{
			Name:             "Invalid priority class should result in error",
			PriorityClass:    "foo",
			ExpectedPriority: scheduling.SystemCriticalPriority,
			Pod: initPausePod(&testutils.PausePodConfig{
				Name:              fmt.Sprintf("pod3-%v", scheduling.SystemClusterCritical),
				Namespace:         metav1.NamespaceSystem,
				PriorityClassName: "foo",
			}),
			ExpectedError: fmt.Errorf("failed to create pause pod: pods \"pod3-system-cluster-critical\" is forbidden: no PriorityClass with name foo was found"),
		},
	}

	// 创建一个带有一些资源的节点
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	_, err := createNode(testCtx.ClientSet, st.MakeNode().Name("node1").Capacity(nodeRes).Obj())
	if err != nil {
		t.Fatalf("Error creating nodes: %v", err)
	}

	pods := make([]*v1.Pod, 0, len(tests))
	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s (Async preemption enabled: %v)", test.Name, asyncPreemptionEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, asyncPreemptionEnabled)

				pod, err := runPausePod(cs, test.Pod)
				if err != nil {
					if test.ExpectedError == nil {
						t.Fatalf("Test [PodPriority/%v]: Error running pause pod: %v", test.PriorityClass, err)
					}
					if err.Error() != test.ExpectedError.Error() {
						t.Fatalf("Test [PodPriority/%v]: Expected error %v but got error %v", test.PriorityClass, test.ExpectedError, err)
					}
					return
				}
				pods = append(pods, pod)
				if pod.Spec.Priority != nil {
					if *pod.Spec.Priority != test.ExpectedPriority {
						t.Errorf("Expected pod %v to have priority %v but was %v", pod.Name, test.ExpectedPriority, pod.Spec.Priority)
					}
				} else {
					t.Errorf("Expected pod %v to have priority %v but was nil", pod.Name, test.PriorityClass)
				}
				testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
			})
		}
	}
	testutils.CleanupNodes(cs, t)
}

func mkPriorityPodWithGrace(tc *testutils.TestContext, name string, priority int32, grace int64) *v1.Pod {
	defaultPodRes := &v1.ResourceRequirements{Requests: v1.ResourceList{
		v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
	}
	pod := initPausePod(&testutils.PausePodConfig{
		Name:      name,
		Namespace: tc.NS.Name,
		Priority:  &priority,
		Labels:    map[string]string{"pod": name},
		Resources: defaultPodRes,
	})
	pod.Spec.TerminationGracePeriodSeconds = &grace
	return pod
}

// 该测试确保当抢占中的 Pod 等待被驱逐者终止时，
// 其他待调度的低优先级 Pod 不会占用抢占后腾出的空间，
// 且高优先级 Pod 尚未被调度。
func TestPreemptionStarvation(t *testing.T) {
	// 初始化调度器。
	testCtx := initTest(t, "preemption")
	cs := testCtx.ClientSet

	tests := []struct {
		name               string
		numExistingPod     int
		numExpectedPending int
		preemptor          *v1.Pod
	}{
		{
			// 该测试确保当抢占中的 Pod 等待被驱逐者终止时，
			// 其他低优先级 Pod 不会占用抢占后腾出的空间，
			// 且高优先级 Pod 尚未被调度。
			name:               "starvation test: higher priority pod is scheduled before the lower priority ones",
			numExistingPod:     10,
			numExpectedPending: 5,
			preemptor: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
		},
	}

	// 创建一个带有一些资源的节点
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	_, err := createNode(testCtx.ClientSet, st.MakeNode().Name("node1").Capacity(nodeRes).Obj())
	if err != nil {
		t.Fatalf("Error creating nodes: %v", err)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s (Async preemption enabled: %v, ClearingNominatedNodeNameAfterBinding: %v)", test.name, asyncPreemptionEnabled, clearingNominatedNodeNameAfterBinding), func(t *testing.T) {
					featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
						features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
						features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
					})

					pendingPods := make([]*v1.Pod, test.numExpectedPending)
					numRunningPods := test.numExistingPod - test.numExpectedPending
					runningPods := make([]*v1.Pod, numRunningPods)
					// 创建并运行 existingPods。
					for i := range numRunningPods {
						runningPods[i], err = createPausePod(cs, mkPriorityPodWithGrace(testCtx, fmt.Sprintf("rpod-%v", i), mediumPriority, 0))
						if err != nil {
							t.Fatalf("Error creating pause pod: %v", err)
						}
					}
					// 确保 runningPods 都已被调度。
					for _, p := range runningPods {
						if err := testutils.WaitForPodToSchedule(testCtx.Ctx, cs, p); err != nil {
							t.Fatalf("Pod %v/%v didn't get scheduled: %v", p.Namespace, p.Name, err)
						}
					}
					// 创建待调度的 Pod。
					for i := 0; i < test.numExpectedPending; i++ {
						pendingPods[i], err = createPausePod(cs, mkPriorityPodWithGrace(testCtx, fmt.Sprintf("ppod-%v", i), mediumPriority, 0))
						if err != nil {
							t.Fatalf("Error creating pending pod: %v", err)
						}
					}
					// 确保所有待调度的 Pod 都被标记为不可调度。
					for _, p := range pendingPods {
						if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, wait.ForeverTestTimeout, false,
							podUnschedulable(cs, p.Namespace, p.Name)); err != nil {
							t.Errorf("Pod %v/%v didn't get marked unschedulable: %v", p.Namespace, p.Name, err)
						}
					}
					// 创建抢占者。
					preemptor, err := createPausePod(cs, test.preemptor)
					if err != nil {
						t.Errorf("Error while creating the preempting pod: %v", err)
					}

					// 确保抢占完成后抢占者被调度。
					if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, cs, preemptor, 60*time.Second); err != nil {
						t.Errorf("Preemptor pod %v didn't get scheduled: %v", preemptor.Name, err)
					}

					// 检查当 feature gate 禁用时，抢占者 Pod 的 .status.nominatedNodeName 是否被设置。
					// 由于 numExistingPod（10）占满了节点，该测试总是预期发生抢占。
					if !clearingNominatedNodeNameAfterBinding {
						if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
							t.Errorf(".status.nominatedNodeName was not set for pod %v/%v: %v", preemptor.Namespace, preemptor.Name, err)
						}
					}
					// 清理
					klog.Info("Cleaning up all pods...")
					allPods := pendingPods
					allPods = append(allPods, runningPods...)
					allPods = append(allPods, preemptor)
					testutils.CleanupPods(testCtx.Ctx, cs, t, allPods)
				})
			}
		}
	}
}

// TestPreemptionRaces 测试其他调度事件和操作不会与抢占过程产生竞态。
func TestPreemptionRaces(t *testing.T) {
	// 初始化调度器。
	testCtx := initTest(t, "preemption-race")
	cs := testCtx.ClientSet

	tests := []struct {
		name              string
		numInitialPods    int // 在运行抢占者之前创建并运行的 Pod
		numAdditionalPods int // 创建抢占者之后创建的 Pod
		numRepetitions    int // 重复测试以检查竞态
		preemptor         *v1.Pod
	}{
		{
			// 该测试确保当抢占中的 Pod 等待被驱逐者终止时，
			// 其他低优先级 Pod 不会占用抢占后腾出的空间，
			// 且高优先级 Pod 尚未被调度。
			name:              "ensures that other pods are not scheduled while preemptor is being marked as nominated (issue #72124)",
			numInitialPods:    2,
			numAdditionalPods: 20,
			numRepetitions:    5,
			preemptor: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(4900, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(4900, resource.DecimalSI)},
				},
			}),
		},
	}

	// 创建一个带有一些资源的节点
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "5000m",
		v1.ResourceMemory: "5000",
	}
	_, err := createNode(testCtx.ClientSet, st.MakeNode().Name("node1").Capacity(nodeRes).Obj())
	if err != nil {
		t.Fatalf("Error creating nodes: %v", err)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s (Async preemption enabled: %v, ClearingNominatedNodeNameAfterBinding: %v)", test.name, asyncPreemptionEnabled, clearingNominatedNodeNameAfterBinding), func(t *testing.T) {
					featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
						features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
						features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
					})

					if test.numRepetitions <= 0 {
						test.numRepetitions = 1
					}
					for n := 0; n < test.numRepetitions; n++ {
						initialPods := make([]*v1.Pod, test.numInitialPods)
						additionalPods := make([]*v1.Pod, test.numAdditionalPods)
						// 创建并运行 existingPods。
						for i := 0; i < test.numInitialPods; i++ {
							initialPods[i], err = createPausePod(cs, mkPriorityPodWithGrace(testCtx, fmt.Sprintf("rpod-%v", i), mediumPriority, 0))
							if err != nil {
								t.Fatalf("Error creating pause pod: %v", err)
							}
						}
						// 确保初始的 Pod 都已被调度。
						for _, p := range initialPods {
							if err := testutils.WaitForPodToSchedule(testCtx.Ctx, cs, p); err != nil {
								t.Fatalf("Pod %v/%v didn't get scheduled: %v", p.Namespace, p.Name, err)
							}
						}
						// 创建抢占者。
						klog.Info("Creating the preemptor pod...")
						preemptor, err := createPausePod(cs, test.preemptor)
						if err != nil {
							t.Errorf("Error while creating the preempting pod: %v", err)
						}

						klog.Info("Creating additional pods...")
						for i := 0; i < test.numAdditionalPods; i++ {
							additionalPods[i], err = createPausePod(cs, mkPriorityPodWithGrace(testCtx, fmt.Sprintf("ppod-%v", i), mediumPriority, 0))
							if err != nil {
								t.Fatalf("Error creating pending pod: %v", err)
							}
						}
						// 确保抢占完成后抢占者被调度。
						if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, cs, preemptor, 60*time.Second); err != nil {
							t.Errorf("Preemptor pod %v didn't get scheduled: %v", preemptor.Name, err)
						}

						// 检查当 feature gate 禁用时，抢占者 Pod 是否获得提名节点名称。
						if !clearingNominatedNodeNameAfterBinding {
							if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
								t.Errorf(".status.nominatedNodeName was not set for pod %v/%v: %v", preemptor.Namespace, preemptor.Name, err)
							}
						}

						klog.Info("Check unschedulable pods still exists and were never scheduled...")
						for _, p := range additionalPods {
							pod, err := cs.CoreV1().Pods(p.Namespace).Get(testCtx.Ctx, p.Name, metav1.GetOptions{})
							if err != nil {
								t.Errorf("Error in getting Pod %v/%v info: %v", p.Namespace, p.Name, err)
							}
							if len(pod.Spec.NodeName) > 0 {
								t.Errorf("Pod %v/%v is already scheduled", p.Namespace, p.Name)
							}
							_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
							if cond != nil && cond.Status != v1.ConditionFalse {
								t.Errorf("Pod %v/%v is no longer unschedulable: %v", p.Namespace, p.Name, err)
							}
						}
						// 清理
						klog.Info("Cleaning up all pods...")
						allPods := additionalPods
						allPods = append(allPods, initialPods...)
						allPods = append(allPods, preemptor)
						testutils.CleanupPods(testCtx.Ctx, cs, t, allPods)
					}
				})
			}
		}
	}
}

func mkMinAvailablePDB(name, namespace string, uid types.UID, minAvailable int, matchLabels map[string]string) *policy.PodDisruptionBudget {
	intMinAvailable := intstr.FromInt32(int32(minAvailable))
	return &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MinAvailable: &intMinAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
}

func addPodConditionReady(pod *v1.Pod) {
	pod.Status = v1.PodStatus{
		Phase: v1.PodRunning,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.PodReady,
				Status: v1.ConditionTrue,
			},
		},
	}
}

// TestPDBInPreemption 测试抢占过程中对 PodDisruptionBudget 的支持。
func TestPDBInPreemption(t *testing.T) {
	// 初始化调度器。
	testCtx := initTest(t, "preemption-pdb")
	cs := testCtx.ClientSet

	initDisruptionController(t, testCtx)

	defaultPodRes := &v1.ResourceRequirements{Requests: v1.ResourceList{
		v1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(100, resource.DecimalSI)},
	}
	defaultNodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}

	tests := []struct {
		name                string
		nodeCnt             int
		pdbs                []*policy.PodDisruptionBudget
		pdbPodNum           []int32
		existingPods        []*v1.Pod
		pod                 *v1.Pod
		preemptedPodIndexes map[int]struct{}
	}{
		{
			name:    "A non-PDB violating pod is preempted despite its higher priority",
			nodeCnt: 1,
			pdbs: []*policy.PodDisruptionBudget{
				mkMinAvailablePDB("pdb-1", testCtx.NS.Name, types.UID("pdb-1-uid"), 2, map[string]string{"foo": "bar"}),
			},
			pdbPodNum: []int32{2},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod1",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					Labels:    map[string]string{"foo": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod2",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					Labels:    map[string]string{"foo": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "mid-pod3",
					Namespace: testCtx.NS.Name,
					Priority:  &mediumPriority,
					Resources: defaultPodRes,
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{2: {}},
		},
		{
			name:    "A node without any PDB violating pods is preferred for preemption",
			nodeCnt: 2,
			pdbs: []*policy.PodDisruptionBudget{
				mkMinAvailablePDB("pdb-1", testCtx.NS.Name, types.UID("pdb-1-uid"), 2, map[string]string{"foo": "bar"}),
			},
			pdbPodNum: []int32{1},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod1",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-1",
					Labels:    map[string]string{"foo": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "mid-pod2",
					Namespace: testCtx.NS.Name,
					Priority:  &mediumPriority,
					NodeName:  "node-2",
					Resources: defaultPodRes,
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(200, resource.DecimalSI)},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{1: {}},
		},
		{
			name:    "A node with fewer PDB violating pods is preferred for preemption",
			nodeCnt: 3,
			pdbs: []*policy.PodDisruptionBudget{
				mkMinAvailablePDB("pdb-1", testCtx.NS.Name, types.UID("pdb-1-uid"), 2, map[string]string{"foo1": "bar"}),
				mkMinAvailablePDB("pdb-2", testCtx.NS.Name, types.UID("pdb-2-uid"), 2, map[string]string{"foo2": "bar"}),
			},
			pdbPodNum: []int32{1, 5},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod1",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-1",
					Labels:    map[string]string{"foo1": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "mid-pod1",
					Namespace: testCtx.NS.Name,
					Priority:  &mediumPriority,
					Resources: defaultPodRes,
					NodeName:  "node-1",
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod2",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-2",
					Labels:    map[string]string{"foo2": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "mid-pod2",
					Namespace: testCtx.NS.Name,
					Priority:  &mediumPriority,
					Resources: defaultPodRes,
					NodeName:  "node-2",
					Labels:    map[string]string{"foo2": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod4",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-3",
					Labels:    map[string]string{"foo2": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod5",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-3",
					Labels:    map[string]string{"foo2": "bar"},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "low-pod6",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Resources: defaultPodRes,
					NodeName:  "node-3",
					Labels:    map[string]string{"foo2": "bar"},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(400, resource.DecimalSI)},
				},
			}),
			// 选择第三个节点是因为在 node 3 上不会违反 PDB，且其牺牲者的优先级低于 node-2 上的。
			preemptedPodIndexes: map[int]struct{}{4: {}, 5: {}, 6: {}},
		},
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s (Async preemption enabled: %v, ClearingNominatedNodeNameAfterBinding: %v)", test.name, asyncPreemptionEnabled, clearingNominatedNodeNameAfterBinding), func(t *testing.T) {
					featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
						features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
						features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
					})

					for i := 1; i <= test.nodeCnt; i++ {
						nodeName := fmt.Sprintf("node-%v", i)
						_, err := createNode(cs, st.MakeNode().Name(nodeName).Capacity(defaultNodeRes).Obj())
						if err != nil {
							t.Fatalf("Error creating node %v: %v", nodeName, err)
						}
					}

					pods := make([]*v1.Pod, len(test.existingPods))
					var err error
					// 创建并运行 existingPods。
					for i, p := range test.existingPods {
						if pods[i], err = runPausePod(cs, p); err != nil {
							t.Fatalf("Test [%v]: Error running pause pod: %v", test.name, err)
						}
						// 添加 Pod 的 Ready 条件，以便 PDB 得到更新。
						addPodConditionReady(p)
						if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).UpdateStatus(testCtx.Ctx, p, metav1.UpdateOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					// 等待 Pod 在调度器缓存中稳定下来。
					if err := waitCachedPodsStable(testCtx, test.existingPods); err != nil {
						t.Fatalf("Not all pods are stable in the cache: %v", err)
					}

					// 创建 PDB。
					for _, pdb := range test.pdbs {
						_, err := testCtx.ClientSet.PolicyV1().PodDisruptionBudgets(testCtx.NS.Name).Create(testCtx.Ctx, pdb, metav1.CreateOptions{})
						if err != nil {
							t.Fatalf("Failed to create PDB: %v", err)
						}
					}
					// 等待 PDB 稳定下来。
					if err := waitForPDBsStable(testCtx, test.pdbs, test.pdbPodNum); err != nil {
						t.Fatalf("Not all pdbs are stable in the cache: %v", err)
					}

					// 创建该 Pod。
					preemptor, err := createPausePod(cs, test.pod)
					if err != nil {
						t.Errorf("Error while creating high priority pod: %v", err)
					}
					// 等待 Pod 被抢占，并确保其他 Pod 没有被抢占。
					for i, p := range pods {
						if _, found := test.preemptedPodIndexes[i]; found {
							if err = wait.PollUntilContextTimeout(testCtx.Ctx, time.Second, wait.ForeverTestTimeout, false,
								podIsGettingEvicted(cs, p.Namespace, p.Name)); err != nil {
								t.Errorf("Test [%v]: Pod %v/%v is not getting evicted.", test.name, p.Namespace, p.Name)
							}
						} else {
							if p.DeletionTimestamp != nil {
								t.Errorf("Test [%v]: Didn't expect pod %v/%v to get preempted.", test.name, p.Namespace, p.Name)
							}
						}
					}
					// 同时检查抢占者 Pod 的 .status.nominatedNodeName 是否被设置。
					if len(test.preemptedPodIndexes) > 0 && !clearingNominatedNodeNameAfterBinding {
						if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
							t.Errorf("Test [%v]: .status.nominatedNodeName was not set for pod %v/%v: %v", test.name, preemptor.Namespace, preemptor.Name, err)
						}
					}

					// 清理
					pods = append(pods, preemptor)
					testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
					if err := cs.PolicyV1().PodDisruptionBudgets(testCtx.NS.Name).DeleteCollection(testCtx.Ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
						t.Errorf("error while deleting PDBs, error: %v", err)
					}
					if err := cs.CoreV1().Nodes().DeleteCollection(testCtx.Ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
						t.Errorf("error whiling deleting nodes, error: %v", err)
					}
				})
			}
		}
	}
}

// TestReadWriteOncePodPreemption 测试使用 ReadWriteOncePod PVC 的 Pod 的抢占场景。
func TestReadWriteOncePodPreemption(t *testing.T) {
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Filter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: volumerestrictions.Name},
					},
				},
				PreFilter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: volumerestrictions.Name},
					},
				},
			},
		}},
	})

	testCtx := testutils.InitTestSchedulerWithOptions(t,
		testutils.InitTestAPIServer(t, "preemption", nil),
		0,
		scheduler.WithProfiles(cfg.Profiles...))
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs := testCtx.ClientSet

	storage := v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}
	volType := v1.HostPathDirectoryOrCreate
	pv1 := st.MakePersistentVolume().
		Name("pv-with-read-write-once-pod-1").
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Capacity(storage.Requests).
		HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/mnt1", Type: &volType}).
		Obj()
	pvc1 := st.MakePersistentVolumeClaim().
		Name("pvc-with-read-write-once-pod-1").
		Namespace(testCtx.NS.Name).
		// PVC 被视为已绑定所需的注解和卷名。
		Annotation(volume.AnnBindCompleted, "true").
		VolumeName(pv1.Name).
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Resources(storage).
		Obj()
	pv2 := st.MakePersistentVolume().
		Name("pv-with-read-write-once-pod-2").
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Capacity(storage.Requests).
		HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/mnt2", Type: &volType}).
		Obj()
	pvc2 := st.MakePersistentVolumeClaim().
		Name("pvc-with-read-write-once-pod-2").
		Namespace(testCtx.NS.Name).
		// PVC 被视为已绑定所需的注解和卷名。
		Annotation(volume.AnnBindCompleted, "true").
		VolumeName(pv2.Name).
		AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
		Resources(storage).
		Obj()

	tests := []struct {
		name                string
		init                func() error
		existingPods        []*v1.Pod
		pod                 *v1.Pod
		unresolvable        bool
		preemptedPodIndexes map[int]struct{}
		cleanup             func() error
	}{
		{
			name: "preempt single pod",
			init: func() error {
				_, err := testutils.CreatePV(cs, pv1)
				if err != nil {
					return fmt.Errorf("cannot create pv: %v", err)
				}
				_, err = testutils.CreatePVC(cs, pvc1)
				if err != nil {
					return fmt.Errorf("cannot create pvc: %v", err)
				}
				return nil
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "victim-pod",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Volumes: []v1.Volume{{
						Name: "volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc1.Name,
							},
						},
					}},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Volumes: []v1.Volume{{
					Name: "volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc1.Name,
						},
					},
				}},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}},
			cleanup: func() error {
				if err := testutils.DeletePVC(cs, pvc1.Name, pvc1.Namespace); err != nil {
					return fmt.Errorf("cannot delete pvc: %v", err)
				}
				if err := testutils.DeletePV(cs, pv1.Name); err != nil {
					return fmt.Errorf("cannot delete pv: %v", err)
				}
				return nil
			},
		},
		{
			name: "preempt two pods",
			init: func() error {
				for _, pv := range []*v1.PersistentVolume{pv1, pv2} {
					_, err := testutils.CreatePV(cs, pv)
					if err != nil {
						return fmt.Errorf("cannot create pv: %v", err)
					}
				}
				for _, pvc := range []*v1.PersistentVolumeClaim{pvc1, pvc2} {
					_, err := testutils.CreatePVC(cs, pvc)
					if err != nil {
						return fmt.Errorf("cannot create pvc: %v", err)
					}
				}
				return nil
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "victim-pod-1",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Volumes: []v1.Volume{{
						Name: "volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc1.Name,
							},
						},
					}},
				}),
				initPausePod(&testutils.PausePodConfig{
					Name:      "victim-pod-2",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Volumes: []v1.Volume{{
						Name: "volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc2.Name,
							},
						},
					}},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Volumes: []v1.Volume{
					{
						Name: "volume-1",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc1.Name,
							},
						},
					},
					{
						Name: "volume-2",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc2.Name,
							},
						},
					},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}, 1: {}},
			cleanup: func() error {
				for _, pvc := range []*v1.PersistentVolumeClaim{pvc1, pvc2} {
					if err := testutils.DeletePVC(cs, pvc.Name, pvc.Namespace); err != nil {
						return fmt.Errorf("cannot delete pvc: %v", err)
					}
				}
				for _, pv := range []*v1.PersistentVolume{pv1, pv2} {
					if err := testutils.DeletePV(cs, pv.Name); err != nil {
						return fmt.Errorf("cannot delete pv: %v", err)
					}
				}
				return nil
			},
		},
		{
			name: "preempt single pod with two volumes",
			init: func() error {
				for _, pv := range []*v1.PersistentVolume{pv1, pv2} {
					_, err := testutils.CreatePV(cs, pv)
					if err != nil {
						return fmt.Errorf("cannot create pv: %v", err)
					}
				}
				for _, pvc := range []*v1.PersistentVolumeClaim{pvc1, pvc2} {
					_, err := testutils.CreatePVC(cs, pvc)
					if err != nil {
						return fmt.Errorf("cannot create pvc: %v", err)
					}
				}
				return nil
			},
			existingPods: []*v1.Pod{
				initPausePod(&testutils.PausePodConfig{
					Name:      "victim-pod",
					Namespace: testCtx.NS.Name,
					Priority:  &lowPriority,
					Volumes: []v1.Volume{
						{
							Name: "volume-1",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvc1.Name,
								},
							},
						},
						{
							Name: "volume-2",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvc2.Name,
								},
							},
						},
					},
				}),
			},
			pod: initPausePod(&testutils.PausePodConfig{
				Name:      "preemptor-pod",
				Namespace: testCtx.NS.Name,
				Priority:  &highPriority,
				Volumes: []v1.Volume{
					{
						Name: "volume-1",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc1.Name,
							},
						},
					},
					{
						Name: "volume-2",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc2.Name,
							},
						},
					},
				},
			}),
			preemptedPodIndexes: map[int]struct{}{0: {}},
			cleanup: func() error {
				for _, pvc := range []*v1.PersistentVolumeClaim{pvc1, pvc2} {
					if err := testutils.DeletePVC(cs, pvc.Name, pvc.Namespace); err != nil {
						return fmt.Errorf("cannot delete pvc: %v", err)
					}
				}
				for _, pv := range []*v1.PersistentVolume{pv1, pv2} {
					if err := testutils.DeletePV(cs, pv.Name); err != nil {
						return fmt.Errorf("cannot delete pv: %v", err)
					}
				}
				return nil
			},
		},
	}

	// 创建一个带有一些资源和标签的节点。
	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}
	nodeObject := st.MakeNode().Name("node1").Capacity(nodeRes).Label("node", "node1").Obj()
	if _, err := createNode(cs, nodeObject); err != nil {
		t.Fatalf("Error creating node: %v", err)
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, clearingNominatedNodeNameAfterBinding := range []bool{true, false} {
			for _, test := range tests {
				t.Run(fmt.Sprintf("%s (Async preemption enabled: %v, ClearingNominatedNodeNameAfterBinding: %v)", test.name, asyncPreemptionEnabled, clearingNominatedNodeNameAfterBinding), func(t *testing.T) {
					featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
						features.SchedulerAsyncPreemption:              asyncPreemptionEnabled,
						features.ClearingNominatedNodeNameAfterBinding: clearingNominatedNodeNameAfterBinding,
					})

					if err := test.init(); err != nil {
						t.Fatalf("Error while initializing test: %v", err)
					}

					pods := make([]*v1.Pod, len(test.existingPods))
					t.Cleanup(func() {
						testutils.CleanupPods(testCtx.Ctx, cs, t, pods)
						if err := test.cleanup(); err != nil {
							t.Errorf("Error cleaning up test: %v", err)
						}
					})
					// 创建并运行 existingPods。
					for i, p := range test.existingPods {
						var err error
						pods[i], err = runPausePod(cs, p)
						if err != nil {
							t.Fatalf("Error running pause pod: %v", err)
						}
					}
					// 创建该 Pod。
					preemptor, err := createPausePod(cs, test.pod)
					if err != nil {
						t.Errorf("Error while creating high priority pod: %v", err)
					}
					pods = append(pods, preemptor)
					// 等待 Pod 被抢占，并确保其他 Pod 没有被抢占。
					for i, p := range pods {
						if _, found := test.preemptedPodIndexes[i]; found {
							if err = wait.PollUntilContextTimeout(testCtx.Ctx, time.Second, wait.ForeverTestTimeout, false,
								podIsGettingEvicted(cs, p.Namespace, p.Name)); err != nil {
								t.Errorf("Pod %v/%v is not getting evicted.", p.Namespace, p.Name)
							}
						} else {
							if p.DeletionTimestamp != nil {
								t.Errorf("Didn't expect pod %v to get preempted.", p.Name)
							}
						}
					}
					// 同时检查抢占者 Pod 是否设置了 NominatedNodeName 字段。
					if len(test.preemptedPodIndexes) > 0 && !clearingNominatedNodeNameAfterBinding {
						if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, cs, preemptor); err != nil {
							t.Errorf("NominatedNodeName field was not set for pod %v: %v", preemptor.Name, err)
						}
					}
				})
			}
		}
	}
}
