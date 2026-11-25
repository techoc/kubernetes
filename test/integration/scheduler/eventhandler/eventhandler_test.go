/*
Copyright 2023 The Kubernetes Authors.

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

package eventhandler

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	schedulerutils "k8s.io/kubernetes/test/integration/scheduler"
	testutils "k8s.io/kubernetes/test/integration/util"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

var lowPriority, mediumPriority, highPriority int32 = 100, 200, 300

var _ fwk.FilterPlugin = &fooPlugin{}

type fooPlugin struct {
}

func (pl *fooPlugin) Name() string {
	return "foo"
}

func (pl *fooPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)
	taints := nodeInfo.Node().Spec.Taints
	if len(taints) == 0 {
		return nil
	}

	if corev1.TolerationsTolerateTaint(logger, pod.Spec.Tolerations, &nodeInfo.Node().Spec.Taints[0], utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators)) {
		return nil
	}
	return fwk.NewStatus(fwk.Unschedulable)
}

func (pl *fooPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}},
	}, nil
}

// newPlugin 返回一个生成指定 Plugin 的工厂。
func newPlugin(plugin fwk.Plugin) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return plugin, nil
	}
}

func TestUpdateNodeEvent(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "test-event", nil)

	taints := []v1.Taint{{Key: v1.TaintNodeUnschedulable, Value: "", Effect: v1.TaintEffectNoSchedule}}
	nodeWrapper := st.MakeNode().Name("node-0").Label("kubernetes.io/hostname", "node-0").Taints(taints).Obj()
	podWrapper := testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testContext.NS.Name})
	fooPlugin := &fooPlugin{}

	registry := frameworkruntime.Registry{
		fooPlugin.Name(): newPlugin(fooPlugin),
	}

	// 配置测试用的 plugin。
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To[string](v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Filter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: fooPlugin.Name()},
					},
					Disabled: []configv1.Plugin{
						{Name: "*"},
					},
				},
			},
		}},
	})

	testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 0, true,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
	)
	defer teardown()

	node, err := testutils.CreateNode(testCtx.ClientSet, nodeWrapper)
	if err != nil {
		t.Fatalf("Creating node error: %v", err)
	}

	pod, err := testutils.CreatePausePod(testCtx.ClientSet, podWrapper)
	if err != nil {
		t.Fatalf("Creating pod error: %v", err)
	}

	if err := testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
		t.Fatalf("Pod %v got scheduled: %v", pod.Name, err)
	}
	node, err = testCtx.ClientSet.CoreV1().Nodes().Get(testCtx.Ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Error while getting a node: %v", err)
	}

	// 更新 Node label 并移除 taint。
	node.Labels["foo"] = "bar"
	node.Spec.Taints = nil

	_, err = testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, node, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Error updating the node: %v", err)
	}

	if err := testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
		t.Errorf("Pod %v was not scheduled: %v", pod.Name, err)
	}
}

func TestUpdateNominatedNodeName(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	testBackoff := time.Minute
	testContext := testutils.InitTestAPIServer(t, "test-event", nil)
	capacity := map[v1.ResourceName]string{
		v1.ResourceMemory: "32",
	}
	var cleanupPods []*v1.Pod

	testNode := st.MakeNode().Name("node-0").Label("kubernetes.io/hostname", "node-0").Capacity(capacity).Obj()
	// 低优先级 Pod 与中优先级 Pod 无法共存，但可与高优先级 Pod 共存。
	podLow := testutils.InitPausePod(&testutils.PausePodConfig{
		Name:      "test-lp-pod",
		Namespace: testContext.NS.Name,
		Priority:  &lowPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceMemory: *resource.NewQuantity(20, resource.DecimalSI)},
		}})
	cleanupPods = append(cleanupPods, podLow)
	podMidNominated := testutils.InitPausePod(&testutils.PausePodConfig{
		Name:      "test-nominated-pod",
		Namespace: testContext.NS.Name,
		Priority:  &mediumPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceMemory: *resource.NewQuantity(25, resource.DecimalSI)},
		}})
	cleanupPods = append(cleanupPods, podMidNominated)
	podHigh := testutils.InitPausePod(&testutils.PausePodConfig{
		Name:      "test-hp-pod",
		Namespace: testContext.NS.Name,
		Priority:  &highPriority,
		Resources: &v1.ResourceRequirements{Requests: v1.ResourceList{
			v1.ResourceMemory: *resource.NewQuantity(10, resource.DecimalSI)},
		}})
	cleanupPods = append(cleanupPods, podHigh)

	tests := []struct {
		name       string
		updateFunc func(testCtx *testutils.TestContext)
	}{
		{
			name: "Preempt nominated pod",
			updateFunc: func(testCtx *testutils.TestContext) {
				// 创建高优先级 Pod 并等待其调度完成，以取消中优先级 Pod 的 nominate。
				pod, err := testutils.CreatePausePod(testCtx.ClientSet, podHigh)
				if err != nil {
					t.Fatalf("Creating pod error: %v", err)
				}
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Fatalf("Pod %v was not scheduled: %v", pod.Name, err)
				}
			},
		},
		{
			name: "Remove nominated pod",
			updateFunc: func(testCtx *testutils.TestContext) {
				if err := testutils.DeletePod(testCtx.ClientSet, podMidNominated.Name, podMidNominated.Namespace); err != nil {
					t.Fatalf("Deleting pod error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		for _, asyncAPICallsEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s (Async API calls enabled: %v)", tt.name, asyncAPICallsEnabled), func(t *testing.T) {
				// SchedulerAsyncAPICalls 特性仅在 1.34+ 版本中需要考虑。
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncAPICalls, asyncAPICallsEnabled)

				// 关闭 SchedulerPopFromBackoffQ，否则无法保证 Pod 不会提前从 backoffQ 中弹出。
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPopFromBackoffQ, false)

				testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 0, true,
					scheduler.WithClock(fakeClock),
					// updateFunc 必须在 nominated Pod 仍在 backoff 队列中时调用，故设置一个较小但非 0 的值。
					scheduler.WithPodInitialBackoffSeconds(int64(testBackoff.Seconds())),
					scheduler.WithPodMaxBackoffSeconds(int64(testBackoff.Seconds())),
				)
				defer teardown()

				_, err := testutils.CreateNode(testCtx.ClientSet, testNode)
				if err != nil {
					t.Fatalf("Creating node error: %v", err)
				}

				// 确认 Node 已进入 scheduler cache。
				if err := testutils.WaitForNodesInCache(testCtx.Ctx, testCtx.Scheduler, 1); err != nil {
					t.Fatalf("Waiting for node in cache error: %v", err)
				}

				// 创建初始低优先级 Pod 并等待其调度完成。
				pod, err := testutils.CreatePausePod(testCtx.ClientSet, podLow)
				if err != nil {
					t.Fatalf("Creating pod error: %v", err)
				}
				if err := testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Fatalf("Pod %v was not scheduled: %v", pod.Name, err)
				}

				// 创建中优先级 Pod，等待其抢占低优先级 Pod 成为 nominated Pod 且自身仍不可调度。
				pod, err = testutils.CreatePausePod(testCtx.ClientSet, podMidNominated)
				if err != nil {
					t.Fatalf("Creating pod error: %v", err)
				}
				if err := testutils.WaitForNominatedNodeName(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("NominatedNodeName field was not set for pod %v: %v", pod.Name, err)
				}
				if err := testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Pod %v haven't become unschedulabe: %v", pod.Name, err)
				}

				// 删除初始低优先级 Pod，使 nominated 且不可调度的 Pod 回到 backoff 队列。
				if err := testutils.DeletePod(testCtx.ClientSet, podLow.Name, podLow.Namespace); err != nil {
					t.Fatalf("Deleting pod error: %v", err)
				}

				// 再创建一个低优先级 Pod；中优先级 Pod 已 nominated 在该 Node 上且资源不足以容纳两者，故其不可调度。
				pod, err = testutils.CreatePausePod(testCtx.ClientSet, podLow)
				if err != nil {
					t.Fatalf("Creating pod error: %v", err)
				}
				if err := testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Fatalf("Pod %v was not scheduled: %v", pod.Name, err)
				}

				// 通过更新删除 nominated Pod 或清除其 nominatedNodeName，触发低优先级 Pod 重新调度。
				// 必须在 nominated Pod 仍处于 backoffQ 时更新，才能覆盖对已 nominated 但未绑定 Pod 的更新处理。
				tt.updateFunc(testCtx)

				// 将时间推进 2 倍 maxPodBackoffSeconds，使低优先级 Pod 离开 backoff 队列。
				fakeClock.Step(2 * testBackoff)

				// 低优先级 Pod 应收到中优先级 Pod 被取消 nominate 的通知，并在资源足够时完成调度。
				if err := testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, podLow); err != nil {
					t.Fatalf("Pod %v was not scheduled: %v", podLow.Name, err)
				}
				testutils.CleanupPods(testCtx.Ctx, testCtx.ClientSet, t, cleanupPods)
			})
		}
	}
}
