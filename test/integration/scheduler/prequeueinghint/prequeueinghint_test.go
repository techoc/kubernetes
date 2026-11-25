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

package prequeueinghint

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/utils/ptr"
)

// preQueueingHintPlugin 是一个测试插件，实现了 PreQueueingHintFn，
// 并记录其 QueueingHintFn 被调用的次数。
type preQueueingHintPlugin struct {
	queueingHintCalls atomic.Int64
	// targetPod 是 PreQueueingHintFn 将返回的 Pod。
	targetPod types.NamespacedName
}

var _ fwk.FilterPlugin = &preQueueingHintPlugin{}
var _ fwk.EnqueueExtensions = &preQueueingHintPlugin{}

func (pl *preQueueingHintPlugin) Name() string {
	return "PreQueueingHintTestPlugin"
}

func (pl *preQueueingHintPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// 拒绝所有 Pod —— 它们需要一个带有特殊标签的节点。
	if nodeInfo.Node().Labels["enable-scheduling"] == "true" {
		return nil
	}
	return fwk.NewStatus(fwk.Unschedulable, "node missing enable-scheduling label")
}

func (pl *preQueueingHintPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel},
			QueueingHintFn: pl.queueingHint,
			PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
				// 只返回目标 Pod。
				return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{pl.targetPod}}, nil
			},
		},
	}, nil
}

func (pl *preQueueingHintPlugin) queueingHint(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	pl.queueingHintCalls.Add(1)
	return fwk.Queue, nil
}

// TestPreQueueingHintNarrowsPodSet 验证当注册了 PreQueueingHintFn 且 feature gate 启用时，
// 在匹配事件发生时只会调用目标 Pod 的 QueueingHintFn。
func TestPreQueueingHintNarrowsPodSet(t *testing.T) {
	tests := []struct {
		name           string
		featureEnabled bool
		// 有 3 个不可调度的 Pod 且 PreQueueingHint 仅指向 pod-1 时，
		// 预期 QueueingHintFn 被调用 1 次（启用）或 3 次（禁用）。
		wantHintCalls int64
	}{
		{
			name:           "feature enabled: only targeted pod evaluated",
			featureEnabled: true,
			wantHintCalls:  1,
		},
		{
			name:           "feature disabled: all pods evaluated",
			featureEnabled: false,
			wantHintCalls:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPreQueueingHints, tt.featureEnabled)

			testCtx := testutils.InitTestAPIServer(t, "prequeueinghint", nil)

			targetPod := types.NamespacedName{Name: "pod-1", Namespace: testCtx.NS.Name}
			plugin := &preQueueingHintPlugin{targetPod: targetPod}

			registry := frameworkruntime.Registry{
				plugin.Name(): func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
					return plugin, nil
				},
			}

			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{{
					SchedulerName: ptr.To[string](v1.DefaultSchedulerName),
					Plugins: &configv1.Plugins{
						Filter: configv1.PluginSet{
							Enabled: []configv1.Plugin{{Name: plugin.Name()}},
						},
					},
				}},
			})

			testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry),
			)
			testutils.SyncSchedulerInformerFactory(testCtx)
			go testCtx.Scheduler.Run(testCtx.SchedulerCtx)

			// 创建一个没有所需标签的节点（Pod 将不可调度）。
			node := st.MakeNode().Name("node-1").Label("kubernetes.io/hostname", "node-1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			// 创建 3 个都不可调度的 Pod。
			for i := 1; i <= 3; i++ {
				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d", i),
						Namespace: testCtx.NS.Name,
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{{
							Name:  "c",
							Image: "pause",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
							},
						}},
					},
				}
				if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			// 等待所有 3 个 Pod 都被尝试调度并变为不可调度。
			if err := testutils.WaitForPodUnschedulableWithTimeout(testCtx.Ctx, testCtx.ClientSet, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: testCtx.NS.Name}}, 30*time.Second); err != nil {
				t.Fatalf("pod-3 not unschedulable: %v", err)
			}

			// 重置计数器。
			plugin.queueingHintCalls.Store(0)

			// 触发节点标签更新事件（匹配插件的 EventsToRegister）。
			nodeCopy := node.DeepCopy()
			nodeCopy.Labels["some-label"] = "some-value"
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, nodeCopy, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			// 等待调度器处理该事件。
			err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
				return plugin.queueingHintCalls.Load() == tt.wantHintCalls, nil
			})
			if err != nil {
				t.Errorf("timed out waiting for QueueingHintFn calls: got %d, want %d", plugin.queueingHintCalls.Load(), tt.wantHintCalls)
			}
		})
	}
}
