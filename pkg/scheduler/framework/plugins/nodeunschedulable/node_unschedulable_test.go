/*
Copyright 2019 The Kubernetes Authors.

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

package nodeunschedulable

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestNodeUnschedulable 验证 Filter 会拒绝不可调度节点，
// 且带对应 toleration 的 Pod 可以落到该节点上。
func TestNodeUnschedulable(t *testing.T) {
	testCases := []struct {
		name       string
		pod        *v1.Pod
		node       *v1.Node
		wantStatus *fwk.Status
	}{
		{
			name: "Does not schedule pod to unschedulable node (node.Spec.Unschedulable==true)",
			pod:  &v1.Pod{},
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonUnschedulable),
		},
		{
			name: "Schedule pod to normal node",
			pod:  &v1.Pod{},
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: false,
				},
			},
		},
		{
			name: "Schedule pod with toleration to unschedulable node (node.Spec.Unschedulable==true)",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Tolerations: []v1.Toleration{
						{
							Key:    v1.TaintNodeUnschedulable,
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
		},
	}

	for _, test := range testCases {
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(test.node)
		_, ctx := ktesting.NewTestContext(t)
		p, err := New(ctx, nil, nil, feature.Features{})
		if err != nil {
			t.Fatalf("creating plugin: %v", err)
		}
		gotStatus := p.(fwk.FilterPlugin).Filter(ctx, nil, test.pod, nodeInfo)
		if diff := cmp.Diff(test.wantStatus, gotStatus); diff != "" {
			t.Errorf("status does not match (-want,+got):\n%s", diff)
		}
	}
}

// TestIsSchedulableAfterNodeChange 验证节点变更的 QueueingHint：
// 只有 Unschedulable 字段发生有利变化时才重新入队，无关污点变化跳过。
func TestIsSchedulableAfterNodeChange(t *testing.T) {
	testCases := []struct {
		name           string
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
		expectedErr    bool
	}{
		{
			name:         "error-wrong-new-object",
			pod:          &v1.Pod{},
			newObj:       "not-a-node",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		{
			name: "error-wrong-old-object",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
			oldObj:       "not-a-node",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		{
			name: "skip-queue-on-unschedulable-node-added",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "queue-on-schedulable-node-added",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: false,
				},
			},
			expectedHint: fwk.Queue,
		},
		{
			name: "skip-unrelated-change-unschedulable-true",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
					Taints: []v1.Taint{
						{
							Key:    v1.TaintNodeNotReady,
							Effect: v1.TaintEffectNoExecute,
						},
					},
				},
			},
			oldObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "skip-unrelated-change-unschedulable-false",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: false,
					Taints: []v1.Taint{
						{
							Key:    v1.TaintNodeNotReady,
							Effect: v1.TaintEffectNoExecute,
						},
					},
				},
			},
			oldObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: false,
				},
			},
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "queue-on-unschedulable-field-change",
			pod:  &v1.Pod{},
			newObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: false,
				},
			},
			oldObj: &v1.Node{
				Spec: v1.NodeSpec{
					Unschedulable: true,
				},
			},
			expectedHint: fwk.Queue,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			pl := &NodeUnschedulable{}
			got, err := pl.isSchedulableAfterNodeChange(logger, testCase.pod, testCase.oldObj, testCase.newObj)
			if err != nil && !testCase.expectedErr {
				t.Errorf("unexpected error: %v", err)
			}
			if got != testCase.expectedHint {
				t.Errorf("isSchedulableAfterNodeChange() = %v, want %v", got, testCase.expectedHint)
			}
		})
	}
}

// TestIsSchedulableAfterPodTolerationChange 验证目标 Pod 新增 toleration 时的 QueueingHint：
// 只有新增能容忍不可调度污点的 toleration 才值得重新入队。
func TestIsSchedulableAfterPodTolerationChange(t *testing.T) {
	testCases := []struct {
		name           string
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
		expectedErr    bool
	}{
		{
			name:         "error-wrong-new-object",
			pod:          &v1.Pod{},
			newObj:       "not-a-pod",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		{
			name:         "error-wrong-old-object",
			pod:          &v1.Pod{},
			newObj:       &v1.Pod{},
			oldObj:       "not-a-pod",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		{
			name:         "queue-when-the-unsched-pod-gets-toleration",
			pod:          st.MakePod().UID("uid").Obj(),
			newObj:       st.MakePod().UID("uid").Toleration(v1.TaintNodeUnschedulable).Obj(),
			oldObj:       st.MakePod().UID("uid").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "skip-when-the-unsched-pod-gets-unrelated-toleration",
			pod:          st.MakePod().UID("uid").Obj(),
			newObj:       st.MakePod().UID("uid").Toleration("unrelated-key").Obj(),
			oldObj:       st.MakePod().UID("uid").Obj(),
			expectedHint: fwk.QueueSkip,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			pl := &NodeUnschedulable{}
			got, err := pl.isSchedulableAfterTargetPodTolerationChange(logger, testCase.pod, testCase.oldObj, testCase.newObj)
			if err != nil && !testCase.expectedErr {
				t.Errorf("unexpected error: %v", err)
			}
			if got != testCase.expectedHint {
				t.Errorf("isSchedulableAfterNodeChange() = %v, want %v", got, testCase.expectedHint)
			}
		})
	}
}

// TestNodeUnschedulable_DeferredResizeSkipped 验证原地扩容被推迟时 PreFilter 跳过、
// Filter 放行，把处理交给抢占流程。
func TestNodeUnschedulable_DeferredResizeSkipped(t *testing.T) {
	ctx := context.Background()
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	pl := &NodeUnschedulable{enableInPlacePodVerticalScalingSchedulerPreemption: true}

	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}
}
