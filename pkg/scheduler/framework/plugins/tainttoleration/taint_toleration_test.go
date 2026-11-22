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

package tainttoleration

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// nodeWithTaints 构造一个仅带名称与污点列表的节点，省去测试中无关字段的铺垫。
func nodeWithTaints(nodeName string, taints []v1.Taint) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Spec: v1.NodeSpec{
			Taints: taints,
		},
	}
}

// podWithTolerations 构造一个仅带名称与容忍列表的 Pod，与 nodeWithTaints 配套使用。
func podWithTolerations(podName string, tolerations []v1.Toleration) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Tolerations: tolerations,
		},
	}
}

// TestTaintTolerationScore 验证打分阶段：对 PreferNoSchedule 污点，容忍污点的节点应得高分，
// 不可容忍污点数量越多得分越低，且分数与可容忍污点数量无关。
func TestTaintTolerationScore(t *testing.T) {
	tests := []struct {
		name                               string
		pod                                *v1.Pod
		nodes                              []*v1.Node
		expectedList                       fwk.NodeScoreList
		enableTaintTolerationComparisonOps bool
	}{
		// 基础场景：同一污点，能容忍的节点得分高于不能容忍的节点。
		{
			name: "node with taints tolerated by the pod, gets a higher score than those node with intolerable taints",
			pod: podWithTolerations("pod1", []v1.Toleration{{
				Key:      "foo",
				Operator: v1.TolerationOpEqual,
				Value:    "bar",
				Effect:   v1.TaintEffectPreferNoSchedule,
			}}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{{
					Key:    "foo",
					Value:  "bar",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
				nodeWithTaints("nodeB", []v1.Taint{{
					Key:    "foo",
					Value:  "blah",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: fwk.MaxNodeScore},
				{Name: "nodeB", Score: 0},
			},
		},
		// 只要污点都被容忍，节点得满分；可容忍污点的数量不影响得分。
		{
			name: "the nodes that all of their taints are tolerated by the pod, get the same score, no matter how many tolerable taints a node has",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{
					Key:      "cpu-type",
					Operator: v1.TolerationOpEqual,
					Value:    "arm64",
					Effect:   v1.TaintEffectPreferNoSchedule,
				}, {
					Key:      "disk-type",
					Operator: v1.TolerationOpEqual,
					Value:    "ssd",
					Effect:   v1.TaintEffectPreferNoSchedule,
				},
			}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{}),
				nodeWithTaints("nodeB", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
				nodeWithTaints("nodeC", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					}, {
						Key:    "disk-type",
						Value:  "ssd",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: fwk.MaxNodeScore},
				{Name: "nodeB", Score: fwk.MaxNodeScore},
				{Name: "nodeC", Score: fwk.MaxNodeScore},
			},
		},
		// 与可容忍污点相反，不可容忍污点的数量会影响得分。
		{
			name: "the more intolerable taints a node has, the lower score it gets.",
			pod: podWithTolerations("pod1", []v1.Toleration{{
				Key:      "foo",
				Operator: v1.TolerationOpEqual,
				Value:    "bar",
				Effect:   v1.TaintEffectPreferNoSchedule,
			}}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{}),
				nodeWithTaints("nodeB", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
				nodeWithTaints("nodeC", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					}, {
						Key:    "disk-type",
						Value:  "ssd",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: fwk.MaxNodeScore},
				{Name: "nodeB", Score: 50},
				{Name: "nodeC", Score: 0},
			},
		},
		// 打分插件只关注 PreferNoSchedule 类型的污点与容忍；其他 effect 不参与打分。
		{
			name: "only taints and tolerations that have effect PreferNoSchedule are checked by taints-tolerations priority function",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{
					Key:      "cpu-type",
					Operator: v1.TolerationOpEqual,
					Value:    "arm64",
					Effect:   v1.TaintEffectNoSchedule,
				}, {
					Key:      "disk-type",
					Operator: v1.TolerationOpEqual,
					Value:    "ssd",
					Effect:   v1.TaintEffectNoSchedule,
				},
			}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{}),
				nodeWithTaints("nodeB", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectNoSchedule,
					},
				}),
				nodeWithTaints("nodeC", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					}, {
						Key:    "disk-type",
						Value:  "ssd",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: fwk.MaxNodeScore},
				{Name: "nodeB", Score: fwk.MaxNodeScore},
				{Name: "nodeC", Score: 0},
			},
		},
		{
			name: "Default behaviour No taints and tolerations, lands on node with no taints",
			// Pod 不带任何容忍。
			pod: podWithTolerations("pod1", []v1.Toleration{}),
			nodes: []*v1.Node{
				// 节点不带任何污点。
				nodeWithTaints("nodeA", []v1.Taint{}),
				nodeWithTaints("nodeB", []v1.Taint{
					{
						Key:    "cpu-type",
						Value:  "arm64",
						Effect: v1.TaintEffectPreferNoSchedule,
					},
				}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: fwk.MaxNodeScore},
				{Name: "nodeB", Score: 0},
			},
		},
		{
			name: "Numeric Gt operator: pod with Gt toleration prefers nodes with matching SLA taint",
			pod: podWithTolerations("pod1", []v1.Toleration{{
				Key:      "node.kubernetes.io/sla",
				Operator: "Gt",
				Value:    "950",
				Effect:   v1.TaintEffectPreferNoSchedule,
			}}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{{
					Key:    "node.kubernetes.io/sla",
					Value:  "800",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
				nodeWithTaints("nodeB", []v1.Taint{{
					Key:    "node.kubernetes.io/sla",
					Value:  "999",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: 0},
				{Name: "nodeB", Score: fwk.MaxNodeScore},
			},
			enableTaintTolerationComparisonOps: true,
		},
		{
			name: "Numeric Lt operator: pod with Lt toleration prefers nodes with matching SLA taint",
			pod: podWithTolerations("pod1", []v1.Toleration{{
				Key:      "node.kubernetes.io/sla",
				Operator: "Lt",
				Value:    "800",
				Effect:   v1.TaintEffectPreferNoSchedule,
			}}),
			nodes: []*v1.Node{
				nodeWithTaints("nodeA", []v1.Taint{{
					Key:    "node.kubernetes.io/sla",
					Value:  "950",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
				nodeWithTaints("nodeB", []v1.Taint{{
					Key:    "node.kubernetes.io/sla",
					Value:  "700",
					Effect: v1.TaintEffectPreferNoSchedule,
				}}),
			},
			expectedList: []fwk.NodeScore{
				{Name: "nodeA", Score: 0},
				{Name: "nodeB", Score: fwk.MaxNodeScore},
			},
			enableTaintTolerationComparisonOps: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 测试上下文携带 klog logger，取消上下文可让插件启动的 goroutine 在用例结束后退出。
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			state := framework.NewCycleState()
			// 打分只依赖快照中的节点列表，不需要真实缓存，故用空 Pod 列表构造快照。
			snapshot := cache.NewSnapshot(nil, test.nodes)
			fh, _ := runtime.NewFramework(ctx, nil, nil, runtime.WithSnapshotSharedLister(snapshot))
			p, err := New(ctx, nil, fh, feature.Features{
				EnableTaintTolerationComparisonOperators: test.enableTaintTolerationComparisonOps,
			})

			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			nodeInfos := tf.BuildNodeInfos(test.nodes)
			// 先走 PreScore，确保插件在打分前已准备好共享状态（如最大不可容忍污点数）。
			status := p.(fwk.PreScorePlugin).PreScore(ctx, state, test.pod, nodeInfos)
			if !status.IsSuccess() {
				t.Errorf("unexpected error: %v", status)
			}
			var gotList fwk.NodeScoreList
			for _, nodeInfo := range nodeInfos {
				nodeName := nodeInfo.Node().Name
				score, status := p.(fwk.ScorePlugin).Score(ctx, state, test.pod, nodeInfo)
				if !status.IsSuccess() {
					t.Errorf("unexpected error: %v", status)
				}
				gotList = append(gotList, fwk.NodeScore{Name: nodeName, Score: score})
			}

			// 打分结果需经归一化后才落到 0..MaxNodeScore 区间，与实际调度链路保持一致。
			status = p.(fwk.ScorePlugin).ScoreExtensions().NormalizeScore(ctx, state, test.pod, gotList)
			if !status.IsSuccess() {
				t.Errorf("unexpected error: %v", status)
			}

			if diff := cmp.Diff(test.expectedList, gotList); diff != "" {
				t.Errorf("Unexpected NodeScoreList (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestTaintTolerationFilter 验证过滤阶段：NoSchedule 污点会挡住无对应容忍的 Pod，
// PreferNoSchedule 污点则不会；同时覆盖 Gt/Lt 数值比较操作符的行为。
func TestTaintTolerationFilter(t *testing.T) {
	tests := []struct {
		name                               string
		pod                                *v1.Pod
		node                               *v1.Node
		wantStatus                         *fwk.Status
		enableTaintTolerationComparisonOps bool
	}{
		{
			name:       "A pod having no tolerations can't be scheduled onto a node with nonempty taints",
			pod:        podWithTolerations("pod1", []v1.Toleration{}),
			node:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
		{
			name: "A pod which can be scheduled on a dedicated node assigned to user1 with effect NoSchedule",
			pod:  podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
		},
		{
			name:       "A pod which can't be scheduled on a dedicated node assigned to user2 with effect NoSchedule",
			pod:        podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			node:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
		{
			name: "A pod can be scheduled onto the node, with a toleration uses operator Exists that tolerates the taints on the node",
			pod:  podWithTolerations("pod1", []v1.Toleration{{Key: "foo", Operator: "Exists", Effect: "NoSchedule"}}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "foo", Value: "bar", Effect: "NoSchedule"}}),
		},
		{
			name: "A pod has multiple tolerations, node has multiple taints, all the taints are tolerated, pod can be scheduled onto the node",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"},
				{Key: "foo", Operator: "Exists", Effect: "NoSchedule"},
			}),
			node: nodeWithTaints("nodeA", []v1.Taint{
				{Key: "dedicated", Value: "user2", Effect: "NoSchedule"},
				{Key: "foo", Value: "bar", Effect: "NoSchedule"},
			}),
		},
		{
			name: "A pod has a toleration that keys and values match the taint on the node, but (non-empty) effect doesn't match, " +
				"can't be scheduled onto the node",
			pod:        podWithTolerations("pod1", []v1.Toleration{{Key: "foo", Operator: "Equal", Value: "bar", Effect: "PreferNoSchedule"}}),
			node:       nodeWithTaints("nodeA", []v1.Taint{{Key: "foo", Value: "bar", Effect: "NoSchedule"}}),
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
		{
			name: "The pod has a toleration that keys and values match the taint on the node, the effect of toleration is empty, " +
				"and the effect of taint is NoSchedule. Pod can be scheduled onto the node",
			pod:  podWithTolerations("pod1", []v1.Toleration{{Key: "foo", Operator: "Equal", Value: "bar"}}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "foo", Value: "bar", Effect: "NoSchedule"}}),
		},
		{
			name: "The pod has a toleration that key and value don't match the taint on the node, " +
				"but the effect of taint on node is PreferNoSchedule. Pod can be scheduled onto the node",
			pod:  podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "PreferNoSchedule"}}),
		},
		{
			name: "The pod has no toleration, " +
				"but the effect of taint on node is PreferNoSchedule. Pod can be scheduled onto the node",
			pod:  podWithTolerations("pod1", []v1.Toleration{}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "PreferNoSchedule"}}),
		},
		{
			name:                               "Pod with Gt toleration cannot be scheduled on node when taint value is lower than threshold",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.example.com/priority-level", Operator: "Gt", Value: "950", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.example.com/priority-level", Value: "800", Effect: "NoSchedule"}}),
			wantStatus:                         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
			enableTaintTolerationComparisonOps: true,
		},
		{
			name:                               "Pod with Gt toleration can be scheduled on node when taint value is higher than threshold",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "750", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "950", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: true,
		},
		{
			name:                               "Pod with Lt toleration cannot be scheduled on node when taint value is higher than threshold",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.example.com/priority-level", Operator: "Lt", Value: "800", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.example.com/priority-level", Value: "950", Effect: "NoSchedule"}}),
			wantStatus:                         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
			enableTaintTolerationComparisonOps: true,
		},
		{
			name:                               "Pod with Lt toleration can be scheduled on node when taint value is lower than threshold",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.kubernetes.io/sla", Operator: "Lt", Value: "950", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "800", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: true,
		},
		{
			name: "Pod with Gt toleration cannot be scheduled on node with non-numeric taint value",
			pod:  podWithTolerations("pod1", []v1.Toleration{{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "950", Effect: "NoSchedule"}}),
			node: nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "high", Effect: "NoSchedule"}}),
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				"node(s) had untolerated taint(s)"),
			enableTaintTolerationComparisonOps: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(test.node)
			p, err := New(ctx, nil, nil, feature.Features{
				EnableTaintTolerationComparisonOperators: test.enableTaintTolerationComparisonOps,
			})
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			gotStatus := p.(fwk.FilterPlugin).Filter(ctx, nil, test.pod, nodeInfo)
			if diff := cmp.Diff(test.wantStatus, gotStatus); diff != "" {
				t.Errorf("Unexpected status (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestTaintTolerationFilterWithFeatureGate 验证 EnableTaintTolerationComparisonOperators 开关：
// 关闭时 Gt/Lt 容忍被忽略，只有 Equal/Exists 生效。
func TestTaintTolerationFilterWithFeatureGate(t *testing.T) {
	tests := []struct {
		name                               string
		pod                                *v1.Pod
		node                               *v1.Node
		enableTaintTolerationComparisonOps bool
		wantStatus                         *fwk.Status
	}{
		{
			name:                               "Pod with Gt toleration can be scheduled when feature gate is enabled and taint value is higher",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "750", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "950", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: true,
			wantStatus:                         nil,
		},
		{
			name: "Pod with Gt toleration cannot be scheduled when feature gate is disabled",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "750", Effect: "NoSchedule"},
			}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "950", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: false,
			wantStatus:                         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
		{
			name:                               "Pod with Lt toleration can be scheduled when feature gate is enabled and taint value is lower",
			pod:                                podWithTolerations("pod1", []v1.Toleration{{Key: "node.kubernetes.io/sla", Operator: "Lt", Value: "950", Effect: "NoSchedule"}}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "800", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: true,
			wantStatus:                         nil,
		},
		{
			name: "Pod with Lt toleration cannot be scheduled when feature gate is disabled",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{Key: "node.kubernetes.io/sla", Operator: "Lt", Value: "950", Effect: "NoSchedule"},
			}),
			node:                               nodeWithTaints("nodeA", []v1.Taint{{Key: "node.kubernetes.io/sla", Value: "800", Effect: "NoSchedule"}}),
			enableTaintTolerationComparisonOps: false,
			wantStatus:                         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
		{
			name: "Pod with mixed tolerations (Equal and Gt) when feature gate is disabled - only Equal is honored",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{Key: "dedicated", Operator: "Equal", Value: "user1", Effect: "NoSchedule"},
				{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "750", Effect: "NoSchedule"},
			}),
			node: nodeWithTaints("nodeA", []v1.Taint{
				{Key: "dedicated", Value: "user1", Effect: "NoSchedule"},
			}),
			enableTaintTolerationComparisonOps: false,
			wantStatus:                         nil,
		},
		{
			name: "Pod with mixed tolerations, Gt toleration needed but filtered out when feature gate is disabled",
			pod: podWithTolerations("pod1", []v1.Toleration{
				{Key: "dedicated", Operator: "Equal", Value: "user1", Effect: "NoSchedule"},
				{Key: "node.kubernetes.io/sla", Operator: "Gt", Value: "750", Effect: "NoSchedule"},
			}),
			node: nodeWithTaints("nodeA", []v1.Taint{
				{Key: "dedicated", Value: "user1", Effect: "NoSchedule"},
				{Key: "node.kubernetes.io/sla", Value: "950", Effect: "NoSchedule"},
			}),
			enableTaintTolerationComparisonOps: false,
			wantStatus:                         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(test.node)
			p, err := New(ctx, nil, nil, feature.Features{
				EnableTaintTolerationComparisonOperators: test.enableTaintTolerationComparisonOps,
			})
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			gotStatus := p.(fwk.FilterPlugin).Filter(ctx, nil, test.pod, nodeInfo)
			if diff := cmp.Diff(test.wantStatus, gotStatus); diff != "" {
				t.Errorf("Unexpected status (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestIsSchedulableAfterNodeChange 验证节点变更事件的重排队提示：
// 只有污点变化让节点从不可调度变为可调度时才提示 Queue，无关变更则 QueueSkip，
// 传入非 Node 对象时返回错误并保守地提示 Queue。
func TestIsSchedulableAfterNodeChange(t *testing.T) {
	tests := []struct {
		name         string
		pod          *v1.Pod
		oldObj       interface{}
		newObj       interface{}
		expectedHint fwk.QueueingHint
		wantErr      bool
	}{
		{
			name:         "backoff-wrong-new-object",
			newObj:       "not-a-node",
			expectedHint: fwk.Queue,
			wantErr:      true,
		},
		{
			name:         "backoff-wrong-old-object",
			newObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			oldObj:       "not-a-node",
			expectedHint: fwk.Queue,
			wantErr:      true,
		},
		{
			name:         "skip-queue-on-untoleratedtaint-node-added",
			pod:          podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			newObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "queue-on-toleratedtaint-node-added",
			pod:          podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			newObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user2", Effect: "NoSchedule"}}),
			expectedHint: fwk.Queue,
		},
		{
			name:         "skip-unrelated-change",
			pod:          podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			newObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}, {Key: "dedicated", Value: "user3", Effect: "NoSchedule"}}),
			oldObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "queue-on-taint-change",
			pod:          podWithTolerations("pod1", []v1.Toleration{{Key: "dedicated", Operator: "Equal", Value: "user2", Effect: "NoSchedule"}}),
			newObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user2", Effect: "NoSchedule"}}),
			oldObj:       nodeWithTaints("nodeA", []v1.Taint{{Key: "dedicated", Value: "user1", Effect: "NoSchedule"}}),
			expectedHint: fwk.Queue,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			pl := &TaintToleration{}
			got, err := pl.isSchedulableAfterNodeChange(logger, test.pod, test.oldObj, test.newObj)
			if (err != nil) != test.wantErr {
				t.Errorf("isSchedulableAfterNodeChange() error = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.expectedHint {
				t.Errorf("isSchedulableAfterNodeChange() = %v, want %v", got, test.expectedHint)
			}
		})
	}
}

// Test_isSchedulableAfterTargetPodTolerationChange 验证目标 Pod 容忍变更事件：
// 新增容忍可能让此前被污点挡住的 Pod 变为可调度，因此提示 Queue。
func Test_isSchedulableAfterTargetPodTolerationChange(t *testing.T) {
	testcases := map[string]struct {
		pod            *v1.Pod
		oldObj, newObj interface{}
		expectedHint   fwk.QueueingHint
	}{
		"queue-on-toleration-added": {
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "ns-1",
				}},
			oldObj: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "ns-1",
				}},
			newObj: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "ns-1",
				},
				Spec: v1.PodSpec{
					Tolerations: []v1.Toleration{
						{
							Key:    "foo",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedHint: fwk.Queue,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			p, err := New(ctx, nil, nil, feature.Features{})
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			actualHint, err := p.(*TaintToleration).isSchedulableAfterTargetPodTolerationChange(logger, tc.pod, tc.oldObj, tc.newObj)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("Unexpected hint (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestTaintToleration_DeferredResizeSkipped 验证延迟原地扩缩容中的 Pod 会被 PreFilter 跳过，
// 但 Filter 仍按普通 Pod 处理并返回 Success。
func TestTaintToleration_DeferredResizeSkipped(t *testing.T) {
	ctx := context.Background()
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	pl := &TaintToleration{enableInPlacePodVerticalScalingSchedulerPreemption: true}

	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}
}
