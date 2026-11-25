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

package podgroup

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	stepsframework "k8s.io/kubernetes/test/integration/scheduler/podgroup/stepsframework"
	testutils "k8s.io/kubernetes/test/integration/util"
)

const hostnameKey = "kubernetes.io/hostname"

// TestPodGroupSchedulingWithPodAntiAffinity 是针对快照
// AssumePod/ForgetPod bug 的回归测试。在 PodGroup 调度周期中，
// 组内 Pod 会被逐个假定（assume）到调度器快照中，因此快照
// 必须保持其 havePodsWithRequiredAntiAffinityNodeInfoList 一致：
// 否则 InterPodAffinity 插件看不到同一周期中较早假定的 Pod
// 所声明的 required anti-affinity 项，会悄悄地把本应分开的
// Pod 调度到一起。
//
// 反亲和 Pod 会先被创建并入队，因此 PodGroup 调度周期（按入队时间
// 对同优先级 Pod 排序）总是在普通 Pod 之前先假定它；同时它被
// 固定在 node-1 上，以保证分配结果是确定性的。普通 Pod 自身
// 没有声明任何反亲和性，所以只能通过假定反亲和 Pod 时记录在
// 快照中的 existing-pods 反亲和项来避免落到 node-1 上——
// 这正是修复所针对的路径。
//
// 单拓扑场景是确定性的回归信号：没有修复时，普通 Pod 会忽略
// 已被假定的反亲和 Pod，整组 Pod 被静默地调度到唯一的节点上，
// 从而使 "unschedulable" 断言失败。多拓扑场景还额外检查普通
// Pod 是否落在 node-2 上。
func TestPodGroupSchedulingWithPodAntiAffinity(t *testing.T) {
	nodeCapacity := map[v1.ResourceName]string{v1.ResourceCPU: "2"}
	podRequest := map[v1.ResourceName]string{v1.ResourceCPU: "1"}

	// antiAffinityPod 会把其他带有 app=foo 标签的 Pod 从它的
	// hostname 上排开。它会被优先假定（比普通 Pod 先创建和
	// 入队），并固定在 node-1 上以保证分配结果确定。
	antiAffinityPod := st.MakePod().Name("anti-pod").Label("app", "foo").
		Req(podRequest).Container("image").
		NodeSelector(map[string]string{hostnameKey: "node-1"}).
		PodAntiAffinity(hostnameKey, &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}, st.PodAntiAffinityWithRequiredReq).
		PodGroupName("pg").Priority(200).Obj()
	// plainPod 匹配反亲和 Pod 的选择器，但自身没有声明反亲和性，
	// 所以只能通过依赖快照的 existing-pods 反亲和路径来
	// 避免落到 node-1 上。
	plainPod := st.MakePod().Name("plain-pod").Label("app", "foo").
		Req(podRequest).Container("image").
		PodGroupName("pg").Priority(200).Obj()

	workload := st.MakeWorkload().Name("workload").
		PodGroupTemplate(st.MakePodGroupTemplate().Name("t").MinCount(2).Obj()).Obj()
	podGroup := st.MakePodGroup().Name("pg").WorkloadRef("t", "workload").Priority(200).MinCount(2).Obj()

	tests := []struct {
		name  string
		nodes []*v1.Node
		steps []stepsframework.Step
	}{
		{
			name: "anti-affinity honored across the cycle on a multi-topology cluster",
			nodes: []*v1.Node{
				st.MakeNode().Name("node-1").Label(hostnameKey, "node-1").Capacity(nodeCapacity).Obj(),
				st.MakeNode().Name("node-2").Label(hostnameKey, "node-2").Capacity(nodeCapacity).Obj(),
			},
			steps: []stepsframework.Step{
				{
					Name:           "Create the PodGroup object",
					CreatePodGroup: podGroup,
				},
				{
					Name:              "Create both pods belonging to the group",
					CreatePodsInOrder: []*v1.Pod{antiAffinityPod, plainPod},
				},
				{
					Name:                 "Verify both pods are scheduled",
					WaitForPodsScheduled: []string{"anti-pod", "plain-pod"},
				},
				{
					Name: "Verify the anti-affinity pod landed on node-1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"anti-pod"},
						Nodes: sets.New("node-1"),
					},
				},
				{
					Name: "Verify the plain pod was kept off node-1 and landed on node-2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"plain-pod"},
						Nodes: sets.New("node-2"),
					},
				},
			},
		},
		{
			name: "anti-affinity keeps the group unschedulable on a single-topology cluster",
			nodes: []*v1.Node{
				st.MakeNode().Name("node-1").Label(hostnameKey, "node-1").Capacity(nodeCapacity).Obj(),
			},
			steps: []stepsframework.Step{
				{
					Name:           "Create the PodGroup object",
					CreatePodGroup: podGroup,
				},
				{
					Name:              "Create both pods belonging to the group",
					CreatePodsInOrder: []*v1.Pod{antiAffinityPod, plainPod},
				},
				{
					Name:                     "Verify the group becomes unschedulable instead of co-locating",
					WaitForPodsUnschedulable: []string{"anti-pod", "plain-pod"},
				},
				{
					Name: "Verify PodGroup condition is set to Unschedulable",
					WaitForPodGroupCondition: &stepsframework.PodGroupConditionCheck{
						PodGroupName:    "pg",
						ConditionStatus: metav1.ConditionFalse,
						Reason:          schedulingapi.PodGroupReasonUnschedulable,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			testCtx := testutils.InitTestSchedulerWithNS(t, "podgroup-anti-affinity",
				// 禁用 backoff
				scheduler.WithPodMaxBackoffSeconds(0),
				scheduler.WithPodInitialBackoffSeconds(0))
			ns := testCtx.NS.Name

			commonSteps := []stepsframework.Step{
				{
					Name:        "Create Nodes",
					CreateNodes: tt.nodes,
				},
				{
					Name:            "Create workloads",
					CreateWorkloads: []*schedulingapi.Workload{workload},
				},
			}

			if err := stepsframework.RunSteps(testCtx, t, ns, append(commonSteps, tt.steps...)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
