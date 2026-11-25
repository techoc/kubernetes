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

package podgroup

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	stepsframework "k8s.io/kubernetes/test/integration/scheduler/podgroup/stepsframework"
	testutils "k8s.io/kubernetes/test/integration/util"
)

// _ 用于避免未使用的 import
var _ = time.Second

func makeTestPods(pgName string, reqCPUs ...string) []*v1.Pod {
	var pods []*v1.Pod
	for i, cpu := range reqCPUs {
		pod := st.MakePod().Name(fmt.Sprintf("%s-pod-%d", pgName, i)).
			PodGroupName(pgName).Priority(100).Obj()

		pod.Spec.Containers = []v1.Container{{
			Name:  "container",
			Image: "image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse(cpu),
				},
			},
		}}

		pods = append(pods, pod)
	}
	return pods
}

func concatPods(podSlices ...[]*v1.Pod) []*v1.Pod {
	var pods []*v1.Pod
	for _, slice := range podSlices {
		pods = append(pods, slice...)
	}
	return pods
}

func podNames(pods []*v1.Pod) []string {
	var names []string
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

func TestCPGScheduling(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.CompositePodGroup:               true,
		features.GenericWorkload:                 true,
		features.TopologyAwareWorkloadScheduling: true,
	})

	tests := []struct {
		name  string
		steps []stepsframework.Step
	}{
		{
			name: "TestCPGHierarchicalScheduling",
			// TestCPGHierarchicalScheduling 验证在混合 Gang 和 Basic 策略的三层 CPG
			// 层级结构中，就绪性和可行性能够正确传播。
			//
			// 树形结构：
			//
			//	                    cpg-root (Gang, Min: 2)
			//	            /                 |                 \
			//	   cpg-sub1 (Gang, Min:2)  cpg-sub2 (Basic)    cpg-sub3 (Gang, Min:2)
			//	   /    |    \              /       \             /         \
			//	 pg1   pg2   pg3          pg4       pg5         pg6        pg7
			//	(S)   (S)   (F)          (S)       (S)         (F)        (F)
			//
			// [Gang] [Gang] [Gang]     [Basic]   [Gang]      [Gang]     [Gang]
			//
			// (S) = 成功（Pod 按策略完成调度）
			// (F) = 失败（Pod 无法调度）
			// 注意：pg6 失败是因为其父节点 cpg-sub3 失败（需要 2 个子节点，但 pg7 失败）。
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").MinGroupCount(2).Priority(100).Children(
									st.MakeCompositePodGroupTemplate().Name("sub1-3-t").MinGroupCount(2).Priority(100).Children(
										st.MakePodGroupTemplate().Name("gang-t1").MinCount(3).Priority(100),
									),
									st.MakeCompositePodGroupTemplate().Name("sub2-t").BasicPolicy().Priority(100).Children(
										st.MakePodGroupTemplate().Name("basic-t").BasicPolicy().Priority(100),
										st.MakePodGroupTemplate().Name("gang-t2").MinCount(3).Priority(100),
									),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg", "root-t").MinGroupCount(2).Priority(100).Obj(),
				},
				{
					Name:                    "Create cpg-sub1",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-sub1").WorkloadRef("workload-cpg", "sub1-3-t").MinGroupCount(2).ParentCompositePodGroup("cpg-root").Priority(100).Obj(),
				},
				{
					Name:                    "Create cpg-sub2",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-sub2").WorkloadRef("workload-cpg", "sub2-t").BasicPolicy().ParentCompositePodGroup("cpg-root").Priority(100).Obj(),
				},
				{
					Name:                    "Create cpg-sub3",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-sub3").WorkloadRef("workload-cpg", "sub1-3-t").MinGroupCount(2).ParentCompositePodGroup("cpg-root").Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg", "gang-t1").ParentCompositePodGroup("cpg-sub1").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg", "gang-t1").ParentCompositePodGroup("cpg-sub1").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg3",
					CreatePodGroup: st.MakePodGroup().Name("pg3").WorkloadRef("workload-cpg", "gang-t1").ParentCompositePodGroup("cpg-sub1").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg4",
					CreatePodGroup: st.MakePodGroup().Name("pg4").WorkloadRef("workload-cpg", "basic-t").ParentCompositePodGroup("cpg-sub2").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:           "Create pg5",
					CreatePodGroup: st.MakePodGroup().Name("pg5").WorkloadRef("workload-cpg", "gang-t2").ParentCompositePodGroup("cpg-sub2").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg6",
					CreatePodGroup: st.MakePodGroup().Name("pg6").WorkloadRef("workload-cpg", "gang-t1").ParentCompositePodGroup("cpg-sub3").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg7",
					CreatePodGroup: st.MakePodGroup().Name("pg7").WorkloadRef("workload-cpg", "gang-t1").ParentCompositePodGroup("cpg-sub3").Priority(100).MinCount(3).Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
						makeTestPods("pg3", "100", "100", "100"), // (F) 不可调度
						makeTestPods("pg4", "1", "1", "1"),
						makeTestPods("pg5", "1", "1", "1"),
						makeTestPods("pg6", "1", "1", "1"),
						makeTestPods("pg7", "100", "100", "100"), // (F) 不可调度
					),
				},
				{
					Name: "Wait for successful pods",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
						makeTestPods("pg4", "1", "1", "1"),
						makeTestPods("pg5", "1", "1", "1"),
					)),
				},
				{
					Name: "Verify failing pods remain unschedulable",
					WaitForPodsUnschedulable: podNames(concatPods(
						makeTestPods("pg3", "1", "1", "1"),
						makeTestPods("pg6", "1", "1", "1"),
						makeTestPods("pg7", "1", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGMinGroupCount",
			// TestCPGMinGroupCount 验证当 Gang CompositePodGroup 至少有 MinGroupCount 个
			// 子节点完全可调度时，它会调度这些子节点。
			//
			// 树形结构：
			//
			//	   cpg-root (Gang, MinGroup: 2)
			//	  /             |              \
			//	pg1            pg2            pg3
			//	(S)            (S)            (F)
			//
			// (S) = 成功
			// (F) = 失败
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-min").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").MinGroupCount(2).Priority(100).Children(
									st.MakePodGroupTemplate().Name("gang-t").MinCount(3).Priority(100),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-min", "root-t").MinGroupCount(2).Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-min", "gang-t").ParentCompositePodGroup("cpg-root").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-min", "gang-t").ParentCompositePodGroup("cpg-root").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg3",
					CreatePodGroup: st.MakePodGroup().Name("pg3").WorkloadRef("workload-cpg-min", "gang-t").ParentCompositePodGroup("cpg-root").Priority(100).MinCount(3).Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
						makeTestPods("pg3", "100", "1", "1"),
					),
				},
				{
					Name: "Wait for successful pods",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
					)),
				},
				{
					Name: "Verify failing pods remain unschedulable",
					WaitForPodsUnschedulable: podNames(concatPods(
						makeTestPods("pg3", "1", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGBasicWithGangChildren",
			// TestCPGBasicWithGangChildren 验证 Basic CompositePodGroup 允许其已就绪的
			// Gang 子组彼此独立地调度。
			//
			// 树形结构：
			//
			//	   cpg-root (Basic)
			//	  /            \
			//	pg1            pg2
			//	(S)            (F)
			//
			// (S) = 成功
			// (F) = 失败
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-basic").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").BasicPolicy().Priority(100).Children(
									st.MakePodGroupTemplate().Name("gang-t").MinCount(3).Priority(100),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-basic", "root-t").BasicPolicy().Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-basic", "gang-t").ParentCompositePodGroup("cpg-root").Priority(100).MinCount(3).Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-basic", "gang-t").ParentCompositePodGroup("cpg-root").Priority(100).MinCount(3).Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "100", "1", "1"),
					),
				},
				{
					Name: "Wait for successful pods",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("pg1", "1", "1", "1"),
					)),
				},
				{
					Name: "Verify failing pods remain unschedulable",
					WaitForPodsUnschedulable: podNames(concatPods(
						makeTestPods("pg2", "1", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGBasicWithBasicChildren",
			// TestCPGBasicWithBasicChildren 验证带有 Basic 子 PodGroup 的 Basic
			// CompositePodGroup 能够成功调度所有 Pod，因为不存在 gang 约束。
			//
			// 树形结构：
			//
			//	   cpg-root (Basic)
			//	  /            \
			//	pg1            pg2
			//	(S)            (S)
			//
			// (S) = 成功
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-basic-basic").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").BasicPolicy().Priority(100).Children(
									st.MakePodGroupTemplate().Name("basic-t").BasicPolicy().Priority(100),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-basic-basic", "root-t").BasicPolicy().Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-basic-basic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-basic-basic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "1", "1"),
						makeTestPods("pg2", "1", "1"),
					),
				},
				{
					Name: "Wait for successful pods",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("pg1", "1", "1"),
						makeTestPods("pg2", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGGangWithBasicChildren",
			// TestCPGGangWithBasicChildren 验证带有 Basic 子节点的 Gang CompositePodGroup
			// 会在根节点强制执行 gang 调度策略（minGroupCount）。
			//
			// 树形结构：
			//
			//	   cpg-root (Gang, Min: 2)
			//	  /          |          \
			//	pg1         pg2         pg3
			//	(S)         (S)         (F)
			//
			// (S) = 成功
			// (F) = 失败
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "8"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-gang-basic").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").MinGroupCount(2).Priority(100).Children(
									st.MakePodGroupTemplate().Name("basic-t").BasicPolicy().Priority(100),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-gang-basic", "root-t").MinGroupCount(2).Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-gang-basic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-gang-basic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:           "Create pg3",
					CreatePodGroup: st.MakePodGroup().Name("pg3").WorkloadRef("workload-cpg-gang-basic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
						makeTestPods("pg3", "10", "10", "10"),
					),
				},
				{
					Name: "Wait for successful pods",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("pg1", "1", "1", "1"),
						makeTestPods("pg2", "1", "1", "1"),
					)),
				},
				{
					Name: "Verify failing pods remain unschedulable",
					WaitForPodsUnschedulable: podNames(concatPods(
						makeTestPods("pg3", "1", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGBasicWithUnschedulableBasicChildren",
			// TestCPGBasicWithUnschedulableBasicChildren 验证当 Basic CompositePodGroup
			// 包含不可调度的 Basic 子 PodGroup 时，这些 Pod 保持未调度状态。
			//
			// 树形结构：
			//
			//	   cpg-root (Basic)
			//	  /            \
			//	pg1            pg2
			//	(F)            (F)
			//
			// (F) = 失败
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-basic-unschedulable").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").BasicPolicy().Priority(100).Children(
									st.MakePodGroupTemplate().Name("basic-t").BasicPolicy().Priority(100),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-basic-unschedulable", "root-t").BasicPolicy().Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-basic-unschedulable", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-basic-unschedulable", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name: "Create Pods",
					CreatePods: concatPods(
						makeTestPods("pg1", "50", "50"),
						makeTestPods("pg2", "50", "50"),
					),
				},
				{
					Name: "Wait for pods to be unschedulable",
					WaitForPodsUnschedulable: podNames(concatPods(
						makeTestPods("pg1", "1", "1"),
						makeTestPods("pg2", "1", "1"),
					)),
				},
			},
		},
		{
			name: "TestCPGDynamicChildAddition",
			// TestCPGDynamicChildAddition 验证 CompositePodGroup（CPG）层级结构成功调度
			// 初始 Pod 之后，动态添加新的子组和子 CPG 时，在调度策略（例如带
			// minGroupCount 的 Gang 调度）下仍能正确工作。
			//
			// 树形结构演进：
			//
			// 阶段 1：
			//
			//	cpg-root (Basic)
			//	   |
			//	  pg1 (Basic)
			//	  (Scheduled)
			//
			// 阶段 2（动态添加 pg2）：
			//
			//	 cpg-root (Basic)
			//	  /      \
			//	pg1      pg2 (Basic)
			//	(S)      (Scheduled)
			//
			// 阶段 3（在 cpg-root 下动态添加 cpg-sub 子树）：
			//
			//	 cpg-root (Basic)
			//	  /   |  \
			//	pg1  pg2  cpg-sub (Gang minGroupCount = 2)
			//	(S)  (S)     |
			//	           sub-pg1 (Basic)
			//	           (Pending - quorum of 2 not met)
			//
			// 阶段 4（在 cpg-sub 下动态添加 sub-pg2）：
			//
			//	 cpg-root (Basic)
			//	  /   |  \
			//	pg1  pg2  cpg-sub (Gang minGroupCount = 2)
			//	(S)  (S)   /     \
			//	        sub-pg1  sub-pg2 (Basic)
			//	        (S)      (S)
			//
			// (S) = 调度成功
			steps: []stepsframework.Step{
				{
					Name:        "Create Node",
					CreateNodes: []*v1.Node{st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "20"}).Obj()},
				},
				{
					Name: "Create Workload",
					CreateWorkloads: []*schedulingapi.Workload{
						st.MakeWorkload().Name("workload-cpg-dynamic").
							Children(
								st.MakeCompositePodGroupTemplate().Name("root-t").BasicPolicy().Priority(100).Children(
									st.MakePodGroupTemplate().Name("basic-t").BasicPolicy().Priority(100),
									st.MakeCompositePodGroupTemplate().Name("sub-cpg-t").MinGroupCount(2).Priority(100).Children(
										st.MakePodGroupTemplate().Name("sub-pg-t").BasicPolicy().Priority(100),
									),
								),
							).Obj(),
					},
				},
				{
					Name:                    "Create root CPG",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-root").WorkloadRef("workload-cpg-dynamic", "root-t").BasicPolicy().Priority(100).Obj(),
				},
				{
					Name:           "Create pg1",
					CreatePodGroup: st.MakePodGroup().Name("pg1").WorkloadRef("workload-cpg-dynamic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:       "Create pg1 pods",
					CreatePods: makeTestPods("pg1", "1", "1"),
				},
				{
					Name:                 "Wait for pg1 pods to be scheduled",
					WaitForPodsScheduled: podNames(makeTestPods("pg1", "1", "1")),
				},
				{
					Name:           "Create pg2",
					CreatePodGroup: st.MakePodGroup().Name("pg2").WorkloadRef("workload-cpg-dynamic", "basic-t").ParentCompositePodGroup("cpg-root").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:       "Create pg2 pods",
					CreatePods: makeTestPods("pg2", "1", "1"),
				},
				{
					Name:                 "Wait for pg2 pods to be scheduled",
					WaitForPodsScheduled: podNames(makeTestPods("pg2", "1", "1")),
				},
				{
					Name:                    "Create cpg-sub",
					CreateCompositePodGroup: st.MakeCompositePodGroup().Name("cpg-sub").WorkloadRef("workload-cpg-dynamic", "sub-cpg-t").MinGroupCount(2).ParentCompositePodGroup("cpg-root").Priority(100).Obj(),
				},
				{
					Name:           "Create sub-pg1",
					CreatePodGroup: st.MakePodGroup().Name("sub-pg1").WorkloadRef("workload-cpg-dynamic", "sub-pg-t").ParentCompositePodGroup("cpg-sub").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:       "Create sub-pg1 pods",
					CreatePods: makeTestPods("sub-pg1", "1", "1"),
				},
				{
					Name:                     "Wait for sub-pg1 pods to be unschedulable (quorum not met)",
					WaitForPodsUnschedulable: podNames(makeTestPods("sub-pg1", "1", "1")),
				},
				{
					Name:           "Create sub-pg2",
					CreatePodGroup: st.MakePodGroup().Name("sub-pg2").WorkloadRef("workload-cpg-dynamic", "sub-pg-t").ParentCompositePodGroup("cpg-sub").Priority(100).BasicPolicy().Obj(),
				},
				{
					Name:       "Create sub-pg2 pods",
					CreatePods: makeTestPods("sub-pg2", "1", "1"),
				},
				{
					Name: "Wait for both sub-pg1 and sub-pg2 pods to be scheduled",
					WaitForPodsScheduled: podNames(concatPods(
						makeTestPods("sub-pg1", "1", "1"),
						makeTestPods("sub-pg2", "1", "1"),
					)),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCtx := testutils.InitTestSchedulerWithNS(t, "cpg-sched",
				scheduler.WithPodMaxBackoffSeconds(1),
				scheduler.WithPodInitialBackoffSeconds(1))

			if err := stepsframework.RunSteps(testCtx, t, testCtx.NS.Name, tt.steps); err != nil {
				t.Errorf("Test failed: %v", err)
			}
		})
	}
}

func TestPodGroupDependentRequiredValidation(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.CompositePodGroup:               true,
		features.GenericWorkload:                 true,
		features.TopologyAwareWorkloadScheduling: true,
	})

	testCtx := testutils.InitTestSchedulerWithNS(t, "pg-validation",
		scheduler.WithPodMaxBackoffSeconds(1),
		scheduler.WithPodInitialBackoffSeconds(1))
	cs, ns := testCtx.ClientSet, testCtx.NS.Name

	// 场景 1：设置了 ParentCompositePodGroupName 且设置了 WorkloadRef -> 应当通过
	pgPass := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-pass", Namespace: ns},
		Spec: schedulingapi.PodGroupSpec{
			SchedulingPolicy: schedulingapi.PodGroupSchedulingPolicy{
				Basic: &schedulingapi.BasicSchedulingPolicy{},
			},
			ParentCompositePodGroupName: new("some-cpg"),
			WorkloadRef: &schedulingapi.WorkloadReference{
				WorkloadName: "some-workload",
				TemplateName: "some-template",
			},
		},
	}

	if _, err := cs.SchedulingV1beta1().PodGroups(ns).Create(testCtx.Ctx, pgPass, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Expected PodGroup with ParentCompositePodGroupName and WorkloadRef to be created successfully, got error: %v", err)
	}

	// 场景 2：设置了 ParentCompositePodGroupName 但未设置 WorkloadRef -> 应当失败
	pgFail := &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-fail", Namespace: ns},
		Spec: schedulingapi.PodGroupSpec{
			SchedulingPolicy: schedulingapi.PodGroupSchedulingPolicy{
				Basic: &schedulingapi.BasicSchedulingPolicy{},
			},
			ParentCompositePodGroupName: new("some-cpg"),
		},
	}

	if _, err := cs.SchedulingV1beta1().PodGroups(ns).Create(testCtx.Ctx, pgFail, metav1.CreateOptions{}); err == nil {
		t.Fatalf("Expected creation of PodGroup with ParentCompositePodGroupName and no WorkloadRef to fail, but it passed")
	}
}
