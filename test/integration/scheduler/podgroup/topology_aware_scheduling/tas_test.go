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

package topologyawarescheduling

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	stepsframework "k8s.io/kubernetes/test/integration/scheduler/podgroup/stepsframework"
	testutils "k8s.io/kubernetes/test/integration/util"
)

func makeGangPodGroup(podGroupName, topologyKey string, minCount int32) *schedulingapi.PodGroup {
	return st.MakePodGroup().Name(podGroupName).WorkloadRef("t1", "workload").TopologyKey(topologyKey).MinCount(minCount).Priority(100).Obj()
}

func makeGangPodGroupWithPriority(podGroupName, topologyKey string, minCount int32, priority int32) *schedulingapi.PodGroup {
	return st.MakePodGroup().Name(podGroupName).WorkloadRef("t1", "workload").TopologyKey(topologyKey).MinCount(minCount).Priority(priority).Obj()
}

func makeBasicPodGroup(podGroupName, topologyKey string) *schedulingapi.PodGroup {
	return st.MakePodGroup().Name(podGroupName).WorkloadRef("t1", "workload").BasicPolicy().TopologyKey(topologyKey).Priority(100).Obj()
}

func makeNode(nodeName, rackLabel, zoneLabel string) *v1.Node {
	return st.MakeNode().Name(nodeName).Label("rack", rackLabel).Label("zone", zoneLabel).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()
}

func makePod(podName, podGroupName string) *v1.Pod {
	return st.MakePod().Name(podName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Container("image").
		PodGroupName(podGroupName).Priority(100).ZeroTerminationGracePeriod().Obj()
}

func makePodWithPriority(podName, podGroupName string, priority int32) *v1.Pod {
	return st.MakePod().Name(podName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "1"}).Container("image").
		PodGroupName(podGroupName).Priority(priority).ZeroTerminationGracePeriod().Obj()
}

// makeAssignedPod 创建一个已分配到集群中某个节点的既有 Pod
func makeAssignedPod(podName, nodeName, consumedCPU string) *v1.Pod {
	return st.MakePod().Name(podName).Node(nodeName).Req(map[v1.ResourceName]string{v1.ResourceCPU: consumedCPU}).Container("image").Priority(100).ZeroTerminationGracePeriod().Obj()
}

func makeAssignedPodWithPriority(podName, nodeName, consumedCPU string, priority int32) *v1.Pod {
	return st.MakePod().Name(podName).Node(nodeName).Req(map[v1.ResourceName]string{v1.ResourceCPU: consumedCPU}).Container("image").Priority(priority).ZeroTerminationGracePeriod().Obj()
}

// makeLargePod 创建一个消耗单个节点全部资源的 Pod
func makeLargePod(podName, podGroupName string) *v1.Pod {
	return st.MakePod().Name(podName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").
		PodGroupName(podGroupName).Priority(100).Obj()
}

// makeUnfittablePod 创建一个请求资源超过单个节点可用资源的 Pod
func makeUnfittablePod(podName, podGroupName string) *v1.Pod {
	return st.MakePod().Name(podName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "3"}).Container("image").
		PodGroupName(podGroupName).Priority(100).Obj()
}

type scenario struct {
	name  string
	steps []stepsframework.Step
}

func TestTopologyAwareSchedulingWithGangPolicy(t *testing.T) {
	tests := []scenario{
		{
			name: "gang schedules on a single rack, when only one feasible rack is available",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks. Racks 1 and 2 can fit 3 pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
						makeNode("node5-rack2", "rack-2", "zone-1"),
						makeNode("node6-zone2", "rack-3", "zone-2"),
					},
				},
				{
					Name: "Create an assigned pod in rack2, making rack2 unable to fit 3 additional pods",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node4-rack2", "2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Verify all pods scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack1"),
					},
				},
			},
		},
		{
			name: "gang remains pending when no feasible resources are found",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-1"),
						makeNode("node4-zone2", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup, requesting more resources in total than available in any rack",
					CreatePods: []*v1.Pod{
						makeLargePod("p1", "pg1"),
						makeLargePod("p2", "pg1"),
						makeLargePod("p3", "pg1"),
					},
				},
				{
					Name:                     "Verify the entire gang becomes unschedulable",
					WaitForPodsUnschedulable: []string{"p1", "p2", "p3"},
				},
			},
		},
		{
			name: "gang remains pending when resources are consumed by existing pods",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in a rack",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name: "Create assigned pods in rack1, making rack1 unable to fit 3 additional pods",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node1-rack1", "2"),
						makeAssignedPod("existing2", "node3-rack1", "2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup, requesting more resources in total than available in any rack",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                     "Verify the entire gang becomes unschedulable",
					WaitForPodsUnschedulable: []string{"p1", "p2", "p3"},
				},
				{
					Name:       "Delete a pod in rack1 to free up resources",
					DeletePods: []string{"existing1"},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
			},
		},
		{
			name: "gang uses pod group preemption when resources are consumed by lower priority existing pods",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in a rack",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name: "Create assigned pods in rack1, making rack1 unable to fit 3 additional pods",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node1-rack1", "2"),
						makeAssignedPod("existing2", "node3-rack1", "2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroupWithPriority("pg1", "rack", 3, 200),
				},
				{
					Name: "Create all pods belonging to the podgroup, requesting more resources in total than available in any rack",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p1", "pg1", 200),
						makePodWithPriority("p2", "pg1", 200),
						makePodWithPriority("p3", "pg1", 200),
					},
				},
				{
					Name:                 "Verify the entire gang is scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:               "Verify that low prio pods are removed",
					WaitForPodsRemoved: []string{"existing1", "existing2"},
				},
			},
		},
		{
			name: "gang uses preemption and schedules on a single rack, choosing placement with the highest score",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-2"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
					},
				},
				{
					Name: "Create low-priority assigned pods making rack1 and rack2 unable to fit 3 additional pods without preemption",
					CreatePods: []*v1.Pod{
						makeAssignedPodWithPriority("low1-rack1", "node1-rack1", "2", 10),
						makeAssignedPodWithPriority("low1-rack2", "node3-rack2", "2", 10),
						makeAssignedPodWithPriority("low2-rack2", "node4-rack2", "2", 10),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroupWithPriority("pg1", "rack", 3, 200),
				},
				{
					Name: "Create all high-priority pods belonging to the podgroup. Preemption occurs, and rack1 scores higher than rack2",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p1", "pg1", 200),
						makePodWithPriority("p2", "pg1", 200),
						makePodWithPriority("p3", "pg1", 200),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:               "Verify that low prio victim pod on rack1 is removed",
					WaitForPodsRemoved: []string{"low1-rack1"},
				},
				{
					// 打分结果：PodGroupPodsCount 对 rack1 和 rack2 的评分相同，
					// 而使用默认策略 MostAllocated 的 NodeResourcesFit 在 rack1 上评分更高（3/4 = 75% vs 3/6 = 50%）。
					Name: "Verify all pods scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
			},
		},
		{
			name: "gang uses preemption and schedules on a single rack, choosing placement with highest allocation percentage with pre-existing pods in cluster",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-2"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
						makeNode("node7-rack3", "rack-3", "zone-2"),
						makeNode("node8-rack3", "rack-3", "zone-2"),
						makeNode("node9-rack4", "rack-4", "zone-2"),
					},
				},
				{
					Name: "Create pre-existing high-priority pods and low-priority victim pods forcing preemption across racks",
					CreatePods: []*v1.Pod{
						makeAssignedPodWithPriority("existing1", "node3-rack2", "2", 200),
						makeAssignedPodWithPriority("existing2", "node8-rack3", "1", 200),
						makeAssignedPodWithPriority("low1-rack1", "node1-rack1", "2", 10),
						makeAssignedPodWithPriority("low1-rack2", "node4-rack2", "2", 10),
						makeAssignedPodWithPriority("low1-rack3", "node6-rack3", "2", 10),
						makeAssignedPodWithPriority("low2-rack3", "node8-rack3", "1", 10),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroupWithPriority("pg1", "rack", 3, 200),
				},
				{
					Name: "Create all high-priority pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p1", "pg1", 200),
						makePodWithPriority("p2", "pg1", 200),
						makePodWithPriority("p3", "pg1", 200),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:               "Verify that low prio victim pod on rack2 is removed",
					WaitForPodsRemoved: []string{"low1-rack2"},
				},
				// 抢占后各机架的分配比例（MostAllocated 策略取最大分配值）：
				// - rack1: (0 + 3)/4 = 0.75
				// - rack2: (2 + 3)/6 = 0.833
				// - rack3: (1 + 3)/6 = 0.667
				// - rack4：不可行（总共只有 2 个 CPU）
				{
					Name: "Verify all pods scheduled on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node3-rack2", "node4-rack2", "node5-rack2"),
					},
				},
			},
		},
		{
			name: "gang schedules on a single rack, choosing placement with the highest score in default placement scoring algorithm",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-2"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup. Pods won't fit on rack3, and will score higher on rack1 than rack2",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					// 打分结果：PodGroupPodsCount 对 rack1 和 rack2 的评分相同，
					// 而使用默认策略 MostAllocated 的 NodeResourcesFit 在 rack1 上评分更高。
					Name: "Verify all pods scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
			},
		},
		{
			name: "gang schedules on a single rack, choosing placement with highest allocation percentage (default placement scoring algorithm) with pre-existing pods in cluster",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-2"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
						makeNode("node7-rack3", "rack-3", "zone-2"),
						makeNode("node8-rack3", "rack-3", "zone-2"),
						makeNode("node9-rack4", "rack-4", "zone-2"),
					},
				},
				{
					Name: "Create pre-existing nodes, which will be considered in the scoring algorithm",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node3-rack2", "2"),
						makeAssignedPod("existing2", "node8-rack3", "1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup. Pods won't fit on rack4, and will score highest on rack2",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				// 每个节点有 2 个 CPU，gang 中每个 Pod 请求 1 个 CPU。
				// PodGroupPodsCount 打分插件对机架 1-3 的评分相同，因为它们都能容纳整个 podgroup。
				// 各机架的分配比例（默认的 "most allocated" 策略取最大分配值）：
				// - rack1: (0 + 3)/4 = 0.75
				// - rack2: (2 + 3)/6 = 0.83
				// - rack3: (1 + 3)/6 = 0.67
				// - rack4：不可行
				{
					Name: "Verify all pods scheduled on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node3-rack2", "node4-rack2", "node5-rack2"),
					},
				},
			},
		},
		{
			name: "two gangs schedule consecutively, each on a separate rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
						makeNode("node5-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to gang pg1. Pods are large, pg1 will only fit on rack1",
					CreatePods: []*v1.Pod{
						makeLargePod("p1", "pg1"),
						makeLargePod("p2", "pg1"),
						makeLargePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=2) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg2", "rack", 2),
				},
				{
					Name: "Create all pods belonging to gang pg2",
					CreatePods: []*v1.Pod{
						makePod("p4", "pg2"),
						makePod("p5", "pg2"),
					},
				},
				{
					Name:                 "Verify the entire gang pg2 is now scheduled",
					WaitForPodsScheduled: []string{"p4", "p5"},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack1 (the only one fitting them)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack1"),
					},
				},
				{
					Name: "Verify all pods in pg2 scheduled on rack2 (the only one fitting them after pg1 is scheduled)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p4", "p5"},
						Nodes: sets.New("node4-rack2", "node5-rack2"),
					},
				},
			},
		},
		{
			name: "two gangs schedule consecutively, both on the same rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=2) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 2),
				},
				{
					Name: "Create all pods belonging to gang pg1",
					CreatePods: []*v1.Pod{
						makeLargePod("p1", "pg1"),
						makePod("p2", "pg1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg2", "rack", 3),
				},
				{
					Name: "Create all pods belonging to gang pg2",
					CreatePods: []*v1.Pod{
						makePod("p3", "pg2"),
						makePod("p4", "pg2"),
						makePod("p5", "pg2"),
					},
				},
				{
					Name:                 "Verify the entire gang pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2"},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack1 (the only one fitting them)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack1"),
					},
				},
				{
					Name:                 "Verify the entire gang pg2 is now scheduled",
					WaitForPodsScheduled: []string{"p3", "p4", "p5"},
				},
				{
					Name: "Verify all pods in pg2 scheduled also on rack1 (the only one fitting them)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p3", "p4", "p5"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack1"),
					},
				},
			},
		},
		{
			name: "two gangs schedule consecutively, different topology keys",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-1"),
						makeNode("node4-zone2", "rack-3", "zone-2"),
						makeNode("node5-zone2", "rack-3", "zone-2"),
						makeNode("node6-zone2", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to gang pg1",
					CreatePods: []*v1.Pod{
						makeLargePod("p1", "pg1"),
						makeLargePod("p2", "pg1"),
						makeLargePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled in one zone",
					CreatePodGroup: makeGangPodGroup("pg2", "zone", 3),
				},
				{
					Name: "Create all pods belonging to gang pg2",
					CreatePods: []*v1.Pod{
						makeLargePod("p4", "pg2"),
						makeLargePod("p5", "pg2"),
						makeLargePod("p6", "pg2"),
					},
				},
				{
					Name:                 "Verify the entire gang pg2 is now scheduled",
					WaitForPodsScheduled: []string{"p4", "p5", "p6"},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack3 (the only one fitting them)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node4-zone2", "node5-zone2", "node6-zone2"),
					},
				},
				{
					Name: "Verify all pods in pg2 scheduled in zone1 (the only one fitting them)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p4", "p5", "p6"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack2"),
					},
				},
			},
		},
		{
			name: "two gangs schedule consecutively, only one fits in the cluster",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in one rack",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to gang pg1",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=2) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg2", "rack", 2),
				},
				{
					Name: "Create all pods belonging to gang pg2",
					CreatePods: []*v1.Pod{
						makePod("p4", "pg2"),
						makePod("p5", "pg2"),
					},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
				{
					Name:                     "Verify the entire second gang becomes unschedulable due to insufficient resources",
					WaitForPodsUnschedulable: []string{"p4", "p5"},
				},
			},
		},
		{
			name: "gang with minCount < pod count schedules on a single rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in two racks, each rack can fit 3 pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=2) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 2),
				},
				{
					Name: "Create 3 pods (more than minCount) belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Verify all pods scheduled on a single rack",
					VerifyAssignedInOneDomain: &stepsframework.VerifyAssignedInOneDomain{
						Pods:        []string{"p1", "p2", "p3"},
						TopologyKey: "rack",
					},
				},
			},
		},
		{
			// TODO: 添加一个测试场景：minCount < pod count，且进入调度周期时调度器在队列中看到多于 minCount 的 Pod。
			name: "gang with minCount < pod count schedules on a single rack, choosing smaller rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks, one can fit 4 pods, second can fit 6 pods, last is too small",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
						makeNode("node5-rack2", "rack-2", "zone-1"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create 3 pods (=minCount) belonging to the gang",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Create an additional pod (pod count > minCount) belonging to the gang",
					CreatePods: []*v1.Pod{
						makePod("p4", "pg1"),
					},
				},
				{
					Name:                 "Verify the the new pods gets scheduled",
					WaitForPodsScheduled: []string{"p4"},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack2, which scored higher in the default placement scoring algorithm",
					// 打分细节：
					// 打分时 pg1 包含 3 个 Pod。机架 1 和 2 都能容纳全部 3 个 Pod，因此 PodGroupPodsCount 给它们的评分相同。
					// 使用默认 mostAllocated 策略的 NodeResourcesFit 给 rack2 的评分高于 rack1。
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3", "p4"},
						Nodes: sets.New("node4-rack2", "node5-rack2"),
					},
				},
				{
					Name: "Create one more pod belonging to the gang",
					CreatePods: []*v1.Pod{
						makePod("p5", "pg1"),
					},
				},
				{
					Name:                     "Verify the last pod becomes unschedulable due to insufficient resources in the rack",
					WaitForPodsUnschedulable: []string{"p5"},
				},
			},
		},
		{
			name: "gang with minCount < pod count schedules on a single rack without preemption, additional pod triggers preemption on the same rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks, one can fit 4 pods, second can fit 6 pods, last is too small",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
						makeNode("node5-rack2", "rack-2", "zone-1"),
						makeNode("node6-rack3", "rack-3", "zone-2"),
					},
				},
				{
					Name: "Create a low-priority assigned pod on rack2",
					CreatePods: []*v1.Pod{
						makeAssignedPodWithPriority("low1-rack2", "node5-rack2", "1", 10),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroupWithPriority("pg1", "rack", 3, 200),
				},
				{
					Name: "Create 3 pods (=minCount) belonging to the gang, fitting on rack2 without preemption",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p1", "pg1", 200),
						makePodWithPriority("p2", "pg1", 200),
						makePodWithPriority("p3", "pg1", 200),
					},
				},
				{
					Name:                 "Verify the initial gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Verify all initial pods scheduled on rack2 without preempting low-priority pod",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3", "low1-rack2"},
						Nodes: sets.New("node4-rack2", "node5-rack2"),
					},
				},
				{
					Name: "Create an additional pod (pod count > minCount) belonging to the gang, which requires preemption on rack2",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p4", "pg1", 200),
					},
				},
				{
					Name:                 "Verify the new pod gets scheduled",
					WaitForPodsScheduled: []string{"p4"},
				},
				{
					Name:               "Verify low-priority pod on rack2 was preempted",
					WaitForPodsRemoved: []string{"low1-rack2"},
				},
				{
					Name: "Verify all gang pods scheduled on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3", "p4"},
						Nodes: sets.New("node4-rack2", "node5-rack2"),
					},
				},
				{
					Name: "Create one more pod belonging to the gang",
					CreatePods: []*v1.Pod{
						makePodWithPriority("p5", "pg1", 200),
					},
				},
				{
					Name:                     "Verify the last pod becomes unschedulable because rack2 is full and preemption cannot place it elsewhere",
					WaitForPodsUnschedulable: []string{"p5"},
				},
				{
					Name: "Verify assignments of scheduled gang pods remain on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3", "p4"},
						Nodes: sets.New("node4-rack2", "node5-rack2"),
					},
				},
			},
		},
		{
			name: "gang does not schedule on a single rack when preemption could free up enough resources",
			steps: []stepsframework.Step{
				{
					Name: "Create single rack that can hold pod group pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name: "Create an assigned pod in rack1, making rack1 unable to fit 2 additional pods",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node2-rack1", "2"),
					},
				},
				{
					Name:           "Create the PodGroup object (Gang with minCount=3) that should be scheduled on one rack",
					CreatePodGroup: makeGangPodGroup("pg1", "rack", 3),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                     "Verify the entire gang is unschedulable",
					WaitForPodsUnschedulable: []string{"p1", "p2", "p3"},
				},
			},
		},
	}

	for _, tt := range tests {
		for _, cpgEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s (CPG: %v)", tt.name, cpgEnabled), func(t *testing.T) {
				runTestScenario(t, tt, cpgEnabled)
			})
		}
	}
}

func TestTopologyAwareSchedulingWithBasicPolicy(t *testing.T) {
	tests := []scenario{
		{
			name: "basic podgroup schedules on the only rack in the cluster",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in only one rack",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire podgroup is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Verify all pods scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
			},
		},
		{
			name: "basic podgroup schedules all pods on a single rack, when there are multiple racks fitting in the cluster",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks and zones, each rack enough to fit 3 pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
						makeNode("node5-zone2", "rack-3", "zone-2"),
						makeNode("node6-zone2", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire gang is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				{
					Name: "Verify all pods scheduled the same rack",
					VerifyAssignedInOneDomain: &stepsframework.VerifyAssignedInOneDomain{
						Pods:        []string{"p1", "p2", "p3"},
						TopologyKey: "rack",
					},
				},
			},
		},
		{
			name: "basic podgroup cannot fit on a single rack, no pods get scheduled on a different rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in 2 racks, each rack enough to fit 2 pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup, more than fitting in a single rack",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify that 2 pods got scheduled",
					WaitForPodsScheduled: []string{"p1", "p2"},
				},
				{
					Name:                     "Verify that the last pod becomes unschedulable due to insufficient resources in the rack",
					WaitForPodsUnschedulable: []string{"p3"},
				},
				{
					Name: "Verify both pods scheduled on the same rack",
					VerifyAssignedInOneDomain: &stepsframework.VerifyAssignedInOneDomain{
						Pods:        []string{"p1", "p2"},
						TopologyKey: "rack",
					},
				},
			},
		},
		{
			name: "basic podgroup does not schedule at all when no pods fit",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in 2 racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup, each pod not fitting any nodes in the cluster",
					CreatePods: []*v1.Pod{
						makeUnfittablePod("p1", "pg1"),
						makeUnfittablePod("p2", "pg1"),
					},
				},
				{
					Name:                     "Verify no pods are scheduled due to insufficient resources in the rack",
					WaitForPodsUnschedulable: []string{"p1", "p2"},
				},
			},
		},
		{
			name: "basic podgroup schedules only some pods when others don't fit on any node",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in 2 racks, each rack enough to fit 2 regular pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup, some of them not fitting any nodes in the cluster",
					CreatePods: []*v1.Pod{
						makeUnfittablePod("p1", "pg1"),
						makePod("p2", "pg1"),
					},
				},
				{
					Name:                 "Verify that the smaller pod got scheduled",
					WaitForPodsScheduled: []string{"p2"},
				},
				{
					Name:                     "Verify that the big pod is not scheduled due to insufficient resources in the rack",
					WaitForPodsUnschedulable: []string{"p1"},
				},
			},
		},
		{
			name: "basic podgroup schedules on a single rack, choosing placement with highest allocation percentage (default placement scoring algorithm)",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-2"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire podgroup is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				// 每个节点有 2 个 CPU，gang 中每个 Pod 请求 1 个 CPU。
				// 分数是在调度第一个 Pod 时计算的，此时并不知道 podgroup 中会有多少个 Pod。
				// PodGroupPodsCount 对每个机架的评分相同（因为两个机架都能容纳第一个 Pod）。
				// NodeResourcesFit 会计算机架中的分配比例（默认的 "most allocated" 策略取最高分配值）：
				// - rack1: 1/4 = 0.25
				// - rack2: 1/6 = 0.17
				{
					Name: "Verify all pods scheduled on rack1",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
			},
		},
		{
			name: "basic podgroup schedules on a single rack, choosing placement with highest allocation percentage with pre-existing pods",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-2"),
						makeNode("node5-rack2", "rack-2", "zone-2"),
						makeNode("node6-rack2", "rack-2", "zone-2"),
						makeNode("node7-rack3", "rack-3", "zone-2"),
						makeNode("node8-rack3", "rack-3", "zone-2"),
						makeNode("node9-rack3", "rack-3", "zone-2"),
						makeNode("node10-rack3", "rack-3", "zone-2"),
					},
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node1-rack1", "1"),
						makeAssignedPod("existing2", "node5-rack2", "1"),
						makeAssignedPod("existing3", "node6-rack2", "1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the entire podgroup is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3"},
				},
				// 每个节点有 2 个 CPU，podgroup 中每个 Pod 请求 1 个 CPU。
				// 分数是在调度第一个 Pod 时计算的，此时并不知道 podgroup 中会有多少个 Pod。
				// PodGroupPodsCount 对每个机架的评分相同（因为两个机架都能容纳第一个 Pod）。
				// NodeResourcesFit 会计算机架中的分配比例（默认的 "most allocated" 策略取最高分配值）：
				// - rack1: 2/6 = 0.33
				// - rack2: 3/6 = 0.5
				// - rack3: 1/8 = 0.125
				{
					Name: "Verify all pods scheduled on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2", "p3"},
						Nodes: sets.New("node4-rack2", "node5-rack2", "node6-rack2"),
					},
				},
			},
		},
		{
			name: "basic podgroup schedules on a single rack, choosing best scoring placement that will not fit all podgroup",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack1", "rack-1", "zone-1"),
						makeNode("node5-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name: "Create pods in rack1, consuming 25% of its resources",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node1-rack1", "2"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					// 修复测试的不稳定性：
					// 为使测试行为确定，先创建 1 个 Pod，并基于它计算分数，
					// 待第一个 Pod 调度完成后再添加更多 Pod。
					Name: "Create the first pod belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
					},
				},
				{
					Name:                 "Verify the pod is scheduled",
					WaitForPodsScheduled: []string{"p1"},
				},
				{
					Name: "Create other pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				// 每个节点有 2 个 CPU，podgroup 的 Pod 请求 1 或 2 个 CPU。
				// 分数是在调度第一个 Pod 时计算的，此时并不知道 podgroup 中会有多少个 Pod。
				// PodGroupPodsCount 对每个机架的评分相同（因为两个机架都能容纳第一个 Pod）。
				// NodeResourcesFit 会计算机架中的分配比例（默认的 "most allocated" 策略取最高分配值）：
				// - rack1: 3/8 = 0.375
				// - rack2: 1/2 = 0.5
				{
					Name:                 "Verify one of the additional pods is scheduled",
					WaitForPodsScheduled: []string{"p2"},
				},
				{
					Name: "Verify pod scheduled on rack2",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node5-rack2"),
					},
				},
				{
					Name:                     "Verify that third pod is not scheduled due to insufficient resources in rack2",
					WaitForPodsUnschedulable: []string{"p3"},
				},
			},
		},
		{
			name: "two basic podgroups schedule consecutively, each on a single rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
						makeNode("node4-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg1",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg2", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg2",
					CreatePods: []*v1.Pod{
						makePod("p3", "pg2"),
						makePod("p4", "pg2"),
					},
				},
				{
					Name:                 "Verify the entire podgroup pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p1", "p2"},
				},
				// 每个节点有 2 个 CPU，podgroup 的 Pod 请求 1 或 2 个 CPU。
				// 分数是在调度第一个 Pod 时计算的，此时并不知道 podgroup 中会有多少个 Pod。
				// PodGroupPodsCount 对每个机架的评分相同（因为两个机架都能容纳第一个 Pod）。
				// 各机架的分配比例（默认的 "most allocated" 策略取最高分配值）：
				// - rack1: 1/6 = 0.17
				// - rack2: 1/2 = 0.5
				{
					Name: "Verify all pods in pg1 scheduled on rack2 (which scored most allocation)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node4-rack2"),
					},
				},
				{
					Name:                 "Verify the entire podgroup pg2 is now scheduled",
					WaitForPodsScheduled: []string{"p3", "p4"},
				},
				{
					Name: "Verify all pods in pg2 scheduled on rack1 (because rack2 is full)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p3", "p4"},
						Nodes: sets.New("node1-rack1", "node2-rack1", "node3-rack1"),
					},
				},
			},
		},
		{
			name: "two basic podgroups schedule consecutively on the only rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in one rack",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack1", "rack-1", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg1",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg2", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg2",
					CreatePods: []*v1.Pod{
						makePod("p3", "pg2"),
						makePod("p4", "pg2"),
						makePod("p5", "pg2"),
					},
				},
				{
					Name:                 "Verify all pods scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3", "p4", "p5"},
				},
			},
		},
		{
			name: "two basic podgroups schedule consecutively, different topology keys",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple zones and racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-zone2", "rack-2", "zone-2"),
						makeNode("node3-zone2", "rack-2", "zone-2"),
						makeNode("node4-zone2", "rack-3", "zone-2"),
						makeNode("node5-zone2", "rack-3", "zone-2"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg1",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
						makePod("p2", "pg1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled in one zone",
					CreatePodGroup: makeBasicPodGroup("pg2", "zone"),
				},
				{
					Name: "Create all pods belonging to podgroup pg2. Large pods, each consumes CPU of an entire node",
					CreatePods: []*v1.Pod{
						makeLargePod("p3", "pg2"),
						makeLargePod("p4", "pg2"),
						makeLargePod("p5", "pg2"),
					},
				},
				{
					Name:                 "Verify all pods scheduled",
					WaitForPodsScheduled: []string{"p1", "p2", "p3", "p4", "p5"},
				},
				{
					Name: "Verify all pods in pg1 scheduled on rack1 (the smallest rack, scoring highest allocation fraction)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node1-rack1"),
					},
				},
				{
					Name: "Verify all pods in pg2 scheduled in zone2 (because zone1 is full)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p3", "p4", "p5"},
						Nodes: sets.New("node2-zone2", "node3-zone2", "node4-zone2", "node5-zone2"),
					},
				},
			},
		},
		{
			name: "two basic podgroups schedule consecutively, one does not fit in the assigned rack",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in multiple racks",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack1", "rack-1", "zone-1"),
						makeNode("node3-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					// 修复测试的不稳定性：
					// 为使测试行为确定，先创建 1 个 Pod，并基于它计算分数，
					// 待第一个 Pod 调度完成后再添加更多 Pod。
					Name: "Create the first pod belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
					},
				},
				{
					Name:                 "Verify the pod is scheduled",
					WaitForPodsScheduled: []string{"p1"},
				},
				{
					Name: "Create other pods belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p2", "pg1"),
						makePod("p3", "pg1"),
					},
				},
				{
					Name:                 "Verify the first of additional pods in pg1 is now scheduled",
					WaitForPodsScheduled: []string{"p2"},
				},
				{
					Name:                     "Verify the last pod becomes unschedulable due to insufficient resources",
					WaitForPodsUnschedulable: []string{"p3"},
				},
				// 每个节点有 2 个 CPU，gang 中每个 Pod 请求 1 个 CPU。
				// 分数是在调度第一个 Pod 时计算的，此时并不知道 podgroup 中会有多少个 Pod。
				// PodGroupPodsCount 对每个机架的评分相同（因为两个机架都能容纳第一个 Pod）。
				// 各机架的分配比例（默认的 "most allocated" 策略取最高分配值）：
				// - rack1: 1/4 = 0.25
				// - rack2: 1/2 = 0.5
				{
					Name: "Verify all pods in pg1 scheduled on rack2 (which scored higher allocation)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node3-rack2"),
					},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg2", "rack"),
				},
				{
					Name: "Create all pods belonging to podgroup pg2",
					CreatePods: []*v1.Pod{
						makePod("p4", "pg2"),
						makePod("p5", "pg2"),
						makePod("p6", "pg2"),
					},
				},
				{
					Name:                 "Verify the entire podgroup pg2 is now scheduled",
					WaitForPodsScheduled: []string{"p4", "p5", "p6"},
				},
				{
					Name: "Verify pods in pg2 scheduled on rack1 (because rack2 is full)",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p4", "p5", "p6"},
						Nodes: sets.New("node1-rack1", "node2-rack1"),
					},
				},
			},
		},
		{
			name: "basic podgroup continues to schedule pods when more resources become available",
			steps: []stepsframework.Step{
				{
					Name: "Create nodes in 2 racks, each rack enough to fit 2 pods",
					CreateNodes: []*v1.Node{
						makeNode("node1-rack1", "rack-1", "zone-1"),
						makeNode("node2-rack2", "rack-2", "zone-1"),
					},
				},
				{
					Name: "Create blocker pod in rack1",
					CreatePods: []*v1.Pod{
						makeAssignedPod("existing1", "node1-rack1", "1"),
					},
				},
				{
					Name:                 "Verify that the blocker pod got scheduled",
					WaitForPodsScheduled: []string{"existing1"},
				},
				{
					Name:           "Create the PodGroup object that should be scheduled on one rack",
					CreatePodGroup: makeBasicPodGroup("pg1", "rack"),
				},
				{
					// 修复测试的不稳定性：
					// 为使测试行为确定，先创建 1 个 Pod，并基于它计算分数，
					// 待第一个 Pod 调度完成后再添加更多 Pod。
					Name: "Create the first pod belonging to the podgroup",
					CreatePods: []*v1.Pod{
						makePod("p1", "pg1"),
					},
				},
				{
					Name:                 "Verify the pod is scheduled",
					WaitForPodsScheduled: []string{"p1"},
				},
				{
					Name: "Create other pods belonging to the podgroup, exceeding the capacity of rack1",
					CreatePods: []*v1.Pod{
						makePod("p2", "pg1"),
					},
				},
				{
					Name: "Verify that p1 got scheduled on rack1, which scored higher in mostAllocated strategy",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1"},
						Nodes: sets.New("node1-rack1"),
					},
				},
				{
					Name:                     "Verify that the other pod becomes unschedulable due to insufficient resources in the rack",
					WaitForPodsUnschedulable: []string{"p2"},
				},
				{
					Name:       "Remove the blocker pod",
					DeletePods: []string{"existing1"},
				},
				{
					Name:                 "Verify that both pods got scheduled",
					WaitForPodsScheduled: []string{"p1", "p2"},
				},
				{
					Name: "Verify that both are on the same rack",
					VerifyAssignments: &stepsframework.VerifyAssignments{
						Pods:  []string{"p1", "p2"},
						Nodes: sets.New("node1-rack1"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		for _, cpgEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s (CPG: %v)", tt.name, cpgEnabled), func(t *testing.T) {
				runTestScenario(t, tt, cpgEnabled)
			})
		}
	}
}

func runTestScenario(t *testing.T, tt scenario, cpgEnabled bool) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.CompositePodGroup:               cpgEnabled,
		features.GenericWorkload:                 true,
		features.TopologyAwareWorkloadScheduling: true,
	})

	testCtx := testutils.InitTestSchedulerWithNS(t, "tas",
		scheduler.WithPodMaxBackoffSeconds(0),
		scheduler.WithPodInitialBackoffSeconds(0))
	ns := testCtx.NS.Name

	workload := st.MakeWorkload().Name("workload").Namespace(ns).
		PodGroupTemplate(st.MakePodGroupTemplate().Name("t1").MinCount(1).Obj()).
		Obj()
	workloadStep := []stepsframework.Step{
		{
			Name:            "Creating workload",
			CreateWorkloads: []*schedulingapi.Workload{workload},
		},
	}
	if err := stepsframework.RunSteps(testCtx, t, ns, append(workloadStep, tt.steps...)); err != nil {
		t.Fatal(err)
	}
}
