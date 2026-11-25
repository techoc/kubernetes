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
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	componentmetrics "k8s.io/component-base/metrics"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

type mockFilterPlugin struct {
	nodeCapacities []nodeCapacity
}

// getCapacity 从 Pod 的 "size" 标签读取大小，未设置或解析失败时按 1 计。
func getCapacity(pod *v1.Pod) int {
	if v, ok := pod.Labels["size"]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 1
}

// Filter 按模拟容量判断 Pod 能否放到节点上。
// 节点容量取自插件中写死的 nodeCapacities，Pod 大小取自 "size" 标签（缺省为 1）。
func (m *mockFilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	newPodCapacity := getCapacity(pod)
	nodeCapacity := 0
	for _, n := range m.nodeCapacities {
		if n.nodeName == nodeInfo.Node().Name {
			nodeCapacity = n.capacity
			break
		}
	}
	currentNodeSize := 0
	for _, p := range nodeInfo.GetPods() {
		currentNodeSize += getCapacity(p.GetPod())
	}
	if currentNodeSize+newPodCapacity > nodeCapacity {
		return fwk.NewStatus(fwk.Unschedulable, "not enough capacity")
	}
	return fwk.NewStatus(fwk.Success)
}

// Name 返回模拟过滤插件的名字。
func (m *mockFilterPlugin) Name() string {
	return "mockFilterPlugin"
}

// nodeCapacity 定义一个模拟节点的总容量。
type nodeCapacity struct {
	nodeName string
	capacity int
}

var _ fwk.FilterPlugin = &mockFilterPlugin{}

// mockPodGroupLister 是测试用的 PodGroupLister，从预置 map 中按名字查找。
type mockPodGroupLister struct {
	podGroups map[string]*schedulingv1beta1.PodGroup
}

// Get 按名字返回预置的 PodGroup，找不到时报错。
func (m *mockPodGroupLister) Get(namespace, name string) (*schedulingv1beta1.PodGroup, error) {
	if pg, ok := m.podGroups[name]; ok {
		return pg, nil
	}
	return nil, fmt.Errorf("pod group %s not found", name)
}

// mockCompositePodGroupLister 是测试用的 CompositePodGroupLister，从预置 map 中按名字查找。
type mockCompositePodGroupLister struct {
	compositePodGroups map[string]*schedulingv1alpha3.CompositePodGroup
}

// Get 按名字返回预置的 CompositePodGroup，找不到时报错。
func (m *mockCompositePodGroupLister) Get(namespace, name string) (*schedulingv1alpha3.CompositePodGroup, error) {
	if cpg, ok := m.compositePodGroups[name]; ok {
		return cpg, nil
	}
	return nil, fmt.Errorf("composite pod group %s not found", name)
}

// makePodGroupPreemptor 用默认 PreemptLowerPriority 策略构造测试用抢占者。
func makePodGroupPreemptor(pg *schedulingv1beta1.PodGroup, pods []*v1.Pod) *podGroupPreemptor {
	return makePodGroupPreemptorWithPreemptionPolicy(pg, pods, schedulingv1beta1.PreemptLowerPriority)
}

// makePodGroupPreemptorWithPreemptionPolicy 用给定策略构造测试用抢占者，
// 优先级直接取自 PodGroup。
func makePodGroupPreemptorWithPreemptionPolicy(pg *schedulingv1beta1.PodGroup, pods []*v1.Pod, policy schedulingv1beta1.PreemptionPolicy) *podGroupPreemptor {
	return &podGroupPreemptor{
		priority:         util.PodGroupPriority(pg),
		pods:             pods,
		podGroup:         pg,
		preemptionPolicy: policy,
	}
}

// TestPodGroupEvaluator_SelectVictimsOnDomain 表驱动校验 SelectVictimsOnDomain：
// 在单个域内按优先级、PDB 和 gang 约束选出应被驱逐的受害者。
func TestPodGroupEvaluator_SelectVictimsOnDomain(t *testing.T) {
	tests := []struct {
		name                           string
		nodes                          []*v1.Node
		initPods                       []*v1.Pod
		initPodGroups                  []*schedulingv1beta1.PodGroup
		preemptor                      *podGroupPreemptor
		pdbs                           []*policy.PodDisruptionBudget
		nodeCapacities                 []nodeCapacity
		expectedVictims                []string
		expectedStatus                 *fwk.Status
		expectedNumPodGroupDisruptions int
		expectedNumPDBViolations       int
	}{
		{
			name: "Priority: mix of no groups and pod groups",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Labels(map[string]string{"size": "1"}).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node1").Priority(lowPriority).Labels(map[string]string{"size": "1"}).PodGroupName("pg1").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Labels(map[string]string{"size": "1"}).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims: []string{"p1"}, // p1 不属于任何 Pod 组，因此优先级低于 p2
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Priority: StartTime of pods from same group with disruption mode single ",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(0, 0)).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims:                []string{"p1"}, // p1 的 StartTime 更晚，因此优先级低于 p2
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Pod group with disruption mode group not reprieved",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"p1", "p2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Complex Mixed: Shared, different, and no groups",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
				st.MakeNode().Name("node4").Obj(),
				st.MakeNode().Name("node5").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(2, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").StartTime(metav1.Unix(0, 0)).Obj(),
				st.MakePod().Name("p4").UID("v4").Node("node4").Priority(midPriority).Obj(),
				st.MakePod().Name("p5").UID("v5").Node("node5").Priority(highPriority).PodGroupName("pg3").StartTime(metav1.Unix(0, 0)).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
				st.MakePodGroup().Name("pg2").UID("pg2").DisruptionModeSingle().Priority(lowPriority).Obj(),
				st.MakePodGroup().Name("pg3").UID("pg3").DisruptionModeSingle().Priority(highPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj(),
					st.MakePod().Name("p-2").UID("p-2").Priority(highPriority).Obj(),
					st.MakePod().Name("p-3").UID("p-3").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 2,
				},
				{
					nodeName: "node4",
					capacity: 2,
				},
				{
					nodeName: "node5",
					capacity: 2,
				},
			},
			expectedVictims:                []string{"p2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "PDB: Mixed groups",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node1").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims:                []string{"victim-no-pdb"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "PodGroup preemptor with PreemptLowerPriority preemption policy performs preemption",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptor: makePodGroupPreemptorWithPreemptionPolicy(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj()},
				schedulingv1beta1.PreemptLowerPriority,
			),
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 1},
				{nodeName: "node2", capacity: 1},
				{nodeName: "node3", capacity: 1},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PodGroup preemptor with PreemptLowerPriority preemption policy performs preemption, ignoring member pod preemption policy",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptor: makePodGroupPreemptorWithPreemptionPolicy(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).PreemptionPolicy(schedulingv1beta1.PreemptLowerPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).PreemptionPolicy(v1.PreemptNever).Obj(),
				},
				schedulingv1beta1.PreemptLowerPriority,
			),
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 1},
				{nodeName: "node2", capacity: 1},
				{nodeName: "node3", capacity: 1},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Preemptor group is eligible if terminating pods are on non-nominated nodes",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node2").Priority(lowPriority).Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
				st.MakePod().Name("other-victim").UID("v2").Node("node1").Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p1").UID("p1").Priority(highPriority).Obj(),
					st.MakePod().Name("p2").UID("p2").Priority(highPriority).NominatedNodeName("node1").Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims: []string{"other-victim", "victim"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Preemptor group is eligible if nominated node has terminating pod belonging to a pod group of higher priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("victim-pg").Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
				st.MakePod().Name("other-victim").UID("v2").Node("node1").Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Priority(highPriority).DisruptionModeAll().Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p1").UID("p1").Priority(midPriority).Obj(),
					st.MakePod().Name("p2").UID("p2").Priority(midPriority).NominatedNodeName("node1").Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims: []string{"other-victim"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Preemptor group is eligible if nominated node has terminating pod belonging to a pod group of higher priority with nil DisruptionMode",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("victim-pg").Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
				st.MakePod().Name("other-victim").UID("v2").Node("node1").Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Priority(highPriority).DisruptionModeSingle().Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p1").UID("p1").Priority(midPriority).Obj(),
					st.MakePod().Name("p2").UID("p2").Priority(midPriority).NominatedNodeName("node1").Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims: []string{"other-victim"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Preempt single lower priority pod",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Priority: Prefer lower priority victim",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node1").Priority(midPriority).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node1").Priority(highPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims: []string{"p1", "p2"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Efficiency: Preempt minimum number of victims",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p4").UID("v4").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 3,
				},
			},
			expectedVictims: []string{"p3", "p4"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PDB: Prefer non-violating victim",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node1").Priority(lowPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 2},
			},
			expectedVictims: []string{"victim-no-pdb"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PDB: Prefer removing non-violating victim over lower priority violating victim",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node1").Priority(midPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 2},
			},
			expectedVictims: []string{"victim-no-pdb"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PodGroup preemptor with PreemptLowerPriority preemption policy performs preemption with PodGroup victims",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
				st.MakePodGroup().Name("pg2").UID("pg2").DisruptionModeSingle().Priority(lowPriority).Obj(),
			},
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 1},
				{nodeName: "node2", capacity: 1},
				{nodeName: "node3", capacity: 1},
			},
			preemptor: makePodGroupPreemptorWithPreemptionPolicy(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj(),
				},
				schedulingv1beta1.PreemptLowerPriority,
			),
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PodGroup preemptor with PreemptLowerPriority preemption policy performs preemption with PodGroup victims, ignoring member pod preemption policy",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
				st.MakePodGroup().Name("pg2").UID("pg2").DisruptionModeSingle().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptorWithPreemptionPolicy(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).PreemptionPolicy(v1.PreemptNever).Obj(),
				},
				schedulingv1beta1.PreemptLowerPriority,
			),
			nodeCapacities: []nodeCapacity{
				{nodeName: "node1", capacity: 1},
				{nodeName: "node2", capacity: 1},
				{nodeName: "node3", capacity: 1},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PDB: Prefer lower priority pod for preemption, when preemption without pdb violation is not possible",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node1").Label("app", "foo").Priority(midPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims:          []string{"p1"},
			expectedStatus:           fwk.NewStatus(fwk.Success),
			expectedNumPDBViolations: 1,
		},
		{
			name: "PodGroup: Preempt group as a whole",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			// 抢占者会被分配到 p1 原先占用的位置。
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node1",
					capacity: 2,
				},
			},
			expectedVictims:                []string{"p1", "p2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "PodGroup: Prefer single pod over podGroup for preemption candidate",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-1").UID("g1").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			// node1 的容量只够容纳「抢占者 + pg1」或「抢占者 + p1」之一。
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 3,
				},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "PodGroup: Preempt group as a whole on single node",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("g1-1").UID("g1").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(midPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(lowPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			// node1 的容量只够容纳「抢占者 + pg1」或「抢占者 + p1」之一。
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 3,
				},
			},
			expectedVictims:                []string{"g1-1", "g1-2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "PDB: Unit violation if any member violates",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("g1-1").UID("g1").Node("node1").Label("app", "foo").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(lowPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			// node1 的容量只够容纳「抢占者 + pg1」或「抢占者 + p1」之一。
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 3,
				},
			},
			expectedVictims: []string{"p1"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Failure: Cannot preempt the victim with higher priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(highPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 100,
				},
			},
			expectedVictims: []string{},
			expectedStatus:  fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
		},
		{
			name: "Failure: Cannot preempt the victim with equal priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(midPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 100,
				},
			},
			expectedVictims: []string{},
			expectedStatus:  fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
		},
		{
			name: "Failure: Cannot preempt if node is empty",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			// 该用例验证即使节点空间充足，没有可抢占受害者时也要中止抢占。
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 100,
				},
			},
			expectedVictims: []string{},
			expectedStatus:  fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
		},
		{
			name: "Priority divergence: candidate victim PodGroup has lower priority than the Pods from that group",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(highPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(highPriority).PodGroupName("pg1").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(midPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"p1", "p2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Priority divergence: candidate victim PodGroup has higher priority than the Pods from that group",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeAll().Priority(midPriority).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(midPriority).Obj(),
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
			},
			expectedVictims: []string{},
			expectedStatus:  fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
		},
		{
			name: "Gang scheduling: schedule as many pods as possible without preempting higher priority pods, but still more than minCount",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
				st.MakeNode().Name("node4").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(highPriority).Obj(),
				st.MakePod().Name("p4").UID("v4").Node("node4").Priority(highPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).MinCount(1).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
				{
					nodeName: "node4",
					capacity: 1,
				},
			},
			// 抢占者有 3 个 Pod、minCount 为 1，但由于 p3、p4 优先级更高无法被抢占，
			// maxScheduledCount 只会是 2。
			expectedVictims: []string{"p1", "p2"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Gang scheduling: do not reprieve victim pod group of lower priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node2").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v3").UID("v3").Node("node3").Namespace(v1.NamespaceDefault).Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Namespace(v1.NamespaceDefault).Priority(midPriority).DisruptionModeAll().MinCount(1).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).MinCount(1).Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"v1", "v2", "v3"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Gang scheduling: preempt a pod group victim but do not schedule full pod group",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
				st.MakeNode().Name("node4").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node2").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v3").UID("v3").Node("node3").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg2").Priority(highPriority).Obj(),
				st.MakePod().Name("v4").UID("v4").Node("node4").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg2").Priority(highPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Namespace(v1.NamespaceDefault).Priority(midPriority).DisruptionModeAll().MinCount(2).Obj(),
				st.MakePodGroup().Name("victim-pg2").UID("victim-pg2").Namespace(v1.NamespaceDefault).Priority(highPriority).DisruptionModeAll().MinCount(2).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).MinCount(1).DisruptionModeAll().Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
				{
					nodeName: "node4",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"v1", "v2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Basic scheduling: schedule as many pods as possible without preempting higher priority pods",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
				st.MakeNode().Name("node4").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(highPriority).Obj(),
				st.MakePod().Name("p4").UID("v4").Node("node4").Priority(highPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).BasicPolicy().Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
				{
					nodeName: "node4",
					capacity: 1,
				},
			},
			expectedVictims: []string{"p1", "p2"},
			expectedStatus:  fwk.NewStatus(fwk.Success),
		},
		{
			name: "Basic scheduling: do not reprieve victim pod group of lower priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node2").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v3").UID("v3").Node("node3").Namespace(v1.NamespaceDefault).Priority(lowPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Namespace(v1.NamespaceDefault).Priority(midPriority).DisruptionModeAll().MinCount(1).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).BasicPolicy().Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"v1", "v2", "v3"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
		{
			name: "Basic scheduling: preempt a pod group victim but do not schedule full pod group",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
				st.MakeNode().Name("node4").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node2").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg").Priority(midPriority).Obj(),
				st.MakePod().Name("v3").UID("v3").Node("node3").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg2").Priority(highPriority).Obj(),
				st.MakePod().Name("v4").UID("v4").Node("node4").Namespace(v1.NamespaceDefault).PodGroupName("victim-pg2").Priority(highPriority).Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Namespace(v1.NamespaceDefault).Priority(midPriority).DisruptionModeAll().MinCount(2).Obj(),
				st.MakePodGroup().Name("victim-pg2").UID("victim-pg2").Namespace(v1.NamespaceDefault).Priority(highPriority).DisruptionModeAll().MinCount(2).Obj(),
			},
			preemptor: makePodGroupPreemptor(
				st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).BasicPolicy().DisruptionModeAll().Obj(),
				[]*v1.Pod{
					st.MakePod().Name("p-a").UID("p-a").Priority(highPriority).Obj(),
					st.MakePod().Name("p-b").UID("p-b").Priority(highPriority).Obj(),
					st.MakePod().Name("p-c").UID("p-c").Priority(highPriority).Obj(),
				},
			),
			nodeCapacities: []nodeCapacity{
				{
					nodeName: "node1",
					capacity: 1,
				},
				{
					nodeName: "node2",
					capacity: 1,
				},
				{
					nodeName: "node3",
					capacity: 1,
				},
				{
					nodeName: "node4",
					capacity: 1,
				},
			},
			expectedVictims:                []string{"v1", "v2"},
			expectedStatus:                 fwk.NewStatus(fwk.Success),
			expectedNumPodGroupDisruptions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
			logger, ctx := ktesting.NewTestContext(t)

			mockFilterFactory := func(ctx context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
				return &mockFilterPlugin{
					nodeCapacities: tt.nodeCapacities,
				}, nil
			}

			registeredPlugins := append([]tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterPluginAsExtensions("mockFilterPlugin", mockFilterFactory, "Filter")},
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			)
			var objs []runtime.Object
			for _, p := range append(tt.initPods, tt.preemptor.pods...) {
				objs = append(objs, p)
			}
			for _, n := range tt.nodes {
				objs = append(objs, n)
			}
			informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
			parallelism := parallelize.DefaultParallelism
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			snapshot := internalcache.NewTestSnapshotWithPodGroups(tt.initPods, tt.nodes, tt.initPodGroups)
			f, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithParallelism(parallelism),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithMutableSnapshotLister(snapshot),
				frameworkruntime.WithLogger(logger),
			)
			if err != nil {
				t.Fatal(err)
			}

			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			// mockSchedulingFunc 模拟抢占者 PodGroup 的调度过程。
			// 逐个 Pod 轮询节点，容量够就分配上去；每分配一个 Pod，下一个从下一个节点开始（轮转）。
			// 若成功分配的 Pod 数不足 minCount，则返回 Unschedulable。
			// 节点容量取自 nodeCapacities，Pod 大小取自 "size" 标签（缺省为 1）。
			var mockSchedulingFunc fwk.PodGroupSchedulingFunc = func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
				minCount := 1
				if tt.preemptor.podGroup != nil {
					if tt.preemptor.podGroup.Spec.SchedulingPolicy.Gang != nil {
						minCount = int(tt.preemptor.podGroup.Spec.SchedulingPolicy.Gang.MinCount)
					}
				}

				nodeMap := make(map[string]fwk.NodeInfo)
				assignedSizes := make(map[string]int)
				currentNodes, _ := f.SnapshotSharedLister().NodeInfos().List()
				for _, n := range currentNodes {
					nodeMap[n.Node().Name] = n
					nodeSize := 0
					for _, existingPod := range n.GetPods() {
						nodeSize += getCapacity(existingPod.GetPod())
					}
					assignedSizes[n.Node().Name] = nodeSize
				}
				assignments := make([]fwk.ProposedAssignment, 0)

				nodeIdx := 0
				numNodes := len(tt.nodes)
				for _, p := range tt.preemptor.Members() {
					pSize := getCapacity(p)
					for step := range numNodes {
						i := (nodeIdx + step) % numNodes
						n := tt.nodes[i]
						nodeCapacity := tt.nodeCapacities[i].capacity
						nodeSize := assignedSizes[n.GetName()]
						if nodeSize+pSize <= nodeCapacity {
							assignments = append(assignments, &mockProposedAssignment{
								pod:        p,
								nodeName:   n.GetName(),
								cycleState: framework.NewCycleState(),
							})
							assignedSizes[n.GetName()] += pSize
							nodeIdx = (i + 1) % numNodes
							break
						}
					}
				}
				if len(assignments) >= minCount {
					return &fwk.PodGroupAssignments{ProposedAssignments: assignments}, fwk.NewStatus(fwk.Success)
				}
				return nil, fwk.NewStatus(fwk.Unschedulable)
			}

			podGroups := make(map[string]*schedulingv1beta1.PodGroup)
			for _, pg := range tt.initPodGroups {
				podGroups[pg.Name] = pg
			}

			pgLister := &mockPodGroupLister{podGroups: podGroups}
			pl := &PodGroupEvaluator{
				Handle:           f,
				podGroupSnapshot: pgLister,
			}

			if err := pl.Handle.MutableSnapshotSharedLister().StartMutations(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			domain, err := newDomainForWorkloadPreemption(logger, snapshot, pgLister, &mockCompositePodGroupLister{}, "test-domain")
			if err != nil {
				t.Fatalf("Failed to create domain: %v", err)
			}
			res, gotStatus := pl.selectVictimsOnDomain(ctx, tt.preemptor, domain, tt.pdbs, mockSchedulingFunc)
			if !gotStatus.IsSuccess() {
				t.Logf("SelectVictimsOnDomain failed: %v", gotStatus.Message())
			}
			if err := pl.Handle.MutableSnapshotSharedLister().EndMutations(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			wantCode := tt.expectedStatus.Code()
			gotCode := gotStatus.Code()
			if gotCode != wantCode {
				t.Errorf("Status mismatch. Want %v, Got %v", wantCode, gotCode)
			}
			if wantCode != fwk.Success {
				return
			}
			if res == nil {
				t.Fatalf("expected non-nil victims on success")
			}

			gotNames := sets.Set[string]{}
			for _, p := range res.victims.Pods {
				gotNames.Insert(p.Name)
			}
			wantNames := sets.New(tt.expectedVictims...)
			if diff := cmp.Diff(wantNames, gotNames); diff != "" {
				t.Errorf("Victims mismatch (-want +got):\n%s", diff)
			}
			if res.numPodGroupDisruptions != tt.expectedNumPodGroupDisruptions {
				t.Errorf("numPodGroupDisruptions mismatch. Want %d, Got %d", tt.expectedNumPodGroupDisruptions, res.numPodGroupDisruptions)
			}
			if res.victims.NumPDBViolations != int64(tt.expectedNumPDBViolations) {
				t.Errorf("NumPDBViolations mismatch. Want %d, Got %d", tt.expectedNumPDBViolations, res.victims.NumPDBViolations)
			}
		})
	}
}

// TestPodGroupEvaluator_SelectVictimsOnDomain_NominatedNodes 校验选择受害者时会考虑
// 已提名的节点，避免抢占者落到已被其他 Pod 提名占用的位置。
func TestPodGroupEvaluator_SelectVictimsOnDomain_NominatedNodes(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
	logger, ctx := ktesting.NewTestContext(t)

	p1 := st.MakePod().Name("p1").UID("p1").Obj()
	p2 := st.MakePod().Name("p2").UID("p2").Obj()

	preemptor := makePodGroupPreemptor(
		st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
		[]*v1.Pod{p1, p2},
	)

	node1 := st.MakeNode().Name("node1").Obj()
	domainNodes := []fwk.NodeInfo{
		framework.NewNodeInfo(),
	}
	domainNodes[0].SetNode(node1)

	// 放一个低优先级 Pod 作为潜在受害者，满足后续检查的前置条件。
	p3 := st.MakePod().Name("p3").UID("p3").Node("node1").Priority(lowPriority).Obj()
	podInfo, _ := framework.NewPodInfo(p3)
	domainNodes[0].AddPodInfo(podInfo)
	objs := []runtime.Object{p1, p2, p3, node1}
	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
	}
	snapshot := internalcache.NewSnapshot([]*v1.Pod{p3}, []*v1.Node{node1})
	f, err := tf.NewFramework(
		ctx,
		registeredPlugins, "",
		frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
		frameworkruntime.WithLogger(logger),
	)
	if err != nil {
		t.Fatal(err)
	}

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	pgLister := &mockPodGroupLister{podGroups: make(map[string]*schedulingv1beta1.PodGroup)}
	domain, err := newDomainForWorkloadPreemption(logger, snapshot, pgLister, &mockCompositePodGroupLister{}, "test-domain")
	if err != nil {
		t.Fatalf("Failed to create domain: %v", err)
	}

	mockSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
		cs1 := framework.NewCycleState()
		cs2 := framework.NewCycleState()
		f.RunPreFilterPlugins(ctx, cs1, p1)
		f.RunPreFilterPlugins(ctx, cs2, p2)
		return &fwk.PodGroupAssignments{
			ProposedAssignments: []fwk.ProposedAssignment{
				&mockProposedAssignment{pod: p1, nodeName: "node1", cycleState: cs1},
				&mockProposedAssignment{pod: p2, nodeName: "", cycleState: cs2},
			},
		}, fwk.NewStatus(fwk.Success)
	}

	pl := &PodGroupEvaluator{Handle: f}

	if err := pl.Handle.MutableSnapshotSharedLister().StartMutations(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	result, gotStatus := pl.selectVictimsOnDomain(ctx, preemptor, domain, nil, mockSchedulingFunc)
	if err := pl.Handle.MutableSnapshotSharedLister().EndMutations(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !gotStatus.IsSuccess() {
		t.Fatalf("SelectVictimsOnDomain failed: %v", gotStatus.Message())
	}

	if result == nil {
		t.Fatalf("expected non-nil result")
	}

	if len(result.nominatedNodeNames) != 1 {
		t.Errorf("Expected 1 nominated node name, got %d", len(result.nominatedNodeNames))
	}

	namespacedName := types.NamespacedName{Namespace: p1.Namespace, Name: p1.Name}
	if info, ok := result.nominatedNodeNames[namespacedName]; !ok || info.NominatedNodeName != "node1" {
		t.Errorf("Expected p1 to be nominated for node1, got %v", info)
	}
}

// TestPodGroupEvaluator_Preempt 端到端校验 PodGroup 抢占流程：
// 选出受害者后驱逐，并验证抢占者最终能按 gang 约束落地。
func TestPodGroupEvaluator_Preempt(t *testing.T) {
	tests := []struct {
		name               string
		nodes              []*v1.Node
		initPods           []*v1.Pod
		initPodGroups      []*schedulingv1beta1.PodGroup
		preemptorPodGroup  *schedulingv1beta1.PodGroup
		preemptorPods      []*v1.Pod
		expectedStatus     *fwk.Status
		expectedNominating map[types.NamespacedName]*fwk.NominatingInfo
	}{
		{
			name: "Preemptor group returns success with current nominating infos if member has nominated node with terminating pods",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(lowPriority).Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
			},
			preemptorPodGroup: st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
			preemptorPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Priority(highPriority).Obj(),
				st.MakePod().Name("p2").UID("p2").Priority(highPriority).NominatedNodeName("node1").Obj(),
			},
			expectedStatus: fwk.NewStatus(fwk.Success, "ongoing preemption on nominated nodes"),
			expectedNominating: map[types.NamespacedName]*fwk.NominatingInfo{
				{Namespace: "", Name: "p1"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: ""},
				{Namespace: "", Name: "p2"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: "node1"},
			},
		},
		{
			name: "Preemptor group returns success with current nominating infos if nominated node has terminating pod belonging to a pod group of lower priority",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(highPriority).PodGroupName("victim-pg").Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Priority(lowPriority).DisruptionModeAll().Obj(),
			},
			preemptorPodGroup: st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
			preemptorPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Priority(highPriority).Obj(),
				st.MakePod().Name("p2").UID("p2").Priority(highPriority).NominatedNodeName("node1").Obj(),
			},
			expectedStatus: fwk.NewStatus(fwk.Success, "ongoing preemption on nominated nodes"),
			expectedNominating: map[types.NamespacedName]*fwk.NominatingInfo{
				{Namespace: "", Name: "p1"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: ""},
				{Namespace: "", Name: "p2"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: "node1"},
			},
		},
		{
			name: "Preemptor group returns success with current nominating infos if nominated node has terminating pod belonging to a pod group of lower priority with DisruptionModePod",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(highPriority).PodGroupName("victim-pg").Condition(v1.DisruptionTarget, v1.ConditionTrue, v1.PodReasonPreemptionByScheduler).Terminating().Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("victim-pg").UID("victim-pg").Priority(lowPriority).DisruptionModeSingle().Obj(),
			},
			preemptorPodGroup: st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
			preemptorPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Priority(highPriority).Obj(),
				st.MakePod().Name("p2").UID("p2").Priority(highPriority).NominatedNodeName("node1").Obj(),
			},
			expectedStatus: fwk.NewStatus(fwk.Success, "ongoing preemption on nominated nodes"),
			expectedNominating: map[types.NamespacedName]*fwk.NominatingInfo{
				{Namespace: "", Name: "p1"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: ""},
				{Namespace: "", Name: "p2"}: {NominatingMode: fwk.ModeOverride, NominatedNodeName: "node1"},
			},
		},
		{
			name: "PodGroup preemptor with PreemptNever policy does not perform preemption",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptorPodGroup: st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).PreemptionPolicy(schedulingv1beta1.PreemptNever).Obj(),
			preemptorPods: []*v1.Pod{
				st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj(),
				st.MakePod().Name("p-2").UID("p-2").Priority(highPriority).Obj(),
			},
			expectedStatus: fwk.NewStatus(fwk.Unschedulable, "not eligible due to preemptionPolicy=Never."),
		},
		{
			name: "PodGroup preemptor with PreemptNever preemption policy does not perform preemption with PodGroup victims",
			nodes: []*v1.Node{
				st.MakeNode().Name("node1").Obj(),
				st.MakeNode().Name("node2").Obj(),
				st.MakeNode().Name("node3").Obj(),
			},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			initPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").UID("pg1").DisruptionModeSingle().Priority(lowPriority).Obj(),
				st.MakePodGroup().Name("pg2").UID("pg2").DisruptionModeSingle().Priority(lowPriority).Obj(),
			},
			preemptorPodGroup: st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).PreemptionPolicy(schedulingv1beta1.PreemptNever).Obj(),
			preemptorPods: []*v1.Pod{
				st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj(),
				st.MakePod().Name("p-2").UID("p-2").Priority(highPriority).Obj(),
			},
			expectedStatus: fwk.NewStatus(fwk.Unschedulable, "not eligible due to preemptionPolicy=Never."),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodGroupPreemptionPolicy, true)
			logger, ctx := ktesting.NewTestContext(t)

			var objs []runtime.Object
			for _, p := range append(tt.initPods, tt.preemptorPods...) {
				objs = append(objs, p)
			}
			for _, n := range tt.nodes {
				objs = append(objs, n)
			}
			informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
			registeredPlugins := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			}
			snapshot := internalcache.NewTestSnapshotWithPodGroups(tt.initPods, tt.nodes, tt.initPodGroups)
			f, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithMutableSnapshotLister(snapshot),
				frameworkruntime.WithLogger(logger),
			)
			if err != nil {
				t.Fatal(err)
			}

			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			podGroups := make(map[string]*schedulingv1beta1.PodGroup)
			for _, pg := range tt.initPodGroups {
				podGroups[pg.Name] = pg
			}

			pgLister := &mockPodGroupLister{podGroups: podGroups}
			pl := &PodGroupEvaluator{
				Handle:                         f,
				podGroupSnapshot:               pgLister,
				pdbLister:                      informerFactory.Policy().V1().PodDisruptionBudgets().Lister(),
				enablePodGroupPreemptionPolicy: true,
			}

			mockSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
				t.Fatal("podGroupSchedulingFunc should not be called when ongoing preemption is detected")
				return nil, fwk.NewStatus(fwk.Error)
			}

			pgInfo := &testPodGroupInfo{
				pg:   tt.preemptorPodGroup,
				pods: tt.preemptorPods,
			}

			gotResult, gotStatus := pl.Preempt(ctx, pgInfo, mockSchedulingFunc)
			if gotStatus.Code() != tt.expectedStatus.Code() || gotStatus.Message() != tt.expectedStatus.Message() {
				t.Errorf("Status mismatch. Want status code %v with message %q, Got code %v with message %q",
					tt.expectedStatus.Code(), tt.expectedStatus.Message(), gotStatus.Code(), gotStatus.Message())
			}
			if gotStatus.Code() != fwk.Success {
				return
			}

			if gotResult == nil {
				t.Fatalf("expected non-nil result")
			}

			if diff := cmp.Diff(tt.expectedNominating, gotResult.NominatingInfos); diff != "" {
				t.Errorf("NominatingInfos mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type mockProposedAssignment struct {
	nodeName   string
	pod        *v1.Pod
	cycleState fwk.CycleState
}

// GetNodeName 返回模拟提议分配的目标节点名。
func (pa *mockProposedAssignment) GetNodeName() string {
	return pa.nodeName
}

// GetPod 返回模拟提议分配对应的 Pod。
func (pa *mockProposedAssignment) GetPod() *v1.Pod {
	return pa.pod
}

// GetPodInfo 返回模拟提议分配的 PodInfo。
func (pa *mockProposedAssignment) GetPodInfo() fwk.PodInfo {
	podInfo, _ := framework.NewPodInfo(pa.pod)
	return podInfo
}

// GetCycleState 返回模拟提议分配的 CycleState。
func (pa *mockProposedAssignment) GetCycleState() fwk.CycleState {
	return pa.cycleState
}

// TestPodGroupPreemptionEvaluationDurationMetric 校验抢占评估耗时指标按结果标签记录。
func TestPodGroupPreemptionEvaluationDurationMetric(t *testing.T) {
	nodeName := "node1"
	preemptorPod := st.MakePod().Name("p1").UID("p1").Obj()
	preemptor := newPodGroupPreemptor(
		&testPodGroupInfo{
			pg:   st.MakePodGroup().Name("preemptor-pg").Priority(highPriority).Obj(),
			pods: []*v1.Pod{preemptorPod},
		},
		false,
	)

	tests := []struct {
		name             string
		evaluationStatus *fwk.Status
	}{
		{
			name:             "scheduling success",
			evaluationStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:             "scheduling error",
			evaluationStatus: fwk.NewStatus(fwk.Error, "failed to schedule"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRegistry := componentmetrics.NewKubeRegistry()
			testRegistry.MustRegister(metrics.PreemptionEvaluationDuration)

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			node := st.MakeNode().Name(nodeName).Obj()
			victimPod := st.MakePod().Name("p2").UID("p2").Node(nodeName).Priority(lowPriority).Obj()

			objs := []runtime.Object{node, victimPod, preemptorPod}
			informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
			registeredPlugins := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			}
			snapshot := internalcache.NewSnapshot([]*v1.Pod{victimPod}, []*v1.Node{node})
			fh, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithMutableSnapshotLister(snapshot),
				frameworkruntime.WithLogger(logger),
			)
			if err != nil {
				t.Fatal(err)
			}
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			pgLister := &mockPodGroupLister{podGroups: make(map[string]*schedulingv1beta1.PodGroup)}

			pl := &PodGroupEvaluator{
				Handle:           fh,
				podGroupSnapshot: pgLister,
				pdbLister:        nil,
			}

			mockSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
				cycleState := framework.NewCycleState()
				fh.RunPreFilterPlugins(ctx, cycleState, preemptorPod)
				if tt.evaluationStatus.IsSuccess() {
					return &fwk.PodGroupAssignments{
						ProposedAssignments: []fwk.ProposedAssignment{
							&mockProposedAssignment{pod: preemptorPod, nodeName: nodeName, cycleState: cycleState},
						},
					}, tt.evaluationStatus
				}
				return nil, tt.evaluationStatus
			}
			domain, err := newDomainForWorkloadPreemption(logger, snapshot, pgLister, &mockCompositePodGroupLister{}, "test-domain")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedStatus := tt.evaluationStatus.Code().String()
			stateBefore := captureEvaluationDurationMetric(testRegistry, "podgroup", expectedStatus)

			if err := pl.Handle.MutableSnapshotSharedLister().StartMutations(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pl.evaluate(ctx, preemptor, domain, mockSchedulingFunc)
			if err := pl.Handle.MutableSnapshotSharedLister().EndMutations(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			stateAfter := captureEvaluationDurationMetric(testRegistry, "podgroup", expectedStatus)

			diff := stateAfter.count - stateBefore.count
			if diff != 1 {
				t.Errorf("Expected %s count delta to be 1, got %d", expectedStatus, diff)
			}
		})
	}
}
