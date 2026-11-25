/*
Copyright 2024 The Kubernetes Authors.

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

package queueing

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndftesting "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	"k8s.io/component-helpers/storage/volume"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

type rejectingPhase string

const (
	rejectingPhasePreEnqueue      rejectingPhase = "PreEnqueue"
	rejectingPhaseSchedulingCycle rejectingPhase = "SchedulingCycle"
)

type CoreResourceEnqueueTestCase struct {
	Name string
	// InitialNodes 是最先要创建的 Node 列表。
	InitialNodes []*v1.Node
	// InitialPods 是首先创建的 Pod 列表（如果不为空）。
	// 注意调度器不会调度这些 Pod，
	// 也就是说，这些 Pod 基本上应该已经被调度过；它们应具有 .spec.nodename。
	InitialPods []*v1.Pod
	// InitialPVCs 是首先创建的 PersistentVolumeClaim 列表。
	// 注意 PV 会随 PVC 自动创建。
	// 此外，PVC 的命名空间会自动填充。
	InitialPVCs []*v1.PersistentVolumeClaim
	// InitialPVs 是首先创建的 PersistentVolume 列表。
	InitialPVs []*v1.PersistentVolume
	// InitialStorageClasses 是首先创建的 StorageClass 列表。
	InitialStorageClasses []*storagev1.StorageClass
	// InitialCSINodes 是首先创建的 CSINode 列表。
	InitialCSINodes []*storagev1.CSINode
	// InitialCSIDrivers 是首先创建的 CSIDriver 列表。
	InitialCSIDrivers []*storagev1.CSIDriver
	// InitialStorageCapacities 是首先创建的 CSIStorageCapacity 列表。
	InitialStorageCapacities []*storagev1.CSIStorageCapacity
	// InitialVolumeAttachment 是首先创建的 VolumeAttachment 列表。
	InitialVolumeAttachment []*storagev1.VolumeAttachment
	// InitialDeviceClasses 是首先创建的 DeviceClass 列表。
	InitialDeviceClasses []*resourceapi.DeviceClass
	// InitialPodGroups 是首先创建的 PodGroup 列表。
	InitialPodGroups []*schedulingapi.PodGroup
	// Pods 是要创建的 Pod 列表。
	// 预期它们最初都是不可调度的。
	Pods []*v1.Pod
	// ExpectedInitialRejectingPhase 指定 Pod 最初应以何种方式被拒绝。
	// rejectingPhase 可以是 "PreEnqueue" 或 "SchedulingCycle"。取值含义如下：
	// - "PreEnqueue"：测试用例等待 Pod 被调度器观察到，
	//   并因阻塞在 PreEnqueue 门控处而被放入不可调度 Pod 池。
	// - "SchedulingCycle"：测试用例让 Pod 经历一次调度周期，
	//   被 PreFilter 或 Filter 门控拒绝。
	// 默认为 "SchedulingCycle"。
	ExpectedInitialRejectingPhase rejectingPhase
	// TriggerFn 是触发事件以移动 Pod 的函数。
	// 它返回一个 map，键为该函数要触发的 ClusterEvent，
	// 值为该事件被触发的次数。
	TriggerFn func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error)
	// WantRequeuedPods 是预期在 triggerFn 之后被重新入队的 Pod 集合。
	WantRequeuedPods sets.Set[string]
	// EnablePlugins 是要启用的插件列表。
	// PrioritySort 和 DefaultPreemption 默认启用，因为框架需要它们。
	// 如果为空，则启用所有插件。
	EnablePlugins []string
	// EnableDRAExtendedResource 表示测试用例是否应在 feature gate
	// DRAExtendedResource 启用的情况下运行。
	EnableDRAExtendedResource bool
	// EnableNodeDeclaredFeatures 表示测试用例是否在 NodeDeclaredFeatures feature gate 启用的情况下运行。
	EnableNodeDeclaredFeatures bool
	// EnableGenericWorkload 表示测试用例是否应在 feature gate
	// GenericWorkload 启用的情况下运行。
	EnableGenericWorkload bool
}

// 我们将所有测试用例集中定义在一处，
// 这样做是因为一次性运行所有这些测试用例会导致超时。
var CoreResourceEnqueueTestCases = []*CoreResourceEnqueueTestCase{
	{
		Name:          "Pod without a required toleration to a node isn't requeued to activeQ",
		EnablePlugins: []string{names.TaintToleration, names.NodeResourcesFit},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 没有所需的 toleration，将被 TaintToleration 插件拒绝。
			//   （TaintToleration 插件在 NodeResourcesFit 插件之前评估。）
			// - Pod2 有所需的 toleration，但请求大量 CPU —— 将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
			st.MakePod().Name("pod2").Toleration(v1.TaintNodeNotReady).Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 通过增加 CPU 容量触发 NodeChange 事件。
			// 这使 Pod2 变为可调度。
			// Pod1 不会被重新入队，因为节点仍未就绪且其没有所需的 toleration。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected by the PodAffinity plugin is requeued when a new Node is created",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Label("anti", "anti").Name("pod1").PodAntiAffinityExists("anti", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod2 将被 PodAffinity 插件拒绝。
			st.MakePod().Label("anti", "anti").Name("pod2").PodAntiAffinityExists("anti", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			node := st.MakeNode().Name("fake-node2").Label("node", "fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected by the NodeAffinity plugin is requeued when a Node's label is updated",
		EnablePlugins: []string{names.NodeAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Label("group", "a").Obj()},
		Pods: []*v1.Pod{
			// - Pod1 将被 NodeAffinity 插件拒绝。
			st.MakePod().Name("pod1").NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
			// - Pod2 将被 NodeAffinity 插件拒绝。
			st.MakePod().Name("pod2").NodeAffinityIn("group", []string{"c"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件以更改标签。
			// 这会使 pod1 被重新入队。
			// 这不会使 pod2 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("fake-node1").Label("group", "b").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeAffinity plugin is not requeued when an updated Node haven't changed the 'match' verdict",
		EnablePlugins: []string{names.NodeAffinity, names.NodeResourcesFit},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("node1").Label("group", "a").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj(),
			st.MakeNode().Name("node2").Label("group", "b").Obj()},
		Pods: []*v1.Pod{
			// - 初始 Pod 在 node1 上会被 NodeAffinity 插件接受，但会被 NodeResources 插件阻止。
			// - 该 Pod 在 node2 上会被 NodeAffinity 插件阻止，因此我们知道对两个测试节点都会查询 NodeAffinity 的 qhint。
			st.MakePod().Name("pod1").NodeAffinityIn("group", []string{"a"}, st.NodeSelectorTypeMatchExpressions).Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件以添加新标签。
			// 这不会导致 Pod 被重新入队，因为在更新之前已经存在匹配，这意味着该插件并未阻碍调度。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("node1").Label("group", "a").Label("node", "fake-node").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected by the NodeAffinity plugin is requeued when a Node is added",
		EnablePlugins: []string{names.NodeAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Label("group", "a").Obj()},
		Pods: []*v1.Pod{
			// - Pod1 将被 NodeAffinity 插件拒绝。
			st.MakePod().Name("pod1").NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
			// - Pod2 将被 NodeAffinity 插件拒绝。
			st.MakePod().Name("pod2").NodeAffinityIn("group", []string{"c"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发带有预期标签的 NodeAdd 事件。
			// 这会使 pod1 被重新入队。
			// 这不会使 pod2 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, st.MakeNode().Name("fake-node2").Label("group", "b").Obj(), metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod updated with toleration requeued to activeQ",
		EnablePlugins: []string{names.TaintToleration},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Taints([]v1.Taint{{Key: "taint-key", Effect: v1.TaintEffectNoSchedule}}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 没有所需的 toleration，将被 TaintToleration 插件拒绝。
			st.MakePod().Name("pod1").Container("image").Obj(),
			st.MakePod().Name("pod2").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 通过为 Pod1 添加 toleration 触发 PodUpdate 事件。
			// 这使 Pod1 变为可调度。Pod2 因没有 toleration 而不会被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, st.MakePod().Name("pod1").Container("image").Toleration("taint-key").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.UnscheduledPod, ActionType: fwk.UpdatePodToleration}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the TaintToleration plugin is requeued when the Node's taint is updated",
		EnablePlugins: []string{names.TaintToleration},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1、pod2 和 pod3 没有所需的 toleration，将被 TaintToleration 插件拒绝。
			st.MakePod().Name("pod1").Toleration("taint-key").Container("image").Obj(),
			st.MakePod().Name("pod2").Toleration("taint-key2").Container("image").Obj(),
			st.MakePod().Name("pod3").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，将现有污点更改为与 pod1 的 toleration 匹配的污点。
			// 这使 Pod1 变为可调度。Pod2 和 pod3 因没有对应 toleration 而不会被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("fake-node").Taints([]v1.Taint{{Key: "taint-key", Effect: v1.TaintEffectNoSchedule}}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the Node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the TaintToleration plugin is requeued when a Node that has the correspoding taint is added",
		EnablePlugins: []string{names.TaintToleration},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 和 Pod2 没有所需的 toleration，将被 TaintToleration 插件拒绝。
			st.MakePod().Name("pod1").Toleration("taint-key").Container("image").Obj(),
			st.MakePod().Name("pod2").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发带有该污点的 NodeCreated 事件。
			// 这使 Pod1 变为可调度。Pod2 因没有 toleration 而不会被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, st.MakeNode().Name("fake-node2").Taints([]v1.Taint{{Key: "taint-key", Effect: v1.TaintEffectNoSchedule}}).Obj(), metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create the Node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when the Pod is updated to scale down",
		EnablePlugins: []string{names.NodeResourcesFit},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 请求大量 CPU，将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 通过减少 pod1 请求的 CPU 来触发 PodUpdate 事件。
			// 这使 Pod1 变为可调度。
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).UpdateResize(testCtx.Ctx, "pod1", st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to resize the pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.UnscheduledPod, ActionType: fwk.UpdatePodScaleDown}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Pod is deleted",
		EnablePlugins: []string{names.NodeResourcesFit},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod2 的请求将被拒绝，因为节点上没有足够的空闲资源
			st.MakePod().Name("pod2").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发已分配的 Pod1 删除事件。
			// 这使 Pod2 变为可调度。
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodDelete: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Node is created",
		EnablePlugins: []string{names.NodeResourcesFit},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 请求大量 CPU，将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
			// - Pod2 请求大量 CPU，将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod2").Req(map[v1.ResourceName]string{v1.ResourceCPU: "5"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeCreated 事件。
			// 这使 Pod1 变为可调度。Pod2 因请求过高而不会被重新入队。
			node := st.MakeNode().Name("fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Node is updated",
		EnablePlugins: []string{names.NodeResourcesFit},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 请求大量 CPU，将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Container("image").Obj(),
			// - Pod2 请求大量 CPU，将被 NodeResourcesFit 插件拒绝。
			st.MakePod().Name("pod2").Req(map[v1.ResourceName]string{v1.ResourceCPU: "5"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node 的 CPU 容量。
			// 这使 Pod1 变为可调度。Pod2 因请求过高而不会被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin isn't requeued when a Node has the extended resource, and DRAExtendedResource is disabled",
		EnablePlugins: []string{names.NodeResourcesFit, names.DynamicResources},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/gpu": "1"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求节点上不可用的 example.com/gpu 扩展资源。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{v1.ResourceCPU: "5", "example.com/gpu": "1"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上无关的（未被请求的）内存容量，这不应使 Pod1 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", v1.ResourceMemory: "4000"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods:          sets.Set[string]{},
		EnableDRAExtendedResource: false,
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin isn't requeued when a Node has the extended resource, and DRAExtendedResource is enabled",
		EnablePlugins: []string{names.NodeResourcesFit, names.DynamicResources},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/gpu": "1"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求节点上不可用的 example.com/gpu 扩展资源。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{v1.ResourceCPU: "5", "example.com/gpu": "1"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上无关的（未被请求的）内存容量，这不应使 Pod1 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", v1.ResourceMemory: "4000"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods:          sets.Set[string]{},
		EnableDRAExtendedResource: true,
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin isn't requeued when a Node does not have the extended resource, and DRAExtendedResource is disabled",
		EnablePlugins: []string{names.NodeResourcesFit, names.DynamicResources},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求节点上不可用的 example.com/gpu 扩展资源。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{"example.com/gpu": "1"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上无关的（未被请求的）内存容量，这不应使 Pod1 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", v1.ResourceMemory: "4000"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods:          sets.Set[string]{},
		EnableDRAExtendedResource: false,
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Node does not have the extended resource, and DRAExtendedResource is enabled",
		EnablePlugins: []string{names.NodeResourcesFit, names.NodeAffinity, names.DynamicResources},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
			st.MakeNode().Name("fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Label("group", "b").Obj(),
		},
		InitialDeviceClasses: []*resourceapi.DeviceClass{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gpu-device-class",
				},
				Spec: resourceapi.DeviceClassSpec{
					ExtendedResourceName: ptr.To("example.com/gpu"),
				},
			},
		},
		Pods: []*v1.Pod{
			// - Pod1 请求了可用数量的 CPU（在 fake-node1 上），但会被 NodeAffinity 插件拒绝。注意，NodeResourcesFit 插件会注册 QHint，因为它拒绝了 fake-node2。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/gpu": "1"}).NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上无关的（未被请求的）内存容量，这不应使 Pod1 被重新入队，
			// 但它仍被重新入队，因为 DynamicResourceAllocation 插件可能会处理该扩展资源。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", v1.ResourceMemory: "4000"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods:          sets.New("pod1"),
		EnableDRAExtendedResource: true,
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when created DeviceClass having the extended resource matching pod's requests, and DRAExtendedResource is enabled",
		EnablePlugins: []string{names.NodeResourcesFit},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求了可用数量的 CPU（在 fake-node1 上），但因缺少扩展资源 example.com/gpu 而会被拒绝。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/gpu": "1"}).Container("image").Obj(),
			st.MakePod().Name("pod2").Res(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/othergpu": "1"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 DeviceClass 创建事件，其扩展资源名称与 Pod 的资源请求相匹配。
			if _, err := testCtx.ClientSet.ResourceV1().DeviceClasses().Create(testCtx.Ctx, &resourceapi.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: "fake-class"}, Spec: resourceapi.DeviceClassSpec{ExtendedResourceName: ptr.To("example.com/gpu")}}, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create the fake-class: %w", err)
			}

			return map[fwk.ClusterEvent]uint64{{Resource: fwk.DeviceClass, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods:          sets.New("pod1"),
		EnableDRAExtendedResource: true,
	},
	{
		Name:                 "Pod rejected by the NodeResourcesFit plugin is requeued when updated DeviceClass has the extended resource, and DRAExtendedResource is enabled",
		EnablePlugins:        []string{names.NodeResourcesFit},
		InitialDeviceClasses: []*resourceapi.DeviceClass{{ObjectMeta: metav1.ObjectMeta{Name: "fake-class"}, Spec: resourceapi.DeviceClassSpec{ExtendedResourceName: nil}}},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求了可用数量的 CPU（在 fake-node1 上），但因缺少扩展资源 example.com/gpu 而会被拒绝。
			st.MakePod().Name("pod1").Res(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/gpu": "1"}).Container("image").Obj(),
			st.MakePod().Name("pod2").Res(map[v1.ResourceName]string{v1.ResourceCPU: "4", "example.com/othergpu": "1"}).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 DeviceClass 更新事件，添加与 Pod 资源请求相匹配的扩展资源名称。
			if _, err := testCtx.ClientSet.ResourceV1().DeviceClasses().Update(testCtx.Ctx, &resourceapi.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: "fake-class"}, Spec: resourceapi.DeviceClassSpec{ExtendedResourceName: ptr.To("example.com/gpu")}}, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the fake-class: %w", err)
			}

			return map[fwk.ClusterEvent]uint64{{Resource: fwk.DeviceClass, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods:          sets.New("pod1"),
		EnableDRAExtendedResource: true,
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin isn't requeued when a Node is updated without increase in the requested resources",
		EnablePlugins: []string{names.NodeResourcesFit, names.NodeAffinity},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
			st.MakeNode().Name("fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Label("group", "b").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求了可用数量的 CPU（在 fake-node1 上），但会被 NodeAffinity 插件拒绝。注意，NodeResourcesFit 插件会注册 QHint，因为它拒绝了 fake-node2。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上无关的（未被请求的）内存容量，这不应使 Pod1 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4", v1.ResourceMemory: "4000"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Node is updated with increase in the requested resources",
		EnablePlugins: []string{names.NodeResourcesFit, names.NodeAffinity},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
			st.MakeNode().Name("fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Label("group", "b").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 请求了可用数量的 CPU（在 fake-node1 上），但会被 NodeAffinity 插件拒绝。注意，NodeResourcesFit 插件会注册 QHint，因为它拒绝了 fake-node2。
			st.MakePod().Name("pod1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 上被请求的 CPU 容量，这应使 Pod1 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "5"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeResourcesFit plugin is requeued when a Node is updated with increase in the allowed pods number",
		EnablePlugins: []string{names.NodeResourcesFit, names.NodeAffinity},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourcePods: "2"}).Obj(),
			st.MakeNode().Name("fake-node2").Capacity(map[v1.ResourceName]string{v1.ResourcePods: "1"}).Label("group", "b").Obj(),
		},
		InitialPods: []*v1.Pod{
			// - 由于亲和性标签，Pod1 将被调度到 fake-node2。
			st.MakePod().Name("pod1").NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Node("fake-node2").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod2 不可调度，因为 Pod1 占满了 fake-node2 的 ResourcePods 限制。注意，NodeResourcesFit 插件会注册 QHint，因为它拒绝了 fake-node2。
			st.MakePod().Name("pod2").NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，增加 fake-node1 允许的 Pod 数量，这应使 Pod2 被重新入队。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node1").Capacity(map[v1.ResourceName]string{v1.ResourcePods: "3"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Updating pod label doesn't retry scheduling if the Pod was rejected by TaintToleration",
		EnablePlugins: []string{names.TaintToleration},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj()},
		Pods: []*v1.Pod{
			// - Pod1 没有所需的 toleration，将被 TaintToleration 插件拒绝。
			st.MakePod().Name("pod1").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, st.MakePod().Name("pod1").Label("key", "val").Container("image").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.UnscheduledPod, ActionType: fwk.UpdatePodLabel}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		// 该测试用例确保返回 PreFilterResult 的 PreFilter 插件也会被插入到 pInfo.UnschedulablePlugins 中，
		// 也就是说，在队列的重新入队流程中会考虑它们。
		// https://github.com/kubernetes/kubernetes/issues/122018
		Name:          "Pod rejected by the PreFilter of NodeAffinity plugin and Filter of NodeResourcesFit is requeued based on both plugins",
		EnablePlugins: []string{names.NodeAffinity, names.NodeResourcesFit},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node").Label("node", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj(),
			st.MakeNode().Name("fake-node2").Label("node", "fake-node2").Label("zone", "zone1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod1 将被 NodeAffinity 插件和 NodeResourcesFit 插件拒绝。
			st.MakePod().Label("unscheduled", "plugins").Name("pod1").NodeAffinityIn("metadata.name", []string{"fake-node"}, st.NodeSelectorTypeMatchFields).Req(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 我们将触发 UpdateNodeAllocatable 和 UpdateNodeLabel 事件，
			// 它们分别对应 NodeResourcesFit 和 NodeAffinity 插件。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, st.MakeNode().Name("fake-node").Label("metadata.name", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4"}).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}

			return map[fwk.ClusterEvent]uint64{
				{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable}: 1,
				{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}:       1,
			}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when deleting the existed pod's label that matches the podAntiAffinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("anti1", "anti1").Label("anti2", "anti2").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod2 和 pod3 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod2").Label("anti1", "anti1").PodAntiAffinityExists("anti1", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod3").Label("anti2", "anti2").PodAntiAffinityExists("anti2", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
		},

		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 删除 Pod 的 'anti1' 标签，使其匹配 pod2 的 antiAffinity。
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, st.MakePod().Name("pod1").Label("anti2", "anti2").Container("image").Node("fake-node").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when deleting the existing pod that matches the target podAntiAffinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("anti1", "anti1").Container("image").Node("fake-node").Obj(),
			st.MakePod().Name("pod2").Label("anti2", "anti2").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 pod4 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod3").Label("anti1", "anti1").PodAntiAffinityExists("anti1", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("anti2", "anti2").PodAntiAffinityExists("anti2", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
		},

		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 删除 pod1 将使 pod3 可调度，因为 pod3 的 antiAffinity 不再匹配 pod1。
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.AssignedPod, ActionType: fwk.Delete}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when deleting the existing pod with podAntiAffinity that matches the target pod",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").PodAntiAffinityExists("anti1", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Node("fake-node").Obj(),
			st.MakePod().Name("pod2").PodAntiAffinityExists("anti2", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 pod4 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod3").Label("anti1", "anti1").Container("image").Obj(),
			st.MakePod().Name("pod4").Label("anti2", "anti2").Container("image").Obj(),
		},

		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 删除 pod1 将使 pod3 可调度，因为 pod1 的 antiAffinity 不再匹配 pod3。
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.AssignedPod, ActionType: fwk.Delete}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pod rejected by the PodAffinity plugin is requeued when updating the existed pod's label to make it match the pod's podAffinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod2 和 pod3 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod2").PodAffinityExists("aaa", "node", st.PodAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod3").PodAffinityExists("bbb", "node", st.PodAffinityWithRequiredReq).Container("image").Obj(),
		},

		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 为 Pod 添加标签，使其匹配 pod2 的 podAffinity。
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, st.MakePod().Name("pod1").Label("aaa", "bbb").Container("image").Node("fake-node").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},

	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when creating the node with the topologyKey label of the pod affinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Label("aaa", "fake-node").Obj()},
		Pods: []*v1.Pod{
			// - pod1 和 pod2 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod1").PodAffinityExists("bbb", "zone", st.PodAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod2").PodAffinityExists("ccc", "region", st.PodAffinityWithRequiredReq).Container("image").Obj(),
		},

		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 创建节点，使其匹配 pod1 的 podAffinity。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, st.MakeNode().Name("fake-node2").Label("zone", "zone1").Obj(), metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create fake-node2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when updating the node with the topologyKey label of the pod affinity",
		EnablePlugins: []string{names.InterPodAffinity, names.NodeAffinity},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node").Label("region", "region-1").Obj(),
			st.MakeNode().Name("fake-node2").Label("region", "region-2").Label("group", "b").Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("affinity1", "affinity1").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - pod2、pod3、pod4 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod2").Label("affinity1", "affinity1").PodAffinityExists("affinity1", "node", st.PodAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod3").Label("affinity1", "affinity1").PodAffinityExists("affinity1", "other", st.PodAffinityWithRequiredReq).Container("image").Obj(),
			// 该 Pod 没有任何标签，因此这个 PodAffinity 不会匹配该 Pod 自身。
			st.MakePod().Name("pod4").PodAffinityExists("affinity1", "region", st.PodAffinityWithRequiredReq).NodeAffinityIn("group", []string{"b"}, st.NodeSelectorTypeMatchExpressions).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 为节点添加 "node" 标签，这可能使 pod2 可调度。
			// 更新节点的 "region" 标签，这可能使 pod4 可调度。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("fake-node").Label("node", "fake-node").Label("region", "region-2").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2", "pod4"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when updating the node with the topologyKey label of the pod anti affinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label("service", "service-a").Label("other", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("anti1", "anti1").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - pod2、pod3、pod4 将被 PodAffinity 插件拒绝。
			st.MakePod().Name("pod2").Label("anti1", "anti1").PodAntiAffinityExists("anti1", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod3").Label("anti1", "anti1").PodAntiAffinityExists("anti1", "service", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("anti1", "anti1").PodAntiAffinityExists("anti1", "other", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 从节点移除 "node" 标签，使其不再匹配 pod2 的 podAntiAffinity。
			// 将 "service" 标签从 service-a 改为 service-b，这可能使 pod3 可调度。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("fake-node").Label("service", "service-b").Label("region", "fake-node").Label("other", "fake-node").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update fake-node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2", "pod3"),
	},
	{
		Name:          "Pod rejected by the InterPodAffinity plugin is requeued when creating the node with the topologyKey label of the pod anti affinity",
		EnablePlugins: []string{names.InterPodAffinity},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Label("zone", "fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("anti1", "anti1").Container("image").Node("fake-node1").Obj(),
		},
		Pods: []*v1.Pod{
			// - pod2 和 pod3 将被 PodAntiAffinity 插件拒绝。
			st.MakePod().Name("pod2").PodAntiAffinityExists("anti1", "zone", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
			st.MakePod().Name("pod3").PodAntiAffinityExists("anti1", "node", st.PodAntiAffinityWithRequiredReq).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 创建节点，使 pod2 和 pod3 可调度。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, st.MakeNode().Name("fake-node2").Label("zone", "fake-node").Obj(), metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create fake-node2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2", "pod3"),
	},
	{
		Name:          "Pod rejected with hostport by the NodePorts plugin is requeued when pod with common hostport is deleted",
		EnablePlugins: []string{names.NodePorts},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}).Node("fake-node").Obj(),
			st.MakePod().Name("pod2").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8081}}).Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod3").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}).Obj(),
			st.MakePod().Name("pod4").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8081}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发已分配的 Pod 删除事件。
			// 因为 Pod1 和 Pod3 使用相同端口，删除 Pod1 会使 Pod3 可调度。
			// 通过将 GracePeriodSeconds 设为 0，使 Pod3 在 Pod1 删除后立即重新入队。
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete Pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodDelete: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pod rejected with hostport by the NodePorts plugin is requeued when new node is created",
		EnablePlugins: []string{names.NodePorts},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}).Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").ContainerPort([]v1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeCreated 事件。
			// 这使 Pod2 变为可调度。
			node := st.MakeNode().Name("fake-node2").Label("node", "fake-node2").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected by the NodeUnschedulable plugin is requeued when the node is turned to ready",
		EnablePlugins: []string{names.NodeUnschedulable},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Unschedulable(true).Obj()},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeUpdate 事件，将节点变为就绪状态。
			// 这使 Pod1 变为可调度。
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, st.MakeNode().Name("fake-node").Unschedulable(false).Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeUnschedulable plugin is requeued when a new node is created",
		EnablePlugins: []string{names.NodeUnschedulable},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Unschedulable(true).Obj()},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeCreated 事件。
			// 这使 Pod1 变为可调度。
			node := st.MakeNode().Name("fake-node2").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the NodeUnschedulable plugin isn't requeued when another unschedulable node is created",
		EnablePlugins: []string{names.NodeUnschedulable},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node1").Unschedulable(true).Obj()},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeCreated 事件，但创建的是不可调度的节点，这不会使 Pod1 可调度。
			node := st.MakeNode().Name("fake-node2").Unschedulable(true).Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a Pod with matching label is scheduled",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").Container("image").Node("fake-node").Obj(),
			st.MakePod().Name("pod2").Label("key2", "val").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key2", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key2").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发已分配的 Pod 添加事件。
			// 只应重新入队 pod3，因为该 Pod 只有 pod3 的 topologyspread 检查的 key1 标签，而没有 pod4 所需的 key2 标签。
			pod := st.MakePod().Name("pod5").Label("key1", "val").Node("fake-node").Container("image").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create Pod %q: %w", pod.Name, err)
			}

			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodAdd: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a scheduled Pod label is updated to match the selector",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").Container("image").Node("fake-node").Obj(),
			st.MakePod().Name("pod2").Label("key2", "val").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key2", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key2").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发已分配的 Pod 更新事件。
			// 只应重新入队 pod3，因为被更新的 Pod 有 key1 标签，
			// 而它只与 pod3 的 topologyspread 所使用的标签选择器相关。
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, st.MakePod().Name("pod1").Label("key3", "val").Container("image").Node("fake-node").Obj(), metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update the pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodUpdate: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a scheduled Pod with matching label is deleted",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "2"}).Obj()},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").Container("image").Node("fake-node").Obj(),
			st.MakePod().Name("pod2").Label("key2", "val").Container("image").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(2)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key2", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key2").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发已分配的 Pod 删除事件。
			// 只应重新入队 pod3，因为该 Pod 只有 pod3 的 topologyspread 检查的 key1 标签，而没有 pod4 所需的 key2 标签。
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete Pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodDelete: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a Node with topology label is created",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Label("node", "fake-node").Obj(),
			st.MakeNode().Name("fake-node2").Label("zone", "fake-zone").Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").Container("image").Node("fake-node1").Obj(),
			st.MakePod().Name("pod2").Label("key1", "val").Container("image").Node("fake-node2").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(2)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(2)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 Node 添加事件。
			// 只应重新入队 pod3，因为该节点只有 node 标签，而没有 pod4 的 topologyspread 所需的 zone 标签。
			node := st.MakeNode().Name("fake-node3").Label("node", "fake-node").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create a new node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod3"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a Node is updated to have the topology label",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Label("node", "fake-node").Label("region", "fake-node").Label("service", "service-a").Obj(),
			st.MakeNode().Name("fake-node2").Label("node", "fake-node").Label("region", "fake-node").Label("service", "service-a").Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node1").Obj(),
			st.MakePod().Name("pod2").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node2").Obj(),
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "region", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node2").Obj(),
			st.MakePod().Name("pod4").Label("key1", "val").SpreadConstraint(1, "service", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node2").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod5、Pod6、Pod7、Pod8、Pod9 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod5").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod6").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod7").Label("key1", "val").SpreadConstraint(1, "region", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod8").Label("key1", "val").SpreadConstraint(1, "other", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod9").Label("key1", "val").SpreadConstraint(1, "service", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 Node 更新事件。
			// 应重新入队 pod5，因为该更新删除了 fake-node2 的 "node" 标签。
			// 应重新入队 pod6，因为该更新为 fake-node2 添加了 "zone" 标签。
			// 不应重新入队 pod7，因为该更新没有改变 "region" 标签的值。
			// 不应重新入队 pod8，因为该更新没有添加 "other" 标签。
			// 应重新入队 pod9，因为该更新改变了 "service" 标签的值。
			node := st.MakeNode().Name("fake-node2").Label("zone", "fake-node").Label("region", "fake-node").Label("service", "service-b").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, node, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod5", "pod6", "pod9"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a Node with a topology label is deleted",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Label("node", "fake-node").Obj(),
			st.MakeNode().Name("fake-node2").Label("zone", "fake-node").Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node1").Obj(),
			st.MakePod().Name("pod2").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node2").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeTaint 删除事件。
			// 只应重新入队 pod4，因为该节点只有 zone 标签，而没有 pod3 所需的 node 标签。
			if err := testCtx.ClientSet.CoreV1().Nodes().Delete(testCtx.Ctx, "fake-node2", metav1.DeleteOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Delete}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod4"),
	},
	{
		Name:          "Pods with PodTopologySpread should be requeued when a NodeTaint of a Node with a topology label has been updated",
		EnablePlugins: []string{names.PodTopologySpread},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Label("node", "fake-node").Obj(),
			st.MakeNode().Name("fake-node2").Label("zone", "fake-node").Obj(),
			st.MakeNode().Name("fake-node3").Label("zone", "fake-node").Taints([]v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule}}).Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node1").Obj(),
			st.MakePod().Name("pod2").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), nil, nil, nil, nil).Container("image").Node("fake-node2").Obj(),
		},
		Pods: []*v1.Pod{
			// - Pod3 和 Pod4 将被 PodTopologySpread 插件拒绝。
			st.MakePod().Name("pod3").Label("key1", "val").SpreadConstraint(1, "node", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Obj(),
			st.MakePod().Name("pod4").Label("key1", "val").SpreadConstraint(1, "zone", v1.DoNotSchedule, st.MakeLabelSelector().Exists("key1").Obj(), ptr.To(int32(3)), nil, nil, nil).Container("image").Toleration("aaa").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 触发 NodeTaint 更新事件。
			// 只应重新入队 pod4，因为该节点只有 zone 标签，而没有 pod3 所需的 node 标签。
			node := st.MakeNode().Name("fake-node3").Label("zone", "fake-node").Taints([]v1.Taint{{Key: "aaa", Value: "bbb", Effect: v1.TaintEffectNoSchedule}}).Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, node, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod4"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is requeued when the PV is added",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pv2 := st.MakePersistentVolume().Name("pv2").Label(v1.LabelTopologyZone, "us-west1-a").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumes().Create(testCtx.Ctx, pv2, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create pv2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolume, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is requeued when the PV is updated",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
			st.MakePersistentVolume().
				Name("pv2").
				Labels(map[string]string{v1.LabelTopologyZone: "us-east1"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pv2 := st.MakePersistentVolume().Name("pv2").Label(v1.LabelTopologyZone, "us-west1-a").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumes().Update(testCtx.Ctx, pv2, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pv2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolume, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is requeued when the PVC bound to the pod is added",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
			st.MakePersistentVolume().
				Name("pv2").
				Labels(map[string]string{v1.LabelTopologyZone: "us-east1"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
			st.MakePod().Name("pod3").Container("image").PVC("pvc3").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pvc2 := st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Create(testCtx.Ctx, pvc2, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to add pvc2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is requeued when the PVC bound to the pod is updated",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
			st.MakePersistentVolume().
				Name("pv2").
				Labels(map[string]string{v1.LabelTopologyZone: "us-east1"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
			st.MakePod().Name("pod3").Container("image").PVC("pvc3").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pvc2 := st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Update(testCtx.Ctx, pvc2, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pvc2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is requeued when the Storage class is added",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			sc1 := st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				Obj()
			if _, err := testCtx.ClientSet.StorageV1().StorageClasses().Create(testCtx.Ctx, sc1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create sc1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.StorageClass, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeZone plugin is not requeued when the PV is updated but the topology is same",
		EnablePlugins: []string{names.VolumeZone},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west1-a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				Labels(map[string]string{v1.LabelTopologyZone: "us-west1-a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
			st.MakePersistentVolume().
				Name("pv2").
				Labels(map[string]string{v1.LabelTopologyZone: "us-east1"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pv2 := st.MakePersistentVolume().Name("pv2").
				Labels(map[string]string{v1.LabelTopologyZone: "us-east1", "unrelated": "unrelated"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumes().Update(testCtx.Ctx, pv2, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pv2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolume, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected by the VolumeRestriction plugin is requeued when the PVC bound to the pod is added",
		EnablePlugins: []string{names.VolumeRestrictions},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pvc2 := st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Create(testCtx.Ctx, pvc2, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to add pvc1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected by the VolumeRestriction plugin is requeued when the pod is deleted",
		EnablePlugins: []string{names.VolumeRestrictions},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc1").Obj(),
			st.MakePod().Name("pod3").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete pod1: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodDelete: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the Node is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-east-1b").Obj(),
		},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				NodeAffinityIn(v1.LabelTopologyZone, []string{"us-east-1a"}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			node := st.MakeNode().Name("fake-node2").Label(v1.LabelTopologyZone, "us-east-1a").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the Node is updated",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes: []*v1.Node{
			st.MakeNode().
				Name("fake-node").
				Label("node", "fake-node").
				Label("aaa", "bbb").
				Obj(),
		},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				NodeAffinityIn("aaa", []string{"ccc"}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			node := st.MakeNode().Name("fake-node").Label("node", "fake-node").Label("aaa", "ccc").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, node, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the PV is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label("aaa", "bbb").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				NodeAffinityIn("aaa", []string{"ccc"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				VolumeName("pv1").
				Annotation(volume.AnnBindCompleted, "true").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.CoreV1().PersistentVolumes().Delete(testCtx.Ctx, "pv1", metav1.DeleteOptions{}); err != nil {
				return nil, fmt.Errorf("failed to delete pv1: %w", err)
			}
			pv1 := st.MakePersistentVolume().
				Name("pv1").
				NodeAffinityIn("aaa", []string{"bbb"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumes().Create(testCtx.Ctx, pv1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create pv: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolume, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the PV is updated",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-east-1a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				NodeAffinityIn(v1.LabelFailureDomainBetaZone, []string{"us-east-1a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				VolumeName("pv1").
				Annotation(volume.AnnBindCompleted, "true").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pv1 := st.MakePersistentVolume().
				Name("pv1").
				NodeAffinityIn(v1.LabelTopologyZone, []string{"us-east-1a"}).
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumes().Update(testCtx.Ctx, pv1, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pv: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolume, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the PVC is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Delete(testCtx.Ctx, "pvc1", metav1.DeleteOptions{}); err != nil {
				return nil, fmt.Errorf("failed to delete pvc1: %w", err)
			}
			pvc1 := st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Create(testCtx.Ctx, pvc1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create pvc: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the PVC is updated",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pvc1 := st.MakePersistentVolumeClaim().
				Name("pvc1").
				VolumeName("pv1").
				Annotation(volume.AnnBindCompleted, "true").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()

			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Update(testCtx.Ctx, pvc1, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update pvc: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the StorageClass is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			sc1 := st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				Obj()
			if _, err := testCtx.ClientSet.StorageV1().StorageClasses().Create(testCtx.Ctx, sc1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create storageclass: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.StorageClass, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the StorageClass's AllowedTopologies is updated",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west-1a").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		InitialStorageClasses: []*storagev1.StorageClass{
			st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				AllowedTopologies([]v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{Key: v1.LabelTopologyZone, Values: []string{"us-west-1c"}},
						},
					},
				}).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			sc1 := st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				AllowedTopologies([]v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{Key: v1.LabelTopologyZone, Values: []string{"us-west-1a"}},
						},
					},
				}).
				Obj()
			if _, err := testCtx.ClientSet.StorageV1().StorageClasses().Update(testCtx.Ctx, sc1, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update storageclass: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.StorageClass, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is not requeued when the StorageClass is updated but the AllowedTopologies is same",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Label(v1.LabelTopologyZone, "us-west-1c").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		InitialStorageClasses: []*storagev1.StorageClass{
			st.MakeStorageClass().
				Name("sc1").
				Label("key", "value").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				AllowedTopologies([]v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{Key: v1.LabelTopologyZone, Values: []string{"us-west-1a"}},
						},
					},
				}).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			sc1 := st.MakeStorageClass().
				Name("sc1").
				Label("key", "updated").
				VolumeBindingMode(storagev1.VolumeBindingWaitForFirstConsumer).
				Provisioner("p").
				AllowedTopologies([]v1.TopologySelectorTerm{
					{
						MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
							{Key: v1.LabelTopologyZone, Values: []string{"us-west-1a"}},
						},
					},
				}).
				Obj()
			if _, err := testCtx.ClientSet.StorageV1().StorageClasses().Update(testCtx.Ctx, sc1, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update storageclass: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.StorageClass, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the CSINode is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().
				Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			csinode1 := st.MakeCSINode().Name("fake-node").Obj()

			if _, err := testCtx.ClientSet.StorageV1().CSINodes().Create(testCtx.Ctx, csinode1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create CSINode: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSINode, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the CSINode's MigratedPluginsAnnotation is updated",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 使用 MakeCSINode 更新 CSINode 时，会因未指定 resourceVersion 而报错。因此这里获取实际的 CSINode。
			csinode, err := testCtx.ClientSet.StorageV1().CSINodes().Get(testCtx.Ctx, "fake-node", metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get CSINode: %w", err)
			}

			metav1.SetMetaDataAnnotation(&csinode.ObjectMeta, v1.MigratedPluginsAnnotationKey, "value")
			if _, err := testCtx.ClientSet.StorageV1().CSINodes().Update(testCtx.Ctx, csinode, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update CSINode: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSINode, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the CSIDriver's StorageCapacity gets disabled",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(true)).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Volume(
				v1.Volume{
					Name: "volume",
					VolumeSource: v1.VolumeSource{
						CSI: &v1.CSIVolumeSource{
							Driver: "csidriver",
						},
					},
				},
			).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 使用 MakeCSIDriver 更新 CSIDriver 时，会因未指定 resourceVersion 而报错。因此这里获取实际的 CSIDriver。
			csidriver, err := testCtx.ClientSet.StorageV1().CSIDrivers().Get(testCtx.Ctx, "csidriver", metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get CSIDriver: %w", err)
			}
			csidriver.Spec.StorageCapacity = ptr.To(false)

			if _, err := testCtx.ClientSet.StorageV1().CSIDrivers().Update(testCtx.Ctx, csidriver, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update CSIDriver: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSIDriver, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is not requeued when the CSIDriver is updated but the storage capacity is originally enabled",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(false)).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Volume(
				v1.Volume{
					Name: "volume",
					VolumeSource: v1.VolumeSource{
						CSI: &v1.CSIVolumeSource{
							Driver: "csidriver",
						},
					},
				},
			).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 使用 MakeCSIDriver 更新 CSIDriver 时，会因未指定 resourceVersion 而报错。因此这里获取实际的 CSIDriver。
			csidriver, err := testCtx.ClientSet.StorageV1().CSIDrivers().Get(testCtx.Ctx, "csidriver", metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get CSIDriver: %w", err)
			}
			csidriver.Spec.StorageCapacity = ptr.To(true)

			if _, err := testCtx.ClientSet.StorageV1().CSIDrivers().Update(testCtx.Ctx, csidriver, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update CSIDriver: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSIDriver, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the CSIStorageCapacity is created",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		InitialStorageClasses: []*storagev1.StorageClass{
			st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingImmediate).
				Provisioner("p").
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			csc := st.MakeCSIStorageCapacity().Name("csc").StorageClassName("sc1").Capacity(resource.NewQuantity(10, resource.BinarySI)).Obj()
			if _, err := testCtx.ClientSet.StorageV1().CSIStorageCapacities(testCtx.NS.Name).Create(testCtx.Ctx, csc, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create CSIStorageCapacity: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is requeued when the CSIStorageCapacity is increased",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				StorageClassName("sc1").
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				StorageClassName(ptr.To("sc1")).
				Obj(),
		},
		InitialStorageCapacities: []*storagev1.CSIStorageCapacity{
			st.MakeCSIStorageCapacity().Name("csc").StorageClassName("sc1").Capacity(resource.NewQuantity(10, resource.BinarySI)).Obj(),
		},
		InitialStorageClasses: []*storagev1.StorageClass{
			st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingImmediate).
				Provisioner("p").
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 使用 MakeCSIStorageCapacity 更新 CSIStorageCapacity 时，会因未指定 resourceVersion 而报错。因此这里获取实际的 CSIStorageCapacity。
			csc, err := testCtx.ClientSet.StorageV1().CSIStorageCapacities(testCtx.NS.Name).Get(testCtx.Ctx, "csc", metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get CSIStorageCapacity: %w", err)
			}
			csc.Capacity = resource.NewQuantity(20, resource.BinarySI)

			if _, err := testCtx.ClientSet.StorageV1().CSIStorageCapacities(testCtx.NS.Name).Update(testCtx.Ctx, csc, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update CSIStorageCapacity: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with node by the VolumeBinding plugin is not requeued when the CSIStorageCapacity is updated but the volumelimit is not increased",
		EnablePlugins: []string{names.VolumeBinding},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				HostPathVolumeSource(&v1.HostPathVolumeSource{Path: "/tmp", Type: ptr.To(v1.HostPathDirectoryOrCreate)}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialStorageCapacities: []*storagev1.CSIStorageCapacity{
			st.MakeCSIStorageCapacity().Name("csc").StorageClassName("sc1").Capacity(resource.NewQuantity(10, resource.BinarySI)).Obj(),
		},
		InitialStorageClasses: []*storagev1.StorageClass{
			st.MakeStorageClass().
				Name("sc1").
				VolumeBindingMode(storagev1.VolumeBindingImmediate).
				Provisioner("p").
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			// 使用 MakeCSIStorageCapacity 更新 CSIStorageCapacity 时，会因未指定 resourceVersion 而报错。因此这里获取实际的 CSIStorageCapacity。
			csc, err := testCtx.ClientSet.StorageV1().CSIStorageCapacities(testCtx.NS.Name).Get(testCtx.Ctx, "csc", metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get CSIStorageCapacity: %w", err)
			}
			csc.Capacity = resource.NewQuantity(5, resource.BinarySI)

			if _, err := testCtx.ClientSet.StorageV1().CSIStorageCapacities(testCtx.NS.Name).Update(testCtx.Ctx, csc, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update CSIStorageCapacity: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected the CSI NodeVolumeLimits plugin is requeued when the CSINode is added",
		EnablePlugins: []string{names.NodeVolumeLimits},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle"}}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(true)).Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").Driver(storagev1.CSINodeDriver{Name: "csidriver", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
				Count: ptr.To(int32(0)),
			}}).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			csinode1 := st.MakeCSINode().Name("csinode").Obj()
			if _, err := testCtx.ClientSet.StorageV1().CSINodes().Create(testCtx.Ctx, csinode1, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create CSINode: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.CSINode, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:          "Pod rejected with PVC by the CSI NodeVolumeLimits plugin is requeued when the pod having related PVC is deleted",
		EnablePlugins: []string{names.NodeVolumeLimits},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle1"}}).
				Obj(),
			st.MakePersistentVolume().Name("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle2"}}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(true)).Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").Driver(storagev1.CSINodeDriver{Name: "csidriver", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
				Count: ptr.To(int32(1)),
			}}).Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Delete(testCtx.Ctx, "pod1", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return nil, fmt.Errorf("failed to delete Pod: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventAssignedPodDelete: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:          "Pod rejected with PVC by the CSI NodeVolumeLimits plugin is requeued when the related PVC is added",
		EnablePlugins: []string{names.NodeVolumeLimits},
		InitialNodes:  []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle1"}}).
				Obj(),
			st.MakePersistentVolume().Name("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle2"}}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(true)).Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").Driver(storagev1.CSINodeDriver{Name: "csidriver", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
				Count: ptr.To(int32(1)),
			}}).Obj(),
		},
		InitialPods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Node("fake-node").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod2").Container("image").PVC("pvc2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pvc := st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj()
			if _, err := testCtx.ClientSet.CoreV1().PersistentVolumeClaims(testCtx.NS.Name).Create(testCtx.Ctx, pvc, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to add pvc2: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod2"),
	},
	{
		Name:         "Pod with PVC rejected the CSI NodeVolumeLimits plugin is requeued when the VolumeAttachment is deleted",
		InitialNodes: []*v1.Node{st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj()},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "csidriver", VolumeHandle: "volumehandle"}}).
				Obj()},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialVolumeAttachment: []*storagev1.VolumeAttachment{
			st.MakeVolumeAttachment().Name("volumeattachment1").
				NodeName("fake-node").
				Attacher("test.storage.gke.io").
				Source(storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To("pv1")}).
				Attached(true).
				Obj(),
		},
		InitialCSIDrivers: []*storagev1.CSIDriver{
			st.MakeCSIDriver().Name("csidriver").StorageCapacity(ptr.To(true)).Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").Driver(storagev1.CSINodeDriver{Name: "csidriver", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
				Count: ptr.To(int32(0)),
			}}).Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PVC("pvc1").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.StorageV1().VolumeAttachments().Delete(testCtx.Ctx, "volumeattachment1", metav1.DeleteOptions{}); err != nil {
				return nil, fmt.Errorf("failed to delete VolumeAttachment: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.VolumeAttachment, ActionType: fwk.Delete}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name: "Pod with Inline Migratable volume (AWSEBSDriver and GCEPDDriver) rejected the CSI NodeVolumeLimits plugin is requeued (only AWSEBSDriver) when the VolumeAttachment is deleted",
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node").Label("node", "fake-node").Obj(),
		},
		InitialPVs: []*v1.PersistentVolume{
			st.MakePersistentVolume().Name("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "ebs.csi.aws.com", VolumeHandle: "volumehandle"}}).
				Obj(),
			st.MakePersistentVolume().Name("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Capacity(v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}).
				PersistentVolumeSource(v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "pd.csi.storage.gke.io", VolumeHandle: "volumehandle"}}).
				Obj(),
		},
		InitialPVCs: []*v1.PersistentVolumeClaim{
			st.MakePersistentVolumeClaim().
				Name("pvc1").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv1").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
			st.MakePersistentVolumeClaim().
				Name("pvc2").
				Annotation(volume.AnnBindCompleted, "true").
				VolumeName("pv2").
				AccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}).
				Resources(v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}).
				Obj(),
		},
		InitialVolumeAttachment: []*storagev1.VolumeAttachment{
			st.MakeVolumeAttachment().Name("volumeattachment1").
				NodeName("fake-node").
				Attacher("ebs.csi.aws.com").
				Source(storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To("pv1")}).
				Attached(true).
				Obj(),
			st.MakeVolumeAttachment().Name("volumeattachment2").
				NodeName("fake-node").
				Attacher("pd.csi.storage.gke.io").
				Source(storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To("pv2")}).
				Attached(true).
				Obj(),
		},
		InitialCSINodes: []*storagev1.CSINode{
			st.MakeCSINode().Name("fake-node").
				Driver(storagev1.CSINodeDriver{Name: "ebs.csi.aws.com", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
					Count: ptr.To(int32(0))}}).
				Driver(storagev1.CSINodeDriver{Name: "pd.csi.storage.gke.io", NodeID: "fake-node", Allocatable: &storagev1.VolumeNodeResources{
					Count: ptr.To(int32(0))}}).
				Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").Volume(
				v1.Volume{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
							VolumeID: "pv1",
						},
					},
				},
			).Obj(),
			st.MakePod().Name("pod2").Container("image").Volume(
				v1.Volume{
					Name: "vol2",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "pv2",
						},
					},
				},
			).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			if err := testCtx.ClientSet.StorageV1().VolumeAttachments().Delete(testCtx.Ctx, "volumeattachment1", metav1.DeleteOptions{}); err != nil {
				return nil, fmt.Errorf("failed to delete VolumeAttachment: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.VolumeAttachment, ActionType: fwk.Delete}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:                       "pod becomes schedulable after node update with required feature",
		EnableNodeDeclaredFeatures: true,
		EnablePlugins:              []string{names.NodeDeclaredFeatures},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("node-1").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("pause").Tolerations([]v1.Toleration{{Key: "featureA", Effect: v1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(200))}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			node, err := testCtx.ClientSet.CoreV1().Nodes().Get(testCtx.Ctx, "node-1", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			updatedNode := node.DeepCopy()
			updatedNode.Status.DeclaredFeatures = []string{"FeatureA"}
			if _, err := testCtx.ClientSet.CoreV1().Nodes().UpdateStatus(testCtx.Ctx, updatedNode, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node status: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeDeclaredFeature}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:                       "pod remains unschedulable after node update that does not update declared features",
		EnableNodeDeclaredFeatures: true,
		EnablePlugins:              []string{names.NodeDeclaredFeatures},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("node-1").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("pause").Tolerations([]v1.Toleration{{Key: "featureA", Effect: v1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(200))}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			node, err := testCtx.ClientSet.CoreV1().Nodes().Get(testCtx.Ctx, "node-1", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			updatedNode := node.DeepCopy()
			if updatedNode.Labels == nil {
				updatedNode.Labels = make(map[string]string)
			}
			updatedNode.Labels["foo"] = "bar"
			if _, err := testCtx.ClientSet.CoreV1().Nodes().Update(testCtx.Ctx, updatedNode, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to update node status: %w", err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.Node, ActionType: fwk.UpdateNodeDeclaredFeature}: 0}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:                       "pod becomes schedulable after pod update that removes feature requirement",
		EnableNodeDeclaredFeatures: true,
		EnablePlugins:              []string{names.NodeDeclaredFeatures},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("node-1").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("pause").Tolerations([]v1.Toleration{{Key: "featureA", Effect: v1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(200))}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pod, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, "pod1", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			updatedPod := pod.DeepCopy()
			// 修改 toleration，使我们不再需要该特性。
			updatedPod.Spec.Tolerations[0].TolerationSeconds = ptr.To(int64(0))
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, updatedPod, metav1.UpdateOptions{}); err != nil {
				return nil, err
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.UnscheduledPod, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.New("pod1"),
	},
	{
		Name:                       "pod remains unschedulable after pod update that does not remove feature requirement",
		EnableNodeDeclaredFeatures: true,
		EnablePlugins:              []string{names.NodeDeclaredFeatures},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("node-1").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("pause").Tolerations([]v1.Toleration{{Key: "featureA", Effect: v1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(200))}}).Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pod, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Get(testCtx.Ctx, "pod1", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			updatedPod := pod.DeepCopy()
			updatedPod.Labels = make(map[string]string)
			updatedPod.Labels["foo"] = "bar"
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Update(testCtx.Ctx, updatedPod, metav1.UpdateOptions{}); err != nil {
				return nil, err
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.UnscheduledPod, ActionType: fwk.Update}: 1}, nil
		},
		WantRequeuedPods: sets.Set[string]{},
	},
	{
		Name:          "Pod rejected by the GangScheduling plugin is requeued when a new pod with matching scheduling group is created",
		EnablePlugins: []string{names.GangScheduling},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Obj(),
		},
		InitialPodGroups: []*schedulingapi.PodGroup{
			st.MakePodGroup().Name("pg1").MinCount(2).WorkloadRef("t", "w").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PodGroupName("pg1").Obj(),
			st.MakePod().Name("pod2").Container("image").PodGroupName("pg2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pod := st.MakePod().Name("pod3").Container("image").PodGroupName("pg1").Obj()
			if _, err := testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create Pod %q: %w", pod.Name, err)
			}
			return map[fwk.ClusterEvent]uint64{framework.EventUnscheduledPodAdd: 1}, nil
		},
		ExpectedInitialRejectingPhase: rejectingPhasePreEnqueue,
		WantRequeuedPods:              sets.New("pod1", "pod3"),
		EnableGenericWorkload:         true,
	},
	{
		Name:          "Pod rejected by the GangScheduling plugin is requeued when a matching pod group is created",
		EnablePlugins: []string{names.GangScheduling},
		InitialNodes: []*v1.Node{
			st.MakeNode().Name("fake-node1").Obj(),
		},
		Pods: []*v1.Pod{
			st.MakePod().Name("pod1").Container("image").PodGroupName("pg1").Obj(),
			st.MakePod().Name("pod2").Container("image").PodGroupName("pg2").Obj(),
		},
		TriggerFn: func(testCtx *testutils.TestContext) (map[fwk.ClusterEvent]uint64, error) {
			pg := st.MakePodGroup().Name("pg1").MinCount(1).WorkloadRef("t", "w").Obj()
			if _, err := testCtx.ClientSet.SchedulingV1beta1().PodGroups(testCtx.NS.Name).Create(testCtx.Ctx, pg, metav1.CreateOptions{}); err != nil {
				return nil, fmt.Errorf("failed to create PodGroup %q: %w", pg.Name, err)
			}
			return map[fwk.ClusterEvent]uint64{{Resource: fwk.PodGroup, ActionType: fwk.Add}: 1}, nil
		},
		ExpectedInitialRejectingPhase: rejectingPhasePreEnqueue,
		WantRequeuedPods:              sets.New("pod1"),
		EnableGenericWorkload:         true,
	},
}

// TestCoreResourceEnqueue 验证因内置默认插件而失败的 Pod
// 能在其注册的事件发生时被正确移动。
func RunTestCoreResourceEnqueue(t *testing.T, tt *CoreResourceEnqueueTestCase) {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	if tt.EnableDRAExtendedResource {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, true)
	}
	if tt.EnableNodeDeclaredFeatures {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, true)

		mockFeature := ndftesting.NewMockFeature(t)
		mockFeature.SetName("FeatureA")
		mockFeature.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
			return len(podInfo.Spec.Tolerations) > 0 && podInfo.Spec.Tolerations[0].TolerationSeconds != nil &&
				*podInfo.Spec.Tolerations[0].TolerationSeconds > 0
		})
		mockFeature.SetMaxVersion(nil)

		ndfFramework := ndf.New([]ndf.Feature{mockFeature})
		ndftesting.SetFrameworkDuringTest(t, *ndfFramework)
	}
	if tt.EnableGenericWorkload {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.GenericWorkload: true,
		})
	}
	logger, _ := ktesting.NewTestContext(t)

	opts := []scheduler.Option{scheduler.WithPodInitialBackoffSeconds(0), scheduler.WithPodMaxBackoffSeconds(0)}
	if tt.EnablePlugins != nil {
		enablePlugins := []configv1.Plugin{
			// 这些是启动调度器所需的插件。
			{Name: "PrioritySort"},
			{Name: "DefaultBinder"},
		}
		for _, pluginName := range tt.EnablePlugins {
			enablePlugins = append(enablePlugins, configv1.Plugin{Name: pluginName})
		}
		profile := configv1.KubeSchedulerProfile{
			SchedulerName: ptr.To("default-scheduler"),
			Plugins: &configv1.Plugins{
				MultiPoint: configv1.PluginSet{
					Enabled: enablePlugins,
					Disabled: []configv1.Plugin{
						{Name: "*"},
					},
				},
			},
		}
		cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
			Profiles: []configv1.KubeSchedulerProfile{profile},
		})
		opts = append(opts, scheduler.WithProfiles(cfg.Profiles...))
	}

	// 使用零退避秒数以绕过 backoffQ。
	// 目的是不启动调度器的队列，从而不启动任何 flush 逻辑。稍后我们将手动弹出并调度 Pod。
	testCtx := testutils.InitTestSchedulerWithOptions(
		t,
		testutils.InitTestAPIServer(t, "core-res-enqueue", nil),
		0,
		opts...,
	)
	testutils.SyncSchedulerInformerFactory(testCtx)

	testCtx.Scheduler.SchedulingQueue.Run(logger)
	defer testCtx.Scheduler.SchedulingQueue.Close()

	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx
	// 创建一个带污点的 Node。
	for _, node := range tt.InitialNodes {
		if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create an initial Node %q: %v", node.Name, err)
		}
	}

	for _, deviceClass := range tt.InitialDeviceClasses {
		if _, err := cs.ResourceV1().DeviceClasses().Create(ctx, deviceClass, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a DeviceClass %q: %v", deviceClass.Name, err)
		}
	}

	for _, csinode := range tt.InitialCSINodes {
		if _, err := testCtx.ClientSet.StorageV1().CSINodes().Create(ctx, csinode, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a CSINode %q: %v", csinode.Name, err)
		}
	}

	for _, csc := range tt.InitialStorageCapacities {
		if _, err := cs.StorageV1().CSIStorageCapacities(ns).Create(ctx, csc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a CSIStorageCapacity %q: %v", csc.Name, err)
		}
	}

	for _, va := range tt.InitialVolumeAttachment {
		if _, err := cs.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a VolumeAttachment %q: %v", va.Name, err)
		}
	}

	for _, csidriver := range tt.InitialCSIDrivers {
		if _, err := cs.StorageV1().CSIDrivers().Create(ctx, csidriver, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a CSIDriver %q: %v", csidriver.Name, err)
		}
	}

	for _, sc := range tt.InitialStorageClasses {
		if _, err := cs.StorageV1().StorageClasses().Create(testCtx.Ctx, sc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a StorageClass %q: %v", sc.Name, err)
		}
	}

	for _, pv := range tt.InitialPVs {
		if _, err := testutils.CreatePV(cs, pv); err != nil {
			t.Fatalf("Failed to create a PV %q: %v", pv.Name, err)
		}
	}

	for _, pvc := range tt.InitialPVCs {
		pvc.Namespace = ns
		if _, err := testutils.CreatePVC(cs, pvc); err != nil {
			t.Fatalf("Failed to create a PVC %q: %v", pvc.Name, err)
		}
	}

	for _, pg := range tt.InitialPodGroups {
		pg.Namespace = ns
		if _, err := cs.SchedulingV1beta1().PodGroups(ns).Create(testCtx.Ctx, pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create a PodGroup %q: %v", pg.Name, err)
		}
	}

	for _, pod := range tt.InitialPods {
		if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create an initial Pod %q: %v", pod.Name, err)
		}
	}

	for _, pod := range tt.Pods {
		if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
		}
	}

	switch tt.ExpectedInitialRejectingPhase {
	case rejectingPhasePreEnqueue:
		// 等待 tt.Pods 在调度队列中变为不可调度。
		if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
			pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
			return len(pendingPods) == len(tt.Pods) && len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()) == 0, nil
		}); err != nil {
			t.Fatalf("Failed to wait for all pods to be present in the scheduling queue: %v", err)
		}
		t.Log("All pods are waiting as unschedulable, will trigger triggerFn")
	case rejectingPhaseSchedulingCycle:
		fallthrough
	default: // 默认为 rejectingPhaseSchedulingCycle。
		// 等待 tt.Pods 出现在调度活动队列中。
		if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
			pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
			return len(pendingPods) == len(tt.Pods) && len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()) == len(tt.Pods), nil
		}); err != nil {
			t.Fatalf("Failed to wait for all pods to be present in the scheduling queue: %v", err)
		}

		t.Log("Confirmed Pods in the scheduling queue, starting to schedule them")

		// 弹出所有 Pod。它们应变为不可调度。
		for i := 0; i < len(tt.Pods); i++ {
			testCtx.Scheduler.ScheduleOne(testCtx.Ctx)
		}
		// 等待 tt.Pods 仍留在调度（不可调度）队列中。
		if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
			activePodsCount := len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ())
			if activePodsCount > 0 {
				return false, fmt.Errorf("active queue was expected to be empty, but found %v Pods", activePodsCount)
			}

			pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
			return len(pendingPods) == len(tt.Pods), nil
		}); err != nil {
			t.Fatalf("Failed to wait for all pods to remain in the scheduling queue after scheduling attempts: %v", err)
		}
		t.Log("finished initial schedulings for all Pods, will trigger triggerFn")
	}

	legacyregistry.Reset() // 在触发前重置指标
	wantTriggeredEvents, err := tt.TriggerFn(testCtx)
	if err != nil {
		t.Fatalf("Failed to trigger the event: %v", err)
	}

	t.Log("finished triggering triggerFn, waiting for the scheduler to handle events")

	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		for e, count := range wantTriggeredEvents {
			vec, err := testutil.GetHistogramVecFromGatherer(legacyregistry.DefaultGatherer, "scheduler_event_handling_duration_seconds", map[string]string{
				"event": string(e.Label()),
			})
			if err != nil {
				return false, err
			}

			if vec.GetAggregatedSampleCount() != count {
				t.Logf("Expected %d sample for event %s, got %d", count, e.Label(), vec.GetAggregatedSampleCount())
				return false, nil
			}
		}

		return true, nil
	}); err != nil {
		t.Fatalf("Failed to wait for the scheduler to handle the event: %v", err)
	}

	t.Log("all events are handled by the scheduler, will check if tt.requeuedPods are requeued")

	// 等待 tt.Pods 仍留在调度队列中。
	var requeuedPods sets.Set[string]
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		requeuedPods = sets.Set[string]{} // 重置
		for _, requeuedPod := range testCtx.Scheduler.SchedulingQueue.PodsInActiveQ() {
			requeuedPods.Insert(requeuedPod.Name)
		}

		return requeuedPods.Equal(tt.WantRequeuedPods), nil
	}); err != nil {
		t.Fatalf("Expect Pods %v to be requeued, but %v are requeued actually", tt.WantRequeuedPods, requeuedPods)
	}
}
