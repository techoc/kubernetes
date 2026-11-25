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

package dra

import (
	"github.com/onsi/gomega"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// compatDevice 描述要放置在共享 counter set 上的设备，以及它在该 counter set 上
// 声明的兼容性组。
type compatDevice struct {
	name   string
	groups []string
}

// compatGroupsSlices 构建组成一个 pool 的两个节点本地 ResourceSlice：
// 一个 counter set slice 和一个 device slice（单个 ResourceSlice 只能设置
// sharedCounters 或 devices 之一，不能同时设置两者）。所有设备都从同一个
// counter set 中取用资源，并声明给定的兼容性组。该 counter set 可容纳两个设备
// （每个消耗一半），因此只有当两个设备的兼容性组有交集时才能被共同分配。
func compatGroupsSlices(nodeName, driverName, counterSet string, devices ...compatDevice) (counters, deviceSlice *resourceapi.ResourceSlice) {
	c := st.MakeResourceSlice(nodeName, driverName)
	c.Name = nodeName + "-" + driverName + "-counters"
	c.Spec.Pool.ResourceSliceCount = 2
	c.Spec.SharedCounters = []resourceapi.CounterSet{{
		Name:     counterSet,
		Counters: map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("8")}},
	}}

	d := st.MakeResourceSlice(nodeName, driverName)
	d.Name = nodeName + "-" + driverName + "-devices"
	d.Spec.Pool.ResourceSliceCount = 2
	for _, dev := range devices {
		d.Spec.Devices = append(d.Spec.Devices, resourceapi.Device{
			Name: dev.name,
			ConsumesCounters: []resourceapi.DeviceCounterConsumption{{
				CounterSet:          counterSet,
				Counters:            map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("4")}},
				CompatibilityGroups: dev.groups,
			}},
		})
	}
	return c.Obj(), d.Obj()
}

// createCompatGroupsPool 创建兼容性组 pool 的两个 slice，
// 并返回创建出的 device slice（即携带 compatibilityGroups 的那个）。
func createCompatGroupsPool(tCtx ktesting.TContext, nodeName, driverName, counterSet string, devices ...compatDevice) *resourceapi.ResourceSlice {
	counters, deviceSlice := compatGroupsSlices(nodeName, driverName, counterSet, devices...)
	createSlice(tCtx, counters)
	return createSlice(tCtx, deviceSlice)
}

// testCompatibilityGroups 通过真实的 apiserver + scheduler 端到端验证
// DRADeviceCompatibilityGroups feature。启用该 feature 时，调度器在 counter set 上
// 共同分配设备时必须强制要求兼容性组有交集；禁用时，apiserver 必须在写入时丢弃该字段。
func testCompatibilityGroups(tCtx ktesting.TContext, enabled bool) {
	if !enabled {
		tCtx.Run("FieldDroppedWhenDisabled", testCompatibilityGroupsFieldDropped)
		return
	}
	tCtx.Run("FieldPersistedWhenEnabled", testCompatibilityGroupsFieldPersisted)
	tCtx.Run("CompatibleCoAllocation", testCompatibleCoAllocation)
	tCtx.Run("IncompatibleDevicesUnschedulable", testIncompatibleUnschedulable)
	tCtx.Run("IncompatibleRejectedCompatibleAdmitted", testIncompatibleAcrossNodes)
}

// testCompatibleCoAllocation 验证在 counter set 上声明相同兼容性组的两个设备
// 可以被共同分配给同一个 claim。
func testCompatibleCoAllocation(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)
	node := firstNodeName(tCtx)

	createCompatGroupsPool(tCtx, node, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
		compatDevice{"dev-1", []string{"mig"}},
	)
	startScheduler(tCtx)

	claim := st.MakeResourceClaim().Name("claim").Namespace(namespace).
		RequestWithNameCount("req", class.Name, 2).Obj()
	createdClaim := createClaim(tCtx, namespace, "-compat", class, claim)

	pod := st.MakePod().Name(podName).Namespace(namespace).Container("c").Obj()
	createdPod := createPod(tCtx, namespace, "-compat", pod, createdClaim)

	scheduled := waitForPodScheduled(tCtx, namespace, createdPod.Name)
	tCtx.Expect(scheduled.Spec.NodeName).To(gomega.Equal(node))

	allocated, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, createdClaim.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get allocated claim")
	tCtx.Expect(allocated.Status.Allocation).ToNot(gomega.BeNil(), "claim should be allocated")
	tCtx.Expect(allocated.Status.Allocation.Devices.Results).To(gomega.HaveLen(2))
}

// testIncompatibleUnschedulable 验证当 claim 唯一可取用的设备在共享 counter set 上
// 声明了互不相交的兼容性组时，Pod 会保持不可调度状态。该 claim 请求两个设备，
// 节点上仅有的两个设备分别属于 "mig" 和 "vgpu"（无交集），且都从同一个 counter set
// 取用资源，因此无法共同分配，没有任何节点能满足该 claim。
func testIncompatibleUnschedulable(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)
	node := firstNodeName(tCtx)

	createCompatGroupsPool(tCtx, node, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
		compatDevice{"dev-1", []string{"vgpu"}},
	)
	startScheduler(tCtx)

	claim := st.MakeResourceClaim().Name("claim").Namespace(namespace).
		RequestWithNameCount("req", class.Name, 2).Obj()
	createdClaim := createClaim(tCtx, namespace, "-incompat", class, claim)

	pod := st.MakePod().Name(podName).Namespace(namespace).Container("c").Obj()
	createdPod := createPod(tCtx, namespace, "-incompat", pod, createdClaim)

	expectPodUnschedulable(tCtx, createdPod, "cannot allocate all claims")

	unallocated, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, createdClaim.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get claim")
	tCtx.Expect(unallocated.Status.Allocation).To(gomega.BeNil(), "incompatible claim must not be allocated")
}

// testIncompatibleAcrossNodes 验证调度器不会共同分配一对不兼容的设备
// （它们是某个节点上唯一的候选设备），而是将 Pod 放置到其设备具有共同兼容性组的
// 节点上。在兼容节点资源耗尽后创建的第二个 Pod 会保持不可调度状态：这证明第一个
// Pod 选择兼容节点是因为不兼容节点确实无法满足 claim，而非偶然。
func testIncompatibleAcrossNodes(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)
	nodeA, nodeB := twoNodeNames(tCtx)

	// nodeA 只提供一对不兼容的设备；nodeB 提供一对兼容的设备。
	createCompatGroupsPool(tCtx, nodeA, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
		compatDevice{"dev-1", []string{"vgpu"}},
	)
	createCompatGroupsPool(tCtx, nodeB, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
		compatDevice{"dev-1", []string{"mig"}},
	)
	startScheduler(tCtx)

	claim := st.MakeResourceClaim().Name("claim").Namespace(namespace).
		RequestWithNameCount("req", class.Name, 2).Obj()
	createdClaim := createClaim(tCtx, namespace, "-cross", class, claim)

	pod := st.MakePod().Name(podName).Namespace(namespace).Container("c").Obj()
	createdPod := createPod(tCtx, namespace, "-cross", pod, createdClaim)

	scheduled := waitForPodScheduled(tCtx, namespace, createdPod.Name)
	tCtx.Expect(scheduled.Spec.NodeName).To(gomega.Equal(nodeB),
		"pod must land on the node with a compatible device pair, not the incompatible one")

	// 第一个 Pod 消耗了 nodeB 的整个 counter set（两个 "mig" 设备）。此时第二个
	// 相同的 Pod 无处可去：nodeB 的资源已耗尽，而 nodeA 上仅有的一对设备不兼容，
	// 因此它必须保持不可调度状态。
	claim2 := st.MakeResourceClaim().Name("claim").Namespace(namespace).
		RequestWithNameCount("req", class.Name, 2).Obj()
	createdClaim2 := createClaim(tCtx, namespace, "-cross-2", class, claim2)

	pod2 := st.MakePod().Name(podName).Namespace(namespace).Container("c").Obj()
	createdPod2 := createPod(tCtx, namespace, "-cross-2", pod2, createdClaim2)

	expectPodUnschedulable(tCtx, createdPod2, "cannot allocate all claims")
}

// testCompatibilityGroupsFieldPersisted 验证 feature gate 往返测试中启用的一半：
// 启用该 feature 时，写入 ResourceSlice 的 compatibilityGroups 在创建后得以保留，
// 并在读回时返回。（禁用的一半由 testCompatibilityGroupsFieldDropped 验证；
// ratcheting 场景——旧对象已使用该字段时在更新中保留——由策略单元测试覆盖，
// 因为它需要切换 gate，而固定配置的集成测试框架无法表达这种切换。）
func testCompatibilityGroupsFieldPersisted(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	_, driverName := createTestClass(tCtx, namespace)
	node := firstNodeName(tCtx)

	created := createCompatGroupsPool(tCtx, node, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
	)
	got, err := tCtx.Client().ResourceV1().ResourceSlices().Get(tCtx, created.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "read the device slice back")
	tCtx.Expect(got.Spec.Devices).To(gomega.HaveLen(1))
	tCtx.Expect(got.Spec.Devices[0].ConsumesCounters[0].CompatibilityGroups).To(
		gomega.ConsistOf("mig"), "compatibilityGroups must be persisted when the feature is enabled")
}

// testCompatibilityGroupsFieldDropped 验证禁用 feature gate 时，apiserver 会在写入时
// 丢弃 compatibilityGroups。
func testCompatibilityGroupsFieldDropped(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	_, driverName := createTestClass(tCtx, namespace)
	node := firstNodeName(tCtx)

	created := createCompatGroupsPool(tCtx, node, driverName, "gpu-0",
		compatDevice{"dev-0", []string{"mig"}},
	)
	tCtx.Expect(created.Spec.Devices).To(gomega.HaveLen(1))
	tCtx.Expect(created.Spec.Devices[0].ConsumesCounters[0].CompatibilityGroups).To(
		gomega.BeEmpty(), "compatibilityGroups must be dropped when the feature is disabled")
}

func firstNodeName(tCtx ktesting.TContext) string {
	tCtx.Helper()
	nodes, err := tCtx.Client().CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list nodes")
	tCtx.Expect(nodes.Items).ToNot(gomega.BeEmpty(), "need at least one node")
	return nodes.Items[0].Name
}

func twoNodeNames(tCtx ktesting.TContext) (string, string) {
	tCtx.Helper()
	nodes, err := tCtx.Client().CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list nodes")
	if len(nodes.Items) < 2 {
		tCtx.Fatalf("need at least two nodes, have %d", len(nodes.Items))
	}
	return nodes.Items[0].Name, nodes.Items[1].Name
}
