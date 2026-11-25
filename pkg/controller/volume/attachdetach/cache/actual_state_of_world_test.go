/*
Copyright 2016 The Kubernetes Authors.

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

package cache

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2/ktesting"
	controllervolumetesting "k8s.io/kubernetes/pkg/controller/volume/attachdetach/testing"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// Test_AddVolumeNode_Positive_NewVolumeNewNode 验证以 attached=true 新增卷/节点：
//   - AddVolumeNode 成功返回生成的卷名；
//   - 该组合在 ASW 中状态为 AttachStateAttached，且只有一条记录。
func Test_AddVolumeNode_Positive_NewVolumeNewNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)

	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)

	// 断言
	if err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", err)
	}

	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, nodeName)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached 验证 attached=false
// 的条目先以 Uncertain 状态入库、不参与上报，随后以 attached=true 再次添加时
// 会被提升为 Attached 并进入上报列表：
//   - 首次添加后 GetAttachState 返回 AttachStateUncertain；
//   - 该卷不出现在 GetVolumesToReportAttached 与 GetAttachedVolumesPerNode 中；
//   - 再次添加生成相同卷名，状态变为 AttachStateAttached 且可被上报。
func Test_AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)

	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	// 以 attached=false 添加：条目先以 Uncertain 状态入库，尚未确认挂载。
	generatedVolumeName, err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, false)

	// 断言
	if err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", err)
	}

	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, nodeName)
	if volumeNodeComboState != AttachStateUncertain {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Uncertain'.", generatedVolumeName, nodeName, volumeNodeComboState)
	}

	allVolumes := asw.GetAttachedVolumes()
	if len(allVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(allVolumes))
	}
	verifyAttachedVolume(t, allVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	// Uncertain 状态不应上报为已挂载，避免 kubelet 误判。
	reportAsAttachedVolumesMap := asw.GetVolumesToReportAttached(logger)
	_, exists := reportAsAttachedVolumesMap[nodeName]
	if exists {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Actual: <node %q exist> Expect: <node does not exist in the reportedAsAttached map", nodeName)
	}

	volumesForNode := asw.GetAttachedVolumesForNode(nodeName)
	if len(volumesForNode) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(volumesForNode))
	}
	verifyAttachedVolume(t, volumesForNode, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	attachedVolumesMap := asw.GetAttachedVolumesPerNode()
	_, exists = attachedVolumesMap[nodeName]
	if exists {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Actual: <node %q exist> Expect: <node does not exist in the reportedAsAttached map", nodeName)
	}

	nodes := asw.GetNodesForAttachedVolume(volumeName)
	if len(nodes) > 0 {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Expect no nodes returned.")
	}

	// 第二次以 attached=true 添加：期望提升状态而不是新增条目。
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)

	// 断言
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	// 卷名由卷规格唯一确定，与节点无关；不一致会导致索引错位。
	if generatedVolumeName != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName,
			generatedVolumeName2)
	}

	volumeNodeComboState = asw.GetAttachState(generatedVolumeName, nodeName)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	nodes = asw.GetNodesForAttachedVolume(volumeName)
	if len(nodes) != 1 {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Expect one node returned.")
	}
	if nodes[0] != nodeName {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Expect node %v, Actual node %v", nodeName, nodes[0])
	}

	attachedVolumesMap = asw.GetAttachedVolumesPerNode()
	_, exists = attachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Actual: <node %q does not exist> Expect: <node does exist in the reportedAsAttached map", nodeName)
	}

}

// Test_AddVolumeNode_Positive_NewVolumeTwoNodesWithFalseAttached 验证同一卷先用
// attached=false 关联节点1、再以 attached=true 关联节点2 的行为：
//   - 两个节点各有一条条目，节点1为 Uncertain、节点2为 Attached；
//   - 只有节点2的卷进入上报列表与按节点索引；
//   - GetNodesForAttachedVolume 只返回处于 Attached 状态的节点。
func Test_AddVolumeNode_Positive_NewVolumeTwoNodesWithFalseAttached(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)

	node1Name := types.NodeName("node1-name")
	node2Name := types.NodeName("node2-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node1Name, devicePath, false)

	// 断言
	if err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", err)
	}

	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, node1Name)
	if volumeNodeComboState != AttachStateUncertain {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Uncertain'.", generatedVolumeName, node1Name, volumeNodeComboState)
	}

	// 节点2以 attached=true 添加，与节点1的 Uncertain 状态形成对照。
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node2Name, devicePath, true)

	// 断言
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	if generatedVolumeName != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName,
			generatedVolumeName2)
	}

	volumeNodeComboState = asw.GetAttachState(generatedVolumeName, node2Name)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName, node2Name, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 2 {
		t.Fatalf("len(attachedVolumes) Expected: <2> Actual: <%v>", len(attachedVolumes))
	}
	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	volumesForNode := asw.GetAttachedVolumesForNode(node2Name)
	if len(volumesForNode) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <2> Actual: <%v>", len(volumesForNode))
	}
	verifyAttachedVolume(t, volumesForNode, generatedVolumeName, string(volumeName), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	attachedVolumesMap := asw.GetAttachedVolumesPerNode()
	attachedVolumesPerNode, exists := attachedVolumesMap[node2Name]
	if !exists || len(attachedVolumesPerNode) != 1 {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeTwoNodesWithFalseAttached failed. Actual: <node %q does not exist> Expect: <node does exist in the reportedAsAttached map", node2Name)
	}

	// 只返回 Attached 状态的节点，节点1的 Uncertain 条目应被排除。
	nodes := asw.GetNodesForAttachedVolume(volumeName)
	if len(nodes) != 1 {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Expect one node returned.")
	}

	reportAsAttachedVolumesMap := asw.GetVolumesToReportAttached(logger)
	reportedVolumes, exists := reportAsAttachedVolumesMap[node2Name]
	if !exists || len(reportedVolumes) != 1 {
		t.Fatalf("AddVolumeNode_Positive_NewVolumeNewNodeWithFalseAttached failed. Actual: <node %q exist> Expect: <node does not exist in the reportedAsAttached map", node2Name)
	}
}

// Test_AddVolumeNode_Positive_ExistingVolumeNewNode 验证同一卷添加到两个不同节点：
//   - 两次调用生成相同的卷名；
//   - 两条卷/节点条目均为 Attached，且共享同一份 volumeSpec。
func Test_AddVolumeNode_Positive_ExistingVolumeNewNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	node2Name := types.NodeName("node2-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	// 同一卷挂到不同节点，期望生成相同卷名但两条独立条目。
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node1Name, devicePath, true)
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node2Name, devicePath, true)

	// 断言
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	// 卷名由卷规格唯一确定，与节点无关。
	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	volumeNode1ComboState := asw.GetAttachState(generatedVolumeName1, node1Name)
	if volumeNode1ComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName1, node1Name, volumeNode1ComboState)
	}

	volumeNode2ComboState := asw.GetAttachState(generatedVolumeName1, node2Name)
	if volumeNode2ComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName1, node2Name, volumeNode2ComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 2 {
		t.Fatalf("len(attachedVolumes) Expected: <2> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_AddVolumeNode_Positive_ExistingVolumeExistingNode 验证对同一卷/节点重复
// AddVolumeNode 不会产生重复条目：第二次调用复用已有条目，仍只有一条记录。
func Test_AddVolumeNode_Positive_ExistingVolumeExistingNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	// 重复添加同一卷/节点，用于验证条目不重复。
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)

	// 断言
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	volumeNodeComboState := asw.GetAttachState(generatedVolumeName1, nodeName)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName1, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_DeleteVolumeNode_Positive_VolumeExistsNodeExists 验证删除已存在的卷/节点：
//   - DeleteVolumeNode 后该组合状态变为 AttachStateDetached；
//   - GetAttachedVolumes 不再返回该条目。
func Test_DeleteVolumeNode_Positive_VolumeExistsNodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	// 删除已挂载条目后应立即从 Attached 列表中消失。
	asw.DeleteVolumeNode(generatedVolumeName, nodeName)

	// 断言
	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, nodeName)
	if volumeNodeComboState != AttachStateDetached {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Detached'.", generatedVolumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_DeleteVolumeNode_Positive_VolumeDoesntExistNodeDoesntExist 验证对空 ASW
// 删除不存在的卷/节点是安全的（幂等），状态保持 Detached 且无条目。
func Test_DeleteVolumeNode_Positive_VolumeDoesntExistNodeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	nodeName := types.NodeName("node-name")

	// 执行
	// 对空状态执行删除，验证操作幂等。
	asw.DeleteVolumeNode(volumeName, nodeName)

	// 断言
	volumeNodeComboState := asw.GetAttachState(volumeName, nodeName)
	if volumeNodeComboState != AttachStateDetached {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Detached'.", volumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_DeleteVolumeNode_Positive_TwoNodesOneDeleted 验证删除同一卷在节点1上的
// 条目不影响节点2：节点1变为 Detached，节点2仍为 Attached 并保留在结果中。
func Test_DeleteVolumeNode_Positive_TwoNodesOneDeleted(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	node2Name := types.NodeName("node2-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node1Name, devicePath, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volumeName, volumeSpec, node2Name, devicePath, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}
	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	// 执行
	// 只删除节点1的条目，节点2应保持 Attached。
	asw.DeleteVolumeNode(generatedVolumeName1, node1Name)

	// 断言
	volumeNodeComboState := asw.GetAttachState(generatedVolumeName1, node1Name)
	if volumeNodeComboState != AttachStateDetached {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Detached'.", generatedVolumeName1, node1Name, volumeNodeComboState)
	}

	volumeNodeComboState = asw.GetAttachState(generatedVolumeName1, node2Name)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName1, node2Name, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_VolumeNodeExists_Positive_VolumeExistsNodeExists 验证 GetAttachState 能
// 正确返回已存在卷/节点的 AttachStateAttached 状态。
func Test_VolumeNodeExists_Positive_VolumeExistsNodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	// 按卷+节点精确匹配，避免只按卷名查询导致误判。
	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, nodeName)

	// 断言
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("%q/%q volume/node combo is marked %q; expected 'Attached'.", generatedVolumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_VolumeNodeExists_Positive_VolumeExistsNodeDoesntExist 验证 GetAttachState
// 按卷+节点精确匹配：查询不存在的节点返回 Detached，已有条目不受影响。
func Test_VolumeNodeExists_Positive_VolumeExistsNodeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	node2Name := types.NodeName("node2-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, node1Name, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	// 用不同节点查询，验证匹配的是卷与节点的组合而非仅卷名。
	volumeNodeComboState := asw.GetAttachState(generatedVolumeName, node2Name)

	// 断言
	if volumeNodeComboState != AttachStateDetached {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Detached'.", generatedVolumeName, node2Name, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_VolumeNodeExists_Positive_VolumeAndNodeDontExist 验证空 ASW 上查询
// GetAttachState 返回 Detached，不会误报已挂载。
func Test_VolumeNodeExists_Positive_VolumeAndNodeDontExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	nodeName := types.NodeName("node-name")

	// 执行
	// 空状态下查询应安全返回 Detached。
	volumeNodeComboState := asw.GetAttachState(volumeName, nodeName)

	// 断言
	if volumeNodeComboState != AttachStateDetached {
		t.Fatalf("%q/%q volume/node combo is marked %q, expected 'Detached'.", volumeName, nodeName, volumeNodeComboState)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_GetAttachedVolumes_Positive_NoVolumesOrNodes 验证空 ASW 的
// GetAttachedVolumes 返回空列表。
func Test_GetAttachedVolumes_Positive_NoVolumesOrNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)

	// 执行
	attachedVolumes := asw.GetAttachedVolumes()

	// 断言
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_GetAttachedVolumes_Positive_OneVolumeOneNode 验证 GetAttachedVolumes
// 返回唯一一条卷/节点条目且字段完整。
func Test_GetAttachedVolumes_Positive_OneVolumeOneNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumes()

	// 断言
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_GetAttachedVolumes_Positive_TwoVolumeTwoNodes 验证两个不同卷/节点
// 条目均被 GetAttachedVolumes 返回，互不干扰。
func Test_GetAttachedVolumes_Positive_TwoVolumeTwoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	node1Name := types.NodeName("node1-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, volume1Name, volume1Spec, node1Name, devicePath, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	node2Name := types.NodeName("node2-name")
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volume2Name, volume2Spec, node2Name, devicePath, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumes()

	// 断言
	if len(attachedVolumes) != 2 {
		t.Fatalf("len(attachedVolumes) Expected: <2> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volume1Name), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName2, string(volume2Name), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_GetAttachedVolumes_Positive_OneVolumeTwoNodes 验证同一卷挂到两个节点时
// GetAttachedVolumes 返回两条记录；第二次添加传入空卷名也能解析出相同卷名。
func Test_GetAttachedVolumes_Positive_OneVolumeTwoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	devicePath := "fake/device/path"
	// 先从卷规格解析插件与唯一卷名，模拟真实调用方传入规范卷名。
	plugin, err := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || plugin == nil {
		t.Fatalf("Failed to get volume plugin from spec %v, %v", volumeSpec, err)
	}
	uniqueVolumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil || uniqueVolumeName == "" {
		t.Fatalf("Failed to get uniqueVolumeName from spec %v, %v", volumeSpec, err)
	}
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, uniqueVolumeName, volumeSpec, node1Name, devicePath, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	node2Name := types.NodeName("node2-name")
	// 第二次传空卷名，验证 ASW 会自行从卷规格推导相同的卷名。
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, v1.UniqueVolumeName(""), volumeSpec, node2Name, devicePath, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumes()

	// 断言
	if len(attachedVolumes) != 2 {
		t.Fatalf("len(attachedVolumes) Expected: <2> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetVolumesMountedByNode_Positive_Set 验证新增条目的默认值：
// mountedByNode 为 true、DetachRequestedTime 为零，无需显式 Set 即可满足预期。
func Test_SetVolumesMountedByNode_Positive_Set(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行：不显式标记，用于验证默认挂载状态。

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSet 验证
// SetVolumesMountedByNode 的覆盖语义：先标记挂载再传 nil 取消标记，
// 最终 mountedByNode 为 false。
func Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSet(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	// 传 nil 表示取消该节点上所有卷的 mounted 标记。
	asw.SetVolumesMountedByNode(logger, nil, nodeName)

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, false /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetVolumesMountedByNode_Positive_UnsetWithoutInitialSet 验证即使没有
// 预先标记，传 nil 调用也会把 mountedByNode 覆盖为 false。
func Test_SetVolumesMountedByNode_Positive_UnsetWithoutInitialSet(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)

	// 执行
	// 即使此前未显式标记，nil 也会把默认值覆盖为 false。
	asw.SetVolumesMountedByNode(logger, nil, nodeName)

	// 断言
	attachedVolumes = asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, false /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSetAddVolumeNodeNotReset
// 验证对已存在卷/节点重复 AddVolumeNode 不会重置 mountedByNode（仍为 false），
// 也不会引入非零的 DetachRequestedTime。
func Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSetAddVolumeNodeNotReset(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	asw.SetVolumesMountedByNode(logger, nil, nodeName)
	// 重新添加同一卷/节点，期望不重置 mountedByNode。
	generatedVolumeName, addErr = asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)

	// 断言
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, false /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSetVerifyDetachRequestedTimePerserved
// 验证 SetVolumesMountedByNode 只影响 mountedByNode，不会清除此前
// SetDetachRequestTime 记录的时间戳；时间戳必须与操作前完全一致。
func Test_SetVolumesMountedByNode_Positive_UnsetWithInitialSetVerifyDetachRequestedTimePerserved(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}
	// 先记录卸载请求时间，后续用于验证它不会被状态变更清除。
	_, err := asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	err = asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	// 保存基准时间戳，断言时要求精确相等而非仅非零。
	expectedDetachRequestedTime := asw.GetAttachedVolumes()[0].DetachRequestedTime

	// 执行
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	asw.SetVolumesMountedByNode(logger, nil, nodeName)

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, false /* expectedMountedByNode */, true /* expectNonZeroDetachRequestedTime */)
	if !expectedDetachRequestedTime.Equal(attachedVolumes[0].DetachRequestedTime) {
		t.Fatalf("DetachRequestedTime changed. Expected: <%v> Actual: <%v>", expectedDetachRequestedTime, attachedVolumes[0].DetachRequestedTime)
	}
}

// Test_RemoveVolumeFromReportAsAttached_Positive_Set 验证新增条目的默认值
// （mountedByNode=true、DetachRequestedTime=0），与取消上报操作无关。
func Test_RemoveVolumeFromReportAsAttached_Positive_Set(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	devicePath := "fake/device/path"
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行：不显式标记，用于验证默认状态。

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_RemoveVolumeFromReportAsAttached_Positive_Marked 验证
// RemoveVolumeFromReportAsAttached 与 SetDetachRequestTime 配合后，
// 条目仍记录非零 DetachRequestedTime，mountedByNode 保持 true。
func Test_RemoveVolumeFromReportAsAttached_Positive_Marked(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	// 先记录卸载请求时间，再取消上报，验证两个状态字段独立更新。
	_, err := asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	markDesireToDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if markDesireToDetachErr != nil {
		t.Fatalf("MarkDesireToDetach failed. Expected: <no error> Actual: <%v>", markDesireToDetachErr)
	}

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, true /* expectNonZeroDetachRequestedTime */)
}

// Test_MarkDesireToDetach_Positive_MarkedAddVolumeNodeReset 验证
// ResetDetachRequestTime 能把已设置的卸载请求时间清零，恢复默认状态。
func Test_MarkDesireToDetach_Positive_MarkedAddVolumeNodeReset(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	_, err := asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	markDesireToDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	// 重置卸载请求时间至 0，验证状态可恢复默认值。
	asw.ResetDetachRequestTime(logger, generatedVolumeName, nodeName)

	// 断言
	if markDesireToDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", markDesireToDetachErr)
	}

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_RemoveVolumeFromReportAsAttached_Positive_UnsetWithInitialSetVolumesMountedByNodePreserved
// 验证两种状态变更互不干扰：取消 mountedByNode 后再设置卸载请求时间，
// mountedByNode 保持 false 且 DetachRequestedTime 非零。
func Test_RemoveVolumeFromReportAsAttached_Positive_UnsetWithInitialSetVolumesMountedByNodePreserved(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}
	// 先取消 mounted 标记再记录卸载请求，验证两者互不覆盖。
	asw.SetVolumesMountedByNode(logger, []v1.UniqueVolumeName{generatedVolumeName}, nodeName)
	asw.SetVolumesMountedByNode(logger, nil, nodeName)
	// 执行
	_, err := asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	removeVolumeDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if removeVolumeDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", removeVolumeDetachErr)
	}

	// 断言
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, false /* expectedMountedByNode */, true /* expectNonZeroDetachRequestedTime */)
}

// Test_RemoveVolumeFromReportAsAttached 验证取消上报后节点仍保留在
// reportedAsAttached 映射中（便于后续状态同步），但其卷列表为空。
func Test_RemoveVolumeFromReportAsAttached(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	removeVolumeDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if removeVolumeDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", removeVolumeDetachErr)
	}

	// 节点仍应保留在映射中，只是卷列表为空，便于状态同步。
	reportAsAttachedVolumesMap := asw.GetVolumesToReportAttached(logger)
	volumes, exists := reportAsAttachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("MarkDesireToDetach_UnmarkDesireToDetach failed. Expected: <node %q exist> Actual: <node does not exist in the reportedAsAttached map", nodeName)
	}
	if len(volumes) > 0 {
		t.Fatalf("len(reportAsAttachedVolumes) Expected: <0> Actual: <%v>", len(volumes))
	}

}

// Test_RemoveVolumeFromReportAsAttached_AddVolumeToReportAsAttached_Positive
// 验证取消上报后可通过 AddVolumeToReportAsAttached 恢复，节点卷列表重新为一条。
func Test_RemoveVolumeFromReportAsAttached_AddVolumeToReportAsAttached_Positive(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	removeVolumeDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if removeVolumeDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", removeVolumeDetachErr)
	}

	reportAsAttachedVolumesMap := asw.GetVolumesToReportAttached(logger)
	volumes, exists := reportAsAttachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("Test_RemoveVolumeFromReportAsAttached_AddVolumeToReportAsAttached_Positive failed. Expected: <node %q exist> Actual: <node does not exist in the reportedAsAttached map", nodeName)
	}
	if len(volumes) > 0 {
		t.Fatalf("len(reportAsAttachedVolumes) Expected: <0> Actual: <%v>", len(volumes))
	}

	// 重新加入上报列表，验证取消与恢复操作可逆。
	asw.AddVolumeToReportAsAttached(logger, generatedVolumeName, nodeName)
	reportAsAttachedVolumesMap = asw.GetVolumesToReportAttached(logger)
	volumes, exists = reportAsAttachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("Test_RemoveVolumeFromReportAsAttached_AddVolumeToReportAsAttached_Positive failed. Expected: <node %q exist> Actual: <node does not exist in the reportedAsAttached map", nodeName)
	}
	if len(volumes) != 1 {
		t.Fatalf("len(reportAsAttachedVolumes) Expected: <1> Actual: <%v>", len(volumes))
	}
}

// Test_RemoveVolumeFromReportAsAttached_Delete_AddVolumeNode 验证卷/节点先取消
// 上报、再删除后重新添加的生命周期：重新添加的卷会再次进入上报列表。
func Test_RemoveVolumeFromReportAsAttached_Delete_AddVolumeNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	removeVolumeDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if removeVolumeDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", removeVolumeDetachErr)
	}

	reportAsAttachedVolumesMap := asw.GetVolumesToReportAttached(logger)
	volumes, exists := reportAsAttachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("Test_RemoveVolumeFromReportAsAttached_Delete_AddVolumeNode failed. Expected: <node %q exists> Actual: <node does not exist in the reportedAsAttached map", nodeName)
	}
	if len(volumes) > 0 {
		t.Fatalf("len(reportAsAttachedVolumes) Expected: <0> Actual: <%v>", len(volumes))
	}

	// 删除后重新添加，验证上报状态随条目生命周期正确重建。
	asw.DeleteVolumeNode(generatedVolumeName, nodeName)

	asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, "" /* 设备路径 */, true)

	reportAsAttachedVolumesMap = asw.GetVolumesToReportAttached(logger)
	volumes, exists = reportAsAttachedVolumesMap[nodeName]
	if !exists {
		t.Fatalf("Test_RemoveVolumeFromReportAsAttached_Delete_AddVolumeNode failed. Expected: <node %q exists> Actual: <node does not exist in the reportedAsAttached map", nodeName)
	}
	if len(volumes) != 1 {
		t.Fatalf("len(reportAsAttachedVolumes) Expected: <1> Actual: <%v>", len(volumes))
	}
}

// Test_SetDetachRequestTime_Positive 验证 SetDetachRequestTime 返回的等待时长
// 随记录时间增长：
//   - 首次调用后立即返回，elapsed 小于 maxWaitTime；
//   - 睡眠 maxWaitTime 后再次调用，elapsed 不小于 maxWaitTime。
func Test_SetDetachRequestTime_Positive(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 使用 1 秒阈值，使两次调用的等待时长差异可被稳定观测。
	maxWaitTime := 1 * time.Second
	etime, err := asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	if etime >= maxWaitTime {
		t.Logf("SetDetachRequestTim Expected: <elapsed time %v is smaller than maxWaitTime %v> Actual <elapsed time is larger than maxWaitTime>", etime, maxWaitTime)
	}
	// 睡眠一个阈值后再次调用，使 elapsed 超过 maxWaitTime。
	time.Sleep(maxWaitTime)
	etime, err = asw.SetDetachRequestTime(logger, generatedVolumeName, nodeName)
	if err != nil {
		t.Fatalf("SetDetachRequestTime failed. Expected: <no error> Actual: <%v>", err)
	}
	if etime < maxWaitTime {
		t.Fatalf("SetDetachRequestTim Expected: <elapsed time %v is larger than maxWaitTime %v> Actual <elapsed time is smaller>", etime, maxWaitTime)
	}
}

// Test_GetAttachedVolumesForNode_Positive_NoVolumesOrNodes 验证空 ASW 上
// 按节点查询返回空列表。
func Test_GetAttachedVolumesForNode_Positive_NoVolumesOrNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	node := types.NodeName("random")

	// 执行
	attachedVolumes := asw.GetAttachedVolumesForNode(node)

	// 断言
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_GetAttachedVolumesForNode_Positive_OneVolumeOneNode 验证按节点查询
// 能返回该节点下唯一的卷条目。
func Test_GetAttachedVolumesForNode_Positive_OneVolumeOneNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, addErr := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)
	if addErr != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", addErr)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumesForNode(nodeName)

	// 断言
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_GetAttachedVolumesForNode_Positive_TwoVolumeTwoNodes 验证按节点查询
// 具有隔离性：节点2只返回自己的卷，节点1的卷不混入。
func Test_GetAttachedVolumesForNode_Positive_TwoVolumeTwoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	node1Name := types.NodeName("node1-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	_, add1Err := asw.AddVolumeNode(logger, volume1Name, volume1Spec, node1Name, devicePath, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	node2Name := types.NodeName("node2-name")
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, volume2Name, volume2Spec, node2Name, devicePath, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumesForNode(node2Name)

	// 断言
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName2, string(volume2Name), node2Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_GetAttachedVolumesForNode_Positive_OneVolumeTwoNodes 验证同一卷挂到
// 两个节点时，按节点查询只返回所查询节点的那一条记录。
func Test_GetAttachedVolumesForNode_Positive_OneVolumeTwoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	devicePath := "fake/device/path"
	logger, _ := ktesting.NewTestContext(t)
	plugin, err := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || plugin == nil {
		t.Fatalf("Failed to get volume plugin from spec %v, %v", volumeSpec, err)
	}
	uniqueVolumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil || uniqueVolumeName == "" {
		t.Fatalf("Failed to get uniqueVolumeName from spec %v, %v", volumeSpec, err)
	}
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, uniqueVolumeName, volumeSpec, node1Name, devicePath, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	node2Name := types.NodeName("node2-name")
	// 传空卷名让 ASW 从卷规格解析，验证与显式卷名一致。
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, v1.UniqueVolumeName(""), volumeSpec, node2Name, devicePath, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumesForNode(node1Name)

	// 断言
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName1, string(volumeName), node1Name, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_OneVolumeTwoNodes_TwoDevicePaths 验证同一卷在两个节点上可保存各自的
// DevicePath，按节点查询返回对应设备路径。
func Test_OneVolumeTwoNodes_TwoDevicePaths(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := types.NodeName("node1-name")
	devicePath1 := "fake/device/path1"
	logger, _ := ktesting.NewTestContext(t)
	plugin, err := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || plugin == nil {
		t.Fatalf("Failed to get volume plugin from spec %v, %v", volumeSpec, err)
	}
	uniqueVolumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil || uniqueVolumeName == "" {
		t.Fatalf("Failed to get uniqueVolumeName from spec %v, %v", volumeSpec, err)
	}
	generatedVolumeName1, add1Err := asw.AddVolumeNode(logger, uniqueVolumeName, volumeSpec, node1Name, devicePath1, true)
	if add1Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add1Err)
	}
	node2Name := types.NodeName("node2-name")
	// 两个节点使用不同设备路径，验证按节点查询返回各自路径。
	devicePath2 := "fake/device/path2"
	generatedVolumeName2, add2Err := asw.AddVolumeNode(logger, v1.UniqueVolumeName(""), volumeSpec, node2Name, devicePath2, true)
	if add2Err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", add2Err)
	}

	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}

	// 执行
	attachedVolumes := asw.GetAttachedVolumesForNode(node2Name)

	// 断言
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	verifyAttachedVolume(t, attachedVolumes, generatedVolumeName2, string(volumeName), node2Name, devicePath2, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_SetNodeStatusUpdateNeededError 验证对不存在的节点调用
// SetNodeStatusUpdateNeeded 不会在 nodesToUpdateStatusFor 中凭空创建条目。
func Test_SetNodeStatusUpdateNeededError(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	nodeName := types.NodeName("node-1")

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	// 节点不存在时不应创建状态条目，避免后续误报需要更新。
	asw.SetNodeStatusUpdateNeeded(logger, nodeName)

	// 断言
	nodesToUpdateStatusFor := asw.GetNodesToUpdateStatusFor()
	if len(nodesToUpdateStatusFor) != 0 {
		t.Fatalf("nodesToUpdateStatusFor should be empty as nodeName does not exist")
	}
}

// Test_updateNodeStatusUpdateNeeded 验证对已存在的节点调用
// updateNodeStatusUpdateNeeded 能正确翻转 statusUpdateNeeded 标志。
func Test_updateNodeStatusUpdateNeeded(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	// 直接构造内部结构体，以便精确控制节点的初始状态。
	asw := &actualStateOfWorld{
		attachedVolumes:        make(map[v1.UniqueVolumeName]attachedVolume),
		nodesToUpdateStatusFor: make(map[types.NodeName]nodeToUpdateStatusFor),
		volumePluginMgr:        volumePluginMgr,
	}
	nodeName := types.NodeName("node-1")
	nodeToUpdate := nodeToUpdateStatusFor{
		nodeName:                  nodeName,
		statusUpdateNeeded:        true,
		volumesToReportAsAttached: make(map[v1.UniqueVolumeName]v1.UniqueVolumeName),
	}
	asw.nodesToUpdateStatusFor[nodeName] = nodeToUpdate

	// 执行
	// 将标志从 true 翻转为 false，验证更新逻辑生效。
	err := asw.updateNodeStatusUpdateNeeded(nodeName, false)

	// 断言
	if err != nil {
		t.Fatalf("updateNodeStatusUpdateNeeded should not return error, but got: %v", err)
	}
	nodesToUpdateStatusFor := asw.GetNodesToUpdateStatusFor()
	if nodesToUpdateStatusFor[nodeName].statusUpdateNeeded {
		t.Fatalf("nodesToUpdateStatusFor should be updated to: false, but got: true")
	}
}

// Test_updateNodeStatusUpdateNeededError 验证对不存在的节点调用
// updateNodeStatusUpdateNeeded 会返回错误，避免静默丢失状态更新请求。
func Test_updateNodeStatusUpdateNeededError(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := &actualStateOfWorld{
		attachedVolumes:        make(map[v1.UniqueVolumeName]attachedVolume),
		nodesToUpdateStatusFor: make(map[types.NodeName]nodeToUpdateStatusFor),
		volumePluginMgr:        volumePluginMgr,
	}
	nodeName := types.NodeName("node-1")

	// 执行
	// 不存在的节点必须报错，否则状态更新会被静默丢弃。
	err := asw.updateNodeStatusUpdateNeeded(nodeName, false)

	// 断言
	if err == nil {
		t.Fatalf("updateNodeStatusUpdateNeeded should return error, but got nothing")
	}
}

// Test_MarkVolumeAsAttached 验证 MarkVolumeAsAttached 将卷直接标记为已挂载：
//   - GetAttachState 返回 AttachStateAttached；
//   - GetAttachedVolumes 包含该卷且设备路径等信息正确。
func Test_MarkVolumeAsAttached(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)

	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"

	// 确认卷规格能解析出可挂载插件，排除测试环境配置问题。
	plugin, err := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || plugin == nil {
		t.Fatalf("Failed to get volume plugin from spec %v, %v", volumeSpec, err)
	}

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, volumeName, volumeSpec, nodeName, devicePath)

	// 断言
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	volumeNodeComboState := asw.GetAttachState(volumeName, nodeName)
	if volumeNodeComboState != AttachStateAttached {
		t.Fatalf("asw says the volume: %q is not attached (%v) to node:%q, it should.",
			volumeName, AttachStateAttached, nodeName)
	}
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}
	verifyAttachedVolume(t, attachedVolumes, volumeName, string(volumeName), nodeName, devicePath, true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_MarkVolumeAsUncertain 验证 MarkVolumeAsUncertain 将卷标记为不确定状态：
//   - GetAttachState 返回 AttachStateUncertain；
//   - 卷仍出现在 GetAttachedVolumes 中，但设备路径为空。
func Test_MarkVolumeAsUncertain(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := types.NodeName("node-name")

	// 确认插件可解析，才能判断 Uncertain 状态确实由实现返回。
	plugin, err := volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
	if err != nil || plugin == nil {
		t.Fatalf("Failed to get volume plugin from spec %v, %v", volumeSpec, err)
	}

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsUncertain(logger, volumeName, volumeSpec, nodeName)

	// 断言
	if err != nil {
		t.Fatalf("MarkVolumeAsUncertain failed. Expected: <no error> Actual: <%v>", err)
	}
	volumeNodeComboState := asw.GetAttachState(volumeName, nodeName)
	if volumeNodeComboState != AttachStateUncertain {
		t.Fatalf("asw says the volume: %q is attached (%v) to node:%q, it should not.",
			volumeName, volumeNodeComboState, nodeName)
	}
	attachedVolumes := asw.GetAttachedVolumes()
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}
	verifyAttachedVolume(t, attachedVolumes, volumeName, string(volumeName), nodeName, "", true /* expectedMountedByNode */, false /* expectNonZeroDetachRequestedTime */)
}

// Test_GetVolumesToReportAttachedForNode_Positive 验证节点状态更新标志的
// 读取即清除语义：
//   - 首次新增卷后查询返回 needsUpdate=true；
//   - 再次查询返回 needsUpdate=false，避免重复上报；
//   - 取消上报后会再次置位，确保状态变化被同步。
func Test_GetVolumesToReportAttachedForNode_Positive(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)

	nodeName := types.NodeName("node-name")
	devicePath := "fake/device/path"

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	generatedVolumeName, err := asw.AddVolumeNode(logger, volumeName, volumeSpec, nodeName, devicePath, true)

	// 断言
	if err != nil {
		t.Fatalf("AddVolumeNode failed. Expected: <no error> Actual: <%v>", err)
	}

	needsUpdate, attachedVolumes := asw.GetVolumesToReportAttachedForNode(logger, nodeName)
	if !needsUpdate {
		t.Fatalf("GetVolumesToReportAttachedForNode_Positive_NewVolumeNewNodeWithTrueAttached failed. Actual: <node %q does not need an update> Expect: <node exists in the reportedAsAttached map and needs an update", nodeName)
	}
	if len(attachedVolumes) != 1 {
		t.Fatalf("len(attachedVolumes) Expected: <1> Actual: <%v>", len(attachedVolumes))
	}

	// 第二次查询应清除标志，验证读取即清除以避免重复同步。
	needsUpdate, _ = asw.GetVolumesToReportAttachedForNode(logger, nodeName)
	if needsUpdate {
		t.Fatalf("GetVolumesToReportAttachedForNode_Positive_NewVolumeNewNodeWithTrueAttached failed. Actual: <node %q needs an update> Expect: <node exists in the reportedAsAttached map and does not need an update", nodeName)
	}

	// 取消上报会再次置位，确保 kubelet 收到状态变化。
	removeVolumeDetachErr := asw.RemoveVolumeFromReportAsAttached(generatedVolumeName, nodeName)
	if removeVolumeDetachErr != nil {
		t.Fatalf("RemoveVolumeFromReportAsAttached failed. Expected: <no error> Actual: <%v>", removeVolumeDetachErr)
	}

	needsUpdate, attachedVolumes = asw.GetVolumesToReportAttachedForNode(logger, nodeName)
	if !needsUpdate {
		t.Fatalf("GetVolumesToReportAttachedForNode_Positive_NewVolumeNewNodeWithTrueAttached failed. Actual: <node %q does not need an update> Expect: <node exists in the reportedAsAttached map and needs an update", nodeName)
	}
	if len(attachedVolumes) != 0 {
		t.Fatalf("len(attachedVolumes) Expected: <0> Actual: <%v>", len(attachedVolumes))
	}
}

// Test_GetVolumesToReportAttachedForNode_UnknownNode 验证查询未知节点时
// needsUpdate 为 false，不会为不存在的节点创建状态记录。
func Test_GetVolumesToReportAttachedForNode_UnknownNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	asw := NewActualStateOfWorld(volumePluginMgr)
	nodeName := types.NodeName("node-name")
	logger, _ := ktesting.NewTestContext(t)
	needsUpdate, _ := asw.GetVolumesToReportAttachedForNode(logger, nodeName)
	if needsUpdate {
		t.Fatalf("GetVolumesToReportAttachedForNode_UnknownNode failed. Actual: <node %q needs an update> Expect: <node does not exist in the reportedAsAttached map and does not need an update", nodeName)
	}
}

// verifyAttachedVolume 在 attachedVolumes 中查找匹配全部期望字段的条目：
//   - 卷名、卷规格名、节点名、设备路径与 mountedByNode 必须完全一致；
//   - DetachRequestedTime 是否为零由 expectNonZeroDetachRequestedTime 决定；
//   - 找不到匹配项时直接 Fatalf，避免后续断言基于错误数据继续。
func verifyAttachedVolume(
	t *testing.T,
	attachedVolumes []AttachedVolume,
	expectedVolumeName v1.UniqueVolumeName,
	expectedVolumeSpecName string,
	expectedNodeName types.NodeName,
	expectedDevicePath string,
	expectedMountedByNode,
	expectNonZeroDetachRequestedTime bool) {
	for _, attachedVolume := range attachedVolumes {
		if attachedVolume.VolumeName == expectedVolumeName &&
			attachedVolume.VolumeSpec.Name() == expectedVolumeSpecName &&
			attachedVolume.NodeName == expectedNodeName &&
			attachedVolume.DevicePath == expectedDevicePath &&
			attachedVolume.MountedByNode == expectedMountedByNode &&
			attachedVolume.DetachRequestedTime.IsZero() == !expectNonZeroDetachRequestedTime {
			return
		}
	}

	t.Fatalf(
		"attachedVolumes (%v) should contain the volume/node combo %q/%q with DevicePath=%q MountedByNode=%v and NonZeroDetachRequestedTime=%v. It does not.",
		attachedVolumes,
		expectedVolumeName,
		expectedNodeName,
		expectedDevicePath,
		expectedMountedByNode,
		expectNonZeroDetachRequestedTime)
}
