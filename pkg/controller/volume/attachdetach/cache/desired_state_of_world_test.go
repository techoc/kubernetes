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

	v1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	controllervolumetesting "k8s.io/kubernetes/pkg/controller/volume/attachdetach/testing"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// Test_AddNode_Positive_NewNode 验证新增节点后：
//   - NodeExists 返回 true；
//   - 该节点没有卷需要挂载。
func Test_AddNode_Positive_NewNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")

	// 执行
	dsw.AddNode(nodeName)

	// 断言
	nodeExists := dsw.NodeExists(nodeName)
	if !nodeExists {
		t.Fatalf("Added node %q does not exist, it should.", nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_AddNode_Positive_ExistingNode 验证重复添加同名节点是幂等的：节点仍存在，
// 且不会产生重复的待挂载卷。
func Test_AddNode_Positive_ExistingNode(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")

	// 执行
	dsw.AddNode(nodeName)

	// 断言
	nodeExists := dsw.NodeExists(nodeName)
	if !nodeExists {
		t.Fatalf("Added node %q does not exist, it should.", nodeName)
	}

	// 执行
	dsw.AddNode(nodeName)

	// 断言
	nodeExists = dsw.NodeExists(nodeName)
	if !nodeExists {
		t.Fatalf("Added node %q does not exist, it should.", nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_AddPod_Positive_NewPodNodeExistsVolumeDoesntExist 验证向已存在节点
// 添加新 Pod/卷后生成一条待挂载记录。
func Test_AddPod_Positive_NewPodNodeExistsVolumeDoesntExist(t *testing.T) {
	// 准备
	podName := "pod-uid"
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	// 必须先添加节点，否则 AddPod 会因缺少归属节点而失败。
	dsw.AddNode(nodeName)
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	// 执行
	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)

	// 断言
	if podErr != nil {
		t.Fatalf("AddPod failed. Expected: <no error> Actual: <%v>", podErr)
	}

	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_AddPod_Positive_NewPodNodeExistsVolumeExists 验证两个不同 Pod 使用
// 同一节点与卷时：待挂载卷只计一条，但两个 Pod 都被记录。
func Test_AddPod_Positive_NewPodNodeExistsVolumeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	pod1Name := "pod1-uid"
	pod2Name := "pod2-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	// 执行
	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volumeSpec, nodeName)

	// 断言
	if podErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			podErr)
	}

	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			pod1Name,
			generatedVolumeName,
			nodeName)
	}

	// 执行
	// 第二个 Pod 复用同一卷，期望卷记录不重复。
	generatedVolumeName, podErr = dsw.AddPod(types.UniquePodName(pod2Name), controllervolumetesting.NewPod(pod2Name, pod2Name), volumeSpec, nodeName)

	// 断言
	if podErr != nil {
		t.Fatalf("AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2Name,
			podErr)
	}

	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			pod1Name,
			generatedVolumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	podScheduled := dsw.GetPodToAdd()
	if len(podScheduled) != 2 {
		t.Fatalf("len(podScheduled) Expected: <2> Actual: <%v>", len(podScheduled))
	}
	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_AddPod_Positive_PodExistsNodeExistsVolumeExists 验证重复添加同一 Pod
// 不会重复计数，待挂载卷仍只有一条。
func Test_AddPod_Positive_PodExistsNodeExistsVolumeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	// 执行
	generatedVolumeName, podErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)

	// 断言
	if podErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podErr)
	}

	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	// 执行
	// 重复添加同一 Pod/卷，验证幂等性。
	generatedVolumeName, podErr = dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)

	// 断言
	if podErr != nil {
		t.Fatalf("AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podErr)
	}

	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_AddPod_Negative_NewPodNodeDoesntExistVolumeDoesntExist 验证未先 AddNode
// 时 AddPod 必须失败，避免产生没有归属节点的悬空卷。
func Test_AddPod_Negative_NewPodNodeDoesntExistVolumeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	volumeExists := dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	// 执行
	// 故意跳过 AddNode，构造节点不存在的失败场景。
	_, podErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)

	// 断言
	if podErr == nil {
		t.Fatalf("AddPod did not fail. Expected: <\"failed to add pod...no node with that name exists in the list of managed nodes\"> Actual: <no error>")
	}

	volumeExists = dsw.VolumeExists(volumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Volume %q/node %q should not exist, but it does.",
			volumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_DeleteNode_Positive_NodeExists 验证删除空节点后 NodeExists 为 false
// 且无待挂载卷。
func Test_DeleteNode_Positive_NodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)

	// 执行
	// 节点为空，删除应成功。
	err := dsw.DeleteNode(nodeName)

	// 断言
	if err != nil {
		t.Fatalf("DeleteNode failed. Expected: <no error> Actual: <%v>", err)
	}

	nodeExists := dsw.NodeExists(nodeName)
	if nodeExists {
		t.Fatalf("Deleted node %q still exists, it should not.", nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_DeleteNode_Positive_NodeDoesntExist 验证删除不存在的节点不报错，
// 保证删除操作幂等。
func Test_DeleteNode_Positive_NodeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	notAddedNodeName := k8stypes.NodeName("node-not-added-name")

	// 执行
	// 对从未添加的节点执行删除，验证幂等。
	err := dsw.DeleteNode(notAddedNodeName)

	// 断言
	if err != nil {
		t.Fatalf("DeleteNode failed. Expected: <no error> Actual: <%v>", err)
	}

	nodeExists := dsw.NodeExists(notAddedNodeName)
	if nodeExists {
		t.Fatalf("Deleted node %q still exists, it should not.", notAddedNodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_DeleteNode_Negative_NodeExistsHasChildVolumes 验证仍持有卷的节点
// 不允许被删除，防止待挂载卷丢失归属。
func Test_DeleteNode_Negative_NodeExistsHasChildVolumes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podAddErr)
	}

	// 执行
	// 节点下仍有 Pod 引用的卷，删除应被拒绝。
	err := dsw.DeleteNode(nodeName)

	// 断言
	if err == nil {
		t.Fatalf("DeleteNode did not fail. Expected: <\"failed to delete node...the node still contains volumes in its list of volumes to attach\"> Actual: <no error>")
	}

	nodeExists := dsw.NodeExists(nodeName)
	if !nodeExists {
		t.Fatalf("Node %q no longer exists, it should.", nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_DeletePod_Positive_PodExistsNodeExistsVolumeExists 验证删除唯一引用
// 某卷的 Pod 后，该卷条目一并移除、待挂载列表清空。
func Test_DeletePod_Positive_PodExistsNodeExistsVolumeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podAddErr)
	}
	volumeExists := dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	// 执行
	// 删除唯一引用该卷的 Pod，卷条目应随之消失。
	dsw.DeletePod(types.UniquePodName(podName), generatedVolumeName, nodeName)

	// 断言
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if volumeExists {
		t.Fatalf(
			"Deleted pod %q from volume %q/node %q. Volume should also be deleted but it still exists.",
			podName,
			generatedVolumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_DeletePod_Positive_2PodsExistNodeExistsVolumesExist 验证两个 Pod 共享
// 同卷时，删除其中一个不会移除卷，待挂载列表仍保留一条。
func Test_DeletePod_Positive_2PodsExistNodeExistsVolumesExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	pod1Name := "pod1-uid"
	pod2Name := "pod2-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	generatedVolumeName1, pod1AddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volumeSpec, nodeName)
	if pod1AddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			pod1AddErr)
	}
	// 两个 Pod 共享同一卷，用于验证删除一个后卷仍在。
	generatedVolumeName2, pod2AddErr := dsw.AddPod(types.UniquePodName(pod2Name), controllervolumetesting.NewPod(pod2Name, pod2Name), volumeSpec, nodeName)
	if pod2AddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2Name,
			pod2AddErr)
	}
	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolumeName1,
			generatedVolumeName2)
	}
	volumeExists := dsw.VolumeExists(generatedVolumeName1, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Volume %q does not exist under node %q, it should.",
			generatedVolumeName1,
			nodeName)
	}

	// 执行
	// 只删除 pod1，pod2 仍引用该卷。
	dsw.DeletePod(types.UniquePodName(pod1Name), generatedVolumeName1, nodeName)

	// 断言
	volumeExists = dsw.VolumeExists(generatedVolumeName1, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Volume %q under node %q should still exist, but it does not.",
			generatedVolumeName1,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName1, string(volumeName))
}

// Test_DeletePod_Positive_PodDoesNotExist 验证删除不存在的 Pod 不影响
// 已有卷条目，保持待挂载列表不变。
func Test_DeletePod_Positive_PodDoesNotExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	pod1Name := "pod1-uid"
	pod2Name := "pod2-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	generatedVolumeName, pod1AddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volumeSpec, nodeName)
	if pod1AddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			pod1AddErr)
	}
	volumeExists := dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			pod1Name,
			generatedVolumeName,
			nodeName)
	}

	// 执行
	// 用不存在的 Pod 名删除，验证不会影响已有 Pod 的卷。
	dsw.DeletePod(types.UniquePodName(pod2Name), generatedVolumeName, nodeName)

	// 断言
	volumeExists = dsw.VolumeExists(generatedVolumeName, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Volume %q/node %q does not exist, it should.",
			generatedVolumeName,
			nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_DeletePod_Positive_NodeDoesNotExist 验证用错误节点删除 Pod 不会
// 误删节点1上的卷；节点2也不会被凭空创建。
func Test_DeletePod_Positive_NodeDoesNotExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	node1Name := k8stypes.NodeName("node1-name")
	dsw.AddNode(node1Name)
	generatedVolumeName, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podAddErr)
	}
	volumeExists := dsw.VolumeExists(generatedVolumeName, node1Name)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolumeName,
			node1Name)
	}
	node2Name := k8stypes.NodeName("node2-name")

	// 执行
	// 节点名指向不存在的 node2，验证不会误删 node1 上的卷。
	dsw.DeletePod(types.UniquePodName(podName), generatedVolumeName, node2Name)

	// 断言
	volumeExists = dsw.VolumeExists(generatedVolumeName, node1Name)
	if !volumeExists {
		t.Fatalf(
			"Volume %q/node %q does not exist, it should.",
			generatedVolumeName,
			node1Name)
	}
	volumeExists = dsw.VolumeExists(generatedVolumeName, node2Name)
	if volumeExists {
		t.Fatalf(
			"node %q exists, it should not.",
			node2Name)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, node1Name, generatedVolumeName, string(volumeName))
}

// Test_DeletePod_Positive_VolumeDoesNotExist 验证用不存在的卷名删除 Pod
// 不会误删已有的 volume1。
func Test_DeletePod_Positive_VolumeDoesNotExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	podName := "pod-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volume1Spec, nodeName)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podAddErr)
	}
	volumeExists := dsw.VolumeExists(generatedVolume1Name, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Added pod %q to volume %q/node %q. Volume does not exist, it should.",
			podName,
			generatedVolume1Name,
			nodeName)
	}
	volume2Name := v1.UniqueVolumeName("volume2-name")

	// 执行
	// 卷名指向不存在的 volume2，验证已有 volume1 不受影响。
	dsw.DeletePod(types.UniquePodName(podName), volume2Name, nodeName)

	// 断言
	volumeExists = dsw.VolumeExists(generatedVolume1Name, nodeName)
	if !volumeExists {
		t.Fatalf(
			"Volume %q/node %q does not exist, it should.",
			generatedVolume1Name,
			nodeName)
	}
	volumeExists = dsw.VolumeExists(volume2Name, nodeName)
	if volumeExists {
		t.Fatalf(
			"volume %q exists, it should not.",
			volume2Name)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolume1Name, string(volume1Name))
}

// Test_NodeExists_Positive_NodeExists 验证未添加的节点查询返回 false
// （函数名与测试场景相反，实际覆盖的是节点不存在的分支）。
func Test_NodeExists_Positive_NodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	notAddedNodeName := k8stypes.NodeName("node-not-added-name")

	// 执行
	// 查询从未添加的节点，验证返回 false。
	notAddedNodeExists := dsw.NodeExists(notAddedNodeName)

	// 断言
	if notAddedNodeExists {
		t.Fatalf("Node %q exists, it should not.", notAddedNodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_NodeExists_Positive_NodeDoesntExist 验证已添加节点的查询返回 true
// （测试名与断言场景相反，实际验证的是节点存在的分支）。
func Test_NodeExists_Positive_NodeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	// 先添加节点，再验证查询返回 true。
	dsw.AddNode(nodeName)

	// 执行
	nodeExists := dsw.NodeExists(nodeName)

	// 断言
	if !nodeExists {
		t.Fatalf("Node %q does not exist, it should.", nodeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_VolumeExists_Positive_VolumeExistsNodeExists 验证 AddPod 生成的卷名
// 能被 VolumeExists 找到，并产生一条待挂载记录。
func Test_VolumeExists_Positive_VolumeExistsNodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	podName := "pod-uid"
	volumeName := v1.UniqueVolumeName("volume-name")
	volumeSpec := controllervolumetesting.GetTestVolumeSpec(string(volumeName), volumeName)
	// 通过 AddPod 间接创建卷条目，再验证 VolumeExists 可见。
	generatedVolumeName, _ := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volumeSpec, nodeName)

	// 执行
	volumeExists := dsw.VolumeExists(generatedVolumeName, nodeName)

	// 断言
	if !volumeExists {
		t.Fatalf("Volume %q does not exist, it should.", generatedVolumeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolumeName, string(volumeName))
}

// Test_VolumeExists_Positive_VolumeDoesntExistNodeExists 验证按卷名精确查询：
// 未创建的 volume2 不存在，已有 volume1 不受影响。
func Test_VolumeExists_Positive_VolumeDoesntExistNodeExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	dsw.AddNode(nodeName)
	podName := "pod-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(podName), controllervolumetesting.NewPod(podName, podName), volume1Spec, nodeName)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			podName,
			podAddErr)
	}
	volume2Name := v1.UniqueVolumeName("volume2-name")

	// 执行
	// 用未创建的 volume2 查询，验证只匹配已存在的卷。
	volumeExists := dsw.VolumeExists(volume2Name, nodeName)

	// 断言
	if volumeExists {
		t.Fatalf("Volume %q exists, it should not.", volume2Name)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 1 {
		t.Fatalf("len(volumesToAttach) Expected: <1> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, nodeName, generatedVolume1Name, string(volume1Name))
}

// Test_VolumeExists_Positive_VolumeDoesntExistNodeDoesntExists 验证空 DSW 上
// 查询卷返回 false，且不会产生待挂载记录。
func Test_VolumeExists_Positive_VolumeDoesntExistNodeDoesntExists(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	nodeName := k8stypes.NodeName("node-name")
	volumeName := v1.UniqueVolumeName("volume-name")

	// 执行
	// 空 DSW 上的查询应安全返回 false。
	volumeExists := dsw.VolumeExists(volumeName, nodeName)

	// 断言
	if volumeExists {
		t.Fatalf("Volume %q exists, it should not.", volumeName)
	}

	volumesToAttach := dsw.GetVolumesToAttach()
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_GetVolumesToAttach_Positive_NoNodes 验证没有任何节点时待挂载列表为空。
func Test_GetVolumesToAttach_Positive_NoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)

	// 执行
	volumesToAttach := dsw.GetVolumesToAttach()

	// 断言
	if len(volumesToAttach) > 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_GetVolumesToAttach_Positive_TwoNodes 验证仅有节点没有 Pod 时
// 待挂载列表仍为空。
func Test_GetVolumesToAttach_Positive_TwoNodes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	node1Name := k8stypes.NodeName("node1-name")
	node2Name := k8stypes.NodeName("node2-name")
	// 只添加节点不加 Pod，构造有节点但无待挂载卷的场景。
	dsw.AddNode(node1Name)
	dsw.AddNode(node2Name)

	// 执行
	volumesToAttach := dsw.GetVolumesToAttach()

	// 断言
	if len(volumesToAttach) != 0 {
		t.Fatalf("len(volumesToAttach) Expected: <0> Actual: <%v>", len(volumesToAttach))
	}
}

// Test_GetVolumesToAttach_Positive_TwoNodesOneVolumeEach 验证两个节点各有一卷时
// GetVolumesToAttach 返回两条记录，且不混淆节点归属。
func Test_GetVolumesToAttach_Positive_TwoNodesOneVolumeEach(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	node1Name := k8stypes.NodeName("node1-name")
	pod1Name := "pod1-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	dsw.AddNode(node1Name)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volume1Spec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			podAddErr)
	}
	node2Name := k8stypes.NodeName("node2-name")
	pod2Name := "pod2-uid"
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	dsw.AddNode(node2Name)
	// 两个节点各有一卷，验证结果按节点聚合。
	generatedVolume2Name, podAddErr := dsw.AddPod(types.UniquePodName(pod2Name), controllervolumetesting.NewPod(pod2Name, pod2Name), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2Name,
			podAddErr)
	}

	// 执行
	volumesToAttach := dsw.GetVolumesToAttach()

	// 断言
	if len(volumesToAttach) != 2 {
		t.Fatalf("len(volumesToAttach) Expected: <2> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, node1Name, generatedVolume1Name, string(volume1Name))
	verifyVolumeToAttach(t, volumesToAttach, node2Name, generatedVolume2Name, string(volume2Name))
}

// Test_GetVolumesToAttach_Positive_TwoNodesOneVolumeEachExtraPod 验证同一
// 节点/卷被多个 Pod 引用时仍只产生一条待挂载记录。
func Test_GetVolumesToAttach_Positive_TwoNodesOneVolumeEachExtraPod(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	node1Name := k8stypes.NodeName("node1-name")
	pod1Name := "pod1-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	dsw.AddNode(node1Name)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volume1Spec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			podAddErr)
	}
	node2Name := k8stypes.NodeName("node2-name")
	pod2Name := "pod2-uid"
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	dsw.AddNode(node2Name)
	generatedVolume2Name, podAddErr := dsw.AddPod(types.UniquePodName(pod2Name), controllervolumetesting.NewPod(pod2Name, pod2Name), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2Name,
			podAddErr)
	}
	// 为节点2的卷再添加一个 Pod，验证卷只计一次。
	pod3Name := "pod3-uid"
	dsw.AddPod(types.UniquePodName(pod3Name), controllervolumetesting.NewPod(pod3Name, pod3Name), volume2Spec, node2Name)
	// 同一 Pod 再次添加，验证不会产生重复记录。
	_, podAddErr = dsw.AddPod(types.UniquePodName(pod3Name), controllervolumetesting.NewPod(pod3Name, pod3Name), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod3Name,
			podAddErr)
	}

	// 执行
	volumesToAttach := dsw.GetVolumesToAttach()

	// 断言
	if len(volumesToAttach) != 2 {
		t.Fatalf("len(volumesToAttach) Expected: <2> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, node1Name, generatedVolume1Name, string(volume1Name))
	verifyVolumeToAttach(t, volumesToAttach, node2Name, generatedVolume2Name, string(volume2Name))
}

// Test_GetVolumesToAttach_Positive_TwoNodesThreeVolumes 验证多节点多卷场景下
// 统计正确，且同名卷的两个 Pod 只计一条。
func Test_GetVolumesToAttach_Positive_TwoNodesThreeVolumes(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)
	node1Name := k8stypes.NodeName("node1-name")
	pod1Name := "pod1-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	dsw.AddNode(node1Name)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volume1Spec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			podAddErr)
	}
	node2Name := k8stypes.NodeName("node2-name")
	pod2aName := "pod2a-name"
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	dsw.AddNode(node2Name)
	generatedVolume2Name1, podAddErr := dsw.AddPod(types.UniquePodName(pod2aName), controllervolumetesting.NewPod(pod2aName, pod2aName), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2aName,
			podAddErr)
	}
	// 节点2上同名卷的第二个 Pod，验证同名卷只计一条。
	pod2bName := "pod2b-name"
	generatedVolume2Name2, podAddErr := dsw.AddPod(types.UniquePodName(pod2bName), controllervolumetesting.NewPod(pod2bName, pod2bName), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2bName,
			podAddErr)
	}
	if generatedVolume2Name1 != generatedVolume2Name2 {
		t.Fatalf(
			"Generated volume names for the same volume should be the same but they are not: %q and %q",
			generatedVolume2Name1,
			generatedVolume2Name2)
	}
	pod3Name := "pod3-uid"
	volume3Name := v1.UniqueVolumeName("volume3-name")
	volume3Spec := controllervolumetesting.GetTestVolumeSpec(string(volume3Name), volume3Name)
	generatedVolume3Name, podAddErr := dsw.AddPod(types.UniquePodName(pod3Name), controllervolumetesting.NewPod(pod3Name, pod3Name), volume3Spec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod3Name,
			podAddErr)
	}

	// 执行
	volumesToAttach := dsw.GetVolumesToAttach()

	// 断言
	if len(volumesToAttach) != 3 {
		t.Fatalf("len(volumesToAttach) Expected: <3> Actual: <%v>", len(volumesToAttach))
	}

	verifyVolumeToAttach(t, volumesToAttach, node1Name, generatedVolume1Name, string(volume1Name))
	verifyVolumeToAttach(t, volumesToAttach, node2Name, generatedVolume2Name1, string(volume2Name))
	verifyVolumeToAttach(t, volumesToAttach, node1Name, generatedVolume3Name, string(volume3Name))
}

// verifyVolumeToAttach 在待挂载列表中查找与期望节点、卷名和卷规格名匹配的
// 条目；找不到时立即 Fatalf，避免测试静默通过。
func verifyVolumeToAttach(
	t *testing.T,
	volumesToAttach []VolumeToAttach,
	expectedNodeName k8stypes.NodeName,
	expectedVolumeName v1.UniqueVolumeName,
	expectedVolumeSpecName string) {
	for _, volumeToAttach := range volumesToAttach {
		if volumeToAttach.NodeName == expectedNodeName &&
			volumeToAttach.VolumeName == expectedVolumeName &&
			volumeToAttach.VolumeSpec.Name() == expectedVolumeSpecName {
			return
		}
	}

	t.Fatalf("volumesToAttach (%v) should contain %q/%q. It does not.", volumesToAttach, expectedVolumeName, expectedNodeName)
}

// Test_GetPodsOnNodes 验证 GetVolumePodsOnNodes 只返回指定卷的 Pod：
//   - 两个节点各有一个不同卷的 Pod，查询 volume1 只应得到 pod1；
//   - 空节点与不存在的节点不会引入额外结果。
func Test_GetPodsOnNodes(t *testing.T) {
	volumePluginMgr, _ := volumetesting.GetTestVolumePluginMgr(t)
	dsw := NewDesiredStateOfWorld(volumePluginMgr)

	// 两个节点各有一个 Pod，分别使用不同的卷
	node1Name := k8stypes.NodeName("node1-name")
	pod1Name := "pod1-uid"
	volume1Name := v1.UniqueVolumeName("volume1-name")
	volume1Spec := controllervolumetesting.GetTestVolumeSpec(string(volume1Name), volume1Name)
	dsw.AddNode(node1Name)
	generatedVolume1Name, podAddErr := dsw.AddPod(types.UniquePodName(pod1Name), controllervolumetesting.NewPod(pod1Name, pod1Name), volume1Spec, node1Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod1Name,
			podAddErr)
	}
	node2Name := k8stypes.NodeName("node2-name")
	pod2Name := "pod2-uid"
	volume2Name := v1.UniqueVolumeName("volume2-name")
	volume2Spec := controllervolumetesting.GetTestVolumeSpec(string(volume2Name), volume2Name)
	dsw.AddNode(node2Name)
	_, podAddErr = dsw.AddPod(types.UniquePodName(pod2Name), controllervolumetesting.NewPod(pod2Name, pod2Name), volume2Spec, node2Name)
	if podAddErr != nil {
		t.Fatalf(
			"AddPod failed for pod %q. Expected: <no error> Actual: <%v>",
			pod2Name,
			podAddErr)
	}

	// 第三个节点没有任何 Pod
	node3Name := k8stypes.NodeName("node3-name")
	dsw.AddNode(node3Name)

	// 执行
	// 传入空节点与不存在的节点，验证只返回 volume1 对应的 Pod。
	pods := dsw.GetVolumePodsOnNodes([]k8stypes.NodeName{node1Name, node2Name, node3Name, "non-existing-node"}, generatedVolume1Name)

	// 断言
	if len(pods) != 1 {
		t.Fatalf("Expected 1 pod, got %d", len(pods))
	}
	if pods[0].Name != pod1Name {
		t.Errorf("Expected pod %s/%s, got %s", pod1Name, pod1Name, pods[0].Name)
	}
}
