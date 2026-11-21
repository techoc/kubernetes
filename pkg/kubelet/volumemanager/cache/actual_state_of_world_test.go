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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// emptyVolumeName 表示调用方未指定卷名，ASW 会自行生成唯一卷名。
var emptyVolumeName = v1.UniqueVolumeName("")

// 调用一次 MarkVolumeAsAttached() 添加卷。
// 验证新卷出现在 GetUnmountedVolumes() 中。
// 验证新卷不出现在 GetGloballyMountedVolumes() 中。
func Test_MarkVolumeAsAttached_Positive_NewVolume(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	devicePath := "fake/device/path"
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)

	// 断言
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeExistsInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName, asw)
}

// 调用一次 MarkVolumeAsAttached() 并显式指定卷名，用于验证 ASW 使用传入的卷名
// 而不是自动生成的卷名来注册卷。
// 验证新卷出现在 GetUnmountedVolumes() 中。
// 验证新卷不出现在 GetGloballyMountedVolumes() 中。
func Test_MarkVolumeAsAttached_SuppliedVolumeName_Positive_NewVolume(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	devicePath := "fake/device/path"
	// 显式指定的卷名故意不符合正常命名规则，用来证明 ASW 不会替换它。
	volumeName := v1.UniqueVolumeName("this-would-never-be-a-volume-name")

	// 执行
	logger, _ := ktesting.NewTestContext(t)
	err := asw.MarkVolumeAsAttached(logger, volumeName, volumeSpec, "" /* nodeName */, devicePath)

	// 断言
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, volumeName, true /* shouldExist */, asw)
	verifyVolumeExistsInUnmountedVolumes(t, volumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, volumeName, asw)
}

// 调用两次 MarkVolumeAsAttached() 添加同一个卷。
// 验证第二次调用不失败（幂等）。
// 验证卷出现在 GetUnmountedVolumes() 中。
// 验证卷不出现在 GetGloballyMountedVolumes() 中。
func Test_MarkVolumeAsAttached_Positive_ExistingVolume(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	devicePath := "fake/device/path"
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)

	// 断言
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeExistsInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName, asw)
}

// 先向数据结构中填充一个卷。
// 调用 AddPodToVolume() 把 pod 关联到该卷。
// 通过 PodExistsInVolume() 验证卷/pod 组合存在。
func Test_AddPodToVolume_Positive_ExistingVolumeNewNode(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	devicePath := "fake/device/path"

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	podName := util.GetUniquePodName(pod)

	mounter, err := plugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := plugin.NewBlockVolumeMapper(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	markVolumeOpts := operationexecutor.MarkVolumeOpts{
		PodName:           podName,
		PodUID:            pod.UID,
		VolumeName:        generatedVolumeName,
		Mounter:           mounter,
		BlockVolumeMapper: mapper,
		VolumeSpec:        volumeSpec,
	}
	err = asw.AddPodToVolume(markVolumeOpts)
	// 断言
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// 卷已被 pod 占用，因此不应再出现在 unmounted 或 globally mounted 列表中。
	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeDoesntExistInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName, asw)
	verifyPodExistsInVolumeAsw(t, podName, generatedVolumeName, "fake/device/path" /* expectedDevicePath */, asw)
	verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName, volumeSpec.Name(), asw)
	verifyVolumeMountedElsewhere(t, podName, generatedVolumeName, false /*expectedMountedElsewhere */, asw)
}

// 先向数据结构中填充一个卷。
// 调用两次 AddPodToVolume() 把同一个 pod 关联到该卷。
// 通过 PodExistsInVolume() 验证卷/pod 组合存在，且第二次调用没有失败。
func Test_AddPodToVolume_Positive_ExistingVolumeExistingNode(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	devicePath := "fake/device/path"

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}

	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	podName := util.GetUniquePodName(pod)

	mounter, err := plugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := plugin.NewBlockVolumeMapper(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	markVolumeOpts := operationexecutor.MarkVolumeOpts{
		PodName:           podName,
		PodUID:            pod.UID,
		VolumeName:        generatedVolumeName,
		Mounter:           mounter,
		BlockVolumeMapper: mapper,
		VolumeSpec:        volumeSpec,
	}
	err = asw.AddPodToVolume(markVolumeOpts)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行：重复添加同一 pod 到同一卷必须幂等。
	err = asw.AddPodToVolume(markVolumeOpts)
	// 断言
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeDoesntExistInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName, asw)
	verifyPodExistsInVolumeAsw(t, podName, generatedVolumeName, "fake/device/path" /* expectedDevicePath */, asw)
	verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName, volumeSpec.Name(), asw)
	verifyVolumeMountedElsewhere(t, podName, generatedVolumeName, false /*expectedMountedElsewhere */, asw)
}

// 把两个 pod 关联到同一个卷（两个 pod 的 spec 解析出相同唯一卷名）。
// 验证两个 pod 都能看到该卷，并互相判定为 "mounted elsewhere"。
func Test_AddTwoPodsToVolume_Positive(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	devicePath := "fake/device/path"

	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name-1",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}

	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod2",
			UID:  "pod2uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name-2",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec1 := &volume.Spec{Volume: &pod1.Spec.Volumes[0]}
	volumeSpec2 := &volume.Spec{Volume: &pod2.Spec.Volumes[0]}
	generatedVolumeName1, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec1)
	require.NoError(t, err)
	generatedVolumeName2, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec2)
	require.NoError(t, err)

	if generatedVolumeName1 != generatedVolumeName2 {
		t.Fatalf(
			"Unique volume names should be the same. unique volume name 1: <%q> unique volume name 2: <%q>, spec1 %v, spec2 %v",
			generatedVolumeName1,
			generatedVolumeName2, volumeSpec1, volumeSpec2)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, generatedVolumeName1, volumeSpec1, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	podName1 := util.GetUniquePodName(pod1)

	mounter1, err := plugin.NewMounter(volumeSpec1, pod1)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper1, err := plugin.NewBlockVolumeMapper(volumeSpec1, pod1)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	markVolumeOpts1 := operationexecutor.MarkVolumeOpts{
		PodName:           podName1,
		PodUID:            pod1.UID,
		VolumeName:        generatedVolumeName1,
		Mounter:           mounter1,
		BlockVolumeMapper: mapper1,
		VolumeSpec:        volumeSpec1,
	}
	err = asw.AddPodToVolume(markVolumeOpts1)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	podName2 := util.GetUniquePodName(pod2)

	mounter2, err := plugin.NewMounter(volumeSpec2, pod2)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper2, err := plugin.NewBlockVolumeMapper(volumeSpec2, pod2)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	markVolumeOpts2 := operationexecutor.MarkVolumeOpts{
		PodName:           podName2,
		PodUID:            pod2.UID,
		VolumeName:        generatedVolumeName1,
		Mounter:           mounter2,
		BlockVolumeMapper: mapper2,
		VolumeSpec:        volumeSpec2,
	}
	err = asw.AddPodToVolume(markVolumeOpts2)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, generatedVolumeName1, true /* shouldExist */, asw)
	verifyVolumeDoesntExistInUnmountedVolumes(t, generatedVolumeName1, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName1, asw)
	verifyPodExistsInVolumeAsw(t, podName1, generatedVolumeName1, "fake/device/path" /* expectedDevicePath */, asw)
	verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName1, volumeSpec1.Name(), asw)
	verifyPodExistsInVolumeAsw(t, podName2, generatedVolumeName2, "fake/device/path" /* expectedDevicePath */, asw)
	verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName2, volumeSpec2.Name(), asw)
	verifyVolumeSpecNameInVolumeAsw(t, podName1, []*volume.Spec{volumeSpec1}, asw)
	verifyVolumeSpecNameInVolumeAsw(t, podName2, []*volume.Spec{volumeSpec2}, asw)
	verifyVolumeMountedElsewhere(t, podName1, generatedVolumeName1, true /*expectedMountedElsewhere */, asw)
	verifyVolumeMountedElsewhere(t, podName2, generatedVolumeName2, true /*expectedMountedElsewhere */, asw)
}

// 验证重建阶段从磁盘发现的卷能被 ASW 正确处理：
// 挂载、卸载或彻底移除后都应从 "found during reconstruction" 记录中清除；
// 不确定的可附加性一旦确定，就不允许再被改写。
func TestActualStateOfWorld_FoundDuringReconstruction(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name           string
		opCallback     func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error
		verifyCallback func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error
	}{
		{
			name: "marking volume mounted should remove volume from found during reconstruction",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				volumeOpts.VolumeMountState = operationexecutor.VolumeMounted
				return asw.MarkVolumeAsMounted(volumeOpts)
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				ok := asw.IsVolumeReconstructed(volumeOpts.VolumeName, volumeOpts.PodName)
				if ok {
					return fmt.Errorf("found unexpected volume in reconstructed volume list")
				}
				return nil
			},
		},
		{
			name: "removing volume from pod should remove volume from found during reconstruction",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				return asw.MarkVolumeAsUnmounted(volumeOpts.PodName, volumeOpts.VolumeName)
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				ok := asw.IsVolumeReconstructed(volumeOpts.VolumeName, volumeOpts.PodName)
				if ok {
					return fmt.Errorf("found unexpected volume in reconstructed volume list")
				}
				return nil
			},
		},
		{
			name: "removing volume entirely from ASOW should remove volume from found during reconstruction",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				err := asw.MarkVolumeAsUnmounted(volumeOpts.PodName, volumeOpts.VolumeName)
				if err != nil {
					return err
				}
				asw.MarkVolumeAsDetached(volumeOpts.VolumeName, "")
				return nil
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				ok := asw.IsVolumeReconstructed(volumeOpts.VolumeName, volumeOpts.PodName)
				if ok {
					return fmt.Errorf("found unexpected volume in reconstructed volume list")
				}
				aswInstance, _ := asw.(*actualStateOfWorld)
				_, found := aswInstance.foundDuringReconstruction[volumeOpts.VolumeName]
				if found {
					return fmt.Errorf("found unexpected volume in reconstructed map")
				}
				return nil
			},
		},
		{
			name: "uncertain attachability is resolved to attachable",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, true)
				return nil
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				verifyVolumeAttachability(t, volumeOpts.VolumeName, asw, volumeAttachabilityTrue)
				return nil
			},
		},
		{
			name: "uncertain attachability is resolved to non-attachable",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, false)
				return nil
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				verifyVolumeAttachability(t, volumeOpts.VolumeName, asw, volumeAttachabilityFalse)
				return nil
			},
		},
		{
			name: "certain (false) attachability cannot be changed",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, false)
				// 已确定后再次更新应为空操作（NOOP）：
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, true)
				return nil
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				verifyVolumeAttachability(t, volumeOpts.VolumeName, asw, volumeAttachabilityFalse)
				return nil
			},
		},
		{
			name: "certain (true) attachability cannot be changed",
			opCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, true)
				// 已确定后再次更新应为空操作（NOOP）：
				asw.UpdateReconstructedVolumeAttachability(volumeOpts.VolumeName, false)
				return nil
			},
			verifyCallback: func(asw ActualStateOfWorld, volumeOpts operationexecutor.MarkVolumeOpts) error {
				verifyVolumeAttachability(t, volumeOpts.VolumeName, asw, volumeAttachabilityTrue)
				return nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
			asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
			devicePath := "fake/device/path"

			pod1 := getTestPod("pod1", "pod1uid", "volume-name-1", "fake-device1")
			volumeSpec1 := &volume.Spec{Volume: &pod1.Spec.Volumes[0]}
			generatedVolumeName1, err := util.GetUniqueVolumeNameFromSpec(
				plugin, volumeSpec1)
			require.NoError(t, err)
			// 用"attachability 不确定"的重建卷构造前置状态，各用例只负责执行操作并校验结果。
			err = asw.AddAttachUncertainReconstructedVolume(logger, generatedVolumeName1, volumeSpec1, "" /* nodeName */, devicePath)
			if err != nil {
				t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
			}
			podName1 := util.GetUniquePodName(pod1)

			mounter1, err := plugin.NewMounter(volumeSpec1, pod1)
			if err != nil {
				t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
			}

			mapper1, err := plugin.NewBlockVolumeMapper(volumeSpec1, pod1)
			if err != nil {
				t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
			}

			markVolumeOpts1 := operationexecutor.MarkVolumeOpts{
				PodName:           podName1,
				PodUID:            pod1.UID,
				VolumeName:        generatedVolumeName1,
				Mounter:           mounter1,
				BlockVolumeMapper: mapper1,
				VolumeSpec:        volumeSpec1,
				VolumeMountState:  operationexecutor.VolumeMountUncertain,
			}
			_, err = asw.CheckAndMarkVolumeAsUncertainViaReconstruction(markVolumeOpts1)
			if err != nil {
				t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
			}
			// 确认前置状态符合预期，否则后续用例的失败原因会难以定位。
			verifyVolumeExistsAsw(t, generatedVolumeName1, true /* shouldExist */, asw)
			verifyVolumeDoesntExistInUnmountedVolumes(t, generatedVolumeName1, asw)
			verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName1, asw)
			verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName1, volumeSpec1.Name(), asw)
			verifyVolumeSpecNameInVolumeAsw(t, podName1, []*volume.Spec{volumeSpec1}, asw)
			verifyVolumeFoundInReconstruction(t, podName1, generatedVolumeName1, asw)
			verifyVolumeAttachability(t, generatedVolumeName1, asw, volumeAttachabilityUncertain)

			if tc.opCallback != nil {
				err = tc.opCallback(asw, markVolumeOpts1)
				if err != nil {
					t.Fatalf("for test %s: %v", tc.name, err)
				}
			}
			err = tc.verifyCallback(asw, markVolumeOpts1)
			if err != nil {
				t.Fatalf("for test %s verification failed: %v", tc.name, err)
			}
		})
	}
}

// 对仍被 pod 挂载的卷调用 MarkVolumeAsDetached() 应被跳过，
// 保证 pod 与卷的关联不被意外破坏。
func Test_MarkVolumeAsDetached_Negative_PodInVolume(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestVolumePluginMgr(t)
	devicePath := "fake/device/path"
	asw := NewActualStateOfWorld("mynode", volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	logger, _ := ktesting.NewTestContext(t)
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	err := asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}

	podName := util.GetUniquePodName(pod)
	mounter, err := plugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := plugin.NewBlockVolumeMapper(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	markVolumeOpts := operationexecutor.MarkVolumeOpts{
		PodName:           podName,
		PodUID:            pod.UID,
		VolumeName:        generatedVolumeName,
		Mounter:           mounter,
		BlockVolumeMapper: mapper,
		VolumeSpec:        volumeSpec,
	}
	err = asw.AddPodToVolume(markVolumeOpts)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	asw.MarkVolumeAsDetached(generatedVolumeName, "" /* nodeName */)

	// 断言：pod 与卷的关联必须仍然存在。
	verifyPodExistsInVolumeAsw(t, podName, generatedVolumeName, "fake/device/path" /* expectedDevicePath */, asw)
}

// getTestPod 构造一个挂载 GCEPersistentDisk 的最小 pod，供多个用例复用。
func getTestPod(podName, podUID, outerVolumeName, pdName string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  types.UID(podUID),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: outerVolumeName,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: pdName,
						},
					},
				},
			},
		},
	}
	return pod
}

// 在空的 ASW 上调用 AddPodToVolume() 添加 pod。
// 验证调用失败并返回 "volume does not exist" 错误。
func Test_AddPodToVolume_Negative_VolumeDoesntExist(t *testing.T) {
	// 准备
	volumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}

	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	plugin, err := volumePluginMgr.FindPluginBySpec(volumeSpec)
	if err != nil {
		t.Fatalf(
			"volumePluginMgr.FindPluginBySpec failed to find volume plugin for %#v with: %v",
			volumeSpec,
			err)
	}

	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec)
	require.NoError(t, err)

	blockplugin, err := volumePluginMgr.FindMapperPluginBySpec(volumeSpec)
	if err != nil {
		t.Fatalf(
			"volumePluginMgr.FindMapperPluginBySpec failed to find volume plugin for %#v with: %v",
			volumeSpec,
			err)
	}

	volumeName, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec)
	require.NoError(t, err)

	podName := util.GetUniquePodName(pod)

	mounter, err := plugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := blockplugin.NewBlockVolumeMapper(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	markVolumeOpts := operationexecutor.MarkVolumeOpts{
		PodName:           podName,
		PodUID:            pod.UID,
		VolumeName:        volumeName,
		Mounter:           mounter,
		BlockVolumeMapper: mapper,
		VolumeSpec:        volumeSpec,
	}
	err = asw.AddPodToVolume(markVolumeOpts)
	// 断言：卷未附加时必须报错，否则后续挂载会作用在不存在的卷上。
	if err == nil {
		t.Fatalf("AddPodToVolume did not fail. Expected: <\"no volume with the name ... exists in the list of attached volumes\"> Actual: <no error>")
	}

	verifyVolumeExistsAsw(t, volumeName, false /* shouldExist */, asw)
	verifyVolumeDoesntExistInUnmountedVolumes(t, volumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, volumeName, asw)
	verifyPodDoesntExistInVolumeAsw(
		t,
		podName,
		volumeName,
		false, /* expectVolumeToExist */
		asw)
	verifyVolumeDoesntExistWithSpecNameInVolumeAsw(t, podName, volumeSpec.Name(), asw)
	verifyVolumeMountedElsewhere(t, podName, generatedVolumeName, false /*expectedMountedElsewhere */, asw)
}

// 调用一次 MarkVolumeAsAttached() 添加卷。
// 调用 MarkDeviceAsMounted() 把卷标记为全局挂载。
// 验证卷同时出现在 GetUnmountedVolumes() 与 GetGloballyMountedVolumes() 中。
func Test_MarkDeviceAsMounted_Positive_NewVolume(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	devicePath := "fake/device/path"
	deviceMountPath := "fake/device/mount/path"
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	err = asw.MarkDeviceAsMounted(generatedVolumeName, devicePath, deviceMountPath, "")

	// 断言
	if err != nil {
		t.Fatalf("MarkDeviceAsMounted failed. Expected: <no error> Actual: <%v>", err)
	}

	// 全局挂载不会让卷从 unmounted 列表消失，两者是不同维度的状态。
	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeExistsInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeExistsInGloballyMountedVolumes(t, generatedVolumeName, asw)
}

// 向数据结构中填充一个带 SELinux 上下文的卷。
// 调用 AddPodToVolume() 把 pod 关联到该卷。
// 通过 PodExistsInVolume() 验证卷/pod 组合存在且 SELinux 上下文正确。
func Test_AddPodToVolume_Positive_SELinux(t *testing.T) {
	// 准备
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, true)
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	devicePath := "fake/device/path"

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	podName := util.GetUniquePodName(pod)

	mounter, err := plugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := plugin.NewBlockVolumeMapper(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewBlockVolumeMapper failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	markVolumeOpts := operationexecutor.MarkVolumeOpts{
		PodName:             podName,
		PodUID:              pod.UID,
		VolumeName:          generatedVolumeName,
		Mounter:             mounter,
		BlockVolumeMapper:   mapper,
		VolumeSpec:          volumeSpec,
		SELinuxMountContext: "system_u:object_r:container_file_t:s0:c0,c1",
		VolumeMountState:    operationexecutor.VolumeMounted,
	}
	err = asw.AddPodToVolume(markVolumeOpts)
	// 断言
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// 除上下文匹配的断言外，还要验证不匹配的 SELinux 标签会被拒绝。
	verifyVolumeExistsAswWithSELinux(t, generatedVolumeName, "system_u:object_r:container_file_t:s0:c0,c1", asw)
	verifyVolumeDoesntExistInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeDoesntExistInGloballyMountedVolumes(t, generatedVolumeName, asw)
	verifyPodExistsInVolumeAswWithSELinux(t, podName, generatedVolumeName, "fake/device/path" /* expectedDevicePath */, "system_u:object_r:container_file_t:s0:c0,c1", asw)
	verifyPodExistsInVolumeSELinuxMismatch(t, podName, generatedVolumeName, "" /* wrong SELinux label */, asw)
	verifyVolumeExistsWithSpecNameInVolumeAsw(t, podName, volumeSpec.Name(), asw)
	verifyVolumeMountedElsewhere(t, podName, generatedVolumeName, false /*expectedMountedElsewhere */, asw)
}

// 调用一次 MarkVolumeAsAttached() 添加卷。
// 调用带 SELinux 上下文的 MarkDeviceAsMounted() 把卷标记为全局挂载。
// 验证卷出现在 GetUnmountedVolumes() 与 GetGloballyMountedVolumes() 中，且上下文一致。
func Test_MarkDeviceAsMounted_Positive_SELinux(t *testing.T) {
	// 准备
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, true)
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	devicePath := "fake/device/path"
	deviceMountPath := "fake/device/mount/path"
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed. Expected: <no error> Actual: <%v>", err)
	}
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	err = asw.MarkDeviceAsMounted(generatedVolumeName, devicePath, deviceMountPath, "system_u:system_r:container_t:s0:c0,c1")

	// 断言
	if err != nil {
		t.Fatalf("MarkDeviceAsMounted failed. Expected: <no error> Actual: <%v>", err)
	}

	verifyVolumeExistsAsw(t, generatedVolumeName, true /* shouldExist */, asw)
	verifyVolumeExistsInUnmountedVolumes(t, generatedVolumeName, asw)
	verifyVolumeExistsInGloballyMountedVolumesWithSELinux(t, generatedVolumeName, "system_u:system_r:container_t:s0:c0,c1", asw)
}

// 验证挂载状态不确定的卷：既不算已挂载也不算已卸载，但必须出现在 "possibly mounted" 列表中，
// 且 PodExistsInVolume/PodRemovedFromVolume 对其返回 false，避免误判为已挂载。
func TestUncertainVolumeMounts(t *testing.T) {
	// 准备
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode" /* nodeName */, volumePluginMgr)
	devicePath := "fake/device/path"

	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume-name-1",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device1",
						},
					},
				},
			},
		},
	}
	volumeSpec1 := &volume.Spec{Volume: &pod1.Spec.Volumes[0]}
	generatedVolumeName1, err := util.GetUniqueVolumeNameFromSpec(
		plugin, volumeSpec1)
	require.NoError(t, err)
	logger, _ := ktesting.NewTestContext(t)
	err = asw.MarkVolumeAsAttached(logger, generatedVolumeName1, volumeSpec1, "" /* nodeName */, devicePath)
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}
	podName1 := util.GetUniquePodName(pod1)

	mounter1, err := plugin.NewMounter(volumeSpec1, pod1)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	markVolumeOpts1 := operationexecutor.MarkVolumeOpts{
		PodName:          podName1,
		PodUID:           pod1.UID,
		VolumeName:       generatedVolumeName1,
		Mounter:          mounter1,
		VolumeSpec:       volumeSpec1,
		VolumeMountState: operationexecutor.VolumeMountUncertain,
	}
	err = asw.AddPodToVolume(markVolumeOpts1)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}
	// 不确定挂载的卷不能出现在"已挂载"列表里，否则会误判为可安全使用。
	mountedVolumes := asw.GetMountedVolumesForPod(podName1)
	volumeFound := false
	for _, volume := range mountedVolumes {
		if volume.InnerVolumeSpecName == volumeSpec1.Name() {
			volumeFound = true
		}
	}
	if volumeFound {
		t.Fatalf("expected volume %s to be not found in asw.GetMountedVolumesForPod", volumeSpec1.Name())
	}

	// 但它必须出现在"可能已挂载"列表中，供清理路径处理。
	possiblyMountedVolumes := asw.GetPossiblyMountedVolumesForPod(podName1)
	volumeFound = false
	for _, volume := range possiblyMountedVolumes {
		if volume.InnerVolumeSpecName == volumeSpec1.Name() {
			volumeFound = true
		}
	}
	if !volumeFound {
		t.Fatalf("expected volume %s to be found in aws.GetPossiblyMountedVolumesForPod", volumeSpec1.Name())
	}

	// 状态不确定时不应宣称 pod 已存在该卷，也不应允许移除，交由重建流程裁决。
	volExists, _, _ := asw.PodExistsInVolume(logger, podName1, generatedVolumeName1, resource.Quantity{}, "")
	if volExists {
		t.Fatalf("expected volume %s to not exist in asw", generatedVolumeName1)
	}
	removed := asw.PodRemovedFromVolume(podName1, generatedVolumeName1)
	if removed {
		t.Fatalf("expected volume %s not to be removed in asw", generatedVolumeName1)
	}
}

// verifyVolumeExistsInGloballyMountedVolumes 断言卷出现在全局挂载列表中。
func verifyVolumeExistsInGloballyMountedVolumes(
	t *testing.T, expectedVolumeName v1.UniqueVolumeName, asw ActualStateOfWorld) {
	globallyMountedVolumes := asw.GetGloballyMountedVolumes()
	for _, volume := range globallyMountedVolumes {
		if volume.VolumeName == expectedVolumeName {
			return
		}
	}

	t.Fatalf(
		"Could not find volume %v in the list of GloballyMountedVolumes for actual state of world %+v",
		expectedVolumeName,
		globallyMountedVolumes)
}

// verifyVolumeExistsInGloballyMountedVolumesWithSELinux 断言卷在全局挂载列表中且 SELinux 上下文匹配。
func verifyVolumeExistsInGloballyMountedVolumesWithSELinux(
	t *testing.T, expectedVolumeName v1.UniqueVolumeName, expectedSELinuxContext string, asw ActualStateOfWorld) {
	globallyMountedVolumes := asw.GetGloballyMountedVolumes()
	for _, volume := range globallyMountedVolumes {
		if volume.VolumeName == expectedVolumeName {
			if volume.SELinuxMountContext == expectedSELinuxContext {
				return
			}
			t.Errorf(
				"Volume %q has wrong SELinux context. Expected %q, got %q",
				expectedVolumeName,
				expectedSELinuxContext,
				volume.SELinuxMountContext)
		}
	}

	t.Fatalf(
		"Could not find volume %v in the list of GloballyMountedVolumes for actual state of world %+v",
		expectedVolumeName,
		globallyMountedVolumes)
}

// verifyVolumeDoesntExistInGloballyMountedVolumes 断言卷不在全局挂载列表中。
func verifyVolumeDoesntExistInGloballyMountedVolumes(
	t *testing.T, volumeToCheck v1.UniqueVolumeName, asw ActualStateOfWorld) {
	globallyMountedVolumes := asw.GetGloballyMountedVolumes()
	for _, volume := range globallyMountedVolumes {
		if volume.VolumeName == volumeToCheck {
			t.Fatalf(
				"Found volume %v in the list of GloballyMountedVolumes. Expected it not to exist.",
				volumeToCheck)
		}
	}
}

// verifyVolumeExistsAsw 断言 VolumeExists 的返回值与期望一致。
func verifyVolumeExistsAsw(
	t *testing.T,
	expectedVolumeName v1.UniqueVolumeName,
	shouldExist bool,
	asw ActualStateOfWorld) {
	volumeExists := asw.VolumeExists(expectedVolumeName)
	if shouldExist != volumeExists {
		t.Fatalf(
			"VolumeExists(%q) response incorrect. Expected: <%v> Actual: <%v>",
			expectedVolumeName,
			shouldExist,
			volumeExists)
	}
}

// verifyVolumeExistsAswWithSELinux 断言卷在已挂载列表中且 SELinux 上下文匹配。
func verifyVolumeExistsAswWithSELinux(
	t *testing.T,
	expectedVolumeName v1.UniqueVolumeName,
	expectedSELinuxContext string,
	asw ActualStateOfWorld) {
	volumes := asw.GetMountedVolumes()
	for _, vol := range volumes {
		if vol.VolumeName == expectedVolumeName {
			if vol.SELinuxMountContext == expectedSELinuxContext {
				return
			}
			t.Errorf(
				"Volume %q has wrong SELinux context, expected %q, got %q",
				expectedVolumeName,
				expectedSELinuxContext,
				vol.SELinuxMountContext)
		}
	}
	t.Errorf("Volume %q not found in ASW", expectedVolumeName)
}

// verifyVolumeExistsInUnmountedVolumes 断言卷出现在未挂载列表中。
func verifyVolumeExistsInUnmountedVolumes(
	t *testing.T, expectedVolumeName v1.UniqueVolumeName, asw ActualStateOfWorld) {
	unmountedVolumes := asw.GetUnmountedVolumes()
	for _, volume := range unmountedVolumes {
		if volume.VolumeName == expectedVolumeName {
			return
		}
	}

	t.Fatalf(
		"Could not find volume %v in the list of UnmountedVolumes for actual state of world %+v",
		expectedVolumeName,
		unmountedVolumes)
}

// verifyVolumeDoesntExistInUnmountedVolumes 断言卷不在未挂载列表中。
func verifyVolumeDoesntExistInUnmountedVolumes(
	t *testing.T, volumeToCheck v1.UniqueVolumeName, asw ActualStateOfWorld) {
	unmountedVolumes := asw.GetUnmountedVolumes()
	for _, volume := range unmountedVolumes {
		if volume.VolumeName == volumeToCheck {
			t.Fatalf(
				"Found volume %v in the list of UnmountedVolumes. Expected it not to exist.",
				volumeToCheck)
		}
	}
}

// verifyPodExistsInVolumeAsw 断言 pod/卷组合存在且 device path 正确（不校验 SELinux 上下文）。
func verifyPodExistsInVolumeAsw(
	t *testing.T,
	expectedPodName volumetypes.UniquePodName,
	expectedVolumeName v1.UniqueVolumeName,
	expectedDevicePath string,
	asw ActualStateOfWorld) {
	verifyPodExistsInVolumeAswWithSELinux(t, expectedPodName, expectedVolumeName, expectedDevicePath, "", asw)
}

// verifyPodExistsInVolumeAswWithSELinux 断言 pod/卷组合存在，并校验 device path 与 SELinux 标签。
func verifyPodExistsInVolumeAswWithSELinux(
	t *testing.T,
	expectedPodName volumetypes.UniquePodName,
	expectedVolumeName v1.UniqueVolumeName,
	expectedDevicePath string,
	expectedSELinuxLabel string,
	asw ActualStateOfWorld) {
	logger, _ := ktesting.NewTestContext(t)
	podExistsInVolume, devicePath, err :=
		asw.PodExistsInVolume(logger, expectedPodName, expectedVolumeName, resource.Quantity{}, expectedSELinuxLabel)
	if err != nil {
		t.Fatalf(
			"ASW PodExistsInVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	if !podExistsInVolume {
		t.Fatalf(
			"ASW PodExistsInVolume result invalid. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	if devicePath != expectedDevicePath {
		t.Fatalf(
			"Invalid devicePath. Expected: <%q> Actual: <%q> ",
			expectedDevicePath,
			devicePath)
	}
}

// verifyVolumeMountedElsewhere 断言卷是否被其它 pod 挂载的判定结果。
func verifyVolumeMountedElsewhere(
	t *testing.T,
	expectedPodName volumetypes.UniquePodName,
	expectedVolumeName v1.UniqueVolumeName,
	expectedMountedElsewhere bool,
	asw ActualStateOfWorld) {
	mountedElsewhere := asw.IsVolumeMountedElsewhere(expectedVolumeName, expectedPodName)
	if mountedElsewhere != expectedMountedElsewhere {
		t.Fatalf(
			"IsVolumeMountedElsewhere assertion failure. Expected : <%t> Actual: <%t>",
			expectedMountedElsewhere,
			mountedElsewhere)
	}
}

// verifyPodDoesntExistInVolumeAsw 断言 pod/卷组合不存在；expectVolumeToExist 控制是否应返回错误。
func verifyPodDoesntExistInVolumeAsw(
	t *testing.T,
	podToCheck volumetypes.UniquePodName,
	volumeToCheck v1.UniqueVolumeName,
	expectVolumeToExist bool,
	asw ActualStateOfWorld) {
	logger, _ := ktesting.NewTestContext(t)
	podExistsInVolume, devicePath, err :=
		asw.PodExistsInVolume(logger, podToCheck, volumeToCheck, resource.Quantity{}, "")
	if !expectVolumeToExist && err == nil {
		t.Fatalf(
			"ASW PodExistsInVolume did not return error. Expected: <error indicating volume does not exist> Actual: <%v>", err)
	}

	if expectVolumeToExist && err != nil {
		t.Fatalf(
			"ASW PodExistsInVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	if podExistsInVolume {
		t.Fatalf(
			"ASW PodExistsInVolume result invalid. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	if devicePath != "" {
		t.Fatalf(
			"Invalid devicePath. Expected: <\"\"> Actual: <%q> ",
			devicePath)
	}
}

// verifyPodExistsInVolumeSELinuxMismatch 断言 SELinux 标签不匹配时返回专门的 mismatch 错误，
// 供调用方区分"卷不存在"和"卷存在但标签不符"。
func verifyPodExistsInVolumeSELinuxMismatch(
	t *testing.T,
	podToCheck volumetypes.UniquePodName,
	volumeToCheck v1.UniqueVolumeName,
	unexpectedSELinuxLabel string,
	asw ActualStateOfWorld) {
	logger, _ := ktesting.NewTestContext(t)
	podExistsInVolume, _, err := asw.PodExistsInVolume(logger, podToCheck, volumeToCheck, resource.Quantity{}, unexpectedSELinuxLabel)
	if podExistsInVolume {
		t.Errorf("expected Pod %s not to exists, but it does", podToCheck)
	}
	if err == nil {
		t.Error("expected PodExistsInVolume to return error, but it returned nil")
	}

	if !IsSELinuxMountMismatchError(err) {
		t.Errorf("expected PodExistsInVolume to return SELinuxMountMismatchError, got %s", err)
	}
}

// verifyVolumeExistsWithSpecNameInVolumeAsw 断言 pod 中存在指定 spec 名的卷。
func verifyVolumeExistsWithSpecNameInVolumeAsw(
	t *testing.T,
	expectedPodName volumetypes.UniquePodName,
	expectedVolumeName string,
	asw ActualStateOfWorld) {
	podExistsInVolume :=
		asw.VolumeExistsWithSpecName(expectedPodName, expectedVolumeName)

	if !podExistsInVolume {
		t.Fatalf(
			"ASW VolumeExistsWithSpecName result invalid. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}
}

// verifyVolumeDoesntExistWithSpecNameInVolumeAsw 断言 pod 中不存在指定 spec 名的卷。
func verifyVolumeDoesntExistWithSpecNameInVolumeAsw(
	t *testing.T,
	podToCheck volumetypes.UniquePodName,
	volumeToCheck string,
	asw ActualStateOfWorld) {
	podExistsInVolume :=
		asw.VolumeExistsWithSpecName(podToCheck, volumeToCheck)

	if podExistsInVolume {
		t.Fatalf(
			"ASW VolumeExistsWithSpecName result invalid. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}
}

// verifyVolumeSpecNameInVolumeAsw 按位置逐项比较已挂载卷的 spec 名，因此调用方需保证顺序一致。
func verifyVolumeSpecNameInVolumeAsw(
	t *testing.T,
	podToCheck volumetypes.UniquePodName,
	volumeSpecs []*volume.Spec,
	asw ActualStateOfWorld) {
	mountedVolumes :=
		asw.GetMountedVolumesForPod(podToCheck)

	for i, volume := range mountedVolumes {
		if volume.InnerVolumeSpecName != volumeSpecs[i].Name() {
			t.Fatalf("Volume spec name does not match Expected: <%q> Actual: <%q>", volumeSpecs[i].Name(), volume.InnerVolumeSpecName)
		}
	}
}

// verifyVolumeFoundInReconstruction 断言卷仍记录在重建发现列表中。
func verifyVolumeFoundInReconstruction(t *testing.T, podToCheck volumetypes.UniquePodName, volumeToCheck v1.UniqueVolumeName, asw ActualStateOfWorld) {
	isRecontructed := asw.IsVolumeReconstructed(volumeToCheck, podToCheck)
	if !isRecontructed {
		t.Fatalf("ASW IsVolumeReconstructed result invalid. expected <true> Actual <false>")
	}
}

// verifyVolumeAttachability 通过 GetAttachedVolumes 检查卷的可附加性；
// 找不到卷时 attachable 保持 false，从而无法与"明确不可附加"区分。
func verifyVolumeAttachability(t *testing.T, volumeToCheck v1.UniqueVolumeName, asw ActualStateOfWorld, expected volumeAttachability) {
	attached := asw.GetAttachedVolumes()
	attachable := false

	for _, volume := range attached {
		if volume.VolumeName == volumeToCheck {
			attachable = volume.PluginIsAttachable
			break
		}
	}

	switch expected {
	case volumeAttachabilityTrue:
		if !attachable {
			t.Errorf("ASW reports %s as not-attachable, when %s was expected", volumeToCheck, expected)
		}
	// ASW 不区分 False 与 Uncertain：
	// Uncertain 只是允许后续被改为 True/False。
	case volumeAttachabilityUncertain, volumeAttachabilityFalse:
		if attachable {
			t.Errorf("ASW reports %s as attachable, when %s was expected", volumeToCheck, expected)
		}
	}
}
