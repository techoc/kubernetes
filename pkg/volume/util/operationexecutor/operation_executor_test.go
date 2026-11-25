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

package operationexecutor

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// 并发测试中各类操作使用的卷/设备数量，统一为 2 以观察是否同时启动。
const (
	numVolumesToMount                    = 2
	numAttachableVolumesToUnmount        = 2
	numNonAttachableVolumesToUnmount     = 2
	numDevicesToUnmount                  = 2
	numVolumesToAttach                   = 2
	numVolumesToDetach                   = 2
	numVolumesToVerifyAttached           = 2
	numVolumesToVerifyControllerAttached = 2
	numVolumesToMap                      = 2
	numAttachableVolumesToUnmap          = 2
	numNonAttachableVolumesToUnmap       = 2
	numDevicesToUnmap                    = 2
)

var _ OperationGenerator = &fakeOperationGenerator{}

// TestOperationExecutor_MountVolume_ConcurrentMountForNonAttachableAndNonDevicemountablePlugins 验证不可挂载、非设备挂载插件的挂载操作可以并发执行。
func TestOperationExecutor_MountVolume_ConcurrentMountForNonAttachableAndNonDevicemountablePlugins(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToMount)
	secretName := "secret-volume"
	volumeName := v1.UniqueVolumeName(secretName)

	// 执行
	for i := range volumesToMount {
		podName := "pod-" + strconv.Itoa(i+1)
		pod := getTestPodWithSecret(podName, secretName)
		volumesToMount[i] = VolumeToMount{
			Pod:                     pod,
			VolumeName:              volumeName,
			PluginIsAttachable:      false, // 该字段决定插件是否可挂载
			PluginIsDeviceMountable: false, // 该字段决定插件是否可挂载设备
			ReportedInUse:           true,
		}
		oe.MountVolume(0 /* waitForAttachTimeOut */, volumesToMount[i], nil /* actualStateOfWorldMounterUpdater */, false /* isRemount */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToMount) {
		t.Fatalf("Unable to start mount operations in Concurrent for non-attachable volumes")
	}
}

// TestOperationExecutor_MountVolume_ConcurrentMountForAttachablePlugins 验证可挂载插件的挂载操作必须串行执行。
func TestOperationExecutor_MountVolume_ConcurrentMountForAttachablePlugins(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToAttach)
	pdName := "pd-volume"
	volumeName := v1.UniqueVolumeName(pdName)
	// 执行
	for i := range volumesToMount {
		podName := "pod-" + strconv.Itoa(i+1)
		pod := getTestPodWithGCEPD(podName, pdName)
		volumesToMount[i] = VolumeToMount{
			Pod:                pod,
			VolumeName:         volumeName,
			PluginIsAttachable: true, // 该字段决定插件是否可挂载
			ReportedInUse:      true,
		}
		oe.MountVolume(0 /* waitForAttachTimeout */, volumesToMount[i], nil /* actualStateOfWorldMounterUpdater */, false /* isRemount */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Mount operations should not start concurrently for attachable volumes")
	}
}

// TestOperationExecutor_MountVolume_ConcurrentMountForDeviceMountablePlugins 验证设备挂载插件的挂载操作必须串行执行。
func TestOperationExecutor_MountVolume_ConcurrentMountForDeviceMountablePlugins(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToAttach)
	pdName := "pd-volume"
	volumeName := v1.UniqueVolumeName(pdName)
	// 执行
	for i := range volumesToMount {
		podName := "pod-" + strconv.Itoa(i+1)
		pod := getTestPodWithGCEPD(podName, pdName)
		volumesToMount[i] = VolumeToMount{
			Pod:                     pod,
			VolumeName:              volumeName,
			PluginIsDeviceMountable: true, // 该字段决定插件是否可挂载设备
			ReportedInUse:           true,
		}
		oe.MountVolume(0 /* waitForAttachTimeout */, volumesToMount[i], nil /* actualStateOfWorldMounterUpdater */, false /* isRemount */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Mount operations should not start concurrently for devicemountable volumes")
	}
}

// TestOperationExecutor_UnmountVolume_ConcurrentUnmountForAllPlugins 验证所有类型插件的卸载操作都可以并发执行。
func TestOperationExecutor_UnmountVolume_ConcurrentUnmountForAllPlugins(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	volumesToUnmount := make([]MountedVolume, numAttachableVolumesToUnmount+numNonAttachableVolumesToUnmount)
	pdName := "pd-volume"
	secretName := "secret-volume"

	// 执行
	for i := 0; i < numNonAttachableVolumesToUnmount+numAttachableVolumesToUnmount; i++ {
		podName := "pod-" + strconv.Itoa(i+1)
		if i < numNonAttachableVolumesToUnmount {
			pod := getTestPodWithSecret(podName, secretName)
			volumesToUnmount[i] = MountedVolume{
				PodName:    volumetypes.UniquePodName(podName),
				VolumeName: v1.UniqueVolumeName(secretName),
				PodUID:     pod.UID,
			}
		} else {
			pod := getTestPodWithGCEPD(podName, pdName)
			volumesToUnmount[i] = MountedVolume{
				PodName:    volumetypes.UniquePodName(podName),
				VolumeName: v1.UniqueVolumeName(pdName),
				PodUID:     pod.UID,
			}
		}
		oe.UnmountVolume(volumesToUnmount[i], nil /* actualStateOfWorldMounterUpdater */, "" /*podsDir*/)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numNonAttachableVolumesToUnmount+numAttachableVolumesToUnmount) {
		t.Fatalf("Unable to start unmount operations concurrently for volume plugins")
	}
}

// TestOperationExecutor_UnmountDeviceConcurrently 验证设备卸载操作必须串行执行。
func TestOperationExecutor_UnmountDeviceConcurrently(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	attachedVolumes := make([]AttachedVolume, numDevicesToUnmount)
	pdName := "pd-volume"

	// 执行
	for i := range attachedVolumes {
		attachedVolumes[i] = AttachedVolume{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node-name",
		}
		oe.UnmountDevice(attachedVolumes[i], nil /* actualStateOfWorldMounterUpdater */, nil /* mount.Interface */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Unmount device operations should not start concurrently")
	}
}

// TestOperationExecutor_AttachSingleNodeVolumeConcurrentlyToSameNode 验证单节点卷挂到同一节点时必须串行执行。
func TestOperationExecutor_AttachSingleNodeVolumeConcurrentlyToSameNode(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToAttach := make([]VolumeToAttach, numVolumesToAttach)
	pdName := "pd-volume"

	// 执行
	for i := range volumesToAttach {
		volumesToAttach[i] = VolumeToAttach{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node",
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.AttachVolume(logger, volumesToAttach[i], nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Attach volume operations should not start concurrently")
	}
}

// TestOperationExecutor_AttachMultiNodeVolumeConcurrentlyToSameNode 验证多节点卷挂到同一节点时必须串行执行。
func TestOperationExecutor_AttachMultiNodeVolumeConcurrentlyToSameNode(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToAttach := make([]VolumeToAttach, numVolumesToAttach)
	pdName := "pd-volume"

	// 执行
	for i := range volumesToAttach {
		volumesToAttach[i] = VolumeToAttach{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node",
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.AttachVolume(logger, volumesToAttach[i], nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Attach volume operations should not start concurrently")
	}
}

// TestOperationExecutor_AttachSingleNodeVolumeConcurrentlyToDifferentNodes 验证单节点卷挂到不同节点时必须串行执行。
func TestOperationExecutor_AttachSingleNodeVolumeConcurrentlyToDifferentNodes(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToAttach := make([]VolumeToAttach, numVolumesToAttach)
	pdName := "pd-volume"

	// 执行
	for i := range volumesToAttach {
		volumesToAttach[i] = VolumeToAttach{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   types.NodeName(fmt.Sprintf("node%d", i)),
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.AttachVolume(logger, volumesToAttach[i], nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Attach volume operations should not start concurrently")
	}
}

// TestOperationExecutor_AttachMultiNodeVolumeConcurrentlyToDifferentNodes 验证多节点卷挂到不同节点时可以并发执行。
func TestOperationExecutor_AttachMultiNodeVolumeConcurrentlyToDifferentNodes(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	volumesToAttach := make([]VolumeToAttach, numVolumesToAttach)
	pdName := "pd-volume"

	// 执行
	for i := range volumesToAttach {
		volumesToAttach[i] = VolumeToAttach{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   types.NodeName(fmt.Sprintf("node%d", i)),
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.AttachVolume(logger, volumesToAttach[i], nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToAttach) {
		t.Fatalf("Attach volume operations should not execute serially")
	}
}

// TestOperationExecutor_DetachSingleNodeVolumeConcurrentlyFromSameNode 验证从同一节点卸载单节点卷时必须串行执行。
func TestOperationExecutor_DetachSingleNodeVolumeConcurrentlyFromSameNode(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	attachedVolumes := make([]AttachedVolume, numVolumesToDetach)
	pdName := "pd-volume"

	// 执行
	for i := range attachedVolumes {
		attachedVolumes[i] = AttachedVolume{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node",
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.DetachVolume(logger, attachedVolumes[i], true /* verifySafeToDetach */, nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("DetachVolume operations should not run concurrently")
	}
}

// TestOperationExecutor_DetachMultiNodeVolumeConcurrentlyFromSameNode 验证从同一节点卸载多节点卷时必须串行执行。
func TestOperationExecutor_DetachMultiNodeVolumeConcurrentlyFromSameNode(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	attachedVolumes := make([]AttachedVolume, numVolumesToDetach)
	pdName := "pd-volume"

	// 执行
	for i := range attachedVolumes {
		attachedVolumes[i] = AttachedVolume{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node",
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.DetachVolume(logger, attachedVolumes[i], true /* verifySafeToDetach */, nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("DetachVolume operations should not run concurrently")
	}
}

// TestOperationExecutor_DetachMultiNodeVolumeConcurrentlyFromDifferentNodes 验证从不同节点卸载多节点卷时可以并发执行。
func TestOperationExecutor_DetachMultiNodeVolumeConcurrentlyFromDifferentNodes(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	attachedVolumes := make([]AttachedVolume, numVolumesToDetach)
	pdName := "pd-volume"

	// 执行
	for i := range attachedVolumes {
		attachedVolumes[i] = AttachedVolume{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   types.NodeName(fmt.Sprintf("node%d", i)),
			VolumeSpec: &volume.Spec{
				PersistentVolume: &v1.PersistentVolume{
					Spec: v1.PersistentVolumeSpec{
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
					},
				},
			},
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.DetachVolume(logger, attachedVolumes[i], true /* verifySafeToDetach */, nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToDetach) {
		t.Fatalf("Attach volume operations should not execute serially")
	}
}

// TestOperationExecutor_VerifyVolumesAreAttachedConcurrentlyOnSameNode 验证同一节点上卷挂载状态校验操作可以并发执行。
func TestOperationExecutor_VerifyVolumesAreAttachedConcurrentlyOnSameNode(t *testing.T) {
	// 准备
	ch, quit, oe := setup()

	// 执行
	for i := 0; i < numVolumesToVerifyAttached; i++ {
		oe.VerifyVolumesAreAttachedPerNode(nil /* attachedVolumes */, "node-name", nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToVerifyAttached) {
		t.Fatalf("VerifyVolumesAreAttached operation is not being run concurrently")
	}
}

// TestOperationExecutor_VerifyVolumesAreAttachedConcurrentlyOnDifferentNodes 验证不同节点上卷挂载状态校验操作可以并发执行。
func TestOperationExecutor_VerifyVolumesAreAttachedConcurrentlyOnDifferentNodes(t *testing.T) {
	// 准备
	ch, quit, oe := setup()

	// 执行
	for i := 0; i < numVolumesToVerifyAttached; i++ {
		oe.VerifyVolumesAreAttachedPerNode(
			nil, /* attachedVolumes */
			types.NodeName(fmt.Sprintf("node-name-%d", i)),
			nil /* actualStateOfWorldAttacherUpdater */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToVerifyAttached) {
		t.Fatalf("VerifyVolumesAreAttached operation is not being run concurrently")
	}
}

// TestOperationExecutor_VerifyControllerAttachedVolumeConcurrently 验证控制器挂载状态校验操作必须串行执行。
func TestOperationExecutor_VerifyControllerAttachedVolumeConcurrently(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToVerifyControllerAttached)
	pdName := "pd-volume"

	// 执行
	for i := range volumesToMount {
		volumesToMount[i] = VolumeToMount{
			VolumeName: v1.UniqueVolumeName(pdName),
		}
		logger, _ := ktesting.NewTestContext(t)
		oe.VerifyControllerAttachedVolume(logger, volumesToMount[i], types.NodeName("node-name"), nil /* actualStateOfWorldMounterUpdater */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("VerifyControllerAttachedVolume should not run concurrently")
	}
}

// TestOperationExecutor_MountVolume_ConcurrentMountForNonAttachablePlugins_VolumeMode_Block 验证块模式下不可挂载插件的映射操作可以并发执行。
func TestOperationExecutor_MountVolume_ConcurrentMountForNonAttachablePlugins_VolumeMode_Block(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToMap)
	secretName := "secret-volume"
	volumeName := v1.UniqueVolumeName(secretName)
	volumeMode := v1.PersistentVolumeBlock
	tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}

	// 执行
	for i := range volumesToMount {
		podName := "pod-" + strconv.Itoa(i+1)
		pod := getTestPodWithSecret(podName, secretName)
		volumesToMount[i] = VolumeToMount{
			Pod:                pod,
			VolumeName:         volumeName,
			PluginIsAttachable: false, // 该字段决定插件是否可挂载
			ReportedInUse:      true,
			VolumeSpec:         tmpSpec,
		}
		oe.MountVolume(0 /* waitForAttachTimeOut */, volumesToMount[i], nil /* actualStateOfWorldMounterUpdater */, false)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numVolumesToMap) {
		t.Fatalf("Unable to start map operations in Concurrent for non-attachable volumes")
	}
}

// TestOperationExecutor_MountVolume_ConcurrentMountForAttachablePlugins_VolumeMode_Block 验证块模式下可挂载插件的映射操作必须串行执行。
func TestOperationExecutor_MountVolume_ConcurrentMountForAttachablePlugins_VolumeMode_Block(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	volumesToMount := make([]VolumeToMount, numVolumesToAttach)
	pdName := "pd-volume"
	volumeName := v1.UniqueVolumeName(pdName)
	volumeMode := v1.PersistentVolumeBlock
	tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}

	// 执行
	for i := range volumesToMount {
		podName := "pod-" + strconv.Itoa(i+1)
		pod := getTestPodWithGCEPD(podName, pdName)
		volumesToMount[i] = VolumeToMount{
			Pod:                pod,
			VolumeName:         volumeName,
			PluginIsAttachable: true, // 该字段决定插件是否可挂载
			ReportedInUse:      true,
			VolumeSpec:         tmpSpec,
		}
		oe.MountVolume(0 /* waitForAttachTimeout */, volumesToMount[i], nil /* actualStateOfWorldMounterUpdater */, false)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Map operations should not start concurrently for attachable volumes")
	}
}

// TestOperationExecutor_UnmountVolume_ConcurrentUnmountForAllPlugins_VolumeMode_Block 验证块模式下所有插件的取消映射操作可以并发执行。
func TestOperationExecutor_UnmountVolume_ConcurrentUnmountForAllPlugins_VolumeMode_Block(t *testing.T) {
	// 准备
	ch, quit, oe := setup()
	volumesToUnmount := make([]MountedVolume, numAttachableVolumesToUnmap+numNonAttachableVolumesToUnmap)
	pdName := "pd-volume"
	secretName := "secret-volume"
	volumeMode := v1.PersistentVolumeBlock
	tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}

	// 执行
	for i := 0; i < numNonAttachableVolumesToUnmap+numAttachableVolumesToUnmap; i++ {
		podName := "pod-" + strconv.Itoa(i+1)
		if i < numNonAttachableVolumesToUnmap {
			pod := getTestPodWithSecret(podName, secretName)
			volumesToUnmount[i] = MountedVolume{
				PodName:    volumetypes.UniquePodName(podName),
				VolumeName: v1.UniqueVolumeName(secretName),
				PodUID:     pod.UID,
				VolumeSpec: tmpSpec,
			}
		} else {
			pod := getTestPodWithGCEPD(podName, pdName)
			volumesToUnmount[i] = MountedVolume{
				PodName:    volumetypes.UniquePodName(podName),
				VolumeName: v1.UniqueVolumeName(pdName),
				PodUID:     pod.UID,
				VolumeSpec: tmpSpec,
			}
		}
		oe.UnmountVolume(volumesToUnmount[i], nil /* actualStateOfWorldMounterUpdater */, "" /* podsDir */)
	}

	// 断言
	if !isOperationRunConcurrently(ch, quit, numNonAttachableVolumesToUnmap+numAttachableVolumesToUnmap) {
		t.Fatalf("Unable to start unmap operations concurrently for volume plugins")
	}
}

// TestOperationExecutor_UnmountDeviceConcurrently_VolumeMode_Block 验证块模式下设备取消映射操作必须串行执行。
func TestOperationExecutor_UnmountDeviceConcurrently_VolumeMode_Block(t *testing.T) {
	t.Parallel()

	// 准备
	ch, quit, oe := setup()
	attachedVolumes := make([]AttachedVolume, numDevicesToUnmap)
	pdName := "pd-volume"
	volumeMode := v1.PersistentVolumeBlock
	tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}

	// 执行
	for i := range attachedVolumes {
		attachedVolumes[i] = AttachedVolume{
			VolumeName: v1.UniqueVolumeName(pdName),
			NodeName:   "node-name",
			VolumeSpec: tmpSpec,
		}
		oe.UnmountDevice(attachedVolumes[i], nil /* actualStateOfWorldMounterUpdater */, nil /* mount.Interface */)
	}

	// 断言
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Unmap device operations should not start concurrently")
	}
}

// fakeOperationGenerator 是 OperationGenerator 的假实现，
// 生成的操作只向 ch 发信号，再阻塞直到测试关闭 quit。
type fakeOperationGenerator struct {
	ch   chan interface{}
	quit chan interface{}
}

// newFakeOperationGenerator 创建一个用 ch/quit 观察操作并发行为的假生成器。
func newFakeOperationGenerator(ch chan interface{}, quit chan interface{}) OperationGenerator {
	return &fakeOperationGenerator{
		ch:   ch,
		quit: quit,
	}
}

// GenerateMountVolumeFunc 生成一个阻塞型挂载操作，用于观察挂载是否并发。
func (fopg *fakeOperationGenerator) GenerateMountVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater, isRemount bool) volumetypes.GeneratedOperations {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}
}

// GenerateUnmountVolumeFunc 生成一个阻塞型卸载操作，用于观察卸载是否并发。
func (fopg *fakeOperationGenerator) GenerateUnmountVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, podsDir string) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateAttachVolumeFunc 生成一个阻塞型挂载（attach）操作。
func (fopg *fakeOperationGenerator) GenerateAttachVolumeFunc(logger klog.Logger, volumeToAttach VolumeToAttach, actualStateOfWorld ActualStateOfWorldAttacherUpdater) volumetypes.GeneratedOperations {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}
}

// GenerateDetachVolumeFunc 生成一个阻塞型卸载（detach）操作。
func (fopg *fakeOperationGenerator) GenerateDetachVolumeFunc(logger klog.Logger, volumeToDetach AttachedVolume, verifySafeToDetach bool, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateVolumesAreAttachedFunc 生成一个阻塞型的卷挂载状态校验操作。
func (fopg *fakeOperationGenerator) GenerateVolumesAreAttachedFunc(attachedVolumes []AttachedVolume, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateUnmountDeviceFunc 生成一个阻塞型设备卸载操作。
func (fopg *fakeOperationGenerator) GenerateUnmountDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, hostutil hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateVerifyControllerAttachedVolumeFunc 生成一个阻塞型的控制器挂载校验操作。
func (fopg *fakeOperationGenerator) GenerateVerifyControllerAttachedVolumeFunc(logger klog.Logger, volumeToMount VolumeToMount, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateExpandVolumeFunc 生成一个阻塞型扩容操作。
func (fopg *fakeOperationGenerator) GenerateExpandVolumeFunc(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateExpandAndRecoverVolumeFunc 生成一个阻塞型扩容及恢复操作。
func (fopg *fakeOperationGenerator) GenerateExpandAndRecoverVolumeFunc(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, resizerName string) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateExpandInUseVolumeFunc 生成一个阻塞型在线扩容操作。
func (fopg *fakeOperationGenerator) GenerateExpandInUseVolumeFunc(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateMapVolumeFunc 生成一个阻塞型块设备映射操作。
func (fopg *fakeOperationGenerator) GenerateMapVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateUnmapVolumeFunc 生成一个阻塞型块设备取消映射操作。
func (fopg *fakeOperationGenerator) GenerateUnmapVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GenerateUnmapDeviceFunc 生成一个阻塞型块设备取消映射操作。
func (fopg *fakeOperationGenerator) GenerateUnmapDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, hostutil hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {
	opFunc := func() volumetypes.OperationContext {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return volumetypes.NewOperationContext(nil, nil, false)
	}
	return volumetypes.GeneratedOperations{
		OperationFunc: opFunc,
	}, nil
}

// GetVolumePluginMgr 返回 nil；这些测试不依赖插件管理器。
func (fopg *fakeOperationGenerator) GetVolumePluginMgr() *volume.VolumePluginMgr {
	return nil
}

// getTestPodWithSecret 构造一个挂载 secret 卷的测试 Pod。
func getTestPodWithSecret(podName, secretName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  types.UID(podName),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: secretName,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: secretName,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  "secret-volume-test",
					Image: "registry.k8s.io/mounttest:0.8",
					Args: []string{
						"--file_content=/etc/secret-volume/data-1",
						"--file_mode=/etc/secret-volume/data-1"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      secretName,
							MountPath: "/data",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
}

// getTestPodWithGCEPD 构造一个挂载 GCE PD 卷的测试 Pod。
func getTestPodWithGCEPD(podName, pdName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  types.UID(podName + string(uuid.NewUUID())),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: pdName,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName:   pdName,
							FSType:   "ext4",
							ReadOnly: false,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  "pd-volume-test",
					Image: "registry.k8s.io/mounttest:0.8",
					Args: []string{
						"--file_content=/etc/pd-volume/data-1",
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      pdName,
							MountPath: "/data",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
}

// isOperationRunSerially 在 5 秒窗口内若观察到多个操作同时启动则返回 false。
func isOperationRunSerially(ch <-chan interface{}, quit chan<- interface{}) bool {
	defer close(quit)
	numOperationsStarted := 0
loop:
	for {
		select {
		case <-ch:
			numOperationsStarted++
			if numOperationsStarted > 1 {
				return false
			}
		case <-time.After(5 * time.Second):
			break loop
		}
	}
	return true
}

// isOperationRunConcurrently 在 5 秒窗口内等待指定数量的操作全部启动。
func isOperationRunConcurrently(ch <-chan interface{}, quit chan<- interface{}, numOperationsToRun int) bool {
	defer close(quit)
	numOperationsStarted := 0
loop:
	for {
		select {
		case <-ch:
			numOperationsStarted++
			if numOperationsStarted == numOperationsToRun {
				return true
			}
		case <-time.After(5 * time.Second):
			break loop
		}
	}
	return false
}

// setup 返回供假生成器发信号的通道，以及使用该生成器的 OperationExecutor。
func setup() (chan interface{}, chan interface{}, OperationExecutor) {
	ch, quit := make(chan interface{}), make(chan interface{})
	return ch, quit, NewOperationExecutor(newFakeOperationGenerator(ch, quit))
}

// startOperationAndBlock 先向 ch 写入以通知测试操作已启动，
// 再阻塞在 quit 上，直到当前测试关闭该通道。
func startOperationAndBlock(ch chan<- interface{}, quit <-chan interface{}) {
	ch <- nil
	<-quit
}
