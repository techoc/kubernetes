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

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"testing"
	"time"

	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/kubernetes/pkg/volume/csimigration"

	"github.com/stretchr/testify/assert"
	"k8s.io/mount-utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/nestedpendingoperations"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// reconcilerLoopSleepDuration is the amount of time the reconciler loop
	// waits between successive executions
	reconcilerLoopSleepDuration = 1 * time.Nanosecond
	// waitForAttachTimeout is the maximum amount of time a
	// operationexecutor.Mount call will wait for a volume to be attached.
	waitForAttachTimeout         = 1 * time.Second
	nodeName                     = k8stypes.NodeName("mynodename")
	kubeletPodsDir               = "fake-dir"
	testOperationBackOffDuration = 100 * time.Millisecond
	reconcilerSyncWaitDuration   = 10 * time.Second
)

// hasAddedPods 恒定返回 true，模拟"节点上已有 Pod"的场景，
// 让 reconciler 认为可以安全执行卷操作（无 Pod 时它可能会跳过某些清理）。
func hasAddedPods() bool { return true }

// 测试场景：DSW/ASW 均为空时运行 reconciler。
// 验证 attach、detach、mount、unmount 等操作一次都不会被调用。
func Test_Run_Positive_DoNothing(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler,
	))
	reconciler := NewReconciler(
		kubeClient,
		false, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)

	// Act
	runReconciler(ctx, reconciler)

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroWaitForAttachCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroMountDeviceCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroSetUpCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 向 desiredStateOfWorld 中放入一个 volume/pod 组合，然后启动 Run()。
// 验证会触发 attach/mount 等操作，且不会触发 detach/unmount。
func Test_Run_Positive_VolumeAttachAndMount(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		false, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Assert
	assert.NoError(t, volumetesting.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 与 VolumeAttachAndMount 相同，但开启 in-tree 到 CSI 的迁移。
// 验证迁移后的卷仍能完成 attach/mount，且没有 detach/unmount。
func Test_Run_Positive_VolumeAttachAndMountMigrationEnabled(t *testing.T) {
	// Arrange
	logger, ctx := ktesting.NewTestContext(t)
	intreeToCSITranslator := csitrans.New()
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", "pd.csi.storage.gke.io-fake-device1")),
					DevicePath: "fake/path",
				},
			},
		},
	}
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)

	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient(v1.AttachedVolume{
		Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", "pd.csi.storage.gke.io-fake-device1")),
		DevicePath: "fake/path",
	})

	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	migratedSpec, err := csimigration.TranslateInTreeSpecToCSI(logger, volumeSpec, pod.Namespace, intreeToCSITranslator)
	if err != nil {
		t.Fatalf("unexpected error while translating spec %v: %v", volumeSpec, err)
	}

	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger,
		podName,
		pod,
		migratedSpec,
		migratedSpec.Name(),
		"",  /* volumeGIDValue */
		nil, /* SELinuxContexts */
	)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}
	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Assert
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 向 DSW 放入一个 volume/pod，并开启 controllerAttachDetachEnabled。
// 验证只有 mount 调用、没有 unmount 调用，同时 attach/detach 由控制器负责，reconciler 不应发起。
func Test_Run_Positive_VolumeMountControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "fake/path",
				},
			},
		},
	}
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 向 DSW 放入一个 volume/pod，开启 controllerAttachDetachEnabled，
// 但卷未被报告为 in-use。验证不会进入 wait-for-mount，也不会触发指数退避重试。
func Test_Run_Negative_VolumeMountControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	time.Sleep(reconcilerSyncWaitDuration)

	ok := oex.IsOperationSafeToRetry(generatedVolumeName, podName, nodeName, operationexecutor.VerifyControllerAttachedVolumeOpName)
	if !ok {
		t.Errorf("operation on volume %s is not safe to retry", generatedVolumeName)
	}

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		0 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		0 /* expectedMountDeviceCallCount */, fakePlugin))
}

// 向 DSW 放入 volume/pod 并运行 Run()，验证 attach/mount 各调用一次且无 detach。
// 随后从 DSW 删除该 volume/pod，验证会发出 unmount/detach。
func Test_Run_Positive_VolumeAttachMountUnmountDetach(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		false, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Assert
	assert.NoError(t, volumetesting.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))

	// Act
	dsw.DeletePodFromVolume(podName, generatedVolumeName)
	waitForDetach(t, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyTearDownCallCount(
		1 /* expectedTearDownCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyDetachCallCount(
		1 /* expectedDetachCallCount */, fakePlugin))
}

// 开启 controllerAttachDetachEnabled，向 DSW 放入 volume/pod 并运行 Run()，
// 验证只调用一次 mount 而无 unmount；从 DSW 删除后验证触发一次 unmount，
// 且全程没有 attach/detach（由控制器负责）。
func Test_Run_Positive_VolumeUnmountControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "fake/path",
				},
			},
		},
	}
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)

	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})
	waitForMount(t, fakePlugin, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))

	// Act
	dsw.DeletePodFromVolume(podName, generatedVolumeName)
	waitForDetach(t, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyTearDownCallCount(
		1 /* expectedTearDownCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 向 DSW 放入一个块设备卷并运行 Run()。
// 验证会调用 attach、get map path、setupDevice，且不调用 detach/teardownDevice。
func Test_Run_Positive_VolumeAttachAndMap(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			UID:       "pod1uid",
			Namespace: "ns",
		},
		Spec: v1.PodSpec{},
	}

	mode := v1.PersistentVolumeBlock
	gcepv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "001", Name: "volume-name"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			VolumeMode: &mode,
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "pvc-volume-name"},
		},
	}

	gcepvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{UID: "pvc-001", Name: "pvc-volume-name", Namespace: "ns"},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: gcepv.Spec.Capacity,
		},
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createtestClientWithPVPVC(gcepv, gcepvc)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		false, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)

	volumeSpec := &volume.Spec{
		PersistentVolume: gcepv,
	}
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Assert
	assert.NoError(t, volumetesting.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyGetMapPodDeviceCallCount(
		1 /* expectedGetMapDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 开启 controllerAttachDetachEnabled，向 DSW 放入块设备卷并运行 Run()。
// 验证调用两次 get map path 和一次 setupDevice，且不调用 teardownDevice；
// attach/detach 由控制器负责，不应出现。
func Test_Run_Positive_BlockVolumeMapControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			UID:       "pod1uid",
			Namespace: "ns",
		},
		Spec: v1.PodSpec{},
	}

	mode := v1.PersistentVolumeBlock
	gcepv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "001", Name: "volume-name"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			VolumeMode: &mode,
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "pvc-volume-name"},
		},
	}
	gcepvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{UID: "pvc-001", Name: "pvc-volume-name", Namespace: "ns"},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: gcepv.Spec.Capacity,
		},
	}

	volumeSpec := &volume.Spec{
		PersistentVolume: gcepv,
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "fake/path",
				},
			},
		},
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createtestClientWithPVPVC(gcepv, gcepvc, v1.AttachedVolume{
		Name:       "fake-plugin/fake-device1",
		DevicePath: "/fake/path",
	})
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)

	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyGetMapPodDeviceCallCount(
		1 /* expectedGetMapDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 向 DSW 放入块设备卷并运行 Run()，验证 attach、两次 get map path、
// setupDevice 被调用且无 detach；从 DSW 删除后验证触发 detach 与 teardownDevice。
func Test_Run_Positive_BlockVolumeAttachMapUnmapDetach(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			UID:       "pod1uid",
			Namespace: "ns",
		},
		Spec: v1.PodSpec{},
	}

	mode := v1.PersistentVolumeBlock
	gcepv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "001", Name: "volume-name"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			VolumeMode: &mode,
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "pvc-volume-name"},
		},
	}
	gcepvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{UID: "pvc-001", Name: "pvc-volume-name", Namespace: "ns"},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: gcepv.Spec.Capacity,
		},
	}

	volumeSpec := &volume.Spec{
		PersistentVolume: gcepv,
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createtestClientWithPVPVC(gcepv, gcepvc)
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		false, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)

	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Assert
	assert.NoError(t, volumetesting.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyGetMapPodDeviceCallCount(
		1 /* expectedGetMapDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))

	// Act
	dsw.DeletePodFromVolume(podName, generatedVolumeName)
	waitForDetach(t, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyTearDownDeviceCallCount(
		1 /* expectedTearDownDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyDetachCallCount(
		1 /* expectedDetachCallCount */, fakePlugin))
}

// 开启 controllerAttachDetachEnabled，向 DSW 放入块设备卷并运行 Run()。
// 验证调用两次 map path 且无 teardownDevice/detach；从 DSW 删除后验证触发一次 teardownDevice，
// 且全程没有 attach/detach。
func Test_Run_Positive_VolumeUnmapControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			UID:       "pod1uid",
			Namespace: "ns",
		},
		Spec: v1.PodSpec{},
	}

	mode := v1.PersistentVolumeBlock
	gcepv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "001", Name: "volume-name"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			VolumeMode: &mode,
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "pvc-volume-name"},
		},
	}
	gcepvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{UID: "pvc-001", Name: "pvc-volume-name", Namespace: "ns"},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: gcepv.Spec.Capacity,
		},
	}

	volumeSpec := &volume.Spec{
		PersistentVolume: gcepv,
	}

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "/fake/path",
				},
			},
		},
	}

	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createtestClientWithPVPVC(gcepv, gcepvc, v1.AttachedVolume{
		Name:       "fake-plugin/fake-device1",
		DevicePath: "/fake/path",
	})
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)

	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act
	runReconciler(ctx, reconciler)

	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})
	waitForMount(t, fakePlugin, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyZeroAttachCalls(fakePlugin))
	assert.NoError(t, volumetesting.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyGetMapPodDeviceCallCount(
		1 /* expectedGetMapDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))

	// Act
	dsw.DeletePodFromVolume(podName, generatedVolumeName)
	waitForDetach(t, generatedVolumeName, asw)

	// Assert
	assert.NoError(t, volumetesting.VerifyTearDownDeviceCallCount(
		1 /* expectedTearDownDeviceCallCount */, fakePlugin))
	assert.NoError(t, volumetesting.VerifyZeroDetachCallCount(fakePlugin))
}

// 验证 MapVolume 在找不到 BlockVolumeMapper 插件时返回可识别的错误，
// 覆盖插件列表为空与插件类型不匹配两种情形。
func Test_GenerateMapVolumeFunc_Plugin_Not_Found(t *testing.T) {
	testCases := map[string]struct {
		volumePlugins  []volume.VolumePlugin
		expectErr      bool
		expectedErrMsg string
	}{
		"volumePlugin is nil": {
			volumePlugins:  []volume.VolumePlugin{},
			expectErr:      true,
			expectedErrMsg: "MapVolume.FindMapperPluginBySpec failed",
		},
		"blockVolumePlugin is nil": {
			volumePlugins:  volumetesting.NewFakeFileVolumePlugin(),
			expectErr:      true,
			expectedErrMsg: "MapVolume.FindMapperPluginBySpec failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			volumePluginMgr := &volume.VolumePluginMgr{}
			volumePluginMgr.InitPlugins(tc.volumePlugins, nil, nil)
			asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
			oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
				nil, /* kubeClient */
				volumePluginMgr,
				nil, /* fakeRecorder */
				nil))

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod1",
					UID:  "pod1uid",
				},
				Spec: v1.PodSpec{},
			}
			volumeMode := v1.PersistentVolumeBlock
			tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}
			volumeToMount := operationexecutor.VolumeToMount{
				Pod:        pod,
				VolumeSpec: tmpSpec}
			err := oex.MountVolume(waitForAttachTimeout, volumeToMount, asw, false)
			// Assert
			if assert.Error(t, err) {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

// 验证 UnmapVolume 在找不到 BlockVolumeMapper 插件时返回可识别的错误，
// 覆盖插件列表为空与插件类型不匹配两种情形。
func Test_GenerateUnmapVolumeFunc_Plugin_Not_Found(t *testing.T) {
	testCases := map[string]struct {
		volumePlugins  []volume.VolumePlugin
		expectErr      bool
		expectedErrMsg string
	}{
		"volumePlugin is nil": {
			volumePlugins:  []volume.VolumePlugin{},
			expectErr:      true,
			expectedErrMsg: "UnmapVolume.FindMapperPluginByName failed",
		},
		"blockVolumePlugin is nil": {
			volumePlugins:  volumetesting.NewFakeFileVolumePlugin(),
			expectErr:      true,
			expectedErrMsg: "UnmapVolume.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			volumePluginMgr := &volume.VolumePluginMgr{}
			volumePluginMgr.InitPlugins(tc.volumePlugins, nil, nil)
			asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
			oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
				nil, /* kubeClient */
				volumePluginMgr,
				nil, /* fakeRecorder */
				nil))
			volumeMode := v1.PersistentVolumeBlock
			tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}
			volumeToUnmount := operationexecutor.MountedVolume{
				PluginName: "fake-file-plugin",
				VolumeSpec: tmpSpec}
			err := oex.UnmountVolume(volumeToUnmount, asw, "" /* podsDir */)
			// Assert
			if assert.Error(t, err) {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

// 验证 UnmapDevice 在找不到 BlockVolumeMapper 插件时返回可识别的错误，
// 覆盖插件列表为空与插件类型不匹配两种情形。
func Test_GenerateUnmapDeviceFunc_Plugin_Not_Found(t *testing.T) {
	testCases := map[string]struct {
		volumePlugins  []volume.VolumePlugin
		expectErr      bool
		expectedErrMsg string
	}{
		"volumePlugin is nil": {
			volumePlugins:  []volume.VolumePlugin{},
			expectErr:      true,
			expectedErrMsg: "UnmapDevice.FindMapperPluginByName failed",
		},
		"blockVolumePlugin is nil": {
			volumePlugins:  volumetesting.NewFakeFileVolumePlugin(),
			expectErr:      true,
			expectedErrMsg: "UnmapDevice.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			volumePluginMgr := &volume.VolumePluginMgr{}
			volumePluginMgr.InitPlugins(tc.volumePlugins, nil, nil)
			asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
			oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
				nil, /* kubeClient */
				volumePluginMgr,
				nil, /* fakeRecorder */
				nil))
			var hostutil hostutil.HostUtils
			volumeMode := v1.PersistentVolumeBlock
			tmpSpec := &volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{VolumeMode: &volumeMode}}}
			deviceToDetach := operationexecutor.AttachedVolume{VolumeSpec: tmpSpec, PluginName: "fake-file-plugin"}
			err := oex.UnmountDevice(deviceToDetach, asw, hostutil)
			// Assert
			if assert.Error(t, err) {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

// 开启 controllerAttachDetachEnabled，向 DSW 放入 volume/pod 并运行 Run()，
// 等待卷完成挂载后，在 ASW 中把卷标记为 fsResizeRequired，
// 验证该标记随后被 reconciler 清除（即文件系统扩容已完成）。
func Test_Run_Positive_VolumeFSResizeControllerAttachEnabled(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	blockMode := v1.PersistentVolumeBlock
	fsMode := v1.PersistentVolumeFilesystem

	var tests = []struct {
		name            string
		volumeMode      *v1.PersistentVolumeMode
		expansionFailed bool
		uncertainTest   bool
		pvName          string
		pvcSize         resource.Quantity
		pvcStatusSize   resource.Quantity
		oldPVSize       resource.Quantity
		newPVSize       resource.Quantity
	}{
		{
			name:          "expand-fs-volume",
			volumeMode:    &fsMode,
			pvName:        "pv",
			pvcSize:       resource.MustParse("10G"),
			pvcStatusSize: resource.MustParse("10G"),
			newPVSize:     resource.MustParse("15G"),
			oldPVSize:     resource.MustParse("10G"),
		},
		{
			name:          "expand-raw-block",
			volumeMode:    &blockMode,
			pvName:        "pv",
			pvcSize:       resource.MustParse("10G"),
			pvcStatusSize: resource.MustParse("10G"),
			newPVSize:     resource.MustParse("15G"),
			oldPVSize:     resource.MustParse("10G"),
		},
		{
			name:            "expand-fs-volume with in-use error",
			volumeMode:      &fsMode,
			expansionFailed: true,
			pvName:          volumetesting.FailWithInUseVolumeName,
			pvcSize:         resource.MustParse("10G"),
			pvcStatusSize:   resource.MustParse("10G"),
			newPVSize:       resource.MustParse("15G"),
			oldPVSize:       resource.MustParse("13G"),
		},
		{
			name:            "expand-fs-volume with unsupported error",
			volumeMode:      &fsMode,
			expansionFailed: false,
			pvName:          volumetesting.FailWithUnSupportedVolumeName,
			pvcSize:         resource.MustParse("10G"),
			pvcStatusSize:   resource.MustParse("10G"),
			newPVSize:       resource.MustParse("15G"),
			oldPVSize:       resource.MustParse("13G"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pv := getTestPV(tc.pvName, tc.volumeMode, tc.oldPVSize)
			pvc := getTestPVC("pv", tc.volumeMode, tc.pvcSize, tc.pvcStatusSize)
			pod := getTestPod(pvc.Name)

			// deep copy before reconciler runs to avoid data race.
			pvWithSize := pv.DeepCopy()
			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(nodeName),
				},
				Spec: v1.NodeSpec{},
				Status: v1.NodeStatus{
					VolumesAttached: []v1.AttachedVolume{
						{
							Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.pvName)),
							DevicePath: "fake/path",
						},
					},
				},
			}
			volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
			seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
			dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
			asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
			kubeClient := createtestClientWithPVPVC(pv, pvc, v1.AttachedVolume{
				Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.pvName)),
				DevicePath: "fake/path",
			})
			fakeRecorder := &record.FakeRecorder{}
			fakeHandler := volumetesting.NewBlockVolumePathHandler()
			oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
				kubeClient,
				volumePluginMgr,
				fakeRecorder,
				fakeHandler))

			reconciler := NewReconciler(
				kubeClient,
				true, /* controllerAttachDetachEnabled */
				reconcilerLoopSleepDuration,
				waitForAttachTimeout,
				nodeName,
				dsw,
				asw,
				hasAddedPods,
				oex,
				mount.NewFakeMounter(nil),
				hostutil.NewFakeHostUtil(nil),
				volumePluginMgr,
				kubeletPodsDir)

			volumeSpec := &volume.Spec{PersistentVolume: pv}
			podName := util.GetUniquePodName(pod)
			volumeName, err := dsw.AddPodToVolume(
				logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
			// Assert
			if err != nil {
				t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
			}
			dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{volumeName})

			// Start the reconciler to fill ASW.
			stopChan, stoppedChan := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stoppedChan)
				reconciler.Run(ctx, stopChan)
			}()
			waitForMount(t, fakePlugin, volumeName, asw)
			// Stop the reconciler.
			close(stopChan)
			<-stoppedChan

			// Simulate what DSOWP does
			pvWithSize.Spec.Capacity[v1.ResourceStorage] = tc.newPVSize
			volumeSpec = &volume.Spec{PersistentVolume: pvWithSize}
			_, err = dsw.AddPodToVolume(logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxContexts */)
			if err != nil {
				t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
			}

			t.Logf("Changing size of the volume to %s", tc.newPVSize.String())
			newSize := tc.newPVSize.DeepCopy()
			dsw.UpdatePersistentVolumeSize(volumeName, newSize)

			_, _, podExistErr := asw.PodExistsInVolume(logger, podName, volumeName, newSize, "" /* SELinuxLabel */)
			if tc.expansionFailed {
				if cache.IsFSResizeRequiredError(podExistErr) {
					t.Fatalf("volume %s should not throw fsResizeRequired error: %v", volumeName, podExistErr)
				}
			} else {
				if !cache.IsFSResizeRequiredError(podExistErr) {
					t.Fatalf("Volume should be marked as fsResizeRequired, but receive unexpected error: %v", podExistErr)
				}
				go reconciler.Run(ctx, wait.NeverStop)

				waitErr := retryWithExponentialBackOff(testOperationBackOffDuration, func() (done bool, err error) {
					mounted, _, err := asw.PodExistsInVolume(logger, podName, volumeName, newSize, "" /* SELinuxContext */)
					return mounted && err == nil, nil
				})
				if waitErr != nil {
					t.Fatalf("Volume resize should succeeded %v", waitErr)
				}
			}

		})
	}
}

// 构造测试用 PVC，指定 PV 名称、卷模式和请求/状态容量。
func getTestPVC(pvName string, volumeMode *v1.PersistentVolumeMode, specSize, statusSize resource.Quantity) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc",
			UID:  "pvcuid",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: specSize,
				},
			},
			VolumeName: pvName,
			VolumeMode: volumeMode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: statusSize,
			},
		},
	}
	return pvc
}

// 构造测试用 PV，指定名称、卷模式与容量，并绑定到名为 pvc 的 Claim。
func getTestPV(pvName string, volumeMode *v1.PersistentVolumeMode, pvSize resource.Quantity) *v1.PersistentVolume {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			UID:  "pvuid",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Name: "pvc"},
			VolumeMode: volumeMode,
			Capacity: v1.ResourceList{
				v1.ResourceStorage: pvSize,
			},
		},
	}
	return pv
}

// 构造一个引用指定 PVC 的测试 Pod，用于驱动 DSW 中的卷需求。
func getTestPod(claimName string) *v1.Pod {
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
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
			},
		},
	}
	return pod
}

// 表驱动测试设备全局挂载的"不确定"状态转换：超时操作应标记为 uncertain，
// 失败操作应回退为未挂载，成功后再超时/失败则保持已挂载。
// 对块设备和文件系统两种模式分别验证卸载/teardown 调用次数。
func Test_UncertainDeviceGlobalMounts(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	var tests = []struct {
		name                   string
		deviceState            operationexecutor.DeviceMountState
		unmountDeviceCallCount int
		volumeName             string
		supportRemount         bool
	}{
		{
			name:                   "timed out operations should result in device marked as uncertain",
			deviceState:            operationexecutor.DeviceMountUncertain,
			unmountDeviceCallCount: 1,
			volumeName:             volumetesting.TimeoutOnMountDeviceVolumeName,
		},
		{
			name:                   "failed operation should result in not-mounted device",
			deviceState:            operationexecutor.DeviceNotMounted,
			unmountDeviceCallCount: 0,
			volumeName:             volumetesting.FailMountDeviceVolumeName,
		},
		{
			name:                   "timeout followed by failed operation should result in non-mounted device",
			deviceState:            operationexecutor.DeviceNotMounted,
			unmountDeviceCallCount: 0,
			volumeName:             volumetesting.TimeoutAndFailOnMountDeviceVolumeName,
		},
		{
			name:                   "success followed by timeout operation should result in mounted device",
			deviceState:            operationexecutor.DeviceGloballyMounted,
			unmountDeviceCallCount: 1,
			volumeName:             volumetesting.SuccessAndTimeoutDeviceName,
			supportRemount:         true,
		},
		{
			name:                   "success followed by failed operation should result in mounted device",
			deviceState:            operationexecutor.DeviceGloballyMounted,
			unmountDeviceCallCount: 1,
			volumeName:             volumetesting.SuccessAndFailOnMountDeviceName,
			supportRemount:         true,
		},
	}

	modes := []v1.PersistentVolumeMode{v1.PersistentVolumeBlock, v1.PersistentVolumeFilesystem}

	hasher := fnv.New128a()
	for modeIndex := range modes {
		for tcIndex := range tests {
			mode := modes[modeIndex]
			tc := tests[tcIndex]
			testName := fmt.Sprintf("%s [%s]", tc.name, mode)
			uniqueTestString := fmt.Sprintf("global-mount-%s", testName)
			uniquePodDir := fmt.Sprintf("%s-%x", kubeletPodsDir, hasher.Sum([]byte(uniqueTestString)))
			t.Run(testName+"[", func(t *testing.T) {
				t.Parallel()
				pv := &v1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: tc.volumeName,
						UID:  "pvuid",
					},
					Spec: v1.PersistentVolumeSpec{
						ClaimRef:   &v1.ObjectReference{Name: "pvc"},
						VolumeMode: &mode,
					},
				}
				pvc := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvc",
						UID:  "pvcuid",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						VolumeName: tc.volumeName,
						VolumeMode: &mode,
					},
				}
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
									PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
										ClaimName: pvc.Name,
									},
								},
							},
						},
					},
				}

				node := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: string(nodeName),
					},
					Spec: v1.NodeSpec{},
					Status: v1.NodeStatus{
						VolumesAttached: []v1.AttachedVolume{
							{
								Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.volumeName)),
								DevicePath: "fake/path",
							},
						},
					},
				}
				volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
				fakePlugin.SupportsRemount = tc.supportRemount
				seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()

				dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
				asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
				kubeClient := createtestClientWithPVPVC(pv, pvc, v1.AttachedVolume{
					Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.volumeName)),
					DevicePath: "fake/path",
				})
				fakeRecorder := &record.FakeRecorder{}
				fakeHandler := volumetesting.NewBlockVolumePathHandler()
				oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
					kubeClient,
					volumePluginMgr,
					fakeRecorder,
					fakeHandler))

				reconciler := NewReconciler(
					kubeClient,
					true, /* controllerAttachDetachEnabled */
					reconcilerLoopSleepDuration,
					waitForAttachTimeout,
					nodeName,
					dsw,
					asw,
					hasAddedPods,
					oex,
					&mount.FakeMounter{},
					hostutil.NewFakeHostUtil(nil),
					volumePluginMgr,
					uniquePodDir)
				volumeSpec := &volume.Spec{PersistentVolume: pv}
				podName := util.GetUniquePodName(pod)
				volumeName, err := dsw.AddPodToVolume(
					logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
				// Assert
				if err != nil {
					t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
				}
				dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{volumeName})

				// Start the reconciler to fill ASW.
				stopChan, stoppedChan := make(chan struct{}), make(chan struct{})
				go func() {
					reconciler.Run(ctx, stopChan)
					close(stoppedChan)
				}()
				waitForVolumeToExistInASW(t, volumeName, asw)
				if tc.volumeName == volumetesting.TimeoutAndFailOnMountDeviceVolumeName {
					// Wait upto 10s for reconciler to catch up
					time.Sleep(reconcilerSyncWaitDuration)
				}

				if tc.volumeName == volumetesting.SuccessAndFailOnMountDeviceName ||
					tc.volumeName == volumetesting.SuccessAndTimeoutDeviceName {
					// wait for mount and then break it via remount
					waitForMount(t, fakePlugin, volumeName, asw)
					asw.MarkRemountRequired(logger, podName)
					time.Sleep(reconcilerSyncWaitDuration)
				}

				if tc.deviceState == operationexecutor.DeviceMountUncertain {
					waitForUncertainGlobalMount(t, volumeName, asw)
				}

				if tc.deviceState == operationexecutor.DeviceGloballyMounted {
					waitForMount(t, fakePlugin, volumeName, asw)
				}

				dsw.DeletePodFromVolume(podName, volumeName)
				waitForDetach(t, volumeName, asw)
				if mode == v1.PersistentVolumeFilesystem {
					err = volumetesting.VerifyUnmountDeviceCallCount(tc.unmountDeviceCallCount, fakePlugin)
				} else {
					if tc.unmountDeviceCallCount == 0 {
						err = volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin)
					} else {
						err = volumetesting.VerifyTearDownDeviceCallCount(tc.unmountDeviceCallCount, fakePlugin)
					}
				}
				if err != nil {
					t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
				}
			})
		}
	}
}

// 表驱动测试 Pod 级卷挂载的"不确定"状态转换，逻辑与设备全局挂载测试类似，
// 验证超时、失败及成功后的重试/重新挂载行为。
func Test_UncertainVolumeMountState(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	var tests = []struct {
		name                   string
		volumeState            operationexecutor.VolumeMountState
		unmountDeviceCallCount int
		unmountVolumeCount     int
		volumeName             string
		supportRemount         bool
		pvcStatusSize          resource.Quantity
		pvSize                 resource.Quantity
	}{
		{
			name:                   "timed out operations should result in volume marked as uncertain",
			volumeState:            operationexecutor.VolumeMountUncertain,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     1,
			volumeName:             volumetesting.TimeoutOnSetupVolumeName,
		},
		{
			name:                   "failed operation should result in not-mounted volume",
			volumeState:            operationexecutor.VolumeNotMounted,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     0,
			volumeName:             volumetesting.FailOnSetupVolumeName,
		},
		{
			name:                   "timeout followed by failed operation should result in non-mounted volume",
			volumeState:            operationexecutor.VolumeNotMounted,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     0,
			volumeName:             volumetesting.TimeoutAndFailOnSetupVolumeName,
		},
		{
			name:                   "success followed by timeout operation should result in mounted volume",
			volumeState:            operationexecutor.VolumeMounted,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     1,
			volumeName:             volumetesting.SuccessAndTimeoutSetupVolumeName,
			supportRemount:         true,
		},
		{
			name:                   "success followed by failed operation should result in mounted volume",
			volumeState:            operationexecutor.VolumeMounted,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     1,
			volumeName:             volumetesting.SuccessAndFailOnSetupVolumeName,
			supportRemount:         true,
		},
		{
			name:                   "mount success but fail to expand filesystem",
			volumeState:            operationexecutor.VolumeMountUncertain,
			unmountDeviceCallCount: 1,
			unmountVolumeCount:     1,
			volumeName:             volumetesting.FailVolumeExpansion,
			supportRemount:         true,
			pvSize:                 resource.MustParse("10G"),
			pvcStatusSize:          resource.MustParse("2G"),
		},
	}
	modes := []v1.PersistentVolumeMode{v1.PersistentVolumeBlock, v1.PersistentVolumeFilesystem}

	hasher := fnv.New128a()
	for modeIndex := range modes {
		for tcIndex := range tests {
			mode := modes[modeIndex]
			tc := tests[tcIndex]
			testName := fmt.Sprintf("%s [%s]", tc.name, mode)
			uniqueTestString := fmt.Sprintf("local-mount-%s", testName)
			uniquePodDir := fmt.Sprintf("%s-%x", kubeletPodsDir, hasher.Sum([]byte(uniqueTestString)))
			t.Run(testName, func(t *testing.T) {
				t.Parallel()
				pv := &v1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: tc.volumeName,
						UID:  "pvuid",
					},
					Spec: v1.PersistentVolumeSpec{
						ClaimRef:   &v1.ObjectReference{Name: "pvc"},
						VolumeMode: &mode,
					},
				}
				if tc.pvSize.CmpInt64(0) > 0 {
					pv.Spec.Capacity = v1.ResourceList{
						v1.ResourceStorage: tc.pvSize,
					}
				}
				pvc := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvc",
						UID:  "pvcuid",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						VolumeName: tc.volumeName,
						VolumeMode: &mode,
					},
				}
				if tc.pvcStatusSize.CmpInt64(0) > 0 {
					pvc.Status = v1.PersistentVolumeClaimStatus{
						Capacity: v1.ResourceList{
							v1.ResourceStorage: tc.pvcStatusSize,
						},
					}
				}
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
									PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
										ClaimName: pvc.Name,
									},
								},
							},
						},
					},
				}

				node := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: string(nodeName),
					},
					Status: v1.NodeStatus{
						VolumesAttached: []v1.AttachedVolume{
							{
								Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.volumeName)),
								DevicePath: "fake/path",
							},
						},
					},
				}

				volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
				fakePlugin.SupportsRemount = tc.supportRemount
				seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
				dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
				asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
				kubeClient := createtestClientWithPVPVC(pv, pvc, v1.AttachedVolume{
					Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", tc.volumeName)),
					DevicePath: "fake/path",
				})
				fakeRecorder := &record.FakeRecorder{}
				fakeHandler := volumetesting.NewBlockVolumePathHandler()
				oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
					kubeClient,
					volumePluginMgr,
					fakeRecorder,
					fakeHandler))

				reconciler := NewReconciler(
					kubeClient,
					true, /* controllerAttachDetachEnabled */
					reconcilerLoopSleepDuration,
					waitForAttachTimeout,
					nodeName,
					dsw,
					asw,
					hasAddedPods,
					oex,
					&mount.FakeMounter{},
					hostutil.NewFakeHostUtil(nil),
					volumePluginMgr,
					uniquePodDir)
				volumeSpec := &volume.Spec{PersistentVolume: pv}
				podName := util.GetUniquePodName(pod)
				volumeName, err := dsw.AddPodToVolume(
					logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
				// Assert
				if err != nil {
					t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
				}
				dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{volumeName})

				// Start the reconciler to fill ASW.
				stopChan, stoppedChan := make(chan struct{}), make(chan struct{})
				go func() {
					reconciler.Run(ctx, stopChan)
					close(stoppedChan)
				}()
				waitForVolumeToExistInASW(t, volumeName, asw)
				// all of these tests rely on device to be globally mounted and hence waiting for global
				// mount ensures that unmountDevice is called as expected.
				waitForGlobalMount(t, volumeName, asw)
				if tc.volumeName == volumetesting.TimeoutAndFailOnSetupVolumeName {
					// Wait upto 10s for reconciler to catchup
					time.Sleep(reconcilerSyncWaitDuration)
				}

				if tc.volumeName == volumetesting.SuccessAndFailOnSetupVolumeName ||
					tc.volumeName == volumetesting.SuccessAndTimeoutSetupVolumeName {
					// wait for mount and then break it via remount
					waitForMount(t, fakePlugin, volumeName, asw)
					asw.MarkRemountRequired(logger, podName)
					time.Sleep(reconcilerSyncWaitDuration)
				}

				if tc.volumeState == operationexecutor.VolumeMountUncertain {
					waitForUncertainPodMount(t, volumeName, podName, asw)
				}

				if tc.volumeState == operationexecutor.VolumeMounted {
					waitForMount(t, fakePlugin, volumeName, asw)
				}

				dsw.DeletePodFromVolume(podName, volumeName)
				waitForDetach(t, volumeName, asw)

				if mode == v1.PersistentVolumeFilesystem {
					if err := volumetesting.VerifyUnmountDeviceCallCount(tc.unmountDeviceCallCount, fakePlugin); err != nil {
						t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
					}
					if err := volumetesting.VerifyTearDownCallCount(tc.unmountVolumeCount, fakePlugin); err != nil {
						t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
					}
				} else {
					if tc.unmountVolumeCount == 0 {
						if err := volumetesting.VerifyZeroUnmapPodDeviceCallCount(fakePlugin); err != nil {
							t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
						}
					} else {
						if err := volumetesting.VerifyUnmapPodDeviceCallCount(tc.unmountVolumeCount, fakePlugin); err != nil {
							t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
						}
					}
					if tc.unmountDeviceCallCount == 0 {
						if err := volumetesting.VerifyZeroTearDownDeviceCallCount(fakePlugin); err != nil {
							t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
						}
					} else {
						if err := volumetesting.VerifyTearDownDeviceCallCount(tc.unmountDeviceCallCount, fakePlugin); err != nil {
							t.Errorf("Error verifying UnMountDeviceCallCount: %v", err)
						}
					}
				}
			})
		}
	}
}

// 轮询等待卷在 ASW 中以 DeviceMountUncertain 状态全局挂载，超时则测试失败。
func waitForUncertainGlobalMount(t *testing.T, volumeName v1.UniqueVolumeName, asw cache.ActualStateOfWorld) {
	// check if volume is globally mounted in uncertain state
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			unmountedVolumes := asw.GetUnmountedVolumes()
			for _, v := range unmountedVolumes {
				if v.VolumeName == volumeName && v.DeviceMountState == operationexecutor.DeviceMountUncertain {
					return true, nil
				}
			}
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf("expected volumes %s to be mounted in uncertain state globally", volumeName)
	}
}

// 轮询等待卷出现在 ASW 的全局已挂载卷列表中，超时则测试失败。
func waitForGlobalMount(t *testing.T, volumeName v1.UniqueVolumeName, asw cache.ActualStateOfWorld) {
	// check if volume is globally mounted
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			mountedVolumes := asw.GetGloballyMountedVolumes()
			for _, v := range mountedVolumes {
				if v.VolumeName == volumeName {
					return true, nil
				}
			}
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf("expected volume devices %s to be mounted globally", volumeName)
	}
}

// 轮询等待卷以"不确定"状态挂载到指定 Pod，超时则测试失败。
func waitForUncertainPodMount(t *testing.T, volumeName v1.UniqueVolumeName, podName types.UniquePodName, asw cache.ActualStateOfWorld) {
	logger, _ := ktesting.NewTestContext(t)
	// check if volume is locally pod mounted in uncertain state
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			mounted, _, err := asw.PodExistsInVolume(logger, podName, volumeName, resource.Quantity{}, "" /* SELinuxContext */)
			if mounted || err != nil {
				return false, nil
			}
			allMountedVolumes := asw.GetAllMountedVolumes()
			for _, v := range allMountedVolumes {
				if v.VolumeName == volumeName {
					return true, nil
				}
			}
			return false, nil
		},
	)

	if err != nil {
		t.Fatalf("expected volumes %s to be mounted in uncertain state for pod", volumeName)
	}
}

// 轮询等待卷出现在 ASW 的已挂载卷列表中，超时则测试失败。
func waitForMount(
	t *testing.T,
	fakePlugin *volumetesting.FakeVolumePlugin,
	volumeName v1.UniqueVolumeName,
	asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			mountedVolumes := asw.GetMountedVolumes()
			for _, mountedVolume := range mountedVolumes {
				if mountedVolume.VolumeName == volumeName {
					return true, nil
				}
			}

			return false, nil
		},
	)

	if err != nil {
		t.Fatalf("Timed out waiting for volume %q to be attached.", volumeName)
	}
}

// 轮询等待 Pod 对应的卷从 ASW 中消失（完成卸载），超时则测试失败。
// GetAllMountedVolumes 同时包含已挂载与不确定状态，因此也能覆盖重建卷的卸载。
func waitForUnmount(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	podName types.UniquePodName,
	asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			// GetAllMountedVolumes includes both VolumeMounted and VolumeMountUncertain
			// states, so this correctly detects unmount for reconstructed (uncertain)
			// volumes as well as normally mounted volumes.
			for _, v := range asw.GetAllMountedVolumes() {
				if v.PodName == podName {
					return false, nil
				}
			}
			return true, nil
		},
	)

	if err != nil {
		t.Fatalf("Timed out waiting for pod %q to be unmount from volume %q.", podName, volumeName)
	}
}

// 轮询等待卷被加入 ASW，超时则测试失败。
func waitForVolumeToExistInASW(t *testing.T, volumeName v1.UniqueVolumeName, asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			if asw.VolumeExists(volumeName) {
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("Timed out waiting for volume %q to be exist in asw.", volumeName)
	}
}

// 轮询等待卷从 ASW 中移除（完成 detach），超时则测试失败。
func waitForDetach(
	t *testing.T,
	volumeName v1.UniqueVolumeName,
	asw cache.ActualStateOfWorld) {
	err := retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			if asw.VolumeExists(volumeName) {
				return false, nil
			}

			return true, nil
		},
	)

	if err != nil {
		t.Fatalf("Timed out waiting for volume %q to be detached.", volumeName)
	}
}

// 以固定因子 3、最多 6 步的指数退避执行条件函数，
// 用于吸收 reconciler 异步循环带来的时序抖动。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    6,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// 构造 fake clientset：对 node get 返回给定的已挂载卷列表，
// 其余请求一律返回错误，从而尽早暴露未预期的 API 调用。
func createTestClient(attachedVolumes ...v1.AttachedVolume) *fake.Clientset {
	fakeClient := &fake.Clientset{}
	if len(attachedVolumes) == 0 {
		attachedVolumes = append(attachedVolumes, v1.AttachedVolume{
			Name:       "fake-plugin/fake-device1",
			DevicePath: "fake/path",
		})
	}
	fakeClient.AddReactor("get", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			return true, &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: string(nodeName)},
				Status: v1.NodeStatus{
					VolumesAttached: attachedVolumes,
				},
			}, nil
		},
	)

	fakeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("no reaction implemented for %s", action)
	})
	return fakeClient
}

// 在后台 goroutine 中启动 reconciler，直到进程结束或 ctx 取消。
func runReconciler(ctx context.Context, reconciler Reconciler) {
	go reconciler.Run(ctx, wait.NeverStop)
}

// 构造 fake clientset：node get 返回已挂载卷，PV/PVC get 返回给定对象，
// 并支持 PVC status 子资源 patch；其余请求返回错误以暴露意外调用。
func createtestClientWithPVPVC(pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, attachedVolumes ...v1.AttachedVolume) *fake.Clientset {
	fakeClient := &fake.Clientset{}
	if len(attachedVolumes) == 0 {
		attachedVolumes = append(attachedVolumes, v1.AttachedVolume{
			Name:       "fake-plugin/pv",
			DevicePath: "fake/path",
		})
	}
	fakeClient.AddReactor("get", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			return true, &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: string(nodeName)},
				Status: v1.NodeStatus{
					VolumesAttached: attachedVolumes,
				},
			}, nil
		})
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, pvc, nil
	})
	fakeClient.AddReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		return true, pv, nil
	})
	fakeClient.AddReactor("patch", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, pvc, nil
		}
		return true, nil, fmt.Errorf("no reaction implemented for %s", action)
	})
	fakeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("no reaction implemented for %s", action)
	})
	return fakeClient
}

// 回归测试：卷在 UnmountDevice 执行期间被重新加入 DSW，
// 重新 attach 时必须沿用 Node.Status 中的 devicePath，不能被卸载流程清空。
// 通过 fake 插件的 hook 精确控制步骤 1-4 的交错顺序。
func Test_Run_Positive_VolumeMountControllerAttachEnabledRace(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "/fake/path",
				},
			},
		},
	}
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()

	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	reconciler := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
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

	// 本测试通过插件回调制造交错执行，步骤顺序与代码顺序不同，按注释中的编号阅读。
	// Some steps are executes out of order in callbacks, follow the numbers.

	// 1. 把卷加入 DSW 并等待其挂载完成，作为竞态测试的初始状态。
	// 1. Add a volume to DSW and wait until it's mounted
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	// reconciler 会并发读取 volumeSpec，因此回调里必须使用独立副本以避免数据竞争。
	// copy before reconciler runs to avoid data race.
	volumeSpecCopy := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(
		logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
	dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeName})

	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}
	// Start the reconciler to fill ASW.
	stopChan, stoppedChan := make(chan struct{}), make(chan struct{})
	go func() {
		reconciler.Run(ctx, stopChan)
		close(stoppedChan)
	}()
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
	// Stop the reconciler.
	close(stopChan)
	<-stoppedChan

	finished := make(chan interface{})
	finishedOnce := &sync.Once{}

	fakePlugin.Lock()
	fakePlugin.UnmountDeviceHook = func(mountPath string) error {
		// Act:
		// 3. 卷正在卸载时把它重新加入 DSW，复现"卸载与重新挂载"的交错。
		// 3. While a volume is being unmounted, add it back to the desired state of world
		logger.Info("UnmountDevice called")
		var generatedVolumeNameCopy v1.UniqueVolumeName
		generatedVolumeNameCopy, err = dsw.AddPodToVolume(
			logger, podName, pod, volumeSpecCopy, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* seLinuxLabel */)
		dsw.MarkVolumesReportedInUse([]v1.UniqueVolumeName{generatedVolumeNameCopy})
		return nil
	}

	fakePlugin.WaitForAttachHook = func(spec *volume.Spec, devicePath string, pod *v1.Pod, spectimeout time.Duration) (string, error) {
		defer finishedOnce.Do(func() {
			close(finished)
		})
		// Assert
		// 4. 卷再次挂载时，UnmountDevice 不应清空 devicePath，否则重新 attach 会丢失设备路径。
		// 4. When the volume is mounted again, expect that UnmountDevice operation did not clear devicePath
		if devicePath == "" {
			logger.Error(nil, "Expected WaitForAttach called with devicePath from Node.Status")
			return "", fmt.Errorf("Expected devicePath from Node.Status")
		}
		return devicePath, nil
	}
	fakePlugin.Unlock()

	// 重新启动 reconciler，让步骤 2 的删除操作触发卸载回调。
	// Start the reconciler again.
	go reconciler.Run(ctx, wait.NeverStop)

	// 2. 从 DSW 删除卷，触发卸载，并等待回调中的步骤 3/4 完成。
	// 2. Delete the volume from DSW (and wait for callbacks)
	dsw.DeletePodFromVolume(podName, generatedVolumeName)

	<-finished
	waitForMount(t, fakePlugin, generatedVolumeName, asw)
}

// 构造一个带有 fake-device1 已挂载记录的 Node 对象。
func getFakeNode() *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "/fake/path",
				},
			},
		},
	}
}

// 构造一个使用内联 GCEPersistentDisk 卷的测试 Pod。
func getInlineFakePod(podName, podUUID, outerName, innerName string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  k8stypes.UID(podUUID),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: outerName,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: innerName,
						},
					},
				},
			},
		},
	}
	return pod
}

// 按给定 kubelet 目录和挂载点构造一个启用了 controller attach/detach 的 reconciler 及 fake 插件。
func getReconciler(kubeletDir string, t *testing.T, volumePaths []string, kubeClient *fake.Clientset) (Reconciler, *volumetesting.FakeVolumePlugin) {
	node := getFakeNode()
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNodeAndRoot(t, node, kubeletDir)
	tmpKubeletPodDir := filepath.Join(kubeletDir, "pods")
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()

	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	if kubeClient == nil {
		kubeClient = createTestClient()
	}

	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	mountPoints := []mount.MountPoint{}
	for _, volumePath := range volumePaths {
		mountPoints = append(mountPoints, mount.MountPoint{Path: volumePath})
	}
	rc := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(mountPoints),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		tmpKubeletPodDir)
	return rc, fakePlugin
}

// 从重建卷启动 Run()，验证 devicePath 和可 attach 属性会依据 Node.Status 回填：
// 一个卷在 Node.Status 中有记录，另一个没有，二者应分别得到不同的更新结果。
func TestReconcileWithUpdateReconstructedFromAPIServer(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Calls Run() with two reconstructed volumes.
	// Verifies the devicePaths + volume attachability are reconstructed from node.status.

	// Arrange
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nodeName),
		},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake-plugin/fake-device1",
					DevicePath: "fake/path",
				},
			},
		},
	}
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgrWithNode(t, node)
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	rc := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
	reconciler := rc.(*reconciler)

	// The pod has two volumes, fake-device1 is attachable, fake-device2 is not.
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
				{
					Name: "volume-name2",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "fake-device2",
						},
					},
				},
			},
		},
	}

	volumeSpec1 := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	volumeName1 := util.GetUniqueVolumeName(fakePlugin.GetPluginName(), "fake-device1")
	volumeSpec2 := &volume.Spec{Volume: &pod.Spec.Volumes[1]}
	volumeName2 := util.GetUniqueVolumeName(fakePlugin.GetPluginName(), "fake-device2")

	assert.NoError(t, asw.AddAttachUncertainReconstructedVolume(logger, volumeName1, volumeSpec1, nodeName, ""))
	assert.NoError(t, asw.MarkDeviceAsUncertain(volumeName1, "/dev/badly/reconstructed", "/var/lib/kubelet/plugins/global1", ""))
	assert.NoError(t, asw.AddAttachUncertainReconstructedVolume(logger, volumeName2, volumeSpec2, nodeName, ""))
	assert.NoError(t, asw.MarkDeviceAsUncertain(volumeName2, "/dev/reconstructed", "/var/lib/kubelet/plugins/global2", ""))

	assert.False(t, reconciler.StatesHasBeenSynced())

	reconciler.volumesNeedUpdateFromNodeStatus = append(reconciler.volumesNeedUpdateFromNodeStatus, volumeName1, volumeName2)
	// Act - 只运行一次 reconcile 循环。
	// 由于 volumesNeedUpdateFromNodeStatus 非空，本次循环只做状态回填，不会触发卸载。
	// Act - run reconcile loop just once.
	// "volumesNeedUpdateFromNodeStatus" is not empty, so no unmount will be triggered.
	reconciler.reconcile(ctx)

	// Assert
	assert.True(t, reconciler.StatesHasBeenSynced())
	assert.Empty(t, reconciler.volumesNeedUpdateFromNodeStatus)

	attachedVolumes := asw.GetAttachedVolumes()
	assert.Lenf(t, attachedVolumes, 2, "two volumes in ASW expected")
	for _, vol := range attachedVolumes {
		if vol.VolumeName == volumeName1 {
			// devicePath + attachability must have been updated from node.status
			assert.True(t, vol.PluginIsAttachable)
			assert.Equal(t, "fake/path", vol.DevicePath)
		}
		if vol.VolumeName == volumeName2 {
			// only attachability was updated from node.status
			assert.False(t, vol.PluginIsAttachable)
			assert.Equal(t, "/dev/reconstructed", vol.DevicePath)
		}
	}
}

// 验证重建卷在 Setup 失败后仍能成功卸载：
// fake 插件在非重建卷时模拟卷数据文件丢失，确保卸载路径不依赖重新加载卷数据。
func TestReconstructedVolumeShouldUnmountSucceedAfterSetupFailed(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	// Arrange
	volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	// fake Setup error
	fakePlugin.SetUpHook = func(plugin volume.VolumePlugin, mounterArgs volume.MounterArgs) error {
		if !mounterArgs.ReconstructedVolume {
			// mock cleaned volume files while it's not a reconstructed volume
			plugin.(*volumetesting.FakeVolumePlugin).NewUnmounterError = errors.New("unmounter failed to load volume data file")
		}
		return errors.New("csiRPCError")
	}
	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	dsw := cache.NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
	asw := cache.NewActualStateOfWorld(nodeName, volumePluginMgr)
	kubeClient := createTestClient()
	fakeRecorder := &record.FakeRecorder{}
	fakeHandler := volumetesting.NewBlockVolumePathHandler()
	oex := operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		fakeRecorder,
		fakeHandler))
	rc := NewReconciler(
		kubeClient,
		true, /* controllerAttachDetachEnabled */
		reconcilerLoopSleepDuration,
		waitForAttachTimeout,
		nodeName,
		dsw,
		asw,
		hasAddedPods,
		oex,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		volumePluginMgr,
		kubeletPodsDir)
	reconciler := rc.(*reconciler)

	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv",
			UID:  "pvuid",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef: &v1.ObjectReference{Name: "pvc"},
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse("1G"),
			},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc",
			UID:  "pvcuid",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("1G"),
				},
			},
			VolumeName: "pv",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse("1G"),
			},
		},
	}
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
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
		},
	}

	volumeSpec := &volume.Spec{PersistentVolume: pv}
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := dsw.AddPodToVolume(logger,
		podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGidValue */, nil)

	// Assert
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}
	err = asw.AddAttachUncertainReconstructedVolume(logger, generatedVolumeName, volumeSpec, "" /* nodeName */, "fake/device/path")
	if err != nil {
		t.Fatalf("MarkVolumeAsAttached failed. Expected: <no error> Actual: <%v>", err)
	}

	mounter, err := fakePlugin.NewMounter(volumeSpec, pod)
	if err != nil {
		t.Fatalf("NewMounter failed. Expected: <no error> Actual: <%v>", err)
	}

	mapper, err := fakePlugin.NewBlockVolumeMapper(volumeSpec, pod)
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
		VolumeMountState:  operationexecutor.VolumeMountUncertain,
	}
	_, err = asw.CheckAndMarkVolumeAsUncertainViaReconstruction(markVolumeOpts)
	if err != nil {
		t.Fatalf("AddPodToVolume failed. Expected: <no error> Actual: <%v>", err)
	}

	// Act first reconcile to trigger mount reconstructed volume
	reconciler.reconcile(ctx)

	// Wait for the operation executor to finish processing the mount failure.
	// SetUp returning is not sufficient because the executor clears the pending
	// operation after the generated operation handles the error.
	err = retryWithExponentialBackOff(
		testOperationBackOffDuration,
		func() (bool, error) {
			return !oex.IsOperationPending(
				generatedVolumeName,
				nestedpendingoperations.EmptyUniquePodName,
				nestedpendingoperations.EmptyNodeName,
			), nil
		},
	)
	if err != nil {
		t.Fatalf("Timed out waiting for failed mount operation to stop being pending")
	}

	waitForUncertainPodMount(t, generatedVolumeName, podName, asw)

	// mock remove pod
	dsw.DeletePodFromVolume(podName, generatedVolumeName)
	// Act second reconcile to trigger unmount reconstructed volume after removed pod from volume
	reconciler.reconcile(ctx)

	// assert volume unmount succeed
	waitForUnmount(t, generatedVolumeName, podName, asw)
}
