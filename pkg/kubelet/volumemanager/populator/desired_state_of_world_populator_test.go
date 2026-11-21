/*
Copyright 2017 The Kubernetes Authors.

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

package populator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

const (
	Removed     string = "removed"
	Terminating string = "terminating"
	Other       string = "other"
)

// pluginPVOmittingClient 将 dswp 的卷插件管理器替换为找不到任何 PV 的客户端，用于模拟卷插件无法获取 PV 信息的场景。
func pluginPVOmittingClient(dswp *desiredStateOfWorldPopulator) {
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return false, nil, nil
	})
	fakeClient.AddReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		return false, nil, nil
	})
	dswp.kubeClient = fakeClient
}

// prepareDswpWithVolume 构造带单个卷的 desiredStateOfWorldPopulator 及配套的 pod manager 与 pod 状态提供者。
func prepareDswpWithVolume(t *testing.T) (*desiredStateOfWorldPopulator, kubepod.Manager, *fakePodStateProvider) {
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, fakePodStateProvider := createDswpWithVolume(t, pv, pvc)
	return dswp, fakePodManager, fakePodStateProvider
}

// TestFindAndAddNewPods_WithDifferentConditions 校验 findAndAddNewPods 在 pod 被新增、终止中以及已删除等不同状态下的行为。
func TestFindAndAddNewPods_WithDifferentConditions(t *testing.T) {
	tests := []struct {
		desc          string
		hasAddedPods  bool
		podState      string
		expectedFound bool // Found pod is added to DSW
	}{
		{
			desc:          "HasAddedPods is false, ShouldPodRuntimeBeRemoved and ShouldPodContainerBeTerminating are both true",
			hasAddedPods:  false,
			podState:      Removed,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is false, ShouldPodRuntimeBeRemoved is false, ShouldPodContainerBeTerminating is true",
			hasAddedPods:  false,
			podState:      Terminating,
			expectedFound: true, // Pod should be added to DSW
		},
		{
			desc:          "HasAddedPods is false, other condition",
			hasAddedPods:  false,
			podState:      Other,
			expectedFound: true, // Pod should be added to DSW
		},
		{
			desc:          "HasAddedPods is true, ShouldPodRuntimeBeRemoved is false, ShouldPodContainerBeTerminating is true",
			hasAddedPods:  true,
			podState:      Terminating,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is true, ShouldPodRuntimeBeRemoved and ShouldPodContainerBeTerminating are both true",
			hasAddedPods:  true,
			podState:      Removed,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is true, other condition",
			hasAddedPods:  true,
			podState:      Other,
			expectedFound: true, // Pod should be added to DSW
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// 创建 dswp
			dswp, fakePodManager, fakePodState := prepareDswpWithVolume(t)

			// 创建 pod
			containers := []v1.Container{
				{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "dswp-test-volume-name",
							MountPath: "/mnt",
						},
					},
				},
			}
			pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

			fakePodManager.AddPod(pod)

			switch tc.podState {
			case Removed:
				fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}
			case Terminating:
				fakePodState.terminating = map[kubetypes.UID]struct{}{pod.UID: {}}
			case Other:
				break
			}

			dswp.hasAddedPods = tc.hasAddedPods
			// 执行动作
			tCtx := ktesting.Init(t)
			dswp.findAndAddNewPods(tCtx)

			// 校验结果
			podsInDSW := dswp.desiredStateOfWorld.GetPods()
			found := false
			if podsInDSW[types.UniquePodName(pod.UID)] {
				found = true
			}

			if found != tc.expectedFound {
				t.Fatalf(
					"Pod with uid %v has expectedFound value %v in pods in DSW %v",
					pod.UID, tc.expectedFound, podsInDSW)
			}
		})
	}
}

// TestFindAndAddNewPods_WithReprocessPodAndVolumeRetrievalError 校验重新处理 pod 时若卷信息获取失败，pod 不应被加入期望状态。
func TestFindAndAddNewPods_WithReprocessPodAndVolumeRetrievalError(t *testing.T) {
	// 创建 dswp
	dswp, fakePodManager, _ := prepareDswpWithVolume(t)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.podPreviouslyProcessed(podName) {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}
	pluginPVOmittingClient(dswp)

	dswp.ReprocessPod(podName)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.podPreviouslyProcessed(podName) {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}
	fakePodManager.RemovePod(pod)
}

// TestFindAndAddNewPods_WithVolumeRetrievalError 校验首次处理 pod 时若卷信息获取失败，pod 不应被加入期望状态。
func TestFindAndAddNewPods_WithVolumeRetrievalError(t *testing.T) {
	// 创建 dswp
	dswp, fakePodManager, _ := prepareDswpWithVolume(t)

	pluginPVOmittingClient(dswp)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if dswp.podPreviouslyProcessed(podName) {
		t.Fatalf("The volumes for the specified pod: %s should not have been processed by the populator", podName)
	}
	if dswp.podHasBeenSeenOnce(podName) {
		t.Fatalf("The volumes for the specified pod: %s should not have been processed by the populator", podName)
	}
}

// mutablePodManager 在 pod manager 接口之外暴露删除 pod 的能力，便于测试模拟 pod 消失。
type mutablePodManager interface {
	GetPodByName(string, string) (*v1.Pod, bool)
	RemovePod(*v1.Pod)
}

// TestFindAndAddNewPods_FindAndRemoveDeletedPods 校验 pod 被标记删除后，findAndRemoveDeletedPods 会移除该 pod 及其挂载的卷，并正确处理孤儿 pod。
func TestFindAndAddNewPods_FindAndRemoveDeletedPods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, _ := prepareDSWPWithPodPV(t)
	podName := util.GetUniquePodName(pod)

	// 将 pod 置为终止状态
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	dswp.podManager.(mutablePodManager).RemovePod(pod)

	dswp.findAndRemoveDeletedPods(logger)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Pod should not been removed from desired state of world since pod state still thinks it exists")
	}

	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// pod 状态已标记为移除，因此 findAndRemoveDeletedPods() 会删除该 pod 及其挂载的卷
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	// podWorker 可能在我们处理完 findAndRemoveDeletedPods() 之后才调用 volume_manager WaitForUnmount()
	dswp.ReprocessPod(podName)
	dswp.findAndRemoveDeletedPods(logger)

	// 上面的 findAndRemoveDeletedPods() 必须检测到孤儿 pod 并将其从 map 中删除
	if _, ok := dswp.pods.processedPods[podName]; ok {
		t.Fatalf("Failed to remove orphanded pods from internal map")
	}

	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := dswp.desiredStateOfWorld.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

// TestFindAndRemoveDeletedPodsWithActualState 校验 findAndRemoveDeletedPods 需要与实际状态对账后才能清理已终止 pod 的卷。
func TestFindAndRemoveDeletedPodsWithActualState(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, _ := prepareDSWPWithPodPV(t)
	fakeASW := dswp.actualStateOfWorld
	podName := util.GetUniquePodName(pod)

	// 将 pod 置为终止状态
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed

	dswp.findAndRemoveDeletedPods(logger)
	// 尽管 Pod 状态已终止，但 pod 仍存在于 pod manager 中，且实际状态中没有该 pod 及卷信息
	// 因此 desired state populator 首次会删除失败
	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	// 与实际状态对账，使卷被加入实际状态
	// 此时 desired state populator 才能成功删除 pod 与卷
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)
	dswp.findAndRemoveDeletedPods(logger)
	if !dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */) {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// 与实际状态对账，使卷被加入实际状态
	// 此时 desired state populator 才能成功删除 pod 与卷
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)
	dswp.findAndRemoveDeletedPods(logger)
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}
}

// TestFindAndRemoveDeletedPodsWithUncertain 校验卷处于 uncertain 状态时，已终止 pod 的清理会被推迟到卷状态确定之后。
func TestFindAndRemoveDeletedPodsWithUncertain(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, pv := prepareDSWPWithPodPV(t)
	podName := util.GetUniquePodName(pod)

	// 将 pod 置为终止状态
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	dswp.podManager.(mutablePodManager).RemovePod(pod)
	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// 通过对账把该卷加入 ASW。
	fakeASW := dswp.actualStateOfWorld
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)

	// 将卷标记为 uncertain
	opts := operationexecutor.MarkVolumeOpts{
		PodName:          util.GetUniquePodName(pod),
		PodUID:           pod.UID,
		VolumeName:       expectedVolumeName,
		VolumeGIDVolume:  "",
		VolumeSpec:       volume.NewSpecFromPersistentVolume(pv, false),
		VolumeMountState: operationexecutor.VolumeMountUncertain,
	}
	err := dswp.actualStateOfWorld.MarkVolumeMountAsUncertain(opts)
	if err != nil {
		t.Fatalf("Failed to set the volume as uncertain: %s", err)
	}

	// pod 状态现在将 pod 标记为已移除，因此 findAndRemoveDeletedPods() 会删除 pod 及其挂载的卷
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := dswp.desiredStateOfWorld.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

// prepareDSWPWithPodPV 构造带 pod 与 PV/PVC 的 desiredStateOfWorldPopulator 及测试数据。
func prepareDSWPWithPodPV(t *testing.T) (*desiredStateOfWorldPopulator, *fakePodStateProvider, *v1.Pod, v1.UniqueVolumeName, *v1.PersistentVolume) {
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, fakesDSW, _, fakePodState := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)
	return dswp, fakePodState, pod, expectedVolumeName, pv
}

// TestFindAndRemoveNonattachableVolumes 校验卷插件从可挂载变为不可挂载后，期望状态中的卷会先保留、随后被正确移除并更新 attachable 字段。
func TestFindAndRemoveNonattachableVolumes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	fakeVolumePluginMgr, fakeVolumePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	dswp, fakePodManager, fakesDSW, _, _ := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	// 将卷插件从可挂载改为不可挂载
	fakeVolumePlugin.NonAttachable = true

	// 该卷应仍然存在
	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)

	dswp.findAndRemoveDeletedPods(logger)
	// 卷插件变为不可挂载后，对应卷的 attachable 字段应随之改变。
	volumesToMount := fakesDSW.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			if volume.PluginIsAttachable {
				t.Fatalf(
					"Volume %v in the list of desired state of world volumes to mount is still attachable. Expected not",
					expectedVolumeName)
			}
		}
	}
}

// TestEphemeralVolumeOwnerCheck 校验仅当当前 pod 是临时卷的属主时，该卷才会被加入期望状态。
func TestEphemeralVolumeOwnerCheck(t *testing.T) {
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pod, pv, pvc := createEphemeralVolumeObjects("dswp-test-pod", "dswp-test-volume-name", false /* not owned */, &mode)
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)
	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("%s should not have been processed by the populator", podName)
	}
	require.Equal(t,
		[]string{fmt.Sprintf("PVC %s/%s was not created for pod %s/%s (pod is not owner)",
			pvc.Namespace, pvc.Name,
			pod.Namespace, pod.Name,
		)},
		dswp.desiredStateOfWorld.PopPodErrors(podName),
	)
}

// TestFindAndAddNewPods_FindAndRemoveDeletedPods_Valid_Block_VolumeDevices 校验使用块设备卷的 pod 在被删除后，其卷也能被正确清理。
func TestFindAndAddNewPods_FindAndRemoveDeletedPods_Valid_Block_VolumeDevices(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 创建 dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, fakesDSW, _, fakePodState := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)

	// 将 pod 置为终止状态
	podGet, exist := fakePodManager.GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	fakePodManager.RemovePod(pod)
	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// pod 被加入 fakePodManager，但 pod 状态已知其已被移除，因此 findAndRemoveDeletedPods() 会删除 pod 及其挂载的卷
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	volumeExists = fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := fakesDSW.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

// TestCreateVolumeSpec_Valid_File_VolumeMounts 校验合法文件卷挂载能生成正确的 VolumeSpec。
func TestCreateVolumeSpec_Valid_File_VolumeMounts(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// 断言
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of filesystem mode and volumeMounts. err: %v", err)
	}
}

// TestCreateVolumeSpec_Valid_Nil_VolumeMounts 校验 pod 未声明卷挂载时不会生成多余的 VolumeSpec。
func TestCreateVolumeSpec_Valid_Nil_VolumeMounts(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 创建 dswp
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
			VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// 断言
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of filesystem mode and volumeMounts. err: %v", err)
	}
}

// TestCreateVolumeSpec_Valid_Block_VolumeDevices 校验合法块设备卷能生成正确的 VolumeSpec。
func TestCreateVolumeSpec_Valid_Block_VolumeDevices(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 创建 dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// 断言
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of block mode and volumeDevices. err: %v", err)
	}
}

// TestCreateVolumeSpec_Invalid_File_VolumeDevices 校验以文件卷方式使用 VolumeDevices 属于非法配置。
func TestCreateVolumeSpec_Invalid_File_VolumeDevices(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 创建 dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// 断言
	if volumeSpec != nil || err == nil {
		t.Fatalf("Unexpected volumeMode and volumeMounts/volumeDevices combination is accepted")
	}
}

// TestCreateVolumeSpec_Invalid_Block_VolumeMounts 校验以块设备方式使用 VolumeMounts 属于非法配置。
func TestCreateVolumeSpec_Invalid_Block_VolumeMounts(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 创建 dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// 创建 pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(tCtx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// 断言
	if volumeSpec != nil || err == nil {
		t.Fatalf("Unexpected volumeMode and volumeMounts/volumeDevices combination is accepted")
	}
}

// TestCheckVolumeFSResize 校验 checkVolumeFSResize 仅在卷确实需要且已挂载时标记 fsResizeRequired。
func TestCheckVolumeFSResize(t *testing.T) {
	setCapacity := func(pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, capacity int) {
		pv.Spec.Capacity = volumeCapacity(capacity)
		pvc.Spec.Resources.Requests = volumeCapacity(capacity)
	}

	testcases := []struct {
		resize      func(*testing.T, *v1.PersistentVolume, *v1.PersistentVolumeClaim, *desiredStateOfWorldPopulator)
		verify      func(*testing.T, []v1.UniqueVolumeName, v1.UniqueVolumeName)
		readOnlyVol bool
		volumeMode  v1.PersistentVolumeMode
		volumeType  string
	}{
		{
			// 卷没有扩容请求，ASW 中的卷不应被标记为 fsResizeRequired
			resize: func(*testing.T, *v1.PersistentVolume, *v1.PersistentVolumeClaim, *desiredStateOfWorldPopulator) {
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("No resize request for any volumes, but found resize required volumes in ASW: %v", vols)
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},

		{
			// 将卷以只读方式使用，因此不应标记为 fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("volume mounted as ReadOnly, but found resize required volumes in ASW: %v", vols)
				}
			},
			readOnlyVol: true,
			volumeMode:  v1.PersistentVolumeFilesystem,
		},
		{
			// 清空 ASW，卷未挂载，因此不应标记为 fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, dswp *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
				clearASW(dswp.actualStateOfWorld, dswp.desiredStateOfWorld, t)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("volume hasn't been mounted, but found resize required volumes in ASW: %v", vols)
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			// ASW 中的卷应被标记为 fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Request resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			// ASW 中的卷应被标记为 fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Requested resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeBlock,
		},
		{
			// ASW 中的卷应被标记为 fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Requested resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
			volumeType: "ephemeral",
		},
	}

	for _, tc := range testcases {
		var pod *v1.Pod
		var pvc *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume

		if tc.volumeType == "ephemeral" {
			pod, pv, pvc = createEphemeralVolumeObjects("dswp-test-pod", "dswp-test-volume-name", true, &tc.volumeMode)
		} else {
			pv, pvc = createResizeRelatedVolumes(&tc.volumeMode)
			containers := []v1.Container{}

			if tc.volumeMode == v1.PersistentVolumeFilesystem {
				containers = append(containers, v1.Container{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      pv.Name,
							MountPath: "/mnt",
							ReadOnly:  tc.readOnlyVol,
						},
					},
				})
			} else {
				containers = append(containers, v1.Container{
					VolumeDevices: []v1.VolumeDevice{
						{
							Name:       pv.Name,
							DevicePath: "/mnt/foobar",
						},
					},
				})
			}

			pod = createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)
			pod.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ReadOnly = tc.readOnlyVol
		}

		dswp, fakePodManager, fakeDSW, _, _ := createDswpWithVolume(t, pv, pvc)
		fakeASW := dswp.actualStateOfWorld
		uniquePodName := types.UniquePodName(pod.UID)
		uniqueVolumeName := v1.UniqueVolumeName("fake-plugin/" + pod.Spec.Volumes[0].Name)

		fakePodManager.AddPod(pod)
		// 填充 dsw，使其包含卷与 pod。
		tCtx := ktesting.Init(t)
		dswp.findAndAddNewPods(tCtx)
		reconcileASW(fakeASW, fakeDSW, t)

		func() {
			tc.resize(t, pv, pvc, dswp)

			resizeRequiredVolumes := reprocess(tCtx, dswp, uniquePodName, fakeDSW, fakeASW, *pv.Spec.Capacity.Storage())

			tc.verify(t, resizeRequiredVolumes, uniqueVolumeName)
		}()
	}
}

// TestCheckVolumeSELinux 校验 checkVolumeSELinux 能根据 pod 的 SELinux 变更选项正确更新卷的 SELinux 上下文。
func TestCheckVolumeSELinux(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, true)
	fullOpts := &v1.SELinuxOptions{
		User:  "system_u",
		Role:  "object_r",
		Type:  "container_t",
		Level: "s0:c1,c2",
	}
	differentFullOpts := &v1.SELinuxOptions{
		User:  "system_u",
		Role:  "object_r",
		Type:  "container_t",
		Level: "s0:c9998,c9999",
	}
	partialOpts := &v1.SELinuxOptions{
		Level: "s0:c3,c4",
	}

	testcases := []struct {
		name                         string
		accessModes                  []v1.PersistentVolumeAccessMode
		existingContainerSELinuxOpts *v1.SELinuxOptions
		newContainerSELinuxOpts      *v1.SELinuxOptions
		seLinuxMountFeatureEnabled   bool
		pluginSupportsSELinux        bool
		expectError                  bool
		expectedContext              string
	}{
		{
			name:                    "RWOP with plugin with SELinux with full context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                    "RWOP with plugin with SELinux with partial context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: partialOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "system_u:object_r:container_file_t:s0:c3,c4",
		},
		{
			name:                    "RWX with plugin with SELinux with full context in pod and SELinuxMount feature disabled",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "", // RWX volumes don't support SELinux
		},
		{
			name:                       "RWX with plugin with SELinux with full context in pod and SELinuxMount feature enabled",
			accessModes:                []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			newContainerSELinuxOpts:    fullOpts,
			pluginSupportsSELinux:      true,
			seLinuxMountFeatureEnabled: true,
			expectedContext:            "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                    "RWOP with plugin with no SELinux with full context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   false,
			expectedContext:         "", // plugin doesn't support SELinux
		},
		{
			name:                    "RWOP with plugin with SELinux with no context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: nil,
			pluginSupportsSELinux:   true,
			expectedContext:         "",
		},
		{
			name:                         "RWOP with plugin with SELinux with full context in pod with existing pod",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      fullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                         "mismatched SELinux with RWX - success",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "",
		},
		{
			name:                         "mismatched SELinux with RWX and SELinuxMount feature disabled",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "",
		},
		{
			name:                         "mismatched SELinux with RWX and SELinuxMount feature enabled",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			seLinuxMountFeatureEnabled:   true,
			expectError:                  true,
			// 原有的 seLinuxOpts 被保留在 DSW 中
			expectedContext: "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                         "mismatched SELinux with RWOP - failure",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectError:                  true,
			// 原有的 seLinuxOpts 被保留在 DSW 中
			expectedContext: "system_u:object_r:container_file_t:s0:c1,c2",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			pv := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dswp-test-volume-name",
				},
				Spec: v1.PersistentVolumeSpec{
					VolumeMode:             ptr.To(v1.PersistentVolumeFilesystem),
					PersistentVolumeSource: v1.PersistentVolumeSource{RBD: &v1.RBDPersistentVolumeSource{}},
					Capacity:               volumeCapacity(1),
					ClaimRef:               &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
					AccessModes:            tc.accessModes,
				},
			}
			pvc := &v1.PersistentVolumeClaim{
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
					VolumeName: pv.Name,
					Resources: v1.VolumeResourceRequirements{
						Requests: pv.Spec.Capacity,
					},
					AccessModes: tc.accessModes,
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase:    v1.ClaimBound,
					Capacity: pv.Spec.Capacity,
				},
			}

			container := v1.Container{
				SecurityContext: &v1.SecurityContext{
					SELinuxOptions: nil,
				},
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      pv.Name,
						MountPath: "/mnt",
					},
				},
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMount, tc.seLinuxMountFeatureEnabled)

			fakeVolumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
			plugin.SupportsSELinux = tc.pluginSupportsSELinux
			dswp, fakePodManager, fakeDSW, _, _ := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)

			tCtx := ktesting.Init(t)
			var existingPod *v1.Pod
			if tc.existingContainerSELinuxOpts != nil {
				// 加入已存在的 pod 与卷
				existingContainer := container
				existingContainer.SecurityContext.SELinuxOptions = tc.existingContainerSELinuxOpts
				existingPod = createPodWithVolume("dswp-old-pod", "dswp-test-volume-name", "file-bound", []v1.Container{existingContainer})
				fakePodManager.AddPod(existingPod)
				dswp.findAndAddNewPods(tCtx)
			}

			newContainer := container
			newContainer.SecurityContext.SELinuxOptions = tc.newContainerSELinuxOpts
			newPod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{newContainer})

			// 执行：加入新的 Pod
			fakePodManager.AddPod(newPod)
			dswp.findAndAddNewPods(tCtx)

			// 断言

			// 检查全局卷状态
			uniquePodName := types.UniquePodName(newPod.UID)
			uniqueVolumeName := v1.UniqueVolumeName("fake-plugin/" + newPod.Spec.Volumes[0].Name)
			volumeExists := fakeDSW.VolumeExists(uniqueVolumeName, tc.expectedContext)
			if !volumeExists {
				t.Errorf(
					"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
					uniqueVolumeName,
					volumeExists)
			}

			// 检查 Pod 本地卷状态
			podExistsInVolume := fakeDSW.PodExistsInVolume(uniquePodName, uniqueVolumeName, tc.expectedContext)
			if !podExistsInVolume && !tc.expectError {
				t.Errorf(
					"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
					podExistsInVolume)
			}
			if podExistsInVolume && tc.expectError {
				t.Errorf(
					"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
					podExistsInVolume)
			}
			errors := fakeDSW.GetPodsWithErrors()
			if tc.expectError && len(errors) == 0 {
				t.Errorf("Expected Pod error, got none")
			}
			if !tc.expectError && len(errors) > 0 {
				t.Errorf("Unexpected Pod errors: %v", errors)
			}
			verifyVolumeExistsInVolumesToMount(t, uniqueVolumeName, false /* expectReportedInUse */, fakeDSW)
		})
	}
}

// createResizeRelatedVolumes 构造一组用于扩容测试的 PV 与 PVC，并设置指定卷模式。
func createResizeRelatedVolumes(volumeMode *v1.PersistentVolumeMode) (pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) {
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{RBD: &v1.RBDPersistentVolumeSource{}},
			Capacity:               volumeCapacity(1),
			ClaimRef:               &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode:             volumeMode,
		},
	}
	pvc = &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: pv.Name,
			Resources: v1.VolumeResourceRequirements{
				Requests: pv.Spec.Capacity,
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: pv.Spec.Capacity,
		},
	}
	return
}

// volumeCapacity 返回仅包含 storage 资源的容量列表。
func volumeCapacity(size int) v1.ResourceList {
	return v1.ResourceList{v1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", size))}
}

// reconcileASW 用期望状态对账实际状态，使 ASW 包含 DSW 中的卷。
func reconcileASW(asw cache.ActualStateOfWorld, dsw cache.DesiredStateOfWorld, t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		err := asw.MarkVolumeAsAttached(logger, volumeToMount.VolumeName, volumeToMount.VolumeSpec, "", "")
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsAttached: %v", err)
		}
		markVolumeOpts := operationexecutor.MarkVolumeOpts{
			PodName:          volumeToMount.PodName,
			PodUID:           volumeToMount.Pod.UID,
			VolumeName:       volumeToMount.VolumeName,
			VolumeGIDVolume:  volumeToMount.VolumeGIDValue,
			VolumeSpec:       volumeToMount.VolumeSpec,
			VolumeMountState: operationexecutor.VolumeMounted,
		}
		err = asw.MarkVolumeAsMounted(markVolumeOpts)
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsMounted: %v", err)
		}
	}
}

// clearASW 通过移除 DSW 中的卷并对账，清空 ASW。
func clearASW(asw cache.ActualStateOfWorld, dsw cache.DesiredStateOfWorld, t *testing.T) {
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		err := asw.MarkVolumeAsUnmounted(volumeToMount.PodName, volumeToMount.VolumeName)
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsUnmounted: %v", err)
		}
	}
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		asw.MarkVolumeAsDetached(volumeToMount.VolumeName, "")
	}
}

// reprocess 将指定 pod 标记为需要重新处理，供测试触发再次同步。
func reprocess(ctx context.Context, dswp *desiredStateOfWorldPopulator, uniquePodName types.UniquePodName,
	dsw cache.DesiredStateOfWorld, asw cache.ActualStateOfWorld, newSize resource.Quantity) []v1.UniqueVolumeName {
	logger := klog.FromContext(ctx)
	dswp.ReprocessPod(uniquePodName)
	dswp.findAndAddNewPods(ctx)
	return getResizeRequiredVolumes(logger, dsw, asw, newSize)
}

// getResizeRequiredVolumes 返回已挂载且容量小于 newSize、需要扩容的卷列表。
func getResizeRequiredVolumes(logger klog.Logger, dsw cache.DesiredStateOfWorld, asw cache.ActualStateOfWorld, newSize resource.Quantity) []v1.UniqueVolumeName {
	resizeRequiredVolumes := []v1.UniqueVolumeName{}
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		_, _, err := asw.PodExistsInVolume(logger, volumeToMount.PodName, volumeToMount.VolumeName, newSize, "" /* SELinuxContext */)
		if cache.IsFSResizeRequiredError(err) {
			resizeRequiredVolumes = append(resizeRequiredVolumes, volumeToMount.VolumeName)
		}
	}
	return resizeRequiredVolumes
}

// verifyVolumeExistsInVolumesToMount 断言期望状态中存在指定卷，且 reportedInUse 符合预期。
func verifyVolumeExistsInVolumesToMount(t *testing.T, expectedVolumeName v1.UniqueVolumeName, expectReportedInUse bool, dsw cache.DesiredStateOfWorld) {
	volumesToMount := dsw.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			if volume.ReportedInUse != expectReportedInUse {
				t.Fatalf(
					"Found volume %v in the list of VolumesToMount, but ReportedInUse incorrect. Expected: <%v> Actual: <%v>",
					expectedVolumeName,
					expectReportedInUse,
					volume.ReportedInUse)
			}
			return
		}
	}

	t.Fatalf(
		"Could not find volume %v in the list of desired state of world volumes to mount %+v",
		expectedVolumeName,
		volumesToMount)
}

// createPodWithVolume 构造引用指定 PV/PVC 的 pod，容器使用给定的挂载配置。
func createPodWithVolume(pod, pv, pvc string, containers []v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod,
			UID:       kubetypes.UID(pod + "-uid"),
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: pv,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "dswp-test-fake-device",
						},
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc,
						},
					},
				},
			},
			Containers: containers,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}
}

// createEphemeralVolumeObjects 构造临时卷场景下的 pod、PV 与 PVC；owned 表示 PV 是否标记为归 pod 所有。
func createEphemeralVolumeObjects(podName, volumeName string, owned bool, volumeMode *v1.PersistentVolumeMode) (pod *v1.Pod, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) {
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			UID:       "dswp-test-pod-uid",
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						Ephemeral: &v1.EphemeralVolumeSource{
							VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{},
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/mnt",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			Capacity:   volumeCapacity(1),
			VolumeMode: volumeMode,
		},
	}
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName + "-" + volumeName,
			Namespace: pod.Namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: volumeName,
			Resources: v1.VolumeResourceRequirements{
				Requests: pv.Spec.Capacity,
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: pv.Spec.Capacity,
		},
	}
	if owned {
		controller := true
		pvc.OwnerReferences = []metav1.OwnerReference{
			{
				UID:        pod.UID,
				Name:       podName,
				Controller: &controller,
			},
		}
	}
	return
}

// createDswpWithVolume 使用给定 PV/PVC 构造 desiredStateOfWorldPopulator 及 pod manager、期望状态缓存、fake runtime 与 pod 状态提供者。
func createDswpWithVolume(t *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) (*desiredStateOfWorldPopulator, kubepod.Manager, cache.DesiredStateOfWorld, *containertest.FakeRuntime, *fakePodStateProvider) {
	fakeVolumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)
	return dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider
}

// fakePodStateProvider 是用于测试的 pod 状态提供者，可显式控制终止与移除判定结果。
type fakePodStateProvider struct {
	terminating map[kubetypes.UID]struct{}
	removed     map[kubetypes.UID]struct{}
}

// ShouldPodContainersBeTerminating 返回该 pod 的容器是否应处于终止中。
func (p *fakePodStateProvider) ShouldPodContainersBeTerminating(uid kubetypes.UID) bool {
	_, ok := p.terminating[uid]
	// 若 ShouldPodRuntimeBeRemoved 返回 true，则 ShouldPodContainerBeTerminating 也应返回 true
	if !ok {
		_, ok = p.removed[uid]
	}
	return ok
}

// ShouldPodRuntimeBeRemoved 返回该 pod 的运行时资源是否应被移除。
func (p *fakePodStateProvider) ShouldPodRuntimeBeRemoved(uid kubetypes.UID) bool {
	_, ok := p.removed[uid]
	return ok
}

// createDswpWithVolumeWithCustomPluginMgr 使用自定义卷插件管理器构造 desiredStateOfWorldPopulator。
func createDswpWithVolumeWithCustomPluginMgr(pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim,
	fakeVolumePluginMgr *volume.VolumePluginMgr) (*desiredStateOfWorldPopulator, kubepod.Manager, cache.DesiredStateOfWorld, *containertest.FakeRuntime, *fakePodStateProvider) {
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, pvc, nil
	})
	fakeClient.AddReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		return true, pv, nil
	})

	fakePodManager := kubepod.NewBasicPodManager()

	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	fakesDSW := cache.NewDesiredStateOfWorld(fakeVolumePluginMgr, seLinuxTranslator)
	fakeASW := cache.NewActualStateOfWorld("fake", fakeVolumePluginMgr)
	fakeRuntime := &containertest.FakeRuntime{}
	fakeStateProvider := &fakePodStateProvider{}

	csiTranslator := csitrans.New()
	dswp := &desiredStateOfWorldPopulator{
		kubeClient:          fakeClient,
		loopSleepDuration:   100 * time.Millisecond,
		podManager:          fakePodManager,
		podStateProvider:    fakeStateProvider,
		desiredStateOfWorld: fakesDSW,
		actualStateOfWorld:  fakeASW,
		pods: processedPods{
			processedPods: make(map[types.UniquePodName]bool)},
		csiMigratedPluginManager: csimigration.NewPluginManager(csiTranslator),
		intreeToCSITranslator:    csiTranslator,
		volumePluginMgr:          fakeVolumePluginMgr,
	}
	return dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider
}
