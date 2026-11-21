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

package kubelet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	core "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestListVolumesForPod 校验 ListVolumesForPod 按 Pod 返回已挂载的卷。
func TestListVolumesForPod(t *testing.T) {
	// 卷管理器的挂载流程依赖异步 reconciler，短模式下跳过以节省时间。
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	pod := podWithUIDNameNsSpec("12345678", "foo", "test", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "container1",
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
					{
						Name:      "vol2",
						MountPath: "/mnt/vol2",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "vol1",
				VolumeSource: v1.VolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
						PDName: "fake-device1",
					},
				},
			},
			{
				Name: "vol2",
				VolumeSource: v1.VolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
						PDName: "fake-device2",
					},
				},
			},
		},
	})

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	// 启动卷管理器主循环，模拟真实 kubelet 的异步挂载过程。
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods([]*v1.Pod{pod})
	err := kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod)
	assert.NoError(t, err)

	podName := util.GetUniquePodName(pod)

	volumesToReturn, volumeExsit := kubelet.ListVolumesForPod(types.UID(podName))
	assert.True(t, volumeExsit, "expected to find volumes for pod %q", podName)

	outerVolumeSpecName1 := "vol1"
	assert.NotNil(t, volumesToReturn[outerVolumeSpecName1], "key %s", outerVolumeSpecName1)

	outerVolumeSpecName2 := "vol2"
	assert.NotNil(t, volumesToReturn[outerVolumeSpecName2], "key %s", outerVolumeSpecName2)
}

// TestPodVolumesExist 校验 podVolumesExist 能识别多个 Pod 的卷是否均已挂载。
func TestPodVolumesExist(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod1",
				UID:  "pod1uid",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container1",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol1",
								MountPath: "/mnt/vol1",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "vol1",
						VolumeSource: v1.VolumeSource{
							RBD: &v1.RBDVolumeSource{
								RBDImage: "fake1",
							},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod2",
				UID:  "pod2uid",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container2",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol2",
								MountPath: "/mnt/vol2",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "vol2",
						VolumeSource: v1.VolumeSource{
							RBD: &v1.RBDVolumeSource{
								RBDImage: "fake2",
							},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod3",
				UID:  "pod3uid",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container3",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol3",
								MountPath: "/mnt/vol3",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "vol3",
						VolumeSource: v1.VolumeSource{
							RBD: &v1.RBDVolumeSource{
								RBDImage: "fake3",
							},
						},
					},
				},
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")

	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods(pods)
	for _, pod := range pods {
		err := kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod)
		assert.NoError(t, err)
	}

	for _, pod := range pods {
		podVolumesExist := kubelet.podVolumesExist(tCtx.Logger(), pod.UID)
		assert.True(t, podVolumesExist, "pod %q", pod.UID)
	}
}

// TestPodVolumeDeadlineAttachAndMount 校验 WaitForAttachAndMount 会响应上下文超时，
// 而不是一直等到内部默认超时。
func TestPodVolumeDeadlineAttachAndMount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubeletWithImageList(t, nil /*imageList*/, false, /* controllerAttachDetachEnabled */
		false /*initFakeVolumePlugin*/, true /*localStorageCapacityIsolation*/, false /*excludePodAdmitHandlers*/, false /*enableResizing*/)

	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	// 这里新增的用例必须使用无法完成挂载的卷，才能触发等待超时。
	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod1",
				UID:  "pod1uid",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container1",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol1",
								MountPath: "/mnt/vol1",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "vol1",
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: "non-existent",
							},
						},
					},
				},
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods(pods)
	for _, pod := range pods {
		start := time.Now()
		// 给上下文设置很短的截止时间，以便快速观察超时行为。
		ctx, cancel := context.WithDeadline(tCtx, time.Now().Add(2*time.Second))
		err := kubelet.volumeManager.WaitForAttachAndMount(ctx, pod)
		delta := time.Since(start)
		// 标准超时为 2 分钟；若几秒内就返回，说明是上下文超时而非内部默认超时导致。
		assert.Lessf(t, delta, 10*time.Second, "WaitForAttachAndMount should timeout when the context is cancelled")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		cancel()
	}
}

// TestPodVolumeDeadlineUnmount 校验 WaitForUnmount 会响应上下文超时。
func TestPodVolumeDeadlineUnmount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubeletWithImageList(t, nil /*imageList*/, false, /* controllerAttachDetachEnabled */
		true /*initFakeVolumePlugin*/, true /*localStorageCapacityIsolation*/, false /*excludePodAdmitHandlers*/, false /*enableResizing*/)

	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	// 这里新增的用例必须使用能够成功挂载的卷，才能验证卸载超时。
	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod1",
				UID:  "pod1uid",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container1",
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "vol1",
								MountPath: "/mnt/vol1",
							},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "vol1",
						VolumeSource: v1.VolumeSource{
							RBD: &v1.RBDVolumeSource{
								RBDImage: "fake-device",
							},
						},
					},
				},
			},
		},
	}

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods(pods)
	for i, pod := range pods {
		if err := kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod); err != nil {
			t.Fatalf("pod %d failed: %v", i, err)
		}
		start := time.Now()
		// 给上下文设置很短的截止时间，以便快速观察超时行为。
		ctx, cancel := context.WithDeadline(tCtx, time.Now().Add(2*time.Second))
		err := kubelet.volumeManager.WaitForUnmount(ctx, pod)
		delta := time.Since(start)
		// 标准超时为 2 分钟；若几秒内就返回，说明是上下文超时而非内部默认超时导致。
		assert.Lessf(t, delta, 10*time.Second, "WaitForUnmount should timeout when the context is cancelled")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		cancel()
	}
}

// TestVolumeAttachAndMountControllerDisabled 校验关闭控制器 attach/detach 时，
// kubelet 自己完成 attach、mount device 与 setup。
func TestVolumeAttachAndMountControllerDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	pod := podWithUIDNameNsSpec("12345678", "foo", "test", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "container1",
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "vol1",
				VolumeSource: v1.VolumeSource{
					RBD: &v1.RBDVolumeSource{
						RBDImage: "fake",
					},
				},
			},
		},
	})

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods([]*v1.Pod{pod})
	err := kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod)
	assert.NoError(t, err)

	podVolumes := kubelet.volumeManager.GetMountedVolumesForPod(
		util.GetUniquePodName(pod))

	// 只断言卷名集合，不依赖卷对象的具体实现。
	expectedPodVolumes := []string{"vol1"}
	assert.Len(t, podVolumes, len(expectedPodVolumes), "Volumes for pod %+v", pod)
	for _, name := range expectedPodVolumes {
		assert.Contains(t, podVolumes, name, "Volumes for pod %+v", pod)
	}
	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, testKubelet.volumePlugin))
}

// TestVolumeUnmountAndDetachControllerDisabled 校验关闭控制器 attach/detach 时，
// kubelet 自己完成 teardown、unmount 与 detach。
func TestVolumeUnmountAndDetachControllerDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	pod := podWithUIDNameNsSpec("12345678", "foo", "test", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "container1",
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "vol1",
				VolumeSource: v1.VolumeSource{
					RBD: &v1.RBDVolumeSource{
						RBDImage: "fake-device",
					},
				},
			},
		},
	})

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	// 加入 Pod。
	kubelet.podManager.SetPods([]*v1.Pod{pod})

	// 确认卷已完成 attach/mount。
	err := kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod)
	assert.NoError(t, err)

	podVolumes := kubelet.volumeManager.GetMountedVolumesForPod(
		util.GetUniquePodName(pod))

	expectedPodVolumes := []string{"vol1"}
	assert.Len(t, podVolumes, len(expectedPodVolumes), "Volumes for pod %+v", pod)
	for _, name := range expectedPodVolumes {
		assert.Contains(t, podVolumes, name, "Volumes for pod %+v", pod)
	}

	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyAttachCallCount(
		1 /* expectedAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, testKubelet.volumePlugin))

	// 移除 Pod。
	// TODO: technically waitForVolumeUnmount
	kubelet.podWorkers.(*fakePodWorkers).setPodRuntimeBeRemoved(pod.UID)
	kubelet.podManager.SetPods([]*v1.Pod{})

	require.NoError(t, kubelet.volumeManager.WaitForUnmount(tCtx, pod))

	// 确认卷已卸载。
	hasMountedVolumes := kubelet.volumeManager.HasPossiblyMountedVolumesForPod(
		util.GetUniquePodName(pod))

	assert.False(t, hasMountedVolumes, "Expected volumes to be unmounted and detached. But some volumes are still mounted")

	assert.NoError(t, volumetest.VerifyTearDownCallCount(
		1 /* expectedTearDownCallCount */, testKubelet.volumePlugin))

	// 确认卷已 detach，且不再出现在“在用卷”列表中。
	assert.NoError(t, waitForVolumeDetach(v1.UniqueVolumeName("fake/fake-device"), kubelet.volumeManager))
	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyDetachCallCount(
		1 /* expectedDetachCallCount */, testKubelet.volumePlugin))
}

// TestVolumeAttachAndMountControllerEnabled 校验开启控制器 attach/detach 时，
// kubelet 只做 mount，不调用 attach。
func TestVolumeAttachAndMountControllerEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, true /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	kubeClient := testKubelet.fakeKubeClient
	kubeClient.AddReactor("get", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			return true, &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname},
				Status: v1.NodeStatus{
					VolumesAttached: []v1.AttachedVolume{
						{
							Name:       "fake/fake-device",
							DevicePath: "fake/path",
						},
					}},
			}, nil
		})
	kubeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("no reaction implemented for %s", action)
	})

	pod := podWithUIDNameNsSpec("12345678", "foo", "test", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "container1",
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "vol1",
				VolumeSource: v1.VolumeSource{
					RBD: &v1.RBDVolumeSource{
						RBDImage: "fake-device",
					},
				},
			},
		},
	})

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	kubelet.podManager.SetPods([]*v1.Pod{pod})

	// 模拟节点状态中的 VolumesInUse 更新。
	go simulateVolumeInUseUpdate(
		v1.UniqueVolumeName("fake/fake-device"),
		tCtx.Done(),
		kubelet.volumeManager)

	require.NoError(t, kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod))

	podVolumes := kubelet.volumeManager.GetMountedVolumesForPod(
		util.GetUniquePodName(pod))
	hasVolumes := kubelet.volumeManager.HasPossiblyMountedVolumesForPod(
		util.GetUniquePodName(pod))
	assert.True(t, hasVolumes, "HasPossiblyMountedVolumesForPod should return true")

	expectedPodVolumes := []string{"vol1"}
	assert.Len(t, podVolumes, len(expectedPodVolumes), "Volumes for pod %+v", pod)
	for _, name := range expectedPodVolumes {
		assert.Contains(t, podVolumes, name, "Volumes for pod %+v", pod)
	}
	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyZeroAttachCalls(testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, testKubelet.volumePlugin))
}

// TestVolumeUnmountAndDetachControllerEnabled 校验开启控制器 attach/detach 时，
// kubelet 只做 unmount，不调用 detach。
func TestVolumeUnmountAndDetachControllerEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testKubelet := newTestKubelet(t, true /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	kubeClient := testKubelet.fakeKubeClient
	kubeClient.AddReactor("get", "nodes",
		func(action core.Action) (bool, runtime.Object, error) {
			return true, &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname},
				Status: v1.NodeStatus{
					VolumesAttached: []v1.AttachedVolume{
						{
							Name:       "fake/fake-device",
							DevicePath: "fake/path",
						},
					}},
			}, nil
		})
	kubeClient.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("no reaction implemented for %s", action)
	})

	pod := podWithUIDNameNsSpec("12345678", "foo", "test", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "container1",
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "vol1",
						MountPath: "/mnt/vol1",
					},
				},
			},
		},
		Volumes: []v1.Volume{
			{
				Name: "vol1",
				VolumeSource: v1.VolumeSource{
					RBD: &v1.RBDVolumeSource{
						RBDImage: "fake-device",
					},
				},
			},
		},
	})

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	go kubelet.volumeManager.Run(tCtx, kubelet.sourcesReady)

	// 加入 Pod。
	kubelet.podManager.SetPods([]*v1.Pod{pod})

	// 模拟节点状态中的 VolumesInUse 更新。
	go simulateVolumeInUseUpdate(
		v1.UniqueVolumeName("fake/fake-device"),
		tCtx.Done(),
		kubelet.volumeManager)

	// 确认卷已完成 attach/mount。
	require.NoError(t, kubelet.volumeManager.WaitForAttachAndMount(tCtx, pod))

	podVolumes := kubelet.volumeManager.GetMountedVolumesForPod(
		util.GetUniquePodName(pod))
	hasVolumes := kubelet.volumeManager.HasPossiblyMountedVolumesForPod(
		util.GetUniquePodName(pod))
	assert.True(t, hasVolumes, "HasPossiblyMountedVolumesForPod should return true")

	expectedPodVolumes := []string{"vol1"}
	assert.Len(t, podVolumes, len(expectedPodVolumes), "Volumes for pod %+v", pod)
	for _, name := range expectedPodVolumes {
		assert.Contains(t, podVolumes, name, "Volumes for pod %+v", pod)
	}

	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyWaitForAttachCallCount(
		1 /* expectedWaitForAttachCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyZeroAttachCalls(testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifyMountDeviceCallCount(
		1 /* expectedMountDeviceCallCount */, testKubelet.volumePlugin))
	assert.NoError(t, volumetest.VerifySetUpCallCount(
		1 /* expectedSetUpCallCount */, testKubelet.volumePlugin))

	// 移除 Pod。
	kubelet.podWorkers.(*fakePodWorkers).setPodRuntimeBeRemoved(pod.UID)
	kubelet.podManager.SetPods([]*v1.Pod{})

	assert.NoError(t, waitForVolumeUnmount(kubelet.volumeManager, pod))

	// 确认卷已卸载。
	podVolumes = kubelet.volumeManager.GetMountedVolumesForPod(
		util.GetUniquePodName(pod))
	hasVolumes = kubelet.volumeManager.HasPossiblyMountedVolumesForPod(
		util.GetUniquePodName(pod))
	assert.False(t, hasVolumes, "HasPossiblyMountedVolumesForPod should return false")

	assert.Empty(t, podVolumes,
		"Expected volumes to be unmounted and detached. But some volumes are still mounted")

	assert.NoError(t, volumetest.VerifyTearDownCallCount(
		1 /* expectedTearDownCallCount */, testKubelet.volumePlugin))

	// 确认卷已 detach，且不再出现在“在用卷”列表中。
	assert.NoError(t, waitForVolumeDetach(v1.UniqueVolumeName("fake/fake-device"), kubelet.volumeManager))
	assert.GreaterOrEqual(t, testKubelet.volumePlugin.GetNewAttacherCallCount(), 1, "Expected plugin NewAttacher to be called at least once")
	assert.NoError(t, volumetest.VerifyZeroDetachCallCount(testKubelet.volumePlugin))
}

// stubVolume 是 volume.Volume 的最小桩实现，用于直接驱动挂载/卸载逻辑。
type stubVolume struct {
	path       string
	attributes volume.Attributes
	volume.MetricsNil
}

// GetPath 返回桩卷的路径。
func (f *stubVolume) GetPath() string {
	return f.path
}

// GetAttributes 返回桩卷的属性。
func (f *stubVolume) GetAttributes() volume.Attributes {
	return f.attributes
}

// SetUp 为空实现，始终成功。
func (f *stubVolume) SetUp(mounterArgs volume.MounterArgs) error {
	return nil
}

// SetUpAt 为空实现，始终成功。
func (f *stubVolume) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	return nil
}

// stubBlockVolume 是块设备卷映射器的最小桩实现。
type stubBlockVolume struct {
	dirPath string
	volName string
}

// GetGlobalMapPath 返回空的全局映射路径。
func (f *stubBlockVolume) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	return "", nil
}

// GetPodDeviceMapPath 返回预置的目录与卷名。
func (f *stubBlockVolume) GetPodDeviceMapPath() (string, string) {
	return f.dirPath, f.volName
}

// SetUpDevice 为空实现，始终成功。
func (f *stubBlockVolume) SetUpDevice() (string, error) {
	return "", nil
}

// MapPodDevice 为空实现，始终成功。
func (f stubBlockVolume) MapPodDevice() error {
	return nil
}

// TearDownDevice 为空实现，始终成功。
func (f *stubBlockVolume) TearDownDevice(mapPath string, devicePath string) error {
	return nil
}

// UnmapPodDevice 为空实现，始终成功。
func (f *stubBlockVolume) UnmapPodDevice() error {
	return nil
}

// SupportsMetrics 返回 false，表示桩卷不提供指标。
func (f *stubBlockVolume) SupportsMetrics() bool {
	return false
}

// GetMetrics 返回 nil，nil。
func (f *stubBlockVolume) GetMetrics() (*volume.Metrics, error) {
	return nil, nil
}
