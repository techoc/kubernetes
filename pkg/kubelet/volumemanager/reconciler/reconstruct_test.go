/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
)

// TestReconstructVolumes 验证 kubelet 重启后从磁盘目录重建卷状态的行为：
// 重建成功的卷标记为不确定但可用于健康探测，重建失败的卷则被清理。
func TestReconstructVolumes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name                                string
		volumePaths                         []string
		expectedVolumesNeedDevicePath       []string
		expectedVolumesFailedReconstruction []string
		verifyFunc                          func(rcInstance *reconciler, fakePlugin *volumetesting.FakeVolumePlugin) error
	}{
		{
			name: "when two pods are using same volume and both are deleted",
			volumePaths: []string{
				filepath.Join("pod1", "volumes", "fake-plugin", "pvc-abcdef"),
				filepath.Join("pod2", "volumes", "fake-plugin", "pvc-abcdef"),
			},
			expectedVolumesNeedDevicePath:       []string{"fake-plugin/pvc-abcdef", "fake-plugin/pvc-abcdef"},
			expectedVolumesFailedReconstruction: []string{},
			verifyFunc: func(rcInstance *reconciler, fakePlugin *volumetesting.FakeVolumePlugin) error {
				mountedPods := rcInstance.actualStateOfWorld.GetMountedVolumes()
				if len(mountedPods) != 0 {
					return fmt.Errorf("expected 0 certain pods in asw got %d", len(mountedPods))
				}
				allPods := rcInstance.actualStateOfWorld.GetAllMountedVolumes()
				if len(allPods) != 2 {
					return fmt.Errorf("expected 2 uncertain pods in asw got %d", len(allPods))
				}
				volumes := rcInstance.actualStateOfWorld.GetPossiblyMountedVolumesForPod("pod1")
				if len(volumes) != 1 {
					return fmt.Errorf("expected 1 uncertain volume in asw got %d", len(volumes))
				}
				// 卷应在 ASW（实际状态世界）中被标记为已重建。
				if reconstructed := rcInstance.actualStateOfWorld.IsVolumeReconstructed("fake-plugin/pvc-abcdef", "pod1"); !reconstructed {
					t.Errorf("expected volume to be marked as reconstructed, got %v", reconstructed)
				}
				// 重建出来的卷必须可参与健康探测，否则无法发现挂载异常。
				if !rcInstance.actualStateOfWorld.IsVolumeMountAttempted("pod1", "fake-plugin/pvc-abcdef") {
					t.Errorf("expected reconstructed volume to be marked as mount-attempted for health probing")
				}
				return nil
			},
		},
		{
			name: "when reconstruction fails for a volume, volumes should be cleaned up",
			volumePaths: []string{
				filepath.Join("pod1", "volumes", "missing-plugin", "pvc-abcdef"),
			},
			expectedVolumesNeedDevicePath:       []string{},
			expectedVolumesFailedReconstruction: []string{"pvc-abcdef"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpKubeletDir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatalf("can't make a temp directory for kubeletPods: %v", err)
			}
			defer os.RemoveAll(tmpKubeletDir)

			// 创建 kubelet 的 Pod 目录。
			tmpKubeletPodDir := filepath.Join(tmpKubeletDir, "pods")
			os.MkdirAll(tmpKubeletPodDir, 0755)

			mountPaths := []string{}

			// 预先创建 Pod 与卷目录，模拟 kubelet 重启前遗留的挂载目录。
			for _, volumePath := range tc.volumePaths {
				vp := filepath.Join(tmpKubeletPodDir, volumePath)
				mountPaths = append(mountPaths, vp)
				os.MkdirAll(vp, 0755)
			}

			rc, fakePlugin := getReconciler(tmpKubeletDir, t, mountPaths, nil /*custom kubeclient*/)
			rcInstance, _ := rc.(*reconciler)

			// 执行被测的重建逻辑。
			rcInstance.reconstructVolumes(logger)

			// 断言重建结果。
			// 转换为 []UniqueVolumeName 以便比较。
			expectedVolumes := make([]v1.UniqueVolumeName, len(tc.expectedVolumesNeedDevicePath))
			for i := range tc.expectedVolumesNeedDevicePath {
				expectedVolumes[i] = v1.UniqueVolumeName(tc.expectedVolumesNeedDevicePath[i])
			}
			if !reflect.DeepEqual(expectedVolumes, rcInstance.volumesNeedUpdateFromNodeStatus) {
				t.Errorf("Expected expectedVolumesNeedDevicePath:\n%v\n got:\n%v", expectedVolumes, rcInstance.volumesNeedUpdateFromNodeStatus)
			}

			// 汇总重建失败的卷名，与期望列表比较（忽略顺序）。
			volumesFailedReconstruction := sets.New[string]()
			for _, vol := range rcInstance.volumesFailedReconstruction {
				volumesFailedReconstruction.Insert(vol.volumeSpecName)
			}
			if !reflect.DeepEqual(sets.List(volumesFailedReconstruction), tc.expectedVolumesFailedReconstruction) {
				t.Errorf("Expected volumesFailedReconstruction:\n%v\n got:\n%v", tc.expectedVolumesFailedReconstruction, sets.List(volumesFailedReconstruction))
			}

			if tc.verifyFunc != nil {
				if err := tc.verifyFunc(rcInstance, fakePlugin); err != nil {
					t.Errorf("Test %s failed: %v", tc.name, err)
				}
			}
		})
	}
}

// TestCleanOrphanVolumes 验证 cleanOrphanVolumes 仅清理不在期望状态中的
// 重建失败卷，仍在使用的卷必须保留。
func TestCleanOrphanVolumes(t *testing.T) {
	type podInfo struct {
		podName         string
		podUID          string
		outerVolumeName string
		innerVolumeName string
	}
	defaultPodInfo := podInfo{
		podName:         "pod1",
		podUID:          "pod1uid",
		outerVolumeName: "volume-name",
		innerVolumeName: "volume-name",
	}
	defaultVolume := podVolume{
		podName:        "pod1uid",
		volumeSpecName: "volume-name",
		volumePath:     "",
		pluginName:     "fake-plugin",
		volumeMode:     v1.PersistentVolumeFilesystem,
	}

	tests := []struct {
		name                        string
		podInfos                    []podInfo
		volumesFailedReconstruction []podVolume
		expectedUnmounts            int
	}{
		{
			name:                        "volume is in DSW and is not cleaned",
			podInfos:                    []podInfo{defaultPodInfo},
			volumesFailedReconstruction: []podVolume{defaultVolume},
			expectedUnmounts:            0,
		},
		{
			name:                        "volume is not in DSW and is cleaned",
			podInfos:                    []podInfo{},
			volumesFailedReconstruction: []podVolume{defaultVolume},
			expectedUnmounts:            1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 准备测试环境：临时 kubelet 目录与 fake 卷插件。
			tmpKubeletDir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatalf("can't make a temp directory for kubeletPods: %v", err)
			}
			defer os.RemoveAll(tmpKubeletDir)

			// 创建 kubelet 的 Pod 目录。
			tmpKubeletPodDir := filepath.Join(tmpKubeletDir, "pods")
			os.MkdirAll(tmpKubeletPodDir, 0755)

			mountPaths := []string{}

			rc, fakePlugin := getReconciler(tmpKubeletDir, t, mountPaths, nil /*custom kubeclient*/)
			rcInstance, _ := rc.(*reconciler)
			rcInstance.volumesFailedReconstruction = tc.volumesFailedReconstruction
			logger, _ := ktesting.NewTestContext(t)
			// 按用例配置把 Pod 与卷加入期望状态与实际状态。
			for _, tpodInfo := range tc.podInfos {
				pod := getInlineFakePod(tpodInfo.podName, tpodInfo.podUID, tpodInfo.outerVolumeName, tpodInfo.innerVolumeName)
				volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
				podName := util.GetUniquePodName(pod)
				volumeName, err := rcInstance.desiredStateOfWorld.AddPodToVolume(
					logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* SELinuxContext */)
				if err != nil {
					t.Fatalf("Error adding volume %s to dsow: %v", volumeSpec.Name(), err)
				}
				rcInstance.actualStateOfWorld.MarkVolumeAsAttached(logger, volumeName, volumeSpec, nodeName, "")
			}

			// 执行孤儿卷清理。
			rcInstance.cleanOrphanVolumes(logger)

			// 断言重建失败列表已清空，并等待异步卸载完成。
			if len(rcInstance.volumesFailedReconstruction) != 0 {
				t.Errorf("Expected volumesFailedReconstruction to be empty, got %+v", rcInstance.volumesFailedReconstruction)
			}
			// 卸载在 goroutine 中异步执行，需要轮询等待其完成。
			var lastErr error
			err = retryWithExponentialBackOff(testOperationBackOffDuration, func() (bool, error) {
				if err := verifyTearDownCalls(fakePlugin, tc.expectedUnmounts); err != nil {
					lastErr = err
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				t.Errorf("Error waiting for volumes to get unmounted: %s: %s", err, lastErr)
			}
		})
	}
}

// verifyTearDownCalls 校验 fake 插件上的 TearDown 调用次数是否符合预期。
func verifyTearDownCalls(plugin *volumetesting.FakeVolumePlugin, expected int) error {
	unmounters := plugin.GetUnmounters()
	if len(unmounters) == 0 && (expected == 0) {
		return nil
	}
	actualCallCount := 0
	for _, unmounter := range unmounters {
		actualCallCount = unmounter.GetTearDownCallCount()
		if actualCallCount == expected {
			return nil
		}
	}
	return fmt.Errorf("expected TearDown calls %d, got %d", expected, actualCallCount)
}

// TestReconstructVolumesMount 验证卷重建后的挂载流程，包括挂载成功、
// SetUp 失败与设备挂载失败三种情况。
func TestReconstructVolumesMount(t *testing.T) {
	// 本测试覆盖“卷重建 + 后续挂载失败”的组合场景。
	// 由于卷是重建出来的，即便最终 SetUp 出错也必须保持不确定状态，
	// 参见 https://github.com/kubernetes/kubernetes/issues/96635
	// 与 https://github.com/kubernetes/kubernetes/pull/110670。
	logger, ctx := ktesting.NewTestContext(t)

	tests := []struct {
		name            string
		volumePath      string
		expectMount     bool
		volumeMode      v1.PersistentVolumeMode
		deviceMountPath string
	}{
		{
			name:       "reconstructed volume is mounted",
			volumePath: filepath.Join("pod1uid", "volumes", "fake-plugin", "volumename"),

			expectMount: true,
			volumeMode:  v1.PersistentVolumeFilesystem,
		},
		{
			name: "reconstructed volume fails to mount",
			// FailOnSetupVolumeName：MountDevice 成功，但 SetUp 失败。
			volumePath:  filepath.Join("pod1uid", "volumes", "fake-plugin", volumetesting.FailOnSetupVolumeName),
			expectMount: false,
			volumeMode:  v1.PersistentVolumeFilesystem,
		},
		{
			name:            "reconstructed volume device map fails",
			volumePath:      filepath.Join("pod1uid", "volumeDevices", "fake-plugin", volumetesting.FailMountDeviceVolumeName),
			volumeMode:      v1.PersistentVolumeBlock,
			deviceMountPath: filepath.Join("plugins", "fake-plugin", "volumeDevices", "pluginDependentPath"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpKubeletDir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatalf("can't make a temp directory for kubeletPods: %v", err)
			}
			defer os.RemoveAll(tmpKubeletDir)

			// 创建 kubelet 的 Pod 目录。
			tmpKubeletPodDir := filepath.Join(tmpKubeletDir, "pods")
			os.MkdirAll(tmpKubeletPodDir, 0755)

			// 预先创建 Pod 与卷目录，模拟重启前遗留的挂载目录。
			vp := filepath.Join(tmpKubeletPodDir, tc.volumePath)
			mountPaths := []string{vp}
			os.MkdirAll(vp, 0755)

			// 第二步准备：向期望状态世界（DSW）填充 Pod 与卷。
			outerName := filepath.Base(tc.volumePath)
			pod, pv, pvc := getPodPVCAndPV(tc.volumeMode, "pod1", outerName, "pvc1")
			volumeSpec := &volume.Spec{PersistentVolume: pv}
			kubeClient := createtestClientWithPVPVC(pv, pvc, v1.AttachedVolume{
				Name:       v1.UniqueVolumeName(fmt.Sprintf("fake-plugin/%s", outerName)),
				DevicePath: "fake/path",
			})

			rc, fakePlugin := getReconciler(tmpKubeletDir, t, mountPaths, kubeClient /*custom kubeclient*/)
			rcInstance, _ := rc.(*reconciler)

			// 第一步：执行卷重建。
			rcInstance.reconstructVolumes(logger)

			// 断言一：重建后的卷处于不确定状态。
			mountedPods := rcInstance.actualStateOfWorld.GetMountedVolumes()
			if len(mountedPods) != 0 {
				t.Errorf("expected 0 mounted volumes, got %+v", mountedPods)
			}
			allPods := rcInstance.actualStateOfWorld.GetAllMountedVolumes()
			if len(allPods) != 1 {
				t.Errorf("expected 1 uncertain volume in asw, got %+v", allPods)
			}

			podName := util.GetUniquePodName(pod)
			volumeName, err := rcInstance.desiredStateOfWorld.AddPodToVolume(
				logger, podName, pod, volumeSpec, volumeSpec.Name(), "" /* volumeGIDValue */, nil /* SELinuxContext */)
			if err != nil {
				t.Fatalf("Error adding volume %s to dsow: %v", volumeSpec.Name(), err)
			}
			logger, _ := ktesting.NewTestContext(t)
			rcInstance.actualStateOfWorld.MarkVolumeAsAttached(logger, volumeName, volumeSpec, nodeName, "")

			rcInstance.populatorHasAddedPods = func() bool {
				// 将 DSW 标记为已填充，允许卷执行卸载流程。
				return true
			}
			// 将设备路径标记为已对账，允许卷执行卸载流程。
			rcInstance.volumesNeedUpdateFromNodeStatus = nil

			// 第二步：执行一次 reconcile。
			rcInstance.reconcile(ctx)

			// 断言二：确认 MountDevice 已被尝试调用。
			var lastErr error
			err = retryWithExponentialBackOff(testOperationBackOffDuration, func() (bool, error) {
				if tc.volumeMode == v1.PersistentVolumeFilesystem {
					if err := volumetesting.VerifyMountDeviceCallCount(1, fakePlugin); err != nil {
						lastErr = err
						return false, nil
					}
					return true, nil
				} else {
					return true, nil
				}
			})
			if err != nil {
				t.Errorf("Error waiting for volumes to get mounted: %s: %s", err, lastErr)
			}

			if tc.expectMount {
				// 期望卷完全挂载成功。
				waitForMount(t, fakePlugin, volumeName, rcInstance.actualStateOfWorld)
				// 确认 SetUp 被调用且成功。
				if err := volumetesting.VerifySetUpCallCount(1, fakePlugin); err != nil {
					t.Errorf("Expected SetUp() to be called, got %s", err)
				}
			} else {
				// 不期望 ASW 发生变化，但仍需等待卷操作结束，避免竞态。
				err = retryWithExponentialBackOff(testOperationBackOffDuration, func() (bool, error) {
					return !rcInstance.operationExecutor.IsOperationPending(volumeName, "pod1uid", nodeName), nil
				})
				if err != nil {
					t.Errorf("Error waiting for operation to get finished: %s", err)
				}
				// 卷应保持不确定状态。
				mountedPods := rcInstance.actualStateOfWorld.GetMountedVolumes()
				if len(mountedPods) != 0 {
					t.Errorf("expected 0 mounted volumes after reconcile, got %+v", mountedPods)
				}
				allPods := rcInstance.actualStateOfWorld.GetAllMountedVolumes()
				if len(allPods) != 1 {
					t.Errorf("expected 1 mounted or uncertain volumes after reconcile, got %+v", allPods)
				}
				if tc.deviceMountPath != "" {
					expectedDeviceMountPath := filepath.Join(tmpKubeletDir, tc.deviceMountPath)
					deviceMountPath := allPods[0].DeviceMountPath
					if expectedDeviceMountPath != deviceMountPath {
						t.Errorf("expected deviceMountPath to be %s, got %s", expectedDeviceMountPath, deviceMountPath)
					}
				}

			}

			// 无论哪种情况都不应尝试卸载。
			verifyTearDownCalls(fakePlugin, 0)
		})
	}
}

// getPodPVCAndPV 构造一组相互关联的 Pod、PV 与 PVC 测试对象。
func getPodPVCAndPV(volumeMode v1.PersistentVolumeMode, podName, pvName, pvcName string) (*v1.Pod, *v1.PersistentVolume, *v1.PersistentVolumeClaim) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			UID:  "pvuid",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Name: pvcName},
			VolumeMode: &volumeMode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
			UID:  "pvcuid",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
			VolumeMode: &volumeMode,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
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
	return pod, pv, pvc
}
