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

package cache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
)

// Test_MarkVolumeAsMountAttempted 验证挂载尝试标记的设置与查询。
func Test_MarkVolumeAsMountAttempted(t *testing.T) {
	volumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode", volumePluginMgr)

	podName := util.GetUniquePodName(&v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1uid"}})
	volumeName := v1.UniqueVolumeName("fake-plugin/vol1")

	// 标记前应为 false。
	if asw.IsVolumeMountAttempted(podName, volumeName) {
		t.Fatalf("expected IsVolumeMountAttempted false before MarkVolumeAsMountAttempted")
	}

	asw.MarkVolumeAsMountAttempted(podName, volumeName)

	// 标记后应为 true。
	if !asw.IsVolumeMountAttempted(podName, volumeName) {
		t.Fatalf("expected IsVolumeMountAttempted true after MarkVolumeAsMountAttempted")
	}
}

// Test_MarkVolumeAsMountAttempted_ClearedOnDeletePodFromVolume 验证从卷中删除 Pod 时
// 会清除挂载尝试标记。
func Test_MarkVolumeAsMountAttempted_ClearedOnDeletePodFromVolume(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	volumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	asw := NewActualStateOfWorld("mynode", volumePluginMgr)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			UID:  "pod1uid",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{{
				Name: "volume-name",
				VolumeSource: v1.VolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
						PDName: "fake-device1",
					},
				},
			}},
		},
	}
	volumeSpec := &volume.Spec{Volume: &pod.Spec.Volumes[0]}
	podName := util.GetUniquePodName(pod)
	generatedVolumeName, err := util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
	if err != nil {
		t.Fatalf("GetUniqueVolumeNameFromSpec failed: %v", err)
	}

	// 先把卷标记为已附加，才能关联 Pod 与卷。
	if err := asw.MarkVolumeAsAttached(logger, emptyVolumeName, volumeSpec, "", "fake/device/path"); err != nil {
		t.Fatalf("MarkVolumeAsAttached failed: %v", err)
	}

	// 标记挂载尝试后，查询应为 true。
	asw.MarkVolumeAsMountAttempted(podName, generatedVolumeName)
	if !asw.IsVolumeMountAttempted(podName, generatedVolumeName) {
		t.Fatalf("expected IsVolumeMountAttempted true after MarkVolumeAsMountAttempted")
	}

	if err := asw.DeletePodFromVolume(podName, generatedVolumeName); err != nil {
		t.Fatalf("DeletePodFromVolume failed: %v", err)
	}

	// 删除 Pod 后标记应被清除。
	if asw.IsVolumeMountAttempted(podName, generatedVolumeName) {
		t.Fatalf("expected IsVolumeMountAttempted false after DeletePodFromVolume")
	}
}

// Test_GetVolumesToReportHealth 验证只有存在于期望状态且尝试过挂载的卷
// 才会被上报健康状态。
func Test_GetVolumesToReportHealth(t *testing.T) {
	// 用例覆盖：已尝试挂载、不在 DSW、未尝试挂载三种情况。
	tests := []struct {
		name          string
		addToDSW      bool
		markAttempted bool
		wantCount     int
	}{
		{
			name:          "attempted but not mounted returns volume",
			addToDSW:      true,
			markAttempted: true,
			wantCount:     1,
		},
		{
			name:          "excludes volume not in DSW",
			addToDSW:      false,
			markAttempted: true,
			wantCount:     0,
		},
		{
			name:          "excludes non-attempted volume",
			addToDSW:      true,
			markAttempted: false,
			wantCount:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			volumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
			seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
			dsw := NewDesiredStateOfWorld(volumePluginMgr, seLinuxTranslator)
			asw := NewActualStateOfWorld("mynode", volumePluginMgr)

			pod, volumeSpec := newCSITestPodAndSpec("pod1", "pod1uid", "vol1", "csi-driver", "handle-1")
			podName := util.GetUniquePodName(pod)
			outerVolumeName := pod.Spec.Volumes[0].Name

			// 构造 DSW 与 ASW，并按用例决定是否把卷加入 DSW。
			var volumeName v1.UniqueVolumeName
			if tt.addToDSW {
				var err error
				volumeName, err = dsw.AddPodToVolume(
					logger, podName, pod, volumeSpec, outerVolumeName, "", nil)
				if err != nil {
					t.Fatalf("AddPodToVolume failed: %v", err)
				}
			} else {
				volumeName = v1.UniqueVolumeName("fake-plugin/vol1")
			}

			if tt.markAttempted {
				asw.MarkVolumeAsMountAttempted(podName, volumeName)
			}

			got := GetVolumesToReportHealth(dsw, asw)
			if len(got) != tt.wantCount {
				t.Fatalf("expected %d volumes, got %d", tt.wantCount, len(got))
			}

			// 返回的卷信息应包含驱动名、句柄与外层卷名，且挂载路径为空。
			if tt.wantCount > 0 {
				if got[0].VolumeName != volumeName {
					t.Errorf("VolumeName: got %q, want %q", got[0].VolumeName, volumeName)
				}
				if got[0].DriverName != "csi-driver" {
					t.Errorf("DriverName: got %q, want %q", got[0].DriverName, "csi-driver")
				}
				if got[0].CSIVolumeHandle != "handle-1" {
					t.Errorf("CSIVolumeHandle: got %q, want %q", got[0].CSIVolumeHandle, "handle-1")
				}
				if got[0].OuterVolumeName != outerVolumeName {
					t.Errorf("OuterVolumeName: got %q, want %q", got[0].OuterVolumeName, outerVolumeName)
				}
				if got[0].PublishPath != "" || got[0].StagingPath != "" {
					t.Errorf("expected empty paths, got staging=%q publish=%q",
						got[0].StagingPath, got[0].PublishPath)
				}
			}
		})
	}
}

// newCSITestPodAndSpec 构造使用 CSI 卷的测试 Pod 与卷规格。
func newCSITestPodAndSpec(podName, podUID, volName, driver, handle string) (*v1.Pod, *volume.Spec) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  types.UID(podUID),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{{
				Name: volName,
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: "pvc-" + volName,
					},
				},
			}},
		},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-" + volName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driver,
					VolumeHandle: handle,
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	// 通过 PVC 引用名为 pv-<volName> 的 CSI 持久卷。
	return pod, &volume.Spec{PersistentVolume: pv}
}
