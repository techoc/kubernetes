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

package volumemanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/mount-utils"
)

const (
	testHostname = "test-hostname"
)

// TestGetMountedVolumesForPodAndGetVolumesInUse 验证卷模式匹配时会挂载并上报 in-use，不匹配时报错且不挂载。
func TestGetMountedVolumesForPodAndGetVolumesInUse(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tests := []struct {
		name            string
		pvMode, podMode v1.PersistentVolumeMode
		expectMount     bool
		expectError     bool
	}{
		{
			name:        "filesystem volume",
			pvMode:      v1.PersistentVolumeFilesystem,
			podMode:     v1.PersistentVolumeFilesystem,
			expectMount: true,
			expectError: false,
		},
		{
			name:        "block volume",
			pvMode:      v1.PersistentVolumeBlock,
			podMode:     v1.PersistentVolumeBlock,
			expectMount: true,
			expectError: false,
		},
		{
			name:        "mismatched volume",
			pvMode:      v1.PersistentVolumeBlock,
			podMode:     v1.PersistentVolumeFilesystem,
			expectMount: false,
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpDir, err := utiltesting.MkTmpdir("volumeManagerTest")
			if err != nil {
				t.Fatalf("can't make a temp dir: %v", err)
			}
			defer func() {
				if err := os.RemoveAll(tmpDir); err != nil {
					t.Fatalf("failed to remove temp dir: %v", err)
				}
			}()
			podManager := kubepod.NewBasicPodManager()

			node, pod, pv, claim := createObjects(test.pvMode, test.podMode)
			kubeClient := fake.NewSimpleClientset(node, pod, pv, claim)

			manager := newTestVolumeManager(t, tmpDir, podManager, kubeClient, node)

			tCtx := ktesting.Init(t)
			defer tCtx.Cancel("test has completed")
			sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
			go manager.Run(tCtx, sourcesReady)

			podManager.SetPods([]*v1.Pod{pod})

			// 模拟节点状态更新。
			go simulateVolumeInUseUpdate(
				v1.UniqueVolumeName(node.Status.VolumesAttached[0].Name),
				tCtx.Done(),
				manager)

			err = manager.WaitForAttachAndMount(ctx, pod)
			if err != nil && !test.expectError {
				t.Errorf("Expected success: %v", err)
			}
			if err == nil && test.expectError {
				t.Errorf("Expected error, got none")
			}

			expectedMounted := pod.Spec.Volumes[0].Name
			actualMounted := manager.GetMountedVolumesForPod(types.UniquePodName(pod.ObjectMeta.UID))
			if test.expectMount {
				if _, ok := actualMounted[expectedMounted]; !ok || (len(actualMounted) != 1) {
					t.Errorf("Expected %v to be mounted to pod but got %v", expectedMounted, actualMounted)
				}
			} else {
				if _, ok := actualMounted[expectedMounted]; ok || (len(actualMounted) != 0) {
					t.Errorf("Expected %v not to be mounted to pod but got %v", expectedMounted, actualMounted)
				}
			}

			expectedInUse := []v1.UniqueVolumeName{}
			if test.expectMount {
				expectedInUse = []v1.UniqueVolumeName{v1.UniqueVolumeName(node.Status.VolumesAttached[0].Name)}
			}
			actualInUse := manager.GetVolumesInUse()
			if !reflect.DeepEqual(expectedInUse, actualInUse) {
				t.Errorf("Expected %v to be in use but got %v", expectedInUse, actualInUse)
			}
		})
	}
}

// TestWaitForAttachAndMountError 验证存在无法挂载的卷时 WaitForAttachAndMount 返回聚合错误。
func TestWaitForAttachAndMountError(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tmpDir, err := utiltesting.MkTmpdir("volumeManagerTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	podManager := kubepod.NewBasicPodManager()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "nsA",
			UID:       "1234",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container1",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumetest.FailMountDeviceVolumeName,
							MountPath: "/vol1",
						},
						{
							Name:      "vol2",
							MountPath: "/vol2",
						},
						{
							Name:      "vol02",
							MountPath: "/vol02",
						},
						{
							Name:      "vol3",
							MountPath: "/vol3",
						},
						{
							Name:      "vol03",
							MountPath: "/vol03",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: volumetest.FailMountDeviceVolumeName,
					VolumeSource: v1.VolumeSource{
						ConfigMap: &v1.ConfigMapVolumeSource{},
					},
				},
				{
					Name: "vol2",
					VolumeSource: v1.VolumeSource{
						RBD: &v1.RBDVolumeSource{},
					},
				},
				{
					Name: "vol02",
					VolumeSource: v1.VolumeSource{
						RBD: &v1.RBDVolumeSource{},
					},
				},
				{
					Name: "vol3",
					VolumeSource: v1.VolumeSource{
						AzureDisk: &v1.AzureDiskVolumeSource{},
					},
				},
				{
					Name: "vol03",
					VolumeSource: v1.VolumeSource{
						AzureDisk: &v1.AzureDiskVolumeSource{},
					},
				},
			},
		},
	}

	kubeClient := fake.NewSimpleClientset(pod)

	manager := newTestVolumeManager(t, tmpDir, podManager, kubeClient, nil)

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
	go manager.Run(tCtx, sourcesReady)

	podManager.SetPods([]*v1.Pod{pod})

	err = manager.WaitForAttachAndMount(ctx, pod)
	if err == nil {
		t.Errorf("Expected error, got none")
	}
	if !strings.Contains(err.Error(),
		"unattached volumes=[vol02 vol2], failed to process volumes=[vol03 vol3]") {
		t.Errorf("Unexpected error info: %v", err)
	}
}

// TestWaitForAttachAndMountVolumeAttachLimitExceededError 验证卷挂载数量超过限制时返回 VolumeAttachLimitExceededError。
func TestWaitForAttachAndMountVolumeAttachLimitExceededError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, true)

	tmpDir, err := utiltesting.MkTmpdir("volumeManagerTest")
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("Failed to remove temporary directory %s: %v", tmpDir, err)
		}
	})

	podManager := kubepod.NewBasicPodManager()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "nsA",
			UID:       "1234",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container1",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "vol1",
							MountPath: "/vol1",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						RBD: &v1.RBDVolumeSource{},
					},
				},
			},
		},
	}
	kubeClient := fake.NewSimpleClientset(pod)

	attachablePlug := &volumetest.FakeVolumePlugin{
		PluginName: "fake",
		CanSupportFn: func(spec *volume.Spec) bool {
			return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.RBD != nil) ||
				(spec.Volume != nil && spec.Volume.RBD != nil)
		},
		VerifyExhaustedEnabled: true,
	}

	plugMgr := &volume.VolumePluginMgr{}
	fakeVolumeHost := volumetest.NewFakeKubeletVolumeHost(t, tmpDir, kubeClient, nil)
	if err := plugMgr.InitPlugins([]volume.VolumePlugin{attachablePlug}, nil, fakeVolumeHost); err != nil {
		t.Fatalf("Failed to initialize volume plugins: %v", err)
	}

	manager := NewVolumeManager(
		true,
		testHostname,
		podManager,
		&fakePodStateProvider{},
		kubeClient,
		plugMgr,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		"",
		&record.FakeRecorder{},
		volumetest.NewBlockVolumePathHandler(),
		nil,
		0)

	tCtx := ktesting.Init(t)
	t.Cleanup(func() { tCtx.Cancel("test has completed") })
	sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
	// 后台运行管理器以驱动挂载/卸载循环。
	go manager.Run(tCtx, sourcesReady)
	podManager.SetPods([]*v1.Pod{pod})

	ctx, cancel := context.WithTimeout(tCtx, 1*time.Second)
	defer cancel()
	err = manager.WaitForAttachAndMount(ctx, pod)

	require.Error(t, err, "Expected an error but got none")

	var attachErr *VolumeAttachLimitExceededError
	require.ErrorAs(t, err, &attachErr, "Error should be of type VolumeAttachLimitExceededError")
	require.Equal(t, []string{"vol1"}, attachErr.UnmountedVolumes, "UnmountedVolumes mismatch")
	require.Equal(t, []string{"vol1"}, attachErr.UnattachedVolumes, "UnattachedVolumes mismatch")
	require.Empty(t, attachErr.VolumesNotInDSW, "VolumesNotInDSW should be empty")
	require.ErrorIs(t, attachErr.OriginalError, context.DeadlineExceeded, "OriginalError should be context.DeadlineExceeded")
}

// TestInitialPendingVolumesForPodAndGetVolumesInUse 验证 PVC 初始为 Pending 时，等待绑定后仍能完成挂载。
func TestInitialPendingVolumesForPodAndGetVolumesInUse(t *testing.T) {
	tCtx := ktesting.Init(t)
	tmpDir, err := utiltesting.MkTmpdir("volumeManagerTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	podManager := kubepod.NewBasicPodManager()

	node, pod, pv, claim := createObjects(v1.PersistentVolumeFilesystem, v1.PersistentVolumeFilesystem)
	claim.Status = v1.PersistentVolumeClaimStatus{
		Phase: v1.ClaimPending,
	}

	kubeClient := fake.NewSimpleClientset(node, pod, pv, claim)

	manager := newTestVolumeManager(t, tmpDir, podManager, kubeClient, node)

	defer tCtx.Cancel("test has completed")
	sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
	go manager.Run(tCtx, sourcesReady)

	// 把 Pod 加入 PodManager，供卷管理器发现。
	podManager.SetPods([]*v1.Pod{pod})

	// 模拟节点状态更新。
	go simulateVolumeInUseUpdate(
		v1.UniqueVolumeName(node.Status.VolumesAttached[0].Name),
		tCtx.Done(),
		manager)

	// 延迟绑定 claim。
	// 异步延迟绑定，模拟 PVC 绑定耗时。
	go delayClaimBecomesBound(t, kubeClient, claim.GetNamespace(), claim.Name)

	err = wait.Poll(100*time.Millisecond, 1*time.Second, func() (bool, error) {
		err = manager.WaitForAttachAndMount(tCtx, pod)
		if err != nil {
			// 预期会出现少量 “PVC not bound” 错误
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Errorf("Expected a volume to be mounted, got: %s", err)
	}

}

// TestGetExtraSupplementalGroupsForPod 验证从 PV 注解解析额外补充组，并忽略无效或已存在的 gid。
func TestGetExtraSupplementalGroupsForPod(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tmpDir, err := utiltesting.MkTmpdir("volumeManagerTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	podManager := kubepod.NewBasicPodManager()

	node, pod, _, claim := createObjects(v1.PersistentVolumeFilesystem, v1.PersistentVolumeFilesystem)

	existingGid := pod.Spec.SecurityContext.SupplementalGroups[0]

	cases := []struct {
		gidAnnotation string
		expected      []int64
	}{
		{
			gidAnnotation: "777",
			expected:      []int64{777},
		},
		{
			gidAnnotation: strconv.FormatInt(int64(existingGid), 10),
			expected:      []int64{},
		},
		{
			gidAnnotation: "a",
			expected:      []int64{},
		},
		{
			gidAnnotation: "",
			expected:      []int64{},
		},
	}

	for _, tc := range cases {
		fs := v1.PersistentVolumeFilesystem
		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pvA",
				Annotations: map[string]string{
					util.VolumeGidAnnotationKey: tc.gidAnnotation,
				},
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					RBD: &v1.RBDPersistentVolumeSource{
						RBDImage: "fake-device",
					},
				},
				ClaimRef: &v1.ObjectReference{
					Name:      claim.ObjectMeta.Name,
					Namespace: claim.ObjectMeta.Namespace,
				},
				VolumeMode: &fs,
			},
		}
		kubeClient := fake.NewSimpleClientset(node, pod, pv, claim)

		manager := newTestVolumeManager(t, tmpDir, podManager, kubeClient, node)

		tCtx := ktesting.Init(t)
		defer tCtx.Cancel("test has completed")
		sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
		go manager.Run(tCtx, sourcesReady)

		podManager.SetPods([]*v1.Pod{pod})

		// 模拟节点状态更新。
		// 模拟节点已上报该卷在使用中，避免等待超时。
		go simulateVolumeInUseUpdate(
			v1.UniqueVolumeName(node.Status.VolumesAttached[0].Name),
			tCtx.Done(),
			manager)

		err = manager.WaitForAttachAndMount(ctx, pod)
		if err != nil {
			t.Errorf("Expected success: %v", err)
			continue
		}

		actual := manager.GetExtraSupplementalGroupsForPod(pod)
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("Expected supplemental groups %v, got %v", tc.expected, actual)
		}
	}
}

type fakePodStateProvider struct {
	shouldRemove map[kubetypes.UID]struct{}
	terminating  map[kubetypes.UID]struct{}
}

// ShouldPodRuntimeBeRemoved 表示该 UID 的 Pod 运行时是否应被移除。
func (p *fakePodStateProvider) ShouldPodRuntimeBeRemoved(uid kubetypes.UID) bool {
	_, ok := p.shouldRemove[uid]
	return ok
}

// ShouldPodContainersBeTerminating 表示该 UID 的 Pod 容器是否正在终止。
func (p *fakePodStateProvider) ShouldPodContainersBeTerminating(uid kubetypes.UID) bool {
	_, ok := p.terminating[uid]
	return ok
}

// newTestVolumeManager 用假插件、假 mounter 与假客户端构造 VolumeManager。
func newTestVolumeManager(t *testing.T, tmpDir string, podManager kubepod.Manager, kubeClient clientset.Interface, node *v1.Node) VolumeManager {
	attachablePlug := &volumetest.FakeVolumePlugin{
		PluginName: "fake",
		Host:       nil,
		CanSupportFn: func(spec *volume.Spec) bool {
			return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.RBD != nil) ||
				(spec.Volume != nil && spec.Volume.RBD != nil)
		},
	}
	unattachablePlug := &volumetest.FakeVolumePlugin{
		PluginName:    "unattachable-fake-plugin",
		Host:          nil,
		NonAttachable: true,
		CanSupportFn: func(spec *volume.Spec) bool {
			return spec.Volume != nil && spec.Volume.ConfigMap != nil
		},
	}
	fakeRecorder := &record.FakeRecorder{}
	plugMgr := &volume.VolumePluginMgr{}
	// TODO (#51147) inject mock prober
	fakeVolumeHost := volumetest.NewFakeKubeletVolumeHost(t, tmpDir, kubeClient, nil)
	fakeVolumeHost.WithNode(node)

	plugMgr.InitPlugins([]volume.VolumePlugin{attachablePlug, unattachablePlug}, nil /* prober */, fakeVolumeHost)
	stateProvider := &fakePodStateProvider{}
	fakePathHandler := volumetest.NewBlockVolumePathHandler()
	vm := NewVolumeManager(
		true,
		testHostname,
		podManager,
		stateProvider,
		kubeClient,
		plugMgr,
		mount.NewFakeMounter(nil),
		hostutil.NewFakeHostUtil(nil),
		"",
		fakeRecorder,
		fakePathHandler,
		nil,
		0)

	return vm
}

// createObjects 构造用于 fake clientset 的对象：PV 已挂载到节点并绑定到 Pod 使用的 claim。
func createObjects(pvMode, podMode v1.PersistentVolumeMode) (*v1.Node, *v1.Pod, *v1.PersistentVolume, *v1.PersistentVolumeClaim) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testHostname},
		Status: v1.NodeStatus{
			VolumesAttached: []v1.AttachedVolume{
				{
					Name:       "fake/fake-device",
					DevicePath: "fake/path",
				},
			}},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "nsA",
			UID:       "1234",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container1",
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claimA",
						},
					},
				},
			},
			SecurityContext: &v1.PodSecurityContext{
				SupplementalGroups: []int64{555},
			},
		},
	}
	switch podMode {
	case v1.PersistentVolumeBlock:
		pod.Spec.Containers[0].VolumeDevices = []v1.VolumeDevice{
			{
				Name:       "vol1",
				DevicePath: "/dev/vol1",
			},
		}
	case v1.PersistentVolumeFilesystem:
		pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
			{
				Name:      "vol1",
				MountPath: "/mnt/vol1",
			},
		}
	default:
		// 卷既未挂载也未映射。
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvA",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				RBD: &v1.RBDPersistentVolumeSource{
					RBDImage: "fake-device",
				},
			},
			ClaimRef: &v1.ObjectReference{
				Namespace: "nsA",
				Name:      "claimA",
			},
			VolumeMode: &pvMode,
		},
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claimA",
			Namespace: "nsA",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pvA",
			VolumeMode: &pvMode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	return node, pod, pv, claim
}

// simulateVolumeInUseUpdate 周期性把卷标记为已在用，模拟 kubelet 的状态上报。
func simulateVolumeInUseUpdate(volumeName v1.UniqueVolumeName, stopCh <-chan struct{}, volumeManager VolumeManager) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			volumeManager.MarkVolumesAsReportedInUse(
				[]v1.UniqueVolumeName{volumeName})
		case <-stopCh:
			return
		}
	}
}

// delayClaimBecomesBound 延迟把 PVC 状态更新为 Bound，模拟绑定耗时。
func delayClaimBecomesBound(
	t *testing.T,
	kubeClient clientset.Interface,
	namespace, claimName string,
) {
	tCtx := ktesting.Init(t)
	time.Sleep(500 * time.Millisecond)
	volumeClaim, err :=
		kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(tCtx, claimName, metav1.GetOptions{})
	if err != nil {
		t.Errorf("Failed to get PVC: %v", err)
	}
	volumeClaim.Status = v1.PersistentVolumeClaimStatus{
		Phase: v1.ClaimBound,
	}
	_, err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Update(tCtx, volumeClaim, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("Failed to update PVC: %v", err)
	}
}

// TestWaitForAllPodsUnmount 验证并发等待所有 Pod 卷卸载，超时返回聚合错误。
func TestWaitForAllPodsUnmount(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name          string
		numPods       int
		podMode       v1.PersistentVolumeMode
		expectedError bool
	}{
		{
			name:          "successful unmount - single pod",
			numPods:       1,
			podMode:       "",
			expectedError: false,
		},
		{
			name:          "timeout waiting for unmount - single pod",
			numPods:       1,
			podMode:       v1.PersistentVolumeFilesystem,
			expectedError: true,
		},
		{
			name:          "concurrent unmount - multiple pods (10) with timeout errors",
			numPods:       10,
			podMode:       v1.PersistentVolumeFilesystem,
			expectedError: true,
		},
		{
			name:          "concurrent unmount - many pods (20) with timeout errors",
			numPods:       20,
			podMode:       v1.PersistentVolumeFilesystem,
			expectedError: true,
		},
		{
			name:          "concurrent unmount - multiple pods without volumes",
			numPods:       10,
			podMode:       "",
			expectedError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ctx context.Context = ktesting.Init(t)
			podManager := kubepod.NewBasicPodManager()

			node, pods, pvs, claims := createMultiplePodsWithVolumes(test.numPods, test.podMode)

			objects := []runtime.Object{node}
			for i := 0; i < test.numPods; i++ {
				objects = append(objects, pods[i], pvs[i], claims[i])
			}
			kubeClient := fake.NewClientset(objects...)

			manager := newTestVolumeManager(t, tmpDir, podManager, kubeClient, node)

			sourcesReady := config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
			go manager.Run(ctx, sourcesReady)

			podManager.SetPods(pods)

			if test.podMode != "" {
				for i := 0; i < test.numPods; i++ {
					volumeName := v1.UniqueVolumeName(node.Status.VolumesAttached[i].Name)
					go simulateVolumeInUseUpdate(volumeName, ctx.Done(), manager)
				}

				// 按 Pod 数量放宽等待卷标记的时间，避免并发场景下超时。
				volumeMarkTimeout := 10*time.Second + time.Duration(test.numPods/10)*5*time.Second
				err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, volumeMarkTimeout, true, func(context.Context) (bool, error) {
					inUseVolumes := manager.GetVolumesInUse()
					return len(inUseVolumes) == test.numPods, nil
				})

				require.NoError(t, err, "Timeout waiting for all %d volumes to be marked as in-use", test.numPods)

				type attachResult struct {
					podName string
					err     error
				}
				resultChan := make(chan attachResult, test.numPods)

				for _, pod := range pods {
					go func() {
						err := manager.WaitForAttachAndMount(ctx, pod)
						resultChan <- attachResult{
							podName: pod.Name,
							err:     err,
						}
					}()
				}

				for i := 0; i < test.numPods; i++ {
					result := <-resultChan
					require.NoError(t, result.err,
						"Failed to wait for attach and mount for pod %s", result.podName)
				}
			}

			// 统一用 1 秒超时触发卸载等待的边界。
			unmountCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			err := manager.WaitForAllPodsUnmount(unmountCtx, pods)

			if test.expectedError {
				require.ErrorIs(t, err, context.DeadlineExceeded, "Expected error due to timeout")
				// 验证聚合错误中恰好包含 numPods 个错误。
				var aggErr utilerrors.Aggregate
				require.ErrorAs(t, err, &aggErr, "Expected error to be an Aggregate error")
				errs := aggErr.Errors()
				require.Len(t, errs, test.numPods, "Expected %d errors but got %d", test.numPods, len(errs))

				// 验证每个 Pod 的卷名都出现在错误信息中，以证明不同 Pod 都在被处理。
				errString := err.Error()
				for i := 0; i < test.numPods; i++ {
					volumeName := fmt.Sprintf("fake/fake-device-%d", i)
					require.Contains(t, errString, volumeName, "Expected error to contain volume name %s for pod-%d", volumeName, i)
				}
			} else {
				require.NoError(t, err, "Expected no error")
			}
		})
	}
}

// createMultiplePodsWithVolumes 构造指定数量的 Pod、PV、PVC 及其挂载配置。
func createMultiplePodsWithVolumes(numPods int, pvMode v1.PersistentVolumeMode) (*v1.Node, []*v1.Pod, []*v1.PersistentVolume, []*v1.PersistentVolumeClaim) {
	attachedVolumes := make([]v1.AttachedVolume, numPods)
	for i := 0; i < numPods; i++ {
		attachedVolumes[i] = v1.AttachedVolume{
			Name:       v1.UniqueVolumeName(fmt.Sprintf("fake/fake-device-%d", i)),
			DevicePath: fmt.Sprintf("fake/path-%d", i),
		}
	}

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testHostname},
		Status: v1.NodeStatus{
			VolumesAttached: attachedVolumes,
		},
	}

	pods := make([]*v1.Pod, numPods)
	pvs := make([]*v1.PersistentVolume, numPods)
	claims := make([]*v1.PersistentVolumeClaim, numPods)

	for i := 0; i < numPods; i++ {
		podName := fmt.Sprintf("pod-%d", i)
		claimName := fmt.Sprintf("claim-%d", i)
		pvName := fmt.Sprintf("pv-%d", i)
		volumeName := fmt.Sprintf("vol-%d", i)
		uid := fmt.Sprintf("uid-%d", i)

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "nsA",
				UID:       kubetypes.UID(uid),
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "container1",
					},
				},
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: claimName,
							},
						},
					},
				},
			},
		}

		switch pvMode {
		case v1.PersistentVolumeFilesystem:
			pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
				{
					Name:      volumeName,
					MountPath: fmt.Sprintf("/mnt/%s", volumeName),
				},
			}
		case v1.PersistentVolumeBlock:
			pod.Spec.Containers[0].VolumeDevices = []v1.VolumeDevice{
				{
					Name:       volumeName,
					DevicePath: fmt.Sprintf("/dev/%s", volumeName),
				},
			}
		}

		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					RBD: &v1.RBDPersistentVolumeSource{
						RBDImage: fmt.Sprintf("fake-device-%d", i),
					},
				},
				ClaimRef: &v1.ObjectReference{
					Namespace: "nsA",
					Name:      claimName,
				},
				VolumeMode: &pvMode,
			},
		}

		claim := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      claimName,
				Namespace: "nsA",
			},
			Spec: v1.PersistentVolumeClaimSpec{
				VolumeName: pvName,
				VolumeMode: &pvMode,
			},
			Status: v1.PersistentVolumeClaimStatus{
				Phase: v1.ClaimBound,
			},
		}

		pods[i] = pod
		pvs[i] = pv
		claims[i] = claim
	}

	return node, pods, pvs, claims
}

// TestVolumeManager_ResizeEphemeralVolume 验证 memory-backed emptyDir 的直接扩容路径与各类错误。
func TestVolumeManager_ResizeEphemeralVolume(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("in-place resize of memory-backed volumes is only supported on Linux")
	}
	quantity200Mi := resource.MustParse("200Mi")

	tests := []struct {
		name              string
		volumes           []v1.Volume
		isMounted         bool
		newSize           *resource.Quantity
		expectedErrSubstr string
		expectedMountOpts []string
	}{
		{
			name: "resizable memory volume - not yet mounted (deferral path)",
			volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium: v1.StorageMediumMemory,
						},
					},
				},
			},
			isMounted:         false,
			newSize:           &quantity200Mi,
			expectedErrSubstr: "is not yet mounted; deferring resize",
		},
		{
			name: "resizable memory volume - already mounted (success path)",
			volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium: v1.StorageMediumMemory,
						},
					},
				},
			},
			isMounted:         true,
			newSize:           &quantity200Mi,
			expectedErrSubstr: "",
			expectedMountOpts: []string{"remount", "size=209715200"},
		},
		{
			name: "non-resizable disk-backed emptyDir volume (error path)",
			volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium: v1.StorageMediumDefault,
						},
					},
				},
			},
			isMounted:         false,
			newSize:           &quantity200Mi,
			expectedErrSubstr: "only memory-backed emptyDir volumes support direct resize",
		},
		{
			name: "non-resizable non-emptyDir volume (error path)",
			volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			isMounted:         false,
			newSize:           &quantity200Mi,
			expectedErrSubstr: "does not support direct resizing",
		},
		{
			name:              "volume not found in pod spec (error path)",
			volumes:           []v1.Volume{},
			isMounted:         false,
			newSize:           &quantity200Mi,
			expectedErrSubstr: "volume vol1 not found in pod test-pod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					UID:  "12345",
				},
				Spec: v1.PodSpec{
					Volumes: tt.volumes,
				},
			}

			plugMgr := &volume.VolumePluginMgr{}
			fakeVolumeHost := volumetest.NewFakeKubeletVolumeHost(t, tmpDir, nil, nil)
			err := plugMgr.InitPlugins(emptydir.ProbeVolumePlugins(), nil, fakeVolumeHost)
			require.NoError(t, err)

			physicalMounter := fakeVolumeHost.GetMounter().(*mount.FakeMounter)
			volPath := filepath.Join(tmpDir, "pods/12345/volumes/kubernetes.io~empty-dir/vol1")

			if tt.isMounted {
				// 预置挂载点，模拟卷已挂载且带有旧 size。
				physicalMounter.MountPoints = []mount.MountPoint{
					{
						Path: volPath,
						Opts: []string{"size=104857600"}, // 100Mi
					},
				}
				err := os.MkdirAll(volPath, 0750)
				require.NoError(t, err)
			} else {
				// 未挂载时移除目录，模拟延迟扩容路径。
				os.RemoveAll(volPath) //nolint:errcheck
			}

			podManager := kubepod.NewBasicPodManager()
			podManager.SetPods([]*v1.Pod{pod})

			manager := NewVolumeManager(
				true,
				testHostname,
				podManager,
				&fakePodStateProvider{},
				nil,
				plugMgr,
				physicalMounter,
				hostutil.NewFakeHostUtil(nil),
				"",
				&record.FakeRecorder{},
				volumetest.NewBlockVolumePathHandler(),
				nil,
				0)

			// 测试 ResizeEphemeralVolume。
			err = manager.ResizeEphemeralVolume(pod, "vol1", tt.newSize)
			if tt.expectedErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErrSubstr)
				return
			}
			require.NoError(t, err)

			// 成功路径下检查当前挂载点的选项。
			var targetMp *mount.MountPoint
			// 从后往前查找最新的目标挂载点，因为重挂载会追加记录。
			for i := len(physicalMounter.MountPoints) - 1; i >= 0; i-- {
				if physicalMounter.MountPoints[i].Path == volPath {
					targetMp = &physicalMounter.MountPoints[i]
					break
				}
			}
			require.NotNil(t, targetMp)
			for _, expectedOpt := range tt.expectedMountOpts {
				assert.Contains(t, targetMp.Opts, expectedOpt)
			}

		})
	}
}
