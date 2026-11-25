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

// kuberuntime_container_test.go 覆盖容器移除、终止、状态转换、生命周期钩子、重启计数与资源更新等行为。
package kuberuntime

import (
	"os"
	"path/filepath"
	"regexp"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/test/utils/ktesting"

	kubelettypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/logs"
)

// TestRemoveContainer 验证移除容器时会同时清理其日志文件与遗留的日志软链接。
func TestRemoveContainer(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)
	// 用真实的 ContainerLogManager 替换测试桩，才能验证日志清理行为。
	logManager, err := logs.NewContainerLogManager(m.runtimeService, m.osInterface, "1", 2, 10, metav1.Duration{Duration: 10 * time.Second})
	require.NoError(t, err)
	m.logManager = logManager

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}

	// 在 fake runtime 中创建 sandbox 与容器，作为测试前置状态。
	_, fakeContainers := makeAndSetFakePod(tCtx, m, fakeRuntime, pod)
	assert.Len(t, fakeContainers, 1)

	containerID := fakeContainers[0].Id
	fakeOS := m.osInterface.(*containertest.FakeOS)
	fakeOS.GlobFn = func(pattern, path string) bool {
		pattern = strings.Replace(pattern, "*", ".*", -1)
		pattern = strings.Replace(pattern, "\\", "\\\\", -1)
		return regexp.MustCompile(pattern).MatchString(path)
	}
	podLogsDirectory := "/var/log/pods"
	expectedContainerLogPath := filepath.Join(podLogsDirectory, "new_bar_12345678", "foo", "0.log")
	expectedContainerLogPathRotated := filepath.Join(podLogsDirectory, "new_bar_12345678", "foo", "0.log.20060102-150405")
	expectedContainerLogSymlink := legacyLogSymlink(containerID, "foo", "bar", "new")

	fakeOS.Create(expectedContainerLogPath)
	fakeOS.Create(expectedContainerLogPathRotated)

	err = m.removeContainer(tCtx, containerID, false)
	assert.NoError(t, err)

	// 校验日志文件与软链接都被删除。
	// 删除顺序无法预测，因此用 ElementsMatch 做无序比较。
	assert.ElementsMatch(t,
		[]string{expectedContainerLogSymlink, expectedContainerLogPath, expectedContainerLogPathRotated},
		fakeOS.Removes)
	// 校验容器本身也已从运行时移除。
	assert.Contains(t, fakeRuntime.Called, "RemoveContainer")
	containers, err := fakeRuntime.ListContainers(tCtx, &runtimeapi.ContainerFilter{Id: containerID})
	assert.NoError(t, err)
	assert.Empty(t, containers)
}

// TestRemoveContainer_keepLogs 验证保留日志模式下移除容器不会删除日志文件。
func TestRemoveContainer_keepLogs(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)
	// 用真实实现替换桩，确保日志策略真正生效。
	logManager, err := logs.NewContainerLogManager(m.runtimeService, m.osInterface, "1", 2, 10, metav1.Duration{Duration: 10 * time.Second})
	require.NoError(t, err)
	m.logManager = logManager

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}

	// 准备 fake runtime 中的 sandbox 与容器。
	_, fakeContainers := makeAndSetFakePod(tCtx, m, fakeRuntime, pod)
	assert.Len(t, fakeContainers, 1)

	containerID := fakeContainers[0].Id
	fakeOS := m.osInterface.(*containertest.FakeOS)
	fakeOS.GlobFn = func(pattern, path string) bool {
		pattern = strings.ReplaceAll(pattern, "*", ".*")
		pattern = strings.ReplaceAll(pattern, "\\", "\\\\")
		return regexp.MustCompile(pattern).MatchString(path)
	}
	podLogsDirectory := "/var/log/pods"
	expectedContainerLogPath := filepath.Join(podLogsDirectory, "new_bar_12345678", "foo", "0.log")
	expectedContainerLogPathRotated := filepath.Join(podLogsDirectory, "new_bar_12345678", "foo", "0.log.20060102-150405")

	_, err = fakeOS.Create(expectedContainerLogPath)
	require.NoError(t, err)
	_, err = fakeOS.Create(expectedContainerLogPathRotated)
	require.NoError(t, err)

	err = m.removeContainer(tCtx, containerID, true)
	require.NoError(t, err)

	// 校验日志文件未被删除。
	// 这里没有删除操作，直接断言 Removes 为空即可。
	require.Empty(t, fakeOS.Removes)
	// 容器本身仍应被移除。
	require.Contains(t, fakeRuntime.Called, "RemoveContainer")
	containers, err := fakeRuntime.ListContainers(tCtx, &runtimeapi.ContainerFilter{Id: containerID})
	require.NoError(t, err)
	require.Empty(t, containers)
}

// TestKillContainer 验证终止 Pod 中的容器；找不到容器时应返回错误。
func TestKillContainer(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, _ := createTestRuntimeManager(tCtx)

	tests := []struct {
		caseName            string
		pod                 *v1.Pod
		containerID         kubecontainer.ContainerID
		containerName       string
		reason              string
		gracePeriodOverride int64
		succeed             bool
	}{
		{
			caseName: "Failed to find container in pods, expect to return error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1_id", Name: "pod1", Namespace: "default"},
				Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "empty_container"}}},
			},
			containerID:         kubecontainer.ContainerID{Type: "docker", ID: "not_exist_container_id"},
			containerName:       "not_exist_container",
			reason:              "unknown reason",
			gracePeriodOverride: 0,
			succeed:             false,
		},
	}

	for _, test := range tests {
		tCtx := ktesting.Init(t)
		err := m.killContainer(tCtx, test.pod, test.containerID, test.containerName, test.reason, "", &test.gracePeriodOverride, nil)
		if test.succeed != (err == nil) {
			t.Errorf("%s: expected %v, got %v (%v)", test.caseName, test.succeed, (err == nil), err)
		}
	}
}

// TestToKubeContainerStatus 验证不同状态（created/running/exited/unknown）的 CRI 容器状态能被正确转换。
func TestToKubeContainerStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	cid := &kubecontainer.ContainerID{Type: "testRuntime", ID: "dummyid"}
	meta := &runtimeapi.ContainerMetadata{Name: "cname", Attempt: 3}
	imageSpec := &runtimeapi.ImageSpec{Image: "fimage"}
	const (
		podUID     types.UID = "12345-abcd"
		createdAt  int64     = 327
		startedAt  int64     = 999
		finishedAt int64     = 1278
	)

	_, _, m, _ := createTestRuntimeManager(tCtx)

	for desc, test := range map[string]struct {
		input    *runtimeapi.ContainerStatus
		expected *kubecontainer.Status
	}{
		"created container": {
			input: &runtimeapi.ContainerStatus{
				Id:        cid.ID,
				Metadata:  meta,
				Image:     imageSpec,
				State:     runtimeapi.ContainerState_CONTAINER_CREATED,
				CreatedAt: createdAt,
			},
			expected: &kubecontainer.Status{
				ID:        *cid,
				Image:     imageSpec.Image,
				State:     kubecontainer.ContainerStateCreated,
				CreatedAt: time.Unix(0, createdAt),
			},
		},
		"running container": {
			input: &runtimeapi.ContainerStatus{
				Id:        cid.ID,
				Metadata:  meta,
				Image:     imageSpec,
				State:     runtimeapi.ContainerState_CONTAINER_RUNNING,
				CreatedAt: createdAt,
				StartedAt: startedAt,
			},
			expected: &kubecontainer.Status{
				ID:        *cid,
				Image:     imageSpec.Image,
				State:     kubecontainer.ContainerStateRunning,
				CreatedAt: time.Unix(0, createdAt),
				StartedAt: time.Unix(0, startedAt),
			},
		},
		"exited container": {
			input: &runtimeapi.ContainerStatus{
				Id:         cid.ID,
				Metadata:   meta,
				Image:      imageSpec,
				State:      runtimeapi.ContainerState_CONTAINER_EXITED,
				CreatedAt:  createdAt,
				StartedAt:  startedAt,
				FinishedAt: finishedAt,
				ExitCode:   int32(121),
				Reason:     "GotKilled",
				Message:    "The container was killed",
			},
			expected: &kubecontainer.Status{
				ID:         *cid,
				Image:      imageSpec.Image,
				State:      kubecontainer.ContainerStateExited,
				CreatedAt:  time.Unix(0, createdAt),
				StartedAt:  time.Unix(0, startedAt),
				FinishedAt: time.Unix(0, finishedAt),
				ExitCode:   121,
				Reason:     "GotKilled",
				Message:    "The container was killed",
			},
		},
		"unknown container": {
			input: &runtimeapi.ContainerStatus{
				Id:        cid.ID,
				Metadata:  meta,
				Image:     imageSpec,
				State:     runtimeapi.ContainerState_CONTAINER_UNKNOWN,
				CreatedAt: createdAt,
				StartedAt: startedAt,
			},
			expected: &kubecontainer.Status{
				ID:        *cid,
				Image:     imageSpec.Image,
				State:     kubecontainer.ContainerStateUnknown,
				CreatedAt: time.Unix(0, createdAt),
				StartedAt: time.Unix(0, startedAt),
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			actual := m.toKubeContainerStatus(tCtx, podUID, test.input, cid.Type)
			assert.Equal(t, test.expected, actual, desc)
		})
	}
}

// TestToKubeContainerStatusWithResources 验证带资源信息的 CRI 容器状态转换，包括 CPU/内存限制与 request 的推算。
func TestToKubeContainerStatusWithResources(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("InPlacePodVerticalScaling is not currently supported on Windows.")
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	logger, tCtx := ktesting.NewTestContext(t)

	const (
		podUID    types.UID = "12345-abcd"
		createdAt int64     = 327
		startedAt int64     = 999
		cName               = "cname-abcd"
	)
	cid := &kubecontainer.ContainerID{Type: "testRuntime", ID: "dummyid"}
	meta := &runtimeapi.ContainerMetadata{Name: cName, Attempt: 3}
	imageSpec := &runtimeapi.ImageSpec{Image: "fimage"}
	labels := map[string]string{
		kubelettypes.KubernetesPodNameLabel:       "pod-12345",
		kubelettypes.KubernetesPodNamespaceLabel:  "default",
		kubelettypes.KubernetesPodUIDLabel:        string(podUID),
		kubelettypes.KubernetesContainerNameLabel: cName,
	}

	_, _, m, _ := createTestRuntimeManager(tCtx)

	for desc, test := range map[string]struct {
		reportedResources *runtimeapi.ContainerResources
		actuatedResources *v1.ResourceRequirements
		Resources         *kubecontainer.ContainerResources
		skipOnWindows     bool
	}{
		"container reporting cpu and memory": {
			reportedResources: func() *runtimeapi.ContainerResources {
				if goruntime.GOOS == "windows" {
					return &runtimeapi.ContainerResources{
						Windows: &runtimeapi.WindowsContainerResources{
							CpuMaximum:         2500,
							CpuCount:           1,
							MemoryLimitInBytes: 524288000,
						},
					}
				}
				return &runtimeapi.ContainerResources{
					Linux: &runtimeapi.LinuxContainerResources{
						CpuQuota:           25000,
						CpuPeriod:          100000,
						MemoryLimitInBytes: 524288000,
						OomScoreAdj:        -998,
					},
				}
			}(),
			Resources: &kubecontainer.ContainerResources{
				CPULimit:    resource.NewMilliQuantity(250, resource.DecimalSI),
				MemoryLimit: resource.NewQuantity(524288000, resource.BinarySI),
			},
			skipOnWindows: true,
		},
		"container reporting cpu only": {
			reportedResources: func() *runtimeapi.ContainerResources {
				if goruntime.GOOS == "windows" {
					return &runtimeapi.ContainerResources{
						Windows: &runtimeapi.WindowsContainerResources{
							CpuMaximum: 2500,
							CpuCount:   2,
						},
					}
				}
				return &runtimeapi.ContainerResources{
					Linux: &runtimeapi.LinuxContainerResources{
						CpuShares: int64(cm.MilliCPUToShares(500)),
						CpuQuota:  50000,
						CpuPeriod: 100000,
					},
				}
			}(),
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.BinarySI),
					v1.ResourceMemory: resource.Quantity{},
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(500, resource.BinarySI),
				},
			},
			Resources: &kubecontainer.ContainerResources{
				CPURequest: resource.NewMilliQuantity(500, resource.DecimalSI),
				CPULimit:   resource.NewMilliQuantity(500, resource.DecimalSI),
			},
		},
		"container reporting memory only": {
			reportedResources: &runtimeapi.ContainerResources{
				Linux: &runtimeapi.LinuxContainerResources{
					MemoryLimitInBytes: 524288000,
					OomScoreAdj:        -998,
				},
				Windows: &runtimeapi.WindowsContainerResources{
					MemoryLimitInBytes: 524288000,
				},
			},
			Resources: &kubecontainer.ContainerResources{
				MemoryLimit: resource.NewQuantity(524288000, resource.BinarySI),
			},
		},
		"container reporting memory limits, reqs from actuated": {
			reportedResources: &runtimeapi.ContainerResources{
				Linux: &runtimeapi.LinuxContainerResources{
					MemoryLimitInBytes: 524288000,
					OomScoreAdj:        -998,
				},
				Windows: &runtimeapi.WindowsContainerResources{
					MemoryLimitInBytes: 524288000,
				},
			},
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: *resource.NewQuantity(262144000, resource.BinarySI),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: *resource.NewQuantity(524288000, resource.BinarySI),
				},
			},
			Resources: &kubecontainer.ContainerResources{
				MemoryRequest: resource.NewQuantity(262144000, resource.BinarySI),
				MemoryLimit:   resource.NewQuantity(524288000, resource.BinarySI),
			},
		},
		"reported resources take precedence to actuated resources": {
			reportedResources: &runtimeapi.ContainerResources{
				Linux: &runtimeapi.LinuxContainerResources{
					CpuShares: int64(cm.MilliCPUToShares(500)),
					CpuQuota:  50000,
					CpuPeriod: 100000,
				},
			},
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(400, resource.BinarySI),
					v1.ResourceMemory: *resource.NewQuantity(262144000, resource.BinarySI),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: *resource.NewMilliQuantity(400, resource.BinarySI),
				},
			},
			Resources: &kubecontainer.ContainerResources{
				CPURequest:    resource.NewMilliQuantity(500, resource.DecimalSI),
				CPULimit:      resource.NewMilliQuantity(500, resource.DecimalSI),
				MemoryRequest: resource.NewQuantity(262144000, resource.BinarySI),
			},
			skipOnWindows: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			if test.skipOnWindows && goruntime.GOOS == "windows" {
				// TODO: 修复失败用例后移除跳过。
				t.Skip("Skip failing test on Windows.")
			}

			input := &runtimeapi.ContainerStatus{
				Id:        cid.ID,
				Metadata:  meta,
				Labels:    labels,
				Image:     imageSpec,
				State:     runtimeapi.ContainerState_CONTAINER_RUNNING,
				CreatedAt: createdAt,
				StartedAt: startedAt,
				Resources: test.reportedResources,
			}

			expected := &kubecontainer.Status{
				ID:        *cid,
				Name:      cName,
				Image:     imageSpec.Image,
				State:     kubecontainer.ContainerStateRunning,
				CreatedAt: time.Unix(0, createdAt),
				StartedAt: time.Unix(0, startedAt),
				Resources: test.Resources,
			}

			if test.actuatedResources != nil {
				require.NoError(t, m.actuatedState.SetContainerResources(logger, podUID, meta.Name, *test.actuatedResources))
				t.Cleanup(func() { _ = m.actuatedState.RemovePod(logger, podUID) })
			}

			actual := m.toKubeContainerStatus(tCtx, podUID, input, cid.Type)
			assert.Equal(t, expected, actual, desc)
		})
	}
}

// TestToKubeContainerStatusWithUser 验证容器用户信息（UID/GID/附加组）的转换受 SupplementalGroupsPolicy 门控控制。
func TestToKubeContainerStatusWithUser(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("Updating Pod Container User is not supported on Windows.")
	}

	tCtx := ktesting.Init(t)
	cid := &kubecontainer.ContainerID{Type: "testRuntime", ID: "dummyid"}
	meta := &runtimeapi.ContainerMetadata{Name: "cname", Attempt: 3}
	imageSpec := &runtimeapi.ImageSpec{Image: "fimage"}
	const (
		podUID    types.UID = "12345-abcd"
		createdAt int64     = 327
		startedAt int64     = 999
	)

	_, _, m, _ := createTestRuntimeManager(tCtx)

	for desc, test := range map[string]struct {
		input          *runtimeapi.ContainerUser
		expected       *kubecontainer.ContainerUser
		featureEnabled bool
	}{
		"non nil user, SupplementalGroupsPolicy is disabled": {
			input: &runtimeapi.ContainerUser{
				Linux: &runtimeapi.LinuxContainerUser{
					Uid:                0,
					Gid:                0,
					SupplementalGroups: []int64{10},
				},
			},
			expected:       nil,
			featureEnabled: false,
		},
		"empty user, SupplementalGroupsPolicy is disabled": {
			input:          &runtimeapi.ContainerUser{},
			expected:       nil,
			featureEnabled: false,
		},
		"nil user, SupplementalGroupsPolicy is disabled": {
			input:          nil,
			expected:       nil,
			featureEnabled: false,
		},
		"non nil user, SupplementalGroupsPolicy is enabled": {
			input: &runtimeapi.ContainerUser{
				Linux: &runtimeapi.LinuxContainerUser{
					Uid:                0,
					Gid:                0,
					SupplementalGroups: []int64{10},
				},
			},
			expected: &kubecontainer.ContainerUser{
				Linux: &kubecontainer.LinuxContainerUser{
					UID:                0,
					GID:                0,
					SupplementalGroups: []int64{10},
				},
			},
			featureEnabled: true,
		},
		"empty user, SupplementalGroupsPolicy is enabled": {
			input:          &runtimeapi.ContainerUser{},
			expected:       &kubecontainer.ContainerUser{},
			featureEnabled: true,
		},
		"nil user, SupplementalGroupsPolicy is enabled": {
			input:          nil,
			expected:       nil,
			featureEnabled: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			// 固定仿真版本，才能在测试中关闭该特性门控。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.34"))
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SupplementalGroupsPolicy, test.featureEnabled)
			cStatus := &runtimeapi.ContainerStatus{
				Id:        cid.ID,
				Metadata:  meta,
				Image:     imageSpec,
				State:     runtimeapi.ContainerState_CONTAINER_RUNNING,
				CreatedAt: createdAt,
				StartedAt: startedAt,
				User:      test.input,
			}
			actual := m.toKubeContainerStatus(tCtx, podUID, cStatus, cid.Type)
			assert.EqualValues(t, test.expected, actual.User, desc)
		})
	}
}

// testLifeCycleHook 是生命周期钩子的共享测试逻辑，供普通容器与重启式 init 容器复用。
func testLifeCycleHook(t *testing.T, testPod *v1.Pod, testContainer *v1.Container) {
	tCtx := ktesting.Init(t)
	// 准备 fake runtime、Pod 与容器 ID。
	fakeRuntime, _, m, _ := createTestRuntimeManager(tCtx)

	gracePeriod := int64(30)
	cID := kubecontainer.ContainerID{
		Type: "docker",
		ID:   "foo",
	}

	cmdPostStart := &v1.Lifecycle{
		PostStart: &v1.LifecycleHandler{
			Exec: &v1.ExecAction{
				Command: []string{"PostStartCMD"},
			},
		},
	}

	httpLifeCycle := &v1.Lifecycle{
		PreStop: &v1.LifecycleHandler{
			HTTPGet: &v1.HTTPGetAction{
				Host: "testHost.com",
				Path: "/GracefulExit",
			},
		},
	}

	cmdLifeCycle := &v1.Lifecycle{
		PreStop: &v1.LifecycleHandler{
			Exec: &v1.ExecAction{
				Command: []string{"PreStopCMD"},
			},
		},
	}

	fakeRunner := &containertest.FakeContainerCommandRunner{}
	fakeHTTP := &fakeHTTP{}
	fakePodStatusProvider := fakePodStatusProvider{
		pod: &kubecontainer.Pod{
			ID:        testPod.UID,
			Name:      testPod.Name,
			Namespace: testPod.Namespace,
		},
		status: &kubecontainer.PodStatus{
			ID:        testPod.UID,
			Name:      testPod.Name,
			Namespace: testPod.Namespace,
			IPs: []string{
				"127.0.0.1",
			},
		},
	}

	lcHanlder := lifecycle.NewHandlerRunner(
		fakeHTTP,
		fakeRunner,
		fakePodStatusProvider,
		nil)

	m.runner = lcHanlder

	// 场景：钩子已配置且执行成功。
	t.Run("PreStop-CMDExec", func(t *testing.T) {
		tCtx := ktesting.Init(t)
		testContainer.Lifecycle = cmdLifeCycle
		_ = m.killContainer(tCtx, testPod, cID, "foo", "testKill", "", &gracePeriod, nil)
		if fakeRunner.Cmd[0] != cmdLifeCycle.PreStop.Exec.Command[0] {
			t.Errorf("CMD Prestop hook was not invoked")
		}
	})

	// 场景：配置了 HTTP 形式的 PreStop 钩子。
	t.Run("PreStop-HTTPGet", func(t *testing.T) {
		t.Run("consistent", func(t *testing.T) {
			tCtx := ktesting.Init(t)
			defer func() { fakeHTTP.req = nil }()
			httpLifeCycle.PreStop.HTTPGet.Port = intstr.FromInt32(80)
			testContainer.Lifecycle = httpLifeCycle
			_ = m.killContainer(tCtx, testPod, cID, "foo", "testKill", "", &gracePeriod, nil)
			if fakeHTTP.req == nil || !strings.Contains(fakeHTTP.req.URL.String(), httpLifeCycle.PreStop.HTTPGet.Host) {
				t.Errorf("HTTP Prestop hook was not invoked")
			}
		})
	})

	// 场景：宽限期过短，PreStop 钩子没有时间执行。
	t.Run("PreStop-NoTimeToRun", func(t *testing.T) {
		tCtx := ktesting.Init(t)
		gracePeriodLocal := int64(0)

		testPod.DeletionGracePeriodSeconds = &gracePeriodLocal
		testPod.Spec.TerminationGracePeriodSeconds = &gracePeriodLocal

		_ = m.killContainer(tCtx, testPod, cID, "foo", "testKill", "", &gracePeriodLocal, nil)
		if fakeHTTP.req != nil {
			t.Errorf("HTTP Prestop hook Should not execute when gracePeriod is 0")
		}
	})

	// 场景：PostStart 脚本会被调用。
	t.Run("PostStart-CmdExe", func(t *testing.T) {
		tCtx := ktesting.Init(t)
		// 先伪造创建容器所需的运行时状态。
		fakeSandBox, _ := makeAndSetFakePod(tCtx, m, fakeRuntime, testPod)
		fakeSandBoxConfig, _ := m.generatePodSandboxConfig(tCtx, testPod, 0)
		testContainer.Lifecycle = cmdPostStart
		fakePodStatus := &kubecontainer.PodStatus{
			ContainerStatuses: []*kubecontainer.Status{
				{
					ID: kubecontainer.ContainerID{
						Type: "docker",
						ID:   testContainer.Name,
					},
					Name:      testContainer.Name,
					State:     kubecontainer.ContainerStateCreated,
					CreatedAt: time.Unix(0, time.Now().Unix()),
				},
			},
		}

		// 创建容器时应触发 PostStart 钩子。
		_, err := m.startContainer(tCtx, fakeSandBox.Id, fakeSandBoxConfig, containerStartSpec(testContainer), testPod, fakePodStatus, nil, "", []string{}, nil)
		if err != nil {
			t.Errorf("startContainer error =%v", err)
		}
		if fakeRunner.Cmd[0] != cmdPostStart.PostStart.Exec.Command[0] {
			t.Errorf("CMD PostStart hook was not invoked")
		}
	})
}

// TestLifeCycleHook 验证普通容器的 PostStart/PreStop 钩子行为。
func TestLifeCycleHook(t *testing.T) {
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bar",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         []string{"testCommand"},
					WorkingDir:      "testWorkingDir",
				},
			},
		},
	}

	testLifeCycleHook(t, testPod, &testPod.Spec.Containers[0])
}

// TestLifeCycleHookForRestartableInitContainer 验证重启式 init 容器同样支持生命周期钩子。
func TestLifeCycleHookForRestartableInitContainer(t *testing.T) {
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bar",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         []string{"testCommand"},
					WorkingDir:      "testWorkingDir",
					RestartPolicy:   &containerRestartPolicyAlways,
				},
			},
		},
	}

	testLifeCycleHook(t, testPod, &testPod.Spec.InitContainers[0])
}

// TestStartSpec 验证 startSpec 能按容器状态正确解析出目标容器 ID（含 init 容器与未知容器场景）。
func TestStartSpec(t *testing.T) {
	podStatus := &kubecontainer.PodStatus{
		ContainerStatuses: []*kubecontainer.Status{
			{
				ID: kubecontainer.ContainerID{
					Type: "docker",
					ID:   "docker-something-something",
				},
				Name: "target",
			},
		},
	}

	for _, tc := range []struct {
		name string
		spec *startSpec
		want *kubecontainer.ContainerID
	}{
		{
			"Regular Container",
			containerStartSpec(&v1.Container{
				Name: "test",
			}),
			nil,
		},
		{
			"Ephemeral Container w/o Target",
			ephemeralContainerStartSpec(&v1.EphemeralContainer{
				EphemeralContainerCommon: v1.EphemeralContainerCommon{
					Name: "test",
				},
			}),
			nil,
		},
		{
			"Ephemeral Container w/ Target",
			ephemeralContainerStartSpec(&v1.EphemeralContainer{
				EphemeralContainerCommon: v1.EphemeralContainerCommon{
					Name: "test",
				},
				TargetContainerName: "target",
			}),
			&kubecontainer.ContainerID{
				Type: "docker",
				ID:   "docker-something-something",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := tc.spec.getTargetID(podStatus); err != nil {
				t.Fatalf("%v: getTargetID got unexpected error: %v", t.Name(), err)
			} else if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("%v: getTargetID got unexpected result. diff:\n%v", t.Name(), diff)
			}
		})
	}
}

// TestRestartCountByLogDir 验证能从日志目录中的文件名推导出容器重启次数。
func TestRestartCountByLogDir(t *testing.T) {
	for _, tc := range []struct {
		filenames    []string
		restartCount int
	}{
		{
			filenames:    []string{"0.log.rotated-log"},
			restartCount: 1,
		},
		{
			filenames:    []string{"0.log"},
			restartCount: 1,
		},
		{
			filenames:    []string{"0.log", "1.log", "2.log"},
			restartCount: 3,
		},
		{
			filenames:    []string{"0.log.rotated", "1.log", "2.log"},
			restartCount: 3,
		},
		{
			filenames:    []string{"5.log.rotated", "6.log.rotated"},
			restartCount: 7,
		},
		{
			filenames:    []string{"5.log.rotated", "6.log", "7.log"},
			restartCount: 8,
		},
		// 没有可解析的日志文件时应返回 0。
		{
			filenames:    []string{},
			restartCount: 0,
		},
		{
			filenames:    []string{"a.log.rotated", "b.log.rotated", "12log.rotated"},
			restartCount: 0,
		},
		// 双重扩展名 .log.log.rotated 仍应取 145 作为计数。
		{
			filenames:    []string{"145.log.log.rotated"},
			restartCount: 146,
		},
		// 超出 int64 的数字无法解析，返回 0。
		{
			filenames:    []string{"92233720368547758089223372036854775808.log.rotated"},
			restartCount: 0,
		},
		// 混合多种文件名时取可解析的最大编号加一。
		{
			filenames:    []string{"9223372036854775808.log.rotated", "23.log", "23a.log", "1aaa.log.rotated", "2.log", "3.log.rotated"},
			restartCount: 24,
		},
		// 带前缀或负号的名称不符合命名规则，返回 0。
		{
			filenames:    []string{"rotated.23.log"},
			restartCount: 0,
		},
		{
			filenames:    []string{"mylog42.log"},
			restartCount: 0,
		},
		{
			filenames:    []string{"-42.log"},
			restartCount: 0,
		},
		// 同一计数出现多次（含轮转文件）只计一次。
		{
			filenames:    []string{"6.log", "6.log.rotated", "6.log.rotated.rotated"},
			restartCount: 7,
		},
	} {
		tempDirPath, err := os.MkdirTemp("", "test-restart-count-")
		assert.NoError(t, err, "create tempdir error")
		defer os.RemoveAll(tempDirPath)
		for _, filename := range tc.filenames {
			err = os.WriteFile(filepath.Join(tempDirPath, filename), []byte("a log line"), 0600)
			assert.NoError(t, err, "could not write log file")
		}
		count, err := calcRestartCountByLogDir(tempDirPath)
		if assert.NoError(t, err) {
			assert.Equal(t, count, tc.restartCount, "count %v should equal restartCount %v", count, tc.restartCount)
		}
	}
}

// TestKillContainerGracePeriod 验证终止宽限期在 Pod 级、容器级与 reason 覆盖之间的优先级。
func TestKillContainerGracePeriod(t *testing.T) {
	tCtx := ktesting.Init(t)

	shortGracePeriod := int64(10)
	mediumGracePeriod := int64(30)
	longGracePeriod := int64(60)

	tests := []struct {
		name                string
		pod                 *v1.Pod
		reason              containerKillReason
		expectedGracePeriod int64
	}{
		{
			name: "default termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{Name: "foo"}}},
			},
			reason:              reasonUnknown,
			expectedGracePeriod: int64(2),
		},
		{
			name: "use pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers:                    []v1.Container{{Name: "foo"}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonUnknown,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "liveness probe overrides pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonLivenessProbe,
			expectedGracePeriod: shortGracePeriod,
		},
		{
			name: "startup probe overrides pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", StartupProbe: &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonStartupProbe,
			expectedGracePeriod: shortGracePeriod,
		},
		{
			name: "startup probe overrides pod termination grace period, probe period > pod period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", StartupProbe: &v1.Probe{TerminationGracePeriodSeconds: &longGracePeriod},
					}},
					TerminationGracePeriodSeconds: &shortGracePeriod,
				},
			},
			reason:              reasonStartupProbe,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "liveness probe overrides pod termination grace period, probe period > pod period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &longGracePeriod},
					}},
					TerminationGracePeriodSeconds: &shortGracePeriod,
				},
			},
			reason:              reasonLivenessProbe,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "non-liveness probe failure, use pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonUnknown,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "non-startup probe failure, use pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "foo", StartupProbe: &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonUnknown,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "all three grace periods set, use pod termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:          "foo",
						StartupProbe:  &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
						LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &mediumGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonUnknown,
			expectedGracePeriod: longGracePeriod,
		},
		{
			name: "all three grace periods set, use startup termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:          "foo",
						StartupProbe:  &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
						LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &mediumGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonStartupProbe,
			expectedGracePeriod: shortGracePeriod,
		},
		{
			name: "all three grace periods set, use liveness termination grace period",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:          "foo",
						StartupProbe:  &v1.Probe{TerminationGracePeriodSeconds: &shortGracePeriod},
						LivenessProbe: &v1.Probe{TerminationGracePeriodSeconds: &mediumGracePeriod},
					}},
					TerminationGracePeriodSeconds: &longGracePeriod,
				},
			},
			reason:              reasonLivenessProbe,
			expectedGracePeriod: mediumGracePeriod,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualGracePeriod := setTerminationGracePeriod(tCtx, test.pod, &test.pod.Spec.Containers[0], "", kubecontainer.ContainerID{}, test.reason)
			require.Equal(t, test.expectedGracePeriod, actualGracePeriod)
		})
	}
}

// TestUpdateContainerResources 验证更新容器资源会调用运行时的 UpdateContainerResources。
func TestUpdateContainerResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, errCreate := createTestRuntimeManager(tCtx)
	require.NoError(t, errCreate)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "foo",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}

	// 准备 fake runtime 中的 sandbox 与容器。
	_, fakeContainers := makeAndSetFakePod(tCtx, m, fakeRuntime, pod)
	assert.Len(t, fakeContainers, 1)

	runtimePod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	cStatus, _, err := m.getPodContainerStatuses(tCtx, runtimePod, "")
	require.NoError(t, err)
	containerID := cStatus[0].ID

	err = m.updateContainerResources(tCtx, pod, &pod.Spec.Containers[0], containerID)
	assert.NoError(t, err)

	// 校验运行时确实收到了资源更新调用。
	assert.Contains(t, fakeRuntime.Called, "UpdateContainerResources")
}

// TestMakeMountsBindMountOptions 验证挂载选项只传递给声明了 BindMountOptions 的挂载点。
func TestMakeMountsBindMountOptions(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	opts := &kubecontainer.RunContainerOptions{
		Mounts: []kubecontainer.Mount{
			{
				Name:             "vol",
				ContainerPath:    "/mnt",
				HostPath:         "/host/path",
				BindMountOptions: []string{"noexec", "nosuid"},
			},
			{
				Name:          "vol2",
				ContainerPath: "/mnt2",
				HostPath:      "/host/path2",
			},
		},
	}
	container := &v1.Container{Name: "test"}

	result := m.makeMounts(opts, container)

	assert.Len(t, result, 2)
	assert.Equal(t, []string{"noexec", "nosuid"}, result[0].MountOptions)
	assert.Empty(t, result[1].MountOptions)
}
