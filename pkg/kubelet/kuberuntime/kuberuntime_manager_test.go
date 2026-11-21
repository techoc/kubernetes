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

package kuberuntime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"sort"
	"strings"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	apitest "k8s.io/cri-api/pkg/apis/testing"
	crierror "k8s.io/cri-api/pkg/errors"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	cmtesting "k8s.io/kubernetes/pkg/kubelet/cm/testing"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	imagetypes "k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

var (
	fakeCreatedAt                int64 = 1
	containerRestartPolicyAlways       = v1.ContainerRestartPolicyAlways
)

// testRuntimeManagerOptions 收集 createTestRuntimeManager 的可选参数。
type testRuntimeManagerOptions struct {
	errors   map[string][]error
	recorder *record.FakeRecorder
}

// testRuntimeManagerOption 是修改测试 runtime manager 选项的函数式选项。
type testRuntimeManagerOption func(*testRuntimeManagerOptions)

// withErrors 返回注入 fake runtime 错误的选项。
func withErrors(errors map[string][]error) testRuntimeManagerOption {
	return func(o *testRuntimeManagerOptions) {
		o.errors = errors
	}
}

// withRecorder 返回设置事件 recorder 的选项。
func withRecorder(recorder *record.FakeRecorder) testRuntimeManagerOption {
	return func(o *testRuntimeManagerOptions) {
		o.recorder = recorder
	}
}

// createTestRuntimeManager 创建带 fake runtime/image service 的
// kubeGenericRuntimeManager 及测试所需默认依赖。
func createTestRuntimeManager(ctx context.Context, opts ...testRuntimeManagerOption) (*apitest.FakeRuntimeService, *apitest.FakeImageService, *kubeGenericRuntimeManager, error) {
	options := &testRuntimeManagerOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.recorder == nil {
		options.recorder = &record.FakeRecorder{}
	}

	fakeRuntimeService := apitest.NewFakeRuntimeService()
	if options.errors != nil {
		fakeRuntimeService.Errors = options.errors
	}
	fakeImageService := apitest.NewFakeImageService()
	// 单测容器均为 BestEffort，machineInfo 只需提供非零内存容量。
	// Only an empty machineInfo is needed here, because in unit test all containers are besteffort,
	// data in machineInfo is not used. If burstable containers are used in unit test in the future,
	// we may want to set memory capacity.
	memoryCapacityQuantity := resource.MustParse(fakeNodeAllocatableMemory)
	machineInfo := &cadvisorapi.MachineInfo{
		MemoryCapacity: uint64(memoryCapacityQuantity.Value()),
	}
	osInterface := &containertest.FakeOS{}
	manager, err := newFakeKubeRuntimeManager(ctx, fakeRuntimeService, fakeImageService, machineInfo, osInterface, &containertest.FakeRuntimeHelper{}, noopoteltrace.NewTracerProvider().Tracer(""), options.recorder)
	return fakeRuntimeService, fakeImageService, manager, err
}

// sandboxTemplate 是用于创建 fake sandbox 的模板。
type sandboxTemplate struct {
	pod         *v1.Pod
	attempt     uint32
	createdAt   int64
	state       runtimeapi.PodSandboxState
	running     bool
	terminating bool
}

// containerTemplate 是用于创建 fake container 的模板。
type containerTemplate struct {
	pod            *v1.Pod
	container      *v1.Container
	sandboxAttempt uint32
	attempt        int
	createdAt      int64
	state          runtimeapi.ContainerState
}

// makeAndSetFakePod 为 pod 创建一个 fake sandbox，并为其每个容器创建一个 fake container。
func makeAndSetFakePod(tCtx ktesting.TContext, m *kubeGenericRuntimeManager, fakeRuntime *apitest.FakeRuntimeService,
	pod *v1.Pod,
) (*apitest.FakePodSandbox, []*apitest.FakeContainer) {
	// 使用与真实 SyncPod 相同的配置生成逻辑，保证 fake 对象 metadata 一致。
	sandbox := makeFakePodSandbox(tCtx, m, sandboxTemplate{
		pod:       pod,
		createdAt: fakeCreatedAt,
		state:     runtimeapi.PodSandboxState_SANDBOX_READY,
	})

	var containers []*apitest.FakeContainer
	newTemplate := func(c *v1.Container) containerTemplate {
		return containerTemplate{
			pod:       pod,
			container: c,
			createdAt: fakeCreatedAt,
			state:     runtimeapi.ContainerState_CONTAINER_RUNNING,
		}
	}
	// 通过 podutil 遍历所有已启用特性下的容器，包含 init 与 ephemeral。
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		containers = append(containers, makeFakeContainer(tCtx, m, newTemplate(c)))
		return true
	})

	fakeRuntime.SetFakeSandboxes([]*apitest.FakePodSandbox{sandbox})
	fakeRuntime.SetFakeContainers(containers)
	return sandbox, containers
}

// makeFakePodSandbox 基于 sandbox 模板创建 fake pod sandbox。
func makeFakePodSandbox(tCtx ktesting.TContext, m *kubeGenericRuntimeManager, template sandboxTemplate) *apitest.FakePodSandbox {
	t := tCtx.TB()
	config, err := m.generatePodSandboxConfig(tCtx, template.pod, template.attempt)
	assert.NoError(t, err, "generatePodSandboxConfig for sandbox template %+v", template)

	podSandboxID := apitest.BuildSandboxName(config.Metadata)
	podSandBoxStatus := &apitest.FakePodSandbox{
		PodSandboxStatus: runtimeapi.PodSandboxStatus{
			Id:        podSandboxID,
			Metadata:  config.Metadata,
			State:     template.state,
			CreatedAt: template.createdAt,
			Network: &runtimeapi.PodSandboxNetworkStatus{
				Ip: apitest.FakePodSandboxIPs[0],
			},
			Labels: config.Labels,
		},
	}
	// assign additional IPs
	// fake sandbox 支持多 IP，用于覆盖双栈场景。
	additionalIPs := apitest.FakePodSandboxIPs[1:]
	additionalPodIPs := make([]*runtimeapi.PodIP, 0, len(additionalIPs))
	for _, ip := range additionalIPs {
		additionalPodIPs = append(additionalPodIPs, &runtimeapi.PodIP{
			Ip: ip,
		})
	}
	if len(additionalPodIPs) > 0 {
		podSandBoxStatus.Network.AdditionalIps = additionalPodIPs
	}
	return podSandBoxStatus
}

// makeFakePodSandboxes 基于模板批量创建 fake pod sandbox，
// 并保证其顺序与模板一致。
func makeFakePodSandboxes(tCtx ktesting.TContext, m *kubeGenericRuntimeManager, templates []sandboxTemplate) []*apitest.FakePodSandbox {
	var fakePodSandboxes []*apitest.FakePodSandbox
	for _, template := range templates {
		fakePodSandboxes = append(fakePodSandboxes, makeFakePodSandbox(tCtx, m, template))
	}
	return fakePodSandboxes
}

// makeFakeContainer 基于容器模板创建 fake container。
func makeFakeContainer(tCtx ktesting.TContext, m *kubeGenericRuntimeManager, template containerTemplate) *apitest.FakeContainer {
	t := tCtx.TB()
	sandboxConfig, err := m.generatePodSandboxConfig(tCtx, template.pod, template.sandboxAttempt)
	assert.NoError(t, err, "generatePodSandboxConfig for container template %+v", template)

	containerConfig, _, err := m.generateContainerConfig(tCtx, template.container, template.pod, template.attempt, "", template.container.Image, []string{}, nil, nil)
	assert.NoError(t, err, "generateContainerConfig for container template %+v", template)

	podSandboxID := apitest.BuildSandboxName(sandboxConfig.Metadata)
	containerID := apitest.BuildContainerName(containerConfig.Metadata, podSandboxID)
	imageRef := containerConfig.Image.Image
	return &apitest.FakeContainer{
		ContainerStatus: runtimeapi.ContainerStatus{
			Id:          containerID,
			Metadata:    containerConfig.Metadata,
			Image:       containerConfig.Image,
			ImageRef:    imageRef,
			CreatedAt:   template.createdAt,
			State:       template.state,
			Labels:      containerConfig.Labels,
			Annotations: containerConfig.Annotations,
			LogPath:     filepath.Join(sandboxConfig.GetLogDirectory(), containerConfig.GetLogPath()),
		},
		SandboxID: podSandboxID,
	}
}

// makeFakeContainers 基于模板批量创建 fake container，
// 并保证其顺序与模板一致。
func makeFakeContainers(tCtx ktesting.TContext, m *kubeGenericRuntimeManager, templates []containerTemplate) []*apitest.FakeContainer {
	var fakeContainers []*apitest.FakeContainer
	for _, template := range templates {
		fakeContainers = append(fakeContainers, makeFakeContainer(tCtx, m, template))
	}
	return fakeContainers
}

// makeTestContainer 创建一个测试用 API 容器。
func makeTestContainer(name, image string) v1.Container {
	return v1.Container{
		Name:  name,
		Image: image,
	}
}

// makeTestPod 创建一个测试用 API pod。
func makeTestPod(podName, podNamespace, podUID string, containers []v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(podUID),
			Name:      podName,
			Namespace: podNamespace,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

// verifyFakeContainerList 比较 fake runtime 中的容器 ID 集合与期望集合。
func verifyFakeContainerList(fakeRuntime *apitest.FakeRuntimeService, expected sets.Set[string]) (sets.Set[string], bool) {
	actual := sets.New[string]()
	for _, c := range fakeRuntime.Containers {
		actual.Insert(c.Id)
	}
	return actual, actual.Equal(expected)
}

// cRecord 只保留测试关心的容器字段。
type cRecord struct {
	name    string
	attempt uint32
	state   runtimeapi.ContainerState
}

// cRecordList 实现 sort.Interface，按名称与 attempt 排序。
type cRecordList []*cRecord

// Len 返回记录数量。
func (b cRecordList) Len() int { return len(b) }

// Swap 交换下标 i 与 j 的记录。
func (b cRecordList) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less 先按容器名、再按 attempt 排序。
func (b cRecordList) Less(i, j int) bool {
	if b[i].name != b[j].name {
		return b[i].name < b[j].name
	}
	return b[i].attempt < b[j].attempt
}

// verifyContainerStatuses 比较 fake runtime 容器的 name/attempt/state，忽略顺序。
// 排序后比较，避免 fake runtime 内部顺序影响断言。
func verifyContainerStatuses(t *testing.T, runtime *apitest.FakeRuntimeService, expected []*cRecord, desc string) {
	actual := []*cRecord{}
	for _, cStatus := range runtime.Containers {
		actual = append(actual, &cRecord{name: cStatus.Metadata.Name, attempt: cStatus.Metadata.Attempt, state: cStatus.State})
	}
	sort.Sort(cRecordList(expected))
	sort.Sort(cRecordList(actual))
	assert.Equal(t, expected, actual, desc)
}

// TestNewKubeRuntimeManager 验证 runtime manager 能正常构造。
func TestNewKubeRuntimeManager(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 构造过程不应返回错误。
	_, _, _, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)
}

// TestVersion 验证 manager 返回与 CRI API 版本一致的版本号。
func TestVersion(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	version, err := m.Version(tCtx)
	assert.NoError(t, err)
	assert.Equal(t, kubeRuntimeAPIVersion, version.String())
}

// TestContainerRuntimeType 验证 manager 报告 fake runtime 的类型。
func TestContainerRuntimeType(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	runtimeType := m.Type()
	assert.Equal(t, apitest.FakeRuntimeName, runtimeType)
}

// TestGetPodStatus 验证由 CRI sandbox/容器还原出的 PodStatus 字段。
func TestGetPodStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	// 准备两个应用容器，创建 fake 对象后按 UID 查询状态。
	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// Set fake sandbox and faked containers to fakeRuntime.
	// 先把 sandbox 与容器写入 fake runtime。
	makeAndSetFakePod(tCtx, m, fakeRuntime, pod)

	// GetPod 与 GetPodStatus 应把 CRI 对象还原为 kubecontainer 视图。
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	assert.Equal(t, pod.UID, podStatus.ID)
	assert.Equal(t, pod.Name, podStatus.Name)
	assert.Equal(t, pod.Namespace, podStatus.Namespace)
	assert.Equal(t, apitest.FakePodSandboxIPs, podStatus.IPs)
}

// TestStopContainerWithNotFoundError 验证 StopContainer 返回 NotFound 时 KillPod 仍成功。
func TestStopContainerWithNotFoundError(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// Set fake sandbox and faked containers to fakeRuntime.
	makeAndSetFakePod(tCtx, m, fakeRuntime, pod)
	// 注入 NotFound，KillPod 应把它当作容器已不存在处理。
	fakeRuntime.InjectError("StopContainer", status.Error(codes.NotFound, "No such container"))
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	p := kubecontainer.ConvertPodStatusToRunningPod("", podStatus)
	gracePeriod := int64(1)
	err = m.KillPod(tCtx, pod, p, &gracePeriod)
	require.NoError(t, err)
}

// TestGetPodStatusWithNotFoundError 验证 ContainerStatus 返回 NotFound 时状态查询不失败。
func TestGetPodStatusWithNotFoundError(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// Set fake sandbox and faked containers to fakeRuntime.
	makeAndSetFakePod(tCtx, m, fakeRuntime, pod)
	// ContainerStatus 的 NotFound 不应导致整个状态查询失败。
	fakeRuntime.InjectError("ContainerStatus", status.Error(codes.NotFound, "No such container"))
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	require.Equal(t, pod.UID, podStatus.ID)
	require.Equal(t, pod.Name, podStatus.Name)
	require.Equal(t, pod.Namespace, podStatus.Namespace)
	require.Equal(t, apitest.FakePodSandboxIPs, podStatus.IPs)
}

// TestGetPods 验证 GetPods/GetPod 返回的 pod 视图与不存在的错误。
func TestGetPods(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.SyncTest("", testGetPods)
}

// testGetPods 是 TestGetPods 的实际执行体。
func testGetPods(tCtx ktesting.TContext) {
	t := tCtx.TB()
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "foo1",
					Image: "busybox",
				},
				{
					Name:  "foo2",
					Image: "busybox",
				},
			},
		},
	}

	// Set fake sandbox and fake containers to fakeRuntime.
	fakeSandbox, fakeContainers := makeAndSetFakePod(tCtx, m, fakeRuntime, pod)

	// Convert the fakeContainers to kubecontainer.Container
	// 把 fake 容器转回 kubecontainer 类型，作为期望值。
	containers := make([]*kubecontainer.Container, len(fakeContainers))
	for i := range containers {
		fakeContainer := fakeContainers[i]
		c, err := m.toKubeContainer(tCtx, &runtimeapi.Container{
			Id:           fakeContainer.Id,
			Metadata:     fakeContainer.Metadata,
			State:        fakeContainer.State,
			Image:        fakeContainer.Image,
			ImageRef:     fakeContainer.ImageRef,
			ImageId:      fakeContainer.ImageId,
			Labels:       fakeContainer.Labels,
			Annotations:  fakeContainer.Annotations,
			PodSandboxId: fakeSandbox.Id,
			CreatedAt:    fakeCreatedAt,
		})
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		containers[i] = c
	}
	// Convert fakeSandbox to kubecontainer.Container
	sandbox, err := m.sandboxToKubeContainer(&runtimeapi.PodSandbox{
		Id:          fakeSandbox.Id,
		Metadata:    fakeSandbox.Metadata,
		State:       fakeSandbox.State,
		CreatedAt:   fakeSandbox.CreatedAt,
		Labels:      fakeSandbox.Labels,
		Annotations: fakeSandbox.Annotations,
	})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	expectedPod := &kubecontainer.Pod{
		ID:         types.UID("12345678"),
		Name:       "foo",
		Namespace:  "new",
		CreatedAt:  uint64(fakeSandbox.CreatedAt),
		Containers: []*kubecontainer.Container{containers[0], containers[1]},
		Sandboxes:  []*kubecontainer.Container{sandbox},
		Timestamp:  time.Now(),
	}
	expected := []*kubecontainer.Pod{expectedPod}

	// 同时验证列表查询与单 pod 查询路径。
	actual, err := m.GetPods(tCtx, false)
	require.NoError(t, err)

	assert.ElementsMatch(t, expected, actual)

	actualPod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	assert.Equal(t, expectedPod, actualPod)

	_, err = m.GetPod(tCtx, "non-existent-uid")
	assert.ErrorIs(t, err, kubecontainer.ErrPodNotFound)
}

// TestGetPodsSorted 验证 GetPods 按创建时间倒序返回。
func TestGetPodsSorted(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"}}

	// 不同创建时间的 sandbox 用于验证排序。
	createdTimestamps := []uint64{10, 5, 20}
	fakeSandboxes := []*apitest.FakePodSandbox{}
	for i, createdAt := range createdTimestamps {
		pod.UID = types.UID(fmt.Sprint(i))
		fakeSandboxes = append(fakeSandboxes, makeFakePodSandbox(tCtx, m, sandboxTemplate{
			pod:       pod,
			createdAt: int64(createdAt),
			state:     runtimeapi.PodSandboxState_SANDBOX_READY,
		}))
	}
	fakeRuntime.SetFakeSandboxes(fakeSandboxes)

	actual, err := m.GetPods(tCtx, false)
	assert.NoError(t, err)

	assert.Len(t, actual, 3)

	// Verify that the pods are sorted by their creation time (newest/biggest timestamp first)
	// GetPods 必须按创建时间从新到旧返回。
	assert.Equal(t, uint64(createdTimestamps[2]), actual[0].CreatedAt)
	assert.Equal(t, uint64(createdTimestamps[0]), actual[1].CreatedAt)
	assert.Equal(t, uint64(createdTimestamps[1]), actual[2].CreatedAt)
}

// TestKillPod 验证 KillPod 会停止全部容器并关闭 sandbox。
func TestKillPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "foo1",
					Image: "busybox",
				},
				{
					Name:  "foo2",
					Image: "busybox",
				},
			},
			EphemeralContainers: []v1.EphemeralContainer{
				{
					EphemeralContainerCommon: v1.EphemeralContainerCommon{
						Name:  "debug",
						Image: "busybox",
					},
				},
			},
		},
	}

	// Set fake sandbox and fake containers to fakeRuntime.
	fakeSandbox, fakeContainers := makeAndSetFakePod(tCtx, m, fakeRuntime, pod)

	// Convert the fakeContainers to kubecontainer.Container
	// 先转换 fake 容器，再转换 sandbox，作为期望的 kubecontainer.Pod。
	containers := make([]*kubecontainer.Container, len(fakeContainers))
	for i := range containers {
		fakeContainer := fakeContainers[i]
		c, err := m.toKubeContainer(tCtx, &runtimeapi.Container{
			Id:       fakeContainer.Id,
			Metadata: fakeContainer.Metadata,
			State:    fakeContainer.State,
			Image:    fakeContainer.Image,
			ImageRef: fakeContainer.ImageRef,
			Labels:   fakeContainer.Labels,
		})
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		containers[i] = c
	}
	runningPod := kubecontainer.Pod{
		ID:         pod.UID,
		Name:       pod.Name,
		Namespace:  pod.Namespace,
		Containers: []*kubecontainer.Container{containers[0], containers[1], containers[2]},
		Sandboxes: []*kubecontainer.Container{
			{
				ID: kubecontainer.ContainerID{
					ID:   fakeSandbox.Id,
					Type: apitest.FakeRuntimeName,
				},
			},
		},
	}

	// KillPod 之后 sandbox 应变为 NOTREADY，容器应为 EXITED。
	err = m.KillPod(tCtx, pod, runningPod, nil)
	assert.NoError(t, err)
	assert.Len(t, fakeRuntime.Containers, 3)
	assert.Len(t, fakeRuntime.Sandboxes, 1)
	for _, sandbox := range fakeRuntime.Sandboxes {
		assert.Equal(t, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, sandbox.State)
	}
	for _, c := range fakeRuntime.Containers {
		assert.Equal(t, runtimeapi.ContainerState_CONTAINER_EXITED, c.State)
	}
}

// TestSyncPod 验证首次同步会创建 sandbox、拉取镜像并启动容器。
func TestSyncPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, fakeImage, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "alpine",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// 首次同步应创建 sandbox、拉取镜像并启动容器。
	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	result := m.SyncPod(tCtx, pod, &kubecontainer.PodStatus{}, []v1.Secret{}, backOff, false)
	assert.NoError(t, result.Error())
	assert.Len(t, fakeRuntime.Containers, 2)
	assert.Len(t, fakeImage.Images, 2)
	assert.Len(t, fakeRuntime.Sandboxes, 1)
	for _, sandbox := range fakeRuntime.Sandboxes {
		assert.Equal(t, runtimeapi.PodSandboxState_SANDBOX_READY, sandbox.State)
	}
	for _, c := range fakeRuntime.Containers {
		assert.Equal(t, runtimeapi.ContainerState_CONTAINER_RUNNING, c.State)
	}
}

// TestSyncPodWithConvertedPodSysctls 验证 SyncPod 把 sysctl 名称由斜杠格式转换为点格式。
func TestSyncPodWithConvertedPodSysctls(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	containers := []v1.Container{
		{
			Name:            "foo",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}

	securityContext := &v1.PodSecurityContext{
		Sysctls: []v1.Sysctl{
			{
				Name:  "kernel/shm_rmid_forced",
				Value: "1",
			},
			{
				Name:  "net/ipv4/ip_local_port_range",
				Value: "1024 65535",
			},
		},
	}
	exceptSysctls := []v1.Sysctl{
		{
			Name:  "kernel.shm_rmid_forced",
			Value: "1",
		},
		{
			Name:  "net.ipv4.ip_local_port_range",
			Value: "1024 65535",
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers:      containers,
			SecurityContext: securityContext,
		},
	}

	// 验证 SyncPod 会把斜杠分隔的 sysctl 规范化为点格式。
	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	result := m.SyncPod(tCtx, pod, &kubecontainer.PodStatus{}, []v1.Secret{}, backOff, false)
	assert.NoError(t, result.Error())
	assert.Equal(t, exceptSysctls, pod.Spec.SecurityContext.Sysctls)
	for _, sandbox := range fakeRuntime.Sandboxes {
		assert.Equal(t, runtimeapi.PodSandboxState_SANDBOX_READY, sandbox.State)
	}
	for _, c := range fakeRuntime.Containers {
		assert.Equal(t, runtimeapi.ContainerState_CONTAINER_RUNNING, c.State)
	}
}

// TestPruneInitContainers 验证每个 init 容器只保留最新 attempt。
func TestPruneInitContainers(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	init1 := makeTestContainer("init1", "busybox")
	init2 := makeTestContainer("init2", "busybox")
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{init1, init2},
		},
	}

	// 同一 init 容器存在多个 attempt，只有最新 attempt 应保留。
	templates := []containerTemplate{
		{pod: pod, container: &init1, attempt: 3, createdAt: 3, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{pod: pod, container: &init1, attempt: 2, createdAt: 2, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{pod: pod, container: &init2, attempt: 1, createdAt: 1, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{pod: pod, container: &init1, attempt: 1, createdAt: 1, state: runtimeapi.ContainerState_CONTAINER_UNKNOWN},
		{pod: pod, container: &init2, attempt: 0, createdAt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{pod: pod, container: &init1, attempt: 0, createdAt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
	}
	// 用 fake helper 生成容器状态，避免手工构造 metadata。
	fakes := makeFakeContainers(tCtx, m, templates)
	fakeRuntime.SetFakeContainers(fakes)
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)

	m.pruneInitContainersBeforeStart(tCtx, pod, podStatus)
	expectedContainers := sets.New[string](fakes[0].Id, fakes[2].Id)
	if actual, ok := verifyFakeContainerList(fakeRuntime, expectedContainers); !ok {
		t.Errorf("expected %v, got %v", expectedContainers, actual)
	}
}

// TestSyncPodWithInitContainers 验证 init 完成前不启动应用容器，sandbox 重建时重跑 init。
func TestSyncPodWithInitContainers(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	initContainers := []v1.Container{
		{
			Name:            "init1",
			Image:           "init",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "alpine",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers:     containers,
			InitContainers: initContainers,
		},
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)

	// 1. should only create the init container.
	// 分阶段验证 init 生命周期：先 init，再应用容器。
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	if err != nil {
		runtimePod = &kubecontainer.Pod{
			ID:        pod.UID,
			Name:      pod.Name,
			Namespace: pod.Namespace,
		}
	}
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result := m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected := []*cRecord{
		{name: initContainers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "start only the init container")

	// 2. should not create app container because init container is still running.
	// init 尚未完成时再次同步不应创建应用容器。
	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	verifyContainerStatuses(t, fakeRuntime, expected, "init container still running; do nothing")

	// 3. should create all app containers because init container finished.
	// Stop init container instance 0.
	// 停止 init 容器，模拟其成功退出。
	sandboxIDs, err := m.getSandboxIDByPodUID(tCtx, pod.UID)
	require.NoError(t, err)
	sandboxID := sandboxIDs[0]
	initID0, err := fakeRuntime.GetContainerID(sandboxID, initContainers[0].Name, 0)
	require.NoError(t, err)
	err = fakeRuntime.StopContainer(tCtx, initID0, 0)
	require.NoError(t, err)
	// Sync again.
	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected = []*cRecord{
		{name: initContainers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{name: containers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
		{name: containers[1].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "init container completed; all app containers should be running")

	// 4. should restart the init container if needed to create a new podsandbox
	// Stop the pod sandbox.
	// sandbox 重建后 init 容器必须重跑。
	err = fakeRuntime.StopPodSandbox(tCtx, sandboxID)
	require.NoError(t, err)
	// Sync again.
	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected = []*cRecord{
		// The first init container instance is purged and no longer visible.
		// The second (attempt == 1) instance has been started and is running.
		{name: initContainers[0].Name, attempt: 1, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
		// All containers are killed.
		{name: containers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{name: containers[1].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "kill all app containers, purge the existing init container, and restart a new one")
}

// TestSyncPodWithRestartAllContainers 验证 RestartAllContainers 规则触发后的清理与
// 重新启动顺序。
func TestSyncPodWithRestartAllContainers(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerRestartRules:                true,
		features.NodeDeclaredFeatures:                 true,
		features.RestartAllContainersOnContainerExits: true,
	})
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	restartPolicyAlways := v1.ContainerRestartPolicyAlways
	initContainers := []v1.Container{
		{
			Name:            "init1",
			Image:           "init",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
		{
			Name:            "foo2",
			Image:           "alpine",
			ImagePullPolicy: v1.PullIfNotPresent,
			RestartPolicy:   &restartPolicyAlways,
			RestartPolicyRules: []v1.ContainerRestartRule{
				{
					Action: v1.ContainerRestartRuleActionRestartAllContainers,
					ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
						Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
						Values:   []int32{42},
					},
				},
			},
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers:     containers,
			InitContainers: initContainers,
		},
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)

	// 1. Run the pod first. First SyncPod should execute the init container.
	// 分阶段触发 RestartAllContainers，验证清理与重新启动。
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	if err != nil {
		runtimePod = &kubecontainer.Pod{
			ID:        pod.UID,
			Name:      pod.Name,
			Namespace: pod.Namespace,
		}
	}
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result := m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected := []*cRecord{
		{name: initContainers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "start only the init container")

	// 2. should run all app containers because init container finished.
	// Stop init container instance 0.
	sandboxIDs, err := m.getSandboxIDByPodUID(tCtx, pod.UID)
	require.NoError(t, err)
	sandboxID := sandboxIDs[0]
	initID0, err := fakeRuntime.GetContainerID(sandboxID, initContainers[0].Name, 0)
	require.NoError(t, err)
	err = fakeRuntime.StopContainer(tCtx, initID0, 0)
	require.NoError(t, err)
	// Sync again.
	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected = []*cRecord{
		{name: initContainers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
		{name: containers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
		{name: containers[1].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "init container completed; all app containers should be running")

	// 3. Exits the container foo2 with code 42, the pod should be marked for RestartAllContainers, and
	// should remove all containers.
	// 让 foo2 以 42 退出，命中 RestartAllContainers 规则。
	sandboxIDs, err = m.getSandboxIDByPodUID(tCtx, pod.UID)
	require.NoError(t, err)
	sandboxID = sandboxIDs[0]
	foo2ID, err := fakeRuntime.GetContainerID(sandboxID, containers[1].Name, 0)
	require.NoError(t, err)
	failedFoo2 := fakeRuntime.Containers[foo2ID]
	failedFoo2.State = runtimeapi.ContainerState_CONTAINER_EXITED
	failedFoo2.ExitCode = 42
	fakeRuntime.Containers[foo2ID] = failedFoo2

	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, true)
	require.NoError(t, result.Error())
	expected = []*cRecord{}
	verifyContainerStatuses(t, fakeRuntime, expected, "kill all containers")

	// 4. Unmark the pod. Now it should start the init container first.
	// 清理标记后应重新从 init 容器开始。
	runtimePod, err = m.GetPod(tCtx, pod.UID)
	require.NoError(t, err)
	podStatus, err = m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())
	expected = []*cRecord{
		{name: initContainers[0].Name, attempt: 0, state: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}
	verifyContainerStatuses(t, fakeRuntime, expected, "start only the init container")
}

// TestSyncPodNoEventsInSteadyState 验证稳态同步不会产生事件。
func TestSyncPodNoEventsInSteadyState(t *testing.T) {
	simplecontainer := v1.Container{
		Name:            "foo1",
		Image:           "busybox",
		ImagePullPolicy: v1.PullIfNotPresent,
	}

	type testCase struct {
		name    string
		podspec v1.PodSpec
	}

	testCases := []testCase{
		{
			name: "simple pod",
			podspec: v1.PodSpec{
				Containers: []v1.Container{simplecontainer},
			},
		},
		{
			name: "pod with image volume",
			podspec: v1.PodSpec{
				Containers: []v1.Container{simplecontainer},
				Volumes: []v1.Volume{
					{
						Name: "image-volume",
						VolumeSource: v1.VolumeSource{
							Image: &v1.ImageVolumeSource{Reference: "image1:latest", PullPolicy: v1.PullAlways},
						},
					},
				},
			},
		},
	}

	consumeEvents := func(recorder *record.FakeRecorder) []string {
		var events []string
		for {
			select {
			case ev := <-recorder.Events:
				events = append(events, ev)
			default:
				return events
			}
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a recorder with a large enough buffer to hold all events generated during a single sync
			// 使用大缓冲 recorder，避免首次 sync 的事件阻塞。
			recorder := record.NewFakeRecorder(100)

			tCtx := ktesting.Init(t)
			_, _, m, err := createTestRuntimeManager(tCtx, withRecorder(recorder))
			require.NoError(t, err)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       "12345678",
					Name:      "foo",
					Namespace: "new",
				},
				Spec: tc.podspec,
			}

			// Sync the pod and store the resulting pod status.
			backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
			result := m.SyncPod(tCtx, pod, &kubecontainer.PodStatus{}, []v1.Secret{}, backOff, false)
			require.NoError(t, result.Error())

			runtimePod, err := m.GetPod(tCtx, pod.UID)
			require.NoError(t, err)
			podStatus, err := m.GetPodStatus(tCtx, runtimePod)
			require.NoError(t, err)

			// We need to consume events from the initial sync, but their content is not important.
			// 首次同步的事件内容不重要，清空缓冲以便观察稳态。
			events := consumeEvents(recorder)
			t.Logf("recorded events during initial sync: %v", events)

			// Sync again, passing in pod status from the previous sync. This should be steady state.
			result = m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
			require.NoError(t, result.Error())

			events = consumeEvents(recorder)
			assert.Empty(t, events, "no events expected during steady state")
		})
	}
}

// makeBasePodAndStatus 返回一个基础 pod 及其状态，
// 假设 sandbox 与所有容器都在运行且就绪。
func makeBasePodAndStatus() (*v1.Pod, *kubecontainer.PodStatus) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "foo-ns",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "foo1",
					Image: "busybox",
				},
				{
					Name:  "foo2",
					Image: "busybox",
				},
				{
					Name:  "foo3",
					Image: "busybox",
				},
			},
		},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					ContainerID: "://id1",
					Name:        "foo1",
					Image:       "busybox",
					State:       v1.ContainerState{Running: &v1.ContainerStateRunning{}},
				},
				{
					ContainerID: "://id2",
					Name:        "foo2",
					Image:       "busybox",
					State:       v1.ContainerState{Running: &v1.ContainerStateRunning{}},
				},
				{
					ContainerID: "://id3",
					Name:        "foo3",
					Image:       "busybox",
					State:       v1.ContainerState{Running: &v1.ContainerStateRunning{}},
				},
			},
		},
	}
	status := &kubecontainer.PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		SandboxStatuses: []*runtimeapi.PodSandboxStatus{
			{
				Id:       "sandboxID",
				State:    runtimeapi.PodSandboxState_SANDBOX_READY,
				Metadata: &runtimeapi.PodSandboxMetadata{Name: pod.Name, Namespace: pod.Namespace, Uid: "sandboxuid", Attempt: uint32(0)},
				Network:  &runtimeapi.PodSandboxNetworkStatus{Ip: "10.0.0.1"},
			},
		},
		ContainerStatuses: []*kubecontainer.Status{
			{
				ID:   kubecontainer.ContainerID{ID: "id1"},
				Name: "foo1", State: kubecontainer.ContainerStateRunning,
				Hash: kubecontainer.HashContainer(&pod.Spec.Containers[0]),
			},
			{
				ID:   kubecontainer.ContainerID{ID: "id2"},
				Name: "foo2", State: kubecontainer.ContainerStateRunning,
				Hash: kubecontainer.HashContainer(&pod.Spec.Containers[1]),
			},
			{
				ID:   kubecontainer.ContainerID{ID: "id3"},
				Name: "foo3", State: kubecontainer.ContainerStateRunning,
				Hash: kubecontainer.HashContainer(&pod.Spec.Containers[2]),
			},
		},
	}
	return pod, status
}

// TestComputePodActions 验证不同 restart policy 与容器状态下的启停决策。
func TestComputePodActions(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// Creating a pair reference pod and status for the test cases to refer
	// the specific fields.
	// 用一对基准 pod/status 作为各用例的修改起点。
	basePod, baseStatus := makeBasePodAndStatus()
	noAction := podActions{
		SandboxID:         baseStatus.SandboxStatuses[0].Id,
		ContainersToStart: []int{},
		ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
	}

	for desc, test := range map[string]struct {
		mutatePodFn    func(*v1.Pod)
		mutateStatusFn func(*kubecontainer.PodStatus)
		actions        podActions
		resetStatusFn  func(*kubecontainer.PodStatus)
	}{
		"everything is good; do nothing": {
			actions: noAction,
		},
		"start pod sandbox and all containers for a new pod": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// No container or sandbox exists.
				status.SandboxStatuses = []*runtimeapi.PodSandboxStatus{}
				status.ContainerStatuses = []*kubecontainer.Status{}
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     true,
				Attempt:           uint32(0),
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"restart exited containers if RestartPolicy == Always": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, restart it,
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container exited with failure, restart it,
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{0, 1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"restart failed containers if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, don't restart it,
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container exited with failure, restart it,
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"restart created but not started containers if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, don't restart it.
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container was created, but never started.
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateCreated
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"don't restart containers if RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// Don't restart any containers.
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: noAction,
		},
		"Kill pod and recreate everything if the pod sandbox is dead, and RestartPolicy == Always": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"Kill pod and recreate all containers (except for the succeeded one) if the pod sandbox is dead, and RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 0
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{0, 2},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"Kill pod and recreate all containers if the PodSandbox does not have an IP": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].Network.Ip = ""
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"Kill and recreate the container if the container's spec changed": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[1].Hash = uint64(432423432)
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{1}),
				ContainersToStart: []int{1},
			},
		},
		"Kill and recreate the container if the liveness check has failed": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				m.livenessManager.Set(status.ContainerStatuses[1].ID, proberesults.Failure, basePod)
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{1}),
				ContainersToStart: []int{1},
			},
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.livenessManager.Remove(status.ContainerStatuses[1].ID)
			},
		},
		"Kill and recreate the container if the startup check has failed": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				m.startupManager.Set(status.ContainerStatuses[1].ID, proberesults.Failure, basePod)
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{1}),
				ContainersToStart: []int{1},
			},
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.startupManager.Remove(status.ContainerStatuses[1].ID)
			},
		},
		"Verify we do not create a pod sandbox if no ready sandbox for pod with RestartPolicy=Never and all containers exited": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// no ready sandbox
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.SandboxStatuses[0].Metadata.Attempt = uint32(1)
				// all containers exited
				for i := range status.ContainerStatuses {
					status.ContainerStatuses[i].State = kubecontainer.ContainerStateExited
					status.ContainerStatuses[i].ExitCode = 0
				}
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(2),
				CreateSandbox:     false,
				KillPod:           true,
				ContainersToStart: []int{},
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
			},
		},
		"Verify we do not create a pod sandbox if no ready sandbox for pod with RestartPolicy=OnFailure and all containers succeeded": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// no ready sandbox
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.SandboxStatuses[0].Metadata.Attempt = uint32(1)
				// all containers succeeded
				for i := range status.ContainerStatuses {
					status.ContainerStatuses[i].State = kubecontainer.ContainerStateExited
					status.ContainerStatuses[i].ExitCode = 0
				}
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(2),
				CreateSandbox:     false,
				KillPod:           true,
				ContainersToStart: []int{},
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
			},
		},
		"Verify we create a pod sandbox if no ready sandbox for pod with RestartPolicy=Never and no containers have ever been created": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// no ready sandbox
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.SandboxStatuses[0].Metadata.Attempt = uint32(2)
				// no visible containers
				status.ContainerStatuses = []*kubecontainer.Status{}
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(3),
				CreateSandbox:     true,
				KillPod:           true,
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
			},
		},
		"Kill and recreate the container if the container is in unknown state": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateUnknown
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{1}),
				ContainersToStart: []int{1},
			},
		},
		"Restart the container if the container is in created state": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateCreated
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
				ContainersToStart: []int{1},
			},
		},
	} {
		// 每个用例都从基准对象重新构造，避免用例间相互污染。
		pod, status := makeBasePodAndStatus()
		if test.mutatePodFn != nil {
			test.mutatePodFn(pod)
		}
		if test.mutateStatusFn != nil {
			test.mutateStatusFn(status)
		}
		tCtx := ktesting.Init(t)
		// computePodActions 不应修改输入 pod/status。
		actions := m.computePodActions(tCtx, pod, status, false)
		verifyActions(t, &test.actions, &actions, desc)
		if test.resetStatusFn != nil {
			test.resetStatusFn(status)
		}
	}
}

// TestComputePodActionsForRestartAllContainers 验证 RestartAllContainers 条件下
// 普通容器与 init 容器的移除和重启决策。
func TestComputePodActionsForRestartAllContainers(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerRestartRules:                true,
		features.NodeDeclaredFeatures:                 true,
		features.RestartAllContainersOnContainerExits: true,
	})
	t.Run("TestComputePodActions", TestComputePodActions)
	t.Run("TestComputePodActionsWithInitContainers", TestComputePodActionsWithInitContainers)

	allContainersRestartingTrue := []v1.PodCondition{
		{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionTrue,
		},
	}
	allContainersRestartingFalse := []v1.PodCondition{
		{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionFalse,
		},
	}

	restartPolicyAlways := v1.ContainerRestartPolicyAlways
	restartPolicyNever := v1.ContainerRestartPolicyNever
	restartAllContainersRules := []v1.ContainerRestartRule{{
		Action: v1.ContainerRestartRuleActionRestartAllContainers,
		ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
			Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
			Values:   []int32{1},
		},
	}}

	for desc, test := range map[string]struct {
		podFunc               func() *v1.Pod
		podStatusFunc         func() *kubecontainer.PodStatus
		restartAllContainers  bool
		containersToRemove    []containerToRemoveInfo
		containersToStart     []int
		initContainersToStart []int
	}{
		"pod not marked for RestartAllContainers": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatus()
				pod.Status.Conditions = allContainersRestartingFalse
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatus()
				return status
			},
		},
		"pod marked for RestartAllContainers": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatus()
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatus()
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "foo1"}, containerID: kubecontainer.ContainerID{ID: "id1"}, kill: true},
				{container: &v1.Container{Name: "foo2"}, containerID: kubecontainer.ContainerID{ID: "id2"}, kill: true},
				{container: &v1.Container{Name: "foo3"}, containerID: kubecontainer.ContainerID{ID: "id3"}, kill: true},
			},
		},
		"pod marked for RestartAllContainers with init containers": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithInitContainers()
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatusWithInitContainers()
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "init1"}, containerID: kubecontainer.ContainerID{ID: "initid1"}},
				{container: &v1.Container{Name: "init2"}, containerID: kubecontainer.ContainerID{ID: "initid2"}},
				{container: &v1.Container{Name: "init3"}, containerID: kubecontainer.ContainerID{ID: "initid3"}},
			},
		},
		"pod marked for RestartAllContainers with restartable init containers": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithRestartableInitContainers()
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatusWithRestartableInitContainers()
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "restartable-init-1"}, containerID: kubecontainer.ContainerID{ID: "initid1"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-2"}, containerID: kubecontainer.ContainerID{ID: "initid2"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-3"}, containerID: kubecontainer.ContainerID{ID: "initid3"}, kill: true},
			},
		},
		"init container exit triggers RestartAllContainers": {
			// The init3 container exited and triggers RestartAllContainers,
			// the init2 container is a running sidecar. First, the init1
			// should be removed; second, the init2 containers should killed and
			// be removed; lastly, init3 container should be removed.
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithInitContainers()
				sidecar := pod.Spec.InitContainers[1]
				sidecar.RestartPolicy = &restartPolicyAlways
				pod.Spec.InitContainers[1] = sidecar
				source := pod.Spec.InitContainers[2]
				source.RestartPolicy = &restartPolicyNever
				source.RestartPolicyRules = restartAllContainersRules
				pod.Spec.InitContainers[2] = source
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatusWithInitContainers()
				sidecarStatus := status.ContainerStatuses[1]
				sidecarStatus.State = kubecontainer.ContainerStateRunning
				status.ContainerStatuses[1] = sidecarStatus
				sourceStatus := status.ContainerStatuses[2]
				sourceStatus.ExitCode = 1
				status.ContainerStatuses[2] = sourceStatus
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "init1"}, containerID: kubecontainer.ContainerID{ID: "initid1"}},
				{container: &v1.Container{Name: "init2"}, containerID: kubecontainer.ContainerID{ID: "initid2"}, kill: true},
				{container: &v1.Container{Name: "init3"}, containerID: kubecontainer.ContainerID{ID: "initid3"}},
			},
		},
		"sidecar container exit triggers RestartAllContainres": {
			// Restartable-init-3 fails and triggers RestartAllContainers.
			// The running foo1 should be killed and removed first; then init-1 and init-2
			// should be killed and removed; lastly init-3 should be removed.
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithRestartableInitContainers()
				source := pod.Spec.InitContainers[2]
				source.RestartPolicy = &restartPolicyAlways
				source.RestartPolicyRules = restartAllContainersRules
				pod.Spec.InitContainers[2] = source
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				pod, status := makeBasePodAndStatusWithRestartableInitContainers()
				sourceStatus := status.ContainerStatuses[2]
				sourceStatus.ExitCode = 1
				sourceStatus.State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2] = sourceStatus
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:  "foo1",
					State: kubecontainer.ContainerStateRunning,
					ID:    kubecontainer.ContainerID{ID: "id1"},
					Hash:  kubecontainer.HashContainer(&pod.Spec.Containers[0]),
				})
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "foo1"}, containerID: kubecontainer.ContainerID{ID: "id1"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-1"}, containerID: kubecontainer.ContainerID{ID: "initid1"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-2"}, containerID: kubecontainer.ContainerID{ID: "initid2"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-3"}, containerID: kubecontainer.ContainerID{ID: "initid3"}},
			},
		},
		"regular container exit triggers RestartAllContainers": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithRestartableInitContainers()
				source := pod.Spec.Containers[2]
				source.RestartPolicy = &restartPolicyNever
				source.RestartPolicyRules = restartAllContainersRules
				pod.Spec.Containers[2] = source
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatusWithRestartableInitContainers()
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:  "foo1",
					State: kubecontainer.ContainerStateRunning,
					ID:    kubecontainer.ContainerID{ID: "id1"},
				})
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:  "foo2",
					State: kubecontainer.ContainerStateRunning,
					ID:    kubecontainer.ContainerID{ID: "id2"},
				})
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:     "foo3",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 1,
					ID:       kubecontainer.ContainerID{ID: "id3"},
				})
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "foo1"}, containerID: kubecontainer.ContainerID{ID: "id1"}, kill: true},
				{container: &v1.Container{Name: "foo2"}, containerID: kubecontainer.ContainerID{ID: "id2"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-1"}, containerID: kubecontainer.ContainerID{ID: "initid1"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-2"}, containerID: kubecontainer.ContainerID{ID: "initid2"}, kill: true},
				{container: &v1.Container{Name: "restartable-init-3"}, containerID: kubecontainer.ContainerID{ID: "initid3"}, kill: true},
				{container: &v1.Container{Name: "foo3"}, containerID: kubecontainer.ContainerID{ID: "id3"}},
			},
		},
		"removes past terminated statuses": {
			// foo3 terminated and triggers restart all containers. All containers have started once at t0, and
			// restarted once at t1. All container statuses should be removed; foo3 should be removed last
			// because it triggered restart all containers.
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatus()
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
				source := pod.Spec.Containers[2]
				source.RestartPolicy = &restartPolicyAlways
				source.RestartPolicyRules = restartAllContainersRules
				pod.Spec.Containers[2] = source
				pod.Status.Conditions = allContainersRestartingTrue
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatus()
				t1 := time.Now()
				t0 := t1.Add(-time.Minute)
				status.ContainerStatuses[0].CreatedAt = t1
				status.ContainerStatuses[1].CreatedAt = t1
				status.ContainerStatuses[2] = &kubecontainer.Status{
					Name:      "foo3",
					State:     kubecontainer.ContainerStateExited,
					ExitCode:  1,
					ID:        kubecontainer.ContainerID{ID: "id3"},
					CreatedAt: t1,
				}
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:      "foo1",
					State:     kubecontainer.ContainerStateExited,
					ExitCode:  99,
					ID:        kubecontainer.ContainerID{ID: "id1-past"},
					CreatedAt: t0,
				})
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:      "foo2",
					State:     kubecontainer.ContainerStateExited,
					ExitCode:  99,
					ID:        kubecontainer.ContainerID{ID: "id2-past"},
					CreatedAt: t0,
				})
				status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
					Name:      "foo3",
					State:     kubecontainer.ContainerStateExited,
					ExitCode:  99,
					ID:        kubecontainer.ContainerID{ID: "id3-past"},
					CreatedAt: t0,
				})
				return status
			},
			restartAllContainers: true,
			containersToRemove: []containerToRemoveInfo{
				{container: &v1.Container{Name: "foo1"}, containerID: kubecontainer.ContainerID{ID: "id1"}, kill: true},
				{container: &v1.Container{Name: "foo1"}, containerID: kubecontainer.ContainerID{ID: "id1-past"}},
				{container: &v1.Container{Name: "foo2"}, containerID: kubecontainer.ContainerID{ID: "id2"}, kill: true},
				{container: &v1.Container{Name: "foo2"}, containerID: kubecontainer.ContainerID{ID: "id2-past"}},
				{container: &v1.Container{Name: "foo3"}, containerID: kubecontainer.ContainerID{ID: "id3-past"}},
				{container: &v1.Container{Name: "foo3"}, containerID: kubecontainer.ContainerID{ID: "id3"}},
			},
		},
		"all containers removed, start init container": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatusWithInitContainers()
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatus()
				// Remove all containers from PodStatus to simulate cleanup
				status.ContainerStatuses = []*kubecontainer.Status{}
				return status
			},
			initContainersToStart: []int{0},
		},
		"all containers removed, start regular container": {
			podFunc: func() *v1.Pod {
				pod, _ := makeBasePodAndStatus()
				return pod
			},
			podStatusFunc: func() *kubecontainer.PodStatus {
				_, status := makeBasePodAndStatus()
				// Remove all containers from PodStatus to simulate cleanup
				status.ContainerStatuses = []*kubecontainer.Status{}
				return status
			},
			containersToStart: []int{0, 1, 2},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod := test.podFunc()
			status := test.podStatusFunc()

			// Initialize the actuated resources.
			// 先初始化 actuated 资源，使 resize 用例有基准。
			m.InitializeActuatedPod(tCtx.Logger(), pod)

			actions := m.computePodActions(tCtx, pod, status, test.restartAllContainers)

			expected := &podActions{
				CreateSandbox:     false,
				KillPod:           false,
				SandboxID:         status.SandboxStatuses[0].Id,
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
				ContainersToStart: []int{},
			}
			if test.containersToStart != nil {
				expected.ContainersToStart = test.containersToStart
			}
			if test.initContainersToStart != nil {
				expected.InitContainersToStart = test.initContainersToStart
			}

			containerSpecByName := make(map[string]*v1.Container)
			for idx, c := range pod.Spec.Containers {
				containerSpecByName[c.Name] = &pod.Spec.Containers[idx]
			}
			for idx, c := range pod.Spec.InitContainers {
				containerSpecByName[c.Name] = &pod.Spec.InitContainers[idx]
			}
			for _, info := range test.containersToRemove {
				cName := info.container.Name
				info.container = containerSpecByName[cName]
				expected.ContainersToReset = append(expected.ContainersToReset, info)
			}

			verifyActions(t, expected, &actions, desc)
		})
	}
}

// getKillMap 根据普通容器下标构造期望的 kill map。
func getKillMap(pod *v1.Pod, status *kubecontainer.PodStatus, cIndexes []int) map[kubecontainer.ContainerID]containerToKillInfo {
	m := map[kubecontainer.ContainerID]containerToKillInfo{}
	for _, i := range cIndexes {
		m[status.ContainerStatuses[i].ID] = containerToKillInfo{
			container: &pod.Spec.Containers[i],
			name:      pod.Spec.Containers[i].Name,
		}
	}
	return m
}

// getKillMapWithInitContainers 根据 init 容器下标构造期望的 kill map。
func getKillMapWithInitContainers(pod *v1.Pod, status *kubecontainer.PodStatus, cIndexes []int) map[kubecontainer.ContainerID]containerToKillInfo {
	m := map[kubecontainer.ContainerID]containerToKillInfo{}
	for _, i := range cIndexes {
		m[status.ContainerStatuses[i].ID] = containerToKillInfo{
			container: &pod.Spec.InitContainers[i],
			name:      pod.Spec.InitContainers[i].Name,
		}
	}
	return m
}

// modifyKillMapContainerImage 更新 kill map 中的容器镜像，模拟 spec 变更。
func modifyKillMapContainerImage(containersToKill map[kubecontainer.ContainerID]containerToKillInfo, status *kubecontainer.PodStatus, cIndexes []int, imageNames []string) map[kubecontainer.ContainerID]containerToKillInfo {
	for idx, i := range cIndexes {
		containerKillInfo := containersToKill[status.ContainerStatuses[i].ID]
		updatedContainer := containerKillInfo.container.DeepCopy()
		updatedContainer.Image = imageNames[idx]
		containersToKill[status.ContainerStatuses[i].ID] = containerToKillInfo{
			container: updatedContainer,
			name:      containerKillInfo.name,
		}
	}
	return containersToKill
}

// verifyActions 清理不关心的字段后比较 podActions。
func verifyActions(t *testing.T, expected, actual *podActions, desc string) {
	if actual.ContainersToKill != nil {
		// Clear the message and reason fields since we don't need to verify them.
		for k, info := range actual.ContainersToKill {
			info.message = ""
			info.reason = ""
			actual.ContainersToKill[k] = info
		}
	}
	if expected.ContainersToUpdate == nil && actual.ContainersToUpdate != nil {
		// No need to distinguish empty and nil maps for the test.
		expected.ContainersToUpdate = map[v1.ResourceName][]containerToUpdateInfo{}
	}
	assert.Equal(t, expected, actual, desc)
}

// TestComputePodActionsWithInitContainers 验证 init 容器各阶段以及 resize 场景的决策。
func TestComputePodActionsWithInitContainers(t *testing.T) {
	cpu400m := resource.MustParse("400m")
	memory400Mi := resource.MustParse("400Mi")
	cpu800m := resource.MustParse("800m")
	memory800Mi := resource.MustParse("800Mi")

	// Creating a pair reference pod and status for the test cases to refer
	// the specific fields.
	// 用一对基准 pod/status 作为各用例的修改起点。
	basePod, baseStatus := makeBasePodAndStatusWithInitContainers()
	noAction := podActions{
		SandboxID:         baseStatus.SandboxStatuses[0].Id,
		ContainersToStart: []int{},
		ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
	}

	for desc, test := range map[string]struct {
		mutatePodFn          func(*v1.Pod)
		mutateStatusFn       func(*kubecontainer.PodStatus)
		actions              podActions
		disableIPPRInitCtrFG bool
		skipWindows          bool
	}{
		"initialization completed; start all containers": {
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"no init containers have been started; start the first one": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = nil
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"initialization in progress; do nothing": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateRunning
			},
			actions: noAction,
		},
		"Kill pod and restart the first init container if the pod sandbox is dead": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"initialization failed; restart the last init container if RestartPolicy == Always": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"initialization failed; restart the last init container if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"initialization failed; kill pod if RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				KillPod:           true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"init container state unknown; kill and recreate the last init container if RestartPolicy == Always": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateUnknown
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{2}),
			},
		},
		"init container state unknown; kill and recreate the last init container if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateUnknown
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{2}),
			},
		},
		"init container state unknown; kill pod if RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateUnknown
			},
			actions: podActions{
				KillPod:           true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, init container failed, but RestartPolicy == Never; kill pod only": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     false,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, and RestartPolicy == Never, but no visible init containers;  create a new pod sandbox": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses = []*kubecontainer.Status{}
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, init container failed, and RestartPolicy == OnFailure; create a new pod sandbox": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"some of the init container statuses are missing but the last init container is running, don't restart preceding ones": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateRunning
				status.ContainerStatuses = status.ContainerStatuses[2:]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				KillPod:           false,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"stale main container from a previous sandbox must not mark pod as initialized": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = []*kubecontainer.Status{
					{
						ID:    kubecontainer.ContainerID{ID: "id1"},
						Name:  "foo1",
						State: kubecontainer.ContainerStateCreated,
					},
				}
				status.ActiveContainerStatuses = nil
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"an init container is in the created state due to an unknown error when starting container; restart it": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateCreated
			},
			actions: podActions{
				KillPod:               false,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"resize request exists on a not yet running non-sidecar init container; start it": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.InitContainers[0].Resources.Requests = v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("200m"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				}
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = nil
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				KillPod:               false,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
				UpdatePodResources:    true,
			},
		},
		"resize of a running non-sidecar init container": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.InitContainers[0].Resources.Requests = v1.ResourceList{
					v1.ResourceCPU:    cpu800m,
					v1.ResourceMemory: memory800Mi,
				}
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:1]
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateRunning
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				KillPod:               false,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: nil,
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
				ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
					v1.ResourceMemory: {
						{
							kubeContainerID: baseStatus.ContainerStatuses[0].ID,
							desiredContainerResources: resourceRequirements{
								memoryRequest: memory800Mi.Value(),
								cpuRequest:    cpu800m.MilliValue(),
							},
							currentContainerResources: &resourceRequirements{
								memoryRequest: memory400Mi.Value(),
								cpuRequest:    cpu400m.MilliValue(),
							},
						},
					},
					v1.ResourceCPU: {
						{
							kubeContainerID: baseStatus.ContainerStatuses[0].ID,
							desiredContainerResources: resourceRequirements{
								memoryRequest: memory800Mi.Value(),
								cpuRequest:    cpu800m.MilliValue(),
							},
							currentContainerResources: &resourceRequirements{
								memoryRequest: memory400Mi.Value(),
								cpuRequest:    cpu400m.MilliValue(),
							},
						},
					},
				},
			},
			skipWindows: true, // Windows does not support resize.
		},
		"resize of a running non-sidecar init container with FG disabled": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.InitContainers[0].Resources.Requests = v1.ResourceList{
					v1.ResourceCPU:    cpu800m,
					v1.ResourceMemory: memory800Mi,
				}
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:1]
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateRunning
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				KillPod:               false,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: nil,
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
				ContainersToUpdate:    map[v1.ResourceName][]containerToUpdateInfo{},
			},
			skipWindows:          true, // Windows does not support resize.
			disableIPPRInitCtrFG: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			if test.skipWindows && goruntime.GOOS == "windows" {
				t.Skip("Skipping test since Windows does not support resize")
			}
			tCtx := ktesting.Init(t)

			if test.disableIPPRInitCtrFG {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingInitContainers, false)
			}

			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			// 初始化 actuated 状态，确保 resize 用例的当前值固定。
			pod, status := makeBasePodAndStatusWithInitContainers()

			// Sync the actuated state with the base values before any resize
			m.InitializeActuatedPod(tCtx.Logger(), pod)

			if test.actions.ContainersToUpdate != nil {
				for res := range test.actions.ContainersToUpdate {
					for i := range test.actions.ContainersToUpdate[res] {
						// Link the expected action to the mutated pod container
						test.actions.ContainersToUpdate[res][i].container = &pod.Spec.InitContainers[0]
					}
				}
			}

			if test.mutatePodFn != nil {
				test.mutatePodFn(pod)
			}
			if test.mutateStatusFn != nil {
				test.mutateStatusFn(status)
			}
			// verifyActions 会清理 message/reason 等无关字段。
			actions := m.computePodActions(tCtx, pod, status, false)
			verifyActions(t, &test.actions, &actions, desc)
		})
	}
}

// makeBasePodAndStatusWithInitContainers 在基础 pod/status 上加入三个 init 容器。
func makeBasePodAndStatusWithInitContainers() (*v1.Pod, *kubecontainer.PodStatus) {
	pod, status := makeBasePodAndStatus()
	pod.Spec.InitContainers = []v1.Container{
		{
			Name:  "init1",
			Image: "bar-image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("400m"),
					v1.ResourceMemory: resource.MustParse("400Mi"),
				},
			},
		},
		{
			Name:  "init2",
			Image: "bar-image",
		},
		{
			Name:  "init3",
			Image: "bar-image",
		},
	}
	// Replace the original statuses of the containers with those for the init
	// containers.
	status.ContainerStatuses = []*kubecontainer.Status{
		{
			ID:   kubecontainer.ContainerID{ID: "initid1"},
			Name: "init1", State: kubecontainer.ContainerStateExited,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[0]),
		},
		{
			ID:   kubecontainer.ContainerID{ID: "initid2"},
			Name: "init2", State: kubecontainer.ContainerStateExited,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[1]),
		},
		{
			ID:   kubecontainer.ContainerID{ID: "initid3"},
			Name: "init3", State: kubecontainer.ContainerStateExited,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[2]),
		},
	}
	status.ActiveContainerStatuses = status.ContainerStatuses
	return pod, status
}

// TestComputePodActionsWithRestartableInitContainers 验证 restartable init 容器
// 与探针状态下的启停决策。
func TestComputePodActionsWithRestartableInitContainers(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// Creating a pair reference pod and status for the test cases to refer
	// the specific fields.
	basePod, baseStatus := makeBasePodAndStatusWithRestartableInitContainers()
	noAction := podActions{
		SandboxID:         baseStatus.SandboxStatuses[0].Id,
		ContainersToStart: []int{},
		ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
	}

	for desc, test := range map[string]struct {
		mutatePodFn    func(*v1.Pod)
		mutateStatusFn func(*v1.Pod, *kubecontainer.PodStatus)
		actions        podActions
		resetStatusFn  func(*kubecontainer.PodStatus)
	}{
		"initialization completed; start all containers": {
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{0, 1, 2},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"no init containers have been started; start the first one": {
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = nil
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"an init container is stuck in the created state; restart it": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateCreated
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"restartable init container has started; start the next": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:1]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{1},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"livenessProbe has not been run; start the nothing": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.livenessManager.Remove(status.ContainerStatuses[1].ID)
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"livenessProbe in progress; start the next": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.livenessManager.Set(status.ContainerStatuses[1].ID, proberesults.Unknown, basePod)
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.livenessManager.Remove(status.ContainerStatuses[1].ID)
			},
		},
		"livenessProbe has completed; start the next": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"kill and recreate the restartable init container if the liveness check has failed": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.livenessManager.Set(status.ContainerStatuses[2].ID, proberesults.Failure, basePod)
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{2}),
				ContainersToStart:     []int{0, 1, 2},
			},
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.livenessManager.Remove(status.ContainerStatuses[2].ID)
			},
		},
		"startupProbe has not been run; do nothing": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.startupManager.Remove(status.ContainerStatuses[1].ID)
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: noAction,
		},
		"startupProbe in progress; do nothing": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.startupManager.Set(status.ContainerStatuses[1].ID, proberesults.Unknown, basePod)
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: noAction,
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.startupManager.Remove(status.ContainerStatuses[1].ID)
			},
		},
		"startupProbe has completed; start the next": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:2]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"kill and recreate the restartable init container if the startup check has failed": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
				pod.Spec.InitContainers[2].StartupProbe = &v1.Probe{}
			},
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				m.startupManager.Set(status.ContainerStatuses[2].ID, proberesults.Failure, basePod)
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{2}),
				ContainersToStart:     []int{},
			},
			resetStatusFn: func(status *kubecontainer.PodStatus) {
				m.startupManager.Remove(status.ContainerStatuses[2].ID)
			},
		},
		"kill and recreate the restartable init container if the container definition changes": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
				pod.Spec.InitContainers[2].Image = "foo-image"
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToKill:      modifyKillMapContainerImage(getKillMapWithInitContainers(basePod, baseStatus, []int{2}), baseStatus, []int{2}, []string{"foo-image"}),
				ContainersToStart:     []int{0, 1, 2},
			},
		},
		"restart terminated restartable init container and next init container": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0, 2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"restart terminated restartable init container and regular containers": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{0, 1, 2},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, restartable init container failed, but RestartPolicy == Never; kill pod only": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     false,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, and RestartPolicy == Never, but no visible restartable init containers;  create a new pod sandbox": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses = []*kubecontainer.Status{}
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, restartable init container failed, and RestartPolicy == OnFailure; create a new pod sandbox": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Pod sandbox not ready, restartable init container failed, and RestartPolicy == Always; create a new pod sandbox": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"initialization failed; restart the last restartable init container even if pod's RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].ExitCode = 137
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"restartable init container state unknown; kill and recreate the last restartable init container even if pod's RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateUnknown
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{2}),
			},
		},
		"restart restartable init container if regular containers are running even if pod's RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses[2].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[2].ExitCode = 137
				// all main containers are running
				for i := 1; i <= 3; i++ {
					status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
						ID:    kubecontainer.ContainerID{ID: fmt.Sprintf("id%d", i)},
						Name:  fmt.Sprintf("foo%d", i),
						State: kubecontainer.ContainerStateRunning,
						Hash:  kubecontainer.HashContainer(&pod.Spec.Containers[i-1]),
					})
				}
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{2},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"kill the pod if all main containers succeeded if pod's RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				// all main containers succeeded
				for i := 1; i <= 3; i++ {
					status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
						ID:       kubecontainer.ContainerID{ID: fmt.Sprintf("id%d", i)},
						Name:     fmt.Sprintf("foo%d", i),
						State:    kubecontainer.ContainerStateExited,
						ExitCode: 0,
						Hash:     kubecontainer.HashContainer(&pod.Spec.Containers[i-1]),
					})
				}
			},
			actions: podActions{
				KillPod:           true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{},
				ContainersToKill:  getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"some of the init container statuses are missing but the last init container is running, restart restartable init and regular containers": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[2:]
				status.ActiveContainerStatuses = status.ContainerStatuses
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0, 1},
				ContainersToStart:     []int{0, 1, 2},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"stale main container with mixed restartable and non-restartable init containers; start the first one": {
			mutatePodFn: func(pod *v1.Pod) {
				// Make the second init container non-restartable while keeping the others restartable.
				pod.Spec.InitContainers[1].RestartPolicy = nil
			},
			mutateStatusFn: func(pod *v1.Pod, status *kubecontainer.PodStatus) {
				status.ContainerStatuses = []*kubecontainer.Status{
					{
						ID:    kubecontainer.ContainerID{ID: "id1"},
						Name:  "foo1",
						State: kubecontainer.ContainerStateCreated,
					},
				}
				status.ActiveContainerStatuses = nil
			},
			actions: podActions{
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			// 预置探针成功结果，避免 probe 状态影响决策。
			pod, status := makeBasePodAndStatusWithRestartableInitContainers()
			m.livenessManager.Set(status.ContainerStatuses[1].ID, proberesults.Success, basePod)
			m.startupManager.Set(status.ContainerStatuses[1].ID, proberesults.Success, basePod)
			m.livenessManager.Set(status.ContainerStatuses[2].ID, proberesults.Success, basePod)
			m.startupManager.Set(status.ContainerStatuses[2].ID, proberesults.Success, basePod)
			if test.mutatePodFn != nil {
				test.mutatePodFn(pod)
			}
			if test.mutateStatusFn != nil {
				test.mutateStatusFn(pod, status)
			}
			tCtx := ktesting.Init(t)
			actions := m.computePodActions(tCtx, pod, status, false)
			verifyActions(t, &test.actions, &actions, desc)
			if test.resetStatusFn != nil {
				test.resetStatusFn(status)
			}
		})
	}
}

// makeBasePodAndStatusWithRestartableInitContainers 构造带三个 restartable init
// 容器的基础 pod/status。
func makeBasePodAndStatusWithRestartableInitContainers() (*v1.Pod, *kubecontainer.PodStatus) {
	pod, status := makeBasePodAndStatus()
	pod.Spec.InitContainers = []v1.Container{
		{
			Name:          "restartable-init-1",
			Image:         "bar-image",
			RestartPolicy: &containerRestartPolicyAlways,
		},
		{
			Name:          "restartable-init-2",
			Image:         "bar-image",
			RestartPolicy: &containerRestartPolicyAlways,
			LivenessProbe: &v1.Probe{},
			StartupProbe:  &v1.Probe{},
		},
		{
			Name:          "restartable-init-3",
			Image:         "bar-image",
			RestartPolicy: &containerRestartPolicyAlways,
			LivenessProbe: &v1.Probe{},
			StartupProbe:  &v1.Probe{},
		},
	}
	// Replace the original statuses of the containers with those for the init
	// containers.
	status.ContainerStatuses = []*kubecontainer.Status{
		{
			ID:   kubecontainer.ContainerID{ID: "initid1"},
			Name: "restartable-init-1", State: kubecontainer.ContainerStateRunning,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[0]),
		},
		{
			ID:   kubecontainer.ContainerID{ID: "initid2"},
			Name: "restartable-init-2", State: kubecontainer.ContainerStateRunning,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[1]),
		},
		{
			ID:   kubecontainer.ContainerID{ID: "initid3"},
			Name: "restartable-init-3", State: kubecontainer.ContainerStateRunning,
			Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[2]),
		},
	}
	status.ActiveContainerStatuses = status.ContainerStatuses
	return pod, status
}

// TestComputePodActionsWithInitAndEphemeralContainers 验证 init 与 ephemeral
// 容器并存时的决策。
func TestComputePodActionsWithInitAndEphemeralContainers(t *testing.T) {
	// Make sure existing test cases pass with feature enabled
	// 先跑已有用例，确保特性开启后行为不回归。
	TestComputePodActions(t)
	TestComputePodActionsWithInitContainers(t)

	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	basePod, baseStatus := makeBasePodAndStatusWithInitAndEphemeralContainers()
	noAction := podActions{
		SandboxID:         baseStatus.SandboxStatuses[0].Id,
		ContainersToStart: []int{},
		ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
	}

	for desc, test := range map[string]struct {
		mutatePodFn    func(*v1.Pod)
		mutateStatusFn func(*kubecontainer.PodStatus)
		actions        podActions
	}{
		"steady state; do nothing; ignore ephemeral container": {
			actions: noAction,
		},
		"No ephemeral containers running; start one": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses = status.ContainerStatuses[:4]
			},
			actions: podActions{
				SandboxID:                  baseStatus.SandboxStatuses[0].Id,
				ContainersToStart:          []int{},
				ContainersToKill:           map[kubecontainer.ContainerID]containerToKillInfo{},
				EphemeralContainersToStart: []int{0},
			},
		},
		"Start second ephemeral container": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, v1.EphemeralContainer{
					EphemeralContainerCommon: v1.EphemeralContainerCommon{
						Name:  "debug2",
						Image: "busybox",
					},
				})
			},
			actions: podActions{
				SandboxID:                  baseStatus.SandboxStatuses[0].Id,
				ContainersToStart:          []int{},
				ContainersToKill:           map[kubecontainer.ContainerID]containerToKillInfo{},
				EphemeralContainersToStart: []int{1},
			},
		},
		"Ephemeral container exited; do not restart": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[4].State = kubecontainer.ContainerStateExited
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{},
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
			},
		},
		"initialization in progress; start ephemeral container": {
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[3].State = kubecontainer.ContainerStateRunning
				status.ContainerStatuses = status.ContainerStatuses[:4]
			},
			actions: podActions{
				SandboxID:                  baseStatus.SandboxStatuses[0].Id,
				ContainersToStart:          []int{},
				ContainersToKill:           map[kubecontainer.ContainerID]containerToKillInfo{},
				EphemeralContainersToStart: []int{0},
			},
		},
		"Create a new pod sandbox if the pod sandbox is dead, init container failed and RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses = status.ContainerStatuses[3:]
				status.ContainerStatuses[0].ExitCode = 137
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Kill pod and do not restart ephemeral container if the pod sandbox is dead": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyAlways },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
			},
			actions: podActions{
				KillPod:               true,
				CreateSandbox:         true,
				SandboxID:             baseStatus.SandboxStatuses[0].Id,
				Attempt:               uint32(1),
				InitContainersToStart: []int{0},
				ContainersToStart:     []int{},
				ContainersToKill:      getKillMapWithInitContainers(basePod, baseStatus, []int{}),
			},
		},
		"Kill pod if all containers exited except ephemeral container": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// all regular containers exited
				for i := 0; i < 3; i++ {
					status.ContainerStatuses[i].State = kubecontainer.ContainerStateExited
					status.ContainerStatuses[i].ExitCode = 0
				}
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				CreateSandbox:     false,
				KillPod:           true,
				ContainersToStart: []int{},
				ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
			},
		},
		"Ephemeral container is in unknown state; leave it alone": {
			mutatePodFn: func(pod *v1.Pod) { pod.Spec.RestartPolicy = v1.RestartPolicyNever },
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.ContainerStatuses[4].State = kubecontainer.ContainerStateUnknown
			},
			actions: noAction,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			pod, status := makeBasePodAndStatusWithInitAndEphemeralContainers()
			if test.mutatePodFn != nil {
				test.mutatePodFn(pod)
			}
			if test.mutateStatusFn != nil {
				test.mutateStatusFn(status)
			}
			tCtx := ktesting.Init(t)
			actions := m.computePodActions(tCtx, pod, status, false)
			verifyActions(t, &test.actions, &actions, desc)
		})
	}
}

// TestComputePodActionsWithContainerRestartRules 验证容器级重启规则下的启停决策。
func TestComputePodActionsWithContainerRestartRules(t *testing.T) {
	tCtx := ktesting.Init(t)
	// Make sure existing test cases pass with feature enabled
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ContainerRestartRules, true)
	// 先跑已有用例，确保容器级重启规则开启后行为不回归。
	TestComputePodActions(t)
	TestComputePodActionsWithInitContainers(t)

	var (
		containerRestartPolicyAlways    = v1.ContainerRestartPolicyAlways
		containerRestartPolicyOnFailure = v1.ContainerRestartPolicyOnFailure
		containerRestartPolicyNever     = v1.ContainerRestartPolicyNever
	)

	// Creating a pair reference pod and status for the test cases to refer
	// the specific fields.
	basePod, baseStatus := makeBasePodAndStatus()
	noAction := podActions{
		SandboxID:         baseStatus.SandboxStatuses[0].Id,
		ContainersToStart: []int{},
		ContainersToKill:  map[kubecontainer.ContainerID]containerToKillInfo{},
	}

	for desc, test := range map[string]struct {
		mutatePodFn    func(*v1.Pod)
		mutateStatusFn func(*kubecontainer.PodStatus)
		actions        podActions
		resetStatusFn  func(*kubecontainer.PodStatus)
	}{
		"restart exited containers if RestartPolicy == Always": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.Containers[0].RestartPolicy = &containerRestartPolicyAlways
				pod.Spec.Containers[1].RestartPolicy = &containerRestartPolicyAlways
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, restart it,
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container exited with failure, restart it,
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{0, 1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"restart failed containers if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.Containers[0].RestartPolicy = &containerRestartPolicyOnFailure
				pod.Spec.Containers[1].RestartPolicy = &containerRestartPolicyOnFailure
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, don't restart it,
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container exited with failure, restart it,
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"restart created but not started containers if RestartPolicy == OnFailure": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.Containers[0].RestartPolicy = &containerRestartPolicyOnFailure
				pod.Spec.Containers[1].RestartPolicy = &containerRestartPolicyOnFailure
				pod.Spec.RestartPolicy = v1.RestartPolicyNever
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// The first container completed, don't restart it.
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0

				// The second container was created, but never started.
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateCreated
			},
			actions: podActions{
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				ContainersToStart: []int{1},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
		"don't restart containers if RestartPolicy == Never": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.Containers[0].RestartPolicy = &containerRestartPolicyNever
				pod.Spec.Containers[1].RestartPolicy = &containerRestartPolicyNever
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				// Don't restart any containers.
				status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[0].ExitCode = 0
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 111
			},
			actions: noAction,
		},
		"Kill pod and recreate all containers (except for the succeeded one) if the pod sandbox is dead": {
			mutatePodFn: func(pod *v1.Pod) {
				pod.Spec.Containers[1].RestartPolicy = &containerRestartPolicyOnFailure
				pod.Spec.RestartPolicy = v1.RestartPolicyAlways
			},
			mutateStatusFn: func(status *kubecontainer.PodStatus) {
				status.SandboxStatuses[0].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
				status.ContainerStatuses[1].State = kubecontainer.ContainerStateExited
				status.ContainerStatuses[1].ExitCode = 0
			},
			actions: podActions{
				KillPod:           true,
				CreateSandbox:     true,
				SandboxID:         baseStatus.SandboxStatuses[0].Id,
				Attempt:           uint32(1),
				ContainersToStart: []int{0, 2},
				ContainersToKill:  getKillMap(basePod, baseStatus, []int{}),
			},
		},
	} {
		_, _, m, err := createTestRuntimeManager(tCtx)
		require.NoError(t, err)
		pod, status := makeBasePodAndStatus()
		if test.mutatePodFn != nil {
			test.mutatePodFn(pod)
		}
		if test.mutateStatusFn != nil {
			test.mutateStatusFn(status)
		}
		actions := m.computePodActions(tCtx, pod, status, false)
		verifyActions(t, &test.actions, &actions, desc)
		if test.resetStatusFn != nil {
			test.resetStatusFn(status)
		}
	}
}

// TestSyncPodWithSandboxAndDeletedPod 验证 pod 已删除时 SyncPod 不会因 sandbox 创建失败而报错。
func TestSyncPodWithSandboxAndDeletedPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)
	fakeRuntime.ErrorOnSandboxCreate = true

	containers := []v1.Container{
		{
			Name:            "foo1",
			Image:           "busybox",
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	// 把 pod 标记为 removed，模拟它已从 pod provider 中消失。
	m.podStateProvider.(*fakePodStateProvider).removed = map[types.UID]struct{}{pod.UID: {}}

	// GetPodStatus and the following SyncPod will not return errors in the
	// case where the pod has been deleted. We are not adding any pods into
	// the fakePodProvider so they are 'deleted'.
	runtimePod, err := m.GetPod(tCtx, pod.UID)
	if err != nil {
		runtimePod = &kubecontainer.Pod{
			ID:        pod.UID,
			Name:      pod.Name,
			Namespace: pod.Namespace,
		}
	}
	podStatus, err := m.GetPodStatus(tCtx, runtimePod)
	require.NoError(t, err)
	result := m.SyncPod(tCtx, pod, podStatus, []v1.Secret{}, backOff, false)
	// This will return an error if the pod has _not_ been deleted.
	require.NoError(t, result.Error())
}

// makeBasePodAndStatusWithInitAndEphemeralContainers 构造含 init 与 ephemeral
// 容器的基础 pod/status。
func makeBasePodAndStatusWithInitAndEphemeralContainers() (*v1.Pod, *kubecontainer.PodStatus) {
	pod, status := makeBasePodAndStatus()
	pod.Spec.InitContainers = []v1.Container{
		{
			Name:  "init1",
			Image: "bar-image",
		},
	}
	pod.Spec.EphemeralContainers = []v1.EphemeralContainer{
		{
			EphemeralContainerCommon: v1.EphemeralContainerCommon{
				Name:  "debug",
				Image: "busybox",
			},
		},
	}
	status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
		ID:   kubecontainer.ContainerID{ID: "initid1"},
		Name: "init1", State: kubecontainer.ContainerStateExited,
		Hash: kubecontainer.HashContainer(&pod.Spec.InitContainers[0]),
	}, &kubecontainer.Status{
		ID:   kubecontainer.ContainerID{ID: "debug1"},
		Name: "debug", State: kubecontainer.ContainerStateRunning,
		Hash: kubecontainer.HashContainer((*v1.Container)(&pod.Spec.EphemeralContainers[0].EphemeralContainerCommon)),
	})
	status.ActiveContainerStatuses = status.ContainerStatuses
	return pod, status
}

// TestComputePodActionsForPodResize 验证 pod/容器级资源的 resize 决策与 kill/restart 语义。
func TestComputePodActionsForPodResize(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("InPlacePodVerticalScaling is currently not supported on Windows")
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)

	logger, tCtx := ktesting.NewTestContext(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	// 提高内存容量，避免 resize 因节点容量不足被拒绝。
	m.machineInfo.MemoryCapacity = 17179860387 // 16GB
	assert.NoError(t, err)

	cpu100m := resource.MustParse("100m")
	cpu200m := resource.MustParse("200m")
	mem100M := resource.MustParse("100Mi")
	mem200M := resource.MustParse("200Mi")
	cpu300m := resource.MustParse("300m")
	cpu600m := resource.MustParse("600m")
	mem300M := resource.MustParse("300Mi")
	mem600M := resource.MustParse("600Mi")
	cpuPolicyRestartNotRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired}
	memPolicyRestartNotRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceMemory, RestartPolicy: v1.NotRequired}
	cpuPolicyRestartRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}
	memPolicyRestartRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceMemory, RestartPolicy: v1.RestartContainer}

	setupActuatedResources := func(pod *v1.Pod, container *v1.Container, actuatedResources v1.ResourceRequirements) {
		actuatedContainer := container.DeepCopy()
		actuatedContainer.Resources = actuatedResources
		require.NoError(t, m.actuatedState.SetContainerResources(logger, pod.UID, actuatedContainer.Name, actuatedContainer.Resources))
	}

	setupActuatedPodResources := func(pod *v1.Pod, actuatedPodResources *v1.ResourceRequirements) {
		require.NoError(t, m.actuatedState.SetPodLevelResources(logger, pod.UID, actuatedPodResources))

	}

	// resize 用例需要 NodeDeclaredFeatures 支持。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, true)
	for desc, test := range map[string]struct {
		setupFn                 func(*v1.Pod)
		getExpectedPodActionsFn func(*v1.Pod, *kubecontainer.PodStatus) *podActions
		podLevelResizeEnabled   bool
	}{
		"Update pod-level CPU and memory resources pod resize disabled": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToStart:  []int{},
					ContainersToKill:   getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
				}
				return &pa
			},
		},
		"Update pod-level CPU and memory resources": {
			setupFn: func(pod *v1.Pod) {
				for i, container := range pod.Spec.Containers {
					container.Resources = v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
					}
					pod.Spec.Containers[i].Resources = container.Resources

					setupActuatedResources(pod, &container, v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    cpu100m.DeepCopy(),
							v1.ResourceMemory: mem100M.DeepCopy(),
						},
					})
				}

				pod.Spec.Resources = &v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
				}

				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				pa := podActions{
					SandboxID:               podStatus.SandboxStatuses[0].Id,
					ContainersToStart:       []int{},
					ContainersToKill:        getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate:      map[v1.ResourceName][]containerToUpdateInfo{},
					UpdatePodLevelResources: true,
				}
				return &pa
			},
			podLevelResizeEnabled: true,
		},
		"Update pod-level and container CPU and memory resources pod resize disabled": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update pod-level resources and container CPU and memory resources": {
			setupFn: func(pod *v1.Pod) {
				c0 := &pod.Spec.Containers[0]
				c0.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				c1 := &pod.Spec.Containers[1]
				c1.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
				}
				c2 := &pod.Spec.Containers[2]
				c2.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				for _, container := range pod.Spec.Containers {
					setupActuatedResources(pod, &container, v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    cpu100m.DeepCopy(),
							v1.ResourceMemory: mem100M.DeepCopy(),
						},
					})
				}
				pod.Spec.Resources = &v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu600m, v1.ResourceMemory: mem600M},
				}
				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu300m.DeepCopy(),
						v1.ResourceMemory: mem300M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
							},
						},
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
							},
						},
					},
					UpdatePodLevelResources: true,
				}
				return &pa
			},
			podLevelResizeEnabled: true,
		},
		"Update pod-level limits and container updated by default": {
			setupFn: func(pod *v1.Pod) {
				c0 := &pod.Spec.Containers[0]
				c0.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c0, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
				// no resources set for c1 container
				c2 := &pod.Spec.Containers[2]
				c2.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c2, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})

				pod.Spec.Resources = &v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu600m, v1.ResourceMemory: mem600M},
				}
				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu300m.DeepCopy(),
						v1.ResourceMemory: mem300M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem600M.Value(),
									cpuLimit:    cpu600m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem300M.Value(),
									cpuLimit:    cpu300m.MilliValue(),
								},
							},
						},
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem600M.Value(),
									cpuLimit:    cpu600m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem300M.Value(),
									cpuLimit:    cpu300m.MilliValue(),
								},
							},
						},
					},
					UpdatePodLevelResources: true,
				}
				return &pa
			},
			podLevelResizeEnabled: true,
		},
		"Update pod-level limits and container to kill updated by default": {
			setupFn: func(pod *v1.Pod) {
				c0 := &pod.Spec.Containers[0]
				c0.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}

				setupActuatedResources(pod, c0, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
				// no resources set for c1 container
				c1 := &pod.Spec.Containers[1]
				setupActuatedResources(pod, c1, v1.ResourceRequirements{})
				c1.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartRequired, memPolicyRestartNotRequired}
				c2 := &pod.Spec.Containers[2]
				c2.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c2, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})

				pod.Spec.Resources = &v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu600m, v1.ResourceMemory: mem600M},
				}
				setupActuatedPodResources(pod, &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu300m.DeepCopy(),
						v1.ResourceMemory: mem300M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				killMap := make(map[kubecontainer.ContainerID]containerToKillInfo)
				killMap[kcs.ID] = containerToKillInfo{
					container: &pod.Spec.Containers[1],
					name:      pod.Spec.Containers[1].Name,
				}
				pa := podActions{
					SandboxID:               podStatus.SandboxStatuses[0].Id,
					ContainersToStart:       []int{1},
					ContainersToKill:        killMap,
					ContainersToUpdate:      map[v1.ResourceName][]containerToUpdateInfo{},
					UpdatePodLevelResources: true,
					UpdatePodResources:      true,
				}
				return &pa
			},
			podLevelResizeEnabled: true,
		},
		"Update container CPU and memory resources": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update container CPU resources": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update container memory resources": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[2]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[2],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update pod-level limits does not mutate container resources": {
			setupFn: func(pod *v1.Pod) {
				pod.Spec.Resources = &v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				}
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU: cpu100m.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs1 := podStatus.FindContainerStatusByName(pod.Spec.Containers[0].Name)
				kcs2 := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				kcs3 := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[0],
								kubeContainerID: kcs1.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{},
							},
							{
								container:       &pod.Spec.Containers[2],
								kubeContainerID: kcs3.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{},
							},
						},
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[0],
								kubeContainerID: kcs1.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{},
							},
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs2.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									cpuLimit: cpu100m.MilliValue(),
								},
							},
							{
								container:       &pod.Spec.Containers[2],
								kubeContainerID: kcs3.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{},
							},
						},
					},
					UpdatePodLevelResources: true,
				}
				return &pa
			},
			podLevelResizeEnabled: true,
		},
		"Nothing when spec.Resources and status.Resources are equal": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU: cpu200m.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToKill:   getKillMap(pod, podStatus, []int{}),
					ContainersToStart:  []int{},
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
				}
				return &pa
			},
		},
		"Nothing when spec.Resources and status.Resources are equal (besteffort)": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.Resources = v1.ResourceRequirements{}
				setupActuatedResources(pod, c, v1.ResourceRequirements{})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToKill:   getKillMap(pod, podStatus, []int{}),
					ContainersToStart:  []int{},
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
				}
				return &pa
			},
		},
		"Update container CPU and memory resources with Restart policy for CPU": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[0]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartRequired, memPolicyRestartNotRequired}
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[0].Name)
				killMap := make(map[kubecontainer.ContainerID]containerToKillInfo)
				killMap[kcs.ID] = containerToKillInfo{
					container: &pod.Spec.Containers[0],
					name:      pod.Spec.Containers[0].Name,
				}
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToStart:  []int{0},
					ContainersToKill:   killMap,
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
					UpdatePodResources: true,
				}
				return &pa
			},
		},
		"Update container CPU and memory resources with Restart policy for memory": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[2]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartRequired}
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				killMap := make(map[kubecontainer.ContainerID]containerToKillInfo)
				killMap[kcs.ID] = containerToKillInfo{
					container: &pod.Spec.Containers[2],
					name:      pod.Spec.Containers[2].Name,
				}
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToStart:  []int{2},
					ContainersToKill:   killMap,
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
					UpdatePodResources: true,
				}
				return &pa
			},
		},
		"Update container memory resources with Restart policy for CPU": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[1]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartRequired, memPolicyRestartNotRequired}
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem200M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[1].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[1],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem200M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update container CPU resources with Restart policy for memory": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[2]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartRequired}
				c.Resources = v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem100M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceCPU: {
							{
								container:       &pod.Spec.Containers[2],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu200m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit: mem100M.Value(),
									cpuLimit:    cpu100m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
		"Update container memory (requests only) with RestartContainer policy for memory": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[2]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartRequired}
				c.Resources = v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
					Requests: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				killMap := make(map[kubecontainer.ContainerID]containerToKillInfo)
				killMap[kcs.ID] = containerToKillInfo{
					container: &pod.Spec.Containers[2],
					name:      pod.Spec.Containers[2].Name,
				}
				pa := podActions{
					SandboxID:          podStatus.SandboxStatuses[0].Id,
					ContainersToStart:  []int{2},
					ContainersToKill:   killMap,
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{},
					UpdatePodResources: true,
				}
				return &pa
			},
		},
		"Update container memory (requests only) with RestartNotRequired policy for memory": {
			setupFn: func(pod *v1.Pod) {
				c := &pod.Spec.Containers[2]
				c.ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartNotRequired}
				c.Resources = v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				}
				setupActuatedResources(pod, c, v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    cpu200m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
					Requests: v1.ResourceList{
						v1.ResourceCPU:    cpu100m.DeepCopy(),
						v1.ResourceMemory: mem200M.DeepCopy(),
					},
				})
			},
			getExpectedPodActionsFn: func(pod *v1.Pod, podStatus *kubecontainer.PodStatus) *podActions {
				kcs := podStatus.FindContainerStatusByName(pod.Spec.Containers[2].Name)
				killMap := make(map[kubecontainer.ContainerID]containerToKillInfo)
				killMap[kcs.ID] = containerToKillInfo{
					container: &pod.Spec.Containers[2],
					name:      pod.Spec.Containers[2].Name,
				}
				pa := podActions{
					SandboxID:         podStatus.SandboxStatuses[0].Id,
					ContainersToStart: []int{},
					ContainersToKill:  getKillMap(pod, podStatus, []int{}),
					ContainersToUpdate: map[v1.ResourceName][]containerToUpdateInfo{
						v1.ResourceMemory: {
							{
								container:       &pod.Spec.Containers[2],
								kubeContainerID: kcs.ID,
								desiredContainerResources: resourceRequirements{
									memoryLimit:   mem200M.Value(),
									memoryRequest: mem100M.Value(),
									cpuLimit:      cpu200m.MilliValue(),
									cpuRequest:    cpu100m.MilliValue(),
								},
								currentContainerResources: &resourceRequirements{
									memoryLimit:   mem200M.Value(),
									memoryRequest: mem200M.Value(),
									cpuLimit:      cpu200m.MilliValue(),
									cpuRequest:    cpu100m.MilliValue(),
								},
							},
						},
					},
				}
				return &pa
			},
		},
	} {
		// 每个用例独立设置 pod-level resize 门控。
		t.Run(desc, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodLevelResourcesVerticalScaling, test.podLevelResizeEnabled)
			pod, status := makeBasePodAndStatus()
			// 默认 resizePolicy 为 NotRequired，符合特性开启后的语义。
			for idx := range pod.Spec.Containers {
				// default resize policy when pod resize feature is enabled
				pod.Spec.Containers[idx].ResizePolicy = []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartNotRequired}
			}
			if test.setupFn != nil {
				test.setupFn(pod)
			}
			t.Cleanup(func() { _ = m.actuatedState.RemovePod(logger, pod.UID) })

			for idx := range pod.Spec.Containers {
				// compute hash
				if kcs := status.FindContainerStatusByName(pod.Spec.Containers[idx].Name); kcs != nil {
					kcs.Hash = kubecontainer.HashContainer(&pod.Spec.Containers[idx])
				}
			}

			tCtx := ktesting.Init(t)
			expectedActions := test.getExpectedPodActionsFn(pod, status)

			// Capture container resource limits before computePodActions to check for mutation
			// 先快照容器 limits，验证 computePodActions 不会修改输入 spec。
			preComputeLimits := make([]v1.ResourceList, len(pod.Spec.Containers))
			for i := range pod.Spec.Containers {
				preComputeLimits[i] = pod.Spec.Containers[i].Resources.Limits.DeepCopy()
			}

			actions := m.computePodActions(tCtx, pod, status, false)

			// Mutation check: Ensure pod.Spec.Containers[i].Resources.Limits is not mutated
			for i := range pod.Spec.Containers {
				if !reflect.DeepEqual(preComputeLimits[i], pod.Spec.Containers[i].Resources.Limits) {
					t.Errorf("pod.Spec.Containers[%d].Resources.Limits was mutated! pre: %v, post: %v", i, preComputeLimits[i], pod.Spec.Containers[i].Resources.Limits)
				}
			}

			verifyActions(t, expectedActions, &actions, desc)
		})
	}
}

// TestComputePodResizeActionForOOMKilledContainer 验证被 OOMKilled 的容器只更新
// pod cgroup，不发起在线 resize。
func TestComputePodResizeActionForOOMKilledContainer(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("in-place resize is only supported on Linux")
	}
	// 原地 resize 仅在 Linux 支持。
	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	_, _, m, err := createTestRuntimeManager(tCtx)
	m.machineInfo.MemoryCapacity = 17179860387 // 16GB
	require.NoError(t, err)

	mem100M := resource.MustParse("100Mi")
	mem200M := resource.MustParse("200Mi")
	cpu100m := resource.MustParse("100m")

	pod, status := makeBasePodAndStatus()
	pod.Spec.Containers = pod.Spec.Containers[:1]
	status.ContainerStatuses = status.ContainerStatuses[:1]

	pod.Spec.Containers[0].Resources = v1.ResourceRequirements{
		Limits:   v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
		Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
	}
	pod.Spec.Containers[0].ResizePolicy = []v1.ContainerResizePolicy{
		{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired},
		{ResourceName: v1.ResourceMemory, RestartPolicy: v1.NotRequired},
	}
	// record the pre-resize resource limits as what was last actuated.
	// 记录 resize 前的 actuated 资源。
	m.InitializeActuatedPod(logger, pod)

	// Pod Resized
	resize := pod.Spec.Containers[0].Resources.DeepCopy()
	resize.Requests[v1.ResourceMemory] = mem200M
	resize.Limits[v1.ResourceMemory] = mem200M
	pod.Spec.Containers[0].Resources = *resize

	// simulate OOMKilled
	// 模拟容器因内存不足被 OOMKilled。
	status.ContainerStatuses[0].State = kubecontainer.ContainerStateExited
	status.ContainerStatuses[0].Hash = kubecontainer.HashContainer(&pod.Spec.Containers[0])

	actions := m.computePodActions(tCtx, pod, status, false)

	// the container is OOMKilled and must not be added to ContainersToUpdate (no live CRI call).
	assert.Empty(t, actions.ContainersToUpdate, "OOMKilled container must not be in ContainersToUpdate")
	// UpdatePodResources must be true so doPodResizeAction updates the pod-level cgroup.
	assert.True(t, actions.UpdatePodResources, "UpdatePodResources must be true for OOMKilled container with pending resize")
}

// TestComputePodResizeActionForOOMKilledInitContainer 验证被 OOMKilled 的 init
// 容器只更新 pod cgroup。
func TestComputePodResizeActionForOOMKilledInitContainer(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("in-place resize is only supported on Linux")
	}
	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	_, _, m, err := createTestRuntimeManager(tCtx)
	m.machineInfo.MemoryCapacity = 17179860387 // 16GB
	require.NoError(t, err)

	mem100M := resource.MustParse("100Mi")
	mem200M := resource.MustParse("200Mi")
	cpu100m := resource.MustParse("100m")

	pod, status := makeBasePodAndStatus()
	// a single init container.
	pod.Spec.InitContainers = []v1.Container{
		{
			Name:  "init1",
			Image: "bar-image",
			Resources: v1.ResourceRequirements{
				Limits:   v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
			},
			ResizePolicy: []v1.ContainerResizePolicy{
				{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired},
				{ResourceName: v1.ResourceMemory, RestartPolicy: v1.NotRequired},
			},
		},
	}
	// no regular containers running so pod is not yet initialized.
	status.ContainerStatuses = []*kubecontainer.Status{
		{
			ID:       kubecontainer.ContainerID{ID: "initid1"},
			Name:     "init1",
			State:    kubecontainer.ContainerStateExited,
			Reason:   "OOMKilled",
			ExitCode: 137,
			Hash:     kubecontainer.HashContainer(&pod.Spec.InitContainers[0]),
		},
	}
	pod.Spec.Containers = nil
	pod.Status.ContainerStatuses = nil

	// record pre-resize resource limits as what was last actuated.
	m.InitializeActuatedPod(logger, pod)

	// Pod Resized
	resize := pod.Spec.InitContainers[0].Resources.DeepCopy()
	resize.Requests[v1.ResourceMemory] = mem200M
	resize.Limits[v1.ResourceMemory] = mem200M
	pod.Spec.InitContainers[0].Resources = *resize

	actions := m.computePodActions(tCtx, pod, status, false)

	// the init container is OOMKilled and must not be in ContainersToUpdate.
	assert.Empty(t, actions.ContainersToUpdate, "OOMKilled init container must not be in ContainersToUpdate")
	// UpdatePodResources must be true so doPodResizeAction updates the pod-level cgroup.
	assert.True(t, actions.UpdatePodResources, "UpdatePodResources must be true for OOMKilled init container with pending resize")
}

// TestComputePodResizeActionForOOMKilledSidecarContainer 验证被 OOMKilled 的 sidecar
// 只更新 pod cgroup。
func TestComputePodResizeActionForOOMKilledSidecarContainer(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("in-place resize is only supported on Linux")
	}
	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	_, _, m, err := createTestRuntimeManager(tCtx)
	m.machineInfo.MemoryCapacity = 17179860387 // 16GB
	require.NoError(t, err)

	mem100M := resource.MustParse("100Mi")
	mem200M := resource.MustParse("200Mi")
	cpu100m := resource.MustParse("100m")

	pod, status := makeBasePodAndStatus()
	pod.Spec.Containers = nil
	pod.Status.ContainerStatuses = nil
	status.ContainerStatuses = nil

	// one running regular container so the pod is considered initialized.
	regularContainer := v1.Container{
		Name:  "app",
		Image: "busybox",
	}
	pod.Spec.Containers = []v1.Container{regularContainer}
	status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
		ID:    kubecontainer.ContainerID{ID: "appid"},
		Name:  "app",
		State: kubecontainer.ContainerStateRunning,
		Hash:  kubecontainer.HashContainer(&pod.Spec.Containers[0]),
	})

	// one sidecar that has been OOMKilled.
	sidecar := v1.Container{
		Name:          "sidecar",
		Image:         "bar-image",
		RestartPolicy: &containerRestartPolicyAlways,
		Resources: v1.ResourceRequirements{
			Limits:   v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
			Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
		},
		ResizePolicy: []v1.ContainerResizePolicy{
			{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired},
			{ResourceName: v1.ResourceMemory, RestartPolicy: v1.NotRequired},
		},
	}
	pod.Spec.InitContainers = []v1.Container{sidecar}
	status.ContainerStatuses = append(status.ContainerStatuses, &kubecontainer.Status{
		ID:       kubecontainer.ContainerID{ID: "sidecarid"},
		Name:     "sidecar",
		State:    kubecontainer.ContainerStateExited,
		Reason:   "OOMKilled",
		ExitCode: 137,
		Hash:     kubecontainer.HashContainer(&pod.Spec.InitContainers[0]),
	})

	// record pre-resize resource limits as what was last actuated.
	m.InitializeActuatedPod(logger, pod)

	// Pod Resized
	resize := pod.Spec.InitContainers[0].Resources.DeepCopy()
	resize.Requests[v1.ResourceMemory] = mem200M
	resize.Limits[v1.ResourceMemory] = mem200M
	pod.Spec.InitContainers[0].Resources = *resize

	actions := m.computePodActions(tCtx, pod, status, false)

	// the sidecar is OOMKilled and must not be in ContainersToUpdate.
	assert.Empty(t, actions.ContainersToUpdate, "OOMKilled sidecar must not be in ContainersToUpdate")
	// UpdatePodResources must be true so doPodResizeAction updates the pod-level cgroup.
	assert.True(t, actions.UpdatePodResources, "UpdatePodResources must be true for OOMKilled sidecar with pending resize")
}

// TestUpdatePodContainerResources 验证更新容器资源时 CRI 调用与 current 值回填。
func TestUpdatePodContainerResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	m.machineInfo.MemoryCapacity = 17179860387 // 16GB
	assert.NoError(t, err)

	cpu100m := resource.MustParse("100m")
	cpu150m := resource.MustParse("150m")
	cpu200m := resource.MustParse("200m")
	cpu250m := resource.MustParse("250m")
	cpu300m := resource.MustParse("300m")
	cpu350m := resource.MustParse("350m")
	mem100M := resource.MustParse("100Mi")
	mem150M := resource.MustParse("150Mi")
	mem200M := resource.MustParse("200Mi")
	mem250M := resource.MustParse("250Mi")
	mem300M := resource.MustParse("300Mi")
	mem350M := resource.MustParse("350Mi")
	res100m100Mi := v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M}
	res150m100Mi := v1.ResourceList{v1.ResourceCPU: cpu150m, v1.ResourceMemory: mem100M}
	res100m150Mi := v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem150M}
	res150m150Mi := v1.ResourceList{v1.ResourceCPU: cpu150m, v1.ResourceMemory: mem150M}
	res200m200Mi := v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M}
	res250m200Mi := v1.ResourceList{v1.ResourceCPU: cpu250m, v1.ResourceMemory: mem200M}
	res200m250Mi := v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem250M}
	res250m250Mi := v1.ResourceList{v1.ResourceCPU: cpu250m, v1.ResourceMemory: mem250M}
	res300m300Mi := v1.ResourceList{v1.ResourceCPU: cpu300m, v1.ResourceMemory: mem300M}
	res350m300Mi := v1.ResourceList{v1.ResourceCPU: cpu350m, v1.ResourceMemory: mem300M}
	res300m350Mi := v1.ResourceList{v1.ResourceCPU: cpu300m, v1.ResourceMemory: mem350M}
	res350m350Mi := v1.ResourceList{v1.ResourceCPU: cpu350m, v1.ResourceMemory: mem350M}

	pod, _ := makeBasePodAndStatusWithRestartableInitContainers()
	// 先放入 fake sandbox/容器，使 updatePodContainerResources 能定位容器。
	makeAndSetFakePod(tCtx, m, fakeRuntime, pod)

	for dsc, tc := range map[string]struct {
		resourceName            v1.ResourceName
		apiSpecResources        []v1.ResourceRequirements
		apiStatusResources      []v1.ResourceRequirements
		requiresRestart         []bool
		invokeUpdateResources   bool
		expectedCurrentLimits   []v1.ResourceList
		expectedCurrentRequests []v1.ResourceList
	}{
		"Guaranteed QoS Pod - CPU & memory resize requested, update CPU": {
			resourceName: v1.ResourceCPU,
			apiSpecResources: []v1.ResourceRequirements{
				{Limits: res150m150Mi, Requests: res150m150Mi},
				{Limits: res250m250Mi, Requests: res250m250Mi},
				{Limits: res350m350Mi, Requests: res350m350Mi},
			},
			apiStatusResources: []v1.ResourceRequirements{
				{Limits: res100m100Mi, Requests: res100m100Mi},
				{Limits: res200m200Mi, Requests: res200m200Mi},
				{Limits: res300m300Mi, Requests: res300m300Mi},
			},
			requiresRestart:         []bool{false, false, false},
			invokeUpdateResources:   true,
			expectedCurrentLimits:   []v1.ResourceList{res150m100Mi, res250m200Mi, res350m300Mi},
			expectedCurrentRequests: []v1.ResourceList{res150m100Mi, res250m200Mi, res350m300Mi},
		},
		"Guaranteed QoS Pod - CPU & memory resize requested, update memory": {
			resourceName: v1.ResourceMemory,
			apiSpecResources: []v1.ResourceRequirements{
				{Limits: res150m150Mi, Requests: res150m150Mi},
				{Limits: res250m250Mi, Requests: res250m250Mi},
				{Limits: res350m350Mi, Requests: res350m350Mi},
			},
			apiStatusResources: []v1.ResourceRequirements{
				{Limits: res100m100Mi, Requests: res100m100Mi},
				{Limits: res200m200Mi, Requests: res200m200Mi},
				{Limits: res300m300Mi, Requests: res300m300Mi},
			},
			requiresRestart:         []bool{false, false, false},
			invokeUpdateResources:   true,
			expectedCurrentLimits:   []v1.ResourceList{res100m150Mi, res200m250Mi, res300m350Mi},
			expectedCurrentRequests: []v1.ResourceList{res100m150Mi, res200m250Mi, res300m350Mi},
		},
	} {
		for _, allSideCarCtrs := range []bool{false, true} {
			var containersToUpdate []containerToUpdateInfo
			containerToUpdateInfo := func(container *v1.Container, idx int) containerToUpdateInfo {
				return containerToUpdateInfo{
					container:       container,
					kubeContainerID: kubecontainer.ContainerID{},
					desiredContainerResources: resourceRequirements{
						memoryLimit:   tc.apiSpecResources[idx].Limits.Memory().Value(),
						memoryRequest: tc.apiSpecResources[idx].Requests.Memory().Value(),
						cpuLimit:      tc.apiSpecResources[idx].Limits.Cpu().MilliValue(),
						cpuRequest:    tc.apiSpecResources[idx].Requests.Cpu().MilliValue(),
					},
					currentContainerResources: &resourceRequirements{
						memoryLimit:   tc.apiStatusResources[idx].Limits.Memory().Value(),
						memoryRequest: tc.apiStatusResources[idx].Requests.Memory().Value(),
						cpuLimit:      tc.apiStatusResources[idx].Limits.Cpu().MilliValue(),
						cpuRequest:    tc.apiStatusResources[idx].Requests.Cpu().MilliValue(),
					},
				}
			}

			if allSideCarCtrs {
				for idx := range pod.Spec.InitContainers {
					// default resize policy when pod resize feature is enabled
					pod.Spec.InitContainers[idx].Resources = tc.apiSpecResources[idx]
					pod.Status.ContainerStatuses[idx].Resources = &tc.apiStatusResources[idx]
					cinfo := containerToUpdateInfo(&pod.Spec.InitContainers[idx], idx)
					containersToUpdate = append(containersToUpdate, cinfo)
				}
			} else {
				for idx := range pod.Spec.Containers {
					// default resize policy when pod resize feature is enabled
					pod.Spec.Containers[idx].Resources = tc.apiSpecResources[idx]
					pod.Status.ContainerStatuses[idx].Resources = &tc.apiStatusResources[idx]
					cinfo := containerToUpdateInfo(&pod.Spec.Containers[idx], idx)
					containersToUpdate = append(containersToUpdate, cinfo)
				}
			}

			// 清空调用记录，便于断言是否触发了 CRI 更新。
			fakeRuntime.Called = []string{}
			err := m.updatePodContainerResources(tCtx, pod, tc.resourceName, containersToUpdate)
			require.NoError(t, err, dsc)

			if tc.invokeUpdateResources {
				assert.Contains(t, fakeRuntime.Called, "UpdateContainerResources", dsc)
			}
			for idx := range len(containersToUpdate) {
				assert.Equal(t, tc.expectedCurrentLimits[idx].Memory().Value(), containersToUpdate[idx].currentContainerResources.memoryLimit, dsc)
				assert.Equal(t, tc.expectedCurrentRequests[idx].Memory().Value(), containersToUpdate[idx].currentContainerResources.memoryRequest, dsc)
				assert.Equal(t, tc.expectedCurrentLimits[idx].Cpu().MilliValue(), containersToUpdate[idx].currentContainerResources.cpuLimit, dsc)
				assert.Equal(t, tc.expectedCurrentRequests[idx].Cpu().MilliValue(), containersToUpdate[idx].currentContainerResources.cpuRequest, dsc)
			}
		}
	}
}

// TestToKubeContainerImageVolumes 验证已拉取镜像卷被映射到容器视图。
func TestToKubeContainerImageVolumes(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, manager, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	const (
		volume1 = "volume-1"
		volume2 = "volume-2"
	)
	imageSpec1 := runtimeapi.ImageSpec{Image: "image-1"}
	imageSpec2 := runtimeapi.ImageSpec{Image: "image-2"}
	errTest := errors.New("pull failed")
	syncResult := kubecontainer.NewSyncResult(kubecontainer.StartContainer, "test")

	for desc, tc := range map[string]struct {
		pullResults          imageVolumePulls
		container            *v1.Container
		expectedError        error
		expectedImageVolumes kubecontainer.ImageVolumes
	}{
		"empty volumes": {},
		"multiple volumes": {
			pullResults: imageVolumePulls{
				volume1: imageVolumePullResult{spec: &imageSpec1},
				volume2: imageVolumePullResult{spec: &imageSpec2},
			},
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{Name: volume1},
					{Name: volume2},
				},
			},
			expectedImageVolumes: kubecontainer.ImageVolumes{
				volume1: &imageSpec1,
				volume2: &imageSpec2,
			},
		},
		"not matching volume": {
			pullResults: imageVolumePulls{
				"different": imageVolumePullResult{spec: &imageSpec1},
			},
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{Name: volume1}},
			},
			expectedImageVolumes: kubecontainer.ImageVolumes{},
		},
		"error in pull result": {
			pullResults: imageVolumePulls{
				volume1: imageVolumePullResult{err: errTest},
			},
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{Name: volume1},
				},
			},
			expectedError: errTest,
		},
	} {
		imageVolumes, err := manager.toKubeContainerImageVolumes(tCtx, tc.pullResults, tc.container, &v1.Pod{}, syncResult)
		if tc.expectedError != nil {
			require.EqualError(t, err, tc.expectedError.Error())
		} else {
			require.NoError(t, err, desc)
		}
		assert.Equal(t, tc.expectedImageVolumes, imageVolumes)
	}
}

// TestGetImageVolumes 验证 SyncPod 前按卷引用拉取镜像卷并收集结果。
func TestGetImageVolumes(t *testing.T) {
	tCtx := ktesting.Init(t)

	_, _, manager, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	const (
		volume1 = "volume-1"
		volume2 = "volume-2"
		image1  = "image-1:latest"
		image2  = "image-2:latest"
	)
	imageSpec1 := runtimeapi.ImageSpec{Image: image1, UserSpecifiedImage: image1}
	imageSpec2 := runtimeapi.ImageSpec{Image: image2, UserSpecifiedImage: image2}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      "test-pod",
			Namespace: "test-namespace",
			Uid:       "test-uid",
		},
	}

	for desc, tc := range map[string]struct {
		pod                      *v1.Pod
		expectedImageVolumePulls imageVolumePulls
		expectedError            error
	}{
		"empty volumes": {
			pod:                      &v1.Pod{Spec: v1.PodSpec{Volumes: []v1.Volume{}}},
			expectedImageVolumePulls: imageVolumePulls{},
		},
		"multiple volumes": {
			pod: &v1.Pod{Spec: v1.PodSpec{Volumes: []v1.Volume{
				{Name: volume1, VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: image1, PullPolicy: v1.PullAlways}}},
				{Name: volume2, VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: image2, PullPolicy: v1.PullAlways}}},
			}}},
			expectedImageVolumePulls: imageVolumePulls{
				volume1: imageVolumePullResult{spec: &imageSpec1},
				volume2: imageVolumePullResult{spec: &imageSpec2},
			},
		},
		"different than image volumes": {
			pod: &v1.Pod{Spec: v1.PodSpec{Volumes: []v1.Volume{
				{Name: volume1, VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{}}},
			}}},
			expectedImageVolumePulls: imageVolumePulls{},
		},
		"multiple volumes but one failed to pull": {
			pod: &v1.Pod{Spec: v1.PodSpec{Volumes: []v1.Volume{
				{Name: volume1, VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: image1, PullPolicy: v1.PullAlways}}},
				{Name: volume2, VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image", PullPolicy: v1.PullNever}}}, // fails
			}}},
			expectedImageVolumePulls: imageVolumePulls{
				volume1: imageVolumePullResult{spec: &imageSpec1},
				volume2: imageVolumePullResult{err: imagetypes.ErrImageNeverPull, msg: `Container image "image" is not present with pull policy of Never`},
			},
		},
	} {

		imageVolumePulls, err := manager.getImageVolumes(tCtx, tc.pod, podSandboxConfig, nil)
		if tc.expectedError != nil {
			require.EqualError(t, err, tc.expectedError.Error())
		} else {
			require.NoError(t, err, desc)
		}
		assert.Equal(t, tc.expectedImageVolumePulls, imageVolumePulls)
	}
}

// TestDoPodResizeAction 验证容器与 pod 级 resize 在 cgroup/runtime 各失败路径下的行为。
func TestDoPodResizeAction(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("unsupported OS")
	}

	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	metrics.Register()
	metrics.PodResizeDurationMilliseconds.Reset()

	// 用例用整数表示 milli/字节，便于构造各类资源值。
	for i, tc := range []struct {
		testName                    string
		currentResources            resourceRequirements
		desiredResources            resourceRequirements
		updatedResources            []v1.ResourceName
		otherContainersHaveLimits   bool
		runtimeErrors               map[string][]error
		expectedError               string
		expectedErrorMessage        string
		expectPodCgroupUpdates      int
		injectPodUpdateCgroupsError error
		currentPodLevelResources    *resourceRequirements
		desiredPodLevelResources    *resourceRequirements
		updatedPodLevelResources    bool
		enablePLR                   bool
	}{
		{
			testName: "Increase cpu and memory requests and limits, with computed pod limits",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
				memoryRequest: 200, memoryLimit: 200,
			},
			otherContainersHaveLimits: true,
			updatedResources:          []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
			expectPodCgroupUpdates:    3, // cpu req, cpu lim, mem lim
		},
		{
			testName: "Increase cpu and memory requests and limits, with computed pod limits and set a runtime error",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
				memoryRequest: 200, memoryLimit: 200,
			},
			otherContainersHaveLimits: true,
			updatedResources:          []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
			expectPodCgroupUpdates:    1,
			runtimeErrors:             map[string][]error{"UpdateContainerResources": {fmt.Errorf("error updating container resources")}},
			expectedError:             "ResizePodInPlaceError",
			expectedErrorMessage:      "error updating container resources",
		},
		{
			testName: "Increase cpu and memory requests and limits, without computed pod limits",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
				memoryRequest: 200, memoryLimit: 200,
			},
			// If some containers don't have limits, pod level limits are not applied
			otherContainersHaveLimits: false,
			updatedResources:          []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
			expectPodCgroupUpdates:    1, // cpu req, cpu lim, mem lim
		},
		{
			testName: "Increase cpu and memory requests only",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 200,
				memoryRequest: 100, memoryLimit: 200,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 150, cpuLimit: 200,
				memoryRequest: 150, memoryLimit: 200,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates: 1, // cpu req
		},
		{
			testName: "Resize memory request no limits",
			currentResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 200,
			},
			// Memory request resize doesn't generate an update action.
			updatedResources: []v1.ResourceName{},
		},
		{
			testName: "Resize cpu request no limits",
			currentResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest:    200,
				memoryRequest: 100,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates: 1, // cpu req
		},
		{
			testName: "Add limits",
			currentResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
			expectPodCgroupUpdates: 0,
		},
		{
			testName: "Add limits and pod limits",
			currentResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			otherContainersHaveLimits: true,
			updatedResources:          []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
			expectPodCgroupUpdates:    2, // cpu lim, memory lim
		},
		{
			testName: "Fail updatePodSandboxResources blocks resize",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates: 0,
			runtimeErrors: map[string][]error{
				"UpdatePodSandboxResources": {fmt.Errorf("runtime sandbox update failed")},
			},
			expectedError:        "ResizePodInPlaceError",
			expectedErrorMessage: "failed to notify runtime for UpdatePodSandboxResources (resource=cpu); resize rejected: updatePodSandboxResources failed for sanboxID \"sandbox-id\": runtime sandbox update failed",
		},
		{
			testName: "Fail SetPodCgroupConfig triggers rollback of Sandbox resources",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
			},
			updatedResources:            []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates:      1,
			injectPodUpdateCgroupsError: fmt.Errorf("cgroup update failed"),
			expectedError:               "ResizePodInPlaceError",
			expectedErrorMessage:        "cgroup update failed",
		},
		{
			testName: "Ignore Unimplemented error from updatePodSandboxResources",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates: 1, // Should proceed to cgroups despite the error
			runtimeErrors: map[string][]error{
				// Use a GRPC Unimplemented error
				"UpdatePodSandboxResources": {status.Error(codes.Unimplemented, "not supported")},
			},
			// No error expected because we swallow Unimplemented
			expectedError:        "",
			expectedErrorMessage: "",
		},
		{
			testName: "Ignore Unimplemented message from updatePodSandboxResources",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 200, cpuLimit: 200,
			},
			updatedResources:       []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates: 1, // Should proceed to cgroups despite the error
			runtimeErrors: map[string][]error{
				// Use a GRPC Unimplemented error
				"UpdatePodSandboxResources": {status.Error(codes.Unknown, "not implemented yet")},
			},
			// No error expected because we swallow Unimplemented
			expectedError:        "",
			expectedErrorMessage: "",
		},
		{
			testName: "Resize pod-level memory request only (skips cgroup write, updates actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 200, memoryLimit: 100, // Memory request increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{},
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level memory limit (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
				memoryRequest: 100, memoryLimit: 200, // Memory limit increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{},
			expectPodCgroupUpdates:   1,
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level CPU request (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 200, cpuLimit: 100, // CPU request increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{},
			expectPodCgroupUpdates:   1,
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level CPU limit (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 200, // CPU limit increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{},
			expectPodCgroupUpdates:   1,
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level CPU limit and container-level CPU limit (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 200, // Container limit increase
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 100,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 300, // Pod limit increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates:   1, // Pod level cgroup update
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level memory request and container-level memory request (updates actuated)",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 200,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 150, memoryLimit: 200, // Container request increase
			},
			currentPodLevelResources: &resourceRequirements{
				memoryRequest: 100, memoryLimit: 200,
			},
			desiredPodLevelResources: &resourceRequirements{
				memoryRequest: 200, memoryLimit: 200, // Pod request increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{v1.ResourceMemory},
			expectPodCgroupUpdates:   0, // Memory request doesn't update cgroup
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level CPU request and container-level CPU request (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				cpuRequest: 100, cpuLimit: 200,
			},
			desiredResources: resourceRequirements{
				cpuRequest: 150, cpuLimit: 200, // Container request increase
			},
			currentPodLevelResources: &resourceRequirements{
				cpuRequest: 100, cpuLimit: 200,
			},
			desiredPodLevelResources: &resourceRequirements{
				cpuRequest: 200, cpuLimit: 200, // Pod request increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{v1.ResourceCPU},
			expectPodCgroupUpdates:   1, // Pod level cgroup update for cpu shares
			enablePLR:                true,
		},
		{
			testName: "Resize pod-level memory limit and container-level memory limit (updates cgroups and actuated)",
			currentResources: resourceRequirements{
				memoryLimit: 200,
			},
			desiredResources: resourceRequirements{
				memoryLimit: 250, // Container limit increase
			},
			currentPodLevelResources: &resourceRequirements{
				memoryLimit: 200,
			},
			desiredPodLevelResources: &resourceRequirements{
				memoryLimit: 300, // Pod limit increase
			},
			updatedPodLevelResources: true,
			updatedResources:         []v1.ResourceName{v1.ResourceMemory},
			expectPodCgroupUpdates:   1, // Pod level cgroup update for memory limit
			enablePLR:                true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			_, _, m, err := createTestRuntimeManager(tCtx, withErrors(tc.runtimeErrors))
			require.NoError(t, err)
			// 打开 CPU 配额，覆盖 CPU limit 的 cgroup 更新路径。
			m.cpuCFSQuota = true // Enforce CPU Limits

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodLevelResourcesVerticalScaling, tc.enablePLR)

			mockCM := cmtesting.NewMockContainerManager(t)
			mockCM.EXPECT().PodHasExclusiveCPUs(logger, mock.Anything).Return(false).Maybe()
			mockCM.EXPECT().ContainerHasExclusiveCPUs(logger, mock.Anything, mock.Anything).Return(false).Maybe()
			m.containerManager = mockCM
			mockPCM := cmtesting.NewMockPodContainerManager(t)
			mockCM.EXPECT().NewPodContainerManager().Return(mockPCM)

			mockPCM.EXPECT().GetPodCgroupConfig(mock.Anything, v1.ResourceMemory).Return(&cm.ResourceConfig{
				Memory: ptr.To(tc.currentResources.memoryLimit),
			}, nil).Maybe()
			mockPCM.EXPECT().GetPodCgroupMemoryUsage(mock.Anything).Return(0, nil).Maybe()
			// Set up mock pod cgroup config
			podCPURequest := tc.currentResources.cpuRequest
			podCPULimit := tc.currentResources.cpuLimit
			if tc.otherContainersHaveLimits {
				podCPURequest += 200
				podCPULimit += 200
			}
			mockPCM.EXPECT().GetPodCgroupConfig(mock.Anything, v1.ResourceCPU).Return(&cm.ResourceConfig{
				CPUShares: ptr.To(cm.MilliCPUToShares(podCPURequest)),
				CPUQuota:  ptr.To(cm.MilliCPUToQuota(podCPULimit, cm.QuotaPeriod)),
			}, nil).Maybe()
			if tc.expectPodCgroupUpdates > 0 {
				// TODO: Update to use proper logger once contextual logging migration is complete
				call := mockPCM.EXPECT().SetPodCgroupConfig(klog.TODO(), mock.Anything, mock.Anything)
				if tc.injectPodUpdateCgroupsError != nil {
					call.Return(tc.injectPodUpdateCgroupsError).Times(1)
				} else {
					call.Return(nil).Times(tc.expectPodCgroupUpdates)
				}
			}

			pod, kps := makeBasePodAndStatus()
			if tc.desiredPodLevelResources != nil {
				// pod spec and allocated resources are already updated as desired when doPodResizeAction() is called.
				// doPodResizeAction 调用时，pod spec 与 allocated 已经是目标值。
				pod.Spec.Resources = &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredPodLevelResources.cpuRequest, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(tc.desiredPodLevelResources.memoryRequest, resource.BinarySI),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredPodLevelResources.cpuLimit, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(tc.desiredPodLevelResources.memoryLimit, resource.BinarySI),
					},
				}
			}
			if tc.currentPodLevelResources != nil {
				// Seed initial actuated state
				initialActuated := &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(tc.currentPodLevelResources.cpuRequest, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(tc.currentPodLevelResources.memoryRequest, resource.BinarySI),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(tc.currentPodLevelResources.cpuLimit, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(tc.currentPodLevelResources.memoryLimit, resource.BinarySI),
					},
					// 设置 actuated 初始值，模拟 resize 前状态。
				}
				require.NoError(t, m.actuatedState.SetPodLevelResources(logger, pod.UID, initialActuated))
			}
			pod.Spec.Containers[0].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredResources.cpuRequest, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(tc.desiredResources.memoryRequest, resource.DecimalSI),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredResources.cpuLimit, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(tc.desiredResources.memoryLimit, resource.DecimalSI),
				},
			}
			if tc.otherContainersHaveLimits {
				resourceList := v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("100M"),
				}
				resources := v1.ResourceRequirements{
					Requests: resourceList,
					Limits:   resourceList,
				}
				pod.Spec.Containers[1].Resources = resources
				pod.Spec.Containers[2].Resources = resources
			}

			m.runtimeHelper = &containertest.FakeRuntimeHelper{
				PodStats: map[types.UID]*statsapi.PodStats{
					pod.UID: {
						PodRef: statsapi.PodReference{
							Name:      pod.Name,
							Namespace: pod.Namespace,
							UID:       string(pod.UID),
						},
						Memory: &statsapi.MemoryStats{
							UsageBytes: ptr.To[uint64](20),
						},
						Containers: []statsapi.ContainerStats{{
							Name: pod.Spec.Containers[0].Name,
							Memory: &statsapi.MemoryStats{
								UsageBytes: ptr.To[uint64](10),
							},
						}},
					},
				},
			}

			updateInfo := containerToUpdateInfo{
				container:                 &pod.Spec.Containers[0],
				kubeContainerID:           kps.ContainerStatuses[0].ID,
				desiredContainerResources: tc.desiredResources,
				currentContainerResources: &tc.currentResources,
			}
			containersToUpdate := make(map[v1.ResourceName][]containerToUpdateInfo)
			for _, r := range tc.updatedResources {
				containersToUpdate[r] = []containerToUpdateInfo{updateInfo}
			}

			actions := podActions{
				ContainersToUpdate:      containersToUpdate,
				UpdatePodLevelResources: tc.updatedPodLevelResources,
				SandboxID:               "sandbox-id",
			}
			resizeResult := m.doPodResizeAction(tCtx, pod, kps, actions)

			if tc.expectedError != "" {
				require.Error(t, resizeResult.Error)
				require.EqualError(t, resizeResult.Error, tc.expectedError)
				require.Equal(t, tc.expectedErrorMessage, resizeResult.Message)
			} else {
				require.NoError(t, resizeResult.Error, resizeResult.Message)
				if tc.desiredPodLevelResources != nil {
					// VERIFY Actuated State for successful resizes
					updatedActuated, found := m.actuatedState.GetPodLevelResources(pod.UID)
					require.True(t, found, "actuated resources should exist for pod")
					if tc.enablePLR {
						assert.Equal(t, tc.desiredPodLevelResources.memoryRequest, updatedActuated.Requests.Memory().Value(), tc.testName)
						assert.Equal(t, tc.desiredPodLevelResources.cpuRequest, updatedActuated.Requests.Cpu().MilliValue(), tc.testName)
					} else {
						assert.Equal(t, tc.currentPodLevelResources.memoryRequest, updatedActuated.Requests.Memory().Value(), tc.testName)
						assert.Equal(t, tc.currentPodLevelResources.cpuRequest, updatedActuated.Requests.Cpu().MilliValue(), tc.testName)
					}
				}
			}

			mock.AssertExpectationsForObjects(t, mockPCM)

			testutil.AssertHistogramTotalCount(t, "kubelet_pod_resize_duration_milliseconds", map[string]string{}, i+1)
		})
	}
	metrics.PodResizeDurationMilliseconds.Reset()
}

// mockVolumeResizeRuntimeHelper 记录 ResizeEphemeralVolume 调用并可注入错误。
type mockVolumeResizeRuntimeHelper struct {
	containertest.FakeRuntimeHelper
	resizeCalls []mockResizeCall
	resizeErr   error
}

// mockResizeCall 记录一次卷 resize 的卷名与目标大小。
type mockResizeCall struct {
	volumeName string
	newSize    *resource.Quantity
}

// ResizeEphemeralVolume 记录调用并返回预设错误。
func (f *mockVolumeResizeRuntimeHelper) ResizeEphemeralVolume(_ *v1.Pod, volumeName string, newSize *resource.Quantity) error {
	f.resizeCalls = append(f.resizeCalls, mockResizeCall{volumeName: volumeName, newSize: newSize})
	return f.resizeErr
}

// TestComputeVolumeResizeAction 验证 memory emptyDir 的扩容/缩容判定。
func TestComputeVolumeResizeAction(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 覆盖门控关闭、非 memory emptyDir、扩容与缩容。
	for _, tc := range []struct {
		testName       string
		enableGate     bool
		actuatedLimit  *resource.Quantity
		newLimit       *resource.Quantity
		volMedium      v1.StorageMedium
		expectUpsize   bool
		expectDownsize bool
	}{
		{
			testName:       "Feature gate disabled",
			enableGate:     false,
			actuatedLimit:  nil,
			newLimit:       resource.NewQuantity(100, resource.BinarySI),
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   false,
			expectDownsize: false,
		},
		{
			testName:       "Non-memory emptyDir volume ignored",
			enableGate:     true,
			actuatedLimit:  nil,
			newLimit:       resource.NewQuantity(100, resource.BinarySI),
			volMedium:      v1.StorageMediumDefault,
			expectUpsize:   false,
			expectDownsize: false,
		},
		{
			testName:       "Upsize: new limit > actuated limit",
			enableGate:     true,
			actuatedLimit:  resource.NewQuantity(100, resource.BinarySI),
			newLimit:       resource.NewQuantity(200, resource.BinarySI),
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   true,
			expectDownsize: false,
		},
		{
			testName:       "Downsize: new limit < actuated limit",
			enableGate:     true,
			actuatedLimit:  resource.NewQuantity(200, resource.BinarySI),
			newLimit:       resource.NewQuantity(100, resource.BinarySI),
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   false,
			expectDownsize: true,
		},
		{
			testName:       "No action: limit removed (new limit nil)",
			enableGate:     true,
			actuatedLimit:  resource.NewQuantity(100, resource.BinarySI),
			newLimit:       nil,
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   false,
			expectDownsize: false,
		},
		{
			testName:       "Force sync: new limit specified, no actuated limit",
			enableGate:     true,
			actuatedLimit:  nil,
			newLimit:       resource.NewQuantity(100, resource.BinarySI),
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   true,
			expectDownsize: false,
		},
		{
			testName:       "No change: limits are equal",
			enableGate:     true,
			actuatedLimit:  resource.NewQuantity(100, resource.BinarySI),
			newLimit:       resource.NewQuantity(100, resource.BinarySI),
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   false,
			expectDownsize: false,
		},
		{
			testName:       "No change: both limits nil",
			enableGate:     true,
			actuatedLimit:  nil,
			newLimit:       nil,
			volMedium:      v1.StorageMediumMemory,
			expectUpsize:   false,
			expectDownsize: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingMemoryBackedVolumes, tc.enableGate)

			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "test-pod-uid",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "mem-vol",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    tc.volMedium,
									SizeLimit: tc.newLimit,
								},
							},
						},
					},
				},
			}

			if tc.actuatedLimit != nil {
				// 设置 actuated 上限，模拟上一次已生效的 limit。
				err := m.actuatedState.SetEmptyDirVolumeLimit(pod.UID, "mem-vol", tc.actuatedLimit)
				require.NoError(t, err)
			}

			var changes podActions
			m.computeVolumeResizeAction(tCtx, pod, &changes)

			if tc.expectUpsize {
				require.Len(t, changes.VolumesToUpsize, 1)
				assert.Equal(t, "mem-vol", changes.VolumesToUpsize[0].Name)
				if tc.newLimit != nil {
					require.NotNil(t, changes.VolumesToUpsize[0].EmptyDir.SizeLimit)
					assert.Equal(t, tc.newLimit.Value(), changes.VolumesToUpsize[0].EmptyDir.SizeLimit.Value())
				} else {
					assert.Nil(t, changes.VolumesToUpsize[0].EmptyDir.SizeLimit)
				}
			} else {
				assert.Empty(t, changes.VolumesToUpsize)
			}

			if tc.expectDownsize {
				require.Len(t, changes.VolumesToDownsize, 1)
				assert.Equal(t, "mem-vol", changes.VolumesToDownsize[0].Name)
				if tc.newLimit != nil {
					require.NotNil(t, changes.VolumesToDownsize[0].EmptyDir.SizeLimit)
					assert.Equal(t, tc.newLimit.Value(), changes.VolumesToDownsize[0].EmptyDir.SizeLimit.Value())
				} else {
					assert.Nil(t, changes.VolumesToDownsize[0].EmptyDir.SizeLimit)
				}
			} else {
				assert.Empty(t, changes.VolumesToDownsize)
			}
		})
	}
}

// TestDoPodResizeAction_Volumes 验证卷 resize 的调用顺序、失败中止与 actuated 状态。
func TestDoPodResizeAction_Volumes(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("unsupported OS")
	}

	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingMemoryBackedVolumes, true)

	// 扩容与缩容都通过 runtime helper 的 ResizeEphemeralVolume。
	for _, tc := range []struct {
		testName          string
		volumesToDownsize []string
		volumesToUpsize   []string
		injectResizeError error
		expectedCalls     []mockResizeCall
		expectedActuated  map[string]*resource.Quantity
		expectedResultErr bool
	}{
		{
			testName:          "Successful volume downsize and upsize",
			volumesToDownsize: []string{"down-vol"},
			volumesToUpsize:   []string{"up-vol"},
			expectedCalls: []mockResizeCall{
				{volumeName: "down-vol", newSize: resource.NewQuantity(100, resource.BinarySI)},
				{volumeName: "up-vol", newSize: resource.NewQuantity(200, resource.BinarySI)},
			},
			expectedActuated: map[string]*resource.Quantity{
				"down-vol": resource.NewQuantity(100, resource.BinarySI),
				"up-vol":   resource.NewQuantity(200, resource.BinarySI),
			},
		},
		{
			testName:          "Abort on downsize error",
			volumesToDownsize: []string{"down-vol"},
			volumesToUpsize:   []string{"up-vol"},
			injectResizeError: fmt.Errorf("resize failed"),
			expectedCalls: []mockResizeCall{
				{volumeName: "down-vol", newSize: resource.NewQuantity(100, resource.BinarySI)},
			},
			expectedActuated:  map[string]*resource.Quantity{},
			expectedResultErr: true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			mockCM := cmtesting.NewMockContainerManager(t)
			mockCM.EXPECT().PodHasExclusiveCPUs(mock.Anything, mock.Anything).Return(false).Maybe()
			mockCM.EXPECT().ContainerHasExclusiveCPUs(mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()
			m.containerManager = mockCM
			mockPCM := cmtesting.NewMockPodContainerManager(t)
			mockCM.EXPECT().NewPodContainerManager().Return(mockPCM)

			mockPCM.EXPECT().GetPodCgroupConfig(mock.Anything, v1.ResourceMemory).Return(&cm.ResourceConfig{
				Memory: new(int64(200)),
			}, nil).Maybe()
			mockPCM.EXPECT().GetPodCgroupConfig(mock.Anything, v1.ResourceCPU).Return(&cm.ResourceConfig{
				CPUShares: new(cm.MilliCPUToShares(100)),
				CPUQuota:  new(cm.MilliCPUToQuota(100, cm.QuotaPeriod)),
			}, nil).Maybe()

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "test-pod-uid",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "down-vol",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: resource.NewQuantity(100, resource.BinarySI),
								},
							},
						},
						{
							Name: "up-vol",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: resource.NewQuantity(200, resource.BinarySI),
								},
							},
						},
					},
				},
			}

			// Pre-seed initial state for both volumes so we can observe state changes
			// 预置两个卷的初始 actuated 值，用于观察状态变化。
			require.NoError(t, m.actuatedState.SetEmptyDirVolumeLimit(pod.UID, "down-vol", resource.NewQuantity(300, resource.BinarySI)))
			require.NoError(t, m.actuatedState.SetEmptyDirVolumeLimit(pod.UID, "up-vol", resource.NewQuantity(50, resource.BinarySI)))

			helper := &mockVolumeResizeRuntimeHelper{
				resizeErr: tc.injectResizeError,
			}
			m.runtimeHelper = helper

			var volumesToDownsize []v1.Volume
			for _, name := range tc.volumesToDownsize {
				for _, vol := range pod.Spec.Volumes {
					if vol.Name == name {
						volumesToDownsize = append(volumesToDownsize, vol)
					}
				}
			}
			var volumesToUpsize []v1.Volume
			for _, name := range tc.volumesToUpsize {
				for _, vol := range pod.Spec.Volumes {
					if vol.Name == name {
						volumesToUpsize = append(volumesToUpsize, vol)
					}
				}
			}

			podStatus := &kubecontainer.PodStatus{}
			actions := podActions{
				VolumesToDownsize: volumesToDownsize,
				VolumesToUpsize:   volumesToUpsize,
				SandboxID:         "sandbox-id",
			}

			result := m.doPodResizeAction(tCtx, pod, podStatus, actions)

			if tc.expectedResultErr {
				require.Error(t, result.Error)
			} else {
				require.NoError(t, result.Error)
			}

			// 校验调用次数和顺序；缩容失败时不应继续扩容。
			require.Len(t, helper.resizeCalls, len(tc.expectedCalls), "number of ResizeEphemeralVolume calls")
			for idx, expectedCall := range tc.expectedCalls {
				actualCall := helper.resizeCalls[idx]
				assert.Equal(t, expectedCall.volumeName, actualCall.volumeName)
				if expectedCall.newSize == nil {
					assert.Nil(t, actualCall.newSize)
				} else {
					require.NotNil(t, actualCall.newSize)
					assert.Equal(t, expectedCall.newSize.Value(), actualCall.newSize.Value())
				}
			}

			// Check final actuated state
			for volName, expectedLimit := range tc.expectedActuated {
				limit, found := m.actuatedState.GetEmptyDirVolumeLimit(pod.UID, volName)
				require.True(t, found, "actuated state should exist for %s", volName)
				assert.Equal(t, expectedLimit.Value(), limit.Value(), "actuated state for %s", volName)
			}
			// If downsize failed, upsize is not executed and up-vol stays at initial state (50)
			if tc.expectedResultErr {
				limit, found := m.actuatedState.GetEmptyDirVolumeLimit(pod.UID, "up-vol")
				require.True(t, found)
				assert.Equal(t, int64(50), limit.Value())
			}
		})
	}
}

// TestIsPodResizeInProgress_Volumes 验证卷 resize 是否进行中的判定。
func TestIsPodResizeInProgress_Volumes(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 是否处于 resize 中由 spec 与 actuated limit 的差异决定。
	for _, tc := range []struct {
		testName        string
		enableGate      bool
		specLimit       *resource.Quantity
		statusLimit     *resource.Quantity
		expectHasResize bool
	}{
		{
			testName:        "Feature gate disabled",
			enableGate:      false,
			specLimit:       resource.NewQuantity(100, resource.BinarySI),
			statusLimit:     resource.NewQuantity(200, resource.BinarySI),
			expectHasResize: false,
		},
		{
			testName:        "Spec set, status not set (initial startup safety)",
			enableGate:      true,
			specLimit:       resource.NewQuantity(100, resource.BinarySI),
			statusLimit:     nil,
			expectHasResize: false,
		},
		{
			testName:        "Spec set, status set, both equal",
			enableGate:      true,
			specLimit:       resource.NewQuantity(100, resource.BinarySI),
			statusLimit:     resource.NewQuantity(100, resource.BinarySI),
			expectHasResize: false,
		},
		{
			testName:        "Spec set, status set, they differ",
			enableGate:      true,
			specLimit:       resource.NewQuantity(100, resource.BinarySI),
			statusLimit:     resource.NewQuantity(200, resource.BinarySI),
			expectHasResize: true,
		},
		{
			testName:        "Spec nil (removed), status set",
			enableGate:      true,
			specLimit:       nil,
			statusLimit:     resource.NewQuantity(200, resource.BinarySI),
			expectHasResize: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingMemoryBackedVolumes, tc.enableGate)

			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "test-pod-uid",
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "mem-vol",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: tc.specLimit,
								},
							},
						},
					},
				},
			}

			if tc.statusLimit != nil {
				err := m.actuatedState.SetEmptyDirVolumeLimit(pod.UID, "mem-vol", tc.statusLimit)
				require.NoError(t, err)
			}

			podStatus := &kubecontainer.PodStatus{}
			hasResize := m.IsPodResizeInProgress(pod, podStatus)
			assert.Equal(t, tc.expectHasResize, hasResize)
		})
	}
}

// TestValidatePodResizeAction 验证内存使用量超过目标 limit 时拒绝 resize。
func TestValidatePodResizeAction(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("unsupported OS")
	}

	// 校验内存使用超过目标 limit 时拒绝缩容或添加 limit。
	for _, tc := range []struct {
		testName                               string
		currentResources, desiredResources     resourceRequirements
		currentPodMemLimit, desiredPodMemLimit *int64
		containerMemoryUsage, podMemoryUsage   *uint64
		expectedError                          bool
	}{
		{
			testName: "Resize memory request no limits",
			currentResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				cpuRequest:    100,
				memoryRequest: 200,
			},
			expectedError: false,
		},
		{
			testName: "Add container limits, low usage",
			currentResources: resourceRequirements{
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			containerMemoryUsage: ptr.To[uint64](10),
			podMemoryUsage:       ptr.To[uint64](10),
			expectedError:        false,
		},
		{
			testName: "Add container limits, high usage",
			currentResources: resourceRequirements{
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			containerMemoryUsage: ptr.To[uint64](200),
			podMemoryUsage:       ptr.To[uint64](200),
			expectedError:        true,
		},
		{
			testName: "Add container limits, missing container usage",
			currentResources: resourceRequirements{
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			podMemoryUsage: ptr.To[uint64](10),
			expectedError:  true,
		},
		{
			testName: "Add container limits, missing pod usage",
			currentResources: resourceRequirements{
				memoryRequest: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			containerMemoryUsage: ptr.To[uint64](10),
			expectedError:        false,
		},
		{
			testName: "Increase container limits",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 200,
			},
			expectedError: false,
		},
		{
			testName: "Decrease container limits, low usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 200,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			containerMemoryUsage: ptr.To[uint64](20),
			podMemoryUsage:       ptr.To[uint64](20),
			expectedError:        false,
		},
		{
			testName: "Decrease container limits, high usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 200,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			containerMemoryUsage: ptr.To[uint64](150),
			podMemoryUsage:       ptr.To[uint64](150),
			expectedError:        true,
		},
		{
			testName: "Add pod limit, low usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredPodMemLimit:   ptr.To[int64](100),
			containerMemoryUsage: ptr.To[uint64](20),
			podMemoryUsage:       ptr.To[uint64](20),
			expectedError:        false,
		},
		{
			testName: "Add pod limit, high usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredPodMemLimit:   ptr.To[int64](100),
			containerMemoryUsage: ptr.To[uint64](20),
			podMemoryUsage:       ptr.To[uint64](200),
			expectedError:        true,
		},
		{
			testName: "Increase pod limits",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodMemLimit:   ptr.To[int64](100),
			desiredPodMemLimit:   ptr.To[int64](200),
			containerMemoryUsage: ptr.To[uint64](20),
			podMemoryUsage:       ptr.To[uint64](20),
			expectedError:        false,
		},
		{
			testName: "Decrease pod limits, low usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodMemLimit:   ptr.To[int64](200),
			desiredPodMemLimit:   ptr.To[int64](100),
			containerMemoryUsage: ptr.To[uint64](20),
			podMemoryUsage:       ptr.To[uint64](20),
			expectedError:        false,
		},
		{
			testName: "Decrease pod limits, high usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodMemLimit:   ptr.To[int64](200),
			desiredPodMemLimit:   ptr.To[int64](100),
			containerMemoryUsage: ptr.To[uint64](50),
			podMemoryUsage:       ptr.To[uint64](150),
			expectedError:        true,
		},
		{
			testName: "Decrease pod limits, missing usage",
			currentResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			desiredResources: resourceRequirements{
				memoryRequest: 100, memoryLimit: 100,
			},
			currentPodMemLimit:   ptr.To[int64](200),
			desiredPodMemLimit:   ptr.To[int64](100),
			containerMemoryUsage: ptr.To[uint64](50),
			podMemoryUsage:       nil,
			expectedError:        true,
		},
	} {
		// 通过 FakeRuntimeHelper 提供容器与 pod 的内存使用量。
		t.Run(tc.testName, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod, kps := makeBasePodAndStatus()
			// pod spec and allocated resources are already updated as desired when doPodResizeAction() is called.
			pod.Spec.Containers[0].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredResources.cpuRequest, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(tc.desiredResources.memoryRequest, resource.DecimalSI),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(tc.desiredResources.cpuLimit, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewQuantity(tc.desiredResources.memoryLimit, resource.DecimalSI),
				},
			}

			updateInfo := containerToUpdateInfo{
				container:                 &pod.Spec.Containers[0],
				kubeContainerID:           kps.ContainerStatuses[0].ID,
				desiredContainerResources: tc.desiredResources,
				currentContainerResources: &tc.currentResources,
			}
			containersToUpdate := map[v1.ResourceName][]containerToUpdateInfo{
				v1.ResourceMemory: {updateInfo},
			}

			actions := podActions{
				ContainersToUpdate: containersToUpdate,
			}

			podStats := &statsapi.PodStats{
				PodRef: statsapi.PodReference{
					Name:      pod.Name,
					Namespace: pod.Namespace,
					UID:       string(pod.UID),
				},
				Memory: &statsapi.MemoryStats{
					UsageBytes: tc.podMemoryUsage,
				},
			}
			for _, container := range pod.Spec.Containers {
				podStats.Containers = append(podStats.Containers, statsapi.ContainerStats{
					Name: container.Name,
					Memory: &statsapi.MemoryStats{
						UsageBytes: tc.containerMemoryUsage,
					},
				})
			}
			m.runtimeHelper = &containertest.FakeRuntimeHelper{
				PodStats: map[types.UID]*statsapi.PodStats{pod.UID: podStats},
			}

			currentPodResources := &cm.ResourceConfig{Memory: tc.currentPodMemLimit}
			desiredPodResources := &cm.ResourceConfig{Memory: tc.desiredPodMemLimit}

			err = m.validatePodResizeAction(tCtx, pod, kps, currentPodResources, desiredPodResources, actions)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestIncrementImageVolumeMetrics 验证镜像卷请求、成功与失败指标。
// 直接注册指标，便于通过 legacyregistry 采集。
func TestIncrementImageVolumeMetrics(t *testing.T) {
	legacyregistry.Register(metrics.ImageVolumeRequestedTotal)      //nolint:errcheck
	legacyregistry.Register(metrics.ImageVolumeMountedSucceedTotal) //nolint:errcheck
	legacyregistry.Register(metrics.ImageVolumeMountedErrorsTotal)  //nolint:errcheck

	testCases := map[string]struct {
		err          error
		msg          string
		volumeMounts []v1.VolumeMount
		imageVolumes kubecontainer.ImageVolumes
		wants        string
	}{
		"without error": {
			volumeMounts: []v1.VolumeMount{
				{Name: "mount1"},
				{Name: "mount2"},
			},
			imageVolumes: kubecontainer.ImageVolumes{
				"mount1": {Image: "image1"},
				"mount2": {Image: "image2"},
				"mount3": {Image: "image3"},
			},
			wants: `
				# HELP kubelet_image_volume_mounted_errors_total [BETA] Number of failed image volume mounts.
				# TYPE kubelet_image_volume_mounted_errors_total counter
        		kubelet_image_volume_mounted_errors_total 0
        		# HELP kubelet_image_volume_mounted_succeed_total [BETA] Number of successful image volume mounts.
        		# TYPE kubelet_image_volume_mounted_succeed_total counter
        		kubelet_image_volume_mounted_succeed_total 2
        		# HELP kubelet_image_volume_requested_total [BETA] Number of requested image volumes.
        		# TYPE kubelet_image_volume_requested_total counter
        		kubelet_image_volume_requested_total 3
			`,
		},
		"with error": {
			err: ErrCreateContainer,
			msg: crierror.ErrImageVolumeMountFailed.Error(),
			volumeMounts: []v1.VolumeMount{
				{Name: "mount1"},
				{Name: "mount2"},
			},
			imageVolumes: kubecontainer.ImageVolumes{
				"mount1": {Image: "image1"},
				"mount2": {Image: "image2"},
				"mount3": {Image: "image3"},
			},
			wants: `
				# HELP kubelet_image_volume_mounted_errors_total [BETA] Number of failed image volume mounts.
				# TYPE kubelet_image_volume_mounted_errors_total counter
        		kubelet_image_volume_mounted_errors_total 2
        		# HELP kubelet_image_volume_mounted_succeed_total [BETA] Number of successful image volume mounts.
        		# TYPE kubelet_image_volume_mounted_succeed_total counter
        		kubelet_image_volume_mounted_succeed_total 0
        		# HELP kubelet_image_volume_requested_total [BETA] Number of requested image volumes.
        		# TYPE kubelet_image_volume_requested_total counter
        		kubelet_image_volume_requested_total 3
			`,
		},
		"with unknown CRI error from message": {
			err: ErrCreateContainer,
			msg: "",
			volumeMounts: []v1.VolumeMount{
				{Name: "mount1"},
				{Name: "mount2"},
			},
			imageVolumes: kubecontainer.ImageVolumes{
				"mount1": {Image: "image1"},
				"mount2": {Image: "image2"},
				"mount3": {Image: "image3"},
			},
			wants: `
				# HELP kubelet_image_volume_mounted_errors_total [BETA] Number of failed image volume mounts.
				# TYPE kubelet_image_volume_mounted_errors_total counter
        		kubelet_image_volume_mounted_errors_total 0
        		# HELP kubelet_image_volume_mounted_succeed_total [BETA] Number of successful image volume mounts.
        		# TYPE kubelet_image_volume_mounted_succeed_total counter
        		kubelet_image_volume_mounted_succeed_total 2
        		# HELP kubelet_image_volume_requested_total [BETA] Number of requested image volumes.
        		# TYPE kubelet_image_volume_requested_total counter
        		kubelet_image_volume_requested_total 3
			`,
		},
		"without used volume": {
			volumeMounts: []v1.VolumeMount{
				{Name: "mount1"},
			},
			imageVolumes: kubecontainer.ImageVolumes{},
			wants: `
				# HELP kubelet_image_volume_mounted_errors_total [BETA] Number of failed image volume mounts.
				# TYPE kubelet_image_volume_mounted_errors_total counter
        		kubelet_image_volume_mounted_errors_total 0
        		# HELP kubelet_image_volume_mounted_succeed_total [BETA] Number of successful image volume mounts.
        		# TYPE kubelet_image_volume_mounted_succeed_total counter
        		kubelet_image_volume_mounted_succeed_total 0
        		# HELP kubelet_image_volume_requested_total [BETA] Number of requested image volumes.
        		# TYPE kubelet_image_volume_requested_total counter
        		kubelet_image_volume_requested_total 0
			`,
		},
	}

	// Run tests.
	// 每个用例前重置计数器。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			metrics.ImageVolumeRequestedTotal.Reset()
			metrics.ImageVolumeMountedErrorsTotal.Reset()
			metrics.ImageVolumeMountedSucceedTotal.Reset()

			// Call the function.
			incrementImageVolumeMetrics(tc.err, tc.msg, &v1.Container{VolumeMounts: tc.volumeMounts}, tc.imageVolumes)

			if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(tc.wants),
				"kubelet_"+metrics.ImageVolumeRequestedTotalKey,
				"kubelet_"+metrics.ImageVolumeMountedSucceedTotalKey,
				"kubelet_"+metrics.ImageVolumeMountedErrorsTotalKey,
			); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestIsPodResizeInProgress 验证容器级与 pod 级资源差异下的 resize 判定。
func TestIsPodResizeInProgress(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	type testResources struct {
		cpuReq, cpuLim, memReq, memLim int64
	}
	type testContainer struct {
		allocated               testResources
		actuated                *testResources
		nonSidecarInit, sidecar bool
		isRunning               bool
		unstarted               bool // Whether the container is missing from the pod status
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, true)
	type testPLR struct {
		allocated testResources
		actuated  *testResources
	}

	// 覆盖容器级与 pod 级资源组合下的 resize 判定。
	tests := []struct {
		name                         string
		podLevelResources            *testPLR
		containers                   []testContainer
		expectHasResize              bool
		inplacePodLevelResizeEnabled bool
	}{{
		name: "simple running container",
		containers: []testContainer{{
			allocated: testResources{100, 100, 100, 100},
			actuated:  &testResources{100, 100, 100, 100},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "simple unstarted container",
		containers: []testContainer{{
			allocated: testResources{100, 100, 100, 100},
			unstarted: true,
		}},
		expectHasResize: false,
	}, {
		name: "simple resized container/cpu req",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{150, 200, 100, 200},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "simple resized container/cpu limit",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 300, 100, 200},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "simple resized container/mem req",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 150, 200},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "simple resized container/cpu+mem req",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{150, 200, 150, 200},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "simple resized container/mem limit",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 300},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "terminated resized container",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{200, 200, 100, 200},
			isRunning: false,
		}},
		expectHasResize: false,
	}, {
		name: "non-sidecar init container",
		containers: []testContainer{{
			allocated:      testResources{100, 200, 100, 200},
			nonSidecarInit: true,
			isRunning:      true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 200},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "non-resized sidecar",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 200},
			sidecar:   true,
			isRunning: true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 200},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "resized sidecar",
		containers: []testContainer{{
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{200, 200, 100, 200},
			sidecar:   true,
			isRunning: true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 200},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "several containers and a resize",
		containers: []testContainer{{
			allocated:      testResources{100, 200, 100, 200},
			nonSidecarInit: true,
			isRunning:      true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{100, 200, 100, 200},
			isRunning: true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			unstarted: true,
		}, {
			allocated: testResources{100, 200, 100, 200},
			actuated:  &testResources{200, 200, 100, 200}, // Resized
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "several containers",
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100, cpuLim: 200},
			actuated:  &testResources{cpuReq: 100, cpuLim: 200},
			sidecar:   true,
			isRunning: true,
		}, {
			allocated: testResources{memReq: 100, memLim: 200},
			actuated:  &testResources{memReq: 100, memLim: 200},
			isRunning: true,
		}, {
			allocated: testResources{cpuReq: 200, memReq: 100},
			actuated:  &testResources{cpuReq: 200, memReq: 100},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "best-effort pod",
		containers: []testContainer{{
			allocated: testResources{},
			actuated:  &testResources{},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "burstable pod/not resizing",
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 100},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "burstable pod/resized",
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 500},
			isRunning: true,
		}},
		expectHasResize: true,
	}, {
		name: "only-plr/not resizing",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 100},
		},
		containers: []testContainer{{
			allocated: testResources{},
			actuated:  &testResources{},
			isRunning: true,
		}},
		expectHasResize: false,
	}, {
		name: "only-plr/resizing",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 200},
			actuated:  &testResources{cpuReq: 100},
		},
		containers: []testContainer{{
			allocated: testResources{},
			actuated:  &testResources{},
			isRunning: true,
		}},
		expectHasResize:              true,
		inplacePodLevelResizeEnabled: true,
	}, {
		name: "only-plr/resizing feature gate disabled",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 200},
			actuated:  &testResources{cpuReq: 100},
		},
		containers: []testContainer{{
			allocated: testResources{},
			actuated:  &testResources{},
			isRunning: true,
		}},
		expectHasResize:              false,
		inplacePodLevelResizeEnabled: false,
	}, {
		name: "plr/container resizing featuregate disabled",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 100},
		},
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 50},
			isRunning: true,
		}},
		expectHasResize:              true,
		inplacePodLevelResizeEnabled: false,
	}, {
		name: "plr/container resizing",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 100},
		},
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100},
			actuated:  &testResources{cpuReq: 50},
			isRunning: true,
		}},
		expectHasResize:              true,
		inplacePodLevelResizeEnabled: true,
	}, {
		name: "plr/pod resizing",
		podLevelResources: &testPLR{
			allocated: testResources{memReq: 100},
			actuated:  &testResources{memReq: 200},
		},
		containers: []testContainer{{
			allocated: testResources{memReq: 100},
			actuated:  &testResources{memReq: 100},
			isRunning: true,
		}},
		expectHasResize:              true,
		inplacePodLevelResizeEnabled: true,
	}, {
		name: "plr mismatch during initial creation",
		podLevelResources: &testPLR{
			allocated: testResources{cpuReq: 200},
		},
		containers: []testContainer{{
			allocated: testResources{cpuReq: 100},
			isRunning: false,
		}},
		expectHasResize:              false,
		inplacePodLevelResizeEnabled: true,
	}}

	mkRequirements := func(r testResources) v1.ResourceRequirements {
		res := v1.ResourceRequirements{
			Requests: v1.ResourceList{},
			Limits:   v1.ResourceList{},
		}
		if r.cpuReq != 0 {
			res.Requests[v1.ResourceCPU] = *resource.NewMilliQuantity(r.cpuReq, resource.DecimalSI)
		}
		if r.cpuLim != 0 {
			res.Limits[v1.ResourceCPU] = *resource.NewMilliQuantity(r.cpuLim, resource.DecimalSI)
		}
		if r.memReq != 0 {
			res.Requests[v1.ResourceMemory] = *resource.NewQuantity(r.memReq, resource.DecimalSI)
		}
		if r.memLim != 0 {
			res.Limits[v1.ResourceMemory] = *resource.NewQuantity(r.memLim, resource.DecimalSI)
		}
		return res
	}
	mkContainer := func(index int, c testContainer) v1.Container {
		container := v1.Container{
			Name:      fmt.Sprintf("c%d", index),
			Resources: mkRequirements(c.allocated),
		}
		if c.sidecar {
			container.RestartPolicy = ptr.To(v1.ContainerRestartPolicyAlways)
		}
		return container
	}

	// 构造 pod 与状态：未启动容器不进入 status，已启动容器标记运行或退出。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodLevelResourcesVerticalScaling, test.inplacePodLevelResizeEnabled)
			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					UID:  "12345",
				},
			}
			podStatus := &kubecontainer.PodStatus{
				ID:   pod.UID,
				Name: pod.Name,
			}

			for i, c := range test.containers {
				// Add the container to the pod
				container := mkContainer(i, c)
				if c.nonSidecarInit || c.sidecar {
					pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
				} else {
					pod.Spec.Containers = append(pod.Spec.Containers, container)
				}

				// Add the container to the pod status, if it's started.
				if !test.containers[i].unstarted {
					cs := kubecontainer.Status{
						Name: container.Name,
					}
					if test.containers[i].isRunning {
						cs.State = kubecontainer.ContainerStateRunning
					} else {
						cs.State = kubecontainer.ContainerStateExited
					}
					podStatus.ContainerStatuses = append(podStatus.ContainerStatuses, &cs)
				}

				// Register the actuated container (if needed)
				// 登记 actuated 资源；缺失时 IsPodResizeInProgress 应返回 false。
				if c.actuated != nil {
					actuatedContainer := container.DeepCopy()
					actuatedContainer.Resources = mkRequirements(*c.actuated)
					require.NoError(t, m.actuatedState.SetContainerResources(logger, pod.UID, actuatedContainer.Name, actuatedContainer.Resources))

					fetched, found := m.actuatedState.GetContainerResources(pod.UID, container.Name)
					require.True(t, found)
					assert.Equal(t, actuatedContainer.Resources, fetched)
				} else {
					_, found := m.actuatedState.GetContainerResources(pod.UID, container.Name)
					require.False(t, found)
				}
			}

			if test.podLevelResources != nil {
				reqs := mkRequirements(test.podLevelResources.allocated)
				pod.Spec.Resources = &reqs

				if test.podLevelResources.actuated != nil {
					actuatedReqs := mkRequirements(*test.podLevelResources.actuated)
					require.NoError(t, m.actuatedState.SetPodLevelResources(logger, pod.UID, &actuatedReqs))

					fetched, found := m.actuatedState.GetPodLevelResources(pod.UID)
					require.True(t, found)
					assert.Equal(t, &actuatedReqs, fetched)
				}
			}

			resizeInProgress := m.IsPodResizeInProgress(pod, podStatus)
			assert.Equal(t, test.expectHasResize, resizeInProgress)
		})
	}
}

// TestDoBackOff 验证崩溃循环 backoff 的判定与错误信息。
// 用 fake clock 控制 backoff 的时间推进。
func TestDoBackOff(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	tests := []struct {
		name              string
		podStatus         *kubecontainer.PodStatus
		backoff           *flowcontrol.Backoff
		backoffUpdateFn   func(*flowcontrol.Backoff, *v1.Pod, *kubecontainer.PodStatus)
		expectedInBackOff bool
		expectedError     error
	}{
		{
			name: "container running",
			podStatus: &kubecontainer.PodStatus{
				ContainerStatuses: []*kubecontainer.Status{
					{
						Name:  "foocontainer",
						State: kubecontainer.ContainerStateRunning,
					},
				},
			},
			backoff:           flowcontrol.NewFakeBackOff(time.Second, time.Minute, fakeClock),
			expectedInBackOff: false,
		},
		{
			name: "not in backoff",
			podStatus: &kubecontainer.PodStatus{
				ContainerStatuses: []*kubecontainer.Status{
					{
						Name:       "foocontainer",
						State:      kubecontainer.ContainerStateExited,
						FinishedAt: fakeClock.Now(),
					},
				},
			},
			backoff:           flowcontrol.NewFakeBackOff(time.Second, time.Minute, fakeClock),
			expectedInBackOff: false,
		},
		{
			name: "in backoff",
			podStatus: &kubecontainer.PodStatus{
				ContainerStatuses: []*kubecontainer.Status{
					{
						Name:       "foocontainer",
						State:      kubecontainer.ContainerStateExited,
						FinishedAt: fakeClock.Now(),
					},
				},
			},
			backoff: flowcontrol.NewFakeBackOff(time.Second, time.Minute, fakeClock),
			backoffUpdateFn: func(backoff *flowcontrol.Backoff, pod *v1.Pod, podStatus *kubecontainer.PodStatus) {
				key := GetBackoffKey(pod, &pod.Spec.Containers[0])
				backoff.Next(key, podStatus.ContainerStatuses[0].FinishedAt)
			},
			expectedInBackOff: true,
			expectedError:     kubecontainer.NewBackoffError(kubecontainer.ErrCrashLoopBackOff, fakeClock.Now().Add(time.Second)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, _, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       "12345678",
					Name:      "foo",
					Namespace: "new",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "foocontainer",
							Image: "busybox",
						},
					},
				},
			}
			container := &pod.Spec.Containers[0]

			// 先让 backoff 记录一次失败，再验证 doBackOff 的判定。
			if tc.backoffUpdateFn != nil {
				tc.backoffUpdateFn(tc.backoff, pod, tc.podStatus)
			}

			inBackOff, msg, err := m.doBackOff(tCtx, pod, container, tc.podStatus, tc.backoff)
			assert.Equal(t, tc.expectedInBackOff, inBackOff)
			assert.Equal(t, tc.expectedError, err)
			if tc.expectedInBackOff {
				assert.NotEmpty(t, msg)
			} else {
				assert.Empty(t, msg)
			}
		})
	}
}

// TestCmpActuatedAllocated 验证 cpuMemoryResourcesEqual 对 nil/空/相等资源的比较。
func TestCmpActuatedAllocated(t *testing.T) {
	tests := []struct {
		name               string
		actuatedResources  *v1.ResourceRequirements
		allocatedResources *v1.ResourceRequirements
		cpuMemoryequal     bool
	}{
		{
			name:               "both nil",
			actuatedResources:  nil,
			allocatedResources: nil,
			cpuMemoryequal:     true,
		},
		{
			name:               "actuated nil and allocated empty",
			actuatedResources:  nil,
			allocatedResources: &v1.ResourceRequirements{},
			cpuMemoryequal:     true,
		},
		{
			name:               "actuated empty allocated nil",
			actuatedResources:  &v1.ResourceRequirements{},
			allocatedResources: nil,
			cpuMemoryequal:     true,
		},
		{
			name: "actuated empty and allocated empty",
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{},
				Limits:   v1.ResourceList{},
			},
			allocatedResources: &v1.ResourceRequirements{},
			cpuMemoryequal:     true,
		},
		{
			name:              "actuated empty and allocated requests limits empty",
			actuatedResources: &v1.ResourceRequirements{},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{},
				Limits:   v1.ResourceList{},
			},
			cpuMemoryequal: true,
		},
		{
			name:              "actuated empty and allocated request limits nil",
			actuatedResources: &v1.ResourceRequirements{},
			allocatedResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits:   nil,
			},
			cpuMemoryequal: true,
		},
		{
			name: "actuated requests limits nil and allocated empty",
			actuatedResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits:   nil,
			},
			allocatedResources: &v1.ResourceRequirements{},
			cpuMemoryequal:     true,
		},
		{
			name: "actuated requests limits nil and allocated requests non-nil",
			actuatedResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits:   nil,
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("10m"),
				},
				Limits: nil,
			},
		},
		{
			name: "actuated requests limits nil and allocated requests non-nil limits nil",
			actuatedResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits:   nil,
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: nil,
			},
		},
		{
			name: "actuated requests non-nil limits nil and allocated requests non-nil limits nil",
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: nil,
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: nil,
			},
			cpuMemoryequal: true,
		},
		{
			name: "actuated requests non-nil limits nil and allocated requests nil limits non-nil",
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: nil,
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
			},
		},
		{
			name: "actuated equals allocated",
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("20Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("20m"),
				},
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("20Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("20m"),
				},
			},
			cpuMemoryequal: true,
		},
		{
			name: "actuated equals allocated for resizable resources",
			actuatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("20Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("20m"),
				},
			},
			allocatedResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("20Mi"),
					v1.ResourceCPU:    resource.MustParse("10m"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("10Mi"),
					v1.ResourceCPU:    resource.MustParse("20m"),
				},
			},
			cpuMemoryequal: true,
		},
	}

	// 只比较 CPU/内存等可调整资源，忽略其他字段。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotEqual := cpuMemoryResourcesEqual(test.actuatedResources, test.allocatedResources)
			assert.Equal(t, test.cpuMemoryequal, gotEqual)
		})
	}
}

// testRuntimeHelper 实现 RuntimeHelper 接口，用于测试 OnPodSandboxReady 的调用。
type testRuntimeHelper struct {
	*containertest.FakeRuntimeHelper
	onPodSandboxReadyCalled       bool
	onPodSandboxReadyPod          *v1.Pod
	onPodSandboxReadyCtx          context.Context
	onPodSandboxReadyError        error
	captureStateFunc              func() // optional function to capture state when OnPodSandboxReady is called
	prepareDynamicResourcesCalled bool
	prepareDynamicResourcesError  error
}

// OnPodSandboxReady 记录调用参数与时机，并返回预设错误。
func (t *testRuntimeHelper) OnPodSandboxReady(ctx context.Context, pod *v1.Pod) error {
	t.onPodSandboxReadyCalled = true
	t.onPodSandboxReadyPod = pod
	t.onPodSandboxReadyCtx = ctx
	if t.captureStateFunc != nil {
		t.captureStateFunc()
	}
	return t.onPodSandboxReadyError
}

// PrepareDynamicResources 记录是否被调用，并返回预设错误。
func (t *testRuntimeHelper) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	t.prepareDynamicResourcesCalled = true
	return t.prepareDynamicResourcesError
}

// TestOnPodSandboxReadyInvocation 验证 OnPodSandboxReady 在正确时机被调用，
// 并校验 DRA 分配调用与 PodReadyToStartContainers 条件之间的顺序。
// 测试步骤如下：
// 1. 构造测试 helper 并注入错误
// 2. 创建 pod（带或不带设备）
// 3. 执行 SyncPod
// 4. 校验设备分配
// 5. 校验 OnPodSandboxReady 的调用
// 6. 校验最终 pod 状态
func TestOnPodSandboxReadyInvocation(t *testing.T) {
	tCtx := ktesting.Init(t)

	tests := []struct {
		name                       string
		onPodSandboxReadyShouldErr bool
		deviceAllocationShouldErr  bool
		expectOnPodSandboxReady    bool
		expectSyncPodSuccess       bool
		expectDeviceAllocation     bool
		description                string
	}{
		{
			name:                       "OnPodSandboxReady succeeds",
			onPodSandboxReadyShouldErr: false,
			deviceAllocationShouldErr:  false,
			expectOnPodSandboxReady:    true,
			expectSyncPodSuccess:       true,
			expectDeviceAllocation:     false,
			description:                "Verifies OnPodSandboxReady is called and succeeds",
		},
		{
			name:                       "OnPodSandboxReady fails but SyncPod continues",
			onPodSandboxReadyShouldErr: true,
			deviceAllocationShouldErr:  false,
			expectOnPodSandboxReady:    true,
			expectSyncPodSuccess:       true, // SyncPod still succeed even if OnPodSandboxReady fails
			expectDeviceAllocation:     false,
			description:                "Verifies OnPodSandboxReady errors don't block pod creation",
		},
		{
			name:                       "PrepareDynamicResources (device allocation) called before OnPodSandboxReady",
			onPodSandboxReadyShouldErr: false,
			deviceAllocationShouldErr:  false,
			expectOnPodSandboxReady:    true,
			expectSyncPodSuccess:       true,
			expectDeviceAllocation:     true,
			description:                "Verifies the order (PrepareDynamicResources -> OnPodSandboxReady) in case of pod with ResourceClaims",
		},
		{
			name:                       "PrepareDynamicResources (device allocation) failure prevents sandbox creation",
			onPodSandboxReadyShouldErr: false,
			deviceAllocationShouldErr:  true,
			expectOnPodSandboxReady:    false,
			expectSyncPodSuccess:       true, // SyncPod doesn't return error, just returns early if `PrepareDynamicResources` call ends up failing
			expectDeviceAllocation:     true,
			description:                "Verifies PrepareDynamicResources failure causes early return in case of pod with ResourceClaims",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// step 1 - setup test helper and inject errors
			// step 1 - 构造 helper，并按用例注入错误。
			fakeRuntime, fakeImage, m, err := createTestRuntimeManager(tCtx)
			require.NoError(t, err)

			testHelper := &testRuntimeHelper{
				FakeRuntimeHelper: &containertest.FakeRuntimeHelper{},
			}
			if test.onPodSandboxReadyShouldErr {
				testHelper.onPodSandboxReadyError = fmt.Errorf("OnPodSandboxReady intentionally failed for testing")
			}
			if test.deviceAllocationShouldErr {
				testHelper.prepareDynamicResourcesError = fmt.Errorf("PrepareDynamicResources intentionally failed for testing")
			}
			m.runtimeHelper = testHelper

			// step 2 - create pod (with/without devices)
			// step 2 - 只有带 ResourceClaims 的 pod 才触发 DRA 分配。
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       "test-pod-uid",
					Name:      "test-pod",
					Namespace: "test-namespace",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:            "test-container",
							Image:           "busybox",
							ImagePullPolicy: v1.PullIfNotPresent,
						},
					},
				},
			}
			if test.expectDeviceAllocation {
				pod.Spec.ResourceClaims = []v1.PodResourceClaim{
					{
						Name: "test-device",
					},
				}
			}

			// step 3 - run SyncPod
			// step 3 - 执行同步。
			backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
			result := m.SyncPod(tCtx, pod, &kubecontainer.PodStatus{}, []v1.Secret{}, backOff, false)

			if test.expectSyncPodSuccess {
				require.NoError(t, result.Error(), test.description)
			} else {
				require.Error(t, result.Error(), test.description)
			}

			// step 4 - verify device allocation
			// step 4 - 验证 DRA 分配调用。
			if test.expectDeviceAllocation {
				require.True(t, testHelper.prepareDynamicResourcesCalled,
					"PrepareDynamicResources should be called for pods with resource claims")

				if test.expectOnPodSandboxReady && testHelper.onPodSandboxReadyCalled {
					require.True(t, testHelper.prepareDynamicResourcesCalled,
						"PrepareDynamicResources must be called before OnPodSandboxReady")
				}
			}

			// step 5 - verify OnPodSandboxReady invocation
			// step 5 - 验证 OnPodSandboxReady 调用。
			assert.Equal(t, test.expectOnPodSandboxReady, testHelper.onPodSandboxReadyCalled, "OnPodSandboxReady invocation mismatch: "+test.description)

			if test.expectOnPodSandboxReady {
				assert.NotNil(t, testHelper.onPodSandboxReadyPod, "OnPodSandboxReady should receive pod")
				assert.Equal(t, pod.UID, testHelper.onPodSandboxReadyPod.UID, "OnPodSandboxReady should receive correct pod UID")
				assert.Equal(t, pod.Name, testHelper.onPodSandboxReadyPod.Name, "OnPodSandboxReady should receive correct pod name")
				assert.Equal(t, pod.Namespace, testHelper.onPodSandboxReadyPod.Namespace, "OnPodSandboxReady should receive correct pod namespace")
				assert.NotNil(t, testHelper.onPodSandboxReadyCtx, "OnPodSandboxReady should receive context")

				require.Len(t, fakeRuntime.Sandboxes, 1, "sandbox should be created before OnPodSandboxReady")
				for _, sandbox := range fakeRuntime.Sandboxes {
					require.Equal(t, runtimeapi.PodSandboxState_SANDBOX_READY, sandbox.State, "sandbox should be ready when OnPodSandboxReady is invoked")
				}
			}

			// step 6 - verify the final pod state
			// step 6 - 验证最终 runtime 状态。
			if test.expectSyncPodSuccess && !test.deviceAllocationShouldErr {
				assert.Len(t, fakeRuntime.Containers, 1, "container should be created")
				assert.Len(t, fakeImage.Images, 1, "image should be pulled")
				for _, c := range fakeRuntime.Containers {
					assert.Equal(t, runtimeapi.ContainerState_CONTAINER_RUNNING, c.State, "container should be running")
				}
			}

			if test.deviceAllocationShouldErr {
				require.Empty(t, fakeRuntime.Sandboxes, "sandbox should not be created when device allocation fails")
				require.Empty(t, fakeRuntime.Containers, "containers should not be created when device allocation fails")
			}
		})
	}
}

// TestOnPodSandboxReadyTiming 验证 OnPodSandboxReady 在 sandbox 创建与网络配置之后、
// 镜像拉取之前被调用。
func TestOnPodSandboxReadyTiming(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, fakeImage, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// track the state of pod when OnPodSandboxReady is invoked
	// 在回调中捕获 sandbox/container/image 数量，验证调用时机。
	var sandboxCount int
	var containerCount int
	var imageCount int

	testHelper := &testRuntimeHelper{
		FakeRuntimeHelper: &containertest.FakeRuntimeHelper{},
		captureStateFunc: func() {
			sandboxCount = len(fakeRuntime.Sandboxes)
			containerCount = len(fakeRuntime.Containers)
			imageCount = len(fakeImage.Images)
		},
	}

	m.runtimeHelper = testHelper

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "timing-test-pod",
			Name:      "timing-test",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "test-container",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	result := m.SyncPod(tCtx, pod, &kubecontainer.PodStatus{}, []v1.Secret{}, backOff, false)
	require.NoError(t, result.Error())

	// verify the order that OnPodSandboxReady should be invoked after sandbox creation but before containers
	// OnPodSandboxReady 必须在 sandbox 创建之后、容器创建之前调用。
	assert.Equal(t, 1, sandboxCount, "sandbox should exist when OnPodSandboxReady is invoked")
	assert.Equal(t, 0, containerCount, "containers should not exist yet when OnPodSandboxReady is invoked")
	// Note that image may or may not be pulled at OnPodSandboxReady time depending on whether image exists
	t.Logf("At OnPodSandboxReady time: sandboxes=%d, containers=%d, images=%d", sandboxCount, containerCount, imageCount)

	// verify the final state of pod
	assert.Len(t, fakeRuntime.Sandboxes, 1, "final sandbox count")
	assert.Len(t, fakeRuntime.Containers, 1, "final container count")
}

// TestSysctlFiltering 验证默认 sysctl 按 hostNetwork/hostIPC/hostUsers 过滤。
// 验证默认 sysctl 会根据 hostNetwork/hostIPC/hostUsers 过滤。
func TestSysctlFiltering(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DefaultPodSysctls, true)
	_, _, m, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)
	m.defaultPodSysctls = map[string]string{
		"net.somaxconn":            "1024",
		"kernel.msgmax":            "true",
		"fs.mqueue.msg_max":        "1024",
		"kernel.domainname":        "my-name",
		"non.whitelisted":          "true",
		"net/ipv4/ip_forward":      "1",
		"kernel/sem":               "250 32000 32 128",
		"user.max_user_namespaces": "1000",
	}

	// 用不同网络/IPC/user namespace 组合构造 pod。
	createTestPodFunc := func(hostNetwork, hostIPC bool, hostUsers *bool) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345678",
				Name:      "foo",
				Namespace: "new",
			},
			Spec: v1.PodSpec{
				HostNetwork: hostNetwork,
				HostIPC:     hostIPC,
				HostUsers:   hostUsers,
				Containers: []v1.Container{
					{
						Name:            "foo1",
						Image:           "busybox",
						ImagePullPolicy: v1.PullIfNotPresent,
					},
				},
			},
		}
	}

	tests := []struct {
		name            string
		hostNetwork     bool
		hostIPC         bool
		hostUsers       *bool
		expectedSysctls map[string]string
	}{
		{
			name: "default",
			expectedSysctls: map[string]string{
				"net.somaxconn":       "1024",
				"net.ipv4.ip_forward": "1",
				"kernel.msgmax":       "true",
				"fs.mqueue.msg_max":   "1024",
				"kernel.sem":          "250 32000 32 128",
				"kernel.domainname":   "my-name",
			},
		},
		{
			name:        "hostNetwork",
			hostNetwork: true,
			expectedSysctls: map[string]string{
				"kernel.msgmax":     "true",
				"fs.mqueue.msg_max": "1024",
				"kernel.sem":        "250 32000 32 128",
			},
		},
		{
			name:    "hostIPC",
			hostIPC: true,
			expectedSysctls: map[string]string{
				"net.somaxconn":       "1024",
				"net.ipv4.ip_forward": "1",
				"kernel.domainname":   "my-name",
			},
		},
		{
			name:            "hostNetwork and hostIPC",
			hostNetwork:     true,
			hostIPC:         true,
			expectedSysctls: map[string]string{},
		},
		{
			name:      "pod uses userNS",
			hostUsers: new(false),
			expectedSysctls: map[string]string{
				"net.somaxconn":            "1024",
				"net.ipv4.ip_forward":      "1",
				"kernel.msgmax":            "true",
				"fs.mqueue.msg_max":        "1024",
				"kernel.sem":               "250 32000 32 128",
				"kernel.domainname":        "my-name",
				"user.max_user_namespaces": "1000",
			},
		},
		{
			name:      "pod uses hostUsers",
			hostUsers: new(true),
			expectedSysctls: map[string]string{
				"net.somaxconn":       "1024",
				"net.ipv4.ip_forward": "1",
				"kernel.msgmax":       "true",
				"fs.mqueue.msg_max":   "1024",
				"kernel.sem":          "250 32000 32 128",
				"kernel.domainname":   "my-name",
			},
		},
	}

	// 只校验最终 sandbox config 中保留下来的 sysctl。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			pod := createTestPodFunc(test.hostNetwork, test.hostIPC, test.hostUsers)
			config, err := m.generatePodSandboxLinuxConfig(tCtx, pod)
			require.NoError(t, err)
			if !reflect.DeepEqual(test.expectedSysctls, config.Sysctls) {
				t.Errorf("Expected sysctls %v, got %v", test.expectedSysctls, config.Sysctls)
			}
		})
	}
}
