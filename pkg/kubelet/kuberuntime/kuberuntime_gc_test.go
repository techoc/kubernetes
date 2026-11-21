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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestSandboxGC 覆盖 evictSandboxes 在不同 sandbox 状态与 Pod 生命周期下的回收行为。
func TestSandboxGC(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	podStateProvider := m.containerGC.podStateProvider.(*fakePodStateProvider)
	makeGCSandbox := func(pod *v1.Pod, attempt uint32, state runtimeapi.PodSandboxState, hasRunningContainers, isTerminating bool, createdAt int64) sandboxTemplate {
		return sandboxTemplate{
			pod:         pod,
			state:       state,
			attempt:     attempt,
			createdAt:   createdAt,
			running:     hasRunningContainers,
			terminating: isTerminating,
		}
	}

	pods := []*v1.Pod{
		makeTestPod("foo1", "new", "1234", []v1.Container{
			makeTestContainer("bar1", "busybox"),
			makeTestContainer("bar2", "busybox"),
		}),
		makeTestPod("foo2", "new", "5678", []v1.Container{
			makeTestContainer("bar3", "busybox"),
		}),
		makeTestPod("deleted", "new", "9012", []v1.Container{
			makeTestContainer("bar4", "busybox"),
		}),
	}

	for _, test := range []struct {
		description          string              // 测试用例描述
		sandboxes            []sandboxTemplate   // sandbox 模板列表
		containers           []containerTemplate // 容器模板列表
		remain               []int               // 剩余 sandbox 的模板索引
		evictTerminatingPods bool
	}{
		{
			description: "notready sandboxes without containers for deleted pods should be garbage collected.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[2], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, false, false, 0),
			},
			containers:           []containerTemplate{},
			remain:               []int{},
			evictTerminatingPods: false,
		},
		{
			description: "ready sandboxes without containers for deleted pods should not be garbage collected.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[2], 0, runtimeapi.PodSandboxState_SANDBOX_READY, false, false, 0),
			},
			containers:           []containerTemplate{},
			remain:               []int{0},
			evictTerminatingPods: false,
		},
		{
			description: "sandboxes for existing pods should not be garbage collected.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_READY, true, false, 0),
				makeGCSandbox(pods[1], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 0),
			},
			containers:           []containerTemplate{},
			remain:               []int{0, 1},
			evictTerminatingPods: false,
		},
		{
			description: "older exited sandboxes without containers for existing pods should be garbage collected if there are more than one exited sandboxes.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[0], 1, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 1),
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 0),
			},
			containers:           []containerTemplate{},
			remain:               []int{0},
			evictTerminatingPods: false,
		},
		{
			description: "older exited sandboxes with containers for existing pods should not be garbage collected even if there are more than one exited sandboxes.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[0], 1, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 1),
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 0),
			},
			containers: []containerTemplate{
				{pod: pods[0], container: &pods[0].Spec.Containers[0], sandboxAttempt: 0, state: runtimeapi.ContainerState_CONTAINER_EXITED},
			},
			remain:               []int{0, 1},
			evictTerminatingPods: false,
		},
		{
			description: "non-running sandboxes for existing pods should be garbage collected if evictTerminatingPods is set.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_READY, true, true, 0),
				makeGCSandbox(pods[1], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, true, 0),
			},
			containers:           []containerTemplate{},
			remain:               []int{0},
			evictTerminatingPods: true,
		},
		{
			description: "sandbox with containers should not be garbage collected.",
			sandboxes: []sandboxTemplate{
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, false, false, 0),
			},
			containers: []containerTemplate{
				{pod: pods[0], container: &pods[0].Spec.Containers[0], state: runtimeapi.ContainerState_CONTAINER_EXITED},
			},
			remain:               []int{0},
			evictTerminatingPods: false,
		},
		{
			description: "multiple sandboxes should be handled properly.",
			sandboxes: []sandboxTemplate{
				// 运行中的 sandbox。
				makeGCSandbox(pods[0], 1, runtimeapi.PodSandboxState_SANDBOX_READY, true, false, 1),
				// 已退出且没有容器的 sandbox。
				makeGCSandbox(pods[0], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 0),
				// 已退出但仍有容器的 sandbox。
				makeGCSandbox(pods[1], 1, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 1),
				// 已退出且没有容器的 sandbox。
				makeGCSandbox(pods[1], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, true, false, 0),
				// 已删除 Pod 的已退出且无容器的 sandbox。
				makeGCSandbox(pods[2], 0, runtimeapi.PodSandboxState_SANDBOX_NOTREADY, false, true, 0),
			},
			containers: []containerTemplate{
				{pod: pods[1], container: &pods[1].Spec.Containers[0], sandboxAttempt: 1, state: runtimeapi.ContainerState_CONTAINER_EXITED},
			},
			remain:               []int{0, 2},
			evictTerminatingPods: false,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			// 每个子测试前重置 Pod 状态，避免用例之间相互影响。
			podStateProvider.removed = make(map[types.UID]struct{})
			podStateProvider.terminated = make(map[types.UID]struct{})
			fakeSandboxes := makeFakePodSandboxes(tCtx, m, test.sandboxes)
			fakeContainers := makeFakeContainers(tCtx, m, test.containers)
			// 根据 sandbox 模板构造已删除或已终止 Pod 的状态集合。
			for _, s := range test.sandboxes {
				if !s.running && s.pod.Name == "deleted" {
					podStateProvider.removed[s.pod.UID] = struct{}{}
				}
				if s.terminating {
					podStateProvider.terminated[s.pod.UID] = struct{}{}
				}
			}
			fakeRuntime.SetFakeSandboxes(fakeSandboxes)
			fakeRuntime.SetFakeContainers(fakeContainers)

			err := m.containerGC.evictSandboxes(tCtx, test.evictTerminatingPods)
			assert.NoError(t, err)
			realRemain, err := fakeRuntime.ListPodSandbox(tCtx, nil)
			assert.NoError(t, err)
			assert.Len(t, realRemain, len(test.remain))
			for _, remain := range test.remain {
				resp, err := fakeRuntime.PodSandboxStatus(tCtx, fakeSandboxes[remain].Id, false)
				assert.NoError(t, err)
				assert.Equal(t, &fakeSandboxes[remain].PodSandboxStatus, resp.Status)
			}
		})
	}
}

// makeGCContainer 构造用于 GC 测试的容器模板。
func makeGCContainer(podName, containerName string, attempt int, createdAt int64, state runtimeapi.ContainerState) containerTemplate {
	container := makeTestContainer(containerName, "test-image")
	pod := makeTestPod(podName, "test-ns", podName, []v1.Container{container})
	return containerTemplate{
		pod:       pod,
		container: &container,
		attempt:   attempt,
		createdAt: createdAt,
		state:     state,
	}
}

// TestContainerGC 覆盖 evictContainers 在各类 GC 策略与 Pod 状态下的回收行为。
func TestContainerGC(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	podStateProvider := m.containerGC.podStateProvider.(*fakePodStateProvider)
	defaultGCPolicy := kubecontainer.GCPolicy{MinAge: time.Hour, MaxPerPodContainer: 2, MaxContainers: 6}

	for _, test := range []struct {
		description          string                  // 测试用例描述
		containers           []containerTemplate     // 容器模板列表
		policy               *kubecontainer.GCPolicy // 容器 GC 策略
		remain               []int                   // 剩余容器的模板索引
		evictTerminatingPods bool
		allSourcesReady      bool
	}{
		{
			description: "all containers should be removed when max container limit is 0",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			policy:               &kubecontainer.GCPolicy{MinAge: time.Minute, MaxPerPodContainer: 1, MaxContainers: 0},
			remain:               []int{},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "max containers should be complied when no max per pod container limit is set",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 4, 4, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 3, 3, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			policy:               &kubecontainer.GCPolicy{MinAge: time.Minute, MaxPerPodContainer: -1, MaxContainers: 4},
			remain:               []int{0, 1, 2, 3},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "no containers should be removed if both max container and per pod container limits are not set",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			policy:               &kubecontainer.GCPolicy{MinAge: time.Minute, MaxPerPodContainer: -1, MaxContainers: -1},
			remain:               []int{0, 1, 2},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "recently started containers should not be removed",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, time.Now().UnixNano(), runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, time.Now().UnixNano(), runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, time.Now().UnixNano(), runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 1, 2},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "oldest containers should be removed when per pod container limit exceeded",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 1},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "running containers should not be removed",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_RUNNING),
			},
			remain:               []int{0, 1, 2},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "no containers should be removed when limits are not exceeded",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 1},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "max container count should apply per (UID, container) pair",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "baz", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "baz", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "baz", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 1, 3, 4, 6, 7},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "max limit should apply and try to keep from every pod",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar2", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar2", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo3", "bar3", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo3", "bar3", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo4", "bar4", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo4", "bar4", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 2, 4, 6, 8},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "oldest pods should be removed if limit exceeded",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo2", "bar2", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo3", "bar3", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo4", "bar4", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo5", "bar5", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo6", "bar6", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo7", "bar7", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 2, 4, 6, 8, 9},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "all non-running containers should be removed when evictTerminatingPods is set",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 2, 2, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo1", "bar1", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("running", "bar2", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo3", "bar3", 0, 0, runtimeapi.ContainerState_CONTAINER_RUNNING),
			},
			remain:               []int{4, 5},
			evictTerminatingPods: true,
			allSourcesReady:      true,
		},
		{
			description: "containers for deleted pods should be removed",
			containers: []containerTemplate{
				makeGCContainer("foo", "bar", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
				// 已删除的 Pod 仍受 MinAge 约束。
				makeGCContainer("deleted", "bar1", 2, time.Now().UnixNano(), runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("deleted", "bar1", 1, 1, runtimeapi.ContainerState_CONTAINER_EXITED),
				makeGCContainer("deleted", "bar1", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0, 1, 2},
			evictTerminatingPods: false,
			allSourcesReady:      true,
		},
		{
			description: "containers for deleted pods may not be removed if allSourcesReady is set false ",
			containers: []containerTemplate{
				makeGCContainer("deleted", "bar1", 0, 0, runtimeapi.ContainerState_CONTAINER_EXITED),
			},
			remain:               []int{0},
			evictTerminatingPods: true,
			allSourcesReady:      false,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			// 每个子测试前重置 Pod 状态，避免用例之间相互影响。
			podStateProvider.removed = make(map[types.UID]struct{})
			podStateProvider.terminated = make(map[types.UID]struct{})
			fakeContainers := makeFakeContainers(tCtx, m, test.containers)
			for _, s := range test.containers {
				if s.pod.Name == "deleted" {
					podStateProvider.removed[s.pod.UID] = struct{}{}
				}
				if s.pod.Name != "running" {
					podStateProvider.terminated[s.pod.UID] = struct{}{}
				}
			}
			fakeRuntime.SetFakeContainers(fakeContainers)

			// 未指定策略时使用默认 GC 策略。
			if test.policy == nil {
				test.policy = &defaultGCPolicy
			}
			err := m.containerGC.evictContainers(tCtx, *test.policy, test.allSourcesReady, test.evictTerminatingPods)
			assert.NoError(t, err)
			realRemain, err := fakeRuntime.ListContainers(tCtx, nil)
			assert.NoError(t, err)
			assert.Len(t, realRemain, len(test.remain))
			for _, remain := range test.remain {
				resp, err := fakeRuntime.ContainerStatus(tCtx, fakeContainers[remain].Id, false)
				assert.NoError(t, err)
				assert.Equal(t, &fakeContainers[remain].ContainerStatus, resp.Status)
			}
		})
	}
}

// TestPodLogDirectoryGC 验证 evictPodLogsDirectories 只删除没有对应 Pod 的日志目录。
// 注意：旧版容器符号链接可能很快被废弃，因此未纳入测试。
func TestPodLogDirectoryGC(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)
	fakeOS := m.osInterface.(*containertest.FakeOS)
	podStateProvider := m.containerGC.podStateProvider.(*fakePodStateProvider)

	// 没有对应 Pod 的 Pod 日志目录应当被删除。
	files := []string{"123", "456", "789", "012", "name_namespace_321", "name_namespace_654"}
	podLogsDirectory := "/var/log/pods"
	removed := []string{
		filepath.Join(podLogsDirectory, "789"),
		filepath.Join(podLogsDirectory, "012"),
		filepath.Join(podLogsDirectory, "name_namespace_654"),
	}
	// 标记部分目录对应的 Pod 已删除，它们应当被清理。
	podStateProvider.removed["012"] = struct{}{}
	podStateProvider.removed["789"] = struct{}{}
	podStateProvider.removed["654"] = struct{}{}

	// 模拟日志目录内容。
	fakeOS.ReadDirFn = func(string) ([]os.DirEntry, error) {
		var dirEntries []os.DirEntry
		for _, file := range files {
			mockDE := containertest.NewMockDirEntry(t)
			mockDE.EXPECT().Name().Return(file)
			dirEntries = append(dirEntries, mockDE)
		}
		return dirEntries, nil
	}

	// allSourcesReady == true 时，没有对应 Pod 的日志目录应当被删除。
	err = m.containerGC.evictPodLogsDirectories(tCtx, true)
	assert.NoError(t, err)
	assert.Equal(t, removed, fakeOS.Removes)

	// allSourcesReady == false 时，日志目录不应被删除。
	fakeOS.Removes = []string{}
	err = m.containerGC.evictPodLogsDirectories(tCtx, false)
	assert.NoError(t, err)
	assert.Empty(t, fakeOS.Removes)
}

// TestUnknownStateContainerGC 验证未知状态的容器会先被停止再被删除。
func TestUnknownStateContainerGC(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeRuntime, _, m, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	// podStateProvider := m.containerGC.podStateProvider.(*fakePodStateProvider)
	defaultGCPolicy := kubecontainer.GCPolicy{MinAge: time.Hour, MaxPerPodContainer: 0, MaxContainers: 0}

	fakeContainers := makeFakeContainers(tCtx, m, []containerTemplate{
		makeGCContainer("foo", "bar", 0, 0, runtimeapi.ContainerState_CONTAINER_UNKNOWN),
	})
	fakeRuntime.SetFakeContainers(fakeContainers)

	err = m.containerGC.evictContainers(tCtx, defaultGCPolicy, true, false)
	assert.NoError(t, err)

	// 未知状态的容器必须先停止再删除，避免残留进程。
	assert.Contains(t, fakeRuntime.GetCalls(), "StopContainer", "RemoveContainer",
		"container in unknown state should be stopped before being removed")

	remain, err := fakeRuntime.ListContainers(tCtx, nil)
	assert.NoError(t, err)
	assert.Empty(t, remain)
}
