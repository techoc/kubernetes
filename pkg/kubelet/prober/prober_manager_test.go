/*
Copyright 2015 The Kubernetes Authors.

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

package prober

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/probe"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// init 预留初始化钩子；当前测试无需额外初始化。
func init() {
}

// defaultProbe 是测试用探针模板：1 秒超时与周期，成功阈值 1，失败阈值 3。
var defaultProbe = &v1.Probe{
	ProbeHandler: v1.ProbeHandler{
		Exec: &v1.ExecAction{},
	},
	TimeoutSeconds:   1,
	PeriodSeconds:    1,
	SuccessThreshold: 1,
	FailureThreshold: 3,
}

// TestAddRemovePods 校验添加、删除 Pod 时探针 worker 的注册与注销。
func TestAddRemovePods(t *testing.T) {
	ktesting.Init(t).SyncTest("", testAddRemovePods)
}

// testAddRemovePods 是 TestAddRemovePods 的 synctest 主体，避免真实等待探针周期。
func testAddRemovePods(tCtx ktesting.TContext) {
	t := tCtx.TB()

	noProbePod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "no_probe_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "no_probe1",
			}, {
				Name: "no_probe2",
			}, {
				Name: "no_probe3",
			}},
		},
	}

	probePod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "probe_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "probe1",
			}, {
				Name:           "readiness",
				ReadinessProbe: defaultProbe,
			}, {
				Name: "probe2",
			}, {
				Name:          "liveness",
				LivenessProbe: defaultProbe,
			}, {
				Name: "probe3",
			}, {
				Name:         "startup",
				StartupProbe: defaultProbe,
			}},
		},
	}

	// 使用真实 manager 配 mock 探针，便于观察 worker 的生命周期。
	m := newTestManager()
	defer cleanup(t, m)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// 不含探针的 Pod 不应注册任何 worker，添加操作应是空操作。
	m.AddPod(tCtx, &noProbePod)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// 添加含探针的 Pod 应注册对应的 worker。
	m.AddPod(tCtx, &probePod)
	probePaths := []probeKey{
		{"probe_pod", "readiness", readiness},
		{"probe_pod", "liveness", liveness},
		{"probe_pod", "startup", startup},
	}
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// 删除未注册探针的 Pod 不应影响现有 worker。
	m.RemovePod(&noProbePod)
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// 删除含探针的 Pod 应注销对应 worker。
	m.RemovePod(&probePod)
	if err := waitForWorkerExit(t, m, probePaths); err != nil {
		t.Fatal(err)
	}
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	// 重复删除同一个 Pod 应是空操作。
	m.RemovePod(&probePod)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}
}

// TestAddPodContinuesAfterExistingWorker 回归验证：某个容器已有 worker 时，
// 再次 AddPod 仍会补齐其他缺失的 worker，而不是提前返回。
func TestAddPodContinuesAfterExistingWorker(t *testing.T) {
	ktesting.Init(t).SyncTest("", testAddPodContinuesAfterExistingWorker)
}

// testAddPodContinuesAfterExistingWorker 在 synctest 中复现 worker 缺失场景。
func testAddPodContinuesAfterExistingWorker(tCtx ktesting.TContext) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()
	ctx := tCtx.Context

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "test_pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:           "container_a",
					ReadinessProbe: defaultProbe,
				},
				{
					Name:           "container_b",
					ReadinessProbe: defaultProbe,
				},
			},
		},
	}

	m := newTestManager()
	defer cleanup(t, m)

	// 第一次 AddPod 为两个容器各注册一个 worker。
	m.AddPod(ctx, &pod)
	if err := expectProbes(m, []probeKey{
		{"test_pod", "container_a", readiness},
		{"test_pod", "container_b", readiness},
	}); err != nil {
		t.Fatalf("after first AddPod: %v", err)
	}

	// 模拟 container_b 的 worker 被移除，而 container_a 的 worker 仍在。
	m.workers[probeKey{"test_pod", "container_b", readiness}].stop()
	synctest.Wait()

	// 第二次 AddPod 应重新注册 container_b 缺失的 worker。
	// 旧实现遇到 container_a 已存在的 worker 就提前返回，
	// 导致 container_b 永远不会被重新注册。
	m.AddPod(ctx, &pod)

	if err := expectProbes(m, []probeKey{
		{"test_pod", "container_a", readiness},
		{"test_pod", "container_b", readiness},
	}); err != nil {
		t.Errorf("container_b worker was not re-registered after second AddPod: %v", err)
	}
}

// TestAddRemovePodsWithRestartableInitContainer 校验 restartable init 容器
// （sidecar）的探针注册与注销行为。
func TestAddRemovePodsWithRestartableInitContainer(t *testing.T) {
	tCtx := ktesting.Init(t)
	m := newTestManager()
	defer cleanup(t, m)
	if err := expectProbes(m, nil); err != nil {
		t.Error(err)
	}

	testCases := []struct {
		desc                        string
		probePaths                  []probeKey
		hasRestartableInitContainer bool
	}{
		{
			desc:                        "pod without sidecar",
			probePaths:                  nil,
			hasRestartableInitContainer: false,
		},
		{
			desc: "pod with sidecar",
			probePaths: []probeKey{
				{"restartable_init_container_pod", "restartable-init", liveness},
				{"restartable_init_container_pod", "restartable-init", readiness},
				{"restartable_init_container_pod", "restartable-init", startup},
			},
			hasRestartableInitContainer: true,
		},
	}

	containerRestartPolicy := func(hasRestartableInitContainer bool) *v1.ContainerRestartPolicy {
		if !hasRestartableInitContainer {
			return nil
		}
		restartPolicy := v1.ContainerRestartPolicyAlways
		return &restartPolicy
	}

	for _, tc := range testCases {
		tCtx.SyncTest(tc.desc, func(tCtx ktesting.TContext) {
			t := tCtx.TB()
			probePod := v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "restartable_init_container_pod",
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{{
						Name: "init",
					}, {
						Name:           "restartable-init",
						LivenessProbe:  defaultProbe,
						ReadinessProbe: defaultProbe,
						StartupProbe:   defaultProbe,
						RestartPolicy:  containerRestartPolicy(tc.hasRestartableInitContainer),
					}},
					Containers: []v1.Container{{
						Name: "main",
					}},
				},
			}

			// 添加含探针的 Pod 应注册对应的 worker。
			m.AddPod(tCtx, &probePod)
			if err := expectProbes(m, tc.probePaths); err != nil {
				t.Error(err)
			}

			// 删除含探针的 Pod 应注销对应 worker。
			m.RemovePod(&probePod)
			if err := waitForWorkerExit(t, m, tc.probePaths); err != nil {
				t.Fatal(err)
			}
			if err := expectProbes(m, nil); err != nil {
				t.Error(err)
			}

			// 重复删除同一个 Pod 应是空操作。
			m.RemovePod(&probePod)
			if err := expectProbes(m, nil); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestCleanupPods 校验 CleanupPods 只清理不在期望集合中的 Pod。
func TestCleanupPods(t *testing.T) {
	ktesting.Init(t).SyncTest("", testCleanupPods)
}

// testCleanupPods 是 TestCleanupPods 的 synctest 主体。
func testCleanupPods(tCtx ktesting.TContext) {
	t := tCtx.TB()
	m := newTestManager()
	defer cleanup(t, m)
	podToCleanup := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod_cleanup",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
			}, {
				Name:          "prober2",
				LivenessProbe: defaultProbe,
			}, {
				Name:         "prober3",
				StartupProbe: defaultProbe,
			}},
		},
	}
	podToKeep := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod_keep",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
			}, {
				Name:          "prober2",
				LivenessProbe: defaultProbe,
			}, {
				Name:         "prober3",
				StartupProbe: defaultProbe,
			}},
		},
	}
	m.AddPod(tCtx, &podToCleanup)
	m.AddPod(tCtx, &podToKeep)

	desiredPods := map[types.UID]sets.Empty{}
	desiredPods[podToKeep.UID] = sets.Empty{}
	m.CleanupPods(desiredPods)

	removedProbes := []probeKey{
		{"pod_cleanup", "prober1", readiness},
		{"pod_cleanup", "prober2", liveness},
		{"pod_cleanup", "prober3", startup},
	}
	expectedProbes := []probeKey{
		{"pod_keep", "prober1", readiness},
		{"pod_keep", "prober2", liveness},
		{"pod_keep", "prober3", startup},
	}
	if err := waitForWorkerExit(t, m, removedProbes); err != nil {
		t.Fatal(err)
	}
	if err := expectProbes(m, expectedProbes); err != nil {
		t.Error(err)
	}
}

// TestCleanupRepeated 对大量 Pod 反复执行清理，验证清理逻辑的幂等性。
func TestCleanupRepeated(t *testing.T) {
	ktesting.Init(t).SyncTest("", testCleanupRepeated)
}

// testCleanupRepeated 是 TestCleanupRepeated 的 synctest 主体。
func testCleanupRepeated(tCtx ktesting.TContext) {
	t := tCtx.TB()
	m := newTestManager()
	defer cleanup(t, m)
	podTemplate := v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:           "prober1",
				ReadinessProbe: defaultProbe,
				LivenessProbe:  defaultProbe,
				StartupProbe:   defaultProbe,
			}},
		},
	}

	const numTestPods = 100
	for i := 0; i < numTestPods; i++ {
		pod := podTemplate
		pod.UID = types.UID(strconv.Itoa(i))
		m.AddPod(tCtx, &pod)
	}

	for i := 0; i < 10; i++ {
		m.CleanupPods(map[types.UID]sets.Empty{})
	}
}

// TestUpdatePodStatus 校验 UpdatePodStatus 依据探针缓存结果回填容器 Ready 状态。
func TestUpdatePodStatus(t *testing.T) {
	ctx := ktesting.Init(t)
	logger := ctx.Logger()
	unprobed := v1.ContainerStatus{
		Name:        "unprobed_container",
		ContainerID: "test://unprobed_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedReady := v1.ContainerStatus{
		Name:        "probed_container_ready",
		ContainerID: "test://probed_container_ready_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedPending := v1.ContainerStatus{
		Name:        "probed_container_pending",
		ContainerID: "test://probed_container_pending_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	probedUnready := v1.ContainerStatus{
		Name:        "probed_container_unready",
		ContainerID: "test://probed_container_unready_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	notStartedNoReadiness := v1.ContainerStatus{
		Name:        "not_started_container_no_readiness",
		ContainerID: "test://not_started_container_no_readiness_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	startedNoReadiness := v1.ContainerStatus{
		Name:        "started_container_no_readiness",
		ContainerID: "test://started_container_no_readiness_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	terminated := v1.ContainerStatus{
		Name:        "terminated_container",
		ContainerID: "test://terminated_container_id",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{},
		},
	}
	podStatus := v1.PodStatus{
		Phase: v1.PodRunning,
		ContainerStatuses: []v1.ContainerStatus{
			unprobed, probedReady, probedPending, probedUnready, notStartedNoReadiness, startedNoReadiness, terminated,
		},
	}

	m := newTestManager()
	// 这里使用伪 worker，不注册清理，避免启动真实 goroutine。

	// 预置探针 worker 与缓存结果，用于驱动状态回填逻辑。
	m.workers = map[probeKey]*worker{
		{testPodUID, unprobed.Name, liveness}:             {},
		{testPodUID, probedReady.Name, readiness}:         {},
		{testPodUID, probedPending.Name, readiness}:       {},
		{testPodUID, probedUnready.Name, readiness}:       {},
		{testPodUID, notStartedNoReadiness.Name, startup}: {},
		{testPodUID, startedNoReadiness.Name, startup}:    {},
		{testPodUID, terminated.Name, readiness}:          {},
	}
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, probedReady.ContainerID), results.Success, &v1.Pod{})
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, probedUnready.ContainerID), results.Failure, &v1.Pod{})
	m.startupManager.Set(kubecontainer.ParseContainerID(logger, startedNoReadiness.ContainerID), results.Success, &v1.Pod{})
	m.readinessManager.Set(kubecontainer.ParseContainerID(logger, terminated.ContainerID), results.Success, &v1.Pod{})

	m.UpdatePodStatus(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: testPodUID,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: unprobed.Name},
				{Name: probedReady.Name},
				{Name: probedPending.Name},
				{Name: probedUnready.Name},
				{Name: notStartedNoReadiness.Name},
				{Name: startedNoReadiness.Name},
				{Name: terminated.Name},
			},
		},
	}, &podStatus)

	expectedReadiness := map[probeKey]bool{
		{testPodUID, unprobed.Name, readiness}:              true,
		{testPodUID, probedReady.Name, readiness}:           true,
		{testPodUID, probedPending.Name, readiness}:         false,
		{testPodUID, probedUnready.Name, readiness}:         false,
		{testPodUID, notStartedNoReadiness.Name, readiness}: false,
		{testPodUID, startedNoReadiness.Name, readiness}:    true,
		{testPodUID, terminated.Name, readiness}:            false,
	}
	for _, c := range podStatus.ContainerStatuses {
		expected, ok := expectedReadiness[probeKey{testPodUID, c.Name, readiness}]
		if !ok {
			t.Fatalf("Missing expectation for test case: %v", c.Name)
		}
		if expected != c.Ready {
			t.Errorf("Unexpected readiness for container %v: Expected %v but got %v",
				c.Name, expected, c.Ready)
		}
	}
}

// TestUpdatePodStatusWithInitContainers 校验 init 容器 Started/Ready 的回填，
// 并覆盖 restartable init 容器（sidecar）的差异。
func TestUpdatePodStatusWithInitContainers(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	notStarted := v1.ContainerStatus{
		Name:        "not_started_container",
		ContainerID: "test://not_started_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	started := v1.ContainerStatus{
		Name:        "started_container",
		ContainerID: "test://started_container_id",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{},
		},
	}
	terminated := v1.ContainerStatus{
		Name:        "terminated_container",
		ContainerID: "test://terminated_container_id",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{},
		},
	}

	m := newTestManager()
	// 这里使用伪 worker，不注册清理，避免启动真实 goroutine。

	// 预置探针 worker 与缓存结果，用于驱动状态回填逻辑。
	m.workers = map[probeKey]*worker{
		{testPodUID, notStarted.Name, startup}: {},
		{testPodUID, started.Name, startup}:    {},
	}
	m.startupManager.Set(kubecontainer.ParseContainerID(logger, started.ContainerID), results.Success, &v1.Pod{})

	testCases := []struct {
		desc                        string
		expectedStartup             map[probeKey]bool
		expectedReadiness           map[probeKey]bool
		hasRestartableInitContainer bool
	}{
		{
			desc: "init containers",
			expectedStartup: map[probeKey]bool{
				{testPodUID, notStarted.Name, startup}: false,
				{testPodUID, started.Name, startup}:    true,
				{testPodUID, terminated.Name, startup}: false,
			},
			expectedReadiness: map[probeKey]bool{
				{testPodUID, notStarted.Name, readiness}: false,
				{testPodUID, started.Name, readiness}:    false,
				{testPodUID, terminated.Name, readiness}: true,
			},
			hasRestartableInitContainer: false,
		},
		{
			desc: "init container with Always restartPolicy",
			expectedStartup: map[probeKey]bool{
				{testPodUID, notStarted.Name, startup}: false,
				{testPodUID, started.Name, startup}:    true,
				{testPodUID, terminated.Name, startup}: false,
			},
			expectedReadiness: map[probeKey]bool{
				{testPodUID, notStarted.Name, readiness}: false,
				{testPodUID, started.Name, readiness}:    true,
				{testPodUID, terminated.Name, readiness}: false,
			},
			hasRestartableInitContainer: true,
		},
	}

	containerRestartPolicy := func(enableSidecarContainers bool) *v1.ContainerRestartPolicy {
		if !enableSidecarContainers {
			return nil
		}
		restartPolicy := v1.ContainerRestartPolicyAlways
		return &restartPolicy
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			podStatus := v1.PodStatus{
				Phase: v1.PodRunning,
				InitContainerStatuses: []v1.ContainerStatus{
					notStarted, started, terminated,
				},
			}

			m.UpdatePodStatus(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: testPodUID,
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name:          notStarted.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
						{
							Name:          started.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
						{
							Name:          terminated.Name,
							RestartPolicy: containerRestartPolicy(tc.hasRestartableInitContainer),
						},
					},
				},
			}, &podStatus)

			for _, c := range podStatus.InitContainerStatuses {
				{
					expected, ok := tc.expectedStartup[probeKey{testPodUID, c.Name, startup}]
					if !ok {
						t.Fatalf("Missing expectation for test case: %v", c.Name)
					}
					if expected != *c.Started {
						t.Errorf("Unexpected startup for container %v: Expected %v but got %v",
							c.Name, expected, *c.Started)
					}
				}
				{
					expected, ok := tc.expectedReadiness[probeKey{testPodUID, c.Name, readiness}]
					if !ok {
						t.Fatalf("Missing expectation for test case: %v", c.Name)
					}
					if expected != c.Ready {
						t.Errorf("Unexpected readiness for container %v: Expected %v but got %v",
							c.Name, expected, c.Ready)
					}
				}
			}
		})
	}
}

// extractedReadinessHandling 从就绪结果通道取一条更新并写入状态管理器，
// 对应 kubelet.syncLoopIteration 中的就绪结果处理片段。
func (m *manager) extractedReadinessHandling(logger klog.Logger) {
	update := <-m.readinessManager.Updates()
	// 这段逻辑摘自 kubelet.syncLoopIteration()，单独抽出来便于测试。
	ready := update.Result == results.Success
	m.statusManager.SetContainerReadiness(logger, update.PodUID, update.ContainerID, ready)
}

// TestUpdateReadiness 校验就绪探针的成功/失败结果能反映到 Pod 状态。
func TestUpdateReadiness(t *testing.T) {
	ktesting.Init(t).SyncTest("", testUpdateReadiness)
}

// testUpdateReadiness 是 TestUpdateReadiness 的 synctest 主体。
func testUpdateReadiness(tCtx ktesting.TContext) {
	t := tCtx.TB()
	logger := tCtx.Logger()
	testPod := getTestPod()
	setTestProbe(testPod, readiness, v1.Probe{})
	m := newTestManager()
	defer cleanup(t, m)

	// 启动就绪结果同步循环，并在测试结束时确保 goroutine 退出。
	stopCh := make(chan struct{})
	go wait.Until(func() { m.extractedReadinessHandling(logger) }, 0, stopCh)
	defer func() {
		close(stopCh)
		// 发送一条更新，让 extractedReadinessHandling() 从阻塞中退出。
		m.readinessManager.Set(kubecontainer.ContainerID{}, results.Success, &v1.Pod{})
	}()

	exec := syncExecProber{}
	exec.set(probe.Success, nil)
	m.prober.exec = &exec

	m.statusManager.SetPodStatus(logger, testPod, getTestRunningStatus())

	m.AddPod(tCtx, testPod)
	probePaths := []probeKey{{testPodUID, testContainerName, readiness}}
	if err := expectProbes(m, probePaths); err != nil {
		t.Error(err)
	}

	// 等待就绪状态变为 true。
	if err := waitForReadyStatus(t, m, true); err != nil {
		t.Error(err)
	}

	// 让探针返回失败。
	exec.set(probe.Failure, nil)

	// 等待就绪状态变为 false。
	if err := waitForReadyStatus(t, m, false); err != nil {
		t.Error(err)
	}
}

// expectProbes 比较 manager 中注册的 worker 与期望集合，返回缺失与多余的 worker。
func expectProbes(m *manager, expectedProbes []probeKey) error {
	// 持读锁遍历 worker 表，避免与 AddPod/RemovePod 并发修改冲突。
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()

	var unexpected []probeKey
	missing := make([]probeKey, len(expectedProbes))
	copy(missing, expectedProbes)

outer:
	for probePath := range m.workers {
		for i, expectedPath := range missing {
			if probePath == expectedPath {
				missing = append(missing[:i], missing[i+1:]...)
				continue outer
			}
		}
		unexpected = append(unexpected, probePath)
	}

	if len(missing) == 0 && len(unexpected) == 0 {
		return nil // 集合完全一致。
	}

	return fmt.Errorf("Unexpected probes: %v; Missing probes: %v;", unexpected, missing)
}

const interval = 1 * time.Second

// 等待给定 worker 退出并完成清理。
func waitForWorkerExit(t ktesting.TB, m *manager, workerPaths []probeKey) error {
	for _, w := range workerPaths {
		condition := func() (bool, error) {
			_, exists := m.getWorker(w.podUID, w.containerName, w.probeType)
			return !exists, nil
		}
		if exited, _ := condition(); exited {
			continue // 已经退出，无需轮询。
		}
		t.Logf("Polling %v", w)
		if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
			return err
		}
	}

	return nil
}

// waitForReadyStatus 轮询等待 Pod 的就绪状态达到期望值。
func waitForReadyStatus(t ktesting.TB, m *manager, ready bool) error {
	condition := func() (bool, error) {
		status, ok := m.statusManager.GetPodStatus(testPodUID)
		if !ok {
			return false, fmt.Errorf("status not found: %q", testPodUID)
		}
		if len(status.ContainerStatuses) != 1 {
			return false, fmt.Errorf("expected single container, found %d", len(status.ContainerStatuses))
		}
		if status.ContainerStatuses[0].ContainerID != testContainerID.String() {
			return false, fmt.Errorf("expected container %q, found %q",
				testContainerID, status.ContainerStatuses[0].ContainerID)
		}
		return status.ContainerStatuses[0].Ready == ready, nil
	}
	t.Logf("Polling for ready state %v", ready)
	if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
		return err
	}

	return nil
}

// 清理运行中的探针，避免 goroutine 泄漏。
func cleanup(t ktesting.TB, m *manager) {
	// 清理所有 Pod 后轮询等待 worker goroutine 退出，防止测试间泄漏。
	m.CleanupPods(nil)

	condition := func() (bool, error) {
		workerCount := m.workerCount()
		if workerCount > 0 {
			t.Logf("Waiting for %d workers to exit...", workerCount)
		}
		return workerCount == 0, nil
	}
	if exited, _ := condition(); exited {
		return // 已经退出，无需轮询。
	}
	if err := wait.Poll(interval, wait.ForeverTestTimeout, condition); err != nil {
		t.Fatalf("Error during cleanup: %v", err)
	}
}

// ctxAwareRunner 实现 kubecontainer.CommandRunner：
// 探针上下文取消后立即失败，行为与真实 CRI 运行时客户端一致。
type ctxAwareRunner struct{}

// RunInContainer 在上下文已取消时立即返回错误，模拟 CRI ExecSync 的行为。
func (r ctxAwareRunner) RunInContainer(ctx context.Context, id kubecontainer.ContainerID, cmd []string, timeout time.Duration) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []byte("ok"), nil
}

// TestProbeWorkersSurviveSyncContextCancellation 验证 AddPod 传入的上下文被取消后，
// 探针 worker 仍会继续探测。Pod worker 在 Pod 开始终止时会取消同步上下文，
// 但就绪探针必须持续执行到终止结束，这样失败的探针才能把 Pod 标记为 NotReady。
// 这是 https://github.com/kubernetes/kubernetes/issues/140881 的回归测试。
func TestProbeWorkersSurviveSyncContextCancellation(t *testing.T) {
	ktesting.Init(t).SyncTest("", testProbeWorkersSurviveSyncContextCancellation)
}

// testProbeWorkersSurviveSyncContextCancellation 在 synctest 中验证取消同步上下文后
// worker 仍能写入探测结果。
func testProbeWorkersSurviveSyncContextCancellation(tCtx ktesting.TContext) {
	t := tCtx.TB()
	logger := tCtx.Logger()
	testPod := getTestPod()
	setTestProbe(testPod, readiness, v1.Probe{})
	m := newTestManager()
	defer cleanup(t, m)

	// 使用真实的 exec 探针与支持上下文取消的 runner，
	// 模拟 CRI ExecSync 在上下文取消后立即失败的行为。
	m.prober = newProber(ctxAwareRunner{}, &record.FakeRecorder{})

	m.statusManager.SetPodStatus(logger, testPod, getTestRunningStatus())

	ctx, cancel := context.WithCancel(tCtx)
	m.AddPod(ctx, testPod)
	// 模拟 Pod worker 在 Pod 开始终止时取消同步上下文。
	cancel()

	// 新容器的 worker 会先向结果缓存写入 Failure，随后必须用真实探测结果写入 Success。
	for {
		select {
		case update := <-m.readinessManager.Updates():
			if update.Result == results.Success {
				return
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Fatal("Probe worker recorded no probe result after the sync context was canceled; probes are likely being canceled by pod termination")
		}
	}
}
