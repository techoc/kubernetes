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

// worker_test.go 覆盖探测 worker 的状态机：初始延迟、成功/失败阈值、hold 语义、重启规则与清理逻辑。
package prober

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/status"
	statustest "k8s.io/kubernetes/pkg/kubelet/status/testing"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/probe"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// init 是测试包的初始化钩子；当前没有额外初始化逻辑。
func init() {
}

// newTestWorkerWithRestartableInitContainer 创建一个以重启式 init 容器（sidecar）为探测对象的测试 worker。
func newTestWorkerWithRestartableInitContainer(m *manager, probeType probeType) *worker {
	pod := getTestPod()

	// 把首个容器改为 restartPolicy=Always 的 init 容器。
	initContainer := pod.Spec.Containers[0]
	initContainer.Name = testContainerName
	restartPolicy := v1.ContainerRestartPolicyAlways
	initContainer.RestartPolicy = &restartPolicy

	// 该容器移入 InitContainers，同时保留一个普通容器以模拟 sidecar 场景。
	pod.Spec.InitContainers = []v1.Container{initContainer}
	pod.Spec.Containers = []v1.Container{{
		Name: "main-container",
	}}

	return newWorker(m, probeType, pod, initContainer)
}

// TestDoProbe 验证三种探针在各种 Pod/容器状态下的 doProbe 返回值与结果写入行为。
func TestDoProbe(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()

	for _, probeType := range [...]probeType{liveness, readiness, startup} {
		// 构造运行中、等待中、已终止、容器名不匹配、Pod 失败等状态。
		runningStatus := getTestRunningStatusWithStarted(probeType != startup)
		pendingStatus := getTestRunningStatusWithStarted(probeType != startup)
		pendingStatus.ContainerStatuses[0].State.Running = nil
		terminatedStatus := getTestRunningStatusWithStarted(probeType != startup)
		terminatedStatus.ContainerStatuses[0].State.Running = nil
		terminatedStatus.ContainerStatuses[0].State.Terminated = &v1.ContainerStateTerminated{
			StartedAt: metav1.Now(),
		}
		otherStatus := getTestRunningStatusWithStarted(probeType != startup)
		otherStatus.ContainerStatuses[0].Name = "otherContainer"
		failedStatus := getTestRunningStatusWithStarted(probeType != startup)
		failedStatus.Phase = v1.PodFailed

		tests := []struct {
			probe                v1.Probe
			podStatus            *v1.PodStatus
			expectContinue       map[string]bool
			expectSet            bool
			expectedResult       results.Result
			setDeletionTimestamp bool
		}{
			{ // No status.
				expectContinue: map[string]bool{
					liveness.String():  true,
					readiness.String(): true,
					startup.String():   true,
				},
			},
			{ // Pod failed
				podStatus: &failedStatus,
			},
			{ // Pod deletion
				podStatus:            &runningStatus,
				setDeletionTimestamp: true,
				expectSet:            true,
				expectContinue: map[string]bool{
					readiness.String(): true,
				},
				expectedResult: results.Success,
			},
			{ // No container status
				podStatus: &otherStatus,
				expectContinue: map[string]bool{
					liveness.String():  true,
					readiness.String(): true,
					startup.String():   true,
				},
			},
			{ // Container waiting
				podStatus: &pendingStatus,
				expectContinue: map[string]bool{
					liveness.String():  true,
					readiness.String(): true,
					startup.String():   true,
				},
				expectSet:      true,
				expectedResult: results.Failure,
			},
			{ // Container terminated
				podStatus:      &terminatedStatus,
				expectSet:      true,
				expectedResult: results.Failure,
			},
			{ // Probe successful.
				podStatus: &runningStatus,
				expectContinue: map[string]bool{
					liveness.String():  true,
					readiness.String(): true,
					startup.String():   true,
				},
				expectSet:      true,
				expectedResult: results.Success,
			},
			{ // Initial delay passed
				podStatus: &runningStatus,
				probe: v1.Probe{
					InitialDelaySeconds: -100,
				},
				expectContinue: map[string]bool{
					liveness.String():  true,
					readiness.String(): true,
					startup.String():   true,
				},
				expectSet:      true,
				expectedResult: results.Success,
			},
		}

		for i, test := range tests {
			w := newTestWorker(m, probeType, test.probe)
			if test.podStatus != nil {
				m.statusManager.SetPodStatus(logger, w.pod, *test.podStatus)
			}
			if test.setDeletionTimestamp {
				now := metav1.Now()
				w.pod.ObjectMeta.DeletionTimestamp = &now
			}
			if c := w.doProbe(ctx); c != test.expectContinue[probeType.String()] {
				t.Errorf("[%s-%d] Expected continue to be %v but got %v", probeType, i, test.expectContinue[probeType.String()], c)
			}
			result, ok := resultsManager(m, probeType).Get(testContainerID)
			if ok != test.expectSet {
				t.Errorf("[%s-%d] Expected to have result: %v but got %v", probeType, i, test.expectSet, ok)
			}
			if result != test.expectedResult {
				t.Errorf("[%s-%d] Expected result: %v but got %v", probeType, i, test.expectedResult, result)
			}

			m.statusManager = status.NewManager(&fake.Clientset{}, kubepod.NewBasicPodManager(), &statustest.FakePodDeletionSafetyProvider{}, kubeletutil.NewPodStartupLatencyTracker())
			resultsManager(m, probeType).Remove(testContainerID)
		}
	}
}

// TestDoProbeWithContainerRestartRules 验证容器级 restartPolicy 与 restartPolicyRules 对探测是否继续的影响。
func TestDoProbeWithContainerRestartRules(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerRestartRules: true,
	})
	TestDoProbe(t)

	var (
		restartPolicyAlways    = v1.ContainerRestartPolicyAlways
		restartPolicyNever     = v1.ContainerRestartPolicyNever
		restartPolicyOnFailure = v1.ContainerRestartPolicyOnFailure
	)

	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	for _, probeType := range [...]probeType{liveness, readiness, startup} {
		testcases := []struct {
			name           string
			container      v1.Container
			podStatus      v1.PodStatus
			expectContinue bool
		}{
			{
				name: "container failed with container restartPolicy=OnFailure",
				container: v1.Container{
					RestartPolicy: &restartPolicyOnFailure,
				},
				podStatus:      getTestRunningStatusWithFailedContainer(),
				expectContinue: true,
			},
			{
				name: "container succeeded with containerRestartPolicy=OnFailure",
				container: v1.Container{
					RestartPolicy: &restartPolicyOnFailure,
				},
				podStatus:      getTestRunningStatusWithSucceededContainer(),
				expectContinue: false,
			},
			{
				name: "container failed with containerRestartPolicy=Always",
				container: v1.Container{
					RestartPolicy: &restartPolicyAlways,
				},
				podStatus:      getTestRunningStatusWithFailedContainer(),
				expectContinue: true,
			},
			{
				name: "container succeeded with containerRestartPolicy=Always",
				container: v1.Container{
					RestartPolicy: &restartPolicyAlways,
				},
				podStatus:      getTestRunningStatusWithSucceededContainer(),
				expectContinue: true,
			},
			{
				name: "container failed with containerRestartPolicy=Never",
				container: v1.Container{
					RestartPolicy: &restartPolicyNever,
				},
				podStatus:      getTestRunningStatusWithFailedContainer(),
				expectContinue: false,
			},
			{
				name: "container succeeded with containerRestartPolicy=Never",
				container: v1.Container{
					RestartPolicy: &restartPolicyNever,
				},
				podStatus:      getTestRunningStatusWithSucceededContainer(),
				expectContinue: false,
			},
			{
				name: "container terminated with matching restartPolicyRules",
				container: v1.Container{
					RestartPolicy: &restartPolicyNever,
					RestartPolicyRules: []v1.ContainerRestartRule{{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{1},
						},
					}},
				},
				podStatus:      getTestRunningStatusWithFailedContainer(),
				expectContinue: true,
			},
			{
				name: "container terminated with non-matching restartPolicyRules",
				container: v1.Container{
					RestartPolicy: &restartPolicyNever,
					RestartPolicyRules: []v1.ContainerRestartRule{{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{99},
						},
					}},
				},
				podStatus:      getTestRunningStatusWithFailedContainer(),
				expectContinue: false,
			},
		}

		for _, tc := range testcases {
			pod := getTestPod()
			setTestProbe(pod, probeType, v1.Probe{})
			c := pod.Spec.Containers[0]
			c.RestartPolicy = tc.container.RestartPolicy
			c.RestartPolicyRules = tc.container.RestartPolicyRules
			pod.Spec.Containers[0] = c
			w := newWorker(m, probeType, pod, pod.Spec.Containers[0])

			m.statusManager.SetPodStatus(logger, w.pod, tc.podStatus)

			if c := w.doProbe(ctx); c != tc.expectContinue {
				t.Errorf("[%s-%s] Expected continue to be %v but got %v", probeType, tc.name, tc.expectContinue, c)
			}
		}
	}
}

// TestDoProbeWithContainerRestartAllContainers 验证 RestartAllContainers 规则下探测的继续/停止行为。
func TestDoProbeWithContainerRestartAllContainers(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerRestartRules:                true,
		features.NodeDeclaredFeatures:                 true,
		features.RestartAllContainersOnContainerExits: true,
	})
	TestDoProbe(t)
	TestDoProbeWithContainerRestartRules(t)

	var (
		restartPolicyNever = v1.ContainerRestartPolicyNever
	)

	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	for _, probeType := range [...]probeType{liveness, readiness, startup} {
		testcases := []struct {
			name           string
			pod            func() v1.Pod
			podStatus      func() v1.PodStatus
			expectContinue bool
		}{
			{
				name: "container terminated with matching restartAllContainers",
				pod: func() v1.Pod {
					pod := getTestPod()
					setTestProbe(pod, probeType, v1.Probe{})
					c := pod.Spec.Containers[0]
					c.RestartPolicy = &restartPolicyNever
					c.RestartPolicyRules = []v1.ContainerRestartRule{{
						Action: v1.ContainerRestartRuleActionRestartAllContainers,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{1},
						},
					}}
					pod.Spec.Containers[0] = c
					return *pod
				},
				podStatus:      getTestRunningStatusWithFailedContainer,
				expectContinue: true,
			},
			{
				name: "container terminated by restartAllContainers",
				pod: func() v1.Pod {
					pod := getTestPod()
					setTestProbe(pod, probeType, v1.Probe{})
					triggerContainer := v1.Container{
						Name:          "trigger",
						RestartPolicy: &restartPolicyNever,
						RestartPolicyRules: []v1.ContainerRestartRule{{
							Action: v1.ContainerRestartRuleActionRestartAllContainers,
							ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
								Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
								Values:   []int32{1},
							},
						}},
					}
					pod.Spec.Containers = append(pod.Spec.Containers, triggerContainer)
					return *pod
				},
				podStatus: func() v1.PodStatus {
					status := getTestRunningStatusWithFailedContainer()
					// 触发容器已终止，用于模拟 restartAllContainers 的触发条件。
					status.ContainerStatuses = append(status.ContainerStatuses, v1.ContainerStatus{
						Name: "trigger",
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{
								ExitCode: 1,
							},
						},
					})
					return status
				},
				expectContinue: true,
			},
			{
				name: "container cleaned up by restartAllContainers",
				pod: func() v1.Pod {
					pod := getTestPod()
					setTestProbe(pod, probeType, v1.Probe{})
					triggerContainer := v1.Container{
						Name:          "trigger",
						RestartPolicy: &restartPolicyNever,
						RestartPolicyRules: []v1.ContainerRestartRule{{
							Action: v1.ContainerRestartRuleActionRestartAllContainers,
							ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
								Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
								Values:   []int32{1},
							},
						}},
					}
					pod.Spec.Containers = append(pod.Spec.Containers, triggerContainer)
					return *pod
				},
				// 清理后容器处于 Waiting、Pod 处于 Pending，探测应继续等待。
				podStatus:      getTestPendingStatus,
				expectContinue: true,
			},
		}

		for _, tc := range testcases {
			pod := tc.pod()
			podStatus := tc.podStatus()
			w := newWorker(m, probeType, &pod, pod.Spec.Containers[0])

			m.statusManager.SetPodStatus(logger, w.pod, podStatus)

			if c := w.doProbe(ctx); c != tc.expectContinue {
				t.Errorf("[%s: %s] Expected continue to be %v but got %v", probeType, tc.name, tc.expectContinue, c)
			}
		}
	}
}

// TestInitialDelay 验证初始延迟期间各探针的默认结果与延迟结束后的正常探测。
func TestInitialDelay(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()

	for _, probeType := range [...]probeType{liveness, readiness, startup} {
		w := newTestWorker(m, probeType, v1.Probe{
			InitialDelaySeconds: 10,
		})
		m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatusWithStarted(probeType != startup))

		expectContinue(t, w, w.doProbe(ctx), "during initial delay")
		// 初始延迟内的默认结果因探针而异：liveness=Success，readiness=Failure，startup=Unknown。
		switch probeType {
		case liveness:
			expectResult(t, w, results.Success, "during initial delay")
		case readiness:
			expectResult(t, w, results.Failure, "during initial delay")
		case startup:
			expectResult(t, w, results.Unknown, "during initial delay")
		}

		// 把启动时间回拨 100 秒，模拟初始延迟已过。
		laterStatus := getTestRunningStatusWithStarted(probeType != startup)
		laterStatus.ContainerStatuses[0].State.Running.StartedAt.Time =
			time.Now().Add(-100 * time.Second)
		m.statusManager.SetPodStatus(logger, w.pod, laterStatus)

		// 第二次探测应真正执行并成功。
		expectContinue(t, w, w.doProbe(ctx), "after initial delay")
		expectResult(t, w, results.Success, "after initial delay")
	}
}

// TestFailureThreshold 验证连续失败达到 failureThreshold 后结果才转为 Failure。
func TestFailureThreshold(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	w := newTestWorker(m, readiness, v1.Probe{SuccessThreshold: 1, FailureThreshold: 3})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatus())

	for i := 0; i < 2; i++ {
		// 先让探测成功，确保每轮从 Success 状态开始。
		m.prober.exec = fakeExecProber{probe.Success, nil}

		for j := 0; j < 3; j++ {
			msg := fmt.Sprintf("%d success (%d)", j+1, i)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Success, msg)
		}

		// 让探测开始失败。
		m.prober.exec = fakeExecProber{probe.Failure, nil}

		// 未达到阈值前结果仍保持 Success。
		for j := 0; j < 2; j++ {
			msg := fmt.Sprintf("%d failing (%d)", j+1, i)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Success, msg)
		}

		// 达到阈值后结果变为 Failure。
		for j := 0; j < 3; j++ {
			msg := fmt.Sprintf("%d failure (%d)", j+3, i)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Failure, msg)
		}
	}
}

// TestSuccessThreshold 验证连续成功达到 successThreshold 后结果才转为 Success。
func TestSuccessThreshold(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	w := newTestWorker(m, readiness, v1.Probe{SuccessThreshold: 3, FailureThreshold: 1})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatus())

	// 从 Failure 状态开始，确保成功计数从零累积。
	w.resultsManager.Set(testContainerID, results.Failure, &v1.Pod{})

	for i := 0; i < 2; i++ {
		// 默认探针失败，结果保持 Failure。
		for j := 0; j < 2; j++ {
			msg := fmt.Sprintf("%d success (%d)", j+1, i)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Failure, msg)
		}

		// 连续成功达到阈值后结果转为 Success。
		for j := 0; j < 3; j++ {
			msg := fmt.Sprintf("%d success (%d)", j+3, i)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Success, msg)
		}

		// 单次失败应立即把结果改回 Failure。
		m.prober.exec = fakeExecProber{probe.Failure, nil}
		msg := fmt.Sprintf("1 failure (%d)", i)
		expectContinue(t, w, w.doProbe(ctx), msg)
		expectResult(t, w, results.Failure, msg)

		// 恢复成功探测，进入下一轮。
		m.prober.exec = fakeExecProber{probe.Success, nil}
	}
}

// TestStartupProbeSuccessThreshold 验证 startup 探针达到成功阈值后进入 hold 状态且不再执行。
func TestStartupProbeSuccessThreshold(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	successThreshold := 1
	failureThreshold := 3
	w := newTestWorker(m, startup, v1.Probe{SuccessThreshold: int32(successThreshold), FailureThreshold: int32(failureThreshold)})
	m.statusManager.SetPodStatus(logger, w.pod, getTestNotRunningStatus())
	m.prober.exec = fakeExecProber{probe.Success, nil}

	for i := 0; i < successThreshold+1; i++ {
		if i < successThreshold {
			// 未达到成功阈值前不 hold，探测持续执行。
			if w.onHold {
				t.Errorf("Prober should not be on hold")
			}
			msg := fmt.Sprintf("%d success", i+1)
			expectContinue(t, w, w.doProbe(ctx), msg)
			expectResult(t, w, results.Success, msg)
		} else {
			// 达到成功阈值后进入 hold，不再执行探测。
			if !w.onHold {
				t.Errorf("Prober should be on hold because successThreshold is exceeded")
			}
			// 达到或超过成功阈值时 resultRun 必须归零。
			if w.resultRun != 0 {
				t.Errorf("Prober resultRun should be 0, but %d", w.resultRun)
			}
		}
	}
}

// TestStartupProbeFailureThreshold 验证 startup 探针在达到失败阈值前结果保持 Unknown，达到后转为 Failure 并 hold。
func TestStartupProbeFailureThreshold(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	successThreshold := 1
	failureThreshold := 3
	w := newTestWorker(m, startup, v1.Probe{SuccessThreshold: int32(successThreshold), FailureThreshold: int32(failureThreshold)})
	m.statusManager.SetPodStatus(logger, w.pod, getTestNotRunningStatus())
	m.prober.exec = fakeExecProber{probe.Failure, nil}

	for i := 0; i < failureThreshold+1; i++ {
		if i < failureThreshold {
			// 未达到失败阈值前不 hold，探测持续执行。
			if w.onHold {
				t.Errorf("Prober should not be on hold")
			}
			msg := fmt.Sprintf("%d failure", i+1)
			expectContinue(t, w, w.doProbe(ctx), msg)
			switch i {
			case 0, 1:
				// 此时结果仍为 Unknown：只有达到失败阈值才会写入 Failure。
				expectResult(t, w, results.Unknown, msg)
				// resultRun 随每次失败递增，直到达到阈值。
				if w.resultRun != i+1 {
					t.Errorf("Prober resultRun should be %d, but %d", i+1, w.resultRun)
				}
			case 2:
				// 达到失败阈值后结果写入 Failure，随后 resultRun 归零。
				expectResult(t, w, results.Failure, msg)
				// 达到失败阈值时必须重置 resultRun。
				if w.resultRun != 0 {
					t.Errorf("Prober resultRun should be 0, but %d", w.resultRun)
				}
			}
		} else {
			// 达到失败阈值后进入 hold，不再执行探测。
			if !w.onHold {
				t.Errorf("Prober should be on hold because failureThreshold is exceeded")
			}
			// 超过失败阈值后 resultRun 保持为 0。
			if w.resultRun != 0 {
				t.Errorf("Prober resultRun should be 0, but %d", w.resultRun)
			}
		}
	}
}

// TestCleanUp 验证 worker 停止后其探针结果与 worker 条目都会被清理。
func TestCleanUp(t *testing.T) {
	ktesting.Init(t).SyncTest("", testCleanUp)
}

// testCleanUp 是 TestCleanUp 的主体，逐个探针类型验证停止与清理。
func testCleanUp(tCtx ktesting.TContext) {
	t := tCtx.TB()
	m := newTestManager()

	for _, probeType := range [...]probeType{liveness, readiness, startup} {
		key := probeKey{testPodUID, testContainerName, probeType}
		w := newTestWorker(m, probeType, v1.Probe{})
		m.statusManager.SetPodStatus(tCtx.Logger(), w.pod, getTestRunningStatusWithStarted(probeType != startup))
		go w.run(tCtx)
		m.workers[key] = w

		// 等待 worker 完成首次探测，确保其已进入运行状态。
		condition := func() (bool, error) {
			ready, _ := resultsManager(m, probeType).Get(testContainerID)
			return ready == results.Success, nil
		}
		if ready, _ := condition(); !ready {
			if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, condition); err != nil {
				t.Fatalf("[%s] Error waiting for worker ready: %v", probeType, err)
			}
		}

		for i := 0; i < 10; i++ {
			w.stop() // 重复调用 stop 必须幂等。
		}
		if err := waitForWorkerExit(t, m, []probeKey{key}); err != nil {
			t.Fatalf("[%s] error waiting for worker exit: %v", probeType, err)
		}

		if _, ok := resultsManager(m, probeType).Get(testContainerID); ok {
			t.Errorf("[%s] Expected result to be cleared.", probeType)
		}
		if _, ok := m.workers[key]; ok {
			t.Errorf("[%s] Expected worker to be cleared.", probeType)
		}
	}
}

// expectResult 断言 worker 当前记录的探针结果等于期望值。
func expectResult(t *testing.T, w *worker, expectedResult results.Result, msg string) {
	result, ok := resultsManager(w.probeManager, w.probeType).Get(w.containerID)
	if !ok {
		t.Errorf("[%s - %s] Expected result to be set, but was not set", w.probeType, msg)
	} else if result != expectedResult {
		t.Errorf("[%s - %s] Expected result to be %v, but was %v",
			w.probeType, msg, expectedResult, result)
	}
}

// expectContinue 断言 doProbe 返回 true（worker 应继续运行）。
func expectContinue(t *testing.T, w *worker, c bool, msg string) {
	if !c {
		t.Errorf("[%s - %s] Expected to continue, but did not", w.probeType, msg)
	}
}

// resultsManager 返回指定探针类型对应的结果管理器。
func resultsManager(m *manager, probeType probeType) results.Manager {
	switch probeType {
	case readiness:
		return m.readinessManager
	case liveness:
		return m.livenessManager
	case startup:
		return m.startupManager
	}
	panic(fmt.Errorf("Unhandled case: %v", probeType))
}

// TestOnHoldOnLivenessOrStartupCheckFailure 验证 liveness/startup 失败后 worker 进入 hold，且容器 ID 变化可解除 hold。
func TestOnHoldOnLivenessOrStartupCheckFailure(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()

	for _, probeType := range [...]probeType{liveness, startup} {
		w := newTestWorker(m, probeType, v1.Probe{SuccessThreshold: 1, FailureThreshold: 1})
		status := getTestRunningStatusWithStarted(probeType != startup)
		m.statusManager.SetPodStatus(logger, w.pod, status)

		// 首次探测失败，worker 应进入 hold。
		m.prober.exec = fakeExecProber{probe.Failure, nil}
		msg := "first probe"
		expectContinue(t, w, w.doProbe(ctx), msg)
		expectResult(t, w, results.Failure, msg)
		if !w.onHold {
			t.Errorf("Prober should be on hold due to %s check failure", probeType)
		}
		// 即使探测恢复成功，hold 期间也不会执行探测，结果保持 Failure。
		m.prober.exec = fakeExecProber{probe.Success, nil}
		msg = "while on hold"
		expectContinue(t, w, w.doProbe(ctx), msg)
		expectResult(t, w, results.Failure, msg)
		if !w.onHold {
			t.Errorf("Prober should be on hold due to %s check failure", probeType)
		}

		// 容器 ID 变化表示容器已重建，hold 被解除，下一次探测会成功。
		status.ContainerStatuses[0].ContainerID = "test://newCont_ID"
		m.statusManager.SetPodStatus(logger, w.pod, status)
		msg = "hold lifted"
		expectContinue(t, w, w.doProbe(ctx), msg)
		expectResult(t, w, results.Success, msg)
		if probeType == liveness && w.onHold {
			t.Errorf("Prober should not be on hold anymore")
		} else if probeType == startup && !w.onHold {
			t.Errorf("Prober should be on hold due to %s check success", probeType)
		}
	}
}

// TestResultRunOnLivenessCheckFailure 验证 liveness 失败达到阈值后 resultRun 归零，使重建后的容器重新获得完整失败次数。
func TestResultRunOnLivenessCheckFailure(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	w := newTestWorker(m, liveness, v1.Probe{SuccessThreshold: 1, FailureThreshold: 3})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatus())

	m.prober.exec = fakeExecProber{probe.Success, nil}
	msg := "initial probe success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
	if w.resultRun != 1 {
		t.Errorf("Prober resultRun should be 1")
	}

	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "probe failure, result success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
	if w.resultRun != 1 {
		t.Errorf("Prober resultRun should be 1")
	}

	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "2nd probe failure, result success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
	if w.resultRun != 2 {
		t.Errorf("Prober resultRun should be 2")
	}

	// 超过失败阈值后 resultRun 归零，保证重建后的 Pod 仍有完整的失败重试次数。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "3rd probe failure, result failure"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Failure, msg)
	if w.resultRun != 0 {
		t.Errorf("Prober resultRun should be reset to 0")
	}
}

// TestResultRunOnStartupCheckFailure 验证 startup 探针失败达到阈值前结果保持 Unknown，达到后转为 Failure。
func TestResultRunOnStartupCheckFailure(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	m := newTestManager()
	w := newTestWorker(m, startup, v1.Probe{SuccessThreshold: 1, FailureThreshold: 3})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatusWithStarted(false))

	// 未达到失败阈值时 startup 探针状态保持不变（初始为失败/Unknown）。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg := "probe failure, result unknown"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Unknown, msg)
	if w.resultRun != 1 {
		t.Errorf("Prober resultRun should be 1")
	}

	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "2nd probe failure, result unknown"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Unknown, msg)
	if w.resultRun != 2 {
		t.Errorf("Prober resultRun should be 2")
	}

	// 超过失败阈值后 resultRun 归零，保证重启后的容器重新获得完整失败次数。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "3rd probe failure, result failure"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Failure, msg)
	if w.resultRun != 0 {
		t.Errorf("Prober resultRun should be reset to 0")
	}
}

// TestDoProbe_TerminatedRestartableInitContainerWithRestartPolicyNever 验证 Pod restartPolicy=Never 时，已终止的重启式 init 容器仍应继续探测。
func TestDoProbe_TerminatedRestartableInitContainerWithRestartPolicyNever(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()

	// 构造以 sidecar 为探测对象的 worker。
	w := newTestWorkerWithRestartableInitContainer(m, startup)

	// Pod 级重启策略设为 Never，但 sidecar 自身的 restartPolicy=Always。
	w.pod.Spec.RestartPolicy = v1.RestartPolicyNever

	// 构造 init 容器已终止的状态。
	terminatedStatus := getTestRunningStatus()
	terminatedStatus.InitContainerStatuses = []v1.ContainerStatus{{
		Name:        testContainerName,
		ContainerID: "test://test_container_id",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				StartedAt: metav1.Now(),
			},
		},
	}}
	terminatedStatus.ContainerStatuses[0].State.Running = nil

	m.statusManager.SetPodStatus(logger, w.pod, terminatedStatus)

	// 即使 Pod restartPolicy=Never，restartPolicy=Always 的 sidecar 终止后仍应继续探测。
	if !w.doProbe(ctx) {
		t.Error("Expected to continue probing for terminated restartable init container with pod restart policy Never")
	}

	// 已终止容器的探测结果应记录为 Failure。
	expectResult(t, w, results.Failure, "restartable init container with pod restart policy Never")
}

// TestDoProbe_TerminatedContainerWithRestartPolicyNever 验证普通容器在 Pod restartPolicy=Never 下终止后停止探测。
func TestDoProbe_TerminatedContainerWithRestartPolicyNever(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()

	// 普通容器没有独立 RestartPolicy，行为应与旧版本一致。
	w := newTestWorker(m, startup, v1.Probe{})

	// 显式清空容器级重启策略。
	w.container.RestartPolicy = nil

	// Pod 级重启策略设为 Never。
	w.pod.Spec.RestartPolicy = v1.RestartPolicyNever

	// 构造容器已终止的状态。
	terminatedStatus := getTestRunningStatus()
	terminatedStatus.ContainerStatuses[0].State.Running = nil
	terminatedStatus.ContainerStatuses[0].State.Terminated = &v1.ContainerStateTerminated{
		StartedAt: metav1.Now(),
	}

	m.statusManager.SetPodStatus(logger, w.pod, terminatedStatus)

	// 容器不会重启，探测应停止。
	if w.doProbe(ctx) {
		t.Error("Expected to stop probing for regular container with pod RestartPolicy=Never")
	}

	// 结果应记录为 Failure。
	expectResult(t, w, results.Failure, "regular container with pod restart policy Never")
}

// TestLivenessProbeDisabledByStarted 验证容器未启动时 liveness 探测被禁用，启动后才真正生效。
func TestLivenessProbeDisabledByStarted(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	m := newTestManager()
	w := newTestWorker(m, liveness, v1.Probe{SuccessThreshold: 1, FailureThreshold: 1})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatusWithStarted(false))
	// 容器尚未启动，liveness 探测失败也不生效。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg := "Not started, probe failure, result success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
	// 将容器标记为已启动。
	m.statusManager.SetContainerStartup(logger, w.pod.UID, w.containerID, true)
	// 此时探测失败应真正生效。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "Started, probe failure, result failure"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Failure, msg)
}

// TestStartupProbeDisabledByStarted 验证容器启动后 startup 探测被禁用。
func TestStartupProbeDisabledByStarted(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	m := newTestManager()
	w := newTestWorker(m, startup, v1.Probe{SuccessThreshold: 1, FailureThreshold: 2})
	m.statusManager.SetPodStatus(logger, w.pod, getTestRunningStatusWithStarted(false))
	// 未达到失败阈值的 startup 探测失败，结果保持 Unknown。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg := "Not started, probe failure, result unknown"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Unknown, msg)
	// startup 探测成功，结果转为 Success。
	m.prober.exec = fakeExecProber{probe.Success, nil}
	msg = "Started, probe success, result success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
	// 将容器标记为已启动，startup 探测随之禁用。
	m.statusManager.SetContainerStartup(logger, w.pod.UID, w.containerID, true)
	// 已启动后 startup 探测失败不再影响结果。
	m.prober.exec = fakeExecProber{probe.Failure, nil}
	msg = "Started, probe failure, result success"
	expectContinue(t, w, w.doProbe(ctx), msg)
	expectResult(t, w, results.Success, msg)
}

// TestChangeContainerStatusOnKubeletRestart 验证 kubelet 重启特性对探针初始结果的影响，覆盖普通容器与 sidecar。
func TestChangeContainerStatusOnKubeletRestart(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	tests := []struct {
		name           string
		featureEnabled bool
		isRestart      bool
		probeType      probeType
		initialValue   results.Result
		expectSet      bool
		isSidecar      bool
		expectedResult results.Result // only checked if expectSet is true
	}{
		{
			name:           "feature enabled, is restart, readiness",
			featureEnabled: true,
			isRestart:      true,
			probeType:      readiness,
			initialValue:   results.Failure,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Failure,
		},
		{
			name:           "feature enabled, is restart, liveness",
			featureEnabled: true,
			isRestart:      true,
			probeType:      liveness,
			initialValue:   results.Success,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Success,
		},
		{
			name:           "feature enabled, is restart, startup",
			featureEnabled: true,
			isRestart:      true,
			probeType:      startup,
			initialValue:   results.Unknown,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Unknown,
		},
		{
			name:           "feature enabled, not restart, readiness",
			featureEnabled: true,
			isRestart:      false,
			probeType:      readiness,
			initialValue:   results.Failure,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Failure,
		},
		{
			name:           "feature enabled, not restart, liveness",
			featureEnabled: true,
			isRestart:      false,
			probeType:      liveness,
			initialValue:   results.Success,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Success,
		},
		{
			name:           "feature enabled, not restart, startup",
			featureEnabled: true,
			isRestart:      false,
			probeType:      startup,
			initialValue:   results.Unknown,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Unknown,
		},
		{
			name:           "feature disabled, is restart, readiness",
			featureEnabled: false,
			isRestart:      true,
			probeType:      readiness,
			expectSet:      false,
			isSidecar:      false,
		},
		{
			name:           "feature disabled, is restart, liveness",
			featureEnabled: false,
			isRestart:      true,
			probeType:      liveness,
			expectSet:      false,
			isSidecar:      false,
		},
		{
			name:           "feature disabled, is restart, startup",
			featureEnabled: false,
			isRestart:      true,
			probeType:      startup,
			expectSet:      false,
			isSidecar:      false,
		},
		{
			name:           "feature disabled, not restart, readiness",
			featureEnabled: false,
			isRestart:      false,
			probeType:      readiness,
			initialValue:   results.Failure,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Failure,
		},
		{
			name:           "feature disabled, not restart, liveness",
			featureEnabled: false,
			isRestart:      false,
			probeType:      liveness,
			initialValue:   results.Success,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Success,
		},
		{
			name:           "feature disabled, not restart, startup",
			featureEnabled: false,
			isRestart:      false,
			probeType:      startup,
			initialValue:   results.Unknown,
			expectSet:      true,
			isSidecar:      false,
			expectedResult: results.Unknown,
		},
		// sidecar 用例用于回归 #136910：重启后 sidecar 的探针结果处理。
		{
			name:           "feature disabled, is restart, startup, sidecar",
			featureEnabled: false,
			isRestart:      true,
			probeType:      startup,
			initialValue:   results.Unknown,
			expectSet:      true,
			isSidecar:      true,
			expectedResult: results.Success, // Sidecars get Success, not initialValue
		},
		{
			name:           "feature disabled, is restart, liveness, sidecar",
			featureEnabled: false,
			isRestart:      true,
			probeType:      liveness,
			initialValue:   results.Success,
			expectSet:      false, // Readiness/liveness probes not set for sidecars on restart
			isSidecar:      true,
		},
		{
			name:           "feature disabled, is restart, readiness, sidecar",
			featureEnabled: false,
			isRestart:      true,
			probeType:      readiness,
			initialValue:   results.Failure,
			expectSet:      false, // Readiness/liveness probes not set for sidecars on restart
			isSidecar:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ChangeContainerStatusOnKubeletRestart, tc.featureEnabled)

			m := newTestManager()

			var w *worker
			var containerID kubecontainer.ContainerID

			if tc.isSidecar {
				w = newTestWorkerWithRestartableInitContainer(m, tc.probeType)
				w.spec = &v1.Probe{InitialDelaySeconds: 1000}
				// sidecar 的状态记录在 InitContainerStatuses 中。
				podStatus := getTestRunningStatus()
				// 把容器状态从普通容器移到 init 容器，模拟 sidecar。
				podStatus.InitContainerStatuses = []v1.ContainerStatus{podStatus.ContainerStatuses[0]}
				podStatus.ContainerStatuses = nil
				podStatus.InitContainerStatuses[0].ContainerID = "test://container-id"
				if tc.isRestart {
					podStatus.InitContainerStatuses[0].State.Running.StartedAt = metav1.Time{Time: m.start.Add(-5 * time.Minute)}
				} else {
					podStatus.InitContainerStatuses[0].State.Running.StartedAt = metav1.Time{Time: m.start.Add(5 * time.Minute)}
				}
				m.statusManager.SetPodStatus(logger, w.pod, podStatus)
				containerID = kubecontainer.ParseContainerID(logger, podStatus.InitContainerStatuses[0].ContainerID)
			} else {
				podStatus := getTestRunningStatus()
				podStatus.ContainerStatuses[0].ContainerID = "test://container-id"
				if tc.isRestart {
					podStatus.ContainerStatuses[0].State.Running.StartedAt = metav1.Time{Time: m.start.Add(-5 * time.Minute)}
				} else {
					podStatus.ContainerStatuses[0].State.Running.StartedAt = metav1.Time{Time: m.start.Add(5 * time.Minute)}
				}
				w = newTestWorker(m, tc.probeType, v1.Probe{InitialDelaySeconds: 1000})
				m.statusManager.SetPodStatus(logger, w.pod, podStatus)
				containerID = kubecontainer.ParseContainerID(logger, podStatus.ContainerStatuses[0].ContainerID)
			}

			w.doProbe(ctx)

			result, ok := resultsManager(m, tc.probeType).Get(containerID)

			if ok != tc.expectSet {
				t.Errorf("Expected result to be set: %v, but got: %v", tc.expectSet, ok)
			}
			if tc.expectSet && result != tc.expectedResult {
				t.Errorf("Expected result %v, but got: %v", tc.expectedResult, result)
			}
		})
	}
}
