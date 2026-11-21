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

package util

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
)

// frozenTime 是测试统一的冻结时间，配合 fake clock 保证时间逻辑可重复。
var frozenTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	// 测试固定使用的 Pod UID 与指标名称。
	uid         = "3c1df8a9-11a8-4791-aeae-184c18cca686"
	metricsName = "kubelet_pod_start_sli_duration_seconds"
)

// TestNoEvents 验证没有任何 Pod 事件时不会记录启动延迟指标。
func TestNoEvents(t *testing.T) {
	t.Run("metrics registered; no incoming events", func(t *testing.T) {

		// 期望指标输出为空
		wants := ""

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods:         map[types.UID]*perPodState{},
			excludedPods: map[types.UID]bool{},
		}

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestPodsRunningBeforeKubeletStarted 验证 kubelet 启动前已在运行的 Pod 不会被计入启动延迟。
func TestPodsRunningBeforeKubeletStarted(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("pod was running for 10m before kubelet started", func(t *testing.T) {

		// 期望指标输出为空
		wants := ""

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods:         map[types.UID]*perPodState{},
			excludedPods: map[types.UID]bool{},
		}

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		podStarted := &corev1.Pod{
			Status: corev1.PodStatus{
				StartTime: &metav1.Time{Time: frozenTime.Add(-10 * time.Minute)},
			},
		}
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime)

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestSinglePodOneImageDownloadRecorded 验证单 Pod、单次镜像拉取的 SLI 计算与直方图输出。
func TestSinglePodOneImageDownloadRecorded(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("single pod; started in 3s, image pulling 100ms", func(t *testing.T) {

		wants := `
# HELP kubelet_pod_start_sli_duration_seconds [ALPHA] Duration in seconds to start a pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch
# TYPE kubelet_pod_start_sli_duration_seconds histogram
kubelet_pod_start_sli_duration_seconds_bucket{le="0.5"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="1"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="2"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="3"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="4"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="5"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="6"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="8"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="10"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="20"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="30"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="45"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="60"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="120"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="180"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="240"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="300"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="360"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="480"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="900"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1200"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1800"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="2700"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="3600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="+Inf"} 1
kubelet_pod_start_sli_duration_seconds_sum 2.9
kubelet_pod_start_sli_duration_seconds_count 1
		`

		fakeClock := testingclock.NewFakeClock(frozenTime)

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		podInit := buildInitializingPod()
		tracker.ObservedPodOnWatch(logger, podInit, frozenTime)

		// 模拟镜像拉取耗时 100ms
		tracker.RecordImageStartedPulling(podInit.UID)
		fakeClock.Step(time.Millisecond * 100)
		tracker.RecordImageFinishedPulling(podInit.UID)

		podState, ok := tracker.pods[podInit.UID]
		if !ok {
			t.Errorf("expected to track pod: %s, but pod not found", podInit.UID)
		}

		if len(podState.imagePullSessions) != 1 {
			t.Errorf("expected one image pull session to be recorded")
		}

		podStarted := buildRunningPod()
		tracker.RecordStatusUpdated(logger, podStarted)

		// 3 秒后在 watch 中再次观察到同一个 Pod
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*3))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(podStarted.UID)

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestSinglePodMultipleDownloadsAndRestartsRecorded 验证重叠镜像拉取按时间并集扣除，且重复观察不会重复记录指标。
func TestSinglePodMultipleDownloadsAndRestartsRecorded(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("single pod; started in 30s, overlapping image pulling between 10th and 20th seconds", func(t *testing.T) {

		wants := `
# HELP kubelet_pod_start_sli_duration_seconds [ALPHA] Duration in seconds to start a pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch
# TYPE kubelet_pod_start_sli_duration_seconds histogram
kubelet_pod_start_sli_duration_seconds_bucket{le="0.5"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="1"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="2"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="3"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="4"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="5"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="6"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="8"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="10"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="20"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="30"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="45"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="60"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="120"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="180"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="240"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="300"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="360"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="480"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="900"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1200"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1800"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="2700"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="3600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="+Inf"} 1
kubelet_pod_start_sli_duration_seconds_sum 20
kubelet_pod_start_sli_duration_seconds_count 1
`

		fakeClock := testingclock.NewFakeClock(frozenTime)

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		podInitializing := buildInitializingPod()
		tracker.ObservedPodOnWatch(logger, podInitializing, frozenTime)
		// Image 1: 10-16s
		fakeClock.SetTime(frozenTime.Add(time.Second * 10))
		tracker.RecordImageStartedPulling(podInitializing.UID)
		// Image 2: 11-18s
		fakeClock.SetTime(frozenTime.Add(time.Second * 11))
		tracker.RecordImageStartedPulling(podInitializing.UID)
		// Image 3: 14-20s
		fakeClock.SetTime(frozenTime.Add(time.Second * 14))
		tracker.RecordImageStartedPulling(podInitializing.UID)
		fakeClock.SetTime(frozenTime.Add(time.Second * 16))
		tracker.RecordImageFinishedPulling(podInitializing.UID)
		fakeClock.SetTime(frozenTime.Add(time.Second * 18))
		tracker.RecordImageFinishedPulling(podInitializing.UID)
		fakeClock.SetTime(frozenTime.Add(time.Second * 20))
		tracker.RecordImageFinishedPulling(podInitializing.UID)

		podState, ok := tracker.pods[podInitializing.UID]
		if !ok {
			t.Errorf("expected to track pod: %s, but pod not found", podInitializing.UID)
		}
		if len(podState.imagePullSessions) != 3 {
			t.Errorf("expected 3 image pull sessions but got %d", len(podState.imagePullSessions))
		}
		totalTime := calculateImagePullingTime(podState.imagePullSessions)
		expectedTime := time.Second * 10
		if totalTime != expectedTime {
			t.Errorf("expected total pulling time: %v but got %v", expectedTime, totalTime)
		}

		// Pod 已启动
		podStarted := buildRunningPod()
		tracker.RecordStatusUpdated(logger, podStarted)

		// 在 30 秒时于 watch 中再次观察到同一个 Pod
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*30))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		// 后续 Pod 观察不应改变指标，因为每个 Pod 只记录一次
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*150))
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*200))
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*250))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(podStarted.UID)

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestPodWithInitContainersAndMainContainers 验证 init 容器运行时长会从 Pod 启动 SLI 中扣除。
func TestPodWithInitContainersAndMainContainers(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("single pod with multiple init containers and main containers", func(t *testing.T) {

		wants := `
# HELP kubelet_pod_start_sli_duration_seconds [ALPHA] Duration in seconds to start a pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch
# TYPE kubelet_pod_start_sli_duration_seconds histogram
kubelet_pod_start_sli_duration_seconds_bucket{le="0.5"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="1"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="2"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="3"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="4"} 0
kubelet_pod_start_sli_duration_seconds_bucket{le="5"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="6"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="8"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="10"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="20"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="30"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="45"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="60"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="120"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="180"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="240"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="300"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="360"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="480"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="900"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1200"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="1800"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="2700"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="3600"} 1
kubelet_pod_start_sli_duration_seconds_bucket{le="+Inf"} 1
kubelet_pod_start_sli_duration_seconds_sum 4.2
kubelet_pod_start_sli_duration_seconds_count 1
		`

		fakeClock := testingclock.NewFakeClock(frozenTime)

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		podInit := buildInitializingPod("init-1", "init-2")
		tracker.ObservedPodOnWatch(logger, podInit, frozenTime)

		// init 容器 1 镜像拉取：0.5s-1.5s
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 500))
		tracker.RecordImageStartedPulling(podInit.UID)
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 1500))
		tracker.RecordImageFinishedPulling(podInit.UID)
		// init 容器 1 运行时长：2s-4s
		fakeClock.SetTime(frozenTime.Add(time.Second * 2))
		tracker.RecordInitContainerStarted(types.UID(uid), fakeClock.Now())
		fakeClock.SetTime(frozenTime.Add(time.Second * 4))
		tracker.RecordInitContainerFinished(types.UID(uid), fakeClock.Now())

		// init 容器 2 镜像拉取：4.2s-5.2s
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 4200))
		tracker.RecordImageStartedPulling(podInit.UID)
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 5200))
		tracker.RecordImageFinishedPulling(podInit.UID)
		// init 容器 2 运行时长：5.5s-7s
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 5500))
		tracker.RecordInitContainerStarted(types.UID(uid), fakeClock.Now())
		fakeClock.SetTime(frozenTime.Add(time.Second * 7))
		tracker.RecordInitContainerFinished(types.UID(uid), fakeClock.Now())

		// 主容器 1 镜像拉取：7.2s-8.2s
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 7200))
		tracker.RecordImageStartedPulling(podInit.UID)
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 8200))
		tracker.RecordImageFinishedPulling(podInit.UID)
		// 主容器 2 镜像拉取：7.3s-8.5s
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 7300))
		tracker.RecordImageStartedPulling(podInit.UID)
		fakeClock.SetTime(frozenTime.Add(time.Millisecond * 8500))
		tracker.RecordImageFinishedPulling(podInit.UID)

		// Pod 在 11 秒时进入运行状态
		podStarted := buildRunningPod()
		tracker.RecordStatusUpdated(logger, podStarted)
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*11))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		state := tracker.pods[types.UID(uid)]
		assert.NotNil(t, state, "Pod state should exist")

		expectedInitRuntime := 2*time.Second + 1500*time.Millisecond
		assert.Equal(t, expectedInitRuntime, state.totalInitContainerRuntime,
			"Total init container runtime should be 3.5s (2s + 1.5s)")

		totalImageTime := calculateImagePullingTime(state.imagePullSessions)
		expectedImageTime := 3300 * time.Millisecond // 1s + 1s + 1.3s concurrent overlap
		assert.Equal(t, expectedImageTime, totalImageTime,
			"Image pulling time should be 3.3s: init-1(1s) + init-2(1s) + concurrent-main(1.3s)")

		assert.Len(t, state.imagePullSessions, 4, "Should have 4 image pull sessions")

		// 清理测试状态
		tracker.DeletePodStartupState(podStarted.UID)

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestImmediatelySchedulablePods 验证不可立即调度的 Pod 会被排除在跟踪与指标之外。
func TestImmediatelySchedulablePods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("pods not immediately schedulable should be excluded from tracking", func(t *testing.T) {
		wants := ""

		fakeClock := testingclock.NewFakeClock(frozenTime)
		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods:         map[types.UID]*perPodState{},
			excludedPods: map[types.UID]bool{},
			clock:        fakeClock,
		}

		// PodScheduled=False 表示该 Pod 不可立即调度
		podNotSchedulable := buildInitializingPod()
		podNotSchedulable.UID = "not-schedulable-pod"
		podNotSchedulable.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: "Unschedulable",
			},
		}
		tracker.ObservedPodOnWatch(logger, podNotSchedulable, frozenTime)
		assert.Empty(t, tracker.pods, "Pod with PodScheduled=False should not be tracked")

		// PodScheduled=True 表示该 Pod 可立即调度
		podSchedulable := buildInitializingPod()
		podSchedulable.UID = "schedulable-pod"
		podSchedulable.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
				Reason: "Scheduled",
			},
		}
		tracker.ObservedPodOnWatch(logger, podSchedulable, frozenTime)
		assert.Len(t, tracker.pods, 1, "Pod with PodScheduled=True should be tracked")

		// 缺少 PodScheduled 条件时默认按可调度处理
		podNoCondition := buildInitializingPod()
		podNoCondition.UID = "no-condition-pod"
		tracker.ObservedPodOnWatch(logger, podNoCondition, frozenTime)
		assert.Len(t, tracker.pods, 2, "Pod without PodScheduled condition should be tracked by default")

		// 确认未产生指标
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(podSchedulable.UID)
		tracker.DeletePodStartupState(podNoCondition.UID)
		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})

	t.Run("pod observed as schedulable first, then becomes unschedulable should not be tracked", func(t *testing.T) {
		wants := ""

		fakeClock := testingclock.NewFakeClock(frozenTime)
		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		// 先观察到 Pod 可调度
		podSchedulable := buildInitializingPod()
		podSchedulable.UID = "becomes-unschedulable"
		tracker.ObservedPodOnWatch(logger, podSchedulable, frozenTime)
		assert.Len(t, tracker.pods, 1, "Pod should be tracked initially")

		// 之后观察到同一 Pod 变为不可调度
		podUnschedulable := buildInitializingPod()
		podUnschedulable.UID = "becomes-unschedulable"
		podUnschedulable.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: "Unschedulable",
			},
		}

		tracker.ObservedPodOnWatch(logger, podUnschedulable, frozenTime.Add(time.Second))
		assert.Empty(t, tracker.pods, "Pod should be removed when it becomes unschedulable")

		// 确认没有产生指标
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		metrics.PodStartSLIDuration.Reset()
	})

	t.Run("pod observed as unschedulable first, then becomes schedulable should remain not tracked", func(t *testing.T) {
		wants := ""

		fakeClock := testingclock.NewFakeClock(frozenTime)
		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods:         map[types.UID]*perPodState{},
			excludedPods: map[types.UID]bool{},
			clock:        fakeClock,
		}

		// 先观察到 Pod 不可调度
		podUnschedulable := buildInitializingPod()
		podUnschedulable.UID = "unschedulable-first"
		podUnschedulable.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: "Unschedulable",
			},
		}

		tracker.ObservedPodOnWatch(logger, podUnschedulable, frozenTime)
		assert.Empty(t, tracker.pods, "Pod should not be tracked when first observed as unschedulable")
		assert.True(t, tracker.excludedPods[podUnschedulable.UID], "Pod should be in excludedPods map")

		// 之后观察到同一 Pod 变为可调度（例如集群自动扩缩容后）
		podSchedulable := buildInitializingPod()
		podSchedulable.UID = "unschedulable-first"
		podSchedulable.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
				Reason: "Scheduled",
			},
		}

		tracker.ObservedPodOnWatch(logger, podSchedulable, frozenTime.Add(time.Second*5))
		assert.Empty(t, tracker.pods, "Pod should remain excluded even after becoming schedulable")
		assert.True(t, tracker.excludedPods[podSchedulable.UID], "Pod should remain in excludedPods map")

		// 完成启动流程，仍不应记录指标
		podRunning := buildRunningPod()
		podRunning.UID = "unschedulable-first"
		tracker.RecordStatusUpdated(logger, podRunning)
		tracker.ObservedPodOnWatch(logger, podRunning, frozenTime.Add(time.Second*10))

		// 确认没有记录 SLI 指标
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), metricsName); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(podRunning.UID)
		assert.Empty(t, tracker.pods)
		assert.False(t, tracker.excludedPods[podRunning.UID], "Pod should be removed from excludedPods on cleanup")
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestStatefulPodExclusion 验证使用持久化存储的有状态 Pod 不参与启动 SLI 统计。
func TestStatefulPodExclusion(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("stateful pods should be excluded from SLI metrics", func(t *testing.T) {
		wantsSLI := ""

		fakeClock := testingclock.NewFakeClock(frozenTime)
		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		// 创建使用 PVC 的有状态 Pod
		statefulPod := buildInitializingPod()
		statefulPod.UID = "stateful-pod"
		statefulPod.Spec.Volumes = []corev1.Volume{
			{
				Name: "persistent-storage",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "test-pvc",
					},
				},
			},
		}

		tracker.ObservedPodOnWatch(logger, statefulPod, frozenTime)

		statefulPodRunning := buildRunningPod()
		statefulPodRunning.UID = "stateful-pod"
		statefulPodRunning.Spec.Volumes = statefulPod.Spec.Volumes
		tracker.RecordStatusUpdated(logger, statefulPodRunning)

		// 3 秒后观察到 Pod 已运行
		tracker.ObservedPodOnWatch(logger, statefulPodRunning, frozenTime.Add(time.Second*3))

		// 确认有状态 Pod 没有 SLI 指标
		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wantsSLI), metricsName); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(statefulPod.UID)
		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// TestIsStatefulPod 验证按卷类型判定 Pod 是否为有状态（PVC 或 hostPath 视为有状态）。
func TestIsStatefulPod(t *testing.T) {
	tests := []struct {
		name     string
		volumes  []corev1.Volume
		expected bool
	}{
		{
			name:     "no volumes is stateless",
			volumes:  nil,
			expected: false,
		},
		{
			name: "projected volume (such as service account token) is stateless",
			volumes: []corev1.Volume{
				{
					Name: "kube-api-access-abcde",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
								{ConfigMap: &corev1.ConfigMapProjection{
									LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
								}},
								{DownwardAPI: &corev1.DownwardAPIProjection{}},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "projected volume with a secret and configmap is stateless",
			volumes: []corev1.Volume{
				{
					Name:         "kube-api-access-abcde",
					VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}},
				},
				{
					Name:         "my-secret",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "s"}},
				},
				{
					Name:         "my-config",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "c"}}},
				},
			},
			expected: false,
		},
		{
			name: "PVC volume is stateful",
			volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc"},
					},
				},
			},
			expected: true,
		},
		{
			name: "PVC with a  projected volume is stateful",
			volumes: []corev1.Volume{
				{
					Name:         "kube-api-access-abcde",
					VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}},
				},
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc"},
					},
				},
			},
			expected: true,
		},
		{
			name: "hostPath volume is stateful",
			volumes: []corev1.Volume{
				{
					Name:         "host-vol",
					VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data"}},
				},
			},
			expected: true,
		},
		{
			name: "emptyDir is stateless",
			volumes: []corev1.Volume{
				{
					Name:         "scratch",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				},
			},
			expected: false,
		},
		{
			name: "downwardAPI is stateless",
			volumes: []corev1.Volume{
				{
					Name:         "podinfo",
					VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{}},
				},
			},
			expected: false,
		},
		{
			name: "secret is stateless",
			volumes: []corev1.Volume{
				{
					Name:         "my-secret",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "s"}},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: tc.volumes,
				},
			}
			result := isStatefulPod(pod)
			assert.Equal(t, tc.expected, result, "isStatefulPod mismatch for %q", tc.name)
		})
	}
}

// TestFirstNetworkPodMetrics 验证只记录节点上第一个非 hostNetwork Pod 的启动耗时。
func TestFirstNetworkPodMetrics(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	t.Run("first network pod; started in 30s, image pulling between 10th and 20th seconds", func(t *testing.T) {

		wants := `
# HELP kubelet_first_network_pod_start_sli_duration_seconds [INTERNAL] Duration in seconds to start the first network pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch
# TYPE kubelet_first_network_pod_start_sli_duration_seconds gauge
kubelet_first_network_pod_start_sli_duration_seconds 30
`

		fakeClock := testingclock.NewFakeClock(frozenTime)

		metrics.Register()

		tracker := &basicPodStartupLatencyTracker{
			pods: map[types.UID]*perPodState{}, excludedPods: map[types.UID]bool{},
			clock: fakeClock,
		}

		// hostNetwork Pod 不应被跟踪
		hostNetworkPodInitializing := buildInitializingPod()
		hostNetworkPodInitializing.UID = "11111-22222"
		hostNetworkPodInitializing.Spec.HostNetwork = true
		tracker.ObservedPodOnWatch(logger, hostNetworkPodInitializing, frozenTime)

		hostNetworkPodStarted := buildRunningPod()
		hostNetworkPodStarted.UID = "11111-22222"
		hostNetworkPodStarted.Spec.HostNetwork = true
		tracker.RecordStatusUpdated(logger, hostNetworkPodStarted)

		// 只跟踪第一个使用网络命名空间的 Pod
		podInitializing := buildInitializingPod()
		tracker.ObservedPodOnWatch(logger, podInitializing, frozenTime)

		// Pod 已启动
		podStarted := buildRunningPod()
		tracker.RecordStatusUpdated(logger, podStarted)

		// 在 30 秒时于 watch 中再次观察到同一个 Pod
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*30))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), "kubelet_first_network_pod_start_sli_duration_seconds", "kubelet_first_network_pod_start_total_duration_seconds"); err != nil {
			t.Fatal(err)
		}

		// 后续 Pod 观察不应改变指标，因为每个 Pod 只记录一次
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*150))
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*200))
		tracker.ObservedPodOnWatch(logger, podStarted, frozenTime.Add(time.Second*250))

		if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(wants), "kubelet_first_network_pod_start_sli_duration_seconds", "kubelet_first_network_pod_start_total_duration_seconds"); err != nil {
			t.Fatal(err)
		}

		// 清理测试状态
		tracker.DeletePodStartupState(hostNetworkPodStarted.UID)
		tracker.DeletePodStartupState(podStarted.UID)

		assert.Empty(t, tracker.pods)
		metrics.PodStartSLIDuration.Reset()
	})
}

// buildInitializingPod 构造处于 PodInitializing 状态的 Pod，可选地附加 init 容器。
func buildInitializingPod(initContainerNames ...string) *corev1.Pod {
	pod := buildPodWithStatus([]corev1.ContainerStatus{
		{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
	})

	// 按需添加 init 容器
	if len(initContainerNames) > 0 {
		pod.Spec.InitContainers = make([]corev1.Container, len(initContainerNames))
		for i, name := range initContainerNames {
			pod.Spec.InitContainers[i] = corev1.Container{Name: name}
		}
	}

	return pod
}

// buildRunningPod 构造容器已运行、时间戳固定在 frozenTime 的 Pod。
func buildRunningPod() *corev1.Pod {
	return buildPodWithStatus([]corev1.ContainerStatus{
		{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(frozenTime)}}},
	})
}

// buildPodWithStatus 以固定 UID 和创建时间构造带容器状态的 Pod。
func buildPodWithStatus(cs []corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			CreationTimestamp: metav1.NewTime(frozenTime),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: cs,
		},
	}
}

// TestCalculateImagePullingTime 验证多次镜像拉取会话按时间区间并集计算总耗时。
func TestCalculateImagePullingTime(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("empty sessions should return zero", func(t *testing.T) {
		sessions := []imagePullSession{}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, time.Duration(0), result)
	})

	t.Run("incomplete sessions should be ignored (for example: kubelet restart)", func(t *testing.T) {
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(3 * time.Second)},                        // valid: 3s
			{start: baseTime.Add(5 * time.Second), end: time.Time{}},                     // incomplete, ignored
			{start: baseTime.Add(10 * time.Second), end: baseTime.Add(12 * time.Second)}, // valid: 2s
		}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, 5*time.Second, result) // 3s + 2s
	})

	t.Run("non-overlapping sessions", func(t *testing.T) {
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(2 * time.Second)},                        // 2s
			{start: baseTime.Add(5 * time.Second), end: baseTime.Add(8 * time.Second)},   // 3s
			{start: baseTime.Add(10 * time.Second), end: baseTime.Add(15 * time.Second)}, // 5s
		}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, 10*time.Second, result) // 2s + 3s + 5s
	})

	t.Run("partially overlapping sessions", func(t *testing.T) {
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(5 * time.Second)},                       // 0-5s
			{start: baseTime.Add(3 * time.Second), end: baseTime.Add(8 * time.Second)},  // 3-8s (overlap 3-5s)
			{start: baseTime.Add(7 * time.Second), end: baseTime.Add(12 * time.Second)}, // 7-12s (overlap 7-8s)
		}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, 12*time.Second, result) // 12s
	})

	t.Run("completely overlapped sessions", func(t *testing.T) {
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(10 * time.Second)},                     // 0-10s
			{start: baseTime.Add(2 * time.Second), end: baseTime.Add(8 * time.Second)}, // 2-8s (completely inside)
			{start: baseTime.Add(3 * time.Second), end: baseTime.Add(5 * time.Second)}, // 3-5s (completely inside)
		}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, 10*time.Second, result) // Only outer session: 10s
	})

	t.Run("partially and completely overlapping sessions", func(t *testing.T) {
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(10 * time.Second)},                     // 0-10s
			{start: baseTime.Add(2 * time.Second), end: baseTime.Add(5 * time.Second)}, // 2-5s (completely inside first)
			{start: baseTime.Add(3 * time.Second), end: baseTime.Add(7 * time.Second)}, // 3-7s (completely inside first and partially overlapping second)
		}
		result := calculateImagePullingTime(sessions)
		assert.Equal(t, 10*time.Second, result) // 10s
	})

	t.Run("completely overlapped and partially overlapped sessions with gap", func(t *testing.T) {
		// 混合完全重叠的会话与相互独立的会话
		sessions := []imagePullSession{
			{start: baseTime, end: baseTime.Add(8 * time.Second)},                        // 0-8s
			{start: baseTime.Add(2 * time.Second), end: baseTime.Add(6 * time.Second)},   // 2-6s (completely inside first)
			{start: baseTime.Add(3 * time.Second), end: baseTime.Add(5 * time.Second)},   // 3-5s (completely inside first)
			{start: baseTime.Add(15 * time.Second), end: baseTime.Add(20 * time.Second)}, // 15-20s (separate)
			{start: baseTime.Add(16 * time.Second), end: baseTime.Add(18 * time.Second)}, // 16-18s (completely inside fourth)
			{start: baseTime.Add(17 * time.Second), end: baseTime.Add(21 * time.Second)}, // 17-21s (partially overlapping fourth and fifth)
		}
		result := calculateImagePullingTime(sessions)
		// 8s（第一组）+ 6s（第二组）= 14s
		assert.Equal(t, 14*time.Second, result)
	})
}
