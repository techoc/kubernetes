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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/clock"
)

// PodStartupLatencyTracker 记录启动延迟计算所需的关键时刻，
// 例如镜像拉取、watch 中首次观察到 Pod 运行等。
type PodStartupLatencyTracker interface {
	// ObservedPodOnWatch 记录 watch 到 Pod 的时刻，并在满足 SLO 条件时上报启动延迟。
	ObservedPodOnWatch(logger klog.Logger, pod *v1.Pod, when time.Time)
	// RecordImageStartedPulling 记录一次镜像拉取开始，用于扣除并发拉取的耗时。
	RecordImageStartedPulling(podUID types.UID)
	// RecordImageFinishedPulling 记录一次镜像拉取结束。
	RecordImageFinishedPulling(podUID types.UID)
	// RecordInitContainerStarted 记录 init 容器开始时间。
	RecordInitContainerStarted(podUID types.UID, startedAt time.Time)
	// RecordInitContainerFinished 记录 init 容器结束并累计其运行时长。
	RecordInitContainerFinished(podUID types.UID, finishedAt time.Time)
	// RecordStatusUpdated 在状态更新时标记 Pod 首次进入 Running 的时刻。
	RecordStatusUpdated(logger klog.Logger, pod *v1.Pod)
	// DeletePodStartupState 清理该 Pod 的跟踪状态。
	DeletePodStartupState(podUID types.UID)
}

// basicPodStartupLatencyTracker 是 PodStartupLatencyTracker 的默认实现。
type basicPodStartupLatencyTracker struct {
	// 保护 pods map 的并发读写。
	lock sync.Mutex
	pods map[types.UID]*perPodState
	// 记录因不可调度而被排除在 SLI 之外的 Pod；即使之后变为可调度也不再重新纳入。
	excludedPods map[types.UID]bool
	// 只对第一个使用网络（非 hostNetwork）的 Pod 记录指标。
	firstNetworkPodSeen bool
	// 时钟注入点，便于测试。
	clock clock.Clock
}

// imagePullSession 表示一次镜像拉取的起止时间，用于计算重叠拉取的总耗时。
type imagePullSession struct {
	start time.Time
	end   time.Time
}

// perPodState 保存单个 Pod 的启动延迟跟踪状态。
type perPodState struct {
	// 按会话跟踪镜像拉取，以便正确处理并发/重叠拉取。
	imagePullSessions       []imagePullSession
	imagePullSessionsStarts []time.Time // 记录多个并发拉取的开始时间
	// init 容器耗时跟踪。
	totalInitContainerRuntime time.Duration
	currentInitContainerStart time.Time
	// 首次观察到 Pod 进入 Running 的时间。
	observedRunningTime time.Time
	// 标记延迟指标是否已记录，避免重复上报。
	metricRecorded bool
}

// NewPodStartupLatencyTracker 创建 PodStartupLatencyTracker 实例。
func NewPodStartupLatencyTracker() PodStartupLatencyTracker {
	return &basicPodStartupLatencyTracker{
		pods:         map[types.UID]*perPodState{},
		excludedPods: map[types.UID]bool{},
		clock:        clock.RealClock{},
	}
}

// ObservedPodOnWatch 在 watch 事件中更新 Pod 跟踪状态，
// 满足启动 SLO 时计算并上报耗时。
func (p *basicPodStartupLatencyTracker) ObservedPodOnWatch(logger klog.Logger, pod *v1.Pod, when time.Time) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Pod 已终止（Failed/Succeeded）时不再需要跟踪启动延迟。
	if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
		delete(p.pods, pod.UID)
		return
	}

	state := p.pods[pod.UID]
	if state == nil {
		// 之前判定为不可调度的 Pod，不再重新纳入跟踪。
		if p.excludedPods[pod.UID] {
			return
		}

		// 首次见到该 Pod：StartTime 未设置时判断是否可调度。
		if pod.Status.StartTime.IsZero() {
			if isPodUnschedulable(pod) {
				p.excludedPods[pod.UID] = true
				return
			}

			// 可调度的 Pod 才创建跟踪记录。
			state = &perPodState{}
			p.pods[pod.UID] = state
		}
		return
	}

	// 后续才得知不可调度的 Pod 要从跟踪中移除，并加入排除集合。
	if isPodUnschedulable(pod) {
		delete(p.pods, pod.UID)
		p.excludedPods[pod.UID] = true
		return
	}

	if state.observedRunningTime.IsZero() {
		// Pod 尚未运行，等待后续状态更新。
		return
	}

	if state.metricRecorded {
		// 延迟已记录，直接跳过。
		return
	}

	if hasPodStartedSLO(pod) {
		// 端到端耗时从创建时间算起，SLO 耗时再扣除镜像拉取和 init 容器耗时。
		podStartingDuration := when.Sub(pod.CreationTimestamp.Time)
		podStartSLOduration := podStartingDuration

		totalImagesPullingTime := calculateImagePullingTime(state.imagePullSessions)
		if totalImagesPullingTime > 0 {
			podStartSLOduration -= totalImagesPullingTime
		}

		if state.totalInitContainerRuntime > 0 {
			podStartSLOduration -= state.totalInitContainerRuntime
		}

		podIsStateful := isStatefulPod(pod)

		logger.Info("Observed pod startup duration",
			"pod", klog.KObj(pod),
			"podStartSLOduration", podStartSLOduration.Seconds(),
			"podStartE2EDuration", podStartingDuration,
			"totalImagesPullingTime", totalImagesPullingTime,
			"totalInitContainerRuntime", state.totalInitContainerRuntime,
			"isStatefulPod", podIsStateful,
			"podCreationTimestamp", pod.CreationTimestamp.Time,
			"imagePullSessionsCount", len(state.imagePullSessions),
			"imagePullSessionsStartsCount", len(state.imagePullSessionsStarts),
			"observedRunningTime", state.observedRunningTime,
			"watchObservedRunningTime", when)

		metrics.PodStartTotalDuration.WithLabelValues().Observe(podStartingDuration.Seconds())
		if !podIsStateful {
			metrics.PodStartSLIDuration.WithLabelValues().Observe(podStartSLOduration.Seconds())
			// 记录第一个非 hostNetwork Pod 的启动耗时，用于定位 CNI 插件问题。
			if !pod.Spec.HostNetwork && !p.firstNetworkPodSeen {
				metrics.FirstNetworkPodStartSLIDuration.Set(podStartSLOduration.Seconds())
				p.firstNetworkPodSeen = true
			}
		}
		state.metricRecorded = true
	}
}

// calculateImagePullingTime 计算镜像拉取总耗时，并正确处理重叠的拉取会话。
func calculateImagePullingTime(sessions []imagePullSession) time.Duration {
	if len(sessions) == 0 {
		return 0
	}

	var totalTime time.Duration
	var currentEnd time.Time

	for i, session := range sessions {
		if session.end.IsZero() {
			continue
		}

		if i == 0 || session.start.After(currentEnd) {
			// 首个会话或与前一会话不重叠：整段计入。
			totalTime += session.end.Sub(session.start)
			currentEnd = session.end
		} else if session.end.After(currentEnd) {
			// 部分重叠：只计入超出当前结束时间的部分。
			totalTime += session.end.Sub(currentEnd)
			currentEnd = session.end
		}
		// 结束时间不晚于当前覆盖范围：完全重叠，不重复计时。
	}

	return totalTime
}

// RecordImageStartedPulling 追加一次拉取开始时间；未跟踪的 Pod 直接忽略。
func (p *basicPodStartupLatencyTracker) RecordImageStartedPulling(podUID types.UID) {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.pods[podUID]
	if state == nil {
		return
	}

	now := p.clock.Now()
	state.imagePullSessionsStarts = append(state.imagePullSessionsStarts, now)
}

// RecordImageFinishedPulling 结束最早的一次拉取会话，配对开始/结束时间。
func (p *basicPodStartupLatencyTracker) RecordImageFinishedPulling(podUID types.UID) {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.pods[podUID]
	if state == nil {
		return
	}

	now := p.clock.Now()

	// 有未完成的拉取时才创建会话。
	if len(state.imagePullSessionsStarts) > 0 {
		// 取最早的开始时间与当前时间配对，保证 FIFO 语义。
		startTime := state.imagePullSessionsStarts[0]
		session := imagePullSession{
			start: startTime,
			end:   now,
		}
		state.imagePullSessions = append(state.imagePullSessions, session)
		state.imagePullSessionsStarts = state.imagePullSessionsStarts[1:]
	}
}

// RecordInitContainerStarted 记录当前 init 容器的开始时间。
func (p *basicPodStartupLatencyTracker) RecordInitContainerStarted(podUID types.UID, startedAt time.Time) {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.pods[podUID]
	if state == nil {
		return
	}

	state.currentInitContainerStart = startedAt
}

// RecordInitContainerFinished 累计 init 容器运行时长，并清空开始时间以便下一个 init 容器使用。
func (p *basicPodStartupLatencyTracker) RecordInitContainerFinished(podUID types.UID, finishedAt time.Time) {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.pods[podUID]
	if state == nil {
		return
	}

	if !state.currentInitContainerStart.IsZero() {
		// 忽略非正时长，防止时钟回拨导致统计失真。
		initDuration := finishedAt.Sub(state.currentInitContainerStart)
		if initDuration > 0 {
			state.totalInitContainerRuntime += initDuration
		}
		state.currentInitContainerStart = time.Time{}
	}
}

// RecordStatusUpdated 在 Pod 首次满足启动 SLO 时记录观察时刻。
func (p *basicPodStartupLatencyTracker) RecordStatusUpdated(logger klog.Logger, pod *v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	state := p.pods[pod.UID]
	if state == nil {
		return
	}

	if state.metricRecorded {
		// 延迟已记录，直接跳过。
		return
	}

	if !state.observedRunningTime.IsZero() {
		// 已记录过首次运行时间，无需重复更新。
		return
	}

	if hasPodStartedSLO(pod) {
		logger.V(3).Info("Mark when the pod was running for the first time", "pod", klog.KObj(pod), "rv", pod.ResourceVersion)
		state.observedRunningTime = p.clock.Now()
	}
}

// hasPodStartedSLO 检查 Pod 的每个容器是否都至少启动过一次。
// 该判断对应 Pod startup latency SLI 的定义，参见
// https://github.com/kubernetes/community/blob/master/sig-scalability/slos/pod_startup_latency.md
func hasPodStartedSLO(pod *v1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil || cs.State.Running.StartedAt.IsZero() {
			return false
		}
	}

	return true
}

// isStatefulPod 按 SLI 文档判断 Pod 是否有状态：
// 挂载了除 secret、configMap、downwardAPI、emptyDir 之外的卷即视为有状态；
// Projected 卷由临时类型组成，也归为无状态。
// 参见 https://github.com/kubernetes/community/blob/master/sig-scalability/slos/pod_startup_latency.md
func isStatefulPod(pod *v1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		// 存在任一非临时类型的卷，即认为是有状态 Pod。
		if volume.Secret == nil &&
			volume.ConfigMap == nil &&
			volume.DownwardAPI == nil &&
			volume.EmptyDir == nil &&
			volume.Projected == nil {
			return true
		}
	}
	return false
}

// isPodUnschedulable 判断 Pod 是否应排除在 SLI 跟踪之外。
// SLI 要求 Pod 无需其他组件动作、不引发抢占即可立即调度；
// PodScheduled=False 的 Pod 不满足该条件，应排除。
func isPodUnschedulable(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodScheduled && condition.Status == v1.ConditionFalse {
			return true
		}
	}
	return false
}

// DeletePodStartupState 删除 Pod 的跟踪状态和排除标记。
func (p *basicPodStartupLatencyTracker) DeletePodStartupState(podUID types.UID) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.pods, podUID)
	delete(p.excludedPods, podUID)
}
