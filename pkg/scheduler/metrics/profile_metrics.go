/*
Copyright 2020 The Kubernetes Authors.

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

package metrics

// 本文件包含与调度 profile 关联的指标辅助函数。

var (
	// ScheduledResult 表示调度成功的结果标签。
	ScheduledResult = "scheduled"
	// UnschedulableResult 表示 Pod 不可调度的结果标签。
	UnschedulableResult = "unschedulable"
	// WaitingOnPreemptionResult 表示 Pod 组正在等待抢占的结果标签。
	WaitingOnPreemptionResult = "waiting_on_preemption"
	// ErrorResult 表示调度过程出错的结果标签。
	ErrorResult = "error"
	// FeasibleResult 表示候选放置可行的结果标签。
	FeasibleResult = "feasible"
	// InfeasibleResult 表示候选放置不可行的结果标签。
	InfeasibleResult = "infeasible"
)

// PodScheduled 记录一次成功的调度尝试，以及自 `start` 以来的耗时。
func PodScheduled(profile string, duration float64) {
	observeScheduleAttemptAndLatency(ScheduledResult, profile, duration)
}

// PodUnschedulable 记录一次针对不可调度 Pod 的调度尝试，以及自 `start` 以来的耗时。
func PodUnschedulable(profile string, duration float64) {
	observeScheduleAttemptAndLatency(UnschedulableResult, profile, duration)
}

// PodScheduleError 记录一次出错的调度尝试，以及自 `start` 以来的耗时。
func PodScheduleError(profile string, duration float64) {
	observeScheduleAttemptAndLatency(ErrorResult, profile, duration)
}

// observeScheduleAttemptAndLatency 按结果与 profile 同时记录调度耗时和尝试次数。
func observeScheduleAttemptAndLatency(result, profile string, duration float64) {
	schedulingLatency.WithLabelValues(result, profile).Observe(duration)
	scheduleAttempts.WithLabelValues(result, profile).Inc()
}

// PodGroupScheduled 记录一次成功的 Pod 组调度尝试，以及自 `start` 以来的耗时。
func PodGroupScheduled(profile string, duration float64) {
	observePodGroupScheduleAttemptAndLatency(ScheduledResult, profile, duration)
}

// PodGroupUnschedulable 记录一次针对不可调度 Pod 组的调度尝试，以及自 `start` 以来的耗时。
func PodGroupUnschedulable(profile string, duration float64) {
	observePodGroupScheduleAttemptAndLatency(UnschedulableResult, profile, duration)
}

// PodGroupWaitingOnPreemption 记录一次因等待抢占而不可调度的 Pod 组调度尝试，以及自 `start` 以来的耗时。
func PodGroupWaitingOnPreemption(profile string, duration float64) {
	observePodGroupScheduleAttemptAndLatency(WaitingOnPreemptionResult, profile, duration)
}

// PodGroupScheduleError 记录一次出错的 Pod 组调度尝试，以及自 `start` 以来的耗时。
func PodGroupScheduleError(profile string, duration float64) {
	observePodGroupScheduleAttemptAndLatency(ErrorResult, profile, duration)
}

// observePodGroupScheduleAttemptAndLatency 按结果与 profile 同时记录 Pod 组调度耗时和尝试次数。
func observePodGroupScheduleAttemptAndLatency(result, profile string, duration float64) {
	podGroupSchedulingLatency.WithLabelValues(result, profile).Observe(duration)
	podGroupScheduleAttempts.WithLabelValues(result, profile).Inc()
}

// RecordGeneratedPlacements 记录单个调度周期内为 Pod 组生成的候选放置数量。
func RecordGeneratedPlacements(profile string, count int) {
	GeneratedPlacementsTotal.WithLabelValues(profile).Add(float64(count))
}

// ObservePlacementEvaluation 按结果记录一次候选放置评估及其耗时。
func ObservePlacementEvaluation(result, profile string, duration float64) {
	PlacementEvaluations.WithLabelValues(result, profile).Inc()
	PlacementEvaluationDuration.WithLabelValues(result, profile).Observe(duration)
}
