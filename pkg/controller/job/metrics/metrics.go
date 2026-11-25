/*
Copyright 2021 The Kubernetes Authors.

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

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// JobControllerSubsystem 是本控制器使用的指标子系统名称。
const JobControllerSubsystem = "job_controller"

var (
	// JobSyncDurationSeconds 记录 Job 同步的耗时。可能的标签值：
	//   completion_mode: Indexed, NonIndexed
	//   result:          success, error
	//   action:          reconciling, tracking, pods_created, pods_deleted
	JobSyncDurationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "job_sync_duration_seconds",
			Help:           "The time it took to sync a job",
			StabilityLevel: metrics.STABLE,
			Buckets:        metrics.ExponentialBuckets(0.004, 2, 15),
		},
		[]string{"completion_mode", "result", "action"},
	)
	// JobSyncNum 记录 Job 同步的次数。可能的标签值：
	//   completion_mode: Indexed, NonIndexed
	//   result:          success, error
	//   action:          reconciling, tracking, pods_created, pods_deleted
	JobSyncNum = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "job_syncs_total",
			Help:           "The number of job syncs",
			StabilityLevel: metrics.STABLE,
		},
		[]string{"completion_mode", "result", "action"},
	)
	// JobFinishedNum 记录已结束的 Job 数量。
	// "CompletionsReached" 这个 reason 标签用于统计成功完成的 Job。
	// 可能的标签值：
	//   completion_mode: Indexed, NonIndexed
	//   result:          failed, succeeded
	//   reason:          "BackoffLimitExceeded", "DeadlineExceeded", "PodFailurePolicy", "FailedIndexes", "MaxFailedIndexesExceeded", "SuccessPolicy", "CompletionsReached"
	JobFinishedNum = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "jobs_finished_total",
			Help:           "The number of finished jobs",
			StabilityLevel: metrics.STABLE,
		},
		[]string{"completion_mode", "result", "reason"},
	)

	// JobByExternalControllerTotal 记录由外部控制器创建并管理的 Job 数量。
	// controller_name 标签的取值对应 managedBy 字段的值。
	JobByExternalControllerTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "jobs_by_external_controller_total",
			Help:           "The number of Jobs managed by an external controller",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"controller_name"},
	)

	// JobPodsFinished 记录 Job 控制器完成跟踪的已结束 Pod 数量。
	// 仅适用于在 JobTrackingWithFinalizers 特性门控开启时创建的 Job。
	// 可能的标签值：
	//   completion_mode: Indexed, NonIndexed
	//   result:          failed, succeeded
	JobPodsFinished = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "job_pods_finished_total",
			Help:           "The number of finished Pods that are fully tracked",
			StabilityLevel: metrics.STABLE,
		},
		[]string{"completion_mode", "result"})

	// PodFailuresHandledByFailurePolicy 记录由 Pod 失败策略处理的已结束 Pod 数量。
	// 可能的标签值：
	//   action: FailJob, Ignore, Count
	PodFailuresHandledByFailurePolicy = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: JobControllerSubsystem,
			Name:      "pod_failures_handled_by_failure_policy_total",
			Help: `The number of failed Pods handled by failure policy with
			respect to the failure policy action applied based on the matched
			rule. Possible values of the action label correspond to the
			possible values for the failure policy rule action, which are:
			"FailJob", "Ignore" and "Count".`,
			StabilityLevel: metrics.BETA,
		},
		[]string{"action"})

	// TerminatedPodsTrackingFinalizerTotal 记录带有 batch.kubernetes.io/job-tracking
	// finalizer 的已终止 Pod 的增加与移除次数，无论其是否归属于某个 Job。
	TerminatedPodsTrackingFinalizerTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: JobControllerSubsystem,
			Name:      "terminated_pods_tracking_finalizer_total",
			Help: `The number of terminated pods (phase=Failed|Succeeded)
that have the finalizer batch.kubernetes.io/job-tracking
The event label can be "add" or "delete".`,
			StabilityLevel: metrics.BETA,
		}, []string{"event"})

	// JobFinishedIndexesTotal 记录已完成的索引（index）数量。
	JobFinishedIndexesTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: JobControllerSubsystem,
			Name:      "job_finished_indexes_total",
			Help: `The number of finished indexes. Possible values for the
			status label are: "succeeded", "failed". Possible values for the
			backoffLimit label are: "perIndex" and "global"`,
		},
		[]string{"status", "backoffLimit"})

	// JobPodsCreationTotal 记录 Job 控制器创建的 Pod 数量，按创建原因
	// （例如是否指定了 PodReplacementPolicy）和创建结果（成功或失败）分类。
	// 可能的标签值：
	//   reason: new, recreate_terminating_or_failed, recreate_failed
	//   status: succeeded, failed
	JobPodsCreationTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: JobControllerSubsystem,
			Name:      "job_pods_creation_total",
			Help: `The number of Pods created by the Job controller labelled with a reason for the Pod creation.
This metric also distinguishes between Pods created using different PodReplacementPolicy settings.
Possible values of the "reason" label are:
"new", "recreate_terminating_or_failed", "recreate_failed".
Possible values of the "status" label are:
"succeeded", "failed".`,
		}, []string{"reason", "status"})

	// JobRequeueSkips 记录因 watch 缓存过期而跳过的 Job 同步次数。
	JobRequeueSkips = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      JobControllerSubsystem,
			Name:           "stale_sync_skips_total",
			Help:           "Total number of Job syncs skipped due to a stale watch cache.",
			StabilityLevel: metrics.ALPHA,
		},
		// 这些是指标的标签（维度）。
		[]string{"group", "resource"},
	)
)

const (
	// 以下是上述指标中 "action" 标签的可能取值。

	// JobSyncActionReconciling 表示 Job 的 Pod 创建/删除预期尚未满足，
	// 控制器正在等待已发出的 Pod 创建/删除操作完成。
	JobSyncActionReconciling = "reconciling"
	// JobSyncActionTracking 表示 Job 的 Pod 创建/删除预期已满足，且活跃
	// Pod 数量与预期一致（即本次同步未发出创建/删除操作）。这是大多数同步
	// 中的常见动作。
	JobSyncActionTracking = "tracking"
	// JobSyncActionPodsCreated 表示控制器创建了 Pod。这发生在活跃 Pod 数量
	// 少于 Job 期望并行度时。
	JobSyncActionPodsCreated = "pods_created"
	// JobSyncActionPodsDeleted 表示控制器删除了 Pod。这发生在 Job 被挂起，
	// 或活跃 Pod 数量超过并行度时。
	JobSyncActionPodsDeleted = "pods_deleted"

	// 以下是上述指标中 "result" 以及 "status"（job_pods_creation_total）
	// 标签的可能取值。

	// Succeeded 表示成功。
	Succeeded = "succeeded"
	// Failed 表示失败。
	Failed = "failed"

	// 以下是 terminated_pods_tracking_finalizer 指标中 "event" 标签的可能
	// 取值。
	// Add 表示新增带跟踪 finalizer 的已终止 Pod。
	Add = "add"
	// Delete 表示删除带跟踪 finalizer 的已终止 Pod。
	Delete = "delete"

	// 以下是 job_pods_creation_total 指标中 "reason" 标签的可能取值。

	// PodCreateNew 表示使用默认策略创建新 Pod。
	PodCreateNew = "new"
	// PodRecreateTerminatingOrFailed 表示因 Pod 正在终止或已失败而重建 Pod。
	PodRecreateTerminatingOrFailed = "recreate_terminating_or_failed"
	// PodRecreateFailed 表示因 Pod 已失败而重建 Pod。
	PodRecreateFailed = "recreate_failed"
)

// registerMetrics 保证指标只注册一次。
var registerMetrics sync.Once

// Register 注册 Job 控制器的所有指标。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(JobSyncDurationSeconds)
		legacyregistry.MustRegister(JobSyncNum)
		legacyregistry.MustRegister(JobFinishedNum)
		legacyregistry.MustRegister(JobPodsFinished)
		legacyregistry.MustRegister(PodFailuresHandledByFailurePolicy)
		legacyregistry.MustRegister(TerminatedPodsTrackingFinalizerTotal)
		legacyregistry.MustRegister(JobFinishedIndexesTotal)
		legacyregistry.MustRegister(JobPodsCreationTotal)
		legacyregistry.MustRegister(JobByExternalControllerTotal)
		legacyregistry.MustRegister(JobRequeueSkips)
	})
}
