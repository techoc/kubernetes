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

// Package metrics 定义调度器暴露的 Prometheus 指标及其注册逻辑。
package metrics

import (
	"sync"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	resourceclaimmetrics "k8s.io/dynamic-resource-allocation/resourceclaim/metrics"
	"k8s.io/kubernetes/pkg/features"
	volumebindingmetrics "k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding/metrics"
)

const (
	// SchedulerSubsystem 是调度器指标使用的子系统名。
	SchedulerSubsystem = "scheduler"
)

// 以下是 work 和 operation 标签的可能取值。
const (
	// PrioritizingExtender 表示扩展器打分阶段的 work/operation 标签值。
	PrioritizingExtender = "prioritizing_extender"
	// Binding 表示绑定阶段的 work/operation 标签值。
	Binding = "binding"
)

const (
	// GoroutineResultSuccess 表示 goroutine 成功完成的 result 标签值。
	GoroutineResultSuccess = "success"
	// GoroutineResultError 表示 goroutine 出错的 result 标签值。
	GoroutineResultError = "error"
)

// ExtensionPoints 是 extension_point 标签的所有可能取值列表。
var ExtensionPoints = []string{
	PreFilter,
	Filter,
	PreFilterExtensionAddPod,
	PreFilterExtensionRemovePod,
	PostFilter,
	PreScore,
	Score,
	ScoreExtensionNormalize,
	PreBind,
	PreBindPreFlight,
	Bind,
	PostBind,
	Reserve,
	Unreserve,
	Permit,
	Sign,
	PlacementGenerate,
	PlacementFeasible,
	PodGroupPostFilter,
}

// 以下常量是 extension_point 标签的取值，对应框架的各个扩展点。
const (
	// PreFilter 对应 PreFilter 扩展点。
	PreFilter = "PreFilter"
	// Filter 对应 Filter 扩展点。
	Filter = "Filter"
	// PreFilterExtensionAddPod 对应 PreFilter 的 AddPod 扩展。
	PreFilterExtensionAddPod = "PreFilterExtensionAddPod"
	// PreFilterExtensionRemovePod 对应 PreFilter 的 RemovePod 扩展。
	PreFilterExtensionRemovePod = "PreFilterExtensionRemovePod"
	// PostFilter 对应 PostFilter 扩展点。
	PostFilter = "PostFilter"
	// PreScore 对应 PreScore 扩展点。
	PreScore = "PreScore"
	// Score 对应 Score 扩展点。
	Score = "Score"
	// ScoreExtensionNormalize 对应 Score 的归一化扩展。
	ScoreExtensionNormalize = "ScoreExtensionNormalize"
	// PreBind 对应 PreBind 扩展点。
	PreBind = "PreBind"
	// PreBindPreFlight 对应 PreBind 的预检扩展。
	PreBindPreFlight = "PreBindPreFlight"
	// Bind 对应 Bind 扩展点。
	Bind = "Bind"
	// PostBind 对应 PostBind 扩展点。
	PostBind = "PostBind"
	// Reserve 对应 Reserve 扩展点。
	Reserve = "Reserve"
	// Unreserve 对应 Unreserve 扩展点。
	Unreserve = "Unreserve"
	// Permit 对应 Permit 扩展点。
	Permit = "Permit"
	// Sign 对应 Sign 扩展点。
	Sign = "Sign"
	// PlacementGenerate 对应放置生成扩展点。
	PlacementGenerate = "PlacementGenerate"
	// PlacementFeasible 对应放置可行性检查扩展点。
	PlacementFeasible = "PlacementFeasible"
	// PlacementScore 对应放置打分扩展点。
	PlacementScore = "PlacementScore"
	// PlacementScoreExtensionNormalize 对应放置打分的归一化扩展。
	PlacementScoreExtensionNormalize = "PlacementScoreExtensionNormalize"
	// PodGroupPostFilter 对应 PodGroup 的 PostFilter 扩展点。
	PodGroupPostFilter = "PodGroupPostFilter"
)

// 以下常量是排队提示（queueing hint）评估结果的标签取值。
const (
	// QueueingHintResultQueue 表示提示为“立即入队”。
	QueueingHintResultQueue = "Queue"
	// QueueingHintResultQueueSkip 表示提示为“跳过入队”。
	QueueingHintResultQueueSkip = "QueueSkip"
	// QueueingHintResultError 表示提示评估出错。
	QueueingHintResultError = "Error"
)

// 以下实体标签值用于 queued_entities 和 queue_incoming_entities 指标。
const (
	// Pod 表示实体类型为 Pod。
	Pod = "pod"
	// PodGroup 表示实体类型为 PodGroup。
	PodGroup = "podgroup"
)

const (
	// PodPoppedInFlightEvent 是 Pod 从 in-flight 事件中弹出时使用的事件标签值。
	PodPoppedInFlightEvent = "PodPopped"
)

// 批量调度尝试的可能结果。
const (
	// BatchAttemptNoHint 表示本次调度没有可用的节点提示。
	BatchAttemptNoHint = "no_hint"
	// BatchAttemptHintUsed 表示调度采用了给出的节点提示。
	BatchAttemptHintUsed = "hint_used"
	// BatchAttemptHintNotUsed 表示给出了提示但调度未采用。
	BatchAttemptHintNotUsed = "hint_not_used"
)

// 批量缓存失效的可能原因。
const (
	// BatchFlushPodFailed 表示上一个 Pod 调度失败。
	BatchFlushPodFailed = "pod_failed"
	// BatchFlushPodSkipped 表示上一个 Pod 由其他 profile 调度。
	BatchFlushPodSkipped = "pod_skipped"
	// BatchFlushPodNominated 表示当前 Pod 带有提名节点。
	BatchFlushPodNominated = "pod_nominated"
	// BatchFlushNodeMissing 表示缓存中的节点在快照中已不存在。
	BatchFlushNodeMissing = "node_missing"
	// BatchFlushEmptyList 表示缓存的候选节点列表为空。
	BatchFlushEmptyList = "empty_list"
	// BatchFlushExpired 表示缓存已过期。
	BatchFlushExpired = "expired"
	// BatchFlushPodIncompatible 表示当前 Pod 与缓存状态不兼容。
	BatchFlushPodIncompatible = "pod_incompatible"
	// BatchFlushPodNotBatchable 表示当前 Pod 不可批量化。
	BatchFlushPodNotBatchable = "pod_not_batchable"
	// BatchFlushFilterError 表示重新过滤时出错。
	BatchFlushFilterError = "filter_error"
	// BatchFlushPreScoreError 表示重新执行 PreScore 时出错。
	BatchFlushPreScoreError = "prescore_error"
	// BatchFlushRescoreError 表示重新打分时出错。
	BatchFlushRescoreError = "rescore_error"
	// BatchFlushNormalizeError 表示分数归一化时出错。
	BatchFlushNormalizeError = "normalize_error"
)

// DRADeviceBindingConditions 指标的状态标签取值。
const (
	// BindingConditionsStatusSuccess 表示绑定条件满足。
	BindingConditionsStatusSuccess = "success"
	// BindingConditionsStatusFailed 表示绑定条件不满足。
	BindingConditionsStatusFailed = "failure"
	// BindingConditionsStatusTimeout 表示等待绑定条件超时。
	BindingConditionsStatusTimeout = "timeout"
	// BindingConditionsStatusError 表示检查绑定条件时出错。
	BindingConditionsStatusError = "error"
)

// 所有直方图指标的最小桶大小为 1ms。
var (
	scheduleAttempts *metrics.CounterVec
	// EventHandlingLatency 记录调度队列处理各类集群事件的耗时。
	EventHandlingLatency *metrics.HistogramVec
	schedulingLatency    *metrics.HistogramVec
	// SchedulingAlgorithmLatency 记录一次调度算法（不含绑定）的执行耗时。
	SchedulingAlgorithmLatency *metrics.Histogram
	// PreemptionVictims 记录单次 Pod 抢占所选牺牲者数量的分布。
	PreemptionVictims *metrics.Histogram
	// PreemptionAttempts 记录集群中累计的抢占尝试次数。
	PreemptionAttempts *metrics.Counter
	pendingPods        *metrics.GaugeVec
	// QueuedEntities 按队列和实体类型记录排队中的调度实体数。
	QueuedEntities *metrics.GaugeVec
	// InFlightEvents 记录调度队列中当前正在跟踪的事件数。
	InFlightEvents *metrics.GaugeVec
	// Goroutines 按工作类型记录当前运行的 goroutine 数量。
	Goroutines *metrics.GaugeVec
	// BatchAttemptStats 记录批量调度尝试的结果分布。
	BatchAttemptStats *metrics.CounterVec
	// BatchCacheFlushed 按原因记录批量缓存失效的次数。
	BatchCacheFlushed *metrics.CounterVec
	// BatchRescoreAttempts 记录批量调度中重新打分的尝试次数。
	BatchRescoreAttempts *metrics.CounterVec
	// BatchRescoreDuration 记录批量调度中重新打分单个节点的耗时。
	BatchRescoreDuration *metrics.HistogramVec
	// GetNodeHintDuration 记录获取节点提示的耗时。
	GetNodeHintDuration *metrics.HistogramVec
	// StoreScheduleResultsDuration 记录保存调度结果的耗时。
	StoreScheduleResultsDuration *metrics.HistogramVec

	// PodSchedulingSLIDuration 记录 Pod 从入队到成功调度的端到端耗时。
	PodSchedulingSLIDuration *metrics.HistogramVec
	// PodSchedulingAttempts 记录成功调度一个 Pod 所用的尝试次数分布。
	PodSchedulingAttempts *metrics.Histogram
	// PodScheduledAfterFlush 记录因超时被移出 unschedulableEntities 后成功调度的 Pod 数。
	PodScheduledAfterFlush *metrics.Counter
	// PreQueueingHintEvaluations 记录各插件 PreQueueingHint 评估的次数与结果。
	PreQueueingHintEvaluations *metrics.CounterVec
	// FrameworkExtensionPointDuration 记录运行某扩展点全部插件的耗时。
	FrameworkExtensionPointDuration *metrics.HistogramVec
	// PluginExecutionDuration 记录在指定扩展点运行单个插件的耗时。
	PluginExecutionDuration *metrics.HistogramVec

	// PermitWaitDuration 记录在 permit 阶段等待的时长。
	PermitWaitDuration *metrics.HistogramVec
	// CacheSize 记录调度器缓存中的节点、Pod 和 assumed Pod 数量。
	CacheSize            *metrics.GaugeVec
	unschedulableReasons *metrics.GaugeVec
	// PluginEvaluationTotal 记录各插件在各扩展点评估 Pod 的次数。
	PluginEvaluationTotal *metrics.CounterVec

	queueingHintExecutionDuration *metrics.HistogramVec
	// SchedulerQueueIncomingPods 按事件和队列类型记录进入调度队列的 Pod 数。
	SchedulerQueueIncomingPods *metrics.CounterVec
	// SchedulerQueueIncomingEntities 按事件、队列和实体类型记录进入调度队列的实体数。
	SchedulerQueueIncomingEntities *metrics.CounterVec

	// 以下两个指标仅在 async-preemption 特性门控启用时才可用。
	// PreemptionGoroutinesDuration 记录抢占 goroutine 的执行耗时。
	PreemptionGoroutinesDuration *metrics.HistogramVec
	// PreemptionGoroutinesExecutionTotal 记录已执行的抢占 goroutine 数量。
	PreemptionGoroutinesExecutionTotal *metrics.CounterVec

	// 以下指标仅在 SchedulerAsyncAPICalls 特性门控启用时才可用。
	// AsyncAPICallsTotal 记录异步分发器执行的 API 调用总数。
	AsyncAPICallsTotal *metrics.CounterVec
	// AsyncAPICallDuration 记录异步分发器执行 API 调用的耗时。
	AsyncAPICallDuration *metrics.HistogramVec
	// AsyncAPIPendingCalls 记录异步队列中当前待处理的 API 调用数。
	AsyncAPIPendingCalls *metrics.GaugeVec

	// 以下指标仅在 DRAExtendedResource 特性门控启用时才可用。
	// 它与 kube-controller-manager 中记录的是同一个指标。
	ResourceClaimCreatesTotal = resourceclaimmetrics.ResourceClaimCreate

	podGroupScheduleAttempts  *metrics.CounterVec
	podGroupSchedulingLatency *metrics.HistogramVec
	// PodGroupSchedulingAlgorithmLatency 记录 PodGroup 调度算法的耗时。
	PodGroupSchedulingAlgorithmLatency *metrics.Histogram

	// 以下指标仅在 TopologyAwareWorkloadScheduling 特性门控启用时才可用。
	// GeneratedPlacementsTotal 记录调度 PodGroup 时生成的候选放置方案数。
	GeneratedPlacementsTotal *metrics.CounterVec
	// PlacementEvaluations 按结果记录评估候选放置方案的次数。
	PlacementEvaluations *metrics.CounterVec
	// PlacementEvaluationDuration 按结果记录评估单个候选放置方案的耗时。
	PlacementEvaluationDuration *metrics.HistogramVec

	// 以下指标仅在 DRADeviceBindingConditions 特性门控启用时才可用。
	// DRABindingConditionsAllocationsTotal 记录使用带 BindingConditions 设备的分配次数。
	DRABindingConditionsAllocationsTotal *metrics.CounterVec
	// DRABindingConditionsPreBindDuration 记录 PreBind 阶段等待 BindingConditions 满足的时长。
	DRABindingConditionsPreBindDuration *metrics.HistogramVec

	// WorkloadPreemptionAttempts 记录工作负载发起的抢占尝试次数。
	WorkloadPreemptionAttempts *metrics.CounterVec
	// WorkloadPreemptionVictims 记录工作负载抢占导致的 Pod 牺牲者数量分布。
	WorkloadPreemptionVictims *metrics.Histogram
	// PreemptionWorkloadDisruptions 记录被抢占的工作负载单元数量分布。
	PreemptionWorkloadDisruptions *metrics.HistogramVec
	// PreemptionEvaluationDuration 记录确定抢占目标的耗时。
	PreemptionEvaluationDuration *metrics.HistogramVec
	// PreemptionExecutionDuration 记录执行抢占的耗时。
	PreemptionExecutionDuration *metrics.HistogramVec
	// PreemptionPDBViolations 记录抢占导致的 PDB 违规次数。
	PreemptionPDBViolations *metrics.CounterVec

	// metricsList 是不论特性门控取值如何、始终需要注册的指标列表。
	metricsList []metrics.Registerable
)

var registerMetrics sync.Once

// Register 注册所有指标，通过 sync.Once 保证只执行一次。
func Register() {
	// 执行实际注册，sync.Once 保证只跑一次。
	registerMetrics.Do(func() {
		InitMetrics()
		RegisterMetrics(metricsList...)
		volumebindingmetrics.RegisterVolumeSchedulingMetrics()

		if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerAsyncPreemption) {
			RegisterMetrics(PreemptionGoroutinesDuration, PreemptionGoroutinesExecutionTotal)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.SchedulerAsyncAPICalls) {
			RegisterMetrics(
				AsyncAPICallsTotal,
				AsyncAPICallDuration,
				AsyncAPIPendingCalls,
			)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
			resourceclaimmetrics.RegisterMetrics()
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
			RegisterMetrics(
				podGroupScheduleAttempts,
				podGroupSchedulingLatency,
				PodGroupSchedulingAlgorithmLatency,
				WorkloadPreemptionAttempts,
				WorkloadPreemptionVictims,
				PreemptionWorkloadDisruptions,
				PreemptionEvaluationDuration,
				PreemptionExecutionDuration,
				PreemptionPDBViolations,
			)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
			RegisterMetrics(
				GeneratedPlacementsTotal,
				PlacementEvaluations,
				PlacementEvaluationDuration,
			)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions) {
			RegisterMetrics(
				DRABindingConditionsAllocationsTotal,
				DRABindingConditionsPreBindDuration,
			)
		}
	})
}

// InitMetrics 构造并初始化所有指标对象。
func InitMetrics() {
	scheduleAttempts = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "schedule_attempts_total",
			Help:           "Number of attempts to schedule pods, by the result. 'unschedulable' means a pod could not be scheduled, while 'error' means an internal scheduler problem.",
			StabilityLevel: metrics.STABLE,
		}, []string{"result", "profile"})

	EventHandlingLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "event_handling_duration_seconds",
			Help:      "Event handling latency in seconds.",
			// 从 0.1ms 开始，最后一个桶为 [~200ms, Inf)。
			Buckets:        metrics.ExponentialBuckets(0.0001, 2, 12),
			StabilityLevel: metrics.ALPHA,
		}, []string{"event"})

	schedulingLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "scheduling_attempt_duration_seconds",
			Help:           "Scheduling attempt latency in seconds (scheduling algorithm + binding)",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.STABLE,
		}, []string{"result", "profile"})
	SchedulingAlgorithmLatency = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "scheduling_algorithm_duration_seconds",
			Help:           "Scheduling algorithm latency in seconds",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.BETA,
		},
	)
	PreemptionVictims = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "preemption_victims",
			Help:      "Number of selected preemption victims for preemption initiated by a single pod",
			// 牺牲者数量超过 64 很少见，因此把 [64, +Inf) 合并为单个桶。
			Buckets:        metrics.ExponentialBuckets(1, 2, 7),
			StabilityLevel: metrics.STABLE,
		})
	PreemptionAttempts = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "preemption_attempts_total",
			Help:           "Total preemption attempts in the cluster till now",
			StabilityLevel: metrics.STABLE,
		})
	pendingPods = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "pending_pods",
			Help:           "Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.",
			StabilityLevel: metrics.STABLE,
		}, []string{"queue"})
	QueuedEntities = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "queued_entities",
			Help:           "Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"queue", "type"})
	InFlightEvents = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "inflight_events",
			Help:           "Number of events currently tracked in the scheduling queue.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"event"})
	Goroutines = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "goroutines",
			Help:           "Number of running goroutines split by the work they do such as binding.",
			StabilityLevel: metrics.BETA,
		}, []string{"operation"})
	BatchAttemptStats = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "batch_attempts_total",
			Help:           "Counts of results when we attempt to use batching.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"profile", "result"})
	BatchCacheFlushed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "batch_cache_flushed_total",
			Help:           "Counts of cache flushes by reason.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"profile", "reason"})
	BatchRescoreAttempts = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "batch_rescore_attempts_total",
			Help:           "Counts of rescore attempts during opportunistic batching.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"profile"})

	PodSchedulingSLIDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "pod_scheduling_sli_duration_seconds",
			Help:      "E2e latency for a pod being scheduled, from the time the pod enters the scheduling queue and might involve multiple scheduling attempts.",
			// 从 10ms 开始，最后一个桶为 [~88m, Inf)。
			Buckets:        metrics.ExponentialBuckets(0.01, 2, 20),
			StabilityLevel: metrics.BETA,
		},
		[]string{"attempts"})

	PodSchedulingAttempts = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "pod_scheduling_attempts",
			Help:           "Number of attempts to successfully schedule a pod.",
			Buckets:        metrics.ExponentialBuckets(1, 2, 5),
			StabilityLevel: metrics.STABLE,
		})

	PodScheduledAfterFlush = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "pod_scheduled_after_flush_total",
			Help:           "Number of pods that were successfully scheduled after being flushed from unschedulableEntities due to timeout. This metric helps detect potential queueing hint misconfigurations or event handling issues.",
			StabilityLevel: metrics.ALPHA,
		})

	PreQueueingHintEvaluations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "pre_queueing_hint_evaluations_total",
			Help:           "Number of PreQueueingHint evaluations, labeled by plugin and result (all_pods or narrowed).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin", "result"})

	FrameworkExtensionPointDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "framework_extension_point_duration_seconds",
			Help:      "Latency for running all plugins of a specific extension point.",
			// 从 0.1ms 开始，最后一个桶为 [~200ms, Inf)。
			Buckets:        metrics.ExponentialBuckets(0.0001, 2, 12),
			StabilityLevel: metrics.STABLE,
		},
		[]string{"extension_point", "status", "profile"})

	PluginExecutionDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "plugin_execution_duration_seconds",
			Help:      "Duration for running a plugin at a specific extension point.",
			// 从 0.01ms 开始，最后一个桶为 [~22ms, Inf)。插件耗时对精度很敏感，
			// 因此采用较小的增长因子（1.5）以获得更细的粒度。
			Buckets:        metrics.ExponentialBuckets(0.00001, 1.5, 20),
			StabilityLevel: metrics.BETA,
		},
		[]string{"plugin", "extension_point", "status"})

	queueingHintExecutionDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "queueing_hint_execution_duration_seconds",
			Help:      "Duration for running a queueing hint function of a plugin.",
			// 从 0.01ms 开始，最后一个桶为 [~22ms, Inf)。插件耗时对精度很敏感，
			// 因此采用较小的增长因子（1.5）以获得更细的粒度。
			Buckets:        metrics.ExponentialBuckets(0.00001, 1.5, 20),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin", "event", "hint"})

	SchedulerQueueIncomingPods = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "queue_incoming_pods_total",
			Help:           "Number of pods added to scheduling queues by event and queue type.",
			StabilityLevel: metrics.STABLE,
		}, []string{"queue", "event"})

	SchedulerQueueIncomingEntities = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "queue_incoming_entities_total",
			Help:           "Number of scheduling entities added to scheduling queues by event, queue type, and entity type. Entity types are either 'pod' (for individual pods that are not members of any podgroup) or 'podgroup'.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"queue", "event", "type"})

	PermitWaitDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "permit_wait_duration_seconds",
			Help:           "Duration of waiting on permit.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.BETA,
		},
		[]string{"result"})

	CacheSize = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "cache_size",
			Help:           "Number of nodes, pods, and assumed (bound) pods in the scheduler cache.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"type"})

	unschedulableReasons = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "unschedulable_pods",
			Help:           "The number of unschedulable pods broken down by plugin name. A pod will increment the gauge for all plugins that caused it to not schedule and so this metric have meaning only when broken down by plugin.",
			StabilityLevel: metrics.BETA,
		}, []string{"plugin", "profile"})

	PluginEvaluationTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "plugin_evaluation_total",
			Help:           "Number of attempts to schedule pods by each plugin and the extension point (available only in PreFilter, Filter, PreScore, and Score).",
			StabilityLevel: metrics.BETA,
		}, []string{"plugin", "extension_point", "profile"})

	PreemptionGoroutinesDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:         SchedulerSubsystem,
			Name:              "preemption_goroutines_duration_seconds",
			Help:              "Duration in seconds for running goroutines for the preemption.",
			Buckets:           metrics.ExponentialBuckets(0.01, 2, 20),
			StabilityLevel:    metrics.ALPHA,
			DeprecatedVersion: "1.37.0",
		},
		[]string{"result"})

	PreemptionGoroutinesExecutionTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "preemption_goroutines_execution_total",
			Help:           "Number of preemption goroutines executed.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"})

	// 以下三个指标（AsyncAPICallsTotal、AsyncAPICallDuration、AsyncAPIPendingCalls）
	// 仅在 SchedulerAsyncAPICalls 特性门控启用时才可用。
	AsyncAPICallsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "async_api_call_execution_total",
			Help:           "Total number of API calls executed by the async dispatcher.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"call_type", "result"})

	AsyncAPICallDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "async_api_call_execution_duration_seconds",
			Help:           "Duration in seconds for executing API call in the async dispatcher.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"call_type", "result"})

	AsyncAPIPendingCalls = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "pending_async_api_calls",
			Help:           "Number of API calls currently pending in the async queue.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"call_type"})

	DRABindingConditionsAllocationsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "dra_bindingconditions_allocations_total",
			Help:           "Number of allocations using devices with BindingConditions, counted per driver per scheduling attempt",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "driver", "status"},
	)

	DRABindingConditionsPreBindDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "dra_bindingconditions_wait_duration_seconds",
			Help:           "Time in seconds spent waiting for BindingConditions to be satisfied during PreBind.",
			Buckets:        metrics.ExponentialBuckets(0.1, 2, 14),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "driver", "status"},
	)

	GetNodeHintDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "get_node_hint_duration_seconds",
			Help:      "Latency for getting a node hint.",
			// 从 0.01ms 开始，最后一个桶为 [~20ms, Inf)
			Buckets:        metrics.ExponentialBuckets(0.00001, 2, 12),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"hinted", "profile"})

	StoreScheduleResultsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "store_schedule_results_duration_seconds",
			Help:      "Latency for storing scheduling results.",
			// 从 0.01ms 开始，最后一个桶为 [~20ms, Inf)
			Buckets:        metrics.ExponentialBuckets(0.00001, 2, 12),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile"})

	BatchRescoreDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "batch_rescore_duration_seconds",
			Help:      "Latency for rescoring a node during opportunistic batching.",
			// 从 0.01ms 开始，最后一个桶为 [~20ms, Inf)
			Buckets:        metrics.ExponentialBuckets(0.00001, 2, 12),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile"})

	// 以下指标（podGroupScheduleAttempts、podGroupSchedulingLatency、
	// PodGroupSchedulingAlgorithmLatency）仅在 GenericWorkload 特性门控启用时才可用。
	podGroupScheduleAttempts = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "podgroup_schedule_attempts_total",
			Help:           "Number of attempts to schedule pod group, by the result. 'unschedulable' means a pod group could not be scheduled, while 'error' means an internal scheduler problem.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"result", "profile"})
	podGroupSchedulingLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "podgroup_scheduling_attempt_duration_seconds",
			Help:           "Pod group scheduling attempt latency in seconds",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		}, []string{"result", "profile"})
	PodGroupSchedulingAlgorithmLatency = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "podgroup_scheduling_algorithm_duration_seconds",
			Help:           "Pod group scheduling algorithm latency in seconds",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		})

	// 工作负载抢占相关指标。
	WorkloadPreemptionAttempts = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "workload_preemption_attempts_total",
			Help:           "Total preemption attempts initiated by workload (including pod groups) in the cluster till now.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"result"})
	WorkloadPreemptionVictims = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "workload_preemption_victims",
			Help:      "Number of pod preemption victims caused by workload preemption.",
			// 从 1 开始，最后一个桶为 [1024, Inf)
			Buckets:        metrics.ExponentialBuckets(1, 2, 11),
			StabilityLevel: metrics.ALPHA,
		})
	PreemptionWorkloadDisruptions = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "preemption_workload_disruptions",
			Help:      "Number of workload preemption units being preempted. A single preemption unit can be all pods in a pod group (in case of DisruptionMode=all), or a single pod (in case of DisruptionMode=single).",
			// 从 1 开始，最后一个桶为 [1024, Inf)
			Buckets:        metrics.ExponentialBuckets(1, 2, 11),
			StabilityLevel: metrics.ALPHA,
		}, []string{"preemptor"})
	PreemptionEvaluationDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "preemption_evaluation_duration_seconds",
			Help:      "Duration in seconds for identifying the target preemption victims.",
			// 从 1ms 开始，最后一个桶为 [~32.8s, Inf)
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 16),
			StabilityLevel: metrics.ALPHA,
		}, []string{"preemptor", "result"})
	PreemptionExecutionDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "preemption_execution_duration_seconds",
			Help:      "Duration in seconds for preempting the target preemption victims. With async preemption enabled, preemption execution does not block the scheduling of other pods.",
			// 从 1ms 开始，最后一个桶为 [~32.8s, Inf)
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 16),
			StabilityLevel: metrics.ALPHA,
		}, []string{"preemptor", "result"})
	PreemptionPDBViolations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "preemption_pdb_violations_total",
			Help:           "Total number of pod disruption budget violations caused by preemption.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"preemptor"},
	)

	// 以下指标（GeneratedPlacementsTotal、PlacementEvaluations、
	// PlacementEvaluationDuration）仅在 TopologyAwareWorkloadScheduling 特性门控启用时才可用。
	GeneratedPlacementsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "generated_placements_total",
			Help:           "Number of candidate placements generated when scheduling pod groups.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"profile"})
	PlacementEvaluations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "placement_evaluations_total",
			Help:           "Number of candidate placements evaluated when scheduling pod groups, by result. 'feasible' means the pod group fit into the placement, while 'infeasible' means it did not.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"result", "profile"})
	PlacementEvaluationDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      SchedulerSubsystem,
			Name:           "placement_evaluation_duration_seconds",
			Help:           "Latency in seconds of evaluating a single candidate placement when scheduling pod groups, by result. 'feasible' means the pod group fit into the placement, while 'infeasible' means it did not.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		}, []string{"result", "profile"})

	metricsList = []metrics.Registerable{
		scheduleAttempts,
		schedulingLatency,
		SchedulingAlgorithmLatency,
		EventHandlingLatency,
		PreemptionVictims,
		PreemptionAttempts,
		pendingPods,
		QueuedEntities,
		PodSchedulingSLIDuration,
		PodSchedulingAttempts,
		PodScheduledAfterFlush,
		FrameworkExtensionPointDuration,
		PluginExecutionDuration,
		SchedulerQueueIncomingPods,
		SchedulerQueueIncomingEntities,
		Goroutines,
		PermitWaitDuration,
		CacheSize,
		unschedulableReasons,
		PluginEvaluationTotal,
		BatchAttemptStats,
		BatchCacheFlushed,
		BatchRescoreAttempts,
		BatchRescoreDuration,
		GetNodeHintDuration,
		StoreScheduleResultsDuration,
		queueingHintExecutionDuration,
		InFlightEvents,
		PreQueueingHintEvaluations,
	}
}

// RegisterMetrics 注册一组指标。
// 导出该函数是为了让 out-of-tree 插件也能注册自己的自定义指标。
func RegisterMetrics(extraMetrics ...metrics.Registerable) {
	for _, metric := range extraMetrics {
		legacyregistry.MustRegister(metric)
	}
}

// GetGather 返回指标的 gatherer，供包外测试用例使用。
func GetGather() metrics.Gatherer {
	return legacyregistry.DefaultGatherer
}

// ActivePods 返回 queue 标签为 active 的 pending pods 指标。
func ActivePods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "active"})
}

// BackoffPods 返回 queue 标签为 backoff 的 pending pods 指标。
func BackoffPods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "backoff"})
}

// UnschedulablePods 返回 queue 标签为 unschedulable 的 pending pods 指标。
func UnschedulablePods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "unschedulable"})
}

// GatedPods 返回 queue 标签为 gated 的 pending pods 指标。
func GatedPods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "gated"})
}

// IncompletePodGroupPods 返回 queue 标签为 "incomplete" 的 pending pods 指标。
func IncompletePodGroupPods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "incomplete"})
}

// PendingPodGroupPods 返回 queue 标签为 "pending" 的 pending pods 指标。
func PendingPodGroupPods() metrics.GaugeMetric {
	return pendingPods.With(metrics.Labels{"queue": "pending"})
}

// ActiveEntities 返回 queue 标签为 "active"、type 标签为指定实体类型（"Pod" 或 "PodGroup"）的 queued entities 指标。
func ActiveEntities(entityType string) metrics.GaugeMetric {
	return QueuedEntities.With(metrics.Labels{"queue": "active", "type": entityType})
}

// BackoffEntities 返回 queue 标签为 "backoff"、type 标签为指定实体类型（"Pod" 或 "PodGroup"）的 queued entities 指标。
func BackoffEntities(entityType string) metrics.GaugeMetric {
	return QueuedEntities.With(metrics.Labels{"queue": "backoff", "type": entityType})
}

// UnschedulableEntities 返回 queue 标签为 "unschedulable"、type 标签为指定实体类型（"Pod" 或 "PodGroup"）的 queued entities 指标。
func UnschedulableEntities(entityType string) metrics.GaugeMetric {
	return QueuedEntities.With(metrics.Labels{"queue": "unschedulable", "type": entityType})
}

// GatedEntities 返回 queue 标签为 "gated"、type 标签为指定实体类型（"Pod" 或 "PodGroup"）的 queued entities 指标。
func GatedEntities(entityType string) metrics.GaugeMetric {
	return QueuedEntities.With(metrics.Labels{"queue": "gated", "type": entityType})
}

// SinceInSeconds 返回从 start 到现在的经过时间（秒）。
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// UnschedulableReason 返回指定插件和 profile 对应的 unschedulable pods 指标。
func UnschedulableReason(plugin string, profile string) metrics.GaugeMetric {
	return unschedulableReasons.With(metrics.Labels{"plugin": plugin, "profile": profile})
}
