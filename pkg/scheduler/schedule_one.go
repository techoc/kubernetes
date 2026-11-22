/*
Copyright 2014 The Kubernetes Authors.

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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	compresource "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	utiltrace "k8s.io/utils/trace"
)

const (
	// 插件指标采样比例。
	pluginMetricsSamplePercent = 10
	// minFeasibleNodesToFind 是每个调度周期至少参与打分的节点数。取值带有经验性，
	// 目的是保证检查足够数量的节点，从而维持最低程度的分散性。
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind 是每个调度周期参与打分的最小节点百分比。
	// 同样为经验值，用于保证检查足够比例的节点，维持最低程度的分散性。
	minFeasibleNodesPercentageToFind = 5
)

// ScheduleOne 对单个调度实体（Pod 或 PodGroup）执行完整的调度流程。
// 各实体在主机选择（host fitting）算法上是串行执行的。
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	logger := klog.FromContext(ctx)
	entity, err := sched.NextEntity(logger)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Error while retrieving next scheduling entity from scheduling queue")
		return
	}
	// 调度队列关闭时 entity 可能为 nil。
	if entity == nil {
		return
	}

	switch specificEntity := entity.(type) {
	case *framework.QueuedPodGroupInfo:
		sched.scheduleOnePodGroup(ctx, specificEntity)
	case *framework.QueuedPodInfo:
		if specificEntity.Pod == nil {
			return
		}
		sched.scheduleOnePod(ctx, specificEntity)
	default:
		utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected entity", "type", fmt.Sprintf("%T", specificEntity))
	}
}

// scheduleOnePod 对单个 Pod 执行完整的调度流程。
func (sched *Scheduler) scheduleOnePod(ctx context.Context, podInfo *framework.QueuedPodInfo) {
	logger := klog.FromContext(ctx)
	pod := podInfo.Pod
	// TODO(knelasevero): 等上下文日志 GA 后，去掉日志调用里重复的 key。
	// https://github.com/kubernetes/kubernetes/issues/111672
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod))
	ctx = klog.NewContext(ctx, logger)
	logger.V(4).Info("About to try and schedule pod", "pod", klog.KObj(pod))

	schedFramework, err := sched.frameworkForPod(pod)
	if err != nil {
		// 理论上不会发生：调度队列只接收 schedulerName 与某个 profile 匹配的 Pod。
		logger.Error(err, "Error occurred")
		sched.SchedulingQueue.Done(podInfo.Pod.UID)
		return
	}
	if sched.skipPodSchedule(ctx, schedFramework, pod) {
		// 此处不把 Pod 放回队列，但必须清理 in-flight 的 Pod 与事件。
		sched.SchedulingQueue.Done(podInfo.Pod.UID)
		return
	}

	logger.V(3).Info("Attempting to schedule pod", "pod", klog.KObj(pod))

	// 同步为 Pod 寻找合适的节点。
	start := time.Now()
	state := framework.NewCycleState()
	// 出于性能考虑，调度器不会在每个周期都为每次插件执行采集并导出 scheduler_plugin_execution_duration 指标，
	// 而是按 pluginMetricsSamplePercent 随机抽样部分调度周期。下面的判断即用于随机选取周期。
	state.SetRecordPluginMetrics(rand.Intn(100) < pluginMetricsSamplePercent)

	// 初始化空的 podsToActivate，由插件填充；没有插件填充时保持为空。
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, state, schedFramework, podInfo, start, podsToActivate)
	if !status.IsSuccess() {
		sched.FailureHandler(schedulingCycleCtx, schedFramework, assumedPodInfo, status, scheduleResult.nominatingInfo, start)
		return
	}

	// 异步把 Pod 绑定到目标节点：上一步的 assume 已让缓存认为 Pod 已在节点上，因此无需等待绑定完成。
	go sched.runBindingCycle(ctx, state, schedFramework, scheduleResult, assumedPodInfo, start, podsToActivate)
}

// runBindingCycle 执行绑定周期算法。
func (sched *Scheduler) runBindingCycle(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	scheduleResult ScheduleResult,
	assumedPodInfo *framework.QueuedPodInfo,
	start time.Time,
	podsToActivate *framework.PodsToActivate) {
	bindingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	metrics.Goroutines.WithLabelValues(metrics.Binding).Inc()
	defer metrics.Goroutines.WithLabelValues(metrics.Binding).Dec()

	status := sched.bindingCycle(bindingCycleCtx, state, schedFramework, scheduleResult, assumedPodInfo, start, podsToActivate)
	if !status.IsSuccess() {
		sched.handleBindingCycleError(bindingCycleCtx, state, schedFramework, assumedPodInfo, start, scheduleResult, status)
		return
	}
}

var clearNominatedNode = &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: ""}

// schedulingCycle 尝试为单个 Pod 完成调度。
func (sched *Scheduler) schedulingCycle(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
	podsToActivate *framework.PodsToActivate,
) (ScheduleResult, *framework.QueuedPodInfo, *fwk.Status) {
	if err := sched.Cache.UpdateSnapshot(klog.FromContext(ctx), sched.nodeInfoSnapshot); err != nil {
		return ScheduleResult{nominatingInfo: clearNominatedNode}, podInfo, fwk.AsStatus(err)
	}

	scheduleResult, status := sched.schedulingAlgorithm(ctx, state, schedFramework, podInfo, start)
	if !status.IsSuccess() {
		return scheduleResult, podInfo, status
	}

	assumedPodInfo, status := sched.prepareForBindingCycle(ctx, state, schedFramework, podInfo, podsToActivate, scheduleResult)
	if !status.IsSuccess() {
		return ScheduleResult{nominatingInfo: clearNominatedNode}, assumedPodInfo, status
	}

	return scheduleResult, assumedPodInfo, nil
}

// prepareForBindingCycle 把调度算法产生的结果应用到内存中，为绑定周期做准备。
func (sched *Scheduler) prepareForBindingCycle(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	podsToActivate *framework.PodsToActivate,
	scheduleResult ScheduleResult,
) (*framework.QueuedPodInfo, *fwk.Status) {
	assumedPodInfo, status := sched.assumeAndReserve(ctx, state, schedFramework, podInfo, scheduleResult)
	if !status.IsSuccess() {
		return assumedPodInfo, status
	}
	assumedPod := assumedPodInfo.Pod

	// 运行 "permit" 插件。
	pluginsWaitTime, runPermitStatus := schedFramework.RunPermitPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
	if runPermitStatus.IsWait() {
		schedFramework.AddWaitingPod(assumedPod, pluginsWaitTime)
	} else if !runPermitStatus.IsSuccess() {
		// 触发 un-reserve 插件，清理与该 Pod 预留相关的状态。
		err := sched.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}

		if runPermitStatus.IsRejected() {
			fitErr := &framework.FitError{
				NumAllNodes: 1,
				Pod:         podInfo.Pod,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewDefaultNodeToStatus(),
				},
			}
			fitErr.Diagnosis.NodeToStatus.Set(scheduleResult.SuggestedHost, runPermitStatus)
			fitErr.Diagnosis.AddPluginStatus(runPermitStatus)
			return assumedPodInfo, fwk.NewStatus(runPermitStatus.Code()).WithError(fitErr)
		}

		return assumedPodInfo, runPermitStatus
	}

	// 调度周期成功结束后，按需弹出并上移等待中的 Pod。
	if len(podsToActivate.Map) != 0 {
		logger := klog.FromContext(ctx)
		sched.SchedulingQueue.Activate(logger, podsToActivate.Map)
		// 激活后清空记录。
		podsToActivate.Map = make(map[string]*v1.Pod)
	}

	return assumedPodInfo, nil
}

// schedulingAlgorithm 对单个 Pod 执行过滤与打分阶段；Pod 不可调度时还会执行 PostFilter。
func (sched *Scheduler) schedulingAlgorithm(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
) (ScheduleResult, *fwk.Status) {
	defer func() {
		metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))
	}()

	pod := podInfo.Pod

	logger := klog.FromContext(ctx)
	scheduleResult, err := sched.SchedulePod(ctx, schedFramework, state, podInfo)
	if err != nil {
		if err == ErrNoNodesAvailable {
			status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable).WithError(err)
			return ScheduleResult{nominatingInfo: clearNominatedNode}, status
		}

		fitError, ok := err.(*framework.FitError)
		if !ok {
			logger.Error(err, "Error selecting node for pod", "pod", klog.KObj(pod))
			return ScheduleResult{nominatingInfo: clearNominatedNode}, fwk.AsStatus(err)
		}

		// PodGroup 调度周期中，不为其中单个 Pod 运行 PostFilter；
		// 而是在整个 PodGroup 周期结束时运行 PodGroupPostFilter。
		if state.IsPodGroupSchedulingCycle() {
			// NominatingInfo 返回 nil，后续可能被 PodGroupPostFilter 覆盖；
			// 是否最终清空由 submitPodGroupAlgorithmResult 决定。
			return ScheduleResult{nominatingInfo: nil}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
		}

		// SchedulePod() 失败可能是因为没有任何节点能容纳该 Pod，因此尝试抢占，
		// 期望下次调度时资源已腾出。即便被抢占的资源被其他 Pod 抢先占用也无害。

		if !schedFramework.HasPostFilterPlugins() {
			logger.V(3).Info("No PostFilter plugins are registered, so no preemption will be performed")
			return ScheduleResult{nominatingInfo: clearNominatedNode}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
		}

		// 运行 PostFilter 插件，尝试让 Pod 在未来的调度周期中可调度。
		result, status := schedFramework.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatus)
		msg := status.Message()
		fitError.Diagnosis.PostFilterMsg = msg
		if status.Code() == fwk.Error {
			utilruntime.HandleErrorWithContext(ctx, nil, "Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		} else {
			logger.V(5).Info("Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		}

		var nominatingInfo *fwk.NominatingInfo
		if result != nil {
			nominatingInfo = result.NominatingInfo
		}
		return ScheduleResult{nominatingInfo: nominatingInfo}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
	}
	return scheduleResult, nil
}

// assumeAndReserve 在调度器内存中 assume 并 reserve 该 Pod。
func (sched *Scheduler) assumeAndReserve(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	scheduleResult ScheduleResult,
) (*framework.QueuedPodInfo, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// 告知缓存：Pod 虽尚未绑定，但假定其已在目标节点上运行。
	// 这样调度器无需等待绑定完成即可继续调度后续 Pod。
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod
	// assume 会给 assumedPod 设置 NodeName=scheduleResult.SuggestedHost。
	err := sched.assume(logger, state, assumedPodInfo, scheduleResult.SuggestedHost)
	if err != nil {
		// 这很可能是重试逻辑的 BUG。此处报错以便重试调度，
		// 依赖 Error 状态会检查 Pod 是否已绑定：若已绑定则不会重新入队，
		// 否则会造成无限循环。
		return assumedPodInfo, fwk.AsStatus(err)
	}

	// 运行 reserve 插件的 Reserve 方法。
	if sts := schedFramework.RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		// 触发 un-reserve，清理与该 Pod 预留相关的状态。
		err := sched.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}

		if sts.IsRejected() {
			fitErr := &framework.FitError{
				NumAllNodes: 1,
				Pod:         podInfo.Pod,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewDefaultNodeToStatus(),
				},
			}
			fitErr.Diagnosis.NodeToStatus.Set(scheduleResult.SuggestedHost, sts)
			fitErr.Diagnosis.AddPluginStatus(sts)
			return assumedPodInfo, fwk.NewStatus(sts.Code()).WithError(fitErr)
		}
		return assumedPodInfo, sts
	}
	return assumedPodInfo, nil
}

// unreserveAndForget 在调度器内存中取消预留并遗忘该 Pod。
// IsPodGroupSchedulingCycle 为 true 的状态下不应在绑定周期调用此函数；
// 不过带此状态的 Pod 也到不了绑定阶段。
func (sched *Scheduler) unreserveAndForget(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	assumedPodInfo *framework.QueuedPodInfo,
	nodeName string,
) error {
	logger := klog.FromContext(ctx)

	schedFramework.RunReservePluginsUnreserve(ctx, state, assumedPodInfo.Pod, nodeName)
	if state.IsPodGroupSchedulingCycle() {
		err := sched.nodeInfoSnapshot.ForgetPod(logger, assumedPodInfo.Pod)
		if err != nil {
			return err
		}
		if assumedPodInfo.Pod.Status.NominatedNodeName != "" {
			// Assume 会清除 nomination；由于此处要回滚 PodGroup 的该阶段， nomination 也需要恢复。
			if sched.SchedulingQueue != nil {
				nominatingInfo := &fwk.NominatingInfo{
					NominatedNodeName: assumedPodInfo.Pod.Status.NominatedNodeName,
					NominatingMode:    fwk.ModeOverride,
				}
				// 这里可以用 AssumedPodInfo：nominator 并不保存完整的 Pod 对象。
				sched.SchedulingQueue.AddNominatedPod(logger, assumedPodInfo.PodInfo, nominatingInfo)
			}
		}
		return nil
	}
	return sched.Cache.ForgetPod(logger, assumedPodInfo.Pod)
}

// bindingCycle 尝试绑定一个已 assume 的 Pod。
func (sched *Scheduler) bindingCycle(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	scheduleResult ScheduleResult,
	assumedPodInfo *framework.QueuedPodInfo,
	start time.Time,
	podsToActivate *framework.PodsToActivate) *fwk.Status {
	logger := klog.FromContext(ctx)

	assumedPod := assumedPodInfo.Pod

	var preFlightStatus *fwk.Status
	if sched.nominatedNodeNameForExpectationEnabled {
		preFlightStatus = schedFramework.RunPreBindPreFlights(ctx, state, assumedPod, scheduleResult.SuggestedHost)
		if preFlightStatus.Code() == fwk.Error ||
			// PreBindPreFlight 不支持 Unschedulable 状态，因此一律按错误处理。
			preFlightStatus.IsRejected() {
			return preFlightStatus
		}
		if preFlightStatus.IsSuccess() || schedFramework.WillWaitOnPermit(ctx, assumedPod) {
			// 设置 NominatedNodeName，告知外部组件（如 cluster autoscaler）Pod 即将绑定到该节点。
			// 仅在 WaitOnPermit 或 PreBind 会执行时设置，否则 Pod 很快就会绑定，无需通知。
			if err := updatePod(ctx, sched.client, schedFramework.APICacher(), assumedPod, nil, &fwk.NominatingInfo{
				NominatedNodeName: scheduleResult.SuggestedHost,
				NominatingMode:    fwk.ModeOverride,
			}); err != nil {
				logger.Error(err, "Failed to update the nominated node name in the binding cycle", "pod", klog.KObj(assumedPod), "nominatedNodeName", scheduleResult.SuggestedHost)
				// 该错误不影响大局，继续执行绑定流程。
			}
		}
	}

	// 运行 "permit" 插件。
	if status := schedFramework.WaitOnPermit(ctx, assumedPod); !status.IsSuccess() {
		if status.IsRejected() {
			fitErr := &framework.FitError{
				NumAllNodes: 1,
				Pod:         assumedPodInfo.Pod,
				Diagnosis: framework.Diagnosis{
					NodeToStatus:         framework.NewDefaultNodeToStatus(),
					UnschedulablePlugins: sets.New(status.Plugin()),
				},
			}
			fitErr.Diagnosis.NodeToStatus.Set(scheduleResult.SuggestedHost, status)
			return fwk.NewStatus(status.Code()).WithError(fitErr)
		}
		return status
	}

	// 此后的任何失败都不会再把 Pod 判定为不可调度：只有在特定扩展点被拒绝才算不可调度，
	// 而 Permit 是调度/绑定周期中最后一个这样的扩展点。PreBind 或 Bind 失败时应移入 BackoffQ 重试。
	//
	// 此处可以调用 Done()，尽早释放调度队列中缓存的集群事件——繁忙集群中这对内存占用很有意义。
	sched.SchedulingQueue.Done(assumedPod.UID)

	// 若将运行 PreBind 插件，先把 Pod 放入 binding map，以便优化抢占判断。
	if preFlightStatus.IsSuccess() {
		var podInPreBindCancel context.CancelCauseFunc
		ctx, podInPreBindCancel = context.WithCancelCause(ctx)
		defer podInPreBindCancel(nil)
		defer schedFramework.RemovePodInPreBind(assumedPod.UID)
		schedFramework.AddPodInPreBind(assumedPod.UID, podInPreBindCancel)
	}
	// 运行 "prebind" 插件。
	if status := schedFramework.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost); !status.IsSuccess() {
		return status
	}

	// 确认 Pod 在 PreBind 期间未被抢占。
	bindingPod := schedFramework.GetPodInPreBind(assumedPod.UID)
	if bindingPod != nil && !bindingPod.MarkPrebound() {
		err := context.Cause(ctx)
		return fwk.AsStatus(err)
	}

	// 运行 "bind" 插件。
	if status := sched.bind(ctx, schedFramework, assumedPod, scheduleResult.SuggestedHost, state); !status.IsSuccess() {
		return status
	}

	// 计算 nodeResourceString 开销较大，日志级别低于 2 时跳过。
	logger.V(2).Info("Successfully bound pod to node", "pod", klog.KObj(assumedPod), "node", scheduleResult.SuggestedHost, "evaluatedNodes", scheduleResult.EvaluatedNodes, "feasibleNodes", scheduleResult.FeasibleNodes)
	metrics.PodScheduled(schedFramework.ProfileName(), metrics.SinceInSeconds(start))
	metrics.PodSchedulingAttempts.Observe(float64(assumedPodInfo.Attempts))
	if assumedPodInfo.InitialAttemptTimestamp != nil {
		metrics.PodSchedulingSLIDuration.WithLabelValues(getAttemptsLabel(assumedPodInfo)).Observe(metrics.SinceInSeconds(*assumedPodInfo.InitialAttemptTimestamp))
	}
	// 统计从 unschedulableEntities 冲刷后成功调度的 Pod 数。
	if assumedPodInfo.WasFlushedFromUnschedulable {
		logger.V(4).Info("Pod scheduled after flush from unschedulableEntities", "pod", klog.KObj(assumedPodInfo.Pod), "unschedulablePlugins", assumedPodInfo.UnschedulablePlugins, "pendingPlugins", assumedPodInfo.PendingPlugins)
		metrics.PodScheduledAfterFlush.Inc()
	}
	// 运行 "postbind" 插件。
	schedFramework.RunPostBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)

	// 绑定周期成功结束后，按需上移等待中的 Pod。
	if len(podsToActivate.Map) != 0 {
		sched.SchedulingQueue.Activate(logger, podsToActivate.Map)
		// 与 schedulingCycle() 不同，这里无需删除记录，因为 podsToActivate.Map 不再被消费。
	}

	return nil
}

func (sched *Scheduler) handleBindingCycleError(
	ctx context.Context,
	state fwk.CycleState,
	fwk framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
	scheduleResult ScheduleResult,
	status *fwk.Status) {
	logger := klog.FromContext(ctx)

	assumedPod := podInfo.Pod
	// 触发 un-reserve 插件，清理与该 Pod 预留相关的状态。
	if forgetErr := sched.unreserveAndForget(ctx, state, fwk, podInfo, scheduleResult.SuggestedHost); forgetErr != nil {
		utilruntime.HandleErrorWithContext(ctx, forgetErr, "ForgetPod failed")
	} else {
		// 绑定周期中 Forget 已 assume 的 Pod 应视为 PodDelete 事件：该 Pod 此前在调度缓存中占用了资源。
		//
		// 不要移动这个 assumed Pod 本身，它始终是 Unschedulable 的。
		// 这里刻意延迟执行：否则 MoveAllToActiveOrBackoffQueue() 会把该事件加入 in-flight 事件，
		// 在插件没有合适 QueueingHint 时仍会把 assumed Pod 移入 backoffQ。
		if status.IsRejected() {
			defer sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, assumedPod, nil, func(pod *v1.Pod) bool {
				return assumedPod.UID != pod.UID
			})
		} else {
			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, assumedPod, nil, nil)
		}
	}

	sched.FailureHandler(ctx, fwk, podInfo, status, clearNominatedNode, start)
}

func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	fwk, ok := sched.Profiles[pod.Spec.SchedulerName]
	if !ok {
		return nil, fmt.Errorf("profile not found for scheduler name %q", pod.Spec.SchedulerName)
	}
	return fwk, nil
}

// skipPodSchedule 判断在特定情形下是否应跳过该 Pod 的调度。
func (sched *Scheduler) skipPodSchedule(ctx context.Context, fwk framework.Framework, pod *v1.Pod) bool {
	// 情形 1：Pod 正在被删除。
	if pod.DeletionTimestamp != nil {
		logger := klog.FromContext(ctx)
		fwk.EventRecorder().WithLogger(logger).Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", "skip schedule deleting pod: %v/%v", pod.Namespace, pod.Name)
		logger.V(3).Info("Skip schedule deleting pod", "pod", klog.KObj(pod))
		return true
	}

	// 情形 2：已 assume 的 Pod 可跳过。
	// 若 Pod 在上一调度周期中、被 assume 之前收到更新事件，可能会被重新加入调度队列。
	isAssumed, err := sched.Cache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to check whether pod is assumed", "pod", klog.KObj(pod))
		return false
	}
	return isAssumed
}

// schedulePod 尝试把给定 Pod 调度到节点列表中的某个节点。
// 成功时返回节点名，失败时返回带原因的 FitError。
func (sched *Scheduler) schedulePod(ctx context.Context, fwk framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) (result ScheduleResult, err error) {
	pod := podInfo.Pod
	trace := utiltrace.New("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	defer trace.LogIfLong(100 * time.Millisecond)

	if sched.nodeInfoSnapshot.NumNodesInPlacement() == 0 {
		return result, ErrNoNodesAvailable
	}

	feasibleNodes, diagnosis, nodeHint, err := sched.findNodesThatFitPod(ctx, fwk, state, podInfo)
	if err != nil {
		return result, err
	}
	trace.Step("Computing predicates done")

	if len(feasibleNodes) == 0 {
		return result, &framework.FitError{
			Pod:         pod,
			NumAllNodes: sched.nodeInfoSnapshot.NumNodesInPlacement(),
			Diagnosis:   diagnosis,
		}
	}

	// 过滤后只剩一个节点时直接使用它。
	if len(feasibleNodes) == 1 {
		node := feasibleNodes[0].Node().Name
		if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
			fwk.StoreScheduleResults(ctx, podInfo.PodSignature, nodeHint, node, nil, sched.CurrentCycle())
		}
		return ScheduleResult{
			SuggestedHost:  node,
			EvaluatedNodes: 1 + diagnosis.NodeToStatus.Len(),
			FeasibleNodes:  1,
		}, nil
	}

	priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, state, pod, feasibleNodes)
	if err != nil {
		return result, err
	}

	sortedPrioritizedNodes := framework.NewSortedScoredNodes(priorityList)
	node := sortedPrioritizedNodes.Pop().Name
	trace.Step("Prioritizing done")

	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		fwk.StoreScheduleResults(ctx, podInfo.PodSignature, nodeHint, node, sortedPrioritizedNodes, sched.CurrentCycle())
	}

	return ScheduleResult{
		SuggestedHost:  node,
		EvaluatedNodes: len(feasibleNodes) + diagnosis.NodeToStatus.Len(),
		FeasibleNodes:  len(feasibleNodes),
	}, err
}

// findNodesThatFitPod 根据框架过滤插件与 filter extender 过滤节点，找出能容纳 Pod 的节点。
func (sched *Scheduler) findNodesThatFitPod(ctx context.Context, schedFramework framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) ([]fwk.NodeInfo, framework.Diagnosis, string, error) {
	logger := klog.FromContext(ctx)
	diagnosis := framework.Diagnosis{
		NodeToStatus: framework.NewDefaultNodeToStatus(),
	}
	allNodes, err := sched.nodeInfoSnapshot.ListNodesInPlacement()
	if err != nil {
		return nil, diagnosis, "", err
	}
	// 运行 "prefilter" 插件。
	pod := podInfo.Pod
	preRes, s, unscheduledPlugins := schedFramework.RunPreFilterPlugins(ctx, state, pod)
	diagnosis.UnschedulablePlugins = unscheduledPlugins
	if !s.IsSuccess() {
		if !s.IsRejected() {
			return nil, diagnosis, "", s.AsError()
		}
		// NodeToStatus 中所有节点共用同一状态，便于抢占时统一处理。
		diagnosis.NodeToStatus.SetAbsentNodesStatus(s)

		// 把 PreFilter 的消息记录到 Diagnosis.PreFilterMsg。
		msg := s.Message()
		diagnosis.PreFilterMsg = msg
		logger.V(5).Info("Status after running PreFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		diagnosis.AddPluginStatus(s)
		return nil, diagnosis, "", nil
	}

	var nodeHint string
	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		// 为简化逻辑，即使已有 nominated 节点也会获取 node hint；未来可考虑跳过。
		nodeHint = schedFramework.GetNodeHint(ctx, pod, podInfo.PodSignature, state, sched.CurrentCycle())
	}

	// NominatedNodeName 可能是上一周期抢占的结果，该节点很可能是唯一能容纳 Pod 的候选，
	// 因此在遍历全部节点前优先尝试它。批处理模块给出的 hint 节点也采用同样策略。
	if len(pod.Status.NominatedNodeName) > 0 || len(nodeHint) > 0 {
		feasibleNodes, err := sched.evaluateNominatedNode(ctx, pod, schedFramework, state, nodeHint, diagnosis)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Evaluation failed on nominated node", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		}
		// nominated 节点通过了全部过滤器，可以直接分配给该 Pod。
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nodeHint, nil
		}
	}

	nodes := allNodes
	if !preRes.AllNodes() {
		nodes = make([]fwk.NodeInfo, 0, len(preRes.NodeNames))
		for nodeName := range preRes.NodeNames {
			// PreRes 可能返回不存在的节点名，需确认节点确实存在于所选放置的快照中。
			if nodeInfo, err := sched.nodeInfoSnapshot.GetNodeInPlacement(nodeName); err == nil {
				nodes = append(nodes, nodeInfo)
			}
		}
		diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(unscheduledPlugins))))
	}
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, schedFramework, state, pod, &diagnosis, nodes)
	// 无论是否出错都要更新 nextStartNodeIndex，保证所有节点都有机会被搜索到。
	processedNodes := len(feasibleNodes) + diagnosis.NodeToStatus.Len()
	sched.nextStartNodeIndex = (sched.nextStartNodeIndex + processedNodes) % len(allNodes)
	if err != nil {
		return nil, diagnosis, nodeHint, err
	}

	feasibleNodesAfterExtender, err := findNodesThatPassExtenders(ctx, sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatus)
	if err != nil {
		return nil, diagnosis, nodeHint, err
	}
	if len(feasibleNodesAfterExtender) != len(feasibleNodes) {
		// Extender 过滤掉了一些节点。
		//
		// Extender 不支持框架中 EnqueueExtensions 那样的重入队机制。
		// 当 Extender 拒绝某些节点导致 Pod 不可调度时，把 fwk.ExtenderName 记入 pInfo.UnschedulablePlugins，
		// 这样任何集群事件都会把该 Pod 从不可调度池重新移入 activeQ/backoffQ。
		// https://github.com/kubernetes/kubernetes/issues/122019
		if diagnosis.UnschedulablePlugins == nil {
			diagnosis.UnschedulablePlugins = sets.New[string]()
		}
		diagnosis.UnschedulablePlugins.Insert(framework.ExtenderName)
	}

	return feasibleNodesAfterExtender, diagnosis, nodeHint, nil
}

func (sched *Scheduler) evaluateNominatedNode(ctx context.Context, pod *v1.Pod, schedFramework framework.Framework, state fwk.CycleState, nodeHint string, diagnosis framework.Diagnosis) ([]fwk.NodeInfo, error) {
	// 未来可考虑在 nominated 节点失败时利用 node hint。
	// https://github.com/kubernetes/kubernetes/issues/135163
	nnn := pod.Status.NominatedNodeName
	if len(nnn) == 0 {
		nnn = nodeHint
	}

	nodeInfo, err := sched.nodeInfoSnapshot.GetNodeInPlacement(nnn)
	if err != nil {
		if _, err := sched.nodeInfoSnapshot.Get(nnn); err != nil {
			return nil, err
		}
		// NNN 节点存在于集群但不在当前放置中，不算错误。
		// PodGroup 放置调度周期会模拟多种潜在放置，此时就可能出现这种情况。
		logger := klog.FromContext(ctx)
		logger.V(4).Info("Pod's nominated node is present in the cluster but not available in the current placement", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		return nil, nil
	}
	node := []fwk.NodeInfo{nodeInfo}
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, schedFramework, state, pod, &diagnosis, node)
	if err != nil {
		return nil, err
	}

	feasibleNodes, err = findNodesThatPassExtenders(ctx, sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatus)
	if err != nil {
		return nil, err
	}

	return feasibleNodes, nil
}

// hasScoring 检查是否配置了节点打分（Score 插件或打分 extender）。
func (sched *Scheduler) hasScoring(fwk framework.Framework) bool {
	if fwk.HasScorePlugins() {
		return true
	}
	for _, extender := range sched.Extenders {
		if extender.IsPrioritizer() {
			return true
		}
	}
	return false
}

// hasExtenderFilters 检查是否存在会过滤节点的 extender。
func (sched *Scheduler) hasExtenderFilters() bool {
	for _, extender := range sched.Extenders {
		if extender.IsFilter() {
			return true
		}
	}
	return false
}

// findNodesThatPassFilters 找出通过所有 Filter 插件的节点。
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	schedFramework framework.Framework,
	state fwk.CycleState,
	pod *v1.Pod,
	diagnosis *framework.Diagnosis,
	nodes []fwk.NodeInfo) ([]fwk.NodeInfo, error) {
	numAllNodes := len(nodes)
	numNodesToFind := sched.numFeasibleNodesToFind(schedFramework.PercentageOfNodesToScore(), int32(numAllNodes))
	if !sched.hasExtenderFilters() && !sched.hasScoring(schedFramework) {
		numNodesToFind = 1
	}

	// 预分配足够容量的可行节点切片，避免扩容并允许直接按下标赋值。
	feasibleNodes := make([]fwk.NodeInfo, numNodesToFind)

	if !schedFramework.HasFilterPlugins() {
		for i := range feasibleNodes {
			feasibleNodes[i] = nodes[(sched.nextStartNodeIndex+i)%numAllNodes]
		}
		return feasibleNodes, nil
	}

	errCh := parallelize.NewResultChannel[error]()
	var feasibleNodesLen int32
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("findNodesThatPassFilters has completed"))

	type nodeStatus struct {
		node   string
		status *fwk.Status
	}
	result := make([]*nodeStatus, numAllNodes)
	checkNode := func(i int) {
		// 从上个调度周期停止的位置开始检查，确保跨 Pod 调度时各节点被检查的机会均等。
		nodeInfo := nodes[(sched.nextStartNodeIndex+i)%numAllNodes]
		status := schedFramework.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if status.Code() == fwk.Error {
			errCh.SendWithCancel(status.AsError(), func() {
				cancel(errors.New("some other Filter operation failed"))
			})
			return
		}
		if status.IsSuccess() {
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			if length > numNodesToFind {
				cancel(errors.New("findNodesThatPassFilters has found enough nodes"))
				atomic.AddInt32(&feasibleNodesLen, -1)
			} else {
				feasibleNodes[length-1] = nodeInfo
			}
		} else {
			result[i] = &nodeStatus{node: nodeInfo.Node().Name, status: status}
		}
	}

	beginCheckNode := time.Now()
	statusCode := fwk.Success
	defer func() {
		// Filter 扩展点的耗时在这里记录而非 framework.go：RunFilterPlugins 是逐节点调用的，
		// 而我们需要的是每个调度周期所有节点的整体耗时。注意该耗时也包含调用 RunPreFilterAddPod 的 addNominatedPods。
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Filter, statusCode.String(), schedFramework.ProfileName()).Observe(metrics.SinceInSeconds(beginCheckNode))
	}()

	// 找到配置数量的可行节点后即停止搜索。
	schedFramework.Parallelizer().Until(ctx, numAllNodes, checkNode, metrics.Filter)
	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	for _, item := range result {
		if item == nil {
			continue
		}
		diagnosis.NodeToStatus.Set(item.node, item.status)
		diagnosis.AddPluginStatus(item.status)
	}
	if err := errCh.Receive(); err != nil {
		statusCode = fwk.Error
		return feasibleNodes, err
	}
	return feasibleNodes, nil
}

// numFeasibleNodesToFind 返回可行节点的目标数量，找到这么多节点后调度器即停止继续搜索。
func (sched *Scheduler) numFeasibleNodesToFind(percentageOfNodesToScore *int32, numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind {
		return numAllNodes
	}

	// 优先使用 profile 中设置的 percentageOfNodesToScore，否则使用全局配置。
	var percentage int32
	if percentageOfNodesToScore != nil {
		percentage = *percentageOfNodesToScore
	} else {
		percentage = sched.percentageOfNodesToScore
	}

	if percentage == 0 {
		percentage = int32(50) - numAllNodes/125
		if percentage < minFeasibleNodesPercentageToFind {
			percentage = minFeasibleNodesPercentageToFind
		}
	}

	numNodes = numAllNodes * percentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}

func findNodesThatPassExtenders(ctx context.Context, extenders []fwk.Extender, pod *v1.Pod, feasibleNodes []fwk.NodeInfo, statuses *framework.NodeToStatus) ([]fwk.NodeInfo, error) {
	logger := klog.FromContext(ctx)

	// Extender 按顺序调用：每个 extender 都可能剔除一部分节点，传给下一个 extender 的节点集合逐步缩小。
	for _, extender := range extenders {
		if len(feasibleNodes) == 0 {
			break
		}
		if !extender.IsInterested(pod) {
			continue
		}

		// failedAndUnresolvableMap 中节点的状态会加入 <statuses>，让调度框架能识别针对特定节点的
		// UnschedulableAndUnresolvable 状态，最终有助于提升抢占效率。
		// 注意：建议把可能返回该状态的 extender 配置在其他 extender 之前。
		feasibleList, failedMap, failedAndUnresolvableMap, err := extender.Filter(pod, feasibleNodes)
		if err != nil {
			if extender.IsIgnorable() {
				logger.Info("Skipping extender as it returned error and has ignorable flag set", "extender", extender, "err", err)
				continue
			}
			return nil, err
		}

		for failedNodeName, failedMsg := range failedAndUnresolvableMap {
			statuses.Set(failedNodeName, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, failedMsg))
		}

		for failedNodeName, failedMsg := range failedMap {
			if _, found := failedAndUnresolvableMap[failedNodeName]; found {
				// failedAndUnresolvableMap 优先于 failedMap；只有在 extender 同时把节点放进两个 map 时才会出现这种情况。
				continue
			}
			statuses.Set(failedNodeName, fwk.NewStatus(fwk.Unschedulable, failedMsg))
		}

		feasibleNodes = feasibleList
	}
	return feasibleNodes, nil
}

// prioritizeNodes 运行 Score 插件为节点打分，各插件分数相加得到节点分数，
// 随后再运行 extender 打分。所有分数最终累加为各节点的总加权分。
func prioritizeNodes(
	ctx context.Context,
	extenders []fwk.Extender,
	schedFramework framework.Framework,
	state fwk.CycleState,
	pod *v1.Pod,
	nodes []fwk.NodeInfo,
) ([]fwk.NodePluginScores, error) {
	logger := klog.FromContext(ctx)
	// 没有配置任何优先级时，所有节点得分均为 1，以便按要求的格式生成优先级列表。
	if len(extenders) == 0 && !schedFramework.HasScorePlugins() {
		result := make([]fwk.NodePluginScores, 0, len(nodes))
		for i := range nodes {
			result = append(result, fwk.NodePluginScores{
				Name:       nodes[i].Node().Name,
				TotalScore: 1,
			})
		}
		return result, nil
	}

	// 运行 PreScore 插件。
	preScoreStatus := schedFramework.RunPreScorePlugins(ctx, state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	// 运行 Score 插件。
	nodesScores, scoreStatus := schedFramework.RunScorePlugins(ctx, state, pod, nodes)
	if !scoreStatus.IsSuccess() {
		return nil, scoreStatus.AsError()
	}

	// 日志级别为 10 时输出更多细节。
	loggerVTen := logger.V(10)
	if loggerVTen.Enabled() {
		for _, nodeScore := range nodesScores {
			for _, pluginScore := range nodeScore.Scores {
				loggerVTen.Info("Plugin scored node for pod", "pod", klog.KObj(pod), "plugin", pluginScore.Name, "node", nodeScore.Name, "score", pluginScore.Score)
			}
		}
	}

	if len(extenders) != 0 && nodes != nil {
		// allNodeExtendersScores 保存所有节点在 extender 处的得分，以节点名为键。
		allNodeExtendersScores := make(map[string]*fwk.NodePluginScores, len(nodes))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := range extenders {
			if !extenders[i].IsInterested(pod) {
				continue
			}
			wg.Add(1)
			go func(extIndex int) {
				metrics.Goroutines.WithLabelValues(metrics.PrioritizingExtender).Inc()
				defer func() {
					metrics.Goroutines.WithLabelValues(metrics.PrioritizingExtender).Dec()
					wg.Done()
				}()
				prioritizedList, weight, err := extenders[extIndex].Prioritize(pod, nodes)
				if err != nil {
					// extender 的打分错误可忽略，交由 Kubernetes 或其他 extender 决定优先级。
					logger.V(5).Info("Failed to run extender's priority function. No score given by this extender.", "error", err, "pod", klog.KObj(pod), "extender", extenders[extIndex].Name())
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for i := range *prioritizedList {
					nodename := (*prioritizedList)[i].Host
					score := (*prioritizedList)[i].Score
					if loggerVTen.Enabled() {
						loggerVTen.Info("Extender scored node for pod", "pod", klog.KObj(pod), "extender", extenders[extIndex].Name(), "node", nodename, "score", score)
					}

					// extender 的最大优先级可能与调度器使用的 MaxNodeScore 不一致，
					// 因此需要把 extender 返回的分数换算到调度器的分数区间。
					finalscore := score * weight * (fwk.MaxNodeScore / extenderv1.MaxExtenderPriority)

					if allNodeExtendersScores[nodename] == nil {
						allNodeExtendersScores[nodename] = &fwk.NodePluginScores{
							Name:   nodename,
							Scores: make([]fwk.PluginScore, 0, len(extenders)),
						}
					}
					allNodeExtendersScores[nodename].Scores = append(allNodeExtendersScores[nodename].Scores, fwk.PluginScore{
						Name:  extenders[extIndex].Name(),
						Score: finalscore,
					})
					allNodeExtendersScores[nodename].TotalScore += finalscore
				}
			}(i)
		}
		// 等待所有 goroutine 结束。
		wg.Wait()
		for i := range nodesScores {
			if score, ok := allNodeExtendersScores[nodes[i].Node().Name]; ok {
				nodesScores[i].Scores = append(nodesScores[i].Scores, score.Scores...)
				nodesScores[i].TotalScore += score.TotalScore
				nodesScores[i].Randomizer = rand.Int()
			}
		}
	}

	if loggerVTen.Enabled() {
		for i := range nodesScores {
			loggerVTen.Info("Calculated node's final score for pod", "pod", klog.KObj(pod), "node", nodesScores[i].Name, "score", nodesScores[i].TotalScore)
		}
	}
	return nodesScores, nil
}

// assume 告知缓存 Pod 已被纳入，从而允许异步绑定。
// 在 PodGroup 调度周期中调用时，assume 作用于快照而非缓存。
func (sched *Scheduler) assume(logger klog.Logger, state fwk.CycleState, assumedPodInfo *framework.QueuedPodInfo, host string) error {
	// 乐观地假定绑定会成功，并在后台发送给 apiserver。
	// 若绑定失败，调度器会立即释放为 assumed Pod 分配的资源。
	assumedPodInfo.Pod.Spec.NodeName = host
	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) {
		// 开启 DRANodeAllocatableResources 时，把计算出的节点可分配资源 claim 状态从 cycle state
		// 复制到 assumed Pod 的 status，使调度器缓存中的 Pod 能反映 DRA 插件在此周期分配的资源，
		// 供 NodeInfo 缓存更新使用。此前调度失败遗留的同名字段会被覆盖。
		// 该字段无需显式清空：handleSchedulingFailure() 重新入队前会重建 Pod 对象。
		assumedPodInfo.Pod.Status.NodeAllocatableResourceClaimStatuses = dynamicresources.ExtractPodNodeAllocatableResourceClaimStatus(logger, state, host)
	}

	if state.IsPodGroupSchedulingCycle() {
		err := sched.nodeInfoSnapshot.AssumePod(assumedPodInfo.PodInfo)
		if err != nil {
			logger.Error(err, "Scheduler snapshot AssumePod failed")
			return err
		}
	} else {
		if err := sched.Cache.AssumePod(logger, assumedPodInfo.Pod); err != nil {
			logger.Error(err, "Scheduler cache AssumePod failed")
			return err
		}
	}
	// 若 assumed Pod 是 nominated Pod，应将其从内部缓存移除。
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumedPodInfo.Pod)
	}

	return nil
}

// bind 把 Pod 绑定到 binding 对象指定的节点。
// 绑定优先级：(1) extender，(2) 框架插件。
// 该方法异步执行，因此指标在内部处理。
func (sched *Scheduler) bind(ctx context.Context, schedFramework framework.Framework, assumed *v1.Pod, targetNode string, state fwk.CycleState) (status *fwk.Status) {
	logger := klog.FromContext(ctx)
	defer func() {
		sched.finishBinding(logger, schedFramework, assumed, targetNode, status)
	}()

	bound, err := sched.extendersBinding(logger, assumed, targetNode)
	if bound {
		return fwk.AsStatus(err)
	}
	return schedFramework.RunBindPlugins(ctx, state, assumed, targetNode)
}

// TODO(#87159): 把这段逻辑移到插件中。
func (sched *Scheduler) extendersBinding(logger klog.Logger, pod *v1.Pod, node string) (bool, error) {
	for _, extender := range sched.Extenders {
		if !extender.IsBinder() || !extender.IsInterested(pod) {
			continue
		}
		err := extender.Bind(&v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target:     v1.ObjectReference{Kind: "Node", Name: node},
		})
		if err != nil && extender.IsIgnorable() {
			logger.Info("Skipping extender in bind as it returned error and has ignorable flag set", "extender", extender, "err", err)
			continue
		}
		return true, err
	}
	return false, nil
}

func (sched *Scheduler) finishBinding(logger klog.Logger, fwk framework.Framework, assumed *v1.Pod, targetNode string, status *fwk.Status) {
	if !status.IsSuccess() {
		logger.V(1).Info("Failed to bind pod", "pod", klog.KObj(assumed))
		return
	}

	fwk.EventRecorder().WithLogger(logger).Eventf(assumed, nil, v1.EventTypeNormal, "Scheduled", "Binding", "Successfully assigned %v/%v to %v", assumed.Namespace, assumed.Name, targetNode)
}

func getAttemptsLabel(p *framework.QueuedPodInfo) string {
	// 按尝试次数细分 Pod 调度耗时，并对次数封顶，避免指标基数过高。
	if p.Attempts >= 15 {
		return "15+"
	}
	return strconv.Itoa(p.Attempts)
}

// handleSchedulingFailure 为 Pod 记录调度失败事件，并更新 Pod condition 与 nominated node name（若有）。
func (sched *Scheduler) handleSchedulingFailure(ctx context.Context, podFwk framework.Framework, podInfo *framework.QueuedPodInfo, status *fwk.Status, nominatingInfo *fwk.NominatingInfo, start time.Time) {
	podInfo = podInfo.DeepCopy()
	calledDone := false
	defer func() {
		if !calledDone {
			// AddUnschedulablePodIfNotPresent 内部通常会调用 DonePod，
			// 但某些边界场景不会调用它，因此这里显式调用，避免 Pod 泄漏。
			sched.SchedulingQueue.Done(podInfo.Pod.UID)
		}
	}()

	logger := klog.FromContext(ctx)
	reason := v1.PodReasonSchedulerError
	if status.IsRejected() {
		reason = v1.PodReasonUnschedulable
	}

	switch reason {
	case v1.PodReasonUnschedulable:
		metrics.PodUnschedulable(podFwk.ProfileName(), metrics.SinceInSeconds(start))
	case v1.PodReasonSchedulerError:
		metrics.PodScheduleError(podFwk.ProfileName(), metrics.SinceInSeconds(start))
	}

	pod := podInfo.Pod
	nominatedPodInfo := podInfo.PodInfo
	isDeferredResize := sched.inPlacePodVerticalScalingSchedulerPreemptionEnabled && compresource.IsPodResizeDeferred(pod)
	err := status.AsError()
	errMsg := status.Message()

	// 清空插件相关字段，避免残留上一次调度尝试的旧数据。FitError 场景下稍后会重新填充。
	// 在这里清空而非在 Pop 时清空，是因为 Pod 调度成功时（如从不可调度池冲刷后）
	// 有时仍需要这些字段用于日志。
	podInfo.ClearRejectorPlugins()

	if err == ErrNoNodesAvailable {
		logger.V(2).Info("Unable to schedule pod; no nodes are registered to the cluster; waiting", "pod", klog.KObj(pod))
	} else if fitError, ok := err.(*framework.FitError); ok { // 把 UnschedulablePlugins 写入 PodInfo，后续可据此高效地在各队列间移动 Pod。
		podInfo.UnschedulablePlugins = fitError.Diagnosis.UnschedulablePlugins
		podInfo.PendingPlugins = fitError.Diagnosis.PendingPlugins
		logger.V(2).Info("Unable to schedule pod; no fit; waiting", "pod", klog.KObj(pod), "err", errMsg)
	} else if errors.Is(err, errPodGroupUnschedulable) {
		logger.V(2).Info("Unable to schedule pod belonging to a pod group; waiting", "pod", klog.KObj(pod), "err", errMsg)
	} else {
		utilruntime.HandleErrorWithContext(ctx, err, "Error scheduling pod; retrying", "pod", klog.KObj(pod))
	}

	// 检查 Pod 是否存在于 informer 缓存中。
	podLister := podFwk.SharedInformerFactory().Core().V1().Pods().Lister()
	cachedPod, e := podLister.Pods(pod.Namespace).Get(pod.Name)
	if e != nil {
		logger.Info("Pod doesn't exist in informer cache", "pod", klog.KObj(pod), "err", e)
		// 此分支不会调用 AddUnschedulablePodIfNotPresent，因此必须显式调用 DonePod。
	} else {
		// extender 场景下 Pod 可能已绑定成功，只是响应超时；此时线上版本的 .spec.nodeName
		// 会与队列内部版本不一致。对延迟扩容的 Pod 而言，缓存中已分配节点是预期行为，不属于 extender 绑定超时导致的不一致。
		if len(cachedPod.Spec.NodeName) != 0 && !isDeferredResize {
			logger.Info("Pod has been assigned to node. Abort adding it back to queue.", "pod", klog.KObj(pod), "node", cachedPod.Spec.NodeName)
			// 此分支不会调用 AddUnschedulablePodIfNotPresent，因此必须显式调用 DonePod。
		} else {
			if cachedPod.UID != podInfo.Pod.UID {
				logger.V(2).Info("Pod was recreated while handling scheduling failure. Skip requeueing and status updates.", "pod", klog.KObj(pod), "oldUID", podInfo.Pod.UID, "newUID", cachedPod.UID)
				return
			}
			// cachedPod 来自 SharedInformer，必须 DeepCopy 后再修改。
			// 忽略该错误：apiserver 未严格校验亲和性项，且出于兼容性无法修正校验逻辑。
			podInfo.PodInfo, _ = framework.NewPodInfo(cachedPod.DeepCopy())
			pod = podInfo.Pod
			nominatedPodInfo = podInfo.PodInfo
			if err := sched.SchedulingQueue.AddUnschedulablePodIfNotPresent(logger, podInfo, sched.SchedulingQueue.SchedulingCycle()); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Error occurred")
			}
			calledDone = true
		}
	}

	// 延迟扩容的 Pod 已绑定并在其节点上运行，调度器只评估扩容可行性并执行抢占，
	// 实际扩容由 kubelet 原地执行。因此提前返回，避免执行绑定或设置 nominated node 状态。
	if isDeferredResize {
		return
	}

	// 用 nominated Pod 信息更新调度队列。否则下一调度周期与调度器收到该 Pod 更新事件之间
	// 会存在竞态条件。这里判空仅是为了兼容测试。
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.AddNominatedPod(logger, nominatedPodInfo, nominatingInfo)
	}

	if err == nil {
		// 只有测试能走到这里。
		return
	}

	msg := truncateMessage(errMsg)
	podFwk.EventRecorder().WithLogger(logger).Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", msg)
	if err := updatePod(ctx, sched.client, podFwk.APICacher(), pod, &v1.PodCondition{
		Type:               v1.PodScheduled,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(&pod.Status, pod.Generation, v1.PodScheduled),
		Status:             v1.ConditionFalse,
		Reason:             reason,
		Message:            errMsg,
	}, nominatingInfo); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error updating pod", "pod", klog.KObj(pod))
	}
}

// truncateMessage 在消息超过 NoteLengthLimit 时进行截断。
func truncateMessage(message string) string {
	max := validation.NoteLengthLimit
	if len(message) <= max {
		return message
	}
	suffix := " ..."
	return message[:max-len(suffix)] + suffix
}

func updatePod(ctx context.Context, client clientset.Interface, apiCacher fwk.APICacher, pod *v1.Pod, condition *v1.PodCondition, nominatingInfo *fwk.NominatingInfo) error {
	if apiCacher != nil {
		// 有 API cacher 时用其 patch Pod status。
		var conditions []*v1.PodCondition
		if condition != nil {
			conditions = []*v1.PodCondition{condition}
		}
		_, err := apiCacher.PatchPodStatus(pod, conditions, nominatingInfo)
		return err
	}
	logger := klog.FromContext(ctx)
	logValues := []any{"pod", klog.KObj(pod)}
	if condition != nil {
		logValues = append(logValues, "conditionType", condition.Type, "conditionStatus", condition.Status, "conditionReason", condition.Reason)
	}
	if nominatingInfo != nil {
		logValues = append(logValues, "nominatedNodeName", nominatingInfo.NominatedNodeName, "nominatingMode", nominatingInfo.Mode())
	}
	logger.V(3).Info("Updating pod condition and nominated node name", logValues...)

	podStatusCopy := pod.Status.DeepCopy()
	// 仅在确实要设置 NominatedNodeName 且新值与旧值不同时才执行更新。
	nnnNeedsUpdate := nominatingInfo.Mode() == fwk.ModeOverride && pod.Status.NominatedNodeName != nominatingInfo.NominatedNodeName
	podConditionNeedsUpdate := condition != nil && podutil.UpdatePodCondition(podStatusCopy, condition)
	if !podConditionNeedsUpdate && !nnnNeedsUpdate {
		return nil
	}
	if nnnNeedsUpdate {
		podStatusCopy.NominatedNodeName = nominatingInfo.NominatedNodeName
	}
	return util.PatchPodStatus(ctx, client, pod.Name, pod.Namespace, &pod.Status, podStatusCopy)
}
