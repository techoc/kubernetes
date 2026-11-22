/*
Copyright The Kubernetes Authors.

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
	"fmt"
	"iter"
	"maps"
	"math/rand"
	"time"

	v1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	schedulingapi "k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

// revertFns 聚合了用于撤销 PodGroup 调度算法模拟期间内存改动（如 Assume Pod、调用 Reserve 插件）
// 的函数。执行时机：
//   - 整个根节点在调度算法中处理完毕后，清理模拟状态；
//   - (复合) PodGroup 算法失败时，立即回滚已做的部分修改；
//   - 基于 Placement 的调度算法中每评估完一个候选 Placement 后，重置状态以便评估下一个。
type revertFns []func()

// append 追加更多的回滚函数。
func (r *revertFns) append(other revertFns) {
	*r = append(*r, other...)
}

// revert 按注册的逆序（LIFO）执行回滚函数。后序操作可能依赖先序操作的副作用，
// 逆序回滚才能正确逐层撤销、保持状态一致，类似 Go 中 defer 的行为。
func (r *revertFns) revert() {
	if r == nil {
		return
	}
	for i := len(*r) - 1; i >= 0; i-- {
		(*r)[i]()
		(*r)[i] = nil // 置 nil 便于 GC 回收。
	}
	*r = nil
}

// errPodGroupUnschedulable 表示 PodGroup 不可调度。
var errPodGroupUnschedulable = fmt.Errorf("pod group is unschedulable")

// scheduleOnePodGroup 为单个 PodGroup 执行完整的工作负载感知调度流程。
func (sched *Scheduler) scheduleOnePodGroup(ctx context.Context, podGroupInfo *framework.QueuedPodGroupInfo) {
	logger := klog.FromContext(ctx)
	// TODO(knelasevero): 等上下文日志 GA 后，去掉日志调用中重复的键。
	// https://github.com/kubernetes/kubernetes/issues/111672

	logger = klog.LoggerWithValues(logger, "podGroupType", podGroupInfo.GetType(), "podGroup", klog.KObj(podGroupInfo))
	ctx = klog.NewContext(ctx, logger)
	start := time.Now()

	if err := sched.Cache.UpdateSnapshot(logger, sched.nodeInfoSnapshot); err != nil {
		logger.Error(err, "Error updating snapshot")
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}

	// 从队列弹出的 PodGroupInfo 可能持有旧版 (Composite)PodGroup 对象，
	// 这里用快照版本覆盖，保证整个调度周期内的对象一致。
	if err := sched.reconcilePodGroupWithSnapshot(podGroupInfo.PodGroupInfo); err != nil {
		// 层级结构可能在感知到形态变化前就被弹出队列，此时需要把 (Composite)PodGroup 放回队列。
		// 将底层 API 对象置为 nil，表示不希望在失败处理器中更新它的条件。

		podGroupInfo.PodGroup = nil
		podGroupInfo.CompositePodGroup = nil
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}
	if err := sched.validatePodGroup(podGroupInfo); err != nil {
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}

	schedFwk := sched.frameworkForPodGroup(podGroupInfo)
	sched.skipPodGroupPodSchedule(ctx, schedFwk, podGroupInfo)
	// skipPodGroupPodSchedule 可能移除组中的部分 Pod，约束会在 PlacementFeasible 阶段重新校验，
	// 因此这里要确认组内是否还有剩余 Pod。

	if len(podGroupInfo.QueuedPodInfos) == 0 {
		// 结束当前进行中的调度尝试，使跳过这些 Pod 期间新到达的成员能够重新入队，
		// 而不是永久挂起。
		if err := sched.SchedulingQueue.AddAttemptedPodGroupIfNeeded(logger, podGroupInfo, sched.SchedulingQueue.SchedulingCycle(), fwk.NewStatus(fwk.Success)); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Failed to finish skipped pod group scheduling attempt", "podGroup", klog.KObj(podGroupInfo))
		}
		return
	}

	logger.V(3).Info("Attempting to schedule pod group", "podGroupType", podGroupInfo.GetType(), "podGroup", klog.KObj(podGroupInfo))

	sched.podGroupCycle(ctx, schedFwk, framework.NewCycleState(), podGroupInfo, start)
}

// reconcilePodGroupWithSnapshot 用快照版本覆盖对象，确保整个周期内对象一致。
// 队列弹出的 PodGroupInfo 可能持有较旧的 PodGroup/CompositePodGroup 对象；
// 层级形态若发生变化（子树增删）则直接报错。
func (sched *Scheduler) reconcilePodGroupWithSnapshot(pgi *framework.PodGroupInfo) error {
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) && (pgi.GetType() == fwk.CompositePodGroupKeyType || pgi.CompositePodGroup != nil) {
		compositePodGroup, err := sched.nodeInfoSnapshot.CompositePodGroups().Get(pgi.Namespace, pgi.Name)
		if err != nil {
			return err
		}
		if pgi.CompositePodGroup != nil && !ptr.Equal(compositePodGroup.Spec.ParentCompositePodGroupName, pgi.CompositePodGroup.Spec.ParentCompositePodGroupName) {
			return fmt.Errorf("different parent in composite pod group between snapshot (%s) and queued entity (%s)",
				ptr.Deref(compositePodGroup.Spec.ParentCompositePodGroupName, "[unset]"),
				ptr.Deref(pgi.CompositePodGroup.Spec.ParentCompositePodGroupName, "[unset]"))
		}
		cpgs, err := sched.nodeInfoSnapshot.CompositePodGroupStates().Get(pgi.Namespace, pgi.Name)
		if err != nil {
			return err
		}
		cpgsChildren := cpgs.GetChildren()
		if len(cpgsChildren) != len(pgi.Children) {
			return fmt.Errorf("different number of children in composite pod group between snapshot (%d) and queued entity (%d)", len(cpgsChildren), len(pgi.Children))
		}
		for i := range pgi.Children {
			err := sched.reconcilePodGroupWithSnapshot(pgi.Children[i])
			if err != nil {
				return err
			}
		}
		pgi.CompositePodGroup = compositePodGroup
	} else {
		podGroup, err := sched.nodeInfoSnapshot.PodGroups().Get(pgi.Namespace, pgi.Name)
		if err != nil {
			return err
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) &&
			!ptr.Equal(podGroup.Spec.ParentCompositePodGroupName, pgi.PodGroup.Spec.ParentCompositePodGroupName) {
			return fmt.Errorf("different parent in pod group between snapshot (%s) and queued entity (%s)",
				ptr.Deref(podGroup.Spec.ParentCompositePodGroupName, "[unset]"),
				ptr.Deref(pgi.PodGroup.Spec.ParentCompositePodGroupName, "[unset]"))
		}
		pgi.PodGroup = podGroup
	}
	return nil
}

// handlePodGroupFailureBeforeScheduling 处理调度开始前 (复合) PodGroup 发生的失败。
func (sched *Scheduler) handlePodGroupFailureBeforeScheduling(ctx context.Context, podGroupInfo *framework.QueuedPodGroupInfo, err error) {
	logger := klog.FromContext(ctx)
	podGroupInfo.ForEachPodInfo(func(podInfo *framework.QueuedPodInfo) bool {
		podFwk, podFwkErr := sched.frameworkForPod(podInfo.Pod)
		if podFwkErr != nil {
			// 理论上不会发生：我们只接受指定了与某个 profile 匹配的调度器名的 Pod。

			logger.Error(podFwkErr, "Error occurred")
			sched.SchedulingQueue.Done(podInfo.Pod.UID)
		} else {
			sched.FailureHandler(ctx, podFwk, podInfo, fwk.AsStatus(err), clearNominatedNode, time.Now())
		}
		return true
	})
	sched.updatePodGroupConditionWithError(ctx, podGroupInfo.PodGroupInfo, err)
	err = sched.SchedulingQueue.AddAttemptedPodGroupIfNeeded(logger, podGroupInfo, sched.SchedulingQueue.SchedulingCycle(), fwk.AsStatus(err))
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to add pod group back to scheduling queue", "podGroupType", podGroupInfo.GetType(), "podGroup", klog.KObj(podGroupInfo))
	}
}

func (sched *Scheduler) updatePodGroupConditionWithError(ctx context.Context, pgi *framework.PodGroupInfo, err error) {
	if pgi.PodGroup != nil {
		sched.updatePodGroupCondition(ctx, pgi, &metav1.Condition{
			Type:    schedulingapi.PodGroupInitiallyScheduled,
			Status:  metav1.ConditionFalse,
			Reason:  schedulingapi.PodGroupReasonSchedulerError,
			Message: err.Error(),
		})
		return
	}
	for _, child := range pgi.GetChildGroups() {
		sched.updatePodGroupConditionWithError(ctx, child, err)
	}
}

// validatePodGroup 确保：
// - 组层级中所有 Pod 的调度器名一致；
// - 组层级中所有 Pod 的抢占策略一致；
// - 根组的优先级与所有 Pod 一致；
// - 根组的抢占策略与所有 Pod 一致。
func (sched *Scheduler) validatePodGroup(podGroupInfo *framework.QueuedPodGroupInfo) error {
	schedulerName := ""
	podGroupPriority := podGroupInfo.GetPriority()
	var err error

	var pgPreemptionPolicy v1.PreemptionPolicy
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) {
		pgPreemptionPolicy = podGroupPreemptionPolicy(podGroupInfo)
	}

	validatePod := func(pod *v1.Pod) error {
		if pod.Spec.SchedulerName != schedulerName {
			return fmt.Errorf("all pods in a single pod group should have the same .spec.schedulerName set, got: %q and %q", pod.Spec.SchedulerName, schedulerName)
		}
		podPriority := corev1helpers.PodPriority(pod)
		if podPriority != podGroupPriority {
			return fmt.Errorf("all pods in a single pod group should have the same priority as the pod group's priority, got %d and %d", podPriority, podGroupPriority)
		}

		pPreemptionPolicy := podPreemptionPolicy(pod)
		if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) {
			// 启用 PodGroupPreemptionPolicy 时，校验 Pod 的抢占策略与根组一致。

			if pPreemptionPolicy != pgPreemptionPolicy {
				return fmt.Errorf("all pods in a single pod group should have the same preemption policy as the pod group's preemption policy, got %v and %v", pPreemptionPolicy, pgPreemptionPolicy)
			}
		} else {
			// 禁用该特性时，抢占策略由组内首个 Pod 决定，需校验组内所有 Pod 的策略一致。

			if pgPreemptionPolicy == "" {
				pgPreemptionPolicy = pPreemptionPolicy
			} else if pPreemptionPolicy != pgPreemptionPolicy {
				return fmt.Errorf("all pods in a single pod group should have the same preemption policy, got %v and %v", pPreemptionPolicy, pgPreemptionPolicy)
			}
		}
		return nil
	}
	podGroupInfo.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		if schedulerName == "" {
			schedulerName = pInfo.Pod.Spec.SchedulerName
		}
		err = validatePod(pInfo.Pod)
		return err == nil
	})
	if err != nil {
		return err
	}

	err = sched.validateScheduledPods(podGroupInfo.PodGroupInfo, validatePod)
	if err != nil {
		return err
	}

	if _, ok := sched.Profiles[schedulerName]; !ok {
		return fmt.Errorf("profile not found for scheduler name %q", schedulerName)
	}

	return nil
}

// podGroupPreemptionPolicy 返回 PodGroup 上设置的抢占策略，未设置时返回默认值 PreemptLowerPriority。
func podGroupPreemptionPolicy(podGroupInfo *framework.QueuedPodGroupInfo) v1.PreemptionPolicy {
	if pg := podGroupInfo.PodGroup; pg != nil && pg.Spec.PreemptionPolicy != nil {
		return v1.PreemptionPolicy(*pg.Spec.PreemptionPolicy)
	}
	if cpg := podGroupInfo.CompositePodGroup; cpg != nil && cpg.Spec.PreemptionPolicy != nil {
		return v1.PreemptionPolicy(*cpg.Spec.PreemptionPolicy)
	}
	return v1.PreemptLowerPriority
}

// podPreemptionPolicy 返回 Pod 上设置的抢占策略，未设置时返回默认值 PreemptLowerPriority。
func podPreemptionPolicy(pod *v1.Pod) v1.PreemptionPolicy {
	if pod != nil && pod.Spec.PreemptionPolicy != nil {
		return *pod.Spec.PreemptionPolicy
	}
	return v1.PreemptLowerPriority
}

// validateScheduledPods 校验组中已调度的 Pod 与未调度 Pod 遵守同样的全局约束
// （如调度器名、优先级）。它递归遍历层级，获取并检查每个叶子组的缓存状态。
func (sched *Scheduler) validateScheduledPods(podGroupInfo *framework.PodGroupInfo, validatePod func(pod *v1.Pod) error) error {
	if podGroupInfo.CompositePodGroup != nil {
		for _, child := range podGroupInfo.GetChildGroups() {
			if err := sched.validateScheduledPods(child, validatePod); err != nil {
				return err
			}
		}
		return nil
	}

	podGroupState, err := sched.nodeInfoSnapshot.PodGroupStates().Get(podGroupInfo.Namespace, podGroupInfo.Name)
	if err != nil {
		return fmt.Errorf("failed to get pod group state: %w", err)
	}
	for _, pod := range podGroupState.ScheduledPods() {
		if err := validatePod(pod); err != nil {
			return err
		}
	}
	return nil
}

// frameworkForPodGroup 获取整个 PodGroup 使用的具体调度框架。
// 前提是已调用 [Scheduler.validatePodGroup]。
func (sched *Scheduler) frameworkForPodGroup(podGroupInfo *framework.QueuedPodGroupInfo) framework.Framework {
	var result framework.Framework
	podGroupInfo.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		result = sched.Profiles[pInfo.Pod.Spec.SchedulerName]
		return false
	})
	return result
}

// skipPodGroupPodSchedule 跳过组中不再需要调度的特定 Pod，
// 例如正在删除（设置了 deletionTimestamp）或已被 Assume 的 Pod。
func (sched *Scheduler) skipPodGroupPodSchedule(ctx context.Context, schedFwk framework.Framework, podGroupInfo *framework.QueuedPodGroupInfo) {
	queuedPodInfosToUpdate := map[fwk.EntityKey][]*framework.QueuedPodInfo{}
	for pgKey, pInfos := range podGroupInfo.QueuedPodInfos {
		filteredQueuedPodInfos := make([]*framework.QueuedPodInfo, 0, len(pInfos))
		for _, podInfo := range pInfos {
			if sched.skipPodSchedule(ctx, schedFwk, podInfo.Pod) {
				// 该 Pod 不再放回队列，但必须清理其进行中状态与事件。
				sched.SchedulingQueue.Done(podInfo.Pod.UID)
				continue
			}
			filteredQueuedPodInfos = append(filteredQueuedPodInfos, podInfo)
		}
		if len(filteredQueuedPodInfos) != len(pInfos) {
			podGroupInfo.QueuedPodInfos[pgKey] = filteredQueuedPodInfos
			if len(filteredQueuedPodInfos) == 0 {
				delete(podGroupInfo.QueuedPodInfos, pgKey)
			}
			queuedPodInfosToUpdate[pgKey] = filteredQueuedPodInfos
		}
	}
	sched.updateUnscheduledPods(podGroupInfo.PodGroupInfo, queuedPodInfosToUpdate)
}

// updateUnscheduledPods 在过滤掉已删除或已 Assume 的 Pod 后，同步 PodGroup 层级中的
// 未调度 Pod 列表。它递归遍历层级，更新每个叶子组的未调度 Pod 列表。
func (sched *Scheduler) updateUnscheduledPods(pgi *framework.PodGroupInfo, queuedPodInfosToUpdate map[fwk.EntityKey][]*framework.QueuedPodInfo) {
	if len(queuedPodInfosToUpdate) == 0 {
		return
	}
	if pgi.CompositePodGroup != nil {
		for _, child := range pgi.GetChildGroups() {
			sched.updateUnscheduledPods(child, queuedPodInfosToUpdate)
		}
		return
	}
	key := pgKey(pgi)
	if podInfos, ok := queuedPodInfosToUpdate[key]; ok {
		pgi.UnscheduledPods = make([]*v1.Pod, 0, len(podInfos))
		for _, pInfo := range podInfos {
			pgi.UnscheduledPods = append(pgi.UnscheduledPods, pInfo.Pod)
		}
		delete(queuedPodInfosToUpdate, key)
	}
}

// podSchedulingContext 保存处理单个 Pod 调度所需的预计算数据。
// 同一 PodGroup 调度周期内对同一 Pod 的每次调度尝试都应使用新的 podSchedulingContext。
type podSchedulingContext struct {
	logger         klog.Logger
	state          *framework.CycleState
	podsToActivate *framework.PodsToActivate
}

// initPodSchedulingContext 为 PodGroup 调度周期中的单个 Pod 初始化调度上下文。
func initPodSchedulingContext(ctx context.Context, pod *v1.Pod, placementCycleState *framework.CycleState) *podSchedulingContext {
	logger := klog.FromContext(ctx)
	// TODO(knelasevero): 等上下文日志 GA 后，去掉日志调用中重复的键。
	// https://github.com/kubernetes/kubernetes/issues/111672

	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod))

	// 同步尝试为 Pod 寻找合适的节点。
	state := framework.NewCycleState()
	// 出于性能考虑，并非每个调度周期的每次插件执行都会采集并导出 scheduler_plugin_execution_duration 指标；
	// 只按 pluginMetricsSamplePercent 指定的比例抽样部分周期。下面这行负责随机挑选要采样的周期。

	state.SetRecordPluginMetrics(rand.Intn(100) < pluginMetricsSamplePercent)

	// 初始化空的 podsToActivate，后续由插件填充或保持为空。
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)

	podGroupCycleState := placementCycleState.GetPodGroupSchedulingCycle()
	// 将当前周期标记为 PodGroup 调度周期。
	state.SetPodGroupSchedulingCycle(podGroupCycleState)
	// 设置 Placement 周期状态，使单 Pod 插件可以访问 Placement 级别的数据。
	state.SetPlacementCycleState(placementCycleState)

	return &podSchedulingContext{
		logger:         logger,
		state:          state,
		podsToActivate: podsToActivate,
	}
}

// podGroupCycle 为给定 PodGroup 运行一次调度周期。调用前应先对集群状态做快照。
func (sched *Scheduler) podGroupCycle(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, rootPodGroupInfo *framework.QueuedPodGroupInfo, start time.Time) {
	pgResults := sched.runRootSchedulingAlgorithm(ctx, schedFwk, podGroupCycleState, rootPodGroupInfo)
	rootStatus := pgResults[pgKey(rootPodGroupInfo.PodGroupInfo)].status
	var completePGResults map[fwk.EntityKey]*podGroupAlgorithmResult
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) && rootPodGroupInfo.GetType() == fwk.CompositePodGroupKeyType {
		completePGResults = completeCompositePodGroupAlgorithmResult(ctx, rootPodGroupInfo, podGroupCycleState, pgResults)
	} else {
		// pgResults 只有 1 个元素。
		queuedPodInfos := rootPodGroupInfo.QueuedPodInfos[pgKey(rootPodGroupInfo.PodGroupInfo)]
		result := completePodGroupAlgorithmResult(ctx, queuedPodInfos, podGroupCycleState, pgResults[pgKey(rootPodGroupInfo.PodGroupInfo)])
		completePGResults = map[fwk.EntityKey]*podGroupAlgorithmResult{pgKey(rootPodGroupInfo.PodGroupInfo): result}
	}

	metrics.PodGroupSchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))

	// 调度失败时运行 PodGroup PostFilter 插件；若任一插件成功，需把组内 Pod 重新放回调度队列。

	if rootStatus.Code() == fwk.Unschedulable {
		var pgSchedulingFunc fwk.PodGroupSchedulingFunc = func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
			results := sched.runRootSchedulingAlgorithm(ctx, schedFwk, framework.NewCycleState(), rootPodGroupInfo)
			proposedAssignments := make([]fwk.ProposedAssignment, 0)
			for _, res := range results {
				proposedAssignments = append(proposedAssignments, makeProposedAssignments(res)...)
			}
			return &fwk.PodGroupAssignments{
				ProposedAssignments: proposedAssignments,
			}, results[pgKey(rootPodGroupInfo.PodGroupInfo)].status
		}
		pgPostFilterResult, status := schedFwk.RunPodGroupPostFilterPlugins(ctx, podGroupCycleState, rootPodGroupInfo.PodGroupInfo, pgSchedulingFunc)
		applyPodGroupPostFilterResult(completePGResults, pgPostFilterResult, status)
	}

	sched.submitPodGroupAlgorithmResult(ctx, schedFwk, podGroupCycleState, rootPodGroupInfo, completePGResults, start, rootStatus)
}

// runRootSchedulingAlgorithm 编排根 PodGroup 的调度尝试，决定是评估单个组还是递归评估
// 复合组层级，并最终清理算法执行期间所做的临时预留。返回的 map 汇总整个层级的调度结果。
func (sched *Scheduler) runRootSchedulingAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, rootPodGroupInfo *framework.QueuedPodGroupInfo) map[fwk.EntityKey]*podGroupAlgorithmResult {
	var revertFns revertFns
	defer revertFns.revert()
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) && rootPodGroupInfo.GetType() == fwk.CompositePodGroupKeyType {
		result := map[fwk.EntityKey]*podGroupAlgorithmResult{}
		rootResult, childRevertFns := sched.podGroupSchedulingRecursiveAlgorithm(ctx, schedFwk, podGroupCycleState, rootPodGroupInfo, rootPodGroupInfo.PodGroupInfo, result)
		revertFns = childRevertFns
		if rootResult.status.IsSuccess() && !rootResult.anyScheduled {
			// 框架要求至少调度成功 1 个 Pod 才会返回 Success 状态。
			rootResult.status = fwk.NewStatus(fwk.Unschedulable).WithError(errPodGroupUnschedulable)
		}
		return result
	}
	result, childRevertFns := sched.podGroupSchedulingAlgorithm(ctx, schedFwk, podGroupCycleState, rootPodGroupInfo.PodGroupInfo, rootPodGroupInfo)
	revertFns = childRevertFns

	if result.status.IsSuccess() && !result.anyScheduled {
		// 框架要求至少调度成功 1 个 Pod 才会返回 Success 状态。
		result.status = fwk.NewStatus(fwk.Unschedulable).WithError(errPodGroupUnschedulable)
	}
	return map[fwk.EntityKey]*podGroupAlgorithmResult{pgKey(result.podGroupInfo): result}
}

// algorithmResult 保存单个 Pod 调度尝试的结果与状态。
type algorithmResult struct {
	// podInfo 是结果所对应的 Pod 信息。
	podInfo *framework.QueuedPodInfo
	// scheduleResult 是调度算法的结果。
	scheduleResult ScheduleResult
	// podCtx 是调度算法使用的单 Pod 上下文。
	podCtx *podSchedulingContext
	// schedulingDuration 是 Pod 调度耗时，用于指标记录。
	schedulingDuration time.Duration
	// status 是调度算法的状态。
	status *fwk.Status
}

// GetPod 返回结果对应的 Pod。
func (ar *algorithmResult) GetPod() *v1.Pod {
	return ar.podInfo.Pod
}

// GetPodInfo 返回结果对应的 PodInfo。
func (ar *algorithmResult) GetPodInfo() fwk.PodInfo {
	return ar.podInfo
}

// GetNodeName 返回调度结果建议的节点名。
func (ar *algorithmResult) GetNodeName() string {
	return ar.scheduleResult.SuggestedHost
}

// GetCycleState 返回该 Pod 调度使用的周期状态。
func (ar *algorithmResult) GetCycleState() fwk.CycleState {
	return ar.podCtx.state
}

// podGroupAlgorithmResult 保存 PodGroup 的调度结果以及据此采取行动所需的信息。
type podGroupAlgorithmResult struct {
	// podGroupInfo 是结果对应的叶子 PodGroup。
	podGroupInfo *framework.PodGroupInfo
	// podResults 是组内每个 Pod 的调度结果列表。只有在组级别 Unschedulable 或 Error 时，
	// 列表才可能少于组内 Pod 总数。
	podResults []algorithmResult
	// status 是 PodGroup 算法的最终状态。
	//
	// Success 表示组可调度且无需抢占，可行的 Pod 应进入绑定周期；仅在组可行且
	// `waitingOnPreemption` 为 false 时才会设置。
	//
	// Unschedulable 表示组不可调度，所有 Pod 应作为不可调度状态放回队列；
	// `waitingOnPreemption` 为 true 的结果应带有 Unschedulable 状态。
	//
	// Error 表示调度因意外错误失败，本次尝试不会调度任何 Pod。

	status *fwk.Status
	// waitingOnPreemption 表示组是否需要或正在等待抢占完成，仅在状态为 Unschedulable 时可置为 true。

	waitingOnPreemption bool
	// placementCycleState 是处理该 Placement 时使用的状态。
	placementCycleState fwk.PlacementCycleState
	// anyScheduled 表示本周期中该组是否至少有 1 个 Pod 被调度。
	anyScheduled bool
}

// podGroupSchedulingDefaultAlgorithm 按固定顺序用标准过滤与打分逻辑尝试调度组内每个 Pod。
// 若某个 Pod 需要抢占才可调度，后续 Pod 会把它视作已调度到该节点、且受害者已在内存中移除。
// 返回的 revertFns 汇总了所有已调度 Pod 的回滚函数，便于在组调度周期失败时撤销临时预留。
func (sched *Scheduler) podGroupSchedulingDefaultAlgorithm(ctx context.Context, schedFwk framework.Framework, placementCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo) (result *podGroupAlgorithmResult, revertFns revertFns) {
	defer func() {
		if !result.status.IsSuccess() {
			revertFns.revert()
			result.anyScheduled = false
		}
	}()

	// 从根 queuedPodGroupInfo 中取出该 PodGroup 对应的排队 Pod 信息。
	queuedPodInfos := queuedPodGroupInfo.QueuedPodInfos[pgKey(podGroupInfo)]
	result = &podGroupAlgorithmResult{
		podGroupInfo:        podGroupInfo,
		podResults:          make([]algorithmResult, 0, len(queuedPodInfos)),
		status:              fwk.NewStatus(fwk.Unschedulable).WithError(errPodGroupUnschedulable),
		waitingOnPreemption: false,
		placementCycleState: placementCycleState,
	}

	logger := klog.FromContext(ctx)
	logger.V(5).Info("Running a pod group scheduling algorithm", "podGroup", klog.KObj(podGroupInfo), "unscheduledPodsCount", len(queuedPodInfos))

	podGroupState, err := sched.nodeInfoSnapshot.PodGroupStates().Get(podGroupInfo.Namespace, podGroupInfo.Name)
	if err != nil {
		result.status = fwk.AsStatus(fmt.Errorf("failed to get podGroup state for podGroup %s to compute gang feasibility: %w", klog.KObj(podGroupInfo), err))
		return result, nil
	}

	// 在尝试调度任何 Pod 之前，先运行 PlacementFeasible 插件检查组能否满足约束。

	placementProgress := framework.PlacementProgress{
		Remaining: len(podGroupInfo.GetUnscheduledPods()),
		Scheduled: podGroupState.ScheduledPodsCount(),
	}
	placementFeasibleStatus := schedFwk.RunPlacementFeasiblePlugins(ctx, placementCycleState, podGroupInfo, placementProgress)
	result.status = placementFeasibleStatus
	if placementFeasibleStatus.IsError() {
		// PlacementFeasible 插件返回错误或意外状态时，不再评估任何 Pod。
		result.status = fwk.AsStatus(fmt.Errorf("failed to evaluate placement feasibility: %w", placementFeasibleStatus.AsError()))
		return result, nil
	}
	if placementFeasibleStatus.Code() == fwk.Unschedulable {
		// PlacementFeasible 返回 Unschedulable 表明即使所有 Pod 都调度成功，组仍无法满足约束，
		// 因此可以提前退出组调度算法。

		result.status = fwk.NewStatus(fwk.Unschedulable, result.status.Reasons()...).WithError(errPodGroupUnschedulable)
		return result, nil
	}

	anyScheduled := false
	for _, podInfo := range queuedPodInfos {
		podResult, revertFn := sched.podGroupPodSchedulingAlgorithm(ctx, schedFwk, placementCycleState, podGroupInfo, podInfo)
		result.podResults = append(result.podResults, podResult)
		if revertFn != nil {
			revertFns.append([]func(){revertFn})
		}

		if !podResult.status.IsSuccess() && !podResult.status.IsRejected() {
			// 算法返回错误或意外状态时，停止评估其余 Pod。
			result.status = fwk.AsStatus(fmt.Errorf("failed to schedule other pod from a pod group: %w", podResult.status.AsError()))
			break
		}

		// 调度当前 Pod 后，检查组是否仍能满足约束。
		placementProgress.Remaining--
		if podResult.status.IsSuccess() {
			placementProgress.Scheduled++
		}
		placementFeasibleStatus := schedFwk.RunPlacementFeasiblePlugins(ctx, placementCycleState, podGroupInfo, placementProgress)
		if placementFeasibleStatus.IsError() {
			// PlacementFeasible 返回错误或意外状态时，停止评估其余 Pod。
			result.status = fwk.AsStatus(fmt.Errorf("failed to evaluate placement feasibility: %w", placementFeasibleStatus.AsError()))
			break
		}

		result.status = placementFeasibleStatus

		// PlacementFeasible 返回 Unschedulable 表明无论再检查多少 Pod 都无法满足约束，
		// 可以提前结束调度循环。

		if placementFeasibleStatus.Code() == fwk.Unschedulable {
			break
		}

		anyScheduled = anyScheduled || podResult.status.IsSuccess()
	}

	if result.status.IsWait() {
		result.status = fwk.NewStatus(fwk.Unschedulable, result.status.Reasons()...).WithError(errPodGroupUnschedulable)
	}

	result.anyScheduled = anyScheduled
	return result, revertFns
}

// podGroupPodSchedulingAlgorithm 对组内单个 Pod 运行调度算法，返回结果及对应的回滚函数。
// 当整个 PodGroup 调度失败时，该回滚函数会撤销为此 Pod 所做的临时节点预留。
func (sched *Scheduler) podGroupPodSchedulingAlgorithm(ctx context.Context, schedFwk framework.Framework, placementCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, podInfo *framework.QueuedPodInfo) (algorithmResult, func()) {
	pod := podInfo.Pod
	podCtx := initPodSchedulingContext(ctx, pod, placementCycleState)
	logger := podCtx.logger
	ctx = klog.NewContext(ctx, logger)
	start := time.Now()

	logger.V(4).Info("Attempting to schedule a pod belonging to a pod group", "podGroup", klog.KObj(podGroupInfo), "pod", klog.KObj(pod))

	scheduleResult, status := sched.schedulingAlgorithm(ctx, podCtx.state, schedFwk, podInfo, start)
	if !status.IsSuccess() {
		return algorithmResult{
			podInfo:            podInfo,
			scheduleResult:     scheduleResult,
			podCtx:             podCtx,
			schedulingDuration: time.Since(start),
			status:             status,
		}, nil
	}
	assumeStatus, revertFn := sched.assumeAndReserveWithRevert(ctx, podCtx.state, schedFwk, podInfo, scheduleResult)
	if !assumeStatus.IsSuccess() {
		return algorithmResult{
			podInfo:            podInfo,
			scheduleResult:     ScheduleResult{nominatingInfo: clearNominatedNode},
			podCtx:             podCtx,
			schedulingDuration: time.Since(start),
			status:             assumeStatus,
		}, nil
	}

	return algorithmResult{
		podInfo:            podInfo,
		scheduleResult:     scheduleResult,
		podCtx:             podCtx,
		schedulingDuration: time.Since(start),
		status:             status,
	}, revertFn
}

func (sched *Scheduler) assumeAndReserveWithRevert(ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	scheduleResult ScheduleResult,
) (*fwk.Status, func()) {
	assumedPodInfo, assumeStatus := sched.assumeAndReserve(ctx, state, schedFramework, podInfo, scheduleResult)
	if !assumeStatus.IsSuccess() {
		return assumeStatus, nil
	}
	return assumeStatus, func() {
		err := sched.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}
	}
}

// completePodGroupAlgorithmResult 确保 podGroupAlgorithmResult 中的 podResults 数量与 QueuedPodInfos 中的 Pod 数一致。
func completePodGroupAlgorithmResult(ctx context.Context, queuedPodInfos []*framework.QueuedPodInfo, podGroupState *framework.CycleState, podGroupResult *podGroupAlgorithmResult) *podGroupAlgorithmResult {
	numInResult := len(podGroupResult.podResults)
	numInQueue := len(queuedPodInfos)
	if numInResult == numInQueue {
		return podGroupResult
	}
	newResults := make([]algorithmResult, numInQueue)
	copy(newResults, podGroupResult.podResults)
	for i := numInResult; i < numInQueue; i++ {
		pInfo := queuedPodInfos[i]
		placementCycleState := framework.NewCycleState()
		placementCycleState.SetPodGroupSchedulingCycle(podGroupState)
		newResults[i] = algorithmResult{
			podInfo: pInfo,
			podCtx:  initPodSchedulingContext(ctx, pInfo.Pod, placementCycleState),
			status:  podGroupResult.status.Clone(),
		}
	}
	podGroupResult.podResults = newResults
	return podGroupResult
}

// completeCompositePodGroupAlgorithmResult 对复合 PodGroup 的调度结果做后处理，
// 确保每个子组的每个 Pod 都有完整状态，并在周期结束前把失败状态沿树向下传播。
func completeCompositePodGroupAlgorithmResult(ctx context.Context, rootPodGroupInfo *framework.QueuedPodGroupInfo, rootCycleState *framework.CycleState, pgResults map[fwk.EntityKey]*podGroupAlgorithmResult) map[fwk.EntityKey]*podGroupAlgorithmResult {
	completeCompositePodGroupAlgorithmResultMap(ctx, rootPodGroupInfo.PodGroupInfo, pgResults, &podGroupAlgorithmResult{})
	for pgKey, queuedPodInfos := range rootPodGroupInfo.QueuedPodInfos {
		pgResult := pgResults[pgKey]
		// 确保组内每个 Pod 在 podResults 中都有一条带状态的记录。
		completePodGroupAlgorithmResult(ctx, queuedPodInfos, rootCycleState, pgResult)
	}
	return pgResults
}

// completeCompositePodGroupAlgorithmResultMap 把调度失败从父组传播到子组。
// 父复合组不满足调度要求时，子组不能提交或绑定，因此必须向下传播失败状态。
func completeCompositePodGroupAlgorithmResultMap(ctx context.Context, podGroupInfo *framework.PodGroupInfo, pgResults map[fwk.EntityKey]*podGroupAlgorithmResult, parentResult *podGroupAlgorithmResult) {
	key := pgKey(podGroupInfo)
	result, ok := pgResults[key]
	// 父复合组失败时，即便子组此前自身评估成功，也必须用父组的失败状态将其作废，
	// 防止其 Pod 进入绑定。
	if !ok || (!parentResult.status.IsSuccess() && result.status.IsSuccess()) {
		result = &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       parentResult.status,
		}
		pgResults[key] = result
	}
	if podGroupInfo.CompositePodGroup != nil {
		for _, child := range podGroupInfo.GetChildGroups() {
			completeCompositePodGroupAlgorithmResultMap(ctx, child, pgResults, result)
		}
	}
}

// applyPodGroupPostFilterResult 根据 PodGroupPostFilter 插件的执行结果更新层级的最终调度结果：
// 正确登记抢占提名以便回收资源，并把调度失败、错误与抢占状态沿层级（含复合组）向下传播，
// 防止无效绑定并保证根级别指标准确。
func applyPodGroupPostFilterResult(completePGResults map[fwk.EntityKey]*podGroupAlgorithmResult, pgPostFilterResult *fwk.PodGroupPostFilterResult, status *fwk.Status) {
	if status.IsSuccess() {
		// PostFilter 插件成功找到了抢占候选：将层级中所有 PodGroup 标记为等待抢占，
		// 并把提名节点关联到叶子组的各个 Pod，以保留原有的放置决策。

		for _, pgResult := range completePGResults {
			pgResult.waitingOnPreemption = true
			if pgResult.podGroupInfo.CompositePodGroup != nil {
				continue
			}
			for j := range pgResult.podResults {
				pod := pgResult.podResults[j].podInfo.Pod
				namespacedName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
				if nodeNameInfo, ok := pgPostFilterResult.NominatingInfos[namespacedName]; ok {
					pgResult.podResults[j].scheduleResult.nominatingInfo = nodeNameInfo
				}
			}
		}
	}
	if status.IsError() {
		for _, pgResult := range completePGResults {
			pgResult.status = status
		}
	} else {
		for _, pgResult := range completePGResults {
			pgResult.status.AppendReason(status.String())
		}
	}
}

// submitPodGroupAlgorithmResult 提交 PodGroup 调度算法的结果。假定结果包含组内所有 Pod；
// 若不包含，则会更新 PodGroupCondition 以反映错误。
// 算法成功时，可调度的 Pod 进入绑定周期；不可调度的 Pod 回到队列等待下一轮组调度。
// 若该组需要抢占，所有 Pod 都会回到队列，等待下一轮组调度验证抢占结果。
func (sched *Scheduler) submitPodGroupAlgorithmResult(ctx context.Context, schedFwk framework.Framework, podGroupState *framework.CycleState, rootPodGroupInfo *framework.QueuedPodGroupInfo, podGroupResults map[fwk.EntityKey]*podGroupAlgorithmResult, start time.Time, rootStatus *fwk.Status) {
	logger := klog.FromContext(ctx)

	for _, podGroupResult := range podGroupResults {
		pgi := podGroupResult.podGroupInfo
		if pgi.CompositePodGroup != nil {
			// 复合 PodGroup 不直接持有 Pod。
			continue
		}
		queuedPodInfos := rootPodGroupInfo.QueuedPodInfos[pgKey(pgi)]
		if len(podGroupResult.podResults) != len(queuedPodInfos) {
			// 理论上不会发生；一旦发生，用错误状态补全结果。
			logger.Error(fmt.Errorf("some pods were not processed"), "scheduling error for pod group", "podGroup", klog.KObj(pgi))
			podGroupResult.status = fwk.NewStatus(fwk.Error, "scheduling error for pod group, some pods were not processed")
			podGroupResult.podResults = nil
			completePodGroupAlgorithmResult(ctx, queuedPodInfos, podGroupState, podGroupResult)
		}
		var scheduledPods, unschedulablePods int
		for i, pInfo := range queuedPodInfos {
			podResult := podGroupResult.podResults[i]
			podCtx := podResult.podCtx
			ctx := klog.NewContext(ctx, podCtx.logger)
			// 为与逐 Pod 调度保持一致，用 `now - 调度耗时` 反推 Pod 的调度开始时间。
			podSchedulingStart := time.Now().Add(-podResult.schedulingDuration)

			if podGroupResult.status.IsError() {
				if podResult.status.IsError() {
					// 若该 Pod 自身因错误失败，则使用它的状态。
					sched.FailureHandler(ctx, schedFwk, pInfo, podResult.status, clearNominatedNode, podSchedulingStart)
					continue
				}
				// 组因错误失败，用组状态拒绝所有 Pod。
				sched.FailureHandler(ctx, schedFwk, pInfo, podGroupResult.status, clearNominatedNode, podSchedulingStart)
				continue
			}
			if podResult.status.IsSuccess() {
				switch {
				case podGroupResult.status.IsSuccess():
					// 绑定前在 cycleState 中关闭 PodGroup 调度标记。
					podCtx.state.SetPodGroupSchedulingCycle(nil)
					// 将调度结果应用到 Pod 并执行其绑定周期。
					assumedPodInfo, status := sched.prepareForBindingCycle(ctx, podCtx.state, schedFwk, pInfo, podCtx.podsToActivate, podResult.scheduleResult)
					if !status.IsSuccess() {
						// 这种罕见情况下直接拒绝该 Pod。
						sched.FailureHandler(ctx, schedFwk, pInfo, status, clearNominatedNode, podSchedulingStart)
						unschedulablePods++
						continue
					}
					go sched.runBindingCycle(ctx, podCtx.state, schedFwk, podResult.scheduleResult, assumedPodInfo, podSchedulingStart, podCtx.podsToActivate)
					scheduledPods++
				case podGroupResult.status.IsRejected():
					if podGroupResult.waitingOnPreemption {
						// Pod 必须作为不可调度状态回到队列，等待抢占完成。
						sched.FailureHandler(ctx, schedFwk, pInfo, podGroupResult.status, podResult.scheduleResult.nominatingInfo, podSchedulingStart)
					} else {
						// 组不可调度，因此 Pod 也必须标记为不可调度，拒绝状态使用组的状态消息。

						status := fwk.NewStatus(fwk.Unschedulable, podGroupResult.status.Message()).WithError(errPodGroupUnschedulable)
						sched.FailureHandler(ctx, schedFwk, pInfo, status, clearNominatedNode, podSchedulingStart)
					}
					unschedulablePods++
				default:
					err := fmt.Errorf("received unexpected pod group scheduling algorithm status code: %s", podGroupResult.status.Code())
					sched.FailureHandler(ctx, schedFwk, pInfo, fwk.AsStatus(err), clearNominatedNode, podSchedulingStart)
					unschedulablePods++
				}
			} else {
				// TBD: 若 Pod 使用了无法保证找到 Placement 的特性（如异构 PodGroup、Pod 间依赖），
				// 应在状态中附加相应消息。
				// Pod 不可调度或需要抢占时，直接调用 FailureHandler。
				sched.FailureHandler(ctx, schedFwk, pInfo, podResult.status, podResult.scheduleResult.nominatingInfo, podSchedulingStart)
				unschedulablePods++
			}
		}

		var condition *metav1.Condition
		switch {
		case podGroupResult.status.IsSuccess():
			condition = &metav1.Condition{
				Type:    schedulingapi.PodGroupInitiallyScheduled,
				Status:  metav1.ConditionTrue,
				Reason:  "Scheduled",
				Message: podGroupResult.status.Message(),
			}
			logger.V(2).Info("Successfully scheduled a pod group", "podGroup", klog.KObj(pgi), "scheduledPods", scheduledPods, "unschedulablePods", unschedulablePods)

		case podGroupResult.status.IsRejected():
			condition = &metav1.Condition{
				Type:    schedulingapi.PodGroupInitiallyScheduled,
				Status:  metav1.ConditionFalse,
				Reason:  schedulingapi.PodGroupReasonUnschedulable,
				Message: podGroupResult.status.Message(),
			}
			if podGroupResult.waitingOnPreemption {
				logger.V(2).Info("Pod group is waiting for preemption", "podGroup", klog.KObj(pgi), "unschedulablePods", unschedulablePods, "err", podGroupResult.status.Message())
			} else {
				logger.V(2).Info("Unable to schedule a pod group", "podGroup", klog.KObj(pgi), "unschedulablePods", unschedulablePods, "err", podGroupResult.status.Message())
			}

		default:
			condition = &metav1.Condition{
				Type:    schedulingapi.PodGroupInitiallyScheduled,
				Status:  metav1.ConditionFalse,
				Reason:  schedulingapi.PodGroupReasonSchedulerError,
				Message: podGroupResult.status.AsError().Error(),
			}
			utilruntime.HandleErrorWithContext(ctx, podGroupResult.status.AsError(), "Error scheduling pod group", "podGroup", klog.KObj(pgi), "errorPods", len(queuedPodInfos))
		}
		sched.updatePodGroupCondition(ctx, pgi, condition)
	}

	rootResult := podGroupResults[pgKey(rootPodGroupInfo.PodGroupInfo)]
	switch {
	case rootResult.status.IsSuccess():
		metrics.PodGroupScheduled(schedFwk.ProfileName(), metrics.SinceInSeconds(start))
	case rootResult.status.IsRejected():
		if rootResult.waitingOnPreemption {
			metrics.PodGroupWaitingOnPreemption(schedFwk.ProfileName(), metrics.SinceInSeconds(start))
		} else {
			metrics.PodGroupUnschedulable(schedFwk.ProfileName(), metrics.SinceInSeconds(start))
		}
	default:
		metrics.PodGroupScheduleError(schedFwk.ProfileName(), metrics.SinceInSeconds(start))
	}

	if err := sched.SchedulingQueue.AddAttemptedPodGroupIfNeeded(logger, rootPodGroupInfo, sched.SchedulingQueue.SchedulingCycle(), rootStatus); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to add attempted pod group to scheduling queue", rootPodGroupInfo.Type, klog.KObj(rootPodGroupInfo))
	}
}

// updatePodGroupCondition 为 PodGroup 打上给定条件。
func (sched *Scheduler) updatePodGroupCondition(ctx context.Context,
	podGroupInfo *framework.PodGroupInfo, condition *metav1.Condition) {
	logger := klog.FromContext(ctx)

	// 若 PodGroup 此前已成功调度，后续为新增 Pod 再调度时不要把条件回退为 False。

	pg := podGroupInfo.PodGroup
	existing := apimeta.FindStatusCondition(pg.Status.Conditions, condition.Type)
	if existing != nil && existing.Status == metav1.ConditionTrue && condition.Status != metav1.ConditionTrue {
		return
	}

	condition.ObservedGeneration = pg.Generation
	newStatus := pg.Status.DeepCopy()
	if !apimeta.SetStatusCondition(&newStatus.Conditions, *condition) {
		return
	}

	if err := util.PatchPodGroupStatus(ctx, sched.client, podGroupInfo.Name, podGroupInfo.Namespace, &pg.Status, newStatus); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to update PodGroup status", "podGroup", klog.KObj(podGroupInfo))
	}
}

// podGroupSchedulingPlacementAlgorithm 尝试多种 PodGroup 调度组合并选出最优解。
// 先用 Placement 生成插件产生候选 Placement（即调度时要考虑的节点集合），
// 再对每个 Placement 用默认算法尝试调度，最后用 Placement 打分插件选出最佳 Placement。
func (sched *Scheduler) podGroupSchedulingPlacementAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo) (finalResult *podGroupAlgorithmResult, revertFns revertFns) {
	logger := klog.FromContext(ctx)
	allNodes, err := sched.nodeInfoSnapshot.ListNodesInPlacement()
	if err != nil {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       fwk.AsStatus(fmt.Errorf("failed to list node infos: %w", err)),
		}, nil
	}

	// 目前一律采集插件指标，待评估其对性能的影响后再调整。
	podGroupCycleState.SetRecordPluginMetrics(true)
	placements, status := schedFwk.RunPlacementGeneratePlugins(ctx, podGroupCycleState, podGroupInfo, allNodes)
	if !status.IsSuccess() {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       status,
		}, nil
	}
	metrics.RecordGeneratedPlacements(schedFwk.ProfileName(), len(placements))

	var anyResult *podGroupAlgorithmResult
	successfulResults := make(map[*fwk.Placement]*podGroupAlgorithmResult)

	parentPlacement := sched.nodeInfoSnapshot.GetPlacement()
	defer func() {
		sched.nodeInfoSnapshot.ForgetPlacement()
		err := sched.nodeInfoSnapshot.AssumePlacement(parentPlacement)
		if err != nil {
			finalResult.status = fwk.AsStatus(fmt.Errorf("failed to restore parent pod group placement: %w", err))
			revertFns.revert()
		}
	}()

	for _, placement := range placements {
		logger.V(4).Info("Assuming placement in snapshot", "placement", placement.Name)
		evaluationStart := time.Now()
		err := sched.nodeInfoSnapshot.AssumePlacement(placement)
		if err != nil {
			return &podGroupAlgorithmResult{
				podGroupInfo: podGroupInfo,
				status:       fwk.AsStatus(fmt.Errorf("failed to assume pod group placement: %w", err)),
			}, nil
		}
		placementCycleState := framework.NewCycleState()
		placementCycleState.SetPodGroupSchedulingCycle(podGroupCycleState)
		result, placementRevertFns := sched.podGroupSchedulingDefaultAlgorithm(ctx, schedFwk, placementCycleState, podGroupInfo, queuedPodGroupInfo)
		placementRevertFns.revert()

		if result.status.IsError() {
			return result, nil
		}

		if anyResult == nil {
			anyResult = result
		}

		// 错误情况已在上面的提前返回中处理；这里只统计可行性结果，不把内部故障算进去。

		evaluationResult := metrics.InfeasibleResult
		if result.status.IsSuccess() {
			evaluationResult = metrics.FeasibleResult
			successfulResults[placement] = result
		}
		metrics.ObservePlacementEvaluation(evaluationResult, schedFwk.ProfileName(), metrics.SinceInSeconds(evaluationStart))
	}

	if len(successfulResults) == 0 {
		// 所有候选都不可行时，仍需为 Pod 发送事件并设置状态；当前任选一个候选结果上报，
		// 将来可能会改为更有意义的选择。
		anyResult.status = fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("0/%d placements are available, first placement status: %v", len(placements), anyResult.status.AsError())).WithError(errPodGroupUnschedulable)
		return anyResult, nil
	}

	bestPlacement, status := sched.findBestPodGroupPlacement(ctx, schedFwk, podGroupCycleState, podGroupInfo, successfulResults)
	if !status.IsSuccess() {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       status,
		}, nil
	}
	bestResult := successfulResults[bestPlacement]

	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		revertFns, err = sched.assumeSubtreeWithRevert(ctx, schedFwk, podGroupInfo, map[fwk.EntityKey]*podGroupAlgorithmResult{pgKey(podGroupInfo): bestResult})
		if err != nil {
			return &podGroupAlgorithmResult{
				podGroupInfo: podGroupInfo,
				status:       fwk.AsStatus(fmt.Errorf("failed to assume the subtree: %w", err)),
			}, nil
		}

		return bestResult, revertFns
	}
	return bestResult, nil
}

// compositePodGroupSchedulingPlacementAlgorithm 尝试多种调度子组的组合并选出最优解。
// 先用 Placement 生成插件产生候选 Placement（即调度时要考虑的节点集合），
// 再对每个 Placement 用默认算法尝试调度，最后用 Placement 打分插件选出最佳 Placement。
func (sched *Scheduler) compositePodGroupSchedulingPlacementAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, root *framework.QueuedPodGroupInfo, podGroupInfo *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) (finalResult *podGroupAlgorithmResult, revertFns revertFns) {
	defer func() {
		results[pgKey(podGroupInfo)] = finalResult
	}()
	logger := klog.FromContext(ctx)
	allNodes, err := sched.nodeInfoSnapshot.ListNodesInPlacement()
	if err != nil {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       fwk.AsStatus(fmt.Errorf("failed to list node infos: %w", err)),
		}, nil
	}

	// 目前一律采集插件指标，待评估其对性能的影响后再调整。
	podGroupCycleState.SetRecordPluginMetrics(true)
	placements, status := schedFwk.RunPlacementGeneratePlugins(ctx, podGroupCycleState, podGroupInfo, allNodes)
	if !status.IsSuccess() {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       status,
		}, nil
	}

	var anyResultSubtree map[fwk.EntityKey]*podGroupAlgorithmResult
	successfulResults := make(map[*fwk.Placement]map[fwk.EntityKey]*podGroupAlgorithmResult)

	parentPlacement := sched.nodeInfoSnapshot.GetPlacement()
	defer func() {
		sched.nodeInfoSnapshot.ForgetPlacement()
		err := sched.nodeInfoSnapshot.AssumePlacement(parentPlacement)
		if err != nil {
			finalResult.status = fwk.AsStatus(fmt.Errorf("failed to restore parent pod group placement: %w", err))
			revertFns.revert()
		}
	}()

	for _, placement := range placements {
		logger.V(4).Info("Assuming placement in snapshot", "placement", placement.Name)
		err := sched.nodeInfoSnapshot.AssumePlacement(placement)
		if err != nil {
			return &podGroupAlgorithmResult{
				podGroupInfo: podGroupInfo,
				status:       fwk.AsStatus(fmt.Errorf("failed to assume pod group placement: %w", err)),
			}, nil
		}
		placementCycleState := framework.NewCycleState()
		placementCycleState.SetPodGroupSchedulingCycle(podGroupCycleState)
		subtreeResult := map[fwk.EntityKey]*podGroupAlgorithmResult{}
		result, placementRevertFns := sched.compositePodGroupSchedulingDefaultAlgorithm(ctx, schedFwk, placementCycleState, root, podGroupInfo, subtreeResult)
		placementRevertFns.revert()

		if result.status.IsError() {
			return result, nil
		}

		if anyResultSubtree == nil {
			anyResultSubtree = subtreeResult
		}

		if result.status.IsSuccess() {
			successfulResults[placement] = subtreeResult
		}
	}

	if len(successfulResults) == 0 {
		// 所有候选都不可行时，仍需为 Pod 发送事件并设置状态；当前任选一个候选结果上报，
		// 将来可能会改为更有意义的选择。
		anyResultRoot := anyResultSubtree[pgKey(podGroupInfo)]
		anyResultRoot.status = fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("0/%d placements are available, first placement status: %v", len(placements), anyResultRoot.status.AsError())).WithError(errPodGroupUnschedulable)
		// 必须把整个 anyResultSubtree 复制到 results，否则后续会用泛化的父级错误
		// (errPodGroupUnschedulable) 重建 Pod 结果，丢掉原有的 *framework.FitError。
		// 丢失 FitError 意味着丢失每个 Pod 的 UnschedulablePlugins，从而破坏 QueueingHints。

		maps.Copy(results, anyResultSubtree)
		return anyResultRoot, nil
	}

	bestPlacement, status := sched.findBestCompositePodGroupPlacement(ctx, schedFwk, podGroupCycleState, podGroupInfo, successfulResults)
	if !status.IsSuccess() {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       status,
		}, nil
	}

	bestResult := successfulResults[bestPlacement]

	revertFns, err = sched.assumeSubtreeWithRevert(ctx, schedFwk, podGroupInfo, bestResult)
	if err != nil {
		return &podGroupAlgorithmResult{
			podGroupInfo: podGroupInfo,
			status:       fwk.AsStatus(fmt.Errorf("failed to assume the subtree: %w", err)),
		}, nil
	}
	maps.Copy(results, bestResult)

	return bestResult[pgKey(podGroupInfo)], revertFns
}

func (sched *Scheduler) findBestPodGroupPlacement(ctx context.Context, schedFwk framework.Framework, podGroupCycleState fwk.PodGroupCycleState, podGroupInfo *framework.PodGroupInfo, successfulResults map[*fwk.Placement]*podGroupAlgorithmResult) (*fwk.Placement, *fwk.Status) {
	if len(successfulResults) == 1 {
		for placement := range successfulResults {
			return placement, nil
		}
	}

	placementPodGroupAssignments, placementStates := makePodGroupAssignments(successfulResults)
	return sched.findBestPlacement(ctx, schedFwk, podGroupCycleState, podGroupInfo, placementPodGroupAssignments, placementStates)
}

func (sched *Scheduler) findBestCompositePodGroupPlacement(ctx context.Context, schedFwk framework.Framework, podGroupCycleState fwk.PodGroupCycleState, podGroupInfo *framework.PodGroupInfo, successfulResults map[*fwk.Placement]map[fwk.EntityKey]*podGroupAlgorithmResult) (*fwk.Placement, *fwk.Status) {
	if len(successfulResults) == 1 {
		for placement := range successfulResults {
			return placement, nil
		}
	}

	placementPodGroupAssignments, placementStates := makeCompositePodGroupAssignments(podGroupInfo, successfulResults)
	return sched.findBestPlacement(ctx, schedFwk, podGroupCycleState, podGroupInfo, placementPodGroupAssignments, placementStates)
}

// findBestPlacement 用 PlacementScore 插件根据调度结果选出最佳 Placement。
func (sched *Scheduler) findBestPlacement(ctx context.Context, schedFwk framework.Framework, podGroupCycleState fwk.PodGroupCycleState, podGroupInfo *framework.PodGroupInfo, placementPodGroupAssignments []*fwk.PodGroupAssignments, placementStates []fwk.PlacementCycleState) (*fwk.Placement, *fwk.Status) {
	scores, status := schedFwk.RunPlacementScorePlugins(ctx, podGroupCycleState, podGroupInfo, placementPodGroupAssignments, placementStates)
	if !status.IsSuccess() {
		return nil, status
	}

	for i := range scores {
		scores[i].Randomizer = rand.Int()
	}

	loggerVTen := klog.FromContext(ctx).V(10)
	if loggerVTen.Enabled() {
		for _, score := range scores {
			for _, pluginScore := range score.Scores {
				loggerVTen.Info("Plugin scored placement for podGroup", "podGroup", klog.KObj(podGroupInfo), "plugin", pluginScore.Name, "placement", score.Placement.Name, "score", pluginScore.Score)
			}
			loggerVTen.Info("Calculated placement's final score for podGroup", "podGroup", klog.KObj(podGroupInfo), "placement", score.Placement.Name, "score", score.TotalScore)
		}
	}

	bestScore := &scores[0]
	for _, score := range scores[1:] {
		if score.TotalScore > bestScore.TotalScore ||
			score.TotalScore == bestScore.TotalScore &&
				score.Randomizer > bestScore.Randomizer {
			bestScore = &score
		}
	}
	return bestScore.Placement, nil
}

// makePodGroupAssignments 把各候选 Placement 的 PodGroup 调度结果转换成
// PlacementScore 插件打分选优所需的格式。
func makePodGroupAssignments(successfulResults map[*fwk.Placement]*podGroupAlgorithmResult) ([]*fwk.PodGroupAssignments, []fwk.PlacementCycleState) {
	placementPodGroupAssignments := make([]*fwk.PodGroupAssignments, 0, len(successfulResults))
	placementStates := make([]fwk.PlacementCycleState, 0, len(successfulResults))
	for placement, result := range successfulResults {
		proposedAssignments := makeProposedAssignments(result)
		placementPodGroupAssignments = append(placementPodGroupAssignments, &fwk.PodGroupAssignments{
			Placement:           placement,
			ProposedAssignments: proposedAssignments,
		})
		placementStates = append(placementStates, result.placementCycleState)
	}
	return placementPodGroupAssignments, placementStates
}

// makePodGroupAssignments 把各候选 Placement 的 CompositePodGroup 调度结果转换成
// PlacementScore 插件打分选优所需的格式。
func makeCompositePodGroupAssignments(pgi *framework.PodGroupInfo, successfulResults map[*fwk.Placement]map[fwk.EntityKey]*podGroupAlgorithmResult) ([]*fwk.PodGroupAssignments, []fwk.PlacementCycleState) {
	placementPodGroupAssignments := make([]*fwk.PodGroupAssignments, 0)
	placementStates := make([]fwk.PlacementCycleState, 0)
	for placement, subtreeResults := range successfulResults {
		var combinedProposedAssignments []fwk.ProposedAssignment
		for result := range successfulLeafResults(pgi, subtreeResults) {
			combinedProposedAssignments = append(combinedProposedAssignments, makeProposedAssignments(result)...)
		}
		placementPodGroupAssignments = append(placementPodGroupAssignments, &fwk.PodGroupAssignments{
			Placement:           placement,
			ProposedAssignments: combinedProposedAssignments,
		})
		placementStates = append(placementStates, subtreeResults[pgKey(pgi)].placementCycleState)
	}
	return placementPodGroupAssignments, placementStates
}

// makeProposedAssignments 从一次 PodGroup 调度结果中构建提议分配列表。
func makeProposedAssignments(res *podGroupAlgorithmResult) []fwk.ProposedAssignment {
	proposedAssignments := make([]fwk.ProposedAssignment, 0)
	for _, podRes := range res.podResults {
		if podRes.status.IsSuccess() && podRes.GetNodeName() != "" {
			proposedAssignments = append(proposedAssignments, &podRes)
		}
	}
	return proposedAssignments
}

// podGroupSchedulingAlgorithm 按策略与约束调度组内 Pod，返回所有已评估 Pod 的结果（未必是组内全部 Pod）。
// 返回的 revertFns 汇总所有已调度 Pod 的回滚函数，便于组调度周期失败时撤销临时预留。
func (sched *Scheduler) podGroupSchedulingAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo) (*podGroupAlgorithmResult, revertFns) {
	podGroupCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
		return sched.podGroupSchedulingPlacementAlgorithm(podGroupCycleCtx, schedFwk, podGroupCycleState, podGroupInfo, queuedPodGroupInfo)
	}
	// 非 TAS 的默认算法不评估候选 Placement，但仍在一个隐式 Placement 上下文中运行，
	// 使 Placement 级别的扩展点能复用与 TAS 相同的状态传递机制。

	placementCycleState := framework.NewCycleState()
	placementCycleState.SetPodGroupSchedulingCycle(podGroupCycleState)
	return sched.podGroupSchedulingDefaultAlgorithm(podGroupCycleCtx, schedFwk, placementCycleState, podGroupInfo, queuedPodGroupInfo)
}

// podGroupSchedulingRecursiveAlgorithm 运行递归的 PodGroup 调度算法：
// 若封装的是复合 PodGroup，则对其子组递归调用；否则对叶子组调用标准调度算法。
// 返回的 revertFns 把所有子组评估产生的回滚函数向上传递到根层级。
func (sched *Scheduler) podGroupSchedulingRecursiveAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, root *framework.QueuedPodGroupInfo, podGroupInfo *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) (*podGroupAlgorithmResult, revertFns) {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Running recursive podgroup scheduling algorithm", "rootType", podGroupInfo.Type, "root", klog.KObj(podGroupInfo))

	var algorithmResult *podGroupAlgorithmResult
	var childRevertFns revertFns
	if podGroupInfo.Type == fwk.PodGroupKeyType {
		algorithmResult, childRevertFns = sched.podGroupSchedulingAlgorithm(ctx, schedFwk, podGroupCycleState, podGroupInfo, root)
		results[pgKey(podGroupInfo)] = algorithmResult
	} else {
		algorithmResult, childRevertFns = sched.compositePodGroupSchedulingAlgorithm(ctx, schedFwk, podGroupCycleState, root, podGroupInfo, results)
	}
	return algorithmResult, childRevertFns
}

func (sched *Scheduler) compositePodGroupSchedulingAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, root *framework.QueuedPodGroupInfo, podGroupInfo *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) (result *podGroupAlgorithmResult, revertFns revertFns) {
	podGroupCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// CPG 要求启用 TopologyAwareWorkloadScheduling 特性。
	return sched.compositePodGroupSchedulingPlacementAlgorithm(podGroupCycleCtx, schedFwk, podGroupCycleState, root, podGroupInfo, results)
}

// compositePodGroupSchedulingDefaultAlgorithm 通过递归调度子组来调度复合 PodGroup。
// 它在递归的每一步用 PlacementFeasible 插件校验复合组约束是否仍可满足，不满足则提前中止并回滚。
// 返回的 revertFns 把所有子组评估产生的回滚函数向上传递到根层级。
func (sched *Scheduler) compositePodGroupSchedulingDefaultAlgorithm(ctx context.Context, schedFwk framework.Framework, placementCycleState *framework.CycleState, root *framework.QueuedPodGroupInfo, podGroupInfo *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) (result *podGroupAlgorithmResult, revertFns revertFns) {
	logger := klog.FromContext(ctx)
	defer func() {
		results[pgKey(podGroupInfo)] = result
		if result.status.IsSuccess() {
			logger.V(5).Info("Composite podgroup scheduling algorithm succeeded", "compositePodGroup", klog.KObj(podGroupInfo))
		} else {
			logger.V(5).Info("Composite podgroup scheduling algorithm failed", "compositePodGroup", klog.KObj(podGroupInfo), "status", result.status)
			revertFns.revert()
			result.anyScheduled = false
		}
	}()

	placementProgress := framework.PlacementProgress{
		Remaining: len(podGroupInfo.Children),
	}
	placementFeasibleStatus := schedFwk.RunPlacementFeasiblePlugins(ctx, placementCycleState, podGroupInfo, placementProgress)

	if placementFeasibleStatus.Code() == fwk.Unschedulable || placementFeasibleStatus.Code() == fwk.Error {
		return &podGroupAlgorithmResult{
			podGroupInfo:        podGroupInfo,
			status:              placementFeasibleStatus,
			placementCycleState: placementCycleState,
		}, revertFns
	}
	anyScheduled := false
	for _, childPGInfo := range podGroupInfo.GetChildGroups() {
		childPodGroupState := framework.NewCycleState()
		childPodGroupState.SetPlacementCycleState(placementCycleState)
		childResult, childRevertFns := sched.podGroupSchedulingRecursiveAlgorithm(ctx, schedFwk, childPodGroupState, root, childPGInfo, results)
		if childResult.status.IsError() {
			return &podGroupAlgorithmResult{
				podGroupInfo:        podGroupInfo,
				status:              fwk.AsStatus(fmt.Errorf("composite pod group evaluation failed due to child error: %w", childResult.status.AsError())),
				placementCycleState: placementCycleState,
			}, revertFns
		}
		anyScheduled = anyScheduled || childResult.anyScheduled
		revertFns.append(childRevertFns)
		placementProgress.Remaining--
		if childResult.status.IsSuccess() {
			placementProgress.Scheduled++
		}
		placementFeasibleStatus = schedFwk.RunPlacementFeasiblePlugins(ctx, placementCycleState, podGroupInfo, placementProgress)
		if placementFeasibleStatus.Code() == fwk.Unschedulable || placementFeasibleStatus.Code() == fwk.Error {
			break
		}
	}

	if placementFeasibleStatus.IsWait() {
		placementFeasibleStatus = fwk.NewStatus(fwk.Unschedulable, placementFeasibleStatus.Reasons()...).WithError(errPodGroupUnschedulable)
	}
	return &podGroupAlgorithmResult{
		podGroupInfo:        podGroupInfo,
		status:              placementFeasibleStatus,
		placementCycleState: placementCycleState,
		anyScheduled:        anyScheduled,
	}, revertFns
}

func pgKey(pgi *framework.PodGroupInfo) fwk.EntityKey {
	if pgi.Type == fwk.CompositePodGroupKeyType {
		return fwk.CompositePodGroupKey(pgi.Namespace, pgi.Name)
	}
	return fwk.PodGroupKey(pgi.Namespace, pgi.Name)
}

// assumeSubtreeWithRevert 对子树中所有 Pod 执行 assumeAndReserveWithRevert。
// 基于 Placement 的算法需要这一步：评估完所有候选后，必须 Assume 选中的结果，
// 层级中其他 Pod 才能看到该结果。
func (sched *Scheduler) assumeSubtreeWithRevert(ctx context.Context, schedFwk framework.Framework, pgi *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) (_ revertFns, err error) {
	if results == nil {
		return nil, fmt.Errorf("results for the subtree are missing")
	}

	var revertFns revertFns
	defer func() {
		if err != nil {
			revertFns.revert()
		}
	}()
	for leafResult := range successfulLeafResults(pgi, results) {
		for _, podResult := range leafResult.podResults {
			if !podResult.status.IsSuccess() || podResult.GetNodeName() == "" {
				continue
			}
			status, revert := sched.assumeAndReserveWithRevert(ctx, podResult.podCtx.state, schedFwk, podResult.podInfo, podResult.scheduleResult)
			if revert != nil {
				revertFns = append(revertFns, revert)
			}
			if !status.IsSuccess() {
				return nil, status.AsError()
			}
		}
	}

	return revertFns, nil
}

// successfulLeafResults 沿树向下遍历成功的叶子节点。只有祖先也成功时叶子才算成功；
// 缺少结果的子树会被跳过。
func successfulLeafResults(root *framework.PodGroupInfo, results map[fwk.EntityKey]*podGroupAlgorithmResult) iter.Seq[*podGroupAlgorithmResult] {
	return func(yield func(*podGroupAlgorithmResult) bool) {
		var walk func(pgi *framework.PodGroupInfo) bool
		walk = func(pgi *framework.PodGroupInfo) bool {
			result, ok := results[pgKey(pgi)]
			// 结果缺失可能是因为该子树因 PlacementFeasible 状态被跳过。
			// 若某子树结果为非成功（如 actualCount < minGroupCount），其所有后代都按非成功、0 个 Pod 处理。
			if !ok || !result.status.IsSuccess() {
				return true
			}

			for _, child := range pgi.Children {
				if !walk(child) {
					return false
				}
			}

			if len(result.podResults) > 0 {
				return yield(result)
			}

			return true
		}
		walk(root)
	}
}
