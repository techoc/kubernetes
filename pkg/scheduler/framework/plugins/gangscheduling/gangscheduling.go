/*
Copyright 2025 The Kubernetes Authors.

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

package gangscheduling

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// Name 是插件在插件注册表和配置中使用的名称。
	Name = names.GangScheduling
	// permitTimeoutDuration 定义 gang Pod 在 permit 阶段等待法定人数的最长时间，
	// 超时后会被拒绝。
	permitTimeoutDuration = 5 * time.Minute
)

// GangScheduling 对带有 Gang 调度策略的 PodGroup 中的 Pod 强制执行“全有或全无”调度。
type GangScheduling struct {
	handle                     fwk.Handle
	podGroupManager            fwk.PodGroupManager
	snapshotLister             fwk.SharedLister
	isCompositePodGroupEnabled bool
}

var _ fwk.EnqueueExtensions = &GangScheduling{}
var _ fwk.PreEnqueuePlugin = &GangScheduling{}
var _ fwk.PermitPlugin = &GangScheduling{}
var _ framework.PlacementFeasiblePlugin = &GangScheduling{}

// New 初始化并返回一个 GangScheduling 插件实例。
func New(_ context.Context, _ runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &GangScheduling{
		handle:                     fh,
		podGroupManager:            fh.PodGroupManager(),
		snapshotLister:             fh.SnapshotSharedLister(),
		isCompositePodGroupEnabled: fts.EnableCompositePodGroup,
	}, nil
}

// Name 返回插件名称。
func (pl *GangScheduling) Name() string {
	return Name
}

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 重新变为可调度的集群事件。
func (pl *GangScheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	events := []fwk.ClusterEventWithHint{
		// 新增 Pod（未调度或预绑定）可能正好凑齐 gang 的 MinCount 要求。
		// PodSchedulingGroup 字段不可变，因此无需订阅 Pod/Update 事件。
		{Event: fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodAdded},
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodAdded},
		// 新增 PodGroup 可能让一个正在等待的 gang 变得可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodGroupAdded},
		// 更新 PodGroup 的 MinCount 可能让它变得可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPodGroupUpdated},
	}

	if pl.isCompositePodGroupEnabled {
		// 新增 CompositePodGroup 可能让一个正在等待的 gang 变得可调度。
		events = append(events, fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.CompositePodGroup, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterCompositePodGroupAdded})
	}

	return events, nil
}

// isSchedulableAfterPodAdded 判断新增的 Pod（未调度或预绑定）是否凑齐了 gang 的 MinCount，
// 从而让之前不可调度的 Pod 变为可调度。
func (pl *GangScheduling) isSchedulableAfterPodAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pod.Spec.SchedulingGroup == nil || addedPod.Spec.SchedulingGroup == nil {
		return fwk.QueueSkip, nil
	}
	addedPodGroupKey := fwk.PodGroupKey(addedPod.Namespace, *addedPod.Spec.SchedulingGroup.PodGroupName)
	if pl.areSameHierarchy(logger, pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName, addedPodGroupKey) {
		return fwk.Queue, nil
	}
	return fwk.QueueSkip, nil
}

// isSchedulableAfterPodGroupUpdated 在 MinCount 降低时触发该组 Pod 重新入队。
func (pl *GangScheduling) isSchedulableAfterPodGroupUpdated(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldPodGroup, newPodGroup, err := util.As[*schedulingapi.PodGroup](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pod.Spec.SchedulingGroup == nil {
		return fwk.QueueSkip, nil
	}
	updatedPodGroupKey := fwk.PodGroupKey(newPodGroup.Namespace, newPodGroup.Name)
	if !pl.areSameHierarchy(logger, pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName, updatedPodGroupKey) {
		return fwk.QueueSkip, nil
	}

	oldPolicy := oldPodGroup.Spec.SchedulingPolicy
	newPolicy := newPodGroup.Spec.SchedulingPolicy

	// 更新非 gang 策略不会让等待中的 Pod 变得可调度。
	if newPolicy.Gang == nil || oldPolicy.Gang == nil {
		return fwk.QueueSkip, nil
	}

	// gang 策略的 MinCount 没有降低时，等待中的 Pod 不会因此变得可调度。
	if newPolicy.Gang.MinCount >= oldPolicy.Gang.MinCount {
		logger.V(5).Info("PodGroup minCount did not decrease, skipping", "pod", klog.KObj(pod), "podGroup", klog.KObj(newPodGroup), "oldMinCount", oldPolicy.Gang.MinCount, "newMinCount", newPolicy.Gang.MinCount)
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("pod group was updated and minCount decreased, enqueuing pod", "pod", klog.KObj(pod), "podGroup", klog.KObj(newPodGroup), "oldMinCount", oldPolicy.Gang.MinCount, "newMinCount", newPolicy.Gang.MinCount)
	return fwk.Queue, nil
}

// isSchedulableAfterPodGroupAdded 判断新增的 PodGroup 是否让等待中的 gang 可调度。
func (pl *GangScheduling) isSchedulableAfterPodGroupAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedPodGroup, err := util.As[*schedulingapi.PodGroup](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pod.Spec.SchedulingGroup == nil {
		return fwk.QueueSkip, nil
	}
	addedPodGroupKey := fwk.PodGroupKey(addedPodGroup.Namespace, addedPodGroup.Name)
	if pl.areSameHierarchy(logger, pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName, addedPodGroupKey) {
		return fwk.Queue, nil
	}
	return fwk.QueueSkip, nil
}

// isSchedulableAfterCompositePodGroupAdded 判断新增的 CompositePodGroup 是否让等待中的 gang 可调度。
func (pl *GangScheduling) isSchedulableAfterCompositePodGroupAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedCPG, err := util.As[*schedulingv1alpha3.CompositePodGroup](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pod.Spec.SchedulingGroup == nil {
		return fwk.QueueSkip, nil
	}

	compositePodGroupKey := fwk.CompositePodGroupKey(addedCPG.Namespace, addedCPG.Name)
	if pl.areSameHierarchy(logger, pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName, compositePodGroupKey) {
		return fwk.Queue, nil
	}
	return fwk.QueueSkip, nil
}

// areSameHierarchy 检查给定的 PodGroup 与目标 key 是否位于同一层级结构中。
// 方法分别取两者的根 CompositePodGroup 然后比较；未启用层级特性时直接比较 key 本身。
func (pl *GangScheduling) areSameHierarchy(logger klog.Logger, namespace, podGroupName string, targetKey fwk.EntityKey) bool {
	if !pl.isCompositePodGroupEnabled {
		return fwk.PodGroupKey(namespace, podGroupName) == targetKey
	}

	podGroupKey := fwk.PodGroupKey(namespace, podGroupName)
	root1, ok1, err := pl.podGroupManager.GetRootKeyForGroup(podGroupKey)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to get root key for key", "key", podGroupKey)
		return false
	}
	if !ok1 {
		return false
	}
	root2, ok2, err := pl.podGroupManager.GetRootKeyForGroup(targetKey)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to get root key for key", "key", targetKey)
		return false
	}
	if !ok2 {
		return false
	}
	return root1 == root2
}

// PreEnqueue 检查 Pod 是否属于某个 gang；若是，则判断该 gang 的可用 Pod 数是否达到 MinCount。
// 未达到时拒绝该 Pod 入队，直到更多 Pod 到达。
func (pl *GangScheduling) PreEnqueue(ctx context.Context, pod *v1.Pod) *fwk.Status {
	if !pl.isCompositePodGroupEnabled {
		return pl.preEnqueueHierarchiesDisabled(pod)
	}
	return pl.preEnqueueWithHierarchies(pod)
}

// preEnqueueWithHierarchies 在启用层级特性时检查 Pod 所属 gang 的可用 Pod 数是否达到 MinCount，
// 未达到则拒绝其入队。
func (pl *GangScheduling) preEnqueueWithHierarchies(pod *v1.Pod) *fwk.Status {
	if pod.Spec.SchedulingGroup == nil {
		return nil
	}

	snapshot, err := pl.handle.PodGroupManager().BuildHierarchySnapshotFromPod(pod)
	if err != nil {
		// 无法构建层级快照（例如找不到根 PodGroup）时，按不可调度处理。
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("failed to build hierarchy snapshot: %v", err))
	}

	namespace := pod.Namespace
	schedulingGroup := pod.Spec.SchedulingGroup

	podGroup, err := snapshot.PodGroups().Get(namespace, *schedulingGroup.PodGroupName)
	if err != nil {
		// PodGroup 对象尚未创建前，该 Pod 一直不可调度。
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for pods's pod group %q to appear in scheduling queue", *schedulingGroup.PodGroupName))
	}

	if podGroup.Spec.ParentCompositePodGroupName == nil || !pl.isCompositePodGroupEnabled {
		if pl.isPGReady(snapshot, namespace, podGroup.Name, func(s fwk.PodGroupState) int { return s.AllPodsCount() }) {
			return nil
		}
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for minCount pods from a gang to appear in scheduling queue")
	}

	return pl.checkCPGHierarchyReadiness(snapshot, namespace, *podGroup.Spec.ParentCompositePodGroupName, func(s fwk.PodGroupState) int { return s.AllPodsCount() })
}

// preEnqueueHierarchiesDisabled 在禁用层级特性时检查 Pod 所属 gang 的可用 Pod 数是否达到 MinCount，
// 未达到则拒绝其入队。仅在 CompositePodGroup 特性门控关闭时使用。
func (pl *GangScheduling) preEnqueueHierarchiesDisabled(pod *v1.Pod) *fwk.Status {
	if pod.Spec.SchedulingGroup == nil {
		return nil
	}

	namespace := pod.Namespace
	schedulingGroup := pod.Spec.SchedulingGroup

	podGroup, err := pl.podGroupManager.PodGroups().Get(namespace, *schedulingGroup.PodGroupName)
	if err != nil {
		// PodGroup 对象尚未创建前，该 Pod 一直不可调度。
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for pods's pod group %q to appear in scheduling queue", *schedulingGroup.PodGroupName))
	}

	policy := podGroup.Spec.SchedulingPolicy
	// 本插件只关心采用 Gang 调度策略的 Pod。
	if policy.Gang == nil {
		return nil
	}

	podGroupState, err := pl.podGroupManager.PodGroupStates().Get(namespace, *schedulingGroup.PodGroupName)
	if err != nil {
		return fwk.AsStatus(err)
	}
	allPodsCount := podGroupState.AllPodsCount()
	if allPodsCount < int(policy.Gang.MinCount) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for minCount pods from a gang to appear in scheduling queue")
	}

	// 法定人数已满足，允许该 Pod 进入调度队列。
	return nil
}

// checkCPGHierarchyReadiness 检查 Composite Pod Group 层级结构是否已就绪、可以调度。
// 先找到根 Composite Pod Group，再递归遍历整棵树来判断就绪状态。
func (pl *GangScheduling) checkCPGHierarchyReadiness(snapshot fwk.PodGroupManager, namespace, startCPGName string, readinessCountFn func(fwk.PodGroupState) int) *fwk.Status {
	cpgKey := fwk.CompositePodGroupKey(namespace, startCPGName)
	rootKey, ok, err := snapshot.GetRootKeyForGroup(cpgKey)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if !ok {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("failed to build hierarchy snapshot: composite pod group object not found in state for %s", cpgKey.String()))
	}

	if !pl.isCPGTreeReady(snapshot, rootKey.Namespace, rootKey.Name, readinessCountFn) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for composite pod group %q tree to meet quorum", rootKey.Name))
	}
	return nil
}

// isCPGTreeReady 递归判断一棵 Composite Pod Group 子树是否达到法定人数。
// readinessCountFn 决定用“全部 Pod 数”还是“已调度 Pod 数”作为就绪计数。
func (pl *GangScheduling) isCPGTreeReady(snapshot fwk.PodGroupManager, namespace, cpgName string, readinessCountFn func(fwk.PodGroupState) int) bool {
	cpgState, err := snapshot.CompositePodGroupStates().Get(namespace, cpgName)
	if err != nil {
		return false
	}

	cpgSpec, err := snapshot.CompositePodGroups().Get(namespace, cpgName)
	if err != nil {
		return false
	}
	minGroupCount := 1
	policy := cpgSpec.Spec.SchedulingPolicy
	if policy.Gang != nil {
		minGroupCount = int(policy.Gang.MinGroupCount)
	}

	successfulChildren := 0
	for _, childKey := range cpgState.GetChildren() {
		childType, _, childName := childKey.Type, childKey.Namespace, childKey.Name
		if childType == fwk.CompositePodGroupKeyType {
			if pl.isCPGTreeReady(snapshot, namespace, childName, readinessCountFn) {
				successfulChildren++
			}
		} else {
			if pl.isPGReady(snapshot, namespace, childName, readinessCountFn) {
				successfulChildren++
			}
		}
	}

	return successfulChildren >= minGroupCount
}

// isPGReady 判断单个 PodGroup 是否达到 MinCount 要求的就绪状态。
func (pl *GangScheduling) isPGReady(snapshot fwk.PodGroupManager, namespace, pgName string, readinessCountFn func(fwk.PodGroupState) int) bool {
	pg, err := snapshot.PodGroups().Get(namespace, pgName)
	if err != nil {
		return false
	}

	minCount := 1
	if pg.Spec.SchedulingPolicy.Gang != nil {
		minCount = int(pg.Spec.SchedulingPolicy.Gang.MinCount)
	}

	pgState, err := snapshot.PodGroupStates().Get(namespace, pgName)
	if err != nil {
		return false
	}

	return readinessCountFn(pgState) >= minCount
}

// Permit 让 gang 中的所有 Pod 都在此阶段等待。一旦等待中（assumed）的 Pod 数达到 gang 的 MinCount，
// 就同时放行该 gang 的所有 Pod 进入绑定阶段。
func (pl *GangScheduling) Permit(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	if pod.Spec.SchedulingGroup == nil {
		return nil, 0
	}

	logger := klog.FromContext(ctx)
	namespace := pod.Namespace
	schedulingGroup := pod.Spec.SchedulingGroup

	var podGroup *schedulingapi.PodGroup
	var err error
	var snapshot fwk.PodGroupManager

	if pl.isCompositePodGroupEnabled {
		snapshot, err = pl.podGroupManager.BuildHierarchySnapshotFromPod(pod)
		if err != nil {
			return fwk.AsStatus(fmt.Errorf("failed to build hierarchy snapshot: %w", err)), 0
		}
		podGroup, err = snapshot.PodGroups().Get(namespace, *schedulingGroup.PodGroupName)
	} else {
		podGroup, err = pl.snapshotLister.PodGroups().Get(namespace, *schedulingGroup.PodGroupName)
		snapshot = pl.podGroupManager
	}

	if err != nil {
		return fwk.AsStatus(fmt.Errorf("failed to get podGroup %s/%s: %w", namespace, *schedulingGroup.PodGroupName, err)), 0
	}

	if pl.isCompositePodGroupEnabled && podGroup.Spec.ParentCompositePodGroupName != nil {
		return pl.permitPodForHierarchy(logger, snapshot, pod, namespace, *podGroup.Spec.ParentCompositePodGroupName)
	}

	podGroupState, err := snapshot.PodGroupStates().Get(namespace, podGroup.Name)
	if err != nil {
		return fwk.AsStatus(err), 0
	}

	return pl.permitPodGroup(logger, pod, podGroup, podGroupState)
}

// permitPodGroup 处理单个 PodGroup 的 permit 逻辑：未达法定人数则等待并激活未调度 Pod，
// 达到则放行所有 assumed Pod。
func (pl *GangScheduling) permitPodGroup(logger klog.Logger, pod *v1.Pod, podGroup *schedulingapi.PodGroup, podGroupState fwk.PodGroupState) (*fwk.Status, time.Duration) {
	if podGroup.Spec.SchedulingPolicy.Gang == nil {
		return nil, 0
	}

	scheduledPodsCount := podGroupState.ScheduledPodsCount()
	if scheduledPodsCount < int(podGroup.Spec.SchedulingPolicy.Gang.MinCount) {
		// 激活该 PodGroup 中未调度的 Pod：它们可能在等这个 Pod 被调度。
		unscheduledPods := podGroupState.UnscheduledPods()
		pl.handle.Activate(logger, unscheduledPods)
		logger.V(4).Info("Quorum is not met for a gang. Waiting for another pod to allow", "pod", klog.KObj(pod), "schedulingGroup", podGroup.Name, "activatedPods", len(unscheduledPods))
		return fwk.NewStatus(fwk.Wait, "waiting for minCount pods from a gang to be scheduled"), permitTimeoutDuration
	}

	assumedPods := podGroupState.AssumedPods()
	logger.V(4).Info("Quorum is met for a gang. Allowing other pods from a gang waiting on permit", "pod", klog.KObj(pod), "schedulingGroup", podGroup.Name, "allowedPods", len(assumedPods))

	// 法定人数已满足：放行当前 Pod，并通知同一 gang 中所有其他等待中的 Pod 继续。
	for podUID := range assumedPods {
		waitingPod := pl.handle.GetWaitingPod(podUID)
		if waitingPod != nil {
			waitingPod.Allow(Name)
		}
	}

	return nil, 0
}

// permitPodForHierarchy 处理层级结构下 Pod 的 permit 逻辑：检查整棵 CPG 树是否达到法定人数。
func (pl *GangScheduling) permitPodForHierarchy(logger klog.Logger, snapshot fwk.PodGroupManager, pod *v1.Pod, namespace string, startCPGName string) (*fwk.Status, time.Duration) {
	cpgKey := fwk.CompositePodGroupKey(namespace, startCPGName)
	rootKey, ok, err := snapshot.GetRootKeyForGroup(cpgKey)
	if err != nil {
		return fwk.AsStatus(err), 0
	}
	if !ok {
		return fwk.AsStatus(fmt.Errorf("failed to build hierarchy snapshot: composite pod group object not found in state for %s", cpgKey.String())), 0
	}

	if !pl.isCPGTreeReady(snapshot, rootKey.Namespace, rootKey.Name, func(s fwk.PodGroupState) int { return s.ScheduledPodsCount() }) {
		pl.activateUnscheduledPodsInHierarchy(logger, snapshot, rootKey.Namespace, rootKey.Name)
		logger.V(4).Info("Quorum is not met for a CPG hierarchy. Waiting for another pod to allow", "pod", klog.KObj(pod), "rootCPG", rootKey.Name)
		return fwk.NewStatus(fwk.Wait, fmt.Sprintf("waiting for composite pod group %q tree to meet quorum", rootKey.Name)), permitTimeoutDuration
	}

	pl.allowAssumedPodsInHierarchy(snapshot, rootKey.Namespace, rootKey.Name)
	logger.V(4).Info("Quorum is met for a CPG hierarchy. Allowing other pods from the hierarchy waiting on permit", "pod", klog.KObj(pod), "rootCPG", rootKey.Name)
	return nil, 0
}

// activateUnscheduledPodsInHierarchy 递归激活整棵 CPG 树中所有 PodGroup 的未调度 Pod。
func (pl *GangScheduling) activateUnscheduledPodsInHierarchy(logger klog.Logger, snapshot fwk.PodGroupManager, namespace, cpgName string) {
	cpgState, err := snapshot.CompositePodGroupStates().Get(namespace, cpgName)
	if err != nil {
		return
	}
	for _, childKey := range cpgState.GetChildren() {
		childType, _, childName := childKey.Type, childKey.Namespace, childKey.Name
		if childType == fwk.CompositePodGroupKeyType {
			pl.activateUnscheduledPodsInHierarchy(logger, snapshot, namespace, childName)
		} else {
			if pgState, err := snapshot.PodGroupStates().Get(namespace, childName); err == nil {
				unscheduledPods := pgState.UnscheduledPods()
				pl.handle.Activate(logger, unscheduledPods)
			}
		}
	}
}

// allowAssumedPodsInHierarchy 递归放行整棵 CPG 树中所有 PodGroup 的 assumed Pod。
func (pl *GangScheduling) allowAssumedPodsInHierarchy(snapshot fwk.PodGroupManager, namespace, cpgName string) {
	cpgState, err := snapshot.CompositePodGroupStates().Get(namespace, cpgName)
	if err != nil {
		return
	}
	for _, childKey := range cpgState.GetChildren() {
		childType, _, childName := childKey.Type, childKey.Namespace, childKey.Name
		if childType == fwk.CompositePodGroupKeyType {
			pl.allowAssumedPodsInHierarchy(snapshot, namespace, childName)
			continue
		}
		if pgState, err := snapshot.PodGroupStates().Get(namespace, childName); err == nil {
			assumedPods := pgState.AssumedPods()
			for podUID := range assumedPods {
				waitingPod := pl.handle.GetWaitingPod(podUID)
				if waitingPod != nil {
					waitingPod.Allow(Name)
				}
			}
		}
	}
}

// PlacementFeasible 在 PodGroup 调度周期中强制校验 gang 的 MinCount 约束。
// 只有 MinCount 已满足、或 PodGroup 未使用 gang 策略时才返回成功。
// 若剩余 Pod 数量已不足以凑齐 MinCount，则返回 Unschedulable 以提前终止 PodGroup 调度周期。
func (pl *GangScheduling) PlacementFeasible(ctx context.Context, placementCycleState fwk.PlacementCycleState, podGroupInfo fwk.PodGroupInfo, args framework.PlacementProgress) *fwk.Status {
	minCount := getMinCount(podGroupInfo)
	remaining := args.Remaining
	scheduled := args.Scheduled

	if remaining+scheduled < minCount {
		// 剩余 Pod 不足，MinCount 已不可能满足。
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("minCount (%d) cannot be satisfied: %d scheduled, %d remaining", minCount, scheduled, remaining))
	}

	if scheduled < minCount {
		// 继续评估剩余 Pod 后 MinCount 有可能满足，先返回 Wait。
		return fwk.NewStatus(fwk.Wait, fmt.Sprintf("minCount (%d) is not yet satisfied: %d scheduled, %d remaining", minCount, scheduled, remaining))
	}

	// MinCount 已满足。
	return nil
}

// getMinCount 返回 PodGroup 或 Composite Pod Group 的最小计数要求；基本组默认返回 1。
func getMinCount(podGroupInfo fwk.PodGroupInfo) int {
	if podGroupInfo.GetType() == fwk.CompositePodGroupKeyType {
		if podGroupInfo.GetCompositePodGroup().Spec.SchedulingPolicy.Gang == nil {
			return 1
		}
		return int(podGroupInfo.GetCompositePodGroup().Spec.SchedulingPolicy.Gang.MinGroupCount)
	}
	pg := podGroupInfo.GetPodGroup()
	if pg.Spec.SchedulingPolicy.Gang == nil {
		return 1
	}
	return int(pg.Spec.SchedulingPolicy.Gang.MinCount)
}
