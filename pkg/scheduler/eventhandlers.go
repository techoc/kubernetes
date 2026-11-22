/*
Copyright 2019 The Kubernetes Authors.

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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	corev1nodeaffinity "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	resourceslicetracker "k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

// addNodeToCache 处理 Node 新增事件：更新缓存，并唤醒可能因该节点而变得可调度的 Pod。
func (sched *Scheduler) addNodeToCache(obj interface{}) {
	evt := fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
	logger := sched.logger
	node, ok := obj.(*v1.Node)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1.Node", "obj", obj)
		return
	}

	logger.V(3).Info("Add event for node", "node", klog.KObj(node))
	sched.Cache.AddNode(logger, node)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, nil, node, nil)
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	start := time.Now()
	logger := sched.logger
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert oldObj to *v1.Node", "oldObj", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert newObj to *v1.Node", "newObj", newObj)
		return
	}

	logger.V(4).Info("Update event for node", "node", klog.KObj(newNode))
	sched.Cache.UpdateNode(logger, oldNode, newNode)
	events := framework.NodeSchedulingPropertiesChange(newNode, oldNode)

	// 分别记录更新缓存与移动队列的耗时，两者相加即为本次事件的处理延迟。
	updatingDuration := metrics.SinceInSeconds(start)

	// NodeSchedulingPropertiesChange 只返回让节点"更适合调度"的变化事件，
	// 因此只有这些事件才值得唤醒不可调度 Pod。
	for _, evt := range events {
		startMoving := time.Now()
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldNode, newNode, nil)
		movingDuration := metrics.SinceInSeconds(startMoving)

		metrics.EventHandlingLatency.WithLabelValues(evt.Label()).Observe(updatingDuration + movingDuration)
	}
}

// deleteNodeFromCache 处理 Node 删除事件：先通知队列再清理缓存。
func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
	evt := fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Delete}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()

	logger := sched.logger
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1.Node", "obj", t.Obj)
			return
		}
	default:
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1.Node", "obj", t)
		return
	}

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, node, nil, nil)

	logger.V(3).Info("Delete event for node", "node", klog.KObj(node))
	if err := sched.Cache.RemoveNode(logger, node); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Scheduler cache RemoveNode failed")
	}
}

// addPod 处理 Pod 新增事件：未分配的 Pod 入调度队列，已分配的 Pod 进缓存。
func (sched *Scheduler) addPod(obj interface{}) {
	logger := sched.logger
	pod, ok := obj.(*v1.Pod)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1.Pod", "obj", obj)
		return
	}

	isPodDeferred := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && resource.IsPodResizeDeferred(pod)
	if responsibleForPod(pod, sched.Profiles) && (!assignedPod(pod) || isPodDeferred) {
		sched.addPodToSchedulingQueue(pod)
	}
	if assignedPod(pod) {
		sched.addAssignedPodToCache(pod)
	}
}

func (sched *Scheduler) updatePod(oldObj, newObj interface{}) {
	logger := sched.logger
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert oldObj to *v1.Pod", "oldObj", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert newObj to *v1.Pod", "newObj", newObj)
		return
	}

	if assignedPod(oldPod) {
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && responsibleForPod(newPod, sched.Profiles) {
			oldDeferred := resource.IsPodResizeDeferred(oldPod)
			newDeferred := resource.IsPodResizeDeferred(newPod)

			if !oldDeferred && newDeferred {
				// 已分配 Pod 转为延迟扩缩容后，需要重新入队，
				// 让调度器评估其在已分配节点上的可行性并可能触发抢占。
				logger.V(3).Info("Assigned pod transitioned to deferred resize, adding to scheduling queue", "pod", klog.KObj(newPod))
				sched.addPodToSchedulingQueue(newPod)
			} else if oldDeferred && !newDeferred {
				// 不再延迟时出队，停止为它评估抢占。
				logger.V(3).Info("Assigned pod is no longer deferred, removing from scheduling queue", "pod", klog.KObj(newPod))
				sched.deletePodFromSchedulingQueue(newPod, false)
			} else if oldDeferred && newDeferred {
				// 仍处于延迟状态但 spec 变化（例如资源请求更新）时，需同步队列中的副本。
				logger.V(3).Info("Assigned pod spec changed while deferred, updating scheduling queue", "pod", klog.KObj(newPod))
				sched.SchedulingQueue.Update(klog.NewContext(context.Background(), logger), oldPod, newPod)
			}
		}
		sched.updateAssignedPodInCache(oldPod, newPod)
	} else if assignedPod(newPod) {
		// 从"未分配"变为"已分配"意味着绑定完成，按新增已分配 Pod 处理即可。
		sched.addAssignedPodToCache(newPod)
		if responsibleForPod(oldPod, sched.Profiles) {
			// 正常情况下该 Pod 不应还在调度队列；但若它被其他组件绑定，
			// 为保证正确性仍需将其移出队列。参数 true 表示这是绑定事件而非 Pod 从集群删除。
			sched.deletePodFromSchedulingQueue(oldPod, true)
		}
	} else if responsibleForPod(oldPod, sched.Profiles) {
		sched.updatePodInSchedulingQueue(oldPod, newPod)
	}
}

// deletePod 处理 Pod 删除事件，区分已分配与未分配两种状态分别清理。
func (sched *Scheduler) deletePod(obj interface{}) {
	logger := sched.logger
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
		if assignedPod(pod) {
			sched.deleteAssignedPodFromCache(pod)
			if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && resource.IsPodResizeDeferred(pod) && responsibleForPod(pod, sched.Profiles) {
				// 已分配 Pod 通常不在调度队列中，但延迟扩缩容的 Pod 会被临时入队，
				// 从集群删除时必须显式清理。
				sched.deletePodFromSchedulingQueue(pod, false)
			}
		} else if responsibleForPod(pod, sched.Profiles) {
			// 参数 false 表示这是 Pod 从集群删除，而非绑定事件。
			sched.deletePodFromSchedulingQueue(pod, false)
		}
		return
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1.Pod", "obj", t.Obj)
			return
		}
		// DeletedFinalStateUnknown 携带的对象可能已过期，不能用它判断是否已分配，
		// 这里两种清理都尝试执行。
		sched.deleteAssignedPodFromCache(pod)
		if responsibleForPod(pod, sched.Profiles) {
			// 参数 false 表示这是 Pod 从集群删除，而非绑定事件。
			sched.deletePodFromSchedulingQueue(pod, false)
		}
		return
	default:
		utilruntime.HandleErrorWithLogger(logger, nil, "Unable to handle object", "objType", fmt.Sprintf("%T", obj), "obj", obj)
		return
	}
}

// addPodToSchedulingQueue 把未分配的 Pod 加入调度队列，并记录对应事件延迟。
func (sched *Scheduler) addPodToSchedulingQueue(pod *v1.Pod) {
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), framework.EventUnscheduledPodAdd.Label())()

	logger := sched.logger
	logger.V(3).Info("Add event for unscheduled pod", "pod", klog.KObj(pod))
	sched.Cache.AddPodGroupMember(pod)
	sched.SchedulingQueue.Add(klog.NewContext(context.Background(), logger), pod)
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnscheduledPodAdd, nil, pod, nil)
	}
}

// syncPodWithDispatcher 通过 API dispatcher 同步 Pod 信息；同步失败时退回原始 Pod。
func (sched *Scheduler) syncPodWithDispatcher(pod *v1.Pod) *v1.Pod {
	enrichedObj, err := sched.APIDispatcher.SyncObject(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to sync pod %s/%s with API dispatcher: %w", pod.Namespace, pod.Name, err))
		return pod
	}
	enrichedPod, ok := enrichedObj.(*v1.Pod)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("cannot convert enrichedObj of type %T to *v1.Pod", enrichedObj))
		return pod
	}
	return enrichedPod
}

// handleAssumedPodDeletion 处理 assumed Pod（已假定绑定但流程未完成）的删除事件。
// 必须立即从缓存中清除它，否则会一直占用资源、导致其他 Pod 被无谓抢占。
// PreBind/Bind 流程仍会继续，但由于 Pod 已不在 apiserver 和缓存中，最终会失败。
func (sched *Scheduler) handleAssumedPodDeletion(pod *v1.Pod) {
	logger := sched.logger
	// 必须使用缓存中的 Pod 副本：它带有已分配的 NodeName，代表真正被占用的资源。
	assumedPod, err := sched.Cache.GetPod(pod)
	if err != nil {
		// 不算错误：Pod 可能已完成绑定周期并被移出缓存，无需再处理。
		logger.V(5).Info("Assumed pod was already forgotten", "pod", klog.KObj(pod))
		return
	}
	pod = assumedPod

	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		// 理论上不会发生：本调度器只接收指定了匹配 Profile 名字的 Pod。
		utilruntime.HandleErrorWithLogger(logger, err, "Unable to get profile for pod", "pod", klog.KObj(pod))
		return
	}

	// Pod 可能处于两种状态之一：
	// 1. 正在等待 WaitOnPermit：直接拒绝，让调度周期快速优雅失败，
	//    并由 handleBindingCycleError 完成清理。
	if !fwk.RejectWaitingPod(pod.UID) {
		// 2. 已不在等待状态（如 PreBind 或 Bind）：无法快速拒绝，
		//    必须在这里显式移除缓存项以释放假定占用的资源。
		if err := sched.Cache.RemoveAssumedPod(logger, pod); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Scheduler cache RemoveAssumedPod failed", "pod", klog.KObj(pod))
		}
	}

	// 移除 assumed Pod 可能释放了资源，触发 AssignedPodDelete 事件让其他 Pod 有机会被调度。
	// 若被遗忘的 Pod 仍在内存中预留资源，handleBindingCycleError 释放资源后会再次唤醒它们。
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, pod, nil, nil)
}

// updatePodInSchedulingQueue 处理未分配 Pod 的更新事件，并同步缓存与队列状态。
func (sched *Scheduler) updatePodInSchedulingQueue(oldPod, newPod *v1.Pod) {
	start := time.Now()
	logger := sched.logger
	// ResourceVersion 相同说明对象未变，直接跳过；否则重复的 Pod 可能进入调度导致异常（见 #96071）。
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}

	defer metrics.EventHandlingLatency.ObserveSince(start, framework.EventUnscheduledPodUpdate.Label())()
	// 此处不是被抢占目标 Pod 的更新（目标 Pod 通过 SchedulingQueue.Update 更新），故 isTargetPod 传 false。
	for _, evt := range framework.PodSchedulingPropertiesChange(newPod, oldPod, false) {
		if evt.Label() != framework.EventUnscheduledPodUpdate.Label() {
			defer metrics.EventHandlingLatency.ObserveSince(start, evt.Label())()
		}
	}

	if sched.APIDispatcher != nil {
		// 有 API dispatcher 时同步最新 Pod 信息；目前更新后的副本会被丢弃，留待后续版本处理。
		_ = sched.syncPodWithDispatcher(newPod)
	}

	sched.Cache.UpdatePodGroupMember(logger, oldPod, newPod)

	isAssumed, err := sched.Cache.IsAssumedPod(newPod)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to check whether pod is assumed", "pod", klog.KObj(newPod))
	}
	if isAssumed {
		if newPod.DeletionTimestamp != nil && oldPod.DeletionTimestamp == nil {
			// assumed Pod 已开始删除，需要特殊处理，因为无法直接更新它在各个结构中的状态。
			sched.handleAssumedPodDeletion(newPod)
		}
		return
	}

	logger.V(4).Info("Update event for unscheduled pod", "pod", klog.KObj(newPod))
	sched.SchedulingQueue.Update(klog.NewContext(context.Background(), logger), oldPod, newPod)
	if hasNominatedNodeNameChanged(oldPod, newPod) {
		// 提名节点发生变化时，需视作 Pod 从旧提名节点被删除：
		// 调度低优先级或同优先级 Pod 时，这类 Pod 被视为已分配。
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, oldPod, nil, getLEPriorityPreCheck(corev1helpers.PodPriority(oldPod)))
	}
}

// hasNominatedNodeNameChanged 判断 Pod 原本已有提名节点且该提名发生了变化。
func hasNominatedNodeNameChanged(oldPod, newPod *v1.Pod) bool {
	return len(oldPod.Status.NominatedNodeName) > 0 && oldPod.Status.NominatedNodeName != newPod.Status.NominatedNodeName
}

// deletePodFromSchedulingQueue 从调度队列移除 Pod；inBinding 表示移除是否由绑定事件引起。
func (sched *Scheduler) deletePodFromSchedulingQueue(pod *v1.Pod, inBinding bool) {
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), framework.EventUnscheduledPodDelete.Label())()

	logger := sched.logger

	logger.V(3).Info("Delete event for unscheduled pod", "pod", klog.KObj(pod))
	sched.SchedulingQueue.Delete(logger, pod)
	if inBinding {
		// 绑定并非真正的 Pod 删除，后续步骤可跳过：必要的通知由绑定流程负责。
		// 除非是罕见的外部绑定，那种情况下还需通知 assume/nomination 占用的资源已释放，
		// 待 https://github.com/kubernetes/kubernetes/issues/134859 修复后再处理。
		return
	}
	isAssumed, err := sched.Cache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to check whether pod is assumed", "pod", klog.KObj(pod))
	}
	if isAssumed {
		// assumed Pod 被删除，需要特殊处理，因为无法直接从各结构中删除它。
		sched.handleAssumedPodDeletion(pod)
		return
	}
	// 非 assumed Pod 需在此显式清理其 PodGroup 成员状态。
	sched.Cache.RemovePodGroupMember(pod)
	if pod.Status.NominatedNodeName != "" {
		// 带提名节点的 Pod 被删除可能解除对其他 Pod 的阻塞：
		// 调度优先级更低或相等的 Pod 时，它被视为已分配。
		// 注意被提名的 Pod 也可能走 handleAssumedPodDeletion 分支，但那条路径已经唤醒了低优先级 Pod。
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, pod, nil, getLEPriorityPreCheck(corev1helpers.PodPriority(pod)))
	}
}

// getLEPriorityPreCheck 返回一个 PreEnqueueCheck，只选中优先级低于或等于给定值的 Pod。
func getLEPriorityPreCheck(priority int32) queue.PreEnqueueCheck {
	return func(pod *v1.Pod) bool {
		return corev1helpers.PodPriority(pod) <= priority
	}
}

// addAssignedPodToCache 把已分配 Pod 加入缓存，并唤醒可能受影响的待调度 Pod。
func (sched *Scheduler) addAssignedPodToCache(pod *v1.Pod) {
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), framework.EventAssignedPodAdd.Label())()

	logger := sched.logger

	logger.V(3).Info("Add event for scheduled pod", "pod", klog.KObj(pod))
	if err := sched.Cache.AddPod(logger, pod); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Scheduler cache AddPod failed", "pod", klog.KObj(pod))
	}

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodAdd, nil, pod, nil)
}

// updateAssignedPodInCache 更新缓存中的已分配 Pod，并触发调度属性变化对应的事件。
func (sched *Scheduler) updateAssignedPodInCache(oldPod, newPod *v1.Pod) {
	start := time.Now()
	defer metrics.EventHandlingLatency.ObserveSince(start, framework.EventAssignedPodUpdate.Label())()

	logger := sched.logger

	if sched.APIDispatcher != nil {
		// 有 API dispatcher 时同步最新 Pod 信息；目前更新后的副本会被丢弃，留待后续版本处理。
		_ = sched.syncPodWithDispatcher(newPod)
	}

	logger.V(4).Info("Update event for scheduled pod", "pod", klog.KObj(oldPod))
	if err := sched.Cache.UpdatePod(logger, oldPod, newPod); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Scheduler cache UpdatePod failed", "pod", klog.KObj(oldPod))
	}

	// 已分配 Pod 不可能是被抢占目标，故 isTargetPod 传 false。
	events := framework.PodSchedulingPropertiesChange(newPod, oldPod, false)

	// 记录缓存更新耗时，与队列移动耗时相加作为事件处理延迟。
	updatingDuration := metrics.SinceInSeconds(start)

	for _, evt := range events {
		startMoving := time.Now()
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldPod, newPod, nil)
		movingDuration := metrics.SinceInSeconds(startMoving)
		metrics.EventHandlingLatency.WithLabelValues(evt.Label()).Observe(updatingDuration + movingDuration)
	}
}

// deleteAssignedPodFromCache 从缓存移除已分配 Pod，并唤醒可能因此受益的待调度 Pod。
func (sched *Scheduler) deleteAssignedPodFromCache(pod *v1.Pod) {
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), framework.EventAssignedPodDelete.Label())()

	logger := sched.logger

	logger.V(3).Info("Delete event for scheduled pod", "pod", klog.KObj(pod))
	if err := sched.Cache.RemovePod(logger, pod); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Scheduler cache RemovePod failed", "pod", klog.KObj(pod))
	}

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, pod, nil, nil)
}

// assignedPod 判断 Pod 是否已分配到节点（已调度并运行）。
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod 判断 Pod 是否指定了由给定调度器集合中的某个 Profile 来调度。
func responsibleForPod(pod *v1.Pod, profiles profile.Map) bool {
	return profiles.HandlesSchedulerName(pod.Spec.SchedulerName)
}

// addPodGroup 处理 PodGroup 新增事件，更新缓存与队列并唤醒相关 Pod。
func (sched *Scheduler) addPodGroup(obj any) {
	evt := fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Add}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
	logger := sched.logger
	pg, ok := obj.(*schedulingv1beta1.PodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1beta1.PodGroup", "obj", obj)
		return
	}

	logger.V(3).Info("Add event for pod group", "podGroup", klog.KObj(pg))
	sched.Cache.AddPodGroup(pg)
	sched.SchedulingQueue.AddPodGroup(logger, pg)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, nil, pg, nil)
}

// updatePodGroup 处理 PodGroup 更新事件；ResourceVersion 未变则忽略。
func (sched *Scheduler) updatePodGroup(oldObj, newObj any) {
	evt := fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Update}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
	logger := sched.logger
	oldPG, ok := oldObj.(*schedulingv1beta1.PodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert oldObj to *v1beta1.PodGroup", "oldObj", oldObj)
		return
	}
	newPG, ok := newObj.(*schedulingv1beta1.PodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert newObj to *v1beta1.PodGroup", "newObj", newObj)
		return
	}

	if oldPG.ResourceVersion == newPG.ResourceVersion {
		return
	}

	logger.V(4).Info("Update event for pod group", "podGroup", klog.KObj(newPG))
	sched.Cache.UpdatePodGroup(logger, oldPG, newPG)
	sched.SchedulingQueue.UpdatePodGroup(logger, newPG)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldPG, newPG, nil)
}

// deletePodGroup 处理 PodGroup 删除事件，兼容 DeletedFinalStateUnknown 的情况。
func (sched *Scheduler) deletePodGroup(obj any) {
	evt := fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Delete}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()

	logger := sched.logger
	var pg *schedulingv1beta1.PodGroup
	switch t := obj.(type) {
	case *schedulingv1beta1.PodGroup:
		pg = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pg, ok = t.Obj.(*schedulingv1beta1.PodGroup)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1beta1.PodGroup", "obj", t.Obj)
			return
		}
	default:
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1beta1.PodGroup", "obj", t)
		return
	}

	logger.V(3).Info("Delete event for pod group", "podGroup", klog.KObj(pg))
	sched.Cache.RemovePodGroup(logger, pg)
	sched.SchedulingQueue.DeletePodGroup(logger, pg)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, pg, nil, nil)
}

// addCompositePodGroup 处理 CompositePodGroup 新增事件。
func (sched *Scheduler) addCompositePodGroup(obj any) {
	evt := fwk.ClusterEvent{Resource: fwk.CompositePodGroup, ActionType: fwk.Add}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
	logger := sched.logger
	cpg, ok := obj.(*schedulingv1alpha3.CompositePodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1alpha3.CompositePodGroup", "obj", obj)
		return
	}

	logger.V(3).Info("Add event for composite pod group", "compositePodGroup", klog.KObj(cpg))
	sched.Cache.AddCompositePodGroup(logger, cpg)
	sched.SchedulingQueue.AddCompositePodGroup(logger, cpg)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, nil, cpg, nil)
}

// updateCompositePodGroup 处理 CompositePodGroup 更新事件；ResourceVersion 未变则忽略。
func (sched *Scheduler) updateCompositePodGroup(oldObj, newObj any) {
	evt := fwk.ClusterEvent{Resource: fwk.CompositePodGroup, ActionType: fwk.Update}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
	logger := sched.logger
	oldCPG, ok := oldObj.(*schedulingv1alpha3.CompositePodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert oldObj to *v1alpha3.CompositePodGroup", "oldObj", oldObj)
		return
	}
	newCPG, ok := newObj.(*schedulingv1alpha3.CompositePodGroup)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert newObj to *v1alpha3.CompositePodGroup", "newObj", newObj)
		return
	}

	if oldCPG.ResourceVersion == newCPG.ResourceVersion {
		return
	}

	logger.V(4).Info("Update event for composite pod group", "compositePodGroup", klog.KObj(newCPG))
	sched.Cache.UpdateCompositePodGroup(logger, oldCPG, newCPG)
	sched.SchedulingQueue.UpdateCompositePodGroup(logger, newCPG)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldCPG, newCPG, nil)
}

// deleteCompositePodGroup 处理 CompositePodGroup 删除事件。
func (sched *Scheduler) deleteCompositePodGroup(obj any) {
	evt := fwk.ClusterEvent{Resource: fwk.CompositePodGroup, ActionType: fwk.Delete}
	defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()

	logger := sched.logger
	var cpg *schedulingv1alpha3.CompositePodGroup
	switch t := obj.(type) {
	case *schedulingv1alpha3.CompositePodGroup:
		cpg = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		cpg, ok = t.Obj.(*schedulingv1alpha3.CompositePodGroup)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1alpha3.CompositePodGroup", "obj", t.Obj)
			return
		}
	default:
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot convert to *v1alpha3.CompositePodGroup", "obj", t)
		return
	}

	logger.V(3).Info("Delete event for composite pod group", "compositePodGroup", klog.KObj(cpg))
	sched.Cache.RemoveCompositePodGroup(logger, cpg)
	sched.SchedulingQueue.DeleteCompositePodGroup(logger, cpg)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, cpg, nil, nil)
}

const (
	// syncedPollPeriod 是轮询各事件处理器同步状态的间隔。
	syncedPollPeriod = 100 * time.Millisecond
)

// WaitForHandlersSync 等待所有事件处理器完成初始同步。
// 同步成功返回 nil；ctx 取消时返回错误，调用方应据此关闭控制器。
func (sched *Scheduler) WaitForHandlersSync(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, syncedPollPeriod, true, func(ctx context.Context) (done bool, err error) {
		for _, handler := range sched.registeredHandlers {
			if !handler.HasSynced() {
				return false, nil
			}
		}
		return true, nil
	})
}

// addAllEventHandlers 为调度器所需的各类 informer 注册事件处理器，测试与正式代码共用。
func addAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
	dynInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	resourceClaimCache *assumecache.AssumeCache,
	resourceSliceTracker *resourceslicetracker.Tracker,
	draManager fwk.SharedDRAManager,
	gvkMap map[fwk.EventResource]fwk.ActionType,
) error {
	var (
		handlerRegistration cache.ResourceEventHandlerRegistration
		err                 error
		handlers            []cache.ResourceEventHandlerRegistration
	)

	logger := sched.logger

	if handlerRegistration, err = informerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sched.addPod,
		UpdateFunc: sched.updatePod,
		DeleteFunc: sched.deletePod,
	}); err != nil {
		return err
	}
	handlers = append(handlers, handlerRegistration)

	if handlerRegistration, err = informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addNodeToCache,
			UpdateFunc: sched.updateNodeInCache,
			DeleteFunc: sched.deleteNodeFromCache,
		},
	); err != nil {
		return err
	}
	handlers = append(handlers, handlerRegistration)

	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		if handlerRegistration, err = informerFactory.Scheduling().V1beta1().PodGroups().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addPodGroup,
			UpdateFunc: sched.updatePodGroup,
			DeleteFunc: sched.deletePodGroup,
		}); err != nil {
			return err
		}
		handlers = append(handlers, handlerRegistration)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		if handlerRegistration, err = informerFactory.Scheduling().V1alpha3().CompositePodGroups().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addCompositePodGroup,
			UpdateFunc: sched.updateCompositePodGroup,
			DeleteFunc: sched.deleteCompositePodGroup,
		}); err != nil {
			return err
		}
		handlers = append(handlers, handlerRegistration)
	}

	// buildEvtResHandler 按注册的动作类型生成只处理相应事件的处理器，
	// 其唯一动作是把集群变化通知给调度队列。
	buildEvtResHandler := func(at fwk.ActionType, resource fwk.EventResource) cache.ResourceEventHandlerFuncs {
		funcs := cache.ResourceEventHandlerFuncs{}
		if at&fwk.Add != 0 {
			evt := fwk.ClusterEvent{Resource: resource, ActionType: fwk.Add}
			funcs.AddFunc = func(obj interface{}) {
				defer metrics.EventHandlingLatency.ObserveSince(time.Now(), evt.Label())()
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, nil, obj, nil)
			}
		}
		if at&fwk.Update != 0 {
			evt := fwk.ClusterEvent{Resource: resource, ActionType: fwk.Update}
			funcs.UpdateFunc = func(old, obj interface{}) {
				start := time.Now()
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, old, obj, nil)
				metrics.EventHandlingLatency.WithLabelValues(evt.Label()).Observe(metrics.SinceInSeconds(start))
			}
		}
		if at&fwk.Delete != 0 {
			evt := fwk.ClusterEvent{Resource: resource, ActionType: fwk.Delete}
			funcs.DeleteFunc = func(obj interface{}) {
				start := time.Now()
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, obj, nil, nil)
				metrics.EventHandlingLatency.WithLabelValues(evt.Label()).Observe(metrics.SinceInSeconds(start))
			}
		}
		return funcs
	}

	for gvk, at := range gvkMap {
		switch gvk {
		case fwk.Node:
			// Node informer 已在上方注册。
		case fwk.Pod, fwk.AssignedPod, fwk.UnscheduledPod, fwk.TargetPod:
			// Pod informer 已在上方注册；AssignedPod、UnscheduledPod 与 TargetPod
			// 是由 Pod 增删改处理器发出的逻辑事件资源，无需单独注册。
		case fwk.CSINode:
			if handlerRegistration, err = informerFactory.Storage().V1().CSINodes().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.CSINode),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.CSIDriver:
			if handlerRegistration, err = informerFactory.Storage().V1().CSIDrivers().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.CSIDriver),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.CSIStorageCapacity:
			if handlerRegistration, err = informerFactory.Storage().V1().CSIStorageCapacities().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.CSIStorageCapacity),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.PersistentVolume:
			// MaxPDVolumeCountPredicate 依赖 PV 数量，因此需要监听 PV 变化。
			//
			// PV 新增：没有可用 PV 时创建的 Pod 会卡在不可调度队列；而静态供给和延迟绑定
			// 存储类创建的未绑定 PV 不会经过 PV 控制器的动态供给与绑定流程，
			// 也就不会触发重新调度事件，所以必须在 PV 新增时主动唤醒 Pod。
			//
			// PV 更新：PV 被控制器或其他组件更新后，bindVolumesWorker 可能因冲突
			// 无法更新 assumed Pod 的卷绑定，从而把 Pod 移回不可调度队列，
			// 因此 PV 更新时也需要唤醒。
			if handlerRegistration, err = informerFactory.Core().V1().PersistentVolumes().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.PersistentVolume),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.PersistentVolumeClaim:
			// PVC 绑定后会影响 MaxPDVolumeCountPredicate 依赖的 PV 数量。
			if handlerRegistration, err = informerFactory.Core().V1().PersistentVolumeClaims().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.PersistentVolumeClaim),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.ResourceClaim:
			if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
				handlerRegistration = resourceClaimCache.AddEventHandler(
					buildEvtResHandler(at, fwk.ResourceClaim),
				)
				handlers = append(handlers, handlerRegistration)
			}
		case fwk.ResourceSlice:
			if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
				if handlerRegistration, err = resourceSliceTracker.AddEventHandler(
					buildEvtResHandler(at, fwk.ResourceSlice),
				); err != nil {
					return err
				}
				handlers = append(handlers, handlerRegistration)
			}
		case fwk.DeviceClass:
			if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
				handler := cache.ResourceEventHandler(buildEvtResHandler(at, fwk.DeviceClass))
				if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
					// 把缓存更新处理链接在调度器事件处理器之前，
					// 确保调度器因 DeviceClass 事件开始调度前，扩展资源缓存已更新。
					//
					// 这里确定是 DefaultDRAManager 且内部使用 ExtendedResourceCache，无需再做类型检查。
					erCache := draManager.DeviceClassResolver().(*extendedresourcecache.ExtendedResourceCache)
					erCache.AddEventHandler(handler)
					handler = erCache
				}
				if handlerRegistration, err = informerFactory.Resource().V1().DeviceClasses().Informer().AddEventHandler(
					handler,
				); err != nil {
					return err
				}
				handlers = append(handlers, handlerRegistration)
			}
		case fwk.StorageClass:
			if handlerRegistration, err = informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.StorageClass),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.VolumeAttachment:
			if handlerRegistration, err = informerFactory.Storage().V1().VolumeAttachments().Informer().AddEventHandler(
				buildEvtResHandler(at, fwk.VolumeAttachment),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		case fwk.PodGroup, fwk.CompositePodGroup:
			// 已在上方显式注册。
		default:
			// 测试中可能不创建 dynInformerFactory。
			if dynInformerFactory == nil {
				continue
			}
			// 动态资源的 GVK 至少要由点分隔的三段组成：<复数 kind>.<版本>.<组>。
			// 合法示例：foos.v1.example.com、bars.v1beta1.a.b.c。
			// 非法示例：foos.v1（只有两段）、foo.v1.example.com（首段应为复数）。
			if strings.Count(string(gvk), ".") < 2 {
				utilruntime.HandleErrorWithLogger(logger, nil, "Incorrect event registration", "gvk", gvk)
				continue
			}
			// 无法识别的资源退回动态 informer 处理。
			gvr, _ := schema.ParseResourceArg(string(gvk))
			dynInformer := dynInformerFactory.ForResource(*gvr).Informer()
			if handlerRegistration, err = dynInformer.AddEventHandler(
				buildEvtResHandler(at, gvk),
			); err != nil {
				return err
			}
			handlers = append(handlers, handlerRegistration)
		}
	}
	sched.registeredHandlers = handlers
	return nil
}

// AdmissionCheck 调用 noderesources/nodeports/nodeAffinity/nodename 的过滤逻辑并返回失败原因。
// kubelet（pkg/kubelet/lifecycle/predicate.go）与调度器都会使用它。
// includeAllFailures 为 false 时只返回第一个失败原因，否则返回全部。
func AdmissionCheck(pod *v1.Pod, nodeInfo *framework.NodeInfo, includeAllFailures bool) []AdmissionResult {
	var admissionResults []AdmissionResult
	insufficientResources := noderesources.Fits(pod, nodeInfo, nil, noderesources.ResourceRequestsOptions{
		EnablePodLevelResources:           utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
		EnableDRAExtendedResource:         utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource),
		EnableDRANodeAllocatableResources: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
	})
	if len(insufficientResources) != 0 {
		for i := range insufficientResources {
			admissionResults = append(admissionResults, AdmissionResult{InsufficientResource: &insufficientResources[i]})
		}
		if !includeAllFailures {
			return admissionResults
		}
	}

	if matches, _ := corev1nodeaffinity.GetRequiredNodeAffinity(pod).Match(nodeInfo.Node()); !matches {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodeaffinity.Name, Reason: nodeaffinity.ErrReasonPod})
		if !includeAllFailures {
			return admissionResults
		}
	}
	if !nodename.Fits(pod, nodeInfo) {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodename.Name, Reason: nodename.ErrReason})
		if !includeAllFailures {
			return admissionResults
		}
	}
	if !nodeports.Fits(pod, nodeInfo) {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodeports.Name, Reason: nodeports.ErrReason})
		if !includeAllFailures {
			return admissionResults
		}
	}
	return admissionResults
}

// AdmissionResult 描述调度器无法接纳 Pod 的原因。
// 若原因是资源不足，InsufficientResource 字段会给出具体细节。
type AdmissionResult struct {
	// Name 是导致失败的插件名（资源不足时为空）。
	Name string
	// Reason 是失败原因。
	Reason string
	// InsufficientResource 描述资源不足的详情，仅在资源类失败时非空。
	InsufficientResource *noderesources.InsufficientResource
}
