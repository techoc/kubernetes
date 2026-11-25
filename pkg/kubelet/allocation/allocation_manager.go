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

package allocation

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation/state"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/status"
)

// podStatusManagerStateFile 是 status manager 保存其状态的文件名。
const (
	allocatedPodsStateFile = "allocated_pods_state"

	initialRetryDelay = 30 * time.Second
	retryDelay        = 3 * time.Minute

	TriggerReasonPodResized    = "pod_resized"
	TriggerReasonPodUpdated    = "pod_updated"
	TriggerReasonPodsAdded     = "pods_added"
	TriggerReasonPodsRemoved   = "pods_removed"
	TriggerReasonPodTerminated = "pod_terminated"

	triggerReasonPeriodic = "periodic_retry"
)

// AllocationManager 跟踪 Pod 的资源分配。
type Manager interface {
	// GetContainerResourceAllocation 返回该容器对应的 AllocatedResources 值。
	GetContainerResourceAllocation(podUID types.UID, containerName string) (v1.ResourceRequirements, bool)

	// GetPodLevelResourceAllocation 返回该 Pod 级别的 AllocatedResources 值。
	GetPodLevelResourceAllocation(podUID types.UID) (*v1.ResourceRequirements, bool)

	// UpdatePodFromAllocation 用分配信息覆盖 Pod spec。
	// 仅在确有更新需要时才执行深拷贝。
	// 返回更新后（或原）的 Pod，以及是否存在已存储的分配。
	UpdatePodFromAllocation(pod *v1.Pod) (*v1.Pod, bool)

	// SetAllocatedResources 将 Pod 各容器已分配的资源写入检查点。
	SetAllocatedResources(logger klog.Logger, allocatedPod *v1.Pod) error

	// AddPodAdmitHandlers 向分配管理器注册准入处理器。
	// TODO: 评估能否移除该方法，直接在分配管理器构造函数中添加这些处理器。
	AddPodAdmitHandlers(handlers lifecycle.PodAdmitHandlers)

	// AddPod 判断 Pod 能否准入；若能准入则更新分配。
	// 返回值依次为：是否可准入、简短的原因词，以及 Pod 被拒绝时的原因说明。
	// allocatedPods 表示已准入的 Pod 及其已分配（allocated）的资源。
	AddPod(ctx context.Context, activePods []*v1.Pod, pod *v1.Pod) (ok bool, reason, message string)

	// RemovePod 移除指定 Pod UID 的所有已存状态。
	RemovePod(logger klog.Logger, uid types.UID)

	// RemoveOrphanedPods 移除不在剩余 Pod 集合中的 Pod 的已存状态。
	RemoveOrphanedPods(remainingPods sets.Set[types.UID])

	// Run 启动分配管理器。当前仅用于周期性重试待处理的 resize。
	Run(ctx context.Context)

	// PushPendingResize 将存在待处理 resize 请求的 Pod 加入队列，供后续重新评估。
	PushPendingResize(logger klog.Logger, uid types.UID)

	// HasPendingResizes 返回当前是否存在待处理的 resize。
	HasPendingResizes() bool

	// RetryPendingResizes 重试所有待处理的 resize。
	RetryPendingResizes(ctx context.Context, trigger string)

	// HasPodAllocatedResources 返回 Pod 是否已分配资源。
	HasPodAllocatedResources(podUID types.UID) bool

	// GetAllocatedPods 返回所有活跃 Pod 及其已分配的资源。
	GetAllocatedPods() []*v1.Pod
}

// manager 是 AllocationManager 接口的实现，持有分配状态、准入处理器与待处理 resize 队列。
type manager struct {
	allocated state.State

	admitHandlers lifecycle.PodAdmitHandlers
	statusManager status.Manager
	sourcesReady  config.SourcesReady

	ticker         *time.Ticker
	triggerPodSync func(context.Context, *v1.Pod)
	getActivePods  func() []*v1.Pod
	getPodByUID    func(types.UID) (*v1.Pod, bool)

	allocationMutex        sync.Mutex
	podsWithPendingResizes []types.UID

	recorder record.EventRecorderLogger
}

// NewManager 创建将分配状态持久化到 checkpoint 目录的分配管理器。
func NewManager(checkpointDirectory string,
	statusManager status.Manager,
	triggerPodSync func(context.Context, *v1.Pod),
	getActivePods func() []*v1.Pod,
	getPodByUID func(types.UID) (*v1.Pod, bool),
	sourcesReady config.SourcesReady,
	recorder record.EventRecorderLogger,
	logger klog.Logger,
) Manager {
	return &manager{
		allocated: newStateImpl(logger, checkpointDirectory, allocatedPodsStateFile),

		statusManager: statusManager,
		admitHandlers: lifecycle.PodAdmitHandlers{},
		sourcesReady:  sourcesReady,

		ticker:         time.NewTicker(initialRetryDelay),
		triggerPodSync: triggerPodSync,
		getActivePods:  getActivePods,
		getPodByUID:    getPodByUID,
		recorder:       recorder,
	}
}

// newStateImpl 按 InPlacePodVerticalScaling 特性门控选择状态检查点实现；
// 初始化失败属于致命且不可恢复的错误，直接 panic。
func newStateImpl(logger klog.Logger, checkpointDirectory, checkpointName string) state.State {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return state.NewNoopStateCheckpoint()
	}

	stateImpl, err := state.NewStateCheckpoint(logger, checkpointDirectory, checkpointName)
	if err != nil {
		// 这是致命且不可恢复的失败。
		logger.Error(err, "Failed to initialize allocation checkpoint manager",
			"checkpointPath", filepath.Join(checkpointDirectory, checkpointName))
		panic(err)
	}

	return stateImpl
}

// NewInMemoryManager 返回一个不持久化状态的分配管理器。
// 仅供测试使用！
func NewInMemoryManager(
	logger klog.Logger,
	statusManager status.Manager,
	triggerPodSync func(context.Context, *v1.Pod),
	getActivePods func() []*v1.Pod,
	getPodByUID func(types.UID) (*v1.Pod, bool),
	sourcesReady config.SourcesReady,
	recorder record.EventRecorderLogger,
) Manager {
	return &manager{
		allocated: state.NewStateMemory(logger, nil),

		statusManager: statusManager,
		admitHandlers: lifecycle.PodAdmitHandlers{},
		sourcesReady:  sourcesReady,

		ticker:         time.NewTicker(initialRetryDelay),
		triggerPodSync: triggerPodSync,
		getActivePods:  getActivePods,
		getPodByUID:    getPodByUID,
		recorder:       recorder,
	}
}

// Run 启动后台协程，周期性检查并处理待处理的 resize。
func (m *manager) Run(ctx context.Context) {
	// 启动一个协程，周期性检查待处理 resize 并在需要时处理。
	go func() {
		logger := klog.FromContext(ctx)
		for {
			select {
			case <-m.ticker.C:
				successfulResizes := m.retryPendingResizes(ctx, triggerReasonPeriodic)
				for _, po := range successfulResizes {
					logger.Info("Successfully retried resize after timeout", "pod", klog.KObj(po))
				}
			case <-ctx.Done():
				m.ticker.Stop()
				return
			}
		}
	}()
}

// RetryPendingResizes 以指定触发原因重试所有待处理 resize（对外入口，实际逻辑在 retryPendingResizes）。
func (m *manager) RetryPendingResizes(ctx context.Context, trigger string) {
	m.retryPendingResizes(ctx, trigger)
}

// retryPendingResizes 在持有 allocationMutex 的情况下重试所有待处理 resize，
// 返回本次成功完成 resize 的 Pod 列表；仍在等待或失败的 Pod 会重新入队。
func (m *manager) retryPendingResizes(ctx context.Context, trigger string) []*v1.Pod {
	logger := klog.FromContext(ctx)
	m.allocationMutex.Lock()
	defer m.allocationMutex.Unlock()

	if !m.sourcesReady.AllReady() {
		logger.V(4).Info("Skipping evaluation of pending resizes; sources are not ready")
		m.ticker.Reset(initialRetryDelay)
		return nil
	}

	m.ticker.Reset(retryDelay)

	var newPendingResizes []types.UID
	var successfulResizes []*v1.Pod

	// 逐一重试所有待处理的 resize。
	for _, uid := range m.podsWithPendingResizes {
		pod, found := m.getPodByUID(uid)
		if !found {
			logger.V(4).Info("Pod not found; removing from pending resizes", "podUID", uid)
			continue
		}

		oldResizeStatus := m.statusManager.GetPodResizeConditions(uid)
		isDeferred := m.statusManager.IsPodResizeDeferred(uid)

		resizeAllocated, err := m.handlePodResourcesResize(ctx, pod)
		switch {
		case err != nil:
			logger.Error(err, "Failed to handle pod resources resize", "pod", klog.KObj(pod))
			newPendingResizes = append(newPendingResizes, uid)
		case m.statusManager.IsPodResizeDeferred(uid):
			logger.V(4).Info("Pod resize is deferred; will reevaluate later", "pod", klog.KObj(pod))
			newPendingResizes = append(newPendingResizes, uid)
		case m.statusManager.IsPodResizeInfeasible(uid):
			logger.V(4).Info("Pod resize is infeasible", "pod", klog.KObj(pod))
		default:
			logger.V(4).Info("Pod resize successfully allocated", "pod", klog.KObj(pod))
			successfulResizes = append(successfulResizes, pod)
			if isDeferred {
				metrics.PodDeferredAcceptedResizes.WithLabelValues(trigger).Inc()
			}
		}

		// resize 状态发生变化时，需要触发 Pod 状态同步。
		newResizeStatus := m.statusManager.GetPodResizeConditions(uid)
		if resizeAllocated || !apiequality.Semantic.DeepEqual(oldResizeStatus, newResizeStatus) {
			m.triggerPodSync(ctx, pod)
		}
	}

	m.podsWithPendingResizes = newPendingResizes
	return successfulResizes
}

// PushPendingResize 将指定 Pod 加入待处理 resize 队列：队列内去重，并按优先级重新排序。
func (m *manager) PushPendingResize(logger klog.Logger, uid types.UID) {
	m.allocationMutex.Lock()
	defer m.allocationMutex.Unlock()

	for _, p := range m.podsWithPendingResizes {
		if p == uid {
			// 该 Pod 已在待处理 resize 队列中，无需重复添加。
			return
		}
	}

	// 将 Pod 追加进队列并按优先级排序。
	m.podsWithPendingResizes = append(m.podsWithPendingResizes, uid)
	m.sortPendingResizes(logger)
}

// sortPendingResizes 对待处理 resize 队列排序：
// - 首先，优先不增加资源请求的 resize；
// - 其次，按 Pod 的 PriorityClass 排序；
// - 再次，按 Pod 的 QoS 等级排序；
// - 最后，优先处于 deferred 状态时间最长的 resize。
func (m *manager) sortPendingResizes(logger klog.Logger) {
	var pendingPods []*v1.Pod
	for _, uid := range m.podsWithPendingResizes {
		pod, found := m.getPodByUID(uid)
		if !found {
			logger.V(4).Info("Pod not found; removing from pending resizes", "podUID", uid)
			continue
		}
		pendingPods = append(pendingPods, pod)
	}

	slices.SortFunc(pendingPods, func(firstPod, secondPod *v1.Pod) int {
		// 第一优先级：不增加资源请求的 resize，这类 resize 预期总能成功。
		firstPodIncreasing := m.isResizeIncreasingRequests(firstPod)
		secondPodIncreasing := m.isResizeIncreasingRequests(secondPod)
		if !firstPodIncreasing {
			return -1
		}
		if !secondPodIncreasing {
			return 1
		}

		// 第二优先级：PriorityClass 更高的 Pod 优先。
		firstPodPriority := int32(0)
		if firstPod.Spec.Priority != nil {
			firstPodPriority = *firstPod.Spec.Priority
		}
		secondPodPriority := int32(0)
		if secondPod.Spec.Priority != nil {
			secondPodPriority = *secondPod.Spec.Priority
		}
		if firstPodPriority > secondPodPriority {
			return -1
		}
		if secondPodPriority > firstPodPriority {
			return 1
		}

		// 第三优先级：QoS 等级更高的 Pod 优先（Guaranteed > Burstable）。
		// BestEffort QoS 的 Pod 没有资源请求或限制，无需在此考虑。
		firstPodQOS := v1qos.GetPodQOS(firstPod)
		secondPodQOS := v1qos.GetPodQOS(secondPod)
		if firstPodQOS == v1.PodQOSGuaranteed && secondPodQOS != v1.PodQOSGuaranteed {
			return -1
		}
		if secondPodQOS == v1.PodQOSGuaranteed && firstPodQOS != v1.PodQOSGuaranteed {
			return 1
		}

		// 其余条件相同时，等待时间更长的 resize 优先评估。
		// 以 PodResizePending 条件的最后切换时间作为等待时长的依据。
		var firstPodLastTransitionTime *metav1.Time
		firstPodResizeConditions := m.statusManager.GetPodResizeConditions(firstPod.UID)
		for _, c := range firstPodResizeConditions {
			if c.Type == v1.PodResizePending {
				firstPodLastTransitionTime = &c.LastTransitionTime
			}
		}
		var secondPodLastTransitionTime *metav1.Time
		secondPodResizeConditions := m.statusManager.GetPodResizeConditions(secondPod.UID)
		for _, c := range secondPodResizeConditions {
			if c.Type == v1.PodResizePending {
				secondPodLastTransitionTime = &c.LastTransitionTime
			}
		}
		if firstPodLastTransitionTime == nil {
			return 1
		}
		if secondPodLastTransitionTime == nil {
			return -1
		}
		if firstPodLastTransitionTime.Before(secondPodLastTransitionTime) {
			return -1
		}
		return 1
	})

	m.podsWithPendingResizes = make([]types.UID, len(pendingPods))
	for i, pod := range pendingPods {
		m.podsWithPendingResizes[i] = pod.UID
	}
}

// isResizeIncreasingRequests 返回资源请求是否出现增长（CPU 或内存任一增加即为 true）。
func (m *manager) isResizeIncreasingRequests(pod *v1.Pod) bool {
	allocatedPod, updated := m.UpdatePodFromAllocation(pod)
	if !updated {
		return false
	}

	opts := resourcehelper.PodResourcesOptions{
		SkipPodLevelResources: !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	}
	oldRequest := resourcehelper.PodRequests(allocatedPod, opts)
	newRequest := resourcehelper.PodRequests(pod, opts)

	return newRequest.Memory().Cmp(*oldRequest.Memory()) > 0 ||
		newRequest.Cpu().Cmp(*oldRequest.Cpu()) > 0
}

// HasPendingResizes 在锁保护下返回待处理 resize 队列是否非空。
func (m *manager) HasPendingResizes() bool {
	m.allocationMutex.Lock()
	defer m.allocationMutex.Unlock()

	return len(m.podsWithPendingResizes) > 0
}

// GetContainerResourceAllocation 返回最近一次写入检查点的 AllocatedResources 值。
// 若检查点管理器尚未初始化，返回 nil 与 false。
func (m *manager) GetContainerResourceAllocation(podUID types.UID, containerName string) (v1.ResourceRequirements, bool) {
	return m.allocated.GetContainerResources(podUID, containerName)
}

// GetPodLevelResourceAllocation 返回最近一次写入检查点的 AllocatedResources 值。
// 若检查点管理器尚未初始化，返回 nil 与 false。
func (m *manager) GetPodLevelResourceAllocation(podUID types.UID) (*v1.ResourceRequirements, bool) {
	return m.allocated.GetPodLevelResources(podUID)
}

// UpdatePodFromAllocation 用分配信息覆盖 Pod spec。
// 仅在确有更新需要时才执行深拷贝。
func (m *manager) UpdatePodFromAllocation(pod *v1.Pod) (*v1.Pod, bool) {
	if pod == nil {
		return pod, false
	}

	allocated, ok := m.allocated.GetPodResourceInfo(pod.UID)
	if !ok {
		return pod, false
	}

	return updatePodFromAllocation(pod, allocated)
}

// updatePodFromAllocation 按特性门控用分配信息覆写 Pod 的各类资源字段，返回是否发生了实际更新。
func updatePodFromAllocation(pod *v1.Pod, allocated state.PodResourceInfo) (*v1.Pod, bool) {
	if pod == nil {
		return pod, false
	}

	updated := false
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		pod, updated = updatePodLevelResourcesFromAllocation(pod, allocated)
	}
	pod, updated = updateContainerResourcesFromAllocation(pod, allocated, updated)
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		pod, updated = updateEmptyDirVolumeLimitsFromAllocation(pod, allocated, updated)
	}

	return pod, updated
}

// updateContainerResourcesFromAllocation 用分配信息覆写普通容器与 init 容器的资源，
// 仅在首次需要修改时才对 Pod 做一次深拷贝。
func updateContainerResourcesFromAllocation(pod *v1.Pod, allocated state.PodResourceInfo, alreadyUpdated bool) (*v1.Pod, bool) {
	updated := alreadyUpdated
	containerAlloc := func(c v1.Container) (v1.ResourceRequirements, bool) {
		if cAlloc, ok := allocated.ContainerResources[c.Name]; ok {
			if !apiequality.Semantic.DeepEqual(c.Resources, cAlloc) {
				// 分配与 Pod spec 不一致，取用分配值。
				if !updated {
					// 若这是首个待执行的更新，先深拷贝 Pod。
					pod = pod.DeepCopy()
					updated = true
				}
				return cAlloc, true
			}
		}
		return v1.ResourceRequirements{}, false
	}

	for i, c := range pod.Spec.Containers {
		if cAlloc, found := containerAlloc(c); found {
			// 分配与 Pod spec 不一致，执行更新。
			pod.Spec.Containers[i].Resources = cAlloc
		}
	}
	for i, c := range pod.Spec.InitContainers {
		if cAlloc, found := containerAlloc(c); found {
			// 分配与 Pod spec 不一致，执行更新。
			pod.Spec.InitContainers[i].Resources = cAlloc
		}
	}
	return pod, updated
}

// updatePodLevelResourcesFromAllocation 用分配信息覆写 Pod 级别的资源字段。
func updatePodLevelResourcesFromAllocation(pod *v1.Pod, allocated state.PodResourceInfo) (*v1.Pod, bool) {
	pAlloc := allocated.PodLevelResources
	if !apiequality.Semantic.DeepEqual(pod.Spec.Resources, pAlloc) {
		// 分配与 Pod spec 不一致，取用分配值并深拷贝 Pod。
		pod = pod.DeepCopy()
		pod.Spec.Resources = pAlloc
		return pod, true
	}
	return pod, false
}

// updateEmptyDirVolumeLimitsFromAllocation 用分配信息覆写内存型 emptyDir 卷的大小上限。
func updateEmptyDirVolumeLimitsFromAllocation(pod *v1.Pod, allocated state.PodResourceInfo, alreadyUpdated bool) (*v1.Pod, bool) {
	updated := alreadyUpdated
	for i, vol := range pod.Spec.Volumes {
		if !VolHasMemoryBackedEmptyDirSizeLimit(&vol) {
			continue
		}
		if alloc, ok := allocated.EmptyDirVolumeLimits[vol.Name]; ok {
			if alloc.Cmp(*vol.EmptyDir.SizeLimit) != 0 {
				if !updated {
					pod = pod.DeepCopy()
					updated = true
				}
				allocCopy := alloc.DeepCopy()
				pod.Spec.Volumes[i].EmptyDir.SizeLimit = &allocCopy
			}
		}
	}
	return pod, updated
}

// HasPodAllocatedResources 返回 Pod 是否已分配资源。
func (m *manager) HasPodAllocatedResources(podUID types.UID) bool {
	_, allocated := m.allocated.GetPodResourceInfo(podUID)
	return allocated
}

// SetAllocatedResources 将 Pod 各容器已分配的资源写入检查点。
func (m *manager) SetAllocatedResources(logger klog.Logger, pod *v1.Pod) error {
	return m.allocated.SetPodResourceInfo(logger, pod.UID, ResourceInfoForPod(pod))
}

// ResourceInfoForPod 构造给定 Pod 的 state.PodResourceInfo，包含容器资源、
// 内存型 emptyDir 卷上限以及 Pod 级资源。
func ResourceInfoForPod(pod *v1.Pod) state.PodResourceInfo {
	var podAlloc state.PodResourceInfo
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) && pod.Spec.Resources != nil {
		podAlloc.PodLevelResources = pod.Spec.Resources.DeepCopy()
	}
	podAlloc.ContainerResources = make(map[string]v1.ResourceRequirements)
	for container, containerType := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers|podutil.Containers) {
		if !IsResizableContainer(container, containerType) {
			continue
		}
		alloc := *container.Resources.DeepCopy()
		podAlloc.ContainerResources[container.Name] = alloc
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		for _, vol := range pod.Spec.Volumes {
			if !VolHasMemoryBackedEmptyDirSizeLimit(&vol) {
				continue
			}
			alloc := vol.EmptyDir.SizeLimit.DeepCopy()
			if podAlloc.EmptyDirVolumeLimits == nil {
				podAlloc.EmptyDirVolumeLimits = make(map[string]*resource.Quantity)
			}
			podAlloc.EmptyDirVolumeLimits[vol.Name] = &alloc
		}
	}

	return podAlloc
}

// AddPodAdmitHandlers 逐个注册传入的准入处理器。
func (m *manager) AddPodAdmitHandlers(handlers lifecycle.PodAdmitHandlers) {
	for _, a := range handlers {
		m.admitHandlers.AddPodAdmitHandler(a)
	}
}

// AddPod 在锁保护下判断 Pod 能否准入；准入成功后写入资源分配检查点。
func (m *manager) AddPod(ctx context.Context, activePods []*v1.Pod, pod *v1.Pod) (bool, string, string) {
	logger := klog.FromContext(ctx)
	m.allocationMutex.Lock()
	defer m.allocationMutex.Unlock()

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		// 为处理 kubelet 重启场景，用检查点中存储的 AllocatedResources（cpu 与 memory）判断准入；
		// 若检查点中已有数据，它即为权威来源。
		pod, _ = m.UpdatePodFromAllocation(pod)
	}

	// 判断 Pod 是否可准入，可准入则更新分配。
	allocatedPods := m.getAllocatedPods(activePods)
	ok, reason, message := m.canAdmitPod(ctx, allocatedPods, pod, lifecycle.AddOperation)

	if ok && utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		// 将 Pod 准入或 resize 时的资源值写入检查点。
		if err := m.SetAllocatedResources(logger, pod); err != nil {
			// TODO(vinaykul,InPlacePodVerticalScaling): 是否有办法从该失败中恢复？待调研。
			logger.Error(err, "SetPodAllocation failed", "pod", klog.KObj(pod))
		}
	}

	return ok, reason, message
}

// RemovePod 移除指定 Pod 的分配状态。
func (m *manager) RemovePod(logger klog.Logger, uid types.UID) {
	if err := m.allocated.RemovePod(logger, uid); err != nil {
		// 删除失败会由 RemoveOrphanedPods 重试，因此可以安全忽略该错误。
		logger.V(3).Info("Failed to delete pod allocation", "podUID", uid, "err", err)
	}
}

// RemoveOrphanedPods 清理不属于剩余 Pod 集合的孤儿分配状态。
func (m *manager) RemoveOrphanedPods(remainingPods sets.Set[types.UID]) {
	m.allocated.RemoveOrphanedPods(remainingPods)
}

// handlePodResourcesResize 判断能否把分配更新为期望资源：无需更新时清除 pending 条件；
// 可以更新时写入检查点并更新状态条件；不可行时记录 pending 条件并上报对应事件。
func (m *manager) handlePodResourcesResize(ctx context.Context, pod *v1.Pod) (bool, error) {
	logger := klog.FromContext(ctx)
	_, updated := m.UpdatePodFromAllocation(pod)
	if !updated {
		// 期望资源等于已分配资源，无需更新分配。
		m.statusManager.ClearPodResizePendingCondition(pod.UID, metrics.DeferredResizeResolutionReverted)
		return false, nil
	}

	// 期望资源与已分配资源不一致，尝试把分配更新为期望值。
	fit, reason, message := m.canAdmitPod(ctx, m.getAllocatedPods(m.getActivePods()), pod, lifecycle.ResizeOperation)
	if fit {
		// 更新 Pod 资源分配检查点。
		if err := m.SetAllocatedResources(logger, pod); err != nil {
			return false, err
		}
		m.statusManager.ClearPodResizePendingCondition(pod.UID, metrics.DeferredResizeResolutionAccepted)

		// 清除上一次 resize 可能残留的错误，并更新 resize in-progress 条件的 generation。
		m.statusManager.ClearPodResizeInProgressCondition(pod.UID)
		m.statusManager.SetPodResizeInProgressCondition(pod.UID, "", "", pod.Generation)

		msg := events.PodResizeStartedMsg(logger, pod, pod.Generation)
		m.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeNormal, events.ResizeStarted, "%s", msg)
		return true, nil
	}

	if reason != "" {
		if m.statusManager.SetPodResizePendingCondition(pod.UID, reason, message, pod.Generation) {
			eventType := events.ResizeDeferred
			if reason == v1.PodReasonInfeasible {
				eventType = events.ResizeInfeasible
			}
			msg := events.PodResizePendingMsg(logger, pod, reason, message, pod.Generation)
			m.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, eventType, "%s", msg)
		}
	}

	return false, nil
}

// canAdmitPod 判断 Pod 能否准入，不能准入时给出原因。其中 "pod" 是新 Pod，"pods" 是所有已准入的 Pod。
// 返回值依次为：是否可准入、简短的原因词，以及 Pod 被拒绝时的原因说明。
// allocatedPods 表示已准入的 Pod 及其已分配（allocated）的资源。
func (m *manager) canAdmitPod(ctx context.Context, allocatedPods []*v1.Pod, pod *v1.Pod, operation lifecycle.Operation) (bool, string, string) {
	logger := klog.FromContext(ctx)
	// 排除正在被评估的 Pod。
	allocatedPods = slices.DeleteFunc(allocatedPods, func(p *v1.Pod) bool { return p.UID == pod.UID })

	// 任一处理器拒绝，则该 Pod 被拒绝。
	attrs := &lifecycle.PodAdmitAttributes{Pod: pod, OtherPods: allocatedPods, Operation: operation}
	for _, podAdmitHandler := range m.admitHandlers {
		if result := podAdmitHandler.Admit(ctx, attrs); !result.Admit {
			logger.Info("Pod admission denied", "podUID", attrs.Pod.UID, "pod", klog.KObj(attrs.Pod), "reason", result.Reason, "message", result.Message, "operation", operation)
			return false, result.Reason, result.Message
		}
	}

	return true, "", ""
}

// getAllocatedPods 过滤出已有分配记录的 Pod 并用分配值覆写其资源；
// 未开启 InPlacePodVerticalScaling 时直接返回原始活跃 Pod 列表。
func (m *manager) getAllocatedPods(activePods []*v1.Pod) []*v1.Pod {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return activePods
	}

	allocatedPods := make([]*v1.Pod, 0, len(activePods))
	for _, pod := range activePods {
		// 过滤掉尚无分配记录的 Pod，这些 Pod 在准入阶段大概率会被拒绝。
		if m.HasPodAllocatedResources(pod.UID) {
			allocatedPod, _ := m.UpdatePodFromAllocation(pod)
			allocatedPods = append(allocatedPods, allocatedPod)
		}
	}
	return allocatedPods
}

// GetAllocatedPods 返回所有已分配资源并完成覆写的活跃 Pod。
func (m *manager) GetAllocatedPods() []*v1.Pod {
	return m.getAllocatedPods(m.getActivePods())
}

// IsResizableContainer 判断指定类型的容器是否支持原地 resize。
func IsResizableContainer(container *v1.Container, containerType podutil.ContainerType) bool {
	switch containerType {
	case podutil.InitContainers:
		return utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingInitContainers) || podutil.IsRestartableInitContainer(container)
	case podutil.Containers:
		return true
	default:
		return false
	}
}

// VolHasMemoryBackedEmptyDirSizeLimit 判断卷是否为设置了非零 SizeLimit 的内存型 emptyDir。
func VolHasMemoryBackedEmptyDirSizeLimit(vol *v1.Volume) bool {
	return vol != nil && vol.EmptyDir != nil && vol.EmptyDir.Medium == v1.StorageMediumMemory && vol.EmptyDir.SizeLimit != nil && !vol.EmptyDir.SizeLimit.IsZero()
}
