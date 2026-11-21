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

//go:generate mockery
package status

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clientset "k8s.io/client-go/kubernetes"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	kubeutil "k8s.io/kubernetes/pkg/kubelet/util"
	statusutil "k8s.io/kubernetes/pkg/util/pod"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// versionedPodStatus 包装 v1.PodStatus 并附带版本号，确保过期（stale）的
// Pod 状态不会被发送到 API server。
type versionedPodStatus struct {
	// version 是每个 Pod 单调递增的版本号，用于比较状态新旧。
	version uint64
	// Pod 名称与命名空间，用于向 API server 发送更新。
	podName      string
	podNamespace string
	// at 是检测到最近一次状态更新的时间，用于统计状态同步延迟。
	at time.Time

	// 当状态是在 SyncTerminatedPod 结束时（或之后）生成时为 true。
	podIsFinished bool

	status v1.PodStatus
}

// manager 负责把 Pod 状态同步到 apiserver，仅在状态发生变化时写入。
// 所有方法都是线程安全的。
type manager struct {
	kubeClient clientset.Interface
	podManager PodManager
	// 从 Pod UID 到对应 Pod 同步状态的映射。
	podStatuses         map[types.UID]versionedPodStatus
	podResizeConditions map[types.UID]podResizeConditions
	podStatusesLock     sync.RWMutex
	podStatusChannel    chan struct{}
	// 从（mirror）Pod UID 到最近一次成功发送到 API server 的状态版本的映射。
	// apiStatusVersions 只能由同步线程访问，避免额外的锁开销。
	apiStatusVersions map[kubetypes.MirrorPodUID]uint64
	podDeletionSafety PodDeletionSafetyProvider

	podStartupLatencyHelper PodStartupLatencyStateHelper
	notifiers               []PodUpdateNotifier
}

// podResizeConditions 保存某个 Pod 当前的 PodResizePending 与 PodResizeInProgress 条件。
type podResizeConditions struct {
	PodResizePending    *v1.PodCondition
	PodResizeInProgress *v1.PodCondition
}

// List 按固定顺序返回已设置的条件列表，便于调用方稳定地遍历与比较。
func (prc podResizeConditions) List() []*v1.PodCondition {
	var conditions []*v1.PodCondition
	if prc.PodResizePending != nil {
		conditions = append(conditions, prc.PodResizePending)
	}
	if prc.PodResizeInProgress != nil {
		conditions = append(conditions, prc.PodResizeInProgress)
	}
	return conditions
}

// PodManager 是 manager 观察 kubelet 实际状态所需的方法子集。
// 各方法的 godoc 参见 pkg/k8s.io/kubernetes/pkg/kubelet/pod.Manager。
type PodManager interface {
	GetPodByUID(types.UID) (*v1.Pod, bool)
	GetMirrorPodByPod(*v1.Pod) (*v1.Pod, bool)
	TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID
	GetUIDTranslations() (podToMirror map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID, mirrorToPod map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID)
}

// PodStatusProvider 知道如何提供 Pod 状态，供需要查询 Pod 权威状态的其他组件使用。
// 它代表 kubelet 视角下运行中 Pod 的实际状态。
type PodStatusProvider interface {
	// GetPodStatus 返回指定 Pod UID 的缓存状态，以及是否为缓存命中。
	GetPodStatus(uid types.UID) (v1.PodStatus, bool)
}

// PodDeletionSafetyProvider 提供 Pod 可安全删除的保证。
type PodDeletionSafetyProvider interface {
	// PodCouldHaveRunningContainers 在 Pod 仍可能有运行中容器时返回 true。
	PodCouldHaveRunningContainers(pod *v1.Pod) bool
}

// PodStartupLatencyStateHelper 负责记录 Pod 启动延迟相关的状态。
type PodStartupLatencyStateHelper interface {
	RecordStatusUpdated(logger klog.Logger, pod *v1.Pod)
	DeletePodStartupState(podUID types.UID)
}

// PodUpdateNotifier 是接收 Pod 状态更新通知的接口。
type PodUpdateNotifier interface {
	// OnPodUpdated 在 Pod 状态更新时被调用。
	OnPodUpdated(logger klog.Logger, pod *v1.Pod, status v1.PodStatus, isAdded bool)
	// OnPodRemoved 在 Pod 终止时被调用。
	OnPodRemoved(logger klog.Logger, pod *v1.Pod)
}

// podStatusNotification 是发送给订阅者的一次状态变更通知。
type podStatusNotification struct {
	pod           *v1.Pod
	status        v1.PodStatus
	isAdded       bool
	podIsFinished bool
}

// Manager 是 kubelet Pod 状态的事实来源（source of truth），应始终保持与最新的
// v1.PodStatus 一致，同时负责把更新同步回 API server。
type Manager interface {
	PodStatusProvider

	// Start 启动与 API server 的状态同步循环。
	Start(ctx context.Context)

	// AddPodUpdateNotifier 添加一个接收 Pod 状态更新的订阅者。
	AddPodUpdateNotifier(notifier PodUpdateNotifier)

	// SetPodStatus 缓存并更新指定 Pod 的状态，同时触发一次状态更新。
	SetPodStatus(logger klog.Logger, pod *v1.Pod, status v1.PodStatus) (v1.PodStatus, bool)

	// SetContainerReadiness 用给定的就绪状态更新缓存的容器状态，并触发状态更新。
	SetContainerReadiness(logger klog.Logger, podUID types.UID, containerID kubecontainer.ContainerID, ready bool)

	// SetPodVolumeHealth 更新指定 Pod 上某个卷的 VolumeHealth 缓存条目。
	// conditions 是该卷完整的期望条件集合（为空表示健康）。若缓存状态发生变化则返回 true。
	SetPodVolumeHealth(logger klog.Logger, podUID types.UID, volumeName string, conditions []v1.VolumeHealthCondition) bool

	// SetContainerStartup 用给定的启动状态更新缓存的容器状态，并触发状态更新。
	SetContainerStartup(logger klog.Logger, podUID types.UID, containerID kubecontainer.ContainerID, started bool)

	// TerminatePod 把指定 Pod 的容器状态重置为 terminated，并触发状态更新。
	TerminatePod(logger klog.Logger, pod *v1.Pod)

	// RemoveOrphanedStatuses 扫描状态缓存，删除不在给定 podUIDs 中的 Pod 条目。
	RemoveOrphanedStatuses(logger klog.Logger, podUIDs map[types.UID]bool)

	// GetPodResizeConditions 返回缓存的 PodStatus Resize 条件值。
	GetPodResizeConditions(podUID types.UID) []*v1.PodCondition

	// SetPodResizePendingCondition 缓存该 Pod 最近一次的 PodResizePending 条件。
	// 若条件的 reason 或 observedGeneration 因本次调用而改变则返回 true。
	SetPodResizePendingCondition(podUID types.UID, reason, message string, observedGeneration int64) bool

	// SetPodResizeInProgressCondition 缓存该 Pod 最近一次的 PodResizeInProgress 条件。
	// 条件已存在时不会更新 observedGeneration，也不允许清空 reason 或 message。
	// 若条件因本次调用而改变则返回 true，同时返回该条件的 observedGeneration。
	SetPodResizeInProgressCondition(podUID types.UID, reason, message string, observedGeneration int64) (int64, bool)

	// ClearPodResizePendingCondition 从缓存中清除该 Pod 的 PodResizePending 条件。
	ClearPodResizePendingCondition(podUID types.UID, resolution metrics.DeferredResizeResolution)

	// ClearPodResizeInProgressCondition 从缓存中清除该 Pod 的 PodResizeInProgress 条件。
	// 若条件被清除，返回被清除条件的 observedGeneration 和 true；否则返回零值和 false。
	ClearPodResizeInProgressCondition(podUID types.UID) (int64, bool)

	// IsPodResizeDeferred 在当前 Pod 扩容被推迟（deferred）时返回 true。
	IsPodResizeDeferred(podUID types.UID) bool

	// IsPodResizeInfeasible 在当前 Pod 扩容不可行（infeasible）时返回 true。
	IsPodResizeInfeasible(podUID types.UID) bool

	// BackfillPodResizeConditions 通过读取给定 Pod 的状态，回填 status manager 的 resize 条件。
	BackfillPodResizeConditions(pods []*v1.Pod)
}

// syncPeriod 是全量状态对账的周期。周期性全量同步用于在单次更新失败后自愈，
// 因此无需对每次失败做额外重试。
const syncPeriod = 10 * time.Second

// NewManager 返回一个可用的 Manager 实现。
func NewManager(kubeClient clientset.Interface, podManager PodManager, podDeletionSafety PodDeletionSafetyProvider, podStartupLatencyHelper PodStartupLatencyStateHelper) Manager {
	// 容量为 1 的带缓冲 channel 充当“有更新待同步”的合并信号：
	// 同步线程未消费前，多次写入只会保留一个待处理信号。
	return &manager{
		kubeClient:              kubeClient,
		podManager:              podManager,
		podStatuses:             make(map[types.UID]versionedPodStatus),
		podResizeConditions:     make(map[types.UID]podResizeConditions),
		podStatusChannel:        make(chan struct{}, 1),
		apiStatusVersions:       make(map[kubetypes.MirrorPodUID]uint64),
		podDeletionSafety:       podDeletionSafety,
		podStartupLatencyHelper: podStartupLatencyHelper,
	}
}

// isPodStatusByKubeletEqual 判断两个 Pod 状态是否相等，忽略不由 kubelet 管理的字段
// （包括非 kubelet 拥有的 Pod 条件、ResourceClaimStatuses、ExtendedResourceClaimStatus
// 以及 NodeAllocatableResourceClaimStatuses）。调用前假定状态已做过归一化。
func isPodStatusByKubeletEqual(oldStatus, status *v1.PodStatus) bool {
	oldCopy := oldStatus.DeepCopy()

	// 只比较 kubelet 拥有或共享的条件，避免与其他组件写入的条件相互干扰。
	newConditions := make(map[v1.PodConditionType]*v1.PodCondition, len(status.Conditions))
	oldConditions := make(map[v1.PodConditionType]*v1.PodCondition, len(oldStatus.Conditions))
	for _, c := range status.Conditions {
		if kubetypes.PodConditionByKubelet(c.Type) || kubetypes.PodConditionSharedByKubelet(c.Type) {
			newConditions[c.Type] = &c
		}
	}
	for _, c := range oldStatus.Conditions {
		if kubetypes.PodConditionByKubelet(c.Type) || kubetypes.PodConditionSharedByKubelet(c.Type) {
			oldConditions[c.Type] = &c
		}
	}

	if len(newConditions) != len(oldConditions) {
		return false
	}
	for _, newCondition := range newConditions {
		oldCondition := oldConditions[newCondition.Type]
		if oldCondition == nil || oldCondition.Status != newCondition.Status || oldCondition.Message != newCondition.Message || oldCondition.Reason != newCondition.Reason {
			return false
		}
	}

	oldCopy.Conditions = status.Conditions
	// ResourceClaimStatuses 不由 kubelet 拥有或修改。
	oldCopy.ResourceClaimStatuses = status.ResourceClaimStatuses
	// ExtendedResourceClaimStatus 不由 kubelet 拥有或修改。
	oldCopy.ExtendedResourceClaimStatus = status.ExtendedResourceClaimStatus
	// NodeAllocatableResourceClaimStatuses 不由 kubelet 拥有或修改。
	oldCopy.NodeAllocatableResourceClaimStatuses = status.NodeAllocatableResourceClaimStatuses

	return apiequality.Semantic.DeepEqual(oldCopy, status)
}

// Start 启动状态同步循环，直到 ctx 取消。
func (m *manager) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 没有 client 时不启动 status manager。这种情况会出现在 master 节点上，
	// 那里由 kubelet 负责引导 master 组件的 Pod。
	if m.kubeClient == nil {
		logger.Info("Kubernetes client is nil, not starting status manager")
		return
	}

	logger.Info("Starting to sync pod status with apiserver")

	//nolint:staticcheck // SA1015 Ticker can leak since this is only called once and doesn't handle termination.
	syncTicker := time.NewTicker(syncPeriod).C

	// syncPod 与 syncBatch 共用同一个 goroutine，避免同步过程中的竞态。
	go wait.Forever(func() {
		for {
			select {
			case <-m.podStatusChannel:
				logger.V(4).Info("Syncing updated statuses")
				m.syncBatch(ctx, false)
			case <-syncTicker:
				logger.V(4).Info("Syncing all statuses")
				// 周期性全量同步：顺带清理孤儿版本并触发不一致状态的对账。
				m.syncBatch(ctx, true)
			}
		}
	}, 0)
}

// AddPodUpdateNotifier 注册一个状态更新订阅者。
func (m *manager) AddPodUpdateNotifier(notifier PodUpdateNotifier) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()
	m.notifiers = append(m.notifiers, notifier)
}

// GetPodResizeConditions 返回最近一次缓存的 Resize 条件。
func (m *manager) GetPodResizeConditions(podUID types.UID) []*v1.PodCondition {
	m.podStatusesLock.RLock()
	defer m.podStatusesLock.RUnlock()
	return m.podResizeConditions[podUID].List()
}

// SetPodResizePendingCondition 缓存该 Pod 最近一次的 PodResizePending 条件。
// 若条件的 reason 或 observedGeneration 因本次调用而改变则返回 true。
func (m *manager) SetPodResizePendingCondition(podUID types.UID, reason, message string, observedGeneration int64) bool {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	previousCondition := m.podResizeConditions[podUID].PodResizePending

	// 除“不可行”外的所有 pending 原因都归一化为 Deferred，
	// 这样条件 reason 只反映 kubelet 对外的两种稳定语义。
	if reason != v1.PodReasonInfeasible {
		// 其他所有“pending”原因都设置为 Deferred。
		reason = v1.PodReasonDeferred
	}

	m.podResizeConditions[podUID] = podResizeConditions{
		PodResizePending:    updatedPodResizeCondition(v1.PodResizePending, m.podResizeConditions[podUID].PodResizePending, reason, message, observedGeneration),
		PodResizeInProgress: m.podResizeConditions[podUID].PodResizeInProgress,
	}

	if previousCondition == nil {
		m.observePendingResizeCount()
		return true
	} else {
		return previousCondition.Reason != reason || previousCondition.ObservedGeneration != observedGeneration
	}
}

// SetPodResizeInProgressCondition 缓存该 Pod 最近一次的 PodResizeInProgress 条件。
// 条件已存在时不会更新 observedGeneration，也不允许清空 reason 或 message。
// 若条件因本次调用而改变则返回 true，同时返回该条件的 observedGeneration。
func (m *manager) SetPodResizeInProgressCondition(podUID types.UID, reason, message string, observedGeneration int64) (int64, bool) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	previousCondition := m.podResizeConditions[podUID].PodResizeInProgress
	if c := m.podResizeConditions[podUID].PodResizeInProgress; c != nil {
		// 保留已有的 reason 与 message。
		if reason == "" && message == "" {
			reason = c.Reason
			message = c.Message
		}
		// observedGeneration 始终保留；如需更新，调用方必须先清除已有条件。
		observedGeneration = c.ObservedGeneration
	}

	m.podResizeConditions[podUID] = podResizeConditions{
		PodResizeInProgress: updatedPodResizeCondition(v1.PodResizeInProgress, m.podResizeConditions[podUID].PodResizeInProgress, reason, message, observedGeneration),
		PodResizePending:    m.podResizeConditions[podUID].PodResizePending,
	}

	if previousCondition == nil {
		m.recordInProgressResizeCount()
	} else {
		// 比较条件时忽略探测与转换时间，否则每次调用都会被视为“已变更”。
		previousCondition.LastProbeTime = m.podResizeConditions[podUID].PodResizeInProgress.LastProbeTime
		previousCondition.LastTransitionTime = m.podResizeConditions[podUID].PodResizeInProgress.LastTransitionTime
	}

	return observedGeneration, !reflect.DeepEqual(previousCondition, m.podResizeConditions[podUID].PodResizeInProgress)
}

// observeDeferredResizeDuration 在条件被清除时记录其处于 deferred 状态的时长。
func (m *manager) observeDeferredResizeDuration(podUID types.UID, cond *v1.PodCondition, resolution metrics.DeferredResizeResolution) {
	if cond == nil || cond.Reason != v1.PodReasonDeferred || cond.LastTransitionTime.IsZero() {
		return
	}
	// 若 Pod 已从 pod manager 中移除（例如 Pod 删除清理期间），GetPodByUID 会返回 nil。
	// 此时 GetPriorityBucketLabel(nil) 返回 PriorityBucketUnknown（"unknown"）。
	pod, _ := m.podManager.GetPodByUID(podUID)
	priorityBucket := metrics.GetPriorityBucketLabel(pod)
	duration := time.Since(cond.LastTransitionTime.Time).Seconds()
	metrics.PodDeferredResizeDurationSeconds.WithLabelValues(string(resolution), string(priorityBucket)).Observe(duration)
}

// ClearPodResizePendingCondition 从缓存中清除该 Pod 的 PodResizePending 条件。
func (m *manager) ClearPodResizePendingCondition(podUID types.UID, resolution metrics.DeferredResizeResolution) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	cond := m.podResizeConditions[podUID].PodResizePending
	if cond == nil {
		return
	}

	m.observeDeferredResizeDuration(podUID, cond, resolution)

	m.podResizeConditions[podUID] = podResizeConditions{
		PodResizeInProgress: m.podResizeConditions[podUID].PodResizeInProgress,
		PodResizePending:    nil,
	}

	m.observePendingResizeCount()
}

// ClearPodResizeInProgressCondition 从缓存中清除该 Pod 的 PodResizeInProgress 条件。
// 若条件被清除，返回被清除条件的 observedGeneration 和 true；否则返回零值和 false。
func (m *manager) ClearPodResizeInProgressCondition(podUID types.UID) (int64, bool) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	if m.podResizeConditions[podUID].PodResizeInProgress == nil {
		return 0, false
	}

	generation := m.podResizeConditions[podUID].PodResizeInProgress.ObservedGeneration
	m.podResizeConditions[podUID] = podResizeConditions{
		PodResizePending:    m.podResizeConditions[podUID].PodResizePending,
		PodResizeInProgress: nil,
	}

	m.recordInProgressResizeCount()
	return generation, true
}

// BackfillPodResizeConditions 通过读取给定 Pod 的状态，回填 status manager 的 resize 条件。
// 典型场景是 kubelet 重启后从 apiserver 恢复内存中的条件状态。
func (m *manager) BackfillPodResizeConditions(pods []*v1.Pod) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	for _, pod := range pods {
		for _, c := range pod.Status.Conditions {
			switch c.Type {
			case v1.PodResizePending:
				newCondition := updatedPodResizeCondition(v1.PodResizePending, nil, c.Reason, c.Message, c.ObservedGeneration)
				oldConditions := m.podResizeConditions[pod.UID]

				m.podResizeConditions[pod.UID] = podResizeConditions{
					PodResizePending:    newCondition,
					PodResizeInProgress: oldConditions.PodResizeInProgress,
				}

			case v1.PodResizeInProgress:
				newCondition := updatedPodResizeCondition(v1.PodResizeInProgress, nil, c.Reason, c.Message, c.ObservedGeneration)
				oldConditions := m.podResizeConditions[pod.UID]

				m.podResizeConditions[pod.UID] = podResizeConditions{
					PodResizeInProgress: newCondition,
					PodResizePending:    oldConditions.PodResizePending,
				}
			}
		}
	}
	m.observePendingResizeCount()
	m.recordInProgressResizeCount()
}

// IsPodResizeDeferred 在当前 Pod 扩容被推迟（deferred）时返回 true。
func (m *manager) IsPodResizeDeferred(podUID types.UID) bool {
	m.podStatusesLock.RLock()
	defer m.podStatusesLock.RUnlock()

	return m.podResizeConditions[podUID].PodResizePending != nil && m.podResizeConditions[podUID].PodResizePending.Reason == v1.PodReasonDeferred
}

// IsPodResizeInfeasible 在当前 Pod 扩容不可行（infeasible）时返回 true。
func (m *manager) IsPodResizeInfeasible(podUID types.UID) bool {
	m.podStatusesLock.RLock()
	defer m.podStatusesLock.RUnlock()

	return m.podResizeConditions[podUID].PodResizePending != nil && m.podResizeConditions[podUID].PodResizePending.Reason == v1.PodReasonInfeasible
}

// GetPodStatus 返回指定 Pod UID 的缓存状态，以及是否为缓存命中。
// 静态 Pod 的 UID 需要先翻译为 mirror Pod 的 UID 才能命中缓存。
func (m *manager) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	m.podStatusesLock.RLock()
	defer m.podStatusesLock.RUnlock()
	status, ok := m.podStatuses[types.UID(m.podManager.TranslatePodUID(uid))]
	return status.status, ok
}

// SetPodStatus 缓存并更新指定 Pod 的状态，同时触发一次状态更新。
func (m *manager) SetPodStatus(logger klog.Logger, pod *v1.Pod, status v1.PodStatus) (v1.PodStatus, bool) {
	var notification *podStatusNotification
	// 通知在锁外发送，避免订阅者回调时持有 podStatusesLock 造成死锁。
	defer func() {
		if notification != nil {
			m.sendNotification(logger, notification)
		}
	}()

	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	// 确保缓存的是深拷贝，防止调用方后续修改影响缓存。
	status = *status.DeepCopy()

	// 为该 Pod 状态设置 observedGeneration。
	status.ObservedGeneration = podutil.CalculatePodStatusObservedGeneration(pod)

	// 设置了删除时间戳时强制更新状态。这是因为即使 Pod 处于非运行状态，
	// pod worker 仍需能够触发更新和/或删除。
	changed, notif := m.updateStatusInternal(logger, pod, status, pod.DeletionTimestamp != nil, false)
	notification = notif

	return m.podStatuses[pod.UID].status, changed
}

// SetContainerReadiness 用给定的就绪状态更新缓存的容器状态，并触发状态更新。
func (m *manager) SetContainerReadiness(logger klog.Logger, podUID types.UID, containerID kubecontainer.ContainerID, ready bool) {
	var notification *podStatusNotification
	defer func() {
		if notification != nil {
			m.sendNotification(logger, notification)
		}
	}()

	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	pod, ok := m.podManager.GetPodByUID(podUID)
	if !ok {
		// Pod 已从 pod manager 移除，说明即将删除，此时更新就绪状态没有意义。
		logger.V(4).Info("Pod has been deleted, no need to update readiness", "podUID", podUID)
		return
	}

	oldStatus, found := m.podStatuses[pod.UID]
	if !found {
		// 首次同步完成前状态缓存里还没有该 Pod，此时丢弃更新，
		// 后续完整状态同步会覆盖并纠正这里丢失的信息。
		logger.Info("Container readiness changed before pod has synced",
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// 找到需要更新的容器。
	containerStatus, _, ok := findContainerStatus(&oldStatus.status, containerID.String())
	if !ok {
		logger.Info("Container readiness changed for unknown container",
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// 状态未变化时直接返回，避免产生无意义的 API 请求。
	if containerStatus.Ready == ready {
		logger.V(4).Info("Container readiness unchanged",
			"ready", ready,
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// 确保不直接修改缓存中的状态，先做深拷贝。
	status := *oldStatus.status.DeepCopy()
	containerStatus, _, _ = findContainerStatus(&status, containerID.String())
	containerStatus.Ready = ready

	// updateConditionFunc 更新对应类型的条件；缺失时追加。
	updateConditionFunc := func(conditionType v1.PodConditionType, condition v1.PodCondition) {
		conditionIndex := -1
		for i, condition := range status.Conditions {
			if condition.Type == conditionType {
				conditionIndex = i
				break
			}
		}
		if conditionIndex != -1 {
			status.Conditions[conditionIndex] = condition
		} else {
			logger.Info("PodStatus missing condition type", "conditionType", conditionType, "status", status)
			status.Conditions = append(status.Conditions, condition)
		}
	}

	allContainerStatuses := append(status.InitContainerStatuses, status.ContainerStatuses...)
	updateConditionFunc(v1.PodReady, GeneratePodReadyCondition(pod, &oldStatus.status, status.Conditions, allContainerStatuses, status.Phase))
	updateConditionFunc(v1.ContainersReady, GenerateContainersReadyCondition(pod, &oldStatus.status, allContainerStatuses, status.Phase))
	_, notification = m.updateStatusInternal(logger, pod, status, false, false)
}

// SetPodVolumeHealth 更新指定 Pod 上某个卷的 VolumeHealth 缓存条目。
// conditions 是该卷完整的期望条件集合（为空表示健康）。若缓存状态发生变化则返回 true。
func (m *manager) SetPodVolumeHealth(logger klog.Logger, podUID types.UID, volumeName string, conditions []v1.VolumeHealthCondition) bool {
	var notification *podStatusNotification
	defer func() {
		if notification != nil {
			m.sendNotification(logger, notification)
		}
	}()

	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	pod, ok := m.podManager.GetPodByUID(podUID)
	if !ok {
		logger.V(4).Info("Pod has been deleted, no need to update volume health", "podUID", podUID)
		return false
	}

	oldStatus, found := m.podStatuses[pod.UID]
	if !found {
		logger.Info("Volume health changed before pod has synced",
			"pod", klog.KObj(pod),
			"volumeName", volumeName)
		return false
	}

	entryIndex := -1
	var existingConditions []v1.VolumeHealthCondition
	for i := range oldStatus.status.VolumeHealth {
		if oldStatus.status.VolumeHealth[i].Name == volumeName {
			entryIndex = i
			existingConditions = oldStatus.status.VolumeHealth[i].HealthConditions
			break
		}
	}

	// 仅当 (status, reason) 集合不同时才 PATCH；纯 message 变化会被抑制。
	if volumeutil.VolumeHealthConditionSetsEqual(existingConditions, conditions) {
		logger.V(4).Info("Volume health unchanged",
			"pod", klog.KObj(pod),
			"volumeName", volumeName)
		return false
	}

	// 确保不直接修改缓存中的状态，先做深拷贝。
	status := *oldStatus.status.DeepCopy()
	newConditions := append([]v1.VolumeHealthCondition(nil), conditions...)
	now := metav1.Now()
	if entryIndex >= 0 {
		status.VolumeHealth[entryIndex].HealthConditions = newConditions
		status.VolumeHealth[entryIndex].LastTransitionTime = now
	} else {
		status.VolumeHealth = append(status.VolumeHealth, v1.PodVolumeHealth{
			Name:               volumeName,
			HealthConditions:   newConditions,
			LastTransitionTime: now,
		})
	}

	changed, notif := m.updateStatusInternal(logger, pod, status, false, false)
	notification = notif
	return changed
}

// SetContainerStartup 用给定的启动状态更新缓存的容器状态，并触发状态更新。
func (m *manager) SetContainerStartup(logger klog.Logger, podUID types.UID, containerID kubecontainer.ContainerID, started bool) {
	var notification *podStatusNotification
	defer func() {
		if notification != nil {
			m.sendNotification(logger, notification)
		}
	}()

	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	pod, ok := m.podManager.GetPodByUID(podUID)
	if !ok {
		logger.V(4).Info("Pod has been deleted, no need to update startup", "podUID", string(podUID))
		return
	}

	oldStatus, found := m.podStatuses[pod.UID]
	if !found {
		logger.Info("Container startup changed before pod has synced",
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// 找到需要更新的容器。
	containerStatus, _, ok := findContainerStatus(&oldStatus.status, containerID.String())
	if !ok {
		logger.Info("Container startup changed for unknown container",
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// Started 是 *bool，需要判空后再比较，避免 nil 解引用。
	if containerStatus.Started != nil && *containerStatus.Started == started {
		logger.V(4).Info("Container startup unchanged",
			"pod", klog.KObj(pod),
			"containerID", containerID.String())
		return
	}

	// 确保不直接修改缓存中的状态，先做深拷贝。
	status := *oldStatus.status.DeepCopy()
	containerStatus, _, _ = findContainerStatus(&status, containerID.String())
	containerStatus.Started = &started

	_, notification = m.updateStatusInternal(logger, pod, status, false, false)
}

// findContainerStatus 在常规容器与 init 容器状态中查找指定 containerID 的条目。
// 返回的 init 标记区分该容器属于哪一类。
func findContainerStatus(status *v1.PodStatus, containerID string) (containerStatus *v1.ContainerStatus, init bool, ok bool) {
	// 找到需要更新的容器。
	for i, c := range status.ContainerStatuses {
		if c.ContainerID == containerID {
			return &status.ContainerStatuses[i], false, true
		}
	}

	for i, c := range status.InitContainerStatuses {
		if c.ContainerID == containerID {
			return &status.InitContainerStatuses[i], true, true
		}
	}

	return nil, false, false

}

// TerminatePod 确保在 Pod 生命周期结束时容器状态被正确填充默认值。
// 由于 kubelet 必须与容器运行时对账才能观察到容器状态，始终存在因垃圾回收、
// 管理员操作或重启导致临时数据丢失而无法获取一个或多个容器状态的可能。
// 该方法保证缺失的容器状态被视为失败，以免错误地把 Pod 描述为成功。
// 若存在 init 容器且 Pod 尚未初始化，仅 init 容器的失败状态就足以说明 Pod 失败，
// 无需覆盖处于 waiting 的容器（除非有证据表明 Pod 此前已启动过这些容器）。
// 它同时确保 Pod 在删除前被转换为终态（Failed 或 Succeeded）。
func (m *manager) TerminatePod(logger klog.Logger, pod *v1.Pod) {
	var notification *podStatusNotification
	// 通知在锁外发送，避免订阅者回调时持有 podStatusesLock。
	defer func() {
		if notification != nil {
			m.sendNotification(logger, notification)
		}
	}()

	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()

	// 确保所有容器都有终态——由于不知道容器是否成功，一律报告为错误。
	oldStatus := &pod.Status
	cachedStatus, isCached := m.podStatuses[pod.UID]
	if isCached {
		oldStatus = &cachedStatus.status
	}
	status := *oldStatus.DeepCopy()

	// Pod 一旦初始化完成，任何缺失的状态都按失败处理。
	if hasPodInitialized(logger, pod) {
		for i := range status.ContainerStatuses {
			if status.ContainerStatuses[i].State.Terminated != nil {
				continue
			}
			status.ContainerStatuses[i].State = v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					Reason:   kubecontainer.ContainerReasonStatusUnknown,
					Message:  "The container could not be located when the pod was terminated",
					ExitCode: 137,
				},
			}
		}
	}

	// init 容器中，除末尾没有任何启动证据的容器外，其余都标记为失败容器。
	for i := range initializedContainers(status.InitContainerStatuses) {
		if status.InitContainerStatuses[i].State.Terminated != nil {
			continue
		}
		status.InitContainerStatuses[i].State = v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				Reason:   kubecontainer.ContainerReasonStatusUnknown,
				Message:  "The container could not be located when the pod was terminated",
				ExitCode: 137,
			},
		}
	}

	// 确保所有 Pod 都被转换为终态。
	// TODO(#116484): Also assign terminal phase to static an pods.
	if !kubetypes.IsStaticPod(pod) {
		switch status.Phase {
		case v1.PodSucceeded, v1.PodFailed:
			// 已经是终态，无需处理。
		case v1.PodPending, v1.PodRunning:
			if status.Phase == v1.PodRunning && isCached {
				// 已缓存的状态仍是 Running 说明 pod worker 未按预期先标记失败，属于程序错误。
				logger.Info("Terminal running pod should have already been marked as failed, programmer error", "pod", klog.KObj(pod), "podUID", pod.UID)
			}
			logger.V(3).Info("Marking terminal pod as failed", "oldPhase", status.Phase, "pod", klog.KObj(pod), "podUID", pod.UID)
			status.Phase = v1.PodFailed
		default:
			logger.Error(fmt.Errorf("unknown phase: %v", status.Phase), "Unknown phase, programmer error", "pod", klog.KObj(pod), "podUID", pod.UID)
			status.Phase = v1.PodFailed
		}
	}

	logger.V(5).Info("TerminatePod calling updateStatusInternal", "pod", klog.KObj(pod), "podUID", pod.UID)
	// forceUpdate=true 且 podIsFinished=true，确保终态一定被推送到 apiserver 并触发删除。
	_, notification = m.updateStatusInternal(logger, pod, status, true, true)
}

// hasPodInitialized 判断 Pod 是否有证据表明曾经启动过常规容器；若没有，这些容器
// 就不应被转换为 terminated 状态。
func hasPodInitialized(logger klog.Logger, pod *v1.Pod) bool {
	// 没有 init 容器的 Pod 总是被视为已初始化。
	if len(pod.Spec.InitContainers) == 0 {
		return true
	}
	// 任意容器曾离开 waiting 状态，即说明 Pod 已初始化。
	for _, status := range pod.Status.ContainerStatuses {
		if status.LastTerminationState.Terminated != nil || status.State.Waiting == nil {
			return true
		}
	}
	// 若最后一个 init 容器曾以退出码 0 完成，则 Pod 已初始化。
	if l := len(pod.Status.InitContainerStatuses); l > 0 {
		container, ok := kubeutil.GetContainerByIndex(pod.Spec.InitContainers, pod.Status.InitContainerStatuses, l-1)
		if !ok {
			logger.V(4).Info("Mismatch between pod spec and status, likely programmer error", "pod", klog.KObj(pod), "containerName", container.Name)
			return false
		}

		containerStatus := pod.Status.InitContainerStatuses[l-1]
		if podutil.IsRestartableInitContainer(&container) {
			if containerStatus.State.Running != nil &&
				containerStatus.Started != nil && *containerStatus.Started {
				return true
			}
		} else { // regular init container
			if state := containerStatus.LastTerminationState; state.Terminated != nil && state.Terminated.ExitCode == 0 {
				return true
			}
			if state := containerStatus.State; state.Terminated != nil && state.Terminated.ExitCode == 0 {
				return true
			}
		}
	}
	// 否则 Pod 没有任何已初始化的记录。
	return false
}

// initializedContainers 返回除处于 Waiting 状态的末尾容器之外的所有状态，这部分
// 容器即至少尝试启动过一次的集合。若全部容器都在 Waiting，则始终返回第一个容器。
func initializedContainers(containers []v1.ContainerStatus) []v1.ContainerStatus {
	for i := len(containers) - 1; i >= 0; i-- {
		if containers[i].State.Waiting == nil || containers[i].LastTerminationState.Terminated != nil {
			return containers[0 : i+1]
		}
	}
	// 始终至少返回一个容器。
	if len(containers) > 0 {
		return containers[0:1]
	}
	return nil
}

// checkContainerStateTransition 确保没有容器试图从 terminated 转换到非 terminated 状态，
// 这种转换是非法的，说明 kubelet 存在逻辑错误。
func checkContainerStateTransition(oldStatuses, newStatuses *v1.PodStatus, podSpec *v1.PodSpec) error {
	// 若策略为总是重启，容器允许离开 terminated 状态。
	if podSpec.RestartPolicy == v1.RestartPolicyAlways {
		return nil
	}
	// 若 Pod 支持原地重启，允许所有容器离开 terminated 状态。
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
		if podutil.AllContainersCouldRestart(podSpec) {
			return nil
		}
	}
	for _, oldStatus := range oldStatuses.ContainerStatuses {
		// 跳过未终止的容器。
		if oldStatus.State.Terminated == nil {
			continue
		}
		// 跳过失败但允许重启的容器。
		if oldStatus.State.Terminated.ExitCode != 0 && podSpec.RestartPolicy == v1.RestartPolicyOnFailure {
			continue
		}
		// 跳过被容器重启策略允许重启的容器。
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			restartable := false
			for _, container := range podSpec.Containers {
				if container.Name == oldStatus.Name && podutil.ContainerShouldRestart(container, *podSpec, oldStatus.State.Terminated.ExitCode) {
					restartable = true
				}
			}
			if restartable {
				continue
			}
		}
		for _, newStatus := range newStatuses.ContainerStatuses {
			if oldStatus.Name == newStatus.Name && newStatus.State.Terminated == nil {
				return fmt.Errorf("terminated container %v attempted illegal transition to non-terminated state", newStatus.Name)
			}
		}
	}

	for i, oldStatus := range oldStatuses.InitContainerStatuses {
		initContainer, ok := kubeutil.GetContainerByIndex(podSpec.InitContainers, oldStatuses.InitContainerStatuses, i)
		if !ok {
			return fmt.Errorf("found mismatch between pod spec and status, container: %v", oldStatus.Name)
		}
		// 跳过可重启的 init 容器，它们始终允许重启。
		if podutil.IsRestartableInitContainer(&initContainer) {
			continue
		}
		// 跳过未终止的容器。
		if oldStatus.State.Terminated == nil {
			continue
		}
		// 跳过失败但允许重启的容器。
		if oldStatus.State.Terminated.ExitCode != 0 && podSpec.RestartPolicy == v1.RestartPolicyOnFailure {
			continue
		}
		// 跳过被容器重启策略允许重启的容器。
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			restartable := false
			for _, container := range podSpec.InitContainers {
				if container.Name == oldStatus.Name && podutil.ContainerShouldRestart(container, *podSpec, oldStatus.State.Terminated.ExitCode) {
					restartable = true
				}
			}
			if restartable {
				continue
			}
		}
		for _, newStatus := range newStatuses.InitContainerStatuses {
			if oldStatus.Name == newStatus.Name && newStatus.State.Terminated == nil {
				return fmt.Errorf("terminated init container %v attempted illegal transition to non-terminated state", newStatus.Name)
			}
		}
	}
	return nil
}

// updateStatusInternal 更新内部状态缓存，并在必要时向 apiserver 排入一次更新。
// 该方法不是线程安全的，必须在已持有锁的函数中调用。
func (m *manager) updateStatusInternal(logger klog.Logger, pod *v1.Pod, status v1.PodStatus, forceUpdate, podIsFinished bool) (bool, *podStatusNotification) {
	var oldStatus v1.PodStatus
	cachedStatus, isCached := m.podStatuses[pod.UID]
	if isCached {
		oldStatus = cachedStatus.status
		// TODO(#116484): Also assign terminal phase to static pods.
		if !kubetypes.IsStaticPod(pod) {
			// 一旦标记为 finished 就不能回退，否则已上报的终态可能被后续更新覆盖。
			if cachedStatus.podIsFinished && !podIsFinished {
				logger.Info("Got unexpected podIsFinished=false, while podIsFinished=true in status cache, programmer error", "pod", klog.KObj(pod))
				podIsFinished = true
			}
		}
	} else if mirrorPod, ok := m.podManager.GetMirrorPodByPod(pod); ok {
		// 静态 Pod 的权威状态在 mirror Pod 上，因此以它作为比较基准。
		oldStatus = mirrorPod.Status
	} else {
		oldStatus = pod.Status
	}

	// 检查容器是否存在非法状态转换。
	if err := checkContainerStateTransition(&oldStatus, &status, &pod.Spec); err != nil {
		logger.Error(err, "Status update on pod aborted", "pod", klog.KObj(pod))
		return false, nil
	}

	// 设置 ContainersReadyCondition.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.ContainersReady)

	// 设置 ReadyCondition.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.PodReady)

	// 设置 InitializedCondition.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.PodInitialized)

	// 设置 PodReadyToStartContainersCondition.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.PodReadyToStartContainers)

	// 设置 PodScheduledCondition.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.PodScheduled)

	// 设置 DisruptionTarget.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.DisruptionTarget)

	// 设置 AllContainersRestarting.LastTransitionTime。
	updateLastTransitionTime(&status, &oldStatus, v1.AllContainersRestarting)

	// 确保 StartTime 在多次更新之间保持不变。
	if oldStatus.StartTime != nil && !oldStatus.StartTime.IsZero() {
		status.StartTime = oldStatus.StartTime.DeepCopy()
	} else if status.StartTime.IsZero() {
		// 若状态中没有启动时间，需要设置一个初始值。
		now := metav1.Now()
		status.StartTime = &now
	}

	// 在 Pod 状态更新中保留卷健康信息。VolumeHealth 由卷健康管理器独立设置，
	// 不属于容器运行时生成的状态，若不保留会被本次更新抹掉。
	if len(oldStatus.VolumeHealth) > 0 && len(status.VolumeHealth) == 0 {
		status.VolumeHealth = oldStatus.VolumeHealth
	}

	// 防止发送不必要的 patch：generation 只应单调递增。
	if oldStatus.ObservedGeneration > status.ObservedGeneration {
		status.ObservedGeneration = oldStatus.ObservedGeneration
	}

	normalizeStatus(pod, &status)

	// 在调试生产环境竞态时（一般不需要）额外记录容器终止状态。
	if loggerV := logger.V(5); loggerV.Enabled() {
		var containers []string
		for _, s := range append(append([]v1.ContainerStatus(nil), status.InitContainerStatuses...), status.ContainerStatuses...) {
			var current, previous string
			switch {
			case s.State.Running != nil:
				current = "running"
			case s.State.Waiting != nil:
				current = "waiting"
			case s.State.Terminated != nil:
				current = fmt.Sprintf("terminated=%d", s.State.Terminated.ExitCode)
			default:
				current = "unknown"
			}
			switch {
			case s.LastTerminationState.Running != nil:
				previous = "running"
			case s.LastTerminationState.Waiting != nil:
				previous = "waiting"
			case s.LastTerminationState.Terminated != nil:
				previous = fmt.Sprintf("terminated=%d", s.LastTerminationState.Terminated.ExitCode)
			default:
				previous = "<none>"
			}
			containers = append(containers, fmt.Sprintf("(%s state=%s previous=%s)", s.Name, current, previous))
		}
		sort.Strings(containers)
		loggerV.Info("updateStatusInternal", "version", cachedStatus.version+1, "podIsFinished", podIsFinished, "pod", klog.KObj(pod), "podUID", pod.UID, "containers", strings.Join(containers, " "))
	}

	// 为刚刚终止的容器递增计数指标。每个 Pod 的每个容器只应计数一次：
	// 在写入缓存之前比较新旧状态，即可精确识别容器进入 Terminated 的时刻。
	checkAndRecordTerminations := func(oldStatuses, newStatuses []v1.ContainerStatus, containerType string) {
		for _, newCStat := range newStatuses {
			if newCStat.State.Terminated != nil {
				// 查找该容器此前的状态。
				var oldCStat *v1.ContainerStatus
				for i := range oldStatuses {
					if oldStatuses[i].Name == newCStat.Name {
						oldCStat = &oldStatuses[i]
						break
					}
				}
				// 此前未终止（或为全新且已终止）时记录指标。
				if oldCStat == nil || oldCStat.State.Terminated == nil {
					metrics.TerminatedContainersTotal.WithLabelValues(containerType, strconv.Itoa(int(newCStat.State.Terminated.ExitCode)), newCStat.State.Terminated.Reason).Inc()
				}
			}
		}
	}
	checkAndRecordTerminations(oldStatus.InitContainerStatuses, status.InitContainerStatuses, metrics.InitContainer)
	checkAndRecordTerminations(oldStatus.ContainerStatuses, status.ContainerStatuses, metrics.Container)
	checkAndRecordTerminations(oldStatus.EphemeralContainerStatuses, status.EphemeralContainerStatuses, metrics.EphemeralContainer)

	// 这样做的目的是防止对同一个 Pod 状态的并发更新相互覆盖，使 Pod 的 phase 单调推进。
	if isCached && isPodStatusByKubeletEqual(&cachedStatus.status, &status) && !forceUpdate {
		logger.V(3).Info("Ignoring same status for pod", "pod", klog.KObj(pod), "status", status)
		return false, nil
	}

	newStatus := versionedPodStatus{
		status:        status,
		version:       cachedStatus.version + 1,
		podName:       pod.Name,
		podNamespace:  pod.Namespace,
		podIsFinished: podIsFinished,
	}

	// 在状态真正推送到 API server 前可能产生多次更新，因此记录从首次更新
	// 到最终提交给 API 的时间跨度，用于统计同步延迟。
	if cachedStatus.at.IsZero() {
		newStatus.at = time.Now()
	} else {
		newStatus.at = cachedStatus.at
	}

	m.podStatuses[pod.UID] = newStatus

	select {
	case m.podStatusChannel <- struct{}{}:
	default:
		// 已有待处理的状态更新信号，无需重复发送。
	}

	podCopy := *pod
	podCopy.Status = status

	// 首次进入缓存且未处于删除中，才视为“新增”Pod。
	notification := &podStatusNotification{
		pod:           &podCopy,
		status:        status,
		isAdded:       !isCached && pod.DeletionTimestamp == nil,
		podIsFinished: podIsFinished,
	}

	return true, notification
}

// sendNotification 把状态变更分发给所有已注册的订阅者。
// 调用方必须已释放 podStatusesLock，因为订阅者回调可能再次访问 manager。
func (m *manager) sendNotification(logger klog.Logger, n *podStatusNotification) {
	for _, notifier := range m.notifiers {
		if !n.podIsFinished {
			notifier.OnPodUpdated(logger, n.pod, n.status, n.isAdded)
		} else {
			notifier.OnPodRemoved(logger, n.pod)
		}
	}
}

// updateLastTransitionTime 更新 Pod 条件的 LastTransitionTime。
func updateLastTransitionTime(status, oldStatus *v1.PodStatus, conditionType v1.PodConditionType) {
	_, condition := podutil.GetPodCondition(status, conditionType)
	if condition == nil {
		return
	}
	// 需要设置 LastTransitionTime；状态未变化时沿用旧值，避免误报状态转换。
	lastTransitionTime := metav1.Now()
	_, oldCondition := podutil.GetPodCondition(oldStatus, conditionType)
	if oldCondition != nil && condition.Status == oldCondition.Status {
		lastTransitionTime = oldCondition.LastTransitionTime
	}
	condition.LastTransitionTime = lastTransitionTime
}

// deletePodStatus 直接从状态缓存中移除指定 Pod。
func (m *manager) deletePodStatus(uid types.UID) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()
	delete(m.podStatuses, uid)
	m.podStartupLatencyHelper.DeletePodStartupState(uid)
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		if conds, exists := m.podResizeConditions[uid]; exists {
			// 在删除前记录被推迟的扩容时长，并同步指标。
			m.observeDeferredResizeDuration(uid, conds.PodResizePending, metrics.DeferredResizeResolutionTerminated)
			delete(m.podResizeConditions, uid)
			m.recordInProgressResizeCount()
			m.observePendingResizeCount()
		}
	}
}

// RemoveOrphanedStatuses 扫描状态缓存，删除不在给定 podUIDs 中的 Pod 条目。
// TODO(filipg): It'd be cleaner if we can do this without signal from user.
func (m *manager) RemoveOrphanedStatuses(logger klog.Logger, podUIDs map[types.UID]bool) {
	m.podStatusesLock.Lock()
	defer m.podStatusesLock.Unlock()
	for key := range m.podStatuses {
		if _, ok := podUIDs[key]; !ok {
			logger.V(5).Info("Removing pod from status map", "podUID", key)
			delete(m.podStatuses, key)
			if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
				if conds, exists := m.podResizeConditions[key]; exists {
					m.observeDeferredResizeDuration(key, conds.PodResizePending, metrics.DeferredResizeResolutionTerminated)
					delete(m.podResizeConditions, key)
					m.recordInProgressResizeCount()
					m.observePendingResizeCount()
				}
			}
		}
	}
}

// syncBatch 把 Pod 状态同步到 apiserver。返回尝试同步的数量，供测试使用。
func (m *manager) syncBatch(ctx context.Context, all bool) int {
	logger := klog.FromContext(ctx)
	type podSync struct {
		podUID    types.UID
		statusUID kubetypes.MirrorPodUID
		status    versionedPodStatus
	}

	var updatedStatuses []podSync
	podToMirror, mirrorToPod := m.podManager.GetUIDTranslations()
	// 临界区：持读锁收集待同步项，避免长时间阻塞状态写入。
	func() {
		m.podStatusesLock.RLock()
		defer m.podStatusesLock.RUnlock()

		// 清理孤儿版本记录，避免 apiStatusVersions 无界增长。
		if all {
			for uid := range m.apiStatusVersions {
				_, hasPod := m.podStatuses[types.UID(uid)]
				_, hasMirror := mirrorToPod[uid]
				if !hasPod && !hasMirror {
					delete(m.apiStatusVersions, uid)
				}
			}
		}

		// 决定哪些 Pod 需要更新状态。
		for uid, status := range m.podStatuses {
			// 把 Pod UID（源）翻译为状态 UID（API 侧 Pod）——
			// 静态 Pod 在源侧用 Pod UID 标识，但在 API 中通过 mirror Pod 的 uid 跟踪。
			uidOfStatus := kubetypes.MirrorPodUID(uid)
			if mirrorUID, ok := podToMirror[kubetypes.ResolvedPodUID(uid)]; ok {
				if mirrorUID == "" {
					// 尚未创建 mirror Pod 的静态 Pod，跳过本次同步。
					logger.V(5).Info("Static pod does not have a corresponding mirror pod; skipping",
						"podUID", uid,
						"pod", klog.KRef(status.podNamespace, status.podName))
					continue
				}
				uidOfStatus = mirrorUID
			}

			// 若收到新的状态更新，则触发更新；否则该 Pod 可以等待下一次全量检查
			// （全量检查同时会执行对账）。
			if !all {
				if m.apiStatusVersions[uidOfStatus] >= status.version {
					continue
				}
				updatedStatuses = append(updatedStatuses, podSync{uid, uidOfStatus, status})
				continue
			}

			// 确保任何新状态、不一致状态或可删除的 Pod 都会更新。若某次更新失败，
			// 下次任意其他 Pod 更新时会重试。
			if m.needsUpdate(logger, types.UID(uidOfStatus), status) {
				updatedStatuses = append(updatedStatuses, podSync{uid, uidOfStatus, status})
			} else if m.needsReconcile(logger, uid, status.status) {
				// 在此删除 apiStatusVersions，强制对 Pod 状态做一次更新。
				// 多数情况下，随后 syncPod() 成功同步后会重新填充该记录。
				delete(m.apiStatusVersions, uidOfStatus)
				updatedStatuses = append(updatedStatuses, podSync{uid, uidOfStatus, status})
			}
		}
	}()

	// 在临界区外逐个同步，避免持锁进行网络请求。
	for _, update := range updatedStatuses {
		logger.V(5).Info("Sync pod status", "podUID", update.podUID, "statusUID", update.statusUID, "version", update.status.version)
		m.syncPod(ctx, update.podUID, update.status)
	}

	return len(updatedStatuses)
}

// syncPod 把给定状态同步到 API server。调用方不得持有状态锁。
func (m *manager) syncPod(ctx context.Context, uid types.UID, status versionedPodStatus) {
	logger := klog.FromContext(ctx)
	// TODO: make me easier to express from client code
	pod, err := m.kubeClient.CoreV1().Pods(status.podNamespace).Get(ctx, status.podName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		logger.V(3).Info("Pod does not exist on the server",
			"podUID", uid,
			"pod", klog.KRef(status.podNamespace, status.podName))
		// 若 Pod 已删除，状态会在 RemoveOrphanedStatuses 中被清除，这里直接忽略更新。
		return
	}
	if err != nil {
		logger.Error(err, "Failed to get status for pod",
			"podUID", uid,
			"pod", klog.KRef(status.podNamespace, status.podName))
		return
	}

	translatedUID := m.podManager.TranslatePodUID(pod.UID)
	// 仅用于比较，把原始 uid 做类型转换。
	if len(translatedUID) > 0 && translatedUID != kubetypes.ResolvedPodUID(uid) {
		// 同名 Pod 被删除后重建：旧状态已无意义，清理后跳过。
		logger.V(2).Info("Pod was deleted and then recreated, skipping status update",
			"pod", klog.KObj(pod),
			"oldPodUID", uid,
			"podUID", translatedUID)
		m.deletePodStatus(uid)
		return
	}

	mergedStatus := mergePodStatus(pod, pod.Status, status.status, m.podDeletionSafety.PodCouldHaveRunningContainers(pod))

	newPod, patchBytes, unchanged, err := statusutil.PatchPodStatus(ctx, m.kubeClient, pod.Namespace, pod.Name, pod.UID, pod.Status, mergedStatus)
	logger.V(3).Info("Patch status for pod", "pod", klog.KObj(pod), "podUID", uid, "patch", string(patchBytes))

	if err != nil {
		logger.Error(err, "Failed to update status for pod", "pod", klog.KObj(pod))
		return
	}
	if unchanged {
		logger.V(3).Info("Status for pod is up-to-date", "pod", klog.KObj(pod), "statusVersion", status.version)
	} else {
		logger.V(3).Info("Status for pod updated successfully", "pod", klog.KObj(pod), "statusVersion", status.version, "status", mergedStatus)
		pod = newPod
		// 传入新对象（API 调用结果，包含更新后的 ResourceVersion）。
		m.podStartupLatencyHelper.RecordStatusUpdated(logger, pod)
	}

	// 统计状态从生成到在 server 上更新所耗的传播时间。
	if status.at.IsZero() {
		logger.V(3).Info("Pod had no status time set", "pod", klog.KObj(pod), "podUID", uid, "version", status.version)
	} else {
		duration := time.Since(status.at).Truncate(time.Millisecond)
		metrics.PodStatusSyncDuration.Observe(duration.Seconds())
	}

	m.apiStatusVersions[kubetypes.MirrorPodUID(pod.UID)] = status.version

	// 不处理 mirror Pod 的优雅删除。
	if m.canBeDeleted(logger, pod, status.status, status.podIsFinished) {
		deleteOptions := metav1.DeleteOptions{
			GracePeriodSeconds: new(int64),
			// 使用 Pod UID 作为删除前置条件，防止误删同名同命名空间下新建的 Pod。
			Preconditions: metav1.NewUIDPreconditions(string(pod.UID)),
		}
		err = m.kubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptions)
		if err != nil {
			logger.Info("Failed to delete status for pod", "pod", klog.KObj(pod), "err", err)
			return
		}
		logger.V(3).Info("Pod fully terminated and removed from etcd", "pod", klog.KObj(pod))
		m.deletePodStatus(uid)
	}
}

// needsUpdate 判断给定 Pod UID 的状态是否已过期。
// 该方法不是线程安全的，只能由同步线程访问。
func (m *manager) needsUpdate(logger klog.Logger, uid types.UID, status versionedPodStatus) bool {
	latest, ok := m.apiStatusVersions[kubetypes.MirrorPodUID(uid)]
	if !ok || latest < status.version {
		return true
	}
	pod, ok := m.podManager.GetPodByUID(uid)
	if !ok {
		return false
	}
	return m.canBeDeleted(logger, pod, status.status, status.podIsFinished)
}

// canBeDeleted 判断 Pod 是否已满足删除条件：必须处于删除中、不是 mirror Pod、
// 阶段为终态，且终止流程已完成。
func (m *manager) canBeDeleted(logger klog.Logger, pod *v1.Pod, status v1.PodStatus, podIsFinished bool) bool {
	if pod.DeletionTimestamp == nil || kubetypes.IsMirrorPod(pod) {
		return false
	}
	// 根据 pod manager 提供的 pod.Status 延迟删除，直到阶段变为终态。
	if !podutil.IsPodPhaseTerminal(pod.Status.Phase) {
		// 便于调试，延迟删除时同时记录 kubelet 本地视角的 phase。
		logger.V(3).Info("Delaying pod deletion as the phase is non-terminal", "phase", pod.Status.Phase, "localPhase", status.Phase, "pod", klog.KObj(pod), "podUID", pod.UID)
		return false
	}
	// 若本次更新是在完成 Pod 终止时产生的，说明终止已完成。
	if podIsFinished {
		logger.V(3).Info("The pod termination is finished as SyncTerminatedPod completes its execution", "phase", pod.Status.Phase, "localPhase", status.Phase, "pod", klog.KObj(pod), "podUID", pod.UID)
		return true
	}
	return false
}

// needsReconcile 把给定状态与 pod manager 中的状态（实际来自 apiserver）比较，
// 判断是否需要与 apiserver 对账。当 apiserver 与 kubelet 的 Pod 状态不一致时，
// kubelet 应强制发送一次更新来纠正，因为 kubelet 才是 Pod 状态的事实来源。
// NOTE(random-liu): 直接传入 mirror Pod uid 并按 uid 获取 mirror Pod 会更简单，但
// 目前 pod manager 只支持通过静态 Pod 获取 mirror Pod，因此这里必须传静态 Pod uid。
// TODO(random-liu): Simplify the logic when mirror pod manager is added.
func (m *manager) needsReconcile(logger klog.Logger, uid types.UID, status v1.PodStatus) bool {
	// Pod 可能是静态 Pod，因此需要先做转换。
	pod, ok := m.podManager.GetPodByUID(uid)
	if !ok {
		logger.V(4).Info("Pod has been deleted, no need to reconcile", "podUID", uid)
		return false
	}
	// 若为静态 Pod，应检查其 mirror Pod，因为只有 mirror Pod 中的状态对我们有意义。
	if kubetypes.IsStaticPod(pod) {
		mirrorPod, ok := m.podManager.GetMirrorPodByPod(pod)
		if !ok {
			logger.V(4).Info("Static pod has no corresponding mirror pod, no need to reconcile", "pod", klog.KObj(pod))
			return false
		}
		pod = mirrorPod
	}

	podStatus := pod.Status.DeepCopy()
	normalizeStatus(pod, podStatus)

	if isPodStatusByKubeletEqual(podStatus, &status) {
		// 若源状态与缓存状态相同，则无需对账，直接返回。
		return false
	}
	logger.V(3).Info("Pod status is inconsistent with cached status for pod, a reconciliation should be triggered",
		"pod", klog.KObj(pod),
		"statusDiff", diff.Diff(podStatus, &status))

	return true
}

// normalizeStatus 把 podStatus 中纳秒精度的时间戳归一化到秒精度
// （*RFC339NANO* -> *RFC3339*）。在把 podStatus 与 apiserver 返回的状态比较之前
// 必须先做此处理，因为 apiserver 不支持 RFC339NANO。
// 将 apiserver 迁移到 RFC339NANO 的相关 issue #15262/PR #15263 已关闭。
func normalizeStatus(pod *v1.Pod, status *v1.PodStatus) *v1.PodStatus {
	bytesPerStatus := kubecontainer.MaxPodTerminationMessageLogLength
	// 按容器数量均分终止消息的字节预算，避免单个 Pod 的总日志量超出 etcd 对象上限。
	if containers := len(pod.Spec.Containers) + len(pod.Spec.InitContainers) + len(pod.Spec.EphemeralContainers); containers > 0 {
		bytesPerStatus = bytesPerStatus / containers
	}
	normalizeTimeStamp := func(t *metav1.Time) {
		*t = t.Rfc3339Copy()
	}
	normalizeContainerState := func(c *v1.ContainerState) {
		if c.Running != nil {
			normalizeTimeStamp(&c.Running.StartedAt)
		}
		if c.Terminated != nil {
			normalizeTimeStamp(&c.Terminated.StartedAt)
			normalizeTimeStamp(&c.Terminated.FinishedAt)
			if len(c.Terminated.Message) > bytesPerStatus {
				c.Terminated.Message = c.Terminated.Message[:bytesPerStatus]
			}
		}
	}

	if status.StartTime != nil {
		normalizeTimeStamp(status.StartTime)
	}
	for i := range status.Conditions {
		condition := &status.Conditions[i]
		normalizeTimeStamp(&condition.LastProbeTime)
		normalizeTimeStamp(&condition.LastTransitionTime)
	}

	normalizeContainerStatuses := func(containerStatuses []v1.ContainerStatus) {
		for i := range containerStatuses {
			cstatus := &containerStatuses[i]
			normalizeContainerState(&cstatus.State)
			normalizeContainerState(&cstatus.LastTerminationState)
		}
	}

	normalizeContainerStatuses(status.ContainerStatuses)
	sort.Sort(kubetypes.SortedContainerStatuses(status.ContainerStatuses))

	normalizeContainerStatuses(status.InitContainerStatuses)
	kubetypes.SortInitContainerStatuses(pod, status.InitContainerStatuses)

	normalizeContainerStatuses(status.EphemeralContainerStatuses)
	sort.Sort(kubetypes.SortedContainerStatuses(status.EphemeralContainerStatuses))

	return status
}

// mergePodStatus 合并 oldPodStatus 与 newPodStatus，以保留非 kubelet 拥有的 Pod 条件，
// 并确保只有在所有运行中的容器都终止后才发生终态转换。该方法不修改旧状态。
func mergePodStatus(pod *v1.Pod, oldPodStatus, newPodStatus v1.PodStatus, couldHaveRunningContainers bool) v1.PodStatus {
	podConditions := make([]v1.PodCondition, 0, len(oldPodStatus.Conditions)+len(newPodStatus.Conditions))

	// 保留所有不由 kubelet 拥有的条件（例如其他组件写入的条件）。
	for _, c := range oldPodStatus.Conditions {
		if !kubetypes.PodConditionByKubelet(c.Type) {
			podConditions = append(podConditions, c)
		}
	}

	transitioningToTerminalPhase := !podutil.IsPodPhaseTerminal(oldPodStatus.Phase) && podutil.IsPodPhaseTerminal(newPodStatus.Phase)

	for _, c := range newPodStatus.Conditions {
		if kubetypes.PodConditionByKubelet(c.Type) {
			podConditions = append(podConditions, c)
		} else if kubetypes.PodConditionSharedByKubelet(c.Type) {
			// 所有“与 kubelet 共享”的条件都替换或追加。
			if c.Type == v1.DisruptionTarget {
				// 用检查保护 DisruptionTarget 条件的更新，确保只在所有容器终止且
				// 阶段为终态时才发送。这样当实际状态转换被延迟时，可避免发送
				// 不必要的 patch 请求去添加该条件。
				if transitioningToTerminalPhase && !couldHaveRunningContainers {
					// 在此重新更新 LastTransitionTime，因为 updateStatusInternal 中设置的
					// 较早转换时间很可能已过期——条件发送被推迟到了所有容器终止之后。
					updateLastTransitionTime(&newPodStatus, &oldPodStatus, c.Type)
					if _, c := podutil.GetPodConditionFromList(newPodStatus.Conditions, c.Type); c != nil {
						// 共享条件在 podConditions 中更新或追加。
						podConditions = statusutil.ReplaceOrAppendPodCondition(podConditions, c)
					}
				}
			}
		}
	}
	newPodStatus.Conditions = podConditions

	// ResourceClaimStatuses 不由 kubelet 拥有或修改。
	newPodStatus.ResourceClaimStatuses = oldPodStatus.ResourceClaimStatuses
	// ExtendedResourceClaimStatus 不由 kubelet 拥有或修改。
	newPodStatus.ExtendedResourceClaimStatus = oldPodStatus.ExtendedResourceClaimStatus
	// NodeAllocatableResourceClaimStatuses 不由 kubelet 拥有或修改。
	newPodStatus.NodeAllocatableResourceClaimStatuses = oldPodStatus.NodeAllocatableResourceClaimStatuses

	// 除非 Pod 确实处于终态，否则延迟将其转换为终态。
	// kubelet 绝不应把仍可能运行容器、仍在占用独占资源的 Pod 报告为终态。
	// 卷等资源由 kubelet 内的子系统对账，若新 Pod 复用独占资源会自行收敛
	// （卸载 -> 释放 -> 挂载），因此无需等待这些资源被卸载。总体而言，kubelet
	// 独占的资源必须在 Pod 被报告为终态前释放；而 API 之上有参与组件的资源则
	// 依赖 Pod 进入终态（或完全删除）来释放。
	if transitioningToTerminalPhase {
		if couldHaveRunningContainers {
			newPodStatus.Phase = oldPodStatus.Phase
			newPodStatus.Reason = oldPodStatus.Reason
			newPodStatus.Message = oldPodStatus.Message
		}
	}

	// 若新阶段为终态，显式把 v1.PodReady 与 v1.ContainersReady 置为 false。
	// kubelet 对就绪条件的对账可能需要一些时间，因此在终态时显式置 false，
	// 确保不会上报“终态 phase + ready=true”的状态。详见 https://issues.k8s.io/108594。
	if podutil.IsPodPhaseTerminal(newPodStatus.Phase) {
		if podutil.IsPodReadyConditionTrue(newPodStatus) || podutil.IsContainersReadyConditionTrue(newPodStatus) {
			containersReadyCondition := generateContainersReadyConditionForTerminalPhase(pod, &oldPodStatus, newPodStatus.Phase)
			podutil.UpdatePodCondition(&newPodStatus, &containersReadyCondition)

			podReadyCondition := generatePodReadyConditionForTerminalPhase(pod, &oldPodStatus, newPodStatus.Phase)
			podutil.UpdatePodCondition(&newPodStatus, &podReadyCondition)
		}
	}

	return newPodStatus
}

// NeedToReconcilePodReadiness 返回 Pod 的 “Ready” 条件是否需要重新对账。
func NeedToReconcilePodReadiness(pod *v1.Pod) bool {
	// 没有 ReadinessGates 时 Ready 条件完全由容器状态决定，无需额外对账。
	if len(pod.Spec.ReadinessGates) == 0 {
		return false
	}
	podReadyCondition := GeneratePodReadyCondition(pod, &pod.Status, pod.Status.Conditions, pod.Status.ContainerStatuses, pod.Status.Phase)
	i, curCondition := podutil.GetPodConditionFromList(pod.Status.Conditions, v1.PodReady)
	// 仅当 “Ready” 条件存在且 Status 或 Message 与期望不符时才需要对账。
	if i >= 0 && (curCondition.Status != podReadyCondition.Status || curCondition.Message != podReadyCondition.Message) {
		return true
	}
	return false
}

// updatedPodResizeCondition 基于旧条件构造新的 resize 条件；reason 变化时才刷新
// LastTransitionTime，避免无意义的转换时间抖动。
func updatedPodResizeCondition(conditionType v1.PodConditionType, oldCondition *v1.PodCondition, reason, message string, observedGeneration int64) *v1.PodCondition {
	now := metav1.NewTime(time.Now())
	var lastTransitionTime metav1.Time
	if oldCondition == nil || oldCondition.Reason != reason {
		lastTransitionTime = now
	} else {
		lastTransitionTime = oldCondition.LastTransitionTime
	}

	return &v1.PodCondition{
		Type:               conditionType,
		Status:             v1.ConditionTrue,
		LastProbeTime:      now,
		LastTransitionTime: lastTransitionTime,
		ObservedGeneration: observedGeneration,
		Reason:             reason,
		Message:            message,
	}
}

// observePendingResizeCount 设置处于 pending 状态的扩容指标。
// 先按 (reason, priorityBucket) 聚合再整体重置指标，避免残留已消失的标签组合。
func (m *manager) observePendingResizeCount() {
	type pendingKey struct {
		reason         string
		priorityBucket metrics.PriorityBucket
	}
	pendingResizeCount := make(map[pendingKey]int)
	for uid, conditions := range m.podResizeConditions {
		if conditions.PodResizePending != nil {
			pod, ok := m.podManager.GetPodByUID(uid)
			if !ok {
				continue
			}
			reason := strings.ToLower(conditions.PodResizePending.Reason)
			priorityBucket := metrics.GetPriorityBucketLabel(pod)
			pendingResizeCount[pendingKey{reason: reason, priorityBucket: priorityBucket}]++
		}
	}

	metrics.PodPendingResizes.Reset()
	for k, count := range pendingResizeCount {
		metrics.PodPendingResizes.WithLabelValues(k.reason, string(k.priorityBucket)).Set(float64(count))
	}
}

// recordInProgressResizeCount 设置处于进行中状态的扩容指标。
func (m *manager) recordInProgressResizeCount() {
	inProgressResizeCount := 0
	for _, conditions := range m.podResizeConditions {
		if conditions.PodResizeInProgress != nil {
			inProgressResizeCount++
		}
	}
	metrics.PodInProgressResizes.Set(float64(inProgressResizeCount))
}
