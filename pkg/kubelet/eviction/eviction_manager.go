/*
Copyright 2016 The Kubernetes Authors.

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

package eviction

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/utils/clock"

	resourcehelper "k8s.io/component-helpers/resource"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	podCleanupTimeout  = 30 * time.Second
	podCleanupPollFreq = time.Second
)

const (
	// signalEphemeralContainerFsLimit 是容器请求的文件系统上可用的存储量信号名。
	signalEphemeralContainerFsLimit string = "ephemeralcontainerfs.limit"
	// signalEphemeralPodFsLimit 是 pod 请求的文件系统上可用的存储量信号名。
	signalEphemeralPodFsLimit string = "ephemeralpodfs.limit"
	// signalEmptyDirFsLimit 是 emptyDir 请求的文件系统上可用的存储量信号名。
	signalEmptyDirFsLimit string = "emptydirfs.limit"
	// immediateEvictionGracePeriodSeconds 是因资源压力需要快速驱逐时，
	// 给 pod 留出的关闭宽限期。
	immediateEvictionGracePeriodSeconds = 1
)

// managerImpl 实现 Manager 接口。
type managerImpl struct {
	// 用于跟踪时间
	clock clock.WithTicker
	// config 保存管理器的配置
	config Config
	// 用于杀死 pod 的函数
	killPodFunc KillPodFunc
	// 负责镜像垃圾回收的接口
	imageGC ImageGC
	// 负责容器垃圾回收的接口
	containerGC ContainerGC
	// 保护内部状态的读写锁
	sync.RWMutex
	// nodeConditions 保存当前存在的节点条件集合
	nodeConditions []v1.NodeConditionType
	// 记录某个节点条件在阈值被满足时最近一次被观察到的时间
	nodeConditionsLastObservedAt nodeConditionsObservedAt
	// nodeRef 是指向节点的引用
	nodeRef *v1.ObjectReference
	// 用于记录节点相关事件
	recorder record.EventRecorder
	// 用于采集系统资源使用统计
	summaryProvider stats.SummaryProvider
	// 记录每个阈值首次被观察到的时间
	thresholdsFirstObservedAt thresholdsObservedAt
	// 记录已满足（含宽限期）但尚未解除的阈值集合
	thresholdsMet []evictionapi.Threshold
	// signalToRankFunc 把资源信号映射到该资源的排序函数。
	signalToRankFunc map[evictionapi.Signal]rankFunc
	// signalToNodeReclaimFuncs 把资源信号映射到一组按序执行的、知道如何回收该资源的函数。
	signalToNodeReclaimFuncs map[evictionapi.Signal]nodeReclaimFuncs
	// 上一次 synchronize 得到的观测值
	lastObservations signalObservations
	// dedicatedImageFs 指示 imagefs 是否位于与 rootfs 独立的设备上
	dedicatedImageFs *bool
	// splitContainerImageFs 指示 containerfs 是否位于与 imagefs 独立的设备上
	splitContainerImageFs *bool
	// thresholdNotifiers 是一组内存阈值通知器，分别针对每个内存驱逐阈值发出通知
	thresholdNotifiers []ThresholdNotifier
	// thresholdsLastUpdated 是 thresholdNotifiers 最近一次被更新的时间。
	thresholdsLastUpdated time.Time
	// 是否支持本地存储容量隔离
	localStorageCapacityIsolation bool
}

// 确保 managerImpl 实现了所需的接口
var _ Manager = &managerImpl{}

// NewManager 返回一个已配置的 Manager，以及一个用于强制实施驱逐配置的准入处理器。
func NewManager(
	summaryProvider stats.SummaryProvider,
	config Config,
	killPodFunc KillPodFunc,
	imageGC ImageGC,
	containerGC ContainerGC,
	recorder record.EventRecorder,
	nodeRef *v1.ObjectReference,
	clock clock.WithTicker,
	localStorageCapacityIsolation bool,
) (Manager, lifecycle.PodAdmitHandler) {
	manager := &managerImpl{
		clock:                         clock,
		killPodFunc:                   killPodFunc,
		imageGC:                       imageGC,
		containerGC:                   containerGC,
		config:                        config,
		recorder:                      recorder,
		summaryProvider:               summaryProvider,
		nodeRef:                       nodeRef,
		nodeConditionsLastObservedAt:  nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:     thresholdsObservedAt{},
		dedicatedImageFs:              nil,
		splitContainerImageFs:         nil,
		thresholdNotifiers:            []ThresholdNotifier{},
		localStorageCapacityIsolation: localStorageCapacityIsolation,
	}
	return manager, manager
}

// Admit 在 pod 的准入会危及节点稳定性时拒绝该 pod。
func (m *managerImpl) Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	m.RLock()
	defer m.RUnlock()

	logger := klog.FromContext(ctx)

	if len(m.nodeConditions) == 0 {
		return lifecycle.PodAdmitResult{Admit: true}
	}
	// 即使处于资源压力下也要放行 Critical pod，因为它们是系统稳定性所必需的。
	// https://github.com/kubernetes/kubernetes/issues/40573 有更多细节。
	if kubelettypes.IsCriticalPod(attrs.Pod) {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	// 除内存压力之外的其他节点条件会拒绝所有 pod
	nodeOnlyHasMemoryPressureCondition := hasNodeCondition(m.nodeConditions, v1.NodeMemoryPressure) && len(m.nodeConditions) == 1
	if nodeOnlyHasMemoryPressureCondition {
		notBestEffort := v1.PodQOSBestEffort != v1qos.GetPodQOS(attrs.Pod)
		if notBestEffort {
			return lifecycle.PodAdmitResult{Admit: true}
		}

		// 当节点处于内存压力时，检查 BestEffort Pod 的容忍度：
		// 若容忍内存压力污点则放行，对其他容忍（如 DiskPressure）则拒绝。
		if corev1helpers.TolerationsTolerateTaint(logger, attrs.Pod.Spec.Tolerations, &v1.Taint{
			Key:    v1.TaintNodeMemoryPressure,
			Effect: v1.TaintEffectNoSchedule,
		}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators)) {
			return lifecycle.PodAdmitResult{Admit: true}
		}
	}

	return lifecycle.PodAdmitResult{
		Admit:   false,
		Reason:  Reason,
		Message: fmt.Sprintf(nodeConditionMessageFmt, m.nodeConditions),
	}
}

// Start 启动控制循环，观察并响应计算资源不足的情况。
func (m *managerImpl) Start(ctx context.Context, diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc, podCleanedUpFunc PodCleanedUpFunc, monitoringInterval time.Duration) {
	logger := klog.FromContext(ctx)
	thresholdHandler := func(message string) {
		logger.Info(message)
		_, _ = m.synchronize(ctx, diskInfoProvider, podFunc)
	}
	logger.Info("Eviction manager: starting control loop")
	if m.config.KernelMemcgNotification || runtime.GOOS == "windows" {
		for _, threshold := range m.config.Thresholds {
			if threshold.Signal == evictionapi.SignalMemoryAvailable || threshold.Signal == evictionapi.SignalAllocatableMemoryAvailable {
				notifier, err := NewMemoryThresholdNotifier(logger, threshold, m.config.PodCgroupRoot, &CgroupNotifierFactory{}, thresholdHandler)
				if err != nil {
					logger.Info("Eviction manager: failed to create memory threshold notifier", "err", err)
				} else {
					go notifier.Start(ctx)
					m.thresholdNotifiers = append(m.thresholdNotifiers, notifier)
				}
			}
		}
	}
	// 启动驱逐管理器的监控循环
	go func() {
		for {
			if err := ctx.Err(); err != nil {
				logger.Info("Eviction manager: monitoring loop exiting", "err", err)
				return
			}
			evictedPods, err := m.synchronize(ctx, diskInfoProvider, podFunc)
			if evictedPods != nil && err == nil {
				logger.Info("Eviction manager: pods evicted, waiting for pod to be cleaned up", "pods", klog.KObjSlice(evictedPods))
				m.waitForPodsCleanup(logger, podCleanedUpFunc, evictedPods)
			} else {
				if err != nil {
					logger.Error(err, "Eviction manager: failed to synchronize")
				}
				select {
				case <-ctx.Done():
					logger.Info("Eviction manager: monitoring loop exiting", "err", ctx.Err())
					return
				case <-time.After(monitoringInterval):
				}
			}
		}
	}()
}

// IsUnderMemoryPressure 在节点处于内存压力下时返回 true。
func (m *managerImpl) IsUnderMemoryPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodeMemoryPressure)
}

// IsUnderDiskPressure 在节点处于磁盘压力下时返回 true。
func (m *managerImpl) IsUnderDiskPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodeDiskPressure)
}

// IsUnderPIDPressure 在节点处于 PID 压力下时返回 true。
func (m *managerImpl) IsUnderPIDPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodePIDPressure)
}

// synchronize 是实施驱逐阈值的主控制循环。
// 返回被杀死的 pod；如果没有杀死任何 pod 则返回 nil。
func (m *managerImpl) synchronize(ctx context.Context, diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc) ([]*v1.Pod, error) {
	logger := klog.FromContext(ctx)
	// 如果没有需要处理的阈值，直接返回
	thresholds := m.config.Thresholds
	if len(thresholds) == 0 && !m.localStorageCapacityIsolation {
		return nil, nil
	}

	logger.V(3).Info("Eviction manager: synchronize housekeeping")
	// 构建排序函数（若尚未构建）
	// TODO: 在 cadvisor 中提供能告知全局清理是否已完成的函数
	if m.dedicatedImageFs == nil {
		hasImageFs, imageFsErr := diskInfoProvider.HasDedicatedImageFs(ctx)
		if imageFsErr != nil {
			// TODO: 应重构为记录错误并重试 HasDedicatedImageFs
			// 若出现瞬时错误，这里将永远不会重试，也就无法设置驱逐信号
			logger.Error(imageFsErr, "Eviction manager: failed to get HasDedicatedImageFs")
			return nil, fmt.Errorf("eviction manager: failed to get HasDedicatedImageFs: %w", imageFsErr)
		}
		m.dedicatedImageFs = &hasImageFs
		splitContainerImageFs, splitErr := diskInfoProvider.HasDedicatedContainerFs(ctx)
		if splitErr != nil {
			// 常见的错误情况是没有拆分文件系统：
			// 查找拆分文件系统标签会报错，这类错误我们希望忽略
			logger.Error(splitErr, "eviction manager: failed to check if we have separate container filesystem. Ignoring.")
		}

		// 如果实际是拆分文件系统，但该特性处于关闭状态，
		// 应返回错误。
		// 这是不健康的状态。
		if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletSeparateDiskGC) && splitContainerImageFs {
			splitDiskError := fmt.Errorf("KubeletSeparateDiskGC is turned off but we still have a split filesystem")
			return nil, splitDiskError
		}
		thresholds, err := UpdateContainerFsThresholds(m.config.Thresholds, hasImageFs, splitContainerImageFs)
		m.config.Thresholds = thresholds
		if err != nil {
			logger.Error(err, "eviction manager: found conflicting containerfs eviction. Ignoring.")
		}
		m.splitContainerImageFs = &splitContainerImageFs
		m.signalToRankFunc = buildSignalToRankFunc(hasImageFs, splitContainerImageFs)
		m.signalToNodeReclaimFuncs = buildSignalToNodeReclaimFuncs(m.imageGC, m.containerGC, hasImageFs, splitContainerImageFs)
	}

	logger.V(3).Info("FileSystem detection", "DedicatedImageFs", m.dedicatedImageFs, "SplitImageFs", m.splitContainerImageFs)
	activePods := podFunc()
	updateStats := true
	summary, err := m.summaryProvider.Get(ctx, updateStats)
	if err != nil {
		logger.Error(err, "Eviction manager: failed to get summary stats")
		return nil, nil
	}

	if m.clock.Since(m.thresholdsLastUpdated) > notifierRefreshInterval {
		m.thresholdsLastUpdated = m.clock.Now()
		for _, notifier := range m.thresholdNotifiers {
			if err := notifier.UpdateThreshold(ctx, summary); err != nil {
				logger.Info("Eviction manager: failed to update notifier", "notifier", notifier.Description(), "err", err)
			}
		}
	}

	// 生成观测值，并获取一个能基于这些观测值推算 pod 使用统计的函数。
	observations, statsFunc := makeSignalObservations(logger, summary)
	debugLogObservations(logger, "observations", observations)

	// 确定不考虑宽限期、当前已满足的阈值集合
	thresholds = thresholdsMet(logger, thresholds, observations, false)
	debugLogThresholdsWithObservation(logger, "thresholds - ignoring grace period", thresholds, observations)

	// 确定之前已满足、但尚未达到关联最小回收量的阈值集合
	if len(m.thresholdsMet) > 0 {
		thresholdsNotYetResolved := thresholdsMet(logger, m.thresholdsMet, observations, true)
		thresholds = mergeThresholds(thresholds, thresholdsNotYetResolved)
	}
	debugLogThresholdsWithObservation(logger, "thresholds - reclaim not satisfied", thresholds, observations)

	// 跟踪每个阈值首次被观察到的时间
	now := m.clock.Now()
	thresholdsFirstObservedAt := thresholdsFirstObservedAt(thresholds, m.thresholdsFirstObservedAt, now)

	// 由当前观测到的阈值所触发的节点条件集合
	nodeConditions := nodeConditions(thresholds)
	if len(nodeConditions) > 0 {
		logger.V(3).Info("Eviction manager: node conditions - observed", "nodeCondition", nodeConditions)
	}

	// 跟踪每个节点条件最近一次被观察到的时间
	nodeConditionsLastObservedAt := nodeConditionsLastObservedAt(nodeConditions, m.nodeConditionsLastObservedAt, now)

	// 节点条件在过渡期窗口内被观察到时报告为 true
	nodeConditions = nodeConditionsObservedSince(nodeConditionsLastObservedAt, m.config.PressureTransitionPeriod, now)
	if len(nodeConditions) > 0 {
		logger.V(3).Info("Eviction manager: node conditions - transition period not met", "nodeCondition", nodeConditions)
	}

	// 确定需要驱动驱逐行为的阈值集合（即所有宽限期都已满足）
	thresholds = thresholdsMetGracePeriod(logger, thresholdsFirstObservedAt, now)
	debugLogThresholdsWithObservation(logger, "thresholds - grace periods satisfied", thresholds, observations)

	// 更新内部状态
	m.Lock()
	m.nodeConditions = nodeConditions
	m.thresholdsFirstObservedAt = thresholdsFirstObservedAt
	m.nodeConditionsLastObservedAt = nodeConditionsLastObservedAt
	m.thresholdsMet = thresholds

	// 确定自上次同步以来统计值发生变化的阈值集合
	thresholds = thresholdsUpdatedStats(logger, thresholds, observations, m.lastObservations)
	debugLogThresholdsWithObservation(logger, "thresholds - updated stats", thresholds, observations)

	m.lastObservations = observations
	m.Unlock()

	// 若本地卷临时存储存在资源用量违规，则驱逐相关 pod
	// 若驱逐已在 localStorageEviction 中完成，则跳过后续驱逐动作
	if m.localStorageCapacityIsolation {
		if evictedPods := m.localStorageEviction(logger, activePods, statsFunc); len(evictedPods) > 0 {
			return evictedPods, nil
		}
	}

	if len(thresholds) == 0 {
		logger.V(3).Info("Eviction manager: no resources are starved")
		return nil, nil
	}

	// 按驱逐优先级对阈值排序
	sort.Sort(byEvictionPriority(thresholds))
	thresholdToReclaim, resourceToReclaim, foundAny := getReclaimableThreshold(logger, thresholds)
	if !foundAny {
		return nil, nil
	}
	logger.Info("Eviction manager: attempting to reclaim", "resourceName", resourceToReclaim)

	// 记录一条事件，说明当前正尝试通过驱逐回收的资源
	m.recorder.Eventf(m.nodeRef, v1.EventTypeWarning, "EvictionThresholdMet", "Attempting to reclaim %s", resourceToReclaim)

	// 在驱逐终端用户 pod 之前，检查是否有可回收的节点级资源来缓解压力。
	if m.reclaimNodeLevelResources(ctx, thresholdToReclaim.Signal, resourceToReclaim) {
		logger.Info("Eviction manager: able to reduce resource pressure without evicting pods.", "resourceName", resourceToReclaim)
		return nil, nil
	}

	logger.Info("Eviction manager: must evict pod(s) to reclaim", "resourceName", resourceToReclaim)

	// 对 pod 进行驱逐排序
	rank, ok := m.signalToRankFunc[thresholdToReclaim.Signal]
	if !ok {
		logger.Error(nil, "Eviction manager: no ranking function for signal", "threshold", thresholdToReclaim.Signal)
		return nil, nil
	}

	// 唯一适合被驱逐的候选是那些曾有容器在运行的 pod。
	if len(activePods) == 0 {
		logger.Error(nil, "Eviction manager: eviction thresholds have been met, but no pods are active to evict")
		return nil, nil
	}

	// 针对指定资源，对正在运行的 pod 进行驱逐排序
	rank(activePods, statsFunc)

	logger.Info("Eviction manager: pods ranked for eviction", "pods", klog.KObjSlice(activePods))

	// 记录当前用于驱逐的已满足阈值的指标年龄。
	for _, t := range thresholds {
		timeObserved := observations[t.Signal].time
		if !timeObserved.IsZero() {
			metrics.EvictionStatsAge.WithLabelValues(string(t.Signal)).Observe(metrics.SinceInSeconds(timeObserved.Time))
		}
	}

	// 每个驱逐间隔内最多杀死一个 pod，避免一次性驱逐过多
	for i := range activePods {
		pod := activePods[i]
		gracePeriodOverride := int64(immediateEvictionGracePeriodSeconds)
		if !isHardEvictionThreshold(thresholdToReclaim) {
			gracePeriodOverride = m.config.MaxPodGracePeriodSeconds
			if pod.Spec.TerminationGracePeriodSeconds != nil {
				gracePeriodOverride = min(m.config.MaxPodGracePeriodSeconds, *pod.Spec.TerminationGracePeriodSeconds)
			}
		}

		message, annotations := evictionMessage(resourceToReclaim, pod, statsFunc, thresholds, observations)
		condition := &v1.PodCondition{
			Type:               v1.DisruptionTarget,
			ObservedGeneration: pod.Generation,
			Status:             v1.ConditionTrue,
			Reason:             v1.PodReasonTerminationByKubelet,
			Message:            message,
		}
		if m.evictPod(logger, pod, gracePeriodOverride, message, annotations, condition) {
			metrics.Evictions.WithLabelValues(string(thresholdToReclaim.Signal)).Inc()
			return []*v1.Pod{pod}, nil
		}
	}
	logger.Info("Eviction manager: unable to evict any pods from the node")
	return nil, nil
}

// waitForPodsCleanup 轮询等待被驱逐的 pod 全部完成清理，
// 超过 podCleanupTimeout 仍未清理完则放弃等待并记录日志。
func (m *managerImpl) waitForPodsCleanup(logger klog.Logger, podCleanedUpFunc PodCleanedUpFunc, pods []*v1.Pod) {
	timeout := m.clock.NewTimer(podCleanupTimeout)
	defer timeout.Stop()
	ticker := m.clock.NewTicker(podCleanupPollFreq)
	defer ticker.Stop()
	for {
		select {
		case <-timeout.C():
			logger.Info("Eviction manager: timed out waiting for pods to be cleaned up", "pods", klog.KObjSlice(pods))
			return
		case <-ticker.C():
			for i, pod := range pods {
				if !podCleanedUpFunc(pod) {
					break
				}
				if i == len(pods)-1 {
					logger.Info("Eviction manager: pods successfully cleaned up", "pods", klog.KObjSlice(pods))
					return
				}
			}
		}
	}
}

// reclaimNodeLevelResources 尝试回收节点级资源。
// 若阈值已被满足且无需驱逐 pod，则返回 true。
func (m *managerImpl) reclaimNodeLevelResources(ctx context.Context, signalToReclaim evictionapi.Signal, resourceToReclaim v1.ResourceName) bool {
	logger := klog.FromContext(ctx)
	nodeReclaimFuncs := m.signalToNodeReclaimFuncs[signalToReclaim]
	for _, nodeReclaimFunc := range nodeReclaimFuncs {
		// 尝试回收处于压力的资源。
		if err := nodeReclaimFunc(ctx); err != nil {
			logger.Info("Eviction manager: unexpected error when attempting to reduce resource pressure", "resourceName", resourceToReclaim, "err", err)
		}

	}
	if len(nodeReclaimFuncs) > 0 {
		summary, err := m.summaryProvider.Get(ctx, true)
		if err != nil {
			logger.Error(err, "Eviction manager: failed to get summary stats after resource reclaim")
			return false
		}

		// 生成观测值，并获取一个能基于这些观测值推算 pod 使用统计的函数。
		observations, _ := makeSignalObservations(logger, summary)
		debugLogObservations(logger, "observations after resource reclaim", observations)

		// 在忽略宽限期的条件下评估所有阈值，
		// 判断基于新的观测值是否已达到最小回收目标
		thresholds := thresholdsMet(logger, m.config.Thresholds, observations, true)
		debugLogThresholdsWithObservation(logger, "thresholds after resource reclaim - ignoring grace period", thresholds, observations)

		if len(thresholds) == 0 {
			return true
		}
	}
	return false
}

// localStorageEviction 检查每个 pod 的 EmptyDir 卷用量，判断其是否超过指定上限、
// 需要被驱逐。同时检查 pod 内的每个容器，若容器 overlay 用量超过上限，也会驱逐该 pod。
func (m *managerImpl) localStorageEviction(logger klog.Logger, pods []*v1.Pod, statsFunc statsFunc) []*v1.Pod {
	evicted := []*v1.Pod{}
	for _, pod := range pods {
		podStats, ok := statsFunc(pod)
		if !ok {
			continue
		}

		if m.emptyDirLimitEviction(logger, podStats, pod) {
			evicted = append(evicted, pod)
			continue
		}

		if m.podEphemeralStorageLimitEviction(logger, podStats, pod) {
			evicted = append(evicted, pod)
			continue
		}

		if m.containerEphemeralStorageLimitEviction(logger, podStats, pod) {
			evicted = append(evicted, pod)
		}
	}

	return evicted
}

// emptyDirLimitEviction 检查 pod 的 emptyDir 卷用量是否超过其 sizeLimit，
// 超限则立即驱逐该 pod，返回是否成功发起驱逐。
func (m *managerImpl) emptyDirLimitEviction(logger klog.Logger, podStats statsapi.PodStats, pod *v1.Pod) bool {
	podVolumeUsed := make(map[string]*resource.Quantity)
	for _, volume := range podStats.VolumeStats {
		podVolumeUsed[volume.Name] = resource.NewQuantity(int64(*volume.UsedBytes), resource.BinarySI)
	}
	for i := range pod.Spec.Volumes {
		source := &pod.Spec.Volumes[i].VolumeSource
		if source.EmptyDir != nil {
			if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) && source.EmptyDir.Medium == v1.StorageMediumMemory {
				// 内存型 emptyDir 卷由 tmpfs 承载，其大小上限由内核强制实施，
				// 无需驱逐管理器再做检查。
				continue
			}
			size := source.EmptyDir.SizeLimit
			used := podVolumeUsed[pod.Spec.Volumes[i].Name]
			if used != nil && size != nil && size.Sign() == 1 && used.Cmp(*size) > 0 {
				// emptyDir 用量超过 sizeLimit，驱逐该 pod
				if m.evictPod(logger, pod, immediateEvictionGracePeriodSeconds, fmt.Sprintf(emptyDirMessageFmt, pod.Spec.Volumes[i].Name, size.String()), nil, nil) {
					metrics.Evictions.WithLabelValues(signalEmptyDirFsLimit).Inc()
					return true
				}
				return false
			}
		}
	}

	return false
}

// podEphemeralStorageLimitEviction 检查 pod 的临时存储总用量是否超过
// pod 级 ephemeral-storage 上限，超限则立即驱逐该 pod。
func (m *managerImpl) podEphemeralStorageLimitEviction(logger klog.Logger, podStats statsapi.PodStats, pod *v1.Pod) bool {
	podLimits := resourcehelper.PodLimits(pod, resourcehelper.PodResourcesOptions{})
	_, found := podLimits[v1.ResourceEphemeralStorage]
	if !found {
		return false
	}

	// pod stats API 汇总了临时存储用量（容器、emptyDir、宿主机文件如 etc-hosts、日志等）
	podEphemeralStorageTotalUsage := &resource.Quantity{}
	if podStats.EphemeralStorage != nil && podStats.EphemeralStorage.UsedBytes != nil {
		podEphemeralStorageTotalUsage = resource.NewQuantity(int64(*podStats.EphemeralStorage.UsedBytes), resource.BinarySI)
	}
	podEphemeralStorageLimit := podLimits[v1.ResourceEphemeralStorage]
	if podEphemeralStorageTotalUsage.Cmp(podEphemeralStorageLimit) > 0 {
		// pod 总用量超过容器的总大小上限，驱逐该 pod
		message := fmt.Sprintf(podEphemeralStorageMessageFmt, podEphemeralStorageLimit.String())
		if m.evictPod(logger, pod, immediateEvictionGracePeriodSeconds, message, nil, nil) {
			metrics.Evictions.WithLabelValues(signalEphemeralPodFsLimit).Inc()
			return true
		}
		return false
	}
	return false
}

// containerEphemeralStorageLimitEviction 检查 pod 内每个容器（含可重启 init 容器）的
// 临时存储用量是否超过其 ephemeral-storage 上限，首个超限的容器即触发驱逐。
func (m *managerImpl) containerEphemeralStorageLimitEviction(logger klog.Logger, podStats statsapi.PodStats, pod *v1.Pod) bool {
	thresholdsMap := make(map[string]*resource.Quantity)
	for _, container := range pod.Spec.Containers {
		ephemeralLimit := container.Resources.Limits.StorageEphemeral()
		if ephemeralLimit != nil && ephemeralLimit.Value() != 0 {
			thresholdsMap[container.Name] = ephemeralLimit
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if !podutil.IsRestartableInitContainer(&container) {
			continue
		}
		ephemeralLimit := container.Resources.Limits.StorageEphemeral()
		if ephemeralLimit != nil && ephemeralLimit.Value() != 0 {
			thresholdsMap[container.Name] = ephemeralLimit
		}
	}

	for _, containerStat := range podStats.Containers {
		containerUsed := diskUsage(containerStat.Logs)
		if !*m.dedicatedImageFs {
			containerUsed.Add(*diskUsage(containerStat.Rootfs))
		}

		if ephemeralStorageThreshold, ok := thresholdsMap[containerStat.Name]; ok {
			if ephemeralStorageThreshold.Cmp(*containerUsed) < 0 {
				if m.evictPod(logger, pod, immediateEvictionGracePeriodSeconds, fmt.Sprintf(containerEphemeralStorageMessageFmt, containerStat.Name, ephemeralStorageThreshold.String()), nil, nil) {
					metrics.Evictions.WithLabelValues(signalEphemeralContainerFsLimit).Inc()
					return true
				}
				return false
			}
		}
	}
	return false
}

// evictPod 通过 killPodFunc 杀死指定 pod，并记录驱逐事件与可选的
// DisruptionTarget 条件。返回是否成功发起驱逐。
func (m *managerImpl) evictPod(logger klog.Logger, pod *v1.Pod, gracePeriodOverride int64, evictMsg string, annotations map[string]string, condition *v1.PodCondition) bool {
	// 如果 pod 被标记为 critical 且是静态 pod，并且已启用 critical pod 注解支持，
	// 则不驱逐此类 pod。静态 pod 在驱逐后不会被重新准入。
	// https://github.com/kubernetes/kubernetes/issues/40573 有更多细节。
	if kubelettypes.IsCriticalPod(pod) {
		logger.Error(nil, "Eviction manager: cannot evict a critical pod", "pod", klog.KObj(pod))
		return false
	}
	// 记录一条事件，标记正在驱逐该 pod
	//nolint:forbidigo // Legacy usage
	m.recorder.AnnotatedEventf(pod, annotations, v1.EventTypeWarning, Reason, "%s", evictMsg)
	// 这是阻塞调用，只有当 pod 及其容器全部被杀死时才会返回。
	logger.V(3).Info("Evicting pod", "pod", klog.KObj(pod), "podUID", pod.UID, "message", evictMsg)
	err := m.killPodFunc(pod, true, &gracePeriodOverride, func(status *v1.PodStatus) {
		status.Phase = v1.PodFailed
		status.Reason = Reason
		status.Message = evictMsg
		if condition != nil {
			condition.ObservedGeneration = podutil.CalculatePodConditionObservedGeneration(status, pod.Generation, v1.DisruptionTarget)
			podutil.UpdatePodCondition(status, condition)
		}
	})
	if err != nil {
		logger.Error(err, "Eviction manager: pod failed to evict", "pod", klog.KObj(pod))
	} else {
		logger.Info("Eviction manager: pod is evicted successfully", "pod", klog.KObj(pod))
	}
	return true
}
