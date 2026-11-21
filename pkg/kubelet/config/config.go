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

package config

import (
	"context"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)

// podStartupSLIObserver 观察 Pod 首次被 watch 到的时间，用于统计 Pod 启动 SLI。
type podStartupSLIObserver interface {
	ObservedPodOnWatch(logger klog.Logger, pod *v1.Pod, when time.Time)
}

// PodConfig 是一个配置多路复用器：它把多个来源的 Pod 配置合并为单一且一致的结构，
// 并按顺序向监听者投递增量变更通知。
type PodConfig struct {
	pods *podStorage
	mux  *mux

	// 传递给监听者的反规范化变更通道。
	updates chan kubetypes.PodUpdate

	// 保存所有已配置来源的集合，sourcesLock 保护其并发访问。
	sourcesLock sync.Mutex
	sources     sets.Set[string]
}

// sourceUpdate 表示某个配置源提交的一次 Pod 列表更新。
type sourceUpdate struct {
	Pods []*v1.Pod
}

// NewPodConfig 创建一个对象，把多个配置源合并为一条规范化的 Pod 配置更新流。
func NewPodConfig(recorder record.EventRecorderLogger, startupSLIObserver podStartupSLIObserver) *PodConfig {
	// 更新通道带缓冲，避免短暂的消费滞后阻塞合并流程。
	updates := make(chan kubetypes.PodUpdate, 50)
	storage := newPodStorage(updates, recorder, startupSLIObserver)
	podConfig := &PodConfig{
		pods:    storage,
		mux:     newMux(storage),
		updates: updates,
		sources: sets.Set[string]{},
	}
	return podConfig
}

// Channel 创建或返回指定配置源的通道，该通道只接受 Pod 更新。
func (c *PodConfig) Channel(ctx context.Context, source string) chan<- sourceUpdate {
	c.sourcesLock.Lock()
	defer c.sourcesLock.Unlock()
	c.sources.Insert(source)
	return c.mux.ChannelWithContext(ctx, source)
}

// SourcesReadyFn 返回一个就绪回调，内部使用给定 logger 调用 SeenAllSources。
func (c *PodConfig) SourcesReadyFn(logger klog.Logger) SourcesReadyFn {
	return func(seenSources sets.Set[string]) bool {
		return c.SeenAllSources(logger, seenSources)
	}
}

// SeenAllSources 在 seenSources 覆盖配置中的全部来源，
// 且每个来源都已发送过 SET 消息时返回 true。
func (c *PodConfig) SeenAllSources(logger klog.Logger, seenSources sets.Set[string]) bool {
	if c.pods == nil {
		return false
	}
	c.sourcesLock.Lock()
	defer c.sourcesLock.Unlock()
	logger.V(5).Info("Looking for sources, have seen", "sources", sets.List(c.sources), "seenSources", seenSources)
	return seenSources.HasAll(sets.List(c.sources)...) && c.pods.seenSources(sets.List(c.sources)...)
}

// Updates 返回一个经过反规范化处理的配置更新通道。
func (c *PodConfig) Updates() <-chan kubetypes.PodUpdate {
	return c.updates
}

// podStorage 维护任意时刻的 Pod 状态，并保证更新按顺序投递到通道。
// 注意：该对象是内存中的“真相来源”，创建时不含任何条目；
// 一旦所有先前读取的来源都可用，它才应被视为权威数据。
type podStorage struct {
	podLock sync.RWMutex
	// 从来源名到 Pod UID 再到 Pod 引用的两级映射。
	pods map[string]map[types.UID]*v1.Pod

	// 保证更新在 updates 通道上严格有序投递。
	updateLock sync.Mutex
	updates    chan<- kubetypes.PodUpdate

	// 保存所有至少发送过一次 SET 的来源集合。
	sourcesSeenLock sync.RWMutex
	sourcesSeen     sets.Set[string]

	// 用于上报事件的事件记录器。
	recorder record.EventRecorderLogger

	// 观察 Pod 首次被 watch 到的时间。
	startupSLIObserver podStartupSLIObserver
}

// TODO: 未来 PodConfigNotificationMode 可以交由 updates 通道的监听者处理，
// 在多监听者场景下尤其如此。
// TODO: 允许使用快照版本初始化存储的当前状态。
func newPodStorage(updates chan<- kubetypes.PodUpdate, recorder record.EventRecorderLogger, startupSLIObserver podStartupSLIObserver) *podStorage {
	return &podStorage{
		pods:               make(map[string]map[types.UID]*v1.Pod),
		updates:            updates,
		sourcesSeen:        sets.Set[string]{},
		recorder:           recorder,
		startupSLIObserver: startupSLIObserver,
	}
}

// Merge 将某个来源的更新规范化并合并进全量 Pod 映射，过滤掉冗余更新后，
// 把零个或多个最小化的更新推入更新通道，并保证投递顺序。
func (s *podStorage) Merge(ctx context.Context, source string, update sourceUpdate) error {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	// 记录本次更新前是否已见过该来源，用于识别“首次 SET”。
	seenBefore := s.sourcesSeen.Has(source)
	adds, updates, deletes, removes, reconciles := s.merge(ctx, source, update)
	firstSet := !seenBefore && s.sourcesSeen.Has(source)

	// 按固定顺序投递各类更新通知：先 REMOVE，再 ADD/UPDATE/DELETE，最后 RECONCILE。
	if len(removes.Pods) > 0 {
		s.updates <- *removes
	}
	if len(adds.Pods) > 0 {
		s.updates <- *adds
	}
	if len(updates.Pods) > 0 {
		s.updates <- *updates
	}
	if len(deletes.Pods) > 0 {
		s.updates <- *deletes
	}
	if firstSet && len(adds.Pods) == 0 && len(updates.Pods) == 0 && len(deletes.Pods) == 0 {
		// 首次见到该来源且没有任何 ADD/UPDATE/DELETE 时发送一个空更新，
		// 以此告知 kubelet 该来源已就绪。
		s.updates <- *adds
	}
	// 只在此处加入 reconcile 支持，因为 kubelet 目前尚不支持 Snapshot 更新。
	if len(reconciles.Pods) > 0 {
		s.updates <- *reconciles
	}

	return nil
}

// merge 计算某个来源的更新相对于现有缓存的差异，
// 并返回 ADD、UPDATE、DELETE、REMOVE、RECONCILE 五类更新。
func (s *podStorage) merge(ctx context.Context, source string, update sourceUpdate) (adds, updates, deletes, removes, reconciles *kubetypes.PodUpdate) {
	s.podLock.Lock()
	defer s.podLock.Unlock()
	logger := klog.FromContext(ctx)

	addPods := []*v1.Pod{}
	updatePods := []*v1.Pod{}
	deletePods := []*v1.Pod{}
	removePods := []*v1.Pod{}
	reconcilePods := []*v1.Pod{}

	pods := s.pods[source]
	if pods == nil {
		pods = make(map[types.UID]*v1.Pod)
	}

	// updatePodsFunc 是用新 Pod 列表 newPods 更新缓存 oldPods 的局部函数。
	// 更新完成后，新的 Pod 会存入缓存 pods。
	// 注意 pods 与 oldPods 可能是同一个缓存。
	updatePodsFunc := func(newPods []*v1.Pod, oldPods, pods map[types.UID]*v1.Pod) {
		filtered := filterInvalidPods(logger, newPods, source, s.recorder)
		for _, ref := range filtered {
			// 在任何比较之前先为 Pod 标注来源，避免来源注解差异被误判为变更。
			if ref.Annotations == nil {
				ref.Annotations = make(map[string]string)
			}
			ref.Annotations[kubetypes.ConfigSourceAnnotationKey] = source
			// 静态 Pod 由节点自身管理，不纳入启动 SLI 统计。
			if !kubetypes.IsStaticPod(ref) {
				s.startupSLIObserver.ObservedPodOnWatch(logger, ref, time.Now())
			}
			if existing, found := oldPods[ref.UID]; found {
				pods[ref.UID] = existing
				needUpdate, needReconcile, needGracefulDelete := checkAndUpdatePod(existing, ref)
				if needUpdate {
					updatePods = append(updatePods, existing)
				} else if needReconcile {
					reconcilePods = append(reconcilePods, existing)
				} else if needGracefulDelete {
					deletePods = append(deletePods, existing)
				}
				continue
			}
			recordFirstSeenTime(logger, ref)
			pods[ref.UID] = ref
			addPods = append(addPods, ref)
		}
	}

	logger.V(4).Info("Setting pods for source", "source", source)
	s.markSourceSet(source)
	// 通过创建新 map 清空旧条目，避免原地修改带来的并发问题。
	oldPods := pods
	pods = make(map[types.UID]*v1.Pod)
	updatePodsFunc(update.Pods, oldPods, pods)
	// 旧缓存中存在但新列表中缺失的 Pod 即为 REMOVE。
	for uid, existing := range oldPods {
		if _, found := pods[uid]; !found {
			removePods = append(removePods, existing)
		}
	}

	s.pods[source] = pods

	adds = &kubetypes.PodUpdate{Op: kubetypes.ADD, Pods: copyPods(addPods), Source: source}
	updates = &kubetypes.PodUpdate{Op: kubetypes.UPDATE, Pods: copyPods(updatePods), Source: source}
	deletes = &kubetypes.PodUpdate{Op: kubetypes.DELETE, Pods: copyPods(deletePods), Source: source}
	removes = &kubetypes.PodUpdate{Op: kubetypes.REMOVE, Pods: copyPods(removePods), Source: source}
	reconciles = &kubetypes.PodUpdate{Op: kubetypes.RECONCILE, Pods: copyPods(reconcilePods), Source: source}

	return adds, updates, deletes, removes, reconciles
}

// markSourceSet 记录某个来源已至少发送过一次 SET。
func (s *podStorage) markSourceSet(source string) {
	s.sourcesSeenLock.Lock()
	defer s.sourcesSeenLock.Unlock()
	s.sourcesSeen.Insert(source)
}

// seenSources 判断给定的所有来源是否都已发送过 SET。
func (s *podStorage) seenSources(sources ...string) bool {
	s.sourcesSeenLock.RLock()
	defer s.sourcesSeenLock.RUnlock()
	return s.sourcesSeen.HasAll(sources...)
}

// filterInvalidPods 过滤同一来源中 Pod 全名重复的条目，并为冲突上报事件。
func filterInvalidPods(logger klog.Logger, pods []*v1.Pod, source string, recorder record.EventRecorderLogger) (filtered []*v1.Pod) {
	names := sets.Set[string]{}
	for i, pod := range pods {
		// 假定每个来源的 Pod 已各自通过校验，此函数只检查是否存在命名冲突。
		name := kubecontainer.GetPodFullName(pod)
		if names.Has(name) {
			logger.Info("Pod failed validation due to duplicate pod name, ignoring", "index", i, "pod", klog.KObj(pod), "source", source)
			recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedValidation, "Error validating pod %s from %s due to duplicate pod name %q, ignoring", format.Pod(pod), source, pod.Name)
			continue
		} else {
			names.Insert(name)
		}

		filtered = append(filtered, pod)
	}
	return
}

// localAnnotations 是 kubelet 自行添加到 Pod 上的注解，比较时需忽略。
var localAnnotations = []string{
	kubetypes.ConfigSourceAnnotationKey,
	kubetypes.ConfigMirrorAnnotationKey,
	kubetypes.ConfigFirstSeenAnnotationKey,
}

// isLocalAnnotationKey 判断给定键是否为 kubelet 本地管理的注解。
func isLocalAnnotationKey(key string) bool {
	for _, localKey := range localAnnotations {
		if key == localKey {
			return true
		}
	}
	return false
}

// isAnnotationMapEqual 在忽略本地注解的前提下比较两个注解 map 是否相等。
func isAnnotationMapEqual(existingMap, candidateMap map[string]string) bool {
	if candidateMap == nil {
		candidateMap = make(map[string]string)
	}
	for k, v := range candidateMap {
		if isLocalAnnotationKey(k) {
			continue
		}
		if existingValue, ok := existingMap[k]; ok && existingValue == v {
			continue
		}
		return false
	}
	for k := range existingMap {
		if isLocalAnnotationKey(k) {
			continue
		}
		// 现有 map 中存在候选 map 已删除的陈旧条目。
		if _, exists := candidateMap[k]; !exists {
			return false
		}
	}
	return true
}

// recordFirstSeenTime 记录该 Pod 首次被看到的本地时间。
func recordFirstSeenTime(logger klog.Logger, pod *v1.Pod) {
	logger.V(4).Info("Receiving a new pod", "pod", klog.KObj(pod))
	pod.Annotations[kubetypes.ConfigFirstSeenAnnotationKey] = kubetypes.NewTimestamp().GetString()
}

// updateAnnotations 返回一个新的注解 map：
// 由 API 对象中的注解加上 kubelet 本地管理的注解组成。
func updateAnnotations(existing, ref *v1.Pod) {
	annotations := make(map[string]string, len(ref.Annotations)+len(localAnnotations))
	for k, v := range ref.Annotations {
		annotations[k] = v
	}
	for _, k := range localAnnotations {
		if v, ok := existing.Annotations[k]; ok {
			annotations[k] = v
		}
	}
	existing.Annotations = annotations
}

// podsDifferSemantically 判断两个 Pod 在语义上是否存在差异
// （忽略 kubelet 本地注解等不影响调度的字段）。
func podsDifferSemantically(existing, ref *v1.Pod) bool {
	if reflect.DeepEqual(existing.Spec, ref.Spec) &&
		reflect.DeepEqual(existing.Labels, ref.Labels) &&
		reflect.DeepEqual(existing.DeletionTimestamp, ref.DeletionTimestamp) &&
		reflect.DeepEqual(existing.DeletionGracePeriodSeconds, ref.DeletionGracePeriodSeconds) &&
		isAnnotationMapEqual(existing.Annotations, ref.Annotations) {
		return false
	}
	return true
}

// checkAndUpdatePod 更新 existing，并按以下规则返回标记：
//   - 若 ref 带来实质性变化，返回 needUpdate=true；
//   - 若 ref 带来实质性变化且属于优雅删除，返回 needGracefulDelete=true；
//   - 若 ref 无实质性变化但改变了 Pod 状态，返回 needReconcile=true；
//   - 否则全部返回 false。
//     目前 needUpdate、needGracefulDelete 与 needReconcile 不会同时为 true。
func checkAndUpdatePod(existing, ref *v1.Pod) (needUpdate, needReconcile, needGracefulDelete bool) {

	// 1. 可能是 reconcile 场景。
	// TODO: 更好的做法是整体更新对象，只保留来源注解或 UID 等特定字段（以保证安全）。
	if !podsDifferSemantically(existing, ref) {
		// 没有实质性变化，不算更新。
		// 只在非更新场景检查 reconcile：若 Pod 本就要更新，额外的 reconcile 是多余的。
		if !reflect.DeepEqual(existing.Status, ref.Status) {
			// 状态发生变化的 Pod 需要 reconcile，因为 kubelet 应是 Pod 状态的真相来源。
			existing.Status = ref.Status
			needReconcile = true
		}
		return
	}

	// 用已有的首次看到时间覆盖 ref 中的值：这是 kubelet 内部注解，无需更新。
	ref.Annotations[kubetypes.ConfigFirstSeenAnnotationKey] = existing.Annotations[kubetypes.ConfigFirstSeenAnnotationKey]

	existing.Spec = ref.Spec
	existing.Labels = ref.Labels
	existing.DeletionTimestamp = ref.DeletionTimestamp
	existing.DeletionGracePeriodSeconds = ref.DeletionGracePeriodSeconds
	existing.Generation = ref.Generation
	existing.Status = ref.Status
	updateAnnotations(existing, ref)

	// 2. 带删除时间戳表示这是一次优雅删除。
	if ref.DeletionTimestamp != nil {
		needGracefulDelete = true
	} else {
		// 3. 否则是一次普通更新。
		needUpdate = true
	}

	return
}

// copyPods 深拷贝 Pod 列表，避免调用方后续修改影响已投递的更新。
func copyPods(sourcePods []*v1.Pod) []*v1.Pod {
	pods := []*v1.Pod{}
	for _, source := range sourcePods {
		// 这里使用深拷贝以防调用方修改原始对象。
		pods = append(pods, source.DeepCopy())
	}
	return pods
}
