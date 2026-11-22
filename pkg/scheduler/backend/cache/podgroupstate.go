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

// Package cache 中的本文件实现 PodGroup 与 CompositePodGroup 的运行时状态管理。
package cache

import (
	"maps"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
)

var generation atomic.Int64

// nextPodGroupGeneration 为 PodGroup 状态分配全局单调递增的 generation，而不是在每个实例内自增。
// 这样同名 PodGroup 被删除后重建时，generation 不会重置或与之前的实例冲突。
func nextPodGroupGeneration() int64 {
	return generation.Add(1)
}

// podGroupStateData 保存 podGroupState 与 podGroupStateSnapshot 共享的数据和操作。
// 注意 podGroup 字段来自观察到的 PodGroup API 对象，其余字段来自观察到的 Pod 对象。
// 因此只要至少有一个 Pod 引用它，podGroupStateData 就可以在没有对应 PodGroup API 对象的情况下存在。
type podGroupStateData struct {
	// 数据变化时递增，用于检测变更并在打快照时避免不必要的克隆。
	generation int64
	// 记录调度器已知的、属于该组的所有 Pod。
	allPods map[types.UID]*v1.Pod
	// 记录该组中尚未调度的 Pod，即既未 assume 也未分配。
	unscheduledPods sets.Set[types.UID]
	// 记录已到达 Reserve 阶段、等待 gang 中其余 Pod 到齐后才允许绑定的 Pod。
	assumedPods map[types.UID]*v1.Pod
	// 记录该组中已分配（绑定）的 Pod。
	assignedPods sets.Set[types.UID]
	// 缓存的 PodGroup API 对象。
	podGroup *schedulingv1beta1.PodGroup
}

func newPodGroupStateData() podGroupStateData {
	return podGroupStateData{
		allPods:         make(map[types.UID]*v1.Pod),
		unscheduledPods: sets.New[types.UID](),
		assumedPods:     make(map[types.UID]*v1.Pod),
		assignedPods:    sets.New[types.UID](),
	}
}

// addPod 把 Pod 加入该组，依据 NodeName 将其放入 assignedPods 或 unscheduledPods。
func (d *podGroupStateData) addPod(pod *v1.Pod) {
	d.generation = nextPodGroupGeneration()
	d.allPods[pod.UID] = pod
	if pod.Spec.NodeName != "" {
		d.assignedPods.Insert(pod.UID)
		// 清理该 Pod 之前可能处于的其他状态，例如先前排队的组成员被外部绑定。
		d.unscheduledPods.Delete(pod.UID)
		delete(d.assumedPods, pod.UID)
	} else {
		d.unscheduledPods.Insert(pod.UID)
	}
}

// updatePod 更新组内的 Pod，绑定时将其移入 assignedPods。
func (d *podGroupStateData) updatePod(oldPod, newPod *v1.Pod) {
	d.generation = nextPodGroupGeneration()
	d.allPods[newPod.UID] = newPod
	if oldPod.Spec.NodeName == "" && newPod.Spec.NodeName != "" {
		d.assignedPods.Insert(newPod.UID)
		// 分配后从 unscheduled 与 assumed 中清除。
		d.unscheduledPods.Delete(newPod.UID)
		delete(d.assumedPods, newPod.UID)
	}
}

// deletePod 从该组状态中移除 Pod。
func (d *podGroupStateData) deletePod(podUID types.UID) {
	d.generation = nextPodGroupGeneration()
	delete(d.allPods, podUID)
	d.unscheduledPods.Delete(podUID)
	delete(d.assumedPods, podUID)
	d.assignedPods.Delete(podUID)
}

// assumePod 将 Pod 标记为组内已 assume。
func (d *podGroupStateData) assumePod(pod *v1.Pod) {
	storedPod, ok := d.allPods[pod.UID]
	// 调度中的 Pod 可能已从集群删除，此时直接忽略。
	if !ok {
		return
	}

	d.generation = nextPodGroupGeneration()
	// 状态中保存的 Pod 若已分配，则放入 assignedPods，否则放入 assumedPods。
	if storedPod.Spec.NodeName != "" {
		d.assignedPods.Insert(pod.UID)
	} else {
		d.assumedPods[pod.UID] = pod
	}
	d.unscheduledPods.Delete(pod.UID)
}

// forgetPod 将 Pod 从 assume 状态退回组内未调度状态。
func (d *podGroupStateData) forgetPod(podUID types.UID) {

	pod := d.allPods[podUID]
	// 调度中的 Pod 可能已从集群删除，此时直接忽略。
	if pod == nil {
		return
	}

	d.generation = nextPodGroupGeneration()

	delete(d.assumedPods, podUID)

	// 已分配的放入 assignedPods，否则放回 unscheduledPods。
	if pod.Spec.NodeName != "" {
		d.assignedPods.Insert(podUID)
	} else {
		d.unscheduledPods.Insert(podUID)
	}
}

// scheduledPods 返回该组中已 assume 或已分配的 Pod。
func (d *podGroupStateData) scheduledPods() []*v1.Pod {
	scheduledPods := make([]*v1.Pod, 0, len(d.assignedPods)+len(d.assumedPods))
	for uid := range d.assignedPods {
		scheduledPods = append(scheduledPods, d.allPods[uid])
	}
	for _, pod := range d.assumedPods {
		scheduledPods = append(scheduledPods, pod)
	}
	return scheduledPods
}

// empty 在组内无任何 Pod 时返回 true。
func (d *podGroupStateData) empty() bool {
	return len(d.allPods) == 0 && d.podGroup == nil
}

// allPodsCount 返回调度器已知的该组 Pod 总数。
func (d *podGroupStateData) allPodsCount() int {
	return len(d.allPods)
}

// scheduledPodsCount 返回该组中已 assume 或已分配的 Pod 数量。
func (d *podGroupStateData) scheduledPodsCount() int {
	return len(d.assumedPods) + len(d.assignedPods)
}

// clone 克隆组状态数据。
// 不深拷贝内部的 Pod 与 PodGroup 对象：它们不应被调度器修改。
// 从外部看，缓存与快照中的对象都是只读的，除非由相应方法显式修改。
func (d *podGroupStateData) clone() podGroupStateData {
	return podGroupStateData{
		generation:      d.generation,
		allPods:         maps.Clone(d.allPods),
		unscheduledPods: d.unscheduledPods.Clone(),
		assumedPods:     maps.Clone(d.assumedPods),
		assignedPods:    d.assignedPods.Clone(),
		podGroup:        d.podGroup,
	}
}

// setPodGroup 设置 PodGroup 对象。
func (d *podGroupStateData) setPodGroup(podGroup *schedulingv1beta1.PodGroup) {
	d.generation = nextPodGroupGeneration()
	d.podGroup = podGroup
}

// removePodGroup 移除 PodGroup 对象。
func (d *podGroupStateData) removePodGroup() {
	d.generation = nextPodGroupGeneration()
	d.podGroup = nil
}

// unscheduledPodsMap 返回该组所有未调度的 Pod，以 Pod 名为键。
func (d *podGroupStateData) unscheduledPodsMap() map[string]*v1.Pod {
	result := make(map[string]*v1.Pod, len(d.unscheduledPods))
	for podUID := range d.unscheduledPods {
		pod := d.allPods[podUID]
		result[pod.Name] = pod
	}
	return result
}

// compositePodGroupStateData 保存 compositePodGroupState 与 compositePodGroupStateSnapshot 共享的数据和操作。
// 注意 compositePodGroup 来自观察到的 CompositePodGroup API 对象，其余字段来自观察到的子 PodGroup 与 CompositePodGroup。
// 因此只要至少有一个子对象引用它，就可以在没有对应 CompositePodGroup API 对象的情况下存在。
type compositePodGroupStateData struct {
	// 数据变化时递增，用于检测变更并在打快照时避免不必要的克隆。
	generation int64
	// 缓存的 CompositePodGroup API 对象。
	compositePodGroup *schedulingv1alpha3.CompositePodGroup
	// 记录所有子 PodGroup 与 CompositePodGroup 的键。
	children sets.Set[fwk.EntityKey]
}

func newCompositePodGroupStateData() compositePodGroupStateData {
	return compositePodGroupStateData{
		children: sets.New[fwk.EntityKey](),
	}
}

// clone 克隆组合组状态数据。
// 不深拷贝内部的 CompositePodGroup 对象：它不应被调度器修改。
// 从外部看，缓存与快照中的对象都是只读的，除非由相应方法显式修改。
func (d *compositePodGroupStateData) clone() compositePodGroupStateData {
	return compositePodGroupStateData{
		generation:        d.generation,
		compositePodGroup: d.compositePodGroup,
		children:          d.children.Clone(),
	}
}

// empty 在组合组状态中既无 CompositePodGroup 也无子对象时返回 true。
func (d *compositePodGroupStateData) empty() bool {
	return d.compositePodGroup == nil && len(d.children) == 0
}

func (d *compositePodGroupStateData) setCompositePodGroup(compositePodGroup *schedulingv1alpha3.CompositePodGroup) {
	d.generation = nextPodGroupGeneration()
	d.compositePodGroup = compositePodGroup
}

func (d *compositePodGroupStateData) removeCompositePodGroup() {
	d.generation = nextPodGroupGeneration()
	d.compositePodGroup = nil
}

// addChild 添加一个子组。
func (d *compositePodGroupStateData) addChild(child fwk.EntityKey) {
	d.generation = nextPodGroupGeneration()
	d.children.Insert(child)
}

// removeChild 移除一个子组。
func (d *compositePodGroupStateData) removeChild(child fwk.EntityKey) {
	d.generation = nextPodGroupGeneration()
	d.children.Delete(child)
}

// getChildren 返回所有子组的键。
func (d *compositePodGroupStateData) getChildren() []fwk.EntityKey {
	var children []fwk.EntityKey
	for child := range d.children {
		children = append(children, child)
	}
	return children
}

// podGroupState 保存 PodGroup 的运行时状态。
type podGroupState struct {
	lock sync.RWMutex
	podGroupStateData
}

func newPodGroupState() *podGroupState {
	return &podGroupState{podGroupStateData: newPodGroupStateData()}
}

// snapshot 返回当前组状态的深拷贝快照，内容不可变。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) snapshot() *podGroupStateSnapshot {
	return &podGroupStateSnapshot{podGroupStateData: pgs.podGroupStateData.clone()}
}

// empty 在组内无 Pod 且缓存的 PodGroup 对象为 nil 时返回 true。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) empty() bool {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.empty()
}

// addPod 把 Pod 加入该组，依据 NodeName 放入 assignedPods 或 unscheduledPods。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) addPod(pod *v1.Pod) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.addPod(pod)
}

// updatePod 更新组内的 Pod，绑定时将其移入 assignedPods。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) updatePod(oldPod, newPod *v1.Pod) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.updatePod(oldPod, newPod)
}

// deletePod 从组状态中移除 Pod。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) deletePod(podUID types.UID) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.deletePod(podUID)
}

// assumePod 将 Pod 标记为组内已 assume。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) assumePod(pod *v1.Pod) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.assumePod(pod)
}

// forgetPod 将 Pod 从 assume 状态退回组内未调度状态。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) forgetPod(podUID types.UID) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.forgetPod(podUID)
}

// setPodGroup 设置 PodGroup 对象。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) setPodGroup(podGroup *schedulingv1beta1.PodGroup) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.setPodGroup(podGroup)
	pgs.podGroupStateData.generation = nextPodGroupGeneration()
}

// removePodGroup 移除 PodGroup 对象。
// 必须在缓存锁的保护下调用。
func (pgs *podGroupState) removePodGroup() {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.podGroupStateData.removePodGroup()
	pgs.podGroupStateData.generation = nextPodGroupGeneration()
}

// AllPods 返回调度器已知的该组所有 Pod 的 UID。
func (pgs *podGroupState) AllPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return sets.KeySet(pgs.podGroupStateData.allPods)
}

// AllPodsCount 返回调度器已知的该组 Pod 总数。
func (pgs *podGroupState) AllPodsCount() int {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.allPodsCount()
}

// UnscheduledPods 返回该组所有未调度的 Pod，即既未 assume 也未分配。
// 返回的 map 类型与 PodActivator.Activate 方法的参数一致。
func (pgs *podGroupState) UnscheduledPods() map[string]*v1.Pod {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.unscheduledPodsMap()
}

// AssumedPods 返回该组中处于 assume 状态（已通过 Reserve 阶段）的 Pod 的 UID。
func (pgs *podGroupState) AssumedPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return sets.KeySet(pgs.podGroupStateData.assumedPods)
}

// AssignedPods 返回该组中已分配（绑定）的 Pod 的 UID。
func (pgs *podGroupState) AssignedPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.assignedPods.Clone()
}

// ScheduledPods 返回该组中已 assume 或已分配的 Pod。
func (pgs *podGroupState) ScheduledPods() []*v1.Pod {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.scheduledPods()
}

// ScheduledPodsCount 返回该组中已 assume 或已分配的 Pod 数量。
func (pgs *podGroupState) ScheduledPodsCount() int {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.scheduledPodsCount()
}

// PodGroup 返回缓存的 PodGroup API 对象。
func (pgs *podGroupState) PodGroup() *schedulingv1beta1.PodGroup {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.podGroup
}

// compositePodGroupState 保存 CompositePodGroup 的运行时状态。
type compositePodGroupState struct {
	lock sync.RWMutex
	compositePodGroupStateData
}

// newCompositePodGroupState 创建新的 compositePodGroupState。
func newCompositePodGroupState() *compositePodGroupState {
	return &compositePodGroupState{compositePodGroupStateData: newCompositePodGroupStateData()}
}

// snapshot 返回当前组合组状态的深拷贝快照，内容不可变。
// 必须在缓存锁的保护下调用。
func (cpgs *compositePodGroupState) snapshot() *compositePodGroupStateSnapshot {
	return &compositePodGroupStateSnapshot{compositePodGroupStateData: cpgs.compositePodGroupStateData.clone()}
}

// empty 在组合组状态中既无 CompositePodGroup 也无子对象时返回 true。
// 必须在缓存锁的保护下调用。
func (cpgs *compositePodGroupState) empty() bool {
	cpgs.lock.RLock()
	defer cpgs.lock.RUnlock()

	return cpgs.compositePodGroupStateData.empty()
}

// setCompositePodGroup 设置 CompositePodGroup 对象。
// 必须在缓存锁的保护下调用。
func (cpgs *compositePodGroupState) setCompositePodGroup(compositePodGroup *schedulingv1alpha3.CompositePodGroup) {
	cpgs.lock.Lock()
	defer cpgs.lock.Unlock()

	cpgs.compositePodGroupStateData.setCompositePodGroup(compositePodGroup)
}

// removeCompositePodGroup 移除 CompositePodGroup 对象。
// 必须在缓存锁的保护下调用。
func (cpgs *compositePodGroupState) removeCompositePodGroup() {
	cpgs.lock.Lock()
	defer cpgs.lock.Unlock()

	cpgs.compositePodGroupStateData.removeCompositePodGroup()
}

// CompositePodGroup 返回缓存的 CompositePodGroup API 对象。
func (cpgs *compositePodGroupState) CompositePodGroup() *schedulingv1alpha3.CompositePodGroup {
	cpgs.lock.RLock()
	defer cpgs.lock.RUnlock()

	return cpgs.compositePodGroupStateData.compositePodGroup
}

// GetChildren 返回子 PodGroup 或子 CompositePodGroup 的键。
func (cpgs *compositePodGroupState) GetChildren() []fwk.EntityKey {
	cpgs.lock.RLock()
	defer cpgs.lock.RUnlock()

	return cpgs.compositePodGroupStateData.getChildren()
}

// podGroupStateSnapshot 是 podGroupState 在某一时刻的不可变副本。
// 在 PodGroup 调度周期开始前生成，用于在周期内跟踪 Pod 状态而不改动实时状态。
type podGroupStateSnapshot struct {
	podGroupStateData
}

// assumePod 在快照中把 Pod 标记为已 assume。
func (s *podGroupStateSnapshot) assumePod(pod *v1.Pod) {
	s.podGroupStateData.assumePod(pod)
}

// forgetPod 在快照中把 Pod 从 assume 状态移除。
func (s *podGroupStateSnapshot) forgetPod(podUID types.UID) {
	s.podGroupStateData.forgetPod(podUID)
}

// AllPods 返回调度器已知的该组所有 Pod 的 UID。
func (s *podGroupStateSnapshot) AllPods() sets.Set[types.UID] {
	return sets.KeySet(s.podGroupStateData.allPods)
}

// UnscheduledPods 返回该组所有未调度的 Pod。
func (s *podGroupStateSnapshot) UnscheduledPods() map[string]*v1.Pod {
	return s.podGroupStateData.unscheduledPodsMap()
}

// AssumedPods 返回该组中所有已 assume 的 Pod 的 UID。
func (s *podGroupStateSnapshot) AssumedPods() sets.Set[types.UID] {
	return sets.KeySet(s.podGroupStateData.assumedPods)
}

// AssignedPods 返回该组中已分配（绑定）的 Pod 的 UID。
func (s *podGroupStateSnapshot) AssignedPods() sets.Set[types.UID] {
	return s.podGroupStateData.assignedPods
}

// ScheduledPods 返回该组中已 assume 或已分配的 Pod。
func (s *podGroupStateSnapshot) ScheduledPods() []*v1.Pod {
	return s.podGroupStateData.scheduledPods()
}

// AllPodsCount 返回调度器已知的该组 Pod 总数。
func (s *podGroupStateSnapshot) AllPodsCount() int {
	return s.podGroupStateData.allPodsCount()
}

// ScheduledPodsCount 返回该组中已 assume 或已分配的 Pod 数量。
func (s *podGroupStateSnapshot) ScheduledPodsCount() int {
	return s.podGroupStateData.scheduledPodsCount()
}

// Clone 返回一个克隆了 podGroupStateData 的组状态快照。
func (s *podGroupStateSnapshot) Clone() *podGroupStateSnapshot {
	return &podGroupStateSnapshot{podGroupStateData: s.podGroupStateData.clone()}
}

// compositePodGroupStateSnapshot 是 compositePodGroupState 在某一时刻的不可变副本。
// 在 PodGroup 调度周期开始前生成，用于跟踪组合组的状态。
type compositePodGroupStateSnapshot struct {
	compositePodGroupStateData
}

// GetChildren 返回子组的键。
func (s *compositePodGroupStateSnapshot) GetChildren() []fwk.EntityKey {
	return s.compositePodGroupStateData.getChildren()
}
