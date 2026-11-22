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

package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

var (
	updateMetricsPeriod = 1 * time.Second
)

// New 返回一个 Cache 实现，并自动启动导出缓存指标的 goroutine。
// ctx 用于关闭该后台 goroutine。
func New(ctx context.Context, apiDispatcher fwk.APIDispatcher, genericWorkloadEnabled, compositePodGroupEnabled bool) Cache {
	cache := newCache(ctx, updateMetricsPeriod, apiDispatcher, genericWorkloadEnabled, compositePodGroupEnabled)
	cache.run()
	return cache
}

// nodeInfoListItem 持有 NodeInfo 指针，作为双向链表节点。
// NodeInfo 更新时会被移到链表头部，越靠近头部越新；
// UpdateSnapshot 借此只扫描上次快照后有变化的节点。
type nodeInfoListItem struct {
	info *framework.NodeInfo
	next *nodeInfoListItem
	prev *nodeInfoListItem
}

// cacheImpl 是 scheduler cache 的具体实现，维护 Pod/Node/镜像/PodGroup 等状态。
type cacheImpl struct {
	stop   <-chan struct{}
	period time.Duration

	// mu 保护该结构体的所有字段。
	mu sync.RWMutex
	// assumedPods 保存已假定调度的 Pod 键，可用该键在 podStates 中查到对应条目。
	assumedPods sets.Set[string]
	// podStates 保存 Pod 键到 Pod 状态的映射。
	podStates map[string]*podState
	nodes     map[string]*nodeInfoListItem
	// headNode 指向 nodes 中最近更新的 NodeInfo，即链表头部。
	headNode *nodeInfoListItem
	nodeTree *nodeTree
	// imageStates 保存镜像名到镜像状态摘要的映射。
	imageStates map[string]*fwk.ImageStateSummary
	// podGroupStates 保存各 PodGroup 的运行时状态（仅当 GenericWorkload 特性门开启时有效）。
	podGroupStates map[fwk.EntityKey]*podGroupState
	// compositePodGroupStates 保存各组合 PodGroup 的运行时状态（仅当 CompositePodGroup 特性门开启时有效）。
	compositePodGroupStates map[fwk.EntityKey]*compositePodGroupState
	// genericWorkloadEnabled 记录 GenericWorkload 特性门的取值。
	genericWorkloadEnabled bool
	// compositePodGroupEnabled 记录 CompositePodGroup 特性门的取值。
	compositePodGroupEnabled bool
	// apiDispatcher 用于需要发送 API 调用的方法，仅当 SchedulerAsyncAPICalls 特性门开启时非 nil。
	apiDispatcher fwk.APIDispatcher

	// pvcRefCountsDelta 保存自上次快照以来 PVCRefCounts 的变化量，键格式为 "namespace/name"。
	// 事件处理器持续写入增量，调度周期内只应用并清空这些增量，
	// 避免每次快照都重新聚合全部 PVC 引用计数。
	pvcRefCountsDelta map[string]int
}

// podState 保存缓存中单个 Pod 的状态。
type podState struct {
	pod *v1.Pod
}

// newCache 创建缓存实例但不启动后台 goroutine，便于测试控制。
func newCache(ctx context.Context, period time.Duration, apiDispatcher fwk.APIDispatcher, genericWorkloadEnabled, compositePodGroupEnabled bool) *cacheImpl {
	logger := klog.FromContext(ctx)
	return &cacheImpl{
		period: period,
		stop:   ctx.Done(),

		nodes:                    make(map[string]*nodeInfoListItem),
		nodeTree:                 newNodeTree(logger, nil),
		assumedPods:              sets.New[string](),
		podStates:                make(map[string]*podState),
		imageStates:              make(map[string]*fwk.ImageStateSummary),
		podGroupStates:           make(map[fwk.EntityKey]*podGroupState),
		compositePodGroupStates:  make(map[fwk.EntityKey]*compositePodGroupState),
		genericWorkloadEnabled:   genericWorkloadEnabled,
		compositePodGroupEnabled: compositePodGroupEnabled,
		apiDispatcher:            apiDispatcher,
		pvcRefCountsDelta:        make(map[string]int),
	}
}

// newNodeInfoListItem 创建一个 nodeInfoListItem。
func newNodeInfoListItem(ni *framework.NodeInfo) *nodeInfoListItem {
	return &nodeInfoListItem{
		info: ni,
	}
}

// moveNodeInfoToHead 把 NodeInfo 移到 cache.nodes 链表头部，表示它是最近更新的节点。
// 调用方需持有缓存锁。
func (cache *cacheImpl) moveNodeInfoToHead(logger klog.Logger, name string) {
	ni, ok := cache.nodes[name]
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "No node info with given name found in the cache", "node", klog.KRef("", name))
		return
	}
	// 已经在头部则无需调整指针。
	if ni == cache.headNode {
		return
	}

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	if cache.headNode != nil {
		cache.headNode.prev = ni
	}
	ni.next = cache.headNode
	ni.prev = nil
	cache.headNode = ni
}

// removeNodeInfoFromList 从 cache.nodes 双向链表中移除 NodeInfo。
// 调用方需持有缓存锁。
func (cache *cacheImpl) removeNodeInfoFromList(logger klog.Logger, name string) {
	ni, ok := cache.nodes[name]
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "No node info with given name found in the cache", "node", klog.KRef("", name))
		return
	}

	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}
	// 删除的是头节点时，需要把头指针后移。
	if ni == cache.headNode {
		cache.headNode = ni.next
	}
	delete(cache.nodes, name)
}

// Dump 导出当前调度缓存的完整快照，仅用于调试，与 UpdateSnapshot 无关。
// 该方法开销较大，不应在关键路径上调用。
func (cache *cacheImpl) Dump() *Dump {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	nodes := make(map[string]*framework.NodeInfo, len(cache.nodes))
	for k, v := range cache.nodes {
		nodes[k] = v.info.SnapshotConcrete()
	}

	return &Dump{
		Nodes:       nodes,
		AssumedPods: cache.assumedPods.Union(nil),
	}
}

// UpdateSnapshot 在每次调度周期开始时对缓存的 NodeInfo 做快照。
// 快照只包含调用时尚未删除的节点，且每个节点的 nodeInfo.Node() 保证非 nil。
// 它按 NodeInfo 的 generation 增量更新现有快照中发生变化的条目。
func (cache *cacheImpl) UpdateSnapshot(logger klog.Logger, nodeSnapshot *Snapshot) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if nodeSnapshot.placementNodes != nil {
		logger.Error(nil, "UpdateSnapshot called with assumed placement. This is unexpected. Placement will be cleared.")
		nodeSnapshot.ForgetPlacement()
	}

	// 取快照记录的上次 generation，作为增量更新的起点。
	snapshotGeneration := nodeSnapshot.generation

	// 节点增删会使所有列表失效，需要整体重建。
	updateAllLists := false
	// 节点在"有/无亲和性 Pod"之间变化时，亲和性列表需要重建。
	updateNodesHavePodsWithAffinity := false
	// 节点在"有/无强制反亲和性 Pod"之间变化时，对应列表需要重建。
	updateNodesHavePodsWithRequiredAntiAffinity := false

	// 清除上一版快照中的假定 Pod，防止上个周期遗漏 Forget 造成状态残留。
	nodeSnapshot.forgetAllAssumedPods(logger)

	// 节点在"有/无非主机级强制反亲和性 Pod"之间变化时（topologyKey 非 kubernetes.io/hostname），列表需要重建。
	updateNodesHavePodsWithRequiredNonHostScopedAntiAffinity := false

	// 从链表头部开始，只更新上次快照之后变更过的 NodeInfo。
	for node := cache.headNode; node != nil; node = node.next {
		if node.info.Generation <= snapshotGeneration {
			// generation 已不新于快照，后面的节点更旧，可以停止扫描。
			break
		}
		if np := node.info.Node(); np != nil {
			existing, ok := nodeSnapshot.nodeInfoMap[np.Name]
			if !ok {
				updateAllLists = true
				existing = &framework.NodeInfo{}
				nodeSnapshot.nodeInfoMap[np.Name] = existing
			}
			clone := node.info.SnapshotConcrete()
			// 比较新旧状态，判断节点是否在"有无亲和性 Pod"上发生了变化。
			if (len(existing.PodsWithAffinity) > 0) != (len(clone.PodsWithAffinity) > 0) {
				updateNodesHavePodsWithAffinity = true
			}
			if (len(existing.PodsWithRequiredAntiAffinity) > 0) != (len(clone.PodsWithRequiredAntiAffinity) > 0) {
				updateNodesHavePodsWithRequiredAntiAffinity = true
			}
			if (len(existing.PodsWithRequiredNonHostScopedAntiAffinity) > 0) != (len(clone.PodsWithRequiredNonHostScopedAntiAffinity) > 0) {
				updateNodesHavePodsWithRequiredNonHostScopedAntiAffinity = true
			}
			// 原地赋值以保留原指针：NodeInfoList 等列表可能仍引用它。
			*existing = *clone
		}
	}
	// 把快照 generation 推进到缓存中最新的位置。
	if cache.headNode != nil {
		nodeSnapshot.generation = cache.headNode.info.Generation
	}

	// 已删除节点会从 nodeTree 移除，但若其上还有未删除的 Pod，仍可能留在 nodes map 中。
	// 因此需要用 nodeTree 的节点数校准快照，清掉幽灵节点。
	if len(nodeSnapshot.nodeInfoMap) > cache.nodeTree.numNodes {
		cache.removeDeletedNodesFromSnapshot(nodeSnapshot)
		updateAllLists = true
	}

	// 把 PVC 引用计数增量应用到快照；若快照将整体重建（updateAllLists=true）则无需应用。
	if !updateAllLists {
		if err := cache.applyPVCRefCountDelta(nodeSnapshot); err != nil {
			logger.Error(err, "rebuilding node snapshot due to unexpected error from refreshing PVC ref counts")
			updateAllLists = true
		}
	}

	if updateAllLists || updateNodesHavePodsWithAffinity || updateNodesHavePodsWithRequiredAntiAffinity || updateNodesHavePodsWithRequiredNonHostScopedAntiAffinity {
		cache.updateNodeInfoSnapshotList(logger, nodeSnapshot, updateAllLists)
	}

	if len(nodeSnapshot.nodeInfoList) != cache.nodeTree.numNodes {
		errMsg := fmt.Sprintf("snapshot state is not consistent, length of NodeInfoList=%v not equal to length of nodes in tree=%v "+
			", length of NodeInfoMap=%v, length of nodes in cache=%v"+
			", trying to recover",
			len(nodeSnapshot.nodeInfoList), cache.nodeTree.numNodes,
			len(nodeSnapshot.nodeInfoMap), len(cache.nodes))
		logger.Error(nil, errMsg)
		// 重建列表供下个周期使用以自愈，但仍返回错误暴露异常；这通常会导致当前周期失败。
		cache.updateNodeInfoSnapshotList(logger, nodeSnapshot, true)
		return errors.New(errMsg)
	}

	if cache.genericWorkloadEnabled {
		// 为本调度周期快照 PodGroup 状态。
		cache.updatePodGroupStateSnapshot(nodeSnapshot)
		if cache.compositePodGroupEnabled {
			// 为本调度周期快照组合 PodGroup 状态。
			cache.updateCompositePodGroupStateSnapshot(nodeSnapshot)
		}
	}

	return nil
}

// updatePodGroupStateSnapshot 更新快照中的 PodGroup 状态部分：
// 删除缓存中已不存在的条目，克隆 generation 已推进的条目。调用方需持有缓存锁。
func (cache *cacheImpl) updatePodGroupStateSnapshot(snapshot *Snapshot) {
	// 删除缓存中已不存在的 PodGroup 状态。
	for key := range snapshot.podGroupStates {
		if _, exists := cache.podGroupStates[key]; !exists {
			delete(snapshot.podGroupStates, key)
		}
	}
	// 只克隆自上次快照以来发生变化的 PodGroup 状态，避免拷贝整个状态集。
	for key, podGroupState := range cache.podGroupStates {
		if existing, ok := snapshot.podGroupStates[key]; ok && existing.generation == podGroupState.generation {
			continue
		}
		snapshot.podGroupStates[key] = podGroupState.snapshot()
	}
}

// updateCompositePodGroupStateSnapshot 更新快照中的组合 PodGroup 状态部分：
// 删除缓存中已不存在的条目，克隆 generation 已推进的条目。调用方需持有缓存锁。
func (cache *cacheImpl) updateCompositePodGroupStateSnapshot(snapshot *Snapshot) {
	// 删除缓存中已不存在的组合 PodGroup 状态。
	for key := range snapshot.compositePodGroupStates {
		if _, exists := cache.compositePodGroupStates[key]; !exists {
			delete(snapshot.compositePodGroupStates, key)
		}
	}
	// 只克隆自上次快照以来发生变化的组合 PodGroup 状态。
	for key, cpgState := range cache.compositePodGroupStates {
		if existing, ok := snapshot.compositePodGroupStates[key]; ok && existing.generation == cpgState.generation {
			continue
		}
		snapshot.compositePodGroupStates[key] = cpgState.snapshot()
	}
}

// updateNodeInfoSnapshotList 重建或增量更新快照中的各类节点列表（含 PVC 引用计数汇总）。
func (cache *cacheImpl) updateNodeInfoSnapshotList(logger klog.Logger, snapshot *Snapshot, updateAll bool) {
	snapshot.havePodsWithAffinityNodeInfoList = make([]fwk.NodeInfo, 0, cache.nodeTree.numNodes)
	snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = make([]fwk.NodeInfo, 0, cache.nodeTree.numNodes)
	snapshot.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = make([]fwk.NodeInfo, 0, cache.nodeTree.numNodes)
	if updateAll {
		snapshot.usedPVCRefCounts = make(map[string]int)
		// 按 nodeTree 中的节点顺序重建列表，保证调度时的节点遍历顺序稳定。
		snapshot.nodeInfoList = make([]fwk.NodeInfo, 0, cache.nodeTree.numNodes)
		nodesList, err := cache.nodeTree.list()
		if err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error occurred while retrieving the list of names of the nodes from node tree")
		}
		for _, nodeName := range nodesList {
			if nodeInfo := snapshot.nodeInfoMap[nodeName]; nodeInfo != nil {
				snapshot.nodeInfoList = append(snapshot.nodeInfoList, nodeInfo)
				if len(nodeInfo.PodsWithAffinity) > 0 {
					snapshot.havePodsWithAffinityNodeInfoList = append(snapshot.havePodsWithAffinityNodeInfoList, nodeInfo)
				}
				if len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 {
					snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
				}
				if len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
					snapshot.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo)
				}
				for key, value := range nodeInfo.PVCRefCounts {
					snapshot.usedPVCRefCounts[key] += value
				}
			} else {
				utilruntime.HandleErrorWithLogger(logger, nil, "Node exists in nodeTree but not in NodeInfoMap, this should not happen", "node", klog.KRef("", nodeName))
			}
		}
		// 全量重建后 PVC 引用计数已是最新，清空增量避免重复应用。
		cache.pvcRefCountsDelta = map[string]int{}
	} else {
		for _, nodeInfo := range snapshot.nodeInfoList {
			if len(nodeInfo.GetPodsWithAffinity()) > 0 {
				snapshot.havePodsWithAffinityNodeInfoList = append(snapshot.havePodsWithAffinityNodeInfoList, nodeInfo)
			}
			if len(nodeInfo.GetPodsWithRequiredAntiAffinity()) > 0 {
				snapshot.havePodsWithRequiredAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
			}
			if len(nodeInfo.GetPodsWithRequiredNonHostScopedAntiAffinity()) > 0 {
				snapshot.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(snapshot.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo)
			}
		}
	}
}

// removeDeletedNodesFromSnapshot 把自上次快照后已被删除的节点从快照中清除。
func (cache *cacheImpl) removeDeletedNodesFromSnapshot(snapshot *Snapshot) {
	toDelete := len(snapshot.nodeInfoMap) - cache.nodeTree.numNodes
	for name := range snapshot.nodeInfoMap {
		if toDelete <= 0 {
			break
		}
		if n, ok := cache.nodes[name]; !ok || n.info.Node() == nil {
			delete(snapshot.nodeInfoMap, name)
			toDelete--
		}
	}
}

// NodeCount 返回缓存中的节点数。仅限测试使用，不应在生产代码中调用。
func (cache *cacheImpl) NodeCount() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.nodes)
}

// PodCount 返回缓存中的 Pod 数（含已删除节点上的 Pod）。仅限测试使用，不应在生产代码中调用。
func (cache *cacheImpl) PodCount() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	// 直接累加各节点的 Pod 数，无需构造切片，避免无谓的内存分配。
	count := 0
	for _, n := range cache.nodes {
		count += len(n.info.Pods)
	}
	return count, nil
}

// AssumePod 把一个待调度 Pod 假定分配到其目标节点并加入缓存。
func (cache *cacheImpl) AssumePod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v(%v) is in the cache, so can't be assumed", key, klog.KObj(pod))
	}

	return cache.addPod(logger, pod, true)
}

// validateAssumedPod 校验 Pod 当前确实处于假定状态且节点未变。调用方需持有缓存锁。
func (cache *cacheImpl) validateAssumedPod(pod *v1.Pod, key string, currState *podState) error {
	if currState.pod.Spec.NodeName != pod.Spec.NodeName {
		return fmt.Errorf("pod %v(%v) was assumed on %v but assigned to %v", key, klog.KObj(pod), pod.Spec.NodeName, currState.pod.Spec.NodeName)
	}
	if !cache.assumedPods.Has(key) {
		return fmt.Errorf("pod %v(%v) is not assumed, so it cannot be removed or forgotten", key, klog.KObj(pod))
	}
	return nil
}

// ForgetPod 从缓存中撤销一个假定 Pod，应在 Pod 仍存在时作为 AssumePod 的逆操作调用。
// 若该 Pod 是 PodGroup 成员，会将其从 PodGroup 状态的 assumed 集合移回未调度集合。
func (cache *cacheImpl) ForgetPod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		// Pod 已不在缓存中，无需处理。
		return nil
	}
	if err := cache.validateAssumedPod(pod, key, currState); err != nil {
		return err
	}
	return cache.removePod(logger, pod, true)
}

// RemoveAssumedPod 在假定 Pod 确已从集群移除时清理缓存中的内部状态。
// 与 ForgetPod 的区别在于：它会把 Pod 从 PodGroup 状态中彻底删除，而不是移回未调度集合。
func (cache *cacheImpl) RemoveAssumedPod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		// Pod 已不在缓存中，无需处理。
		return nil
	}
	if err := cache.validateAssumedPod(pod, key, currState); err != nil {
		return err
	}
	return cache.removePod(logger, pod, false)
}

// addPod 把 Pod 加入缓存；assumePod 为 true 时标记为假定状态。调用方需持有缓存锁。
func (cache *cacheImpl) addPod(logger klog.Logger, pod *v1.Pod, assumePod bool) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}
	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		n = newNodeInfoListItem(framework.NewNodeInfo())
		cache.nodes[pod.Spec.NodeName] = n
	}

	// 先减旧、后加新，使增量只反映本次变更的净效果。
	// 先减旧、后加新，使增量只反映本次变更的净效果。
	// new_delta = old_delta + (PVCRefCounts_after − PVCRefCounts_before)
	cache.refreshPVCRefCountsDelta(n.info, -1)
	defer cache.refreshPVCRefCountsDelta(n.info, 1)

	n.info.AddPod(pod)
	cache.moveNodeInfoToHead(logger, pod.Spec.NodeName)
	ps := &podState{
		pod: pod,
	}
	cache.podStates[key] = ps
	if assumePod {
		cache.assumedPods.Insert(key)
	}

	if !cache.isPodGroupMember(pod) {
		return nil
	}
	if assumePod {
		cache.assumePodGroupMember(pod)
	} else {
		cache.addPodGroupMember(pod)
	}
	return nil
}

// updatePod 用新 Pod 替换旧 Pod。调用方需持有缓存锁。
func (cache *cacheImpl) updatePod(logger klog.Logger, oldPod, newPod *v1.Pod) error {
	if err := cache.removePod(logger, oldPod, false); err != nil {
		return err
	}
	return cache.addPod(logger, newPod, false)
}

// removePod 从缓存的节点信息中移除 Pod；若节点已无 Node 对象且不再有 Pod，则清理缓存中的节点条目。
// 调用方需持有缓存锁。
func (cache *cacheImpl) removePod(logger klog.Logger, pod *v1.Pod, forgetPod bool) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	n, ok := cache.nodes[pod.Spec.NodeName]
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Node not found when trying to remove pod", "node", klog.KRef("", pod.Spec.NodeName), "podKey", key, "pod", klog.KObj(pod))
	} else {
		// new_delta = old_delta + (PVCRefCounts_after − PVCRefCounts_before)
		cache.refreshPVCRefCountsDelta(n.info, -1)
		defer cache.refreshPVCRefCountsDelta(n.info, 1)

		if err := n.info.RemovePod(logger, pod); err != nil {
			return err
		}
		if len(n.info.Pods) == 0 && n.info.Node() == nil {
			cache.removeNodeInfoFromList(logger, pod.Spec.NodeName)
		} else {
			cache.moveNodeInfoToHead(logger, pod.Spec.NodeName)
		}
	}

	delete(cache.podStates, key)
	delete(cache.assumedPods, key)

	if !cache.isPodGroupMember(pod) {
		return nil
	}
	if forgetPod {
		cache.forgetPodGroupMember(logger, pod)
	} else {
		cache.removePodGroupMember(pod)
	}

	return nil
}

// AddPod 处理 Pod 的 Add 事件：若已假定则该为 Added 并更新，否则直接加入缓存。
func (cache *cacheImpl) AddPod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	switch {
	case ok && cache.assumedPods.Has(key):
		// 假定时 Pod 已在缓存中，这里只需用 Add 事件的数据更新，保证状态最新。
		if err = cache.updatePod(logger, currState.pod, pod); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error occurred while updating pod")
		}
		if currState.pod.Spec.NodeName != pod.Spec.NodeName {
			// 实际绑定的节点与假定节点不同，说明调度器可能已放弃该 Pod，忽略更新。
			logger.Info("Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Spec.NodeName), "currentNode", klog.KRef("", currState.pod.Spec.NodeName))
			return nil
		}
	case !ok:
		if err = cache.addPod(logger, pod, false); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error occurred while adding pod")
		}
	default:
		return fmt.Errorf("pod %v(%v) was already in added state", key, klog.KObj(pod))
	}
	return nil
}

// UpdatePod 处理 Pod 的 Update 事件，替换缓存中的旧 Pod 对象。
func (cache *cacheImpl) UpdatePod(logger klog.Logger, oldPod, newPod *v1.Pod) error {
	key, err := framework.GetPodKey(oldPod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not added to scheduler cache, so cannot be updated", key, klog.KObj(oldPod))
	}

	// 假定状态的 Pod 不应收到 Update 事件；它必须先收到 Add 事件完成从 Assumed 到 Added 的转换。
	if cache.assumedPods.Has(key) {
		return fmt.Errorf("assumed pod %v(%v) should not be updated", key, klog.KObj(oldPod))
	}

	if currState.pod.Spec.NodeName != newPod.Spec.NodeName {
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod updated on a different node than previously added to. Scheduler cache is corrupted and can badly affect scheduling decisions", "podKey", key, "pod", klog.KObj(oldPod))
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	return cache.updatePod(logger, oldPod, newPod)
}

// RemovePod 处理 Pod 的 Delete 事件，从缓存中移除对应 Pod。
func (cache *cacheImpl) RemovePod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not found in scheduler cache, so cannot be removed from it", key, klog.KObj(pod))
	}
	if currState.pod.Spec.NodeName != pod.Spec.NodeName {
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Spec.NodeName), "currentNode", klog.KRef("", currState.pod.Spec.NodeName))
		if pod.Spec.NodeName != "" {
			// 若调度器漏掉了 Delete 事件，从 informer 缓存拿到的可能是 NodeName 为空的旧状态，属正常情况。
			utilruntime.HandleErrorWithLogger(logger, nil, "Scheduler cache is corrupted and can badly affect scheduling decisions")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}
	return cache.removePod(logger, currState.pod, false)
}

// IsAssumedPod 返回 Pod 是否处于假定调度状态。
func (cache *cacheImpl) IsAssumedPod(pod *v1.Pod) (bool, error) {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return false, err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.assumedPods.Has(key), nil
}

// GetPod 返回缓存中的 Pod；其所在节点可能已从主缓存删除，
// 这对正确处理 Pod 更新事件很有用。
func (cache *cacheImpl) GetPod(pod *v1.Pod) (*v1.Pod, error) {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return nil, err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	podState, ok := cache.podStates[key]
	if !ok {
		return nil, fmt.Errorf("pod %v(%v) does not exist in scheduler cache", key, klog.KObj(pod))
	}

	return podState.pod, nil
}

// AddNode 处理 Node 的 Add 事件，更新节点树、节点信息与镜像状态。
func (cache *cacheImpl) AddNode(logger klog.Logger, node *v1.Node) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[node.Name]
	if !ok {
		n = newNodeInfoListItem(framework.NewNodeInfo())
		cache.nodes[node.Name] = n
	} else {
		cache.removeNodeImageStates(n.info.Node())
	}
	cache.moveNodeInfoToHead(logger, node.Name)

	cache.nodeTree.addNode(logger, node)
	cache.addNodeImageStates(node, n.info)
	n.info.SetNode(node)
}

// UpdateNode 处理 Node 的 Update 事件，用新节点信息替换缓存中的旧数据。
func (cache *cacheImpl) UpdateNode(logger klog.Logger, oldNode, newNode *v1.Node) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	n, ok := cache.nodes[newNode.Name]
	if !ok {
		n = newNodeInfoListItem(framework.NewNodeInfo())
		cache.nodes[newNode.Name] = n
		cache.nodeTree.addNode(logger, newNode)
	} else {
		cache.removeNodeImageStates(n.info.Node())
	}
	cache.moveNodeInfoToHead(logger, newNode.Name)

	cache.nodeTree.updateNode(logger, oldNode, newNode)
	cache.addNodeImageStates(newNode, n.info)
	n.info.SetNode(newNode)
}

// RemoveNode 从缓存的节点树中移除节点。
// 节点上可能还有 Pod，其删除事件尚未到达；节点树是事实来源，这些 Pod 视为已移除。
// 但缓存仍保留一个含 Pod 列表的幽灵节点，直到所有 Pod 删除事件到齐；
// 幽灵节点不会进入快照。
func (cache *cacheImpl) RemoveNode(logger klog.Logger, node *v1.Node) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	n, ok := cache.nodes[node.Name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.Name)
	}

	// 仅扣减增量：节点删除后可由 RemovePod 继续扣减其上的 Pod 引用，无需再次加回。
	cache.refreshPVCRefCountsDelta(n.info, -1)

	n.info.RemoveNode()
	// 只有节点上已无 Pod 时才删除 NodeInfo。Pod 与节点来自不同的 watch，
	// 即使 Pod 删除发生在节点删除之前，事件也可能延后到达，不能无条件清理。
	if len(n.info.Pods) == 0 {
		cache.removeNodeInfoFromList(logger, node.Name)
	} else {
		cache.moveNodeInfoToHead(logger, node.Name)
	}
	if err := cache.nodeTree.removeNode(logger, node); err != nil {
		return err
	}
	cache.removeNodeImageStates(node)
	return nil
}

// GetNode 返回缓存中节点的副本。仅限测试使用，不应在生产代码中调用。
func (cache *cacheImpl) GetNode(name string) (*framework.NodeInfo, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	n, ok := cache.nodes[name]
	if !ok {
		return nil, fmt.Errorf("node %v does not exist in scheduler cache", name)
	}
	return n.info.SnapshotConcrete(), nil
}

// addNodeImageStates 把节点上的镜像状态写入 nodeInfo，并同步更新缓存的 imageStates。
// 调用方需持有缓存锁。
func (cache *cacheImpl) addNodeImageStates(node *v1.Node, nodeInfo *framework.NodeInfo) {
	newSum := make(map[string]*fwk.ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			// 更新全局镜像状态：记录该镜像在哪些节点上可用。
			state, ok := cache.imageStates[name]
			if !ok {
				state = &fwk.ImageStateSummary{
					Size:  image.SizeBytes,
					Nodes: sets.New(node.Name),
				}
				cache.imageStates[name] = state
			} else {
				state.Nodes.Insert(node.Name)
			}
			// 每个镜像名只保留一个摘要对象，避免同一镜像的多个名称重复创建。
			if _, ok := newSum[name]; !ok {
				newSum[name] = state
			}
		}
	}
	nodeInfo.ImageStates = newSum
}

// removeNodeImageStates 从 imageStates 中移除该节点对各镜像的记录。
// 若某镜像不再出现在任何节点上，则连同镜像条目一起删除。
func (cache *cacheImpl) removeNodeImageStates(node *v1.Node) {
	if node == nil {
		return
	}

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			state, ok := cache.imageStates[name]
			if ok {
				state.Nodes.Delete(node.Name)
				if state.Nodes.Len() == 0 {
					// 清理未被使用的镜像，保证 imageStates 的大小等于各节点上不同镜像的总数。
					delete(cache.imageStates, name)
				}
			}
		}
	}
}

// run 启动定期更新缓存指标的后台 goroutine。
func (cache *cacheImpl) run() {
	go wait.Until(cache.updateMetrics, cache.period, cache.stop)
}

// updateMetrics 更新 Pod、假定 Pod 及节点数量的缓存大小指标。
func (cache *cacheImpl) updateMetrics() {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	metrics.CacheSize.WithLabelValues("assumed_pods").Set(float64(len(cache.assumedPods)))
	metrics.CacheSize.WithLabelValues("pods").Set(float64(len(cache.podStates)))
	metrics.CacheSize.WithLabelValues("nodes").Set(float64(len(cache.nodes)))
}

// isPodGroupMember 判断 Pod 是否属于某个 PodGroup；仅在 GenericWorkload 特性门开启时为真。
func (cache *cacheImpl) isPodGroupMember(pod *v1.Pod) bool {
	return cache.genericWorkloadEnabled && pod.Spec.SchedulingGroup != nil
}

// AddPodGroupMember 把一个未分配、未假定的 Pod 加入缓存中对应的 PodGroup 状态。
func (cache *cacheImpl) AddPodGroupMember(pod *v1.Pod) {
	if !cache.isPodGroupMember(pod) {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.addPodGroupMember(pod)
}

// UpdatePodGroupMember 更新缓存中 Pod 在所属 PodGroup 状态里的条目。
func (cache *cacheImpl) UpdatePodGroupMember(logger klog.Logger, oldPod, newPod *v1.Pod) {
	if !cache.isPodGroupMember(newPod) {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.updatePodGroupMember(logger, oldPod, newPod)
}

// RemovePodGroupMember 从缓存中移除 Pod 在其 PodGroup 状态里的条目。
func (cache *cacheImpl) RemovePodGroupMember(pod *v1.Pod) {
	if !cache.isPodGroupMember(pod) {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.removePodGroupMember(pod)
}

// addPodGroupMember 把 Pod 加入其 PodGroup 状态，必要时创建该组条目。调用方需持有缓存锁。
func (cache *cacheImpl) addPodGroupMember(pod *v1.Pod) {
	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	podGroupState, exists := cache.podGroupStates[key]
	if !exists {
		podGroupState = newPodGroupState()
		cache.podGroupStates[key] = podGroupState
	}

	podGroupState.addPod(pod)
}

// updatePodGroupMember 更新 Pod 在其 PodGroup 状态中的条目。调用方需持有缓存锁。
func (cache *cacheImpl) updatePodGroupMember(logger klog.Logger, oldPod, newPod *v1.Pod) {
	key := fwk.PodGroupKey(newPod.Namespace, *newPod.Spec.SchedulingGroup.PodGroupName)
	podGroupState, exists := cache.podGroupStates[key]
	if !exists {
		// 正常情况下该状态应由早前的 Pod Add 事件创建；缺失说明漏掉了 Add 事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod group state not found for update, this indicates a missed add event", "pod", klog.KObj(newPod), "fwk.EntityKey", key)
		return
	}

	podGroupState.updatePod(oldPod, newPod)
}

// removePodGroupMember 从 PodGroup 状态中移除 Pod，组为空时删除该组条目。调用方需持有缓存锁。
func (cache *cacheImpl) removePodGroupMember(pod *v1.Pod) {
	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	podGroupState, exists := cache.podGroupStates[key]
	if !exists {
		return
	}
	podGroupState.deletePod(pod.UID)
	if podGroupState.empty() {
		// 即使没有成员 Pod，只要 PodGroup 对象仍存在，状态就应保留；
		// 只有成员为空且对象已移除时才删除状态。
		delete(cache.podGroupStates, key)
	}
}

// assumePodGroupMember 把 Pod 在其 PodGroup 状态中标记为假定调度。调用方需持有缓存锁。
func (cache *cacheImpl) assumePodGroupMember(pod *v1.Pod) {
	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	podGroupState, exists := cache.podGroupStates[key]
	if !exists {
		podGroupState = newPodGroupState()
		podGroupState.allPods[pod.UID] = pod
		cache.podGroupStates[key] = podGroupState
	}
	podGroupState.assumePod(pod)
}

// forgetPodGroupMember 把 Pod 从 PodGroup 状态的 assumed 集合移回未调度集合。调用方需持有缓存锁。
func (cache *cacheImpl) forgetPodGroupMember(logger klog.Logger, pod *v1.Pod) {
	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	pgs, exists := cache.podGroupStates[key]
	if !exists {
		// 状态应由早前的 Add 或 Assume 事件创建；缺失说明漏掉了事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod group state not found for forget, this indicates a missed add or assume event", "pod", klog.KObj(pod), "fwk.EntityKey", key)
		return
	}
	pgs.forgetPod(pod.UID)
}

// PodGroupStates 返回此缓存的 PodGroupStateLister。
func (cache *cacheImpl) PodGroupStates() fwk.PodGroupStateLister {
	return &podGroupStateListerImpl{cache: cache}
}

// podGroupStateListerImpl 实现 PodGroup 状态的查询接口。
type podGroupStateListerImpl struct {
	cache *cacheImpl
}

// Get 按命名空间和组名返回缓存中的 PodGroup 状态。
func (l *podGroupStateListerImpl) Get(namespace, podGroupName string) (fwk.PodGroupState, error) {
	if !l.cache.genericWorkloadEnabled {
		return nil, fmt.Errorf("generic workload feature gate is disabled")
	}
	l.cache.mu.RLock()
	defer l.cache.mu.RUnlock()

	key := fwk.PodGroupKey(namespace, podGroupName)
	podGroupState, exists := l.cache.podGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("pod group state not found for pod group %s/%s", namespace, podGroupName)
	}
	return podGroupState, nil
}

// PodGroups 返回此缓存的 PodGroupLister。
func (cache *cacheImpl) PodGroups() fwk.PodGroupLister {
	return &podGroupListerImpl{cache: cache}
}

// podGroupListerImpl 实现 PodGroup 对象的查询接口。
type podGroupListerImpl struct {
	cache *cacheImpl
}

// Get 返回缓存中的 PodGroup 对象。
func (l *podGroupListerImpl) Get(namespace, name string) (*schedulingv1beta1.PodGroup, error) {
	if !l.cache.genericWorkloadEnabled {
		return nil, fmt.Errorf("generic workload feature gate is disabled")
	}
	l.cache.mu.RLock()
	defer l.cache.mu.RUnlock()

	key := fwk.PodGroupKey(namespace, name)
	pgs, exists := l.cache.podGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("pod group state not found for pod group %s", key)
	}
	pg := pgs.podGroup
	if pg == nil {
		return nil, fmt.Errorf("pod group object not found for pod group %s", key)
	}
	return pg, nil
}

// CompositePodGroupStates 返回此缓存的 CompositePodGroupStateLister。
func (cache *cacheImpl) CompositePodGroupStates() fwk.CompositePodGroupStateLister {
	return &compositePodGroupStateListerImpl{cache: cache}
}

// compositePodGroupStateListerImpl 实现组合 PodGroup 状态的查询接口。
type compositePodGroupStateListerImpl struct {
	cache *cacheImpl
}

// Get 按命名空间和组名返回缓存中的组合 PodGroup 状态。
func (l *compositePodGroupStateListerImpl) Get(namespace string, podGroupName string) (fwk.CompositePodGroupState, error) {
	if !l.cache.compositePodGroupEnabled {
		return nil, fmt.Errorf("composite pod group feature gate is disabled")
	}
	l.cache.mu.RLock()
	defer l.cache.mu.RUnlock()

	key := fwk.CompositePodGroupKey(namespace, podGroupName)
	cpgs, exists := l.cache.compositePodGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("composite pod group state not found for composite pod group %s", key)
	}
	return cpgs, nil
}

// CompositePodGroups 返回此缓存的 CompositePodGroupLister。
func (cache *cacheImpl) CompositePodGroups() fwk.CompositePodGroupLister {
	return &compositePodGroupListerImpl{cache: cache}
}

// compositePodGroupListerImpl 实现组合 PodGroup 对象的查询接口。
type compositePodGroupListerImpl struct {
	cache *cacheImpl
}

// Get 返回缓存中的组合 PodGroup 对象。
func (l *compositePodGroupListerImpl) Get(namespace, name string) (*schedulingv1alpha3.CompositePodGroup, error) {
	if !l.cache.compositePodGroupEnabled {
		return nil, fmt.Errorf("composite pod group feature gate is disabled")
	}
	l.cache.mu.RLock()
	defer l.cache.mu.RUnlock()

	key := fwk.CompositePodGroupKey(namespace, name)
	cpgs, exists := l.cache.compositePodGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("composite pod group state not found for composite pod group %s", key)
	}
	cpg := cpgs.compositePodGroup
	if cpg == nil {
		return nil, fmt.Errorf("composite pod group object not found for composite pod group %s", key)
	}
	return cpg, nil
}

// BindPod 通过向 dispatcher 添加 bind API 调用来处理 Pod 绑定。
// 仅当 SchedulerAsyncAPICalls 特性门开启时才应调用此方法。
func (cache *cacheImpl) BindPod(binding *v1.Binding) (<-chan error, error) {
	// 不改动缓存：Pod 已被假定，绑定失败时会被 Forget，无需在此重复处理。
	onFinish := make(chan error, 1)
	err := cache.apiDispatcher.Add(apicalls.Implementations.PodBinding(binding), fwk.APICallOptions{
		OnFinish: onFinish,
	})
	if fwk.IsUnexpectedError(err) {
		return onFinish, err
	}
	return onFinish, nil
}

// refreshPVCRefCountsDelta 把节点的 PVC 引用计数累加到 cache.pvcRefCountsDelta，
// 供 UpdateSnapshot 时应用。sign 为 +1 表示加入节点贡献，-1 表示移除。
func (cache *cacheImpl) refreshPVCRefCountsDelta(nodeInfo *framework.NodeInfo, sign int) {
	for key, count := range nodeInfo.PVCRefCounts {
		cache.pvcRefCountsDelta[key] += sign * count
	}
}

// applyPVCRefCountDelta 把增量合并到快照的 PVC 引用计数，删除归零条目并清空增量。
func (cache *cacheImpl) applyPVCRefCountDelta(snapshot *Snapshot) error {
	for key, delta := range cache.pvcRefCountsDelta {
		snapshot.usedPVCRefCounts[key] += delta
		if refCount := snapshot.usedPVCRefCounts[key]; refCount <= 0 {
			if refCount < 0 {
				delete(snapshot.usedPVCRefCounts, key)
				return fmt.Errorf("PVC %s had negative ref count %v", key, refCount)
			}
			delete(snapshot.usedPVCRefCounts, key)
		}
	}
	cache.pvcRefCountsDelta = map[string]int{}
	return nil
}

// AddPodGroup 把 PodGroup 对象加入缓存，并维护其与父组合 PodGroup 的关联。
func (cache *cacheImpl) AddPodGroup(podGroup *schedulingv1beta1.PodGroup) {
	if !cache.genericWorkloadEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := fwk.PodGroupKey(podGroup.Namespace, podGroup.Name)
	pgs, exists := cache.podGroupStates[key]
	if !exists {
		pgs = newPodGroupState()
		cache.podGroupStates[key] = pgs
	}
	pgs.setPodGroup(podGroup)

	if !cache.compositePodGroupEnabled || podGroup.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parentKey := fwk.CompositePodGroupKey(podGroup.Namespace, *podGroup.Spec.ParentCompositePodGroupName)

	// 父组合 PodGroup 可能尚未到达，但迟早会被观察到；先创建占位条目，避免丢失父子关系。
	parent, parentExists := cache.compositePodGroupStates[parentKey]
	if !parentExists {
		parent = newCompositePodGroupState()
		cache.compositePodGroupStates[parentKey] = parent
	}
	parent.addChild(key)
}

// UpdatePodGroup 更新缓存中的 PodGroup 对象。
func (cache *cacheImpl) UpdatePodGroup(logger klog.Logger, oldPodGroup, newPodGroup *schedulingv1beta1.PodGroup) {
	if !cache.genericWorkloadEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := fwk.PodGroupKey(newPodGroup.Namespace, newPodGroup.Name)
	pgs, exists := cache.podGroupStates[key]
	if !exists {
		// 状态应由早前的 Add 事件创建；缺失说明漏掉了事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod group state not found for update, this indicates a missed add event", "podGroup", klog.KObj(newPodGroup))
		return
	}
	pgs.setPodGroup(newPodGroup)
}

// RemovePodGroup 从缓存中移除 PodGroup 对象及其父子关联。
func (cache *cacheImpl) RemovePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup) {
	if !cache.genericWorkloadEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := fwk.PodGroupKey(podGroup.Namespace, podGroup.Name)
	pgs, exists := cache.podGroupStates[key]
	if !exists {
		// 状态应由早前的 Add 事件创建；缺失说明漏掉了事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod group state not found for removal", "podGroup", klog.KObj(podGroup))
		return
	}

	if cache.compositePodGroupEnabled && podGroup.Spec.ParentCompositePodGroupName != nil {
		parentKey := fwk.CompositePodGroupKey(podGroup.Namespace, *podGroup.Spec.ParentCompositePodGroupName)
		if parent, exists := cache.compositePodGroupStates[parentKey]; exists {
			parent.removeChild(key)
			if parent.empty() {
				// 父组合对象即使没有子节点，只要自身对象仍存在就应保留；此处为空说明对象也已移除。
				delete(cache.compositePodGroupStates, parentKey)
			}
		}
	}

	pgs.removePodGroup()
	if pgs.empty() {
		delete(cache.podGroupStates, key)
	}
}

// AddCompositePodGroup 把组合 PodGroup 加入缓存，并维护其与父级的关系。
func (cache *cacheImpl) AddCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup) {
	if !cache.compositePodGroupEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := fwk.CompositePodGroupKey(cpg.Namespace, cpg.Name)
	cpgState, exists := cache.compositePodGroupStates[key]
	if !exists {
		cpgState = newCompositePodGroupState()
		cache.compositePodGroupStates[key] = cpgState
	}
	cpgState.setCompositePodGroup(cpg)

	if cpg.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parentKey := fwk.CompositePodGroupKey(cpg.Namespace, *cpg.Spec.ParentCompositePodGroupName)

	parent, parentExists := cache.compositePodGroupStates[parentKey]
	if !parentExists {
		parent = newCompositePodGroupState()
		cache.compositePodGroupStates[parentKey] = parent
	}
	parent.addChild(key)
}

// UpdateCompositePodGroup 更新缓存中的组合 PodGroup 对象。
func (cache *cacheImpl) UpdateCompositePodGroup(logger klog.Logger, oldCPG, newCPG *schedulingv1alpha3.CompositePodGroup) {
	if !cache.compositePodGroupEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	key := fwk.CompositePodGroupKey(newCPG.Namespace, newCPG.Name)
	pgs, exists := cache.compositePodGroupStates[key]
	if !exists {
		// 状态应由早前的 Add 事件创建；缺失说明漏掉了事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Pod group state not found for update, this indicates a missed add event", "podGroup", klog.KObj(newCPG))
		return
	}
	pgs.setCompositePodGroup(newCPG)
}

// RemoveCompositePodGroup 从缓存中移除组合 PodGroup 及其父子关联。
func (cache *cacheImpl) RemoveCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup) {
	if !cache.compositePodGroupEnabled {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	key := fwk.CompositePodGroupKey(cpg.Namespace, cpg.Name)
	cpgs, exists := cache.compositePodGroupStates[key]
	if !exists {
		// 状态应由早前的 Add 事件创建；缺失说明漏掉了事件。
		utilruntime.HandleErrorWithLogger(logger, nil, "Composite pod group state not found for removal", "compositePodGroup", klog.KObj(cpg))
		return
	}

	if cpgs.compositePodGroup != nil && cpgs.compositePodGroup.Spec.ParentCompositePodGroupName != nil {
		parentKey := fwk.CompositePodGroupKey(cpg.Namespace, *cpgs.compositePodGroup.Spec.ParentCompositePodGroupName)
		parent, exists := cache.compositePodGroupStates[parentKey]
		if !exists {
			return
		}
		parent.removeChild(key)
		if parent.empty() {
			delete(cache.compositePodGroupStates, parentKey)
		}
	}

	cpgs.removeCompositePodGroup()
	if cpgs.empty() {
		delete(cache.compositePodGroupStates, key)
	}
}

// BuildHierarchySnapshotFromPod 返回给定 Pod 所属 PodGroup 层级的快照。
func (cache *cacheImpl) BuildHierarchySnapshotFromPod(pod *v1.Pod) (fwk.PodGroupManager, error) {
	if pod.Spec.SchedulingGroup == nil {
		return nil, fmt.Errorf("pod has no scheduling group")
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// 1. 沿父级链向上找到根 CPG/PG。
	currentKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	pgs, exists := cache.podGroupStates[currentKey]
	if !exists {
		return nil, fmt.Errorf("pod group state not found for %s", currentKey.String())
	}

	pg := pgs.podGroup
	if pg == nil {
		return nil, fmt.Errorf("pod group object not found in state for %s", currentKey.String())
	}

	if cache.compositePodGroupEnabled && pg.Spec.ParentCompositePodGroupName != nil {
		currentKey = fwk.CompositePodGroupKey(pod.Namespace, *pg.Spec.ParentCompositePodGroupName)
		for range schedulingv1alpha3.WorkloadMaxTreeDepth - 1 {
			cpgs, exists := cache.compositePodGroupStates[currentKey]
			if !exists {
				return nil, fmt.Errorf("parent composite pod group state not found for %s", currentKey.String())
			}
			cpg := cpgs.compositePodGroup
			if cpg == nil {
				return nil, fmt.Errorf("composite pod group object not found in state for %s", currentKey.String())
			}
			if cpg.Spec.ParentCompositePodGroupName == nil {
				break
			}
			currentKey = fwk.CompositePodGroupKey(pod.Namespace, *cpg.Spec.ParentCompositePodGroupName)
		}
	}

	// 2. 从根开始向下遍历，填充层级快照。
	snapshot := NewEmptySnapshot()
	visited := sets.New[fwk.EntityKey]()
	err := cache.buildPodGroupStateSnapshotTree(currentKey, snapshot, visited)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// buildPodGroupStateSnapshotTree 从给定键开始递归构建 PodGroup 状态树快照。
// 调用方需持有缓存锁。
func (cache *cacheImpl) buildPodGroupStateSnapshotTree(key fwk.EntityKey, snapshot *Snapshot, visited sets.Set[fwk.EntityKey]) error {
	if visited.Has(key) {
		return fmt.Errorf("cycle detected in composite pod group hierarchy: %s", key.String())
	}
	visited.Insert(key)

	switch key.Type {
	case fwk.PodGroupKeyType:
		pgs, exists := cache.podGroupStates[key]
		if !exists {
			return fmt.Errorf("pod group state not found for %s", key.String())
		}
		snapshot.podGroupStates[key] = &podGroupStateSnapshot{podGroupStateData: pgs.podGroupStateData.clone()}

	case fwk.CompositePodGroupKeyType:
		cpgs, exists := cache.compositePodGroupStates[key]
		if !exists {
			return fmt.Errorf("composite pod group state not found for %s", key.String())
		}
		snapshot.compositePodGroupStates[key] = &compositePodGroupStateSnapshot{compositePodGroupStateData: cpgs.compositePodGroupStateData.clone()}

		children := cpgs.children.Clone()
		for childKey := range children {
			if err := cache.buildPodGroupStateSnapshotTree(childKey, snapshot, visited); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetRootKeyForGroup 返回给定 EntityKey 所在层级的根键。
// 键必须是 PodGroupKey 或 CompositePodGroupKey 类型。
func (cache *cacheImpl) GetRootKeyForGroup(key fwk.EntityKey) (fwk.EntityKey, bool, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	currentKey := key
	visited := sets.New[fwk.EntityKey]()
	for {
		if visited.Has(currentKey) {
			return fwk.EntityKey{}, false, fmt.Errorf("cycle detected in the hierarchy: %v", visited.UnsortedList())
		}
		visited.Insert(currentKey)

		switch currentKey.Type {
		case fwk.PodGroupKeyType:
			pgs, exists := cache.podGroupStates[currentKey]
			if !exists {
				return fwk.EntityKey{}, false, nil
			}
			pg := pgs.podGroup
			if pg == nil {
				return fwk.EntityKey{}, false, nil
			}
			if !cache.compositePodGroupEnabled || pg.Spec.ParentCompositePodGroupName == nil {
				return currentKey, true, nil
			}
			currentKey = fwk.CompositePodGroupKey(pg.Namespace, *pg.Spec.ParentCompositePodGroupName)
		case fwk.CompositePodGroupKeyType:
			cpgs, exists := cache.compositePodGroupStates[currentKey]
			if !exists {
				return fwk.EntityKey{}, false, nil
			}
			cpg := cpgs.compositePodGroup
			if cpg == nil {
				return fwk.EntityKey{}, false, nil
			}
			if cpg.Spec.ParentCompositePodGroupName == nil {
				return currentKey, true, nil
			}
			currentKey = fwk.CompositePodGroupKey(cpg.Namespace, *cpg.Spec.ParentCompositePodGroupName)
		case fwk.PodKeyType:
			return fwk.EntityKey{}, false, fmt.Errorf("pod key type not supported in GetRootKeyForGroup for %s", currentKey.String())
		}
	}
}
