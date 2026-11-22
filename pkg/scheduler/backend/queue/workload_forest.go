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

package queue

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// workloadForest 维护已观察到的 PodGroup 对象的一致视图。
// 它保证调度队列的不变量不受调度缓存异步更新的影响。
// 调度队列之外，应以缓存为准。
// 该结构非线程安全，只能在 PriorityQueue 的锁内访问。
type workloadForest struct {
	podGroups          map[fwk.EntityKey]*schedulingv1beta1.PodGroup
	compositePodGroups map[fwk.EntityKey]*schedulingv1alpha3.CompositePodGroup
	// children 记录父 CompositePodGroup 到其直接子项（PodGroup 或 CompositePodGroup）的映射。
	// 该结构的不变量：无论父对象是否已被观察到，子到父的关系都会先记录在此。
	// 这样父对象最终加入时无需遍历所有组再反向补全子项。

	children                   map[fwk.EntityKey]sets.Set[fwk.EntityKey]
	isCompositePodGroupEnabled bool
}

func newWorkloadForest(isCompositePodGroupEnabled bool) *workloadForest {
	return &workloadForest{
		podGroups:                  make(map[fwk.EntityKey]*schedulingv1beta1.PodGroup),
		compositePodGroups:         make(map[fwk.EntityKey]*schedulingv1alpha3.CompositePodGroup),
		children:                   make(map[fwk.EntityKey]sets.Set[fwk.EntityKey]),
		isCompositePodGroupEnabled: isCompositePodGroupEnabled,
	}
}

// addPodGroup 将一个 PodGroup 加入森林。
func (wf *workloadForest) addPodGroup(podGroup *schedulingv1beta1.PodGroup) {
	pgKey := podGroupKey(podGroup)
	wf.podGroups[pgKey] = podGroup

	if !wf.podGroupHasParent(podGroup) {
		return
	}

	parentKey := fwk.CompositePodGroupKey(podGroup.Namespace, *podGroup.Spec.ParentCompositePodGroupName)
	_, exists := wf.children[parentKey]
	if !exists {
		wf.children[parentKey] = sets.New[fwk.EntityKey]()
	}
	wf.children[parentKey].Insert(pgKey)
}

// updatePodGroup 更新森林中的一个 PodGroup。
func (wf *workloadForest) updatePodGroup(podGroup *schedulingv1beta1.PodGroup) {
	wf.podGroups[podGroupKey(podGroup)] = podGroup
}

// deletePodGroup 从森林中移除一个 PodGroup。
func (wf *workloadForest) deletePodGroup(podGroup *schedulingv1beta1.PodGroup) {
	pgKey := podGroupKey(podGroup)
	delete(wf.podGroups, pgKey)

	if !wf.podGroupHasParent(podGroup) {
		return
	}

	parentKey := fwk.CompositePodGroupKey(podGroup.Namespace, *podGroup.Spec.ParentCompositePodGroupName)
	parentChildren, exists := wf.children[parentKey]
	if !exists {
		return
	}
	parentChildren.Delete(pgKey)
	if parentChildren.Len() == 0 {
		delete(wf.children, parentKey)
	}
}

// addCompositePodGroup 将一个 CompositePodGroup 加入森林。
func (wf *workloadForest) addCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) {
	cpgKey := compositePodGroupKey(cpg)
	wf.compositePodGroups[cpgKey] = cpg

	if cpg.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parentKey := fwk.CompositePodGroupKey(cpg.Namespace, *cpg.Spec.ParentCompositePodGroupName)
	_, exists := wf.children[parentKey]
	if !exists {
		wf.children[parentKey] = sets.New[fwk.EntityKey]()
	}
	wf.children[parentKey].Insert(cpgKey)
}

// updateCompositePodGroup 更新森林中的一个 CompositePodGroup。
func (wf *workloadForest) updateCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) {
	wf.compositePodGroups[compositePodGroupKey(cpg)] = cpg
}

// deleteCompositePodGroup 从森林中移除一个 CompositePodGroup。
func (wf *workloadForest) deleteCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) {
	cpgKey := compositePodGroupKey(cpg)
	delete(wf.compositePodGroups, cpgKey)

	if cpg.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parentKey := fwk.CompositePodGroupKey(cpg.Namespace, *cpg.Spec.ParentCompositePodGroupName)
	parentChildren, exists := wf.children[parentKey]
	if !exists {
		return
	}

	parentChildren.Delete(cpgKey)
	if parentChildren.Len() == 0 {
		delete(wf.children, parentKey)
	}
}

// getRootLookupInfoForPod 返回 Pod 所属层级的根组（PodGroup 或 CompositePodGroup）的查找信息。
func (wf *workloadForest) getRootLookupInfoForPod(pod *v1.Pod) (*framework.QueuedPodGroupInfo, bool) {
	podGroup, exists := wf.podGroups[podGroupKeyForPod(pod)]
	if !exists {
		return nil, false
	}
	return wf.getRootLookupInfoForPodGroup(podGroup)
}

// getRootLookupInfoForPodGroup 返回给定 PodGroup 所属层级的根组查找信息。
func (wf *workloadForest) getRootLookupInfoForPodGroup(podGroup *schedulingv1beta1.PodGroup) (*framework.QueuedPodGroupInfo, bool) {
	podGroup, exists := wf.podGroups[podGroupKey(podGroup)]
	if !exists {
		return nil, false
	}

	if !wf.podGroupHasParent(podGroup) {
		return &framework.QueuedPodGroupInfo{
			PodGroupInfo: &framework.PodGroupInfo{
				Namespace: podGroup.Namespace,
				Name:      podGroup.Name,
				Type:      fwk.PodGroupKeyType,
			},
		}, true
	}
	return wf.getRootLookupInfoForParentCPG(*podGroup.Spec.ParentCompositePodGroupName, podGroup.Namespace)
}

// getRootLookupInfoForCPG 返回给定 CompositePodGroup 所属层级的根 CPG 查找信息。
func (wf *workloadForest) getRootLookupInfoForCPG(cpg *schedulingv1alpha3.CompositePodGroup) (*framework.QueuedPodGroupInfo, bool) {
	return wf.getRootLookupInfoForParentCPG(cpg.Name, cpg.Namespace)
}

// getRootLookupInfoForParentCPG 沿父链向上遍历，返回根 CompositePodGroup 的查找信息。
func (wf *workloadForest) getRootLookupInfoForParentCPG(parentName, namespace string) (*framework.QueuedPodGroupInfo, bool) {
	currParentName := parentName
	visited := sets.New[fwk.EntityKey]()
	for {
		cpgKey := fwk.CompositePodGroupKey(namespace, currParentName)
		if visited.Has(cpgKey) {
			// TODO(jdzikowski): 将 logger 传递到调度队列的 getPod 方法。
			utilruntime.HandleError(fmt.Errorf("cycle detected in composite pod group hierarchy: %s/%s", parentName, namespace))
			return nil, false
		}
		visited.Insert(cpgKey)

		cpg, exists := wf.compositePodGroups[cpgKey]
		if !exists {
			return nil, false
		}

		if cpg.Spec.ParentCompositePodGroupName == nil {
			return &framework.QueuedPodGroupInfo{
				PodGroupInfo: &framework.PodGroupInfo{
					Namespace: cpg.Namespace,
					Name:      cpg.Name,
					Type:      fwk.CompositePodGroupKeyType,
				},
			}, true
		}
		currParentName = *cpg.Spec.ParentCompositePodGroupName
	}
}

// getLeafPodGroups 返回以给定组为根的子树中所有作为叶节点的 PodGroup。
func (wf *workloadForest) getLeafPodGroups(logger klog.Logger, rootLookupInfo *framework.QueuedPodGroupInfo) []*schedulingv1beta1.PodGroup {
	var pgs []*schedulingv1beta1.PodGroup
	var key fwk.EntityKey
	if rootLookupInfo.GetType() == fwk.PodGroupKeyType {
		key = fwk.PodGroupKey(rootLookupInfo.GetNamespace(), rootLookupInfo.GetName())
		pg, exists := wf.podGroups[key]
		if !exists {
			return pgs
		}
		pgs = append(pgs, pg)
		return pgs
	}

	key = fwk.CompositePodGroupKey(rootLookupInfo.GetNamespace(), rootLookupInfo.GetName())
	queue := []fwk.EntityKey{key}
	visited := sets.New[fwk.EntityKey]()

	for len(queue) > 0 {
		currKey := queue[0]
		queue = queue[1:]

		if visited.Has(currKey) {
			utilruntime.HandleErrorWithLogger(logger, nil, "cycle detected in composite pod group hierarchy", "cpg-name", rootLookupInfo.GetName(), "namespace", rootLookupInfo.GetNamespace())
			return pgs
		}
		visited.Insert(currKey)

		children, exists := wf.children[currKey]
		if !exists {
			continue
		}

		for childKey := range children {
			if pg, isPG := wf.podGroups[childKey]; isPG {
				pgs = append(pgs, pg)
			}
			if _, isCPG := wf.compositePodGroups[childKey]; isCPG {
				queue = append(queue, childKey)
			}
		}
	}

	return pgs
}

// getPodGroup 返回查找信息对应的当前 PodGroup 对象。
func (wf *workloadForest) getPodGroup(pgLookup *schedulingv1beta1.PodGroup) (*schedulingv1beta1.PodGroup, bool) {
	podGroup, ok := wf.podGroups[podGroupKey(pgLookup)]
	return podGroup, ok
}

// getCompositePodGroup 返回查找信息对应的当前 CompositePodGroup 对象。
func (wf *workloadForest) getCompositePodGroup(cpgLookup *schedulingv1alpha3.CompositePodGroup) (*schedulingv1alpha3.CompositePodGroup, bool) {
	podGroup, ok := wf.compositePodGroups[compositePodGroupKey(cpgLookup)]
	return podGroup, ok
}

// buildPodGroupInfoForPG 为给定 PodGroup 构造 PodGroupInfo 表示，
// 借助 visited 集合检测层级中的环。
func (wf *workloadForest) buildPodGroupInfoForPG(logger klog.Logger, pg *schedulingv1beta1.PodGroup, visited sets.Set[fwk.EntityKey]) *framework.PodGroupInfo {
	key := podGroupKey(pg)
	if visited.Has(key) {
		utilruntime.HandleErrorWithLogger(logger, nil, "cycle detected in composite pod group hierarchy", "pg-name", pg.Name, "namespace", pg.Namespace)
		return nil
	}
	visited.Insert(key)

	return &framework.PodGroupInfo{
		Namespace: pg.Namespace,
		Name:      pg.Name,
		Type:      fwk.PodGroupKeyType,
		PodGroup:  pg,
		Children:  make([]*framework.PodGroupInfo, 0),
	}
}

// buildPodGroupInfoForCPG 递归为给定 CompositePodGroup 及其所有子项构造 PodGroupInfo 表示，
// 同样用 visited 集合防止层级成环。
func (wf *workloadForest) buildPodGroupInfoForCPG(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup, visited sets.Set[fwk.EntityKey]) *framework.PodGroupInfo {
	key := compositePodGroupKey(cpg)
	if visited.Has(key) {
		utilruntime.HandleErrorWithLogger(logger, nil, "cycle detected in composite pod group hierarchy", "cpg-name", cpg.Name, "namespace", cpg.Namespace)
		return nil
	}
	visited.Insert(key)

	pgi := &framework.PodGroupInfo{
		Namespace:         cpg.Namespace,
		Name:              cpg.Name,
		Type:              fwk.CompositePodGroupKeyType,
		CompositePodGroup: cpg,
		Children:          make([]*framework.PodGroupInfo, 0),
	}

	childrenSet, ok := wf.children[key]
	if !ok {
		return pgi
	}
	for childKey := range childrenSet {
		if childPG, ok := wf.podGroups[childKey]; ok {
			if childInfo := wf.buildPodGroupInfoForPG(logger, childPG, visited); childInfo != nil {
				pgi.Children = append(pgi.Children, childInfo)
			}
		}
		if childCPG, ok := wf.compositePodGroups[childKey]; ok {
			if childInfo := wf.buildPodGroupInfoForCPG(logger, childCPG, visited); childInfo != nil {
				pgi.Children = append(pgi.Children, childInfo)
			}
		}
	}
	return pgi
}

// buildQueuedPodGroupInfo 从给定的根查找信息出发构造 QueuedPodGroupInfo，
// 展开完整的 PodGroupInfo 层级并初始化 QueuedPodInfos map。
func (wf *workloadForest) buildQueuedPodGroupInfo(logger klog.Logger, rootLookup *framework.QueuedPodGroupInfo) *framework.QueuedPodGroupInfo {
	var pgi *framework.PodGroupInfo
	switch rootLookup.GetType() {
	case fwk.PodGroupKeyType:
		if pg, ok := wf.podGroups[fwk.PodGroupKey(rootLookup.GetNamespace(), rootLookup.GetName())]; ok && pg != nil {
			pgi = wf.buildPodGroupInfoForPG(logger, pg, sets.New[fwk.EntityKey]())
		}
	case fwk.CompositePodGroupKeyType:
		if cpg, ok := wf.compositePodGroups[fwk.CompositePodGroupKey(rootLookup.GetNamespace(), rootLookup.GetName())]; ok && cpg != nil {
			pgi = wf.buildPodGroupInfoForCPG(logger, cpg, sets.New[fwk.EntityKey]())
		}
	}

	if pgi == nil {
		return nil
	}
	return &framework.QueuedPodGroupInfo{
		PodGroupInfo:   pgi,
		QueuedPodInfos: make(map[fwk.EntityKey][]*framework.QueuedPodInfo),
	}
}

// podGroupHasParent 检查 PodGroup 是否配置了父 CompositePodGroup，
// 并且 CompositePodGroup 特性已启用。
func (wf *workloadForest) podGroupHasParent(pg *schedulingv1beta1.PodGroup) bool {
	return wf.isCompositePodGroupEnabled && pg.Spec.ParentCompositePodGroupName != nil
}
