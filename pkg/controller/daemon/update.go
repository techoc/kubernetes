/*
Copyright 2017 The Kubernetes Authors.

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

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"

	"k8s.io/klog/v2"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/daemon/util"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"k8s.io/utils/ptr"
)

// rollingUpdate 在更新策略允许的范围内确定要删除的旧 Pod 或要在节点上额外创建的新 Pod。
func (dsc *DaemonSetsController) rollingUpdate(ctx context.Context, ds *apps.DaemonSet, nodeList []*v1.Node, hash string) error {
	logger := klog.FromContext(ctx)
	nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ctx, ds, false)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}
	maxSurge, maxUnavailable, desiredNumberScheduled, err := dsc.updatedDesiredNodeCounts(ctx, ds, nodeList, nodeToDaemonPods)
	if err != nil {
		return fmt.Errorf("couldn't get unavailable numbers: %v", err)
	}

	now := dsc.failedPodsBackoff.Clock.Now()

	// 不允许 surge 时，只删除足够数量的旧 Pod 以保持在 maxUnavailable 限制内，
	// 新 Pod 由核心管理循环在这些节点上创建。
	//
	// 前提：
	// * 管理循环保证每个节点最多一个 Pod；
	// * 管理循环负责创建新 Pod；
	// * 管理循环负责处理失败 Pod；
	// * 被删除的 Pod 不计入不可用数，否则节点宕机时更新无法推进。
	// 不变量：
	// * 不可用的新 Pod 数必须小于 maxUnavailable；
	// * 拥有可用旧 Pod 的节点，在不违反其他不变量的前提下可作为删除候选。
	//
	if maxSurge == 0 {
		var numUnavailable int
		var allowedReplacementPods []string
		var candidatePodsToDelete []string
		for nodeName, pods := range nodeToDaemonPods {
			newPod, oldPod, ok := findUpdatedPodsOnNode(ds, pods, hash)
			if !ok {
				// Pod 数量异常时交给管理循环清理，此处保守地按不可用处理。
				logger.V(3).Info("DaemonSet has excess pods on node, skipping to allow the core loop to process", "daemonset", klog.KObj(ds), "node", klog.KRef("", nodeName))
				numUnavailable++
				continue
			}
			switch {
			case oldPod == nil && newPod == nil, oldPod != nil && newPod != nil:
				// 缺失或重复的 Pod 由管理循环修正，在此之前按不可用处理。
				numUnavailable++
			case newPod != nil:
				// 已是新版本，只需检查其可用性。
				if !podutil.IsPodAvailable(newPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
					// 新版本 Pod 不可用会占用 maxUnavailable 配额。
					numUnavailable++
				}
			default:
				// 只有旧版本 Pod 才能作为替换候选。
				switch {
				case !podutil.IsPodAvailable(oldPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}):
					// 旧 Pod 已不可用时必须立即替换，即使会超过 maxUnavailable。
					logger.V(5).Info("DaemonSet pod on node is out of date and not available, allowing replacement", "daemonset", klog.KObj(ds), "pod", klog.KObj(oldPod), "node", klog.KRef("", nodeName))
					// 单独记录，优先于普通候选执行。
					if allowedReplacementPods == nil {
						allowedReplacementPods = make([]string, 0, len(nodeToDaemonPods))
					}
					allowedReplacementPods = append(allowedReplacementPods, oldPod.Name)
					numUnavailable++
				case numUnavailable >= maxUnavailable:
					// 配额已用尽，继续遍历也不会得到可执行候选。
					continue
				default:
					logger.V(5).Info("DaemonSet pod on node is out of date, this is a candidate to replace", "daemonset", klog.KObj(ds), "pod", klog.KObj(oldPod), "node", klog.KRef("", nodeName))
					// 记录可用旧 Pod 的候选，稍后统一排序删除。
					if candidatePodsToDelete == nil {
						candidatePodsToDelete = make([]string, 0, maxUnavailable)
					}
					candidatePodsToDelete = append(candidatePodsToDelete, oldPod.Name)
				}
			}
		}

		// 先使用必须替换的 Pod，再用普通候选填充剩余配额。
		logger.V(5).Info("DaemonSet allowing replacements", "daemonset", klog.KObj(ds), "replacements", len(allowedReplacementPods), "maxUnavailable", maxUnavailable, "numUnavailable", numUnavailable, "candidates", len(candidatePodsToDelete))
		remainingUnavailable := maxUnavailable - numUnavailable
		if remainingUnavailable < 0 {
			remainingUnavailable = 0
		}
		if max := len(candidatePodsToDelete); remainingUnavailable > max {
			remainingUnavailable = max
		}
		oldPodsToDelete := append(allowedReplacementPods, candidatePodsToDelete[:remainingUnavailable]...)

		return dsc.syncNodes(ctx, ds, oldPodsToDelete, nil, hash)
	}

	// 允许 surge 时，旧 Pod 不可用即创建新 Pod，最多额外创建 maxSurge 个。
	//
	// 前提：
	// * 管理循环保证每个节点最多两个 Pod（一旧一新）；
	// * 节点上无 Pod 时由管理循环创建；
	// * 管理循环负责处理失败 Pod；
	// * 被删除的 Pod 不计入不可用数，否则节点宕机时更新无法推进。
	// 不变量：
	// * 旧 Pod 不可用的节点可立即创建新 Pod；
	// * 新 Pod 可用后删除对应的旧 Pod；
	// * 任意时刻为可用旧 Pod 创建的新 Pod 不超过 maxSurge 个。
	//
	// 已在新节点就绪或位于不再调度节点上的旧 Pod。
	var oldPodsToDelete []string
	// 因调度约束变化而需要删除的候选 Pod。
	var shouldNotRunPodsToDelete []string
	var candidateNewNodes []string
	var allowedNewNodes []string
	var numSurge int
	var numAvailable int

	// 容忍度与节点亲和性对所有节点都相同，提前解析一次避免在每个节点上重复计算。
	tolerations := daemonPodTolerations(ds)
	requiredNodeAffinity := nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity)
	for nodeName, pods := range nodeToDaemonPods {
		newPod, oldPod, ok := findUpdatedPodsOnNode(ds, pods, hash)
		if !ok {
			// Pod 数量异常时交给管理循环清理，此处保守地按 surge 节点计数。
			logger.V(3).Info("DaemonSet has excess pods on node, skipping to allow the core loop to process", "daemonset", klog.KObj(ds), "node", klog.KRef("", nodeName))
			numSurge++
			continue
		}

		// 先统计所有节点的可用性（包括因调度约束即将停止调度的节点），
		// 后续据此限制删除数量，保证应用始终有可用 Pod。
		if oldPod != nil {
			if podutil.IsPodAvailable(oldPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
				numAvailable++
			}
		} else if newPod != nil {
			if podutil.IsPodAvailable(newPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
				numAvailable++
			}
		}

		switch {
		case oldPod == nil:
			// 没有旧 Pod，无需在此处理，管理循环会补齐新 Pod。
		case newPod == nil:
			// 只有旧 Pod，属于可 surge 的节点。
			switch {
			case !podutil.IsPodAvailable(oldPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}):
				node, err := dsc.nodeLister.Get(nodeName)
				if err != nil {
					return fmt.Errorf("couldn't get node for nodeName %q: %v", nodeName, err)
				}
				if shouldRun, _ := nodeShouldRunDaemonPod(logger, node, ds, tolerations, requiredNodeAffinity); !shouldRun {
					logger.V(5).Info("DaemonSet pod on node is not available and does not match scheduling constraints, remove old pod", "daemonset", klog.KObj(ds), "node", nodeName, "oldPod", klog.KObj(oldPod))
					oldPodsToDelete = append(oldPodsToDelete, oldPod.Name)
					continue
				}
				// 旧 Pod 已不可用，允许立即在此创建新 Pod。
				logger.V(5).Info("Pod on node is out of date and not available, allowing replacement", "daemonset", klog.KObj(ds), "pod", klog.KObj(oldPod), "node", klog.KRef("", nodeName))
				// 单独记录，优先于普通 surge 候选执行。
				if allowedNewNodes == nil {
					allowedNewNodes = make([]string, 0, len(nodeToDaemonPods))
				}
				allowedNewNodes = append(allowedNewNodes, nodeName)
			default:
				node, err := dsc.nodeLister.Get(nodeName)
				if err != nil {
					return fmt.Errorf("couldn't get node for nodeName %q: %v", nodeName, err)
				}
				if shouldRun, _ := nodeShouldRunDaemonPod(logger, node, ds, tolerations, requiredNodeAffinity); !shouldRun {
					shouldNotRunPodsToDelete = append(shouldNotRunPodsToDelete, oldPod.Name)
					continue
				}
				if numSurge >= maxSurge {
					// surge 配额已用尽，无需继续挑选候选。
					continue
				}
				logger.V(5).Info("DaemonSet pod on node is out of date, this is a surge candidate", "daemonset", klog.KObj(ds), "pod", klog.KObj(oldPod), "node", klog.KRef("", nodeName))
				// 记录普通 surge 候选，稍后按配额创建。
				if candidateNewNodes == nil {
					candidateNewNodes = make([]string, 0, maxSurge)
				}
				candidateNewNodes = append(candidateNewNodes, nodeName)
			}
		default:
			// 新旧 Pod 并存，说明此前已在该节点 surge，需要判断新 Pod 是否已就绪。
			if !podutil.IsPodAvailable(newPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
				// 新 Pod 尚未就绪，继续占用一个 surge 名额。
				numSurge++
				continue
			}
			// 新 Pod 已就绪，可以安全删除旧 Pod。
			logger.V(5).Info("DaemonSet pod on node is available, remove old pod", "daemonset", klog.KObj(ds), "newPod", klog.KObj(newPod), "node", nodeName, "oldPod", klog.KObj(oldPod))
			oldPodsToDelete = append(oldPodsToDelete, oldPod.Name)
		}
	}

	// 优先使用必须替换的节点，再用普通候选填充剩余 surge 配额。
	logger.V(5).Info("DaemonSet allowing replacements", "daemonset", klog.KObj(ds), "replacements", len(allowedNewNodes), "maxSurge", maxSurge, "numSurge", numSurge, "candidates", len(candidateNewNodes))
	remainingSurge := maxSurge - numSurge

	// 使用 maxSurge 时应用所有者期望 100% 可用。
	// 调度约束从节点 A 迁移到节点 B 时，必须等 B 上 Pod 就绪才删除 A 上的 Pod，
	// 避免出现完全没有可用 Pod 的窗口。
	if deletablePodsNumber := numAvailable - desiredNumberScheduled; deletablePodsNumber > 0 {
		if shouldNotRunPodsToDeleteNumber := len(shouldNotRunPodsToDelete); deletablePodsNumber > shouldNotRunPodsToDeleteNumber {
			deletablePodsNumber = shouldNotRunPodsToDeleteNumber
		}
		for _, podToDeleteName := range shouldNotRunPodsToDelete[:deletablePodsNumber] {
			podToDelete, err := dsc.podLister.Pods(ds.Namespace).Get(podToDeleteName)
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("couldn't get pod which should be deleted due to scheduling constraints %q: %v", podToDeleteName, err)
			}
			logger.V(5).Info("DaemonSet pod on node should be deleted due to scheduling constraints", "daemonset", klog.KObj(ds), "pod", klog.KObj(podToDelete), "node", podToDelete.Spec.NodeName)
			oldPodsToDelete = append(oldPodsToDelete, podToDeleteName)
		}
	}

	if remainingSurge < 0 {
		remainingSurge = 0
	}
	if max := len(candidateNewNodes); remainingSurge > max {
		remainingSurge = max
	}
	newNodesToCreate := append(allowedNewNodes, candidateNewNodes[:remainingSurge]...)

	return dsc.syncNodes(ctx, ds, oldPodsToDelete, newNodesToCreate, hash)
}

// findUpdatedPodsOnNode 检查节点上未删除的 Pod：新、旧版本各最多一个时返回 true，
// 出现多个时返回 false。此时跳过该节点，交给管理循环清理多余 Pod，下次再处理。
func findUpdatedPodsOnNode(ds *apps.DaemonSet, podsOnNode []*v1.Pod, hash string) (newPod, oldPod *v1.Pod, ok bool) {
	for _, pod := range podsOnNode {
		// 正在删除的 Pod 不应参与新旧版本判定。
		if pod.DeletionTimestamp != nil {
			continue
		}
		generation, err := util.GetTemplateGeneration(ds)
		if err != nil {
			generation = nil
		}
		if util.IsPodUpdated(pod, hash, generation) {
			if newPod != nil {
				return nil, nil, false
			}
			newPod = pod
		} else {
			if oldPod != nil {
				return nil, nil, false
			}
			oldPod = pod
		}
	}
	return newPod, oldPod, true
}

// constructHistory 查找 DaemonSet 控制的所有历史版本，更新当前版本号或按需创建新历史；
// 同时去重当前历史，并为已有历史补充缺失的唯一标签。
func (dsc *DaemonSetsController) constructHistory(ctx context.Context, ds *apps.DaemonSet) (cur *apps.ControllerRevision, old []*apps.ControllerRevision, err error) {
	var histories []*apps.ControllerRevision
	var currentHistories []*apps.ControllerRevision
	histories, err = dsc.controlledHistories(ctx, ds)
	if err != nil {
		return nil, nil, err
	}
	for _, history := range histories {
		// 历史缺少唯一标签时补上；使用历史名称而非计算哈希，避免哈希冲突的顾虑。
		if _, ok := history.Labels[apps.DefaultDaemonSetUniqueLabelKey]; !ok {
			toUpdate := history.DeepCopy()
			toUpdate.Labels[apps.DefaultDaemonSetUniqueLabelKey] = toUpdate.Name
			history, err = dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Update(ctx, toUpdate, metav1.UpdateOptions{})
			if err != nil {
				return nil, nil, err
			}
		}
		// 将历史与当前 DaemonSet 模板比较，区分当前版本与旧版本。
		found := false
		found, err = Match(ds, history)
		if err != nil {
			return nil, nil, err
		}
		if found {
			currentHistories = append(currentHistories, history)
		} else {
			old = append(old, history)
		}
	}

	currRevision := maxRevision(old) + 1
	switch len(currentHistories) {
	case 0:
		// 没有匹配当前模板的历史时创建新快照。
		cur, err = dsc.snapshot(ctx, ds, currRevision)
		if err != nil {
			return nil, nil, err
		}
	default:
		cur, err = dsc.dedupCurHistories(ctx, ds, currentHistories)
		if err != nil {
			return nil, nil, err
		}
		// 当前历史修订号落后时推进到最新，避免后续清理误删。
		if cur.Revision < currRevision {
			toUpdate := cur.DeepCopy()
			toUpdate.Revision = currRevision
			_, err = dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Update(ctx, toUpdate, metav1.UpdateOptions{})
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return cur, old, err
}

// cleanupHistory 按修订历史限制清理旧 ControllerRevision。
func (dsc *DaemonSetsController) cleanupHistory(ctx context.Context, ds *apps.DaemonSet, old []*apps.ControllerRevision) error {
	// 统计历史时要包含已删除的终态 Pod，避免误删仍被引用的版本。
	nodesToDaemonPods, err := dsc.getNodesToDaemonPods(ctx, ds, true)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	toKeep := int(*ds.Spec.RevisionHistoryLimit)
	toKill := len(old) - toKeep
	if toKill <= 0 {
		return nil
	}

	// 收集仍在使用的哈希，只保留被存活 Pod 引用的历史。
	liveHashes := make(map[string]bool)
	for _, pods := range nodesToDaemonPods {
		for _, pod := range pods {
			if hash := pod.Labels[apps.DefaultDaemonSetUniqueLabelKey]; len(hash) > 0 {
				liveHashes[hash] = true
			}
		}
	}

	// 从最旧的版本开始清理，保证保留的是最近的修订。
	sort.Sort(historiesByRevision(old))
	for _, history := range old {
		if toKill <= 0 {
			break
		}
		if hash := history.Labels[apps.DefaultDaemonSetUniqueLabelKey]; liveHashes[hash] {
			continue
		}
		// 未被任何 Pod 引用的旧历史可以安全删除。
		err := dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Delete(ctx, history.Name, metav1.DeleteOptions{})
		if err != nil {
			return err
		}
		toKill--
	}
	return nil
}

// maxRevision 返回给定历史列表中的最大修订号。
func maxRevision(histories []*apps.ControllerRevision) int64 {
	max := int64(0)
	for _, history := range histories {
		if history.Revision > max {
			max = history.Revision
		}
	}
	return max
}

// dedupCurHistories 将多个内容相同的当前历史合并为一个：
// 保留修订号最大的版本，并把 Pod 标签与历史记录统一到该版本。
func (dsc *DaemonSetsController) dedupCurHistories(ctx context.Context, ds *apps.DaemonSet, curHistories []*apps.ControllerRevision) (*apps.ControllerRevision, error) {
	if len(curHistories) == 1 {
		return curHistories[0], nil
	}
	var maxRevision int64
	var keepCur *apps.ControllerRevision
	for _, cur := range curHistories {
		if cur.Revision >= maxRevision {
			keepCur = cur
			maxRevision = cur.Revision
		}
	}
	// 删除重复历史前先把 Pod 标签切到保留版本，否则引用会悬空。
	pods, err := dsc.getDaemonPods(ctx, ds)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		if pod.Labels[apps.DefaultDaemonSetUniqueLabelKey] != keepCur.Labels[apps.DefaultDaemonSetUniqueLabelKey] {
			patchRaw := map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						apps.DefaultDaemonSetUniqueLabelKey: keepCur.Labels[apps.DefaultDaemonSetUniqueLabelKey],
					},
				},
			}
			patchJSON, err := json.Marshal(patchRaw)
			if err != nil {
				return nil, err
			}
			_, err = dsc.kubeClient.CoreV1().Pods(ds.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patchJSON, metav1.PatchOptions{})
			if err != nil {
				return nil, err
			}
		}
	}
	// Pod 标签迁移完成后再删除重复历史。
	for _, cur := range curHistories {
		if cur.Name == keepCur.Name {
			continue
		}
		// 删除重复的历史版本。
		err = dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Delete(ctx, cur.Name, metav1.DeleteOptions{})
		if err != nil {
			return nil, err
		}
	}
	return keepCur, nil
}

// controlledHistories 返回 DaemonSet 控制的所有 ControllerRevision，
// 并通过收养/遗弃来协调 ControllerRef。
// 注意返回的是 informer 缓存中的对象指针，修改前必须先深拷贝。
func (dsc *DaemonSetsController) controlledHistories(ctx context.Context, ds *apps.DaemonSet) ([]*apps.ControllerRevision, error) {
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return nil, err
	}

	// 列出全部历史而非按标签筛选，以覆盖标签已不匹配但 ControllerRef 仍指向本控制器的对象。
	histories, err := dsc.historyLister.ControllerRevisions(ds.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	// 尝试收养对象前先做一次非缓存一致性读，确认 DaemonSet 未被删除（见 #42639），
	// 防止在列出 Pod 之后、收养之前对象已被移除。
	canAdoptFunc := controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace).Get(ctx, ds.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != ds.UID {
			return nil, fmt.Errorf("original DaemonSet %v/%v is gone: got uid %v, wanted %v", ds.Namespace, ds.Name, fresh.UID, ds.UID)
		}
		return fresh, nil
	})
	// 交由 ControllerRefManager 完成收养/遗弃，避免直接修改缓存对象。
	cm := controller.NewControllerRevisionControllerRefManager(dsc.crControl, ds, selector, controllerKind, canAdoptFunc)
	return cm.ClaimControllerRevisions(ctx, histories)
}

// Match 检查 DaemonSet 当前模板是否与给定历史中保存的模板一致。
func Match(ds *apps.DaemonSet, history *apps.ControllerRevision) (bool, error) {
	patch, err := getPatch(ds)
	if err != nil {
		return false, err
	}
	return bytes.Equal(patch, history.Data.Raw), nil
}

// getPatch 返回一个可用于将 DaemonSet 恢复到旧版本的 strategic merge patch。
// 错误为 nil 时 patch 有效。当前只保存 Pod 模板；今后可以增减保存的字段，
// 同时仍兼容此前记录的 patch。
func getPatch(ds *apps.DaemonSet) ([]byte, error) {
	dsBytes, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	err = json.Unmarshal(dsBytes, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})

	// 构造只替换 spec.template 的 patch，避免把无关字段写入历史。
	spec := raw["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	specCopy["template"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

// snapshot 为当前 DaemonSet 模板创建一个带唯一名称的 ControllerRevision。
// 名称冲突时先复用内容一致的历史，否则递增 collisionCount 后重试。
func (dsc *DaemonSetsController) snapshot(ctx context.Context, ds *apps.DaemonSet, revision int64) (*apps.ControllerRevision, error) {
	patch, err := getPatch(ds)
	if err != nil {
		return nil, err
	}
	hash := controller.ComputeHash(&ds.Spec.Template, ds.Status.CollisionCount)
	name := ds.Name + "-" + hash
	history := &apps.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ds.Namespace,
			Labels:          labelsutil.CloneAndAddLabel(ds.Spec.Template.Labels, apps.DefaultDaemonSetUniqueLabelKey, hash),
			Annotations:     ds.Annotations,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ds, controllerKind)},
		},
		Data:     runtime.RawExtension{Raw: patch},
		Revision: revision,
	}

	history, err = dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Create(ctx, history, metav1.CreateOptions{})
	if outerErr := err; errors.IsAlreadyExists(outerErr) {
		logger := klog.FromContext(ctx)
		// TODO: 这里是否可以直接从 historyLister 读取？
		existedHistory, getErr := dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return nil, getErr
		}
		// 名称冲突可能是先前重试留下的相同历史，先确认内容是否一致。
		done, matchErr := Match(ds, existedHistory)
		if matchErr != nil {
			return nil, matchErr
		}
		if done {
			return existedHistory, nil
		}

		// 内容不同则是真正的哈希冲突；从 API 读取最新对象，确保只在必要时递增 collisionCount。
		currDS, getErr := dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace).Get(ctx, ds.Name, metav1.GetOptions{})
		if getErr != nil {
			return nil, getErr
		}
		// 若计算哈希时用的 collisionCount 已过期，直接重试，避免无谓地推进计数。
		if !reflect.DeepEqual(currDS.Status.CollisionCount, ds.Status.CollisionCount) {
			return nil, fmt.Errorf("found a stale collision count (%d, expected %d) of DaemonSet %q while processing; will retry until it is updated", ptr.Deref(ds.Status.CollisionCount, 0), ptr.Deref(currDS.Status.CollisionCount, 0), ds.Name)
		}
		if currDS.Status.CollisionCount == nil {
			currDS.Status.CollisionCount = new(int32)
		}
		*currDS.Status.CollisionCount++
		_, updateErr := dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace).UpdateStatus(ctx, currDS, metav1.UpdateOptions{})
		if updateErr != nil {
			return nil, updateErr
		}
		logger.V(2).Info("Found a hash collision for DaemonSet - bumping collisionCount to resolve it", "daemonset", klog.KObj(ds), "collisionCount", *currDS.Status.CollisionCount)
		return nil, outerErr
	}
	return history, err
}

// updatedDesiredNodeCounts 计算实际允许的 surge、不可用与目标调度 Pod 数，
// 并为每个未调度节点在 nodeToDaemonPods 中补上空切片，便于后续按节点遍历。
func (dsc *DaemonSetsController) updatedDesiredNodeCounts(ctx context.Context, ds *apps.DaemonSet, nodeList []*v1.Node, nodeToDaemonPods map[string][]*v1.Pod) (int, int, int, error) {
	var desiredNumberScheduled int
	logger := klog.FromContext(ctx)
	tolerations := daemonPodTolerations(ds)
	requiredNodeAffinity := nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity)
	for i := range nodeList {
		node := nodeList[i]
		wantToRun, _ := nodeShouldRunDaemonPod(logger, node, ds, tolerations, requiredNodeAffinity)
		if !wantToRun {
			continue
		}
		desiredNumberScheduled++

		if _, exists := nodeToDaemonPods[node.Name]; !exists {
			nodeToDaemonPods[node.Name] = nil
		}
	}

	maxUnavailable, err := util.UnavailableCount(ds, desiredNumberScheduled)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("invalid value for MaxUnavailable: %v", err)
	}

	maxSurge, err := util.SurgeCount(ds, desiredNumberScheduled)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("invalid value for MaxSurge: %v", err)
	}

	// apiserver 可能返回 surge 与 unavailable 同时为 0 的无效配置，
	// 此时回退到 unavailable=1，保证更新能够推进。
	if desiredNumberScheduled > 0 && maxUnavailable == 0 && maxSurge == 0 {
		logger.Info("DaemonSet is not configured for surge or unavailability, defaulting to accepting unavailability", "daemonset", klog.KObj(ds))
		maxUnavailable = 1
	}
	logger.V(5).Info("DaemonSet with maxSurge and maxUnavailable", "daemonset", klog.KObj(ds), "maxSurge", maxSurge, "maxUnavailable", maxUnavailable)
	return maxSurge, maxUnavailable, desiredNumberScheduled, nil
}

// historiesByRevision 按修订号升序排列历史，供清理时优先淘汰旧版本。
type historiesByRevision []*apps.ControllerRevision

// Len 返回历史数量。
func (h historiesByRevision) Len() int { return len(h) }

// Swap 交换两个历史的位置。
func (h historiesByRevision) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Less 按修订号比较历史。
func (h historiesByRevision) Less(i, j int) bool {
	return h[i].Revision < h[j].Revision
}
