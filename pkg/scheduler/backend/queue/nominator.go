/*
Copyright 2024 The Kubernetes Authors.

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
	"slices"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

// nominator 保存被提名（nominated）到各节点上运行的 Pod。
// 之所以单独维护，是因为这里保存的 Pod 的 nominatedNodeName 可能与调度器本地看到的不同；
// 我们需要能按 UID 找到这些 Pod 并更新或删除它们。
type nominator struct {
	// nLock 保护 nominator 的所有操作，不得用于其他用途。
	// 注意：拿到 nLock 之后不要再拿 "SchedulingQueue.lock"、"activeQueue.lock" 或 "backoffQueue.lock"，
	// 必须先拿后三者，否则可能死锁。
	// 正确的加锁顺序是：SchedulingQueue.lock > activeQueue.lock = backoffQueue.lock > nLock。
	nLock sync.RWMutex

	// podLister 用于确认给定 Pod 是否仍然存在、且尚未被调度。
	podLister listersv1.PodLister
	// nominatedPods 按节点名索引，值为被提名到该节点运行的 Pod 列表；
	// 这些 Pod 可能位于 activeQ 或 unschedulableEntities 中。
	nominatedPods map[string][]podRef
	// nominatedPodToNode 按 Pod UID 索引，值为该 Pod 被提名到的节点名。
	nominatedPodToNode map[types.UID]string
}

// newPodNominator 创建一个 nominator，podLister 可为 nil（此时不校验 Pod 存活状态）。
func newPodNominator(podLister listersv1.PodLister) *nominator {
	return &nominator{
		podLister:          podLister,
		nominatedPods:      make(map[string][]podRef),
		nominatedPodToNode: make(map[types.UID]string),
	}
}

// addNominatedPod 把 Pod 加入给定节点的提名列表。
// 在抢占流程中，为 Pod 选定节点后会调用它。我们先更新内存中的提名结构，
// 再发送更新 Pod 对象的 API 请求，以免与后续调度周期产生竞态。
func (npm *nominator) addNominatedPod(logger klog.Logger, pi fwk.PodInfo, nominatingInfo *fwk.NominatingInfo) {
	npm.nLock.Lock()
	npm.addNominatedPodUnlocked(logger, pi, nominatingInfo)
	npm.nLock.Unlock()
}

// addNominatedPodUnlocked 是 addNominatedPod 的无锁版本，调用方必须持有 nLock。
func (npm *nominator) addNominatedPodUnlocked(logger klog.Logger, pi fwk.PodInfo, nominatingInfo *fwk.NominatingInfo) {
	// 无论 Pod 是否已存在都先删除旧记录，保证同一 Pod 在结构中只有一份。
	npm.deleteUnlocked(pi.GetPod())

	var nodeName string
	if nominatingInfo.Mode() == fwk.ModeOverride {
		nodeName = nominatingInfo.NominatedNodeName
	} else if nominatingInfo.Mode() == fwk.ModeNoop {
		nodeName = pi.GetPod().Status.NominatedNodeName
	}

	// 节点名为空意味着清除提名；不要用 "" 作为 nominatedPods 的键。
	if nodeName == "" {
		return
	}

	if npm.podLister != nil {
		// Pod 已被删除或已经绑定到节点时，不再给它登记提名。
		updatedPod, err := npm.podLister.Pods(pi.GetPod().Namespace).Get(pi.GetPod().Name)
		if err != nil {
			logger.V(4).Info("Pod doesn't exist in podLister, aborted adding it to the nominator", "pod", klog.KObj(pi.GetPod()))
			return
		}
		if updatedPod.Spec.NodeName != "" {
			logger.V(4).Info("Pod is already scheduled to a node, aborted adding it to the nominator", "pod", klog.KObj(pi.GetPod()), "node", updatedPod.Spec.NodeName)
			return
		}
	}

	npm.nominatedPodToNode[pi.GetPod().UID] = nodeName
	for _, np := range npm.nominatedPods[nodeName] {
		if np.uid == pi.GetPod().UID {
			logger.V(4).Info("Pod already exists in the nominator", "pod", np.uid)
			return
		}
	}
	npm.nominatedPods[nodeName] = append(npm.nominatedPods[nodeName], podToRef(pi.GetPod()))
}

// UpdateNominatedPod 用 newPodInfo 替换 oldPod 对应的提名记录。
func (npm *nominator) UpdateNominatedPod(logger klog.Logger, oldPod *v1.Pod, newPodInfo fwk.PodInfo) {
	npm.nLock.Lock()
	defer npm.nLock.Unlock()
	// 有时在内存中为 Pod 预留了 NominatedNode 之后，紧接着收到一个不带 NominatedNode 的 Update 事件。
	// 此时更新 Pod 指针时要继续保留这个预留节点，否则提名会意外丢失。
	var nominatingInfo *fwk.NominatingInfo
	// 以下情况不会进入这个 if 分支：
	// (1) Update 事件新增了 NominatedNode 信息
	// (2) Update 事件更新了 NominatedNode 信息
	// (3) Update 事件删除了 NominatedNode 信息
	if nominatedNodeName(oldPod) == "" && nominatedNodeName(newPodInfo.GetPod()) == "" {
		if nnn, ok := npm.nominatedPodToNode[oldPod.UID]; ok {
			// 只有这种情况才需要继续保留 NominatedNode
			nominatingInfo = &fwk.NominatingInfo{
				NominatingMode:    fwk.ModeOverride,
				NominatedNodeName: nnn,
			}
		}
	}
	// 无论 nominatedNodeName 是否变化都要更新，以确保 Pod 指针被刷新。
	npm.deleteUnlocked(oldPod)
	npm.addNominatedPodUnlocked(logger, newPodInfo, nominatingInfo)
}

// DeleteNominatedPodIfExists 从 nominatedPods 中删除 pod（若存在）。
func (npm *nominator) DeleteNominatedPodIfExists(pod *v1.Pod) {
	npm.nLock.Lock()
	npm.deleteUnlocked(pod)
	npm.nLock.Unlock()
}

// deleteUnlocked 是删除提名记录的无锁版本，调用方必须持有 nLock。
func (npm *nominator) deleteUnlocked(p *v1.Pod) {
	nnn, ok := npm.nominatedPodToNode[p.UID]
	if !ok {
		return
	}
	for i, np := range npm.nominatedPods[nnn] {
		if np.uid == p.UID {
			npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn][:i], npm.nominatedPods[nnn][i+1:]...)
			if len(npm.nominatedPods[nnn]) == 0 {
				delete(npm.nominatedPods, nnn)
			}
			break
		}
	}
	delete(npm.nominatedPodToNode, p.UID)
}

// nominatedPodsForNode 返回被提名到 nodeName 的 Pod 引用副本。
// 返回副本而不是内部切片，避免调用方修改到 nominator 的内部状态。
func (npm *nominator) nominatedPodsForNode(nodeName string) []podRef {
	npm.nLock.RLock()
	defer npm.nLock.RUnlock()
	return slices.Clone(npm.nominatedPods[nodeName])
}

// nominatedNodeName 返回 Pod 状态中的提名节点名。
func nominatedNodeName(pod *v1.Pod) string {
	return pod.Status.NominatedNodeName
}

// podRef 只保存提名逻辑需要的 Pod 字段，避免持有整个 Pod 指针导致状态过期。
type podRef struct {
	name            string
	namespace       string
	uid             types.UID
	schedulingGroup *v1.PodSchedulingGroup
}

// podToRef 从 Pod 提取出提名所需的最小字段集合。
func podToRef(pod *v1.Pod) podRef {
	return podRef{
		name:            pod.Name,
		namespace:       pod.Namespace,
		uid:             pod.UID,
		schedulingGroup: pod.Spec.SchedulingGroup,
	}
}

// toPod 把 podRef 还原成一个最小化的 Pod 对象，供过滤插件使用。
func (np podRef) toPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      np.name,
			Namespace: np.namespace,
			UID:       np.uid,
		},
		Spec: v1.PodSpec{
			SchedulingGroup: np.schedulingGroup,
		},
	}
}
