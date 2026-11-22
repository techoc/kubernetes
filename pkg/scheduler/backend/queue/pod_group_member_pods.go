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
	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// podGroupMemberPods 记录各 PodGroup 的成员 Pod。
// 该结构不是线程安全的，只能在持有 PriorityQueue 锁的情况下访问。
type podGroupMemberPods struct {
	// podGroupToPodInfos 先按 PodGroup key（pg-type/namespace/pg-name）索引 QueuedPodInfo，
	// 再按 Pod key（namespace/pg-name）索引。
	podGroupToPodInfos map[fwk.EntityKey]map[fwk.EntityKey]*framework.QueuedPodInfo
	// podsRecorder 记录当前成员 Pod 总数的 gauge 指标。
	podsRecorder metrics.GaugeMetric
}

// newPodGroupMemberPods 创建空的成员 Pod 记录，并绑定 gauge 指标记录器。
func newPodGroupMemberPods(podsRecorder metrics.GaugeMetric) *podGroupMemberPods {
	return &podGroupMemberPods{
		podGroupToPodInfos: make(map[fwk.EntityKey]map[fwk.EntityKey]*framework.QueuedPodInfo),
		podsRecorder:       podsRecorder,
	}
}

// add 记录一个 Pod 信息。
// 同时递增 gauge 指标。
func (p *podGroupMemberPods) add(pInfo *framework.QueuedPodInfo) {
	pgKey, pKey := podGroupKeyForPod(pInfo.Pod), podKey(pInfo.Pod)
	if p.podGroupToPodInfos[pgKey] == nil {
		p.podGroupToPodInfos[pgKey] = make(map[fwk.EntityKey]*framework.QueuedPodInfo)
	}
	p.podGroupToPodInfos[pgKey][pKey] = pInfo
	p.podsRecorder.Inc()
}

// has 判断给定 Pod 是否已被记录。
func (p *podGroupMemberPods) has(podLookup *v1.Pod) bool {
	pgKey, pKey := podGroupKeyForPod(podLookup), podKey(podLookup)
	return p.podGroupToPodInfos[pgKey][pKey] != nil
}

// len 返回被记录的所有 Pod 的总数。
func (p *podGroupMemberPods) len() int {
	l := 0
	for _, pods := range p.podGroupToPodInfos {
		l += len(pods)
	}
	return l
}

// get 返回给定 Pod 对应的排队信息。
func (p *podGroupMemberPods) get(podLookup *v1.Pod) *framework.QueuedPodInfo {
	pgKey, pKey := podGroupKeyForPod(podLookup), podKey(podLookup)
	return p.podGroupToPodInfos[pgKey][pKey]
}

// update 用新 Pod 对象刷新已有记录；找到则刷新后的排队信息，否则返回 nil。
func (p *podGroupMemberPods) update(newPod *v1.Pod) *framework.QueuedPodInfo {
	pgKey, pKey := podGroupKeyForPod(newPod), podKey(newPod)
	if pInfo, ok := p.podGroupToPodInfos[pgKey][pKey]; ok {
		pInfo.Pod = newPod
		return pInfo
	}
	return nil
}

// delete 删除指定 Pod；找到则返回其排队信息，否则返回 nil。
// 同时递减 gauge 指标；组内没有成员后一并清理该组的映射，避免留下空 map。
func (p *podGroupMemberPods) delete(podLookup *v1.Pod) *framework.QueuedPodInfo {
	pgKey, pKey := podGroupKeyForPod(podLookup), podKey(podLookup)
	if pInfo, ok := p.podGroupToPodInfos[pgKey][pKey]; ok {
		delete(p.podGroupToPodInfos[pgKey], pKey)
		if len(p.podGroupToPodInfos[pgKey]) == 0 {
			delete(p.podGroupToPodInfos, pgKey)
		}
		p.podsRecorder.Dec()
		return pInfo
	}
	return nil
}

// list 返回所有被记录的 Pod。
func (p *podGroupMemberPods) list() []*v1.Pod {
	var pods []*v1.Pod
	for _, pgPods := range p.podGroupToPodInfos {
		for _, pInfo := range pgPods {
			pods = append(pods, pInfo.Pod)
		}
	}
	return pods
}

// clear 删除并返回指定 PodGroup（按 namespace 和 name 定位）的全部排队信息。
// 同时为被清理的每个 Pod 递减 gauge 指标。
func (p *podGroupMemberPods) clear(namespace, name string) []*framework.QueuedPodInfo {
	pgKey := fwk.PodGroupKey(namespace, name)
	if pInfos, ok := p.podGroupToPodInfos[pgKey]; ok {
		delete(p.podGroupToPodInfos, pgKey)
		var pInfoList []*framework.QueuedPodInfo
		for _, pInfo := range pInfos {
			pInfoList = append(pInfoList, pInfo)
			p.podsRecorder.Dec()
		}
		return pInfoList
	}
	return nil
}
