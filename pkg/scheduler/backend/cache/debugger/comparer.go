/*
Copyright 2018 The Kubernetes Authors.

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

package debugger

import (
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// CacheComparer 比较 informer 中的真实状态与调度器缓存快照，用于排查缓存不一致。
type CacheComparer struct {
	NodeLister corelisters.NodeLister
	PodLister  corelisters.PodLister
	Cache      internalcache.Cache
	PodQueue   internalqueue.SchedulingQueue
}

// Compare 比较 NodeLister/PodLister 中的节点和 Pod 与缓存快照的差异。
func (c *CacheComparer) Compare(logger klog.Logger) error {
	logger.V(3).Info("Cache comparer started")
	defer logger.V(3).Info("Cache comparer finished")

	nodes, err := c.NodeLister.List(labels.Everything())
	if err != nil {
		return err
	}

	pods, err := c.PodLister.List(labels.Everything())
	if err != nil {
		return err
	}

	// 快照是缓存某一时刻的视图，之后对它做比对不会干扰调度器运行。
	dump := c.Cache.Dump()

	// Pod 可能在队列中尚未被调度，不能算作缓存缺失；这里忽略错误仅用于调试。
	pendingPods, _ := c.PodQueue.PendingPods()

	if missed, redundant := c.CompareNodes(nodes, dump.Nodes); len(missed)+len(redundant) != 0 {
		logger.Info("Cache mismatch", "missedNodes", missed, "redundantNodes", redundant)
	}

	if missed, redundant := c.ComparePods(pods, pendingPods, dump.Nodes); len(missed)+len(redundant) != 0 {
		logger.Info("Cache mismatch", "missedPods", missed, "redundantPods", redundant)
	}

	return nil
}

// CompareNodes 比较真实节点与缓存中的节点，返回缺失和多余的名字。
func (c *CacheComparer) CompareNodes(nodes []*v1.Node, nodeinfos map[string]*framework.NodeInfo) (missed, redundant []string) {
	actual := []string{}
	for _, node := range nodes {
		actual = append(actual, node.Name)
	}

	cached := []string{}
	for nodeName := range nodeinfos {
		cached = append(cached, nodeName)
	}

	return compareStrings(actual, cached)
}

// ComparePods 比较真实 Pod 与缓存中的 Pod；等待队列中的 Pod 视为已缓存。
func (c *CacheComparer) ComparePods(pods, waitingPods []*v1.Pod, nodeinfos map[string]*framework.NodeInfo) (missed, redundant []string) {
	actual := []string{}
	for _, pod := range pods {
		actual = append(actual, string(pod.UID))
	}

	cached := []string{}
	for _, nodeinfo := range nodeinfos {
		for _, p := range nodeinfo.Pods {
			cached = append(cached, string(p.GetPod().UID))
		}
	}
	for _, pod := range waitingPods {
		cached = append(cached, string(pod.UID))
	}

	return compareStrings(actual, cached)
}

// compareStrings 对两个有序字符串序列做差集，actual 中缺失的记入 missed，cached 中多出的记入 redundant。
func compareStrings(actual, cached []string) (missed, redundant []string) {
	missed, redundant = []string{}, []string{}

	sort.Strings(actual)
	sort.Strings(cached)

	compare := func(i, j int) int {
		if i == len(actual) {
			return 1
		} else if j == len(cached) {
			return -1
		}
		return strings.Compare(actual[i], cached[j])
	}

	for i, j := 0, 0; i < len(actual) || j < len(cached); {
		switch compare(i, j) {
		case 0:
			i++
			j++
		case -1:
			missed = append(missed, actual[i])
			i++
		case 1:
			redundant = append(redundant, cached[j])
			j++
		}
	}

	return
}
