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

// Package debugger 提供调度器缓存与调度队列的诊断输出。
//
// 当调度结果异常时，可以把缓存中的节点信息和待调度 Pod 写入日志，
// 用于还原当时的调度现场。
package debugger

import (
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// CacheDumper 把调度器缓存和调度队列中的部分信息写入日志，便于调试。
type CacheDumper struct {
	cache    internalcache.Cache
	podQueue queue.SchedulingQueue
}

// DumpAll 把缓存的节点信息和调度队列信息写入调度器日志。
func (d *CacheDumper) DumpAll(logger klog.Logger) {
	d.dumpNodes(logger)
	d.dumpSchedulingQueue(logger)
}

// dumpNodes 把 NodeInfo 写入调度器日志。
func (d *CacheDumper) dumpNodes(logger klog.Logger) {
	dump := d.cache.Dump()
	nodeInfos := make([]string, 0, len(dump.Nodes))
	for name, nodeInfo := range dump.Nodes {
		nodeInfos = append(nodeInfos, d.printNodeInfo(name, nodeInfo))
	}
	// 节点条目之间加空行，避免日志连成一片难以阅读。
	logger.Info("Dump of cached NodeInfo", "nodes", strings.Join(nodeInfos, "\n\n"))
}

// dumpSchedulingQueue 把调度队列中的 Pod 写入调度器日志。
func (d *CacheDumper) dumpSchedulingQueue(logger klog.Logger) {
	pendingPods, s := d.podQueue.PendingPods()
	var podData strings.Builder
	for _, p := range pendingPods {
		podData.WriteString(printPod(p))
	}
	logger.Info("Dump of scheduling queue", "summary", s, "pods", podData.String())
}

// printNodeInfo 把 NodeInfo 的关键部分格式化为字符串。
func (d *CacheDumper) printNodeInfo(name string, n *framework.NodeInfo) string {
	var nodeData strings.Builder
	nodeData.WriteString(fmt.Sprintf("Node name: %s\nDeleted: %t\nRequested Resources: %+v\nAllocatable Resources:%+v\nScheduled Pods(number: %v):\n",
		name, n.Node() == nil, n.Requested, n.Allocatable, len(n.Pods)))
	// 输出该节点上已绑定的 Pod。
	for _, p := range n.Pods {
		nodeData.WriteString(printPod(p.GetPod()))
	}
	// 输出该节点上被提名的 Pod：它们尚未绑定，但会影响后续调度决策。
	nominatedPodInfos := d.podQueue.NominatedPodsForNode(name)
	if len(nominatedPodInfos) != 0 {
		nodeData.WriteString(fmt.Sprintf("Nominated Pods(number: %v):\n", len(nominatedPodInfos)))
		for _, pi := range nominatedPodInfos {
			nodeData.WriteString(printPod(pi.GetPod()))
		}
	}
	return nodeData.String()
}

// printPod 把 Pod 的关键字段格式化为一行字符串。
func printPod(p *v1.Pod) string {
	return fmt.Sprintf("name: %v, namespace: %v, uid: %v, phase: %v, nominated node: %v\n", p.Name, p.Namespace, p.UID, p.Status.Phase, p.Status.NominatedNodeName)
}
