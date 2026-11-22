/*
Copyright 2019 The Kubernetes Authors.

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

// Package testing 提供构造调度测试所需 Pod、Node 等对象的辅助函数。
package testing

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

// keyVal 保存一个待打到 Pod 上的标签键值对。
// 用切片而非 map 遍历标签，保证打标签的顺序可复现。
type keyVal struct {
	k string
	v string
}

// MakeNodesAndPodsForEvenPodsSpread 为 EvenPodsSpread 特性提供测试辅助。
// 它构造一个由运行中的 Pod 和 Node 组成的假集群，两者数量由入参决定。
// Pod 和 Node 的规格按以下规则生成：
//   - 每个 Node 带唯一标签："node: node<i>"。
//   - 每个 Node 带轮转标签："zone: zone[0-9]"。
//   - 根据传入的标签，每个 Pod 轮流被打上
//     "key1"、"key1,key2"、...、"key1,key2,...,keyN" 的标签。
func MakeNodesAndPodsForEvenPodsSpread(labels map[string]string, existingPodsNum, allNodesNum, filteredNodesNum int) (existingPods []*v1.Pod, allNodes []*v1.Node, filteredNodes []*v1.Node) {
	var labelPairs []keyVal
	for k, v := range labels {
		labelPairs = append(labelPairs, keyVal{k: k, v: v})
	}
	// 固定 10 个 zone，使同一测试内的 Node 分布可预期。
	zones := 10
	// 构造 Node。
	for i := 0; i < allNodesNum; i++ {
		node := MakeNode().Name(fmt.Sprintf("node%d", i)).
			Label(v1.LabelTopologyZone, fmt.Sprintf("zone%d", i%zones)).
			Label(v1.LabelHostname, fmt.Sprintf("node%d", i)).Obj()
		allNodes = append(allNodes, node)
	}
	// 过滤结果取前 filteredNodesNum 个 Node，模拟 Node 被过滤掉的场景。
	filteredNodes = allNodes[:filteredNodesNum]
	// 构造 Pod。
	for i := 0; i < existingPodsNum; i++ {
		podWrapper := MakePod().Name(fmt.Sprintf("pod%d", i)).Node(fmt.Sprintf("node%d", i%allNodesNum))
		// 依次打上 labels[0]、labels[0,1]、...、labels[all]，制造不同的标签匹配数。
		for _, p := range labelPairs[:i%len(labelPairs)+1] {
			podWrapper = podWrapper.Label(p.k, p.v)
		}
		existingPods = append(existingPods, podWrapper.Obj())
	}
	return
}
