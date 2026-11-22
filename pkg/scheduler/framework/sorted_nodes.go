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

package framework

import (
	"container/heap"

	fwk "k8s.io/kube-scheduler/framework"
)

var _ SortedScoredNodes = (*sortedNodeScores)(nil)

// sortedNodeScores 是基于最大堆实现的已评分节点列表。
type sortedNodeScores struct {
	nodes nodeScoreHeap
}

// NewSortedScoredNodes 创建一个以 TotalScore 为键的最大堆支持的 SortedScoredNodes。
// 同分时再按 Randomizer 排序，避免固定偏向某些节点，让调度结果有随机性。
func NewSortedScoredNodes(nodeScoreList []fwk.NodePluginScores) SortedScoredNodes {
	var h nodeScoreHeap = nodeScoreList
	heap.Init(&h)
	return &sortedNodeScores{nodes: h}
}

// Pop 弹出并当前得分最高的节点。
func (s *sortedNodeScores) Pop() fwk.NodePluginScores {
	return heap.Pop(&s.nodes).(fwk.NodePluginScores)
}

// Len 返回堆中剩余的节点数。
func (s *sortedNodeScores) Len() int {
	return s.nodes.Len()
}

// UnorderedList 以堆内部顺序返回所有节点（未按得分排序）。
// 仅复制外层切片，每个 NodePluginScores.Scores 切片仍与堆共享。
// 调用方不得修改返回的元素，否则会破坏堆的不变式。
func (s *sortedNodeScores) UnorderedList() []fwk.NodePluginScores {
	result := make([]fwk.NodePluginScores, len(s.nodes))
	copy(result, s.nodes)
	return result
}

// nodeScoreHeap 是 fwk.NodePluginScores 的最大堆。
// 排序先比较 TotalScore，同分再用 Randomizer 打破平局，
// 防止得分相同的节点总是以固定顺序被选中。
type nodeScoreHeap []fwk.NodePluginScores

var _ heap.Interface = &nodeScoreHeap{}

func (h nodeScoreHeap) Len() int { return len(h) }
func (h nodeScoreHeap) Less(i, j int) bool {
	return (h[i].TotalScore > h[j].TotalScore ||
		(h[i].TotalScore == h[j].TotalScore && h[i].Randomizer > h[j].Randomizer))
}
func (h nodeScoreHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *nodeScoreHeap) Push(x interface{}) {
	*h = append(*h, x.(fwk.NodePluginScores))
}

func (h *nodeScoreHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
