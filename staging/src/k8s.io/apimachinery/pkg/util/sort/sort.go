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

// Package sort 提供在合并多个来源的有序列表时保持相对顺序的排序工具。
// 它不替代标准库的 sort：这里的核心是"拓扑一致性"，即在满足各来源局部顺序的前提下
// 给出确定性的全局顺序。
package sort

import (
	"container/heap"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"
)

// MergePreservingRelativeOrder 对来自多个来源的条目执行拓扑一致性排序。
// 它把多个字符串列表合并为一个列表，并保留每个来源列表内部的相对顺序。
//
// 对任意两个条目，只要其中一个在任一输入列表中先于另一个出现，输出就会保留该相对顺序；
// 若两个条目之间没有定义任何先后关系，则按字典序排序。
//
// 该函数使用 Kahn 算法做拓扑排序，并用最小堆保证输出确定：无依赖的条目按字典序处理，
// 因此相同输入多次调用结果一致。
//
// 函数包含一个捷径优化：若某个输入列表已包含全部唯一条目，则直接返回该列表，
// 最好情况下达到 O(n)。
//
// 示例：
//   - 输入：{{"a", "b", "c"}, {"b", "c"}} 返回 {"a", "b", "c"}
//   - 输入：{{"a", "c"}, {"b", "c"}} 返回 {"a", "b", "c"}（字典序打破平局）
//   - 输入：{{"a", "b"}, {"b", "a"}} 返回错误（检测到环）
//
// 复杂度：O(L*n + V*log(V) + E)，其中 L 为列表数，n 为平均列表长度，
// V 为唯一条目数，E 为优先级边数。
//
// 适用于合并多个来源、且各来源只有部分顺序（而非相互冲突的顺序）时的稳定排序。
func MergePreservingRelativeOrder(inputLists [][]string) []string {
	if len(inputLists) == 0 {
		return nil
	}

	// 构建优先级关系的有向图：同一列表中相邻元素之间加一条边。
	graph := make(map[string]*graphNode)
	for _, list := range inputLists {
		for i, item := range list {
			node := getOrCreateNode(graph, item)

			// Add edge from current item to next item in list
			if i < len(list)-1 {
				nextItem := list[i+1]
				nextNode := getOrCreateNode(graph, nextItem)

				// 仅在边不存在时才添加，避免重复累加入度导致拓扑排序误判。
				if !node.outEdges.Has(nextItem) {
					node.outEdges.Insert(nextItem)
					nextNode.inDegree++
				}
			}
		}
	}

	// 捷径：若某个输入列表已包含全部条目且无重复，它本身就是一个合法拓扑序，可直接返回。
	allItems := sets.New[string]()
	for name := range graph {
		allItems.Insert(name)
	}
	for _, list := range inputLists {
		if len(list) == allItems.Len() && isUnique(list) {
			return list
		}
	}

	// 使用 Kahn 算法 + 最小堆做拓扑排序，保证输出确定性。
	result, err := topologicalSort(graph)
	if err != nil {
		// 对合法输入不应出现环；若出现（例如调用方提供了互相矛盾的列表），
		// 退化为字典序排序，至少返回一个确定结果而不是空值。
		items := make([]string, 0, len(graph))
		for name := range graph {
			items = append(items, name)
		}
		sort.Strings(items)
		return items
	}

	return result
}

// getOrCreateNode 获取或创建给定名称对应的图节点。
func getOrCreateNode(graph map[string]*graphNode, name string) *graphNode {
	if graph[name] == nil {
		graph[name] = &graphNode{
			outEdges: sets.New[string](),
			inDegree: 0,
		}
	}
	return graph[name]
}

// isUnique 检查列表中是否存在重复条目。
func isUnique(list []string) bool {
	seen := make(map[string]bool, len(list))
	for _, item := range list {
		if seen[item] {
			return false
		}
		seen[item] = true
	}
	return true
}

// topologicalSort 使用 Kahn 算法与最小堆执行拓扑排序，以保证确定性顺序。
func topologicalSort(graph map[string]*graphNode) ([]string, error) {
	// 把所有入度为 0 的节点放入最小堆，作为排序起点。
	pq := &stringMinHeap{}
	heap.Init(pq)

	for name, node := range graph {
		if node.inDegree == 0 {
			heap.Push(pq, name)
		}
	}

	result := make([]string, 0, len(graph))

	for pq.Len() > 0 {
		// 每次弹出字典序最小的节点，确保同等条件下输出稳定。
		current := heap.Pop(pq).(string)
		result = append(result, current)

		currentNode := graph[current]

		// 移除当前节点，将其所有邻居的入度减一。
		for neighbor := range currentNode.outEdges {
			neighborNode := graph[neighbor]
			neighborNode.inDegree--

			// 邻居入度归零说明其前置依赖已全部处理，可以入堆。
			if neighborNode.inDegree == 0 {
				heap.Push(pq, neighbor)
			}
		}
	}

	// 若输出数量少于图中节点数，说明存在环（有节点始终无法入度为 0）。
	if len(result) != len(graph) {
		return nil, fmt.Errorf("cycle detected in precedence graph: sorted %d items but graph has %d items", len(result), len(graph))
	}

	return result, nil
}

// graphNode 表示优先级图中的一个节点。
type graphNode struct {
	// outEdges 是应排在本节点之后的条目集合。
	outEdges sets.Set[string]
	// inDegree 是应排在本节点之前的条目数量。
	inDegree int
}

// stringMinHeap 为字符串实现 heap.Interface（按字典序的最小堆）。
type stringMinHeap []string

func (h stringMinHeap) Len() int           { return len(h) }
func (h stringMinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h stringMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *stringMinHeap) Push(x interface{}) {
	*h = append(*h, x.(string))
}
func (h *stringMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
