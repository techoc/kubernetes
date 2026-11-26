// Copyright ©2015 The gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package traverse 提供基础的图遍历原语。
package traverse

import (
	"golang.org/x/tools/container/intsets"

	"k8s.io/kubernetes/third_party/forked/gonum/graph"
	"k8s.io/kubernetes/third_party/forked/gonum/graph/internal/linear"
)

// BreadthFirst 实现带状态的广度优先遍历。
type BreadthFirst struct {
	EdgeFilter func(graph.Edge) bool
	Visit      func(u, v graph.Node)
	queue      linear.NodeQueue
	visited    *intsets.Sparse
}

// Walk 从给定节点开始对图 g 做广度优先遍历。若 EdgeFilter 非 nil，只沿其返回 true 的边
// 前进；若 until 非 nil，遇到第一个使其返回 true 的节点即停止并返回。遍历过程中若 Visit
// 非 nil，则对每条经过的边调用它，传入边连接的两个节点。
func (b *BreadthFirst) Walk(g graph.Graph, from graph.Node, until func(n graph.Node, d int) bool) graph.Node {
	if b.visited == nil {
		b.visited = &intsets.Sparse{}
	}
	b.queue.Enqueue(from)
	b.visited.Insert(from.ID())

	var (
		depth     int
		children  int
		untilNext = 1
	)
	for b.queue.Len() > 0 {
		t := b.queue.Dequeue()
		if until != nil && until(t, depth) {
			return t
		}
		for _, n := range g.From(t) {
			if b.EdgeFilter != nil && !b.EdgeFilter(g.Edge(t, n)) {
				continue
			}
			if b.visited.Has(n.ID()) {
				continue
			}
			if b.Visit != nil {
				b.Visit(t, n)
			}
			b.visited.Insert(n.ID())
			children++
			b.queue.Enqueue(n)
		}
		if untilNext--; untilNext == 0 {
			depth++
			untilNext = children
			children = 0
		}
	}

	return nil
}

// WalkAll 对图中每个未访问节点调用 Walk，且忽略边的方向。before 和 after 分别在每次
// 遍历开始前和结束后调用（非 nil 时）；during 在遍历到每个节点时调用。
func (b *BreadthFirst) WalkAll(g graph.Undirected, before, after func(), during func(graph.Node)) {
	b.Reset()
	for _, from := range g.Nodes() {
		if b.Visited(from) {
			continue
		}
		if before != nil {
			before()
		}
		b.Walk(g, from, func(n graph.Node, _ int) bool {
			if during != nil {
				during(n)
			}
			return false
		})
		if after != nil {
			after()
		}
	}
}

// Visited 返回节点 n 在本次遍历中是否被访问过。
func (b *BreadthFirst) Visited(n graph.Node) bool {
	return b.visited != nil && b.visited.Has(n.ID())
}

// Reset 重置遍历器状态以便复用。
func (b *BreadthFirst) Reset() {
	b.queue.Reset()
	if b.visited != nil {
		b.visited.Clear()
	}
}

// DepthFirst 实现带状态的深度优先遍历。
type DepthFirst struct {
	EdgeFilter func(graph.Edge) bool
	Visit      func(u, v graph.Node)
	stack      linear.NodeStack
	visited    *intsets.Sparse
}

// Walk 从给定节点开始对图 g 做深度优先遍历。若 EdgeFilter 非 nil，只沿其返回 true 的边
// 前进；若 until 非 nil，遇到第一个使其返回 true 的节点即停止并返回。遍历过程中若 Visit
// 非 nil，则对每条经过的边调用它，传入边连接的两个节点。
func (d *DepthFirst) Walk(g graph.Graph, from graph.Node, until func(graph.Node) bool) graph.Node {
	if d.visited == nil {
		d.visited = &intsets.Sparse{}
	}
	d.stack.Push(from)
	d.visited.Insert(from.ID())

	for d.stack.Len() > 0 {
		t := d.stack.Pop()
		if until != nil && until(t) {
			return t
		}
		for _, n := range g.From(t) {
			if d.EdgeFilter != nil && !d.EdgeFilter(g.Edge(t, n)) {
				continue
			}
			if d.visited.Has(n.ID()) {
				continue
			}
			if d.Visit != nil {
				d.Visit(t, n)
			}
			d.visited.Insert(n.ID())
			d.stack.Push(n)
		}
	}

	return nil
}

// WalkAll 对图中每个未访问节点调用 Walk，且忽略边的方向。before 和 after 分别在每次
// 遍历开始前和结束后调用（非 nil 时）；during 在遍历到每个节点时调用。
func (d *DepthFirst) WalkAll(g graph.Undirected, before, after func(), during func(graph.Node)) {
	d.Reset()
	for _, from := range g.Nodes() {
		if d.Visited(from) {
			continue
		}
		if before != nil {
			before()
		}
		d.Walk(g, from, func(n graph.Node) bool {
			if during != nil {
				during(n)
			}
			return false
		})
		if after != nil {
			after()
		}
	}
}

// Visited 返回节点 n 在本次遍历中是否被访问过。
func (d *DepthFirst) Visited(n graph.Node) bool {
	return d.visited != nil && d.visited.Has(n.ID())
}

// Reset 重置遍历器状态以便复用。
func (d *DepthFirst) Reset() {
	d.stack = d.stack[:0]
	if d.visited != nil {
		d.visited.Clear()
	}
}
