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

// VisitableGraph 提供按邻接关系访问节点的能力。
type VisitableGraph interface {
	graph.Graph

	// VisitFrom 对从给定节点可直接到达的所有节点调用 visitor。
	// visitor 返回 false 时提前结束访问。
	VisitFrom(from graph.Node, visitor func(graph.Node) (shouldContinue bool))
}

// VisitingDepthFirst 在可访问图上实现带状态的深度优先遍历。
type VisitingDepthFirst struct {
	EdgeFilter func(graph.Edge) bool
	Visit      func(u, v graph.Node)
	stack      linear.NodeStack
	visited    *intsets.Sparse
}

// Walk 从给定节点开始对图 g 做深度优先遍历。若 EdgeFilter 非 nil，只沿其返回 true 的边
// 前进；若 until 非 nil，遇到第一个使其返回 true 的节点即停止并返回。遍历过程中若 Visit
// 非 nil，则对每条经过的边调用它，传入边连接的两个节点。
func (d *VisitingDepthFirst) Walk(g VisitableGraph, from graph.Node, until func(graph.Node) bool) graph.Node {
	if d.visited == nil {
		d.visited = &intsets.Sparse{}
	}
	d.stack.Push(from)
	d.visited.Insert(from.ID())
	if until != nil && until(from) {
		return from
	}

	var found graph.Node
	for d.stack.Len() > 0 {
		t := d.stack.Pop()
		g.VisitFrom(t, func(n graph.Node) (shouldContinue bool) {
			if d.EdgeFilter != nil && !d.EdgeFilter(g.Edge(t, n)) {
				return true
			}
			if d.visited.Has(n.ID()) {
				return true
			}
			if d.Visit != nil {
				d.Visit(t, n)
			}
			d.visited.Insert(n.ID())
			d.stack.Push(n)
			if until != nil && until(n) {
				found = n
				return false
			}
			return true
		})
		if found != nil {
			return found
		}
	}
	return nil
}

// Visited 返回节点 n 在本次遍历中是否被访问过。
func (d *VisitingDepthFirst) Visited(n graph.Node) bool {
	return d.visited != nil && d.visited.Has(n.ID())
}

// Reset 重置遍历器状态以便复用。
func (d *VisitingDepthFirst) Reset() {
	d.stack = d.stack[:0]
	if d.visited != nil {
		d.visited.Clear()
	}
}
