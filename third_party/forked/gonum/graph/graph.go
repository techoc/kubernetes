// Copyright ©2014 The gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package graph

// Node 表示图节点，返回图内唯一的整数 ID。
type Node interface {
	ID() int
}

// Edge 表示图边。有向图中方向由 from -> to 给出，无向图中边没有方向语义。
type Edge interface {
	From() Node
	To() Node
	Weight() float64
}

// Graph 是图的通用接口。
type Graph interface {
	// Has 判断节点是否在图中。
	Has(Node) bool

	// Nodes 返回图中所有节点。
	Nodes() []Node

	// From 返回从给定节点可直接到达的所有节点。
	From(Node) []Node

	// HasEdgeBeteen 不考虑方向，判断节点 x 和 y 之间是否有边。
	HasEdgeBetween(x, y Node) bool

	// Edge 返回 u 到 v 的边，不存在时返回 nil。v 必须是从 u 可直接到达的节点。
	Edge(u, v Node) Edge
}

// Undirected 表示无向图。
type Undirected interface {
	Graph

	// EdgeBetween 返回节点 x 和 y 之间的边。
	EdgeBetween(x, y Node) Edge
}

// Directed 表示有向图。
type Directed interface {
	Graph

	// HasEdgeFromTo 判断图中是否存在 u 到 v 的边。
	HasEdgeFromTo(u, v Node) bool

	// To 返回所有可直接到达给定节点的节点。
	To(Node) []Node
}

// Weighter 定义能够报告边权的图。
type Weighter interface {
	// Weight 在 Edge(x, y) 非 nil 时返回 x 与 y 之间边的权重。
	// 如果 x 与 y 是同一节点或两者之间没有边，返回值由实现决定。
	// 当 x 与 y 之间存在边或 ID 相同时 ok 为 true，否则为 false。
	Weight(x, y Node) (w float64, ok bool)
}

// NodeAdder 定义向图添加任意节点的接口。
type NodeAdder interface {
	// NewNodeID 返回一个新的唯一 ID。
	NewNodeID() int

	// AddNode 向图添加节点；若 ID 与已有节点重复则 panic。
	AddNode(Node)
}

// NodeRemover 定义从图删除节点的接口。
type NodeRemover interface {
	// RemoveNode 删除节点及其关联边；节点不存在时不做任何事。
	RemoveNode(Node)
}

// EdgeSetter 定义向图添加边的接口。
type EdgeSetter interface {
	// SetEdge 添加一条从一个节点到另一个节点的边。
	// 若图支持添加节点，缺失的端点会被自动创建，否则 panic。
	// e.From 与 e.To 的 ID 相同时也会 panic。
	SetEdge(e Edge)
}

// EdgeRemover 定义从图删除边的接口。
type EdgeRemover interface {
	// RemoveEdge 删除指定边但保留端点节点；边不存在时不做任何事。
	RemoveEdge(Edge)
}

// Builder 表示可增删节点和边的图。
type Builder interface {
	NodeAdder
	EdgeSetter
}

// UndirectedBuilder 表示无向图的构建器。
type UndirectedBuilder interface {
	Undirected
	Builder
}

// DirectedBuilder 表示有向图的构建器。
type DirectedBuilder interface {
	Directed
	Builder
}

// Copy 把源图中的节点和边复制到目标图，边按无向处理，且不先清空目标图。
// 若源图与目标图存在相同节点 ID，Copy 会 panic。
//
// 源图为无向、目标图为有向时，复制后目标图中两条方向的边都会存在。
//
// 源图为有向、目标图为无向时，若两个节点间存在权重不同的互逆边，
// 复制后目标图中该边的权重未定义。如需按既定规则消解冲突，可使用 Undirect。
func Copy(dst Builder, src Graph) {
	nodes := src.Nodes()
	for _, n := range nodes {
		dst.AddNode(n)
	}
	for _, u := range nodes {
		for _, v := range src.From(u) {
			dst.SetEdge(src.Edge(u, v))
		}
	}
}
