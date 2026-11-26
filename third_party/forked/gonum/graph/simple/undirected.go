// Copyright ©2014 The gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package simple

import (
	"fmt"

	"golang.org/x/tools/container/intsets"

	"k8s.io/kubernetes/third_party/forked/gonum/graph"
)

// UndirectedGraph 实现通用的无向图。
type UndirectedGraph struct {
	nodes map[int]graph.Node
	edges map[int]edgeHolder

	self, absent float64

	freeIDs intsets.Sparse
	usedIDs intsets.Sparse
}

// NewUndirectedGraph 创建一个自环边和缺省边权重分别取指定值的无向图。
func NewUndirectedGraph(self, absent float64) *UndirectedGraph {
	return &UndirectedGraph{
		nodes: make(map[int]graph.Node),
		edges: make(map[int]edgeHolder),

		self:   self,
		absent: absent,
	}
}

// NewNodeID 返回可供新节点使用的唯一 ID；在真正加入图之前该 ID 并不生效。
func (g *UndirectedGraph) NewNodeID() int {
	if len(g.nodes) == 0 {
		return 0
	}
	if len(g.nodes) == maxInt {
		panic(fmt.Sprintf("simple: cannot allocate node: no slot"))
	}

	var id int
	if g.freeIDs.Len() != 0 && g.freeIDs.TakeMin(&id) {
		return id
	}
	if id = g.usedIDs.Max(); id < maxInt {
		return id + 1
	}
	for id = 0; id < maxInt; id++ {
		if !g.usedIDs.Has(id) {
			return id
		}
	}
	panic("unreachable")
}

// AddNode 把节点 n 加入图；ID 与已有节点重复时 panic。
func (g *UndirectedGraph) AddNode(n graph.Node) {
	if _, exists := g.nodes[n.ID()]; exists {
		panic(fmt.Sprintf("simple: node ID collision: %d", n.ID()))
	}
	g.nodes[n.ID()] = n
	g.edges[n.ID()] = &sliceEdgeHolder{self: n.ID()}

	g.freeIDs.Remove(n.ID())
	g.usedIDs.Insert(n.ID())
}

// RemoveNode 删除节点 n 及其关联边；节点不存在时不做任何事。
func (g *UndirectedGraph) RemoveNode(n graph.Node) {
	if _, ok := g.nodes[n.ID()]; !ok {
		return
	}
	delete(g.nodes, n.ID())

	g.edges[n.ID()].Visit(func(neighbor int, edge graph.Edge) {
		g.edges[neighbor] = g.edges[neighbor].Delete(n.ID())
	})
	delete(g.edges, n.ID())

	g.freeIDs.Insert(n.ID())
	g.usedIDs.Remove(n.ID())

}

// SetEdge 添加边 e；端点不存在时会自动创建。e.From 与 e.To 的 ID 相同时 panic。
func (g *UndirectedGraph) SetEdge(e graph.Edge) {
	var (
		from = e.From()
		fid  = from.ID()
		to   = e.To()
		tid  = to.ID()
	)

	if fid == tid {
		panic("simple: adding self edge")
	}

	if !g.Has(from) {
		g.AddNode(from)
	}
	if !g.Has(to) {
		g.AddNode(to)
	}

	g.edges[fid] = g.edges[fid].Set(tid, e)
	g.edges[tid] = g.edges[tid].Set(fid, e)
}

// RemoveEdge 删除边 e 但保留端点节点；边不存在时不做任何事。
func (g *UndirectedGraph) RemoveEdge(e graph.Edge) {
	from, to := e.From(), e.To()
	if _, ok := g.nodes[from.ID()]; !ok {
		return
	}
	if _, ok := g.nodes[to.ID()]; !ok {
		return
	}

	g.edges[from.ID()] = g.edges[from.ID()].Delete(to.ID())
	g.edges[to.ID()] = g.edges[to.ID()].Delete(from.ID())
}

// Node 返回图中指定 ID 的节点。
func (g *UndirectedGraph) Node(id int) graph.Node {
	return g.nodes[id]
}

// Has 判断节点是否在图中。
func (g *UndirectedGraph) Has(n graph.Node) bool {
	_, ok := g.nodes[n.ID()]
	return ok
}

// Nodes 返回图中所有节点。
func (g *UndirectedGraph) Nodes() []graph.Node {
	nodes := make([]graph.Node, len(g.nodes))
	i := 0
	for _, n := range g.nodes {
		nodes[i] = n
		i++
	}

	return nodes
}

// Edges 返回图中所有边。
func (g *UndirectedGraph) Edges() []graph.Edge {
	var edges []graph.Edge

	seen := make(map[[2]int]struct{})
	for _, u := range g.edges {
		u.Visit(func(neighbor int, e graph.Edge) {
			uid := e.From().ID()
			vid := e.To().ID()
			if _, ok := seen[[2]int{uid, vid}]; ok {
				return
			}
			seen[[2]int{uid, vid}] = struct{}{}
			seen[[2]int{vid, uid}] = struct{}{}
			edges = append(edges, e)
		})
	}

	return edges
}

// From 返回图中从 n 可直接到达的所有节点。
func (g *UndirectedGraph) From(n graph.Node) []graph.Node {
	if !g.Has(n) {
		return nil
	}

	nodes := make([]graph.Node, g.edges[n.ID()].Len())
	i := 0
	g.edges[n.ID()].Visit(func(neighbor int, edge graph.Edge) {
		nodes[i] = g.nodes[neighbor]
		i++
	})

	return nodes
}

// HasEdgeBetween 判断节点 x 和 y 之间是否有边。
func (g *UndirectedGraph) HasEdgeBetween(x, y graph.Node) bool {
	_, ok := g.edges[x.ID()].Get(y.ID())
	return ok
}

// Edge 返回 u 到 v 的边，不存在时返回 nil。v 必须是从 u 可直接到达的节点。
func (g *UndirectedGraph) Edge(u, v graph.Node) graph.Edge {
	return g.EdgeBetween(u, v)
}

// EdgeBetween 返回节点 x 和 y 之间的边。
func (g *UndirectedGraph) EdgeBetween(x, y graph.Node) graph.Edge {
	// 无需单独检查 neigh 是否存在：访问边时已隐含该判断。
	if !g.Has(x) {
		return nil
	}

	edge, _ := g.edges[x.ID()].Get(y.ID())
	return edge
}

// Weight 在 Edge(x, y) 非 nil 时返回 x 与 y 之间边的权重。若两者是同一节点或没有边，
// 则返回图的自环值或缺省边权重。当存在边或两者 ID 相同时 ok 为 true，否则为 false。
func (g *UndirectedGraph) Weight(x, y graph.Node) (w float64, ok bool) {
	xid := x.ID()
	yid := y.ID()
	if xid == yid {
		return g.self, true
	}
	if n, ok := g.edges[xid]; ok {
		if e, ok := n.Get(yid); ok {
			return e.Weight(), true
		}
	}
	return g.absent, false
}

// Degree 返回节点 n 在图中的度。
func (g *UndirectedGraph) Degree(n graph.Node) int {
	if _, ok := g.nodes[n.ID()]; !ok {
		return 0
	}

	return g.edges[n.ID()].Len()
}
