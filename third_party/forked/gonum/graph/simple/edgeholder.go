package simple

import "k8s.io/kubernetes/third_party/forked/gonum/graph"

// edgeHolder 保存与某个节点相连的边集合，每个相邻节点最多对应一条边
type edgeHolder interface {
	// Visit 对每条边调用 visitor，传入边和相邻节点 ID
	Visit(visitor func(neighbor int, edge graph.Edge))
	// Delete 删除与指定相邻节点相连的边
	Delete(neighbor int) edgeHolder
	// Set 保存与指定相邻节点相连的边
	Set(neighbor int, edge graph.Edge) edgeHolder
	// Get 返回与指定相邻节点相连的边
	Get(neighbor int) (graph.Edge, bool)
	// Len 返回边数
	Len() int
}

// sliceEdgeHolder 用切片保存与自身相连的边
type sliceEdgeHolder struct {
	self  int
	edges []graph.Edge
}

func (e *sliceEdgeHolder) Visit(visitor func(neighbor int, edge graph.Edge)) {
	for _, edge := range e.edges {
		if edge.From().ID() == e.self {
			visitor(edge.To().ID(), edge)
		} else {
			visitor(edge.From().ID(), edge)
		}
	}
}
func (e *sliceEdgeHolder) Delete(neighbor int) edgeHolder {
	edges := e.edges[:0]
	for i, edge := range e.edges {
		if edge.From().ID() == e.self {
			if edge.To().ID() == neighbor {
				continue
			}
		} else {
			if edge.From().ID() == neighbor {
				continue
			}
		}
		edges = append(edges, e.edges[i])
	}
	e.edges = edges
	return e
}
func (e *sliceEdgeHolder) Set(neighbor int, newEdge graph.Edge) edgeHolder {
	for i, edge := range e.edges {
		if edge.From().ID() == e.self {
			if edge.To().ID() == neighbor {
				e.edges[i] = newEdge
				return e
			}
		} else {
			if edge.From().ID() == neighbor {
				e.edges[i] = newEdge
				return e
			}
		}
	}

	if len(e.edges) < 4 {
		e.edges = append(e.edges, newEdge)
		return e
	}

	h := mapEdgeHolder(make(map[int]graph.Edge, len(e.edges)+1))
	for i, edge := range e.edges {
		if edge.From().ID() == e.self {
			h[edge.To().ID()] = e.edges[i]
		} else {
			h[edge.From().ID()] = e.edges[i]
		}
	}
	h[neighbor] = newEdge
	return h
}
func (e *sliceEdgeHolder) Get(neighbor int) (graph.Edge, bool) {
	for _, edge := range e.edges {
		if edge.From().ID() == e.self {
			if edge.To().ID() == neighbor {
				return edge, true
			}
		} else {
			if edge.From().ID() == neighbor {
				return edge, true
			}
		}
	}
	return nil, false
}
func (e *sliceEdgeHolder) Len() int {
	return len(e.edges)
}

// mapEdgeHolder 用 map 保存相邻节点到边的映射
type mapEdgeHolder map[int]graph.Edge

func (e mapEdgeHolder) Visit(visitor func(neighbor int, edge graph.Edge)) {
	for neighbor, edge := range e {
		visitor(neighbor, edge)
	}
}
func (e mapEdgeHolder) Delete(neighbor int) edgeHolder {
	delete(e, neighbor)
	return e
}
func (e mapEdgeHolder) Set(neighbor int, edge graph.Edge) edgeHolder {
	e[neighbor] = edge
	return e
}
func (e mapEdgeHolder) Get(neighbor int) (graph.Edge, bool) {
	edge, ok := e[neighbor]
	return edge, ok
}
func (e mapEdgeHolder) Len() int {
	return len(e)
}
