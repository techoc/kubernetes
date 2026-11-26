// Copyright ©2014 The gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package simple 提供满足 gonum/graph 接口的一组简单图实现。
package simple

import (
	"math"

	"k8s.io/kubernetes/third_party/forked/gonum/graph"
)

// Node 是简单的图节点。
type Node int

// ID 返回节点 ID。
func (n Node) ID() int {
	return int(n)
}

// Edge 是简单的图边。
type Edge struct {
	F, T graph.Node
	W    float64
}

// From 返回边的起点。
func (e Edge) From() graph.Node { return e.F }

// To 返回边的终点。
func (e Edge) To() graph.Node { return e.T }

// Weight 返回边的权重。
func (e Edge) Weight() float64 { return e.W }

// maxInt 是当前机器 int 类型的最大值。
const maxInt int = int(^uint(0) >> 1)

// isSame 判断两个 float64 是否相等，且视两个 NaN 为相等。
func isSame(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}
