/*
Copyright 2015 The Kubernetes Authors.

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

package jsonpath

import "fmt"

// NodeType 标识解析树节点的类型。
type NodeType int

// Type 返回自身，为节点类型提供便捷的默认实现。
//
// 所有节点都内嵌 NodeType，从而自动获得 Type() 方法，无需逐个实现。
func (t NodeType) Type() NodeType {
	return t
}

// String 返回节点类型的可读名称，便于调试与错误信息。
func (t NodeType) String() string {
	return NodeTypeName[t]
}

const (
	NodeText NodeType = iota
	NodeArray
	NodeList
	NodeField
	NodeIdentifier
	NodeFilter
	NodeInt
	NodeFloat
	NodeWildcard
	NodeRecursive
	NodeUnion
	NodeBool
)

// NodeTypeName 返回 NodeType 的字符串表示；未知类型返回空串。
var NodeTypeName = map[NodeType]string{
	NodeText:       "NodeText",
	NodeArray:      "NodeArray",
	NodeList:       "NodeList",
	NodeField:      "NodeField",
	NodeIdentifier: "NodeIdentifier",
	NodeFilter:     "NodeFilter",
	NodeInt:        "NodeInt",
	NodeFloat:      "NodeFloat",
	NodeWildcard:   "NodeWildcard",
	NodeRecursive:  "NodeRecursive",
	NodeUnion:      "NodeUnion",
	NodeBool:       "NodeBool",
}

// Node 是 AST 节点的公共接口：标识自身类型并格式化为可读形式。
type Node interface {
	Type() NodeType
	String() string
}

// ListNode 保存一串节点。
type ListNode struct {
	NodeType
	Nodes []Node // 按词法顺序排列的子节点。
}

func newList() *ListNode {
	return &ListNode{NodeType: NodeList}
}

// append 按词法顺序追加子节点。
func (l *ListNode) append(n Node) {
	l.Nodes = append(l.Nodes, n)
}

func (l *ListNode) String() string {
	return l.Type().String()
}

// TextNode 保存纯文本。
type TextNode struct {
	NodeType
	Text string // 文本内容，可能跨多行。
}

func newText(text string) *TextNode {
	return &TextNode{NodeType: NodeText, Text: text}
}

func (t *TextNode) String() string {
	return fmt.Sprintf("%s: %s", t.Type(), t.Text)
}

// FieldNode 保存结构体字段。
type FieldNode struct {
	NodeType
	Value string
}

func newField(value string) *FieldNode {
	return &FieldNode{NodeType: NodeField, Value: value}
}

func (f *FieldNode) String() string {
	return fmt.Sprintf("%s: %s", f.Type(), f.Value)
}

// IdentifierNode 保存一个标识符。
type IdentifierNode struct {
	NodeType
	Name string
}

func newIdentifier(value string) *IdentifierNode {
	return &IdentifierNode{
		NodeType: NodeIdentifier,
		Name:     value,
	}
}

func (f *IdentifierNode) String() string {
	return fmt.Sprintf("%s: %s", f.Type(), f.Name)
}

// ParamsEntry 保存 ArrayNode 的参数信息。
//
// Known 表示解析时已确定具体下标；Derived 表示下标来自表达式求值而非字面量。
type ParamsEntry struct {
	Value   int
	Known   bool // 解析时已知的值。
	Derived bool
}

// ArrayNode 保存数组下标选择的范围、起止与步长信息。
//
// 对应 a[start:end:step]；缺省项由 ParamsEntry.Known 标记。
type ArrayNode struct {
	NodeType
	Params [3]ParamsEntry // 起始、结束、步长。
}

func newArray(params [3]ParamsEntry) *ArrayNode {
	return &ArrayNode{
		NodeType: NodeArray,
		Params:   params,
	}
}

func (a *ArrayNode) String() string {
	return fmt.Sprintf("%s: %v", a.Type(), a.Params)
}

// FilterNode 保存过滤操作数与运算符。
//
// 对应 [?(@.left op right)]；Left/Right 为表达式子树，operator 为比较符。
type FilterNode struct {
	NodeType
	Left     *ListNode
	Right    *ListNode
	Operator string
}

func newFilter(left, right *ListNode, operator string) *FilterNode {
	return &FilterNode{
		NodeType: NodeFilter,
		Left:     left,
		Right:    right,
		Operator: operator,
	}
}

func (f *FilterNode) String() string {
	return fmt.Sprintf("%s: %s %s %s", f.Type(), f.Left, f.Operator, f.Right)
}

// IntNode 保存整数值。
type IntNode struct {
	NodeType
	Value int
}

func newInt(num int) *IntNode {
	return &IntNode{NodeType: NodeInt, Value: num}
}

func (i *IntNode) String() string {
	return fmt.Sprintf("%s: %d", i.Type(), i.Value)
}

// FloatNode 保存浮点值。
type FloatNode struct {
	NodeType
	Value float64
}

func newFloat(num float64) *FloatNode {
	return &FloatNode{NodeType: NodeFloat, Value: num}
}

func (i *FloatNode) String() string {
	return fmt.Sprintf("%s: %f", i.Type(), i.Value)
}

// WildcardNode 表示通配符。
type WildcardNode struct {
	NodeType
}

func newWildcard() *WildcardNode {
	return &WildcardNode{NodeType: NodeWildcard}
}

func (i *WildcardNode) String() string {
	return i.Type().String()
}

// RecursiveNode 表示递归下降操作符。
type RecursiveNode struct {
	NodeType
}

func newRecursive() *RecursiveNode {
	return &RecursiveNode{NodeType: NodeRecursive}
}

func (r *RecursiveNode) String() string {
	return r.Type().String()
}

// UnionNode 表示 ListNode 的并集。
//
// 对应 (expr1,expr2,...)：各分支的结果按出现顺序合并。
type UnionNode struct {
	NodeType
	Nodes []*ListNode
}

func newUnion(nodes []*ListNode) *UnionNode {
	return &UnionNode{NodeType: NodeUnion, Nodes: nodes}
}

func (u *UnionNode) String() string {
	return u.Type().String()
}

// BoolNode 保存 bool 值。
type BoolNode struct {
	NodeType
	Value bool
}

func newBool(value bool) *BoolNode {
	return &BoolNode{NodeType: NodeBool, Value: value}
}

func (b *BoolNode) String() string {
	return fmt.Sprintf("%s: %t", b.Type(), b.Value)
}
