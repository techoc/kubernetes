/*
Copyright 2018 The Kubernetes Authors.

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

package garbagecollector

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// dotVertex 是依赖图节点在 DOT（Graphviz）格式下的表示，携带 UID、GVK 及删除状态等诊断信息。
type dotVertex struct {
	uid                types.UID
	gvk                schema.GroupVersionKind
	namespace          string
	name               string
	missingFromGraph   bool
	beingDeleted       bool
	deletingDependents bool
	virtual            bool
}

// MarshalDOT 将节点以 DOT 语法写入 w，输出带属性的节点定义。
func (v *dotVertex) MarshalDOT(w io.Writer) error {
	attrs := v.Attributes()
	if _, err := fmt.Fprintf(w, "  %q [\n", v.uid); err != nil {
		return err
	}
	for _, a := range attrs {
		if _, err := fmt.Fprintf(w, "    %s=%q\n", a.Key, a.Value); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "  ];\n"); err != nil {
		return err
	}
	return nil
}

// String 返回节点的可读描述，包含 GVK、名称、命名空间与各种状态标记。
func (v *dotVertex) String() string {
	kind := v.gvk.Kind + "." + v.gvk.Version
	if len(v.gvk.Group) > 0 {
		kind = kind + "." + v.gvk.Group
	}
	missing := ""
	if v.missingFromGraph {
		missing = "(missing)"
	}
	deleting := ""
	if v.beingDeleted {
		deleting = "(deleting)"
	}
	deletingDependents := ""
	if v.deletingDependents {
		deleting = "(deletingDependents)"
	}
	virtual := ""
	if v.virtual {
		virtual = "(virtual)"
	}
	return fmt.Sprintf(`%s/%s[%s]-%v%s%s%s%s`, kind, v.name, v.namespace, v.uid, missing, deleting, deletingDependents, virtual)
}

// attribute 表示 DOT 节点的一个键值对属性。
type attribute struct {
	Key   string
	Value string
}

// Attributes 生成节点的 DOT 属性列表：label 供人阅读，group/version/kind 等属性便于程序解析与过滤。
func (v *dotVertex) Attributes() []attribute {
	kubectlString := v.gvk.Kind + "." + v.gvk.Version
	if len(v.gvk.Group) > 0 {
		kubectlString = kubectlString + "." + v.gvk.Group
	}
	kubectlString = kubectlString + "/" + v.name

	label := fmt.Sprintf(`uid=%v
namespace=%v
%v
`,
		v.uid,
		v.namespace,
		kubectlString,
	)

	conditionStrings := []string{}
	if v.beingDeleted {
		conditionStrings = append(conditionStrings, "beingDeleted")
	}
	if v.deletingDependents {
		conditionStrings = append(conditionStrings, "deletingDependents")
	}
	if v.virtual {
		conditionStrings = append(conditionStrings, "virtual")
	}
	if v.missingFromGraph {
		conditionStrings = append(conditionStrings, "missingFromGraph")
	}
	conditionString := strings.Join(conditionStrings, ",")
	if len(conditionString) > 0 {
		label = label + conditionString + "\n"
	}

	return []attribute{
		{Key: "label", Value: label},
		// 这些属性让元数据出现在正确的位置，但并非标准的可渲染属性。
		{Key: "group", Value: v.gvk.Group},
		{Key: "version", Value: v.gvk.Version},
		{Key: "kind", Value: v.gvk.Kind},
		{Key: "namespace", Value: v.namespace},
		{Key: "name", Value: v.name},
		{Key: "uid", Value: string(v.uid)},
		{Key: "missing", Value: fmt.Sprintf(`%v`, v.missingFromGraph)},
		{Key: "beingDeleted", Value: fmt.Sprintf(`%v`, v.beingDeleted)},
		{Key: "deletingDependents", Value: fmt.Sprintf(`%v`, v.deletingDependents)},
		{Key: "virtual", Value: fmt.Sprintf(`%v`, v.virtual)},
	}
}

// NewDOTVertex 根据依赖图中的节点构造 DOT 顶点；未被观测到的节点标记为 virtual。
func NewDOTVertex(node *node) *dotVertex {
	gv, err := schema.ParseGroupVersion(node.identity.APIVersion)
	if err != nil {
		// 这表明 API 对象存储前本应阻止此类错误序列化。
		utilruntime.HandleError(err)
	}
	return &dotVertex{
		uid:                node.identity.UID,
		gvk:                gv.WithKind(node.identity.Kind),
		namespace:          node.identity.Namespace,
		name:               node.identity.Name,
		beingDeleted:       node.isBeingDeleted(),
		deletingDependents: node.isDeletingDependents(),
		virtual:            !node.isObserved(),
	}
}

// NewMissingdotVertex 为图中缺失的所有者构造虚拟顶点，标记 missingFromGraph 以便定位悬空引用。
func NewMissingdotVertex(ownerRef metav1.OwnerReference) *dotVertex {
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	if err != nil {
		// 这表明 API 对象存储前本应阻止此类错误序列化。
		utilruntime.HandleError(err)
	}
	return &dotVertex{
		uid:              ownerRef.UID,
		gvk:              gv.WithKind(ownerRef.Kind),
		name:             ownerRef.Name,
		missingFromGraph: true,
	}
}

// ToDOTNodesAndEdges 在持锁状态下导出全图的顶点与边，供调试接口使用。
func (m *concurrentUIDToNode) ToDOTNodesAndEdges() ([]*dotVertex, []dotEdge) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()

	return toDOTNodesAndEdges(m.uidToNode)
}

// dotEdge 表示依赖图中的一条有向边：F 是被依赖者，T 是所有者。
type dotEdge struct {
	F types.UID
	T types.UID
}

// MarshalDOT 将边以 DOT 语法写入 w。
func (e dotEdge) MarshalDOT(w io.Writer) error {
	_, err := fmt.Fprintf(w, "  %q -> %q;\n", e.F, e.T)
	return err
}

// toDOTNodesAndEdges 将节点映射转换为 DOT 顶点与边，并对结果排序以保证输出稳定可比较。
func toDOTNodesAndEdges(uidToNode map[types.UID]*node) ([]*dotVertex, []dotEdge) {
	nodes := []*dotVertex{}
	edges := []dotEdge{}

	uidToVertex := map[types.UID]*dotVertex{}

	// 先收集所有节点再加边，这样遇到缺失的所有者时可以按需补建虚拟顶点，无需预先处理悬空引用。
	for _, node := range uidToNode {
		// 没有任何属主或依赖者的孤立对象对调试依赖图没有价值，跳过。
		if node.dependentsLength() == 0 && len(node.getOwners()) == 0 {
			continue
		}
		vertex := NewDOTVertex(node)
		uidToVertex[node.identity.UID] = vertex
		nodes = append(nodes, vertex)
	}
	for _, node := range uidToNode {
		currVertex := uidToVertex[node.identity.UID]
		for _, ownerRef := range node.getOwners() {
			currOwnerVertex, ok := uidToVertex[ownerRef.UID]
			if !ok {
				currOwnerVertex = NewMissingdotVertex(ownerRef)
				uidToVertex[node.identity.UID] = currOwnerVertex
				nodes = append(nodes, currOwnerVertex)
			}
			edges = append(edges, dotEdge{F: currVertex.uid, T: currOwnerVertex.uid})
		}
	}

	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].uid < nodes[j].uid })
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].F != edges[j].F {
			return edges[i].F < edges[j].F
		}
		return edges[i].T < edges[j].T
	})

	return nodes, edges
}

// ToDOTNodesAndEdgesForObj 在持锁状态下导出指定 UID 及其关联子图的顶点与边。
func (m *concurrentUIDToNode) ToDOTNodesAndEdgesForObj(uids ...types.UID) ([]*dotVertex, []dotEdge) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()

	return toDOTNodesAndEdgesForObj(m.uidToNode, uids...)
}

// toDOTNodesAndEdgesForObj 沿属主与依赖者关系做广度优先遍历，收集目标 UID 的关联子图后复用常规转换逻辑。
func toDOTNodesAndEdgesForObj(uidToNode map[types.UID]*node, uids ...types.UID) ([]*dotVertex, []dotEdge) {
	uidsToCheck := append([]types.UID{}, uids...)
	interestingNodes := map[types.UID]*node{}

	// 先收集需要检查的节点集合，再把子集交给常规转换函数处理。
	for i := 0; i < len(uidsToCheck); i++ {
		uid := uidsToCheck[i]
		// 若 UID 已被处理过说明遍历出现重复，跳过以避免死循环。
		if _, ok := interestingNodes[uid]; ok {
			continue
		}
		node, ok := uidToNode[uid]
		// 图中不存在该 UID 时跳过；同一 UID 可能被重复加入列表，但已有去重检查，不会死循环。
		if !ok {
			continue
		}

		interestingNodes[node.identity.UID] = node

		for _, ownerRef := range node.getOwners() {
			// 已检查过的 UID 不再重复入队。
			if _, ok := interestingNodes[ownerRef.UID]; ok {
				continue
			}
			uidsToCheck = append(uidsToCheck, ownerRef.UID)
		}
		for _, dependent := range node.getDependents() {
			// 已检查过的 UID 不再重复入队。
			if _, ok := interestingNodes[dependent.identity.UID]; ok {
				continue
			}
			uidsToCheck = append(uidsToCheck, dependent.identity.UID)
		}
	}

	return toDOTNodesAndEdges(interestingNodes)
}

// NewDebugHandler 创建暴露依赖图 DOT 输出的调试 HTTP 处理器。
func NewDebugHandler(controller *GarbageCollector) http.Handler {
	return &debugHTTPHandler{controller: controller}
}

type debugHTTPHandler struct {
	controller *GarbageCollector
}

// marshalDOT 将顶点与边序列化为完整的 DOT digraph 文档。
func marshalDOT(w io.Writer, nodes []*dotVertex, edges []dotEdge) error {
	if _, err := w.Write([]byte("strict digraph full {\n")); err != nil {
		return err
	}
	if len(nodes) > 0 {
		if _, err := w.Write([]byte("  // Node definitions.\n")); err != nil {
			return err
		}
		for _, node := range nodes {
			if err := node.MarshalDOT(w); err != nil {
				return err
			}
		}
	}
	if len(edges) > 0 {
		if _, err := w.Write([]byte("  // Edge definitions.\n")); err != nil {
			return err
		}
		for _, edge := range edges {
			if err := edge.MarshalDOT(w); err != nil {
				return err
			}
		}
	}
	if _, err := w.Write([]byte("}\n")); err != nil {
		return err
	}
	return nil
}

// ServeHTTP 处理 /graph 调试请求：按 uid 参数过滤子图，未带参数时返回全图。
func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/graph" {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	var nodes []*dotVertex
	var edges []dotEdge
	if uidStrings := req.URL.Query()["uid"]; len(uidStrings) > 0 {
		uids := []types.UID{}
		for _, uidString := range uidStrings {
			uids = append(uids, types.UID(uidString))
		}
		nodes, edges = h.controller.dependencyGraphBuilder.uidToNode.ToDOTNodesAndEdgesForObj(uids...)

	} else {
		nodes, edges = h.controller.dependencyGraphBuilder.uidToNode.ToDOTNodesAndEdges()
	}

	b := bytes.NewBuffer(nil)
	if err := marshalDOT(b, nodes, edges); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 明确设置 DOT 类型并禁用内容嗅探，避免浏览器误解析响应。
	w.Header().Set("Content-Type", "text/vnd.graphviz")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(b.Bytes())
	w.WriteHeader(http.StatusOK)
}

// DebuggingHandler 返回垃圾回收器的调试 HTTP 处理器。
func (gc *GarbageCollector) DebuggingHandler() http.Handler {
	return NewDebugHandler(gc)
}
