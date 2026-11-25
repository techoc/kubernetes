/*
Copyright 2017 The Kubernetes Authors.

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

package node

import (
	"fmt"
	"sync"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	pvutil "k8s.io/kubernetes/pkg/api/v1/persistentvolume"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/third_party/forked/gonum/graph"
	"k8s.io/kubernetes/third_party/forked/gonum/graph/simple"
)

// Graph 维护一个有向无环图，表示 Kubernetes 资源到 Node 的依赖关系。
// 所有边都指向 Node 顶点，方向为：资源 -> ... -> Node。
// 用于 Node 授权器快速判断某个资源是否与指定节点关联。
//
// 图的边方向示例：
//
//	node <- pod
//	pod  <- secret,configmap,pvc
//	pvc  <- pv
//	pv   <- secret
//
// 当 kubelet 请求访问某个资源时，授权器在图中查找从该资源到 kubelet 所在节点的路径。
// 如果存在路径，则允许访问；否则拒绝。
type Graph struct {
	lock sync.RWMutex // 保护图的并发访问
	// graph.SetEdge 仅允许由 Graph.addEdgeLocked 调用，以确保边和索引同步更新。
	graph *simple.DirectedAcyclicGraph // 底层有向无环图实现
	// vertices 按 类型 -> 命名空间 -> 名称 -> 顶点 组织，用于快速查找。
	vertices map[vertexType]namespaceVertexMapping // 快速查找顶点：类型 -> 命名空间 -> 名称 -> 顶点

	// destinationEdgeIndex 保存 顶点 -> 可达目标 Node ID 集合 的映射。
	destinationEdgeIndex map[int]*intSet // 优化：缓存每个顶点可达的 Node ID 集合，避免重复 DFS
	// destinationEdgeThreshold 是维护索引所需的最少不同目标 ID 数。
	destinationEdgeThreshold int // 只有当顶点的出边数超过此阈值时，才维护 destinationEdgeIndex（节省内存）
}

// namedVertex 实现 graph.Node 接口，表示图中一个 API 对象的顶点。
// 记录对象的类型、命名空间和名称，用于在图中定位和标识资源。
type namedVertex struct {
	name       string
	namespace  string
	id         int
	vertexType vertexType
}

func newNamedVertex(vertexType vertexType, namespace, name string, id int) *namedVertex {
	return &namedVertex{
		vertexType: vertexType,
		name:       name,
		namespace:  namespace,
		id:         id,
	}
}
func (n *namedVertex) ID() int {
	return n.id
}
func (n *namedVertex) String() string {
	if len(n.namespace) == 0 {
		return vertexTypes[n.vertexType] + ":" + n.name
	}
	return vertexTypes[n.vertexType] + ":" + n.namespace + "/" + n.name
}

// destinationEdge 是一条包含到最终目标顶点（Node）引用的边。
// 用于快速判断一个资源是否可达某个特定的 Node，避免完整的图遍历。
// 仅当从顶点 T 出发只能到达单个叶节点（Node）时使用。
type destinationEdge struct {
	F           graph.Node
	T           graph.Node
	Destination graph.Node
}

func newDestinationEdge(from, to, destination graph.Node) graph.Edge {
	return &destinationEdge{F: from, T: to, Destination: destination}
}
func (e *destinationEdge) From() graph.Node   { return e.F }
func (e *destinationEdge) To() graph.Node     { return e.T }
func (e *destinationEdge) Weight() float64    { return 0 }
func (e *destinationEdge) DestinationID() int { return e.Destination.ID() }

// namespaceVertexMapping 表示 命名空间 -> 名称 -> 顶点 的映射
type namespaceVertexMapping map[string]nameVertexMapping

// nameVertexMapping 表示 名称 -> 顶点 的映射
type nameVertexMapping map[string]*namedVertex

// NewGraph 创建用于 Node 授权检查的空资源依赖图。
func NewGraph() *Graph {
	return &Graph{
		vertices: map[vertexType]namespaceVertexMapping{},
		graph:    simple.NewDirectedAcyclicGraph(0, 0),

		destinationEdgeIndex: map[int]*intSet{},
		// 200 是通过实验确定的阈值：低于此值时，维护索引的开销大于直接遍历。
		// 索引需要额外的时间和内存来维护，所以不能设得太低。
		destinationEdgeThreshold: 200,
	}
}

// vertexType 表示顶点所代表的 API 对象类型。
// 使用 byte 类型以最小化顶点占用的空间。
type vertexType byte

const (
	configMapVertexType vertexType = iota
	sliceVertexType
	nodeVertexType
	podVertexType
	pvcVertexType
	pvVertexType
	resourceClaimVertexType
	secretVertexType
	vaVertexType
	serviceAccountVertexType
	pcrVertexType
)

var vertexTypes = map[vertexType]string{
	configMapVertexType:      "configmap",
	sliceVertexType:          "resourceslice",
	nodeVertexType:           "node",
	podVertexType:            "pod",
	pvcVertexType:            "pvc",
	pvVertexType:             "pv",
	resourceClaimVertexType:  "resourceclaim",
	secretVertexType:         "secret",
	vaVertexType:             "volumeattachment",
	serviceAccountVertexType: "serviceAccount",
	pcrVertexType:            "podcertificaterequest",
}

// vertexTypeWithAuthoritativeIndex 表示哪些类型的顶点可以维护权威的 destination edge 索引。
// 即：如果索引存在，则它总是包含从该顶点在图中可达的所有 Node。
// 对于 PV 和 Node 类型，索引不是权威的（因为 PV->PVC->Pod->Node 链路可能在 PV 添加时尚未建立）。
var vertexTypeWithAuthoritativeIndex = map[vertexType]bool{
	configMapVertexType:      true,
	sliceVertexType:          true,
	podVertexType:            true,
	pvcVertexType:            true,
	resourceClaimVertexType:  true,
	vaVertexType:             true,
	serviceAccountVertexType: true,
	pcrVertexType:            true,
}

// 必须在持有写锁时调用
func (g *Graph) getOrCreateVertexLocked(vertexType vertexType, namespace, name string) *namedVertex {
	if vertex, exists := g.getVertexRLocked(vertexType, namespace, name); exists {
		return vertex
	}
	return g.createVertexLocked(vertexType, namespace, name)
}

// 必须在持有读锁时调用
func (g *Graph) getVertexRLocked(vertexType vertexType, namespace, name string) (*namedVertex, bool) {
	vertex, exists := g.vertices[vertexType][namespace][name]
	return vertex, exists
}

// 必须在持有写锁时调用
func (g *Graph) createVertexLocked(vertexType vertexType, namespace, name string) *namedVertex {
	typedVertices, exists := g.vertices[vertexType]
	if !exists {
		typedVertices = namespaceVertexMapping{}
		g.vertices[vertexType] = typedVertices
	}

	namespacedVertices, exists := typedVertices[namespace]
	if !exists {
		namespacedVertices = map[string]*namedVertex{}
		typedVertices[namespace] = namespacedVertices
	}

	vertex := newNamedVertex(vertexType, namespace, name, g.graph.NewNodeID())
	namespacedVertices[name] = vertex
	g.graph.AddNode(vertex)

	return vertex
}

// 必须在持有写锁时调用
// deleteVertexLocked 删除指定顶点及其所有关联边。
// 同时清理孤立顶点（度数为 1 的邻居，即只有与被删除顶点之间的边）。
func (g *Graph) deleteVertexLocked(vertexType vertexType, namespace, name string) {
	vertex, exists := g.getVertexRLocked(vertexType, namespace, name)
	if !exists {
		return
	}

	// 查找只有一条边的现有邻居，即仅与被删除顶点相邻的节点。
	neighborsToRemove := []graph.Node{}
	edgesToRemoveFromIndexes := []graph.Edge{}
	g.graph.VisitFrom(vertex, func(neighbor graph.Node) bool {
		// 下游邻居只有一条边，该边必来自当前顶点，因此一并删除。
		if g.graph.Degree(neighbor) == 1 {
			neighborsToRemove = append(neighborsToRemove, neighbor)
		}
		return true
	})
	g.graph.VisitTo(vertex, func(neighbor graph.Node) bool {
		if g.graph.Degree(neighbor) == 1 {
			// 上游邻居只有一条边，该边必指向当前顶点，因此一并删除。
			neighborsToRemove = append(neighborsToRemove, neighbor)
		} else {
			// 若两顶点间的边是 destination edge，递减该邻居索引中的目标 ID 计数。
			edgesToRemoveFromIndexes = append(edgesToRemoveFromIndexes, g.graph.EdgeBetween(vertex, neighbor))
		}
		return true
	})

	// 删除顶点。
	g.removeVertexLocked(vertex)

	// 删除因此失去所有边的邻居。
	for _, neighbor := range neighborsToRemove {
		g.removeVertexLocked(neighbor.(*namedVertex))
	}

	// 从目标索引中移除因出边减少而受影响的边。
	for _, edge := range edgesToRemoveFromIndexes {
		g.removeEdgeFromDestinationIndexLocked(edge)
	}
}

// 必须在持有写锁时调用
// deleteEdgesLocked 删除从指定类型的顶点到目标顶点的所有边。
// 会删除孤立的源顶点，但不会删除目标顶点本身。
func (g *Graph) deleteEdgesLocked(fromType, toType vertexType, toNamespace, toName string) {
	// 查找目标顶点。
	toVert, exists := g.getVertexRLocked(toType, toNamespace, toName)
	if !exists {
		return
	}

	// 删除 fromType 类型顶点与目标顶点之间的所有边。
	neighborsToRemove := []*namedVertex{}
	edgesToRemove := []graph.Edge{}
	g.graph.VisitTo(toVert, func(from graph.Node) bool {
		fromVert := from.(*namedVertex)
		if fromVert.vertexType != fromType {
			return true
		}
		// 该邻居只有一条边，且必指向目标顶点，因此一并删除。
		if g.graph.Degree(fromVert) == 1 {
			neighborsToRemove = append(neighborsToRemove, fromVert)
		} else {
			edgesToRemove = append(edgesToRemove, g.graph.EdgeBetween(from, toVert))
		}
		return true
	})

	// 清理孤立顶点。
	for _, v := range neighborsToRemove {
		g.removeVertexLocked(v)
	}

	// 删除边，并递减因出边减少而受影响的邻居索引。
	for _, edge := range edgesToRemove {
		g.graph.RemoveEdge(edge)
		g.removeEdgeFromDestinationIndexLocked(edge)
	}
}

// removeEdgeFromDestinationIndexLocked 是 recomputeDestinationIndexLocked 的删除边快速路径。
// 仅更新索引中受影响的目标 ID 引用计数，避免全量重算。
func (g *Graph) removeEdgeFromDestinationIndexLocked(e graph.Edge) {
	n := e.From()
	// 边数较少的顶点不维护索引。

	edgeCount := g.graph.Degree(n)
	if edgeCount < g.destinationEdgeThreshold {
		delete(g.destinationEdgeIndex, n.ID())
		return
	}

	// 若索引存在，递减其中 nodeID -> destinationID 的引用计数。
	index := g.destinationEdgeIndex[n.ID()]
	if index == nil {
		return
	}
	if destinationEdge, ok := e.(*destinationEdge); ok {
		index.decrement(destinationEdge.DestinationID())
	}
}

// addEdgeToDestinationIndexLocked 是 recomputeDestinationIndexLocked 的添加边快速路径。
// 如果索引已存在，直接递增目标 ID 的引用计数；否则全量重算索引。
func (g *Graph) addEdgeToDestinationIndexLocked(e graph.Edge) {
	n := e.From()
	index := g.destinationEdgeIndex[n.ID()]
	if index == nil {
		// 索引不存在，改用全量计算。
		g.recomputeDestinationIndexLocked(n)
		return
	}
	// 将新边快速加入已有索引。
	if destinationEdge, ok := e.(*destinationEdge); ok {
		index.increment(destinationEdge.DestinationID())
	}
}

// 必须在持有写锁时调用
// removeVertexLocked 从图和索引中移除指定顶点，但不处理邻居顶点的索引。
func (g *Graph) removeVertexLocked(v *namedVertex) {
	g.graph.RemoveNode(v)
	delete(g.destinationEdgeIndex, v.ID())
	delete(g.vertices[v.vertexType][v.namespace], v.name)
	if len(g.vertices[v.vertexType][v.namespace]) == 0 {
		delete(g.vertices[v.vertexType], v.namespace)
	}
}

// 必须在持有写锁时调用
// recomputeDestinationIndexLocked 重新计算指定顶点的 destination ID 索引。
// 仅当顶点的出边数超过 destinationEdgeThreshold 时才维护索引。
func (g *Graph) recomputeDestinationIndexLocked(n graph.Node) {
	// 边数较少的顶点不维护索引。

	edgeCount := g.graph.Degree(n)
	if edgeCount < g.destinationEdgeThreshold {
		delete(g.destinationEdgeIndex, n.ID())
		return
	}

	// 获取或创建索引。
	index := g.destinationEdgeIndex[n.ID()]
	if index == nil {
		index = newIntSet()
	} else {
		index.reset()
	}

	// 填充索引。
	g.graph.VisitFrom(n, func(dest graph.Node) bool {
		if destinationEdge, ok := g.graph.EdgeBetween(n, dest).(*destinationEdge); ok {
			index.increment(destinationEdge.DestinationID())
		}
		return true
	})
	g.destinationEdgeIndex[n.ID()] = index
}

// AddPod 将 Pod 及其引用的资源添加到图中。
// 仅在 spec.NodeName 已填充时调用（Pod 已调度到节点）。
// 建立以下边关系（Pod 绑定到节点后不可变）：
//
//	pod       -> node
//	secret    -> pod
//	configmap -> pod
//	pvc       -> pod
//	svcacct   -> pod
//	resourceclaim -> pod
func (g *Graph) AddPod(pod *corev1.Pod) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("AddPod").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()

	g.deleteVertexLocked(podVertexType, pod.Namespace, pod.Name)
	podVertex := g.getOrCreateVertexLocked(podVertexType, pod.Namespace, pod.Name)
	nodeVertex := g.getOrCreateVertexLocked(nodeVertexType, "", pod.Spec.NodeName)
	// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
	g.addEdgeLocked(podVertex, nodeVertex, nodeVertex)

	// 对于 mirror pod，不添加到其他资源的边。
	// 节点绝不能通过创建 mirror pod 来获得对其他 API 对象的权限。
	// NodeRestriction 准入插件 已经阻止创建此类 Pod，这里短路是纵深防御。
	if _, isMirrorPod := pod.Annotations[corev1.MirrorPodAnnotationKey]; isMirrorPod {
		return
	}

	// Pod 无条件获得其 ServiceAccount 的访问权限。
	// 未来可以根据 Pod 是否实际挂载了 ServiceAccount token 或 PodCertificate 来限制此访问。
	if len(pod.Spec.ServiceAccountName) > 0 {
		serviceAccountVertex := g.getOrCreateVertexLocked(serviceAccountVertexType, pod.Namespace, pod.Spec.ServiceAccountName)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(serviceAccountVertex, podVertex, nodeVertex)
	}

	podutil.VisitPodSecretNames(pod, func(secret string) bool {
		secretVertex := g.getOrCreateVertexLocked(secretVertexType, pod.Namespace, secret)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(secretVertex, podVertex, nodeVertex)
		return true
	})

	podutil.VisitPodConfigmapNames(pod, func(configmap string) bool {
		configmapVertex := g.getOrCreateVertexLocked(configMapVertexType, pod.Namespace, configmap)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(configmapVertex, podVertex, nodeVertex)
		return true
	})

	for _, v := range pod.Spec.Volumes {
		claimName := ""
		if v.PersistentVolumeClaim != nil {
			claimName = v.PersistentVolumeClaim.ClaimName
		} else if v.Ephemeral != nil {
			claimName = ephemeral.VolumeClaimName(pod, &v)
		}
		if claimName != "" {
			pvcVertex := g.getOrCreateVertexLocked(pvcVertexType, pod.Namespace, claimName)
			// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
			g.addEdgeLocked(pvcVertex, podVertex, nodeVertex)
		}
	}

	// 授予 kubelet 访问 Pod 引用或为其创建的所有 ResourceClaim（包括扩展资源 claim）。
	// PodClaims 会跳过尚未创建或故意不创建的 claim（因为不需要）。
	for claimName := range resourceclaim.PodClaims(pod) {
		claimVertex := g.getOrCreateVertexLocked(resourceClaimVertexType, pod.Namespace, claimName)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(claimVertex, podVertex, nodeVertex)
	}
}

// 必须在持有写锁时调用。
// addEdgeLocked 是添加边的唯一入口，不能直接调用 g.graph.SetEdge。
// 如果 destination 非空，创建带目标信息的 destinationEdge（用于快速查找）。
// 注意：如果 from 属于 vertexTypeWithAuthoritativeIndex，则 destination 必须非 nil。
func (g *Graph) addEdgeLocked(from, to, destination *namedVertex) {
	if destination != nil {
		e := newDestinationEdge(from, to, destination)
		g.graph.SetEdge(e)
		g.addEdgeToDestinationIndexLocked(e)
		return
	}

	// 不应该为需要维护权威索引的顶点创建不带 Node 标签的边。
	// 如果进入此分支，说明 Node 授权器有 bug。
	if vertexTypeWithAuthoritativeIndex[from.vertexType] {
		panic(fmt.Sprintf("vertex of type %q must have destination edges only", vertexTypes[from.vertexType]))
	}
	g.graph.SetEdge(simple.Edge{F: from, T: to})
}

// DeletePod 移除指定 Pod 顶点及其关联边，并清理因此孤立的资源顶点。
func (g *Graph) DeletePod(name, namespace string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("DeletePod").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()
	// 删除 Pod 顶点时，会自动清理指向它的 secret/configmap/pvc 等边。
	// 如果这些上游顶点因此变成孤立顶点（度数为 1），也会被级联删除。
	g.deleteVertexLocked(podVertexType, namespace, name)
}

// AddPodCertificateRequest 将 PodCertificateRequest 添加到图中。
//
// PCR 在图中有两条可能的边：
//
//   - PCR -> Pod (-> Node)
//   - PCR -> Node
//
// 只添加直接的 PCR -> Node 边，因为授权检查已经足够，且图遍历路径更短。
// noderestriction 准入插件 确保所有 PCR 都有有效的 node、serviceaccount 和 pod 组合。
func (g *Graph) AddPodCertificateRequest(pcr *certsv1.PodCertificateRequest) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("AddPodCertificateRequest").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()

	g.deleteVertexLocked(pcrVertexType, pcr.ObjectMeta.Namespace, pcr.ObjectMeta.Name)
	pcrVertex := g.getOrCreateVertexLocked(pcrVertexType, pcr.ObjectMeta.Namespace, pcr.ObjectMeta.Name)
	nodeVertex := g.getOrCreateVertexLocked(nodeVertexType, "", string(pcr.Spec.NodeName))
	g.addEdgeLocked(pcrVertex, nodeVertex, nodeVertex)
}

// DeletePodCertificateRequest 从图中移除 PodCertificateRequest。
func (g *Graph) DeletePodCertificateRequest(pcr *certsv1.PodCertificateRequest) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("DeletePodCertificateRequest").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()
	g.deleteVertexLocked(pcrVertexType, pcr.ObjectMeta.Namespace, pcr.ObjectMeta.Name)
}

// AddPV 将 PV 及其引用的资源添加到图中。
// 建立以下边关系：
//
//	secret -> pv
//	pv -> pvc
//
// 由于 PV 可能在其绑定的 PVC->Pod->Node 链路建立之前就被添加，
// 这些边不携带 Node 信息（destination 为 nil）。
func (g *Graph) AddPV(pv *corev1.PersistentVolume) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("AddPV").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()

	// 清除现有边，因为 PV 的 ClaimRef 或 Secret 引用可能已变更
	g.deleteVertexLocked(pvVertexType, "", pv.Name)

	// 如果 PV 绑定了 PVC，建立新边
	if pv.Spec.ClaimRef != nil {
		pvVertex := g.getOrCreateVertexLocked(pvVertexType, "", pv.Name)

		// 因为我们不知道 pvc -> pod -> node 链路的另一端（或者它可能还不存在），
		// 所以无法在这些边上装饰 Kubernetes Node 信息（destination 为 nil）。
		g.addEdgeLocked(pvVertex, g.getOrCreateVertexLocked(pvcVertexType, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name), nil)
		pvutil.VisitPVSecretNames(pv, func(namespace, secret string, kubeletVisible bool) bool {
			// 只授予 kubelet 可见的 secret 的访问权限（在 PVC 所在命名空间中）
			if kubeletVisible {
				// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
				g.addEdgeLocked(g.getOrCreateVertexLocked(secretVertexType, namespace, secret), pvVertex, nil)
			}
			return true
		})
	}
}

// DeletePV 移除指定 PV 顶点及其关联边，并清理因此孤立的资源顶点。
func (g *Graph) DeletePV(name string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("DeletePV").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()
	g.deleteVertexLocked(pvVertexType, "", name)
}

// AddVolumeAttachment 将 VolumeAttachment 添加到图中。
// 建立以下边关系：
//
//	volume attachment -> node
func (g *Graph) AddVolumeAttachment(attachmentName, nodeName string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("AddVolumeAttachment").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()

	// 清除现有边，因为 VolumeAttachment 可能已变更节点
	g.deleteVertexLocked(vaVertexType, "", attachmentName)

	// 如果有节点，建立新边
	if len(nodeName) > 0 {
		vaVertex := g.getOrCreateVertexLocked(vaVertexType, "", attachmentName)
		nodeVertex := g.getOrCreateVertexLocked(nodeVertexType, "", nodeName)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(vaVertex, nodeVertex, nodeVertex)
	}
}

// DeleteVolumeAttachment 移除指定 VolumeAttachment 顶点及其到 Node 的边。
func (g *Graph) DeleteVolumeAttachment(name string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("DeleteVolumeAttachment").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()
	g.deleteVertexLocked(vaVertexType, "", name)
}

// AddResourceSlice 将 ResourceSlice 添加到图中。
// 建立以下边关系：
//
//	node resource slice -> node
func (g *Graph) AddResourceSlice(sliceName, nodeName string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("AddResourceSlice").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()

	// 清除现有边
	g.deleteVertexLocked(sliceVertexType, "", sliceName)

	// 如果有节点，建立新边
	if len(nodeName) > 0 {
		sliceVertex := g.getOrCreateVertexLocked(sliceVertexType, "", sliceName)
		nodeVertex := g.getOrCreateVertexLocked(nodeVertexType, "", nodeName)
		// 添加边必须通过 addEdgeLocked，不能直接调用 g.graph.SetEdge。
		g.addEdgeLocked(sliceVertex, nodeVertex, nodeVertex)
	}
}

// DeleteResourceSlice 移除指定 ResourceSlice 顶点及其到 Node 的边。
func (g *Graph) DeleteResourceSlice(sliceName string) {
	start := time.Now()
	defer func() {
		graphActionsDuration.WithLabelValues("DeleteResourceSlice").Observe(time.Since(start).Seconds())
	}()
	g.lock.Lock()
	defer g.lock.Unlock()
	g.deleteVertexLocked(sliceVertexType, "", sliceName)
}
