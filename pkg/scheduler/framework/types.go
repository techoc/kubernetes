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

// Package framework 提供调度框架的核心类型与辅助函数。
//
// 本文件定义了调度器缓存与调度队列共用的聚合信息结构（NodeInfo、PodInfo、
// QueuedPodInfo、QueuedPodGroupInfo 等），它们把 API 对象加工成便于调度决策的
// 形态——例如预先解析亲和性选择器、缓存资源请求量。命名空间的
// framework 接口本身由 k8s.io/kube-scheduler/framework 提供（此处别名为 fwk）。
package framework

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

var generation int64

var (
	// basicActionTypes 是所有资源共用的基本动作类型。
	basicActionTypes = []fwk.ActionType{fwk.Add, fwk.Delete, fwk.Update}
	// podActionTypes 是仅适用于 Pod 事件的动作类型。
	podActionTypes = []fwk.ActionType{fwk.UpdatePodLabel, fwk.UpdatePodScaleDown, fwk.UpdatePodScaleUp, fwk.UpdatePodToleration, fwk.UpdatePodSchedulingGatesEliminated, fwk.UpdatePodGeneratedResourceClaim}
	// nodeActionTypes 是仅适用于 Node 事件的动作类型。
	nodeActionTypes = []fwk.ActionType{fwk.UpdateNodeAllocatable, fwk.UpdateNodeLabel, fwk.UpdateNodeTaint, fwk.UpdateNodeCondition, fwk.UpdateNodeAnnotation, fwk.UpdateNodeDeclaredFeature, fwk.UpdateNodePreemptionPolicy}
)

var (
	// allResources 列出会产生集群事件的全部资源类型，用于生成指标标签。
	allResources = []fwk.EventResource{
		fwk.Pod,
		fwk.AssignedPod,
		fwk.UnscheduledPod,
		fwk.TargetPod,
		fwk.Node,
		fwk.PersistentVolume,
		fwk.PersistentVolumeClaim,
		fwk.CSINode,
		fwk.CSIDriver,
		fwk.CSIStorageCapacity,
		fwk.StorageClass,
		fwk.VolumeAttachment,
		fwk.ResourceClaim,
		fwk.ResourceSlice,
		fwk.DeviceClass,
		fwk.PodGroup,
	}
)

// AllClusterEventLabels 返回指标中所有可能出现的集群事件标签。
// 由资源与基本动作的组合、以及 Pod/Node 特有的动作组成。
func AllClusterEventLabels() []string {
	labels := []string{UnschedulableTimeout, ForceActivate}
	for _, r := range allResources {
		for _, a := range basicActionTypes {
			labels = append(labels, fwk.ClusterEvent{Resource: r, ActionType: a}.Label())
		}
	}
	for _, a := range podActionTypes {
		labels = append(labels, fwk.ClusterEvent{Resource: fwk.Pod, ActionType: a}.Label())
	}
	for _, a := range nodeActionTypes {
		labels = append(labels, fwk.ClusterEvent{Resource: fwk.Node, ActionType: a}.Label())
	}
	return labels
}

// ClusterEventIsWildCard 判断给定集群事件是否遵循通配符语义（资源为 "*" 且动作为 All）。
func ClusterEventIsWildCard(ce fwk.ClusterEvent) bool {
	return ce.Resource == fwk.WildCard && ce.ActionType == fwk.All
}

// MatchClusterEvents 判断 ce 是否匹配 incomingEvent。
// "匹配"指 incomingEvent 与 ce 相同、或比 ce 更具体。
// 例如 ce.ActionType 为 Update、incomingEvent.ActionType 为 UpdateNodeLabel 时返回 true，
// 因为 UpdateNodeLabel 比 Update 更具体；反之则返回 false。
// 这基于调度器会把到来的集群事件尽可能解释为具体事件这一前提：
// 若 incomingEvent 是 Node/Update，就表示这次 Node 更新无法归入任何具体的
// Node 更新事件。
//
// 若 ce.Resource 为 "*"，对 incomingEvent.Resource 没有要求；
// 反过来若 incomingEvent.Resource 为 "*"，只有 ce.Resource 为 "*" 才接受
// （当前调度队列实现中不应出现这种情况）。
//
// 注意：通配符事件是个特例——它会强制所有 Pod 进入 activeQ/backoffQ，
// 但除非 ce 本身也是通配符事件，否则这里仍将其视为不匹配。
func MatchClusterEvents(ce, incomingEvent fwk.ClusterEvent) bool {
	return ClusterEventIsWildCard(ce) ||
		matchEventResources(ce.Resource, incomingEvent.Resource) && ce.ActionType&incomingEvent.ActionType != 0 && incomingEvent.ActionType <= ce.ActionType
}

// matchEventResources 判断 r 是否与到来的资源匹配。
func matchEventResources(r, resource fwk.EventResource) bool {
	// 通配符匹配所有资源。
	return r == fwk.WildCard ||
		// 精确匹配。
		r == resource ||
		// Pod 可以匹配 AssignedPod、UnscheduledPod 或 TargetPod。
		r == fwk.Pod && (resource == fwk.AssignedPod || resource == fwk.UnscheduledPod || resource == fwk.TargetPod)
}

// MatchAnyClusterEvent 判断 ce 是否匹配 incomingEvents 中的任一事件。
func MatchAnyClusterEvent(ce fwk.ClusterEvent, incomingEvents []fwk.ClusterEvent) bool {
	for _, e := range incomingEvents {
		if MatchClusterEvents(e, ce) {
			return true
		}
	}
	return false
}

// UnrollWildCardResource 将通配符资源展开为各具体资源的事件列表。
// GenericWorkload 启用时额外包含 PodGroup。
func UnrollWildCardResource() []fwk.ClusterEventWithHint {
	events := []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIDriver, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIStorageCapacity, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceClaim, ActionType: fwk.All}},
		{Event: fwk.ClusterEvent{Resource: fwk.DeviceClass, ActionType: fwk.All}},
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		events = append(events, fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.All}})
	}
	return events
}

// UnrollPodEvent 把资源为 Pod 的集群事件拆成三个事件：
// AssignedPod、UnscheduledPod 和 TargetPod。
// 调用方需保证 event.Resource == fwk.Pod。
func UnrollPodEvent(event fwk.ClusterEvent) []fwk.ClusterEvent {
	return []fwk.ClusterEvent{
		{Resource: fwk.AssignedPod, ActionType: event.ActionType},
		{Resource: fwk.UnscheduledPod, ActionType: event.ActionType},
		{Resource: fwk.TargetPod, ActionType: event.ActionType},
	}
}

// NodeInfo 是节点级别的聚合信息，缓存调度决策所需的节点状态。
type NodeInfo struct {
	// 节点原始 API 对象。
	node *v1.Node

	// 运行在该节点上的 Pod。
	Pods []fwk.PodInfo

	// 带有亲和性的 Pod 子集。
	PodsWithAffinity []fwk.PodInfo

	// 带有必需反亲和性的 Pod 子集。
	PodsWithRequiredAntiAffinity []fwk.PodInfo

	// 带有非主机名作用域必需反亲和性（topologyKey 不是 hostname）的 Pod 子集。
	// InterPodAffinityHostnameFastPath 特性门控关闭时该切片必须为空。
	PodsWithRequiredNonHostScopedAntiAffinity []fwk.PodInfo

	// 节点上已分配的端口。
	UsedPorts fwk.HostPortInfo

	// 节点上所有 Pod 的资源请求总量。包含 assumed Pod——调度器已发出绑定请求、
	// 但可能尚未完成调度的 Pod。
	Requested *Resource
	// 节点上所有 Pod 的资源请求总量，但每个容器的 CPU/内存请求会被施加一个最小值。
	// 它不反映节点的真实资源请求，而是为了避免把大量零请求 Pod 堆到同一节点上。
	NonZeroRequested *Resource
	// 显式以 int64 存储可分配资源（Node.Status.Allocatable.*），
	// 避免每次做类型转换和访问 map。
	Allocatable *Resource

	// ImageStates 仅在镜像确实位于该节点上时保存对应条目，
	// 可用于检查镜像是否存在，以及基于镜像状态实现高级策略（如镜像局部性调度）。
	ImageStates map[string]*fwk.ImageStateSummary

	// PVCRefCounts 记录 PVC 名称到节点上使用它的 Pod 数量的映射。
	// 键格式为 "namespace/name"。
	PVCRefCounts map[string]int

	// NodeInfo 每次变更都会递增 generation，
	// 以便对象未变化时跳过克隆。
	Generation int64

	// DeclaredFeatures 是节点声明的特性集合。
	DeclaredFeatures ndf.FeatureSet
}

// GetPods 返回节点上的全部 Pod 信息。
func (n *NodeInfo) GetPods() []fwk.PodInfo {
	return n.Pods
}

// GetPodsWithAffinity 返回带亲和性或反亲和性的 Pod 信息。
func (n *NodeInfo) GetPodsWithAffinity() []fwk.PodInfo {
	return n.PodsWithAffinity
}

// GetPodsWithRequiredAntiAffinity 返回带必需反亲和性的 Pod 信息。
func (n *NodeInfo) GetPodsWithRequiredAntiAffinity() []fwk.PodInfo {
	return n.PodsWithRequiredAntiAffinity
}

// GetPodsWithRequiredNonHostScopedAntiAffinity 返回带非主机名作用域必需反亲和性的 Pod 信息。
func (n *NodeInfo) GetPodsWithRequiredNonHostScopedAntiAffinity() []fwk.PodInfo {
	return n.PodsWithRequiredNonHostScopedAntiAffinity
}

// GetUsedPorts 返回节点上已分配端口的信息。
func (n *NodeInfo) GetUsedPorts() fwk.HostPortInfo {
	return n.UsedPorts
}

// GetRequested 返回节点上 Pod 的资源请求总量。
func (n *NodeInfo) GetRequested() fwk.Resource {
	return n.Requested
}

// GetNonZeroRequested 返回施加最小请求值后的资源请求总量。
func (n *NodeInfo) GetNonZeroRequested() fwk.Resource {
	return n.NonZeroRequested
}

// GetAllocatable 返回节点的可分配资源。
func (n *NodeInfo) GetAllocatable() fwk.Resource {
	return n.Allocatable
}

// GetImageStates 返回节点上的镜像状态信息。
func (n *NodeInfo) GetImageStates() map[string]*fwk.ImageStateSummary {
	return n.ImageStates
}

// GetPVCRefCounts 返回 PVC 被节点上 Pod 引用的次数。
func (n *NodeInfo) GetPVCRefCounts() map[string]int {
	return n.PVCRefCounts
}

// GetGeneration 返回 NodeInfo 的版本号，每次变更都会递增。
func (n *NodeInfo) GetGeneration() int64 {
	return n.Generation
}

// GetNodeDeclaredFeatures 返回节点声明的特性集合。
func (n *NodeInfo) GetNodeDeclaredFeatures() ndf.FeatureSet {
	return n.DeclaredFeatures
}

// NodeInfo 实现 klog.KMetadata，因此 klog.KObjSlice(nodes) 可以直接处理 []*NodeInfo。
var _ klog.KMetadata = &NodeInfo{}

// GetName 返回 NodeInfo 包裹的节点名称；节点或节点名为 nil 时返回可读的占位值。
// 该方法属于 klog.KMetadata 接口。
func (n *NodeInfo) GetName() string {
	if n == nil {
		return "<nil>"
	}
	if n.node == nil {
		return "<no node>"
	}
	return n.node.Name
}

// GetNamespace 属于 klog.KMetadata 接口。Node 不是命名空间级资源，这里始终返回空字符串。
func (n *NodeInfo) GetNamespace() string {
	return ""
}

// Node 返回该节点的原始 API 对象。
func (n *NodeInfo) Node() *v1.Node {
	if n == nil {
		return nil
	}
	return n.node
}

// Snapshot 与 SnapshotConcrete 一样返回节点副本，但返回类型为 fwk.NodeInfo，
// 用于让 NodeInfo 实现 fwk.NodeInfo 接口。
func (n *NodeInfo) Snapshot() fwk.NodeInfo {
	return n.SnapshotConcrete()
}

// SnapshotConcrete 返回节点的副本；其中 ImageStates 被复制但不带 Nodes 字段。
func (n *NodeInfo) SnapshotConcrete() *NodeInfo {
	clone := &NodeInfo{
		node:             n.node,
		Requested:        n.Requested.Clone(),
		NonZeroRequested: n.NonZeroRequested.Clone(),
		Allocatable:      n.Allocatable.Clone(),
		UsedPorts:        make(fwk.HostPortInfo),
		ImageStates:      make(map[string]*fwk.ImageStateSummary),
		PVCRefCounts:     make(map[string]int),
		Generation:       n.Generation,
		DeclaredFeatures: n.DeclaredFeatures.Clone(),
	}
	if len(n.Pods) > 0 {
		clone.Pods = append([]fwk.PodInfo(nil), n.Pods...)
	}
	if len(n.UsedPorts) > 0 {
		// HostPortInfo 是嵌套 map，必须逐层复制，浅拷贝会共享底层 map。
		for ip, portMap := range n.UsedPorts {
			clone.UsedPorts[ip] = make(map[fwk.ProtocolPort]struct{})
			for protocolPort, v := range portMap {
				clone.UsedPorts[ip][protocolPort] = v
			}
		}
	}
	if len(n.PodsWithAffinity) > 0 {
		clone.PodsWithAffinity = append([]fwk.PodInfo(nil), n.PodsWithAffinity...)
	}
	if len(n.PodsWithRequiredAntiAffinity) > 0 {
		clone.PodsWithRequiredAntiAffinity = append([]fwk.PodInfo(nil), n.PodsWithRequiredAntiAffinity...)
	}
	if len(n.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
		clone.PodsWithRequiredNonHostScopedAntiAffinity = append([]fwk.PodInfo(nil), n.PodsWithRequiredNonHostScopedAntiAffinity...)
	}
	if len(n.ImageStates) > 0 {
		state := make(map[string]*fwk.ImageStateSummary, len(n.ImageStates))
		for imageName, imageState := range n.ImageStates {
			state[imageName] = imageState.Snapshot()
		}
		clone.ImageStates = state
	}
	for key, value := range n.PVCRefCounts {
		clone.PVCRefCounts[key] = value
	}

	return clone
}

// String 返回 NodeInfo 的可读表示。
func (n *NodeInfo) String() string {
	podKeys := make([]string, len(n.Pods))
	for i, p := range n.Pods {
		podKeys[i] = p.GetPod().Name
	}
	return fmt.Sprintf("&NodeInfo{Pods:%v, RequestedResource:%#v, NonZeroRequest: %#v, UsedPort: %#v, AllocatableResource:%#v}",
		podKeys, n.Requested, n.NonZeroRequested, n.UsedPorts, n.Allocatable)
}

// AddPodInfo 把 Pod 信息加入 NodeInfo。
// 已经有 PodInfo 时优先用本方法，避免重复计算亲和性选择器。
func (n *NodeInfo) AddPodInfo(podInfo fwk.PodInfo) {
	n.Pods = append(n.Pods, podInfo)
	if podWithAffinity(podInfo.GetPod()) {
		n.PodsWithAffinity = append(n.PodsWithAffinity, podInfo)
	}
	if podWithRequiredAntiAffinity(podInfo.GetPod()) {
		n.PodsWithRequiredAntiAffinity = append(n.PodsWithRequiredAntiAffinity, podInfo)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InterPodAffinityHostnameFastPath) && podWithRequiredNonHostScopedAntiAffinity(podInfo.GetPod()) {
		n.PodsWithRequiredNonHostScopedAntiAffinity = append(n.PodsWithRequiredNonHostScopedAntiAffinity, podInfo)
	}
	n.update(podInfo, 1)
}

// AddPod 是 AddPodInfo 的包装，从 Pod 对象现场构造 PodInfo。
func (n *NodeInfo) AddPod(pod *v1.Pod) {
	// 忽略解析错误：apiserver 对亲和性条款的校验并不完备，
	// 而为了向后兼容也无法修正该校验逻辑。
	podInfo, _ := NewPodInfo(pod)
	n.AddPodInfo(podInfo)
}

func podWithAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && (affinity.PodAffinity != nil || affinity.PodAntiAffinity != nil)
}

func podWithRequiredAntiAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && affinity.PodAntiAffinity != nil &&
		len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0
}

func podWithRequiredNonHostScopedAntiAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	if affinity != nil && affinity.PodAntiAffinity != nil {
		for _, term := range affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if term.TopologyKey != v1.LabelHostname {
				return true
			}
		}
	}
	return false
}

func removeFromSlice(logger klog.Logger, s []fwk.PodInfo, k string) ([]fwk.PodInfo, fwk.PodInfo) {
	var removedPod fwk.PodInfo
	for i := range s {
		tmpKey, err := GetPodKey(s[i].GetPod())
		if err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Cannot get pod key", "pod", klog.KObj(s[i].GetPod()))
			continue
		}
		if k == tmpKey {
			removedPod = s[i]
			// 用末尾元素覆盖待删元素，避免移动整个切片。
			s[i] = s[len(s)-1]
			// 清空原末尾引用，避免 PodInfo 被切片底层数组继续持有。
			s[len(s)-1] = nil
			s = s[:len(s)-1]
			break
		}
	}
	// 空切片统一重置为 nil，便于单测中做 DeepEqual 比较。
	if len(s) == 0 {
		return nil, removedPod
	}
	return s, removedPod
}

// RemovePod 从 NodeInfo 中扣除指定 Pod 的信息。
func (n *NodeInfo) RemovePod(logger klog.Logger, pod *v1.Pod) error {
	k, err := GetPodKey(pod)
	if err != nil {
		return err
	}
	if podWithAffinity(pod) {
		n.PodsWithAffinity, _ = removeFromSlice(logger, n.PodsWithAffinity, k)
	}
	if podWithRequiredAntiAffinity(pod) {
		n.PodsWithRequiredAntiAffinity, _ = removeFromSlice(logger, n.PodsWithRequiredAntiAffinity, k)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InterPodAffinityHostnameFastPath) && podWithRequiredNonHostScopedAntiAffinity(pod) {
		n.PodsWithRequiredNonHostScopedAntiAffinity, _ = removeFromSlice(logger, n.PodsWithRequiredNonHostScopedAntiAffinity, k)
	}

	var removedPod fwk.PodInfo
	if n.Pods, removedPod = removeFromSlice(logger, n.Pods, k); removedPod != nil {
		n.update(removedPod, -1)
		return nil
	}
	return fmt.Errorf("no corresponding pod %s in pods of node %s", pod.Name, n.node.Name)
}

// update 根据 Pod 及其符号更新节点聚合信息。
// AddPod 传入 `+1`，RemovePod 传入 `-1`。
func (n *NodeInfo) update(podInfo fwk.PodInfo, sign int64) {
	podResource := podInfo.CalculateResource()
	n.Requested.MilliCPU += sign * podResource.Resource.GetMilliCPU()
	n.Requested.Memory += sign * podResource.Resource.GetMemory()
	n.Requested.EphemeralStorage += sign * podResource.Resource.GetEphemeralStorage()
	if n.Requested.ScalarResources == nil && len(podResource.Resource.GetScalarResources()) > 0 {
		n.Requested.ScalarResources = map[v1.ResourceName]int64{}
	}
	for rName, rQuant := range podResource.Resource.GetScalarResources() {
		n.Requested.ScalarResources[rName] += sign * rQuant
	}
	n.NonZeroRequested.MilliCPU += sign * podResource.Non0CPU
	n.NonZeroRequested.Memory += sign * podResource.Non0Mem

	// 添加 Pod 时占用端口，移除 Pod 时释放端口。
	n.updateUsedPorts(podInfo.GetPod(), sign > 0)
	n.updatePVCRefCounts(podInfo.GetPod(), sign > 0)

	n.Generation = nextGeneration()
}

// updateUsedPorts 更新 NodeInfo 的 UsedPorts。
func (n *NodeInfo) updateUsedPorts(pod *v1.Pod, add bool) {
	for _, port := range schedutil.GetHostPorts(pod) {
		if add {
			n.UsedPorts.Add(port.HostIP, string(port.Protocol), port.HostPort)
		} else {
			n.UsedPorts.Remove(port.HostIP, string(port.Protocol), port.HostPort)
		}
	}
}

// updatePVCRefCounts 更新 NodeInfo 的 PVCRefCounts。
func (n *NodeInfo) updatePVCRefCounts(pod *v1.Pod, add bool) {
	for key := range PodPVCKeys(pod) {
		if add {
			n.PVCRefCounts[key] += 1
		} else {
			n.PVCRefCounts[key] -= 1
			if n.PVCRefCounts[key] <= 0 {
				delete(n.PVCRefCounts, key)
			}
		}
	}
}

// SetNode 设置节点原始对象，并同步刷新可分配资源与声明特性。
func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Allocatable = NewResource(node.Status.Allocatable)
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		// 用 TryMap 而非 Map，以容忍节点上存在未知特性：
		// 我们只关心与已知特性的匹配，丢弃未知特性没有风险。
		n.DeclaredFeatures = ndf.DefaultFramework.TryMap(node.Status.DeclaredFeatures)
	}
	n.Generation = nextGeneration()
}

// RemoveNode 清除节点对象，但保留其余聚合信息（Pod、资源请求等）。
func (n *NodeInfo) RemoveNode() {
	n.node = nil
	n.Generation = nextGeneration()
}

// nextGeneration 单调递增地分配版本号，确保不同版本绝不重复。
// 版本号碰撞在节点被删除后以同名重新加入时尤其危险。见 issue#63262。
func nextGeneration() int64 {
	return atomic.AddInt64(&generation, 1)
}

// QueuedEntityInfo 表示调度队列中一个可调度实体，
// 可以是单个 Pod（QueuedPodInfo），也可以是一组 Pod（QueuedPodGroupInfo）。
type QueuedEntityInfo interface {
	fwk.QueuedEntityInfo
	// GetName 返回实体的名称。
	GetName() string
	// GetNamespace 返回实体的命名空间。
	GetNamespace() string
	// ForEachPodInfo 遍历实体中的所有 QueuedPodInfo 并执行 fn。
	// fn 返回 false 时提前结束遍历。
	ForEachPodInfo(fn func(pInfo *QueuedPodInfo) bool)
	// Update 更新实体中指定的 Pod，并返回更新后的 QueuedPodInfo。
	Update(pod *v1.Pod) (*QueuedPodInfo, error)
	// Gated 返回实体是否被某个插件在 PreEnqueue 阶段拦截。
	Gated() bool
	// Size 返回实体中的 Pod 数量。
	Size() int
	// IncAttempts 递增 QueueingParams 中的调度尝试次数。
	IncAttempts()
	// SetInitialAttemptTimestamp 设置 QueueingParams 中的首次尝试时间戳。
	SetInitialAttemptTimestamp(t time.Time)
	// SetWasFlushedFromUnschedulable 设置 QueueingParams 中的 WasFlushedFromUnschedulable 标记。
	SetWasFlushedFromUnschedulable(flushed bool)
	// SetBackoffExpiration 设置 QueueingParams 中的退避到期时间。
	SetBackoffExpiration(t time.Time)
	// SetGatingPlugin 设置 QueueingParams 中的拦截插件及其关注事件。
	SetGatingPlugin(gatingPlugin string, gatingEvents []fwk.ClusterEvent)
	// GetFlushTimestamp 返回 QueueingParams 中的最近刷新时间。
	GetFlushTimestamp() time.Time
	// SetFlushTimestamp 设置 QueueingParams 中的最近刷新时间。
	SetFlushTimestamp(t time.Time)
}

// QueueingParams 保存实体（Pod 或 PodGroup）在调度队列中的排队状态与历史信息。
//
// 注意给实现者：请在更上层的结构（如 QueuedPodGroupInfo 或 QueuedPodInfo）
// 上定义 setter，不要直接给 QueueingParams 定义 setter。
type QueueingParams struct {
	// 实体加入调度队列的时间。
	Timestamp time.Time
	// 调度成功前的全部调度尝试次数，用于记录 attempts 指标。
	Attempts int
	// BackoffExpiration 是实体完成退避的时间。
	// 属于某个 PodGroup 的 Pod 该字段为空，改用 QueuedPodGroupInfo 的 BackoffExpiration。
	// SchedulerPopFromBackoffQ 启用时该值会对齐到退避排序窗口；
	// 如此，BackoffExpiration 相同（同一时间桶）的实体会先按优先级、再按时间戳排序，
	// 保证从 backoffQ 弹出时会考虑即将到期实体的优先级。
	BackoffExpiration time.Time
	// 实体被判定为 Unschedulable 的调度尝试总次数。
	// 通常等于 Attempts，但实体因 Error 状态（如网络错误）失败时不会递增。
	// 用于计算重试前必须经历的退避时间。
	UnschedulableCount int
	// 实体连续收到 Error 状态的次数，收到非 Error 状态时清零。
	//
	// 返回 Error（如 kube-apiserver 不稳定）时不宜立即重试，因此需要退避机制，
	// 否则会给 kube-apiserver 带来更大压力。
	// 但这类退避不应按普通 Unschedulable 的方式计算，因为目的不同：
	// Unschedulable 等状态的退避是对浪费调度周期的惩罚，
	// 而 Error 状态的退避是为了保护 kube-apiserver。
	// 因此需要用 ConsecutiveErrorsCount 与 UnschedulableCount 分别统计。
	// 相关讨论见 https://github.com/kubernetes/kubernetes/issues/128744。
	ConsecutiveErrorsCount int
	// 记录实体最近一次是否因超时（而非事件）从 unschedulableEntities 周期性刷新进入 activeQ。
	// 用于发现刷新后很快变为可调度的实体——这可能意味着队列提示配置有误或事件处理有 bug。
	// 实体因任何原因重新入队时该标记都会被清除。
	WasFlushedFromUnschedulable bool
	// 记录实体最近一次从 unschedulable 队列刷新的时间。
	FlushTimestamp time.Time
	// 实体首次加入活跃队列的时间。实体在调度成功前可能被多次重新入队。
	// 初始化后不应再更新，用于统计实体的端到端调度延迟。
	InitialAttemptTimestamp *time.Time
	// 记录在 PreFilter、Filter、Reserve 或 Permit（WaitOnPermit）扩展点
	// 返回 Unschedulable 或 UnschedulableAndUnresolvable 的插件名。
	// 若实体在其他扩展点被拒绝，会被当作意外错误（如临时网络问题、插件实现问题等），
	// 在短暂退避后重试——因为这类失败无需新集群事件也可能自行恢复。
	UnschedulablePlugins sets.Set[string]
	// 记录实体收到 Pending 状态的插件名。
	PendingPlugins sets.Set[string]
	// 记录在 PreEnqueue 阶段拦截实体的插件名。
	GatingPlugin string
	// 记录拦截插件注册的事件，作为缓存避免重复计算哪些事件可能解除拦截。
	GatingPluginEvents []fwk.ClusterEvent
}

// GetTimestamp 返回实体的入队时间。
func (qp *QueueingParams) GetTimestamp() time.Time {
	return qp.Timestamp
}

// GetAttempts 返回实体的调度尝试次数。
func (qp *QueueingParams) GetAttempts() int {
	return qp.Attempts
}

// GetBackoffExpiration 返回实体的退避到期时间。
func (qp *QueueingParams) GetBackoffExpiration() time.Time {
	return qp.BackoffExpiration
}

// GetUnschedulableCount 返回实体被判定为 Unschedulable 的次数。
func (qp *QueueingParams) GetUnschedulableCount() int {
	return qp.UnschedulableCount
}

// GetConsecutiveErrorsCount 返回实体连续收到 Error 状态的次数。
func (qp *QueueingParams) GetConsecutiveErrorsCount() int {
	return qp.ConsecutiveErrorsCount
}

// GetInitialAttemptTimestamp 返回实体首次尝试调度的时间。
func (qp *QueueingParams) GetInitialAttemptTimestamp() *time.Time {
	return qp.InitialAttemptTimestamp
}

// GetUnschedulablePlugins 返回判定实体 Unschedulable 的插件集合。
func (qp *QueueingParams) GetUnschedulablePlugins() sets.Set[string] {
	return qp.UnschedulablePlugins
}

// GetPendingPlugins 返回判定实体 Pending 的插件集合。
func (qp *QueueingParams) GetPendingPlugins() sets.Set[string] {
	return qp.PendingPlugins
}

// GetGatingPlugin 返回在 PreEnqueue 阶段拦截实体的插件名。
func (qp *QueueingParams) GetGatingPlugin() string {
	return qp.GatingPlugin
}

// GetGatingPluginEvents 返回拦截插件注册的事件列表。
func (qp *QueueingParams) GetGatingPluginEvents() []fwk.ClusterEvent {
	return qp.GatingPluginEvents
}

// GetFlushTimestamp 返回实体最近一次从 unschedulable 队列刷新的时间。
func (qp *QueueingParams) GetFlushTimestamp() time.Time {
	return qp.FlushTimestamp
}

// DeepCopy 返回 QueueingParams 的深拷贝，其中集合与事件切片都会复制。
func (qp *QueueingParams) DeepCopy() *QueueingParams {
	return &QueueingParams{
		Timestamp:                   qp.Timestamp,
		FlushTimestamp:              qp.FlushTimestamp,
		Attempts:                    qp.Attempts,
		UnschedulableCount:          qp.UnschedulableCount,
		InitialAttemptTimestamp:     qp.InitialAttemptTimestamp,
		BackoffExpiration:           qp.BackoffExpiration,
		UnschedulablePlugins:        qp.UnschedulablePlugins.Clone(),
		PendingPlugins:              qp.PendingPlugins.Clone(),
		GatingPlugin:                qp.GatingPlugin,
		GatingPluginEvents:          slices.Clone(qp.GatingPluginEvents),
		ConsecutiveErrorsCount:      qp.ConsecutiveErrorsCount,
		WasFlushedFromUnschedulable: qp.WasFlushedFromUnschedulable,
	}
}

// QueuedPodInfo 是 Pod 的包装，附带其在调度队列中的状态信息，
// 例如入队时间戳、退避到期时间等。
type QueuedPodInfo struct {
	*PodInfo
	QueueingParams
	// 用于机会式批处理（opportunistic batching）的 Pod 签名。
	PodSignature fwk.PodSignature
}

// Type 返回实体键类型，即 Pod 类型。
func (pqi *QueuedPodInfo) Type() fwk.EntityKeyType {
	return fwk.PodKeyType
}

// ForEachPodInfo 对自身执行 fn；单个 Pod 的实体遍历只会回调一次。
func (pqi *QueuedPodInfo) ForEachPodInfo(fn func(pInfo *QueuedPodInfo) bool) {
	_ = fn(pqi)
}

// Update 更新 QueuedPodInfo 中的 Pod，并清除缓存的 PodSignature，
// 因为更新后的 Pod 可能不再匹配旧版本计算出的签名。
func (pqi *QueuedPodInfo) Update(pod *v1.Pod) (*QueuedPodInfo, error) {
	pqi.PodSignature = nil
	err := pqi.PodInfo.Update(pod)
	return pqi, err
}

// GetPodInfo 返回实体的 fwk.PodInfo 接口视图。
func (pqi *QueuedPodInfo) GetPodInfo() fwk.PodInfo {
	return pqi.PodInfo
}

// Gated 返回 Pod 是否被某个插件拦截。
func (pqi *QueuedPodInfo) Gated() bool {
	return pqi.QueueingParams.GatingPlugin != ""
}

// GetPriority 返回 Pod 的优先级。
func (pqi *QueuedPodInfo) GetPriority() int32 {
	return corev1helpers.PodPriority(pqi.GetPod())
}

// DeepCopy 返回 QueuedPodInfo 的深拷贝。
func (pqi *QueuedPodInfo) DeepCopy() *QueuedPodInfo {
	return &QueuedPodInfo{
		PodInfo:        pqi.PodInfo.DeepCopy(),
		QueueingParams: *pqi.QueueingParams.DeepCopy(),
		PodSignature:   pqi.PodSignature,
	}
}

// Size 返回实体中的 Pod 数量，单个 Pod 恒为 1。
func (pqi *QueuedPodInfo) Size() int {
	return 1
}

// IncAttempts 递增调度尝试次数。
func (pqi *QueuedPodInfo) IncAttempts() {
	pqi.Attempts++
}

// SetInitialAttemptTimestamp 设置首次尝试时间；已设置过则保持不变。
func (pqi *QueuedPodInfo) SetInitialAttemptTimestamp(t time.Time) {
	if pqi.InitialAttemptTimestamp == nil {
		pqi.InitialAttemptTimestamp = &t
	}
}

// SetWasFlushedFromUnschedulable 设置"刚从 unschedulable 队列刷新"标记。
func (pqi *QueuedPodInfo) SetWasFlushedFromUnschedulable(flushed bool) {
	pqi.WasFlushedFromUnschedulable = flushed
}

// SetBackoffExpiration 设置退避到期时间。
func (pqi *QueuedPodInfo) SetBackoffExpiration(t time.Time) {
	pqi.BackoffExpiration = t
}

// SetGatingPlugin 记录拦截插件及其注册的事件。
func (pqi *QueuedPodInfo) SetGatingPlugin(gatingPlugin string, gatingEvents []fwk.ClusterEvent) {
	pqi.GatingPlugin = gatingPlugin
	pqi.GatingPluginEvents = gatingEvents
}

// ClearRejectorPlugins 清除记录 Pod 上次被拒绝原因的插件相关字段。
func (pqi *QueuedPodInfo) ClearRejectorPlugins() {
	pqi.QueueingParams.UnschedulablePlugins.Clear()
	pqi.QueueingParams.PendingPlugins.Clear()
	pqi.QueueingParams.GatingPlugin = ""
	pqi.QueueingParams.GatingPluginEvents = nil
}

// SetFlushTimestamp 设置最近一次刷新时间。
func (pqi *QueuedPodInfo) SetFlushTimestamp(t time.Time) {
	pqi.FlushTimestamp = t
}

// QueuedPodGroupInfo 是 PodGroupInfo 的包装，附带 PodGroup 在调度队列中的状态，
// 并保存该 PodGroup 下所有正在排队的 Pod。
type QueuedPodGroupInfo struct {
	*PodGroupInfo
	QueueingParams
	// QueuedPodInfos 是当前排队的 PodGroup 成员 Pod。
	// map 的键为 PodGroup 键，格式与 framework.PodGroupKey 一致；
	// 值为对应叶子 PodGroup 的排队 Pod 切片，切片顺序按优先级和时间戳确定。
	QueuedPodInfos map[fwk.EntityKey][]*QueuedPodInfo
}

// Type 返回实体键类型，即 PodGroup 类型。
func (pgqi *QueuedPodGroupInfo) Type() fwk.EntityKeyType {
	return fwk.PodGroupKeyType
}

// AddPod 在 Pod 属于该 PodGroup 时把它加入队列信息。
// 层级结构下需要定位到对应的叶子 PodGroup。
func (pgqi *QueuedPodGroupInfo) AddPod(pInfo *QueuedPodInfo) {
	if pInfo.Pod.Namespace != pgqi.GetNamespace() {
		return
	}

	if pgqi.GetPodGroup() == nil && pgqi.GetCompositePodGroup() == nil {
		return
	}

	if pInfo.Pod.Spec.SchedulingGroup == nil || pInfo.Pod.Spec.SchedulingGroup.PodGroupName == nil {
		return
	}

	leafPG, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, *pInfo.Pod.Spec.SchedulingGroup.PodGroupName)
	if leafPG == nil {
		return
	}

	if pgqi.QueuedPodInfos == nil {
		pgqi.QueuedPodInfos = make(map[fwk.EntityKey][]*QueuedPodInfo)
	}

	key := fwk.PodGroupKey(leafPG.Namespace, leafPG.Name)
	index, _ := slices.BinarySearchFunc(pgqi.QueuedPodInfos[key], pInfo, PodGroupMemberPodsOrderingFunc)

	pgqi.QueuedPodInfos[key] = slices.Insert(pgqi.QueuedPodInfos[key], index, pInfo)
	leafPG.UnscheduledPods = slices.Insert(leafPG.UnscheduledPods, index, pInfo.Pod)
}

// RemovePod 在 Pod 属于该 PodGroup 时把它从队列信息中移除，返回被移除的 QueuedPodInfo。
// 层级结构下需要同时清理叶子 PodGroup 的 UnscheduledPods。
func (pgqi *QueuedPodGroupInfo) RemovePod(pod *v1.Pod) *QueuedPodInfo {
	if pod.Namespace != pgqi.GetNamespace() {
		return nil
	}

	if pgqi.QueuedPodInfos == nil {
		return nil
	}

	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return nil
	}

	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	list, exists := pgqi.QueuedPodInfos[key]
	if !exists {
		return nil
	}

	var removed *QueuedPodInfo
	for i, p := range list {
		if p.Pod.Name == pod.Name && p.Pod.Namespace == pod.Namespace {
			removed = p
			pgqi.QueuedPodInfos[key] = slices.Delete(list, i, i+1)
			if len(pgqi.QueuedPodInfos[key]) == 0 {
				delete(pgqi.QueuedPodInfos, key)
			}
			break
		}
	}

	// 同步从叶子 PodGroup 的 UnscheduledPods 中移除。
	leafPG, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, *pod.Spec.SchedulingGroup.PodGroupName)
	if leafPG == nil {
		return removed
	}

	for i, p := range leafPG.UnscheduledPods {
		if p.Name == pod.Name && p.Namespace == pod.Namespace {
			leafPG.UnscheduledPods = slices.Delete(leafPG.UnscheduledPods, i, i+1)
			break
		}
	}

	return removed
}

// HasQueuedPodInfos 返回是否还有排队中的成员 Pod。
func (pgqi *QueuedPodGroupInfo) HasQueuedPodInfos() bool {
	if len(pgqi.QueuedPodInfos) == 0 {
		return false
	}
	for _, list := range pgqi.QueuedPodInfos {
		if len(list) > 0 {
			return true
		}
	}
	return false
}

// PodGroupMemberPodsOrderingFunc 按优先级降序、尝试次数降序、时间戳升序排列 PodGroup 成员 Pod，
// 最后以 Pod 名称升序作为确定性的最终裁决。
func PodGroupMemberPodsOrderingFunc(a, b *QueuedPodInfo) int {
	if a.GetPriority() > b.GetPriority() {
		return -1
	} else if a.GetPriority() < b.GetPriority() {
		return 1
	}
	// 优先级相同则按尝试次数裁决。
	// 由于时间戳在每个调度周期后都会重建，
	// 尝试次数越多说明 Pod 越早加入，应排在前面。
	if a.Attempts > b.Attempts {
		return -1
	} else if a.Attempts < b.Attempts {
		return 1
	}
	// 优先级与尝试次数都相同，则按时间戳裁决。
	if a.Timestamp.Before(b.Timestamp) {
		return -1
	} else if a.Timestamp.After(b.Timestamp) {
		return 1
	}
	// 在时钟分辨率较低的平台（如 Windows）上，快速连续加入的 Pod 可能拥有相同时间戳。
	// 回退到 Pod 名称比较，保证顺序确定且稳定。
	if a.Pod.Name < b.Pod.Name {
		return -1
	} else if a.Pod.Name > b.Pod.Name {
		return 1
	}
	return 0
}

// ForEachPodInfo 遍历 PodGroup 中所有排队成员 Pod 并执行 fn。
// fn 返回 false 时提前结束遍历。
func (pgqi *QueuedPodGroupInfo) ForEachPodInfo(fn func(pInfo *QueuedPodInfo) bool) {
	for _, list := range pgqi.QueuedPodInfos {
		for _, pInfo := range list {
			ok := fn(pInfo)
			if !ok {
				return
			}
		}
	}
}

// Update 更新 PodGroup 中指定 Pod 的信息，并同步叶子 PodGroup 的 UnscheduledPods。
func (pgqi *QueuedPodGroupInfo) Update(pod *v1.Pod) (*QueuedPodInfo, error) {
	if pgqi.QueuedPodInfos == nil {
		return nil, fmt.Errorf("pod %s/%s to update not found in the queued group info", pod.Namespace, pod.Name)
	}

	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return nil, fmt.Errorf("pod %s/%s to update not found in the queued group info", pod.Namespace, pod.Name)
	}

	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	list, exists := pgqi.QueuedPodInfos[key]
	if !exists {
		return nil, fmt.Errorf("pod %s/%s to update not found in the queued group info", pod.Namespace, pod.Name)
	}

	for _, pInfo := range list {
		if pInfo.Pod.Name != pod.Name || pInfo.Pod.Namespace != pod.Namespace {
			continue
		}
		err := pInfo.PodInfo.Update(pod)

		leafPG, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, *pod.Spec.SchedulingGroup.PodGroupName)
		if leafPG != nil {
			for i, p := range leafPG.UnscheduledPods {
				if p.Name == pod.Name && p.Namespace == pod.Namespace {
					leafPG.UnscheduledPods[i] = pod
					break
				}
			}
		}
		return pInfo, err
	}

	return nil, fmt.Errorf("pod %s/%s to update not found in the queued group info", pod.Namespace, pod.Name)
}

// Gated 返回 PodGroup 是否被某个插件拦截。
func (pgqi *QueuedPodGroupInfo) Gated() bool {
	return pgqi.QueueingParams.GatingPlugin != ""
}

// GetPriority 返回 PodGroup 的优先级，CompositePodGroup 取其自身优先级。
func (pgqi *QueuedPodGroupInfo) GetPriority() int32 {
	if pgqi.PodGroup != nil {
		return schedutil.PodGroupPriority(pgqi.PodGroup)
	}
	if pgqi.CompositePodGroup != nil {
		return schedutil.CompositePodGroupPriority(pgqi.CompositePodGroup)
	}
	return 0
}

// Size 返回 PodGroup 中所有排队成员 Pod 的总数。
func (pgqi *QueuedPodGroupInfo) Size() int {
	size := 0
	for _, pInfos := range pgqi.QueuedPodInfos {
		size += len(pInfos)
	}
	return size
}

// IncAttempts 递增 PodGroup 及其所有成员 Pod 的调度尝试次数。
func (pgqi *QueuedPodGroupInfo) IncAttempts() {
	pgqi.Attempts++
	for _, pInfos := range pgqi.QueuedPodInfos {
		for _, pInfo := range pInfos {
			pInfo.IncAttempts()
		}
	}
}

// SetInitialAttemptTimestamp 设置 PodGroup 及所有成员 Pod 的首次尝试时间。
func (pgqi *QueuedPodGroupInfo) SetInitialAttemptTimestamp(t time.Time) {
	if pgqi.InitialAttemptTimestamp == nil {
		pgqi.InitialAttemptTimestamp = &t
	}
	// 即便首次尝试时间已设置，PodGroup 仍可能有新 Pod 加入，
	// 因此每次都要尝试为所有成员 Pod 设置该时间。
	for _, pInfos := range pgqi.QueuedPodInfos {
		for _, pInfo := range pInfos {
			pInfo.SetInitialAttemptTimestamp(t)
		}
	}
}

// SetWasFlushedFromUnschedulable 设置 PodGroup 及其成员的"刚从 unschedulable 队列刷新"标记。
func (pgqi *QueuedPodGroupInfo) SetWasFlushedFromUnschedulable(flushed bool) {
	pgqi.WasFlushedFromUnschedulable = flushed
	for _, pInfos := range pgqi.QueuedPodInfos {
		for _, pInfo := range pInfos {
			pInfo.SetWasFlushedFromUnschedulable(flushed)
		}
	}
}

// SetBackoffExpiration 设置 PodGroup 的退避到期时间。
func (pgqi *QueuedPodGroupInfo) SetBackoffExpiration(t time.Time) {
	// 成员 Pod 共享同一退避到期时间，无需逐个设置。
	pgqi.BackoffExpiration = t
}

// SetGatingPlugin 记录拦截 PodGroup 的插件及其事件。
func (pgqi *QueuedPodGroupInfo) SetGatingPlugin(gatingPlugin string, gatingEvents []fwk.ClusterEvent) {
	// 每个成员 Pod 有自己的拦截插件与事件，不应把 PodGroup 的设置广播给它们。
	pgqi.GatingPlugin = gatingPlugin
	pgqi.GatingPluginEvents = gatingEvents
}

// SetFlushTimestamp 设置 PodGroup 的最近刷新时间。
func (pgqi *QueuedPodGroupInfo) SetFlushTimestamp(t time.Time) {
	// 该时间只在根节点检查，无需设置给所有成员。
	pgqi.FlushTimestamp = t
}

// AddPodGroup 把 PodGroup 加入队列信息的层级结构。
func (pgqi *QueuedPodGroupInfo) AddPodGroup(pg *schedulingv1beta1.PodGroup) {
	// 只添加非根 PodGroup：根节点已经在层级结构中。
	if !utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) || pg.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parent, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, *pg.Spec.ParentCompositePodGroupName)
	if parent != nil && parent.GetCompositePodGroup() != nil {
		for _, child := range parent.Children {
			if child.GetName() == pg.Name {
				return
			}
		}
		pgInfo := &PodGroupInfo{
			Namespace: pg.Namespace,
			Name:      pg.Name,
			Type:      fwk.PodGroupKeyType,
			PodGroup:  pg,
			Children:  make([]*PodGroupInfo, 0),
		}
		parent.Children = append(parent.Children, pgInfo)
	}
}

// UpdatePodGroup 更新层级结构中指定 PodGroup 的 API 对象。
func (pgqi *QueuedPodGroupInfo) UpdatePodGroup(pg *schedulingv1beta1.PodGroup) {
	node, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, pg.Name)
	if node != nil && node.GetPodGroup() != nil {
		node.PodGroup = pg
	}
}

// RemovePodGroup 从层级结构中移除 PodGroup，
// 返回被移除 PodGroup 整个子树中的所有排队 Pod。
func (pgqi *QueuedPodGroupInfo) RemovePodGroup(pg *schedulingv1beta1.PodGroup) []*QueuedPodInfo {
	node, parent := findNodeAndParent(pgqi.PodGroupInfo, nil, pg.Name)
	if node == nil {
		return nil
	}

	if parent != nil {
		for i, child := range parent.Children {
			if child.GetName() == pg.Name {
				parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
				break
			}
		}
	}

	return pgqi.deleteSubtreePods(node)
}

// AddCompositePodGroup 把 CompositePodGroup 子树加入队列信息的层级结构。
func (pgqi *QueuedPodGroupInfo) AddCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup, subtree *PodGroupInfo) {
	if cpg.Spec.ParentCompositePodGroupName == nil {
		return
	}

	parent, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, *cpg.Spec.ParentCompositePodGroupName)
	if parent != nil && parent.GetCompositePodGroup() != nil {
		for _, child := range parent.Children {
			if child.GetName() == cpg.Name {
				return
			}
		}
		parent.Children = append(parent.Children, subtree)
	}
}

// UpdateCompositePodGroup 更新层级结构中指定 CompositePodGroup 的 API 对象。
func (pgqi *QueuedPodGroupInfo) UpdateCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) {
	node, _ := findNodeAndParent(pgqi.PodGroupInfo, nil, cpg.Name)
	if node != nil && node.GetCompositePodGroup() != nil {
		node.CompositePodGroup = cpg
	}
}

// RemoveCompositePodGroup 从层级结构中移除 CompositePodGroup，
// 返回被移除子树中的所有排队 Pod。
func (pgqi *QueuedPodGroupInfo) RemoveCompositePodGroup(cpg *schedulingv1alpha3.CompositePodGroup) []*QueuedPodInfo {
	node, parent := findNodeAndParent(pgqi.PodGroupInfo, nil, cpg.Name)
	if node == nil {
		return nil
	}

	if parent != nil {
		for i, child := range parent.Children {
			if child.GetName() == cpg.Name {
				parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
				break
			}
		}
	}

	return pgqi.deleteSubtreePods(node)
}

// findNodeAndParent 用深度优先搜索在层级结构中按名称查找节点，
// 返回目标节点及其父节点；目标为根节点时父节点为 nil。
func findNodeAndParent(curr, parent *PodGroupInfo, name string) (*PodGroupInfo, *PodGroupInfo) {
	if curr.GetName() == name {
		return curr, parent
	}
	if curr.GetCompositePodGroup() != nil {
		for _, child := range curr.Children {
			if n, p := findNodeAndParent(child, curr, name); n != nil {
				return n, p
			}
		}
	}
	return nil, nil
}

// deleteSubtreePods 递归遍历给定节点的子树，从 QueuedPodInfos 中删除所有遇到的 PodGroup，
// 返回被成功移除的全部 QueuedPodInfo。
func (pgqi *QueuedPodGroupInfo) deleteSubtreePods(curr *PodGroupInfo) []*QueuedPodInfo {
	removedPods := make([]*QueuedPodInfo, 0)
	if curr.GetPodGroup() != nil {
		key := fwk.PodGroupKey(curr.Namespace, curr.Name)
		if pods, ok := pgqi.QueuedPodInfos[key]; ok {
			removedPods = append(removedPods, pods...)
			delete(pgqi.QueuedPodInfos, key)
		}
	}
	for _, child := range curr.Children {
		removedPods = append(removedPods, pgqi.deleteSubtreePods(child)...)
	}
	return removedPods
}

// newQueuedPodGroupInfo 根据 PodGroup 对象构造空的 QueuedPodGroupInfo。
func newQueuedPodGroupInfo(pg *schedulingv1beta1.PodGroup) *QueuedPodGroupInfo {
	return &QueuedPodGroupInfo{
		PodGroupInfo: &PodGroupInfo{
			Namespace: pg.Namespace,
			Name:      pg.Name,
			Type:      fwk.PodGroupKeyType,
			PodGroup:  pg,
			Children:  make([]*PodGroupInfo, 0),
		},
		QueuedPodInfos: make(map[fwk.EntityKey][]*QueuedPodInfo),
	}
}

// PodGroupInfo 包装 PodGroup API 对象及其成员 Pod 列表，
// 通常作为 PodGroup 调度周期插件的输入。
type PodGroupInfo struct {
	// PodGroup 所在的命名空间。
	Namespace string
	// PodGroup 的名称。
	Name string
	// PodGroup 的类型：CompositePodGroup 或 PodGroup。
	Type fwk.EntityKeyType
	// 当前正在参与调度的 Pod。
	// 如需获取已调度（assumed 或 assigned）的 Pod，可使用 PodGroupManager.PodGroupState。
	// Pod 顺序按签名、优先级和时间戳确定，只有叶子 PodGroup 才有未调度 Pod。
	UnscheduledPods []*v1.Pod
	// PodGroup API 对象。
	PodGroup *schedulingv1beta1.PodGroup
	// CompositePodGroup API 对象，仅在 CompositePodGroup 特性启用时设置。
	CompositePodGroup *schedulingv1alpha3.CompositePodGroup
	// 子 PodGroup 列表，只有 CompositePodGroup 才有子节点。
	Children []*PodGroupInfo
}

// GetName 返回 PodGroup 的名称。
func (pgi *PodGroupInfo) GetName() string {
	return pgi.Name
}

// GetNamespace 返回 PodGroup 的命名空间。
func (pgi *PodGroupInfo) GetNamespace() string {
	return pgi.Namespace
}

// GetType 返回 PodGroup 的实体键类型。
func (pgi *PodGroupInfo) GetType() fwk.EntityKeyType {
	return pgi.Type
}

// GetKey 返回 PodGroup 的字符串键，格式为 "类型/命名空间/名称"。
func (pgi *PodGroupInfo) GetKey() string {
	return fmt.Sprintf("%s/%s/%s", pgi.Type, pgi.Namespace, pgi.Name)
}

// GetUnscheduledPods 返回 PodGroup 的未调度 Pod。
// 对 CompositePodGroup 会递归汇总层级中所有叶子 PodGroup 的未调度 Pod。
func (pgi *PodGroupInfo) GetUnscheduledPods() []*v1.Pod {
	if pgi.PodGroup != nil {
		return pgi.UnscheduledPods
	}
	var pods []*v1.Pod
	for _, child := range pgi.Children {
		pods = append(pods, child.GetUnscheduledPods()...)
	}
	return pods
}

// GetPodGroup 返回 PodGroup API 对象。
func (pgi *PodGroupInfo) GetPodGroup() *schedulingv1beta1.PodGroup {
	return pgi.PodGroup
}

// GetCompositePodGroup 返回 CompositePodGroup API 对象。
func (pgi *PodGroupInfo) GetCompositePodGroup() *schedulingv1alpha3.CompositePodGroup {
	return pgi.CompositePodGroup
}

// GetChildren 返回子 PodGroup 的接口视图切片。
func (pgi *PodGroupInfo) GetChildren() []fwk.PodGroupInfo {
	if len(pgi.Children) == 0 {
		return nil
	}
	children := make([]fwk.PodGroupInfo, len(pgi.Children))
	for i, child := range pgi.GetChildGroups() {
		children[i] = child
	}
	return children
}

// GetCreationTimestamp 返回 PodGroup 或 CompositePodGroup 的创建时间。
func (pgi *PodGroupInfo) GetCreationTimestamp() time.Time {
	if pgi.PodGroup != nil {
		return pgi.PodGroup.CreationTimestamp.Time
	}
	return pgi.CompositePodGroup.CreationTimestamp.Time
}

// GetChildGroups 返回按创建时间排序的子 PodGroup 列表，时间相同时按名称排序。
func (pgi *PodGroupInfo) GetChildGroups() []*PodGroupInfo {
	if pgi.CompositePodGroup == nil {
		// 只有 CompositePodGroup 才有子 PodGroup。
		return nil
	}
	result := make([]*PodGroupInfo, len(pgi.Children))
	copy(result, pgi.Children)
	// 先按创建时间排序；时间相同时以名称裁决，保证顺序确定。
	slices.SortFunc(result, func(a, b *PodGroupInfo) int {
		aTime := a.GetCreationTimestamp()
		bTime := b.GetCreationTimestamp()
		if aTime.Before(bTime) {
			return -1
		}
		if aTime.After(bTime) {
			return 1
		}
		if a.Name < b.Name {
			return -1
		} else if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return result
}

// PodInfo 是 Pod 的包装，附带预先计算好的信息以加速处理。
// 这些信息通常不可变，例如预处理过的 Pod 间亲和性选择器。
type PodInfo struct {
	Pod                        *v1.Pod
	RequiredAffinityTerms      []fwk.AffinityTerm
	RequiredAntiAffinityTerms  []fwk.AffinityTerm
	PreferredAffinityTerms     []fwk.WeightedAffinityTerm
	PreferredAntiAffinityTerms []fwk.WeightedAffinityTerm
	// 缓存 Pod 的资源计算结果。仅当 InPlacePodVerticalScaling 启用时值才可能变化；
	// 此时缓存中的已分配 Pod 会整体重建 PodInfo。
	// 该值在把已调度 Pod 加入 NodeInfo 时计算；移除 Pod（如寻找抢占受害者或从集群删除）
	// 时直接复用缓存，可显著减少重复计算。
	// 注意：不要直接访问该字段，应通过 CalculateResource 获取。
	cachedResource *fwk.PodResource
}

// GetName 返回 Pod 的名称。
func (pi *PodInfo) GetName() string {
	return pi.Pod.Name
}

// GetNamespace 返回 Pod 的命名空间。
func (pi *PodInfo) GetNamespace() string {
	return pi.Pod.Namespace
}

// GetPod 返回底层 Pod 对象。
func (pi *PodInfo) GetPod() *v1.Pod {
	return pi.Pod
}

// GetRequiredAffinityTerms 返回必需亲和性条款。
func (pi *PodInfo) GetRequiredAffinityTerms() []fwk.AffinityTerm {
	return pi.RequiredAffinityTerms
}

// GetRequiredAntiAffinityTerms 返回必需反亲和性条款。
func (pi *PodInfo) GetRequiredAntiAffinityTerms() []fwk.AffinityTerm {
	return pi.RequiredAntiAffinityTerms
}

// GetPreferredAffinityTerms 返回优先亲和性条款。
func (pi *PodInfo) GetPreferredAffinityTerms() []fwk.WeightedAffinityTerm {
	return pi.PreferredAffinityTerms
}

// GetPreferredAntiAffinityTerms 返回优先反亲和性条款。
func (pi *PodInfo) GetPreferredAntiAffinityTerms() []fwk.WeightedAffinityTerm {
	return pi.PreferredAntiAffinityTerms
}

// DeepCopy 返回 PodInfo 的深拷贝。
func (pi *PodInfo) DeepCopy() *PodInfo {
	return &PodInfo{
		Pod:                        pi.Pod.DeepCopy(),
		RequiredAffinityTerms:      pi.RequiredAffinityTerms,
		RequiredAntiAffinityTerms:  pi.RequiredAntiAffinityTerms,
		PreferredAffinityTerms:     pi.PreferredAffinityTerms,
		PreferredAntiAffinityTerms: pi.PreferredAntiAffinityTerms,
		cachedResource:             pi.cachedResource,
	}
}

// Update 在传入 Pod 的 UID 相同时更新 PodInfo 中的 Pod 指针，UID 不一致则返回错误。
func (pi *PodInfo) Update(pod *v1.Pod) error {
	if pod == nil {
		return fmt.Errorf("cannot update with nil pod")
	}
	if pi.Pod == nil {
		return fmt.Errorf("cannot update PodInfo - its Pod is nil")
	}
	if pi.Pod.UID != pod.UID {
		return fmt.Errorf("pod UID mismatch, expected %v, got %v", pi.Pod.UID, pod.UID)
	}
	pi.Pod = pod
	// 清空缓存，强制下次 CalculateResource 重新计算。
	pi.cachedResource = nil
	return nil
}

// CalculateResource 返回 Pod 的资源请求量，首次调用时计算并缓存结果。
func (pi *PodInfo) CalculateResource() fwk.PodResource {
	if pi.cachedResource != nil {
		return *pi.cachedResource
	}
	inPlacePodVerticalScalingEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling)
	podLevelResourcesEnabled := utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)
	inPlacePodLevelResourcesVerticalScalingEnabled := utilfeature.DefaultMutableFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling)
	nodeAllocatableResourcesDRAEnabled := utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources)

	requests := resourcehelper.PodRequests(pi.Pod, resourcehelper.PodResourcesOptions{
		UseStatusResources: inPlacePodVerticalScalingEnabled,
		InPlacePodLevelResourcesVerticalScalingEnabled: inPlacePodLevelResourcesVerticalScalingEnabled,
		// PodLevelResources 启用时 SkipPodLevelResources 才为 false。
		SkipPodLevelResources:                    !podLevelResourcesEnabled,
		UseDRANodeAllocatableResourceClaimStatus: nodeAllocatableResourcesDRAEnabled,
	})
	isPodLevelResourcesSet := podLevelResourcesEnabled && resourcehelper.IsPodLevelRequestsSet(pi.Pod)
	nonMissingContainerRequests := getNonMissingContainerRequests(requests, isPodLevelResourcesSet)
	non0Requests := requests
	if len(nonMissingContainerRequests) > 0 {
		non0Requests = resourcehelper.PodRequests(pi.Pod, resourcehelper.PodResourcesOptions{
			UseStatusResources: inPlacePodVerticalScalingEnabled,
			InPlacePodLevelResourcesVerticalScalingEnabled: inPlacePodLevelResourcesVerticalScalingEnabled,
			// PodLevelResources 启用时 SkipPodLevelResources 才为 false。
			SkipPodLevelResources:                    !podLevelResourcesEnabled,
			NonMissingContainerRequests:              nonMissingContainerRequests,
			UseDRANodeAllocatableResourceClaimStatus: nodeAllocatableResourcesDRAEnabled,
		})
	}
	non0CPU := non0Requests[v1.ResourceCPU]
	non0Mem := non0Requests[v1.ResourceMemory]

	var res Resource
	res.Add(requests)
	podResource := fwk.PodResource{
		Resource: &res,
		Non0CPU:  non0CPU.MilliValue(),
		Non0Mem:  non0Mem.Value(),
	}
	pi.cachedResource = &podResource
	return podResource
}

// ExtenderName 是 Extender 拒绝某些节点时写入 UnschedulablePlugins 的伪插件名。
const ExtenderName = "Extender"

// Diagnosis 记录用于诊断调度失败的详细信息。
type Diagnosis struct {
	// 记录被 PreFilter（通过 PreFilterResult）或 Filter 插件拒绝的节点状态，
	// 以及不在记录中的节点的通用状态。通过 PreFilter/Filter 的节点不在其中。
	NodeToStatus *NodeToStatus
	// 返回 Unschedulable 或 UnschedulableAndUnresolvable 的插件集合。
	UnschedulablePlugins sets.Set[string]
	// 返回 Pending 的插件集合。
	PendingPlugins sets.Set[string]
	// 记录 PreFilter 插件返回的消息。
	PreFilterMsg string
	// 记录 PostFilter 插件返回的消息。
	PostFilterMsg string
}

// FitError 描述 Pod 无法适配任何节点的错误。
type FitError struct {
	Pod         *v1.Pod
	NumAllNodes int
	Diagnosis   Diagnosis
}

const (
	// NoNodeAvailableMsg 是无节点可用时错误消息的格式化模板。
	NoNodeAvailableMsg = "0/%v nodes are available"
)

// AddPluginStatus 按插件状态将其记入 Unschedulable 或 Pending 插件集合。
func (d *Diagnosis) AddPluginStatus(sts *fwk.Status) {
	if sts.Plugin() == "" {
		return
	}
	if sts.IsRejected() {
		if d.UnschedulablePlugins == nil {
			d.UnschedulablePlugins = sets.New[string]()
		}
		d.UnschedulablePlugins.Insert(sts.Plugin())
	}
	if sts.Code() == fwk.Pending {
		if d.PendingPlugins == nil {
			d.PendingPlugins = sets.New[string]()
		}
		d.PendingPlugins.Insert(sts.Plugin())
	}
}

// Error 返回 Pod 无法适配各节点的详细信息。
// 消息格式为 "0/X nodes are available: <PreFilterMsg>. <FilterMsg>. <PostFilterMsg>."。
func (f *FitError) Error() string {
	reasonMsg := fmt.Sprintf(NoNodeAvailableMsg+":", f.NumAllNodes)
	preFilterMsg := f.Diagnosis.PreFilterMsg
	if preFilterMsg != "" {
		// PreFilter 插件返回不可调度，直接把它的消息加入原因。
		reasonMsg += fmt.Sprintf(" %v.", preFilterMsg)
	}

	if preFilterMsg == "" {
		// 调度周期顺利通过 PreFilter 扩展点。
		//
		// PreFilter 返回不可调度时，框架会把同一状态写入 NodeToStatusReader 的所有节点；
		// 因此 PreFilter 失败时不应再附加 NodeToStatusReader 的消息，否则原因会重复。
		reasons := make(map[string]int)
		f.Diagnosis.NodeToStatus.ForEachExplicitNode(func(_ string, status *fwk.Status) {
			for _, reason := range status.Reasons() {
				reasons[reason]++
			}
		})
		if f.Diagnosis.NodeToStatus.Len() < f.NumAllNodes {
			// 为 NodeToStatusReader 中缺失的节点补上预定义原因。
			for _, reason := range f.Diagnosis.NodeToStatus.AbsentNodesStatus().Reasons() {
				reasons[reason] += f.NumAllNodes - f.Diagnosis.NodeToStatus.Len()
			}
		}

		sortReasonsHistogram := func() []string {
			var reasonStrings []string
			for k, v := range reasons {
				reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
			}
			sort.Strings(reasonStrings)
			return reasonStrings
		}
		sortedFilterMsg := sortReasonsHistogram()
		if len(sortedFilterMsg) != 0 {
			reasonMsg += fmt.Sprintf(" %v.", strings.Join(sortedFilterMsg, ", "))
		}
	}

	// 附加 PostFilter 插件的消息。
	// 无论调度周期在 PreFilter 还是 Filter 失败都可以附加：两种情况下都可能运行 PostFilter。
	postFilterMsg := f.Diagnosis.PostFilterMsg
	if postFilterMsg != "" {
		reasonMsg += fmt.Sprintf(" %v", postFilterMsg)
	}
	return reasonMsg
}

// NewPodInfo 根据 Pod 构造 PodInfo，并预处理亲和性条款。
func NewPodInfo(pod *v1.Pod) (*PodInfo, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod cannot be nil")
	}
	pInfo := &PodInfo{Pod: pod}

	var preferredAffinityTerms []v1.WeightedPodAffinityTerm
	var preferredAntiAffinityTerms []v1.WeightedPodAffinityTerm
	if affinity := pod.Spec.Affinity; affinity != nil {
		if a := affinity.PodAffinity; a != nil {
			preferredAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
		if a := affinity.PodAntiAffinity; a != nil {
			preferredAntiAffinityTerms = a.PreferredDuringSchedulingIgnoredDuringExecution
		}
	}

	// 解析亲和性条款，汇总所有解析错误后一并返回。
	var parseErrs []error
	requiredAffinityTerms, err := fwk.GetAffinityTerms(pod, fwk.GetPodAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAffinityTerms: %w", err))
	}
	requiredAntiAffinityTerms, err := fwk.GetAffinityTerms(pod,
		fwk.GetPodAntiAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("requiredAntiAffinityTerms: %w", err))
	}
	weightedAffinityTerms, err := fwk.GetWeightedAffinityTerms(pod, preferredAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAffinityTerms: %w", err))
	}
	weightedAntiAffinityTerms, err := fwk.GetWeightedAffinityTerms(pod, preferredAntiAffinityTerms)
	if err != nil {
		parseErrs = append(parseErrs, fmt.Errorf("preferredAntiAffinityTerms: %w", err))
	}

	pInfo.RequiredAffinityTerms = requiredAffinityTerms
	pInfo.RequiredAntiAffinityTerms = requiredAntiAffinityTerms
	pInfo.PreferredAffinityTerms = weightedAffinityTerms
	pInfo.PreferredAntiAffinityTerms = weightedAntiAffinityTerms

	return pInfo, utilerrors.NewAggregate(parseErrs)
}

// Resource 是可计算资源的集合。
// 实现与 fwk.Resource 接口分离，因为 Add 与 SetMaxResource 的实现
// 依赖调度器内部的 util 函数。
type Resource struct {
	MilliCPU         int64
	Memory           int64
	EphemeralStorage int64
	// 显式以 int 存储 allowedPodNumber（即 Node.Status.Allocatable.Pods().Value()），
	// 避免频繁类型转换，提升性能。
	AllowedPodNumber int
	// 标量资源（扩展资源）的数量。
	ScalarResources map[v1.ResourceName]int64
}

// GetMilliCPU 返回 CPU 请求量，单位为毫核。
func (r *Resource) GetMilliCPU() int64 {
	return r.MilliCPU
}

// GetMemory 返回内存请求量，单位为字节。
func (r *Resource) GetMemory() int64 {
	return r.Memory
}

// GetEphemeralStorage 返回临时存储请求量。
func (r *Resource) GetEphemeralStorage() int64 {
	return r.EphemeralStorage
}

// GetAllowedPodNumber 返回允许的 Pod 数量上限。
func (r *Resource) GetAllowedPodNumber() int {
	return r.AllowedPodNumber
}

// GetScalarResources 返回标量资源映射。
func (r *Resource) GetScalarResources() map[v1.ResourceName]int64 {
	return r.ScalarResources
}

// NewResource 根据 ResourceList 创建 Resource。
func NewResource(rl v1.ResourceList) *Resource {
	r := &Resource{}
	r.Add(rl)
	return r
}

// Add 把 ResourceList 中的资源累加到 Resource 中。
func (r *Resource) Add(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.MilliCPU += rQuant.MilliValue()
		case v1.ResourceMemory:
			r.Memory += rQuant.Value()
		case v1.ResourcePods:
			r.AllowedPodNumber += int(rQuant.Value())
		case v1.ResourceEphemeralStorage:
			r.EphemeralStorage += rQuant.Value()
		default:
			if schedutil.IsScalarResourceName(rName) {
				r.AddScalar(rName, rQuant.Value())
			}
		}
	}
}

// Clone 返回 Resource 的副本，标量资源 map 也会复制。
func (r *Resource) Clone() *Resource {
	res := &Resource{
		MilliCPU:         r.MilliCPU,
		Memory:           r.Memory,
		AllowedPodNumber: r.AllowedPodNumber,
		EphemeralStorage: r.EphemeralStorage,
	}
	if r.ScalarResources != nil {
		res.ScalarResources = make(map[v1.ResourceName]int64, len(r.ScalarResources))
		for k, v := range r.ScalarResources {
			res.ScalarResources[k] = v
		}
	}
	return res
}

// AddScalar 把指定标量资源的值累加 quantity。
func (r *Resource) AddScalar(name v1.ResourceName, quantity int64) {
	r.SetScalar(name, r.ScalarResources[name]+quantity)
}

// SetScalar 设置指定标量资源的值。
func (r *Resource) SetScalar(name v1.ResourceName, quantity int64) {
	// 按需分配标量资源 map，避免为没有扩展资源的节点付出额外内存。
	if r.ScalarResources == nil {
		r.ScalarResources = map[v1.ResourceName]int64{}
	}
	r.ScalarResources[name] = quantity
}

// SetMaxResource 逐项比较 ResourceList，取各资源的最大值。
func (r *Resource) SetMaxResource(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuantity := range rl {
		switch rName {
		case v1.ResourceMemory:
			r.Memory = max(r.Memory, rQuantity.Value())
		case v1.ResourceCPU:
			r.MilliCPU = max(r.MilliCPU, rQuantity.MilliValue())
		case v1.ResourceEphemeralStorage:
			r.EphemeralStorage = max(r.EphemeralStorage, rQuantity.Value())
		default:
			if schedutil.IsScalarResourceName(rName) {
				r.SetScalar(rName, max(r.ScalarResources[rName], rQuantity.Value()))
			}
		}
	}
}

// NewNodeInfo 返回一个可直接使用的空 NodeInfo。
// 若传入 Pod，其信息会被聚合进返回的对象。
func NewNodeInfo(pods ...*v1.Pod) *NodeInfo {
	ni := &NodeInfo{
		Requested:        &Resource{},
		NonZeroRequested: &Resource{},
		Allocatable:      &Resource{},
		Generation:       nextGeneration(),
		UsedPorts:        make(fwk.HostPortInfo),
		ImageStates:      make(map[string]*fwk.ImageStateSummary),
		PVCRefCounts:     make(map[string]int),
	}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

// getNonMissingContainerRequests 返回调度器在容器级与 Pod 级请求都未设置时
// 采用的默认非零 CPU/内存请求。它确保每个容器都有最小资源请求，
// 使调度器能聚合请求量并找到合适的节点。
//
// 行为取决于是否设置了 Pod 级资源：
//  1. 未设置 Pod 级资源时，返回包含以下默认值的 ResourceList：
//     - CPU：schedutil.DefaultMilliCPURequest
//     - 内存：schedutil.DefaultMemoryRequest
//  2. 设置了 Pod 级资源时，仅当 CPU 或内存在容器级*和* Pod 级都缺失时，
//     才把对应默认值（schedutil.DefaultMilliCPURequest 或 schedutil.DefaultMemoryRequest）
//     加入返回的 ResourceList。
//
// 注意：这些默认值不会写入 Pod 对象，仅在调度器选择节点时使用。
func getNonMissingContainerRequests(requests v1.ResourceList, podLevelResourcesSet bool) v1.ResourceList {
	if !podLevelResourcesSet {
		return v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI),
		}
	}

	nonMissingContainerRequests := make(v1.ResourceList, 2)
	// Pod 级与容器级 CPU 请求都未设置时以 DefaultMilliCPURequest 兜底。
	// 注意 apiserver 的默认值逻辑会把非零的容器级 CPU 请求传播到 Pod 级
	// （前提是未显式设置 Pod 级请求）。
	if _, exists := requests[v1.ResourceCPU]; !exists {
		nonMissingContainerRequests[v1.ResourceCPU] = *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI)
	}

	// Pod 级与容器级内存请求都未设置时以 DefaultMemoryRequest 兜底。
	// 注意 apiserver 的默认值逻辑会把非零的容器级内存请求传播到 Pod 级
	// （前提是未显式设置 Pod 级请求）。
	if _, exists := requests[v1.ResourceMemory]; !exists {
		nonMissingContainerRequests[v1.ResourceMemory] = *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI)
	}
	return nonMissingContainerRequests

}

// GetPodKey 返回 Pod 的缓存键（即 UID）。
func GetPodKey(pod *v1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}

// GetPodNamespacedName 返回 Pod 的命名空间限定名，格式为 "namespace/name"。
func GetPodNamespacedName(pod *v1.Pod) string {
	return GetNamespacedName(pod.Namespace, pod.Name)
}

// GetNamespacedName 返回命名空间级资源的名称字符串，格式为 "namespace/name"。
func GetNamespacedName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// PodPVCKeys 返回一个迭代器，产出 Pod 各卷引用的 PVC 的命名空间限定键（"namespace/name"）。
// 非 PVC 卷会被跳过。
func PodPVCKeys(pod *v1.Pod) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			if !yield(GetNamespacedName(pod.Namespace, v.PersistentVolumeClaim.ClaimName)) {
				return
			}
		}
	}
}
