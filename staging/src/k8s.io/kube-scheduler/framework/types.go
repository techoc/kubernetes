/*
Copyright 2025 The Kubernetes Authors.

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

package framework

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/klog/v2"
)

// ActionType is an integer to represent one type of resource change.
// Different ActionTypes can be bit-wised to compose new semantics.
//
// 用位掩码表示资源变更，使插件可以同时注册“多个细分事件”（例如一次声明所有节点更新），
// 而调度器在分发时仍能快速判断某个事件是否命中。
type ActionType int64

// Constants for ActionTypes.
// CAUTION for contributors: When you add a new ActionType, you must update the following:
// - The list of basicActionTypes, podActionTypes, and nodeActionTypes at k/k/pkg/scheduler/framework/types.go
// - String() method.
const (
	Add ActionType = 1 << iota
	Delete

	// 各细分事件必须是独立的单比特位，才能用按位或组合成下面的 Update 和 All。
	// 这样插件注册一个组合事件，调度器分发时按位测试即可，无需逐个比较。

	// UpdateNodeXYZ is only applicable for Node events.
	// If you use UpdateNodeXYZ,
	// your plugin's QueueingHint is only executed for the specific sub-Update event.
	// It's better to narrow down the scope of the event by using them instead of just using Update event
	// for better performance in requeueing.
	// 细分节点更新事件，让插件只关心真正会影响自己的那部分变化，
	// 避免每次节点状态抖动都触发无意义的重新入队。
	UpdateNodeAllocatable
	UpdateNodeLabel
	// UpdateNodeTaint is an update for node's taints or node.Spec.Unschedulable.
	UpdateNodeTaint
	UpdateNodeCondition
	UpdateNodeAnnotation
	// UpdateNodeDeclaredFeature is an update for node's declared features.
	UpdateNodeDeclaredFeature
	// UpdateNodePreemptionPolicy is an update for node's preemption policy.
	UpdateNodePreemptionPolicy

	// UpdatePodXYZ is only applicable for Pod events.
	// If you use UpdatePodXYZ,
	// your plugin's QueueingHint is only executed for the specific sub-Update event.
	// It's better to narrow down the scope of the event by using them instead of Update event
	// for better performance in requeueing.
	// 同理，Pod 的更新按字段细分；重入队本身有开销，越早过滤掉无关事件越好。
	UpdatePodLabel
	// UpdatePodScaleDown is an update for pod's scale down (i.e., any resource request is reduced).
	UpdatePodScaleDown
	// UpdatePodScaleUp is an update for pod's scale up (i.e., any resource request is increased).
	UpdatePodScaleUp
	// UpdatePodToleration is an addition for pod's tolerations.
	// (Due to API validation, we can add, but cannot modify or remove tolerations.)
	UpdatePodToleration
	// UpdatePodSchedulingGatesEliminated is an update for pod's scheduling gates, which eliminates all scheduling gates in the Pod.
	UpdatePodSchedulingGatesEliminated
	// UpdatePodGeneratedResourceClaim is an update of the list of ResourceClaims generated for the pod.
	// Depends on the DynamicResourceAllocation feature gate.
	UpdatePodGeneratedResourceClaim

	All ActionType = 1<<iota - 1
	// All 是所有已定义比特的并集，新增 ActionType 时它会自动扩展，无需手动维护。

	// Use the general Update type if you don't either know or care the specific sub-Update type to use.
	// Update 是各细分更新位的并集，供不关心具体更新内容的插件使用；
	// 用组合位而非单独一个新位，保证细分事件也能命中宽泛订阅。
	Update = UpdateNodeAllocatable | UpdateNodeLabel | UpdateNodeTaint | UpdateNodeCondition | UpdateNodeAnnotation | UpdateNodeDeclaredFeature | UpdateNodePreemptionPolicy | UpdatePodLabel | UpdatePodScaleDown | UpdatePodScaleUp | UpdatePodToleration | UpdatePodSchedulingGatesEliminated | UpdatePodGeneratedResourceClaim

	// None is a special ActionType that is only used internally.
	// 零值表示“没有任何动作”，作为无效值哨兵，避免把空事件误判成某种变更。
	None ActionType = 0
)

func (a ActionType) String() string {
	// 组合位（如 Update、All）单独匹配，保证日志和指标里出现的是可读的组合名，
	// 而不是一长串按位或出来的数字。
	switch a {
	case Add:
		return "Add"
	case Delete:
		return "Delete"
	case UpdateNodeAllocatable:
		return "UpdateNodeAllocatable"
	case UpdateNodeLabel:
		return "UpdateNodeLabel"
	case UpdateNodeTaint:
		return "UpdateNodeTaint"
	case UpdateNodeCondition:
		return "UpdateNodeCondition"
	case UpdateNodeAnnotation:
		return "UpdateNodeAnnotation"
	case UpdateNodeDeclaredFeature:
		return "UpdateNodeDeclaredFeature"
	case UpdateNodePreemptionPolicy:
		return "UpdateNodePreemptionPolicy"
	case UpdatePodLabel:
		return "UpdatePodLabel"
	case UpdatePodScaleDown:
		return "UpdatePodScaleDown"
	case UpdatePodScaleUp:
		return "UpdatePodScaleUp"
	case UpdatePodToleration:
		return "UpdatePodToleration"
	case UpdatePodSchedulingGatesEliminated:
		return "UpdatePodSchedulingGatesEliminated"
	case UpdatePodGeneratedResourceClaim:
		return "UpdatePodGeneratedResourceClaim"
	case All:
		return "All"
	case Update:
		return "Update"
	}

	// Shouldn't reach here.
	// 未知位返回空串，方便调用方把它当无效值过滤掉，而不是打印出误导性的数字。
	return ""
}

// EventResource is basically short for group/version/kind, which can uniquely represent a particular API resource.
//
// 用字符串标识资源类型，既覆盖内置资源也覆盖扩展资源（带 group 前缀），
// 插件注册事件时无需依赖具体的 Go 类型。
type EventResource string

// Constants for GVKs.
//
// CAUTION for contributors: When you add a new EventResource, you must register a new one to allResources at k/k/pkg/scheduler/framework/types.go
//
// Note:
// - UpdatePodXYZ or UpdateNodeXYZ: triggered by updating particular parts of a Pod or a Node, e.g. updatePodLabel.
// Use specific events rather than general ones (updatePodLabel vs update) can make the requeueing process more efficient
// and consume less memory as less events will be cached at scheduler.
const (
	// There are a couple of notes about how the scheduler notifies the events of Pods:
	// - Add: add events could be triggered by either a newly created Pod or an existing Pod that is scheduled to a Node.
	// - Delete: delete events could be triggered by:
	//           - a Pod that is deleted
	//           - a Pod that was assumed, but gets un-assumed due to some errors in the binding cycle.
	//           - an existing Pod that was unscheduled but gets scheduled to a Node.
	//
	// Note that the Pod event type includes the events for the unscheduled Pod itself.
	// i.e., when unscheduled Pods are updated, the scheduling queue checks with Pod/Update QueueingHint(s) whether the update may make the pods schedulable,
	// and requeues them to activeQ/backoffQ when at least one QueueingHint(s) return Queue.
	// Plugins **have to** implement a QueueingHint for Pod/Update event
	// if the rejection from them could be resolved by updating unscheduled Pods themselves.
	// Example: Pods that require excessive resources may be rejected by the noderesources plugin,
	// if this unscheduled pod is updated to require fewer resources,
	// the previous rejection from noderesources plugin can be resolved.
	// this plugin would implement QueueingHint for Pod/Update event
	// that returns Queue when such label changes are made in unscheduled Pods.
	//
	// There is one general pod resource: Pod, that contains three specific pod resources: AssignedPod, UnscheduledPod, and TargetPod.
	// Plugins can and are expected to register to specific pod events for better performance.
	// Pod 是宽泛兜底项，只有无法判断该关心哪类 Pod 时才用它。
	Pod EventResource = "Pod"
	// AssignedPod resource is associated with the cluster event that gets triggered when a scheduled pod is updated.
	// 已绑定的 Pod 变化通常意味着资源占用变化，影响正在排队的其他 Pod。
	AssignedPod EventResource = "AssignedPod"
	// UnscheduledPod resource is associated with the cluster event that gets triggered when an unscheduled pod is updated, other than the target pod.
	// 其他待调度 Pod 的变化，可能让某个被拒绝的 Pod 重新有机会（例如它们释放了竞争资源）。
	UnscheduledPod EventResource = "UnscheduledPod"
	// TargetPod resource is associated with the cluster event that gets triggered when an unscheduled pod itself is updated.
	// 被拒绝的 Pod 自身变化最直接：之前因资源过大被拒，调小请求后就能重新入队。
	TargetPod EventResource = "TargetPod"
	// 把 Pod 拆成三类，是为了让 QueueingHint 只评估相关的 Pod 集合，减少重入队计算量。

	Node                  EventResource = "Node"
	PersistentVolume      EventResource = "PersistentVolume"
	PersistentVolumeClaim EventResource = "PersistentVolumeClaim"
	CSINode               EventResource = "storage.k8s.io/CSINode"
	CSIDriver             EventResource = "storage.k8s.io/CSIDriver"
	VolumeAttachment      EventResource = "storage.k8s.io/VolumeAttachment"
	CSIStorageCapacity    EventResource = "storage.k8s.io/CSIStorageCapacity"
	StorageClass          EventResource = "storage.k8s.io/StorageClass"
	ResourceClaim         EventResource = "resource.k8s.io/ResourceClaim"
	ResourceSlice         EventResource = "resource.k8s.io/ResourceSlice"
	DeviceClass           EventResource = "resource.k8s.io/DeviceClass"
	PodGroup              EventResource = "scheduling.k8s.io/PodGroup"
	CompositePodGroup     EventResource = "scheduling.k8s.io/CompositePodGroup"

	// WildCard is a special EventResource to match all resources.
	// e.g., If you register `{Resource: "*", ActionType: All}` in EventsToRegister,
	// all coming clusterEvents will be admitted. Be careful to register it, it will
	// increase the computing pressure in requeueing unless you really need it.
	//
	// Meanwhile, if the coming clusterEvent is a wildcard one, all pods
	// will be moved from unschedulablePod pool to activeQ/backoffQ forcibly.
	// 通配符绕过 QueueingHint 直接全量重入队，代价高，只在确实无法定位具体资源时使用。
	WildCard EventResource = "*"
)

// ClusterEventWithHint 把“发生了什么”和“怎么判断要不要重调度”绑在一起注册。
//
// 这样插件注册事件时就自带过滤逻辑，调度器不必为每种事件再回头查一遍插件实现。
type ClusterEventWithHint struct {
	Event ClusterEvent
	// QueueingHintFn is executed for the Pod rejected by this plugin when the above Event happens,
	// and filters out events to reduce useless retry of Pod's scheduling.
	// It's an optional field. If not set,
	// the scheduling of Pods will be always retried with backoff when this Event happens.
	// (the same as Queue)
	// 两级过滤：PreQueueingHintFn 先粗筛出可能受影响的 Pod，QueueingHintFn 再逐个精筛，
	// 避免对每个事件都遍历全部不可调度 Pod。
	QueueingHintFn QueueingHintFn
	// PreQueueingHintFn is called once per event to narrow the set of pods to evaluate with QueueingHint.
	// If set, only pods identified in the returned PreQueueingHintResult are checked.
	// 粗筛成本低、精度差，精筛成本高；分两层是为了在事件很多时省下大量精筛调用。
	PreQueueingHintFn PreQueueingHintFn
}

// QueueingHintFn returns a hint that signals whether the event can make a Pod,
// which was rejected by this plugin in the past scheduling cycle, schedulable or not.
// It's called before a Pod gets moved from unschedulableQ to backoffQ or activeQ.
// If it returns an error, we'll take the returned QueueingHint as `Queue` at the caller whatever we returned here so that
// we can prevent the Pod from being stuck in the unschedulable pod pool.
//
// - `pod`: the Pod to be enqueued, which is rejected by this plugin in the past.
// - `oldObj` `newObj`: the object involved in that event.
//   - For example, the given event is "Node deleted", the `oldObj` will be that deleted Node.
//   - `oldObj` is nil if the event is add event.
//   - `newObj` is nil if the event is delete event.
//
// 错误一律按 Queue 处理，是故意偏向“多调度一次”而非“漏调度”：
// 宁可多付出一次调度周期，也不能让本可运行的 Pod 永久卡在不可调度队列里。
type QueueingHintFn func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (QueueingHint, error)

// PreQueueingHintResult is the return value of PreQueueingHintFn.
// When AllPods is true, all unschedulable pods are evaluated (existing behavior).
// When AllPods is false, only pods listed in Pods are evaluated.
//
// 两种模式兼顾兼容与性能：不实现粗筛的插件走 AllPods 保持旧行为，
// 能精确定位的插件只列出候选，减少后续逐 Pod 判断的开销。
type PreQueueingHintResult struct {
	AllPods bool
	Pods    []types.NamespacedName
}

// PreQueueingHintFn is called once per event before iterating unschedulable pods.
// PreQueueingHintFn will not be called when the deleted object is unknown (both oldObj and newObj are nil).
// It returns a PreQueueingHintResult indicating which pods should be evaluated
// by QueueingHintFn for this event. If it returns an error, the error is logged
// and the result is treated as AllPods (all unschedulable pods are evaluated).
//
// - oldObj is the old object in update events, or the deleted object in delete events.
// - newObj is the new/added object. It is nil for delete events.
//
// 出错时退化为 AllPods，与 QueueingHintFn 一样遵循“漏调度比多调度更糟”的原则。
// 删除对象未知时不调用，是因为此时没有可用信息做粗筛，调用也不会得到有意义的结果。
type PreQueueingHintFn func(logger klog.Logger, oldObj, newObj interface{}) (PreQueueingHintResult, error)

// QueueingHint 只表达“要不要重新入队”这一种决策，
// 细节判断留给插件，避免调度器理解每种插件的业务语义。
type QueueingHint int

const (
	// QueueSkip implies that the cluster event has no impact on
	// scheduling of the pod.
	QueueSkip QueueingHint = iota

	// Queue implies that the Pod may be schedulable by the event.
	Queue
)

func (s QueueingHint) String() string {
	switch s {
	case QueueSkip:
		return "QueueSkip"
	case Queue:
		return "Queue"
	}
	return ""
}

// ClusterEvent abstracts how a system resource's state gets changed.
// Resource represents the standard API resources such as Pod, Node, etc.
// ActionType denotes the specific change such as Add, Update or Delete.
// ClusterEvent 是插件与调度队列之间的通用事件格式。
// CustomLabel 让少数没有对应 GVK 的事件（如超时）也能进入同一套指标体系。
type ClusterEvent struct {
	Resource   EventResource
	ActionType ActionType

	// CustomLabel describes this cluster event.
	// It's an optional field to control Label(), which is used in logging and metrics.
	// Normally, it's not necessary to set this field; only used for special events like UnschedulableTimeout.
	CustomLabel string
}

// Label is used for logging and metrics.
// Label 生成稳定的指标标签；自定义标签优先，保证特殊事件不会污染通用事件的标签维度。
func (ce ClusterEvent) Label() string {
	if ce.CustomLabel != "" {
		return ce.CustomLabel
	}

	return fmt.Sprintf("%v%v", ce.Resource, ce.ActionType)
}

// NodeInfo is node level aggregated information.
// NodeInfo 把节点及其上所有 Pod 的聚合信息缓存成一份快照视图。
// 预计算这些派生数据，是因为 Filter/Score 阶段会被频繁调用，每次重算代价过高。
// generation 字段让快照可以判断数据是否变化，从而跳过不必要的克隆。
type NodeInfo interface {
	// Node returns overall information about this node.
	Node() *v1.Node
	// GetPods returns Pods running on the node.
	GetPods() []PodInfo
	// GetPodsWithAffinity returns the subset of pods with inter-pod (anti-)affinity.
	GetPodsWithAffinity() []PodInfo
	// GetPodsWithRequiredAntiAffinity returns the subset of pods with required inter-pod anti-affinity.
	GetPodsWithRequiredAntiAffinity() []PodInfo
	// GetPodsWithRequiredNonHostScopedAntiAffinity returns the subset of pods with required inter-pod anti-affinity that is non-hostname scoped.
	// This must be empty when the InterPodAffinityHostnameFastPath feature gate is disabled.
	GetPodsWithRequiredNonHostScopedAntiAffinity() []PodInfo
	// GetUsedPorts returns the ports allocated on the node.
	GetUsedPorts() HostPortInfo
	// GetRequested returns total requested resources of all pods on this node. This includes assumed
	// pods, which scheduler has sent for binding, but may not be scheduled yet.
	GetRequested() Resource
	// GetNonZeroRequested return total requested resources of all pods on this node with a minimum value
	// applied to each container's CPU and memory requests. This does not reflect
	// the actual resource requests for this node, but is used to avoid scheduling
	// many zero-request pods onto one node.
	GetNonZeroRequested() Resource
	// We store allocatedResources (which is Node.Status.Allocatable.*) explicitly
	// as int64, to avoid conversions and accessing map.
	GetAllocatable() Resource
	// GetImageStates returns the entry of an image if and only if this image is on the node. The entry can be used for
	// checking an image's existence and advanced usage (e.g., image locality scheduling policy) based on the image
	// state information.
	GetImageStates() map[string]*ImageStateSummary
	// GetPVCRefCounts returns a mapping of PVC names to the number of pods on the node using it.
	// Keys are in the format "namespace/name".
	GetPVCRefCounts() map[string]int
	// Whenever NodeInfo changes, generation is bumped.
	// This is used to avoid cloning it if the object didn't change.
	GetGeneration() int64
	// GetNodeDeclaredFeatures returns the declared feature set of the node.
	GetNodeDeclaredFeatures() ndf.FeatureSet
	// Snapshot returns a copy of this node, Except that ImageStates is copied without the Nodes field.
	Snapshot() NodeInfo
	// String returns representation of human readable format of this NodeInfo.
	String() string

	// AddPodInfo adds pod information to this NodeInfo.
	// Consider using this instead of AddPod if a PodInfo is already computed.
	AddPodInfo(podInfo PodInfo)
	// RemovePod subtracts pod information from this NodeInfo.
	RemovePod(logger klog.Logger, pod *v1.Pod) error
	// SetNode sets the overall node information.
	SetNode(node *v1.Node)
}

// QueuedEntityInfo is an interface that represents a schedulable entity in the scheduling queue.
// It can be a single Pod (QueuedPodInfo) or a group of Pods (QueuedPodGroupInfo).
// QueuedEntityInfo 统一单个 Pod 和 PodGroup 的队列元数据。
// 队列的排序、退避和统计只需要这些共性字段，不需要知道实体具体是 Pod 还是组。
type QueuedEntityInfo interface {
	// Type returns the type of the entity.
	Type() EntityKeyType
	// GetPriority returns the priority of the entity.
	GetPriority() int32
	// GetTimestamp returns the time entity added to the scheduling queue.
	GetTimestamp() time.Time
	// GetAttempts returns the number of all schedule attempts before successfully scheduled.
	// It's used to record the # attempts metric.
	GetAttempts() int
	// GetBackoffExpiration returns the time when the entity will complete its backoff.
	// It's empty for Pods that belong to a pod group. QueuedPodGroupInfo's BackoffExpiration is used instead.
	// If the SchedulerPopFromBackoffQ feature is enabled, the value is aligned to the backoff ordering window.
	// Then, two entities with the same BackoffExpiration (time bucket) are ordered by priority and eventually the timestamp,
	// to make sure popping from the backoffQ considers priority of entities that are close to the expiration time.
	GetBackoffExpiration() time.Time
	// GetUnschedulableCount returns the total number of the scheduling attempts that this entity gets unschedulable.
	// Basically it equals Attempts, but when the entity fails with the Error status (e.g., the network error),
	// this count won't be incremented.
	// It's used to calculate the backoff time this entity is obliged to get before retrying.
	GetUnschedulableCount() int
	// GetConsecutiveErrorsCount returns the number of the error status that this entity gets sequentially.
	// This count is reset when the entity gets another status than Error.
	//
	// If the error status is returned (e.g., kube-apiserver is unstable), we don't want to immediately retry the entity and hence need a backoff retry mechanism
	// because that might push more burden to the kube-apiserver.
	// But, we don't want to calculate the backoff time in the same way as the normal unschedulable reason
	// since the purpose is different; the backoff for a unschedulable status etc is for the punishment of wasting the scheduling cycles,
	// whereas the backoff for the error status is for the protection of the kube-apiserver.
	// That's why we need to distinguish ConsecutiveErrorsCount for the error status and UnschedulableCount for the unschedulable status.
	// See https://github.com/kubernetes/kubernetes/issues/128744 for the discussion.
	GetConsecutiveErrorsCount() int
	// GetInitialAttemptTimestamp returns the time when the entity is added to the queue for the first time. The entity may be added
	// back to the queue multiple times before it's successfully scheduled.
	// It shouldn't be updated once initialized. It's used to record the e2e scheduling
	// latency for an entity.
	GetInitialAttemptTimestamp() *time.Time
	// GetUnschedulablePlugins records the plugin names that the entity failed with Unschedulable or UnschedulableAndUnresolvable status
	// at specific extension points: PreFilter, Filter, Reserve, or Permit (WaitOnPermit).
	// If entities are rejected at other extension points,
	// they're assumed to be unexpected errors (e.g., temporal network issue, plugin implementation issue, etc)
	// and retried soon after a backoff period.
	// That is because such failures could be solved regardless of incoming cluster events (registered in EventsToRegister).
	GetUnschedulablePlugins() sets.Set[string]
	// GetPendingPlugins records the plugin names that the entity failed with Pending status.
	GetPendingPlugins() sets.Set[string]
	// GetGatingPlugin records the plugin name that gated the entity at PreEnqueue.
	GetGatingPlugin() string
	// GetGatingPluginEvents records the events registered by the plugin that gated the entity at PreEnqueue.
	// We have it as a cache purpose to avoid re-computing which event(s) might ungate the entity.
	GetGatingPluginEvents() []ClusterEvent
}

// PodInfo is a wrapper to a Pod with additional pre-computed information to
// accelerate processing. This information is typically immutable (e.g., pre-processed
// inter-pod affinity selectors).
// PodInfo 缓存预处理过的亲和性条件：选择器解析一次后可反复使用。
// 亲和性在调度期间不变，所以这些派生数据可以安全地共享给多个插件。
type PodInfo interface {
	// GetPod returns the wrapped Pod
	GetPod() *v1.Pod
	// GetRequiredAffinityTerms returns the precomputed affinity terms.
	GetRequiredAffinityTerms() []AffinityTerm
	// GetRequiredAffinitRequiredAntiAffinityTermsyTerms returns the precomputed anti-affinity terms.
	GetRequiredAntiAffinityTerms() []AffinityTerm
	// GetPreferredAffinityTerms returns the precomputed affinity terms with weights.
	GetPreferredAffinityTerms() []WeightedAffinityTerm
	// GetPreferredAntiAffinityTerms returns the precomputed anti-affinity terms with weights.
	GetPreferredAntiAffinityTerms() []WeightedAffinityTerm
	// CalculateResource is only intended to be used by NodeInfo.
	CalculateResource() PodResource
}

// PodResource contains the result of CalculateResource and is intended to be used only internally.
type PodResource struct {
	Resource Resource
	Non0CPU  int64
	Non0Mem  int64
}

// AffinityTerm is a processed version of v1.PodAffinityTerm.
type AffinityTerm struct {
	Namespaces        sets.Set[string]
	Selector          labels.Selector
	TopologyKey       string
	NamespaceSelector labels.Selector
}

// Matches returns true if the pod matches the label selector and namespaces or namespace selector.
func (at *AffinityTerm) Matches(pod *v1.Pod, nsLabels labels.Set) bool {
	if at.Namespaces.Has(pod.Namespace) || at.NamespaceSelector.Matches(nsLabels) {
		return at.Selector.Matches(labels.Set(pod.Labels))
	}
	return false
}

// WeightedAffinityTerm is a "processed" representation of v1.WeightedAffinityTerm.
type WeightedAffinityTerm struct {
	AffinityTerm
	Weight int32
}

// GetAffinityTerms receives a Pod and affinity terms and returns the namespaces and
// selectors of the terms.
func GetAffinityTerms(pod *v1.Pod, v1Terms []v1.PodAffinityTerm) ([]AffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []AffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i])
		if err != nil {
			// We get here if the label selector failed to process
			return nil, err
		}
		terms = append(terms, *t)
	}
	return terms, nil
}

func newAffinityTerm(pod *v1.Pod, term *v1.PodAffinityTerm) (*AffinityTerm, error) {
	selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		return nil, err
	}

	namespaces := getNamespacesFromPodAffinityTerm(pod, term)
	nsSelector, err := metav1.LabelSelectorAsSelector(term.NamespaceSelector)
	if err != nil {
		return nil, err
	}

	return &AffinityTerm{Namespaces: namespaces, Selector: selector, TopologyKey: term.TopologyKey, NamespaceSelector: nsSelector}, nil
}

// getNamespacesFromPodAffinityTerm returns a set of names according to the namespaces indicated in podAffinityTerm.
// If namespaces is empty it considers the given pod's namespace.
func getNamespacesFromPodAffinityTerm(pod *v1.Pod, podAffinityTerm *v1.PodAffinityTerm) sets.Set[string] {
	names := sets.Set[string]{}
	if len(podAffinityTerm.Namespaces) == 0 && podAffinityTerm.NamespaceSelector == nil {
		names.Insert(pod.Namespace)
	} else {
		names.Insert(podAffinityTerm.Namespaces...)
	}
	return names
}

// GetPodAffinityTerms returns the list of PodAffinityTerms specified in the PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution field.
func GetPodAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAffinity != nil {
		if len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		// TODO: Uncomment this block when implement RequiredDuringSchedulingRequiredDuringExecution.
		// if len(affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
		//	terms = append(terms, affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
		// }
	}
	return terms
}

// GetWeightedAffinityTerms returns affinity terms with weights, namespaces and selectors of the terms.
func GetWeightedAffinityTerms(pod *v1.Pod, v1Terms []v1.WeightedPodAffinityTerm) ([]WeightedAffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []WeightedAffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i].PodAffinityTerm)
		if err != nil {
			// We get here if the label selector failed to process
			return nil, err
		}
		terms = append(terms, WeightedAffinityTerm{AffinityTerm: *t, Weight: v1Terms[i].Weight})
	}
	return terms, nil
}

// GetPodAntiAffinityTerms returns the list of PodAffinityTerms specified in the PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution field.
func GetPodAntiAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAntiAffinity != nil {
		if len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		// TODO: Uncomment this block when implement RequiredDuringSchedulingRequiredDuringExecution.
		// if len(affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
		//	terms = append(terms, affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
		// }
	}
	return terms
}

// Resource is a collection of compute resources.
// Resource 用 int64 的毫核/字节数表达资源量，避免浮点误差在比较和累加时累积。
// 方法集合刻意保持最小，只覆盖调度器真正需要的读取与取最大值操作。
type Resource interface {
	GetMilliCPU() int64
	GetMemory() int64
	GetEphemeralStorage() int64
	// We return AllowedPodNumber (which is Node.Status.Allocatable.Pods().Value())
	// explicitly as int, to avoid conversions and improve performance.
	GetAllowedPodNumber() int
	// ScalarResources returns a map for resource names to their scalar values
	GetScalarResources() map[v1.ResourceName]int64
	// SetMaxResource compares with ResourceList and takes max value for each Resource.
	SetMaxResource(rl v1.ResourceList)
}

// ImageStateSummary provides summarized information about the state of an image.
type ImageStateSummary struct {
	// Size of the image
	Size int64
	// Used to track how many nodes have this image, it is computed from the Nodes field below
	// during the execution of Snapshot.
	NumNodes int
	// A set of node names for nodes having this image present. This field is used for
	// keeping track of the nodes during update/add/remove events.
	Nodes sets.Set[string]
}

// Snapshot returns a copy without Nodes field of ImageStateSummary
func (iss *ImageStateSummary) Snapshot() *ImageStateSummary {
	return &ImageStateSummary{
		Size:     iss.Size,
		NumNodes: iss.Nodes.Len(),
	}
}

// DefaultBindAllHostIP defines the default ip address used to bind to all host.
const DefaultBindAllHostIP = "0.0.0.0"

// ProtocolPort represents a protocol port pair, e.g. tcp:80.
type ProtocolPort struct {
	Protocol string
	Port     int32
}

// NewProtocolPort creates a ProtocolPort instance.
// NewProtocolPort 把 TCP 作为缺省协议，因为绝大多数端口冲突检查都发生在 TCP 上；
// 由构造函数统一补全，后续 Add/Remove/CheckConflict 才能只比较规范化的键。
func NewProtocolPort(protocol string, port int32) *ProtocolPort {
	pp := &ProtocolPort{
		Protocol: protocol,
		Port:     port,
	}

	if len(pp.Protocol) == 0 {
		pp.Protocol = string(v1.ProtocolTCP)
	}

	return pp
}

// HostPortInfo stores mapping from ip to a set of ProtocolPort
type HostPortInfo map[string]map[ProtocolPort]struct{}

// Add adds (ip, protocol, port) to HostPortInfo
// Add 用两层 map 记录 (ip, protocol, port) 三元组，
// 这样检查冲突时可以先按 ip 定位，再按协议端口查找，省去全表扫描。
func (h HostPortInfo) Add(ip, protocol string, port int32) {
	if port <= 0 {
		return
	}

	h.sanitize(&ip, &protocol)

	pp := NewProtocolPort(protocol, port)
	if _, ok := h[ip]; !ok {
		h[ip] = map[ProtocolPort]struct{}{
			*pp: {},
		}
		return
	}

	h[ip][*pp] = struct{}{}
}

// Remove removes (ip, protocol, port) from HostPortInfo
// Remove 在最后一个端口被移除时连 ip 键一起删掉，避免残留空 map 让 Len 和冲突检查多走分支。
func (h HostPortInfo) Remove(ip, protocol string, port int32) {
	if port <= 0 {
		return
	}

	h.sanitize(&ip, &protocol)

	pp := NewProtocolPort(protocol, port)
	if m, ok := h[ip]; ok {
		delete(m, *pp)
		if len(h[ip]) == 0 {
			delete(h, ip)
		}
	}
}

// Len returns the total number of (ip, protocol, port) tuple in HostPortInfo
func (h HostPortInfo) Len() int {
	length := 0
	for _, m := range h {
		length += len(m)
	}
	return length
}

// CheckConflict checks if the input (ip, protocol, port) conflicts with the existing
// ones in HostPortInfo.
// CheckConflict 必须双向检查：绑定 0.0.0.0 会占用所有 IP，
// 而绑定具体 IP 时也要看是否已有 0.0.0.0 的监听，否则会漏判冲突。
func (h HostPortInfo) CheckConflict(ip, protocol string, port int32) bool {
	if port <= 0 {
		return false
	}

	h.sanitize(&ip, &protocol)

	pp := NewProtocolPort(protocol, port)

	// If ip is 0.0.0.0 check all IP's (protocol, port) pair
	if ip == DefaultBindAllHostIP {
		for _, m := range h {
			if _, ok := m[*pp]; ok {
				return true
			}
		}
		return false
	}

	// If ip isn't 0.0.0.0, only check IP and 0.0.0.0's (protocol, port) pair
	for _, key := range []string{DefaultBindAllHostIP, ip} {
		if m, ok := h[key]; ok {
			if _, ok2 := m[*pp]; ok2 {
				return true
			}
		}
	}

	return false
}

// sanitize the parameters
// sanitize 把空值归一成默认值，让上层传入的零值也能得到符合直觉的结果。
func (h HostPortInfo) sanitize(ip, protocol *string) {
	if len(*ip) == 0 {
		*ip = DefaultBindAllHostIP
	}
	if len(*protocol) == 0 {
		*protocol = string(v1.ProtocolTCP)
	}
}

// EntityKeyType is the type of an entity.
type EntityKeyType string

const (
	PodKeyType               EntityKeyType = "pod"
	PodGroupKeyType          EntityKeyType = "podgroup"
	CompositePodGroupKeyType EntityKeyType = "compositepodgroup"
)

// PodGroupInfo is a wrapper around the PodGroup API object together with a list of unscheduled pods that belong to the pod group.
// Typically used as an input to pod group scheduling cycle plugins.
// PodGroupInfo 把 API 对象与当前待调度 Pod 列表打包，供 pod group 调度周期的插件使用。
// 区分普通组和复合组，是因为后者没有对应的单一 API 对象，需要从子组聚合信息。
type PodGroupInfo interface {
	// GetUnscheduledPods returns pods that are currently being considered for scheduling.
	// The order of the pods is deterministic and based on signature, priority and timestamp.
	// This structure only contains the pods considered for scheduling in the pod group scheduling cycle.
	GetUnscheduledPods() []*v1.Pod

	// GetName returns the PodGroup name that is used to identify the pod group.
	GetName() string
	// GetNamespace returns the namespace the pod group belongs to.
	GetNamespace() string
	// GetType returns the type of the pod group.
	GetType() EntityKeyType
	// GetKey returns the key uniquely identifying the pod group.
	GetKey() string
	// GetPodGroup returns the PodGroup API object or nil if the group is a composite pod group.
	GetPodGroup() *schedulingv1beta1.PodGroup
	// GetCompositePodGroup returns the associated composite pod group or nil if the group is not a composite pod group.
	// It should only be used when the CompositePodGroup feature gate is enabled.
	GetCompositePodGroup() *schedulingv1alpha3.CompositePodGroup
	// GetChildren returns the child pod groups of this pod group.
	// Only composite pod groups have children.
	GetChildren() []PodGroupInfo
}

// Placement determines the resources to be considered when scheduling a pod group.
// Pod group scheduling cycle can check multiple placements and select the one that results
// in the best pod assignments.
// Placement 描述一次候选放置方案，名字仅用于诊断，不参与调度语义。
// 把候选节点集合限制在 Placement 内，便于对同一组尝试多种放置并比较优劣。
type Placement struct {
	// Name uniquely identifies the placement.
	// This is used for diagnostics and debugability.
	// The choice of the name is up to the PlacementGeneratePlugin.
	Name string
	// Nodes specifies the nodes that are valid for this placement.
	// Scheduler will try to schedule the pod group using only those nodes.
	Nodes []NodeInfo
}

// ProposedAssignment associates pod of a pod group with a proposed node assignment, determined in pod group scheduling cycle.
// ProposedAssignment 保存 pod group 调度周期中的临时结果；
// CycleState 一并带出，让后续阶段能复用当时的计算数据而不必重新计算。
type ProposedAssignment interface {
	// GetPod returns the pod that has the proposed node assignment.
	GetPod() *v1.Pod
	// GetPodInfo returns the PodInfo for the pod that has the proposed node assignment.
	// It should be treated as immutable, read only data.
	GetPodInfo() PodInfo
	// GetNodeName returns the name of the proposed node for the pod.
	GetNodeName() string
	// GetCycleState returns the CycleState computed for this pod during pod group scheduling cycle.
	GetCycleState() CycleState
}

// PodGroupAssignments holds the temporary assignments of pods in a pod group to nodes for a placement.
// Can be used in the pod group scheduling cycle to determine the best placement for a pod group.
type PodGroupAssignments struct {
	*Placement
	// ProposedAssignments associates pods with proposed nodes that were determined for a given placement
	// during the pod group scheduling cycle.
	// The pods are guaranteed to also be present in the PodGroupInfo.
	ProposedAssignments []ProposedAssignment
}

// EntityKey uniquely identifies a specific instance of an entity (like PodGroup or CompositePodGroup).
// EntityKey 用 (类型, 命名空间, 名字) 三元组区分不同实体，
// 因为同名的 Pod 和 PodGroup 可以同时存在，不能只用名字做键。
type EntityKey struct {
	Type      EntityKeyType
	Name      string
	Namespace string
}

func (ek EntityKey) GetName() string {
	return ek.Name
}

func (ek EntityKey) GetNamespace() string {
	return ek.Namespace
}

func (ek EntityKey) GetType() EntityKeyType {
	return ek.Type
}

// String 按“类型/命名空间/名字”拼接，作为键时比单独的 Name 更不容易碰撞。
func (ek EntityKey) String() string {
	return fmt.Sprintf("%s/%s/%s", ek.Type, ek.Namespace, ek.Name)
}

// MustParseEntityKey returns the entity key for a given key.
// It should be only used in tests.
// MustParseEntityKey 对非法输入返回零值而不是 panic，方便在测试和日志解析里宽容处理坏数据；
// “Must”只是命名习惯，调用方仍需检查返回值是否有效。
func MustParseEntityKey(key string) EntityKey {
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return EntityKey{}
	}
	return EntityKey{Type: EntityKeyType(parts[0]), Namespace: parts[1], Name: parts[2]}
}

// PodKey returns the key for a pod.
// 下面几个构造函数集中管理 EntityKey 的类型字段，避免调用方手写字符串时拼错类型。
func PodKey(namespace, name string) EntityKey {
	return EntityKey{Type: PodKeyType, Namespace: namespace, Name: name}
}

// PodGroupKey returns the key for a pod group.
func PodGroupKey(namespace, name string) EntityKey {
	return EntityKey{Type: PodGroupKeyType, Namespace: namespace, Name: name}
}

// CompositePodGroupKey returns the key for a composite pod group.
func CompositePodGroupKey(namespace, name string) EntityKey {
	return EntityKey{Type: CompositePodGroupKeyType, Namespace: namespace, Name: name}
}
