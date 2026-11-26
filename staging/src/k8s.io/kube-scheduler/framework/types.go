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

// ActionType 是以整数表示一种资源变更类型的位掩码，
// 不同 ActionTypes 可以按位组合出新的语义。
// 用位掩码表示资源变更，使插件可以同时注册“多个细分事件”（例如一次声明所有节点更新），
// 而调度器在分发时仍能快速判断某个事件是否命中。
type ActionType int64

// ActionTypes 的常量定义。
// 贡献者注意：新增 ActionType 时，必须同步更新以下内容：
// - k/k/pkg/scheduler/framework/types.go 中 basicActionTypes、podActionTypes 和 nodeActionTypes 列表
// - String() 方法。
const (
	Add ActionType = 1 << iota
	Delete

	// 各细分事件必须是独立的单比特位，才能用按位或组合成下面的 Update 和 All。
	// 这样插件注册一个组合事件，调度器分发时按位测试即可，无需逐个比较。

	// UpdateNodeXYZ 系列仅适用于 Node 事件。使用它们后，插件的 QueueingHint 只会对
	// 对应的细分 sub-Update 事件执行；比起直接用 Update 事件，收窄事件范围能提升
	// 重新入队的性能。即让插件只关心真正会影响自己的那部分变化，
	// 避免每次节点状态抖动都触发无意义的重新入队。
	UpdateNodeAllocatable
	UpdateNodeLabel
	// UpdateNodeTaint 表示节点污点或 node.Spec.Unschedulable 的变化。
	UpdateNodeTaint
	UpdateNodeCondition
	UpdateNodeAnnotation
	// UpdateNodeDeclaredFeature 表示节点声明特性的变化。
	UpdateNodeDeclaredFeature
	// UpdateNodePreemptionPolicy 表示节点抢占策略的变化。
	UpdateNodePreemptionPolicy

	// UpdatePodXYZ 系列仅适用于 Pod 事件。使用它们后，插件的 QueueingHint 只会对
	// 对应的细分 sub-Update 事件执行；比起直接用 Update 事件，收窄事件范围能提升
	// 重新入队的性能。同理，Pod 的更新按字段细分；重入队本身有开销，越早过滤掉无关事件越好。
	UpdatePodLabel
	// UpdatePodScaleDown 表示 Pod 缩容（任一资源请求减少）。
	UpdatePodScaleDown
	// UpdatePodScaleUp 表示 Pod 扩容（任一资源请求增加）。
	UpdatePodScaleUp
	// UpdatePodToleration 表示新增 Pod toleration。
	// API 校验只允许新增 toleration，不能修改或删除。
	UpdatePodToleration
	// UpdatePodSchedulingGatesEliminated 表示 Pod 的调度 gate 被全部清除。
	UpdatePodSchedulingGatesEliminated
	// UpdatePodGeneratedResourceClaim 表示为该 Pod 生成的 ResourceClaim 列表发生变化。
	// 依赖 DynamicResourceAllocation 特性开关。
	UpdatePodGeneratedResourceClaim

	All ActionType = 1<<iota - 1
	// All 是所有已定义比特的并集，新增 ActionType 时它会自动扩展，无需手动维护。

	// 如果你不确定或不关心该用哪个具体的 sub-Update 类型，可以使用通用的 Update 类型。
	// Update 是各细分更新位的并集，供不关心具体更新内容的插件使用；
	// 用组合位而非单独一个新位，保证细分事件也能命中宽泛订阅。
	Update = UpdateNodeAllocatable | UpdateNodeLabel | UpdateNodeTaint | UpdateNodeCondition | UpdateNodeAnnotation | UpdateNodeDeclaredFeature | UpdateNodePreemptionPolicy | UpdatePodLabel | UpdatePodScaleDown | UpdatePodScaleUp | UpdatePodToleration | UpdatePodSchedulingGatesEliminated | UpdatePodGeneratedResourceClaim

	// None 是仅供内部使用的特殊 ActionType。零值表示“没有任何动作”，作为无效值哨兵，
	// 避免把空事件误判成某种变更。
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

	// 理论上不应走到这里。未知位返回空串，方便调用方把它当无效值过滤掉，而不是打印出误导性的数字。
	return ""
}

// EventResource 基本上是 group/version/kind 的简称，可唯一表示某个 API 资源。
// 用字符串标识资源类型，既覆盖内置资源也覆盖扩展资源（带 group 前缀），
// 插件注册事件时无需依赖具体的 Go 类型。
type EventResource string

// GVK 的常量定义。
//
// 贡献者注意：新增 EventResource 时，必须把它注册到 k/k/pkg/scheduler/framework/types.go 的 allResources 中。
const (
	// 关于调度器如何通知 Pod 事件，有几点说明：
	// - Add：可能由新建的 Pod 触发，也可能由被调度到某个 Node 上的既有 Pod 触发。
	// - Delete：可能由以下情况触发：
	//           - 被删除的 Pod
	//           - 已 assume、但因绑定周期中出现某些错误而 un-assume 的 Pod
	//           - 原本未调度、但被调度到某个 Node 上的既有 Pod
	//
	// 注意：Pod 事件类型也包含未调度 Pod 自身的事件。
	// 即未调度的 Pod 被更新时，调度队列会通过 Pod/Update 的 QueueingHint(s) 检查该更新
	// 是否可能让 Pod 变得可调度，只要有一个 QueueingHint 返回 Queue，就将其重新入队到
	// activeQ/backoffQ。若插件的拒绝可以通过更新未调度 Pod 自身来化解，
	// 插件**必须**为 Pod/Update 事件实现 QueueingHint。
	// 示例：请求过多资源的 Pod 可能被 noderesources 插件拒绝；如果这个未调度的 Pod
	// 被更新为请求更少的资源，noderesources 之前的拒绝就可以化解。
	// 该插件就应实现针对 Pod/Update 事件的 QueueingHint，
	// 在未调度 Pod 发生此类变更时返回 Queue。
	//
	// Pod 是一个通用资源，包含三个具体的 Pod 资源：AssignedPod、UnscheduledPod 和 TargetPod。
	// 出于性能考虑，插件可以（也应当）注册具体的 Pod 事件。
	// Pod 是宽泛兜底项，只有无法判断该关心哪类 Pod 时才用它。
	Pod EventResource = "Pod"
	// AssignedPod 对应“已调度的 Pod 被更新”这一集群事件。
	// 已绑定的 Pod 变化通常意味着资源占用变化，影响正在排队的其他 Pod。
	AssignedPod EventResource = "AssignedPod"
	// UnscheduledPod 对应“除目标 Pod 以外的未调度 Pod 被更新”这一集群事件。
	// 其他待调度 Pod 的变化，可能让某个被拒绝的 Pod 重新有机会（例如它们释放了竞争资源）。
	UnscheduledPod EventResource = "UnscheduledPod"
	// TargetPod 对应“未调度 Pod 自身被更新”这一集群事件。
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

	// WildCard 是能匹配所有资源的特殊 EventResource。
	// 例如，在 EventsToRegister 里注册 `{Resource: "*", ActionType: All}`，
	// 所有到来的 clusterEvent 都会被接纳。除非确实需要，注册它要谨慎，
	// 它会推高重新入队的计算压力。
	//
	// 同时，如果到来的 clusterEvent 本身是通配符事件，所有 Pod
	// 都会被强制从 unschedulablePod 池移动到 activeQ/backoffQ。
	// 通配符绕过 QueueingHint 直接全量重入队，代价高，只在确实无法定位具体资源时使用。
	//
	// 注意：
	// - UpdatePodXYZ 或 UpdateNodeXYZ：由更新 Pod 或 Node 的特定部分触发，例如 updatePodLabel。
	// 与笼统事件（update）相比，使用具体事件（updatePodLabel）能让重入队流程更高效、
	// 更省内存，因为调度器缓存的事件更少。
	WildCard EventResource = "*"
)

// ClusterEventWithHint 把“发生了什么”和“怎么判断要不要重调度”绑在一起注册。
//
// 这样插件注册事件时就自带过滤逻辑，调度器不必为每种事件再回头查一遍插件实现。
type ClusterEventWithHint struct {
	Event ClusterEvent
	// 当上述 Event 发生时，QueueingHintFn 会针对曾被本插件拒绝的 Pod 执行，
	// 过滤掉无关事件，减少 Pod 调度的无谓重试。该字段可选；若不设置，
	// 此 Event 发生时 Pod 的调度总是带退避地重试（与返回 Queue 等效）。
	// 两级过滤：PreQueueingHintFn 先粗筛出可能受影响的 Pod，QueueingHintFn 再逐个精筛，
	// 避免对每个事件都遍历全部不可调度 Pod。
	QueueingHintFn QueueingHintFn
	// PreQueueingHintFn 每个事件调用一次，用于收窄要交给 QueueingHint 评估的 Pod 集合。
	// 若设置，则只检查返回的 PreQueueingHintResult 中指明的 Pod。
	// 粗筛成本低、精度差，精筛成本高；分两层是为了在事件很多时省下大量精筛调用。
	PreQueueingHintFn PreQueueingHintFn
}

// QueueingHintFn 返回一个提示，表示该事件能否让一个在本插件过去的调度周期中被拒绝的
// Pod 重新变得可调度。它会在 Pod 从 unschedulableQ 移动到 backoffQ 或 activeQ 之前被调用。
// 若返回错误，无论这里返回什么，调用方都会把返回的 QueueingHint 当作 `Queue` 处理，
// 以避免 Pod 卡在不可调度 Pod 池中。
//
// - `pod`：待入队的 Pod，即过去被本插件拒绝的 Pod。
// - `oldObj` `newObj`：该事件涉及的对象。
//   - 例如，事件是 "Node deleted" 时，`oldObj` 就是那个被删除的 Node。
//   - 事件为 add 时 `oldObj` 为 nil。
//   - 事件为 delete 时 `newObj` 为 nil。
//
// 错误一律按 Queue 处理，是故意偏向“多调度一次”而非“漏调度”：
// 宁可多付出一次调度周期，也不能让本可运行的 Pod 永久卡在不可调度队列里。
type QueueingHintFn func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (QueueingHint, error)

// PreQueueingHintResult 是 PreQueueingHintFn 的返回值。
// AllPods 为 true 时评估所有不可调度 Pod（即现有行为）；
// 为 false 时只评估 Pods 中列出的 Pod。
// 两种模式兼顾兼容与性能：不实现粗筛的插件走 AllPods 保持旧行为，
// 能精确定位的插件只列出候选，减少后续逐 Pod 判断的开销。
type PreQueueingHintResult struct {
	AllPods bool
	Pods    []types.NamespacedName
}

// PreQueueingHintFn 在每个事件迭代不可调度 Pod 之前被调用一次。
// 当被删除的对象未知时（oldObj 和 newObj 均为 nil），不会调用 PreQueueingHintFn。
// 它返回一个 PreQueueingHintResult，指明该事件下哪些 Pod 应由 QueueingHintFn 评估；
// 若返回错误，该错误会被记录日志，且结果按 AllPods 处理（评估所有不可调度 Pod）。
//
// - oldObj 是更新事件中的旧对象，或删除事件中被删除的对象。
// - newObj 是新加入/新增的对象；删除事件时为 nil。
//
// 出错时退化为 AllPods，与 QueueingHintFn 一样遵循“漏调度比多调度更糟”的原则。
// 删除对象未知时不调用，是因为此时没有可用信息做粗筛，调用也不会得到有意义的结果。
type PreQueueingHintFn func(logger klog.Logger, oldObj, newObj interface{}) (PreQueueingHintResult, error)

// QueueingHint 只表达“要不要重新入队”这一种决策，
// 细节判断留给插件，避免调度器理解每种插件的业务语义。
type QueueingHint int

const (
	// QueueSkip 表示该集群事件对 Pod 是否可调度
	// 的调度。
	QueueSkip QueueingHint = iota

	// Queue 表示该事件可能让 Pod 变得可调度。
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

// ClusterEvent 抽象了系统资源状态的变化方式：Resource 表示 Pod、Node 这类标准 API 资源，
// ActionType 指明具体的变更（如 Add、Update 或 Delete）。
// ClusterEvent 是插件与调度队列之间的通用事件格式。
// CustomLabel 让少数没有对应 GVK 的事件（如超时）也能进入同一套指标体系。
type ClusterEvent struct {
	Resource   EventResource
	ActionType ActionType

	// CustomLabel 描述该集群事件。
	// 该可选字段控制 Label()，后者用于日志与指标。
	// 通常无需设置，只用于 UnschedulableTimeout 之类的特殊事件。
	CustomLabel string
}

// Label 用于日志与指标：生成稳定的指标标签；自定义标签优先，保证特殊事件不会污染通用事件的标签维度。
func (ce ClusterEvent) Label() string {
	if ce.CustomLabel != "" {
		return ce.CustomLabel
	}

	return fmt.Sprintf("%v%v", ce.Resource, ce.ActionType)
}

// NodeInfo 是节点级别的聚合信息，把节点及其上所有 Pod 的状态缓存成一份快照视图。
// 预计算这些派生数据，是因为 Filter/Score 阶段会被频繁调用，每次重算代价过高。
// generation 字段让快照可以判断数据是否变化，从而跳过不必要的克隆。
type NodeInfo interface {
	// Node 返回节点的整体信息。
	Node() *v1.Node
	// GetPods 返回节点上运行的 Pod。
	GetPods() []PodInfo
	// GetPodsWithAffinity 返回带 Pod 间（反）亲和约束的 Pod 子集。
	GetPodsWithAffinity() []PodInfo
	// GetPodsWithRequiredAntiAffinity 返回带硬性反亲和约束的 Pod 子集。
	GetPodsWithRequiredAntiAffinity() []PodInfo
	// GetPodsWithRequiredNonHostScopedAntiAffinity 返回带非 hostname 作用域硬性反亲和的 Pod 子集。
	// InterPodAffinityHostnameFastPath 关闭时必须返回空。
	GetPodsWithRequiredNonHostScopedAntiAffinity() []PodInfo
	// GetUsedPorts 返回节点上已分配的端口。
	GetUsedPorts() HostPortInfo
	// GetRequested 返回节点上所有 Pod 的请求量总和，包括已发送绑定但可能尚未调度的
	// 的 assumed Pod。
	GetRequested() Resource
	// GetNonZeroRequested 在汇总请求量时，对每个容器的 CPU/内存请求施加最小值。
	// 它不反映节点上实际的资源请求量，
	// 而是用来避免把资源请求极小的 Pod 都调度到同一节点。
	// 避免把大量零请求 Pod 调度到同一节点。
	GetNonZeroRequested() Resource
	// allocatedResources（即 Node.Status.Allocatable.*）显式存为 int64，
	// 避免反复查 map 与类型转换。
	GetAllocatable() Resource
	// GetImageStates 仅当镜像在节点上时才返回其条目，可用于
	// 判断镜像是否存在，以及基于镜像位置做调度等高级决策。
	// state information.
	GetImageStates() map[string]*ImageStateSummary
	// GetPVCRefCounts 返回 PVC 名到节点上使用它的 Pod 数的映射。
	// Keys are in the format "namespace/name".
	GetPVCRefCounts() map[string]int
	// NodeInfo 每次变化都会递增 generation。
	// 据此在未变化时跳过克隆。
	GetGeneration() int64
	// GetNodeDeclaredFeatures 返回节点声明的特性集合。
	GetNodeDeclaredFeatures() ndf.FeatureSet
	// Snapshot 复制该节点，但 ImageStates 不复制 Nodes 字段。
	Snapshot() NodeInfo
	// String 返回 NodeInfo 的可读表示。
	String() string

	// AddPodInfo 把 Pod 信息加入该 NodeInfo。
	// 若已算好 PodInfo，优先用它而非 AddPod。
	AddPodInfo(podInfo PodInfo)
	// RemovePod 从该 NodeInfo 中扣减 Pod 信息。
	RemovePod(logger klog.Logger, pod *v1.Pod) error
	// SetNode 设置节点的整体信息。
	SetNode(node *v1.Node)
}

// QueuedEntityInfo 是表示调度队列中一个可调度实体的接口，
// 具体可以是单个 Pod（QueuedPodInfo）或一组 Pod（QueuedPodGroupInfo）。
// QueuedEntityInfo 统一单个 Pod 和 PodGroup 的队列元数据。
// 队列的排序、退避和统计只需要这些共性字段，不需要知道实体具体是 Pod 还是组。
type QueuedEntityInfo interface {
	// Type returns the type of the entity.
	Type() EntityKeyType
	// GetPriority 返回实体的优先级。
	GetPriority() int32
	// GetTimestamp 返回实体入调度队列的时间。
	GetTimestamp() time.Time
	// GetAttempts 返回成功调度前的全部尝试次数。
	// 用于记录尝试次数指标。
	GetAttempts() int
	// GetBackoffExpiration 返回实体退避结束的时间。
	// 属于 PodGroup 的 Pod 此值为空，改由 QueuedPodGroupInfo 承载。
	// 启用 SchedulerPopFromBackoffQ 时，该值会对齐到退避排序窗口。
	// 同一时间桶内的实体再按优先级、最后按时间戳排序，
	// 确保退避队列出队时兼顾临近到期实体的优先级。
	GetBackoffExpiration() time.Time
	// GetUnschedulableCount 返回该实体被判定不可调度的尝试总数。
	// 通常等于 Attempts，但 Error 状态（如网络错误）的失败
	// this count won't be incremented.
	// 不计入其中；该值用于计算重试前的退避时长。
	GetUnschedulableCount() int
	// GetConsecutiveErrorsCount 返回连续 Error 状态的次数。
	// 出现非 Error 状态时清零。
	//
	// 返回 Error 状态时（如 kube-apiserver 不稳定），不应立即重试，而需退避，
	// 否则会给 kube-apiserver 增加更多负担。
	// Error 的退避不能与普通不可调度同一算法，
	// 因为目的不同：不可调度的退避是对浪费调度周期的惩罚，
	// 而 Error 的退避是为了保护 kube-apiserver。
	// 因此需要区分 Error 用的 ConsecutiveErrorsCount 与不可调度用的 UnschedulableCount。
	// See https://github.com/kubernetes/kubernetes/issues/128744 for the discussion.
	GetConsecutiveErrorsCount() int
	// GetInitialAttemptTimestamp 返回实体首次入队的时间。实体在成功调度前
	// 可能被多次重新入队。
	// 初始化后不应再更新该值，用于记录端到端调度
	// latency for an entity.
	GetInitialAttemptTimestamp() *time.Time
	// GetUnschedulablePlugins 记录实体在
	// PreFilter、Filter、Reserve 或 Permit（WaitOnPermit）阶段返回不可调度状态的插件名。
	// 其它扩展点的拒绝被视为
	// 非预期错误（如临时网络问题、插件实现问题等），
	// 退避后很快重试。
	// 因为这类失败与集群事件无关，无论哪些事件发生都无法解决。
	GetUnschedulablePlugins() sets.Set[string]
	// GetPendingPlugins 记录让实体返回 Pending 状态的插件名。
	GetPendingPlugins() sets.Set[string]
	// GetGatingPlugin 记录在 PreEnqueue 阶段挡住实体的插件名。
	GetGatingPlugin() string
	// GetGatingPluginEvents 记录该 gating 插件注册的事件。
	// 缓存这些事件，避免重复计算哪些事件可能解除阻塞。
	GetGatingPluginEvents() []ClusterEvent
}

// PodInfo 是对 Pod 的包装，附带为加速处理而预计算的信息，
// 这些信息通常不可变（如预处理过的 Pod 间亲和性选择器）。
// PodInfo 缓存预处理过的亲和性条件：选择器解析一次后可反复使用。
// 亲和性在调度期间不变，所以这些派生数据可以安全地共享给多个插件。
type PodInfo interface {
	// GetPod 返回被包装的 Pod。
	GetPod() *v1.Pod
	// GetRequiredAffinityTerms 返回预计算的亲和项。
	GetRequiredAffinityTerms() []AffinityTerm
	// GetRequiredAntiAffinityTerms 返回预计算的反亲和项。
	GetRequiredAntiAffinityTerms() []AffinityTerm
	// GetPreferredAffinityTerms 返回带权重的预计算亲和项。
	GetPreferredAffinityTerms() []WeightedAffinityTerm
	// GetPreferredAntiAffinityTerms 返回带权重的预计算反亲和项。
	GetPreferredAntiAffinityTerms() []WeightedAffinityTerm
	// CalculateResource 仅供 NodeInfo 使用。
	CalculateResource() PodResource
}

// PodResource 保存 CalculateResource 的结果，仅限内部使用。
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

// Pod 同时匹配 label selector 与 namespace/命名空间选择器时返回 true。
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

// GetAffinityTerms 接收 Pod 与亲和项，返回命名空间及
// selectors of the terms.
func GetAffinityTerms(pod *v1.Pod, v1Terms []v1.PodAffinityTerm) ([]AffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []AffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i])
		if err != nil {
			// 走到这里说明 label selector 解析失败，
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

// getNamespacesFromPodAffinityTerm 按项中的 namespaces 返回名字集合。
// 为空时取给定 Pod 所在的命名空间。
func getNamespacesFromPodAffinityTerm(pod *v1.Pod, podAffinityTerm *v1.PodAffinityTerm) sets.Set[string] {
	names := sets.Set[string]{}
	if len(podAffinityTerm.Namespaces) == 0 && podAffinityTerm.NamespaceSelector == nil {
		names.Insert(pod.Namespace)
	} else {
		names.Insert(podAffinityTerm.Namespaces...)
	}
	return names
}

// GetPodAffinityTerms 返回 PodAffinity 硬约束字段中的亲和项列表。
func GetPodAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAffinity != nil {
		if len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后取消此段注释。
		// if len(affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
		//	terms = append(terms, affinity.PodAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
		// }
	}
	return terms
}

// GetWeightedAffinityTerms 返回带权重、命名空间和选择器的亲和项。
func GetWeightedAffinityTerms(pod *v1.Pod, v1Terms []v1.WeightedPodAffinityTerm) ([]WeightedAffinityTerm, error) {
	if v1Terms == nil {
		return nil, nil
	}

	var terms []WeightedAffinityTerm
	for i := range v1Terms {
		t, err := newAffinityTerm(pod, &v1Terms[i].PodAffinityTerm)
		if err != nil {
			// 走到这里说明 label selector 解析失败，
			return nil, err
		}
		terms = append(terms, WeightedAffinityTerm{AffinityTerm: *t, Weight: v1Terms[i].Weight})
	}
	return terms, nil
}

// GetPodAntiAffinityTerms 返回 PodAntiAffinity 硬约束字段中的反亲和项列表。
func GetPodAntiAffinityTerms(affinity *v1.Affinity) (terms []v1.PodAffinityTerm) {
	if affinity != nil && affinity.PodAntiAffinity != nil {
		if len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			terms = affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后取消此段注释。
		// if len(affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution) != 0 {
		//	terms = append(terms, affinity.PodAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution...)
		// }
	}
	return terms
}

// Resource 是计算资源的集合，用 int64 的毫核/字节数表达资源量，
// 避免浮点误差在比较和累加时累积。
// 方法集合刻意保持最小，只覆盖调度器真正需要的读取与取最大值操作。
type Resource interface {
	GetMilliCPU() int64
	GetMemory() int64
	GetEphemeralStorage() int64
	// We return AllowedPodNumber (which is Node.Status.Allocatable.Pods().Value())
	// 显式存为 int，避免转换开销。
	GetAllowedPodNumber() int
	// ScalarResources 返回资源名到标量值的映射。
	GetScalarResources() map[v1.ResourceName]int64
	// SetMaxResource 与 ResourceList 逐项比较并取最大值。
	SetMaxResource(rl v1.ResourceList)
}

// ImageStateSummary 汇总镜像状态信息。
type ImageStateSummary struct {
	// Size of the image
	Size int64
	// 用于统计有多少节点持有该镜像，由下面的 Nodes 字段计算得出。
	// during the execution of Snapshot.
	NumNodes int
	// 持有该镜像的节点名集合，用于
	// 在更新/新增/删除事件中跟踪节点。
	Nodes sets.Set[string]
}

// Snapshot 返回不含 Nodes 字段的 ImageStateSummary 副本。
func (iss *ImageStateSummary) Snapshot() *ImageStateSummary {
	return &ImageStateSummary{
		Size:     iss.Size,
		NumNodes: iss.Nodes.Len(),
	}
}

// DefaultBindAllHostIP 是bind 到所有主机时使用的默认 IP。
const DefaultBindAllHostIP = "0.0.0.0"

// ProtocolPort 表示协议与端口对，例如 tcp:80。
type ProtocolPort struct {
	Protocol string
	Port     int32
}

// NewProtocolPort 创建 ProtocolPort 实例，把 TCP 作为缺省协议，
// 因为绝大多数端口冲突检查都发生在 TCP 上；
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

// HostPortInfo 保存 IP 到 ProtocolPort 集合的映射。
type HostPortInfo map[string]map[ProtocolPort]struct{}

// Add 把 (ip, protocol, port) 加入 HostPortInfo。HostPortInfo 用两层 map 记录三元组，
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

// Remove 从 HostPortInfo 移除 (ip, protocol, port)；在某个 ip 下最后一个端口被移除时
// 连 ip 键一起删掉，避免残留空 map 让 Len 和冲突检查多走分支。
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

// Len 返回 HostPortInfo 中 (ip, protocol, port) 三元组的总数。
func (h HostPortInfo) Len() int {
	length := 0
	for _, m := range h {
		length += len(m)
	}
	return length
}

// CheckConflict 检查输入 (ip, protocol, port) 是否与 HostPortInfo 中已有条目冲突。
// 它必须双向检查：绑定 0.0.0.0 会占用所有 IP，
// 而绑定具体 IP 时也要看是否已有 0.0.0.0 的监听，否则会漏判冲突。
func (h HostPortInfo) CheckConflict(ip, protocol string, port int32) bool {
	if port <= 0 {
		return false
	}

	h.sanitize(&ip, &protocol)

	pp := NewProtocolPort(protocol, port)

	// ip 为 0.0.0.0 时检查所有 IP 的 (protocol, port) 对。
	if ip == DefaultBindAllHostIP {
		for _, m := range h {
			if _, ok := m[*pp]; ok {
				return true
			}
		}
		return false
	}

	// 否则只检查该 IP 与 0.0.0.0 的 (protocol, port) 对。
	for _, key := range []string{DefaultBindAllHostIP, ip} {
		if m, ok := h[key]; ok {
			if _, ok2 := m[*pp]; ok2 {
				return true
			}
		}
	}

	return false
}

// sanitize 归一化参数：把空值归一成默认值，让上层传入的零值也能得到符合直觉的结果。
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

// PodGroupInfo 把 PodGroup API 对象与属于该组的未调度 Pod 列表打包在一起，
// 通常作为 pod group 调度周期插件的输入。
// 区分普通组和复合组，是因为后者没有对应的单一 API 对象，需要从子组聚合信息。
type PodGroupInfo interface {
	// GetUnscheduledPods 返回当前正在参与调度决策的 Pod。
	// 顺序由签名、优先级与时间戳确定，保证可复现。
	// This structure only contains the pods considered for scheduling in the pod group scheduling cycle.
	GetUnscheduledPods() []*v1.Pod

	// GetName 返回标识该 PodGroup 的名字。
	GetName() string
	// GetNamespace 返回 PodGroup 所属的命名空间。
	GetNamespace() string
	// GetType returns the type of the pod group.
	GetType() EntityKeyType
	// GetKey 返回唯一标识该 PodGroup 的 key。
	GetKey() string
	// GetPodGroup 返回 PodGroup API 对象；组合组返回 nil。
	GetPodGroup() *schedulingv1beta1.PodGroup
	// GetCompositePodGroup 返回关联的组合 PodGroup；非组合组返回 nil。
	// 仅在启用 CompositePodGroup 特性开关时使用。
	GetCompositePodGroup() *schedulingv1alpha3.CompositePodGroup
	// GetChildren 返回该 PodGroup 的子组。
	// 只有组合 PodGroup 才有子组。
	GetChildren() []PodGroupInfo
}

// Placement 决定调度一个 pod group 时要考虑哪些资源。pod group 调度周期可以
// 检查多个 Placement，选出最终 Pod 分配效果最好的一个。
// Placement 描述一次候选放置方案，名字仅用于诊断，不参与调度语义。
// 把候选节点集合限制在 Placement 内，便于对同一组尝试多种放置并比较优劣。
type Placement struct {
	// Name 唯一标识该 Placement。
	// 用于诊断与调试。
	// 命名由 PlacementGeneratePlugin 决定。
	Name string
	// Nodes 指定该 Placement 可用的节点。
	// 调度器只会尝试在这些节点上调度该 PodGroup。
	Nodes []NodeInfo
}

// ProposedAssignment 保存 pod group 调度周期中确定的结果：组内 Pod 与其候选节点的对应关系；
// CycleState 一并带出，让后续阶段能复用当时的计算数据而不必重新计算。
type ProposedAssignment interface {
	// GetPod 返回已获得建议节点的 Pod。
	GetPod() *v1.Pod
	// GetPodInfo 返回该 Pod 的 PodInfo。
	// 应视为不可变的只读数据。
	GetPodInfo() PodInfo
	// GetNodeName 返回为该 Pod 建议的节点名。
	GetNodeName() string
	// GetCycleState 返回 PodGroup 调度周期内为该 Pod 计算的 CycleState。
	GetCycleState() CycleState
}

// PodGroupAssignments 保存某 Placement 下 PodGroup 成员到节点的临时分配。
// 用于 PodGroup 调度周期中挑选最佳 Placement。
type PodGroupAssignments struct {
	*Placement
	// ProposedAssignments 把某 Placement 下确定的建议节点与 Pod 关联起来，
	// 该结果产生于 PodGroup 调度周期。
	// 这些 Pod 一定也能在 PodGroupInfo 中找到。
	ProposedAssignments []ProposedAssignment
}

// EntityKey 唯一标识某个实体（如 PodGroup 或 CompositePodGroup）的具体实例。
// 它用 (类型, 命名空间, 名字) 三元组区分不同实体，
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

// MustParseEntityKey 返回给定 key 对应的实体键，只应在测试中使用。
// 它对非法输入返回零值而不是 panic，方便在测试和日志解析里宽容处理坏数据；
// “Must”只是命名习惯，调用方仍需检查返回值是否有效。
func MustParseEntityKey(key string) EntityKey {
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return EntityKey{}
	}
	return EntityKey{Type: EntityKeyType(parts[0]), Namespace: parts[1], Name: parts[2]}
}

// PodKey 返回某个 pod 的实体键。下面几个构造函数集中管理 EntityKey 的类型字段，
// 避免调用方手写字符串时拼错类型。
func PodKey(namespace, name string) EntityKey {
	return EntityKey{Type: PodKeyType, Namespace: namespace, Name: name}
}

// PodGroupKey 返回 PodGroup 的 key。
func PodGroupKey(namespace, name string) EntityKey {
	return EntityKey{Type: PodGroupKeyType, Namespace: namespace, Name: name}
}

// CompositePodGroupKey 返回组合 PodGroup 的 key。
func CompositePodGroupKey(namespace, name string) EntityKey {
	return EntityKey{Type: CompositePodGroupKeyType, Namespace: namespace, Name: name}
}
