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

package v1beta2

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// ControllerRevisionHashLabelKey 是用于标识 ControllerRevision 哈希的标签键。
	ControllerRevisionHashLabelKey = "controller-revision-hash"
	// StatefulSetRevisionLabel 是 StatefulSet 修订版本标签，与 ControllerRevisionHashLabelKey 相同。
	StatefulSetRevisionLabel = ControllerRevisionHashLabelKey
	// DeprecatedRollbackTo 是已废弃的注解键，用于记录 Deployment 回滚的目标版本。
	DeprecatedRollbackTo = "deprecated.deployment.rollback.to"
	// DeprecatedTemplateGeneration 是已废弃的注解键，用于记录 DaemonSet 模板的生成号。
	DeprecatedTemplateGeneration = "deprecated.daemonset.template.generation"
	// StatefulSetPodNameLabel 是标识 Pod 所属 StatefulSet 名称的标签键。
	StatefulSetPodNameLabel = "statefulset.kubernetes.io/pod-name"
)

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// 被伸缩对象期望的实例数量。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=0
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// 被伸缩对象实际观测到的实例数量。
	// +optional
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// selector 是针对应与副本数匹配的 Pod 的标签查询。更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	// +optional
	// +mapType=atomic
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`

	// 针对应与副本数匹配的 Pod 的标签选择器。这是基于 map 的选择器与表达能力更强的
	// 基于集合的选择器二者的序列化形式，如此一来客户端无需再做内省即可使用。
	// 字符串格式与查询参数语法一致。如果目标类型仅支持基于 map 的选择器，
	// 则该字段与基于 map 的选择器字段都会被填充。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	TargetSelector string `json:"targetSelector,omitempty" protobuf:"bytes,3,opt,name=targetSelector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=autoscaling,v1,Scale
// +k8s:isSubresource="/scale"

// Scale 表示对某个资源的伸缩请求。
type Scale struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 定义伸缩行为。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec ScaleSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 伸缩操作的当前状态。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。只读。
	// +optional
	Status ScaleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=Scale,result=Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=Scale,result=Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,StatefulSet

// 已废弃 —— 该 GroupVersion 的 StatefulSet 已被 apps/v1/StatefulSet 取代，详见发布说明。
// StatefulSet 表示一组具有一致身份标识的 Pod。
// 身份标识定义为：
//   - 网络：单一且稳定的 DNS 名称与主机名。
//   - 存储：按需提供任意数量的 VolumeClaim。
//
// StatefulSet 保证给定的网络身份始终映射到相同的存储身份。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type StatefulSet struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义该集合中 Pod 期望的身份标识。
	// +required
	Spec StatefulSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是该 StatefulSet 中 Pod 的当前状态。这些数据可能存在一定程度的时间延迟。
	// +optional
	Status StatefulSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodManagementPolicyType 定义在有状态副本集下创建 Pod 的策略。
type PodManagementPolicyType string

const (
	// OrderedReadyPodManagement 在扩容时按严格递增顺序创建 Pod，在缩容时按严格递减顺序删除，
	// 且仅在前一个 Pod 就绪或终止后才继续。任意时刻最多只变更一个 Pod。
	OrderedReadyPodManagement PodManagementPolicyType = "OrderedReady"
	// ParallelPodManagement 在有状态副本集副本数变更时立即创建或删除 Pod，
	// 不会等待 Pod 就绪或完成终止。
	ParallelPodManagement PodManagementPolicyType = "Parallel"
)

// StatefulSetUpdateStrategy 指明 StatefulSet 控制器执行更新时采用的策略，
// 并包含按指定策略执行更新所需的其他参数。
type StatefulSetUpdateStrategy struct {
	// Type 表示 StatefulSetUpdateStrategy 的类型。默认为 RollingUpdate。
	// +optional
	Type StatefulSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetStrategyType"`
	// 当 Type 为 RollingUpdateStatefulSetStrategyType 时，用 RollingUpdate 传递相关参数。
	// +optional
	RollingUpdate *RollingUpdateStatefulSetStrategy `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// StatefulSetUpdateStrategyType 是字符串枚举类型，列举了 StatefulSet 控制器所有可能的更新策略。
type StatefulSetUpdateStrategyType string

const (
	// RollingUpdateStatefulSetStrategyType 表示更新会依照 StatefulSet 的排序约束，
	// 应用到 StatefulSet 中的所有 Pod。以该策略执行扩容时，新 Pod 会依据
	// StatefulSet 的 updateRevision 所指明的规格版本创建。
	RollingUpdateStatefulSetStrategyType StatefulSetUpdateStrategyType = "RollingUpdate"
	// OnDeleteStatefulSetStrategyType 禁用有序滚动重启。版本跟踪以尽力而为的方式进行 ——
	// 控制器会尝试最终使 StatefulSet 的 currentRevision 与 updateRevision 收敛。
	// Pod 被手动删除时，会依据 StatefulSetSpec 重新创建。
	// 以该策略执行扩容时，新 Pod 会依据 StatefulSet 的 updateRevision 所指明的规格版本创建。
	OnDeleteStatefulSetStrategyType StatefulSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateStatefulSetStrategy 用于传递 RollingUpdateStatefulSetStrategyType 的参数。
type RollingUpdateStatefulSetStrategy struct {
	// Partition 表示 StatefulSet 更新时分段的起始序号。滚动更新期间，
	// 序号从 Replicas-1 到 Partition 的所有 Pod 都会被更新，
	// 序号从 Partition-1 到 0 的所有 Pod 保持不动。这便于进行金丝雀发布。默认值为 0。
	// +optional
	Partition *int32 `json:"partition,omitempty" protobuf:"varint,1,opt,name=partition"`
	// 更新期间最多不可用的 Pod 数量。
	// 值可以是绝对数字（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 绝对数字由百分比向上取整得出。该值不能为 0。
	// 默认为 1。该字段为 beta 级别且默认启用。该字段适用于 0 到 Replicas-1 范围内的所有 Pod。
	// 也就是说，0 到 Replicas-1 范围内只要有任何不可用的 Pod，都会被计入 MaxUnavailable。
	// 对于 OrderedReady 的 podManagementPolicy，该设置可能不生效；该策略确保 Pod 逐个创建并就绪。
	//
	// +featureGate=MaxUnavailableStatefulSet
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"varint,2,opt,name=maxUnavailable"`
}

// PersistentVolumeClaimRetentionPolicyType 是字符串枚举，用于确定当控制它的 StatefulSet
// 被删除或缩容时，VolumeClaimTemplates 生成的卷何时被删除。
type PersistentVolumeClaimRetentionPolicyType string

const (
	// RetainPersistentVolumeClaimRetentionPolicyType 为默认的
	// PersistentVolumeClaimRetentionPolicy，指定与 StatefulSet VolumeClaimTemplates
	// 关联的 PersistentVolumeClaim 不会被删除。
	RetainPersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Retain"
	// DeletePersistentVolumeClaimRetentionPolicyType 指定与 StatefulSet VolumeClaimTemplates
	// 关联的 PersistentVolumeClaim 会在 StatefulSetPersistentVolumeClaimRetentionPolicy
	// 所指定的场景下被删除。
	DeletePersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Delete"
)

// StatefulSetPersistentVolumeClaimRetentionPolicy 描述对由 StatefulSet VolumeClaimTemplates
// 创建的 PVC 所采用的策略。
type StatefulSetPersistentVolumeClaimRetentionPolicy struct {
	// WhenDeleted 指定当 StatefulSet 被删除时，由 StatefulSet VolumeClaimTemplates
	// 创建的 PVC 会发生什么。默认策略 `Retain` 使 PVC 不受 StatefulSet 删除的影响。
	// `Delete` 策略会使这些 PVC 被删除。
	// +optional
	WhenDeleted PersistentVolumeClaimRetentionPolicyType `json:"whenDeleted,omitempty" protobuf:"bytes,1,opt,name=whenDeleted,casttype=PersistentVolumeClaimRetentionPolicyType"`
	// WhenScaled 指定当 StatefulSet 缩容时，由 StatefulSet VolumeClaimTemplates
	// 创建的 PVC 会发生什么。默认策略 `Retain` 使 PVC 不受缩容的影响。
	// `Delete` 策略会使超出副本数的那些 Pod 所对应的 PVC 被删除。
	// +optional
	WhenScaled PersistentVolumeClaimRetentionPolicyType `json:"whenScaled,omitempty" protobuf:"bytes,2,opt,name=whenScaled,casttype=PersistentVolumeClaimRetentionPolicyType"`
}

// StatefulSetOrdinals 描述本 StatefulSet 中副本序号的分配策略。
type StatefulSetOrdinals struct {
	// start 表示第一个副本的起始序号。可用于从其他序号（例如从 1 开始）为副本编号，
	// 以替代默认的从 0 开始命名，也可用于协调副本从一个 StatefulSet 逐步迁移到另一个。
	// 如果设置，副本序号范围为：
	//   [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas)。
	// 如果未设置，默认为 0，副本序号范围为：
	//   [0, .spec.replicas)。
	// +optional
	Start int32 `json:"start" protobuf:"varint,1,opt,name=start"`
}

// StatefulSetSpec 是 StatefulSet 的规格定义。
type StatefulSetSpec struct {
	// replicas 是给定 Template 期望的副本数。
	// 这里的副本是指同一 Template 的多个实例，但每个副本同样具有一致的身份标识。
	// 如果未指定，默认为 1。
	// TODO: 考虑重命名该字段。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// selector 是针对应与副本数匹配的 Pod 的标签查询。
	// 它必须与 Pod 模板的标签相匹配。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	// +k8s:alpha(since: "1.37")=+k8s:required
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// template 是描述当副本数不足时所创建 Pod 的对象。StatefulSet 生成的每个 Pod
	// 都会符合该 Template，但各自拥有区别于集合内其他 Pod 的唯一身份标识。
	// 每个 Pod 的命名格式为 <statefulsetname>-<podindex>。例如，名为 "web" 的
	// StatefulSet 中序号为 "3" 的 Pod 会被命名为 "web-3"。
	// template.spec.restartPolicy 唯一允许的值为 "Always"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// volumeClaimTemplates 是允许 Pod 引用的 PVC 列表。
	// StatefulSet 控制器负责将网络身份映射到 PVC，以保持 Pod 的身份标识。
	// 该列表中的每个 PVC 必须在模板的某个容器中至少有一个同名（按名称匹配）的 volumeMount。
	// 该列表中的 PVC 优先于模板中同名的任何 volume。
	// TODO: 定义当同名 PVC 已存在时的行为。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	VolumeClaimTemplates []v1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty" protobuf:"bytes,4,rep,name=volumeClaimTemplates"`

	// serviceName 是管理该 StatefulSet 的 Service 的名称。
	// 该 Service 必须先于 StatefulSet 存在，并负责该集合的网络身份。
	// Pod 获得的 DNS/主机名遵循如下模式：
	// pod-specific-string.serviceName.default.svc.cluster.local
	// 其中 "pod-specific-string" 由 StatefulSet 控制器管理。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	ServiceName string `json:"serviceName" protobuf:"bytes,5,opt,name=serviceName"`

	// podManagementPolicy 控制初始扩容、替换节点上的 Pod 或缩容期间如何创建 Pod。
	// 默认策略为 `OrderedReady`：按递增顺序创建 Pod（先 pod-0，再 pod-1，依此类推），
	// 控制器会等待每个 Pod 就绪后再继续。缩容时按相反顺序移除 Pod。
	// 另一种策略 `Parallel` 会并行创建 Pod 以匹配期望规模而不等待，缩容时则一次性删除所有 Pod。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	PodManagementPolicy PodManagementPolicyType `json:"podManagementPolicy,omitempty" protobuf:"bytes,6,opt,name=podManagementPolicy,casttype=PodManagementPolicyType"`

	// updateStrategy 表示当 Template 发生变更时，用于更新 StatefulSet 中 Pod 的
	// StatefulSetUpdateStrategy。
	// +optional
	UpdateStrategy StatefulSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,7,opt,name=updateStrategy"`

	// revisionHistoryLimit 是 StatefulSet 修订历史中保留的最大修订版本数。
	// 修订历史包含所有未被当前应用的 StatefulSetSpec 版本所代表的修订版本。默认值为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,8,opt,name=revisionHistoryLimit"`

	// 新创建的 Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,9,opt,name=minReadySeconds"`

	// persistentVolumeClaimRetentionPolicy 描述由 volumeClaimTemplates 创建的 PVC 的生命周期。
	// 默认情况下，所有 PVC 按需创建并保留，直到被手动删除。
	// 该策略允许改变这一生命周期，例如在其 StatefulSet 被删除或其 Pod 缩容时删除相应的 PVC。
	// +optional
	PersistentVolumeClaimRetentionPolicy *StatefulSetPersistentVolumeClaimRetentionPolicy `json:"persistentVolumeClaimRetentionPolicy,omitempty" protobuf:"bytes,10,opt,name=persistentVolumeClaimRetentionPolicy"`

	// ordinals 控制 StatefulSet 中副本序号的编号方式。
	// 默认行为是第一个副本编号为 "0"，之后每增加一个副本序号加一。
	// +optional
	Ordinals *StatefulSetOrdinals `json:"ordinals,omitempty" protobuf:"bytes,11,opt,name=ordinals"`
}

// StatefulSetStatus 表示 StatefulSet 的当前状态。
type StatefulSetStatus struct {
	// observedGeneration 是该 StatefulSet 最近一次观测到的 generation，对应 StatefulSet 的
	// generation，由 API Server 在变更时更新。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// replicas 是 StatefulSet 控制器创建的 Pod 数量。
	Replicas int32 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`

	// readyReplicas 是由该 StatefulSet 控制器创建且处于 Ready 条件的 Pod 数量。
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,3,opt,name=readyReplicas"`

	// currentReplicas 是 StatefulSet 控制器依据 currentRevision 所指明的
	// StatefulSet 版本创建的 Pod 数量。
	CurrentReplicas int32 `json:"currentReplicas,omitempty" protobuf:"varint,4,opt,name=currentReplicas"`

	// updatedReplicas 是 StatefulSet 控制器依据 updateRevision 所指明的
	// StatefulSet 版本创建的 Pod 数量。
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,5,opt,name=updatedReplicas"`

	// currentRevision 如果非空，表示用于在序列 [0,currentReplicas) 中生成 Pod 的
	// StatefulSet 版本。
	CurrentRevision string `json:"currentRevision,omitempty" protobuf:"bytes,6,opt,name=currentRevision"`

	// updateRevision 如果非空，表示用于在序列 [replicas-updatedReplicas,replicas) 中生成 Pod 的
	// StatefulSet 版本。
	UpdateRevision string `json:"updateRevision,omitempty" protobuf:"bytes,7,opt,name=updateRevision"`

	// collisionCount 是 StatefulSet 的哈希冲突计数。StatefulSet 控制器在需要为
	// 最新的 ControllerRevision 生成名称时，用该字段作为避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// 表示对 statefulset 当前状态最新的可用观测结果。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []StatefulSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`

	// 该 StatefulSet 所面向的可用 Pod 总数（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas" protobuf:"varint,11,opt,name=availableReplicas"`
}

// StatefulSetConditionType 表示 StatefulSet 条件的类型。
type StatefulSetConditionType string

// StatefulSetCondition 描述某一时刻 statefulset 的状态。
type StatefulSetCondition struct {
	// statefulset 条件的类型。
	// +optional
	Type StatefulSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件最近一次转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明该转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,StatefulSetList

// StatefulSetList 是 StatefulSet 的集合。
type StatefulSetList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StatefulSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,Deployment

// 已废弃 —— 该 GroupVersion 的 Deployment 已被 apps/v1/Deployment 取代，详见发布说明。
// Deployment 支持对 Pod 和 ReplicaSet 的声明式更新。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type Deployment struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 期望的 Deployment 行为规格。
	// +required
	Spec DeploymentSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近观测到的 Deployment 状态。
	// +optional
	Status DeploymentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeploymentSpec 是 Deployment 期望行为的规格定义。
type DeploymentSpec struct {
	// 期望的 Pod 数量。使用指针以区分显式设置为零与未指定两种情况。默认为 1。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// Pod 的标签选择器。被其选中的 Pod 所在的现有 ReplicaSet 都会受到该 Deployment 的影响。
	// 它必须与 Pod 模板的标签相匹配。
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// Template 描述将被创建的 Pod。
	// template.spec.restartPolicy 唯一允许的值为 "Always"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// 用新 Pod 替换现有 Pod 时所采用的 Deployment 策略。
	// +optional
	// +patchStrategy=retainKeys
	Strategy DeploymentStrategy `json:"strategy,omitempty" patchStrategy:"retainKeys" protobuf:"bytes,4,opt,name=strategy"`

	// 新创建的 Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,5,opt,name=minReadySeconds"`

	// 为支持回滚而保留的旧 ReplicaSet 数量。
	// 使用指针以区分显式设置为零与未指定两种情况。默认为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`

	// 表示该 Deployment 已暂停。
	// +optional
	Paused bool `json:"paused,omitempty" protobuf:"varint,7,opt,name=paused"`

	// Deployment 在被判定为失败之前取得进展的最长秒数。失败后 Deployment 控制器仍会继续处理，
	// 并在 Deployment 状态中给出原因为 ProgressDeadlineExceeded 的条件。注意，Deployment 暂停期间
	// 不估算进展。默认为 600 秒。
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty" protobuf:"varint,9,opt,name=progressDeadlineSeconds"`
}

const (
	// DefaultDeploymentUniqueLabelKey 是默认的选择器键，会被添加到现有 ReplicaSet
	// （以及添加到其 Pod 上的标签键），以防止现有 ReplicaSet 选中新 Pod
	// （或旧 Pod 被新 ReplicaSet 选中）。
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy 描述如何用新 Pod 替换现有 Pod。
type DeploymentStrategy struct {
	// Deployment 的类型。可取 "Recreate" 或 "RollingUpdate"。默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=DeploymentStrategyType"`

	// 滚动更新配置参数。仅当 DeploymentStrategyType = RollingUpdate 时存在。
	//---
	// TODO: 按照我们约定的 oneOf 形式更新此处，无论最终约定是什么。
	// +optional
	RollingUpdate *RollingUpdateDeployment `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DeploymentStrategyType 表示 Deployment 的策略类型。
type DeploymentStrategyType string

const (
	// 创建新 Pod 之前先杀掉所有现有 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// 通过滚动更新用新 ReplicaSet 替换旧 ReplicaSet，即逐步缩容旧 ReplicaSet 并扩容新 ReplicaSet。
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// 用于控制滚动更新期望行为的规格。
type RollingUpdateDeployment struct {
	// 更新期间最多不可用的 Pod 数量。
	// 值可以是绝对数字（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 绝对数字由百分比向下取整得出。
	// 当 MaxSurge 为 0 时该值不能为 0。
	// 默认为 25%。
	// 示例：设为 30% 时，滚动更新一开始就可以立即将旧 ReplicaSet 缩容至期望 Pod 数的 70%。
	// 新 Pod 就绪后，可以进一步缩容旧 ReplicaSet，再扩容新 ReplicaSet，
	// 从而确保更新过程中任意时刻可用的 Pod 总数至少为期望 Pod 数的 70%。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 可超出期望 Pod 数调度的最多 Pod 数量。
	// 值可以是绝对数字（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 当 MaxUnavailable 为 0 时该值不能为 0。
	// 绝对数字由百分比向上取整得出。
	// 默认为 25%。
	// 示例：设为 30% 时，滚动更新一开始就可以立即扩容新 ReplicaSet，
	// 使新旧 Pod 总数不超过期望 Pod 数的 130%。旧 Pod 被杀掉后，
	// 可以进一步扩容新 ReplicaSet，确保更新过程中任意时刻运行的 Pod 总数
	// 最多为期望 Pod 数的 130%。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DeploymentStatus 是最近观测到的 Deployment 状态。
type DeploymentStatus struct {
	// Deployment 控制器观测到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 该 Deployment 所面向的非终止中 Pod 总数（其标签与选择器匹配）。
	// +optional
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// 该 Deployment 所面向的、具有期望模板规格的非终止中 Pod 总数。
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,3,opt,name=updatedReplicas"`

	// 该 Deployment 所面向的、处于 Ready 条件的非终止中 Pod 总数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,7,opt,name=readyReplicas"`

	// 该 Deployment 所面向的可用非终止中 Pod 总数（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,4,opt,name=availableReplicas"`

	// 该 Deployment 所面向的不可用 Pod 总数。这是该 Deployment 达到 100% 可用容量
	// 仍所需的 Pod 总数。它们可能是运行中但尚未可用的 Pod，也可能是尚未创建的 Pod。
	// +optional
	UnavailableReplicas int32 `json:"unavailableReplicas,omitempty" protobuf:"varint,5,opt,name=unavailableReplicas"`

	// 该 Deployment 所面向的终止中 Pod 总数。终止中 Pod 具有非空的
	// .metadata.deletionTimestamp，且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 该字段为 beta 字段，需要启用 DeploymentReplicaSetTerminatingReplicas 特性（默认启用）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,9,opt,name=terminatingReplicas"`

	// 表示对 Deployment 当前状态最新的可用观测结果。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DeploymentCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`

	// Deployment 的哈希冲突计数。Deployment 控制器在需要为最新的 ReplicaSet
	// 生成名称时，用该字段作为避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,8,opt,name=collisionCount"`
}

// DeploymentConditionType 表示 Deployment 条件的类型。
type DeploymentConditionType string

// 以下是 Deployment 的有效条件。
const (
	// Available 表示 Deployment 可用，即至少满足最小可用副本数的 Pod 已启动并运行
	// 至少 minReadySeconds。
	DeploymentAvailable DeploymentConditionType = "Available"
	// Progressing 表示 Deployment 正在推进。当创建或接纳新的 ReplicaSet，以及新 Pod 扩容
	// 或旧 Pod 缩容时，都视为 Deployment 有进展。暂停的 Deployment 或未指定
	// progressDeadlineSeconds 时不估算进展。
	DeploymentProgressing DeploymentConditionType = "Progressing"
	// 当 Deployment 的某个 Pod 创建或删除失败时，会添加 ReplicaFailure 条件。
	DeploymentReplicaFailure DeploymentConditionType = "ReplicaFailure"
)

// DeploymentCondition 描述某一时刻 Deployment 的状态。
type DeploymentCondition struct {
	// Deployment 条件的类型。
	// +optional
	Type DeploymentConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DeploymentConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 该条件最近一次更新的时间。
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,6,opt,name=lastUpdateTime"`
	// 条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,7,opt,name=lastTransitionTime"`
	// 条件最近一次转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明该转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DeploymentList

// DeploymentList 是 Deployment 的列表。
type DeploymentList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Deployment 的列表。
	Items []Deployment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// DaemonSetUpdateStrategy 是用于控制 DaemonSet 更新策略的结构。
type DaemonSetUpdateStrategy struct {
	// DaemonSet 更新的类型。可取 "RollingUpdate" 或 "OnDelete"。默认为 RollingUpdate。
	// +optional
	Type DaemonSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`

	// 滚动更新配置参数。仅当 type = "RollingUpdate" 时存在。
	//---
	// TODO: 按照我们约定的 oneOf 形式更新此处，无论最终约定是什么。与 Deployment 的 `strategy.rollingUpdate` 相同。
	// 参见 https://github.com/kubernetes/kubernetes/issues/35345
	// +optional
	RollingUpdate *RollingUpdateDaemonSet `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DaemonSetUpdateStrategyType 表示 DaemonSet 的更新策略类型。
type DaemonSetUpdateStrategyType string

const (
	// 通过滚动更新用新守护进程替换旧守护进程，即在每个节点上逐个替换。
	RollingUpdateDaemonSetStrategyType DaemonSetUpdateStrategyType = "RollingUpdate"

	// 仅在旧守护进程被杀掉时才替换它。
	OnDeleteDaemonSetStrategyType DaemonSetUpdateStrategyType = "OnDelete"
)

// 用于控制守护进程集滚动更新期望行为的规格。
type RollingUpdateDaemonSet struct {
	// 更新期间最多不可用的 DaemonSet Pod 数量。值可以是绝对数字（例如 5），
	// 也可以是更新开始时 DaemonSet Pod 总数的百分比（例如 10%）。
	// 绝对数字由百分比向上取整得出。
	// 当 MaxSurge 为 0 时该值不能为 0。
	// 默认值为 1。
	// 示例：设为 30% 时，任意时刻最多有应运行该守护进程 Pod 的节点总数
	// （即 status.desiredNumberScheduled）的 30% 可以因更新而停止其 Pod。
	// 更新时先停止最多 30% 的 DaemonSet Pod，然后在原处拉起新的 DaemonSet Pod。
	// 新 Pod 可用后，再继续处理其他 DaemonSet Pod，从而确保更新过程中
	// 任意时刻至少有原始 DaemonSet Pod 总数的 70% 可用。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 更新期间，已有可用 DaemonSet Pod 的节点中，最多可同时存在更新后 DaemonSet Pod 的节点数量。
	// 值可以是绝对数字（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 当 MaxUnavailable 为 0 时该值不能为 0。
	// 绝对数字由百分比向上取整得出，最小为 1。
	// 默认值为 0。
	// 示例：设为 30% 时，最多有应运行该守护进程 Pod 的节点总数
	// （即 status.desiredNumberScheduled）的 30% 可以在旧 Pod 被标记为删除之前先创建新 Pod。
	// 更新时先在 30% 的节点上启动新 Pod。一旦更新后的 Pod 可用（就绪至少 minReadySeconds），
	// 该节点上的旧 DaemonSet Pod 就会被标记为删除。如果旧 Pod 因任何原因不可用
	// （Ready 转为 false、被驱逐或被排空），则会立即在该节点上创建更新后的 Pod，而不考虑 surge 限制。
	// 允许 surge 意味着当就绪检查失败时，守护进程集在任意节点上消耗的资源可能翻倍，
	// 因此资源密集型的守护进程集应考虑到它们可能在中断期间引发驱逐。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DaemonSetSpec 是守护进程集的规格定义。
type DaemonSetSpec struct {
	// 针对由守护进程集管理的 Pod 的标签查询。必须匹配才能被控制。
	// 它必须与 Pod 模板的标签相匹配。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,1,opt,name=selector"`

	// 描述将要创建的 Pod 的对象。DaemonSet 会在每个匹配模板节点选择器的节点上
	// （若未指定节点选择器则在每个节点上）恰好创建该 Pod 的一个副本。
	// template.spec.restartPolicy 唯一允许的值为 "Always"。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,2,opt,name=template"`

	// 用新 Pod 替换现有 DaemonSet Pod 的更新策略。
	// +optional
	UpdateStrategy DaemonSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,3,opt,name=updateStrategy"`

	// 新创建的 DaemonSet Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// 为支持回滚而保留的旧历史版本数量。
	// 使用指针以区分显式设置为零与未指定两种情况。默认为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`
}

// DaemonSetStatus 表示守护进程集的当前状态。
type DaemonSetStatus struct {
	// 正在运行至少一个守护进程 Pod 且本应运行该守护进程 Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	CurrentNumberScheduled int32 `json:"currentNumberScheduled" protobuf:"varint,1,opt,name=currentNumberScheduled"`

	// 正在运行守护进程 Pod 但本不应运行该守护进程 Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	NumberMisscheduled int32 `json:"numberMisscheduled" protobuf:"varint,2,opt,name=numberMisscheduled"`

	// 本应运行该守护进程 Pod 的节点总数（包括已正确运行该守护进程 Pod 的节点）。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	DesiredNumberScheduled int32 `json:"desiredNumberScheduled" protobuf:"varint,3,opt,name=desiredNumberScheduled"`

	// 本应运行该守护进程 Pod，且至少有一个守护进程 Pod 通过就绪探针并处于 Ready 条件的节点总数。
	NumberReady int32 `json:"numberReady" protobuf:"varint,4,opt,name=numberReady"`

	// 守护进程集控制器最近一次观测到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,5,opt,name=observedGeneration"`

	// 正在运行更新后守护进程 Pod 的节点总数。
	// +optional
	UpdatedNumberScheduled int32 `json:"updatedNumberScheduled,omitempty" protobuf:"varint,6,opt,name=updatedNumberScheduled"`

	// 本应运行该守护进程 Pod，且至少有一个守护进程 Pod 正在运行并可用
	// （就绪至少 spec.minReadySeconds）的节点数。
	// +optional
	NumberAvailable int32 `json:"numberAvailable,omitempty" protobuf:"varint,7,opt,name=numberAvailable"`

	// 本应运行该守护进程 Pod，但没有任何守护进程 Pod 正在运行且可用
	// （就绪至少 spec.minReadySeconds）的节点数。
	// +optional
	NumberUnavailable int32 `json:"numberUnavailable,omitempty" protobuf:"varint,8,opt,name=numberUnavailable"`

	// DaemonSet 的哈希冲突计数。DaemonSet 控制器在需要为最新的
	// ControllerRevision 生成名称时，用该字段作为避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// 表示对 DaemonSet 当前状态最新的可用观测结果。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DaemonSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`
}

// DaemonSetConditionType 表示 DaemonSet 条件的类型。
type DaemonSetConditionType string

// TODO: 补充 DaemonSet 的有效条件类型。

// DaemonSetCondition 描述某一时刻 DaemonSet 的状态。
type DaemonSetCondition struct {
	// DaemonSet 条件的类型。
	// +optional
	Type DaemonSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DaemonSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件最近一次转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明该转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DaemonSet

// 已废弃 —— 该 GroupVersion 的 DaemonSet 已被 apps/v1/DaemonSet 取代，详见发布说明。
// DaemonSet 表示守护进程集的配置。
// +k8s:supportsSubresource="/status"
type DaemonSet struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 该守护进程集期望的行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec DaemonSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 该守护进程集的当前状态。这些数据可能存在一定程度的时间延迟。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status DaemonSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

const (
	// DefaultDaemonSetUniqueLabelKey 是默认标签键，会被添加到现有 DaemonSet Pod 上，
	// 以便在 DaemonSet 模板更新期间区分新旧 DaemonSet Pod。
	DefaultDaemonSetUniqueLabelKey = ControllerRevisionHashLabelKey
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DaemonSetList

// DaemonSetList 是守护进程集的集合。
type DaemonSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 守护进程集的列表。
	Items []DaemonSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ReplicaSet

// 已废弃 —— 该 GroupVersion 的 ReplicaSet 已被 apps/v1/ReplicaSet 取代，详见发布说明。
// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type ReplicaSet struct {
	metav1.TypeMeta `json:""`

	// 如果 ReplicaSet 的 Labels 为空，则默认与其管理的 Pod 的 Labels 一致。
	// 标准对象的元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 ReplicaSet 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec ReplicaSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是最近观测到的 ReplicaSet 状态。
	// 这些数据可能存在一定程度的时间延迟。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ReplicaSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ReplicaSetList

// ReplicaSetList 是 ReplicaSet 的集合。
type ReplicaSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ReplicaSet 的列表。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Items []ReplicaSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReplicaSetSpec 是 ReplicaSet 的规格定义。
type ReplicaSetSpec struct {
	// Replicas 是期望的 Pod 数量。
	// 使用指针以区分显式设置为零与未指定两种情况。
	// 默认为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// 新创建的 Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// Selector 是针对应与副本数匹配的 Pod 的标签查询。
	// 受该副本集控制的 Pod 所必须匹配的标签键和值。
	// 它必须与 Pod 模板的标签相匹配。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// Template 是描述当副本数不足时所创建 Pod 的对象。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/#pod-template
	// +optional
	Template v1.PodTemplateSpec `json:"template,omitempty" protobuf:"bytes,3,opt,name=template"`
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近一次观测到的非终止中 Pod 数量。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// 标签与副本集 Pod 模板标签匹配的非终止中 Pod 数量。
	// +optional
	FullyLabeledReplicas int32 `json:"fullyLabeledReplicas,omitempty" protobuf:"varint,2,opt,name=fullyLabeledReplicas"`

	// 该 ReplicaSet 所面向的、处于 Ready 条件的非终止中 Pod 数量。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,4,opt,name=readyReplicas"`

	// 该副本集可用的非终止中 Pod 数量（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,5,opt,name=availableReplicas"`

	// 该副本集的终止中 Pod 数量。终止中 Pod 具有非空的 .metadata.deletionTimestamp，
	// 且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 该字段为 beta 字段，需要启用 DeploymentReplicaSetTerminatingReplicas 特性（默认启用）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,7,opt,name=terminatingReplicas"`

	// ObservedGeneration 反映最近一次观测到的 ReplicaSet 的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`

	// 表示对副本集当前状态最新的可用观测结果。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []ReplicaSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`
}

// ReplicaSetConditionType 表示 ReplicaSet 条件的类型。
type ReplicaSetConditionType string

// 以下是副本集的有效条件。
const (
	// 当副本集的某个 Pod 因配额不足、limit range、Pod 安全策略、节点选择器等原因创建失败，
	// 或因 kubelet 宕机、finalizer 失败而删除失败时，会添加 ReplicaSetReplicaFailure 条件。
	ReplicaSetReplicaFailure ReplicaSetConditionType = "ReplicaFailure"
)

// ReplicaSetCondition 描述某一时刻副本集的状态。
type ReplicaSetCondition struct {
	// 副本集条件的类型。
	// +optional
	Type ReplicaSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=ReplicaSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件最近一次转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明该转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ControllerRevision

// 已废弃 —— 该 GroupVersion 的 ControllerRevision 已被 apps/v1/ControllerRevision 取代，详见发布说明。
// ControllerRevision 实现了状态数据的不可变快照。客户端负责序列化和反序列化
// 包含其内部状态的对象。
// ControllerRevision 一旦成功创建就不能更新。API Server 会拒绝所有
// 试图修改 Data 字段的请求。但 ControllerRevision 可以被删除。注意，由于它同时被
// DaemonSet 和 StatefulSet 控制器用于更新和回滚，该对象为 beta 级别。
// 不过，它在未来版本中可能会发生名称和表示形式的变化，客户端不应依赖其稳定性。
// 它主要供控制器内部使用。
type ControllerRevision struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Data 是状态的序列化表示。
	// +required
	Data runtime.RawExtension `json:"data" protobuf:"bytes,2,opt,name=data"`

	// Revision 表示 Data 所代表状态的修订版本。
	// +optional
	Revision int64 `json:"revision" protobuf:"varint,3,opt,name=revision"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ControllerRevisionList

// ControllerRevisionList 是包含 ControllerRevision 对象列表的资源。
type ControllerRevisionList struct {
	metav1.TypeMeta `json:""`

	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ControllerRevision 的列表。
	Items []ControllerRevision `json:"items" protobuf:"bytes,2,rep,name=items"`
}
