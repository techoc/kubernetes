/*
Copyright 2016 The Kubernetes Authors.

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

// Package v1beta1 提供 apps/v1beta1 API 组的类型定义。
// 该组版本已弃用，仅在迁移到 apps/v1 期间保留兼容。
package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// StatefulSet 与 ControllerRevision 使用的标签键。
const (
	// ControllerRevisionHashLabelKey 是标识 Pod 所属 ControllerRevision 的标签键。
	ControllerRevisionHashLabelKey = "controller-revision-hash"
	// StatefulSetRevisionLabel 与 ControllerRevisionHashLabelKey 取值相同，是后者的别名。
	StatefulSetRevisionLabel = ControllerRevisionHashLabelKey
	// StatefulSetPodNameLabel 是记录 Pod 所属 StatefulSet 名称的标签键。
	StatefulSetPodNameLabel = "statefulset.kubernetes.io/pod-name"
)

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// replicas 是被伸缩对象当前观测到的实例数。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=0
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// replicas 是被伸缩对象实际观测到的实例数。
	// +optional
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// selector 是对应计入副本数的 Pod 的标签查询。更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	// +optional
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`

	// targetSelector 是对应计入副本数的 Pod 的标签选择器，是 map 型与表达力更强的集合型选择器的序列化形式。
	// 这样客户端无需再做内省。字符串格式与 query-param 语法一致。
	// 若目标类型仅支持 map 型选择器，则本字段与 map 型选择器字段都会被填充。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	TargetSelector string `json:"targetSelector,omitempty" protobuf:"bytes,3,opt,name=targetSelector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
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

	// spec 定义伸缩行为。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec ScaleSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// status 定义伸缩的当前状态。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。只读。
	// +optional
	Status ScaleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.5
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,StatefulSet

// 已弃用：StatefulSet 的该组版本已被 apps/v1beta2/StatefulSet 取代，详见发布说明。
// StatefulSet 表示一组具有一致身份的 Pod。
// 身份定义为：
//   - 网络：单一且稳定的 DNS 名称与主机名。
//   - 存储：按请求数量提供的 VolumeClaim。
//
// StatefulSet 保证给定的网络身份始终映射到相同的存储身份。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type StatefulSet struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义该集合中 Pod 的期望身份。
	// +required
	Spec StatefulSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是该 StatefulSet 中 Pod 的当前状态，可能存在一段时间滞后。
	// +optional
	Status StatefulSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodManagementPolicyType 定义在 StatefulSet 中创建 Pod 的策略。
type PodManagementPolicyType string

// PodManagementPolicyType 的合法取值。
const (
	// OrderedReadyPodManagement 在扩容时按严格递增顺序创建 Pod，缩容时按严格递减顺序删除，
	// 仅当前一个 Pod 就绪或终止后才继续，且任一时刻最多变更一个 Pod。
	OrderedReadyPodManagement PodManagementPolicyType = "OrderedReady"
	// ParallelPodManagement 在 StatefulSet 副本数变化时立即创建或删除 Pod，
	// 不等待 Pod 就绪或终止完成。
	ParallelPodManagement PodManagementPolicyType = "Parallel"
)

// StatefulSetUpdateStrategy 表示 StatefulSet 控制器执行更新时采用的策略，
// 以及该策略所需的附加参数。
type StatefulSetUpdateStrategy struct {
	// Type 表示更新策略的类型。
	// +optional
	Type StatefulSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetStrategyType"`
	// RollingUpdate 在 Type 为 RollingUpdateStatefulSetStrategyType 时用于传递参数。
	// +optional
	RollingUpdate *RollingUpdateStatefulSetStrategy `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// StatefulSetUpdateStrategyType 枚举 StatefulSet 控制器支持的更新策略。
type StatefulSetUpdateStrategyType string

// StatefulSetUpdateStrategyType 的合法取值。
const (
	// RollingUpdateStatefulSetStrategyType 表示在遵守 StatefulSet 顺序约束的前提下更新全部 Pod。
	// 采用该策略扩容时，新 Pod 使用 StatefulSet updateRevision 指定的模板版本创建。
	RollingUpdateStatefulSetStrategyType StatefulSetUpdateStrategyType = "RollingUpdate"
	// OnDeleteStatefulSetStrategyType 关闭有序滚动重启。
	// 版本跟踪以尽力而为的方式进行：控制器最终会尝试使 currentRevision 与 updateRevision 收敛。
	// Pod 被手动删除时按 StatefulSetSpec 重建。
	// 采用该策略扩容时，新 Pod 使用 StatefulSet updateRevision 指定的模板版本创建。
	OnDeleteStatefulSetStrategyType StatefulSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateStatefulSetStrategy 为 RollingUpdateStatefulSetStrategyType 传递参数。
type RollingUpdateStatefulSetStrategy struct {
	// Partition 表示 StatefulSet 更新时的分区起点序号。滚动更新期间，序号从 Replicas-1 到 Partition 的 Pod 会被更新，
	// 从 Partition-1 到 0 的 Pod 保持不变。可用于金丝雀发布，默认值为 0。
	// +optional
	Partition *int32 `json:"partition,omitempty" protobuf:"varint,1,opt,name=partition"`
	// maxUnavailable 是更新期间允许不可用的最大 Pod 数。
	// 可取绝对值（如 5）或期望 Pod 数的百分比（如 10%），百分比向上取整为绝对值，不能为 0。
	// 默认为 1。该字段处于 beta 阶段且默认启用，作用于序号 0 到 Replicas-1 的全部 Pod；
	// 即该范围内只要存在不可用的 Pod，就会计入 MaxUnavailable。
	// 对 OrderedReady 策略可能无效：该策略保证 Pod 逐个创建并就绪。
	//
	// +featureGate=MaxUnavailableStatefulSet
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"varint,2,opt,name=maxUnavailable"`
}

// PersistentVolumeClaimRetentionPolicyType 枚举在控制它的 StatefulSet 被删除或缩容时，
// VolumeClaimTemplates 对应卷的删除策略。
type PersistentVolumeClaimRetentionPolicyType string

// PersistentVolumeClaimRetentionPolicyType 的合法取值。
const (
	// RetainPersistentVolumeClaimRetentionPolicyType 是默认值，表示不删除与 StatefulSet VolumeClaimTemplates
	// 关联的 PersistentVolumeClaim。
	RetainPersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Retain"
	// DeletePersistentVolumeClaimRetentionPolicyType 表示在 StatefulSetPersistentVolumeClaimRetentionPolicy
	// 指定的情形下删除与 StatefulSet VolumeClaimTemplates 关联的 PersistentVolumeClaim。
	DeletePersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Delete"
)

// StatefulSetPersistentVolumeClaimRetentionPolicy 描述由 StatefulSet VolumeClaimTemplates 创建的 PVC 的生命周期策略。
type StatefulSetPersistentVolumeClaimRetentionPolicy struct {
	// whenDeleted 指定 StatefulSet 被删除时，由 VolumeClaimTemplates 创建的 PVC 如何处理。
	// 默认策略 `Retain` 表示 PVC 不受删除影响，`Delete` 表示删除这些 PVC。
	// +optional
	WhenDeleted PersistentVolumeClaimRetentionPolicyType `json:"whenDeleted,omitempty" protobuf:"bytes,1,opt,name=whenDeleted,casttype=PersistentVolumeClaimRetentionPolicyType"`
	// whenScaled 指定 StatefulSet 缩容时，由 VolumeClaimTemplates 创建的 PVC 如何处理。
	// 默认策略 `Retain` 表示 PVC 不受缩容影响，`Delete` 表示删除副本数之外多余 Pod 对应的 PVC。
	// +optional
	WhenScaled PersistentVolumeClaimRetentionPolicyType `json:"whenScaled,omitempty" protobuf:"bytes,2,opt,name=whenScaled,casttype=PersistentVolumeClaimRetentionPolicyType"`
}

// StatefulSetOrdinals 描述该 StatefulSet 中副本序号的分配策略。
type StatefulSetOrdinals struct {
	// start 表示第一个副本的序号，可用于改用非默认的编号起点（例如从 1 开始），
	// 或编排副本从一个 StatefulSet 逐步迁移到另一个。
	// 设置后，副本序号范围为：
	//   [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas)。
	// 未设置时默认为 0，副本序号范围为：
	//   [0, .spec.replicas)。
	// +optional
	Start int32 `json:"start" protobuf:"varint,1,opt,name=start"`
}

// StatefulSetSpec 是 StatefulSet 的规格定义。
type StatefulSetSpec struct {
	// replicas 是给定模板期望的副本数。这些副本是同一模板的实例，但各自拥有一致的身份。
	// 未指定时默认为 1。
	// TODO: 考虑重命名该字段。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// selector 是匹配副本数的 Pod 标签查询，必须与 Pod 模板的标签一致。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	// +k8s:alpha(since: "1.37")=+k8s:required
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`

	// template 描述副本不足时创建的 Pod。StatefulSet 生成的每个 Pod 都符合该模板，
	// 但拥有区别于集合中其他 Pod 的唯一身份，命名为 <statefulsetname>-<podindex>。
	// 例如名为 "web" 的 StatefulSet 中序号为 "3" 的 Pod 名为 "web-3"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// volumeClaimTemplates 是允许 Pod 引用的 PVC 列表。StatefulSet 控制器负责将网络身份映射到这些 PVC，
	// 以保持 Pod 的身份。列表中每个 PVC 必须在模板的某个容器中至少有一个同名 volumeMount。
	// 列表中 PVC 优先于模板中同名的卷。
	// TODO: 定义同名 PVC 已存在时的行为。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	VolumeClaimTemplates []v1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty" protobuf:"bytes,4,rep,name=volumeClaimTemplates"`

	// serviceName 是管理该 StatefulSet 的 Service 名称。该 Service 必须先于 StatefulSet 存在，
	// 并负责集合的网络身份。Pod 的 DNS/主机名格式为
	// pod-specific-string.serviceName.default.svc.cluster.local，
	// 其中 "pod-specific-string" 由 StatefulSet 控制器管理。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	ServiceName string `json:"serviceName" protobuf:"bytes,5,opt,name=serviceName"`

	// podManagementPolicy 控制初始扩容、替换节点上的 Pod 以及缩容时 Pod 的创建方式。
	// 默认策略 `OrderedReady` 按序号递增创建（pod-0、pod-1 等），控制器等待每个 Pod 就绪后再继续；
	// 缩容时按相反顺序移除。另一策略 `Parallel` 不等待，并行创建 Pod 以快速达到期望副本数，
	// 缩容时一次性删除全部 Pod。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	PodManagementPolicy PodManagementPolicyType `json:"podManagementPolicy,omitempty" protobuf:"bytes,6,opt,name=podManagementPolicy,casttype=PodManagementPolicyType"`

	// updateStrategy 表示模板发生变更时用于更新 StatefulSet 中 Pod 的更新策略。
	// +optional
	UpdateStrategy StatefulSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,7,opt,name=updateStrategy"`

	// revisionHistoryLimit 是 StatefulSet 修订历史中保留的最大修订数。
	// 修订历史包含所有未被当前应用的 StatefulSetSpec 版本所代表的修订，默认值为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,8,opt,name=revisionHistoryLimit"`

	// minReadySeconds 是新创建的 Pod 在没有容器崩溃的情况下保持就绪、被视为可用所需的最小秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,9,opt,name=minReadySeconds"`

	// persistentVolumeClaimRetentionPolicy 描述由 volumeClaimTemplates 创建的 PVC 的生命周期。
	// 默认按需创建并保留所有 PVC，直到手动删除。该策略可改变此行为，例如删除 StatefulSet
	// 或其 Pod 缩容时一并删除相应 PVC。
	// +optional
	PersistentVolumeClaimRetentionPolicy *StatefulSetPersistentVolumeClaimRetentionPolicy `json:"persistentVolumeClaimRetentionPolicy,omitempty" protobuf:"bytes,10,opt,name=persistentVolumeClaimRetentionPolicy"`

	// ordinals 控制 StatefulSet 中副本序号的编号方式。默认将第一个副本编号为 "0"，
	// 此后每增加一个副本序号加一。
	// +optional
	Ordinals *StatefulSetOrdinals `json:"ordinals,omitempty" protobuf:"bytes,11,opt,name=ordinals"`
}

// StatefulSetStatus 表示 StatefulSet 的当前状态。
type StatefulSetStatus struct {
	// observedGeneration 是该 StatefulSet 观测到的最新 generation，对应 API Server 在变更时更新的
	// StatefulSet generation。
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// replicas 是 StatefulSet 控制器创建的 Pod 数。
	Replicas int32 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`

	// readyReplicas 是该 StatefulSet 控制器创建且处于 Ready 条件的 Pod 数。
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,3,opt,name=readyReplicas"`

	// currentReplicas 是 StatefulSet 控制器按 currentRevision 所指版本创建的 Pod 数。
	CurrentReplicas int32 `json:"currentReplicas,omitempty" protobuf:"varint,4,opt,name=currentReplicas"`

	// updatedReplicas 是 StatefulSet 控制器按 updateRevision 所指版本创建的 Pod 数。
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,5,opt,name=updatedReplicas"`

	// currentRevision 非空时，表示用于生成序号区间 [0,currentReplicas) 内 Pod 的 StatefulSet 版本。
	CurrentRevision string `json:"currentRevision,omitempty" protobuf:"bytes,6,opt,name=currentRevision"`

	// updateRevision 非空时，表示用于生成序号区间 [replicas-updatedReplicas,replicas) 内 Pod 的 StatefulSet 版本。
	UpdateRevision string `json:"updateRevision,omitempty" protobuf:"bytes,7,opt,name=updateRevision"`

	// collisionCount 是 StatefulSet 的哈希碰撞次数。控制器创建最新 ControllerRevision 名称时，
	// 用它作为避免碰撞的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// conditions 是对 StatefulSet 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []StatefulSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`

	// availableReplicas 是该 StatefulSet 关联的可用 Pod 总数（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas" protobuf:"varint,11,opt,name=availableReplicas"`
}

// StatefulSetConditionType 表示 StatefulSet 条件类型的字符串枚举。
type StatefulSetConditionType string

// StatefulSetCondition 描述 StatefulSet 在某一时刻的状态。
type StatefulSetCondition struct {
	// Type 是 StatefulSet 条件的类型。
	// +optional
	Type StatefulSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetConditionType"`
	// Status 是条件的状态，取值为 True、False 或 Unknown。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// LastTransitionTime 是条件最近一次发生状态转换的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// Reason 是条件最近一次状态转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// Message 是人可读的消息，说明状态转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.5
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,StatefulSetList

// StatefulSetList 是 StatefulSet 的集合。
type StatefulSetList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Items 是 StatefulSet 对象列表。
	Items []StatefulSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,Deployment

// 已弃用：Deployment 的该组版本已被 apps/v1beta2/Deployment 取代，详见发布说明。
// Deployment 支持对 Pod 和 ReplicaSet 进行声明式更新。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type Deployment struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 对 Deployment 期望行为的规格定义。
	// +required
	Spec DeploymentSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近观测到的 Deployment 状态。
	// +optional
	Status DeploymentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeploymentSpec 是对 Deployment 期望行为的规格定义。
type DeploymentSpec struct {
	// replicas 是期望的 Pod 数。使用指针以区分显式设为零与未指定。默认为 1。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// selector 是 Pod 的标签选择器，其选中的 Pod 所在 ReplicaSet 会受该 Deployment 影响。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`

	// Template 描述将要创建的 Pod。template.spec.restartPolicy 仅允许取值 "Always"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// 用新 Pod 替换现有 Pod 时采用的部署策略。
	// +optional
	// +patchStrategy=retainKeys
	Strategy DeploymentStrategy `json:"strategy,omitempty" patchStrategy:"retainKeys" protobuf:"bytes,4,opt,name=strategy"`

	// minReadySeconds 是新创建的 Pod 在没有容器崩溃的情况下保持就绪、被视为可用所需的最小秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,5,opt,name=minReadySeconds"`

	// revisionHistoryLimit 是为支持回滚而保留的旧 ReplicaSet 数量。
	// 使用指针以区分显式设为零与未指定。默认为 2。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`

	// paused 表示部署已暂停。
	// +optional
	Paused bool `json:"paused,omitempty" protobuf:"varint,7,opt,name=paused"`

	// 已弃用。
	// rollbackTo 是该 Deployment 要回滚到的配置，回滚完成后会被清空。
	// +optional
	RollbackTo *RollbackConfig `json:"rollbackTo,omitempty" protobuf:"bytes,8,opt,name=rollbackTo"`

	// progressDeadlineSeconds 是 Deployment 被认为失败前取得进展的最大时间（秒）。
	// 部署控制器仍会继续处理失败的 Deployment，并在状态中显示原因为 ProgressDeadlineExceeded 的条件。
	// 注意，Deployment 暂停期间不会计算进展。默认为 600 秒。
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty" protobuf:"varint,9,opt,name=progressDeadlineSeconds"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DeploymentRollback

// 已弃用。
// DeploymentRollback 保存回滚 Deployment 所需的信息。
type DeploymentRollback struct {
	metav1.TypeMeta `json:""`
	// 必填：必须与某个 Deployment 的名称一致。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 要更新到 Deployment 上的注解。
	// +optional
	UpdatedAnnotations map[string]string `json:"updatedAnnotations,omitempty" protobuf:"bytes,2,rep,name=updatedAnnotations"`
	// 本次 Deployment 回滚的配置。
	// +optional
	RollbackTo RollbackConfig `json:"rollbackTo" protobuf:"bytes,3,opt,name=rollbackTo"`
}

// 已弃用。
// RollbackConfig 描述回滚的目标版本。
type RollbackConfig struct {
	// 回滚到的修订版本。设为 0 表示回滚到最近一个修订版本。
	// +optional
	Revision int64 `json:"revision,omitempty" protobuf:"varint,1,opt,name=revision"`
}

// Deployment 使用的默认标签键。
const (
	// DefaultDeploymentUniqueLabelKey 是添加到现有 ReplicaSet 选择器上的默认键
	// （也是添加到其 Pod 上的标签键），用于阻止现有 ReplicaSet 选中新 Pod，
	// 以及新 ReplicaSet 选中旧 Pod。
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy 描述如何用新 Pod 替换现有 Pod。
type DeploymentStrategy struct {
	// 部署类型，可取 "Recreate" 或 "RollingUpdate"，默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=DeploymentStrategyType"`

	// 滚动更新的配置参数，仅当 DeploymentStrategyType 为 RollingUpdate 时存在。
	//---
	// TODO: 待 oneOf 约定确定后按该约定更新此处。
	// +optional
	RollingUpdate *RollingUpdateDeployment `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DeploymentStrategyType 表示部署策略类型的字符串枚举。
type DeploymentStrategyType string

// DeploymentStrategyType 的合法取值。
const (
	// 先杀掉所有现有 Pod，再创建新 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// 通过滚动更新用新 ReplicaSet 替换旧 ReplicaSet，即逐步缩容旧 ReplicaSet 并扩容新 ReplicaSet。
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// RollingUpdateDeployment 控制滚动更新的期望行为。
type RollingUpdateDeployment struct {
	// 更新期间允许不可用的最大 Pod 数。可取绝对值（如 5）或期望 Pod 数的百分比（如 10%），
	// 百分比向下取整为绝对值。当 MaxSurge 为 0 时该值不能为 0，默认为 25%。
	// 例如设为 30% 时，滚动更新一开始即可将旧 ReplicaSet 缩容到期望 Pod 数的 70%；
	// 新 Pod 就绪后可继续缩容旧 ReplicaSet 并扩容新 ReplicaSet，从而保证更新期间
	// 始终至少有 70% 的期望 Pod 可用。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 可超出期望 Pod 数调度的最大 Pod 数。可取绝对值（如 5）或期望 Pod 数的百分比（如 10%）。
	// 当 MaxUnavailable 为 0 时该值不能为 0，百分比向上取整为绝对值，默认为 25%。
	// 例如设为 30% 时，滚动更新一开始即可扩容新 ReplicaSet，使新旧 Pod 总数不超过期望值的 130%；
	// 旧 Pod 被杀掉后可继续扩容新 ReplicaSet，从而保证更新期间运行中的 Pod 总数不超过期望值的 130%。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DeploymentStatus 是最近观测到的 Deployment 状态。
type DeploymentStatus struct {
	// 部署控制器观测到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 该 Deployment 关联的未终止 Pod 总数（标签与选择器匹配）。
	// +optional
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// 该 Deployment 关联且具有期望模板规格的未终止 Pod 总数。
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,3,opt,name=updatedReplicas"`

	// 该 Deployment 关联且处于 Ready 条件的未终止 Pod 总数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,7,opt,name=readyReplicas"`

	// 该 Deployment 关联的可用未终止 Pod 总数（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,4,opt,name=availableReplicas"`

	// 该 Deployment 关联的不可用 Pod 总数，即达到 100% 可用容量仍缺少的 Pod 数。
	// 这些 Pod 可能是运行中但尚不可用，也可能是尚未创建的。
	// +optional
	UnavailableReplicas int32 `json:"unavailableReplicas,omitempty" protobuf:"varint,5,opt,name=unavailableReplicas"`

	// 该 Deployment 关联的终止中 Pod 总数。终止中的 Pod 具有非空 .metadata.deletionTimestamp，
	// 且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 该字段处于 beta 阶段，需要启用 DeploymentReplicaSetTerminatingReplicas 特性（默认启用）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,9,opt,name=terminatingReplicas"`

	// 对 Deployment 当前状态的最新可用观测。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DeploymentCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`

	// collisionCount 是 Deployment 的哈希碰撞次数。控制器创建新 ReplicaSet 名称时，
	// 用该字段作为避免碰撞的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,8,opt,name=collisionCount"`
}

// DeploymentConditionType 表示 Deployment 条件类型的字符串枚举。
type DeploymentConditionType string

// Deployment 的合法条件类型。
const (
	// Available 表示 Deployment 可用，即至少所需的最小可用副本数已运行至少 minReadySeconds。
	DeploymentAvailable DeploymentConditionType = "Available"
	// Progressing 表示 Deployment 正在取得进展。新建或接管 ReplicaSet、扩容新 Pod 或缩容旧 Pod
	// 都视为取得进展。暂停的 Deployment 或未指定 progressDeadlineSeconds 时不计算进展。
	DeploymentProgressing DeploymentConditionType = "Progressing"
	// ReplicaFailure 在 Deployment 的某个 Pod 创建或删除失败时添加。
	DeploymentReplicaFailure DeploymentConditionType = "ReplicaFailure"
)

// DeploymentCondition 描述 Deployment 在某一时刻的状态。
type DeploymentCondition struct {
	// Type 是 Deployment 条件的类型。
	// +optional
	Type DeploymentConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DeploymentConditionType"`
	// Status 是条件的状态，取值为 True、False 或 Unknown。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// LastUpdateTime 是该条件最近一次更新的时间。
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,6,opt,name=lastUpdateTime"`
	// LastTransitionTime 是条件最近一次发生状态转换的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,7,opt,name=lastTransitionTime"`
	// Reason 是条件最近一次状态转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// Message 是人可读的消息，说明状态转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DeploymentList

// DeploymentList 是 Deployment 的列表。
type DeploymentList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Deployment 对象列表。
	Items []Deployment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ControllerRevision

// 已弃用：ControllerRevision 的该组版本已被 apps/v1beta2/ControllerRevision 取代，详见发布说明。
// ControllerRevision 是不可变的状态数据快照。客户端负责序列化和反序列化包含其内部状态的对象。
// ControllerRevision 创建成功后不可更新。API Server 会拒绝所有试图修改 Data 字段的请求，
// 但允许删除。注意，由于 DaemonSet 和 StatefulSet 控制器都用它进行更新与回滚，该对象处于 beta 阶段；
// 未来版本中其名称和表示形式仍可能变化，客户端不应依赖其稳定性。它主要供控制器内部使用。
type ControllerRevision struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// data 是状态的序列化表示。
	// +required
	Data runtime.RawExtension `json:"data" protobuf:"bytes,2,opt,name=data"`

	// revision 表示 Data 所代表状态的修订号。
	// +optional
	Revision int64 `json:"revision" protobuf:"varint,3,opt,name=revision"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ControllerRevisionList

// ControllerRevisionList 是包含若干 ControllerRevision 对象的资源。
type ControllerRevisionList struct {
	metav1.TypeMeta `json:""`

	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ControllerRevision 对象列表。
	Items []ControllerRevision `json:"items" protobuf:"bytes,2,rep,name=items"`
}
