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

package v1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// ControllerRevisionHashLabelKey 是存储 ControllerRevision 哈希的 label key。
	ControllerRevisionHashLabelKey = "controller-revision-hash"
	// StatefulSetRevisionLabel 是 StatefulSet 修订版本的 label，与 ControllerRevisionHashLabelKey 相同。
	StatefulSetRevisionLabel = ControllerRevisionHashLabelKey
	// DeprecatedRollbackTo 是已弃用的回滚目标注解 key。
	DeprecatedRollbackTo = "deprecated.deployment.rollback.to"
	// DeprecatedTemplateGeneration 是已弃用的 DaemonSet 模板 generation 注解 key。
	DeprecatedTemplateGeneration = "deprecated.daemonset.template.generation"
	// StatefulSetPodNameLabel 是记录 StatefulSet 所属 Pod 名称的 label key。
	StatefulSetPodNameLabel = "statefulset.kubernetes.io/pod-name"
	// PodIndexLabel 是记录 Pod 在 StatefulSet 中序号的 label key。
	PodIndexLabel = "apps.kubernetes.io/pod-index"
)

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// StatefulSet 表示一组具有一致身份的 Pod。
// 身份定义为：
//   - 网络：单一稳定的 DNS 和主机名。
//   - 存储：按需提供任意数量的 VolumeClaim。
//
// StatefulSet 保证给定的网络身份始终映射到相同的存储身份。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type StatefulSet struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义该集合中 Pod 的期望身份。
	// +required
	Spec StatefulSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是该 StatefulSet 中 Pod 的当前状态。这些数据
	// 可能存在一定时间窗口的滞后。
	// +optional
	Status StatefulSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodManagementPolicyType 定义 StatefulSet 下创建 Pod 的策略。
// +enum
type PodManagementPolicyType string

const (
	// OrderedReadyPodManagement 在扩容时按严格递增顺序创建 Pod，
	// 在缩容时按严格递减顺序删除 Pod，且仅当上一个 Pod 就绪或终止后才继续。
	// 任何时刻最多只会变更一个 Pod。
	OrderedReadyPodManagement PodManagementPolicyType = "OrderedReady"
	// ParallelPodManagement 在 StatefulSet 副本数变化后立即创建和删除 Pod，
	// 不会等待 Pod 就绪或终止完成。
	ParallelPodManagement PodManagementPolicyType = "Parallel"
)

// StatefulSetUpdateStrategy 表示 StatefulSet 控制器执行更新时使用的策略，
// 包含按指定策略执行更新所需的其他参数。
type StatefulSetUpdateStrategy struct {
	// Type 表示 StatefulSetUpdateStrategy 的类型。
	// 默认为 RollingUpdate。
	// +optional
	Type StatefulSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetStrategyType"`
	// 当 Type 为 RollingUpdateStatefulSetStrategyType 时，RollingUpdate 用于传递参数。
	// +optional
	RollingUpdate *RollingUpdateStatefulSetStrategy `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// StatefulSetUpdateStrategyType 是字符串枚举类型，枚举
// StatefulSet 控制器所有可能的更新策略。
// +enum
type StatefulSetUpdateStrategyType string

const (
	// RollingUpdateStatefulSetStrategyType 表示更新会按照 StatefulSet 的
	// 顺序约束应用到 StatefulSet 中的所有 Pod。使用该策略执行扩容时，
	// 新 Pod 会依据 StatefulSet 的 updateRevision 所指的 spec 版本创建。
	RollingUpdateStatefulSetStrategyType StatefulSetUpdateStrategyType = "RollingUpdate"
	// OnDeleteStatefulSetStrategyType 禁用有序的滚动重启。版本
	// 跟踪尽力而为——控制器会尝试最终使 StatefulSet 的 currentRevision 与 updateRevision 收敛。
	// Pod 在被手动删除时根据 StatefulSetSpec 重建。
	// 使用该策略执行扩容时，新 Pod 会依据 StatefulSet 的 updateRevision 所指的 spec 版本创建。
	OnDeleteStatefulSetStrategyType StatefulSetUpdateStrategyType = "OnDelete"
	// RecreateStatefulSetStrategyType 表示在创建任何新版本 Pod 之前，
	// 先删除所有现有 Pod 并等待其完全终止。
	// 这确保旧版本和新版本的 Pod 不会同时运行。
	// 这是 alpha 类型，需要启用 StatefulSetRecreateStrategy feature gate。
	// 启用该 feature gate 时允许切换到 Recreate 策略，也允许从该策略切换到其他更新策略。
	//
	// +featureGate=StatefulSetRecreateStrategy
	RecreateStatefulSetStrategyType StatefulSetUpdateStrategyType = "Recreate"
)

// RollingUpdateStatefulSetStrategy 用于传递 RollingUpdateStatefulSetStrategyType 的参数。
type RollingUpdateStatefulSetStrategy struct {
	// Partition 表示 StatefulSet 更新时的分区序号。滚动更新期间，
	// 从序号 Replicas-1 到 Partition 的所有 Pod 都会被更新，
	// 从序号 Partition-1 到 0 的所有 Pod 保持不变。
	// 这有助于执行基于 canary 的发布。默认值为 0。
	// +optional
	Partition *int32 `json:"partition,omitempty" protobuf:"varint,1,opt,name=partition"`
	// 更新期间最多不可用的 Pod 数。
	// 值可以是绝对数（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 绝对数由百分比向上取整得到，不能为 0。
	// 默认为 1。该字段为 beta 级别，默认启用。该字段适用于 0 到
	// Replicas-1 范围内的所有 Pod。也就是说，如果 0 到 Replicas-1 范围内存在
	// 任何不可用的 Pod，都会计入 MaxUnavailable。
	// 对于 OrderedReady podManagementPolicy，该设置可能不生效；该策略确保
	// Pod 逐个创建并就绪。
	//
	// +featureGate=MaxUnavailableStatefulSet
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"varint,2,opt,name=maxUnavailable"`
}

// PersistentVolumeClaimRetentionPolicyType 是字符串枚举，用于确定
// 当控制它的 StatefulSet 被删除或缩容时，VolumeClaimTemplates 产生的卷何时被删除。
type PersistentVolumeClaimRetentionPolicyType string

const (
	// RetainPersistentVolumeClaimRetentionPolicyType 是默认的
	// PersistentVolumeClaimRetentionPolicy，指定与 StatefulSet VolumeClaimTemplates
	// 关联的 PersistentVolumeClaims 不会被删除。
	RetainPersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Retain"
	// DeletePersistentVolumeClaimRetentionPolicyType 指定
	// 与 StatefulSet VolumeClaimTemplates 关联的 PersistentVolumeClaims
	// 会在 StatefulSetPersistentVolumeClaimRetentionPolicy 指定的场景下被删除。
	DeletePersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Delete"
)

// StatefulSetPersistentVolumeClaimRetentionPolicy 描述对由
// StatefulSet VolumeClaimTemplates 创建的 PVC 使用的保留策略。
type StatefulSetPersistentVolumeClaimRetentionPolicy struct {
	// WhenDeleted 指定 StatefulSet 被删除时，由 StatefulSet VolumeClaimTemplates
	// 创建的 PVC 会发生什么。默认策略 `Retain` 使 PVC 不受 StatefulSet 删除影响，
	// `Delete` 策略则会删除这些 PVC。
	// +optional
	WhenDeleted PersistentVolumeClaimRetentionPolicyType `json:"whenDeleted,omitempty" protobuf:"bytes,1,opt,name=whenDeleted,casttype=PersistentVolumeClaimRetentionPolicyType"`
	// WhenScaled 指定 StatefulSet 缩容时，由 StatefulSet VolumeClaimTemplates
	// 创建的 PVC 会发生什么。默认策略 `Retain` 使 PVC 不受缩容影响，
	// `Delete` 策略则会删除副本数之外多余 Pod 对应的 PVC。
	// +optional
	WhenScaled PersistentVolumeClaimRetentionPolicyType `json:"whenScaled,omitempty" protobuf:"bytes,2,opt,name=whenScaled,casttype=PersistentVolumeClaimRetentionPolicyType"`
}

// StatefulSetOrdinals 描述该 StatefulSet 中副本序号的分配策略。
type StatefulSetOrdinals struct {
	// start 是表示首个副本索引的数字。可用它从备用索引（例如从 1 开始）
	// 为副本编号，而非默认的从 0 开始；也可用于协调副本从一个
	// StatefulSet 逐步迁移到另一个。
	// 如果设置，副本索引范围为：
	//   [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas)。
	// 如果未设置，默认为 0，副本索引范围为：
	//   [0, .spec.replicas)。
	// +optional
	Start int32 `json:"start" protobuf:"varint,1,opt,name=start"`
}

// StatefulSetSpec 是 StatefulSet 的 spec。
type StatefulSetSpec struct {
	// replicas 是给定 Template 的期望副本数。
	// 这里的副本是指同一 Template 的实例，但每个副本也具有一致的身份。
	// 如果未指定，默认为 1。
	// TODO: 考虑重命名该字段。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// selector 是对应匹配副本数的 Pod 的 label 查询。
	// 它必须匹配 Pod 模板的 label。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	// +k8s:alpha(since: "1.37")=+k8s:required
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// template 是描述副本不足时将要创建的 Pod 的对象。
	// StatefulSet 创建的每个 Pod 都符合该 Template，但各自具有
	// 区别于其他 Pod 的唯一身份。每个 Pod 的命名格式为
	// <statefulsetname>-<podindex>。例如，名为 "web" 的 StatefulSet 中
	// 索引为 "3" 的 Pod 会被命名为 "web-3"。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// volumeClaimTemplates 是允许 Pod 引用的 PVC 列表。
	// StatefulSet 控制器负责以维护 Pod 身份的方式，将网络身份映射到
	// 这些 PVC。列表中的每个 PVC 必须在模板的某个容器中至少有一个
	// 名称匹配的 volumeMount。列表中同名的 PVC 优先于模板中的任何 volume。
	// TODO: 定义当同名 PVC 已存在时的行为。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	// +listType=atomic
	VolumeClaimTemplates []v1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty" protobuf:"bytes,4,rep,name=volumeClaimTemplates"`

	// serviceName 是管理该 StatefulSet 的 Service 名称。
	// 该 Service 必须在 StatefulSet 之前存在，并负责该集合的网络身份。
	// Pod 获得的 DNS/主机名遵循以下模式：
	// pod-specific-string.serviceName.default.svc.cluster.local
	// 其中 "pod-specific-string" 由 StatefulSet 控制器管理。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	ServiceName string `json:"serviceName" protobuf:"bytes,5,opt,name=serviceName"`

	// podManagementPolicy 控制初始扩容、替换节点上的 Pod 以及缩容时
	// 如何创建 Pod。默认策略为 `OrderedReady`：Pod 按递增顺序创建
	// （pod-0、pod-1 等），控制器会等待每个 Pod 就绪后再继续。
	// 缩容时按相反顺序删除 Pod。
	// 另一策略 `Parallel` 会并行创建 Pod 以快速达到期望规模而不等待，
	// 缩容时会一次性删除所有 Pod。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	// +k8s:alpha(since: "1.37")=+k8s:optional
	PodManagementPolicy PodManagementPolicyType `json:"podManagementPolicy,omitempty" protobuf:"bytes,6,opt,name=podManagementPolicy,casttype=PodManagementPolicyType"`

	// updateStrategy 表示当 Template 发生变更时，用于更新
	// StatefulSet 中 Pod 的 StatefulSetUpdateStrategy。
	// +optional
	UpdateStrategy StatefulSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,7,opt,name=updateStrategy"`

	// revisionHistoryLimit 是 StatefulSet 修订历史中保留的最大修订数。
	// 修订历史由所有未被当前应用的 StatefulSetSpec 版本表示的修订组成。
	// 默认值为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,8,opt,name=revisionHistoryLimit"`

	// 新建 Pod 在被视为可用之前，需要保持就绪且容器不发生崩溃的最少秒数。
	// 默认为 0（Pod 就绪后即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,9,opt,name=minReadySeconds"`

	// persistentVolumeClaimRetentionPolicy 描述由 volumeClaimTemplates 创建的
	// PVC 的生命周期。默认情况下，所有 PVC 按需创建并保留，直到被手动删除。
	// 该策略允许改变生命周期，例如在其 StatefulSet 被删除或其 Pod 缩容时
	// 删除 PVC。
	// +optional
	PersistentVolumeClaimRetentionPolicy *StatefulSetPersistentVolumeClaimRetentionPolicy `json:"persistentVolumeClaimRetentionPolicy,omitempty" protobuf:"bytes,10,opt,name=persistentVolumeClaimRetentionPolicy"`

	// ordinals 控制 StatefulSet 中副本索引的编号方式。
	// 默认行为是首个副本索引为 "0"，之后每增加一个副本索引加一。
	// +optional
	Ordinals *StatefulSetOrdinals `json:"ordinals,omitempty" protobuf:"bytes,11,opt,name=ordinals"`
}

// StatefulSetStatus 表示 StatefulSet 的当前状态。
type StatefulSetStatus struct {
	// observedGeneration 是该 StatefulSet 最近一次观察到的 generation，对应
	// StatefulSet 的 generation，在发生变更时由 API Server 更新。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// replicas 是 StatefulSet 控制器创建的 Pod 数。
	Replicas int32 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`

	// readyReplicas 是为该 StatefulSet 创建的、处于 Ready Condition 的 Pod 数。
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,3,opt,name=readyReplicas"`

	// currentReplicas 是 StatefulSet 控制器依据 currentRevision 所指版本
	// 创建的 Pod 数。
	CurrentReplicas int32 `json:"currentReplicas,omitempty" protobuf:"varint,4,opt,name=currentReplicas"`

	// updatedReplicas 是 StatefulSet 控制器依据 updateRevision 所指版本
	// 创建的 Pod 数。
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,5,opt,name=updatedReplicas"`

	// currentRevision 如果非空，表示用于生成序列 [0,currentReplicas) 中
	// Pod 的 StatefulSet 版本。
	CurrentRevision string `json:"currentRevision,omitempty" protobuf:"bytes,6,opt,name=currentRevision"`

	// updateRevision 如果非空，表示用于生成序列
	// [replicas-updatedReplicas,replicas) 中 Pod 的 StatefulSet 版本。
	UpdateRevision string `json:"updateRevision,omitempty" protobuf:"bytes,7,opt,name=updateRevision"`

	// collisionCount 是 StatefulSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// 表示对 statefulset 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []StatefulSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`

	// 该 statefulset 所针对的可用 Pod 总数（至少就绪 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas" protobuf:"varint,11,opt,name=availableReplicas"`
}

// StatefulSetConditionType 表示 StatefulSet 的条件类型。
type StatefulSetConditionType string

const (
	// StatefulSetProgressing 表示 StatefulSet 正在更新中。
	// 该条件为 alpha 类型，需要启用 StatefulSetRecreateStrategy feature gate。
	StatefulSetProgressing StatefulSetConditionType = "Progressing"
)

// StatefulSetCondition 描述某个时刻 statefulset 的状态。
type StatefulSetCondition struct {
	// statefulset 条件的类型。
	// +optional
	Type StatefulSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=StatefulSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件上一次从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上一次转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 人类可读的消息，说明该次转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// StatefulSetList 是 StatefulSet 的集合。
type StatefulSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 stateful set 的列表。
	Items []StatefulSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// Deployment 支持对 Pod 和 ReplicaSet 的声明式更新。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type Deployment struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 该 Deployment 期望行为的 spec。
	// +required
	Spec DeploymentSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近观察到的 Deployment 状态。
	// +optional
	Status DeploymentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeploymentSpec 是 Deployment 期望行为的 spec。
type DeploymentSpec struct {
	// 期望的 Pod 数。使用指针以区分显式零值和未指定。默认为 1。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// Pod 的 label selector。其 Pod 被选中的现有 ReplicaSet 会受该
	// deployment 影响。它必须匹配 Pod 模板的 label。
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// Template 描述将要创建的 Pod。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// 用新 Pod 替换现有 Pod 时使用的 deployment 策略。
	// +optional
	// +patchStrategy=retainKeys
	Strategy DeploymentStrategy `json:"strategy,omitempty" patchStrategy:"retainKeys" protobuf:"bytes,4,opt,name=strategy"`

	// 新建 Pod 在被视为可用之前，需要保持就绪且容器不发生崩溃的最少秒数。
	// 默认为 0（Pod 就绪后即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,5,opt,name=minReadySeconds"`

	// 为支持回滚而保留的旧 ReplicaSet 数量。
	// 使用指针以区分显式零值和未指定。默认为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`

	// 表示该 deployment 已暂停。
	// +optional
	Paused bool `json:"paused,omitempty" protobuf:"varint,7,opt,name=paused"`

	// deployment 在被视为失败前取得进展的最长时间（秒）。deployment 控制器
	// 会继续处理失败的 deployment，并在 deployment 状态中显示原因为
	// ProgressDeadlineExceeded 的条件。注意，deployment 暂停期间不会估算进展。
	// 默认为 600 秒。
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty" protobuf:"varint,9,opt,name=progressDeadlineSeconds"`
}

const (
	// DefaultDeploymentUniqueLabelKey 是添加到现有 ReplicaSet 的 selector 的默认 key
	// （也是添加到其 Pod 的 label key），用于防止现有 ReplicaSet
	// 选中新 Pod（以及新 ReplicaSet 选中旧 Pod）。
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy 描述如何用新 Pod 替换现有 Pod。
type DeploymentStrategy struct {
	// deployment 的类型。可以是 "Recreate" 或 "RollingUpdate"，默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=DeploymentStrategyType"`

	// 滚动更新配置参数。仅当 DeploymentStrategyType 为
	// RollingUpdate 时存在。
	//---
	// TODO: 按我们约定的 oneOf 方式更新，无论最终约定是什么。
	// +optional
	RollingUpdate *RollingUpdateDeployment `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DeploymentStrategyType 表示 deployment 的更新策略类型。
// +enum
type DeploymentStrategyType string

const (
	// RecreateDeploymentStrategyType 在创建新 Pod 前杀掉所有现有 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// RollingUpdateDeploymentStrategyType 通过滚动更新用新 ReplicaSet 替换旧 ReplicaSet，
	// 即逐步缩减旧 ReplicaSet 并扩容新 ReplicaSet。
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// RollingUpdateDeployment 用于指定滚动更新的期望行为。
type RollingUpdateDeployment struct {
	// 更新期间最多不可用的 Pod 数。
	// 值可以是绝对数（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 绝对数由百分比向下取整得到。
	// 如果 MaxSurge 为 0，则该值不能为 0。
	// 默认为 25%。
	// 示例：当该值设为 30% 时，滚动更新开始时旧 ReplicaSet 可立即
	// 缩减到期望 Pod 数的 70%。新 Pod 就绪后，可进一步缩减旧 ReplicaSet，
	// 随后扩容新 ReplicaSet，确保更新期间可用 Pod 总数始终不少于
	// 期望 Pod 数的 70%。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 可超出期望 Pod 数调度的最大 Pod 数。
	// 值可以是绝对数（例如 5），也可以是期望 Pod 数的百分比（例如 10%）。
	// 如果 MaxUnavailable 为 0，则该值不能为 0。
	// 绝对数由百分比向上取整得到。
	// 默认为 25%。
	// 示例：当该值设为 30% 时，滚动更新开始时新 ReplicaSet 可立即扩容，
	// 使新旧 Pod 总数不超过期望 Pod 数的 130%。旧 Pod 被杀掉后，
	// 可进一步扩容新 ReplicaSet，确保更新期间运行的 Pod 总数
	// 始终不超过期望 Pod 数的 130%。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DeploymentStatus 是最近观察到的 Deployment 状态。
type DeploymentStatus struct {
	// deployment 控制器观察到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 该 deployment 所针对的非终止中 Pod 总数（其 label 匹配 selector）。
	// +optional
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// 该 deployment 所针对的、具有期望模板 spec 的非终止中 Pod 总数。
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,3,opt,name=updatedReplicas"`

	// 该 Deployment 所针对的、处于 Ready Condition 的非终止中 Pod 总数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,7,opt,name=readyReplicas"`

	// 该 deployment 所针对的可用非终止中 Pod 总数（至少就绪 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,4,opt,name=availableReplicas"`

	// 该 deployment 所针对的不可用 Pod 总数。这是该 deployment 达到 100%
	// 可用容量仍需要的 Pod 总数。这些 Pod 可能是正在运行但尚未可用的 Pod，
	// 也可能是尚未创建的 Pod。
	// +optional
	UnavailableReplicas int32 `json:"unavailableReplicas,omitempty" protobuf:"varint,5,opt,name=unavailableReplicas"`

	// 该 deployment 所针对的终止中 Pod 总数。终止中的 Pod 具有非空的
	// .metadata.deletionTimestamp，且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要启用 DeploymentReplicaSetTerminatingReplicas feature（默认启用）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,9,opt,name=terminatingReplicas"`

	// 表示对 deployment 当前状态的最新可用观测。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DeploymentCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`

	// Deployment 的哈希冲突计数。当控制器需要为最新的
	// ReplicaSet 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,8,opt,name=collisionCount"`
}

// DeploymentConditionType 表示 deployment 的条件类型。
type DeploymentConditionType string

// 以下是 deployment 的有效条件。
const (
	// DeploymentAvailable 表示 deployment 可用，即满足最低可用性要求的副本
	// 至少已运行 minReadySeconds。条件值为 "Available"。
	DeploymentAvailable DeploymentConditionType = "Available"
	// DeploymentProgressing 表示 deployment 正在推进。当创建或接纳新的 replica set，
	// 以及新 Pod 扩容或旧 Pod 缩容时，视为 deployment 有进展。
	// 对于已暂停或未指定 progressDeadlineSeconds 的 deployment，不估算进展。
	// 条件值为 "Progressing"。
	DeploymentProgressing DeploymentConditionType = "Progressing"
	// DeploymentReplicaFailure 在 deployment 的某个 Pod 创建或删除失败时添加。
	// 条件值为 "ReplicaFailure"。
	DeploymentReplicaFailure DeploymentConditionType = "ReplicaFailure"
)

// DeploymentCondition 描述某个时刻 deployment 的状态。
type DeploymentCondition struct {
	// deployment 条件的类型。
	// +optional
	Type DeploymentConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DeploymentConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 该条件最后一次更新的时间。
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,6,opt,name=lastUpdateTime"`
	// 条件上一次从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,7,opt,name=lastTransitionTime"`
	// 条件上一次转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 人类可读的消息，说明该次转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// DeploymentList 是 Deployment 的列表。
type DeploymentList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Deployment 的列表。
	Items []Deployment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// DaemonSetUpdateStrategy 用于控制 DaemonSet 更新策略的结构体。
type DaemonSetUpdateStrategy struct {
	// daemon set 更新的类型。可以是 "RollingUpdate" 或 "OnDelete"，默认为 RollingUpdate。
	// +optional
	Type DaemonSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`

	// 滚动更新配置参数。仅当 type 为 "RollingUpdate" 时存在。
	//---
	// TODO: 按我们约定的 oneOf 方式更新，无论最终约定是什么。
	// 与 Deployment 的 `strategy.rollingUpdate` 相同。
	// 参见 https://github.com/kubernetes/kubernetes/issues/35345
	// +optional
	RollingUpdate *RollingUpdateDaemonSet `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DaemonSetUpdateStrategyType 表示 DaemonSet 的更新策略类型。
// +enum
type DaemonSetUpdateStrategyType string

const (
	// RollingUpdateDaemonSetStrategyType 通过滚动更新用新 daemon 替换旧 daemon，
	// 即在每个节点上逐个替换。
	RollingUpdateDaemonSetStrategyType DaemonSetUpdateStrategyType = "RollingUpdate"

	// OnDeleteDaemonSetStrategyType 仅在旧 daemon 被杀掉后才替换它。
	OnDeleteDaemonSetStrategyType DaemonSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateDaemonSet 用于指定 daemon set 滚动更新的期望行为。
type RollingUpdateDaemonSet struct {
	// 更新期间最多不可用的 DaemonSet Pod 数。值可以是绝对数（例如 5），
	// 也可以是更新开始时 DaemonSet Pod 总数的百分比（例如 10%）。
	// 绝对数由百分比向上取整得到。
	// 如果 MaxSurge 为 0，则该值不能为 0。
	// 默认值为 1。
	// 示例：当该值设为 30% 时，任意时刻最多 30% 的、本应运行 daemon Pod 的
	// 节点（即 status.desiredNumberScheduled）可以因更新而停止其 Pod。
	// 更新开始时最多停止 30% 的 DaemonSet Pod，然后在原位置启动新的
	// DaemonSet Pod。新 Pod 可用后，继续更新其他 DaemonSet Pod，
	// 从而确保更新期间原始 DaemonSet Pod 中至少有 70% 始终可用。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 更新期间允许在已有可用 DaemonSet Pod 的节点上创建更新后 DaemonSet Pod
	// 的最大节点数。值可以是绝对数（例如 5），也可以是期望 Pod 数的百分比
	// （例如 10%）。如果 MaxUnavailable 为 0，则该值不能为 0。
	// 绝对数由百分比向上取整得到，最小为 1。默认值为 0。
	// 示例：当该值设为 30% 时，最多 30% 的、本应运行 daemon Pod 的节点
	// （即 status.desiredNumberScheduled）可以在旧 Pod 标记为删除之前
	// 创建新 Pod。更新开始时先在 30% 的节点上启动新 Pod。一旦更新后的
	// Pod 可用（至少就绪 minReadySeconds），该节点上的旧 DaemonSet Pod
	// 就会被标记为删除。如果旧 Pod 因任何原因变为不可用（Ready 转为 false、
	// 被驱逐或被排空），则会立即在该节点上创建更新后的 Pod，而不考虑 surge 限制。
	// 允许 surge 意味着，如果就绪检查失败，daemonset 在任意节点上消耗的
	// 资源可能翻倍，因此资源密集型 daemonset 应考虑中断期间可能引发的驱逐。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DaemonSetSpec 是 daemon set 的 spec。
type DaemonSetSpec struct {
	// 对由 daemon set 管理的 Pod 的 label 查询。
	// 必须匹配才能被控制。它必须匹配 Pod 模板的 label。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,1,opt,name=selector"`

	// 描述将要创建的 Pod 的对象。
	// DaemonSet 会在每个匹配模板 node selector 的节点上（若未指定 node
	// selector 则在每个节点上）恰好创建一个该 Pod 的副本。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	// +required
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,2,opt,name=template"`

	// 用新 Pod 替换现有 DaemonSet Pod 的更新策略。
	// +optional
	UpdateStrategy DaemonSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,3,opt,name=updateStrategy"`

	// 新建 DaemonSet Pod 在被视为可用之前，需要保持就绪且容器不发生崩溃的
	// 最少秒数。默认为 0（Pod 就绪后即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// 为支持回滚而保留的旧历史数量。
	// 使用指针以区分显式零值和未指定。默认为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`
}

// DaemonSetStatus 表示 daemon set 的当前状态。
type DaemonSetStatus struct {
	// 运行至少 1 个 daemon Pod 且本应运行该 daemon Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	CurrentNumberScheduled int32 `json:"currentNumberScheduled" protobuf:"varint,1,opt,name=currentNumberScheduled"`

	// 正在运行 daemon Pod 但本不应运行该 daemon Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	NumberMisscheduled int32 `json:"numberMisscheduled" protobuf:"varint,2,opt,name=numberMisscheduled"`

	// 本应运行 daemon Pod 的节点总数（包括正确运行 daemon Pod 的节点）。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	DesiredNumberScheduled int32 `json:"desiredNumberScheduled" protobuf:"varint,3,opt,name=desiredNumberScheduled"`

	// numberReady 是本应运行 daemon Pod 且至少有一个 daemon Pod
	// 处于 Ready Condition 的节点数。
	NumberReady int32 `json:"numberReady" protobuf:"varint,4,opt,name=numberReady"`

	// daemon set 控制器最近观察到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,5,opt,name=observedGeneration"`

	// 正在运行更新后 daemon Pod 的节点总数。
	// +optional
	UpdatedNumberScheduled int32 `json:"updatedNumberScheduled,omitempty" protobuf:"varint,6,opt,name=updatedNumberScheduled"`

	// 本应运行 daemon Pod 且至少有一个 daemon Pod 正在运行并可用
	// （至少就绪 spec.minReadySeconds）的节点数。
	// +optional
	NumberAvailable int32 `json:"numberAvailable,omitempty" protobuf:"varint,7,opt,name=numberAvailable"`

	// 本应运行 daemon Pod 但没有一个 daemon Pod 正在运行且可用
	// （至少就绪 spec.minReadySeconds）的节点数。
	// +optional
	NumberUnavailable int32 `json:"numberUnavailable,omitempty" protobuf:"varint,8,opt,name=numberUnavailable"`

	// DaemonSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// 表示对 DaemonSet 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DaemonSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`
}

// DaemonSetConditionType 表示 DaemonSet 的条件类型。
type DaemonSetConditionType string

// TODO: 补充 DaemonSet 的有效条件类型。

// DaemonSetCondition 描述某个时刻 DaemonSet 的状态。
type DaemonSetCondition struct {
	// DaemonSet 条件的类型。
	// +optional
	Type DaemonSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DaemonSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件上一次从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上一次转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 人类可读的消息，说明该次转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// DaemonSet 表示 daemon set 的配置。
// +k8s:supportsSubresource="/status"
type DaemonSet struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 该 daemon set 的期望行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec DaemonSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 该 daemon set 的当前状态。这些数据可能存在一定时间窗口的滞后。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status DaemonSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

const (
	// DefaultDaemonSetUniqueLabelKey 是添加到现有 DaemonSet Pod 的默认 label key，
	// 用于在 DaemonSet 模板更新期间区分新旧 DaemonSet Pod。
	DefaultDaemonSetUniqueLabelKey = ControllerRevisionHashLabelKey
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// DaemonSetList 是 daemon set 的集合。
type DaemonSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// daemon set 的列表。
	Items []DaemonSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"
type ReplicaSet struct {
	metav1.TypeMeta `json:""`

	// 如果 ReplicaSet 的 Labels 为空，则默认与其管理的 Pod 相同。
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 ReplicaSet 期望行为的 spec。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec ReplicaSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是最近观察到的 ReplicaSet 状态。
	// 这些数据可能存在一定时间窗口的滞后。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ReplicaSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

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

// ReplicaSetSpec 是 ReplicaSet 的 spec。
type ReplicaSetSpec struct {
	// Replicas 是期望的 Pod 数。
	// 使用指针以区分显式零值和未指定。默认为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// 新建 Pod 在被视为可用之前，需要保持就绪且容器不发生崩溃的最少秒数。
	// 默认为 0（Pod 就绪后即被视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// Selector 是对应匹配副本数的 Pod 的 label 查询。
	// 要被该 replica set 控制，label 的 key 和 value 必须匹配。
	// 它必须匹配 Pod 模板的 label。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +required
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`

	// Template 是描述副本不足时将要创建的 Pod 的对象。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/#pod-template
	// +optional
	Template v1.PodTemplateSpec `json:"template,omitempty" protobuf:"bytes,3,opt,name=template"`
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近观察到的非终止中 Pod 数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// label 匹配该 replicaset Pod 模板 label 的非终止中 Pod 数。
	// +optional
	FullyLabeledReplicas int32 `json:"fullyLabeledReplicas,omitempty" protobuf:"varint,2,opt,name=fullyLabeledReplicas"`

	// 该 ReplicaSet 所针对的、处于 Ready Condition 的非终止中 Pod 数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,4,opt,name=readyReplicas"`

	// 该 replica set 的可用非终止中 Pod 数（至少就绪 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,5,opt,name=availableReplicas"`

	// 该 replica set 的终止中 Pod 数。终止中的 Pod 具有非空的
	// .metadata.deletionTimestamp，且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要启用 DeploymentReplicaSetTerminatingReplicas feature（默认启用）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,7,opt,name=terminatingReplicas"`

	// ObservedGeneration 反映最近观察到的 ReplicaSet 的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`

	// 表示对 replica set 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []ReplicaSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`
}

// ReplicaSetConditionType 表示 replica set 的条件类型。
type ReplicaSetConditionType string

// 以下是 replica set 的有效条件。
const (
	// ReplicaSetReplicaFailure 在 replica set 的某个 Pod 因配额不足、limit range、
	// pod security policy、node selector 等原因无法创建，或因 kubelet 宕机、
	// finalizer 失败而无法删除时添加。
	ReplicaSetReplicaFailure ReplicaSetConditionType = "ReplicaFailure"
)

// ReplicaSetCondition 描述某个时刻 replica set 的状态。
type ReplicaSetCondition struct {
	// replica set 条件的类型。
	// +optional
	Type ReplicaSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=ReplicaSetConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	// +optional
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 条件上一次从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上一次转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 人类可读的消息，说明该次转换的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// ControllerRevision 实现状态数据的不可变快照。客户端
// 负责序列化和反序列化包含其内部状态的对象。
// ControllerRevision 一旦成功创建，就无法更新。
// API Server 会拒绝所有试图修改 Data 字段的请求。不过 ControllerRevision
// 可以被删除。注意，由于 DaemonSet 和 StatefulSet 控制器都使用它进行更新和回滚，
// 该对象属于 beta 级别。但是在未来版本中，其名称和表示形式可能会发生变化，
// 客户端不应依赖其稳定性。它主要供控制器内部使用。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.9

// ControllerRevisionList 是包含 ControllerRevision 对象列表的资源。
type ControllerRevisionList struct {
	metav1.TypeMeta `json:""`

	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ControllerRevision 的列表。
	Items []ControllerRevision `json:"items" protobuf:"bytes,2,rep,name=items"`
}
