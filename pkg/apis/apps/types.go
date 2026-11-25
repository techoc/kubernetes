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

package apps

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StatefulSet 表示一组具有一致身份的 Pod。
// 身份定义包括：
// - 网络：单一稳定的 DNS 和主机名。
// - 存储：按需提供任意数量的 VolumeClaim。
//
// StatefulSet 保证给定的网络身份始终映射到相同的存储身份。
type StatefulSet struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义该集合中 Pod 的期望身份。
	// +optional
	Spec StatefulSetSpec

	// Status 是该 StatefulSet 中 Pod 的当前状态。这些数据可能存在一定程度的时间延迟。
	// +optional
	Status StatefulSetStatus
}

// PodManagementPolicyType 定义 StatefulSet 下创建 Pod 的策略。
type PodManagementPolicyType string

const (
	// OrderedReadyPodManagement 在扩容时按严格递增顺序创建 Pod，在缩容时按严格递减顺序删除，
	// 且仅在前一个 Pod 就绪或终止后才继续。任意时刻最多只变更一个 Pod。
	OrderedReadyPodManagement PodManagementPolicyType = "OrderedReady"
	// ParallelPodManagement 会在 StatefulSet 副本数变更时立即创建和删除 Pod，
	// 不会等待 Pod 就绪或完成终止。
	ParallelPodManagement PodManagementPolicyType = "Parallel"
)

// StatefulSetUpdateStrategy 表示 StatefulSet 控制器执行更新时所采用的
// 策略，包含按指定策略执行更新所需的附加参数。
type StatefulSetUpdateStrategy struct {
	// Type 表示 StatefulSetUpdateStrategy 的类型。
	Type StatefulSetUpdateStrategyType
	// 当 Type 为 RollingUpdateStatefulSetStrategyType 时，用 RollingUpdate 传递相关参数。
	RollingUpdate *RollingUpdateStatefulSetStrategy
}

// StatefulSetUpdateStrategyType 是字符串枚举类型，枚举
// StatefulSet 控制器所有可能的更新策略。
type StatefulSetUpdateStrategyType string

const (
	// RollingUpdateStatefulSetStrategyType 表示更新会按照 StatefulSet 的
	// 顺序约束应用到 StatefulSet 中的所有 Pod。使用该策略执行扩容时，
	// 新 Pod 会依据 StatefulSet 的 updateRevision 所指的 spec 版本创建。
	RollingUpdateStatefulSetStrategyType StatefulSetUpdateStrategyType = "RollingUpdate"
	// OnDeleteStatefulSetStrategyType 禁用有序滚动重启。版本跟踪以尽力而为的方式进行 ——
	// 控制器会尝试最终使 StatefulSet 的 currentRevision 与 updateRevision 收敛。
	// Pod 被手动删除时，会依据 StatefulSetSpec 重新创建。
	// 以该策略执行扩容时，新 Pod 会依据 StatefulSet 的 updateRevision 所指明的规格版本创建。
	OnDeleteStatefulSetStrategyType StatefulSetUpdateStrategyType = "OnDelete"
	// RecreateStatefulSetStrategyType 表示在创建任何新版本 Pod 之前，
	// 先删除所有现有 Pod 并等待其完全终止。
	// 这确保旧版本和新版本的 Pod 不会同时运行。
	// 这是 alpha 类型，需要启用 StatefulSetRecreateStrategy feature gate。
	// 启用该 feature gate 时允许切换到 Recreate 策略，也允许从该策略切换到其他更新策略。
	// +featureGate=StatefulSetRecreateStrategy
	RecreateStatefulSetStrategyType StatefulSetUpdateStrategyType = "Recreate"
)

// RollingUpdateStatefulSetStrategy 用于传递 RollingUpdateStatefulSetStrategyType 的参数。
type RollingUpdateStatefulSetStrategy struct {
	// Partition 表示 StatefulSet 更新时分段的起始序号。滚动更新期间，
	// 序号从 Replicas-1 到 Partition 的所有 Pod 都会被更新，
	// 序号从 Partition-1 到 0 的所有 Pod 保持不动。这便于进行金丝雀发布。默认值为 0。
	Partition int32
	// 更新期间允许不可用的 Pod 最大数量。
	// 值可以是绝对数量（如 5）或期望 Pod 数的百分比（如 10%）。
	// 绝对数量由百分比向上取整计算得出。此值不能为 0。
	// 默认为 1。此字段为 beta 级别，默认启用。该字段适用于范围 0 到
	// Replicas-1 内的所有 Pod。也就是说，若 0 到 Replicas-1 范围内存在任何不可用 Pod，
	// 都会被计入 MaxUnavailable。
	// 该设置对 OrderedReady 的 podManagementPolicy 可能无效，该策略确保 Pod 逐个创建并就绪。
	//
	// +featureGate=MaxUnavailableStatefulSet
	// +optional
	MaxUnavailable *intstr.IntOrString
}

// PersistentVolumeClaimRetentionPolicyType 是一个字符串枚举，用于决定当所属
// StatefulSet 被删除或缩容时，VolumeClaimTemplates 中的卷何时被删除。
type PersistentVolumeClaimRetentionPolicyType string

const (
	// RetainPersistentVolumeClaimRetentionPolicyType 是默认的
	// PersistentVolumeClaimRetentionPolicy，表示与 StatefulSet VolumeClaimTemplates
	// 关联的 PersistentVolumeClaims 不会被删除。
	RetainPersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Retain"
	// DeletePersistentVolumeClaimRetentionPolicyType 表示与 StatefulSet
	// VolumeClaimTemplates 关联的 PersistentVolumeClaims，
	// 将在 StatefulSetPersistentVolumeClaimRetentionPolicy 所指定的场景下被删除。
	DeletePersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Delete"
)

// StatefulSetPersistentVolumeClaimRetentionPolicy 描述由
// StatefulSet VolumeClaimTemplates 创建的 PVC 使用的保留策略。
type StatefulSetPersistentVolumeClaimRetentionPolicy struct {
	// WhenDeleted 指定当 StatefulSet 被删除时，由其 VolumeClaimTemplates
	// 创建的 PVC 如何处理。默认的 `Retain` 策略使 PVC 不受 StatefulSet 删除
	// 的影响。`Delete` 策略会导致这些 PVC 被删除。
	WhenDeleted PersistentVolumeClaimRetentionPolicyType
	// WhenScaled 指定当 StatefulSet 缩容时，由其 VolumeClaimTemplates
	// 创建的 PVC 如何处理。默认的 `Retain` 策略使 PVC 不受缩容的影响。
	// `Delete` 策略会导致超出副本数的多余 Pod 所关联的 PVC 被删除。
	WhenScaled PersistentVolumeClaimRetentionPolicyType
}

// StatefulSetOrdinals 描述本 StatefulSet 中副本序号分配所采用的策略。
type StatefulSetOrdinals struct {
	// start 表示第一个副本的起始序号。可用于从其他序号（例如从 1 开始）为副本编号，
	// 以替代默认的从 0 开始命名，也可用于协调副本从一个 StatefulSet 逐步迁移到另一个。
	// 如果设置，副本序号范围为：
	// [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas)。
	// 如果未设置，默认为 0，副本序号范围为：
	// [0, .spec.replicas)。
	// +optional
	Start int32
}

// StatefulSetSpec 是 StatefulSet 的规格定义。
type StatefulSetSpec struct {
	// replicas 是给定 Template 的期望副本数。
	// 这里的副本是指同一 Template 的实例，但每个副本也具有一致的身份。
	// 如果未指定，默认为 1。
	// TODO: 考虑重命名该字段。
	// +optional
	Replicas int32

	// Selector 是对 Pod 的标签查询，用于匹配副本数量。
	// 若为空，默认使用 Pod 模板上的标签。
	// 更多信息参见: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// Template 是描述待创建 Pod 的对象；当检测到副本不足时，由 StatefulSet 创建。
	// StatefulSet 创建的每个 Pod 都会满足此 Template，
	// 但拥有区别于该 StatefulSet 其余 Pod 的唯一身份。
	// 每个 Pod 将按 <statefulsetname>-<podindex> 格式命名。例如，名为
	// "web" 的 StatefulSet 中序号为 "3" 的 Pod 将被命名为 "web-3"。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	Template api.PodTemplateSpec

	// VolumeClaimTemplates 是 Pod 可以引用的声明（claim）列表。
	// StatefulSet 控制器负责将网络身份映射到声明，且映射方式保持 Pod 的身份不变。
	// 该列表中的每个声明都必须在模板的某个容器中至少有一个按名称匹配的
	// volumeMount。列表中的声明优先于模板中同名的卷。
	// TODO: 定义已存在同名声明时的行为。
	// +optional
	VolumeClaimTemplates []api.PersistentVolumeClaim

	// ServiceName 是管理该 StatefulSet 的 Service 名称。
	// 该 Service 必须在 StatefulSet 之前存在，并负责该集合的网络身份。
	// Pod 获得的 DNS/主机名遵循如下模式：
	// pod-specific-string.serviceName.default.svc.cluster.local，
	// 其中 "pod-specific-string" 由 StatefulSet 控制器管理。
	// +optional
	ServiceName string

	// PodManagementPolicy 控制在初次扩容、替换节点上的 Pod 或缩容时如何创建 Pod。
	// 默认策略是 `OrderedReady`：Pod 按序号递增顺序创建（先 pod-0，再 pod-1，依此类推），
	// 控制器会等待每个 Pod 就绪后才继续。缩容时按相反顺序删除 Pod。
	// 另一种策略是 `Parallel`：并行创建 Pod 以尽快达到期望规模且不等待，
	// 缩容时则一次性删除所有 Pod。
	// +optional
	PodManagementPolicy PodManagementPolicyType

	// updateStrategy 表示当 Template 发生变更时，用于更新
	// StatefulSet 中 Pod 的 StatefulSetUpdateStrategy。
	UpdateStrategy StatefulSetUpdateStrategy

	// revisionHistoryLimit 是 StatefulSet 修订历史中保留的最大修订数。
	// 修订历史由所有未被当前应用的 StatefulSetSpec 版本表示的修订组成。
	// 默认值为 10。
	RevisionHistoryLimit *int32

	// 新建 Pod 必须保持 Ready 状态的最短秒数。
	// 其任何容器都不得崩溃，才能被认为可用。
	// 默认为 0，即 Pod 一旦 Ready 就被认为可用。
	// +optional
	MinReadySeconds int32

	// persistentVolumeClaimRetentionPolicy 描述由 volumeClaimTemplates 创建的
	// 持久卷声明（PVC）的生命周期。默认情况下，所有持久卷声明按需创建，
	// 并一直保留到手动删除。该策略允许改变此生命周期，例如在其 StatefulSet
	// 被删除时、或其 Pod 被缩容时删除相应的持久卷声明。
	// +optional
	PersistentVolumeClaimRetentionPolicy *StatefulSetPersistentVolumeClaimRetentionPolicy

	// ordinals 控制 StatefulSet 中副本索引的编号方式。
	// 默认行为是首个副本索引为 "0"，之后每增加一个副本索引加一。
	// +optional
	Ordinals *StatefulSetOrdinals
}

// StatefulSetStatus 表示 StatefulSet 的当前状态。
type StatefulSetStatus struct {
	// observedGeneration 是该 StatefulSet 最近一次观测到的 generation，对应 StatefulSet 的
	// generation，由 API Server 在变更时更新。
	// +optional
	ObservedGeneration *int64

	// replicas 是 StatefulSet 控制器创建的 Pod 数。
	Replicas int32

	// readyReplicas 是 StatefulSet 控制器创建的、处于 Ready 条件的 Pod 数。
	ReadyReplicas int32

	// currentReplicas 是 StatefulSet 控制器依据 currentRevision 所指版本
	// 创建的 Pod 数。
	CurrentReplicas int32

	// updatedReplicas 是 StatefulSet 控制器依据 updateRevision 所指明的
	// StatefulSet 版本创建的 Pod 数量。
	UpdatedReplicas int32

	// currentRevision 如果非空，表示用于生成序列 [0,currentReplicas) 中
	// Pod 的 StatefulSet 版本。
	CurrentRevision string

	// updateRevision 非空时，表示用于生成序号区间 [replicas-updatedReplicas,replicas) 内 Pod 的 StatefulSet 版本。
	UpdateRevision string

	// collisionCount 是 StatefulSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32

	// 表示对 statefulset 当前状态的最新可用观测。
	Conditions []StatefulSetCondition

	// 该 StatefulSet 面向的可用 Pod（就绪至少 minReadySeconds）总数。
	// +optional
	AvailableReplicas int32
}

// StatefulSetConditionType 描述 StatefulSet 的条件类型。
type StatefulSetConditionType string

const (
	// StatefulSetProgressing 表示 StatefulSet 正在更新中。
	// 该条件为 alpha 类型，需要启用 StatefulSetRecreateStrategy feature gate。
	StatefulSetProgressing StatefulSetConditionType = "Progressing"
)

// TODO: 补充 Statefulset 的有效条件类型。

// StatefulSetCondition 描述某个时刻 statefulset 的状态。
type StatefulSetCondition struct {
	// statefulset 条件的类型。
	Type StatefulSetConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	Status api.ConditionStatus
	// 该条件最近一次更新的时间。
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	Reason string
	// 用自然语言描述状态变化的详细信息。
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StatefulSetList 是 StatefulSet 的集合。
type StatefulSetList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []StatefulSet
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ControllerRevision 实现状态数据的不可变快照。客户端负责序列化与反序列化
// 包含其内部状态的对象。
// ControllerRevision 一旦成功创建，就不能再更新。
// API Server 会使所有试图修改 Data 字段的请求校验失败。
// 不过，ControllerRevision 可以被删除。
type ControllerRevision struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// Data 是表示该状态的对象。
	Data runtime.RawExtension

	// revision 表示 Data 所代表状态的修订号。
	Revision int64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ControllerRevisionList 是包含 ControllerRevision 对象列表的资源。
type ControllerRevisionList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 ControllerRevision 对象的列表。
	Items []ControllerRevision
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Deployment 为 Pod 和 ReplicaSet 提供声明式更新。
type Deployment struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Deployment 期望行为的规格定义。
	// +optional
	Spec DeploymentSpec

	// Deployment 最近一次观测到的状态。
	// +optional
	Status DeploymentStatus
}

// DeploymentSpec 指定 Deployment 的状态。
type DeploymentSpec struct {
	// 期望的 Pod 数量。
	Replicas int32

	// Pod 的标签选择器。被其选中的现有 ReplicaSet 中的 Pod
	// 将是本次 Deployment 所影响的对象。
	// +optional
	Selector *metav1.LabelSelector

	// Template 描述将被创建的 Pod。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	Template api.PodTemplateSpec

	// 用新 Pod 替换现有 Pod 时采用的部署策略。
	// +optional
	Strategy DeploymentStrategy

	// 新建 Pod 必须保持 Ready 状态的最短秒数。
	// 且没有任何容器崩溃，才会被视为可用。
	// 默认为 0，即 Pod 一旦 Ready 就被认为可用。
	// +optional
	MinReadySeconds int32

	// 保留的旧 ReplicaSet 数量，以便回滚。
	// 使用指针类型，以区分显式设置为 0 与未设置。
	// 默认设置为 int32 最大值（即 2147483647），意味着"保留所有旧 ReplicaSet"。
	// +optional
	RevisionHistoryLimit *int32

	// 表示 Deployment 已暂停，不会被 Deployment 控制器处理。
	// +optional
	Paused bool

	// 已弃用。
	// 本 Deployment 正在回滚到的配置。回滚完成后会被清除。
	// +optional
	RollbackTo *RollbackConfig

	// Deployment 在被视为失败之前能够取得进展的最长时间（秒）。
	// Deployment 控制器会继续处理失败的 Deployment，并在 Deployment 状态中
	// 呈现一个带 ProgressDeadlineExceeded 原因的条件。注意，在 Deployment
	// 暂停期间不会估算进展。默认设置为 int32 最大值（即 2147483647），
	// 表示“无截止时间”。
	// +optional
	ProgressDeadlineSeconds *int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeploymentRollback 保存回滚 Deployment 所需的信息。
// 已弃用。
type DeploymentRollback struct {
	metav1.TypeMeta
	// 必填：必须与某个 Deployment 的 Name 一致。
	Name string
	// 要更新到 Deployment 上的注解。
	// +optional
	UpdatedAnnotations map[string]string
	// 本次 Deployment 回滚的配置。
	RollbackTo RollbackConfig
}

// RollbackConfig 描述要回滚到的某个修订版本的状态。
// 已弃用。
type RollbackConfig struct {
	// 要回滚到的修订号。若设置为 0，则回滚到上一个修订。
	// +optional
	Revision int64
}

const (
	// DefaultDeploymentUniqueLabelKey 是添加到现有 RC（以及添加到其 Pod 上的标签键）
	// 上的选择器的默认键，用于防止现有 RC 选择新的 Pod
	// （以及新 RC 选择旧的 Pod）。
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy 保存 Deployment 的更新策略与滚动更新行为的相关信息。
type DeploymentStrategy struct {
	// Deployment 的类型。可取 "Recreate" 或 "RollingUpdate"。默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType

	// 滚动更新配置参数。仅当 DeploymentStrategyType = RollingUpdate 时存在。
	//---
	// TODO: 按我们最终确定的 oneOf 约定更新此处。
	// +optional
	RollingUpdate *RollingUpdateDeployment
}

// DeploymentStrategyType 定义 Deployment 的更新策略类型。
type DeploymentStrategyType string

const (
	// RecreateDeploymentStrategyType 在创建新 Pod 前杀掉所有现有 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// RollingUpdateDeploymentStrategyType 使用滚动更新以新 RC 替换旧 RC，
	// 即逐步缩容旧 RC 并扩容新 RC。
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// RollingUpdateDeployment 是控制滚动更新期望行为的规格。
type RollingUpdateDeployment struct {
	// 更新期间允许不可用的 Pod 最大数量。
	// 值可以是绝对数量（如 5）或更新开始时 Pod 总数的百分比（如 10%）。
	// 绝对数量由百分比向下取整计算得出。
	// 若 MaxSurge 为 0，则此值不能为 0。
	// 默认使用固定值 1。
	// 示例：设为 30% 时，滚动更新一开始旧的 RC 就可以立即缩容 30%。
	// 一旦新 Pod 就绪，旧 RC 可进一步缩容，接着扩容新 RC，
	// 确保更新期间始终至少有 70% 的原始 Pod 可用。
	// +optional
	MaxUnavailable intstr.IntOrString

	// 可以调度到超出原始 Pod 数量的最大 Pod 数。
	// 值可以是绝对数量（如 5）或更新开始时 Pod 总数的百分比（如 10%）。
	// 若 MaxUnavailable 为 0，则此值不能为 0。
	// 绝对数量由百分比向上取整计算得出。
	// 默认使用固定值 1。
	// 示例：设为 30% 时，滚动更新一开始，新 RC 就可以立即扩容 30%。
	// 一旦旧 Pod 被杀掉，新 RC 可进一步扩容，
	// 确保更新期间任意时刻运行的 Pod 总数最多为原始 Pod 数的 130%。
	// +optional
	MaxSurge intstr.IntOrString
}

// DeploymentStatus 保存 Deployment 观测状态的相关信息。
type DeploymentStatus struct {
	// 部署控制器观测到的 generation。
	// +optional
	ObservedGeneration int64

	// 该 Deployment 关联的未终止 Pod 总数（标签与选择器匹配）。
	// +optional
	Replicas int32

	// 该 Deployment 关联且具有期望模板规格的未终止 Pod 总数。
	// +optional
	UpdatedReplicas int32

	// 该 Deployment 关联且处于 Ready 条件的未终止 Pod 总数。
	// +optional
	ReadyReplicas int32

	// 该 Deployment 关联的可用未终止 Pod 总数（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32

	// 该 Deployment 所面向的不可用 Pod 总数。这是该 Deployment 达到 100% 可用容量
	// 仍所需的 Pod 总数。它们可能是运行中但尚未可用的 Pod，也可能是尚未创建的 Pod。
	// +optional
	UnavailableReplicas int32

	// 本 Deployment 所针对的终止中 Pod 总数。终止中 Pod 的 .metadata.deletionTimestamp 非空，
	// 且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要开启 DeploymentReplicaSetTerminatingReplicas 特性（默认开启）。
	// +optional
	TerminatingReplicas *int32

	// 对 Deployment 当前状态的最新可用观测。
	Conditions []DeploymentCondition

	// Deployment 的哈希冲突计数。Deployment 控制器在需要为最新的 ReplicaSet
	// 生成名称时，用该字段作为避免冲突的机制。
	// +optional
	CollisionCount *int32
}

// DeploymentConditionType 定义 Deployment 的条件类型。
type DeploymentConditionType string

// 以下是 deployment 的有效条件。
const (
	// Available 表示 Deployment 可用，即至少所需的最小可用副本数已运行至少 minReadySeconds。
	DeploymentAvailable DeploymentConditionType = "Available"
	// Progressing 表示 Deployment 正在推进。当创建或收养新的 ReplicaSet、
	// 扩容新 Pod 或缩容旧 Pod 时，视为 Deployment 取得了进展。
	// 对于暂停的 Deployment，或未指定 progressDeadlineSeconds 时，
	// 不估算进展。
	DeploymentProgressing DeploymentConditionType = "Progressing"
	// ReplicaFailure 在 Deployment 的某个 Pod 创建或删除失败时添加。
	DeploymentReplicaFailure DeploymentConditionType = "ReplicaFailure"
)

// DeploymentCondition 描述 Deployment 在某一时刻的状态。
type DeploymentCondition struct {
	// Deployment 条件的类型。
	Type DeploymentConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	Status api.ConditionStatus
	// 该条件最近一次更新的时间。
	LastUpdateTime metav1.Time
	// 条件最近一次从一种状态变为另一种状态的时间。
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	Reason string
	// 用自然语言描述状态变化的详细信息。
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeploymentList 定义多个 Deployment。
type DeploymentList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 Deployment 的列表。
	Items []Deployment
}

// DaemonSetUpdateStrategy 定义 DaemonSet 的更新策略。
type DaemonSetUpdateStrategy struct {
	// DaemonSet 更新的类型。可取 "RollingUpdate" 或 "OnDelete"。
	// +optional
	Type DaemonSetUpdateStrategyType

	// 滚动更新配置参数。仅当 type = "RollingUpdate" 时存在。
	//---
	// TODO: 按我们最终确定的 oneOf 约定更新此处。
	// 与 Deployment 的 `strategy.rollingUpdate` 相同。
	// 参见 https://github.com/kubernetes/kubernetes/issues/35345
	// +optional
	RollingUpdate *RollingUpdateDaemonSet
}

// DaemonSetUpdateStrategyType 是 DaemonSet 更新所采用的策略类型。
type DaemonSetUpdateStrategyType string

const (
	// RollingUpdateDaemonSetStrategyType 使用滚动更新以新 daemon 替换旧 daemon，
	// 即在每个节点上逐一替换。
	RollingUpdateDaemonSetStrategyType DaemonSetUpdateStrategyType = "RollingUpdate"

	// OnDeleteDaemonSetStrategyType 仅在旧 daemon 被杀掉后才替换它。
	OnDeleteDaemonSetStrategyType DaemonSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateDaemonSet 是控制 DaemonSet 滚动更新期望行为的规格。
type RollingUpdateDaemonSet struct {
	// 更新期间允许不可用的 DaemonSet Pod 最大数量。
	// 值可以是绝对数量（如 5）或更新开始时 DaemonSet Pod 总数的百分比（如 10%）。
	// 绝对数量由百分比向上取整计算得出。
	// 若 MaxSurge 为 0，则此值不能为 0。
	// 默认值为 1。
	// 示例：设为 30% 时，最多允许 30% 的节点
	// （即 status.desiredNumberScheduled）处于不可用状态。
	// 任何给定时刻，这些节点上的 Pod 都可以因更新而停止。更新开始时，
	// 最多停止 30% 的 DaemonSet Pod，然后在原位置启动新 Pod。
	// 新 Pod 可用后，再继续处理其他 DaemonSet Pod，
	// 从而确保更新期间始终至少有 70% 的原始 DaemonSet Pod 可用。
	// +optional
	MaxUnavailable intstr.IntOrString

	// 更新期间，允许在已有可用 DaemonSet Pod 的节点上创建更新后 DaemonSet Pod 的最大节点数。
	// 值可以是绝对数量（如 5）或期望 Pod 数的百分比（如 10%）。
	// 若 MaxUnavailable 为 0，则此值不能为 0。
	// 绝对数量由百分比向上取整计算，至少为 1。
	// 默认值为 0。
	// 示例：设为 30% 时，30% 拥有可用 daemon Pod 的节点可以在旧 Pod 被标记
	// 删除之前先创建新 Pod。更新开始时在 30% 的节点上启动新 Pod。一旦更新后
	// Pod 可用（至少就绪 minReadySeconds），该节点上的旧 DaemonSet Pod 就被标记删除。
	// 若旧 Pod 因任何原因变得不可用（Ready 变为 false、被驱逐或被排空），
	// 会立即在该节点上创建更新后 Pod，且不受 surge 限制。
	// 允许 surge 意味着：若就绪检查失败，daemonset 在某节点上消耗的资源
	// 可能翻倍，因此资源密集型的 daemonset 应考虑到中断期间可能引发驱逐。
	// +optional
	MaxSurge intstr.IntOrString
}

// DaemonSetSpec 是 daemon set 的 spec。
type DaemonSetSpec struct {
	// 该 daemon set 所管理的 Pod 的标签查询。
	// 必须匹配才能被控制。
	// 若为空，默认使用 Pod 模板上的标签。
	// 更多信息参见: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// 描述将被创建的 Pod 的对象。
	// DaemonSet 会在每个匹配模板 node selector 的节点上
	// （若未指定 node selector，则在每个节点上）
	// 恰好创建一个此 Pod 的副本。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// 更多信息参见: https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	Template api.PodTemplateSpec

	// 用新 Pod 替换现有 DaemonSet Pod 的更新策略。
	// +optional
	UpdateStrategy DaemonSetUpdateStrategy

	// 新创建的 DaemonSet Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32

	// 已弃用。
	// 表示模板某个特定 generation 的序号。由系统填充。只能在创建时设置。
	// +optional
	TemplateGeneration int64

	// 保留的旧历史记录数量，以便回滚。
	// 使用指针类型，以区分显式设置为 0 与未设置。
	// 默认为 10。
	// +optional
	RevisionHistoryLimit *int32
}

// DaemonSetStatus 表示 daemon set 的当前状态。
type DaemonSetStatus struct {
	// 运行至少 1 个 daemon Pod 且本应运行该 daemon Pod 的节点数。
	CurrentNumberScheduled int32

	// 正在运行守护进程 Pod 但本不应运行该守护进程 Pod 的节点数。
	NumberMisscheduled int32

	// 本应运行 daemon Pod 的节点总数（包括正确运行 daemon Pod 的节点）。
	DesiredNumberScheduled int32

	// 应当运行 daemon Pod 且有一个或多个 daemon Pod 正在运行并就绪的节点数。
	NumberReady int32

	// daemon set 控制器最近观察到的 generation。
	// +optional
	ObservedGeneration int64

	// 正在运行更新后 daemon Pod 的节点总数。
	// +optional
	UpdatedNumberScheduled int32

	// 应当运行该 daemon Pod、且有一个或多个该 daemon Pod 正在运行并就绪
	// （至少就绪 spec.minReadySeconds 时长）的节点总数。
	// +optional
	NumberAvailable int32

	// 应当运行该 daemon Pod、但没有一个该 daemon Pod 正在运行并就绪
	// （至少就绪 spec.minReadySeconds 时长）的节点总数。
	// +optional
	NumberUnavailable int32

	// DaemonSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32

	// 表示对 DaemonSet 当前状态的最新可用观测。
	Conditions []DaemonSetCondition
}

// DaemonSetConditionType 定义 DaemonSet 的条件类型。
type DaemonSetConditionType string

// TODO: 补充 DaemonSet 的有效条件类型。

// DaemonSetCondition 描述某个时刻 DaemonSet 的状态。
type DaemonSetCondition struct {
	// DaemonSet 条件的类型。
	Type DaemonSetConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	Status api.ConditionStatus
	// 条件最近一次从一种状态变为另一种状态的时间。
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	Reason string
	// 用自然语言描述状态变化的详细信息。
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaemonSet 表示 daemon set 的配置。
type DaemonSet struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 该 daemon set 的期望行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec DaemonSetSpec

	// 本 daemon set 的当前状态。这些数据可能在一段时间内处于过期状态。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status DaemonSetStatus
}

const (
	// DaemonSetTemplateGenerationKey 是在 DaemonSet 模板更新期间，
	// 添加到 daemon set Pod 上用于区分新旧 Pod 模板的标签键。
	// DEPRECATED: 请改用 DefaultDaemonSetUniqueLabelKey。
	DaemonSetTemplateGenerationKey string = "pod-template-generation"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaemonSetList 是 daemon set 的集合。
type DaemonSetList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// daemon set 的列表。
	Items []DaemonSet
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
type ReplicaSet struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 ReplicaSet 期望行为的规格。
	// +optional
	Spec ReplicaSetSpec

	// Status 是本 ReplicaSet 的当前状态。这些数据可能在
	// 一段时间内处于过期状态。
	// +optional
	Status ReplicaSetStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicaSetList 是 ReplicaSet 的集合。
type ReplicaSetList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []ReplicaSet
}

// ReplicaSetSpec 是 ReplicaSet 的 spec。
// 作为 ReplicaSet 的内部表示，必须设置 Template。
type ReplicaSetSpec struct {
	// Replicas 是期望的副本数量。
	Replicas int32

	// 新建 Pod 必须保持 Ready 状态的最短秒数。
	// 且没有任何容器崩溃，才会被视为可用。
	// 默认为 0，即 Pod 一旦 Ready 就被认为可用。
	// +optional
	MinReadySeconds int32

	// Selector 是对 Pod 的标签查询，用于匹配副本数量。
	// 必须匹配才能被控制。
	// 若为空，默认使用 Pod 模板上的标签。
	// 更多信息参见: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// Template 是描述待创建 Pod 的对象；当检测到副本不足时创建。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +optional
	Template api.PodTemplateSpec
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近一次观测到的非终止中 Pod 数量。
	// 更多信息参见: https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Replicas int32

	// label 匹配该 replicaset Pod 模板 label 的非终止中 Pod 数。
	// +optional
	FullyLabeledReplicas int32

	// 该 ReplicaSet 所针对的、处于 Ready Condition 的非终止中 Pod 数。
	// +optional
	ReadyReplicas int32

	// 该副本集可用的非终止中 Pod 数量（就绪至少 minReadySeconds）。
	// +optional
	AvailableReplicas int32

	// 本 replica set 的终止中 Pod 数。终止中 Pod 的 .metadata.deletionTimestamp 非空，
	// 且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要开启 DeploymentReplicaSetTerminatingReplicas 特性（默认开启）。
	// +optional
	TerminatingReplicas *int32

	// ObservedGeneration 反映最近观察到的 ReplicaSet 的 generation。
	// +optional
	ObservedGeneration int64

	// 对 replica set 当前状态的最新可用观测。
	// +optional
	Conditions []ReplicaSetCondition
}

// ReplicaSetConditionType 是 replica set 的条件类型。
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
	Type ReplicaSetConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	Status api.ConditionStatus
	// 该条件最近一次从一种状态变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	// +optional
	Reason string
	// 用自然语言描述状态变化的详细信息。
	// +optional
	Message string
}
