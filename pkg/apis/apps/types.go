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
// Identities are defined as:
// - 网络：单一稳定的 DNS 和主机名。
// - 存储：按需提供任意数量的 VolumeClaim。
//
// The StatefulSet guarantees that a given network identity will always
// map to the same storage identity.
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
	// ParallelPodManagement will create and delete pods as soon as the stateful set
	// replica count is changed, and will not wait for pods to be ready or complete
	// termination.
	ParallelPodManagement PodManagementPolicyType = "Parallel"
)

// StatefulSetUpdateStrategy indicates the strategy that the StatefulSet
// controller will use to perform updates. It includes any additional parameters
// necessary to perform the update for the indicated strategy.
type StatefulSetUpdateStrategy struct {
	// Type indicates the type of the StatefulSetUpdateStrategy.
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
	// Absolute number is calculated from percentage by rounding up. This can not be 0.
	// Defaults to 1. This field is beta-level and is enabled by default. The field applies to all pods in the range 0 to
	// Replicas-1. That means if there is any unavailable pod in the range 0 to Replicas-1, it
	// will be counted towards MaxUnavailable.
	// This setting might not be effective for the OrderedReady podManagementPolicy. That policy ensures pods are created and become ready one at a time.
	//
	// +featureGate=MaxUnavailableStatefulSet
	// +optional
	MaxUnavailable *intstr.IntOrString
}

// PersistentVolumeClaimRetentionPolicyType is a string enumeration of the policies that will determine
// when volumes from the VolumeClaimTemplates will be deleted when the controlling StatefulSet is
// deleted or scaled down.
type PersistentVolumeClaimRetentionPolicyType string

const (
	// RetainPersistentVolumeClaimRetentionPolicyType is the default
	// PersistentVolumeClaimRetentionPolicy and specifies that
	// 与 StatefulSet VolumeClaimTemplates 关联的 PersistentVolumeClaims。
	// will not be deleted.
	RetainPersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Retain"
	// DeletePersistentVolumeClaimRetentionPolicyType specifies that
	// 与 StatefulSet VolumeClaimTemplates 关联的 PersistentVolumeClaims。
	// will be deleted in the scenario specified in
	// StatefulSetPersistentVolumeClaimPolicy.
	DeletePersistentVolumeClaimRetentionPolicyType PersistentVolumeClaimRetentionPolicyType = "Delete"
)

// StatefulSetPersistentVolumeClaimRetentionPolicy describes the policy used for PVCs
// StatefulSet VolumeClaimTemplates 创建的 PVC 使用的保留策略。
type StatefulSetPersistentVolumeClaimRetentionPolicy struct {
	// WhenDeleted specifies what happens to PVCs created from StatefulSet
	// VolumeClaimTemplates when the StatefulSet is deleted. The default policy
	// of `Retain` causes PVCs to not be affected by StatefulSet deletion. The
	// `Delete` policy causes those PVCs to be deleted.
	WhenDeleted PersistentVolumeClaimRetentionPolicyType
	// WhenScaled specifies what happens to PVCs created from StatefulSet
	// VolumeClaimTemplates when the StatefulSet is scaled down. The default
	// policy of `Retain` causes PVCs to not be affected by a scaledown. The
	// `Delete` policy causes the associated PVCs for any excess pods above
	// the replica count to be deleted.
	WhenScaled PersistentVolumeClaimRetentionPolicyType
}

// StatefulSetOrdinals describes the policy used for replica ordinal assignment
// in this StatefulSet.
type StatefulSetOrdinals struct {
	// start 表示第一个副本的起始序号。可用于从其他序号（例如从 1 开始）为副本编号，
	// 以替代默认的从 0 开始命名，也可用于协调副本从一个 StatefulSet 逐步迁移到另一个。
	// 如果设置，副本序号范围为：
	// 如果未设置，默认为 0，副本序号范围为：
	// [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas).
	// [0, .spec.replicas).
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
	// If empty, defaulted to labels on the pod template.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// Template 是描述待创建 Pod 的对象；当
	// insufficient replicas are detected. Each pod stamped out by the StatefulSet
	// will fulfill this Template, but have a unique identity from the rest
	// of the StatefulSet. Each pod will be named with the format
	// <statefulsetname>-<podindex>. For example, a pod in a StatefulSet named
	// "web" with index number "3" would be named "web-3".
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	Template api.PodTemplateSpec

	// VolumeClaimTemplates is a list of claims that pods are allowed to reference.
	// The StatefulSet controller is responsible for mapping network identities to
	// claims in a way that maintains the identity of a pod. Every claim in
	// this list must have at least one matching (by name) volumeMount in one
	// container in the template. A claim in this list takes precedence over
	// any volumes in the template, with the same name.
	// TODO: Define the behavior if a claim already exists with the same name.
	// +optional
	VolumeClaimTemplates []api.PersistentVolumeClaim

	// ServiceName is the name of the service that governs this StatefulSet.
	// This service must exist before the StatefulSet, and is responsible for
	// the network identity of the set. Pods get DNS/hostnames that follow the
	// pattern: pod-specific-string.serviceName.default.svc.cluster.local
	// where "pod-specific-string" is managed by the StatefulSet controller.
	// +optional
	ServiceName string

	// PodManagementPolicy controls how pods are created during initial scale up,
	// when replacing pods on nodes, or when scaling down. The default policy is
	// `OrderedReady`, where pods are created in increasing order (pod-0, then
	// pod-1, etc) and the controller will wait until each pod is ready before
	// continuing. When scaling down, the pods are removed in the opposite order.
	// The alternative policy is `Parallel` which will create pods in parallel
	// to match the desired scale without waiting, and on scale down will delete
	// all pods at once.
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
	// without any of its container crashing for it to be considered available.
	// 默认为 0，即 Pod 一旦 Ready 就被认为可用。
	// +optional
	MinReadySeconds int32

	// persistentVolumeClaimRetentionPolicy describes the lifecycle of persistent
	// volume claims created from volumeClaimTemplates. By default, all persistent
	// volume claims are created as needed and retained until manually deleted. This
	// policy allows the lifecycle to be altered, for example by deleting persistent
	// volume claims when their stateful set is deleted, or when their pod is scaled
	// down.
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

	// readyReplicas is the number of Pods created by the StatefulSet controller that have a Ready Condition.
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
	// [replicas-updatedReplicas,replicas)
	UpdateRevision string

	// collisionCount 是 StatefulSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32

	// 表示对 statefulset 当前状态的最新可用观测。
	Conditions []StatefulSetCondition

	// Total number of available pods (ready for at least minReadySeconds) targeted by this statefulset.
	// +optional
	AvailableReplicas int32
}

// StatefulSetConditionType describes the condition types of StatefulSets.
type StatefulSetConditionType string

const (
	// StatefulSetProgressing 表示 StatefulSet 正在更新中。
	// 该条件为 alpha 类型，需要启用 StatefulSetRecreateStrategy feature gate。
	StatefulSetProgressing StatefulSetConditionType = "Progressing"
)

// TODO: Add valid condition types for Statefulsets.

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

// ControllerRevision implements an immutable snapshot of state data. Clients
// are responsible for serializing and deserializing the objects that contain
// their internal state.
// Once a ControllerRevision has been successfully created, it can not be updated.
// The API Server will fail validation of all requests that attempt to mutate
// the Data field. ControllerRevisions may, however, be deleted.
type ControllerRevision struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// Data is the Object representing the state.
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

	// Items is the list of ControllerRevision objects.
	Items []ControllerRevision
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Deployment provides declarative updates for Pods and ReplicaSets.
type Deployment struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Specification of the desired behavior of the Deployment.
	// +optional
	Spec DeploymentSpec

	// Most recently observed status of the Deployment.
	// +optional
	Status DeploymentStatus
}

// DeploymentSpec specifies the state of a Deployment.
type DeploymentSpec struct {
	// Number of desired pods.
	Replicas int32

	// Label selector for pods. Existing ReplicaSets whose pods are
	// selected by this will be the ones affected by this deployment.
	// +optional
	Selector *metav1.LabelSelector

	// Template describes the pods that will be created.
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

	// The number of old ReplicaSets to retain to allow rollback.
	// 使用指针类型，以区分显式设置为 0 与未设置。
	// This is set to the max value of int32 (i.e. 2147483647) by default, which means
	// "retaining all old ReplicaSets".
	// +optional
	RevisionHistoryLimit *int32

	// Indicates that the deployment is paused and will not be processed by the
	// deployment controller.
	// +optional
	Paused bool

	// 已弃用。
	// The config this deployment is rolling back to. Will be cleared after rollback is done.
	// +optional
	RollbackTo *RollbackConfig

	// The maximum time in seconds for a deployment to make progress before it
	// is considered to be failed. The deployment controller will continue to
	// process failed deployments and a condition with a ProgressDeadlineExceeded
	// reason will be surfaced in the deployment status. Note that progress will
	// not be estimated during the time a deployment is paused. This is set to
	// the max value of int32 (i.e. 2147483647) by default, which means "no deadline".
	// +optional
	ProgressDeadlineSeconds *int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeploymentRollback 保存回滚 Deployment 所需的信息。
// 已弃用。
type DeploymentRollback struct {
	metav1.TypeMeta
	// Required: This must match the Name of a deployment.
	Name string
	// 要更新到 Deployment 上的注解。
	// +optional
	UpdatedAnnotations map[string]string
	// The config of this deployment rollback.
	RollbackTo RollbackConfig
}

// RollbackConfig specifies the state of a revision to roll back to.
// 已弃用。
type RollbackConfig struct {
	// The revision to rollback to. If set to 0, rollback to the last revision.
	// +optional
	Revision int64
}

const (
	// DefaultDeploymentUniqueLabelKey is the default key of the selector that is added
	// to existing RCs (and label key that is added to its pods) to prevent the existing RCs
	// to select new pods (and old pods being select by new RC).
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy stores information about the strategy and rolling-update
// behavior of a deployment.
type DeploymentStrategy struct {
	// Deployment 的类型。可取 "Recreate" 或 "RollingUpdate"。默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType

	// Rolling update config params. Present only if DeploymentStrategyType =
	// RollingUpdate.
	//---
	// TODO: 按我们最终确定的 oneOf 约定更新此处，
	// to be.
	// +optional
	RollingUpdate *RollingUpdateDeployment
}

// DeploymentStrategyType defines strategies with a deployment.
type DeploymentStrategyType string

const (
	// RecreateDeploymentStrategyType 在创建新 Pod 前杀掉所有现有 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// RollingUpdateDeploymentStrategyType - Replace the old RCs by new one using rolling update i.e gradually scale down the old RCs and scale up the new one.
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// RollingUpdateDeployment is the spec to control the desired behavior of rolling update.
type RollingUpdateDeployment struct {
	// 更新期间允许不可用的 Pod 最大数量。
	// Value can be an absolute number (ex: 5) or a percentage of total pods at the start of update (ex: 10%).
	// Absolute number is calculated from percentage by rounding down.
	// This can not be 0 if MaxSurge is 0.
	// By default, a fixed value of 1 is used.
	// Example: when this is set to 30%, the old RC can be scaled down by 30%
	// immediately when the rolling update starts. Once new pods are ready, old RC
	// can be scaled down further, followed by scaling up the new RC, ensuring
	// that at least 70% of original number of pods are available at all times
	// during the update.
	// +optional
	MaxUnavailable intstr.IntOrString

	// The maximum number of pods that can be scheduled above the original number of
	// pods.
	// Value can be an absolute number (ex: 5) or a percentage of total pods at
	// the start of the update (ex: 10%). This can not be 0 if MaxUnavailable is 0.
	// Absolute number is calculated from percentage by rounding up.
	// By default, a value of 1 is used.
	// Example: when this is set to 30%, the new RC can be scaled up by 30%
	// immediately when the rolling update starts. Once old pods have been killed,
	// new RC can be scaled up further, ensuring that total number of pods running
	// at any time during the update is at most 130% of original pods.
	// +optional
	MaxSurge intstr.IntOrString
}

// DeploymentStatus holds information about the observed status of a deployment.
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

	// 本 Deployment 所针对的终止中 Pod 总数。终止中 Pod 的 .metadata.deletionTimestamp
	// .metadata.deletionTimestamp and have not yet reached the Failed or Succeeded .status.phase.
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

// DeploymentConditionType defines conditions of a deployment.
type DeploymentConditionType string

// 以下是 deployment 的有效条件。
const (
	// Available 表示 Deployment 可用，即至少所需的最小可用副本数已运行至少 minReadySeconds。
	DeploymentAvailable DeploymentConditionType = "Available"
	// Progressing means the deployment is progressing. Progress for a deployment is
	// considered when a new replica set is created or adopted, and when new pods scale
	// up or old pods scale down. Progress is not estimated for paused deployments or
	// when progressDeadlineSeconds is not specified.
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

// DeploymentList defines multiple deployments.
type DeploymentList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 Deployment 的列表。
	Items []Deployment
}

// DaemonSetUpdateStrategy defines a strategy to update a daemon set.
type DaemonSetUpdateStrategy struct {
	// Type of daemon set update. Can be "RollingUpdate" or "OnDelete".
	// +optional
	Type DaemonSetUpdateStrategyType

	// Rolling update config params. Present only if type = "RollingUpdate".
	//---
	// TODO: 按我们最终确定的 oneOf 约定更新此处，
	// to be. Same as Deployment `strategy.rollingUpdate`.
	// See https://github.com/kubernetes/kubernetes/issues/35345
	// +optional
	RollingUpdate *RollingUpdateDaemonSet
}

// DaemonSetUpdateStrategyType is a strategy according to which a daemon set
// gets updated.
type DaemonSetUpdateStrategyType string

const (
	// RollingUpdateDaemonSetStrategyType - Replace the old daemons by new ones using rolling update i.e replace them on each node one after the other.
	RollingUpdateDaemonSetStrategyType DaemonSetUpdateStrategyType = "RollingUpdate"

	// OnDeleteDaemonSetStrategyType 仅在旧 daemon 被杀掉后才替换它。
	OnDeleteDaemonSetStrategyType DaemonSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateDaemonSet is the spec to control the desired behavior of daemon set rolling update.
type RollingUpdateDaemonSet struct {
	// The maximum number of DaemonSet pods that can be unavailable during the
	// update. Value can be an absolute number (ex: 5) or a percentage of total
	// number of DaemonSet pods at the start of the update (ex: 10%). Absolute
	// number is calculated from percentage by rounding up.
	// This cannot be 0 if MaxSurge is 0
	// Default value is 1.
	// 示例：设为 30% 时，最多允许 30% 的节点
	// （即 status.desiredNumberScheduled）处于不可用状态。
	// can have their pods stopped for an update at any given time. The update
	// starts by stopping at most 30% of those DaemonSet pods and then brings
	// up new DaemonSet pods in their place. Once the new pods are available,
	// it then proceeds onto other DaemonSet pods, thus ensuring that at least
	// 70% of original number of DaemonSet pods are available at all times during
	// the update.
	// +optional
	MaxUnavailable intstr.IntOrString

	// The maximum number of nodes with an existing available DaemonSet pod that
	// can have an updated DaemonSet pod during during an update.
	// 值可以是绝对数量（如 5）或期望 Pod 数的百分比（如 10%）。
	// This can not be 0 if MaxUnavailable is 0.
	// Absolute number is calculated from percentage by rounding up to a minimum of 1.
	// Default value is 0.
	// 示例：设为 30% 时，最多允许 30% 的节点
	// （即 status.desiredNumberScheduled）处于不可用状态。
	// can have their a new pod created before the old pod is marked as deleted.
	// The update starts by launching new pods on 30% of nodes. Once an updated
	// pod is available (Ready for at least minReadySeconds) the old DaemonSet pod
	// on that node is marked deleted. If the old pod becomes unavailable for any
	// reason (Ready transitions to false, is evicted, or is drained) an updated
	// pod is immediately created on that node without considering surge limits.
	// Allowing surge implies the possibility that the resources consumed by the
	// daemonset on any given node can double if the readiness check fails, and
	// so resource intensive daemonsets should take into account that they may
	// cause evictions during disruption.
	// +optional
	MaxSurge intstr.IntOrString
}

// DaemonSetSpec 是 daemon set 的 spec。
type DaemonSetSpec struct {
	// A label query over pods that are managed by the daemon set.
	// 必须匹配才能被控制。
	// If empty, defaulted to labels on Pod template.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// An object that describes the pod that will be created.
	// The DaemonSet will create exactly one copy of this pod on every node
	// that matches the template's node selector (or on every node if no node
	// selector is specified).
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	Template api.PodTemplateSpec

	// An update strategy to replace existing DaemonSet pods with new pods.
	// +optional
	UpdateStrategy DaemonSetUpdateStrategy

	// 新创建的 DaemonSet Pod 在被视为可用之前，必须处于就绪状态且容器均未崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32

	// 已弃用。
	// A sequence number representing a specific generation of the template.
	// Populated by the system. It can be set only during the creation.
	// +optional
	TemplateGeneration int64

	// The number of old history to retain to allow rollback.
	// 使用指针类型，以区分显式设置为 0 与未设置。
	// Defaults to 10.
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

	// The number of nodes that should be running the daemon pod and have one
	// or more of the daemon pod running and ready.
	NumberReady int32

	// daemon set 控制器最近观察到的 generation。
	// +optional
	ObservedGeneration int64

	// 正在运行更新后 daemon Pod 的节点总数。
	// +optional
	UpdatedNumberScheduled int32

	// 应当运行该
	// daemon pod and have one or more of the daemon pod running and
	// available (ready for at least spec.minReadySeconds)
	// +optional
	NumberAvailable int32

	// 应当运行该
	// daemon pod and have none of the daemon pod running and available
	// (ready for at least spec.minReadySeconds)
	// +optional
	NumberUnavailable int32

	// DaemonSet 的哈希冲突计数。当控制器需要为最新的
	// ControllerRevision 生成名称时，将该字段用作避免冲突的机制。
	// +optional
	CollisionCount *int32

	// 表示对 DaemonSet 当前状态的最新可用观测。
	Conditions []DaemonSetCondition
}

// DaemonSetConditionType defines a daemon set condition.
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

	// The desired behavior of this daemon set.
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec DaemonSetSpec

	// The current status of this daemon set. This data may be
	// 在一段时间内处于过期状态。
	// Populated by the system.
	// Read-only.
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status DaemonSetStatus
}

const (
	// DaemonSetTemplateGenerationKey is the key of the labels that is added
	// to daemon set pods to distinguish between old and new pod templates
	// during DaemonSet template update.
	// DEPRECATED: DefaultDaemonSetUniqueLabelKey is used instead.
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

	// Status is the current status of this ReplicaSet. This data may be
	// 在一段时间内处于过期状态。
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
// As the internal representation of a ReplicaSet, it must have
// a Template set.
type ReplicaSetSpec struct {
	// Replicas is the number of desired replicas.
	Replicas int32

	// 新建 Pod 必须保持 Ready 状态的最短秒数。
	// 且没有任何容器崩溃，才会被视为可用。
	// 默认为 0，即 Pod 一旦 Ready 就被认为可用。
	// +optional
	MinReadySeconds int32

	// Selector 是对 Pod 的标签查询，用于匹配副本数量。
	// 必须匹配才能被控制。
	// If empty, defaulted to labels on pod template.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector

	// Template 是描述待创建 Pod 的对象；当
	// insufficient replicas are detected.
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +optional
	Template api.PodTemplateSpec
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近一次观测到的非终止中 Pod 数量。
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
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

// ReplicaSetConditionType is a condition of a replica set.
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
	// The last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	// +optional
	Reason string
	// 用自然语言描述状态变化的详细信息。
	// +optional
	Message string
}
