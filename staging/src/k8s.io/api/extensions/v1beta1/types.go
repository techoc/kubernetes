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

package v1beta1

import (
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// 被伸缩对象的期望实例数。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=0
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// 实际观察到的被伸缩对象实例数。
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// selector 是对应与副本数的 Pod 的标签查询。更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	// +optional
	// +mapType=atomic
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`

	// 与副本数匹配的 Pod 的标签选择器。这是 map 型选择器和表达力更强的
	// set 型选择器的序列化形式，以免客户端自行做内省。字符串格式与
	// query-param 语法一致。若目标类型只支持 map 型选择器，则本字段和
	// map 型选择器字段都会被填充。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	TargetSelector string `json:"targetSelector,omitempty" protobuf:"bytes,3,opt,name=targetSelector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.2
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:isSubresource="/scale"

// Scale 表示对资源的伸缩请求。
type Scale struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 定义 scale 的行为。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec ScaleSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// scale 的当前状态。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。只读。
	// +optional
	Status ScaleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=Scale,result=Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=Scale,result=Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,Deployment

// 已废弃：本版本的 Deployment 已被 apps/v1beta2/Deployment 取代，详见发布说明。
// Deployment 支持对 Pod 和 ReplicaSet 的声明式更新。
// +k8s:supportsSubresource="/status"
type Deployment struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Deployment 期望行为的规格说明。
	// +optional
	Spec DeploymentSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近一次观察到的 Deployment 状态。
	// +optional
	Status DeploymentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeploymentSpec 是 Deployment 期望行为的规格说明。
type DeploymentSpec struct {
	// 期望的 Pod 数量。用指针以区分显式置零与未指定。默认为 1。
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// Pod 的标签选择器。Pod 被其选中的已有 ReplicaSet 会受本 Deployment 影响。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`

	// Template 描述将要创建的 Pod。
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`

	// 用新 Pod 替换旧 Pod 时采用的策略。
	// +optional
	// +patchStrategy=retainKeys
	Strategy DeploymentStrategy `json:"strategy,omitempty" patchStrategy:"retainKeys" protobuf:"bytes,4,opt,name=strategy"`

	// 新创建的 Pod 需要就绪且容器不崩溃的最小秒数，才会被视为可用。
	// 默认为 0（一旦就绪即视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,5,opt,name=minReadySeconds"`

	// 为支持回滚而保留的旧 ReplicaSet 数量。
	// 用指针以区分显式置零与未指定。
	// 默认为 int32 的最大值（即 2147483647），表示保留全部旧 ReplicaSet。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`

	// 表示 Deployment 已暂停，不会被 Deployment 控制器处理。
	// +optional
	Paused bool `json:"paused,omitempty" protobuf:"varint,7,opt,name=paused"`

	// 已废弃。本 Deployment 要回滚到的配置，回滚完成后会被清空。
	// +optional
	RollbackTo *RollbackConfig `json:"rollbackTo,omitempty" protobuf:"bytes,8,opt,name=rollbackTo"`

	// Deployment 在被视为失败前取得进展的最长秒数。Deployment 控制器仍会继续
	// 处理失败的 Deployment，并在状态中给出原因为 ProgressDeadlineExceeded 的
	// condition。暂停期间不估算进展。默认为 int32 的最大值（即 2147483647），
	// 表示没有 deadline。
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty" protobuf:"varint,9,opt,name=progressDeadlineSeconds"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16

// 已废弃。DeploymentRollback 存放回滚 Deployment 所需的信息。
type DeploymentRollback struct {
	metav1.TypeMeta `json:""`
	// 必填：必须与 Deployment 的 Name 一致。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 要更新到 Deployment 上的注解。
	// +optional
	UpdatedAnnotations map[string]string `json:"updatedAnnotations,omitempty" protobuf:"bytes,2,rep,name=updatedAnnotations"`
	// 本次回滚的配置。
	RollbackTo RollbackConfig `json:"rollbackTo" protobuf:"bytes,3,opt,name=rollbackTo"`
}

// RollbackConfig 已废弃。
type RollbackConfig struct {
	// 要回滚到的版本号。设为 0 表示回滚到上一个版本。
	// +optional
	Revision int64 `json:"revision,omitempty" protobuf:"varint,1,opt,name=revision"`
}

// Deployment 唯一标签键的默认值集合。
const (
	// DefaultDeploymentUniqueLabelKey 是添加到已有 RC 的选择器（以及其 Pod 上的标签键）的默认键，
	// 用于防止旧 RC 选中新 Pod，也防止新 RC 选中旧 Pod。
	DefaultDeploymentUniqueLabelKey string = "pod-template-hash"
)

// DeploymentStrategy 描述如何用新 Pod 替换旧 Pod。
type DeploymentStrategy struct {
	// Deployment 的类型。可为 "Recreate" 或 "RollingUpdate"，默认为 RollingUpdate。
	// +optional
	Type DeploymentStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=DeploymentStrategyType"`

	// 滚动更新的配置参数，仅当 DeploymentStrategyType 为 RollingUpdate 时存在。
	//---
	// TODO: 待 oneOf 约定确定后，按约定更新此处。
	// +optional
	RollingUpdate *RollingUpdateDeployment `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DeploymentStrategyType 表示 Deployment 的更新策略类型。
type DeploymentStrategyType string

// Deployment 更新策略类型常量。
const (
	// 先杀掉所有已有 Pod，再创建新 Pod。
	RecreateDeploymentStrategyType DeploymentStrategyType = "Recreate"

	// 通过滚动更新用新 RC 替换旧 RC，即逐步缩减旧 RC、扩容新 RC。
	RollingUpdateDeploymentStrategyType DeploymentStrategyType = "RollingUpdate"
)

// RollingUpdateDeployment 控制滚动更新的期望行为。
type RollingUpdateDeployment struct {
	// 更新期间最多不可用的 Pod 数。可取绝对值（如 5）或期望 Pod 数的百分比（如 10%）。
	// 绝对值由百分比向下取整得出。若 MaxSurge 为 0，则本值不能为 0。
	// 默认使用固定值 1。
	// 例如设为 30% 时，滚动更新一开始旧 RC 就可以立刻缩容到期望 Pod 数的 70%。
	// 新 Pod 就绪后可继续缩容旧 RC、扩容新 RC，确保更新过程中可用 Pod 总数
	// 始终不低于期望 Pod 数的 70%。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 可调度的 Pod 数最多可超出期望 Pod 数多少。
	// 可取绝对值（如 5）或期望 Pod 数的百分比（如 10%）。
	// 若 MaxUnavailable 为 0，则本值不能为 0。
	// 绝对值由百分比向上取整得出。默认值为 1。
	// 例如设为 30% 时，滚动更新一开始新 RC 就可以立刻扩容，使新旧 Pod 总数
	// 不超过期望 Pod 数的 130%。旧 Pod 被杀掉后可继续扩容新 RC，确保更新
	// 过程中任意时刻运行的 Pod 总数最多为期望 Pod 数的 130%。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DeploymentStatus 是最近一次观察到的 Deployment 状态。
type DeploymentStatus struct {
	// Deployment 控制器观察到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 本 Deployment 所针对的非终止态 Pod 总数（标签与选择器匹配）。
	// +optional
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`

	// 本 Deployment 所针对、且模板规格符合期望的非终止态 Pod 总数。
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty" protobuf:"varint,3,opt,name=updatedReplicas"`

	// 本 Deployment 所针对、处于 Ready 状态的非终止态 Pod 总数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,7,opt,name=readyReplicas"`

	// 本 Deployment 所针对的可用非终止态 Pod 总数（至少就绪 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,4,opt,name=availableReplicas"`

	// 本 Deployment 所针对的不可用 Pod 总数，即要达到 100% 可用容量还缺的 Pod 数。
	// 可能是仍在运行但尚未可用的 Pod，也可能是还未创建的 Pod。
	// +optional
	UnavailableReplicas int32 `json:"unavailableReplicas,omitempty" protobuf:"varint,5,opt,name=unavailableReplicas"`

	// 本 Deployment 所针对的终止中 Pod 总数。终止中 Pod 的 .metadata.deletionTimestamp
	// 非空，且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要开启 DeploymentReplicaSetTerminatingReplicas 特性（默认开启）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,9,opt,name=terminatingReplicas"`

	// 对 Deployment 当前状态的最新可用观测。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DeploymentCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`

	// Deployment 的哈希冲突计数。控制器创建新 ReplicaSet 命名时，用本字段规避名称冲突。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,8,opt,name=collisionCount"`
}

// DeploymentConditionType 表示 Deployment condition 的类型。
type DeploymentConditionType string

// Deployment 的有效 condition 类型。
const (
	// Available 表示 Deployment 可用，即至少有要求的最低副本数已运行至少 minReadySeconds。
	DeploymentAvailable DeploymentConditionType = "Available"
	// Progressing 表示 Deployment 正在推进。新建或接纳新 ReplicaSet、扩容新 Pod 或
	// 缩容旧 Pod 都算有进展。暂停的 Deployment 或未指定 progressDeadlineSeconds 时
	// 不估算进展。
	DeploymentProgressing DeploymentConditionType = "Progressing"
	// ReplicaFailure 表示 Deployment 中有 Pod 创建或删除失败。
	DeploymentReplicaFailure DeploymentConditionType = "ReplicaFailure"
)

// DeploymentCondition 描述 Deployment 在某一时刻的状态。
type DeploymentCondition struct {
	// Deployment condition 的类型。
	Type DeploymentConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DeploymentConditionType"`
	// condition 的状态，取值为 True、False、Unknown 之一。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 本 condition 最近一次更新的时间。
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty" protobuf:"bytes,6,opt,name=lastUpdateTime"`
	// condition 最近一次发生状态转换的时间。
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,7,opt,name=lastTransitionTime"`
	// 最近一次状态转换的原因。
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明状态转换的细节。
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DeploymentList

// DeploymentList 是 Deployment 列表。
type DeploymentList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Deployment 列表。
	Items []Deployment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// DaemonSetUpdateStrategy 表示 DaemonSet 控制器执行更新时采用的策略，
// 包含该策略所需的附加参数。
type DaemonSetUpdateStrategy struct {
	// DaemonSet 的更新类型。可为 "RollingUpdate" 或 "OnDelete"，默认为 OnDelete。
	// +optional
	Type DaemonSetUpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`

	// 滚动更新配置参数，仅当 type 为 "RollingUpdate" 时存在。
	//---
	// TODO: 待 oneOf 约定确定后，按约定更新此处。与 Deployment 的 `strategy.rollingUpdate` 一致。
	// 见 https://github.com/kubernetes/kubernetes/issues/35345
	// +optional
	RollingUpdate *RollingUpdateDaemonSet `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

// DaemonSetUpdateStrategyType 表示 DaemonSet 的更新策略类型。
type DaemonSetUpdateStrategyType string

// DaemonSet 更新策略类型常量。
const (
	// 通过滚动更新替换旧 daemon，即在各节点上逐个替换。
	RollingUpdateDaemonSetStrategyType DaemonSetUpdateStrategyType = "RollingUpdate"

	// 仅在旧 daemon 被删除后才替换它。
	OnDeleteDaemonSetStrategyType DaemonSetUpdateStrategyType = "OnDelete"
)

// RollingUpdateDaemonSet 控制 daemon set 滚动更新的期望行为。
type RollingUpdateDaemonSet struct {
	// 更新期间最多不可用的 DaemonSet Pod 数。可取绝对值（如 5），或占更新开始时
	// DaemonSet Pod 总数的百分比（如 10%）。绝对值由百分比向上取整得出。
	// 若 MaxSurge 为 0，则本值不能为 0。默认值为 1。
	// 例如设为 30% 时，任意时刻应运行该 daemon Pod 的节点总数（即
	// status.desiredNumberScheduled）中，最多 30% 的节点上的 Pod 可因更新而停止。
	// 更新先停止最多 30% 的 DaemonSet Pod，再在原处拉起新 Pod。新 Pod 可用后继续
	// 处理其余 Pod，从而在整个更新过程中保证原有 DaemonSet Pod 至少有 70% 可用。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`

	// 更新期间，已有可用 DaemonSet Pod 的节点上最多可同时存在多少个更新后的 Pod。
	// 可取绝对值（如 5）或期望 Pod 数的百分比（如 10%）。若 MaxUnavailable 为 0，
	// 则本值不能为 0。绝对值由百分比向上取整且最小为 1。默认值为 0。
	// 例如设为 30% 时，应运行该 daemon Pod 的节点总数（即
	// status.desiredNumberScheduled）中，最多 30% 的节点可在旧 Pod 标记为删除前
	// 先创建新 Pod。更新先在 30% 的节点上拉起新 Pod，新 Pod 可用（至少就绪
	// minReadySeconds）后，该节点上的旧 DaemonSet Pod 被标记为删除。若旧 Pod 因
	// 任何原因（Ready 转为 false、被驱逐或被排水）变为不可用，则立即在该节点创建
	// 更新后的 Pod，不受 surge 上限限制。允许 surge 意味着就绪检查失败时，某节点上
	// daemonset 消耗的资源可能翻倍，资源密集型 daemonset 应考虑这一点：中断期间
	// 可能触发驱逐。这是 alpha 字段，需要开启 DaemonSetUpdateSurge 特性门控。
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty" protobuf:"bytes,2,opt,name=maxSurge"`
}

// DaemonSetSpec 是 daemon set 的规格说明。
type DaemonSetSpec struct {
	// 对 daemon set 管理的 Pod 的标签查询。必须匹配才能被其控制。
	// 若为空，则默认取 Pod 模板上的标签。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,1,opt,name=selector"`

	// 描述将要创建的 Pod 的对象。DaemonSet 会在每个匹配模板节点选择器的节点上
	// （若未指定节点选择器则在所有节点上）恰好创建一个该 Pod 的副本。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,2,opt,name=template"`

	// 用新 Pod 替换现有 DaemonSet Pod 的更新策略。
	// +optional
	UpdateStrategy DaemonSetUpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,3,opt,name=updateStrategy"`

	// 新创建的 DaemonSet Pod 需要就绪且容器不崩溃的最小秒数，才会被视为可用。
	// 默认为 0（一旦就绪即视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// 已废弃。表示模板特定代际的序号，由系统填充，只能在创建时设置。
	// +optional
	TemplateGeneration int64 `json:"templateGeneration,omitempty" protobuf:"varint,5,opt,name=templateGeneration"`

	// 为支持回滚而保留的旧历史数量。用指针以区分显式置零与未指定。
	// 默认为 10。
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`
}

// DaemonSetStatus 表示 daemon set 的当前状态。
type DaemonSetStatus struct {
	// 运行着至少一个 daemon Pod、且本应运行该 daemon Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	CurrentNumberScheduled int32 `json:"currentNumberScheduled" protobuf:"varint,1,opt,name=currentNumberScheduled"`

	// 运行着 daemon Pod、但本不应运行该 daemon Pod 的节点数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	NumberMisscheduled int32 `json:"numberMisscheduled" protobuf:"varint,2,opt,name=numberMisscheduled"`

	// 本应运行 daemon Pod 的节点总数（包括正常运行的节点）。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
	DesiredNumberScheduled int32 `json:"desiredNumberScheduled" protobuf:"varint,3,opt,name=desiredNumberScheduled"`

	// 本应运行 daemon Pod、且有一个或多个 daemon Pod 处于 Ready 状态的节点数。
	NumberReady int32 `json:"numberReady" protobuf:"varint,4,opt,name=numberReady"`

	// DaemonSet 控制器观察到的最新 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,5,opt,name=observedGeneration"`

	// 正在运行更新后 daemon Pod 的节点总数。
	// +optional
	UpdatedNumberScheduled int32 `json:"updatedNumberScheduled,omitempty" protobuf:"varint,6,opt,name=updatedNumberScheduled"`

	// 本应运行 daemon Pod、且有一个或多个 daemon Pod 处于可用状态
	// （至少就绪 spec.minReadySeconds）的节点数。
	// +optional
	NumberAvailable int32 `json:"numberAvailable,omitempty" protobuf:"varint,7,opt,name=numberAvailable"`

	// 本应运行 daemon Pod、但没有任何 daemon Pod 处于可用状态
	// （至少就绪 spec.minReadySeconds）的节点数。
	// +optional
	NumberUnavailable int32 `json:"numberUnavailable,omitempty" protobuf:"varint,8,opt,name=numberUnavailable"`

	// DaemonSet 的哈希冲突计数。控制器创建新 ControllerRevision 命名时，用本字段规避名称冲突。
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty" protobuf:"varint,9,opt,name=collisionCount"`

	// 对 DaemonSet 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []DaemonSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,10,rep,name=conditions"`
}

// DaemonSetConditionType 表示 DaemonSet condition 的类型。
type DaemonSetConditionType string

// TODO: 补充 DaemonSet 的有效 condition 类型。

// DaemonSetCondition 描述 DaemonSet 在某一时刻的状态。
type DaemonSetCondition struct {
	// DaemonSet condition 的类型。
	Type DaemonSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=DaemonSetConditionType"`
	// condition 的状态，取值为 True、False、Unknown 之一。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// condition 最近一次发生状态转换的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 最近一次状态转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明状态转换的细节。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DaemonSet

// 已废弃：本版本的 DaemonSet 已被 apps/v1beta2/DaemonSet 取代，详见发布说明。
// DaemonSet 表示一个 daemon set 的配置。
// +k8s:supportsSubresource="/status"
type DaemonSet struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 本 daemon set 的期望行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec DaemonSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 本 daemon set 的当前状态。该数据可能有一定程度的滞后。
	// 由系统填充，只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status DaemonSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DaemonSet 唯一标签键常量。
const (
	// 已废弃：改用 DefaultDaemonSetUniqueLabelKey。
	// DaemonSetTemplateGenerationKey 是添加到 daemon set Pod 上的标签键，
	// 用于在 DaemonSet 模板更新时区分新旧 Pod 模板。
	DaemonSetTemplateGenerationKey string = "pod-template-generation"

	// DefaultDaemonSetUniqueLabelKey 是默认标签键，添加到已有 DaemonSet Pod 上，
	// 用于在 DaemonSet 模板更新时区分新旧 DaemonSet Pod。
	DefaultDaemonSetUniqueLabelKey = appsv1beta1.ControllerRevisionHashLabelKey
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,DaemonSetList

// DaemonSetList 是 daemon set 的集合。
type DaemonSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// daemon set 列表。
	Items []DaemonSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.14
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,Ingress

// Ingress 是一组规则，允许入站连接到达后端定义的端点。
// Ingress 可配置为服务提供外部可访问的 URL、负载均衡流量、终止 SSL、
// 提供基于名称的虚拟主机等。
// 已废弃：本版本的 Ingress 已被 networking.k8s.io/v1beta1 Ingress 取代，详见发布说明。
// +k8s:supportsSubresource="/status"
type Ingress struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 是 Ingress 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec IngressSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是 Ingress 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status IngressStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1
// +k8s:prerelease-lifecycle-gen:deprecated=1.14
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,IngressList

// IngressList 是 Ingress 的集合。
type IngressList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Ingress 列表。
	Items []Ingress `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// IngressSpec 描述用户期望存在的 Ingress。
type IngressSpec struct {
	// IngressClassName 是 IngressClass 集群资源的名字。对应的 IngressClass 决定
	// 由哪个控制器实现该资源。它取代了已废弃的 `kubernetes.io/ingress.class` 注解。
	// 为向后兼容，该注解一旦设置，其优先级高于本字段。若字段与注解的值不同，
	// 控制器可以发出警告。本 API 的实现应忽略未指定 class 的 Ingress。
	// 某个 IngressClass 资源可被标记为默认，用于给本字段设置默认值。
	// 更多信息请参考 IngressClass 文档。
	// +optional
	IngressClassName *string `json:"ingressClassName,omitempty" protobuf:"bytes,4,opt,name=ingressClassName"`

	// 用于处理不匹配任何规则的请求的默认后端。'backend' 和 'rules' 至少要指定一个。
	// 本字段为可选，以便负载均衡控制器或默认值逻辑指定全局默认值。
	// +optional
	Backend *IngressBackend `json:"backend,omitempty" protobuf:"bytes,1,opt,name=backend"`

	// TLS 配置。目前 Ingress 只支持单一 TLS 端口 443。如果列表中多个条目指定了
	// 不同的主机，并且实现该 Ingress 的控制器支持 SNI，则它们会通过 SNI TLS
	// 扩展按主机名在同一端口上复用。
	// +optional
	// +listType=atomic
	TLS []IngressTLS `json:"tls,omitempty" protobuf:"bytes,2,rep,name=tls"`

	// 配置 Ingress 的主机规则列表。若未指定，或没有规则匹配，所有流量都发送到默认后端。
	// +optional
	// +listType=atomic
	Rules []IngressRule `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`
	// TODO: 支持通过 claim 指定负载均衡器 IP
}

// IngressTLS 描述与 Ingress 关联的传输层安全配置。
type IngressTLS struct {
	// Hosts 是 TLS 证书中包含的主机列表。列表中的值必须与 tlsSecret 中使用的名字
	// 一致。若未指定，则沿用实现本 Ingress 的负载均衡控制器的通配符主机设置。
	// +optional
	// +listType=atomic
	Hosts []string `json:"hosts,omitempty" protobuf:"bytes,1,rep,name=hosts"`
	// SecretName 是用于在 443 端口终止 SSL 流量的 Secret 名字。
	// 本字段可选，以支持仅基于 SNI 主机名进行 SSL 路由。
	// 若监听器中的 SNI 主机与 IngressRule 使用的 "Host" 头字段冲突，
	// 则 SNI 主机用于终止 SSL，Host 头的值用于路由。
	// +optional
	SecretName string `json:"secretName,omitempty" protobuf:"bytes,2,opt,name=secretName"`
	// TODO: 考虑支持指定不同的终止模式、协议等。
}

// IngressStatus 描述 Ingress 的当前状态。
type IngressStatus struct {
	// LoadBalancer 包含负载均衡器的当前状态。
	// +optional
	LoadBalancer IngressLoadBalancerStatus `json:"loadBalancer,omitempty" protobuf:"bytes,1,opt,name=loadBalancer"`
}

// IngressLoadBalancerStatus 表示负载均衡器的状态。
type IngressLoadBalancerStatus struct {
	// Ingress 是包含负载均衡器接入点的列表。
	// +optional
	// +listType=atomic
	Ingress []IngressLoadBalancerIngress `json:"ingress,omitempty" protobuf:"bytes,1,rep,name=ingress"`
}

// IngressLoadBalancerIngress 表示负载均衡器接入点的状态。
type IngressLoadBalancerIngress struct {
	// 对于基于 IP 的负载均衡器接入点，设置 IP。
	// +optional
	IP string `json:"ip,omitempty" protobuf:"bytes,1,opt,name=ip"`

	// 对于基于 DNS 的负载均衡器接入点，设置 Hostname。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,2,opt,name=hostname"`

	// Ports 提供本负载均衡器暴露的端口信息。
	// +listType=atomic
	// +optional
	Ports []IngressPortStatus `json:"ports,omitempty" protobuf:"bytes,4,rep,name=ports"`
}

// IngressPortStatus 表示服务端口的错误状态。
type IngressPortStatus struct {
	// 接入端口号。
	Port int32 `json:"port" protobuf:"varint,1,opt,name=port"`

	// 接入端口的协议。支持的值为："TCP"、"UDP"、"SCTP"。
	Protocol v1.Protocol `json:"protocol" protobuf:"bytes,2,opt,name=protocol,casttype=Protocol"`

	// Error 用于记录服务端口的问题，其格式须遵循以下规则：
	// - 内置错误值在本文件中指定，且使用 CamelCase 命名；
	// - 云服务商特定的错误值名字必须符合 foo.example.com/CamelCase 格式。
	// ---
	// 其匹配的正则为 (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +optional
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Error *string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
}

// IngressRule 表示将指定主机下的路径映射到相关后端服务的规则。
// 入站请求先按主机匹配，再路由到匹配的 IngressRuleValue 所对应的后端。
type IngressRule struct {
	// Host 是 RFC 3986 定义的网络主机的完全限定域名。
	// 与 RFC 3986 中 URI 的 "host" 部分相比，有以下差异：
	// 1. 不允许使用 IP。目前 IngressRuleValue 只能作用于父 Ingress 规格中的 IP。
	// 2. 不识别 `:` 分隔符，因为不允许端口。
	//    目前 Ingress 的端口对 http 隐式取 :80，对 https 隐式取 :443。
	// 这两点将来都可能变化。
	// 入站请求先与主机匹配，再匹配 IngressRuleValue。若未指定主机，
	// Ingress 会仅依据 IngressRuleValue 路由所有流量。
	//
	// Host 可以是 "精确" 值，即不带结尾点的域名（如 "foo.bar.com"），也可以是
	// "通配" 值，即前缀一个通配符标签的域名（如 "*.foo.com"）。通配符字符 '*'
	// 必须单独作为第一个 DNS 标签出现，且只匹配单个标签。不能只有通配符标签
	// （例如 Host == "*"）。请求与 Host 字段的匹配方式如下：
	// 1. 若 Host 为精确值，当 http host 头等于 Host 时请求匹配该规则。
	// 2. 若 Host 为通配值，当 http host 头等于该通配规则去掉第一个标签后的后缀时，
	//    请求匹配该规则。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,1,opt,name=host"`
	// IngressRuleValue 表示本 IngressRule 路由请求的规则。
	// 若未指定，规则默认为 http 的 catch-all。此时是把匹配主机的流量还是把全部
	// 流量发往默认后端，由实现该 Ingress 的控制器决定。目前仅支持 Http 这种
	// IngressRuleValue。
	// +optional
	IngressRuleValue `json:"" protobuf:"bytes,2,opt,name=ingressRuleValue"`
}

// IngressRuleValue 表示对入站请求应用的规则。规则满足时，请求被路由到指定后端。
// 目前不允许在单个 Ingress 中混用不同类型的规则，因此下列字段必须恰好设置一个。
type IngressRuleValue struct {
	//TODO:
	// 1. 考虑重命名本资源及相关规则，使其不再与 Ingress 绑定，因为它们也可用于
	//    路由集群内部流量。
	// 2. 考虑为负载均衡器可用的 ingress 类型专属全局选项（如 http keep-alive）
	//    增加字段。

	// http 是指向后端的 http 选择器列表。路径与入站请求的路径匹配。目前它可以
	// 包含 RFC 3986 定义的 URL 常规 "path" 部分不允许的字符。路径必须以 '/' 开头。
	// 后端定义流量将被转发到的目标服务端点。
	HTTP *HTTPIngressRuleValue `json:"http,omitempty" protobuf:"bytes,1,opt,name=http"`
}

// HTTPIngressRuleValue 是指向后端的 http 选择器列表。
// 对于 http://<host>/<path>?<searchpart> -> backend 这样的例子（各部分对应
// RFC 3986），本资源用于匹配最后一个 '/' 之后、第一个 '?' 或 '#' 之前的全部内容。
type HTTPIngressRuleValue struct {
	// 将请求映射到后端的路径集合。
	// +listType=atomic
	Paths []HTTPIngressPath `json:"paths" protobuf:"bytes,1,rep,name=paths"`
	// TODO: 考虑为负载均衡器可用的 ingress 类型专属全局选项（如 http keep-alive）
	// 增加字段。
}

// PathType 表示 HTTPIngressPath 引用的路径类型。
type PathType string

// 路径匹配类型常量。
const (
	// PathTypeExact 精确且区分大小写地匹配 URL 路径。
	PathTypeExact = PathType("Exact")

	// PathTypePrefix 基于按 '/' 切分的 URL 路径前缀匹配，区分大小写，且逐路径元素进行。
	// 路径元素指路径按 '/' 分隔得到的标签列表。若路径 p 的每个元素都是请求路径
	// 对应元素的逐元素前缀，则请求与 p 匹配。注意，若路径最后一个元素只是请求路径
	// 最后一个元素的子串，则不算匹配（例如 /foo/bar 匹配 /foo/bar/baz，
	// 但不匹配 /foo/barbaz）。若 Ingress 规格中存在多个匹配路径，优先取最长者。
	// 例如：
	// - /foo/bar 不匹配对 /foo/barbaz 的请求
	// - /foo/bar 匹配对 /foo/bar 和 /foo/bar/baz 的请求
	// - /foo 和 /foo/ 都匹配对 /foo 和 /foo/ 的请求；若两者都出现在 Ingress
	//   规格中，优先取更长的 /foo/。
	PathTypePrefix = PathType("Prefix")

	// PathTypeImplementationSpecific 的匹配方式由 IngressClass 决定。
	// 实现可以把它当作独立的 PathType，也可等同于 Prefix 或 Exact 处理。
	PathTypeImplementationSpecific = PathType("ImplementationSpecific")
)

// HTTPIngressPath 把路径与后端关联起来，路径匹配的入站 URL 会被转发到该后端。
type HTTPIngressPath struct {
	// Path 与入站请求的路径匹配。目前它可以包含 RFC 3986 定义的 URL 常规
	// "path" 部分不允许的字符。路径必须以 '/' 开头。未指定时匹配入站请求的
	// 所有路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`

	// PathType 决定 Path 匹配的解释方式，可取以下值之一：
	// * Exact：精确匹配 URL 路径。
	// * Prefix：基于按 '/' 切分的 URL 路径前缀匹配，逐路径元素进行。路径元素
	//   指路径按 '/' 分隔得到的标签列表。若路径 p 的每个元素都是请求路径对应
	//   元素的逐元素前缀，则请求与 p 匹配。注意，若路径最后一个元素只是请求
	//   路径最后一个元素的子串，则不算匹配（例如 /foo/bar 匹配 /foo/bar/baz，
	//   但不匹配 /foo/barbaz）。
	// * ImplementationSpecific：匹配方式由 IngressClass 决定。实现可以把它当作
	//   独立的 PathType，也可等同于 Prefix 或 Exact 处理。
	// 实现必须支持所有路径类型。默认为 ImplementationSpecific。
	PathType *PathType `json:"pathType,omitempty" protobuf:"bytes,3,opt,name=pathType"`

	// Backend 定义流量将被转发到的目标服务端点。
	Backend IngressBackend `json:"backend" protobuf:"bytes,2,opt,name=backend"`
}

// IngressBackend 描述给定服务和端口的所有端点。
type IngressBackend struct {
	// 指定引用服务的名字。
	// +optional
	ServiceName string `json:"serviceName,omitempty" protobuf:"bytes,1,opt,name=serviceName"`

	// 指定引用服务的端口。
	// +optional
	ServicePort intstr.IntOrString `json:"servicePort,omitempty" protobuf:"bytes,2,opt,name=servicePort"`

	// Resource 是指向 Ingress 对象所在命名空间中另一 Kubernetes 资源的 ObjectRef。
	// 若指定了 resource，则不得指定 serviceName 和 servicePort。
	// +optional
	Resource *v1.TypedLocalObjectReference `json:"resource,omitempty" protobuf:"bytes,3,opt,name=resource"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=Scale,result=Scale
// +genclient:method=ApplyScale,verb=apply,subresource=scale,input=Scale,result=Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ReplicaSet

// 已废弃：本版本的 ReplicaSet 已被 apps/v1beta2/ReplicaSet 取代，详见发布说明。
// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
// +k8s:supportsSubresource="/status"
type ReplicaSet struct {
	metav1.TypeMeta `json:""`

	// 若 ReplicaSet 的 Labels 为空，则默认与其管理的 Pod 的标签一致。
	// 标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 ReplicaSet 期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ReplicaSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是最近一次观察到的 ReplicaSet 状态。该数据可能有一定程度的滞后。
	// 由系统填充，只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ReplicaSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:prerelease-lifecycle-gen:deprecated=1.8
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=apps,v1,ReplicaSetList

// ReplicaSetList 是 ReplicaSet 的集合。
type ReplicaSetList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ReplicaSet 列表。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Items []ReplicaSet `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReplicaSetSpec 是 ReplicaSet 的规格说明。
type ReplicaSetSpec struct {
	// Replicas 是期望的 Pod 数量。用指针以区分显式置零与未指定。默认为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// 新创建的 Pod 需要就绪且容器不崩溃的最小秒数，才会被视为可用。
	// 默认为 0（一旦就绪即视为可用）。
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// Selector 是对应与副本数的 Pod 的标签查询。若为空，则默认取 Pod 模板上的标签。
	// 要被本 replica set 控制，Pod 的标签键和值必须匹配。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`

	// Template 是描述副本数不足时将要创建的 Pod 的对象。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/#pod-template
	// +optional
	Template v1.PodTemplateSpec `json:"template,omitempty" protobuf:"bytes,3,opt,name=template"`
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近一次观察到的非终止态 Pod 数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// 标签与 replica set 的 Pod 模板标签匹配的非终止态 Pod 数。
	// +optional
	FullyLabeledReplicas int32 `json:"fullyLabeledReplicas,omitempty" protobuf:"varint,2,opt,name=fullyLabeledReplicas"`

	// 本 ReplicaSet 所针对、处于 Ready 状态的非终止态 Pod 数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,4,opt,name=readyReplicas"`

	// 本 replica set 的可用非终止态 Pod 数（至少就绪 minReadySeconds）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,5,opt,name=availableReplicas"`

	// 本 replica set 的终止中 Pod 数。终止中 Pod 的 .metadata.deletionTimestamp 非空，
	// 且尚未进入 Failed 或 Succeeded 的 .status.phase。
	//
	// 这是 beta 字段，需要开启 DeploymentReplicaSetTerminatingReplicas 特性（默认开启）。
	// +optional
	TerminatingReplicas *int32 `json:"terminatingReplicas,omitempty" protobuf:"varint,7,opt,name=terminatingReplicas"`

	// ObservedGeneration 反映最近一次观察到的 ReplicaSet 的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`

	// 对 replica set 当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []ReplicaSetCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`
}

// ReplicaSetConditionType 表示 ReplicaSet condition 的类型。
type ReplicaSetConditionType string

// replica set 的有效 condition 类型。
const (
	// 当 replica set 的某个 Pod 因配额不足、LimitRange、Pod 安全策略、节点选择器等
	// 原因创建失败，或因 kubelet 宕机、finalizer 失败而删除失败时，添加 ReplicaSetReplicaFailure。
	ReplicaSetReplicaFailure ReplicaSetConditionType = "ReplicaFailure"
)

// ReplicaSetCondition 描述 replica set 在某一时刻的状态。
type ReplicaSetCondition struct {
	// replica set condition 的类型。
	Type ReplicaSetConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=ReplicaSetConditionType"`
	// condition 的状态，取值为 True、False、Unknown 之一。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// condition 最近一次发生状态转换的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 最近一次状态转换的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 便于人阅读的消息，说明状态转换的细节。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.3
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,NetworkPolicy

// 自 1.9 起废弃：本版本的 NetworkPolicy 已被 networking/v1/NetworkPolicy 取代。
// NetworkPolicy 描述一组 Pod 允许哪些网络流量。
type NetworkPolicy struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 本 NetworkPolicy 期望行为的规格说明。
	// +optional
	Spec NetworkPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 以墓碑形式保留，以说明为何 3 是保留的 protobuf tag。
	// 该注释掉的字段应保留；将来若重新实现 NetworkPolicyStatus，
	// 应使用不同的 protobuf 名字和 tag！
	// Status NetworkPolicyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// 自 1.9 起废弃：本版本的 PolicyType 已被 networking/v1/PolicyType 取代。
// PolicyType 字符串描述 NetworkPolicy 的类型，在 1.8 中属于 beta 级别。
type PolicyType string

// NetworkPolicy 规则类型常量。
const (
	// PolicyTypeIngress 表示影响被选中 Pod 入站流量的 NetworkPolicy。
	PolicyTypeIngress PolicyType = "Ingress"
	// PolicyTypeEgress 表示影响被选中 Pod 出站流量的 NetworkPolicy。
	PolicyTypeEgress PolicyType = "Egress"
)

// 自 1.9 起废弃：本版本的 NetworkPolicySpec 已被 networking/v1/NetworkPolicySpec 取代。
type NetworkPolicySpec struct {
	// 选择本 NetworkPolicy 对象作用的 Pod。ingress 规则数组应用于本字段选中的所有 Pod。
	// 多个 NetworkPolicy 可以选择同一批 Pod，此时各自的 ingress 规则叠加生效。
	// 本字段不可省略，遵循标准标签选择器语义。空的 podSelector 匹配本命名空间中的所有 Pod。
	PodSelector metav1.LabelSelector `json:"podSelector" protobuf:"bytes,1,opt,name=podSelector"`

	// 应用于被选中 Pod 的 ingress 规则列表。满足以下任一条件即允许流量进入 Pod：
	// 没有 NetworkPolicy 选中该 Pod，或流量来自 Pod 所在节点，
	// 或流量匹配 podSelector 选中该 Pod 的所有 NetworkPolicy 对象中的至少一条 ingress 规则。
	// 若本字段为空，则本 NetworkPolicy 不允许任何流量进入
	// （其作用仅是让被选中的 Pod 默认处于隔离状态）。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Ingress []NetworkPolicyIngressRule `json:"ingress,omitempty" protobuf:"bytes,2,rep,name=ingress"`

	// 应用于被选中 Pod 的 egress 规则列表。满足以下条件即允许 Pod 的出站流量：
	// 没有 NetworkPolicy 选中该 Pod（且集群策略允许该流量），或流量匹配
	// podSelector 选中该 Pod 的所有 NetworkPolicy 对象中的至少一条 egress 规则。
	// 若本字段为空，则本 NetworkPolicy 限制所有出站流量
	// （其作用仅是让被选中的 Pod 默认处于隔离状态）。
	// 该字段在 1.8 中属于 beta 级别。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Egress []NetworkPolicyEgressRule `json:"egress,omitempty" protobuf:"bytes,3,rep,name=egress"`

	// 本 NetworkPolicy 涉及的规则类型列表。有效取值为 ["Ingress"]、["Egress"]
	// 或 ["Ingress", "Egress"]。若未指定，则按 Ingress 或 Egress 规则是否存在来推断：
	// 含 Egress 段的策略视为影响 Egress，所有策略（无论是否含 Ingress 段）都视为影响 Ingress。
	// 若要写仅 egress 的策略，必须显式指定 policyTypes [ "Egress" ]。
	// 同样，若要写明确禁止任何 egress 的策略，必须把 "Egress" 纳入 policyTypes
	// （因为这种策略不含 Egress 段，否则会被默认推断为只有 [ "Ingress" ]）。
	// 该字段在 1.8 中属于 beta 级别。
	// +optional
	// +listType=atomic
	PolicyTypes []PolicyType `json:"policyTypes,omitempty" protobuf:"bytes,4,rep,name=policyTypes,casttype=PolicyType"`
}

// 自 1.9 起废弃：本版本的 NetworkPolicyIngressRule 已被 networking/v1/NetworkPolicyIngressRule 取代。
// 仅当流量同时匹配 ports 和 from 时，NetworkPolicyIngressRule 才匹配该流量。
type NetworkPolicyIngressRule struct {
	// 本规则选中的 Pod 上允许访问的端口列表。列表中各项按逻辑或组合。
	// 若本字段为空或缺失，则本规则匹配所有端口（流量不受端口限制）。
	// 若本字段非空，则仅当流量匹配列表中至少一个端口时才允许。
	// +optional
	// +listType=atomic
	Ports []NetworkPolicyPort `json:"ports,omitempty" protobuf:"bytes,1,rep,name=ports"`

	// 可以访问本规则选中 Pod 的源列表。列表中各项按逻辑或组合。
	// 若本字段为空或缺失，则本规则匹配所有源（流量不受源限制）。
	// 若本字段非空，则仅当流量匹配 from 列表中至少一项时才允许。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	From []NetworkPolicyPeer `json:"from,omitempty" protobuf:"bytes,2,rep,name=from"`
}

// 自 1.9 起废弃：本版本的 NetworkPolicyEgressRule 已被 networking/v1/NetworkPolicyEgressRule 取代。
// NetworkPolicyEgressRule 描述允许从 NetworkPolicySpec 的 podSelector 匹配的 Pod
// 流出的一组流量。流量必须同时匹配 ports 和 to。该类型在 1.8 中属于 beta 级别。
type NetworkPolicyEgressRule struct {
	// 出站流量的目标端口列表。列表中各项按逻辑或组合。若本字段为空或缺失，
	// 则本规则匹配所有端口（流量不受端口限制）。若本字段非空，则仅当流量
	// 匹配列表中至少一个端口时才允许。
	// +optional
	// +listType=atomic
	Ports []NetworkPolicyPort `json:"ports,omitempty" protobuf:"bytes,1,rep,name=ports"`

	// 本规则选中 Pod 的出站流量目标列表。列表中各项按逻辑或组合。若本字段为空
	// 或缺失，则本规则匹配所有目标（流量不受目标限制）。若本字段非空，则仅当
	// 流量匹配 to 列表中至少一项时才允许。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	To []NetworkPolicyPeer `json:"to,omitempty" protobuf:"bytes,2,rep,name=to"`
}

// 自 1.9 起废弃：本版本的 NetworkPolicyPort 已被 networking/v1/NetworkPolicyPort 取代。
type NetworkPolicyPort struct {
	// 可选：流量必须匹配的协议（TCP、UDP 或 SCTP）。未指定时默认为 TCP。
	// +optional
	Protocol *v1.Protocol `json:"protocol,omitempty" protobuf:"bytes,1,opt,name=protocol,casttype=k8s.io/api/core/v1.Protocol"`

	// 给定协议下的端口，可以是数字或 Pod 上的命名端口。若未提供，则匹配所有端口名和端口号。
	// 若提供，则仅匹配指定协议 AND 指定端口上的流量。
	// +optional
	Port *intstr.IntOrString `json:"port,omitempty" protobuf:"bytes,2,opt,name=port"`

	// 若设置，表示策略应允许从 port 到 endPort（含两端）的端口范围。若未定义 port，
	// 或 port 是命名（字符串）端口，则不能定义本字段。endPort 必须大于或等于 port。
	// +optional
	EndPort *int32 `json:"endPort,omitempty" protobuf:"bytes,3,opt,name=endPort"`
}

// 自 1.9 起废弃：本版本的 IPBlock 已被 networking/v1/IPBlock 取代。
// IPBlock 描述允许 NetworkPolicySpec 的 podSelector 匹配的 Pod 访问的某个 CIDR
// （如 "192.168.1.0/24"、"2001:db8::/64"）。except 条目描述不应包含在本规则内的 CIDR。
type IPBlock struct {
	// CIDR 是表示 IP 段的字符串，合法示例为 "192.168.1.0/24" 或 "2001:db8::/64"。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	CIDR string `json:"cidr" protobuf:"bytes,1,name=cidr"`
	// Except 是不应包含在 IP 段内的 CIDR 切片，合法示例为 "192.168.1.0/24" 或 "2001:db8::/64"。
	// 若 except 的值超出 CIDR 范围，将被拒绝。
	// +optional
	// +listType=atomic
	Except []string `json:"except,omitempty" protobuf:"bytes,2,rep,name=except"`
}

// 自 1.9 起废弃：本版本的 NetworkPolicyPeer 已被 networking/v1/NetworkPolicyPeer 取代。
type NetworkPolicyPeer struct {
	// 选择 Pod 的标签选择器，遵循标准标签选择器语义；若存在但为空，则选择所有 Pod。
	//
	// 若同时设置了 NamespaceSelector，则整个 NetworkPolicyPeer 选择 NamespaceSelector
	// 选中的命名空间中匹配 PodSelector 的 Pod；否则选择策略自身命名空间中匹配
	// PodSelector 的 Pod。
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty" protobuf:"bytes,1,opt,name=podSelector"`

	// 使用集群范围的标签选择命名空间，遵循标准标签选择器语义；若存在但为空，
	// 则选择所有命名空间。
	//
	// 若同时设置了 PodSelector，则整个 NetworkPolicyPeer 选择 NamespaceSelector
	// 选中的命名空间中匹配 PodSelector 的 Pod；否则选择 NamespaceSelector 选中的
	// 命名空间中的所有 Pod。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,2,opt,name=namespaceSelector"`

	// IPBlock 针对某个 IP 段定义策略。若设置了本字段，则其他两个字段都不能设置。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	IPBlock *IPBlock `json:"ipBlock,omitempty" protobuf:"bytes,3,rep,name=ipBlock"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.3
// +k8s:prerelease-lifecycle-gen:deprecated=1.9
// +k8s:prerelease-lifecycle-gen:removed=1.16
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,NetworkPolicyList

// 自 1.9 起废弃：本版本的 NetworkPolicyList 已被 networking/v1/NetworkPolicyList 取代。
// NetworkPolicyList 是 NetworkPolicy 对象的列表。
type NetworkPolicyList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 schema 对象的列表。
	Items []NetworkPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}
