/*
Copyright The Kubernetes Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
)

const (
	// EvictionResponderImperativeEviction 是默认 responder，通过带退避的 imperative
	// Eviction API（pods/<name>/eviction 子资源）驱逐 pod。
	EvictionResponderImperativeEviction string = "imperative-eviction.k8s.io/evictor"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:supportsSubresource="/status"

// EvictionRequest 定义一项请求，理想情况下应促成 .spec.target 的优雅驱逐
// （例如终止一个 pod）
//
// evictionrequest-controller 观察所有 EvictionRequest 的 intent，并将其转换为
// Eviction。
//   - .spec.requester 会作为 label 设置到 Eviction 上，便于查找。
//   - 每个 target 可以分配一组 responder。这些 responder 观察 Eviction 对象，
//     实现驱逐逻辑，并把进展更新到 Eviction 的 status 中。
//
// 一般而言 EvictionRequest 与 Eviction 是多对多关系；当 target 是 pod 时是多对一。
//
// 如果所有 requester 都撤回了对同一 target 的驱逐 intent，驱逐就会被取消。
// 删除 EvictionRequest 也算作撤回。当某个 target 的所有 EvictionRequest 都被移除后，
// 相应的 Eviction 最终会被垃圾回收。
//
// +k8s:validation-gen-nolint // Note: remove this when the API got GA
type EvictionRequest struct {
	metav1.TypeMeta `json:",inline"`

	// metadata 是标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义该 eviction request 的规格。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec EvictionRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示最近观测到的 eviction request 状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status EvictionRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// EvictionRequestSpec 是 EvictionRequest 的规格。
type EvictionRequestSpec struct {
	// target 包含对要被驱逐的对象（例如 pod）的引用。
	// 该字段必填且不可变。
	// +required
	// +k8s:immutable
	Target EvictionRequestTarget `json:"target" protobuf:"bytes,1,opt,name=target"`

	// requester 用于标识请求驱逐 target 的实体。
	//
	// 必须是合法的带域名前缀的 key（例如 "acme.io/foo"）。
	// 域名 *.k8s.io 和 *.kubernetes.io 为保留域名。
	// 该字段必填且不可变。
	// +required
	// +k8s:required
	// +k8s:immutable
	// +k8s:format=k8s-prefixed-label-key
	// +k8s:customValidation
	Requester string `json:"requester" protobuf:"bytes,2,opt,name=requester"`

	// intent 指定对目标 target 应采取的动作。
	//
	// - Eviction 表示 requester 希望驱逐 target。
	// - Withdrawn 表示 requester 不再希望驱逐 target。
	//   如果同一 target 的所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	//   取消的后果：
	//   - Inactive 的 responder 永远不会运行。
	//   - Active 的 responder 应当取消驱逐。
	//   - Completed 或 Interrupted 的 responder 不应采取任何动作。
	// +required
	// +k8s:required
	Intent EvictionRequestIntent `json:"intent" protobuf:"bytes,3,opt,name=intent,casttype=EvictionRequestIntent"`
}

// EvictionRequestTarget 包含对要被驱逐的对象的引用。
// +union
type EvictionRequestTarget struct {
	// pod 指向要被驱逐/终止的 pod。
	// 不支持属于 PodGroup 的 pod（即设置了 .spec.schedulingGroup）。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Pod *EvictionRequestPodReference `json:"pod,omitempty" protobuf:"bytes,1,opt,name=pod"`
}

// EvictionRequestPodReference 包含在同一命名空间内定位被引用的 pod 所需的足够信息。
type EvictionRequestPodReference struct {
	// name 是目标的名字。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// uid 是目标的 uid。
	// 可在目标的 .metadata.uid 中找到，为 8-4-4-4-12 格式的小写 UUID。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-uuid
	UID apimachinerytypes.UID `json:"uid" protobuf:"bytes,2,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

// EvictionRequestIntent 表示 requester 的 intent。
// +enum
// +k8s:enum
type EvictionRequestIntent string

// 以下是每个 requester 可以设置的 intent。
const (
	// EvictionRequestIntentEviction 表示 requester 希望驱逐 target。
	EvictionRequestIntentEviction EvictionRequestIntent = "Eviction"

	// EvictionRequestIntentWithdrawn 表示 requester 不再希望驱逐 target。
	// 如果同一 target 的所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	// 取消的后果：
	// - Inactive 的 responder 永远不会运行。
	// - Active 的 responder 应当取消驱逐。
	// - Completed 或 Interrupted 的 responder 不应采取任何动作。
	EvictionRequestIntentWithdrawn EvictionRequestIntent = "Withdrawn"
)

// EvictionRequestStatus 表示最近观测到的 eviction request 状态。
type EvictionRequestStatus struct {
	// conditions 包含该 eviction request 的相关信息。
	//
	// EvictionRequest 特有的 conditions 有：TargetEvicted 或 Failed（由 evictionrequest-controller 管理）。
	// - Failed 表示任何 eviction responder 都不再处理该 eviction request。
	//   这可能发生在请求被取消，或没有 responder 成功驱逐 target（例如终止或删除 pod）时。
	// - TargetEvicted 表示 target 已被驱逐（例如 pod 已被终止或删除）。
	//
	// 如果驱逐未成功且提交了新的 Eviction intent，这些 conditions 可以被重置。
	//
	// conditions 列表的最大长度为 100。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	// +k8s:maxItems=100
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// observedGeneration 是 evictionrequest-controller 观测到的 EvictionRequest 的 .metadata.generation。
	// 观测到的 generation 值不能为负，且只能递增。
	// 最小值为 1。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	// +k8s:optional
	// +k8s:minimum=1
	// +k8s:update=NoUnset
	// +k8s:monotonic
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,2,opt,name=observedGeneration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// EvictionRequestList 包含 EvictionRequests 资源列表。
type EvictionRequestList struct {
	metav1.TypeMeta `json:",inline"`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 EvictionRequests 列表。
	Items []EvictionRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:supportsSubresource="/status"

// Eviction 发起一次驱逐流程，理想情况下应促成 .spec.target 的优雅驱逐
// （例如终止一个 pod）。
//
// evictionrequest-controller 观察所有 EvictionRequest 的 intent，并将其转换为
// Eviction，并管理 Eviction 的生命周期。
// 即使在 requester 撤回请求后，.status.requesters 中仍会保留这些 requester。
// 如果所有 requester 都撤回了对同一 target 的驱逐 intent，驱逐将被取消。
// 当与该 Eviction 的 .spec.target 对应的所有 EvictionRequest 都被移除后，
// 这个 Eviction 对象最终会被垃圾回收。
//
// 如果 target 是 pod，.status.targetResponders 将根据 Pod 的
// .spec.evictionResponders 填充。
//
// 当 responder 在 .status.targetResponders 中看到自身状态为 Active 时，应通过观察
// .status 来进行协作与通信，帮助驱逐 target。随后各 responder 应定期更新
// .status.responders 中的 ResponderStatus，以表明驱逐流程的进展或完成。
// 如果 .status.responders[].heartbeatTime 未在 Eviction API 定义的 heartbeat 截止时间内
// 更新（当前为 20 分钟），驱逐将转交给优先级更低的下一个 responder。
//
// 如果没有其他 responder 且 target 是 pod，最后一个优先级为 100 的默认
// imperative-eviction.k8s.io/evictor responder 将通过 imperative Eviction API
// （pods/<name>/eviction 子资源）驱逐该 pod。
// +k8s:validation-gen-nolint // Note: remove this when the API got GA
type Eviction struct {
	metav1.TypeMeta `json:",inline"`

	// metadata 是标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// 由 evictionrequest-controller 设置的 .metadata.name 仅作提示，可能发生变化。
	// 应使用 .spec.target 字段精确标识 target。
	//
	// requester 和 responder 的名字会作为 label key，按以下格式之一添加到
	// eviction 的 labels 中：
	// 1. acme.io/foo: "requester"
	// 2. acme.io/foo: "responder"
	// 3. acme.io/foo: "requester-responder"
	//
	// 可用的角色 label 值请参见 EvictionParticipantRole。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义该 eviction 的规格。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec EvictionSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示最近观测到的 eviction 状态。
	// 由 responder 和 evictionrequest-controller 填充。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status EvictionStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// EvictionParticipantRole 表示 eviction 参与者的角色。
type EvictionParticipantRole string

const (
	// EvictionParticipantRoleRequester 标识创建 EvictionRequest 的 requester。
	EvictionParticipantRoleRequester EvictionParticipantRole = "requester"

	// EvictionParticipantRoleResponder 标识响应 Eviction 的 responder。
	EvictionParticipantRoleResponder EvictionParticipantRole = "responder"

	// EvictionParticipantRoleRequesterResponder 表示同时是 "requester" 和 "responder"。
	EvictionParticipantRoleRequesterResponder EvictionParticipantRole = "requester-responder"
)

// EvictionSpec 是 Eviction 的规格。
type EvictionSpec struct {
	// target 包含对要被驱逐的对象（例如 pod）的引用。
	// 该字段必填且不可变。
	// +required
	// +k8s:immutable
	Target EvictionTarget `json:"target" protobuf:"bytes,1,opt,name=target"`
}

// EvictionTarget 包含对要被驱逐的对象的引用。
// +union
type EvictionTarget struct {
	// pod 指向要被驱逐/终止的 pod。
	// 不支持属于 PodGroup 的 pod（即设置了 .spec.schedulingGroup）。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Pod *EvictionPodReference `json:"pod,omitempty" protobuf:"bytes,1,opt,name=pod"`
}

// EvictionPodReference 包含在同一命名空间内定位被引用的 pod 所需的足够信息。
type EvictionPodReference struct {
	// name 是目标的名字。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// uid 是目标的 uid。
	// 可在目标的 .metadata.uid 中找到，为 8-4-4-4-12 格式的小写 UUID。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-uuid
	UID apimachinerytypes.UID `json:"uid" protobuf:"bytes,2,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

// EvictionStatus 表示最近观测到的 eviction request 状态。
type EvictionStatus struct {
	// conditions 包含该 eviction request 的相关信息。
	//
	// Eviction 特有的 conditions 有：TargetEvicted 或 Failed（由 evictionrequest-controller 管理）。
	// - Failed 表示任何 eviction responder 都不再处理该 eviction request。
	//   这可能发生在请求被取消，或没有 responder 成功驱逐 target（例如终止或删除 pod）时。
	// - TargetEvicted 表示 target 已被驱逐（例如 pod 已被终止或删除）。
	//
	// 	conditions 列表的最大长度为 100。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	// +k8s:maxItems=100
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// observedGeneration 是 evictionrequest-controller 观测到的 Eviction 的 .metadata.generation。
	// 观测到的 generation 值不能为负，且只能递增。
	// 最小值为 1。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	// +k8s:optional
	// +k8s:minimum=1
	// +k8s:monotonic
	// +k8s:update=NoUnset
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,2,opt,name=observedGeneration"`

	// requesters 用于标识请求驱逐 target 的实体。
	// 如果所有 requester 都撤回其驱逐 intent，驱逐将被取消。
	//
	// requesters 列表的最大长度为 100。
	// 若超出该限制，应优先丢弃 intent 为 Withdrawn 的 requester。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=100
	Requesters []Requester `json:"requesters,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,3,rep,name=requesters"`

	// targetResponders 指向最终应响应该 Eviction 的 responder，以帮助优雅驱逐 target。
	// 这些 responder 根据其优先级按顺序被选中：evictionrequest-controller 将
	// TargetResponder 的 .state 字段设为 Active。允许的 Active responder 最多 1 个。
	// 最终每个 responder 可能进入 Interrupted、Canceled 或 Completed 状态。
	// responder 应观察这些状态以推进自身生命周期。
	//
	// 如果 target 是 pod，该字段根据 Pod 的 .spec.evictionResponders 填充。
	// 根据 target 的不同，列表中还可能加入默认 responder。
	//
	// 默认 responder：
	// - 若 target 是 pod，则将优先级为 100 的 imperative-eviction.k8s.io/evictor responder
	//   加入列表。它将调用 imperative Eviction API（pods/<name>/eviction 子资源）。
	//   该调用可能因 PodDisruptionBudget 阻止 pod 终止而失败。
	//   此时它会更新 responder 的 message，并带退避重试。
	//
	// responders 列表的最大长度为 11。
	// 列表的长度和 key 一旦设置便不可更改。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=11
	TargetResponders []TargetResponder `json:"targetResponders,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,4,rep,name=targetResponders"`

	// responders 表示每个已声明 responder 的驱逐流程状态。
	//
	// responder 列表的长度和 .name 字段应与 .status.targetResponders 一致。
	// 只有 .name 在 .targetResponders[].state 中处于 Active 状态的 responder 才应被更新，
	// 也只有它们可以被修改。允许对该列表做首次初始化。
	//
	// 每个 ResponderStatus 由 evictionrequest-controller 初始化，之后由指定的 responder 管理。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=11
	Responders []ResponderStatus `json:"responders,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,5,rep,name=responders"`
}

// EvictionConditionType 表示 Eviction 的 condition 类型。
type EvictionConditionType string

// 以下是 eviction request 内置的 conditions。
const (
	// EvictionConditionFailed 表示任何 eviction responder 都不再处理该 eviction request。
	// 这可能发生在请求被取消，或没有 responder 成功驱逐 target（例如终止或删除 pod）时。
	EvictionConditionFailed EvictionConditionType = "Failed"

	// EvictionConditionTargetEvicted 表示 target 已被驱逐（例如 pod 已被终止或删除）。
	EvictionConditionTargetEvicted EvictionConditionType = "TargetEvicted"
)

// EvictionConditionReason 表示 Eviction 的 condition reason。
type EvictionConditionReason string

// 以下是 eviction request 内置的 condition reason。
const (
	// EvictionConditionReasonAwaitingEviction 表示该 Eviction 工作正常，target 已被安排驱逐。
	// 该 reason 用于 Failed 和 TargetEvicted condition。
	EvictionConditionReasonAwaitingEviction EvictionConditionReason = "AwaitingEviction"
	// EvictionConditionReasonEvictionInvalid 表示初始配置不合法，因此不接受该 Eviction。
	// 该 reason 用于 Failed condition。
	EvictionConditionReasonEvictionInvalid EvictionConditionReason = "EvictionInvalid"
	// EvictionConditionReasonCanceledDueToNoRequesters 表示不存在 target 相同且 .spec.intent 为 Eviction
	// 的 EvictionRequest，因此该 Eviction 被取消。
	// 该 reason 用于 Failed condition。
	EvictionConditionReasonCanceledDueToNoRequesters EvictionConditionReason = "CanceledDueToNoRequesters"
	// EvictionConditionReasonSucceeded 表示该 Eviction 已成功驱逐 target。
	// 该 reason 用于 Failed condition。
	EvictionConditionReasonSucceeded EvictionConditionReason = "Succeeded"
	// EvictionConditionReasonNoFurtherResponder 表示 Eviction 的 responder 未能驱逐 target，
	// 且没有更多可用的 responder。
	// 该 reason 用于 Failed condition。
	EvictionConditionReasonNoFurtherResponder EvictionConditionReason = "NoFurtherResponder"
	// EvictionConditionReasonPodDeleted 表示作为 target 的 pod 已被删除。
	// 该 reason 用于 TargetEvicted condition。
	EvictionConditionReasonPodDeleted EvictionConditionReason = "PodDeleted"
	// EvictionConditionReasonPodTerminal 表示作为 target 的 pod 已进入终止状态。
	// 该 reason 用于 TargetEvicted condition。
	EvictionConditionReasonPodTerminal EvictionConditionReason = "PodTerminal"
	// EvictionConditionReasonEvictionFailed 表示对 target 的驱逐未成功。
	// 该 reason 用于 TargetEvicted condition。
	EvictionConditionReasonEvictionFailed EvictionConditionReason = "EvictionFailed"
)

// Requester 用于标识请求驱逐 target 的实体。
// +structType=atomic
type Requester struct {
	// name 用于标识请求驱逐 target 的实体。
	//
	// 必须是合法的带域名前缀的 key（例如 "acme.io/foo"）。
	// 每个 requester 的该字段必须唯一。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-prefixed-label-key
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// intent 指定对目标 target 应采取的动作。
	//
	// - Eviction 表示 requester 希望驱逐 target。
	// - Withdrawn 表示 requester 不再希望驱逐 target。
	//   如果所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	//   取消的后果：
	//   - Inactive 的 responder 永远不会运行。
	//   - Active 的 responder 应当取消驱逐。
	//   - Completed 或 Interrupted 的 responder 不应采取任何动作。
	// +required
	// +k8s:required
	Intent RequesterIntent `json:"intent" protobuf:"bytes,2,opt,name=intent,casttype=RequesterIntent"`
}

// RequesterIntent 表示 requester 的 intent。
// +enum
// +k8s:enum
type RequesterIntent string

// 以下是每个 requester 可以设置的 intent。
const (
	// RequesterIntentEviction 表示 requester 希望驱逐 target。
	RequesterIntentEviction RequesterIntent = "Eviction"

	// RequesterIntentWithdrawn 表示 requester 不再希望驱逐 target。
	// 如果所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	// 取消的后果：
	// - Inactive 的 responder 永远不会运行。
	// - Active 的 responder 应当取消驱逐。
	// - Completed 或 Interrupted 的 responder 不应采取任何动作。
	RequesterIntentWithdrawn RequesterIntent = "Withdrawn"
)

// TargetResponder 用于指定响应 Eviction 的 responder。
// responder 应通过观察 Eviction API（见 .state）进行协作，以帮助优雅驱逐 target
// （例如终止一个 pod）。
// +structType=atomic
type TargetResponder struct {
	// name 用于标识响应 Eviction 的 responder。
	//
	// 必须是合法的带域名前缀的 key（例如 "acme.io/foo"）。
	// 每个 responder 的该字段必须唯一。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-prefixed-label-key
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// priority 是该 responder 的优先级；evictionrequest-controller 优先选择优先级高的。
	// 若有多个 responder 优先级相同，则按域名从高到低（按字母序）选择域名靠前的那个：
	// 先按字母序比较顶级域名标签，再比较较低的域名标签，最后比较 key。
	//
	// 作为 pod 管理控制器的 responder 应将此字段设为 10000，
	// 以便其他 responder 进行抢占或注册兜底。
	//
	// 最小值为 0，最大值为 100000。
	// 区间 0-999 保留给 *.k8s.io 后缀的 responder。
	// 该字段必填且不可变。
	// +required
	// +k8s:required
	// +k8s:minimum=0
	// +k8s:maximum=100000
	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	Priority *int32 `json:"priority" protobuf:"varint,2,opt,name=priority"`

	// state 是由 evictionrequest-controller 分配的状态；responder 应观察该状态以推进自身生命周期。
	// - Inactive 表示 responder 尚不应处理此 eviction request。
	// - Active 表示 responder 正在运行或即将启动。
	//   同时，evictionrequest-controller 已在 ResponderStatus 中设置 startTime。
	//
	//   处于 Active 的 responder 应通过更新 .status.responders 参与驱逐流程，其中 .name 为该
	//   Active responder 的名字。应定期更新 ResponderStatus 字段以表明驱逐流程的进展或完成。
	//   如果 .status.responders[].heartbeatTime 未在 Eviction API 定义的 heartbeat 截止时间内
	//   更新（当前为 20 分钟），驱逐将转交给优先级更低的下一个 responder。同一时刻只能有一个
	//   Active responder。
	// - Interrupted 表示 responder 启动失败，或未及时更新 ResponderStatus 中的 heartbeatTime。
	// - Canceled 表示 responder 已被取消。换言之，不存在 target 相同且 .spec.intent 为
	//   Eviction 的 EvictionRequest。
	// - Completed 表示 responder 已成功完成，并在 ResponderStatus 中设置了 completionTime。
	//
	// 各 responder 的更多细节请参见 .status.responders 中的 ResponderStatus。
	// +required
	// +k8s:required
	State ResponderStateType `json:"state" protobuf:"bytes,3,opt,name=state,casttype=ResponderStateType"`
}

// ResponderStateType 表示由 evictionrequest-controller 分配的状态。
// +enum
// +k8s:enum
type ResponderStateType string

const (
	// ResponderStateInactive 表示 responder 尚不应处理此 eviction request。
	ResponderStateInactive ResponderStateType = "Inactive"

	// ResponderStateActive 表示 responder 正在运行或即将启动。
	// 同时，evictionrequest-controller 已在 ResponderStatus 中设置 startTime。
	//
	// 处于 Active 的 responder 应通过更新 .status.responders 参与驱逐流程，其中 .name 为该
	// Active responder 的名字。应定期更新 ResponderStatus 字段以表明驱逐流程的进展或完成。
	// 如果 .status.responders[].heartbeatTime 未在 Eviction API 定义的 heartbeat 截止时间内
	// 更新（当前为 20 分钟），驱逐将转交给优先级更低的下一个 responder。同一时刻只能有一个
	// Active responder。
	ResponderStateActive ResponderStateType = "Active"

	// ResponderStateInterrupted 表示 responder 启动失败，或未及时更新 ResponderStatus 中的 heartbeatTime。
	ResponderStateInterrupted ResponderStateType = "Interrupted"

	// ResponderStateCanceled 表示 responder 已被取消。换言之，不存在 target 相同且 .spec.intent
	// 为 Eviction 的 EvictionRequest。
	ResponderStateCanceled ResponderStateType = "Canceled"

	// ResponderStateCompleted 表示 responder 已成功完成，并在 ResponderStatus 中设置了 completionTime。
	ResponderStateCompleted ResponderStateType = "Completed"
)

// ResponderStatus 表示 responder 观测到的驱逐流程最近状态。
// 只应由名字为 .name 字段的指定 responder 更新。
// +structType=granular
type ResponderStatus struct {
	// name 用于标识响应 Eviction 的 responder。
	//
	// 必须是合法的带域名前缀的 key（例如 "acme.io/foo"）。
	// 该字段由 Kubernetes 初始化，且每个 responder 必须唯一。
	// 该字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-prefixed-label-key
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// startTime 记录该 responder 被指定为 Active、应开始处理 eviction request 的时间。
	// 设置时应反映当前时间。
	// 当该 responder 变为 Active 时，该字段由 Kubernetes 初始化。
	// 一旦设置便不可变。
	// +optional
	// +k8s:optional
	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	StartTime *metav1.Time `json:"startTime,omitempty" protobuf:"bytes,2,opt,name=startTime"`

	// heartbeatTime 是 responder 最近一次上报驱逐流程仍在进行中的时间。
	// 设置时应反映当前时间。
	// responder 应避免 heartbeat 比 20 秒更频繁，以免压垮控制平面。
	// +optional
	// +k8s:optional
	HeartbeatTime *metav1.Time `json:"heartbeatTime,omitempty" protobuf:"bytes,3,opt,name=heartbeatTime"`

	// expectedCompletionTime 是 responder 预期完成当前驱逐步骤的时间。
	// 该时间不能设为过去。
	// 如果无法估算，可以省略。
	// +optional
	// +k8s:optional
	ExpectedCompletionTime *metav1.Time `json:"expectedCompletionTime,omitempty" protobuf:"bytes,4,opt,name=expectedCompletionTime"`

	// completionTime 记录 responder 停止处理 eviction request 的时间。
	// Completed 意味着 responder 已全部或部分完成驱逐流程，这可能已导致 target 被驱逐
	// （例如 pod 被终止）。
	// 设置时应反映当前时间。
	// 一旦设置便不可变。
	// +optional
	// +k8s:optional
	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	CompletionTime *metav1.Time `json:"completionTime,omitempty" protobuf:"bytes,5,opt,name=completionTime"`

	// message 提供关于 responder 状态和驱逐流程的人类可读详情。
	// 最大长度为 4000 个字符。
	// +optional
	// +k8s:optional
	// +k8s:maxLength=4000
	Message *string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// EvictionList 包含 Eviction 资源列表。
type EvictionList struct {
	metav1.TypeMeta `json:",inline"`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 Eviction 列表。
	Items []Eviction `json:"items" protobuf:"bytes,2,rep,name=items"`
}
