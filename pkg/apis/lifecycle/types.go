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

package lifecycle

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
)

const (
	// EvictionResponderImperativeEviction 是默认 responder，通过带退避的 imperative
	// Eviction API（pods/<name>/eviction 子资源）驱逐 pod。
	EvictionResponderImperativeEviction string = "imperative-eviction.k8s.io/evictor"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EvictionRequest 定义一项请求，理想情况下应促成对
// .spec.target（例如 Pod 的终止）的优雅驱逐。
//
// evictionrequest-controller 观察所有 EvictionRequest 的意图，并将其转换为
// Eviction。
//   - .spec.requester 会被设置为 Eviction 上的 label，便于查找。
//   - 每个目标都可以分配一组 responder。Eviction 对象由这些 responder
//     观测，responder 实现驱逐逻辑并在 Eviction 的 status 中更新进度。
//
// 通常 EvictionRequest 与 Eviction 之间是多对多关系；
// 若目标是 Pod，则为多对一。
//
// 若所有请求者都撤回对同一目标的驱逐意图，驱逐将被取消。
// 删除 EvictionRequest 同样视为撤回。当某个目标的所有 EvictionRequest
// 都被移除后，对应的 Eviction 最终会被垃圾回收。
type EvictionRequest struct {
	metav1.TypeMeta

	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta

	// spec 定义该 eviction request 的规格。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec EvictionRequestSpec

	// status 表示最近观测到的 eviction request 状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status EvictionRequestStatus
}

// EvictionRequestSpec 是 EvictionRequest 的规格。
type EvictionRequestSpec struct {
	// target 包含对要被驱逐的对象（例如 Pod）的引用。
	// 该字段必填且不可变。
	// +required
	Target EvictionRequestTarget

	// requester 用于标识请求驱逐 target 的实体。
	//
	// 必须是带域名前缀的合法 key（例如 "acme.io/foo"）。
	// 域名 *.k8s.io 和 *.kubernetes.io 保留。
	// 该字段必填且不可变。
	// +required
	Requester string

	// intent 指定对目标对象应采取的动作。
	//
	// - Eviction 表示请求者希望对目标执行驱逐。
	// - Withdrawn 表示请求者不再希望对目标执行驱逐。
	// 如果同一 target 的所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	//   Cancellation consequences:
	// - Inactive 的 responder 永远不会运行。
	// - Active 的 responder 应当取消驱逐。
	// - Completed 或 Interrupted 的 responder 不应采取任何动作。
	// +required
	Intent EvictionRequestIntent
}

// EvictionRequestTarget 包含对要被驱逐的对象的引用。
type EvictionRequestTarget struct {
	// pod 引用一个将被驱逐/终止的 Pod。
	// 不支持属于 PodGroup 的 Pod（即设置了 .spec.schedulingGroup 的 Pod）。
	// +optional
	Pod *EvictionRequestPodReference
}

// EvictionRequestPodReference contains enough information to locate the referenced pod inside the
// same namespace.
type EvictionRequestPodReference struct {
	// 目标名称。
	// 该字段必填。
	// +required
	Name string
	// uid 是目标对象的 UID，可在目标的 .metadata.uid 中找到，
	// 为 8-4-4-4-12 格式的小写 UUID。
	// 该字段必填。
	// +required
	UID apimachinerytypes.UID
}

// EvictionRequestIntent 表示 requester 的 intent。
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
	// conditions contain information about the eviction request.
	//
	// EvictionRequest specific conditions are: TargetEvicted or Failed (managed by evictionrequest-controller).
	// - Failed means that the eviction request is no longer being processed
	//   by any eviction responder. This can happen if the request is canceled or if no responder
	//   managed to evict the target (e.g. terminate or delete a pod).
	// - TargetEvicted means that the target has been evicted (e.g. a pod has been terminated or deleted).
	//
	// These conditions can be reset if the eviction was unsuccessful and a new Eviction intent has
	// been submitted.
	//
	// The maximum length of the conditions list is 100.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition

	// observedGeneration 是 evictionrequest-controller 观测到的 EvictionRequest 的 .metadata.generation。
	// 观测到的 generation 值不能为负，且只能递增。
	// 最小值为 1。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	ObservedGeneration *int64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EvictionRequestList 包含 EvictionRequests 资源列表。
type EvictionRequestList struct {
	metav1.TypeMeta
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 EvictionRequests 列表。
	Items []EvictionRequest
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Eviction 发起一次驱逐流程，理想情况下应促成对 .spec.target
// （例如 Pod 的终止）的优雅驱逐。
//
// evictionrequest-controller 观察所有 EvictionRequest 的意图，并将其转换为
// Eviction，并管理 Eviction 的生命周期。
// 即使在 requester 撤回请求后，.status.requesters 中仍会保留这些 requester。
// 若所有请求者都撤回对同一目标的驱逐意图，驱逐将被取消。
// 当与该 Eviction 的 .spec.target 对应的所有 EvictionRequest 都被移除后，
// 这个 Eviction 对象最终会被垃圾回收。
//
// 如果 target 是 pod，.status.targetResponders 将根据 Pod 的
// .spec.evictionResponders 填充。
//
// responder 应当观测并通过 .status 进行通信：当它们在
// .status.targetResponders 中看到自身状态为 Active 时，应协助完成目标驱逐。
// 随后 ResponderStatus 结构体需被周期性更新，以在 .status.responders 中
// 反映每个 responder 的驱逐进度或完成情况。若
// .status.responders[].heartbeatTime 未在 Eviction API 定义的心跳截止时间
// （目前为 20 分钟）内更新，驱逐将移交给下一个优先级更低的 responder。
//
// 如果没有其他 responder 且 target 是 pod，最后一个优先级为 100 的默认
// imperative-eviction.k8s.io/evictor responder 将通过 imperative Eviction API
// （pods/<name>/eviction 子资源）驱逐该 pod。
type Eviction struct {
	metav1.TypeMeta

	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
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
	metav1.ObjectMeta

	// spec 定义该 eviction 的规格。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec EvictionSpec

	// status 表示最近观测到的 eviction 状态。
	// 由 responder 和 evictionrequest-controller 填充。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status EvictionStatus
}

// EvictionParticipantRole 表示 eviction 参与者的角色。
type EvictionParticipantRole string

const (
	// EvictionParticipantRoleRequester identifies a requester which creates EvictionRequests.
	EvictionParticipantRoleRequester EvictionParticipantRole = "requester"

	// EvictionParticipantRoleResponder 标识响应 Eviction 的 responder。
	EvictionParticipantRoleResponder EvictionParticipantRole = "responder"

	// EvictionParticipantRoleRequesterResponder 表示同时是 "requester" 和 "responder"。
	EvictionParticipantRoleRequesterResponder EvictionParticipantRole = "requester-responder"
)

// EvictionSpec 是 Eviction 的规格。
type EvictionSpec struct {
	// target 包含对要被驱逐的对象（例如 Pod）的引用。
	// 该字段必填且不可变。
	// +required
	Target EvictionTarget
}

// EvictionTarget 包含对要被驱逐的对象的引用。
// +union
type EvictionTarget struct {
	// pod 引用一个将被驱逐/终止的 Pod。
	// 不支持属于 PodGroup 的 Pod（即设置了 .spec.schedulingGroup 的 Pod）。
	// +optional
	Pod *EvictionPodReference
}

// EvictionPodReference contains enough information to locate the referenced pod inside the same
// namespace.
type EvictionPodReference struct {
	// 目标名称。
	// 该字段必填。
	// +required
	Name string
	// uid 是目标对象的 UID，可在目标的 .metadata.uid 中找到，
	// 为 8-4-4-4-12 格式的小写 UUID。
	// 该字段必填。
	// +required
	UID apimachinerytypes.UID
}

// EvictionStatus 表示最近观测到的 eviction request 状态。
type EvictionStatus struct {
	// conditions contain information about the eviction request.
	//
	// Eviction specific conditions are: TargetEvicted or Failed (managed by evictionrequest-controller).
	// - Failed means that the eviction request is no longer being processed
	//   by any eviction responder. This can happen if the request is canceled or if no responder
	//   managed to evict the target (e.g. terminate or delete a pod).
	// - TargetEvicted means that the target has been evicted (e.g. a pod has been terminated or deleted).
	//
	// 	The maximum length of the conditions list is 100.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition

	// observedGeneration 是 evictionrequest-controller 观测到的 Eviction 的 .metadata.generation。
	// 观测到的 generation 值不能为负，且只能递增。
	// 最小值为 1。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	ObservedGeneration *int64

	// requesters 用于标识请求驱逐 target 的实体。
	// 如果所有 requester 都撤回其驱逐 intent，驱逐将被取消。
	// requesters 列表的最大长度为 100。
	// 若超出该限制，应优先丢弃 intent 为 Withdrawn 的 requester。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Requesters []Requester

	// targetResponders 引用最终应响应本次驱逐、以协助目标优雅驱逐的
	// responder。这些 responder 按各自指定的优先级顺序被依次选中，
	// 选中方式是将 TargetResponder 的 .state 字段设为 Active。
	// 允许处于 Active 状态的 responder 最大数量为 1。
	// 每个 responder 最终会进入 Interrupted、Canceled 或 Completed 状态之一。
	// responder 应当观测这些状态以推进自身的生命周期。
	//
	// 如果 target 是 pod，该字段根据 Pod 的 .spec.evictionResponders 填充。
	// 根据 target 的不同，列表中还可能加入默认 responder。
	//
	// 默认 responder：
	// - 若 target 是 pod，则将优先级为 100 的 imperative-eviction.k8s.io/evictor responder
	// 加入列表。它将调用 imperative Eviction API（pods/<name>/eviction 子资源）。
	// 该调用可能因 PodDisruptionBudget 阻止 pod 终止而失败。
	// 此时它会更新 responder 的 message，并带退避重试。
	//
	// responders 列表的最大长度为 11。
	// 列表的长度和 key 一旦设置便不可更改。
	// 该字段由 evictionrequest-controller 管理。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	TargetResponders []TargetResponder

	// responders 表示每个已声明 responder 的驱逐流程状态。
	// responder 列表的长度和 .name 字段应与 .status.targetResponders 一致。
	// 只有 .name 在 .targetResponders[].state 中处于 Active 状态的 responder 才应被更新，
	// 也只有它们可以被修改。允许对该列表做首次初始化。
	// 每个 ResponderStatus 由 evictionrequest-controller 初始化，之后由指定的 responder 管理。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Responders []ResponderStatus
}

type EvictionConditionType string

// 以下是 eviction request 内置的 conditions。
const (
	// EvictionConditionFailed means that the eviction request is no longer being processed
	// by any eviction responder. This can happen if the request is canceled or if no responder
	// managed to evict the target (e.g. terminate or delete a pod).
	EvictionConditionFailed EvictionConditionType = "Failed"

	// EvictionConditionTargetEvicted means that the target has been evicted (e.g. a pod has been
	// terminated or deleted).
	EvictionConditionTargetEvicted EvictionConditionType = "TargetEvicted"
)

type EvictionConditionReason string

// 以下是 eviction request 内置的 condition reason。
const (
	// EvictionConditionReasonAwaitingEviction 表示该 Eviction 工作正常，target 已被安排驱逐。
	// 该 reason 用于 Failed 和 TargetEvicted condition。
	EvictionConditionReasonAwaitingEviction EvictionConditionReason = "AwaitingEviction"
	// EvictionConditionReasonEvictionInvalid 表示 Eviction 未被接受，
	// 因为初始配置无效。
	// 该原因用于 Failed 状态的条件。
	EvictionConditionReasonEvictionInvalid EvictionConditionReason = "EvictionInvalid"
	// EvictionConditionReasonCanceledDueToNoRequesters 表示不存在 target 相同且 .spec.intent 为 Eviction
	// 的 EvictionRequest，因此该 Eviction 被取消。
	// 该原因用于 Failed 状态的条件。
	EvictionConditionReasonCanceledDueToNoRequesters EvictionConditionReason = "CanceledDueToNoRequesters"
	// EvictionConditionReasonSucceeded 表示该 Eviction 已成功驱逐 target。
	// 该原因用于 Failed 状态的条件。
	EvictionConditionReasonSucceeded EvictionConditionReason = "Succeeded"
	// EvictionConditionReasonNoFurtherResponder 表示 Eviction 的 responder 未能驱逐 target，
	// 且没有更多可用的 responder。
	// 该原因用于 Failed 状态的条件。
	EvictionConditionReasonNoFurtherResponder EvictionConditionReason = "NoFurtherResponder"
	// EvictionConditionReasonPodDeleted 表示作为 target 的 pod 已被删除。
	// 该原因用于 TargetEvicted 条件。
	EvictionConditionReasonPodDeleted EvictionConditionReason = "PodDeleted"
	// EvictionConditionReasonPodTerminal 表示作为 target 的 pod 已进入终止状态。
	// 该原因用于 TargetEvicted 条件。
	EvictionConditionReasonPodTerminal EvictionConditionReason = "PodTerminal"
	// EvictionConditionReasonEvictionFailed 表示对 target 的驱逐未成功。
	// 该原因用于 TargetEvicted 条件。
	EvictionConditionReasonEvictionFailed EvictionConditionReason = "EvictionFailed"
)

// Requester 用于标识请求驱逐 target 的实体。
// +structType=atomic
type Requester struct {
	// name 用于标识请求驱逐 target 的实体。
	//
	// 必须是带域名前缀的合法 key（例如 "acme.io/foo"）。
	// 每个 requester 的该字段必须唯一。
	// 该字段必填。
	// +required
	Name string

	// intent 指定对目标对象应采取的动作。
	//
	// - Eviction 表示请求者希望对目标执行驱逐。
	// - Withdrawn 表示请求者不再希望对目标执行驱逐。
	// 如果所有 requester 的 intent 都变为 Withdrawn，驱逐将被取消。
	//   Cancellation consequences:
	// - Inactive 的 responder 永远不会运行。
	// - Active 的 responder 应当取消驱逐。
	// - Completed 或 Interrupted 的 responder 不应采取任何动作。
	// +required
	Intent RequesterIntent
}

// RequesterIntent 表示 requester 的 intent。
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
	// 必须是带域名前缀的合法 key（例如 "acme.io/foo"）。
	// 每个响应者的此字段必须唯一。
	// 该字段必填。
	// +required
	Name string

	// priority 是该 responder 的优先级。evictionrequest-controller 会优先选择
	// 优先级更高的 responder。若有多个 responder 优先级相同，则选中域名在
	// "higher domain" 字母序中排在最前的那个：即先按字母序比较顶级域名标签，
	// 再依次比较更低层级的域名标签，最后比较 key。
	//
	// 作为 pod 管理控制器的 responder 应将此字段设为 10000，
	// 以便其他 responder 既能抢占，也能在其后注册回退。
	//
	// 最小值为 0，最大值为 100000。
	// 0-999 区间保留给后缀为 *.k8s.io 的响应者。
	// 该字段必填且不可变。
	// +required
	Priority *int32

	// state 是由 evictionrequest-controller 分配的状态；responder 应观察该状态以推进自身生命周期。
	// - Inactive 表示 responder 尚不应处理此 eviction request。
	// - Active 表示 responder 正在运行或即将启动。
	// 同时，evictionrequest-controller 已在 ResponderStatus 中设置 startTime。
	// 处于 Active 的 responder 应通过更新 .status.responders 参与驱逐流程，其中 .name 为该
	// Active responder 的名字。应定期更新 ResponderStatus 字段以表明驱逐流程的进展或完成。
	// 如果 .status.responders[].heartbeatTime 未在 Eviction API 定义的 heartbeat 截止时间内
	// 更新（当前为 20 分钟），驱逐将转交给优先级更低的下一个 responder。同一时刻只能有一个
	// Active responder。
	// - Interrupted 表示 responder 启动失败，或未及时更新 ResponderStatus 中的 heartbeatTime。
	// - Canceled 表示 responder 已被取消。换言之，不存在 target 相同且 .spec.intent 为
	// Eviction 的 EvictionRequest。
	// - Completed 表示 responder 已成功完成，并在 ResponderStatus 中设置了 completionTime。
	// 各 responder 的更多细节请参见 .status.responders 中的 ResponderStatus。
	// +required
	State ResponderStateType
}

// ResponderStateType 表示由 evictionrequest-controller 分配的状态。
type ResponderStateType string

const (
	// ResponderStateInactive 表示 responder 尚不应处理此 eviction request。
	ResponderStateInactive ResponderStateType = "Inactive"

	// ResponderStateActive 表示 responder 正在运行或即将启动。
	// 同时，evictionrequest-controller 已在 ResponderStatus 中设置 startTime。
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
	// 必须是带域名前缀的合法 key（例如 "acme.io/foo"）。
	// 该字段由 Kubernetes 初始化，且每个 responder 必须唯一。
	// 该字段必填。
	// +required
	Name string

	// startTime 记录该 responder 被指定为 Active、应当开始处理驱逐请求的时间。
	// 设置时应反映当前时间。
	// 该字段由 Kubernetes 在该 responder 变为 Active 时初始化。
	// 一旦设置便不可变。
	// +optional
	StartTime *metav1.Time

	// heartbeatTime 是 responder 最后一次上报驱逐过程仍在进行的时间。
	// 设置时应反映当前时间。
	// responder 应避免心跳频率高于每 20 秒一次，以免压垮控制面。
	// +optional
	HeartbeatTime *metav1.Time

	// expectedCompletionTime 是 responder 预期完成当前驱逐步骤的时间。
	// 该时间不能设为过去。
	// 如果无法估算，可以省略。
	// +optional
	ExpectedCompletionTime *metav1.Time

	// completionTime 记录 responder 停止处理 eviction request 的时间。
	// Completed 意味着 responder 已全部或部分完成驱逐流程，这可能已导致 target 被驱逐
	// （例如 pod 被终止）。
	// 设置时应反映当前时间。
	// 一旦设置便不可变。
	// +optional
	CompletionTime *metav1.Time

	// message 提供关于 responder 状态和驱逐流程的人类可读详情。
	// 最大长度为 4000 个字符。
	// +optional
	Message *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EvictionList 包含 Eviction 资源列表。
type EvictionList struct {
	metav1.TypeMeta
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 Eviction 列表。
	Items []Eviction
}
