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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// 所有 Kubernetes label 都需要加上 Kubernetes 前缀，以区别于最终用户定义的 label。
	// 更多信息：https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#label-selector-and-annotation-conventions
	labelPrefix = "batch.kubernetes.io/"

	// CronJobScheduledTimestampAnnotation 是 Job 的调度时间戳 annotation。
	// 它记录运行中 Job 的原始/预期调度时间戳，采用 RFC3339 格式。
	CronJobScheduledTimestampAnnotation = labelPrefix + "cronjob-scheduled-timestamp"

	// JobCompletionIndexAnnotation 记录 Indexed Job 中 Pod 的 completion index。
	JobCompletionIndexAnnotation = labelPrefix + "job-completion-index"
	// JobTrackingFinalizer 是 Job 所属 Pod 的 finalizer，防止 Pod 在计入 Job status 之前被删除。
	//
	// 此外，apiserver 和 job controller 也使用这个字符串作为 Job annotation，
	// 标记正在通过 pod finalizer 跟踪的 Job。不过该用法在 kubernetes 1.26 中已废弃：
	// 从 1.27 起（JobTrackingWithFinalizers 达到 GA 之后的一个版本），
	// apiserver 和 job controller 会忽略该 annotation，并始终使用 finalizer 跟踪 Job。
	JobTrackingFinalizer = labelPrefix + "job-tracking"
	// JobNameLabel 是 Job 的 name label，所有 Job label 都以 batch.kubernetes.io 为前缀。
	// 历史上 job controller 为 job-name 和 controller-uid 使用的是无前缀 label，
	// 出于一致性考虑，Kubernetes 仍然识别这些无前缀 label。
	JobNameLabel = labelPrefix + "job-name"
	// ControllerUidLabel 用于以编程方式获取属于某个 Job 的 Pod。
	// 出于历史兼容原因，也支持一个不带 batch.kubernetes.io 前缀的对应 label。
	ControllerUidLabel = labelPrefix + "controller-uid"
	// JobIndexFailureCountAnnotation 记录 Pod 对应 index 的失败次数，这些失败计入 backoff limit。
	JobIndexFailureCountAnnotation = labelPrefix + "job-index-failure-count"
	// JobIndexIgnoredFailureCountAnnotation 记录 Pod 对应 index 的失败次数，
	// 根据 pod failure policy 这些失败不计入 backoff limit。annotation 缺失时视为 0。
	JobIndexIgnoredFailureCountAnnotation = labelPrefix + "job-index-ignored-failure-count"
	// JobControllerName 是内置 Job controller 在 managedBy 字段上使用的保留值。
	JobControllerName = "kubernetes.io/job-controller"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// Job 表示一个 Job 的配置。
// +k8s:supportsSubresource="/status"
type Job struct {
	metav1.TypeMeta `json:""`
	// 标准对象的 metadata。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Job 期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec JobSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Job 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status JobStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// JobList 是 Job 的集合。
type JobList struct {
	metav1.TypeMeta `json:""`
	// 标准 list 的 metadata。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 Job 列表。
	Items []Job `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CompletionMode 指定如何跟踪 Job 的 Pod completion。
// +enum
type CompletionMode string

const (
	// NonIndexedCompletion 是一种 Job completion 模式。在该模式下，
	// 当成功完成的 Pod 数量达到 .spec.completions 时，Job 被视为完成。
	// 各个 Pod completion 之间彼此等价。
	NonIndexedCompletion CompletionMode = "NonIndexed"

	// IndexedCompletion 是一种 Job completion 模式。在该模式下，
	// Job 的每个 Pod 会获得一个 0 到 (.spec.completions - 1) 的 completion index。
	// 当每个 completion index 都有一个 Pod 成功完成时，Job 被视为完成。
	IndexedCompletion CompletionMode = "Indexed"
)

// PodFailurePolicyAction 指定如何处理 Pod 失败。
// +enum
type PodFailurePolicyAction string

const (
	// PodFailurePolicyActionFailJob 在 Pod 失败时将该 Pod 所属的 Job 标记为 Failed，并终止所有运行中的 Pod。
	PodFailurePolicyActionFailJob PodFailurePolicyAction = "FailJob"

	// PodFailurePolicyActionFailIndex 在 Pod 失败时将该 Pod 的 index 标记为失败，避免该 index 内重启。
	// 该 action 只能在设置 backoffLimitPerIndex 时使用。
	PodFailurePolicyActionFailIndex PodFailurePolicyAction = "FailIndex"

	// PodFailurePolicyActionIgnore 在 Pod 失败时不增加 .backoffLimit 的计数
	// （由 Job 的 .status.failed 字段表示），并创建替代 Pod。
	PodFailurePolicyActionIgnore PodFailurePolicyAction = "Ignore"

	// PodFailurePolicyActionCount 在 Pod 失败时按默认方式处理：
	// 增加 .backoffLimit 的计数（由 Job 的 .status.failed 字段表示）。
	PodFailurePolicyActionCount PodFailurePolicyAction = "Count"
)

// PodFailurePolicyOnExitCodesOperator 指定容器 exit code 与指定值集合之间的关系运算符。
// +enum
type PodFailurePolicyOnExitCodesOperator string

const (
	// PodFailurePolicyOnExitCodesOpIn 表示 exit code 必须落在指定值集合中。
	PodFailurePolicyOnExitCodesOpIn PodFailurePolicyOnExitCodesOperator = "In"
	// PodFailurePolicyOnExitCodesOpNotIn 表示 exit code 不得落在指定值集合中。
	PodFailurePolicyOnExitCodesOpNotIn PodFailurePolicyOnExitCodesOperator = "NotIn"
)

// PodReplacementPolicy 指定创建替代 Pod 的策略。
// +enum
type PodReplacementPolicy string

const (
	// TerminatingOrFailed 表示当 Pod 正在终止（具有 metadata.deletionTimestamp）或失败时重新创建 Pod。
	TerminatingOrFailed PodReplacementPolicy = "TerminatingOrFailed"
	// Failed 表示等到先前创建的 Pod 完全终止（处于 Failed 或 Succeeded phase）后再创建替代 Pod。
	Failed PodReplacementPolicy = "Failed"
)

// PodFailurePolicyOnExitCodesRequirement 描述基于容器 exit code 处理失败 Pod 的条件。
// 具体而言，它查找每个 app 容器和 init 容器状态的 .state.terminated.exitCode，
// 分别对应 Pod status 中的 .status.containerStatuses 和 .status.initContainerStatuses 字段。
// 成功结束（exit code 为 0）的容器不参与条件检查。
type PodFailurePolicyOnExitCodesRequirement struct {
	// 将 exit code 检查限制在指定名称的容器上。为 null 时，该规则适用于所有容器。
	// 指定时，它必须匹配 Pod template 中某个 container 或 initContainer 的名称。
	// +optional
	ContainerName *string `json:"containerName,omitempty" protobuf:"bytes,1,opt,name=containerName"`

	// 表示容器 exit code 与指定值集合之间的关系。成功结束（exit code 为 0）的容器
	// 不参与条件检查。可能取值包括：
	//
	// - In：只要至少有一个容器 exit code（当 'containerName' 字段未限制时可能有多个）
	//   落在指定值集合中，条件即满足。
	// - NotIn：只要至少有一个容器 exit code（当 'containerName' 字段未限制时可能有多个）
	//   不在指定值集合中，条件即满足。
	// 未来可能增加其他取值。客户端遇到未知 operator 时，应视为条件不满足。
	Operator PodFailurePolicyOnExitCodesOperator `json:"operator" protobuf:"bytes,2,req,name=operator"`

	// 指定值集合。每个取到的容器 exit code（多个容器时可能有多个）都会按照 operator
	// 与该集合进行比对。值列表必须有序且不得重复。In operator 不能使用值 '0'。
	// 至少需要一个元素，最多允许 255 个元素。
	// +listType=set
	Values []int32 `json:"values" protobuf:"varint,3,rep,name=values"`
}

// PodFailurePolicyOnPodConditionsPattern 描述用于匹配实际 Pod condition type 的模式。
type PodFailurePolicyOnPodConditionsPattern struct {
	// 指定所需的 Pod condition type。要匹配某个 Pod condition，指定 type 必须等于该 Pod condition 的 type。
	Type corev1.PodConditionType `json:"type" protobuf:"bytes,1,req,name=type"`

	// 指定所需的 Pod condition status。要匹配某个 Pod condition，指定 status 必须等于该 Pod condition 的 status。
	// 默认为 True。
	// +optional
	Status corev1.ConditionStatus `json:"status" protobuf:"bytes,2,req,name=status"`
}

// PodFailurePolicyRule 描述条件满足时如何处理 Pod 失败。
// 每条规则只能使用 onExitCodes 或 onPodConditions 之一，不能同时使用。
type PodFailurePolicyRule struct {
	// 指定条件满足时对 Pod 失败采取的 action。可能取值包括：
	//
	// - FailJob：将该 Pod 所属的 Job 标记为 Failed，并终止所有运行中的 Pod。
	// - FailIndex：将该 Pod 的 index 标记为 Failed，且不再重启。
	// - Ignore：不增加 .backoffLimit 的计数，并创建替代 Pod。
	// - Count：按默认方式处理该 Pod —— 增加 .backoffLimit 的计数。
	// 未来可能增加其他取值。客户端遇到未知 action 时应跳过该规则。
	Action PodFailurePolicyAction `json:"action" protobuf:"bytes,1,req,name=action"`

	// 表示对容器 exit code 的条件。
	// +optional
	OnExitCodes *PodFailurePolicyOnExitCodesRequirement `json:"onExitCodes,omitempty" protobuf:"bytes,2,opt,name=onExitCodes"`

	// 表示对 Pod condition 的条件。该条件以 Pod condition 模式列表表示；
	// 只要有一个模式匹配实际 Pod condition，条件即满足。最多允许 20 个元素。
	// +listType=atomic
	// +optional
	OnPodConditions []PodFailurePolicyOnPodConditionsPattern `json:"onPodConditions,omitempty" protobuf:"bytes,3,opt,name=onPodConditions"`
}

// PodFailurePolicy 描述失败的 Pod 如何影响 backoffLimit。
type PodFailurePolicy struct {
	// Pod 失败策略规则列表，按顺序求值。一旦某条规则匹配了 Pod 失败，其余规则将被忽略。
	// 如果没有规则匹配，则采用默认处理方式：增加 Pod 失败计数，并与 backoffLimit 比较。
	// 最多允许 20 个元素。
	// +listType=atomic
	Rules []PodFailurePolicyRule `json:"rules" protobuf:"bytes,1,opt,name=rules"`
}

// SuccessPolicy 描述在什么情况下可以基于部分 index 的成功将 Job 判定为 succeeded。
type SuccessPolicy struct {
	// rules 表示在 `.status.succeeded >= .spec.completions` 之前将 Job 判定为 successful 的候选规则列表。
	// 一旦满足其中任意规则，就会添加 "SuccessCriteriaMet" condition，并清除残留的 Pod。
	// 此类 Job 的终止状态带有 "Complete" condition。
	// 此外，这些规则按顺序求值；Job 满足某条规则后，其他规则将被忽略。
	// 最多允许 20 个元素。
	// +listType=atomic
	Rules []SuccessPolicyRule `json:"rules" protobuf:"bytes,1,opt,name=rules"`
}

// SuccessPolicyRule 描述将 Job 判定为 succeeded 的规则。
// 每条规则必须至少指定 "succeededIndexes" 或 "succeededCount" 之一。
type SuccessPolicyRule struct {
	// succeededIndexes 指定必须包含在 Job 实际 succeeded index 集合中的 index 集合。
	// index 列表必须位于 0 到 ".spec.completions-1" 之间，且不得重复。至少需要一个元素。
	// index 以逗号分隔的区间表示。区间可以是十进制整数，也可以是由连字符连接的两个十进制整数，
	// 后者表示由首尾元素界定的一段连续编号。
	// 例如，若已完成的 index 为 1、3、4、5 和 7，则表示为 "1,3-5,7"。
	// 该字段为 null 时不会采用任何默认值，也不会在任何时候被求值。
	//
	// +optional
	SucceededIndexes *string `json:"succeededIndexes,omitempty" protobuf:"bytes,1,opt,name=succeededIndexes"`

	// succeededCount 指定 Job 实际 succeeded index 集合所需的最小元素数量。
	// 当 succeededCount 与 succeededIndexes 一起使用时，检查仅限 succeededIndexes 指定的 index 集合。
	// 例如，succeededIndexes 为 "1-4"、succeededCount 为 "3"，而已完成的 index 为 "1"、"3" 和 "5" 时，
	// Job 不会被判定为 succeeded，因为该规则只考虑 "1" 和 "3"。
	// 该字段为 null 时不会采用任何默认值，也不会在任何时候被求值。
	// 指定时必须为正整数。
	//
	// +optional
	SucceededCount *int32 `json:"succeededCount,omitempty" protobuf:"varint,2,opt,name=succeededCount"`
}

// JobSpec 描述 Job 的执行方式。
type JobSpec struct {

	// 指定 Job 在任意时刻期望运行的最大 Pod 数量。当 ((.spec.completions - .status.successful) < .spec.parallelism)
	// 即剩余工作量小于最大并行度时，稳态下实际运行的 Pod 数会小于该值。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	// +optional
	Parallelism *int32 `json:"parallelism,omitempty" protobuf:"varint,1,opt,name=parallelism"`

	// 指定 Job 期望成功完成的 Pod 数量。设为 null 表示任一 Pod 成功即代表所有 Pod 成功，
	// 且 parallelism 可以是任意正值。设为 1 表示 parallelism 限制为 1，
	// 且该 Pod 成功即代表 Job 成功。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	// +optional
	Completions *int32 `json:"completions,omitempty" protobuf:"varint,2,opt,name=completions"`

	// 指定从 startTime 起，Job 在系统尝试终止它之前可以持续运行的秒数；该值必须为正整数。
	// 如果 Job 被暂停（在创建时或通过更新），该计时器实际上会停止，并在 Job 恢复时重置。
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,3,opt,name=activeDeadlineSeconds"`

	// 指定处理失败 Pod 的策略。具体来说，可指定一组 action 以及触发相应 action 所需满足的条件。
	// 为空时采用默认行为：增加失败 Pod 计数（由 Job 的 .status.failed 字段表示），
	// 并与 backoffLimit 比较。该字段不能与 restartPolicy=OnFailure 一起使用。
	//
	// +optional
	PodFailurePolicy *PodFailurePolicy `json:"podFailurePolicy,omitempty" protobuf:"bytes,11,opt,name=podFailurePolicy"`

	// successPolicy 指定何时可以将 Job 判定为 succeeded。为空时采用默认行为：
	// 只有成功 Pod 的数量等于 completions 时，Job 才会被判定为 succeeded。
	// 指定该字段时，它必须不可变，且只对 Indexed Job 生效。
	// 一旦 Job 满足 SuccessPolicy，残留的 Pod 会被终止。
	//
	// +optional
	SuccessPolicy *SuccessPolicy `json:"successPolicy,omitempty" protobuf:"bytes,16,opt,name=successPolicy"`

	// 指定将 Job 标记为 failed 之前的重试次数。默认为 6，
	// 除非指定了 backoffLimitPerIndex（仅限 Indexed Job）。
	// 指定 backoffLimitPerIndex 时，backoffLimit 默认为 2147483647。
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty" protobuf:"varint,7,opt,name=backoffLimit"`

	// 指定在某个 index 内将 index 标记为 failed 之前的重试次数上限。
	// 启用后，每个 index 的失败次数保存在 Pod 的
	// batch.kubernetes.io/job-index-failure-count annotation 中。
	// 只有在 Job 的 completionMode=Indexed 且 Pod 的 restart policy 为 Never 时才能设置。
	// 该字段不可变。
	// +optional
	BackoffLimitPerIndex *int32 `json:"backoffLimitPerIndex,omitempty" protobuf:"varint,12,opt,name=backoffLimitPerIndex"`

	// 在设置 backoffLimitPerIndex 时，指定将 Job 标记为 failed 之前允许的失败 index 最大数量。
	// 一旦失败 index 数量超过该值，整个 Job 会被标记为 Failed 并终止执行。
	// 为 null 时，Job 会继续执行所有 index，并最终被标记上 `Complete` Job condition。
	// 只有在设置 backoffLimitPerIndex 时才能指定该字段。
	// 该值可以为 null 或最大到 completions。当 completions 大于 10^5 时，
	// 该字段必填且必须小于或等于 10^4。
	// +optional
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("backoffLimitPerIndex")
	MaxFailedIndexes *int32 `json:"maxFailedIndexes,omitempty" protobuf:"varint,13,opt,name=maxFailedIndexes"`

	// TODO: 待 https://github.com/kubernetes/kubernetes/issues/28486 修复后启用。
	// 可选：要保留的失败 Pod 数量。
	// +optional
	// FailedPodsLimit *int32 `json:"failedPodsLimit,omitempty" protobuf:"varint,9,opt,name=failedPodsLimit"`

	// 用于筛选与 Pod 数量匹配的 Pod 的 label query。
	// 通常该字段由系统自动设置。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,4,opt,name=selector"`

	// manualSelector 控制是否由用户指定 Pod label 和 Pod selector。
	// 除非清楚自己在做什么，否则不要设置 `manualSelector`。
	// 为 false 或未设置时，系统为该 Job 挑选唯一的 label，并附加到 Pod template 上。
	// 为 true 时，用户需要自行选择唯一 label 并指定 selector。
	// label 不唯一可能导致该 Job 以及其他 Job 无法正常工作。
	// 不过，在通过旧版 `extensions/v1beta1` API 创建的 Job 中可能会看到
	// `manualSelector=true`。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#specifying-your-own-pod-selector
	// +optional
	ManualSelector *bool `json:"manualSelector,omitempty" protobuf:"varint,5,opt,name=manualSelector"`

	// 描述执行 Job 时将创建的 Pod。
	// template.spec.restartPolicy 只允许取 "Never" 或 "OnFailure"。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	Template corev1.PodTemplateSpec `json:"template" protobuf:"bytes,6,opt,name=template"`

	// ttlSecondsAfterFinished 限制已完成执行（Complete 或 Failed）的 Job 的存活时间。
	// 设置该字段后，Job 完成 ttlSecondsAfterFinished 秒后即可被自动删除。
	// 删除 Job 时仍会遵守其生命周期保证（例如 finalizer）。未设置该字段时，Job 不会被自动删除。
	// 设为零时，Job 完成后立即可以被删除。
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty" protobuf:"varint,8,opt,name=ttlSecondsAfterFinished"`

	// completionMode 指定如何跟踪 Pod completion，可取 `NonIndexed`（默认）或 `Indexed`。
	//
	// `NonIndexed` 表示当成功完成的 Pod 数量达到 .spec.completions 时，Job 被视为完成。
	// 各个 Pod completion 之间彼此等价。
	//
	// `Indexed` 表示 Job 的每个 Pod 会获得一个 0 到 (.spec.completions - 1) 的 completion index，
	// 该 index 记录在 batch.kubernetes.io/job-completion-index annotation 中。
	// 当每个 index 都有一个成功完成的 Pod 时，Job 被视为完成。
	// 取值为 `Indexed` 时，必须指定 .spec.completions，且 `.spec.parallelism` 必须小于或等于 10^5。
	// 此外，Pod 名称形如 `$(job-name)-$(index)-$(random-string)`，
	// Pod hostname 形如 `$(job-name)-$(index)`。
	//
	// 未来可能增加更多 completion mode。如果 Job controller 遇到无法识别的 mode
	// （升级期间因版本偏差可能出现这种情况），将跳过对该 Job 的更新。
	// +optional
	CompletionMode *CompletionMode `json:"completionMode,omitempty" protobuf:"bytes,9,opt,name=completionMode,casttype=CompletionMode"`

	// suspend 指定 Job controller 是否创建 Pod。如果创建 Job 时 suspend 为 true，
	// Job controller 不会创建任何 Pod。如果 Job 在创建后被暂停（即该标志从 false 变为 true），
	// Job controller 会删除该 Job 关联的所有活跃 Pod。用户需要设计工作负载以妥善处理这种情况。
	// 暂停 Job 会重置 Job 的 StartTime 字段，实际上也会重置 ActiveDeadlineSeconds 计时器。
	// 默认为 false。
	//
	// +optional
	Suspend *bool `json:"suspend,omitempty" protobuf:"varint,10,opt,name=suspend"`

	// podReplacementPolicy 指定何时创建替代 Pod。可能取值包括：
	// - TerminatingOrFailed：当 Pod 正在终止（具有 metadata.deletionTimestamp）或失败时重新创建 Pod。
	// - Failed：等到先前创建的 Pod 完全终止（处于 Failed 或 Succeeded phase）后再创建替代 Pod。
	//
	// 使用 podFailurePolicy 时，只允许取 Failed。
	// 不使用 podFailurePolicy 时，TerminatingOrFailed 和 Failed 都允许。
	// +optional
	PodReplacementPolicy *PodReplacementPolicy `json:"podReplacementPolicy,omitempty" protobuf:"bytes,14,opt,name=podReplacementPolicy,casttype=podReplacementPolicy"`

	// ManagedBy 字段指明管理 Job 的 controller。k8s Job controller 会协调未设置该字段
	// 或字段值为保留字符串 `kubernetes.io/job-controller` 的 Job，
	// 而跳过该字段为自定义值的 Job。
	// 该值必须是合法的域名前缀路径（例如 acme.io/foo）：第一个 "/" 之前的所有字符
	// 必须符合 RFC 1123 定义的合法子域名，之后的字符必须符合 RFC 3986 定义的合法 HTTP Path 字符。
	// 该值不能超过 63 个字符。该字段不可变。
	// +optional
	ManagedBy *string `json:"managedBy,omitempty" protobuf:"bytes,15,opt,name=managedBy"`

	// scheduling 定义该 Job 的 Workload-aware Scheduling 配置。设置后，
	// 它指定调度策略（basic 或 gang）、拓扑约束、中断模式以及共享的 resource claim。
	// 省略时，Job 默认采用 basic 调度策略，其行为等同于标准的逐 Pod 调度。
	// 该字段处于 alpha 阶段，需要启用 WorkloadWithJob feature gate。
	// 该字段不可变，包括是否设置本身；创建后只能修改 policy.gang.minCount。
	//
	// +featureGate=WorkloadWithJob
	// +optional
	// +k8s:ifDisabled(WorkloadWithJob)=+k8s:forbidden
	// +k8s:optional
	// +k8s:update=NoSet
	// +k8s:update=NoUnset
	Scheduling *JobSchedulingConfiguration `json:"scheduling,omitempty" protobuf:"bytes,17,opt,name=scheduling"`
}

// JobSchedulingConfiguration 组合可复用的 workload-aware scheduling 构建块。
type JobSchedulingConfiguration struct {
	// SchedulingPolicy 定义该 Job 的调度策略。Basic 或 Gang 必须且只能设置其一。
	// 该字段创建后不可变：不能新增或移除策略。策略类型（basic/gang）由手写校验逻辑固定；
	// 只有 schedulingPolicy.gang.minCount 可以修改。
	//
	// +optional
	// +k8s:optional
	// +k8s:update=NoSet
	// +k8s:update=NoUnset
	SchedulingPolicy *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy `json:"schedulingPolicy,omitempty" protobuf:"bytes,1,opt,name=schedulingPolicy"`

	// SchedulingConstraints 定义 Job 所属 Pod 的调度约束（例如拓扑约束）。
	// 该字段创建后不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	SchedulingConstraints *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,2,opt,name=schedulingConstraints"`

	// DisruptionMode 定义 Job 所属 Pod 可以被中断的模式，可取 Single 或 All。
	// 该字段创建后不可变：不能新增或移除，所选模式也不能修改。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *schedulingv1alpha3.WorkloadPodGroupDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,3,opt,name=disruptionMode"`

	// ResourceClaims 定义 Job 中 Pod 之间可以共享哪些 ResourceClaim。
	// Pod 若在自己的 Spec.ResourceClaims 中定义一个与 PodGroup 的 claim 完全匹配的 claim，
	// 即可使用该 PodGroup claim 分配到的设备。两者名称必须相同，且指向同一个
	// ResourceClaim 或 ResourceClaimTemplate。
	// 最多可设置 4 个 claim，与最终生成的 PodGroup 的限制一致。
	// 该列表创建后不可变：既不能新增、删除，也不能修改条目。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=4
	// +k8s:immutable
	ResourceClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim `json:"resourceClaims,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,4,rep,name=resourceClaims"`
}

// JobStatus 表示 Job 的当前状态。
type JobStatus struct {
	// 对象当前状态的最新可用观测结果。Job 失败时，某个 condition 的 type 为 "Failed" 且 status 为 true。
	// Job 被暂停时，某个 condition 的 type 为 "Suspended" 且 status 为 true；
	// Job 恢复后，该 condition 的 status 会变为 false。Job 完成时，某个 condition 的 type 为 "Complete" 且 status 为 true。
	//
	// Job 处于终止状态（"Complete" 或 "Failed"）时被视为已结束。
	// Job 不能同时具有 "Complete" 和 "Failed" condition，也不能同时处于
	// "Complete" 和 "FailureTarget" condition。
	// "Complete"、"Failed" 和 "FailureTarget" condition 不能被禁用。
	//
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=atomic
	Conditions []JobCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// 表示 job controller 开始处理 Job 的时间。如果 Job 以暂停状态创建，
	// 该字段直到首次恢复时才会设置。每次从暂停状态恢复时，该字段都会重置。
	// 采用 RFC3339 格式，时区为 UTC。
	//
	// 一旦设置，只有在 Job 被暂停时才能移除该字段；Job 未暂停或已结束时不能修改。
	//
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty" protobuf:"bytes,2,opt,name=startTime"`

	// 表示 Job 完成的时间。不保证在不同操作之间按 happens-before 顺序设置。
	// 采用 RFC3339 格式，时区为 UTC。
	// 完成时间仅在 Job 成功结束时设置。
	// 该值不能更新或移除，且与 startTime 字段表示的时间相同或更晚。
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty" protobuf:"bytes,3,opt,name=completionTime"`

	// 处于 pending 或 running 且未终止（没有 deletionTimestamp）的 Pod 数量。
	// 已结束的 Job 该值为 0。
	// +optional
	Active int32 `json:"active,omitempty" protobuf:"varint,4,opt,name=active"`

	// 进入 Succeeded phase 的 Pod 数量。
	// 对于给定的 spec，该值单调递增；但弹性 indexed job 缩容时可能下降。
	// +optional
	Succeeded int32 `json:"succeeded,omitempty" protobuf:"varint,5,opt,name=succeeded"`

	// 进入 Failed phase 的 Pod 数量。该值单调递增。
	// +optional
	Failed int32 `json:"failed,omitempty" protobuf:"varint,6,opt,name=failed"`

	// 正在终止的 Pod 数量（处于 Pending 或 Running phase 且带有 deletionTimestamp）。
	// +optional
	Terminating *int32 `json:"terminating,omitempty" protobuf:"varint,11,opt,name=terminating"`

	// completedIndexes 以文本格式保存 .spec.completionMode = "Indexed" 时已完成的 index。
	// index 以逗号分隔的十进制整数表示，按递增顺序排列。三个或以上连续数字会压缩，
	// 用该段的首尾元素加连字符表示。
	// 例如，已完成的 index 为 1、3、4、5 和 7 时，表示为 "1,3-5,7"。
	// +optional
	CompletedIndexes string `json:"completedIndexes,omitempty" protobuf:"bytes,7,opt,name=completedIndexes"`

	// FailedIndexes 保存设置 spec.backoffLimitPerIndex 时的失败 index。
	// 其文本格式与 `completedIndexes` 字段类似：以逗号分隔的十进制整数，按递增顺序排列；
	// 三个或以上连续数字会压缩，用该段的首尾元素加连字符表示。
	// 例如，失败的 index 为 1、3、4、5 和 7 时，表示为 "1,3-5,7"。
	// 失败 index 集合不能与已完成 index 集合重叠。
	//
	// +optional
	FailedIndexes *string `json:"failedIndexes,omitempty" protobuf:"bytes,10,opt,name=failedIndexes"`

	// uncountedTerminatedPods 保存已终止但尚未被 job controller 计入 status 计数器的 Pod UID。
	//
	// job controller 创建 Pod 时会带上 finalizer。Pod 终止（成功或失败）后，
	// controller 分三步将其计入 Job status：
	//
	// 1. 将 Pod UID 加入该字段的数组。
	// 2. 移除 Pod finalizer。
	// 3. 从数组中移除 Pod UID，同时增加相应的计数器。
	//
	// 旧版 Job 可能不使用该字段进行跟踪，此时该字段保持为 null。
	// 已结束 Job 的该结构为空。
	// +optional
	UncountedTerminatedPods *UncountedTerminatedPods `json:"uncountedTerminatedPods,omitempty" protobuf:"bytes,8,opt,name=uncountedTerminatedPods"`

	// 具有 Ready condition 且未终止（没有 deletionTimestamp）的活跃 Pod 数量。
	Ready *int32 `json:"ready,omitempty" protobuf:"varint,9,opt,name=ready"`
}

// UncountedTerminatedPods 保存已终止但尚未计入 Job status 计数器的 Pod UID。
type UncountedTerminatedPods struct {
	// succeeded 保存成功 Pod 的 UID。
	// +listType=set
	// +optional
	Succeeded []types.UID `json:"succeeded,omitempty" protobuf:"bytes,1,rep,name=succeeded,casttype=k8s.io/apimachinery/pkg/types.UID"`

	// failed 保存失败 Pod 的 UID。
	// +listType=set
	// +optional
	Failed []types.UID `json:"failed,omitempty" protobuf:"bytes,2,rep,name=failed,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

// JobConditionType 表示 Job condition 的类型。
type JobConditionType string

// 以下是 Job 的内置 condition。
const (
	// JobSuspended 表示 Job 已被暂停。
	JobSuspended JobConditionType = "Suspended"
	// JobComplete 表示 Job 已完成执行。
	JobComplete JobConditionType = "Complete"
	// JobFailed 表示 Job 执行失败。
	JobFailed JobConditionType = "Failed"
	// JobFailureTarget 表示 Job 即将执行失败。
	JobFailureTarget JobConditionType = "FailureTarget"
	// JobSuccessCriteriaMet 表示 Job 已满足成功条件。
	JobSuccessCriteriaMet JobConditionType = "SuccessCriteriaMet"
)

const (
	// JobReasonPodFailurePolicy 表示由于失败 Pod 命中了 pod failure policy 规则
	// 而添加 Job 失败 condition。
	// https://kep.k8s.io/3329
	JobReasonPodFailurePolicy string = "PodFailurePolicy"
	// JobReasonBackoffLimitExceeded 表示 Job 内 Pod 的失败次数超过了 backOffLimit。
	JobReasonBackoffLimitExceeded string = "BackoffLimitExceeded"
	// JobReasonDeadlineExceeded 表示 Job 运行时长超过了 ActiveDeadline。
	JobReasonDeadlineExceeded string = "DeadlineExceeded"
	// JobReasonMaxFailedIndexesExceeded 表示 Job 的某个 index 失败。
	JobReasonMaxFailedIndexesExceeded string = "MaxFailedIndexesExceeded"
	// JobReasonFailedIndexes 表示 Job 存在失败的 index。
	JobReasonFailedIndexes string = "FailedIndexes"
	// JobReasonSuccessPolicy 表示由于 Job 满足 successPolicy 而添加 SuccessCriteriaMet condition。
	JobReasonSuccessPolicy string = "SuccessPolicy"
	// JobReasonCompletionsReached 表示由于成功的 Job Pod 数量达到 completions 而添加 SuccessCriteriaMet condition。
	JobReasonCompletionsReached string = "CompletionsReached"
)

// JobCondition 描述 Job 的当前状态。
type JobCondition struct {
	// Job condition 的类型，Complete 或 Failed。
	Type JobConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=JobConditionType"`
	// condition 的状态，取 True、False 或 Unknown 之一。
	Status corev1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=k8s.io/api/core/v1.ConditionStatus"`
	// 上次检查该 condition 的时间。
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// condition 上次从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// condition 上次状态转换的简要原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 以人类可读的形式描述上次状态转换的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// JobTemplateSpec 描述从 template 创建 Job 时该 Job 应具有的数据。
type JobTemplateSpec struct {
	// 从该 template 创建的 Job 的标准对象 metadata。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Job 期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec JobSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// CronJob 表示一个 cron job 的配置。
// +k8s:supportsSubresource="/status"
type CronJob struct {
	metav1.TypeMeta `json:""`
	// 标准对象的 metadata。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// cron job 期望行为的规格说明，包括调度计划。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec CronJobSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// cron job 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status CronJobStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// CronJobList 是 cron job 的集合。
type CronJobList struct {
	metav1.TypeMeta `json:""`

	// 标准 list 的 metadata。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CronJob 列表。
	Items []CronJob `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CronJobSpec 描述 Job 的执行方式以及实际运行时间。
type CronJobSpec struct {

	// Cron 格式的调度计划，参见 https://en.wikipedia.org/wiki/Cron.
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Schedule string `json:"schedule" protobuf:"bytes,1,opt,name=schedule"`

	// 给定调度计划所用的时区名称，参见 https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.
	// 未指定时，默认为 kube-controller-manager 进程的时区。
	// 合法时区名称和时区偏移集合由 API server 在 CronJob 校验时、
	// 以及 controller manager 在执行时从系统级时区数据库加载。
	// 如果找不到系统级时区数据库，则改用内置版本。
	// 如果时区名称在 CronJob 生命周期内变为无效，或因主机配置变更而失效，
	// controller 将停止创建新 Job，并创建原因为 UnknownTimeZone 的系统事件。
	// 更多信息参见 https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones
	// +optional
	TimeZone *string `json:"timeZone,omitempty" protobuf:"bytes,8,opt,name=timeZone"`

	// 可选：Job 因任何原因错过调度时间后，允许启动的截止时间（秒）。
	// 错过的 Job 执行会被计为失败。
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" protobuf:"varint,2,opt,name=startingDeadlineSeconds"`

	// 指定如何处理 Job 的并发执行。合法取值包括：
	//
	// - "Allow"（默认）：允许 CronJob 并发运行；
	// - "Forbid"：禁止并发运行，若上一次运行尚未结束则跳过下一次；
	// - "Replace"：取消当前运行的 Job 并用新 Job 替换
	// +optional
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty" protobuf:"bytes,3,opt,name=concurrencyPolicy,casttype=ConcurrencyPolicy"`

	// 该标志告诉 controller 暂停后续执行，不影响已经启动的执行。默认为 false。
	// +optional
	Suspend *bool `json:"suspend,omitempty" protobuf:"varint,4,opt,name=suspend"`

	// 指定执行 CronJob 时将创建的 Job。
	JobTemplate JobTemplateSpec `json:"jobTemplate" protobuf:"bytes,5,opt,name=jobTemplate"`

	// 要保留的成功结束 Job 的数量。该值必须为非负整数，默认为 3。
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty" protobuf:"varint,6,opt,name=successfulJobsHistoryLimit"`

	// 要保留的失败结束 Job 的数量。该值必须为非负整数，默认为 1。
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty" protobuf:"varint,7,opt,name=failedJobsHistoryLimit"`
}

// ConcurrencyPolicy 描述如何处理 Job。
// 以下并发策略只能指定其一。如果均未指定，则默认使用 AllowConcurrent。
// +enum
type ConcurrencyPolicy string

const (
	// AllowConcurrent 允许 CronJob 并发运行。
	AllowConcurrent ConcurrencyPolicy = "Allow"

	// ForbidConcurrent 禁止并发运行，若上一次运行尚未结束则跳过下一次。
	ForbidConcurrent ConcurrencyPolicy = "Forbid"

	// ReplaceConcurrent 取消当前运行的 Job 并用新 Job 替换。
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

// CronJobStatus 表示 cron job 的当前状态。
type CronJobStatus struct {
	// 指向当前运行中 Job 的指针列表。
	// +optional
	// +listType=atomic
	Active []corev1.ObjectReference `json:"active,omitempty" protobuf:"bytes,1,rep,name=active"`

	// Job 上次被成功调度的时间信息。
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty" protobuf:"bytes,4,opt,name=lastScheduleTime"`

	// Job 上次成功完成的时间信息。
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty" protobuf:"bytes,5,opt,name=lastSuccessfulTime"`
}
