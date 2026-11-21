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

package batch

import (
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "k8s.io/kubernetes/pkg/apis/core"
)

const (
	// 无前缀的标签保留给终端用户使用，
	// 因此我们添加 batch.kubernetes.io 前缀，以标明这些标签是 Kubernetes 官方标签。
	// 参见 https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#label-selector-and-annotation-conventions
	labelPrefix = "batch.kubernetes.io/"
	// JobTrackingFinalizer 是 Job 所属 Pod 的终结器，防止这些 Pod 在计入 Job 状态之前被删除。
	//
	// 此外，apiserver 和 job controller 也把该字符串用作 Job 的注解，
	// 用于标记那些正在使用 Pod 终结器进行跟踪的 Job。
	// 不过，这一行为在 Kubernetes 1.26 中已被弃用。这意味着在 1.27 及以后版本中，
	// 即 JobTrackingWithFinalizers 晋升为 GA 之后的一个发布版本，
	// apiserver 和 job controller 将忽略该注解，并始终使用终结器来跟踪 Job。
	JobTrackingFinalizer = labelPrefix + "job-tracking"
	// LegacyJobName 和 LegacyControllerUid 是早期使用无前缀形式设置的遗留标签。
	LegacyJobNameLabel       = "job-name"
	LegacyControllerUidLabel = "controller-uid"
	// JobName 是一种便于用户引用 Job 的方式，会被设置到 Job 的标签中。
	JobNameLabel = labelPrefix + LegacyJobNameLabel
	// Controller UID 用于 Job 的选择器和标签。
	ControllerUidLabel = labelPrefix + LegacyControllerUidLabel
	// 该注解记录与 Pod 对应的索引的失败次数，这些失败次数会计入退避上限（backoff limit）。
	JobIndexFailureCountAnnotation = labelPrefix + "job-index-failure-count"
	// 该注解记录与 Pod 对应的索引的失败次数，根据 Pod 失败策略，这些失败次数
	// 不计入退避上限。如果该注解不存在，则默认为零。
	JobIndexIgnoredFailureCountAnnotation = labelPrefix + "job-index-ignored-failure-count"
	// JobControllerName 是内置 Job 控制器为 managedBy 字段保留的值。
	JobControllerName = "kubernetes.io/job-controller"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Job 表示单个 Job 的配置。
type Job struct {
	metav1.TypeMeta
	// 对象的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 描述 Job 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec JobSpec

	// Job 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status JobStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// JobList 是 Job 的集合。
type JobList struct {
	metav1.TypeMeta
	// 列表的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 Job 列表。
	Items []Job
}

// JobTemplateSpec 描述从模板创建 Job 时该 Job 应具备的数据。
type JobTemplateSpec struct {
	// 从该模板创建的 Job 的标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 描述 Job 期望行为的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec JobSpec
}

// CompletionMode 指定如何跟踪 Job 中 Pod 的完成状态。
type CompletionMode string

const (
	// NonIndexedCompletion 是一种 Job 完成模式。在该模式下，当成功完成的 Pod 数量
	// 达到 .spec.completions 时，Job 即被视为完成。各个 Pod 的完成是等价的。
	NonIndexedCompletion CompletionMode = "NonIndexed"

	// IndexedCompletion 是一种 Job 完成模式。在该模式下，Job 的 Pod 会获得一个
	// 从 0 到 (.spec.completions - 1) 的关联完成索引。
	// 当每个完成索引都有一个 Pod 成功完成时，Job 即被视为完成。
	IndexedCompletion CompletionMode = "Indexed"
)

// PodFailurePolicyAction 指定如何处理 Pod 失败。
// +enum
type PodFailurePolicyAction string

const (
	// 这是一种可针对 Pod 失败采取的动作：将 Pod 所属的 Job 标记为 Failed，并终止所有正在运行的 Pod。
	PodFailurePolicyActionFailJob PodFailurePolicyAction = "FailJob"

	// 这是一种可针对 Pod 失败采取的动作：将对应的 Job 索引标记为失败，
	// 以避免在该索引内重启。该动作只能在设置了 backoffLimitPerIndex 时使用。
	PodFailurePolicyActionFailIndex PodFailurePolicyAction = "FailIndex"

	// 这是一种可针对 Pod 失败采取的动作：不增加计入 .backoffLimit 的计数
	// （即 Job 的 .status.failed 字段），并创建一个新的 Pod 来替代它。
	PodFailurePolicyActionIgnore PodFailurePolicyAction = "Ignore"

	// 这是一种可针对 Pod 失败采取的动作：按默认方式处理该 Pod 失败，
	// 即增加计入 .backoffLimit 的计数（Job 的 .status.failed 字段）。
	PodFailurePolicyActionCount PodFailurePolicyAction = "Count"
)

// PodFailurePolicyOnExitCodesOperator 表示容器退出码与指定值之间的关系运算符。
// +enum
type PodFailurePolicyOnExitCodesOperator string

const (
	// PodFailurePolicyOnExitCodesOpIn 表示退出码必须位于指定值集合中。
	PodFailurePolicyOnExitCodesOpIn PodFailurePolicyOnExitCodesOperator = "In"
	// PodFailurePolicyOnExitCodesOpNotIn 表示退出码必须不在指定值集合中。
	PodFailurePolicyOnExitCodesOpNotIn PodFailurePolicyOnExitCodesOperator = "NotIn"
)

// PodReplacementPolicy 指定创建替代 Pod 的策略。
// +enum
type PodReplacementPolicy string

const (
	// TerminatingOrFailed 表示当 Pod 正在终止（具有 metadata.deletionTimestamp）或已失败时，重新创建该 Pod。
	TerminatingOrFailed PodReplacementPolicy = "TerminatingOrFailed"
	// Failed 表示要等待先前创建的 Pod 完全终止（进入 Failed 或 Succeeded 阶段）后，再创建替代 Pod。
	Failed PodReplacementPolicy = "Failed"
)

// PodFailurePolicyOnExitCodesRequirement 描述基于容器退出码处理失败 Pod 的条件。
// 具体来说，它会查找每个应用容器和 init 容器状态中的 .state.terminated.exitCode，
// 这两个状态分别由 Pod 状态中的 .status.containerStatuses 和
// .status.initContainerStatuses 字段表示。成功完成（退出码为 0）的容器
// 不参与该条件的检查。
type PodFailurePolicyOnExitCodesRequirement struct {
	// 将退出码检查限制在具有指定名称的容器上。为 null 时，该规则适用于所有容器。
	// 如果指定了该字段，它必须匹配 Pod 模板中的某个 container 或 initContainer 名称。
	// +optional
	ContainerName *string

	// 表示容器退出码与指定值之间的关系。成功完成（退出码为 0）的容器
	// 不参与该条件的检查。可能的值包括：
	//
	// - In：如果至少有一个容器退出码（当存在多个未被 'containerName' 字段
	//   限制的容器时可能有多个）位于指定值集合中，则条件满足。
	// - NotIn：如果至少有一个容器退出码（当存在多个未被 'containerName' 字段
	//   限制的容器时可能有多个）不在指定值集合中，则条件满足。
	// 将来可能会添加更多值。客户端遇到未知运算符时，应认为该条件不满足。
	Operator PodFailurePolicyOnExitCodesOperator

	// 指定值的集合。每个返回的容器退出码（如果有多个容器则可能有多个）
	// 都会按照该运算符与这个值集合进行比较。值列表必须有序，
	// 且不得包含重复项。In 运算符不能使用值 '0'。
	// 至少需要填写一个元素，最多允许 255 个元素。
	// +listType=set
	Values []int32
}

// PodFailurePolicyOnPodConditionsPattern 描述用于匹配实际 Pod 条件类型的模式。
type PodFailurePolicyOnPodConditionsPattern struct {
	// 指定所需的 Pod 条件类型。要匹配某个 Pod 条件，指定的类型必须与该 Pod 条件的类型相等。
	Type api.PodConditionType
	// 指定所需的 Pod 条件状态。要匹配某个 Pod 条件，指定的状态必须与该 Pod 条件的状态相等。
	// 默认为 True。
	// +optional
	Status api.ConditionStatus
}

// PodFailurePolicyRule 描述当条件满足时如何处理 Pod 失败。
// 每条规则中只能使用 OnExitCodes 和 onPodConditions 其中之一，不能同时使用。
type PodFailurePolicyRule struct {
	// 指定当条件满足时针对 Pod 失败所采取的动作。可能的值包括：
	//
	// - FailJob：表示将 Pod 所属的 Job 标记为 Failed，并终止所有正在运行的 Pod。
	// - FailIndex：表示将 Pod 对应的索引标记为 Failed，且不再重启该索引。
	// - Ignore：表示不增加计入 .backoffLimit 的计数，并创建替代 Pod。
	// - Count：表示按默认方式处理该 Pod，即增加计入 .backoffLimit 的计数。
	// 将来可能会添加更多值。客户端遇到未知动作时应跳过该规则。
	Action PodFailurePolicyAction

	// 表示对容器退出码的条件。
	// +optional
	OnExitCodes *PodFailurePolicyOnExitCodesRequirement

	// 表示对 Pod 条件的条件。该条件以 Pod 条件模式列表的形式表示。
	// 如果至少有一个模式匹配了实际的 Pod 条件，则该条件满足。最多允许 20 个元素。
	// +listType=atomic
	// +optional
	OnPodConditions []PodFailurePolicyOnPodConditionsPattern
}

// PodFailurePolicy 描述失败的 Pod 如何影响 backoffLimit。
type PodFailurePolicy struct {
	// Pod 失败策略规则列表。规则按顺序求值。
	// 一旦某条规则匹配了某个 Pod 失败，其余规则将被忽略。
	// 如果没有任何规则匹配该 Pod 失败，则采用默认处理：增加 Pod 失败计数，
	// 并将其与 backoffLimit 进行比较。最多允许 20 个元素。
	// +listType=atomic
	Rules []PodFailurePolicyRule
}

// SuccessPolicy 描述在什么情况下可以基于部分索引的成功将 Job 声明为成功。
type SuccessPolicy struct {
	// rules 表示一组可选规则，用于在 `.status.succeeded >= .spec.completions` 之前
	// 将 Job 声明为成功。一旦满足其中任意规则，就会添加 "SuccessCriteriaMet" 条件，
	// 并清理残留的 Pod。此类 Job 的终止状态带有 "Complete" 条件。
	// 此外，这些规则按顺序求值；一旦 Job 满足其中一条规则，其他规则将被忽略。
	// 最多允许 20 个元素。
	// +listType=atomic
	Rules []SuccessPolicyRule
}

// SuccessPolicyRule 描述将 Job 声明为成功的规则。
// 每条规则必须至少指定 "succeededIndexes" 或 "succeededCount" 之一。
type SuccessPolicyRule struct {
	// succeededIndexes 指定一组索引，这些索引必须包含在 Job 实际的成功索引集合中。
	// 索引列表必须位于 0 到 ".spec.completions-1" 之间，且不得包含重复项。
	// 至少需要填写一个元素。
	// 索引以逗号分隔的区间形式表示。
	// 区间可以是十进制整数，也可以是连字符分隔的一对十进制整数。
	// 数字由该系列的第一个和最后一个元素表示，中间用连字符连接。
	// 例如，如果完成的索引是 1、3、4、5 和 7，则表示为 "1,3-5,7"。
	// 当该字段为 null 时，它不会默认取任何值，也不会在任何时候被求值。
	//
	// +optional
	SucceededIndexes *string

	// succeededCount 指定 Job 实际成功索引集合所需的最小规模。
	// 当 succeededCount 与 succeededIndexes 一起使用时，检查仅限于
	// succeededIndexes 指定的索引集合。
	// 例如，若 succeededIndexes 为 "1-4"，succeededCount 为 "3"，
	// 而完成的索引为 "1"、"3" 和 "5"，则 Job 不会被声明为成功，
	// 因为该规则只考虑 "1" 和 "3" 这两个索引。
	// 当该字段为 null 时，它不会默认取任何值，也不会在任何时候被求值。
	// 如果指定，则必须为正整数。
	//
	// +optional
	SucceededCount *int32
}

// JobSpec 描述 Job 的执行方式。
type JobSpec struct {

	// 指定 Job 在任意时刻应运行的 Pod 的最大期望数量。
	// 当 ((.spec.completions - .status.successful) < .spec.parallelism) 时，
	// 即剩余工作量小于最大并行度时，稳态下实际运行的 Pod 数量会少于该值。
	// +optional
	Parallelism *int32

	// 指定 Job 期望成功完成的 Pod 数量。
	// 设置为 null 表示任意一个 Pod 成功即代表所有 Pod 成功，
	// 且并行度可以是任意正值。
	// 设置为 1 表示并行度被限制为 1，且该 Pod 成功即表示 Job 成功。
	// +optional
	Completions *int32

	// 指定从 startTime 起算的持续时间（秒），Job 在此期间可以保持活跃，
	// 超过后系统将尝试终止它；该值必须为正整数。
	// 如果 Job 被暂停（在创建时或通过更新），该计时器实际上会停止，
	// 并在 Job 恢复时重新计时。
	// +optional
	ActiveDeadlineSeconds *int64

	// 指定处理失败 Pod 的策略。具体来说，它可以指定一组动作以及触发相应动作
	// 所需满足的条件。
	// 如果为空，则采用默认行为：增加失败 Pod 的计数（即 Job 的 .status.failed 字段），
	// 并将其与 backoffLimit 进行比较。
	// 该字段不能与 .spec.podTemplate.spec.restartPolicy=OnFailure 同时使用。
	//
	// +optional
	PodFailurePolicy *PodFailurePolicy

	// successPolicy 指定在什么情况下可以将 Job 声明为成功。
	// 如果为空，则采用默认行为：只有当成功的 Pod 数量等于 completions 时，
	// Job 才会被声明为成功。
	// 如果指定了该字段，则它必须是不可变的，且仅对 Indexed 类型的 Job 有效。
	// 一旦 Job 满足 SuccessPolicy，残留的 Pod 将被终止。
	//
	// +optional
	SuccessPolicy *SuccessPolicy

	// 指定在将 Job 标记为失败之前的重试次数。
	// 默认为 6，除非指定了 backoffLimitPerIndex（仅适用于 Indexed Job）。
	// 当指定了 backoffLimitPerIndex 时，backoffLimit 默认为 2147483647。
	// +optional
	BackoffLimit *int32

	// 指定在某个索引内将索引标记为失败之前的重试次数上限。
	// 启用后，每个索引的失败次数会保存在 Pod 的
	// batch.kubernetes.io/job-index-failure-count 注解中。
	// 只有在 Job 的 completionMode=Indexed 且 Pod 的重启策略为 Never 时才能设置该字段。
	// 该字段不可变。
	// +optional
	BackoffLimitPerIndex *int32

	// 当设置了 backoffLimitPerIndex 时，该字段指定在将 Job 标记为失败之前
	// 允许的失败索引的最大数量。一旦失败索引数量超过该值，整个 Job 将被标记为 Failed
	// 并终止执行。如果为 null，Job 会继续执行所有索引，并最终被标记为 `Complete` 条件。
	// 只有在设置了 backoffLimitPerIndex 时才能指定该字段。
	// 该值可以是 null，或不超过 completions。当 completions 大于 10^5 时，
	// 该字段必填且必须小于或等于 10^4。
	// +optional
	MaxFailedIndexes *int32

	// TODO 待 https://github.com/kubernetes/kubernetes/issues/28486 修复后启用
	// 要保留的失败 Pod 的可选数量。
	// +optional
	// FailedPodsLimit *int32

	// 用于对 Pod 进行筛选的标签查询，这些 Pod 的数量需要与 Job 的完成数量相匹配。
	// 通常，系统会自动为你设置该字段。
	// +optional
	Selector *metav1.LabelSelector

	// manualSelector 控制 Pod 标签和 Pod 选择器的生成方式。
	// 除非你非常清楚自己在做什么，否则不要设置 `manualSelector`。
	// 当为 false 或未设置时，系统会为此 Job 挑选唯一的标签，
	// 并将这些标签追加到 Pod 模板中。当为 true 时，
	// 用户需要自行挑选唯一的标签并指定选择器。
	// 如果挑选的标签不唯一，可能导致此 Job 和其他 Job 无法正常工作。
	// 不过，在使用旧的 `extensions/v1beta1` API 创建的 Job 中，
	// 可能会看到 `manualSelector=true`。
	// +optional
	ManualSelector *bool

	// 描述执行 Job 时将创建的 Pod。
	// template.spec.restartPolicy 仅允许取值为 "Never" 或 "OnFailure"。
	Template api.PodTemplateSpec

	// ttlSecondsAfterFinished 限制已完成执行（Complete 或 Failed）的 Job 的存活时间。
	// 如果设置了该字段，则在 Job 完成 ttlSecondsAfterFinished 秒后，
	// 该 Job 就符合自动删除的条件。
	// 删除 Job 时，其生命周期保证（例如终结器）仍会被遵守。
	// 如果未设置该字段，Job 不会被自动删除。
	// 如果该字段设置为零，则 Job 完成后立即符合删除条件。
	// +optional
	TTLSecondsAfterFinished *int32

	// completionMode 指定如何跟踪 Pod 的完成状态。它可以是 `NonIndexed`（默认）
	// 或 `Indexed`。
	//
	// `NonIndexed` 表示当成功完成的 Pod 数量达到 .spec.completions 时，
	// Job 即被视为完成。每个 Pod 的完成都是等价的。
	//
	// `Indexed` 表示 Job 的 Pod 会获得一个从 0 到 (.spec.completions - 1) 的
	// 关联完成索引，该索引记录在注解 batch.kubernetes.io/job-completion-index 中。
	// 当每个索引都有一个成功完成的 Pod 时，Job 即被视为完成。
	// 当取值为 `Indexed` 时，必须指定 .spec.completions，
	// 且 `.spec.parallelism` 必须小于或等于 10^5。
	// 此外，Pod 名称的形式为 `$(job-name)-$(index)-$(random-string)`，
	// Pod 主机名的形式为 `$(job-name)-$(index)`。
	//
	// 将来可能会添加更多完成模式。
	// 如果 Job 控制器遇到无法识别的模式（在升级过程中由于版本偏差可能发生这种情况），
	// 控制器将跳过对该 Job 的更新。
	// +optional
	CompletionMode *CompletionMode

	// suspend 指定 Job 控制器是否应创建 Pod。
	// 如果创建 Job 时 suspend 设置为 true，则 Job 控制器不会创建任何 Pod。
	// 如果 Job 在创建后被暂停（即该标志从 false 变为 true），
	// Job 控制器将删除与该 Job 关联的所有活跃 Pod。
	// 用户必须设计自己的工作负载以优雅地处理这种情况。
	// 暂停 Job 会重置 Job 的 StartTime 字段，实际上也会重置 ActiveDeadlineSeconds 计时器。
	// 默认为 false。
	//
	// +optional
	Suspend *bool

	// podReplacementPolicy 指定何时创建替代 Pod。可能的值包括：
	// - TerminatingOrFailed 表示当 Pod 正在终止（具有 metadata.deletionTimestamp）
	//   或已失败时重新创建 Pod。
	// - Failed 表示要等待先前创建的 Pod 完全终止（进入 Failed 或 Succeeded 阶段）
	//   后再创建替代 Pod。
	//
	// 使用 podFailurePolicy 时，只能取 Failed。
	// 不使用 podFailurePolicy 时，TerminatingOrFailed 和 Failed 都是允许的值。
	// +optional
	PodReplacementPolicy *PodReplacementPolicy

	// ManagedBy 字段指明管理 Job 的控制器。
	// k8s Job 控制器会协调未设置该字段，或该字段值为保留字符串
	// `kubernetes.io/job-controller` 的 Job，但会跳过协调该字段为自定义值的 Job。
	// 该值必须是有效的带域名前缀的路径（例如 acme.io/foo）：
	// 第一个 "/" 之前的所有字符必须是 RFC 1123 定义的有效子域名，
	// 第一个 "/" 之后的所有字符必须是 RFC 3986 定义的有效 HTTP 路径字符。
	// 该值不能超过 63 个字符。
	// 该字段不可变。
	// +optional
	ManagedBy *string

	// scheduling 定义该 Job 的工作负载感知调度配置。
	// 设置后，它指定调度策略（basic 或 gang）、拓扑约束、中断模式和共享资源声明。
	// 如果省略，Job 默认采用 basic 调度策略，其行为与标准的逐 Pod 调度相同。
	// 该字段处于 alpha 阶段，需要启用 WorkloadWithJob 特性门控。
	// 一旦设置，该字段不可变，但 policy.gang.minCount 除外。
	//
	// +featureGate=WorkloadWithJob
	// +optional
	Scheduling *JobSchedulingConfiguration
}

// JobSchedulingConfiguration 组合来自 scheduling.k8s.io 的可复用调度构件，
// 供 Job 使用。
type JobSchedulingConfiguration struct {
	// SchedulingPolicy 定义该 Job 的调度策略。
	// Basic 或 Gang 必须且只能设置其中一个。
	//
	// +optional
	SchedulingPolicy *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy

	// SchedulingConstraints 为 Job 的 Pod 定义可选的调度约束（例如拓扑约束）。
	//
	// +optional
	SchedulingConstraints *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints

	// DisruptionMode 定义 Job 的 Pod 可以被中断的方式。
	// 取值为 Single 或 All 之一。
	//
	// +optional
	DisruptionMode *schedulingv1alpha3.WorkloadPodGroupDisruptionMode

	// ResourceClaims 定义哪些 ResourceClaim 可以在 Job 的 Pod 之间共享。
	// Pod 通过在自身 Spec.ResourceClaims 中定义一个与 PodGroup 的 claim 完全匹配的
	// claim，来消费分配给该 PodGroup claim 的设备。
	// 该 claim 必须名称相同，并引用同一个 ResourceClaim 或 ResourceClaimTemplate。
	// 最多可以设置 4 个 claim，与生成的 PodGroup 的限制保持一致。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	ResourceClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim
}

// JobStatus 表示 Job 的当前状态。
type JobStatus struct {

	// 对对象当前状态的最新可用观测信息。当 Job 失败时，其中一个条件的类型为 "Failed"
	// 且 status 为 true。当 Job 被暂停时，其中一个条件的类型为 "Suspended"
	// 且 status 为 true；当 Job 恢复时，该条件的 status 会变为 false。
	// 当 Job 完成时，其中一个条件的类型为 "Complete" 且 status 为 true。
	//
	// 当 Job 处于终止状态（"Complete" 或 "Failed"）时，被视为已完成。
	// Job 不能同时具有 "Complete" 和 "Failed" 条件。
	// 此外，也不能同时处于 "Complete" 和 "FailureTarget" 条件。
	// "Complete"、"Failed" 和 "FailureTarget" 条件不能被禁用。
	//
	// +optional
	Conditions []JobCondition

	// 表示 job controller 开始处理 Job 的时间。
	// 如果 Job 以暂停状态创建，则该字段直到首次恢复时才会设置。
	// 每次从暂停状态恢复 Job 时，该字段都会被重置。
	// 该字段以 RFC3339 格式表示，且为 UTC 时间。
	//
	// 一旦设置，只有在 Job 被暂停时才能移除该字段。
	// 当 Job 未暂停或已完成时，不能修改该字段。
	//
	// +optional
	StartTime *metav1.Time

	// 表示 Job 完成的时间。不保证在不同操作之间按 happens-before 顺序设置。
	// 该字段以 RFC3339 格式表示，且为 UTC 时间。
	// 完成时间仅在 Job 成功完成时设置，除此之外不会设置。
	// 该值不能被更新或移除。该值表示的时间点与 startTime 字段相同或更晚。
	// +optional
	CompletionTime *metav1.Time

	// 处于 pending 或 running 状态且未终止（没有 deletionTimestamp）的 Pod 数量。
	// 对于已完成的 Job，该值为零。
	// +optional
	Active int32

	// 进入 Succeeded 阶段的 Pod 数量。
	// 对于给定的 spec，该值单调递增。不过，当弹性索引 Job 缩容时，
	// 该值可能会减少。
	// +optional
	Succeeded int32

	// 进入 Failed 阶段的 Pod 数量。
	// 该值单调递增。
	// +optional
	Failed int32

	// 正在终止的 Pod 数量（处于 Pending 或 Running 阶段且具有 deletionTimestamp）。
	// +optional
	Terminating *int32

	// 当 .spec.completionMode = "Indexed" 时，completedIndexes 以文本格式
	// 保存已完成的索引。索引以逗号分隔的十进制整数表示，并按递增顺序列出。
	// 三个或更多连续的数字会被压缩，用该系列的第一个和最后一个元素表示，
	// 中间以连字符连接。
	// 例如，如果完成的索引是 1、3、4、5 和 7，则表示为 "1,3-5,7"。
	// +optional
	CompletedIndexes string

	// 当设置了 spec.backoffLimitPerIndex 时，FailedIndexes 保存失败的索引。
	// 索引的文本格式与 `completedIndexes` 字段类似，即以逗号分隔的十进制整数表示，
	// 并按递增顺序列出。三个或更多连续的数字会被压缩，
	// 用该系列的第一个和最后一个元素表示，中间以连字符连接。
	// 例如，如果失败的索引是 1、3、4、5 和 7，则表示为 "1,3-5,7"。
	// 失败索引集合不能与完成索引集合重叠。
	//
	// +optional
	FailedIndexes *string

	// uncountedTerminatedPods 保存已终止但尚未被 job controller 计入状态计数器的 Pod 的 UID。
	//
	// job controller 创建 Pod 时会带有终结器。当 Pod 终止（成功或失败）时，
	// 控制器会分三步将其计入 Job 状态：
	//
	// 1. 将 Pod 的 UID 添加到该字段中对应的数组。
	// 2. 移除 Pod 的终结器。
	// 3. 从数组中移除该 Pod 的 UID，同时增加相应的计数器。
	//
	// 这里顺序很关键：先记录 UID 再摘除终结器，避免 Pod 被提前删除而漏计。
	// 旧的 Job 可能没有使用该字段进行跟踪，此时该字段保持为 null。
	// 对于已完成的 Job，该结构体为空。
	// +optional
	UncountedTerminatedPods *UncountedTerminatedPods

	// 具有 Ready 条件且未终止（没有 deletionTimestamp）的活跃 Pod 数量。
	// +optional
	Ready *int32
}

// UncountedTerminatedPods 保存已终止但尚未计入 Job 状态计数器的 Pod 的 UID。
type UncountedTerminatedPods struct {
	// succeeded 保存成功 Pod 的 UID。
	// +listType=set
	// +optional
	Succeeded []types.UID

	// failed 保存失败 Pod 的 UID。
	// +listType=set
	// +optional
	Failed []types.UID
}

// JobConditionType 是 JobCondition.Type 的有效取值。
type JobConditionType string

// 以下是 Job 的有效条件。
const (
	// JobSuspended 表示 Job 已被暂停。
	JobSuspended JobConditionType = "Suspended"
	// JobComplete 表示 Job 已完成执行。
	JobComplete JobConditionType = "Complete"
	// JobFailed 表示 Job 执行失败。
	JobFailed JobConditionType = "Failed"
	// FailureTarget 表示 Job 即将执行失败。
	JobFailureTarget JobConditionType = "FailureTarget"
	// JobSuccessCriteriaMet 表示 Job 已达到成功状态，将被标记为 Completed。
	JobSuccessCriteriaMet JobConditionType = "SuccessCriteriaMet"
)

// JobCondition 描述 Job 的当前状态。
type JobCondition struct {
	// Job 条件的类型。
	Type JobConditionType
	// 条件的状态，取值为 True、False 或 Unknown 之一。
	Status api.ConditionStatus
	// 上次检查该条件的时间。
	// +optional
	LastProbeTime metav1.Time
	// 上次条件从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time
	// 条件上次状态转变的（简要）原因。
	// +optional
	Reason string
	// 便于人类阅读的消息，说明上次状态转变的详细信息。
	// +optional
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CronJob 表示单个 CronJob 的配置。
type CronJob struct {
	metav1.TypeMeta
	// 对象的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 描述 CronJob 期望行为的规格，包括调度计划。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec CronJobSpec

	// CronJob 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status CronJobStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CronJobList 是 CronJob 的集合。
type CronJobList struct {
	metav1.TypeMeta
	// 列表的标准元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 CronJob 列表。
	Items []CronJob
}

// CronJobSpec 描述 Job 的执行方式以及实际运行时间。
type CronJobSpec struct {

	// Cron 格式的调度计划，参见 https://en.wikipedia.org/wiki/Cron。
	Schedule string

	// 给定调度计划所用的时区名称，参见 https://en.wikipedia.org/wiki/List_of_tz_database_time_zones。
	// 如果未指定，则默认为 kube-controller-manager 进程所在的时区。
	// 有效时区名称集合和时区偏移量由 API server 在 CronJob 校验时、
	// 以及 controller manager 在执行时从系统级时区数据库中加载。
	// 如果找不到系统级时区数据库，则改用内置的数据库版本。
	// 如果时区名称在 CronJob 生命周期内变得无效，或由于主机配置变更而失效，
	// 控制器将停止创建新 Job，并创建原因为 UnknownTimeZone 的系统事件。
	// 更多信息请参见 https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones
	// +optional
	TimeZone *string

	// 可选参数，表示如果 Job 因任何原因错过了计划执行时间，
	// 允许启动该 Job 的截止时间（秒）。错过的 Job 执行将被计为失败。
	// +optional
	StartingDeadlineSeconds *int64

	// 指定如何处理 Job 的并发执行。有效值包括：
	//
	// - "Allow"（默认）：允许 CronJob 并发运行；
	// - "Forbid"：禁止并发运行，如果上一次运行尚未结束则跳过下一次运行；
	// - "Replace"：取消当前正在运行的 Job，并用新 Job 替换它。
	// +optional
	ConcurrencyPolicy ConcurrencyPolicy

	// 该标志告诉控制器暂停后续的执行，但不影响已经开始的执行。默认为 false。
	// +optional
	Suspend *bool

	// 指定执行 CronJob 时将创建的 Job。
	JobTemplate JobTemplateSpec

	// 要保留的成功完成的 Job 数量。
	// 该字段为指针类型，用于区分显式设置为零和未指定。
	// +optional
	SuccessfulJobsHistoryLimit *int32

	// 要保留的失败完成的 Job 数量。
	// 该字段为指针类型，用于区分显式设置为零和未指定。
	// +optional
	FailedJobsHistoryLimit *int32
}

// ConcurrencyPolicy 描述如何处理 Job 的并发。
// 只能指定以下并发策略中的一种。
// 如果未指定任何策略，则默认使用 AllowConcurrent。
type ConcurrencyPolicy string

const (
	// AllowConcurrent 允许 CronJob 并发运行。
	AllowConcurrent ConcurrencyPolicy = "Allow"

	// ForbidConcurrent 禁止并发运行，如果上一次运行尚未结束则跳过下一次运行。
	ForbidConcurrent ConcurrencyPolicy = "Forbid"

	// ReplaceConcurrent 取消当前正在运行的 Job，并用新 Job 替换它。
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

// CronJobStatus 表示 CronJob 的当前状态。
type CronJobStatus struct {
	// 指向当前正在运行的 Job 的指针列表。
	// +optional
	Active []api.ObjectReference

	// 上次成功调度 Job 的时间信息。
	// +optional
	LastScheduleTime *metav1.Time

	// 上次成功完成 Job 的时间信息。
	// +optional
	LastSuccessfulTime *metav1.Time
}
