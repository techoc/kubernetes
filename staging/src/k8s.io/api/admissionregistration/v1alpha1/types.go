/*
Copyright 2022 The Kubernetes Authors.

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
	v1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Rule 是 APIGroups、APIVersion 和 Resources 的元组。建议确保所有元组展开均有效。
type Rule = v1.Rule

// ScopeType 指定 Rule 的作用范围。
// +enum
type ScopeType = v1.ScopeType

const (
	// ClusterScope 表示范围限于集群级对象。Namespace 对象属于集群级。
	ClusterScope ScopeType = v1.ClusterScope
	// NamespacedScope 表示范围限于命名空间级对象。
	NamespacedScope ScopeType = v1.NamespacedScope
	// AllScopes 表示包含所有范围。
	AllScopes ScopeType = v1.AllScopes
)

// ParameterNotFoundActionType 指定失败策略，定义在 binding 引用的参数找不到时
// 如何评估该 binding。
// +enum
type ParameterNotFoundActionType string

const (
	// Allow 表示查找 binding 参数出错时被忽略。
	AllowAction ParameterNotFoundActionType = "Allow"
	// Deny 表示查找 binding 参数出错时拒绝请求。
	DenyAction ParameterNotFoundActionType = "Deny"
)

// FailurePolicyType 指定失败策略，定义如何处理来自准入端点的无法识别的错误。
// +enum
type FailurePolicyType string

const (
	// Ignore 表示调用准入 webhook 或准入策略出错时被忽略。
	Ignore FailurePolicyType = "Ignore"
	// Fail 表示调用准入 webhook 或准入策略出错会导致资源准入失败。
	Fail FailurePolicyType = "Fail"
)

// MatchPolicyType 指定匹配策略的类型。
// +enum
type MatchPolicyType string

const (
	// Exact 表示请求只有在完全匹配给定规则时才发送给准入 webhook 或准入策略。
	Exact MatchPolicyType = "Exact"
	// Equivalent 表示请求通过等价的 API 组或版本修改了 rules 中列出的资源时，
	// 也会发送给准入 webhook 或准入策略。例如 `autoscaling/v1` 和 `autoscaling/v2`
	// 的 HorizontalPodAutoscaler 是等价的：两个 API 呈现的是同一组资源。
	Equivalent MatchPolicyType = "Equivalent"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26

// ValidatingAdmissionPolicy 描述一种准入验证策略，它接受或拒绝对象而不修改对象。
// +k8s:supportsSubresource="/status"
type ValidatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 ValidatingAdmissionPolicy 的期望行为。
	Spec ValidatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// status 表示 ValidatingAdmissionPolicy 的当前状态，包括有助于判断策略是否
	// 按预期运行的警告信息。
	// 由系统填充。
	// 只读。
	// +optional
	Status ValidatingAdmissionPolicyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ValidatingAdmissionPolicyStatus 表示 ValidatingAdmissionPolicy 的状态。
type ValidatingAdmissionPolicyStatus struct {
	// observedGeneration 是控制器观察到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`
	// typeChecking 包含每个表达式的类型检查结果。
	// 此字段存在表示类型检查已完成。
	// +optional
	TypeChecking *TypeChecking `json:"typeChecking,omitempty" protobuf:"bytes,2,opt,name=typeChecking"`
	// conditions 表示对策略当前状态的最新可用观察。
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" protobuf:"bytes,3,rep,name=conditions"`
}

// TypeChecking 包含对 ValidatingAdmissionPolicy 中表达式进行类型检查的结果。
type TypeChecking struct {
	// expressionWarnings 包含每个表达式的类型检查警告。
	// +optional
	// +listType=atomic
	ExpressionWarnings []ExpressionWarning `json:"expressionWarnings,omitempty" protobuf:"bytes,1,rep,name=expressionWarnings"`
}

// ExpressionWarning 是针对某个特定表达式的警告信息。
type ExpressionWarning struct {
	// fieldRef 是指向该表达式的字段路径。
	// 例如，validations 中第一项的表达式引用路径为 "spec.validations[0].expression"。
	FieldRef string `json:"fieldRef" protobuf:"bytes,2,opt,name=fieldRef"`
	// warning 以人类可读形式包含类型检查信息。
	// 每行包含表达式被检查时所对照的类型，以及编译器给出的类型检查错误。
	Warning string `json:"warning" protobuf:"bytes,3,opt,name=warning"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26

// ValidatingAdmissionPolicyList 是 ValidatingAdmissionPolicy 的列表。
type ValidatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 的列表。
	Items []ValidatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicySpec 指定 AdmissionPolicy 期望行为的规格。
type ValidatingAdmissionPolicySpec struct {
	// paramKind 指定用于参数化此策略的资源 kind。
	// 若缺失，则此策略没有参数，params CEL 变量不会提供给验证表达式。
	// 若 paramKind 指向不存在的 kind，则策略定义配置错误，会应用 FailurePolicy。
	// 若指定了 paramKind 但 ValidatingAdmissionPolicyBinding 中未设置 paramRef，params 变量为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// matchConstraints 指定此策略旨在验证哪些资源。
	// 当请求匹配_所有_约束时，AdmissionPolicy 才关注该请求。
	// 但为防止集群进入无法通过 API 恢复的不稳定状态，
	// ValidatingAdmissionPolicy 不能匹配 ValidatingAdmissionPolicy 和 ValidatingAdmissionPolicyBinding。
	// 必填。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// validations 包含用于执行验证的 CEL 表达式。
	// validations 和 auditAnnotations 不能同时为空，至少需要指定其中一个。
	// +listType=atomic
	// +optional
	Validations []Validation `json:"validations,omitempty" protobuf:"bytes,3,rep,name=validations"`

	// failurePolicy 定义如何处理准入策略的失败。失败可能来自 CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义或绑定。
	//
	// 若 spec.paramKind 指向不存在的 Kind，则策略无效。
	// 若 spec.paramRef.name 指向不存在的资源，则绑定无效。
	//
	// failurePolicy 不定义如何处理评估结果为 false 的验证。
	//
	// 当 failurePolicy 设为 Fail 时，由 ValidatingAdmissionPolicyBinding 的
	// validationActions 定义如何强制执行失败。
	//
	// 允许值为 Ignore 或 Fail。默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// auditAnnotations 包含 CEL 表达式，用于为 API 请求的审计事件生成审计注解。
	// validations 和 auditAnnotations 不能同时为空，至少需要指定其中一个。
	// +listType=atomic
	// +optional
	AuditAnnotations []AuditAnnotation `json:"auditAnnotations,omitempty" protobuf:"bytes,5,rep,name=auditAnnotations"`

	// matchConditions 是请求被验证前必须满足的条件列表。
	// 匹配条件用于过滤已被规则、namespaceSelector 和 objectSelector 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求。最多允许 64 个匹配条件。
	//
	// 若提供了参数对象，可通过 `params` 句柄以与验证表达式相同的方式访问。
	//
	// 精确匹配逻辑（按顺序）为：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过此策略。
	//   2. 若所有 matchCondition 都评估为 TRUE，评估此策略。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝请求
	//      - 若 failurePolicy=Ignore，跳过此策略
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// variables 包含可用于组合其他表达式的变量定义。
	// 每个变量定义为一个具名 CEL 表达式。
	// 此处定义的变量可在策略的其他表达式中通过 `variables` 访问，
	// MatchConditions 除外，因为它在策略其余部分之前评估。
	//
	// 变量的表达式只能引用列表中此前定义的变量，不能引用其后的变量。
	// 因此 Variables 必须按首次出现的顺序排列且无环。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	Variables []Variable `json:"variables,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=variables"`
}

// MatchCondition 是请求被验证前必须匹配的条件。
type MatchCondition v1.MatchCondition

// ParamKind 是 Group、Kind 和 Version 的元组。
// +structType=atomic
type ParamKind struct {
	// apiVersion 是资源所属的 API 组版本，格式为 "group/version"。
	// 必填。
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,1,rep,name=apiVersion"`

	// kind 是资源所属的 API kind。
	// 必填。
	Kind string `json:"kind,omitempty" protobuf:"bytes,2,rep,name=kind"`
}

// Validation 指定用于执行验证的 CEL 表达式。
type Validation struct {
	// expression 是由 CEL 评估的表达式。
	// 参考：https://github.com/google/cel-spec
	// CEL 表达式可访问 API 请求/响应的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自传入请求的对象。DELETE 请求中值为 null。
	// - 'oldObject' - 已存在的对象。CREATE 请求中值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的策略绑定所引用的参数资源。仅当策略指定了 ParamKind 时才填充。
	// - 'namespaceObject' - 传入对象所属的命名空间对象。集群级资源的值为 null。
	// - 'variables' - 组合变量的映射，从变量名到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求主体（用户或 service account）执行授权检查。
	//   详见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问。
	// 其他元数据属性不可访问。
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 可访问的属性名在表达式中访问时按以下规则转义：
	// - '__' 转义为 '__underscores__'
	// - '.' 转义为 '__dot__'
	// - '-' 转义为 '__dash__'
	// - '/' 转义为 '__slash__'
	// - 与 CEL 保留关键字完全匹配的属性名转义为 '__{keyword}__'。这些关键字是：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return"。
	// 示例：
	//   - 访问名为 "namespace" 的属性：{"Expression": "object.__namespace__ > 0"}
	//   - 访问名为 "x-prop" 的属性：{"Expression": "object.x__dash__prop > 0"}
	//   - 访问名为 "redact__d" 的属性：{"Expression": "object.redact__underscores__d > 0"}
	//
	// 对 list 类型为 'set' 或 'map' 的数组，相等比较忽略元素顺序，即 [1, 2] == [2, 1]。
	// 对带有 x-kubernetes-list-type 的数组，连接操作遵循该 list 类型的语义：
	//   - 'set'：`X + Y` 执行并集，保留 `X` 中所有元素的数组位置，
	//     追加 `Y` 中不相交的元素并保持其相对顺序。
	//   - 'map'：`X + Y` 执行合并，保留 `X` 中所有键的数组位置，
	//     当 `X` 和 `Y` 的键集合相交时用 `Y` 中的值覆盖。
	//     `Y` 中键不相交的元素会被追加并保持其相对顺序。
	// 必填。
	Expression string `json:"expression" protobuf:"bytes,1,opt,name=Expression"`
	// message 是验证失败时显示的消息。若 Expression 包含换行，则必须提供 message。
	// message 不得包含换行。
	// 若未设置，消息为 "failed rule: {Rule}"。
	// 例如 "must be a URL with the host matching spec.host"。
	// 若 Expression 包含换行，则 message 必填。
	// message 不得包含换行。
	// 若未设置，消息为 "failed Expression: {Expression}"。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// reason 以机器可读方式描述此验证失败的原因。
	// 若这是列表中第一个失败的验证，此原因以及相应的 HTTP 响应码
	// 会用于返回给客户端的 HTTP 响应。
	// 当前支持的原因有："Unauthorized"、"Forbidden"、"Invalid"、"RequestEntityTooLarge"。
	// 若未设置，给客户端的响应中使用 StatusReasonInvalid。
	// +optional
	Reason *metav1.StatusReason `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// messageExpression 声明一个 CEL 表达式，其求值结果为验证失败时返回的消息。
	// 由于 messageExpression 用作失败消息，它必须求值为字符串。
	// 若验证同时设置了 message 和 messageExpression，验证失败时使用 messageExpression。
	// 若 messageExpression 导致运行时错误，会记录该错误，并按 messageExpression 未设置的方式生成验证失败消息。
	// 若 messageExpression 求值为空字符串、仅含空格的字符串或包含换行的字符串，
	// 也会按 messageExpression 未设置的方式生成验证失败消息，并记录 messageExpression
	// 产生了空字符串/仅含空格的字符串/含换行的字符串这一事实。
	// messageExpression 可访问与 `expression` 相同的所有变量，但不包括 'authorizer' 和 'authorizer.requestResource'。
	// 示例：
	// "object.x must be less than max ("+string(params.max)+")"
	// +optional
	MessageExpression string `json:"messageExpression,omitempty" protobuf:"bytes,4,opt,name=messageExpression"`
}

// Variable 定义用于组合的变量。
type Variable struct {
	// name 是变量的名称。该名称必须是合法的 CEL 标识符，且在所有变量中唯一。
	// 可通过 `variables` 在其他表达式中访问该变量。
	// 例如，若 name 为 "foo"，则该变量可用作 `variables.foo`。
	Name string `json:"name" protobuf:"bytes,1,opt,name=Name"`

	// expression 是求值后作为变量值的表达式。
	// 此 CEL 表达式可访问与 Validation 中 CEL 表达式相同的标识符。
	Expression string `json:"expression" protobuf:"bytes,2,opt,name=Expression"`
}

// AuditAnnotation 描述如何为 API 请求生成审计注解。
type AuditAnnotation struct {
	// key 指定审计注解的键。一个 ValidatingAdmissionPolicy 中的审计注解键必须唯一。
	// 键必须是限定名（[A-Za-z0-9][-A-Za-z0-9_.]*），长度不超过 63 字节。
	//
	// 该键与 ValidatingAdmissionPolicy 的资源名组合，构成审计注解键：
	// "{ValidatingAdmissionPolicy name}/{key}"。
	//
	// 若某个准入 webhook 使用与此 ValidatingAdmissionPolicy 相同的资源名和相同的
	// 审计注解键，则注解键完全相同。此时，第一个写入该键的注解会包含在审计事件中，
	// 后续所有同键注解都会被丢弃。
	//
	// 必填。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`

	// valueExpression 是由 CEL 求值以生成审计注解值的表达式。
	// 该表达式必须求值为字符串或 null。若求值为字符串，则以该字符串值包含审计注解。
	// 若求值为 null 或空字符串，则省略该审计注解。
	// valueExpression 长度不得超过 5kb。
	// 若 valueExpression 的结果超过 10kb，会被截断为 10kb。
	//
	// 若多个 ValidatingAdmissionPolicyBinding 资源匹配同一 API 请求，
	// 会对每个绑定求值 valueExpression。所有 valueExpression 产生的唯一值
	// 会用逗号连接成一个列表。
	//
	// 必填。
	ValueExpression string `json:"valueExpression" protobuf:"bytes,2,opt,name=valueExpression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26

// ValidatingAdmissionPolicyBinding 将 ValidatingAdmissionPolicy 与参数化资源绑定。
// ValidatingAdmissionPolicyBinding 与参数 CRD 共同定义集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个绑定会使其策略被评估 N 次；不使用参数的策略/绑定 N 为 1，
// 否则 N 为该绑定选中的参数数量。
//
// 策略的 CEL 表达式计算出的 CEL 开销必须低于最大 CEL 预算。
// 每次策略评估都有独立的 CEL 开销预算。
// 增加/删除策略、绑定或参数不会影响某个给定（策略、绑定、参数）组合
// 是否在其自身 CEL 预算内。
type ValidatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 ValidatingAdmissionPolicyBinding 的期望行为。
	// +required
	Spec ValidatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26

// ValidatingAdmissionPolicyBindingList 是 ValidatingAdmissionPolicyBinding 的列表。
type ValidatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 的列表。
	Items []ValidatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicyBindingSpec 是 ValidatingAdmissionPolicyBinding 的规格。
type ValidatingAdmissionPolicyBindingSpec struct {
	// policyName 引用 ValidatingAdmissionPolicyBinding 所绑定的 ValidatingAdmissionPolicy 名称。
	// 若被引用的资源不存在，此绑定被视为无效并被忽略。
	// 必填。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// paramRef 指定用于配置准入控制策略的参数资源。
	// 它应指向所绑定 ValidatingAdmissionPolicy 的 ParamKind 中指定类型的资源。
	// 若策略指定了 ParamKind 但 ParamRef 引用的资源不存在，此绑定被视为配置错误，
	// 会应用 ValidatingAdmissionPolicy 的 FailurePolicy。
	// 若策略未指定 ParamKind，则忽略此字段，规则在无参数的情况下评估。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// matchResources 声明哪些资源匹配此绑定并将由其验证。
	// 注意，这与策略的 matchConstraints 取交集，因此只有被策略匹配的请求才能由此选中。
	// 若未设置，则策略匹配的所有资源都由该绑定验证。
	// 当 resourceRules 未设置时，它不限制资源匹配。若资源被本对象的其他字段匹配，则会被验证。
	// 注意这与 ValidatingAdmissionPolicy 的 matchConstraints 不同，后者要求 resourceRules 必填。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`

	// validationActions 声明如何强制执行所引用 ValidatingAdmissionPolicy 的验证。
	// 若验证结果为 false，始终按这些动作强制执行。
	//
	// 由 ValidatingAdmissionPolicy 的 FailurePolicy 定义的失败，仅在 FailurePolicy
	// 设为 Fail 时才按这些动作强制执行，否则忽略这些失败。
	// 这包括编译错误、运行时错误以及策略的配置错误。
	//
	// validationActions 声明为动作值的集合。顺序无关紧要，且不得包含重复动作。
	//
	// 支持的动作值有：
	//
	// "Deny" 表示验证失败导致请求被拒绝。
	//
	// "Warn" 表示验证失败通过 HTTP Warning 头报告给请求客户端，警告码为 299。
	// 允许和拒绝的准入响应都可以携带警告。
	//
	// "Audit" 表示验证失败被包含在为该请求发布的审计事件中。
	// 审计事件将包含一条 `validation.policy.admission.k8s.io/validation_failure`
	// 审计注解，其值包含验证失败的细节，格式为对象组成的 JSON 列表，每个对象包含以下字段：
	// - message：验证失败消息字符串
	// - policy：ValidatingAdmissionPolicy 的资源名
	// - binding：ValidatingAdmissionPolicyBinding 的资源名
	// - expressionIndex：ValidatingAdmissionPolicy 中失败验证的索引
	// - validationActions：针对该验证失败采取的强制动作
	// 审计注解示例：
	// `"validation.policy.admission.k8s.io/validation_failure": "[{\"message\": \"Invalid value\", {\"policy\": \"policy.example.com\", {\"binding\": \"policybinding.example.com\", {\"expressionIndex\": \"1\", {\"validationActions\": [\"Audit\"]}]"`
	//
	// 客户端应预期能处理额外值：忽略任何不认识的值即可。
	//
	// "Deny" 和 "Warn" 不能同时使用，因为这种组合会在
	// API 响应体和 HTTP 警告头中无谓地重复同一验证失败信息。
	//
	// 必填。
	// +listType=set
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	ValidationActions []ValidationAction `json:"validationActions,omitempty" protobuf:"bytes,4,rep,name=validationActions"`
}

// ParamRef 描述如何定位参数，用作策略绑定所应用规则表达式的输入。
// +structType=atomic
type ParamRef struct {
	// name 是被引用资源的名称。
	//
	// `name` 和 `selector` 互斥。设置其中一个时，另一个必须未设置。
	//
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,rep,name=name"`

	// namespace 是被引用资源所在的命名空间，用于将参数搜索限制在特定命名空间。
	// 对 `name` 和 `selector` 字段均适用。
	//
	// 在策略中指定命名空间级的 `paramKind` 并将此字段留空，即可使用按命名空间划分的参数。
	//
	// - 若 `paramKind` 是集群级的，此字段必须未设置。设置此字段会导致配置错误。
	//
	// - 若 `paramKind` 是命名空间级的，此字段未设置时会使用被准入评估对象所在的命名空间。
	//   注意，若留空，该绑定不得匹配任何集群级资源，否则会导致错误。
	//
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,rep,name=namespace"`

	// selector 用于根据标签匹配多个参数对象。使用 selector: {} 可匹配该 ParamKind 的所有资源。
	//
	// 若找到多个参数，它们都会被策略表达式评估，结果按逻辑与合并在一起。
	//
	// `name` 或 `selector` 必须设置其中一个，但两者互斥。设置其中一个时，另一个必须未设置。
	//
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,3,rep,name=selector"`

	// parameterNotFoundAction 控制当资源存在、name 或 selector 有效，
	// 但绑定没有匹配到任何参数时的行为。若设为 `Allow`，则没有匹配参数
	// 会被该绑定视为验证成功。若设为 `Deny`，则没有匹配参数会
	// 受策略的 `failurePolicy` 约束。
	//
	// 允许值为 `Allow` 或 `Deny`。
	// 默认为 `Deny`。
	// +optional
	ParameterNotFoundAction *ParameterNotFoundActionType `json:"parameterNotFoundAction,omitempty" protobuf:"bytes,4,rep,name=parameterNotFoundAction"`
}

// MatchResources 根据对象是否满足匹配条件，决定是否对其运行准入控制策略。
// 排除规则优先于包含规则（若资源同时匹配两者，则被排除）。
// +structType=atomic
type MatchResources struct {
	// namespaceSelector 根据对象所在命名空间是否匹配选择器，决定是否对其运行准入控制策略。
	// 若对象本身是命名空间，则基于 object.metadata.labels 进行匹配。
	// 若对象是其他集群级资源，则永远不会跳过该策略。
	//
	// 例如，若要对命名空间未关联 "runlevel" 为 "0" 或 "1" 的所有对象运行 webhook，
	// 应按如下方式设置选择器：
	// "namespaceSelector": {
	//   "matchExpressions": [
	//     {
	//       "key": "runlevel",
	//       "operator": "NotIn",
	//       "values": [
	//         "0",
	//         "1"
	//       ]
	//     }
	//   ]
	// }
	//
	// 反之，若只想对命名空间关联 "environment" 为 "prod" 或 "staging" 的对象运行策略，
	// 应按如下方式设置选择器：
	// "namespaceSelector": {
	//   "matchExpressions": [
	//     {
	//       "key": "environment",
	//       "operator": "In",
	//       "values": [
	//         "prod",
	//         "staging"
	//       ]
	//     }
	//   ]
	// }
	//
	// 更多标签选择器示例参见
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/。
	//
	// 默认为空的 LabelSelector，匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,1,opt,name=namespaceSelector"`
	// objectSelector 根据对象是否具有匹配的标签，决定是否运行策略。
	// objectSelector 会对将发送给策略表达式（CEL）的 oldObject 和 newObject 都进行评估，
	// 任一对象匹配选择器即视为匹配。null 对象（创建时的 oldObject、删除时的 newObject）
	// 或不能带标签的对象（如 DeploymentRollback 或 PodProxyOptions）不视为匹配。
	// 仅当 webhook 是 opt-in 时才使用 object selector，因为终端用户可以通过设置标签跳过准入 webhook。
	// 默认为空的 LabelSelector，匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,2,opt,name=objectSelector"`
	// resourceRules 描述准入策略匹配哪些资源/子资源上的哪些操作。
	// 当操作匹配_任一_ Rule 时，策略关注该操作。
	// +listType=atomic
	// +optional
	ResourceRules []NamedRuleWithOperations `json:"resourceRules,omitempty" protobuf:"bytes,3,rep,name=resourceRules"`
	// excludeResourceRules 描述策略不应关注哪些资源/子资源上的哪些操作。
	// 排除规则优先于包含规则（若资源同时匹配两者，则被排除）。
	// +listType=atomic
	// +optional
	ExcludeResourceRules []NamedRuleWithOperations `json:"excludeResourceRules,omitempty" protobuf:"bytes,4,rep,name=excludeResourceRules"`
	// matchPolicy 定义如何使用 "MatchResources" 列表匹配传入请求。
	// 允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：仅当请求完全匹配指定规则时才匹配。
	// 例如，若 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 但 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则准入策略不关注对 apps/v1beta1 或 extensions/v1beta1 API 组的请求。
	//
	// - Equivalent：若请求修改的是 rules 中列出的资源，即使通过其他 API 组或版本，也匹配。
	// 例如，若 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则准入策略**会**关注对 apps/v1beta1 或 extensions/v1beta1 API 组的请求。
	// 必要时 API server 会将请求转换为匹配的资源 API。
	//
	// 默认为 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,7,opt,name=matchPolicy,casttype=MatchPolicyType"`
}

// ValidationAction 指定策略强制执行的动作。
// +enum
type ValidationAction string

const (
	// Deny 表示验证失败导致请求被拒绝。
	Deny ValidationAction = "Deny"
	// Warn 表示验证失败通过 HTTP Warning 头报告给请求客户端，警告码为 299。
	// 允许和拒绝的准入响应都可以携带警告。
	Warn ValidationAction = "Warn"
	// Audit 表示验证失败被包含在为该请求发布的审计事件中。
	// 审计事件将包含一条 `validation.policy.admission.k8s.io/validation_failure`
	// 审计注解，其值包含验证失败的细节。
	Audit ValidationAction = "Audit"
)

// NamedRuleWithOperations 是带 ResourceNames 的 Operations 和 Resources 元组。
// +structType=atomic
type NamedRuleWithOperations struct {
	// resourceNames 是本规则适用名称的可选白名单。空集表示允许所有。
	// +listType=atomic
	// +optional
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,1,rep,name=resourceNames"`
	// RuleWithOperations 是 Operations 和 Resources 的元组。
	RuleWithOperations `json:"" protobuf:"bytes,2,opt,name=ruleWithOperations"`
}

// RuleWithOperations 是 Operations 和 Resources 的元组。建议确保所有元组展开均有效。
type RuleWithOperations = v1.RuleWithOperations

// OperationType 指定请求的操作类型。
// +enum
type OperationType = v1.OperationType

// 这些常量应与 k8s.io/kubernetes/pkg/admission/interface.go 中定义的保持一致。
const (
	// OperationAll 表示所有操作。
	OperationAll OperationType = v1.OperationAll
	// Create 表示创建操作。
	Create OperationType = v1.Create
	// Update 表示更新操作。
	Update OperationType = v1.Update
	// Delete 表示删除操作。
	Delete OperationType = v1.Delete
	// Connect 表示连接操作。
	Connect OperationType = v1.Connect
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicy 描述一种准入变更策略，它会修改进入准入链的对象。
type MutatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 MutatingAdmissionPolicy 的期望行为。
	Spec MutatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyList 是 MutatingAdmissionPolicy 的列表。
type MutatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 的列表。
	Items []MutatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicySpec 指定准入策略期望行为的规格。
type MutatingAdmissionPolicySpec struct {
	// paramKind 指定用于参数化此策略的资源 kind。
	// 若缺失，则此策略没有参数，params CEL 变量不会提供给验证表达式。
	// 若 paramKind 指向不存在的 kind，则策略定义配置错误，会应用 FailurePolicy。
	// 若指定了 paramKind 但 MutatingAdmissionPolicyBinding 中未设置 paramRef，params 变量为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// matchConstraints 指定此策略旨在验证哪些资源。
	// 当请求匹配_所有_约束时，MutatingAdmissionPolicy 才关注该请求。
	// 但为防止集群进入无法通过 API 恢复的不稳定状态，
	// MutatingAdmissionPolicy 不能匹配 MutatingAdmissionPolicy 和 MutatingAdmissionPolicyBinding。
	// 允许 CREATE、UPDATE 和 CONNECT 操作，不能匹配 DELETE 操作。
	// '*' 匹配 CREATE、UPDATE 和 CONNECT。
	// 必填。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// variables 包含可用于组合其他表达式的变量定义。
	// 每个变量定义为一个具名 CEL 表达式。
	// 此处定义的变量可在策略的其他表达式中通过 `variables` 访问，
	// matchConditions 除外，因为它在策略其余部分之前评估。
	//
	// 变量的表达式只能引用列表中此前定义的变量，不能引用其后的变量。
	// 因此 variables 必须按首次出现的顺序排列且无环。
	// +listType=atomic
	// +optional
	Variables []Variable `json:"variables,omitempty" protobuf:"bytes,3,rep,name=variables"`

	// mutations 包含对匹配对象执行的操作。
	// mutations 不能为空，至少需要一条变更。
	// mutations 按顺序评估，并根据 reinvocationPolicy 重新调用。
	// 策略的变更对该策略的每个绑定都会调用，且重新调用按绑定逐个进行。
	//
	// +listType=atomic
	// +optional
	Mutations []Mutation `json:"mutations,omitempty" protobuf:"bytes,4,rep,name=mutations"`

	// failurePolicy 定义如何处理准入策略的失败。失败可能来自 CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义或绑定。
	//
	// 若 paramKind 指向不存在的 Kind，则策略无效。
	// 若 paramRef.name 指向不存在的资源，则绑定无效。
	//
	// failurePolicy 不定义如何处理评估结果为 false 的验证。
	//
	// 允许值为 Ignore 或 Fail。默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,5,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// matchConditions 是请求被验证前必须满足的条件列表。
	// 匹配条件用于过滤已被 matchConstraints 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求。最多允许 64 个匹配条件。
	//
	// 若提供了参数对象，可通过 `params` 句柄以与验证表达式相同的方式访问。
	//
	// 精确匹配逻辑（按顺序）为：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过此策略。
	//   2. 若所有 matchCondition 都评估为 TRUE，评估此策略。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝请求
	//      - 若 failurePolicy=Ignore，跳过此策略
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// reinvocationPolicy 表示在一次准入评估中，变更操作是否可在每个
	// MutatingAdmissionPolicyBinding 上被调用多次。
	// 允许值为 "Never" 和 "IfNeeded"。
	//
	// Never：在一次准入评估中，每个绑定的变更操作不会被调用超过一次。
	//
	// IfNeeded：对单个准入请求，变更操作可在每个绑定上被调用多次，且不保证
	// 相对于其他准入插件、准入 webhook、本策略的绑定和准入策略的顺序。
	// 只有当变更操作调用之后又修改了对象时，才会重新调用变更操作。
	// 必填。
	ReinvocationPolicy ReinvocationPolicyType `json:"reinvocationPolicy,omitempty" protobuf:"bytes,7,opt,name=reinvocationPolicy,casttype=ReinvocationPolicyType"`
}

// Mutation 指定用于执行变更的 CEL 表达式。
type Mutation struct {
	// patchType 表示使用的补丁策略。
	// 允许值为 "ApplyConfiguration" 和 "JSONPatch"。
	// 必填。
	//
	// +unionDiscriminator
	PatchType PatchType `json:"patchType" protobuf:"bytes,2,opt,name=patchType,casttype=PatchType"`

	// applyConfiguration 定义对象期望的配置值。
	// 配置通过 [structured merge diff](https://github.com/kubernetes-sigs/structured-merge-diff)
	// 应用到准入对象上。使用 CEL 表达式创建 apply configuration。
	ApplyConfiguration *ApplyConfiguration `json:"applyConfiguration,omitempty" protobuf:"bytes,3,opt,name=applyConfiguration"`

	// jsonPatch 定义一个 [JSON patch](https://jsonpatch.com/) 操作，用于对对象执行变更。
	// 使用 CEL 表达式创建 JSON 补丁。
	JSONPatch *JSONPatch `json:"jsonPatch,omitempty" protobuf:"bytes,4,opt,name=jsonPatch"`
}

// PatchType 指定变更的补丁操作类型。
// +enum
type PatchType string

const (
	// ApplyConfiguration 表示变更使用 apply configuration 修改对象。
	PatchTypeApplyConfiguration PatchType = "ApplyConfiguration"
	// JSONPatch 表示通过 JSON Patch 修改对象。
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// ApplyConfiguration 定义对象期望的配置值。
type ApplyConfiguration struct {
	// expression 由 CEL 求值以创建 apply configuration。
	// 参考：https://github.com/google/cel-spec
	//
	// apply configuration 在 CEL 中通过对象初始化声明。例如，下面的 CEL 表达式
	// 返回只设置一个字段的 apply configuration：
	//
	//	Object{
	//	  spec: Object.spec{
	//	    serviceAccountName: "example"
	//	  }
	//	}
	//
	// apply configuration 不能修改原子结构体、map 或数组，否则可能意外删除
	// 未包含在 apply configuration 中的值。
	//
	// CEL 表达式可访问创建 apply configuration 所需的对象类型：
	//
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）
	//
	// CEL 表达式可访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自传入请求的对象。DELETE 请求中值为 null。
	// - 'oldObject' - 已存在的对象。CREATE 请求中值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的策略绑定所引用的参数资源。仅当策略指定了 ParamKind 时才填充。
	// - 'namespaceObject' - 传入对象所属的命名空间对象。集群级资源的值为 null。
	// - 'variables' - 组合变量的映射，从变量名到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求主体（用户或 service account）执行授权检查。
	//   详见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问。
	// 其他元数据属性不可访问。
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 必填。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// JSONPatch 定义一个 JSON Patch。
type JSONPatch struct {
	// expression 由 CEL 求值以创建 [JSON patch](https://jsonpatch.com/)。
	// 参考：https://github.com/google/cel-spec
	//
	// expression 必须返回一个 JSONPatch 值数组。
	//
	// 例如，下面的 CEL 表达式返回一个有条件修改某值的 JSON patch：
	//
	//	  [
	//	    JSONPatch{op: "test", path: "/spec/example", value: "Red"},
	//	    JSONPatch{op: "replace", path: "/spec/example", value: "Green"}
	//	  ]
	//
	// 要为补丁值定义对象，使用 Object 类型。例如：
	//
	//	  [
	//	    JSONPatch{
	//	      op: "add",
	//	      path: "/spec/selector",
	//	      value: Object.spec.selector{matchLabels: {"environment": "test"}}
	//	    }
	//	  ]
	//
	// 要将包含 '/' 和 '~' 的字符串用作 JSONPatch 路径键，使用 "jsonpatch.escapeKey"。例如：
	//
	//	  [
	//	    JSONPatch{
	//	      op: "add",
	//	      path: "/metadata/labels/" + jsonpatch.escapeKey("example.com/environment"),
	//	      value: "test"
	//	    },
	//	  ]
	//
	// CEL 表达式可访问创建 JSON patch 和对象所需的类型：
	//
	// - 'JSONPatch' - JSON Patch 操作的 CEL 类型。JSONPatch 有 'op'、'from'、'path' 和 'value' 字段。
	//   详见 [JSON patch](https://jsonpatch.com/)。'value' 字段可设为字符串、
	//   整数、数组、map 或对象。若设置，'path' 和 'from' 字段必须是
	//   [JSON pointer](https://datatracker.ietf.org/doc/html/rfc6901/) 字符串，
	//   其中可用 'jsonpatch.escapeKey()' CEL 函数转义包含 '/' 和 '~' 的路径键。
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）
	//
	// CEL 表达式可访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自传入请求的对象。DELETE 请求中值为 null。
	// - 'oldObject' - 已存在的对象。CREATE 请求中值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的策略绑定所引用的参数资源。仅当策略指定了 ParamKind 时才填充。
	// - 'namespaceObject' - 传入对象所属的命名空间对象。集群级资源的值为 null。
	// - 'variables' - 组合变量的映射，从变量名到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求主体（用户或 service account）执行授权检查。
	//   详见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置的 CEL ResourceCheck。
	//
	// CEL 表达式可访问 [Kubernetes CEL 函数库](https://kubernetes.io/docs/reference/using-api/cel/#cel-options-language-features-and-libraries)
	// 以及：
	//
	// - 'jsonpatch.escapeKey' - 执行 JSONPatch 键转义。'~' 和 '/' 分别转义为 '~0' 和 `~1'。
	//
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 必填。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// ReinvocationPolicyType 指定当其他准入插件也执行修改时使用何种策略类型。
// +enum
type ReinvocationPolicyType = v1.ReinvocationPolicyType

const (
	// NeverReinvocationPolicy 表示在一次准入评估中，变更操作不得被调用超过一次。
	NeverReinvocationPolicy ReinvocationPolicyType = v1.NeverReinvocationPolicy
	// IfNeededReinvocationPolicy 表示如果在初次变更调用之后，被准入对象又被其他
	// 准入插件修改，则在本次准入评估中变更操作可以至少再调用一次。
	IfNeededReinvocationPolicy ReinvocationPolicyType = v1.IfNeededReinvocationPolicy
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyBinding 将 MutatingAdmissionPolicy 与参数化资源绑定。
// MutatingAdmissionPolicyBinding 与可选的参数资源共同定义集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个绑定会使其策略被评估 N 次；不使用参数的策略/绑定 N 为 1，
// 否则 N 为该绑定选中的参数数量。
// 每次评估都受 [runtime cost budget](https://kubernetes.io/docs/reference/using-api/cel/#runtime-cost-budget) 约束。
//
// 增加/删除策略、绑定或参数不会影响某个给定（策略、绑定、参数）组合
// 是否在其自身 CEL 预算内。
type MutatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 MutatingAdmissionPolicyBinding 的期望行为。
	Spec MutatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyBindingList 是 MutatingAdmissionPolicyBinding 的列表。
type MutatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 的列表。
	Items []MutatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicyBindingSpec 是 MutatingAdmissionPolicyBinding 的规格。
type MutatingAdmissionPolicyBindingSpec struct {
	// policyName 引用 MutatingAdmissionPolicyBinding 所绑定的 MutatingAdmissionPolicy 名称。
	// 若被引用的资源不存在，此绑定被视为无效并被忽略。
	// 必填。
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// paramRef 指定用于配置准入控制策略的参数资源。
	// 它应指向所绑定 MutatingAdmissionPolicy 的 spec.ParamKind 中指定类型的资源。
	// 若策略指定了 ParamKind 但 ParamRef 引用的资源不存在，此绑定被视为配置错误，
	// 会应用 MutatingAdmissionPolicy 的 FailurePolicy。
	// 若策略未指定 ParamKind，则忽略此字段，规则在无参数的情况下评估。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// matchResources 限制哪些资源匹配此绑定并可由其变更。
	// 注意，若 matchResources 匹配某资源，该资源还必须匹配策略的 matchConstraints
	// 和 matchConditions，才可被变更。
	// 当 matchResources 未设置时，它不限制资源匹配，资源只需匹配策略的 matchConstraints
	// 和 matchConditions 即可被变更。
	// 此外，matchResources.resourceRules 是可选的，未设置时不限制匹配。
	// 注意这与 MutatingAdmissionPolicy 的 matchConstraints 不同，后者要求 resourceRules 必填。
	// 允许 CREATE、UPDATE 和 CONNECT 操作，不能匹配 DELETE 操作。
	// '*' 匹配 CREATE、UPDATE 和 CONNECT。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`
}
