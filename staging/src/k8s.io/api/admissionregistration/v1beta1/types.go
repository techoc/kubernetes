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

package v1beta1

import (
	v1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Rule 是 APIGroups、APIVersion 和 Resources 组成的元组。建议确保所有元组展开都是有效的。
type Rule = v1.Rule

// ScopeType 指定 Rule 的作用域。
// +enum
type ScopeType = v1.ScopeType

const (
	// ClusterScope 表示作用域限定为集群作用域对象。Namespace 对象属于集群作用域。
	ClusterScope ScopeType = v1.ClusterScope
	// NamespacedScope 表示作用域限定为命名空间作用域对象。
	NamespacedScope ScopeType = v1.NamespacedScope
	// AllScopes 表示包含所有作用域。
	AllScopes ScopeType = v1.AllScopes
)

// ParameterNotFoundActionType 指定一种失败策略，定义在 binding 的 perNamespaceParamRef
// 所引用的参数未找到时如何评估该 binding。
type ParameterNotFoundActionType string

const (
	// Allow 表示若找不到参数资源，所有请求都将被允许。
	AllowAction ParameterNotFoundActionType = "Allow"
	// Deny 表示若找不到参数资源，所有请求都将被拒绝。
	DenyAction ParameterNotFoundActionType = "Deny"
)

// FailurePolicyType 指定失败策略，定义如何处理来自 admission endpoint 的无法识别的错误。
type FailurePolicyType string

const (
	// Ignore 表示忽略调用 webhook 时发生的错误。
	Ignore FailurePolicyType = "Ignore"
	// Fail 表示调用 webhook 时发生错误会导致准入失败。
	Fail FailurePolicyType = "Fail"
)

// MatchPolicyType 指定匹配策略的类型。
type MatchPolicyType string

const (
	// Exact 表示仅当请求与给定规则完全匹配时才发送给 webhook。
	Exact MatchPolicyType = "Exact"
	// Equivalent 表示若请求通过另一个 API 组或版本修改了 rules 中列出的资源，也应发送给 webhook。
	Equivalent MatchPolicyType = "Equivalent"
)

// SideEffectClass 指定 webhook 可能产生的副作用类型。
type SideEffectClass string

const (
	// SideEffectClassUnknown 表示对调用 webhook 的副作用一无所知。
	// 若带有 dry-run 属性的请求会触发对此 webhook 的调用，则该请求将改为失败。
	SideEffectClassUnknown SideEffectClass = "Unknown"
	// SideEffectClassNone 表示调用 webhook 不会产生副作用。
	SideEffectClassNone SideEffectClass = "None"
	// SideEffectClassSome 表示调用 webhook 可能产生副作用。
	// 若带有 dry-run 属性的请求会触发对此 webhook 的调用，则该请求将改为失败。
	SideEffectClassSome SideEffectClass = "Some"
	// SideEffectClassNoneOnDryRun 表示调用 webhook 可能产生副作用，
	// 但若被审查的请求带有 dry-run 属性，副作用会被抑制。
	SideEffectClassNoneOnDryRun SideEffectClass = "NoneOnDryRun"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.28
// ValidatingAdmissionPolicy 描述一种准入验证策略的定义，它接受或拒绝对象而不修改对象。
// +k8s:supportsSubresource="/status"
type ValidatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 ValidatingAdmissionPolicy 的期望行为。
	Spec ValidatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// status 表示 ValidatingAdmissionPolicy 的当前状态，包括有助于判断策略
	// 是否按预期运行的警告。
	// 由系统填充。
	// 只读。
	// +optional
	Status ValidatingAdmissionPolicyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ValidatingAdmissionPolicyStatus 表示准入验证策略的状态。
type ValidatingAdmissionPolicyStatus struct {
	// observedGeneration 是控制器观测到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`
	// typeChecking 包含每个表达式的类型检查结果。
	// 该字段存在表示类型检查已完成。
	// +optional
	TypeChecking *TypeChecking `json:"typeChecking,omitempty" protobuf:"bytes,2,opt,name=typeChecking"`
	// conditions 表示策略当前状态的最新可用观测结果。
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" protobuf:"bytes,3,rep,name=conditions"`
}

// ValidatingAdmissionPolicyConditionType 是准入验证策略的条件类型。
type ValidatingAdmissionPolicyConditionType string

// TypeChecking 包含对 ValidatingAdmissionPolicy 中表达式进行类型检查的结果。
type TypeChecking struct {
	// expressionWarnings 包含每个表达式的类型检查警告。
	// +optional
	// +listType=atomic
	ExpressionWarnings []ExpressionWarning `json:"expressionWarnings,omitempty" protobuf:"bytes,1,rep,name=expressionWarnings"`
}

// ExpressionWarning 是针对特定表达式的警告信息。
type ExpressionWarning struct {
	// fieldRef 是指向该表达式所在字段的路径。
	// 例如，对第一个 validation 表达式的引用为 "spec.validations[0].expression"。
	FieldRef string `json:"fieldRef" protobuf:"bytes,2,opt,name=fieldRef"`
	// warning 以人类可读形式包含类型检查信息的内容。
	// 每行警告包含表达式被检查时所对照的类型，以及来自编译器的类型检查错误。
	Warning string `json:"warning" protobuf:"bytes,3,opt,name=warning"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.28
// ValidatingAdmissionPolicyList 是 ValidatingAdmissionPolicy 的列表。
type ValidatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 的列表。
	Items []ValidatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicySpec 是 AdmissionPolicy 期望行为的规格。
type ValidatingAdmissionPolicySpec struct {
	// paramKind 指定用于参数化此策略的资源种类。
	// 若未指定，则该策略没有参数，验证表达式中不会提供 params CEL 变量。
	// 若 ParamKind 指向不存在的 Kind，则此策略定义配置有误，将应用 FailurePolicy。
	// 若指定了 paramKind 但 ValidatingAdmissionPolicyBinding 中未设置 paramRef，
	// 则 params 变量为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// matchConstraints 指定此策略设计用于验证哪些资源。
	// 当请求匹配_所有_约束时，AdmissionPolicy 才关心该请求。
	// 然而，为防止集群陷入无法通过 API 恢复的不稳定状态，
	// ValidatingAdmissionPolicy 不能匹配 ValidatingAdmissionPolicy 和
	// ValidatingAdmissionPolicyBinding。
	// 必需。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// validations 包含用于执行验证的 CEL 表达式。
	// validations 和 auditAnnotations 不可同时为空，至少需要其中一个。
	// +listType=atomic
	// +optional
	Validations []Validation `json:"validations,omitempty" protobuf:"bytes,3,rep,name=validations"`

	// failurePolicy 定义如何处理准入策略的失败。失败可能来自 CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义或 binding。
	//
	// 若 spec.paramKind 指向不存在的 Kind，则策略无效。
	// 若 spec.paramRef.name 指向不存在的资源，则 binding 无效。
	//
	// failurePolicy 不定义如何处置评估结果为 false 的验证。
	//
	// 当 failurePolicy 设为 Fail 时，由 ValidatingAdmissionPolicyBinding 的
	// validationActions 定义如何强制执行失败。
	//
	// 允许值为 Ignore 或 Fail，默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// auditAnnotations 包含 CEL 表达式，用于为 API 请求的审计事件生成审计注解。
	// validations 和 auditAnnotations 不可同时为空，至少需要其中一个。
	// +listType=atomic
	// +optional
	AuditAnnotations []AuditAnnotation `json:"auditAnnotations,omitempty" protobuf:"bytes,5,rep,name=auditAnnotations"`

	// matchConditions 是请求被验证前必须满足的条件列表。
	// 匹配条件过滤已被规则、namespaceSelector 和 objectSelector 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求，最多允许 64 个匹配条件。
	//
	// 若提供了参数对象，可通过 `params` 句柄访问，方式与验证表达式相同。
	//
	// 精确匹配逻辑如下（按顺序）：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过该策略。
	//   2. 若所有 matchCondition 评估为 TRUE，评估该策略。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝该请求
	//      - 若 failurePolicy=Ignore，跳过该策略
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// variables 包含可用于组合其他表达式的变量定义，每个变量定义为一个命名的 CEL 表达式。
	// 此处定义的变量在策略的其他表达式中可通过 `variables` 使用，但 MatchConditions 除外，
	// 因为 MatchConditions 在策略其余部分之前评估。
	//
	// 变量的表达式可以引用列表中先前定义的变量，但不能引用之后的变量。
	// 因此变量必须按首次出现的顺序排列且无环。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	Variables []Variable `json:"variables,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=variables"`
}

// ParamKind 是 Group、Kind 和 Version 组成的元组。
// +structType=atomic
type ParamKind struct {
	// apiVersion 是资源所属的 API 组版本，格式为 "group/version"。
	// 必需。
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,1,rep,name=apiVersion"`

	// kind 是资源所属的 API kind。
	// 必需。
	Kind string `json:"kind,omitempty" protobuf:"bytes,2,rep,name=kind"`
}

// Validation 指定用于执行验证的 CEL 表达式。
type Validation struct {
	// expression 表示将由 CEL 评估的表达式。
	// 参考：https://github.com/google/cel-spec
	// CEL 表达式可以访问 API 请求/响应的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自入站请求的对象。DELETE 请求时值为 null。
	// - 'oldObject' - 现有对象。CREATE 请求时值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的 policy binding 所引用的参数资源。仅当策略具有 ParamKind 时填充。
	// - 'namespaceObject' - 入站对象所属的命名空间对象。集群作用域资源时值为 null。
	// - 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求的主体（用户或 service account）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并配置了请求资源的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问，
	// 其他 metadata 属性不可访问。
	//
	// 仅可访问形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名。
	// 在表达式中访问时，可访问的属性名按以下规则转义：
	// - '__' 转义为 '__underscores__'
	// - '.' 转义为 '__dot__'
	// - '-' 转义为 '__dash__'
	// - '/' 转义为 '__slash__'
	// - 与 CEL 保留关键字完全匹配的属性名转义为 '__{keyword}__'。关键字包括：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return"。
	// 示例：
	//   - 访问名为 "namespace" 的属性：{"Expression": "object.__namespace__ > 0"}
	//   - 访问名为 "x-prop" 的属性：{"Expression": "object.x__dash__prop > 0"}
	//   - 访问名为 "redact__d" 的属性：{"Expression": "object.redact__underscores__d > 0"}
	//
	// 对 list 类型为 'set' 或 'map' 的数组，相等比较忽略元素顺序，即 [1, 2] == [2, 1]。
	// 对带有 x-kubernetes-list-type 的数组，拼接遵循该列表类型的语义：
	//   - 'set'：`X + Y` 执行并集，保留 `X` 中所有元素的位置，追加 `Y` 中不相交的元素并保持其相对顺序。
	//   - 'map'：`X + Y` 执行合并，保留 `X` 中所有键的位置，当 `X` 与 `Y` 的键集相交时用 `Y` 的值覆盖，
	//     追加 `Y` 中键不相交的元素并保持其相对顺序。
	// 必需。
	Expression string `json:"expression" protobuf:"bytes,1,opt,name=Expression"`
	// message 表示验证失败时显示的消息。若 Expression 包含换行，则必须提供该消息。
	// 消息不得包含换行。若未设置，消息为 "failed rule: {Rule}"。
	// 例如 "must be a URL with the host matching spec.host"。
	// 若 Expression 包含换行，则必须提供 Message。
	// 消息不得包含换行。
	// 若未设置，消息为 "failed Expression: {Expression}"。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// reason 以机器可读的方式描述此验证失败的原因。
	// 若这是列表中第一个失败的验证，该原因及相应的 HTTP 响应码将用于
	// 返回给客户端的 HTTP 响应。
	// 当前支持的原因有："Unauthorized"、"Forbidden"、"Invalid"、"RequestEntityTooLarge"。
	// 若未设置，响应中使用 StatusReasonInvalid。
	// +optional
	Reason *metav1.StatusReason `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// messageExpression 声明一个 CEL 表达式，其求值结果为该规则失败时返回的验证失败消息。
	// 由于 messageExpression 用作失败消息，它必须求值为字符串。
	// 若验证项同时提供了 message 和 messageExpression，失败时使用 messageExpression。
	// 若 messageExpression 求值出现运行时错误，会记录该错误，并按未设置 messageExpression
	// 的方式生成验证失败消息。若 messageExpression 求值为空字符串、仅含空格的字符串或
	// 含换行的字符串，同样按未设置该字段的方式生成失败消息，并记录该字段产生了空字符串/
	// 仅含空格的字符串/含换行的字符串这一事实。
	// messageExpression 可访问与 `expression` 相同的所有变量，但 'authorizer' 和
	// 'authorizer.requestResource' 除外。
	// 示例：
	// "object.x must be less than max ("+string(params.max)+")"
	// +optional
	MessageExpression string `json:"messageExpression,omitempty" protobuf:"bytes,4,opt,name=messageExpression"`
}

// Variable 是用于组合的变量定义，定义为一个命名的表达式。
// +structType=atomic
type Variable struct {
	// name 是变量的名称，必须是合法的 CEL 标识符且在所有变量中唯一。
	// 该变量可在其他表达式中通过 `variables` 访问。
	// 例如，name 为 "foo" 时，变量可通过 `variables.foo` 使用。
	Name string `json:"name" protobuf:"bytes,1,opt,name=Name"`

	// expression 是将被求值作为变量值的表达式。
	// 该 CEL 表达式可访问与 Validation 中 CEL 表达式相同的标识符。
	Expression string `json:"expression" protobuf:"bytes,2,opt,name=Expression"`
}

// AuditAnnotation 描述如何为 API 请求生成审计注解。
type AuditAnnotation struct {
	// key 指定审计注解键。一个 ValidatingAdmissionPolicy 的审计注解键必须唯一。
	// 该键必须是限定名（[A-Za-z0-9][-A-Za-z0-9_.]*）且长度不超过 63 字节。
	//
	// 该键与 ValidatingAdmissionPolicy 的资源名组合构成审计注解键：
	// "{ValidatingAdmissionPolicy name}/{key}"。
	//
	// 若某个 admission webhook 使用与此 ValidatingAdmissionPolicy 相同的资源名和相同的
	// 审计注解键，则注解键会完全相同。此时，首个写入该键的注解会包含在审计事件中，
	// 后续所有同键注解都会被丢弃。
	//
	// 必需。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`

	// valueExpression 表示由 CEL 求值以产生审计注解值的表达式，结果必须是字符串或 null。
	// 若求值为字符串，审计注解将以该字符串值包含进来；若求值为 null 或空字符串，则省略该注解。
	// valueExpression 长度不得超过 5kb，求值结果超过 10kb 时将被截断为 10kb。
	//
	// 若多个 ValidatingAdmissionPolicyBinding 资源匹配同一 API 请求，则会对每个 binding
	// 求值 valueExpression，所有由 valueExpression 产生的唯一值会以逗号分隔的列表合并。
	//
	// 必需。
	ValueExpression string `json:"valueExpression" protobuf:"bytes,2,opt,name=valueExpression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.28

// ValidatingAdmissionPolicyBinding 将 ValidatingAdmissionPolicy 与参数化资源绑定。
// ValidatingAdmissionPolicyBinding 和参数 CRD 共同定义集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个 binding 会使其策略被评估 N 次：不使用 params 的
// 策略/binding 中 N 为 1，否则 N 为 binding 选择的参数数量。
//
// 策略的 CEL 表达式必须具有低于最大 CEL 预算的计算成本。
// 每次策略评估都有独立的 CEL 成本预算。添加/删除策略、binding 或参数
// 不会影响给定（策略、binding、参数）组合是否在其自身 CEL 预算内。
type ValidatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 ValidatingAdmissionPolicyBinding 的期望行为。
	// +required
	Spec ValidatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.28

// ValidatingAdmissionPolicyBindingList 是 ValidatingAdmissionPolicyBinding 的列表。
type ValidatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 的列表。
	Items []ValidatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicyBindingSpec 是 ValidatingAdmissionPolicyBinding 的规格。
type ValidatingAdmissionPolicyBindingSpec struct {
	// policyName 引用此 ValidatingAdmissionPolicyBinding 绑定到的 ValidatingAdmissionPolicy 名称。
	// 若被引用的资源不存在，该 binding 被视为无效并会被忽略。
	// 必需。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// paramRef 指定用于配置准入策略的参数资源，应指向被绑定 ValidatingAdmissionPolicy
	// 的 ParamKind 中指定类型的资源。若策略指定了 ParamKind 但 ParamRef 引用的资源
	// 不存在，该 binding 被视为配置有误，将应用 ValidatingAdmissionPolicy 的 FailurePolicy。
	// 若策略未指定 ParamKind，则忽略此字段，规则在没有参数的情况下评估。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// matchResources 声明哪些资源匹配此 binding 并将被其验证。
	// 注意这与策略的 matchConstraints 取交集，因此只有被策略匹配的请求才能被此 binding 选择。
	// 若未设置，则策略匹配的所有资源都由该 binding 验证。
	// 当 resourceRules 未设置时，它不约束资源匹配；若资源被此对象的其他字段匹配，则会被验证。
	// 注意这与 ValidatingAdmissionPolicy 的 matchConstraints 不同，后者要求 resourceRules。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`

	// validationActions 声明被引用 ValidatingAdmissionPolicy 的验证如何强制执行。
	// 若某验证评估为 false，始终按这些动作强制执行。
	//
	// 由 ValidatingAdmissionPolicy 的 FailurePolicy 定义的失败仅在 FailurePolicy 设为 Fail
	// 时按这些动作强制执行，否则失败会被忽略。这包括编译错误、运行时错误和策略配置错误。
	//
	// validationActions 声明为动作值的集合，顺序无关，且不得包含重复动作。
	//
	// 支持的动作值有：
	//
	// "Deny" 表示验证失败导致请求被拒绝。
	//
	// "Warn" 表示验证失败通过 HTTP Warning 头报告给请求客户端，警告码为 299。
	// 允许和拒绝的准入响应都可以发送警告。
	//
	// "Audit" 表示验证失败被包含在请求的审计事件中。审计事件将包含
	// `validation.policy.admission.k8s.io/validation_failure` 审计注解，其值包含
	// 验证失败的详情，格式为对象列表的 JSON，每个对象包含以下字段：
	// - message：验证失败消息字符串
	// - policy：ValidatingAdmissionPolicy 的资源名
	// - binding：ValidatingAdmissionPolicyBinding 的资源名
	// - expressionIndex：ValidatingAdmissionPolicy 中失败验证的索引
	// - validationActions：对该验证失败执行的强制动作
	// 审计注解示例：
	// `"validation.policy.admission.k8s.io/validation_failure": "[{\"message\": \"Invalid value\", {\"policy\": \"policy.example.com\", {\"binding\": \"policybinding.example.com\", {\"expressionIndex\": \"1\", {\"validationActions\": [\"Audit\"]}]"`
	//
	// 客户端应预期通过忽略任何无法识别的值来处理额外值。
	//
	// "Deny" 和 "Warn" 不可同时使用，因为这种组合会在 API 响应体和 HTTP 警告头中
	// 无谓地重复验证失败信息。
	//
	// 必需。
	// +listType=set
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	ValidationActions []ValidationAction `json:"validationActions,omitempty" protobuf:"bytes,4,rep,name=validationActions"`
}

// ParamRef 描述如何定位用作 policy binding 规则表达式输入的 params。
// +structType=atomic
type ParamRef struct {
	// name 是被引用资源的名称。
	//
	// `name` 或 `selector` 必须设置其一，但二者互斥：设置一个时另一个必须不设置。
	//
	// 若要为所有准入请求使用单一参数，可设置 `name` 字段、留空 `selector`，
	// 并在 `paramKind` 为命名空间作用域时设置 namespace。
	//
	Name string `json:"name,omitempty" protobuf:"bytes,1,rep,name=name"`

	// namespace 是被引用资源所在的命名空间，用于将参数搜索限制在特定命名空间，
	// 对 `name` 和 `selector` 字段都适用。
	//
	// 可通过在策略中指定命名空间作用域的 `paramKind` 并留空此字段来使用按命名空间的参数。
	//
	// - 若 `paramKind` 是集群作用域的，则必须不设置此字段，设置会导致配置错误。
	//
	// - 若 `paramKind` 是命名空间作用域的，此字段留空时将使用被准入评估对象的命名空间。
	//   需注意若留空，binding 不得匹配任何集群作用域资源，否则会导致错误。
	//
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,rep,name=namespace"`

	// selector 可根据标签匹配多个参数对象。提供 selector: {} 可匹配 ParamKind 的所有资源。
	//
	// 若找到多个参数，它们都会用策略表达式评估，结果按逻辑与合并。
	//
	// `name` 或 `selector` 必须设置其一，但二者互斥：设置一个时另一个必须不设置。
	//
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,3,rep,name=selector"`

	// parameterNotFoundAction 控制 binding 在资源存在、name 或 selector 有效，
	// 但没有参数被 binding 匹配时的行为。若设为 `Allow`，没有匹配参数将被视为
	// binding 验证成功；若设为 `Deny`，没有匹配参数将受策略的 `failurePolicy` 约束。
	//
	// 允许值为 `Allow` 或 `Deny`。
	//
	// 必需
	ParameterNotFoundAction *ParameterNotFoundActionType `json:"parameterNotFoundAction,omitempty" protobuf:"bytes,4,rep,name=parameterNotFoundAction"`
}

// MatchResources 根据对象是否满足匹配条件决定是否在对象上运行准入控制策略。
// exclude 规则优先于 include 规则（若资源同时匹配两者，则被排除）。
// +structType=atomic
type MatchResources struct {
	// namespaceSelector 根据对象的命名空间是否匹配选择器决定是否在对象上运行准入控制策略。
	// 若对象本身是命名空间，则对 object.metadata.labels 进行匹配。若对象是其他集群作用域
	// 资源，则永不跳过该策略。
	//
	// 例如，若要对命名空间未关联 "runlevel" 为 "0" 或 "1" 的所有对象运行策略，
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
	// 若只想对命名空间关联 "environment" 为 "prod" 或 "staging" 的对象运行策略，
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
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,1,opt,name=namespaceSelector"`
	// objectSelector 根据对象是否具有匹配标签决定是否运行验证。objectSelector 同时
	// 针对将发送给 CEL 验证的 oldObject 和 newObject 求值，任一对象匹配即视为匹配。
	// null 对象（CREATE 时的 oldObject 或 DELETE 时的 newObject）或不能有标签的对象
	// （如 DeploymentRollback 或 PodProxyOptions 对象）不算匹配。
	// 仅当 webhook 是选择性启用时才使用 object selector，因为最终用户可通过设置标签跳过准入 webhook。
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,2,opt,name=objectSelector"`
	// resourceRules 描述 ValidatingAdmissionPolicy 匹配哪些资源/子资源上的哪些操作。
	// 当操作匹配_任一_ Rule 时，策略关心该操作。
	// +listType=atomic
	// +optional
	ResourceRules []NamedRuleWithOperations `json:"resourceRules,omitempty" protobuf:"bytes,3,rep,name=resourceRules"`
	// excludeResourceRules 描述 ValidatingAdmissionPolicy 不应关心哪些资源/子资源上的哪些操作。
	// exclude 规则优先于 include 规则（若资源同时匹配两者，则被排除）。
	// +listType=atomic
	// +optional
	ExcludeResourceRules []NamedRuleWithOperations `json:"excludeResourceRules,omitempty" protobuf:"bytes,4,rep,name=excludeResourceRules"`
	// matchPolicy 定义如何使用 "MatchResources" 列表匹配入站请求，允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：仅当请求与指定规则完全匹配时才匹配。例如，若 deployments 可通过
	// apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，但 "rules" 只包括
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给 ValidatingAdmissionPolicy。
	//
	// - Equivalent：只要请求修改了 rules 中列出的资源，即使通过另一个 API 组或版本也匹配。
	// 例如，若 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包括 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给 ValidatingAdmissionPolicy。
	//
	// 默认为 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,7,opt,name=matchPolicy,casttype=MatchPolicyType"`
}

// ValidationAction 指定策略的强制动作。
// +enum
type ValidationAction string

const (
	// Deny 表示验证失败导致请求被拒绝。
	Deny ValidationAction = "Deny"
	// Warn 表示验证失败通过 HTTP Warning 头报告给请求客户端，警告码为 299。
	// 允许和拒绝的准入响应都可以发送警告。
	Warn ValidationAction = "Warn"
	// Audit 表示验证失败被包含在请求的审计事件中。审计事件将包含
	// `validation.policy.admission.k8s.io/validation_failure` 审计注解，其值包含验证失败的详情。
	Audit ValidationAction = "Audit"
)

// NamedRuleWithOperations 是带 ResourceNames 的 Operations 和 Resources 元组。
// +structType=atomic
type NamedRuleWithOperations struct {
	// resourceNames 是规则适用的名称白名单（可选），空集合表示允许所有内容。
	// +listType=atomic
	// +optional
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,1,rep,name=resourceNames"`
	// RuleWithOperations 是 Operations 和 Resources 组成的元组。
	RuleWithOperations `json:"" protobuf:"bytes,2,opt,name=ruleWithOperations"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,ValidatingWebhookConfiguration

// ValidatingWebhookConfiguration 描述 admission webhook 的配置，该 webhook 接受或拒绝对象而不修改对象。
// 自 v1.16 起弃用，计划于 v1.19 移除。请改用 admissionregistration.k8s.io/v1 ValidatingWebhookConfiguration。
type ValidatingWebhookConfiguration struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// webhooks 是 webhook 列表以及受影响资源和操作。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Webhooks []ValidatingWebhook `json:"webhooks,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=Webhooks"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,ValidatingWebhookConfigurationList

// ValidatingWebhookConfigurationList 是 ValidatingWebhookConfiguration 的列表。
type ValidatingWebhookConfigurationList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingWebhookConfiguration 的列表。
	Items []ValidatingWebhookConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingWebhookConfiguration

// MutatingWebhookConfiguration 描述 admission webhook 的配置，该 webhook 接受或拒绝对象并可能修改对象。
// 自 v1.16 起弃用，计划于 v1.19 移除。请改用 admissionregistration.k8s.io/v1 MutatingWebhookConfiguration。
type MutatingWebhookConfiguration struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// webhooks 是 webhook 列表以及受影响资源和操作。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Webhooks []MutatingWebhook `json:"webhooks,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=Webhooks"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.16
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingWebhookConfigurationList

// MutatingWebhookConfigurationList 是 MutatingWebhookConfiguration 的列表。
type MutatingWebhookConfigurationList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// MutatingWebhookConfiguration 的列表。
	Items []MutatingWebhookConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingWebhook 描述一个 admission webhook 及其适用的资源和操作。
type ValidatingWebhook struct {
	// name 是 admission webhook 的名称，应为完全限定名，如 imagepolicy.kubernetes.io，
	// 其中 "imagepolicy" 是 webhook 名称，kubernetes.io 是组织名。
	// 必需。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// clientConfig 定义如何与该 hook 通信。
	// 必需
	ClientConfig WebhookClientConfig `json:"clientConfig" protobuf:"bytes,2,opt,name=clientConfig"`

	// rules 描述 webhook 关心哪些资源/子资源上的哪些操作。若操作匹配_任一_ Rule，
	// webhook 就关心该操作。但是，为防止 ValidatingAdmissionWebhook 和 MutatingAdmissionWebhook
	// 将集群置于必须完全禁用插件才能恢复的状态，它们永远不会针对
	// ValidatingWebhookConfiguration 和 MutatingWebhookConfiguration 对象的准入请求被调用。
	// +listType=atomic
	Rules []RuleWithOperations `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`

	// failurePolicy 定义如何处理来自 admission endpoint 的无法识别的错误，
	// 允许值为 Ignore 或 Fail，默认为 Ignore。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// matchPolicy 定义如何使用 "rules" 列表匹配入站请求，允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：仅当请求与指定规则完全匹配时才匹配。例如，若 deployments 可通过
	// apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，但 "rules" 只包括
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给 webhook。
	//
	// - Equivalent：只要请求修改了 rules 中列出的资源，即使通过另一个 API 组或版本也匹配。
	// 例如，若 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包括 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给 webhook。
	//
	// 默认为 "Exact"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,9,opt,name=matchPolicy,casttype=MatchPolicyType"`

	// namespaceSelector 根据对象的命名空间是否匹配选择器决定是否在该对象上运行 webhook。
	// 若对象本身是命名空间，则对 object.metadata.labels 进行匹配。若对象是其他集群作用域
	// 资源，则永不跳过该 webhook。
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
	// 若只想对命名空间关联 "environment" 为 "prod" 或 "staging" 的对象运行 webhook，
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
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,5,opt,name=namespaceSelector"`

	// objectSelector 根据对象是否具有匹配标签决定是否运行 webhook。objectSelector 同时
	// 针对将发送给 webhook 的 oldObject 和 newObject 求值，任一对象匹配即视为匹配。
	// null 对象（CREATE 时的 oldObject 或 DELETE 时的 newObject）或不能有标签的对象
	// （如 DeploymentRollback 或 PodProxyOptions 对象）不算匹配。
	// 仅当 webhook 是选择性启用时才使用 object selector，因为最终用户可通过设置标签跳过准入 webhook。
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,10,opt,name=objectSelector"`

	// sideEffects 声明此 webhook 是否有副作用，可接受值为 Unknown、None、Some、NoneOnDryRun。
	// 有副作用的 webhook 必须实现协调系统，因为请求可能被准入链中后续步骤拒绝，
	// 从而需要撤销副作用。带有 dryRun 属性的请求若匹配副作用为 Unknown 或 Some 的
	// webhook，将自动被拒绝。默认为 Unknown。
	// +optional
	// +listType=atomic
	SideEffects *SideEffectClass `json:"sideEffects,omitempty" protobuf:"bytes,6,opt,name=sideEffects,casttype=SideEffectClass"`

	// timeoutSeconds 指定此 webhook 的超时时间。超时后，根据失败策略，
	// webhook 调用会被忽略或 API 调用失败。超时值必须在 1 到 30 秒之间，默认为 30 秒。
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,7,opt,name=timeoutSeconds"`

	// admissionReviewVersions 是 webhook 期望的 `AdmissionReview` 版本的按优先级排序列表。
	// API server 会尝试使用列表中它支持的第一个版本。若列表中指定的版本都不被 API server 支持，
	// 该对象的验证将失败。若持久化的 webhook 配置指定了允许的版本但不含 API Server 已知的任何版本，
	// 对该 webhook 的调用将失败并受失败策略约束。默认为 `['v1beta1']`。
	// +optional
	// +listType=atomic
	AdmissionReviewVersions []string `json:"admissionReviewVersions,omitempty" protobuf:"bytes,8,rep,name=admissionReviewVersions"`

	// matchConditions 是请求被发送给此 webhook 前必须满足的条件列表。
	// 匹配条件过滤已被规则、namespaceSelector 和 objectSelector 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求，最多允许 64 个匹配条件。
	//
	// 精确匹配逻辑如下（按顺序）：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过该 webhook。
	//   2. 若所有 matchCondition 评估为 TRUE，调用该 webhook。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝该请求
	//      - 若 failurePolicy=Ignore，忽略错误并跳过该 webhook
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,11,rep,name=matchConditions"`
}

// MutatingWebhook 描述一个 admission webhook 及其适用的资源和操作。
type MutatingWebhook struct {
	// name 是 admission webhook 的名称，应为完全限定名，如 imagepolicy.kubernetes.io，
	// 其中 "imagepolicy" 是 webhook 名称，kubernetes.io 是组织名。
	// 必需。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// clientConfig 定义如何与该 hook 通信。
	// 必需
	ClientConfig WebhookClientConfig `json:"clientConfig" protobuf:"bytes,2,opt,name=clientConfig"`

	// rules 描述 webhook 关心哪些资源/子资源上的哪些操作。若操作匹配_任一_ Rule，
	// webhook 就关心该操作。但是，为防止 ValidatingAdmissionWebhook 和 MutatingAdmissionWebhook
	// 将集群置于必须完全禁用插件才能恢复的状态，它们永远不会针对
	// ValidatingWebhookConfiguration 和 MutatingWebhookConfiguration 对象的准入请求被调用。
	// +listType=atomic
	Rules []RuleWithOperations `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`

	// failurePolicy 定义如何处理来自 admission endpoint 的无法识别的错误，
	// 允许值为 Ignore 或 Fail，默认为 Ignore。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// matchPolicy 定义如何使用 "rules" 列表匹配入站请求，允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：仅当请求与指定规则完全匹配时才匹配。例如，若 deployments 可通过
	// apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，但 "rules" 只包括
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给 webhook。
	//
	// - Equivalent：只要请求修改了 rules 中列出的资源，即使通过另一个 API 组或版本也匹配。
	// 例如，若 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包括 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给 webhook。
	//
	// 默认为 "Exact"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,9,opt,name=matchPolicy,casttype=MatchPolicyType"`

	// namespaceSelector 根据对象的命名空间是否匹配选择器决定是否在该对象上运行 webhook。
	// 若对象本身是命名空间，则对 object.metadata.labels 进行匹配。若对象是其他集群作用域
	// 资源，则永不跳过该 webhook。
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
	// 若只想对命名空间关联 "environment" 为 "prod" 或 "staging" 的对象运行 webhook，
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
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,5,opt,name=namespaceSelector"`

	// objectSelector 根据对象是否具有匹配标签决定是否运行 webhook。objectSelector 同时
	// 针对将发送给 webhook 的 oldObject 和 newObject 求值，任一对象匹配即视为匹配。
	// null 对象（CREATE 时的 oldObject 或 DELETE 时的 newObject）或不能有标签的对象
	// （如 DeploymentRollback 或 PodProxyOptions 对象）不算匹配。
	// 仅当 webhook 是选择性启用时才使用 object selector，因为最终用户可通过设置标签跳过准入 webhook。
	// 默认为空的 LabelSelector，匹配所有内容。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,11,opt,name=objectSelector"`

	// sideEffects 声明此 webhook 是否有副作用，可接受值为 Unknown、None、Some、NoneOnDryRun。
	// 有副作用的 webhook 必须实现协调系统，因为请求可能被准入链中后续步骤拒绝，
	// 从而需要撤销副作用。带有 dryRun 属性的请求若匹配副作用为 Unknown 或 Some 的
	// webhook，将自动被拒绝。默认为 Unknown。
	// +optional
	SideEffects *SideEffectClass `json:"sideEffects,omitempty" protobuf:"bytes,6,opt,name=sideEffects,casttype=SideEffectClass"`

	// timeoutSeconds 指定此 webhook 的超时时间。超时后，根据失败策略，
	// webhook 调用会被忽略或 API 调用失败。超时值必须在 1 到 30 秒之间，默认为 30 秒。
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,7,opt,name=timeoutSeconds"`

	// admissionReviewVersions 是 webhook 期望的 `AdmissionReview` 版本的按优先级排序列表。
	// API server 会尝试使用列表中它支持的第一个版本。若列表中指定的版本都不被 API server 支持，
	// 该对象的验证将失败。若持久化的 webhook 配置指定了允许的版本但不含 API Server 已知的任何版本，
	// 对该 webhook 的调用将失败并受失败策略约束。默认为 `['v1beta1']`。
	// +optional
	// +listType=atomic
	AdmissionReviewVersions []string `json:"admissionReviewVersions,omitempty" protobuf:"bytes,8,rep,name=admissionReviewVersions"`

	// reinvocationPolicy 指示此 webhook 在单次准入评估中是否应被多次调用，
	// 允许值为 "Never" 和 "IfNeeded"。
	//
	// Never：在单次准入评估中 webhook 不会被调用超过一次。
	//
	// IfNeeded：若被准入的对象在初次调用 webhook 后被其他准入插件修改，
	// webhook 会在该准入评估中至少被额外调用一次。指定此选项的 webhook _必须_ 是幂等的，
	// 能够处理它们此前已准入的对象。注意：
	// * 额外调用次数不保证恰好为一次。
	// * 若额外调用导致对象被进一步修改，不保证 webhook 会再次被调用。
	// * 使用此选项的 webhook 可能会被重新排序，以最小化额外调用次数。
	// * 若要确保所有变更完成后再验证对象，请改用 validating admission webhook。
	//
	// 默认为 "Never"。
	// +optional
	ReinvocationPolicy *ReinvocationPolicyType `json:"reinvocationPolicy,omitempty" protobuf:"bytes,10,opt,name=reinvocationPolicy,casttype=ReinvocationPolicyType"`

	// matchConditions 是请求被发送给此 webhook 前必须满足的条件列表。
	// 匹配条件过滤已被规则、namespaceSelector 和 objectSelector 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求，最多允许 64 个匹配条件。
	//
	// 精确匹配逻辑如下（按顺序）：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过该 webhook。
	//   2. 若所有 matchCondition 评估为 TRUE，调用该 webhook。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝该请求
	//      - 若 failurePolicy=Ignore，忽略错误并跳过该 webhook
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,12,rep,name=matchConditions"`
}

// ReinvocationPolicyType 指定当其他准入插件也执行修改时使用何种策略类型。
// +enum
type ReinvocationPolicyType = v1.ReinvocationPolicyType

const (
	// NeverReinvocationPolicy 表示在单次准入评估中 mutation 不得被调用超过一次。
	NeverReinvocationPolicy ReinvocationPolicyType = "Never"
	// IfNeededReinvocationPolicy 表示若被准入的对象在初次 mutation 调用后被其他准入插件修改，
	// mutation 可在该准入评估中被额外调用至少一次。
	IfNeededReinvocationPolicy ReinvocationPolicyType = "IfNeeded"
)

// RuleWithOperations 是 Operations 和 Resources 组成的元组，建议确保所有元组展开都是有效的。
type RuleWithOperations = v1.RuleWithOperations

// OperationType 指定请求的操作类型。
// +enum
type OperationType = v1.OperationType

// 这些常量应与 k8s.io/kubernetes/pkg/admission/interface.go 中定义的常量保持同步。
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

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
type WebhookClientConfig struct {
	// url 以标准 URL 形式（`scheme://host:port/path`）给出 webhook 的位置，
	// `url` 和 `service` 必须且只能指定一个。
	//
	// `host` 不应指向集群内运行的服务，应改用 `service` 字段。在某些 apiserver 中
	// host 可能通过外部 DNS 解析（例如 `kube-apiserver` 不能解析集群内 DNS，
	// 因为那会违反分层原则）。`host` 也可以是 IP 地址。
	//
	// 请注意，除非你非常谨慎地确保在所有可能调用此 webhook 的 apiserver 主机上都运行该 webhook，
	// 否则使用 `localhost` 或 `127.0.0.1` 作为 `host` 是有风险的。这类安装通常不可移植，
	// 即不易在新集群中启用。
	//
	// scheme 必须为 "https"，URL 必须以 "https://" 开头。
	//
	// path 可选，若存在可以是 URL 中允许的任何字符串。可通过 path 向 webhook 传递任意字符串，
	// 例如集群标识符。
	//
	// 不允许使用 user 或 basic auth，如 "user:password@"。也不允许片段（"#..."）和查询参数（"?..."）。
	//
	// +optional
	URL *string `json:"url,omitempty" protobuf:"bytes,3,opt,name=url"`

	// service 是对此 webhook 所对应服务的引用，必须指定 `service` 或 `url` 之一。
	//
	// 若 webhook 在集群内运行，应使用 `service`。
	//
	// +optional
	Service *ServiceReference `json:"service,omitempty" protobuf:"bytes,1,opt,name=service"`

	// caBundle 是 PEM 编码的 CA 证书包，用于校验 webhook 的服务端证书。
	// 若未指定，则使用 apiserver 上的系统信任根证书。
	// +optional
	CABundle []byte `json:"caBundle,omitempty" protobuf:"bytes,2,opt,name=caBundle"`
}

// ServiceReference 保存对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// namespace 是服务所在的命名空间。
	// 必需
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// name 是服务的名称。
	// 必需
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// path 是可选的 URL 路径，将包含在发送给该服务的所有请求中。
	// +optional
	Path *string `json:"path,omitempty" protobuf:"bytes,3,opt,name=path"`

	// port 是承载 webhook 的服务端口，为向后兼容默认为 443。
	// port 应为合法端口号（1-65535，含两端）。
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,4,opt,name=port"`
}

// MatchCondition 表示请求被发送给 webhook 前必须满足的条件。
type MatchCondition struct {
	// name 是此匹配条件的标识符，用于 MatchConditions 的 strategic merge，
	// 也作为日志用途的标识符。好的名称应能描述关联表达式的含义。
	// 名称必须是限定名，由字母数字字符、'-'、'_' 或 '.' 组成，且以字母数字字符开头和结尾
	// （如 'MyName'、'my.name' 或 '123-abc'，校验所用正则为
	// '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]'），可带可选的 DNS 子域前缀和 '/'
	// （如 'example.com/MyName'）。
	//
	// 必需。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// expression 表示将由 CEL 求值且必须返回 bool 的表达式。
	// CEL 表达式可访问 AdmissionRequest 和 Authorizer 的内容，这些内容被组织为 CEL 变量：
	//
	// 'object' - 来自入站请求的对象。DELETE 请求时值为 null。
	// 'oldObject' - 现有对象。CREATE 请求时值为 null。
	// 'request' - 准入请求的属性（/pkg/apis/admission/types.go#AdmissionRequest）。
	// 'authorizer' - CEL Authorizer，可用于对请求的主体（用户或 service account）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// 'authorizer.requestResource' - 由 'authorizer' 构造并配置了请求资源的 CEL ResourceCheck。
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 必需。
	Expression string `json:"expression" protobuf:"bytes,2,opt,name=expression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingAdmissionPolicy

// MutatingAdmissionPolicy 描述一种准入变更策略的定义，它对进入准入链的对象进行变更。
type MutatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 MutatingAdmissionPolicy 的期望行为。
	Spec MutatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingAdmissionPolicyList

// MutatingAdmissionPolicyList 是 MutatingAdmissionPolicy 的列表。
type MutatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 的列表。
	Items []MutatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicySpec 是准入策略期望行为的规格。
type MutatingAdmissionPolicySpec struct {
	// paramKind 指定用于参数化此策略的资源种类。
	// 若未指定，则该策略没有参数，验证表达式中不会提供 params CEL 变量。
	// 若 paramKind 指向不存在的 Kind，则此策略定义配置有误，将应用 FailurePolicy。
	// 若指定了 paramKind 但 MutatingAdmissionPolicyBinding 中未设置 paramRef，
	// 则 params 变量为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// matchConstraints 指定此策略设计用于验证哪些资源。
	// 当请求匹配_所有_约束时，MutatingAdmissionPolicy 才关心该请求。
	// 然而，为防止集群陷入无法通过 API 恢复的不稳定状态，
	// MutatingAdmissionPolicy 不能匹配 MutatingAdmissionPolicy 和 MutatingAdmissionPolicyBinding。
	// 允许 CREATE、UPDATE 和 CONNECT 操作，不能匹配 DELETE 操作。
	// '*' 匹配 CREATE、UPDATE 和 CONNECT。
	// 必需。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// variables 包含可用于组合其他表达式的变量定义，每个变量定义为一个命名的 CEL 表达式。
	// 此处定义的变量在策略的其他表达式中可通过 `variables` 使用，但 matchConditions 除外，
	// 因为 matchConditions 在策略其余部分之前评估。
	//
	// 变量的表达式可以引用列表中先前定义的变量，但不能引用之后的变量。
	// 因此变量必须按首次出现的顺序排列且无环。
	// +listType=atomic
	// +optional
	Variables []Variable `json:"variables,omitempty" protobuf:"bytes,3,rep,name=variables"`

	// mutations 包含对匹配对象执行的操作。
	// mutations 不可为空，至少需要一项 mutation。
	// mutations 按顺序评估，并根据 reinvocationPolicy 重新调用。
	// 策略的 mutations 会针对该策略的每个 binding 调用，且 mutation 的重新调用按 binding 逐个进行。
	//
	// +listType=atomic
	// +optional
	Mutations []Mutation `json:"mutations,omitempty" protobuf:"bytes,4,rep,name=mutations"`

	// failurePolicy 定义如何处理准入策略的失败。失败可能来自 CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义或 binding。
	//
	// 若 paramKind 指向不存在的 Kind，则策略无效。
	// 若 paramRef.name 指向不存在的资源，则 binding 无效。
	//
	// failurePolicy 不定义如何处置评估结果为 false 的验证。
	//
	// 允许值为 Ignore 或 Fail，默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,5,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// matchConditions 是请求被验证前必须满足的条件列表。
	// 匹配条件过滤已被 matchConstraints 匹配的请求。
	// 空的 matchConditions 列表匹配所有请求，最多允许 64 个匹配条件。
	//
	// 若提供了参数对象，可通过 `params` 句柄访问，方式与验证表达式相同。
	//
	// 精确匹配逻辑如下（按顺序）：
	//   1. 若任一 matchCondition 评估为 FALSE，跳过该策略。
	//   2. 若所有 matchCondition 评估为 TRUE，评估该策略。
	//   3. 若任一 matchCondition 评估出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，拒绝该请求
	//      - 若 failurePolicy=Ignore，跳过该策略
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// reinvocationPolicy 指示在单次准入评估中每个 MutatingAdmissionPolicyBinding
	// 是否可多次调用 mutation，允许值为 "Never" 和 "IfNeeded"。
	//
	// Never：在单次准入评估中每个 binding 的 mutation 不会被调用超过一次。
	//
	// IfNeeded：在单次准入请求中每个 binding 的 mutation 可被调用超过一次，且不保证
	// 与其他准入插件、准入 webhook、本策略的其他 binding 及准入策略之间的顺序。
	// 只有在该 mutation 调用后其他 mutation 改变了对象时才会重新调用 mutation。
	// 必需。
	ReinvocationPolicy ReinvocationPolicyType `json:"reinvocationPolicy,omitempty" protobuf:"bytes,7,opt,name=reinvocationPolicy,casttype=ReinvocationPolicyType"`
}

// Mutation 指定用于执行 mutation 的 CEL 表达式。
type Mutation struct {
	// patchType 指示使用的 patch 策略，允许值为 "ApplyConfiguration" 和 "JSONPatch"。
	// 必需。
	//
	// +unionDiscriminator
	PatchType PatchType `json:"patchType" protobuf:"bytes,2,opt,name=patchType,casttype=PatchType"`

	// applyConfiguration 定义对象期望的配置值。配置通过
	// [结构化合并差异](https://github.com/kubernetes-sigs/structured-merge-diff) 应用到准入对象，
	// 使用 CEL 表达式创建 apply configuration。
	ApplyConfiguration *ApplyConfiguration `json:"applyConfiguration,omitempty" protobuf:"bytes,3,opt,name=applyConfiguration"`

	// jsonPatch 定义一个 [JSON patch](https://jsonpatch.com/) 操作以对对象执行 mutation。
	// 使用 CEL 表达式创建 JSON patch。
	JSONPatch *JSONPatch `json:"jsonPatch,omitempty" protobuf:"bytes,4,opt,name=jsonPatch"`
}

// PatchType 指定 mutation 的 patch 操作类型。
// +enum
type PatchType string

const (
	// ApplyConfiguration 表示 mutation 使用 apply configuration 来修改对象。
	PatchTypeApplyConfiguration PatchType = "ApplyConfiguration"
	// JSONPatch 表示通过 JSON Patch 修改对象。
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// ApplyConfiguration 定义对象期望的配置值。
type ApplyConfiguration struct {
	// expression 将由 CEL 求值以创建 apply configuration。
	// 参考：https://github.com/google/cel-spec
	//
	// Apply configuration 在 CEL 中使用对象初始化声明。例如下面的 CEL 表达式
	// 返回一个设置单个字段的 apply configuration：
	//
	//	Object{
	//	  spec: Object.spec{
	//	    serviceAccountName: "example"
	//	  }
	//	}
	//
	// Apply configuration 不能修改原子结构体、map 或数组，否则可能意外删除
	// 未包含在 apply configuration 中的值。
	//
	// CEL 表达式可访问创建 apply configuration 所需的对象类型：
	//
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）。
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）。
	//
	// CEL 表达式可访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自入站请求的对象。DELETE 请求时值为 null。
	// - 'oldObject' - 现有对象。CREATE 请求时值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的 policy binding 所引用的参数资源。仅当策略具有 ParamKind 时填充。
	// - 'namespaceObject' - 入站对象所属的命名空间对象。集群作用域资源时值为 null。
	// - 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求的主体（用户或 service account）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并配置了请求资源的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问，
	// 其他 metadata 属性不可访问。
	//
	// 仅可访问形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名。
	// 必需。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// JSONPatch 定义一个 JSON Patch。
type JSONPatch struct {
	// expression 将由 CEL 求值以创建 [JSON patch](https://jsonpatch.com/)。
	// 参考：https://github.com/google/cel-spec
	//
	// expression 必须返回一个 JSONPatch 值数组。
	//
	// 例如，下面的 CEL 表达式返回一个有条件修改值的 JSON patch：
	//
	//	  [
	//	    JSONPatch{op: "test", path: "/spec/example", value: "Red"},
	//	    JSONPatch{op: "replace", path: "/spec/example", value: "Green"}
	//	  ]
	//
	// 要为 patch 值定义对象，可使用 Object 类型。例如：
	//
	//	  [
	//	    JSONPatch{
	//	      op: "add",
	//	      path: "/spec/selector",
	//	      value: Object.spec.selector{matchLabels: {"environment": "test"}}
	//	    }
	//	  ]
	//
	// 若要将含 '/' 和 '~' 的字符串用作 JSONPatch 路径键，使用 "jsonpatch.escapeKey"。例如：
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
	// - 'JSONPatch' - JSON Patch 操作的 CEL 类型，包含 'op'、'from'、'path' 和 'value' 字段，
	//   详见 [JSON patch](https://jsonpatch.com/)。'value' 字段可设为 string、integer、
	//   array、map 或 object。若设置，'path' 和 'from' 字段必须设为
	//   [JSON pointer](https://datatracker.ietf.org/doc/html/rfc6901/) 字符串，
	//   其中可用 'jsonpatch.escapeKey()' CEL 函数转义含 '/' 和 '~' 的路径键。
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）。
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）。
	//
	// CEL 表达式可访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用变量：
	//
	// - 'object' - 来自入站请求的对象。DELETE 请求时值为 null。
	// - 'oldObject' - 现有对象。CREATE 请求时值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的 policy binding 所引用的参数资源。仅当策略具有 ParamKind 时填充。
	// - 'namespaceObject' - 入站对象所属的命名空间对象。集群作用域资源时值为 null。
	// - 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - CEL Authorizer，可用于对请求的主体（用户或 service account）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并配置了请求资源的 CEL ResourceCheck。
	//
	// CEL 表达式可访问 [Kubernetes CEL 函数库](https://kubernetes.io/docs/reference/using-api/cel/#cel-options-language-features-and-libraries)
	// 以及：
	//
	// - 'jsonpatch.escapeKey' - 执行 JSONPatch 键转义，将 '~' 和 '/' 分别转义为 '~0' 和 `~1'。
	//
	//
	// 仅可访问形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名。
	// 必需。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingAdmissionPolicyBinding

// MutatingAdmissionPolicyBinding 将 MutatingAdmissionPolicy 与参数化资源绑定。
// MutatingAdmissionPolicyBinding 和可选的参数资源共同定义集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个 binding 会使其策略被评估 N 次：不使用 params 的
// 策略/binding 中 N 为 1，否则 N 为 binding 选择的参数数量。
// 每次评估受 [运行时成本预算](https://kubernetes.io/docs/reference/using-api/cel/#runtime-cost-budget) 约束。
//
// 添加/删除策略、binding 或参数不会影响给定（策略、binding、参数）组合是否在其自身 CEL 预算内。
type MutatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 定义 MutatingAdmissionPolicyBinding 的期望行为。
	Spec MutatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:prerelease-lifecycle-gen:replacement=admissionregistration.k8s.io,v1,MutatingAdmissionPolicyBindingList

// MutatingAdmissionPolicyBindingList 是 MutatingAdmissionPolicyBinding 的列表。
type MutatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 的列表。
	Items []MutatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicyBindingSpec 是 MutatingAdmissionPolicyBinding 的规格。
type MutatingAdmissionPolicyBindingSpec struct {
	// policyName 引用此 MutatingAdmissionPolicyBinding 绑定到的 MutatingAdmissionPolicy 名称。
	// 若被引用的资源不存在，该 binding 被视为无效并会被忽略。
	// 必需。
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// paramRef 指定用于配置准入策略的参数资源，应指向被绑定 MutatingAdmissionPolicy
	// 的 spec.ParamKind 中指定类型的资源。若策略指定了 ParamKind 但 ParamRef 引用的资源
	// 不存在，该 binding 被视为配置有误，将应用 MutatingAdmissionPolicy 的 FailurePolicy。
	// 若策略未指定 ParamKind，则忽略此字段，规则在没有参数的情况下评估。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// matchResources 限制哪些资源匹配此 binding 并可被其变更。
	// 注意若 matchResources 匹配某资源，该资源还必须匹配策略的 matchConstraints 和
	// matchConditions 后才可被变更。
	// 当 matchResources 未设置时，它不约束资源匹配，资源只需匹配策略的 matchConstraints
	// 和 matchConditions 即可被变更。
	// 此外，matchResources.resourceRules 可选，未设置时不约束匹配。
	// 注意这与 MutatingAdmissionPolicy 的 matchConstraints 不同，后者要求 resourceRules。
	// 允许 CREATE、UPDATE 和 CONNECT 操作，不能匹配 DELETE 操作。
	// '*' 匹配 CREATE、UPDATE 和 CONNECT。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`
}
