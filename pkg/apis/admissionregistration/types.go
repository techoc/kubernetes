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

package admissionregistration

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Rule 是 APIGroups、APIVersion 与 Resources 组成的元组。建议确保该元组展开后的所有组合都是合法的。
type Rule struct {
	// APIGroups 是资源所属的 API 组。'*' 表示所有组。
	// 如果出现 '*'，该切片的长度必须为 1。
	// 必填。
	APIGroups []string

	// APIVersions 是资源所属的 API 版本。'*' 表示所有版本。
	// 如果出现 '*'，该切片的长度必须为 1。
	// 必填。
	APIVersions []string

	// Resources 是该规则适用的资源列表。
	//
	// 例如：
	// 'pods' 表示 pods。
	// 'pods/log' 表示 pods 的 log 子资源。
	// '*' 表示所有资源，但不包括子资源。
	// 'pods/*' 表示 pods 的所有子资源。
	// '*/scale' 表示所有资源的 scale 子资源。
	// '*/*' 表示所有资源及其子资源。
	//
	// 如果出现通配符，校验规则会确保各资源之间不重叠。
	//
	// 根据外层对象的不同，可能不允许使用子资源。
	// 必填。
	Resources []string

	// scope 指定该规则的作用域。
	// 合法取值为 "Cluster"、"Namespaced" 和 "*"。
	// "Cluster" 表示只有集群级资源才匹配该规则。
	// Namespace 这一 API 对象属于集群级资源。
	// "Namespaced" 表示只有命名空间级资源才匹配该规则。
	// "*" 表示不限制作用域。
	// 子资源与其父资源的作用域相同。
	// 默认为 "*"。
	//
	// +optional
	Scope *ScopeType
}

// ScopeType 指定所使用的作用域类型。
type ScopeType string

// ScopeType 的合法取值。
const (
	// ClusterScope 表示作用域限定为集群级对象。
	// Namespace 对象属于集群级资源。
	ClusterScope ScopeType = "Cluster"
	// NamespacedScope 表示作用域限定为命名空间级对象。
	NamespacedScope ScopeType = "Namespaced"
	// AllScopes 表示包含所有作用域。
	AllScopes ScopeType = "*"
)

// ParameterNotFoundActionType 指定一种失败策略，用于定义当其 perNamespaceParamRef
// 引用的参数未找到时应如何评估绑定。
type ParameterNotFoundActionType string

// 参数未找到时允许的处置动作。
const (
	// Allow 表示当找不到任何参数资源时，所有请求都会被准入。
	AllowAction ParameterNotFoundActionType = "Allow"
	// Deny 表示当找不到任何参数资源时，所有请求都会被拒绝。
	DenyAction ParameterNotFoundActionType = "Deny"
)

// FailurePolicyType 指定失败策略的类型。
type FailurePolicyType string

// 失败策略的合法取值。
const (
	// Ignore 表示调用准入 webhook 或准入策略时发生的错误会被忽略。
	Ignore FailurePolicyType = "Ignore"
	// Fail 表示调用准入 webhook 或准入策略时发生的错误会导致资源准入失败。
	Fail FailurePolicyType = "Fail"
)

// MatchPolicyType 指定匹配策略的类型。
type MatchPolicyType string

// 匹配策略的合法取值。
const (
	// Exact 表示只有当请求与给定规则完全匹配时，才会发送给准入 webhook 或准入策略。
	Exact MatchPolicyType = "Exact"
	// Equivalent 表示只要请求通过另一个 API 组或版本修改了规则中列出的资源，
	// 就应发送给准入 webhook 或准入策略。
	Equivalent MatchPolicyType = "Equivalent"
)

// SideEffectClass 表示调用 webhook 所产生的副作用类型。
type SideEffectClass string

// webhook 副作用类型的合法取值。
const (
	// SideEffectClassUnknown 表示不清楚调用 webhook 会有哪些副作用。
	// 如果带 dry-run 属性的请求会触发对该 webhook 的调用，则该请求会直接失败。
	SideEffectClassUnknown SideEffectClass = "Unknown"
	// SideEffectClassNone 表示调用 webhook 不会有任何副作用。
	SideEffectClassNone SideEffectClass = "None"
	// SideEffectClassSome 表示调用 webhook 可能会产生副作用。
	// 如果带 dry-run 属性的请求会触发对该 webhook 的调用，则该请求会直接失败。
	SideEffectClassSome SideEffectClass = "Some"
	// SideEffectClassNoneOnDryRun 表示调用 webhook 可能会产生副作用，但如果被审查的
	// 请求带有 dry-run 属性，这些副作用会被抑制。
	SideEffectClassNoneOnDryRun SideEffectClass = "NoneOnDryRun"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicy 描述了一种准入校验策略的定义，它只接受或拒绝对象而不修改对象。
type ValidatingAdmissionPolicy struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// ValidatingAdmissionPolicy 期望行为的规格说明。
	Spec ValidatingAdmissionPolicySpec
	// ValidatingAdmissionPolicy 的状态，包括用于判断策略是否按预期运行的警告信息。
	// 由系统填充。
	// 只读。
	// +optional
	Status ValidatingAdmissionPolicyStatus
}

// ValidatingAdmissionPolicyStatus 表示准入校验策略的状态。
type ValidatingAdmissionPolicyStatus struct {
	// 控制器观测到的 generation。
	// +optional
	ObservedGeneration int64
	// 每个表达式的类型检查结果。
	// 该字段出现即表示类型检查已完成。
	// +optional
	TypeChecking *TypeChecking
	// conditions 表示对策略当前状态的最新可用观测结果。
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition
}

// ValidatingAdmissionPolicyConditionType 是准入校验策略的条件类型。
type ValidatingAdmissionPolicyConditionType string

// TypeChecking 包含对 ValidatingAdmissionPolicy 中表达式进行类型检查的结果。
type TypeChecking struct {
	// 每个表达式的类型检查警告。
	// +optional
	// +listType=atomic
	ExpressionWarnings []ExpressionWarning
}

// ExpressionWarning 是面向特定表达式的警告信息。
type ExpressionWarning struct {
	// 指向该表达式的字段路径。
	// 例如，对 validations 中第一项的表达式的引用为 "spec.validations[0].expression"。
	FieldRef string
	// 人类可读形式的类型检查信息内容。
	// 每条警告行包含表达式被检查时所依据的类型，后接编译器返回的类型检查错误。
	Warning string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicyList 是 ValidatingAdmissionPolicy 的列表。
type ValidatingAdmissionPolicyList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// ValidatingAdmissionPolicy 列表。
	Items []ValidatingAdmissionPolicy
}

// ValidatingAdmissionPolicySpec 是 AdmissionPolicy 期望行为的规格说明。
type ValidatingAdmissionPolicySpec struct {
	// ParamKind 指定用于为该策略提供参数（参数化）的资源类型。
	// 若未指定，则该策略没有参数，也不会向校验表达式提供 param CEL 变量。
	// 如果 ParamKind 指向一个不存在的 Kind，则该策略定义为配置错误，会应用 FailurePolicy。
	// 如果指定了 paramKind 但 ValidatingAdmissionPolicyBinding 中未设置 paramRef，则 params 变量为 null。
	// +optional
	ParamKind *ParamKind

	// MatchConstraints 指定该策略旨在校验哪些资源。
	// 当请求匹配_所有_约束（Constraint）时，AdmissionPolicy 才会关注该请求。
	// 但是，为防止集群陷入无法通过 API 恢复的不稳定状态，
	// ValidatingAdmissionPolicy 不能匹配 ValidatingAdmissionPolicy 和 ValidatingAdmissionPolicyBinding。
	// 必填。
	MatchConstraints *MatchResources

	// validations 包含用于校验准入请求的 CEL 表达式。
	// validations 和 auditAnnotations 不能同时为空；至少需要提供一个 validations 或 auditAnnotations。
	// +optional
	Validations []Validation

	// failurePolicy 定义如何处理该准入策略的失败。失败可能由以下原因引起：CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义与绑定。
	//
	// 如果 spec.paramKind 指向不存在的 Kind，则策略无效。
	// 如果 spec.paramRef.name 指向不存在的资源，则绑定无效。
	//
	// failurePolicy 不定义如何处置计算结果为 false 的校验。
	//
	// 当 failurePolicy 设为 Fail 时，由 ValidatingAdmissionPolicyBinding 的 validationActions
	// 定义如何强制执行失败。
	//
	// 允许的值为 Ignore 或 Fail。默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType

	// auditAnnotations 包含 CEL 表达式，用于为 API 请求的审计事件生成审计注解。
	// validations 和 auditAnnotations 不能同时为空；至少需要提供一个 validations 或 auditAnnotations。
	// 每条 ValidatingAdmissionPolicy 最多允许 20 个 auditAnnotation。
	// +optional
	AuditAnnotations []AuditAnnotation

	// MatchConditions 是请求被校验前必须满足的条件列表。
	// 匹配条件用于过滤已被规则、namespaceSelector 和 objectSelector 匹配过的请求。
	// 空的 matchConditions 列表匹配所有请求。
	// 最多允许 64 个匹配条件。
	//
	// 如果提供了参数对象，则可以通过 `params` 句柄访问它，方式与校验表达式相同。
	//
	// 精确的匹配逻辑如下（按顺序）：
	//   1. 如果_任意_ matchCondition 计算结果为 FALSE，则跳过该策略。
	//   2. 如果_所有_ matchCondition 计算结果均为 TRUE，则对该策略进行求值。
	//   3. 如果任一 matchCondition 计算出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，则拒绝该请求
	//      - 若 failurePolicy=Ignore，则跳过该策略
	//
	// +optional
	MatchConditions []MatchCondition

	// Variables 包含可在其他表达式组合中使用的变量定义。
	// 每个变量都定义为一个具名的 CEL 表达式。
	// 此处定义的变量在策略的其他表达式中可通过 `variables` 访问，但 MatchConditions 除外，
	// 因为 MatchConditions 会在策略其余部分之前求值。
	//
	// 变量的表达式可以引用列表中更早定义的其他变量，但不能引用其后的变量。
	// 因此，Variables 必须按首次出现的顺序排序且无环。
	// +listType=atomic
	// +optional
	Variables []Variable
}

// ParamKind 是由 Group、Kind 和 Version 组成的元组。
type ParamKind struct {
	// APIVersion 是资源所属的 API 组版本。
	// 格式为 "group/version"。
	// 必填。
	APIVersion string

	// Kind 是资源所属的 API kind。
	// 必填。
	Kind string
}

// Validation 指定用于执行校验的 CEL 表达式。
type Validation struct {
	// Expression 表示将由 CEL 求值的表达式。
	// 参考：https://github.com/google/cel-spec
	// CEL 表达式可以访问 API 请求/响应的内容，这些内容被组织为 CEL 变量以及一些其他有用的变量：
	//
	//'object' - 来自传入请求的对象。对于 DELETE 请求，其值为 null。
	//'oldObject' - 已有的对象。对于 CREATE 请求，其值为 null。
	//'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	//'params' - 被评估的策略绑定所引用的参数资源。仅当策略具有 ParamKind 时才会填充。
	//'namespaceObject' - 传入对象所属的命名空间对象。对于集群级资源，其值为 null。
	//'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//  例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - 一个 CEL Authorizer，可用于对请求主体（用户或服务账号）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置而成的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问。
	// 其他元数据属性均不可访问。
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名才可访问。
	// 在表达式中访问时，可访问的属性名按如下规则转义：
	// - '__' 转义为 '__underscores__'
	// - '.' 转义为 '__dot__'
	// - '-' 转义为 '__dash__'
	// - '/' 转义为 '__slash__'
	// - 与 CEL 保留关键字完全匹配的属性名转义为 '__{keyword}__'。这些关键字包括：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return"。
	// 示例：
	//   - 访问名为 "namespace" 的属性：{"Expression": "object.__namespace__ > 0"}
	//   - 访问名为 "x-prop" 的属性：{"Expression": "object.x__dash__prop > 0"}
	//   - 访问名为 "redact__d" 的属性：{"Expression": "object.redact__underscores__d > 0"}
	//
	// 对 list 类型为 'set' 或 'map' 的数组所做的相等比较会忽略元素顺序，即 [1, 2] == [2, 1]。
	// 对带有 x-kubernetes-list-type 的数组所做的拼接遵循该列表类型的语义：
	//   - 'set'：`X + Y` 执行并集操作，保留 `X` 中所有元素的位置，
	//     并将 `Y` 中不相交的元素追加到末尾，保持其相对顺序。
	//   - 'map'：`X + Y` 执行合并操作，保留 `X` 中所有键的位置，但当 `X` 与 `Y` 的键集合相交时，
	//     对应的值会被 `Y` 中的值覆盖。`Y` 中键不相交的元素会被追加，保持其相对顺序。
	// 必填。
	Expression string
	// Message 表示校验失败时显示的消息。当 Expression 包含换行符时必须提供该消息，且消息本身不得包含换行符。
	// 如果未设置，消息为 "failed rule: {Rule}"。
	// 例如 "must be a URL with the host matching spec.host"。
	// 如果指定了 ExpressMessage，则忽略 Message。
	// 如果 Expression 包含换行符，则 Eith Message 或 ExpressMessage 为必填。
	// 消息不得包含换行符。
	// 如果未设置，消息为 "failed Expression: {Expression}"。
	// +optional
	Message string
	// Reason 表示对校验失败原因的机器可读描述。
	// 如果这是列表中第一个失败的校验，则该原因以及对应的 HTTP 响应码
	// 会包含在返回给客户端的 HTTP 响应中。
	// 当前支持的原因有："Unauthorized"、"Forbidden"、"Invalid"、"RequestEntityTooLarge"。
	// 如果未设置，则响应中使用 StatusReasonInvalid。
	// +optional
	Reason *metav1.StatusReason
	// messageExpression 声明一个 CEL 表达式，其求值结果为该规则失败时返回的校验失败消息。
	// 由于 messageExpression 用作失败消息，它必须求值为字符串。
	// 如果一条校验同时包含 message 和 messageExpression，则校验失败时使用 messageExpression。
	// 如果 messageExpression 求值出现运行时错误，会记录该错误，并像未设置 messageExpression 字段一样
	// 生成校验失败消息。如果 messageExpression 求值为空字符串、仅包含空格的字符串或含换行符的字符串，
	// 同样会像未设置该字段一样生成校验失败消息，并记录该字段产生了空字符串/纯空格字符串/含换行符字符串的事实。
	// messageExpression 可访问与 `expression` 相同的所有变量，但 'authorizer' 和 'authorizer.requestResource' 除外。
	// 示例：
	// "object.x must be less than max ("+string(params.max)+")"
	// +optional
	MessageExpression string
}

// Variable 是用于组合（composition）的变量定义。变量定义为一个具名表达式。
// +structType=atomic
type Variable struct {
	// Name 是变量的名称。该名称必须是合法的 CEL 标识符，且在所有变量中唯一。
	// 可以通过 `variables` 在其他表达式中访问该变量。
	// 例如，若名称为 "foo"，则该变量可通过 `variables.foo` 访问。
	Name string

	// Expression 是会被求值并作为变量值的表达式。
	// 该 CEL 表达式可访问与 Validation 中的 CEL 表达式相同的标识符。
	Expression string
}

// AuditAnnotation 描述如何为 API 请求生成审计注解。
type AuditAnnotation struct {
	// key 指定审计注解的键。同一个 ValidatingAdmissionPolicy 的审计注解键必须唯一。
	// 该键必须是一个合格名称（[A-Za-z0-9][-A-Za-z0-9_.]*），且长度不超过 63 字节。
	//
	// 该键会与 ValidatingAdmissionPolicy 的资源名组合，构成审计注解键：
	// "{ValidatingAdmissionPolicy name}/{key}"。
	//
	// 如果某个准入 webhook 使用与此 ValidatingAdmissionPolicy 相同的资源名
	// 以及相同的审计注解键，则两者的注解键完全相同。
	// 在这种情况下，只有第一个用该键写入的注解会包含在审计事件中，
	// 后续所有同键注解都会被丢弃。
	//
	// 必填。
	Key string

	// valueExpression 表示由 CEL 求值以生成审计注解值的表达式。
	// 该表达式必须求值为字符串或 null。如果求值为字符串，则该字符串作为审计注解的值包含进来；
	// 如果求值为 null 或空字符串，则省略该审计注解。
	// valueExpression 的长度不得超过 5kb。
	// 如果 valueExpression 的结果长度超过 10kb，则会被截断到 10kb。
	//
	// 如果有多个 ValidatingAdmissionPolicyBinding 资源匹配某个 API 请求，
	// 则会针对每个绑定对 valueExpression 求值一次。
	// 所有 valueExpression 产生的唯一值会以逗号分隔的列表形式连接在一起。
	//
	// 必填。
	ValueExpression string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicyBinding 将 ValidatingAdmissionPolicy 与参数化的资源绑定在一起。
// ValidatingAdmissionPolicyBinding 与参数 CRD 共同定义了集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个绑定都会使其策略被求值 N 次：对于不使用参数的策略/绑定，N 为 1；
// 否则 N 为该绑定所选参数的数量。
//
// 策略的 CEL 表达式计算出的 CEL 开销必须低于最大 CEL 预算。
// 每次策略求值都有独立的 CEL 开销预算。
// 增加/删除策略、绑定或参数不会影响某个给定的（策略、绑定、参数）组合是否在其自身的 CEL 预算内。
type ValidatingAdmissionPolicyBinding struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// ValidatingAdmissionPolicyBinding 期望行为的规格说明。
	Spec ValidatingAdmissionPolicyBindingSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicyBindingList 是 PolicyBinding 的列表。
type ValidatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// PolicyBinding 列表。
	Items []ValidatingAdmissionPolicyBinding
}

// ValidatingAdmissionPolicyBindingSpec 是 ValidatingAdmissionPolicyBinding 的规格说明。
type ValidatingAdmissionPolicyBindingSpec struct {
	// PolicyName 引用该 ValidatingAdmissionPolicyBinding 所绑定的 ValidatingAdmissionPolicy 名称。
	// 如果引用的资源不存在，则该绑定被视为无效并被忽略。
	// 必填。
	PolicyName string

	// paramRef 指定用于配置准入控制策略的参数资源。
	// 它应指向所绑定 ValidatingAdmissionPolicy 的 ParamKind 中指定类型的资源。
	// 如果策略指定了 ParamKind，但 ParamRef 引用的资源不存在，则该绑定被视为配置错误，
	// 并会应用 ValidatingAdmissionPolicy 的 FailurePolicy。
	// 如果策略未指定 ParamKind，则忽略该字段，并在不带参数的情况下求值规则。
	// +optional
	ParamRef *ParamRef

	// MatchResources 声明哪些资源匹配此绑定并将由其校验。
	// 请注意，这会与策略的 matchConstraints 取交集，因此此绑定只能选中已被策略匹配的请求。
	// 如果未设置，则该绑定会校验策略匹配的所有资源。
	// 当 resourceRules 未设置时，它不限制资源匹配。如果某个资源被该对象的其他字段匹配，则会被校验。
	// 请注意，这与 ValidatingAdmissionPolicy 的 matchConstraints 不同，后者要求必须提供 resourceRules。
	// +optional
	MatchResources *MatchResources

	// validationActions 声明如何强制执行所引用 ValidatingAdmissionPolicy 的校验（Validations）。
	// 如果某条校验求值为 false，则始终按这些动作强制执行。
	//
	// 只有当 FailurePolicy 设为 Fail 时，ValidatingAdmissionPolicy 的 FailurePolicy 所定义的失败
	// 才会按这些动作强制执行；否则失败会被忽略。这包括策略的编译错误、运行时错误和配置错误。
	//
	// validationActions 声明为一组动作值，顺序无关紧要，且不得包含重复的动作。
	//
	// 支持的动作值有：
	//
	// "Deny" 表示校验失败会导致请求被拒绝。
	//
	// "Warn" 表示校验失败会通过 HTTP Warning 头报告给请求客户端，警告码为 299。
	// 无论准入响应是允许还是拒绝，都可以发送警告。
	//
	// "Audit" 表示校验失败会被包含在针对该请求发布的审计事件中。
	// 审计事件将包含一个 `validation.policy.admission.k8s.io/validation_failure` 审计注解，
	// 其值包含校验失败的详细信息，格式为 JSON 对象列表，每个对象包含以下字段：
	// - message：校验失败消息字符串
	// - policy：ValidatingAdmissionPolicy 的资源名
	// - binding：ValidatingAdmissionPolicyBinding 的资源名
	// - expressionIndex：在 ValidatingAdmissionPolicy 中失败的校验的索引
	// - validationActions：针对该校验失败执行的强制动作
	// 审计注解示例：
	// `"validation.policy.admission.k8s.io/validation_failure": "[{\"message\": \"Invalid value\", {\"policy\": \"policy.example.com\", {\"binding\": \"policybinding.example.com\", {\"expressionIndex\": \"1\", {\"validationActions\": [\"Audit\"]}]"`
	//
	// 客户端应当预期能处理额外的值，即忽略任何无法识别的值。
	//
	// "Deny" 和 "Warn" 不能同时使用，因为这种组合会在 API 响应体和 HTTP 警告头中
	// 无谓地重复报告同一次校验失败。
	//
	// 必填。
	ValidationActions []ValidationAction
}

// ParamRef 描述如何定位参数，这些参数会作为输入提供给策略绑定所应用的规则表达式。
// +structType=atomic
type ParamRef struct {
	// name 是被引用资源的名称。
	//
	// 必须设置 `name` 或 `selector` 之一，但 `name` 与 `selector` 互斥：
	// 设置其中一个时，另一个必须留空。
	//
	// 只需设置 `name` 字段、留空 `selector`，并在 `paramKind` 为命名空间级时设置 namespace，
	// 即可配置一个对所有准入请求都使用的单一参数。
	//
	// +optional
	Name string

	// namespace 是被引用资源所在的命名空间，用于将参数搜索限定到特定命名空间，
	// 对 `name` 和 `selector` 字段均适用。
	//
	// 在策略中指定命名空间级的 `paramKind` 并留空此字段，即可使用按命名空间划分的参数。
	//
	// - 如果 `paramKind` 是集群级的，则必须不设置此字段，否则会导致配置错误。
	//
	// - 如果 `paramKind` 是命名空间级的，留空此字段时会使用被准入对象所在的命名空间。
	//   请注意，如果留空，则该绑定不得匹配任何集群级资源，否则会导致错误。
	//
	// +optional
	Namespace string

	// selector 可用于按标签匹配多个参数对象。
	// 提供 selector: {} 可匹配该 ParamKind 的所有资源。
	//
	// 如果找到多个参数，会针对策略表达式对它们逐一求值，并将结果按逻辑与（AND）组合。
	//
	// 必须设置 `name` 或 `selector` 之一，但 `name` 与 `selector` 互斥：
	// 设置其中一个时，另一个必须留空。
	//
	// +optional
	Selector *metav1.LabelSelector

	// parameterNotFoundAction 控制当资源存在、name 或 selector 有效，但绑定未匹配到任何参数时的行为。
	// 如果设为 `Allow`，则没有匹配到参数会被该绑定视为校验成功。
	// 如果设为 `Deny`，则没有匹配到参数会受策略的 `failurePolicy` 约束。
	//
	// 允许值为 `Allow` 或 `Deny`。
	//
	// 必填。
	ParameterNotFoundAction *ParameterNotFoundActionType
}

// MatchResources 根据对象是否满足匹配条件，决定是否对其运行准入控制策略。
// 排除规则优先于包含规则（如果某资源同时匹配两者，则会被排除）。
type MatchResources struct {
	// NamespaceSelector 根据对象所在命名空间是否匹配该选择器，决定是否对该对象运行准入控制策略。
	// 如果对象本身是一个命名空间，则匹配针对 object.metadata.labels 进行。
	// 如果对象是另一个集群级资源，则永远不会跳过该策略。
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
	// 反之，如果只想对命名空间关联了 "environment" 为 "prod" 或 "staging" 的对象运行该策略，
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
	// 更多标签选择器示例参见：
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector
	// ObjectSelector 根据对象是否具有匹配的标签来决定是否运行该策略。
	// objectSelector 会同时针对将发送给 CEL 策略的 oldObject 和 newObject 求值，
	// 只要其中任一对象匹配选择器即视为匹配。null 对象（CREATE 时为 oldObject，
	// DELETE 时为 newObject）或无法具有标签的对象（如 DeploymentRollback 或 PodProxyOptions 对象）
	// 均视为不匹配。
	// 仅当 webhook 是选择加入（opt-in）时才使用对象选择器，因为终端用户可以通过设置标签
	// 来跳过该准入 webhook。
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector
	// ResourceRules 描述该策略匹配哪些资源/子资源上的哪些操作。
	// 只要匹配_任意_ Rule，该策略就关注该操作。
	// +listType=atomic
	// +optional
	ResourceRules []NamedRuleWithOperations
	// ExcludeResourceRules 描述该策略不应关注哪些资源/子资源上的哪些操作。
	// 排除规则优先于包含规则（如果某资源同时匹配两者，则会被排除）。
	// +listType=atomic
	// +optional
	ExcludeResourceRules []NamedRuleWithOperations
	// matchPolicy 定义如何使用 "MatchResources" 列表来匹配传入的请求。
	// 允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：只有当请求与指定规则完全匹配时才匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 但 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给该策略。
	//
	// - Equivalent：只要请求修改了规则中列出的资源，即使通过另一个 API 组或版本，也会匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给该策略。
	//
	// 默认为 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType
}

// ValidationAction 指定一种策略强制动作。
type ValidationAction string

// 策略强制动作的合法取值。
const (
	// Deny 表示校验失败会导致请求被拒绝。
	Deny ValidationAction = "Deny"
	// Warn 表示校验失败会通过 HTTP Warning 头（警告码 299）报告给请求客户端。
	// 无论准入响应是允许还是拒绝，都可以发送警告。
	Warn ValidationAction = "Warn"
	// Audit 表示校验失败会被包含在针对该请求发布的审计事件中。
	// 审计事件将包含一个 `validation.policy.admission.k8s.io/validation_failure` 审计注解，
	// 其值包含校验失败的详细信息。
	Audit ValidationAction = "Audit"
)

// NamedRuleWithOperations 是由 Operations、Resources 以及 ResourceNames 组成的元组。
type NamedRuleWithOperations struct {
	// ResourceNames 是该规则适用名称的可选白名单。空集合表示允许所有名称。
	// +optional
	ResourceNames []string
	// RuleWithOperations 是由 Operations 和 Resources 组成的元组。
	RuleWithOperations RuleWithOperations
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingWebhookConfiguration 描述一种准入 webhook 的配置，它只接受或拒绝对象而不修改对象。
type ValidatingWebhookConfiguration struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// Webhooks 是 webhook 及其所影响的资源与操作的列表。
	// +optional
	Webhooks []ValidatingWebhook
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingWebhookConfigurationList 是 ValidatingWebhookConfiguration 的列表。
type ValidatingWebhookConfigurationList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// ValidatingWebhookConfiguration 列表。
	Items []ValidatingWebhookConfiguration
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MutatingWebhookConfiguration 描述一种准入 webhook 的配置，它可以接受或拒绝对象，也可以修改对象。
type MutatingWebhookConfiguration struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// Webhooks 是 webhook 及其所影响的资源与操作的列表。
	// +optional
	Webhooks []MutatingWebhook
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MutatingWebhookConfigurationList 是 MutatingWebhookConfiguration 的列表。
type MutatingWebhookConfigurationList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// MutatingWebhookConfiguration 列表。
	Items []MutatingWebhookConfiguration
}

// ValidatingWebhook 描述一个准入 webhook 及其适用的资源和操作。
type ValidatingWebhook struct {
	// 准入 webhook 的名称。
	// 名称应为完全限定名，例如 imagepolicy.kubernetes.io，其中 "imagepolicy" 是 webhook 名称，
	// kubernetes.io 是组织名称。
	// 必填。
	Name string

	// ClientConfig 定义如何与该 hook 通信。
	// 必填。
	ClientConfig WebhookClientConfig

	// Rules 描述该 webhook 关注哪些资源/子资源上的哪些操作。
	// 只要匹配_任意_ Rule，该 webhook 就关注该操作。
	Rules []RuleWithOperations

	// FailurePolicy 定义如何处理来自准入端点的无法识别的错误，
	// 允许值为 Ignore 或 Fail，默认为 Ignore。
	// +optional
	FailurePolicy *FailurePolicyType

	// matchPolicy 定义如何使用 "rules" 列表来匹配传入的请求。
	// 允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：只有当请求与指定规则完全匹配时才匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 但 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给该 webhook。
	//
	// - Equivalent：只要请求修改了规则中列出的资源，即使通过另一个 API 组或版本，也会匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给该 webhook。
	//
	// +optional
	MatchPolicy *MatchPolicyType

	// NamespaceSelector 根据对象所在命名空间是否匹配该选择器，决定是否对该对象运行 webhook。
	// 如果对象本身是一个命名空间，则匹配针对 object.metadata.labels 进行。
	// 如果对象是另一个集群级资源，则永远不会跳过该 webhook。
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
	// 反之，如果只想对命名空间关联了 "environment" 为 "prod" 或 "staging" 的对象运行 webhook，
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
	// 更多标签选择器示例参见：
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector

	// ObjectSelector 根据对象是否具有匹配的标签来决定是否运行 webhook。
	// objectSelector 会同时针对将发送给 webhook 的 oldObject 和 newObject 求值，
	// 只要其中任一对象匹配选择器即视为匹配。null 对象（CREATE 时为 oldObject，
	// DELETE 时为 newObject）或无法具有标签的对象（如 DeploymentRollback 或 PodProxyOptions 对象）
	// 均视为不匹配。
	// 仅当 webhook 是选择加入（opt-in）时才使用对象选择器，因为终端用户可以通过设置标签
	// 来跳过该准入 webhook。
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector

	// SideEffects 声明该 webhook 是否有副作用。
	// 可接受的值为：Unknown、None、Some、NoneOnDryRun。
	// 有副作用的 webhook 必须实现一套协调（reconciliation）系统，因为请求可能会被准入链中
	// 后续步骤拒绝，从而需要撤销这些副作用。
	// 带 dryRun 属性的请求如果匹配到 sideEffects 为 Unknown 或 Some 的 webhook，会被自动拒绝。
	// 默认为 Unknown。
	// +optional
	SideEffects *SideEffectClass

	// TimeoutSeconds 指定该 webhook 的超时时间。超时后，根据失败策略，
	// 要么忽略该 webhook 调用，要么让 API 调用失败。
	// 超时值必须在 1 到 30 秒之间。
	// +optional
	TimeoutSeconds *int32

	// AdmissionReviewVersions 是 webhook 期望的 `AdmissionReview` 版本的有序首选列表。
	// API 服务器会尝试使用列表中它支持的第一个版本。如果列表中的所有版本都不被 API 服务器支持，
	// 则该对象的校验会失败。
	// 如果 webhook 配置已经以 API 服务器无法理解的版本持久化，则对该 webhook 的调用会失败，
	// 并受失败策略约束。
	// +optional
	AdmissionReviewVersions []string

	// MatchConditions 是请求被发送到该 webhook 之前必须满足的条件列表。
	// 匹配条件用于过滤已被规则、namespaceSelector 和 objectSelector 匹配过的请求。
	// 空的 matchConditions 列表匹配所有请求。
	// 最多允许 64 个匹配条件。
	//
	// 精确的匹配逻辑如下（按顺序）：
	//   1. 如果_任意_ matchCondition 计算结果为 FALSE，则跳过该 webhook。
	//   2. 如果_所有_ matchCondition 计算结果均为 TRUE，则调用该 webhook。
	//   3. 如果任一 matchCondition 计算出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，则拒绝该请求
	//      - 若 failurePolicy=Ignore，则忽略该错误并跳过该 webhook
	//
	// +optional
	MatchConditions []MatchCondition
}

// MutatingWebhook 描述一个准入 webhook 及其适用的资源和操作。
type MutatingWebhook struct {
	// 准入 webhook 的名称。
	// 名称应为完全限定名，例如 imagepolicy.kubernetes.io，其中 "imagepolicy" 是 webhook 名称，
	// kubernetes.io 是组织名称。
	// 必填。
	Name string

	// ClientConfig 定义如何与该 hook 通信。
	// 必填。
	ClientConfig WebhookClientConfig

	// Rules 描述该 webhook 关注哪些资源/子资源上的哪些操作。
	// 只要匹配_任意_ Rule，该 webhook 就关注该操作。
	Rules []RuleWithOperations

	// FailurePolicy 定义如何处理来自准入端点的无法识别的错误，
	// 允许值为 Ignore 或 Fail，默认为 Ignore。
	// +optional
	FailurePolicy *FailurePolicyType

	// matchPolicy 定义如何使用 "rules" 列表来匹配传入的请求。
	// 允许值为 "Exact" 或 "Equivalent"。
	//
	// - Exact：只有当请求与指定规则完全匹配时才匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 但 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求不会被发送给该 webhook。
	//
	// - Equivalent：只要请求修改了规则中列出的资源，即使通过另一个 API 组或版本，也会匹配该请求。
	// 例如，如果 deployments 可通过 apps/v1、apps/v1beta1 和 extensions/v1beta1 修改，
	// 且 "rules" 只包含 `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]`，
	// 则对 apps/v1beta1 或 extensions/v1beta1 的请求会被转换为 apps/v1 后发送给该 webhook。
	//
	// +optional
	MatchPolicy *MatchPolicyType

	// NamespaceSelector 根据对象所在命名空间是否匹配该选择器，决定是否对该对象运行 webhook。
	// 如果对象本身是一个命名空间，则匹配针对 object.metadata.labels 进行。
	// 如果对象是另一个集群级资源，则永远不会跳过该 webhook。
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
	// 反之，如果只想对命名空间关联了 "environment" 为 "prod" 或 "staging" 的对象运行 webhook，
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
	// 更多标签选择器示例参见：
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	//
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector

	// ObjectSelector 根据对象是否具有匹配的标签来决定是否运行 webhook。
	// objectSelector 会同时针对将发送给 webhook 的 oldObject 和 newObject 求值，
	// 只要其中任一对象匹配选择器即视为匹配。null 对象（CREATE 时为 oldObject，
	// DELETE 时为 newObject）或无法具有标签的对象（如 DeploymentRollback 或 PodProxyOptions 对象）
	// 均视为不匹配。
	// 仅当 webhook 是选择加入（opt-in）时才使用对象选择器，因为终端用户可以通过设置标签
	// 来跳过该准入 webhook。
	// 默认为空的 LabelSelector，它匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector

	// SideEffects 声明该 webhook 是否有副作用。
	// 可接受的值为：Unknown、None、Some、NoneOnDryRun。
	// 有副作用的 webhook 必须实现一套协调（reconciliation）系统，因为请求可能会被准入链中
	// 后续步骤拒绝，从而需要撤销这些副作用。
	// 带 dryRun 属性的请求如果匹配到 sideEffects 为 Unknown 或 Some 的 webhook，会被自动拒绝。
	// 默认为 Unknown。
	// +optional
	SideEffects *SideEffectClass

	// TimeoutSeconds 指定该 webhook 的超时时间。超时后，根据失败策略，
	// 要么忽略该 webhook 调用，要么让 API 调用失败。
	// 超时值必须在 1 到 30 秒之间。
	// +optional
	TimeoutSeconds *int32

	// AdmissionReviewVersions 是 webhook 期望的 `AdmissionReview` 版本的有序首选列表。
	// API 服务器会尝试使用列表中它支持的第一个版本。如果列表中的所有版本都不被 API 服务器支持，
	// 则该对象的校验会失败。
	// 如果 webhook 配置已经以 API 服务器无法理解的版本持久化，则对该 webhook 的调用会失败，
	// 并受失败策略约束。
	// +optional
	AdmissionReviewVersions []string

	// reinvocationPolicy 指示该 webhook 在单次准入评估中是否应被多次调用。
	// 允许值为 "Never" 和 "IfNeeded"。
	//
	// Never：在单次准入评估中，该 webhook 不会被调用超过一次。
	//
	// IfNeeded：如果在初次调用 webhook 之后，被准入的对象被其他准入插件修改，
	// 则该 webhook 会在该准入评估中至少被额外调用一次。
	// 指定此选项的 webhook *必须* 是幂等的，因此必须能够处理它们之前已准入过的对象。
	// 注意：
	// * 额外调用的次数不保证恰好为一次。
	// * 如果额外调用导致对象被进一步修改，不保证 webhook 会再次被调用。
	// * 使用此选项的 webhook 可能会被重新排序，以尽量减少额外调用的次数。
	// * 如果要在所有变更都确保完成后再校验对象，请改用校验型准入 webhook。
	//
	// 默认为 "Never"。
	// +optional
	ReinvocationPolicy *ReinvocationPolicyType

	// MatchConditions 是请求被发送到该 webhook 之前必须满足的条件列表。
	// 匹配条件用于过滤已被规则、namespaceSelector 和 objectSelector 匹配过的请求。
	// 空的 matchConditions 列表匹配所有请求。
	// 最多允许 64 个匹配条件。
	//
	// 精确的匹配逻辑如下（按顺序）：
	//   1. 如果_任意_ matchCondition 计算结果为 FALSE，则跳过该 webhook。
	//   2. 如果_所有_ matchCondition 计算结果均为 TRUE，则调用该 webhook。
	//   3. 如果任一 matchCondition 计算出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，则拒绝该请求
	//      - 若 failurePolicy=Ignore，则忽略该错误并跳过该 webhook
	//
	// +optional
	MatchConditions []MatchCondition
}

// ReinvocationPolicyType 指定当其他准入插件也执行修改时所采用的策略类型。
// +enum
type ReinvocationPolicyType string

// 重新调用策略的预定义取值。
var (
	// NeverReinvocationPolicy 表示在单次准入评估中，该变更操作不得被调用超过一次。
	NeverReinvocationPolicy ReinvocationPolicyType = "Never"
	// IfNeededReinvocationPolicy 表示如果在初次变更调用之后，被准入的对象被其他准入插件修改，
	// 则该变更操作在准入评估中可能至少被额外调用一次。
	IfNeededReinvocationPolicy ReinvocationPolicyType = "IfNeeded"
)

// RuleWithOperations 是由 Operations 和 Resources 组成的元组。建议确保该元组展开后的所有组合都是合法的。
type RuleWithOperations struct {
	// Operations 是准入 hook 关注的操作：CREATE、UPDATE，或用 * 表示所有操作。
	// 如果出现 '*'，该切片的长度必须为 1。
	// 必填。
	Operations []OperationType
	// Rule 被嵌入其中，描述该规则的其他条件，如 APIGroups、APIVersions、Resources 等。
	Rule
}

// OperationType 指定准入 hook 关注的操作类型。
type OperationType string

// 这些常量需要与 k8s.io/kubernetes/pkg/admission/interface.go 中定义的常量保持同步。
const (
	// OperationAll 用通配符 '*' 表示所有操作类型。
	OperationAll OperationType = "*"
	// Create 表示创建操作。
	Create OperationType = "CREATE"
	// Update 表示更新操作。
	Update OperationType = "UPDATE"
	// Delete 表示删除操作。
	Delete OperationType = "DELETE"
	// Connect 表示连接（子资源）操作。
	Connect OperationType = "CONNECT"
)

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
type WebhookClientConfig struct {
	// `url` 以标准 URL 形式（`scheme://host:port/path`）给出 webhook 的位置。
	// `url` 与 `service` 必须且只能指定其中一个。
	//
	// `host` 不应指向集群内运行的服务，而应使用 `service` 字段。
	// 在某些 API 服务器中，host 可能通过外部 DNS 解析（例如 kube-apiserver 无法解析集群内 DNS，
	// 因为那会违反分层原则）。`host` 也可以是 IP 地址。
	//
	// 请注意，除非你非常确定要在所有运行 apiserver 且可能需要调用该 webhook 的主机上都运行它，
	// 否则使用 `localhost` 或 `127.0.0.1` 作为 `host` 是有风险的。
	// 这类安装很可能不具备可移植性，即难以在新集群中重新搭建。
	//
	// scheme 必须是 "https"；URL 必须以 "https://" 开头。
	//
	// path 是可选的，如果存在，可以是 URL 中允许的任何字符串。
	// 可以通过 path 向 webhook 传递任意字符串，例如集群标识符。
	//
	// 不允许在 URL 中携带用户名或基本认证信息。
	// 同样不允许片段（"#..."）和查询参数（"?..."）。
	//
	// +optional
	URL *string

	// `service` 是对该 webhook 所对应服务的引用。`service` 或 `url` 必须指定其中一个。
	//
	// 如果 webhook 运行在集群内部，则应使用 `service`。
	//
	// +optional
	Service *ServiceReference

	// `caBundle` 是经过 PEM 编码的 CA 证书包，用于校验 webhook 的服务器证书。
	// 如果未指定，则使用 API 服务器上的系统信任根证书。
	// +optional
	CABundle []byte
}

// ServiceReference 保存对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// `namespace` 是服务所在的命名空间。
	// 必填。
	Namespace string
	// `name` 是服务的名称。
	// 必填。
	Name string

	// `path` 是可选的 URL 路径，会在发往该服务的所有请求中携带。
	// +optional
	Path *string

	// 如果指定，则表示承载 webhook 的服务上的端口。
	// `port` 应为合法端口号（1-65535，含两端）。
	// +optional
	Port int32
}

// MatchCondition 表示请求被发送到 webhook 之前必须满足的条件。
type MatchCondition struct {
	// Name 是该匹配条件的标识符，用于 MatchConditions 的策略性合并，同时也作为日志用途的标识符。
	// 好的名称应能描述对应的表达式。
	// Name 必须是限定名称，由字母数字字符、'-'、'_' 或 '.' 组成，且必须以字母数字字符开头和结尾
	// （例如 'MyName'、'my.name' 或 '123-abc'；校验所用的正则为
	// '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]'），并可带有可选的 DNS 子域前缀和 '/'
	// （例如 'example.com/MyName'）。
	//
	// 必填。
	Name string

	// Expression 表示将由 CEL 求值的表达式，其结果必须为 bool。
	// CEL 表达式可以访问 AdmissionRequest 和 Authorizer 的内容，这些内容被组织为 CEL 变量：
	//
	// 'object' - 来自传入请求的对象。对于 DELETE 请求，其值为 null。
	// 'oldObject' - 已有的对象。对于 CREATE 请求，其值为 null。
	// 'request' - 准入请求的属性（/pkg/apis/admission/types.go#AdmissionRequest）。
	// 'authorizer' - 一个 CEL Authorizer，可用于对请求主体（用户或服务账号）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置而成的 CEL ResourceCheck。
	// 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 必填。
	Expression string
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicy 描述一种可能修改对象的准入策略。
type MutatingAdmissionPolicy struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// MutatingAdmissionPolicy 期望行为的规格说明。
	Spec MutatingAdmissionPolicySpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyList 是 MutatingAdmissionPolicy 的列表。
type MutatingAdmissionPolicyList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// ValidatingAdmissionPolicy 列表。
	Items []MutatingAdmissionPolicy
}

// MutatingAdmissionPolicySpec 是该准入策略期望行为的规格说明。
type MutatingAdmissionPolicySpec struct {
	// paramKind 指定用于为该策略提供参数（参数化）的资源类型。
	// 若未指定，则该策略没有参数，也不会向校验表达式提供 param CEL 变量。
	// 如果 paramKind 指向一个不存在的 Kind，则该策略定义为配置错误，会应用 FailurePolicy。
	// 如果指定了 paramKind 但 MutatingAdmissionPolicyBinding 中未设置 paramRef，则 params 变量为 null。
	// +optional
	ParamKind *ParamKind

	// matchConstraints 指定该策略旨在校验哪些资源。
	// 当请求匹配_所有_约束（Constraint）时，该 AdmissionPolicy 才会关注该请求。
	// 但是，为防止集群陷入无法通过 API 恢复的不稳定状态，
	// MutatingAdmissionPolicy 不能匹配 MutatingAdmissionPolicy 和 MutatingAdmissionPolicyBinding。
	// 仅允许 CREATE、UPDATE 和 CONNECT 操作。
	// '*' 仅匹配 CREATE、UPDATE 和 CONNECT。
	// 必填。
	MatchConstraints *MatchResources

	// variables 包含可在其他表达式组合中使用的变量定义。
	// 每个变量都定义为一个具名的 CEL 表达式。
	// 此处定义的变量在策略的其他表达式中可通过 `variables` 访问，但 matchConditions 除外，
	// 因为 matchConditions 会在策略其余部分之前求值。
	//
	// 变量的表达式可以引用列表中更早定义的其他变量，但不能引用其后的变量。
	// 因此，variables 必须按首次出现的顺序排序且无环。
	// +listType=atomic
	// +optional
	Variables []Variable

	// mutations 包含要对匹配对象执行的操作。
	// mutations 不能为空，至少需要一项变更。
	// mutations 按顺序求值，并根据 reinvocationPolicy 被重新调用。
	// 策略的变更会针对该策略的每个绑定各调用一次，且变更的重新调用按绑定逐一进行。
	//
	// +listType=atomic
	// +optional
	Mutations []Mutation

	// failurePolicy 定义如何处理该准入策略的失败。失败可能由以下原因引起：CEL 表达式解析错误、
	// 类型检查错误、运行时错误，以及无效或配置错误的策略定义与绑定。
	//
	// 如果 paramKind 指向不存在的 Kind，则策略无效。
	// 如果 paramRef.name 指向不存在的资源，则绑定无效。
	//
	// failurePolicy 不定义如何处置计算结果为 false 的校验。
	//
	// 允许的值为 Ignore 或 Fail。默认为 Fail。
	// +optional
	FailurePolicy *FailurePolicyType

	// matchConditions 是请求被校验前必须满足的条件列表。
	// 匹配条件用于过滤已被 matchConstraints 匹配过的请求。
	// 空的 matchConditions 列表匹配所有请求。
	// 最多允许 64 个匹配条件。
	//
	// 如果提供了参数对象，则可以通过 `params` 句柄访问它，方式与校验表达式相同。
	//
	// 精确的匹配逻辑如下（按顺序）：
	//   1. 如果_任意_ matchCondition 计算结果为 FALSE，则跳过该策略。
	//   2. 如果_所有_ matchCondition 计算结果均为 TRUE，则对该策略进行求值。
	//   3. 如果任一 matchCondition 计算出错（但没有为 FALSE 的）：
	//      - 若 failurePolicy=Fail，则拒绝该请求
	//      - 若 failurePolicy=Ignore，则跳过该策略
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition

	// reinvocationPolicy 指示在单次准入评估中，是否允许每个 MutatingAdmissionPolicyBinding
	// 多次调用这些变更操作。
	// 允许值为 "Never" 和 "IfNeeded"。
	//
	// Never：在单次准入评估中，每个绑定对这些变更的调用不会超过一次。
	//
	// IfNeeded：对于单次准入请求，这些变更每个绑定可能被调用多次，且不保证相对于其他准入插件、
	// 准入 webhook、该策略的其他绑定以及其他准入策略的调用顺序。
	// 只有在这些变更被调用后又修改了对象时，才会重新调用变更。
	// 必填。
	ReinvocationPolicy ReinvocationPolicyType
}

// Mutation 指定执行变更（Mutation）的操作。
type Mutation struct {
	// patchType 指示所使用的补丁策略。
	// 允许值为 "ApplyConfiguration" 和 "JSONPatch"。
	// 必填。
	//
	// +unionDiscriminator
	PatchType PatchType

	// applyConfiguration 定义对象期望的配置值。
	// 该配置通过 [structured merge diff](https://github.com/kubernetes-sigs/structured-merge-diff)
	// 应用到准入对象上。
	// 使用一个 CEL 表达式来创建 apply configuration。
	ApplyConfiguration *ApplyConfiguration

	// jsonPatch 定义一个 [JSON patch](https://jsonpatch.com/)，用于对对象执行变更。
	// 使用一个 CEL 表达式来创建该 JSON 补丁。
	JSONPatch *JSONPatch
}

// PatchType 指定变更所使用的补丁操作类型。
// +enum
type PatchType string

// 变更补丁类型的合法取值。
const (
	// PatchTypeApplyConfiguration 表示该变更使用 apply configuration 来修改对象。
	PatchTypeApplyConfiguration PatchType = "ApplyConfiguration"
	// PatchTypeJSONPatch 表示通过 JSON Patch 来修改对象。
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// ApplyConfiguration 定义对象期望的配置值。
type ApplyConfiguration struct {
	// expression 将由 CEL 求值以创建 apply configuration。
	// 参考：https://github.com/google/cel-spec
	//
	// Apply configuration 在 CEL 中通过对象初始化来声明。例如，以下 CEL 表达式
	// 返回一个只设置单个字段的 apply configuration：
	//
	//	Object{
	//	  spec: Object.spec{
	//	    serviceAccountName: "example"
	//	  }
	//	}
	//
	// Apply configuration 不得修改原子结构体、map 或数组，否则可能意外删除
	// apply configuration 中未包含的值。
	//
	// CEL 表达式可以访问创建 apply configuration 所需的对象类型：
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）。
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）。
	//
	// CEL 表达式可以访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用的变量：
	//
	// - 'object' - 来自传入请求的对象。对于 DELETE 请求，其值为 null。
	// - 'oldObject' - 已有的对象。对于 CREATE 请求，其值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的策略绑定所引用的参数资源。仅当策略具有 ParamKind 时才会填充。
	// - 'namespaceObject' - 传入对象所属的命名空间对象。对于集群级资源，其值为 null。
	// - 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - 一个 CEL Authorizer，可用于对请求主体（用户或服务账号）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置而成的 CEL ResourceCheck。
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从对象根部访问。
	// 其他元数据属性均不可访问。
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名才可访问。
	// 必填。
	Expression string
}

// JSONPatch 定义一个 JSON Patch。
type JSONPatch struct {
	// expression 将由 CEL 求值以创建 [JSON patch](https://jsonpatch.com/)。
	// 参考：https://github.com/google/cel-spec
	//
	// expression 必须返回一个 JSONPatch 值数组。
	//
	// 例如，以下 CEL 表达式返回一个有条件修改值的 JSON 补丁：
	//
	//	  [
	//	    JSONPatch{op: "test", path: "/spec/example", value: "Red"},
	//	    JSONPatch{op: "replace", path: "/spec/example", value: "Green"}
	//	  ]
	//
	// 要为补丁值定义对象，请使用 Object 类型。例如：
	//
	//	  [
	//	    JSONPatch{
	//	      op: "add",
	//	      path: "/spec/selector",
	//	      value: Object.spec.selector{matchLabels: {"environment": "test"}}
	//	    }
	//	  ]
	//
	// 要使用包含 '/' 和 '~' 的字符串作为 JSONPatch 路径键，请使用 "jsonpatch.escapeKey"。例如：
	//
	//	  [
	//	    JSONPatch{
	//	      op: "add",
	//	      path: "/metadata/labels/" + jsonpatch.escapeKey("example.com/environment"),
	//	      value: "test"
	//	    },
	//	  ]
	//
	// CEL 表达式可以访问创建 JSON 补丁和对象所需的类型：
	//
	// - 'JSONPatch' - JSON Patch 操作的 CEL 类型。JSONPatch 包含 'op'、'from'、'path' 和 'value' 字段。
	//   更多细节参见 [JSON patch](https://jsonpatch.com/)。'value' 字段可设为字符串、
	//   整数、数组、map 或对象。如果设置了 'path' 和 'from' 字段，它们必须是
	//   [JSON pointer](https://datatracker.ietf.org/doc/html/rfc6901/) 字符串，
	//   其中可以使用 'jsonpatch.escapeKey()' CEL 函数来转义包含 '/' 和 '~' 的路径键。
	// - 'Object' - 资源对象的 CEL 类型。
	// - 'Object.<fieldName>' - 对象字段的 CEL 类型（如 'Object.spec'）。
	// - 'Object.<fieldName1>.<fieldName2>...<fieldNameN>` - 嵌套字段的 CEL 类型（如 'Object.spec.containers'）。
	//
	// CEL 表达式可以访问 API 请求的内容，这些内容被组织为 CEL 变量以及一些其他有用的变量：
	//
	// - 'object' - 来自传入请求的对象。对于 DELETE 请求，其值为 null。
	// - 'oldObject' - 已有的对象。对于 CREATE 请求，其值为 null。
	// - 'request' - API 请求的属性（[ref](/pkg/apis/admission/types.go#AdmissionRequest)）。
	// - 'params' - 被评估的策略绑定所引用的参数资源。仅当策略具有 ParamKind 时才会填充。
	// - 'namespaceObject' - 传入对象所属的命名空间对象。对于集群级资源，其值为 null。
	// - 'variables' - 组合变量的映射，从名称到其惰性求值的值。
	//   例如，名为 'foo' 的变量可通过 'variables.foo' 访问。
	// - 'authorizer' - 一个 CEL Authorizer，可用于对请求主体（用户或服务账号）执行授权检查。
	//   参见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz
	// - 'authorizer.requestResource' - 由 'authorizer' 构造并使用请求资源配置而成的 CEL ResourceCheck。
	//
	// CEL 表达式可以访问 [Kubernetes CEL 函数库](https://kubernetes.io/docs/reference/using-api/cel/#cel-options-language-features-and-libraries)，
	// 以及：
	//
	// - 'jsonpatch.escapeKey' - 执行 JSONPatch 键转义，'~' 和 '/' 分别被转义为 '~0' 和 '~1'。
	//
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名才可访问。
	// 必填。
	Expression string
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyBinding 将 MutatingAdmissionPolicy 与参数化的资源绑定在一起。
// MutatingAdmissionPolicyBinding 与可选的参数资源共同定义了集群管理员如何为集群配置策略。
//
// 对于给定的准入请求，每个绑定都会使其策略被求值 N 次：对于不使用参数的策略/绑定，N 为 1；
// 否则 N 为该绑定所选参数的数量。
// 每次求值都受 [运行时开销预算](https://kubernetes.io/docs/reference/using-api/cel/#runtime-cost-budget) 的限制。
//
// 增加/删除策略、绑定或参数不会影响某个给定的（策略、绑定、参数）组合是否在其自身的 CEL 预算内。
type MutatingAdmissionPolicyBinding struct {
	metav1.TypeMeta
	// 标准对象元数据；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta
	// MutatingAdmissionPolicyBinding 期望行为的规格说明。
	Spec MutatingAdmissionPolicyBindingSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// MutatingAdmissionPolicyBindingList 是 MutatingAdmissionPolicyBinding 的列表。
type MutatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta
	// PolicyBinding 列表。
	Items []MutatingAdmissionPolicyBinding
}

// MutatingAdmissionPolicyBindingSpec 是 MutatingAdmissionPolicyBinding 的规格说明。
type MutatingAdmissionPolicyBindingSpec struct {
	// policyName 引用该 MutatingAdmissionPolicyBinding 所绑定的 MutatingAdmissionPolicy 名称。
	// 如果引用的资源不存在，则该绑定被视为无效并被忽略。
	// 必填。
	PolicyName string
	// paramRef 指定用于配置准入控制策略的参数资源。
	// 它应指向所绑定 MutatingAdmissionPolicy 的 spec.ParamKind 中指定类型的资源。
	// 如果策略指定了 ParamKind，但 ParamRef 引用的资源不存在，则该绑定被视为配置错误，
	// 并会应用 MutatingAdmissionPolicy 的 FailurePolicy。
	// 如果策略未指定 ParamKind，则忽略该字段，并在不带参数的情况下求值规则。
	// +optional
	ParamRef *ParamRef

	// matchResources 限定哪些资源匹配此绑定，从而可能被该绑定变更。
	// 请注意，即使 matchResources 匹配了某资源，该资源还必须匹配策略的 matchConstraints
	// 和 matchConditions，才可以被变更。
	// 当 matchResources 未设置时，它不限制资源匹配；资源只需匹配策略的 matchConstraints
	// 和 matchConditions 即可被变更。
	// 此外，matchResources.resourceRules 是可选的，未设置时不限制匹配。
	// 请注意，这与 MutatingAdmissionPolicy 的 matchConstraints 不同，后者要求必须提供 resourceRules。
	// 允许 CREATE、UPDATE 和 CONNECT 操作，不允许匹配 DELETE 操作。
	// '*' 匹配 CREATE、UPDATE 和 CONNECT。
	// +optional
	MatchResources *MatchResources
}
