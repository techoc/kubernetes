/*
Copyright 2019 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Rule 是 APIGroups、APIVersion 与 Resources 的三元组。
// 建议确保元组展开后的每一项都是合法值。
type Rule struct {
	// 资源所属的 API 组，'*' 表示所有组。
	// 出现 '*' 时切片长度必须为 1。必填。
	// +listType=atomic
	APIGroups []string `json:"apiGroups,omitempty" protobuf:"bytes,1,rep,name=apiGroups"`

	// 资源所属的 API 版本，'*' 表示所有版本。
	// 出现 '*' 时切片长度必须为 1。必填。
	// +listType=atomic
	APIVersions []string `json:"apiVersions,omitempty" protobuf:"bytes,2,rep,name=apiVersions"`

	// 本规则适用的资源列表。例如：
	// 'pods' 表示 pods；'pods/log' 表示 pods 的 log 子资源；
	// '*' 表示所有资源（不含子资源）；'pods/*' 表示 pods 的所有子资源；
	// '*/scale' 表示所有 scale 子资源；'*/*' 表示所有资源及其子资源。
	//
	// 若使用了通配符，校验规则会确保资源之间不重叠；
	// 根据外层对象的不同，子资源可能不被允许。必填。
	// +listType=atomic
	Resources []string `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`

	// 本规则的作用域，合法值为 "Cluster"、"Namespaced" 和 "*"。
	// "Cluster" 仅匹配集群级资源（Namespace 对象属于集群级）；
	// "Namespaced" 仅匹配命名空间级资源；"*" 不限制作用域。
	// 子资源沿用其父资源的作用域。默认 "*"。
	//
	// +optional
	Scope *ScopeType `json:"scope,omitempty" protobuf:"bytes,4,rep,name=scope"`
}

// ScopeType 指定 Rule 的作用域。
// +enum
type ScopeType string

const (
	// 仅匹配集群级对象。Namespace 对象属于集群级。
	ClusterScope ScopeType = "Cluster"
	// 仅匹配命名空间级对象。
	NamespacedScope ScopeType = "Namespaced"
	// 包含所有作用域。
	AllScopes ScopeType = "*"
)

// FailurePolicyType 定义无法识别的 admission endpoint 错误如何处理。
// +enum
type FailurePolicyType string

const (
	// 忽略调用 webhook 时发生的错误。
	Ignore FailurePolicyType = "Ignore"
	// 调用 webhook 出错时准入失败。
	Fail FailurePolicyType = "Fail"
)

// ParameterNotFoundActionType 定义 binding 引用的参数对象不存在时
// 如何判定该 binding。
type ParameterNotFoundActionType string

const (
	// 找不到参数资源时允许所有请求。
	AllowAction ParameterNotFoundActionType = "Allow"
	// 找不到参数资源时拒绝所有请求。
	DenyAction ParameterNotFoundActionType = "Deny"
)

// MatchPolicyType 指定匹配策略类型。
// +enum
type MatchPolicyType string

const (
	// 仅当请求与规则完全匹配时才发送给 webhook。
	Exact MatchPolicyType = "Exact"
	// 只要请求修改的是规则所列资源，即使通过其他 API 组或版本访问，也发送给 webhook。
	Equivalent MatchPolicyType = "Equivalent"
)

// SideEffectClass 表示 webhook 可能产生的副作用类型。
// +enum
type SideEffectClass string

const (
	// 不清楚调用 webhook 会产生什么副作用。
	// dry-run 请求若会触发该 webhook，则直接失败。
	SideEffectClassUnknown SideEffectClass = "Unknown"
	// 调用 webhook 没有任何副作用。
	SideEffectClassNone SideEffectClass = "None"
	// 调用 webhook 可能产生副作用。
	// dry-run 请求若会触发该 webhook，则直接失败。
	SideEffectClassSome SideEffectClass = "Some"
	// 调用 webhook 可能有副作用，但被审查的请求带 dry-run 属性时副作用会被抑制。
	SideEffectClassNoneOnDryRun SideEffectClass = "NoneOnDryRun"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// ValidatingAdmissionPolicy 定义准入校验策略：接受或拒绝对象，但不修改对象。
// +k8s:supportsSubresource="/status"
type ValidatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 的期望行为。
	Spec ValidatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// 当前状态，含用于判断策略是否按预期工作的告警信息。
	// 由系统写入，只读。
	// +optional
	Status ValidatingAdmissionPolicyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ValidatingAdmissionPolicyStatus 表示准入校验策略的状态。
type ValidatingAdmissionPolicyStatus struct {
	// 控制器观测到的 generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`
	// 每个表达式的类型检查结果；该字段出现即表示类型检查已完成。
	// +optional
	TypeChecking *TypeChecking `json:"typeChecking,omitempty" protobuf:"bytes,2,opt,name=typeChecking"`
	// 策略当前状态的最新观测结果。
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" protobuf:"bytes,3,rep,name=conditions"`
}

// ValidatingAdmissionPolicyConditionType 是准入校验策略的条件类型。
type ValidatingAdmissionPolicyConditionType string

// TypeChecking 包含 ValidatingAdmissionPolicy 中表达式的类型检查结果。
type TypeChecking struct {
	// 每个表达式的类型检查告警。
	// +optional
	// +listType=atomic
	ExpressionWarnings []ExpressionWarning `json:"expressionWarnings,omitempty" protobuf:"bytes,1,rep,name=expressionWarnings"`
}

// ExpressionWarning 是针对某个表达式的告警信息。
type ExpressionWarning struct {
	// 指向该表达式的字段路径，例如 validations 第一项的表达式为
	// "spec.validations[0].expression"。
	FieldRef string `json:"fieldRef" protobuf:"bytes,2,opt,name=fieldRef"`
	// 可读的类型检查信息；每行是表达式的检查类型及其对应的编译器报错。
	Warning string `json:"warning" protobuf:"bytes,3,opt,name=warning"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// ValidatingAdmissionPolicyList 是 ValidatingAdmissionPolicy 的列表。
type ValidatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 列表。
	Items []ValidatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicySpec 描述准入策略的期望行为。
type ValidatingAdmissionPolicySpec struct {
	// 用于为本策略提供参数的资源 kind。缺省时策略无参数，校验表达式中不会提供 params 变量。
	// 若指向不存在的 kind，则视为配置错误并应用 FailurePolicy。
	// 若指定了 paramKind 但 binding 未设置 paramRef，params 变量为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// 本策略要校验哪些资源。请求需匹配所有约束才会被处理。
	// 为防止集群进入无法通过 API 恢复的不稳定状态，本策略不能匹配
	// ValidatingAdmissionPolicy 和 ValidatingAdmissionPolicyBinding。必填。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// 执行校验的 CEL 表达式。validations 与 auditAnnotations 不能同时为空，至少其一。
	// +listType=atomic
	// +optional
	Validations []Validation `json:"validations,omitempty" protobuf:"bytes,3,rep,name=validations"`

	// 策略执行失败时的处理方式。失败原因包括 CEL 表达式解析错误、类型检查错误、
	// 运行时错误，以及无效或配置错误的策略定义/binding。
	//
	// paramKind 指向不存在的 Kind 时策略无效；paramRef.name 指向不存在的资源时 binding 无效。
	// 校验结果为 false 不属于此处定义的失败。
	// failurePolicy 为 Fail 时，具体执行方式由 binding 的 validationActions 决定。
	// 合法值为 Ignore 或 Fail，默认 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// 为 API 请求审计事件生成审计注解的 CEL 表达式。
	// 与 validations 至少有一个非空。
	// +listType=atomic
	// +optional
	AuditAnnotations []AuditAnnotation `json:"auditAnnotations,omitempty" protobuf:"bytes,5,rep,name=auditAnnotations"`

	// 请求被校验前必须满足的条件列表。它在 rules、namespaceSelector、
	// objectSelector 之后进一步过滤；空列表匹配所有请求，最多允许 64 个。
	// 若提供了参数对象，可像校验表达式一样通过 `params` 访问。
	//
	// 匹配逻辑依次为：
	//   1. 任一 matchCondition 为 FALSE，跳过该策略；
	//   2. 全部为 TRUE，执行该策略；
	//   3. 有条件出错（但无 FALSE）时：
	//      - failurePolicy=Fail：拒绝请求；
	//      - failurePolicy=Ignore：跳过该策略。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// 可复用的变量定义，每个变量是一个具名 CEL 表达式。
	// 变量可在策略其他表达式中通过 `variables` 访问，但 MatchConditions 除外：
	// 它先于策略其余部分执行。
	// 变量表达式只能引用列表中更早定义的变量，因此必须按首次出现顺序排列且无环。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	Variables []Variable `json:"variables,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=variables"`
}

// ParamKind 是 Group、Kind 和 Version 组成的三元组。
// +structType=atomic
type ParamKind struct {
	// 资源所属的 API 组版本，形如 "group/version"。必填。
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,1,rep,name=apiVersion"`

	// 资源所属的 API kind。必填。
	Kind string `json:"kind,omitempty" protobuf:"bytes,2,rep,name=kind"`
}

// Validation 指定执行校验所用的 CEL 表达式。
type Validation struct {
	// 由 CEL 求值的表达式，语法见 https://github.com/google/cel-spec。
	// 表达式可访问 API 请求/响应内容，组织成以下 CEL 变量：
	//
	// - 'object'：传入请求中的对象；DELETE 请求为 null。
	// - 'oldObject'：现存对象；CREATE 请求为 null。
	// - 'request'：API 请求的属性（/pkg/apis/admission/types.go#AdmissionRequest）。
	// - 'params'：binding 引用的参数资源；仅当策略定义了 ParamKind 时才有值。
	// - 'namespaceObject'：对象所属的命名空间对象；集群级资源为 null。
	// - 'variables'：组合变量到其惰性求值的映射，如 'variables.foo'。
	// - 'authorizer'：CEL 授权器，可对请求主体（用户或服务账号）做权限检查，
	//   见 https://pkg.go.dev/k8s.io/apiserver/pkg/cel/library#Authz。
	// - 'authorizer.requestResource'：由 authorizer 构造并绑定请求资源的 CEL ResourceCheck。
	//
	// 根对象始终可访问 apiVersion、kind、metadata.name 和 metadata.generateName，
	// 其他元属性不可访问。只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问，
	// 在表达式中按如下规则转义：
	// - '__' 转义为 '__underscores__'；'.' 为 '__dot__'；
	// - '-' 为 '__dash__'；'/' 为 '__slash__'；
	// - 与 CEL 保留字完全相同的属性名转义为 '__{keyword}__'，保留字包括：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return"。
	// 例如访问 "namespace" 属性写作 object.__namespace__，访问 "x-prop" 写作 object.x__dash__prop。
	//
	// list 类型为 'set' 或 'map' 的数组比较相等时忽略元素顺序，即 [1, 2] == [2, 1]。
	// 数组拼接遵循 x-kubernetes-list-type 的语义：
	//   - 'set'：`X + Y` 取并集，保留 X 中元素的位置，Y 中不重合的元素按原相对顺序追加；
	//   - 'map'：`X + Y` 按键合并，X 中键的位置保留，键重合处用 Y 的值覆盖，
	//     Y 中不重合的键按原相对顺序追加。必填。
	Expression string `json:"expression" protobuf:"bytes,1,opt,name=Expression"`
	// 校验失败时显示的消息。表达式含换行时必须提供，且消息本身不能含换行。
	// 未设置时使用 "failed Expression: {Expression}"。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// 机器可读的校验失败原因。若这是列表中首个失败的校验，该原因与对应 HTTP
	// 状态码会用于返回给客户端。当前支持 "Unauthorized"、"Forbidden"、
	// "Invalid"、"RequestEntityTooLarge"；未设置时默认 StatusReasonInvalid。
	// +optional
	Reason *metav1.StatusReason `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// 求值为校验失败消息的 CEL 表达式，结果必须是字符串。
	// 与 message 同时存在时优先使用本字段。
	// 若表达式运行时出错，或结果为空串、全空格、含换行，则按未设置处理并记录日志。
	// 可访问的变量与 expression 相同，但不含 authorizer 和 authorizer.requestResource。
	// 示例："object.x must be less than max ("+string(params.max)+")"。
	// +optional
	MessageExpression string `json:"messageExpression,omitempty" protobuf:"bytes,4,opt,name=messageExpression"`
}

// Variable 定义一个可复用的变量，即一个具名表达式。
// +structType=atomic
type Variable struct {
	// 变量名，必须是合法 CEL 标识符且在变量间唯一。
	// 可通过 `variables` 在其他表达式中访问，如 "foo" 对应 `variables.foo`。
	Name string `json:"name" protobuf:"bytes,1,opt,name=Name"`

	// 求值为变量值的表达式，可访问的标识符与 Validation 中的表达式相同。
	Expression string `json:"expression" protobuf:"bytes,2,opt,name=Expression"`
}

// AuditAnnotation 描述如何为 API 请求生成审计注解。
type AuditAnnotation struct {
	// 审计注解的 key，在同一策略内必须唯一，须为不超过 63 字节的合法限定名。
	// 最终注解 key 为 "{策略名}/{key}"。
	// 若其他 admission webhook 使用同名策略和相同 key，注解 key 会冲突：
	// 仅第一条写入的注解会保留在审计事件中，后续同名注解被丢弃。必填。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`

	// 由 CEL 求值产生注解值的表达式，结果必须是字符串或 null。
	// 字符串会写入注解；null 或空串则省略该注解。
	// 表达式长度不超过 5kb，结果超过 10kb 会被截断。
	// 多个 binding 匹配同一请求时，各 binding 的值会去重后以逗号连接。必填。
	ValueExpression string `json:"valueExpression" protobuf:"bytes,2,opt,name=valueExpression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// ValidatingAdmissionPolicyBinding 将策略与参数化资源绑定。
// 它与参数 CRD 共同定义集群管理员如何配置策略。
//
// 对一次准入请求，每个 binding 会让其策略执行 N 次：不使用参数时 N=1，
// 否则 N 为该 binding 选中的参数数量。
//
// 策略中 CEL 表达式的计算开销必须低于 CEL 预算上限，每次求值独立计算预算；
// 增删策略、binding 或参数不应影响某个（策略、binding、参数）组合是否在预算内。
type ValidatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// binding 的期望行为。
	// +required
	Spec ValidatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// ValidatingAdmissionPolicyBindingList 是绑定对象的列表。
type ValidatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 列表。
	Items []ValidatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingAdmissionPolicyBindingSpec 描述 binding 的规格。
type ValidatingAdmissionPolicyBindingSpec struct {
	// 本 binding 所绑定的策略名。引用的资源不存在时 binding 无效并被忽略。必填。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// 用于配置策略的参数资源，类型应与所绑定策略的 paramKind 一致。
	// 策略定义了 paramKind 但参数资源不存在时，binding 视为配置错误并应用策略的 FailurePolicy。
	// 策略未定义 paramKind 时忽略该字段，规则在无参数情况下求值。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// 声明本 binding 匹配并要校验的资源。它与策略的 matchConstraints 取交集，
	// 因此只有被策略匹配到的请求才会被选中。未设置时校验策略匹配的所有资源。
	// resourceRules 未设置时不限制资源匹配，其他字段匹配即校验；
	// 这与策略的 matchConstraints 不同，后者要求 resourceRules 必填。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`

	// 声明所绑定策略的校验结果如何执行。校验结果为 false 时始终按这些动作处理。
	// 策略 FailurePolicy 定义的失败仅在取值为 Fail 时按这些动作执行，否则忽略，
	// 包括编译错误、运行时错误和策略配置错误。
	//
	// 这是动作值的集合，顺序无关，不允许重复。支持的动作：
	// - "Deny"：校验失败导致请求被拒绝；
	// - "Warn"：通过 HTTP Warning 头（code 299）向客户端报告，允许和拒绝的响应都可带告警；
	// - "Audit"：将失败写入审计事件的
	//   `validation.policy.admission.k8s.io/validation_failure` 注解，
	//   值为 JSON 对象列表，字段包括 message、policy、binding、expressionIndex、
	//   validationActions。
	//
	// 客户端应忽略无法识别的新动作。"Deny" 与 "Warn" 不能同时使用，
	// 否则会在响应体和告警头中重复报告同一失败。必填。
	// +listType=set
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	ValidationActions []ValidationAction `json:"validationActions,omitempty" protobuf:"bytes,4,rep,name=validationActions"`
}

// ParamRef 描述如何定位 binding 中规则表达式所需的参数对象。
// +structType=atomic
type ParamRef struct {
	// 被引用资源的名字。name 与 selector 必须二选一且互斥。
	// 若要用同一个参数处理所有请求，设置 name、留空 selector，
	// 并在 paramKind 为命名空间级时指定 namespace。
	//
	Name string `json:"name,omitempty" protobuf:"bytes,1,rep,name=name"`

	// 被引用资源的命名空间，将参数搜索限制在特定命名空间，对 name 和 selector 都生效。
	// 在策略中指定命名空间级 paramKind 并留空本字段，即可使用按命名空间区分的参数。
	// paramKind 为集群级时本字段必须留空，否则配置错误；
	// paramKind 为命名空间级时留空将使用被准入对象的命名空间，
	// 此时 binding 不得匹配任何集群级资源，否则会报错。
	//
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,rep,name=namespace"`

	// 按标签匹配多个参数对象；selector: {} 匹配该 ParamKind 的所有资源。
	// 找到多个参数时各自求值，结果按逻辑与合并。
	// name 与 selector 必须二选一且互斥。
	//
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,3,rep,name=selector"`

	// 资源存在、name/selector 合法但 binding 未匹配到任何参数时的行为。
	// `Allow` 将无参数视为校验通过；`Deny` 则按策略的 failurePolicy 处理。必填，
	// 合法值为 `Allow` 或 `Deny`。
	ParameterNotFoundAction *ParameterNotFoundActionType `json:"parameterNotFoundAction,omitempty" protobuf:"bytes,4,rep,name=parameterNotFoundAction"`
}

// MatchResources 根据对象是否满足匹配条件决定是否对其执行准入策略。
// 排除规则优先于包含规则：同时匹配时以排除为准。
// +structType=atomic
type MatchResources struct {
	// 根据对象所属命名空间是否匹配选择器决定是否执行策略。
	// 对象本身就是命名空间时，匹配其 metadata.labels；其他集群级资源从不跳过策略。
	// 例如要排除 runlevel 为 "0" 或 "1" 的命名空间：
	// "namespaceSelector": {"matchExpressions": [{"key": "runlevel", "operator": "NotIn", "values": ["0", "1"]}]}
	// 只看 "environment" 为 "prod" 或 "staging" 的命名空间则用 In 操作符。
	// 更多示例见 https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,1,opt,name=namespaceSelector"`
	// 根据对象标签决定是否执行校验。oldObject 和 newObject 分别匹配，任一命中即可。
	// null 对象（create 时的 oldObject、delete 时的 newObject）及无标签能力的对象
	// （如 DeploymentRollback、PodProxyOptions）视为不匹配。
	// 仅当 webhook 是可选启用时才用对象选择器，因为终端用户可自行设置标签绕过。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,2,opt,name=objectSelector"`
	// 策略匹配哪些资源/子资源上的哪些操作，命中任一 Rule 即处理。
	// +listType=atomic
	// +optional
	ResourceRules []NamedRuleWithOperations `json:"resourceRules,omitempty" protobuf:"bytes,3,rep,name=resourceRules"`
	// 策略不关心的资源/子资源操作。排除规则优先于包含规则。
	// +listType=atomic
	// +optional
	ExcludeResourceRules []NamedRuleWithOperations `json:"excludeResourceRules,omitempty" protobuf:"bytes,4,rep,name=excludeResourceRules"`
	// "MatchResources" 列表如何匹配传入请求，合法值为 "Exact" 或 "Equivalent"。
	// Exact：仅完全匹配规则时才命中，例如规则只写了 apps/v1 的 deployments，
	// 则 apps/v1beta1、extensions/v1beta1 的请求不会发送。
	// Equivalent：通过其他 API 组或版本修改同一资源也算命中，上述请求会先转换为
	// apps/v1 再发送。默认 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,7,opt,name=matchPolicy,casttype=MatchPolicyType"`
}

// ValidationAction 指定策略的执行动作。
// +enum
type ValidationAction string

const (
	// 校验失败导致请求被拒绝。
	Deny ValidationAction = "Deny"
	// 通过 HTTP Warning 头（code 299）报告失败，允许和拒绝的响应都可带告警。
	Warn ValidationAction = "Warn"
	// 将失败写入审计事件的 validation.policy.admission.k8s.io/validation_failure 注解。
	Audit ValidationAction = "Audit"
)

// NamedRuleWithOperations 是 Operations、Resources 与 ResourceNames 组成的元组。
// +structType=atomic
type NamedRuleWithOperations struct {
	// 规则适用的资源名白名单，空集表示不限制。
	// +listType=atomic
	// +optional
	ResourceNames []string `json:"resourceNames,omitempty" protobuf:"bytes,1,rep,name=resourceNames"`
	// RuleWithOperations 是 Operations 与 Resources 组成的元组。
	RuleWithOperations `json:"" protobuf:"bytes,2,opt,name=ruleWithOperations"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// ValidatingWebhookConfiguration 描述校验型准入 webhook 的配置：接受或拒绝对象，不修改对象。
type ValidatingWebhookConfiguration struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// webhook 列表及其影响的资源和操作。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Webhooks []ValidatingWebhook `json:"webhooks,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=Webhooks"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// ValidatingWebhookConfigurationList 是配置对象的列表。
type ValidatingWebhookConfigurationList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingWebhookConfiguration 列表。
	Items []ValidatingWebhookConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// MutatingWebhookConfiguration 描述变更型准入 webhook 的配置：接受或拒绝对象，并可修改对象。
type MutatingWebhookConfiguration struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// webhook 列表及其影响的资源和操作。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Webhooks []MutatingWebhook `json:"webhooks,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=Webhooks"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16

// MutatingWebhookConfigurationList 是配置对象的列表。
type MutatingWebhookConfigurationList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// MutatingWebhookConfiguration 列表。
	Items []MutatingWebhookConfiguration `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ValidatingWebhook 描述一个校验型 webhook 及其适用的资源和操作。
type ValidatingWebhook struct {
	// webhook 名称，建议为全限定名，如 imagepolicy.kubernetes.io：
	// "imagepolicy" 是 webhook 名，kubernetes.io 是组织名。必填。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// 与 webhook 通信的配置。必填。
	ClientConfig WebhookClientConfig `json:"clientConfig" protobuf:"bytes,2,opt,name=clientConfig"`

	// webhook 关心哪些资源/子资源上的哪些操作，命中任一 Rule 即处理。
	// 为防止 webhook 把集群带入必须完全禁用插件才能恢复的状态，
	// 对 webhook 配置对象本身的准入请求永远不会调用任何 webhook。
	// +listType=atomic
	Rules []RuleWithOperations `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`

	// 无法识别的 endpoint 错误如何处理，合法值为 Ignore 或 Fail，默认 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// "rules" 列表如何匹配请求，合法值为 "Exact" 或 "Equivalent"。
	// Exact 仅精确匹配；Equivalent 下通过其他组/版本修改同一资源也会先转换后发送。
	// 默认 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,9,opt,name=matchPolicy,casttype=MatchPolicyType"`

	// 按对象所属命名空间决定是否调用 webhook。对象本身是命名空间时匹配其
	// metadata.labels；其他集群级资源从不跳过 webhook。
	// 例如排除 runlevel 为 "0" 或 "1" 的命名空间：
	// "namespaceSelector": {"matchExpressions": [{"key": "runlevel", "operator": "NotIn", "values": ["0", "1"]}]}
	// 只处理 "environment" 为 "prod"/"staging" 则用 In 操作符。
	// 详见 https://kubernetes.io/docs/concepts/overview/working-with-objects/labels。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,5,opt,name=namespaceSelector"`

	// 按对象标签决定是否调用 webhook。oldObject 与 newObject 任一匹配即可；
	// null 对象及无标签能力的对象（如 DeploymentRollback、PodProxyOptions）不算匹配。
	// 仅对可选启用的 webhook 使用，因为用户可自行设置标签绕过。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,10,opt,name=objectSelector"`

	// 声明 webhook 是否有副作用。合法值为 None、NoneOnDryRun
	// （通过 v1beta1 创建的 webhook 还可为 Some 或 Unknown）。
	// 有副作用的 webhook 必须实现回滚机制，因为后续准入步骤仍可能拒绝请求。
	// dryRun 请求若命中 sideEffects 为 Unknown 或 Some 的 webhook 会被自动拒绝。
	SideEffects *SideEffectClass `json:"sideEffects" protobuf:"bytes,6,opt,name=sideEffects,casttype=SideEffectClass"`

	// webhook 调用超时时间。超时后按 failurePolicy 决定忽略调用还是让 API 请求失败。
	// 取值须在 1 到 30 秒之间，默认 10 秒。
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,7,opt,name=timeoutSeconds"`

	// webhook 期望的 AdmissionReview 版本，按优先级排序。
	// API server 使用列表中首个支持的版本；都不支持时该对象校验失败。
	// 已持久化的配置若不含任何 API server 已知版本，调用会失败并按 failurePolicy 处理。
	// +listType=atomic
	AdmissionReviewVersions []string `json:"admissionReviewVersions" protobuf:"bytes,8,rep,name=admissionReviewVersions"`

	// 请求发送给 webhook 前必须满足的条件，在 rules、namespaceSelector、
	// objectSelector 之后进一步过滤。空列表匹配所有请求，最多 64 个。
	//
	// 匹配逻辑依次为：
	//   1. 任一条件为 FALSE，跳过该 webhook；
	//   2. 全部为 TRUE，调用该 webhook；
	//   3. 有条件出错（但无 FALSE）时：failurePolicy=Fail 则拒绝请求；
	//      failurePolicy=Ignore 则忽略错误并跳过该 webhook。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,11,opt,name=matchConditions"`
}

// MutatingWebhook 描述一个变更型 webhook 及其适用的资源和操作。
type MutatingWebhook struct {
	// webhook 名称，建议为全限定名，如 imagepolicy.kubernetes.io：
	// "imagepolicy" 是 webhook 名，kubernetes.io 是组织名。必填。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// 与 webhook 通信的配置。必填。
	ClientConfig WebhookClientConfig `json:"clientConfig" protobuf:"bytes,2,opt,name=clientConfig"`

	// webhook 关心哪些资源/子资源上的哪些操作，命中任一 Rule 即处理。
	// 为防止 webhook 把集群带入必须完全禁用插件才能恢复的状态，
	// 对 webhook 配置对象本身的准入请求永远不会调用任何 webhook。
	// +listType=atomic
	Rules []RuleWithOperations `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`

	// 无法识别的 endpoint 错误如何处理，合法值为 Ignore 或 Fail，默认 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,4,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// "rules" 列表如何匹配请求，合法值为 "Exact" 或 "Equivalent"。
	// Exact 仅精确匹配；Equivalent 下通过其他组/版本修改同一资源也会先转换后发送。
	// 默认 "Equivalent"。
	// +optional
	MatchPolicy *MatchPolicyType `json:"matchPolicy,omitempty" protobuf:"bytes,9,opt,name=matchPolicy,casttype=MatchPolicyType"`

	// 按对象所属命名空间决定是否调用 webhook。对象本身是命名空间时匹配其
	// metadata.labels；其他集群级资源从不跳过 webhook。
	// 例如排除 runlevel 为 "0" 或 "1" 的命名空间：
	// "namespaceSelector": {"matchExpressions": [{"key": "runlevel", "operator": "NotIn", "values": ["0", "1"]}]}
	// 只处理 "environment" 为 "prod"/"staging" 则用 In 操作符。
	// 详见 https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,5,opt,name=namespaceSelector"`

	// 按对象标签决定是否调用 webhook。oldObject 与 newObject 任一匹配即可；
	// null 对象及无标签能力的对象（如 DeploymentRollback、PodProxyOptions）不算匹配。
	// 仅对可选启用的 webhook 使用，因为用户可自行设置标签绕过。
	// 默认空 LabelSelector，匹配所有对象。
	// +optional
	ObjectSelector *metav1.LabelSelector `json:"objectSelector,omitempty" protobuf:"bytes,11,opt,name=objectSelector"`

	// 声明 webhook 是否有副作用。合法值为 None、NoneOnDryRun
	// （通过 v1beta1 创建的 webhook 还可为 Some 或 Unknown）。
	// 有副作用的 webhook 必须实现回滚机制，因为后续准入步骤仍可能拒绝请求。
	// dryRun 请求若命中 sideEffects 为 Unknown 或 Some 的 webhook 会被自动拒绝。
	SideEffects *SideEffectClass `json:"sideEffects" protobuf:"bytes,6,opt,name=sideEffects,casttype=SideEffectClass"`

	// webhook 调用超时时间。超时后按 failurePolicy 决定忽略调用还是让 API 请求失败。
	// 取值须在 1 到 30 秒之间，默认 10 秒。
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,7,opt,name=timeoutSeconds"`

	// webhook 期望的 AdmissionReview 版本，按优先级排序。
	// API server 使用列表中首个支持的版本；都不支持时该对象校验失败。
	// 已持久化的配置若不含任何 API server 已知版本，调用会失败并按 failurePolicy 处理。
	// +listType=atomic
	AdmissionReviewVersions []string `json:"admissionReviewVersions" protobuf:"bytes,8,rep,name=admissionReviewVersions"`

	// 一次准入评估中是否可多次调用该 webhook，合法值为 "Never" 和 "IfNeeded"。
	// Never：最多调用一次。
	// IfNeeded：若首次调用后对象又被其他插件修改，则至少再调用一次。
	// 选择 IfNeeded 的 webhook 必须幂等，能处理自己此前准入过的对象。
	// 注意：额外调用次数不保证恰好一次；额外调用引发的新修改不保证再次触发调用；
	// 使用此选项的 webhook 可能被重排以减少额外调用。若要确保所有变更完成后再校验，
	// 应改用校验型 webhook。默认 "Never"。
	// +optional
	ReinvocationPolicy *ReinvocationPolicyType `json:"reinvocationPolicy,omitempty" protobuf:"bytes,10,opt,name=reinvocationPolicy,casttype=ReinvocationPolicyType"`

	// 请求发送给 webhook 前必须满足的条件，在 rules、namespaceSelector、
	// objectSelector 之后进一步过滤。空列表匹配所有请求，最多 64 个。
	//
	// 匹配逻辑依次为：
	//   1. 任一条件为 FALSE，跳过该 webhook；
	//   2. 全部为 TRUE，调用该 webhook；
	//   3. 有条件出错（但无 FALSE）时：failurePolicy=Fail 则拒绝请求；
	//      failurePolicy=Ignore 则忽略错误并跳过该 webhook。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,12,opt,name=matchConditions"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// MutatingAdmissionPolicy 定义准入变更策略，用于修改进入准入链的对象。
type MutatingAdmissionPolicy struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// 策略的期望行为。
	Spec MutatingAdmissionPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// MutatingAdmissionPolicyList 是策略对象的列表。
type MutatingAdmissionPolicyList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// ValidatingAdmissionPolicy 列表。
	Items []MutatingAdmissionPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicySpec 定义变更策略的期望行为。
type MutatingAdmissionPolicySpec struct {
	// 用于参数化本策略的资源 kind。缺省时无参数，表达式中不提供 params 变量。
	// 指向不存在的 kind 时视为配置错误并应用 FailurePolicy；
	// 指定 kind 但 binding 未设置 paramRef 时 params 为 null。
	// +optional
	ParamKind *ParamKind `json:"paramKind,omitempty" protobuf:"bytes,1,rep,name=paramKind"`

	// 本策略要处理哪些资源，请求需满足所有约束。
	// 为防止集群进入无法通过 API 恢复的状态，不能匹配本策略及其 binding 对象。
	// 仅允许 CREATE、UPDATE、CONNECT 操作，不能匹配 DELETE；'*' 表示这三者。必填。
	MatchConstraints *MatchResources `json:"matchConstraints,omitempty" protobuf:"bytes,2,rep,name=matchConstraints"`

	// 可复用的变量定义，每个变量是具名 CEL 表达式。
	// 可在策略其他表达式中通过 `variables` 访问，但 matchConditions 除外，
	// 因为它先于策略其余部分求值。
	// 变量只能引用列表中更早定义的变量，因此须按首次出现顺序排列且无环。
	// +listType=atomic
	// +optional
	Variables []Variable `json:"variables,omitempty" protobuf:"bytes,3,rep,name=variables"`

	// 对匹配对象执行的操作，至少一项。按顺序求值，并按 reinvocationPolicy 重新调用。
	// 每个 binding 各自调用，重调用也按 binding 粒度进行。
	//
	// +listType=atomic
	// +optional
	Mutations []Mutation `json:"mutations,omitempty" protobuf:"bytes,4,rep,name=mutations"`

	// 策略失败时的处理方式，失败包括 CEL 解析、类型检查、运行时错误以及无效的
	// 策略定义或 binding。paramKind 指向不存在的 Kind 时策略无效，
	// paramRef.name 指向不存在的资源时 binding 无效。
	// 本字段不涉及校验结果为 false 的情况。合法值 Ignore 或 Fail，默认 Fail。
	// +optional
	FailurePolicy *FailurePolicyType `json:"failurePolicy,omitempty" protobuf:"bytes,5,opt,name=failurePolicy,casttype=FailurePolicyType"`

	// 请求被处理前必须满足的条件，在 matchConstraints 之后过滤。
	// 空列表匹配所有请求，最多 64 个。
	//
	// 若提供了参数对象，可像校验表达式一样通过 `params` 访问。
	//
	// 匹配逻辑依次为：
	//   1. 任一条件为 FALSE，跳过该策略；
	//   2. 全部为 TRUE，执行该策略；
	//   3. 有条件出错（但无 FALSE）时：failurePolicy=Fail 则拒绝请求；
	//      failurePolicy=Ignore 则跳过该策略。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	MatchConditions []MatchCondition `json:"matchConditions,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=matchConditions"`

	// 一次准入评估中每个 binding 是否可多次调用变更操作，合法值 "Never"、"IfNeeded"。
	// Never：每个 binding 最多一次。
	// IfNeeded：每个 binding 可多次调用，且与其他插件、webhook、binding、策略的
	// 顺序不保证；只有在本操作之后对象又被变更时才会重新调用。必填。
	ReinvocationPolicy ReinvocationPolicyType `json:"reinvocationPolicy,omitempty" protobuf:"bytes,7,opt,name=reinvocationPolicy,casttype=ReinvocationPolicyType"`
}

// Mutation 指定执行变更所用的 CEL 表达式。
type Mutation struct {
	// 使用的补丁策略，合法值 "ApplyConfiguration" 和 "JSONPatch"。必填。
	//
	// +unionDiscriminator
	PatchType PatchType `json:"patchType" protobuf:"bytes,2,opt,name=patchType,casttype=PatchType"`

	// 对象期望的配置值，通过结构化合并差异应用到准入对象，
	// 配置本身由 CEL 表达式生成。
	ApplyConfiguration *ApplyConfiguration `json:"applyConfiguration,omitempty" protobuf:"bytes,3,opt,name=applyConfiguration"`

	// 对对象执行变更的 JSON Patch，由 CEL 表达式生成。
	JSONPatch *JSONPatch `json:"jsonPatch,omitempty" protobuf:"bytes,4,opt,name=jsonPatch"`
}

// PatchType 指定变更所使用的补丁类型。
// +enum
type PatchType string

const (
	// 使用 apply configuration 修改对象。
	PatchTypeApplyConfiguration PatchType = "ApplyConfiguration"
	// 通过 JSON Patch 修改对象。
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// ApplyConfiguration 定义对象期望的配置值。
type ApplyConfiguration struct {
	// 由 CEL 求值生成 apply configuration，语法见 https://github.com/google/cel-spec。
	// apply configuration 通过对象初始化声明，例如只设置一个字段：
	//	Object{spec: Object.spec{serviceAccountName: "example"}}
	// 不允许修改原子结构、map 或数组，以免误删未出现在配置中的值。
	//
	// 可用的类型：'Object'（资源对象）、'Object.<fieldName>'（字段）、
	// 'Object.<fieldName1>.<fieldName2>...<fieldNameN>'（嵌套字段）。
	// 可用的变量与 Validation 表达式相同：object、oldObject、request、params、
	// namespaceObject、variables、authorizer、authorizer.requestResource 等。
	//
	// 根对象始终可访问 apiVersion、kind、metadata.name 和 metadata.generateName，
	// 其他元属性不可访问；属性名须形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*`。必填。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// JSONPatch 定义一条 JSON Patch。
type JSONPatch struct {
	// 由 CEL 求值生成 JSON Patch（https://jsonpatch.com/），须返回 JSONPatch 值数组。
	// 例如先 test 再 replace 修改一个值：
	//	[JSONPatch{op: "test", path: "/spec/example", value: "Red"},
	//	 JSONPatch{op: "replace", path: "/spec/example", value: "Green"}]
	// patch 中的对象用 Object 类型构造；路径 key 含 '/' 或 '~' 时
	// 须用 jsonpatch.escapeKey 转义。
	//
	// 可用类型：'JSONPatch'（含 op、from、path、value；value 可为字符串、整数、
	// 数组、map 或对象；path/from 须为 JSON pointer 字符串）、'Object'（资源对象）、
	// 'Object.<fieldName>'（字段）及 'Object.<fieldName1>.<fieldName2>...<fieldNameN>'（嵌套字段）。
	//
	// 可访问 API 请求内容，组织为 CEL 变量：object（传入对象，DELETE 时为 null）、
	// oldObject（现存对象，CREATE 时为 null）、request（请求属性）、
	// params（binding 引用的参数，仅定义了 ParamKind 时有值）、
	// namespaceObject（所属命名空间，集群级资源为 null）、variables（组合变量映射）、
	// authorizer（CEL 授权器）及 authorizer.requestResource。
	//
	// 除 Kubernetes CEL 函数库外，还提供 'jsonpatch.escapeKey' 用于转义路径 key：
	// '~' 转义为 '~0'，'/' 转义为 '~1'。
	//
	//
	// 只有形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 必填。
	Expression string `json:"expression,omitempty" protobuf:"bytes,1,opt,name=expression"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// MutatingAdmissionPolicyBinding 将变更策略与参数化资源绑定。
// 它与可选的参数资源共同定义集群管理员如何配置策略。
//
// 对一次请求，每个 binding 让策略执行 N 次：无参数时 N=1，否则为参数数量。
// 每次求值受运行时开销预算约束；增删策略、binding 或参数不应影响某个
// （策略、binding、参数）组合是否在预算内。
type MutatingAdmissionPolicyBinding struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// binding 的期望行为。
	Spec MutatingAdmissionPolicyBindingSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// MutatingAdmissionPolicyBindingList 是绑定对象的列表。
type MutatingAdmissionPolicyBindingList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// PolicyBinding 列表。
	Items []MutatingAdmissionPolicyBinding `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MutatingAdmissionPolicyBindingSpec 描述 binding 的规格。
type MutatingAdmissionPolicyBindingSpec struct {
	// 本 binding 绑定的策略名。引用的资源不存在时 binding 无效并被忽略。必填。
	PolicyName string `json:"policyName,omitempty" protobuf:"bytes,1,rep,name=policyName"`

	// 配置策略所用的参数资源，类型应与所绑定策略的 spec.paramKind 一致。
	// 策略定义了 paramKind 但参数不存在时 binding 视为配置错误并应用策略的 FailurePolicy；
	// 策略未定义 paramKind 时忽略该字段。
	// +optional
	ParamRef *ParamRef `json:"paramRef,omitempty" protobuf:"bytes,2,rep,name=paramRef"`

	// 限制本 binding 匹配并可变更的资源。即使命中，仍需满足策略的
	// matchConstraints 和 matchConditions 才会被变更。未设置时不加限制。
	// 其中 resourceRules 可选，留空不限制；这与策略的 matchConstraints 不同。
	// 仅允许 CREATE、UPDATE、CONNECT，不能匹配 DELETE；'*' 表示这三者。
	// +optional
	MatchResources *MatchResources `json:"matchResources,omitempty" protobuf:"bytes,3,rep,name=matchResources"`
}

// ReinvocationPolicyType 定义其他准入插件也修改对象时采用的策略。
// +enum
type ReinvocationPolicyType string

const (
	// 单次准入评估中变更操作最多调用一次。
	NeverReinvocationPolicy ReinvocationPolicyType = "Never"
	// 首次调用后对象又被其他插件修改时，变更操作至少再调用一次。
	IfNeededReinvocationPolicy ReinvocationPolicyType = "IfNeeded"
)

// RuleWithOperations 是 Operations 与 Resources 组成的元组，建议确保展开后各项合法。
type RuleWithOperations struct {
	// 准入 hook 关心的操作：CREATE、UPDATE、DELETE、CONNECT，或 '*' 表示以上全部及未来新增操作。
	// 出现 '*' 时切片长度必须为 1。必填。
	// +listType=atomic
	Operations []OperationType `json:"operations,omitempty" protobuf:"bytes,1,rep,name=operations,casttype=OperationType"`
	// 内嵌的 Rule，描述 APIGroups、APIVersions、Resources 等其他匹配条件。
	Rule `json:"" protobuf:"bytes,2,opt,name=rule"`
}

// OperationType 表示请求的操作类型。
// +enum
type OperationType string

// 这些常量须与 k8s.io/kubernetes/pkg/admission/interface.go 中的定义保持一致。
const (
	OperationAll OperationType = "*"
	Create       OperationType = "CREATE"
	Update       OperationType = "UPDATE"
	Delete       OperationType = "DELETE"
	Connect      OperationType = "CONNECT"
)

// WebhookClientConfig 包含与 webhook 建立 TLS 连接所需的信息。
type WebhookClientConfig struct {
	// webhook 地址，标准 URL 形式 `scheme://host:port/path`；url 与 service 必须二选一。
	// host 不应指向集群内服务，那要改用 service：部分 apiserver 只能通过外部 DNS
	// 解析 host（kube-apiserver 无法解析集群内 DNS，否则违反分层），host 也可以是 IP。
	// 用 localhost 或 127.0.0.1 作 host 风险很高，除非确保所有可能调用该 webhook 的
	// apiserver 主机上都运行着它，否则部署难以迁移。
	// scheme 必须为 "https"，URL 必须以 "https://" 开头。path 可选，可携带任意字符串
	// （如集群标识）。不允许 user:password@ 形式的用户名密码、片段（#...）及查询参数（?...）。
	//
	// +optional
	URL *string `json:"url,omitempty" protobuf:"bytes,3,opt,name=url"`

	// webhook 对应的服务引用，与 url 二选一。webhook 在集群内运行时应使用 service。
	//
	// +optional
	Service *ServiceReference `json:"service,omitempty" protobuf:"bytes,1,opt,name=service"`

	// PEM 编码的 CA 证书包，用于校验 webhook 服务端证书。未指定时使用 apiserver 的系统信任根。
	// +optional
	CABundle []byte `json:"caBundle,omitempty" protobuf:"bytes,2,opt,name=caBundle"`
}

// ServiceReference 持有对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// 服务所在命名空间。必填。
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`
	// 服务名称。必填。
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// 请求该服务时附带的 URL 路径，可选。
	// +optional
	Path *string `json:"path,omitempty" protobuf:"bytes,3,opt,name=path"`

	// 承载 webhook 的服务端口。为兼容旧版本默认 443，合法范围 1-65535。
	// +optional
	Port *int32 `json:"port,omitempty" protobuf:"varint,4,opt,name=port"`
}

// MatchCondition 表示请求发送给 webhook 前必须满足的条件。
type MatchCondition struct {
	// 匹配条件的标识符，用于 MatchConditions 的合并及日志定位，应能概括对应表达式。
	// 须为合法限定名：字母数字开头结尾，中间可为 '-'、'_'、'.'，
	// 可带 DNS 子域前缀和 '/'（如 'example.com/MyName'）。
	//
	// 必填。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// 由 CEL 求值且结果必须为 bool。可访问 AdmissionRequest 和 Authorizer 的内容，
	// 变量有：object（DELETE 时为 null）、oldObject（CREATE 时为 null）、
	// request（请求属性）、authorizer（CEL 授权器）、authorizer.requestResource。
	// CEL 文档见 https://kubernetes.io/docs/reference/using-api/cel/。
	//
	// 必填。
	Expression string `json:"expression" protobuf:"bytes,2,opt,name=expression"`
}
