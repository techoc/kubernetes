/*
Copyright 2014 The Kubernetes Authors.

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

// Package authorizer 定义授权决策的通用契约：输入是请求属性（Attributes），
// 输出是 Allow/Deny/NoOpinion 以及可选的“条件化”决策。
// 该包不依赖 admission 或 registry，避免循环引用。
package authorizer

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

// Attributes 是 Authorizer 用来获取请求元数据的接口，这些元数据用于
// 计算无条件或有条件的授权决策。
//
// Attributes 必须被视为只读快照：授权器可以并发调用这些方法，且不得
// 修改返回的选择器或其中的对象。
type Attributes interface {
	// GetUser 返回用于授权判断的 user.Info 对象。
	GetUser() user.Info

	// GetVerb 返回与 API 请求关联的 kube 动词（包括 get、list、watch、
	// create、update、patch、delete、deletecollection 和 proxy），
	// 或非 API 请求对应的小写 HTTP 动词（包括 get、put、post、patch、delete）。
	GetVerb() string

	// 当 IsReadOnly() == true 时，请求没有副作用，缓存、日志记录及其他
	// 附带行为除外。
	IsReadOnly() bool

	// GetNamespace 返回对象的命名空间，仅当请求针对 REST 对象时存在。
	GetNamespace() string

	// GetResource 返回对象的资源类型，仅当请求针对 REST 对象时存在。
	GetResource() string

	// GetSubresource 返回所请求的子资源，如果存在的话。
	GetSubresource() string

	// GetName 返回从请求中解析出的对象名称。并非所有请求类型都会带有
	// 该名称，但 get、update、delete 请求一定会有。
	GetName() string

	// GetAPIGroup 返回资源所属的组，仅当请求针对 REST 对象时存在。
	GetAPIGroup() string

	// GetAPIVersion 返回所请求的组的版本，仅当请求针对 REST 对象时存在。
	GetAPIVersion() string

	// IsResourceRequest 对指向 API 资源的请求（如 /api/v1/nodes）返回 true，
	// 对非资源端点（如 /api、/healthz）返回 false。
	IsResourceRequest() bool

	// GetPath 返回请求的路径。
	GetPath() string

	// GetFieldSelector 是惰性求值且线程安全的，会缓存解析结果与错误。
	// 若字段选择器无法解析则返回错误。
	// 返回的 requirements 必须视为只读，不得修改。
	GetFieldSelector() (fields.Requirements, error)

	// GetLabelSelector 是惰性求值且线程安全的，会缓存解析结果与错误。
	// 若标签选择器无法解析则返回错误。
	// 返回的 requirements 必须视为只读，不得修改。
	GetLabelSelector() (labels.Requirements, error)
}

// UnconditionalAuthorizer 是 Authorizer 的缩减权限变体，只赋予调用方
// 调用不感知条件的 Authorize 方法的能力。
//
// 当调用方不需要条件授权时，应传入该接口而非完整 Authorizer，以限制
// 其能力面。
type UnconditionalAuthorizer interface {
	Authorize(ctx context.Context, a Attributes) (authorized Decision, reason string, err error)
}

// Authorizer 基于对 Attributes 接口方法进行零次或多次调用所获得的信息
// 做出授权决策。它可能在返回任意决策的同时返回一个错误，该错误是否关键
// 由调用方决定。
//
// kube-apiserver 的 WithAuthorization 过滤器在决策为 Allow 时会忽略错误；
// 但当错误伴随 Deny 或 NoOpinion 返回时，会响应状态码 500（而不是通常
// 的 403）。
//
// 任何授权器都必须实现此接口；但在传递授权器句柄时，可以根据接收方是否
// 应当执行条件授权，选择传递 Authorizer 还是更小的 UnconditionalAuthorizer
// 接口。
type Authorizer interface {
	UnconditionalAuthorizer

	// ConditionsAwareAuthorize 返回无条件、有条件或合并后的决策，其中
	// 错误和原因都是 Decision 结构的一部分。
	//
	// 不感知条件的授权器必须以如下方式实现此函数：
	// "return authorizer.ConditionsAwareDecisionFromParts(self.Authorize(ctx, a))"，
	// 从而使感知条件的调用方得到与直接调用 Authorize 相同的输出。
	// 调用方只应调用 Authorize 或 ConditionsAwareAuthorize 其中之一，
	// 而不是两者都调用。
	ConditionsAwareAuthorize(ctx context.Context, a Attributes) ConditionsAwareDecision

	// EvaluateConditions 针对此前未知的数据评估一个有条件或合并后的
	// ConditionsAwareDecision。
	//
	// 不支持条件的授权器应当 fail closed，返回
	// authorizer.DecisionDeny、""、authorizer.ErrorConditionEvaluationNotSupported。
	//
	// context 只应用于超时/取消/链路追踪，不得影响评估结果。只有给定的
	// decision 和 data 可以影响结果，且 data 必须非 nil。
	EvaluateConditions(ctx context.Context, decision ConditionsAwareDecision, data ConditionsData) (authorized Decision, reason string, err error)
}

// ErrorConditionEvaluationNotSupported 由不支持条件评估的授权器实现返回。
//
// 不支持条件的授权器必须 fail closed：返回 Deny 而不是 NoOpinion，
// 否则条件化请求可能被后续授权器意外放行。
var ErrorConditionEvaluationNotSupported = errors.New("condition evaluation not supported")

// AuthorizerFunc 实现了 Authorizer 接口。
//
// 函数式适配器：默认不提供条件能力，EvaluateConditions 固定 fail closed。
var _ Authorizer = AuthorizerFunc(nil)

// AuthorizerFunc 把普通函数提升为 Authorizer。
type AuthorizerFunc func(ctx context.Context, a Attributes) (Decision, string, error)

// Authorize 调用底层函数。
func (f AuthorizerFunc) Authorize(ctx context.Context, a Attributes) (Decision, string, error) {
	return f(ctx, a)
}

// ConditionsAwareAuthorize 把无条件决策包装成条件化决策，满足 Authorizer
// 对“非条件感知实现”的约定。
func (f AuthorizerFunc) ConditionsAwareAuthorize(ctx context.Context, a Attributes) ConditionsAwareDecision {
	return ConditionsAwareDecisionFromParts(f.Authorize(ctx, a))
}

// EvaluateConditions 不支持条件求值，因此 fail closed。
func (f AuthorizerFunc) EvaluateConditions(_ context.Context, _ ConditionsAwareDecision, _ ConditionsData) (Decision, string, error) {
	return DecisionDeny, "", ErrorConditionEvaluationNotSupported
}

// RuleResolver 提供某种机制，用于解析在某个命名空间内适用于给定用户的
// 规则列表。
type RuleResolver interface {
	// RulesFor 返回集群级规则列表、指定命名空间内的规则列表、不完整状态
	// 以及错误。
	RulesFor(ctx context.Context, user user.Info, namespace string) ([]ResourceRuleInfo, []NonResourceRuleInfo, bool, error)
}

// RequestAttributesGetter 提供从 http.Request 中提取 Attributes 的函数。
type RequestAttributesGetter interface {
	GetRequestAttributes(user.Info, *http.Request) Attributes
}

// AttributesRecord 实现了 Attributes 接口。
//
// 字段全部导出是为了让各 filter 能低成本构造；调用方应把实例当作
// 不可变值传递，不要在使用期间修改字段。
type AttributesRecord struct {
	User            user.Info
	Verb            string
	Namespace       string
	APIGroup        string
	APIVersion      string
	Resource        string
	Subresource     string
	Name            string
	ResourceRequest bool
	Path            string

	FieldSelectorRequirements fields.Requirements
	FieldSelectorParsingErr   error
	LabelSelectorRequirements labels.Requirements
	LabelSelectorParsingErr   error
}

func (a AttributesRecord) GetUser() user.Info {
	return a.User
}

// GetVerb 返回 kube 动词（get/list/watch/...），非资源请求则是小写 HTTP 动词。
func (a AttributesRecord) GetVerb() string {
	return a.Verb
}

// IsReadOnly 仅把 get/list/watch 视为只读；create/update/patch/delete 都可能
// 产生副作用，即便 dry-run 请求也仍按原动词判定。
func (a AttributesRecord) IsReadOnly() bool {
	return a.Verb == "get" || a.Verb == "list" || a.Verb == "watch"
}

func (a AttributesRecord) GetNamespace() string {
	return a.Namespace
}

func (a AttributesRecord) GetResource() string {
	return a.Resource
}

func (a AttributesRecord) GetSubresource() string {
	return a.Subresource
}

func (a AttributesRecord) GetName() string {
	return a.Name
}

func (a AttributesRecord) GetAPIGroup() string {
	return a.APIGroup
}

func (a AttributesRecord) GetAPIVersion() string {
	return a.APIVersion
}

func (a AttributesRecord) IsResourceRequest() bool {
	return a.ResourceRequest
}

func (a AttributesRecord) GetPath() string {
	return a.Path
}

func (a AttributesRecord) GetFieldSelector() (fields.Requirements, error) {
	return a.FieldSelectorRequirements, a.FieldSelectorParsingErr
}

func (a AttributesRecord) GetLabelSelector() (labels.Requirements, error) {
	return a.LabelSelectorRequirements, a.LabelSelectorParsingErr
}

// Decision 表示最终的、无条件的授权决策。Decision 的零值（0）是
// DecisionDeny。
//
// 零值即 Deny 是刻意的 fail-closed 设计：忘记赋值的决策不会误放行。
type Decision int

const (
	// DecisionDeny 表示授权器决定拒绝该操作。
	DecisionDeny Decision = iota
	// DecisionAllow 表示授权器决定放行该操作。
	DecisionAllow
	// DecisionNoOpinion 表示授权器对是否放行或拒绝该操作不表态。若存在
	// 多个联合（unioned）授权器，这意味着该请求仍可能被后续的某个授权器
	// 放行。
	DecisionNoOpinion
)

// String 返回稳定的人类可读名称，用于日志与指标；未知值保留数字便于排障。
func (d Decision) String() string {
	switch d {
	case DecisionDeny:
		return "Deny"
	case DecisionAllow:
		return "Allow"
	case DecisionNoOpinion:
		return "NoOpinion"
	default:
		return fmt.Sprintf("Unknown (%d)", int(d))
	}
}

// Condition 表示单个授权条件，它是 ConditionsMap 的一部分。条件的效果
// 取决于它位于 ConditionsMap 的 Deny/NoOpinion/Allow 条件列表中的哪一类。
//
// Condition 必须是不可变且线程安全的。
//
// 条件化授权的核心：授权器先给出“若满足某条件则放行/拒绝”的决策，
// 由调用方在拿到真实数据（如资源内容）后再求值，从而避免授权器直接
// 读取对象存储。
type Condition interface {
	// GetID 在创建该条件的授权器范围内唯一标识此条件，按 Kubernetes
	// label key 规则校验。形如 *.k8s.io 或 *.kubernetes.io 的域名一律
	// 保留给 Kubernetes 使用。必填。
	GetID() string

	// GetType 描述条件的类型（当存在多种可能类型时），应当按 Kubernetes
	// label key 格式书写。形如 *.k8s.io 或 *.kubernetes.io 的域名一律
	// 保留给 Kubernetes 使用。可选，若授权器已知如何评估该条件，
	// 可以省略。
	GetType() string

	// GetCondition 返回待评估条件的字符串编码形式。它是由 ConditionsData
	// 到布尔值（或错误）的纯函数、确定性函数，不一定人类可读。
	// 若仅凭 ID 授权器已知如何评估该条件，则此项可选。
	GetCondition() string

	// GetDescription 是可选的、面向用户的描述，可作为错误消息展示或用于
	// 调试。可选。
	GetDescription() string

	// Evaluate 将条件求值为布尔值、返回错误，或返回"unevaluatable"
	// （无法求值）。若授权器已持有预编译的条件，可直接借此省去一次序列化
	// 往返（其反序列化/解析开销可能很高）。但若条件经历过序列化/反序列化
	// 往返（例如调用方是聚合 API server），授权器可能需要借助
	// ConditionsMap.Evaluate 中的 evaluateFunc，从序列化形式求值该条件。
	// Evaluate 必须可以安全地重复且并发调用。
	//
	// context 只应用于超时/取消/链路追踪，不得影响评估结果。只有条件本身
	// 和 data 可以影响结果。
	Evaluate(ctx context.Context, data ConditionsData) ConditionEvaluationResult
}

// EvaluateConditionFunc 是能够将条件具体求值为布尔值或错误的函数。
// 返回错误表示条件无法判定，调用方应据此 fail closed。
type EvaluateConditionFunc func(ctx context.Context, condition Condition, data ConditionsData) (bool, error)

// MaybeEvaluateConditionFunc 用于尝试评估条件：当既无法给出真值也无法
// 给出错误时，返回 Unevaluatable。
type MaybeEvaluateConditionFunc func(ctx context.Context, condition Condition, data ConditionsData) ConditionEvaluationResult

// AdmissionOperation 表示准入操作，例如 CREATE、UPDATE、DELETE。相关
// 常量定义在 k8s.io/apiserver/pkg/admission 中，但类型定义在此处，
// 因为本包比 admission 包更通用（从而避免循环导入）。
type AdmissionOperation string

// ConditionsData 表示可供条件求值的数据，按设计它是 admission.Attributes
// 的子集。
//
// 之所以是子集：授权层不应依赖 admission 包（会形成循环），同时限制
// 条件能看到的输入，降低信息泄露面。
type ConditionsData interface {
	// GetName 返回请求中给出的对象名称。在 CREATE 操作中，客户端可以省略
	// 名称、依赖服务端生成名称；若如此，此方法将返回空字符串。
	GetName() string
	// GetNamespace 返回与请求关联的命名空间（如有）。
	GetNamespace() string
	// GetResource 返回所请求资源的名称，不是 kind。例如：pods。
	GetResource() schema.GroupVersionResource
	// GetSubresource 返回所请求子资源的名称。子资源是独立于父资源作用域
	// 内的另一种资源，且可能具有不同的 kind。例如 /pods 的资源为 "pods"、
	// kind 为 "Pod"；/pods/foo/status 的资源为 "pods"、子资源为 "status"、
	// kind 为 "Pod"（因为 status 作用于 pod）。而 pod 的 binding 资源位于
	// /pods/foo/binding，其资源为 "pods"、子资源为 "binding"、
	// kind 为 "Binding"。
	GetSubresource() string
	// GetOperation 返回正在执行的操作。
	GetOperation() AdmissionOperation
	// GetOperationOptions 返回当前操作的操作选项。
	GetOperationOptions() runtime.Object
	// IsDryRun 表示本次请求的修改绝对不会被持久化。这是为了防止带有副作用
	// 且依赖调谐机制的准入控制器被压垮。但取值为 false 并不意味着修改一定会
	// 被持久化，因为后续校验步骤仍可能拒绝该请求。
	IsDryRun() bool
	// GetObject 返回来自入站请求的对象，仅在 CREATE 和 UPDATE 请求中有值。
	GetObject() runtime.Object
	// GetOldObject 返回存储中已存在的对象，仅在 UPDATE 和 DELETE 请求中有值。
	GetOldObject() runtime.Object
	// GetKind 返回被操作对象的类型，例如：Pod。
	GetKind() schema.GroupVersionKind
	// GetUserInfo 返回发起请求的用户的信息。
	GetUserInfo() user.Info
}
