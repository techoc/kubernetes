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

// Attributes is an interface used by an Authorizer to get information about a
// request's metadata, that is used to compute an unconditional or conditional
// authorization decision.
//
// Attributes 必须被视为只读快照：授权器可以并发调用这些方法，且不得
// 修改返回的选择器或其中的对象。
type Attributes interface {
	// GetUser returns the user.Info object to authorize
	GetUser() user.Info

	// GetVerb returns the kube verb associated with API requests (this includes get, list, watch, create, update, patch, delete, deletecollection, and proxy),
	// or the lowercased HTTP verb associated with non-API requests (this includes get, put, post, patch, and delete)
	GetVerb() string

	// When IsReadOnly() == true, the request has no side effects, other than
	// caching, logging, and other incidentals.
	IsReadOnly() bool

	// The namespace of the object, if a request is for a REST object.
	GetNamespace() string

	// The kind of object, if a request is for a REST object.
	GetResource() string

	// GetSubresource returns the subresource being requested, if present
	GetSubresource() string

	// GetName returns the name of the object as parsed off the request.  This will not be present for all request types, but
	// will be present for: get, update, delete
	GetName() string

	// The group of the resource, if a request is for a REST object.
	GetAPIGroup() string

	// GetAPIVersion returns the version of the group requested, if a request is for a REST object.
	GetAPIVersion() string

	// IsResourceRequest returns true for requests to API resources, like /api/v1/nodes,
	// and false for non-resource endpoints like /api, /healthz
	IsResourceRequest() bool

	// GetPath returns the path of the request
	GetPath() string

	// ParseFieldSelector is lazy, thread-safe, and stores the parsed result and error.
	// It returns an error if the field selector cannot be parsed.
	// The returned requirements must be treated as readonly and not modified.
	GetFieldSelector() (fields.Requirements, error)

	// ParseLabelSelector is lazy, thread-safe, and stores the parsed result and error.
	// It returns an error if the label selector cannot be parsed.
	// The returned requirements must be treated as readonly and not modified.
	GetLabelSelector() (labels.Requirements, error)
}

// UnconditionalAuthorizer is a downscoped variant of Authorizer, which only gives the
// caller the ability to call the conditions-unaware Authorize method.
//
// 当调用方不需要条件授权时，应传入该接口而非完整 Authorizer，以限制
// 其能力面。
type UnconditionalAuthorizer interface {
	Authorize(ctx context.Context, a Attributes) (authorized Decision, reason string, err error)
}

// Authorizer makes an authorization decision based on information gained by making
// zero or more calls to methods of the Attributes interface. It might return
// an error together with any decision. It is up to the caller to decide whether
// that error is critical or not.
//
// The kube-apiserver WithAuthorization filter ignores errors when the decision is
// Allow, but returns response code 500 if an error is returned with a Deny or
// NoOpinion (instead of the usual 403).
//
// Any authorizer must implement this interface, but when passing a handle to an
// authorizer, one might choose whether to pass the Authorizer or smaller UnconditionalAuthorizer
// interface, depending on whether the receiver should be able to perform conditional
// authorization or not.
type Authorizer interface {
	UnconditionalAuthorizer

	// ConditionsAwareAuthorize returns an unconditional, conditional, or unioned
	// decision, where the error and reason is part of the Decision struct.
	//
	// An authorizer who is not conditions-aware MUST implement this function as
	// "return authorizer.ConditionsAwareDecisionFromParts(self.Authorize(ctx, a))",
	// such that conditions-aware callers to this authorizer get the same output
	// as if they called Authorize. Callers are only expected to call one of
	// Authorize or ConditionsAwareAuthorize, not both.
	ConditionsAwareAuthorize(ctx context.Context, a Attributes) ConditionsAwareDecision

	// EvaluateConditions evaluates a conditional or unioned ConditionsAwareDecision against previously-unknown data.
	//
	// An authorizer who does not support conditions should fail closed and
	// return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
	//
	// The context should only be used for timeouts/cancellation/tracing, and should not influence the
	// evaluation outcome. Only the given decision and data may infuence the outcome. data must be non-nil.
	EvaluateConditions(ctx context.Context, decision ConditionsAwareDecision, data ConditionsData) (authorized Decision, reason string, err error)
}

// ErrorConditionEvaluationNotSupported is returned by authorizer implementations
// that do not support condition evaluation.
//
// 不支持条件的授权器必须 fail closed：返回 Deny 而不是 NoOpinion，
// 否则条件化请求可能被后续授权器意外放行。
var ErrorConditionEvaluationNotSupported = errors.New("condition evaluation not supported")

// AuthorizerFunc implements Authorizer
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

// RuleResolver provides a mechanism for resolving the list of rules that apply to a given user within a namespace.
type RuleResolver interface {
	// RulesFor get the list of cluster wide rules, the list of rules in the specific namespace, incomplete status and errors.
	RulesFor(ctx context.Context, user user.Info, namespace string) ([]ResourceRuleInfo, []NonResourceRuleInfo, bool, error)
}

// RequestAttributesGetter provides a function that extracts Attributes from an http.Request
type RequestAttributesGetter interface {
	GetRequestAttributes(user.Info, *http.Request) Attributes
}

// AttributesRecord implements Attributes interface.
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

// Decision represents an final, unconditional authorization decision.
// The zero value (0) of Decision is DecisionDeny.
//
// 零值即 Deny 是刻意的 fail-closed 设计：忘记赋值的决策不会误放行。
type Decision int

const (
	// DecisionDeny means that an authorizer decided to deny the action.
	DecisionDeny Decision = iota
	// DecisionAllow means that an authorizer decided to allow the action.
	DecisionAllow
	// DecisionNoOpinion means that an authorizer has no opinion on whether
	// to allow or deny an action. If there are multiple unioned authorizers,
	// this means that the request can thus get allowed by some later authorizer.
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

// Condition represents one authorization condition that is part of a ConditionsMap.
// The effect of a condition is defined by whether it is part of the Deny/NoOpinion/Allow
// conditions list in the ConditionsMap.
//
// A Condition must be immutable and thread-safe.
//
// 条件化授权的核心：授权器先给出“若满足某条件则放行/拒绝”的决策，
// 由调用方在拿到真实数据（如资源内容）后再求值，从而避免授权器直接
// 读取对象存储。
type Condition interface {
	// GetID uniquely identifies this condition within the scope of the authorizer
	// that authored it. Validated as a Kubernetes label key.
	// Any domain of form *.k8s.io or *.kubernetes.io is reserved for Kubernetes use.
	// Required.
	GetID() string

	// GetType describes the type of the condition, if there are multiple possibilities.
	// Should be formatted as a Kubernetes label key.
	// Any domain of form *.k8s.io or *.kubernetes.io is reserved for Kubernetes use.
	// Optional. Can be omitted if the authorizer already knows how to evaluate the condition.
	GetType() string

	// GetCondition returns a string encoding of the condition to be evaluated.
	// It is a pure, deterministic function from ConditionsData to a boolean (or error).
	// Might or might not be human-readable.
	// Optional, if the ID alone is enough for the authorizer to know how to evaluate the condition.
	GetCondition() string

	// GetDescription is an optional human-friendly description that can be shown
	// as an error message or for debugging. Optional.
	GetDescription() string

	// Evaluate evaluates the condition to a boolean, returns an error, or returns "unevaluatable".
	// If an authorizer already has a pre-compiled condition, this avoids one serialization roundtrip,
	// with potentially expensive deserialization/parsing. However, if the condition underwent a
	// serialize/deserialize roundtrip (e.g. when the caller is an aggregated API server), the authorizer
	// might have to evaluate the condition from its serialized form using evaluateFunc in
	// ConditionsMap.Evaluate.
	// Evaluate must be safe to call repeatedly and concurrently.
	//
	// The context should only be used for timeouts/cancellation/tracing, and should not influence the
	// evaluation outcome. Only the condition itself and data can infuence the outcome.
	Evaluate(ctx context.Context, data ConditionsData) ConditionEvaluationResult
}

// EvaluateConditionFunc is a function that is able to concretely evaluate a condition to a boolean or error.
// 返回错误表示条件无法判定，调用方应据此 fail closed。
type EvaluateConditionFunc func(ctx context.Context, condition Condition, data ConditionsData) (bool, error)

// MaybeEvaluateConditionFunc allows potentially evaluating a condition, returning Unevaluatable if a truth value or error cannot be assigned.
type MaybeEvaluateConditionFunc func(ctx context.Context, condition Condition, data ConditionsData) ConditionEvaluationResult

// AdmissionOperation represents the admission operation,
// for example CREATE, UPDATE, DELETE. The constants are
// defined in k8s.io/apiserver/pkg/admission, but the
// type is defined here, because this package is more generic
// than the admission package (thus avoiding import cycles)
type AdmissionOperation string

// ConditionsData represents the data available for conditions
// to evaluate against. This is by design a subset of admission.Attributes.
//
// 之所以是子集：授权层不应依赖 admission 包（会形成循环），同时限制
// 条件能看到的输入，降低信息泄露面。
type ConditionsData interface {
	// GetName returns the name of the object as presented in the request. On a CREATE operation, the client
	// may omit name and rely on the server to generate the name. If that is the case, this method will return
	// the empty string
	GetName() string
	// GetNamespace is the namespace associated with the request (if any)
	GetNamespace() string
	// GetResource is the name of the resource being requested. This is not the kind. For example: pods
	GetResource() schema.GroupVersionResource
	// GetSubresource is the name of the subresource being requested. This is a different resource, scoped to the parent resource, but it may have a different kind.
	// For instance, /pods has the resource "pods" and the kind "Pod", while /pods/foo/status has the resource "pods", the sub resource "status", and the kind "Pod"
	// (because status operates on pods). The binding resource for a pod though may be /pods/foo/binding, which has resource "pods", subresource "binding", and kind "Binding".
	GetSubresource() string
	// GetOperation is the operation being performed
	GetOperation() AdmissionOperation
	// GetOperationOptions is the options for the operation being performed
	GetOperationOptions() runtime.Object
	// IsDryRun indicates that modifications will definitely not be persisted for this request. This is to prevent
	// admission controllers with side effects and a method of reconciliation from being overwhelmed.
	// However, a value of false for this does not mean that the modification will be persisted, because it
	// could still be rejected by a subsequent validation step.
	IsDryRun() bool
	// GetObject is the object from the incoming request. Only populated for CREATE and UPDATE requests.
	GetObject() runtime.Object
	// GetOldObject is the existing object in storage. Only populated for UPDATE and DELETE requests.
	GetOldObject() runtime.Object
	// GetKind is the type of object being manipulated. For example: Pod
	GetKind() schema.GroupVersionKind
	// GetUserInfo is information about the requesting user
	GetUserInfo() user.Info
}
