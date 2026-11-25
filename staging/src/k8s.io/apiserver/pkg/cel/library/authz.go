/*
Copyright 2023 The Kubernetes Authors.

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

package library

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// Authz 为 CEL 提供授权检查函数库。
// 注意：只有 API 中向 CEL 表达式提供了 'authorizer' 变量的字段才支持授权检查；
// 具体哪些字段可用请查阅使用 CEL 的 API 字段文档。
//
// Authz 提供用于执行授权检查的 CEL 函数库扩展。
// 注意：授权检查仅支持 API 中为 CEL 表达式提供 'authorizer' 变量的
// CEL 表达式字段。请查阅使用 CEL 表达式的 API 字段文档，确认是否提供
// 'authorizer' 变量。
//
// path
//
// 返回一个 PathCheck，用于检查对非资源请求路径（例如 /healthz）的授权。
// 如果 path 是空字符串，则返回错误。
// 注意：不强制要求以 '/' 开头。
//
//	<Authorizer>.path(<string>) <PathCheck>
//
// 示例：
//
//	authorizer.path('/healthz') // returns a PathCheck for the '/healthz' API path
//	authorizer.path('') // results in "path must not be empty" error
//	authorizer.path('  ') // results in "path must not be empty" error
//
// group
//
// 返回一个 GroupCheck，用于检查对特定 API 组的 API 资源的授权。
// 注意：授权检查仅支持 API 中为 CEL 表达式提供 'authorizer' 变量的
// CEL 表达式字段。请查阅使用 CEL 表达式的 API 字段文档，确认是否提供
// 'authorizer' 变量。
//
//	<Authorizer>.group(<string>) <GroupCheck>
//
// 示例：
//
//	authorizer.group('apps') // returns a GroupCheck for the 'apps' API group
//	authorizer.group('') // returns a GroupCheck for the core API group
//	authorizer.group('example.com') // returns a GroupCheck for the custom resources in the 'example.com' API group
//
// serviceAccount
//
// 返回一个 Authorizer，用于检查对给定服务账号命名空间和名称的授权。
// 如果 name 不是合法的 DNS subdomain 字符串（按 RFC 1123 定义），则返回错误。
// 如果 namespace 不是合法的 DNS label（按 RFC 1123 定义），则返回错误。
//
//	<Authorizer>.serviceAccount(<string>, <string>) <Authorizer>
//
// 示例：
//
//	authorizer.serviceAccount('default', 'myserviceaccount') // returns an Authorizer for the service account with namespace 'default' and name 'myserviceaccount'
//	authorizer.serviceAccount('not@a#valid!namespace', 'validname') // returns an error
//	authorizer.serviceAccount('valid.example.com', 'invalid@*name') // returns an error
//
// resource
//
// 返回一个 ResourceCheck，用于检查对特定 API 资源的授权。
// 注意：提供的 resource 字符串应当是 Kubernetes API 资源的小写复数名称。
//
//	<GroupCheck>.resource(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('apps').resource('deployments') // returns a ResourceCheck for the 'deployments' resources in the 'apps' group.
//	authorizer.group('').resource('pods') // returns a ResourceCheck for the 'pods' resources in the core group.
//	authorizer.group('apps').resource('') // results in "resource must not be empty" error
//	authorizer.group('apps').resource('  ') // results in "resource must not be empty" error
//
// subresource
//
// 返回一个 ResourceCheck，用于检查对某个 API 资源特定子资源的授权。
// 如果 subresource 设为 ""，则该 ResourceCheck 的 subresource 字段视为未设置。
//
//	<ResourceCheck>.subresource(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('').resource('pods').subresource('status') // returns a ResourceCheck the 'status' subresource of 'pods'
//	authorizer.group('apps').resource('deployments').subresource('scale') // returns a ResourceCheck the 'scale' subresource of 'deployments'
//	authorizer.group('example.com').resource('widgets').subresource('scale') // returns a ResourceCheck for the 'scale' subresource of the 'widgets' custom resource
//	authorizer.group('example.com').resource('widgets').subresource('') // returns a ResourceCheck for the 'widgets' resource.
//
// namespace
//
// 返回一个 ResourceCheck，用于检查对特定命名空间的授权。
// 对集群级资源，不需要调用 namespace()；namespace 默认为 ""，
// 而这正是检查集群级资源时应使用的正确命名空间值。
// 如果 namespace 设为 ""，该 ResourceCheck 将检查集群级（cluster scope）授权。
//
//	<ResourceCheck>.namespace(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('apps').resource('deployments').namespace('test') // returns a ResourceCheck for 'deployments' in the 'test' namespace
//	authorizer.group('').resource('pods').namespace('default') // returns a ResourceCheck for 'pods' in the 'default' namespace
//	authorizer.group('').resource('widgets').namespace('') // returns a ResourceCheck for 'widgets' in the cluster scope
//
// name
//
// 返回一个 ResourceCheck，用于检查对特定资源名称的授权。
// 如果 name 设为 ""，则该 ResourceCheck 的 name 字段视为未设置。
//
//	<ResourceCheck>.name(<name>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('apps').resource('deployments').namespace('test').name('backend') // returns a ResourceCheck for the 'backend' 'deployments' resource in the 'test' namespace
//	authorizer.group('apps').resource('deployments').namespace('test').name('') // returns a ResourceCheck for the 'deployments' resource in the 'test' namespace
//
// check
//
// 对 PathCheck：检查发起请求的主体（用户或服务账号）是否被授权使用该路径的 HTTP 请求动词。
// 对 ResourceCheck：检查发起请求的主体（用户或服务账号）是否被授权使用该 API 动词以及 ResourceCheck 配置的授权检查项。
// check 操作可能开销很大，在使用 webhook 授权模式的集群中尤其如此。
//
//	<PathCheck>.check(<check>) <Decision>
//	<ResourceCheck>.check(<check>) <Decision>
//
// 示例：
//
//	authorizer.group('').resource('pods').namespace('default').check('create') // Checks if the principal (user or service account) is authorized create pods in the 'default' namespace.
//	authorizer.path('/healthz').check('get') // Checks if the principal (user or service account) is authorized to make HTTP GET requests to the /healthz API path.
//
// allowed
//
// 如果授权器对该检查的决定是 "allow"，返回 true。注意：如果授权器的决定是
// "no opinion"（不表态），'allowed' 函数将返回 false。
//
//	<Decision>.allowed() <bool>
//
// 示例：
//
//	authorizer.group('').resource('pods').namespace('default').check('create').allowed() // Returns true if the principal (user or service account) is allowed create pods in the 'default' namespace.
//	authorizer.path('/healthz').check('get').allowed()  // Returns true if the principal (user or service account) is allowed to make HTTP GET requests to the /healthz API path.
//
// reason
//
// 返回授权决定的字符串原因
//
//	<Decision>.reason() <string>
//
// 示例：
//
//	authorizer.path('/healthz').check('GET').reason()
//
// errored
//
// 如果授权检查产生错误，返回 true。
//
//	<Decision>.errored() <bool>
//
// 示例：
//
//	authorizer.group('').resource('pods').namespace('default').check('create').errored() // Returns true if the authorization check resulted in an error
//
// error
//
// 如果授权检查产生错误，返回该错误；否则返回空字符串。
//
//	<Decision>.error() <string>
//
// 示例：
//
//	authorizer.group('').resource('pods').namespace('default').check('create').error()
//
// fieldSelector
//
// 接收字符串形式的 field selector，解析为字段选择器要求，并将其纳入授权检查。
// 如果字段选择器解析失败，则授权检查中不包含任何字段选择器要求。
// Kubernetes 1.31+ 引入，Authz 库版本 1。
//
//	<ResourceCheck>.fieldSelector(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('').resource('pods').fieldSelector('spec.nodeName=mynode').check('list').allowed()
//
// labelSelector（v1 引入，Kubernetes 1.31+）
//
// 接收字符串形式的 label selector，解析为标签选择器要求，并将其纳入授权检查。
// 如果标签选择器解析失败，则授权检查中不包含任何标签选择器要求。
// Kubernetes 1.31+ 引入，Authz 库版本 1。
//
//	<ResourceCheck>.labelSelector(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('').resource('pods').labelSelector('app=example').check('list').allowed()
//
// Authz 返回可注册到 CEL 环境的授权函数库。
func Authz() cel.EnvOption {
	return cel.Lib(authzLib)
}

var authzLib = &authz{}

// authz 实现 cel.Library，提供 path/group/serviceAccount/resource 等函数。
type authz struct{}

// LibraryName 返回库名称，用于环境配置与冲突检测。
func (*authz) LibraryName() string {
	return "kubernetes.authz"
}

// Types 返回该库引入的 CEL 对象类型。
func (*authz) Types() []*cel.Type {
	return []*cel.Type{
		AuthorizerType,
		PathCheckType,
		GroupCheckType,
		ResourceCheckType,
		DecisionType}
}

// declarations 返回函数重载声明。
func (*authz) declarations() map[string][]cel.FunctionOpt {
	return authzLibraryDecls
}

// authzLibraryDecls 定义 Authz 库的全部函数重载：
// 以 Authorizer 为接收者的 path/group/serviceAccount，以 GroupCheck 为接收者的 resource，
// 以 ResourceCheck 为接收者的 subresource/namespace/name/check，以及 Decision 上的查询方法。
var authzLibraryDecls = map[string][]cel.FunctionOpt{
	"path": {
		cel.MemberOverload("authorizer_path", []*cel.Type{AuthorizerType, cel.StringType}, PathCheckType,
			cel.BinaryBinding(authorizerPath))},
	"group": {
		cel.MemberOverload("authorizer_group", []*cel.Type{AuthorizerType, cel.StringType}, GroupCheckType,
			cel.BinaryBinding(authorizerGroup))},
	"serviceAccount": {
		cel.MemberOverload("authorizer_serviceaccount", []*cel.Type{AuthorizerType, cel.StringType, cel.StringType}, AuthorizerType,
			cel.FunctionBinding(authorizerServiceAccount))},
	"resource": {
		cel.MemberOverload("groupcheck_resource", []*cel.Type{GroupCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(groupCheckResource))},
	"subresource": {
		cel.MemberOverload("resourcecheck_subresource", []*cel.Type{ResourceCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(resourceCheckSubresource))},
	"namespace": {
		cel.MemberOverload("resourcecheck_namespace", []*cel.Type{ResourceCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(resourceCheckNamespace))},
	"name": {
		cel.MemberOverload("resourcecheck_name", []*cel.Type{ResourceCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(resourceCheckName))},
	"check": {
		cel.MemberOverload("pathcheck_check", []*cel.Type{PathCheckType, cel.StringType}, DecisionType,
			cel.BinaryBinding(pathCheckCheck)),
		cel.MemberOverload("resourcecheck_check", []*cel.Type{ResourceCheckType, cel.StringType}, DecisionType,
			cel.BinaryBinding(resourceCheckCheck))},
	"errored": {
		cel.MemberOverload("decision_errored", []*cel.Type{DecisionType}, cel.BoolType,
			cel.UnaryBinding(decisionErrored))},
	"error": {
		cel.MemberOverload("decision_error", []*cel.Type{DecisionType}, cel.StringType,
			cel.UnaryBinding(decisionError))},
	"allowed": {
		cel.MemberOverload("decision_allowed", []*cel.Type{DecisionType}, cel.BoolType,
			cel.UnaryBinding(decisionAllowed))},
	"reason": {
		cel.MemberOverload("decision_reason", []*cel.Type{DecisionType}, cel.StringType,
			cel.UnaryBinding(decisionReason))},
}

// CompileOptions 把声明转换为 CEL 环境选项。
func (*authz) CompileOptions() []cel.EnvOption {
	options := make([]cel.EnvOption, 0, len(authzLibraryDecls))
	for name, overloads := range authzLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 无额外程序选项：授权检查在绑定的 Go 函数内同步完成。
func (*authz) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// AuthzSelectors 为授权检查增加 fieldSelector 与 labelSelector 过滤能力，
// 需要同时启用 Authz 库。使用方式与可用性参见 Authz 库文档。
//
// AuthzSelectors 提供一个 CEL 函数库扩展，用于给授权检查添加 fieldSelector 和
// labelSelector 过滤器。它需要配合 Authz 库使用。
// authorizer 变量的用法与可用范围见 Authz 库文档。
//
// fieldSelector
//
// 接收字符串形式的 field selector，解析为字段选择器要求，并将其纳入授权检查。
// 如果字段选择器解析失败，则授权检查中不包含任何字段选择器要求。
// Kubernetes 1.31+ 引入。
//
//	<ResourceCheck>.fieldSelector(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('').resource('pods').fieldSelector('spec.nodeName=mynode').check('list').allowed()
//
// labelSelector
//
// 接收字符串形式的 label selector，解析为标签选择器要求，并将其纳入授权检查。
// 如果标签选择器解析失败，则授权检查中不包含任何标签选择器要求。
// Kubernetes 1.31+ 引入。
//
//	<ResourceCheck>.labelSelector(<string>) <ResourceCheck>
//
// 示例：
//
//	authorizer.group('').resource('pods').labelSelector('app=example').check('list').allowed()
//
// AuthzSelectors 返回可注册到 CEL 环境的选择器扩展库。
func AuthzSelectors() cel.EnvOption {
	return cel.Lib(authzSelectorsLib)
}

var authzSelectorsLib = &authzSelectors{}

// authzSelectors 实现 cel.Library，只新增 fieldSelector/labelSelector 两个函数。
type authzSelectors struct{}

// LibraryName 返回库名称。
func (*authzSelectors) LibraryName() string {
	return "kubernetes.authzSelectors"
}

// Types 返回该库使用的对象类型：只扩展 ResourceCheck。
func (*authzSelectors) Types() []*cel.Type {
	return []*cel.Type{ResourceCheckType}
}

// declarations 返回函数重载声明。
func (*authzSelectors) declarations() map[string][]cel.FunctionOpt {
	return authzSelectorsLibraryDecls
}

// authzSelectorsLibraryDecls 定义选择器扩展的函数重载。
var authzSelectorsLibraryDecls = map[string][]cel.FunctionOpt{
	"fieldSelector": {
		cel.MemberOverload("authorizer_fieldselector", []*cel.Type{ResourceCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(resourceCheckFieldSelector))},
	"labelSelector": {
		cel.MemberOverload("authorizer_labelselector", []*cel.Type{ResourceCheckType, cel.StringType}, ResourceCheckType,
			cel.BinaryBinding(resourceCheckLabelSelector))},
}

// CompileOptions 把声明转换为 CEL 环境选项。
func (*authzSelectors) CompileOptions() []cel.EnvOption {
	options := make([]cel.EnvOption, 0, len(authzSelectorsLibraryDecls))
	for name, overloads := range authzSelectorsLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 无额外程序选项。
func (*authzSelectors) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// authorizerPath 实现 authorizer.path(string)：路径去空白后不能为空。
func authorizerPath(arg1, arg2 ref.Val) ref.Val {
	authz, ok := arg1.(authorizerVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	path, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	// 空白路径既无法匹配任何 API 路径，也容易掩盖配置错误，直接报错。
	if len(strings.TrimSpace(path)) == 0 {
		return types.NewErr("path must not be empty")
	}

	return authz.pathCheck(path)
}

// authorizerGroup 实现 authorizer.group(string)：空字符串表示 core group。
func authorizerGroup(arg1, arg2 ref.Val) ref.Val {
	authz, ok := arg1.(authorizerVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	group, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	return authz.groupCheck(group)
}

// authorizerServiceAccount 实现 authorizer.serviceAccount(namespace, name)：
// 返回以该 ServiceAccount 身份进行后续检查的 Authorizer。
func authorizerServiceAccount(args ...ref.Val) ref.Val {
	argn := len(args)
	if argn != 3 {
		return types.NoSuchOverloadErr()
	}

	authz, ok := args[0].(authorizerVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[0])
	}

	namespace, ok := args[1].Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[1])
	}

	name, ok := args[2].Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[2])
	}

	// 在编译/求值阶段校验名称格式，避免构造出不可能存在的 SA 身份。
	if errors := apimachineryvalidation.ValidateServiceAccountName(name, false); len(errors) > 0 {
		return types.NewErr("Invalid service account name")
	}
	if errors := apimachineryvalidation.ValidateNamespaceName(namespace, false); len(errors) > 0 {
		return types.NewErr("Invalid service account namespace")
	}
	return authz.serviceAccount(namespace, name)
}

// groupCheckResource 实现 GroupCheck.resource(string)：资源名不能为空。
func groupCheckResource(arg1, arg2 ref.Val) ref.Val {
	groupCheck, ok := arg1.(groupCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	resource, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	if len(strings.TrimSpace(resource)) == 0 {
		return types.NewErr("resource must not be empty")
	}
	return groupCheck.resourceCheck(resource)
}

// resourceCheckSubresource 设置子资源名；空字符串表示无子资源。
func resourceCheckSubresource(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	subresource, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	// 值类型拷贝后修改，保持 CEL 值的不可变语义。
	result := resourceCheck
	result.subresource = subresource
	return result
}

// resourceCheckFieldSelector 记录原始字段选择器字符串；
// 解析推迟到 Authorize 时进行，解析失败时仍会发起检查并带上解析错误。
func resourceCheckFieldSelector(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	fieldSelector, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	result := resourceCheck
	result.fieldSelector = fieldSelector
	return result
}

// resourceCheckLabelSelector 记录原始标签选择器字符串，解析同样推迟到 Authorize。
func resourceCheckLabelSelector(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	labelSelector, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	result := resourceCheck
	result.labelSelector = labelSelector
	return result
}

// resourceCheckNamespace 设置命名空间；空字符串表示集群级资源。
func resourceCheckNamespace(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	namespace, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	result := resourceCheck
	result.namespace = namespace
	return result
}

// resourceCheckName 设置资源名；空字符串表示不限定具体实例。
func resourceCheckName(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	name, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	result := resourceCheck
	result.name = name
	return result
}

// pathCheckCheck 执行非资源请求的授权检查。verb 为 HTTP 方法。
func pathCheckCheck(arg1, arg2 ref.Val) ref.Val {
	pathCheck, ok := arg1.(pathCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	httpRequestVerb, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	return pathCheck.Authorize(context.TODO(), httpRequestVerb)
}

// resourceCheckCheck 执行资源请求的授权检查。
func resourceCheckCheck(arg1, arg2 ref.Val) ref.Val {
	resourceCheck, ok := arg1.(resourceCheckVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	apiVerb, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	return resourceCheck.Authorize(context.TODO(), apiVerb)
}

// decisionErrored 返回检查是否出错。
func decisionErrored(arg ref.Val) ref.Val {
	decision, ok := arg.(decisionVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(decision.err != nil)
}

// decisionError 返回错误字符串；无错误时返回空串。
func decisionError(arg ref.Val) ref.Val {
	decision, ok := arg.(decisionVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	if decision.err == nil {
		return types.String("")
	}
	return types.String(decision.err.Error())
}

// decisionAllowed 仅在决策为 Allow 时返回 true；
// NoOpinion 不算 allowed，避免把“无意见”误当作授权通过。
func decisionAllowed(arg ref.Val) ref.Val {
	decision, ok := arg.(decisionVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(decision.authDecision == authorizer.DecisionAllow)
}

// decisionReason 返回授权决策的原因字符串。
func decisionReason(arg ref.Val) ref.Val {
	decision, ok := arg.(decisionVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.String(decision.reason)
}

var (
	// AuthorizerType 是 authorizer 变量的 CEL 类型。
	AuthorizerType = cel.ObjectType("kubernetes.authorization.Authorizer")
	// PathCheckType 是非资源路径检查的 CEL 类型。
	PathCheckType = cel.ObjectType("kubernetes.authorization.PathCheck")
	// GroupCheckType 是 API group 级检查的 CEL 类型。
	GroupCheckType = cel.ObjectType("kubernetes.authorization.GroupCheck")
	// ResourceCheckType 是资源级检查的 CEL 类型。
	ResourceCheckType = cel.ObjectType("kubernetes.authorization.ResourceCheck")
	// DecisionType 是授权决策结果的 CEL 类型。
	DecisionType = cel.ObjectType("kubernetes.authorization.Decision")
)

// Resource 表示一个 API 资源。
type Resource interface {
	// GetName 返回请求中呈现的对象名称。CREATE 操作中客户端可能省略名称、
	// 由服务端生成，此时该方法返回空字符串
	GetName() string
	// GetNamespace 返回请求关联的命名空间（若有）
	GetNamespace() string
	// GetResource 返回被请求的资源名（不是 kind）。例如：pods
	GetResource() schema.GroupVersionResource
	// GetSubresource 返回被请求的子资源名。子资源是作用在父资源上的另一种资源，kind 可能不同。
	// 例如 /pods 的资源是 "pods"、kind 是 "Pod"，而 /pods/foo/status 的资源是 "pods"、
	// 子资源是 "status"、kind 仍是 "Pod"（因为 status 作用于 pod）。pod 的绑定子资源则是
	// /pods/foo/binding，资源 "pods"、子资源 "binding"、kind "Binding"。
	GetSubresource() string
}

// NewAuthorizerVal 为给定用户与授权器构造 CEL authorizer 值。
func NewAuthorizerVal(userInfo user.Info, authorizer authorizer.UnconditionalAuthorizer) ref.Val {
	return authorizerVal{receiverOnlyObjectVal: receiverOnlyVal(AuthorizerType), userInfo: userInfo, authAuthorizer: authorizer}
}

// NewResourceAuthorizerVal 构造已预填当前请求资源信息的 ResourceCheck 值，
// 使 CEL 中的 authorizer 变量默认针对当前请求的资源做检查。
func NewResourceAuthorizerVal(userInfo user.Info, authorizer authorizer.UnconditionalAuthorizer, requestResource Resource) ref.Val {
	a := authorizerVal{receiverOnlyObjectVal: receiverOnlyVal(AuthorizerType), userInfo: userInfo, authAuthorizer: authorizer}
	resource := requestResource.GetResource()
	g := a.groupCheck(resource.Group)
	r := g.resourceCheck(resource.Resource)
	r.subresource = requestResource.GetSubresource()
	r.namespace = requestResource.GetNamespace()
	r.name = requestResource.GetName()
	return r
}

// authorizerVal 是 CEL 中 authorizer 变量的实现，持有身份与底层授权器。
type authorizerVal struct {
	receiverOnlyObjectVal
	userInfo       user.Info
	authAuthorizer authorizer.UnconditionalAuthorizer
}

// pathCheck 构造针对指定路径的检查对象。
func (a authorizerVal) pathCheck(path string) pathCheckVal {
	return pathCheckVal{receiverOnlyObjectVal: receiverOnlyVal(PathCheckType), authorizer: a, path: path}
}

// groupCheck 构造针对指定 API group 的检查对象。
func (a authorizerVal) groupCheck(group string) groupCheckVal {
	return groupCheckVal{receiverOnlyObjectVal: receiverOnlyVal(GroupCheckType), authorizer: a, group: group}
}

// serviceAccount 返回以指定 ServiceAccount 为身份的 authorizerVal；
// 底层授权器保持不变，只有 userInfo 被替换。
func (a authorizerVal) serviceAccount(namespace, name string) authorizerVal {
	sa := &serviceaccount.ServiceAccountInfo{Name: name, Namespace: namespace}
	return authorizerVal{
		receiverOnlyObjectVal: receiverOnlyVal(AuthorizerType),
		userInfo:              sa.UserInfo(),
		authAuthorizer:        a.authAuthorizer,
	}
}

// pathCheckVal 表示一次非资源路径检查。
type pathCheckVal struct {
	receiverOnlyObjectVal
	authorizer authorizerVal
	path       string
}

// Authorize 以当前身份对路径发起授权检查。
func (a pathCheckVal) Authorize(ctx context.Context, verb string) ref.Val {
	attr := &authorizer.AttributesRecord{
		Path: a.path,
		Verb: verb,
		User: a.authorizer.userInfo,
	}

	decision, reason, err := a.authorizer.authAuthorizer.Authorize(ctx, attr)
	return newDecision(decision, err, reason)
}

// groupCheckVal 表示针对某个 API group 的检查构造器。
type groupCheckVal struct {
	receiverOnlyObjectVal
	authorizer authorizerVal
	group      string
}

// resourceCheck 构造针对指定资源的检查对象。
func (g groupCheckVal) resourceCheck(resource string) resourceCheckVal {
	return resourceCheckVal{receiverOnlyObjectVal: receiverOnlyVal(ResourceCheckType), groupCheck: g, resource: resource}
}

// resourceCheckVal 表示一次资源检查的完整参数。
type resourceCheckVal struct {
	receiverOnlyObjectVal
	groupCheck    groupCheckVal
	resource      string
	subresource   string
	namespace     string
	name          string
	fieldSelector string
	labelSelector string
}

// Authorize 以当前身份对资源发起授权检查。
// APIVersion 固定为 "*"：CEL 表达式不感知请求的具体版本，匹配任意版本更符合语义。
func (a resourceCheckVal) Authorize(ctx context.Context, verb string) ref.Val {
	attr := &authorizer.AttributesRecord{
		ResourceRequest: true,
		APIGroup:        a.groupCheck.group,
		APIVersion:      "*",
		Resource:        a.resource,
		Subresource:     a.subresource,
		Namespace:       a.namespace,
		Name:            a.name,
		Verb:            verb,
		User:            a.groupCheck.authorizer.userInfo,
	}

	// 选择器解析失败时不丢弃检查，而是把解析错误一并交给授权器，
	// 由授权器按自身策略处理（通常意味着更严格）。
	if len(a.fieldSelector) > 0 {
		selector, err := fields.ParseSelector(a.fieldSelector)
		if err != nil {
			attr.FieldSelectorRequirements, attr.FieldSelectorParsingErr = nil, err
		} else {
			attr.FieldSelectorRequirements, attr.FieldSelectorParsingErr = selector.Requirements(), nil
		}
	}
	if len(a.labelSelector) > 0 {
		requirements, err := labels.ParseToRequirements(a.labelSelector)
		if err != nil {
			attr.LabelSelectorRequirements, attr.LabelSelectorParsingErr = nil, err
		} else {
			attr.LabelSelectorRequirements, attr.LabelSelectorParsingErr = requirements, nil
		}
	}

	decision, reason, err := a.groupCheck.authorizer.authAuthorizer.Authorize(ctx, attr)
	return newDecision(decision, err, reason)
}

// newDecision 把授权器返回值包装为 CEL decision 值。
func newDecision(authDecision authorizer.Decision, err error, reason string) decisionVal {
	return decisionVal{receiverOnlyObjectVal: receiverOnlyVal(DecisionType), authDecision: authDecision, err: err, reason: reason}
}

// decisionVal 是 CEL 中 Decision 对象的实现。
type decisionVal struct {
	receiverOnlyObjectVal
	err          error
	authDecision authorizer.Decision
	reason       string
}

// receiverOnlyObjectVal 为“只有接收者函数、不向 CEL 暴露任何字段”的对象类型
// 提供 ref.Val 实现。
type receiverOnlyObjectVal struct {
	typeValue *types.Type
}

// receiverOnlyVal 返回给定类型的 receiverOnlyObjectVal。
func receiverOnlyVal(objectType *cel.Type) receiverOnlyObjectVal {
	return receiverOnlyObjectVal{typeValue: types.NewTypeValue(objectType.String())}
}

// ConvertToNative 实现 ref.Val.ConvertToNative：这些对象不支持转为 Go 原生类型。
func (a receiverOnlyObjectVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion error from '%s' to '%v'", a.typeValue.String(), typeDesc)
}

// ConvertToType 实现 ref.Val.ConvertToType：只允许转回自身类型或 type 类型。
func (a receiverOnlyObjectVal) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case a.typeValue:
		return a
	case types.TypeType:
		return a.typeValue
	}
	return types.NewErr("type conversion error from '%s' to '%s'", a.typeValue, typeVal)
}

// Equal 实现 ref.Val.Equal：仅当另一方是同一类型的 receiverOnlyObjectVal 时比较。
func (a receiverOnlyObjectVal) Equal(other ref.Val) ref.Val {
	o, ok := other.(receiverOnlyObjectVal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(a == o)
}

// Type 实现 ref.Val.Type。
func (a receiverOnlyObjectVal) Type() ref.Type {
	return a.typeValue
}

// Value 实现 ref.Val.Value：这些对象不能作为普通值读取。
func (a receiverOnlyObjectVal) Value() any {
	return types.NoSuchOverloadErr()
}
