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

package validating

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/cel-go/cel"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/version"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/library"
	"k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/klog/v2"
)

// maxTypesToCheck 是单条 ResourceRule 解析出的 GVK 数量上限，
// 避免宽泛的规则（如多个组×版本×资源）导致过多的 schema 解析开销。
const maxTypesToCheck = 10

// TypeChecker 对 ValidatingAdmissionPolicy 中的 CEL 表达式做静态类型检查，
// 以便在策略写入 Status 时提前暴露类型错误，而不是等到请求路径上才失败。
type TypeChecker struct {
	SchemaResolver resolver.SchemaResolver
	RestMapper     meta.RESTMapper
}

// TypeCheckingContext 保存被检查策略的相关信息；对调用方而言是不透明结构。
type TypeCheckingContext struct {
	gvks          []schema.GroupVersionKind
	declTypes     []*apiservercel.DeclType
	paramGVK      schema.GroupVersionKind
	paramDeclType *apiservercel.DeclType

	variables []v1.Variable
}

// typeOverwrite 把 object/params 的 CEL 类型替换为本次检查解析出的声明类型；
// 两者都可能为 nil（例如参数类型解析失败），此时回退到 Dyn 类型。
type typeOverwrite struct {
	object *apiservercel.DeclType
	params *apiservercel.DeclType
}

// TypeCheckingResult 记录类型检查发现的问题、可能返回的错误，以及检查所针对的 GVK。
type TypeCheckingResult struct {
	// GVK 是本次结果关联的 GroupVersionKind。
	GVK schema.GroupVersionKind
	// Issues 包含类型检查结果中面向用户的机器可读信息。
	Issues error
	// Err 是类型检查过程中遇到的内部错误。
	Err error
}

// TypeCheckingResults 是 TypeCheckingResult 的集合。
type TypeCheckingResults []*TypeCheckingResult

// String 把每个非空结果拼接成多行文本；空结果被跳过，
// 这样调用方可以安全地把输出直接作为 Warning 文本。
func (rs TypeCheckingResults) String() string {
	var messages []string
	for _, r := range rs {
		message := r.String()
		if message != "" {
			messages = append(messages, message)
		}
	}
	return strings.Join(messages, "\n")
}

// String 把单条结果转换为人类可读文本；输出总是以换行结尾，多行拼接时格式保持一致。
func (r *TypeCheckingResult) String() string {
	if r.Issues == nil && r.Err == nil {
		return ""
	}
	if r.Err != nil {
		return fmt.Sprintf("%v: type checking error: %v\n", r.GVK, r.Err)
	}
	return fmt.Sprintf("%v: %s\n", r.GVK, r.Issues)
}

// Check 对给定策略执行类型检查，并把结果格式化为可直接写入 policy.Status 的
// []ExpressionWarning；没有 warning 时返回 nil。不会修改策略对象本身，
// 调用方需要自行更新 Status。
func (c *TypeChecker) Check(policy *v1.ValidatingAdmissionPolicy) []v1.ExpressionWarning {
	ctx := c.CreateContext(policy)

	// 待返回的 warnings；不预分配容量：类型错误是少数情况，避免每次调用都分配多余内存。
	var warnings []v1.ExpressionWarning // 有意不预分配容量

	// 检查 spec.validations[*] 中的主验证表达式及其 message 表达式；
	// 注意 messageExpression 是可选的，只有非空时才检查，否则会误报空表达式错误。
	fieldRef := field.NewPath("spec", "validations")
	for i, v := range policy.Spec.Validations {
		results := c.CheckExpression(ctx, v.Expression)
		if len(results) != 0 {
			warnings = append(warnings, v1.ExpressionWarning{
				FieldRef: fieldRef.Index(i).Child("expression").String(),
				Warning:  results.String(),
			})
		}
		// 注意 MessageExpression 是可选的。
		if v.MessageExpression == "" {
			continue
		}
		results = c.CheckExpression(ctx, v.MessageExpression)
		if len(results) != 0 {
			warnings = append(warnings, v1.ExpressionWarning{
				FieldRef: fieldRef.Index(i).Child("messageExpression").String(),
				Warning:  results.String(),
			})
		}
	}

	return warnings
}

// CreateContext 从策略定义解析所有类型及其 schema 并构造上下文。
// 解析失败时采用“尽量检查”策略：跳过无法解析的 GVK，而不是让整个检查失败，
// 因为类型检查只影响 Status 中的 warning，绝不能改变策略的执行语义。
func (c *TypeChecker) CreateContext(policy *v1.ValidatingAdmissionPolicy) *TypeCheckingContext {
	ctx := new(TypeCheckingContext)
	allGvks := c.typesToCheck(policy)
	gvks := make([]schema.GroupVersionKind, 0, len(allGvks))
	declTypes := make([]*apiservercel.DeclType, 0, len(allGvks))
	for _, gvk := range allGvks {
		declType, err := c.declType(gvk)
		if err != nil {
			// 即使出错，类型检查错误也绝不能改变策略的行为。
			if !errors.Is(err, resolver.ErrSchemaNotFound) {
				// 除 ErrSchemaNotFound 外都属于内部错误。
				klog.V(2).ErrorS(err, "internal error: schema resolution failure", "gvk", gvk)
			}
			// schema 未找到或内部错误都直接跳过。
			continue
		}
		gvks = append(gvks, gvk)
		declTypes = append(declTypes, declType)
	}
	ctx.gvks = gvks
	ctx.declTypes = declTypes

	paramsGVK := c.paramsGVK(policy) // 可能为空，下游已正确处理。
	paramsDeclType, err := c.declType(paramsGVK)
	if err != nil {
		if !errors.Is(err, resolver.ErrSchemaNotFound) {
			klog.V(2).ErrorS(err, "internal error: cannot resolve schema for params", "gvk", paramsGVK)
		}
		paramsDeclType = nil
	}
	ctx.paramGVK = paramsGVK
	ctx.paramDeclType = paramsDeclType
	ctx.variables = policy.Spec.Variables
	return ctx
}

// compiler 为单个 GVK 构造一个类型检查专用的编译器；
// typeOverwrite 决定 object/params 变量使用哪个声明类型。
func (c *TypeChecker) compiler(ctx *TypeCheckingContext, typeOverwrite typeOverwrite) (*plugincel.CompositedCompiler, error) {
	envSet, err := buildEnvSet(
		/* hasParams */ !ctx.paramGVK.Empty(),
		/* hasAuthorizer */ true,
		typeOverwrite)
	if err != nil {
		return nil, err
	}
	compiler, err := plugincel.NewCompositedCompilerForTypeChecking(envSet)
	if err != nil {
		return nil, err
	}
	compiler.Compiler = &typeCheckingCompiler{typeOverwrite: typeOverwrite, compiler: compiler}
	return compiler, nil
}

// CheckExpression 在给定上下文下对单个表达式做类型检查。
// 对 context 中的每个 GVK 分别编译一次：同一个表达式在不同资源类型下
// 可能合法也可能非法，因此结果按 GVK 逐条收集。
func (c *TypeChecker) CheckExpression(ctx *TypeCheckingContext, expression string) TypeCheckingResults {
	var results TypeCheckingResults
	for i, gvk := range ctx.gvks {
		declType := ctx.declTypes[i]
		compiler, err := c.compiler(ctx, typeOverwrite{
			object: declType,
			params: ctx.paramDeclType,
		})
		if err != nil {
			utilruntime.HandleError(err)
			continue
		}
		options := plugincel.OptionalVariableDeclarations{
			HasParams:     !ctx.paramGVK.Empty(),
			HasAuthorizer: true,
		}
		compiler.CompileAndStoreVariables(convertv1beta1Variables(ctx.variables), options, environment.StoredExpressions)
		result := compiler.CompileCELExpression(celExpression(expression), options, environment.StoredExpressions)
		if err := result.Error; err != nil {
			typeCheckingResult := &TypeCheckingResult{GVK: gvk}
			// ErrorTypeInvalid 表示策略本身写错了，属于用户可见的 warning；
			// 其余错误（如内部错误）单独记录，不混入 Issues。
			if err.Type == apiservercel.ErrorTypeInvalid {
				typeCheckingResult.Issues = err
			} else {
				typeCheckingResult.Err = err
			}
			results = append(results, typeCheckingResult)
		}
	}
	return results
}

// celExpression 让裸字符串满足 ExpressionAccessor，便于复用编译器接口。
type celExpression string

// GetExpression 返回表达式的原始文本。
func (c celExpression) GetExpression() string {
	return string(c)
}

// ReturnTypes 声明表达式可能的返回类型；类型检查阶段不约束输出类型，
// 因此统一返回 Any。
func (c celExpression) ReturnTypes() []*cel.Type {
	return []*cel.Type{cel.AnyType}
}

// generateUniqueTypeName 生成带纳秒后缀的类型名，避免同一 Kind 在多个
// GVK 下解析出同名 DeclType 而互相冲突。
func generateUniqueTypeName(kind string) string {
	return fmt.Sprintf("%s%d", kind, time.Now().Nanosecond())
}

// declType 把 GVK 对应的 OpenAPI schema 转换为 CEL 声明类型；
// gvk 为空或 schema 不可用时返回 (nil, nil)，表示退化为 Dyn 类型。
func (c *TypeChecker) declType(gvk schema.GroupVersionKind) (*apiservercel.DeclType, error) {
	if gvk.Empty() {
		return nil, nil
	}
	s, err := c.SchemaResolver.ResolveSchema(gvk)
	if err != nil {
		return nil, err
	}
	declType := common.SchemaDeclType(&openapi.Schema{Schema: s}, true)
	if declType == nil {
		return nil, nil
	}
	return declType.MaybeAssignTypeName(generateUniqueTypeName(gvk.Kind)), nil
}

// paramsGVK 从 ParamKind 构造参数 GVK；未配置或 APIVersion 非法时返回空 GVK，
// 调用方据此把 params 变量退化为 Dyn 类型。
func (c *TypeChecker) paramsGVK(policy *v1.ValidatingAdmissionPolicy) schema.GroupVersionKind {
	if policy.Spec.ParamKind == nil {
		return schema.GroupVersionKind{}
	}
	gv, err := schema.ParseGroupVersion(policy.Spec.ParamKind.APIVersion)
	if err != nil {
		return schema.GroupVersionKind{}
	}
	return gv.WithKind(policy.Spec.ParamKind.Kind)
}

// typesToCheck 从策略中提取需要类型检查的 GVK 列表，结果按 Group、Version、Kind 排序。
// 只依据 MatchConstraints 中可枚举的规则计算；通配符规则被跳过，
// 因为无法为其确定有限的 GVK 集合。RESTMapper 可能因发现缓存尚未刷新而解析失败，
// 因此每个策略最多触发一次刷新重试。
func (c *TypeChecker) typesToCheck(p *v1.ValidatingAdmissionPolicy) []schema.GroupVersionKind {
	gvks := sets.New[schema.GroupVersionKind]()
	if p.Spec.MatchConstraints == nil || len(p.Spec.MatchConstraints.ResourceRules) == 0 {
		return nil
	}
	restMapperRefreshAttempted := false // 每个策略最多刷新一次 RESTMapper 并重试解析。
	for _, rule := range p.Spec.MatchConstraints.ResourceRules {
		groups := extractGroups(&rule.Rule)
		if len(groups) == 0 {
			continue
		}
		versions := extractVersions(&rule.Rule)
		if len(versions) == 0 {
			continue
		}
		resources := extractResources(&rule.Rule)
		if len(resources) == 0 {
			continue
		}
		// 对 GVR 排序，使下面的循环产生一致的结果：同样的规则总是得到相同的
		// GVK 列表与相同的截断点，warning 因此稳定可复现。
		sort.Strings(groups)
		sort.Strings(versions)
		sort.Strings(resources)
		count := 0
		for _, group := range groups {
			for _, version := range versions {
				for _, resource := range resources {
					gvr := schema.GroupVersionResource{
						Group:    group,
						Version:  version,
						Resource: resource,
					}
					resolved, err := c.RestMapper.KindsFor(gvr)
					if err != nil {
						if restMapperRefreshAttempted {
							// 每个策略最多刷新一次 RESTMapper。
							continue
						}
						c.tryRefreshRESTMapper()
						restMapperRefreshAttempted = true
						resolved, err = c.RestMapper.KindsFor(gvr)
						if err != nil {
							continue
						}
					}
					for _, r := range resolved {
						if !r.Empty() {
							gvks.Insert(r)
							count++
							// 已收集到数量上限即提前返回，保证单个策略的类型检查耗时有上界。
							if count == maxTypesToCheck {
								if gvks.Len() == 0 {
									return nil
								}
								return sortGVKList(gvks.UnsortedList())
							}
						}
					}
				}
			}
		}
	}
	if gvks.Len() == 0 {
		return nil
	}
	return sortGVKList(gvks.UnsortedList())
}

// extractGroups 提取规则中的 API 组；出现通配符则放弃整个规则，
// 因为无法枚举无限多的组来做类型检查。
func extractGroups(rule *v1.Rule) []string {
	groups := make([]string, 0, len(rule.APIGroups))
	for _, group := range rule.APIGroups {
		// 出现通配符就放弃。
		if strings.ContainsAny(group, "*") {
			return nil
		}
		groups = append(groups, group)
	}
	return groups
}

// extractVersions 提取规则中的 API 版本；出现通配符则放弃整个规则，理由同 extractGroups。
func extractVersions(rule *v1.Rule) []string {
	versions := make([]string, 0, len(rule.APIVersions))
	for _, version := range rule.APIVersions {
		if strings.ContainsAny(version, "*") {
			return nil
		}
		versions = append(versions, version)
	}
	return versions
}

// extractResources 提取规则中的资源名。这里跳过通配符与子资源（含 '/'）：
// 前者无法枚举，后者没有独立的顶层 schema。
func extractResources(rule *v1.Rule) []string {
	resources := make([]string, 0, len(rule.Resources))
	for _, resource := range rule.Resources {
		// 跳过通配符与子资源。
		if strings.ContainsAny(resource, "*/") {
			continue
		}
		resources = append(resources, resource)
	}
	return resources
}

// sortGVKList 按 Group、Version、Kind 排序并原地返回列表本身。
func sortGVKList(list []schema.GroupVersionKind) []schema.GroupVersionKind {
	sort.Slice(list, func(i, j int) bool {
		if g := strings.Compare(list[i].Group, list[j].Group); g != 0 {
			return g < 0
		}
		if v := strings.Compare(list[i].Version, list[j].Version); v != 0 {
			return v < 0
		}
		return strings.Compare(list[i].Kind, list[j].Kind) < 0
	})
	return list
}

// tryRefreshRESTMapper 在 RESTMapper 支持刷新时执行刷新。
func (c *TypeChecker) tryRefreshRESTMapper() {
	if r, ok := c.RestMapper.(meta.ResettableRESTMapper); ok {
		r.Reset()
	}
}

// buildEnvSet 组装类型检查用的 CEL 环境：namespace/request 为手写类型，
// object/oldObject/params 来自 schema 解析，authorizer 只需其类型结构。
func buildEnvSet(hasParams bool, hasAuthorizer bool, types typeOverwrite) (*environment.EnvSet, error) {
	baseEnv := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion())
	requestType := plugincel.BuildRequestType()
	namespaceType := plugincel.BuildNamespaceType()

	var varOpts []cel.EnvOption
	var declTypes []*apiservercel.DeclType

	// namespace：手写类型。
	declTypes = append(declTypes, namespaceType)
	varOpts = append(varOpts, createVariableOpts(namespaceType, plugincel.NamespaceVarName)...)

	// request：手写类型。
	declTypes = append(declTypes, requestType)
	varOpts = append(varOpts, createVariableOpts(requestType, plugincel.RequestVarName)...)

	// object 与 oldObject 使用同一类型，来自 constraints 解析；
	// 类型检查阶段无法得知对象是否已存在，因此共享。
	if types.object != nil {
		declTypes = append(declTypes, types.object)
	}
	varOpts = append(varOpts, createVariableOpts(types.object, plugincel.ObjectVarName, plugincel.OldObjectVarName)...)

	// params：由 ParamKind 定义。
	if hasParams {
		if types.params != nil {
			declTypes = append(declTypes, types.params)
		}
		varOpts = append(varOpts, createVariableOpts(types.params, plugincel.ParamsVarName)...)
	}

	// authorizer：对所有策略表达式隐式可用。
	if hasAuthorizer {
		// 只需要它的类型结构，不需要变量本身。
		varOpts = append(varOpts, cel.Variable("authorizer", library.AuthorizerType))
	}

	return baseEnv.Extend(
		environment.VersionedOptions{
			// 特性纪元实际是 1.26，但这里人为设为 1.0，因为这些选项应当始终存在。
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions:        varOpts,
			DeclTypes:         declTypes,
		},
	)
}

// createVariableOpts 生成一组 EnvOption，用于创建包含 declType 对应变量的 CEL 环境；
// declType 可以为 nil，此时变量退化为 DynType。
// 这是刻意的：schema 缺失时宁可放弃类型检查，也不能阻止策略生效。
func createVariableOpts(declType *apiservercel.DeclType, variables ...string) []cel.EnvOption {
	opts := make([]cel.EnvOption, 0, len(variables))
	t := cel.DynType
	if declType != nil {
		t = declType.CelType()
	}
	for _, v := range variables {
		opts = append(opts, cel.Variable(v, t))
	}
	return opts
}

// typeCheckingCompiler 包装标准编译器，只做类型检查而不生成/初始化程序，
// 从而避免类型检查引入运行期开销。
type typeCheckingCompiler struct {
	compiler      *plugincel.CompositedCompiler
	typeOverwrite typeOverwrite
}

// CompileCELExpression 编译给定表达式。实现与
// staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/compile.go 相同，区别在于：
// - object、oldObject、params 使用具体类型而非 Dyn
// - 编译器不强制要求输出类型
// - 编译器不初始化 program
func (c *typeCheckingCompiler) CompileCELExpression(expressionAccessor plugincel.ExpressionAccessor, options plugincel.OptionalVariableDeclarations, mode environment.Type) plugincel.CompilationResult {
	resultError := func(errorString string, errType apiservercel.ErrorType) plugincel.CompilationResult {
		return plugincel.CompilationResult{
			Error: &apiservercel.Error{
				Type:   errType,
				Detail: errorString,
			},
			ExpressionAccessor: expressionAccessor,
		}
	}
	env, err := c.compiler.Env(mode)
	if err != nil {
		return resultError(fmt.Sprintf("fail to build env: %v", err), apiservercel.ErrorTypeInternal)
	}
	ast, issues := env.Compile(expressionAccessor.GetExpression())
	// issues 非空即编译错误；把原始错误文本交给调用方，按 ErrorTypeInvalid
	// 归入用户可见的 warning。
	if issues != nil {
		return resultError(issues.String(), apiservercel.ErrorTypeInvalid)
	}
	// 类型检查器不需要 program，但输出类型仍必须设置。
	return plugincel.CompilationResult{
		OutputType: ast.OutputType(),
	}
}

// 编译期断言 typeCheckingCompiler 实现了 plugincel.Compiler。
var _ plugincel.Compiler = (*typeCheckingCompiler)(nil)
