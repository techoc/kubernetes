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

package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"

	"k8s.io/apimachinery/pkg/util/version"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/library"
	"k8s.io/apiserver/pkg/cel/mutation"
)

const (
	// 这些是暴露给 CEL 表达式的根变量名；它们同时也是 webhook 与
	// 策略作者可见的 API 契约，改名属于破坏性变更。
	ObjectVarName                    = "object"
	OldObjectVarName                 = "oldObject"
	ParamsVarName                    = "params"
	RequestVarName                   = "request"
	NamespaceVarName                 = "namespaceObject"
	AuthorizerVarName                = "authorizer"
	RequestResourceAuthorizerVarName = "authorizer.requestResource"
	VariableVarName                  = "variables"
)

// BuildRequestType generates a DeclType for AdmissionRequest. This may be replaced with a utility that
// converts the native type definition to apiservercel.DeclType once such a utility becomes available.
// The 'uid' field is omitted since it is not needed for in-process admission review.
// The 'object' and 'oldObject' fields are omitted since they are exposed as root level CEL variables.
func BuildRequestType() *apiservercel.DeclType {
	// field/fields 是局部辅助函数：DeclField 的构造参数较长，
	// 包装后可以让下面的 schema 定义接近声明式写法。
	field := func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField {
		return apiservercel.NewDeclField(name, declType, required, nil, nil)
	}
	fields := func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField {
		result := make(map[string]*apiservercel.DeclField, len(fields))
		for _, f := range fields {
			result[f.Name] = f
		}
		return result
	}
	gvkType := apiservercel.NewObjectType("kubernetes.GroupVersionKind", fields(
		field("group", apiservercel.StringType, true),
		field("version", apiservercel.StringType, true),
		field("kind", apiservercel.StringType, true),
	))
	gvrType := apiservercel.NewObjectType("kubernetes.GroupVersionResource", fields(
		field("group", apiservercel.StringType, true),
		field("version", apiservercel.StringType, true),
		field("resource", apiservercel.StringType, true),
	))
	userInfoType := apiservercel.NewObjectType("kubernetes.UserInfo", fields(
		field("username", apiservercel.StringType, false),
		field("uid", apiservercel.StringType, false),
		field("groups", apiservercel.NewListType(apiservercel.StringType, -1), false),
		field("extra", apiservercel.NewMapType(apiservercel.StringType, apiservercel.NewListType(apiservercel.StringType, -1), -1), false),
	))
	// 列表/映射的 -1 表示不限制元素个数，与 CEL 的 unbounded 语义一致。
	return apiservercel.NewObjectType("kubernetes.AdmissionRequest", fields(
		field("kind", gvkType, true),
		field("resource", gvrType, true),
		field("subResource", apiservercel.StringType, false),
		field("requestKind", gvkType, true),
		field("requestResource", gvrType, true),
		field("requestSubResource", apiservercel.StringType, false),
		field("name", apiservercel.StringType, true),
		field("namespace", apiservercel.StringType, false),
		field("operation", apiservercel.StringType, true),
		field("userInfo", userInfoType, true),
		field("dryRun", apiservercel.BoolType, false),
		field("options", apiservercel.DynType, false),
	))
}

// BuildNamespaceType generates a DeclType for Namespace.
// Certain nested fields in Namespace (e.g. managedFields, ownerReferences etc.) are omitted in the generated DeclType
// by design.
func BuildNamespaceType() *apiservercel.DeclType {
	field := func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField {
		return apiservercel.NewDeclField(name, declType, required, nil, nil)
	}
	fields := func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField {
		result := make(map[string]*apiservercel.DeclField, len(fields))
		for _, f := range fields {
			result[f.Name] = f
		}
		return result
	}

	// 只暴露策略常用的命名空间字段；managedFields、ownerReferences 等
	// 大对象被有意省略，以控制 CEL 求值成本。
	specType := apiservercel.NewObjectType("kubernetes.NamespaceSpec", fields(
		field("finalizers", apiservercel.NewListType(apiservercel.StringType, -1), true),
	))
	conditionType := apiservercel.NewObjectType("kubernetes.NamespaceCondition", fields(
		field("status", apiservercel.StringType, true),
		field("type", apiservercel.StringType, true),
		field("lastTransitionTime", apiservercel.TimestampType, true),
		field("message", apiservercel.StringType, true),
		field("reason", apiservercel.StringType, true),
	))
	statusType := apiservercel.NewObjectType("kubernetes.NamespaceStatus", fields(
		field("conditions", apiservercel.NewListType(conditionType, -1), true),
		field("phase", apiservercel.StringType, true),
	))
	metadataType := apiservercel.NewObjectType("kubernetes.NamespaceMetadata", fields(
		field("name", apiservercel.StringType, true),
		field("generateName", apiservercel.StringType, true),
		field("namespace", apiservercel.StringType, true),
		field("labels", apiservercel.NewMapType(apiservercel.StringType, apiservercel.StringType, -1), true),
		field("annotations", apiservercel.NewMapType(apiservercel.StringType, apiservercel.StringType, -1), true),
		field("UID", apiservercel.StringType, true),
		field("creationTimestamp", apiservercel.TimestampType, true),
		field("deletionGracePeriodSeconds", apiservercel.IntType, true),
		field("deletionTimestamp", apiservercel.TimestampType, true),
		field("generation", apiservercel.IntType, true),
		field("resourceVersion", apiservercel.StringType, true),
		field("finalizers", apiservercel.NewListType(apiservercel.StringType, -1), true),
	))
	return apiservercel.NewObjectType("kubernetes.Namespace", fields(
		field("metadata", metadataType, true),
		field("spec", specType, true),
		field("status", statusType, true),
	))
}

// CompilationResult represents a compiled validations expression.
// Error 与 Program 互斥：编译失败时 Program 为 nil，调用方需先检查 Error。
type CompilationResult struct {
	Program            cel.Program
	Error              *apiservercel.Error
	ExpressionAccessor ExpressionAccessor
	OutputType         *cel.Type
}

// Compiler provides a CEL expression compiler configured with the desired admission related CEL variables and
// environment mode.
//
// 实现会按 OptionalVariableDeclarations 预构建环境，避免每次编译都重新
// 组装 CEL 选项。
type Compiler interface {
	CompileCELExpression(expressionAccessor ExpressionAccessor, options OptionalVariableDeclarations, mode environment.Type) CompilationResult
}

type compiler struct {
	varEnvs variableDeclEnvs
}

// NewCompiler 基于给定环境构建编译器；environment 组合较少，构造期一次性建好。
func NewCompiler(env *environment.EnvSet) Compiler {
	return &compiler{varEnvs: mustBuildEnvs(env)}
}

// variableDeclEnvs 以可选变量声明为键缓存 CEL 环境。
type variableDeclEnvs map[OptionalVariableDeclarations]*environment.EnvSet

// CompileCELExpression returns a compiled CEL expression.
// perCallLimit was added for testing purpose only. Callers should always use const PerCallLimit from k8s.io/apiserver/pkg/apis/cel/config.go as input.
func (c compiler) CompileCELExpression(expressionAccessor ExpressionAccessor, options OptionalVariableDeclarations, envType environment.Type) CompilationResult {
	// resultError 统一构造失败结果，并保留 expressionAccessor，
	// 便于上层把错误关联回具体的表达式。
	resultError := func(errorString string, errType apiservercel.ErrorType, cause error) CompilationResult {
		return CompilationResult{
			Error: &apiservercel.Error{
				Type:   errType,
				Detail: errorString,
				Cause:  cause,
			},
			ExpressionAccessor: expressionAccessor,
		}
	}

	env, err := c.varEnvs[options].Env(envType)
	if err != nil {
		return resultError(fmt.Sprintf("unexpected error loading CEL environment: %v", err), apiservercel.ErrorTypeInternal, nil)
	}

	ast, issues := env.Compile(expressionAccessor.GetExpression())
	if issues != nil {
		return resultError("compilation failed: "+issues.String(), apiservercel.ErrorTypeInvalid, apiservercel.NewCompilationError(issues))
	}
	// 返回类型必须与访问器声明的类型之一精确匹配；cel.AnyType 作为
	// dyn 返回值同样接受。
	found := false
	returnTypes := expressionAccessor.ReturnTypes()
	for _, returnType := range returnTypes {
		if ast.OutputType().IsExactType(returnType) || cel.AnyType.IsExactType(returnType) {
			found = true
			break
		}
	}
	if !found {
		var reason string
		if len(returnTypes) == 1 {
			reason = fmt.Sprintf("must evaluate to %v but got %v", returnTypes[0].String(), ast.OutputType().String())
		} else {
			reason = fmt.Sprintf("must evaluate to one of %v but got %v", returnTypes, ast.OutputType().String())
		}

		return resultError(reason, apiservercel.ErrorTypeInvalid, nil)
	}

	// 转成 checked expression 可提前暴露类型检查层面的问题；
	// 理论上 Compile 通过后不会失败，所以归为内部错误。
	_, err = cel.AstToCheckedExpr(ast)
	if err != nil {
		// should be impossible since env.Compile returned no issues
		return resultError("unexpected compilation error: "+err.Error(), apiservercel.ErrorTypeInternal, nil)
	}
	// InterruptCheckFrequency 让长表达式可被中断，防止单个策略耗尽 CPU。
	prog, err := env.Program(ast,
		cel.InterruptCheckFrequency(celconfig.CheckFrequency),
	)
	if err != nil {
		return resultError("program instantiation failed: "+err.Error(), apiservercel.ErrorTypeInternal, nil)
	}
	return CompilationResult{
		Program:            prog,
		ExpressionAccessor: expressionAccessor,
		OutputType:         ast.OutputType(),
	}
}

// mustBuildEnvs 预构建所有变量组合的环境；组合是 2×2×2 共 8 种
// （params、authorizer、patch types）。构建失败属于编程错误，直接 panic。
func mustBuildEnvs(baseEnv *environment.EnvSet) variableDeclEnvs {
	requestType := BuildRequestType()
	namespaceType := BuildNamespaceType()
	envs := make(variableDeclEnvs, 8) // since the number of variable combinations is small, pre-build a environment for each
	for _, hasParams := range []bool{false, true} {
		for _, hasAuthorizer := range []bool{false, true} {
			var err error
			{
				decl := OptionalVariableDeclarations{HasParams: hasParams, HasAuthorizer: hasAuthorizer}
				envs[decl], err = createEnvForOpts(baseEnv, namespaceType, requestType, decl)
				if err != nil {
					panic(err)
				}
			}
			{
				decl := OptionalVariableDeclarations{HasParams: hasParams, HasAuthorizer: hasAuthorizer, HasPatchTypes: true}
				envs[decl], err = createEnvForOpts(baseEnv, namespaceType, requestType, decl)
				if err != nil {
					panic(err)
				}
			}
		}
	}
	return envs
}

// createEnvForOpts 在基础环境上扩展准入所需的根变量与类型声明。
// 先扩展通用变量，再在需要时追加 patch 相关选项，保持各组合的继承关系一致。
func createEnvForOpts(baseEnv *environment.EnvSet, namespaceType *apiservercel.DeclType, requestType *apiservercel.DeclType, opts OptionalVariableDeclarations) (*environment.EnvSet, error) {
	var envOpts []cel.EnvOption
	envOpts = append(envOpts,
		cel.Variable(ObjectVarName, cel.DynType),
		cel.Variable(OldObjectVarName, cel.DynType),
		cel.Variable(NamespaceVarName, namespaceType.CelType()),
		cel.Variable(RequestVarName, requestType.CelType()))
	if opts.HasParams {
		envOpts = append(envOpts, cel.Variable(ParamsVarName, cel.DynType))
	}
	if opts.HasAuthorizer {
		envOpts = append(envOpts,
			cel.Variable(AuthorizerVarName, library.AuthorizerType),
			cel.Variable(RequestResourceAuthorizerVarName, library.ResourceCheckType))
	}

	extended, err := baseEnv.Extend(
		environment.VersionedOptions{
			// Feature epoch was actually 1.26, but we artificially set it to 1.0 because these
			// options should always be present.
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions:        envOpts,
			DeclTypes: []*apiservercel.DeclType{
				namespaceType,
				requestType,
			},
		},
		environment.StrictCostOpt,
	)
	if err != nil {
		return nil, fmt.Errorf("environment misconfigured: %w", err)
	}

	if opts.HasPatchTypes {
		extended, err = extended.Extend(hasPatchTypes)
		if err != nil {
			return nil, fmt.Errorf("environment misconfigured: %w", err)
		}
	}
	return extended, nil
}

// hasPatchTypes 提供 JSONPatch 库与动态类型解析器，供 mutating 策略
// 在 CEL 中构造 patch 使用。IntroducedVersion 被刻意设为 1.0，
// 使其不受特性门控影响、始终可用。
var hasPatchTypes = environment.VersionedOptions{
	// Feature epoch was actually 1.32, but we artificially set it to 1.0 because these
	// options should always be present.
	IntroducedVersion: version.MajorMinor(1, 0),
	EnvOptions: []cel.EnvOption{
		common.ResolverEnvOption(&mutation.DynamicTypeResolver{}),
		environment.UnversionedLib(library.JSONPatch), // for jsonPatch.escape() function
	},
}
