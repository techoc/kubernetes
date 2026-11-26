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

package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types/ref"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/util/version"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// CEL 表达式中暴露给用户的变量名。这些名字属于 API 契约，改名会破坏现有 authorizer 配置。
const (
	subjectAccessReviewRequestVarName = "request"

	fieldSelectorVarName = "fieldSelector"
	labelSelectorVarName = "labelSelector"
)

// CompilationResult 表示已编译的授权 CEL 表达式
type CompilationResult struct {
	Program            cel.Program
	ExpressionAccessor ExpressionAccessor

	// 记录表达式是否使用 fieldSelector 与 labelSelector，
	// 未使用时可跳过相应数据的构造，减少求值开销。
	UsesFieldSelector bool
	UsesLabelSelector bool
}

// EvaluationResult 包含 CEL 求值所需的最小字段与元数据
type EvaluationResult struct {
	EvalResult         ref.Val
	ExpressionAccessor ExpressionAccessor
}

// Compiler 是按指定环境模式编译 CEL 表达式的接口
type Compiler interface {
	CompileCELExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error)
}

// compiler 持有已按兼容版本扩展过的 CEL 环境集合。
type compiler struct {
	envSet *environment.EnvSet
}

// NewDefaultCompiler 按默认兼容版本返回一个 Compiler。
// 注意：构造 Compiler 依赖已初始化的特性门控与兼容版本。
func NewDefaultCompiler() Compiler {
	return NewCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
}

// NewCompiler 返回一个 Compiler。
//
// 通过 mustBuildEnv 注入 request 变量声明；构建失败属于编程错误，直接 panic。
func NewCompiler(env *environment.EnvSet) Compiler {
	return &compiler{
		envSet: mustBuildEnv(env),
	}
}

// CompileCELExpression 编译单个条件表达式并校验返回类型。
// 同时静态扫描 AST 判断是否引用了 fieldSelector/labelSelector，供求值时跳过无用转换。
func (c compiler) CompileCELExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error) {
	// 统一构造错误结果，始终带回 accessor 以便调用方定位是哪个表达式出错。
	resultError := func(errorString string, errType apiservercel.ErrorType) (CompilationResult, error) {
		err := &apiservercel.Error{
			Type:   errType,
			Detail: errorString,
		}
		return CompilationResult{
			ExpressionAccessor: expressionAccessor,
		}, err
	}
	env, err := c.envSet.Env(environment.StoredExpressions)
	if err != nil {
		return resultError(fmt.Sprintf("unexpected error loading CEL environment: %v", err), apiservercel.ErrorTypeInternal)
	}
	ast, issues := env.Compile(expressionAccessor.GetExpression())
	if issues != nil {
		return resultError("compilation failed: "+issues.String(), apiservercel.ErrorTypeInvalid)
	}
	found := false
	returnTypes := expressionAccessor.ReturnTypes()
	for _, returnType := range returnTypes {
		if ast.OutputType() == returnType {
			found = true
			break
		}
	}
	// 返回类型不符属于用户配置错误（Invalid），而非内部错误。
	if !found {
		var reason string
		if len(returnTypes) == 1 {
			reason = fmt.Sprintf("must evaluate to %v but got %v", returnTypes[0].String(), ast.OutputType())
		} else {
			reason = fmt.Sprintf("must evaluate to one of %v", returnTypes)
		}

		return resultError(reason, apiservercel.ErrorTypeInvalid)
	}
	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		// env.Compile 未报错，这里不应失败
		return resultError("unexpected compilation error: "+err.Error(), apiservercel.ErrorTypeInternal)
	}
	celAST, err := celast.ToAST(checkedExpr)
	if err != nil {
		// env.Compile 未报错，这里不应失败
		return resultError("unexpected compilation error: "+err.Error(), apiservercel.ErrorTypeInternal)
	}

	var usesFieldSelector, usesLabelSelector bool
	celast.PreOrderVisit(celast.NavigateAST(celAST), celast.NewExprVisitor(func(e celast.Expr) {
		// 已知两者都用到了，无需继续检查
		if usesFieldSelector && usesLabelSelector {
			return
		}

		var fieldName string
		switch e.Kind() {
		case celast.SelectKind:
			// 普通选择（.fieldSelector / .labelSelector）
			fieldName = e.AsSelect().FieldName()
		case celast.CallKind:
			// 可选选择（.?fieldSelector / .?labelSelector）
			if e.AsCall().FunctionName() != operators.OptSelect {
				return
			}
			args := e.AsCall().Args()
			// args[0] 是接收者（`.?` 前的部分），args[1] 是被可选选择的字段名（`.?` 后的部分）
			if len(args) != 2 || args[1].Kind() != celast.LiteralKind || args[1].AsLiteral().Type() != cel.StringType {
				return
			}
			fieldName, _ = args[1].AsLiteral().Value().(string)
		}

		switch fieldName {
		case fieldSelectorVarName:
			usesFieldSelector = true
		case labelSelectorVarName:
			usesLabelSelector = true
		}
	}))

	prog, err := env.Program(ast)
	if err != nil {
		return resultError("program instantiation failed: "+err.Error(), apiservercel.ErrorTypeInternal)
	}
	return CompilationResult{
		Program:            prog,
		ExpressionAccessor: expressionAccessor,
		UsesFieldSelector:  usesFieldSelector,
		UsesLabelSelector:  usesLabelSelector,
	}, nil
}

// mustBuildEnv 在基础环境上扩展出 request 变量及其类型声明。
// 构建失败说明声明有误，属于启动期不可恢复错误，故 panic。
func mustBuildEnv(baseEnv *environment.EnvSet) *environment.EnvSet {
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
	subjectAccessReviewSpecRequestType := buildRequestType(field, fields)
	extended, err := baseEnv.Extend(
		environment.VersionedOptions{
			// 记为 1.0，因为该功能在
			// 首个支持它的版本中就已提供
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions: []cel.EnvOption{
				cel.Variable(subjectAccessReviewRequestVarName, subjectAccessReviewSpecRequestType.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				subjectAccessReviewSpecRequestType,
			},
		},
	)
	if err != nil {
		panic(fmt.Sprintf("environment misconfigured: %v", err))
	}

	return extended
}

// buildRequestType 为 SubjectAccessReviewSpec 生成 DeclType。
// 此处新增属性时，也必须同步到 convertObjectToUnstructured。
func buildRequestType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	resourceAttributesType := buildResourceAttributesType(field, fields)
	nonResourceAttributesType := buildNonResourceAttributesType(field, fields)
	return apiservercel.NewObjectType("kubernetes.SubjectAccessReviewSpec", fields(
		field("resourceAttributes", resourceAttributesType, false),
		field("nonResourceAttributes", nonResourceAttributesType, false),
		field("user", apiservercel.StringType, false),
		field("groups", apiservercel.NewListType(apiservercel.StringType, -1), false),
		field("extra", apiservercel.NewMapType(apiservercel.StringType, apiservercel.NewListType(apiservercel.StringType, -1), -1), false),
		field("uid", apiservercel.StringType, false),
	))
}

// buildResourceAttributesType 为 ResourceAttributes 生成 DeclType。
// 此处新增属性时，也必须同步到 convertObjectToUnstructured。
func buildResourceAttributesType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	resourceAttributesFields := []*apiservercel.DeclField{
		field("namespace", apiservercel.StringType, false),
		field("verb", apiservercel.StringType, false),
		field("group", apiservercel.StringType, false),
		field("version", apiservercel.StringType, false),
		field("resource", apiservercel.StringType, false),
		field("subresource", apiservercel.StringType, false),
		field("name", apiservercel.StringType, false),
	}
	resourceAttributesFields = append(resourceAttributesFields, field("fieldSelector", buildFieldSelectorType(field, fields), false))
	resourceAttributesFields = append(resourceAttributesFields, field("labelSelector", buildLabelSelectorType(field, fields), false))
	return apiservercel.NewObjectType("kubernetes.ResourceAttributes", fields(resourceAttributesFields...))
}

// buildFieldSelectorType 声明 fieldSelector 的 CEL 类型：rawSelector 与结构化 requirements 二选一。
func buildFieldSelectorType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	return apiservercel.NewObjectType("kubernetes.FieldSelectorAttributes", fields(
		field("rawSelector", apiservercel.StringType, false),
		field("requirements", apiservercel.NewListType(buildSelectorRequirementType(field, fields), -1), false),
	))
}

// buildLabelSelectorType 声明 labelSelector 的 CEL 类型，结构与 fieldSelector 相同。
func buildLabelSelectorType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	return apiservercel.NewObjectType("kubernetes.LabelSelectorAttributes", fields(
		field("rawSelector", apiservercel.StringType, false),
		field("requirements", apiservercel.NewListType(buildSelectorRequirementType(field, fields), -1), false),
	))
}

// buildSelectorRequirementType 声明选择器条目的 CEL 类型（key/operator/values）。
func buildSelectorRequirementType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	return apiservercel.NewObjectType("kubernetes.SelectorRequirement", fields(
		field("key", apiservercel.StringType, false),
		field("operator", apiservercel.StringType, false),
		field("values", apiservercel.NewListType(apiservercel.StringType, -1), false),
	))
}

// buildNonResourceAttributesType 为 NonResourceAttributes 生成 DeclType。
// 此处新增属性时，也必须同步到 convertObjectToUnstructured。
func buildNonResourceAttributesType(field func(name string, declType *apiservercel.DeclType, required bool) *apiservercel.DeclField, fields func(fields ...*apiservercel.DeclField) map[string]*apiservercel.DeclField) *apiservercel.DeclType {
	return apiservercel.NewObjectType("kubernetes.NonResourceAttributes", fields(
		field("path", apiservercel.StringType, false),
		field("verb", apiservercel.StringType, false),
	))
}

// convertObjectToUnstructured 把 SubjectAccessReviewSpec 转成 CEL 可直接访问的 map。
//
// 必须与上面的 DeclType 声明逐字段保持一致：声明里有而这里缺失的字段在求值时会报“no such key”，
// 反之则会暴露未声明的数据。includeFieldSelector/includeLabelSelector 为 false 时跳过
// 相应选择器的转换，这是一条纯粹的性能优化路径。
func convertObjectToUnstructured(obj *authorizationv1.SubjectAccessReviewSpec, includeFieldSelector, includeLabelSelector bool) map[string]interface{} {
	// 构造包含 SubjectAccessReview 所有用户与字符串属性字段的版本，即使字段为 omitempty 也保留，供 CEL 求值
	//
	// extra 为 nil 时替换为空 map，保证表达式里 .extra 始终可访问而不会因 null 报错。
	extra := obj.Extra
	if extra == nil {
		extra = map[string]authorizationv1.ExtraValue{}
	}
	ret := map[string]interface{}{
		"user":   obj.User,
		"groups": obj.Groups,
		"uid":    string(obj.UID),
		"extra":  extra,
	}
	if obj.ResourceAttributes != nil {
		resourceAttributes := map[string]interface{}{
			"namespace":   obj.ResourceAttributes.Namespace,
			"verb":        obj.ResourceAttributes.Verb,
			"group":       obj.ResourceAttributes.Group,
			"version":     obj.ResourceAttributes.Version,
			"resource":    obj.ResourceAttributes.Resource,
			"subresource": obj.ResourceAttributes.Subresource,
			"name":        obj.ResourceAttributes.Name,
		}

		if includeFieldSelector && obj.ResourceAttributes.FieldSelector != nil {
			// 结构化 requirements 与原始 rawSelector 互斥，后写入者生效（与 API 语义一致）。
			if len(obj.ResourceAttributes.FieldSelector.Requirements) > 0 {
				requirements := make([]map[string]interface{}, 0, len(obj.ResourceAttributes.FieldSelector.Requirements))
				for _, r := range obj.ResourceAttributes.FieldSelector.Requirements {
					requirements = append(requirements, map[string]interface{}{
						"key":      r.Key,
						"operator": r.Operator,
						"values":   r.Values,
					})
				}
				resourceAttributes[fieldSelectorVarName] = map[string]interface{}{"requirements": requirements}
			}
			if len(obj.ResourceAttributes.FieldSelector.RawSelector) > 0 {
				resourceAttributes[fieldSelectorVarName] = map[string]interface{}{"rawSelector": obj.ResourceAttributes.FieldSelector.RawSelector}
			}
		}

		if includeLabelSelector && obj.ResourceAttributes.LabelSelector != nil {
			if len(obj.ResourceAttributes.LabelSelector.Requirements) > 0 {
				requirements := make([]map[string]interface{}, 0, len(obj.ResourceAttributes.LabelSelector.Requirements))
				for _, r := range obj.ResourceAttributes.LabelSelector.Requirements {
					requirements = append(requirements, map[string]interface{}{
						"key":      r.Key,
						"operator": r.Operator,
						"values":   r.Values,
					})
				}
				resourceAttributes[labelSelectorVarName] = map[string]interface{}{"requirements": requirements}
			}
			if len(obj.ResourceAttributes.LabelSelector.RawSelector) > 0 {
				resourceAttributes[labelSelectorVarName] = map[string]interface{}{"rawSelector": obj.ResourceAttributes.LabelSelector.RawSelector}
			}
		}

		ret["resourceAttributes"] = resourceAttributes
	}
	if obj.NonResourceAttributes != nil {
		ret["nonResourceAttributes"] = map[string]string{
			"verb": obj.NonResourceAttributes.Verb,
			"path": obj.NonResourceAttributes.Path,
		}
	}
	return ret
}
