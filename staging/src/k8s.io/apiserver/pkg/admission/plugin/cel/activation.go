/*
Copyright 2024 The Kubernetes Authors.

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
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/cel-go/interpreter"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/library"
)

// activationOptions holds options for newActivation.
// 用函数式选项而非新增参数，避免调用点随选项增加而频繁改动。
type activationOptions struct {
	// useSchemalessTypeRef 为 true 时，对象使用无 schema 的类型引用（schemaless）来求值，
	// 否则沿用已注册的 OpenAPI schema。
	useSchemalessTypeRef bool
}

// activationOption defines a functional option for newActivation.
// 函数式选项：每次调用都新建一个闭包，保证选项之间互不影响。
type activationOption func(*activationOptions)

// useSchemalessTypeRef returns an activationOption that configures the activation to use SchemalessTypeRef on the versioned attributes.
// 仅用于 admission 策略插件等尚未切换到无 schema 求值的调用方。
func useSchemalessTypeRef() activationOption {
	return func(o *activationOptions) {
		o.useSchemalessTypeRef = true
	}
}

// newActivation creates an activation for CEL admission plugins from the given request, admission chain and
// variable binding information.
// 返回的 activation 在单次请求的整个表达式求值过程中复用，因此其中的值在构建后必须视为只读。
func newActivation(compositionCtx CompositionContext, versionedAttr *admission.VersionedAttributes, request *admissionv1.AdmissionRequest, inputs OptionalVariableBindings, namespace *v1.Namespace, options ...activationOption) (*evaluationActivation, error) {
	// 先应用全部选项，再根据选项决定如何准备对象。
	opts := &activationOptions{}
	for _, o := range options {
		o(opts)
	}
	if opts.useSchemalessTypeRef && versionedAttr != nil {
		// 在读取 CELValue 之前设置标志，确保后续取到的对象使用无 schema 表示。
		// SA1019: admission.LazyObject.UseSchemalessTypeRef is deprecated: This field is temporary and will be removed when schemaless becomes the default.
		// SA1019: admission.LazyObject.UseSchemalessTypeRef is deprecated: This field is temporary and will be removed when schemaless becomes the default.
		//nolint:staticcheck
		versionedAttr.VersionedObject.UseSchemalessTypeRef = true
		//nolint:staticcheck
		versionedAttr.VersionedOldObject.UseSchemalessTypeRef = true
	}

	// 旧对象在 CREATE 时为空，CELValue 需能容忍这种情况（返回 null 值）。
	celOldObjVal, err := versionedAttr.VersionedOldObject.CELValue()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare oldObject variable for evaluation: %w", err)
	}

	celObjVal, err := versionedAttr.VersionedObject.CELValue()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare object variable for evaluation: %w", err)
	}

	// params/authorizer 都是可选变量：未提供时保持 nil，ResolveName 会据此报告变量不存在。
	var paramsVal, authorizerVal, requestResourceAuthorizerVal any
	if inputs.VersionedParams != nil {
		paramsVal = common.SchemalessTypedToVal(inputs.VersionedParams)
	}

	if inputs.Authorizer != nil {
		// 授权检查以请求发起者的身份执行，而不是以 apiserver 自身身份，避免越权。
		authorizerVal = library.NewAuthorizerVal(versionedAttr.GetUserInfo(), inputs.Authorizer)
		requestResourceAuthorizerVal = library.NewResourceAuthorizerVal(versionedAttr.GetUserInfo(), inputs.Authorizer, versionedAttr)
	}

	// request 与 namespace 总是以无 schema 形式暴露给表达式，保证 CEL 看到的字段稳定。
	requestVal := common.SchemalessTypedToVal(request)
	namespaceVal := common.SchemalessTypedToVal(namespace)
	va := &evaluationActivation{
		object:                    celObjVal,
		oldObject:                 celOldObjVal,
		params:                    paramsVal,
		request:                   requestVal,
		namespace:                 namespaceVal,
		authorizer:                authorizerVal,
		requestResourceAuthorizer: requestResourceAuthorizerVal,
	}

	// composition is an optional feature that only applies for ValidatingAdmissionPolicy and MutatingAdmissionPolicy.
	// 变量在这里惰性求值，因此真正的开销要等到表达式引用它们时才会产生。
	if compositionCtx != nil {
		va.variables = compositionCtx.Variables(va)
	}
	return va, nil
}

// evaluationActivation 是 cel-go 求值器看到的变量集合。
// 所有字段在构建完成后只读；evaluationActivation 本身不是并发安全的，
// 因为同一请求内的表达式串行求值。
type evaluationActivation struct {
	object, oldObject, params, request, namespace, authorizer, requestResourceAuthorizer, variables interface{}
}

// ResolveName returns a value from the activation by qualified name, or false if the name
// could not be found.
// 返回 false 让 cel-go 报 "no such attribute"，而不是静默得到 null；
// params 例外——它在未绑定时显式返回 nil，以符合 CEL 变量语义。
func (a *evaluationActivation) ResolveName(name string) (interface{}, bool) {
	switch name {
	case ObjectVarName:
		return a.object, true
	case OldObjectVarName:
		return a.oldObject, true
	case ParamsVarName:
		return a.params, true // params may be null
	case RequestVarName:
		return a.request, true
	case NamespaceVarName:
		return a.namespace, true
	case AuthorizerVarName:
		// 未配置 authorizer 时视为变量不存在，避免策略误以为可以调用 authorizer()。
		return a.authorizer, a.authorizer != nil
	case RequestResourceAuthorizerVarName:
		return a.requestResourceAuthorizer, a.requestResourceAuthorizer != nil
	case VariableVarName: // variables always present
		return a.variables, true
	default:
		return nil, false
	}
}

// Parent returns the parent of the current activation, may be nil.
// If non-nil, the parent will be searched during resolve calls.
// 策略插件使用扁平命名空间，不需要父激活链。
func (a *evaluationActivation) Parent() interpreter.Activation {
	return nil
}

// Evaluate runs a compiled CEL admission plugin expression using the provided activation and CEL
// runtime cost budget.
// 返回值中的 remainingBudget 为 -1 表示预算已耗尽或无法计算，调用方应停止后续规则。
// 返回的 error 仅在成本核算出现不可恢复问题时非 nil；表达式自身的错误记录在 EvaluationResult.Error 中。
func (a *evaluationActivation) Evaluate(ctx context.Context, compositionCtx CompositionContext, compilationResult CompilationResult, remainingBudget int64) (EvaluationResult, int64, error) {
	var evaluation = EvaluationResult{}
	if compilationResult.ExpressionAccessor == nil { // in case of placeholder
		// 占位表达式（例如仅用于声明匹配条件）不产生任何开销。
		return evaluation, remainingBudget, nil
	}

	evaluation.ExpressionAccessor = compilationResult.ExpressionAccessor
	if compilationResult.Error != nil {
		// 编译错误按 invalid 处理：这是策略作者的问题，不是 apiserver 内部故障。
		evaluation.Error = &cel.Error{
			Type:   cel.ErrorTypeInvalid,
			Detail: fmt.Sprintf("compilation error: %v", compilationResult.Error),
			Cause:  compilationResult.Error,
		}
		return evaluation, remainingBudget, nil
	}
	if compilationResult.Program == nil {
		// 编译成功却没有程序，说明内部状态不一致。
		evaluation.Error = &cel.Error{
			Type:   cel.ErrorTypeInternal,
			Detail: "unexpected internal error compiling expression",
		}
		return evaluation, remainingBudget, nil
	}
	t1 := time.Now()
	evalResult, evalDetails, err := compilationResult.Program.ContextEval(ctx, a)
	// budget may be spent due to lazy evaluation of composited variables
	// 组合变量在表达式引用时才求值，其成本必须从同一预算中扣除，防止绕过成本限制。
	if compositionCtx != nil {
		compositionCost := compositionCtx.GetAndResetCost()
		if compositionCost > remainingBudget {
			return evaluation, -1, &cel.Error{
				Type:   cel.ErrorTypeInvalid,
				Detail: "validation failed due to running out of cost budget, no further validation rules will be run",
				Cause:  cel.ErrOutOfBudget,
			}
		}
		remainingBudget -= compositionCost
	}
	elapsed := time.Since(t1)
	evaluation.Elapsed = elapsed
	if evalDetails == nil {
		// 无法取得成本明细时保守地终止，避免不受限制的求值。
		return evaluation, -1, &cel.Error{
			Type:   cel.ErrorTypeInternal,
			Detail: fmt.Sprintf("runtime cost could not be calculated for expression: %v, no further expression will be run", compilationResult.ExpressionAccessor.GetExpression()),
		}
	} else {
		rtCost := evalDetails.ActualCost()
		if rtCost == nil {
			// 成本未知等同于超出预算：宁可拒绝也不放任。
			return evaluation, -1, &cel.Error{
				Type:   cel.ErrorTypeInvalid,
				Detail: fmt.Sprintf("runtime cost could not be calculated for expression: %v, no further expression will be run", compilationResult.ExpressionAccessor.GetExpression()),
				Cause:  cel.ErrOutOfBudget,
			}
		} else {
			// 先比较 MaxInt64 再转换，避免 uint64 溢出成负数绕过预算检查。
			if *rtCost > math.MaxInt64 || int64(*rtCost) > remainingBudget {
				return evaluation, -1, &cel.Error{
					Type:   cel.ErrorTypeInvalid,
					Detail: "validation failed due to running out of cost budget, no further validation rules will be run",
					Cause:  cel.ErrOutOfBudget,
				}
			}
			remainingBudget -= int64(*rtCost)
		}
	}
	if err != nil {
		// 运行时错误不终止整个请求，仅让当前规则失败，其余规则仍可继续。
		evaluation.Error = &cel.Error{
			Type:   cel.ErrorTypeInvalid,
			Detail: fmt.Sprintf("expression '%v' resulted in error: %v", compilationResult.ExpressionAccessor.GetExpression(), err),
		}
	} else {
		evaluation.EvalResult = evalResult
	}
	return evaluation, remainingBudget, nil
}
