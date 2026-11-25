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
	"context"
	"math"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/admission"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/lazy"
)

const variablesTypeName = "kubernetes.variables"

// CompositedCompiler 编译带有变量组合的表达式。
//
// 变量组合（variable composition）把多个 CEL 变量声明合并到一个合成对象中，
// 使策略可以引用其他策略/参数中定义的变量，同时保持各自的类型与编译结果。
type CompositedCompiler struct {
	Compiler
	ConditionCompiler
	MutatingCompiler

	state *compositionState
}

// CompositedConditionEvaluator 提供带变量组合的条件表达式的求值。
// 表达式必须返回布尔值。
type CompositedConditionEvaluator struct {
	ConditionEvaluator

	state *compositionState
}

// CompositedEvaluator 提供带变量组合的单个表达式的求值。
// 表达式可返回的类型在编译期确定。
type CompositedEvaluator struct {
	MutatingEvaluator

	state *compositionState
}

// NewCompositedCompiler 在给定基础环境上扩展出 variables 变量并返回编译器。
//
// 新增的 kubernetes.variables 对象类型初始为空，随着变量被编译逐步添加字段，
// 这样后续表达式就能以类型安全的方式引用已声明的变量。
func NewCompositedCompiler(envSet *environment.EnvSet) (*CompositedCompiler, error) {
	newMapType := apiservercel.NewObjectType(variablesTypeName, map[string]*apiservercel.DeclField{})

	newEnvSet, err := envSet.Extend(environment.VersionedOptions{
		IntroducedVersion: version.MajorMinor(1, 0),
		EnvOptions: []cel.EnvOption{
			cel.Variable("variables", newMapType.CelType()),
		},
		DeclTypes: []*apiservercel.DeclType{
			newMapType,
		},
	})
	if err != nil {
		return nil, err
	}

	state := &compositionState{
		mapType:           newMapType,
		EnvSet:            newEnvSet,
		compiledVariables: map[string]CompilationResult{},
	}

	compiler := NewCompiler(state.EnvSet)
	conditionCompiler := &conditionCompiler{compiler}
	mutation := &mutatingCompiler{compiler}
	return &CompositedCompiler{
		Compiler:          compiler,
		ConditionCompiler: conditionCompiler,
		MutatingCompiler:  mutation,
		state:             state,
	}, nil
}

// NewCompositedCompilerForTypeChecking 用于类型检查场景创建 CompositedCompiler。
// 它会初始化组合状态，但把 Compilers 留空，因为预期由调用方
// （即执行类型检查的一方）替换它们。
func NewCompositedCompilerForTypeChecking(envSet *environment.EnvSet) (*CompositedCompiler, error) {
	newMapType := apiservercel.NewObjectType(variablesTypeName, map[string]*apiservercel.DeclField{})

	newEnvSet, err := envSet.Extend(environment.VersionedOptions{
		IntroducedVersion: version.MajorMinor(1, 0),
		EnvOptions: []cel.EnvOption{
			cel.Variable("variables", newMapType.CelType()),
		},
		DeclTypes: []*apiservercel.DeclType{
			newMapType,
		},
	})
	if err != nil {
		return nil, err
	}

	state := &compositionState{
		mapType:           newMapType,
		EnvSet:            newEnvSet,
		compiledVariables: map[string]CompilationResult{},
	}
	return &CompositedCompiler{
		state: state,
	}, nil
}

// Env 返回给定 mode 对应的 CEL 环境。
func (c *CompositedCompiler) Env(mode environment.Type) (*cel.Env, error) {
	return c.state.Env(mode)
}

// CreateContext 创建一个带组合状态的上下文，供求值时惰性求值变量。
func (c *CompositedCompiler) CreateContext(parent context.Context) CompositionContext {
	return c.state.CreateContext(parent)
}

// CompileAndStoreVariables 批量编译变量并登记其类型；单个变量编译失败不会
// 中断其余变量，错误保留在各自的结果中，在求值时再暴露。
func (c *CompositedCompiler) CompileAndStoreVariables(variables []NamedExpressionAccessor, options OptionalVariableDeclarations, mode environment.Type) {
	for _, v := range variables {
		_ = c.CompileAndStoreVariable(v, options, mode)
	}
}

// CompileAndStoreVariable 编译单个变量，把其输出类型加入 variables 对象，并
// 记录编译结果以便后续求值。
func (c *CompositedCompiler) CompileAndStoreVariable(variable NamedExpressionAccessor, options OptionalVariableDeclarations, mode environment.Type) CompilationResult {
	result := c.Compiler.CompileCELExpression(variable, options, mode)
	c.state.AddField(variable.GetName(), result.OutputType)
	c.state.compiledVariables[variable.GetName()] = result
	return result
}

// CompileCondition 编译条件表达式，并包装成带组合状态的求值器。
func (c *CompositedCompiler) CompileCondition(expressions []ExpressionAccessor, optionalDecls OptionalVariableDeclarations, envType environment.Type) ConditionEvaluator {
	condition := c.ConditionCompiler.CompileCondition(expressions, optionalDecls, envType)
	return &CompositedConditionEvaluator{
		ConditionEvaluator: condition,
		state:              c.state,
	}
}

// CompileEvaluator 为给定的表达式、选项与环境编译一个 mutatingEvaluator。
func (c *CompositedCompiler) CompileMutatingEvaluator(expression ExpressionAccessor, optionalDecls OptionalVariableDeclarations, envType environment.Type) MutatingEvaluator {
	mutation := c.MutatingCompiler.CompileMutatingEvaluator(expression, optionalDecls, envType)
	return &CompositedEvaluator{
		MutatingEvaluator: mutation,
		state:             c.state,
	}
}

// compositionState 保存合成变量对象类型与各变量的编译结果，被编译器与求值器共享。
type compositionState struct {
	*environment.EnvSet

	mapType           *apiservercel.DeclType
	compiledVariables map[string]CompilationResult
}

// AddField 把变量名与类型加入合成对象，使后续表达式能引用它。
func (c *compositionState) AddField(name string, celType *cel.Type) {
	c.mapType.Fields[name] = apiservercel.NewDeclField(name, convertCelTypeToDeclType(celType), true, nil, nil)
}

// CreateContext 创建求值上下文。
func (c *compositionState) CreateContext(parent context.Context) CompositionContext {
	return &compositionContext{
		Context: parent,
		state:   c,
	}
}

// CompositionContext 在普通 context 之上提供变量绑定与成本累计能力。
type CompositionContext interface {
	context.Context
	Variables(activation any) ref.Val
	GetAndResetCost() int64
}

type compositionContext struct {
	context.Context

	state           *compositionState
	accumulatedCost int64
}

// Variables 构造惰性求值的 variables 映射：每个变量在被访问时才执行其 CEL
// 程序，未被引用的变量不会产生开销。
func (c *compositionContext) Variables(activation any) ref.Val {
	lazyMap := lazy.NewMapValue(c.state.mapType)
	for name, result := range c.state.compiledVariables {
		accessor := &variableAccessor{
			name:       name,
			result:     result,
			activation: activation,
			context:    c,
		}
		lazyMap.Append(name, accessor.Callback)
	}
	return lazyMap
}

// ForInput 为求值创建组合上下文，使条件表达式能够访问变量。
func (f *CompositedConditionEvaluator) ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *v1.AdmissionRequest, optionalVars OptionalVariableBindings, namespace *corev1.Namespace, runtimeCELCostBudget int64) ([]EvaluationResult, int64, error) {
	ctx = f.state.CreateContext(ctx)
	return f.ConditionEvaluator.ForInput(ctx, versionedAttr, request, optionalVars, namespace, runtimeCELCostBudget)
}

func (c *compositionContext) reportCost(cost int64) {
	c.accumulatedCost += cost
}

// GetAndResetCost 返回自上次调用以来累计的变量求值成本并清零。
//
// 变量求值成本需要计入调用方表达式的总成本预算，因此单独累计并在每个表达式
// 求值前取走。
func (c *compositionContext) GetAndResetCost() int64 {
	cost := c.accumulatedCost
	c.accumulatedCost = 0
	return cost
}

// variableAccessor 是惰性映射中单个变量的回调载体。
type variableAccessor struct {
	name       string
	result     CompilationResult
	activation any
	context    *compositionContext
}

// Callback 求值变量并把其成本累加到上下文；编译或求值失败以 CEL 错误值返回，
// 由引用它的表达式决定如何传播。
func (a *variableAccessor) Callback(_ *lazy.MapValue) ref.Val {
	if a.result.Error != nil {
		return types.NewErr("composited variable %q fails to compile: %v", a.name, a.result.Error)
	}

	v, details, err := a.result.Program.ContextEval(a.context, a.activation)
	if details == nil {
		return types.NewErr("unable to get evaluation details of variable %q", a.name)
	}
	costPtr := details.ActualCost()
	if costPtr == nil {
		return types.NewErr("unable to calculate cost of variable %q", a.name)
	}
	cost := int64(*costPtr)
	// 成本以 uint64 表示，转 int64 前先钳制，避免溢出成负数而绕过预算。
	if *costPtr > math.MaxInt64 {
		cost = math.MaxInt64
	}
	a.context.reportCost(cost)

	if err != nil {
		return types.NewErr("composited variable %q fails to evaluate: %v", a.name, err)
	}
	return v
}

// convertCelTypeToDeclType 将 cel.Type 转换为 DeclType，供
// TypeProvider 与成本估算器使用。
// List 与 map 类型按需创建，其参数递归转换。
//
// 无法识别的类型退化为 Dyn，保证类型转换不会因为新类型而失败。
func convertCelTypeToDeclType(celType *cel.Type) *apiservercel.DeclType {
	if celType == nil {
		return apiservercel.DynType
	}
	switch celType {
	case cel.AnyType:
		return apiservercel.AnyType
	case cel.BoolType:
		return apiservercel.BoolType
	case cel.BytesType:
		return apiservercel.BytesType
	case cel.DoubleType:
		return apiservercel.DoubleType
	case cel.DurationType:
		return apiservercel.DurationType
	case cel.IntType:
		return apiservercel.IntType
	case cel.NullType:
		return apiservercel.NullType
	case cel.StringType:
		return apiservercel.StringType
	case cel.TimestampType:
		return apiservercel.TimestampType
	case cel.UintType:
		return apiservercel.UintType
	default:
		// 容器类型（list/map）通过 trait 识别，并按参数个数递归构造。
		if celType.HasTrait(traits.ContainerType) && celType.HasTrait(traits.IndexerType) {
			parameters := celType.Parameters()
			switch len(parameters) {
			case 1:
				elemType := convertCelTypeToDeclType(parameters[0])
				return apiservercel.NewListType(elemType, -1)
			case 2:
				keyType := convertCelTypeToDeclType(parameters[0])
				valueType := convertCelTypeToDeclType(parameters[1])
				return apiservercel.NewMapType(keyType, valueType, -1)
			}
		}
		return apiservercel.DynType
	}
}
