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

// Package cel 提供准入插件（校验、变异与策略）编译和求值 CEL 表达式的共享抽象。
// 它刻意不了解具体的策略资源：调用方把自己的类型适配为 ExpressionAccessor，
// 本包只处理编译结果与求值的输入/输出。
package cel

import (
	"context"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"

	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/cel/environment"
)

// ExpressionAccessor 是编译器所需的最小 CEL 表达式视图。
// 由各个准入插件提供实现，使本包不依赖任何具体的策略 API 类型。
type ExpressionAccessor interface {
	// GetExpression 返回待编译的 CEL 源码文本。
	GetExpression() string
	// ReturnTypes 列出表达式允许产生的类型；包含多个条目表示插件接受
	// 多种结果类型的并集。
	ReturnTypes() []*cel.Type
}

// NamedExpressionAccessor 在 ExpressionAccessor 之上增加了名称。
type NamedExpressionAccessor interface {
	ExpressionAccessor

	GetName() string // 遵循 ExpressionAccessor 的命名约定。
}

// EvaluationResult 包含一次 CEL 求值的最小必需字段与元数据。
// 字段刻意导出，方便插件既上报指标（Elapsed），又无需重新求值即可
// 渲染逐表达式的错误。
type EvaluationResult struct {
	// EvalResult 是表达式产生的 CEL 值；保留原始 ref.Val，
	// 由调用方决定如何转换为 Go 值。
	EvalResult ref.Val
	// ExpressionAccessor 是产生 EvalResult 的表达式，方便调用方把
	// 结果与源表达式对应起来。
	ExpressionAccessor ExpressionAccessor
	// Elapsed 是这条表达式单次求值所花的时间。
	Elapsed time.Duration
	// Error 非空表示该表达式求值失败；同一批次中的其他结果
	// 可能仍然可用。
	Error error
}

// OptionalVariableDeclarations 声明为表达式声明了哪些可选 CEL 变量。
//
// 声明集合是编译缓存键的一部分：同一表达式配不同声明会编译成不同的程序。
// 变量的实际绑定在求值时通过 OptionalVariableBindings 单独提供。
type OptionalVariableDeclarations struct {
	// HasParams 指定是否声明了 "params" 变量；
	// 即使声明了，"params" 也可能绑定为 null。
	HasParams bool
	// HasAuthorizer 指定是否声明了 "authorizer" 与 "authorizer.requestResource"
	// 变量；声明后这两个变量预期非 null。
	HasAuthorizer bool
	// HasPatchTypes 指定 CEL 中是否可用 JSONPatch、Object、Object.metadata 等类型；
	// 这些类型用于初始化创建补丁所需的 CEL 类型化对象。
	HasPatchTypes bool
}

// ConditionCompiler 提供在 CEL 类型化值与其他类型/值之间转换的编译辅助函数。
type ConditionCompiler interface {
	// CompileCondition 用于 CEL 表达式的编译。
	// envType 选择 CEL 环境（例如含或不含可选变量）；实现必须以
	// 表达式、声明集合与 envType 作为键缓存已编译的程序。
	CompileCondition(expressions []ExpressionAccessor, optionalDecls OptionalVariableDeclarations, envType environment.Type) ConditionEvaluator
}

// OptionalVariableBindings 提供可选 CEL 变量的表达式绑定。
// 绑定是运行时值；对应的声明必须已在编译期启用，否则表达式无法引用它们。
type OptionalVariableBindings struct {
	// VersionedParams 提供 "params" 变量的绑定；即使
	// OptionalVariableDeclarations.HasParams 为 true，该绑定也可能为 nil。
	VersionedParams runtime.Object
	// Authorizer 提供用于 "authorizer" 与 "authorizer.requestResource"
	// 变量绑定的授权器；若表达式编译时
	// OptionalVariableDeclarations.HasAuthorizer 为 true，此处必须非 nil。
	Authorizer authorizer.UnconditionalAuthorizer
}

// ConditionEvaluator 保存“求值为条件的 CEL 表达式”的编译结果，既用于校验
// 也用于 precondition。它要求传入对象已经转换为底层 CEL 代码期望的版本
// （由策略定义的匹配条件指明）；versionedParams 可以为 nil。
type ConditionEvaluator interface {
	// ForInput 把已编译的 CEL 类型化值转换为求值后的 CEL 类型化值。
	// runtimeCELCostBudget 仅用于测试；调用方应始终使用
	// k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 RuntimeCELCostBudget 作为入参。
	// 若计算了成本预算，条件应返回剩余预算。
	//
	// 实现必须支持并发使用：一个编译后的 evaluator 服务所有请求。
	ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *v1.AdmissionRequest, optionalVars OptionalVariableBindings, namespace *corev1.Namespace, runtimeCELCostBudget int64) ([]EvaluationResult, int64, error)

	// CompilationErrors 返回编译结果中的错误列表。
	CompilationErrors() []error
}

// MutatingCompiler 提供在 CEL 类型化值与其他类型/值之间转换的编译辅助函数。
type MutatingCompiler interface {
	// CompileMutatingEvaluator 用于 CEL 表达式的编译。
	// 返回的 evaluator 产生 JSON 补丁以及构建它所用的类型化对象列表。
	CompileMutatingEvaluator(expression ExpressionAccessor, optionalDecls OptionalVariableDeclarations, envType environment.Type) MutatingEvaluator
}

// MutatingEvaluator 保存“求值为变异的 CEL 表达式”的编译结果。
// 它要求传入对象已经转换为底层 CEL 代码期望的版本
// （由策略定义的匹配条件指明）；versionedParams 可以为 nil。
type MutatingEvaluator interface {
	// ForInput 把已编译的 CEL 类型化值转换为表示一次变异的 CEL 类型化值。
	// runtimeCELCostBudget 仅用于测试；调用方应始终使用
	// k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 RuntimeCELCostBudget 作为入参。
	// 若计算了成本预算，应返回剩余预算。
	//
	ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *v1.AdmissionRequest, optionalVars OptionalVariableBindings, namespace *corev1.Namespace, runtimeCELCostBudget int64) (EvaluationResult, int64, error)

	// CompilationErrors 返回编译 mutatingEvaluator 过程中的错误列表。
	CompilationErrors() []error
}
