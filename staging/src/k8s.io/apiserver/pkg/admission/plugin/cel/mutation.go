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

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/cel/environment"
)

// mutatingCompiler 实现 MutatingCompiler。
// 它把编译工作委托给共享的 Compiler，仅替换编译结果的包装方式，
// 从而保证变更（mutating）与校验（validating）两类表达式使用同一套
// 环境、库与成本预算规则。
type mutatingCompiler struct {
	compiler Compiler
}

// CompileMutatingEvaluator 为准入插件编译 CEL 表达式，并返回可用于执行该表达式的 MutatingEvaluator。
// mode 决定编译时可见的 CEL 环境版本（例如是否允许使用可选库），
// 必须在构建期固定，避免同一表达式在不同版本间行为漂移。
func (p *mutatingCompiler) CompileMutatingEvaluator(expressionAccessor ExpressionAccessor, options OptionalVariableDeclarations, mode environment.Type) MutatingEvaluator {
	compilationResult := p.compiler.CompileCELExpression(expressionAccessor, options, mode)
	return NewMutatingEvaluator(compilationResult)
}

// mutatingEvaluator 持有编译产物；编译错误被保留到求值阶段再暴露，
// 使调用方可以先完成初始化、再统一处理策略中的错误表达式。
type mutatingEvaluator struct {
	compilationResult CompilationResult
}

// NewMutatingEvaluator 用编译结果构造 MutatingEvaluator。
// 即使 compilationResult 携带错误也照常返回，错误由 CompilationErrors 报告。
func NewMutatingEvaluator(compilationResult CompilationResult) MutatingEvaluator {
	return &mutatingEvaluator{compilationResult}
}

// ForInput 求值已编译的 CEL 表达式并返回求值结果。
// 每次求值的错误随求值结果一并返回，便于调用方区分“表达式错误”与“运行时失败”。
// runtimeCELCostBudget 仅用于测试注入；生产调用方应始终传入
// k8s.io/apiserver/pkg/apis/cel/config.go 中的 RuntimeCELCostBudget 常量，
// 否则可能绕过对恶意/低效表达式的算力保护。
func (p *mutatingEvaluator) ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *admissionv1.AdmissionRequest, inputs OptionalVariableBindings, namespace *v1.Namespace, runtimeCELCostBudget int64) (EvaluationResult, int64, error) {
	// 组合表达式需要 compositionCtx 来复用子表达式的求值上下文；非组合场景下为 nil。
	compositionCtx, _ := ctx.(CompositionContext)

	activation, err := newActivation(compositionCtx, versionedAttr, request, inputs, namespace)
	if err != nil {
		return EvaluationResult{}, -1, err
	}
	evaluation, remainingBudget, err := activation.Evaluate(ctx, compositionCtx, p.compilationResult, runtimeCELCostBudget)
	if err != nil {
		return evaluation, -1, err
	}
	return evaluation, remainingBudget, nil

}

// CompilationErrors 返回编译 mutatingEvaluator 期间产生的全部错误。
// 未编译成功时返回单元素切片而非 nil，方便调用方统一追加到聚合错误中。
func (p *mutatingEvaluator) CompilationErrors() (compilationErrors []error) {
	if p.compilationResult.Error != nil {
		return []error{p.compilationResult.Error}
	}
	return nil
}
