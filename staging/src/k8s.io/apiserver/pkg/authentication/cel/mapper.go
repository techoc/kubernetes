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
	"fmt"

	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/interpreter"
)

var _ ClaimsMapper = &mapper{}
var _ UserMapper = &mapper{}

// mapper 同时实现 ClaimsMapper 与 UserMapper 接口，同一个实现服务于
// claims 与 userInfo 两类求值，区别仅在于激活变量名。
type mapper struct {
	// 编译后的 CEL 表达式；求值时按顺序逐个执行。
	compilationResults []CompilationResult
}

// CELMapper 分别持有 username、groups、uid、extra、claimValidation 与 userValidation
// 的已编译 CEL 表达式，按用途分组保存，便于认证插件按需调用。
type CELMapper struct {
	Username             ClaimsMapper
	Groups               ClaimsMapper
	UID                  ClaimsMapper
	Extra                ClaimsMapper
	ClaimValidationRules ClaimsMapper
	UserValidationRules  UserMapper
}

// NewClaimsMapper 构造一个 ClaimsMapper。
func NewClaimsMapper(compilationResults []CompilationResult) ClaimsMapper {
	return &mapper{
		compilationResults: compilationResults,
	}
}

// NewUserMapper 构造一个 UserMapper。
func NewUserMapper(compilationResults []CompilationResult) UserMapper {
	return &mapper{
		compilationResults: compilationResults,
	}
}

// EvalClaimMapping 求值给定的 claim 映射表达式并返回一个 EvaluationResult，
// 用于期望恰好产生一个结果的映射表达式（如 username）；结果数量不符时报错。
func (m *mapper) EvalClaimMapping(ctx context.Context, claims traits.Mapper) (EvaluationResult, error) {
	results, err := m.eval(ctx, &varNameActivation{name: claimsVarName, value: claims})
	if err != nil {
		return EvaluationResult{}, err
	}
	if len(results) != 1 {
		return EvaluationResult{}, fmt.Errorf("expected 1 evaluation result, got %d", len(results))
	}
	return results[0], nil
}

// EvalClaimMappings 求值给定的表达式并返回 EvaluationResult 列表，
// 用于会产生多个结果的表达式（如 groups、extra）。
func (m *mapper) EvalClaimMappings(ctx context.Context, claims traits.Mapper) ([]EvaluationResult, error) {
	return m.eval(ctx, &varNameActivation{name: claimsVarName, value: claims})
}

// EvalUser 求值给定的 user 表达式并返回 EvaluationResult 列表；
// userInfo 变量在编译期已按 user.Info 的结构做类型检查。
func (m *mapper) EvalUser(ctx context.Context, userInfo traits.Mapper) ([]EvaluationResult, error) {
	return m.eval(ctx, &varNameActivation{name: userVarName, value: userInfo})
}

// eval 逐个执行编译后的表达式。
// 任一表达式求值失败即整体失败：认证过程中宁可拒绝也不返回部分映射结果。
func (m *mapper) eval(ctx context.Context, input *varNameActivation) ([]EvaluationResult, error) {
	evaluations := make([]EvaluationResult, len(m.compilationResults))

	for i, compilationResult := range m.compilationResults {
		var evaluation = &evaluations[i]
		evaluation.ExpressionAccessor = compilationResult.ExpressionAccessor

		evalResult, _, err := compilationResult.Program.ContextEval(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("expression '%s' resulted in error: %w", compilationResult.ExpressionAccessor.GetExpression(), err)
		}

		evaluation.EvalResult = evalResult
	}

	return evaluations, nil
}

var _ interpreter.Activation = &varNameActivation{}

// varNameActivation 把单个变量名绑定到一个 map，供 CEL 程序解析顶层变量。
type varNameActivation struct {
	name  string
	value traits.Mapper
}

// ResolveName 只解析匹配的变量名，其余名称返回未找到，避免误暴露其他变量。
func (v *varNameActivation) ResolveName(name string) (any, bool) {
	if v.name != name {
		return nil, false
	}
	return v.value, true
}

// Parent 返回 nil：本激活无父级，不存在链式变量解析。
func (v *varNameActivation) Parent() interpreter.Activation { return nil }
