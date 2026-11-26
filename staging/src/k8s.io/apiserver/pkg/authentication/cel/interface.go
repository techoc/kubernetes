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

// Package cel 包含认证相关的 CEL 接口与结构体。
package cel

import (
	"context"

	celgo "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// ExpressionAccessor 提供对 CEL 表达式的访问。
//
// 把"表达式文本"与"期望返回类型"解耦，使编译器无需了解具体的配置结构体。
type ExpressionAccessor interface {
	GetExpression() string
	ReturnTypes() []*celgo.Type
}

// CompilationResult 表示编译后的校验表达式。
type CompilationResult struct {
	Program            celgo.Program
	AST                *celgo.Ast
	ExpressionAccessor ExpressionAccessor
}

// EvaluationResult 包含 CEL 求值必需的最小字段与元数据。
type EvaluationResult struct {
	EvalResult         ref.Val
	ExpressionAccessor ExpressionAccessor
}

// Compiler 是配置了认证相关 CEL 变量的表达式编译器。
//
// claims 与 user 两套变量环境不同（前者来自 IdP 的 token claims，后者是
// 已构造的 UserInfo），因此分成两个编译入口，避免变量互相泄漏。
type Compiler interface {
	CompileClaimsExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error)
	CompileUserExpression(expressionAccessor ExpressionAccessor) (CompilationResult, error)
}

// ClaimsMapper 是配置了 claims CEL 变量的表达式求值器。
type ClaimsMapper interface {
	// EvalClaimMapping 求值为单个表达式的 claim 映射（用户名、组、uid），
	// 返回 EvaluationResult。
	EvalClaimMapping(ctx context.Context, claims traits.Mapper) (EvaluationResult, error)
	// EvalClaimMappings 求值一组表达式（extra 映射与 claim 校验），
	// 返回 EvaluationResult 列表。
	EvalClaimMappings(ctx context.Context, claims traits.Mapper) ([]EvaluationResult, error)
}

// UserMapper 是配置了 user CEL 变量的表达式求值器。
type UserMapper interface {
	// EvalUser 求值一组用户校验表达式，返回 EvaluationResult 列表。
	//
	EvalUser(ctx context.Context, userInfo traits.Mapper) ([]EvaluationResult, error)
}

var _ ExpressionAccessor = &ClaimMappingExpression{}

// ClaimMappingExpression 是映射某个 claim 的 CEL 表达式。
type ClaimMappingExpression struct {
	Expression string
}

// GetExpression 返回 CEL 表达式。
func (v *ClaimMappingExpression) GetExpression() string {
	return v.Expression
}

// ReturnTypes 返回 CEL 表达式的返回类型。
func (v *ClaimMappingExpression) ReturnTypes() []*celgo.Type {
	// 返回类型仅供校验：claim 映射表达式拿到的 claims 是
	// map[string]interface{}，编译期无法确定真实返回类型，
	// 严格类型检查推迟到求值阶段进行。
	//
	return []*celgo.Type{celgo.AnyType}
}

var _ ExpressionAccessor = &ClaimValidationCondition{}

// ClaimValidationCondition 是校验某个 claim 的 CEL 表达式。
type ClaimValidationCondition struct {
	Expression string
	Message    string
}

// GetExpression 返回 CEL 表达式。
func (v *ClaimValidationCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 返回 CEL 表达式的返回类型。
func (v *ClaimValidationCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.BoolType}
}

var _ ExpressionAccessor = &ExtraMappingExpression{}

// ExtraMappingExpression 是把 extra 映射为一组值的 CEL 表达式。
type ExtraMappingExpression struct {
	Key        string
	Expression string
}

// GetExpression 返回 CEL 表达式。
func (v *ExtraMappingExpression) GetExpression() string {
	return v.Expression
}

// ReturnTypes 返回 CEL 表达式的返回类型。
func (v *ExtraMappingExpression) ReturnTypes() []*celgo.Type {
	// 返回类型仅供校验：映射表达式拿到的 claims 是
	// map[string]interface{}，编译期无法确定真实返回类型，
	// 严格类型检查推迟到求值阶段进行。
	//
	return []*celgo.Type{celgo.AnyType}
}

var _ ExpressionAccessor = &UserValidationCondition{}

// UserValidationCondition 是校验 User 的 CEL 表达式。
type UserValidationCondition struct {
	Expression string
	Message    string
}

// GetExpression 返回 CEL 表达式。
func (v *UserValidationCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 返回 CEL 表达式的返回类型。
func (v *UserValidationCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.BoolType}
}
