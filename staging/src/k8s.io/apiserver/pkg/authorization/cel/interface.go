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

// Package cel 实现基于 CEL 的授权条件匹配，供结构化授权配置
// （AuthorizationConfiguration 中的 CEL 匹配条件）使用。
package cel

import (
	celgo "github.com/google/cel-go/cel"
)

// ExpressionAccessor 抽象一条待编译的 CEL 表达式及其静态返回类型。
// 返回类型在编译期参与类型检查，使拼写错误或类型不匹配的表达式
// 在配置加载阶段就失败，而不是等到首个授权请求。
type ExpressionAccessor interface {
	GetExpression() string
	ReturnTypes() []*celgo.Type
}

var _ ExpressionAccessor = &SubjectAccessReviewMatchCondition{}

// SubjectAccessReviewMatchCondition 是一条把 SubjectAccessReview 请求映射到一组取值的 CEL 表达式。
// 返回类型固定为 bool——匹配条件必须给出明确的真/假，不能返回
// 可空或动态值，否则无法用于授权判定。
type SubjectAccessReviewMatchCondition struct {
	Expression string
}

// GetExpression 返回原始 CEL 表达式文本。
func (v *SubjectAccessReviewMatchCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 声明表达式必须返回 bool。
func (v *SubjectAccessReviewMatchCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.BoolType}
}
