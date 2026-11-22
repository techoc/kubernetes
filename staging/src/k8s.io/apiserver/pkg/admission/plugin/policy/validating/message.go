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
	celgo "github.com/google/cel-go/cel"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
)

var _ cel.ExpressionAccessor = (*MessageExpressionCondition)(nil)

// MessageExpressionCondition 表示策略中的 messageExpression：它在失败时被求值，
// 用于生成比静态 message 更具体的拒绝原因。求值结果必须是字符串，
// 因此 ReturnTypes 固定声明为 string。
type MessageExpressionCondition struct {
	// MessageExpression 是 CEL 表达式源码。
	MessageExpression string
}

// GetExpression 返回供 CEL 编译器解析的表达式文本。
func (m *MessageExpressionCondition) GetExpression() string {
	return m.MessageExpression
}

// ReturnTypes 限定表达式返回类型为 string，编译期即可拒绝返回其他类型的表达式。
func (m *MessageExpressionCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.StringType}
}
