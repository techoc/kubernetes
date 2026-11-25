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

package validating

import (
	"strings"

	celmetrics "k8s.io/apiserver/pkg/admission/plugin/policy/validating/metrics"
)

// ErrorType 解析策略决策，得到指标可理解的错误类型。
//
// 该函数把内部 PolicyDecision 映射为指标维度。之所以按消息前缀判断，
// 是因为 PolicyDecision 只携带文本消息，没有独立的错误分类字段；前缀
// 由编译器和 CEL 运行时固定产生，修改这些文案会破坏指标归类。
func ErrorType(decision *PolicyDecision) celmetrics.ValidationErrorType {
	// 评估结果为允许即视为无错误，不进入失败指标。
	if decision.Evaluation == EvalAdmit {
		return celmetrics.ValidationNoError
	}
	// 编译期失败与运行期失败分开统计，便于区分策略编写错误和求值问题。
	if strings.HasPrefix(decision.Message, "compilation") {
		return celmetrics.ValidationCompileError
	}
	// 超出成本预算单独归类：这类失败通常是策略过于复杂，而非策略逻辑错误。
	if strings.HasPrefix(decision.Message, "validation failed due to running out of cost budget") {
		return celmetrics.ValidatingOutOfBudget
	}
	return celmetrics.ValidatingInvalidError
}
