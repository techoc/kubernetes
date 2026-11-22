/*
Copyright 2025 The Kubernetes Authors.

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

package mutating

import (
	"strings"

	celmetrics "k8s.io/apiserver/pkg/admission/plugin/policy/mutating/metrics"
)

// ErrorType decodes the error to determine the error type
// that the metrics understand.
// 这里用错误字符串前缀而非错误类型来分类，是因为错误来自 CEL 编译与求值层，
// 该层未导出可用于类型断言的错误值；改动上述错误消息会导致指标分类静默退化，
// 因此这些字符串实际上构成隐式契约。
func ErrorType(err error) celmetrics.MutationErrorType {
	if err == nil {
		return celmetrics.MutationNoError
	}
	// 编译期错误（如表达式类型不匹配、未知变量）单独计数，便于区分策略编写错误与运行时失败。
	if strings.HasPrefix(err.Error(), "compilation") {
		return celmetrics.MutationCompileError
	}
	// 预算耗尽说明策略本身合法但过于昂贵，属于需要策略作者关注的另一类问题。
	if strings.HasPrefix(err.Error(), "validation failed due to running out of cost budget") {
		return celmetrics.MutatingOutOfBudget
	}
	return celmetrics.MutatingInvalidError
}
