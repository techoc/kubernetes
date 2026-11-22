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

package metrics

import (
	"errors"

	apiservercel "k8s.io/apiserver/pkg/cel"
)

// ErrorType decodes the error to determine the error type
// that the metrics understand.
// 只暴露有限的枚举值，避免错误字符串进入指标标签造成基数爆炸。
func ErrorType(err error) MutationErrorType {
	if err == nil {
		return MutationNoError
	}
	// 用 errors.Is 而非类型断言，以便包装后的错误仍能被正确归类。
	if errors.Is(err, apiservercel.ErrCompilation) {
		return MutationCompileError
	}
	if errors.Is(err, apiservercel.ErrOutOfBudget) {
		return MutatingOutOfBudget
	}
	// 其余一律归为 invalid：拒绝请求但不泄露内部细节。
	return MutatingInvalidError
}
