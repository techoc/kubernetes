/*
Copyright The Kubernetes Authors.

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
	"errors"
	"fmt"
	"strings"
)

// enhancedError wraps an error with additional CEL hint for "no such key" errors.
// It is used to ensure EnhanceRuntimeError is idempotent.
//
// enhancedError 在原错误之外附加 CEL 使用提示，
// 并以自身类型作为标记，使 EnhanceRuntimeError 具备幂等性。
//
// 字段说明：error 是被包装的原始错误，Unwrap 会将其暴露给 errors.Is/As。
type enhancedError struct {
	error
}

// Unwrap 返回被包装的原始错误，使标准错误判断能够穿透这层包装。
func (e *enhancedError) Unwrap() error { return e.error }

// EnhanceRuntimeError provides a more helpful error message for "no such key" errors.
// It detects CEL runtime errors that occur when accessing non-existent map keys
// and adds a hint about using optional field access patterns.
// For all other errors, it returns the error unchanged.
//
// Note: This function focuses on enhancing the CEL error message itself.
// Additional context (like device ID) should be added by the caller when wrapping the error.
//
// EnhanceRuntimeError 针对访问不存在的映射键所产生的 CEL 运行时错误，
// 追加关于可选链（?. 配合 orValue()）与 has() 守卫的提示；其他错误原样返回。
func EnhanceRuntimeError(err error) error {
	// 空错误直接返回，避免后续处理出现空指针。
	if err == nil {
		return nil
	}

	// Check if already enhanced using type assertion for idempotency.
	// 若错误已被包装过则直接返回，保证重复调用不会重复追加提示。
	var enhanced *enhancedError
	if errors.As(err, &enhanced) {
		return err
	}

	errMsg := err.Error()
	// Check if this is a "no such key" error.
	// Using Contains instead of HasPrefix to handle wrapped errors.
	// 使用 Contains 而非 HasPrefix，这样带有前缀的包装错误也能被识别。
	if strings.Contains(errMsg, "no such key:") {
		// 命中时用 enhancedError 包装原错误并附上可选字段访问建议。
		return &enhancedError{
			error: fmt.Errorf("%w. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields", err),
		}
	}

	// Not a "no such key" error, return unchanged.
	// 非缺键错误保持原样返回。
	return err
}
