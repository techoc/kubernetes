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
	"errors"
	"testing"

	"github.com/google/cel-go/cel"
)

// TestOutOfBudgetError 验证 CEL 预算耗尽错误同时匹配具体哨兵 ErrOutOfBudget
// 与分类哨兵 ErrInvalid，调用方可用任一粒度做判断。
func TestOutOfBudgetError(t *testing.T) {
	err := &Error{
		Type:   ErrorTypeInvalid,
		Detail: "expression out of budget",
		Cause:  ErrOutOfBudget,
	}
	if !errors.Is(err, ErrOutOfBudget) {
		t.Errorf("unexpected %v is not %v", err, ErrOutOfBudget)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("unexpected %v is not %v", err, ErrInvalid)
	}
}

// TestCompilationError 验证编译错误：
//   - ErrCompilation 归类为 ErrInvalid；
//   - 包装后的 *Error 仍可被 errors.As 还原出 CompilationError 并保留原始 Issues 指针，
//     这对把 CEL 诊断信息透传给用户至关重要。
func TestCompilationError(t *testing.T) {
	if !errors.Is(ErrCompilation, ErrInvalid) {
		t.Errorf("unexpected %v is not %v", ErrCompilation, ErrInvalid)
	}
	issues := &cel.Issues{}
	err := &Error{
		Type:   ErrorTypeInvalid,
		Detail: "fake compilation failed",
		Cause:  NewCompilationError(issues),
	}
	if !errors.Is(err, ErrCompilation) {
		t.Errorf("unexpected %v is not %v", err, ErrCompilation)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("unexpected %v is not %v", err, ErrInvalid)
	}
	var compilationErr *CompilationError
	if errors.As(err, &compilationErr); compilationErr == nil {
		t.Errorf("unexpected %v cannot be fitted into CompilationError", err)
	}
	if compilationErr.Issues != issues {
		t.Errorf("retrieved issues is not the original")
	}
}
