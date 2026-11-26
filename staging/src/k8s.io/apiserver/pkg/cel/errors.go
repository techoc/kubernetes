/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"

	"github.com/google/cel-go/cel"
)

// ErrInternal 表示表达式因内部原因求值失败的基础错误。
// 任何 Type 为 ErrorInternal 的 Error 都视为与 ErrInternal 相等
var ErrInternal = fmt.Errorf("internal")

// ErrInvalid 表示表达式求值失败但非内部原因的基础错误。
// 任何 Type 为 ErrorInvalid 的 Error 都视为与 ErrInvalid 相等。
var ErrInvalid = fmt.Errorf("invalid")

// ErrRequired 表示必填表达式缺失时的基础错误。
//
// 任何 Type 为 ErrorRequired 的 Error 都视为与 ErrRequired 相等。
var ErrRequired = fmt.Errorf("required")

// ErrCompilation 表示表达式编译失败的基础错误。
// 任何 CompilationError 都包装 ErrCompilation。
// ErrCompilation 包装 ErrInvalid。
var ErrCompilation = fmt.Errorf("%w: compilation error", ErrInvalid)

// ErrOutOfBudget 表示表达式超出预算而失败的基础错误。
var ErrOutOfBudget = fmt.Errorf("out of budget")

// Error 实现 'error' 接口，表示一个 XValidation 错误。
type Error struct {
	Type   ErrorType
	Detail string

	// Cause 是可选的被包装错误，便于以编程方式获取详细错误信息。
	Cause error
}

var _ error = &Error{}

// Error 实现 error 接口。
func (v *Error) Error() string {
	return v.Detail
}

// Is 让 *Error 能通过 errors.Is 与哨兵错误匹配。
// 只按 Type 归类，不比较 Detail，调用方可以据此区分"表达式非法"
// 与"求值内部错误"，而不必依赖错误文本。
func (v *Error) Is(err error) bool {
	switch v.Type {
	case ErrorTypeRequired:
		return err == ErrRequired
	case ErrorTypeInvalid:
		return err == ErrInvalid
	case ErrorTypeInternal:
		return err == ErrInternal
	}
	return false
}

// Unwrap 返回被包装的 Cause。
func (v *Error) Unwrap() error {
	return v.Cause
}

// ErrorType 是机器可读的值，提供 XValidation 无效的更详细原因。
type ErrorType string

const (
	// ErrorTypeRequired 用于报告未提供 withNullable 值
	// （如空字符串、null 或空数组）。参见 Required()。
	//
	ErrorTypeRequired ErrorType = "RuleRequired"
	// ErrorTypeInvalid 用于报告格式错误的值
	ErrorTypeInvalid ErrorType = "RuleInvalid"
	// 参见 InternalError()。
	ErrorTypeInternal ErrorType = "InternalError"
)

// CompilationError 表示表达式编译期间的错误。
// 它包装 ErrCompilation。
type CompilationError struct {
	err    *Error
	Issues *cel.Issues
}

// NewCompilationError 包装 cel.Issues，表示编译失败。
func NewCompilationError(issues *cel.Issues) *CompilationError {
	return &CompilationError{
		Issues: issues,
		err: &Error{
			Type:   ErrorTypeInvalid,
			Detail: fmt.Sprintf("compilation error: %s", issues),
		}}
}

// Error 返回面向用户的编译错误文本。
func (e *CompilationError) Error() string {
	return e.err.Error()
}

// Unwrap 同时暴露内部 *Error 与 ErrCompilation 哨兵，
// 这样 errors.Is 既能匹配 ErrInvalid（经由 *Error.Is），也能匹配 ErrCompilation。
func (e *CompilationError) Unwrap() []error {
	return []error{e.err, ErrCompilation}
}
