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

// Package errors 提供类似 https://github.com/pkg/errors 的本地错误包装实现，支持堆栈。
// 该实现从零编写，更简单，并非 pkg/errors 的分支。
package errors

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	// defaultErrorExitCode 定义通用错误码 1。
	defaultErrorExitCode = 1
)

// errorWithStack 实现 Error/Unwrap 接口，用于错误包装和堆栈跟踪。
type errorWithStack struct {
	msg   error
	cause error
	stack string
}

// Error 返回包装后的错误文本。
func (e errorWithStack) Error() string {
	return e.msg.Error()
}

// Unwrap 返回被包装的原始错误，供 errors.Is 和 errors.As 遍历错误链。
func (e errorWithStack) Unwrap() error {
	return e.cause
}

// Wrap 包装错误并附加堆栈。
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return &errorWithStack{
		msg:   fmt.Errorf("%s: %w", message, err),
		cause: err,
		stack: callStack(),
	}
}

// Wrapf 与 Wrap 类似，但支持格式化和参数。
func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &errorWithStack{
		msg:   fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err),
		cause: err,
		stack: callStack(),
	}
}

// Unwrap 使用标准库方法解包错误。
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// WithMessage 为错误添加消息并附加堆栈。
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &errorWithStack{
		msg:   fmt.Errorf("%s: %s", message, err.Error()),
		stack: callStack(),
	}
}

// WithMessagef 与 WithMessage 类似，但支持格式化和参数。
func WithMessagef(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &errorWithStack{
		msg:   fmt.Errorf("%s: %s", fmt.Sprintf(format, args...), err.Error()),
		stack: callStack(),
	}
}

// New 调用标准库 errors.New 并附加堆栈。
func New(message string) error {
	return &errorWithStack{
		msg:   errors.New(message),
		stack: callStack(),
	}
}

// Errorf 调用标准库 errors.Errorf 并附加堆栈。
func Errorf(format string, args ...any) error {
	return &errorWithStack{
		msg:   fmt.Errorf(format, args...),
		stack: callStack(),
	}
}

// StackTrace 以字符串形式返回错误的堆栈。
func StackTrace(err error) string {
	s, ok := err.(*errorWithStack) //nolint:errorlint
	if !ok {
		return "no stack trace"
	}
	return s.stack
}

// Is 包装标准库 errors.Is。
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As 包装标准库 errors.As。
func As(err error, target any) bool {
	return errors.As(err, target)
}

// callStack 获取创建错误位置的调用栈。
// 跳过 3 个调用者，使错误创建位置位于栈顶；格式类似 runtime.PrintStack() 和 panic()。
func callStack() string {
	pc := make([]uintptr, 32)
	n := runtime.Callers(3, pc)
	frames := runtime.CallersFrames(pc[:n])
	buf := bytes.Buffer{}

	for {
		frame, more := frames.Next()
		fmt.Fprintf(&buf, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}

	return buf.String()
}

// HandleError 是 handleError 的默认包装，使用 exitWithCode 作为回调处理错误。
func HandleError(err error) {
	handleError(err, exitWithCode)
}

func handleError(err error, handleFunc func(string, int)) {
	if err == nil {
		return
	}

	msg := fmt.Sprintf("%s\nTo see the stack trace of this error execute with --v=5 or higher", err.Error())
	// 检查 klog 详细级别是否足以输出堆栈。
	f := flag.CommandLine.Lookup("v")
	if f != nil {
		// 假设 "v" 标志按 klog 的 "Level" 类型别名包含可解析的 Int32，因此不处理 ParseInt 错误。
		if v, e := strconv.ParseInt(f.Value.String(), 10, 32); e == nil {
			// https://git.k8s.io/community/contributors/devel/sig-instrumentation/logging.md
			// klog.V(5) - 跟踪级别
			if v > 4 {
				msg = fmt.Sprintf("%v\n%s", err.Error(), StackTrace(err))
			}
		}
	}

	handleFunc(msg, defaultErrorExitCode)
}

// exitWithCode 可选地将消息输出到 stderr，然后以指定错误码退出。
func exitWithCode(msg string, code int) {
	if len(msg) > 0 {
		// 必要时添加换行。
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}

		_, _ = fmt.Fprintf(os.Stderr, "error: %s", msg)
	}
	os.Exit(code)
}
