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

package ktesting

import (
	"errors"
	"fmt"
	"strings"
)

// WithError 创建一个 context，用于收集测试失败，
// 并在调用方结束时将其存入提供的 error 实例。用法如下：
//
//	func doSomething(tCtx ktesting.TContext) (finalErr error) {
//	     tCtx, finalize := WithError(tCtx, &finalErr)
//	     defer finalize()
//	     ...
//	     tCtx.Fatal("some failure")
//
// 如果发生了测试失败，finalize 会覆盖该变量中已有的任何错误，
// 否则变量保持不变。如果有多个测试错误，
// 则会用 errors.Join 将它们全部包装起来。
//
// 测试失败不会传播到父 context。
// WithRESTConfig 会用根据该配置创建的新客户端初始化所有 client-go 客户端。
// 当前测试名称会被包含在 UserAgent 中。
func (tCtx TContext) WithError(err *error) (TContext, func()) {
	tCtx.capture = &capture{}

	return tCtx, func() {
		// Recover 必须在 deferred 函数中调用。如果在被 defer 调用的函数
		// （比如下面的 finalize）中调用，它会返回 nil。
		if e := recover(); e != nil {
			if _, ok := e.(fatalWithError); !ok {
				// 不是我们自己的 panic，继续向外抛出，而不是设置错误。
				panic(e)
			}
		}

		tCtx.finalize(err)
	}
}

func (tCtx TContext) finalize(err *error) {
	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	errs := tCtx.capture.errors
	if tCtx.capture.failed && len(errs) == 0 {
		errs = []error{errFailedWithNoExplanation}
	}
	if len(errs) == 0 {
		return
	}
	*err = failures{errors.Join(errs...)}
}

type failures struct {
	error
}

func (e failures) GomegaString() string {
	// 不需要重复输出字符串。Gomega 本身已经格式化过一次错误，
	// 之后它调用 GomegaString 只是为了得到一个已无必要的摘要。
	return ""
}

// buildHeader 负责处理以下情况：
// - "ERROR:<non-empty prefix><optional header><suffix>"：有 header 时同时使用 prefix 和 suffix，否则只使用 suffix
// - "<empty prefix><optional header><suffix>"：仅在有 header 时使用 suffix
func (tCtx TContext) buildHeader(prefix, suffix string) string {
	if tCtx.perTestHeader != nil {
		return prefix + tCtx.perTestHeader() + suffix
	}
	if prefix != "" {
		return suffix
	}
	return ""
}

// indent 要么只缩进后续各行，要么连同第一行一起缩进所有行。
func indent(msg string, all bool) string {
	header := ""
	if all {
		header = "\t"
	}
	return header + strings.ReplaceAll(msg, "\n", "\n\t")
}

func (tCtx TContext) Skip(args ...any) {
	tCtx.Helper()
	// Enable `go vet printf` by directly calling fmt.Sprintln.
	msg := strings.TrimSpace(fmt.Sprintln(args...))
	tCtx.TB().Skip("SKIP:", tCtx.buildHeader(" ", " ")+tCtx.steps+indent(msg, false))
}

func (tCtx TContext) Skipf(format string, args ...any) {
	tCtx.Helper()
	// Enable `go vet printf` by directly calling fmt.Sprintf.
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	tCtx.TB().Skip("SKIP:", tCtx.buildHeader(" ", " ")+tCtx.steps+indent(msg, false))
}

func (tCtx TContext) Log(args ...any) {
	tCtx.Helper()
	// Enable `go vet printf` by directly calling fmt.Sprintln.
	msg := strings.TrimSpace(fmt.Sprintln(args...))
	tCtx.TB().Log(tCtx.buildHeader("", " ") + tCtx.steps + indent(msg, false))
}

func (tCtx TContext) Logf(format string, args ...any) {
	tCtx.Helper()
	// Enable `go vet printf` by directly calling fmt.Sprintf.
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	tCtx.TB().Log(tCtx.buildHeader("", " ") + tCtx.steps + indent(msg, false))
}

func (tCtx TContext) Error(args ...any) {
	if tCtx.capture == nil {
		tCtx.Helper()
		msg := strings.TrimSpace(fmt.Sprintln(args...))
		// 将 ERROR 放在 header *之前*，使其作为失败信息更醒目。
		tCtx.TB().Error("ERROR:" + tCtx.buildHeader(" ", "\n") + indent(tCtx.steps+msg, true))
		return
	}

	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	// Gomega 在 https://github.com/onsi/gomega/blob/f804ac6ada8d36164ecae0513295de8affce1245/internal/gomega.go#L37 处添加了开头换行。
	// 这里去掉开头和结尾的换行，因为 ktesting 会用 "ERROR" 前缀让错误更加醒目，
	// 不需要额外的空行。此外，Sprintln（`go vet printf` 所必需的）也会
	// 添加一个我们不想要的结尾换行。
	msg := strings.TrimSpace(fmt.Sprintln(args...))
	tCtx.capture.errors = append(tCtx.capture.errors, errors.New(tCtx.steps+msg))
	tCtx.capture.failed = true
}

func (tCtx TContext) Errorf(format string, args ...any) {
	if tCtx.capture == nil {
		tCtx.Helper()
		// 直接调用 fmt.Sprintln，以便启用 `go vet printf`。
		msg := strings.TrimSpace(fmt.Sprintf(format, args...))
		// 将 ERROR 放在 header *之前*，使其作为失败信息更醒目。
		tCtx.TB().Error("ERROR:" + tCtx.buildHeader(" ", "\n") + indent(tCtx.steps+msg, true))
		return
	}

	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	tCtx.capture.errors = append(tCtx.capture.errors, errors.New(tCtx.steps+msg))
	tCtx.capture.failed = true
}

func (tCtx TContext) Fail() {
	if tCtx.capture == nil {
		tCtx.TB().Fail()
		return
	}

	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	tCtx.capture.failed = true
}

func (tCtx TContext) FailNow() {
	if tCtx.capture == nil {
		tCtx.TB().FailNow()
		return
	}

	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	tCtx.capture.failed = true
	panic(failed)
}

func (tCtx TContext) Failed() bool {
	if tCtx.capture == nil {
		return tCtx.TB().Failed()
	}

	tCtx.capture.mutex.Lock()
	defer tCtx.capture.mutex.Unlock()

	return tCtx.capture.failed
}

func (tCtx TContext) Fatal(args ...any) {
	if tCtx.capture == nil {
		tCtx.Helper()
		// 直接调用 fmt.Sprintln，以便启用 `go vet printf`。
		msg := strings.TrimSpace(fmt.Sprintln(args...))
		// 将 FATAL ERROR 放在 header *之前*，使其作为失败信息更醒目。
		tCtx.TB().Fatal("FATAL ERROR:" + tCtx.buildHeader(" ", "\n") + indent(tCtx.steps+msg, true))
	}

	tCtx.Error(args...)
	tCtx.FailNow()
}

func (tCtx TContext) Fatalf(format string, args ...any) {
	if tCtx.capture == nil {
		tCtx.Helper()
		// 直接调用 fmt.Sprintf，以便启用 `go vet printf`。
		msg := strings.TrimSpace(fmt.Sprintf(format, args...))
		// 将 FATAL ERROR 放在 header *之前*，使其作为失败信息更醒目。
		tCtx.TB().Fatal("FATAL ERROR:" + tCtx.buildHeader(" ", "\n") + indent(tCtx.steps+msg, true))
		return
	}

	tCtx.Errorf(format, args...)
	tCtx.FailNow()
}

// fatalWithError 是内部类型，绝不应向外传播。
// 唯一可能发生这种情况的场景是开发者忘记通过 defer 调用 finalize。
// 这个字符串会对该情况作出解释，以防开发者真的看到它。
type fatalWithError string

const failed = fatalWithError("WithError TContext encountered a fatal error, but the finalize function was not called via defer as it should have been.")

var errFailedWithNoExplanation = errors.New("WithError context was marked as failed without recording an error")
