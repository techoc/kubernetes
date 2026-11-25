/*
Copyright 2014 The Kubernetes Authors.

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

package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	ginkgotypes "github.com/onsi/ginkgo/v2/types"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
)

// MakeMatcher 基于单个回调函数构建一个 gomega.Matcher。
// 该函数接收要检查的实际值。
// 检查有三种可能的结果：
//
//   - 返回错误，随后由 Gomega 转换为失败
//
//   - 返回非 nil 的 failure 函数，当需要失败字符串时
//     由 Gomega 调用。这有助于
//     避免在 Eventually 或 Consistently 的中间失败中
//     不必要地准备失败字符串。
//
//   - 函数和错误都为 nil，表示检查通过
func MakeMatcher[T interface{}](match func(actual T) (failure func() string, err error)) types.GomegaMatcher {
	return &matcher[T]{
		match: match,
	}
}

type matcher[T interface{}] struct {
	match   func(actual T) (func() string, error)
	failure func() string
}

func (m *matcher[T]) Match(actual interface{}) (success bool, err error) {
	if actual, ok := actual.(T); ok {
		failure, err := m.match(actual)
		if err != nil {
			return false, err
		}
		m.failure = failure
		if failure != nil {
			return false, nil
		}
		return true, nil
	}
	var empty T
	return false, gomega.StopTrying(fmt.Sprintf("internal error: expected %T, got:\n%s", empty, format.Object(actual, 1)))
}

func (m *matcher[T]) FailureMessage(actual interface{}) string {
	return m.failure()
}

func (m matcher[T]) NegatedFailureMessage(actual interface{}) string {
	return m.failure()
}

var _ types.GomegaMatcher = &matcher[string]{}

// Gomega 返回一个可像 gomega 一样用于表达断言的接口。
// 区别在于失败的断言会作为错误返回，
// error:
//
//	if err := Gomega().Expect(pod.Status.Phase).To(gomega.Equal(v1.Running)); err != nil {
//	    return fmt.Errorf("test pod not running: %w", err)
//	}
//
// 可以包装此错误，为失败提供额外上下文。
// 然后测试应使用 ExpectNoError 将非 nil 错误转换为失败。
//
// 使用这种方式时，不需要调用偏移和额外的
// Expect 描述，因为调用 ExpectNoError 时会 dump 调用栈，
// 额外的描述可以通过包装错误来添加。
//
// 异步断言默认使用 framework 的 Poll 间隔和 PodStart 超时。
func Gomega() GomegaInstance {
	return gomegaInstance{}
}

type GomegaInstance interface {
	Expect(actual interface{}) Assertion
	Eventually(ctx context.Context, args ...interface{}) AsyncAssertion
	Consistently(ctx context.Context, args ...interface{}) AsyncAssertion
}

type Assertion interface {
	Should(matcher types.GomegaMatcher) error
	ShouldNot(matcher types.GomegaMatcher) error
	To(matcher types.GomegaMatcher) error
	ToNot(matcher types.GomegaMatcher) error
	NotTo(matcher types.GomegaMatcher) error
}

type AsyncAssertion interface {
	Should(matcher types.GomegaMatcher) error
	ShouldNot(matcher types.GomegaMatcher) error

	WithTimeout(interval time.Duration) AsyncAssertion
	WithPolling(interval time.Duration) AsyncAssertion
}

type gomegaInstance struct{}

var _ GomegaInstance = gomegaInstance{}

func (g gomegaInstance) Expect(actual interface{}) Assertion {
	return assertion{actual: actual}
}

func (g gomegaInstance) Eventually(ctx context.Context, args ...interface{}) AsyncAssertion {
	return newAsyncAssertion(ctx, args, false)
}

func (g gomegaInstance) Consistently(ctx context.Context, args ...interface{}) AsyncAssertion {
	return newAsyncAssertion(ctx, args, true)
}

func newG() (*FailureError, gomega.Gomega) {
	var failure FailureError
	g := gomega.NewGomega(func(msg string, callerSkip ...int) {
		failure = FailureError{
			msg: msg,
		}
	})

	return &failure, g
}

type assertion struct {
	actual interface{}
}

func (a assertion) Should(matcher types.GomegaMatcher) error {
	err, g := newG()
	if !g.Expect(a.actual).Should(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a assertion) ShouldNot(matcher types.GomegaMatcher) error {
	err, g := newG()
	if !g.Expect(a.actual).ShouldNot(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a assertion) To(matcher types.GomegaMatcher) error {
	err, g := newG()
	if !g.Expect(a.actual).To(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a assertion) ToNot(matcher types.GomegaMatcher) error {
	err, g := newG()
	if !g.Expect(a.actual).ToNot(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a assertion) NotTo(matcher types.GomegaMatcher) error {
	err, g := newG()
	if !g.Expect(a.actual).NotTo(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

type asyncAssertion struct {
	ctx          context.Context
	args         []interface{}
	timeout      time.Duration
	interval     time.Duration
	consistently bool
}

func newAsyncAssertion(ctx context.Context, args []interface{}, consistently bool) asyncAssertion {
	return asyncAssertion{
		ctx:  ctx,
		args: args,
		// 使用 PodStart 作为默认值，因为等待 Pod 是
		// 最常见的操作。
		timeout:      TestContext.timeouts.PodStart,
		interval:     TestContext.timeouts.Poll,
		consistently: consistently,
	}
}

func (a asyncAssertion) newAsync() (*FailureError, gomega.AsyncAssertion) {
	err, g := newG()
	var assertion gomega.AsyncAssertion
	if a.consistently {
		assertion = g.Consistently(a.ctx, a.args...)
	} else {
		assertion = g.Eventually(a.ctx, a.args...)
	}
	assertion = assertion.WithTimeout(a.timeout).WithPolling(a.interval)
	return err, assertion
}

func (a asyncAssertion) Should(matcher types.GomegaMatcher) error {
	err, assertion := a.newAsync()
	if !assertion.Should(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a asyncAssertion) ShouldNot(matcher types.GomegaMatcher) error {
	err, assertion := a.newAsync()
	if !assertion.ShouldNot(matcher) {
		err.backtrace()
		return *err
	}
	return nil
}

func (a asyncAssertion) WithTimeout(timeout time.Duration) AsyncAssertion {
	a.timeout = timeout
	return a
}

func (a asyncAssertion) WithPolling(interval time.Duration) AsyncAssertion {
	a.interval = interval
	return a
}

// FailureError 是一种错误，其错误字符串旨在直接传给
// ginkgo.Fail，即不需要添加诸如 "unexpected error" 的前缀。
// 也不必要 dump 错误结构体。
type FailureError struct {
	msg            string
	fullStackTrace string
}

func (f FailureError) Error() string {
	return f.msg
}

func (f FailureError) Backtrace() string {
	return f.fullStackTrace
}

func (f FailureError) Is(target error) bool {
	return target == ErrFailure
}

func (f *FailureError) backtrace() {
	f.fullStackTrace = ginkgotypes.NewCodeLocationWithStackTrace(2).FullStackTrace
}

// ErrFailure 是一个空错误，可包装它以指示某个错误
// 是 FailureError。它也可用于测试 FailureError：
//
// 如 [gomega.Expect] 中一样，explain 参数可用于提供
//
//	...
//	err := someOperation()
//	if errors.Is(err, ErrFailure) {
//	    ...
//	}
var ErrFailure error = FailureError{}

// ExpectNoError 检查 "err" 是否设置，如果设置则在记录错误的同时使断言失败。
//
// 与 [gomega.Expect] 中一样，explain 参数可用于提供
// 通过以下两种方式之一提供失败时的附加信息：
//   - 单个字符串直接用作失败消息的第一行。
//   - 带附加参数的字符串通过 [fmt.Sprintf] 处理。
func ExpectNoError(err error, explain ...interface{}) {
	ExpectNoErrorWithOffset(1, err, explain...)
}

// ExpectNoErrorWithOffset 检查 "err" 是否设置，如果设置则在比调用方高 "offset" 层的位置记录错误并使断言失败
// （例如调用链 f -> g -> ExpectNoErrorWithOffset(1, ...) 时，错误将记录为来自 "f"）。
//
// 与 [gomega.Expect] 中一样，explain 参数可用于提供
// 通过以下两种方式之一提供失败时的附加信息：
//   - 单个字符串直接用作失败消息的第一行。
//   - 带附加参数的字符串通过 [fmt.Sprintf] 处理。
func ExpectNoErrorWithOffset(offset int, err error, explain ...interface{}) {
	if err == nil {
		return
	}

	// 错误通常包含未导出的字段。因此必须使用
	// 能打印这些字段的格式化器。
	prefix := ""
	if len(explain) > 0 {
		if str, ok := explain[0].(string); ok {
			prefix = fmt.Sprintf(str, explain[1:]...) + ": "
		} else {
			prefix = fmt.Sprintf("unexpected explain arguments, need format string: %v", explain)
		}
	}

	// 这里刻意不使用 gomega.Expect，而是
	// 完全控制哪些信息展示在哪里：
	// - 记录完整的错误对象，因为它可能包含
	//   未包含在其 error 字符串中的附加信息
	//   string.
	// - 不把它放进失败消息，因为
	//   它可能让失败消息变得很大，和/或
	//   影响错误聚合效果：两处
	//   同一代码行的失败可能无法在
	//   https://go.k8s.io/triage 中匹配，因为错误细节过于
	//   different.
	//
	// 有些错误把所有相关信息都放在 Error 字符串里。
	// 对这类错误可以跳过冗余的日志消息。
	// 对于我们自己制造的失败，只记录额外的栈回溯，
	// 因为它不包含在失败消息中。
	var failure FailureError
	if errors.As(err, &failure) && failure.Backtrace() != "" {
		log(offset+1, fmt.Sprintf("Failed inside E2E framework:\n    %s", strings.ReplaceAll(failure.Backtrace(), "\n", "\n    ")))
	} else if !errors.Is(err, ErrFailure) {
		log(offset+1, fmt.Sprintf("Unexpected error: %s\n%s", prefix, format.Object(err, 1)))
	}
	Fail(prefix+err.Error(), 1+offset)
}
