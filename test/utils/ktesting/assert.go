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
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	gtypes "github.com/onsi/gomega/types"
)

// FailureError 是一种错误，其错误字符串应当直接传给
// [TContext.Fatal]，也就是说不需要添加诸如 "unexpected error" 的前缀，
// 也不需要打印整个错误结构体。
type FailureError struct {
	Msg            string
	FullStackTrace string
}

func (f FailureError) Error() string {
	return f.Msg
}

func (f FailureError) Backtrace() string {
	return f.FullStackTrace
}

func (f FailureError) Is(target error) bool {
	return target == ErrFailure
}

// ErrFailure 是一个空错误，可以被包装以表明某个错误是 FailureError。
// 它也可用于判断一个错误是否为 FailureError：
//
//	return fmt.Errorf("some problem%w", ErrFailure)
//	...
//	err := someOperation()
//	if errors.Is(err, ErrFailure) {
//	    ...
//	}
var ErrFailure error = FailureError{}

func gomegaAssertion(tCtx TContext, fatal bool, actual interface{}, extra ...interface{}) gomega.Assertion {
	testingT := gtypes.GomegaTestingT(tCtx)
	if !fatal {
		testingT = assertTestingT{tCtx}
	}
	return gomega.NewWithT(testingT).Expect(actual, extra...)
}

// assertTestingT 使用 TContext.Errorf 实现 Fatalf（Gomega 用来报告失败的唯一函数），
// 也就是说断言失败后测试会继续执行。Helper 方法会被直接透传。
type assertTestingT struct {
	TContext
}

var _ gtypes.GomegaTestingT = assertTestingT{}

func (a assertTestingT) Fatalf(format string, args ...any) {
	a.Helper()
	a.Errorf(format, args...)
}

// ExpectNoError 断言没有发生错误，如果发生错误则测试失败。
//
// 与 [gomega] 中一样，可选的说明（explanation）可以是：
//   - 一个 [fmt.Sprintf] 格式字符串及其参数
//   - 一个返回字符串的函数，只有在需要时才会被延迟调用来构造说明
//
// 如果提供了说明，它将替换失败消息中默认的 "Unexpected error"。
// 与包装错误时一样，它会在末尾加上冒号，再拼接其他细节。
// 因此，说明不应以标点或换行结尾。
//
// 使用 ExpectNoError 而非对应的 Gomega 或 testify 断言的好处是：
// 失败消息很短（便于在 https://go.k8s.io/triage 中聚合），
// 而更多细节会记录在测试日志输出中（便于排查某一次具体的失败）。
//
// 辅助包应当返回派生自 [FailureError] 的错误。
// 这样常规 linter 会强制测试代码检查该错误，
// 并且测试代码应为失败提供额外上下文，就像打印或包装错误时所做的那样：
//
//	tCtx.ExpectNoError(somehelper.CreateSomething(tCtx, ...), "creating the first foobar")
//	tCtx.ExpectNoError(somehelper.CreateSomething(tCtx, ...), "creating the second foobar")
func (tCtx TContext) ExpectNoError(err error, explain ...interface{}) {
	tCtx.Helper()
	tCtx.noError(tCtx.Fatalf, err, explain...)
}

// AssertNoError 是 ExpectNoError 的变体，它报告意外错误但不会中止测试。
// 如果没有发生错误，则返回 true。
func (tCtx TContext) AssertNoError(err error, explain ...interface{}) bool {
	tCtx.Helper()
	return tCtx.noError(tCtx.Errorf, err, explain...)
}

func (tCtx TContext) noError(failf func(format string, args ...any), err error, explain ...interface{}) bool {
	if err == nil {
		return true
	}

	tCtx.Helper()
	description := buildDescription(explain...)

	if errors.Is(err, ErrFailure) {
		var failure FailureError
		if tCtx.capture == nil && errors.As(err, &failure) {
			if backtrace := failure.Backtrace(); backtrace != "" {
				if description != "" {
					tCtx.Log(description)
				}
				tCtx.Logf("Failed at:\n%s", backtrace)
			}
		}
		if description != "" {
			failf("%s: %s", description, err.Error())
			return false
		}
		failf("%s", err.Error())
		return false
	}

	if description == "" {
		description = "Unexpected error"
	}
	if tCtx.capture == nil {
		tCtx.Logf("%s:\n%s", description, format.Object(err, 0))
	}
	failf("%s: %v", description, err.Error())
	return false
}

func buildDescription(explain ...interface{}) string {
	switch len(explain) {
	case 0:
		return ""
	case 1:
		if describe, ok := explain[0].(func() string); ok {
			return describe()
		}
	}
	return fmt.Sprintf(explain[0].(string), explain[1:]...)
}

// Eventually 包装了 [gomega.Eventually]。支持的参数类型有：
//   - 一个函数，其第一个参数为 `tCtx ktesting.TContext` 或 `ctx context.Context`，
//     此外可以带更多参数和任意返回值。
//   - 一个随时间变化的值，通常是 channel。
//
// 对于函数，上下文由 Eventually 提供。其他参数必须通过 WithParameters 传入。
// 第一个返回值会传给 Gomega matcher，其余所有返回值（特别是额外的 error）
// 都必须为 null。作为一种特殊情况，带 `tCtx ktesting.TContext` 参数且没有
// 返回值的函数可以与 gomega.Succeed 这个 matcher 组合使用。
//
// 与直接使用 [gomega.Eventually] 不同，在回调函数中做额外的断言总是可行的，
// 只要这些断言使用传入的 TContext 即可。断言失败会被视为临时失败，
// 因此 Eventually 会继续轮询。这可以用来对一个值做多重断言，
// 而不必编写自定义 matcher：
//
//	cb := func(tCtx ktesting.TContext) {
//	    value, err := doSomething(...)
//	    tCtx.ExpectNoError(err, "something failed")
//	    tCtx.Assert(value.a).To(gomega.Equal(42), "the answer")
//	    tCtx.Assert(value.b).To(gomega.Equal("the fish"), "thanks")
//	}
//	tCtx.Eventually(cb).Should(gomega.Succeed())
//
// 发生失败时测试会停止。如果想继续执行，请使用 AssertEventually。
//
// 这里使用 Gomega 默认的轮询间隔和超时时间。设置特定的超时时间可能很有用：
//
//	tCtx.Eventually(cb).Timeout(5 * time.Second).Should(gomega.Succeed(), "foobar should succeed")
//
// 在回调中取消 context 只会影响回调内部的代码。传给 Eventually 的 context
// 不会被取消。如果已经知道预期条件无法再达成，想立即终止轮询，
// 请使用 [gomega.StopTrying]：
//
//	cb := func(func(tCtx ktesting.TContext) int {
//	    value, err := doSomething(...)
//	    if errors.Is(err, SomeFinalErr) {
//	        // 这条消息会完全替换正常的失败消息，
//	        // 因此应当包含所有相关信息。
//	        //
//	        // github.com/onsi/gomega/format 是格式化任意数据的好方法。
//	        // 它使用缩进，对于 Kubernetes API 结构体还会回退到 YAML
//	        // 以保证可读性。
//	        gomega.StopTrying("permanent failure, last value:\n%s", format.Object(value, 1 /* 缩进一级 */)).
//	            Wrap(err).Now()
//	    }
//	    tCtx.ExpectNoError(err, "something failed")
//	    return value
//	}
//	tCtx.Eventually(cb).Should(gomega.Equal(42), "should be the answer to everything")
//
// 若要在特定超时后再次轮询，请使用 [gomega.TryAgainAfter]。
// 这在 [Consistently] 中忽略间歇性错误时尤其有用。
//
//	cb := func(func(tCtx ktesting.TContext) int {
//	    value, err := doSomething(...)
//	    var intermittentErr SomeIntermittentError
//	    if errors.As(err, &intermittentErr) {
//	        gomega.TryAgainAfter(intermittentErr.RetryPeriod).Wrap(err).Now()
//	    }
//	    tCtx.ExpectNoError(err, "something failed")
//	    return value
//	 }
//	 tCtx.Eventually(cb).Should(gomega.Equal(42), "should be the answer to everything")
func (tCtx TContext) Eventually(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(tCtx).Eventually, arg)
}

// AssertEventually 是 Eventually 的变体，它只记录失败，不停止测试。
func (tCtx TContext) AssertEventually(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(assertTestingT{tCtx}).Eventually, arg)
}

// Consistently 包装 [gomega.Consistently] 的方式与 [Eventually] 包装
// [gomega.Eventually] 的方式相同。
func (tCtx TContext) Consistently(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(tCtx).Consistently, arg)
}

// AssertConsistently 是 Consistently 的变体，它只记录失败，不停止测试。
func (tCtx TContext) AssertConsistently(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(assertTestingT{tCtx}).Consistently, arg)
}

// newAsyncAssertion 必须与 client-go ktesting 中对应的 newAsyncAssertion 保持一致，
// 但不包括那个包中对 TContext 的支持。
func (tCtx TContext) newAsyncAssertion(eventuallyOrConsistently func(actualOrCtx any, args ...any) gomega.AsyncAssertion, arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	// switch arg := arg.(type) {
	// case func(tCtx TContext):
	// 	// 用 reflect 处理起来比较麻烦，所以直接在这里覆盖这种情况……
	// 	return eventuallyOrConsistently(tCtx, func(g gomega.Gomega, ctx context.Context) (err error) {
	// 		tCtx := WithContext(tCtx, ctx)
	// 		tCtx, finalize := WithError(tCtx, &err)
	// 		defer finalize()
	// 		arg(tCtx)
	// 	})
	// default:
	v := reflect.ValueOf(arg)
	if v.Kind() != reflect.Func {
		// 交给 Gomega 处理。
		return eventuallyOrConsistently(tCtx, arg)
	}
	t := v.Type()
	if t.NumIn() == 0 || t.In(0) != tContextType {
		// 不是我们可以包装的函数。
		return eventuallyOrConsistently(tCtx, arg)
	}
	// 构建一个包装函数，用 context 代替 TContext 作为第一个参数。
	// 包装函数被调用时会构造出 TContext，然后调用真正的函数。
	in := make([]reflect.Type, t.NumIn())
	in[0] = contextType
	for i := 1; i < t.NumIn(); i++ {
		in[i] = t.In(i)
	}
	out := make([]reflect.Type, t.NumOut())
	for i := range t.NumOut() {
		out[i] = t.Out(i)
	}
	// 最后一个返回值必须是 error，因为我们需要能够返回断言失败，
	// 所以如果函数本身没有 error 返回值，就必须添加一个。
	addErrResult := t.NumOut() == 0 || t.Out(t.NumOut()-1) != errorType
	if addErrResult {
		out = append(out, errorType)
	}
	wrapperType := reflect.FuncOf(in, out, t.IsVariadic())
	wrapper := reflect.MakeFunc(wrapperType, func(args []reflect.Value) (results []reflect.Value) {
		var err error
		tCtx, finalize := tCtx.WithContext(args[0].Interface().(context.Context)).
			WithCancel().
			WithError(&err)
		args[0] = reflect.ValueOf(tCtx)
		defer func() {
			// 这段逻辑在 finalize 之后执行。
			// 如果是正常返回，就必须把 finalize 设置的 err 值回填。
			if r := recover(); r != nil {
				// 不，不需要结果。
				panic(r)
			}
			errValue := reflect.ValueOf(err)
			if err == nil {
				// reflect 不接受这种情况（"returned zero Value"）。
				// 我们需要一个类型正确的值。
				errValue = reflect.New(errorType).Elem()
			}
			// 如果调用发生了 panic，且该 panic 被 finalize() 恢复，
			// 那么 results 仍然为 nil。我们需要填入空值。
			if len(results) == 0 && t.NumOut() > 0 {
				for t := range t.Outs() {
					results = append(results, reflect.New(t).Elem())
				}
			}
			if addErrResult {
				results = append(results, errValue)
				return
			}
			if results[len(results)-1].IsNil() && err != nil {
				results[len(results)-1] = errValue
			}
		}()
		defer finalize() // 必须直接调用，否则它无法恢复 panic。
		return v.Call(args)
	})
	return eventuallyOrConsistently(tCtx, wrapper.Interface())
}

var (
	contextType  = reflect.TypeFor[context.Context]()
	errorType    = reflect.TypeFor[error]()
	tContextType = reflect.TypeFor[TContext]()
)
