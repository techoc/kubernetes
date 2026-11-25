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

package ktesting

import (
	"context"
	"reflect"
	"slices"

	"github.com/onsi/gomega"
	gtypes "github.com/onsi/gomega/types"

	"k8s.io/kubernetes/test/utils/ktesting"
)

// 参见 [ktesting.FailureError]。
type FailureError = ktesting.FailureError

// 参见 [ktesting.ErrFailure]。
var ErrFailure error = ktesting.ErrFailure

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

// 参见 [ktesting.Eventually]。
func (tCtx TContext) Eventually(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(tCtx).Eventually, arg)
}

// 参见 [ktesting.AssertEventually]。
func (tCtx TContext) AssertEventually(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(assertTestingT{tCtx}).Eventually, arg)
}

// 参见 [ktesting.Consistently]。
func (tCtx TContext) Consistently(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(tCtx).Consistently, arg)
}

// 参见 [ktesting.AssertConsistently]。
func (tCtx TContext) AssertConsistently(arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	return tCtx.newAsyncAssertion(gomega.NewWithT(assertTestingT{tCtx}).Consistently, arg)
}

// newAsyncAssertion 必须与基础 ktesting 中对应的 newAsyncAssertion 保持一致，
// 但增加了对本包 TContext 的支持。
func (tCtx TContext) newAsyncAssertion(eventuallyOrConsistently func(actualOrCtx any, args ...any) gomega.AsyncAssertion, arg any) gomega.AsyncAssertion {
	tCtx.Helper()
	v := reflect.ValueOf(arg)
	if v.Kind() != reflect.Func {
		// 交给 Gomega 处理。
		return eventuallyOrConsistently(tCtx, arg)
	}
	t := v.Type()
	if t.NumIn() == 0 || !slices.Contains(supportedContextTypes, t.In(0)) {
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
		var argTCtx any
		var finalize func()
		switch t.In(0) {
		case baseContextType:
			// 构造一个基础的 TContext。
			argTCtx, finalize = tCtx.TContext.WithContext(args[0].Interface().(context.Context)).
				WithCancel().
				WithError(&err)
		case k8sContextType:
			// 构造一个 client-go 的 TContext。
			argTCtx, finalize = tCtx.WithContext(args[0].Interface().(context.Context)).
				WithCancel().
				WithError(&err)
		}
		args[0] = reflect.ValueOf(argTCtx)
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
	contextType           = reflect.TypeFor[context.Context]()
	errorType             = reflect.TypeFor[error]()
	k8sContextType        = reflect.TypeFor[TContext]()
	baseContextType       = reflect.TypeFor[ktesting.TContext]()
	supportedContextTypes = []reflect.Type{k8sContextType, baseContextType}
)
