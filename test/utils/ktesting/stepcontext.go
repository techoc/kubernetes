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

// WithStep 创建一个 context，其中会给所有错误和日志
// 消息加上前缀，类似于错误的包装。这可以嵌套，留下
// 一串"面包屑"线索，帮助定位测试中问题发生的位置，
// 或为什么会产生某些日志输出：
//
//	ERROR: bake cake: set heat for baking: oven not found
//
// 该字符串应描述即将进行的操作（"starting
// the controller"、"list items"），或所操作的对象（"HTTP server"）。
// 多个不同的前缀会以冒号连接。
func (tCtx TContext) WithStep(step string) TContext {
	tCtx.steps += step + ": "
	return tCtx
}

// 当带有步骤信息的 context 会被多次使用时，
// Step 很有用：
//
//	ktesting.Step(tCtx, "step 1", func(tCtx ktesting.TContext) {
//	 tCtx.Log(...)
//	    if (... ) {
//	       tCtx.Failf(...)
//	    }
//	)}
//
// 在回调中，tCtx 变量是已经添加了步骤的那个。
// 这样就不必引入多个不同的 context 变量，
// 也避免了用错变量的风险。
func (tCtx TContext) Step(step string, cb func(tCtx TContext)) {
	tCtx.Helper()
	cb(tCtx.WithStep(step))
}

// Value 会拦截对特殊 "GINKGO_SPEC_CONTEXT" 的查找，
// 并包装底层 reporter，使步骤在报告中可见。
func (tCtx TContext) Value(key any) any {
	if tCtx.steps != "" {
		if s, ok := key.(string); ok && s == ginkgoSpecContextKey {
			if reporter, ok := tCtx.Context.Value(key).(ginkgoReporter); ok {
				return ginkgoReporter(&stepReporter{reporter: reporter, steps: tCtx.steps})
			}
		}
	}
	return tCtx.Context.Value(key)
}

type stepReporter struct {
	reporter ginkgoReporter
	steps    string
}

var _ ginkgoReporter = &stepReporter{}

func (s *stepReporter) AttachProgressReporter(reporter func() string) func() {
	return s.reporter.AttachProgressReporter(func() string {
		report := reporter()
		return s.steps + report
	})
}
