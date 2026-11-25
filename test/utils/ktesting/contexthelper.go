/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"time"
)

// cleanupErr 在测试完成后取消 context 时创建 cause。
// 它是一个 context.Canceled 错误。
func cleanupErr(testName string) error {
	return canceledError(fmt.Sprintf("test %s is cleaning up", testName))
}

type canceledError string

func (c canceledError) Error() string { return string(c) }

func (c canceledError) Is(target error) bool {
	return target == context.Canceled
}

// withTimeout 对应 [context.WithTimeout]。与
// [context.WithTimeout] 不同，它会在测试清理时自动取消，在到达
// 截止时间时提供给定的 cause，且其 cancel 函数
// 需要传入 cause。
func withTimeout(ctx context.Context, tb TB, timeout time.Duration, timeoutCause string) (context.Context, func(cause string)) {
	tb.Helper()

	now := time.Now()

	cancelCtx, cancel := context.WithCancelCause(ctx)
	after := time.NewTimer(timeout)
	stopCtx, stop := context.WithCancel(ctx) // 仅供内部使用，不需要 cause。
	done := make(chan struct{})
	tb.Cleanup(func() {
		cancel(cleanupErr(tb.Name()))
		stop()
		// 等待 goroutine 终止。这很重要，因为否则
		// goroutine 可能会在测试*结束之后*通过 tb 输出日志，
		// 从而导致 panic。
		<-done
	})
	go func() {
		defer close(done)
		select {
		case <-stopCtx.Done():
			after.Stop()
			// 这里不需要设置 cause。会使用父 context 的
			// cause 或错误。
		case <-after.C:
			// 使用此 tCtx 的代码在遇到取消时，
			// 可能会、也可能不会记录上面的信息。
			// 但为稳妥起见，最好由我们来记录，
			// 以防万一。
			//
			// 如果能连同调用方的源代码位置一起记录就更好了，
			// 但 testing.Logf 不支持这样做。
			tb.Log(fmt.Sprintf("\nINFO: canceling context: %s\n", timeoutCause))
			cancel(canceledError(timeoutCause))
		}
	}()

	// 确定哪个截止时间更早：我们的还是父 context 的。
	deadline := now.Add(timeout)
	if parentDeadline, ok := ctx.Deadline(); ok {
		if deadline.After(parentDeadline) {
			deadline = parentDeadline
		}
	}

	// 我们始终有截止时间。
	return deadlineContext{Context: cancelCtx, deadline: deadline}, func(cause string) {
		var cancelCause error
		if cause != "" {
			cancelCause = canceledError(cause)
		}
		cancel(cancelCause)
	}
}

type deadlineContext struct {
	context.Context
	deadline time.Time
}

func (d deadlineContext) Deadline() (time.Time, bool) {
	return d.deadline, true
}
