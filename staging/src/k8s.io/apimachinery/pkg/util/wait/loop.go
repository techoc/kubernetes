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

package wait

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
)

// loopConditionUntilContext 按 timer 定义的间隔执行给定条件，直到 context 被取消、
// 条件返回 true 或条件返回错误。
// 若 sliding 为 true，则周期在条件执行之后计算；若为 false，则周期包含条件执行时间。
// 若 immediate 为 false，则在首次调用条件前先等待一个周期；若为 true，
// 则在等待前先调用一次条件，从而保证无论 context 是否已取消，条件至少被执行一次。
// 返回的错误是最后一次条件返回的错误；若 context 被终止，则返回 context 的错误。
//
// 这是 wait 包中所有轮询的公共循环结构，因此把边界语义集中在一处，
// 避免各 Poll/Until 变体出现行为分歧。
func loopConditionUntilContext(ctx context.Context, t Timer, immediate, sliding bool, condition ConditionWithContextFunc) error {
	defer t.Stop()

	var timeCh <-chan time.Time
	doneCh := ctx.Done()

	// 非滑动模式需要先拿到计时通道，但此时还不读取，首轮等待由下面的 immediate 逻辑决定。
	if !sliding {
		timeCh = t.C()
	}

	// if immediate is true the condition is
	// guaranteed to be executed at least once,
	// if we haven't requested immediate execution, delay once
	// immediate 为 true 时条件保证至少执行一次；否则先等待一个周期。
	if immediate {
		// 用闭包加 defer 包住条件调用，使条件内的 panic 经由 HandleCrashWithContext
		// 记录后不会直接击穿整个轮询循环。
		if ok, err := func() (bool, error) {
			defer runtime.HandleCrashWithContext(ctx)
			return condition(ctx)
		}(); err != nil || ok {
			return err
		}
	}

	// 滑动模式下计时从条件执行完毕后才开始，因此在这里才取得通道。
	if sliding {
		timeCh = t.C()
	}

	for {

		// Wait for either the context to be cancelled or the next invocation be called
		// 等待 context 取消或下一次调用到期，两者任一先到即继续。
		select {
		case <-doneCh:
			return ctx.Err()
		case <-timeCh:
		}

		// IMPORTANT: Because there is no channel priority selection in golang
		// it is possible for very short timers to "win" the race in the previous select
		// repeatedly even when the context has been canceled.  We therefore must
		// explicitly check for context cancellation on every loop and exit if true to
		// guarantee that we don't invoke condition more than once after context has
		// been cancelled.
		//
		// 重要：Go 的 select 没有通道优先级，极短的 timer 可能在 context 已取消的情况下
		// 反复"赢得"上面的 select。因此必须在每次循环显式检查取消状态并退出，
		// 保证 context 取消后条件不会再被多调用一次。
		if err := ctx.Err(); err != nil {
			return err
		}

		// 非滑动模式：条件执行前就推进计时器，使周期覆盖条件本身的耗时。
		if !sliding {
			t.Next()
		}
		if ok, err := func() (bool, error) {
			defer runtime.HandleCrashWithContext(ctx)
			return condition(ctx)
		}(); err != nil || ok {
			return err
		}
		// 滑动模式：条件执行完毕后才推进计时器，使相邻两次条件调用之间至少间隔一个完整周期。
		if sliding {
			t.Next()
		}
	}
}
