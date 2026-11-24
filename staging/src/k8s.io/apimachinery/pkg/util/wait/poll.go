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
)

// PollUntilContextCancel 按固定间隔执行条件函数，直到条件成功、返回错误或 context 被取消/超时。
// 默认先等待一个 interval 再执行；immediate=true 时先立即执行一次。
// 若条件执行时间超过 interval，下一轮会立即开始；极短间隔下，取消被观察到前可能多执行几次。
func PollUntilContextCancel(ctx context.Context, interval time.Duration, immediate bool, condition ConditionWithContextFunc) error {
	return loopConditionUntilContext(ctx, Backoff{Duration: interval}.Timer(), immediate, false, condition)
}

// PollUntilContextTimeout 为轮询额外创建带超时的 context，超过 timeout 后自动终止。
// 条件成功或提前返回时会通过 cancel 释放定时器资源；其他执行语义与 PollUntilContextCancel 相同。
func PollUntilContextTimeout(ctx context.Context, interval, timeout time.Duration, immediate bool, condition ConditionWithContextFunc) error {
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, timeout)
	defer deadlineCancel()
	return loopConditionUntilContext(deadlineCtx, Backoff{Duration: interval}.Timer(), immediate, false, condition)
}

// Poll 按固定间隔检查条件，直到条件成功、返回错误或达到 timeout。
// 首次检查前一定先等待 interval，且至少执行一次。
// 已弃用：该接口不能返回 context 的真实取消原因，请改用 PollUntilContextTimeout。
func Poll(interval, timeout time.Duration, condition ConditionFunc) error {
	return PollWithContext(context.Background(), interval, timeout, condition.WithContext())
}

// PollWithContext 在指定 context 和 timeout 的共同约束下按固定间隔检查条件。
// 首次检查前先等待 interval，至少执行一次；context 取消与 timeout 谁先发生就由谁终止。
// 已弃用：请使用 PollUntilContextTimeout，以获得明确的 context 错误语义。
func PollWithContext(ctx context.Context, interval, timeout time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, false, poller(interval, timeout), condition)
}

// PollUntil 按固定间隔检查条件，直到条件成功、返回错误或 stopCh 被关闭。
// 首次检查前先等待 interval，且至少执行一次。
// 已弃用：请使用 PollUntilContextCancel，并将停止信号转换为 context。
func PollUntil(interval time.Duration, condition ConditionFunc, stopCh <-chan struct{}) error {
	return PollUntilWithContext(ContextForChannel(stopCh), interval, condition.WithContext())
}

// PollUntilWithContext 按固定间隔检查条件，直到条件成功、返回错误或 context 结束。
// 首次检查前先等待 interval，且至少执行一次。
// 已弃用：请使用 PollUntilContextCancel。
func PollUntilWithContext(ctx context.Context, interval time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, false, poller(interval, 0), condition)
}

// PollInfinite 按固定间隔无限检查条件，直到条件成功或返回错误。
// 首次检查前先等待 interval；条件执行过久时可能跳过部分预定间隔。
// 已弃用：请使用 PollUntilContextCancel 并传入不会自动取消的 context。
func PollInfinite(interval time.Duration, condition ConditionFunc) error {
	return PollInfiniteWithContext(context.Background(), interval, condition.WithContext())
}

// PollInfiniteWithContext 按固定间隔持续检查条件，直到条件成功、返回错误或 context 结束。
// 首次检查前先等待 interval；条件执行过久时可能跳过部分预定间隔。
// 已弃用：请使用 PollUntilContextCancel。
func PollInfiniteWithContext(ctx context.Context, interval time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, false, poller(interval, 0), condition)
}

// PollImmediate 先立即检查条件，再按 interval 重试，直到成功、出错或达到 timeout。
// 即使 timeout 很短，也保证至少执行一次条件检查。
// 已弃用：请使用 PollUntilContextTimeout，并设置 immediate=true。
func PollImmediate(interval, timeout time.Duration, condition ConditionFunc) error {
	return PollImmediateWithContext(context.Background(), interval, timeout, condition.WithContext())
}

// PollImmediateWithContext 先立即检查条件，再按 interval 重试，直到成功、出错或 context/timeout 结束。
// 保证至少执行一次条件检查；context 与 timeout 谁先结束就返回对应结果。
// 已弃用：请使用 PollUntilContextTimeout，并设置 immediate=true。
func PollImmediateWithContext(ctx context.Context, interval, timeout time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, true, poller(interval, timeout), condition)
}

// PollImmediateUntil 先立即检查条件，再按 interval 重试，直到成功、出错或 stopCh 关闭。
// 保证至少执行一次条件检查。
// 已弃用：请使用 PollUntilContextCancel，并设置 immediate=true。
func PollImmediateUntil(interval time.Duration, condition ConditionFunc, stopCh <-chan struct{}) error {
	return PollImmediateUntilWithContext(ContextForChannel(stopCh), interval, condition.WithContext())
}

// PollImmediateUntilWithContext 先立即检查条件，再按 interval 重试，直到成功、出错或 context 结束。
// 保证至少执行一次条件检查。
// 已弃用：请使用 PollUntilContextCancel，并设置 immediate=true。
func PollImmediateUntilWithContext(ctx context.Context, interval time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, true, poller(interval, 0), condition)
}

// PollImmediateInfinite 立即执行首次条件检查，之后按 interval 无限重试，直到成功或出错。
// 已弃用：请使用 PollUntilContextCancel，并设置 immediate=true。
func PollImmediateInfinite(interval time.Duration, condition ConditionFunc) error {
	return PollImmediateInfiniteWithContext(context.Background(), interval, condition.WithContext())
}

// PollImmediateInfiniteWithContext 立即执行首次条件检查，之后按 interval 重试，直到成功、出错或 context 结束。
// 已弃用：请使用 PollUntilContextCancel，并设置 immediate=true。
func PollImmediateInfiniteWithContext(ctx context.Context, interval time.Duration, condition ConditionWithContextFunc) error {
	return poll(ctx, true, poller(interval, 0), condition)
}

// poll 是旧版 Poll 系列接口共用的内部循环。
// immediate 决定是否先执行条件；wait 决定每次触发时机及总等待时长；condition 执行实际检查。
// 该函数保留旧接口的超时错误行为，新代码应使用 loopConditionUntilContext。
func poll(ctx context.Context, immediate bool, wait waitWithContextFunc, condition ConditionWithContextFunc) error {
	// 立即模式保证先至少调用一次条件函数，再进入等待。
	if immediate {
		done, err := runConditionWithCrashProtectionWithContext(ctx, condition)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

	select {
	case <-ctx.Done():
		// 直接返回 ctx.Err() 会破坏旧接口的向后兼容性；需要真实 context 错误的调用方应改用新的 PollUntilContext* 方法。
		// 已弃用的 Poll* 系列在截止时间到达和 context 取消时都返回 ErrWaitTimeout；
		// 只有新的 context 感知接口会暴露真实的中断原因。
		return ErrWaitTimeout
	default:
		return waitForWithContext(ctx, wait, condition)
	}
}

// poller 创建周期性发送通知的 waitFunc，并在 timeout 到达后关闭通知通道。
// interval 极短时可能来不及发送任何 tick；timeout=0 表示无限运行，此时调用方必须负责关闭 done，
// 否则内部 goroutine 会一直存活。通道不缓冲，接收方未及时读取时本次 tick 会被丢弃。
func poller(interval, timeout time.Duration) waitWithContextFunc {
	return waitWithContextFunc(func(ctx context.Context) <-chan struct{} {
		ch := make(chan struct{})

		// ticker 在独立 goroutine 中运行，因此必须确保非零 timeout 或 context 取消时能够终止该 goroutine，避免泄漏。
		go func() {
			defer close(ch)

			tick := time.NewTicker(interval)
			defer tick.Stop()

			var after <-chan time.Time
			if timeout != 0 {
				// time.After 虽然使用方便，但如果提前退出，底层计时器可能仍会存活较长时间。
				timer := time.NewTimer(timeout)
				after = timer.C
				defer timer.Stop()
			}

			for {
				select {
				case <-tick.C:
					// 如果消费者尚未准备好接收本次信号，则丢弃该 tick，继续检查其他通道。
					select {
					case ch <- struct{}{}:
					default:
					}
				case <-after:
					return
				case <-ctx.Done():
					return
				}
			}
		}()

		return ch
	})
}
