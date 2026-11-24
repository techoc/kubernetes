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
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// DelayFunc 每次调用都会返回下一次需要等待的时间间隔。
// 返回值可以动态变化，因此可用于实现固定轮询、指数退避和随机抖动等策略。
type DelayFunc func() time.Duration

// Timer 将任意延迟函数包装为 Timer，使计时器能够适应每次调用产生的不同间隔。
// 如果只需要简单退避，优先使用 Backoff{...}.Timer()，其实现更加高效。
func (fn DelayFunc) Timer(c clock.Clock) Timer {
	return &variableTimer{fn: fn, new: c.NewTimer}
}

// Until 按延迟函数提供的间隔反复执行条件函数，直到 context 被取消、条件满足或条件返回错误。
// immediate 控制是否先立即执行一次；sliding 控制等待间隔是否从条件执行结束后开始计算。
func (fn DelayFunc) Until(ctx context.Context, immediate, sliding bool, condition ConditionWithContextFunc) error {
	return loopConditionUntilContext(ctx, &variableTimer{fn: fn, new: internalClock.NewTimer}, immediate, sliding, condition)
}

// Concurrent 返回一个可由多个 goroutine 并发调用的延迟函数包装。
// 内部通过互斥锁串行化对原函数的调用，因此共享该函数的 goroutine 会互相等待；
// 仅当原延迟函数执行很快、且确实需要共享状态时才适合使用。
func (fn DelayFunc) Concurrent() DelayFunc {
	var lock sync.Mutex
	return func() time.Duration {
		lock.Lock()
		defer lock.Unlock()
		return fn()
	}
}
