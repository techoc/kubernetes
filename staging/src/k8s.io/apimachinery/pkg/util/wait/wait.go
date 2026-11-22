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

package wait

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
)

// 对于如下风格的测试：
//
//	...
//	<- time.After(timeout):
//	   t.Errorf("Timed out")
//
// timeout 的取值应当实际上是"永远"。显然我们不希望测试真的永久卡死，但 30s
// 对于在负载很高的机器上可能拖慢运行的因素（GC、磁盘寻道等）而言已足够接近"永远"，
// 又不至于在测试真的出问题时让开发者不得不 ctrl-c。
var ForeverTestTimeout = time.Second * 30

// NeverStop 可传给 Until，使其永不停止。
// 它是一个永不关闭的 channel，因此对应的停止信号永远不会触发。
var NeverStop <-chan struct{} = make(chan struct{})

// Group 用于启动一组 goroutine 并等待它们全部完成。
type Group struct {
	wg sync.WaitGroup
}

// Wait 阻塞直到组内所有 goroutine 结束。
func (g *Group) Wait() {
	g.wg.Wait()
}

// StartWithChannel 在组内启动新 goroutine 运行 f，并把 stopCh 传给 f。
// f 应在 stopCh 可读（被关闭）时停止。
func (g *Group) StartWithChannel(stopCh <-chan struct{}, f func(stopCh <-chan struct{})) {
	g.Start(func() {
		f(stopCh)
	})
}

// StartWithContext 在组内启动新 goroutine 运行 f，并把 ctx 传给 f。
// f 应在 ctx.Done() 可读时停止。
func (g *Group) StartWithContext(ctx context.Context, f func(context.Context)) {
	g.Start(func() {
		f(ctx)
	})
}

// Start 在组内启动新 goroutine 运行 f。
// Add 必须在启动 goroutine 之前调用，否则 Wait 可能在 Done 之前就返回。
func (g *Group) Start(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

// Forever 每隔 period 调用一次 f，永不停止。
//
// Forever 是 Until 的语法糖（等价于 Until(f, period, NeverStop)）。
func Forever(f func(), period time.Duration) {
	Until(f, period, NeverStop)
}

// jitterRand 是专用于抖动计算的随机源。
// 默认使用 rand.Float64，但作为包级变量可以被替换，以便单元测试获得确定性结果。
var jitterRand = rand.Float64

// Jitter 返回 duration 到 duration + maxFactor*duration 之间的随机时长。
//
// 它让客户端避免形成周期性同步行为（例如所有节点同时重试）。
// maxFactor <= 0 时会采用建议的默认值 1.0。
func Jitter(duration time.Duration, maxFactor float64) time.Duration {
	if maxFactor <= 0.0 {
		maxFactor = 1.0
	}
	wait := duration + time.Duration(jitterRand()*maxFactor*float64(duration))
	return wait
}

// ConditionFunc 在条件满足时返回 true；若循环应中止则返回错误。
type ConditionFunc func() (done bool, err error)

// ConditionWithContextFunc 在条件满足时返回 true；若循环应中止则返回错误。
//
// 调用方会传入 context，供条件函数感知取消或超时。
type ConditionWithContextFunc func(context.Context) (done bool, err error)

// WithContext 把 ConditionFunc 转换为 ConditionWithContextFunc（忽略传入的 context）。
func (cf ConditionFunc) WithContext() ConditionWithContextFunc {
	return func(context.Context) (done bool, err error) {
		return cf()
	}
}

// ContextForChannel 提供一个 context：当 parentCh 被关闭时，它被视为已取消。
// 当且仅当 parentCh 已关闭时，Err() 返回 context.Canceled。
func ContextForChannel(parentCh <-chan struct{}) context.Context {
	return channelContext{stopCh: parentCh}
}

// 编译期断言：channelContext 实现 context.Context。
var _ context.Context = channelContext{}

// channelContext 在 stopCh 关闭时表现得如同 context 被取消。
// 它没有 deadline 与 value，只提供取消语义，因而适合把 channel 适配成 context。
type channelContext struct {
	stopCh <-chan struct{}
}

// Done 直接返回 stopCh：关闭即代表取消。
func (c channelContext) Done() <-chan struct{} { return c.stopCh }

// Err 在 stopCh 已关闭时返回 context.Canceled，否则返回 nil。
// 用 select 的非阻塞 default 分支判断，避免阻塞调用方。
func (c channelContext) Err() error {
	select {
	case <-c.stopCh:
		return context.Canceled
	default:
		return nil
	}
}

// Deadline 永远返回零值：channelContext 不支持超时。
func (c channelContext) Deadline() (time.Time, bool) { return time.Time{}, false }

// Value 永远返回 nil：channelContext 不携带任何值。
func (c channelContext) Value(key any) any { return nil }

// runConditionWithCrashProtection 在崩溃保护下运行 ConditionFunc。
// 条件函数中的 panic 会被 HandleCrash 捕获并记录，而不会让进程崩溃。
//
// Deprecated: 遗留轮询方法移除后将一并删除。
func runConditionWithCrashProtection(condition ConditionFunc) (bool, error) {
	//nolint:logcheck // Already deprecated.
	defer runtime.HandleCrash()
	return condition()
}

// runConditionWithCrashProtectionWithContext 在崩溃保护下运行 ConditionWithContextFunc。
//
// Deprecated: 遗留轮询方法移除后将一并删除。
func runConditionWithCrashProtectionWithContext(ctx context.Context, condition ConditionWithContextFunc) (bool, error) {
	defer runtime.HandleCrashWithContext(ctx)
	return condition(ctx)
}

// waitFunc 创建一个 channel：每次应执行测试时收到一个元素，最后一次执行后 channel 关闭。
//
// Deprecated: 将在未来版本中移除，改用 loopConditionUntilContext。
type waitFunc func(done <-chan struct{}) <-chan struct{}

// WithContext 把 waitFunc 转换为等价的 waitWithContextFunc（用 ctx.Done() 作为完成信号）。
func (w waitFunc) WithContext() waitWithContextFunc {
	return func(ctx context.Context) <-chan struct{} {
		return w(ctx.Done())
	}
}

// waitWithContextFunc 创建一个 channel：每次应执行测试时收到一个元素，最后一次执行后关闭。
//
// 当指定 context 被取消或到期时，函数会停止发送元素并立即返回。
//
// Deprecated: 将在未来版本中移除，改用 loopConditionUntilContext。
type waitWithContextFunc func(ctx context.Context) <-chan struct{}

// waitForWithContext 按 'wait' 的节奏持续检查 'fn'。
//
// waitForWithContext 从 'wait()' 取得一个 channel，然后对 channel 上出现的每个值
// 调用一次 'fn'，并在 channel 关闭后再调用一次。若 channel 已关闭而 'fn'
// 无错误地返回 false，则 waitForWithContext 返回 ErrWaitTimeout。
//
// 若 'fn' 返回错误，循环结束并返回该错误；若 'fn' 返回 true，循环结束并返回 nil。
//
// 若 ctx.Done() 已关闭而 fn 从未返回 true，则返回 context.Canceled。
//
// 当 ctx.Done() 关闭时，由于 Go 的 `select` 语句是"均匀伪随机"的，
// `fn` 仍可能再运行一次或多次，但 waitForWithContext 最终一定会返回。
//
// Deprecated: 将在未来版本中移除，改用 loopConditionUntilContext。
func waitForWithContext(ctx context.Context, wait waitWithContextFunc, fn ConditionWithContextFunc) error {
	// 用独立的 waitCtx 驱动 wait，使函数返回时（defer cancel）能终止 wait 的发送 goroutine，
	// 而不会影响调用方传入的 ctx。
	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := wait(waitCtx)
	for {
		select {
		case _, open := <-c:
			// 即使 channel 已关闭（open=false）也要再执行一次 fn，
			// 这是该 API 的既有约定。
			ok, err := runConditionWithCrashProtectionWithContext(ctx, fn)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			if !open {
				return ErrWaitTimeout
			}
		case <-ctx.Done():
			// 返回 ctx.Err() 会破坏向后兼容，需要错误语义的调用方应改用
			// 新的 PollUntilContext* 系列方法。
			return ErrWaitTimeout
		}
	}
}
