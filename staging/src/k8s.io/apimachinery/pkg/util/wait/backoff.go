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
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/clock"
)

// Backoff 保存 Backoff 函数所用的参数。
type Backoff struct {
	// 初始时长。
	Duration time.Duration
	// 在未达到 Steps 与 Cap 限制时，每次迭代把 Duration 乘以 Factor。
	// 不应为负数。
	// jitter 不会影响 duration 参数的更新。
	Factor float64
	// 每次迭代的休眠时长为 duration 加上在 [0, jitter*duration) 区间内
	// 均匀随机选取的额外量。
	Jitter float64
	// Duration 参数还可继续变化的剩余迭代次数（但可能因触达上限而提前停止）。
	// 若不为正，则 Duration 不再变化。与 Factor、Cap 配合用于指数退避。
	Steps int
	// Duration 参数修订值的上限。若乘以 Factor 后会超过该上限，
	// 则把 duration 设为该上限并把 steps 置零。
	Cap time.Duration
}

// Step 返回由原始 Duration 与 Jitter 决定的休眠时长。
// 调用会修改 Backoff 的 Steps 与 Duration。nil 的 Backoff 始终返回零时长。
func (b *Backoff) Step() time.Duration {
	if b == nil {
		return 0
	}
	var nextDuration time.Duration
	nextDuration, b.Duration, b.Steps = delay(b.Steps, b.Duration, b.Cap, b.Factor, b.Jitter)
	return nextDuration
}

// DelayFunc 返回一个函数，用于根据 b 的参数计算下一次等待间隔。
// 它不会修改原始 backoff，但返回的函数只可安全地由单个 goroutine 使用。
func (b Backoff) DelayFunc() DelayFunc {
	// 把字段复制到闭包内，避免修改调用方传入的 Backoff。
	steps := b.Steps
	duration := b.Duration
	cap := b.Cap
	factor := b.Factor
	jitter := b.Jitter

	return func() time.Duration {
		var nextDuration time.Duration
		// jitter 每一步单独应用，不会在多次步骤间累积。
		nextDuration, duration, steps = delay(steps, duration, cap, factor, jitter)
		return nextDuration
	}
}

// Timer 返回适合该 backoff 参数、可与 wait 系列函数配合使用的计时器实现。
func (b Backoff) Timer() Timer {
	// 需要逐步变化或抖动时使用可变计时器；固定时长则复用 Ticker。
	if b.Steps > 1 || b.Jitter != 0 {
		return &variableTimer{new: internalClock.NewTimer, fn: b.DelayFunc()}
	}
	if b.Duration > 0 {
		return &fixedTimer{new: internalClock.NewTicker, interval: b.Duration}
	}
	return newNoopTimer()
}

// delay 实现本包的核心延迟算法。
// 返回本次休眠时长、下一次的基准 duration 以及剩余的 steps。
func delay(steps int, duration, cap time.Duration, factor, jitter float64) (_ time.Duration, next time.Duration, nextSteps int) {
	// steps 非正时不再改变基准 duration。
	if steps < 1 {
		if jitter > 0 {
			return Jitter(duration, jitter), duration, 0
		}
		return duration, duration, 0
	}
	steps--

	// 计算下一步的间隔。
	if factor != 0 {
		next = time.Duration(float64(duration) * factor)
		if cap > 0 && next > cap {
			next = cap
			steps = 0
		}
	} else {
		next = duration
	}

	// 为本步加上 jitter。
	if jitter > 0 {
		duration = Jitter(duration, jitter)
	}

	return duration, next, steps

}

// DelayWithReset 返回一个 DelayFunc，用于计算下一次等待间隔。
// 每隔 resetInterval 就把 backoff 参数重置为初始状态。
// 该方法可安全地被多个 goroutine 调用，但设置 Factor 时所有调用都会推进
// backoff 状态。若 Factor 为零，则等同于调用 b.DelayFunc()，因为没有 Factor
// 时 Steps 不起作用。若 resetInterval 为零，则不执行退避，等同于调用
// factor 和 steps 都为零的 DelayFunc。
func (b Backoff) DelayWithReset(c clock.Clock, resetInterval time.Duration) DelayFunc {
	if b.Factor <= 0 {
		return b.DelayFunc()
	}
	if resetInterval <= 0 {
		b.Steps = 0
		b.Factor = 0
		return b.DelayFunc()
	}
	return (&backoffManager{
		backoff:        b,
		initialBackoff: b,
		resetInterval:  resetInterval,

		clock:     c,
		lastStart: c.Now(),
		timer:     nil,
	}).Step
}

// Until 循环执行 f，直到 stopCh 关闭，每次间隔 period。
//
// Until 是 JitterUntil 的语法糖：jitter 为零且 sliding = true
// （即 period 计时在 f 完成后才开始）。
//
// 上下文日志：支持上下文日志的代码应改用 UntilWithContext。
func Until(f func(), period time.Duration, stopCh <-chan struct{}) {
	JitterUntil(f, period, 0.0, true, stopCh)
}

// UntilWithContext 循环执行 f，直到 context 结束，每次间隔 period。
//
// UntilWithContext 是 JitterUntilWithContext 的语法糖：jitter 为零且
// sliding = true（即 period 计时在 f 完成后才开始）。
func UntilWithContext(ctx context.Context, f func(context.Context), period time.Duration) {
	JitterUntilWithContext(ctx, f, period, 0.0, true)
}

// NonSlidingUntil 循环执行 f，直到 stopCh 关闭，每次间隔 period。
//
// NonSlidingUntil 是 JitterUntil 的语法糖：jitter 为零且 sliding = false
// （即 period 计时与函数开始同时启动）。
//
// 上下文日志：支持上下文日志的代码应改用 NonSlidingUntilWithContext。
func NonSlidingUntil(f func(), period time.Duration, stopCh <-chan struct{}) {
	JitterUntil(f, period, 0.0, false, stopCh)
}

// NonSlidingUntilWithContext 循环执行 f，直到 context 结束，每次间隔 period。
//
// NonSlidingUntilWithContext 是 JitterUntilWithContext 的语法糖：jitter 为零且
// sliding = false（即 period 计时与函数开始同时启动）。
func NonSlidingUntilWithContext(ctx context.Context, f func(context.Context), period time.Duration) {
	JitterUntilWithContext(ctx, f, period, 0.0, false)
}

// JitterUntil 循环执行 f，直到 stopCh 关闭，每次间隔 period。
//
// 若 jitterFactor 为正，每次运行 f 前都会对 period 加抖动。
// 若 jitterFactor 非正，period 保持不变、不加抖动。
//
// 若 sliding 为 true，period 在 f 运行结束后计算；若为 false，period 包含 f 的运行时间。
//
// 关闭 stopCh 即可停止。若 stopCh 已关闭，f 可能不会被调用。
// 不想停止时传入 NeverStop。
//
// 上下文日志：支持上下文日志的代码应改用 JitterUntilWithContext。
func JitterUntil(f func(), period time.Duration, jitterFactor float64, sliding bool, stopCh <-chan struct{}) {
	BackoffUntil(f, NewJitteredBackoffManager(period, jitterFactor, &clock.RealClock{}), sliding, stopCh)
}

// JitterUntilWithContext 循环执行 f，直到 context 结束，每次间隔 period。
//
// 若 jitterFactor 为正，每次运行 f 前都会对 period 加抖动。
// 若 jitterFactor 非正，period 保持不变、不加抖动。
//
// 若 sliding 为 true，period 在 f 运行结束后计算；若为 false，period 包含 f 的运行时间。
//
// 取消 context 即可停止。若 context 已结束，f 可能不会被调用。
func JitterUntilWithContext(ctx context.Context, f func(context.Context), period time.Duration, jitterFactor float64, sliding bool) {
	BackoffUntilWithContext(ctx, f, NewJitteredBackoffManager(period, jitterFactor, &clock.RealClock{}), sliding)
}

// BackoffUntil 循环执行 f，直到 stopCh 关闭，每次间隔由 BackoffManager 决定。
//
// 若 sliding 为 true，period 在 f 运行结束后计算；若为 false，period 包含 f 的运行时间。
//
// 上下文日志：支持上下文日志的代码应改用 BackoffUntilWithContext。
func BackoffUntil(f func(), backoff BackoffManager, sliding bool, stopCh <-chan struct{}) {
	BackoffUntilWithContext(ContextForChannel(stopCh), func(context.Context) { f() }, backoff, sliding)
}

// BackoffUntilWithContext 循环执行 f，直到 context 结束，每次间隔由 BackoffManager 决定。
//
// 若 sliding 为 true，period 在 f 运行结束后计算；若为 false，period 包含 f 的运行时间。
func BackoffUntilWithContext(ctx context.Context, f func(ctx context.Context), backoff BackoffManager, sliding bool) {
	var t clock.Timer
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !sliding {
			t = backoff.Backoff()
		}

		// 用 HandleCrash 包裹 f，避免单次 panic 终止整个循环。
		func() {
			defer runtime.HandleCrashWithContext(ctx)
			f(ctx)
		}()

		if sliding {
			t = backoff.Backoff()
		}

		// 注意：Go 的 select 没有优先级，这里存在竞态：
		// t.C 与 ctx.Done() 同时就绪时可能选中 t.C。为缓解该问题，
		// 每次循环开始都会重新检查 ctx.Done()，避免多执行一次 f()。
		select {
		case <-ctx.Done():
			// 尽量停止计时器；若已触发则先排空通道，避免泄漏。
			if !t.Stop() {
				<-t.C()
			}
			return
		case <-t.C():
		}
	}
}

// backoffManager 以线程安全的方式向调用方提供简单的退避行为。
type backoffManager struct {
	backoff        Backoff
	initialBackoff Backoff
	resetInterval  time.Duration

	clock clock.Clock

	lock      sync.Mutex
	lastStart time.Time
	timer     clock.Timer
}

// Step 返回期望的下一次等待时长，并按 resetInterval 周期性重置退避状态。
func (b *backoffManager) Step() time.Duration {
	b.lock.Lock()
	defer b.lock.Unlock()

	switch {
	case b.resetInterval == 0:
		b.backoff = b.initialBackoff
	case b.clock.Now().Sub(b.lastStart) > b.resetInterval:
		b.backoff = b.initialBackoff
		b.lastStart = b.clock.Now()
	}
	return b.backoff.Step()
}

// Backoff 实现 BackoffManager.Backoff，返回一个计时器供调用方阻塞等待指数退避。
// 第二次调用 Backoff() 前必须先排空上一次返回的计时器。
func (b *backoffManager) Backoff() clock.Timer {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.timer == nil {
		b.timer = b.clock.NewTimer(b.Step())
	} else {
		b.timer.Reset(b.Step())
	}
	return b.timer
}

// Timer 返回一个新的 Timer 实例，与所有其他计时器共享时钟与重置行为。
func (b *backoffManager) Timer() Timer {
	return DelayFunc(b.Step).Timer(b.clock)
}

// BackoffManager 根据底层实现以特定方案管理退避。
type BackoffManager interface {
	// Backoff 返回一个每次调用都会 Reset 的共享 clock.Timer。该方法不保证
	// 多线程安全。调用方应等待 Timer.C() 排空后再继续退避。若在上一个
	// Backoff() 返回的计时器完成前再次调用 Backoff()，第一个计时器不会被排空，
	// 行为不确定。
	Backoff() clock.Timer
}

// Deprecated: Will be removed when the legacy polling functions are removed.
type exponentialBackoffManagerImpl struct {
	backoff              *Backoff
	backoffTimer         clock.Timer
	lastBackoffStart     time.Time
	initialBackoff       time.Duration
	backoffResetDuration time.Duration
	clock                clock.Clock
}

// NewExponentialBackoffManager 返回一个管理指数退避的 manager。每次退避都会加抖动，
// 且不超过给定的最大值。若在 resetDuration 内未调用退避，则重置退避状态。
// 该 manager 用于在上游不健康时降低负载。
//
// Deprecated: 将在旧版 Poll 方法移除时删除。调用方应构造 Backoff 结构体，
// 用 DelayWithReset() 获得会周期性自我重置的 DelayFunc，然后在调用
// wait.BackoffUntil 时使用其 Timer()。
//
// 不要这样：
//
//	bm := wait.NewExponentialBackoffManager(init, max, reset, factor, jitter, clock)
//	...
//	wait.BackoffUntil(..., bm.Backoff, ...)
//
// 而应这样：
//
//	delayFn := wait.Backoff{
//	  Duration: init,
//	  Cap:      max,
//	  Steps:    int(math.Ceil(float64(max) / float64(init))), // now a required argument
//	  Factor:   factor,
//	  Jitter:   jitter,
//	}.DelayWithReset(reset, clock)
//	wait.BackoffUntil(..., delayFn.Timer(), ...)
func NewExponentialBackoffManager(initBackoff, maxBackoff, resetDuration time.Duration, backoffFactor, jitter float64, c clock.Clock) BackoffManager {
	return &exponentialBackoffManagerImpl{
		backoff: &Backoff{
			Duration: initBackoff,
			Factor:   backoffFactor,
			Jitter:   jitter,

			// 当前 wait.Backoff 在 steps 用尽后返回 Backoff.Duration，这并非此处
			// 理想行为；因此把 Steps 设为 max int，假定永远不会用完。
			Steps: math.MaxInt32,
			Cap:   maxBackoff,
		},
		backoffTimer:         nil,
		initialBackoff:       initBackoff,
		lastBackoffStart:     c.Now(),
		backoffResetDuration: resetDuration,
		clock:                c,
	}
}

// getNextBackoff 在超过重置窗口时把退避状态恢复为初始值，并返回下一次等待时长。
func (b *exponentialBackoffManagerImpl) getNextBackoff() time.Duration {
	if b.clock.Now().Sub(b.lastBackoffStart) > b.backoffResetDuration {
		b.backoff.Steps = math.MaxInt32
		b.backoff.Duration = b.initialBackoff
	}
	b.lastBackoffStart = b.clock.Now()
	return b.backoff.Step()
}

// Backoff 实现 BackoffManager.Backoff，返回一个计时器供调用方阻塞等待指数退避。
// 第二次调用 Backoff() 前必须先排空上一次返回的计时器。
func (b *exponentialBackoffManagerImpl) Backoff() clock.Timer {
	if b.backoffTimer == nil {
		b.backoffTimer = b.clock.NewTimer(b.getNextBackoff())
	} else {
		b.backoffTimer.Reset(b.getNextBackoff())
	}
	return b.backoffTimer
}

// Deprecated: Will be removed when the legacy polling functions are removed.
type jitteredBackoffManagerImpl struct {
	clock        clock.Clock
	duration     time.Duration
	jitter       float64
	backoffTimer clock.Timer
}

// NewJitteredBackoffManager 返回一个按给定时长加抖动的 BackoffManager。
// 若 jitter 为负，则不加抖动。
//
// Deprecated: 将在旧版 Poll 方法移除时删除。调用方应构造 Backoff 结构体，
// 并在调用 wait.BackoffUntil 时使用其 Timer()。
//
// 不要这样：
//
//	bm := wait.NewJitteredBackoffManager(duration, jitter, clock)
//	...
//	wait.BackoffUntil(..., bm.Backoff, ...)
//
// 而应这样：
//
//	wait.BackoffUntil(..., wait.Backoff{Duration: duration, Jitter: jitter}.Timer(), ...)
func NewJitteredBackoffManager(duration time.Duration, jitter float64, c clock.Clock) BackoffManager {
	return &jitteredBackoffManagerImpl{
		clock:        c,
		duration:     duration,
		jitter:       jitter,
		backoffTimer: nil,
	}
}

// getNextBackoff 返回加抖动后的下一次等待时长。
func (j *jitteredBackoffManagerImpl) getNextBackoff() time.Duration {
	jitteredPeriod := j.duration
	if j.jitter > 0.0 {
		jitteredPeriod = Jitter(j.duration, j.jitter)
	}
	return jitteredPeriod
}

// Backoff 实现 BackoffManager.Backoff，返回一个计时器供调用方阻塞等待抖动退避。
// 第二次调用 Backoff() 前必须先排空上一次返回的计时器。
func (j *jitteredBackoffManagerImpl) Backoff() clock.Timer {
	backoff := j.getNextBackoff()
	if j.backoffTimer == nil {
		j.backoffTimer = j.clock.NewTimer(backoff)
	} else {
		j.backoffTimer.Reset(backoff)
	}
	return j.backoffTimer
}

// ExponentialBackoff 以指数退避重复执行条件检查。
//
// 它反复检查条件然后休眠，用 `backoff.Step()` 决定休眠时长并调整
// Duration 与 Steps。以下任一情况发生时停止并返回：
// 1. 条件检查返回 true 或错误，
// 2. 已执行 `backoff.Steps` 次条件检查，或
// 3. 完成了一次被时长上限截断的休眠。
// 情况 (1) 返回条件函数给出的错误。
// 其他所有情况返回 ErrWaitTimeout。
//
// 由于退避经常需要被取消，建议使用 ExponentialBackoffWithContext
// 并向方法传入 context。
func ExponentialBackoff(backoff Backoff, condition ConditionFunc) error {
	for backoff.Steps > 0 {
		if ok, err := runConditionWithCrashProtection(condition); err != nil || ok {
			return err
		}
		if backoff.Steps == 1 {
			break
		}
		time.Sleep(backoff.Step())
	}
	return ErrWaitTimeout
}

// ExponentialBackoffWithContext 以指数退避重复执行条件检查。
// 若条件返回错误、context 被取消或超时，或超过 backoff 定义的最大尝试次数
// （ErrWaitTimeout），立即返回错误。条件返回错误时退避立即停止。
// 条件函数的调用次数不会超过 backoff.Steps。
func ExponentialBackoffWithContext(ctx context.Context, backoff Backoff, condition ConditionWithContextFunc) error {
	for backoff.Steps > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if ok, err := runConditionWithCrashProtectionWithContext(ctx, condition); err != nil || ok {
			return err
		}

		// 最后一次尝试后不再休眠，直接以超时结束。
		if backoff.Steps == 1 {
			break
		}

		waitBeforeRetry := backoff.Step()
		// 等待期间也要响应取消，避免不可中断的 sleep。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitBeforeRetry):
		}
	}

	return ErrWaitTimeout
}
