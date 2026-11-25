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

// Step 在互斥锁保护下计算下一次退避时长。
// 若 resetInterval 为 0，或距上次退避已超过重置窗口，则先恢复初始 Backoff，再推进一步。
func (b *Backoff) Step() time.Duration {
	if b == nil {
		return 0
	}
	var nextDuration time.Duration
	nextDuration, b.Duration, b.Steps = delay(b.Steps, b.Duration, b.Cap, b.Factor, b.Jitter)
	return nextDuration
}

// DelayFunc 将 Backoff 包装成可反复调用的延迟函数。
// 每次调用都会推进退避状态；返回的函数本身不保证并发安全。
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

// Timer 根据当前 Backoff 创建计时器。固定间隔时使用更高效的固定计时器；
// 配置指数增长或抖动时使用每轮重新计算间隔的可变计时器。
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

// delay 计算一次退避的实际等待时长，以及下一轮使用的基准时长和剩余步数。
// 先应用指数因子并处理最大值，再为本次等待添加随机抖动；抖动不会累积到下一轮基准值。
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

// DelayWithReset 返回一个带静默期重置能力的延迟函数。
// 距离上次调用超过 resetInterval 后，会从 Backoff 的初始参数重新开始计算；
// 内部使用互斥锁保护共享状态，返回函数可供多个 goroutine 并发调用。
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

// Until 按固定周期循环执行 f，直到 stopCh 关闭。
// 这是 JitterUntil 的简化形式：不添加抖动，并采用滑动间隔（每次执行结束后再等待 period）。
func Until(f func(), period time.Duration, stopCh <-chan struct{}) {
	JitterUntil(f, period, 0.0, true, stopCh)
}

// UntilWithContext 按固定周期循环执行 f，直到 context 被取消。
// 不添加抖动，并采用滑动间隔；回调可以通过收到的 context 感知取消。
func UntilWithContext(ctx context.Context, f func(context.Context), period time.Duration) {
	JitterUntilWithContext(ctx, f, period, 0.0, true)
}

// NonSlidingUntil 按固定周期循环执行 f，直到 stopCh 关闭。
// 采用非滑动间隔：计时从本轮回调开始时推进，因此回调耗时会占用 period。
func NonSlidingUntil(f func(), period time.Duration, stopCh <-chan struct{}) {
	JitterUntil(f, period, 0.0, false, stopCh)
}

// NonSlidingUntilWithContext 按固定周期循环执行 f，直到 context 被取消。
// 采用非滑动间隔，回调执行时间计入 period。
func NonSlidingUntilWithContext(ctx context.Context, f func(context.Context), period time.Duration) {
	JitterUntilWithContext(ctx, f, period, 0.0, false)
}

// JitterUntil 按周期循环执行 f，直到 stopCh 关闭，并为每轮等待加入随机抖动。
// jitterFactor>0 时等待时长会随机增加；否则使用固定 period。
// sliding=true 表示回调结束后再等待完整周期；false 表示回调耗时计入周期。
func JitterUntil(f func(), period time.Duration, jitterFactor float64, sliding bool, stopCh <-chan struct{}) {
	BackoffUntil(f, NewJitteredBackoffManager(period, jitterFactor, &clock.RealClock{}), sliding, stopCh)
}

// JitterUntilWithContext 按周期循环执行 f，直到 context 被取消，并为每轮等待加入随机抖动。
// jitterFactor>0 时随机增加等待时长；sliding 决定周期从回调结束还是开始时计算。
func JitterUntilWithContext(ctx context.Context, f func(context.Context), period time.Duration, jitterFactor float64, sliding bool) {
	BackoffUntilWithContext(ctx, f, NewJitteredBackoffManager(period, jitterFactor, &clock.RealClock{}), sliding)
}

// BackoffUntil 按 BackoffManager 提供的动态间隔循环执行 f，直到 stopCh 关闭。
// sliding=true 时回调结束后才计算下一次退避；false 时回调耗时计入等待周期。
func BackoffUntil(f func(), backoff BackoffManager, sliding bool, stopCh <-chan struct{}) {
	BackoffUntilWithContext(ContextForChannel(stopCh), func(context.Context) { f() }, backoff, sliding)
}

// BackoffUntilWithContext 按 BackoffManager 提供的动态间隔循环执行 f，直到 context 被取消。
// 每轮执行前检查取消状态，并在等待阶段同时监听计时器和 context；sliding 控制周期计算起点。
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

// Backoff 返回一个可等待本轮退避时长的底层计时器。
// 首次调用时创建计时器，后续调用复用并重置它；调用方在再次调用前必须排空上次计时器通道。
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

// Deprecated: 移除旧轮询函数时将一并移除。
type exponentialBackoffManagerImpl struct {
	backoff              *Backoff
	backoffTimer         clock.Timer
	lastBackoffStart     time.Time
	initialBackoff       time.Duration
	backoffResetDuration time.Duration
	clock                clock.Clock
}

// NewExponentialBackoffManager 创建指数退避管理器，退避时长逐步增加、受最大值限制并附加抖动。
// 若超过 resetDuration 没有继续退避，则下一次调用会恢复初始时长。
// 已弃用：新代码建议使用 Backoff.DelayWithReset 创建 DelayFunc。
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

// getNextBackoff 返回本轮加入随机抖动后的等待时长；抖动只作用于本轮，不改变基准 duration。
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

// Deprecated: 移除旧轮询函数时将一并移除。
type jitteredBackoffManagerImpl struct {
	clock        clock.Clock
	duration     time.Duration
	jitter       float64
	backoffTimer clock.Timer
}

// NewJitteredBackoffManager 创建固定基准时长的抖动退避管理器。
// jitter>0 时每轮随机增加等待时间，jitter<=0 时不增加抖动。
// 已弃用：新代码建议使用 Backoff{Duration, Jitter}.Timer()。
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

// ExponentialBackoff 使用指数退避重复执行条件检查，直到条件成功、返回错误或耗尽 Steps。
// 每次失败后按 Backoff.Step() 计算等待时长；条件错误会立即返回，步数耗尽则返回 ErrWaitTimeout。
// 该旧接口使用不可取消的 time.Sleep；需要响应取消时应使用 ExponentialBackoffWithContext。
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

// ExponentialBackoffWithContext 使用指数退避执行条件检查，并在检查前及等待期间响应 context 取消。
// 条件成功或返回错误时立即结束；步数耗尽返回 ErrWaitTimeout；取消/超时则返回 context 对应错误。
// 条件调用次数不会超过 Backoff.Steps，最后一次失败后不再额外休眠。
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
