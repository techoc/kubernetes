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
	"time"

	"k8s.io/utils/clock"
)

// Timer 抽象等待函数与运行时计时器之间的交互。测试代码可以自行实现该接口；
// 普通调用方建议通过 Backoff 创建 Timer，避免直接操作底层计时器。
// 该接口刻意只暴露 C、Next、Stop 三个方法，隐藏容易误用的 Reset 等操作。
// 同一个 Timer 实例不保证支持多个 goroutine 并发调用。
type Timer interface {
	// C 返回计时器触发时可读的时间通道。调用 Stop 后不得继续等待该通道；
	// 调用方可以在 Timer 生命周期内缓存 C() 的返回值。
	C() <-chan time.Time
	// Next 通知计时器开始计算下一个等待间隔。调用前必须先排空 C() 通道；
	// 调用 Stop 后不得再调用 Next。
	Next()
	// Stop 释放计时器持有的运行时资源；即使此前没有调用其他方法，也可以安全调用。
	Stop()
}

// noopTimer 始终就绪，用于不需要真正等待的路径（如 Backoff 已耗尽）。
type noopTimer struct {
	closedCh <-chan time.Time
}

// newNoopTimer 创建一个立即就绪的计时器，并为其分配独立通道，
// 避免多个互不相关的计时器争用同一个通道锁。
func newNoopTimer() noopTimer {
	ch := make(chan time.Time)
	close(ch)
	return noopTimer{closedCh: ch}
}

func (t noopTimer) C() <-chan time.Time {
	return t.closedCh
}
func (noopTimer) Next() {}
func (noopTimer) Stop() {}

// variableTimer 每次 Next 都重新计算延迟，对应指数退避等场景。
// 惰性创建底层 timer，避免未使用的 Timer 占用运行时资源。
type variableTimer struct {
	fn  DelayFunc
	t   clock.Timer
	new func(time.Duration) clock.Timer
}

func (t *variableTimer) C() <-chan time.Time {
	if t.t == nil {
		d := t.fn()
		t.t = t.new(d)
	}
	return t.t.C()
}
func (t *variableTimer) Next() {
	// 尚未创建 timer 时无需重置；下次 C() 会用新的延迟创建。
	if t.t == nil {
		return
	}
	d := t.fn()
	t.t.Reset(d)
}
func (t *variableTimer) Stop() {
	if t.t == nil {
		return
	}
	t.t.Stop()
	t.t = nil
}

// fixedTimer 使用固定间隔的 Ticker，Next 为空操作。
type fixedTimer struct {
	interval time.Duration
	t        clock.Ticker
	new      func(time.Duration) clock.Ticker
}

func (t *fixedTimer) C() <-chan time.Time {
	if t.t == nil {
		t.t = t.new(t.interval)
	}
	return t.t.C()
}
func (t *fixedTimer) Next() {
	// 固定间隔计时器不需要在每轮调用时重置 Ticker，因此这里不执行任何操作。
}
func (t *fixedTimer) Stop() {
	if t.t == nil {
		return
	}
	t.t.Stop()
	t.t = nil
}

var (
	// RealTimer 暴露真实时钟的 Timer 构造函数，供需要 clock.Timer 的调用方使用。
	RealTimer = clock.RealClock{}.NewTimer
)

var (
	// internalClock 是包内部使用的时钟；测试可以替换它来控制时间推进。
	internalClock = clock.RealClock{}
)
