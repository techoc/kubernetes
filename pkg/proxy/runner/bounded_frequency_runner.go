/*
Copyright 2017 The Kubernetes Authors.

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

// Package runner 提供按频率约束调度周期任务的辅助工具。
package runner

import (
	"fmt"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// BoundedFrequencyRunner 管理用户提供的任务函数的执行频率。
type BoundedFrequencyRunner struct {
	name string // 本实例的名字，用于日志

	minInterval   time.Duration // 两次执行之间的最小间隔
	retryInterval time.Duration // 执行失败后到重试之间的间隔
	maxInterval   time.Duration // 两次执行之间的最大间隔

	run chan struct{} // 容量 1 的通道，用于触发一次异步执行

	fn               func() error // 待执行的任务函数
	minIntervalTimer clock.Timer
	nextRunTimer     clock.Timer // 合并计时器，同时承担 maxInterval 与 retryInterval 的计时职责
	clock            clock.Clock
}

// NewBoundedFrequencyRunner 创建并返回一个新的 BoundedFrequencyRunner，
// 用于管理给定函数 fn 的执行频率。
//
// 该 runner 保证两个性质：
//  1. 最小间隔（minInterval）：一次执行*完成*到下一次执行*开始*之间至少间隔 minInterval。
//     冷却期内的 Run() 调用会合并并延迟到间隔结束，避免突发执行。
//  2. 最大间隔（maxInterval）：fn 保证每个 maxInterval 至少执行一次，
//     即使没有显式调用 Run() 也能周期性执行（例如用于刷新状态）。
//
// maxInterval 必须大于或等于 minInterval，否则本函数会 panic。
//
// 若 fn 返回错误，则最迟在 retryInterval 后再次执行
// （除非 Run() 或 maxInterval 等其他触发让它更早执行）；任何一次成功执行都会取消重试安排。
func NewBoundedFrequencyRunner(name string, fn func() error, minInterval, retryInterval, maxInterval time.Duration) *BoundedFrequencyRunner {
	return construct(name, fn, minInterval, retryInterval, maxInterval, clock.RealClock{})
}

// construct 创建一个依赖项（时钟）可注入的实例，便于测试中使用假时钟。
func construct(name string, fn func() error, minInterval, retryInterval, maxInterval time.Duration, clock clock.Clock) *BoundedFrequencyRunner {
	// 参数约束必须尽早失败：否则计时器的重置顺序会破坏最小间隔保证。
	if maxInterval < minInterval {
		panic(fmt.Sprintf("%s: maxInterval (%v) must be >= minInterval (%v)", name, maxInterval, minInterval))
	}

	bfr := &BoundedFrequencyRunner{
		name: name,
		fn:   fn,

		minInterval:   minInterval,
		retryInterval: retryInterval,
		maxInterval:   maxInterval,

		// run 通道容量为 1，使多个 Run() 调用只保留一个待执行信号。
		run:   make(chan struct{}, 1),
		clock: clock,
	}

	return bfr
}

// Loop 处理周期计时器和执行请求，应当作为 goroutine 调用。
// 每次执行后都会等待 minInterval，确保不会因外部频繁触发而过于密集地运行。
func (bfr *BoundedFrequencyRunner) Loop(stop <-chan struct{}) {
	klog.V(3).InfoS("Loop running", "runner", bfr.name)
	// 循环退出后关闭 run 通道，向调用方表明执行请求将不再被处理；
	// 此后 Run() 的发送会命中 default 分支，不会阻塞。
	defer close(bfr.run)

	bfr.minIntervalTimer = bfr.clock.NewTimer(bfr.minInterval)
	defer bfr.minIntervalTimer.Stop()

	// 用 maxInterval 初始化，保证即使没有显式 Run() 也会周期性执行。
	bfr.nextRunTimer = bfr.clock.NewTimer(bfr.maxInterval)
	defer bfr.nextRunTimer.Stop()

	for {
		select {
		case <-stop:
			klog.V(3).InfoS("Loop stopping", "runner", bfr.name)
			return
		case <-bfr.nextRunTimer.C(): // Wait on the single timer
		case <-bfr.run:
		}

		// 执行前先停止计时器，这样使用假时钟的测试可以通过 HasWaiters() 与之同步；
		// 任务执行完毕后再统一重置。
		bfr.minIntervalTimer.Stop()
		bfr.nextRunTimer.Stop()

		var err error
		// 任务函数崩溃时只记录并继续，避免整个进程因单个任务而退出。
		func() {
			defer utilruntime.HandleCrash()
			err = bfr.fn()
		}()

		// 根据执行结果决定下一次触发间隔：失败时可能缩短到 retryInterval。
		nextInterval := bfr.maxInterval
		if err != nil {
			// 出错时取 retryInterval 与 maxInterval 中较小者，保证及时重试。
			if bfr.retryInterval < nextInterval {
				nextInterval = bfr.retryInterval
			}
			klog.V(3).InfoS("scheduling retry", "runner", bfr.name, "interval", nextInterval, "error", err)
		}
		// 重置两个计时器：minInterval 控制冷却，nextRunTimer 控制下一次触发。
		bfr.minIntervalTimer.Reset(bfr.minInterval)
		bfr.nextRunTimer.Reset(nextInterval)

		// 即使有新的执行请求也要等冷却期结束，这是最小间隔保证的关键。
		select {
		case <-stop:
			klog.V(3).InfoS("Loop stopping", "runner", bfr.name)
			return
		case <-bfr.minIntervalTimer.C():
		}
	}
}

// Run 请求尽快执行任务函数。如果 Loop 没有运行，该请求可能被无限期推迟。
// 一旦队列中已有待执行请求，在其被执行前后续的 Run() 调用都不会再起作用。
func (bfr *BoundedFrequencyRunner) Run() {
	// 非阻塞写法保证 Run() 本身不会因执行中的任务而卡住调用方：
	// 通道为空时写入一个元素作为请求；非空则说明已有请求在等待，直接丢弃。
	select {
	case bfr.run <- struct{}{}:
	default:
	}
}
