/*
Copyright 2021 The Kubernetes Authors.

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

package eventclock

import (
	"time"

	baseclock "k8s.io/utils/clock"
)

// EventFunc 在给定时刻或之后执行某些工作。
// 给定时刻。
//
// 回调参数是"调度触发时"的时间，而不是函数实际执行的时刻，这样业务逻辑判断
// 时间边界时不受 goroutine 调度延迟影响，测试结果可复现。
type EventFunc func(time.Time)

// EventClock 是一个主动时钟抽象，适用于可用假时钟测试的代码；
// 假时钟自身决定时间如何推进。其时序范式是调用 EventFunc，
// 而非通过 channel 同步，这样假时钟能掌握相关活动何时完成。
//
// 掌握相关活动何时完成。
//
// 之所以用回调而非 channel：假时钟需要在"推进时间"时同步等待所有到期事件执行完毕，
// 才能保证测试断言看到的状态一致；回调让假时钟能直接串行驱动，避免竞态。
// 这也意味着回调不能阻塞，否则会卡住时钟推进。
type Interface interface {
	baseclock.PassiveClock

	// Sleep 在给定时长（或更长）后返回。
	Sleep(d time.Duration)

	// EventAfterDuration 在给定时长（或更长）后调用给定 EventFunc，
	// 并传入发起调用的时刻。
	EventAfterDuration(f EventFunc, d time.Duration)

	// EventAfterTime 在给定时刻或之后调用给定 EventFunc，
	// 并传入发起调用的时刻。
	EventAfterTime(f EventFunc, t time.Time)
}
