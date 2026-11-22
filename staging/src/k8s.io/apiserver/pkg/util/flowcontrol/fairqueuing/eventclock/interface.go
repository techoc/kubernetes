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

// EventFunc does some work that needs to be done at or after the
// given time.
//
// 回调参数是"调度触发时"的时间，而不是函数实际执行的时刻，这样业务逻辑判断
// 时间边界时不受 goroutine 调度延迟影响，测试结果可复现。
type EventFunc func(time.Time)

// EventClock is an active clock abstraction for use in code that is
// testable with a fake clock that itself determines how time may be
// advanced.  The timing paradigm is invoking EventFuncs rather than
// synchronizing through channels, so that the fake clock has a handle
// on when associated activity is done.
//
// 之所以用回调而非 channel：假时钟需要在"推进时间"时同步等待所有到期事件执行完毕，
// 才能保证测试断言看到的状态一致；回调让假时钟能直接串行驱动，避免竞态。
// 这也意味着回调不能阻塞，否则会卡住时钟推进。
type Interface interface {
	baseclock.PassiveClock

	// Sleep returns after the given duration (or more).
	Sleep(d time.Duration)

	// EventAfterDuration invokes the given EventFunc after the given duration (or more),
	// passing the time when the invocation was launched.
	EventAfterDuration(f EventFunc, d time.Duration)

	// EventAfterTime invokes the given EventFunc at the given time or later,
	// passing the time when the invocation was launched.
	EventAfterTime(f EventFunc, t time.Time)
}
