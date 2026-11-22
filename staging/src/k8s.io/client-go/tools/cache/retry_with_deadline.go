/*
Copyright 2022 The Kubernetes Authors.

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

package cache

import (
	"time"

	"k8s.io/utils/clock"
)

// RetryWithDeadline 根据错误发生的时间窗口决定是否继续重试。
// 它不负责睡眠：调用方在 ShouldRetry 返回 true 时自行退避后重试。
type RetryWithDeadline interface {
	// After 记录一次错误；只有 isRetryable 认可的错误才会参与时间窗口计算。
	After(error)
	// ShouldRetry 报告当前是否仍在允许的重试窗口内。
	ShouldRetry() bool
}

// retryWithDeadlineImpl 维护首个与最近一个可重试错误的时间戳。
type retryWithDeadlineImpl struct {
	firstErrorTime   time.Time
	lastErrorTime    time.Time
	maxRetryDuration time.Duration
	minResetPeriod   time.Duration
	isRetryable      func(error) bool
	clock            clock.Clock
}

// NewRetryWithDeadline 构造重试窗口跟踪器。
// maxRetryDuration 为从首个可重试错误起允许重试的总时长；minResetPeriod 为
// 两次错误间隔超过多久时重置窗口，避免偶发错误累积成永久失败。
func NewRetryWithDeadline(maxRetryDuration, minResetPeriod time.Duration, isRetryable func(error) bool, clock clock.Clock) RetryWithDeadline {
	return &retryWithDeadlineImpl{
		firstErrorTime:   time.Time{},
		lastErrorTime:    time.Time{},
		maxRetryDuration: maxRetryDuration,
		minResetPeriod:   minResetPeriod,
		isRetryable:      isRetryable,
		clock:            clock,
	}
}

// reset 清空时间窗口，下一次可重试错误将重新开始计时。
func (r *retryWithDeadlineImpl) reset() {
	r.firstErrorTime = time.Time{}
	r.lastErrorTime = time.Time{}
}

// After 实现 RetryWithDeadline。
func (r *retryWithDeadlineImpl) After(err error) {
	if r.isRetryable(err) {
		// 距上次错误已超过 minResetPeriod，视为新的一段故障，重新计时。
		if r.clock.Now().Sub(r.lastErrorTime) >= r.minResetPeriod {
			r.reset()
		}

		// 只记录第一个可重试错误的时间，作为窗口起点。
		if r.firstErrorTime.IsZero() {
			r.firstErrorTime = r.clock.Now()
		}
		r.lastErrorTime = r.clock.Now()
	}
}

// ShouldRetry 实现 RetryWithDeadline。maxRetryDuration<=0 表示永久禁用重试。
func (r *retryWithDeadlineImpl) ShouldRetry() bool {
	if r.maxRetryDuration <= time.Duration(0) {
		return false
	}

	if r.clock.Now().Sub(r.firstErrorTime) <= r.maxRetryDuration {
		return true
	}

	r.reset()
	return false
}
