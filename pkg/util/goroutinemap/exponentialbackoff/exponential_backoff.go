/*
Copyright 2016 The Kubernetes Authors.

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

// Package exponentialbackoff 为 GoRoutineMap 和 NestedPendingOperations
// 提供指数退避逻辑。
package exponentialbackoff

import (
	"fmt"
	"time"
)

const (
	// initialDurationBeforeRetry 是出错后 GoroutineMap 拒绝在同一目标上
	// 启动新操作的初始时长（启用 exponentialBackOffOnError 时生效）。
	// 每次连续出错都会使等待时长翻倍。
	initialDurationBeforeRetry = 500 * time.Millisecond

	// maxDurationBeforeRetry 是 durationBeforeRetry 指数增长的上限。
	// 取值比 2 分钟略多一些，便于从日志中识别出由此常量造成的超时。
	maxDurationBeforeRetry = 2*time.Minute + 2*time.Second
)

// ExponentialBackoff 记录最近一次错误及禁止重试的时长。
type ExponentialBackoff struct {
	lastError           error
	lastErrorTime       time.Time
	durationBeforeRetry time.Duration
}

// SafeToRetry 在距离 lastErrorTime 的退避期尚未结束时返回错误，否则返回 nil。
func (expBackoff *ExponentialBackoff) SafeToRetry(operationName string) error {
	if time.Since(expBackoff.lastErrorTime) <= expBackoff.durationBeforeRetry {
		return NewExponentialBackoffError(operationName, *expBackoff)
	}

	return nil
}

// Update 将退避时长翻倍（首次从初始值起算），并刷新错误及其发生时间。
func (expBackoff *ExponentialBackoff) Update(err *error) {
	if expBackoff.durationBeforeRetry == 0 {
		expBackoff.durationBeforeRetry = initialDurationBeforeRetry
	} else {
		expBackoff.durationBeforeRetry = 2 * expBackoff.durationBeforeRetry
		if expBackoff.durationBeforeRetry > maxDurationBeforeRetry {
			expBackoff.durationBeforeRetry = maxDurationBeforeRetry
		}
	}

	expBackoff.lastError = *err
	expBackoff.lastErrorTime = time.Now()
}

// GenerateNoRetriesPermittedMsg 生成禁止重试的说明信息，包含最早可重试的时间。
func (expBackoff *ExponentialBackoff) GenerateNoRetriesPermittedMsg(operationName string) string {
	return fmt.Sprintf("Operation for %q failed. No retries permitted until %v (durationBeforeRetry %v). Error: %v",
		operationName,
		expBackoff.lastErrorTime.Add(expBackoff.durationBeforeRetry),
		expBackoff.durationBeforeRetry,
		expBackoff.lastError)
}

// NewExponentialBackoffError 创建一个新的指数退避错误。
func NewExponentialBackoffError(
	operationName string, expBackoff ExponentialBackoff) error {
	return exponentialBackoffError{
		operationName: operationName,
		expBackoff:    expBackoff,
	}
}

// IsExponentialBackoff 判断 GoroutineMap 返回的错误是否表示：
// 因启用 exponentialBackOffOnError 且同名操作在退避期内失败过，新操作无法启动。
func IsExponentialBackoff(err error) bool {
	switch err.(type) {
	case exponentialBackoffError:
		return true
	default:
		return false
	}
}

// exponentialBackoffError 是 GoroutineMap 在因退避而拒绝启动新操作时返回的错误：
// 启用了 exponentialBackOffOnError，且同名操作在 durationBeforeRetry 期内失败过。
type exponentialBackoffError struct {
	operationName string
	expBackoff    ExponentialBackoff
}

var _ error = exponentialBackoffError{}

// Error 实现 error 接口，输出操作名、上次失败时间及最早可重试时间。
func (err exponentialBackoffError) Error() string {
	return fmt.Sprintf(
		"Failed to create operation with name %q. An operation with that name failed at %v. No retries permitted until %v (%v). Last error: %q.",
		err.operationName,
		err.expBackoff.lastErrorTime,
		err.expBackoff.lastErrorTime.Add(err.expBackoff.durationBeforeRetry),
		err.expBackoff.durationBeforeRetry,
		err.expBackoff.lastError)
}
