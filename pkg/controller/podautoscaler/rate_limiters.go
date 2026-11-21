/*
Copyright 2015 The Kubernetes Authors.

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

package podautoscaler

import (
	"time"

	"k8s.io/client-go/util/workqueue"
)

// FixedItemIntervalRateLimiter 对每个条目使用固定的重试间隔，不做指数退避。
type FixedItemIntervalRateLimiter struct {
	interval time.Duration
}

var _ workqueue.TypedRateLimiter[string] = &FixedItemIntervalRateLimiter{}

// NewFixedItemIntervalRateLimiter 创建使用固定间隔的限速器。
func NewFixedItemIntervalRateLimiter(interval time.Duration) workqueue.TypedRateLimiter[string] {
	return &FixedItemIntervalRateLimiter{
		interval: interval,
	}
}

// When 返回该条目的下一次重试延迟，始终为固定间隔。
func (r *FixedItemIntervalRateLimiter) When(item string) time.Duration {
	return r.interval
}

// NumRequeues 返回已重试次数；固定间隔策略不跟踪次数，恒返回 1 作为占位。
func (r *FixedItemIntervalRateLimiter) NumRequeues(item string) int {
	return 1
}

// Forget 表示条目已结束重试；固定间隔策略无需清理状态，故为空实现。
func (r *FixedItemIntervalRateLimiter) Forget(item string) {
}

// NewDefaultHPARateLimiter 创建 HPA 默认的限速器，以 resync 间隔作为固定重试间隔，
// 保证失败的重试节奏与常规同步周期一致。
func NewDefaultHPARateLimiter(interval time.Duration) workqueue.TypedRateLimiter[string] {
	return NewFixedItemIntervalRateLimiter(interval)
}
