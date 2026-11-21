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

package ipam

import (
	"time"
)

// Timeout 管理某次节点同步操作的重新同步（resync）循环间隔。
// 间隔随操作是否报错而调整：连续出错时按指数退避增长，直至 MaxBackoff 上限；
// 成功一次后重置为常规 Resync 间隔。
type Timeout struct {
	// Resync 是无错误时的默认超时（重新同步）间隔。
	Resync time.Duration
	// MaxBackoff 是处于错误退避状态时的最大超时间隔。
	MaxBackoff time.Duration
	// InitialRetry 是首次报告错误后的初始重试间隔。
	InitialRetry time.Duration

	// errs 记录连续发生的错误次数，成功时清零。
	errs int
	// current 保存当前的退避超时间隔。
	current time.Duration
}

// Update 根据本次操作结果更新超时状态：成功则清零错误次数并恢复 Resync 间隔；
// 失败则首次使用 InitialRetry，之后每次翻倍，直到达到 MaxBackoff。
func (b *Timeout) Update(ok bool) {
	if ok {
		b.errs = 0
		b.current = b.Resync
		return
	}

	b.errs++
	// 第一次出错使用较短的 InitialRetry，避免长时间等待；
	// 之后每次失败翻倍退避，防止在持续故障时疯狂重试压制 API server。
	if b.errs == 1 {
		b.current = b.InitialRetry
		return
	}

	b.current *= 2
	// 退避上限封顶，保证故障恢复后不会无限期延迟下一次同步。
	if b.current >= b.MaxBackoff {
		b.current = b.MaxBackoff
	}
}

// Next 返回下一次操作应使用的超时间隔：无错误时用 Resync，
// 处于退避状态时返回当前计算出的 current。
func (b *Timeout) Next() time.Duration {
	if b.errs == 0 {
		return b.Resync
	}
	return b.current
}
