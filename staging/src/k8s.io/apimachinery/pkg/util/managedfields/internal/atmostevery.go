/*
Copyright 2020 The Kubernetes Authors.

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

package internal

import (
	"sync"
	"time"
)

// AtMostEvery 保证方法在任意指定时间窗口内最多执行一次。
// 典型用途是限制错误日志或昂贵操作的频率；它通过互斥锁保证并发安全，
// 因此多个 goroutine 同时调用时也只会有一个成功。
type AtMostEvery struct {
	delay    time.Duration
	lastCall time.Time
	mutex    sync.Mutex
}

// NewAtMostEvery 创建一个 AtMostEvery，按给定间隔限制方法执行频率。
func NewAtMostEvery(delay time.Duration) *AtMostEvery {
	return &AtMostEvery{
		delay: delay,
	}
}

// updateLastCall 在允许执行时更新 lastCall 并返回 true；距离上次调用太近则返回 false。
// 检查与更新必须在同一把锁内完成，否则并发调用可能同时通过检查。
func (s *AtMostEvery) updateLastCall() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if time.Since(s.lastCall) < s.delay {
		return false
	}
	s.lastCall = time.Now()
	return true
}

// Do 在距上次执行已过去足够时间时运行 fn 并返回 true；
// 否则不执行任何操作并返回 false。
// 注意：fn 在锁外执行，避免长时间持锁；这也意味着两个成功调用之间仍可能并发重叠，
// 只是触发频率受到限制。
func (s *AtMostEvery) Do(fn func()) bool {
	if !s.updateLastCall() {
		return false
	}
	fn()
	return true
}
