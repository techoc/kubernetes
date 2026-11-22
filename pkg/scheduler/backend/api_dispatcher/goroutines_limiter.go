/*
Copyright 2025 The Kubernetes Authors.

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

// Package apidispatcher 负责调度器内部的 API 调用分发。
package apidispatcher

import (
	"sync"
)

// goroutinesLimiter 限制同时分发的 goroutine 数量，防止并发 API 调用压垮 apiserver。
type goroutinesLimiter struct {
	lock   sync.RWMutex
	cond   *sync.Cond
	closed bool

	// maxGoroutines 是 APIDispatcher 允许并发分发的 goroutine 上限。
	maxGoroutines int
	// goroutines 是当前正在运行（已获得许可）的 goroutine 数。
	// 必须在 lock 保护下访问。
	goroutines int
}

// newGoroutinesLimiter 创建一个 goroutinesLimiter，并将并发上限设为 maxGoroutines。
func newGoroutinesLimiter(maxGoroutines int) *goroutinesLimiter {
	p := goroutinesLimiter{
		maxGoroutines: maxGoroutines,
	}
	p.cond = sync.NewCond(&p.lock)

	return &p
}

// acquire 尝试占用一个 goroutine 名额；名额用尽时阻塞等待，直到有人释放或 limiter 关闭。
// 占用成功返回 true；limiter 已关闭则返回 false，调用方应放弃分发。
// 使用完毕后必须调用 release 归还名额。
func (gl *goroutinesLimiter) acquire() bool {
	gl.lock.Lock()
	defer gl.lock.Unlock()

	for gl.goroutines >= gl.maxGoroutines {
		if gl.closed {
			return false
		}
		// 在 cond 上等待释放或关闭信号；Wait 会自动释放锁，被唤醒后重新加锁。
		gl.cond.Wait()
	}
	gl.goroutines++
	return true
}

// release 归还一个先前通过 acquire 占用的名额，并广播唤醒等待者。
func (gl *goroutinesLimiter) release() {
	gl.lock.Lock()
	defer gl.lock.Unlock()

	gl.goroutines--
	gl.cond.Broadcast()
}

// close 关闭 limiter，使所有阻塞在 acquire 的调用方立即返回 false 并停止分发。
func (gl *goroutinesLimiter) close() {
	gl.lock.Lock()
	defer gl.lock.Unlock()

	gl.closed = true
	gl.cond.Broadcast()
}
