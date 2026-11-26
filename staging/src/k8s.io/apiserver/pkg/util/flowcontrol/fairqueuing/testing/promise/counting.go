/*
Copyright 2019 The Kubernetes Authors.

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

// Package promise 提供带 goroutine 活动计数的 WriteOnce 测试实现。
// 它用于需要精确观测“有多少 goroutine 处于阻塞/活跃”的公平排队测试，
// 与生产实现相比多维护了一个 counter.GoRoutineCounter。
package promise

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/util/flowcontrol/counter"
	promiseifc "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/promise"
)

// countingPromise 实现 WriteOnce 接口。
// 该实现基于条件变量。
// 同时跟踪活跃 goroutine：
// 等待该变量被设置的 goroutine 会使计数减 1，
// 此类 goroutine 解除阻塞时再增加。
// unblocked.
//
// 计数约定：进入 Wait 前 -1，被唤醒时按 waitingCount 一次性 +N，
// 使计数器始终等于“当前可运行的 goroutine 数”。
type countingPromise struct {
	lock          sync.Locker
	cond          sync.Cond
	activeCounter counter.GoRoutineCounter // 活跃 goroutine 计数
	waitingCount  int                      // 因该变量未设置而空闲的 goroutine 数
	isSet         bool
	value         interface{}
}

var _ promiseifc.WriteOnce = &countingPromise{}

// NewCountingWriteOnce 创建一个使用锁并统计 goroutine 活动情况的 WriteOnce。
//
// 最后三个参数与普通 WriteOnce 工厂类似：
// - 可选的初始值，
// - 可选的 "done" channel，
// - "done" channel 可选后设置的值。
// 注意：该实现对 `doneCh` 变为可选的响应不等 Get 调用。
//
// 若 `doneCh != nil`，调用方承诺及时关闭它
// （在 Go 运行时调度器允许的范围内），并在关闭前增加 goroutine 计数。
//
// WriteOnce 的 Get 方法必须在不持有锁的情况下调用。
// WriteOnce 的 Set 方法必须在持有锁的情况下调用。
func NewCountingWriteOnce(activeCounter counter.GoRoutineCounter, lock sync.Locker, initial interface{}, doneCh <-chan struct{}, doneVal interface{}) promiseifc.WriteOnce {
	p := &countingPromise{
		lock:          lock,
		cond:          *sync.NewCond(lock),
		activeCounter: activeCounter,
		// initial 非 nil 即视为已设置，避免调用方额外调用 Set。
		isSet: initial != nil,
		value: initial,
	}
	if doneCh != nil {
		activeCounter.Add(1) // 为紧接着启动的 goroutine 增加计数。
		go func() {
			defer activeCounter.Add(-1) // 为该 goroutine 的完成减少计数。
			defer runtime.HandleCrash()
			// 在 channel receive 阻塞前先“挂起”本 goroutine，防止计数器虚高。
			activeCounter.Add(-1) // 为 channel receive 记录挂起。
			<-doneCh
			// 解除上述 receive 阻塞的 goroutine 必须已为该激活记过账。
			//
			lock.Lock()
			defer lock.Unlock()
			p.Set(doneVal)
		}()
	}
	return p
}

// Get 在未设置时阻塞，并同步调整 goroutine 计数。
// 调用方必须在不持有 lock 的情况下调用，否则 cond.Wait 无法重新获取锁。
func (p *countingPromise) Get() interface{} {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.isSet {
		p.waitingCount++
		p.activeCounter.Add(-1)
		p.cond.Wait()
	}
	return p.value
}

// Set 只生效一次；返回 false 表示此前已被设置（例如先被取消）。
func (p *countingPromise) Set(value interface{}) bool {
	if p.isSet {
		return false
	}
	p.isSet = true
	p.value = value
	// 一次性唤醒所有等待者，并按人数补回活跃计数。
	if p.waitingCount > 0 {
		p.activeCounter.Add(p.waitingCount)
		p.waitingCount = 0
		p.cond.Broadcast()
	}
	return true
}
