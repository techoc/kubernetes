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

package cacher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type status int

const (
	// Pending 表示缓存尚未完成首次同步，或同步中断后正在重建。
	Pending status = iota
	// Ready 表示缓存已同步，可以安全地响应读请求。
	Ready
	// Stopped 表示缓存已永久停止，所有等待者都应立即失败。
	Stopped
)

// ready 是一个三态条件变量，未 Stopped 时会阻塞直到 Ready。
// 初始状态为 Pending，其状态机图如下。
//
// Pending <------> Ready -----> Stopped
//
//	|                           ^
//	└---------------------------┘
type ready struct {
	state       status // represent the state of the variable
	lastErr     error
	generation  int           // represent the number of times we have transtioned to ready
	lock        sync.RWMutex  // protect the state and generation variables
	restartLock sync.Mutex    // protect the transition from ready to pending where the channel is recreated
	waitCh      chan struct{} // blocks until is ready or stopped

	clock               clock.Clock
	lastStateChangeTime time.Time
}

func newReady(c clock.Clock) *ready {
	r := &ready{
		waitCh: make(chan struct{}),
		state:  Pending,
		clock:  c,
	}
	r.updateLastStateChangeTimeLocked()

	return r
}

// 状态变为 Ready 或 Stopped 后关闭 channel
//
// 返回通道而不是布尔值，让调用方可以 select 多个事件。持有 restartLock 是为了
// 与 set() 中重建 waitCh 的操作互斥，避免拿到即将被替换的旧通道。
func (r *ready) done() chan struct{} {
	r.restartLock.Lock()
	defer r.restartLock.Unlock()
	return r.waitCh
}

// wait 阻塞直到状态为 Ready 或 Stopped；Stopped 时返回错误。
func (r *ready) wait(ctx context.Context) error {
	_, err := r.waitAndReadGeneration(ctx)
	return err
}

// waitAndReadGenration 阻塞直到状态为 Ready 或 Stopped，
// Ready 时返回进入 ready 状态的次数，否则返回错误。
//
// 返回 generation 用于让调用方判断"我等待期间缓存是否又重建过"，例如避免把
// 旧同步周期计算出的 resourceVersion 当作有效结果。
func (r *ready) waitAndReadGeneration(ctx context.Context) (int, error) {
	for {
		// 仅当状态为 Pending 时 r.done() 才会阻塞
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-r.done():
		}

		r.lock.RLock()
		if r.state == Pending {
			// 因为允许在 Pending 与 Ready 之间切换，
			// 若快速经历 Pending -> Ready -> Pending，
			// 此前等待的进程可能被唤醒后又看到 Pending
			// 状态。此时必须再次等待，
			// 避免系统状态不一致——部分进程在状态回到 Pending 后
			// 仍未被阻塞。
			//
			// 通道已关闭但状态又回到 Pending，说明期间发生了快速的状态往返；
			// 重新循环以获取新的 waitCh 继续等待。
			r.lock.RUnlock()
			continue
		}
		generation, err := r.readGenerationLocked()
		r.lock.RUnlock()
		return generation, err
	}
}

// check 返回状态上次变更至今经过的时间与当前状态值。
func (r *ready) check() (time.Duration, error) {
	_, elapsed, err := r.checkAndReadGeneration()
	return elapsed, err
}

// checkAndReadGeneration 返回当前 generation、状态上次变更至今经过的时间与当前状态值。
func (r *ready) checkAndReadGeneration() (int, time.Duration, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	generation, err := r.readGenerationLocked()
	return generation, r.clock.Since(r.lastStateChangeTime), err
}

// readGenerationLocked 把内部状态翻译成对调用方有意义的 (generation, error)；
// 调用时必须已持有 lock。
func (r *ready) readGenerationLocked() (int, error) {
	switch r.state {
	case Pending:
		// 保留上一次同步失败的原因，便于调用方与日志定位问题。
		if r.lastErr == nil {
			return 0, fmt.Errorf("storage is (re)initializing")
		} else {
			return 0, fmt.Errorf("storage is (re)initializing: %w", r.lastErr)
		}
	case Ready:
		return r.generation, nil
	case Stopped:
		return 0, fmt.Errorf("apiserver cacher is stopped")
	default:
		return 0, fmt.Errorf("unexpected apiserver cache state: %v", r.state)
	}
}

func (r *ready) setReady() {
	r.set(true, nil)
}

func (r *ready) setError(err error) {
	r.set(false, err)
}

// 把状态设为 Pending（false）或 Ready（true）；状态为 Stopped 时无效。
//
// Stopped 是不可逆终态：停止后即使收到 Ready 信号也不能恢复，否则关闭中的
// 缓存会重新开始对外服务。
func (r *ready) set(ok bool, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.state == Stopped {
		return
	}
	r.lastErr = err
	if ok && r.state == Pending {
		r.state = Ready
		r.generation++
		r.updateLastStateChangeTimeLocked()
		// 幂等关闭：可能已被 stop() 或先前的 set 关闭过。
		select {
		case <-r.waitCh:
		default:
			close(r.waitCh)
		}
	} else if !ok && r.state == Ready {
		// 创建 waitCh 可能存在竞态：
		// 若有其他角色进入 wait() 方法
		//
		// 先确认旧通道已关闭再替换，保证正在等待的 goroutine 不会被悬挂；
		// restartLock 保证替换与 done() 的读取互斥。
		select {
		case <-r.waitCh:
			r.restartLock.Lock()
			r.waitCh = make(chan struct{})
			r.restartLock.Unlock()
		default:
		}
		r.state = Pending
		r.updateLastStateChangeTimeLocked()
	}
}

// 停止条件变量并设为 Stopped。该状态不可逆。
func (r *ready) stop() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.state != Stopped {
		r.state = Stopped
		r.updateLastStateChangeTimeLocked()
	}
	// 唤醒所有等待者，让它们立即看到 Stopped 并返回错误。
	select {
	case <-r.waitCh:
	default:
		close(r.waitCh)
	}
}

// updateLastStateChangeTimeLocked 记录状态切换时间，供 check() 计算停滞时长；
// 调用时必须已持有 lock。
func (r *ready) updateLastStateChangeTimeLocked() {
	r.lastStateChangeTime = r.clock.Now()
}
