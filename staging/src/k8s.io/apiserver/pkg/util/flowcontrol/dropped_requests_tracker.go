/*
Copyright 2023 The Kubernetes Authors.

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

package flowcontrol

import (
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"
)

const (
	// maxRetryAfter 表示 Retry-After 的最大取值：上限 32 秒——再长会让客户端长时间空转，也可能被误当作服务不可用。
	maxRetryAfter = int64(32)
)

// DroppedRequestsTracker 是一个接口，用于跟踪系统中被丢弃请求的历史，以便调整 Retry-After 头部、避免系统过载。
// DroppedRequestsTracker 按优先级跟踪被丢弃请求的历史，
// 据此动态调整 Retry-After，避免客户端在过载时盲目重试形成正反馈。
type DroppedRequestsTracker interface {
	// RecordDroppedRequest records a request that was just
	// dropped from processing.
	RecordDroppedRequest(plName string)

	// GetRetryAfter returns the current suggested value of
	// RetryAfter value.
	GetRetryAfter(plName string) int64
}

// unixStat keeps a statistic how many requests were dropped within
// a single second.
type unixStat struct {
	unixTime int64
	requests int64
}

// droppedRequestsStats 保存单个优先级级别的丢弃统计。
// 设计要点：绝大多数调用命中读锁快路径（同一秒内自增原子计数），
// 仅在跨秒时获取写锁并重算 retryAfter。
type droppedRequestsStats struct {
	lock sync.RWMutex

	// 只保留与当前 retryAfter 窗口相关的条目，防止无界增长。
	history []unixStat

	// To reduce lock-contention, we store the information about
	// the current second here, which we can then access under
	// reader lock.
	currentUnix  int64
	currentCount atomic.Int64

	retryAfter           atomic.Int64
	retryAfterUpdateUnix int64
}

// newDroppedRequestsStats 创建统计对象，初始 Retry-After 为 1 秒。
func newDroppedRequestsStats(nowUnix int64) *droppedRequestsStats {
	result := &droppedRequestsStats{
		// We assume that we can bump at any time after first dropped request.
		retryAfterUpdateUnix: 0,
	}
	result.retryAfter.Store(1)
	return result
}

// recordDroppedRequest 记录一次丢弃；同一秒内走读锁快路径，
// 跨秒时切换到写锁，把上一秒的计数归档并重新计算 Retry-After。
func (s *droppedRequestsStats) recordDroppedRequest(unixTime int64) {
	// Short path - if the current second matches passed time,
	// just update the stats.
	if done := func() bool {
		s.lock.RLock()
		defer s.lock.RUnlock()
		if s.currentUnix == unixTime {
			s.currentCount.Add(1)
			return true
		}
		return false
	}(); done {
		return
	}

	// We trigger the change of <currentUnix>.
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentUnix == unixTime {
		s.currentCount.Add(1)
		return
	}

	s.updateHistory(s.currentUnix, s.currentCount.Load())
	s.currentUnix = unixTime
	s.currentCount.Store(1)

	// We only consider updating retryAfter when bumping the current second.
	// However, given that we didn't report anything for the current second,
	// we recompute it based on statistics from the previous one.
	s.updateRetryAfterIfNeededLocked(unixTime)
}

// updateHistory 追加统计并裁剪过期条目。
// 保留窗口取 2*retryAfter（上限 maxRetryAfter），因为判断与展示只需该窗口。
func (s *droppedRequestsStats) updateHistory(unixTime int64, count int64) {
	s.history = append(s.history, unixStat{unixTime: unixTime, requests: count})

	startIndex := 0
	// Entries that exceed 2*retryAfter or maxRetryAfter are never going to be needed.
	maxHistory := 2 * s.retryAfter.Load()
	if maxHistory > maxRetryAfter {
		maxHistory = maxRetryAfter
	}
	for ; startIndex < len(s.history) && unixTime-s.history[startIndex].unixTime > maxHistory; startIndex++ {
	}
	if startIndex > 0 {
		s.history = s.history[startIndex:]
	}
}

// updateRetryAfterIfNeededLocked updates the retryAfter based on the number of
// dropped requests in the last `retryAfter` seconds:
//   - if there were less than `retryAfter` dropped requests, it decreases
//     retryAfter
//   - if there were at least 3*`retryAfter` dropped requests, it increases
//     retryAfter
//
// The rationale behind these numbers being fairly low is that APF is queuing
// requests and rejecting (dropping) them is a last resort, which is not expected
// unless a given priority level is actually overloaded.
//
// Additionally, we rate-limit the increases of retryAfter to wait at least
// `retryAfter' seconds after the previous increase to avoid multiple bumps
// on a single spike.
//
// We're working with the interval [unixTime-retryAfter, unixTime).
//
// 调用方必须持有写锁。
func (s *droppedRequestsStats) updateRetryAfterIfNeededLocked(unixTime int64) {
	retryAfter := s.retryAfter.Load()

	droppedRequests := int64(0)
	for i := len(s.history) - 1; i >= 0; i-- {
		if unixTime-s.history[i].unixTime > retryAfter {
			break
		}
		if s.history[i].unixTime < unixTime {
			droppedRequests += s.history[i].requests
		}
	}

	// 需求超过 3 倍窗口时翻倍（类似 TCP 退避），但受速率限制：
	// 距离上次增长不足 retryAfter 秒则忽略，避免单个尖峰引发连续翻倍。
	if unixTime-s.retryAfterUpdateUnix >= retryAfter && droppedRequests >= 3*retryAfter {
		// We try to mimic the TCP algorithm and thus are doubling
		// the retryAfter here.
		retryAfter *= 2
		if retryAfter >= maxRetryAfter {
			retryAfter = maxRetryAfter
		}
		s.retryAfter.Store(retryAfter)
		s.retryAfterUpdateUnix = unixTime
		return
	}

	// 需求低于窗口时线性递减，逐步恢复到 1 秒。
	if droppedRequests < retryAfter && retryAfter > 1 {
		// We try to mimc the TCP algorithm and thus are linearly
		// scaling down the retryAfter here.
		retryAfter--
		s.retryAfter.Store(retryAfter)
		return
	}
}

// droppedRequestsTracker 实现 DroppedRequestsTracker 接口，用于为新丢弃的请求调整 Retry-After 头部，避免系统过载。
// droppedRequestsTracker 按优先级名称聚合 droppedRequestsStats。
type droppedRequestsTracker struct {
	now func() time.Time

	lock    sync.RWMutex
	plStats map[string]*droppedRequestsStats
}

// NewDroppedRequestsTracker is creating a new instance of
// DroppedRequestsTracker.
func NewDroppedRequestsTracker() DroppedRequestsTracker {
	return newDroppedRequestsTracker(clock.RealClock{}.Now)
}

// newDroppedRequestsTracker 允许注入时钟，便于测试退避行为。
func newDroppedRequestsTracker(now func() time.Time) *droppedRequestsTracker {
	return &droppedRequestsTracker{
		now:     now,
		plStats: make(map[string]*droppedRequestsStats),
	}
}

// RecordDroppedRequest 记录某优先级的一次丢弃；优先级的统计对象按需惰性创建。
func (t *droppedRequestsTracker) RecordDroppedRequest(plName string) {
	unixTime := t.now().Unix()

	stats := func() *droppedRequestsStats {
		// The list of priority levels should change very infrequently,
		// so in almost all cases, the fast path should be enough.
		t.lock.RLock()
		if plStats, ok := t.plStats[plName]; ok {
			t.lock.RUnlock()
			return plStats
		}
		t.lock.RUnlock()

		// 慢路径：持有写锁更新 map。双重检查：解锁后可能有其他 goroutine 先创建了该条目。
		t.lock.Lock()
		defer t.lock.Unlock()
		if plStats, ok := t.plStats[plName]; ok {
			return plStats
		}
		stats := newDroppedRequestsStats(unixTime)
		t.plStats[plName] = stats
		return stats
	}()

	stats.recordDroppedRequest(unixTime)
}

// GetRetryAfter 返回该优先级的建议 Retry-After；从未丢弃过则返回 1 秒。
func (t *droppedRequestsTracker) GetRetryAfter(plName string) int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if plStats, ok := t.plStats[plName]; ok {
		return plStats.retryAfter.Load()
	}
	return 1
}
