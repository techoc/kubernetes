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
	// RecordDroppedRequest 记录一个刚刚
	// 被丢弃的请求。
	RecordDroppedRequest(plName string)

	// GetRetryAfter 返回当前建议的
	// RetryAfter 值。
	GetRetryAfter(plName string) int64
}

// unixStat 记录单秒内被丢弃请求的数量。
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

	// 为减少锁竞争，把当前秒的信息
	// 保存在这里，这样可以在
	// 读锁下访问。
	currentUnix  int64
	currentCount atomic.Int64

	retryAfter           atomic.Int64
	retryAfterUpdateUnix int64
}

// newDroppedRequestsStats 创建统计对象，初始 Retry-After 为 1 秒。
func newDroppedRequestsStats(nowUnix int64) *droppedRequestsStats {
	result := &droppedRequestsStats{
		// 假设第一次丢弃请求后随时可以递增计数。
		retryAfterUpdateUnix: 0,
	}
	result.retryAfter.Store(1)
	return result
}

// recordDroppedRequest 记录一次丢弃；同一秒内走读锁快路径，
// 跨秒时切换到写锁，把上一秒的计数归档并重新计算 Retry-After。
func (s *droppedRequestsStats) recordDroppedRequest(unixTime int64) {
	// 快路径：若当前秒与传入时间一致，
	// 只更新统计。
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

	// 触发 <currentUnix> 的变化。
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentUnix == unixTime {
		s.currentCount.Add(1)
		return
	}

	s.updateHistory(s.currentUnix, s.currentCount.Load())
	s.currentUnix = unixTime
	s.currentCount.Store(1)

	// 只在推进当前秒时考虑更新 retryAfter。
	// 但由于当前秒尚未上报任何数据，
	// 因此根据上一秒的统计重新计算。
	s.updateRetryAfterIfNeededLocked(unixTime)
}

// updateHistory 追加统计并裁剪过期条目。
// 保留窗口取 2*retryAfter（上限 maxRetryAfter），因为判断与展示只需该窗口。
func (s *droppedRequestsStats) updateHistory(unixTime int64, count int64) {
	s.history = append(s.history, unixStat{unixTime: unixTime, requests: count})

	startIndex := 0
	// 超过 2*retryAfter 或 maxRetryAfter 的条目永远不会被用到。
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

// updateRetryAfterIfNeededLocked 根据过去 `retryAfter` 秒内
// 被丢弃的请求数更新 retryAfter：
//   - 若丢弃请求少于 `retryAfter` 个，则减小
//     retryAfter
//   - 若丢弃请求至少 3*`retryAfter` 个，则增大
//     retryAfter
//
// 这些阈值定得较低，是因为 APF 会排队请求，
// 拒绝（丢弃）是最后手段，只有在某个优先级确实过载时
// 才会发生。
//
// 此外，对 retryAfter 的增大做限速：上次增大后至少要等
// `retryAfter` 秒，避免一次尖峰导致多次递增。
//
// 使用区间 [unixTime-retryAfter, unixTime)。
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
		// 模仿 TCP 算法，因此这里把
		// retryAfter 翻倍。
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
		// 模仿 TCP 算法，因此这里
		// 线性缩小 retryAfter。
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

// NewDroppedRequestsTracker 创建
// 一个新的 DroppedRequestsTracker 实例。
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
		// 优先级列表很少变化，
		// 因此绝大多数情况下快路径就足够了。
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
