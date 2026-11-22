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

package flowcontrol

import (
	"math/rand"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// backoffEntry 保存单个 key 的退避状态。
type backoffEntry struct {
	backoff    time.Duration
	lastUpdate time.Time
}

// Backoff 按 key 维护独立的指数退避计数，并对所有 key 的访问做并发保护。
//
// 典型用法：某个 key 处理失败后调用 Next 推进退避，处理成功或状态稳定后
// 调用 Reset 清除；IsInBackOffSince 用于判断是否仍在退避窗口内，
// 避免对不健康的 key 过度重试。
type Backoff struct {
	sync.RWMutex
	Clock clock.Clock
	// HasExpiredFunc controls the logic that determines whether the backoff
	// counter should be reset, and when to GC old backoff entries. If nil, the
	// default hasExpired function will restart the backoff factor to the
	// beginning after observing time has passed at least equal to 2*maxDuration
	//
	// HasExpiredFunc 控制何时认为退避已过期（重置计数、GC 旧条目）。
	// 为 nil 时使用默认逻辑：距上次更新超过 2*maxDuration 即视为稳定并重新开始。
	HasExpiredFunc  func(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool
	defaultDuration time.Duration
	maxDuration     time.Duration
	// perItemBackoff 按 key 保存退避条目；由嵌入的 RWMutex 保护。
	perItemBackoff map[string]*backoffEntry
	rand           *rand.Rand

	// maxJitterFactor adds jitter to the exponentially backed off delay.
	// if maxJitterFactor is zero, no jitter is added to the delay in
	// order to maintain current behavior.
	//
	// maxJitterFactor 为指数退避延迟添加抖动。为零时不加抖动，保持原有行为。
	maxJitterFactor float64
}

// NewFakeBackOff 创建使用指定（通常是 fake）时钟、无抖动的 Backoff，便于测试。
func NewFakeBackOff(initial, max time.Duration, tc clock.Clock) *Backoff {
	return newBackoff(tc, initial, max, 0.0)
}

// NewBackOff 创建使用真实时钟、无抖动的 Backoff。
func NewBackOff(initial, max time.Duration) *Backoff {
	return NewBackOffWithJitter(initial, max, 0.0)
}

// NewFakeBackOffWithJitter 创建使用指定时钟并启用抖动的 Backoff，便于测试。
func NewFakeBackOffWithJitter(initial, max time.Duration, tc clock.Clock, maxJitterFactor float64) *Backoff {
	return newBackoff(tc, initial, max, maxJitterFactor)
}

// NewBackOffWithJitter 创建使用真实时钟并启用抖动的 Backoff。
// 抖动可降低多个客户端在同一时刻集中重试（thundering herd）的概率。
func NewBackOffWithJitter(initial, max time.Duration, maxJitterFactor float64) *Backoff {
	clock := clock.RealClock{}
	return newBackoff(clock, initial, max, maxJitterFactor)
}

func newBackoff(clock clock.Clock, initial, max time.Duration, maxJitterFactor float64) *Backoff {
	var random *rand.Rand
	// 只有需要抖动时才创建随机源，避免无谓开销。
	if maxJitterFactor > 0 {
		random = rand.New(rand.NewSource(clock.Now().UnixNano()))
	}
	return &Backoff{
		perItemBackoff:  map[string]*backoffEntry{},
		Clock:           clock,
		defaultDuration: initial,
		maxDuration:     max,
		maxJitterFactor: maxJitterFactor,
		rand:            random,
	}
}

// Get the current backoff Duration
//
// Get 返回指定 key 当前的退避时长；key 不存在时返回 0。
func (p *Backoff) Get(id string) time.Duration {
	p.RLock()
	defer p.RUnlock()
	var delay time.Duration
	entry, ok := p.perItemBackoff[id]
	if ok {
		delay = entry.backoff
	}
	return delay
}

// move backoff to the next mark, capping at maxDuration
//
// Next 把指定 key 的退避推进到下一档（指数增长并封顶 maxDuration）。
// 若条目不存在或已过期，则从初始值重新开始。
func (p *Backoff) Next(id string, eventTime time.Time) {
	p.Lock()
	defer p.Unlock()
	entry, ok := p.perItemBackoff[id]
	if !ok || p.hasExpired(eventTime, entry.lastUpdate, p.maxDuration) {
		// 首次失败或长期未更新：重置计数，从默认值起算并加上抖动。
		entry = p.initEntryUnsafe(id)
		entry.backoff += p.jitter(entry.backoff)
	} else {
		delay := entry.backoff * 2       // exponential
		delay += p.jitter(entry.backoff) // add some jitter to the delay
		entry.backoff = min(delay, p.maxDuration)
	}
	entry.lastUpdate = p.Clock.Now()
}

// Reset forces clearing of all backoff data for a given key.
//
// Reset 清除指定 key 的全部退避数据（即重置退避）。
func (p *Backoff) Reset(id string) {
	p.Lock()
	defer p.Unlock()
	delete(p.perItemBackoff, id)
}

// Returns True if the elapsed time since eventTime is smaller than the current backoff window
//
// IsInBackOffSince 判断自 eventTime 起经过的时间是否仍小于当前退避窗口。
func (p *Backoff) IsInBackOffSince(id string, eventTime time.Time) bool {
	p.RLock()
	defer p.RUnlock()
	entry, ok := p.perItemBackoff[id]
	if !ok {
		return false
	}
	if p.hasExpired(eventTime, entry.lastUpdate, p.maxDuration) {
		return false
	}
	return p.Clock.Since(eventTime) < entry.backoff
}

// Returns True if time since lastupdate is less than the current backoff window.
//
// IsInBackOffSinceUpdate 判断距上次更新时间是否仍小于当前退避窗口。
func (p *Backoff) IsInBackOffSinceUpdate(id string, eventTime time.Time) bool {
	p.RLock()
	defer p.RUnlock()
	entry, ok := p.perItemBackoff[id]
	if !ok {
		return false
	}
	if p.hasExpired(eventTime, entry.lastUpdate, p.maxDuration) {
		return false
	}
	return eventTime.Sub(entry.lastUpdate) < entry.backoff
}

// Garbage collect records that have aged past their expiration, which defaults
// to 2*maxDuration (see hasExpired godoc). Backoff users are expected to invoke
// this periodically.
//
// GC 清理已过期的记录（默认超过 2*maxDuration，见 hasExpired）。
// 使用方需要定期调用，防止 perItemBackoff 无限增长。
func (p *Backoff) GC() {
	p.Lock()
	defer p.Unlock()
	now := p.Clock.Now()
	for id, entry := range p.perItemBackoff {
		if p.hasExpired(now, entry.lastUpdate, p.maxDuration) {
			delete(p.perItemBackoff, id)
		}
	}
}

// DeleteEntry 删除指定 key 的条目；与 Reset 等价，保留以兼容既有调用。
func (p *Backoff) DeleteEntry(id string) {
	p.Lock()
	defer p.Unlock()
	delete(p.perItemBackoff, id)
}

// Take a lock on *Backoff, before calling initEntryUnsafe
//
// initEntryUnsafe 创建并登记新条目；调用前必须已持有写锁（方法名 Unsafe 即源于此）。
func (p *Backoff) initEntryUnsafe(id string) *backoffEntry {
	entry := &backoffEntry{backoff: p.defaultDuration}
	p.perItemBackoff[id] = entry
	return entry
}

func (p *Backoff) jitter(delay time.Duration) time.Duration {
	if p.rand == nil {
		return 0
	}

	return time.Duration(p.rand.Float64() * p.maxJitterFactor * float64(delay))
}

// Unless an alternate function is provided, after 2*maxDuration we restart the backoff factor to the beginning
//
// hasExpired 判断退避是否已过期；未提供 HasExpiredFunc 时默认阈值为 2*maxDuration。
func (p *Backoff) hasExpired(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool {
	if p.HasExpiredFunc != nil {
		return p.HasExpiredFunc(eventTime, lastUpdate, maxDuration)
	}
	return eventTime.Sub(lastUpdate) > maxDuration*2 // consider stable if it's ok for twice the maxDuration
}
