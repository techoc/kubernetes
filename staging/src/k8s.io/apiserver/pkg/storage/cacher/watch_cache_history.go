/*
Copyright The Kubernetes Authors.

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
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/klog/v2"
)

const (
	// defaultLowerBoundCapacity 是事件缓存容量下界的默认值。
	// TODO: 确定可以把它降到什么值。
	defaultLowerBoundCapacity = 100

	// defaultUpperBoundCapacity 应当足以保留所需的历史记录。
	defaultUpperBoundCapacity = 100 * 1024
)

// newWatchCacheHistory 创建动态扩容的循环缓冲区，初始容量取下界，
// 并按事件新鲜度窗口计算容量上界。
func newWatchCacheHistory(config *ImmutableWatchCacheConfig, eventFreshDuration time.Duration) *watchCacheHistory {
	h := &watchCacheHistory{
		config:             config,
		capacity:           defaultLowerBoundCapacity,
		cache:              make([]*watchCacheEvent, defaultLowerBoundCapacity),
		lowerBoundCapacity: defaultLowerBoundCapacity,
		upperBoundCapacity: capacityUpperBound(eventFreshDuration),
		startIndex:         0,
		endIndex:           0,
		eventFreshDuration: eventFreshDuration,
	}
	metrics.WatchCacheCapacity.WithLabelValues(config.groupResource.Group, config.groupResource.Resource).Set(float64(h.capacity))
	return h
}

// capacityUpperBound 表示 watch cache 可以扩容到的最大容量。
// 目标是让高 churn 场景下仍能覆盖整个 eventFreshDuration 的窗口。
func capacityUpperBound(eventFreshDuration time.Duration) int {
	if eventFreshDuration <= DefaultEventFreshDuration {
		return defaultUpperBoundCapacity
	}
	// eventFreshDuration 决定 watch 事件应当在 watch cache 中存储多久。
	// 在极高的 churn 场景下，需要在 watch cache 中存储更多事件，
	// 因此必须相应地扩容。基于这一点，对于较大的 eventFreshDuration
	// 值，我们将 watch cache 容量的上界设为按 eventFreshDuration 与
	// DefaultEventFreshDuration 之比成正比的值。由于 watch cache 的大小
	// 只能翻倍增长，我们把该比例向上取整到 2 的下一个幂。
	exponent := int(math.Ceil((math.Log2(eventFreshDuration.Seconds() / DefaultEventFreshDuration.Seconds()))))
	if maxExponent := int(math.Floor((math.Log2(math.MaxInt32 / defaultUpperBoundCapacity)))); exponent > maxExponent {
		// 确保容量上界能放进 32 位整数：位移过大将溢出 int，因此显式截断并告警。
		exponent = maxExponent
		klog.Warningf("Capping watch cache capacity upper bound to %v", defaultUpperBoundCapacity<<exponent)
	}
	return defaultUpperBoundCapacity << exponent
}

// watchCacheHistory 是以环形数组实现的事件历史窗口。
// 所有方法带 Locked 后缀，调用方必须持有 watchCache 的写锁。
type watchCacheHistory struct {
	config *ImmutableWatchCacheConfig

	// 历史记录窗口的最大容量。
	capacity int

	// 由于事件缓存大小是动态的，此为容量的上界。
	upperBoundCapacity int

	// 由于事件缓存大小是动态的，此为容量的下界。
	lowerBoundCapacity int

	// cache 被用作环形缓冲区——其“当前”内容存储在
	// [start_index%capacity, end_index%capacity) 区间内，
	// 也就是说“当前”内容恰好有 end_index-start_index 个元素。
	cache      []*watchCacheEvent
	startIndex int
	endIndex   int
	// removedEventSinceRelist 记录自上次 relist 以来 `cache` 循环缓冲区中是否已有
	// 事件被淘汰。该标志决定 GetIntervalLocked 能安全回溯到哪个 resourceVersion。
	removedEventSinceRelist bool

	// watchcache 至少保留这么长时间的 watch 历史。
	eventFreshDuration time.Duration
}

// 调用方必须已持有写锁。
func (w *watchCacheHistory) updateCache(event *watchCacheEvent) {
	w.resizeCacheLocked(event.RecordTime)
	if w.isCacheFullLocked() {
		// 缓存已满——移除最旧的那个元素。
		// 覆盖写即“淘汰”：只推进 startIndex，稍后写入会覆盖对应槽位。
		w.startIndex++
		w.removedEventSinceRelist = true
	}
	w.cache[w.endIndex%w.capacity] = event
	w.endIndex++
}

// 按需调整缓存容量：缓存满且全部事件都在 eventFreshDuration 内时翻倍扩容；
// 最近四分之一事件已落在窗口外时减半缩容，避免在边界上反复抖动。
//
// 收缩条件特意看“最近四分之一”的事件，避免在窗口边界上反复扩缩容。
func (w *watchCacheHistory) resizeCacheLocked(eventTime time.Time) {
	if w.isCacheFullLocked() && eventTime.Sub(w.cache[w.startIndex%w.capacity].RecordTime) < w.eventFreshDuration {
		capacity := min(w.capacity*2, w.upperBoundCapacity)
		if capacity > w.capacity {
			w.doCacheResizeLocked(capacity)
		}
		return
	}
	if w.isCacheFullLocked() && eventTime.Sub(w.cache[(w.endIndex-w.capacity/4)%w.capacity].RecordTime) > w.eventFreshDuration {
		capacity := max(w.capacity/2, w.lowerBoundCapacity)
		if capacity < w.capacity {
			w.doCacheResizeLocked(capacity)
		}
		return
	}
}

// 判断事件缓存是否已满。
// 调用方必须已持有写锁。
func (w *watchCacheHistory) isCacheFullLocked() bool {
	return w.endIndex == w.startIndex+w.capacity
}

// 按新容量重建 watchCache 的事件数组。
// 调用方必须已持有写锁。
func (w *watchCacheHistory) doCacheResizeLocked(capacity int) {
	newCache := make([]*watchCacheEvent, capacity)
	if capacity < w.capacity {
		// 若缓存容量发生收缩，则调整 startIndex。
		// 收缩时丢弃最旧的事件，并把 startIndex 调整到仍能完整映射新数组的位置。
		w.startIndex = w.endIndex - capacity
	}
	for i := w.startIndex; i < w.endIndex; i++ {
		newCache[i%capacity] = w.cache[i%w.capacity]
	}
	w.cache = newCache
	metrics.RecordsWatchCacheCapacityChange(w.config.groupResource, w.capacity, capacity)
	w.capacity = capacity
}

// 检查给定索引是否仍有效。调用方必须已持锁。
func (w *watchCacheHistory) isIndexValidLocked(index int) bool {
	return index >= w.startIndex
}

// 清空循环缓冲区，且保证 startIndex 不回退。
// 调用方必须已持有写锁。
func (w *watchCacheHistory) ResetLocked() {
	w.startIndex = w.endIndex
	w.removedEventSinceRelist = false
	clear(w.cache)
}

const (
	// watch 通道大小的下限。为兼容旧行为暂保持 10，待指标证明调低安全后再改。
	minWatchChanSize = 10
	// 同时使用索引与 trigger selector 时 watch 通道的大小上限。
	maxWatchChanSizeWithIndexAndTrigger = 10
	// 使用索引但无 trigger selector 时 watch 通道的大小上限。
	// // 为兼容旧行为暂保持 1000，待指标证明调低安全后再改。
	maxWatchChanSizeWithIndexWithoutTrigger = 1000
	// 未使用索引时 watch 通道的大小上限。
	maxWatchChanSizeWithoutIndex = 100
)

// suggestedWatchChannelSize 估算 watch 事件通道大小：约等于每秒事件数，
// 再按索引/选择器能力夹在 [minWatchChanSize, maxChanSize] 之间。
func (w *watchCacheHistory) suggestedWatchChannelSize(indexExists, triggerUsed bool) int {
	// 用启发式估算通道大小：大致容纳一秒的历史。没有精确数据，
	// // 但缓存保存的是最近 <eventFreshDuration> 的更新，
	// // 因此用容量除以历史窗口长度近似每秒事件数。
	chanSize := int(math.Ceil(float64(w.capacity) / w.eventFreshDuration.Seconds()))

	// 最后夹到合理区间，避免过小或过大。
	chanSize = max(chanSize, minWatchChanSize)
	var maxChanSize int
	switch {
	case indexExists && triggerUsed:
		maxChanSize = maxWatchChanSizeWithIndexAndTrigger
	case indexExists && !triggerUsed:
		maxChanSize = maxWatchChanSizeWithIndexWithoutTrigger
	case !indexExists:
		maxChanSize = maxWatchChanSizeWithoutIndex
	}
	return min(chanSize, maxChanSize)
}

// 返回从给定 resourceVersion 起取事件的 watchCacheInterval。调用方必须已持锁。
func (w *watchCacheHistory) GetIntervalLocked(resourceVersion uint64, listResourceVersion uint64, locker sync.Locker) (*watchCacheInterval, error) {
	size := w.endIndex - w.startIndex
	var oldest uint64
	switch {
	case listResourceVersion > 0 && !w.removedEventSinceRelist:
		// 如果自上次 relist 后缓冲区中没有事件被淘汰，那么我们能够下发的最旧 watch
		// 事件就是 list 的 resourceVersion+1。
		// 利用 list 的 RV 可安全回溯到更早的事件，即使它们已被后续事件覆盖。
		oldest = listResourceVersion + 1
	case size > 0:
		// 不满足上一条件，说明缓冲区已有事件被淘汰，或从未完成过 list
		//（后者只出现在不执行 list/replace 就直接填充缓冲区的单测中）。
		// 此时能下发的最旧事件就是缓冲区中的第一个。
		oldest = w.cache[w.startIndex%w.capacity].ResourceVersion
	default:
		return nil, fmt.Errorf("watch cache isn't correctly initialized")
	}

	if resourceVersion < oldest-1 {
		// 请求的 RV 已滑出窗口，返回 410 让客户端重新 list。
		return nil, errors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", resourceVersion, oldest-1))
	}

	// 二分查找 resourceVersion 大于给定值的最小下标。
	// 环形缓冲的逻辑下标从 startIndex 起算，取模映射到物理槽位。
	f := func(i int) bool {
		return w.cache[(w.startIndex+i)%w.capacity].ResourceVersion > resourceVersion
	}
	first := sort.Search(size, f)
	indexerFunc := func(i int) *watchCacheEvent {
		return w.cache[i%w.capacity]
	}
	ci := newCacheInterval(w.startIndex+first, w.endIndex, indexerFunc, w.config.indexValidator, resourceVersion, locker)
	return ci, nil
}

// 返回循环缓冲区中最旧事件的 resourceVersion。
func (w *watchCacheHistory) OldestResourceVersionLocked() uint64 {
	return w.cache[w.startIndex%w.capacity].ResourceVersion
}

// 返回事件历史缓存的当前容量。
func (w *watchCacheHistory) Capacity() int {
	return w.capacity
}
