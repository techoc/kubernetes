/*
Copyright 2021 The Kubernetes Authors.

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
	"sync"

	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage/cacher/store"
)

// cacheIntervalSource provides the iteration logic for a watchCacheInterval.
type cacheIntervalSource interface {
	Next() (*watchCacheEvent, error)
}

// watchCacheInterval serves as an abstraction over a source
// of watchCacheEvents. It delegates iteration to a cacheIntervalSource
// and holds common metadata (resourceVersion, initialEventsEndBookmark).
//
// 区间对象让 watch 在持有 cache 读锁的极短时间内“预订”一段事件范围，
// 之后的遍历在锁外完成，避免长时间阻塞写入路径。
type watchCacheInterval struct {
	// source provides the iteration logic for this interval.
	source cacheIntervalSource

	// resourceVersion is the resourceVersion from which
	// the interval was constructed.
	resourceVersion uint64

	// initialEventsEndBookmark will be sent after sending all events in cacheInterval
	//
	// 仅用于 SendInitialEvents 语义：客户端据此得知“初始事件已发完”，
	// 后续可以安全地以 bookmark 的 RV 作为 watch 起点。
	initialEventsEndBookmark *watchCacheEvent
}

// Next returns the next item in the cache interval provided the cache
// interval is still valid. An error is returned if the interval is
// invalidated.
func (wci *watchCacheInterval) Next() (*watchCacheEvent, error) {
	return wci.source.Next()
}

// indexerFunc 按下标取出事件；下标对环形缓冲区需取模。
type indexerFunc func(int) *watchCacheEvent

// indexValidator 判断下标是否仍在有效窗口内（未被环形缓冲区覆盖）。
type indexValidator func(int) bool

// newCacheInterval 构造基于环形缓冲区的事件区间。
// locker 应为 watchCache 读锁的 RLocker，因为 indexer/indexValidator 只读。
func newCacheInterval(startIndex, endIndex int, indexer indexerFunc, indexValidator indexValidator, resourceVersion uint64, locker sync.Locker) *watchCacheInterval {
	return &watchCacheInterval{
		source: &historyCacheIntervalSource{
			startIndex:     startIndex,
			endIndex:       endIndex,
			indexer:        indexer,
			indexValidator: indexValidator,
			buffer:         &watchCacheIntervalBuffer{buffer: make([]*watchCacheEvent, bufferSize)},
			lock:           locker,
		},
		resourceVersion: resourceVersion,
	}
}

// newCacheIntervalFromStore is meant to handle the case of rv=0, such that the events
// returned by Next() need to be events from a List() done on the underlying store of
// the watch cache.
// The items returned in the interval will be sorted by Key.
//
// rv=0 的 list/watch 不走事件历史，而是直接从存储快照取全量对象，包装成 Added 事件。
func newCacheIntervalFromStore(resourceVersion uint64, snap store.Snapshot, key string, matchesSingle bool) (*watchCacheInterval, error) {
	buffer := &watchCacheIntervalBuffer{}
	var allItems []interface{}
	var err error
	if matchesSingle {
		// 按 name 精确查询只需取一个对象，避免全量遍历。
		item, exists, err := snap.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if exists {
			allItems = append(allItems, item)
		}
	} else {
		allItems, err = snap.OrderedListPrefix("", "")
		if err != nil {
			return nil, err
		}
	}
	buffer.buffer = make([]*watchCacheEvent, len(allItems))
	for i, item := range allItems {
		elem, ok := item.(*store.Element)
		if !ok {
			return nil, fmt.Errorf("not a storeElement: %v", elem)
		}
		buffer.buffer[i] = storeElementToWatchCacheEvent(elem, resourceVersion)
		buffer.endIndex++
	}
	ci := &watchCacheInterval{
		source:          &snapshotCacheIntervalSource{buffer: buffer},
		resourceVersion: resourceVersion,
	}

	return ci, nil
}

// newCacheIntervalFromLazySnapshot builds an interval backed by an immutable store snapshot
// Unlike newCacheIntervalFromStore, it captures snapshot reference which takes O(1) under watch-cache RLock
// and defers O(N) traversal to first Next() call post lock release
//
// 这是大集群 list 的关键优化：持锁时间与对象数无关，避免全量 list 期间阻塞写请求。
func newCacheIntervalFromLazySnapshot(resourceVersion uint64, snap store.Snapshot) *watchCacheInterval {
	return &watchCacheInterval{
		source: &lazySnapshotCacheIntervalSource{
			snapshot:        snap,
			resourceVersion: resourceVersion,
		},
		resourceVersion: resourceVersion,
	}
}

// storeElementToWatchCacheEvent 把存储元素转换为 Added 事件；标签与字段预先算好，避免每次过滤重算。
func storeElementToWatchCacheEvent(elem *store.Element, resourceVersion uint64) *watchCacheEvent {
	return &watchCacheEvent{
		Type:            watch.Added,
		Object:          elem.Object,
		ObjLabels:       elem.Labels,
		ObjFields:       elem.Fields,
		Key:             elem.Key,
		ResourceVersion: resourceVersion,
	}
}

// historyCacheIntervalSource serves events from the watchCache circular buffer.
// It maintains a window of events over the underlying source and copies them
// in batches into an internal buffer to reduce lock acquisition frequency.
//
// An interval can be either valid or invalid at any given point of time.
// When the circular buffer is full and an event needs to be popped off,
// watchCache::startIndex is incremented. In this case, an interval tracking
// that popped event is valid only if it has already been copied to its
// internal buffer. However, for efficiency we perform that lazily and we
// mark an interval as invalid iff we need to copy events from the watchCache
// and we end up needing events that have already been popped off. This
// translates to the following condition:
//
//	historyCacheIntervalSource::startIndex >= watchCache::startIndex.
//
// When this condition becomes false, the interval is no longer valid and
// should not be used to retrieve and serve elements from the underlying
// source.
type historyCacheIntervalSource struct {
	// startIndex denotes the starting point of the interval
	// being considered. The value is the index in the actual
	// source of watchCacheEvents. If the source of events is
	// the watchCache, then this must be used modulo capacity.
	startIndex int

	// endIndex denotes the ending point of the interval being
	// considered. The value is the index in the actual source
	// of events. If the source of the events is the watchCache,
	// then this should be used modulo capacity.
	endIndex int

	// indexer is meant to inject behaviour for how an event must
	// be retrieved from the underlying source given an index.
	indexer indexerFunc

	// indexValidator is used to check if a given index is still
	// valid perspective. If it is deemed that the index is not
	// valid, then this interval can no longer be used to serve
	// events. Use of indexValidator is warranted only in cases
	// where the window of events in the underlying source can
	// change over time. Furthermore, an interval is invalid if
	// its startIndex no longer coincides with the startIndex of
	// underlying source.
	indexValidator indexValidator

	// buffer holds watchCacheEvents that this interval returns on
	// a call to Next(). This exists mainly to reduce acquiring the
	// lock on each invocation of Next().
	buffer *watchCacheIntervalBuffer

	// lock effectively protects access to the underlying source
	// of events through - indexer and indexValidator.
	//
	// Given that indexer and indexValidator only read state, if
	// possible, Locker obtained through RLocker() is provided.
	lock sync.Locker
}

// Next returns the next event from the watchCache circular buffer.
// An error is returned if the interval has been invalidated.
//
// An interval can be either valid or invalid at any given point of time.
// When the circular buffer is full and an event needs to be popped off,
// watchCache::startIndex is incremented. In this case, an interval tracking
// that popped event is valid only if it has already been copied to its
// internal buffer. However, for efficiency we perform that lazily and we
// mark an interval as invalid iff we need to copy events from the watchCache
// and we end up needing events that have already been popped off. This
// translates to the following condition:
//
//	historyCacheIntervalSource::startIndex >= watchCache::startIndex.
//
// When this condition becomes false, the interval is no longer valid and
// should not be used to retrieve and serve elements from the underlying
// source.
func (s *historyCacheIntervalSource) Next() (*watchCacheEvent, error) {
	// if there are items in the buffer to return, return from
	// the buffer.
	if event, exists := s.buffer.next(); exists {
		return event, nil
	}
	// check if there are still other events in this interval
	// that can be processed.
	if s.startIndex >= s.endIndex {
		return nil, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	// 拿到锁后必须重新校验：等待锁期间环形缓冲区可能已覆盖起点，此时数据已丢失。
	if valid := s.indexValidator(s.startIndex); !valid {
		return nil, fmt.Errorf("cache interval invalidated, interval startIndex: %d", s.startIndex)
	}

	s.fillBuffer()
	if event, exists := s.buffer.next(); exists {
		return event, nil
	}
	return nil, nil
}

// fillBuffer 一次批量取满缓冲区，把锁获取频率从“每事件一次”降到“每 100 个事件一次”。
func (s *historyCacheIntervalSource) fillBuffer() {
	s.buffer.startIndex = 0
	s.buffer.endIndex = 0
	for s.startIndex < s.endIndex && !s.buffer.isFull() {
		event := s.indexer(s.startIndex)
		if event == nil {
			break
		}
		s.buffer.buffer[s.buffer.endIndex] = event
		s.buffer.endIndex++
		s.startIndex++
	}
}

// snapshotCacheIntervalSource serves events from a pre-populated buffer.
type snapshotCacheIntervalSource struct {
	buffer *watchCacheIntervalBuffer
}

func (s *snapshotCacheIntervalSource) Next() (*watchCacheEvent, error) {
	event, exists := s.buffer.next()
	if !exists {
		return nil, nil
	}
	return event, nil
}

// lazySnapshotCacheIntervalSource serves events from an immutable snapshot.
// The snapshot reference is captured under the watchCache lock, but the O(N)
// traversal that materializes the events is deferred until the first Next()
// call so that it runs off the watchCache lock.
type lazySnapshotCacheIntervalSource struct {
	// snapshot is an immutable point-in-time copy of the store.
	snapshot store.Snapshot
	// resourceVersion is assigned to every watchCacheEvent produced by this source.
	resourceVersion uint64
	// loaded indicates whether items has been materialized from the snapshot.
	loaded bool
	// items holds the result of OrderedListPrefix, populated on the first Next() call
	items []interface{}
	// currentIndex tracks the current position within items.
	currentIndex int
}

// Next 首次调用时才物化快照内容；此后逐条转换为 Added 事件。
// 该方法不持锁，因此不同 watcher 可并行遍历各自快照。
func (s *lazySnapshotCacheIntervalSource) Next() (*watchCacheEvent, error) {
	if !s.loaded {
		items, err := s.snapshot.OrderedListPrefix("", "")
		if err != nil {
			return nil, err
		}
		s.items = items
		s.loaded = true
	}
	if s.currentIndex >= len(s.items) {
		return nil, nil
	}
	elem, ok := s.items[s.currentIndex].(*store.Element)
	if !ok {
		return nil, fmt.Errorf("not a storeElement: %v", s.items[s.currentIndex])
	}
	s.currentIndex++
	return storeElementToWatchCacheEvent(elem, s.resourceVersion), nil
}

// bufferSize 是批量预取的容量；权衡锁竞争与单次持锁时长。
const bufferSize = 100

// watchCacheIntervalBuffer is used to reduce acquiring
// the lock on each invocation of historyCacheIntervalSource.Next().
type watchCacheIntervalBuffer struct {
	// buffer is used to hold watchCacheEvents that
	// the interval returns on a call to Next().
	buffer []*watchCacheEvent
	// The first element of buffer is defined by startIndex,
	// its last element is defined by endIndex.
	startIndex int
	endIndex   int
}

// next returns the next event present in the interval buffer provided
// it is not empty.
func (wcib *watchCacheIntervalBuffer) next() (*watchCacheEvent, bool) {
	if wcib.isEmpty() {
		return nil, false
	}
	next := wcib.buffer[wcib.startIndex]
	// clean the unused event reference in the buffer. If this is not
	// done, event if the watch event is aged out from the watch
	// cache, it will not be GCed during the lifetime of the watcher
	// that holds a reference to the buffer.
	//
	// 置 nil 是必要的内存回收措施：长连接的 watcher 可能长期持有缓冲区，
	// 不清空会让已过期的对象无法被 GC。
	wcib.buffer[wcib.startIndex] = nil
	wcib.startIndex++
	return next, true
}

func (wcib *watchCacheIntervalBuffer) isFull() bool {
	return wcib.endIndex >= bufferSize
}

func (wcib *watchCacheIntervalBuffer) isEmpty() bool {
	return wcib.startIndex == wcib.endIndex
}
