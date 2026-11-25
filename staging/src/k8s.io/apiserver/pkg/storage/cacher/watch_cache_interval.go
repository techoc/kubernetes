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

// cacheIntervalSource 为 watchCacheInterval 提供事件的迭代逻辑。
type cacheIntervalSource interface {
	Next() (*watchCacheEvent, error)
}

// watchCacheInterval 是对 watchCacheEvent 某个来源的抽象封装。
// 它把具体的迭代工作委托给一个 cacheIntervalSource，
// 并保存通用元数据（resourceVersion、initialEventsEndBookmark）。
//
// 区间对象让 watch 在持有 cache 读锁的极短时间内“预订”一段事件范围，
// 之后的遍历在锁外完成，避免长时间阻塞写入路径。
type watchCacheInterval struct {
	// source 为该区间提供事件的迭代逻辑。
	source cacheIntervalSource

	// resourceVersion 是构造该区间时所使用的 resourceVersion。
	resourceVersion uint64

	// initialEventsEndBookmark 会在发送完 cacheInterval 中的所有事件之后发送
	//
	// 仅用于 SendInitialEvents 语义：客户端据此得知“初始事件已发完”，
	// 后续可以安全地以 bookmark 的 RV 作为 watch 起点。
	initialEventsEndBookmark *watchCacheEvent
}

// Next 返回该 cache interval 中的下一个元素，前提是该区间仍然有效。
// 如果区间已经失效，则返回错误。
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

// newCacheIntervalFromStore 用于处理 rv=0 的情况：此时 Next() 返回的事件
// 必须来自对 watch cache 底层存储执行的一次 List()。
// 该区间返回的条目会按 Key 排序。
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

// newCacheIntervalFromLazySnapshot 构造一个由不可变存储快照支撑的区间。
// 与 newCacheIntervalFromStore 不同，它只是捕获快照引用，因此在 watch cache
// 读锁内只需 O(1)；而 O(N) 的遍历被推迟到释放锁之后的第一次 Next() 调用。
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

// historyCacheIntervalSource 从 watchCache 的环形缓冲区中提供事件。
// 它在底层来源上维护一个事件窗口，并按批次把事件复制到内部缓冲区，
// 以降低加锁频率。
//
// 区间在任意时刻要么有效、要么无效。当环形缓冲区已满且需要弹出一个事件时，
// watchCache::startIndex 会自增。在这种情况下，跟踪该被弹出事件的区间，
// 只有当它已经被复制到自身内部缓冲区时才仍然有效。不过出于效率考虑，
// 我们是惰性执行该复制的：当且仅当我们需要从 watchCache 复制事件、
// 而最终用到的事件已经被弹出时，才把区间标记为无效。这等价于如下条件：
//
//	historyCacheIntervalSource::startIndex >= watchCache::startIndex.
//
// 当该条件不再成立时，区间即失效，不应再被用于从底层来源检索并提供元素。
type historyCacheIntervalSource struct {
	// startIndex 表示所考虑区间的起始位置。该值是事件真实来源
	// （watchCacheEvents）中的下标。若事件来源是 watchCache，
	// 则该下标必须对 capacity 取模后使用。
	startIndex int

	// endIndex 表示所考虑区间的结束位置。该值是事件真实来源中的下标。
	// 若事件来源是 watchCache，则该下标应当对 capacity 取模后使用。
	endIndex int

	// indexer 用于注入“在给定下标时如何从底层来源取出一个事件”的行为。
	indexer indexerFunc

	// indexValidator 用于判断给定下标是否仍然有效。如果判定该下标已失效，
	// 那么这个区间就不能再用于提供事件。只有当底层来源中的事件窗口
	// 会随时间变化时，才有必要使用 indexValidator。
	// 此外，若区间的 startIndex 不再与底层来源的 startIndex 一致，
	// 该区间即为无效。
	indexValidator indexValidator

	// buffer 保存该区间在 Next() 调用时返回的 watchCacheEvent。
	// 它的存在主要是为了减少每次调用 Next() 时的加锁开销。
	buffer *watchCacheIntervalBuffer

	// lock 实际上保护的是通过 indexer 和 indexValidator 访问底层事件来源。
	//
	// 鉴于 indexer 和 indexValidator 只读取状态，在可能的情况下，
	// 提供由 RLocker() 获得的 Locker（即读锁）。
	lock sync.Locker
}

// Next 返回 watchCache 环形缓冲区中的下一个事件。
// 若该区间已被判定失效，则返回错误。
//
// 区间在任意时刻要么有效、要么无效。当环形缓冲区已满且需要弹出一个事件时，
// watchCache::startIndex 会自增。在这种情况下，跟踪该被弹出事件的区间，
// 只有当它已经被复制到自身内部缓冲区时才仍然有效。不过出于效率考虑，
// 我们是惰性执行该复制的：当且仅当我们需要从 watchCache 复制事件、
// 而最终用到的事件已经被弹出时，才把区间标记为无效。这等价于如下条件：
//
//	historyCacheIntervalSource::startIndex >= watchCache::startIndex.
//
// 当该条件不再成立时，区间即失效，不应再被用于从底层来源检索并提供元素。
func (s *historyCacheIntervalSource) Next() (*watchCacheEvent, error) {
	// 如果缓冲区里还有可返回的元素，就直接从缓冲区返回。
	if event, exists := s.buffer.next(); exists {
		return event, nil
	}
	// 检查该区间内是否还有可供处理的其他事件。
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

// snapshotCacheIntervalSource 从预先填充好的缓冲区中提供事件。
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

// lazySnapshotCacheIntervalSource 从不可变快照中提供事件。
// 快照引用是在 watchCache 锁内捕获的，而把事件真正物化出来的那次 O(N) 遍历
// 被推迟到第一次 Next() 调用，从而在锁外执行。
type lazySnapshotCacheIntervalSource struct {
	// snapshot 是存储在某一刻的不可变副本。
	snapshot store.Snapshot
	// resourceVersion 会被赋给该 source 产生的每一个 watchCacheEvent。
	resourceVersion uint64
	// loaded 表示 items 是否已经从快照中物化出来。
	loaded bool
	// items 保存 OrderedListPrefix 的结果，在第一次 Next() 调用时填充
	items []interface{}
	// currentIndex 跟踪当前在 items 中的位置。
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

// watchCacheIntervalBuffer 用于减少 historyCacheIntervalSource.Next()
// 每次调用时的加锁开销。
type watchCacheIntervalBuffer struct {
	// buffer 用于保存该区间在 Next() 调用时返回的 watchCacheEvent。
	buffer []*watchCacheEvent
	// buffer 的第一个元素由 startIndex 界定，
	// 最后一个元素由 endIndex 界定。
	startIndex int
	endIndex   int
}

// next 返回该区间缓冲区中的下一个事件，前提是缓冲区非空。
func (wcib *watchCacheIntervalBuffer) next() (*watchCacheEvent, bool) {
	if wcib.isEmpty() {
		return nil, false
	}
	next := wcib.buffer[wcib.startIndex]
	// 清理缓冲区中已不再使用的事件引用。若不做清理，
	// 即使 watch 事件已经从 watch cache 中被老化淘汰，
	// 在持有该缓冲区引用的 watcher 存活期间，
	// 这些事件也无法被 GC 回收。
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
