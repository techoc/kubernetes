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

package synctrack

import (
	"sync"
	"sync/atomic"
)

// Lazy 把 Evaluate 的计算推迟到真正需要时；Notify 会使已缓存的结果失效。
// 多个 goroutine 可能并发调用 Evaluate。
// 并发语义：多个 goroutine 同时 Get 时共享同一次 Evaluate 的结果（单飞效果），
// 但出错的结果不会被缓存，下一次 Get 可能重新调用 Evaluate。
type Lazy[T any] struct {
	Evaluate func() (T, error)

	// cache 指向当前的缓存条目；Notify 通过整体替换指针来使旧结果失效。
	cache atomic.Pointer[cacheEntry[T]]
}

// cacheEntry 是一次 Evaluate 结果的承载单元，内部用读写锁保证只计算一次。
type cacheEntry[T any] struct {
	eval   func() (T, error)
	lock   sync.RWMutex
	result *T
}

// get 返回缓存结果；未命中时在写锁下调用 eval 并缓存成功结果。
// 用指针 nil 表示"尚无结果"，因此即使 T 的零值也有意义。
func (e *cacheEntry[T]) get() (T, error) {
	// 快路径：已有结果时只加读锁，避免所有读者串行化。
	if cur := func() *T {
		e.lock.RLock()
		defer e.lock.RUnlock()
		return e.result
	}(); cur != nil {
		return *cur, nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	// 双重检查：可能在等待写锁期间已有其他 goroutine 算出了结果。
	if e.result != nil {
		return *e.result, nil
	}
	r, err := e.eval()
	// 只缓存成功结果：错误通常是暂时的，缓存错误会让状态无法自愈。
	if err == nil {
		e.result = &r
	}
	return r, err
}

func (z *Lazy[T]) newCacheEntry() *cacheEntry[T] {
	return &cacheEntry[T]{eval: z.Evaluate}
}

// Notify 在有变化发生、需要重新执行 Evaluate 时调用。
func (z *Lazy[T]) Notify() { z.cache.Swap(z.newCacheEntry()) }

// Get 用于获取 Evaluate 的当前结果。如果当前缓存值已因 Notify 调用而过期，
// 会同步执行 Evaluate；如果后续 Get 发生在下一次 Notify 之前，它们都会等待
// 同一个返回值。
//
// 出错的结果不会被缓存，这会导致 Evaluate 被多次调用！
func (z *Lazy[T]) Get() (T, error) {
	e := z.cache.Load()
	if e == nil {
		// 没有强制要求走构造函数，所以 cache 可能为 nil。
		// 若多个 Get 竞争初始化，CompareAndSwap 保证只有一个胜出。
		// 用 CAS 而非直接 Store，避免并发的首次 Get 覆盖掉别人已经建好的条目。
		z.cache.CompareAndSwap(nil, z.newCacheEntry())
		e = z.cache.Load()
	}
	return e.get()
}
