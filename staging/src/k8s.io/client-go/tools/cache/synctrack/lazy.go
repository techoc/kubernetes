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

// Lazy defers the computation of `Evaluate` to when it is necessary. It is
// possible that Evaluate will be called in parallel from multiple goroutines.
// Lazy 把 Evaluate 的计算推迟到真正需要时；Notify 会使已缓存的结果失效。
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

// Notify should be called when something has changed necessitating a new call
// to Evaluate.
func (z *Lazy[T]) Notify() { z.cache.Swap(z.newCacheEntry()) }

// Get should be called to get the current result of a call to Evaluate. If the
// current cached value is stale (due to a call to Notify), then Evaluate will
// be called synchronously. If subsequent calls to Get happen (without another
// Notify), they will all wait for the same return value.
//
// Error returns are not cached and will cause multiple calls to evaluate!
func (z *Lazy[T]) Get() (T, error) {
	e := z.cache.Load()
	if e == nil {
		// Since we don't force a constructor, nil is a possible value.
		// If multiple Gets race to set this, the swap makes sure only
		// one wins.
		// 用 CAS 而非直接 Store，避免并发的首次 Get 覆盖掉别人已经建好的条目。
		z.cache.CompareAndSwap(nil, z.newCacheEntry())
		e = z.cache.Load()
	}
	return e.get()
}
