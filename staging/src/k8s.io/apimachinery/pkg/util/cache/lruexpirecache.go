/*
Copyright 2016 The Kubernetes Authors.

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

// Package cache 提供带容量上限与 TTL 的并发安全缓存。
package cache

import (
	"container/list"
	"sync"
	"time"
)

// Clock 定义获取当前时间的接口，便于测试注入可控时钟。
// 抽出时钟是为了测试能确定性地控制过期时间，而不必真实 sleep。
type Clock interface {
	Now() time.Time
}

// realClock 通过 time.Now() 实现 Clock 接口。
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// LRUExpireCache 是带 TTL 和容量上限的 LRU 缓存：访问过的条目会提升到队首，
// 超过 TTL 的条目视为失效，容量不足时淘汰最久未使用的条目。
//
// evictionList 以“最近使用在前”的顺序维护元素，entries 提供
// O(1) 查找；两个结构必须在锁内同步更新。容量满时淘汰最久未使用的条目，
// 过期条目则在 Get 时惰性删除。
type LRUExpireCache struct {
	// clock 用于获取当前时间并计算条目的过期时刻。
	clock Clock

	lock sync.Mutex

	maxSize      int
	evictionList list.List
	entries      map[interface{}]*list.Element
}

// NewLRUExpireCache 创建指定最大容量的过期 LRU 缓存，使用系统时钟。
func NewLRUExpireCache(maxSize int) *LRUExpireCache {
	return NewLRUExpireCacheWithClock(maxSize, realClock{})
}

// NewLRUExpireCacheWithClock 创建指定最大容量的过期 LRU 缓存，并使用调用方提供的时钟。
func NewLRUExpireCacheWithClock(maxSize int, clock Clock) *LRUExpireCache {
	// maxSize 为 0 会让 evictionList.Len() >= maxSize 恒成立却无元素可淘汰，
	// 因此直接拒绝。
	if maxSize <= 0 {
		panic("maxSize must be > 0")
	}

	return &LRUExpireCache{
		clock:   clock,
		maxSize: maxSize,
		entries: map[interface{}]*list.Element{},
	}
}

// cacheEntry 是 evictionList 中存储的实际值；key 单独保存，便于淘汰时
// 同步删除 entries。
type cacheEntry struct {
	key        interface{}
	value      interface{}
	expireTime time.Time
}

// Add 将 key/value 写入缓存，并设置 ttl。若 key 已存在则更新值和过期时间，同时将条目提升为最近使用。
func (c *LRUExpireCache) Add(key interface{}, value interface{}, ttl time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// key 已存在：更新现有节点，不创建重复条目。
	// 已存在的 key 只更新值、TTL 并提升优先级，不新建节点，
	// 从而保证 entries 与链表始终一一对应。
	oldElement, ok := c.entries[key]
	if ok {
		c.evictionList.MoveToFront(oldElement)
		oldElement.Value.(*cacheEntry).value = value
		oldElement.Value.(*cacheEntry).expireTime = c.clock.Now().Add(ttl)
		return
	}

	// 达到容量上限时，先淘汰链表尾部（最久未使用）的条目。
	if c.evictionList.Len() >= c.maxSize {
		toEvict := c.evictionList.Back()
		c.evictionList.Remove(toEvict)
		delete(c.entries, toEvict.Value.(*cacheEntry).key)
	}

	// 创建新条目并插入链表队首。
	entry := &cacheEntry{
		key:        key,
		value:      value,
		expireTime: c.clock.Now().Add(ttl),
	}
	element := c.evictionList.PushFront(entry)
	c.entries[key] = element
}

// Get 查询指定 key。条目存在且未过期时返回 value 和 true；不存在或已过期时返回 nil 和 false。
func (c *LRUExpireCache) Get(key interface{}) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	element, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	// 过期条目在读取时惰性删除，避免后台清理 goroutine 带来的复杂度。
	if c.clock.Now().After(element.Value.(*cacheEntry).expireTime) {
		c.evictionList.Remove(element)
		delete(c.entries, key)
		return nil, false
	}

	c.evictionList.MoveToFront(element)

	return element.Value.(*cacheEntry).value, true
}

// Remove 如果指定 key 存在，则从链表和索引映射中同时删除。
func (c *LRUExpireCache) Remove(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	element, ok := c.entries[key]
	if !ok {
		return
	}

	c.evictionList.Remove(element)
	delete(c.entries, key)
}

// RemoveAll 删除所有满足 predicate 的 key，并同步维护链表与索引映射。
func (c *LRUExpireCache) RemoveAll(predicate func(key any) bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 在遍历中删除 map 条目是安全的（Go 明确规定）。
	for key, element := range c.entries {
		if predicate(key) {
			c.evictionList.Remove(element)
			delete(c.entries, key)
		}
	}
}

// Keys 返回缓存中所有尚未过期的 key。
//
// 返回 key 后，条目仍可能在下一次 Get 前过期，因此后续 Get 可能返回未命中。
// 返回顺序为从最久未使用到最近使用。
//
// 只读遍历，不删除已过期条目（Get 会负责清理）；返回值顺序
// 依赖链表，故从 Back 向前遍历得到“最久未用优先”的顺序。
func (c *LRUExpireCache) Keys() []interface{} {
	c.lock.Lock()
	defer c.lock.Unlock()

	now := c.clock.Now()

	val := make([]interface{}, 0, c.evictionList.Len())
	for element := c.evictionList.Back(); element != nil; element = element.Prev() {
		// 只返回尚未过期的条目；Keys 不负责删除过期条目。
		if !now.After(element.Value.(*cacheEntry).expireTime) {
			val = append(val, element.Value.(*cacheEntry).key)
		}
	}

	return val
}
