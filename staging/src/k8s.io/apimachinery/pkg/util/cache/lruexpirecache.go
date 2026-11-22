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

// Clock defines an interface for obtaining the current time.
// 中文说明：抽出时钟是为了测试能确定性地控制过期时间，而不必真实 sleep。
type Clock interface {
	Now() time.Time
}

// realClock implements the Clock interface by calling time.Now()
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// LRUExpireCache is a cache that ensures the mostly recently accessed keys are returned with
// a ttl beyond which keys are forcibly expired.
//
// 中文说明：evictionList 以“最近使用在前”的顺序维护元素，entries 提供
// O(1) 查找；两个结构必须在锁内同步更新。容量满时淘汰最久未使用的条目，
// 过期条目则在 Get 时惰性删除。
type LRUExpireCache struct {
	// clock is used to obtain the current time
	clock Clock

	lock sync.Mutex

	maxSize      int
	evictionList list.List
	entries      map[interface{}]*list.Element
}

// NewLRUExpireCache creates an expiring cache with the given size
func NewLRUExpireCache(maxSize int) *LRUExpireCache {
	return NewLRUExpireCacheWithClock(maxSize, realClock{})
}

// NewLRUExpireCacheWithClock creates an expiring cache with the given size, using the specified clock to obtain the current time.
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

// Add adds the value to the cache at key with the specified maximum duration.
func (c *LRUExpireCache) Add(key interface{}, value interface{}, ttl time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Key already exists
	// 中文说明：已存在的 key 只更新值、TTL 并提升优先级，不新建节点，
	// 从而保证 entries 与链表始终一一对应。
	oldElement, ok := c.entries[key]
	if ok {
		c.evictionList.MoveToFront(oldElement)
		oldElement.Value.(*cacheEntry).value = value
		oldElement.Value.(*cacheEntry).expireTime = c.clock.Now().Add(ttl)
		return
	}

	// Make space if necessary
	if c.evictionList.Len() >= c.maxSize {
		toEvict := c.evictionList.Back()
		c.evictionList.Remove(toEvict)
		delete(c.entries, toEvict.Value.(*cacheEntry).key)
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		value:      value,
		expireTime: c.clock.Now().Add(ttl),
	}
	element := c.evictionList.PushFront(entry)
	c.entries[key] = element
}

// Get returns the value at the specified key from the cache if it exists and is not
// expired, or returns false.
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

// Remove removes the specified key from the cache if it exists
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

// RemoveAll removes all keys that match predicate.
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

// Keys returns all unexpired keys in the cache.
//
// Keep in mind that subsequent calls to Get() for any of the returned keys
// might return "not found".
//
// Keys are returned ordered from least recently used to most recently used.
//
// 中文说明：只读遍历，不删除已过期条目（Get 会负责清理）；返回值顺序
// 依赖链表，故从 Back 向前遍历得到“最久未用优先”的顺序。
func (c *LRUExpireCache) Keys() []interface{} {
	c.lock.Lock()
	defer c.lock.Unlock()

	now := c.clock.Now()

	val := make([]interface{}, 0, c.evictionList.Len())
	for element := c.evictionList.Back(); element != nil; element = element.Prev() {
		// Only return unexpired keys
		if !now.After(element.Value.(*cacheEntry).expireTime) {
			val = append(val, element.Value.(*cacheEntry).key)
		}
	}

	return val
}
