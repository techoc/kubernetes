/*
Copyright 2014 The Kubernetes Authors.

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

package cache

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// ExpirationCache 实现了 Store 接口：
//  1. 所有条目在插入时自动打时间戳
//     a. key 依据原始对象和 keyFunc 计算
//     b. 该 key 下插入的是带时间戳的条目
//  2. 过期基于过期策略在读取时惰性发生
//     a. 正在使缓存中*任何*条目过期时，不允许插入新条目
//  3. 返回前会剥离未过期条目的时间戳
//
// 注意：ExpirationCache 天生比普通 threadSafeStore 慢，因为每次检查条目是否
// 过期都要持有写锁。
//
// 额外说明：过期是惰性的，只有被访问到的条目才会删除，因此
// 未访问的过期条目会一直占用内存直到 List/Replace 或进程退出。
type ExpirationCache struct {
	cacheStorage     ThreadSafeStore
	keyFunc          KeyFunc
	clock            clock.Clock
	expirationPolicy ExpirationPolicy
	// expirationLock 是一把写锁，保证我们不会基于过期的时间戳比较而覆盖新插入的对象
	//
	// 所有“读-判断-删”与插入/替换都必须持有它，构成一个串行化的失效边界。
	expirationLock sync.Mutex
}

// ExpirationPolicy 决定对象何时过期。目前抽象成接口只是为了让单元测试不依赖系统时钟。
type ExpirationPolicy interface {
	IsExpired(obj *TimestampedEntry) bool
}

// TTLPolicy 实现基于 TTL 的 ExpirationPolicy。
//
// 只有 TTL 为正数时才启用过期；<=0 表示永不过期。
type TTLPolicy struct {
	//	 >0：淘汰存活时间超过 ttl 的条目
	//	<=0：不淘汰任何条目
	TTL time.Duration

	// 用于计算 ttl 过期的时钟
	// 注入时钟便于单测，不依赖真实时间流逝。
	Clock clock.Clock
}

// IsExpired 在给定对象比 ttl 更旧、或无法确定其存活时间时返回 true。
func (p *TTLPolicy) IsExpired(obj *TimestampedEntry) bool {
	// 先判 TTL 短路，避免对永不过期的缓存做无意义的时间比较。
	return p.TTL > 0 && p.Clock.Since(obj.Timestamp) > p.TTL
}

// TimestampedEntry 是 ExpirationCache 中唯一允许存储的类型。
// 注意在不同计算机之间共享时间戳并不安全：如果从 API Server 获取时间戳并在
// 客户端机器上作为 ExpirationCache 的依据，行为可能不一致。
type TimestampedEntry struct {
	Obj       interface{}
	Timestamp time.Time
	key       string
}

// getTimestampedEntry 返回给定 key 下存储的 TimestampedEntry。
//
// 通过类型断言容错：缓存中若混入其他类型，视为不存在而不是 panic。
func (c *ExpirationCache) getTimestampedEntry(key string) (*TimestampedEntry, bool) {
	item, _ := c.cacheStorage.Get(key)
	if tsEntry, ok := item.(*TimestampedEntry); ok {
		return tsEntry, true
	}
	return nil, false
}

// getOrExpire 仅当 TimestampedEntry 中的对象尚未过期时取出该对象，
// 删除期间持有写锁。
func (c *ExpirationCache) getOrExpire(key string) (interface{}, bool) {
	// 从判定条目“过期”到将其删除期间阻止所有插入，
	// 避免未过期的条目恰好在 Delete 之前用同一 key 挤进来。
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	timestampedItem, exists := c.getTimestampedEntry(key)
	if !exists {
		return nil, false
	}
	if c.expirationPolicy.IsExpired(timestampedItem) {
		// 过期条目在这里顺带清除，读路径因此会写入缓存。
		c.cacheStorage.Delete(key)
		return nil, false
	}
	return timestampedItem.Obj, true
}

// GetByKey 返回 key 下存储的条目；不存在时 exists=false。
func (c *ExpirationCache) GetByKey(key string) (interface{}, bool, error) {
	// Store 接口保留 error 返回值以兼容其他实现，这里永远为 nil。
	obj, exists := c.getOrExpire(key)
	return obj, exists, nil
}

// Get 返回未过期的条目，过程中顺带清理已过期条目。
//
// 对已过期条目而言，Get 与“不存在”表现一致：返回 exists=false 而非错误。
func (c *ExpirationCache) Get(obj interface{}) (interface{}, bool, error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	obj, exists := c.getOrExpire(key)
	return obj, exists, nil
}

// List 返回未过期条目的列表，过程中顺带清理已过期条目。
func (c *ExpirationCache) List() []interface{} {
	items := c.cacheStorage.List()

	// 逐个经 getOrExpire，顺带清理过期项；因此 List 的开销与条目数成正比。
	list := make([]interface{}, 0, len(items))
	for _, item := range items {
		key := item.(*TimestampedEntry).key
		if obj, exists := c.getOrExpire(key); exists {
			list = append(list, obj)
		}
	}
	return list
}

// LastStoreSyncResourceVersion 返回缓存观察到的最新 resource version。
func (c *ExpirationCache) LastStoreSyncResourceVersion() string {
	return c.cacheStorage.LastStoreSyncResourceVersion()
}

// Bookmark 记录缓存中出现的新的 resource version。
func (c *ExpirationCache) Bookmark(rv string) {
	c.cacheStorage.Bookmark(rv)
}

// ListKeys 返回过期缓存中的全部 key。
//
// 注意：返回的 key 可能包含已过期但尚未被惰性清理的条目。
func (c *ExpirationCache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// Add 为条目打上时间戳并插入缓存，会覆盖同一 key 下的已有条目。
func (c *ExpirationCache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()

	// 时间戳在锁内采集，保证与并发删除的先后关系可判定。
	c.cacheStorage.Add(key, &TimestampedEntry{obj, c.clock.Now(), key})
	return nil
}

// 由于没有使用场景，Update 尚未实现，这里直接调用 `Add`，
// 效果相当于刷新时间戳。
func (c *ExpirationCache) Update(obj interface{}) error {
	return c.Add(obj)
}

// Delete 从缓存中移除条目。
func (c *ExpirationCache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	// 传入 obj 让底层 store 在对象已变化时跳过删除，避免误删并发写入的新值。
	c.cacheStorage.DeleteWithObject(key, obj)
	return nil
}

// Replace 会先把给定列表中的所有条目转换为 TimestampedEntry，再执行替换操作。
// 替换操作会删除 ExpirationCache `c` 中原有的内容。
func (c *ExpirationCache) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	// 同一批替换共享一个时间戳，避免列表中靠后的条目看起来更“新”。
	ts := c.clock.Now()
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = &TimestampedEntry{item, ts, key}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync 对该类型是空操作
func (c *ExpirationCache) Resync() error {
	return nil
}

// NewTTLStore 创建并返回一个使用 TTLPolicy 的 ExpirationCache。
func NewTTLStore(keyFunc KeyFunc, ttl time.Duration) Store {
	return NewExpirationStore(keyFunc, &TTLPolicy{ttl, clock.RealClock{}})
}

// NewExpirationStore 为给定策略创建并返回一个 ExpirationCache。
//
// 底层使用无索引的 ThreadSafeStore；若需要索引，应改用 NewIndexer。
func NewExpirationStore(keyFunc KeyFunc, expirationPolicy ExpirationPolicy) Store {
	return &ExpirationCache{
		cacheStorage:     NewThreadSafeStore(Indexers{}, Indices{}),
		keyFunc:          keyFunc,
		clock:            clock.RealClock{},
		expirationPolicy: expirationPolicy,
	}
}
