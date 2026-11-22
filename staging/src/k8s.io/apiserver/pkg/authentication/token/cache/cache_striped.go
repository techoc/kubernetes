/*
Copyright 2017 The Kubernetes Authors.

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
	"hash/fnv"
	"time"
)

// split cache lookups across N striped caches
// 分片降低单把锁的争用；每个分片内部各自加锁，互不影响。
type stripedCache struct {
	stripeCount uint32
	hashFunc    func(string) uint32
	caches      []cache
}

// hashFunc 把 key 映射到分片索引；返回值还需对 stripeCount 取模。
type hashFunc func(string) uint32

// newCacheFunc 用于构造每个分片，便于测试注入不同的 cache 实现。
type newCacheFunc func() cache

// newStripedCache 创建 stripeCount 个独立缓存并预先分配切片，避免运行时扩容。
func newStripedCache(stripeCount int, hash hashFunc, newCacheFunc newCacheFunc) cache {
	caches := []cache{}
	for i := 0; i < stripeCount; i++ {
		caches = append(caches, newCacheFunc())
	}
	return &stripedCache{
		stripeCount: uint32(stripeCount),
		hashFunc:    hash,
		caches:      caches,
	}
}

// get 按 key 定位分片后读取；分片索引计算必须与 set/remove 完全一致。
func (c *stripedCache) get(key string) (*cacheRecord, bool) {
	return c.caches[c.hashFunc(key)%c.stripeCount].get(key)
}

// set 写入 key 对应的分片，并交给该分片处理 TTL。
func (c *stripedCache) set(key string, value *cacheRecord, ttl time.Duration) {
	c.caches[c.hashFunc(key)%c.stripeCount].set(key, value, ttl)
}

// remove 从 key 对应的分片删除条目。
func (c *stripedCache) remove(key string) {
	c.caches[c.hashFunc(key)%c.stripeCount].remove(key)
}

// fnvHashFunc 使用 FNV-1a 的 32 位变体做快速散列；不用于安全场景，仅做均匀分布。
func fnvHashFunc(key string) uint32 {
	f := fnv.New32()
	f.Write([]byte(key))
	return f.Sum32()
}
