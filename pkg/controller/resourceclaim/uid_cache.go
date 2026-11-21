/*
Copyright 2022 The Kubernetes Authors.

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

package resourceclaim

import (
	"k8s.io/utils/lru"

	"k8s.io/apimachinery/pkg/types"
)

// uidCache 是 ResourceClaim UID 的 LRU 缓存。
// 用于记住已经处理过的 UID，避免对同一对象重复执行昂贵操作；
// LRU 淘汰保证内存占用有上限。
type uidCache struct {
	cache *lru.Cache
}

// newUIDCache 创建一个最大容量为 maxCacheEntries 的 uidCache。
func newUIDCache(maxCacheEntries int) *uidCache {
	return &uidCache{
		cache: lru.New(maxCacheEntries),
	}
}

// Add 将 UID 写入缓存；超过容量时由 LRU 策略淘汰最久未使用的条目。
func (c *uidCache) Add(uid types.UID) {
	c.cache.Add(uid, nil)
}

// Has 判断 UID 是否在缓存中，用于识别此前已处理过的对象。
// 注意 Get 会刷新条目的使用时间，因此查询也会改变 LRU 顺序。
func (c *uidCache) Has(uid types.UID) bool {
	_, found := c.cache.Get(uid)
	return found
}
