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

package devicetainteviction

import (
	"k8s.io/utils/lru"

	"k8s.io/apimachinery/pkg/types"
)

// uidCache 是基于 LRU 的 UID 集合缓存，用于记录已经处理过的设备 UID。
// 只关心 UID 是否存在，因此 value 统一存 nil；采用 LRU 可限制内存占用，
// 避免长期运行的控制器因设备反复创建删除而无界增长。
type uidCache struct {
	cache *lru.Cache
}

// newUIDCache 创建一个最多容纳 maxCacheEntries 个 UID 的缓存。
func newUIDCache(maxCacheEntries int) *uidCache {
	return &uidCache{
		cache: lru.New(maxCacheEntries),
	}
}

// add 将 UID 加入缓存，标记该设备已被处理。
func (c *uidCache) add(uid types.UID) {
	c.cache.Add(uid, nil)
}

// has 判断 UID 是否已在缓存中，即该设备此前是否已被处理过。
// Get 会刷新该条目的最近使用时间，使其在 LRU 中不被优先淘汰。
func (c *uidCache) has(uid types.UID) bool {
	_, found := c.cache.Get(uid)
	return found
}
