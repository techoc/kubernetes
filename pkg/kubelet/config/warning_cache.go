/*
Copyright The Kubernetes Authors.

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

package config

import (
	"sync"

	"k8s.io/utils/lru"
)

// warningCache 是用于对静态 Pod 告警消息去重的 LRU 缓存。
type warningCache struct {
	cache *lru.Cache
	mu    sync.Mutex
}

// newWarningCache 创建指定容量的告警缓存。
func newWarningCache(maxCacheEntries int) *warningCache {
	return &warningCache{
		cache: lru.New(maxCacheEntries),
	}
}

// addIfAbsent 原子地检查告警是否已存在，不存在则加入。
// 首次出现返回 true，已存在返回 false。
// 用互斥锁保证检查与写入的原子性，避免并发解析重复告警。
func (c *warningCache) addIfAbsent(warning string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 告警已存在则不再重复输出。
	if _, found := c.cache.Get(warning); found {
		return false
	}

	// 首次出现，记入缓存。
	c.cache.Add(warning, nil)
	return true
}
