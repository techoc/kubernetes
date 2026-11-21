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

package garbagecollector

import (
	"k8s.io/utils/lru"
)

// ReferenceCache 是针对 uid（以 objectReference 表示）的 LRU 缓存。
// 垃圾回收器用它记住已处理过的属主引用：缓存命中即说明该属主此前已入图，
// 无需重复构建节点，从而避免依赖图中的重复处理。
type ReferenceCache struct {
	cache *lru.Cache
}

// NewReferenceCache 创建一个最多缓存 maxCacheEntries 个引用的 ReferenceCache。
// 设置容量上限是为了防止随集群对象增长而无限占用内存。
func NewReferenceCache(maxCacheEntries int) *ReferenceCache {
	return &ReferenceCache{
		cache: lru.New(maxCacheEntries),
	}
}

// Add 将一个 uid（以 objectReference 表示）加入缓存。
func (c *ReferenceCache) Add(reference objectReference) {
	c.cache.Add(reference, nil)
}

// Has 判断某个 uid（以 objectReference 表示）是否在缓存中。
func (c *ReferenceCache) Has(reference objectReference) bool {
	_, found := c.cache.Get(reference)
	return found
}
