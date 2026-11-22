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

package eventratelimit

import (
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/lru"
)

// cache 是按特定类型缓存速率限制器的接口。
type cache interface {
	// get 返回与指定 key 关联的速率限制器。
	get(key interface{}) flowcontrol.RateLimiter
}

// singleCache 是只保存一个固定对象的缓存。
type singleCache struct {
	// 缓存持有的唯一速率限制器。
	rateLimiter flowcontrol.RateLimiter
}

// get 返回缓存中唯一的速率限制器，与 key 无关。
func (c *singleCache) get(key interface{}) flowcontrol.RateLimiter {
	return c.rateLimiter
}

// lruCache 是基于最近最少使用策略的缓存。
type lruCache struct {
	// 用于创建新速率限制器的工厂函数。
	rateLimiterFactory func() flowcontrol.RateLimiter
	// 底层 LRU 缓存。
	cache *lru.Cache
}

// get 返回 key 对应的速率限制器；不存在时先创建再缓存，避免重复构造。
func (c *lruCache) get(key interface{}) flowcontrol.RateLimiter {
	value, found := c.cache.Get(key)
	if !found {
		rateLimter := c.rateLimiterFactory()
		c.cache.Add(key, rateLimter)
		return rateLimter
	}
	return value.(flowcontrol.RateLimiter)
}
