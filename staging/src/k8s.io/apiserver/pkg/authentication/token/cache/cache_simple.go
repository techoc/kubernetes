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

// Package cache 为 token 认证结果提供有界、带 TTL 的内存缓存，避免每个
// 请求都去调用远端 token 校验服务。
package cache

import (
	"time"

	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/utils/clock"
)

// simpleCache 是 cache 接口的单实现，直接包装带过期时间的通用缓存。
// 使用注入的 clock 是为了让测试能够控制时间推进，而不必真实等待 TTL。
type simpleCache struct {
	cache *utilcache.Expiring
}

// newSimpleCache 创建底层基于 Expiring 的缓存。Expiring 自身保证并发
// 安全，因此 simpleCache 无需额外加锁。
func newSimpleCache(clock clock.Clock) cache {
	return &simpleCache{cache: utilcache.NewExpiringWithClock(clock)}
}

// get 取出缓存记录并做类型断言。命中但类型不符时返回 false，避免把
// 损坏的数据当作有效认证结果使用。
func (c *simpleCache) get(key string) (*cacheRecord, bool) {
	record, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	value, ok := record.(*cacheRecord)
	return value, ok
}

// set 写入记录并设置 TTL；过期后由底层缓存自动淘汰。
func (c *simpleCache) set(key string, value *cacheRecord, ttl time.Duration) {
	c.cache.Set(key, value, ttl)
}

// remove 主动删除记录，用于 token 被撤销等需要立即失效的场景。
func (c *simpleCache) remove(key string) {
	c.cache.Delete(key)
}
