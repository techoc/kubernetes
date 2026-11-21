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

package cache

import (
	"time"

	expirationcache "k8s.io/client-go/tools/cache"
)

// ObjectCache 是过期缓存的简单封装：
// 1. 使用字符串类型 key；
// 2. 缓存过期时通过 updater 直接取值；
// 3. 然后把新值写回缓存。
type ObjectCache struct {
	cache   expirationcache.Store
	updater func() (interface{}, error)
}

// objectEntry 是带字符串 key 的缓存条目。
type objectEntry struct {
	key string
	obj interface{}
}

// NewObjectCache 创建带 updater 的 ObjectCache；updater 返回需要缓存的对象。
func NewObjectCache(f func() (interface{}, error), ttl time.Duration) *ObjectCache {
	return &ObjectCache{
		updater: f,
		cache:   expirationcache.NewTTLStore(stringKeyFunc, ttl),
	}
}

// stringKeyFunc 使用条目自身的字符串 key 作为缓存键。
func stringKeyFunc(obj interface{}) (string, error) {
	key := obj.(objectEntry).key
	return key, nil
}

// Get 按字符串 key 获取缓存对象；缓存未命中时调用 updater 取值并写回缓存。
func (c *ObjectCache) Get(key string) (interface{}, error) {
	value, ok, err := c.cache.Get(objectEntry{key: key})
	if err != nil {
		return nil, err
	}
	if !ok {
		obj, err := c.updater()
		if err != nil {
			return nil, err
		}
		// 未命中时把 updater 的结果写回缓存，供后续请求复用。
		err = c.cache.Add(objectEntry{
			key: key,
			obj: obj,
		})
		if err != nil {
			return nil, err
		}
		return obj, nil
	}
	return value.(objectEntry).obj, nil
}

// Add 按字符串 key 写入缓存对象。
func (c *ObjectCache) Add(key string, obj interface{}) error {
	return c.cache.Add(objectEntry{key: key, obj: obj})
}
