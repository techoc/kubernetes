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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
)

// fakeThreadSafeMap 在删除时额外把 key 送入 channel，便于测试观察过期删除行为。
type fakeThreadSafeMap struct {
	ThreadSafeStore
	deletedKeys chan<- string
}

// Delete 只有在 deletedKeys 非空时才写入 channel，避免测试未消费 channel 时永久阻塞。
func (c *fakeThreadSafeMap) Delete(key string) {
	if c.deletedKeys != nil {
		c.ThreadSafeStore.Delete(key)
		c.deletedKeys <- key
	}
}

// FakeExpirationPolicy keeps the list for keys which never expires.
//
// 用作测试替身：通过 NeverExpire 显式控制哪些 key 会被判定为过期，
// 从而摆脱对真实时钟的依赖。
type FakeExpirationPolicy struct {
	NeverExpire     sets.Set[string]
	RetrieveKeyFunc KeyFunc
}

// IsExpired used to check if object is expired.
//
// 语义与真实的 ExpirationPolicy 一致：不在 NeverExpire 集合中的 key 即视为过期。
func (p *FakeExpirationPolicy) IsExpired(obj *TimestampedEntry) bool {
	key, _ := p.RetrieveKeyFunc(obj)
	return !p.NeverExpire.Has(key)
}

// NewFakeExpirationStore creates a new instance for the ExpirationCache.
//
// 直接构造 ExpirationCache 而非走 NewExpirationCache，以便注入 fake 存储和外部时钟，
// 让测试可以确定性地推进时间。
func NewFakeExpirationStore(keyFunc KeyFunc, deletedKeys chan<- string, expirationPolicy ExpirationPolicy, cacheClock clock.Clock) Store {
	cacheStorage := NewThreadSafeStore(Indexers{}, Indices{})
	return &ExpirationCache{
		cacheStorage:     &fakeThreadSafeMap{cacheStorage, deletedKeys},
		keyFunc:          keyFunc,
		clock:            cacheClock,
		expirationPolicy: expirationPolicy,
	}
}
