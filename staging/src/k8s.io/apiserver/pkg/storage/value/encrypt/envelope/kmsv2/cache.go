/*
Copyright 2023 The Kubernetes Authors.

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

// Package kmsv2 transforms values for storage at rest using a Envelope v2 provider
package kmsv2

import (
	"crypto/sha256"
	"hash"
	"sync"
	"time"
	"unsafe"

	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/utils/clock"
)

// simpleCache 存储 value.Transformer 中负责解密的子集（value.Read），以此在类型层面
// 强制缓存中的 transformer 不被用于加密。这在 nonce 冲突的语境下尤为重要：由从 etcd
// 取回的加密 DEK 构造出的 transformer 无法维护自身的 nonce 计数器状态。
// 只缓存 value.Read 可以从类型上禁止拿缓存里的 transformer 去加密，避免
// 从 etcd 恢复的 DEK 因 nonce 计数器状态丢失而重复 nonce（GCM 下是灾难性的）。
type simpleCache struct {
	cache *utilcache.Expiring
	ttl   time.Duration
	// hashPool 是每个 cache 独有的 hash.Hash 对象池（避免构造 Hash 带来的分配），
	// 并使用 SHA-256 以防止哈希碰撞。
	// 缓存键是 DEK 的 SHA-256，既压缩内存又避免把明文 DEK 作为 map 键。
	hashPool        *sync.Pool
	providerName    string
	mu              sync.Mutex                          // guards call to set
	recordCacheSize func(providerName string, size int) // for unit tests
}

// newSimpleCache 构造带过期时间的缓存；AllowExpiredGet 是安全的，因为
// 对同一密文 DEK，解出的明文 DEK 恒定，过期后仍可复用。
func newSimpleCache(clock clock.Clock, ttl time.Duration, providerName string) *simpleCache {
	cache := utilcache.NewExpiringWithClock(clock)
	cache.AllowExpiredGet = true // for a given key, the value (the decryptTransformer) is always the same
	return &simpleCache{
		cache: cache,
		ttl:   ttl,
		hashPool: &sync.Pool{
			New: func() interface{} {
				return sha256.New()
			},
		},
		providerName:    providerName,
		recordCacheSize: metrics.RecordDekSourceCacheSize,
	}
}

// given a key, return the transformer, or nil if it does not exist in the cache
func (c *simpleCache) get(key []byte) value.Read {
	record, ok := c.cache.Get(c.keyFunc(key))
	if !ok {
		return nil
	}
	return record.(value.Read)
}

// set 为给定 key 缓存这条记录。
// panic 而非返回错误：空 key/空 transformer 说明调用方逻辑错误，继续运行
// 只会把问题推迟到更难排查的解密失败。
func (c *simpleCache) set(key []byte, transformer value.Read) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(key) == 0 {
		panic("key must not be empty")
	}
	if transformer == nil {
		panic("transformer must not be nil")
	}
	c.cache.Set(c.keyFunc(key), transformer, c.ttl)
	// Add metrics for cache size
	c.recordCacheSize(c.providerName, c.cache.Len())
}

// keyFunc 通过对输入做哈希生成字符串键，从而降低缓存的内存占用。
// 复用 hash.Hash 并跳过 base64，是为了在高频读取路径上减少内存分配。
func (c *simpleCache) keyFunc(s []byte) string {
	h := c.hashPool.Get().(hash.Hash)
	h.Reset()

	if _, err := h.Write(s); err != nil {
		panic(err) // Write() on hash never fails
	}
	key := toString(h.Sum(nil)) // skip base64 encoding to save an allocation
	c.hashPool.Put(h)

	return key
}

// toString 为避免内存分配而执行了一些“不洁”的 unsafe 操作。
// 依赖 unsafe 把字节切片零拷贝转成字符串；调用方必须保证 b 之后不再被修改，
// hash.Sum 返回的正是刚分配且不再复用的切片，满足该前提。
func toString(b []byte) string {
	// unsafe.SliceData relies on cap whereas we want to rely on len
	if len(b) == 0 {
		return ""
	}
	// Copied from go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
