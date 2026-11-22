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

package aes

import (
	"bytes"
	"time"
	"unsafe"

	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/utils/clock"
)

// simpleCache 缓存 authenticated data 到 transformer 的映射。
//
// 由于同一 authenticated data 对应的 transformer 永远相同，缓存过期只用于控制内存，
// 因此允许读取过期条目（AllowExpiredGet）。
type simpleCache struct {
	cache *utilcache.Expiring
	ttl   time.Duration
}

// newSimpleCache 创建缓存并开启 AllowExpiredGet。
func newSimpleCache(clock clock.Clock, ttl time.Duration) *simpleCache {
	cache := utilcache.NewExpiringWithClock(clock)
	// "Stale" entries are always valid for us because the TTL is just used to prevent
	// unbounded growth on the cache - for a given info the transformer is always the same.
	// The key always corresponds to the exact same value, with the caveat that
	// since we use the value.Context.AuthenticatedData to overwrite old keys,
	// we always have to check that the info matches (to validate the transformer is correct).
	cache.AllowExpiredGet = true
	return &simpleCache{
		cache: cache,
		ttl:   ttl,
	}
}

// given a key, return the transformer, or nil if it does not exist in the cache
//
// 除了 key 匹配，还必须比对 info：AuthenticatedData 可能被复用/覆盖，
// 只有 info 一致才能保证 transformer 正确。
func (c *simpleCache) get(info []byte, dataCtx value.Context) *transformerWithInfo {
	val, ok := c.cache.Get(keyFunc(dataCtx))
	if !ok {
		return nil
	}

	transformer := val.(*transformerWithInfo)

	if !bytes.Equal(transformer.info, info) {
		return nil
	}

	return transformer
}

// set caches the record for the key
//
// 这些 panic 是内部不变式：空 key 或 nil transformer 说明调用方逻辑有误，
// 继续运行会导致加解密错误，不如尽早暴露。
func (c *simpleCache) set(dataCtx value.Context, transformer *transformerWithInfo) {
	if dataCtx == nil || len(dataCtx.AuthenticatedData()) == 0 {
		panic("authenticated data must not be empty")
	}
	if transformer == nil {
		panic("transformer must not be nil")
	}
	if len(transformer.info) == 0 {
		panic("info must not be empty")
	}
	c.cache.Set(keyFunc(dataCtx), transformer, c.ttl)
}

// keyFunc 直接用 authenticated data 作为缓存键。
func keyFunc(dataCtx value.Context) string {
	return toString(dataCtx.AuthenticatedData())
}

// toString performs unholy acts to avoid allocations
//
// 仅当 b 由 make 分配且不会被原地修改时安全。
func toString(b []byte) string {
	// unsafe.SliceData relies on cap whereas we want to rely on len
	if len(b) == 0 {
		return ""
	}
	// Copied from go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
