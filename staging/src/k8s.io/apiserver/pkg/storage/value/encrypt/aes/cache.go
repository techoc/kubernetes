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
	//
	// "过期"条目对我们始终有效：TTL 仅用于防止缓存无限增长，同一 info 对应的 transformer 不变。
	// 键始终对应完全相同的值；但要注意，
	// 由于用 value.Context.AuthenticatedData 覆盖旧键，
	// 必须检查 info 是否匹配，以确认 transformer 正确。
	cache.AllowExpiredGet = true
	return &simpleCache{
		cache: cache,
		ttl:   ttl,
	}
}

// 根据 key 返回 transformer；缓存中不存在则返回 nil
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

// set 按 key 缓存记录
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

// toString 为省分配使用了些"邪门"手段
//
// 仅当 b 由 make 分配且不会被原地修改时安全。
func toString(b []byte) string {
	// unsafe.SliceData 依据 cap，而这里需要依据 len
	if len(b) == 0 {
		return ""
	}
	// 摘自 go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
