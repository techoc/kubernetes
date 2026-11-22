/*
Copyright 2025 The Kubernetes Authors.

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

// Package impersonation 实现 HTTP 请求的模拟（impersonate）头处理，
// 包括传统 impersonation 与受约束 impersonation（constrained）两种模式。
package impersonation

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"time"
	"unsafe"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/cache"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/utils/lru"
)

// modeIndexCache is a simple username -> impersonation mode cache that is based on the assumption
// that a particular user is likely to use a single mode of impersonation for all impersonated requests
// that they make.  it remembers which impersonation mode was last successful for a username, and tries
// that mode first for future impersonation checks.  this makes it so that the amortized cost of legacy
// impersonation remains the same, and the cost of constrained impersonation is one extra authorization
// check in additional to the existing checks of regular impersonation.
//
// 该缓存只影响“先尝试哪种模式”，不改变授权结果；命中与否都只是性能优化。
type modeIndexCache struct {
	cache *lru.Cache
}

// get 返回该用户名上次成功的模式索引。
func (c *modeIndexCache) get(attributes authorizer.Attributes) (int, bool) {
	idx, ok := c.cache.Get(modeIndexCacheKey(attributes))
	if !ok {
		return 0, false
	}
	return idx.(int), true
}

// set 记录本次成功的模式索引。
func (c *modeIndexCache) set(attributes authorizer.Attributes, idx int) {
	c.cache.Add(modeIndexCacheKey(attributes), idx)
}

// modeIndexCacheKey 用用户名构造定长缓存键。
func modeIndexCacheKey(attributes authorizer.Attributes) string {
	key := attributes.GetUser().GetName()
	// hash the name so our cache size is predicable regardless of the size of usernames
	// collisions do not matter for this logic as it simply changes the ordering of the modes used
	h := fnv.New128a()
	h.Write([]byte(key))
	var sum [16]byte
	h.Sum(sum[:0])
	buf := make([]byte, 32) // 16 bytes of hash -> 32 hex chars
	hex.Encode(buf, sum[:])
	return toString(buf) // only safe because buf is heap allocated via make
}

// newModeIndexCache 创建容量固定的 LRU，避免用户数增长导致内存无界。
func newModeIndexCache() *modeIndexCache {
	return &modeIndexCache{
		// each entry is roughly ~100 bytes (hashed string key + int value + LRU list/map overhead)
		// thus at even 10k entries, we should use about 1 MB memory
		// this hardcoded size allows us to remember many users without leaking memory
		cache: lru.New(10_000),
	}
}

// impersonationCache tracks successful impersonation attempts for a given mode with a short TTL.
//
// when skipAttributes is false, it maps [wantedUser, attributes] -> impersonatedUserInfo
// when skipAttributes is true, it maps [wantedUser, requestor] -> impersonatedUserInfo
//
// thus each constrained impersonation mode needs two of these caches:
// the outer cache sets skipAttributes to false and thus covers the overall impersonation attempt, see constrainedImpersonationModeState.check.
// the inner cache sets skipAttributes to true and only covers the authorization checks that
// are not dependent on the specific request being made, see impersonationModeState.check.
//
// 外层缓存按完整请求属性命中，内层缓存只按请求者命中，用于复用与具体请求无关的授权结果。
type impersonationCache struct {
	cache          *cache.Expiring
	skipAttributes bool
}

// get 查询缓存；键构造失败时记录错误并视为未命中（fail-safe 地重新授权）。
func (c *impersonationCache) get(k *impersonationCacheKey) *impersonatedUserInfo {
	key, err := k.key(c.skipAttributes)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to build impersonation cache key: %w", err))
		return nil
	}
	impersonatedUser, ok := c.cache.Get(key)
	if !ok {
		return nil
	}
	return impersonatedUser.(*impersonatedUserInfo)
}

// set 写入缓存，TTL 固定 10s，与 TokenSuccessCacheTTL 保持一致，
// 以便权限撤销能在可接受的延迟内生效。
func (c *impersonationCache) set(k *impersonationCacheKey, impersonatedUser *impersonatedUserInfo) {
	key, err := k.key(c.skipAttributes)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to build impersonation cache key: %w", err))
		return
	}
	c.cache.Set(key, impersonatedUser, 10*time.Second) // hardcode the same short TTL as used by TokenSuccessCacheTTL
}

// newImpersonationCache 创建缓存；skipAttributes 决定缓存键是否包含请求属性。
func newImpersonationCache(skipAttributes bool) *impersonationCache {
	return &impersonationCache{
		cache:          cache.NewExpiring(),
		skipAttributes: skipAttributes,
	}
}

// The attribute accessors known to cache key construction. If this fails to compile, the cache
// implementation may need to be updated.
//
// 编译期约束：authorizer.Attributes 若新增方法，这里会失败，提醒同步更新 buildKey，
// 否则新字段可能被静默排除在缓存键之外，导致跨请求的错误命中。
var _ authorizer.Attributes = (interface {
	GetUser() user.Info
	GetVerb() string
	IsReadOnly() bool
	GetNamespace() string
	GetResource() string
	GetSubresource() string
	GetName() string
	GetAPIGroup() string
	GetAPIVersion() string
	IsResourceRequest() bool
	GetPath() string
	GetFieldSelector() (fields.Requirements, error)
	GetLabelSelector() (labels.Requirements, error)
})(nil)

// The user info accessors known to cache key construction. If this fails to compile, the cache
// implementation may need to be updated.
//
// 同上，user.Info 的方法集变化必须同步到 addUser。
var _ user.Info = (interface {
	GetName() string
	GetUID() string
	GetGroups() []string
	GetExtra() map[string][]string
})(nil)

// impersonationCacheKey allows for lazy building of string cache keys based on the inputs.
// See impersonationCache details above for the semantics around skipAttributes.
// Note that the same impersonationCacheKey can be used with any value for skipAttributes.
//
// 两种键按需惰性计算并就地缓存，避免一次检查里重复构造键。
type impersonationCacheKey struct {
	wantedUser *user.DefaultInfo
	attributes authorizer.Attributes

	// lazily calculated values at point of use
	keyAttr string
	errAttr error
	keyUser string
	errUser error
}

// key 根据 skipAttributes 选择带属性或不带属性的键。
func (k *impersonationCacheKey) key(skipAttributes bool) (string, error) {
	if skipAttributes {
		return k.keyWithoutAttributes()
	}
	return k.keyWithAttributes()
}

// keyWithAttributes 返回包含完整请求属性的键，结果被缓存。
func (k *impersonationCacheKey) keyWithAttributes() (string, error) {
	if len(k.keyAttr) != 0 || k.errAttr != nil {
		return k.keyAttr, k.errAttr
	}

	k.keyAttr, k.errAttr = buildKey(k.wantedUser, k.attributes)
	return k.keyAttr, k.errAttr
}

// keyWithoutAttributes 只使用请求者身份构造键：构造一个仅含 User 的假 attributes，
// 从而复用 buildKey 的同一套编码逻辑。
func (k *impersonationCacheKey) keyWithoutAttributes() (string, error) {
	if len(k.keyUser) != 0 || k.errUser != nil {
		return k.keyUser, k.errUser
	}

	// fake attributes that just contain the requestor to allow us to reuse buildKey
	requestor := k.attributes.GetUser()
	attributes := authorizer.AttributesRecord{User: requestor}

	k.keyUser, k.errUser = buildKey(k.wantedUser, attributes)
	return k.keyUser, k.errUser
}

// buildKey creates a hashed string key based on the inputs that is namespaced to the requestor.
// A cryptographically secure hash is used to minimize the chance of collisions.
func buildKey(wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (string, error) {
	// 无法完整理解 attributes 时放弃缓存而不是冒险命中错误条目。
	fieldSelector, err := attributes.GetFieldSelector()
	if err != nil {
		return "", err // if we do not fully understand the attributes, just skip caching altogether
	}

	labelSelector, err := attributes.GetLabelSelector()
	if err != nil {
		return "", err // if we do not fully understand the attributes, just skip caching altogether
	}

	requestor := attributes.GetUser()

	// the chance of a hash collision is impractically small, but the only way that would lead to a
	// privilege escalation is if you could get the cache key of a different user.  if you somehow
	// get a collision with your own username, you already have that permission since we only set
	// values in the cache after a successful impersonation.  Thus, we include the requestor
	// username in the cache key.  It is safe to assume that a user has no control over their own
	// username since that is controlled by the authenticator.  Even though many of the other inputs
	// are under the control of the requestor, they cannot explode the cache due to the hashing.
	//
	// 把请求者名字放进键前缀并作为命名空间，确保哈希碰撞不会跨用户复用授权结果。
	b := newCacheKeyBuilder(requestor.GetName())

	addUser(b, wantedUser)
	addUser(b, requestor)

	// 请求属性用长度前缀编码，避免拼接歧义（例如 "a"+"bc" 与 "ab"+"c"）。
	b.addLengthPrefixed(func(b *cacheKeyBuilder) {
		b.
			addString(attributes.GetVerb()).
			addBool(attributes.IsReadOnly()).
			addString(attributes.GetNamespace()).
			addString(attributes.GetResource()).
			addString(attributes.GetSubresource()).
			addString(attributes.GetName()).
			addString(attributes.GetAPIGroup()).
			addString(attributes.GetAPIVersion()).
			addBool(attributes.IsResourceRequest()).
			addString(attributes.GetPath())
	})

	b.addLengthPrefixed(func(b *cacheKeyBuilder) {
		for _, req := range fieldSelector {
			b.addStringSlice([]string{req.Field, string(req.Operator), req.Value})
		}
	})

	b.addLengthPrefixed(func(b *cacheKeyBuilder) {
		for _, req := range labelSelector {
			b.addString(req.String())
		}
	})

	return b.build(), nil
}

// addUser 把用户身份的全部字段编码进键。
func addUser(b *cacheKeyBuilder, u user.Info) {
	b.addLengthPrefixed(func(b *cacheKeyBuilder) {
		b.
			addString(u.GetName()).
			addString(u.GetUID()).
			addStringSlice(u.GetGroups()).
			addLengthPrefixed(func(b *cacheKeyBuilder) {
				extra := u.GetExtra()
				// extra 是 map，必须排序遍历，否则同一用户会生成不同的键。
				for _, key := range sets.StringKeySet(extra).List() {
					b.addString(key)
					b.addStringSlice(extra[key])
				}
			})
	})
}

// cacheKeyBuilder builds a binary key from structured inputs using uint32 length-prefixed encoding, then hashes
// it with SHA-256.  All methods operate on a single shared buffer to avoid closure and child-builder allocations.
type cacheKeyBuilder struct {
	namespace string // in the programming sense, not the Kubernetes concept
	builder   []byte
}

// newCacheKeyBuilder 预分配 384 字节，覆盖典型键大小以减少扩容。
func newCacheKeyBuilder(namespace string) *cacheKeyBuilder {
	// start with a reasonable size to avoid too many allocations
	return &cacheKeyBuilder{namespace: namespace, builder: make([]byte, 0, 384)}
}

// addString 以长度前缀方式追加字符串。
func (c *cacheKeyBuilder) addString(value string) *cacheKeyBuilder {
	c.addLengthPrefixed(func(c *cacheKeyBuilder) {
		c.builder = append(c.builder, value...)
	})
	return c
}

// addStringSlice 以长度前缀方式追加字符串切片，每个元素自身也带长度前缀。
func (c *cacheKeyBuilder) addStringSlice(values []string) *cacheKeyBuilder {
	c.addLengthPrefixed(func(c *cacheKeyBuilder) {
		for _, v := range values {
			c.addString(v)
		}
	})
	return c
}

// addBool 用单字节 0/1 编码布尔值。
func (c *cacheKeyBuilder) addBool(value bool) *cacheKeyBuilder {
	var b byte
	if value {
		b = 1
	}
	c.builder = append(c.builder, b)
	return c
}

// builderContinuation 是在长度前缀区间内写数据的回调。
type builderContinuation func(child *cacheKeyBuilder)

// addLengthPrefixed 先占位 4 字节长度，执行回调后回填实际长度。
func (c *cacheKeyBuilder) addLengthPrefixed(f builderContinuation) {
	offset := len(c.builder)
	c.builder = append(c.builder, 0, 0, 0, 0) // placeholder for uint32 length
	f(c)
	binary.BigEndian.PutUint32(c.builder[offset:], uint32(len(c.builder)-offset-4))
}

// build 对缓冲区做 SHA-256 并追加命名空间，得到定长键。
func (c *cacheKeyBuilder) build() string {
	hashed := sha256.Sum256(c.builder) // reduce the size of the cache key to keep the overall cache size small
	// sha256 = 32 bytes -> 64 hex chars + "/" + namespace
	buf := make([]byte, 0, 64+1+len(c.namespace))
	buf = hex.AppendEncode(buf, hashed[:])
	buf = append(buf, '/')
	buf = append(buf, c.namespace...)
	return toString(buf) // only safe because buf is heap allocated via make
}

// toString performs unholy acts to avoid allocations
//
// 仅在 b 由 make 分配、不会被原地修改时安全；调用点都满足这一前提。
func toString(b []byte) string {
	// unsafe.SliceData relies on cap whereas we want to rely on len
	if len(b) == 0 {
		return ""
	}
	// Copied from go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
