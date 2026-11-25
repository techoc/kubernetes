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

// modeIndexCache 是一个简单的“用户名 -> 模拟模式”缓存，前提假设是同一个用户
// 发出的所有模拟请求通常只用一种模拟模式。它记住某个用户名上次成功的模拟模式，
// 并在后续模拟检查中优先尝试该模式。这样 legacy 模拟的均摊成本保持不变，
// constrained 模拟的成本也只在常规模拟的既有检查之上多一次授权调用。
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
	// 对名字做哈希，使缓存大小可预测，不受用户名长度影响；
	// 碰撞在这里无关紧要，只会改变各模式的尝试顺序。
	h := fnv.New128a()
	h.Write([]byte(key))
	var sum [16]byte
	h.Sum(sum[:0])
	buf := make([]byte, 32) // 16 字节哈希 -> 32 个十六进制字符
	hex.Encode(buf, sum[:])
	return toString(buf) // 仅在 buf 由 make 分配堆内存时才安全
}

// newModeIndexCache 创建容量固定的 LRU，避免用户数增长导致内存无界。
func newModeIndexCache() *modeIndexCache {
	return &modeIndexCache{
		// 每个条目约 100 字节（哈希字符串键 + int 值 + LRU list/map 开销），
		// 即使 1 万条也只占约 1 MB 内存。
		// 写死容量既能记住大量用户，又不会持续泄漏内存。
		cache: lru.New(10_000),
	}
}

// impersonationCache 以很短的 TTL 记录某个模式下成功的模拟尝试。
//
// skipAttributes 为 false 时，映射关系是 [wantedUser, attributes] -> impersonatedUserInfo；
// skipAttributes 为 true 时，映射关系是 [wantedUser, requestor] -> impersonatedUserInfo。
//
// 因此每种 constrained 模拟模式都需要两个这样的缓存：
// 外层缓存把 skipAttributes 设为 false，覆盖整次模拟尝试，见 constrainedImpersonationModeState.check；
// 内层缓存把 skipAttributes 设为 true，只覆盖那些与具体请求无关的授权检查，
// 见 impersonationModeState.check。
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
	c.cache.Set(key, impersonatedUser, 10*time.Second) // 沿用 TokenSuccessCacheTTL 使用的相同短 TTL
}

// newImpersonationCache 创建缓存；skipAttributes 决定缓存键是否包含请求属性。
func newImpersonationCache(skipAttributes bool) *impersonationCache {
	return &impersonationCache{
		cache:          cache.NewExpiring(),
		skipAttributes: skipAttributes,
	}
}

// 构造缓存键时已知会使用到的 attribute 访问方法。如果这里编译失败，
// 说明缓存实现可能需要更新。
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

// 构造缓存键时已知会使用到的 user info 访问方法。如果这里编译失败，
// 说明缓存实现可能需要更新。
//
// 同上，user.Info 的方法集变化必须同步到 addUser。
var _ user.Info = (interface {
	GetName() string
	GetUID() string
	GetGroups() []string
	GetExtra() map[string][]string
})(nil)

// impersonationCacheKey 根据输入惰性构造字符串缓存键。
// skipAttributes 的语义见上文 impersonationCache 的说明。
// 注意：同一个 impersonationCacheKey 可以配合任意 skipAttributes 取值使用。
//
// 两种键按需惰性计算并就地缓存，避免一次检查里重复构造键。
type impersonationCacheKey struct {
	wantedUser *user.DefaultInfo
	attributes authorizer.Attributes

	// 在实际使用处才计算的值
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

	// 构造只包含请求者的假 attributes，以便复用 buildKey
	requestor := k.attributes.GetUser()
	attributes := authorizer.AttributesRecord{User: requestor}

	k.keyUser, k.errUser = buildKey(k.wantedUser, attributes)
	return k.keyUser, k.errUser
}

// buildKey 根据输入构造以请求者为命名空间的哈希字符串键。
// 使用密码学安全的哈希以尽量降低碰撞概率。
func buildKey(wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (string, error) {
	// 无法完整理解 attributes 时放弃缓存而不是冒险命中错误条目。
	fieldSelector, err := attributes.GetFieldSelector()
	if err != nil {
		return "", err // 无法完全理解 attributes 时直接跳过缓存
	}

	labelSelector, err := attributes.GetLabelSelector()
	if err != nil {
		return "", err // 无法完全理解 attributes 时直接跳过缓存
	}

	requestor := attributes.GetUser()

	// 哈希碰撞的概率小到不现实，而唯一可能提权的情形是拿到别人的缓存键。
	// 万一和自己的用户名发生碰撞，你也本来就有该权限——缓存只在模拟成功后才写入值。
	// 因此我们把请求者用户名放进缓存键。用户名由认证器控制，可以安全地假设用户无法左右它。
	// 其他输入大多由请求者掌控，但因为做了哈希，它们无法把缓存撑爆。
	// 把请求者名字作为键的命名空间前缀，可确保哈希碰撞不会跨用户复用授权结果。
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

// cacheKeyBuilder 用 uint32 长度前缀编码把结构化输入拼成二进制键，再用 SHA-256 散列。
// 所有方法共用同一个缓冲区，避免闭包和子 builder 的额外分配。
type cacheKeyBuilder struct {
	namespace string // 这里的 namespace 是编程概念，不是 Kubernetes 的 Namespace
	builder   []byte
}

// newCacheKeyBuilder 预分配 384 字节，覆盖典型键大小以减少扩容。
func newCacheKeyBuilder(namespace string) *cacheKeyBuilder {
	// 初始容量取一个合理值，减少反复扩容
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
	c.builder = append(c.builder, 0, 0, 0, 0) // uint32 长度的占位
	f(c)
	binary.BigEndian.PutUint32(c.builder[offset:], uint32(len(c.builder)-offset-4))
}

// build 对缓冲区做 SHA-256 并追加命名空间，得到定长键。
func (c *cacheKeyBuilder) build() string {
	hashed := sha256.Sum256(c.builder) // 散列以缩短缓存键，控制整体缓存体积
	// sha256 = 32 字节 -> 64 个十六进制字符 + "/" + namespace
	buf := make([]byte, 0, 64+1+len(c.namespace))
	buf = hex.AppendEncode(buf, hashed[:])
	buf = append(buf, '/')
	buf = append(buf, c.namespace...)
	return toString(buf) // 仅在 buf 由 make 分配堆内存时才安全
}

// toString 做一些“不干净”的操作来避免内存分配
//
// 仅在 b 由 make 分配、不会被原地修改时安全；调用点都满足这一前提。
func toString(b []byte) string {
	// unsafe.SliceData 依赖 cap，而我们需要的是 len
	if len(b) == 0 {
		return ""
	}
	// 复制自 go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
