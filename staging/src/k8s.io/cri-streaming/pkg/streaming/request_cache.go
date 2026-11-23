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

package streaming

import (
	"container/list"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

var (
	// cacheTTL is the timeout after which tokens become invalid.
	// cacheTTL 是 token 的有效期（1 分钟）。超过该时间后，token 失效，对应请求无法再被取出。
	cacheTTL = 1 * time.Minute
	// maxInFlight is the maximum number of in-flight requests to allow.
	// maxInFlight 是缓存允许同时存在的最大请求数，用于防止恶意请求耗尽内存（DoS 防护）。
	maxInFlight = 1000
	// tokenLen is the length of the random base64 encoded token identifying the request.
	// tokenLen 是用于标识请求的随机 URL 安全 base64 token 的长度（字符数）。
	tokenLen = 8
)

// requestCache caches streaming (exec/attach/port-forward) requests and generates a single-use
// random token for their retrieval. The requestCache is used for building streaming URLs without
// the need to encode every request parameter in the URL.
//
// requestCache 缓存 exec/attach/port-forward 的流式请求，并为每个请求生成一个一次性随机 token。
// 这样 kubectl 等客户端只需携带简短的 token 即可取回完整请求，无需把所有参数编码进 URL
// （这些参数可能包含敏感信息或过大的结构体）。
// 数据结构说明：
//   - tokens：token 到链表元素的映射，提供 O(1) 查找；
//   - ll：按插入时间排序的双向链表，表头最新、表尾最旧，便于按时间顺序清理过期请求；
//   - lock：保护两个数据结构的并发访问。
type requestCache struct {
	// clock is used to obtain the current time
	// clock 用于获取当前时间，测试时可替换为 FakeClock 来模拟时间流逝。
	clock clock.Clock

	// tokens maps the generate token to the request for fast retrieval.
	// tokens 把 token 映射到链表元素，实现快速定位。
	tokens map[string]*list.Element
	// ll maintains an age-ordered request list for faster garbage collection of expired requests.
	// ll 维护按时间排序的请求链表，最旧的在尾部，便于高效回收过期项。
	ll *list.List

	// lock 保证 Insert/Consume/gc 对共享结构的互斥访问。
	lock sync.Mutex
}

// Type representing an *ExecRequest, *AttachRequest, or *PortForwardRequest.
// request 表示可以缓存的请求类型，实际为 *ExecRequest、*AttachRequest 或 *PortForwardRequest 之一。
type request interface{}

// cacheEntry 是缓存中的单个条目，包含 token、原始请求和过期时间。
// token 字段带 datapolicy:"token" 标签，用于数据策略标记（敏感信息）。
type cacheEntry struct {
	token      string `datapolicy:"token"`
	req        request
	expireTime time.Time
}

// newRequestCache 创建并初始化一个 requestCache：
// 使用真实时钟、空链表和空的 token 映射。
func newRequestCache() *requestCache {
	return &requestCache{
		clock:  clock.RealClock{},
		ll:     list.New(),
		tokens: make(map[string]*list.Element),
	}
}

// Insert the given request into the cache and returns the token used for fetching it out.
// Insert 把请求存入缓存，并返回可用于取回它的 token。
//
// 处理流程：
//  1. 加锁；
//  2. 先执行 gc 清理过期条目，腾出空间；
//  3. 若缓存已满（达到 maxInFlight），返回 ResourceExhausted 错误；
//  4. 生成唯一 token；
//  5. 构造条目（过期时间为当前时间 + cacheTTL）插入链表头部，并登记到 token 映射。
func (c *requestCache) Insert(req request) (token string, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove expired entries.
	// 先回收过期条目，避免误判缓存已满。
	c.gc()
	// If the cache is full, reject the request.
	// 缓存已满，拒绝新请求以防御 DoS。
	if c.ll.Len() == maxInFlight {
		return "", NewErrorTooManyInFlight()
	}
	// 生成不与现有 token 冲突的随机 token。
	token, err = c.uniqueToken()
	if err != nil {
		return "", err
	}
	// 新条目插入链表头部（最新），过期时间为当前时间 + TTL。
	ele := c.ll.PushFront(&cacheEntry{token, req, c.clock.Now().Add(cacheTTL)})

	c.tokens[token] = ele
	return token, nil
}

// Consume the token (remove it from the cache) and return the cached request, if found.
// Consume 根据 token 取出并删除对应请求——token 是一次性的，消费后即失效。
//
// 处理流程：
//  1. 加锁并在 tokens 中查找 token，不存在直接返回 false；
//  2. 从链表和映射中同时删除该条目；
//  3. 检查是否已过期：过期则返回 false（请求依然被删除，不会残留在缓存中）；
//  4. 否则返回缓存的请求。
func (c *requestCache) Consume(token string) (req request, found bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	ele, ok := c.tokens[token]
	// token 不存在（从未生成、已被消费或已被 gc 清理）。
	if !ok {
		return nil, false
	}
	// 无论是否过期，都先把条目从两个数据结构中移除。
	c.ll.Remove(ele)
	delete(c.tokens, token)

	entry := ele.Value.(*cacheEntry)
	if c.clock.Now().After(entry.expireTime) {
		// Entry already expired.
		// 条目已过期，视为不存在。
		return nil, false
	}
	return entry.req, true
}

// uniqueToken generates a random URL-safe token and ensures uniqueness.
// uniqueToken 生成一个 URL 安全的随机 token，并确保它在当前缓存中唯一。
//
// 实现细节：
//   - 先根据目标字符数 tokenLen 反推需要的随机字节数（base64 每 6 位编码 1 个字符，故乘 6/8 并向上取整）；
//   - 用 crypto/rand 填充随机字节，再做 RawURLEncoding（无填充、URL 安全）编码，截取前 tokenLen 个字符；
//   - 最多尝试 10 次，全部冲突则返回错误。
//
// 注意：此处检查唯一性时用的是完整编码串 encoded，而返回的是其前 tokenLen 个字符 token，
// 这是代码现状（截取后未再单独校验截断结果的唯一性）。
func (c *requestCache) uniqueToken() (string, error) {
	const maxTries = 10
	// Number of bytes to be tokenLen when base64 encoded.
	// 算出编码后能达到 tokenLen 个字符所需的随机字节数。
	tokenSize := math.Ceil(float64(tokenLen) * 6 / 8)
	rawToken := make([]byte, int(tokenSize))
	for i := 0; i < maxTries; i++ {
		// 从密码学安全随机源读取字节。
		if _, err := rand.Read(rawToken); err != nil {
			return "", err
		}
		// 使用 URL 安全的 base64 编码（无 = 填充），并截取指定长度。
		encoded := base64.RawURLEncoding.EncodeToString(rawToken)
		token := encoded[:tokenLen]
		// If it's unique, return it. Otherwise retry.
		// 与已有 token 不冲突则返回，否则重试。
		if _, exists := c.tokens[encoded]; !exists {
			return token, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique token")
}

// Must be write-locked prior to calling.
// gc 回收所有过期条目。
// 调用前提：调用方必须已持有写锁（Insert 会在锁内调用它）。
//
// 由于链表按时间排序，最旧条目在尾部，因此只需从尾部开始逐个检查：
// 遇到未过期的条目即可停止（后面的更新鲜，必然也未过期）。
func (c *requestCache) gc() {
	now := c.clock.Now()
	for c.ll.Len() > 0 {
		oldest := c.ll.Back()
		entry := oldest.Value.(*cacheEntry)
		// 最旧的都未过期，则其余都未过期，直接结束。
		if !now.After(entry.expireTime) {
			return
		}

		// Oldest value is expired; remove it.
		// 最旧条目已过期：同时从链表和 token 映射中删除。
		c.ll.Remove(oldest)
		delete(c.tokens, entry.token)
	}
}
