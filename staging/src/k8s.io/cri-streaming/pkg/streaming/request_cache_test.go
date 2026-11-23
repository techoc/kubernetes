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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testingclock "k8s.io/utils/clock/testing"
)

// TestInsert 验证 Insert 的行为：token 长度正确、缓存容量上限生效、满额时返回 429 错误。
// 同时验证：消费掉一个请求后可以继续插入，而最旧的请求在整个过程中始终有效（不会被提前淘汰）。
func TestInsert(t *testing.T) {
	c, _ := newTestCache()

	// Insert normal
	// 第一次插入：记录下 token，后续作为"最旧请求"的标识。
	oldestTok, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assert.Len(t, oldestTok, tokenLen)
	assertCacheSize(t, c, 1)

	// Insert until full
	// 持续插入到容量上限减 1：这里的循环次数保证加上前面的 1 条后还差 1 条满额。
	for i := 0; i < maxInFlight-2; i++ {
		tok, err := c.Insert(nextRequest())
		require.NoError(t, err)
		assert.Len(t, tok, tokenLen)
	}
	assertCacheSize(t, c, maxInFlight-1)

	// 插入最后一条，使缓存达到满额。
	newestReq := nextRequest()
	newestTok, err := c.Insert(newestReq)
	require.NoError(t, err)
	assert.Len(t, newestTok, tokenLen)
	assertCacheSize(t, c, maxInFlight)
	// 满额时最旧的请求仍在，说明没有发生淘汰。
	require.Contains(t, c.tokens, oldestTok, "oldest request should still be cached")

	// Consume newest token.
	// 消费最新请求后腾出一个空位，最旧请求应不受影响。
	req, ok := c.Consume(newestTok)
	assert.True(t, ok, "newest request should still be cached")
	assert.Equal(t, newestReq, req)
	require.Contains(t, c.tokens, oldestTok, "oldest request should still be cached")

	// Insert again (still full)
	// 再次插入填满刚腾出的空位，应当成功。
	tok, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assert.Len(t, tok, tokenLen)
	assertCacheSize(t, c, maxInFlight)

	// Insert again (should evict)
	// 缓存已满时再插入应被拒绝，并映射为 HTTP 429 + Retry-After 响应。
	_, err = c.Insert(nextRequest())
	assert.Error(t, err, "should reject further requests")
	recorder := httptest.NewRecorder()
	require.NoError(t, WriteError(err, recorder))
	errResponse := recorder.Result()
	assert.Equal(t, http.StatusTooManyRequests, errResponse.StatusCode)
	assert.Equal(t, strconv.Itoa(int(cacheTTL.Seconds())), errResponse.Header.Get("Retry-After"))

	// 被拒绝的请求不应改变缓存内容，最旧请求依然可取。
	assertCacheSize(t, c, maxInFlight)
	_, ok = c.Consume(oldestTok)
	assert.True(t, ok, "oldest request should be valid")
}

// TestConsume 覆盖 Consume 的各类场景：正常消费、乱序消费、重复消费、无 token 消费、消费过期条目。
// 每种子场景都用独立代码块隔离状态，并校验缓存大小的正确变化。
func TestConsume(t *testing.T) {
	c, clock := newTestCache()

	{ // Insert & consume.
		// 场景一：插入后立即消费，应取回原请求且缓存清空。
		req := nextRequest()
		tok, err := c.Insert(req)
		require.NoError(t, err)
		assertCacheSize(t, c, 1)

		cachedReq, ok := c.Consume(tok)
		assert.True(t, ok)
		assert.Equal(t, req, cachedReq)
		assertCacheSize(t, c, 0)
	}

	{ // Insert & consume out of order
		// 场景二：乱序消费。先插入 req1、req2，再先消费 req2、后消费 req1。
		// 验证 token 映射与链表顺序互不影响，两个请求都能正确取回。
		req1 := nextRequest()
		tok1, err := c.Insert(req1)
		require.NoError(t, err)
		assertCacheSize(t, c, 1)

		req2 := nextRequest()
		tok2, err := c.Insert(req2)
		require.NoError(t, err)
		assertCacheSize(t, c, 2)

		cachedReq2, ok := c.Consume(tok2)
		assert.True(t, ok)
		assert.Equal(t, req2, cachedReq2)
		assertCacheSize(t, c, 1)

		cachedReq1, ok := c.Consume(tok1)
		assert.True(t, ok)
		assert.Equal(t, req1, cachedReq1)
		assertCacheSize(t, c, 0)
	}

	{ // Consume a second time
		// 场景三：同一 token 消费两次。第一次成功，第二次应失败（token 是一次性的）。
		req := nextRequest()
		tok, err := c.Insert(req)
		require.NoError(t, err)
		assertCacheSize(t, c, 1)

		cachedReq, ok := c.Consume(tok)
		assert.True(t, ok)
		assert.Equal(t, req, cachedReq)
		assertCacheSize(t, c, 0)

		_, ok = c.Consume(tok)
		assert.False(t, ok)
		assertCacheSize(t, c, 0)
	}

	{ // Consume without insert
		// 场景四：消费从未插入过的 token，应返回 false 且缓存不变。
		_, ok := c.Consume("fooBAR")
		assert.False(t, ok)
		assertCacheSize(t, c, 0)
	}

	{ // Consume expired
		// 场景五：插入后让时间前进超过 TTL（用 FakeClock 模拟），再消费应失败，
		// 但条目仍会被删除，缓存恢复为空。
		tok, err := c.Insert(nextRequest())
		require.NoError(t, err)
		assertCacheSize(t, c, 1)

		clock.Step(2 * cacheTTL)

		_, ok := c.Consume(tok)
		assert.False(t, ok)
		assertCacheSize(t, c, 0)
	}
}

// TestGC 验证 gc 只清理过期条目，且能正确处理空缓存、部分过期、全部过期等情况。
// 通过 FakeClock 精确控制每条请求的过期时间：tok1/tok2 过期，tok3/tok4 未过期。
func TestGC(t *testing.T) {
	c, clock := newTestCache()

	// When empty
	// 空缓存上执行 gc 应当是安全的空操作。
	c.gc()
	assertCacheSize(t, c, 0)

	// 插入 tok1，10 秒后插入 tok2。
	tok1, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assertCacheSize(t, c, 1)
	clock.Step(10 * time.Second)
	tok2, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assertCacheSize(t, c, 2)

	// expired: tok1, tok2
	// non-expired: tok3, tok4
	// 时间前进 2*TTL，此时 tok1、tok2 均已过期。
	// 下一次 Insert 会先触发 gc，清掉这两条，因此插入后缓存大小应为 1。
	clock.Step(2 * cacheTTL)
	tok3, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assertCacheSize(t, c, 1)
	// 再过 10 秒插入 tok4，此时 tok3 尚未过期，缓存大小为 2。
	clock.Step(10 * time.Second)
	tok4, err := c.Insert(nextRequest())
	require.NoError(t, err)
	assertCacheSize(t, c, 2)

	// 验证：过期的 tok1/tok2 消费失败，未过期的 tok3/tok4 消费成功。
	_, ok := c.Consume(tok1)
	assert.False(t, ok)
	_, ok = c.Consume(tok2)
	assert.False(t, ok)
	_, ok = c.Consume(tok3)
	assert.True(t, ok)
	_, ok = c.Consume(tok4)
	assert.True(t, ok)

	// When full, nothing is expired.
	// 填满缓存且全部未过期时，Insert 触发的 gc 不应清理任何条目，随后应能正常插入。
	for i := 0; i < maxInFlight; i++ {
		_, err := c.Insert(nextRequest())
		require.NoError(t, err)
	}
	assertCacheSize(t, c, maxInFlight)

	// When everything is expired
	// 全部条目过期后，下一次 Insert 应通过 gc 全部清理，然后只留下新插入的这一条。
	clock.Step(2 * cacheTTL)
	_, err = c.Insert(nextRequest())
	require.NoError(t, err)
	assertCacheSize(t, c, 1)
}

// newTestCache 创建一个使用 FakeClock 的 requestCache，返回缓存实例与时钟，
// 便于测试通过 clock.Step 精确控制时间推进，而不必真的等待 TTL 过去。
func newTestCache() (*requestCache, *testingclock.FakeClock) {
	c := newRequestCache()
	fakeClock := testingclock.NewFakeClock(time.Now())
	c.clock = fakeClock
	return c, fakeClock
}

// assertCacheSize 同时校验 token 映射与链表的长度：
// 二者必须相等（缓存内部结构一致），且都等于期望的条目数。
func assertCacheSize(t *testing.T, cache *requestCache, expectedSize int) {
	tokenLen := len(cache.tokens)
	llLen := cache.ll.Len()
	assert.Equal(t, tokenLen, llLen, "inconsistent cache size! len(tokens)=%d; len(ll)=%d", tokenLen, llLen)
	assert.Equal(t, expectedSize, tokenLen, "unexpected cache size!")
}

// requestUID 是为测试请求生成唯一标识的计数器。
var requestUID = 0

// nextRequest 返回一个递增的整数作为测试请求，保证每次插入的请求互不相同。
func nextRequest() interface{} {
	requestUID++
	return requestUID
}
