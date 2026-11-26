// 本包复制自 github.com/gregjones/httpcache，改动如下：
// - ioutil.NopCloser 替换为 io.NopCloser（已弃用）
// - ioutil.ReadAll 替换为 io.ReadAll（已弃用）

// Package httpcache 提供一个 http.RoundTripper 实现，用作 HTTP 响应缓存。
// 该缓存基本符合 RFC，且只适合作为「私有」缓存
// （即用于 web 浏览器或 API 客户端，而非共享代理）。
//
// 该实现仅供单个客户端本地使用，因此忽略 private/public 与 s-maxage 等共享缓存语义；
// client-go 用它缓存 discovery 文档，以 ETag 协商减少重复传输。
package httpcache

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"time"
)

const (
	stale = iota
	fresh
	transparent
	// XFromCache 是缓存命中响应上附加的头。
	XFromCache = "X-From-Cache"
)

// Cache 接口被 Transport 用于存取响应。实现必须并发安全，
// 因为 Transport 可能被多个请求同时使用。
type Cache interface {
	// Get 返回缓存响应的字节表示，值非空时第二个返回值为 true。
	Get(key string) (responseBytes []byte, ok bool)
	// Set 按 key 存储响应的字节表示。
	Set(key string, responseBytes []byte)
	// Delete 删除 key 对应的值。
	Delete(key string)
}

// cacheKey 返回 req 的缓存键：
// GET 直接用 URL 作 key，其他方法加上 method 前缀，避免对同一 URL 的不同写操作互相污染。
func cacheKey(req *http.Request) string {
	if req.Method == http.MethodGet {
		return req.URL.String()
	} else {
		return req.Method + " " + req.URL.String()
	}
}

// CachedResponse 返回 req 对应的缓存 http.Response，不存在则返回 nil。
//
// 返回的 Response.Body 由内存字节重建，调用方仍需按常规关闭它。
func CachedResponse(c Cache, req *http.Request) (resp *http.Response, err error) {
	cachedVal, ok := c.Get(cacheKey(req))
	if !ok {
		return
	}

	b := bytes.NewBuffer(cachedVal)
	return http.ReadResponse(bufio.NewReader(b), req)
}

// MemoryCache 是 Cache 的实现，把响应存放在内存 map 中。
//
// 无容量上限，仅适合缓存体量可控的元数据（如 discovery 文档）；大响应请用磁盘后端。
type MemoryCache struct {
	mu    sync.RWMutex
	items map[string][]byte
}

// Get 返回响应的 []byte 表示，存在时返回 true，否则返回 false。
//
// 读锁只在查表期间持有，返回的切片不与缓存内部状态共享写路径。
func (c *MemoryCache) Get(key string) (resp []byte, ok bool) {
	c.mu.RLock()
	resp, ok = c.items[key]
	c.mu.RUnlock()
	return resp, ok
}

// Set 按 key 把响应存入缓存。
func (c *MemoryCache) Set(key string, resp []byte) {
	c.mu.Lock()
	c.items[key] = resp
	c.mu.Unlock()
}

// Delete 从缓存中移除 key。
func (c *MemoryCache) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// NewMemoryCache 返回一个用内存 map 存储条目的 Cache。
func NewMemoryCache() *MemoryCache {
	c := &MemoryCache{items: map[string][]byte{}}
	return c
}

// Transport 实现 http.RoundTripper：尽量从缓存返回响应以避免网络请求，并在重复请求上附加
// etag/if-modified-since 等校验头，让服务端可以返回 304 Not Modified。
type Transport struct {
	// 实际发起请求的 RoundTripper；为 nil 时使用 http.DefaultTransport。
	Transport http.RoundTripper
	Cache     Cache
	// 为 true 时，来自缓存的响应会额外带上 X-From-Cache 头。
	// 该头便于调用方与测试区分命中缓存的响应。
	MarkCachedResponses bool
}

// NewTransport 用给定 Cache 创建 Transport，并把 MarkCachedResponses 设为 true。
func NewTransport(c Cache) *Transport {
	return &Transport{Cache: c, MarkCachedResponses: true}
}

// Client 返回带响应缓存的 *http.Client。
func (t *Transport) Client() *http.Client {
	return &http.Client{Transport: t}
}

// varyMatches：除非 Vary 所列请求头的缓存值与新请求全部匹配，否则返回 false。
//
// 命中缓存前必须校验 Vary：缓存时把请求头值存进 X-Varied-* 伪头，比较时再还原。
func varyMatches(cachedResp *http.Response, req *http.Request) bool {
	for _, header := range headerAllCommaSepValues(cachedResp.Header, "vary") {
		header = http.CanonicalHeaderKey(header)
		if header != "" && req.Header.Get(header) != cachedResp.Header.Get("X-Varied-"+header) {
			return false
		}
	}
	return true
}

// RoundTrip 接收 Request 返回 Response：缓存中有新鲜响应时直接返回，不连接服务端；
// 若只有陈旧响应，则把其校验头设到新请求上，让服务端有机会返回 NotModified，
// 此时再返回缓存响应。
func (t *Transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	cacheKey := cacheKey(req)
	// Range 请求要的是部分内容，与缓存的完整响应语义不同，因此不缓存。
	cacheable := (req.Method == "GET" || req.Method == "HEAD") && req.Header.Get("range") == ""
	var cachedResp *http.Response
	if cacheable {
		cachedResp, err = CachedResponse(t.Cache, req)
	} else {
		// 非安全方法意味着资源可能被修改，先使旧缓存失效。
		t.Cache.Delete(cacheKey)
	}

	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	if cacheable && cachedResp != nil && err == nil {
		if t.MarkCachedResponses {
			cachedResp.Header.Set(XFromCache, "1")
		}

		if varyMatches(cachedResp, req) {
			// 只有新请求与缓存时的 Vary 头一致才能使用缓存值。
			freshness := getFreshness(cachedResp.Header, req.Header)
			if freshness == fresh {
				return cachedResp, nil
			}

			if freshness == stale {
				// 过期但可校验：带上条件请求头，让服务端有机会返回 304。
				var req2 *http.Request
				// 调用方未设置校验头时，补上缓存响应的校验头。
				etag := cachedResp.Header.Get("etag")
				if etag != "" && req.Header.Get("etag") == "" {
					req2 = cloneRequest(req)
					req2.Header.Set("if-none-match", etag)
				}
				lastModified := cachedResp.Header.Get("last-modified")
				if lastModified != "" && req.Header.Get("last-modified") == "" {
					if req2 == nil {
						req2 = cloneRequest(req)
					}
					req2.Header.Set("if-modified-since", lastModified)
				}
				if req2 != nil {
					req = req2
				}
			}
		}

		resp, err = transport.RoundTrip(req)
		if err == nil && req.Method == "GET" && resp.StatusCode == http.StatusNotModified {
			// 用缓存中的响应替换 304，但用少量新头更新它：
			// 304 只携带少数字段，用这些端到端头刷新缓存副本，其余信息仍以原响应为准。
			endToEndHeaders := getEndToEndHeaders(resp.Header)
			for _, header := range endToEndHeaders {
				cachedResp.Header[header] = resp.Header[header]
			}
			resp = cachedResp
		} else if (err != nil || (cachedResp != nil && resp.StatusCode >= 500)) &&
			req.Method == "GET" && canStaleOnError(cachedResp.Header, req.Header) {
			// 传输故障且启用 stale-if-error 时，若有缓存内容则直接返回缓存；
			// 仅在服务端/传输故障时使用陈旧缓存；4xx 等客户端错误不适用。
			return cachedResp, nil
		} else {
			// 失败或非 200 的响应不可信，清掉旧缓存避免下一次继续命中错误内容。
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Cache.Delete(cacheKey)
			}
			if err != nil {
				return nil, err
			}
		}
	} else {
		reqCacheControl := parseCacheControl(req.Header)
		if _, ok := reqCacheControl["only-if-cached"]; ok {
			// only-if-cached 要求绝不回源；没有可用缓存时按 RFC 返回 504 而不是真实请求。
			resp = newGatewayTimeoutResponse(req)
		} else {
			resp, err = transport.RoundTrip(req)
			if err != nil {
				return nil, err
			}
		}
	}

	if cacheable && canStore(parseCacheControl(req.Header), parseCacheControl(resp.Header)) {
		// 记录 Vary 涉及的请求头取值，供后续命中时比对。
		for _, varyKey := range headerAllCommaSepValues(resp.Header, "vary") {
			varyKey = http.CanonicalHeaderKey(varyKey)
			fakeHeader := "X-Varied-" + varyKey
			reqValue := req.Header.Get(varyKey)
			if reqValue != "" {
				resp.Header.Set(fakeHeader, reqValue)
			}
		}
		switch req.Method {
		case "GET":
			// 推迟写缓存，直到读到 EOF：
			// 未读完的响应可能不完整（连接中断），只有读到 EOF 才写入缓存。
			resp.Body = &cachingReadCloser{
				R: resp.Body,
				OnEOF: func(r io.Reader) {
					resp := *resp
					resp.Body = io.NopCloser(r)
					respBytes, err := httputil.DumpResponse(&resp, true)
					if err == nil {
						t.Cache.Set(cacheKey, respBytes)
					}
				},
			}
		default:
			respBytes, err := httputil.DumpResponse(resp, true)
			if err == nil {
				t.Cache.Set(cacheKey, respBytes)
			}
		}
	} else {
		t.Cache.Delete(cacheKey)
	}
	return resp, nil
}

// ErrNoDateHeader 表示 HTTP 头中缺少 Date 头。
var ErrNoDateHeader = errors.New("no Date header")

// Date 解析并返回 Date 头的值。
func Date(respHeaders http.Header) (date time.Time, err error) {
	dateHeader := respHeaders.Get("date")
	if dateHeader == "" {
		err = ErrNoDateHeader
		return
	}

	return time.Parse(time.RFC1123, dateHeader)
}

type realClock struct{}

func (c *realClock) since(d time.Time) time.Duration {
	return time.Since(d)
}

type timer interface {
	since(d time.Time) time.Duration
}

var clock timer = &realClock{}

// getFreshness 依据请求与响应的 cache-control 值，返回 fresh/stale/transparent 之一。
//
// fresh 表示响应可以直接返回；stale 表示响应需要先验证再返回；
// transparent 表示不应使用该响应来满足本次请求。
//
// 由于这只是私有缓存，cache-control 中的 'public' 与 'private' 并不重要；s-maxage 同样不使用。
//
// 计算顺序遵循 RFC 7234：请求侧 no-cache 优先，其次是响应侧 no-cache，
// 再根据 max-age/Expires 与请求侧的 max-age/min-fresh/max-stale 调整有效寿命。
func getFreshness(respHeaders, reqHeaders http.Header) (freshness int) {
	respCacheControl := parseCacheControl(respHeaders)
	reqCacheControl := parseCacheControl(reqHeaders)
	if _, ok := reqCacheControl["no-cache"]; ok {
		return transparent
	}
	if _, ok := respCacheControl["no-cache"]; ok {
		return stale
	}
	if _, ok := reqCacheControl["only-if-cached"]; ok {
		return fresh
	}

	date, err := Date(respHeaders)
	if err != nil {
		return stale
	}
	currentAge := clock.since(date)

	var lifetime time.Duration
	var zeroDuration time.Duration

	// 响应同时含 Expires 与 max-age 时，max-age 优先，即使 Expires 更严格。
	if maxAge, ok := respCacheControl["max-age"]; ok {
		lifetime, err = time.ParseDuration(maxAge + "s")
		if err != nil {
			lifetime = zeroDuration
		}
	} else {
		expiresHeader := respHeaders.Get("Expires")
		if expiresHeader != "" {
			expires, err := time.Parse(time.RFC1123, expiresHeader)
			if err != nil {
				lifetime = zeroDuration
			} else {
				lifetime = expires.Sub(date)
			}
		}
	}

	if maxAge, ok := reqCacheControl["max-age"]; ok {
		// 表示客户端愿意接受年龄不超过指定秒数的响应。
		lifetime, err = time.ParseDuration(maxAge + "s")
		if err != nil {
			lifetime = zeroDuration
		}
	}
	if minfresh, ok := reqCacheControl["min-fresh"]; ok {
		// 表示客户端要求响应至少在指定秒数内保持新鲜。
		minfreshDuration, err := time.ParseDuration(minfresh + "s")
		if err == nil {
			currentAge = time.Duration(currentAge + minfreshDuration)
		}
	}

	if maxstale, ok := reqCacheControl["max-stale"]; ok {
		// max-stale 表示客户端愿意接受已过期的响应：有值时接受过期不超过指定秒数的响应，
		// 无值则接受任意陈旧度的响应。按 RFC 这类响应应带 Warning 头，但实现起来麻烦且价值存疑；
		// 若确实需要，应另设返回值来区分。
		if maxstale == "" {
			return fresh
		}
		maxstaleDuration, err := time.ParseDuration(maxstale + "s")
		if err == nil {
			currentAge = time.Duration(currentAge - maxstaleDuration)
		}
	}

	if lifetime > currentAge {
		return fresh
	}

	return stale
}

// 请求或响应任一方的 cache-control 含 stale-if-error 扩展时返回 true：https://tools.ietf.org/html/rfc5861
func canStaleOnError(respHeaders, reqHeaders http.Header) bool {
	respCacheControl := parseCacheControl(respHeaders)
	reqCacheControl := parseCacheControl(reqHeaders)

	var err error
	lifetime := time.Duration(-1)

	if staleMaxAge, ok := respCacheControl["stale-if-error"]; ok {
		if staleMaxAge != "" {
			lifetime, err = time.ParseDuration(staleMaxAge + "s")
			if err != nil {
				return false
			}
		} else {
			return true
		}
	}
	if staleMaxAge, ok := reqCacheControl["stale-if-error"]; ok {
		if staleMaxAge != "" {
			lifetime, err = time.ParseDuration(staleMaxAge + "s")
			if err != nil {
				return false
			}
		} else {
			return true
		}
	}

	if lifetime >= 0 {
		date, err := Date(respHeaders)
		if err != nil {
			return false
		}
		currentAge := clock.since(date)
		if lifetime > currentAge {
			return true
		}
	}

	return false
}

// 304 合并时只采用端到端头；逐跳头（Connection、Transfer-Encoding 等）属于当前连接，
// 不能写回缓存副本。
func getEndToEndHeaders(respHeaders http.Header) []string {
	// 这些头始终是逐跳头。
	hopByHopHeaders := map[string]struct{}{
		"Connection":          {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {},
		"Trailers":            {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	}

	for _, extra := range strings.Split(respHeaders.Get("connection"), ",") {
		// Connection 头中列出的头同样视为逐跳头。
		if strings.Trim(extra, " ") != "" {
			hopByHopHeaders[http.CanonicalHeaderKey(extra)] = struct{}{}
		}
	}
	endToEndHeaders := []string{}
	for respHeader := range respHeaders {
		if _, ok := hopByHopHeaders[respHeader]; !ok {
			endToEndHeaders = append(endToEndHeaders, respHeader)
		}
	}
	return endToEndHeaders
}

// canStore 只要请求或响应任一方声明 no-store 就不缓存，符合最小惊讶原则。
func canStore(reqCacheControl, respCacheControl cacheControl) (canStore bool) {
	if _, ok := respCacheControl["no-store"]; ok {
		return false
	}
	if _, ok := reqCacheControl["no-store"]; ok {
		return false
	}
	return true
}

func newGatewayTimeoutResponse(req *http.Request) *http.Response {
	var braw bytes.Buffer
	braw.WriteString("HTTP/1.1 504 Gateway Timeout\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(&braw), req)
	if err != nil {
		panic(err)
	}
	return resp
}

// cloneRequest 克隆给定的 *http.Request：结构体与 Header map 都只做浅拷贝。
// （本函数版权归 goauth2 作者：https://code.google.com/p/goauth2）
func cloneRequest(r *http.Request) *http.Request {
	// 结构体浅拷贝。
	r2 := new(http.Request)
	*r2 = *r
	// Header 深拷贝。
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	return r2
}

type cacheControl map[string]string

// parseCacheControl 把逗号分隔的指令解析为 map；无值的指令（如 no-cache）映射为空串，
// 其存在性即可表达语义。
func parseCacheControl(headers http.Header) cacheControl {
	cc := cacheControl{}
	ccHeader := headers.Get("Cache-Control")
	for _, part := range strings.Split(ccHeader, ",") {
		part = strings.Trim(part, " ")
		if part == "" {
			continue
		}
		if strings.ContainsRune(part, '=') {
			keyval := strings.Split(part, "=")
			cc[strings.Trim(keyval[0], " ")] = strings.Trim(keyval[1], ",")
		} else {
			cc[part] = ""
		}
	}
	return cc
}

// headerAllCommaSepValues 返回指定头的全部逗号分隔值（已去空白）。按 HTTP/1.1 规范
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2 第 4.2 节，同一头多次出现时，
// 若其值为逗号分隔列表，应拼接起来。
func headerAllCommaSepValues(headers http.Header, name string) []string {
	var vals []string
	for _, val := range headers[http.CanonicalHeaderKey(name)] {
		fields := strings.Split(val, ",")
		for i, f := range fields {
			fields[i] = strings.TrimSpace(f)
		}
		vals = append(vals, fields...)
	}
	return vals
}

// cachingReadCloser 包装 ReadCloser R：读到 EOF 时，用从 R 读出的内容的完整副本
// 调用 OnEOF 回调。
//
// 保留完整副本是为了在响应被消费完后仍能序列化入缓存；代价是响应体会在内存中多留一份。
type cachingReadCloser struct {
	// 底层的 ReadCloser。
	R io.ReadCloser
	// 读到 EOF 时以 R 内容的副本调用 OnEOF。
	OnEOF func(io.Reader)

	buf bytes.Buffer // buf stores a copy of the content of R.
}

// Read 从 R 读取最多 len(p) 字节，返回读取字节数；数据读尽时返回 io.EOF，并以已读内容的完整副本调用 OnEOF。
func (r *cachingReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.R.Read(p)
	r.buf.Write(p[:n])
	if err == io.EOF {
		// EOF 只出现一次，因此这里保证 OnEOF 恰好执行一次。
		r.OnEOF(bytes.NewReader(r.buf.Bytes()))
	}
	return n, err
}

func (r *cachingReadCloser) Close() error {
	return r.R.Close()
}

// NewMemoryCacheTransport 返回一个使用内存缓存实现的 Transport。
func NewMemoryCacheTransport() *Transport {
	c := NewMemoryCache()
	t := NewTransport(c)
	return t
}
