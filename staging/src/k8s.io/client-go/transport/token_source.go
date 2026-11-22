/*
Copyright 2018 The Kubernetes Authors.

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

// Package transport 提供构造 REST 客户端 HTTP 传输层的工具。
//
// 这里的 token source、CA 轮换等实现都遵循“每次请求前按需刷新、
// 失败时退回旧值”的策略，避免在长连接场景下因凭据过期中断请求。

package transport

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/klog/v2"
)

// TokenSourceWrapTransport returns a WrapTransport that injects bearer tokens
// authentication from an oauth2.TokenSource.
func TokenSourceWrapTransport(ts oauth2.TokenSource) func(http.RoundTripper) http.RoundTripper {
	return func(rt http.RoundTripper) http.RoundTripper {
		// oauth2.Transport 负责在请求头写入 Bearer token；base 保留给取消与包装检查。
		return &tokenSourceTransport{
			base: rt,
			ort: &oauth2.Transport{
				Source: ts,
				Base:   rt,
			},
		}
	}
}

// ResettableTokenSource 是支持主动失效旧凭据的 TokenSource。
//
// 当服务器返回 401 时，transport 可据此让缓存中“签发时间较早”的 token 失效，
// 迫使下一次请求重新获取，而不会误伤并发请求中刚换来的新 token。
type ResettableTokenSource interface {
	oauth2.TokenSource
	ResetTokenOlderThan(time.Time)
}

// ResettableTokenSourceWrapTransport returns a WrapTransport that injects bearer tokens
// authentication from an ResettableTokenSource.
func ResettableTokenSourceWrapTransport(ts ResettableTokenSource) func(http.RoundTripper) http.RoundTripper {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &tokenSourceTransport{
			base: rt,
			ort: &oauth2.Transport{
				Source: ts,
				Base:   rt,
			},
			// 保存可重置版本，供 401 处理逻辑使用。
			src: ts,
		}
	}
}

// NewCachedFileTokenSource returns a resettable token source which reads a
// token from a file at a specified path and periodically reloads it.
func NewCachedFileTokenSource(path string) *cachingTokenSource {
	return &cachingTokenSource{
		now:    time.Now,
		leeway: 10 * time.Second,
		base: &fileTokenSource{
			path: path,
			// This period was picked because it is half of the duration between when the kubelet
			// refreshes a projected service account token and when the original token expires.
			// Default token lifetime is 10 minutes, and the kubelet starts refreshing at 80% of lifetime.
			// This should induce re-reading at a frequency that works with the token volume source.
			period: time.Minute,
		},
	}
}

// NewCachedTokenSource returns resettable token source with caching. It reads
// a token from a designed TokenSource if not in cache or expired.
func NewCachedTokenSource(ts oauth2.TokenSource) *cachingTokenSource {
	return &cachingTokenSource{
		now:  time.Now,
		base: ts,
	}
}

// tokenSourceTransport 在请求发出前注入 token，并在 401 时按需刷新缓存。
type tokenSourceTransport struct {
	base http.RoundTripper
	ort  http.RoundTripper
	// src 仅由 ResettableTokenSourceWrapTransport 设置；为 nil 时不处理 401。
	src ResettableTokenSource
}

var _ utilnet.RoundTripperWrapper = &tokenSourceTransport{}

func (tst *tokenSourceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// This is to allow --token to override other bearer token providers.
	// 调用方已显式设置 Authorization 时不再覆盖，保证 --token 的优先级。
	if req.Header.Get("Authorization") != "" {
		return tst.base.RoundTrip(req)
	}
	// record time before RoundTrip to make sure newly acquired Unauthorized
	// token would not be reset. Another request from user is required to reset
	// and proceed.
	//
	// 只让“本次请求开始之前”就已存在的 token 失效；并发请求在此期间拿到的新
	// 凭据不受影响，避免相互踢掉刚刷新成功的 token。
	start := time.Now()
	resp, err := tst.ort.RoundTrip(req)
	if err == nil && resp != nil && resp.StatusCode == 401 && tst.src != nil {
		tst.src.ResetTokenOlderThan(start)
	}
	return resp, err
}

func (tst *tokenSourceTransport) CancelRequest(req *http.Request) {
	// 与实际发出请求的 RoundTripper 保持一致，才能取消到正确的底层连接。
	if req.Header.Get("Authorization") != "" {
		tryCancelRequest(tst.base, req)
		return
	}
	tryCancelRequest(tst.ort, req)
}

func (tst *tokenSourceTransport) WrappedRoundTripper() http.RoundTripper { return tst.base }

// fileTokenSource 每次取值都重新读取文件，用于 projected service account token。
type fileTokenSource struct {
	path string
	// period 是本实现假定的 token 有效期，用于计算 oauth2.Token.Expiry。
	period time.Duration
}

var _ = oauth2.TokenSource(&fileTokenSource{})

func (ts *fileTokenSource) Token() (*oauth2.Token, error) {
	tokb, err := os.ReadFile(ts.path)
	if err != nil {
		return nil, fmt.Errorf("failed to read token file %q: %v", ts.path, err)
	}
	tok := strings.TrimSpace(string(tokb))
	// 空文件通常意味着投影卷尚未完成写入，直接报错比发送空 Bearer 更易诊断。
	if len(tok) == 0 {
		return nil, fmt.Errorf("read empty token from file %q", ts.path)
	}

	return &oauth2.Token{
		AccessToken: tok,
		Expiry:      time.Now().Add(ts.period),
	}, nil
}

// cachingTokenSource 缓存底层 TokenSource 的结果，减少重复取 token 的开销。
type cachingTokenSource struct {
	base   oauth2.TokenSource
	leeway time.Duration

	sync.RWMutex
	tok *oauth2.Token
	// t 记录 token 的写入时间，ResetTokenOlderThan 用它判断是否失效。
	t time.Time

	// for testing
	now func() time.Time
}

func (ts *cachingTokenSource) Token() (*oauth2.Token, error) {
	now := ts.now()
	// fast path
	// 读锁快路径：绝大多数请求命中缓存，避免在热路径上串行化。
	ts.RLock()
	tok := ts.tok
	ts.RUnlock()

	// 提前 leeway 视为过期，给时钟漂移和请求往返留出余量。
	if tok != nil && tok.Expiry.Add(-1*ts.leeway).After(now) {
		return tok, nil
	}

	// slow path
	// 双重检查：可能在等待写锁期间已有其他请求刷新了缓存。
	ts.Lock()
	defer ts.Unlock()
	if tok := ts.tok; tok != nil && tok.Expiry.Add(-1*ts.leeway).After(now) {
		return tok, nil
	}

	tok, err := ts.base.Token()
	if err != nil {
		if ts.tok == nil {
			return nil, err
		}
		// Not using a caller-provided logger isn't ideal, but impossible to fix
		// without new APIs that go up all the way to HTTPWrappersForConfig.
		// This is currently deemed not worth changing (too much effort, not enough benefit).
		//
		// 刷新失败但仍有旧 token 时继续使用旧值：对短期凭据而言，
		// 过期的 token 往往仍被服务器接受，比直接让请求失败更可用。
		klog.TODO().Error(err, "Unable to rotate token")
		return ts.tok, nil
	}

	ts.t = ts.now()
	ts.tok = tok
	return tok, nil
}

func (ts *cachingTokenSource) ResetTokenOlderThan(t time.Time) {
	ts.Lock()
	defer ts.Unlock()
	// 只清除在此时间点之前已写入的 token，保护并发刷新得到的新 token。
	if ts.t.Before(t) {
		ts.tok = nil
		ts.t = time.Time{}
	}
}
