/*
Copyright 2021 The Kubernetes Authors.

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

package filters

import (
	"net/http"
	"strings"
)

var (
	// 健康检查探针与指标抓取从不被拒绝，即使在关闭延迟期过后
	// 仍继续服务这些请求。
	//
	// 探针与指标抓取豁免的原因是：它们通常来自本机或监控系统，
	// 在优雅关闭窗口内仍需要反映真实状态；拒绝它们会让外部误判实例已死。
	pathPrefixesExemptFromRetryAfter = []string{
		"/readyz",
		"/livez",
		"/healthz",
		"/metrics",
	}
)

// isRequestExemptFunc 在请求不应被拒绝时返回 true
// （即以 Retry-After 响应拒绝），否则返回 false。
type isRequestExemptFunc func(*http.Request) bool

// retryAfterParams 决定如何构造 Retry-After 响应
type retryAfterParams struct {
	// TearDownConnection 为 true 时，应在响应中发送 'Connection: close'
	// 头，让 net/http 拆除 TCP 连接。
	//
	// 关闭连接是为了防止客户端复用已有连接继续发送请求，
	// 迫使其通过新建连接（可能命中新实例）重试。
	TearDownConnection bool

	// Message 描述服务器为何发送 Retry-After 响应
	Message string
}

// shouldRespondWithRetryAfterFunc 在满足特定条件时返回 true，
// 表示请求应以 Retry-After 响应拒绝。
// 返回的 retryAfterParams 包含如何构造
// Retry-After 响应的说明。
type shouldRespondWithRetryAfterFunc func() (*retryAfterParams, bool)

// 若指定的 shutdownDelayDurationElapsedFn channel 已关闭，
// WithRetryAfter 用 429 拒绝所有新请求。
//
// 包括新连接或已有连接上的新请求。
// shutdownDelayDurationElapsedFn 关闭后到达的所有新请求
// 都会收到 429 及如下响应头：
//   - 'Retry-After: N'（客户端可在 N 秒后重试，希望落在新的 apiserver 实例上）
//   - 'Connection: close'：拆除 TCP 连接
//
// TODO：能否把 WithWaitGroup 与本过滤器合并？
func WithRetryAfter(handler http.Handler, shutdownDelayDurationElapsedCh <-chan struct{}) http.Handler {
	shutdownRetryAfterParams := &retryAfterParams{
		TearDownConnection: true,
		Message:            "The apiserver is shutting down, please try again later.",
	}

	// 注意：WithRetryAfter 和 WithWaitGroup 必须使用完全相同的 isRequestExemptFunc
	// 'isRequestExemptFromRetryAfter'，否则 SafeWaitGroup 可能无限等待，阻碍服务器优雅退出；
	// 两者判断不一致会导致被 wait group 跟踪的请求被这里提前拒绝，或反之，最终卡住优雅退出。
	return withRetryAfter(handler, isRequestExemptFromRetryAfter, func() (*retryAfterParams, bool) {
		select {
		case <-shutdownDelayDurationElapsedCh:
			return shutdownRetryAfterParams, true
		default:
			return nil, false
		}
	})
}

// withRetryAfter 是带可注入策略的实现，便于测试替换豁免与触发条件。
func withRetryAfter(handler http.Handler, isRequestExemptFn isRequestExemptFunc, shouldRespondWithRetryAfterFn shouldRespondWithRetryAfterFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		params, send := shouldRespondWithRetryAfterFn()
		// 未到关闭窗口或请求豁免时，正常交给下游处理。
		if !send || isRequestExemptFn(req) {
			handler.ServeHTTP(w, req)
			return
		}

		// 走到这里说明该发送 Retry-After 响应了
		//
		// 摘自 net/http2 库
		// HTTP/2 不允许 "Connection" 头（RFC 7540, 8.1.2.2），
		// 但仍尊重 "Connection" == "close"：发送 GOAWAY 并在空闲时
		// 拆除 TCP 连接，与 HTTP/1 的做法一致。
		if params.TearDownConnection {
			w.Header().Set("Connection", "close")
		}

		// 返回 429 状态，要求客户端 5 秒后再试：
		// 固定 5 秒是经验值：足够 kube-proxy/负载均衡摘除该实例，又不至于让客户端等待过久。
		w.Header().Set("Retry-After", "5")
		http.Error(w, params.Message, http.StatusTooManyRequests)
	})
}

// isRequestExemptFromRetryAfter 在给定请求不应被拒绝时返回 true
// （即以 'Retry-After' 响应拒绝）。
// 注意：'WithRetryAfter' 与 'WithWaitGroup' 过滤器都应使用该函数
// 来豁免相应的请求。
func isRequestExemptFromRetryAfter(r *http.Request) bool {
	return isKubeApiserverUserAgent(r) || hasExemptPathPrefix(r)
}

// isKubeApiserverUserAgent 在 user-agent 与本地 loopback 设置的值
// 匹配时返回 true。
// 注意：此时认证过滤器尚未执行，无法从请求 context 中
// 取得已认证的用户信息。
//
// 因此只能依赖 User-Agent 识别内部回环请求；该判断只影响“是否拒绝”，
// 不参与任何鉴权决策，伪造 UA 最多换来一次正常处理。
func isKubeApiserverUserAgent(req *http.Request) bool {
	return strings.HasPrefix(req.UserAgent(), "kube-apiserver/")
}

// hasExemptPathPrefix 判断路径是否属于探针/指标等豁免前缀。
func hasExemptPathPrefix(r *http.Request) bool {
	for _, whiteListedPrefix := range pathPrefixesExemptFromRetryAfter {
		if strings.HasPrefix(r.URL.Path, whiteListedPrefix) {
			return true
		}
	}
	return false
}
