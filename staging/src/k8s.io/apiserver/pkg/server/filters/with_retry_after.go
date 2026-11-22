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
	// health probes and metrics scraping are never rejected, we will continue
	// serving these requests after shutdown delay duration elapses.
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

// isRequestExemptFunc returns true if the request should not be rejected,
// with a Retry-After response, otherwise it returns false.
type isRequestExemptFunc func(*http.Request) bool

// retryAfterParams dictates how the Retry-After response is constructed
type retryAfterParams struct {
	// TearDownConnection is true when we should send a 'Connection: close'
	// header in the response so net/http can tear down the TCP connection.
	//
	// 关闭连接是为了防止客户端复用已有连接继续发送请求，
	// 迫使其通过新建连接（可能命中新实例）重试。
	TearDownConnection bool

	// Message describes why Retry-After response has been sent by the server
	Message string
}

// shouldRespondWithRetryAfterFunc returns true if the requests should
// be rejected with a Retry-After response once certain conditions are met.
// The retryAfterParams returned contains instructions on how to
// construct the Retry-After response.
type shouldRespondWithRetryAfterFunc func() (*retryAfterParams, bool)

// WithRetryAfter rejects any incoming new request(s) with a 429
// if the specified shutdownDelayDurationElapsedFn channel is closed
//
// It includes new request(s) on a new or an existing TCP connection
// Any new request(s) arriving after shutdownDelayDurationElapsedFn is closed
// are replied with a 429 and the following response headers:
//   - 'Retry-After: N` (so client can retry after N seconds, hopefully on a new apiserver instance)
//   - 'Connection: close': tear down the TCP connection
//
// TODO: is there a way to merge WithWaitGroup and this filter?
func WithRetryAfter(handler http.Handler, shutdownDelayDurationElapsedCh <-chan struct{}) http.Handler {
	shutdownRetryAfterParams := &retryAfterParams{
		TearDownConnection: true,
		Message:            "The apiserver is shutting down, please try again later.",
	}

	// NOTE: both WithRetryAfter and WithWaitGroup must use the same exact isRequestExemptFunc 'isRequestExemptFromRetryAfter,
	// otherwise SafeWaitGroup might wait indefinitely and will prevent the server from shutting down gracefully.
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

		// If we are here this means it's time to send Retry-After response
		//
		// Copied from net/http2 library
		// "Connection" headers aren't allowed in HTTP/2 (RFC 7540, 8.1.2.2),
		// but respect "Connection" == "close" to mean sending a GOAWAY and tearing
		// down the TCP connection when idle, like we do for HTTP/1.
		if params.TearDownConnection {
			w.Header().Set("Connection", "close")
		}

		// Return a 429 status asking the client to try again after 5 seconds
		// 固定 5 秒是经验值：足够 kube-proxy/负载均衡摘除该实例，又不至于让客户端等待过久。
		w.Header().Set("Retry-After", "5")
		http.Error(w, params.Message, http.StatusTooManyRequests)
	})
}

// isRequestExemptFromRetryAfter returns true if the given request should be exempt
// from being rejected with a 'Retry-After' response.
// NOTE: both 'WithRetryAfter' and 'WithWaitGroup' filters should use this function
// to exempt the set of requests from being rejected or tracked.
func isRequestExemptFromRetryAfter(r *http.Request) bool {
	return isKubeApiserverUserAgent(r) || hasExemptPathPrefix(r)
}

// isKubeApiserverUserAgent returns true if the user-agent matches
// the one set by the local loopback.
// NOTE: we can't look up the authenticated user informaion from the
// request context since the authentication filter has not executed yet.
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
