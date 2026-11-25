/*
Copyright 2024 The Kubernetes Authors.

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

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// TestRequestTimeoutBehavior 验证请求超时在写/刷新响应前后的不同表现。
// 超时过滤器在未写出响应时能返回 504；一旦已写出，就只能标记
// writer 超时并中止连接。用例同时覆盖 HTTP/1.1 与 HTTP/2，因为两者的
// 连接中止语义（EOF vs stream reset）不同，客户端必须都能正确处理。
func TestRequestTimeoutBehavior(t *testing.T) {
	type setup struct {
		name                        string
		clientTimeout               time.Duration
		serverReqTimeout            time.Duration
		handlerWritesBeforeTimeout  bool
		handlerFlushesBeforeTimeout bool
		waiter                      waiter
	}
	type expectation struct {
		clientErr                verifier
		clientStatusCodeExpected int
		clientRespBodyReadErr    verifier
		handlerWriteErr          verifier
	}

	tests := []struct {
		setup        setup
		expectations map[string]expectation
	}{
		// 场景：
		//   a) 是否启用超时过滤器：是
		//   b) 客户端是否在请求 URI 中指定超时：否
		//   c) 请求超时之前，handler 是否已向 ResponseWriter 对象写入：否
		//   d) 请求超时之前，handler 是否已 flush ResponseWriter 对象：否
		// 观察到的行为：
		//  超时过滤器发现请求的 context 已超过其 deadline，由于此时
		//  ResponseWriter 对象尚未被写入，因此会发生以下情况：
		//   - 它把 ResponseWriter 对象标记为 timeout=true，此后任何
		//   向它写入的尝试都会得到 'http: Handler timeout' 错误
		//   - 它向客户端发送 504 状态码
		// 预期（http/1x 与 http/2.0 行为一致）：
		//  客户端：
		//   - 客户端收到 '504 GatewayTimeout' 状态码
		//   - 读取 Response 对象的 Body 会得到一个错误
		//  服务端：
		//   - 向 ResponseWriter 写入会立刻得到 'http: Handler timeout'
		//     错误
		{
			setup: setup{
				name:                        "timeout occurs before the handler writes to or flushes the ResponseWriter",
				clientTimeout:               0,     // b
				handlerWritesBeforeTimeout:  false, // c
				handlerFlushesBeforeTimeout: false, // d
				serverReqTimeout:            time.Second,
			},
			expectations: map[string]expectation{
				"HTTP/2.0": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: http.StatusGatewayTimeout,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				"HTTP/1.1": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: http.StatusGatewayTimeout,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},

		// 场景：
		//   a) 是否启用超时过滤器：是
		//   b) 客户端是否在请求 URI 中指定超时：否
		//   c) 请求超时之前，handler 是否已向 ResponseWriter 对象写入：是
		//   d) 请求超时之前，handler 是否已 flush ResponseWriter 对象：否
		// 观察到的行为：
		//  超时过滤器发现请求的 context 已超过其 deadline，由于此时
		//  ResponseWriter 对象已经被写入过，因此会发生以下情况：
		//   - 它把 ResponseWriter 对象标记为 timeout=true，此后任何向它
		//   写入的尝试都会得到 'http: Handler timeout' 错误
		//   - 由于 ResponseWriter 对象已经被写入过，无法再向客户端发送
		//   '504 GatewayTimeout'，于是它以 'net/http: abort Handler' 错误 panic
		{
			setup: setup{
				name:                        "timeout occurs after the handler writes to the ResponseWriter",
				clientTimeout:               0,     // b
				handlerWritesBeforeTimeout:  true,  // c
				handlerFlushesBeforeTimeout: false, // d
				serverReqTimeout:            time.Second,
			},
			expectations: map[string]expectation{
				// 预期：
				//  - 客户端：收到 stream reset 错误，没有来自服务端的 'Response'
				//  - 服务端：向 ResponseWriter 写入得到 'http: Handler timeout' 错误
				"HTTP/2.0": {
					clientErr:                wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// 预期：
				//  - 客户端：收到 'io.EOF' 错误，没有来自服务端的 'Response'
				//  - 服务端：向 ResponseWriter 写入得到 'http: Handler timeout' 错误
				"HTTP/1.1": {
					clientErr:                wantError{io.EOF},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},

		// 场景：
		//   a) 是否启用超时过滤器：是
		//   b) 客户端是否在请求 URI 中指定超时：否
		//   c) 请求超时之前，handler 是否已向 ResponseWriter 对象写入：是
		//   d) 请求超时之前，handler 是否已 flush ResponseWriter 对象：是
		// 观察到的行为：
		//  超时过滤器发现请求的 context 已超过其 deadline，由于此时
		//  ResponseWriter 对象已经被写入过，因此会发生以下情况：
		//   - 它把 ResponseWriter 对象标记为 timeout=true，此后任何向它
		//   写入的尝试都会得到 'http: Handler timeout' 错误
		//   - 由于 ResponseWriter 对象已经被写入过，无法再向客户端发送
		//   '504 GatewayTimeout'，于是它以 'net/http: abort Handler' 错误 panic
		{
			setup: setup{
				name:                        "timeout occurs after the handler writes to and flushes the ResponseWriter",
				clientTimeout:               0,    // b
				handlerWritesBeforeTimeout:  true, // c
				handlerFlushesBeforeTimeout: true, // d
				serverReqTimeout:            time.Second,
			},
			expectations: map[string]expectation{
				// 预期：
				//  - 客户端：由于 ResponseWriter 已被 flush，客户端会收到
				//  服务端返回的响应，但读取响应体应当得到一个 stream reset 错误。
				//  - 服务端：向 ResponseWriter 写入得到 'http: Handler timeout' 错误
				"HTTP/2.0": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: 200,
					clientRespBodyReadErr:    wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// 预期：
				//  - 客户端：由于 ResponseWriter 已被 flush，客户端会收到
				//  服务端返回的响应，但读取响应体会得到 'unexpected EOF' 错误。
				//  - 服务端：向 ResponseWriter 写入得到 'http: Handler timeout' 错误
				"HTTP/1.1": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: 200,
					clientRespBodyReadErr:    wantError{io.ErrUnexpectedEOF},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},

		// 场景：
		//   a) 是否启用超时过滤器：是
		//   b) 客户端是否在请求 URI 中指定超时：是
		//   c) 请求超时之前，handler 是否已向 ResponseWriter 对象写入：是
		//   d) 请求超时之前，handler 是否已 flush ResponseWriter 对象：否
		// 观察到的行为：
		// 当超时过滤器发现请求 context 已超过其 deadline 时，会发生：
		//   - 它把 ResponseWriter 标记为 timeout=true，之后再向其写入
		//     都会返回 'http: Handler timeout' 错误
		//   - 由于 ResponseWriter 已被写出过，无法再向客户端发送
		//     '504 GatewayTimeout'，因此它以 'net/http: abort Handler' 错误中止
		// 同时，net/http 客户端也会发现自身请求 context 超过了 deadline，
		// 因而以 'context deadline exceeded' 错误中止。
		// 注意：虽然由于往返时间，客户端通常最先收到 context deadline 错误，
		// 但 CI 过载时也可能出现抖动，因此需要两种错误都接受才能稳定不 flake。
		// 两个来源的错误都合法，因此用 wantEitherOr 而非固定断言，
		// 否则 CI 负载波动会造成偶发失败。
		{
			setup: setup{
				name:                        "client specifies a timeout",
				clientTimeout:               time.Second,                 // b
				handlerWritesBeforeTimeout:  true,                        // c
				handlerFlushesBeforeTimeout: false,                       // d
				serverReqTimeout:            wait.ForeverTestTimeout * 2, // 该值不应生效

				// 取请求超时的两倍，以便抵御 CI 上的偶发失败（flaky）
				waiter: &waitWithDuration{after: 2 * time.Second},
			},
			expectations: map[string]expectation{
				// 预期：
				//  - 客户端：要么从自己的 transport 收到 context.DeadlineExceeded 错误，
				//    要么收到服务端返回的错误
				//  - 服务端：向 ResponseWriter 写入会得到 'http: Handler timeout'
				"HTTP/2.0": {
					clientErr: wantEitherOr{
						err:      context.DeadlineExceeded,
						contains: "stream error: stream ID 1; INTERNAL_ERROR; received from peer",
					},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// 预期：
				//  - 客户端：要么从自己的 transport 收到 context.DeadlineExceeded
				//    错误，要么收到服务端返回的错误
				//  - 服务端：向 ResponseWriter 写入会得到 'http: Handler timeout'
				"HTTP/1.1": {
					clientErr: wantEitherOr{
						err:      context.DeadlineExceeded,
						contains: "EOF",
					},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},
	}
	for _, test := range tests {
		for _, proto := range []string{"HTTP/1.1", "HTTP/2.0"} { // 每个用例都会分别用 http/1x 和 http/2.0 跑一遍
			t.Run(fmt.Sprintf("%s/%s", test.setup.name, proto), func(t *testing.T) {
				setup := test.setup
				want, ok := test.expectations[proto]
				if !ok {
					t.Fatalf("wrong test setup - no expectation for %s", proto)
				}

				fakeAudit := &fakeAudit{}
				config, _ := setUp(t)
				config.AuditPolicyRuleEvaluator = fakeAudit
				config.AuditBackend = fakeAudit

				// 配置服务端启动参数 --request-timeout
				config.RequestTimeout = setup.serverReqTimeout

				s, err := config.Complete(nil).New("test", NewEmptyDelegate())
				if err != nil {
					t.Fatalf("Error in setting up a GenericAPIServer object: %v", err)
				}

				// 借助它，handler 会一直阻塞直到超时发生
				waiter := setup.waiter
				if waiter == nil {
					waiter = &waitWithChannelClose{after: make(chan time.Time)}
				}

				// 这是我们期望服务端上请求 context 拥有的超时。
				//  - 若客户端未在请求 URI 中指定超时参数，则默认取 --request-timeout
				//  - 否则取客户端指定的超时
				// 服务端会取二者较小值，这里两个值相等或客户端更小，便于精确断言。
				reqCtxTimeoutWant := config.RequestTimeout
				if setup.clientTimeout > 0 {
					reqCtxTimeoutWant = setup.clientTimeout
				}

				handlerDoneCh := make(chan struct{})
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(handlerDoneCh)

					ctx := r.Context()
					if r.Proto != proto {
						t.Errorf("expected protocol: %q, but got: %q", proto, r.Proto)
						return
					}

					// TODO: 我们尚不支持 `FlushError`
					flusher, ok := w.(http.Flusher)
					if !ok {
						t.Errorf("expected ResponseWriter object to implement FlushError")
						return
					}

					// 确认我们拿到的是正确的请求
					//  - 它必须是一个非长时运行（non long-running）请求
					//  - 它必须带有 received-at 时间戳，这样我们才能
					//    准确计算出请求的 deadline
					//  - 请求的 context 必须具有预期的 deadline
					reqInfo, ok := request.RequestInfoFrom(ctx)
					if !ok {
						t.Errorf("expected the request context to have a RequestInfo associated")
						return
					}
					if config.LongRunningFunc(r, reqInfo) {
						t.Errorf("wrong test setup, wanted a non long-running request, but got: %#v", reqInfo)
						return
					}
					receivedAt, ok := request.ReceivedTimestampFrom(ctx)
					if !ok {
						t.Errorf("expected the request context to have a received-at timestamp")
						return
					}
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Errorf("expected the request context to have a deadline")
						return
					}
					if want, got := reqCtxTimeoutWant, deadline.Sub(receivedAt); want != got {
						t.Errorf("expected the request context to have a deadline of: %s, but got: %s", want, got)
						return
					}

					// 在超时发生之前，handler 是否向 ResponseWriter
					// 对象写入过或 flush 过？
					if setup.handlerWritesBeforeTimeout {
						if _, err := w.Write([]byte("hello")); err != nil {
							t.Errorf("unexpected error from Write: %v", err)
							return
						}
					}
					if setup.handlerFlushesBeforeTimeout {
						flusher.Flush()
					}

					// 等待请求 context 的 deadline 到期
					<-waiter.wait()

					// 在超时发生后写入 ResponseWriter。
					// 此时必须返回 ErrHandlerTimeout，证明 writer 已被标记为超时。
					_, err := w.Write([]byte("a"))
					want.handlerWriteErr.verify(t, err)

					// 在超时发生之后 flush ResponseWriter 对象
					// http.Flusher 不会返回错误
					flusher.Flush()
				})
				s.Handler.NonGoRestfulMux.Handle("/ping", handler)

				server := httptest.NewUnstartedServer(s.Handler)
				defer server.Close()
				if proto == "HTTP/2.0" {
					server.EnableHTTP2 = true
				}
				server.StartTLS()

				func() {
					defer waiter.close()

					client := server.Client()

					url := fmt.Sprintf("%s/ping", server.URL)
					// 如果用户指定了超时，就把它
					// 追加到请求 URI 中
					if setup.clientTimeout > 0 {
						url = fmt.Sprintf("%s?timeout=%s", url, setup.clientTimeout)
					}
					// 如果客户端指定了超时，我们就必须
					// 用相同的 deadline 来串联请求 context，
					// client-go 目前就是这种行为。
					ctx := context.Background()
					if setup.clientTimeout > 0 {
						var cancel context.CancelFunc
						ctx, cancel = context.WithTimeout(ctx, setup.clientTimeout)
						defer cancel()
					}

					req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
					if err != nil {
						t.Errorf("failed to create a new http request - %v", err)
						return
					}

					resp, err := client.Do(req)
					want.clientErr.verify(t, err)

					// 我们是否期望一个有效的 http 状态码？
					switch {
					case want.clientStatusCodeExpected > 0:
						if resp == nil {
							t.Errorf("expected a response from the server: %v", err)
							return
						}
						if resp.StatusCode != want.clientStatusCodeExpected {
							t.Errorf("expected a status code: %d, but got: %#v", want.clientStatusCodeExpected, resp)
						}

						// 读取响应体，并校验我们的预期
						_, err = io.ReadAll(resp.Body)
						want.clientRespBodyReadErr.verify(t, err)

						if err := resp.Body.Close(); err != nil {
							t.Errorf("unexpected error while closing the Body of the Response: %v", err)
						}
					default:
						if resp != nil {
							t.Errorf("did not expect a Response from the server, but got: %#v", resp)
						}
						return
					}
				}()

				select {
				case <-handlerDoneCh:
				case <-time.After(wait.ForeverTestTimeout):
					t.Errorf("expected the request handler to have terminated")
				}
			})
		}
	}
}

type verifier interface {
	verify(t *testing.T, got error)
}

// wantNoError 断言没有错误。
type wantNoError struct{}

func (v wantNoError) verify(t *testing.T, got error) {
	t.Helper()
	if got != nil {
		t.Errorf("unexpected error: %v", got)
	}
}

// wantContains 断言错误文本包含指定子串，用于匹配底层库的措辞。
type wantContains struct {
	contains string
}

func (v wantContains) verify(t *testing.T, got error) {
	t.Helper()

	switch {
	case got != nil:
		if !strings.Contains(got.Error(), v.contains) {
			t.Errorf("expected the error to contain: %q, but got: %v", v.contains, got)
		}
	default:
		t.Errorf("expected an error that contains %q, but got none", v.contains)
	}
}

// wantError 断言错误链中包含指定哨兵错误（errors.Is 语义）。
type wantError struct {
	err error
}

func (v wantError) verify(t *testing.T, got error) {
	t.Helper()

	switch {
	case got != nil:
		if !errors.Is(got, v.err) {
			t.Errorf("expected error: %v, but got: %v", v.err, got)
		}
	default:
		t.Errorf("expected an error %v, but got none", v.err)
	}
}

// wantEitherOr 接受哨兵错误或子串任一匹配，用于容忍竞态导致的两种合法结果。
type wantEitherOr struct {
	err      error
	contains string
}

func (v wantEitherOr) verify(t *testing.T, got error) {
	t.Helper()

	switch {
	case got != nil:
		if !(errors.Is(got, v.err) || strings.Contains(got.Error(), v.contains)) {
			t.Errorf("expected the error to contain: %q or be: %v, but got: %v", v.contains, v.err, got)
		}
	default:
		t.Errorf("expected an error to contain: %q or be: %v, but got none", v.contains, v.err)
	}
}

// waiter 抽象 handler 中等待超时的方式，便于区分"固定时长"与"由测试关闭通道"。
type waiter interface {
	wait() <-chan time.Time
	close()
}

// waitWithDuration 按固定时长等待，用于客户端自身超时的场景。
type waitWithDuration struct {
	after time.Duration
}

func (w waitWithDuration) wait() <-chan time.Time { return time.After(w.after) }
func (w waitWithDuration) close()                 {}

// waitWithChannelClose 让 handler 阻塞到测试主动关闭通道，避免依赖真实时间。
type waitWithChannelClose struct {
	after chan time.Time
}

func (w waitWithChannelClose) wait() <-chan time.Time {
	// 对于 http/2，我们按以下步骤进行：
	// a) 让 handler 无限期阻塞
	// b) 这迫使写超时在服务端发生
	// c) http2 客户端在写超时发生之后会立刻
	//    收到一个 stream reset 错误
	// d) 随后客户端通过调用 close 关闭通道
	// e) handler 解除阻塞并结束
	return w.after
}

func (w waitWithChannelClose) close() { close(w.after) }
