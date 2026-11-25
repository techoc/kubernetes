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
		// scenario:
		//   a) timeout filter enabled: Yes
		//   b) client specifies timeout in the request URI: No
		//   c) the handler writes to the ResponseWriter object before request times out: No
		//   d) the handler flushes the ResponseWriter object before request times out: No
		// observation:
		//  the timeout filter detects that the context of the request has exceeded its
		//  deadline, since the ResponseWriter object has not been written to yet,
		//  the following takes place:
		//   - it marks the ResponseWriter object as timeout=true, so any further
		//   attempt to write to it will yield an 'http: Handler timeout' error
		//   - it sends 504 status code to the client
		// expectation (same behavior for both http/1x and http/2.0):
		//  client:
		//   - client receives a '504 GatewayTimeout' status code
		//   - reading the Body of the Response object yields an error
		//  server:
		//   - Write to the ResponseWriter yields an 'http: Handler timeout'
		//     error immediately.
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

		// scenario:
		//   a) timeout filter enabled: Yes
		//   b) client specifies timeout in the request URI: No
		//   c) the handler writes to the ResponseWriter object before request times out: Yes
		//   d) the handler flushes the ResponseWriter object before request times out: No
		// observation:
		//  the timeout filter detects that the context of the request has exceeded its
		//  deadline, since the ResponseWriter object has already been written to,
		//  the following takes place:
		//   - it marks the ResponseWriter object as timeout=true, so any further attempt
		//   to write to it will yield an 'http: Handler timeout' error
		//   - it can't send '504 GatewayTimeout' to the client since the ResponseWriter
		//   object has already been written to, so it panics with 'net/http: abort Handler' error
		{
			setup: setup{
				name:                        "timeout occurs after the handler writes to the ResponseWriter",
				clientTimeout:               0,     // b
				handlerWritesBeforeTimeout:  true,  // c
				handlerFlushesBeforeTimeout: false, // d
				serverReqTimeout:            time.Second,
			},
			expectations: map[string]expectation{
				// expectation:
				//  - client: receives a stream reset error, no 'Response' from the server
				//  - server: Write to the ResponseWriter yields an 'http: Handler timeout' error
				"HTTP/2.0": {
					clientErr:                wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// expectation:
				//  - client: receives an 'io.EOF' error, no 'Response' from the server
				//  - server: Write to the ResponseWriter yields an 'http: Handler timeout' error
				"HTTP/1.1": {
					clientErr:                wantError{io.EOF},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},

		// scenario:
		//   a) timeout filter enabled: Yes
		//   b) client specifies timeout in the request URI: No
		//   c) the handler writes to the ResponseWriter object before request times out: Yes
		//   d) the handler flushes the ResponseWriter object before request times out: Yes
		// observation:
		//  the timeout filter detects that the context of the request has exceeded its
		//  deadline, since the ResponseWriter object has already been written to,
		//  the following takes place:
		//   - it marks the ResponseWriter object as timeout=true, so any further attempt
		//   to write to it will yield an 'http: Handler timeout' error
		//   - it can't send '504 GatewayTimeout' to the client since the ResponseWriter
		//   object has already been written to, so it panics with 'net/http: abort Handler' error
		{
			setup: setup{
				name:                        "timeout occurs after the handler writes to and flushes the ResponseWriter",
				clientTimeout:               0,    // b
				handlerWritesBeforeTimeout:  true, // c
				handlerFlushesBeforeTimeout: true, // d
				serverReqTimeout:            time.Second,
			},
			expectations: map[string]expectation{
				// expectation:
				//  - client: since the ResponseWriter has been flushed the client
				//  receives a response from the server, but reading the response body
				//  is expected to yield a stream reset error.
				//  - server: Write to the ResponseWriter yields an 'http: Handler timeout' error
				"HTTP/2.0": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: 200,
					clientRespBodyReadErr:    wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// expectation:
				//  - client: since the ResponseWriter has been flushed the client
				//  receives a response from the server, but reading the response body
				//  will yield an 'unexpected EOF' error.
				//  - server: Write to the ResponseWriter yields an 'http: Handler timeout' error
				"HTTP/1.1": {
					clientErr:                wantNoError{},
					clientStatusCodeExpected: 200,
					clientRespBodyReadErr:    wantError{io.ErrUnexpectedEOF},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
			},
		},

		// scenario:
		//   a) timeout filter enabled: Yes
		//   b) client specifies timeout in the request URI: Yes
		//   c) the handler writes to the ResponseWriter object before request times out: Yes
		//   d) the handler flushes the ResponseWriter object before request times out: No
		// observation:
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
				serverReqTimeout:            wait.ForeverTestTimeout * 2, // this should not be in effect

				// twice the request timeout so it can withstand flakes in CI
				waiter: &waitWithDuration{after: 2 * time.Second},
			},
			expectations: map[string]expectation{
				// expectation:
				//  - client: receives either a context.DeadlineExceeded error from its transport
				//    or it receives the error from the server
				//  - server: Write to the ResponseWriter will yields an 'http: Handler timeout'
				"HTTP/2.0": {
					clientErr: wantEitherOr{
						err:      context.DeadlineExceeded,
						contains: "stream error: stream ID 1; INTERNAL_ERROR; received from peer",
					},
					clientStatusCodeExpected: 0,
					clientRespBodyReadErr:    wantNoError{},
					handlerWriteErr:          wantError{http.ErrHandlerTimeout},
				},
				// expectation:
				//  - client: receives either a context.DeadlineExceeded error from its
				//    transport, or it receives the error from the server
				//  - server: Write to the ResponseWriter will yields an 'http: Handler timeout'
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
		for _, proto := range []string{"HTTP/1.1", "HTTP/2.0"} { // every test is run for both http/1x and http/2.0
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

				// setup server run option --request-timeout
				config.RequestTimeout = setup.serverReqTimeout

				s, err := config.Complete(nil).New("test", NewEmptyDelegate())
				if err != nil {
					t.Fatalf("Error in setting up a GenericAPIServer object: %v", err)
				}

				// using this, the handler blocks until the timeout occurs
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

					// TODO: we don't support `FlushError` yet
					flusher, ok := w.(http.Flusher)
					if !ok {
						t.Errorf("expected ResponseWriter object to implement FlushError")
						return
					}

					// make sure that we have the right request
					//  - it must be a non long-running request
					//  - it must have a received-at timestamp so we
					//    can calculate the request deadline accurately.
					//  - the context of the request must have the
					//    expected deadline
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

					// does the handler write to or flush the
					// ResponseWriter object before timeout occurs?
					if setup.handlerWritesBeforeTimeout {
						if _, err := w.Write([]byte("hello")); err != nil {
							t.Errorf("unexpected error from Write: %v", err)
							return
						}
					}
					if setup.handlerFlushesBeforeTimeout {
						flusher.Flush()
					}

					// wait for the request context deadline to elapse
					<-waiter.wait()

					// 在超时发生后写入 ResponseWriter。
					// 此时必须返回 ErrHandlerTimeout，证明 writer 已被标记为超时。
					_, err := w.Write([]byte("a"))
					want.handlerWriteErr.verify(t, err)

					// flush the ResponseWriter object after timeout happens
					// http.Flusher does not return an error
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
					// if the user has specified a timeout then add
					// it to the request URI
					if setup.clientTimeout > 0 {
						url = fmt.Sprintf("%s?timeout=%s", url, setup.clientTimeout)
					}
					// if the client has specified a timeout then we
					// must wire the request context with the same
					// deadline, this is how client-go behaves today.
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

					// do we expect a valid http status code?
					switch {
					case want.clientStatusCodeExpected > 0:
						if resp == nil {
							t.Errorf("expected a response from the server: %v", err)
							return
						}
						if resp.StatusCode != want.clientStatusCodeExpected {
							t.Errorf("expected a status code: %d, but got: %#v", want.clientStatusCodeExpected, resp)
						}

						// read off the body of the response, and verify what we expect
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
	// for http/2, we do the following:
	// a) let the handler block indefinitely
	// b) this forces the write timeout to occur on the server side
	// c) the http2 client receives a stream reset error immediately
	//    after the write timeout occurs.
	// d) the client then closes the channel by calling close
	// e) the handler unblocks and terminates
	return w.after
}

func (w waitWithChannelClose) close() { close(w.after) }
