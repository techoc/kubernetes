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

package filters

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// 注意：以下测试断言的是 net/http 返回的底层错误。如果某个 Go 版本
// 改变了返回的错误类型，这些测试在升级 Go 时可能会失败；我们可以通过
// 把测试设置中期望的错误改为新观察到的错误来修复它们。
// 本文件用真实 net/http 服务器刻画 per-request 读/写 deadline 的底层行为，
// 作为 filters 中 SetReadDeadline/SetWriteDeadline 实现的“事实基线”。断言依赖
// 标准库错误文本，升级 Go 时可能需要同步调整。

// TestPerRequestWithWriteEventuallyReturnsDeadlineError 验证：deadline 过后持续写入，
// 直到内核/缓冲写满时 Write 才返回超时错误；期间不 flush 时客户端拿不到内容。
func TestPerRequestWithWriteEventuallyReturnsDeadlineError(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-request 写 deadline 的行为。
	//
	// 场景：我们想观察 ResponseWriter 对象的 Write 方法在什么情况下返回
	// "i/o timeout" 错误：
	//  - a) write deadline 设置为 100ms
	//  - b) handler 等待直到 write deadline 超时
	//  - c) 超时发生后，handler 每次持续写入 1kB 数据
	// 预期行为如下：
	//  - d) 客户端：预期从服务器收到一个错误
	//  - e) 服务器：ResponseWriter 对象的 Write 方法在其内部缓冲区
	//       写满后应返回 "i/o timeout" 错误
	//
	// 内部缓冲区大小为 4kB 字节：
	//
	// http/1x：net.Conn 由默认大小 4kB 字节的带缓冲 Writer
	// （*bufio.Writer）写入：
	//  a) https://github.com/golang/go/blob/ffb3e574012ce9d3d5193d7b8df135189b8a6671/src/net/http/server.go#L1650
	//  b) https://github.com/golang/go/blob/ffb3e574012ce9d3d5193d7b8df135189b8a6671/src/net/http/server.go#L2014
	//
	// http/2.0：带缓冲的 writer（4kB 字节）写入底层 net.Conn
	//  a) https://github.com/golang/go/blob/ffb3e574012ce9d3d5193d7b8df135189b8a6671/src/net/http/h2_bundle.go#L3568
	//  b) https://github.com/golang/go/blob/ffb3e574012ce9d3d5193d7b8df135189b8a6671/src/net/http/h2_bundle.go#L4279
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		clientErrVerifier  verifier
		handlerErrVerifier verifier
	}{
		{
			protoMajor: 1, // http/1x
			// 注意：http/1x 不能使用基于 channel 的 waiter，
			// 因为请求 handler 与连接服务 goroutine 在同一个
			// goroutine 中执行
			// https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/4460-per-request-deadline#client-hanging-indefinitely
			// write timeout 设置为 100ms，等待 5s 应当足以
			// 抵御 CI 中的偶发失败。
			waiter:             &waitWithDuration{after: 5 * time.Second},
			clientErrVerifier:  wantContains{contains: "local error: tls: bad record MAC"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2, // http/2.0
			waiter:             &waitForClose{after: make(chan time.Time)},
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}

				// a) 为本请求设置写 deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
					return
				}

				// b) 等待直到 write deadline 超时
				<-test.waiter.wait()

				// c) 每次持续写入 1kB 数据，Write
				// 最终会返回 i/o timeout 错误
				func() {
					now := time.Now()
					written := 0
					defer func() {
						t.Logf("server: after timeout, Write wrote %dkB, duration: %s", written/1000, time.Since(now))
					}()
					for {
						n, err := w.Write(bytes.Repeat([]byte("a"), 1000))
						if err != nil {
							handlerErrCh <- err
							break
						}
						written += n
					}
				}()
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				defer test.waiter.close()
				client := server.Client()
				client.Timeout = wait.ForeverTestTimeout

				_, err := client.Get(server.URL)
				// d) 验证客户端收到相应的错误。
				// 客户端不应看到响应体，因为超时在 handler 返回之前
				// 就已到期，且 handler 在超时前从未调用 flush。
				test.clientErrVerifier.verify(t, err)
			}()

			// e) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithFlushReturnsErrorAfterDeadline 验证 deadline 过后的 FlushError
// 立即返回超时错误——即使此前写入的数据还停留在缓冲区。
func TestPerRequestWithFlushReturnsErrorAfterDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-request 写 deadline 的行为。
	//
	// 场景：handler 在 write 超时发生后写入并 flush ResponseWriter 对象：
	//  - a) write deadline 设置为 100ms
	//  - b) handler 等待直到 write deadline 超时
	//  - c) handler 写入若干字节，使缓冲区中有内容可 flush
	//  - d) handler 调用 FlushError
	// 超时在 handler 写入 ResponseWriter 对象之前发生，
	// 因此预期如下：
	//    e) 客户端：预期从服务器收到一个错误
	//    f) 服务器：ResponseWriter 对象的 FlushError 方法应
	//       立即返回 "i/o timeout" 错误。
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		clientErrVerifier  verifier
		handlerErrVerifier verifier
	}{
		{
			protoMajor: 1,
			// write timeout 设置为 100ms，等待 5s 应当
			// 足以抵御 CI 中的偶发失败。
			waiter:             &waitWithDuration{after: 5 * time.Second},
			clientErrVerifier:  wantContains{contains: "local error: tls: bad record MAC"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			waiter:             &waitForClose{after: make(chan time.Time)},
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}
				flusher, ok := w.(interface{ FlushError() error })
				if !ok {
					t.Errorf("server: expected ResponseWriter object to implement FlushError")
					return
				}

				// a) 为本请求设置写 deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
					return
				}

				// b) 等待直到 write deadline 超时
				<-test.waiter.wait()

				// c) 写入若干字节，使缓冲区中有内容可 flush，
				// 客户端绝不应在响应中看到这些字节。
				// 注意：由于内部带缓冲的 Writer 大小为 4kB，这次
				// Write 操作不应真正写入 net.Conn
				if _, err := w.Write([]byte("hello")); err != nil {
					t.Errorf("server: unexpected error from Write after timeout: %v", err)
					return
				}
				// d) flush 缓冲区中的内容
				// 注意：另一方面，预期 FlushError 会
				// 立即返回超时错误。
				if err := flusher.FlushError(); err != nil {
					handlerErrCh <- err
				}

				// 注意：预期 handler goroutine 一旦从 Write 或 FlushError
				// 收到错误就立即终止。这正是我们防止请求 handler
				// 无限运行的方式。
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				defer test.waiter.close()

				_, err := server.Client().Get(server.URL)
				// e) 验证客户端收到相应的错误
				test.clientErrVerifier.verify(t, err)
			}()

			// f) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithClientNeverReceivesContentFlushedAfterDeadline 验证在 deadline
// 之前写入、deadline 之后才 flush 的内容不会到达客户端。
func TestPerRequestWithClientNeverReceivesContentFlushedAfterDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 写 deadline 的行为。
	//
	// 场景：handler 在 write 超时发生之前写入 ResponseWriter 对象但不 flush：
	//  - a) handler 写入若干字节，使后续有内容可 flush
	//  - b) write deadline 设置为 100ms
	//  - c) 等待直到 write deadline 超时
	//  - d) handler 调用 FlushError
	// 超时发生在 handler 写入之后、但尚未 flush ResponseWriter 对象之前，
	// 预期如下：
	//  - e) 客户端：预期从服务器收到一个错误
	/// - f) 服务器：ResponseWriter 对象的 FlushError 方法应
	//       立即返回 "i/o timeout" 错误
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		clientErrVerifier  verifier
		handlerErrVerifier verifier
	}{
		{
			protoMajor: 1,
			// write timeout 设置为 100ms，等待 5s 应当
			// 足以抵御 CI 中的偶发失败。
			waiter:             &waitWithDuration{after: 5 * time.Second},
			clientErrVerifier:  wantContains{contains: "local error: tls: bad record MAC"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			waiter:             &waitForClose{after: make(chan time.Time)},
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}
				flusher, ok := w.(interface{ FlushError() error })
				if !ok {
					t.Errorf("server: expected ResponseWriter object to implement FlushError")
					return
				}

				// a) 写入若干字节，使内部缓冲区
				// 未满，先不要 flush
				if _, err := w.Write([]byte("hello")); err != nil {
					t.Errorf("server: unexpected error from Write: %v", err)
					return
				}

				// b) 为本请求设置 write deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
					return
				}

				// c) 等待直到 write deadline 超时
				<-test.waiter.wait()

				// d) flush 缓冲区中的内容
				if err := flusher.FlushError(); err != nil {
					handlerErrCh <- err
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				defer test.waiter.close()

				_, err := server.Client().Get(server.URL)
				// f) 验证客户端收到相应的错误
				// 注意：由于 'a'（handler 写入但未 flush），且
				// handler 在超时前没有返回，客户端不会收到包含
				// 已写数据的 http.Response。
				test.clientErrVerifier.verify(t, err)
			}()

			// f) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithClientReceivesContentFlushedBeforeDeadline 验证 deadline 前已
// 写入并 flush 的内容可以到达客户端；deadline 后的写入失败不应影响已发送部分。
func TestPerRequestWithClientReceivesContentFlushedBeforeDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 写 deadline 的行为。
	//
	// 场景：handler 在 write deadline 超时之前写入并 flush ResponseWriter 对象：
	//  - a) handler 向 ResponseWriter 对象写入消息 1（"hello"）
	//  - b) handler 调用 FlushError
	//  - c) write deadline 设置为 100ms
	//  - d) 等待直到 write deadline 超时
	//  - e) handler 向 ResponseWriter 对象写入消息 2（"world"）
	//  - f) handler 调用 FlushError
	// 超时发生在 handler 写入并 flush ResponseWriter 对象之后，
	// 因此预期如下：
	//  - g) 客户端：预期从服务器收到响应（200 OK），读取响应 Body 时，
	//       客户端会先从流中读到消息 1，随后遇到错误。
	//       客户端绝不应看到消息 2。
	//  - h) 服务器：ResponseWriter 对象的 FlushError 方法应在
	//       write 超时后立即返回 "i/o timeout" 错误
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		clientErrVerifier  verifier
		handlerErrVerifier verifier
	}{
		{
			protoMajor: 1,
			// write timeout 设置为 100ms，等待 5s 应当
			// 足以抵御 CI 中的偶发失败。
			waiter:             &waitWithDuration{after: 5 * time.Second},
			clientErrVerifier:  wantContains{contains: "local error: tls: bad record MAC"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			waiter:             &waitForClose{after: make(chan time.Time)},
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			msg1, msg2 := "hello", "world"
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}
				flusher, ok := w.(interface{ FlushError() error })
				if !ok {
					t.Errorf("server: expected ResponseWriter object to implement FlushError")
					return
				}

				// a) 写入消息 1
				if _, err := w.Write([]byte(msg1)); err != nil {
					t.Errorf("server: unexpected error from Write: %v", err)
					return
				}
				// b) flush 已写入缓冲区的内容
				if err := flusher.FlushError(); err != nil {
					t.Errorf("server: unexpected error from FlushError: %v", err)
					return
				}

				// c) 为本请求设置 write deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
					return
				}

				// d) 等待直到 write deadline 超时
				<-test.waiter.wait()

				// e) 写入消息 2（客户端绝不应看到
				// 以下消息，测试将在步骤 g) 中
				// 验证这一点）
				if _, err := w.Write([]byte(msg2)); err != nil {
					t.Errorf("server: unexpected error from Write after timeout: %v", err)
				}
				// f) flush 缓冲区中的内容
				if err := flusher.FlushError(); err != nil {
					handlerErrCh <- err
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				defer test.waiter.close()
				resp, err := server.Client().Get(server.URL)
				if err != nil {
					t.Errorf("unexpected error from client.Get: %v", err)
					return
				}
				// g) 客户端预期从服务器收到响应，因为内容
				// 在 write deadline 超时前已写入并 flush
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
				}
				defer closeResponseBody(t, resp)

				// g) 客户端预期从响应 Body 中读到在 write
				// deadline 超时前已 flush 的内容
				b := make([]byte, len(msg1))
				if _, err := io.ReadFull(resp.Body, b); err != nil || msg1 != string(b) {
					t.Errorf("expected to read the initial write from the server, but got: %v", err)
				}

				// g) 此时，任何进一步读取响应
				// Body 的尝试都应产生错误
				_, err = io.ReadAll(resp.Body)
				test.clientErrVerifier.verify(t, err)
			}()

			// h) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithHandlerShouldAbortWriteAfterDeadline 验证无限写入的 handler 会在
// deadline 到达时因 Write 报错而终止，不会永久挂起占用连接。
func TestPerRequestWithHandlerShouldAbortWriteAfterDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 写 deadline 的行为。
	//
	// 场景：handler 无限持续写入 ResponseWriter 对象：
	//  - a) write deadline 设置为 300ms
	//  - b) handler 无限持续写入 ResponseWriter 对象，
	//       不显式调用 FlushError
	// 预期：
	//  - c) 客户端：由于服务器已写入并 flush 了部分内容，
	//       预期从服务器收到响应（200 OK）。
	//  - d) 服务器：预期 Write 方法在 write deadline 到期后
	//       立即返回 "i/o timeout" 错误，并且 handler
	//       因此终止
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		clientErrVerifier  verifier
		handlerErrVerifier verifier
	}{
		{
			protoMajor: 1,
			clientErrVerifier: wantEitherOr{
				err:      io.ErrUnexpectedEOF,
				contains: "local error: tls: bad record MA",
			},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}

				// a) 为本请求设置写 deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
					return
				}

				// b) 无限持续写入 ResponseWriter 对象
				if _, err := io.Copy(w, neverEnding('a')); err != nil {
					handlerErrCh <- err
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				client := server.Client()
				client.Timeout = wait.ForeverTestTimeout
				resp, err := client.Get(server.URL)
				// c) 正常情况下客户端预期从服务器收到响应，
				// 但在压力下服务器也可能返回错误：
				//   go test -race k8s.io/apiserver/pkg/endpoints/filters -c
				//   stress -p=256 ./filters.test -test.run=TestPerRequestWriteDeadlineWithHandlerWritingIndefinitely
				// 上述压力测试会使服务器返回错误
				if err != nil {
					test.clientErrVerifier.verify(t, err)
					return
				}
				defer closeResponseBody(t, resp)
				_, err = io.ReadAll(resp.Body)
				// c) 读取响应 Body 应产生错误
				test.clientErrVerifier.verify(t, err)
			}()

			// d) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithBodyReadShouldTimeoutAfterDeadline 验证读 deadline 能中断
// “客户端迟迟不发送 body”的请求，避免服务端 goroutine 被慢客户端拖住。
func TestPerRequestWithBodyReadShouldTimeoutAfterDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 读 deadline 的行为。
	//
	// 场景：客户端设置了请求 body，但从不发送任何内容：
	//  - a) 客户端设置请求 body，保持流打开但不发送任何内容
	//  - b) read deadline 设置为 500ms
	//  - c) handler 尝试读取请求的 Body
	//  - d) read 超时到期
	//  - e) handler 写入并 flush ResponseWriter 对象
	// read 超时发生后，预期如下：
	//  - f) 客户端：预期看到服务器的响应，因为本测试中
	//       handler 正常完成了执行
	//  - g) 服务器：读取请求 Body 应产生
	//       "i/o timeout" 错误
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		handlerErrVerifier verifier
	}{
		{
			protoMajor:         1,
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			msg := "hello"
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}
				flusher, ok := w.(interface{ FlushError() error })
				if !ok {
					t.Errorf("server: expected ResponseWriter object to implement FlushError")
					return
				}

				// b) 为本请求设置 read deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetReadDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetReadDeadline: %v", err)
					return
				}

				// c) 读取请求的 Body
				if _, err := io.Copy(io.Discard, req.Body); err != nil {
					// d) 由于客户端在 read deadline 到期前从未
					// 发送任何数据，read 超时在此处到期
					handlerErrCh <- err
				}

				// e) read 超时发生后，handler 写入并
				// flush ResponseWriter 对象
				if _, err := w.Write([]byte(msg)); err != nil {
					t.Errorf("server: unexpected error from Write after read timeout: %v", err)
					return
				}
				if err := flusher.FlushError(); err != nil {
					t.Errorf("server: unexpected error from FlushError after read timeout: %v", err)
					return
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			reader, writer := io.Pipe()
			defer func() {
				if err := writer.Close(); err != nil {
					t.Errorf("expected no error from Close, but got: %v", err)
				}
			}()

			func() {
				// a) 客户端设置了带 Body 的请求，
				// 但从不向其写入。
				resp, err := server.Client().Post(server.URL, "text/foo", reader)
				if err != nil {
					t.Errorf("expected no error from Post, but got: %v", err)
					return
				}
				defer closeResponseBody(t, resp)

				// f) 由于 handler 已写出响应，我们预期
				// 客户端能成功读取它。
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
				}
				if got, err := io.ReadAll(resp.Body); err != nil || string(got) != msg {
					t.Errorf("expected the client to read the response: want: %q, got: %q, error: %v", msg, string(got), err)
				}
			}()

			// g) 验证 handler 返回了正确的错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithBodyReadShouldYieldPartialContentBeforeDeadline 验证 deadline 前
// 已读到的 body 数据不会因后续读取超时而丢失或回滚。
func TestPerRequestWithBodyReadShouldYieldPartialContentBeforeDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-request 读 deadline 的行为。
	//
	// 场景：handler 部分读取请求 Body 之后 read deadline 到期：
	//  - a) 客户端设置带 Body 的请求
	//  - b) 客户端向 Body 写入（消息 1 + 消息 2），然后保持流打开
	//  - c) handler 从请求 Body 恰好读取 N 字节，N=len(消息 1)
	//  - d) read deadline 设置为 300ms
	//  - e) handler 尝试读取请求 body 的剩余字节
	//  - f) read 超时在 300ms 后发生（因为流仍打开）
	//  - g) handler 终止
	//  - h) handler 终止后客户端关闭流
	// 预期：
	//  - i) 客户端：由于本测试中 handler 正常返回，
	//       预期客户端会看到服务器的响应。
	//  - j) 服务器：handler 在读取请求 Body 时应遇到
	//       "i/o timeout" 错误
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		handlerErrVerifier verifier
	}{
		{
			protoMajor:         1,
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
		{
			protoMajor:         2,
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			msg1, msg2 := "hello", "world"
			handlerErrCh, handlerDoneCh := make(chanErr, 1), make(chan struct{})
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				defer close(handlerDoneCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected an ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}

				// c) 从请求 Body 读取消息 1，N = len(消息 1)
				b := make([]byte, len(msg1))
				n, err := io.ReadFull(req.Body, b)
				if err != nil || string(b[:n]) != msg1 {
					t.Errorf("server: expected content in request body, want: %s, got: %s, error: %v", msg1, string(b[:n]), err)
					return
				}

				// d) 为本请求设置 read deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetReadDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error from SetReadDeadline: %v", err)
					return
				}

				// e) handler 读取 Body 的剩余字节，
				// 最终超时。
				// 注意：此时请求 Body 中仍有一些字节剩余，
				// 且不会到达 EOF，因为客户端会一直保持流
				// 打开，直到 handler 终止之后
				got, err := io.ReadAll(req.Body)
				// f) read 超时已发生
				if msg2 != string(got) {
					t.Errorf("server: expected %q, but got: %q", msg2, string(got))
					return
				}
				if err != nil {
					handlerErrCh <- err
				}

				// g) handler 终止
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			reader, writer := io.Pipe()
			clientWriterDoneCh := make(chan error, 1)
			go func() {
				defer close(clientWriterDoneCh)
				// b) 向 Body 写入（消息 1 + 消息 2），
				// 但先不关闭流
				if _, err := writer.Write([]byte(msg1 + msg2)); err != nil {
					t.Errorf("unexpected error from Write to Body of the request: %v", err)
				}

				// h) 等待 handler 终止后再
				// 关闭流
				<-handlerDoneCh
				if err := writer.Close(); err != nil {
					t.Errorf("unexpected error from Close: %v", err)
				}
			}()

			client := server.Client()

			// a) 设置带 Body 的请求
			resp, err := client.Post(server.URL, "text/foo", reader)
			if err != nil {
				t.Errorf("expected no error, but got: %v", err)
				return
			}

			select {
			case <-clientWriterDoneCh:
			case <-time.After(wait.ForeverTestTimeout):
				t.Errorf("expected the client writer to have terminated")
			}

			// i) 客户端收到响应，因为 handler 正常终止
			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
			}
			defer closeResponseBody(t, resp)

			// j) 验证 handler 返回了正确的错误
			err = handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}
}

// TestPerRequestWithReadingEmptyBodyShouldNotYieldErrorAfterDeadline 验证空 body 的
// 读取不受读 deadline 影响（无数据可读时无需等待网络）。
func TestPerRequestWithReadingEmptyBodyShouldNotYieldErrorAfterDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-request 读 deadline 的行为。
	//
	// 场景：如果请求 body 为空，read deadline 会有影响吗？
	//  - a) 客户端设置一个带空 body 的请求
	//  - b) read deadline 设置为 100ms
	//  - c) 等待 read deadline 到期
	//  - d) handler 读取请求的 Body
	// 预期：
	//  - e) 服务器：read 超时对 handler 没有影响
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		handlerErrVerifier verifier
	}{
		{
			protoMajor:         1,
			waiter:             &waitWithDuration{after: 5 * time.Second},
			handlerErrVerifier: wantNoError{},
		},
		{
			protoMajor:         2,
			waiter:             &waitWithDuration{after: 5 * time.Second},
			handlerErrVerifier: wantNoError{},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerErrCh)
				if req.ProtoMajor != test.protoMajor {
					t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
					return
				}

				// b) 为本请求设置 read deadline
				ctrl := http.NewResponseController(w)
				if err := ctrl.SetReadDeadline(time.Now().Add(deadline)); err != nil {
					t.Errorf("server: unexpected error SetReadDeadline: %v", err)
					return
				}

				// c) 等待 read deadline 到期
				<-test.waiter.wait()

				// 我们预期读取空 body 不会超时，因为对于服务端
				// 请求，Request Body 永远非 nil，但在没有 body 时
				// 会立即返回 EOF。
				t.Logf("request Body type: %T", req.Body)
				// d) handler 读取请求的 Body，
				// 这应立即返回且不产生任何错误。
				if _, err := io.Copy(io.Discard, req.Body); err != nil {
					handlerErrCh <- err
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			func() {
				defer test.waiter.close()
				// a) 客户端设置一个不带任何 body 的请求
				resp, err := server.Client().Get(server.URL)
				if err != nil {
					t.Errorf("expected no error from Post, but got: %v", err)
					return
				}
				defer closeResponseBody(t, resp)
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
				}
			}()

			// e) 验证 handler 没有返回错误
			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)
		})
	}

}

// TestPerRequestWithHijackedConnectionShouldResetDeadline 验证 hijack 后的连接
// 不受 handler 设置的读/写 deadline 约束——hijack 相当于接管原始 net.Conn。
func TestPerRequestWithHijackedConnectionShouldResetDeadline(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 读/写 deadline 的行为。
	//
	// 场景：read/write deadline 会对被 hijack 的连接产生
	// 不利影响吗？
	//  - a) 为该请求同时设置 read 和 write deadline
	//  - b) handler hijack 该连接
	//  - c) 让 read 和 write deadline 到期
	//  - d) handler 使用被 hijack 的连接向客户端发送消息
	//  - e) 客户端收到该消息
	// 我们预期 handler 设置的 read/write deadline 对被 hijack
	// 的连接没有影响。
	t.Parallel()

	const deadline = 100 * time.Millisecond
	msg := "hello"
	handlerDoneCh := make(chanErr, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(handlerDoneCh)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("server: expected ResponseWriter object to implement Hijacker")
			return
		}

		// a) 为本请求同时设置 read 和 write deadline
		ctrl := http.NewResponseController(w)
		if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
			t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
			return
		}
		if err := ctrl.SetReadDeadline(time.Now().Add(deadline)); err != nil {
			t.Errorf("server: unexpected error from SetReadDeadline: %v", err)
			return
		}

		// b) hijack 该连接
		conn, bufrw, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("server: unexpected error from Hijack: %v", err)
			return
		}

		// c) 让 read 和 write deadline 都到期，5s 应当足以
		// 抵御 CI 中的任何偶发失败
		<-time.After(5 * time.Second)

		// d) 使用原始连接向客户端发送消息，
		// read/write 超时后连接应当仍然可用
		payload := fmt.Sprintf("HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(msg), msg)
		if _, err := bufrw.Write([]byte(payload)); err != nil {
			t.Errorf("server: unexpected error from Write: %v", err)
			return
		}
		if err := bufrw.Flush(); err != nil {
			t.Errorf("server: unexpected error from Flush: %v", err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Errorf("unexpected error from Close: %v", err)
		}
	}))

	defer server.Close()
	server.StartTLS()

	func() {
		resp, err := server.Client().Get(server.URL)
		if err != nil {
			t.Errorf("expected no error from client.Get")
			return
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
		}
		defer closeResponseBody(t, resp)
		// e) 验证客户端如预期收到消息
		if got, err := io.ReadAll(resp.Body); err != nil || msg != string(got) {
			t.Errorf("expected to read what the hijacked connection wrote, want: %q, got: %q, err: %v", msg, got, err)
		}
	}()

	err := handlerDoneCh.wait(t, time.After(wait.ForeverTestTimeout))
	wantNoError{}.verify(t, err)
}

// TestPerRequestWithConnectionIsReused 验证首个请求超时后连接仍可复用，
// 从而确认分母端 deadline 不会污染 keep-alive 连接的后续请求。
func TestPerRequestWithConnectionIsReused(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-request 写 deadline 的行为。
	//
	// 场景：我们想观察当连接上承载的前一个请求因 write 超时失败后，
	// 客户端的连接是否会被复用。我们依次发送两个请求 "/1" 和 "/2"：
	//  - a) 客户端发送第一个请求，路径为 "/1"
	//  - b) handler 为第一个请求写入并 flush 一些内容
	//  - c) handler 为第一个请求设置 write deadline 为 100ms
	//  - d) handler 等待直到 write deadline 超时
	//  - e) 超时后 handler 写入并 flush ResponseWriter 对象
	//  - f) 客户端收到第一个请求的响应
	//  - g) 客户端发送第二个请求，路径为 "/2"
	//  - h) 服务器向客户端发送 200 OK
	t.Parallel()

	const deadline = 100 * time.Millisecond
	tests := []struct {
		protoMajor         int
		waiter             waiter
		clientErrVerifier  verifier
		handlerErrVerifier verifier
		connReuseFn        func(t *testing.T, ci httptrace.GotConnInfo)
	}{
		{
			protoMajor: 1, // http/1x
			// write timeout 设置为 100ms，等待 5s 应当足以
			// 抵御 CI 中的偶发失败。
			waiter:             &waitWithDuration{after: 5 * time.Second},
			clientErrVerifier:  wantContains{contains: "local error: tls: bad record MAC"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
			connReuseFn:        shouldUseNewConnection,
		},
		{
			protoMajor:         2, // http/2.0
			waiter:             &waitForClose{after: make(chan time.Time)},
			clientErrVerifier:  wantContains{"stream error: stream ID 1; INTERNAL_ERROR; received from peer"},
			handlerErrVerifier: wantError{err: os.ErrDeadlineExceeded},
			connReuseFn:        shouldUseExistingConnection,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("HTTP/%d", test.protoMajor), func(t *testing.T) {
			handlerErrCh := make(chanErr, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.URL.Path == "/1":
					func() {
						defer close(handlerErrCh)
						if req.ProtoMajor != test.protoMajor {
							t.Errorf("server: expected ProtoMajor: %d, but got: %s", test.protoMajor, req.Proto)
							return
						}
						flusher, ok := w.(interface{ FlushError() error })
						if !ok {
							t.Errorf("server: expected ResponseWriter object to implement FlushError")
							return
						}

						// b) 写入并 flush，使客户端能看到部分内容
						if _, err := w.Write([]byte("hello")); err != nil {
							t.Errorf("server: unexpected error from Write: %v", err)
							return
						}
						if err := flusher.FlushError(); err != nil {
							t.Errorf("server: unexpected error from FlushError: %v", err)
							return
						}

						// c) 为第一个请求设置 write deadline
						ctrl := http.NewResponseController(w)
						if err := ctrl.SetWriteDeadline(time.Now().Add(deadline)); err != nil {
							t.Errorf("server: unexpected error from SetWriteDeadline: %v", err)
							return
						}

						// d) 等待直到 write deadline 超时
						<-test.waiter.wait()

						// e) 写入并 flush 以获取超时错误
						if _, err := w.Write([]byte("world")); err != nil {
							t.Errorf("server: unexpected error from Write after timeout: %v", err)
							return
						}
						if err := flusher.FlushError(); err != nil {
							handlerErrCh <- err
							return
						}
					}()
				case req.URL.Path == "/2":
					// h) 在此返回，向客户端发送 200 OK
				}
			}))

			defer server.Close()
			if test.protoMajor == 2 {
				server.EnableHTTP2 = true
			}
			server.StartTLS()

			client := server.Client()
			func() {
				defer test.waiter.close()
				// a) 发送第一个请求，它应当总是走新的连接
				resp, err := sendWithTrace(t, client, server.URL+"/1", shouldUseNewConnection)
				if err != nil {
					t.Errorf("unexpected error from client.Get for the first request: %v", err)
					return
				}

				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
				}
				defer closeResponseBody(t, resp)

				_, err = io.ReadAll(resp.Body)
				// f) 验证客户端收到相应的错误
				test.clientErrVerifier.verify(t, err)
			}()

			err := handlerErrCh.wait(t, time.After(wait.ForeverTestTimeout))
			test.handlerErrVerifier.verify(t, err)

			// g) 使用同一个底层 transport 对象发送第二个请求，
			// 验证第一个请求的连接被复用
			func() {
				resp, err := sendWithTrace(t, client, server.URL+"/2", test.connReuseFn)
				if err != nil {
					t.Errorf("unexpected error from client.Get: %v", err)
					return
				}
				defer closeResponseBody(t, resp)
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
				}
			}()
		})
	}
}

// TestPerRequestWithSlowReader 是一个跳过的手动实验：模拟慢速读客户端，
// 用于复现“写超时被慢读放大”的场景，不适合放进常规 CI。
func TestPerRequestWithSlowReader(t *testing.T) {
	// 本测试用标准 net http 服务器记录 per-handler 写 deadline 的行为。
	//
	// 场景：通过在客户端读取服务器响应时加入延迟来模拟慢速网络：
	// - a) handler 为请求分配一个 deadline 为 5ms 的 context
	// - b) handler 为该请求设置 write deadline 为 50ms
	// - c) handler 每次写入并 flush 1kB 数据到 ResponseWriter 对象，
	//   直到写满 10MB 数据或 write deadline 到期，以先到者为准
	// - d) 客户端收到服务器的响应
	// - e) 客户端每次读取 125 字节，间隔延迟 1ms
	t.Skip("This test is experimental, to simulate a client that reads the response body slowly")

	const (
		// 设为 false 可复现无限挂起的请求
		// 注意：将其设为 false 会导致 CI 中的测试超时
		enableWriteTimeout = true

		ctxDeadline   = 5 * time.Millisecond
		writeDeadline = 50 * time.Millisecond
	)

	handlerErrCh := make(chanErr, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		now := time.Now()
		var written int
		defer func() {
			t.Logf("server: the handler completed, written %dkB of payload, took: %s", written/1000, time.Since(now))
			close(handlerErrCh)
		}()

		// a) 分配一个 deadline 为 5ms 的 context
		// 注意：context deadline 对 ResponseWriter 对象没有影响
		ctx, cancel := context.WithTimeout(context.Background(), ctxDeadline)
		defer cancel()
		req = req.WithContext(ctx)

		if req.ProtoMajor != 2 {
			t.Errorf("server: expected an HTTP/2.0 request, but got: %s", req.Proto)
			return
		}

		flusher, ok := w.(interface{ FlushError() error })
		if !ok {
			t.Errorf("server: expected ResponseWriter object to implement FlushError")
			return
		}
		if enableWriteTimeout {
			// b) 为该请求设置 write deadline
			ctrl := http.NewResponseController(w)
			if err := ctrl.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
				t.Errorf("unexpected error from SetWriteDeadline: %v", err)
				return
			}
		}

		// c) handler 每次写入 1kB 数据，最多写入
		// 10MB（10*1000*1000）字节，一旦 Write 返回错误
		// 就立即中止。
		for i := 0; i < 10*1000; i++ {
			n, err := w.Write(bytes.Repeat([]byte("a"), 1000))
			if err != nil {
				handlerErrCh <- err
				return
			}
			written += n
			if err := flusher.FlushError(); err != nil {
				handlerErrCh <- err
				return
			}
		}
	}))

	defer server.Close()
	server.EnableHTTP2 = true
	server.StartTLS()

	clientErrCh := make(chanErr, 1)
	go func() {
		defer close(clientErrCh)

		// 客户端侧不设置带 deadline 的请求 context
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/foo", nil)
		if err != nil {
			t.Errorf("failed to create a new request: %v", err)
			return
		}

		resp, err := server.Client().Do(req)
		if err != nil {
			t.Errorf("unexpected error from client.Do: %v", err)
			return
		}

		// d) 客户端预期收到服务器的响应
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected staus code: %d, but got: %d", http.StatusOK, resp.StatusCode)
		}
		defer closeResponseBody(t, resp)

		// e) 我们每次读取 125 字节，然后休眠 1ms，
		// 这会给整个响应读取过程增加 80 秒
		// （10*1000*1000/125 毫秒）的延迟，以此模拟
		// 超过 wait.ForeverTestTimeout（30s）的慢读者或慢网络，
		// 因此我们预期在 ForeverTestTimeout 到期后
		// 测试会超时并失败。
		//
		// 在没有 write 超时的情况下，我们观察到：
		// 服务器：
		//  - handler 成功写完了整个 10MB 的 payload，
		//    handler 完成大约耗时 1 分钟。
		//  - FlushError 正常完成、没有报错
		//  - handler 在 1 分钟后返回
		// 注意：请求 context 带有 5ms 的 deadline，对
		// Write/Flush 操作没有任何影响。
		//
		// 客户端：
		//  - 客户端持续读取响应 body，如果测试不在
		//    ForeverTestTimeout 到期后提前结束，客户端
		//    最终会成功读完整个 body。
		//  - 由于响应的 Body 仍在使用中，连接会保持活跃
		//
		// 当我们在服务端启用 write 超时后，会看到：
		// 服务器：
		//  - write 超时在 50ms 后发生，handler 只写完了
		//    部分 payload。
		//  - Write 或 FlushError 返回 'i/o timeout' 错误
		//  - handler 在 write 超时后立即返回
		//
		// 客户端：
		//  - 客户端会读完响应 body 中已写出的那部分 payload，
		//    随后会看到 stream reset 错误
		b := make([]byte, 125)
		now := time.Now()
		var read, iteration int
		for {
			iteration++
			n, err := io.ReadFull(resp.Body, b)
			if err != nil {
				clientErrCh <- err
				break
			}
			read += n
			time.Sleep(time.Millisecond)
			if iteration%1000 == 0 {
				t.Logf("elapsed=%s, read=%dkB", time.Since(now), read/1000)
			}
		}
		t.Logf("elapsed=%s, read=%dKB", time.Since(now), read/1000)
	}()

	handlerWait, clientWait := time.After(wait.ForeverTestTimeout), time.After(wait.ForeverTestTimeout)
	err := handlerErrCh.wait(t, handlerWait)
	wantError{err: os.ErrDeadlineExceeded}.verify(t, err)

	err = clientErrCh.wait(t, clientWait)
	wantContains{contains: "stream error: stream ID 1; INTERNAL_ERROR; received from peer"}.verify(t, err)
}

// sendWithTrace 发送 GET 并在拿到连接时回调 f，用于断言复用/新建连接。
func sendWithTrace(t *testing.T, client *http.Client, url string, f func(*testing.T, httptrace.GotConnInfo)) (*http.Response, error) {
	t.Helper()
	ctx := t.Context()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("failed to create new request: %v", err)
	}

	trace := &httptrace.ClientTrace{
		GotConn: func(ci httptrace.GotConnInfo) {
			t.Logf("GotConnInfo: %+v", ci)
			f(t, ci)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	return client.Do(req)
}

func shouldUseExistingConnection(t *testing.T, ci httptrace.GotConnInfo) {
	t.Helper()
	if !ci.Reused {
		t.Errorf("expected an existing TCP connection to be reused, but got: %+v", ci)
	}
}

func shouldUseNewConnection(t *testing.T, ci httptrace.GotConnInfo) {
	t.Helper()
	if ci.Reused {
		t.Errorf("expected a new connection, but got: %+v", ci)
	}
}

// neverEnding 是无限可读的 io.Reader，用于让 handler 持续写入直到 deadline。
type neverEnding byte

func (b neverEnding) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func closeResponseBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if err := resp.Body.Close(); err != nil {
		t.Errorf("unexpected error while closing the Body of the Response object: %v", err)
	}
}

// waiter 抽象 handler 等待 deadline 到期的两种方式：睡眠或 channel 关闭。
type waiter interface {
	wait() <-chan time.Time
	close()
}

// 基于睡眠的 waiter 实现：请求 handler 在返回前睡眠一段
// 指定时长，我们需要明智地选择睡眠时长，
// 以避免 CI 中的偶发失败。
type waitWithDuration struct {
	after time.Duration
}

func (w waitWithDuration) wait() <-chan time.Time { return time.After(w.after) }
func (w waitWithDuration) close()                 {}

// 基于 channel 的 waiter 实现：请求 handler 等待某个 channel
// 关闭，步骤如下：
// a) 客户端向 http/2.0 服务器发送请求
// a) 请求 handler 设置 per-request write timeout，然后
// b) 请求 handler 无限期阻塞等待该 channel 关闭
// c) write 超时到期，http/2.0 服务器异步重置该 stream
// d) write 超时发生后，客户端立即收到
// stream reset 错误
// e) 随后客户端关闭该 channel
// f) 请求 handler 解除阻塞并终止
//
// 该 waiter 只能用于 http/2.0：请求 handler 运行在与 tcp
// 连接服务 goroutine 不同的 goroutine 上，这使得连接服务循环
// 可以异步重置 http2 stream。另一方面，http/1x 在连接服务
// goroutine 所在的同一 goroutine 中执行请求 handler，
// 这会迫使连接服务 goroutine 等待 handler 返回。
// 参见 https://github.com/golang/go/blob/b8ac61e6e64c92f23d8cf868a92a70d13e20a124/src/net/http/server.go#L3285
type waitForClose struct {
	after chan time.Time
}

func (w waitForClose) wait() <-chan time.Time {
	return w.after
}

func (w waitForClose) close() { close(w.after) }

// chanErr 是带超时的错误通道包装：先取错误，再等通道关闭以确保 handler goroutine 退出。
type chanErr chan error

func (ch chanErr) wait(t *testing.T, timeoutCh <-chan time.Time) error {
	t.Helper()

	var (
		err error
		ok  bool
	)

	select {
	case err, ok = <-ch:
		if !ok {
			return err
		}
	case <-timeoutCh:
		t.Errorf("timed out while waiting to receive from the channel")
	}

	// 我们等待该 channel 被关闭，这使得测试 goroutine
	// 能够等待其他 goroutine 先完成。
	select {
	case <-ch:
	case <-timeoutCh:
		t.Errorf("timed out while waiting to receive from the channel")
	}

	return err
}

// verifier 统一断言错误形态，使各用例可以把“期望错误”作为数据而非代码表达。
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

// wantContains 断言错误文本包含给定子串；用于标准库错误文本可能随版本微调的场景。
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

// wantError 用 errors.Is 断言具体错误（如 os.ErrDeadlineExceeded）。
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

// wantEitherOr 接受两种等价的可观察错误：标准库在不同版本/并发压力下可能返回其一。
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
