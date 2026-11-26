/*
Copyright 2020 The Kubernetes Authors.

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
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// TestProbabilisticGoawayDecider 验证按概率决定是否发送 GOAWAY 的判定器：
// chance=0 永不发送，chance=1 总是发送，中间值由随机源与阈值决定。
func TestProbabilisticGoawayDecider(t *testing.T) {
	cases := []struct {
		name         string
		chance       float64
		nextFn       func(chance float64) func() float64
		expectGOAWAY bool
	}{
		{
			name:   "always not GOAWAY",
			chance: 0,
			nextFn: func(chance float64) func() float64 {
				return rand.Float64
			},
			expectGOAWAY: false,
		},
		{
			name:   "always GOAWAY",
			chance: 1,
			nextFn: func(chance float64) func() float64 {
				return rand.Float64
			},
			expectGOAWAY: true,
		},
		{
			name:   "hit GOAWAY",
			chance: rand.Float64() + 0.01,
			nextFn: func(chance float64) func() float64 {
				return func() float64 {
					return chance - 0.001
				}
			},
			expectGOAWAY: true,
		},
		{
			name:   "does not hit GOAWAY",
			chance: rand.Float64() + 0.01,
			nextFn: func(chance float64) func() float64 {
				return func() float64 {
					return chance + 0.001
				}
			},
			expectGOAWAY: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := probabilisticGoawayDecider{chance: tc.chance, next: tc.nextFn(tc.chance)}
			result := d.Goaway(nil)
			if result != tc.expectGOAWAY {
				t.Errorf("expect GOAWAY: %v, got: %v", tc.expectGOAWAY, result)
			}
		})
	}
}

const (
	urlGet             = "/get"
	urlPost            = "/post"
	urlWatch           = "/watch"
	urlGetWithGoaway   = "/get-with-goaway"
	urlPostWithGoaway  = "/post-with-goaway"
	urlWatchWithGoaway = "/watch-with-goaway"
)

var (
	// responseBody 是测试 GOAWAY server 对每个请求返回的响应体；
	// 对 watch 请求，测试 server 每秒推送 1 字节。
	responseBody = []byte("hello")

	// requestPostBody 是客户端以 POST 方法请求测试 GOAWAY server 时必须发送的请求体，
	// 否则测试 server 会返回 400 HTTP 状态码。
	requestPostBody = responseBody
)

// newTestGOAWAYServer 启动一个启用 HTTP/2 的测试服务端，handler 包装了 GOAWAY filter。
func newTestGOAWAYServer() (*httptest.Server, error) {
	watchHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()

		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(200)

		flusher, _ := w.(http.Flusher)
		flusher.Flush()

		count := 0
		for {
			<-timer.C
			n, err := w.Write(responseBody[count : count+1])
			if err != nil {
				return
			}
			flusher.Flush()
			count += n
			if count == len(responseBody) {
				return
			}
		}
	})
	getHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(responseBody)
		return
	})
	postHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !reflect.DeepEqual(requestPostBody, reqBody) {
			http.Error(w, fmt.Sprintf("expect request body: %s, got: %s", requestPostBody, reqBody), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(responseBody)
		return
	})

	mux := http.NewServeMux()
	mux.Handle(urlGet, WithProbabilisticGoaway(getHandler, 0))
	mux.Handle(urlPost, WithProbabilisticGoaway(postHandler, 0))
	mux.Handle(urlWatch, WithProbabilisticGoaway(watchHandler, 0))
	mux.Handle(urlGetWithGoaway, WithProbabilisticGoaway(getHandler, 1))
	mux.Handle(urlPostWithGoaway, WithProbabilisticGoaway(postHandler, 1))
	mux.Handle(urlWatchWithGoaway, WithProbabilisticGoaway(watchHandler, 1))

	s := httptest.NewUnstartedServer(mux)

	http2Options := &http2.Server{}

	if err := http2.ConfigureServer(s.Config, http2Options); err != nil {
		return nil, fmt.Errorf("failed to configure test server to be HTTP2 server, err: %v", err)
	}

	s.TLS = s.Config.TLSConfig

	return s, nil
}

// watchResponse 包装 watch 响应，包含服务端发送的数据及可能发生的错误。
type watchResponse struct {
	// body 是测试 GOAWAY server 发送给客户端的响应数据。
	body []byte
	// 若 watch 请求既未以 EOF 结束，也未产生 http2.GoAwayError，err 会被设为非 nil。
	err error
}

// requestGOAWAYServer 按给定 url 以指定方法和数据请求测试 GOAWAY 服务器；若请求是 watch，则返回非 nil channel，watch 结束后可从中取得 watchResponse。
// requestGOAWAYServer 发起 watch 请求并把响应流通过 channel 交给调用方，
// 便于测试异步观察 GOAWAY 行为。
func requestGOAWAYServer(client *http.Client, serverBaseURL, url string) (<-chan watchResponse, error) {
	method := http.MethodGet
	var reqBody io.Reader

	if url == urlPost || url == urlPostWithGoaway {
		method = http.MethodPost
		reqBody = bytes.NewReader(requestPostBody)
	}

	req, err := http.NewRequest(method, serverBaseURL+url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("unexpect new request error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed request test server, err: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body and status code is %d, error: %v", resp.StatusCode, err)
		}

		return nil, fmt.Errorf("expect response status code: %d, but got: %d. response body: %s", http.StatusOK, resp.StatusCode, body)
	}

	// 已收到 watch 字节，不应被打断。
	if url == urlWatch || url == urlWatchWithGoaway {
		ch := make(chan watchResponse)
		go func() {
			defer resp.Body.Close()

			body := make([]byte, 0)
			buffer := make([]byte, 1)
			for {
				n, err := resp.Body.Read(buffer)
				if err != nil {
					// urlWatch 会收到 io.EOF；
					// urlWatchWithGoaway 会收到 http2.GoAwayError。
					if err == io.EOF {
						err = nil
					} else if _, ok := err.(http2.GoAwayError); ok {
						err = nil
					}

					ch <- watchResponse{
						body: body,
						err:  err,
					}
					return
				}
				body = append(body, buffer[0:n]...)
			}
		}()
		return ch, nil
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body, error: %v", err)
	}

	if !reflect.DeepEqual(responseBody, body) {
		return nil, fmt.Errorf("expect response body: %s, got: %s", string(responseBody), string(body))
	}

	return nil, nil
}

// TestClientReceivedGOAWAY 验证客户端收到 GOAWAY 后，在途 watch 请求不受影响，且新请求会使用新连接。
// TestClientReceivedGOAWAY 验证客户端能收到并处理 GOAWAY 帧，且可重新建立连接继续 watch。
func TestClientReceivedGOAWAY(t *testing.T) {
	s, err := newTestGOAWAYServer()
	if err != nil {
		t.Fatalf("failed to set-up test GOAWAY http server, err: %v", err)
	}

	s.StartTLS()
	defer s.Close()

	cases := []struct {
		name string
		reqs []string
		// expectConnections 恒等于 GOAWAY 请求数（urlGoaway 或 urlWatchWithGoaway）+ 1。
		expectConnections int
	}{
		{
			name:              "all normal requests use only one connection",
			reqs:              []string{urlGet, urlPost, urlGet},
			expectConnections: 1,
		},
		{
			name:              "got GOAWAY after set-up watch",
			reqs:              []string{urlPost, urlWatch, urlGetWithGoaway, urlGet, urlPost},
			expectConnections: 2,
		},
		{
			name:              "got GOAWAY after set-up watch, and set-up a new watch",
			reqs:              []string{urlGet, urlWatch, urlGetWithGoaway, urlWatch, urlGet, urlPost},
			expectConnections: 2,
		},
		{
			name:              "got 2 GOAWAY after set-up watch",
			reqs:              []string{urlPost, urlWatch, urlGetWithGoaway, urlGetWithGoaway, urlGet, urlPost},
			expectConnections: 3,
		},
		{
			name:              "combine with watch-with-goaway",
			reqs:              []string{urlGet, urlWatchWithGoaway, urlGet, urlWatch, urlGetWithGoaway, urlGet, urlPost},
			expectConnections: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// localAddr 表示建立了多少条 TCP 连接。
			localAddr := make([]string, 0)

			// 创建 http client。
			dialFn := func(network, addr string, cfg *tls.Config) (conn net.Conn, err error) {
				conn, err = tls.Dial(network, addr, cfg)
				if err != nil {
					t.Fatalf("unexpect connection err: %v", err)
				}

				localAddr = append(localAddr, conn.LocalAddr().String())
				return
			}
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{http2.NextProtoTLS},
			}
			tr := &http.Transport{
				TLSHandshakeTimeout: 10 * time.Second,
				TLSClientConfig:     tlsConfig,
				// 禁用连接池，避免额外连接导致测试不稳定。
				//
				MaxIdleConnsPerHost: -1,
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return dialFn(network, addr, tlsConfig)
				},
			}
			if err := http2.ConfigureTransport(tr); err != nil {
				t.Fatalf("failed to configure http transport, err: %v", err)
			}

			client := &http.Client{
				Transport: tr,
			}

			watchChs := make([]<-chan watchResponse, 0)
			for _, url := range tc.reqs {
				w, err := requestGOAWAYServer(client, s.URL, url)
				if err != nil {
					t.Fatalf("failed to request server, err: %v", err)
				}
				if w != nil {
					watchChs = append(watchChs, w)
				}
			}

			// 检查 TCP 连接数。
			if tc.expectConnections != len(localAddr) {
				t.Fatalf("expect TCP connection: %d, actual: %d", tc.expectConnections, len(localAddr))
			}

			// 检查 watch 请求是否被 GOAWAY 帧打断。
			watchTimeout := time.NewTimer(time.Second * 10)
			defer watchTimeout.Stop()
			for _, watchCh := range watchChs {
				select {
				case watchResp := <-watchCh:
					if watchResp.err != nil {
						t.Fatalf("watch response got an unexepct error: %v", watchResp.err)
					}
					if !reflect.DeepEqual(responseBody, watchResp.body) {
						t.Fatalf("in-flight watch was broken by GOAWAY frame, expect response body: %s, got: %s", responseBody, watchResp.body)
					}
				case <-watchTimeout.C:
					t.Error("watch receive timeout")
				}
			}
		})
	}
}

// TestGOAWAYHTTP1Requests 验证 HTTP/1.1 请求不受 GOAWAY 逻辑影响（GOAWAY 是 HTTP/2 概念）。
func TestGOAWAYHTTP1Requests(t *testing.T) {
	s := httptest.NewUnstartedServer(WithProbabilisticGoaway(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}), 1))

	http2Options := &http2.Server{}

	if err := http2.ConfigureServer(s.Config, http2Options); err != nil {
		t.Fatalf("failed to configure test server to be HTTP2 server, err: %v", err)
	}

	s.TLS = s.Config.TLSConfig
	s.StartTLS()
	defer s.Close()

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"http/1.1"},
	}

	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	resp, err := client.Get(s.URL)
	if err != nil {
		t.Fatalf("failed to request the server, err: %v", err)
	}

	if v := resp.Header.Get("Connection"); v != "" {
		t.Errorf("expect response HTTP header Connection to be empty, but got: %s", v)
	}
}

// TestGOAWAYConcurrency 在并发请求下触发 GOAWAY，验证不会丢请求、不会破坏连接复用。
func TestGOAWAYConcurrency(t *testing.T) {
	s, err := newTestGOAWAYServer()
	if err != nil {
		t.Fatalf("failed to set-up test GOAWAY http server, err: %v", err)
	}

	s.StartTLS()
	defer s.Close()

	// 创建 http client。
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{http2.NextProtoTLS},
	}
	tr := &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
		MaxIdleConnsPerHost: 25,
	}
	if err := http2.ConfigureTransport(tr); err != nil {
		t.Fatalf("failed to configure http transport, err: %v", err)
	}

	client := &http.Client{
		Transport: tr,
	}
	if err != nil {
		t.Fatalf("failed to set-up client, err: %v", err)
	}

	const (
		requestCount = 300
		workers      = 10
	)

	expectWatchers := 0

	urlsForTest := []string{urlGet, urlPost, urlWatch, urlGetWithGoaway, urlPostWithGoaway, urlWatchWithGoaway}
	urls := make(chan string, requestCount)
	for i := 0; i < requestCount; i++ {
		index := rand.Intn(len(urlsForTest))
		url := urlsForTest[index]

		if url == urlWatch || url == urlWatchWithGoaway {
			expectWatchers++
		}

		urls <- url
	}
	close(urls)

	wg := &sync.WaitGroup{}
	wg.Add(workers)

	watchers := make(chan (<-chan watchResponse), expectWatchers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			for {
				url, ok := <-urls
				if !ok {
					return
				}

				w, err := requestGOAWAYServer(client, s.URL, url)
				if err != nil {
					t.Errorf("failed to request %q, err: %v", url, err)
				}

				if w != nil {
					watchers <- w
				}
			}
		}()
	}

	wg.Wait()

	// 检查 watch 请求是否被 GOAWAY 帧打断。
	watchTimeout := time.NewTimer(time.Second * 10)
	defer watchTimeout.Stop()
	for i := 0; i < expectWatchers; i++ {
		var watcher <-chan watchResponse

		select {
		case watcher = <-watchers:
		default:
			t.Fatalf("expect watcher count: %d, but got: %d", expectWatchers, i)
		}

		select {
		case watchResp := <-watcher:
			if watchResp.err != nil {
				t.Fatalf("watch response got an unexepct error: %v", watchResp.err)
			}
			if !reflect.DeepEqual(responseBody, watchResp.body) {
				t.Fatalf("in-flight watch was broken by GOAWAY frame, expect response body: %s, got: %s", responseBody, watchResp.body)
			}
		case <-watchTimeout.C:
			t.Error("watch receive timeout")
		}
	}
}
