/*
Copyright 2015 The Kubernetes Authors.

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

package spdy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/armon/go-socks5"

	"k8s.io/streaming/pkg/httpstream"
)

// 让同一个 handler 既能模拟升级成功，也能模拟四种升级失败（缺头、状态码不对等），
// 避免为每种错误情形单独写一套服务端。
type serverHandlerConfig struct {
	shouldError      bool
	statusCode       int
	connectionHeader string
	upgradeHeader    string
}

// shouldError 为真时按配置伪造响应头，用来验证客户端会把它们判为升级失败；
// 为假时才真正跑一遍 SPDY 握手，并回显收到的第一条流。
func serverHandler(t *testing.T, config serverHandlerConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if config.shouldError {
			if e, a := httpstream.HeaderUpgrade, req.Header.Get(httpstream.HeaderConnection); e != a {
				t.Fatalf("expected connection=upgrade header, got '%s", a)
			}

			w.Header().Set(httpstream.HeaderConnection, config.connectionHeader)
			w.Header().Set(httpstream.HeaderUpgrade, config.upgradeHeader)
			w.WriteHeader(config.statusCode)

			return
		}

		// 通过 channel 把新流从回调里交给主 goroutine：回调在 SPDY 收帧 goroutine 上执行，
		// 直接在里面做读写会和后面的 io.Copy 抢同一个流。
		streamCh := make(chan httpstream.Stream)

		responseUpgrader := NewResponseUpgrader()
		spdyConn := responseUpgrader.UpgradeResponse(w, req, func(s httpstream.Stream, replySent <-chan struct{}) error {
			streamCh <- s
			return nil
		})
		if spdyConn == nil {
			t.Fatal("unexpected nil spdyConn")
		}
		defer spdyConn.Close()

		stream := <-streamCh
		io.Copy(stream, stream)
	}
}

// 用一个类型同时表示“造 HTTP 服务”和“造 HTTPS 服务”，测试表就能只换工厂不改流程。
type serverFunc func(http.Handler) *httptest.Server

// 同时覆盖普通转发和 CONNECT 隧道两种代理行为，并记录 hook 观察到的请求信息，
// 这样测试能在同一次请求里校验代理是否被用到、以及认证头是否正确。
type testHTTPProxyHandler struct {
	handlerDone sync.WaitGroup
	hook        func(*http.Request) bool
	httpProxy   httputil.ReverseProxy
	t           testing.TB
}

// hook 返回 false 代表这条请求该被代理拒绝；用它来触发客户端侧的错误分支。
func newTestHTTPProxyHandler(t testing.TB, hook func(*http.Request) bool) *testHTTPProxyHandler {
	return &testHTTPProxyHandler{
		hook: hook,
		httpProxy: httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = "http"
				req.URL.Host = req.Host
			},
		},
		t: t,
	}
}

// WaitGroup 包住整个 ServeHTTP：CONNECT 隧道会 hijack 连接并长时间阻塞，
// 测试结束时必须先等 handler 退出，再关掉连接，避免把 t.Logf 打到已结束的测试上。
func (h *testHTTPProxyHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	h.handlerDone.Add(1)
	defer h.handlerDone.Done()

	if h.hook != nil {
		if ok := h.hook(req); !ok {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	// 普通请求交给 ReverseProxy；只有 CONNECT 需要自己拨号并接管原始字节流，
	// 因为ReverseProxy 不会把 TCP 连接转换成隧道。
	if req.Method != http.MethodConnect {
		h.httpProxy.ServeHTTP(rw, req)
		return
	}

	sconn, err := net.Dial("tcp", req.Host)
	if err != nil {
		h.t.Logf("failed to dial proxy backend, host=%s: %v", req.Host, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer sconn.Close()

	hj, ok := rw.(http.Hijacker)
	if !ok {
		h.t.Logf("response writer cannot hijack for host=%s", req.Host)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 200 必须先写：Hijack 之后 net/http 不再代写响应，
	// 而客户端只有在收到 200 后才会把连接当作隧道使用。
	rw.WriteHeader(http.StatusOK)
	conn, brw, err := hj.Hijack()
	if err != nil {
		h.t.Logf("failed to hijack client connection, host=%s: %v", req.Host, err)
		return
	}
	defer conn.Close()

	if err := brw.Flush(); err != nil {
		h.t.Logf("failed to flush pending writes, host=%s: %v", req.Host, err)
		return
	}
	// 只转发 Hijack 前已经读进 bufio.Reader 的部分，后续字节交给下面的双向 io.Copy，
	// 否则预读数据会丢失，隧道里的第一条 SPDY 帧就不完整了。
	if _, err := io.Copy(sconn, io.LimitReader(brw, int64(brw.Reader.Buffered()))); err != nil {
		h.t.Logf("failed to flush buffered reads, host=%s: %v", req.Host, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(conn, sconn)
	}()
	go func() {
		defer wg.Done()
		io.Copy(sconn, conn)
	}()
	wg.Wait()
}

// CONNECT 隧道的生命周期由客户端决定，必须等它退出再拆测试环境；
// 否则关掉 httptest.Server 时上面的 io.Copy 还在跑，容易报 use of closed connection。
func (h *testHTTPProxyHandler) Wait() {
	h.handlerDone.Wait()
}

// 证书里的主机名是 example.com，配合 InsecureSkipVerify 和 hostname verification
// 两种客户端配置，就能分别测出“信任但跳过校验”与“严格校验失败”两条路径。
func httpsServerInvalidHostname(t *testing.T) serverFunc {
	return func(h http.Handler) *httptest.Server {
		cert, err := tls.X509KeyPair(exampleCert, exampleKey)
		if err != nil {
			t.Errorf("https (invalid hostname): proxy_test: %v", err)
		}
		ts := httptest.NewUnstartedServer(h)
		ts.TLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		ts.StartTLS()
		return ts
	}
}

// 用同时覆盖 127.0.0.1/::1 的证书，让走 IP 的 httptest.Server 也能通过主机名校验；
// 和 invalidHostname 版本一对，专门验证 RootCAs 配置生效的情况。
func httpsServerValidHostname(t *testing.T) serverFunc {
	return func(h http.Handler) *httptest.Server {
		cert, err := tls.X509KeyPair(localhostCert, localhostKey)
		if err != nil {
			t.Errorf("https (valid hostname): proxy_test: %v", err)
		}
		ts := httptest.NewUnstartedServer(h)
		ts.TLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		ts.StartTLS()
		return ts
	}
}

// 把测试证书做成 RootCAs 传给客户端，这样 TLS 校验走的是显式信任链，
// 既不依赖系统证书库，也不会受运行环境影响。
func localhostCertPool(t *testing.T) *x509.CertPool {
	localhostPool := x509.NewCertPool()

	if !localhostPool.AppendCertsFromPEM(localhostCert) {
		t.Errorf("error setting up localhostCert pool")
	}
	return localhostPool
}

// be sure to unset environment variable https_proxy (if exported) before testing, otherwise the testing will fail unexpectedly.
// 表驱动覆盖直连、HTTP/HTTPS 代理、代理链路里任意一跳是 HTTPS 的组合，
// 重点验证 InsecureSkipVerify/RootCAs 与代理认证的交互；下面还会回写一条消息确认流可用。
func TestRoundTripAndNewConnection(t *testing.T) {
	localhostPool := localhostCertPool(t)

	testCases := map[string]struct {
		serverFunc             func(http.Handler) *httptest.Server
		proxyServerFunc        func(http.Handler) *httptest.Server
		proxyAuth              *url.Userinfo
		clientTLS              *tls.Config
		serverConnectionHeader string
		serverUpgradeHeader    string
		serverStatusCode       int
		shouldError            bool
	}{
		"no headers": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "",
			serverUpgradeHeader:    "",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true,
		},
		"no upgrade header": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true,
		},
		"no connection header": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true,
		},
		"no switching protocol status code": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusForbidden,
			shouldError:            true,
		},
		"http": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"https (invalid hostname + InsecureSkipVerify)": {
			serverFunc:             httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"https (invalid hostname + hostname verification)": {
			serverFunc:             httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: false},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true,
		},
		"https (valid hostname + RootCAs)": {
			serverFunc:             httpsServerValidHostname(t),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied http->http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httptest.NewServer,
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https (invalid hostname + InsecureSkipVerify) -> http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https with auth (invalid hostname + InsecureSkipVerify) -> http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httpsServerInvalidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https (invalid hostname + hostname verification) -> http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: false},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true, // fails because the client doesn't trust the proxy
		},
		"proxied https (valid hostname + RootCAs) -> http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httpsServerValidHostname(t),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https with auth (valid hostname + RootCAs) -> http": {
			serverFunc:             httptest.NewServer,
			proxyServerFunc:        httpsServerValidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https (invalid hostname + InsecureSkipVerify) -> https (invalid hostname)": {
			serverFunc:             httpsServerInvalidHostname(t),
			proxyServerFunc:        httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false, // works because the test proxy ignores TLS errors
		},
		"proxied https with auth (invalid hostname + InsecureSkipVerify) -> https (invalid hostname)": {
			serverFunc:             httpsServerInvalidHostname(t),
			proxyServerFunc:        httpsServerInvalidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false, // works because the test proxy ignores TLS errors
		},
		"proxied https (invalid hostname + hostname verification) -> https (invalid hostname)": {
			serverFunc:             httpsServerInvalidHostname(t),
			proxyServerFunc:        httpsServerInvalidHostname(t),
			clientTLS:              &tls.Config{InsecureSkipVerify: false},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true, // fails because the client doesn't trust the proxy
		},
		"proxied https (valid hostname + RootCAs) -> https (valid hostname + RootCAs)": {
			serverFunc:             httpsServerValidHostname(t),
			proxyServerFunc:        httpsServerValidHostname(t),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied https with auth (valid hostname + RootCAs) -> https (valid hostname + RootCAs)": {
			serverFunc:             httpsServerValidHostname(t),
			proxyServerFunc:        httpsServerValidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied valid https, proxy auth with chars that percent escape -> valid https": {
			serverFunc:             httpsServerValidHostname(t),
			proxyServerFunc:        httpsServerValidHostname(t),
			proxyAuth:              url.UserPassword("proxy user", "proxypasswd%"),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
	}

	for k, testCase := range testCases {
		t.Run(k, func(t *testing.T) {
			// 每个子用例都起自己的 server/proxy，避免并行时共享监听端口和状态。
			server := testCase.serverFunc(serverHandler(
				t, serverHandlerConfig{
					shouldError:      testCase.shouldError,
					statusCode:       testCase.serverStatusCode,
					connectionHeader: testCase.serverConnectionHeader,
					upgradeHeader:    testCase.serverUpgradeHeader,
				},
			))
			defer server.Close()
			t.Logf("Server URL: %v", server.URL)

			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("error creating request: %s", err)
			}
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("error creating request: %s", err)
			}

			spdyTransport, err := NewRoundTripper(testCase.clientTLS)
			if err != nil {
				t.Fatalf("error creating SpdyRoundTripper: %v", err)
			}

			var proxierCalled bool
			var proxyCalledWithHost string
			var proxyCalledWithAuth bool
			var proxyCalledWithAuthHeader string
			// 不依赖环境变量里的代理设置，直接把 transport 的 proxier 替换成测试专用的，
			// 顺带记录它看到的 host 和认证头，用于后面的断言。
			if testCase.proxyServerFunc != nil {
				proxyHandler := newTestHTTPProxyHandler(t, func(req *http.Request) bool {
					proxyCalledWithHost = req.Host

					proxyAuthHeaderName := "Proxy-Authorization"
					_, proxyCalledWithAuth = req.Header[proxyAuthHeaderName]
					proxyCalledWithAuthHeader = req.Header.Get(proxyAuthHeaderName)
					return true
				})
				defer proxyHandler.Wait()

				proxy := testCase.proxyServerFunc(proxyHandler)
				defer proxy.Close()

				t.Logf("Proxy URL: %v", proxy.URL)

				spdyTransport.proxier = func(proxierReq *http.Request) (*url.URL, error) {
					proxierCalled = true
					proxyURL, err := url.Parse(proxy.URL)
					if err != nil {
						return nil, err
					}
					proxyURL.User = testCase.proxyAuth
					return proxyURL, nil
				}
			}

			client := &http.Client{Transport: spdyTransport}

			resp, err := client.Do(req)
			var conn httpstream.Connection
			// 只有 Do 成功才继续建连：升级响应由 NewConnection 二次校验，
			// 失败用例里 err 可能出现在两个阶段中的任意一个。
			if err == nil {
				conn, err = spdyTransport.NewConnection(resp)
			}
			haveErr := err != nil
			if e, a := testCase.shouldError, haveErr; e != a {
				t.Fatalf("shouldError=%t, got %t: %v", e, a, err)
			}
			if testCase.shouldError {
				return
			}
			defer conn.Close()

			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("expected http 101 switching protocols, got %d", resp.StatusCode)
			}

			stream, err := conn.CreateStream(http.Header{})
			if err != nil {
				t.Fatalf("error creating client stream: %s", err)
			}

			// 回显一轮 “hello”，同时验证流的读写两端都通了，
			// 而不只是 HTTP 头看起来像一次成功的升级。
			n, err := stream.Write([]byte("hello"))
			if err != nil {
				t.Fatalf("error writing to stream: %s", err)
			}
			if n != 5 {
				t.Fatalf("expected to write 5 bytes, but actually wrote %d", n)
			}

			b := make([]byte, 5)
			n, err = stream.Read(b)
			if err != nil {
				t.Fatalf("error reading from stream: %s", err)
			}
			if n != 5 {
				t.Fatalf("expected to read 5 bytes, but actually read %d", n)
			}
			if e, a := "hello", string(b[0:n]); e != a {
				t.Fatalf("expected '%s', got '%s'", e, a)
			}

			if testCase.proxyServerFunc != nil {
				if !proxierCalled {
					t.Fatal("expected to use a proxy but proxier in SpdyRoundTripper wasn't called")
				}
				if proxyCalledWithHost != serverURL.Host {
					t.Fatalf("expected to see a call to the proxy for backend %q, got %q", serverURL.Host, proxyCalledWithHost)
				}
			}

			if testCase.proxyAuth != nil {
				expectedUsername := testCase.proxyAuth.Username()
				expectedPassword, _ := testCase.proxyAuth.Password()
				username, password, ok := (&http.Request{Header: http.Header{"Authorization": []string{proxyCalledWithAuthHeader}}}).BasicAuth()
				if !ok {
					t.Fatalf("invalid proxy auth header %s", proxyCalledWithAuthHeader)
				}
				if username != expectedUsername || password != expectedPassword {
					t.Fatalf("expected proxy auth \"%s:%s\", got \"%s:%s\"", expectedUsername, expectedPassword, username, password)
				}
			} else if proxyCalledWithAuth {
				// 没配代理认证就不该凭空多出 Proxy-Authorization 头。
				t.Fatalf("proxy authorization unexpected, got %q", proxyCalledWithAuthHeader)
			}
		})
	}
}

// Tests SpdyRoundTripper constructors
// 专测配置项之间的互斥关系：UpgradeTransport 与 TLS/Proxier 不能同时给，
// 同时确认构造函数返回的 TLS 配置符合预期、不会被悄悄改写。
func TestRoundTripConstuctor(t *testing.T) {
	testCases := map[string]struct {
		tlsConfig         *tls.Config
		proxier           func(req *http.Request) (*url.URL, error)
		upgradeTransport  http.RoundTripper
		expectedTLSConfig *tls.Config
		errMsg            string
	}{
		"Basic TLSConfig; no error": {
			tlsConfig:         &tls.Config{InsecureSkipVerify: true},
			expectedTLSConfig: &tls.Config{InsecureSkipVerify: true},
			upgradeTransport:  nil,
		},
		"Basic TLSConfig and Proxier: no error": {
			tlsConfig:         &tls.Config{InsecureSkipVerify: true},
			proxier:           func(req *http.Request) (*url.URL, error) { return nil, nil },
			expectedTLSConfig: &tls.Config{InsecureSkipVerify: true},
			upgradeTransport:  nil,
		},
		"TLSConfig with UpgradeTransport: error": {
			tlsConfig:         &tls.Config{InsecureSkipVerify: true},
			upgradeTransport:  &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			expectedTLSConfig: &tls.Config{InsecureSkipVerify: true},
			errMsg:            "SpdyRoundTripper: UpgradeTransport is mutually exclusive to TLSConfig or Proxier",
		},
		"Proxier with UpgradeTransport: error": {
			proxier:           func(req *http.Request) (*url.URL, error) { return nil, nil },
			upgradeTransport:  &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			expectedTLSConfig: &tls.Config{InsecureSkipVerify: true},
			errMsg:            "SpdyRoundTripper: UpgradeTransport is mutually exclusive to TLSConfig or Proxier",
		},
		"Only UpgradeTransport: no error": {
			upgradeTransport:  &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			expectedTLSConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			spdyRoundTripper, err := NewRoundTripperWithConfig(
				RoundTripperConfig{
					TLS:              testCase.tlsConfig,
					Proxier:          testCase.proxier,
					UpgradeTransport: testCase.upgradeTransport,
				},
			)
			if testCase.errMsg != "" {
				if err == nil {
					t.Fatalf("expected error but received none")
				}
				if !strings.Contains(err.Error(), testCase.errMsg) {
					t.Fatalf("expected error message (%s), got (%s)", err.Error(), testCase.errMsg)
				}
			}
			if testCase.errMsg == "" {
				if err != nil {
					t.Fatalf("unexpected error received: %v", err)
				}
				actualTLSConfig := spdyRoundTripper.TLSClientConfig()
				if !reflect.DeepEqual(testCase.expectedTLSConfig, actualTLSConfig) {
					t.Errorf("expected TLSConfig (%v), got (%v)",
						testCase.expectedTLSConfig, actualTLSConfig)
				}
			}
		})
	}
}

// NO_PROXY 支持 CIDR 是 Kubernetes 环境的现实需求（Pod/Service 网段），
// 标准库的 httpproxy 只认域名和 IP；这个测试确认 CIDR 内的目标绕过代理、其余仍走 delegate。
// 同时把小写 no_proxy 清空，避免系统上已有的小写值干扰判定。
func TestProxierWithNoProxyCIDR(t *testing.T) {
	t.Setenv("NO_PROXY", "10.0.0.0/8,2001:db8::/32")
	t.Setenv("no_proxy", "")

	expectedProxy, err := url.Parse("http://proxy.example:8080")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	delegateCalls := 0
	delegate := func(*http.Request) (*url.URL, error) {
		delegateCalls++
		return expectedProxy, nil
	}

	// delegateCalls 同时用来证明命中 CIDR 时根本没有走到标准库的代理解析，
	// 而不只是返回了 nil 结果。
	proxier := newProxierWithNoProxyCIDR(delegate)

	req, err := http.NewRequest(http.MethodGet, "https://10.1.2.3", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proxyURL, err := proxier(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if proxyURL != nil {
		t.Fatalf("expected nil proxyURL, got %v", proxyURL)
	}
	if delegateCalls != 0 {
		t.Fatalf("expected delegateCalls=0, got %d", delegateCalls)
	}

	req, err = http.NewRequest(http.MethodGet, "https://[2001:db8::1]", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proxyURL, err = proxier(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if proxyURL != nil {
		t.Fatalf("expected nil proxyURL, got %v", proxyURL)
	}
	if delegateCalls != 0 {
		t.Fatalf("expected delegateCalls=0, got %d", delegateCalls)
	}

	req, err = http.NewRequest(http.MethodGet, "https://192.168.1.10", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proxyURL, err = proxier(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if proxyURL == nil {
		t.Fatalf("expected non-nil proxyURL")
	}
	if proxyURL.String() != expectedProxy.String() {
		t.Fatalf("expected proxyURL %q, got %q", expectedProxy.String(), proxyURL.String())
	}
	if delegateCalls != 1 {
		t.Fatalf("expected delegateCalls=1, got %d", delegateCalls)
	}

	req, err = http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proxyURL, err = proxier(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if proxyURL == nil {
		t.Fatalf("expected non-nil proxyURL")
	}
	if proxyURL.String() != expectedProxy.String() {
		t.Fatalf("expected proxyURL %q, got %q", expectedProxy.String(), proxyURL.String())
	}
	if delegateCalls != 2 {
		t.Fatalf("expected delegateCalls=2, got %d", delegateCalls)
	}
}

// 借 SOCKS5 的 Rewriter 钩子把客户端实际使用的认证方式和目标地址抠出来，
// 让测试能断言 SpdyRoundTripper 确实走了代理、且把认证信息传到了 SOCKS5 层。
type Interceptor struct {
	Authorization       socks5.AuthContext
	proxyCalledWithHost *string
}

// 返回的是 SOCKS5 协议里的 method 编号和用户名密码等 payload，
// 测试据此区分“无认证”(0) 和“用户名密码认证”(2)。
func (i *Interceptor) GetAuthContext() (int, map[string]string) {
	return int(i.Authorization.Method), i.Authorization.Payload
}

// Rewrite 不改写目标地址，只记录它；返回原 DestAddr 保证连接仍按客户端请求转发。
func (i *Interceptor) Rewrite(ctx context.Context, req *socks5.Request) (context.Context, *socks5.AddrSpec) {
	*i.proxyCalledWithHost = req.DestAddr.Address()
	i.Authorization = socks5.AuthContext(*req.AuthContext)
	return ctx, req.DestAddr
}

// be sure to unset environment variable https_proxy (if exported) before testing, otherwise the testing will fail unexpectedly.
// 与 HTTP 代理测试对称，这里专门覆盖 socks5:// 代理：认证有无、目标 HTTP/HTTPS 的组合，
// 并通过 Interceptor 校验 SOCKS5 握手阶段真正收到的 method 与目标地址。
func TestRoundTripSocks5AndNewConnection(t *testing.T) {
	localhostPool := localhostCertPool(t)

	// creds 为 nil 表示不需要认证；非 nil 时构造 UserPassAuthenticator，
	// 这样同一个工厂就能覆盖代理要求认证和客户端提供认证两类场景。
	socks5Server := func(creds *socks5.StaticCredentials, interceptor *Interceptor) *socks5.Server {
		var conf *socks5.Config
		if creds != nil {
			authenticator := socks5.UserPassAuthenticator{Credentials: creds}
			conf = &socks5.Config{
				AuthMethods: []socks5.Authenticator{authenticator},
				Rewriter:    interceptor,
			}
		} else {
			conf = &socks5.Config{Rewriter: interceptor}
		}

		ts, err := socks5.New(conf)
		if err != nil {
			t.Errorf("failed to create sock5 server: %v", err)
		}
		return ts
	}

	testCases := map[string]struct {
		clientTLS              *tls.Config
		proxyAuth              *url.Userinfo
		serverConnectionHeader string
		serverFunc             serverFunc
		serverStatusCode       int
		serverUpgradeHeader    string
		shouldError            bool
	}{
		"proxied without auth -> http": {
			serverFunc:             httptest.NewServer,
			serverConnectionHeader: "Upgrade",
			serverStatusCode:       http.StatusSwitchingProtocols,
			serverUpgradeHeader:    "SPDY/3.1",
			shouldError:            false,
		},
		"proxied with invalid auth -> http": {
			serverFunc:             httptest.NewServer,
			proxyAuth:              url.UserPassword("invalid", "auth"),
			serverConnectionHeader: "Upgrade",
			serverStatusCode:       http.StatusSwitchingProtocols,
			serverUpgradeHeader:    "SPDY/3.1",
			shouldError:            true,
		},
		"proxied with valid auth -> http": {
			serverFunc:             httptest.NewServer,
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			serverConnectionHeader: "Upgrade",
			serverStatusCode:       http.StatusSwitchingProtocols,
			serverUpgradeHeader:    "SPDY/3.1",
			shouldError:            false,
		},
		"proxied with valid auth -> https (invalid hostname + InsecureSkipVerify)": {
			serverFunc:             httpsServerInvalidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{InsecureSkipVerify: true},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
		"proxied with valid auth -> https (invalid hostname + hostname verification)": {
			serverFunc:             httpsServerInvalidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{InsecureSkipVerify: false},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            true,
		},
		"proxied with valid auth -> https (valid hostname + RootCAs)": {
			serverFunc:             httpsServerValidHostname(t),
			proxyAuth:              url.UserPassword("proxyuser", "proxypasswd"),
			clientTLS:              &tls.Config{RootCAs: localhostPool},
			serverConnectionHeader: "Upgrade",
			serverUpgradeHeader:    "SPDY/3.1",
			serverStatusCode:       http.StatusSwitchingProtocols,
			shouldError:            false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// 每个用例自建 server 与 SOCKS5 监听，避免共享端口和状态串味。
			server := testCase.serverFunc(serverHandler(
				t, serverHandlerConfig{
					shouldError:      testCase.shouldError,
					statusCode:       testCase.serverStatusCode,
					connectionHeader: testCase.serverConnectionHeader,
					upgradeHeader:    testCase.serverUpgradeHeader,
				},
			))
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("error creating request: %s", err)
			}

			spdyTransport, err := NewRoundTripper(testCase.clientTLS)
			if err != nil {
				t.Fatalf("error creating SpdyRoundTripper: %v", err)
			}
			var proxierCalled bool
			var proxyCalledWithHost string

			interceptor := &Interceptor{proxyCalledWithHost: &proxyCalledWithHost}

			proxyHandler := socks5Server(nil, interceptor)

			if testCase.proxyAuth != nil {
				proxyHandler = socks5Server(&socks5.StaticCredentials{
					"proxyuser": "proxypasswd", // Socks5 server static credentials when client authentication is expected
				}, interceptor)
			}

			// closed 通道区分“测试正常结束导致的连接关闭”和真正的 ServeConn 错误，
			// 避免已结束的子用例还在 t.Errorf。
			closed := make(chan struct{})
			isClosed := func() bool {
				select {
				case <-closed:
					return true
				default:
					return false
				}
			}

			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("socks5Server: proxy_test: Listen: %v", err)
			}
			defer l.Close()

			// 只接受一条连接就够了：每个子用例只发一次请求，
			// SOCKS5 服务器本身会在 ServeConn 里处理后续复用。
			go func(shoulderror bool) {
				conn, err := l.Accept()
				if err != nil {
					if isClosed() {
						return
					}

					t.Errorf("error accepting connection: %s", err)
				}

				if err := proxyHandler.ServeConn(conn); err != nil && !shoulderror {
					// If the connection request is closed before the channel is closed
					// the test will fail with a ServeConn error. Since the test only return
					// early if expects shouldError=true, the channel is closed at the end of
					// the test, just before all the deferred connections Close() are executed.
					if isClosed() {
						return
					}

					t.Errorf("ServeConn error: %s", err)
				}
			}(testCase.shouldError)
			// 直接把 proxier 指向本地 SOCKS5 监听，不依赖环境变量，
			// 并把代理 URL 的用户信息填上客户端提供的认证数据。
			spdyTransport.proxier = func(proxierReq *http.Request) (*url.URL, error) {
				proxierCalled = true
				return &url.URL{
					Scheme: "socks5",
					Host:   net.JoinHostPort("127.0.0.1", strconv.Itoa(l.Addr().(*net.TCPAddr).Port)),
					User:   testCase.proxyAuth,
				}, nil
			}

			client := &http.Client{Transport: spdyTransport}

			resp, err := client.Do(req)
			haveErr := err != nil
			if e, a := testCase.shouldError, haveErr; e != a {
				t.Fatalf("shouldError=%t, got %t: %v", e, a, err)
			}
			if testCase.shouldError {
				return
			}

			conn, err := spdyTransport.NewConnection(resp)
			haveErr = err != nil
			if e, a := testCase.shouldError, haveErr; e != a {
				t.Fatalf("shouldError=%t, got %t: %v", e, a, err)
			}
			if testCase.shouldError {
				return
			}

			defer conn.Close()

			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("expected http 101 switching protocols, got %d", resp.StatusCode)
			}

			stream, err := conn.CreateStream(http.Header{})
			if err != nil {
				t.Fatalf("error creating client stream: %s", err)
			}

			// 回显一轮 “hello”，同时验证流的读写两端都通了，
			// 而不只是 HTTP 头看起来像一次成功的升级。
			n, err := stream.Write([]byte("hello"))
			if err != nil {
				t.Fatalf("error writing to stream: %s", err)
			}
			if n != 5 {
				t.Fatalf("expected to write 5 bytes, but actually wrote %d", n)
			}

			b := make([]byte, 5)
			n, err = stream.Read(b)
			if err != nil {
				t.Fatalf("error reading from stream: %s", err)
			}
			if n != 5 {
				t.Fatalf("expected to read 5 bytes, but actually read %d", n)
			}
			if e, a := "hello", string(b[0:n]); e != a {
				t.Fatalf("expected '%s', got '%s'", e, a)
			}

			if !proxierCalled {
				t.Fatal("xpected to use a proxy but proxier in SpdyRoundTripper wasn't called")
			}

			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("error creating request: %s", err)
			}
			if proxyCalledWithHost != serverURL.Host {
				t.Fatalf("expected to see a call to the proxy for backend %q, got %q", serverURL.Host, proxyCalledWithHost)
			}

			authMethod, authUser := interceptor.GetAuthContext()

			// 从 SOCKS5 层反向验证客户端确实把认证信息带上了：
			// 有配置时期望 method=2（UserPass）且用户名正确，无配置时 method=0 且无 payload。
			if testCase.proxyAuth != nil {
				expectedSocks5AuthMethod := 2
				expectedSocks5AuthUser := "proxyuser"

				if expectedSocks5AuthMethod != authMethod {
					t.Fatalf("socks5 Proxy authorization unexpected, got %d, expected %d", authMethod, expectedSocks5AuthMethod)
				}

				if expectedSocks5AuthUser != authUser["Username"] {
					t.Fatalf("socks5 Proxy authorization user unexpected, got %q, expected %q", authUser["Username"], expectedSocks5AuthUser)
				}
			} else {
				if authMethod != 0 {
					t.Fatalf("proxy authentication method unexpected, got %d", authMethod)
				}
				if len(authUser) != 0 {
					t.Fatalf("unexpected proxy user: %v", authUser)
				}
			}

			// The channel must be closed before any of the connections are closed
			// 先通知 Accept/ServeConn 的 goroutine“这是正常收尾”，再让 defer 关连接，
			// 否则关闭本身会被当成 ServeConn 错误报出来。
			close(closed)
		})
	}
}

// 已取消的 context 必须在 Dial 阶段就被尊重，HTTP 和 HTTPS 两条路径都不能漏；
// 否则取消请求后底层 TCP 连接还会傻傻地等超时。
func TestRoundTripPassesContextToDialer(t *testing.T) {
	urls := []string{"http://127.0.0.1:1233/", "https://127.0.0.1:1233/"}
	for _, u := range urls {
		t.Run(u, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			spdyTransport, err := NewRoundTripper(&tls.Config{})
			if err != nil {
				t.Fatalf("error creating SpdyRoundTripper: %v", err)
			}
			_, err = spdyTransport.Dial(req)
			if err == nil || err.Error() != "dial tcp 127.0.0.1:1233: operation was canceled" {
				t.Errorf("expected error %q, got %v", "dial tcp 127.0.0.1:1233: operation was canceled", err)
			}
		})
	}
}

// exampleCert was generated from crypto/tls/generate_cert.go with the following command:
//
//	go run generate_cert.go  --rsa-bits 2048 --host example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var exampleCert = []byte(`-----BEGIN CERTIFICATE-----
MIIDADCCAeigAwIBAgIQVHG3Fn9SdWayyLOZKCW1vzANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMCAXDTcwMDEwMTAwMDAwMFoYDzIwODQwMTI5MTYw
MDAwWjASMRAwDgYDVQQKEwdBY21lIENvMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEArTCu9fiIclNgDdWHphewM+JW55dCb5yYGlJgCBvwbOx547M9p+tn
zm9QOhsdZDHDZsG9tqnWxE2Nc1HpIJyOlfYsOoonpEoG/Ep6nnK91ngj0bn/JlNy
+i/bwU4r97MOukvnOIQez9/D9jAJaOX2+b8/d4lRz9BsqiwJyg+ynZ5tVVYj7aMi
vXnd6HOnJmtqutOtr3beucJnkd6XbwRkLUcAYATT+ZihOWRbTuKqhCg6zGkJOoUG
f8sX61JjoilxiURA//ftGVbdTCU3DrmGmardp5NNOHbumMYU8Vhmqgx1Bqxb+9he
7G42uW5YWYK/GqJzgVPjjlB2dOGj9KrEWQIDAQABo1AwTjAOBgNVHQ8BAf8EBAMC
AqQwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUwAwEB/zAWBgNVHREE
DzANggtleGFtcGxlLmNvbTANBgkqhkiG9w0BAQsFAAOCAQEAig4AIi9xWs1+pLES
eeGGdSDoclplFpcbXANnsYYFyLf+8pcWgVi2bOmb2gXMbHFkB07MA82wRJAUTaA+
2iNXVQMhPCoA7J6ADUbww9doJX2S9HGyArhiV/MhHtE8txzMn2EKNLdhhk3N9rmV
x/qRbWAY1U2z4BpdrAR87Fe81Nlj7h45csW9K+eS+NgXipiNTIfEShKgCFM8EdxL
1WXg7r9AvYV3TNDPWTjLsm1rQzzZQ7Uvcf6deWiNodZd8MOT/BFLclDPTK6cF2Hr
UU4dq6G4kCwMSxWE4cM3HlZ4u1dyIt47VbkP0rtvkBCXx36y+NXYA5lzntchNFZP
uvEQdw==
-----END CERTIFICATE-----`)

var exampleKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEArTCu9fiIclNgDdWHphewM+JW55dCb5yYGlJgCBvwbOx547M9
p+tnzm9QOhsdZDHDZsG9tqnWxE2Nc1HpIJyOlfYsOoonpEoG/Ep6nnK91ngj0bn/
JlNy+i/bwU4r97MOukvnOIQez9/D9jAJaOX2+b8/d4lRz9BsqiwJyg+ynZ5tVVYj
7aMivXnd6HOnJmtqutOtr3beucJnkd6XbwRkLUcAYATT+ZihOWRbTuKqhCg6zGkJ
OoUGf8sX61JjoilxiURA//ftGVbdTCU3DrmGmardp5NNOHbumMYU8Vhmqgx1Bqxb
+9he7G42uW5YWYK/GqJzgVPjjlB2dOGj9KrEWQIDAQABAoIBAQClt4CiYaaF5ltx
wVDjz6TNcJUBUs3CKE+uWAYFnF5Ii1nyU876Pxj8Aaz9fHZ6Kde0GkwiXY7gFOj1
YHo2tzcELSKS/SEDZcYbYFTGCjq13g1AH74R+SV6WZLn+5m8kPvVrM1ZWap188H5
bmuCkRDqVmIvShkbRW7EwhC35J9fiuW3majC/sjmsxtxyP6geWmu4f5/Ttqahcdb
osPZIgIIPzqAkNtkLTi7+meHYI9wlrGhL7XZTwnJ1Oc/Y67zzmbthLYB5YFSLUew
rXT58jtSjX4gbiQyheBSrWxW08QE4qYg6jJlAdffHhWv72hJW2MCXhuXp8gJs/Do
XLRHGwSBAoGBAMdNtsbe4yae/QeHUPGxNW0ipa0yoTF6i+VYoxvqiRMzDM3+3L8k
dgI1rr4330SivqDahMA/odWtM/9rVwJI2B2QhZLMHA0n9ytH007OO9TghgVB12nN
xosRYBpKdHXyyvV/MUZl7Jux6zKIzRDWOkF95VVYPcAaxJqd1E5/jJ6JAoGBAN51
QrebA1w/jfydeqQTz1sK01sbO4HYj4qGfo/JarVqGEkm1azeBBPPRnHz3jNKnCkM
S4PpqRDased3NIcViXlAgoqPqivZ8mQa/Rb146l7WaTErASHsZ023OGrxsr/Ed6N
P3GrmvxVJjebaFNaQ9sP80dLkpgeas0t2TY8iQNRAoGATOcnx8TpUVW3vNfx29DN
FLdxxkrq9/SZVn3FMlhlXAsuva3B799ZybB9JNjaRdmmRNsMrkHfaFvU3JHGmRMS
kRXa9LHdgRYSwZiNaLMbUyDvlce6HxFPswmZU4u3NGvi9KeHk+pwSgN1BaLTvdNr
1ymE/FF4QlAR3LdZ3JBK6kECgYEA0wW4/CJ31ZIURoW8SNjh4iMqy0nR8SJVR7q9
Y/hU2TKDRyEnoIwaohAFayNCrLUh3W5kVAXa8roB+OgDVAECH5sqOfZ+HorofD19
x8II7ESujLZj1whBXDkm3ovsT7QWZ17lyBZZNvQvBKDPHgKKS8udowv1S4fPGENd
wS07a4ECgYEAwLSbmMIVJme0jFjsp5d1wOGA2Qi2ZwGIAVlsbnJtygrU/hSBfnu8
VfyJSCgg3fPe7kChWKlfcOebVKSb68LKRsz1Lz1KdbY0HOJFp/cT4lKmDAlRY9gq
LB4rdf46lV0mUkvd2/oofIbTrzukjQSnyfLawb/2uJGV1IkTcZcn9CI=
-----END RSA PRIVATE KEY-----`)

// localhostCert was generated from crypto/tls/generate_cert.go with the following command:
//
//	go run generate_cert.go  --rsa-bits 2048 --host 127.0.0.1,::1,example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var localhostCert = []byte(`-----BEGIN CERTIFICATE-----
MIIDGTCCAgGgAwIBAgIRALL5AZcefF4kkYV1SEG6YrMwDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAgFw03MDAxMDEwMDAwMDBaGA8yMDg0MDEyOTE2
MDAwMFowEjEQMA4GA1UEChMHQWNtZSBDbzCCASIwDQYJKoZIhvcNAQEBBQADggEP
ADCCAQoCggEBALQ/FHcyVwdFHxARbbD2KBtDUT7Eni+8ioNdjtGcmtXqBv45EC1C
JOqqGJTroFGJ6Q9kQIZ9FqH5IJR2fOOJD9kOTueG4Vt1JY1rj1Kbpjefu8XleZ5L
SBwIWVnN/lEsEbuKmj7N2gLt5AH3zMZiBI1mg1u9Z5ZZHYbCiTpBrwsq6cTlvR9g
dyo1YkM5hRESCzsrL0aUByoo0qRMD8ZsgANJwgsiO0/M6idbxDwv1BnGwGmRYvOE
Hxpy3v0Jg7GJYrvnpnifJTs4nw91N5X9pXxR7FFzi/6HTYDWRljvTb0w6XciKYAz
bWZ0+cJr5F7wB7ovlbm7HrQIR7z7EIIu2d8CAwEAAaNoMGYwDgYDVR0PAQH/BAQD
AgKkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1UdEwEB/wQFMAMBAf8wLgYDVR0R
BCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAAAAAAAAAAAAAAAAEwDQYJKoZI
hvcNAQELBQADggEBAFPPWopNEJtIA2VFAQcqN6uJK+JVFOnjGRoCrM6Xgzdm0wxY
XCGjsxY5dl+V7KzdGqu858rCaq5osEBqypBpYAnS9C38VyCDA1vPS1PsN8SYv48z
DyBwj+7R2qar0ADBhnhWxvYO9M72lN/wuCqFKYMeFSnJdQLv3AsrrHe9lYqOa36s
8wxSwVTFTYXBzljPEnSaaJMPqFD8JXaZK1ryJPkO5OsCNQNGtatNiWAf3DcmwHAT
MGYMzP0u4nw47aRz9shB8w+taPKHx2BVwE1m/yp3nHVioOjXqA1fwRQVGclCJSH1
D2iq3hWVHRENgjTjANBPICLo9AZ4JfN6PH19mnU=
-----END CERTIFICATE-----`)

// localhostKey is the private key for localhostCert.
var localhostKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAtD8UdzJXB0UfEBFtsPYoG0NRPsSeL7yKg12O0Zya1eoG/jkQ
LUIk6qoYlOugUYnpD2RAhn0WofkglHZ844kP2Q5O54bhW3UljWuPUpumN5+7xeV5
nktIHAhZWc3+USwRu4qaPs3aAu3kAffMxmIEjWaDW71nllkdhsKJOkGvCyrpxOW9
H2B3KjViQzmFERILOysvRpQHKijSpEwPxmyAA0nCCyI7T8zqJ1vEPC/UGcbAaZFi
84QfGnLe/QmDsYliu+emeJ8lOzifD3U3lf2lfFHsUXOL/odNgNZGWO9NvTDpdyIp
gDNtZnT5wmvkXvAHui+VubsetAhHvPsQgi7Z3wIDAQABAoIBAGmw93IxjYCQ0ncc
kSKMJNZfsdtJdaxuNRZ0nNNirhQzR2h403iGaZlEpmdkhzxozsWcto1l+gh+SdFk
bTUK4MUZM8FlgO2dEqkLYh5BcMT7ICMZvSfJ4v21E5eqR68XVUqQKoQbNvQyxFk3
EddeEGdNrkb0GDK8DKlBlzAW5ep4gjG85wSTjR+J+muUv3R0BgLBFSuQnIDM/IMB
LWqsja/QbtB7yppe7jL5u8UCFdZG8BBKT9fcvFIu5PRLO3MO0uOI7LTc8+W1Xm23
uv+j3SY0+v+6POjK0UlJFFi/wkSPTFIfrQO1qFBkTDQHhQ6q/7GnILYYOiGbIRg2
NNuP52ECgYEAzXEoy50wSYh8xfFaBuxbm3ruuG2W49jgop7ZfoFrPWwOQKAZS441
VIwV4+e5IcA6KkuYbtGSdTYqK1SMkgnUyD/VevwAqH5TJoEIGu0pDuKGwVuwqioZ
frCIAV5GllKyUJ55VZNbRr2vY2fCsWbaCSCHETn6C16DNuTCe5C0JBECgYEA4JqY
5GpNbMG8fOt4H7hU0Fbm2yd6SHJcQ3/9iimef7xG6ajxsYrIhg1ft+3IPHMjVI0+
9brwHDnWg4bOOx/VO4VJBt6Dm/F33bndnZRkuIjfSNpLM51P+EnRdaFVHOJHwKqx
uF69kihifCAG7YATgCveeXImzBUSyZUz9UrETu8CgYARNBimdFNG1RcdvEg9rC0/
p9u1tfecvNySwZqU7WF9kz7eSonTueTdX521qAHowaAdSpdJMGODTTXaywm6cPhQ
jIfj9JZZhbqQzt1O4+08Qdvm9TamCUB5S28YLjza+bHU7nBaqixKkDfPqzCyilpX
yVGGL8SwjwmN3zop/sQXAQKBgC0JMsESQ6YcDsRpnrOVjYQc+LtW5iEitTdfsaID
iGGKihmOI7B66IxgoCHMTws39wycKdSyADVYr5e97xpR3rrJlgQHmBIrz+Iow7Q2
LiAGaec8xjl6QK/DdXmFuQBKqyKJ14rljFODP4QuE9WJid94bGqjpf3j99ltznZP
4J8HAoGAJb4eb4lu4UGwifDzqfAPzLGCoi0fE1/hSx34lfuLcc1G+LEu9YDKoOVJ
9suOh0b5K/bfEy9KrVMBBriduvdaERSD8S3pkIQaitIz0B029AbE4FLFf9lKQpP2
KR8NJEkK99Vh/tew6jAMll70xFrE7aF8VLXJVE7w4sQzuvHxl9Q=
-----END RSA PRIVATE KEY-----
`)
