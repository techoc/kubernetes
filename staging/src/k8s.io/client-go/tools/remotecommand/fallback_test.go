/*
Copyright 2023 The Kubernetes Authors.

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

package remotecommand

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnettesting "k8s.io/apimachinery/pkg/util/net/testing"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	utilexec "k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream"
)

// TestFallbackClient_WebSocketPrimarySucceeds 验证主执行器（WebSocket）成功时
// 不会触发回退，数据经 STDIN 写入、STDOUT 读回后保持一致。
func TestFallbackClient_WebSocketPrimarySucceeds(t *testing.T) {
	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		defer conns.conn.Close()
		// 将 STDIN 流回环到 STDOUT 流。
		_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建回退客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 STDIN 和 STDOUT 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	require.NoError(t, err)
	websocketExecutor, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	require.NoError(t, err)
	spdyExecutor, err := NewSPDYExecutor(&rest.Config{Host: websocketLocation.Host}, "POST", websocketLocation)
	require.NoError(t, err)
	// 永不回退，因此始终使用 websocketExecutor，它对接 websocket 服务端会成功。
	exec, err := NewFallbackExecutor(websocketExecutor, spdyExecutor, func(error) bool { return false })
	require.NoError(t, err)
	// 生成随机数据并设置为从 STDIN 流发送；数据将从 STDOUT 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		// 启动 WebSocket "exec" 客户端上的流式传输。
		errorChan <- exec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		if err != nil {
			t.Errorf("unexpected error")
		}
	}

	data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Errorf("error reading the stream: %v", err)
		return
	}
	// 检查 STDIN 发送的随机数据与 STDOUT 返回的数据一致。
	if !bytes.Equal(randomData, data) {
		t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
	}
}

// TestFallbackClient_SPDYSecondarySucceeds 验证主执行器不可用时按回调判定
// 回退到 SPDY，且数据完整性不受影响。
func TestFallbackClient_SPDYSecondarySucceeds(t *testing.T) {
	// 创建假 SPDY 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	spdyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var stdin, stdout bytes.Buffer
		ctx, err := createHTTPStreams(w, req, &StreamOptions{
			Stdin:  &stdin,
			Stdout: &stdout,
		})
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		defer ctx.conn.Close()
		_, err = io.Copy(ctx.stdoutStream, ctx.stdinStream)
		if err != nil {
			t.Fatalf("error copying STDIN to STDOUT: %v", err)
		}
	}))
	defer spdyServer.Close()

	spdyLocation, err := url.Parse(spdyServer.URL)
	require.NoError(t, err)
	websocketExecutor, err := NewWebSocketExecutor(&rest.Config{Host: spdyLocation.Host}, "GET", spdyServer.URL)
	require.NoError(t, err)
	spdyExecutor, err := NewSPDYExecutor(&rest.Config{Host: spdyLocation.Host}, "POST", spdyLocation)
	require.NoError(t, err)
	// 总是回退到 spdyExecutor，且 spdyExecutor 对接假 spdy 服务端会成功。
	exec, err := NewFallbackExecutor(websocketExecutor, spdyExecutor, func(error) bool { return true })
	require.NoError(t, err)
	// 生成随机数据并设置为从 STDIN 流发送；数据将从 STDOUT 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		errorChan <- exec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		if err != nil {
			t.Errorf("unexpected error")
		}
	}

	data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Errorf("error reading the stream: %v", err)
		return
	}
	// 检查 STDIN 发送的随机数据与 STDOUT 返回的数据一致。
	if !bytes.Equal(randomData, data) {
		t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
	}
}

// TestFallbackClient_PrimaryAndSecondaryFail 验证两个执行器都失败时向调用方
// 返回错误，而不是静默成功。将主执行器协议改成 V4 以模拟服务端不支持的版本。
func TestFallbackClient_PrimaryAndSecondaryFail(t *testing.T) {
	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		defer conns.conn.Close()
		// 将 STDIN 流回环到 STDOUT 流。
		_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建回退客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 STDIN 和 STDOUT 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	require.NoError(t, err)
	websocketExecutor, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	require.NoError(t, err)
	spdyExecutor, err := NewSPDYExecutor(&rest.Config{Host: websocketLocation.Host}, "POST", websocketLocation)
	require.NoError(t, err)
	// 总是回退到 spdyExecutor，但 spdyExecutor 对接 websocket 服务端会失败。
	exec, err := NewFallbackExecutor(websocketExecutor, spdyExecutor, func(error) bool { return true })
	require.NoError(t, err)
	// 将 websocket executor 改为请求不被支持的远程命令 v4。
	fallbackExec, ok := exec.(*FallbackExecutor)
	assert.True(t, ok, "error casting executor as FallbackExecutor")
	websocketExec, ok := fallbackExec.primary.(*wsStreamExecutor)
	assert.True(t, ok, "error casting executor as websocket executor")
	// 将尝试的子协议版本设为 V4；websocket 服务端只接受 V5。
	websocketExec.protocols = []string{remotecommand.StreamProtocolV4Name}

	// 生成随机数据并设置为从 STDIN 流发送；数据将从 STDOUT 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		errorChan <- exec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		// 确保次执行器返回了错误。
		require.Error(t, err)
	}
}

// TestFallbackClient_SPDYSecondaryNonZeroExitCode 验证回退后的 SPDY 路径仍能
// 把非零退出码解析为 utilexec.ExitError，并保留退出前的 stdout 输出。
func TestFallbackClient_SPDYSecondaryNonZeroExitCode(t *testing.T) {
	const expectedExitCode = 23
	const expectedStdout = "stdout-before-exit"

	// 创建假 SPDY 服务端：先写 stdout，再写一个 v4 状态错误。
	spdyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, err := createHTTPStreams(w, req, &StreamOptions{
			Stdin:  strings.NewReader("input"),
			Stdout: &bytes.Buffer{},
		})
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		defer ctx.conn.Close()

		if _, err := io.WriteString(ctx.stdoutStream, expectedStdout); err != nil {
			t.Fatalf("error writing stdout stream: %v", err)
		}

		statusErr := &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure,
			Reason: remotecommand.NonZeroExitCodeReason,
			Details: &metav1.StatusDetails{
				Causes: []metav1.StatusCause{
					{
						Type:    remotecommand.ExitCodeCauseType,
						Message: "23",
					},
				},
			},
			Message: "command terminated with non-zero exit code: 23",
		}}
		if err := ctx.writeStatus(statusErr); err != nil {
			t.Fatalf("error writing status stream: %v", err)
		}
	}))
	defer spdyServer.Close()

	spdyLocation, err := url.Parse(spdyServer.URL)
	require.NoError(t, err)
	// 主 websocket 执行器指向仅支持 SPDY 的端点，应当失败。
	websocketExecutor, err := NewWebSocketExecutor(&rest.Config{Host: spdyLocation.Host}, "GET", spdyServer.URL+"?stdin=true&stdout=true")
	require.NoError(t, err)
	spdyExecutor, err := NewSPDYExecutor(&rest.Config{Host: spdyLocation.Host}, "POST", spdyLocation)
	require.NoError(t, err)

	var sawPrimaryError atomic.Bool
	exec, err := NewFallbackExecutor(websocketExecutor, spdyExecutor, func(err error) bool {
		sawPrimaryError.Store(true)
		return true
	})
	require.NoError(t, err)

	var stdout bytes.Buffer
	err = exec.StreamWithContext(context.Background(), StreamOptions{
		Stdin:  strings.NewReader("input"),
		Stdout: &stdout,
	})
	require.Error(t, err)
	require.True(t, sawPrimaryError.Load(), "expected primary websocket path to fail and trigger fallback")

	var exitErr utilexec.ExitError
	require.ErrorAs(t, err, &exitErr, "expected ExitError from secondary SPDY path, got: %T: %v", err, err)
	require.Equal(t, expectedExitCode, exitErr.ExitStatus())
	require.Equal(t, expectedStdout, stdout.String())
}

// localhostCert 由 crypto/tls/generate_cert.go 通过以下命令生成（命令原样保留）：
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

// localhostKey 是 localhostCert 的私钥。
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

// 参见 (https://github.com/kubernetes/kubernetes/issues/126134)。
//
// TestFallbackClient_WebSocketHTTPSProxyNoFallback 验证经由 HTTPS 代理时
// WebSocket 升级失败不会被误判为可回退错误，也不会重复调用代理。
func TestFallbackClient_WebSocketHTTPSProxyNoFallback(t *testing.T) {
	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		t.Errorf("https (valid hostname): proxy_test: %v", err)
	}

	var proxyCalled atomic.Int64
	proxyHandler := utilnettesting.NewHTTPProxyHandler(t, func(req *http.Request) bool {
		proxyCalled.Add(1)
		return true
	})
	defer proxyHandler.Wait()

	proxyServer := httptest.NewUnstartedServer(proxyHandler)
	proxyServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	proxyServer.StartTLS()
	defer proxyServer.Close() //nolint:errcheck

	proxyLocation, err := url.Parse(proxyServer.URL)
	require.NoError(t, err)

	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		defer conns.conn.Close() //nolint:errcheck
		// 将 STDIN 流回环到 STDOUT 流。
		_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
		if err != nil {
			t.Fatalf("websocket copy error: %v", err)
		}
	}))
	defer websocketServer.Close() //nolint:errcheck

	// 创建 WebSocket 客户端（executor），指向 TLS 代理服务器；
	// 代理服务器会向假 websocket 服务端打开 websocket 连接。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	require.NoError(t, err)
	clientConfig := &rest.Config{
		Host:            websocketLocation.Host,
		TLSClientConfig: rest.TLSClientConfig{CAData: localhostCert},
		Proxy: func(req *http.Request) (*url.URL, error) {
			return proxyLocation, nil
		},
	}
	websocketExecutor, err := NewWebSocketExecutor(clientConfig, "GET", websocketServer.URL)
	require.NoError(t, err)
	emptyURL, _ := url.Parse("")
	spdyExecutor, err := NewSPDYExecutor(clientConfig, "POST", emptyURL)
	require.NoError(t, err)
	// 使用 websocket 时不回退到 spdyExecutor。
	sawHTTPSProxyError := false
	exec, err := NewFallbackExecutor(websocketExecutor, spdyExecutor, func(err error) bool {
		if httpstream.IsUpgradeFailure(err) {
			t.Errorf("saw upgrade failure: %v", err)
			return true
		}
		if httpstream.IsHTTPSProxyError(err) {
			sawHTTPSProxyError = true
			t.Logf("saw https proxy error: %v", err)
			return true
		}
		return false
	})
	require.NoError(t, err)

	// 生成随机数据并设置为从 STDIN 流发送；数据将从 STDOUT 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		errorChan <- exec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		if err != nil {
			t.Errorf("unexpected error")
		}
	}

	data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Errorf("error reading the stream: %v", err)
		return
	}
	// 检查 STDIN 发送的随机数据与 STDOUT 返回的数据一致。
	if !bytes.Equal(randomData, data) {
		t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
	}

	// 确保没有观察到 https 代理错误
	if sawHTTPSProxyError {
		t.Errorf("expected to *not* see https proxy error")
	}
	// 确保代理只被调用了一次
	if e, a := int64(1), proxyCalled.Load(); e != a {
		t.Errorf("expected %d proxy call, got %d", e, a)
	}
}
