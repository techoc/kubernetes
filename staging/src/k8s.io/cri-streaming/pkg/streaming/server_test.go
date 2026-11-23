/*
Copyright 2016 The Kubernetes Authors.

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

package streaming

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
)

// 测试用常量：testAddr 是测试监听地址，testContainerID 是容器 ID，testPodSandboxID 是 pod sandbox ID。
const (
	testAddr         = "localhost:12345"
	testContainerID  = "container789"
	testPodSandboxID = "pod0987"
)

// TestGetExec 验证 GetExec 在普通 HTTP、TLS 以及带基础路径前缀三种配置下都能生成正确的 exec URL，
// 并且 URL 中的 token 能在缓存中取回原始请求。
func TestGetExec(t *testing.T) {
	// 普通 HTTP 服务，BaseURL 应根据 Addr 自动生成。
	serv, err := NewServer(Config{
		Addr: testAddr,
	}, nil)
	assert.NoError(t, err)

	// 启用 TLS 的服务，生成的 URL 应使用 https scheme。
	tlsServer, err := NewServer(Config{
		Addr:      testAddr,
		TLSConfig: &tls.Config{},
	}, nil)
	assert.NoError(t, err)

	// 带基础路径前缀的服务，生成的 URL 应保留该前缀。
	const pathPrefix = "cri/shim"
	prefixServer, err := NewServer(Config{
		Addr: testAddr,
		BaseURL: &url.URL{
			Scheme: "http",
			Host:   testAddr,
			Path:   "/" + pathPrefix + "/",
		},
	}, nil)
	assert.NoError(t, err)

	// 辅助函数：用 URL 中解析出的 token 从缓存中取出请求，断言 token 有效且请求内容与预期一致。
	assertRequestToken := func(expectedReq *runtimeapi.ExecRequest, cache *requestCache, token string) {
		req, ok := cache.Consume(token)
		require.True(t, ok, "token %s not found!", token)
		assert.Equal(t, expectedReq, req)
	}
	// 构造一个开启了 tty 和 stdin 的 exec 请求作为输入。
	request := &runtimeapi.ExecRequest{
		ContainerId: testContainerID,
		Cmd:         []string{"echo", "foo"},
		Tty:         true,
		Stdin:       true,
	}
	{ // Non-TLS
		// 断言普通服务返回的 URL 以 http://<addr>/exec/ 开头，且尾部 token 可消费出原请求。
		resp, err := serv.GetExec(request)
		assert.NoError(t, err)
		expectedURL := "http://" + testAddr + "/exec/"
		assert.Contains(t, resp.Url, expectedURL)
		token := strings.TrimPrefix(resp.Url, expectedURL)
		assertRequestToken(request, serv.(*server).cache, token)
	}

	{ // TLS
		// 断言 TLS 服务生成的 URL 以 https:// 开头，token 同样有效。
		resp, err := tlsServer.GetExec(request)
		assert.NoError(t, err)
		expectedURL := "https://" + testAddr + "/exec/"
		assert.Contains(t, resp.Url, expectedURL)
		token := strings.TrimPrefix(resp.Url, expectedURL)
		assertRequestToken(request, tlsServer.(*server).cache, token)
	}

	{ // Path prefix
		// 断言带前缀的服务生成的 URL 包含 /cri/shim/exec/，token 可被正确消费。
		resp, err := prefixServer.GetExec(request)
		assert.NoError(t, err)
		expectedURL := "http://" + testAddr + "/" + pathPrefix + "/exec/"
		assert.Contains(t, resp.Url, expectedURL)
		token := strings.TrimPrefix(resp.Url, expectedURL)
		assertRequestToken(request, prefixServer.(*server).cache, token)
	}
}

// TestValidateExecAttachRequest 用表驱动方式验证 validateExecRequest 与 validateAttachRequest 的校验规则：
// 至少要开启一个标准流、tty 与 stderr 不能同时为真，合法配置应通过。
func TestValidateExecAttachRequest(t *testing.T) {
	// config 描述一组 tty 与三个标准流的开关组合，字段顺序为 tty、stdin、stdout、stderr。
	type config struct {
		tty    bool
		stdin  bool
		stdout bool
		stderr bool
	}
	// 每个用例包含描述、若干输入组合以及是否预期报错；对每组输入同时校验 exec 与 attach 两种请求。
	for _, tc := range []struct {
		desc      string
		configs   []config
		expectErr bool
	}{
		{
			desc:      "at least one stream must be true",
			expectErr: true,
			configs: []config{
				// 所有流都关闭，或只开 tty，都应被拒绝。
				{false, false, false, false},
				{true, false, false, false}},
		},
		{
			desc:      "tty and stderr cannot both be true",
			expectErr: true,
			configs: []config{
				// 以下组合都同时开启了 tty 与 stderr，应被拒绝。
				{true, false, false, true},
				{true, false, true, true},
				{true, true, false, true},
				{true, true, true, true},
			},
		},
		{
			desc:      "a valid config should pass",
			expectErr: false,
			configs: []config{
				// 以下为合法组合：未同时开启 tty 与 stderr，且至少开启一个标准流。
				{false, false, false, true},
				{false, false, true, false},
				{false, false, true, true},
				{false, true, false, false},
				{false, true, false, true},
				{false, true, true, false},
				{false, true, true, true},
				{true, false, true, false},
				{true, true, false, false},
				{true, true, true, false},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for _, c := range tc.configs {
				// validate the exec request.
				// 按当前组合构造 ExecRequest（带上合法容器 ID 和命令），断言校验结果符合预期。
				execReq := &runtimeapi.ExecRequest{
					ContainerId: testContainerID,
					Cmd:         []string{"date"},
					Tty:         c.tty,
					Stdin:       c.stdin,
					Stdout:      c.stdout,
					Stderr:      c.stderr,
				}
				err := validateExecRequest(execReq)
				assert.Equal(t, tc.expectErr, err != nil, "config: %v,  err: %v", c, err)

				// validate the attach request.
				// 用同样的组合构造 AttachRequest，断言其校验结果与 exec 完全一致。
				attachReq := &runtimeapi.AttachRequest{
					ContainerId: testContainerID,
					Tty:         c.tty,
					Stdin:       c.stdin,
					Stdout:      c.stdout,
					Stderr:      c.stderr,
				}
				err = validateAttachRequest(attachReq)
				assert.Equal(t, tc.expectErr, err != nil, "config: %v, err: %v", c, err)
			}
		})
	}
}

// TestGetAttach 验证 GetAttach 在普通 HTTP 与 TLS 配置下都能生成正确的 attach URL，且 token 可在缓存中换回原请求。
func TestGetAttach(t *testing.T) {
	// 普通 HTTP 服务。
	serv, err := NewServer(Config{
		Addr: testAddr,
	}, nil)
	require.NoError(t, err)

	// 启用 TLS 的服务，URL 应使用 https。
	tlsServer, err := NewServer(Config{
		Addr:      testAddr,
		TLSConfig: &tls.Config{},
	}, nil)
	require.NoError(t, err)

	// 辅助函数：消费 token 并断言取回的请求与传入的 AttachRequest 完全一致。
	assertRequestToken := func(expectedReq *runtimeapi.AttachRequest, cache *requestCache, token string) {
		req, ok := cache.Consume(token)
		require.True(t, ok, "token %s not found!", token)
		assert.Equal(t, expectedReq, req)
	}

	// 输入为开启 stdin 与 tty 的 attach 请求。
	request := &runtimeapi.AttachRequest{
		ContainerId: testContainerID,
		Stdin:       true,
		Tty:         true,
	}
	{ // Non-TLS
		// 断言 URL 前缀为 http://<addr>/attach/，且尾随 token 能取回原请求。
		resp, err := serv.GetAttach(request)
		assert.NoError(t, err)
		expectedURL := "http://" + testAddr + "/attach/"
		assert.Contains(t, resp.Url, expectedURL)
		token := strings.TrimPrefix(resp.Url, expectedURL)
		assertRequestToken(request, serv.(*server).cache, token)
	}

	{ // TLS
		// 断言 URL 前缀为 https://<addr>/attach/，token 同样有效。
		resp, err := tlsServer.GetAttach(request)
		assert.NoError(t, err)
		expectedURL := "https://" + testAddr + "/attach/"
		assert.Contains(t, resp.Url, expectedURL)
		token := strings.TrimPrefix(resp.Url, expectedURL)
		assertRequestToken(request, tlsServer.(*server).cache, token)
	}
}

// TestGetPortForward 验证 GetPortForward 在普通 HTTP 与 TLS 配置下生成的 URL 前缀正确，
// 且 token 对应的缓存请求中保留了传入的 pod sandbox ID。
func TestGetPortForward(t *testing.T) {
	podSandboxID := testPodSandboxID
	// 输入为带四个端口的端口转发请求。
	request := &runtimeapi.PortForwardRequest{
		PodSandboxId: podSandboxID,
		Port:         []int32{1, 2, 3, 4},
	}

	{ // Non-TLS
		// 创建普通服务，断言返回的 URL 以 http://<addr>/portforward/ 开头。
		serv, err := NewServer(Config{
			Addr: testAddr,
		}, nil)
		assert.NoError(t, err)
		resp, err := serv.GetPortForward(request)
		assert.NoError(t, err)
		expectedURL := "http://" + testAddr + "/portforward/"
		assert.True(t, strings.HasPrefix(resp.Url, expectedURL))
		// 从 URL 取出 token 并消费缓存，断言其中的 pod sandbox ID 与输入一致。
		token := strings.TrimPrefix(resp.Url, expectedURL)
		req, ok := serv.(*server).cache.Consume(token)
		require.True(t, ok, "token %s not found!", token)
		assert.Equal(t, testPodSandboxID, req.(*runtimeapi.PortForwardRequest).PodSandboxId)
	}

	{ // TLS
		// 创建 TLS 服务，断言 URL 使用 https，并通过 token 取回相同的 pod sandbox ID。
		tlsServer, err := NewServer(Config{
			Addr:      testAddr,
			TLSConfig: &tls.Config{},
		}, nil)
		assert.NoError(t, err)
		resp, err := tlsServer.GetPortForward(request)
		assert.NoError(t, err)
		expectedURL := "https://" + testAddr + "/portforward/"
		assert.True(t, strings.HasPrefix(resp.Url, expectedURL))
		token := strings.TrimPrefix(resp.Url, expectedURL)
		req, ok := tlsServer.(*server).cache.Consume(token)
		require.True(t, ok, "token %s not found!", token)
		assert.Equal(t, testPodSandboxID, req.(*runtimeapi.PortForwardRequest).PodSandboxId)
	}
}

// TestServeExec 通过公共的 runRemoteCommandTest 验证 exec 流式服务端的完整交互。
func TestServeExec(t *testing.T) {
	runRemoteCommandTest(t, "exec")
}

// TestServeAttach 通过公共的 runRemoteCommandTest 验证 attach 流式服务端的完整交互。
func TestServeAttach(t *testing.T) {
	runRemoteCommandTest(t, "attach")
}

// TestServePortForward 验证端口转发服务端的完整交互：先收到两个通道上的端口号，再回显写入的数据。
func TestServePortForward(t *testing.T) {
	// 启动测试服务，并确保结束后关闭。
	s, testServer := startTestServer(t)
	defer testServer.Close()

	// 构造端口转发请求并解析出返回的流式 URL。
	resp, err := s.GetPortForward(&runtimeapi.PortForwardRequest{
		PodSandboxId: testPodSandboxID,
		Port:         []int32{testPort},
	})
	require.NoError(t, err)
	reqURL, err := url.Parse(resp.Url)
	require.NoError(t, err)

	// 用 WebSocket 拨号连接该 URL，建立双向数据通道。
	wsConn := dialWebSocket(t, reqURL)
	defer wsConn.Close()

	// 服务端会先在 error 通道以外的数据通道上发送小端编码的端口号，这里断言通道 0 和 1 的内容。
	expectedPortBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(expectedPortBytes, uint16(testPort))
	requireWebSocketPayload(t, wsConn, 0, expectedPortBytes, false)
	requireWebSocketPayload(t, wsConn, 1, expectedPortBytes, false)

	// 向通道 0 写入输入数据，断言服务端原样回显（带 portforward 前缀）。
	writeWebSocketPayload(t, wsConn, 0, []byte("portforward"+testInput))
	requireWebSocketPayload(t, wsConn, 0, []byte("portforward"+testOutput), false)
}

// Run the remote command test.
// commandType is either "exec" or "attach".
// runRemoteCommandTest 对 exec 或 attach 执行端到端测试：创建请求、拨 WebSocket、校验三个通道的收发，并验证 token 的一次性语义。
func runRemoteCommandTest(t *testing.T, commandType string) {
	s, testServer := startTestServer(t)
	defer testServer.Close()

	// 根据命令类型构造对应的 CRI 请求，开启全部三个标准流，并解析返回的 URL。
	var reqURL *url.URL
	stdin, stdout, stderr := true, true, true
	containerID := testContainerID
	switch commandType {
	case "exec":
		resp, err := s.GetExec(&runtimeapi.ExecRequest{
			ContainerId: containerID,
			Cmd:         []string{"echo"},
			Stdin:       stdin,
			Stdout:      stdout,
			Stderr:      stderr,
		})
		require.NoError(t, err)
		reqURL, err = url.Parse(resp.Url)
		require.NoError(t, err)
	case "attach":
		resp, err := s.GetAttach(&runtimeapi.AttachRequest{
			ContainerId: containerID,
			Stdin:       stdin,
			Stdout:      stdout,
			Stderr:      stderr,
		})
		require.NoError(t, err)
		reqURL, err = url.Parse(resp.Url)
		require.NoError(t, err)
	}

	// 拨号并建立 WebSocket 连接。
	wsConn := dialWebSocket(t, reqURL)
	defer wsConn.Close()

	// 断言通道 2（stderr）先收到服务端输出的错误信息，再向通道 0 写入输入，并校验通道 1 的回显；
	// skipEmpty 为 true，因为服务端可能先发送一个空的 stdout 帧。
	requireWebSocketPayload(t, wsConn, 2, []byte(commandType+testErr), false)
	writeWebSocketPayload(t, wsConn, 0, []byte(commandType+testInput))
	requireWebSocketPayload(t, wsConn, 1, []byte(commandType+testOutput), true)

	// Repeat request with the same URL should be a 404.
	// token 只能消费一次，重复请求同一 URL 应返回 404。
	resp, err := http.Get(reqURL.String())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// startTestServer 创建一个把请求转发给 Server.ServeHTTP 的 httptest.Server，
// 并用其真实地址作为 BaseURL 构造带 fakeRuntime 的服务实例；返回的服务由调用方负责关闭测试服务器。
func startTestServer(t *testing.T) (Server, *httptest.Server) {
	var s Server
	// 用闭包延迟引用 s，使 httptest 服务器能将请求转交给稍后创建的服务实例。
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(w, r)
	}))
	cleanup := true
	// 若中途失败则自动关闭测试服务器；成功返回后由调用方关闭。
	defer func() {
		if cleanup {
			testServer.Close()
		}
	}()

	// 以测试服务器的真实 URL 作为 BaseURL，保证生成的流式 URL 指向测试服务。
	testURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	rt := newFakeRuntime(t)
	config := DefaultConfig
	config.BaseURL = testURL
	s, err = NewServer(config, rt)
	require.NoError(t, err)

	cleanup = false // Caller must close the test server.
	return s, testServer
}

// 流式交互中使用的固定测试数据：testInput 为客户端输入，testOutput 为服务端输出，
// testErr 为错误信息，testPort 为端口转发测试使用的端口号。
const (
	testInput  = "abcdefg"
	testOutput = "fooBARbaz"
	testErr    = "ERROR!!!"
	testPort   = 12345
)

// newFakeRuntime 创建一个绑定当前测试对象的 fakeRuntime，便于在运行时实现中做断言。
func newFakeRuntime(t *testing.T) *fakeRuntime {
	return &fakeRuntime{
		t: t,
	}
}

// fakeRuntime 是 Runtime 接口的测试替身，通过标准流收发带前缀的固定数据来验证服务端行为。
type fakeRuntime struct {
	// t 是绑定的测试对象，用于在运行时实现中做断言。
	t *testing.T
}

// Exec 断言传入的是测试容器 ID，并以 "exec" 为前缀完成三个标准流的收发。
func (f *fakeRuntime) Exec(_ context.Context, containerID string, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	assert.Equal(f.t, testContainerID, containerID)
	doServerStreams(f.t, "exec", stdin, stdout, stderr)
	return nil
}

// Attach 断言传入的是测试容器 ID，并以 "attach" 为前缀完成三个标准流的收发。
func (f *fakeRuntime) Attach(_ context.Context, containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	assert.Equal(f.t, testContainerID, containerID)
	doServerStreams(f.t, "attach", stdin, stdout, stderr)
	return nil
}

// PortForward 断言传入的是测试 sandbox ID 和端口，并以 "portforward" 为前缀在同一 stream 上完成收发。
func (f *fakeRuntime) PortForward(_ context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	assert.Equal(f.t, testPodSandboxID, podSandboxID)
	assert.EqualValues(f.t, testPort, port)
	doServerStreams(f.t, "portforward", stream, stream, nil)
	return nil
}

// Send & receive expected input/output. Must be the inverse of doClientStreams.
// Function will block until the expected i/o is finished.
// doServerStreams 模拟服务端：若提供 stderr 则先写错误信息，再读取客户端输入，最后写回输出；函数会阻塞直到收发完成。
func doServerStreams(t *testing.T, prefix string, stdin io.Reader, stdout, stderr io.Writer) {
	if stderr != nil {
		writeExpected(t, "server stderr", stderr, prefix+testErr)
	}
	readExpected(t, "server stdin", stdin, prefix+testInput)
	writeExpected(t, "server stdout", stdout, prefix+testOutput)
}

// Read and verify the expected string from the stream.
// readExpected 从流中读取恰好 len(expected) 个字节，断言读取成功且内容与预期字符串一致。
func readExpected(t *testing.T, streamName string, r io.Reader, expected string) {
	result := make([]byte, len(expected))
	_, err := io.ReadAtLeast(r, result, len(expected))
	assert.NoError(t, err, "stream %s", streamName)
	assert.Equal(t, expected, string(result), "stream %s", streamName)
}

// Write and verify success of the data over the stream.
// writeExpected 向流写入给定字符串，断言写入无错误且字节数等于字符串长度。
func writeExpected(t *testing.T, streamName string, w io.Writer, data string) {
	n, err := io.WriteString(w, data)
	assert.NoError(t, err, "stream %s", streamName)
	assert.Equal(t, len(data), n, "stream %s", streamName)
}

// dialWebSocket 把流式 URL 的 scheme 转换为 ws/wss，声明 v4.channel.k8s.io 子协议后建立 WebSocket 连接。
func dialWebSocket(t *testing.T, reqURL *url.URL) *websocket.Conn {
	t.Helper()

	wsURL := *reqURL
	switch wsURL.Scheme {
	case "http":
		wsURL.Scheme = "ws"
	case "https":
		wsURL.Scheme = "wss"
	default:
		t.Fatalf("unsupported URL scheme %q", wsURL.Scheme)
	}

	dialer := websocket.Dialer{
		Subprotocols: []string{"v4.channel.k8s.io"},
	}
	conn, _, err := dialer.Dial(wsURL.String(), nil)
	require.NoError(t, err)
	return conn
}

// writeWebSocketPayload 在 payload 前加上一个字节的通道号，组成 Kubernetes 通道帧后以二进制消息发送。
func writeWebSocketPayload(t *testing.T, conn *websocket.Conn, channel byte, payload []byte) {
	t.Helper()
	frame := make([]byte, len(payload)+1)
	frame[0] = channel
	copy(frame[1:], payload)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, frame))
}

// requireWebSocketPayload 不断读取 WebSocket 消息，跳过空帧和非目标通道的帧，
// 直到找到指定通道的帧并断言其 payload 与 expected 一致；skipEmpty 为 true 时会跳过空 payload。
func requireWebSocketPayload(t *testing.T, conn *websocket.Conn, channel byte, expected []byte, skipEmpty bool) {
	t.Helper()
	for {
		_, frame, err := conn.ReadMessage()
		require.NoError(t, err)
		if len(frame) == 0 {
			continue
		}
		if frame[0] != channel {
			continue
		}
		payload := frame[1:]
		if skipEmpty && len(payload) == 0 {
			continue
		}
		assert.Equal(t, expected, payload)
		return
	}
}
