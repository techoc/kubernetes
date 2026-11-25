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

package portforward

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gwebsocket "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	constants "k8s.io/apimachinery/pkg/util/portforward"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport/websocket"
	"k8s.io/klog/v2"
)

func init() {
	// 注册 klog 标准参数，避免测试中日志初始化报错。
	klog.InitFlags(nil)
}

// TestTunnelingConnection_ReadWriteClose 验证端到端：客户端通过 WebSocket 隧道承载 SPDY，
// 写入的数据能从服务端 SPDY 流中完整读出，且协商出的子协议正确。
func TestTunnelingConnection_ReadWriteClose(t *testing.T) {
	// streamChan 接收上游 SPDY 服务端创建的流。
	streamChan := make(chan httpstream.Stream)
	defer close(streamChan)
	stopServerChan := make(chan struct{})
	defer close(stopServerChan)
	// 创建隧道连接服务端，并接入模拟的上游 SPDY 服务端。
	tunnelingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var upgrader = gwebsocket.Upgrader{
			CheckOrigin:  func(r *http.Request) bool { return true },
			Subprotocols: []string{constants.WebsocketsSPDYTunnelingPortForwardV1},
		}
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		defer conn.Close() //nolint:errcheck
		if conn.Subprotocol() != constants.WebsocketsSPDYTunnelingPortForwardV1 {
			t.Errorf("Not acceptable agreement Subprotocol: %v", conn.Subprotocol())
			return
		}
		tunnelingConn := NewTunnelingConnectionWithLogger(klog.LoggerWithName(klog.Background(), "server"), conn)
		spdyConn, err := spdy.NewServerConnection(tunnelingConn, justQueueStream(streamChan))
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		defer spdyConn.Close() //nolint:errcheck
		<-stopServerChan
	}))
	defer tunnelingServer.Close()
	// 让客户端隧道连接拨号到隧道服务端。
	url, err := url.Parse(tunnelingServer.URL)
	require.NoError(t, err)
	//nolint:logcheck // Intentionally uses the old API.
	dialer, err := NewSPDYOverWebsocketDialer(url, &rest.Config{Host: url.Host})
	require.NoError(t, err)
	spdyClient, protocol, err := dialer.Dial(constants.PortForwardV1Name)
	require.NoError(t, err)
	assert.Equal(t, constants.PortForwardV1Name, protocol)
	defer spdyClient.Close() //nolint:errcheck
	// 创建 SPDY 客户端流，服务端对应流会进入 streamChan。
	// 从客户端流发送数据，再读取服务端流并验证数据是否经隧道完整传输。
	expected := "This is a test tunneling SPDY data through websockets."
	var actual []byte
	go func() {
		clientStream, err := spdyClient.CreateStream(http.Header{})
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		_, err = io.Copy(clientStream, strings.NewReader(expected))
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		clientStream.Close() //nolint:errcheck
	}()
	select {
	case serverStream := <-streamChan:
		actual, err = io.ReadAll(serverStream)
		require.NoError(t, err)
		defer serverStream.Close() //nolint:errcheck
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("timeout waiting for spdy stream to arrive on channel.")
	}
	assert.Equal(t, expected, string(actual), "error validating tunneled string")
}

// TestTunnelingConnection_LocalRemoteAddress 验证 TunnelingConnection 暴露的
// LocalAddr/RemoteAddr 是合法的 TCP 地址（net.Conn 语义要求）。
func TestTunnelingConnection_LocalRemoteAddress(t *testing.T) {
	stopServerChan := make(chan struct{})
	defer close(stopServerChan)
	tunnelingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var upgrader = gwebsocket.Upgrader{
			CheckOrigin:  func(r *http.Request) bool { return true },
			Subprotocols: []string{constants.WebsocketsSPDYTunnelingPortForwardV1},
		}
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		defer conn.Close() //nolint:errcheck
		if conn.Subprotocol() != constants.WebsocketsSPDYTunnelingPortForwardV1 {
			t.Errorf("Not acceptable agreement Subprotocol: %v", conn.Subprotocol())
		}
		<-stopServerChan
	}))
	defer tunnelingServer.Close()
	// 创建客户端隧道连接。
	url, err := url.Parse(tunnelingServer.URL)
	require.NoError(t, err)
	tConn, err := dialForTunnelingConnection(url)
	require.NoError(t, err, "error creating client tunneling connection")
	defer tConn.Close() //nolint:errcheck
	// 验证 LocalAddr 和 RemoteAddr。
	localAddr := tConn.LocalAddr()
	remoteAddr := tConn.RemoteAddr()
	assert.Equal(t, "tcp", localAddr.Network(), "tunneling connection must be TCP")
	assert.Equal(t, "tcp", remoteAddr.Network(), "tunneling connection must be TCP")
	_, err = net.ResolveTCPAddr("tcp", localAddr.String())
	assert.NoError(t, err, "tunneling connection local addr should parse")
	_, err = net.ResolveTCPAddr("tcp", remoteAddr.String())
	assert.NoError(t, err, "tunneling connection remote addr should parse")
}

// TestTunnelingConnection_ReadWriteDeadlines 验证 deadline 相关方法在 WebSocket 隧道上可用：
// 零值关闭 deadline，未来时间可正常设置。
func TestTunnelingConnection_ReadWriteDeadlines(t *testing.T) {
	stopServerChan := make(chan struct{})
	defer close(stopServerChan)
	tunnelingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var upgrader = gwebsocket.Upgrader{
			CheckOrigin:  func(r *http.Request) bool { return true },
			Subprotocols: []string{constants.WebsocketsSPDYTunnelingPortForwardV1},
		}
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		defer conn.Close() //nolint:errcheck
		if conn.Subprotocol() != constants.WebsocketsSPDYTunnelingPortForwardV1 {
			t.Errorf("Not acceptable agreement Subprotocol: %v", conn.Subprotocol())
			return
		}
		<-stopServerChan
	}))
	defer tunnelingServer.Close()
	// 创建客户端隧道连接。
	url, err := url.Parse(tunnelingServer.URL)
	require.NoError(t, err)
	tConn, err := dialForTunnelingConnection(url)
	require.NoError(t, err, "error creating client tunneling connection")
	defer tConn.Close() //nolint:errcheck
	// 验证读、写截止时间。
	err = tConn.SetReadDeadline(time.Time{})
	assert.NoError(t, err, "setting zero deadline should always succeed; turns off deadline")
	err = tConn.SetWriteDeadline(time.Time{})
	assert.NoError(t, err, "setting zero deadline should always succeed; turns off deadline")
	err = tConn.SetDeadline(time.Time{})
	assert.NoError(t, err, "setting zero deadline should always succeed; turns off deadline")
	err = tConn.SetReadDeadline(time.Now().AddDate(10, 0, 0))
	assert.NoError(t, err, "setting deadline 10 year from now succeeds")
	err = tConn.SetWriteDeadline(time.Now().AddDate(10, 0, 0))
	assert.NoError(t, err, "setting deadline 10 year from now succeeds")
	err = tConn.SetDeadline(time.Now().AddDate(10, 0, 0))
	assert.NoError(t, err, "setting deadline 10 year from now succeeds")
}

// dialForTunnelingConnection 对给定 URL 发起 WebSocket 升级请求，
// 返回封装该 WebSocket 连接的 TunnelingConnection；出错时返回错误。
//
// 测试辅助：手动完成 WebSocket 升级并构造客户端 TunnelingConnection，绕过 Dialer 的 SPDY 封装。
func dialForTunnelingConnection(url *url.URL) (*TunnelingConnection, error) {
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	// 隧道连接必须使用端口转发隧道协议发起 WebSocket 升级。
	tunnelingProtocols := []string{constants.WebsocketsSPDYTunnelingPortForwardV1}
	transport, holder, err := websocket.RoundTripperFor(&rest.Config{Host: url.Host})
	if err != nil {
		return nil, err
	}
	conn, err := websocket.Negotiate(transport, holder, req, tunnelingProtocols...)
	if err != nil {
		return nil, err
	}
	//nolint:logcheck // Intentionally uses the old API.
	return NewTunnelingConnection("client", conn), nil
}

// justQueueStream 返回一个 streamHandler，把收到的每条流塞进 channel，
// 使测试线程可以同步处理，无需真实的上游服务。
func justQueueStream(streams chan httpstream.Stream) func(httpstream.Stream, <-chan struct{}) error {
	return func(stream httpstream.Stream, replySent <-chan struct{}) error {
		streams <- stream
		return nil
	}
}
