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

package wsstream

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"golang.org/x/net/websocket"
)

// 把 httptest.Server 的启动和地址提取合在一起：所有测试都要拿这个地址
// 去拨号，重复两行样板只会让真正要验证的握手/帧格式逻辑被淹没。
func newServer(handler http.Handler) (*httptest.Server, string) {
	server := httptest.NewServer(handler)
	serverAddr := server.Listener.Addr().String()
	return server, serverAddr
}

// 覆盖原始二进制帧下的全部通道方向组合，并验证帧首字节确实是通道号：
// 只要编码或方向判断错一处，客户端读到的内容就会带上错误的字节或卡住。
func TestRawConn(t *testing.T) {
	// 五种通道各测一种情形：双向、双向（服务端发）、忽略、只读、只写。
	channels := []ChannelType{ReadWriteChannel, ReadWriteChannel, IgnoreChannel, ReadChannel, WriteChannel}
	conn := NewConn(NewDefaultChannelProtocols(channels))

	s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn.Open(w, req)
	}))
	defer s.Close()

	client, err := websocket.Dial("ws://"+addr, "", "http://localhost/")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// io.ReadAll 要等到对端关闭管道才返回，而关闭发生在接收循环退出后；
	// 等 ready 只保证通道已建好，不能保证数据已经送达。
	<-conn.ready
	wg := sync.WaitGroup{}

	// 在 goroutine 中等待并读取客户端写入；必须先启动读取，再触发服务端投递，
	// 否则 io.ReadAll 会一直等不到 EOF。
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := io.ReadAll(conn.channels[0])
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(data, []byte("client")) {
			t.Errorf("unexpected server read: %v", data)
		}
	}()

	// 客户端写入时手工拼上通道字节 0，验证服务端按帧首字节分发而非按连接顺序分发。
	if n, err := client.Write(append([]byte{0}, []byte("client")...)); err != nil || n != 7 {
		t.Fatalf("%d: %v", n, err)
	}

	// 客户端应读到服务端写入及对应的通道字节。
	wg.Add(1)
	go func() {
		defer wg.Done()
		if n, err := conn.channels[1].Write([]byte("server")); err != nil && n != 6 {
			t.Errorf("%d: %v", n, err)
		}
	}()

	// 期望读到 7 字节而不是 6：通道字节也在线上，容易被漏算。
	data := make([]byte, 1024)
	if n, err := io.ReadAtLeast(client, data, 6); n != 7 || err != nil {
		t.Fatalf("%d: %v", n, err)
	}
	if !reflect.DeepEqual(data[:7], append([]byte{1}, []byte("server")...)) {
		t.Errorf("unexpected client read: %v", data[:7])
	}

	// 忽略通道的读写都应静默丢弃，不向对端发送数据。
	if n, err := conn.channels[2].Write([]byte("test")); n != 4 || err != nil {
		t.Errorf("writes should be ignored")
	}
	data = make([]byte, 1024)
	if n, err := conn.channels[2].Read(data); n != 0 || err != io.EOF {
		t.Errorf("reads should be ignored")
	}

	// 向只读通道写入应被丢弃，不能阻塞。
	if n, err := conn.channels[3].Write([]byte("test")); n != 4 || err != nil {
		t.Errorf("writes should be ignored")
	}

	// 从只写通道读取应立即返回 EOF，不能阻塞。
	data = make([]byte, 1024)
	if n, err := conn.channels[4].Read(data); n != 0 || err != io.EOF {
		t.Errorf("reads should be ignored")
	}

	// 客户端向只写通道发送的数据应被丢弃，不能阻塞接收循环。
	if n, err := client.Write(append([]byte{4}, []byte("ignored")...)); err != nil || n != 8 {
		t.Fatalf("%d: %v", n, err)
	}

	client.Close()
	wg.Wait()
}

// 同 TestRawConn 的收发路径，但走文本子协议：重点验证通道号变成 '0'/'1'
// 字符、载荷被 base64 编解码，且线上帧长度随编码结果变化。
func TestBase64Conn(t *testing.T) {
	conn := NewConn(NewDefaultChannelProtocols([]ChannelType{ReadWriteChannel, ReadWriteChannel}))
	s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn.Open(w, req)
	}))
	defer s.Close()

	config, err := websocket.NewConfig("ws://"+addr, "http://localhost/")
	if err != nil {
		t.Fatal(err)
	}
	// 必须显式声明子协议，否则会协商到默认的二进制通道协议。
	config.Protocol = []string{"base64.channel.k8s.io"}
	client, err := websocket.DialConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	<-conn.ready
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := io.ReadAll(conn.channels[0])
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(data, []byte("client")) {
			t.Errorf("unexpected server read: %s", string(data))
		}
	}()

	// 通道号写成字符 '0' 而非字节 0；服务端若忘记减 '0' 就会索引到不存在的通道。
	clientData := base64.StdEncoding.EncodeToString([]byte("client"))
	if n, err := client.Write(append([]byte{'0'}, clientData...)); err != nil || n != len(clientData)+1 {
		t.Fatalf("%d: %v", n, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if n, err := conn.channels[1].Write([]byte("server")); err != nil && n != 6 {
			t.Errorf("%d: %v", n, err)
		}
	}()

	// "server" 编码后是 8 字节，加上通道字符共 9 字节；硬编码长度能顺带
	// 抓到“服务端忘了 base64 编码”这类错误，否则内容断言才会失败得更晚。
	data := make([]byte, 1024)
	if n, err := io.ReadAtLeast(client, data, 9); n != 9 || err != nil {
		t.Fatalf("%d: %v", n, err)
	}
	expect := []byte(base64.StdEncoding.EncodeToString([]byte("server")))

	if !reflect.DeepEqual(data[:9], append([]byte{'1'}, expect...)) {
		t.Errorf("unexpected client read: %v", data[:9])
	}

	client.Close()
	wg.Wait()
}

// 握手场景的通用表：supported 描述服务端允许哪些协议及其编码，
// requested 是客户端提议列表，expected 是应当协商出的协议；error 表示预期握手失败。
type versionTest struct {
	supported map[string]bool // 子协议名 -> 是否使用二进制帧
	requested []string
	error     bool
	expected  string
}

// 这张表被 Conn 和 Reader 两边的版本测试共用，保证两种实现遵循同一套协商规则。
// 特别覆盖“提议多个协议时选服务端认识的最后一个”和“空协议名兜底”两种边界。
func versionTests() []versionTest {
	const (
		binary = true
		base64 = false
	)
	return []versionTest{
		{
			supported: nil,
			requested: []string{"raw"},
			error:     true,
		},
		{
			supported: map[string]bool{"": binary, "raw": binary, "base64": base64},
			requested: nil,
			expected:  "",
		},
		{
			supported: map[string]bool{"": binary, "raw": binary, "base64": base64},
			requested: []string{"v1.raw"},
			error:     true,
		},
		{
			supported: map[string]bool{"": binary, "raw": binary, "base64": base64},
			requested: []string{"v1.raw", "v1.base64"},
			error:     true,
		}, {
			supported: map[string]bool{"": binary, "raw": binary, "base64": base64},
			requested: []string{"v1.raw", "raw"},
			expected:  "raw",
		},
		{
			supported: map[string]bool{"": binary, "v1.raw": binary, "v1.base64": base64, "v2.raw": binary, "v2.base64": base64},
			requested: []string{"v1.raw"},
			expected:  "v1.raw",
		},
		{
			supported: map[string]bool{"": binary, "v1.raw": binary, "v1.base64": base64, "v2.raw": binary, "v2.base64": base64},
			requested: []string{"v2.base64"},
			expected:  "v2.base64",
		},
	}
}

// 用 versionTests 驱动真实的 HTTP/WebSocket 握手，验证协议协商结果，
// 而不是只测内部的 handshake 辅助函数——后者无法暴露 ServeHTTP 时序上的竞态。
func TestVersionedConn(t *testing.T) {
	for i, test := range versionTests() {
		func() {
			supportedProtocols := map[string]ChannelProtocolConfig{}
			for p, binary := range test.supported {
				supportedProtocols[p] = ChannelProtocolConfig{
					Binary:   binary,
					Channels: []ChannelType{ReadWriteChannel},
				}
			}
			conn := NewConn(supportedProtocols)
			// Open 返回的协议通过 channel 传给测试，建立明确的同步关系，
			// 避免测试直接并发读取 conn.selectedProtocol。
			selectedProtocol := make(chan string)
			s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				p, _, _ := conn.Open(w, req)
				selectedProtocol <- p
			}))
			defer s.Close()

			config, err := websocket.NewConfig("ws://"+addr, "http://localhost/")
			if err != nil {
				t.Fatal(err)
			}
			config.Protocol = test.requested
			client, err := websocket.DialConfig(config)
			if err != nil {
				if !test.error {
					t.Fatalf("test %d: didn't expect error: %v", i, err)
				} else {
					return
				}
			}
			defer client.Close()
			if test.error && err == nil {
				t.Fatalf("test %d: expected an error", i)
			}

			<-conn.ready
			if got, expected := <-selectedProtocol, test.expected; got != expected {
				t.Fatalf("test %d: unexpected protocol version: got=%s expected=%s", i, got, expected)
			}
		}()
	}
}

// 逐项验证“升级头 + 子协议列表”的组合判断，重点是区分头名的大小写不敏感
// 和头值的大小写敏感：把两者混为一谈会误放行错误版本的客户端。
func TestIsWebSocketRequestWithStreamCloseProtocol(t *testing.T) {
	tests := map[string]struct {
		headers  map[string]string
		expected bool
	}{
		"No headers returns false": {
			headers:  map[string]string{},
			expected: false,
		},
		"Only connection upgrade header is false": {
			headers: map[string]string{
				"Connection": "upgrade",
			},
			expected: false,
		},
		"Only websocket upgrade header is false": {
			headers: map[string]string{
				"Upgrade": "websocket",
			},
			expected: false,
		},
		"Only websocket and connection upgrade headers is false": {
			headers: map[string]string{
				"Connection": "upgrade",
				"Upgrade":    "websocket",
			},
			expected: false,
		},
		"Missing connection/upgrade header is false": {
			headers: map[string]string{
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v5.channel.k8s.io",
			},
			expected: false,
		},
		"Websocket connection upgrade headers with v5 protocol is true": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v5.channel.k8s.io",
			},
			expected: true,
		},
		// 头名大小写无关，头值大小写敏感：这一项正是为了锁住该差异。
		"Websocket connection upgrade headers with wrong case v5 protocol is false": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v5.CHANNEL.k8s.io", // 头值区分大小写
			},
			expected: false,
		},
		"Websocket connection upgrade headers with v4 protocol is false": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v4.channel.k8s.io",
			},
			expected: false,
		},
		"Websocket connection upgrade headers with multiple protocols but missing v5 is false": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v4.channel.k8s.io,v3.channel.k8s.io,v2.channel.k8s.io",
			},
			expected: false,
		},
		"Websocket connection upgrade headers with multiple protocols including v5 and spaces is true": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v5.channel.k8s.io,  v4.channel.k8s.io",
			},
			expected: true,
		},
		"Websocket connection upgrade headers with multiple protocols out of order including v5 and spaces is true": {
			headers: map[string]string{
				"Connection":            "upgrade",
				"Upgrade":               "websocket",
				WebSocketProtocolHeader: "v4.channel.k8s.io, v5.channel.k8s.io, v3.channel.k8s.io",
			},
			expected: true,
		},

		"Websocket connection upgrade headers key is case-insensitive": {
			headers: map[string]string{
				"Connection":             "upgrade",
				"Upgrade":                "websocket",
				"sec-websocket-protocol": "v4.channel.k8s.io, v5.channel.k8s.io, v3.channel.k8s.io",
			},
			expected: true,
		},
	}

	for name, test := range tests {
		req, err := http.NewRequest(http.MethodGet, "http://www.example.com/", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for key, value := range test.headers {
			req.Header.Add(key, value)
		}
		actual := IsWebSocketRequestWithStreamCloseProtocol(req)
		if actual != test.expected {
			t.Errorf("%s: expected (%t), got (%t)", name, test.expected, actual)
		}
	}
}

// 只认 V5 精确拼写的判断表；V4 及更早版本不支持半关闭信号，
// 大小写变形同样不能被接受，否则会错误启用关闭帧解析逻辑。
func TestProtocolSupportsStreamClose(t *testing.T) {
	tests := map[string]struct {
		protocol string
		expected bool
	}{
		"empty protocol returns false": {
			protocol: "",
			expected: false,
		},
		"not binary protocol returns false": {
			protocol: "base64.channel.k8s.io",
			expected: false,
		},
		"V1 protocol returns false": {
			protocol: "channel.k8s.io",
			expected: false,
		},
		"V4 protocol returns false": {
			protocol: "v4.channel.k8s.io",
			expected: false,
		},
		"V5 protocol returns true": {
			protocol: "v5.channel.k8s.io",
			expected: true,
		},
		"V5 protocol wrong case returns false": {
			protocol: "V5.channel.K8S.io",
			expected: false,
		},
	}

	for name, test := range tests {
		actual := protocolSupportsStreamClose(test.protocol)
		if actual != test.expected {
			t.Errorf("%s: expected (%t), got (%t)", name, test.expected, actual)
		}
	}
}
