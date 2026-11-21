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

package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	restclient "k8s.io/client-go/rest"
	. "k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// fakePortForwarder 模拟端口转发行为，实现了 portforward.PortForwarder 接口。
// 它按端口记录期望收到的、实际收到的数据，以及需要回发给客户端的数据，
// 从而在测试中验证双向数据流是否正确。
type fakePortForwarder struct {
	lock sync.Mutex
	// expected 保存每个端口期望从流中读取到的数据
	expected map[int32]string
	// received 保存每个端口实际从流中读取到的数据
	received map[int32]string
	// send 保存每个端口需要写回流、发往客户端的数据
	send map[int32]string
}

var _ portforward.PortForwarder = &fakePortForwarder{}

// PortForward 处理单个端口的数据流：先按期望长度读取客户端发来的数据，
// 记录到 received 供后续断言，再把预设的数据写回流回送给客户端。
func (pf *fakePortForwarder) PortForward(_ context.Context, name string, uid string, port int32, stream io.ReadWriteCloser) error {
	defer stream.Close()

	// 按期望长度读取客户端数据；长度不一致说明流协议或分帧有问题。
	received := make([]byte, len(pf.expected[port]))
	n, err := stream.Read(received)
	if err != nil {
		return fmt.Errorf("error reading from client for port %d: %v", port, err)
	}
	if n != len(pf.expected[port]) {
		return fmt.Errorf("unexpected length read from client for port %d: got %d, expected %d. data=%q", port, n, len(pf.expected[port]), string(received))
	}

	// 多个端口的流可能并发处理，写 received 时需要加锁保护。
	pf.lock.Lock()
	pf.received[port] = string(received)
	pf.lock.Unlock()

	// 将预设数据回发给客户端，完成一次完整的双向转发。
	io.Copy(stream, strings.NewReader(pf.send[port]))

	return nil
}

// fakePortForwardServer 创建一个能够处理端口转发请求的 HTTP handler。
// 它在 ServePortForward 返回后校验服务端实际收到的数据与期望值完全一致。
func fakePortForwardServer(t *testing.T, testName string, serverSends, expectedFromClient map[int32]string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		pf := &fakePortForwarder{
			expected: expectedFromClient,
			received: make(map[int32]string),
			send:     serverSends,
		}
		portforward.ServePortForward(w, req, pf, "pod", "uid", nil, 0, 10*time.Second, portforward.SupportedProtocols)

		for port, expected := range expectedFromClient {
			actual, ok := pf.received[port]
			if !ok {
				t.Errorf("%s: server didn't receive any data for port %d", testName, port)
				continue
			}

			if expected != actual {
				t.Errorf("%s: server expected to receive %q, got %q for port %d", testName, expected, actual, port)
			}
		}

		for port, actual := range pf.received {
			if _, ok := expectedFromClient[port]; !ok {
				t.Errorf("%s: server unexpectedly received %q for port %d", testName, actual, port)
			}
		}
	})
}

// TestForwardPorts 端到端验证端口转发：通过 SPDY 建立转发通道，
// 在客户端本地监听端口上收发数据，并断言服务端收到的内容与预期一致。
func TestForwardPorts(t *testing.T) {
	tests := map[string]struct {
		ports       []string
		clientSends map[int32]string
		serverSends map[int32]string
	}{
		"forward 1 port with no data either direction": {
			ports: []string{":5000"},
		},
		"forward 2 ports with bidirectional data": {
			ports: []string{":5001", ":6000"},
			clientSends: map[int32]string{
				5001: "abcd",
				6000: "ghij",
			},
			serverSends: map[int32]string{
				5001: "1234",
				6000: "5678",
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			server := httptest.NewServer(fakePortForwardServer(t, testName, test.serverSends, test.clientSends))
			defer server.Close()

			transport, upgrader, err := spdy.RoundTripperFor(&restclient.Config{})
			if err != nil {
				t.Fatal(err)
			}
			url, _ := url.Parse(server.URL)
			dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)

			// 缓冲区大小为 1，保证 close 不会因无接收者而阻塞。
			stopChan := make(chan struct{}, 1)
			readyChan := make(chan struct{})

			pf, err := New(dialer, test.ports, stopChan, readyChan, os.Stdout, os.Stderr)
			if err != nil {
				t.Fatalf("%s: unexpected error calling New: %v", testName, err)
			}

			// ForwardPorts 会阻塞直到 stopChan 关闭，故放到 goroutine 中执行，
			// 并通过 doneChan 等待其真正退出。
			doneChan := make(chan error)
			go func() {
				doneChan <- pf.ForwardPorts()
			}()
			// 必须等 Ready 后再查询端口，否则本地监听可能尚未建立。
			<-pf.Ready

			forwardedPorts, err := pf.GetPorts()
			if err != nil {
				t.Fatal(err)
			}

			// 请求里用的是 ":port" 形式，本地端口由系统分配，
			// 因此建立 remote -> local 的映射才能拨号到真实的本地监听地址。
			remoteToLocalMap := map[int32]int32{}
			for _, forwardedPort := range forwardedPorts {
				remoteToLocalMap[int32(forwardedPort.Remote)] = int32(forwardedPort.Local)
			}

			clientSend := func(port int32, data string) error {
				clientConn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", remoteToLocalMap[port]))
				if err != nil {
					return fmt.Errorf("%s: error dialing %d: %s", testName, port, err)

				}
				defer clientConn.Close()

				n, err := clientConn.Write([]byte(data))
				if err != nil && err != io.EOF {
					return fmt.Errorf("%s: Error sending data '%s': %s", testName, data, err)
				}
				if n == 0 {
					return fmt.Errorf("%s: unexpected write of 0 bytes", testName)
				}
				// 回读服务端发来的固定长度数据，验证反向数据流。
				b := make([]byte, 4)
				_, err = clientConn.Read(b)
				if err != nil && err != io.EOF {
					return fmt.Errorf("%s: Error reading data: %s", testName, err)
				}
				// 字节级比对，确保转发过程未篡改或截断数据。
				if !bytes.Equal([]byte(test.serverSends[port]), b) {
					return fmt.Errorf("%s: expected to read '%s', got '%s'", testName, test.serverSends[port], b)
				}
				return nil
			}
			for port, data := range test.clientSends {
				if err := clientSend(port, data); err != nil {
					t.Error(err)
				}
			}
			// 通知 ForwardPorts 停止监听并清理本地端口。
			close(stopChan)

			// 等待 ForwardPorts 真正返回，确认清理过程没有报错。
			err = <-doneChan
			if err != nil {
				t.Errorf("%s: unexpected error: %s", testName, err)
			}
		})
	}

}

// TestForwardPortsReturnsErrorWhenAllBindsFailed 验证当所有端口绑定都失败时
// ForwardPorts 会返回错误：先用 pf1 占用一个本地端口，再让 pf2 复用同一
// 端口组合，pf2 必须因绑定冲突而失败，而不是静默地忽略错误。
func TestForwardPortsReturnsErrorWhenAllBindsFailed(t *testing.T) {
	server := httptest.NewServer(fakePortForwardServer(t, "allBindsFailed", nil, nil))
	defer server.Close()

	transport, upgrader, err := spdy.RoundTripperFor(&restclient.Config{})
	if err != nil {
		t.Fatal(err)
	}
	url, _ := url.Parse(server.URL)
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)

	stopChan1 := make(chan struct{}, 1)
	// 用 defer 确保 pf1 无论测试成功或失败都会被停止，释放占用的端口。
	defer close(stopChan1)
	readyChan1 := make(chan struct{})

	pf1, err := New(dialer, []string{":5555"}, stopChan1, readyChan1, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("error creating pf1: %v", err)
	}
	go pf1.ForwardPorts()
	<-pf1.Ready

	forwardedPorts, err := pf1.GetPorts()
	if err != nil {
		t.Fatal(err)
	}
	if len(forwardedPorts) != 1 {
		t.Fatalf("expected 1 forwarded port, got %#v", forwardedPorts)
	}
	// 用 pf1 实际分配到的 local:remote 组合，确保 pf2 与它完全冲突。
	duplicateSpec := fmt.Sprintf("%d:%d", forwardedPorts[0].Local, forwardedPorts[0].Remote)

	stopChan2 := make(chan struct{}, 1)
	readyChan2 := make(chan struct{})
	pf2, err := New(dialer, []string{duplicateSpec}, stopChan2, readyChan2, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("error creating pf2: %v", err)
	}
	// 同步调用：所有绑定都失败时应直接返回错误而非阻塞等待 Ready。
	if err := pf2.ForwardPorts(); err == nil {
		t.Fatal("expected non-nil error for pf2.ForwardPorts")
	}
}

// TestForwardPortsWebSocketV4Framing 验证 WebSocket v4 子协议下的端口转发帧格式：
// 服务端需先在数据通道和错误通道上分别发送包含端口号的前导帧（preamble），
// 客户端据此识别每个通道对应的端口，随后才是真正的数据帧。
func TestForwardPortsWebSocketV4Framing(t *testing.T) {
	const testPort int32 = 5001
	const clientPayload = "abcd"
	const serverPayload = "1234"

	// done 用于等待服务端 handler 处理完毕，避免客户端提前退出
	// 而错过服务端的校验错误。
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(done)

		// 从请求中解析 v4 选项：包含协议版本与端口协商信息，
		// 与 SPDY 路径不同，WebSocket 需要显式声明 v4。
		opts, err := portforward.NewV4Options(req)
		if err != nil {
			t.Errorf("failed to build v4 options: %v", err)
			return
		}

		pf := &fakePortForwarder{
			expected: map[int32]string{testPort: clientPayload},
			received: make(map[int32]string),
			send:     map[int32]string{testPort: serverPayload},
		}
		portforward.ServePortForward(w, req, pf, "pod", "uid", opts, 0, 10*time.Second, portforward.SupportedProtocols)

		// ServePortForward 返回后，检查服务端确实收到了客户端发来的数据。
		got, ok := pf.received[testPort]
		if !ok {
			t.Errorf("server did not receive data for port %d", testPort)
			return
		}
		if got != clientPayload {
			t.Errorf("server expected %q, got %q for port %d", clientPayload, got, testPort)
		}
	}))
	defer server.Close()

	// 把 http 换成 ws，并显式指定 v4 子协议，模拟真实客户端的握手。
	wsURL := strings.Replace(server.URL, "http://", "ws://", 1) + fmt.Sprintf("?port=%d", testPort)
	dialer := &websocket.Dialer{
		Subprotocols: []string{"v4." + wsstream.ChannelWebSocketProtocol},
	}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial failed: %v", err)
	}
	defer conn.Close()

	// v4 帧的前导部分使用小端 16 位整数表示端口号。
	expectedPortBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(expectedPortBytes, uint16(testPort))

	// 循环读取直到数据通道(0)与错误通道(1)的前导帧都收到为止，
	// 两者都携带相同的端口号，用来把通道与端口关联起来。
	seenDataPreamble := false
	seenErrorPreamble := false
	for !(seenDataPreamble && seenErrorPreamble) {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("failed reading websocket preamble frame: %v", err)
		}
		if len(frame) == 0 {
			continue
		}
		channel := frame[0]
		payload := frame[1:]
		switch channel {
		case 0:
			if !bytes.Equal(expectedPortBytes, payload) {
				t.Fatalf("unexpected data-channel preamble payload: %q", payload)
			}
			seenDataPreamble = true
		case 1:
			if !bytes.Equal(expectedPortBytes, payload) {
				t.Fatalf("unexpected error-channel preamble payload: %q", payload)
			}
			seenErrorPreamble = true
		}
	}

	// 构造数据帧：首字节是通道号 0，其后是真正的负载数据。
	frame := append([]byte{0}, []byte(clientPayload)...)
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatalf("failed writing data frame: %v", err)
	}

	// 跳过空帧及非数据通道(错误通道)帧，直到读到服务端回发的数据。
	gotServerPayload := false
	for !gotServerPayload {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("failed reading websocket response frame: %v", err)
		}
		if len(frame) == 0 {
			continue
		}
		if frame[0] != 0 {
			continue
		}
		if bytes.Equal(frame[1:], []byte(serverPayload)) {
			gotServerPayload = true
		}
	}

	// 主动关闭连接，使服务端 handler 的 ServePortForward 返回并完成其校验。
	if err := conn.Close(); err != nil {
		t.Fatalf("failed closing websocket connection: %v", err)
	}

	// 等待服务端校验结束，超时则说明 handler 卡住，可能存在协议死锁。
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for websocket portforward handler to complete")
	}
}
