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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// 基本往返：确认 Reader 把底层 io.Reader 的内容原样送进 WebSocket，
// 不因分块或帧边界改变字节序列。
func TestStream(t *testing.T) {
	input := "some random text"
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(bytes.NewBuffer([]byte(input)), true, NewDefaultReaderProtocols())
	r.SetIdleTimeout(time.Second)
	data, err := readWebSocket(r, t, nil)
	if !reflect.DeepEqual(data, []byte(input)) {
		t.Errorf("unexpected server read: %v", data)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// ping=true 时应先发一个空帧再发数据帧；逐帧读而不是 io.ReadAll，
// 因为合并字节流之后无法判断空帧是否真的存在。
func TestStreamPing(t *testing.T) {
	input := "some random text"
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(bytes.NewBuffer([]byte(input)), true, NewDefaultReaderProtocols())
	r.SetIdleTimeout(time.Second)
	err := expectWebSocketFrames(r, t, nil, [][]byte{
		{},
		[]byte(input),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// 协商到 base64 子协议后，线上应看到编码后的文本而不是原始字节；
// 这保证不支持二进制帧的中间代理也能传输流内容。
func TestStreamBase64(t *testing.T) {
	input := "some random text"
	encoded := base64.StdEncoding.EncodeToString([]byte(input))
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(bytes.NewBuffer([]byte(input)), true, NewDefaultReaderProtocols())
	data, err := readWebSocket(r, t, nil, "base64.binary.k8s.io")
	if !reflect.DeepEqual(data, []byte(encoded)) {
		t.Errorf("unexpected server read: %v\n%v", data, []byte(encoded))
	}
	if err != nil {
		t.Fatal(err)
	}
}

// 版本化子协议同样要按各自的 Binary 配置选编码方式；
// 手工列一张带 v1/v2 的协议表，避免把版本号当成编码方式的隐含条件。
func TestStreamVersionedBase64(t *testing.T) {
	input := "some random text"
	encoded := base64.StdEncoding.EncodeToString([]byte(input))
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(bytes.NewBuffer([]byte(input)), true, map[string]ReaderProtocolConfig{
		"":                        {Binary: true},
		"binary.k8s.io":           {Binary: true},
		"base64.binary.k8s.io":    {Binary: false},
		"v1.binary.k8s.io":        {Binary: true},
		"v1.base64.binary.k8s.io": {Binary: false},
		"v2.binary.k8s.io":        {Binary: true},
		"v2.base64.binary.k8s.io": {Binary: false},
	})
	data, err := readWebSocket(r, t, nil, "v2.base64.binary.k8s.io")
	if !reflect.DeepEqual(data, []byte(encoded)) {
		t.Errorf("unexpected server read: %v\n%v", data, []byte(encoded))
	}
	if err != nil {
		t.Fatal(err)
	}
}

// 复用 Conn 测试的协商表，确认 Reader 的握手逻辑与 Conn 一致；
// 两边共用一份表，协议规则变更时不会出现只有一侧跟进的漂移。
func TestStreamVersionedCopy(t *testing.T) {
	for i, test := range versionTests() {
		func() {
			supportedProtocols := map[string]ReaderProtocolConfig{}
			for p, binary := range test.supported {
				supportedProtocols[p] = ReaderProtocolConfig{
					Binary: binary,
				}
			}
			input := "some random text"
			//nolint:logcheck // Intentionally uses the old API.
			r := NewReader(bytes.NewBuffer([]byte(input)), true, supportedProtocols)
			// Copy 通过 r.err 把结果送回调用方；握手失败时服务端写 503，
			// 让客户端的 DialConfig 拿到错误，从而覆盖表里的 error 场景。
			s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				err := r.Copy(w, req)
				if err != nil {
					w.WriteHeader(503)
				}
			}))
			defer s.Close()

			config, err := websocket.NewConfig("ws://"+addr, "http://localhost/")
			if err != nil {
				t.Error(err)
				return
			}
			config.Protocol = test.requested
			client, err := websocket.DialConfig(config)
			if err != nil {
				if !test.error {
					t.Errorf("test %d: didn't expect error: %v", i, err)
				}
				return
			}
			defer client.Close()
			if test.error && err == nil {
				t.Errorf("test %d: expected an error", i)
				return
			}

			<-r.err
			if got, expected := r.selectedProtocol, test.expected; got != expected {
				t.Errorf("test %d: unexpected protocol version: got=%s expected=%s", i, got, expected)
			}
		}()
	}
}

// 底层 io.Reader 返回非 EOF 错误时，已经读到的内容仍应送达客户端，
// 错误再通过 Copy 的返回值上报；验证“部分数据不丢”。
func TestStreamError(t *testing.T) {
	input := "some random text"
	errs := &errorReader{
		reads: [][]byte{
			[]byte("some random"),
			[]byte(" text"),
		},
		err: fmt.Errorf("bad read"),
	}
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(errs, false, NewDefaultReaderProtocols())

	data, err := readWebSocket(r, t, nil)
	if !reflect.DeepEqual(data, []byte(input)) {
		t.Errorf("unexpected server read: %v", data)
	}
	if err == nil || err.Error() != "bad read" {
		t.Fatal(err)
	}
}

// 底层 Reader panic 不应带崩整个进程：注入一个只做 recover 的 handleCrash，
// 验证 panic 被拦在流的 goroutine 内且不会污染已写出的数据。
func TestStreamSurvivesPanic(t *testing.T) {
	input := "some random text"
	errs := &errorReader{
		reads: [][]byte{
			[]byte("some random"),
			[]byte(" text"),
		},
		panicMessage: "bad read",
	}
	//nolint:logcheck // Intentionally uses the old API.
	r := NewReader(errs, false, NewDefaultReaderProtocols())

	// 不在 handler 中调用 runtime.HandleCrash()，否则测试会被 panic 中断；
	// 此处仅注入 recover，验证吞掉 panic 后流的表现。
	r.handleCrash = func(_ context.Context, additionalHandlers ...func(context.Context, interface{})) { recover() }

	data, err := readWebSocket(r, t, nil)
	if !reflect.DeepEqual(data, []byte(input)) {
		t.Errorf("unexpected server read: %v", data)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// 客户端在流还没读完时关连接是竞态高发场景：重复多次，
// 确认服务端不会因为写已关闭的 socket 而卡死或返回莫名其妙的错误。
func TestStreamClosedDuringRead(t *testing.T) {
	// 循环 25 次是为了覆盖“关闭发生在读间隙”和“关闭发生在写间隙”等不同时序。
	for i := 0; i < 25; i++ {
		ch := make(chan struct{})
		input := "some random text"
		errs := &errorReader{
			reads: [][]byte{
				[]byte("some random"),
				[]byte(" text"),
			},
			err:   fmt.Errorf("stuff"),
			pause: ch,
		}
		//nolint:logcheck // Intentionally uses the old API.
		r := NewReader(errs, false, NewDefaultReaderProtocols())

		data, err := readWebSocket(r, t, func(c *websocket.Conn) {
			c.Close()
			close(ch)
		})
		// 提前关闭连接时，服务端必须返回可识别的关闭错误。
		if err == nil || !strings.Contains(err.Error(), "use of closed network connection") {
			t.Fatal(err)
		}
		// 提前关闭时只能收到输入数据的前缀，不能出现额外字节。
		if !bytes.HasPrefix([]byte(input), data) && len(data) != 0 {
			t.Fatalf("unexpected server read: %q", string(data))
		}
	}
}

// 可控的假 Reader：按预设分块返回数据，耗尽后再返回错误、panic 或阻塞，
// 便于分别模拟正常分块、读取失败和客户端提前关闭三种情形。
type errorReader struct {
	reads        [][]byte
	err          error
	panicMessage string
	pause        chan struct{}
}

// 数据块全部吐完才走异常路径：pause 用于把“返回错误”推迟到测试明确放行时，
// 从而制造客户端先关连接、服务端仍在读的窗口。
func (r *errorReader) Read(p []byte) (int, error) {
	if len(r.reads) == 0 {
		if r.pause != nil {
			<-r.pause
		}
		if len(r.panicMessage) != 0 {
			panic(r.panicMessage)
		}
		return 0, r.err
	}
	next := r.reads[0]
	r.reads = r.reads[1:]
	copy(p, next)
	return len(next), nil
}

// 起一个临时服务端跑 Reader.Copy，返回客户端读到的全部字节和 Copy 的错误。
// 用 channel 传 Copy 结果是因为它在 HTTP handler 的 goroutine 里返回，
// 与测试主体的时序完全独立。
func readWebSocket(r *Reader, t *testing.T, fn func(*websocket.Conn), protocols ...string) ([]byte, error) {
	errCh := make(chan error, 1)
	s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		errCh <- r.Copy(w, req)
	}))
	defer s.Close()

	config, _ := websocket.NewConfig("ws://"+addr, "http://"+addr)
	config.Protocol = protocols
	client, err := websocket.DialConfig(config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if fn != nil {
		fn(client)
	}

	data, err := io.ReadAll(client)
	if err != nil {
		return data, err
	}
	return data, <-errCh
}

// 逐帧校验工具：readWebSocket 把字节拼接后无法区分帧边界，
// 需要确认 ping 空帧、数据帧各自独立时用这个版本。
func expectWebSocketFrames(r *Reader, t *testing.T, fn func(*websocket.Conn), frames [][]byte, protocols ...string) error {
	errCh := make(chan error, 1)
	s, addr := newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		errCh <- r.Copy(w, req)
	}))
	defer s.Close()

	config, _ := websocket.NewConfig("ws://"+addr, "http://"+addr)
	config.Protocol = protocols
	ws, err := websocket.DialConfig(config)
	if err != nil {
		return err
	}
	defer ws.Close()

	if fn != nil {
		fn(ws)
	}

	for i := range frames {
		var data []byte
		if err := websocket.Message.Receive(ws, &data); err != nil {
			return err
		}
		if !reflect.DeepEqual(frames[i], data) {
			return fmt.Errorf("frame %d did not match expected: %v", data, err)
		}
	}
	// 数据发完后应正常关闭，多出来的帧说明发送端多写了一次。
	var data []byte
	if err := websocket.Message.Receive(ws, &data); err != io.EOF {
		return fmt.Errorf("expected no more frames: %v (%v)", err, data)
	}
	return <-errCh
}
