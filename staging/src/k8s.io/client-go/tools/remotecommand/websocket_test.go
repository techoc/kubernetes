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
	"encoding/json"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gwebsocket "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnettesting "k8s.io/apimachinery/pkg/util/net/testing"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// 假服务端把 STDIN 流复制到 STDOUT，客户端比对发出的 1MB 随机数据与读回的数据是否一致。
// 该测试可用 "stress" 工具反复运行，检查是否存在会导致 WebSocket 流出问题的数据。
func TestWebSocketClient_LoopbackStdinToStdout(t *testing.T) {
	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		// 将 STDIN 流回环到 STDOUT 流。
		_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
		if err != nil {
			t.Fatalf("error copying STDIN to STDOUT: %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 STDIN 和 STDOUT 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
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
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
	data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Fatalf("error reading the stream: %v", err)
	}
	// 检查 STDIN 发送的随机数据与 STDOUT 返回的数据一致。
	if !bytes.Equal(randomData, data) {
		t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
	}
}

// TestWebSocketClient_DifferentBufferSizes 用不同的服务端读缓冲区大小重复 STDIN->STDOUT 回环，
// 覆盖跨缓冲区边界的分帧/重组逻辑。
func TestWebSocketClient_DifferentBufferSizes(t *testing.T) {
	// websocket 服务端读取 STDIN 时使用 1k、4k、64k、128k 四种缓冲区大小；
	// io.Copy 的标准缓冲区大小为 32k。
	bufferSizes := []int{1 * 1024, 4 * 1024, 64 * 1024, 128 * 1024}
	for _, bufferSize := range bufferSizes {
		// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
		websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
			if err != nil {
				t.Fatalf("error on webSocketServerStreams: %v", err)
			}
			defer conns.conn.Close()
			// 将 STDIN 流回环到 STDOUT 流，并使用指定大小的缓冲区。
			buffer := make([]byte, bufferSize)
			_, err = io.CopyBuffer(conns.stdoutStream, conns.stdinStream, buffer)
			if err != nil {
				t.Fatalf("error copying STDIN to STDOUT: %v", err)
			}
		}))
		defer websocketServer.Close()

		// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
		// 客户端请求必须加上 STDIN 和 STDOUT 的 query 参数。
		websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
		websocketLocation, err := url.Parse(websocketServer.URL)
		if err != nil {
			t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
		}
		exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
		if err != nil {
			t.Errorf("unexpected error creating websocket executor: %v", err)
		}
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
			// 校验协商到的是远程命令 v5 协议。
			streamExec := exec.(*wsStreamExecutor)
			if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
				t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
			}
		}
		data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
		if err != nil {
			t.Errorf("error reading the stream: %v", err)
			return
		}
		// 检查 STDIN 发送的全部随机数据与 STDOUT 返回的一致。
		if !bytes.Equal(randomData, data) {
			t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
		}
	}
}

// TestWebSocketClient_LoopbackStdinAsPipe 通过 io.Pipe 提供 STDIN 并在写完后关闭，
// 验证管道式输入（长度未知）也能被正确流式发送。
func TestWebSocketClient_LoopbackStdinAsPipe(t *testing.T) {
	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		// 将 STDIN 流回环到 STDOUT 流。
		_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
		if err != nil {
			t.Fatalf("error copying STDIN to STDOUT: %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 STDIN 和 STDOUT 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	// 生成随机数据并写入 STDIN 管道；相同数据将从 STDOUT 通道返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	reader, writer := io.Pipe()
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:  reader,
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		// 启动 WebSocket "exec" 客户端上的流式传输。
		errorChan <- exec.StreamWithContext(context.Background(), *options)
	}()
	// 把随机数据写入连接 STDIN 的管道，然后关闭管道。
	_, err = writer.Write(randomData)
	if err != nil {
		t.Fatalf("unable to write random data to STDIN pipe: %v", err)
	}
	writer.Close()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		if err != nil {
			t.Errorf("unexpected error")
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
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

// TestWebSocketClient_LoopbackStdinToStderr 验证 STDIN 数据经 STDERR 通道回环后保持一致。
func TestWebSocketClient_LoopbackStdinToStderr(t *testing.T) {
	// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDERR 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		// 将 STDIN 流回环到 STDERR 流。
		_, err = io.Copy(conns.stderrStream, conns.stdinStream)
		if err != nil {
			t.Fatalf("error copying STDIN to STDERR: %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 STDIN 和 STDERR 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stderr=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	// 生成随机数据并设置为从 STDIN 流发送；数据将从 STDERR 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stderr bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stderr: &stderr,
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
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
	data, err := io.ReadAll(bytes.NewReader(stderr.Bytes()))
	if err != nil {
		t.Errorf("error reading the stream: %v", err)
		return
	}
	// 检查 STDIN 发送的随机数据与 STDERR 返回的数据一致。
	if !bytes.Equal(randomData, data) {
		t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
	}
}

// TestWebSocketClient_MultipleReadChannels 同时从 STDOUT 与 STDERR 两个流读取，
// 验证同一连接上的多路读取不会互相阻塞或丢数据。
func TestWebSocketClient_MultipleReadChannels(t *testing.T) {
	// 创建假 WebSocket 服务端，用 TeeReader 把 STDIN 数据同时复制到 STDOUT 与 STDERR 流。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		// TeeReader 把从 STDIN 读到的数据复制一份到 STDERR。
		stdinReader := io.TeeReader(conns.stdinStream, conns.stderrStream)
		// 同时把 STDIN 复制到 STDOUT。
		_, err = io.Copy(conns.stdoutStream, stdinReader)
		if err != nil {
			t.Errorf("error copying STDIN to STDOUT: %v", err)
		}
	}))
	defer websocketServer.Close()
	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 stdin、stdout 和 stderr 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true" + "&" + "stderr=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	// 生成 1MB 随机数据并设置为从 STDIN 流发送；数据将从 STDOUT 和 STDERR 缓冲区返回。
	randomSize := 1024 * 1024
	randomData := make([]byte, randomSize)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stdout, stderr bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stdout: &stdout,
		Stderr: &stderr,
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
			t.Errorf("unexpected error: %v", err)
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
	// 校验从 STDOUT 流读到的数据与 STDIN 流发送的一致。
	stdoutBytes, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Fatalf("error reading the stream: %v", err)
	}
	if !bytes.Equal(stdoutBytes, randomData) {
		t.Errorf("unexpected data received (%d) sent (%d)", len(stdoutBytes), len(randomData))
	}
	// 校验从 STDERR 流读到的数据与 STDIN 流发送的一致。
	stderrBytes, err := io.ReadAll(bytes.NewReader(stderr.Bytes()))
	if err != nil {
		t.Fatalf("error reading the stream: %v", err)
	}
	if !bytes.Equal(stderrBytes, randomData) {
		t.Errorf("unexpected data received (%d) sent (%d)", len(stderrBytes), len(randomData))
	}
}

// randomExitCode 返回 1-127 的随机退出码，特意避开 0（0 表示成功）。
func randomExitCode() int {
	errorCode := mrand.Intn(128)
	if errorCode == 0 {
		errorCode = 1
	}
	return errorCode
}

// TestWebSocketClient_ErrorStream 验证服务端在 error 流写入结构化非零退出码时，
// 客户端能解析出形如 "command terminated with exit code N" 的错误。
func TestWebSocketClient_ErrorStream(t *testing.T) {
	expectedExitCode := randomExitCode()
	// 创建假 WebSocket 服务端：在 error 流上返回结构化的退出码错误。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		_, err = io.Copy(conns.stderrStream, conns.stdinStream)
		if err != nil {
			t.Fatalf("error copying STDIN to STDERR: %v", err)
		}
		// 强制在 error 流上返回非零退出码错误。
		err = conns.writeStatus(&apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure,
			Reason: remotecommand.NonZeroExitCodeReason,
			Details: &metav1.StatusDetails{
				Causes: []metav1.StatusCause{
					{
						Type:    remotecommand.ExitCodeCauseType,
						Message: fmt.Sprintf("%d", expectedExitCode),
					},
				},
			},
		}})
		if err != nil {
			t.Fatalf("error writing status: %v", err)
		}
	}))
	defer websocketServer.Close()

	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stderr=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	randomData := make([]byte, 256)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	var stderr bytes.Buffer
	options := &StreamOptions{
		Stdin:  bytes.NewReader(randomData),
		Stderr: &stderr,
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
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
		// 预期在 error 流上得到退出码错误。
		if err == nil {
			t.Errorf("expected error, but received none")
		}
		expectedError := fmt.Sprintf("command terminated with exit code %d", expectedExitCode)
		// 比对带退出码的预期错误与实际错误。
		if expectedError != err.Error() {
			t.Errorf("expected error (%s), got (%s)", expectedError, err)
		}
	}
}

// fakeTerminalSizeQueue 实现 TerminalSizeQueue，返回预设数量的随机窗口大小，
// 并把返回值记录下来供测试断言。
type fakeTerminalSizeQueue struct {
	maxSizes      int
	terminalSizes []TerminalSize
}

// newTerminalSizeQueue 创建最多返回 max 个窗口大小的队列。
func newTerminalSizeQueue(max int) *fakeTerminalSizeQueue {
	return &fakeTerminalSizeQueue{
		maxSizes:      max,
		terminalSizes: make([]TerminalSize, 0, max),
	}
}

// Next 返回下一个随机窗口大小；达到上限后返回 nil 表示队列结束。
func (f *fakeTerminalSizeQueue) Next() *TerminalSize {
	if len(f.terminalSizes) >= f.maxSizes {
		return nil
	}
	size := randomTerminalSize()
	f.terminalSizes = append(f.terminalSizes, size)
	return &size
}

// randomTerminalSize 生成各字段在 uint16 范围内的随机窗口大小。
func randomTerminalSize() TerminalSize {
	randWidth := uint16(mrand.Intn(int(math.Pow(2, 16))))
	randHeight := uint16(mrand.Intn(int(math.Pow(2, 16))))
	return TerminalSize{
		Width:  randWidth,
		Height: randHeight,
	}
}

// randReader 持续产生随机数据，并记录所有已返回的字节用于比对；
// lock 保护 closed 与 randBytes 以支持并发访问。
type randReader struct {
	randBytes []byte
	closed    bool
	lock      sync.Mutex
}

// Read 实现 Reader 接口：用随机数据填充传入缓冲区，返回填充的字节数和错误；
// randReader 关闭后返回 0 和 io.EOF。
func (r *randReader) Read(b []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return 0, io.EOF
	}
	n, err := rand.Read(b)
	c := bytes.Clone(b)
	r.randBytes = append(r.randBytes, c...)
	return n, err
}

// Close 实现 Closer 接口：将 closed 字段置为 true；此后调用 Read() 返回 0 和 EOF，返回 nil 错误。
func (r *randReader) Close() (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.closed = true
	return nil
}

// TestWebSocketClient_MultipleWriteChannels 同时向连接写入 STDIN 与 TTY resize，
// 验证写锁能串行化两个写线程且不丢数据。
func TestWebSocketClient_MultipleWriteChannels(t *testing.T) {
	// 构造假的终端窗口大小队列，并准备记录 websocket 另一端实际收到的 actualTerminalSizes。
	numSizeQueue := 10000
	sizeQueue := newTerminalSizeQueue(numSizeQueue)
	actualTerminalSizes := make([]TerminalSize, 0, numSizeQueue)
	// 构造 ReadCloser，通过 websocket 连接在 STDIN 流上持续发送随机数据。
	stdinReader := randReader{randBytes: []byte{}, closed: false}
	// 创建假 WebSocket 服务端，并发接收 STDIN 流与 resize 流（TerminalSizes）；
	// 保存 resize 流中的 TerminalSize 数据供后续校验。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var wg sync.WaitGroup
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		// 启动协程将 STDIN 流回环到 STDOUT 流。
		wg.Add(1)
		go func() {
			_, err := io.Copy(conns.stdoutStream, conns.stdinStream)
			if err != nil {
				t.Errorf("error copying STDIN to STDOUT: %v", err)
			}
			wg.Done()
		}()
		// 读取终端 resize 请求，存入 actualTerminalSizes
		for i := 0; i < numSizeQueue; i++ {
			actualTerminalSize := <-conns.resizeChan
			actualTerminalSizes = append(actualTerminalSizes, actualTerminalSize)
		}
		stdinReader.Close() // 停止随机 STDIN 流的生成
		wg.Wait()           // 等待 STDIN 到 STDOUT 的字节全部复制完成
	}))
	defer websocketServer.Close()
	// 创建 WebSocket 客户端（executor），指向 "websocketServer"。
	// 客户端请求必须加上 stdin、stdout 和 TTY 的 query 参数。
	websocketServer.URL = websocketServer.URL + "?" + "tty=true" + "&" + "stdin=true" + "&" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdin:             &stdinReader,
		Stdout:            &stdout,
		Tty:               true,
		TerminalSizeQueue: sizeQueue,
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
			t.Errorf("unexpected error: %v", err)
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
	// 检查 STDIN 发送的随机数据与 STDOUT 返回的一致，*并且*确认发送与接收了
	// 一定量的随机数据，以验证并发性。
	stdoutBytes, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		t.Fatalf("error reading the stream: %v", err)
	}
	if len(stdoutBytes) == 0 {
		t.Errorf("No STDOUT bytes processed before resize stream finished: %d", len(stdoutBytes))
	}
	if !bytes.Equal(stdoutBytes, stdinReader.randBytes) {
		t.Errorf("unexpected data received (%d) sent (%d)", len(stdoutBytes), len(stdinReader.randBytes))
	}
	// 校验 resize 流发送的随机 TerminalSizes 与 websocket 服务端实际收到的一致。
	if len(actualTerminalSizes) != numSizeQueue {
		t.Errorf("expected received terminal size window (%d), got (%d)",
			numSizeQueue, len(actualTerminalSizes))
	}
	for i, actual := range actualTerminalSizes {
		expected := sizeQueue.terminalSizes[i]
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected terminal resize window %v, got %v", expected, actual)
		}
	}
}

// TestWebSocketClient_ProtocolVersions 验证 V2-V4 子协议都能被协商成功。
func TestWebSocketClient_ProtocolVersions(t *testing.T) {
	// 创建一个接受远程命令子协议 V2-V4 版本的原始 websocket 服务端。
	var upgrader = gwebsocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 接受所有请求
		},
		Subprotocols: []string{
			remotecommand.StreamProtocolV4Name,
			remotecommand.StreamProtocolV3Name,
			remotecommand.StreamProtocolV2Name,
		},
	}
	// 升级原始 websocket 服务端连接。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("unable to upgrade to create websocket connection: %v", err)
		}
		defer conn.Close()
	}))
	defer websocketServer.Close()

	// 配置带 STDOUT 流的 websocket 客户端。
	websocketServer.URL = websocketServer.URL + "?" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	// 依次尝试旧版远程命令协议，校验请求的协议版本正是被协商的版本。
	versions := []string{
		remotecommand.StreamProtocolV4Name,
		remotecommand.StreamProtocolV3Name,
		remotecommand.StreamProtocolV2Name,
	}
	for _, requestedVersion := range versions {
		streamExec := exec.(*wsStreamExecutor)
		streamExec.protocols = []string{requestedVersion}
		var stdout bytes.Buffer
		options := &StreamOptions{
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
		case <-errorChan:
			// 校验协商到的是请求的远程命令协议版本。
			streamExec := exec.(*wsStreamExecutor)
			if requestedVersion != streamExec.negotiated {
				t.Fatalf("expected protocol version (%s), got (%s)", requestedVersion, streamExec.negotiated)
			}
		}
	}
}

// TestWebSocketClient_BadHandshake 验证客户端请求服务端不支持的子协议时，
// 握手失败并返回 "bad handshake" 错误。
func TestWebSocketClient_BadHandshake(t *testing.T) {
	// 创建假 WebSocket 服务端（仅支持 V5 子协议）。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// bad handshake 意味着 websocket 服务端不会完成初始化。
		_, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err == nil {
			t.Fatalf("expected error, but received none.")
		}
		if !strings.Contains(err.Error(), "websocket server finished before becoming ready") {
			t.Errorf("expected websocket server error, but got: %v", err)
		}
	}))
	defer websocketServer.Close()

	websocketServer.URL = websocketServer.URL + "?" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	streamExec := exec.(*wsStreamExecutor)
	// 将尝试的子协议版本设为 V4；websocket 服务端只接受 V5。
	streamExec.protocols = []string{remotecommand.StreamProtocolV4Name}

	var stdout bytes.Buffer
	options := &StreamOptions{
		Stdout: &stdout,
	}
	errorChan := make(chan error)
	go func() {
		// 启动 WebSocket "exec" 客户端上的流式传输。
		errorChan <- streamExec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		// 预期升级连接失败 —— "bad handshake" 错误。
		if err == nil {
			t.Errorf("expected error but received none")
		}
		if !strings.Contains(err.Error(), "bad handshake") {
			t.Errorf("expected bad handshake error, got (%s)", err)
		}
	}
}

// TestWebSocketClient_HeartbeatTimeout 把 ping 周期设得远大于 deadline，强制心跳超时，
// 验证客户端会返回 i/o timeout 而不是无限等待。
func TestWebSocketClient_HeartbeatTimeout(t *testing.T) {
	blockRequestCtx, unblockRequest := context.WithCancel(context.Background())
	defer unblockRequest()
	// 创建一个会阻塞的假 WebSocket 服务端。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
		<-blockRequestCtx.Done()
	}))
	defer websocketServer.Close()
	// 创建连接到假服务端的 websocket 客户端。
	websocketServer.URL = websocketServer.URL + "?" + "stdin=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	streamExec := exec.(*wsStreamExecutor)
	// ping 周期大于 ping 期限，强制触发超时。
	pingPeriod := wait.ForeverTestTimeout // 让心跳期限在未续期的情况下过期
	pingDeadline := time.Second           // 给建立流预留 1 秒准备时间
	streamExec.heartbeatPeriod = pingPeriod
	streamExec.heartbeatDeadline = pingDeadline
	// 通过 STDIN 向 websocket 服务端发送一些随机数据。
	randomData := make([]byte, 128)
	if _, err := rand.Read(randomData); err != nil {
		t.Errorf("unexpected error reading random data: %v", err)
	}
	options := &StreamOptions{
		Stdin: bytes.NewReader(randomData),
	}
	errorChan := make(chan error)
	go func() {
		// 启动 WebSocket "exec" 客户端上的流式传输。
		errorChan <- streamExec.StreamWithContext(context.Background(), *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expected heartbeat timeout, got none.")
	case err := <-errorChan:
		// 预期心跳超时错误。
		if err == nil {
			t.Fatalf("expected error but received none")
		}
		if !strings.Contains(err.Error(), "i/o timeout") {
			t.Errorf("expected heartbeat timeout error, got (%s)", err)
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
}

// TestWebSocketClient_TextMessageTypeError 验证服务端返回 TextMessage（协议要求 BinaryMessage）时，
// 客户端会报 "unexpected message type"。
func TestWebSocketClient_TextMessageTypeError(t *testing.T) {
	var upgrader = gwebsocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 接受所有请求
		},
		Subprotocols: []string{remotecommand.StreamProtocolV5Name},
	}
	// 升级原始 websocket 服务端连接，返回错误的消息类型 "TextMessage"。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("unable to upgrade to create websocket connection: %v", err)
		}
		defer conn.Close()
		msg := []byte("test message with wrong message type.")
		stdOutMsg := append([]byte{remotecommand.StreamStdOut}, msg...)
		// 错误的消息类型 "TextMessage"。
		err = conn.WriteMessage(gwebsocket.TextMessage, stdOutMsg)
		if err != nil {
			t.Fatalf("error writing text message to websocket: %v", err)
		}

	}))
	defer websocketServer.Close()

	// 配置带 STDOUT 流的 websocket 客户端。
	websocketServer.URL = websocketServer.URL + "?" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
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
		// 预期消息类型错误。
		if err == nil {
			t.Fatalf("expected error but received none")
		}
		if !strings.Contains(err.Error(), "unexpected message type") {
			t.Errorf("expected bad message type error, got (%s)", err)
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
}

// TestWebSocketClient_EmptyMessageHandled 验证收到完全空的消息（连 stream id 都没有）时，
// 读取会失败并走清理流程，而不是 panic 或挂起。
func TestWebSocketClient_EmptyMessageHandled(t *testing.T) {
	var upgrader = gwebsocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 接受所有请求
		},
		Subprotocols: []string{remotecommand.StreamProtocolV5Name},
	}
	// 升级原始 websocket 服务端连接，返回错误的消息类型 "TextMessage"。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("unable to upgrade to create websocket connection: %v", err)
		}
		defer conn.Close()
		// 发送完全空的消息，连最初的 stream id 也没有。
		conn.WriteMessage(gwebsocket.BinaryMessage, []byte{}) //nolint:errcheck
	}))
	defer websocketServer.Close()

	// 配置带 STDOUT 流的 websocket 客户端。
	websocketServer.URL = websocketServer.URL + "?" + "stdout=true"
	websocketLocation, err := url.Parse(websocketServer.URL)
	if err != nil {
		t.Fatalf("Unable to parse WebSocket server URL: %s", websocketServer.URL)
	}
	exec, err := NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", websocketServer.URL)
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	var stdout bytes.Buffer
	options := &StreamOptions{
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
		// 预期读取初始 stream id 时出错。
		if err == nil {
			t.Fatalf("expected error but received none")
		}
		if !strings.Contains(err.Error(), "read stream id") {
			t.Errorf("expected error reading stream id, got (%s)", err)
		}
		// 校验协商到的是远程命令 v5 协议。
		streamExec := exec.(*wsStreamExecutor)
		if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
			t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
		}
	}
}

// TestWebSocketClient_ExecutorErrors 验证构造/使用 executor 的错误路径：
// 非法认证配置导致 transport 创建失败，nil context 导致 StreamWithContext 报错。
func TestWebSocketClient_ExecutorErrors(t *testing.T) {
	// 非法配置会导致 websocket executor 构造函数创建 transport 出错。
	config := rest.Config{
		ExecProvider: &clientcmdapi.ExecConfig{},
		AuthProvider: &clientcmdapi.AuthProviderConfig{},
	}
	_, err := NewWebSocketExecutor(&config, "GET", "http://localhost")
	if err == nil {
		t.Errorf("expecting executor constructor error, but received none.")
	} else if !strings.Contains(err.Error(), "error creating websocket transports") {
		t.Errorf("expecting error creating transports, got (%s)", err.Error())
	}
	// 验证 nil context 会让 StreamWithContext 报错
	exec, err := NewWebSocketExecutor(&rest.Config{}, "GET", "http://localhost")
	if err != nil {
		t.Errorf("unexpected error creating websocket executor: %v", err)
	}
	errorChan := make(chan error)
	go func() {
		// 启动 WebSocket "exec" 客户端上的流式传输。
		var ctx context.Context
		errorChan <- exec.StreamWithContext(ctx, StreamOptions{})
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		// 预期 nil context 错误。
		if err == nil {
			t.Fatalf("expected error but received none")
		}
		if !strings.Contains(err.Error(), "nil Context") {
			t.Errorf("expected nil context error, got (%s)", err)
		}
	}
}

// TestWebSocketClient_HeartbeatSucceeds 验证周期小于 deadline 时 ping/pong 心跳正常完成，
// 且 pong 携带的消息与 ping 一致。
func TestWebSocketClient_HeartbeatSucceeds(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	var upgrader = gwebsocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // 接受所有请求
		},
	}
	// 升级原始 websocket 服务端连接；它会自动回应 Ping。
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("unable to upgrade to create websocket connection: %v", err)
		}
		defer conn.Close()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}))
	defer websocketServer.Close()
	// 创建原始 websocket 客户端，连接到 websocket 服务端。
	url := strings.ReplaceAll(websocketServer.URL, "http", "ws")
	client, _, err := gwebsocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	// 用客户端 websocket 连接创建心跳并启动；"period" 小于 "deadline"，ping/pong 心跳会成功。
	var expectedMsg = "test heartbeat message"
	var period = 100 * time.Millisecond
	var deadline = 200 * time.Millisecond
	heartbeat := newHeartbeat(logger, client, period, deadline)
	heartbeat.setMessage(expectedMsg)
	// 给 handler 挂一个 channel 以取回 "pong" 消息。
	pongMsgCh := make(chan string)
	pongHandler := heartbeat.conn.PongHandler()
	heartbeat.conn.SetPongHandler(func(msg string) error {
		pongMsgCh <- msg
		return pongHandler(msg)
	})
	go heartbeat.start()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, _, err := client.ReadMessage()
			if err != nil {
				t.Logf("client err reading message: %v", err)
				return
			}
		}
	}()

	select {
	case actualMsg := <-pongMsgCh:
		close(heartbeat.closer)
		// 校验收到的 pong 消息与 ping 发送的一致。
		if expectedMsg != actualMsg {
			t.Errorf("expected received pong message (%s), got (%s)", expectedMsg, actualMsg)
		}
	case <-time.After(period * 4):
		// 该分支不应发生。
		close(heartbeat.closer)
		t.Errorf("unexpected heartbeat timeout")
	}
	wg.Wait()
}

// TestLateStreamCreation 验证关闭所有流读取器之后不允许再创建新流，
// 防止资源已清理后仍有流被注册而泄漏。
func TestLateStreamCreation(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	c := newWSStreamCreator(logger, nil)
	c.closeAllStreamReaders(nil)
	if err := c.setStream(0, nil); err == nil {
		t.Fatal("expected error adding stream after closeAllStreamReaders")
	}
}

// TestWebSocketClient_StreamsAndExpectedErrors 覆盖 wsStreamCreator 的流创建与各类错误：
// 未知流类型、重复注册、关闭后读写等。
func TestWebSocketClient_StreamsAndExpectedErrors(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	// 校验 Stream 相关函数。
	c := newWSStreamCreator(logger, nil)
	headers := http.Header{}
	headers.Set(v1.StreamType, v1.StreamTypeStdin)
	s, err := c.CreateStream(headers)
	if err != nil {
		t.Errorf("unexpected stream creation error: %v", err)
	}
	expectedStreamID := uint32(remotecommand.StreamStdIn)
	actualStreamID := s.Identifier()
	if expectedStreamID != actualStreamID {
		t.Errorf("expecting stream id (%d), got (%d)", expectedStreamID, actualStreamID)
	}
	actualHeaders := s.Headers()
	if !reflect.DeepEqual(headers, actualHeaders) {
		t.Errorf("expecting stream headers (%v), got (%v)", headers, actualHeaders)
	}
	// 校验流 reset 不返回错误。
	err = s.Reset()
	if err != nil {
		t.Errorf("unexpected error in stream reset: %v", err)
	}
	// 校验连接为 nil 时 Close 会报错。
	err = s.Close()
	if err == nil {
		t.Errorf("expecting stream Close error, but received none")
	}
	if !strings.Contains(err.Error(), "Close() on already closed stream") {
		t.Errorf("expected stream close error, got (%s)", err)
	}
	// 校验连接为 nil 时 Write 会报错。
	n, err := s.Write([]byte("not written"))
	if n != 0 {
		t.Errorf("expected zero bytes written, wrote (%d) instead", n)
	}
	if err == nil {
		t.Errorf("expecting stream Write error, but received none")
	}
	if !strings.Contains(err.Error(), "write on closed stream") {
		t.Errorf("expected stream write error, got (%s)", err)
	}
	// 校验 CreateStream 的错误 —— 未知流
	headers = http.Header{}
	headers.Set(v1.StreamType, "UNKNOWN")
	_, err = c.CreateStream(headers)
	if err == nil {
		t.Errorf("expecting CreateStream error, but received none")
	} else if !strings.Contains(err.Error(), "unknown stream type") {
		t.Errorf("expecting unknown stream type error, got (%s)", err.Error())
	}
	// 校验 CreateStream 的错误 —— 重复流
	headers.Set(v1.StreamType, v1.StreamTypeError)
	c.streams[remotecommand.StreamErr] = &stream{}
	_, err = c.CreateStream(headers)
	if err == nil {
		t.Errorf("expecting CreateStream error, but received none")
	} else if !strings.Contains(err.Error(), "duplicate stream") {
		t.Errorf("expecting duplicate stream error, got (%s)", err.Error())
	}
}

// options 描述一次 exec/attach 需要哪些流。
type options struct {
	stdin  bool
	stdout bool
	stderr bool
	tty    bool
}

// streamOptionsFromRequest 从请求 query 中解析出需要开启的流。
func streamOptionsFromRequest(req *http.Request) *options {
	query := req.URL.Query()
	tty := query.Get("tty") == "true"
	stdin := query.Get("stdin") == "true"
	stdout := query.Get("stdout") == "true"
	stderr := query.Get("stderr") == "true"
	return &options{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		tty:    tty,
	}
}

// websocketStreams 汇总服务端一侧的连接与各数据流，便于测试中读写。
type websocketStreams struct {
	conn         io.Closer
	stdinStream  io.ReadCloser
	stdoutStream io.WriteCloser
	stderrStream io.WriteCloser
	writeStatus  func(status *apierrors.StatusError) error
	resizeStream io.ReadCloser
	resizeChan   chan TerminalSize
	tty          bool
}

// webSocketServerStreams 建立服务端流；若请求了 resize 流，额外启动协程解码窗口大小事件。
func webSocketServerStreams(req *http.Request, w http.ResponseWriter, opts *options) (*websocketStreams, error) {
	conn, err := createWebSocketStreams(req, w, opts)
	if err != nil {
		return nil, err
	}

	if conn.resizeStream != nil {
		conn.resizeChan = make(chan TerminalSize)
		go handleResizeEvents(req.Context(), conn.resizeStream, conn.resizeChan)
	}

	return conn, nil
}

// handleResizeEvents 持续解码 resize 事件并发送到 channel；
// 流结束或请求 context 结束时退出并关闭 channel。
func handleResizeEvents(ctx context.Context, stream io.Reader, channel chan<- TerminalSize) {
	defer close(channel)

	decoder := json.NewDecoder(stream)
	for {
		size := TerminalSize{}
		if err := decoder.Decode(&size); err != nil {
			break
		}

		select {
		case channel <- size:
		case <-ctx.Done():
			// 为避免协程泄漏，http 请求结束后就退出；若进程启动失败、没有消费者
			// 处理这些 resize 事件，通常会走到该分支。
			return
		}
	}
}

// createChannels 按 remotecommand 的固定通道编号构造通道类型表；
// error 与 resize 通道始终开启。
func createChannels(opts *options) []wsstream.ChannelType {
	// 打开请求的通道，并始终打开 error 通道
	channels := make([]wsstream.ChannelType, 5)
	channels[remotecommand.StreamStdIn] = readChannel(opts.stdin)
	channels[remotecommand.StreamStdOut] = writeChannel(opts.stdout)
	channels[remotecommand.StreamStdErr] = writeChannel(opts.stderr)
	channels[remotecommand.StreamErr] = wsstream.WriteChannel
	channels[remotecommand.StreamResize] = wsstream.ReadChannel
	return channels
}

// readChannel 在 real 为 true 时返回 wsstream.ReadChannel，否则返回 wsstream.IgnoreChannel。
func readChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.ReadChannel
	}
	return wsstream.IgnoreChannel
}

// writeChannel 在 real 为 true 时返回 wsstream.WriteChannel，否则返回 wsstream.IgnoreChannel。
func writeChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.WriteChannel
	}
	return wsstream.IgnoreChannel
}

// createWebSocketStreams 返回包含 websocket 连接及 exec/attach 所需流的 websocketStreams。
func createWebSocketStreams(req *http.Request, w http.ResponseWriter, opts *options) (*websocketStreams, error) {
	channels := createChannels(opts)
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		remotecommand.StreamProtocolV5Name: {
			Binary:   true,
			Channels: channels,
		},
	})
	conn.SetIdleTimeout(4 * time.Hour)
	// 打开连接即响应 WebSocket 客户端，协商 WebSocket 升级连接与子协议。
	_, streams, err := conn.Open(w, req)
	if err != nil {
		return nil, err
	}

	// 向最靠前的可写通道发送一个空消息；客户端以收到该空消息作为“连接已建立”的信号。
	//nolint:errcheck
	switch {
	case opts.stdout:
		streams[remotecommand.StreamStdOut].Write([]byte{})
	case opts.stderr:
		streams[remotecommand.StreamStdErr].Write([]byte{})
	default:
		streams[remotecommand.StreamErr].Write([]byte{})
	}

	wsStreams := &websocketStreams{
		conn:         conn,
		stdinStream:  streams[remotecommand.StreamStdIn],
		stdoutStream: streams[remotecommand.StreamStdOut],
		stderrStream: streams[remotecommand.StreamStdErr],
		tty:          opts.tty,
		resizeStream: streams[remotecommand.StreamResize],
	}

	// 把状态错误序列化为 JSON 写入 error 流。
	wsStreams.writeStatus = func(stream io.Writer) func(status *apierrors.StatusError) error {
		return func(status *apierrors.StatusError) error {
			bs, err := json.Marshal(status.Status())
			if err != nil {
				return err
			}
			_, err = stream.Write(bs)
			return err
		}
	}(streams[remotecommand.StreamErr])

	return wsStreams, nil
}

// TestWebSocketClient_ProxySucceeds 验证配置了 HTTP/HTTPS 代理时，WebSocket exec 仍能
// 通过代理建立连接并完成 STDIN->STDOUT 回环。
func TestWebSocketClient_ProxySucceeds(t *testing.T) {
	// 校验对每种列举的协议方案 websocket 代理都能成功。
	proxySchemes := []string{"http", "https"}
	for _, proxyScheme := range proxySchemes {
		// 创建代理 handler，并统计其被调用的次数。
		var proxyCalled atomic.Int64
		proxyHandler := utilnettesting.NewHTTPProxyHandler(t, func(req *http.Request) bool {
			proxyCalled.Add(1)
			return true
		})
		defer proxyHandler.Wait()
		// 创建并启动代理服务器，按 scheme 决定是否加入 TLS 功能。
		proxyServer := httptest.NewUnstartedServer(proxyHandler)
		if proxyScheme == "https" {
			cert, err := tls.X509KeyPair(localhostCert, localhostKey)
			if err != nil {
				t.Errorf("https (valid hostname): proxy_test: %v", err)
			}
			proxyServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
			proxyServer.StartTLS()
		} else {
			proxyServer.Start()
		}
		defer proxyServer.Close() //nolint:errcheck
		proxyLocation, err := url.Parse(proxyServer.URL)
		require.NoError(t, err)
		t.Logf("Proxy URL: %s", proxyLocation.String())

		// 创建假 WebSocket 服务端：把收到的 STDIN 数据复制回 STDOUT 流。
		websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			conns, err := webSocketServerStreams(req, w, streamOptionsFromRequest(req))
			if err != nil {
				t.Fatalf("error on webSocketServerStreams: %v", err)
			}
			defer conns.conn.Close() //nolint:errcheck
			// 将 STDIN 流回环到 STDOUT 流。
			_, err = io.Copy(conns.stdoutStream, conns.stdinStream)
			if err != nil {
				t.Fatalf("error copying STDIN to STDOUT: %v", err)
			}
		}))
		defer websocketServer.Close() //nolint:errcheck

		// 创建 WebSocket 客户端（executor），指向 TLS 代理服务器；
		// 代理服务器会向假 websocket 服务端打开 websocket 连接。
		websocketServer.URL = websocketServer.URL + "?" + "stdin=true" + "&" + "stdout=true"
		websocketLocation, err := url.Parse(websocketServer.URL)
		require.NoError(t, err)
		clientConfig := &rest.Config{
			Host: websocketLocation.Host,
			// "http" 方案时不会被使用。
			TLSClientConfig: rest.TLSClientConfig{CAData: localhostCert},
			Proxy: func(req *http.Request) (*url.URL, error) {
				return proxyLocation, nil
			},
		}
		exec, err := NewWebSocketExecutor(clientConfig, "GET", websocketServer.URL)
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
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验协商到的是远程命令 v5 协议。
			streamExec := exec.(*wsStreamExecutor)
			if remotecommand.StreamProtocolV5Name != streamExec.negotiated {
				t.Fatalf("expected remote command v5 protocol, got (%s)", streamExec.negotiated)
			}
		}
		data, err := io.ReadAll(bytes.NewReader(stdout.Bytes()))
		if err != nil {
			t.Fatalf("error reading the stream: %v", err)
		}
		// 检查 STDIN 发送的随机数据与 STDOUT 返回的数据一致。
		t.Logf("comparing %d random bytes sent data versus received", len(randomData))
		if !bytes.Equal(randomData, data) {
			t.Errorf("unexpected data received: %d sent: %d", len(data), len(randomData))
		} else {
			t.Log("success--random bytes are the same")
		}
		// 确保代理只被调用了一次
		if e, a := int64(1), proxyCalled.Load(); e != a {
			t.Errorf("expected %d proxy call, got %d", e, a)
		}
	}
}
