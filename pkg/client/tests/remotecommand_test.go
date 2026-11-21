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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	remotecommandconsts "k8s.io/apimachinery/pkg/util/remotecommand"
	restclient "k8s.io/client-go/rest"
	remoteclient "k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/cri-streaming/pkg/streaming/remotecommand"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/streaming/pkg/httpstream"
)

// fakeExecutor 模拟远程命令执行器，实现 remotecommand 服务端所需的接口。
// 它校验请求参数，按配置输出 stdout/stderr，并记录收到的 stdin 供测试断言。
type fakeExecutor struct {
	t        *testing.T
	testName string
	// errorData 非空时直接返回该错误，用于模拟执行失败。
	errorData string
	// stdoutData 与 stderrData 是每次写入对应输出流的数据。
	stdoutData string
	stderrData string
	// expectStdin 表示是否期望收到 stdin 数据；stdinReceived 记录实际收到的内容。
	expectStdin   bool
	stdinReceived bytes.Buffer
	tty           bool
	// messageCount 是输出数据的重复次数，用于验证多条消息。
	messageCount int
	// command 记录客户端传入的命令；exec 区分 Exec(true) 与 Attach(false)。
	command []string
	exec    bool
}

// ExecInContainer 实现 Exec 接口，将调用转发到共用的 run 逻辑，并传入命令。
func (ex *fakeExecutor) ExecInContainer(_ context.Context, name string, uid string, container string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize, timeout time.Duration) error {
	return ex.run(name, uid, container, cmd, in, out, err, tty)
}

// AttachContainer 实现 Attach 接口，与 Exec 共用 run 逻辑，但无命令参数。
func (ex *fakeExecutor) AttachContainer(_ context.Context, name string, uid string, container string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	return ex.run(name, uid, container, nil, in, out, err, tty)
}

// run 是 Exec/Attach 的共用实现：先校验 pod、uid 及命令等参数，然后按配置
// 输出数据、读取 stdin。任何不符预期之处都通过 testing.T 记录为失败。
func (ex *fakeExecutor) run(name string, uid string, container string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool) error {
	// 记录命令与 tty 标志，供 run 内部及测试后续校验。
	ex.command = cmd
	ex.tty = tty

	if e, a := "pod", name; e != a {
		ex.t.Errorf("%s: pod: expected %q, got %q", ex.testName, e, a)
	}
	if e, a := "uid", uid; e != a {
		ex.t.Errorf("%s: uid: expected %q, got %q", ex.testName, e, a)
	}
	if ex.exec {
		if e, a := "ls /", strings.Join(ex.command, " "); e != a {
			ex.t.Errorf("%s: command: expected %q, got %q", ex.testName, e, a)
		}
	} else {
		if len(ex.command) > 0 {
			ex.t.Errorf("%s: command: expected nothing, got %v", ex.testName, ex.command)
		}
	}

	// 配置了 errorData 时优先模拟执行失败，不产生任何输出。
	if len(ex.errorData) > 0 {
		return errors.New(ex.errorData)
	}

	// 按 messageCount 重复写入，验证客户端能连续接收多条消息。
	if len(ex.stdoutData) > 0 {
		for i := 0; i < ex.messageCount; i++ {
			fmt.Fprint(out, ex.stdoutData)
		}
	}

	if len(ex.stderrData) > 0 {
		for i := 0; i < ex.messageCount; i++ {
			fmt.Fprint(err, ex.stderrData)
		}
	}

	// 仅在用例期望时读取 stdin，避免在无 stdin 的用例中阻塞等待。
	if ex.expectStdin {
		io.Copy(&ex.stdinReceived, in)
	}

	return nil
}

// fakeServer 构造一个 HTTP handler，根据 exec 标志分别调用 ServeExec 或
// ServeAttach 建立流式服务端，并在请求处理完毕后校验收到的 stdin。
// requestReceived 用于通知主测试 goroutine：请求已被服务端接收。
func fakeServer(t *testing.T, requestReceived chan struct{}, testName string, exec bool, stdinData, stdoutData, stderrData, errorData string, tty bool, messageCount int, serverProtocols []string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		executor := &fakeExecutor{
			t:            t,
			testName:     testName,
			errorData:    errorData,
			stdoutData:   stdoutData,
			stderrData:   stderrData,
			expectStdin:  len(stdinData) > 0,
			tty:          tty,
			messageCount: messageCount,
			exec:         exec,
		}

		// 从请求中解析流选项（如协议版本），失败则直接终止本 handler。
		opts, err := remotecommand.NewOptions(req)
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		if exec {
			// Exec 需要额外的命令参数，从查询串中取得后交给 ServeExec。
			cmd := req.URL.Query()[api.ExecCommandParam]
			remotecommand.ServeExec(w, req, executor, "pod", "uid", "container", cmd, opts, 0, 10*time.Second, serverProtocols)
		} else {
			// Attach 不涉及命令，直接交给 ServeAttach。
			remotecommand.ServeAttach(w, req, executor, "pod", "uid", "container", opts, 0, 10*time.Second, serverProtocols)
		}

		// 服务端流结束后校验收到的 stdin 与期望值完全一致。
		if e, a := strings.Repeat(stdinData, messageCount), executor.stdinReceived.String(); e != a {
			t.Errorf("%s: stdin: expected %q, got %q", testName, e, a)
		}
		close(requestReceived)
	})
}

// TestStream 对 Exec 与 Attach 分别验证流式传输的各类场景：
// 执行错误、标准输入输出、超大 stdin 以及 TTY 模式，
// 并检查客户端与服务端协议列表组合下的兼容行为。
func TestStream(t *testing.T) {
	testCases := []struct {
		TestName        string
		Stdin           string
		Stdout          string
		Stderr          string
		Error           string
		Tty             bool
		MessageCount    int
		ClientProtocols []string
		ServerProtocols []string
	}{
		{
			TestName:        "error",
			Error:           "bail",
			Stdout:          "a",
			ClientProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
			ServerProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
		},
		{
			TestName:        "in/out/err",
			Stdin:           "a",
			Stdout:          "b",
			Stderr:          "c",
			MessageCount:    100,
			ClientProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
			ServerProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
		},
		{
			TestName:        "oversized stdin",
			Stdin:           strings.Repeat("a", 20*1024*1024),
			Stdout:          "b",
			Stderr:          "",
			MessageCount:    1,
			ClientProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
			ServerProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
		},
		{
			TestName:        "in/out/tty",
			Stdin:           "a",
			Stdout:          "b",
			Tty:             true,
			MessageCount:    100,
			ClientProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
			ServerProtocols: []string{remotecommandconsts.StreamProtocolV2Name},
		},
	}

	for _, testCase := range testCases {
		// 每个场景都跑 Exec 与 Attach 两种模式，确保两条服务端路径都被覆盖。
		for _, exec := range []bool{true, false} {
			var name string
			if exec {
				name = testCase.TestName + " (exec)"
			} else {
				name = testCase.TestName + " (attach)"
			}

			t.Run(name, func(t *testing.T) {
				var (
					streamIn             io.Reader
					streamOut, streamErr io.Writer
				)
				localOut := &bytes.Buffer{}
				localErr := &bytes.Buffer{}

				requestReceived := make(chan struct{})
				server := httptest.NewServer(fakeServer(t, requestReceived, name, exec, testCase.Stdin, testCase.Stdout, testCase.Stderr, testCase.Error, testCase.Tty, testCase.MessageCount, testCase.ServerProtocols))
				defer server.Close()

				// 构造一个最简 RESTClient，只为生成带正确参数的请求 URL；
				// 真正的流式升级由下面的 SPDY executor 完成。
				url, _ := url.ParseRequestURI(server.URL)
				config := restclient.ClientContentConfig{
					GroupVersion: schema.GroupVersion{Group: "x"},
					Negotiator:   runtime.NewClientNegotiator(legacyscheme.Codecs.WithoutConversion(), schema.GroupVersion{Group: "x"}),
				}
				c, err := restclient.NewRESTClient(url, "", config, nil, nil)
				if err != nil {
					t.Fatalf("failed to create a client: %v", err)
				}
				req := c.Post().Resource("testing")

				if exec {
					// Exec 通过重复的 command 参数传递命令及其参数。
					req.Param("command", "ls")
					req.Param("command", "/")
				}

				if len(testCase.Stdin) > 0 {
					req.Param(api.ExecStdinParam, "1")
					streamIn = strings.NewReader(strings.Repeat(testCase.Stdin, testCase.MessageCount))
				}

				if len(testCase.Stdout) > 0 {
					req.Param(api.ExecStdoutParam, "1")
					streamOut = localOut
				}

				// TTY 模式与 stderr 互斥：启用 TTY 时服务端会把 stderr 合并进 stdout，
				// 因此不应再单独请求 stderr 通道。
				if testCase.Tty {
					req.Param(api.ExecTTYParam, "1")
				} else if len(testCase.Stderr) > 0 {
					req.Param(api.ExecStderrParam, "1")
					streamErr = localErr
				}

				conf := &restclient.Config{
					Host: server.URL,
				}
				transport, upgradeTransport, err := spdy.RoundTripperFor(conf)
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", name, err)
				}
				// 显式传入客户端支持的协议列表，与服务端协议列表组合，
				// 以验证协议协商能选出双方都支持的版本。
				e, err := remoteclient.NewSPDYExecutorForProtocols(transport, upgradeTransport, "POST", req.URL(), testCase.ClientProtocols...)
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", name, err)
				}
				err = e.StreamWithContext(context.Background(), remoteclient.StreamOptions{
					Stdin:  streamIn,
					Stdout: streamOut,
					Stderr: streamErr,
					Tty:    testCase.Tty,
				})
				hasErr := err != nil

				if len(testCase.Error) > 0 {
					// 期望执行出错：用 Contains 匹配，因为错误信息可能带有前缀或上下文。
					if !hasErr {
						t.Errorf("%s: expected an error", name)
					} else {
						if e, a := testCase.Error, err.Error(); !strings.Contains(a, e) {
							t.Errorf("%s: expected error stream read %q, got %q", name, e, a)
						}
					}
					return
				}

				if hasErr {
					t.Fatalf("%s: unexpected error: %v", name, err)
				}

				// 逐字节比对输出，MessageCount 次重复都必须完整无缺。
				if len(testCase.Stdout) > 0 {
					if e, a := strings.Repeat(testCase.Stdout, testCase.MessageCount), localOut; e != a.String() {
						t.Fatalf("%s: expected stdout data %q, got %q", name, e, a)
					}
				}

				if testCase.Stderr != "" {
					if e, a := strings.Repeat(testCase.Stderr, testCase.MessageCount), localErr; e != a.String() {
						t.Fatalf("%s: expected stderr data %q, got %q", name, e, a)
					}
				}

				// 确认请求确实到达服务端；带超时防止服务端 handler 卡死时无限等待。
				select {
				case <-requestReceived:
				case <-time.After(time.Minute):
					t.Errorf("%s: expected fakeServerInstance to receive request", name)
				}
			})
		}
	}
}

// fakeUpgrader 是一个可编程的 upgrader 桩：记录收到的请求与响应，
// 返回预设的连接或错误，并可选择性地校验传入的响应对象是否一致。
type fakeUpgrader struct {
	req           *http.Request
	resp          *http.Response
	conn          httpstream.Connection
	err, connErr  error
	checkResponse bool
	called        bool

	t *testing.T
}

// RoundTrip 记录请求并返回预设的响应与错误，标记自身已被调用。
func (u *fakeUpgrader) RoundTrip(req *http.Request) (*http.Response, error) {
	u.called = true
	u.req = req
	return u.resp, u.err
}

// NewConnection 返回预设的连接与错误；启用 checkResponse 时校验
// 传入的响应对象与 RoundTrip 返回的是同一个，确保对象在升级链路中被正确传递。
func (u *fakeUpgrader) NewConnection(resp *http.Response) (httpstream.Connection, error) {
	if u.checkResponse && u.resp != resp {
		u.t.Errorf("response objects passed did not match: %#v", resp)
	}
	return u.conn, u.connErr
}

// fakeConnection 通过嵌入 httpstream.Connection 接口，得到一个无需实现任何
// 方法的连接桩，仅用于在 Dial 中做对象身份比较。
type fakeConnection struct {
	httpstream.Connection
}

// TestDial 验证通用流式 upgrader 的 Dial 行为：这是所有基于流的 upgrader
// 共用的逻辑，与具体协议无关。它确保调用方可以脱离核心 Kubernetes 客户端
// 配置的默认行为，使用泛型的 stream executor 完成连接建立。
func TestDial(t *testing.T) {
	upgrader := &fakeUpgrader{
		t:             t,
		checkResponse: true,
		conn:          &fakeConnection{},
		resp: &http.Response{
			StatusCode: http.StatusSwitchingProtocols,
			Body:       io.NopCloser(&bytes.Buffer{}),
		},
	}
	dialer := spdy.NewDialerForStreaming(spdy.NewUpgraderForStreaming(upgrader), &http.Client{Transport: upgrader}, "POST", &url.URL{Host: "something.com", Scheme: "https"})
	conn, protocol, err := dialer.Dial("protocol1")
	if err != nil {
		t.Fatal(err)
	}
	if conn != upgrader.conn {
		t.Errorf("unexpected connection: %#v", conn)
	}
	if !upgrader.called {
		t.Errorf("request not called")
	}
	_ = protocol
}
