/*
Copyright 2025 The Kubernetes Authors.

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

	restclient "k8s.io/client-go/rest"
	remoteclient "k8s.io/client-go/tools/remotecommand"
	clientspdy "k8s.io/client-go/transport/spdy"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	streaming "k8s.io/cri-streaming/pkg/streaming"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
	streamingspdy "k8s.io/streaming/pkg/httpstream/spdy"
)

// 以下常量定义 CRI streaming 测试中使用的容器 ID 与标准输入/输出数据，
// 供 fake runtime 与客户端断言共同引用，确保收发两端数据一致。
const (
	execContainerID   = "cri-streaming-exec-container"
	attachContainerID = "cri-streaming-attach-container"

	// 以下三组分别是 exec 与 attach 的标准输入、标准输出与标准错误数据。
	execInput  = "exec-stdin"
	execOutput = "exec-stdout"
	execErr    = "exec-stderr"

	attachInput  = "attach-stdin"
	attachOutput = "attach-stdout"
	attachErr    = "attach-stderr"
)

// fakeStreamingRuntime 实现 streaming 服务端所需的 Runtime 接口，
// 仅提供 Exec/Attach 的最小可用行为，用于验证 SPDY 流通道是否打通。
type fakeStreamingRuntime struct{}

// Exec 校验容器 ID 与标准输入，然后向输出流写回预期的数据，
// 使客户端能据此断言整个 SPDY 流式链路工作正常。
func (*fakeStreamingRuntime) Exec(ctx context.Context, containerID string, cmd []string, in io.Reader, out, errStream io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	if containerID != execContainerID {
		return fmt.Errorf("unexpected exec container ID: %q", containerID)
	}

	stdinData, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if string(stdinData) != execInput {
		return fmt.Errorf("unexpected exec stdin: %q", string(stdinData))
	}

	// 依次写入 stdout 与 stderr，验证两个独立通道都被正确建立。
	if _, err := io.WriteString(out, execOutput); err != nil {
		return err
	}
	if _, err := io.WriteString(errStream, execErr); err != nil {
		return err
	}

	return nil
}

// Attach 与 Exec 类似：先校验容器 ID 与标准输入，再向输出流写回预期数据。
func (*fakeStreamingRuntime) Attach(ctx context.Context, containerID string, in io.Reader, out, errStream io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	if containerID != attachContainerID {
		return fmt.Errorf("unexpected attach container ID: %q", containerID)
	}

	stdinData, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if string(stdinData) != attachInput {
		return fmt.Errorf("unexpected attach stdin: %q", string(stdinData))
	}

	if _, err := io.WriteString(out, attachOutput); err != nil {
		return err
	}
	if _, err := io.WriteString(errStream, attachErr); err != nil {
		return err
	}

	return nil
}

// PortForward 本测试不涉及端口转发，故显式报错防止被误用。
func (*fakeStreamingRuntime) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	return errors.New("not implemented")
}

// newCRIStreamingTestServer 搭建一个真实的 HTTP 测试服务器并把 CRI streaming
// 服务端挂到其上。server 变量先声明后赋值，是因为 HTTP handler 通过闭包引用它，
// 这样 handler 中始终使用的是最终创建好的 streaming.Server 实例。
func newCRIStreamingTestServer(t *testing.T) (streaming.Server, *httptest.Server) {
	t.Helper()

	var server streaming.Server
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.ServeHTTP(w, r)
	}))

	// streaming 服务端需要基于自身 BaseURL 生成 Exec/Attach URL，
	// 因此用测试服务器实际监听的地址来配置它。
	baseURL, err := url.Parse(httpServer.URL)
	if err != nil {
		httpServer.Close()
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	config := streaming.DefaultConfig
	config.BaseURL = baseURL

	server, err = streaming.NewServer(config, &fakeStreamingRuntime{})
	if err != nil {
		// 创建失败时主动关闭已启动的 HTTP 服务器，避免端口泄漏。
		httpServer.Close()
		t.Fatalf("failed to create cri streaming server: %v", err)
	}

	return server, httpServer
}

// TestCRIStreamingSPDYExecAttachCompatibility 验证 client-go 的 SPDY executor
// 能够与从仓库中拆出的 cri-streaming 服务端正常完成 Exec 与 Attach 两个端点的流式传输，
// 确保两者即使分别维护也能保持协议兼容。
func TestCRIStreamingSPDYExecAttachCompatibility(t *testing.T) {
	server, httpServer := newCRIStreamingTestServer(t)
	defer httpServer.Close()

	// 用 go-cmp 风格的独立子测试分别覆盖 Exec 与 Attach，
	// 任一失败不影响另一分支的定位。
	t.Run("exec", func(t *testing.T) {
		// 先向服务端请求 Exec URL；该 URL 指向真实的 httptest 服务器，
		// 后续 executor 将基于它发起 SPDY 升级请求。
		response, err := server.GetExec(&runtimeapi.ExecRequest{
			ContainerId: execContainerID,
			Cmd:         []string{"echo", "exec"},
			Stdin:       true,
			Stdout:      true,
			Stderr:      true,
		})
		if err != nil {
			t.Fatalf("failed to get exec URL: %v", err)
		}

		requestURL, err := url.Parse(response.Url)
		if err != nil {
			t.Fatalf("failed to parse exec URL: %v", err)
		}

		executor, err := remoteclient.NewSPDYExecutor(&restclient.Config{Host: requestURL.Host}, "POST", requestURL)
		if err != nil {
			t.Fatalf("failed to build exec executor: %v", err)
		}

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		// 超时保护：流式请求涉及多路复用与 goroutine 协作，
		// 一旦死锁不至于让整个测试永久挂起。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = executor.StreamWithContext(ctx, remoteclient.StreamOptions{
			Stdin:  strings.NewReader(execInput),
			Stdout: &stdout,
			Stderr: &stderr,
		})
		if err != nil {
			t.Fatalf("unexpected exec stream error: %v", err)
		}

		// 校验 fake runtime 写回的 stdout/stderr 能被客户端正确接收，
		// 以此证明 SPDY 的多路 stream 通道均已打通。
		if got := stdout.String(); got != execOutput {
			t.Fatalf("unexpected exec stdout: %q", got)
		}
		if got := stderr.String(); got != execErr {
			t.Fatalf("unexpected exec stderr: %q", got)
		}
	})

	t.Run("attach", func(t *testing.T) {
		// 与 exec 分支同理：先取得 Attach URL，再驱动 client-go executor
		// 与服务端建立 SPDY 流式连接。
		response, err := server.GetAttach(&runtimeapi.AttachRequest{
			ContainerId: attachContainerID,
			Stdin:       true,
			Stdout:      true,
			Stderr:      true,
		})
		if err != nil {
			t.Fatalf("failed to get attach URL: %v", err)
		}

		requestURL, err := url.Parse(response.Url)
		if err != nil {
			t.Fatalf("failed to parse attach URL: %v", err)
		}

		executor, err := remoteclient.NewSPDYExecutor(&restclient.Config{Host: requestURL.Host}, "POST", requestURL)
		if err != nil {
			t.Fatalf("failed to build attach executor: %v", err)
		}

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		// 防止流式建立过程无限等待，超时后由 context 主动取消。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = executor.StreamWithContext(ctx, remoteclient.StreamOptions{
			Stdin:  strings.NewReader(attachInput),
			Stdout: &stdout,
			Stderr: &stderr,
		})
		if err != nil {
			t.Fatalf("unexpected attach stream error: %v", err)
		}

		// 断言 fake runtime 的回显数据经 SPDY 各通道正确送达客户端。
		if got := stdout.String(); got != attachOutput {
			t.Fatalf("unexpected attach stdout: %q", got)
		}
		if got := stderr.String(); got != attachErr {
			t.Fatalf("unexpected attach stderr: %q", got)
		}
	})
}

// TestCRIStreamingSPDYExecCompatibilityWithStreamingUpgraderAdapter 验证
// client-go 的兼容性 upgrader 适配路径：用 k8s.io/streaming 构造 SPDY
// roundtripper，再通过适配器将其包装成 client-go remotecommand 的 executor，
// 确保新旧两套传输实现可以互操作。
func TestCRIStreamingSPDYExecCompatibilityWithStreamingUpgraderAdapter(t *testing.T) {
	server, httpServer := newCRIStreamingTestServer(t)
	defer httpServer.Close()

	// 取得目标 Exec URL，作为适配后 executor 的连接地址。
	response, err := server.GetExec(&runtimeapi.ExecRequest{
		ContainerId: execContainerID,
		Cmd:         []string{"echo", "exec"},
		Stdin:       true,
		Stdout:      true,
		Stderr:      true,
	})
	if err != nil {
		t.Fatalf("failed to get exec URL: %v", err)
	}

	requestURL, err := url.Parse(response.Url)
	if err != nil {
		t.Fatalf("failed to parse exec URL: %v", err)
	}

	// 用 streaming 包构造 SPDY roundtripper，它是协议兼容的关键：
	// 既要能发起 SPDY 升级，也要提供心跳以维持长连接。
	roundTripper, err := streamingspdy.NewRoundTripperWithConfig(streamingspdy.RoundTripperConfig{
		PingPeriod: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to build streaming roundtripper: %v", err)
	}

	// 通过 NewUpgraderForStreaming 适配成 client-go 期望的 upgrader 接口，
	// 从而验证跨包组合后的 executor 依然可用。
	executor, err := remoteclient.NewSPDYExecutorForTransports(
		roundTripper,
		clientspdy.NewUpgraderForStreaming(roundTripper),
		"POST",
		requestURL,
	)
	if err != nil {
		t.Fatalf("failed to build adapter-based executor: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = executor.StreamWithContext(ctx, remoteclient.StreamOptions{
		Stdin:  strings.NewReader(execInput),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("unexpected exec stream error: %v", err)
	}

	// 最终断言与直接使用 client-go executor 的用例一致：
	// 收发数据必须完全匹配，说明适配层没有破坏流语义。
	if got := stdout.String(); got != execOutput {
		t.Fatalf("unexpected exec stdout: %q", got)
	}
	if got := stderr.String(); got != execErr {
		t.Fatalf("unexpected exec stderr: %q", got)
	}
}
