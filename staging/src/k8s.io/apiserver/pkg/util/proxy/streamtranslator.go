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

package proxy

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mxk/go-flowrate/flowrate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	constants "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/apiserver/pkg/util/proxy/metrics"
	"k8s.io/client-go/tools/remotecommand"
	clientspdy "k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream/spdy"
)

// StreamTranslatorHandler 把 WebSocket 流数据转换为 SPDY，
// 以代理到 kubelet（及 ContainerRuntime）。
//
// 这是 exec/attach/port-forward 迁移到 WebSocket 协议期间的兼容层：
// 客户端用 WebSocket 连到 apiserver，apiserver 再以 SPDY 连到 kubelet。
type StreamTranslatorHandler struct {
	// Location 是上游代理的位置，用作升级请求时向上游服务器拨号的地址。
	//
	Location *url.URL
	// Transport 提供可选的 round tripper 用于代理。为 nil 时使用默认代理传输。
	Transport http.RoundTripper
	// MaxBytesPerSec 在需要时对流 Reader/Writer 限速。
	// 0 表示不限速。限速作用在单个流上，而非底层连接。
	MaxBytesPerSec int64
	// Options 定义请求的流（如 stdin、stdout）。
	Options Options
}

// NewStreamTranslatorHandler 创建一个新的代理 handler。Responder 是必需的，
// 用于把错误返回给调用方。
func NewStreamTranslatorHandler(location *url.URL, transport http.RoundTripper, maxBytesPerSec int64, opts Options) *StreamTranslatorHandler {
	return &StreamTranslatorHandler{
		Location:       location,
		Transport:      transport,
		MaxBytesPerSec: maxBytesPerSec,
		Options:        opts,
	}
}

// ServeHTTP 升级进入的 WebSocket 连接，并桥接到上游 SPDY 流。
func (h *StreamTranslatorHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 创建 WebSocket server，并接入请求指定的各个流。如果该 websocket 端点无法完成升级，
	// websocket 库会把错误返回给客户端：升级失败视为客户端错误（400），
	// 此时还没有可用的 websocket 流来回写状态。
	websocketStreams, err := webSocketServerStreams(req, w, h.Options)
	if err != nil {
		// 客户端错误会增加 bad request 状态码。
		metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusBadRequest))
		return
	}
	defer websocketStreams.conn.Close()

	// 创建 SPDY executor，并确保不跟随重定向：Location 指向集群内 kubelet，被重定向到任意
	// 地址会变成 SSRF 通道。
	spdyRoundTripper, err := spdy.NewRoundTripperWithConfig(spdy.RoundTripperConfig{UpgradeTransport: h.Transport, PingPeriod: 5 * time.Second})
	if err != nil {
		metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusInternalServerError))
		websocketStreams.writeStatus(apierrors.NewInternalError(err)) //nolint:errcheck
		return
	}
	spdyExecutor, err := remotecommand.NewSPDYExecutorRejectRedirects(
		spdyRoundTripper,
		clientspdy.NewUpgraderForStreaming(spdyRoundTripper),
		"POST",
		h.Location,
	)
	if err != nil {
		metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusInternalServerError))
		websocketStreams.writeStatus(apierrors.NewInternalError(err)) //nolint:errcheck
		return
	}

	// 把 WebSocket server 流的输出接到 SPDY client 的输入。当传输速率超过 "MaxBytesPerSec"
	// 时（零表示未设置）会对 stdin/stdout/stderr 流限速；对各个流而不是底层连接限速，
	// 在两个同向流（例如 stdout、stderr）同时被打满时表现*可能*不同。
	// 方向说明：stdin 是 websocket -> SPDY，因此用 Reader 限速；stdout/stderr
	// 是 SPDY -> websocket，因此用 Writer 限速。
	opts := remotecommand.StreamOptions{}
	if h.Options.Stdin {
		stdin := websocketStreams.stdinStream
		if h.MaxBytesPerSec > 0 {
			stdin = flowrate.NewReader(stdin, h.MaxBytesPerSec)
		}
		opts.Stdin = stdin
	}
	if h.Options.Stdout {
		stdout := websocketStreams.stdoutStream
		if h.MaxBytesPerSec > 0 {
			stdout = flowrate.NewWriter(stdout, h.MaxBytesPerSec)
		}
		opts.Stdout = stdout
	}
	if h.Options.Stderr {
		stderr := websocketStreams.stderrStream
		if h.MaxBytesPerSec > 0 {
			stderr = flowrate.NewWriter(stderr, h.MaxBytesPerSec)
		}
		opts.Stderr = stderr
	}
	if h.Options.Tty {
		// TTY 模式下必须转发终端尺寸，否则远端程序无法正确渲染。
		opts.Tty = true
		opts.TerminalSizeQueue = &translatorSizeQueue{resizeChan: websocketStreams.resizeChan}
	}
	// 启动已连接流的 SPDY 客户端。WebSocket 服务端流的输出
	// 会被转发到 SPDY 客户端。SPDY 执行错误
	// 通过 websocket 错误流上报。
	err = spdyExecutor.StreamWithContext(req.Context(), opts)
	if err != nil {
		//nolint:errcheck   // Ignore writeStatus returned error
		if statusErr, ok := err.(*apierrors.StatusError); ok {
			// 增加 status error 中返回的状态码。
			metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(int(statusErr.Status().Code)))
			websocketStreams.writeStatus(statusErr)
		} else if exitErr, ok := err.(exec.CodeExitError); ok && exitErr.Exited() {
			// 容器返回了退出码，因此不是
			// 流转换器的错误——给指标加上 StatusOK。
			metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusOK))
			websocketStreams.writeStatus(codeExitToStatusError(exitErr))
		} else {
			metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusInternalServerError))
			websocketStreams.writeStatus(apierrors.NewInternalError(err))
		}
		return
	}

	metrics.IncStreamTranslatorRequest(req.Context(), strconv.Itoa(http.StatusOK))
	// 把成功状态写回 WebSocket 客户端。
	//nolint:errcheck
	websocketStreams.writeStatus(&apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusSuccess,
	}})
}

// translatorSizeQueue 把 WebSocket resizeChan 的大小事件
// 送入 SPDY 客户端输入。实现 TerminalSizeQueue
// 接口。
//
// resizeChan 关闭时返回 nil，这是 TerminalSizeQueue 约定“没有更多尺寸更新”的信号。
type translatorSizeQueue struct {
	resizeChan chan remotecommand.TerminalSize
}

func (t *translatorSizeQueue) Next() *remotecommand.TerminalSize {
	size, ok := <-t.resizeChan
	if !ok {
		return nil
	}
	return &size
}

// codeExitToStatusError 把传入的 CodeExitError 转换为
// 可通过 writeStatus 在错误流上发送的类型。
//
// 容器内命令以非零码退出不是 apiserver 的错误，因此以 StatusFailure 加
// NonZeroExitCodeReason 回传，客户端 kubectl 据此返回相同退出码。
func codeExitToStatusError(exitErr exec.CodeExitError) *apierrors.StatusError {
	rc := exitErr.ExitStatus()
	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status: metav1.StatusFailure,
			Reason: constants.NonZeroExitCodeReason,
			Details: &metav1.StatusDetails{
				Causes: []metav1.StatusCause{
					{
						Type:    constants.ExitCodeCauseType,
						Message: fmt.Sprintf("%d", rc),
					},
				},
			},
			Message: fmt.Sprintf("command terminated with non-zero exit code: %v", exitErr),
		},
	}
}
