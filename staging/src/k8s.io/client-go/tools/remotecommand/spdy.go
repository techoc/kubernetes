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
	"context"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/util/remotecommand"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
)

// spdyStreamExecutor 负责通过 httpstream 连接传输标准 shell 流。
type spdyStreamExecutor struct {
	upgrader  spdy.Upgrader
	transport http.RoundTripper

	method    string
	url       *url.URL
	protocols []string
	// rejectRedirects 为 true 时把重定向视为错误；升级请求被重定向通常意味着
	// 请求打到了错误的端点，继续跟随可能把命令发到非预期服务器。
	rejectRedirects bool // 为 true 时，收到来自上游的重定向即视为错误
}

// NewSPDYExecutor 连接给定的服务端，并把连接升级为多路复用的双向流。
func NewSPDYExecutor(config *restclient.Config, method string, url *url.URL) (Executor, error) {
	wrapper, upgradeRoundTripper, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}
	return NewSPDYExecutorForTransports(wrapper, upgradeRoundTripper, method, url)
}

// NewSPDYExecutorRejectRedirects 返回一个 Executor：在调用 "Stream"（已弃用）
// 或 "StreamWithContext"（推荐）时，会把后续连接升级为 SPDY 双向流连接。
// 此外，如果在这些 "Stream" 调用尝试升级的过程中上游服务端返回重定向，
// 则返回错误。
func NewSPDYExecutorRejectRedirects(transport http.RoundTripper, upgrader spdy.Upgrader, method string, url *url.URL) (Executor, error) {
	executor, err := NewSPDYExecutorForTransports(transport, upgrader, method, url)
	if err != nil {
		return nil, err
	}
	spdyExecutor := executor.(*spdyStreamExecutor)
	spdyExecutor.rejectRedirects = true
	return spdyExecutor, nil
}

// NewSPDYExecutorForTransports 使用给定 transport 连接服务端，
// 并用给定 upgrader 把响应升级为多路复用的双向流。
//
// 按从新到旧的顺序声明协议，服务端选择第一个双方都支持的版本；
// V5 引入 CLOSE 信号，V4 支持退出码，更旧的版本为兼容保留。
func NewSPDYExecutorForTransports(transport http.RoundTripper, upgrader spdy.Upgrader, method string, url *url.URL) (Executor, error) {
	return NewSPDYExecutorForProtocols(
		transport, upgrader, method, url,
		remotecommand.StreamProtocolV5Name,
		remotecommand.StreamProtocolV4Name,
		remotecommand.StreamProtocolV3Name,
		remotecommand.StreamProtocolV2Name,
		remotecommand.StreamProtocolV1Name,
	)
}

// NewSPDYExecutorForProtocols 连接服务端，并仅使用给定的协议把连接升级为
// 多路复用的双向流。该函数为测试而暴露，多数调用方应使用
// NewSPDYExecutor 或 NewSPDYExecutorForTransports。
func NewSPDYExecutorForProtocols(transport http.RoundTripper, upgrader spdy.Upgrader, method string, url *url.URL, protocols ...string) (Executor, error) {
	return &spdyStreamExecutor{
		upgrader:  upgrader,
		transport: transport,
		method:    method,
		url:       url,
		protocols: protocols,
	}, nil
}

// Stream 向服务端打开一个协议流处理器并开始传输，直到客户端关闭连接
// 或服务端断开。
//
// 该版本没有 context，调用方无法取消；新代码应使用 StreamWithContext 以便
// 在取消时释放连接。
func (e *spdyStreamExecutor) Stream(options StreamOptions) error {
	return e.StreamWithContext(context.Background(), options)
}

// newConnectionAndStream 新建一个 SPDY 连接，并在其上创建流协议处理器。
//
// 协议协商结果为空串时回退到 V1：旧服务端可能完全不返回协议头，
// 此时只能按最早的流布局工作。
func (e *spdyStreamExecutor) newConnectionAndStream(ctx context.Context, options StreamOptions) (httpstream.Connection, streamProtocolHandler, error) {
	req, err := http.NewRequestWithContext(ctx, e.method, e.url.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request: %v", err)
	}

	client := http.Client{Transport: e.transport}
	if e.rejectRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return fmt.Errorf("redirect not allowed")
		}
	}
	conn, protocol, err := spdy.NegotiateStreaming(
		e.upgrader,
		&client,
		req,
		e.protocols...,
	)
	if err != nil {
		return nil, nil, err
	}

	var streamer streamProtocolHandler

	logger := klog.FromContext(ctx)
	switch protocol {
	case remotecommand.StreamProtocolV5Name:
		streamer = newStreamProtocolV5(options)
	case remotecommand.StreamProtocolV4Name:
		streamer = newStreamProtocolV4(options)
	case remotecommand.StreamProtocolV3Name:
		streamer = newStreamProtocolV3(options)
	case remotecommand.StreamProtocolV2Name:
		streamer = newStreamProtocolV2(options)
	case "":
		logger.V(4).Info("The server did not negotiate a streaming protocol version, falling back", "protocol", remotecommand.StreamProtocolV1Name)
		fallthrough
	case remotecommand.StreamProtocolV1Name:
		streamer = newStreamProtocolV1(options)
	}

	return conn, streamer, nil
}

// StreamWithContext 向服务端打开一个协议流处理器并开始传输，直到客户端关闭
// 连接、服务端断开或 context 结束。
func (e *spdyStreamExecutor) StreamWithContext(ctx context.Context, options StreamOptions) error {
	conn, streamer, err := e.newConnectionAndStream(ctx, options)
	if err != nil {
		return err
	}
	defer conn.Close()

	// stream 在独立协程运行，因为 handler 内部可能阻塞在 stdin 读取；
	// panic 与 error 分别经通道回传，保证握手阶段之后的 panic 也能到达调用方。
	panicChan := make(chan any, 1)
	errorChan := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// SPDY 执行器不需要同步流的创建，因此传入 nil 的 ready 通道。
		// 底层的 spdystream 库能在没有竞态的情况下处理流的多路复用。
		errorChan <- streamer.stream(klog.FromContext(ctx), conn, nil)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case err := <-errorChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
