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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

const (
	// 3 分钟与 Go 默认的 TCP keep-alive 探测节奏兼容，既能及时发现
	// 半开连接，又不会给网络设备造成过多探测流量。
	defaultKeepAlivePeriod = 3 * time.Minute
)

// 静态 TLS 参数在这里确定；证书内容则通过 GetConfigForClient 动态获取，
// 从而支持运行期热更新证书而无需重建 listener。
func (s *SecureServingInfo) tlsConfig(stopCh <-chan struct{}) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		// 不能用 SSLv3：存在 POODLE 与 BEAST 漏洞。
		// 不能用 TLSv1.0：使用 CBC 加密套件时存在 POODLE 与 BEAST 漏洞。
		// 不能用 TLSv1.1：使用了 RC4 加密套件。
		MinVersion: tls.VersionTLS12,
		// 为 Go 1.7 的 HTTP Server 启用 HTTP2。
		NextProtos: []string{"h2", "http/1.1"},
	}

	// 这些是 tls.Config 的静态部分。
	if s.DisableHTTP2 {
		klog.Info("Forcing use of http/1.1 only")
		tlsConfig.NextProtos = []string{"http/1.1"}
	}
	if s.MinTLSVersion > 0 {
		tlsConfig.MinVersion = s.MinTLSVersion
	}
	if len(s.CipherSuites) > 0 {
		tlsConfig.CipherSuites = s.CipherSuites
		// 对显式配置的不安全套件只告警不拒绝：兼容性优先，
		// 但让运维在日志中能发现风险配置。
		insecureCiphers := flag.InsecureTLSCiphers()
		for i := 0; i < len(s.CipherSuites); i++ {
			for cipherName, cipherID := range insecureCiphers {
				if s.CipherSuites[i] == cipherID {
					klog.Warningf("Use of insecure cipher '%s' detected.", cipherName)
				}
			}
		}
	}
	if len(s.CurvePreferences) > 0 {
		tlsConfig.CurvePreferences = s.CurvePreferences
	}

	if s.ClientCA != nil {
		// 在请求中填充 PeerCertificates，但不拒绝没有证书的客户端：证书可交由认证器校验，其他认证方式也仍然可用。
		// 因此这里用 RequestClientCert 而不是 RequireAndVerifyClientCert：是否需要客户端证书由认证器决定，而不是 TLS 层。
		tlsConfig.ClientAuth = tls.RequestClientCert
	}

	if s.ClientCA != nil || s.Cert != nil || len(s.SNICerts) > 0 {
		dynamicCertificateController := dynamiccertificates.NewDynamicServingCertificateController(
			tlsConfig,
			s.ClientCA,
			s.Cert,
			s.SNICerts,
			nil, // TODO 研究如何把 event recorder 传到这里。目前只输出 klog 消息。
		)

		if s.ClientCA != nil {
			s.ClientCA.AddListener(dynamicCertificateController)
		}
		if s.Cert != nil {
			s.Cert.AddListener(dynamicCertificateController)
		}
		// 从 stopCh 生成 context，避免修改那些仍依赖 apiserver 的文件。
		// TODO: 看看能否把 ctx 传给当前方法。
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-stopCh:
				cancel() // stopCh 已关闭，因此取消 context。
			case <-ctx.Done():
			}
		}()
		// RunOnce 仅用于预热；文件必须已经存在，失败并不致命，
		// 后续 Run 阶段仍会重试并最终 fail closed。
		if controller, ok := s.ClientCA.(dynamiccertificates.ControllerRunner); ok {
			// 预热一次数据；即使失败也没关系，因为最终会 fail closed。
			// 文件必须已存在，这里只是为了方便。
			if err := controller.RunOnce(ctx); err != nil {
				klog.Warningf("Initial population of client CA failed: %v", err)
			}

			go controller.Run(ctx, 1)
		}
		if controller, ok := s.Cert.(dynamiccertificates.ControllerRunner); ok {
			// 预热一次数据；即使失败也没关系，因为最终会 fail closed。
			// 文件必须已存在，这里只是为了方便。
			if err := controller.RunOnce(ctx); err != nil {
				klog.Warningf("Initial population of default serving certificate failed: %v", err)
			}

			go controller.Run(ctx, 1)
		}
		for _, sniCert := range s.SNICerts {
			sniCert.AddListener(dynamicCertificateController)
			if controller, ok := sniCert.(dynamiccertificates.ControllerRunner); ok {
				// 预热一次数据；即使失败也没关系，因为最终会 fail closed。
				// 文件必须已存在，这里只是为了方便。
				if err := controller.RunOnce(ctx); err != nil {
					klog.Warningf("Initial population of SNI serving certificate failed: %v", err)
				}

				go controller.Run(ctx, 1)
			}
		}

		// 预热一次数据；即使失败也没关系，因为最终会 fail closed。
		// 文件必须已存在，这里只是为了方便。
		if err := dynamicCertificateController.RunOnce(); err != nil {
			klog.Warningf("Initial population of dynamic certificates failed: %v", err)
		}
		go dynamicCertificateController.Run(1, stopCh)

		// 每次握手都会回调，返回当前最新证书；这才是热更新的关键接线。
		tlsConfig.GetConfigForClient = dynamicCertificateController.GetConfigForClient
	}

	return tlsConfig, nil
}

// Serve 运行安全的 HTTP 服务。仅当证书无法加载或首次监听失败时返回错误。
// 实际的服务循环（可通过关闭 stopCh 停止）运行在 goroutine 中，因此 Serve 不阻塞。
// 返回的 stoppedCh 在所有非接管的活跃请求处理完成后关闭。
// 返回的 listenerStoppedCh 在底层 http Server 停止监听后关闭。
func (s *SecureServingInfo) Serve(handler http.Handler, shutdownTimeout time.Duration, stopCh <-chan struct{}) (<-chan struct{}, <-chan struct{}, error) {
	if s.Listener == nil {
		return nil, nil, fmt.Errorf("listener must not be nil")
	}

	tlsConfig, err := s.tlsConfig(stopCh)
	if err != nil {
		return nil, nil, err
	}

	secureServer := &http.Server{
		Addr:           s.Listener.Addr().String(),
		Handler:        handler,
		MaxHeaderBytes: 1 << 20,
		TLSConfig:      tlsConfig,

		// IdleTimeout 与客户端 transport 的 keep-alive 对齐，
		// 避免两端对连接空闲时间判断不一致而反复重建连接。
		IdleTimeout:       90 * time.Second, // 与 http.DefaultTransport 的 keep-alive 超时一致。
		ReadHeaderTimeout: 32 * time.Second, // 略小于 requestTimeoutUpperBound。
	}

	setMaxHeaderValueCount(secureServer)

	if !s.DisableHTTP2 {
		// 调研显示，集群中至少 99% 的序列化资源小于 256kb。
		// 该值足够大，可让大多数 API POST 请求装进单帧；
		// 同时足够小，使得每连接缓冲区乘以 MaxConcurrentStreams 后仍可接受。
		const resourceBody99Percentile = 256 * 1024

		http2Options := &http2.Server{
			IdleTimeout: 90 * time.Second, // 与 http.DefaultTransport 的 keep-alive 超时一致。
			// 把默认 1MB 的每流缓冲区与最大帧大小缩小，同时仍能容纳大多数 API POST 请求。
			MaxUploadBufferPerStream: resourceBody99Percentile,
			MaxReadFrameSize:         resourceBody99Percentile,
		}

		// 使用覆盖后的并发流设置，或显式采用默认值 250，以便确定 MaxUploadBufferPerConnection 的大小。
		if s.HTTP2MaxStreamsPerConnection > 0 {
			http2Options.MaxConcurrentStreams = uint32(s.HTTP2MaxStreamsPerConnection)
		} else {
			// 与客户端使用的 http2.initialMaxConcurrentStreams 一致。
			// 这使得恶意客户端最多只能打开 400 个流，随后连接会被强制关闭。
			// https://github.com/golang/net/commit/b225e7ca6dde1ef5a5ae5ce922861bda011cfabd
			http2Options.MaxConcurrentStreams = 100
		}

		// 把连接缓冲区从默认 1MB 增大，以处理指定数量的并发流。
		http2Options.MaxUploadBufferPerConnection = http2Options.MaxUploadBufferPerStream * int32(http2Options.MaxConcurrentStreams)
		// 把设置应用到服务端。
		if err := http2.ConfigureServer(secureServer, http2Options); err != nil {
			return nil, nil, fmt.Errorf("error configuring http2: %v", err)
		}
	}

	// 握手错误通常来自扫描器或配置错误，数量可能极大；
	// 由专用 writer 降级到 V(5) 并计入指标，避免刷爆日志。
	tlsErrorWriter := &tlsHandshakeErrorWriter{os.Stderr}
	tlsErrorLogger := log.New(tlsErrorWriter, "", 0)
	secureServer.ErrorLog = tlsErrorLogger

	klog.Infof("Serving securely on %s", secureServer.Addr)
	return RunServer(secureServer, s.Listener, shutdownTimeout, stopCh)
}

// RunServer 启动一个 goroutine 持续服务，直到 stopCh 关闭。
//
// 返回的 stoppedCh 在所有非接管的活跃请求处理完成后关闭。
//
// 该函数不阻塞。
// TODO: 当 kube-apiserver 移除不安全服务后，改为私有。
func RunServer(
	server *http.Server,
	ln net.Listener,
	shutDownTimeout time.Duration,
	stopCh <-chan struct{},
) (<-chan struct{}, <-chan struct{}, error) {
	if ln == nil {
		return nil, nil, fmt.Errorf("listener must not be nil")
	}

	// Shutdown 会等待活跃请求完成（受 shutDownTimeout 限制），
	// 这是在快速退出与不中断请求之间取的折中。
	serverShutdownCh, listenerStoppedCh := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(serverShutdownCh)
		<-stopCh
		ctx, cancel := context.WithTimeout(context.Background(), shutDownTimeout)
		defer cancel()
		err := server.Shutdown(ctx)
		if err != nil {
			klog.Errorf("Failed to shutdown server: %v", err)
		}
	}()

	go func() {
		defer utilruntime.HandleCrash()
		defer close(listenerStoppedCh)

		var listener net.Listener
		listener = tcpKeepAliveListener{ln}
		if server.TLSConfig != nil {
			listener = tls.NewListener(listener, server.TLSConfig)
		}

		err := server.Serve(listener)

		// Serve 在未收到 stopCh 时返回错误属于意外（例如 listener 失效），
		// 直接 panic 让上层重启进程，而不是静默接受不可用状态。
		msg := fmt.Sprintf("Stopped listening on %s", ln.Addr().String())
		select {
		case <-stopCh:
			klog.Info(msg)
		default:
			panic(fmt.Sprintf("%s due to error: %v", msg, err))
		}
	}()

	return serverShutdownCh, listenerStoppedCh, nil
}

// tcpKeepAliveListener 为接受的连接设置 TCP keep-alive 超时。
// ListenAndServe 与 ListenAndServeTLS 使用它，使失效的 TCP 连接
// （例如下载途中合上笔记本）最终被回收。
//
// 抄自 Go 1.7.2 net/http/server.go。
type tcpKeepAliveListener struct {
	net.Listener
}

// Accept 接受连接并启用 TCP keep-alive；非 TCP 连接保持不变。
func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(defaultKeepAlivePeriod)
	}
	return c, nil
}

// tlsHandshakeErrorWriter 把 TLS 握手错误以 V(5) 的 trace 级别写入 klog，
// 避免握手错误刷屏。
type tlsHandshakeErrorWriter struct {
	out io.Writer
}

const tlsHandshakeErrorPrefix = "http: TLS handshake error"

// Write 只对握手错误做降级处理并计数；其他日志原样输出，
// 因为那些通常是真正需要关注的服务端错误。
func (w *tlsHandshakeErrorWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), tlsHandshakeErrorPrefix) {
		klog.V(5).Info(string(p))
		metrics.TLSHandshakeErrors.Inc()
		return len(p), nil
	}

	// 非 TLS 握手错误照常记录。
	return w.out.Write(p)
}
