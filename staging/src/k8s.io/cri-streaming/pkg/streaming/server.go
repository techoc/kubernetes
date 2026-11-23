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

package streaming

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	restful "github.com/emicklei/go-restful/v3"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
)

// Server is the library interface to serve the stream requests.
// Server 是服务于流式请求的库接口：它自身实现 HTTP Handler，对外提供获取 exec、attach、portforward 流式 URL 的方法，以及服务的启动与停止能力。
type Server interface {
	http.Handler

	// Get the serving URL for the requests.
	// Requests must not be nil. Responses may be nil iff an error is returned.
	// 根据请求生成对应的流式访问 URL。请求不能为 nil；当且仅当返回错误时，响应才可能为 nil。
	// 每个方法都会把请求写入缓存，并把生成的 token 拼进返回的 URL，后续 HTTP 请求凭该 token 取回请求信息。
	GetExec(*runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error)
	GetAttach(req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error)
	GetPortForward(*runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error)

	// Start the server.
	// addr is the address to serve on (address:port) stayUp indicates whether the server should
	// listen until Stop() is called, or automatically stop after all expected connections are
	// closed. Calling Get{Exec,Attach,PortForward} increments the expected connection count.
	// Function does not return until the server is stopped.
	// 启动服务，该函数直到服务停止后才会返回。stayUp 为 true 表示一直监听直到调用 Stop()；
	// 为 false 则表示在所有预期连接都关闭后自动停止，每次调用 Get{Exec,Attach,PortForward} 都会增加预期连接计数。
	Start(stayUp bool) error
	// Stop the server, and terminate any open connections.
	// 停止服务，并终止所有仍然打开的连接。
	Stop() error
}

// Runtime is the interface to execute the commands and provide the streams.
// Runtime 是执行命令并提供数据流的接口，由底层容器运行时实现；Server 通过 criAdapter 适配后再调用它。
type Runtime interface {
	// 在指定容器中执行命令：in/out/err 分别是标准输入、标准输出与标准错误流，tty 表示是否分配终端，resize 用于接收终端尺寸变化。
	Exec(ctx context.Context, containerID string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error
	// 附着到指定容器：参数含义与 Exec 类似，但不接收要执行的命令。
	Attach(ctx context.Context, containerID string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error
	// 为指定 pod sandbox 的某个端口建立转发，stream 是与客户端双向通信的数据流。
	PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error
}

// Config defines the options used for running the stream server.
// Config 定义运行流式服务所需的配置选项。
type Config struct {
	// The host:port address the server will listen on.
	// 服务监听的 host:port 地址。
	Addr string
	// The optional base URL for constructing streaming URLs. If empty, the baseURL will be
	// constructed from the serve address.
	// Note that for port "0", the URL port will be set to actual port in use.
	// 构造流式 URL 时使用的可选基础 URL；为空时根据监听地址自动生成。
	// 注意：当端口为 "0" 时，URL 中的端口会被替换为实际分配的端口。
	BaseURL *url.URL

	// How long to leave idle connections open for.
	// 空闲连接保持打开的最长时间。
	StreamIdleTimeout time.Duration
	// How long to wait for clients to create streams. Only used for SPDY streaming.
	// 等待客户端创建 stream 的超时时间，仅用于 SPDY 流式传输。
	StreamCreationTimeout time.Duration

	// The streaming protocols the server supports (understands and permits).  See
	// k8s.io/kubernetes/pkg/kubelet/server/remotecommand/constants.go for available protocols.
	// Only used for SPDY streaming.
	// 服务支持（理解并允许）的远程命令流式协议列表，仅用于 SPDY 流式传输。
	SupportedRemoteCommandProtocols []string

	// The streaming protocols the server supports (understands and permits).  See
	// k8s.io/kubernetes/pkg/kubelet/server/portforward/constants.go for available protocols.
	// Only used for SPDY streaming.
	// 服务支持（理解并允许）的端口转发流式协议列表，仅用于 SPDY 流式传输。
	SupportedPortForwardProtocols []string

	// The config for serving over TLS. If nil, TLS will not be used.
	// 通过 TLS 提供服务时使用的配置；为 nil 时不启用 TLS。
	TLSConfig *tls.Config
}

// DefaultConfig provides default values for server Config. The DefaultConfig is partial, so
// some fields like Addr must still be provided.
// DefaultConfig 提供服务配置的默认值；它只是部分配置，调用方仍需提供 Addr 等必填字段。
var DefaultConfig = Config{
	StreamIdleTimeout:               4 * time.Hour,
	StreamCreationTimeout:           remotecommandserver.DefaultStreamCreationTimeout,
	SupportedRemoteCommandProtocols: remotecommandserver.SupportedStreamingProtocols,
	SupportedPortForwardProtocols:   portforward.SupportedProtocols,
}

// NewServer creates a new Server for stream requests.
// TODO(tallclair): Add auth(n/z) interface & handling.
// NewServer 创建一个用于处理流式请求的 Server；目前尚未实现认证与授权。
func NewServer(config Config, runtime Runtime) (Server, error) {
	// 组装 server 实例：config 保存配置，runtime 经 criAdapter 适配成 remotecommand/portforward 所需接口，cache 用于暂存请求 token。
	s := &server{
		config:  config,
		runtime: &criAdapter{runtime},
		cache:   newRequestCache(),
	}

	// 未显式指定 BaseURL 时，用监听地址构造；若启用了 TLS，则使用 https 协议。
	if s.config.BaseURL == nil {
		s.config.BaseURL = &url.URL{
			Scheme: "http",
			Host:   s.config.Addr,
		}
		if s.config.TLSConfig != nil {
			s.config.BaseURL.Scheme = "https"
		}
	}

	// 定义三个流式端点及其处理函数：path 是含 {token} 占位符的路由模板，handler 是对应的处理函数。
	ws := &restful.WebService{}
	endpoints := []struct {
		path    string
		handler restful.RouteFunction
	}{
		{"/exec/{token}", s.serveExec},
		{"/attach/{token}", s.serveAttach},
		{"/portforward/{token}", s.servePortForward},
	}
	// 若 URL 带有基础路径前缀，则注册的路由也要加上该前缀。
	// If serving relative to a base path, set that here.
	pathPrefix := path.Dir(s.config.BaseURL.Path)
	// 每个端点同时支持 GET 和 POST 两种 HTTP 方法。
	for _, e := range endpoints {
		for _, method := range []string{"GET", "POST"} {
			ws.Route(ws.
				Method(method).
				Path(path.Join(pathPrefix, e.path)).
				To(e.handler))
		}
	}
	// 把 WebService 加入容器，并让 server 同时持有路由 handler 与底层 http.Server。
	handler := restful.NewContainer()
	handler.Add(ws)
	s.handler = handler
	s.server = &http.Server{
		Addr:      s.config.Addr,
		Handler:   s.handler,
		TLSConfig: s.config.TLSConfig,
	}

	return s, nil
}

// server 是 Server 接口的具体实现，其字段含义如下：
// config 保存服务运行所需的配置；runtime 是适配后的容器运行时，供各流式处理器调用；
// handler 是 go-restful 路由容器，负责分发 HTTP 请求；cache 按 token 存放待处理的 CRI 请求；
// server 是底层 HTTP 服务器，负责监听与 TLS。
type server struct {
	config  Config
	runtime *criAdapter
	handler http.Handler
	cache   *requestCache
	server  *http.Server
}

// validateExecRequest 校验 exec 请求的必填字段以及 tty/stderr 的合法性，返回 gRPC 风格的状态错误。
func validateExecRequest(req *runtimeapi.ExecRequest) error {
	// 容器 ID 是必填项。
	if req.ContainerId == "" {
		return status.Errorf(codes.InvalidArgument, "missing required container_id")
	}
	if req.Tty && req.Stderr {
		// If TTY is set, stderr cannot be true because multiplexing is not
		// supported.
		// 分配 TTY 时 stdout 与 stderr 合并输出，不支持再单独复用 stderr 通道。
		return status.Errorf(codes.InvalidArgument, "tty and stderr cannot both be true")
	}
	// 至少要开启一个标准流，否则流式传输没有意义。
	if !req.Stdin && !req.Stdout && !req.Stderr {
		return status.Errorf(codes.InvalidArgument, "one of stdin, stdout, or stderr must be set")
	}
	return nil
}

// GetExec 校验 exec 请求，将其缓存并生成一个带 token 的 exec 流式 URL 返回给调用方。
func (s *server) GetExec(req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	// 先做参数校验，失败时直接返回错误，不会污染 token 缓存。
	if err := validateExecRequest(req); err != nil {
		return nil, err
	}
	// 把请求存入缓存，拿到一次性使用的 token。
	token, err := s.cache.Insert(req)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ExecResponse{
		// 用 method 和 token 拼出客户端稍后要访问的 URL。
		Url: s.buildURL("exec", token),
	}, nil
}

// validateAttachRequest 校验 attach 请求，规则与 exec 请求相同。
func validateAttachRequest(req *runtimeapi.AttachRequest) error {
	// 容器 ID 是必填项。
	if req.ContainerId == "" {
		return status.Errorf(codes.InvalidArgument, "missing required container_id")
	}
	if req.Tty && req.Stderr {
		// If TTY is set, stderr cannot be true because multiplexing is not
		// supported.
		// 分配 TTY 时不支持单独的 stderr 通道。
		return status.Errorf(codes.InvalidArgument, "tty and stderr cannot both be true")
	}
	// 至少要开启一个标准流。
	if !req.Stdin && !req.Stdout && !req.Stderr {
		return status.Errorf(codes.InvalidArgument, "one of stdin, stdout, and stderr must be set")
	}
	return nil
}

// GetAttach 校验 attach 请求，将其缓存并生成一个带 token 的 attach 流式 URL 返回给调用方。
func (s *server) GetAttach(req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	// 参数校验失败时直接返回，不写入缓存。
	if err := validateAttachRequest(req); err != nil {
		return nil, err
	}
	// 缓存请求并获取一次性 token。
	token, err := s.cache.Insert(req)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.AttachResponse{
		Url: s.buildURL("attach", token),
	}, nil
}

// GetPortForward 校验端口转发请求的必填 sandbox ID，将其缓存并生成带 token 的流式 URL。
func (s *server) GetPortForward(req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	// pod sandbox ID 是必填项。
	if req.PodSandboxId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required pod_sandbox_id")
	}
	// 缓存请求并获取一次性 token。
	token, err := s.cache.Insert(req)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.PortForwardResponse{
		Url: s.buildURL("portforward", token),
	}, nil
}

// Start 在配置的地址上监听并提供服务；当前仅实现了 stayUp=true 的情况。
func (s *server) Start(stayUp bool) error {
	if !stayUp {
		// TODO(tallclair): Implement this.
		// stayUp=false（连接全部关闭后自动退出）尚未实现，明确返回错误而非静默忽略。
		return errors.New("stayUp=false is not yet implemented")
	}

	// 在 TCP 上监听配置的地址，失败则把错误直接返回给调用方。
	listener, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	// Use the actual address as baseURL host. This handles the "0" port case.
	// 用实际监听到的地址更新 baseURL 的 host，这样才能正确处理端口为 "0" 时由系统分配端口的情况。
	s.config.BaseURL.Host = listener.Addr().String()
	if s.config.TLSConfig != nil {
		// 配置了 TLS 时启用 HTTPS；证书来自 TLSConfig，因此这里的证书和私钥参数留空。
		return s.server.ServeTLS(listener, "", "") // Use certs from TLSConfig.
	}
	// 未配置 TLS 时以普通 HTTP 方式提供服务。
	return s.server.Serve(listener)
}

// Stop 关闭底层 HTTP 服务器，同时终止其上仍在进行的连接。
func (s *server) Stop() error {
	return s.server.Close()
}

// ServeHTTP 让 server 实现 http.Handler，把请求转交给 go-restful 路由容器处理。
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// buildURL 以 BaseURL 为基准解析出 "method/token" 相对路径，得到客户端可访问的完整流式 URL。
func (s *server) buildURL(method, token string) string {
	return s.config.BaseURL.ResolveReference(&url.URL{
		Path: path.Join(method, token),
	}).String()
}

// serveExec 处理 /exec/{token} 请求：消费 token 取回原 ExecRequest，并转交给 remotecommand 的 ServeExec。
func (s *server) serveExec(req *restful.Request, resp *restful.Response) {
	// 从路径参数中取出 token。
	token := req.PathParameter("token")
	// 消费缓存中的请求；token 不存在或已被用过时返回 404。
	cachedRequest, ok := s.cache.Consume(token)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}
	// 校验缓存对象的类型，类型不匹配同样视为无效请求。
	exec, ok := cachedRequest.(*runtimeapi.ExecRequest)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}

	// 把 CRI 请求中的标准流与 tty 设置转换为 remotecommand 的流选项。
	streamOpts := &remotecommandserver.Options{
		Stdin:  exec.Stdin,
		Stdout: exec.Stdout,
		Stderr: exec.Stderr,
		TTY:    exec.Tty,
	}

	// 调用 remotecommand 的服务端处理逻辑，传入容器 ID、命令、超时与支持的协议列表；pod 相关参数为空串（未使用）。
	remotecommandserver.ServeExec(
		resp.ResponseWriter,
		req.Request,
		s.runtime,
		"", // unused: podName
		"", // unusued: podUID
		exec.ContainerId,
		exec.Cmd,
		streamOpts,
		s.config.StreamIdleTimeout,
		s.config.StreamCreationTimeout,
		s.config.SupportedRemoteCommandProtocols)
}

// serveAttach 处理 /attach/{token} 请求：消费 token 取回原 AttachRequest，并转交给 remotecommand 的 ServeAttach。
func (s *server) serveAttach(req *restful.Request, resp *restful.Response) {
	// 从路径中取 token，并用它一次性消费缓存的请求。
	token := req.PathParameter("token")
	cachedRequest, ok := s.cache.Consume(token)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}
	// 类型不符说明该 token 对应的请求不是 attach，按未找到处理。
	attach, ok := cachedRequest.(*runtimeapi.AttachRequest)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}

	// 把 CRI 请求中的标准流与 tty 设置转换为 remotecommand 的流选项。
	streamOpts := &remotecommandserver.Options{
		Stdin:  attach.Stdin,
		Stdout: attach.Stdout,
		Stderr: attach.Stderr,
		TTY:    attach.Tty,
	}
	// 调用 remotecommand 的服务端 attach 逻辑，传入容器 ID 及超时、协议等配置。
	remotecommandserver.ServeAttach(
		resp.ResponseWriter,
		req.Request,
		s.runtime,
		"", // unused: podName
		"", // unusued: podUID
		attach.ContainerId,
		streamOpts,
		s.config.StreamIdleTimeout,
		s.config.StreamCreationTimeout,
		s.config.SupportedRemoteCommandProtocols)
}

// servePortForward 处理 /portforward/{token} 请求：消费 token 取回原 PortForwardRequest，并转交给端口转发处理器。
func (s *server) servePortForward(req *restful.Request, resp *restful.Response) {
	// 从路径取 token 并一次性消费缓存中的请求，token 无效时返回 404。
	token := req.PathParameter("token")
	cachedRequest, ok := s.cache.Consume(token)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}
	// 类型不符时同样按未找到处理。
	pf, ok := cachedRequest.(*runtimeapi.PortForwardRequest)
	if !ok {
		http.NotFound(resp.ResponseWriter, req.Request)
		return
	}

	// 根据请求中的端口列表构造 v4 端口转发选项，端口格式非法时返回 400。
	portForwardOptions, err := portforward.BuildV4Options(pf.Port)
	if err != nil {
		resp.WriteError(http.StatusBadRequest, err)
		return
	}

	// 调用端口转发服务端逻辑，传入 pod sandbox ID、超时与支持的协议列表。
	portforward.ServePortForward(
		resp.ResponseWriter,
		req.Request,
		s.runtime,
		pf.PodSandboxId,
		"", // unused: podUID
		portForwardOptions,
		s.config.StreamIdleTimeout,
		s.config.StreamCreationTimeout,
		s.config.SupportedPortForwardProtocols)
}

// criAdapter wraps the Runtime functions to conform to the remotecommand interfaces.
// The adapter binds the container ID to the container name argument, and the pod sandbox ID to the pod name.
// criAdapter 包装 Runtime，使其满足 remotecommand 与 portforward 定义的接口：
// 把 remotecommand 的容器名参数当作 CRI 的容器 ID，把 podName 当作 CRI 的 pod sandbox ID。
type criAdapter struct {
	// Runtime 是内嵌的底层容器运行时实现。
	Runtime
}

// 编译期断言：确保 criAdapter 分别满足 remotecommand 的 Executor、Attacher 以及 portforward 的 PortForwarder 接口。
var _ remotecommandserver.Executor = &criAdapter{}
var _ remotecommandserver.Attacher = &criAdapter{}
var _ portforward.PortForwarder = &criAdapter{}

// ExecInContainer 把 remotecommand 的调用转发给 Runtime.Exec，忽略不用的 pod 信息与 timeout，并将 container 作为容器 ID 传入。
func (a *criAdapter) ExecInContainer(ctx context.Context, podName string, podUID string, container string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize, timeout time.Duration) error {
	return a.Runtime.Exec(ctx, container, cmd, in, out, err, tty, resize)
}

// AttachContainer 把 remotecommand 的调用转发给 Runtime.Attach，忽略不用的 pod 信息，并将 container 作为容器 ID 传入。
func (a *criAdapter) AttachContainer(ctx context.Context, podName string, podUID string, container string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	return a.Runtime.Attach(ctx, container, in, out, err, tty, resize)
}

// PortForward 把 portforward 的调用转发给 Runtime.PortForward，这里 podName 实际对应 CRI 的 pod sandbox ID。
func (a *criAdapter) PortForward(ctx context.Context, podName string, podUID string, port int32, stream io.ReadWriteCloser) error {
	return a.Runtime.PortForward(ctx, podName, port, stream)
}
