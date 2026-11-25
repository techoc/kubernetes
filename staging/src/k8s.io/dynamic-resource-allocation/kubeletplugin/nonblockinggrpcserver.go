/*
Copyright 2022 The Kubernetes Authors.

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

package kubeletplugin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

// requestID 是全局递增的请求编号，用于给每个一元调用或流分配唯一的日志标识。
var requestID int64

// grpcServer 封装在 Unix 域套接字上运行的 gRPC 服务器，支持非阻塞启动与优雅停止。
type grpcServer struct {
	// grpcVerbosity 是请求/响应日志的详细级别；wg 用于等待 Serve goroutine 退出；
	// endpoint 是监听端点；server 是底层 gRPC 服务器。
	grpcVerbosity int
	wg            sync.WaitGroup
	endpoint      endpoint
	server        *grpc.Server
}

// registerService 是把具体服务注册到 gRPC 服务器的函数类型。
type registerService func(s *grpc.Server)

// startGRPCServer 在 Unix 域套接字上启动 gRPC 服务器，并在后台 goroutine 中处理任意服务的请求。
// 后台遇到的错误会调用 errHandler（这类错误是致命的，应导致进程退出）。
func startGRPCServer(logger klog.Logger, grpcVerbosity int, unaryInterceptors []grpc.UnaryServerInterceptor, streamInterceptors []grpc.StreamServerInterceptor, endpoint endpoint, errHandler func(ctx context.Context, err error), services ...registerService) (*grpcServer, error) {
	// 用传入的 logger 构造根上下文，供后续拦截器注入。
	ctx := klog.NewContext(context.Background(), logger)

	// 初始化服务器对象。
	s := &grpcServer{
		endpoint:      endpoint,
		grpcVerbosity: grpcVerbosity,
	}

	// 在端点上创建监听器。
	listener, err := endpoint.listen(ctx)
	// 监听失败时包装错误信息。
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", s.endpoint.path(), err)
	}

	// gRPC 服务器关闭时会自动关闭监听套接字，因此无需在此重复关闭。
	// 每个调用的上下文都要注入 logger，因为方法实现内部可能产生日志输出。
	// 准备服务器选项。
	var opts []grpc.ServerOption
	// 一元拦截器链：先注入上下文，再记录请求，最后才是调用方提供的拦截器。
	finalUnaryInterceptors := []grpc.UnaryServerInterceptor{
		unaryContextInterceptor(ctx),
		s.interceptor,
	}
	// 追加调用方提供的一元拦截器。
	finalUnaryInterceptors = append(finalUnaryInterceptors, unaryInterceptors...)
	// 流拦截器链：同样先注入上下文，再记录流，最后追加自定义拦截器。
	finalStreamInterceptors := []grpc.StreamServerInterceptor{
		streamContextInterceptor(ctx),
		s.streamInterceptor,
	}
	// 追加调用方提供的流拦截器。
	finalStreamInterceptors = append(finalStreamInterceptors, streamInterceptors...)
	// 把一元拦截器链装配为服务器选项。
	opts = append(opts, grpc.ChainUnaryInterceptor(finalUnaryInterceptors...))
	// 把流拦截器链装配为服务器选项。
	opts = append(opts, grpc.ChainStreamInterceptor(finalStreamInterceptors...))
	// 创建 gRPC 服务器。
	s.server = grpc.NewServer(opts...)
	// 注册所有服务。
	for _, service := range services {
		service(s.server)
	}
	// 计入后台 goroutine，便于停止时等待其退出。
	s.wg.Add(1)
	// 后台运行 Serve；结束后根据错误情况调用错误处理或记录优雅退出。
	go func() {
		defer s.wg.Done()
		// 在监听器上开始服务请求。
		err := s.server.Serve(listener)
		// Serve 返回错误视为致命错误，交给 errHandler 处理。
		if err != nil {
			errHandler(ctx, err)
		} else {
			// 正常退出时记录日志。
			logger.V(3).Info("GRPC server terminated gracefully", "endpoint", endpoint.path())
		}
	}()

	// 记录服务器已启动。
	logger.V(3).Info("GRPC server started", "endpoint", endpoint.path())
	return s, nil
}

// unaryContextInterceptor 把 valueCtx 中的值注入到一元调用的上下文中。
func unaryContextInterceptor(valueCtx context.Context) grpc.UnaryServerInterceptor {
	// 返回的拦截器先合并上下文，再交给实际处理函数。
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// 以调用上下文为根，valueCtx 作为取值回退。
		ctx = mergeContexts(ctx, valueCtx)
		return handler(ctx, req)
	}
}

// streamContextInterceptor 对流调用做与 unaryContextInterceptor 相同的上下文注入。
func streamContextInterceptor(valueCtx context.Context) grpc.StreamServerInterceptor {
	// 返回的拦截器合并流上下文，并用包装后的流调用 handler。
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// 合并流上下文与值上下文。
		ctx := mergeContexts(ss.Context(), valueCtx)
		// 用包装流调用 handler，使 Context() 返回合并后的上下文。
		return handler(srv, mergeServerStream{ServerStream: ss, ctx: ctx})
	}
}

// mergeServerStream 包装 grpc.ServerStream，使其返回合并后的上下文。
type mergeServerStream struct {
	// 内嵌原始流。
	// ctx 是合并后的上下文。
	grpc.ServerStream
	ctx context.Context
}

// Context 返回合并后的上下文。
func (m mergeServerStream) Context() context.Context {
	return m.ctx
}

// mergeContexts 创建一个新上下文：取消信号由根上下文决定；
// 当根上下文没有某个值时，回退到 valueCtx 中的值。
func mergeContexts(rootCtx, valueCtx context.Context) context.Context {
	// 返回自定义上下文实现。
	return mergeCtx{
		Context:  rootCtx,
		valueCtx: valueCtx,
	}
}

// mergeCtx 实现“取消看根上下文、取值带回退”的组合上下文。
type mergeCtx struct {
	// 嵌入的根上下文，负责取消与截止。
	// valueCtx 是值查找的备用上下文。
	context.Context
	valueCtx context.Context
}

// Value 优先从根上下文取值，取不到时回退到 valueCtx。
func (m mergeCtx) Value(i interface{}) interface{} {
	// 先查根上下文。
	if v := m.Context.Value(i); v != nil {
		return v
	}
	// 没有则从备用上下文取。
	return m.valueCtx.Value(i)
}

// interceptor 处理每个一元请求：分配递增的请求 ID，把带 ID 与方法名的 logger 放入上下文，
// 并记录请求与响应；发生 panic 时先记录再重新抛出。
func (s *grpcServer) interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// 原子递增获得唯一请求 ID。
	requestID := atomic.AddInt64(&requestID, 1)
	// 取出上下文中注入的 logger。
	logger := klog.FromContext(ctx)
	// 附加请求 ID 与方法名。
	logger = klog.LoggerWithValues(logger, "requestID", requestID, "method", info.FullMethod)
	// 把增强后的 logger 放回上下文。
	ctx = klog.NewContext(ctx, logger)
	// 记录请求内容。
	logger.V(s.grpcVerbosity).Info("handling request", "request", req)
	// 用 defer 捕获 panic：记录后原样重抛。
	defer func() {
		if r := recover(); r != nil {
			logger.Error(nil, "handling request panicked", "panic", r, "request", req)
			panic(r)
		}
	}()
	// 调用实际处理函数。
	resp, err = handler(ctx, req)
	// 根据处理结果记录成功或失败。
	if err != nil {
		logger.Error(err, "handling request failed")
	} else {
		logger.V(s.grpcVerbosity).Info("handling request succeeded", "response", resp)
	}
	return
}

// streamInterceptor 与 interceptor 类似，但作用于流调用：分配请求 ID、注入 logger，
// 并用 logStream 包装流以记录每条发送的消息。
func (s *grpcServer) streamInterceptor(server interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// 原子递增获得唯一请求 ID。
	requestID := atomic.AddInt64(&requestID, 1)
	// 取得流上下文。
	ctx := stream.Context()
	// 附加请求 ID 与方法名后放回上下文。
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithValues(logger, "requestID", requestID, "method", info.FullMethod)
	ctx = klog.NewContext(ctx, logger)
	// 包装流以记录发送的消息。
	stream = logStream{
		ServerStream:  stream,
		ctx:           ctx,
		grpcVerbosity: s.grpcVerbosity,
	}
	// 记录流处理开始。
	logger.V(s.grpcVerbosity).Info("handling stream")
	// 调用实际的流处理函数。
	err := handler(server, stream)
	// 记录流处理成功或失败。
	if err != nil {
		logger.Error(err, "handling stream failed")
	} else {
		logger.V(s.grpcVerbosity).Info("handling stream succeeded")
	}
	return err

}

// logStream 包装 grpc.ServerStream，记录每次发送的消息及其结果。
type logStream struct {
	// 内嵌原始流。
	grpc.ServerStream
	// ctx 是带 logger 的上下文；grpcVerbosity 是日志详细级别。
	ctx           context.Context
	grpcVerbosity int
}

// Context 返回包装后的上下文（含 logger）。
func (l logStream) Context() context.Context {
	return l.ctx
}

// SendMsg 在发送消息前后记录日志，并把底层发送错误一并记录。
func (l logStream) SendMsg(msg interface{}) error {
	// 取出上下文中的 logger。
	logger := klog.FromContext(l.ctx)
	// 记录待发送的消息。
	logger.V(l.grpcVerbosity).Info("sending stream message", "message", msg)
	// 调用底层发送。
	err := l.ServerStream.SendMsg(msg)
	// 记录发送失败或成功。
	if err != nil {
		logger.Error(err, "sending stream message failed")
	} else {
		logger.V(l.grpcVerbosity).Info("sending stream message succeeded")
	}
	return err
}

// stop 停止服务器并等待后台 goroutine 退出；方法幂等，可在 nil 指针上调用。
func (s *grpcServer) stop() {
	// nil 接收者直接返回。
	if s == nil {
		return
	}
	// 仅在服务器存在时调用 Stop。
	if s.server != nil {
		s.server.Stop()
	}
	// 等待 Serve goroutine 结束。
	s.wg.Wait()
	// 清空句柄，保证重复调用安全。
	s.server = nil
}
