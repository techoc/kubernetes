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

package cri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	"k8s.io/cri-client/pkg/util"
)

// remoteImageService is a gRPC implementation of internalapi.ImageManagerService.
// remoteImageService 是 internalapi.ImageManagerService 接口的 gRPC 远程实现。
// 字段含义：timeout 是除 PullImage 以外各 RPC 的超时时间；imageClient 是真正发起
// CRI v1 调用的客户端桩；conn 是底层 gRPC 连接，由 Close 关闭；useStreaming
// 决定列表操作是否走流式 RPC。
type remoteImageService struct {
	timeout     time.Duration
	imageClient runtimeapi.ImageServiceClient
	conn        *grpc.ClientConn
	// useStreaming indicates whether to use streaming RPCs for list operations
	// when the CRIListStreaming feature gate is enabled. It falls back to false
	// if a streaming RPC returns Unimplemented. Multiple goroutines may
	// concurrently observe Unimplemented and store false, but that is harmless
	// because the store is idempotent.
	// useStreaming 标记列表操作是否使用流式 RPC；服务端返回 Unimplemented 时
	// 会被置为 false 并回退到一元 RPC。并发写入是幂等的，因此无需加锁。
	useStreaming atomic.Bool
}

// RemoteImageServiceBuilder builds a new internalapi.ImageManagerService.
//
// Construct a builder with NewRemoteImageServiceBuilder, then chain With*
// methods to set non-default options before calling Build. Adding new options
// here is preferred over adding parameters to NewRemoteImageService so that
// default values can be changed in a single place without having to update
// every callsite.
// RemoteImageServiceBuilder 负责按可选配置组装出 ImageManagerService。
// 用法是先通过 NewRemoteImageServiceBuilder 创建构造器，链式调用 With* 方法
// 设置非默认值，最后再调用 Build；新增选项集中在此处可避免改动所有调用点。
// 字段含义：endpoint 是 gRPC 端点；connectionTimeout 同时用于建连与 RPC 超时；
// tracerProvider 提供链路追踪；tracerProviderSet 记录是否显式调用过
// WithTracerProvider（用于区分显式传 nil 与未设置）；useStreaming 决定列表
// 操作是否使用流式 RPC。
type RemoteImageServiceBuilder struct {
	endpoint          string
	connectionTimeout time.Duration
	tracerProvider    trace.TracerProvider
	// tracerProviderSet tracks whether WithTracerProvider was called, so that
	// an explicit nil can be distinguished from the unset default and used to
	// opt out of installing the otelgrpc stats handler entirely.
	// tracerProviderSet 记录调用方是否显式设置过 tracer provider：显式 nil
	// 表示完全不安装 otelgrpc stats handler，与“未设置”走 noop 默认不同。
	tracerProviderSet bool
	// useStreaming indicates whether to use streaming RPCs for list operations
	// when the CRIListStreaming feature gate is enabled. It is expected to
	// default to true once the feature graduates to GA.
	// useStreaming 决定列表操作是否优先使用流式 RPC；待特性 GA 后默认值将改为 true。
	useStreaming bool
}

// NewRemoteImageServiceBuilder returns a builder with default options for
// constructing a remote image service.
// NewRemoteImageServiceBuilder 返回一个使用默认配置的构造器：各字段为零值，
// 由后续 With* 方法覆盖，必填项在 Build 时校验。
func NewRemoteImageServiceBuilder() *RemoteImageServiceBuilder {
	return &RemoteImageServiceBuilder{}
}

// WithEndpoint sets the gRPC endpoint of the remote image service.
// WithEndpoint 设置远端镜像服务的 gRPC 端点地址，返回构造器以便链式调用。
func (b *RemoteImageServiceBuilder) WithEndpoint(endpoint string) *RemoteImageServiceBuilder {
	b.endpoint = endpoint
	return b
}

// WithConnectionTimeout sets the timeout used when connecting to the remote
// image service.
// WithConnectionTimeout 设置连接远端镜像服务所用的超时时间。
func (b *RemoteImageServiceBuilder) WithConnectionTimeout(connectionTimeout time.Duration) *RemoteImageServiceBuilder {
	b.connectionTimeout = connectionTimeout
	return b
}

// WithTracerProvider sets the OpenTelemetry tracer provider used to
// instrument the gRPC client. If WithTracerProvider is not called, the
// otelgrpc stats handler is installed with a noop tracer provider, so no
// traces are produced but gRPC context propagation still works. Passing a nil
// provider explicitly opts out of installing the stats handler entirely,
// disabling both tracing and context propagation.
// WithTracerProvider 设置用于 gRPC 埋点的 OpenTelemetry tracer provider。
// 显式传入 nil 表示彻底不安装 stats handler；不调用本方法则安装 noop provider，
// 只保留上下文传播而不产生真实 trace。
func (b *RemoteImageServiceBuilder) WithTracerProvider(tp trace.TracerProvider) *RemoteImageServiceBuilder {
	b.tracerProvider = tp
	// 记录“已显式设置”，使显式 nil 与未设置（零值）得以区分。
	b.tracerProviderSet = true
	return b
}

// WithUseStreaming controls whether streaming RPCs are used for list
// operations. If the runtime returns an Unimplemented error, the client
// automatically falls back to the corresponding unary RPC.
// WithUseStreaming 控制列表操作是否使用流式 RPC；若运行时返回 Unimplemented，
// 客户端会自动回退到对应的一元 RPC。
func (b *RemoteImageServiceBuilder) WithUseStreaming(useStreaming bool) *RemoteImageServiceBuilder {
	b.useStreaming = useStreaming
	return b
}

// Build creates a new internalapi.ImageManagerService using the configured
// options.
// Build 按已配置的选项创建 internalapi.ImageManagerService。
func (b *RemoteImageServiceBuilder) Build(ctx context.Context) (internalapi.ImageManagerService, error) {
	// endpoint 与连接超时均为必填项，提前校验可避免建连时出现难以理解的错误。
	if b.endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	if b.connectionTimeout <= 0 {
		return nil, errors.New("connectionTimeout must be positive")
	}
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Connecting to image service", "endpoint", b.endpoint)
	// 把端点字符串解析为 gRPC 拨号地址与对应的 dialer（unix socket 需要自定义 dialer）。
	addr, dialer, err := util.GetAddressAndDialer(b.endpoint)
	if err != nil {
		return nil, err
	}

	// 建连与随后的服务校验都受连接超时约束，超时后自动取消。
	ctx, cancel := context.WithTimeout(ctx, b.connectionTimeout)
	defer cancel()

	var dialOpts []grpc.DialOption
	// CRI 走本地连接，因此使用不安全传输；authority 固定为 localhost；
	// 自定义 dialer 负责连到端点；并放宽单次接收消息大小以容纳大镜像元数据。
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority("localhost"),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	// When no tracer provider was configured, fall back to a noop provider
	// so context propagation still works without producing real traces.
	// See https://github.com/open-telemetry/opentelemetry-go-contrib/tree/main/examples/passthrough
	// 未配置 tracer provider 时退化为 noop：不产生真实 span，但仍能传播 trace 上下文。
	if !b.tracerProviderSet {
		b.tracerProvider = noop.NewTracerProvider()
	}
	// Install the otelgrpc stats handler unless the caller explicitly opted
	// out by calling WithTracerProvider(nil).
	// 只有显式传 nil 才会跳过 stats handler，从而同时关闭追踪与上下文传播。
	if b.tracerProvider != nil {
		tracingOpts := []otelgrpc.Option{
			otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
			otelgrpc.WithPropagators(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})),
			otelgrpc.WithTracerProvider(b.tracerProvider),
		}
		// 将配置好的 otelgrpc 客户端 handler 加入拨号选项。
		dialOpts = append(dialOpts,
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(tracingOpts...)))
	}

	// 配置建连退避参数，避免运行时尚未就绪时发生频繁重连。
	connParams := grpc.ConnectParams{
		Backoff: backoff.DefaultConfig,
	}
	connParams.MinConnectTimeout = minConnectionTimeout
	connParams.Backoff.BaseDelay = baseBackoffDelay
	connParams.Backoff.MaxDelay = maxBackoffDelay
	// 将退避参数写入拨号选项。
	dialOpts = append(dialOpts,
		grpc.WithConnectParams(connParams),
	)

	// grpc.NewClient 只是惰性创建客户端，并不保证服务端可达；真正的可达性
	// 由下面的 validateServiceConnection 通过一次 RPC 验证。
	conn, err := grpc.NewClient(clientTargetForAddress(addr), dialOpts...)
	if err != nil {
		logger.Error(err, "Connect remote image service failed", "address", addr)
		return nil, err
	}

	// 构造服务对象，并把流式开关写入原子变量供各 RPC 读取。
	service := &remoteImageService{
		timeout: b.connectionTimeout,
		conn:    conn,
	}
	service.useStreaming.Store(b.useStreaming)

	// 校验 CRI v1 镜像 API 可用；失败时包装错误上下文后返回。
	if err := service.validateServiceConnection(ctx, conn, b.endpoint); err != nil {
		return nil, fmt.Errorf("validate service connection: %w", err)
	}

	return service, nil
}

// NewRemoteImageService creates a new internalapi.ImageManagerService.
// If useStreaming is true, streaming RPCs will be used for list operations
// instead of unary RPCs. If the runtime returns an Unimplemented error,
// the client automatically falls back to unary RPCs.
//
// Deprecated: Use NewRemoteImageServiceBuilder so that default values
// (e.g. useStreaming) can be changed in a single place without updating
// every callsite.
// NewRemoteImageService 是旧版构造函数，仅为兼容保留，内部委托给 builder，
// 以便默认值集中在一处维护。
func NewRemoteImageService(ctx context.Context, endpoint string, connectionTimeout time.Duration, tp trace.TracerProvider, useStreaming bool) (internalapi.ImageManagerService, error) {
	// 把各参数映射为 builder 选项，建连与校验仍由 Build 统一完成。
	return NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(connectionTimeout).
		WithTracerProvider(tp).
		WithUseStreaming(useStreaming).
		Build(ctx)
}

// Close will shutdown the internal gRPC client connection.
// Close 关闭内部持有的 gRPC 客户端连接。
func (r *remoteImageService) Close(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Closing image service connection")
	// 连接关闭后该服务对象不应再被使用；关闭错误直接返回给调用方。
	return r.conn.Close()
}

// validateServiceConnection tries to connect to the remote image service by
// using the CRI v1 API version and fails if that's not possible.
// validateServiceConnection 通过 CRI v1 镜像 API 验证远端服务可用，
// 不可用时返回错误。
func (r *remoteImageService) validateServiceConnection(ctx context.Context, conn *grpc.ClientConn, endpoint string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Validating the CRI v1 API image version")
	// 先创建 v1 客户端桩，后续所有镜像 RPC 都复用它。
	r.imageClient = runtimeapi.NewImageServiceClient(conn)

	// 用一次轻量的 ImageFsInfo 调用探测服务端是否实现了 CRI v1 镜像 API；
	// grpc.NewClient 本身是惰性的，不会发现服务端不兼容。
	if _, err := r.imageClient.ImageFsInfo(ctx, &runtimeapi.ImageFsInfoRequest{}); err != nil {
		return fmt.Errorf("validate CRI v1 image API for endpoint %q: %w", endpoint, err)
	}

	logger.V(2).Info("Validated CRI v1 image API")
	return nil
}

// ListImages lists available images.
// ListImages 列出节点上已有的镜像。
func (r *remoteImageService) ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	// 列表操作统一加超时，避免运行时卡住时 kubelet 被无限阻塞。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 特性开启且尚未被回退时走流式 RPC，否则走传统的一元 RPC。
	if r.useStreaming.Load() {
		return r.streamImagesV1(ctx, filter)
	}
	return r.listImagesV1(ctx, filter)
}

// listImagesV1 通过一元 ListImages RPC 获取镜像列表，并透传过滤条件。
func (r *remoteImageService) listImagesV1(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	// 将 filter 直接放入请求，nil 表示不过滤。
	resp, err := r.imageClient.ListImages(ctx, &runtimeapi.ListImagesRequest{
		Filter: filter,
	})
	if err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "ListImages with filter from image service failed", "filter", filter)
		// 调用失败时不返回部分结果，错误原样上抛。
		return nil, err
	}

	// 成功时只返回镜像列表。
	return resp.Images, nil
}

// streamImagesV1 通过流式 StreamImages RPC 分批接收镜像列表。
func (r *remoteImageService) streamImagesV1(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	logger := klog.FromContext(ctx)
	// 建立流时带上过滤条件；服务端未实现该 RPC 时错误要到 Recv 才会出现。
	stream, err := r.imageClient.StreamImages(ctx, &runtimeapi.StreamImagesRequest{
		Filter: filter,
	})
	if err != nil {
		logger.Error(err, "StreamImages from image service failed", "filter", filter)
		return nil, err
	}

	var images []*runtimeapi.Image
	for {
		// 逐批接收响应，直到服务端关闭流（io.EOF）。
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// 服务端未实现流式 RPC 时，持久关闭流式开关并回退到一元 RPC，
			// 使后续调用不再重复尝试流式接口。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamImages not implemented, falling back to ListImages", "filter", filter)
				r.useStreaming.Store(false)
				return r.listImagesV1(ctx, filter)
			}
			// 其他错误直接上抛，并带上已接收条目数便于排查。
			logger.Error(err, "StreamImages recv failed", "filter", filter, "itemsReceived", len(images))
			return nil, err
		}
		// 累积各批镜像，最终一次性返回完整列表。
		images = append(images, resp.Images...)
	}

	return images, nil
}

// ImageStatus returns the status of the image.
// ImageStatus 查询指定镜像的状态。
func (r *remoteImageService) ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	// 查询操作加超时，避免运行时无响应时长时间阻塞。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.imageStatusV1(ctx, image, verbose)
}

// imageStatusV1 调用 CRI v1 ImageStatus RPC，并校验返回镜像的关键字段是否完整。
func (r *remoteImageService) imageStatusV1(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	// verbose 决定运行时是否返回额外的镜像信息。
	resp, err := r.imageClient.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{
		Image:   image,
		Verbose: verbose,
	})
	if err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "Get ImageStatus from image service failed", "image", image.Image)
		return nil, err
	}

	// 镜像存在时，CRI 要求 Id 与 Size 必须有效；缺失说明运行时返回的数据不合法。
	if resp.Image != nil {
		if resp.Image.Id == "" || resp.Image.Size == 0 {
			errorMessage := fmt.Sprintf("Id or size of image %q is not set", image.Image)
			err := errors.New(errorMessage)
			logger := klog.FromContext(ctx)
			logger.Error(err, "ImageStatus failed", "image", image.Image)
			return nil, err
		}
	}

	return resp, nil
}

// PullImage pulls an image with authentication config.
// PullImage 按给定的认证信息拉取镜像，返回运行时分配的镜像引用。
func (r *remoteImageService) PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	// 拉取镜像可能耗时很长，这里只派生可取消的 context 而不设置固定超时，
	// 生命周期由上层（如 kubelet）通过取消父 context 控制。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return r.pullImageV1(ctx, image, auth, podSandboxConfig)
}

// pullImageV1 通过 CRI v1 PullImage RPC 拉取镜像，并规范化错误与返回值。
func (r *remoteImageService) pullImageV1(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	// 认证信息与 PodSandboxConfig 一并下发，便于运行时按 sandbox 的网络、
	// 凭证环境完成拉取。
	resp, err := r.imageClient.PullImage(ctx, &runtimeapi.PullImageRequest{
		Image:         image,
		Auth:          auth,
		SandboxConfig: podSandboxConfig,
	})
	if err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "PullImage from image service failed", "image", image.Image)

		// We can strip the code from unknown status errors since they add no value
		// and will make them easier to read in the logs/events.
		//
		// It also ensures that checking custom error types from pkg/kubelet/images/types.go
		// works in `imageManager.EnsureImageExists` (pkg/kubelet/images/image_manager.go).
		// 对 codes.Unknown 错误去掉状态码、只保留消息：状态码没有额外价值，
		// 还会妨碍 kubelet 侧识别 pkg/kubelet/images/types.go 中的自定义错误类型。
		statusErr, ok := status.FromError(err)
		if ok && statusErr.Code() == codes.Unknown {
			return "", errors.New(statusErr.Message())
		}

		return "", err
	}

	// 运行时必须返回非空 ImageRef，否则上层后续无法引用该镜像。
	if resp.ImageRef == "" {
		logger := klog.FromContext(ctx)
		logger.Error(errors.New("PullImage failed"), "ImageRef of image is not set", "image", image.Image)
		errorMessage := fmt.Sprintf("imageRef of image %q is not set", image.Image)
		return "", errors.New(errorMessage)
	}

	return resp.ImageRef, nil
}

// RemoveImage removes the image.
// RemoveImage 从运行时中删除指定镜像。
func (r *remoteImageService) RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) (err error) {
	// 删除操作加超时，避免运行时卡住。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 响应体无需使用，只需判断是否出错；失败时记录日志并上抛错误。
	if _, err = r.imageClient.RemoveImage(ctx, &runtimeapi.RemoveImageRequest{
		Image: image,
	}); err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "RemoveImage from image service failed", "image", image.Image)
		return err
	}

	return nil
}

// ImageFsInfo returns information of the filesystem that is used to store images.
// ImageFsInfo 返回用于存储镜像的文件系统信息。
func (r *remoteImageService) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	// 查询操作加超时，避免运行时无响应时长时间阻塞。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.imageFsInfoV1(ctx)
}

// imageFsInfoV1 调用 CRI v1 ImageFsInfo RPC 获取镜像存储的文件系统信息。
func (r *remoteImageService) imageFsInfoV1(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	// 该 RPC 无需请求参数，直接发起调用。
	resp, err := r.imageClient.ImageFsInfo(ctx, &runtimeapi.ImageFsInfoRequest{})
	if err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "ImageFsInfo from image service failed")
		return nil, err
	}
	return resp, nil
}
