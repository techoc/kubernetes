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
	"strings"
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

	"k8s.io/component-base/logs/logreduction"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"

	"k8s.io/cri-client/pkg/util"
)

// remoteRuntimeService is a gRPC implementation of internalapi.RuntimeService.
// remoteRuntimeService 是 internalapi.RuntimeService 接口的 gRPC 实现，负责把 CRI 调用转发给容器运行时。
type remoteRuntimeService struct {
	// timeout 是单次同步请求的超时时间；runtimeClient 是调用 CRI RuntimeService 的 gRPC 客户端。
	timeout       time.Duration
	runtimeClient runtimeapi.RuntimeServiceClient
	// Cache last per-container error message to reduce log spam
	// logReduction 按容器缓存最近一次的错误消息，抑制重复错误日志刷屏。
	// conn 是与容器运行时建立的底层 gRPC 连接，关闭服务时需要一并释放。
	logReduction *logreduction.LogReduction
	conn         *grpc.ClientConn
	// useStreaming indicates whether to use streaming RPCs for list operations
	// when the CRIListStreaming feature gate is enabled. It falls back to false
	// if a streaming RPC returns Unimplemented. Multiple goroutines may
	// concurrently observe Unimplemented and store false, but that is harmless
	// because the store is idempotent.
	// useStreaming 标记列表类操作是否改用流式 RPC；遇到 Unimplemented 时会被置回 false。
	// 多个协程可能同时写入 false，但写入是幂等的，无需加锁保护。
	useStreaming atomic.Bool
}

const (
	// How frequently to report identical errors
	// identicalErrorDelay 是相同错误消息允许重复上报的最小间隔。
	identicalErrorDelay = 1 * time.Minute

	// connection parameters
	// 以下常量共同约束重连退避策略：最大退避、初始退避和最小连接超时。
	maxBackoffDelay      = 3 * time.Second
	baseBackoffDelay     = 100 * time.Millisecond
	minConnectionTimeout = 5 * time.Second
)

// CRIVersion is the type for valid Container Runtime Interface (CRI) API
// versions.
// CRIVersion 表示合法的 CRI API 版本类型。
type CRIVersion string

var (
	// ErrContainerStatusNil indicates that the returned container status is nil.
	// ErrContainerStatusNil 表示运行时返回的容器状态为空。
	ErrContainerStatusNil = errors.New("container status is nil")

	// ErrCommandTimedOut indicates that the exec sync command timed.
	// ErrCommandTimedOut 表示 exec 同步命令执行超时。
	ErrCommandTimedOut = errors.New("command timed out")
)

const (
	// CRIVersionV1 references the v1 CRI API.
	// CRIVersionV1 表示 v1 版本的 CRI API。
	CRIVersionV1 CRIVersion = "v1"
)

// RemoteRuntimeServiceBuilder builds a new internalapi.RuntimeService.
//
// Construct a builder with NewRemoteRuntimeServiceBuilder, then chain With*
// methods to set non-default options before calling Build. Adding new options
// here is preferred over adding parameters to NewRemoteRuntimeService so that
// default values can be changed in a single place without having to update
// every callsite.
// RemoteRuntimeServiceBuilder 负责构建 internalapi.RuntimeService。
// 先调用 NewRemoteRuntimeServiceBuilder 取得 builder，再链式调用 With* 设置非默认选项，最后调用 Build。
// 把新选项放在这里优于给 NewRemoteRuntimeService 增加参数：默认值只需在一处修改，无需改动所有调用点。
type RemoteRuntimeServiceBuilder struct {
	// endpoint 是运行时服务的 gRPC 端点；connectionTimeout 是建连和请求的超时时间；
	// tracerProvider 是用于 gRPC 插桩的 OpenTelemetry tracer provider。
	endpoint          string
	connectionTimeout time.Duration
	tracerProvider    trace.TracerProvider
	// tracerProviderSet tracks whether WithTracerProvider was called, so that
	// an explicit nil can be distinguished from the unset default and used to
	// opt out of installing the otelgrpc stats handler entirely.
	// tracerProviderSet 记录是否调用过 WithTracerProvider，用于区分“显式传入 nil”和“未设置”。
	// 显式 nil 意味着完全不安装 otelgrpc stats handler。
	tracerProviderSet bool
	// useStreaming indicates whether to use streaming RPCs for list operations
	// when the CRIListStreaming feature gate is enabled. It is expected to
	// default to true once the feature graduates to GA.
	// useStreaming 标记列表操作是否使用流式 RPC（受 CRIListStreaming 特性门控）。
	// 该特性毕业为 GA 后预期默认值为 true。
	useStreaming bool
}

// NewRemoteRuntimeServiceBuilder returns a builder with default options for
// constructing a remote runtime service.
// NewRemoteRuntimeServiceBuilder 返回一个使用默认选项的 builder，用于构造远端运行时服务。
func NewRemoteRuntimeServiceBuilder() *RemoteRuntimeServiceBuilder {
	// 直接返回零值实例，默认值在 Build 中补全，选项由 With* 方法设置。
	return &RemoteRuntimeServiceBuilder{}
}

// WithEndpoint sets the gRPC endpoint of the remote runtime service.
// WithEndpoint 设置远端运行时服务的 gRPC 端点。
func (b *RemoteRuntimeServiceBuilder) WithEndpoint(endpoint string) *RemoteRuntimeServiceBuilder {
	b.endpoint = endpoint
	// 返回 builder 本身以支持链式调用。
	return b
}

// WithConnectionTimeout sets the timeout used when connecting to the remote
// runtime service.
// WithConnectionTimeout 设置连接远端运行时服务所使用的超时时间。
func (b *RemoteRuntimeServiceBuilder) WithConnectionTimeout(connectionTimeout time.Duration) *RemoteRuntimeServiceBuilder {
	b.connectionTimeout = connectionTimeout
	// 返回 builder 本身以支持链式调用。
	return b
}

// WithTracerProvider sets the OpenTelemetry tracer provider used to
// instrument the gRPC client. If WithTracerProvider is not called, the
// otelgrpc stats handler is installed with a noop tracer provider, so no
// traces are produced but gRPC context propagation still works. Passing a nil
// provider explicitly opts out of installing the stats handler entirely,
// disabling both tracing and context propagation.
// WithTracerProvider 设置用于 gRPC 插桩的 OpenTelemetry tracer provider。
// 未调用本方法时会用 noop provider 安装 stats handler：不产生 trace，但保留上下文传播。
// 显式传入 nil 则完全不安装 stats handler，同时关闭追踪与上下文传播。
func (b *RemoteRuntimeServiceBuilder) WithTracerProvider(tp trace.TracerProvider) *RemoteRuntimeServiceBuilder {
	b.tracerProvider = tp
	// 用单独的布尔量记住“调用过本方法”，从而把显式 nil 和未设置区分开。
	b.tracerProviderSet = true
	// 返回 builder 本身以支持链式调用。
	return b
}

// WithUseStreaming controls whether streaming RPCs are used for list
// operations. If the runtime returns an Unimplemented error, the client
// automatically falls back to the corresponding unary RPC.
// NOTE: useStreaming is supposed to be gated by the CRIListStreaming feature
// gate and is expected to default to true once the feature graduates to GA,
// at which point this method may be removed.
// WithUseStreaming 控制列表操作是否使用流式 RPC；运行时返回 Unimplemented 时会自动回退到相应的一元 RPC。
// 注意：本选项应由 CRIListStreaming 特性门控，特性 GA 后预期默认值为 true，届时本方法可能被移除。
func (b *RemoteRuntimeServiceBuilder) WithUseStreaming(useStreaming bool) *RemoteRuntimeServiceBuilder {
	b.useStreaming = useStreaming
	// 返回 builder 本身以支持链式调用。
	return b
}

// Build creates a new internalapi.RuntimeService using the configured
// options.
// Build 按已配置的选项创建 internalapi.RuntimeService。
func (b *RemoteRuntimeServiceBuilder) Build(ctx context.Context) (internalapi.RuntimeService, error) {
	// endpoint 是必填项，缺失时尽早报错，避免后续连接到空地址。
	if b.endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	// 连接超时必须为正数，否则派生的 context 会立即过期。
	if b.connectionTimeout <= 0 {
		return nil, errors.New("connectionTimeout must be positive")
	}
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Connecting to runtime service", "endpoint", b.endpoint)
	// 把端点解析为地址与拨号器，以同时支持 unix socket 等本地传输方式。
	addr, dialer, err := util.GetAddressAndDialer(b.endpoint)
	if err != nil {
		return nil, err
	}
	// 用配置的超时限制整个建连与校验过程，defer 保证及时释放定时器资源。
	ctx, cancel := context.WithTimeout(ctx, b.connectionTimeout)
	defer cancel()

	// 组装拨号选项：CRI 走本地 insecure 连接，并放宽接收消息大小上限。
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority("localhost"),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	// When no tracer provider was configured, fall back to a noop provider
	// so context propagation still works without producing real traces.
	// See https://github.com/open-telemetry/opentelemetry-go-contrib/tree/main/examples/passthrough
	// 未配置 tracer provider 时回退到 noop provider：不产生真实 trace，但仍保持上下文传播。
	if !b.tracerProviderSet {
		b.tracerProvider = noop.NewTracerProvider()
	}
	// Install the otelgrpc stats handler unless the caller explicitly opted
	// out by calling WithTracerProvider(nil).
	// 除非调用方通过 WithTracerProvider(nil) 显式关闭，否则安装 otelgrpc stats handler。
	if b.tracerProvider != nil {
		// 记录消息收发事件，并使用 TraceContext/Baggage 传播器透传链路上下文。
		tracingOpts := []otelgrpc.Option{
			otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
			otelgrpc.WithPropagators(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})),
			otelgrpc.WithTracerProvider(b.tracerProvider),
		}
		dialOpts = append(dialOpts,
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(tracingOpts...)))
	}

	// 设置连接退避参数，限定重连间隔的基数与上限，避免频繁重连打满运行时。
	connParams := grpc.ConnectParams{
		Backoff: backoff.DefaultConfig,
	}
	connParams.MinConnectTimeout = minConnectionTimeout
	connParams.Backoff.BaseDelay = baseBackoffDelay
	connParams.Backoff.MaxDelay = maxBackoffDelay
	dialOpts = append(dialOpts,
		grpc.WithConnectParams(connParams),
	)

	// grpc.NewClient 只创建客户端、不立即建连；真正的连通性由下面的 validateServiceConnection 校验。
	conn, err := grpc.NewClient(clientTargetForAddress(addr), dialOpts...)
	if err != nil {
		logger.Error(err, "Connect remote runtime service failed", "address", addr)
		return nil, err
	}

	// 构造服务对象，保存超时、日志抑制器与连接；useStreaming 用原子变量存储以支持并发读写。
	service := &remoteRuntimeService{
		timeout:      b.connectionTimeout,
		logReduction: logreduction.NewLogReduction(identicalErrorDelay),
		conn:         conn,
	}
	service.useStreaming.Store(b.useStreaming)

	// 通过一次 Version 调用确认 CRI v1 API 确实可用，否则视为构建失败。
	if err := service.validateServiceConnection(ctx, conn, b.endpoint); err != nil {
		return nil, fmt.Errorf("validate service connection: %w", err)
	}

	// 校验通过才返回服务实例，调用方无需再关心建连细节。
	return service, nil
}

// NewRemoteRuntimeService creates a new internalapi.RuntimeService.
// If useStreaming is true, streaming RPCs will be used for list operations
// instead of unary RPCs. If the runtime returns an Unimplemented error,
// the client automatically falls back to unary RPCs.
//
// Deprecated: Use NewRemoteRuntimeServiceBuilder so that default values
// (e.g. useStreaming) can be changed in a single place without updating
// every callsite.
// NewRemoteRuntimeService 直接构造远端运行时服务；已废弃，请改用 builder 以集中管理默认值。
func NewRemoteRuntimeService(ctx context.Context, endpoint string, connectionTimeout time.Duration, tp trace.TracerProvider, useStreaming bool) (internalapi.RuntimeService, error) {
	// 委托给 builder，按参数顺序链式设置选项后统一调用 Build。
	return NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(connectionTimeout).
		WithTracerProvider(tp).
		WithUseStreaming(useStreaming).
		Build(ctx)
}

// Close will shutdown the internal gRPC client connection.
// Close 关闭内部持有的 gRPC 客户端连接。
func (r *remoteRuntimeService) Close(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Closing runtime service connection")
	// context 仅用于取 logger；真正的释放由底层 grpc.ClientConn 完成。
	return r.conn.Close()
}

// validateServiceConnection tries to connect to the remote runtime service by
// using the CRI v1 API version and fails if that's not possible.
// validateServiceConnection 用 CRI v1 API 校验与远端运行时服务的连通性，不支持则构建失败。
func (r *remoteRuntimeService) validateServiceConnection(ctx context.Context, conn *grpc.ClientConn, endpoint string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Validating the CRI v1 API runtime version")
	// 基于传入连接创建 v1 RuntimeService 客户端并保存到服务对象，后续调用复用它。
	r.runtimeClient = runtimeapi.NewRuntimeServiceClient(conn)

	// 发起一次空的 Version 请求，确认运行时确实实现了 CRI v1 API。
	if _, err := r.runtimeClient.Version(ctx, &runtimeapi.VersionRequest{}); err != nil {
		return fmt.Errorf("validate CRI v1 runtime API for endpoint %q: %w", endpoint, err)
	}

	logger.V(2).Info("Validated CRI v1 runtime API")
	return nil
}

// Version returns the runtime name, runtime version and runtime API version.
// Version 返回运行时名称、运行时版本以及运行时 API 版本。
func (r *remoteRuntimeService) Version(ctx context.Context, apiVersion string) (*runtimeapi.VersionResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] Version", "apiVersion", apiVersion, "timeout", r.timeout)

	// 为本次调用设独立超时，避免慢运行时无限阻塞调用方。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 当前仅支持 v1，委托给具体的 v1 实现。
	return r.versionV1(ctx, apiVersion)
}

// versionV1 实现 CRI v1 的 Version 调用，并校验响应字段完整性。
func (r *remoteRuntimeService) versionV1(ctx context.Context, apiVersion string) (*runtimeapi.VersionResponse, error) {
	// 构造 VersionRequest，携带调用方要求的 API 版本。
	typedVersion, err := r.runtimeClient.Version(ctx, &runtimeapi.VersionRequest{
		Version: apiVersion,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Version from runtime service failed")
		return nil, err
	}

	logger.V(10).Info("[RemoteRuntimeService] Version Response", "apiVersion", typedVersion)

	// kubelet 依赖以下四个字段，逐一收集缺失项后一次性报错，便于定位问题。
	var missingFields []string
	if typedVersion.Version == "" {
		missingFields = append(missingFields, "Version")
	}
	if typedVersion.RuntimeName == "" {
		missingFields = append(missingFields, "RuntimeName")
	}
	if typedVersion.RuntimeApiVersion == "" {
		missingFields = append(missingFields, "RuntimeApiVersion")
	}
	if typedVersion.RuntimeVersion == "" {
		missingFields = append(missingFields, "RuntimeVersion")
	}
	if len(missingFields) > 0 {
		return nil, fmt.Errorf("not all fields are set in VersionResponse (missing %s)", strings.Join(missingFields, ", "))
	}

	// 字段齐全才把响应交给调用方；此处 err 必为 nil。
	return typedVersion, err
}

// RunPodSandbox creates and starts a pod-level sandbox. Runtimes should ensure
// the sandbox is in ready state.
// RunPodSandbox 创建并启动一个 Pod 级别的 sandbox，运行时应保证其进入 ready 状态。
func (r *remoteRuntimeService) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	// Use 2 times longer timeout for sandbox operation (4 mins by default)
	// TODO: Make the pod sandbox timeout configurable.
	// sandbox 创建涉及镜像拉取与网络配置，耗时通常远超普通调用，故使用两倍超时。
	timeout := r.timeout * 2

	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] RunPodSandbox", "config", config, "runtimeHandler", runtimeHandler, "timeout", timeout)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 把配置和运行时处理器（如不同 CRI 运行时）一并传给运行时。
	resp, err := r.runtimeClient.RunPodSandbox(ctx, &runtimeapi.RunPodSandboxRequest{
		Config:         config,
		RuntimeHandler: runtimeHandler,
	})

	if err != nil {
		logger.Error(err, "RunPodSandbox from runtime service failed")
		return "", err
	}

	podSandboxID := resp.PodSandboxId

	// 即使 RPC 成功，也必须拿到非空 ID，否则后续无法引用该 sandbox。
	if podSandboxID == "" {
		errorMessage := fmt.Sprintf("PodSandboxId is not set for sandbox %q", config.Metadata)
		err := errors.New(errorMessage)
		logger.Error(err, "RunPodSandbox failed")
		return "", err
	}

	logger.V(10).Info("[RemoteRuntimeService] RunPodSandbox Response", "podSandboxID", podSandboxID)

	return podSandboxID, nil
}

// StopPodSandbox stops the sandbox. If there are any running containers in the
// sandbox, they should be forced to termination.
// StopPodSandbox 停止 sandbox；其中的运行中容器应被强制终止。
func (r *remoteRuntimeService) StopPodSandbox(ctx context.Context, podSandBoxID string) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] StopPodSandbox", "podSandboxID", podSandBoxID, "timeout", r.timeout)

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 只需返回错误，响应体无有用信息；失败时记录带 ID 的日志便于排查。
	if _, err := r.runtimeClient.StopPodSandbox(ctx, &runtimeapi.StopPodSandboxRequest{
		PodSandboxId: podSandBoxID,
	}); err != nil {
		logger.Error(err, "StopPodSandbox from runtime service failed", "podSandboxID", podSandBoxID)
		return err
	}

	logger.V(10).Info("[RemoteRuntimeService] StopPodSandbox Response", "podSandboxID", podSandBoxID)

	return nil
}

// RemovePodSandbox removes the sandbox. If there are any containers in the
// sandbox, they should be forcibly removed.
// RemovePodSandbox 删除 sandbox；其中的容器应被强制删除。
func (r *remoteRuntimeService) RemovePodSandbox(ctx context.Context, podSandBoxID string) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] RemovePodSandbox", "podSandboxID", podSandBoxID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 删除操作要求运行时清理该 sandbox 下的所有容器，失败直接上抛。
	if _, err := r.runtimeClient.RemovePodSandbox(ctx, &runtimeapi.RemovePodSandboxRequest{
		PodSandboxId: podSandBoxID,
	}); err != nil {
		logger.Error(err, "RemovePodSandbox from runtime service failed", "podSandboxID", podSandBoxID)
		return err
	}

	logger.V(10).Info("[RemoteRuntimeService] RemovePodSandbox Response", "podSandboxID", podSandBoxID)

	return nil
}

// PodSandboxStatus returns the status of the PodSandbox.
// PodSandboxStatus 返回指定 PodSandbox 的状态。
func (r *remoteRuntimeService) PodSandboxStatus(ctx context.Context, podSandBoxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] PodSandboxStatus", "podSandboxID", podSandBoxID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.podSandboxStatusV1(ctx, podSandBoxID, verbose)
}

// podSandboxStatusV1 实现 CRI v1 的 PodSandboxStatus 调用，并校验状态字段。
func (r *remoteRuntimeService) podSandboxStatusV1(ctx context.Context, podSandBoxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	// verbose 为 true 时运行时可返回额外的调试信息。
	resp, err := r.runtimeClient.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{
		PodSandboxId: podSandBoxID,
		Verbose:      verbose,
	})
	if err != nil {
		return nil, err
	}

	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] PodSandboxStatus Response", "podSandboxID", podSandBoxID, "status", resp.Status)

	status := resp.Status
	// 状态为 nil 时交由上层处理；非 nil 时校验其字段是否满足 CRI 约定。
	if resp.Status != nil {
		if err := verifySandboxStatus(status); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// ListPodSandbox returns a list of PodSandboxes.
// ListPodSandbox 按过滤器返回 PodSandbox 列表。
func (r *remoteRuntimeService) ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ListPodSandbox", "filter", filter, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 根据特性开关选择流式或一元实现；流式实现在运行时不支持时会自动回退。
	if r.useStreaming.Load() {
		return r.streamPodSandboxesV1(ctx, filter)
	}
	return r.listPodSandboxV1(ctx, filter)
}

// listPodSandboxV1 通过一元 RPC 获取 PodSandbox 列表。
func (r *remoteRuntimeService) listPodSandboxV1(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	// 过滤器可为 nil，表示不过滤、返回全部 sandbox。
	resp, err := r.runtimeClient.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: filter,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListPodSandbox with filter from runtime service failed", "filter", filter)
		return nil, err
	}

	logger.V(10).Info("[RemoteRuntimeService] ListPodSandbox Response", "filter", filter, "items", resp.Items)

	return resp.Items, nil
}

// streamPodSandboxesV1 通过流式 RPC 分页接收 PodSandbox，遇到不支持时回退到一元 RPC。
func (r *remoteRuntimeService) streamPodSandboxesV1(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	logger := klog.FromContext(ctx)
	// 先创建流；注意 Unimplemented 通常不会在此时返回，而在后续 Recv 时才暴露。
	stream, err := r.runtimeClient.StreamPodSandboxes(ctx, &runtimeapi.StreamPodSandboxesRequest{
		Filter: filter,
	})
	if err != nil {
		logger.Error(err, "StreamPodSandboxes from runtime service failed", "filter", filter)
		return nil, err
	}

	// 流按批次返回数据，需逐批累加到结果切片，直到读到 EOF。
	var items []*runtimeapi.PodSandbox
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// 运行时未实现该流式 RPC：关闭全局流式开关，并用一元 RPC 重新执行本次查询。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamPodSandboxes not implemented, falling back to ListPodSandbox", "filter", filter)
				r.useStreaming.Store(false)
				return r.listPodSandboxV1(ctx, filter)
			}
			logger.Error(err, "StreamPodSandboxes recv failed", "filter", filter, "itemsReceived", len(items))
			return nil, err
		}
		items = append(items, resp.PodSandboxes...)
	}

	logger.V(10).Info("[RemoteRuntimeService] StreamPodSandboxes Response", "filter", filter, "items", items)

	return items, nil
}

// CreateContainer creates a new container in the specified PodSandbox.
// CreateContainer 在指定的 PodSandbox 中创建一个新容器。
func (r *remoteRuntimeService) CreateContainer(ctx context.Context, podSandBoxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] CreateContainer", "podSandboxID", podSandBoxID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.createContainerV1(ctx, podSandBoxID, config, sandboxConfig)
}

// createContainerV1 实现 CRI v1 的 CreateContainer 调用，并校验返回的容器 ID。
func (r *remoteRuntimeService) createContainerV1(ctx context.Context, podSandBoxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	logger := klog.FromContext(ctx)
	// 同时传入 sandbox ID、容器配置和 sandbox 配置，后者用于继承网络/卷等上下文。
	resp, err := r.runtimeClient.CreateContainer(ctx, &runtimeapi.CreateContainerRequest{
		PodSandboxId:  podSandBoxID,
		Config:        config,
		SandboxConfig: sandboxConfig,
	})
	if err != nil {
		logger.Error(err, "CreateContainer in sandbox from runtime service failed", "podSandboxID", podSandBoxID)
		return "", err
	}

	logger.V(10).Info("[RemoteRuntimeService] CreateContainer", "podSandboxID", podSandBoxID, "containerID", resp.ContainerId)
	// 创建成功但 ID 为空属于异常响应，必须报错，否则后续无法启动该容器。
	if resp.ContainerId == "" {
		errorMessage := fmt.Sprintf("ContainerId is not set for container %q", config.Metadata)
		err := errors.New(errorMessage)
		logger.Error(err, "CreateContainer failed")
		return "", err
	}

	return resp.ContainerId, nil
}

// StartContainer starts the container.
// StartContainer 启动已创建的容器。
func (r *remoteRuntimeService) StartContainer(ctx context.Context, containerID string) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] StartContainer", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 只关心错误；响应体为空。
	if _, err := r.runtimeClient.StartContainer(ctx, &runtimeapi.StartContainerRequest{
		ContainerId: containerID,
	}); err != nil {
		logger.Error(err, "StartContainer from runtime service failed", "containerID", containerID)
		return err
	}
	logger.V(10).Info("[RemoteRuntimeService] StartContainer Response", "containerID", containerID)

	return nil
}

// StopContainer stops a running container with a grace period (i.e., timeout).
// StopContainer 在宽限期内停止运行中的容器，超时后运行时应强制终止。
func (r *remoteRuntimeService) StopContainer(ctx context.Context, containerID string, timeout int64) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] StopContainer", "containerID", containerID, "timeout", timeout)
	// Use timeout + default timeout (2 minutes) as timeout to leave extra time
	// for SIGKILL container and request latency.
	// 在优雅停止时长之上再叠加默认超时，给 SIGKILL 和请求往返留出余量，避免 RPC 提前超时。
	t := r.timeout + time.Duration(timeout)*time.Second
	ctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()

	// 清洗该容器的历史错误记录，使本次操作产生的错误能重新被记录。
	r.logReduction.ClearID(containerID)

	if _, err := r.runtimeClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: containerID,
		Timeout:     timeout,
	}); err != nil {
		logger.Error(err, "StopContainer from runtime service failed", "containerID", containerID)
		return err
	}
	logger.V(10).Info("[RemoteRuntimeService] StopContainer Response", "containerID", containerID)

	return nil
}

// RemoveContainer removes the container. If the container is running, the container
// should be forced to removal.
// RemoveContainer 删除容器；若容器仍在运行，运行时应强制删除。
func (r *remoteRuntimeService) RemoveContainer(ctx context.Context, containerID string) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] RemoveContainer", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 与 StopContainer 同理，先清除该容器的错误抑制记录。
	r.logReduction.ClearID(containerID)
	if _, err := r.runtimeClient.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{
		ContainerId: containerID,
	}); err != nil {
		logger.Error(err, "RemoveContainer from runtime service failed", "containerID", containerID)
		return err
	}
	logger.V(10).Info("[RemoteRuntimeService] RemoveContainer Response", "containerID", containerID)

	return nil
}

// ListContainers lists containers by filters.
// ListContainers 按过滤器列出容器。
func (r *remoteRuntimeService) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ListContainers", "filter", filter, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 按开关选择流式或一元实现；流式不被支持时会自动回退。
	if r.useStreaming.Load() {
		return r.streamContainersV1(ctx, filter)
	}
	return r.listContainersV1(ctx, filter)
}

// listContainersV1 通过一元 RPC 获取容器列表。
func (r *remoteRuntimeService) listContainersV1(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	// filter 为 nil 表示返回全部容器。
	resp, err := r.runtimeClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: filter,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListContainers with filter from runtime service failed", "filter", filter)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] ListContainers Response", "filter", filter, "containers", resp.Containers)

	return resp.Containers, nil
}

// streamContainersV1 通过流式 RPC 分批接收容器列表，不支持时回退到一元 RPC。
func (r *remoteRuntimeService) streamContainersV1(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	logger := klog.FromContext(ctx)
	stream, err := r.runtimeClient.StreamContainers(ctx, &runtimeapi.StreamContainersRequest{
		Filter: filter,
	})
	if err != nil {
		logger.Error(err, "StreamContainers from runtime service failed", "filter", filter)
		return nil, err
	}

	// 逐批读取并汇总，直到 EOF 表示流结束。
	var containers []*runtimeapi.Container
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// Unimplemented 在 Recv 阶段才出现，此时关闭流式开关并改用一元 RPC 重查。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamContainers not implemented, falling back to ListContainers", "filter", filter)
				r.useStreaming.Store(false)
				return r.listContainersV1(ctx, filter)
			}
			logger.Error(err, "StreamContainers recv failed", "filter", filter, "itemsReceived", len(containers))
			return nil, err
		}
		containers = append(containers, resp.Containers...)
	}

	logger.V(10).Info("[RemoteRuntimeService] StreamContainers Response", "filter", filter, "containers", containers)

	return containers, nil
}

// ContainerStatus returns the container status.
// ContainerStatus 返回指定容器的状态。
func (r *remoteRuntimeService) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ContainerStatus", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.containerStatusV1(ctx, containerID, verbose)
}

// containerStatusV1 实现 CRI v1 的 ContainerStatus 调用，并对错误日志做抑制、对状态做校验。
func (r *remoteRuntimeService) containerStatusV1(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	resp, err := r.runtimeClient.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
		ContainerId: containerID,
		Verbose:     verbose,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		// Don't spam the log with endless messages about the same failure.
		// kubelet 会高频轮询状态；同一容器的相同错误只在间隔到期后打印一次。
		if r.logReduction.ShouldMessageBePrinted(err.Error(), containerID) {
			logger.Error(err, "ContainerStatus from runtime service failed", "containerID", containerID)
		}
		return nil, err
	}
	// 调用恢复成功后清除抑制记录，便于下次失败时立即记录。
	r.logReduction.ClearID(containerID)
	logger.V(10).Info("[RemoteRuntimeService] ContainerStatus Response", "containerID", containerID, "status", resp.Status)

	status := resp.Status
	// 状态非 nil 时校验关键字段，避免把不完整的状态交给上层。
	if resp.Status != nil {
		if err := verifyContainerStatus(status); err != nil {
			logger.Error(err, "verify ContainerStatus failed", "containerID", containerID)
			return nil, err
		}
	}

	return resp, nil
}

// UpdateContainerResources updates a containers resource config
// UpdateContainerResources 更新容器的资源配置。
func (r *remoteRuntimeService) UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] UpdateContainerResources", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 从 resources 中分别取 Linux/Windows 配置，缺失的一项为 nil，由运行时按需忽略。
	if _, err := r.runtimeClient.UpdateContainerResources(ctx, &runtimeapi.UpdateContainerResourcesRequest{
		ContainerId: containerID,
		Linux:       resources.GetLinux(),
		Windows:     resources.GetWindows(),
	}); err != nil {
		logger.Error(err, "UpdateContainerResources from runtime service failed", "containerID", containerID)
		return err
	}
	logger.V(10).Info("[RemoteRuntimeService] UpdateContainerResources Response", "containerID", containerID)

	return nil
}

// ExecSync executes a command in the container, and returns the stdout output.
// If command exits with a non-zero exit code, an error is returned.
// ExecSync 在容器内同步执行命令并返回标准输出；命令以非零码退出时会返回错误。
func (r *remoteRuntimeService) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ExecSync", "containerID", containerID, "timeout", timeout)
	// Do not set timeout when timeout is 0.
	// timeout 为 0 表示不限制命令执行时长，此时只挂 context.Cancel 而不设截止时间。
	var cancel context.CancelFunc
	if timeout != 0 {
		// Use timeout + default timeout (2 minutes) as timeout to leave some time for
		// the runtime to do cleanup.
		// 额外叠加默认超时，给运行时清理进程留出时间，避免父 context 先于命令结束而取消。
		ctx, cancel = context.WithTimeout(ctx, r.timeout+timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return r.execSyncV1(ctx, containerID, cmd, timeout)
}

// execSyncV1 实现 CRI v1 的 ExecSync 调用，把 gRPC 超时映射为包级错误并包装非零退出码。
func (r *remoteRuntimeService) execSyncV1(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	// CRI 字段以秒为单位，这里把 time.Duration 转成整数秒。
	timeoutSeconds := int64(timeout.Seconds())
	req := &runtimeapi.ExecSyncRequest{
		ContainerId: containerID,
		Cmd:         cmd,
		Timeout:     timeoutSeconds,
	}
	resp, err := r.runtimeClient.ExecSync(ctx, req)
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ExecSync cmd from runtime service failed", "containerID", containerID, "cmd", cmd)

		// interpret DeadlineExceeded gRPC errors as timedout errors
		// 把 gRPC 的 DeadlineExceeded 转译为更易识别的 ErrCommandTimedOut，便于上层分类处理。
		if status.Code(err) == codes.DeadlineExceeded {
			err = fmt.Errorf("%w: %q timed out after %s", ErrCommandTimedOut, strings.Join(cmd, " "), timeout)
		}

		return nil, nil, err
	}

	logger.V(10).Info("[RemoteRuntimeService] ExecSync Response", "containerID", containerID, "exitCode", resp.ExitCode)
	err = nil
	// 命令以非零码退出并未导致 RPC 失败，需要包装成 CodeExitError 以便保留退出码信息。
	if resp.ExitCode != 0 {
		err = utilexec.CodeExitError{
			Err:  fmt.Errorf("command '%s' exited with %d: %s", strings.Join(cmd, " "), resp.ExitCode, resp.Stderr),
			Code: int(resp.ExitCode),
		}
	}

	// 无论是否出错都返回捕获到的 stdout/stderr，调用方可据此诊断。
	return resp.Stdout, resp.Stderr, err
}

// Exec prepares a streaming endpoint to execute a command in the container, and returns the address.
// Exec 准备一个用于在容器中执行命令的流式端点，并返回其地址。
func (r *remoteRuntimeService) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] Exec", "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.execV1(ctx, req)
}

// execV1 实现 CRI v1 的 Exec 调用，并确保返回的流地址非空。
func (r *remoteRuntimeService) execV1(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	resp, err := r.runtimeClient.Exec(ctx, req)
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Exec cmd from runtime service failed", "containerID", req.ContainerId, "cmd", req.Cmd)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] Exec Response")

	// 这里只拿到 URL，真正的流式传输由调用方后续连接该地址完成；URL 为空则响应无效。
	if resp.Url == "" {
		errorMessage := "URL is not set"
		err := errors.New(errorMessage)
		logger.Error(err, "Exec failed")
		return nil, err
	}

	return resp, nil
}

// Attach prepares a streaming endpoint to attach to a running container, and returns the address.
// Attach 准备一个连接到运行中容器的流式端点，并返回其地址。
func (r *remoteRuntimeService) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] Attach", "containerID", req.ContainerId, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.attachV1(ctx, req)
}

// attachV1 实现 CRI v1 的 Attach 调用，并确保返回的流地址非空。
func (r *remoteRuntimeService) attachV1(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	logger := klog.FromContext(ctx)
	resp, err := r.runtimeClient.Attach(ctx, req)
	if err != nil {
		logger.Error(err, "Attach container from runtime service failed", "containerID", req.ContainerId)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] Attach Response", "containerID", req.ContainerId)

	// 与 Exec 相同，URL 为空意味着运行时没有给出可用的 attach 端点。
	if resp.Url == "" {
		errorMessage := "URL is not set"
		err := errors.New(errorMessage)
		logger.Error(err, "Attach failed")
		return nil, err
	}
	return resp, nil
}

// PortForward prepares a streaming endpoint to forward ports from a PodSandbox, and returns the address.
// PortForward 准备一个从 PodSandbox 转发端口的流式端点，并返回其地址。
func (r *remoteRuntimeService) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] PortForward", "podSandboxID", req.PodSandboxId, "port", req.Port, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.portForwardV1(ctx, req)
}

// portForwardV1 实现 CRI v1 的 PortForward 调用，并确保返回的流地址非空。
func (r *remoteRuntimeService) portForwardV1(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	resp, err := r.runtimeClient.PortForward(ctx, req)

	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "PortForward from runtime service failed", "podSandboxID", req.PodSandboxId)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] PortForward Response", "podSandboxID", req.PodSandboxId)

	// 缺少 URL 时该响应无法用于建立端口转发连接，按失败处理。
	if resp.Url == "" {
		errorMessage := "URL is not set"
		err := errors.New(errorMessage)
		logger.Error(err, "PortForward failed")
		return nil, err
	}

	return resp, nil
}

// UpdatePodSandboxResources synchronously updates the PodSandboxConfig with
// the pod-level resource configuration.
// UpdatePodSandboxResources 用 Pod 级别的资源配置同步更新 PodSandboxConfig。
func (r *remoteRuntimeService) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] UpdatePodSandboxResources", "PodSandboxId", req.PodSandboxId, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 直接透传请求与响应，客户端不做字段改写。
	resp, err := r.runtimeClient.UpdatePodSandboxResources(ctx, req)
	if err != nil {
		logger.Error(err, "UpdatePodSandboxResources from runtime service failed", "podSandboxID", req.PodSandboxId)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] UpdatePodSandboxResources Response", "podSandboxID", req.PodSandboxId)

	return resp, nil
}

// UpdateRuntimeConfig updates the config of a runtime service. The only
// update payload currently supported is the pod CIDR assigned to a node,
// and the runtime service just proxies it down to the network plugin.
// UpdateRuntimeConfig 更新运行时服务的配置；目前仅支持节点 Pod CIDR，运行时会把它转交给网络插件。
func (r *remoteRuntimeService) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *runtimeapi.RuntimeConfig) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] UpdateRuntimeConfig", "runtimeConfig", runtimeConfig, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Response doesn't contain anything of interest. This translates to an
	// Event notification to the network plugin, which can't fail, so we're
	// really looking to surface destination unreachable.
	// 响应体没有有效信息：该调用本质是给网络插件发通知，不会失败，这里主要关注目标是否可达。
	if _, err := r.runtimeClient.UpdateRuntimeConfig(ctx, &runtimeapi.UpdateRuntimeConfigRequest{
		RuntimeConfig: runtimeConfig,
	}); err != nil {
		return err
	}
	logger.V(10).Info("[RemoteRuntimeService] UpdateRuntimeConfig Response", "runtimeConfig", runtimeConfig)

	return nil
}

// Status returns the status of the runtime.
// Status 返回运行时整体的状态信息。
func (r *remoteRuntimeService) Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] Status", "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.statusV1(ctx, verbose)
}

// statusV1 实现 CRI v1 的 Status 调用，并确认关键健康条件已上报。
func (r *remoteRuntimeService) statusV1(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	logger := klog.FromContext(ctx)
	resp, err := r.runtimeClient.Status(ctx, &runtimeapi.StatusRequest{
		Verbose: verbose,
	})
	if err != nil {
		logger.Error(err, "Status from runtime service failed")
		return nil, err
	}

	logger.V(10).Info("[RemoteRuntimeService] Status Response", "status", resp.Status)

	// kubelet 依赖 RuntimeReady 与 NetworkReady 两个条件；缺失时视为无效响应。
	if resp.Status == nil || len(resp.Status.Conditions) < 2 {
		errorMessage := "RuntimeReady or NetworkReady condition are not set"
		err := errors.New(errorMessage)
		logger.Error(err, "Status failed")
		return nil, err
	}

	return resp, nil
}

// ContainerStats returns the stats of the container.
// ContainerStats 返回单个容器的统计信息。
func (r *remoteRuntimeService) ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ContainerStats", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.containerStatsV1(ctx, containerID)
}

// containerStatsV1 实现 CRI v1 的 ContainerStats 调用，并对重复错误做日志抑制。
func (r *remoteRuntimeService) containerStatsV1(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	logger := klog.FromContext(ctx)
	resp, err := r.runtimeClient.ContainerStats(ctx, &runtimeapi.ContainerStatsRequest{
		ContainerId: containerID,
	})
	if err != nil {
		// 统计信息会被高频采集，同一容器的连续相同错误只在间隔到期后打印。
		if r.logReduction.ShouldMessageBePrinted(err.Error(), containerID) {
			logger.Error(err, "ContainerStats from runtime service failed", "containerID", containerID)
		}
		return nil, err
	}
	// 成功后清除抑制记录，下次失败可立即上报。
	r.logReduction.ClearID(containerID)
	logger.V(10).Info("[RemoteRuntimeService] ContainerStats Response", "containerID", containerID, "stats", resp.GetStats())

	return resp.GetStats(), nil
}

// ListContainerStats returns the list of ContainerStats given the filter.
// ListContainerStats 按过滤器返回容器统计信息列表。
func (r *remoteRuntimeService) ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ListContainerStats", "filter", filter)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 按开关选择流式或一元实现；流式不支持时自动回退。
	if r.useStreaming.Load() {
		return r.streamContainerStatsV1(ctx, filter)
	}
	return r.listContainerStatsV1(ctx, filter)
}

// listContainerStatsV1 通过一元 RPC 获取容器统计信息列表。
func (r *remoteRuntimeService) listContainerStatsV1(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	resp, err := r.runtimeClient.ListContainerStats(ctx, &runtimeapi.ListContainerStatsRequest{
		Filter: filter,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListContainerStats with filter from runtime service failed", "filter", filter)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] ListContainerStats Response", "filter", filter, "stats", resp.GetStats())

	return resp.GetStats(), nil
}

// streamContainerStatsV1 通过流式 RPC 分批接收容器统计信息，不支持时回退到一元 RPC。
func (r *remoteRuntimeService) streamContainerStatsV1(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	logger := klog.FromContext(ctx)
	stream, err := r.runtimeClient.StreamContainerStats(ctx, &runtimeapi.StreamContainerStatsRequest{
		Filter: filter,
	})
	if err != nil {
		logger.Error(err, "StreamContainerStats from runtime service failed", "filter", filter)
		return nil, err
	}

	// 逐批汇总结果，直到读到 EOF。
	var stats []*runtimeapi.ContainerStats
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// 运行时未实现该流式 RPC：关闭流式开关并回退到一元 RPC 重查。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamContainerStats not implemented, falling back to ListContainerStats", "filter", filter)
				r.useStreaming.Store(false)
				return r.listContainerStatsV1(ctx, filter)
			}
			logger.Error(err, "StreamContainerStats recv failed", "filter", filter, "itemsReceived", len(stats))
			return nil, err
		}
		stats = append(stats, resp.ContainerStats...)
	}

	logger.V(10).Info("[RemoteRuntimeService] StreamContainerStats Response", "filter", filter, "stats", stats)

	return stats, nil
}

// PodSandboxStats returns the stats of the pod.
// PodSandboxStats 返回单个 Pod 的统计信息。
func (r *remoteRuntimeService) PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] PodSandboxStats", "podSandboxID", podSandboxID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.podSandboxStatsV1(ctx, podSandboxID)
}

// podSandboxStatsV1 实现 CRI v1 的 PodSandboxStats 调用，并对重复错误做日志抑制。
func (r *remoteRuntimeService) podSandboxStatsV1(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	logger := klog.FromContext(ctx)
	resp, err := r.runtimeClient.PodSandboxStats(ctx, &runtimeapi.PodSandboxStatsRequest{
		PodSandboxId: podSandboxID,
	})
	if err != nil {
		// 与容器统计类似，按 sandbox ID 抑制重复的失败日志。
		if r.logReduction.ShouldMessageBePrinted(err.Error(), podSandboxID) {
			logger.Error(err, "PodSandbox from runtime service failed", "podSandboxID", podSandboxID)
		}
		return nil, err
	}
	// 恢复成功后清除抑制记录。
	r.logReduction.ClearID(podSandboxID)
	logger.V(10).Info("[RemoteRuntimeService] PodSandbox Response", "podSandboxID", podSandboxID, "stats", resp.GetStats())

	return resp.GetStats(), nil
}

// ListPodSandboxStats returns the list of pod sandbox stats given the filter
// ListPodSandboxStats 按过滤器返回 Pod sandbox 的统计信息列表。
func (r *remoteRuntimeService) ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ListPodSandboxStats", "filter", filter)
	// Set timeout, because runtimes are able to cache disk stats results
	// 运行时可能缓存磁盘统计结果，因此仍需设置超时防止调用悬挂。
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 按开关选择流式或一元实现；流式不支持时自动回退。
	if r.useStreaming.Load() {
		return r.streamPodSandboxStatsV1(ctx, filter)
	}
	return r.listPodSandboxStatsV1(ctx, filter)
}

// listPodSandboxStatsV1 通过一元 RPC 获取 Pod sandbox 统计信息列表。
func (r *remoteRuntimeService) listPodSandboxStatsV1(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	resp, err := r.runtimeClient.ListPodSandboxStats(ctx, &runtimeapi.ListPodSandboxStatsRequest{
		Filter: filter,
	})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListPodSandboxStats with filter from runtime service failed", "filter", filter)
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] ListPodSandboxStats Response", "filter", filter, "stats", resp.GetStats())

	return resp.GetStats(), nil
}

// streamPodSandboxStatsV1 通过流式 RPC 分批接收 Pod sandbox 统计信息，不支持时回退到一元 RPC。
func (r *remoteRuntimeService) streamPodSandboxStatsV1(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	logger := klog.FromContext(ctx)
	stream, err := r.runtimeClient.StreamPodSandboxStats(ctx, &runtimeapi.StreamPodSandboxStatsRequest{
		Filter: filter,
	})
	if err != nil {
		logger.Error(err, "StreamPodSandboxStats from runtime service failed", "filter", filter)
		return nil, err
	}

	// 逐批汇总，直到流结束。
	var stats []*runtimeapi.PodSandboxStats
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// 运行时未实现该流式 RPC：关闭流式开关并回退到一元 RPC 重查。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamPodSandboxStats not implemented, falling back to ListPodSandboxStats", "filter", filter)
				r.useStreaming.Store(false)
				return r.listPodSandboxStatsV1(ctx, filter)
			}
			logger.Error(err, "StreamPodSandboxStats recv failed", "filter", filter, "itemsReceived", len(stats))
			return nil, err
		}
		stats = append(stats, resp.PodSandboxStats...)
	}

	logger.V(10).Info("[RemoteRuntimeService] StreamPodSandboxStats Response", "filter", filter, "stats", stats)

	return stats, nil
}

// ReopenContainerLog reopens the container log file.
// ReopenContainerLog 重新打开容器的日志文件（例如日志轮转后需要重新链接）。
func (r *remoteRuntimeService) ReopenContainerLog(ctx context.Context, containerID string) (err error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info("[RemoteRuntimeService] ReopenContainerLog", "containerID", containerID, "timeout", r.timeout)
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 只关心错误；响应体为空。
	if _, err := r.runtimeClient.ReopenContainerLog(ctx, &runtimeapi.ReopenContainerLogRequest{ContainerId: containerID}); err != nil {
		logger.Error(err, "ReopenContainerLog from runtime service failed", "containerID", containerID)
		return err
	}

	logger.V(10).Info("[RemoteRuntimeService] ReopenContainerLog Response", "containerID", containerID)
	return nil
}

// CheckpointContainer triggers a checkpoint of the given CheckpointContainerRequest
// CheckpointContainer 根据给定的 CheckpointContainerRequest 触发容器检查点操作。
func (r *remoteRuntimeService) CheckpointContainer(ctx context.Context, options *runtimeapi.CheckpointContainerRequest) error {
	logger := klog.FromContext(ctx)
	logger.V(10).Info(
		"[RemoteRuntimeService] CheckpointContainer",
		"options",
		options,
	)
	// 参数校验：nil 请求无法解引用，负超时也无意义，均在发起 RPC 前拒绝。
	if options == nil {
		return errors.New("CheckpointContainer requires non-nil CheckpointRestoreOptions parameter")
	}
	if options.Timeout < 0 {
		return errors.New("CheckpointContainer requires the timeout value to be > 0")
	}

	// 用立即执行的匿名函数计算最终超时：优先采用用户指定的更大值，否则回退到 CRI 默认超时。
	ctx, cancel := func(ctx context.Context) (context.Context, context.CancelFunc) {
		defaultTimeout := int64(r.timeout / time.Second)
		if options.Timeout > defaultTimeout {
			// The user requested a specific timeout, let's use that if it
			// is larger than the CRI default.
			// 用户请求的超时大于默认值时，按用户请求执行。
			return context.WithTimeout(ctx, time.Duration(options.Timeout)*time.Second)
		}
		// If the user requested a timeout less than the
		// CRI default, let's use the CRI default.
		// 用户请求的超时小于默认值时改写请求字段并采用 CRI 默认超时。
		options.Timeout = defaultTimeout
		return context.WithTimeout(ctx, r.timeout)
	}(ctx)
	defer cancel()

	_, err := r.runtimeClient.CheckpointContainer(
		ctx,
		options,
	)

	if err != nil {
		logger.Error(
			err,
			"CheckpointContainer from runtime service failed",
			"containerID",
			options.ContainerId,
		)
		return err
	}
	logger.V(10).Info(
		"[RemoteRuntimeService] CheckpointContainer Response",
		"containerID",
		options.ContainerId,
	)

	return nil
}

// CheckpointPod triggers a checkpoint of the given CheckpointPodRequest
// CheckpointPod 根据给定的 CheckpointPodRequest 触发 Pod 检查点操作。
func (r *remoteRuntimeService) CheckpointPod(ctx context.Context, options *runtimeapi.CheckpointPodRequest) error {
	logger := klog.FromContext(ctx)
	logger.V(10).Info(
		"[RemoteRuntimeService] CheckpointPod",
		"options",
		options,
	)
	// 必须先做 nil 校验，后续才能安全读取 options 字段。
	if options == nil {
		return errors.New("CheckpointPod requires non-nil CheckpointPodRequest parameter")
	}

	// The checkpoint is bounded by the caller's context deadline (the kubelet
	// sets it from PodCheckpoint.spec.timeoutSeconds, clamped to its configured
	// ceiling) rather than a request field (KEP-5823). The default runtime
	// request timeout applies only when the caller did not set a deadline.
	// 检查点的时限由调用方 context 的 deadline 决定（KEP-5823），而非请求字段；
	// 仅当调用方未设置 deadline 时，才回退到运行时默认请求超时。
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	_, err := r.runtimeClient.CheckpointPod(
		ctx,
		options,
	)

	if err != nil {
		logger.Error(
			err,
			"CheckpointPod from runtime service failed",
			"podSandboxID",
			options.PodSandboxId,
		)
		return err
	}
	logger.V(10).Info(
		"[RemoteRuntimeService] CheckpointPod Response",
		"podSandboxID",
		options.PodSandboxId,
	)

	return nil
}

// RestorePod restores a pod sandbox from a checkpoint
// RestorePod 从检查点恢复一个 Pod sandbox。
func (r *remoteRuntimeService) RestorePod(ctx context.Context, options *runtimeapi.RestorePodRequest) (*runtimeapi.RestorePodResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(10).Info(
		"[RemoteRuntimeService] RestorePod",
		"options",
		options,
	)
	// 与 CheckpointPod 一致，先做 nil 校验以避免解引用空指针。
	if options == nil {
		return nil, errors.New("RestorePod requires non-nil RestorePodRequest parameter")
	}

	// As with CheckpointPod, the restore is bounded by the caller's context
	// deadline rather than a request field (KEP-5823). The default runtime
	// request timeout applies only when the caller did not set a deadline.
	// 与 CheckpointPod 相同，恢复时限由调用方 context 的 deadline 决定（KEP-5823）；
	// 仅当调用方未设置 deadline 时才回退到默认请求超时。
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	resp, err := r.runtimeClient.RestorePod(
		ctx,
		options,
	)

	if err != nil {
		logger.Error(
			err,
			"RestorePod from runtime service failed",
			"checkpointPath",
			options.CheckpointPath,
		)
		return nil, err
	}
	// 即使 RPC 成功也需防御性检查 nil 响应，避免后续解引用崩溃。
	if resp == nil {
		return nil, errors.New("RestorePod runtime service returned a nil response")
	}
	logger.V(10).Info(
		"[RemoteRuntimeService] RestorePod Response",
		"podSandboxID",
		resp.PodSandboxId,
		"restoredContainerCount",
		len(resp.RestoredContainers),
	)

	return resp, nil
}

// GetContainerEvents 建立容器事件的流式订阅，把收到的事件投递到 channel，并在连接就绪时回调通知调用方。
func (r *remoteRuntimeService) GetContainerEvents(ctx context.Context, containerEventsCh chan *runtimeapi.ContainerEventResponse, connectionEstablishedCallback func(runtimeapi.RuntimeService_GetContainerEventsClient)) error {
	// 先创建事件流客户端；建连失败则直接返回错误。
	containerEventsStreamingClient, err := r.runtimeClient.GetContainerEvents(ctx, &runtimeapi.GetEventsRequest{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "GetContainerEvents failed to get streaming client")
		return err
	}

	if connectionEstablishedCallback != nil {
		// The connection is successfully established and we have a streaming client ready for use.
		// 连接已就绪且拿到可用客户端，回调可用于启动依赖该流的后续逻辑。
		connectionEstablishedCallback(containerEventsStreamingClient)
	}

	// 持续接收事件：EOF 表示流被关闭，其他错误同样视为终止；空响应则跳过。
	for {
		resp, err := containerEventsStreamingClient.Recv()
		if errors.Is(err, io.EOF) {
			logger.Error(err, "container events stream is closed")
			return err
		}
		if err != nil {
			logger.Error(err, "failed to receive streaming container event")
			return err
		}
		if resp != nil {
			containerEventsCh <- resp
			logger.V(4).Info("container event received", "resp", resp)
		}
	}
}

// ListMetricDescriptors gets the descriptors for the metrics that will be returned in ListPodSandboxMetrics.
// ListMetricDescriptors 获取 ListPodSandboxMetrics 将要返回的指标描述符。
func (r *remoteRuntimeService) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	resp, err := r.runtimeClient.ListMetricDescriptors(ctx, &runtimeapi.ListMetricDescriptorsRequest{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListMetricDescriptors from runtime service failed")
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] ListMetricDescriptors Response", "stats", resp.GetDescriptors())

	return resp.GetDescriptors(), nil
}

// ListPodSandboxMetrics retrieves the metrics for all pod sandboxes.
// ListPodSandboxMetrics 获取所有 Pod sandbox 的指标。
func (r *remoteRuntimeService) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// 按开关选择流式或一元实现；流式不支持时自动回退。
	if r.useStreaming.Load() {
		return r.streamPodSandboxMetricsV1(ctx)
	}
	return r.listPodSandboxMetricsV1(ctx)
}

// listPodSandboxMetricsV1 通过一元 RPC 获取 Pod sandbox 指标列表。
func (r *remoteRuntimeService) listPodSandboxMetricsV1(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	resp, err := r.runtimeClient.ListPodSandboxMetrics(ctx, &runtimeapi.ListPodSandboxMetricsRequest{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "ListPodSandboxMetrics from runtime service failed")
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] ListPodSandboxMetrics Response", "stats", resp.GetPodMetrics())

	return resp.GetPodMetrics(), nil
}

// streamPodSandboxMetricsV1 通过流式 RPC 分批接收 Pod sandbox 指标，不支持时回退到一元 RPC。
func (r *remoteRuntimeService) streamPodSandboxMetricsV1(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	logger := klog.FromContext(ctx)
	stream, err := r.runtimeClient.StreamPodSandboxMetrics(ctx, &runtimeapi.StreamPodSandboxMetricsRequest{})
	if err != nil {
		logger.Error(err, "StreamPodSandboxMetrics from runtime service failed")
		return nil, err
	}

	// 逐批汇总指标，直到流结束。
	var metrics []*runtimeapi.PodSandboxMetrics
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// If the RPC is unimplemented, disable streaming and fall back to the unary RPC.
			// The Unimplemented status is not returned when creating the stream,
			// but when calling Recv() on the stream.
			// 运行时未实现该流式 RPC：关闭流式开关并回退到一元 RPC 重查。
			if status.Code(err) == codes.Unimplemented {
				logger.Info("StreamPodSandboxMetrics not implemented, falling back to ListPodSandboxMetrics")
				r.useStreaming.Store(false)
				return r.listPodSandboxMetricsV1(ctx)
			}
			logger.Error(err, "StreamPodSandboxMetrics recv failed", "itemsReceived", len(metrics))
			return nil, err
		}
		metrics = append(metrics, resp.PodSandboxMetrics...)
	}

	logger.V(10).Info("[RemoteRuntimeService] StreamPodSandboxMetrics Response", "metrics", metrics)

	return metrics, nil
}

// RuntimeConfig returns the configuration information of the runtime.
// RuntimeConfig 返回运行时的配置信息。
func (r *remoteRuntimeService) RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	resp, err := r.runtimeClient.RuntimeConfig(ctx, &runtimeapi.RuntimeConfigRequest{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "RuntimeConfig from runtime service failed")
		return nil, err
	}
	logger.V(10).Info("[RemoteRuntimeService] RuntimeConfigResponse", "linuxConfig", resp.GetLinux())

	return resp, nil
}
