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

package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"k8s.io/klog/v2"
	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	drapbv1beta1 "k8s.io/kubelet/pkg/apis/dra/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// defaultClientCallTimeout 是 DRA 驱动响应任意 gRPC 调用的默认时限。
// kubelet 通过向 RegisterPlugin 传 nil 来使用该值。部分测试会使用更短的
// 超时以加快测试。
//
// 该值是 kubelet 重试周期的一半（依据
// https://github.com/kubernetes/kubernetes/commit/0449cef8fd5217d394c5cd331d852bd50983e6b3）。
const defaultClientCallTimeout = 45 * time.Second

// 本包装器支持的所有 API 版本，按从新到旧排序。
var servicesSupportedByKubelet = []string{
	drapbv1.DRAPluginService,
	drapbv1beta1.DRAPluginService,
}

// kubelet 支持的所有 DRAResourceHealth API 版本，按从新到旧排序。
// kubelet 选择插件同样声明支持的第一个版本。
//
// TODO(harche)：在 1.40 中移除 v1alpha1。它作为三个版本的过渡期
// （1.37 至 1.39）存在，供那些在 v1 出现之前就已发布 v1alpha1
// 健康服务的驱动使用。
var healthServicesSupportedByKubelet = []string{
	drahealthv1.DRAResourceHealthService,
	drahealthv1alpha1.DRAResourceHealthService,
}

// DRAPlugin 描述一个已注册的 DRA 驱动插件。它通过调用插件实现的 gRPC 接口
// 来完成 kubelet 侧的 prepare/unprepare 操作。
type DRAPlugin struct {
	driverName    string
	conn          *grpc.ClientConn
	endpoint      string
	chosenService string // 例如 drapbv1.DRAPluginService
	// chosenHealthService 是协商确定的 DRAResourceHealth gRPC 服务
	// （例如 drahealthv1.DRAResourceHealthService）；插件未提供可选健康服务时为空。
	chosenHealthService string
	clientCallTimeout   time.Duration

	mutex         sync.Mutex
	backgroundCtx context.Context

	healthStreamCtx    context.Context
	healthStreamCancel context.CancelFunc
}

// DriverName 返回该插件的驱动名称。
func (p *DRAPlugin) DriverName() string {
	return p.driverName
}

// NodePrepareResources 调用插件的 NodePrepareResources RPC，为 Pod 准备资源。
func (p *DRAPlugin) NodePrepareResources(
	ctx context.Context,
	req *drapbv1.NodePrepareResourcesRequest,
	opts ...grpc.CallOption,
) (*drapbv1.NodePrepareResourcesResponse, error) {
	logger := klog.FromContext(ctx).WithName("dra-plugin")
	// 为日志附加驱动名与端点，并写入上下文供后续调用复用。
	logger = klog.LoggerWithValues(logger, "driverName", p.driverName, "endpoint", p.endpoint)
	ctx = klog.NewContext(ctx, logger)
	logger.V(4).Info("Calling NodePrepareResources rpc", "request", req)

	// 为单次 RPC 设置超时，避免插件无响应时阻塞 kubelet。
	ctx, cancel := context.WithTimeout(ctx, p.clientCallTimeout)
	defer cancel()

	var err error
	var response *drapbv1.NodePrepareResourcesResponse
	// 按注册时协商的服务版本选择对应的客户端实现。
	switch p.chosenService {
	case drapbv1beta1.DRAPluginService:
		client := drapbv1beta1.NewDRAPluginClient(p.conn)
		response, err = drapbv1beta1.V1Beta1ClientWrapper{DRAPluginClient: client}.NodePrepareResources(ctx, req)
	case drapbv1.DRAPluginService:
		client := drapbv1.NewDRAPluginClient(p.conn)
		response, err = client.NodePrepareResources(ctx, req)
	default:
		// 不应发生：validateSupportedServices 只会返回这里支持的服务。
		return nil, fmt.Errorf("internal error: unsupported chosen service: %q", p.chosenService)
	}
	logger.V(4).Info("Done calling NodePrepareResources rpc", "response", response, "err", err)
	return response, err
}

// NodeUnprepareResources 调用插件的 NodeUnprepareResources RPC，释放 Pod 占用的资源。
func (p *DRAPlugin) NodeUnprepareResources(
	ctx context.Context,
	req *drapbv1.NodeUnprepareResourcesRequest,
	opts ...grpc.CallOption,
) (*drapbv1.NodeUnprepareResourcesResponse, error) {
	logger := klog.FromContext(ctx).WithName("dra-plugin")
	// 为日志附加驱动名与端点，并写入上下文供后续调用复用。
	logger = klog.LoggerWithValues(logger, "driverName", p.driverName, "endpoint", p.endpoint)
	ctx = klog.NewContext(ctx, logger)

	logger.V(4).Info("Calling NodeUnprepareResource rpc", "request", req)

	// 为单次 RPC 设置超时，避免插件无响应时阻塞 kubelet。
	ctx, cancel := context.WithTimeout(ctx, p.clientCallTimeout)
	defer cancel()

	var err error
	var response *drapbv1.NodeUnprepareResourcesResponse
	// 按注册时协商的服务版本选择对应的客户端实现。
	switch p.chosenService {
	case drapbv1beta1.DRAPluginService:
		client := drapbv1beta1.NewDRAPluginClient(p.conn)
		response, err = drapbv1beta1.V1Beta1ClientWrapper{DRAPluginClient: client}.NodeUnprepareResources(ctx, req)
	case drapbv1.DRAPluginService:
		client := drapbv1.NewDRAPluginClient(p.conn)
		response, err = client.NodeUnprepareResources(ctx, req)
	default:
		// 不应发生：validateSupportedServices 只会返回这里支持的服务。
		return nil, fmt.Errorf("internal error: unsupported chosen service: %q", p.chosenService)
	}
	logger.V(4).Info("Done calling NodeUnprepareResources rpc", "response", response, "err", err)
	return response, err
}

// newMetricsInterceptor 返回记录 DRA gRPC 调用耗时的客户端拦截器。
func newMetricsInterceptor(driverName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, conn, opts...)
		// 以驱动名、方法名与状态码为标签记录调用耗时。
		metrics.DRAGRPCOperationsDuration.WithLabelValues(driverName, method, status.Code(err).String()).Observe(time.Since(start).Seconds())
		return err
	}
}

// SetHealthStream 保存当前健康流的上下文与取消函数。
func (p *DRAPlugin) SetHealthStream(ctx context.Context, cancel context.CancelFunc) {
	// 加锁保护健康流上下文，避免与取消操作发生竞态。
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.healthStreamCtx = ctx
	p.healthStreamCancel = cancel
}

// HealthStreamCancel 返回当前健康流的取消函数（如果存在）。
func (p *DRAPlugin) HealthStreamCancel() context.CancelFunc {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.healthStreamCancel
}

// NodeWatchResources 建立接收 DRA 插件健康更新的流，使用注册时协商的健康 gRPC
// 服务版本。当插件只支持 v1alpha1 时，返回的流会透明地把响应转换为 v1，
// 使 kubelet 其余部分只需处理 v1 类型。
func (p *DRAPlugin) NodeWatchResources(ctx context.Context) (drahealthv1.DRAResourceHealth_NodeWatchResourcesClient, error) {
	logger := klog.FromContext(ctx).WithName("dra-plugin")
	logger = klog.LoggerWithValues(logger, "driverName", p.driverName, "endpoint", p.endpoint)

	logger.V(4).Info("Starting WatchResources stream", "healthService", p.chosenHealthService)
	var stream drahealthv1.DRAResourceHealth_NodeWatchResourcesClient
	var err error
	// 健康服务只在注册时协商成功才会启动；这里按协商结果选择客户端。
	switch p.chosenHealthService {
	case drahealthv1.DRAResourceHealthService:
		client := drahealthv1.NewDRAResourceHealthClient(p.conn)
		stream, err = client.NodeWatchResources(ctx, &drahealthv1.NodeWatchResourcesRequest{})
	case drahealthv1alpha1.DRAResourceHealthService:
		client := drahealthv1alpha1.NewDRAResourceHealthClient(p.conn)
		stream, err = drahealthv1.V1Alpha1ClientWrapper{Client: client}.NodeWatchResources(ctx, &drahealthv1.NodeWatchResourcesRequest{})
	default:
		// 不应发生：只有在注册时协商出健康服务后才会启动健康流。
		return nil, fmt.Errorf("internal error: unsupported chosen health service: %q", p.chosenHealthService)
	}
	if err != nil {
		// 建立流失败时记录日志并返回错误，由上层决定是否重试。
		logger.Error(err, "NodeWatchResources RPC call failed")
		return nil, err
	}

	logger.V(4).Info("NodeWatchResources stream initiated successfully")
	return stream, nil
}
