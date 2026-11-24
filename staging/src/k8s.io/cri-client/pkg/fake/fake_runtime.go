/*
Copyright 2017 The Kubernetes Authors.

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

package fake

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	kubeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	apitest "k8s.io/cri-api/pkg/apis/testing"
	"k8s.io/cri-client/pkg/util"
	utilexec "k8s.io/utils/exec"
)

// RemoteRuntime 是假（测试用）的远程容器运行时，同时充当 CRI RuntimeService 与 ImageService 的 gRPC 服务端。
// 它本身主要负责请求参数转换和响应包装，真正的状态维护与业务逻辑委托给内嵌的两个假服务。
type RemoteRuntime struct {
	// server 是底层的 gRPC 服务端实例，负责在指定端点上监听并处理 RPC。
	server *grpc.Server
	// RuntimeService 保存假的运行时服务，维护 PodSandbox、容器及其状态、统计与指标等数据。
	RuntimeService *apitest.FakeRuntimeService
	// ImageService 保存假的镜像服务，维护镜像列表、镜像状态与文件系统用量等数据。
	ImageService *apitest.FakeImageService
	// UnsafeImageServiceServer 嵌入后提供 mustEmbedUnimplementedImageServiceServer 方法，
	// 使本类型满足镜像服务 gRPC 服务端的注册要求。
	kubeapi.UnsafeImageServiceServer
	// UnsafeRuntimeServiceServer 嵌入后提供 mustEmbedUnimplementedRuntimeServiceServer 方法，
	// 使本类型满足运行时服务 gRPC 服务端的注册要求。
	kubeapi.UnsafeRuntimeServiceServer
}

// NewFakeRemoteRuntime 创建一个新的 RemoteRuntime：构造假的运行时/镜像服务与 gRPC 服务端，
// 并把当前对象注册为这两个 CRI 服务的 gRPC 实现。
func NewFakeRemoteRuntime() *RemoteRuntime {
	// 分别创建独立的假运行时服务与假镜像服务，二者各自维护自己的状态。
	fakeRuntimeService := apitest.NewFakeRuntimeService()
	fakeImageService := apitest.NewFakeImageService()

	// 用 gRPC 服务端和两个假服务组装出完整的假远程运行时。
	f := &RemoteRuntime{
		server:         grpc.NewServer(),
		RuntimeService: fakeRuntimeService,
		ImageService:   fakeImageService,
	}
	// 把 f 同时注册为运行时服务与镜像服务的服务端实现，
	// 这样同一个 gRPC 服务端就可以处理两类 CRI 请求。
	kubeapi.RegisterRuntimeServiceServer(f.server, f)
	kubeapi.RegisterImageServiceServer(f.server, f)

	return f
}

// Start 在指定端点上启动假运行时：创建监听器、在后台提供 gRPC 服务，
// 并把假运行时的 RuntimeReady 与 NetworkReady 条件都置为就绪。
func (f *RemoteRuntime) Start(endpoint string) error {
	// 根据 endpoint（unix socket 路径或 tcp 地址）创建监听器；失败时带上端点信息返回错误。
	l, err := util.CreateListener(endpoint)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %v", endpoint, err)
	}

	// 用独立 goroutine 运行 gRPC 服务，避免阻塞 Start 的调用方。
	go f.server.Serve(l)

	// 覆盖假服务的状态：运行时和网络都报告为 ready，使 kubelet 认为该运行时可用。
	f.RuntimeService.FakeStatus = &kubeapi.RuntimeStatus{
		Conditions: []*kubeapi.RuntimeCondition{
			{Type: kubeapi.RuntimeReady, Status: true},
			{Type: kubeapi.NetworkReady, Status: true},
		},
	}

	return nil
}

// Stop 停止假运行时的 gRPC 服务端，同时关闭其监听器与连接；不会等待在途请求完成。
func (f *RemoteRuntime) Stop() {
	f.server.Stop()
}

// Version 返回运行时名称、运行时版本与运行时 API 版本；请求中的 Version 即 apiVersion。
func (f *RemoteRuntime) Version(ctx context.Context, req *kubeapi.VersionRequest) (*kubeapi.VersionResponse, error) {
	// 把请求携带的 apiVersion 直接传给假服务的 Version 方法。
	return f.RuntimeService.Version(ctx, req.Version)
}

// RunPodSandbox 创建并启动一个 PodSandbox；成功时必须确保 sandbox 已就绪，并返回假服务生成的新 sandbox ID。
func (f *RemoteRuntime) RunPodSandbox(ctx context.Context, req *kubeapi.RunPodSandboxRequest) (*kubeapi.RunPodSandboxResponse, error) {
	// 拆出请求中的 sandbox 配置与 runtime handler 两个参数传给假服务。
	sandboxID, err := f.RuntimeService.RunPodSandbox(ctx, req.Config, req.RuntimeHandler)
	if err != nil {
		return nil, err
	}

	// 将假服务分配的 ID 包装成 CRI 响应。
	return &kubeapi.RunPodSandboxResponse{PodSandboxId: sandboxID}, nil
}

// StopPodSandbox 停止 sandbox 内的进程并回收其网络资源；sandbox 内的容器会被强制终止。
func (f *RemoteRuntime) StopPodSandbox(ctx context.Context, req *kubeapi.StopPodSandboxRequest) (*kubeapi.StopPodSandboxResponse, error) {
	// 按请求中的 PodSandboxId 定位 sandbox 并调用假服务停止它。
	err := f.RuntimeService.StopPodSandbox(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}

	// 停止操作无返回数据，成功时返回空响应。
	return &kubeapi.StopPodSandboxResponse{}, nil
}

// RemovePodSandbox 删除 sandbox；其中仍在运行的容器必须被强制终止并删除。
// 该操作是幂等的，sandbox 已删除时不应报错。
func (f *RemoteRuntime) RemovePodSandbox(ctx context.Context, req *kubeapi.RemovePodSandboxRequest) (*kubeapi.RemovePodSandboxResponse, error) {
	// 按请求中的 PodSandboxId 调用假服务执行删除。
	err := f.RuntimeService.RemovePodSandbox(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}

	// 删除操作无返回数据，成功时返回空响应。
	return &kubeapi.RemovePodSandboxResponse{}, nil
}

// PodSandboxStatus 返回指定 PodSandbox 的状态；sandbox 不存在时返回错误。
// 此处固定传入 verbose=false，只查询基础状态。
func (f *RemoteRuntime) PodSandboxStatus(ctx context.Context, req *kubeapi.PodSandboxStatusRequest) (*kubeapi.PodSandboxStatusResponse, error) {
	// 第二个参数 false 表示不需要 sandbox 的详细信息。
	resp, err := f.RuntimeService.PodSandboxStatus(ctx, req.PodSandboxId, false)
	if err != nil {
		return nil, err
	}

	// 假服务已构造好完整响应，直接透传。
	return resp, nil
}

// ListPodSandbox 按请求中的 Filter 列出 PodSandbox。
func (f *RemoteRuntime) ListPodSandbox(ctx context.Context, req *kubeapi.ListPodSandboxRequest) (*kubeapi.ListPodSandboxResponse, error) {
	// 过滤条件由假服务在其维护的 sandbox 集合上应用。
	items, err := f.RuntimeService.ListPodSandbox(ctx, req.Filter)
	if err != nil {
		return nil, err
	}

	// 将过滤结果包装成 CRI 响应。
	return &kubeapi.ListPodSandboxResponse{Items: items}, nil
}

// CreateContainer 在指定 PodSandbox 中新建容器，返回假服务分配的容器 ID。
func (f *RemoteRuntime) CreateContainer(ctx context.Context, req *kubeapi.CreateContainerRequest) (*kubeapi.CreateContainerResponse, error) {
	// 依次传入所属 sandbox ID、容器配置与 sandbox 配置，让假服务记录对应关系。
	containerID, err := f.RuntimeService.CreateContainer(ctx, req.PodSandboxId, req.Config, req.SandboxConfig)
	if err != nil {
		return nil, err
	}

	// 将生成的容器 ID 包装成 CRI 响应。
	return &kubeapi.CreateContainerResponse{ContainerId: containerID}, nil
}

// StartContainer 启动指定容器，使其状态切换为运行中。
func (f *RemoteRuntime) StartContainer(ctx context.Context, req *kubeapi.StartContainerRequest) (*kubeapi.StartContainerResponse, error) {
	// 按请求中的 ContainerId 调用假服务完成状态转换。
	err := f.RuntimeService.StartContainer(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}

	// 启动操作无返回数据，成功时返回空响应。
	return &kubeapi.StartContainerResponse{}, nil
}

// StopContainer 在给定的宽限期（超时时间）内停止运行中的容器；该操作是幂等的，容器已停止时不应报错。
func (f *RemoteRuntime) StopContainer(ctx context.Context, req *kubeapi.StopContainerRequest) (*kubeapi.StopContainerResponse, error) {
	// 把容器 ID 和超时时间一并透传给假服务。
	err := f.RuntimeService.StopContainer(ctx, req.ContainerId, req.Timeout)
	if err != nil {
		return nil, err
	}

	// 停止操作无返回数据，成功时返回空响应。
	return &kubeapi.StopContainerResponse{}, nil
}

// RemoveContainer 删除容器，运行中的容器会被强制删除；该操作是幂等的，容器已删除时不应报错。
func (f *RemoteRuntime) RemoveContainer(ctx context.Context, req *kubeapi.RemoveContainerRequest) (*kubeapi.RemoveContainerResponse, error) {
	// 按请求中的 ContainerId 调用假服务执行删除。
	err := f.RuntimeService.RemoveContainer(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}

	// 删除操作无返回数据，成功时返回空响应。
	return &kubeapi.RemoveContainerResponse{}, nil
}

// ListContainers 按请求中的 Filter 列出容器。
func (f *RemoteRuntime) ListContainers(ctx context.Context, req *kubeapi.ListContainersRequest) (*kubeapi.ListContainersResponse, error) {
	// 过滤逻辑由假服务在其维护的容器集合上完成。
	items, err := f.RuntimeService.ListContainers(ctx, req.Filter)
	if err != nil {
		return nil, err
	}

	// 将过滤结果包装成 CRI 响应。
	return &kubeapi.ListContainersResponse{Containers: items}, nil
}

// ContainerStatus 返回指定容器的状态；容器不存在时返回错误。
// 此处固定传入 verbose=false，只查询基础状态。
func (f *RemoteRuntime) ContainerStatus(ctx context.Context, req *kubeapi.ContainerStatusRequest) (*kubeapi.ContainerStatusResponse, error) {
	// 第二个参数 false 表示不要求返回容器的详细信息。
	resp, err := f.RuntimeService.ContainerStatus(ctx, req.ContainerId, false)
	if err != nil {
		return nil, err
	}

	// 假服务已构造好完整响应，直接透传。
	return resp, nil
}

// ExecSync 在容器内同步执行命令，返回标准输出、标准错误与退出码。
// 请求中的超时以秒为单位，这里转换为 time.Duration。
func (f *RemoteRuntime) ExecSync(ctx context.Context, req *kubeapi.ExecSyncRequest) (*kubeapi.ExecSyncResponse, error) {
	// 退出码默认 0，只有命令以非零状态退出时才会被改写。
	var exitCode int32
	// 把以秒为单位的超时换算成 Duration 后交给假服务执行命令。
	stdout, stderr, err := f.RuntimeService.ExecSync(ctx, req.ContainerId, req.Cmd, time.Duration(req.Timeout)*time.Second)
	if err != nil {
		// 尝试把错误断言为 utilexec.ExitError，以便提取进程退出码。
		exitError, ok := err.(utilexec.ExitError)
		if !ok {
			// 非退出错误（如注入的普通错误）直接向上传播。
			return nil, err
		}
		// 命令非零退出属于预期结果：记录退出码，仍把 stdout/stderr 返回给调用方。
		exitCode = int32(exitError.ExitStatus())
	}

	// 用捕获的输出和退出码构造 CRI 响应。
	return &kubeapi.ExecSyncResponse{
		Stdout:   stdout,
		Stderr:   stderr,
		ExitCode: exitCode,
	}, nil
}

// Exec 为容器内命令执行准备流式端点，请求与响应都直接交给假服务处理。
func (f *RemoteRuntime) Exec(ctx context.Context, req *kubeapi.ExecRequest) (*kubeapi.ExecResponse, error) {
	return f.RuntimeService.Exec(ctx, req)
}

// Attach 为附着到运行中容器准备流式端点，直接委托给假服务。
func (f *RemoteRuntime) Attach(ctx context.Context, req *kubeapi.AttachRequest) (*kubeapi.AttachResponse, error) {
	return f.RuntimeService.Attach(ctx, req)
}

// PortForward 为 PodSandbox 的端口转发准备流式端点，直接委托给假服务。
func (f *RemoteRuntime) PortForward(ctx context.Context, req *kubeapi.PortForwardRequest) (*kubeapi.PortForwardResponse, error) {
	return f.RuntimeService.PortForward(ctx, req)
}

// ContainerStats 返回指定容器的统计信息；容器不存在时返回错误。
func (f *RemoteRuntime) ContainerStats(ctx context.Context, req *kubeapi.ContainerStatsRequest) (*kubeapi.ContainerStatsResponse, error) {
	// 按容器 ID 从假服务取出统计信息。
	stats, err := f.RuntimeService.ContainerStats(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}

	// 将统计信息包装成 CRI 响应。
	return &kubeapi.ContainerStatsResponse{Stats: stats}, nil
}

// ListContainerStats 按 Filter 返回所有运行中容器的统计信息。
func (f *RemoteRuntime) ListContainerStats(ctx context.Context, req *kubeapi.ListContainerStatsRequest) (*kubeapi.ListContainerStatsResponse, error) {
	// 过滤在假服务中完成。
	stats, err := f.RuntimeService.ListContainerStats(ctx, req.Filter)
	if err != nil {
		return nil, err
	}

	return &kubeapi.ListContainerStatsResponse{Stats: stats}, nil
}

// PodSandboxStats 返回指定 Pod 的统计信息；Pod 不存在时返回错误。
func (f *RemoteRuntime) PodSandboxStats(ctx context.Context, req *kubeapi.PodSandboxStatsRequest) (*kubeapi.PodSandboxStatsResponse, error) {
	// 按 PodSandboxId 从假服务取出统计信息。
	stats, err := f.RuntimeService.PodSandboxStats(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}

	return &kubeapi.PodSandboxStatsResponse{Stats: stats}, nil
}

// ListPodSandboxStats 按 Filter 返回所有运行中 Pod 的统计信息。
func (f *RemoteRuntime) ListPodSandboxStats(ctx context.Context, req *kubeapi.ListPodSandboxStatsRequest) (*kubeapi.ListPodSandboxStatsResponse, error) {
	// 过滤在假服务中完成。
	stats, err := f.RuntimeService.ListPodSandboxStats(ctx, req.Filter)
	if err != nil {
		return nil, err
	}

	return &kubeapi.ListPodSandboxStatsResponse{Stats: stats}, nil
}

// UpdateRuntimeConfig 根据请求更新运行时配置。
func (f *RemoteRuntime) UpdateRuntimeConfig(ctx context.Context, req *kubeapi.UpdateRuntimeConfigRequest) (*kubeapi.UpdateRuntimeConfigResponse, error) {
	// 只把请求里的 RuntimeConfig 子结构传给假服务。
	err := f.RuntimeService.UpdateRuntimeConfig(ctx, req.RuntimeConfig)
	if err != nil {
		return nil, err
	}

	// 更新操作无返回数据，成功时返回空响应。
	return &kubeapi.UpdateRuntimeConfigResponse{}, nil
}

// Status 返回运行时状态；此处固定传入 verbose=false。
func (f *RemoteRuntime) Status(ctx context.Context, req *kubeapi.StatusRequest) (*kubeapi.StatusResponse, error) {
	// 只查询基础状态，不请求详细信息。
	resp, err := f.RuntimeService.Status(ctx, false)
	if err != nil {
		return nil, err
	}

	// 假服务已构造好完整响应，直接透传。
	return resp, nil
}

// UpdateContainerResources 更新容器的资源配置。
func (f *RemoteRuntime) UpdateContainerResources(ctx context.Context, req *kubeapi.UpdateContainerResourcesRequest) (*kubeapi.UpdateContainerResourcesResponse, error) {
	// 把请求中的 Linux 资源字段重新包装为 ContainerResources 后传给假服务。
	err := f.RuntimeService.UpdateContainerResources(ctx, req.ContainerId, &kubeapi.ContainerResources{Linux: req.Linux})
	if err != nil {
		return nil, err
	}

	return &kubeapi.UpdateContainerResourcesResponse{}, nil
}

// ReopenContainerLog 重新打开指定容器的日志文件。
func (f *RemoteRuntime) ReopenContainerLog(ctx context.Context, req *kubeapi.ReopenContainerLogRequest) (*kubeapi.ReopenContainerLogResponse, error) {
	// 按容器 ID 调用假服务执行重新打开日志的操作。
	err := f.RuntimeService.ReopenContainerLog(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}

	return &kubeapi.ReopenContainerLogResponse{}, nil
}

// CheckpointContainer 为指定容器创建检查点。
// 注意：这里没有透传 req，而是向假服务传入了一个新建的空 CheckpointContainerRequest。
func (f *RemoteRuntime) CheckpointContainer(ctx context.Context, req *kubeapi.CheckpointContainerRequest) (*kubeapi.CheckpointContainerResponse, error) {
	err := f.RuntimeService.CheckpointContainer(ctx, &kubeapi.CheckpointContainerRequest{})
	if err != nil {
		return nil, err
	}

	return &kubeapi.CheckpointContainerResponse{}, nil
}

// CheckpointPod 为指定 PodSandbox 创建检查点，请求被原样交给假服务记录。
func (f *RemoteRuntime) CheckpointPod(ctx context.Context, req *kubeapi.CheckpointPodRequest) (*kubeapi.CheckpointPodResponse, error) {
	err := f.RuntimeService.CheckpointPod(ctx, req)
	if err != nil {
		return nil, err
	}

	// 检查点操作本身无返回数据，成功时返回空响应。
	return &kubeapi.CheckpointPodResponse{}, nil
}

// RestorePod 从检查点恢复 PodSandbox，直接把假服务的响应返回给调用方。
func (f *RemoteRuntime) RestorePod(ctx context.Context, req *kubeapi.RestorePodRequest) (*kubeapi.RestorePodResponse, error) {
	response, err := f.RuntimeService.RestorePod(ctx, req)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// GetContainerEvents 实现容器事件流接口；当前为空实现，直接返回 nil，不推送任何事件。
func (f *RemoteRuntime) GetContainerEvents(req *kubeapi.GetEventsRequest, ces kubeapi.RuntimeService_GetContainerEventsServer) error {
	return nil
}

// ListMetricDescriptors 返回指标描述符列表，供调用方理解 ListPodSandboxMetrics 返回的指标含义。
func (f *RemoteRuntime) ListMetricDescriptors(ctx context.Context, req *kubeapi.ListMetricDescriptorsRequest) (*kubeapi.ListMetricDescriptorsResponse, error) {
	// 假服务不依赖请求参数，直接返回预置的描述符集合。
	descs, err := f.RuntimeService.ListMetricDescriptors(ctx)
	if err != nil {
		return nil, err
	}

	return &kubeapi.ListMetricDescriptorsResponse{Descriptors: descs}, nil
}

// ListPodSandboxMetrics 返回所有 PodSandbox 的指标数据。
func (f *RemoteRuntime) ListPodSandboxMetrics(ctx context.Context, req *kubeapi.ListPodSandboxMetricsRequest) (*kubeapi.ListPodSandboxMetricsResponse, error) {
	// 假服务返回其维护的全部 PodSandbox 指标，不做额外过滤。
	podMetrics, err := f.RuntimeService.ListPodSandboxMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return &kubeapi.ListPodSandboxMetricsResponse{PodMetrics: podMetrics}, nil
}

// RuntimeConfig 返回运行时的配置信息，数据来自假服务的预设配置。
func (f *RemoteRuntime) RuntimeConfig(ctx context.Context, req *kubeapi.RuntimeConfigRequest) (*kubeapi.RuntimeConfigResponse, error) {
	resp, err := f.RuntimeService.RuntimeConfig(ctx)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// UpdatePodSandboxResources 同步更新 PodSandbox 的资源配置，请求与响应均直接透传给假服务。
func (f *RemoteRuntime) UpdatePodSandboxResources(ctx context.Context, req *kubeapi.UpdatePodSandboxResourcesRequest) (*kubeapi.UpdatePodSandboxResourcesResponse, error) {
	return f.RuntimeService.UpdatePodSandboxResources(ctx, req)
}

// StreamPodSandboxes 以流的形式返回 PodSandbox 列表；此处一次性查询并发送单个响应。
func (f *RemoteRuntime) StreamPodSandboxes(req *kubeapi.StreamPodSandboxesRequest, stream kubeapi.RuntimeService_StreamPodSandboxesServer) error {
	// 使用流自身的 Context 作为调用上下文，并应用请求中的过滤条件。
	items, err := f.RuntimeService.ListPodSandbox(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	// 把查询结果作为一条消息写入流。
	return stream.Send(&kubeapi.StreamPodSandboxesResponse{PodSandboxes: items})
}

// StreamContainers 以流的形式返回容器列表，按请求 Filter 过滤后发送单个响应。
func (f *RemoteRuntime) StreamContainers(req *kubeapi.StreamContainersRequest, stream kubeapi.RuntimeService_StreamContainersServer) error {
	// 上下文取自流对象，保证客户端取消能传递到查询过程。
	items, err := f.RuntimeService.ListContainers(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	return stream.Send(&kubeapi.StreamContainersResponse{Containers: items})
}

// StreamContainerStats 以流的形式返回容器统计信息，按请求 Filter 过滤后发送单个响应。
func (f *RemoteRuntime) StreamContainerStats(req *kubeapi.StreamContainerStatsRequest, stream kubeapi.RuntimeService_StreamContainerStatsServer) error {
	// 上下文取自流对象，过滤条件由假服务处理。
	stats, err := f.RuntimeService.ListContainerStats(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	return stream.Send(&kubeapi.StreamContainerStatsResponse{ContainerStats: stats})
}

// StreamPodSandboxStats 以流的形式返回 PodSandbox 统计信息，按请求 Filter 过滤后发送单个响应。
func (f *RemoteRuntime) StreamPodSandboxStats(req *kubeapi.StreamPodSandboxStatsRequest, stream kubeapi.RuntimeService_StreamPodSandboxStatsServer) error {
	// 上下文取自流对象，过滤条件由假服务处理。
	stats, err := f.RuntimeService.ListPodSandboxStats(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	return stream.Send(&kubeapi.StreamPodSandboxStatsResponse{PodSandboxStats: stats})
}

// StreamPodSandboxMetrics 以流的形式返回 PodSandbox 指标，发送单个响应。
func (f *RemoteRuntime) StreamPodSandboxMetrics(req *kubeapi.StreamPodSandboxMetricsRequest, stream kubeapi.RuntimeService_StreamPodSandboxMetricsServer) error {
	// 假服务的指标查询不接受过滤条件，直接使用流的上下文。
	podMetrics, err := f.RuntimeService.ListPodSandboxMetrics(stream.Context())
	if err != nil {
		return err
	}
	return stream.Send(&kubeapi.StreamPodSandboxMetricsResponse{PodSandboxMetrics: podMetrics})
}

// StreamImages 以流的形式返回镜像列表，按请求 Filter 过滤后发送单个响应。
// 注意：这里走的是假镜像服务，而不是假运行时服务。
func (f *RemoteRuntime) StreamImages(req *kubeapi.StreamImagesRequest, stream kubeapi.ImageService_StreamImagesServer) error {
	// 从流对象取上下文，委托假镜像服务按过滤条件列出镜像。
	images, err := f.ImageService.ListImages(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	return stream.Send(&kubeapi.StreamImagesResponse{Images: images})
}

// Close 关闭假运行时持有的内部 gRPC 客户端连接，实际动作由假运行时服务完成。
func (f *RemoteRuntime) Close(ctx context.Context) error {
	return f.RuntimeService.Close(ctx)
}
