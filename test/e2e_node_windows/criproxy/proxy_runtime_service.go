/*
Copyright The Kubernetes Authors.

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

package criproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-client/pkg/util"
	"k8s.io/kubernetes/test/e2e/framework"
	utilexec "k8s.io/utils/exec"
)

const (
	Version                   = "Version"
	RunPodSandbox             = "RunPodSandbox"
	StopPodSandbox            = "StopPodSandbox"
	RemovePodSandbox          = "RemovePodSandbox"
	PodSandboxStatus          = "PodSandboxStatus"
	ListPodSandbox            = "ListPodSandbox"
	CreateContainer           = "CreateContainer"
	StartContainer            = "StartContainer"
	StopContainer             = "StopContainer"
	RemoveContainer           = "RemoveContainer"
	ListContainers            = "ListContainers"
	ContainerStatus           = "ContainerStatus"
	UpdateContainerResources  = "UpdateContainerResources"
	ReopenContainerLog        = "ReopenContainerLog"
	ExecSync                  = "ExecSync"
	Exec                      = "Exec"
	Attach                    = "Attach"
	PortForward               = "PortForward"
	ContainerStats            = "ContainerStats"
	ListContainerStats        = "ListContainerStats"
	PodSandboxStats           = "PodSandboxStats"
	ListPodSandboxStats       = "ListPodSandboxStats"
	UpdateRuntimeConfig       = "UpdateRuntimeConfig"
	Status                    = "Status"
	CheckpointContainer       = "CheckpointContainer"
	CheckpointPod             = "CheckpointPod"
	RestorePod                = "RestorePod"
	GetContainerEvents        = "GetContainerEvents"
	ListMetricDescriptors     = "ListMetricDescriptors"
	ListPodSandboxMetrics     = "ListPodSandboxMetrics"
	RuntimeConfig             = "RuntimeConfig"
	UpdatePodSandboxResources = "UpdatePodSandboxResources"
	// 流式 API
	StreamPodSandboxes      = "StreamPodSandboxes"
	StreamContainers        = "StreamContainers"
	StreamContainerStats    = "StreamContainerStats"
	StreamPodSandboxStats   = "StreamPodSandboxStats"
	StreamPodSandboxMetrics = "StreamPodSandboxMetrics"
	// 流式 API 的每次发送注入点，在每次 stream.Send() 前调用。
	StreamPodSandboxesSend = "StreamPodSandboxesSend"
	StreamContainersSend   = "StreamContainersSend"
)

// AddInjector 向下一次 RuntimeService 调用注入错误或延迟。
func (p *RemoteRuntime) AddInjector(injector func(string) error) {
	p.injectors = append(p.injectors, injector)
}

// ResetInjectors 重置所有已注册的 injector。
func (p *RemoteRuntime) ResetInjectors() {
	p.injectors = []func(string) error{}
}

func (p *RemoteRuntime) runInjectors(apiName string) error {
	for _, injector := range p.injectors {
		if err := injector(apiName); err != nil {
			return err
		}
	}
	return nil
}

// RemoteRuntime 表示远端容器运行时的代理。
type RemoteRuntime struct {
	server         *grpc.Server
	injectors      []func(string) error
	runtimeService internalapi.RuntimeService
	imageService   internalapi.ImageManagerService
	runtimeapi.UnsafeImageServiceServer
	runtimeapi.UnsafeRuntimeServiceServer
}

// NewRemoteRuntimeProxy 创建一个新的 RemoteRuntime。
func NewRemoteRuntimeProxy(runtimeService internalapi.RuntimeService, imageService internalapi.ImageManagerService) *RemoteRuntime {
	p := &RemoteRuntime{
		server:         grpc.NewServer(),
		runtimeService: runtimeService,
		imageService:   imageService,
	}
	runtimeapi.RegisterRuntimeServiceServer(p.server, p)
	runtimeapi.RegisterImageServiceServer(p.server, p)

	return p
}

// Start 启动远端运行时代理。
func (p *RemoteRuntime) Start(endpoint string) error {
	l, err := util.CreateListener(endpoint)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %w", endpoint, err)
	}

	go func() {
		if err := p.server.Serve(l); err != nil {
			framework.Failf("Failed to start cri proxy : %v", err)
		}
	}()
	return nil
}

// Stop 停止假的远端运行时代理。
func (p *RemoteRuntime) Stop() {
	p.server.Stop()
}

// Version 返回运行时名称、运行时版本和运行时 API 版本。
func (p *RemoteRuntime) Version(ctx context.Context, req *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	if err := p.runInjectors(Version); err != nil {
		return nil, err
	}
	return p.runtimeService.Version(ctx, req.Version)
}

// RunPodSandbox 创建并启动 pod 级别的 sandbox。runtime 必须确保
// 成功时 sandbox 处于 ready 状态。
func (p *RemoteRuntime) RunPodSandbox(ctx context.Context, req *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	if err := p.runInjectors(RunPodSandbox); err != nil {
		return nil, err
	}

	sandboxID, err := p.runtimeService.RunPodSandbox(ctx, req.Config, req.RuntimeHandler)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: sandboxID}, nil
}

// StopPodSandbox 停止 sandbox 中所有正在运行的进程，并
// 回收分配给 sandbox 的网络资源（如 IP 地址）。
// 如果 sandbox 中还有运行中的容器，必须强制
// 终止它们。
func (p *RemoteRuntime) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	if err := p.runInjectors(StopPodSandbox); err != nil {
		return nil, err
	}

	err := p.runtimeService.StopPodSandbox(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.StopPodSandboxResponse{}, nil
}

// RemovePodSandbox 移除 sandbox。如果其中有正在运行的容器，
// 必须强制终止并移除它们。
// 此调用是幂等的，sandbox 已被移除时
// 不得返回错误。
func (p *RemoteRuntime) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	if err := p.runInjectors(RemovePodSandbox); err != nil {
		return nil, err
	}
	err := p.runtimeService.RemovePodSandbox(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}

	return &runtimeapi.RemovePodSandboxResponse{}, nil
}

// PodSandboxStatus 返回 PodSandbox 的状态。如果 PodSandbox
// 不存在，返回错误。
func (p *RemoteRuntime) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	if err := p.runInjectors(PodSandboxStatus); err != nil {
		return nil, err
	}

	resp, err := p.runtimeService.PodSandboxStatus(ctx, req.PodSandboxId, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListPodSandbox 返回 PodSandbox 列表。
func (p *RemoteRuntime) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	if err := p.runInjectors(ListPodSandbox); err != nil {
		return nil, err
	}

	items, err := p.runtimeService.ListPodSandbox(ctx, req.Filter)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxResponse{Items: items}, nil
}

// CreateContainer 在指定 PodSandbox 中创建新容器
func (p *RemoteRuntime) CreateContainer(ctx context.Context, req *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	if err := p.runInjectors(CreateContainer); err != nil {
		return nil, err
	}

	containerID, err := p.runtimeService.CreateContainer(ctx, req.PodSandboxId, req.Config, req.SandboxConfig)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.CreateContainerResponse{ContainerId: containerID}, nil
}

// StartContainer 启动容器。
func (p *RemoteRuntime) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	if err := p.runInjectors(StartContainer); err != nil {
		return nil, err
	}

	err := p.runtimeService.StartContainer(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.StartContainerResponse{}, nil
}

// StopContainer 在 grace period（即超时）内停止运行中的容器。
// 此调用是幂等的，容器已被停止时
// 不得返回错误。
func (p *RemoteRuntime) StopContainer(ctx context.Context, req *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	if err := p.runInjectors(StopContainer); err != nil {
		return nil, err
	}

	err := p.runtimeService.StopContainer(ctx, req.ContainerId, req.Timeout)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

// RemoveContainer 移除容器。如果容器正在运行，
// 必须强制移除。
// 此调用是幂等的，容器已被移除时
// 不得返回错误。
func (p *RemoteRuntime) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	if err := p.runInjectors(RemoveContainer); err != nil {
		return nil, err
	}

	err := p.runtimeService.RemoveContainer(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.RemoveContainerResponse{}, nil
}

// ListContainers 按 filter 列出所有容器。
func (p *RemoteRuntime) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	if err := p.runInjectors(ListContainers); err != nil {
		return nil, err
	}

	items, err := p.runtimeService.ListContainers(ctx, req.Filter)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListContainersResponse{Containers: items}, nil
}

// ContainerStatus 返回容器状态。如果容器
// 不存在，返回错误。
func (p *RemoteRuntime) ContainerStatus(ctx context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	if err := p.runInjectors(ContainerStatus); err != nil {
		return nil, err
	}

	resp, err := p.runtimeService.ContainerStatus(ctx, req.ContainerId, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ExecSync 在容器中同步执行命令。
func (p *RemoteRuntime) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	if err := p.runInjectors(ExecSync); err != nil {
		return nil, err
	}

	var exitCode int32
	stdout, stderr, err := p.runtimeService.ExecSync(ctx, req.ContainerId, req.Cmd, time.Duration(req.Timeout)*time.Second)
	if err != nil {
		var exitError utilexec.ExitError
		ok := errors.As(err, &exitError)
		if !ok {
			return nil, err
		}
		exitCode = int32(exitError.ExitStatus())
	}
	return &runtimeapi.ExecSyncResponse{
		Stdout:   stdout,
		Stderr:   stderr,
		ExitCode: exitCode,
	}, nil
}

// Exec 准备一个流式端点，在容器中执行命令。
func (p *RemoteRuntime) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	if err := p.runInjectors(Exec); err != nil {
		return nil, err
	}

	return p.runtimeService.Exec(ctx, req)
}

// Attach 准备一个流式端点，附加到运行中的容器。
func (p *RemoteRuntime) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	if err := p.runInjectors(Attach); err != nil {
		return nil, err
	}

	return p.runtimeService.Attach(ctx, req)
}

// PortForward 准备一个流式端点，从 PodSandbox 转发端口。
func (p *RemoteRuntime) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	if err := p.runInjectors(PortForward); err != nil {
		return nil, err
	}

	return p.runtimeService.PortForward(ctx, req)
}

// ContainerStats 返回容器统计信息。如果容器
// 不存在，调用返回错误。
func (p *RemoteRuntime) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	if err := p.runInjectors(ContainerStats); err != nil {
		return nil, err
	}

	stats, err := p.runtimeService.ContainerStats(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ContainerStatsResponse{Stats: stats}, nil
}

// ListContainerStats 返回所有运行中容器的统计信息。
func (p *RemoteRuntime) ListContainerStats(ctx context.Context, req *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	if err := p.runInjectors(ListContainerStats); err != nil {
		return nil, err
	}

	stats, err := p.runtimeService.ListContainerStats(ctx, req.Filter)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListContainerStatsResponse{Stats: stats}, nil
}

// PodSandboxStats 返回 pod 的统计信息。如果 pod
// 不存在，调用返回错误。
func (p *RemoteRuntime) PodSandboxStats(ctx context.Context, req *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	if err := p.runInjectors(PodSandboxStats); err != nil {
		return nil, err
	}

	stats, err := p.runtimeService.PodSandboxStats(ctx, req.PodSandboxId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.PodSandboxStatsResponse{Stats: stats}, nil
}

// ListPodSandboxStats 返回所有运行中 pod 的统计信息。
func (p *RemoteRuntime) ListPodSandboxStats(ctx context.Context, req *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	if err := p.runInjectors(ListPodSandboxStats); err != nil {
		return nil, err
	}

	stats, err := p.runtimeService.ListPodSandboxStats(ctx, req.Filter)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxStatsResponse{Stats: stats}, nil
}

// UpdateRuntimeConfig 根据给定请求更新运行时配置。
func (p *RemoteRuntime) UpdateRuntimeConfig(ctx context.Context, req *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	if err := p.runInjectors(UpdateRuntimeConfig); err != nil {
		return nil, err
	}

	err := p.runtimeService.UpdateRuntimeConfig(ctx, req.RuntimeConfig)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
}

// UpdatePodSandboxResources 同步更新 PodSandboxConfig。
func (p *RemoteRuntime) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	if err := p.runInjectors(UpdatePodSandboxResources); err != nil {
		return nil, err
	}

	return p.runtimeService.UpdatePodSandboxResources(ctx, req)
}

// Status 返回运行时状态。
func (p *RemoteRuntime) Status(ctx context.Context, req *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	if err := p.runInjectors(Status); err != nil {
		return nil, err
	}

	resp, err := p.runtimeService.Status(ctx, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateContainerResources 更新容器的 ContainerConfig。
func (p *RemoteRuntime) UpdateContainerResources(ctx context.Context, req *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	if err := p.runInjectors(UpdateContainerResources); err != nil {
		return nil, err
	}

	err := p.runtimeService.UpdateContainerResources(ctx, req.ContainerId, &runtimeapi.ContainerResources{Linux: req.Linux})
	if err != nil {
		return nil, err
	}
	return &runtimeapi.UpdateContainerResourcesResponse{}, nil
}

// ReopenContainerLog 重新打开容器日志文件。
func (p *RemoteRuntime) ReopenContainerLog(ctx context.Context, req *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	if err := p.runInjectors(ReopenContainerLog); err != nil {
		return nil, err
	}

	err := p.runtimeService.ReopenContainerLog(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ReopenContainerLogResponse{}, nil
}

// CheckpointContainer 为给定容器创建 checkpoint。
func (p *RemoteRuntime) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	if err := p.runInjectors(CheckpointContainer); err != nil {
		return nil, err
	}

	err := p.runtimeService.CheckpointContainer(ctx, req)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.CheckpointContainerResponse{}, nil
}

// CheckpointPod 为给定 pod sandbox 创建 checkpoint。
func (p *RemoteRuntime) CheckpointPod(ctx context.Context, req *runtimeapi.CheckpointPodRequest) (*runtimeapi.CheckpointPodResponse, error) {
	if err := p.runInjectors(CheckpointPod); err != nil {
		return nil, err
	}

	err := p.runtimeService.CheckpointPod(ctx, req)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.CheckpointPodResponse{}, nil
}

// RestorePod 从 checkpoint 恢复 pod sandbox。
func (p *RemoteRuntime) RestorePod(ctx context.Context, req *runtimeapi.RestorePodRequest) (*runtimeapi.RestorePodResponse, error) {
	if err := p.runInjectors(RestorePod); err != nil {
		return nil, err
	}

	response, err := p.runtimeService.RestorePod(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (p *RemoteRuntime) GetContainerEvents(req *runtimeapi.GetEventsRequest, ces runtimeapi.RuntimeService_GetContainerEventsServer) error {
	if err := p.runInjectors(GetContainerEvents); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ces.Context())
	defer cancel()

	// 接收 pod 生命周期事件的 channel 容量。这个数字
	// 有点随意，将来可能调整。
	plegChannelCapacity := 1000
	containerEventsResponseCh := make(chan *runtimeapi.ContainerEventResponse, plegChannelCapacity)
	errCh := make(chan error, 1)
	go func() {
		defer close(containerEventsResponseCh)
		errCh <- p.runtimeService.GetContainerEvents(ctx, containerEventsResponseCh, nil)
	}()

	for event := range containerEventsResponseCh {
		if err := ces.Send(event); err != nil {
			cancel()
			return status.Errorf(codes.Unknown, "Failed to send event: %v", err)
		}
	}

	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

// ListMetricDescriptors 获取将在 ListPodSandboxMetrics 中返回的指标描述符。
func (p *RemoteRuntime) ListMetricDescriptors(ctx context.Context, req *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	if err := p.runInjectors(ListMetricDescriptors); err != nil {
		return nil, err
	}

	descs, err := p.runtimeService.ListMetricDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListMetricDescriptorsResponse{Descriptors: descs}, nil
}

// ListPodSandboxMetrics 获取所有 pod sandbox 的指标。
func (p *RemoteRuntime) ListPodSandboxMetrics(ctx context.Context, req *runtimeapi.ListPodSandboxMetricsRequest) (*runtimeapi.ListPodSandboxMetricsResponse, error) {
	if err := p.runInjectors(ListPodSandboxMetrics); err != nil {
		return nil, err
	}

	podMetrics, err := p.runtimeService.ListPodSandboxMetrics(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxMetricsResponse{PodMetrics: podMetrics}, nil
}

// RuntimeConfig 返回运行时的配置信息。
func (p *RemoteRuntime) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	if err := p.runInjectors(RuntimeConfig); err != nil {
		return nil, err
	}

	resp, err := p.runtimeService.RuntimeConfig(ctx)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// StreamPodSandboxes 返回 PodSandbox 流。
func (p *RemoteRuntime) StreamPodSandboxes(req *runtimeapi.StreamPodSandboxesRequest, stream runtimeapi.RuntimeService_StreamPodSandboxesServer) error {
	if err := p.runInjectors(StreamPodSandboxes); err != nil {
		return err
	}

	items, err := p.runtimeService.ListPodSandbox(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := p.runInjectors(StreamPodSandboxesSend); err != nil {
			return err
		}
		if err := stream.Send(&runtimeapi.StreamPodSandboxesResponse{PodSandboxes: []*runtimeapi.PodSandbox{item}}); err != nil {
			return err
		}
	}
	return nil
}

// StreamContainers 返回容器流。
func (p *RemoteRuntime) StreamContainers(req *runtimeapi.StreamContainersRequest, stream runtimeapi.RuntimeService_StreamContainersServer) error {
	if err := p.runInjectors(StreamContainers); err != nil {
		return err
	}

	items, err := p.runtimeService.ListContainers(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := p.runInjectors(StreamContainersSend); err != nil {
			return err
		}
		if err := stream.Send(&runtimeapi.StreamContainersResponse{Containers: []*runtimeapi.Container{item}}); err != nil {
			return err
		}
	}
	return nil
}

// StreamContainerStats 返回容器统计信息流。
func (p *RemoteRuntime) StreamContainerStats(req *runtimeapi.StreamContainerStatsRequest, stream runtimeapi.RuntimeService_StreamContainerStatsServer) error {
	if err := p.runInjectors(StreamContainerStats); err != nil {
		return err
	}

	stats, err := p.runtimeService.ListContainerStats(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	for _, stat := range stats {
		if err := stream.Send(&runtimeapi.StreamContainerStatsResponse{ContainerStats: []*runtimeapi.ContainerStats{stat}}); err != nil {
			return err
		}
	}
	return nil
}

// StreamPodSandboxStats 返回 pod sandbox 统计信息流。
func (p *RemoteRuntime) StreamPodSandboxStats(req *runtimeapi.StreamPodSandboxStatsRequest, stream runtimeapi.RuntimeService_StreamPodSandboxStatsServer) error {
	if err := p.runInjectors(StreamPodSandboxStats); err != nil {
		return err
	}

	stats, err := p.runtimeService.ListPodSandboxStats(stream.Context(), req.Filter)
	if err != nil {
		return err
	}
	for _, stat := range stats {
		if err := stream.Send(&runtimeapi.StreamPodSandboxStatsResponse{PodSandboxStats: []*runtimeapi.PodSandboxStats{stat}}); err != nil {
			return err
		}
	}
	return nil
}

// StreamPodSandboxMetrics 返回 pod sandbox 指标流。
func (p *RemoteRuntime) StreamPodSandboxMetrics(req *runtimeapi.StreamPodSandboxMetricsRequest, stream runtimeapi.RuntimeService_StreamPodSandboxMetricsServer) error {
	if err := p.runInjectors(StreamPodSandboxMetrics); err != nil {
		return err
	}

	podMetrics, err := p.runtimeService.ListPodSandboxMetrics(stream.Context())
	if err != nil {
		return err
	}
	for _, metric := range podMetrics {
		if err := stream.Send(&runtimeapi.StreamPodSandboxMetricsResponse{PodSandboxMetrics: []*runtimeapi.PodSandboxMetrics{metric}}); err != nil {
			return err
		}
	}
	return nil
}
