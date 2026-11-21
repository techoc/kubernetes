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

package kuberuntime

import (
	"context"
	"time"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// instrumentedRuntimeService 包装 RuntimeService，并记录操作次数、耗时与错误指标。
type instrumentedRuntimeService struct {
	service internalapi.RuntimeService
}

// newInstrumentedRuntimeService 基于已有的 RuntimeService 创建带指标埋点的实现。
func newInstrumentedRuntimeService(service internalapi.RuntimeService) internalapi.RuntimeService {
	return &instrumentedRuntimeService{service: service}
}

// instrumentedImageManagerService 包装 ImageManagerService，并记录操作次数、耗时与错误指标。
type instrumentedImageManagerService struct {
	service internalapi.ImageManagerService
}

// newInstrumentedImageManagerService 基于已有的 ImageManagerService 创建带指标埋点的实现。
func newInstrumentedImageManagerService(service internalapi.ImageManagerService) internalapi.ImageManagerService {
	return &instrumentedImageManagerService{service: service}
}

// recordOperation 记录一次操作的调用次数与耗时。
func recordOperation(operation string, start time.Time) {
	metrics.RuntimeOperations.WithLabelValues(operation).Inc()
	metrics.RuntimeOperationsDuration.WithLabelValues(operation).Observe(metrics.SinceInSeconds(start))
}

// recordError 在操作出错时累加对应的错误计数指标。
func recordError(operation string, err error) {
	if err != nil {
		metrics.RuntimeOperationsErrors.WithLabelValues(operation).Inc()
	}
}

// Version 调用运行时服务的 Version 接口获取运行时版本信息，并记录指标。
func (in instrumentedRuntimeService) Version(ctx context.Context, apiVersion string) (*runtimeapi.VersionResponse, error) {
	const operation = "version"
	defer recordOperation(operation, time.Now())

	out, err := in.service.Version(ctx, apiVersion)
	recordError(operation, err)
	return out, err
}

// Status 调用运行时服务的 Status 接口获取运行时状态，并记录指标。
func (in instrumentedRuntimeService) Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	const operation = "status"
	defer recordOperation(operation, time.Now())

	out, err := in.service.Status(ctx, verbose)
	recordError(operation, err)
	return out, err
}

// CreateContainer 调用运行时服务创建容器，并记录指标。
func (in instrumentedRuntimeService) CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	const operation = "create_container"
	defer recordOperation(operation, time.Now())

	out, err := in.service.CreateContainer(ctx, podSandboxID, config, sandboxConfig)
	recordError(operation, err)
	return out, err
}

// StartContainer 调用运行时服务启动容器，并记录指标。
func (in instrumentedRuntimeService) StartContainer(ctx context.Context, containerID string) error {
	const operation = "start_container"
	defer recordOperation(operation, time.Now())

	err := in.service.StartContainer(ctx, containerID)
	recordError(operation, err)
	return err
}

// StopContainer 调用运行时服务停止容器，并记录指标。
func (in instrumentedRuntimeService) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	const operation = "stop_container"
	defer recordOperation(operation, time.Now())

	err := in.service.StopContainer(ctx, containerID, timeout)
	recordError(operation, err)
	return err
}

// RemoveContainer 调用运行时服务删除容器，并记录指标。
func (in instrumentedRuntimeService) RemoveContainer(ctx context.Context, containerID string) error {
	const operation = "remove_container"
	defer recordOperation(operation, time.Now())

	err := in.service.RemoveContainer(ctx, containerID)
	recordError(operation, err)
	return err
}

// ListContainers 调用运行时服务列出容器，并记录指标。
func (in instrumentedRuntimeService) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	const operation = "list_containers"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListContainers(ctx, filter)
	recordError(operation, err)
	return out, err
}

// ContainerStatus 调用运行时服务查询容器状态，并记录指标。
func (in instrumentedRuntimeService) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	const operation = "container_status"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ContainerStatus(ctx, containerID, verbose)
	recordError(operation, err)
	return out, err
}

// UpdateContainerResources 调用运行时服务更新容器资源，并记录指标。
func (in instrumentedRuntimeService) UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error {
	const operation = "update_container"
	defer recordOperation(operation, time.Now())

	err := in.service.UpdateContainerResources(ctx, containerID, resources)
	recordError(operation, err)
	return err
}

// ReopenContainerLog 调用运行时服务重新打开容器日志，并记录指标。
func (in instrumentedRuntimeService) ReopenContainerLog(ctx context.Context, containerID string) error {
	const operation = "reopen_container_log"
	defer recordOperation(operation, time.Now())

	err := in.service.ReopenContainerLog(ctx, containerID)
	recordError(operation, err)
	return err
}

// ExecSync 调用运行时服务同步执行命令，并记录指标。
func (in instrumentedRuntimeService) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error) {
	const operation = "exec_sync"
	defer recordOperation(operation, time.Now())

	stdout, stderr, err := in.service.ExecSync(ctx, containerID, cmd, timeout)
	recordError(operation, err)
	return stdout, stderr, err
}

// Exec 调用运行时服务发起流式 exec 请求，并记录指标。
func (in instrumentedRuntimeService) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	const operation = "exec"
	defer recordOperation(operation, time.Now())

	resp, err := in.service.Exec(ctx, req)
	recordError(operation, err)
	return resp, err
}

// Attach 调用运行时服务发起 attach 请求，并记录指标。
func (in instrumentedRuntimeService) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	const operation = "attach"
	defer recordOperation(operation, time.Now())

	resp, err := in.service.Attach(ctx, req)
	recordError(operation, err)
	return resp, err
}

// RunPodSandbox 调用运行时服务创建并启动 Pod 沙箱，并额外记录沙箱启动耗时与失败数。
func (in instrumentedRuntimeService) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	const operation = "run_podsandbox"
	// 沙箱创建是启动路径上的关键耗时点，额外记录其耗时与失败次数。
	startTime := time.Now()
	defer recordOperation(operation, startTime)
	defer metrics.RunPodSandboxDuration.ObserveSince(startTime, runtimeHandler)()

	out, err := in.service.RunPodSandbox(ctx, config, runtimeHandler)
	recordError(operation, err)
	if err != nil {
		// 沙箱启动失败按 runtimeHandler 维度单独计数，便于定位运行时问题。
		metrics.RunPodSandboxErrors.WithLabelValues(runtimeHandler).Inc()
	}
	return out, err
}

// StopPodSandbox 调用运行时服务停止 Pod 沙箱，并记录指标。
func (in instrumentedRuntimeService) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	const operation = "stop_podsandbox"
	defer recordOperation(operation, time.Now())

	err := in.service.StopPodSandbox(ctx, podSandboxID)
	recordError(operation, err)
	return err
}

// RemovePodSandbox 调用运行时服务删除 Pod 沙箱，并记录指标。
func (in instrumentedRuntimeService) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	const operation = "remove_podsandbox"
	defer recordOperation(operation, time.Now())

	err := in.service.RemovePodSandbox(ctx, podSandboxID)
	recordError(operation, err)
	return err
}

// PodSandboxStatus 调用运行时服务查询 Pod 沙箱状态，并记录指标。
func (in instrumentedRuntimeService) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	const operation = "podsandbox_status"
	defer recordOperation(operation, time.Now())

	out, err := in.service.PodSandboxStatus(ctx, podSandboxID, verbose)
	recordError(operation, err)
	return out, err
}

// ListPodSandbox 调用运行时服务列出 Pod 沙箱，并记录指标。
func (in instrumentedRuntimeService) ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	const operation = "list_podsandbox"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListPodSandbox(ctx, filter)
	recordError(operation, err)
	return out, err
}

// ContainerStats 调用运行时服务获取容器统计信息，并记录指标。
func (in instrumentedRuntimeService) ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	const operation = "container_stats"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ContainerStats(ctx, containerID)
	recordError(operation, err)
	return out, err
}

// ListContainerStats 调用运行时服务列出容器统计信息，并记录指标。
func (in instrumentedRuntimeService) ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	const operation = "list_container_stats"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListContainerStats(ctx, filter)
	recordError(operation, err)
	return out, err
}

// PodSandboxStats 调用运行时服务获取 Pod 沙箱统计信息，并记录指标。
func (in instrumentedRuntimeService) PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	const operation = "podsandbox_stats"
	defer recordOperation(operation, time.Now())

	out, err := in.service.PodSandboxStats(ctx, podSandboxID)
	recordError(operation, err)
	return out, err
}

// ListPodSandboxStats 调用运行时服务列出 Pod 沙箱统计信息，并记录指标。
func (in instrumentedRuntimeService) ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	const operation = "list_podsandbox_stats"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListPodSandboxStats(ctx, filter)
	recordError(operation, err)
	return out, err
}

// PortForward 调用运行时服务发起端口转发请求，并记录指标。
func (in instrumentedRuntimeService) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	const operation = "port_forward"
	defer recordOperation(operation, time.Now())

	resp, err := in.service.PortForward(ctx, req)
	recordError(operation, err)
	return resp, err
}

// UpdatePodSandboxResources 调用运行时服务更新 Pod 沙箱资源，并记录指标。
func (in instrumentedRuntimeService) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	const operation = "update_podsandbox_resources"
	defer recordOperation(operation, time.Now())

	resp, err := in.service.UpdatePodSandboxResources(ctx, req)
	recordError(operation, err)
	return resp, err
}

// UpdateRuntimeConfig 调用运行时服务更新运行时配置，并记录指标。
func (in instrumentedRuntimeService) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *runtimeapi.RuntimeConfig) error {
	const operation = "update_runtime_config"
	defer recordOperation(operation, time.Now())

	err := in.service.UpdateRuntimeConfig(ctx, runtimeConfig)
	recordError(operation, err)
	return err
}

// ListImages 调用镜像服务列出镜像，并记录指标。
func (in instrumentedImageManagerService) ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	const operation = "list_images"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListImages(ctx, filter)
	recordError(operation, err)
	return out, err
}

// ImageStatus 调用镜像服务查询镜像状态，并记录指标。
func (in instrumentedImageManagerService) ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	const operation = "image_status"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ImageStatus(ctx, image, verbose)
	recordError(operation, err)
	return out, err
}

// PullImage 调用镜像服务拉取镜像，并记录指标。
func (in instrumentedImageManagerService) PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	const operation = "pull_image"
	defer recordOperation(operation, time.Now())

	imageRef, err := in.service.PullImage(ctx, image, auth, podSandboxConfig)
	recordError(operation, err)
	return imageRef, err
}

// RemoveImage 调用镜像服务删除镜像，并记录指标。
func (in instrumentedImageManagerService) RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) error {
	const operation = "remove_image"
	defer recordOperation(operation, time.Now())

	err := in.service.RemoveImage(ctx, image)
	recordError(operation, err)
	return err
}

// ImageFsInfo 调用镜像服务获取镜像文件系统信息，并记录指标。
func (in instrumentedImageManagerService) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	const operation = "image_fs_info"
	defer recordOperation(operation, time.Now())

	fsInfo, err := in.service.ImageFsInfo(ctx)
	recordError(operation, err)
	return fsInfo, nil
}

// Close 关闭镜像服务客户端，并记录指标。
func (in instrumentedImageManagerService) Close(ctx context.Context) error {
	const operation = "close"
	defer recordOperation(operation, time.Now())

	err := in.service.Close(ctx)
	recordError(operation, err)
	return err
}

// CheckpointContainer 调用运行时服务对容器执行检查点，并记录指标。
func (in instrumentedRuntimeService) CheckpointContainer(ctx context.Context, options *runtimeapi.CheckpointContainerRequest) error {
	const operation = "checkpoint_container"
	defer recordOperation(operation, time.Now())

	err := in.service.CheckpointContainer(ctx, options)
	recordError(operation, err)
	return err
}

// CheckpointPod 调用运行时服务对 Pod 执行检查点，并记录指标。
func (in instrumentedRuntimeService) CheckpointPod(ctx context.Context, options *runtimeapi.CheckpointPodRequest) error {
	const operation = "checkpoint_pod"
	defer recordOperation(operation, time.Now())

	err := in.service.CheckpointPod(ctx, options)
	recordError(operation, err)
	return err
}

// RestorePod 调用运行时服务从检查点恢复 Pod，并记录指标。
func (in instrumentedRuntimeService) RestorePod(ctx context.Context, options *runtimeapi.RestorePodRequest) (*runtimeapi.RestorePodResponse, error) {
	const operation = "restore_pod"
	defer recordOperation(operation, time.Now())

	response, err := in.service.RestorePod(ctx, options)
	recordError(operation, err)
	return response, err
}

// GetContainerEvents 订阅运行时服务的容器事件流，并记录指标。
func (in instrumentedRuntimeService) GetContainerEvents(ctx context.Context, containerEventsCh chan *runtimeapi.ContainerEventResponse, connectionEstablishedCallback func(runtimeapi.RuntimeService_GetContainerEventsClient)) error {
	const operation = "get_container_events"
	defer recordOperation(operation, time.Now())

	err := in.service.GetContainerEvents(ctx, containerEventsCh, connectionEstablishedCallback)
	recordError(operation, err)
	return err
}

// ListMetricDescriptors 调用运行时服务列出指标描述符，并记录指标。
func (in instrumentedRuntimeService) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	const operation = "list_metric_descriptors"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListMetricDescriptors(ctx)
	recordError(operation, err)
	return out, err
}

// ListPodSandboxMetrics 调用运行时服务列出 Pod 沙箱指标，并记录指标。
func (in instrumentedRuntimeService) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	const operation = "list_podsandbox_metrics"
	defer recordOperation(operation, time.Now())

	out, err := in.service.ListPodSandboxMetrics(ctx)
	recordError(operation, err)
	return out, err
}

// RuntimeConfig 调用运行时服务获取运行时配置，并记录指标。
func (in instrumentedRuntimeService) RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	const operation = "runtime_config"
	defer recordOperation(operation, time.Now())

	out, err := in.service.RuntimeConfig(ctx)
	recordError(operation, err)
	return out, err
}

// Close 关闭运行时服务客户端，并记录指标。
func (in instrumentedRuntimeService) Close(ctx context.Context) error {
	const operation = "close"
	defer recordOperation(operation, time.Now())

	err := in.service.Close(ctx)
	recordError(operation, err)
	return err
}
