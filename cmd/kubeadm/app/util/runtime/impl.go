/*
Copyright 2024 The Kubernetes Authors.

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

package runtime

import (
	"context"
	"time"

	criapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	criclient "k8s.io/cri-client/pkg"
)

type defaultImpl struct{}

// Impl 是容器运行时实现的接口。
type Impl interface {
	NewRemoteRuntimeService(ctx context.Context, endpoint string, connectionTimeout time.Duration) (criapi.RuntimeService, error)
	NewRemoteImageService(ctx context.Context, endpoint string, connectionTimeout time.Duration) (criapi.ImageManagerService, error)
	RuntimeConfig(ctx context.Context, runtimeService criapi.RuntimeService) (*runtimeapi.RuntimeConfigResponse, error)
	Status(ctx context.Context, runtimeService criapi.RuntimeService, verbose bool) (*runtimeapi.StatusResponse, error)
	ListPodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error)
	StopPodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, sandboxID string) error
	RemovePodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, podSandboxID string) error
	PullImage(ctx context.Context, imageService criapi.ImageManagerService, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error)
	ImageStatus(ctx context.Context, imageService criapi.ImageManagerService, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error)
}

// NewRemoteRuntimeService 连接 endpoint 并创建远程 CRI 运行时服务，连接等待不超过 connectionTimeout。
func (*defaultImpl) NewRemoteRuntimeService(ctx context.Context, endpoint string, connectionTimeout time.Duration) (criapi.RuntimeService, error) {
	return criclient.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(connectionTimeout).
		Build(ctx)
}

// NewRemoteImageService 连接 endpoint 并创建远程 CRI 镜像服务，连接等待不超过 connectionTimeout。
func (*defaultImpl) NewRemoteImageService(ctx context.Context, endpoint string, connectionTimeout time.Duration) (criapi.ImageManagerService, error) {
	return criclient.NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(connectionTimeout).
		Build(ctx)
}

// RuntimeConfig 将运行时配置请求转发给 CRI 运行时服务。
func (*defaultImpl) RuntimeConfig(ctx context.Context, runtimeService criapi.RuntimeService) (*runtimeapi.RuntimeConfigResponse, error) {
	return runtimeService.RuntimeConfig(ctx)
}

// Status 将运行时状态请求转发给 CRI 运行时服务，verbose 控制是否返回详细信息。
func (*defaultImpl) Status(ctx context.Context, runtimeService criapi.RuntimeService, verbose bool) (*runtimeapi.StatusResponse, error) {
	return runtimeService.Status(ctx, verbose)
}

// ListPodSandbox 按 filter 查询 CRI 运行时服务中的 Pod sandbox。
func (*defaultImpl) ListPodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	return runtimeService.ListPodSandbox(ctx, filter)
}

// StopPodSandbox 请求 CRI 运行时服务停止指定 sandbox。
func (*defaultImpl) StopPodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, sandboxID string) error {
	return runtimeService.StopPodSandbox(ctx, sandboxID)
}

// RemovePodSandbox 请求 CRI 运行时服务移除指定 Pod sandbox。
func (*defaultImpl) RemovePodSandbox(ctx context.Context, runtimeService criapi.RuntimeService, podSandboxID string) error {
	return runtimeService.RemovePodSandbox(ctx, podSandboxID)
}

// PullImage 将镜像拉取请求及认证、sandbox 配置转发给 CRI 镜像服务，返回镜像引用。
func (*defaultImpl) PullImage(ctx context.Context, imageService criapi.ImageManagerService, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	return imageService.PullImage(ctx, image, auth, podSandboxConfig)
}

// ImageStatus 将镜像状态请求转发给 CRI 镜像服务，verbose 控制是否返回详细信息。
func (*defaultImpl) ImageStatus(ctx context.Context, imageService criapi.ImageManagerService, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	return imageService.ImageStatus(ctx, image, verbose)
}
