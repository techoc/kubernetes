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

package devicemanager

import (
	"context"
	"fmt"
	"sync"
	"time"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	plugin "k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/plugin/v1beta1"
)

// endpoint 对应一个已注册的设备插件，负责管理与该插件的 gRPC 通信，
// 并缓存插件上报的设备状态。
type endpoint interface {
	// getPreferredAllocation 请求插件给出优先的设备分配建议。
	getPreferredAllocation(ctx context.Context, available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error)
	// allocate 请求插件为容器分配指定设备。
	allocate(ctx context.Context, devs []string) (*pluginapi.AllocateResponse, error)
	// preStartContainer 在容器启动前通知插件执行准备工作。
	preStartContainer(ctx context.Context, devs []string) (*pluginapi.PreStartContainerResponse, error)
	// setStopTime 记录插件停止的时间点。
	setStopTime(t time.Time)
	// isStopped 返回插件是否已停止。
	isStopped() bool
	// stopGracePeriodExpired 返回插件是否已超过停止宽限期。
	stopGracePeriodExpired() bool
	// socketPath 返回插件注册的 socket 路径。
	socketPath() string
}

// endpointImpl 是 endpoint 接口的实现。
type endpointImpl struct {
	// mutex 保护 stopTime 的并发访问。
	mutex sync.Mutex
	// resourceName 是插件上报的资源名称。
	resourceName string
	// socket 是插件监听的 socket 路径。
	socket string
	// api 是与插件通信的 gRPC 客户端。
	api pluginapi.DevicePluginClient
	// stopTime 是插件停止的时间；零值表示仍在运行。
	stopTime time.Time
	// client 仅供测试使用。
	client plugin.Client // for testing only
}

// newEndpointImpl 为给定插件创建 endpoint，用于正常的设备插件注册流程。
func newEndpointImpl(p plugin.DevicePlugin) *endpointImpl {
	return &endpointImpl{
		api:          p.API(),
		resourceName: p.Resource(),
		socket:       p.SocketPath(),
	}
}

// newStoppedEndpointImpl 创建一个已标记停止的 endpoint，
// 用于 kubelet 重启后、设备插件重新注册之前的过渡阶段。
func newStoppedEndpointImpl(resourceName string) *endpointImpl {
	return &endpointImpl{
		resourceName: resourceName,
		stopTime:     time.Now(),
	}
}

// socketPath 返回插件 socket 路径。
func (e *endpointImpl) socketPath() string {
	return e.socket
}

// isStopped 判断插件是否已停止。
func (e *endpointImpl) isStopped() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return !e.stopTime.IsZero()
}

// stopGracePeriodExpired 判断停止宽限期是否已过。
func (e *endpointImpl) stopGracePeriodExpired() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return !e.stopTime.IsZero() && time.Since(e.stopTime) > endpointStopGracePeriod
}

// setStopTime 记录插件停止时间。
func (e *endpointImpl) setStopTime(t time.Time) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.stopTime = t
}

// getPreferredAllocation 向设备插件发起 GetPreferredAllocation gRPC 调用。
func (e *endpointImpl) getPreferredAllocation(ctx context.Context, available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
	// 已停止的插件不能处理请求。
	if e.isStopped() {
		return nil, fmt.Errorf(errEndpointStopped, e)
	}
	return e.api.GetPreferredAllocation(ctx, &pluginapi.PreferredAllocationRequest{
		ContainerRequests: []*pluginapi.ContainerPreferredAllocationRequest{
			{
				AvailableDeviceIDs:   available,
				MustIncludeDeviceIDs: mustInclude,
				AllocationSize:       int32(size),
			},
		},
	})
}

// allocate 向设备插件发起 Allocate gRPC 调用。
func (e *endpointImpl) allocate(ctx context.Context, devs []string) (*pluginapi.AllocateResponse, error) {
	if e.isStopped() {
		return nil, fmt.Errorf(errEndpointStopped, e)
	}
	return e.api.Allocate(ctx, &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{
			{DevicesIds: devs},
		},
	})
}

// preStartContainer 向设备插件发起 PreStartContainer gRPC 调用。
func (e *endpointImpl) preStartContainer(ctx context.Context, devs []string) (*pluginapi.PreStartContainerResponse, error) {
	if e.isStopped() {
		return nil, fmt.Errorf(errEndpointStopped, e)
	}
	// PreStartContainer 有独立的 RPC 超时，避免阻塞容器启动。
	ctx, cancel := context.WithTimeout(ctx, pluginapi.KubeletPreStartContainerRPCTimeoutInSecs*time.Second)
	defer cancel()
	return e.api.PreStartContainer(ctx, &pluginapi.PreStartContainerRequest{
		DevicesIds: devs,
	})
}
