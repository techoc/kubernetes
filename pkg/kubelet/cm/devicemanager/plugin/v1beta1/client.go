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

package v1beta1

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/klog/v2"
	api "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// DevicePlugin 接口提供访问设备插件资源、API 与 unix socket 的方法。
type DevicePlugin interface {
	// 返回插件的 gRPC 客户端。
	API() api.DevicePluginClient
	// 返回插件管理的资源名。
	Resource() string
	// 返回插件 socket 路径。
	SocketPath() string
}

// Client 接口提供建立/关闭 gRPC 连接以及运行设备插件 gRPC 客户端的方法。
type Client interface {
	// 建立 gRPC 连接。
	Connect(context.Context) error
	// 运行 gRPC 客户端。
	Run(context.Context)
	// 关闭 gRPC 连接。
	Disconnect(klog.Logger) error
	// 返回 socket 路径。
	SocketPath() string
}

// client 是 Client 的实现；mutex 保护 grpc/client 字段的并发读写。
type client struct {
	mutex    sync.Mutex
	resource string
	socket   string
	grpc     *grpc.ClientConn
	handler  ClientHandler
	client   api.DevicePluginClient
}

// NewPluginClient 返回已初始化的设备插件客户端。
func NewPluginClient(r string, socketPath string, h ClientHandler) Client {
	return &client{
		resource: r,
		socket:   socketPath,
		handler:  h,
	}
}

// Connect 在设备管理器与设备插件之间建立 gRPC 连接。
func (c *client) Connect(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	client, conn, err := dial(ctx, c.socket)
	if err != nil {
		logger.Error(err, "Unable to connect to device plugin client with socket path", "path", c.socket)
		return err
	}
	// 连接建立成功后才写入字段，避免 Run/Disconnect 看到半初始化状态。
	c.mutex.Lock()
	c.grpc = conn
	c.client = client
	c.mutex.Unlock()
	// 通知 handler 插件已连接，由 handler 触发后续注册流程。
	return c.handler.PluginConnected(ctx, c.resource, c)
}

// Run 运行设备插件 gRPC 客户端，持续消费 ListAndWatch 流。
func (c *client) Run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// FIXME：向 ListAndWatch 传递真实 context 会导致
	// TestDevicePluginReRegistration 因 "context cancelled" 错误而失败。
	// 启动 ListAndWatch；流结束即认为插件异常退出，记录错误后返回。
	stream, err := c.client.ListAndWatch(context.TODO(), &api.Empty{})
	if err != nil {
		logger.Error(err, "ListAndWatch ended unexpectedly for device plugin", "resource", c.resource)
		return
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			logger.Error(err, "ListAndWatch ended unexpectedly for device plugin", "resource", c.resource)
			return
		}
		logger.V(2).Info("State pushed for device plugin", "resource", c.resource, "resourceCapacity", len(response.Devices))
		// 把插件推送的最新设备列表交给 handler，用于更新可分配设备状态。
		c.handler.PluginListAndWatchReceiver(logger, c.resource, response)
	}
}

// Disconnect 关闭设备管理器与设备插件之间的 gRPC 连接。
func (c *client) Disconnect(logger klog.Logger) error {
	c.mutex.Lock()
	if c.grpc != nil {
		if err := c.grpc.Close(); err != nil {
			logger.V(2).Info("Failed to close grpc connection", "resource", c.Resource(), "err", err)
		}
		c.grpc = nil
	}
	c.mutex.Unlock()
	// 即使连接已为 nil 也要通知 handler，保持插件状态机一致。
	c.handler.PluginDisconnected(logger, c.resource, c.socket)

	logger.V(2).Info("Device plugin disconnected", "resource", c.resource)
	return nil
}

// Resource 返回插件管理的资源名。
func (c *client) Resource() string {
	return c.resource
}

// API 返回底层 gRPC 客户端。
func (c *client) API() api.DevicePluginClient {
	return c.client
}

// SocketPath 返回插件 socket 路径。
func (c *client) SocketPath() string {
	return c.socket
}

// dial 与已注册的设备插件建立 gRPC 通信，
// 参见 https://godoc.org/google.golang.org/grpc#Dial。
func dial(ctx context.Context, unixSocketPath string) (api.DevicePluginClient, *grpc.ClientConn, error) {
	// 设置 10 秒超时，避免拨号无限阻塞 kubelet 启动。
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 使用 unix socket 并阻塞拨号；Authority 固定为 localhost 以匹配服务端校验。
	c, err := grpc.DialContext(ctx, unixSocketPath,
		grpc.WithAuthority("localhost"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)

	if err != nil {
		// 拨号失败时返回带上下文的错误，便于定位具体 socket。
		return nil, nil, fmt.Errorf(errFailedToDialDevicePlugin+" %v", err)
	}

	return api.NewDevicePluginClient(c), c, nil
}
