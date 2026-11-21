/*
Copyright 2019 The Kubernetes Authors.

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

// Package operationexecutor 实现插件注册与注销操作的执行接口，
// 借助 goroutinemap 保证同一个插件上不会同时触发多个操作。
package operationexecutor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/tools/record"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
)

const (
	// dialTimeoutDuration 是连接插件 socket 的超时；notifyTimeoutDuration 是通知插件注册状态的超时。
	dialTimeoutDuration   = 10 * time.Second
	notifyTimeoutDuration = 5 * time.Second
)

// 编译期断言：operationGenerator 必须实现 OperationGenerator 接口。
var _ OperationGenerator = &operationGenerator{}

// operationGenerator 生成注册/注销插件的操作函数。
type operationGenerator struct {

	// recorder 用于向 API server 记录事件。
	recorder record.EventRecorder
}

// NewOperationGenerator 返回 operationGenerator 实例。
func NewOperationGenerator(recorder record.EventRecorder) OperationGenerator {

	return &operationGenerator{
		recorder: recorder,
	}
}

// OperationGenerator 接口把操作执行器的函数抽取出来，便于依赖注入。
type OperationGenerator interface {
	// GenerateRegisterPluginFunc 生成执行插件注册所需的函数。
	GenerateRegisterPluginFunc(
		ctx context.Context,
		socketPath string,
		UUID types.UID,
		pluginHandlers map[string]cache.PluginHandler,
		actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error

	// GenerateUnregisterPluginFunc 生成执行插件注销所需的函数。
	GenerateUnregisterPluginFunc(
		ctx context.Context,
		pluginInfo cache.PluginInfo,
		actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error
}

// GenerateRegisterPluginFunc 返回闭包：连接插件 socket、校验插件、更新实际状态缓存，
// 并通知插件注册结果。
func (og *operationGenerator) GenerateRegisterPluginFunc(
	ctx context.Context,
	socketPath string,
	pluginUUID types.UID,
	pluginHandlers map[string]cache.PluginHandler,
	actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error {

	registerPluginFunc := func() error {
		logger := klog.FromContext(ctx)

		client, conn, err := dial(ctx, socketPath, dialTimeoutDuration)
		if err != nil {
			return fmt.Errorf("RegisterPlugin error -- dial failed at socket %s, err: %v", socketPath, err)
		}
		defer conn.Close()

		// 为 GetInfo 调用创建独立的超时上下文，避免阻塞注册流程。
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		infoResp, err := client.GetInfo(ctxWithTimeout, &registerapi.InfoRequest{})
		if err != nil {
			return fmt.Errorf("RegisterPlugin error -- failed to get plugin info using RPC GetInfo at socket %s, err: %v", socketPath, err)
		}

		// 没有对应类型的处理器时，通知插件失败并返回错误。
		handler, ok := pluginHandlers[infoResp.Type]
		if !ok {
			if err := og.notifyPlugin(ctx, client, false, fmt.Sprintf("RegisterPlugin error -- no handler registered for plugin type: %s at socket %s", infoResp.Type, socketPath)); err != nil {
				return fmt.Errorf("RegisterPlugin error -- failed to send error at socket %s, err: %v", socketPath, err)
			}
			return fmt.Errorf("RegisterPlugin error -- no handler registered for plugin type: %s at socket %s", infoResp.Type, socketPath)
		}

		// 插件未上报 endpoint 时，默认使用 socket 路径。
		if infoResp.Endpoint == "" {
			infoResp.Endpoint = socketPath
		}
		if err := handler.ValidatePlugin(ctx, infoResp.Name, infoResp.Endpoint, infoResp.SupportedVersions); err != nil {
			if err = og.notifyPlugin(ctx, client, false, fmt.Sprintf("RegisterPlugin error -- plugin validation failed with err: %v", err)); err != nil {
				return fmt.Errorf("RegisterPlugin error -- failed to send error at socket %s, err: %v", socketPath, err)
			}
			return fmt.Errorf("RegisterPlugin error -- pluginHandler.ValidatePluginFunc failed")
		}
		// 先写入实际状态缓存，再调用消费者的注册逻辑；这样注册过程中若收到删除事件，
		// 也能按注销流程处理。
		err = actualStateOfWorldUpdater.AddPlugin(ctx, cache.PluginInfo{
			SocketPath: socketPath,
			UUID:       pluginUUID,
			Handler:    handler,
			Name:       infoResp.Name,
			Endpoint:   infoResp.Endpoint,
		})
		if err != nil {
			logger.Error(err, "RegisterPlugin error -- failed to add plugin", "path", socketPath)
		}
		if err := handler.RegisterPlugin(ctx, infoResp.Name, infoResp.Endpoint, infoResp.SupportedVersions, nil); err != nil {
			// 注册失败时回滚实际状态缓存。
			actualStateOfWorldUpdater.RemovePlugin(socketPath)
			return og.notifyPlugin(ctx, client, false, fmt.Sprintf("RegisterPlugin error -- plugin registration failed with err: %v", err))
		}

		// 注册完成后才发送通知，确保即使通知失败，Register 也总是在校验之后被调用。
		if err := og.notifyPlugin(ctx, client, true, ""); err != nil {
			// 通知失败时同样回滚缓存并注销插件。
			actualStateOfWorldUpdater.RemovePlugin(socketPath)
			handler.DeRegisterPlugin(ctx, infoResp.Name, infoResp.Endpoint)
			return fmt.Errorf("RegisterPlugin error -- failed to send registration status at socket %s, err: %v", socketPath, err)
		}
		return nil
	}
	return registerPluginFunc
}

// GenerateUnregisterPluginFunc 返回闭包：从实际状态缓存移除插件，并调用消费者的注销逻辑。
func (og *operationGenerator) GenerateUnregisterPluginFunc(
	ctx context.Context,
	pluginInfo cache.PluginInfo,
	actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error {

	unregisterPluginFunc := func() error {
		logger := klog.FromContext(ctx)

		if pluginInfo.Handler == nil {
			return fmt.Errorf("UnregisterPlugin error -- failed to get plugin handler for %s", pluginInfo.SocketPath)
		}
		// 先移除实际状态缓存中的插件，再调用消费者的注销逻辑；这样注销过程中若收到注册事件，
		// 也能按注册流程处理。
		actualStateOfWorldUpdater.RemovePlugin(pluginInfo.SocketPath)

		pluginInfo.Handler.DeRegisterPlugin(ctx, pluginInfo.Name, pluginInfo.Endpoint)

		logger.V(4).Info("DeRegisterPlugin called", "pluginName", pluginInfo.Name, "pluginHandler", pluginInfo.Handler)
		return nil
	}
	return unregisterPluginFunc
}

// notifyPlugin 通过 gRPC 把注册状态通知给插件。
func (og *operationGenerator) notifyPlugin(ctx context.Context, client registerapi.RegistrationClient, registered bool, errStr string) error {
	ctx, cancel := context.WithTimeout(ctx, notifyTimeoutDuration)
	defer cancel()

	status := &registerapi.RegistrationStatus{
		PluginRegistered: registered,
		Error:            errStr,
	}

	// 通知调用本身失败时返回包装后的错误；业务错误字符串则作为普通错误返回。
	if _, err := client.NotifyRegistrationStatus(ctx, status); err != nil {
		return fmt.Errorf("%s: %w", errStr, err)
	}

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

// dial 与发现的插件 socket 建立 gRPC 连接（https://godoc.org/google.golang.org/grpc#Dial）。
func dial(ctx context.Context, unixSocketPath string, timeout time.Duration) (registerapi.RegistrationClient, *grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 使用 unix domain socket 拨号，并等待连接建立（WithBlock）。
	c, err := grpc.DialContext(ctx, unixSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial socket %s, err: %v", unixSocketPath, err)
	}

	return registerapi.NewRegistrationClient(c), c, nil
}
