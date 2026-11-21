/*
Copyright 2018 The Kubernetes Authors.

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

package pluginwatcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	v1beta1 "k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher/example_plugin_apis/v1beta1"
	v1beta2 "k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher/example_plugin_apis/v1beta2"
)

// examplePlugin 是配合 plugin watcher 使用的示例插件。
type examplePlugin struct {
	grpcServer         *grpc.Server
	wg                 sync.WaitGroup
	registrationStatus chan registerapi.RegistrationStatus // for testing
	endpoint           string                              // for testing
	pluginName         string
	pluginType         string
	versions           []string

	registerapi.UnsafeRegistrationServer
}

// pluginServiceV1Beta1 是 v1beta1 示例服务的实现。
type pluginServiceV1Beta1 struct {
	server *examplePlugin
	v1beta1.UnsafeExampleServer
}

// GetExampleInfo 返回空的 v1beta1 响应并记录请求字段。
func (s *pluginServiceV1Beta1) GetExampleInfo(ctx context.Context, rqt *v1beta1.ExampleRequest) (*v1beta1.ExampleResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("GetExampleInfo v1beta1field", "field", rqt.V1Beta1Field)
	return &v1beta1.ExampleResponse{}, nil
}

// RegisterService 把 v1beta1 服务注册到 gRPC server。
func (s *pluginServiceV1Beta1) RegisterService() {
	v1beta1.RegisterExampleServer(s.server.grpcServer, s)
}

// pluginServiceV1Beta2 是 v1beta2 示例服务的实现。
type pluginServiceV1Beta2 struct {
	server *examplePlugin
	v1beta2.UnsafeExampleServer
}

// GetExampleInfo 返回空的 v1beta2 响应并记录请求字段。
func (s *pluginServiceV1Beta2) GetExampleInfo(ctx context.Context, rqt *v1beta2.ExampleRequest) (*v1beta2.ExampleResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("GetExampleInfo v1beta2_field", "field", rqt.V1Beta2Field)
	return &v1beta2.ExampleResponse{}, nil
}

// RegisterService 把 v1beta2 服务注册到 gRPC server。
func (s *pluginServiceV1Beta2) RegisterService() {
	v1beta2.RegisterExampleServer(s.server.grpcServer, s)
}

// NewExamplePlugin 返回已初始化的 examplePlugin 实例。
func NewExamplePlugin() *examplePlugin {
	return &examplePlugin{}
}

// NewTestExamplePlugin 返回用于测试的 examplePlugin 实例。
func NewTestExamplePlugin(pluginName string, pluginType string, endpoint string, advertisedVersions ...string) *examplePlugin {
	return &examplePlugin{
		pluginName:         pluginName,
		pluginType:         pluginType,
		endpoint:           endpoint,
		versions:           advertisedVersions,
		registrationStatus: make(chan registerapi.RegistrationStatus),
	}
}

// GetPluginInfo 返回插件对应的 cache.PluginInfo。
func GetPluginInfo(plugin *examplePlugin) cache.PluginInfo {
	return cache.PluginInfo{
		SocketPath: plugin.endpoint,
	}
}

// GetInfo 是 plugin watcher 调用的 RPC，返回插件类型、名称、端点与支持的版本。
func (e *examplePlugin) GetInfo(ctx context.Context, req *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	return &registerapi.PluginInfo{
		Type:              e.pluginType,
		Name:              e.pluginName,
		Endpoint:          e.endpoint,
		SupportedVersions: e.versions,
	}, nil
}

// NotifyRegistrationStatus 接收注册结果并转发给测试通道。
func (e *examplePlugin) NotifyRegistrationStatus(ctx context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("Notify registration status", "status", status)

	if e.registrationStatus != nil {
		e.registrationStatus <- *status
	}

	return &registerapi.RegistrationStatusResponse{}, nil
}

// Serve 启动 plugin watcher 服务器与一个或多个插件服务。
func (e *examplePlugin) Serve(ctx context.Context, services ...string) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting example server", "endpoint", e.endpoint)
	// 在 unix socket 上监听；失败直接返回。
	lis, err := net.Listen("unix", e.endpoint)
	if err != nil {
		return err
	}

	logger.Info("Example server started", "endpoint", e.endpoint)
	// 创建 gRPC server 并注册插件注册服务。
	e.grpcServer = grpc.NewServer()

	// 注册 kubelet plugin watcher API。
	registerapi.RegisterRegistrationServer(e.grpcServer, e)

	// 按名称注册请求的示例服务版本。
	for _, service := range services {
		switch service {
		case "v1beta1":
			v1beta1 := &pluginServiceV1Beta1{server: e}
			v1beta1.RegisterService()
		case "v1beta2":
			v1beta2 := &pluginServiceV1Beta2{server: e}
			v1beta2.RegisterService()
		default:
			return fmt.Errorf("unsupported service: '%s'", service)
		}
	}

	// 在后台启动服务，Stop 时通过 WaitGroup 等待退出。
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		// Blocking call to accept incoming connections.
		if err := e.grpcServer.Serve(lis); err != nil {
			logger.Error(err, "Example server stopped serving")
		}
	}()

	return nil
}

// Stop 停止 gRPC server、等待服务退出并删除 socket 文件。
func (e *examplePlugin) Stop(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Stopping example server", "endpoint", e.endpoint)

	e.grpcServer.Stop()
	c := make(chan struct{})
	go func() {
		defer close(c)
		e.wg.Wait()
	}()

	// 最多等待 1 秒，超时返回错误，避免 Stop 卡死。
	select {
	case <-c:
		break
	case <-time.After(time.Second):
		return errors.New("timed out on waiting for stop completion")
	}

	// socket 文件可能已被删除，忽略 IsNotExist。
	if err := os.Remove(e.endpoint); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
