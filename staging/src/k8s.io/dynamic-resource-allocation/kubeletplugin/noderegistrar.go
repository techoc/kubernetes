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

package kubeletplugin

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

// nodeRegistrar 是节点上的插件注册服务，内嵌通用注册服务器并持有一个 gRPC 服务器。
type nodeRegistrar struct {
	// registrationServer 实现 kubelet 的插件注册 gRPC 接口。
	// server 是承载注册服务的非阻塞 gRPC 服务器。
	registrationServer
	server *grpcServer
}

// startRegistrar returns a running instance.
// startRegistrar 创建并启动一个注册服务实例。
func startRegistrar(logger klog.Logger, grpcVerbosity int, interceptors []grpc.UnaryServerInterceptor, streamInterceptors []grpc.StreamServerInterceptor, driverName string, supportedServices []string, draEndpointPath string, pluginRegistrationEndpoint endpoint, errHandler func(ctx context.Context, err error)) (*nodeRegistrar, error) {
	// 构造注册服务器；DRA 用 supportedVersions 字段描述所提供的服务（如 "v1beta1.DRAPlugin"）。
	n := &nodeRegistrar{
		registrationServer: registrationServer{
			driverName:        driverName,
			draEndpointPath:   draEndpointPath,
			supportedVersions: supportedServices, // DRA uses this field to describe provided services (e.g. "v1beta1.DRAPlugin").
		},
	}
	// 在插件注册端点上启动 gRPC 服务器，并把注册服务注册进去。
	s, err := startGRPCServer(logger, grpcVerbosity, interceptors, streamInterceptors, pluginRegistrationEndpoint, errHandler, func(grpcServer *grpc.Server) {
		registerapi.RegisterRegistrationServer(grpcServer, n)
	})
	// 启动失败时包装错误返回。
	if err != nil {
		return nil, fmt.Errorf("start gRPC server: %v", err)
	}
	// 保存服务器句柄，供后续停止使用。
	n.server = s
	return n, nil
}

// stop ensures that the registrar is not running anymore and cleans up all resources.
// It is idempotent and may be called with a nil pointer.
// stop 停止注册服务并清理资源；方法幂等，可在 nil 指针上调用。
func (s *nodeRegistrar) stop() {
	// nil 接收者直接返回。
	if s == nil {
		return
	}
	// 委托 gRPC 服务器停止并等待。
	s.server.stop()
}
