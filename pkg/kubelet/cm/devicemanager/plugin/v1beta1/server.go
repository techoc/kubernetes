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
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/opencontainers/selinux/go-selinux"
	"google.golang.org/grpc"

	core "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"
	api "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
)

// Server 接口定义设备插件注册服务器的方法。
type Server interface {
	cache.PluginHandler
	healthz.HealthChecker
	// Start 启动注册服务器并开始监听 unix socket。
	Start(klog.Logger) error
	// Stop 停止注册服务器并断开所有插件客户端。
	Stop(klog.Logger) error
	// SocketPath 返回注册服务器的 socket 路径。
	SocketPath() string
}

// server 实现设备插件注册服务器。
type server struct {
	socketName string
	socketDir  string
	mutex      sync.Mutex
	wg         sync.WaitGroup
	grpc       *grpc.Server
	rhandler   RegistrationHandler
	chandler   ClientHandler
	clients    map[string][]Client

	// lastError 记录最近的运行时错误；在真正出错之前服务器被视为健康。
	lastError error

	api.UnsafeRegistrationServer
}

// NewServer 返回初始化好的设备插件注册服务器。
func NewServer(logger klog.Logger, socketPath string, rh RegistrationHandler, ch ClientHandler) (Server, error) {
	// socket 路径必须是非空的绝对路径，否则无法可靠创建 socket。
	if socketPath == "" || !filepath.IsAbs(socketPath) {
		return nil, fmt.Errorf(errBadSocket+" %s", socketPath)
	}

	dir, name := filepath.Split(socketPath)

	logger.V(2).Info("Creating device plugin registration server", "version", api.Version, "socket", socketPath)
	s := &server{
		socketName: name,
		socketDir:  dir,
		rhandler:   rh,
		chandler:   ch,
		clients:    make(map[string][]Client),
	}

	return s, nil
}

// Start 创建 socket 目录、清理旧插件目录并启动 gRPC 注册服务器。
func (s *server) Start(logger klog.Logger) error {
	logger.V(2).Info("Starting device plugin registration server")

	if err := os.MkdirAll(s.socketDir, 0750); err != nil {
		logger.Error(err, "Failed to create the device plugin socket directory", "directory", s.socketDir)
		return err
	}

	if selinux.GetEnabled() {
		if err := selinux.SetFileLabel(s.socketDir, kubeletconfig.KubeletPluginsDirSELinuxLabel); err != nil {
			logger.Error(err, "Unprivileged containerized plugins might not work. Could not set selinux context on socket dir", "path", s.socketDir)
		}
	}

	// 目前把整个目录的清理工作交给 Handler（尽管理论上可以直接清空目录），
	// 因为 Handler 也把 checkpoint 等文件存放在这里。
	if err := s.rhandler.CleanupPluginDirectory(logger, s.socketDir); err != nil {
		logger.Error(err, "Failed to cleanup the device plugin directory", "directory", s.socketDir)
		return err
	}

	ln, err := net.Listen("unix", s.SocketPath())
	if err != nil {
		logger.Error(err, "Failed to listen to socket while starting device plugin registry")
		return err
	}

	s.wg.Add(1)
	s.grpc = grpc.NewServer([]grpc.ServerOption{}...)

	api.RegisterRegistrationServer(s.grpc, s)
	// 在后台运行 gRPC 服务；Serve 返回错误时把服务器标记为不健康。
	go func() {
		defer s.wg.Done()
		s.setHealthy()
		if err = s.grpc.Serve(ln); err != nil {
			s.setUnhealthy(err)
			logger.Error(err, "Error while serving device plugin registration grpc server")
		}
	}()

	return nil
}

// Stop 断开所有客户端并停止 gRPC 服务器。
func (s *server) Stop(logger klog.Logger) error {
	s.visitClients(func(r string, c Client) {
		if err := s.disconnectClient(logger, r, c); err != nil {
			logger.Error(err, "Failed to disconnect device plugin client", "resourceName", r)
		}
	})

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.grpc == nil {
		return nil
	}

	s.grpc.Stop()
	s.wg.Wait()
	s.grpc = nil
	// kubelet 终止过程中不再需要注册服务器；即使它已停止，
	// 也认为 kubelet 处于健康状态。
	s.setHealthy()
	logger.V(2).Info("Stopping device plugin registration server")

	return nil
}

// SocketPath 返回注册服务器的 socket 完整路径。
func (s *server) SocketPath() string {
	return filepath.Join(s.socketDir, s.socketName)
}

// Register 处理设备插件注册请求：校验版本与资源名，并连接插件客户端。
func (s *server) Register(ctx context.Context, r *api.RegisterRequest) (*api.Empty, error) {
	logger := klog.FromContext(ctx)
	logger.Info("Got registration request from device plugin with resource", "resourceName", r.ResourceName)
	// 统计注册请求次数，便于观察插件注册行为。
	metrics.DevicePluginRegistrationCount.WithLabelValues(r.ResourceName).Inc()

	if !s.isVersionCompatibleWithPlugin(r.Version) {
		err := fmt.Errorf(errUnsupportedVersion, r.Version, api.SupportedVersions)
		logger.Error(err, "Bad registration request from device plugin with resource", "resourceName", r.ResourceName)
		return &api.Empty{}, err
	}

	// 资源名必须是扩展资源（如 example.com/foo）。
	if !v1helper.IsExtendedResourceName(core.ResourceName(r.ResourceName)) {
		err := fmt.Errorf(errInvalidResourceName, r.ResourceName)
		logger.Error(err, "Bad registration request from device plugin")
		return &api.Empty{}, err
	}

	if err := s.connectClient(ctx, r.ResourceName, filepath.Join(s.socketDir, r.Endpoint)); err != nil {
		logger.Error(err, "Error connecting to device plugin client")
		return &api.Empty{}, err
	}

	return &api.Empty{}, nil
}

// isVersionCompatibleWithPlugin 判断插件声明的版本中是否有当前支持版本。
func (s *server) isVersionCompatibleWithPlugin(versions ...string) bool {
	// TODO(vikasc)：目前只有一个受支持版本，因此这样处理没有问题。将来需要支持
	// 多个版本时，可能需要扩展此函数以返回受支持的版本。
	// 例如，kubelet 支持 v1beta1 和 v1beta2，而设备插件提供 v1alpha1 和 v1beta1，
	// 此函数应返回 v1beta1。
	for _, version := range versions {
		for _, supportedVersion := range api.SupportedVersions {
			if version == supportedVersion {
				return true
			}
		}
	}
	return false
}

// visitClients 遍历所有客户端；遍历时临时释放锁，避免回调中死锁。
func (s *server) visitClients(visit func(r string, c Client)) {
	s.mutex.Lock()
	for r, cs := range s.clients {
		for _, c := range cs {
			// 回调期间释放锁，允许回调再次访问服务器状态。
			s.mutex.Unlock()
			visit(r, c)
			s.mutex.Lock()
		}
	}
	s.mutex.Unlock()
}

// Name 返回插件处理器的名称。
func (s *server) Name() string {
	return "device-plugin"
}

// Check 返回最近一次运行时错误，供健康检查使用。
func (s *server) Check(_ *http.Request) error {
	return s.lastError
}

// setHealthy 把 gRPC 服务器的健康状态设为健康。
func (s *server) setHealthy() {
	s.lastError = nil
}

// setUnhealthy 把 gRPC 服务器的健康状态设为不健康。
func (s *server) setUnhealthy(err error) {
	if err == nil {
		s.lastError = fmt.Errorf("device registration error: device plugin registration gRPC server failed and no device plugins can register")
		return
	}
	s.lastError = fmt.Errorf("device registration error: device plugin registration gRPC server failed and no device plugins can register: %w", err)
}
