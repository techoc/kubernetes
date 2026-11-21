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
	"os"
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	api "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
)

// GetPluginHandler 返回插件处理器，并尝试创建 DEPRECATION 标记文件。
func (s *server) GetPluginHandler(ctx context.Context) cache.PluginHandler {
	logger := klog.FromContext(ctx)
	// 创建标记文件用于提示该路径已废弃；失败只记录日志，不影响注册流程。
	if f, err := os.Create(s.socketDir + "DEPRECATION"); err != nil {
		logger.Error(err, "Failed to create deprecation file at socket dir", "path", s.socketDir)
	} else {
		f.Close()
		logger.V(4).Info("Created deprecation file", "path", f.Name())
	}
	return s
}

// RegisterPlugin 在校验通过后建立到插件的客户端连接。
func (s *server) RegisterPlugin(ctx context.Context, pluginName string, endpoint string, versions []string, pluginClientTimeout *time.Duration) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Registering plugin at endpoint", "plugin", pluginName, "endpoint", endpoint)
	return s.connectClient(ctx, pluginName, endpoint)
}

// DeRegisterPlugin 断开指定插件端点的连接。
func (s *server) DeRegisterPlugin(ctx context.Context, pluginName, endpoint string) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Deregistering plugin", "plugin", pluginName, "endpoint", endpoint)
	// DeRegisterPlugin 中的 endpoint 就是 socket 路径。
	// 找不到客户端说明已经断开，直接忽略。
	client := s.getClient(pluginName, endpoint)
	if client != nil {
		if err := s.disconnectClient(logger, pluginName, client); err != nil {
			logger.Error(err, "disconnecting client", "plugin", pluginName, "endpoint", endpoint)
		}
	}
}

// ValidatePlugin 校验插件声明的版本与资源名是否合法。
func (s *server) ValidatePlugin(ctx context.Context, pluginName string, endpoint string, versions []string) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Got plugin at endpoint with versions", "plugin", pluginName, "endpoint", endpoint, "versions", versions)

	// 插件必须支持当前 manager 的 API 版本，否则无法通信。
	if !s.isVersionCompatibleWithPlugin(versions...) {
		return fmt.Errorf("manager version, %s, is not among plugin supported versions %v", api.Version, versions)
	}

	// 资源名必须是合法的扩展资源名，防止插件冒用内置资源。
	if !v1helper.IsExtendedResourceName(core.ResourceName(pluginName)) {
		return fmt.Errorf("invalid name of device plugin socket: %s", fmt.Sprintf(errInvalidResourceName, pluginName))
	}

	logger.V(2).Info("Device plugin validated", "plugin", pluginName, "endpoint", endpoint, "versions", versions)
	return nil
}

// connectClient 创建客户端、登记后发起连接，成功后启动后台运行协程。
func (s *server) connectClient(ctx context.Context, name string, socketPath string) error {
	logger := klog.FromContext(ctx)
	c := NewPluginClient(name, socketPath, s.chandler)

	s.registerClient(logger, name, c)
	if err := c.Connect(ctx); err != nil {
		// 连接失败必须回滚登记，否则缓存中会残留不可用的客户端。
		s.deregisterClient(logger, name, socketPath)
		logger.Error(err, "Failed to connect to new client", "resource", name, "socketPath", socketPath)
		return err
	}

	logger.V(2).Info("Connected to new client", "resource", name, "socketPath", socketPath)
	// 连接成功后异步运行 ListAndWatch，避免阻塞注册流程。
	go func() {
		s.runClient(ctx, name, c)
	}()

	return nil
}

// disconnectClient 先注销登记，再关闭底层连接。
func (s *server) disconnectClient(logger klog.Logger, name string, c Client) error {
	s.deregisterClient(logger, name, c.SocketPath())
	return c.Disconnect(logger)
}

// registerClient 在锁保护下把客户端追加到同名插件的列表。
func (s *server) registerClient(logger klog.Logger, name string, c Client) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.clients[name] = append(s.clients[name], c)
	logger.V(2).Info("Registered client", "name", name, "socketPath", c.SocketPath())
}

// deregisterClient 重建客户端列表，移除指定 socket 的客户端。
func (s *server) deregisterClient(logger klog.Logger, name string, socketPath string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 注销时重建列表而非原地修改；仅当名字与 socket 路径都匹配才移除，
	// 保证同名多连接时只删除目标客户端。
	var newClients []Client
	for _, c := range s.clients[name] {
		if c.SocketPath() == socketPath {
			logger.V(2).Info("Deregistered client", "name", name, "socketPath", socketPath)
			continue
		}
		newClients = append(newClients, c)
	}

	if len(newClients) == 0 {
		delete(s.clients, name)
	} else {
		s.clients[name] = newClients
	}
}

// runClient 运行客户端；客户端退出后重新查找并断开，避免残留连接。
func (s *server) runClient(ctx context.Context, name string, c Client) {
	logger := klog.FromContext(ctx)
	c.Run(ctx)

	// Run 返回后重新查找，客户端可能已被其他路径注销。
	c = s.getClient(name, c.SocketPath())
	if c == nil {
		return
	}

	if err := s.disconnectClient(logger, name, c); err != nil {
		logger.Error(err, "Unable to disconnect client", "resource", name, "client", c)
	}
}

// getClient 在锁保护下按名字与 socket 路径查找客户端。
func (s *server) getClient(name string, socketPath string) Client {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, c := range s.clients[name] {
		if c.SocketPath() == socketPath {
			return c
		}
	}
	return nil
}
