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

/*
Package cache 实现 kubelet 插件管理器用于跟踪已注册插件的数据结构。
*/
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// ActualStateOfWorld 定义 kubelet 插件管理器"实际世界"缓存的一组线程安全操作。
// 该缓存维护从 socket 文件路径到插件信息的映射，记录当前节点上所有已连接的插件。
type ActualStateOfWorld interface {

	// GetRegisteredPlugins 返回当前实际状态中已成功注册的插件列表。
	GetRegisteredPlugins() []PluginInfo

	// AddPlugin 把给定插件加入缓存；SocketPath 为空时返回错误。
	// 这里无需处理"时间戳不一致则更新"的情况：插件重新注册前一定已由 reconciler
	// 注销并从实际状态缓存中删除，之后才会以新时间戳重新加入。
	AddPlugin(ctx context.Context, pluginInfo PluginInfo) error

	// RemovePlugin 删除指定 socket 路径的插件；路径不存在时为空操作。
	RemovePlugin(socketPath string)

	// PluginExistsWithCorrectTimestamp 检查实际状态中是否存在该插件且时间戳一致。
	// Deprecated: 请改用 PluginExistsWithCorrectUUID，后者具备更好的跨平台支持。
	PluginExistsWithCorrectTimestamp(pluginInfo PluginInfo) bool

	// PluginExistsWithCorrectUUID 检查实际状态中是否存在该插件且 UUID 一致。
	PluginExistsWithCorrectUUID(pluginInfo PluginInfo) bool
}

// NewActualStateOfWorld 创建并返回 ActualStateOfWorld 实例。
func NewActualStateOfWorld() ActualStateOfWorld {
	return &actualStateOfWorld{
		socketFileToInfo: make(map[string]PluginInfo),
	}
}

// actualStateOfWorld 是 ActualStateOfWorld 的实现，map 读写由内嵌 RWMutex 保护。
type actualStateOfWorld struct {

	// socketFileToInfo 保存已成功注册的插件：键为插件 socket 文件路径，值为 PluginInfo。
	socketFileToInfo map[string]PluginInfo
	sync.RWMutex
}

// 编译期断言：actualStateOfWorld 实现 ActualStateOfWorld 接口。
var _ ActualStateOfWorld = &actualStateOfWorld{}

// PluginInfo 保存插件信息。
type PluginInfo struct {
	SocketPath string
	Timestamp  time.Time
	UUID       types.UID
	Handler    PluginHandler
	Name       string
	Endpoint   string
}

// AddPlugin 在持有写锁的情况下把插件写入缓存。
func (asw *actualStateOfWorld) AddPlugin(ctx context.Context, pluginInfo PluginInfo) error {
	asw.Lock()
	defer asw.Unlock()

	logger := klog.FromContext(ctx)

	// socket 路径是缓存键，为空说明插件信息不完整，必须拒绝。
	if pluginInfo.SocketPath == "" {
		return fmt.Errorf("socket path is empty")
	}
	if _, ok := asw.socketFileToInfo[pluginInfo.SocketPath]; ok {
		// 重复注册只记录日志并覆盖，最终状态以最新信息为准。
		logger.V(2).Info("Plugin exists in actual state cache", "path", pluginInfo.SocketPath)
	}
	asw.socketFileToInfo[pluginInfo.SocketPath] = pluginInfo
	return nil
}

// RemovePlugin 删除指定 socket 路径的插件。
func (asw *actualStateOfWorld) RemovePlugin(socketPath string) {
	asw.Lock()
	defer asw.Unlock()

	delete(asw.socketFileToInfo, socketPath)
}

// GetRegisteredPlugins 复制当前所有插件信息后返回，避免调用方直接持有内部 map。
func (asw *actualStateOfWorld) GetRegisteredPlugins() []PluginInfo {
	asw.RLock()
	defer asw.RUnlock()

	currentPlugins := []PluginInfo{}
	for _, pluginInfo := range asw.socketFileToInfo {
		currentPlugins = append(currentPlugins, pluginInfo)
	}
	return currentPlugins
}

// PluginExistsWithCorrectTimestamp 同时校验 socket 路径与时间戳是否匹配。
func (asw *actualStateOfWorld) PluginExistsWithCorrectTimestamp(pluginInfo PluginInfo) bool {
	asw.RLock()
	defer asw.RUnlock()

	// 必须同时满足路径存在且时间戳与期望状态一致，防止把旧实例误判为新实例。
	actualStatePlugin, exists := asw.socketFileToInfo[pluginInfo.SocketPath]
	return exists && (actualStatePlugin.Timestamp == pluginInfo.Timestamp)
}

// PluginExistsWithCorrectUUID 同时校验 socket 路径与 UUID 是否匹配。
func (asw *actualStateOfWorld) PluginExistsWithCorrectUUID(pluginInfo PluginInfo) bool {
	asw.RLock()
	defer asw.RUnlock()

	// 必须同时满足路径存在且 UUID 与期望状态一致；UUID 在跨平台上更可靠。
	actualStatePlugin, exists := asw.socketFileToInfo[pluginInfo.SocketPath]
	return exists && (actualStatePlugin.UUID == pluginInfo.UUID)
}
