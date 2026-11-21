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
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2"
)

// DesiredStateOfWorld 定义 kubelet 插件管理器"期望世界"缓存的线程安全操作。
// 该缓存维护从 socket 文件路径到插件信息的映射，记录节点上所有应存在的插件。
type DesiredStateOfWorld interface {
	// 若缓存中不存在则添加插件；已存在则更新时间戳。socketPath 为空时返回错误。
	AddOrUpdatePlugin(ctx context.Context, socketPath string) error

	// 删除指定 socket 路径的插件；不存在时为空操作。
	RemovePlugin(socketPath string)

	// 返回当前期望状态中需要注册的插件列表。
	GetPluginsToRegister() []PluginInfo

	// 检查指定 socket 路径是否存在于当前期望状态缓存。
	PluginExists(socketPath string) bool
}

// NewDesiredStateOfWorld 创建 DesiredStateOfWorld 实例。
func NewDesiredStateOfWorld() DesiredStateOfWorld {
	return &desiredStateOfWorld{
		socketFileToInfo: make(map[string]PluginInfo),
	}
}

// desiredStateOfWorld 是 DesiredStateOfWorld 的实现。
type desiredStateOfWorld struct {

	// socketFileToInfo 保存应存在的插件：键为 socket 路径，值为 PluginInfo。
	socketFileToInfo map[string]PluginInfo
	sync.RWMutex
}

// 编译期断言：desiredStateOfWorld 实现 DesiredStateOfWorld 接口。
var _ DesiredStateOfWorld = &desiredStateOfWorld{}

// generatePluginMsgDetailed 生成用于日志的详细错误信息。
func generatePluginMsgDetailed(prefixMsg, suffixMsg, socketPath, details string) (detailedMsg string) {
	return fmt.Sprintf("%v for plugin at %q %v %v", prefixMsg, socketPath, details, suffixMsg)
}

// generatePluginMsg 生成面向事件/用户的简要信息与用于日志的详细信息。
func generatePluginMsg(prefixMsg, suffixMsg, socketPath, details string) (simpleMsg, detailedMsg string) {
	simpleMsg = fmt.Sprintf("%v for plugin at %q %v", prefixMsg, socketPath, suffixMsg)
	return simpleMsg, generatePluginMsgDetailed(prefixMsg, suffixMsg, socketPath, details)
}

// GenerateMsgDetailed 返回用于日志的插件注册详细信息。
// 格式为 "<prefixMsg> <plugin details> <suffixMsg>"。
func (plugin *PluginInfo) GenerateMsgDetailed(prefixMsg, suffixMsg string) (detailedMsg string) {
	detailedStr := fmt.Sprintf("(plugin details: %v)", plugin)
	return generatePluginMsgDetailed(prefixMsg, suffixMsg, plugin.SocketPath, detailedStr)
}

// GenerateMsg 返回用户友好的简要信息与用于日志的详细信息。
// 格式为 "<prefixMsg> <plugin details> <suffixMsg>"。
func (plugin *PluginInfo) GenerateMsg(prefixMsg, suffixMsg string) (simpleMsg, detailedMsg string) {
	detailedStr := fmt.Sprintf("(plugin details: %v)", plugin)
	return generatePluginMsg(prefixMsg, suffixMsg, plugin.SocketPath, detailedStr)
}

// GenerateErrorDetailed 返回用于日志的插件注册详细错误。
// 格式为 "<prefixMsg> <plugin details>: <err> "。
func (plugin *PluginInfo) GenerateErrorDetailed(prefixMsg string, err error) (detailedErr error) {
	return errors.New(plugin.GenerateMsgDetailed(prefixMsg, errSuffix(err)))
}

// GenerateError 返回用户友好的简要错误与用于日志的详细错误。
// 格式为 "<prefixMsg> <plugin details>: <err> "。
func (plugin *PluginInfo) GenerateError(prefixMsg string, err error) (simpleErr, detailedErr error) {
	simpleMsg, detailedMsg := plugin.GenerateMsg(prefixMsg, errSuffix(err))
	return errors.New(simpleMsg), errors.New(detailedMsg)
}

// errSuffix 生成 ": <err>" 形式的后缀；err 为 nil 时返回空串。
func errSuffix(err error) string {
	errStr := ""
	if err != nil {
		errStr = fmt.Sprintf(": %v", err)
	}
	return errStr
}

// AddOrUpdatePlugin 在锁保护下新增或更新插件信息。
func (dsw *desiredStateOfWorld) AddOrUpdatePlugin(ctx context.Context, socketPath string) error {
	dsw.Lock()
	defer dsw.Unlock()

	logger := klog.FromContext(ctx)

	// socket 路径是缓存键，为空说明调用方传参错误。
	if socketPath == "" {
		return fmt.Errorf("socket path is empty")
	}
	if _, ok := dsw.socketFileToInfo[socketPath]; ok {
		logger.V(2).Info("Plugin exists in desired state cache, timestamp will be updated", "path", socketPath)
	}

	// 只更新期望状态中的时间戳与 UUID，不更新实际状态；
	// reconciler 需要比较实际状态与期望状态中的插件版本，以决定是否重新注册。
	// 每次更新都生成新的 UUID，使 reconciler 能识别出插件已变化。
	dsw.socketFileToInfo[socketPath] = PluginInfo{
		SocketPath: socketPath,
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
	}
	return nil
}

// RemovePlugin 删除指定 socket 路径的插件。
func (dsw *desiredStateOfWorld) RemovePlugin(socketPath string) {
	dsw.Lock()
	defer dsw.Unlock()

	delete(dsw.socketFileToInfo, socketPath)
}

// GetPluginsToRegister 复制当前所有插件信息并返回。
func (dsw *desiredStateOfWorld) GetPluginsToRegister() []PluginInfo {
	dsw.RLock()
	defer dsw.RUnlock()

	pluginsToRegister := []PluginInfo{}
	for _, pluginInfo := range dsw.socketFileToInfo {
		pluginsToRegister = append(pluginsToRegister, pluginInfo)
	}
	return pluginsToRegister
}

// PluginExists 检查指定 socket 路径是否存在。
func (dsw *desiredStateOfWorld) PluginExists(socketPath string) bool {
	dsw.RLock()
	defer dsw.RUnlock()

	_, exists := dsw.socketFileToInfo[socketPath]
	return exists
}
