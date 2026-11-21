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

// Package reconciler 实现把期望状态与实际状态对齐的接口，
// 通过触发注册/注销插件等动作完成调谐。
package reconciler

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/operationexecutor"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
)

// Reconciler 周期性运行调谐循环，通过触发注册与注销操作使期望状态与实际状态一致。
// 同时提供按插件类型注册 handler 的入口。
type Reconciler interface {
	// Run 启动调谐循环，周期性检查插件是否正确注册/注销，不满足时触发相应操作。
	Run(ctx context.Context)

	// AddHandler 为指定插件类型添加 handler，并加入实际状态缓存。
	AddHandler(pluginType string, pluginHandler cache.PluginHandler)
}

// NewReconciler 返回 Reconciler 实例。
//
// 参数说明：
// operationExecutor：安全触发注册/注销操作（防止同一 socket 路径上并发触发多个操作）；
// loopSleepDuration：调谐循环两次执行之间的休眠时间；
// desiredStateOfWorld：期望状态缓存；
// actualStateOfWorld：实际状态缓存。
func NewReconciler(
	operationExecutor operationexecutor.OperationExecutor,
	loopSleepDuration time.Duration,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld) Reconciler {
	return &reconciler{
		operationExecutor:   operationExecutor,
		loopSleepDuration:   loopSleepDuration,
		desiredStateOfWorld: desiredStateOfWorld,
		actualStateOfWorld:  actualStateOfWorld,
		handlers:            make(map[string]cache.PluginHandler),
	}
}

// reconciler 是 Reconciler 的实现，handlers 由内嵌 RWMutex 保护。
type reconciler struct {
	operationExecutor   operationexecutor.OperationExecutor
	loopSleepDuration   time.Duration
	desiredStateOfWorld cache.DesiredStateOfWorld
	actualStateOfWorld  cache.ActualStateOfWorld
	handlers            map[string]cache.PluginHandler
	sync.RWMutex
}

// 编译期断言：reconciler 实现 Reconciler 接口。
var _ Reconciler = &reconciler{}

// Run 按固定周期执行调谐，直到 ctx 取消。
func (rc *reconciler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		rc.reconcile(ctx)
	}, rc.loopSleepDuration)
}

// AddHandler 在锁保护下登记插件类型对应的 handler。
func (rc *reconciler) AddHandler(pluginType string, pluginHandler cache.PluginHandler) {
	rc.Lock()
	defer rc.Unlock()

	rc.handlers[pluginType] = pluginHandler
}

// getHandlers 返回 handler 的副本，避免调用方在锁外修改内部 map。
func (rc *reconciler) getHandlers() map[string]cache.PluginHandler {
	rc.RLock()
	defer rc.RUnlock()

	var copyHandlers = make(map[string]cache.PluginHandler)
	for pluginType, handler := range rc.handlers {
		copyHandlers[pluginType] = handler
	}
	return copyHandlers
}

// reconcile 执行一轮调谐：先注销多余/过期的插件，再注册缺失的插件。
func (rc *reconciler) reconcile(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 先处理注销再处理注册，避免同一插件在旧连接未断开时被重复注册。

	// 确保应注销的插件都被注销。
	for _, registeredPlugin := range rc.actualStateOfWorld.GetRegisteredPlugins() {
		unregisterPlugin := false
		if !rc.desiredStateOfWorld.PluginExists(registeredPlugin.SocketPath) {
			unregisterPlugin = true
		} else {
			// 对于同时存在于实际与期望状态、但 UUID 不一致的插件也要先注销；
			// 遍历期望状态，找出同一 socket 路径但 UUID 不同的插件。
			for _, dswPlugin := range rc.desiredStateOfWorld.GetPluginsToRegister() {
				if dswPlugin.SocketPath == registeredPlugin.SocketPath && dswPlugin.UUID != registeredPlugin.UUID {
					logger.V(5).Info("An updated version of plugin has been found, unregistering the plugin first before reregistering", "plugin", registeredPlugin)
					unregisterPlugin = true
					break
				}
			}
		}

		// 触发注销；已存在/退避中的错误属于预期，忽略即可。
		if unregisterPlugin {
			logger.V(5).Info("Starting operationExecutor.UnregisterPlugin", "plugin", registeredPlugin)
			err := rc.operationExecutor.UnregisterPlugin(ctx, registeredPlugin, rc.actualStateOfWorld)
			if err != nil &&
				!goroutinemap.IsAlreadyExists(err) &&
				!exponentialbackoff.IsExponentialBackoff(err) {
				// goroutinemap.IsAlreadyExists 与 exponentialbackoff.IsExponentialBackoff 属于预期错误，
				// 忽略；其他错误记录日志。
				logger.Error(err, "OperationExecutor.UnregisterPlugin failed", "plugin", registeredPlugin)
			}
			if err == nil {
				logger.V(1).Info("OperationExecutor.UnregisterPlugin started", "plugin", registeredPlugin)
			}
		}
	}

	// 确保应注册的插件都被注册。
	for _, pluginToRegister := range rc.desiredStateOfWorld.GetPluginsToRegister() {
		// 实际状态中不存在或 UUID 不一致时才注册。
		if !rc.actualStateOfWorld.PluginExistsWithCorrectUUID(pluginToRegister) {
			logger.V(5).Info("Starting operationExecutor.RegisterPlugin", "plugin", pluginToRegister)
			err := rc.operationExecutor.RegisterPlugin(ctx, pluginToRegister.SocketPath, pluginToRegister.UUID, rc.getHandlers(), rc.actualStateOfWorld)
			if err != nil &&
				!goroutinemap.IsAlreadyExists(err) &&
				!exponentialbackoff.IsExponentialBackoff(err) {
				// 同样忽略预期内的并发/退避错误。
				logger.Error(err, "OperationExecutor.RegisterPlugin failed", "plugin", pluginToRegister)
			}
			if err == nil {
				logger.V(1).Info("OperationExecutor.RegisterPlugin started", "plugin", pluginToRegister)
			}
		}
	}
}
