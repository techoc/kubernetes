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

// Package operationexecutor 提供插件注册与注销操作的执行接口。
// 它借助 goroutinemap 保证同一个插件上不会并发触发多个操作。
package operationexecutor

import (
	"context"

	"k8s.io/apimachinery/pkg/types"

	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
)

// OperationExecutor 定义插件注册与注销操作。
// 操作通过 GoRoutineMap 执行，保证同一 socket 路径上不会并发触发多个操作。
//
// 这些操作应具备幂等性（例如插件已注册时 RegisterPlugin 仍应成功），
// 但具体行为依赖各插件类型的处理器实现。
//
// 操作成功后，actualStateOfWorld 会被更新以反映插件的注册/注销状态。
//
// 操作异步执行，因此启动后发生的错误只会记录日志，
// 协程随即终止且不会更新 actualStateOfWorld。
type OperationExecutor interface {
	// RegisterPlugin 使用插件处理器映射中的处理器注册指定插件，
	// 并更新实际状态缓存。
	RegisterPlugin(ctx context.Context, socketPath string, UUID types.UID, pluginHandlers map[string]cache.PluginHandler, actualStateOfWorld ActualStateOfWorldUpdater) error

	// UnregisterPlugin 使用给定处理器映射中的处理器注销插件，
	// 并更新实际状态缓存。
	UnregisterPlugin(ctx context.Context, pluginInfo cache.PluginInfo, actualStateOfWorld ActualStateOfWorldUpdater) error
}

// NewOperationExecutor 创建 OperationExecutor 实例。
func NewOperationExecutor(
	operationGenerator OperationGenerator) OperationExecutor {

	return &operationExecutor{
		// 同一 socket 路径的操作串行化，避免并发注册/注销。
		pendingOperations:  goroutinemap.NewGoRoutineMap(true /* exponentialBackOffOnError */),
		operationGenerator: operationGenerator,
	}
}

// ActualStateOfWorldUpdater 定义在注册/注销成功后更新实际状态缓存的操作。
type ActualStateOfWorldUpdater interface {
	// AddPlugin 在缓存中没有相同 socket 路径的插件时添加给定插件。
	// socketPath 为空时返回错误。
	AddPlugin(ctx context.Context, pluginInfo cache.PluginInfo) error

	// RemovePlugin 从实际状态缓存中删除指定 socket 路径的插件。
	// 插件不存在时为空操作。
	RemovePlugin(socketPath string)
}

type operationExecutor struct {
	// pendingOperations 跟踪进行中的操作，确保同一对象上不会并发执行多个操作。
	pendingOperations goroutinemap.GoRoutineMap

	// operationGenerator 提供生成具体操作函数的实现。
	operationGenerator OperationGenerator
}

// 编译期断言：operationExecutor 实现了 OperationExecutor 接口。
var _ OperationExecutor = &operationExecutor{}

// IsOperationPending 返回指定 socket 路径上是否有操作尚未完成。
func (oe *operationExecutor) IsOperationPending(socketPath string) bool {
	return oe.pendingOperations.IsOperationPending(socketPath)
}

// RegisterPlugin 生成并异步执行插件注册操作。
func (oe *operationExecutor) RegisterPlugin(
	ctx context.Context,
	socketPath string,
	pluginUUID types.UID,
	pluginHandlers map[string]cache.PluginHandler,
	actualStateOfWorld ActualStateOfWorldUpdater) error {
	generatedOperation :=
		oe.operationGenerator.GenerateRegisterPluginFunc(ctx, socketPath, pluginUUID, pluginHandlers, actualStateOfWorld)

	return oe.pendingOperations.Run(
		socketPath, generatedOperation)
}

// UnregisterPlugin 生成并异步执行插件注销操作。
func (oe *operationExecutor) UnregisterPlugin(
	ctx context.Context,
	pluginInfo cache.PluginInfo,
	actualStateOfWorld ActualStateOfWorldUpdater) error {
	generatedOperation :=
		oe.operationGenerator.GenerateUnregisterPluginFunc(ctx, pluginInfo, actualStateOfWorld)

	return oe.pendingOperations.Run(
		pluginInfo.SocketPath, generatedOperation)
}
