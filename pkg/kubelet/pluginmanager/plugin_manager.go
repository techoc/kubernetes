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

package pluginmanager

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/operationexecutor"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/reconciler"
)

// PluginManager 运行一组异步循环，计算需要注册/注销的插件并执行相应操作。
type PluginManager interface {
	// Run 启动插件管理器及其控制的全部异步循环。
	Run(ctx context.Context, sourcesReady config.SourcesReady, stopCh <-chan struct{})

	// AddHandler 为指定插件类型注册处理器。
	// 处理器会加入实际状态缓存，并在插件注册/注销时传给期望状态缓存使用。
	AddHandler(pluginType string, pluginHandler cache.PluginHandler)
}

const (
	// loopSleepDuration 是协调循环两次执行之间的等待时间。
	loopSleepDuration = 1 * time.Second
)

// NewPluginManager 返回 PluginManager 接口的具体实现。
func NewPluginManager(
	sockDir string,
	recorder record.EventRecorder) PluginManager {
	// 分别维护期望状态与实际状态缓存。
	asw := cache.NewActualStateOfWorld()
	dsw := cache.NewDesiredStateOfWorld()
	// 协调器负责把期望状态逐步收敛到实际状态。
	reconciler := reconciler.NewReconciler(
		operationexecutor.NewOperationExecutor(
			operationexecutor.NewOperationGenerator(
				recorder,
			),
		),
		loopSleepDuration,
		dsw,
		asw,
	)

	pm := &pluginManager{
		// 插件监听器负责发现 socket 并填充期望状态。
		desiredStateOfWorldPopulator: pluginwatcher.NewWatcher(
			sockDir,
			dsw,
		),
		reconciler:          reconciler,
		desiredStateOfWorld: dsw,
		actualStateOfWorld:  asw,
	}
	return pm
}

// pluginManager 是 PluginManager 接口的实现。
type pluginManager struct {
	// desiredStateOfWorldPopulator（插件监听器）运行异步周期循环，
	// 负责填充 desiredStateOfWorld。
	desiredStateOfWorldPopulator *pluginwatcher.Watcher

	// reconciler 运行异步周期循环，通过 operationExecutor 触发注册/注销操作，
	// 使 desiredStateOfWorld 与 actualStateOfWorld 保持一致。
	reconciler reconciler.Reconciler

	// actualStateOfWorld 记录管理器视角下的实际状态，即哪些插件已注册。
	// 协调器触发的注册/注销操作成功完成后会更新该结构。
	actualStateOfWorld cache.ActualStateOfWorld

	// desiredStateOfWorld 记录插件管理器视角下的期望状态，即应注册哪些插件。
	// 该结构由插件监听器填充。
	desiredStateOfWorld cache.DesiredStateOfWorld
}

// 编译期断言：pluginManager 实现了 PluginManager 接口。
var _ PluginManager = &pluginManager{}

// Run 启动插件发现与协调循环，并在 stopCh 关闭后退出。
func (pm *pluginManager) Run(ctx context.Context, sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	defer runtime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)

	// 监听器启动失败时直接退出，避免在无发现能力的情况下空转。
	if err := pm.desiredStateOfWorldPopulator.Start(ctx, stopCh); err != nil {
		logger.Error(err, "The desired_state_of_world populator (plugin watcher) starts failed!")
		return
	}

	logger.V(2).Info("The desired_state_of_world populator (plugin watcher) starts")

	logger.Info("Starting Kubelet Plugin Manager")
	// 协调循环在后台运行，直到上下文取消。
	go pm.reconciler.Run(ctx)

	// 注册插件管理器指标。
	metrics.Register(pm.actualStateOfWorld, pm.desiredStateOfWorld)
	// 阻塞直到收到停止信号。
	<-stopCh
	logger.Info("Shutting down Kubelet Plugin Manager")
}

// AddHandler 为指定插件类型注册处理器。
func (pm *pluginManager) AddHandler(pluginType string, handler cache.PluginHandler) {
	pm.reconciler.AddHandler(pluginType, handler)
}
