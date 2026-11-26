/*
Copyright 2021 The Kubernetes Authors.

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

package openapiv3

import (
	"context"
	"fmt"
	"net/http"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
)

const (
	// 中文补充：远端规范的拉取比较贵，成功后果断拉长到分钟级；
	// 本地 delegate 的规范基本是内存操作，一秒一次也无所谓，能更快跟上变化。
	// 失败时按指数退避，上限一小时，避免后端长时间不可用时无限重试打爆它。
	successfulUpdateDelay      = time.Minute
	successfulUpdateDelayLocal = time.Second
	failedUpdateMaxExpDelay    = time.Hour
)

// 中文补充：把"下一次该怎么调度"作为 sync 的返回值而不是让 sync 自己操作队列，
// 好处是决策逻辑可测试，队列操作集中在 processNextWorkItem 一处。
type syncAction int

const (
	// 中文补充：三种动作分别对应"按固定间隔再试""按退避再试""对象没了不用再试"。
	syncRequeue syncAction = iota
	syncRequeueRateLimited
	syncNothing
)

// AggregationController 定期检查每个 APIService 处理的 group/version 列表，并更新发现页面。
//
// 中文补充：OpenAPI v3 规范是周期性拉取而不是 watch 驱动的——APIService 的变化
// 只能说明"该重新拉了"，并不能告诉你远端规范的内容；内容只能靠定时同步获得。
// syncHandler 做成字段是为了在单测里替换掉真实的下载逻辑。
type AggregationController struct {
	openAPIAggregationManager aggregator.SpecProxier
	queue                     workqueue.TypedRateLimitingInterface[string]

	// 便于在测试中注入。
	syncHandler func(key string) (syncAction, error)
}

// NewAggregationController 创建一个新的 OpenAPI 聚合控制器。
func NewAggregationController(openAPIAggregationManager aggregator.SpecProxier) *AggregationController {
	c := &AggregationController{
		openAPIAggregationManager: openAPIAggregationManager,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](successfulUpdateDelay, failedUpdateMaxExpDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "open_api_v3_aggregation_controller"},
		),
	}

	c.syncHandler = c.sync

	// 每个 Service 至少更新一次，包括不来自 APIService 的本地服务。
	// 中文补充：local delegate 不会触发 AddAPIService，所以启动时主动入队一次，
	// 否则这部分规范永远不会被拉取；延后一秒是给 generic server 的初始化留点时间。
	for _, name := range openAPIAggregationManager.GetAPIServiceNames() {
		c.queue.AddAfter(name, time.Second)
	}

	return c
}

// Run 是启动控制器的旧式包装。
//
//logcheck:context // RunWithContext should be used instead of Run in code which supports contextual logging.
func (c *AggregationController) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 启动 OpenAPI v3 聚合控制器并阻塞到 context 取消。
func (c *AggregationController) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting OpenAPI V3 AggregationController")
	defer logger.Info("Shutting down OpenAPI V3 AggregationController")

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)

	<-ctx.Done()
}

func (c *AggregationController) runWorker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for c.processNextWorkItem(logger) {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
func (c *AggregationController) processNextWorkItem(logger klog.Logger) bool {
	key, quit := c.queue.Get()
	defer c.queue.Done(key)
	if quit {
		return false
	}

	if aggregator.IsLocalAPIService(key) {
		// 本地委托目标每秒聚合一次，提高日志级别以免刷屏。
		logger.V(6).Info("OpenAPI AggregationController: Processing item", "key", key)
	} else {
		logger.V(4).Info("OpenAPI AggregationController: Processing item", "key", key)
	}

	action, err := c.syncHandler(key)
	if err == nil {
		// 中文补充：成功后清掉失败计数，下一次退避重新从最小值开始；
		// 否则偶发失败会让这个 key 长期停留在很长的退避间隔上。
		c.queue.Forget(key)
	} else {
		utilruntime.HandleError(fmt.Errorf("loading OpenAPI spec for %q failed with: %v", key, err))
	}

	switch action {
	case syncRequeue:
		if aggregator.IsLocalAPIService(key) {
			logger.V(7).Info("OpenAPI AggregationController: action for local item: Requeue", "key", key, "duration", successfulUpdateDelayLocal)
			c.queue.AddAfter(key, successfulUpdateDelayLocal)
		} else {
			logger.V(7).Info("OpenAPI AggregationController: action for item: Requeue", "key", key)
			c.queue.AddAfter(key, successfulUpdateDelay)
		}
	case syncRequeueRateLimited:
		logger.Info("OpenAPI AggregationController: action for item: Rate Limited Requeue", "key", key)
		c.queue.AddRateLimited(key)
	case syncNothing:
		logger.Info("OpenAPI AggregationController: action for item: Nothing (removed from the queue)", "key", key)
	}

	return true
}

func (c *AggregationController) sync(key string) (syncAction, error) {
	// 中文补充：ErrAPIServiceNotFound 说明对象已被删除，属于正常收敛路径，
	// 不当成错误上报也不再入队；其他错误才走退避重试。
	if err := c.openAPIAggregationManager.UpdateAPIServiceSpec(key); err != nil {
		if err == aggregator.ErrAPIServiceNotFound {
			return syncNothing, nil
		}
		return syncRequeueRateLimited, err
	}
	return syncRequeue, nil
}

// AddAPIService 向 OpenAPI 聚合中新增 APIService。
//
// 中文补充：local APIService 没有可拉取的远端规范，启动时已在构造函数里统一入队，
// 这里再入队只会产生重复项，因此直接跳过。
func (c *AggregationController) AddAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	c.openAPIAggregationManager.AddUpdateAPIService(handler, apiService)
	c.queue.AddAfter(apiService.Name, time.Second)
}

// UpdateAPIService 更新 APIService 的信息与 handler。
func (c *AggregationController) UpdateAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	c.openAPIAggregationManager.AddUpdateAPIService(handler, apiService)
	key := apiService.Name
	if c.queue.NumRequeues(key) > 0 {
		// 该项之前失败过，先从失败队列移除，1 秒后重试。
		// 中文补充：配置刚变更过，之前失败的原因很可能已经消失，
		// 没必要让它继续按旧的长退避等待；清掉失败记录，一秒后立刻重试。
		c.queue.Forget(key)
		c.queue.AddAfter(key, time.Second)
	}
}

// RemoveAPIService 从 OpenAPI 聚合控制器中移除 APIService。
func (c *AggregationController) RemoveAPIService(apiServiceName string) {
	c.openAPIAggregationManager.RemoveAPIServiceSpec(apiServiceName)
	// 只有之前失败的项才会在这里移除；成功同步的项 processNextWorkItem 能自行识别，不会再次入队。
	// 中文补充： Forget 只清失败计数，不会撤销已经 AddAfter 的定时项；
	// 那一项到点后 sync 会拿到 ErrAPIServiceNotFound 并自然收敛。
	c.queue.Forget(apiServiceName)
}
