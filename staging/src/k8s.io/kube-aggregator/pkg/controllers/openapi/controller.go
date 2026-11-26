/*
Copyright 2016 The Kubernetes Authors.

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

package openapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
)

const (
	// 成功同步后一分钟再轮询一次，感知聚合服务侧 spec 的变化；
	// 失败时从 successfulUpdateDelay 起指数退避，上限一小时，避免故障服务导致热循环。
	successfulUpdateDelay      = time.Minute
	successfulUpdateDelayLocal = time.Second
	failedUpdateMaxExpDelay    = time.Hour
)

// syncAction 把一次同步的结果与“下一步怎么处理”绑定在一起，
// 让 sync 只负责判断状态、processNextWorkItem 只负责执行队列动作，职责分离。
type syncAction int

// syncRequeue 表示同步成功后按固定间隔再查一次，感知远端 spec 变化。
// syncRequeueRateLimited 表示失败后交给限速队列退避重试。
// syncNothing 表示无需再处理，例如服务不再提供 spec，此时清掉失败计数。
const (
	syncRequeue syncAction = iota
	syncRequeueRateLimited
	syncNothing
)

// AggregationController 定期检查 APIService 的 OpenAPI spec 变化，
// 并在必要时更新或删除它们。
// 用定时轮询而非事件驱动，是因为远端 APIService 的 spec 可能在聚合层之外发生变化，
// 没有本地资源事件可以监听；队列负责合并事件与退避重试。
type AggregationController struct {
	openAPIAggregationManager aggregator.SpecAggregator
	queue                     workqueue.TypedRateLimitingInterface[string]
	downloader                *aggregator.Downloader

	// 允许为测试注入。
	// 把同步逻辑抽成函数字段，测试可直接替换，无需构造真实的 aggregator。
	syncHandler func(key string) (syncAction, error)
}

// NewAggregationController 创建一个新的 OpenAPI 聚合控制器。
// 限速队列的退避从成功轮询间隔起、到上限一小时为止：
// 这样失败重试既不会比正常轮询更频繁，也能在长时间故障时逐步降低请求压力。
func NewAggregationController(downloader *aggregator.Downloader, openAPIAggregationManager aggregator.SpecAggregator) *AggregationController {
	c := &AggregationController{
		openAPIAggregationManager: openAPIAggregationManager,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](successfulUpdateDelay, failedUpdateMaxExpDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "open_api_aggregation_controller"},
		),
		downloader: downloader,
	}

	c.syncHandler = c.sync

	return c
}

// Run 是启动控制器的旧式包装。
//
//logcheck:context // RunWithContext should be used instead of Run in code which supports contextual logging.
func (c *AggregationController) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 启动 OpenAPI 聚合控制器并阻塞到 context 取消。
func (c *AggregationController) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting OpenAPI AggregationController")
	defer logger.Info("Shutting down OpenAPI AggregationController")

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)

	<-ctx.Done()
}

func (c *AggregationController) runWorker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for c.processNextWorkItem(logger) {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
// 统一的队列处理骨架：syncHandler 给出动作，这里负责落地；
// 错误只记录日志，具体重试策略由 syncAction 决定，避免两处重复判断。
func (c *AggregationController) processNextWorkItem(logger klog.Logger) bool {
	key, quit := c.queue.Get()
	defer c.queue.Done(key)
	if quit {
		return false
	}

	logger.V(4).Info("OpenAPI AggregationController: Processing item", "key", key)

	action, err := c.syncHandler(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("loading OpenAPI spec for %q failed with: %v", key, err))
	}

	switch action {
	case syncRequeue:
		c.queue.AddAfter(key, successfulUpdateDelay)
	case syncRequeueRateLimited:
		logger.Info("OpenAPI AggregationController: action for item: Rate Limited Requeue", "key", key)
		c.queue.AddRateLimited(key)
	case syncNothing:
		c.queue.Forget(key)
	}

	return true
}

// sync 把一次 spec 更新的结果归类成队列动作。
// 服务不再提供 spec（ErrAPIServiceNotFound）是正常终态，不再重试；
// 其他错误都视为暂时性故障，交给限速队列退避重试。
func (c *AggregationController) sync(key string) (syncAction, error) {
	if err := c.openAPIAggregationManager.UpdateAPIServiceSpec(key); err != nil {
		if err == aggregator.ErrAPIServiceNotFound {
			return syncNothing, nil
		} else {
			return syncRequeueRateLimited, err
		}
	}
	return syncRequeue, nil
}

// AddAPIService 向 OpenAPI 聚合添加新的 API Service。
// 本地 APIService（Service 为 nil）由聚合层自身提供，不需要下载 spec，直接跳过；
// 远端的一个在 1 秒后入队，尽快拉取首次 spec。
func (c *AggregationController) AddAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	if err := c.openAPIAggregationManager.AddUpdateAPIService(apiService, handler); err != nil {
		utilruntime.HandleError(fmt.Errorf("adding %q to AggregationController failed with: %v", apiService.Name, err))
	}
	c.queue.AddAfter(apiService.Name, time.Second)
}

// UpdateAPIService 更新 APIService 的信息与 handler。
func (c *AggregationController) UpdateAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	if err := c.openAPIAggregationManager.UpdateAPIServiceSpec(apiService.Name); err != nil {
		utilruntime.HandleError(fmt.Errorf("Error updating APIService %q with err: %v", apiService.Name, err))
	}
	key := apiService.Name
	if c.queue.NumRequeues(key) > 0 {
		// 该条目之前失败过：把它移出失败队列，并在 1 秒后更新它。
		// 之前失败过：清掉失败计数并立即安排一次重试，
		// 让 handler 变更尽快生效，而不是继续等待旧的退避时间。
		c.queue.Forget(key)
		c.queue.AddAfter(key, time.Second)
	}
	// 否则：该条目之前是成功的，很快就会被更新（successfulUpdateDelay 之后）；
	// 这里不再重复添加，否则会造成条目重复。
	// 之前成功过的话队列里已有定时轮询任务，这里不再追加，避免同一个 key 重复排队。
}

// RemoveAPIService 从 OpenAPI 聚合控制器中移除 API Service。
// Forget 只清掉该 key 的失败记录：若它之前在重试队列中，这次移除也就取消了重试；
// 若之前是成功的正常轮询，processNextWorkItem 会发现对象已不存在而不再重新入队。
func (c *AggregationController) RemoveAPIService(apiServiceName string) {
	c.openAPIAggregationManager.RemoveAPIService(apiServiceName)
	// 只有之前失败的项才会在这里移除；成功同步的项 processNextWorkItem 能自行识别，不会再次入队。
	c.queue.Forget(apiServiceName)
}
