/*
Copyright 2024 The Kubernetes Authors.

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

package external

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1apihelper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions/apiregistration/v1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers"
	availabilitymetrics "k8s.io/kube-aggregator/pkg/controllers/status/metrics"
)

// AvailableConditionController 检查已注册本地 API Service 的可用性。
//
// 中文补充：local 指 Spec.Service 为空、请求由 delegate 直接处理的 APIService。
// 它们不做网络探测，因此这里只负责把 Available 条件维持为真，
// 与远端可用性控制器形成互补：后者才需要真正探活。
type AvailableConditionController struct {
	apiServiceClient apiregistrationclient.APIServicesGetter

	apiServiceLister listers.APIServiceLister
	apiServiceSynced cache.InformerSynced

	// 便于在测试中注入。
	syncFn func(key string) error

	queue workqueue.TypedRateLimitingInterface[string]

	// 注册到 legacy registry 的指标。
	metrics *availabilitymetrics.Metrics
}

// New 创建一个新的本地可用性 AvailableConditionController。
//
// 中文补充：resync 周期设得比一般控制器短，是因为本地路由是否可用受外部网络可达性影响，
// 仅靠 watch APIService 变更发现不了这类退化；定时重读状态能更快恢复。
// 见注释里链接的议题。
func New(
	apiServiceInformer informers.APIServiceInformer,
	apiServiceClient apiregistrationclient.APIServicesGetter,
	metrics *availabilitymetrics.Metrics,
) (*AvailableConditionController, error) {
	c := &AvailableConditionController{
		apiServiceClient: apiServiceClient,
		apiServiceLister: apiServiceInformer.Lister(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			// 这里希望重入队间隔较短：控制器虽然监听 API，但可用性还取决于 Service 网络的可达性，
			// 而这类外部因素无法通过 watch 感知。较短间隔能把中断时间压到最低，同时仍避免忙等。
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](5*time.Millisecond, 30*time.Second),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "LocalAvailabilityController"},
		),
		metrics: metrics,
	}

	// 对本地控制器做 resync：数量少，开销可接受；到节点的网络断开但仍尝试路由时，
	// 重新检查实际发现文档能更快发现健康问题。见
	// https://github.com/openshift/origin/issues/17159#issuecomment-341798063
	// 中文补充：local APIService 数量很少，所以定时全量 resync 的开销可接受；
	// 用 AddEventHandlerWithResyncPeriod 返回的 Registration 取 HasSynced，
	// 是为了确认 handler 真的被调过，而不只是 informer 的 store 填满了。
	apiServiceHandler, _ := apiServiceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addAPIService,
			UpdateFunc: c.updateAPIService,
			DeleteFunc: c.deleteAPIService,
		},
		30*time.Second)
	c.apiServiceSynced = apiServiceHandler.HasSynced

	c.syncFn = c.sync

	return c, nil
}

func (c *AvailableConditionController) sync(key string) error {
	// 中文补充：删除事件也会入队，对象此时可能已经不存在；
	// Forget 对应 metric 的清理，避免出现"对象没了但指标还在"的幽灵数据。
	originalAPIService, err := c.apiServiceLister.Get(key)
	if apierrors.IsNotFound(err) {
		c.metrics.ForgetAPIService(key)
		return nil
	}
	if err != nil {
		return err
	}

	if originalAPIService.Spec.Service != nil {
		// 本控制器只处理本地 APIService。
		return nil
	}

	// 本地 APIService 始终视为可用。
	// 中文补充：只在副本上改状态，lister 缓存里的对象不能被写；
	// 而且 UpdateStatus 前后要拿原始对象比较，副本正是为了保留这个对比基准。
	apiService := originalAPIService.DeepCopy()
	apiregistrationv1apihelper.SetAPIServiceCondition(apiService, apiregistrationv1apihelper.NewLocalAvailableAPIServiceCondition())
	_, err = c.updateAPIServiceStatus(originalAPIService, apiService)
	return err
}

// updateAPIServiceStatus 只在检测到变化时提交更新。由于 resync 周期短、要快速发现失效的
// APIService，这里不能频繁提交无变化的更新。
func (c *AvailableConditionController) updateAPIServiceStatus(originalAPIService, newAPIService *apiregistrationv1.APIService) (*apiregistrationv1.APIService, error) {
	// 每次同步都更新该指标，以反映实际状态。
	c.metrics.SetUnavailableGauge(newAPIService)

	if equality.Semantic.DeepEqual(originalAPIService.Status, newAPIService.Status) {
		return newAPIService, nil
	}

	orig := apiregistrationv1apihelper.GetAPIServiceConditionByType(originalAPIService, apiregistrationv1.Available)
	now := apiregistrationv1apihelper.GetAPIServiceConditionByType(newAPIService, apiregistrationv1.Available)
	// 中文补充：缺失的 Available 条件按 Unknown 处理，这样和显式的 Unknown 状态
	// 在日志里呈现一致；真正决定是否写回还是靠上面的 DeepEqual。
	unknown := apiregistrationv1.APIServiceCondition{
		Type:   apiregistrationv1.Available,
		Status: apiregistrationv1.ConditionUnknown,
	}
	if orig == nil {
		orig = &unknown
	}
	if now == nil {
		now = &unknown
	}
	if *orig != *now {
		klog.V(2).InfoS("changing APIService availability", "name", newAPIService.Name, "oldStatus", orig.Status, "newStatus", now.Status, "message", now.Message, "reason", now.Reason)
	}

	newAPIService, err := c.apiServiceClient.APIServices().UpdateStatus(context.TODO(), newAPIService, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	c.metrics.SetUnavailableCounter(originalAPIService, newAPIService)
	return newAPIService, nil
}

// Run 启动 AvailableConditionController 循环，管理 API Service 的可用性条件。
//
// 中文补充：多个 worker 是安全的，因为同一 key 会被 workqueue 去重且串行处理；
// 增加 worker 只是为了不因某个慢请求拖住其他 APIService。
func (c *AvailableConditionController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting LocalAvailability controller")
	defer klog.Info("Shutting down LocalAvailability controller")

	// 这里不只等 informer 同步，还等 handler 真正被调用；三个 handler 都是把同一对象入队，
	// 等它们都触发后队列才能尽量去重。
	if !controllers.WaitForCacheSync("LocalAvailability", stopCh, c.apiServiceSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *AvailableConditionController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
func (c *AvailableConditionController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncFn(key)
	if err == nil {
		// 中文补充：成功后清掉失败次数，退避重新归零，避免偶发抖动留下长尾延迟。
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %w", key, err))
	c.queue.AddRateLimited(key)

	return true
}

func (c *AvailableConditionController) addAPIService(obj interface{}) {
	castObj := obj.(*apiregistrationv1.APIService)
	klog.V(4).Infof("Adding %s", castObj.Name)
	c.queue.Add(castObj.Name)
}

func (c *AvailableConditionController) updateAPIService(oldObj, _ interface{}) {
	oldCastObj := oldObj.(*apiregistrationv1.APIService)
	klog.V(4).Infof("Updating %s", oldCastObj.Name)
	c.queue.Add(oldCastObj.Name)
}

func (c *AvailableConditionController) deleteAPIService(obj interface{}) {
	// 中文补充：watch 的删除事件可能给 DeletedFinalStateUnknown 墓碑而不是原对象，
	// 因为 informer 可能错过了DELETE；再解一层才能拿到名字，否则这里的入队会漏掉。
	castObj, ok := obj.(*apiregistrationv1.APIService)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*apiregistrationv1.APIService)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting %q", castObj.Name)
	c.queue.Add(castObj.Name)
}
