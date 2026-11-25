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

package apiserver

import (
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions/apiregistration/v1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers"
)

// APIHandlerManager 定义了 API handler 应具备的行为。
// 把“如何把某个 GroupVersion 挂到代理路由上”抽象成接口，
// 让注册控制器只负责监听 APIService 变化，而不依赖具体的 handler 实现；
// 测试时也能用假实现替代真实的聚合 handler。
type APIHandlerManager interface {
	AddAPIService(apiService *v1.APIService) error
	RemoveAPIService(apiServiceName string)
}

// APIServiceRegistrationController 负责注册和移除 API services。
// 该控制器监听 APIService 资源，把变化同步到代理 handler 的路由表：
// 新增/更新时挂载路由，删除时摘除。用 workqueue 而不是在事件回调里直接改路由，
// 是为了借助队列的合并与限速能力，避免事件风暴时频繁改动共享的代理状态。
type APIServiceRegistrationController struct {
	apiHandlerManager APIHandlerManager

	apiServiceLister listers.APIServiceLister
	apiServiceSynced cache.InformerSynced

	// 允许为测试注入。
	// 把同步逻辑抽成函数字段，测试时可以直接替换，无需构造完整的 informer 与 handler。
	syncFn func(key string) error

	// 限速队列：合并短时间内同一 key 的重复事件，并在同步失败时按退避重试。
	// APIService 变动不频繁但改路由代价较高，合并能避免大量无效操作。
	queue workqueue.TypedRateLimitingInterface[string]
}

var _ dynamiccertificates.Listener = &APIServiceRegistrationController{}

// NewAPIServiceRegistrationController 返回一个新的 APIServiceRegistrationController。
// 构造函数把 informer 的 lister / HasSynced 与 handler 管理器绑在一起，并注册三类事件回调；
// 回调只做入队，真正的同步留给 worker，从而让事件处理保持轻量。
func NewAPIServiceRegistrationController(apiServiceInformer informers.APIServiceInformer, apiHandlerManager APIHandlerManager) *APIServiceRegistrationController {
	c := &APIServiceRegistrationController{
		apiHandlerManager: apiHandlerManager,
		apiServiceLister:  apiServiceInformer.Lister(),
		apiServiceSynced:  apiServiceInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "APIServiceRegistrationController"},
		),
	}

	apiServiceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addAPIService,
		UpdateFunc: c.updateAPIService,
		DeleteFunc: c.deleteAPIService,
	})

	c.syncFn = c.sync

	return c
}

// sync 把队列中的 key 翻译成对代理 handler 的一次更新。
// 以“对象是否还在”作为增删依据，而不是依赖事件类型：这样即使事件乱序或合并，
// 最终都能以当前状态为准，避免基于陈旧事件做出错误增删。
func (c *APIServiceRegistrationController) sync(key string) error {
	apiService, err := c.apiServiceLister.Get(key)
	if apierrors.IsNotFound(err) {
		c.apiHandlerManager.RemoveAPIService(key)
		return nil
	}
	if err != nil {
		return err
	}

	return c.apiHandlerManager.AddAPIService(apiService)
}

// Run starts APIServiceRegistrationController which will process all registration requests until stopCh is closed.
func (c *APIServiceRegistrationController) Run(stopCh <-chan struct{}, handlerSyncedCh chan<- struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting APIServiceRegistrationController")
	defer klog.Info("Shutting down APIServiceRegistrationController")

	if !controllers.WaitForCacheSync("APIServiceRegistrationController", stopCh, c.apiServiceSynced) {
		return
	}

	// 先对全部 APIService 做一次同步，确保代理 handler 是完整的。
	// 在开启 worker 前必须先全量同步一次：informer 的 add 事件只能保证缓存里有对象，
	// 不能保证代理 handler 已挂载全部路由。用 PollImmediateUntil 重试而不是一次失败就退出，
	// 是因为此时聚合层还没就绪，短暂失败（例如 handler 尚未初始化）是常态。
	if err := wait.PollImmediateUntil(time.Second, func() (bool, error) {
		services, err := c.apiServiceLister.List(labels.Everything())
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to initially list APIServices: %v", err))
			return false, nil
		}
		for _, s := range services {
			if err := c.apiHandlerManager.AddAPIService(s); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to initially sync APIService %s: %v", s.Name, err))
				return false, nil
			}
		}
		return true, nil
	}, stopCh); wait.Interrupted(err) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for proxy handler to initialize"))
		return
	} else if err != nil {
		panic(fmt.Errorf("unexpected error: %v", err))
	}
	close(handlerSyncedCh)

	// 只启动一个 worker 线程：这是低频变动的 API，而且聚合 server 的添加逻辑不是线程安全的。
	// 只起一个 worker：APIService 变动频率低，单线程足以处理；
	// 更重要的是代理 handler 的路由表不是线程安全的，并发写入会引发数据竞争。

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *APIServiceRegistrationController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
// 同步成功就 Forget 清掉失败计数；失败则 AddRateLimited 重新入队，
// 借助限速队列的指数退避避免持续失败时陷入热循环。
func (c *APIServiceRegistrationController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncFn(key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

// enqueueInternal 把对象转换为 namespace/name 形式的 key 后入队。
// APIService 是集群级资源，key 实际就是名字；用统一的 key 函数可以正确处理删除 tombstone。
func (c *APIServiceRegistrationController) enqueueInternal(obj *v1.APIService) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Couldn't get key for object %#v: %v", obj, err)
		return
	}

	c.queue.Add(key)
}

func (c *APIServiceRegistrationController) addAPIService(obj interface{}) {
	castObj := obj.(*v1.APIService)
	klog.V(4).Infof("Adding %s", castObj.Name)
	c.enqueueInternal(castObj)
}

func (c *APIServiceRegistrationController) updateAPIService(obj, _ interface{}) {
	castObj := obj.(*v1.APIService)
	klog.V(4).Infof("Updating %s", castObj.Name)
	c.enqueueInternal(castObj)
}

// deleteAPIService 处理删除事件。
// informer 在 watch 断开期间可能用 DeletedFinalStateUnknown（tombstone）投递删除，
// 因此必须先尝试从 tombstone 中取出原始对象；否则会漏掉删除，导致代理路由悬挂。
func (c *APIServiceRegistrationController) deleteAPIService(obj interface{}) {
	castObj, ok := obj.(*v1.APIService)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*v1.APIService)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting %q", castObj.Name)
	c.enqueueInternal(castObj)
}

// Enqueue 把所有 apiservices 入队以重新处理；
// 控制器用该方法在代理证书内容变化时收到通知。
// 代理证书变更会影响所有 APIService 的连接，所以这里把全部对象重新入队，
// 让每个 handler 用最新的 cert/key 重建传输层配置。
func (c *APIServiceRegistrationController) Enqueue() {
	apiServices, err := c.apiServiceLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	for _, apiService := range apiServices {
		c.addAPIService(apiService)
	}
}
