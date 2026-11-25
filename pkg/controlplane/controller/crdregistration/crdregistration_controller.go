/*
Copyright 2017 The Kubernetes Authors.

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

package crdregistration

import (
	"fmt"
	"time"

	"k8s.io/klog/v2"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	crdlisters "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// AutoAPIServiceRegistration 是调用方可在本地重新声明并正确转换的接口，
// 用于添加和移除 APIService。
type AutoAPIServiceRegistration interface {
	// AddAPIServiceToSync 添加待自动注册的 APIService。
	AddAPIServiceToSync(in *v1.APIService)
	// RemoveAPIServiceToSync 移除待自动注册的 APIService。
	RemoveAPIServiceToSync(name string)
}

// crdRegistrationController 监听 CRD 变更并同步对应的 APIService。
type crdRegistrationController struct {
	crdLister crdlisters.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced

	apiServiceRegistration AutoAPIServiceRegistration

	syncHandler func(groupVersion schema.GroupVersion) error

	syncedInitialSet chan struct{}

	// 工作队列用于去重，并支持出错时方便的限流重试。
	// 队列实际以 groupVersion 为 key。
	queue workqueue.TypedRateLimitingInterface[schema.GroupVersion]
}

// NewCRDRegistrationController 创建一个控制器，把 CRD 的组版本注册到自动 APIService 注册器，
// 使两者自动保持同步。
func NewCRDRegistrationController(crdinformer crdinformers.CustomResourceDefinitionInformer, apiServiceRegistration AutoAPIServiceRegistration) *crdRegistrationController {
	c := &crdRegistrationController{
		crdLister:              crdinformer.Lister(),
		crdSynced:              crdinformer.Informer().HasSynced,
		apiServiceRegistration: apiServiceRegistration,
		syncedInitialSet:       make(chan struct{}),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[schema.GroupVersion](),
			workqueue.TypedRateLimitingQueueConfig[schema.GroupVersion]{Name: "crd_autoregistration_controller"},
		),
	}
	c.syncHandler = c.handleVersionUpdate

	crdinformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			cast := obj.(*apiextensionsv1.CustomResourceDefinition)
			c.enqueueCRD(cast)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// 新旧对象都入队，确保正确移除与添加对应的 APIService。
			// 工作队列会去重，最终只保留有变化的部分。
			c.enqueueCRD(oldObj.(*apiextensionsv1.CustomResourceDefinition))
			c.enqueueCRD(newObj.(*apiextensionsv1.CustomResourceDefinition))
		},
		DeleteFunc: func(obj interface{}) {
			cast, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.V(2).Infof("Couldn't get object from tombstone %#v", obj)
					return
				}
				cast, ok = tombstone.Obj.(*apiextensionsv1.CustomResourceDefinition)
				if !ok {
					klog.V(2).Infof("Tombstone contained unexpected object: %#v", obj)
					return
				}
			}
			c.enqueueCRD(cast)
		},
	})

	return c
}

// Run 启动控制器并处理初始 CRD 列表。
func (c *crdRegistrationController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	// 确保关闭工作队列，从而让 worker 退出。
	defer c.queue.ShutDown()

	klog.Infof("Starting crd-autoregister controller")
	defer klog.Infof("Shutting down crd-autoregister controller")

	// 开始工作前等待辅助缓存填充完毕。
	if !cache.WaitForNamedCacheSync("crd-autoregister", stopCh, c.crdSynced) {
		return
	}

	// 对列表中的每个元素处理一次。
	if crds, err := c.crdLister.List(labels.Everything()); err != nil {
		utilruntime.HandleError(err)
	} else {
		for _, crd := range crds {
			for _, version := range crd.Spec.Versions {
				if err := c.syncHandler(schema.GroupVersion{Group: crd.Spec.Group, Version: version.Name}); err != nil {
					utilruntime.HandleError(err)
				}
			}
		}
	}
	close(c.syncedInitialSet)

	// 根据 workers 数量启动工作线程。
	for range workers {
		// runWorker 循环工作直到出错，.Until 会在出错后一秒重启 worker。

		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// 等待停止信号。
	<-stopCh
}

// WaitForInitialSync 阻塞直到初始 CRD 集合处理完毕。
func (c *crdRegistrationController) WaitForInitialSync() {
	<-c.syncedInitialSet
}

// runWorker 持续处理工作队列中的 CRD。
func (c *crdRegistrationController) runWorker() {
	// 热循环直到收到停止信号。processNextWorkItem 会在无工作时自动等待，
	// 因此无需额外等待。
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key，退出时返回 false。
func (c *crdRegistrationController) processNextWorkItem() bool {
	// 从队列取出下一个工作项，该 key 用于在缓存中查找对象。
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// 必须向队列标记该工作项已处理完成。
	defer c.queue.Done(key)

	// 对该 key 执行实际处理逻辑。
	err := c.syncHandler(key)
	if err == nil {
		// 无错误时通知队列停止跟踪该 key 的历史，
		// 从而重置逐项限流的失败计数。
		c.queue.Forget(key)
		return true
	}

	// 出现失败必须上报，该方法支持可插拔的错误处理，
	// 例如用于集群监控。
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	// 失败后应重新入队稍后处理。该方法会加入退避，
	// 避免对特定项热循环（它们短期内可能仍无法成功），
	// 同时保护控制器整体（若所有操作都失败，需冷静下来，
	// 否则会饿死其他有用的工作）。
	c.queue.AddRateLimited(key)

	return true
}

// enqueueCRD 将 CRD 加入工作队列。
func (c *crdRegistrationController) enqueueCRD(crd *apiextensionsv1.CustomResourceDefinition) {
	for _, version := range crd.Spec.Versions {
		c.queue.Add(schema.GroupVersion{Group: crd.Spec.Group, Version: version.Name})
	}
}

// handleVersionUpdate 处理 CRD 版本变更，同步 APIService 注册。
func (c *crdRegistrationController) handleVersionUpdate(groupVersion schema.GroupVersion) error {
	apiServiceName := groupVersion.Version + "." + groupVersion.Group

	// 检查所有 CRD；数量通常不多，若后续有性能问题可加索引。
	crds, err := c.crdLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, crd := range crds {
		if crd.Spec.Group != groupVersion.Group {
			continue
		}
		for _, version := range crd.Spec.Versions {
			if version.Name != groupVersion.Version || !version.Served {
				continue
			}

			c.apiServiceRegistration.AddAPIServiceToSync(&v1.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: apiServiceName},
				Spec: v1.APIServiceSpec{
					Group:                groupVersion.Group,
					Version:              groupVersion.Version,
					GroupPriorityMinimum: 1000, // CRD 的组优先级应相对较低
					VersionPriority:      100,  // 与相同 VersionPriority 的其他 APIService 一样，按 Kubernetes 版本规则排序
				},
			})
			return nil
		}
	}

	c.apiServiceRegistration.RemoveAPIServiceToSync(apiServiceName)
	return nil
}
