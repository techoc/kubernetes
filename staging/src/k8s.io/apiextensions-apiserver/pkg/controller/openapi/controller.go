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

package openapi

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/cached"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/validation/spec"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder"
)

// Controller 监听 CustomResourceDefinition 并发布其校验 schema。
type Controller struct {
	crdLister  listers.CustomResourceDefinitionLister
	crdsSynced cache.InformerSynced

	// 便于测试时注入自定义实现。
	syncFn func(string) error

	queue workqueue.TypedRateLimitingInterface[string]

	staticSpec *spec.Swagger

	openAPIService *handler.OpenAPIService

	// 按名称索引的 spec；spec 在请求时惰性构建。
	// 此处的锁只保护 map。
	lock        sync.Mutex
	specsByName map[string]*specCache
}

// specCache 保存某个 CRD 的合并版本 spec 及 CRD 对象。
// spec 在请求时由 CRD 对象惰性生成。
// mergedVersionSpec 仅在实例化时创建且不再变更；crdCache 是 cached.Replaceable，
// 更新是线程安全的。因此该结构无需加锁。
type specCache struct {
	crdCache          cached.LastSuccess[*apiextensionsv1.CustomResourceDefinition]
	mergedVersionSpec cached.Value[*spec.Swagger]
}

// update 用新的 CRD 对象（附哈希）替换缓存内容。
func (s *specCache) update(crd *apiextensionsv1.CustomResourceDefinition) {
	s.crdCache.Store(cached.Static(crd, generateCRDHash(crd)))
}

// createSpecCache 为 CRD 创建惰性合并 spec 缓存。
func createSpecCache(crd *apiextensionsv1.CustomResourceDefinition) *specCache {
	s := specCache{}
	s.update(crd)

	s.mergedVersionSpec = cached.Transform[*apiextensionsv1.CustomResourceDefinition](func(crd *apiextensionsv1.CustomResourceDefinition, etag string, err error) (*spec.Swagger, string, error) {
		if err != nil {
			// 理论上不会发生，但真发生时原样返回错误。
			return nil, "", err
		}
		mergeSpec := &spec.Swagger{}
		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}
			s, err := builder.BuildOpenAPIV2(crd, v.Name, builder.Options{
				V2:                      true,
				IncludeSelectableFields: utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors),
			})
			// 必须在此裁剪默认值，才能与已裁剪默认值的静态 spec 干净合并
			if err != nil {
				return nil, "", err
			}
			s.Definitions = handler.PruneDefaults(s.Definitions)
			mergeSpec, err = builder.MergeSpecs(mergeSpec, s)
			if err != nil {
				return nil, "", err
			}
		}
		return mergeSpec, generateCRDHash(crd), nil
	}, &s.crdCache)
	return &s
}

// NewController 使用给定的 CustomResourceDefinition informer 创建 Controller。
func NewController(crdInformer informers.CustomResourceDefinitionInformer) *Controller {
	c := &Controller{
		crdLister:  crdInformer.Lister(),
		crdsSynced: crdInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "crd_openapi_controller"},
		),
		specsByName: map[string]*specCache{},
	}

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
		DeleteFunc: c.deleteCustomResourceDefinition,
	})

	c.syncFn = c.sync
	return c
}

// Run 设置 openAPIAggregationManager 并启动 worker。
func (c *Controller) Run(staticSpec *spec.Swagger, openAPIService *handler.OpenAPIService, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	defer klog.Infof("Shutting down OpenAPI controller")

	klog.Infof("Starting OpenAPI controller")

	c.staticSpec = staticSpec
	c.openAPIService = openAPIService

	if !cache.WaitForCacheSync(stopCh, c.crdsSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// 预先创建初始 spec，避免启动时为每个 CRD 各合并一次
	crds, err := c.crdLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to initially list all CRDs: %v", err))
		return
	}
	for _, crd := range crds {
		// 只有已 Established 的 CRD 才对外提供 API
		if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
			continue
		}
		c.specsByName[crd.Name] = createSpecCache(crd)
	}
	c.updateSpecLocked()

	// 该 API 变更不频繁，只启动一个 worker
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

// runWorker 持续处理队列中的工作项，直到队列关闭。
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
func (c *Controller) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	// 记录耗时过长的聚合操作
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed > time.Second {
			klog.Warningf("slow openapi aggregation of %q: %s", key, elapsed)
		}
	}()

	err := c.syncFn(key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	c.queue.AddRateLimited(key)
	return true
}

// sync 协调单个 CRD 对应的 OpenAPI spec 缓存。
func (c *Controller) sync(name string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	crd, err := c.crdLister.Get(name)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// 是否需要移除该 CRD 的所有 spec？
	if errors.IsNotFound(err) || !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
		if _, found := c.specsByName[name]; !found {
			return nil
		}
		delete(c.specsByName, name)
		klog.V(2).Infof("Updating CRD OpenAPI spec because %s was removed", name)
		regenerationCounter.With(map[string]string{"crd": name, "reason": "remove"})
		c.updateSpecLocked()
		return nil
	}

	// 若已存在该 CRD 的 spec，则更新它。
	// specCache.update() 附带 ETag，更新后 ETag 不变则被视为 noop。
	s, exists := c.specsByName[crd.Name]
	if exists {
		s.update(crd)
		klog.V(2).Infof("Updating CRD OpenAPI spec because %s changed", name)
		regenerationCounter.With(map[string]string{"crd": name, "reason": "update"})
		return nil
	}

	c.specsByName[crd.Name] = createSpecCache(crd)
	klog.V(2).Infof("Updating CRD OpenAPI spec because %s changed", name)
	regenerationCounter.With(map[string]string{"crd": name, "reason": "add"})
	c.updateSpecLocked()
	return nil
}

// updateSpecLocked 更新缓存的 spec 图。
func (c *Controller) updateSpecLocked() {
	specList := make([]cached.Value[*spec.Swagger], 0, len(c.specsByName))
	for crd := range c.specsByName {
		specList = append(specList, c.specsByName[crd].mergedVersionSpec)
	}

	cache := cached.MergeList(func(results []cached.Result[*spec.Swagger]) (*spec.Swagger, string, error) {
		localCRDSpec := make([]*spec.Swagger, 0, len(results))
		for k := range results {
			if results[k].Err == nil {
				localCRDSpec = append(localCRDSpec, results[k].Value)
			}
		}
		mergedSpec, err := builder.MergeSpecs(c.staticSpec, localCRDSpec...)
		if err != nil {
			return nil, "", fmt.Errorf("failed to merge specs: %v", err)
		}
		// 返回 UUID 作为 etag，是因为只有 CRD 发生变化时才会重建 merger。
		// 基于哈希的 etag 开销更大，因为 CRD 尚未预先序列化。
		return mergedSpec, uuid.New().String(), nil
	}, specList)
	c.openAPIService.UpdateSpecLazy(cache)
}

// addCustomResourceDefinition 处理 CRD 新增事件。
func (c *Controller) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding customresourcedefinition %s", castObj.Name)
	c.enqueue(castObj)
}

// updateCustomResourceDefinition 处理 CRD 更新事件，仅关心新对象。
func (c *Controller) updateCustomResourceDefinition(oldObj, newObj interface{}) {
	castNewObj := newObj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating customresourcedefinition %s", castNewObj.Name)
	c.enqueue(castNewObj)
}

// deleteCustomResourceDefinition 处理 CRD 删除事件，并兼容 tombstone 对象。
func (c *Controller) deleteCustomResourceDefinition(obj interface{}) {
	castObj, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		// 删除事件可能携带 DeletedFinalStateUnknown tombstone
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*apiextensionsv1.CustomResourceDefinition)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting customresourcedefinition %q", castObj.Name)
	c.enqueue(castObj)
}

// enqueue 将 CRD 名称加入工作队列。
func (c *Controller) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	c.queue.Add(obj.Name)
}

// generateCRDHash 生成由 UID 与 Generation 组成的缓存键，任一变化即视为新版本。
func generateCRDHash(crd *apiextensionsv1.CustomResourceDefinition) string {
	return fmt.Sprintf("%s,%d", crd.UID, crd.Generation)
}
