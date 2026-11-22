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

package apiserver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	autoscaling "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

// DiscoveryController 依据 CRD 的变化，把自定义资源的发现信息
// 同步到传统 discovery 和聚合 discovery 的处理器。
type DiscoveryController struct {
	versionHandler  *versionDiscoveryHandler
	groupHandler    *groupDiscoveryHandler
	resourceManager discoveryendpoint.ResourceManager

	crdLister  listers.CustomResourceDefinitionLister
	crdsSynced cache.InformerSynced

	// 允许测试时注入，替换为真实的同步函数。
	syncFn func(version schema.GroupVersion) error

	queue workqueue.TypedRateLimitingInterface[schema.GroupVersion]
}

// NewDiscoveryController 创建发现控制器，并注册 CRD 的增删改事件处理。
func NewDiscoveryController(
	crdInformer informers.CustomResourceDefinitionInformer,
	versionHandler *versionDiscoveryHandler,
	groupHandler *groupDiscoveryHandler,
	resourceManager discoveryendpoint.ResourceManager,
) *DiscoveryController {
	c := &DiscoveryController{
		versionHandler:  versionHandler,
		groupHandler:    groupHandler,
		resourceManager: resourceManager,
		crdLister:       crdInformer.Lister(),
		crdsSynced:      crdInformer.Informer().HasSynced,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[schema.GroupVersion](),
			workqueue.TypedRateLimitingQueueConfig[schema.GroupVersion]{Name: "DiscoveryController"},
		),
	}

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
		DeleteFunc: c.deleteCustomResourceDefinition,
	})

	c.syncFn = c.sync

	return c
}

// sync 针对指定的组版本，根据当前所有 CRD 重新计算并设置发现信息。
func (c *DiscoveryController) sync(version schema.GroupVersion) error {

	apiVersionsForDiscovery := []metav1.GroupVersionForDiscovery{}
	apiResourcesForDiscovery := []metav1.APIResource{}
	aggregatedAPIResourcesForDiscovery := []apidiscoveryv2.APIResourceDiscovery{}
	versionsForDiscoveryMap := map[metav1.GroupVersion]bool{}

	crds, err := c.crdLister.List(labels.Everything())
	if err != nil {
		return err
	}
	foundVersion := false
	foundGroup := false
	for _, crd := range crds {
		if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
			continue
		}

		if crd.Spec.Group != version.Group {
			continue
		}

		foundThisVersion := false
		var storageVersionHash string
		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}
			// 只要有任一 Served 版本，就说明该组应出现在 discovery 中。
			foundGroup = true

			gv := metav1.GroupVersion{Group: crd.Spec.Group, Version: v.Name}
			if !versionsForDiscoveryMap[gv] {
				versionsForDiscoveryMap[gv] = true
				apiVersionsForDiscovery = append(apiVersionsForDiscovery, metav1.GroupVersionForDiscovery{
					GroupVersion: crd.Spec.Group + "/" + v.Name,
					Version:      v.Name,
				})
			}
			if v.Name == version.Version {
				foundThisVersion = true
			}
			if v.Storage {
				storageVersionHash = discovery.StorageVersionHash(gv.Group, gv.Version, crd.Spec.Names.Kind)
			}
		}

		if !foundThisVersion {
			continue
		}
		foundVersion = true

		verbs := metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"})
		// 若 CRD 处于终止中，则不允许部分动词（写操作已关闭）。
		if apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Terminating) {
			verbs = metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "watch"})
		}

		apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
			Name:               crd.Status.AcceptedNames.Plural,
			SingularName:       crd.Status.AcceptedNames.Singular,
			Namespaced:         crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
			Kind:               crd.Status.AcceptedNames.Kind,
			Verbs:              verbs,
			ShortNames:         crd.Status.AcceptedNames.ShortNames,
			Categories:         crd.Status.AcceptedNames.Categories,
			StorageVersionHash: storageVersionHash,
		})

		subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, version.Version)
		if err != nil {
			return err
		}

		if c.resourceManager != nil {
			var scope apidiscoveryv2.ResourceScope
			if crd.Spec.Scope == apiextensionsv1.NamespaceScoped {
				scope = apidiscoveryv2.ScopeNamespace
			} else {
				scope = apidiscoveryv2.ScopeCluster
			}
			apiResourceDiscovery := apidiscoveryv2.APIResourceDiscovery{
				Resource:         crd.Status.AcceptedNames.Plural,
				SingularResource: crd.Status.AcceptedNames.Singular,
				Scope:            scope,
				ResponseKind: &metav1.GroupVersionKind{
					Group:   version.Group,
					Version: version.Version,
					Kind:    crd.Status.AcceptedNames.Kind,
				},
				Verbs:      verbs,
				ShortNames: crd.Status.AcceptedNames.ShortNames,
				Categories: crd.Status.AcceptedNames.Categories,
			}
			if subresources != nil && subresources.Status != nil {
				apiResourceDiscovery.Subresources = append(apiResourceDiscovery.Subresources, apidiscoveryv2.APISubresourceDiscovery{
					Subresource: "status",
					ResponseKind: &metav1.GroupVersionKind{
						Group:   version.Group,
						Version: version.Version,
						Kind:    crd.Status.AcceptedNames.Kind,
					},
					Verbs: metav1.Verbs([]string{"get", "patch", "update"}),
				})
			}
			if subresources != nil && subresources.Scale != nil {
				apiResourceDiscovery.Subresources = append(apiResourceDiscovery.Subresources, apidiscoveryv2.APISubresourceDiscovery{
					Subresource: "scale",
					ResponseKind: &metav1.GroupVersionKind{
						Group:   autoscaling.GroupName,
						Version: "v1",
						Kind:    "Scale",
					},
					Verbs: metav1.Verbs([]string{"get", "patch", "update"}),
				})

			}
			aggregatedAPIResourcesForDiscovery = append(aggregatedAPIResourcesForDiscovery, apiResourceDiscovery)
		}

		if subresources != nil && subresources.Status != nil {
			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Name:       crd.Status.AcceptedNames.Plural + "/status",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Kind:       crd.Status.AcceptedNames.Kind,
				Verbs:      metav1.Verbs([]string{"get", "patch", "update"}),
			})
		}

		if subresources != nil && subresources.Scale != nil {
			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Group:      autoscaling.GroupName,
				Version:    "v1",
				Kind:       "Scale",
				Name:       crd.Status.AcceptedNames.Plural + "/scale",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Verbs:      metav1.Verbs([]string{"get", "patch", "update"}),
			})
		}
	}

	if !foundGroup {
		c.groupHandler.unsetDiscovery(version.Group)
		c.versionHandler.unsetDiscovery(version)

		if c.resourceManager != nil {
			c.resourceManager.RemoveGroup(version.Group)
		}
		return nil
	}

	sortGroupDiscoveryByKubeAwareVersion(apiVersionsForDiscovery)

	apiGroup := metav1.APIGroup{
		Name:     version.Group,
		Versions: apiVersionsForDiscovery,
		// 排序后列表的首项即为该组的偏好版本。
		PreferredVersion: apiVersionsForDiscovery[0],
	}
	c.groupHandler.setDiscovery(version.Group, discovery.NewAPIGroupHandler(Codecs, apiGroup))

	if !foundVersion {
		c.versionHandler.unsetDiscovery(version)

		if c.resourceManager != nil {
			c.resourceManager.RemoveGroupVersion(metav1.GroupVersion{
				Group:   version.Group,
				Version: version.Version,
			})
		}
		return nil
	}
	c.versionHandler.setDiscovery(version, discovery.NewAPIVersionHandler(Codecs, version, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return apiResourcesForDiscovery
	})))

	sort.Slice(aggregatedAPIResourcesForDiscovery, func(i, j int) bool {
		return aggregatedAPIResourcesForDiscovery[i].Resource < aggregatedAPIResourcesForDiscovery[j].Resource
	})
	if c.resourceManager != nil {
		c.resourceManager.AddGroupVersion(version.Group, apidiscoveryv2.APIVersionDiscovery{
			Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			Version:   version.Version,
			Resources: aggregatedAPIResourcesForDiscovery,
		})
		// CRD 的默认优先级。
		c.resourceManager.SetGroupVersionPriority(metav1.GroupVersion(version), 1000, 100)
	}
	return nil
}

// sortGroupDiscoveryByKubeAwareVersion 按 Kubernetes 版本感知的规则对版本列表降序排序。
func sortGroupDiscoveryByKubeAwareVersion(gd []metav1.GroupVersionForDiscovery) {
	sort.Slice(gd, func(i, j int) bool {
		return version.CompareKubeAwareVersionStrings(gd[i].Version, gd[j].Version) > 0
	})
}

// Run 启动控制器：先完成初始全量同步，再进入单工作协程的处理循环，直至 stopCh 关闭。
func (c *DiscoveryController) Run(stopCh <-chan struct{}, synchedCh chan<- struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	defer klog.Info("Shutting down DiscoveryController")

	klog.Info("Starting DiscoveryController")

	if !cache.WaitForCacheSync(stopCh, c.crdsSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// 先做一次全量同步，确保初始时能提供完整的 discovery 结果。
	if err := wait.PollUntilContextCancel(context.Background(), time.Second, true, func(ctx context.Context) (bool, error) {
		crds, err := c.crdLister.List(labels.Everything())
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to initially list CRDs: %v", err))
			return false, nil
		}
		for _, crd := range crds {
			for _, v := range crd.Spec.Versions {
				gv := schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name}
				if err := c.sync(gv); err != nil {
					utilruntime.HandleError(fmt.Errorf("failed to initially sync CRD version %v: %v", gv, err))
					return false, nil
				}
			}
		}
		return true, nil
	}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			utilruntime.HandleError(fmt.Errorf("timed out waiting for initial discovery sync"))
			return
		}
		utilruntime.HandleError(fmt.Errorf("unexpected error: %w", err))
		return
	}
	close(synchedCh)

	// 该 API 变化缓慢，只启动一个工作协程即可，避免不必要的并发。
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

// runWorker 不断从队列取出并处理工作项，直到队列关闭。
func (c *DiscoveryController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个键；到退出时机时返回 false。
func (c *DiscoveryController) processNextWorkItem() bool {
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

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

// enqueue 把 CRD 的每个版本作为独立的同步键加入队列。
func (c *DiscoveryController) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	for _, v := range obj.Spec.Versions {
		c.queue.Add(schema.GroupVersion{Group: obj.Spec.Group, Version: v.Name})
	}
}

// addCustomResourceDefinition 处理 CRD 新增事件。
func (c *DiscoveryController) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding customresourcedefinition %s", castObj.Name)
	c.enqueue(castObj)
}

// updateCustomResourceDefinition 处理 CRD 更新事件。
func (c *DiscoveryController) updateCustomResourceDefinition(oldObj, newObj interface{}) {
	castNewObj := newObj.(*apiextensionsv1.CustomResourceDefinition)
	castOldObj := oldObj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating customresourcedefinition %s", castOldObj.Name)
	// 同时入队新旧对象，确保相关版本能被正确移除和添加。
	// 工作队列会消除重复项，只保留实际发生变化的条目。
	c.enqueue(castNewObj)
	c.enqueue(castOldObj)
}

// deleteCustomResourceDefinition 处理 CRD 删除事件，并兼容 tombstone 对象。
func (c *DiscoveryController) deleteCustomResourceDefinition(obj interface{}) {
	castObj, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
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
