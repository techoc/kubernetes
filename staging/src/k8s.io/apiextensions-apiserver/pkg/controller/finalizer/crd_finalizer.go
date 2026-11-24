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

// Package finalizer 实现 CRD 删除时的 finalizer 控制器。
// 当 CRD 被删除时，控制器负责删除该 CRD 的所有 CR 实例，
// 然后移除 finalizer，允许 CRD 真正从 etcd 中删除。
// 避免遗留的 CR 数据在 CRD 删除后成为孤儿。
package finalizer

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

// OverlappingBuiltInResources 返回与 CRD 存储路径重叠的内置资源。
// 这些内置资源（如 APIService、CRD 本身）不应被 CRD finalizer 删除，
// 即使它们的存储路径与某个 CRD 的 CR 路径重叠。
func OverlappingBuiltInResources() map[schema.GroupResource]bool {
	return map[schema.GroupResource]bool{
		{Group: "apiregistration.k8s.io", Resource: "apiservices"}:             true,
		{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}: true,
	}
}

// CRDFinalizer 在 CRD 删除时，负责删除该 CRD 的所有 CR 实例。
// 流程：CRD 标记删除 → 列出所有 CR → 批量删除 → 移除 finalizer → CRD 真正删除
type CRDFinalizer struct {
	crdClient      client.CustomResourceDefinitionsGetter
	crClientGetter CRClientGetter

	crdLister listers.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced

	// 通过函数注入同步逻辑，便于单元测试替换。
	syncFn func(ctx context.Context, key string) error

	queue workqueue.TypedRateLimitingInterface[string]
}

// ListerCollectionDeleter 组合了列出和批量删除 CR 的能力。
type ListerCollectionDeleter interface {
	rest.Lister
	rest.CollectionDeleter
}

// CRClientGetter 根据 CRD 获取对应的 CR 列表和删除接口。
type CRClientGetter interface {
	// GetCustomResourceListerCollectionDeleter 根据 CRD 获取对应的自定义资源列表器和集合删除器。
	// 实现方可使用 CRD UID 区分同名但生命周期不同的资源定义。
	GetCustomResourceListerCollectionDeleter(crd *apiextensionsv1.CustomResourceDefinition) (ListerCollectionDeleter, error)
}

// NewCRDFinalizer 创建 CRD finalizer 控制器。
func NewCRDFinalizer(
	crdInformer informers.CustomResourceDefinitionInformer,
	crdClient client.CustomResourceDefinitionsGetter,
	crClientGetter CRClientGetter,
) *CRDFinalizer {
	c := &CRDFinalizer{
		crdClient:      crdClient,
		crdLister:      crdInformer.Lister(),
		crdSynced:      crdInformer.Informer().HasSynced,
		crClientGetter: crClientGetter,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "crd_finalizer"},
		),
	}

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
	})

	c.syncFn = c.sync

	return c
}

func (c *CRDFinalizer) sync(ctx context.Context, key string) error {
	cachedCRD, err := c.crdLister.Get(key)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	// CRD 尚未进入删除流程，或清理 finalizer 已不存在，无需处理。
	if cachedCRD.DeletionTimestamp.IsZero() || !apiextensionshelpers.CRDHasFinalizer(cachedCRD, apiextensionsv1.CustomResourceCleanupFinalizer) {
		return nil
	}

	crd := cachedCRD.DeepCopy()

	// 更新 Terminating 状态条件，告知客户端实例清理可能需要较长时间。
	apiextensionshelpers.SetCRDCondition(crd, apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Terminating,
		Status:  apiextensionsv1.ConditionTrue,
		Reason:  "InstanceDeletionInProgress",
		Message: "CustomResource deletion is in progress",
	})
	crd, err = c.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 对象可能已被删除或并发修改；后续事件或队列重试会再次触发处理。
		return nil
	}
	if err != nil {
		return err
	}

	// 现在可以开始删除 CR 实例。优先通过 REST API 删除，以确保正常执行 Admission 链路。
	// 该控制器掌握对应端点，能够保证 DeleteCollection 可用；CRD 尚未 Established 时无需删除实例。
	if OverlappingBuiltInResources()[schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}] {
		// 跳过实例删除并记录原因，随后移除 finalizer，使 CRD 可以继续完成删除。
		apiextensionshelpers.SetCRDCondition(crd, apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "OverlappingBuiltInResource",
			Message: "instances overlap with built-in resources in storage",
		})
	} else if apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
		cond, deleteErr := c.deleteInstances(ctx, crd)
		apiextensionshelpers.SetCRDCondition(crd, cond)
		if deleteErr != nil {
			if _, err = c.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{}); err != nil {
				utilruntime.HandleError(err)
			}
			return deleteErr
		}
	} else {
		apiextensionshelpers.SetCRDCondition(crd, apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "NeverEstablished",
			Message: "resource was never established",
		})
	}

	apiextensionshelpers.CRDRemoveFinalizer(crd, apiextensionsv1.CustomResourceCleanupFinalizer)
	_, err = c.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 对象可能已被删除或并发修改；后续事件或队列重试会再次触发处理。
		return nil
	}
	return err
}

func (c *CRDFinalizer) deleteInstances(ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition) (apiextensionsv1.CustomResourceDefinitionCondition, error) {
	// 现在开始删除 CR 实例。虽然使用 REST API 客户端更理想，但相同 URL 可能指向旧版 ThirdPartyResource，
	// 从而误删非当前 CRD 的资源；因此这里直接访问专用存储。由于存储由本控制器管理，可以使用 DeleteCollection。
	crClient, err := c.crClientGetter.GetCustomResourceListerCollectionDeleter(crd)
	if err != nil {
		err = fmt.Errorf("unable to find a custom resource client for %s.%s: %v", crd.Status.AcceptedNames.Plural, crd.Spec.Group, err)
		return apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "InstanceDeletionFailed",
			Message: fmt.Sprintf("could not list instances: %v", err),
		}, err
	}

	allResources, err := crClient.List(ctx, nil)
	if err != nil {
		return apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "InstanceDeletionFailed",
			Message: fmt.Sprintf("could not list instances: %v", err),
		}, err
	}

	deletedNamespaces := sets.String{}
	deleteErrors := []error{}
	for _, item := range allResources.(*unstructured.UnstructuredList).Items {
		metadata, err := meta.Accessor(&item)
		if err != nil {
			utilruntime.HandleError(err)
			continue
		}
		if deletedNamespaces.Has(metadata.GetNamespace()) {
			continue
		}
		// 同一命名空间只执行一次集合删除，避免重复发起相同请求。
		deletedNamespaces.Insert(metadata.GetNamespace())
		nsCtx := genericapirequest.WithNamespace(ctx, metadata.GetNamespace())
		if _, err := crClient.DeleteCollection(nsCtx, rest.ValidateAllObjectFunc, nil, nil); err != nil {
			deleteErrors = append(deleteErrors, err)
			continue
		}
	}
	if deleteError := utilerrors.NewAggregate(deleteErrors); deleteError != nil {
		return apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "InstanceDeletionFailed",
			Message: fmt.Sprintf("could not issue all deletes: %v", deleteError),
		}, deleteError
	}

	// 删除请求提交后，还需要等待所有 CR 实例真正从存储中消失；这里先使用简单轮询确认结果。
	// TODO：不同 API Server 的缓存未必已全部同步，过期实例仍可能继续创建 CR。
	// TODO：未来若有 API Server 状态上报机制，应检查所有实例是否已完成同步，再确认删除完成。
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		listObj, err := crClient.List(ctx, nil)
		if err != nil {
			return false, err
		}
		if len(listObj.(*unstructured.UnstructuredList).Items) == 0 {
			return true, nil
		}
		klog.FromContext(ctx).V(2).Info("Waiting for items to be removed", "resource", crd.Status.AcceptedNames.Plural, "group", crd.Spec.Group, "count", len(listObj.(*unstructured.UnstructuredList).Items))
		return false, nil
	})
	if err != nil {
		return apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Terminating,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "InstanceDeletionCheck",
			Message: fmt.Sprintf("could not confirm zero CustomResources remaining: %v", err),
		}, err
	}
	return apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Terminating,
		Status:  apiextensionsv1.ConditionFalse,
		Reason:  "InstanceDeletionCompleted",
		Message: "removed all instances",
	}, nil
}

// Run 是兼容旧调用方的包装方法，将 stopCh 转换为 Context 后启动控制器。
//
//logcheck:context // 支持上下文日志的代码应调用 RunWithContext，而不是兼容旧接口的 Run。
func (c *CRDFinalizer) Run(workers int, stopCh <-chan struct{}) {
	c.RunWithContext(workers, wait.ContextForChannel(stopCh))
}

// RunWithContext 启动指定数量的工作线程；等待 CRD Informer 缓存同步后开始处理队列，直到上下文取消。
func (c *CRDFinalizer) RunWithContext(workers int, ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting CRDFinalizer")
	defer logger.Info("Shutting down CRDFinalizer")

	if !cache.WaitForCacheSync(ctx.Done(), c.crdSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
}

// runWorker 持续从工作队列取出 CRD 键，并调用同步函数处理删除流程。
func (c *CRDFinalizer) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 从队列中取出并处理一个 CRD 键；队列关闭或需要退出时返回 false。
func (c *CRDFinalizer) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncFn(ctx, key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

// enqueue 根据 CRD 元数据生成队列键，并将待清理的 CRD 放入工作队列。
func (c *CRDFinalizer) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

// addCustomResourceDefinition 处理 CRD 新增事件，只将已标记删除且带有清理 finalizer 的对象入队。
func (c *CRDFinalizer) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	// 只将已进入删除流程的 CRD 入队。
	if !castObj.DeletionTimestamp.IsZero() && apiextensionshelpers.CRDHasFinalizer(castObj, apiextensionsv1.CustomResourceCleanupFinalizer) {
		c.enqueue(castObj)
	}
}

// updateCustomResourceDefinition 处理 CRD 更新事件，并判断是否需要重新执行实例清理。
func (c *CRDFinalizer) updateCustomResourceDefinition(oldObj, newObj interface{}) {
	oldCRD := oldObj.(*apiextensionsv1.CustomResourceDefinition)
	newCRD := newObj.(*apiextensionsv1.CustomResourceDefinition)
	// 只处理仍处于删除流程且清理 finalizer 尚未移除的 CRD。
	if newCRD.DeletionTimestamp.IsZero() || !apiextensionshelpers.CRDHasFinalizer(newCRD, apiextensionsv1.CustomResourceCleanupFinalizer) {
		return
	}

	// Informer 重新同步事件也重新入队，确保遗漏的清理任务能够恢复。
	if oldCRD.ResourceVersion == newCRD.ResourceVersion {
		c.enqueue(newCRD)
		return
	}

	// 如果新旧对象的差异仅在 Terminating 条件，通常是本控制器自己写入的状态；此时无需重新入队，
	// 否则会因自身更新形成热循环。处理失败时，工作队列会通过限速重试机制重新入队。
	// 这里通过深拷贝并比较对象来排除该类状态差异；该资源流量和规模较小，后续可考虑更轻量的比较方式。
	oldCopy := oldCRD.DeepCopy()
	newCopy := newCRD.DeepCopy()
	oldCopy.ResourceVersion = ""
	newCopy.ResourceVersion = ""
	apiextensionshelpers.RemoveCRDCondition(oldCopy, apiextensionsv1.Terminating)
	apiextensionshelpers.RemoveCRDCondition(newCopy, apiextensionsv1.Terminating)

	if !reflect.DeepEqual(oldCopy, newCopy) {
		c.enqueue(newCRD)
	}
}
