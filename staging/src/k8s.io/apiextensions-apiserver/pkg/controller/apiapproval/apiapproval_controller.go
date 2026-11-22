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

// Package apiapproval 维护受保护 API group 的 KubernetesAPIApprovalPolicyConformant 条件。
package apiapproval

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// KubernetesAPIApprovalPolicyConformantConditionController 维护 KubernetesAPIApprovalPolicyConformant 条件。
type KubernetesAPIApprovalPolicyConformantConditionController struct {
	crdClient client.CustomResourceDefinitionsGetter

	crdLister listers.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced

	// 便于测试时注入自定义实现。
	syncFn func(ctx context.Context, key string) error

	queue workqueue.TypedRateLimitingInterface[string]

	// lastSeenProtectedAnnotation 记录控制器上次更新条件时看到的受保护注解值（按 CRD 名）。
	// 避免 HA 中不同版本的 apiextensions-apiserver 为写入正确的 message 而互相争抢。
	lastSeenProtectedAnnotationLock sync.Mutex
	lastSeenProtectedAnnotation     map[string]string
}

// NewKubernetesAPIApprovalPolicyConformantConditionController 构造 KubernetesAPIApprovalPolicyConformant schema 条件控制器。
func NewKubernetesAPIApprovalPolicyConformantConditionController(
	crdInformer informers.CustomResourceDefinitionInformer,
	crdClient client.CustomResourceDefinitionsGetter,
) *KubernetesAPIApprovalPolicyConformantConditionController {
	c := &KubernetesAPIApprovalPolicyConformantConditionController{
		crdClient: crdClient,
		crdLister: crdInformer.Lister(),
		crdSynced: crdInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubernetes_api_approval_conformant_condition_controller"},
		),
		lastSeenProtectedAnnotation: map[string]string{},
	}

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
		DeleteFunc: c.deleteCustomResourceDefinition,
	})

	c.syncFn = c.sync

	return c
}

// calculateCondition 计算新的 KubernetesAPIApprovalPolicyConformant 条件。
func calculateCondition(crd *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceDefinitionCondition {
	// 只有受保护社区 group 才需要 API 审批
	if !apihelpers.IsProtectedCommunityGroup(crd.Spec.Group) {
		return nil
	}

	approvalState, reason := apihelpers.GetAPIApprovalState(crd.Annotations)
	switch approvalState {
	case apihelpers.APIApprovalInvalid:
		return &apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.KubernetesAPIApprovalPolicyConformant,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "InvalidAnnotation",
			Message: reason,
		}
	case apihelpers.APIApprovalMissing:
		return &apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.KubernetesAPIApprovalPolicyConformant,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "MissingAnnotation",
			Message: reason,
		}
	case apihelpers.APIApproved:
		return &apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.KubernetesAPIApprovalPolicyConformant,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "ApprovedAnnotation",
			Message: reason,
		}
	case apihelpers.APIApprovalBypassed:
		return &apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.KubernetesAPIApprovalPolicyConformant,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "UnapprovedAnnotation",
			Message: reason,
		}
	default:
		return &apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.KubernetesAPIApprovalPolicyConformant,
			Status:  apiextensionsv1.ConditionUnknown,
			Reason:  "UnknownAnnotation",
			Message: reason,
		}
	}
}

// sync 协调单个 CRD 的 API 审批条件。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) sync(ctx context.Context, key string) error {
	inCustomResourceDefinition, err := c.crdLister.Get(key)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	// 同一注解值避免重复计算
	protectionAnnotationValue := inCustomResourceDefinition.Annotations[apiextensionsv1.KubeAPIApprovedAnnotation]
	c.lastSeenProtectedAnnotationLock.Lock()
	lastSeen, seenBefore := c.lastSeenProtectedAnnotation[inCustomResourceDefinition.Name]
	c.lastSeenProtectedAnnotationLock.Unlock()
	if seenBefore && protectionAnnotationValue == lastSeen {
		return nil
	}

	// 检查旧条件
	cond := calculateCondition(inCustomResourceDefinition)
	if cond == nil {
		// group 不可变；若当前无需条件，也就不必移除已有条件。
		return nil
	}
	old := apihelpers.FindCRDCondition(inCustomResourceDefinition, apiextensionsv1.KubernetesAPIApprovalPolicyConformant)

	// 若条件各项完全一致，则不必写入
	if old != nil && old.Status == cond.Status && old.Reason == cond.Reason && old.Message == cond.Message {
		// 未采取动作，无需更新注解记录。
		return nil
	}

	// 更新条件
	crd := inCustomResourceDefinition.DeepCopy()
	apihelpers.SetCRDCondition(crd, *cond)

	_, err = c.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 对象已被删除或在此期间被修改，后续会再次被调度
		return nil
	}
	if err != nil {
		return err
	}

	// 记录注解值，避免对同一注解重复更新（以及 HA 环境下 API server 之间的争抢）。
	c.lastSeenProtectedAnnotationLock.Lock()
	defer c.lastSeenProtectedAnnotationLock.Unlock()
	c.lastSeenProtectedAnnotation[crd.Name] = protectionAnnotationValue

	return nil
}

// Run 启动控制器。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) Run(workers int, stopCh <-chan struct{}) {
	c.RunWithContext(workers, wait.ContextForChannel(stopCh))
}

// RunWithContext 以上下文方式启动控制器。
//
//logcheck:context // 支持上下文日志的代码应改用 RunWithContext 而不是 Run。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) RunWithContext(workers int, ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting KubernetesAPIApprovalPolicyConformantConditionController")
	defer klog.Infof("Shutting down KubernetesAPIApprovalPolicyConformantConditionController")

	if !cache.WaitForCacheSync(ctx.Done(), c.crdSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
}

// runWorker 持续处理队列中的工作项，直到队列关闭。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 key；需要退出时返回 false。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) processNextWorkItem(ctx context.Context) bool {
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

// enqueue 将对象对应的 key 加入工作队列。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

// addCustomResourceDefinition 处理 CRD 新增事件。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding %s", castObj.Name)
	c.enqueue(castObj)
}

// updateCustomResourceDefinition 处理 CRD 更新事件，仅关心新对象。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) updateCustomResourceDefinition(obj, _ interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating %s", castObj.Name)
	c.enqueue(castObj)
}

// deleteCustomResourceDefinition 处理 CRD 删除事件，并清理记录的注解值。
func (c *KubernetesAPIApprovalPolicyConformantConditionController) deleteCustomResourceDefinition(obj interface{}) {
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

	c.lastSeenProtectedAnnotationLock.Lock()
	defer c.lastSeenProtectedAnnotationLock.Unlock()
	delete(c.lastSeenProtectedAnnotation, castObj.Name)
}
