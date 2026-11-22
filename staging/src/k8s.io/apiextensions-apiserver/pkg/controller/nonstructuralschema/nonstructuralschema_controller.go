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

package nonstructuralschema

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

// ConditionController 维护 NonStructuralSchema condition。
type ConditionController struct {
	crdClient client.CustomResourceDefinitionsGetter

	crdLister listers.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced

	// 允许测试注入。
	syncFn func(ctx context.Context, key string) error

	queue workqueue.TypedRateLimitingInterface[string]

	// lastSeenGeneration 按 CRD 名称记录本控制器上次更新 condition 的 generation。
	// 这样在 HA 部署下，多个 apiextensions-apiserver 不会为同一个消息反复互抢。
	lastSeenGenerationLock sync.Mutex
	lastSeenGeneration     map[string]int64
}

// NewConditionController 构造 non-structural schema condition 控制器。
func NewConditionController(
	crdInformer informers.CustomResourceDefinitionInformer,
	crdClient client.CustomResourceDefinitionsGetter,
) *ConditionController {
	c := &ConditionController{
		crdClient: crdClient,
		crdLister: crdInformer.Lister(),
		crdSynced: crdInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "non_structural_schema_condition_controller"},
		),
		lastSeenGeneration: map[string]int64{},
	}

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
		DeleteFunc: c.deleteCustomResourceDefinition,
	})

	c.syncFn = c.sync

	return c
}

// calculateCondition 计算某个 CRD 的 NonStructuralSchema condition。
// 返回 nil 表示没有违规，应移除该 condition。
func calculateCondition(in *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceDefinitionCondition {
	cond := &apiextensionsv1.CustomResourceDefinitionCondition{
		Type:   apiextensionsv1.NonStructuralSchema,
		Status: apiextensionsv1.ConditionUnknown,
	}

	allErrs := field.ErrorList{}

	if in.Spec.PreserveUnknownFields {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "preserveUnknownFields"),
			in.Spec.PreserveUnknownFields,
			fmt.Sprint("must be false")))
	}

	for i, v := range in.Spec.Versions {
		// 没有 schema 的版本不参与结构性判断。
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}

		internalSchema := &apiextensionsinternal.CustomResourceValidation{}
		if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(v.Schema, internalSchema, nil); err != nil {
			klog.Errorf("failed to convert CRD validation to internal version: %v", err)
			continue
		}
		s, err := schema.NewStructural(internalSchema.OpenAPIV3Schema)
		if err != nil {
			cond.Reason = "StructuralError"
			cond.Message = fmt.Sprintf("failed to check validation schema for version %s: %v", v.Name, err)
			return cond
		}

		pth := field.NewPath("spec", "versions").Index(i).Child("schema", "openAPIV3Schema")

		allErrs = append(allErrs, schema.ValidateStructural(pth, s)...)
	}

	if len(allErrs) == 0 {
		return nil
	}

	cond.Status = apiextensionsv1.ConditionTrue
	cond.Reason = "Violations"
	cond.Message = allErrs.ToAggregate().Error()

	return cond
}

// sync 处理队列中的一个 CRD，按需更新其 NonStructuralSchema condition。
func (c *ConditionController) sync(ctx context.Context, key string) error {
	inCustomResourceDefinition, err := c.crdLister.Get(key)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	// 同一 generation 不重复计算
	c.lastSeenGenerationLock.Lock()
	lastSeen, seenBefore := c.lastSeenGeneration[inCustomResourceDefinition.Name]
	c.lastSeenGenerationLock.Unlock()
	if seenBefore && inCustomResourceDefinition.Generation <= lastSeen {
		return nil
	}

	// 检查旧 condition
	cond := calculateCondition(inCustomResourceDefinition)
	old := apiextensionshelpers.FindCRDCondition(inCustomResourceDefinition, apiextensionsv1.NonStructuralSchema)

	if cond == nil && old == nil {
		return nil
	}
	if cond != nil && old != nil && old.Status == cond.Status && old.Reason == cond.Reason && old.Message == cond.Message {
		return nil
	}

	// 更新 condition
	crd := inCustomResourceDefinition.DeepCopy()
	if cond == nil {
		apiextensionshelpers.RemoveCRDCondition(crd, apiextensionsv1.NonStructuralSchema)
	} else {
		cond.LastTransitionTime = metav1.NewTime(time.Now())
		apiextensionshelpers.SetCRDCondition(crd, *cond)
	}

	_, err = c.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 删除或期间被修改，会再次触发，这里静默返回
		return nil
	}
	if err != nil {
		return err
	}

	// 记录 generation，避免同一 generation 反复更新（以及 HA 环境下多个
	// apiserver 之间的互抢）。
	c.lastSeenGenerationLock.Lock()
	defer c.lastSeenGenerationLock.Unlock()
	c.lastSeenGeneration[crd.Name] = crd.Generation

	return nil
}

// Run 启动控制器。
//
//logcheck:context // 支持上下文日志的代码应改用 RunWithContext 而不是 Run。
func (c *ConditionController) Run(workers int, stopCh <-chan struct{}) {
	c.RunWithContext(workers, wait.ContextForChannel(stopCh))
}

// RunWithContext 是 Run 的上下文感知版本。
func (c *ConditionController) RunWithContext(workers int, ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting NonStructuralSchemaConditionController")
	defer klog.Infof("Shutting down NonStructuralSchemaConditionController")

	if !cache.WaitForCacheSync(ctx.Done(), c.crdSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
}

// runWorker 是单个 worker 的循环，持续处理队列直到队列关闭。
func (c *ConditionController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 key。到了退出时间时返回 false。
func (c *ConditionController) processNextWorkItem(ctx context.Context) bool {
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

// enqueue 把对象对应的 key 加入队列。
func (c *ConditionController) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

// addCustomResourceDefinition 是 informer 的 Add 回调。
func (c *ConditionController) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding %s", castObj.Name)
	c.enqueue(castObj)
}

// updateCustomResourceDefinition 是 informer 的 Update 回调；旧对象不参与处理。
func (c *ConditionController) updateCustomResourceDefinition(obj, _ interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating %s", castObj.Name)
	c.enqueue(castObj)
}

// deleteCustomResourceDefinition 是 informer 的 Delete 回调，负责清理 generation 记录。
func (c *ConditionController) deleteCustomResourceDefinition(obj interface{}) {
	castObj, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		// 删除事件可能以 tombstone 形式送达，需要再解一层。
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

	c.lastSeenGenerationLock.Lock()
	defer c.lastSeenGenerationLock.Unlock()
	delete(c.lastSeenGeneration, castObj.Name)
}
