/*
Copyright 2018 The Kubernetes Authors.

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

// Package establish 实现 CRD 建立（Establishing）控制器。
// 当 CRD 的 NamesAccepted 条件为 True 时，设置 Established 条件为 True。
// Established 条件表示 CRD 的 HTTP 路由已安装，CR 可以通过 API 访问。
// 在 HA 模式下，控制器会延迟处理，避免多个 API Server 竞争。
package establish

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

// EstablishingController 控制 CRD 何时进入 Established 状态。
// Established 表示 CRD 的 HTTP 路由已安装，CR 可以通过 API 访问。
type EstablishingController struct {
	crdClient client.CustomResourceDefinitionsGetter
	crdLister listers.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced

	// 通过函数注入同步逻辑，便于单元测试替换。
	syncFn func(ctx context.Context, key string) error

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewEstablishingController 创建 CRD 建立控制器。
func NewEstablishingController(crdInformer informers.CustomResourceDefinitionInformer,
	crdClient client.CustomResourceDefinitionsGetter) *EstablishingController {
	ec := &EstablishingController{
		crdClient: crdClient,
		crdLister: crdInformer.Lister(),
		crdSynced: crdInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "crdEstablishing"},
		),
	}

	ec.syncFn = ec.sync

	return ec
}

// QueueCRD 将 CRD 加入建立队列，支持延迟添加（用于 HA 模式下的竞争避免）。
func (ec *EstablishingController) QueueCRD(key string, timeout time.Duration) {
	ec.queue.AddAfter(key, timeout)
}

// RunWithContext 启动 EstablishingController，等待缓存同步后运行工作线程，直到上下文取消。
func (ec *EstablishingController) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer ec.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.V(2).Info("Starting EstablishingController")
	defer logger.V(2).Info("Shutting down EstablishingController")

	if !cache.WaitForCacheSync(ctx.Done(), ec.crdSynced) {
		return
	}

	// 建立控制器不是性能瓶颈，因此只启动一个工作线程即可。
	go wait.UntilWithContext(ctx, ec.runWorker, time.Second)

	<-ctx.Done()
}

//logcheck:context // 支持上下文日志的代码应调用 RunWithContext，而不是兼容旧接口的 Run。
func (ec *EstablishingController) Run(stopCh <-chan struct{}) {
	ec.RunWithContext(wait.ContextForChannel(stopCh))
}

func (ec *EstablishingController) runWorker(ctx context.Context) {
	for ec.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 从队列中取出并处理一个 CRD 键。
// 队列关闭或需要退出时返回 false。
func (ec *EstablishingController) processNextWorkItem(ctx context.Context) bool {
	key, quit := ec.queue.Get()
	if quit {
		return false
	}
	defer ec.queue.Done(key)

	err := ec.syncFn(ctx, key)
	if err == nil {
		ec.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	ec.queue.AddRateLimited(key)

	return true
}

// sync 将 CRD 设置为 Established 状态。
// 如果 NamesAccepted 为 True 且 Established 尚未设置，则设置 Established 条件。
// 如果 Conversion Webhook 的 CABundle 无效，设置 Established=False 并说明原因。
func (ec *EstablishingController) sync(ctx context.Context, key string) error {
	cachedCRD, err := ec.crdLister.Get(key)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	if !apiextensionshelpers.IsCRDConditionTrue(cachedCRD, apiextensionsv1.NamesAccepted) ||
		apiextensionshelpers.IsCRDConditionTrue(cachedCRD, apiextensionsv1.Established) {
		return nil
	}

	crd := cachedCRD.DeepCopy()

	// 如果 Conversion Webhook 的 CABundle 无效，则将 Established 设为 False 并记录原因。
	if cachedCRD.Spec.Conversion != nil &&
		cachedCRD.Spec.Conversion.Webhook != nil &&
		cachedCRD.Spec.Conversion.Webhook.ClientConfig != nil &&
		len(webhook.ValidateCABundle(field.NewPath(""), cachedCRD.Spec.Conversion.Webhook.ClientConfig.CABundle)) > 0 {
		errorCondition := apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Established,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "InvalidCABundle",
			Message: "The conversion webhook CABundle is invalid",
		}
		apiextensionshelpers.SetCRDCondition(crd, errorCondition)
	} else {
		establishedCondition := apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Established,
			Status:  apiextensionsv1.ConditionTrue,
			Reason:  "InitialNamesAccepted",
			Message: "the initial names have been accepted",
		}
		apiextensionshelpers.SetCRDCondition(crd, establishedCondition)
	}

	// 通过 Status 子资源将新的 CRD 条件写回 API Server。
	_, err = ec.crdClient.CustomResourceDefinitions().UpdateStatus(ctx, crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// 对象可能已被删除或并发修改；后续事件或队列重试会再次触发处理。
		return nil
	}
	if err != nil {
		return err
	}

	return nil
}
