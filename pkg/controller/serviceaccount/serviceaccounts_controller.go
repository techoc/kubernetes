/*
Copyright 2014 The Kubernetes Authors.

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

package serviceaccount

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// ServiceAccountsControllerOptions 包含创建 ServiceAccountsController 所需的选项。
type ServiceAccountsControllerOptions struct {
	// ServiceAccounts 是必须保证在每个命名空间中都存在的 ServiceAccount 列表。
	ServiceAccounts []v1.ServiceAccount

	// ServiceAccountResync 是 ServiceAccount 全量重新同步的间隔。
	// 非零时按该频率重新列出所有 ServiceAccount；为零时尽量推迟重列，
	// 直到 watch 关闭或超时才触发。
	ServiceAccountResync time.Duration

	// NamespaceResync 是 Namespace 全量重新同步的间隔。
	// 非零时按该频率重新列出所有命名空间；为零时尽量推迟重列，
	// 直到 watch 关闭或超时才触发。
	NamespaceResync time.Duration
}

// DefaultServiceAccountsControllerOptions 返回创建控制器的默认选项：
// 只保证每个命名空间存在名为 default 的 ServiceAccount。
func DefaultServiceAccountsControllerOptions() ServiceAccountsControllerOptions {
	return ServiceAccountsControllerOptions{
		ServiceAccounts: []v1.ServiceAccount{
			{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		},
	}
}

// NewServiceAccountsController 创建控制器实例，注册 ServiceAccount 删除以及
// Namespace 增删事件的处理函数，并设置同步处理器。
func NewServiceAccountsController(logger klog.Logger, saInformer coreinformers.ServiceAccountInformer, nsInformer coreinformers.NamespaceInformer, cl clientset.Interface, options ServiceAccountsControllerOptions) (*ServiceAccountsController, error) {
	e := &ServiceAccountsController{
		client:                  cl,
		serviceAccountsToEnsure: options.ServiceAccounts,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "serviceaccount"},
		),
	}

	saHandler, _ := saInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			e.serviceAccountDeleted(logger, obj)
		},
	}, options.ServiceAccountResync)
	e.saLister = saInformer.Lister()
	e.saListerSynced = saHandler.HasSynced

	nsHandler, _ := nsInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    e.namespaceAdded,
		UpdateFunc: e.namespaceUpdated,
	}, options.NamespaceResync)
	e.nsLister = nsInformer.Lister()
	e.nsListerSynced = nsHandler.HasSynced

	e.syncHandler = e.syncNamespace

	return e, nil
}

// ServiceAccountsController 负责保证每个活跃命名空间中存在指定的 ServiceAccount。
type ServiceAccountsController struct {
	client                  clientset.Interface
	serviceAccountsToEnsure []v1.ServiceAccount

	// 可在测试中注入自定义同步逻辑，避免依赖真实 informer。
	syncHandler func(ctx context.Context, key string) error

	saLister       corelisters.ServiceAccountLister
	saListerSynced cache.InformerSynced

	nsLister       corelisters.NamespaceLister
	nsListerSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]
}

// Run 启动控制器并阻塞，直到 ctx 被取消。
func (c *ServiceAccountsController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Starting service account controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down service account controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	// lister 未同步完成前不能处理工作项，否则可能因缓存为空而重复创建。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.saListerSynced, c.nsListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// serviceAccountDeleted 在 ServiceAccount 被删除时把其命名空间入队，以便重建。
func (c *ServiceAccountsController) serviceAccountDeleted(logger klog.Logger, obj interface{}) {
	// informer 可能只给出墓碑事件，需要先解出原始对象才能拿到命名空间。
	sa, ok := obj.(*v1.ServiceAccount)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Couldn't get object from tombstone", "obj", obj)
			return
		}
		sa, ok = tombstone.Obj.(*v1.ServiceAccount)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Tombstone contained object that is not a ServiceAccount", "type", fmt.Sprintf("%T", obj))
			return
		}
	}
	c.queue.Add(sa.Namespace)
}

// namespaceAdded 在新命名空间创建时把其名称入队。
func (c *ServiceAccountsController) namespaceAdded(obj interface{}) {
	namespace := obj.(*v1.Namespace)
	c.queue.Add(namespace.Name)
}

// namespaceUpdated 在命名空间更新或重新列出时把其名称入队。
// 这里入队是幂等的，因此无需区分具体变更字段。
func (c *ServiceAccountsController) namespaceUpdated(oldObj interface{}, newObj interface{}) {
	newNamespace := newObj.(*v1.Namespace)
	c.queue.Add(newNamespace.Name)
}

// runWorker 持续处理队列中的工作项，直到 processNextWorkItem 返回 false。
func (c *ServiceAccountsController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个键；队列关闭时返回 false。
func (c *ServiceAccountsController) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	// 同步成功后清除该键的失败计数，避免后续重试被限速。
	err := c.syncHandler(ctx, key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Service account work item failed", "item", key)
	c.queue.AddRateLimited(key)

	return true
}

// syncNamespace 保证指定活跃命名空间中存在所有配置的 ServiceAccount。
func (c *ServiceAccountsController) syncNamespace(ctx context.Context, key string) error {
	startTime := time.Now()
	defer func() {
		klog.FromContext(ctx).V(4).Info("Finished syncing namespace", "namespace", key, "duration", time.Since(startTime))
	}()

	ns, err := c.nsLister.Get(key)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if ns.Status.Phase != v1.NamespaceActive {
		// 非活跃命名空间即将删除，不应在里面创建对象。
		return nil
	}

	createFailures := []error{}
	for _, sa := range c.serviceAccountsToEnsure {
		switch _, err := c.saLister.ServiceAccounts(ns.Name).Get(sa.Name); {
		case err == nil:
			continue
		case apierrors.IsNotFound(err):
		case err != nil:
			return err
		}
		// 这里直接修改循环变量是安全的：该字段只写不读。
		// TODO 等 fake client 支持无命名空间创建后去掉这段赋值。
		sa.Namespace = ns.Name

		if _, err := c.client.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &sa, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			// 命名空间正在终止时的创建失败无需计入，控制器会在下次同步时重新评估。
			if !apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
				createFailures = append(createFailures, err)
			}
		}
	}

	return utilerrors.Flatten(utilerrors.NewAggregate(createFailures))
}
