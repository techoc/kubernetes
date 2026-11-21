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

package rootcacertpublisher

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// RootCACertConfigMapName 是存放用于访问 api-server 证书的 ConfigMap 的名称。
const (
	RootCACertConfigMapName = "kube-root-ca.crt"
	DescriptionAnnotation   = "kubernetes.io/description"
	Description             = "Contains a CA bundle that can be used to verify the kube-apiserver when using internal endpoints such as the internal service IP or kubernetes.default.svc. " +
		"No other usage is guaranteed across distributions of Kubernetes clusters."
)

// init 注册控制器指标，必须在控制器启动前完成。
func init() {
	registerMetrics()
}

// NewPublisher 构造用于管理各命名空间证书 ConfigMap 的控制器，
// 保证每个命名空间中都存在存放根 CA 的 ConfigMap。
// 它监听 ConfigMap 的删除/更新与命名空间的添加/更新事件，
// 将受影响的命名空间加入工作队列。
func NewPublisher(cmInformer coreinformers.ConfigMapInformer, nsInformer coreinformers.NamespaceInformer, cl clientset.Interface, rootCA []byte) (*Publisher, error) {
	e := &Publisher{
		client: cl,
		rootCA: rootCA,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "root_ca_cert_publisher",
			},
		),
	}

	cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: e.configMapDeleted,
		UpdateFunc: e.configMapUpdated,
	})
	e.cmLister = cmInformer.Lister()
	e.cmListerSynced = cmInformer.Informer().HasSynced

	nsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    e.namespaceAdded,
		UpdateFunc: e.namespaceUpdated,
	})
	e.nsListerSynced = nsInformer.Informer().HasSynced

	e.syncHandler = e.syncNamespace

	return e, nil

}

// Publisher 管理各命名空间内的证书 ConfigMap 对象。
type Publisher struct {
	client clientset.Interface
	rootCA []byte

	// 同步函数可在测试中替换，便于注入假的同步逻辑。
	syncHandler func(ctx context.Context, key string) error

	cmLister       corelisters.ConfigMapLister
	cmListerSynced cache.InformerSynced

	nsListerSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]
}

// Run 启动控制器：等待 ConfigMap informer 同步后，开启多个 worker 持续处理队列，直到 ctx 被取消。
func (c *Publisher) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting root CA cert publisher controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down root CA cert publisher controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.cmListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// configMapDeleted 处理 ConfigMap 删除事件：只有根 CA ConfigMap 被删除时，才把其命名空间重新入队以触发重建。
func (c *Publisher) configMapDeleted(obj interface{}) {
	cm, err := convertToCM(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if cm.Name != RootCACertConfigMapName {
		return
	}
	c.queue.Add(cm.Namespace)
}

// configMapUpdated 处理 ConfigMap 更新事件：根 CA ConfigMap 被改动时把命名空间入队，以便校正内容。
func (c *Publisher) configMapUpdated(_, newObj interface{}) {
	cm, err := convertToCM(newObj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if cm.Name != RootCACertConfigMapName {
		return
	}
	c.queue.Add(cm.Namespace)
}

// namespaceAdded 处理命名空间添加事件：新命名空间可能需要根 CA ConfigMap，因此将其入队。
func (c *Publisher) namespaceAdded(obj interface{}) {
	namespace := obj.(*v1.Namespace)
	c.queue.Add(namespace.Name)
}

// namespaceUpdated 处理命名空间更新事件：只有进入 Active 状态时才入队，
// 因为只有活跃命名空间才需要确保根 CA ConfigMap 存在。
func (c *Publisher) namespaceUpdated(oldObj interface{}, newObj interface{}) {
	newNamespace := newObj.(*v1.Namespace)
	if newNamespace.Status.Phase != v1.NamespaceActive {
		return
	}
	c.queue.Add(newNamespace.Name)
}

// runWorker 持续从队列中取出条目处理，直到 processNextWorkItem 返回 false（队列关闭）。
func (c *Publisher) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个键（命名空间）：同步失败时记录错误并限速重试，
// 成功后清除该键的重试计数；队列关闭时返回 false 表示应退出。
func (c *Publisher) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if err := c.syncHandler(ctx, key); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncing %q failed: %v", key, err))
		c.queue.AddRateLimited(key)
		return true
	}

	c.queue.Forget(key)
	return true
}

// syncNamespace 同步单个命名空间：若根 CA ConfigMap 不存在则创建，
// 若其数据与说明注解不符则更新，使命名空间始终持有最新的根 CA 证书。
func (c *Publisher) syncNamespace(ctx context.Context, ns string) (err error) {
	startTime := time.Now()
	defer func() {
		recordMetrics(startTime, err)
		klog.FromContext(ctx).V(4).Info("Finished syncing namespace", "namespace", ns, "elapsedTime", time.Since(startTime))
	}()

	cm, err := c.cmLister.ConfigMaps(ns).Get(RootCACertConfigMapName)
	switch {
	case apierrors.IsNotFound(err):
		_, err = c.client.CoreV1().ConfigMaps(ns).Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        RootCACertConfigMapName,
				Annotations: map[string]string{DescriptionAnnotation: Description},
			},
			Data: map[string]string{
				"ca.crt": string(c.rootCA),
			},
		}, metav1.CreateOptions{})
		// 命名空间不存在或正在终止时创建必然失败，重试也无意义，故直接吞掉该错误。
		if apierrors.IsNotFound(err) || apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
			return nil
		}
		return err
	case err != nil:
		return err
	}

	data := map[string]string{
		"ca.crt": string(c.rootCA),
	}

	// 数据与用途注解都正确时无需更新，避免无意义的写操作。
	if reflect.DeepEqual(cm.Data, data) && len(cm.Annotations[DescriptionAnnotation]) > 0 {
		return nil
	}

	// lister 返回的是缓存中的对象，必须深拷贝后再改，避免污染 informer 缓存。
	cm = cm.DeepCopy()
	cm.Data = data
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations[DescriptionAnnotation] = Description

	_, err = c.client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

// convertToCM 将 informer 回调中的对象转换为 ConfigMap：
// 删除事件可能传来 DeletedFinalStateUnknown 墓碑，需从中取出原始对象。
func convertToCM(obj interface{}) (*v1.ConfigMap, error) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return nil, fmt.Errorf("couldn't get object from tombstone %#v", obj)
		}
		cm, ok = tombstone.Obj.(*v1.ConfigMap)
		if !ok {
			return nil, fmt.Errorf("tombstone contained object that is not a ConfigMap %#v", obj)
		}
	}
	return cm, nil
}
