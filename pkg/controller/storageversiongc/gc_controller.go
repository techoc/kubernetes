/*
Copyright 2020 The Kubernetes Authors.

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

package storageversiongc

import (
	"context"
	"fmt"
	"sync"
	"time"

	apiserverinternalv1alpha1 "k8s.io/api/apiserverinternal/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storageversion"
	apiserverinternalinformers "k8s.io/client-go/informers/apiserverinternal/v1alpha1"
	coordinformers "k8s.io/client-go/informers/coordination/v1"
	"k8s.io/client-go/kubernetes"
	coordlisters "k8s.io/client-go/listers/coordination/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/controlplane/apiserver"

	"k8s.io/klog/v2"
)

// Controller 监听 kube-apiserver 的 identity lease 与 StorageVersion 对象，
// 清理已失效 apiserver 留下的存储版本条目；条目被清空时直接删除该 StorageVersion。
type Controller struct {
	kubeclientset kubernetes.Interface // 用于读取 lease、查询与更新 StorageVersion

	leaseLister  coordlisters.LeaseLister // 从缓存读取 identity lease
	leasesSynced cache.InformerSynced     // lease informer 的缓存同步状态

	storageVersionSynced cache.InformerSynced // StorageVersion informer 的缓存同步状态

	leaseQueue          workqueue.TypedRateLimitingInterface[string] // 已删除 lease 的处理队列
	storageVersionQueue workqueue.TypedRateLimitingInterface[string] // 待检查的 StorageVersion 队列
}

// NewStorageVersionGC 创建控制器并注册事件处理器：
// lease 只关注删除事件，StorageVersion 关注创建与更新事件。
func NewStorageVersionGC(ctx context.Context, clientset kubernetes.Interface, leaseInformer coordinformers.LeaseInformer, storageVersionInformer apiserverinternalinformers.StorageVersionInformer) *Controller {
	c := &Controller{
		kubeclientset:        clientset,
		leaseLister:          leaseInformer.Lister(),
		leasesSynced:         leaseInformer.Informer().HasSynced,
		storageVersionSynced: storageVersionInformer.Informer().HasSynced,
		leaseQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "storage_version_garbage_collector_leases"},
		),
		storageVersionQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "storage_version_garbage_collector_storageversions"},
		),
	}
	logger := klog.FromContext(ctx)
	_, err := leaseInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			c.onDeleteLease(logger, obj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	utilruntime.Must(err)
	// 使用 informer 默认重同步周期，周期性重新检查已有的 StorageVersion。
	_, err = storageVersionInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.onAddStorageVersion(logger, obj)
		},
		UpdateFunc: func(old, newObj interface{}) {
			c.onUpdateStorageVersion(logger, old, newObj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	utilruntime.Must(err)

	return c
}

// Run 启动两个 worker（lease 与 StorageVersion 各一个），等待缓存同步后开始处理队列，
// 直到上下文被取消。
func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting storage version garbage collector")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down storage version garbage collector")
		c.leaseQueue.ShutDown()
		c.storageVersionQueue.ShutDown()
		wg.Wait()
	}()

	// 必须等缓存同步完毕，否则从 lister 读取 lease 时可能误判为"不存在"，
	// 从而错误清理仍有效的存储版本条目。
	if !cache.WaitForCacheSync(ctx.Done(), c.leasesSynced, c.storageVersionSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// identity lease 删除与 StorageVersion 更新都不频繁，各分配一个 worker 即可。
	// lease worker 处理正常的 lease 删除，StorageVersion worker 处理新建/更新时
	// 就带上无效 apiserver ID 的情况（后者很少发生）。两个 worker 同时更新同一
	// StorageVersion 时可能产生冲突，但该资源更新本身幂等，交给工作队列重试即可。
	wg.Go(func() {
		wait.UntilWithContext(ctx, c.runLeaseWorker, time.Second)
	})
	wg.Go(func() {
		wait.UntilWithContext(ctx, c.runStorageVersionWorker, time.Second)
	})
	<-ctx.Done()
}

// runLeaseWorker 持续处理 lease 删除队列，直到队列关闭。
func (c *Controller) runLeaseWorker(ctx context.Context) {
	for c.processNextLease(ctx) {
	}
}

// processNextLease 取出一个 lease 键并处理：成功则清除重试计数，
// 失败则记录错误并按限速重新入队，避免持续失败时忙循环。
func (c *Controller) processNextLease(ctx context.Context) bool {
	key, quit := c.leaseQueue.Get()
	if quit {
		return false
	}
	defer c.leaseQueue.Done(key)

	err := c.processDeletedLease(ctx, key)
	if err == nil {
		c.leaseQueue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("lease %v failed with: %v", key, err))
	c.leaseQueue.AddRateLimited(key)
	return true
}

// runStorageVersionWorker 持续处理 StorageVersion 队列，直到队列关闭。
func (c *Controller) runStorageVersionWorker(ctx context.Context) {
	for c.processNextStorageVersion(ctx) {
	}
}

// processNextStorageVersion 取出一个 StorageVersion 键并同步：
// 成功则清除重试计数，失败则限速重试。
func (c *Controller) processNextStorageVersion(ctx context.Context) bool {
	key, quit := c.storageVersionQueue.Get()
	if quit {
		return false
	}
	defer c.storageVersionQueue.Done(key)

	err := c.syncStorageVersion(ctx, key)
	if err == nil {
		c.storageVersionQueue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("storage version %v failed with: %v", key, err))
	c.storageVersionQueue.AddRateLimited(key)
	return true
}

// processDeletedLease 处理 identity lease 的删除事件。
// 先从 apiserver 复核该 lease 确实已删除（事件可能延迟，对象已重建），
// 再遍历所有 StorageVersion 剔除对应 apiserver 的记录。
func (c *Controller) processDeletedLease(ctx context.Context, name string) error {
	_, err := c.kubeclientset.CoordinationV1().Leases(metav1.NamespaceSystem).Get(ctx, name, metav1.GetOptions{})
	// lease 仍存在，说明事件过期或对象已被重建，不做任何处理。
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	// 仅在 identity lease 删除时触发，因此全量列举 StorageVersion 的频率可接受。
	storageVersionList, err := c.kubeclientset.InternalV1alpha1().StorageVersions().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var errors []error
	for _, sv := range storageVersionList.Items {
		var serverStorageVersions []apiserverinternalv1alpha1.ServerStorageVersion
		hasStaleRecord := false
		for _, ssv := range sv.Status.StorageVersions {
			if ssv.APIServerID == name {
				// 剔除已删除 apiserver 的记录，其余记录保留。
				hasStaleRecord = true
				continue
			}
			serverStorageVersions = append(serverStorageVersions, ssv)
		}
		if !hasStaleRecord {
			// 该 StorageVersion 不含目标 apiserver 的记录，跳过更新。
			continue
		}
		if err := c.updateOrDeleteStorageVersion(ctx, &sv, serverStorageVersions); err != nil {
			errors = append(errors, err)
		}
	}

	// 聚合所有失败，保证一个 StorageVersion 更新失败不会阻断其他对象的处理。
	return utilerrors.NewAggregate(errors)
}

// syncStorageVersion 检查单个 StorageVersion 中是否含有无效 apiserver ID，
// 并剔除这些记录。判断依据是对应 identity lease 是否存在且带有 kube-apiserver 标签；
// 通过直接查询 apiserver（而非缓存）避免把刚刚创建的 lease 误判为不存在。
func (c *Controller) syncStorageVersion(ctx context.Context, name string) error {
	sv, err := c.kubeclientset.InternalV1alpha1().StorageVersions().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// 触发检查的对象已被删除，无需继续处理。
		return nil
	}
	if err != nil {
		return err
	}

	hasInvalidID := false
	var serverStorageVersions []apiserverinternalv1alpha1.ServerStorageVersion
	for _, v := range sv.Status.StorageVersions {
		lease, err := c.kubeclientset.CoordinationV1().Leases(metav1.NamespaceSystem).Get(ctx, v.APIServerID, metav1.GetOptions{})
		if err != nil || lease == nil || lease.Labels == nil ||
			lease.Labels[controlplane.IdentityLeaseComponentLabelKey] != apiserver.KubeAPIServer {
			// apiserver 上也没有对应的 identity lease，确认该记录已失效，需要清理。
			hasInvalidID = true
			continue
		}
		serverStorageVersions = append(serverStorageVersions, v)
	}
	// 所有条目都有效，无需更新。
	if !hasInvalidID {
		return nil
	}
	return c.updateOrDeleteStorageVersion(ctx, sv, serverStorageVersions)
}

// onAddStorageVersion 处理 StorageVersion 新增事件，入队做有效性检查。
func (c *Controller) onAddStorageVersion(logger klog.Logger, obj interface{}) {
	castObj := obj.(*apiserverinternalv1alpha1.StorageVersion)
	c.enqueueStorageVersion(logger, castObj)
}

// onUpdateStorageVersion 处理 StorageVersion 更新事件。
// 只检查新对象：旧状态无需清理，需要修正的是当前状态。
func (c *Controller) onUpdateStorageVersion(logger klog.Logger, oldObj, newObj interface{}) {
	castNewObj := newObj.(*apiserverinternalv1alpha1.StorageVersion)
	c.enqueueStorageVersion(logger, castNewObj)
}

// enqueueStorageVersion 仅当 StorageVersion 含有无效 apiserver 记录时才入队，
// 避免正常的存储版本更新触发不必要的 API 查询。
func (c *Controller) enqueueStorageVersion(logger klog.Logger, obj *apiserverinternalv1alpha1.StorageVersion) {
	for _, sv := range obj.Status.StorageVersions {
		lease, err := c.leaseLister.Leases(metav1.NamespaceSystem).Get(sv.APIServerID)
		if err != nil || lease == nil || lease.Labels == nil ||
			lease.Labels[controlplane.IdentityLeaseComponentLabelKey] != apiserver.KubeAPIServer {
			// 缓存中找不到有效 lease，入队进一步向 apiserver 复核。
			logger.V(4).Info("Observed storage version with invalid apiserver entry", "objName", obj.Name)
			c.storageVersionQueue.Add(obj.Name)
			return
		}
	}
}

// onDeleteLease 处理 lease 删除事件，兼容 DeletedFinalStateUnknown 墓碑。
// 只关心 kube-system 命名空间下、带有 kube-apiserver 身份标签的 lease。
func (c *Controller) onDeleteLease(logger klog.Logger, obj interface{}) {
	castObj, ok := obj.(*coordinationv1.Lease)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		castObj, ok = tombstone.Obj.(*coordinationv1.Lease)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Lease %#v", obj))
			return
		}
	}

	if castObj.Namespace == metav1.NamespaceSystem &&
		castObj.Labels != nil &&
		castObj.Labels[controlplane.IdentityLeaseComponentLabelKey] == apiserver.KubeAPIServer {
		logger.V(4).Info("Observed lease deleted", "castObjName", castObj.Name)
		c.enqueueLease(castObj)
	}
}

// enqueueLease 把被删除 lease 的名字入队。
func (c *Controller) enqueueLease(obj *coordinationv1.Lease) {
	c.leaseQueue.Add(obj.Name)
}

// updateOrDeleteStorageVersion 在清理后的记录为空时删除整个 StorageVersion，
// 否则更新状态并重新计算通用编码版本，保证编码版本反映剩余 apiserver 的实际能力。
func (c *Controller) updateOrDeleteStorageVersion(ctx context.Context, sv *apiserverinternalv1alpha1.StorageVersion, serverStorageVersions []apiserverinternalv1alpha1.ServerStorageVersion) error {
	if len(serverStorageVersions) == 0 {
		return c.kubeclientset.InternalV1alpha1().StorageVersions().Delete(
			ctx, sv.Name, metav1.DeleteOptions{})
	}
	sv.Status.StorageVersions = serverStorageVersions
	storageversion.SetCommonEncodingVersion(sv)
	_, err := c.kubeclientset.InternalV1alpha1().StorageVersions().UpdateStatus(
		ctx, sv, metav1.UpdateOptions{})
	return err
}
