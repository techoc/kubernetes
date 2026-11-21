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

package apiserverleasegc

import (
	"context"
	"time"

	v1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	informers "k8s.io/client-go/informers/coordination/v1"
	"k8s.io/client-go/kubernetes"
	listers "k8s.io/client-go/listers/coordination/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

// Controller 删除过期的 apiserver lease。
type Controller struct {
	kubeclientset kubernetes.Interface

	leaseLister   listers.LeaseLister
	leaseInformer cache.SharedIndexInformer
	leasesSynced  cache.InformerSynced

	leaseNamespace string

	gcCheckPeriod time.Duration
}

// NewAPIServerLeaseGC 创建一个新的 Controller。
func NewAPIServerLeaseGC(clientset kubernetes.Interface, gcCheckPeriod time.Duration, leaseNamespace, leaseLabelSelector string) *Controller {
	// 只关心单个 namespace 且带标签筛选的一小部分数据，因此自建专用 informer 而非共享工厂。
	leaseInformer := informers.NewFilteredLeaseInformer(
		clientset,
		leaseNamespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(listOptions *metav1.ListOptions) {
			listOptions.LabelSelector = leaseLabelSelector
		})
	return &Controller{
		kubeclientset:  clientset,
		leaseLister:    listers.NewLeaseLister(leaseInformer.GetIndexer()),
		leaseInformer:  leaseInformer,
		leasesSynced:   leaseInformer.HasSynced,
		leaseNamespace: leaseNamespace,
		gcCheckPeriod:  gcCheckPeriod,
	}
}

// Run 启动一个 worker。
//
//logcheck:context // 支持上下文日志的代码中应改用 RunWithContext。
func (c *Controller) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 启动一个带上下文的 worker。
func (c *Controller) RunWithContext(ctx context.Context) {
	logger := klog.FromContext(ctx)
	defer utilruntime.HandleCrashWithContext(ctx)
	defer logger.Info("Shutting down apiserver lease garbage collector")

	logger.Info("Starting apiserver lease garbage collector")

	// 启动范围受限的专用 informer。
	go c.leaseInformer.RunWithContext(ctx)

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.leasesSynced) {
		utilruntime.HandleErrorWithContext(ctx, nil, "Timed out waiting for caches to sync")
		return
	}

	go wait.UntilWithContext(ctx, c.gc, c.gcCheckPeriod)

	<-ctx.Done()
}

// gc 定期清理过期 lease，删除前会从 apiserver 二次确认以避免误删。
func (c *Controller) gc(ctx context.Context) {
	leases, err := c.leaseLister.Leases(c.leaseNamespace).List(labels.Everything())
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Error while listing apiserver leases")
		return
	}
	for _, lease := range leases {
		// 先根据缓存快速判断，减少不必要的 API 调用。
		if !isLeaseExpired(lease) {
			continue
		}
		// 删除前向 apiserver 拉取最新租约二次确认，避免误删已被续租的对象。
		lease, err := c.kubeclientset.CoordinationV1().Leases(c.leaseNamespace).Get(ctx, lease.Name, metav1.GetOptions{})
		if err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Error getting lease")
			continue
		}
		if errors.IsNotFound(err) || lease == nil {
			// HA 集群中租约可能已被其他 apiserver 上的 GC 删除，属正常情况。
			logger.V(4).Info("Cannot find apiserver lease", "err", err)
			continue
		}
		// 再按 apiserver 返回的最新数据判断是否过期。
		if !isLeaseExpired(lease) {
			continue
		}
		if err := c.kubeclientset.CoordinationV1().Leases(c.leaseNamespace).Delete(
			ctx, lease.Name, metav1.DeleteOptions{}); err != nil {
			if errors.IsNotFound(err) {
				// HA 集群中租约可能已被其他 apiserver 的 GC 抢先删除，属正常情况。
				logger.V(4).Info("Apiserver lease is gone already", "err", err)
			} else {
				logger.Error(err, "Error deleting lease")
			}
		}
	}
}

// isLeaseExpired 判断租约是否过期；缺少续约时间或租约时长视为非法并应回收。
func isLeaseExpired(lease *v1.Lease) bool {
	currentTime := time.Now()
	// apiserver lease 控制器创建的租约应带续约时间与时长；缺失这些字段的租约非法，应被回收。
	return lease.Spec.RenewTime == nil ||
		lease.Spec.LeaseDurationSeconds == nil ||
		lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second).Before(currentTime)
}
