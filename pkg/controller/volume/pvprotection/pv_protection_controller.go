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

package pvprotection

import (
	"context"
	"fmt"
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
	"k8s.io/kubernetes/pkg/controller/util/protectionutil"
	"k8s.io/kubernetes/pkg/util/slice"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// Controller 负责从不再被 PVC 使用的 PV 上移除 PVProtectionFinalizer，
// 并为缺少该 finalizer 的存量 PV 补加，防止 PV 被使用时被误删除。
type Controller struct {
	client clientset.Interface

	pvLister       corelisters.PersistentVolumeLister
	pvListerSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewPVProtectionController 创建 PV 保护控制器，注册 PV 的 Add/Update 事件处理函数。
func NewPVProtectionController(logger klog.Logger, pvInformer coreinformers.PersistentVolumeInformer, cl clientset.Interface) (*Controller, error) {
	e := &Controller{
		client: cl,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "pvprotection"},
		),
	}

	e.pvLister = pvInformer.Lister()
	e.pvListerSynced = pvInformer.Informer().HasSynced
	// Add 与 Update 共用处理逻辑：两者都只关心对象当前状态，
	// 删除事件无需入队（finalizer 保证删除前会先收到 Update）。
	_, err := pvInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e.pvAddedUpdated(logger, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			e.pvAddedUpdated(logger, new)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add PV event handler: %w", err)
	}

	return e, nil
}

// Run 启动控制器：先等待缓存同步，再拉起 worker，直到 context 取消。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting PV protection controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down PV protection controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.pvListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 持续处理工作队列，直到队列被关闭（processNextWorkItem 返回 false）。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 PV key；队列关闭时返回 false。
// 失败时通过限速队列重试，避免热点 PV 无限快速重试。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	pvKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(pvKey)

	pvName := pvKey

	err := c.processPV(ctx, pvName)
	if err == nil {
		c.queue.Forget(pvKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("PV %v failed with : %v", pvKey, err))
	c.queue.AddRateLimited(pvKey)

	return true
}

// processPV 根据 PV 状态维护 finalizer：删除中的未使用 PV 移除 finalizer；
// 普通 PV 若缺 finalizer 则补加（兼容启用 admission 插件前创建的存量 PV）。
func (c *Controller) processPV(ctx context.Context, pvName string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Processing PV", "PV", klog.KRef("", pvName))
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished processing PV", "PV", klog.KRef("", pvName), "cost", time.Since(startTime))
	}()

	pv, err := c.pvLister.Get(pvName)
	if apierrors.IsNotFound(err) {
		logger.V(4).Info("PV not found, ignoring", "PV", klog.KRef("", pvName))
		return nil
	}
	if err != nil {
		return err
	}

	if protectionutil.IsDeletionCandidate(pv, volumeutil.PVProtectionFinalizer) {
		// PV 正在被删除：先确认是否仍被使用，未使用才能移除 finalizer 放行删除
		isUsed := c.isBeingUsed(pv)
		if !isUsed {
			return c.removeFinalizer(ctx, pv)
		}
		logger.V(4).Info("Keeping PV because it is being used", "PV", klog.KRef("", pvName))
	}

	if protectionutil.NeedToAddFinalizer(pv, volumeutil.PVProtectionFinalizer) {
		// 正常 PV 应带 finalizer；通常由 admission 插件添加，这里兜底处理
		// 插件启用前创建的存量 PV。
		return c.addFinalizer(ctx, pv)
	}
	return nil
}

// addFinalizer 在深拷贝上追加保护 finalizer 后更新，避免污染 informer 缓存中的对象。
func (c *Controller) addFinalizer(ctx context.Context, pv *v1.PersistentVolume) error {
	pvClone := pv.DeepCopy()
	pvClone.ObjectMeta.Finalizers = append(pvClone.ObjectMeta.Finalizers, volumeutil.PVProtectionFinalizer)
	_, err := c.client.CoreV1().PersistentVolumes().Update(ctx, pvClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(3).Info("Error adding protection finalizer to PV", "PV", klog.KObj(pv), "err", err)
		return err
	}
	logger.V(3).Info("Added protection finalizer to PV", "PV", klog.KObj(pv))
	return nil
}

// removeFinalizer 在深拷贝上移除保护 finalizer 后更新，避免污染 informer 缓存。
func (c *Controller) removeFinalizer(ctx context.Context, pv *v1.PersistentVolume) error {
	pvClone := pv.DeepCopy()
	pvClone.ObjectMeta.Finalizers = slice.RemoveString(pvClone.ObjectMeta.Finalizers, volumeutil.PVProtectionFinalizer, nil)
	_, err := c.client.CoreV1().PersistentVolumes().Update(ctx, pvClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(3).Info("Error removing protection finalizer from PV", "PV", klog.KObj(pv), "err", err)
		return err
	}
	logger.V(3).Info("Removed protection finalizer from PV", "PV", klog.KObj(pv))
	return nil
}

// isBeingUsed 依据 PV status 判断是否仍被绑定使用；该状态由 PV controller 维护，
// 这里不直接查 PVC，避免跨对象依赖和额外 API 调用。
func (c *Controller) isBeingUsed(pv *v1.PersistentVolume) bool {
	// 只有 Bound 相位代表仍在使用中
	if pv.Status.Phase == v1.VolumeBound {
		// the PV is being used now
		return true
	}

	return false
}

// pvAddedUpdated 处理 PV 新增/更新事件：仅在需要增删 finalizer 时才入队，
// 避免无关事件造成无谓的 API 更新。
func (c *Controller) pvAddedUpdated(logger klog.Logger, obj interface{}) {
	// informer 理论上只会给出 PV，类型断言失败属于异常情况，记录后跳过
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", obj))
		return
	}
	logger.V(4).Info("Got event on PV", "PV", klog.KObj(pv))

	if protectionutil.NeedToAddFinalizer(pv, volumeutil.PVProtectionFinalizer) || protectionutil.IsDeletionCandidate(pv, volumeutil.PVProtectionFinalizer) {
		c.queue.Add(pv.Name)
	}
}
