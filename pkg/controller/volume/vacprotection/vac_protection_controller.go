/*
Copyright 2024 The Kubernetes Authors.

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

package vacprotection

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/util/protectionutil"
	"k8s.io/kubernetes/pkg/util/slice"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/ptr"
)

const (
	vacNameKeyIndex = "volumeAttributesClassName"
)

// Controller 负责给未被任何 PV 或 PVC 使用的 VolumeAttributesClass（VAC）
// 添加或移除 VACProtectionFinalizer，防止仍有对象引用的 VAC 被提前删除。
//
// 本控制器只依赖 informer 缓存，没有向 API Server 发起 Get/List 请求，因此存在
// finalizer 被过早移除的窗口：informer 事件到达的先后顺序无法保证。
//
// 一种典型场景：
//  1. 存在一个未被任何 PVC 使用的 VAC，它已同步到所有 informer
//     （external-provisioner、external-resizer、KCM）
//
//  2. 与此同时：
//
//       * 用户创建了引用该 VAC 的 PVC。
//
//       * 另一个用户删除了该 VAC。
//
//  3. 带 DeletionTimestamp 的 VAC 删除事件先到达本控制器；由于 PVC 创建事件
//     尚未同步到 KCM 的 informer，控制器误判 VAC 未被使用，移除 finalizer
//     使其被删除。随后 PVC 创建事件先于 VAC 删除事件到达 external-provisioner，
//     后者会尝试使用一个即将被删除的 VAC 来制备卷。
//
//       * 如果 external-provisioner 在 informer 中先看到 VAC 再看到删除，
//     制备会成功。
//
//       * 反之，external-provisioner 会因找不到 VAC 而制备失败。
//
// 要彻底解决这一竞态，需要在处理时向 API Server 发起 Get/List 请求；但在大规模
// 集群中这会带来性能问题，与既有 PVC 保护控制器面临的情况类似，详见
// https://github.com/kubernetes/kubernetes/issues/109282

// Controller 通过 VAC 保护 finalizer 延迟删除仍被 PV 或 PVC 引用的 VolumeAttributesClass。
type Controller struct {
	client clientset.Interface

	pvcSynced cache.InformerSynced
	pvSynced  cache.InformerSynced
	vacLister storagelisters.VolumeAttributesClassLister
	vacSynced cache.InformerSynced

	getPVsAssignedToVAC  func(vacName string) ([]*v1.PersistentVolume, error)
	getPVCsAssignedToVAC func(vacName string) ([]*v1.PersistentVolumeClaim, error)

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewVACProtectionController 构造控制器：为 VAC 注册增删事件处理，
// 为 PV/PVC informer 建立按引用 VAC 名称的索引，并挂接 PV/PVC 的更新与删除事件。
// 索引使 isBeingUsed 无需遍历所有 PV/PVC 即可判断某个 VAC 是否被引用。
func NewVACProtectionController(logger klog.Logger,
	client clientset.Interface,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	vacInformer storageinformers.VolumeAttributesClassInformer) (*Controller, error) {
	c := &Controller{
		client:    client,
		pvcSynced: pvcInformer.Informer().HasSynced,
		pvSynced:  pvInformer.Informer().HasSynced,
		vacLister: vacInformer.Lister(),
		vacSynced: vacInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "vacprotection",
			},
		),
	}

	_, err := vacInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.vacAddedUpdated(logger, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			c.vacAddedUpdated(logger, new)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler to VAC informer: %w", err)
	}

	err = pvInformer.Informer().AddIndexers(cache.Indexers{vacNameKeyIndex: func(obj interface{}) ([]string, error) {
		pv, ok := obj.(*v1.PersistentVolume)
		if !ok {
			return []string{}, nil
		}
		return getPVReferencedVACNames(pv), nil
	}})
	if err != nil {
		return nil, fmt.Errorf("failed to add index to PV informer: %w", err)
	}

	pvIndexer := pvInformer.Informer().GetIndexer()
	c.getPVsAssignedToVAC = func(vacName string) ([]*v1.PersistentVolume, error) {
		objs, err := pvIndexer.ByIndex(vacNameKeyIndex, vacName)
		if err != nil {
			return nil, err
		}
		pvcs := make([]*v1.PersistentVolume, 0, len(objs))
		for _, obj := range objs {
			pvc, ok := obj.(*v1.PersistentVolume)
			if !ok {
				continue
			}
			pvcs = append(pvcs, pvc)
		}
		return pvcs, nil
	}

	err = pvcInformer.Informer().AddIndexers(cache.Indexers{vacNameKeyIndex: func(obj interface{}) ([]string, error) {
		pvc, ok := obj.(*v1.PersistentVolumeClaim)
		if !ok {
			return []string{}, nil
		}
		return getPVCReferencedVACNames(pvc), nil
	}})
	if err != nil {
		return nil, fmt.Errorf("failed to add index to PVC informer: %w", err)
	}

	pvcIndexer := pvcInformer.Informer().GetIndexer()
	c.getPVCsAssignedToVAC = func(vacName string) ([]*v1.PersistentVolumeClaim, error) {
		objs, err := pvcIndexer.ByIndex(vacNameKeyIndex, vacName)
		if err != nil {
			return nil, err
		}
		pvcs := make([]*v1.PersistentVolumeClaim, 0, len(objs))
		for _, obj := range objs {
			pvc, ok := obj.(*v1.PersistentVolumeClaim)
			if !ok {
				continue
			}
			pvcs = append(pvcs, pvc)
		}
		return pvcs, nil
	}

	_, err = pvcInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			c.pvcUpdated(logger, old, new)
		},
		DeleteFunc: func(obj interface{}) {
			c.pvcDeleted(logger, obj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler to PVC informer: %w", err)
	}

	_, err = pvInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			c.pvUpdated(logger, old, new)
		},
		DeleteFunc: func(obj interface{}) {
			c.pvDeleted(logger, obj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("failed to add event handler to PV informer: %w", err)
	}
	return c, nil
}

// Run 等待三个 informer 同步完成后启动 workers 个 worker goroutine，
// 并阻塞直到 ctx 被取消；退出时先关闭队列，再等待所有 worker 结束。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting VAC protection controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down VAC protection controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.pvSynced, c.pvcSynced, c.vacSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 循环调用 processNextWorkItem 处理队列，直到队列关闭（processNextWorkItem 返回 false）。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 从队列中取出一个 VAC 名称并调用 processVAC 处理。
// 成功时调用 Forget 清除失败计数；失败时记录错误并按限速重新入队，保证最终一致。
// 队列关闭后返回 false，worker 随之退出。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	vacKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(vacKey)

	err := c.processVAC(ctx, vacKey)
	if err == nil {
		c.queue.Forget(vacKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("VAC %v failed with : %w", vacKey, err))
	c.queue.AddRateLimited(vacKey)

	return true
}

// processVAC 处理单个 VAC：待删除候选对象先检查是否仍被引用，未被引用则移除
// finalizer 允许删除；非删除对象若缺少 finalizer 则补上，防止后续被误删。
// VAC 已从缓存中消失时说明已被删除，直接忽略即可。
func (c *Controller) processVAC(ctx context.Context, vacName string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Processing VAC", "VAC", klog.KRef("", vacName))
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished processing VAC", "VAC", klog.KRef("", vacName), "cost", time.Since(startTime))
	}()

	vac, err := c.vacLister.Get(vacName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(4).Info("VAC not found, ignoring", "VAC", klog.KRef("", vacName))
			return nil
		}
		return err
	}

	if protectionutil.IsDeletionCandidate(vac, volumeutil.VACProtectionFinalizer) {
		// VAC 正在等待删除：只有确认没有 PV/PVC 引用后才能放行，
		// 否则保留 finalizer 阻止删除。
		isUsed := c.isBeingUsed(ctx, vac)
		if !isUsed {
			return c.removeFinalizer(ctx, vac)
		}
		logger.V(4).Info("Keeping VAC because it is being used", "PVC", klog.KRef("", vacName))
	}

	if protectionutil.NeedToAddFinalizer(vac, volumeutil.VACProtectionFinalizer) {
		return c.addFinalizer(ctx, vac)
	}

	return nil
}

// addFinalizer 在 VAC 的副本上追加保护 finalizer 并更新回 API Server。
// 必须在副本上修改，避免写回 informer 缓存中的共享对象。
func (c *Controller) addFinalizer(ctx context.Context, vac *storagev1.VolumeAttributesClass) error {
	vacClone := vac.DeepCopy()
	vacClone.ObjectMeta.Finalizers = append(vacClone.ObjectMeta.Finalizers, volumeutil.VACProtectionFinalizer)
	_, err := c.client.StorageV1().VolumeAttributesClasses().Update(ctx, vacClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(3).Info("Error adding protection finalizer to VAC", "VAC", klog.KObj(vac), "err", err)
		return err
	}
	logger.V(3).Info("Added protection finalizer to VAC", "VAC", klog.KObj(vac))
	return nil
}

// removeFinalizer 从 VAC 的副本上移除保护 finalizer 并更新回 API Server，
// 使 Kubernetes 能够真正完成删除。使用副本的原因同 addFinalizer。
func (c *Controller) removeFinalizer(ctx context.Context, vac *storagev1.VolumeAttributesClass) error {
	vacClone := vac.DeepCopy()
	vacClone.ObjectMeta.Finalizers = slice.RemoveString(vacClone.ObjectMeta.Finalizers, volumeutil.VACProtectionFinalizer, nil)
	_, err := c.client.StorageV1().VolumeAttributesClasses().Update(ctx, vacClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(3).Info("Error removing protection finalizer from VAC", "VAC", klog.KObj(vac), "err", err)
		return err
	}
	logger.V(3).Info("Removed protection finalizer from VAC", "VAC", klog.KObj(vac))
	return nil
}

// isBeingUsed 通过 informer 索引查找引用该 VAC 的 PV 和 PVC。
// 查询出错时保守地返回 true：宁可保留 finalizer 让删除卡住，也不能误删
// 仍被引用的 VAC；索引是缓存视图，所以这一判断可能滞后于真实状态。
func (c *Controller) isBeingUsed(ctx context.Context, vac *storagev1.VolumeAttributesClass) bool {
	logger := klog.FromContext(ctx)

	pvs, err := c.getPVsAssignedToVAC(vac.Name)
	if err != nil {
		logger.Error(err, "Error getting PVs assigned to VAC", "VAC", klog.KObj(vac))
		return true
	}
	if len(pvs) > 0 {
		return true
	}

	pvcs, err := c.getPVCsAssignedToVAC(vac.Name)
	if err != nil {
		logger.Error(err, "Error getting PVCs assigned to VAC", "VAC", klog.KObj(vac))
		return true
	}
	if len(pvcs) > 0 {
		return true
	}
	return false
}

// vacAddedUpdated 响应 VAC 的新增与更新事件：只在需要添加或移除 finalizer
// 时才入队，避免处理无关的状态变化。
func (c *Controller) vacAddedUpdated(logger klog.Logger, obj interface{}) {
	vac, ok := obj.(*storagev1.VolumeAttributesClass)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("VAC informer returned non-VAC object: %#v", obj))
		return
	}
	logger.V(4).Info("Got event on VAC", "VAC", klog.KObj(vac))

	if protectionutil.NeedToAddFinalizer(vac, volumeutil.VACProtectionFinalizer) || protectionutil.IsDeletionCandidate(vac, volumeutil.VACProtectionFinalizer) {
		c.queue.Add(vac.Name)
	}
}

// pvcDeleted 响应 PVC 删除事件：把该 PVC 引用过的所有 VAC 入队，
// 使原本被保护而等待删除的 VAC 有机会移除 finalizer。
func (c *Controller) pvcDeleted(logger klog.Logger, obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PVC informer returned non-PVC object: %#v", obj))
		return
	}
	logger.V(4).Info("Got event on PVC", "PVC", klog.KObj(pvc))
	vacNames := getPVCReferencedVACNames(pvc)
	for _, vacName := range vacNames {
		c.queue.Add(vacName)
	}
}

// pvcUpdated 响应 PVC 更新事件：只把从旧对象中不再被引用、且新对象中也未引用
// 的 VAC 入队。取“旧减新”的差集是因为只有引用减少才可能释放 VAC；
// 新增引用由 VAC 自身的添加 finalizer 流程处理。
func (c *Controller) pvcUpdated(logger klog.Logger, old, new interface{}) {
	oldPVC, ok := old.(*v1.PersistentVolumeClaim)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PVC informer returned non-PVC object: %#v", old))
		return
	}
	newPVC, ok := new.(*v1.PersistentVolumeClaim)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PVC informer returned non-PVC object: %#v", new))
		return
	}

	logger.V(4).Info("Got event on PVC", "PVC", klog.KObj(newPVC))

	vavNames := sets.New(getPVCReferencedVACNames(oldPVC)...).Delete(getPVCReferencedVACNames(newPVC)...).UnsortedList()
	for _, vacName := range vavNames {
		c.queue.Add(vacName)
	}
}

// pvUpdated 响应 PV 更新事件：把从旧对象到新对象之间不再被引用的 VAC 入队，
// 与 pvcUpdated 同理，仅引用减少才需要重新检查 finalizer。
func (c *Controller) pvUpdated(logger klog.Logger, old, new interface{}) {
	oldPV, ok := old.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", old))
		return
	}
	newPV, ok := new.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", new))
		return
	}

	logger.V(4).Info("Got event on PV", "PV", klog.KObj(newPV))
	vavNames := sets.New(getPVReferencedVACNames(oldPV)...).Delete(getPVReferencedVACNames(newPV)...).UnsortedList()
	for _, vacName := range vavNames {
		c.queue.Add(vacName)
	}
}

// pvDeleted 响应 PV 删除事件：把被删 PV 引用过的所有 VAC 入队，
// 以便释放那些因该 PV 存在而保留的 finalizer。
func (c *Controller) pvDeleted(logger klog.Logger, obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", obj))
		return
	}
	logger.V(4).Info("Got event on PV", "PV", klog.KObj(pv))
	vacNames := getPVReferencedVACNames(pv)
	for _, vacName := range vacNames {
		c.queue.Add(vacName)
	}
}

// getPVCReferencedVACNames 收集 PVC 的 spec、status 当前值和 modify 目标值中
// 出现的所有 VAC 名称并用 set 去重：一个 PVC 可能在不同阶段同时引用多个 VAC，
// 任一引用都意味着对应 VAC 不能被删除。
func getPVCReferencedVACNames(pvc *v1.PersistentVolumeClaim) []string {
	keys := sets.New[string]()
	vacName := ptr.Deref(pvc.Spec.VolumeAttributesClassName, "")
	if vacName != "" {
		keys.Insert(vacName)
	}
	vacName = ptr.Deref(pvc.Status.CurrentVolumeAttributesClassName, "")
	if vacName != "" {
		keys.Insert(vacName)
	}
	status := pvc.Status.ModifyVolumeStatus
	if status != nil && status.TargetVolumeAttributesClassName != "" {
		keys.Insert(status.TargetVolumeAttributesClassName)
	}
	return keys.UnsortedList()
}

// getPVReferencedVACNames 返回 PV 的 spec 中引用的 VAC 名称；
// PV 只会引用一个 VAC，所以这里无需像 PVC 那样收集三处字段。
func getPVReferencedVACNames(pv *v1.PersistentVolume) []string {
	result := []string{}
	vacName := ptr.Deref(pv.Spec.VolumeAttributesClassName, "")
	if vacName != "" {
		result = append(result, vacName)
	}
	return result
}
