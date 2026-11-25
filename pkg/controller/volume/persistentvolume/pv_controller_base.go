/*
Copyright 2016 The Kubernetes Authors.

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

package persistentvolume

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume/metrics"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/slice"
	vol "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	"k8s.io/klog/v2"
)

// 本文件包含控制器的基础框架，即处理 PV/PVC 添加/更新/删除事件的通用逻辑。
// 真正的绑定、制备、回收与删除操作在 pv_controller.go 中实现。

// ControllerParameters 保存创建新 PersistentVolume 控制器所需的参数。
type ControllerParameters struct {
	KubeClient                clientset.Interface
	SyncPeriod                time.Duration
	VolumePlugins             []vol.VolumePlugin
	VolumeInformer            coreinformers.PersistentVolumeInformer
	ClaimInformer             coreinformers.PersistentVolumeClaimInformer
	ClassInformer             storageinformers.StorageClassInformer
	PodInformer               coreinformers.PodInformer
	NodeInformer              coreinformers.NodeInformer
	EnableDynamicProvisioning bool
}

// NewController 创建并初始化一个新的 PersistentVolume 控制器。
func NewController(ctx context.Context, p ControllerParameters) (*PersistentVolumeController, error) {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "persistentvolume-controller"})

	controller := &PersistentVolumeController{
		volumes:                       newPersistentVolumeOrderedIndex(),
		claims:                        cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		kubeClient:                    p.KubeClient,
		eventBroadcaster:              eventBroadcaster,
		eventRecorder:                 eventRecorder,
		runningOperations:             goroutinemap.NewGoRoutineMap(true /* exponentialBackOffOnError */),
		enableDynamicProvisioning:     p.EnableDynamicProvisioning,
		createProvisionedPVRetryCount: createProvisionedPVRetryCount,
		createProvisionedPVInterval:   createProvisionedPVInterval,
		claimQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "claims"},
		),
		volumeQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "volumes"},
		),
		resyncPeriod:        p.SyncPeriod,
		operationTimestamps: metrics.NewOperationStartTimeCache(),
	}

	// 这里传入 nil 作为 prober，因为 PV 控制器不感知 Flexvolume。
	if err := controller.volumePluginMgr.InitPlugins(p.VolumePlugins, nil /* prober */, controller); err != nil {
		return nil, fmt.Errorf("could not initialize volume plugins for PersistentVolume Controller: %w", err)
	}

	logger := klog.FromContext(ctx)
	_, err := p.VolumeInformer.Informer().AddEventHandlerWithOptions(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { controller.enqueueWork(ctx, controller.volumeQueue, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { controller.enqueueWork(ctx, controller.volumeQueue, newObj) },
			DeleteFunc: func(obj interface{}) { controller.enqueueWork(ctx, controller.volumeQueue, obj) },
		},
		cache.HandlerOptions{Logger: &logger},
	)
	if err != nil {
		return nil, fmt.Errorf("could not add volume event handler: %w", err)
	}
	controller.volumeLister = p.VolumeInformer.Lister()
	controller.volumeListerSynced = p.VolumeInformer.Informer().HasSynced

	_, err = p.ClaimInformer.Informer().AddEventHandlerWithOptions(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { controller.enqueueWork(ctx, controller.claimQueue, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { controller.enqueueWork(ctx, controller.claimQueue, newObj) },
			DeleteFunc: func(obj interface{}) { controller.enqueueWork(ctx, controller.claimQueue, obj) },
		},
		cache.HandlerOptions{Logger: &logger},
	)
	if err != nil {
		return nil, fmt.Errorf("could not add claim event handler: %w", err)
	}
	controller.claimLister = p.ClaimInformer.Lister()
	controller.claimListerSynced = p.ClaimInformer.Informer().HasSynced

	controller.classLister = p.ClassInformer.Lister()
	controller.classListerSynced = p.ClassInformer.Informer().HasSynced
	controller.podLister = p.PodInformer.Lister()
	controller.podIndexer = p.PodInformer.Informer().GetIndexer()
	controller.podListerSynced = p.PodInformer.Informer().HasSynced
	controller.NodeLister = p.NodeInformer.Lister()
	controller.NodeListerSynced = p.NodeInformer.Informer().HasSynced

	// 建立按 PVC 键索引 Pod 的自定义索引器，这样查找引用某 PVC 的 Pod 时无需遍历全部 Pod。
	if err := common.AddPodPVCIndexerIfNotPresent(controller.podIndexer); err != nil {
		return nil, fmt.Errorf("could not initialize attach detach controller: %w", err)
	}

	csiTranslator := csitrans.New()
	controller.translator = csiTranslator
	controller.csiMigratedPluginManager = csimigration.NewPluginManager(csiTranslator)

	return controller, nil
}

// initializeCaches 用 etcd 中的初始数据填充控制器缓存，
// 确保首次 addClaim/addVolume 事件到来前缓存已就绪，从而完成控制器的初始同步。
func (ctrl *PersistentVolumeController) initializeCaches(logger klog.Logger, volumeLister corelisters.PersistentVolumeLister, claimLister corelisters.PersistentVolumeClaimLister) {
	volumeList, err := volumeLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "PersistentVolumeController can't initialize caches")
		return
	}
	for _, volume := range volumeList {
		volumeClone := volume.DeepCopy()
		if _, err = ctrl.storeVolumeUpdate(logger, volumeClone); err != nil {
			logger.Error(err, "Error updating volume cache")
		}
	}

	claimList, err := claimLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "PersistentVolumeController can't initialize caches")
		return
	}
	for _, claim := range claimList {
		if _, err = ctrl.storeClaimUpdate(logger, claim.DeepCopy()); err != nil {
			logger.Error(err, "Error updating claim cache")
		}
	}
	logger.V(4).Info("Controller initialized")
}

// enqueueWork 把卷或声明的名字加入给定的工作队列。
func (ctrl *PersistentVolumeController) enqueueWork(ctx context.Context, queue workqueue.TypedInterface[string], obj interface{}) {
	// 注意处理“对象已删除”事件：DeltaFIFO 可能用 DeletedFinalStateUnknown 包装对象。
	logger := klog.FromContext(ctx)
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	objName, err := controller.KeyFunc(obj)
	if err != nil {
		logger.Error(err, "Failed to get key from object")
		return
	}
	logger.V(5).Info("Enqueued for sync", "objName", objName)
	queue.Add(objName)
}

// storeVolumeUpdate 把卷的新版本写入控制器缓存，返回是否为新版本。
func (ctrl *PersistentVolumeController) storeVolumeUpdate(logger klog.Logger, volume interface{}) (bool, error) {
	return storeObjectUpdate(logger, ctrl.volumes.store, volume, "volume")
}

// storeClaimUpdate 把声明的新版本写入控制器缓存，返回是否为新版本。
func (ctrl *PersistentVolumeController) storeClaimUpdate(logger klog.Logger, claim interface{}) (bool, error) {
	return storeObjectUpdate(logger, ctrl.claims, claim, "claim")
}

// updateVolume 在 worker 线程中运行，处理“卷添加”“卷更新”和“周期性同步”事件：
// 仅当缓存写入的是新版本时才执行同步。
func (ctrl *PersistentVolumeController) updateVolume(ctx context.Context, volume *v1.PersistentVolume) error {
	// 先把新版本写入缓存；若是旧版本则跳过处理。
	logger := klog.FromContext(ctx)
	new, err := ctrl.storeVolumeUpdate(logger, volume)
	if err != nil {
		logger.Error(err, "Error storing volume update")
		return err
	}
	if !new {
		return nil
	}

	err = ctrl.syncVolume(ctx, volume)
	if err != nil {
		if errors.IsConflict(err) {
			// 版本冲突很常见，控制器可直接从该状态恢复。
			logger.V(3).Info("Could not sync volume", "volumeName", volume.Name, "err", err)
		} else {
			logger.Error(err, "Could not sync volume", "volumeName", volume.Name)
		}
		return err
	}
	return nil
}

// deleteVolume 在 worker 线程中运行，处理“卷删除”事件，
// 清理缓存并记录删除耗时指标，同时触发关联声明的同步。
func (ctrl *PersistentVolumeController) deleteVolume(ctx context.Context, volume *v1.PersistentVolume) {
	logger := klog.FromContext(ctx)
	if err := ctrl.volumes.store.Delete(volume); err != nil {
		logger.Error(err, "Volume deletion encountered", "volumeName", volume.Name)
	} else {
		logger.V(4).Info("volume deleted", "volumeName", volume.Name)
	}
	// 若缓存中存在该卷的删除起始时间则记录删除耗时指标；
	// 缓存中无记录时该调用为空操作，RecordMetric 会负责清理时间戳缓存项。
	metrics.RecordMetric(volume.Name, &ctrl.operationTimestamps, nil)

	if volume.Spec.ClaimRef == nil {
		return
	}
	// 卷被删除时主动同步其关联声明，避免声明一直等到下个同步周期才更新为 Lost 状态。
	claimKey := claimrefToClaimKey(volume.Spec.ClaimRef)
	logger.V(5).Info("deleteVolume: scheduling sync of claim", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
	ctrl.claimQueue.Add(claimKey)
}

// updateClaim 在 worker 线程中运行，处理“声明添加”“声明更新”和“周期性同步”事件：
// 仅当缓存写入的是新版本时才执行同步。
func (ctrl *PersistentVolumeController) updateClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) error {
	// 先把新版本写入缓存；若是旧版本则跳过处理。
	logger := klog.FromContext(ctx)
	new, err := ctrl.storeClaimUpdate(logger, claim)
	if err != nil {
		logger.Error(err, "Error storing claim update")
		return err
	}
	if !new {
		return nil
	}
	err = ctrl.syncClaim(ctx, claim)
	if err != nil {
		if errors.IsConflict(err) {
			// 版本冲突很常见，控制器可直接从该状态恢复。
			logger.V(3).Info("Could not sync claim", "PVC", klog.KObj(claim), "err", err)
		} else {
			logger.Error(err, "Could not sync claim", "PVC", klog.KObj(claim))
		}
		return err
	}
	return nil
}

// 单元测试 [5-5] [5-6] [5-7]
// deleteClaim 在 worker 线程中运行，处理“声明删除”事件，
// 清理缓存与时间戳，并触发关联卷的同步。
func (ctrl *PersistentVolumeController) deleteClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) {
	logger := klog.FromContext(ctx)
	if err := ctrl.claims.Delete(claim); err != nil {
		logger.Error(err, "Claim deletion encountered", "PVC", klog.KObj(claim))
	}
	claimKey := claimToClaimKey(claim)
	logger.V(4).Info("Claim deleted", "PVC", klog.KObj(claim))
	// 清理可能残留的未完成的制备起始时间戳。
	// 单元测试 [5-8] [5-9]
	ctrl.operationTimestamps.Delete(claimKey)

	volumeName := claim.Spec.VolumeName
	if volumeName == "" {
		logger.V(5).Info("deleteClaim: volume not bound", "PVC", klog.KObj(claim))
		return
	}

	// 声明被删除时主动同步其关联卷，避免卷一直等到下个同步周期才进入 Released 状态。
	logger.V(5).Info("deleteClaim: scheduling sync of volume", "PVC", klog.KObj(claim), "volumeName", volumeName)
	ctrl.volumeQueue.Add(volumeName)
}

// Run 启动本控制器的所有控制循环，直到 ctx 结束。
func (ctrl *PersistentVolumeController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线。
	ctrl.eventBroadcaster.StartStructuredLogging(3)
	ctrl.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: ctrl.kubeClient.CoreV1().Events("")})
	defer ctrl.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting persistent volume controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down persistent volume controller")
		ctrl.claimQueue.ShutDown()
		ctrl.volumeQueue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, ctrl.volumeListerSynced, ctrl.claimListerSynced, ctrl.classListerSynced, ctrl.podListerSynced, ctrl.NodeListerSynced) {
		return
	}

	ctrl.initializeCaches(logger, ctrl.volumeLister, ctrl.claimLister)
	metrics.Register(ctrl.volumes.store, ctrl.claims, &ctrl.volumePluginMgr)
	volumeutil.RegisterMetrics()

	wg.Go(func() {
		wait.Until(func() { ctrl.resync(ctx) }, ctrl.resyncPeriod, ctx.Done())
	})
	wg.Go(func() {
		wait.UntilWithContext(ctx, ctrl.volumeWorker, time.Second)
	})
	wg.Go(func() {
		wait.UntilWithContext(ctx, ctrl.claimWorker, time.Second)
	})
	<-ctx.Done()
}

// updateClaimMigrationAnnotations 修复声明上的 CSI 迁移注解；无变化时返回副本不写 API。
func (ctrl *PersistentVolumeController) updateClaimMigrationAnnotations(ctx context.Context,
	claim *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	// TODO: 优化 update[Claim|Volume]MigrationAnnotations，无需修改时避免复制 claim/volume。
	// 但这需要重构，并会改变函数语义：不复制时，返回值可能时而为副本、时而为原对象引用，
	// 这种不确定性可能不受欢迎。
	// 先深拷贝声明，避免直接修改 informer 缓存中的对象。
	claimClone := claim.DeepCopy()
	logger := klog.FromContext(ctx)
	modified := updateMigrationAnnotations(logger, ctrl.csiMigratedPluginManager, ctrl.translator, claimClone.Annotations, true)
	if !modified {
		return claimClone, nil
	}
	newClaim, err := ctrl.kubeClient.CoreV1().PersistentVolumeClaims(claimClone.Namespace).Update(ctx, claimClone, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("persistent Volume Controller can't anneal migration annotations: %v", err)
	}
	_, err = ctrl.storeClaimUpdate(logger, newClaim)
	if err != nil {
		return nil, fmt.Errorf("persistent Volume Controller can't anneal migration annotations: %v", err)
	}
	return newClaim, nil
}

// updateVolumeMigrationAnnotationsAndFinalizers 修复卷上的迁移注解与删除保护 finalizer；
// 无变化时返回副本不写 API。
func (ctrl *PersistentVolumeController) updateVolumeMigrationAnnotationsAndFinalizers(ctx context.Context,
	volume *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	volumeClone := volume.DeepCopy()
	logger := klog.FromContext(ctx)
	annModified := updateMigrationAnnotations(logger, ctrl.csiMigratedPluginManager, ctrl.translator, volumeClone.Annotations, false)
	modifiedFinalizers, finalizersModified := modifyDeletionFinalizers(logger, ctrl.csiMigratedPluginManager, volumeClone)
	if !annModified && !finalizersModified {
		return volumeClone, nil
	}
	if finalizersModified {
		volumeClone.ObjectMeta.SetFinalizers(modifiedFinalizers)
	}
	newVol, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Update(ctx, volumeClone, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("persistent Volume Controller can't anneal migration annotations or finalizer: %v", err)
	}
	_, err = ctrl.storeVolumeUpdate(logger, newVol)
	if err != nil {
		return nil, fmt.Errorf("persistent Volume Controller can't anneal migration annotations or finalizer: %v", err)
	}
	return newVol, nil
}

// modifyDeletionFinalizers 根据回收策略以及是否为 in-tree 卷来更新 finalizer 列表：
// 仅当 PV 的回收策略为 Delete 时才添加 in-tree 删除保护 finalizer；
// 回收策略为 Retain 或 Recycle 时移除它，以反映 PV 上回收策略的更新。
// 同时移除外部 PV 删除保护 finalizer，以处理 CSI 迁移回滚/禁用场景。
func modifyDeletionFinalizers(logger klog.Logger, cmpm CSIMigratedPluginManager, volume *v1.PersistentVolume) ([]string, bool) {
	modified := false
	var outFinalizers []string
	if !metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnDynamicallyProvisioned) {
		// 删除保护 finalizer 目前仅对动态制备的卷有意义。
		return volume.Finalizers, false
	}
	if volume.Finalizers != nil {
		outFinalizers = append(outFinalizers, volume.Finalizers...)
	}
	provisioner := volume.Annotations[storagehelpers.AnnDynamicallyProvisioned]
	if cmpm.IsMigrationEnabledForPlugin(provisioner) {
		// 已启用 CSI 迁移时移除 PV 上的 in-tree 删除保护 finalizer。
		if slices.Contains(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer) {
			outFinalizers = slice.RemoveString(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer, nil)
			modified = true
		}
		return outFinalizers, modified
	}
	// 检查是否为 in-tree 卷。
	if !strings.HasPrefix(provisioner, "kubernetes.io/") {
		// 制备插件名不以已知 in-tree 插件前缀开头，不属于 in-tree 卷。
		return volume.Finalizers, false
	}
	reclaimPolicy := volume.Spec.PersistentVolumeReclaimPolicy
	// 若尚未存在，则补回 in-tree PV 删除保护 finalizer。
	if reclaimPolicy == v1.PersistentVolumeReclaimDelete && !slices.Contains(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer) {
		logger.V(4).Info("Adding in-tree pv deletion protection finalizer on volume", "volumeName", volume.Name)
		outFinalizers = append(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer)
		modified = true
	} else if (reclaimPolicy == v1.PersistentVolumeReclaimRetain || reclaimPolicy == v1.PersistentVolumeReclaimRecycle) && slices.Contains(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer) {
		// 回收策略为 Retain 或 Recycle 时移除 in-tree 删除保护 finalizer。
		logger.V(4).Info("Removing in-tree pv deletion protection finalizer on volume", "volumeName", volume.Name)
		outFinalizers = slice.RemoveString(outFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer, nil)
		modified = true
	}
	// 移除外部 PV 删除保护 finalizer。
	if slices.Contains(outFinalizers, storagehelpers.PVDeletionProtectionFinalizer) {
		logger.V(4).Info("Removing external pv deletion protection finalizer on volume", "volumeName", volume.Name)
		outFinalizers = slice.RemoveString(outFinalizers, storagehelpers.PVDeletionProtectionFinalizer, nil)
		modified = true
	}
	return outFinalizers, modified
}

// updateMigrationAnnotations 检查注解映射中的制备器名称，按功能开关判断是否启用 CSI 迁移：
// 启用时为对应制备器添加 "pv.kubernetes.io/migrated-to" 注解，
// 回滚场景下迁移关闭时移除该注解。返回注解映射是否被修改。
func updateMigrationAnnotations(logger klog.Logger, cmpm CSIMigratedPluginManager, translator CSINameTranslator, ann map[string]string, claim bool) bool {
	var csiDriverName string
	var err error

	if ann == nil {
		// 没有注解时无法得知制备器及迁移状态，不做修改。
		return false
	}
	var provisionerKey string
	if claim {
		provisionerKey = storagehelpers.AnnStorageProvisioner
	} else {
		provisionerKey = storagehelpers.AnnDynamicallyProvisioned
	}
	provisioner, ok := ann[provisionerKey]
	if !ok {
		if claim {
			// 对声明还要检查 beta 版制备器注解，确保兼容旧对象。
			provisioner, ok = ann[storagehelpers.AnnBetaStorageProvisioner]
			if !ok {
				return false
			}
		} else {
			// 卷是静态制备的，没有制备器注解。
			return false
		}
	}

	migratedToDriver := ann[storagehelpers.AnnMigratedTo]
	if cmpm.IsMigrationEnabledForPlugin(provisioner) {
		csiDriverName, err = translator.GetCSINameFromInTreeName(provisioner)
		if err != nil {
			logger.Error(err, "Could not update volume migration annotations. Migration enabled for plugin but could not find corresponding driver name", "plugin", provisioner)
			return false
		}
		if migratedToDriver != csiDriverName {
			ann[storagehelpers.AnnMigratedTo] = csiDriverName
			return true
		}
	} else {
		if migratedToDriver != "" {
			// 迁移注解仍存在但当前该功能已关闭，需移除注解。
			delete(ann, storagehelpers.AnnMigratedTo)
			return true
		}
	}
	return false
}

// volumeWorker 运行一个 worker 线程，从队列取出条目处理后标记完成，
// 保证同一 key 的 syncHandler 不会被并发调用。
func (ctrl *PersistentVolumeController) volumeWorker(ctx context.Context) {
	for ctrl.processNextVolumeWorkItem(ctx) {
	}
}

// processNextVolumeWorkItem 处理卷队列中的一个 key；队列关闭时返回 false。
func (ctrl *PersistentVolumeController) processNextVolumeWorkItem(ctx context.Context) bool {
	key, quit := ctrl.volumeQueue.Get()
	if quit {
		return false
	}
	defer ctrl.volumeQueue.Done(key)

	err := ctrl.syncVolumeByKey(ctx, key)
	if err == nil {
		ctrl.volumeQueue.Forget(key)
		return true
	}

	ctrl.volumeQueue.AddRateLimited(key)

	return true
}

// syncVolumeByKey 处理队列中 key 对应的单个卷：按 informer/本地缓存判断是更新还是删除。
func (ctrl *PersistentVolumeController) syncVolumeByKey(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("syncVolumeByKey", "volumeKey", key)

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.V(4).Info("Error getting name of volume to get volume from informer", "volumeKey", key, "err", err)
		return nil
	}

	volume, err := ctrl.volumeLister.Get(name)
	if err == nil {
		// 卷仍在 informer 缓存中，说明事件是添加/更新/同步。
		return ctrl.updateVolume(ctx, volume)
	}
	if !errors.IsNotFound(err) {
		logger.V(2).Info("Error getting volume from informer", "volumeKey", key, "err", err)
		return err
	}

	// 卷已不在 informer 缓存中，说明事件是删除。
	volumeObj, found, err := ctrl.volumes.store.GetByKey(key)
	if err != nil {
		logger.V(2).Info("Error getting volume from cache", "volumeKey", key, "err", err)
		return err
	}
	if !found {
		// 控制器已处理过删除事件，并已从缓存中删除该卷。
		logger.V(2).Info("Deletion of volume was already processed", "volumeKey", key)
		return nil
	}
	volume, ok := volumeObj.(*v1.PersistentVolume)
	if !ok {
		logger.Error(nil, "Expected volume, got", "obj", volumeObj)
		// 类型断言失败不重试：这属于编程错误，重试无意义。
		return nil
	}
	ctrl.deleteVolume(ctx, volume)
	return nil
}

// claimWorker 运行一个 worker 线程，从队列取出条目处理后标记完成，
// 保证同一 key 的 syncHandler 不会被并发调用。
func (ctrl *PersistentVolumeController) claimWorker(ctx context.Context) {
	for ctrl.processNextClaimWorkItem(ctx) {
	}
}

// processNextClaimWorkItem 处理声明队列中的一个 key；队列关闭时返回 false。
func (ctrl *PersistentVolumeController) processNextClaimWorkItem(ctx context.Context) bool {
	key, quit := ctrl.claimQueue.Get()
	if quit {
		return false
	}
	defer ctrl.claimQueue.Done(key)

	err := ctrl.syncClaimByKey(ctx, key)
	if err == nil {
		ctrl.claimQueue.Forget(key)
		return true
	}

	ctrl.claimQueue.AddRateLimited(key)

	return true
}

// syncClaimByKey 处理队列中 key 对应的单个声明：按 informer/本地缓存判断是更新还是删除。
func (ctrl *PersistentVolumeController) syncClaimByKey(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("syncClaimByKey", "claimKey", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.V(4).Info("Error getting namespace & name of claim to get claim from informer", "claimKey", key, "err", err)
		return nil
	}

	claim, err := ctrl.claimLister.PersistentVolumeClaims(namespace).Get(name)
	if err == nil {
		// 声明仍在 informer 缓存中，说明事件是添加/更新/同步。
		return ctrl.updateClaim(ctx, claim)
	}
	if !errors.IsNotFound(err) {
		logger.V(2).Info("Error getting claim from informer", "claimKey", key, "err", err)
		return err
	}

	// 声明已不在 informer 缓存中，说明事件是删除。
	claimObj, found, err := ctrl.claims.GetByKey(key)
	if err != nil {
		logger.V(2).Info("Error getting claim from cache", "claimKey", key, "err", err)
		return err
	}
	if !found {
		// 控制器已处理过删除事件，并已从缓存中删除该声明。
		logger.V(2).Info("Deletion of claim was already processed", "claimKey", key)
		return nil
	}
	claim, ok := claimObj.(*v1.PersistentVolumeClaim)
	if !ok {
		logger.Error(nil, "Expected claim, got", "obj", claimObj)
		// 类型断言失败不重试：这属于编程错误，重试无意义。
		return nil
	}
	ctrl.deleteClaim(ctx, claim)
	return nil
}

// resync 补充共享 informer 重同步周期较长的不足：不希望所有 PV/PVC 共享 informer
// 的使用者都被迫使用很短的重同步周期，因此本控制器自行周期性地把所有对象重新入队。
func (ctrl *PersistentVolumeController) resync(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Resyncing PV controller")

	pvcs, err := ctrl.claimLister.List(labels.NewSelector())
	if err != nil {
		logger.Info("Cannot list claims", "err", err)
		return
	}
	for _, pvc := range pvcs {
		ctrl.enqueueWork(ctx, ctrl.claimQueue, pvc)
	}

	pvs, err := ctrl.volumeLister.List(labels.NewSelector())
	if err != nil {
		logger.Info("Cannot list persistent volumes", "err", err)
		return
	}
	for _, pv := range pvs {
		ctrl.enqueueWork(ctx, ctrl.volumeQueue, pv)
	}
}

// setClaimProvisioner 把存储类制备器名称写入声明的
// volume.kubernetes.io/storage-provisioner 注解。
func (ctrl *PersistentVolumeController) setClaimProvisioner(ctx context.Context, claim *v1.PersistentVolumeClaim, provisionerName string) (*v1.PersistentVolumeClaim, error) {
	if val, ok := claim.Annotations[storagehelpers.AnnStorageProvisioner]; ok && val == provisionerName {
		// 注解已是目标值，无需更新。
		return claim, nil
	}

	// 参数中的声明可能指向 watcher 缓存对象，绝不能直接修改，必须先深拷贝。
	claimClone := claim.DeepCopy()
	// TODO: 弃用期结束后移除 beta 存储制备器注解。
	logger := klog.FromContext(ctx)
	metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, storagehelpers.AnnBetaStorageProvisioner, provisionerName)
	metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, storagehelpers.AnnStorageProvisioner, provisionerName)
	updateMigrationAnnotations(logger, ctrl.csiMigratedPluginManager, ctrl.translator, claimClone.Annotations, true)
	newClaim, err := ctrl.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(ctx, claimClone, metav1.UpdateOptions{})
	if err != nil {
		return newClaim, err
	}
	_, err = ctrl.storeClaimUpdate(logger, newClaim)
	if err != nil {
		return newClaim, err
	}
	return newClaim, nil
}

// 以下为无状态辅助函数。

// getClaimStatusForLogging 返回声明的关键状态字段，便于日志输出。
func getClaimStatusForLogging(claim *v1.PersistentVolumeClaim) string {
	bound := metav1.HasAnnotation(claim.ObjectMeta, storagehelpers.AnnBindCompleted)
	boundByController := metav1.HasAnnotation(claim.ObjectMeta, storagehelpers.AnnBoundByController)

	return fmt.Sprintf("phase: %s, bound to: %q, bindCompleted: %v, boundByController: %v", claim.Status.Phase, claim.Spec.VolumeName, bound, boundByController)
}

// getVolumeStatusForLogging 返回卷的关键状态字段，便于日志输出。
func getVolumeStatusForLogging(volume *v1.PersistentVolume) string {
	boundByController := metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnBoundByController)
	claimName := ""
	if volume.Spec.ClaimRef != nil {
		claimName = fmt.Sprintf("%s/%s (uid: %s)", volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name, volume.Spec.ClaimRef.UID)
	}
	return fmt.Sprintf("phase: %s, bound to: %q, boundByController: %v", volume.Status.Phase, claimName, boundByController)
}

// storeObjectUpdate 用 informer 回调或控制器自身修改后的对象版本更新缓存。
// 缓存被更新时返回 true；对象版本过旧应忽略时返回 false。
func storeObjectUpdate(logger klog.Logger, store cache.Store, obj interface{}, className string) (bool, error) {
	objName, err := controller.KeyFunc(obj)
	if err != nil {
		return false, fmt.Errorf("couldn't get key for object %+v: %w", obj, err)
	}
	oldObj, found, err := store.Get(obj)
	if err != nil {
		return false, fmt.Errorf("error finding %s %q in controller cache: %w", className, objName, err)
	}

	objAccessor, err := meta.Accessor(obj)
	if err != nil {
		return false, err
	}
	if !found {
		// 缓存中不存在，是新对象。
		logger.V(4).Info("storeObjectUpdate, adding obj", "storageClassName", className, "objName", objName, "resourceVersion", objAccessor.GetResourceVersion())
		if err = store.Add(obj); err != nil {
			return false, fmt.Errorf("error adding %s %q to controller cache: %w", className, objName, err)
		}
		return true, nil
	}

	oldObjAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		return false, err
	}

	objResourceVersion, err := strconv.ParseInt(objAccessor.GetResourceVersion(), 10, 64)
	if err != nil {
		return false, fmt.Errorf("error parsing ResourceVersion %q of %s %q: %s", objAccessor.GetResourceVersion(), className, objName, err)
	}
	oldObjResourceVersion, err := strconv.ParseInt(oldObjAccessor.GetResourceVersion(), 10, 64)
	if err != nil {
		return false, fmt.Errorf("error parsing old ResourceVersion %q of %s %q: %s", oldObjAccessor.GetResourceVersion(), className, objName, err)
	}

	// 只丢弃更旧的版本，相同版本仍放行，以便获得周期性同步事件。
	if oldObjResourceVersion > objResourceVersion {
		logger.V(4).Info("storeObjectUpdate: ignoring obj", "storageClassName", className, "objName", objName, "resourceVersion", objAccessor.GetResourceVersion())
		return false, nil
	}

	logger.V(4).Info("storeObjectUpdate updating obj with version", "storageClassName", className, "objName", objName, "resourceVersion", objAccessor.GetResourceVersion())
	if err = store.Update(obj); err != nil {
		return false, fmt.Errorf("error updating %s %q in controller cache: %w", className, objName, err)
	}
	return true, nil
}
