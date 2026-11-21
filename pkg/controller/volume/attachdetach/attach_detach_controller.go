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

// Package attachdetach 实现一个控制器，用于管理卷的挂载（attach）与卸载（detach）操作。
package attachdetach

import (
	"context"
	"fmt"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformersv1 "k8s.io/client-go/informers/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/populator"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/reconciler"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/statusupdater"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/util"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
	"k8s.io/mount-utils"
)

// TimerConfig 保存内部挂载/卸载定时器的配置，仅应用于加速测试。
// 生产环境建议使用 DefaultTimerConfig。
type TimerConfig struct {
	// ReconcilerLoopPeriod 是协调循环两次执行之间的等待时间。
	ReconcilerLoopPeriod time.Duration

	// ReconcilerMaxWaitForUnmountDuration 是控制器等待卷在节点上安全卸载的最长时间。
	// 超时后控制器会认为节点或 kubelet 已无响应，仍会强制卸载该卷。
	ReconcilerMaxWaitForUnmountDuration time.Duration

	// DesiredStateOfWorldPopulatorLoopSleepPeriod 是期望状态填充循环两次执行之间的等待时间。
	DesiredStateOfWorldPopulatorLoopSleepPeriod time.Duration

	// DesiredStateOfWorldPopulatorListPodsRetryDuration 是期望状态填充循环两次 List Pods 之间的等待时间。
	DesiredStateOfWorldPopulatorListPodsRetryDuration time.Duration
}

// DefaultTimerConfig 是 Attach/Detach 控制器定时器的默认配置。
var DefaultTimerConfig = TimerConfig{
	ReconcilerLoopPeriod:                              100 * time.Millisecond,
	ReconcilerMaxWaitForUnmountDuration:               6 * time.Minute,
	DesiredStateOfWorldPopulatorLoopSleepPeriod:       1 * time.Minute,
	DesiredStateOfWorldPopulatorListPodsRetryDuration: 3 * time.Minute,
}

// AttachDetachController 定义该控制器支持的操作。
type AttachDetachController interface {
	Run(ctx context.Context)
	GetDesiredStateOfWorld() cache.DesiredStateOfWorld
}

// NewAttachDetachController 创建并返回一个新的 AttachDetachController 实例。
func NewAttachDetachController(
	ctx context.Context,
	kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	csiNodeInformer storageinformersv1.CSINodeInformer,
	csiDriverInformer storageinformersv1.CSIDriverInformer,
	volumeAttachmentInformer storageinformersv1.VolumeAttachmentInformer,
	plugins []volume.VolumePlugin,
	prober volume.DynamicPluginProber,
	disableReconciliationSync bool,
	reconcilerSyncDuration time.Duration,
	disableForceDetachOnTimeout bool,
	timerConfig TimerConfig) (AttachDetachController, error) {

	logger := klog.FromContext(ctx)

	adc := &attachDetachController{
		kubeClient:  kubeClient,
		pvcLister:   pvcInformer.Lister(),
		pvcsSynced:  pvcInformer.Informer().HasSynced,
		pvLister:    pvInformer.Lister(),
		pvsSynced:   pvInformer.Informer().HasSynced,
		podLister:   podInformer.Lister(),
		podsSynced:  podInformer.Informer().HasSynced,
		podIndexer:  podInformer.Informer().GetIndexer(),
		nodeLister:  nodeInformer.Lister(),
		nodesSynced: nodeInformer.Informer().HasSynced,
		pvcQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "pvcs"},
		),
	}

	adc.csiNodeLister = csiNodeInformer.Lister()
	adc.csiNodeSynced = csiNodeInformer.Informer().HasSynced

	adc.csiDriverLister = csiDriverInformer.Lister()
	adc.csiDriversSynced = csiDriverInformer.Informer().HasSynced

	adc.volumeAttachmentLister = volumeAttachmentInformer.Lister()
	adc.volumeAttachmentSynced = volumeAttachmentInformer.Informer().HasSynced

	if err := adc.volumePluginMgr.InitPlugins(plugins, prober, adc); err != nil {
		return nil, fmt.Errorf("could not initialize volume plugins for Attach/Detach Controller: %w", err)
	}

	adc.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
	recorder := adc.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "attachdetach-controller"})
	blkutil := volumepathhandler.NewBlockVolumePathHandler()

	adc.desiredStateOfWorld = cache.NewDesiredStateOfWorld(&adc.volumePluginMgr)
	adc.actualStateOfWorld = cache.NewActualStateOfWorld(&adc.volumePluginMgr)
	adc.attacherDetacher =
		operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
			kubeClient,
			&adc.volumePluginMgr,
			recorder,
			blkutil))
	adc.nodeStatusUpdater = statusupdater.NewNodeStatusUpdater(
		kubeClient, nodeInformer.Lister(), adc.actualStateOfWorld)

	// 以下参数使用传入的选项值进行初始化。
	adc.reconciler = reconciler.NewReconciler(
		timerConfig.ReconcilerLoopPeriod,
		timerConfig.ReconcilerMaxWaitForUnmountDuration,
		reconcilerSyncDuration,
		disableReconciliationSync,
		disableForceDetachOnTimeout,
		adc.desiredStateOfWorld,
		adc.actualStateOfWorld,
		adc.attacherDetacher,
		adc.nodeStatusUpdater,
		adc.nodeLister,
		recorder)

	csiTranslator := csitrans.New()
	adc.intreeToCSITranslator = csiTranslator
	adc.csiMigratedPluginManager = csimigration.NewPluginManager(csiTranslator)

	adc.desiredStateOfWorldPopulator = populator.NewDesiredStateOfWorldPopulator(
		timerConfig.DesiredStateOfWorldPopulatorLoopSleepPeriod,
		timerConfig.DesiredStateOfWorldPopulatorListPodsRetryDuration,
		podInformer.Lister(),
		adc.desiredStateOfWorld,
		&adc.volumePluginMgr,
		pvcInformer.Lister(),
		pvInformer.Lister(),
		adc.csiMigratedPluginManager,
		adc.intreeToCSITranslator)

	_, err := podInformer.Informer().AddEventHandlerWithOptions(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			adc.podAdd(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			adc.podUpdate(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			adc.podDelete(logger, obj)
		},
	}, kcache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add pod event handler: %w", err)
	}

	// 使用自定义索引器按 PVC 键索引 Pod，这样查找引用某个 PVC 的 Pod 时无需每次遍历全部 Pod。
	if err := common.AddPodPVCIndexerIfNotPresent(adc.podIndexer); err != nil {
		return nil, fmt.Errorf("could not initialize attach detach controller: %w", err)
	}

	_, err = nodeInformer.Informer().AddEventHandlerWithOptions(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			adc.nodeAdd(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			adc.nodeUpdate(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			adc.nodeDelete(logger, obj)
		},
	}, kcache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add node event handler: %w", err)
	}

	_, err = pvcInformer.Informer().AddEventHandlerWithOptions(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			adc.enqueuePVC(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			adc.enqueuePVC(new)
		},
	}, kcache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add pvc event handler: %w", err)
	}

	return adc, nil
}

// attachDetachController 是 Attach/Detach 控制器的实现，
// 通过对比期望状态与实际状态驱动卷的挂载和卸载。
type attachDetachController struct {
	// kubeClient 是 volumehost 用于与 API Server 通信的客户端。
	kubeClient clientset.Interface

	// pvcLister 是从 API Server 获取并缓存 PVC 对象的共享 lister；因为它与其他控制器共用，缓存中的对象应视为不可变。
	pvcLister  corelisters.PersistentVolumeClaimLister
	pvcsSynced kcache.InformerSynced

	// pvLister 是从 API Server 获取并缓存 PV 对象的共享 lister；因为它与其他控制器共用，缓存中的对象应视为不可变。
	pvLister  corelisters.PersistentVolumeLister
	pvsSynced kcache.InformerSynced

	podLister  corelisters.PodLister
	podsSynced kcache.InformerSynced
	podIndexer kcache.Indexer

	nodeLister  corelisters.NodeLister
	nodesSynced kcache.InformerSynced

	csiNodeLister storagelistersv1.CSINodeLister
	csiNodeSynced kcache.InformerSynced

	// csiDriverLister 是获取并缓存 CSIDriver 对象的共享 lister；因为它与其他控制器共用，缓存中的对象应视为不可变。
	csiDriverLister  storagelistersv1.CSIDriverLister
	csiDriversSynced kcache.InformerSynced

	// volumeAttachmentLister 是获取并缓存 VolumeAttachment 对象的共享 lister；因为它与其他控制器共用，缓存中的对象应视为不可变。
	volumeAttachmentLister storagelistersv1.VolumeAttachmentLister
	volumeAttachmentSynced kcache.InformerSynced

	// volumePluginMgr 用于初始化和查找卷插件。
	volumePluginMgr volume.VolumePluginMgr

	// desiredStateOfWorld 保存控制器眼中的期望状态：管理哪些节点、希望哪些卷挂载到哪些节点，以及哪些引用这些卷的 Pod 被调度到了这些节点。
	// 该结构由控制器根据 informer 提供的节点与 Pod 对象流填充。
	desiredStateOfWorld cache.DesiredStateOfWorld

	// actualStateOfWorld 保存控制器眼中的实际状态，即哪些卷挂载到了哪些节点。
	// 它在控制器触发的挂载/卸载操作成功后，以及与存储提供方周期性同步真实状态时被填充。
	actualStateOfWorld cache.ActualStateOfWorld

	// attacherDetacher 用于启动异步的挂载与卸载操作。
	attacherDetacher operationexecutor.OperationExecutor

	// reconciler 运行异步周期循环，通过 attacherDetacher 触发挂载/卸载操作，使实际状态与期望状态趋于一致。
	reconciler reconciler.Reconciler

	// nodeStatusUpdater 用已挂载卷列表更新节点状态。
	nodeStatusUpdater statusupdater.NodeStatusUpdater

	// desiredStateOfWorldPopulator 运行异步周期循环，根据 podInformer 填充当前 Pod 信息。
	desiredStateOfWorldPopulator populator.DesiredStateOfWorldPopulator

	// broadcaster 负责广播事件。
	broadcaster record.EventBroadcaster

	// pvcQueue 用于对待处理的 PVC 对象排队。
	pvcQueue workqueue.TypedRateLimitingInterface[string]

	// csiMigratedPluginManager 用于识别已迁移到 CSI 的树内插件。
	csiMigratedPluginManager csimigration.PluginManager

	// intreeToCSITranslator 将树内卷规格转换为 CSI 卷规格。
	intreeToCSITranslator csimigration.InTreeToCSITranslator
}

// Run 启动 Attach/Detach 控制器：等待各 informer 缓存同步，初始化期望状态与实际状态，
// 然后并发运行协调循环、期望状态填充循环和 PVC 工作队列，直到 ctx 结束。
func (adc *attachDetachController) Run(ctx context.Context) {
	defer runtime.HandleCrash()

	// 启动事件处理流水线。
	adc.broadcaster.StartStructuredLogging(3)
	adc.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: adc.kubeClient.CoreV1().Events("")})
	defer adc.broadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting attach detach controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down attach detach controller")
		adc.pvcQueue.ShutDown()
		wg.Wait()
	}()

	synced := []kcache.InformerSynced{adc.podsSynced, adc.nodesSynced, adc.pvcsSynced, adc.pvsSynced,
		adc.csiNodeSynced, adc.csiDriversSynced, adc.volumeAttachmentSynced}
	if !kcache.WaitForNamedCacheSyncWithContext(ctx, synced...) {
		return
	}

	err := adc.populateActualStateOfWorld(logger)
	if err != nil {
		logger.Error(err, "Error populating the actual state of world")
	}
	err = adc.populateDesiredStateOfWorld(logger)
	if err != nil {
		logger.Error(err, "Error populating the desired state of world")
	}

	metrics.Register(
		adc.pvcLister,
		adc.pvLister,
		adc.podLister,
		adc.actualStateOfWorld,
		adc.desiredStateOfWorld,
		&adc.volumePluginMgr,
		adc.csiMigratedPluginManager,
		adc.intreeToCSITranslator,
	)

	wg.Go(func() {
		adc.reconciler.Run(ctx)
	})
	wg.Go(func() {
		adc.desiredStateOfWorldPopulator.Run(ctx)
	})
	wg.Go(func() {
		wait.UntilWithContext(ctx, adc.pvcWorker, time.Second)
	})
	<-ctx.Done()
}

// populateActualStateOfWorld 从节点状态中的 VolumesAttached 初始化实际状态，
// 同时处理 VolumeAttachment 对象，并把节点加入期望状态。
func (adc *attachDetachController) populateActualStateOfWorld(logger klog.Logger) error {
	logger.V(5).Info("Populating ActualStateOfworld")
	nodes, err := adc.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for _, node := range nodes {
		nodeName := types.NodeName(node.Name)

		for _, attachedVolume := range node.Status.VolumesAttached {
			uniqueName := attachedVolume.Name
			// 只有在卷未被任何 Pod 使用时传入 nil VolumeSpec 才是安全的：此类卷会在第一轮协调时被卸载，而卸载本身不需要卷规格。
			// 若卷正被 Pod 使用，populateDesiredStateOfWorld 会扫描 Pod 并用正确的规格更新实际状态中的对应卷。
			err = adc.actualStateOfWorld.MarkVolumeAsAttached(logger, uniqueName, nil /* VolumeSpec */, nodeName, attachedVolume.DevicePath)
			if err != nil {
				logger.Error(err, "Failed to mark the volume as attached")
				continue
			}
		}
		adc.actualStateOfWorld.SetVolumesMountedByNode(logger, node.Status.VolumesInUse, nodeName)
		adc.addNodeToDswp(node, types.NodeName(node.Name))
	}
	err = adc.processVolumeAttachments(logger)
	if err != nil {
		logger.Error(err, "Failed to process volume attachments")
	}
	return err
}

// getNodeVolumeDevicePath 返回指定节点上某个已挂载卷的设备路径。
func (adc *attachDetachController) getNodeVolumeDevicePath(
	volumeName v1.UniqueVolumeName, nodeName types.NodeName) (string, error) {
	var devicePath string
	var found bool
	node, err := adc.nodeLister.Get(string(nodeName))
	if err != nil {
		return devicePath, err
	}
	for _, attachedVolume := range node.Status.VolumesAttached {
		if volumeName == attachedVolume.Name {
			devicePath = attachedVolume.DevicePath
			found = true
			break
		}
	}
	if !found {
		err = fmt.Errorf("Volume %s not found on node %s", volumeName, nodeName)
	}

	return devicePath, err
}

// populateDesiredStateOfWorld 遍历所有 Pod 填充期望状态；
// 对已挂载的卷，用 Pod 中解析出的真实卷规格回填实际状态中先前为 nil 的规格。
func (adc *attachDetachController) populateDesiredStateOfWorld(logger klog.Logger) error {
	logger.V(5).Info("Populating DesiredStateOfworld")

	pods, err := adc.podLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, pod := range pods {
		adc.podAdd(logger, pod)
		for _, podVolume := range pod.Spec.Volumes {
			nodeName := types.NodeName(pod.Spec.NodeName)
			// 实际状态中的卷规格此前为 nil，这里用从 Pod 解析出的正确规格替换。若实际状态中的卷没有对应的 Pod，它会被卸载，规格无关紧要。
			volumeSpec, err := util.CreateVolumeSpecWithNodeMigration(logger, podVolume, pod, nodeName, &adc.volumePluginMgr, adc.pvcLister, adc.pvLister, adc.csiMigratedPluginManager, adc.intreeToCSITranslator)
			if err != nil {
				logger.Error(
					err,
					"Error creating spec for volume of pod",
					"pod", klog.KObj(pod),
					"volumeName", podVolume.Name)
				continue
			}
			plugin, err := adc.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
			if err != nil || plugin == nil {
				logger.V(10).Info(
					"Skipping volume for pod: it does not implement attacher interface",
					"pod", klog.KObj(pod),
					"volumeName", podVolume.Name,
					"err", err)
				continue
			}
			volumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
			if err != nil {
				logger.Error(
					err,
					"Failed to find unique name for volume of pod",
					"pod", klog.KObj(pod),
					"volumeName", podVolume.Name)
				continue
			}
			attachState := adc.actualStateOfWorld.GetAttachState(volumeName, nodeName)
			if attachState == cache.AttachStateAttached {
				logger.V(10).Info("Volume is attached to node. Marking as attached in ActualStateOfWorld",
					"node", klog.KRef("", string(nodeName)),
					"volumeName", volumeName)
				devicePath, err := adc.getNodeVolumeDevicePath(volumeName, nodeName)
				if err != nil {
					logger.Error(err, "Failed to find device path")
					continue
				}
				err = adc.actualStateOfWorld.MarkVolumeAsAttached(logger, volumeName, volumeSpec, nodeName, devicePath)
				if err != nil {
					logger.Error(err, "Failed to update volume spec for node", "node", klog.KRef("", string(nodeName)))
				}
			}
		}
	}

	return nil
}

// podAdd 处理 Pod 添加事件：只处理已调度的 Pod，把其卷加入期望状态。
func (adc *attachDetachController) podAdd(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if pod == nil || !ok {
		return
	}
	if pod.Spec.NodeName == "" {
		// 忽略没有 NodeName 的 Pod，说明它尚未被调度。
		return
	}

	volumeActionFlag := util.DetermineVolumeAction(
		pod,
		true /* default volume action */)

	util.ProcessPodVolumes(logger, pod, volumeActionFlag, /* addVolumes */
		adc.desiredStateOfWorld, &adc.volumePluginMgr, adc.pvcLister, adc.pvLister, adc.csiMigratedPluginManager, adc.intreeToCSITranslator)
}

// GetDesiredStateOfWorld 返回与该控制器关联的期望状态。
func (adc *attachDetachController) GetDesiredStateOfWorld() cache.DesiredStateOfWorld {
	return adc.desiredStateOfWorld
}

// podUpdate 处理 Pod 更新事件：按更新后的 Pod 重新处理其卷。
func (adc *attachDetachController) podUpdate(logger klog.Logger, oldObj, newObj interface{}) {
	pod, ok := newObj.(*v1.Pod)
	if pod == nil || !ok {
		return
	}
	if pod.Spec.NodeName == "" {
		// 忽略没有 NodeName 的 Pod，说明它尚未被调度。
		return
	}

	volumeActionFlag := util.DetermineVolumeAction(
		pod,
		true /* default volume action */)

	util.ProcessPodVolumes(logger, pod, volumeActionFlag, /* addVolumes */
		adc.desiredStateOfWorld, &adc.volumePluginMgr, adc.pvcLister, adc.pvLister, adc.csiMigratedPluginManager, adc.intreeToCSITranslator)
}

// podDelete 处理 Pod 删除事件：兼容墓碑对象，并把 Pod 的卷从期望状态中移除。
func (adc *attachDetachController) podDelete(logger klog.Logger, obj interface{}) {
	// informer 可能只给出墓碑对象，先取出其中保存的真实 Pod。
	if tombstone, ok := obj.(kcache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*v1.Pod)
	if !ok {
		runtime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
		return
	}

	util.ProcessPodVolumes(logger, pod, false, /* addVolumes */
		adc.desiredStateOfWorld, &adc.volumePluginMgr, adc.pvcLister, adc.pvLister, adc.csiMigratedPluginManager, adc.intreeToCSITranslator)
}

// nodeAdd 处理节点添加事件：先按节点更新处理，再标记需要同步节点状态，
// 以规避节点添加时清空已挂载卷字段导致的状态不一致。
func (adc *attachDetachController) nodeAdd(logger klog.Logger, obj interface{}) {
	node, ok := obj.(*v1.Node)
	// TODO: investigate if nodeName is empty then if we can return
	// kubernetes/kubernetes/issues/37777
	// 节点添加可能清空已挂载卷字段，因此下面主动触发与真实状态的同步。
	if node == nil || !ok {
		return
	}
	nodeName := types.NodeName(node.Name)
	adc.nodeUpdate(logger, nil, obj)
	// kubernetes/kubernetes/issues/37586
	// 规避节点添加时清空已挂载卷字段的问题，确保与实际状态同步。
	adc.actualStateOfWorld.SetNodeStatusUpdateNeeded(logger, nodeName)
}

// nodeUpdate 处理节点更新事件：把节点加入期望状态并处理其 VolumesInUse。
func (adc *attachDetachController) nodeUpdate(logger klog.Logger, oldObj, newObj interface{}) {
	node, ok := newObj.(*v1.Node)
	// TODO: investigate if nodeName is empty then if we can return
	if node == nil || !ok {
		return
	}

	nodeName := types.NodeName(node.Name)
	adc.addNodeToDswp(node, nodeName)
	adc.processVolumesInUse(logger, nodeName, node.Status.VolumesInUse)
}

// nodeDelete 处理节点删除事件：从期望状态中移除节点并处理其 VolumesInUse。
func (adc *attachDetachController) nodeDelete(logger klog.Logger, obj interface{}) {
	node, ok := obj.(*v1.Node)
	if node == nil || !ok {
		return
	}

	nodeName := types.NodeName(node.Name)
	if err := adc.desiredStateOfWorld.DeleteNode(nodeName); err != nil {
		// 节点排空过程中可能发生删除失败，但仍需记录到日志中便于排查。
		logger.Info("Error removing node from desired-state-of-world", "node", klog.KObj(node), "err", err)
	}

	adc.processVolumesInUse(logger, nodeName, node.Status.VolumesInUse)
}

// enqueuePVC 把 PVC 对象的命名空间/名称键加入工作队列，供后台 worker 处理。
func (adc *attachDetachController) enqueuePVC(obj interface{}) {
	key, err := kcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}
	adc.pvcQueue.Add(key)
}

// pvcWorker 持续处理 pvcQueue 中的条目，直到队列关闭。
func (adc *attachDetachController) pvcWorker(ctx context.Context) {
	for adc.processNextItem(klog.FromContext(ctx)) {
	}
}

// processNextItem 从队列取出一个 PVC 键进行同步；失败时限速重试，成功时调用 Forget 清除失败记录。
func (adc *attachDetachController) processNextItem(logger klog.Logger) bool {
	keyObj, shutdown := adc.pvcQueue.Get()
	if shutdown {
		return false
	}
	defer adc.pvcQueue.Done(keyObj)

	if err := adc.syncPVCByKey(logger, keyObj); err != nil {
		// 失败时立即限速重新入队重试，不必等待下一次全量同步。
		adc.pvcQueue.AddRateLimited(keyObj)
		runtime.HandleError(fmt.Errorf("failed to sync pvc %q, will retry again: %w", keyObj, err))
		return true
	}

	// 成功后调用 Forget，避免该条目在没有新变化时被重复处理。
	adc.pvcQueue.Forget(keyObj)
	return true
}

// syncPVCByKey 同步单个 PVC：找到引用该 PVC 的活跃 Pod，并把它们的卷加入期望状态，
// 使 PVC 绑定状态的变化能及时触发卷挂载。
func (adc *attachDetachController) syncPVCByKey(logger klog.Logger, key string) error {
	logger.V(5).Info("syncPVCByKey", "pvcKey", key)
	namespace, name, err := kcache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.V(4).Info("Error getting namespace & name of pvc to get pvc from informer", "pvcKey", key, "err", err)
		return nil
	}
	pvc, err := adc.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		logger.V(4).Info("Error getting pvc from informer", "pvcKey", key, "err", err)
		return nil
	}
	if err != nil {
		return err
	}

	if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
		// 未绑定或没有 VolumeName 的 PVC 无需处理。
		return nil
	}

	objs, err := adc.podIndexer.ByIndex(common.PodPVCIndex, key)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			continue
		}
		// 只关心已调度到节点且仍处于运行中的 Pod。
		if len(pod.Spec.NodeName) == 0 || volumeutil.IsPodTerminated(pod, pod.Status) {
			continue
		}
		volumeActionFlag := util.DetermineVolumeAction(
			pod,
			true /* default volume action */)

		util.ProcessPodVolumes(logger, pod, volumeActionFlag, /* addVolumes */
			adc.desiredStateOfWorld, &adc.volumePluginMgr, adc.pvcLister, adc.pvLister, adc.csiMigratedPluginManager, adc.intreeToCSITranslator)
	}
	return nil
}

// processVolumesInUse 根据节点的 Status.VolumesInUse 更新实际状态，把对应卷标记为已由该节点挂载。
func (adc *attachDetachController) processVolumesInUse(
	logger klog.Logger, nodeName types.NodeName, volumesInUse []v1.UniqueVolumeName) {
	logger.V(4).Info("processVolumesInUse for node", "node", klog.KRef("", string(nodeName)))
	adc.actualStateOfWorld.SetVolumesMountedByNode(logger, volumesInUse, nodeName)
}

// processVolumeAttachments 处理 VolumeAttachment 对象，必须在用节点状态填充 ASW 之后调用。
// 对每个 VA，若对应卷尚不在 ASW 中，则以“不确定（uncertain）”状态加入；随后协调器会根据 DSW 判断：
//
//	若卷仍在 DSW 中，则尝试挂载；
//	若不在（可能是悬空挂载），则将其卸载。
func (adc *attachDetachController) processVolumeAttachments(logger klog.Logger) error {
	vas, err := adc.volumeAttachmentLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Failed to list VolumeAttachment objects")
		return err
	}
	for _, va := range vas {
		nodeName := types.NodeName(va.Spec.NodeName)
		pvName := va.Spec.Source.PersistentVolumeName
		if pvName == nil {
			// 当前 VA 对象只针对 CSI 卷创建，pvName 为 nil 属于异常情况，记录告警。
			logger.Info("Skipping the va as its pvName is nil", "node", klog.KRef("", string(nodeName)), "vaName", va.Name)
			continue
		}
		pv, err := adc.pvLister.Get(*pvName)
		if err != nil {
			logger.Error(err, "Unable to lookup pv object", "PV", klog.KRef("", *pvName))
			continue
		}

		var plugin volume.AttachableVolumePlugin
		volumeSpec := volume.NewSpecFromPersistentVolume(pv, false)

		// 若 PV 已迁移，需要把树内卷规格转换为 CSI 卷规格。
		if inTreePluginName, err := adc.csiMigratedPluginManager.GetInTreePluginNameFromSpec(pv, nil); err == nil {
			if adc.csiMigratedPluginManager.IsMigrationEnabledForPlugin(inTreePluginName) {
				// PV 已迁移，应由 CSI 插件而非树内插件处理。
				plugin, _ = adc.volumePluginMgr.FindAttachablePluginByName(csi.CSIPluginName)
				// 对 AzureFile 而言无需传 podNamespace：有无该参数生成的卷名相同。
				volumeSpec, err = csimigration.TranslateInTreeSpecToCSI(logger, volumeSpec, "" /* podNamespace */, adc.intreeToCSITranslator)
				if err != nil {
					logger.Error(err, "Failed to translate intree volumeSpec to CSI volumeSpec for volume", "node", klog.KRef("", string(nodeName)), "inTreePluginName", inTreePluginName, "vaName", va.Name, "PV", klog.KRef("", *pvName))
					continue
				}
			}
		}

		if plugin == nil {
			plugin, err = adc.volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
			if err != nil || plugin == nil {
				// VA 只针对 CSI 卷创建，找不到 attacher 插件属于异常，记录告警。
				logger.Info("Skipping processing the volume on node, no attacher interface found", "node", klog.KRef("", string(nodeName)), "PV", klog.KRef("", *pvName), "err", err)
				continue
			}
		}

		volumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)
		if err != nil {
			logger.Error(err, "Failed to find unique name for volume", "node", klog.KRef("", string(nodeName)), "vaName", va.Name, "PV", klog.KRef("", *pvName))
			continue
		}
		attachState := adc.actualStateOfWorld.GetAttachState(volumeName, nodeName)
		if attachState == cache.AttachStateDetached {
			logger.V(1).Info("Marking volume attachment as uncertain as volume is not attached", "node", klog.KRef("", string(nodeName)), "volumeName", volumeName, "attachState", attachState)
			err = adc.actualStateOfWorld.MarkVolumeAsUncertain(logger, volumeName, volumeSpec, nodeName)
			if err != nil {
				logger.Error(err, "MarkVolumeAsUncertain fail to add the volume to ASW", "node", klog.KRef("", string(nodeName)), "volumeName", volumeName)
			}
		}
	}
	return nil
}

var _ volume.VolumeHost = &attachDetachController{}
var _ volume.AttachDetachVolumeHost = &attachDetachController{}

// CSINodeLister 返回 CSINode 的 lister。
func (adc *attachDetachController) CSINodeLister() storagelistersv1.CSINodeLister {
	return adc.csiNodeLister
}

// CSIDriverLister 返回 CSIDriver 的 lister。
func (adc *attachDetachController) CSIDriverLister() storagelistersv1.CSIDriverLister {
	return adc.csiDriverLister
}

// IsAttachDetachController 标识当前 VolumeHost 由 Attach/Detach 控制器实现。
func (adc *attachDetachController) IsAttachDetachController() bool {
	return true
}

// VolumeAttachmentLister 返回 VolumeAttachment 的 lister。
func (adc *attachDetachController) VolumeAttachmentLister() storagelistersv1.VolumeAttachmentLister {
	return adc.volumeAttachmentLister
}

// 以下是 VolumeHost 接口的实现。
// 由于当前卷插件初始化代码的结构，所有初始化方都必须实现挂载代码用到的 kubelet 专用方法，// 即使该初始化方（例如本 Attach/Detach 控制器）根本不执行挂载。
// 相关问题见 kubernetes/kubernetes/issues/14217。
// GetPluginDir 返回卷插件目录；Attach/Detach 控制器不执行挂载，故返回空字符串。
func (adc *attachDetachController) GetPluginDir(podUID string) string {
	return ""
}

// GetVolumeDevicePluginDir 返回卷设备插件目录；本控制器不涉及该路径，返回空字符串。
func (adc *attachDetachController) GetVolumeDevicePluginDir(podUID string) string {
	return ""
}

// GetPodsDir 返回 Pod 目录；本控制器无本地挂载需求，返回空字符串。
func (adc *attachDetachController) GetPodsDir() string {
	return ""
}

// GetPodVolumeDir 返回 Pod 的卷目录；本控制器不执行挂载，返回空字符串。
func (adc *attachDetachController) GetPodVolumeDir(podUID types.UID, pluginName, volumeName string) string {
	return ""
}

// GetPodPluginDir 返回 Pod 的插件目录；本控制器不执行挂载，返回空字符串。
func (adc *attachDetachController) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetPodVolumeDeviceDir 返回 Pod 的卷设备目录；本控制器不执行挂载，返回空字符串。
func (adc *attachDetachController) GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetKubeClient 返回该控制器使用的 kube 客户端。
func (adc *attachDetachController) GetKubeClient() clientset.Interface {
	return adc.kubeClient
}

// NewWrapperMounter 不支持：Attach/Detach 控制器不负责挂载操作。
func (adc *attachDetachController) NewWrapperMounter(volName string, spec volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return nil, fmt.Errorf("NewWrapperMounter not supported by Attach/Detach controller's VolumeHost implementation")
}

// NewWrapperUnmounter 不支持：Attach/Detach 控制器不负责卸载本地挂载。
func (adc *attachDetachController) NewWrapperUnmounter(volName string, spec volume.Spec, podUID types.UID) (volume.Unmounter, error) {
	return nil, fmt.Errorf("NewWrapperUnmounter not supported by Attach/Detach controller's VolumeHost implementation")
}

// GetMounter 返回挂载接口；本控制器不挂载卷，故返回 nil。
func (adc *attachDetachController) GetMounter() mount.Interface {
	return nil
}

// GetNodeAllocatable 返回节点可分配资源；本控制器不管理节点资源，返回空列表。
func (adc *attachDetachController) GetNodeAllocatable() (v1.ResourceList, error) {
	return v1.ResourceList{}, nil
}

// GetAttachedVolumesFromNodeStatus 返回节点状态中的已挂载卷；本场景不使用，返回空映射。
func (adc *attachDetachController) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	return map[v1.UniqueVolumeName]string{}, nil
}

// GetSecretFunc 返回读取 Secret 的函数；本控制器不支持该操作，调用时返回错误。
func (adc *attachDetachController) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	return func(_, _ string) (*v1.Secret, error) {
		return nil, fmt.Errorf("GetSecret unsupported in attachDetachController")
	}
}

// GetConfigMapFunc 返回读取 ConfigMap 的函数；本控制器不支持该操作，调用时返回错误。
func (adc *attachDetachController) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	return func(_, _ string) (*v1.ConfigMap, error) {
		return nil, fmt.Errorf("GetConfigMap unsupported in attachDetachController")
	}
}

// GetServiceAccountTokenFunc 返回申请 ServiceAccount Token 的函数；本控制器不支持该操作。
func (adc *attachDetachController) GetServiceAccountTokenFunc() func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return nil, fmt.Errorf("GetServiceAccountToken unsupported in attachDetachController")
	}
}

// DeleteServiceAccountTokenFunc 返回删除 ServiceAccount Token 的函数；本控制器不支持该操作。
func (adc *attachDetachController) DeleteServiceAccountTokenFunc() func(types.UID) {
	return func(types.UID) {
		// nolint:logcheck
		klog.ErrorS(nil, "DeleteServiceAccountToken unsupported in attachDetachController")
	}
}

// addNodeToDswp 根据节点注解判断它是否由 Attach/Detach 控制器管理，若是则将其加入期望状态。
func (adc *attachDetachController) addNodeToDswp(node *v1.Node, nodeName types.NodeName) {
	if _, exists := node.Annotations[volumeutil.ControllerManagedAttachAnnotation]; exists {
		// 节点带有该注解，说明应由 Attach/Detach 控制器管理，因此加入期望状态。
		adc.desiredStateOfWorld.AddNode(nodeName)
	}
}

// GetNodeLabels 不支持：Attach/Detach 控制器不需要读取节点标签。
func (adc *attachDetachController) GetNodeLabels() (map[string]string, error) {
	return nil, fmt.Errorf("GetNodeLabels() unsupported in Attach/Detach controller")
}

// GetNodeName 返回节点名；本控制器不与单个节点绑定，返回空值。
func (adc *attachDetachController) GetNodeName() types.NodeName {
	return ""
}

// GetEventRecorder 返回事件记录器；本控制器通过 broadcaster 直接记录事件，返回 nil。
func (adc *attachDetachController) GetEventRecorder() record.EventRecorder {
	return nil
}

// GetSubpather 返回 subpath 接口；Attach/Detach 控制器不需要 subpath 操作。
func (adc *attachDetachController) GetSubpather() subpath.Interface {
	// Attach/Detach 控制器不需要处理 subpath。
	return nil
}

// GetCSIDriverLister 返回 CSIDriver 的 lister。
func (adc *attachDetachController) GetCSIDriverLister() storagelistersv1.CSIDriverLister {
	return adc.csiDriverLister
}
