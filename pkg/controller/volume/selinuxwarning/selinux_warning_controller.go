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

package selinuxwarning

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformersv1 "k8s.io/client-go/informers/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/util"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	volumecache "k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/cache"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/translator"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/ptr"
)

// SELinuxWarning 控制器在两个 Pod *可能* 用不同 SELinux 标签使用同一卷时，
// 发出告警事件并记录指标。它是可选组件，在禁用 SELinux 的集群上不执行任何操作，
// 除告警事件外也不会修改任何 API 对象。
//
// 该控制器监听 *所有* Pod 并报告其所有卷的问题，
// 无论 Pod 是否一直处于 Pending，或是否配置了反亲和性而永远不会运行在同一节点上。
type Controller struct {
	kubeClient       clientset.Interface
	podLister        corelisters.PodLister
	podIndexer       cache.Indexer
	podsSynced       cache.InformerSynced
	pvLister         corelisters.PersistentVolumeLister
	pvsSynced        cache.InformerSynced
	pvcLister        corelisters.PersistentVolumeClaimLister
	pvcsSynced       cache.InformerSynced
	csiDriverLister  storagelisters.CSIDriverLister
	csiDriversSynced cache.InformerSynced

	vpm               *volume.VolumePluginMgr
	cmpm              csimigration.PluginManager
	csiTranslator     csimigration.InTreeToCSITranslator
	seLinuxTranslator *translator.ControllerSELinuxTranslator
	eventBroadcaster  record.EventBroadcaster
	eventRecorder     record.EventRecorder
	queue             workqueue.TypedRateLimitingInterface[cache.ObjectName]

	labelCache volumecache.VolumeCache
}

// NewController 构造 SELinuxWarning 控制器：初始化事件记录器、卷插件、CSI 迁移转换器，
// 为 Pod 建立 PVC 索引，并为 Pod/PVC/PV/CSIDriver 注册事件处理器。
func NewController(
	ctx context.Context,
	kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	csiDriverInformer storageinformersv1.CSIDriverInformer,
	plugins []volume.VolumePlugin,
	prober volume.DynamicPluginProber,
) (*Controller, error) {

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "selinux_warning"})
	seLinuxTranslator := &translator.ControllerSELinuxTranslator{}

	c := &Controller{
		kubeClient:        kubeClient,
		podLister:         podInformer.Lister(),
		podIndexer:        podInformer.Informer().GetIndexer(),
		podsSynced:        podInformer.Informer().HasSynced,
		pvLister:          pvInformer.Lister(),
		pvsSynced:         pvInformer.Informer().HasSynced,
		pvcLister:         pvcInformer.Lister(),
		pvcsSynced:        pvcInformer.Informer().HasSynced,
		csiDriverLister:   csiDriverInformer.Lister(),
		csiDriversSynced:  csiDriverInformer.Informer().HasSynced,
		vpm:               &volume.VolumePluginMgr{},
		seLinuxTranslator: seLinuxTranslator,

		eventBroadcaster: eventBroadcaster,
		eventRecorder:    recorder,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig[cache.ObjectName](
			workqueue.DefaultTypedControllerRateLimiter[cache.ObjectName](),
			workqueue.TypedRateLimitingQueueConfig[cache.ObjectName]{
				Name: "selinux_warning",
			},
		),
		labelCache: volumecache.NewVolumeLabelCache(seLinuxTranslator),
	}

	err := c.vpm.InitPlugins(plugins, prober, c)
	if err != nil {
		return nil, fmt.Errorf("could not initialize volume plugins for SELinux warning controller: %w", err)
	}
	csiTranslator := csitrans.New()
	c.csiTranslator = csiTranslator
	c.cmpm = csimigration.NewPluginManager(csiTranslator)

	// 按 PVC 键为 Pod 建立索引，这样查找引用某 PVC 的 Pod 时无需每次遍历全部 Pod。
	err = common.AddPodPVCIndexerIfNotPresent(c.podIndexer)
	if err != nil {
		return nil, fmt.Errorf("could not initialize SELinux warning controller: %w", err)
	}

	logger := klog.FromContext(ctx)
	_, err = podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueuePod(logger, obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { c.updatePod(logger, oldObj, newObj) },
		DeleteFunc: func(obj interface{}) { c.enqueuePod(logger, obj) },
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, err
	}

	_, err = pvcInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.addPVC(logger, obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { c.updatePVC(logger, oldObj, newObj) },
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, err
	}

	_, err = pvInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.addPV(logger, obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { c.updatePV(logger, oldObj, newObj) },
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, err
	}

	_, err = csiDriverInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.addCSIDriver(logger, obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { c.updateCSIDriver(logger, oldObj, newObj) },
		DeleteFunc: func(obj interface{}) { c.deleteCSIDriver(logger, obj) },
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, err
	}

	RegisterMetrics(logger, c.labelCache)

	return c, nil
}

// enqueuePod 把 Pod 对象转换为名字（兼容删除事件中的 tombstone）后放入工作队列。
func (c *Controller) enqueuePod(_ klog.Logger, obj interface{}) {
	podRef, err := cache.DeletionHandlingObjectToName(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for pod %#v: %w", obj, err))
	}
	c.queue.Add(podRef)
}

// updatePod 处理 Pod 更新事件。
// 与本控制器相关的 Pod.Spec 字段（卷、SELinux 标签、privileged 标记）创建后不可变，
// 因此仅在 Pod 进入终态（Failed/Succeeded）时才需处理，
// 此时 kubelet 会卸载卷，控制器应将其从缓存中移除。
func (c *Controller) updatePod(logger klog.Logger, oldObj, newObj interface{}) {
	// 与本控制器相关的 Pod.Spec 字段（卷、SELinux 标签、privileged 标记）创建后不可变，
	// 只在 Pod 进入终态时响应更新：kubelet 会卸载卷，控制器需要据此清理缓存。
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		return
	}

	// 优化：理论上大多数 Pod 更新入队后都是空操作，
	// 为节省 CPU，只把可能引发控制器动作的更新入队。
	if oldPod.Status.Phase == newPod.Status.Phase {
		return
	}
	if newPod.Status.Phase != v1.PodFailed && newPod.Status.Phase != v1.PodSucceeded {
		return
	}
	c.enqueuePod(logger, newObj)
}

// addPVC 处理 PVC 添加事件：仅当 PVC 已绑定时才重新入队引用它的 Pod。
func (c *Controller) addPVC(logger klog.Logger, obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		return
	}
	if pvc.Spec.VolumeName == "" {
		// 未绑定的 PVC 尚无卷，无需处理。
		return
	}
	c.enqueueAllPodsForPVC(logger, pvc.Namespace, pvc.Name)
}

// updatePVC 处理 PVC 更新事件：仅在 PVC 从“未绑定”变为“已绑定”时重新入队相关 Pod。
func (c *Controller) updatePVC(logger klog.Logger, oldObj, newObj interface{}) {
	oldPVC, ok := oldObj.(*v1.PersistentVolumeClaim)
	if !ok {
		return
	}
	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok {
		return
	}
	if oldPVC.Spec.VolumeName != newPVC.Spec.VolumeName {
		// PVC 刚刚完成绑定，卷信息现在可用。
		c.enqueueAllPodsForPVC(logger, newPVC.Namespace, newPVC.Name)
	}
}

// addPV 处理 PV 添加事件：对已完整绑定到某 PVC 的 PV，重新入队引用该 PVC 的 Pod。
func (c *Controller) addPV(logger klog.Logger, obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	claimRef := pv.Spec.ClaimRef
	if claimRef == nil {
		// 未绑定的 PV 无需处理。
		return
	}
	if claimRef.UID == "" {
		// 绑定尚未完成（无 UID），无需处理。
		return
	}
	c.enqueueAllPodsForPVC(logger, claimRef.Namespace, claimRef.Name)
}

// updatePV 处理 PV 更新事件：当 PV 的绑定关系发生变化（新绑定或解绑）时，
// 重新入队引用对应 PVC 的 Pod。
func (c *Controller) updatePV(logger klog.Logger, oldObj, newObj interface{}) {
	oldPV, ok := oldObj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	newPV, ok := newObj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	newClaimRef := newPV.Spec.ClaimRef
	if newClaimRef == nil {
		// 未绑定的 PV 无需处理。
		return
	}
	if newClaimRef.UID == "" {
		// 绑定尚未完成（无 UID），无需处理。
		return
	}
	oldClaimRef := oldPV.Spec.ClaimRef
	if oldClaimRef == nil || oldClaimRef.UID != newClaimRef.UID {
		// PV 刚被绑定或解绑，需重新入队该 PVC 所在命名空间中使用它的 Pod。
		c.enqueueAllPodsForPVC(logger, newClaimRef.Namespace, newClaimRef.Name)
	}
}

// enqueueAllPodsForPVC 借助 Pod 索引找出引用给定 PVC 的所有 Pod 并入队。
func (c *Controller) enqueueAllPodsForPVC(logger klog.Logger, namespace, name string) {
	// 保守地重新入队与该 PVC 同一命名空间的所有 Pod，代价是该命名空间下所有 Pod 事件都会被重新发送。
	// 可能的优化方向：
	// - 在 informer 回调中用 Pod informer 精确解析哪些 Pod 使用该 PVC，但可能延长回调阻塞时间。
	// - 维护一份“等待某 PVC 的 Pod”缓存，在回调中直接查表。
	// - 改为把 PVC 名字入队，由 worker 线程解析具体 Pod，但队列中将同时存在 PVC 和 Pod 两类元素。
	objs, err := c.podIndexer.ByIndex(common.PodPVCIndex, fmt.Sprintf("%s/%s", namespace, name))
	if err != nil {
		logger.Error(err, "listing pods from cache")
		return
	}
	for _, obj := range objs {
		c.enqueuePod(logger, obj)
	}
}

// addCSIDriver 处理 CSIDriver 添加事件：SELinuxMount 能力会影响卷的有效 SELinux 策略，
// 因此需重新入队使用该驱动的 Pod 重新评估冲突。
func (c *Controller) addCSIDriver(_ klog.Logger, obj interface{}) {
	csiDriver, ok := obj.(*storagev1.CSIDriver)
	if !ok {
		return
	}
	// 新增驱动意味着 SELinuxMount 能力从“无”变为“有”，使用该驱动卷的 Pod 的有效策略可能改变：
	// SELinuxMount 为 false/nil 时忽略 Pod 的 SELinuxChangePolicy，视作 `Recursive`；
	// 为 true 时才使用 Pod 实际声明的策略。重新入队相关 Pod 以重新评估冲突。
	c.enqueueAllPodsForCSIDriver(csiDriver.Name)
}

// updateCSIDriver 处理 CSIDriver 更新事件：仅当 SELinuxMount 开关发生变化时，
// 重新入队使用该驱动的 Pod 以重新评估冲突。
func (c *Controller) updateCSIDriver(_ klog.Logger, oldObj, newObj interface{}) {
	oldCSIDriver, ok := oldObj.(*storagev1.CSIDriver)
	if !ok {
		return
	}
	newCSIDriver, ok := newObj.(*storagev1.CSIDriver)
	if !ok {
		return
	}
	oldSELinuxMount := ptr.Deref(oldCSIDriver.Spec.SELinuxMount, false)
	newSELinuxMount := ptr.Deref(newCSIDriver.Spec.SELinuxMount, false)
	if oldSELinuxMount != newSELinuxMount {
		// SELinuxMount 开关变化会改变卷的有效 SELinuxChangePolicy：
		// false/nil 时视作 `Recursive`，true 时使用 Pod 声明的策略。
		// 重新入队使用该驱动的 Pod 以重新评估冲突。
		c.enqueueAllPodsForCSIDriver(newCSIDriver.Name)
	}
}

// deleteCSIDriver 处理 CSIDriver 删除事件：若被删除的驱动声明支持 SELinuxMount，
// 其能力将回退为默认的不支持，需重新入队使用该驱动的 Pod。
func (c *Controller) deleteCSIDriver(_ klog.Logger, obj interface{}) {
	csiDriver, ok := obj.(*storagev1.CSIDriver)
	if !ok {
		return
	}
	if ptr.Deref(csiDriver.Spec.SELinuxMount, false) {
		// 被删除的驱动曾声明支持 SELinuxMount；没有 CSIDriver 实例时默认为 `SELinuxMount: false`。
		// 之前使用 Pod 实际声明的策略，之后将视作 `Recursive`，
		// 因此需重新入队相关 Pod 以重新评估冲突。
		c.enqueueAllPodsForCSIDriver(csiDriver.Name)
	}
}

// enqueueAllPodsForCSIDriver 把缓存中记录的使用指定 CSI 驱动的所有 Pod 入队。
func (c *Controller) enqueueAllPodsForCSIDriver(csiDriverName string) {
	podKeys := c.labelCache.GetPodsForCSIDriver(csiDriverName)
	for _, podKey := range podKeys {
		c.queue.Add(podKey)
	}
}

// Run 启动控制器：等待缓存同步后运行 workers 个工作协程处理队列，
// 直到 ctx 结束再排空队列并关闭事件广播器。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting SELinux warning controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down SELinux warning controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	c.eventBroadcaster.StartStructuredLogging(3) // 与其他 KCM 控制器保持一致，使用日志级别 3
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.kubeClient.CoreV1().Events("")})
	defer c.eventBroadcaster.Shutdown()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.podsSynced, c.pvcsSynced, c.pvsSynced, c.csiDriversSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 持续处理队列中的条目，直到 processNextWorkItem 返回 false（队列关闭）。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 从队列取出一个 Pod 并同步；
// 失败时记录错误并按速率限制重新入队，成功时调用 Forget 重置退避。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(item)

	err := c.sync(ctx, item)
	if err == nil {
		c.queue.Forget(item)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %w", item, err))
	c.queue.AddRateLimited(item)

	return true
}

// sync 对可能需要处理的 Pod 执行一次同步；返回错误时该 Pod 会被重新入队。
// Pod 被删除或进入终态时走删除路径，从标签缓存中移除其卷。
func (c *Controller) sync(ctx context.Context, podRef cache.ObjectName) error {
	logger := klog.FromContext(ctx)

	pod, err := c.podLister.Pods(podRef.Namespace).Get(podRef.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Pod 已从 API 中消失，按删除处理。
			return c.syncPodDelete(ctx, podRef)
		}
		logger.V(5).Info("Error getting pod from informer", "pod", klog.KObj(pod), "podUID", pod.UID, "err", err)
		return err
	}
	if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
		// Pod 已进入终态，kubelet 正在卸载卷，应将其从缓存中移除。
		return c.syncPodDelete(ctx, podRef)
	}

	return c.syncPod(ctx, pod)
}

// syncPodDelete 从标签缓存中移除指定 Pod 的所有卷记录。
func (c *Controller) syncPodDelete(ctx context.Context, podKey cache.ObjectName) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Deleting pod", "key", podKey)

	c.labelCache.DeletePod(logger, podKey)
	return nil
}

// syncPod 计算 Pod 每个卷的 SELinux 标签并写入标签缓存，
// 聚合处理过程中遇到的错误返回（部分失败不阻断其他卷）。
func (c *Controller) syncPod(ctx context.Context, pod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Syncing pod", "pod", klog.KObj(pod))

	errs := []error{}
	volumeSpecs := make(map[string]*volume.Spec)

	// 预先解析所有卷的 Spec，避免在挂载遍历中重复处理。
	for i := range pod.Spec.Volumes {
		spec, err := util.CreateVolumeSpec(logger, pod.Spec.Volumes[i], pod, c.vpm, c.pvcLister, c.pvLister, c.cmpm, c.csiTranslator)
		if err != nil {
			// PVC 或 PV 尚未创建时这是常见情况：记录错误但继续处理其余卷。
			errs = append(errs, err)
			continue
		}
		volumeSpecs[pod.Spec.Volumes[i].Name] = spec
	}

	mounts, _, seLinuxLabels := volumeutil.GetPodVolumeNames(pod, true /* collectSELinuxOptions */)
	for _, mount := range mounts.UnsortedList() {
		opts := seLinuxLabels[mount]
		spec, found := volumeSpecs[mount]
		if !found {
			// 该卷在上一步 CreateVolumeSpec 失败，错误已记录，跳过即可。
			logger.V(4).Info("skipping not found volume", "pod", klog.KObj(pod), "volume", mount)
			continue
		}
		mountInfo, err := volumeutil.GetMountSELinuxLabel(spec, opts, pod.Spec.SecurityContext, c.vpm, c.seLinuxTranslator)
		if err != nil {
			errors.Is(err, &volumeutil.MultipleSELinuxLabelsError{})
			if volumeutil.IsMultipleSELinuxLabelsError(err) {
				c.eventRecorder.Eventf(pod, v1.EventTypeWarning, "MultipleSELinuxLabels", "Volume %q is mounted twice with different SELinux labels inside this pod", mount)
			}
			logger.V(4).Info("failed to get SELinux label", "pod", klog.KObj(pod), "volume", mount, "err", err)
			errs = append(errs, err)
			continue
		}

		// 使用与 kubelet 挂载时 -o context 相同的标签。
		// 若 Pod 选择了 Recursive 策略，这里标签为空，不参与冲突检测。
		seLinuxLabel := mountInfo.SELinuxMountLabel

		err = c.syncVolume(logger, pod, spec, seLinuxLabel, mountInfo.PluginSupportsSELinuxContextMount)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errorutils.NewAggregate(errs)
}

// syncVolume 将单个卷及其 SELinux 标签、变更策略写入标签缓存，并上报检测到的冲突事件。
func (c *Controller) syncVolume(logger klog.Logger, pod *v1.Pod, spec *volume.Spec, seLinuxLabel string, pluginSupportsSELinuxContextMount bool) error {
	plugin, err := c.vpm.FindPluginBySpec(spec)
	if err != nil {
		// 控制器只加载与 SELinux 相关的插件，未知插件的卷直接跳过。
		logger.V(4).Info("Skipping volume of unknown plugin", "volume", spec.Name(), "err", err)
		return nil
	}

	uniqueVolumeName, err := volumeutil.GetUniqueVolumeNameFromSpec(plugin, spec)
	if err != nil {
		return fmt.Errorf("failed to get unique volume name for volume %q: %w", spec.Name(), err)
	}

	changePolicy := v1.SELinuxChangePolicyMountOption
	if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.SELinuxChangePolicy != nil {
		changePolicy = *pod.Spec.SecurityContext.SELinuxChangePolicy
		logger.V(5).Info("Using Pod SELinux change policy", "pod", klog.KObj(pod), "changePolicy", changePolicy)
	}
	if !pluginSupportsSELinuxContextMount && changePolicy != v1.SELinuxChangePolicyRecursive {
		logger.V(5).Info("Volume does not support SELinux context mount, setting changePolicy to Recursive", "pod", klog.KObj(pod), "volume", spec.Name())
		changePolicy = v1.SELinuxChangePolicyRecursive
	}

	if seLinuxLabel == "" && changePolicy != v1.SELinuxChangePolicyRecursive {
		logger.V(5).Info("Pod has empty SELinux label, setting changePolicy to Recursive", "pod", klog.KObj(pod))
		changePolicy = v1.SELinuxChangePolicyRecursive
	}

	csiDriver, err := csi.GetCSIDriverName(spec)
	if err != nil {
		// 获取 CSI 驱动名失败，说明这很可能不是 CSI 卷。
		csiDriver = ""
	}
	logger.V(4).Info("Syncing pod volume", "pod", klog.KObj(pod), "volume", spec.Name(), "label", seLinuxLabel, "uniqueVolumeName", uniqueVolumeName, "changePolicy", changePolicy, "csiDriver", csiDriver)

	conflicts := c.labelCache.AddVolume(logger, uniqueVolumeName, cache.MetaObjectToName(pod), seLinuxLabel, changePolicy, csiDriver)
	c.reportConflictEvents(logger, conflicts)

	return nil
}

// reportConflictEvents 为检测到的每个卷冲突发出 Normal 事件；
// 若冲突涉及的 Pod 已被删除（说明冲突已自然消解），则跳过上报。
func (c *Controller) reportConflictEvents(logger klog.Logger, conflicts []volumecache.Conflict) {
	for _, conflict := range conflicts {
		pod, err := c.podLister.Pods(conflict.Pod.Namespace).Get(conflict.Pod.Name)
		if err != nil {
			logger.V(2).Info("failed to get first pod for event", "pod", conflict.Pod, "err", err)
			// 冲突因其中一个 Pod 被删除而自然消解，再上报没有意义。
			return
		}
		c.eventRecorder.Event(pod, v1.EventTypeNormal, conflict.EventReason, conflict.EventMessage())
	}
}
