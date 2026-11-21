/*
Copyright 2017 The Kubernetes Authors.

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

package expand

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller/volume/events"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

const (
	// 默认卷扩容 worker 数量。
	defaultWorkerCount = 10
)

// ExpandController 负责触发持久卷（PV）的扩容流程。
type ExpandController interface {
	// Run 启动控制器，处理队列中的扩容请求，直到 ctx 被取消。
	Run(ctx context.Context)
}

// CSINameTranslator 根据 in-tree 插件名解析出对应的 CSI 驱动名。
type CSINameTranslator interface {
	// GetCSINameFromInTreeName 返回 in-tree 插件名对应的 CSI 驱动名。
	GetCSINameFromInTreeName(pluginName string) (string, error)
}

// Deprecated: 此控制器已被废弃，目前存在的唯一目的是在必要时添加注解，
// 以便卷可以在控制平面中被外部组件扩容。
type expandController struct {
	// kubeClient 是 volumehost 用来与 API server 通信的客户端。
	kubeClient clientset.Interface

	// pvcLister 是从 API server 获取和缓存 PVC 的共享 lister。
	// 由于与其他控制器共享，存储中的 PVC 对象必须视为不可变，禁止就地修改。
	pvcLister  corelisters.PersistentVolumeClaimLister
	pvcsSynced cache.InformerSynced

	// volumePluginMgr 用于初始化和获取卷插件。
	volumePluginMgr volume.VolumePluginMgr

	// recorder 用于向 API server 记录事件。
	recorder record.EventRecorder

	operationGenerator operationexecutor.OperationGenerator

	queue workqueue.TypedRateLimitingInterface[string]

	translator CSINameTranslator

	csiMigratedPluginManager csimigration.PluginManager
}

// NewExpandController 创建用于扩容 PV 的控制器实例，注册 PVC 事件处理器并初始化卷插件。
func NewExpandController(
	ctx context.Context,
	kubeClient clientset.Interface,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	plugins []volume.VolumePlugin,
	translator CSINameTranslator,
	csiMigratedPluginManager csimigration.PluginManager) (ExpandController, error) {

	expc := &expandController{
		kubeClient: kubeClient,
		pvcLister:  pvcInformer.Lister(),
		pvcsSynced: pvcInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "volume_expand"},
		),
		translator:               translator,
		csiMigratedPluginManager: csiMigratedPluginManager,
	}

	// 插件初始化失败意味着后续无法判断卷是否可扩容，必须直接返回错误而不是带病运行。
	if err := expc.volumePluginMgr.InitPlugins(plugins, nil, expc); err != nil {
		return nil, fmt.Errorf("could not initialize volume plugins for Expand Controller : %+v", err)
	}

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(3)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	expc.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "volume_expand"})
	blkutil := volumepathhandler.NewBlockVolumePathHandler()

	expc.operationGenerator = operationexecutor.NewOperationGenerator(
		kubeClient,
		&expc.volumePluginMgr,
		expc.recorder,
		blkutil)

	logger := klog.FromContext(ctx)
	_, err := pvcInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: expc.enqueuePVC,
		// 只在请求容量或状态容量真的增大时才入队，避免 PVC 上无关字段的更新
		// （例如注解变化）造成无意义的扩容同步。
		UpdateFunc: func(old, new interface{}) {
			oldPVC, ok := old.(*v1.PersistentVolumeClaim)
			if !ok {
				return
			}

			oldReq := oldPVC.Spec.Resources.Requests[v1.ResourceStorage]
			oldCap := oldPVC.Status.Capacity[v1.ResourceStorage]
			newPVC, ok := new.(*v1.PersistentVolumeClaim)
			if !ok {
				return
			}
			newReq := newPVC.Spec.Resources.Requests[v1.ResourceStorage]
			newCap := newPVC.Status.Capacity[v1.ResourceStorage]
			// PVC 在两种情况下入队：
			// 1. 用户增大了 PVC 的请求容量 —— 卷需要扩容；
			// 2. PVC 的状态容量已增大 —— 绑定的 PV 很可能刚完成文件系统扩容，
			//    需要移除 PV 上的 AnnPreResizeCapacity 注解。
			if newReq.Cmp(oldReq) > 0 || newCap.Cmp(oldCap) > 0 {
				expc.enqueuePVC(new)
			}
		},
		DeleteFunc: expc.enqueuePVC,
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add pvc event handler: %w", err)
	}

	return expc, nil
}

// enqueuePVC 把处于 Bound 状态的 PVC 加入工作队列；其他状态的 PVC 无需扩容。
func (expc *expandController) enqueuePVC(obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		return
	}

	// 只有已绑定的 PVC 才有对应的 PV 可供扩容。
	if pvc.Status.Phase == v1.ClaimBound {
		// 使用可处理 tombstone 的 key 函数，删除事件中的对象也能正确入队。
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(pvc)
		if err != nil {
			runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pvc, err))
			return
		}
		expc.queue.Add(key)
	}
}

// processNextWorkItem 从队列取出一个 key 并执行同步；出错时限速重试，
// 直到队列关闭返回 false 通知 worker 退出。
func (expc *expandController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := expc.queue.Get()
	if shutdown {
		return false
	}
	defer expc.queue.Done(key)

	err := expc.syncHandler(ctx, key)
	if err == nil {
		// 成功后清空该 key 的失败计数，避免后续偶发失败被过度退避。
		expc.queue.Forget(key)
		return true
	}

	// 失败必须限速重试：扩容可能依赖外部控制器，瞬间重试只会制造压力。
	runtime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	expc.queue.AddRateLimited(key)

	return true
}

// syncHandler 执行卷扩容的实际协调逻辑。
// 若返回错误，PVC 会被重新入队以便后续重试。
func (expc *expandController) syncHandler(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pvc, err := expc.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	// PVC 已被删除：无需扩容，直接忽略，避免把已删除对象反复入队。
	if apierrors.IsNotFound(err) {
		return nil
	}
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(5).Info("Error getting PVC from informer", "pvcKey", key, "err", err)
		return err
	}

	pv, err := expc.getPersistentVolume(ctx, pvc)
	if err != nil {
		logger.V(5).Info("Error getting Persistent Volume for PVC from informer", "pvcKey", key, "pvcUID", pvc.UID, "err", err)
		return err
	}

	// 必须确认 PVC 与 PV 双向绑定关系完整（UID 匹配），否则可能扩容到错误的卷。
	if pv.Spec.ClaimRef == nil || pvc.Namespace != pv.Spec.ClaimRef.Namespace || pvc.UID != pv.Spec.ClaimRef.UID {
		err := fmt.Errorf("persistent Volume is not bound to PVC being updated : %s", key)
		logger.V(4).Info("", "err", err)
		return err
	}

	pvcRequestSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcStatusSize := pvc.Status.Capacity[v1.ResourceStorage]

	// 仅在两种情况下继续扩容：
	// 1. PVC 请求容量大于当前状态容量（用户提交了扩容请求）；
	// 2. PV 上仍残留扩容前容量注解（需要清理或完成后续步骤）。
	if pvcRequestSize.Cmp(pvcStatusSize) <= 0 && !metav1.HasAnnotation(pv.ObjectMeta, util.AnnPreResizeCapacity) {
		return nil
	}

	volumeSpec := volume.NewSpecFromPersistentVolume(pv, false)
	migratable, err := expc.csiMigratedPluginManager.IsMigratable(volumeSpec)
	if err != nil {
		logger.V(4).Info("Failed to check CSI migration status for PVC with error", "pvcKey", key, "err", err)
		return nil
	}
	// 必须先处理 CSI 迁移场景，再调用 FindExpandablePluginBySpec 查找 in-tree 插件。
	// CSI 迁移场景必须先处理：此时扩容应由外部 resizer 完成，
	// 本控制器只负责打注解，不能再走 in-tree 插件的扩容路径。
	if migratable {
		inTreePluginName, err := expc.csiMigratedPluginManager.GetInTreePluginNameFromSpec(volumeSpec.PersistentVolume, volumeSpec.Volume)
		if err != nil {
			logger.V(4).Info("Error getting in-tree plugin name from persistent volume", "volumeName", volumeSpec.PersistentVolume.Name, "err", err)
			return err
		}

		// 记录正常事件告知用户扩容已交由外部 resizer 处理，避免用户误以为卡住。
		msg := fmt.Sprintf("CSI migration enabled for %s; waiting for external resizer to expand the pvc", inTreePluginName)
		expc.recorder.Event(pvc, v1.EventTypeNormal, events.ExternalExpanding, msg)
		csiResizerName, err := expc.translator.GetCSINameFromInTreeName(inTreePluginName)
		if err != nil {
			// 无法确定 resizer 名称时记录警告事件并返回错误，让工作队列重试。
			errorMsg := fmt.Sprintf("error getting CSI driver name for pvc %s, with error %v", key, err)
			expc.recorder.Event(pvc, v1.EventTypeWarning, events.ExternalExpanding, errorMsg)
			return errors.New(errorMsg)
		}

		// 把 resizer 名称写入 PVC 注解，外部 resizer 依赖该注解识别需要处理的 PVC。
		pvc, err := util.SetClaimResizer(pvc, csiResizerName, expc.kubeClient)
		if err != nil {
			errorMsg := fmt.Sprintf("error setting resizer annotation to pvc %s, with error %v", key, err)
			expc.recorder.Event(pvc, v1.EventTypeWarning, events.ExternalExpanding, errorMsg)
			return errors.New(errorMsg)
		}
		return nil
	}

	volumePlugin, err := expc.volumePluginMgr.FindExpandablePluginBySpec(volumeSpec)
	if err != nil || volumePlugin == nil {
		msg := "waiting for an external controller to expand this PVC"
		eventType := v1.EventTypeNormal
		// 查找插件报错属于异常情况，用 Warning 区分于正常的外部扩容等待。
		if err != nil {
			eventType = v1.EventTypeWarning
		}
		expc.recorder.Event(pvc, eventType, events.ExternalExpanding, msg)
		logger.Info("Waiting for an external controller to expand the PVC", "pvcKey", key, "pvcUID", pvc.UID)
		// 预期由外部插件处理扩容，继续排队只会无意义地重复轮询，因此直接返回成功。
		return nil
	}

	volumeResizerName := volumePlugin.GetPluginName()
	return expc.expand(logger, pvc, pv, volumeResizerName)
}

// expand 根据特性开关选择扩容路径，生成并执行扩容操作。
func (expc *expandController) expand(logger klog.Logger, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, resizerName string) error {
	// 若节点扩容已完成且 PV 上留有扩容前容量注解，先清理注解并结束：
	// 该注解仅用于扩容过程中的回滚判断，完成后不应继续保留。
	if expc.isNodeExpandComplete(logger, pvc, pv) && metav1.HasAnnotation(pv.ObjectMeta, util.AnnPreResizeCapacity) {
		return util.DeleteAnnPreResizeCapacity(pv, expc.GetKubeClient())
	}

	var generatedOptions volumetypes.GeneratedOperations
	var err error
	// 开启失败恢复特性时，使用同时支持扩容与失败恢复的操作生成路径。
	if utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure) {
		generatedOptions, err = expc.operationGenerator.GenerateExpandAndRecoverVolumeFunc(pvc, pv, resizerName)
		if err != nil {
			logger.Error(err, "Error starting ExpandVolume for pvc", "PVC", klog.KObj(pvc))
			return err
		}
	} else {
		// 旧路径：先在 PVC 上标记正在扩容及使用的 resizer，防止重复操作。
		pvc, err := util.MarkResizeInProgressWithResizer(pvc, resizerName, expc.kubeClient)
		if err != nil {
			logger.Error(err, "Error setting PVC in progress with error", "PVC", klog.KObj(pvc), "err", err)
			return err
		}

		generatedOptions, err = expc.operationGenerator.GenerateExpandVolumeFunc(pvc, pv)
		if err != nil {
			logger.Error(err, "Error starting ExpandVolume for pvc with error", "PVC", klog.KObj(pvc), "err", err)
			return err
		}
	}

	// 执行生成的操作链；返回的错误会让工作队列限速重试。
	logger.V(5).Info("Starting ExpandVolume for volume", "volumeName", util.GetPersistentVolumeClaimQualifiedName(pvc))
	_, detailedErr := generatedOptions.Run()

	return detailedErr
}

// TODO 让并发度可配置（通过 workers 参数）。此前 nestedpendingoperations 会派生无限数量的 goroutine。
// Run 启动控制器：等待缓存同步后开启固定数量的 worker，直到 ctx 被取消。
func (expc *expandController) Run(ctx context.Context) {
	defer runtime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting expand controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down expand controller")
		// 先关闭队列让 worker 的 Get 解锁退出，再等待全部 worker 结束，避免遗漏进行中的任务。
		expc.queue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前 lister 中数据不完整，此时处理 PVC 会误判扩容状态。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, expc.pvcsSynced) {
		return
	}

	for i := 0; i < defaultWorkerCount; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, expc.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 持续处理工作队列中的条目，直到 processNextWorkItem 报告队列已关闭。
func (expc *expandController) runWorker(ctx context.Context) {
	for expc.processNextWorkItem(ctx) {
	}
}

// getPersistentVolume 根据 PVC 绑定的 VolumeName 从 API server 获取对应的 PV。
func (expc *expandController) getPersistentVolume(ctx context.Context, pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	volumeName := pvc.Spec.VolumeName
	pv, err := expc.kubeClient.CoreV1().PersistentVolumes().Get(ctx, volumeName, metav1.GetOptions{})

	// 包装错误以保留卷名上下文，方便定位是哪块卷的 PV 获取失败。
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %q: %v", volumeName, err)
	}

	return pv, nil
}

// isNodeExpandComplete 判断节点扩容是否完成：pvc.Status.Capacity >= pv.Spec.Capacity。
func (expc *expandController) isNodeExpandComplete(logger klog.Logger, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) bool {
	logger.V(4).Info("pv and pvc capacity", "PV", klog.KObj(pv), "pvCapacity", pv.Spec.Capacity[v1.ResourceStorage], "PVC", klog.KObj(pvc), "pvcCapacity", pvc.Status.Capacity[v1.ResourceStorage])
	pvcSpecCap := pvc.Spec.Resources.Requests.Storage()
	pvcStatusCap, pvCap := pvc.Status.Capacity[v1.ResourceStorage], pv.Spec.Capacity[v1.ResourceStorage]

	// 由于允许缩容，必须同时检查 PVC 的实际容量是否达到请求容量、以及是否达到 PV 的规格容量，
	// 只与其中一个比较会在缩容场景下误判完成。
	if pvcStatusCap.Cmp(*pvcSpecCap) >= 0 && pvcStatusCap.Cmp(pvCap) >= 0 {
		return true
	}
	return false
}

// 以下方法实现 VolumeHost 接口。扩容控制器不需要本地挂载路径，故路径类方法返回空串。
// GetPluginDir 返回插件目录（此控制器不使用，始终为空）。
func (expc *expandController) GetPluginDir(pluginName string) string {
	return ""
}

// GetVolumeDevicePluginDir 返回卷设备插件目录（此控制器不使用，始终为空）。
func (expc *expandController) GetVolumeDevicePluginDir(pluginName string) string {
	return ""
}

// GetPodsDir 返回 pod 目录（此控制器不使用，始终为空）。
func (expc *expandController) GetPodsDir() string {
	return ""
}

// GetPodVolumeDir 返回 pod 的卷目录（此控制器不使用，始终为空）。
func (expc *expandController) GetPodVolumeDir(podUID types.UID, pluginName string, volumeName string) string {
	return ""
}

// GetPodVolumeDeviceDir 返回 pod 的卷设备目录（此控制器不使用，始终为空）。
func (expc *expandController) GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetPodPluginDir 返回 pod 的插件目录（此控制器不使用，始终为空）。
func (expc *expandController) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetKubeClient 返回控制器使用的 kube 客户端，供卷插件调用。
func (expc *expandController) GetKubeClient() clientset.Interface {
	return expc.kubeClient
}

// NewWrapperMounter 构造包装 mounter；扩容控制器不需要挂载卷，故返回不支持错误。
func (expc *expandController) NewWrapperMounter(volName string, spec volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return nil, fmt.Errorf("NewWrapperMounter not supported by expand controller's VolumeHost implementation")
}

// NewWrapperUnmounter 构造包装 unmounter；扩容控制器不需要卸载卷，故返回不支持错误。
func (expc *expandController) NewWrapperUnmounter(volName string, spec volume.Spec, podUID types.UID) (volume.Unmounter, error) {
	return nil, fmt.Errorf("NewWrapperUnmounter not supported by expand controller's VolumeHost implementation")
}

// GetMounter 返回 mounter 接口（此控制器不执行挂载操作，返回 nil）。
func (expc *expandController) GetMounter() mount.Interface {
	return nil
}

// GetNodeAllocatable 返回节点可分配资源；扩容控制器不关心节点资源，返回空列表。
func (expc *expandController) GetNodeAllocatable() (v1.ResourceList, error) {
	return v1.ResourceList{}, nil
}

// GetSecretFunc 返回读取 Secret 的函数；扩容场景不需要，返回的函数调用即报错。
func (expc *expandController) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	return func(_, _ string) (*v1.Secret, error) {
		return nil, fmt.Errorf("GetSecret unsupported in expandController")
	}
}

// GetConfigMapFunc 返回读取 ConfigMap 的函数；扩容场景不需要，返回的函数调用即报错。
func (expc *expandController) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	return func(_, _ string) (*v1.ConfigMap, error) {
		return nil, fmt.Errorf("GetConfigMap unsupported in expandController")
	}
}

// GetAttachedVolumesFromNodeStatus 返回节点状态中的挂载卷；扩容控制器不需要，返回空 map。
func (expc *expandController) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	return map[v1.UniqueVolumeName]string{}, nil
}

// GetServiceAccountTokenFunc 返回获取 ServiceAccount token 的函数；扩容场景不需要，调用即报错。
func (expc *expandController) GetServiceAccountTokenFunc() func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return nil, fmt.Errorf("GetServiceAccountToken unsupported in expandController")
	}
}

// DeleteServiceAccountTokenFunc 返回删除 ServiceAccount token 的函数；此实现仅记录不支持的日志。
func (expc *expandController) DeleteServiceAccountTokenFunc() func(types.UID) {
	return func(types.UID) {
		//nolint:logcheck
		klog.ErrorS(nil, "DeleteServiceAccountToken unsupported in expandController")
	}
}

// GetNodeLabels 返回节点标签；扩容控制器不需要，返回不支持错误。
func (expc *expandController) GetNodeLabels() (map[string]string, error) {
	return nil, fmt.Errorf("GetNodeLabels unsupported in expandController")
}

// GetNodeName 返回节点名（扩容控制器无节点上下文，返回空）。
func (expc *expandController) GetNodeName() types.NodeName {
	return ""
}

// GetEventRecorder 返回事件记录器，供卷插件记录扩容相关事件。
func (expc *expandController) GetEventRecorder() record.EventRecorder {
	return expc.recorder
}

// GetSubpather 返回 subpath 接口；扩容控制器不需要 subpath 功能，返回 nil。
func (expc *expandController) GetSubpather() subpath.Interface {
	// 扩容流程不涉及 subpath 挂载，故无需提供实现。
	return nil
}
