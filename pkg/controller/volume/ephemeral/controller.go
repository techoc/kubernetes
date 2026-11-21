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

package ephemeral

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	ephemeralvolumemetrics "k8s.io/kubernetes/pkg/controller/volume/ephemeral/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/events"
)

// Controller 为 Pod 规格中的临时内联卷（ephemeral inline volume）创建 PVC。
type Controller interface {
	// Run 启动控制器，使用 workers 个 worker 并发处理队列，直至 ctx 被取消。
	Run(ctx context.Context, workers int)
}

// ephemeralController 是 Controller 的实现，通过 Pod 与 PVC 的 informer
// 维护临时卷对应的 PVC 生命周期。
// ephemeralController 是 Controller 的实现，通过 Pod 与 PVC 的 informer
// 维护临时卷对应的 PVC 生命周期。
type ephemeralController struct {
	// kubeClient 供 volumehost 与 API 服务器通信，用于创建 PVC。
	kubeClient clientset.Interface

	// pvcLister 是共享的 PVC lister，与其他控制器共用，
	// 因此缓存中的 PVC 对象必须视为不可变，不得就地修改。
	pvcLister corelisters.PersistentVolumeClaimLister
	// pvcsSynced 用于等待 PVC informer 缓存同步完成。
	pvcsSynced cache.InformerSynced

	// podLister 是共享的 Pod lister，与其他控制器共用，
	// 因此缓存中的 Pod 对象必须视为不可变。
	podLister corelisters.PodLister
	// podSynced 用于等待 Pod informer 缓存同步完成。
	podSynced cache.InformerSynced

	// podIndexer 安装了通用的 PodPVC 索引，删除 PVC 时可直接定位引用它的 Pod，
	// 避免遍历全部 Pod。
	podIndexer cache.Indexer

	// recorder 用于把卷处理失败记录为 API 事件。
	recorder record.EventRecorder

	// queue 保存待处理 Pod 的命名空间/名称键，失败后按速率限制重试。
	queue workqueue.TypedRateLimitingInterface[string]
}

// NewController 创建临时卷控制器，注册 Pod 与 PVC 事件处理器并安装 PVC 索引。
func NewController(
	ctx context.Context,
	kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer) (Controller, error) {

	ec := &ephemeralController{
		kubeClient: kubeClient,
		podLister:  podInformer.Lister(),
		podIndexer: podInformer.Informer().GetIndexer(),
		podSynced:  podInformer.Informer().HasSynced,
		pvcLister:  pvcInformer.Lister(),
		pvcsSynced: pvcInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ephemeral_volume"},
		),
	}

	ephemeralvolumemetrics.RegisterMetrics()

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	ec.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "ephemeral_volume"})

	logger := klog.FromContext(ctx)
	_, err := podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: ec.enqueuePod,
		// Pod 规格不可变，更新事件不会带来需要同步到 PVC 的变化，可忽略；
		// Pod 删除后 PVC 会由属主引用触发垃圾回收自动清理，也无需处理。
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add pod event handler: %w", err)
	}
	_, err = pvcInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		DeleteFunc: ec.onPVCDelete,
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add pvc event handler: %w", err)
	}
	if err := common.AddPodPVCIndexerIfNotPresent(ec.podIndexer); err != nil {
		return nil, fmt.Errorf("could not initialize ephemeral volume controller: %w", err)
	}

	return ec, nil
}

// enqueuePod 在 Pod 包含临时内联卷且尚未进入删除流程时，把它加入工作队列。
// 没有临时卷的 Pod 无需处理，避免无谓占用 worker。
// enqueuePod 在 Pod 包含临时内联卷且尚未进入删除流程时，把它加入工作队列。
// 没有临时卷的 Pod 无需处理，避免无谓占用 worker。
func (ec *ephemeralController) enqueuePod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}

	// 已进入删除流程的 Pod 不再为其创建 PVC，否则新建资源会被立即回收。
	if pod.DeletionTimestamp != nil {
		return
	}

	for _, vol := range pod.Spec.Volumes {
		if vol.Ephemeral != nil {
			// 只要有一个临时卷就需要处理该 Pod。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(pod)
			if err != nil {
				runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pod, err))
				return
			}
			ec.queue.Add(key)
			break
		}
	}
}

// onPVCDelete 在 PVC 被删除后重新入队引用它的 Pod，以便重建临时卷 PVC。
// 索引器已把范围限定为引用该 PVC 的 Pod，无需遍历全部 Pod。
// onPVCDelete 在 PVC 被删除后重新入队引用它的 Pod，以便重建临时卷 PVC。
// 索引器已把范围限定为引用该 PVC 的 Pod，无需遍历全部 Pod。
func (ec *ephemeralController) onPVCDelete(obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		return
	}

	// 无论有意还是误删，只要仍有 Pod 通过临时卷引用该 PVC，就需要重建。
	// 通用索引器已预先过滤出引用它的 Pod。
	objs, err := ec.podIndexer.ByIndex(common.PodPVCIndex, fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
	if err != nil {
		runtime.HandleError(fmt.Errorf("listing pods from cache: %v", err))
		return
	}
	for _, obj := range objs {
		ec.enqueuePod(obj)
	}
}

// Run 等待 informer 缓存同步后启动 workers 个 worker 处理队列，
// 并在 ctx 取消时关闭队列、等待 worker 退出。
// Run 等待 informer 缓存同步后启动 workers 个 worker 处理队列，
// 并在 ctx 取消时关闭队列、等待 worker 退出。
func (ec *ephemeralController) Run(ctx context.Context, workers int) {
	defer runtime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting ephemeral volume controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down ephemeral volume controller")
		ec.queue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前不能处理事件，否则 lister 返回的数据不完整会导致误判。
	// 缓存未同步前不能处理事件，否则 lister 返回的数据不完整会导致误判。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, ec.podSynced, ec.pvcsSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, ec.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 持续处理队列，直到队列关闭。
// runWorker 持续处理队列，直到队列关闭。
func (ec *ephemeralController) runWorker(ctx context.Context) {
	for ec.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理一个队列元素：成功时清除重试计数，失败时限速重试。
// 返回 false 表示队列已关闭，worker 应退出。
// processNextWorkItem 处理一个队列元素：成功时清除重试计数，失败时限速重试。
// 返回 false 表示队列已关闭，worker 应退出。
func (ec *ephemeralController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := ec.queue.Get()
	if shutdown {
		// 队列已关闭，通知 worker 退出，避免继续空转。
		return false
	}
	defer ec.queue.Done(key)

	err := ec.syncHandler(ctx, key)
	if err == nil {
		// 成功后清除失败历史，防止后续失败继承过长的退避间隔。
		ec.queue.Forget(key)
		return true
	}

	// 失败时限速重试，避免冲突等暂时性错误导致忙等。
	runtime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	ec.queue.AddRateLimited(key)

	return true
}

// syncHandler 处理一个可能需要创建临时卷 PVC 的 Pod。
// 返回错误会导致该 Pod 重新入队重试。
func (ec *ephemeralController) syncHandler(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// 键格式非法无法通过重试修复，直接返回错误交由上层记录。
		return err
	}
	pod, err := ec.podLister.Pods(namespace).Get(name)
	logger := klog.FromContext(ctx)
	if err != nil {
		if errors.IsNotFound(err) {
			// Pod 已消失，无需重试；队列中的旧事件自然过期即可。
			logger.V(5).Info("Ephemeral: nothing to do for pod, it is gone", "podKey", key)
			return nil
		}
		logger.V(5).Info("Error getting pod from informer", "pod", klog.KObj(pod), "podUID", pod.UID, "err", err)
		return err
	}

	// 正在删除的 Pod 无需再创建 PVC，返回 nil 避免无意义重试。
	if pod.DeletionTimestamp != nil {
		logger.V(5).Info("Ephemeral: nothing to do for pod, it is marked for deletion", "podKey", key)
		return nil
	}

	for _, vol := range pod.Spec.Volumes {
		if err := ec.handleVolume(ctx, pod, vol); err != nil {
			// 卷处理失败时记录 Pod 事件，方便用户从 Pod 描述中看到原因。
			ec.recorder.Event(pod, v1.EventTypeWarning, events.FailedBinding, fmt.Sprintf("ephemeral volume %s: %v", vol.Name, err))
			return fmt.Errorf("pod %s, ephemeral volume %s: %v", key, vol.Name, err)
		}
	}

	return nil
}

// handleVolume 处理 Pod 的单个卷：仅临时内联卷需要创建 PVC。
// 已存在但属主不匹配的 PVC 视为冲突，返回错误以触发重试。
func (ec *ephemeralController) handleVolume(ctx context.Context, pod *v1.Pod, vol v1.Volume) error {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Ephemeral: checking volume", "volumeName", vol.Name)
	// 普通卷不归本控制器管理，直接跳过。
	// 普通卷不归本控制器管理，直接跳过。
	if vol.Ephemeral == nil {
		return nil
	}

	pvcName := ephemeral.VolumeClaimName(pod, &vol)
	pvc, err := ec.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
	if err != nil && !errors.IsNotFound(err) {
		// 非 NotFound 的读取错误无法判断 PVC 是否已经存在，
		// 必须重试，避免误建重复资源。
		return err
	}
	if pvc != nil {
		// 同名 PVC 若不属于该 Pod，属于命名冲突，不能复用也不能覆盖。
		if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
			return err
		}
		// Already created, nothing more to do.
		logger.V(5).Info("Ephemeral: PVC already created", "volumeName", vol.Name, "PVC", klog.KObj(pvc))
		return nil
	}

	// 以 Pod 为属主创建 PVC：属主引用使 Pod 删除时 PVC 能被垃圾回收。
	isTrue := true
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               pod.Name,
					UID:                pod.UID,
					Controller:         &isTrue,
					BlockOwnerDeletion: &isTrue,
				},
			},
			Annotations: vol.Ephemeral.VolumeClaimTemplate.Annotations,
			Labels:      vol.Ephemeral.VolumeClaimTemplate.Labels,
		},
		Spec: vol.Ephemeral.VolumeClaimTemplate.Spec,
	}
	ephemeralvolumemetrics.EphemeralVolumeCreateAttempts.Inc()
	_, err = ec.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		// 创建失败（如冲突）通过返回错误进入限速重试；指标单独计数便于告警。
		ephemeralvolumemetrics.EphemeralVolumeCreateFailures.Inc()
		return fmt.Errorf("create PVC %s: %v", pvcName, err)
	}
	return nil
}
