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

package statefulset

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/pkg/controller/statefulset/metrics"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
)

// controllerKind 是本控制器处理的 StatefulSet 的 GroupVersionKind。
var controllerKind = apps.SchemeGroupVersion.WithKind("StatefulSet")

// podKind 是 Pod 的 GroupVersionKind，用于匹配 ownerReference。
var podKind = v1.SchemeGroupVersion.WithKind("Pod")

// StatefulSetController 负责监听 StatefulSet 与 Pod 的变化并驱动其同步。
type StatefulSetController struct {
	// kubeClient 用于访问 Kubernetes API。
	kubeClient clientset.Interface
	// control 负责同步 StatefulSet；抽象为接口以便于测试。
	control StatefulSetControlInterface
	// podControl 用于 patch Pod。
	podControl controller.PodControlInterface
	// podIndexer 支持按 ControllerRef UID 查找 Pod。
	podIndexer cache.Indexer
	// podLister 从共享 informer 缓存中 list/get Pod。
	podLister corelisters.PodLister
	// podListerSynced 表示 Pod informer 是否至少同步过一次。
	podListerSynced cache.InformerSynced
	// setLister 从共享 informer 缓存中 list/get StatefulSet。
	setLister appslisters.StatefulSetLister
	// setListerSynced 表示 StatefulSet informer 是否至少同步过一次。
	setListerSynced cache.InformerSynced
	// pvcListerSynced 表示 PVC informer 是否至少同步过一次。
	pvcListerSynced cache.InformerSynced
	// revListerSynced 表示 ControllerRevision informer 是否至少同步过一次。
	revListerSynced cache.InformerSynced
	// queue 保存待同步的 StatefulSet key。
	queue workqueue.TypedRateLimitingInterface[string]
	// eventBroadcaster 是事件处理管道的核心。
	eventBroadcaster record.EventBroadcaster
	// clock 封装时间获取，便于测试中控制时间。
	clock clock.PassiveClock
	// consistencyStore 跟踪控制器写操作的状态，用于一致性校验。
	consistencyStore consistencyutil.ConsistencyStore
}

// 以下 GroupResource 用于一致性存储中按资源类型区分对象。
var (
	statefulSetGroupResource = schema.GroupResource{
		Group:    "apps",
		Resource: "statefulsets",
	}
	podGroupResource = schema.GroupResource{
		Group:    "",
		Resource: "pods",
	}
	persistentVolumeClaimGroupResource = schema.GroupResource{
		Group:    "",
		Resource: "persistentvolumeclaims",
	}
)

// NewStatefulSetController 创建一个新的 StatefulSet 控制器。
func NewStatefulSetController(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	setInformer appsinformers.StatefulSetInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	revInformer appsinformers.ControllerRevisionInformer,
	kubeClient clientset.Interface,
) *StatefulSetController {
	logger := klog.FromContext(ctx)
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "statefulset-controller"})

	// 注册控制器相关指标。
	metrics.Register()
	if err := history.AddControllerRevisionControllerIndexer(revInformer.Informer()); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to add controller revision controller indexer: %w", err))
	}

	var consistencyStore consistencyutil.ConsistencyStore
	var podWriteCallback func(pod *v1.Pod, rs *metav1.OwnerReference)
	// 仅在一致性特性开启时构造带写回调的一致性存储，避免无谓开销。
	if utilfeature.DefaultFeatureGate.Enabled(features.StaleControllerConsistencyStatefulSet) {
		consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			podGroupResource:                   podInformer.Informer().GetStore(),
			persistentVolumeClaimGroupResource: pvcInformer.Informer().GetStore(),
			statefulSetGroupResource:           setInformer.Informer().GetStore(),
		})
		podWriteCallback = func(pod *v1.Pod, ss *metav1.OwnerReference) {
			// owner 为空说明不是写操作关心的对象，直接跳过。
			if ss == nil {
				return
			}
			consistencyStore.WroteAt(
				types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      ss.Name,
				},
				ss.UID,
				podGroupResource,
				pod.ResourceVersion,
			)
		}
	} else {
		consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}
	ssc := &StatefulSetController{
		kubeClient: kubeClient,
		control: NewDefaultStatefulSetControl(
			NewStatefulPodControl(
				kubeClient,
				podInformer.Lister(),
				pvcInformer.Lister(),
				recorder,
				consistencyStore),
			NewRealStatefulSetStatusUpdater(kubeClient, setInformer.Lister(), consistencyStore),
			history.NewHistory(kubeClient, revInformer.Lister(), revInformer.Informer().GetIndexer()),
		),
		pvcListerSynced: pvcInformer.Informer().HasSynced,
		revListerSynced: revInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "statefulset"},
		),
		podControl: controller.RealPodControl{KubeClient: kubeClient, Recorder: recorder, OnWrite: podWriteCallback},

		eventBroadcaster: eventBroadcaster,
		clock:            clock.RealClock{},
		consistencyStore: consistencyStore,
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		// 为 Pod informer 注册事件处理函数，将相关变化转化为 StatefulSet 入队。
		AddFunc: func(obj interface{}) {
			ssc.addPod(logger, obj)
		},
		// 标签变化时需要同时检查新旧 ControllerRef 所属的 StatefulSet。
		UpdateFunc: func(oldObj, newObj interface{}) {
			ssc.updatePod(logger, oldObj, newObj)
		},
		// 删除事件需考虑 tombstone，因此使用 deletePod 处理。
		DeleteFunc: func(obj interface{}) {
			ssc.deletePod(logger, obj)
		},
	})
	ssc.podLister = podInformer.Lister()
	ssc.podListerSynced = podInformer.Informer().HasSynced
	controller.AddPodControllerIndexer(podInformer.Informer())
	ssc.podIndexer = podInformer.Informer().GetIndexer()
	setInformer.Informer().AddEventHandler(
		// StatefulSet 自身的变化同样入队；副本数变化时记录日志便于排查。
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ssc.enqueueStatefulSet(logger, obj)
			},
			UpdateFunc: func(old, cur interface{}) {
				oldPS := old.(*apps.StatefulSet)
				curPS := cur.(*apps.StatefulSet)
				if oldPS.Status.Replicas != curPS.Status.Replicas {
					logger.V(4).Info("Observed updated replica count for StatefulSet", "statefulSet", klog.KObj(curPS), "oldReplicas", oldPS.Status.Replicas, "newReplicas", curPS.Status.Replicas)
				}
				ssc.enqueueStatefulSet(logger, cur)
			},
			DeleteFunc: func(obj interface{}) {
				ssc.enqueueStatefulSet(logger, obj)
			},
		},
	)
	ssc.setLister = setInformer.Lister()
	ssc.setListerSynced = setInformer.Informer().HasSynced

	// TODO: Watch volumes
	return ssc
}

// Run 启动 StatefulSet 控制器的工作协程，直到 ctx 结束。
func (ssc *StatefulSetController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)

	// 启动事件处理管道，将事件同时输出到日志与 API server。
	ssc.eventBroadcaster.StartStructuredLogging(3)
	ssc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: ssc.kubeClient.CoreV1().Events("")})
	defer ssc.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting stateful set controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down statefulset controller")
		ssc.queue.ShutDown()
		wg.Wait()
	}()

	// 必须等所有 informer 同步完成后再启动 worker，避免缓存为空导致误判。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, ssc.podListerSynced, ssc.setListerSynced, ssc.pvcListerSynced, ssc.revListerSynced) {
		return
	}

	// 启动多个 worker 并发处理队列，提升吞吐。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, ssc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// addPod 将 Pod 所属的 StatefulSet 加入同步队列。
func (ssc *StatefulSetController) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)

	// 控制器重启后可能收到已处于删除中的 Pod；按删除处理避免误当作新建观察。
	if pod.DeletionTimestamp != nil {
		ssc.deletePod(logger, pod)
		return
	}

	// 有 ControllerRef 时直接同步其所属 set，这是最可靠的所有权依据。
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		set := ssc.resolveControllerRef(pod.Namespace, controllerRef)
		if set == nil {
			return
		}
		logger.V(4).Info("Pod created with labels", "pod", klog.KObj(pod), "labels", pod.Labels)
		ssc.enqueueStatefulSet(logger, set)
		return
	}

	// 孤儿 Pod：查找所有可能匹配的 set，让它们尝试接管。
	sets := ssc.getStatefulSetsForPod(logger, pod)
	if len(sets) == 0 {
		return
	}
	logger.V(4).Info("Orphan Pod created with labels", "pod", klog.KObj(pod), "labels", pod.Labels)
	for _, set := range sets {
		ssc.enqueueStatefulSet(logger, set)
	}
}

// updatePod 将新旧 Pod 相关的 StatefulSet 加入同步队列。
func (ssc *StatefulSetController) updatePod(logger klog.Logger, old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	// re-list 可能收到资源版本未变的更新事件；同一 Pod 不同版本 RV 必然不同，故可跳过。
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}

	// 记录标签是否变化，用于判断孤儿 Pod 是否需要考虑被接管。
	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)

	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	// ControllerRef 被修改时，旧控制器也需要同步以清理所有权。
	if controllerRefChanged && oldControllerRef != nil {
		if set := ssc.resolveControllerRef(oldPod.Namespace, oldControllerRef); set != nil {
			ssc.enqueueStatefulSet(logger, set)
		}
	}

	// 有 ControllerRef 时直接同步其所属 set。
	if curControllerRef != nil {
		set := ssc.resolveControllerRef(curPod.Namespace, curControllerRef)
		if set == nil {
			return
		}
		logger.V(4).Info("Pod objectMeta updated", "pod", klog.KObj(curPod), "oldObjectMeta", oldPod.ObjectMeta, "newObjectMeta", curPod.ObjectMeta)
		// 相位变化单独记日志，便于观察 Pod 生命周期。
		if oldPod.Status.Phase != curPod.Status.Phase {
			logger.V(4).Info("StatefulSet Pod phase changed", "pod", klog.KObj(curPod), "statefulSet", klog.KObj(set), "podPhase", curPod.Status.Phase)
		}
		ssc.enqueueStatefulSet(logger, set)
		// TODO: MinReadySeconds in the Pod will generate an Available condition to be added in
		// the Pod status which in turn will trigger a requeue of the owning replica set thus
		// having its status updated with the newly available replica.
		if !podutil.IsPodReady(oldPod) && podutil.IsPodReady(curPod) && set.Spec.MinReadySeconds > 0 {
			logger.V(2).Info("StatefulSet will be enqueued after minReadySeconds for availability check", "statefulSet", klog.KObj(set), "minReadySeconds", set.Spec.MinReadySeconds)
			// 当前队列无法为多个就绪时间不同的 Pod 分别计时；
			// 完整补偿在 syncStatefulSet 结尾处的再次入队中完成。
			ssc.enqueueSSAfter(logger, set, time.Duration(set.Spec.MinReadySeconds)*time.Second)
		}
		return
	}

	// 孤儿 Pod：仅在标签或所有权变化时，才让匹配的 set 尝试接管。
	if labelChanged || controllerRefChanged {
		sets := ssc.getStatefulSetsForPod(logger, curPod)
		if len(sets) == 0 {
			return
		}
		logger.V(4).Info("Orphan Pod objectMeta updated", "pod", klog.KObj(curPod), "oldObjectMeta", oldPod.ObjectMeta, "newObjectMeta", curPod.ObjectMeta)
		for _, set := range sets {
			ssc.enqueueStatefulSet(logger, set)
		}
	}
}

// deletePod 将已删除 Pod 所属的 StatefulSet 加入队列，并处理 tombstone。
func (ssc *StatefulSetController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	// 删除事件可能被丢弃，relist 时通过 tombstone 补发；其中的对象信息可能已过期。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Couldn't get object from tombstone", "obj", obj)
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Tombstone contained object that is not a pod", "type", fmt.Sprintf("%T", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// 孤儿 Pod 的删除无需任何控制器关心。
		return
	}
	set := ssc.resolveControllerRef(pod.Namespace, controllerRef)
	if set == nil {
		return
	}
	logger.V(4).Info("Pod deleted.", "pod", klog.KObj(pod), "caller", utilruntime.GetCaller())
	ssc.enqueueStatefulSet(logger, set)
}

// getPodsForStatefulSet 返回 set 应当管理的 Pod，并通过接管/放弃来调和 ControllerRef。
// 注意：返回的 Pod 是指向缓存对象的指针，修改前必须先拷贝。
// NOTE: Returned Pods are pointers to objects from the cache.
func (ssc *StatefulSetController) getPodsForStatefulSet(ctx context.Context, set *apps.StatefulSet, selector labels.Selector) ([]*v1.Pod, error) {
	podsForSts, err := controller.FilterPodsByOwner(ssc.podIndexer, &set.ObjectMeta, "StatefulSet", true)
	if err != nil {
		return nil, err
	}

	// filter 只认领名称匹配本 set 的 Pod，其余释放或忽略。
	filter := func(pod *v1.Pod) bool {
		return isMemberOf(set, pod)
	}

	cm := controller.NewPodControllerRefManager(ssc.podControl, set, selector, controllerKind, ssc.canAdoptFunc(ctx, set))
	return cm.ClaimPods(ctx, podsForSts, filter)
}

// canAdoptFunc 返回接管前的复查函数：在 list Pod/ControllerRevision 一段时间后再做一次无缓存读，
// 以避免在 set 已被删除后仍错误接管（见 #42639）。
func (ssc *StatefulSetController) canAdoptFunc(ctx context.Context, set *apps.StatefulSet) func(ctx2 context.Context) error {
	return controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := ssc.kubeClient.AppsV1().StatefulSets(set.Namespace).Get(ctx, set.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		// UID 不一致说明同名 set 已被重建，原 set 已不存在。
		if fresh.UID != set.UID {
			return nil, fmt.Errorf("original StatefulSet %v/%v is gone: got uid %v, wanted %v", set.Namespace, set.Name, fresh.UID, set.UID)
		}
		return fresh, nil
	})
}

// adoptOrphanRevisions 接管 selector 匹配且无控制器的 ControllerRevision。
func (ssc *StatefulSetController) adoptOrphanRevisions(ctx context.Context, set *apps.StatefulSet) error {
	revisions, err := ssc.control.ListRevisions(set)
	if err != nil {
		return err
	}
	orphanRevisions := make([]*apps.ControllerRevision, 0)
	for i := range revisions {
		// 只接管没有 owner 的 revision，避免抢夺其他控制器的对象。
		if metav1.GetControllerOf(revisions[i]) == nil {
			orphanRevisions = append(orphanRevisions, revisions[i])
		}
	}
	// 存在孤儿 revision 时，先复查 set 是否仍存在再执行接管。
	if len(orphanRevisions) > 0 {
		canAdoptErr := ssc.canAdoptFunc(ctx, set)(ctx)
		if canAdoptErr != nil {
			return fmt.Errorf("can't adopt ControllerRevisions: %v", canAdoptErr)
		}
		return ssc.control.AdoptOrphanRevisions(set, orphanRevisions)
	}
	return nil
}

// getStatefulSetsForPod 返回可能与给定 Pod 匹配的 StatefulSet 列表。
func (ssc *StatefulSetController) getStatefulSetsForPod(logger klog.Logger, pod *v1.Pod) []*apps.StatefulSet {
	sets, err := ssc.setLister.GetPodStatefulSets(pod)
	if err != nil {
		return nil
	}
	// 多个 set 选中同一 Pod 属于用户配置错误，记录错误但不中断处理。
	if len(sets) > 1 {
		// ControllerRef 会防止误操作，但多 set 匹配仍需上报。
		setNames := []string{}
		for _, s := range sets {
			setNames = append(setNames, s.Name)
		}
		utilruntime.HandleErrorWithLogger(logger, nil, "More than one StatefulSet is selecting pods with same labels", "podLabels", pod.Labels, "sets", setNames)
	}
	return sets
}

// resolveControllerRef 根据 ControllerRef 找到对应的 StatefulSet；无法匹配时返回 nil。
func (ssc *StatefulSetController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *apps.StatefulSet {
	// informer 不支持按 UID 查找，因此先按名称查找再校验 UID；Kind 不符时直接跳过。
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	set, err := ssc.setLister.StatefulSets(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	// 同名但不是同一个对象（UID 不同），视为无法解析。
	if set.UID != controllerRef.UID {
		return nil
	}
	return set
}

// enqueueStatefulSet 将给定 StatefulSet 的 key 加入工作队列。
func (ssc *StatefulSetController) enqueueStatefulSet(logger klog.Logger, obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Couldn't get key for object", "object", obj)
		return
	}
	ssc.queue.Add(key)
}

// enqueueSSAfter 在指定延迟后将 StatefulSet 的 key 加入队列，用于 MinReadySeconds 可用性复查。
func (ssc *StatefulSetController) enqueueSSAfter(logger klog.Logger, ss *apps.StatefulSet, duration time.Duration) {
	key, err := controller.KeyFunc(ss)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Couldn't get key for StatefulSet object", "statefulSet", klog.KObj(ss))
		return
	}
	ssc.queue.AddAfter(key, duration)
}

// processNextWorkItem 从队列取出一个 key 并处理，处理完成后标记完成；
// 同一 key 不会被并发处理。
func (ssc *StatefulSetController) processNextWorkItem(ctx context.Context) bool {
	key, quit := ssc.queue.Get()
	if quit {
		return false
	}
	// 无论成功失败都标记完成，避免队列项永久处于处理中状态。
	defer ssc.queue.Done(key)
	// 同步失败时限速重试，成功时清除该 key 的失败计数。
	if err := ssc.sync(ctx, key); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error syncing StatefulSet, requeuing", "key", key)
		ssc.queue.AddRateLimited(key)
	} else {
		ssc.queue.Forget(key)
	}
	return true
}

// worker 启动一个工作协程，循环处理队列直到队列关闭。
func (ssc *StatefulSetController) worker(ctx context.Context) {
	for ssc.processNextWorkItem(ctx) {
	}
}

// sync 同步给定 key 对应的 StatefulSet。
func (ssc *StatefulSetController) sync(ctx context.Context, key string) error {
	startTime := ssc.clock.Now()
	logger := klog.FromContext(ctx)
	// 记录每次同步耗时，便于性能观察。
	defer func() {
		logger.V(4).Info("Finished syncing statefulset", "key", key, "time", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	ssNamespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	// 同步前先确保一致性存储已就绪，否则跳过本次同步以避免基于陈旧状态操作。
	if err := ssc.consistencyStore.EnsureReady(ssNamespacedName); err != nil {
		var consistencyErr *consistencyutil.ConsistencyError
		if errors.As(err, &consistencyErr) {
			metrics.StatefulSetRequeueSkips.WithLabelValues(
				consistencyErr.GroupResource.Group,
				consistencyErr.GroupResource.Resource,
			).Inc()
		}
		return err
	}

	set, err := ssc.setLister.StatefulSets(namespace).Get(name)
	// set 已被删除，清理一致性状态后正常返回。
	if apierrors.IsNotFound(err) {
		ssc.consistencyStore.Clear(ssNamespacedName, "")
		logger.Info("StatefulSet has been deleted", "key", key)
		return nil
	}
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to retrieve StatefulSet from store", "key", key)
		return err
	}

	// selector 非法属于不可恢复错误，直接忽略而不再重试。
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error converting StatefulSet selector", "key", key)
		return nil
	}

	// 先接管孤儿 revision，保证后续同步使用完整的历史版本。
	if err := ssc.adoptOrphanRevisions(ctx, set); err != nil {
		return err
	}

	pods, err := ssc.getPodsForStatefulSet(ctx, set, selector)
	if err != nil {
		return err
	}

	return ssc.syncStatefulSet(ctx, set, pods)
}

// syncStatefulSet 对 (StatefulSet, []*v1.Pod) 执行同步。
func (ssc *StatefulSetController) syncStatefulSet(ctx context.Context, set *apps.StatefulSet, pods []*v1.Pod) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Syncing StatefulSet with pods", "statefulSet", klog.KObj(set), "pods", len(pods))
	var status *apps.StatefulSetStatus
	var nextSyncDuration *time.Duration
	var err error
	// 状态计算与下次同步时间的计算使用同一时刻，保证可用性判断一致。
	now := ssc.clock.Now()
	status, err = ssc.control.UpdateStatefulSet(ctx, set, pods, now)
	if err != nil {
		return err
	}
	logger.V(4).Info("Successfully synced StatefulSet", "statefulSet", klog.KObj(set))
	// 安排下次可用性检查作为兜底，防止队列抢占导致检查丢失或过早同步。
	// 以及同步过早（见 https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133）的问题。
	if set.Spec.MinReadySeconds > 0 && status != nil && status.ReadyReplicas != status.AvailableReplicas {
		// 兜底使用 minReadySeconds，确保 availableReplicas 最终一定会被更新。
		nextSyncDuration = ptr.To(time.Duration(set.Spec.MinReadySeconds) * time.Second)
		// 优先采用计算出的最早 Pod 就绪检查时间。
		if nextCheck := controller.FindMinNextPodAvailabilityCheck(pods, set.Spec.MinReadySeconds, now, ssc.clock); nextCheck != nil {
			nextSyncDuration = nextCheck
		}
	}
	if nextSyncDuration != nil {
		ssc.enqueueSSAfter(logger, set, *nextSyncDuration)
	}
	return nil
}
