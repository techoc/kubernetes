/*
Copyright 2015 The Kubernetes Authors.

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

package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	unversionedapps "k8s.io/client-go/kubernetes/typed/apps/v1"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/daemon/metrics"
	"k8s.io/kubernetes/pkg/controller/daemon/util"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// BurstReplicas 限制单轮创建/删除 Pod 的数量，避免批量启动 Pod 时对 APIServer 造成冲击。
	// 取 250 是因为更大的值可能引发 registry 拒绝服务问题。

	BurstReplicas = 250

	// StatusUpdateRetries 限制状态更新失败后的重试次数；超过后由工作队列限速重新入队。

	StatusUpdateRetries = 1

	// BackoffGCInterval 是两次失败 Pod 退避缓存 GC 之间的间隔。

	BackoffGCInterval = 1 * time.Minute
)

// DaemonSet 事件原因常量。

const (
	// SelectingAllReason 表示 DaemonSet 的 selector 为空、选中了所有 Pod。

	SelectingAllReason = "SelectingAll"
	// FailedPlacementReason 表示 DaemonSet 无法将 Pod 调度到指定节点。

	FailedPlacementReason = "FailedPlacement"
	// FailedDaemonPodReason 表示 DaemonSet 的 Pod 进入 Failed 状态。

	FailedDaemonPodReason = "FailedDaemonPod"
	// SucceededDaemonPodReason 表示 DaemonSet 的 Pod 进入 Succeeded 状态。

	SucceededDaemonPodReason = "SucceededDaemonPod"
)

var (
	daemonsetGroupResource = schema.GroupResource{
		Group:    "apps",
		Resource: "daemonsets",
	}
	podGroupResource = schema.GroupResource{
		Group:    "",
		Resource: "pods",
	}
)

// controllerKind 是本控制器操作的 DaemonSet 对应的 GroupVersionKind，用于解析 ControllerRef。

var controllerKind = apps.SchemeGroupVersion.WithKind("DaemonSet")

// DaemonSetsController 负责把系统中的 DaemonSet 对象与实际运行的 Pod 保持同步。

type DaemonSetsController struct {
	kubeClient clientset.Interface

	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	podControl controller.PodControlInterface
	crControl  controller.ControllerRevisionControlInterface

	// 创建/删除这么多副本后暂时挂起同步，等观察到对应的 watch 事件再恢复，
	// 防止在事件尚未到达时重复创建或删除。

	burstReplicas int

	// 以字段形式保存同步函数，便于测试中注入替身。

	syncHandler func(ctx context.Context, dsKey string) error
	// 以字段形式保存入队函数，便于单元测试注入。

	enqueueDaemonSet func(ds *apps.DaemonSet)
	// 记录每个 DaemonSet 预期将观察到的 Pod 创建/删除事件，避免在期望未满足时重复操作。

	expectations controller.ControllerExpectationsInterface
	// 从共享 informer 缓存读取 DaemonSet。

	dsLister appslisters.DaemonSetLister
	// DaemonSet 缓存至少完成一次同步后返回 true；作为字段便于测试注入。

	dsStoreSynced cache.InformerSynced
	// 从共享 informer 缓存读取 ControllerRevision 历史。

	historyLister appslisters.ControllerRevisionLister
	// ControllerRevision 缓存至少完成一次同步后返回 true；作为字段便于测试注入。

	historyStoreSynced cache.InformerSynced
	// 从共享 informer 缓存读取 Pod。

	podLister corelisters.PodLister
	// 支持按节点名或 ControllerRef UID 索引查找 Pod。

	podIndexer cache.Indexer
	// Pod 缓存至少完成一次同步后返回 true；作为字段便于测试注入。

	podStoreSynced cache.InformerSynced
	// 从共享 informer 缓存读取 Node。

	nodeLister corelisters.NodeLister
	// Node 缓存至少完成一次同步后返回 true；作为字段便于测试注入。

	nodeStoreSynced cache.InformerSynced

	// 待同步 DaemonSet 的键，按限速队列处理。

	queue workqueue.TypedRateLimitingInterface[string]

	// 节点更新队列：节点标签/污点变化会影响 DaemonSet 的调度判定，需要单独入队调和。

	nodeUpdateQueue workqueue.TypedRateLimitingInterface[string]

	failedPodsBackoff *flowcontrol.Backoff

	consistencyStore consistencyutil.ConsistencyStore
}

// NewDaemonSetsController 创建 DaemonSetsController，注册四类资源的事件处理器，
// 并根据特性开关决定是否启用写入一致性检查。
func NewDaemonSetsController(
	ctx context.Context,
	daemonSetInformer appsinformers.DaemonSetInformer,
	historyInformer appsinformers.ControllerRevisionInformer,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	failedPodsBackoff *flowcontrol.Backoff,
) (*DaemonSetsController, error) {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	logger := klog.FromContext(ctx)
	var consistencyStore consistencyutil.ConsistencyStore
	var podWriteCallback func(pod *v1.Pod, ds *metav1.OwnerReference)
	// 启用一致性特性时，记录每次 Pod 写入的资源版本，供后续同步判断本地读是否已追上写入。
	if utilfeature.DefaultFeatureGate.Enabled(features.StaleControllerConsistencyDaemonSet) {
		consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			podGroupResource:       podInformer.Informer().GetStore(),
			daemonsetGroupResource: daemonSetInformer.Informer().GetStore(),
		})
		podWriteCallback = func(pod *v1.Pod, ds *metav1.OwnerReference) {
			if ds == nil {
				return
			}
			consistencyStore.WroteAt(
				types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      ds.Name,
				},
				ds.UID,
				podGroupResource,
				pod.ResourceVersion,
			)
		}
	} else {
		consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}

	dsc := &DaemonSetsController{
		kubeClient:       kubeClient,
		eventBroadcaster: eventBroadcaster,
		eventRecorder:    eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "daemonset-controller"}),
		podControl: controller.RealPodControl{
			KubeClient: kubeClient,
			Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "daemonset-controller"}),
			OnWrite:    podWriteCallback,
		},

		crControl: controller.RealControllerRevisionControl{
			KubeClient: kubeClient,
		},
		burstReplicas: BurstReplicas,
		expectations:  controller.NewControllerExpectations(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "daemonset",
			},
		),
		nodeUpdateQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "daemonset-node-updates",
			},
		),
		consistencyStore: consistencyStore,
	}

	// DaemonSet 自身变化直接入队；Pod/历史变化需先找到所属 DaemonSet 再入队。
	daemonSetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dsc.addDaemonset(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dsc.updateDaemonset(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dsc.deleteDaemonset(logger, obj)
		},
	})
	dsc.dsLister = daemonSetInformer.Lister()
	dsc.dsStoreSynced = daemonSetInformer.Informer().HasSynced

	historyInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dsc.addHistory(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dsc.updateHistory(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dsc.deleteHistory(logger, obj)
		},
	})
	dsc.historyLister = historyInformer.Lister()
	dsc.historyStoreSynced = historyInformer.Informer().HasSynced

	// 监听 Pod 的创建与删除：只有观察到上一轮创建/删除的全部效果（期望值被满足）后，
	// 才允许 DaemonSet 继续创建或删除 Pod。

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dsc.addPod(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dsc.updatePod(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dsc.deletePod(logger, obj)
		},
	})
	dsc.podLister = podInformer.Lister()
	dsc.podStoreSynced = podInformer.Informer().HasSynced
	controller.AddPodNodeNameIndexer(podInformer.Informer())
	controller.AddPodControllerIndexer(podInformer.Informer())
	dsc.podIndexer = podInformer.Informer().GetIndexer()

	// Node 变化进入独立队列，避免海量节点更新淹没 DaemonSet 同步队列。
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dsc.addNode(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dsc.updateNode(logger, oldObj, newObj)
		},
	},
	)
	dsc.nodeStoreSynced = nodeInformer.Informer().HasSynced
	dsc.nodeLister = nodeInformer.Lister()

	// 通过字段间接调用，便于测试替换同步逻辑。
	dsc.syncHandler = dsc.syncDaemonSet
	dsc.enqueueDaemonSet = dsc.enqueue

	dsc.failedPodsBackoff = failedPodsBackoff

	metrics.Register()
	return dsc, nil
}

// addDaemonset 处理 DaemonSet 新增事件，将其键加入同步队列。
func (dsc *DaemonSetsController) addDaemonset(logger klog.Logger, obj interface{}) {
	ds := obj.(*apps.DaemonSet)
	logger.V(4).Info("Adding daemon set", "daemonset", klog.KObj(ds))
	dsc.enqueueDaemonSet(ds)
}

// updateDaemonset 处理 DaemonSet 更新事件；若 UID 变化则视作先删后建（重建场景），
// 清理旧对象后再入队新对象。
func (dsc *DaemonSetsController) updateDaemonset(logger klog.Logger, cur, old interface{}) {
	oldDS := old.(*apps.DaemonSet)
	curDS := cur.(*apps.DaemonSet)

	// 仅凭 Update 事件无法区分重建，手动触发旧对象的删除流程以清理期望值。
	// TODO: make a KEP and fix informers to always call the delete event handler on re-create
	if curDS.UID != oldDS.UID {
		key, err := controller.KeyFunc(oldDS)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", oldDS, err))
			return
		}
		dsc.deleteDaemonset(logger, cache.DeletedFinalStateUnknown{
			Key: key,
			Obj: oldDS,
		})
	}

	logger.V(4).Info("Updating daemon set", "daemonset", klog.KObj(oldDS))
	dsc.enqueueDaemonSet(curDS)
}

// deleteDaemonset 处理 DaemonSet 删除事件，兼容墓碑对象；
// 清理一致性记录与期望值，确保同名重建时状态干净。
func (dsc *DaemonSetsController) deleteDaemonset(logger klog.Logger, obj interface{}) {
	ds, ok := obj.(*apps.DaemonSet)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		ds, ok = tombstone.Obj.(*apps.DaemonSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a DaemonSet %#v", obj))
			return
		}
	}
	logger.V(4).Info("Deleting daemon set", "daemonset", klog.KObj(ds))

	key, err := controller.KeyFunc(ds)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ds, err))
		return
	}
	dsc.consistencyStore.Clear(
		types.NamespacedName{
			Name:      ds.Name,
			Namespace: ds.Namespace,
		},
		ds.UID,
	)

	// 同名重建时旧的期望值已无意义，必须清除以免新对象被阻塞。
	// Delete expectations for the DaemonSet so if we create a new one with the same name it starts clean
	dsc.expectations.DeleteExpectations(logger, key)

	dsc.queue.Add(key)
}

// Run 启动事件监听与同步循环：等待缓存同步后启动多组 worker，并定期回收失败退避记录。
func (dsc *DaemonSetsController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	dsc.eventBroadcaster.StartStructuredLogging(3)
	dsc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: dsc.kubeClient.CoreV1().Events("")})
	defer dsc.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting daemon sets controller")

	// 等待队列和 worker 退出；事件广播器也需在退出时关闭。
	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down daemon sets controller")
		dsc.queue.ShutDown()
		dsc.nodeUpdateQueue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前不做调和，否则会基于不完整状态做出错误决策。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, dsc.podStoreSynced, dsc.nodeStoreSynced, dsc.historyStoreSynced, dsc.dsStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, dsc.runWorker, time.Second)
		})
		wg.Go(func() {
			wait.UntilWithContext(ctx, dsc.runNodeUpdateWorker, time.Second)
		})
	}
	// 定期回收退避缓存中不再使用的条目，防止键无限增长。
	wg.Go(func() {
		wait.Until(dsc.failedPodsBackoff.GC, BackoffGCInterval, ctx.Done())
	})
	<-ctx.Done()
}

// runWorker 持续处理同步队列，直到队列关闭（控制器退出）。
func (dsc *DaemonSetsController) runWorker(ctx context.Context) {
	for dsc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出队首键并同步；成功则清除限速记录，失败则限速重试。
// 队列关闭时返回 false 通知 worker 退出。
func (dsc *DaemonSetsController) processNextWorkItem(ctx context.Context) bool {
	dsKey, quit := dsc.queue.Get()
	if quit {
		return false
	}
	defer dsc.queue.Done(dsKey)

	err := dsc.syncHandler(ctx, dsKey)
	if err == nil {
		dsc.queue.Forget(dsKey)
		return true
	}

	// 同步失败必须限速重试，避免错误对象导致忙循环。
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	dsc.queue.AddRateLimited(dsKey)

	return true
}

// enqueue 根据 DaemonSet 的命名空间/名称生成键并入队。
func (dsc *DaemonSetsController) enqueue(ds *apps.DaemonSet) {
	key, err := controller.KeyFunc(ds)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", ds, err))
		return
	}

	// TODO: Handle overlapping controllers better. See comment in ReplicationManager.
	dsc.queue.Add(key)
}

// enqueueDaemonSetAfter 延迟入队，用于等待 MinReadySeconds 或退避窗口结束。
func (dsc *DaemonSetsController) enqueueDaemonSetAfter(obj interface{}, after time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	// TODO: Handle overlapping controllers better. See comment in ReplicationManager.
	dsc.queue.AddAfter(key, after)
}

// getDaemonSetsForPod 返回标签选择器可能匹配该 Pod 的 DaemonSet 列表。
func (dsc *DaemonSetsController) getDaemonSetsForPod(pod *v1.Pod) []*apps.DaemonSet {
	sets, err := dsc.dsLister.GetPodDaemonSets(pod)
	if err != nil {
		return nil
	}
	// 多个 DaemonSet 选中同一 Pod 属于用户配置错误，ControllerRef 会限制实际动作但需告警。
	if len(sets) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		utilruntime.HandleError(fmt.Errorf("user error! more than one daemon is selecting pods with labels: %+v", pod.Labels))
	}
	return sets
}

// getDaemonSetsForHistory 返回标签选择器可能匹配该 ControllerRevision 的 DaemonSet 列表。
func (dsc *DaemonSetsController) getDaemonSetsForHistory(logger klog.Logger, history *apps.ControllerRevision) []*apps.DaemonSet {
	daemonSets, err := dsc.dsLister.GetHistoryDaemonSets(history)
	if err != nil || len(daemonSets) == 0 {
		return nil
	}
	// 多个 DaemonSet 选中同一历史对象属于用户配置错误，记录日志便于排查。
	if len(daemonSets) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		logger.V(4).Info("Found more than one DaemonSet selecting the ControllerRevision. This is potentially a user error",
			"controllerRevision", klog.KObj(history), "labels", history.Labels)
	}
	return daemonSets
}

// addHistory 在 ControllerRevision 创建或控制器重启时发现该对象，
// 找到管理它的 DaemonSet（或可能收养它的孤儿 DaemonSet）并入队。
func (dsc *DaemonSetsController) addHistory(logger klog.Logger, obj interface{}) {
	history := obj.(*apps.ControllerRevision)
	// 重启后对象可能已处于待删除状态，按删除事件处理。
	if history.DeletionTimestamp != nil {
		// On a restart of the controller manager, it's possible for an object to
		// show up in a state that is already pending deletion.
		dsc.deleteHistory(logger, history)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	if controllerRef := metav1.GetControllerOf(history); controllerRef != nil {
		ds := dsc.resolveControllerRef(history.Namespace, controllerRef)
		if ds == nil {
			return
		}
		logger.V(4).Info("Observed a ControllerRevision", "controllerRevision", klog.KObj(history))
		return
	}

	// Otherwise, it's an orphan. Get a list of all matching DaemonSets and sync
	// them to see if anyone wants to adopt it.
	daemonSets := dsc.getDaemonSetsForHistory(logger, history)
	if len(daemonSets) == 0 {
		return
	}
	logger.V(4).Info("Orphan ControllerRevision added", "controllerRevision", klog.KObj(history))
	for _, ds := range daemonSets {
		dsc.enqueueDaemonSet(ds)
	}
}

// updateHistory 处理 ControllerRevision 更新：ResourceVersion 未变说明是周期性 resync，直接忽略；
// ControllerRef 或标签变化时需要同时唤醒新旧所属 DaemonSet，以便收养或清理。
func (dsc *DaemonSetsController) updateHistory(logger klog.Logger, old, cur interface{}) {
	curHistory := cur.(*apps.ControllerRevision)
	oldHistory := old.(*apps.ControllerRevision)
	// 周期性 resync 会产生 ResourceVersion 不变的更新事件，无需处理。
	if curHistory.ResourceVersion == oldHistory.ResourceVersion {
		// Periodic resync will send update events for all known ControllerRevisions.
		return
	}

	curControllerRef := metav1.GetControllerOf(curHistory)
	oldControllerRef := metav1.GetControllerOf(oldHistory)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if ds := dsc.resolveControllerRef(oldHistory.Namespace, oldControllerRef); ds != nil {
			dsc.enqueueDaemonSet(ds)
		}
	}

	// If it has a ControllerRef, that's all that matters.
	if curControllerRef != nil {
		ds := dsc.resolveControllerRef(curHistory.Namespace, curControllerRef)
		if ds == nil {
			return
		}
		logger.V(4).Info("Observed an update to a ControllerRevision", "controllerRevision", klog.KObj(curHistory))
		dsc.enqueueDaemonSet(ds)
		return
	}

	// Otherwise, it's an orphan. If anything changed, sync matching controllers
	// to see if anyone wants to adopt it now.
	labelChanged := !reflect.DeepEqual(curHistory.Labels, oldHistory.Labels)
	if labelChanged || controllerRefChanged {
		daemonSets := dsc.getDaemonSetsForHistory(logger, curHistory)
		if len(daemonSets) == 0 {
			return
		}
		logger.V(4).Info("Orphan ControllerRevision updated", "controllerRevision", klog.KObj(curHistory))
		for _, ds := range daemonSets {
			dsc.enqueueDaemonSet(ds)
		}
	}
}

// deleteHistory 处理 ControllerRevision 删除，兼容墓碑对象；孤儿历史无人关心，
// 有 ControllerRef 时唤醒所属 DaemonSet。
func (dsc *DaemonSetsController) deleteHistory(logger klog.Logger, obj interface{}) {
	history, ok := obj.(*apps.ControllerRevision)

	// When a delete is dropped, the relist will notice a ControllerRevision in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale. If the ControllerRevision
	// changed labels the new DaemonSet will not be woken up till the periodic resync.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("Couldn't get object from tombstone %#v", obj))
			return
		}
		history, ok = tombstone.Obj.(*apps.ControllerRevision)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("Tombstone contained object that is not a ControllerRevision %#v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(history)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}
	ds := dsc.resolveControllerRef(history.Namespace, controllerRef)
	if ds == nil {
		return
	}
	logger.V(4).Info("ControllerRevision deleted", "controllerRevision", klog.KObj(history))
	dsc.enqueueDaemonSet(ds)
}

// addPod 处理 Pod 新增：若 Pod 已被归属则记录创建期望并唤醒 DaemonSet；
// 孤儿 Pod 只唤醒可能收养它的 DaemonSet，不记录期望（没有控制器在等待它）。
func (dsc *DaemonSetsController) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)

	// 重启后看到的新 Pod 可能已处于待删除状态，按删除处理避免误判为创建观测。
	if pod.DeletionTimestamp != nil {

		// on a restart of the controller manager, it's possible a new pod shows up in a state that
		// is already pending deletion. Prevent the pod from being a creation observation.
		dsc.deletePod(logger, pod)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		ds := dsc.resolveControllerRef(pod.Namespace, controllerRef)
		if ds == nil {
			return
		}
		dsKey, err := controller.KeyFunc(ds)
		if err != nil {
			return
		}
		logger.V(4).Info("Pod added", "pod", klog.KObj(pod))
		dsc.expectations.CreationObserved(logger, dsKey)
		dsc.enqueueDaemonSet(ds)
		return
	}

	// Otherwise, it's an orphan. Get a list of all matching DaemonSets and sync
	// them to see if anyone wants to adopt it.
	// 孤儿 Pod 没有控制器在等待，不能记录创建期望，否则会永久阻塞该 DaemonSet。
	// DO NOT observe creation because no controller should be waiting for an
	// orphan.
	dss := dsc.getDaemonSetsForPod(pod)
	if len(dss) == 0 {
		return
	}
	logger.V(4).Info("Orphan Pod added", "pod", klog.KObj(pod))
	for _, ds := range dss {
		dsc.enqueueDaemonSet(ds)
	}
}

// updatePod 处理 Pod 更新：ResourceVersion 未变是周期性 resync；
// ControllerRef 或标签变化时需要唤醒新旧 DaemonSet；
// 进入 Ready 且配置 MinReadySeconds 时延迟再次入队，以重新统计可用性。
func (dsc *DaemonSetsController) updatePod(logger klog.Logger, old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	// 同一 Pod 的不同版本 ResourceVersion 必然不同，相同则说明是 resync。
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// Periodic resync will send update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}

	// 优雅删除先设置删除时间戳，kubelet 随后才真正移除 Pod；提前入队以免等待过久。
	if curPod.DeletionTimestamp != nil {
		// when a pod is deleted gracefully its deletion timestamp is first modified to reflect a grace period,
		// and after such time has passed, the kubelet actually deletes it from the store. We receive an update
		// for modification of the deletion timestamp and expect an ds to create more replicas asap, not wait
		// until the kubelet actually deletes the pod.
		dsc.deletePod(logger, curPod)
		return
	}

	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// The ControllerRef was changed. Sync the old controller, if any.
		if ds := dsc.resolveControllerRef(oldPod.Namespace, oldControllerRef); ds != nil {
			dsc.enqueueDaemonSet(ds)
		}
	}

	// If it has a ControllerRef, that's all that matters.
	if curControllerRef != nil {
		ds := dsc.resolveControllerRef(curPod.Namespace, curControllerRef)
		if ds == nil {
			return
		}
		logger.V(4).Info("Pod updated", "pod", klog.KObj(curPod))
		dsc.enqueueDaemonSet(ds)
		// Pod 刚变 Ready 时可用性需等待 MinReadySeconds，延迟入队以触发状态更新。
		changedToReady := !podutil.IsPodReady(oldPod) && podutil.IsPodReady(curPod)
		// See https://github.com/kubernetes/kubernetes/pull/38076 for more details
		if changedToReady && ds.Spec.MinReadySeconds > 0 {
			// 多等一秒抵消 AddAfter 的毫秒级时钟偏差。
			// Add a second to avoid milliseconds skew in AddAfter.
			// See https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133 for more info.
			dsc.enqueueDaemonSetAfter(ds, (time.Duration(ds.Spec.MinReadySeconds)*time.Second)+time.Second)
		}
		return
	}

	// Otherwise, it's an orphan. If anything changed, sync matching controllers
	// to see if anyone wants to adopt it now.
	dss := dsc.getDaemonSetsForPod(curPod)
	if len(dss) == 0 {
		return
	}
	logger.V(4).Info("Orphan Pod updated", "pod", klog.KObj(curPod))
	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)
	if labelChanged || controllerRefChanged {
		for _, ds := range dss {
			dsc.enqueueDaemonSet(ds)
		}
	}
}

// deletePod 处理 Pod 删除，兼容墓碑对象；记录删除期望并唤醒所属 DaemonSet，
// 使其能立即补建副本。
func (dsc *DaemonSetsController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	// When a delete is dropped, the relist will notice a pod in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value. Note that this value might be stale. If the pod
	// changed labels the new daemonset will not be woken up till the periodic
	// resync.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %#v", obj))
			return
		}
	}

	// 孤儿 Pod 的删除没有控制器关心。
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}
	ds := dsc.resolveControllerRef(pod.Namespace, controllerRef)
	if ds == nil {
		return
	}
	dsKey, err := controller.KeyFunc(ds)
	if err != nil {
		return
	}
	logger.V(4).Info("Pod deleted", "pod", klog.KObj(pod))
	dsc.expectations.DeletionObserved(logger, dsKey)
	dsc.enqueueDaemonSet(ds)
}

// addNode 处理 Node 新增，将节点名加入节点更新队列，
// 以便重新评估每个 DaemonSet 是否应在该节点运行 Pod。
func (dsc *DaemonSetsController) addNode(logger klog.Logger, obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("couldn't get node from object %#v", obj))
		return
	}

	logger.V(4).Info("Queuing node addition", "node", klog.KObj(node))
	dsc.nodeUpdateQueue.Add(node.Name)
}

// shouldIgnoreNodeUpdate 判断节点更新是否只涉及与 DaemonSet 调度无关的属性。
// 目前仅比较标签和污点；若其他调用方开始依赖节点其他属性，需同步扩展此函数。
func shouldIgnoreNodeUpdate(oldNode, curNode v1.Node) bool {
	return apiequality.Semantic.DeepEqual(oldNode.Labels, curNode.Labels) &&
		apiequality.Semantic.DeepEqual(oldNode.Spec.Taints, curNode.Spec.Taints)
}

// updateNode 处理 Node 更新：仅当标签或污点变化时才入队，
// 因为只有这些属性会改变 DaemonSet 的调度判定。
func (dsc *DaemonSetsController) updateNode(logger klog.Logger, old, cur interface{}) {
	oldNode := old.(*v1.Node)
	curNode := cur.(*v1.Node)

	if shouldIgnoreNodeUpdate(*oldNode, *curNode) {
		return
	}

	logger.V(4).Info("Queuing node update", "node", klog.KObj(curNode))
	dsc.nodeUpdateQueue.Add(curNode.Name)
}

// getDaemonPods 返回归属于给定 DaemonSet 的 Pod，并通过收养/解除归属调和 ControllerRef。
// 返回的 Pod 是指向缓存对象的指针，修改前必须深拷贝。
func (dsc *DaemonSetsController) getDaemonPods(ctx context.Context, ds *apps.DaemonSet) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return nil, err
	}
	// List all pods indexed to DS UID and Orphan pods
	pods, err := controller.FilterPodsByOwner(dsc.podIndexer, &ds.ObjectMeta, "DaemonSet", true)
	if err != nil {
		return nil, err
	}
	// 收养前用非缓存的 quorum 读复查对象是否已被删除，避免收养一个正在消失的 DaemonSet（见 #42639）。
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing Pods (see #42639).
	dsNotDeleted := controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace).Get(ctx, ds.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != ds.UID {
			return nil, fmt.Errorf("original DaemonSet %v/%v is gone: got uid %v, wanted %v", ds.Namespace, ds.Name, fresh.UID, ds.UID)
		}
		return fresh, nil
	})

	// Use ControllerRefManager to adopt/orphan as needed.
	cm := controller.NewPodControllerRefManager(dsc.podControl, ds, selector, controllerKind, dsNotDeleted)
	return cm.ClaimPods(ctx, pods)
}

// getNodesToDaemonPods 返回节点到该 DaemonSet 的 Pod 的映射，并调和 ControllerRef。
// 返回的 Pod 是指向缓存对象的指针，修改前必须深拷贝。
func (dsc *DaemonSetsController) getNodesToDaemonPods(ctx context.Context, ds *apps.DaemonSet, includeDeletedTerminal bool) (map[string][]*v1.Pod, error) {
	claimedPods, err := dsc.getDaemonPods(ctx, ds)
	if err != nil {
		return nil, err
	}
	// Group Pods by Node name.
	nodeToDaemonPods := make(map[string][]*v1.Pod)
	logger := klog.FromContext(ctx)
	for _, pod := range claimedPods {
		// 终态且待删除的 Pod 已由 kubelet/PodGC 处理，控制器无需再介入。
		if !includeDeletedTerminal && podutil.IsPodTerminal(pod) && pod.DeletionTimestamp != nil {
			// This Pod has a finalizer or is already scheduled for deletion from the
			// store by the kubelet or the Pod GC. The DS controller doesn't have
			// anything else to do with it.
			continue
		}
		nodeName, err := util.GetTargetNodeName(pod)
		if err != nil {
			logger.V(4).Info("Failed to get target node name of Pod in DaemonSet",
				"pod", klog.KObj(pod), "daemonset", klog.KObj(ds))
			continue
		}

		nodeToDaemonPods[nodeName] = append(nodeToDaemonPods[nodeName], pod)
	}

	return nodeToDaemonPods, nil
}

// resolveControllerRef 解析 ControllerRef 指向的 DaemonSet；Kind 不匹配、
// 找不到对象或 UID 不一致时返回 nil。
func (dsc *DaemonSetsController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *apps.DaemonSet {
	// 缓存索引只支持按名称查找，因此先按名称取再校验 UID。
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	ds, err := dsc.dsLister.DaemonSets(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if ds.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return ds
}

// podsShouldBeOnNode 判定指定节点上应创建/删除哪些 DaemonSet Pod：
// 返回需要在该节点创建 Pod 的节点列表与需要删除的 Pod 名称列表。
func (dsc *DaemonSetsController) podsShouldBeOnNode(
	logger klog.Logger,
	node *v1.Node,
	nodeToDaemonPods map[string][]*v1.Pod,
	ds *apps.DaemonSet,
	hash string,
	tolerations []v1.Toleration,
	requiredNodeAffinity nodeaffinity.RequiredNodeAffinity,
) (nodesNeedingDaemonPods, podsToDelete []string) {

	shouldRun, shouldContinueRunning := nodeShouldRunDaemonPod(logger, node, ds, tolerations, requiredNodeAffinity)
	daemonPods, exists := nodeToDaemonPods[node.Name]

	switch {
	case shouldRun && !exists:
		// If daemon pod is supposed to be running on node, but isn't, create daemon pod.
		nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, node.Name)
		// 节点应继续运行 DaemonSet Pod 时，清理失败/成功副本与多余副本。
	case shouldContinueRunning:
		// If a daemon pod failed, delete it
		// If there's non-daemon pods left on this node, we will create it in the next sync loop
		var daemonPodsRunning []*v1.Pod
		for _, pod := range daemonPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			// 这是 DaemonSet 与 kubelet 拒绝 Pod 反复拉锯的关键位置，必须退避避免热循环。
			if pod.Status.Phase == v1.PodFailed {
				// This is a critical place where DS is often fighting with kubelet that rejects pods.
				// We need to avoid hot looping and backoff.
				backoffKey := failedPodsBackoffKey(ds, node.Name)

				now := dsc.failedPodsBackoff.Clock.Now()
				inBackoff := dsc.failedPodsBackoff.IsInBackOffSinceUpdate(backoffKey, now)
				if inBackoff {
					delay := dsc.failedPodsBackoff.Get(backoffKey)
					logger.V(4).Info("Deleting failed pod on node has been limited by backoff",
						"pod", klog.KObj(pod), "node", klog.KObj(node), "currentDelay", delay)
					dsc.enqueueDaemonSetAfter(ds, delay)
					continue
				}

				dsc.failedPodsBackoff.Next(backoffKey, now)

				msg := fmt.Sprintf("Found failed daemon pod %s/%s on node %s, will try to kill it", pod.Namespace, pod.Name, node.Name)
				logger.V(2).Info("Found failed daemon pod on node, will try to kill it", "pod", klog.KObj(pod), "node", klog.KObj(node))
				// Emit an event so that it's discoverable to users.
				dsc.eventRecorder.Eventf(ds, v1.EventTypeWarning, FailedDaemonPodReason, "%s", msg)
				podsToDelete = append(podsToDelete, pod.Name)
			} else if pod.Status.Phase == v1.PodSucceeded {
				msg := fmt.Sprintf("Found succeeded daemon pod %s/%s on node %s, will try to delete it", pod.Namespace, pod.Name, node.Name)
				logger.V(2).Info("Found succeeded daemon pod on node, will try to delete it", "pod", klog.KObj(pod), "node", klog.KObj(node))
				// Emit an event so that it's discoverable to users.
				dsc.eventRecorder.Eventf(ds, v1.EventTypeNormal, SucceededDaemonPodReason, "%s", msg)
				podsToDelete = append(podsToDelete, pod.Name)
			} else {
				daemonPodsRunning = append(daemonPodsRunning, pod)
			}
		}

		// 未启用 surge 时每个节点只保留最早创建的一个运行副本。
		// When surge is not enabled, if there is more than 1 running pod on a node delete all but the oldest
		if !util.AllowsSurge(ds) {
			if len(daemonPodsRunning) <= 1 {
				// There are no excess pods to be pruned, and no pods to create
				break
			}

			sort.Sort(podByCreationTimestampAndPhase(daemonPodsRunning))
			for i := 1; i < len(daemonPodsRunning); i++ {
				podsToDelete = append(podsToDelete, daemonPodsRunning[i].Name)
			}
			break
		}

		if len(daemonPodsRunning) <= 1 {
			// // There are no excess pods to be pruned
			if len(daemonPodsRunning) == 0 && shouldRun {
				// We are surging so we need to have at least one non-deleted pod on the node
				nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, node.Name)
			}
			break
		}

		// 启用 surge 时仅在新旧副本处于滚动替换的中间状态才短暂允许两个副本。
		// When surge is enabled, we allow 2 pods if and only if the oldest pod matching the current hash state
		// is not ready AND the oldest pod that doesn't match the current hash state is ready. All other pods are
		// deleted. If neither pod is ready, only the one matching the current hash revision is kept.
		var oldestNewPod, oldestOldPod *v1.Pod
		sort.Sort(podByCreationTimestampAndPhase(daemonPodsRunning))
		for _, pod := range daemonPodsRunning {
			if pod.Labels[apps.ControllerRevisionHashLabelKey] == hash {
				if oldestNewPod == nil {
					oldestNewPod = pod
					continue
				}
			} else {
				if oldestOldPod == nil {
					oldestOldPod = pod
					continue
				}
			}
			podsToDelete = append(podsToDelete, pod.Name)
		}
		if oldestNewPod != nil && oldestOldPod != nil {
			switch {
			case !podutil.IsPodReady(oldestOldPod):
				logger.V(5).Info("Pod from daemonset is no longer ready and will be replaced with newer pod", "oldPod", klog.KObj(oldestOldPod), "daemonset", klog.KObj(ds), "newPod", klog.KObj(oldestNewPod))
				podsToDelete = append(podsToDelete, oldestOldPod.Name)
			case podutil.IsPodAvailable(oldestNewPod, ds.Spec.MinReadySeconds, metav1.Time{Time: dsc.failedPodsBackoff.Clock.Now()}):
				logger.V(5).Info("Pod from daemonset is now ready and will replace older pod", "newPod", klog.KObj(oldestNewPod), "daemonset", klog.KObj(ds), "oldPod", klog.KObj(oldestOldPod))
				podsToDelete = append(podsToDelete, oldestOldPod.Name)
			}
		}

		// 节点不应再运行 DaemonSet Pod，删除其上所有副本。
	case !shouldContinueRunning && exists:
		// If daemon pod isn't supposed to run on node, but it is, delete all daemon pods on node.
		for _, pod := range daemonPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			podsToDelete = append(podsToDelete, pod.Name)
		}
	}

	return nodesNeedingDaemonPods, podsToDelete
}

// updateDaemonSet 先调和节点上的 Pod，再在期望满足后执行滚动更新，最后清理旧历史版本。
func (dsc *DaemonSetsController) updateDaemonSet(ctx context.Context, ds *apps.DaemonSet, nodeList []*v1.Node, hash, key string, old []*apps.ControllerRevision) error {
	err := dsc.manage(ctx, ds, nodeList, hash)
	if err != nil {
		return err
	}

	// 上一轮的创建/删除尚未观测完成时不做滚动更新，避免并发操作放大。
	// Process rolling updates if we're ready.
	if dsc.expectations.SatisfiedExpectations(klog.FromContext(ctx), key) {
		switch ds.Spec.UpdateStrategy.Type {
		case apps.OnDeleteDaemonSetStrategyType:
		case apps.RollingUpdateDaemonSetStrategyType:
			err = dsc.rollingUpdate(ctx, ds, nodeList, hash)
		}
		if err != nil {
			return err
		}
	}

	err = dsc.cleanupHistory(ctx, ds, old)
	if err != nil {
		return fmt.Errorf("failed to clean up revisions of DaemonSet: %w", err)
	}

	return nil
}

// manage 管理 DaemonSet Pod 在节点上的调度与运行：找出缺 Pod 的节点和多余的 Pod，
// 调用 syncNodes 执行创建与删除。
func (dsc *DaemonSetsController) manage(ctx context.Context, ds *apps.DaemonSet, nodeList []*v1.Node, hash string) error {
	// Find out the pods which are created for the nodes by DaemonSet.
	nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ctx, ds, false)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	// For each node, if the node is running the daemon pod but isn't supposed to, kill the daemon
	// pod. If the node is supposed to run the daemon pod, but isn't, create the daemon pod on the node.
	logger := klog.FromContext(ctx)
	var nodesNeedingDaemonPods, podsToDelete []string
	// 容差与必需节点亲和性对所有节点相同，且解析开销较大，循环外只计算一次。
	// The daemon pod tolerations and the parsed required node affinity are
	// the same for every node; build them once instead of once per node.
	tolerations := daemonPodTolerations(ds)
	requiredNodeAffinity := nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity)
	for _, node := range nodeList {
		nodesNeedingDaemonPodsOnNode, podsToDeleteOnNode := dsc.podsShouldBeOnNode(
			logger, node, nodeToDaemonPods, ds, hash, tolerations, requiredNodeAffinity)

		nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, nodesNeedingDaemonPodsOnNode...)
		podsToDelete = append(podsToDelete, podsToDeleteOnNode...)
	}

	// 指向已不存在节点的未调度 Pod 永远无法被调度，PodGC 也清理不了，需由本控制器删除。
	// Remove unscheduled pods assigned to not existing nodes when daemonset pods are scheduled by scheduler.
	// If node doesn't exist then pods are never scheduled and can't be deleted by PodGCController.
	podsToDelete = append(podsToDelete, getUnscheduledPodsWithoutNode(nodeList, nodeToDaemonPods)...)

	// Label new pods using the hash label value of the current history when creating them
	if err = dsc.syncNodes(ctx, ds, podsToDelete, nodesNeedingDaemonPods, hash); err != nil {
		return err
	}

	return nil
}

// syncNodes 删除指定 Pod 并在指定节点创建 DaemonSet Pod，返回聚合错误。
func (dsc *DaemonSetsController) syncNodes(ctx context.Context, ds *apps.DaemonSet, podsToDelete, nodesNeedingDaemonPods []string, hash string) error {
	// 必须在发起创建/删除前设置期望值，否则事件可能先于期望记录到达而被丢弃。
	// We need to set expectations before creating/deleting pods to avoid race conditions.
	logger := klog.FromContext(ctx)
	dsKey, err := controller.KeyFunc(ds)
	if err != nil {
		return fmt.Errorf("couldn't get key for object %#v: %v", ds, err)
	}

	createDiff := len(nodesNeedingDaemonPods)
	deleteDiff := len(podsToDelete)

	if createDiff > dsc.burstReplicas {
		createDiff = dsc.burstReplicas
	}
	if deleteDiff > dsc.burstReplicas {
		deleteDiff = dsc.burstReplicas
	}

	dsc.expectations.SetExpectations(logger, dsKey, createDiff, deleteDiff)

	// 错误通道容量覆盖全部操作，避免 goroutine 写入时阻塞。
	// error channel to communicate back failures.  make the buffer big enough to avoid any blocking
	errCh := make(chan error, createDiff+deleteDiff)

	logger.V(4).Info("Nodes needing daemon pods for daemon set, creating", "daemonset", klog.KObj(ds), "needCount", nodesNeedingDaemonPods, "createCount", createDiff)
	createWait := sync.WaitGroup{}
	// If the returned error is not nil we have a parse error.
	// The controller handles this via the hash.
	generation, err := util.GetTemplateGeneration(ds)
	if err != nil {
		generation = nil
	}
	template := util.CreatePodTemplate(ds.Spec.Template, generation, hash)
	// 采用慢启动批量创建：首批失败即停止扩大批次，避免配额不足等场景刷屏 APIServer 与事件系统。
	// Batch the pod creates. Batch sizes start at SlowStartInitialBatchSize
	// and double with each successful iteration in a kind of "slow start".
	// This handles attempts to start large numbers of pods that would
	// likely all fail with the same error. For example a project with a
	// low quota that attempts to create a large number of pods will be
	// prevented from spamming the API service with the pod create requests
	// after one of its pods fails.  Conveniently, this also prevents the
	// event spam that those failures would generate.
	batchSize := min(createDiff, controller.SlowStartInitialBatchSize)
	for pos := 0; createDiff > pos; batchSize, pos = min(2*batchSize, createDiff-(pos+batchSize)), pos+batchSize {
		errorCount := len(errCh)
		createWait.Add(batchSize)
		for i := pos; i < pos+batchSize; i++ {
			// 并发创建副本，每个 goroutine 用独立深拷贝的模板。
			go func(ix int) {
				defer createWait.Done()

				podTemplate := template.DeepCopy()
				// 改写 Pod 的节点亲和性以确保默认调度器绑定到目标节点；目标节点不会存在冲突的亲和性。
				// The pod's NodeAffinity will be updated to make sure the Pod is bound
				// to the target node by default scheduler. It is safe to do so because there
				// should be no conflicting node affinity with the target node.
				podTemplate.Spec.Affinity = util.ReplaceDaemonSetPodNodeNameNodeAffinity(
					podTemplate.Spec.Affinity, nodesNeedingDaemonPods[ix])

				err := dsc.podControl.CreatePods(ctx, ds.Namespace, podTemplate,
					ds, metav1.NewControllerRef(ds, controllerKind))

				if err != nil {
					if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
						// If the namespace is being torn down, we can safely ignore
						// this error since all subsequent creations will fail.
						return
					}
				}
				if err != nil {
					logger.V(2).Info("Failed creation, decrementing expectations for daemon set", "daemonset", klog.KObj(ds))
					dsc.expectations.CreationObserved(logger, dsKey)
					errCh <- err
					utilruntime.HandleError(err)
				}
			}(i)
		}
		createWait.Wait()
		// any skipped pods that we never attempted to start shouldn't be expected.
		skippedPods := createDiff - (batchSize + pos)
		if errorCount < len(errCh) && skippedPods > 0 {
			logger.V(2).Info("Slow-start failure. Skipping creation pods, decrementing expectations for daemon set", "skippedPods", skippedPods, "daemonset", klog.KObj(ds))
			dsc.expectations.LowerExpectations(logger, dsKey, skippedPods, 0)
			// The skipped pods will be retried later. The next controller resync will
			// retry the slow start process.
			break
		}
	}

	logger.V(4).Info("Pods to delete for daemon set, deleting", "daemonset", klog.KObj(ds), "toDeleteCount", podsToDelete, "deleteCount", deleteDiff)
	deleteWait := sync.WaitGroup{}
	deleteWait.Add(deleteDiff)
	for i := 0; i < deleteDiff; i++ {
		go func(ix int) {
			defer deleteWait.Done()
			// 删除失败也要递减期望值，因为永远等不到对应的删除事件。
			if err := dsc.podControl.DeletePod(ctx, ds.Namespace, podsToDelete[ix], ds); err != nil {
				// We are cleaning up an expectation that this delete will be observed,
				// since any failure to delete the pod means that we will never observe
				// the delete.
				dsc.expectations.DeletionObserved(logger, dsKey)
				if !apierrors.IsNotFound(err) {
					logger.V(2).Info("Failed deletion, decremented expectations for daemon set", "daemonset", klog.KObj(ds))
					errCh <- err
					utilruntime.HandleError(err)
				}
			}
		}(i)
	}
	deleteWait.Wait()

	// collect errors if any for proper reporting/retry logic in the controller
	errors := []error{}
	close(errCh)
	for err := range errCh {
		errors = append(errors, err)
	}
	return utilerrors.NewAggregate(errors)
}

// storeDaemonSetStatus 在状态无变化时跳过写入，否则带重试更新 DaemonSet 状态；
// 重试前重新 GET 获取最新 ResourceVersion，避免冲突。
func storeDaemonSetStatus(
	ctx context.Context,
	dsClient unversionedapps.DaemonSetInterface,
	ds *apps.DaemonSet, desiredNumberScheduled,
	currentNumberScheduled,
	numberMisscheduled,
	numberReady,
	updatedNumberScheduled,
	numberAvailable,
	numberUnavailable int,
	updateObservedGen bool) (*apps.DaemonSet, error) {
	// 所有计数字段都未变化且 ObservedGeneration 已跟上时无需写 API。
	if int(ds.Status.DesiredNumberScheduled) == desiredNumberScheduled &&
		int(ds.Status.CurrentNumberScheduled) == currentNumberScheduled &&
		int(ds.Status.NumberMisscheduled) == numberMisscheduled &&
		int(ds.Status.NumberReady) == numberReady &&
		int(ds.Status.UpdatedNumberScheduled) == updatedNumberScheduled &&
		int(ds.Status.NumberAvailable) == numberAvailable &&
		int(ds.Status.NumberUnavailable) == numberUnavailable &&
		ds.Status.ObservedGeneration >= ds.Generation {
		return nil, nil
	}

	toUpdate := ds.DeepCopy()

	var updateErr, getErr error
	for i := 0; ; i++ {
		if updateObservedGen {
			toUpdate.Status.ObservedGeneration = ds.Generation
		}
		toUpdate.Status.DesiredNumberScheduled = int32(desiredNumberScheduled)
		toUpdate.Status.CurrentNumberScheduled = int32(currentNumberScheduled)
		toUpdate.Status.NumberMisscheduled = int32(numberMisscheduled)
		toUpdate.Status.NumberReady = int32(numberReady)
		toUpdate.Status.UpdatedNumberScheduled = int32(updatedNumberScheduled)
		toUpdate.Status.NumberAvailable = int32(numberAvailable)
		toUpdate.Status.NumberUnavailable = int32(numberUnavailable)

		var result *apps.DaemonSet
		if result, updateErr = dsClient.UpdateStatus(ctx, toUpdate, metav1.UpdateOptions{}); updateErr == nil {
			return result, nil
		}

		// 超过重试上限后停止，由工作队列限速重新入队。
		// Stop retrying if we exceed statusUpdateRetries - the DaemonSet will be requeued with a rate limit.
		if i >= StatusUpdateRetries {
			break
		}
		// Update the set with the latest resource version for the next poll
		if toUpdate, getErr = dsClient.Get(ctx, ds.Name, metav1.GetOptions{}); getErr != nil {
			// If the GET fails we can't trust status.Replicas anymore. This error
			// is bound to be more interesting than the update failure.
			return nil, getErr
		}
	}
	return nil, updateErr
}

// updateDaemonSetStatus 遍历节点统计所需/已调度/就绪/可用/已更新的副本数并写回状态。
func (dsc *DaemonSetsController) updateDaemonSetStatus(ctx context.Context, ds *apps.DaemonSet, nodeList []*v1.Node, hash string, updateObservedGen bool) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating daemon set status")
	nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ctx, ds, false)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	var desiredNumberScheduled, currentNumberScheduled, numberMisscheduled, numberReady, updatedNumberScheduled, numberAvailable int
	now := dsc.failedPodsBackoff.Clock.Now()
	tolerations := daemonPodTolerations(ds)
	requiredNodeAffinity := nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity)
	for _, node := range nodeList {
		shouldRun, _ := nodeShouldRunDaemonPod(logger, node, ds, tolerations, requiredNodeAffinity)
		scheduled := len(nodeToDaemonPods[node.Name]) > 0

		if shouldRun {
			desiredNumberScheduled++
			if !scheduled {
				continue
			}

			currentNumberScheduled++
			// 按创建时间排序，只以最早的副本作为该节点的状态代表。
			// Sort the daemon pods by creation time, so that the oldest is first.
			daemonPods, _ := nodeToDaemonPods[node.Name]
			sort.Sort(podByCreationTimestampAndPhase(daemonPods))
			pod := daemonPods[0]
			if podutil.IsPodReady(pod) {
				numberReady++
				if podutil.IsPodAvailable(pod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
					numberAvailable++
				}
			}
			// If the returned error is not nil we have a parse error.
			// The controller handles this via the hash.
			generation, err := util.GetTemplateGeneration(ds)
			if err != nil {
				generation = nil
			}
			if util.IsPodUpdated(pod, hash, generation) {
				updatedNumberScheduled++
			}
		} else {
			if scheduled {
				numberMisscheduled++
			}
		}
	}
	numberUnavailable := desiredNumberScheduled - numberAvailable

	updatedDS, err := storeDaemonSetStatus(ctx, dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace), ds, desiredNumberScheduled, currentNumberScheduled, numberMisscheduled, numberReady, updatedNumberScheduled, numberAvailable, numberUnavailable, updateObservedGen)
	if err != nil {
		return fmt.Errorf("error storing status for daemon set %#v: %w", ds, err)
	}
	if updatedDS != nil {
		dsc.consistencyStore.WroteAt(
			types.NamespacedName{Name: ds.Name, Namespace: ds.Namespace},
			updatedDS.UID,
			daemonsetGroupResource,
			updatedDS.ResourceVersion,
		)
	}

	// MinReadySeconds 到期后再次入队，作为时钟偏差的最后一道防线。
	// Resync the DaemonSet after MinReadySeconds as a last line of defense to guard against clock-skew.
	if ds.Spec.MinReadySeconds > 0 && numberReady != numberAvailable {
		dsc.enqueueDaemonSetAfter(ds, time.Duration(ds.Spec.MinReadySeconds)*time.Second)
	}
	return nil
}

// syncDaemonSet 是单个 DaemonSet 的主同步流程：校验一致性、构建历史、
// 调和 Pod、更新状态；删除中的对象只跳过状态更新。
func (dsc *DaemonSetsController) syncDaemonSet(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := dsc.failedPodsBackoff.Clock.Now()

	defer func() {
		logger.V(4).Info("Finished syncing daemon set", "daemonset", key, "time", dsc.failedPodsBackoff.Clock.Now().Sub(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	dsNamespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	// 若 informer 尚未观察到自己的写入，则本地读与已生效状态不一致，先返回等待。
	// If our writes have not yet been observed by our informers, we should not
	// continue since our reads will not be in sync with our previously enacted
	// state.
	if err := dsc.consistencyStore.EnsureReady(dsNamespacedName); err != nil {
		var consistencyErr *consistencyutil.ConsistencyError
		if errors.As(err, &consistencyErr) {
			metrics.DaemonsetRequeueSkips.WithLabelValues(
				consistencyErr.GroupResource.Group,
				consistencyErr.GroupResource.Resource,
			).Inc()
		}
		return err
	}

	ds, err := dsc.dsLister.DaemonSets(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		logger.V(3).Info("Daemon set has been deleted", "daemonset", key)
		dsc.expectations.DeleteExpectations(logger, key)
		dsc.consistencyStore.Clear(dsNamespacedName, "")
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to retrieve ds %v from store: %v", key, err)
	}

	nodeList, err := dsc.nodeLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("couldn't get list of nodes when syncing daemon set %#v: %v", ds, err)
	}

	// 空 selector 会选中所有 Pod，风险过大，记录事件并拒绝处理。
	everything := metav1.LabelSelector{}
	if reflect.DeepEqual(ds.Spec.Selector, &everything) {
		dsc.eventRecorder.Eventf(ds, v1.EventTypeWarning, SelectingAllReason, "This daemon set is selecting all pods. A non-empty selector is required.")
		return nil
	}

	// 上一轮创建/删除的期望未满足时不调和，避免重复操作。
	// Don't process a daemon set until all its creations and deletions have been processed.
	// For example if daemon set foo asked for 3 new daemon pods in the previous call to manage,
	// then we do not want to call manage on foo until the daemon pods have been created.
	dsKey, err := controller.KeyFunc(ds)
	if err != nil {
		return fmt.Errorf("couldn't get key for object %#v: %v", ds, err)
	}

	// 删除中的 DaemonSet 其历史与 Pod 的垃圾回收顺序无法保证，状态不可可靠计算，直接返回。
	// If the DaemonSet is being deleted (either by foreground deletion or
	// orphan deletion), we cannot be sure if the DaemonSet history objects
	// it owned still exist -- those history objects can either be deleted
	// or orphaned. Garbage collector doesn't guarantee that it will delete
	// DaemonSet pods before deleting DaemonSet history objects, because
	// DaemonSet history doesn't own DaemonSet pods. We cannot reliably
	// calculate the status of a DaemonSet being deleted. Therefore, return
	// here without updating status for the DaemonSet being deleted.
	if ds.DeletionTimestamp != nil {
		return nil
	}

	// Construct histories of the DaemonSet, and get the hash of current history
	cur, old, err := dsc.constructHistory(ctx, ds)
	if err != nil {
		return fmt.Errorf("failed to construct revisions of DaemonSet: %v", err)
	}
	hash := cur.Labels[apps.DefaultDaemonSetUniqueLabelKey]

	// 期望未满足时只更新状态，且不能推进 ObservedGeneration（该版本尚未被真正处理）。
	if !dsc.expectations.SatisfiedExpectations(logger, dsKey) {
		// Only update status. Don't raise observedGeneration since controller didn't process object of that generation.
		return dsc.updateDaemonSetStatus(ctx, ds, nodeList, hash, false)
	}

	err = dsc.updateDaemonSet(ctx, ds, nodeList, hash, dsKey, old)
	statusErr := dsc.updateDaemonSetStatus(ctx, ds, nodeList, hash, true)
	switch {
	// 调和错误与状态错误同时发生时优先返回调和错误。
	case err != nil && statusErr != nil:
		// If there was an error, and we failed to update status,
		// log it and return the original error.
		logger.Error(statusErr, "Failed to update status", "daemonSet", klog.KObj(ds))
		return err
	case err != nil:
		return err
	case statusErr != nil:
		return statusErr
	}

	return nil
}

// NodeShouldRunDaemonPod 检查 (Node, DaemonSet) 组合的前置条件，返回两个判定：
//   - shouldRun：节点上没有 Pod 时，是否应创建一个；
//   - shouldContinueRunning：节点上已有 Pod 时，是否应继续保留。
func NodeShouldRunDaemonPod(logger klog.Logger, node *v1.Node, ds *apps.DaemonSet) (bool, bool) {
	return nodeShouldRunDaemonPod(logger, node, ds, daemonPodTolerations(ds),
		nodeaffinity.NewRequiredNodeAffinity(ds.Spec.Template.Spec.NodeSelector, ds.Spec.Template.Spec.Affinity))
}

// daemonPodTolerations 返回 DaemonSet Pod 的容差：模板容差加上守护 Pod 的默认容差。
// 通过修改副本而非原模板，避免污染 DaemonSet 对象。
func daemonPodTolerations(ds *apps.DaemonSet) []v1.Toleration {
	spec := ds.Spec.Template.Spec
	util.AddOrUpdateDaemonPodTolerations(&spec)
	return spec.Tolerations
}

// nodeShouldRunDaemonPod 是面向多节点循环的 NodeShouldRunDaemonPod 内部版本：
// 容差和必需节点亲和性只取决于 DaemonSet 本身，由调用方预计算后传入，
// 避免在集群每个节点上重复解析节点亲和性这一主要开销。
func nodeShouldRunDaemonPod(logger klog.Logger, node *v1.Node, ds *apps.DaemonSet, tolerations []v1.Toleration, requiredNodeAffinity nodeaffinity.RequiredNodeAffinity) (bool, bool) {
	// If the daemon set specifies a node name, check that it matches with node.Name.
	// Pod 模板指定了节点名时，只有该节点可以运行副本。
	if !(ds.Spec.Template.Spec.NodeName == "" || ds.Spec.Template.Spec.NodeName == node.Name) {
		return false, false
	}

	taints := node.Spec.Taints
	fitsNodeAffinity, fitsTaints := predicates(logger, node, taints, tolerations, requiredNodeAffinity)
	if !fitsNodeAffinity {
		return false, false
	}

	// 已有副本若能容忍 NoExecute 污点则继续运行，否则既不创建也不保留。
	if !fitsTaints {
		// Scheduled daemon pods should continue running if they tolerate NoExecute taint.
		_, hasUntoleratedTaint := v1helper.FindMatchingUntoleratedTaint(logger, taints, tolerations, func(t *v1.Taint) bool {
			return t.Effect == v1.TaintEffectNoExecute
		}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
		return false, !hasUntoleratedTaint
	}

	return true, true
}

// predicates 判断 DaemonSet Pod 是否满足节点的亲和性与污点约束；
// tolerations 与 requiredNodeAffinity 必须由 DaemonSet Pod 模板推导（见 NodeShouldRunDaemonPod）。
func predicates(logger klog.Logger, node *v1.Node, taints []v1.Taint, tolerations []v1.Toleration, requiredNodeAffinity nodeaffinity.RequiredNodeAffinity) (fitsNodeAffinity, fitsTaints bool) {
	// 为向后兼容忽略亲和性解析错误，仅按不匹配处理。
	// Ignore parsing errors for backwards compatibility.
	fitsNodeAffinity, _ = requiredNodeAffinity.Match(node)
	_, hasUntoleratedTaint := v1helper.FindMatchingUntoleratedTaint(logger, taints, tolerations, func(t *v1.Taint) bool {
		return t.Effect == v1.TaintEffectNoExecute || t.Effect == v1.TaintEffectNoSchedule
	}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
	fitsTaints = !hasUntoleratedTaint
	return
}

// NewPod 按 DaemonSet 的 Pod 模板在指定节点上构造一个新 Pod，并补充默认容差。
func NewPod(ds *apps.DaemonSet, nodeName string) *v1.Pod {
	newPod := &v1.Pod{Spec: ds.Spec.Template.Spec, ObjectMeta: ds.Spec.Template.ObjectMeta}
	newPod.Namespace = ds.Namespace
	newPod.Spec.NodeName = nodeName

	// 守护 Pod 需要默认容差才能容忍常见系统污点。
	// Added default tolerations for DaemonSet pods.
	util.AddOrUpdateDaemonPodTolerations(&newPod.Spec)

	return newPod
}

type podByCreationTimestampAndPhase []*v1.Pod

// Len 返回待排序 Pod 的数量。
func (o podByCreationTimestampAndPhase) Len() int { return len(o) }

// Swap 交换两个 Pod 的位置。
func (o podByCreationTimestampAndPhase) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 定义排序优先级：已调度 Pod 优先，其次按创建时间、名称排序，
// 使最早的副本稳定地排在前面。
func (o podByCreationTimestampAndPhase) Less(i, j int) bool {
	// 已调度的 Pod 视为更稳定，排在未调度 Pod 之前。
	// Scheduled Pod first
	if len(o[i].Spec.NodeName) != 0 && len(o[j].Spec.NodeName) == 0 {
		return true
	}

	if len(o[i].Spec.NodeName) == 0 && len(o[j].Spec.NodeName) != 0 {
		return false
	}

	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// failedPodsBackoffKey 生成退避键，按 DaemonSet、观测版本与节点维度隔离失败计数。
func failedPodsBackoffKey(ds *apps.DaemonSet, nodeName string) string {
	return fmt.Sprintf("%s/%d/%s", ds.UID, ds.Status.ObservedGeneration, nodeName)
}

// getUnscheduledPodsWithoutNode 返回绑定到已不存在节点的未调度 Pod 名称；
// 这类 Pod 无法被 PodGC 清理，必须由 DaemonSet 控制器删除。
func getUnscheduledPodsWithoutNode(runningNodesList []*v1.Node, nodeToDaemonPods map[string][]*v1.Pod) []string {
	var results []string
	isNodeRunning := make(map[string]bool, len(runningNodesList))
	for _, node := range runningNodesList {
		isNodeRunning[node.Name] = true
	}

	for n, pods := range nodeToDaemonPods {
		if isNodeRunning[n] {
			continue
		}
		for _, pod := range pods {
			if len(pod.Spec.NodeName) == 0 {
				results = append(results, pod.Name)
			}
		}
	}

	return results
}

// runNodeUpdateWorker 持续处理节点更新队列，直到队列关闭。
func (dsc *DaemonSetsController) runNodeUpdateWorker(ctx context.Context) {
	for dsc.processNextNodeUpdate(ctx) {
	}
}

// processNextNodeUpdate 处理一个节点更新：成功清除限速记录，失败则限速重试。
func (dsc *DaemonSetsController) processNextNodeUpdate(ctx context.Context) bool {
	nodeName, quit := dsc.nodeUpdateQueue.Get()
	if quit {
		return false
	}
	defer dsc.nodeUpdateQueue.Done(nodeName)

	err := dsc.syncNodeUpdate(ctx, nodeName)
	if err == nil {
		dsc.nodeUpdateQueue.Forget(nodeName)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %w", nodeName, err))
	dsc.nodeUpdateQueue.AddRateLimited(nodeName)

	return true
}

// syncNodeUpdate 在节点属性变化后重新评估所有 DaemonSet，
// 仅当调度判定发生改变或误调度计数非零时才入队对应 DaemonSet。
func (dsc *DaemonSetsController) syncNodeUpdate(ctx context.Context, nodeName string) error {
	logger := klog.FromContext(ctx)

	node, err := dsc.nodeLister.Get(nodeName)
	if apierrors.IsNotFound(err) {
		logger.V(3).Info("Node not found, skipping update", "node", nodeName)
		return nil
	}
	if err != nil {
		return fmt.Errorf("error getting node %s: %w", nodeName, err)
	}

	dsList, err := dsc.dsLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("error listing daemon sets: %w", err)
	}

	podsOnNode, err := dsc.podIndexer.ByIndex(controller.PodNodeNameKeyIndex, nodeName)
	if err != nil {
		return fmt.Errorf("error getting pods by node name: %w", err)
	}

	podsByDS := make(map[string][]*v1.Pod)
	for _, obj := range podsOnNode {
		pod := obj.(*v1.Pod)
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil || controllerRef.Kind != controllerKind.Kind {
			continue
		}
		dsKey := cache.NewObjectName(pod.Namespace, controllerRef.Name).String()
		podsByDS[dsKey] = append(podsByDS[dsKey], pod)
	}

	for _, ds := range dsList {
		shouldRun, shouldContinueRunning := NodeShouldRunDaemonPod(logger, node, ds)

		dsKey, err := controller.KeyFunc(ds)
		if err != nil {
			return fmt.Errorf("error getting key for object %#v: %w", ds, err)
		}
		daemonPods := podsByDS[dsKey]
		scheduled := len(daemonPods) > 0

		// 只在需要创建/删除副本或需要重算误调度计数时入队，避免节点更新放大为全量调和。
		// Enqueue DaemonSet for sync in the following scenarios:
		// 1. (shouldRun && !scheduled): Node now meets scheduling requirements but no pod exists
		//    - Need to create a new pod on this node
		// 2. (!shouldContinueRunning && scheduled): Node no longer meets requirements but pod exists
		//    - Need to delete the existing pod from this node
		// 3. (scheduled && ds.Status.NumberMisscheduled > 0): DaemonSet pod exists and misscheduled count is nonzero.
		//    - For example: a pod was scheduled before the node became unready and tainted; after the node becomes ready and taints are removed, the pod may now be valid again.
		//    - Need to recalculate NumberMisscheduled to ensure the DaemonSet status accurately reflects the current scheduling state.
		if (shouldRun && !scheduled) || (!shouldContinueRunning && scheduled) || (scheduled && ds.Status.NumberMisscheduled > 0) {
			dsc.enqueueDaemonSet(ds)
		}
	}

	return nil
}
