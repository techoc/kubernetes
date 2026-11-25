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

// ### 注意 ###
//
// 本文件同时实现了 ReplicaSet 与 ReplicationController。
//
// 对 RC 而言，对象在进出时会被转换（见 ../replication/），
// 就好像 ReplicationController 只是 ReplicaSet 的一个旧 API 版本。
// 但 RC 与 RS 仍然使用独立的存储，并且各自实例化一个 ReplicaSetController 对象。
//
// 在日志消息中应使用 rsc.Kind，而不要硬编码 "ReplicaSet"。

package replicaset

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/replicaset/metrics"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// 以下常量定义副本批量操作上限、状态重试次数以及索引名称。
const (
	// BurstReplicas 是 ReplicaSet manager 中 burstReplica 字段的合理取值，
	// 依据 Kubernetes 1.0 的性能需求确定。
	BurstReplicas = 500

	// statusUpdateRetries 是更新 ReplicaSet 状态时的重试次数。
	statusUpdateRetries = 1

	// controllerUIDIndex 是 ReplicaSet store 索引函数的名称，按 ReplicaSet 的 controllerUID 建索引。
	controllerUIDIndex = "controllerUID"
)

// 以下 GroupResource 用于一致性检查：标记 ReplicaSet 与 Pod 的写入位置。
var (
	replicaSetGroupResource = schema.GroupResource{
		Group:    "apps",
		Resource: "replicasets",
	}
	podGroupResource = schema.GroupResource{
		Group:    "",
		Resource: "pods",
	}
)

// ReplicaSetController 负责把系统中存储的 ReplicaSet 对象与实际运行的 Pod 保持同步。
type ReplicaSetController struct {
	// GroupVersionKind 表示控制器类型。该结构体的不同实例可处理不同的 GVK，
	// 例如借助适配器，本结构体也可用于处理 ReplicationController。
	schema.GroupVersionKind

	kubeClient clientset.Interface
	podControl controller.PodControlInterface
	// podIndexer 支持按 ControllerRef 的 UID 查找 Pod。
	podIndexer       cache.Indexer
	eventBroadcaster record.EventBroadcaster

	// ReplicaSet 在创建/删除这么多副本后会被暂时挂起，
	// 待观察到对应的 watch 事件后再恢复正常操作。
	burstReplicas int
	// syncHandler 便于在测试中注入替身，替换 syncReplicaSet。
	syncHandler func(ctx context.Context, rsKey string) error

	// expectations 是一个 TTLCache，记录每个 rc 预期会观察到的 Pod 创建/删除事件。
	expectations *controller.UIDTrackingControllerExpectations

	// rsLister 是 ReplicaSet 的存储，由传给 NewReplicaSetController 的共享 informer 填充。
	rsLister appslisters.ReplicaSetLister
	// rsListerSynced 在 Pod store 至少同步过一次后返回 true。
	// 作为结构体成员是为了便于测试时注入。
	rsListerSynced cache.InformerSynced
	rsIndexer      cache.Indexer

	// podLister 是 Pod 的存储，由传给 NewReplicaSetController 的共享 informer 填充。
	podLister corelisters.PodLister
	// podListerSynced 在 Pod store 至少同步过一次后返回 true。
	// 作为结构体成员是为了便于测试时注入。
	podListerSynced cache.InformerSynced

	// queue 存放需要同步的控制器。
	queue workqueue.TypedRateLimitingInterface[string]

	clock clock.PassiveClock

	consistencyStore consistencyutil.ConsistencyStore

	// controllerFeatures 是与控制器类型相关的特性，详见 ReplicaSetControllerFeatures。
	controllerFeatures ReplicaSetControllerFeatures
}

// ReplicaSetControllerFeatures 可根据控制器类型（GVK）进行设置。
// 这些特性不必与 FeatureGate 一一对应，也不要求具备相同的稳定性或经历灰度毕业流程。
type ReplicaSetControllerFeatures struct {
	EnableStatusTerminatingReplicas bool
}

// DefaultReplicaSetControllerFeatures 返回 ReplicaSet 控制器特性的默认配置。
func DefaultReplicaSetControllerFeatures() ReplicaSetControllerFeatures {
	return ReplicaSetControllerFeatures{
		EnableStatusTerminatingReplicas: true,
	}
}

// NewReplicaSetController 使用指定的事件记录器配置一个 ReplicaSet 控制器。
func NewReplicaSetController(ctx context.Context, rsInformer appsinformers.ReplicaSetInformer, podInformer coreinformers.PodInformer, kubeClient clientset.Interface, burstReplicas int) *ReplicaSetController {
	logger := klog.FromContext(ctx)
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	if err := metrics.Register(legacyregistry.Register); err != nil {
		logger.Error(err, "unable to register metrics")
	}

	// 启用一致性检查时，用回调记录 Pod 写入的 ResourceVersion；特性关闭时用空实现代替。
	var consistencyStore consistencyutil.ConsistencyStore
	var podWriteCallback func(pod *v1.Pod, rs *metav1.OwnerReference)
	if utilfeature.DefaultFeatureGate.Enabled(features.StaleControllerConsistencyReplicaSet) {
		consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			podGroupResource:        podInformer.Informer().GetStore(),
			replicaSetGroupResource: rsInformer.Informer().GetStore(),
		})
		podWriteCallback = func(pod *v1.Pod, rs *metav1.OwnerReference) {
			if rs == nil {
				return
			}
			consistencyStore.WroteAt(
				types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      rs.Name,
				},
				rs.UID,
				podGroupResource,
				pod.ResourceVersion,
			)
		}
	} else {
		consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}

	return NewBaseController(logger, rsInformer, podInformer, kubeClient, burstReplicas,
		apps.SchemeGroupVersion.WithKind("ReplicaSet"),
		"replicaset_controller",
		"replicaset",
		controller.RealPodControl{
			KubeClient: kubeClient,
			Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "replicaset-controller"}),
			OnWrite:    podWriteCallback,
		},
		eventBroadcaster,
		DefaultReplicaSetControllerFeatures(),
		consistencyStore,
	)
}

// NewBaseController 是 NewReplicaSetController 的实现，通过额外注入参数，
// 也可以作为 NewReplicationController 的实现。
func NewBaseController(logger klog.Logger, rsInformer appsinformers.ReplicaSetInformer, podInformer coreinformers.PodInformer, kubeClient clientset.Interface, burstReplicas int,
	gvk schema.GroupVersionKind, metricOwnerName, queueName string, podControl controller.PodControlInterface, eventBroadcaster record.EventBroadcaster, controllerFeatures ReplicaSetControllerFeatures, consistencyStore consistencyutil.ConsistencyStore) *ReplicaSetController {

	rsc := &ReplicaSetController{
		GroupVersionKind: gvk,
		kubeClient:       kubeClient,
		podControl:       podControl,
		eventBroadcaster: eventBroadcaster,
		burstReplicas:    burstReplicas,
		expectations:     controller.NewUIDTrackingControllerExpectations(controller.NewControllerExpectations()),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: queueName},
		),
		clock:              clock.RealClock{},
		controllerFeatures: controllerFeatures,
		consistencyStore:   consistencyStore,
	}

	rsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rsc.addRS(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			rsc.updateRS(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			rsc.deleteRS(logger, obj)
		},
	})
	rsInformer.Informer().AddIndexers(cache.Indexers{
		controllerUIDIndex: func(obj interface{}) ([]string, error) {
			rs, ok := obj.(*apps.ReplicaSet)
			if !ok {
				return []string{}, nil
			}
			controllerRef := metav1.GetControllerOf(rs)
			if controllerRef == nil {
				return []string{}, nil
			}
			return []string{string(controllerRef.UID)}, nil
		},
	})
	rsc.rsIndexer = rsInformer.Informer().GetIndexer()
	rsc.rsLister = rsInformer.Lister()
	rsc.rsListerSynced = rsInformer.Informer().HasSynced

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rsc.addPod(logger, obj)
		},
		// 这会在每次 Pod 变化时（例如宿主分配）都触发 ReplicaSet 同步。看似小题大做，
		// 但最频繁的 Pod 更新是状态变化，且相关 ReplicaSet 只从本地存储读取，因此开销可接受。
		UpdateFunc: func(oldObj, newObj interface{}) {
			rsc.updatePod(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			rsc.deletePod(logger, obj)
		},
	})
	rsc.podLister = podInformer.Lister()
	rsc.podListerSynced = podInformer.Informer().HasSynced
	controller.AddPodControllerIndexer(podInformer.Informer()) //nolint:errcheck
	rsc.podIndexer = podInformer.Informer().GetIndexer()
	rsc.syncHandler = rsc.syncReplicaSet

	return rsc
}

// Run 启动监听与同步，并阻塞直到 ctx 被取消。
func (rsc *ReplicaSetController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理管线。
	rsc.eventBroadcaster.StartStructuredLogging(3)
	rsc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: rsc.kubeClient.CoreV1().Events("")})
	defer rsc.eventBroadcaster.Shutdown()

	controllerName := strings.ToLower(rsc.Kind)
	logger := klog.FromContext(ctx)
	logger.Info("Starting controller", "name", controllerName)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down controller", "name", controllerName)
		rsc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, rsc.podListerSynced, rsc.rsListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, rsc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// getReplicaSetsWithSameController 返回与给定 ReplicaSet 拥有同一所有者的 ReplicaSet 列表。
func (rsc *ReplicaSetController) getReplicaSetsWithSameController(logger klog.Logger, rs *apps.ReplicaSet) []*apps.ReplicaSet {
	controllerRef := metav1.GetControllerOf(rs)
	if controllerRef == nil {
		utilruntime.HandleError(fmt.Errorf("ReplicaSet has no controller: %v", rs))
		return nil
	}

	objects, err := rsc.rsIndexer.ByIndex(controllerUIDIndex, string(controllerRef.UID))
	if err != nil {
		utilruntime.HandleError(err)
		return nil
	}
	relatedRSs := make([]*apps.ReplicaSet, 0, len(objects))
	for _, obj := range objects {
		relatedRSs = append(relatedRSs, obj.(*apps.ReplicaSet))
	}

	if klogV := logger.V(2); klogV.Enabled() {
		klogV.Info("Found related ReplicaSets", "replicaSet", klog.KObj(rs), "relatedReplicaSets", klog.KObjSlice(relatedRSs))
	}

	return relatedRSs
}

// getPodReplicaSets 返回与给定 Pod 匹配（选择器命中）的 ReplicaSet 列表。
func (rsc *ReplicaSetController) getPodReplicaSets(pod *v1.Pod) []*apps.ReplicaSet {
	rss, err := rsc.rsLister.GetPodReplicaSets(pod)
	if err != nil {
		return nil
	}
	if len(rss) > 1 {
		// ControllerRef 能保证我们不会做出离谱的操作，但列表中有多个匹配项仍说明用户配置有误。
		utilruntime.HandleError(fmt.Errorf("user error! more than one %v is selecting pods with labels: %+v", rsc.Kind, pod.Labels))
	}
	return rss
}

// resolveControllerRef 返回 ControllerRef 所引用的控制器；
// 若无法解析为类型正确的匹配控制器，则返回 nil。
func (rsc *ReplicaSetController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *apps.ReplicaSet {
	// lister 不支持按 UID 查找，因此先按名称查找，再核对 UID。
	// 若 Kind 不匹配，连按名称查找都不必尝试。
	if controllerRef.Kind != rsc.Kind {
		return nil
	}
	rs, err := rsc.rsLister.ReplicaSets(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if rs.UID != controllerRef.UID {
		// 按名称找到的控制器并非 ControllerRef 指向的那一个（可能已被重建）。
		return nil
	}
	return rs
}

// enqueueRS 将给定 ReplicaSet 的 key 加入工作队列。
func (rsc *ReplicaSetController) enqueueRS(rs *apps.ReplicaSet) {
	key, err := controller.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", rs, err))
		return
	}

	rsc.queue.Add(key)
}

// enqueueRSAfter 在指定延时后把 ReplicaSet 的 key 重新加入队列，用于 MinReadySeconds 可用性复查。
func (rsc *ReplicaSetController) enqueueRSAfter(rs *apps.ReplicaSet, duration time.Duration) {
	key, err := controller.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", rs, err))
		return
	}

	rsc.queue.AddAfter(key, duration)
}

// addRS 处理 ReplicaSet 新增事件，将其入队。
func (rsc *ReplicaSetController) addRS(logger klog.Logger, obj interface{}) {
	rs := obj.(*apps.ReplicaSet)
	logger.V(4).Info("Adding", "replicaSet", klog.KObj(rs))
	rsc.enqueueRS(rs)
}

// updateRS 是 ReplicaSet 更新事件的回调；UID 变化视为“先删后建”，同时总是重新入队。
func (rsc *ReplicaSetController) updateRS(logger klog.Logger, old, cur interface{}) {
	oldRS := old.(*apps.ReplicaSet)
	curRS := cur.(*apps.ReplicaSet)

	// TODO: 提交 KEP，并修复 informer，使其在对象重建后始终调用删除事件处理器。
	if curRS.UID != oldRS.UID {
		key, err := controller.KeyFunc(oldRS)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", oldRS, err))
			return
		}
		rsc.deleteRS(logger, cache.DeletedFinalStateUnknown{
			Key: key,
			Obj: oldRS,
		})
	}

	// 直觉上似乎只需在 Spec 变化时入队，但每次触发本回调都同步更安全：
	// informer 全量重新同步时，可以重新入队那些尚无 Pod、且上次创建 Pod 失败过的 ReplicaSet
	// （因为我们不会阻塞等待 Pod 创建完成），避免它们无限期停滞。
	// 每次都入队确实会带来一些无谓的同步（例如 Status.Replicas 更新又触发了本回调），
	// 但额外的重新同步开销不大：尚未满足 expectations 的 ReplicaSet 不会被真正同步，
	// 且所有列表查询都走本地存储。
	if *(oldRS.Spec.Replicas) != *(curRS.Spec.Replicas) {
		logger.V(4).Info("replicaSet updated. Desired pod count change.", "replicaSet", klog.KObj(oldRS), "oldReplicas", *(oldRS.Spec.Replicas), "newReplicas", *(curRS.Spec.Replicas))
	}
	rsc.enqueueRS(curRS)
}

// deleteRS 处理 ReplicaSet 删除事件：清理一致性记录与 expectations，并把 key 入队。
func (rsc *ReplicaSetController) deleteRS(logger klog.Logger, obj interface{}) {
	rs, ok := obj.(*apps.ReplicaSet)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		rs, ok = tombstone.Obj.(*apps.ReplicaSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a ReplicaSet %#v", obj))
			return
		}
	}

	key, err := controller.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", rs, err))
		return
	}

	logger.V(4).Info("Deleting", "replicaSet", klog.KObj(rs))

	rsc.consistencyStore.Clear(types.NamespacedName{
		Namespace: rs.Namespace,
		Name:      rs.Name,
	}, rs.UID)
	// 清理该 ReplicaSet 的 expectations，这样同名的新 ReplicaSet 创建时能从干净状态开始。
	rsc.expectations.DeleteExpectations(logger, key)

	rsc.queue.Add(key)
}

// addPod 处理 Pod 新增事件：把管理该 Pod 的 ReplicaSet 入队，并更新其 expectations。
func (rsc *ReplicaSetController) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)

	if pod.DeletionTimestamp != nil {
		// controller-manager 重启后，新出现的 Pod 可能已处于待删除状态。
		// 按删除事件处理，避免把它误当作一次“创建”观测。
		rsc.deletePod(logger, pod)
		return
	}

	// 有 ControllerRef 时，直接以它为准。
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		rs := rsc.resolveControllerRef(pod.Namespace, controllerRef)
		if rs == nil {
			return
		}
		rsKey, err := controller.KeyFunc(rs)
		if err != nil {
			return
		}
		logger.V(4).Info("Pod created", "pod", klog.KObj(pod), "detail", pod)
		rsc.expectations.CreationObserved(logger, rsKey)
		rsc.queue.Add(rsKey)
		return
	}

	// 否则该 Pod 是孤儿：找出所有匹配的 ReplicaSet 并同步，看是否有对象愿意纳管它。
	// 这里绝不能记录创建观测，因为不应有任何控制器在等待孤儿 Pod 的创建。
	rss := rsc.getPodReplicaSets(pod)
	if len(rss) == 0 {
		return
	}
	logger.V(4).Info("Orphan Pod created", "pod", klog.KObj(pod), "detail", pod)
	for _, rs := range rss {
		rsc.enqueueRS(rs)
	}
}

// updatePod 处理 Pod 更新事件：找出管理它的 ReplicaSet 并唤醒。
// 若 Pod 标签发生变化，则新旧 ReplicaSet 都需要唤醒。old 与 cur 必须是 *v1.Pod 类型。
func (rsc *ReplicaSetController) updatePod(logger klog.Logger, old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// informer 的周期性重新同步会为所有已知 Pod 发送更新事件。
		// 同一 Pod 的两个不同版本必然有不同的 ResourceVersion，因此这里可以直接忽略。
		return
	}

	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)
	if curPod.DeletionTimestamp != nil {
		// Pod 优雅删除时，会先把 deletionTimestamp 设置为宽限期结束时间，
		// 之后 kubelet 才真正把它从存储中删除。我们收到 deletionTimestamp 的更新事件后，
		// 就应期望 ReplicaSet 尽快补充副本，而不是等到 kubelet 真正删除 Pod。
		// 这与 Pod Phase 变化不同：ReplicaSet 从不会主动触发 Phase 变化，因此不存在等待该变化的问题。
		rsc.deletePod(logger, curPod)
		if labelChanged {
			// 无需检查 oldPod.DeletionTimestamp，因为 deletionTimestamp 一旦设置就不能撤销。
			rsc.deletePod(logger, oldPod)
		}
		return
	}

	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// ControllerRef 发生了变化，需要同步旧的控制器（如果存在）。
		if rs := rsc.resolveControllerRef(oldPod.Namespace, oldControllerRef); rs != nil {
			rsc.enqueueRS(rs)
		}
	}

	// 有 ControllerRef 时直接以它为准。
	if curControllerRef != nil {
		rs := rsc.resolveControllerRef(curPod.Namespace, curControllerRef)
		if rs == nil {
			return
		}
		logger.V(4).Info("Pod objectMeta updated.", "pod", klog.KObj(oldPod), "oldObjectMeta", oldPod.ObjectMeta, "curObjectMeta", curPod.ObjectMeta)
		rsc.enqueueRS(rs)
		// TODO: Pod 的 MinReadySeconds 会在 Pod 状态中生成 Available 条件，进而触发所属 ReplicaSet 重新入队，
		//       使其状态计入新变为可用的副本。目前可在 Pod 转为 Ready 并重新入队后，
		//       通过重新同步控制器的 MinReadySeconds 来模拟该更新。
		//       若多个 Pod 的就绪时间不同，当前队列无法准确分别跟踪；syncReplicaSet 末尾会尝试再次重新同步。
		//       注意这仍受 #29229 影响，只是把问题从 Deployment 控制器下移到了 ReplicaSet 控制器。
		if !podutil.IsPodReady(oldPod) && podutil.IsPodReady(curPod) && rs.Spec.MinReadySeconds > 0 {
			logger.V(2).Info("pod will be enqueued after a while for availability check", "duration", rs.Spec.MinReadySeconds, "kind", rsc.Kind, "pod", klog.KObj(oldPod))
			rsc.enqueueRSAfter(rs, time.Duration(rs.Spec.MinReadySeconds)*time.Second)
		}
		return
	}

	// 否则该 Pod 是孤儿：只要有任何变化，就同步匹配的控制器，看是否有人愿意现在纳管它。
	if labelChanged || controllerRefChanged {
		rss := rsc.getPodReplicaSets(curPod)
		if len(rss) == 0 {
			return
		}
		logger.V(4).Info("Orphan Pod objectMeta updated.", "pod", klog.KObj(oldPod), "oldObjectMeta", oldPod.ObjectMeta, "curObjectMeta", curPod.ObjectMeta)
		for _, rs := range rss {
			rsc.enqueueRS(rs)
		}
	}
}

// deletePod 处理 Pod 删除事件：把管理该 Pod 的 ReplicaSet 入队并更新 expectations。
// obj 可能是 *v1.Pod，也可能是 DeletionFinalStateUnknown 标记对象。
func (rsc *ReplicaSetController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	// 删除事件丢失时，relist 会发现 store 中存在而列表中不存在的 Pod，
	// 从而插入一个包含已删除 key/value 的 tombstone 对象。注意该值可能已过期；
	// 若 Pod 的标签发生变化，新的 ReplicaSet 要等到周期性重新同步时才会被唤醒。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %#v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// 没有控制器需要关心孤儿 Pod 的删除。
		return
	}
	rs := rsc.resolveControllerRef(pod.Namespace, controllerRef)
	if rs == nil {
		return
	}
	rsKey, err := controller.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", rs, err))
		return
	}
	logger.V(4).Info("Pod deleted", "delete_by", utilruntime.GetCaller(), "deletion_timestamp", pod.DeletionTimestamp, "pod", klog.KObj(pod))
	rsc.expectations.DeletionObserved(logger, rsKey, controller.PodKey(pod))
	rsc.queue.Add(rsKey)
}

// worker 运行一个工作线程：不断从队列取出条目、处理并标记完成。
// 它保证同一 key 不会被并发调用 syncHandler。
func (rsc *ReplicaSetController) worker(ctx context.Context) {
	for rsc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个条目：成功则清除限流状态，失败则限流重试。
func (rsc *ReplicaSetController) processNextWorkItem(ctx context.Context) bool {
	key, quit := rsc.queue.Get()
	if quit {
		return false
	}
	defer rsc.queue.Done(key)

	err := rsc.syncHandler(ctx, key)
	if err == nil {
		rsc.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("sync %q failed with %v", key, err))
	rsc.queue.AddRateLimited(key)

	return true
}

// manageReplicas 检查并调整给定 ReplicaSet 的副本数。
// 不会修改 activePods；创建/删除 Pod 出错时会让该 ReplicaSet 重新入队。
func (rsc *ReplicaSetController) manageReplicas(ctx context.Context, activePods []*v1.Pod, rs *apps.ReplicaSet) error {
	diff := len(activePods) - int(*(rs.Spec.Replicas))
	rsKey, err := controller.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for %v %#v: %v", rsc.Kind, rs, err))
		return nil
	}
	logger := klog.FromContext(ctx)
	if diff < 0 {
		diff *= -1
		if diff > rsc.burstReplicas {
			diff = rsc.burstReplicas
		}
		// TODO: 像跟踪删除一样跟踪创建操作的 UID。当前记录 Pod UID 需要等待创建结果，
		//       这要求在创建期间持锁，可能成为性能瓶颈。应提前生成 Pod UID，并通过 ExpectCreations 保存。
		rsc.expectations.ExpectCreations(logger, rsKey, diff)
		logger.V(2).Info("Too few replicas", "replicaSet", klog.KObj(rs), "need", *(rs.Spec.Replicas), "creating", diff)
		// 批量创建 Pod：批大小从 SlowStartInitialBatchSize 开始，每成功一轮就翻倍，类似“慢启动”。
		// 这样可应对一次性启动大量 Pod 且它们都因同一错误失败的情形。
		// 例如配额较低的项目试图创建大量 Pod 时，某个 Pod 失败后就不会继续用创建请求淹没 API 服务；
		// 顺带也避免了这些失败产生的大量事件。
		successfulCreations, err := slowStartBatch(diff, controller.SlowStartInitialBatchSize, func() error {
			err := rsc.podControl.CreatePods(ctx, rs.Namespace, &rs.Spec.Template, rs, metav1.NewControllerRef(rs, rsc.GroupVersionKind))
			if err != nil {
				if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
					// 命名空间正在终止时，任何创建都会失败，直接忽略该错误即可。
					return nil
				}
			}
			return err
		})

		// 从未尝试创建的“跳过”Pod 不应计入 expectations。
		// 这些 Pod 会在下次控制器重新同步时通过慢启动流程重试。
		if skippedPods := diff - successfulCreations; skippedPods > 0 {
			logger.V(2).Info("Slow-start failure. Skipping creation of pods, decrementing expectations", "podsSkipped", skippedPods, "kind", rsc.Kind, "replicaSet", klog.KObj(rs))
			for i := 0; i < skippedPods; i++ {
				// informer 不会观察到这些 Pod 的创建，因此递减预期的创建数。
				rsc.expectations.CreationObserved(logger, rsKey)
			}
		}
		return err
	} else if diff > 0 {
		if diff > rsc.burstReplicas {
			diff = rsc.burstReplicas
		}
		logger.V(2).Info("Too many replicas", "replicaSet", klog.KObj(rs), "need", *(rs.Spec.Replicas), "deleting", diff)

		relatedPods, err := rsc.getIndirectlyRelatedPods(logger, rs)
		utilruntime.HandleError(err)

		// 选择要删除的 Pod，优先删除处于启动早期阶段的那些。
		podsToDelete := getPodsToDelete(activePods, relatedPods, diff)

		// 快照预期会被删除的 Pod 的 key（ns/name），这样无论观察到
		// deletionTimestamp 更新事件还是删除事件，都能恰好记录一次 expectation。
		// 注意：若 Pod/RS 的标签变化导致 Pod 成为孤儿，即使其他 Pod 被删除，
		// 该 RS 也只有等到 expectations 过期后才会被唤醒。
		rsc.expectations.ExpectDeletions(logger, rsKey, getPodKeys(podsToDelete))

		errCh := make(chan error, diff)
		var wg sync.WaitGroup
		wg.Add(diff)
		for _, pod := range podsToDelete {
			go func(targetPod *v1.Pod) {
				defer wg.Done()
				if err := rsc.podControl.DeletePod(ctx, rs.Namespace, targetPod.Name, rs); err != nil {
					// 删除失败时 informer 不会观察到该删除事件，因此递减预期的删除数。
					podKey := controller.PodKey(targetPod)
					rsc.expectations.DeletionObserved(logger, rsKey, podKey)
					if !apierrors.IsNotFound(err) {
						logger.V(2).Info("Failed to delete pod, decremented expectations", "pod", podKey, "kind", rsc.Kind, "replicaSet", klog.KObj(rs))
						errCh <- err
					}
				}
			}(pod)
		}
		wg.Wait()

		select {
		case err := <-errCh:
			// 所有错误此前都已记录，且很可能是同一个，因此只返回第一个即可。
			if err != nil {
				return err
			}
		default:
		}
	}

	return nil
}

// syncReplicaSet 只在 expectations 已满足（即不再期待更多 Pod 创建/删除事件）时，
// 同步给定 key 对应的 ReplicaSet。该函数不应以同一 key 并发调用。
func (rsc *ReplicaSetController) syncReplicaSet(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := rsc.clock.Now()
	defer func() {
		logger.V(4).Info("Finished syncing", "kind", rsc.Kind, "key", key, "duration", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	rsNamespacedName := types.NamespacedName{Namespace: namespace, Name: name}
	if err := rsc.consistencyStore.EnsureReady(rsNamespacedName); err != nil {
		var consistencyErr *consistencyutil.ConsistencyError
		if errors.As(err, &consistencyErr) {
			metrics.ReplicaSetRequeueSkips.WithLabelValues(
				consistencyErr.GroupResource.Group,
				consistencyErr.GroupResource.Resource,
			).Inc()
		}
		return err
	}

	rs, err := rsc.rsLister.ReplicaSets(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		logger.V(4).Info("deleted", "kind", rsc.Kind, "key", key)
		rsc.consistencyStore.Clear(rsNamespacedName, "")
		rsc.expectations.DeleteExpectations(logger, key)
		return nil
	}
	if err != nil {
		return err
	}

	rsNeedsSync := rsc.expectations.SatisfiedExpectations(logger, key)
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error converting pod selector to selector for rs %v/%v: %v", namespace, name, err))
		return nil
	}

	// 列出所有按 RS UID 归档的 Pod 以及孤儿 Pod。
	allRSPods, err := controller.FilterPodsByOwner(rsc.podIndexer, &rs.ObjectMeta, rsc.Kind, true)
	if err != nil {
		return err
	}

	// 注意：activePods 与 terminatingPods 指向的是缓存中的对象，若要修改必须先拷贝。
	allActivePods := controller.FilterActivePods(logger, allRSPods)
	activePods, err := rsc.claimPods(ctx, rs, selector, allActivePods)
	if err != nil {
		return err
	}

	var terminatingPods []*v1.Pod
	if utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) && rsc.controllerFeatures.EnableStatusTerminatingReplicas {
		allTerminatingPods := controller.FilterTerminatingPods(allRSPods)
		terminatingPods = controller.FilterClaimedPods(rs, selector, allTerminatingPods)
	}

	var manageReplicasErr error
	var nextSyncDuration *time.Duration
	if rsNeedsSync && rs.DeletionTimestamp == nil {
		manageReplicasErr = rsc.manageReplicas(ctx, activePods, rs)
	}
	rs = rs.DeepCopy()
	// 状态与 nextSyncDuration 使用同一时间点计算，保证可用性判断一致。
	now := rsc.clock.Now()
	newStatus := calculateStatus(rs, activePods, terminatingPods, manageReplicasErr, rsc.controllerFeatures, now)

	// Pod 增减时总是更新状态。
	updatedRS, err := updateReplicaSetStatus(logger, rsc.kubeClient.AppsV1().ReplicaSets(rs.Namespace), rs, newStatus, rsc.controllerFeatures)
	if err != nil {
		// 状态更新失败可能有多种原因；返回错误会让 ReplicaSet 重新入队，又不会造成忙循环。
		return err
	}
	rsc.consistencyStore.WroteAt(
		types.NamespacedName{Name: rs.Name, Namespace: rs.Namespace},
		rs.UID,
		replicaSetGroupResource,
		updatedRS.ResourceVersion,
	)
	if manageReplicasErr != nil {
		return manageReplicasErr
	}
	// 安排下一次可用性检查，作为队列抢占（所有 Pod 的可用性检查共用一个队列 key）
	// 或过早同步的最后一道防线，详见 https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133。
	if updatedRS.Spec.MinReadySeconds > 0 &&
		updatedRS.Status.ReadyReplicas != updatedRS.Status.AvailableReplicas {
		// 回退方案使用 .spec.minReadySeconds，确保 .status.availableReplicas 最终一定会被更新。
		nextSyncDuration = ptr.To(time.Duration(updatedRS.Spec.MinReadySeconds) * time.Second)
		// 状态与 nextSyncDuration 使用同一时间点 now，以得到一致的 Pod 可用性判断。
		if nextCheck := controller.FindMinNextPodAvailabilityCheck(activePods, updatedRS.Spec.MinReadySeconds, now, rsc.clock); nextCheck != nil {
			nextSyncDuration = nextCheck
		}
	}
	if nextSyncDuration != nil {
		rsc.queue.AddAfter(key, *nextSyncDuration)
	}
	return nil
}

// claimPods 通过 ControllerRefManager 让给定 ReplicaSet 纳管匹配的 Pod，返回成功纳管的 Pod 列表。
func (rsc *ReplicaSetController) claimPods(ctx context.Context, rs *apps.ReplicaSet, selector labels.Selector, filteredPods []*v1.Pod) ([]*v1.Pod, error) {
	// 如果要执行纳管，应在列出 Pod 之后用不经缓存的一致性读重新检查对象是否已被删除（见 #42639）。
	canAdoptFunc := controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := rsc.kubeClient.AppsV1().ReplicaSets(rs.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != rs.UID {
			return nil, fmt.Errorf("original %v %v/%v is gone: got uid %v, wanted %v", rsc.Kind, rs.Namespace, rs.Name, fresh.UID, rs.UID)
		}
		return fresh, nil
	})
	cm := controller.NewPodControllerRefManager(rsc.podControl, rs, selector, rsc.GroupVersionKind, canAdoptFunc)
	return cm.ClaimPods(ctx, filteredPods)
}

// slowStartBatch 共调用给定函数 count 次：先以小批量试探错误，成功后再逐步提速。
//
// 调用按批次组织，首批大小为 initialBatchSize，批次内可并发调用多次。
// 某批全部成功后，下一批规模会指数级增大；只要某批出现失败，
// 等待当前批完成后就跳过所有剩余批次。
//
// 返回成功调用的次数。
func slowStartBatch(count int, initialBatchSize int, fn func() error) (int, error) {
	remaining := count
	successes := 0
	for batchSize := min(remaining, initialBatchSize); batchSize > 0; batchSize = min(2*batchSize, remaining) {
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			go func() {
				defer wg.Done()
				if err := fn(); err != nil {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		curSuccesses := batchSize - len(errCh)
		successes += curSuccesses
		if len(errCh) > 0 {
			return successes, <-errCh
		}
		remaining -= batchSize
	}
	return successes, nil
}

// getIndirectlyRelatedPods 返回所有由“与给定 ReplicaSet 同一所有者”的 ReplicaSet 所拥有的 Pod。
func (rsc *ReplicaSetController) getIndirectlyRelatedPods(logger klog.Logger, rs *apps.ReplicaSet) ([]*v1.Pod, error) {
	var relatedPods []*v1.Pod
	seen := make(map[types.UID]*apps.ReplicaSet)
	for _, relatedRS := range rsc.getReplicaSetsWithSameController(logger, rs) {
		selector, err := metav1.LabelSelectorAsSelector(relatedRS.Spec.Selector)
		if err != nil {
			// 选择器非法的对象不会匹配任何 Pod。
			continue
		}
		pods, err := rsc.podLister.Pods(relatedRS.Namespace).List(selector)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if otherRS, found := seen[pod.UID]; found {
				logger.V(5).Info("Pod is owned by both", "pod", klog.KObj(pod), "kind", rsc.Kind, "replicaSets", klog.KObjSlice([]klog.KMetadata{otherRS, relatedRS}))
				continue
			}
			seen[pod.UID] = relatedRS
			relatedPods = append(relatedPods, pod)
		}
	}
	logger.V(4).Info("Found related pods", "kind", rsc.Kind, "replicaSet", klog.KObj(rs), "pods", klog.KObjSlice(relatedPods))
	return relatedPods, nil
}

// getPodsToDelete 选出要删除的 diff 个 Pod，优先删除启动早期阶段的 Pod。
func getPodsToDelete(filteredPods, relatedPods []*v1.Pod, diff int) []*v1.Pod {
	// 若要删除全部 Pod，就无需排序。diff 始终 <= len(filteredPods)，不必处理大于的情况。
	if diff < len(filteredPods) {
		podsWithRanks := getPodsRankedByRelatedPodsOnSameNode(filteredPods, relatedPods)
		sort.Sort(podsWithRanks)
		reportSortingDeletionAgeRatioMetric(filteredPods, diff)
	}
	return filteredPods[:diff]
}

// reportSortingDeletionAgeRatioMetric 上报被选中的 Pod 相对最年轻就绪 Pod 的年龄比值指标。
func reportSortingDeletionAgeRatioMetric(filteredPods []*v1.Pod, diff int) {
	now := time.Now()
	youngestTime := time.Time{}
	// 先找出最年轻的就绪 Pod；Pod 列表不一定按时间戳排序，因此需要遍历全部就绪 Pod。
	for _, pod := range filteredPods {
		if pod.CreationTimestamp.Time.After(youngestTime) && podutil.IsPodReady(pod) {
			youngestTime = pod.CreationTimestamp.Time
		}
	}

	// 对每个将被删除的 Pod，上报其年龄与最年轻 Pod 年龄的比值。
	for _, pod := range filteredPods[:diff] {
		if !podutil.IsPodReady(pod) {
			continue
		}
		ratio := float64(now.Sub(pod.CreationTimestamp.Time).Milliseconds() / now.Sub(youngestTime).Milliseconds())
		metrics.SortingDeletionAgeRatio.Observe(ratio)
	}
}

// getPodsRankedByRelatedPodsOnSameNode 为每个待排序 Pod 计算一个 rank，
// 其值等于 relatedPods 中与它同节点的活跃 Pod 数量；relatedPods 通常应是 podsToRank 的超集。
func getPodsRankedByRelatedPodsOnSameNode(podsToRank, relatedPods []*v1.Pod) controller.ActivePodsWithRanks {
	podsOnNode := make(map[string]int)
	for _, pod := range relatedPods {
		if controller.IsPodActive(pod) {
			podsOnNode[pod.Spec.NodeName]++
		}
	}
	ranks := make([]int, len(podsToRank))
	for i, pod := range podsToRank {
		ranks[i] = podsOnNode[pod.Spec.NodeName]
	}
	return controller.ActivePodsWithRanks{Pods: podsToRank, Rank: ranks, Now: metav1.Now()}
}

// getPodKeys 返回一组 Pod 的 key（命名空间/名称）列表。
func getPodKeys(pods []*v1.Pod) []string {
	podKeys := make([]string, 0, len(pods))
	for _, pod := range pods {
		podKeys = append(podKeys, controller.PodKey(pod))
	}
	return podKeys
}
