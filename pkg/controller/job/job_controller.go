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

package job

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/feature"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/job/metrics"
	"k8s.io/kubernetes/pkg/controller/job/util"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// controllerKind 保存该控制器类型对应的 schema.GroupVersionKind。
var controllerKind = batch.SchemeGroupVersion.WithKind("Job")

var (
	// SyncJobBatchPeriod 是对 Job 触发批量同步的延迟周期。导出以便测试调整。
	SyncJobBatchPeriod = time.Second
	// DefaultJobApiBackOff 是 API 重试的初始退避周期。导出以便测试调整。
	DefaultJobApiBackOff = time.Second
	// MaxJobApiBackOff 是 API 重试的最大退避周期。导出以便测试调整。
	MaxJobApiBackOff = time.Minute
	// DefaultJobPodFailureBackOff 是 Pod 失败后重建的默认退避周期。导出以便测试调整。
	DefaultJobPodFailureBackOff = 10 * time.Second
	// MaxJobPodFailureBackOff 是 Pod 失败后重建的最大退避周期。导出以便测试调整。
	MaxJobPodFailureBackOff = 10 * time.Minute
	// MaxUncountedPods 是 .status.uncountedTerminatedPods 中切片的最大长度，
	// 使其序列化后的体积大致保持在 20 KB 以下。导出以便测试调整。
	MaxUncountedPods = 500
	// MaxPodCreateDeletePerSync 是单次同步中最多可创建或删除的 Pod 数量。导出以便测试调整。
	MaxPodCreateDeletePerSync = 500
)

// Controller 确保所有 Job 对象都有相应的 Pod 来运行其配置的工作负载。
type Controller struct {
	kubeClient clientset.Interface
	podControl controller.PodControlInterface

	// 以下字段可在测试中注入替身实现。
	updateStatusHandler func(ctx context.Context, job *batch.Job) (*batch.Job, error)
	syncHandler         func(ctx context.Context, jobKey string) error
	// podStoreSynced 在 Pod 存储至少同步过一次后返回 true。
	// 作为结构体成员是为了便于测试时注入。
	podStoreSynced cache.InformerSynced
	// jobStoreSynced 在 Job 存储至少同步过一次后返回 true。
	// 作为结构体成员是为了便于测试时注入。
	jobStoreSynced cache.InformerSynced

	// 一个 TTLCache，记录每个 rc 预期会观察到的 Pod 创建/删除事件。
	expectations controller.ControllerExpectationsInterface

	// finalizerExpectations 跟踪那些控制器预期会观察到跟踪 finalizer 被移除的 Pod UID。
	finalizerExpectations *uidTrackingExpectations

	// Job 的存储。
	jobLister batchv1listers.JobLister

	// Pod 的存储，由 podController 填充。
	podStore corelisters.PodLister

	// podIndexer 支持按 ControllerRef UID 查找 Pod。
	podIndexer cache.Indexer

	// workloadIndexer 支持按 Job 名称查找 Workload。
	workloadIndexer cache.Indexer
	// podGroupIndexer 支持按 Workload 名称查找 PodGroup。
	podGroupIndexer cache.Indexer

	// Workload/PodGroup 的 lister 与同步检查。
	// 当对应特性门控关闭时，这些字段为 nil。
	workloadLister schedulinglisters.WorkloadLister
	podGroupLister schedulinglisters.PodGroupLister

	workloadStoreSynced cache.InformerSynced
	podGroupStoreSynced cache.InformerSynced

	// 待更新的 Job 队列。
	queue workqueue.TypedRateLimitingInterface[string]

	// 已被删除、但仍需移除 Job 跟踪 finalizer 的孤立 Pod 队列。
	orphanQueue workqueue.TypedRateLimitingInterface[orphanPodKey]

	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	clock clock.WithTicker

	// 存储用于计算 Pod 失败后重建的指数退避延迟所需的信息。
	podBackoffStore *backoffStore

	// finishedJobExpectations 记录那些状态已完成、但尚未收到对应更新事件的 Job ID。
	finishedJobExpectations sync.Map

	// consistencyStore 保存 Job 的一致性相关信息。
	consistencyStore consistencyutil.ConsistencyStore
}

// syncJobCtx 汇集单次 syncJob 过程中共享的中间状态。
type syncJobCtx struct {
	job                             *batch.Job
	pods                            []*v1.Pod
	finishedCondition               *batch.JobCondition
	activePods                      []*v1.Pod
	succeeded                       int32
	failed                          int32
	prevSucceededIndexes            orderedIntervals
	succeededIndexes                orderedIntervals
	failedIndexes                   *orderedIntervals
	newBackoffRecord                backoffRecord
	expectedRmFinalizers            sets.Set[types.UID]
	uncounted                       *uncountedTerminatedPods
	podsWithDelayedDeletionPerIndex map[int]*v1.Pod
	terminating                     *int32
	ready                           int32

	// 仅当 shouldManageWorkloadForJob 判定该 Job 适合接入 Workload 时才设置。
	podGroup *schedulingv1beta1.PodGroup
}

// orphanPodKeyKind 区分孤立 Pod 队列键的两种查找方式。
type orphanPodKeyKind int

const (
	// OrphanPodKeyKindName 表示按 Pod 名称查找。
	OrphanPodKeyKindName orphanPodKeyKind = iota
	// OrphanPodKeyKindSelector 表示按标签选择器查找。
	OrphanPodKeyKindSelector
)

// orphanPodKey 是孤立 Pod 队列的键，按名称或标签选择器定位 Pod。
type orphanPodKey struct {
	// 取值为名称或选择器。
	kind      orphanPodKeyKind
	namespace string
	// 取值为 Pod 名称或 Pod 标签选择器。
	value string
}

var (
	jobGroupResource = schema.GroupResource{
		Group:    "batch",
		Resource: "jobs",
	}
	podGroupResource = schema.GroupResource{
		Group:    "",
		Resource: "pods",
	}
)

// NewController 创建一个新的 Job 控制器，使相关 Pod 与其对应的 Job 对象保持同步。
// 当 WorkloadWithJob 特性门控关闭时，workloadInformer 和 podGroupInformer 可以为 nil；
// 门控开启时则必须传入。
func NewController(ctx context.Context, kubeClient clientset.Interface,
	podInformer coreinformers.PodInformer, jobInformer batchinformers.JobInformer,
	workloadInformer schedulinginformers.WorkloadInformer,
	podGroupInformer schedulinginformers.PodGroupInformer) (*Controller, error) {

	return newControllerWithClock(ctx, kubeClient, &clock.RealClock{},
		podInformer, jobInformer, workloadInformer, podGroupInformer)
}

// newControllerWithClock 使用指定的时钟创建控制器，便于测试中控制时间流逝。
func newControllerWithClock(ctx context.Context, kubeClient clientset.Interface, clock clock.WithTicker,
	podInformer coreinformers.PodInformer, jobInformer batchinformers.JobInformer,
	workloadInformer schedulinginformers.WorkloadInformer,
	podGroupInformer schedulinginformers.PodGroupInformer,
) (*Controller, error) {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	logger := klog.FromContext(ctx)

	var consistencyStore consistencyutil.ConsistencyStore
	var podWriteCallback func(pod *v1.Pod, ds *metav1.OwnerReference)
	if feature.DefaultFeatureGate.Enabled(features.StaleControllerConsistencyJob) {
		// 记录写入资源版本，供同步前确认 informer 是否已观察到本控制器的写入。
		consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			podGroupResource: podInformer.Informer().GetStore(),
			jobGroupResource: jobInformer.Informer().GetStore(),
		})
		podWriteCallback = func(pod *v1.Pod, job *metav1.OwnerReference) {
			if job == nil {
				return
			}
			consistencyStore.WroteAt(
				types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      job.Name,
				},
				job.UID,
				podGroupResource,
				pod.ResourceVersion,
			)
		}
	} else {
		consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}

	jm := &Controller{
		kubeClient: kubeClient,
		podControl: controller.RealPodControl{
			KubeClient: kubeClient,
			Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "job-controller"}),
			OnWrite:    podWriteCallback,
		},
		expectations:            controller.NewControllerExpectations(),
		finalizerExpectations:   newUIDTrackingExpectations(),
		queue:                   workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.NewTypedItemExponentialFailureRateLimiter[string](DefaultJobApiBackOff, MaxJobApiBackOff), workqueue.TypedRateLimitingQueueConfig[string]{Name: "job", Clock: clock}),
		orphanQueue:             workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.NewTypedItemExponentialFailureRateLimiter[orphanPodKey](DefaultJobApiBackOff, MaxJobApiBackOff), workqueue.TypedRateLimitingQueueConfig[orphanPodKey]{Name: "job_orphan_pod", Clock: clock}),
		broadcaster:             eventBroadcaster,
		recorder:                eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "job-controller"}),
		clock:                   clock,
		podBackoffStore:         newBackoffStore(),
		finishedJobExpectations: sync.Map{},
		consistencyStore:        consistencyStore,
	}

	if _, err := jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			jm.addJob(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			jm.updateJob(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			jm.deleteJob(logger, obj)
		},
	}); err != nil {
		return nil, fmt.Errorf("adding Job event handler: %w", err)
	}
	jm.jobLister = jobInformer.Lister()
	jm.jobStoreSynced = jobInformer.Informer().HasSynced

	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			jm.addPod(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			jm.updatePod(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			jm.deletePod(logger, obj, true)
		},
	}); err != nil {
		return nil, fmt.Errorf("adding Pod event handler: %w", err)
	}
	jm.podStore = podInformer.Lister()
	jm.podStoreSynced = podInformer.Informer().HasSynced

	err := controller.AddPodControllerIndexer(podInformer.Informer())
	if err != nil {
		return nil, fmt.Errorf("adding Pod controller UID indexer: %w", err)
	}
	jm.podIndexer = podInformer.Informer().GetIndexer()

	jm.updateStatusHandler = jm.updateJobStatus
	jm.syncHandler = jm.syncJob

	if feature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) {
		if err := jm.addSchedulingInformers(logger, workloadInformer, podGroupInformer); err != nil {
			return nil, err
		}
	}

	metrics.Register()

	return jm, nil
}

// Run 启动负责监听和同步 Job 的主 goroutine。
func (jm *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线。
	jm.broadcaster.StartStructuredLogging(3)
	jm.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: jm.kubeClient.CoreV1().Events("")})
	defer jm.broadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting job controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down job controller")
		jm.queue.ShutDown()
		jm.orphanQueue.ShutDown()
		wg.Wait()
	}()

	syncFuncs := []cache.InformerSynced{jm.podStoreSynced, jm.jobStoreSynced}
	if feature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) {
		syncFuncs = append(syncFuncs, jm.workloadStoreSynced, jm.podGroupStoreSynced)
	}
	if !cache.WaitForNamedCacheSyncWithContext(ctx, syncFuncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, jm.worker, time.Second)
		})
		wg.Go(func() {
			wait.UntilWithContext(ctx, jm.orphanWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// getPodJobs 返回可能与某个 Pod 匹配的 Job 列表。
func (jm *Controller) getPodJobs(pod *v1.Pod) []*batch.Job {
	jobs, err := jm.jobLister.GetPodJobs(pod)
	if err != nil {
		return nil
	}
	if len(jobs) > 1 {
		// ControllerRef 会防止误操作，但多个 Job 同时选中该 Pod 仍属于用户配置错误。
		utilruntime.HandleError(fmt.Errorf("user error! more than one job is selecting pods with labels: %+v", pod.Labels))
	}
	ret := make([]*batch.Job, 0, len(jobs))
	for i := range jobs {
		ret = append(ret, &jobs[i])
	}
	return ret
}

// resolveControllerRef 返回 ControllerRef 所指向的控制器；
// 若无法解析为类型正确且匹配的控制器则返回 nil。
func (jm *Controller) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *batch.Job {
	// 无法按 UID 查询，因此先按名称查询再校验 UID。
	// 类型不匹配时连名称查询都不必做。
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	job, err := jm.jobLister.Jobs(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if job.UID != controllerRef.UID {
		// 按名称找到的控制器与 ControllerRef 指向的并非同一个对象。
		return nil
	}
	return job
}

// addPod 在 Pod 创建时入队管理它的控制器，并更新相应的预期计数。
func (jm *Controller) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)
	recordFinishedPodWithTrackingFinalizer(nil, pod)
	if pod.DeletionTimestamp != nil {
		// 控制器重启后，新出现的 Pod 可能已处于待删除状态；按删除而非创建处理，
		// 避免误当作一次创建观察而破坏预期计数。
		jm.deletePod(logger, pod, false)
		return
	}

	// 有 ControllerRef 时，其指向的控制器即为唯一归属。
	if controllerRef := metav1.GetControllerOf(pod); controllerRef != nil {
		job := jm.resolveControllerRef(pod.Namespace, controllerRef)
		if job == nil {
			return
		}
		jobKey, err := controller.KeyFunc(job)
		if err != nil {
			return
		}
		jm.expectations.CreationObserved(logger, jobKey)
		jm.enqueueSyncJobBatched(logger, job)
		return
	}

	// 否则该 Pod 是孤立对象，先清理其 finalizer。
	if hasJobTrackingFinalizer(pod) {
		jm.enqueueOrphanPod(pod, false)
	}
	// 唤醒所有可能匹配的控制器，看是否有 Job 愿意收养它。
	// 不要记录创建观察：没有控制器会等待孤立 Pod 的创建事件。
	for _, job := range jm.getPodJobs(pod) {
		jm.enqueueSyncJobBatched(logger, job)
	}
}

// updatePod 在 Pod 更新时找出管理它的 Job 并唤醒它们。
// Pod 标签变化时新旧 Job 都需要唤醒；old 和 cur 必须是 *v1.Pod 类型。
func (jm *Controller) updatePod(logger klog.Logger, old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	recordFinishedPodWithTrackingFinalizer(oldPod, curPod)
	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// 周期性 resync 会为所有已知 Pod 发送更新事件；
		// 同一 Pod 的不同版本必然有不同的 ResourceVersion，因此这里可直接跳过。
		return
	}
	if curPod.DeletionTimestamp != nil {
		// 优雅删除时先设置删除时间戳，宽限期过后 kubelet 才真正从存储中移除 Pod。
		// 因此收到删除时间戳的更新就应按删除处理，让 Job 尽快补建 Pod，
		// 而不必等到 kubelet 真正删除。
		jm.deletePod(logger, curPod, false)
		return
	}

	// 不检查 oldPod 是否带有 finalizer：所有权转移期间，finalizer 可能代表新所有者
	// 被重新添加又移除。若多次 Pod 更新合并成一个事件，oldPod 和 curPod 中的
	// finalizer 可能都已被移除，因此以最新状态为准。
	finalizerRemoved := !hasJobTrackingFinalizer(curPod)
	curControllerRef := metav1.GetControllerOf(curPod)
	oldControllerRef := metav1.GetControllerOf(oldPod)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// ControllerRef 发生了变化，需要同步旧控制器（如果存在）。
		if job := jm.resolveControllerRef(oldPod.Namespace, oldControllerRef); job != nil {
			if finalizerRemoved {
				key, err := controller.KeyFunc(job)
				if err == nil {
					jm.finalizerExpectations.finalizerRemovalObserved(logger, key, curPod.UID)
				}
			}
			jm.enqueueSyncJobBatched(logger, job)
		}
	}

	// 有 ControllerRef 时，其指向的控制器即为唯一归属。
	if curControllerRef != nil {
		job := jm.resolveControllerRef(curPod.Namespace, curControllerRef)
		if job == nil {
			return
		}
		if finalizerRemoved {
			key, err := controller.KeyFunc(job)
			if err == nil {
				jm.finalizerExpectations.finalizerRemovalObserved(logger, key, curPod.UID)
			}
		}
		jm.enqueueSyncJobBatched(logger, job)
		return
	}

	// 否则该 Pod 是孤立对象，先清理其 finalizer。
	if hasJobTrackingFinalizer(curPod) {
		jm.enqueueOrphanPod(curPod, false)
	}
	// 若标签或归属发生变化，唤醒匹配的控制器，看现在是否有 Job 愿意收养它。
	labelChanged := !reflect.DeepEqual(curPod.Labels, oldPod.Labels)
	if labelChanged || controllerRefChanged {
		for _, job := range jm.getPodJobs(curPod) {
			jm.enqueueSyncJobBatched(logger, job)
		}
	}
}

// deletePod 在 Pod 删除时入队管理它的 Job 并更新预期计数。
// obj 可以是 *v1.Pod，也可以是 DeleteFinalStateUnknown 标记对象。
func (jm *Controller) deletePod(logger klog.Logger, obj interface{}, final bool) {
	pod, ok := obj.(*v1.Pod)
	if final {
		recordFinishedPodWithTrackingFinalizer(pod, nil)
	}

	// 删除事件丢失时，relist 会发现存储中的 Pod 不在最新列表里，从而插入一个
	// 含已删除键值对的墓碑对象。注意其中的值可能已过期：若 Pod 标签变了，
	// 新的 Job 要等到周期性 resync 才会被唤醒。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %+v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(pod)
	hasFinalizer := hasJobTrackingFinalizer(pod)
	if controllerRef == nil {
		// 正常情况下没有控制器关心孤立 Pod 的删除；
		// 但它可能原本属于某个 Job，只是 GC 已移除了引用。
		if hasFinalizer {
			jm.enqueueOrphanPod(pod, false)
		}
		return
	}
	job := jm.resolveControllerRef(pod.Namespace, controllerRef)
	if job == nil || util.IsJobFinished(job) {
		// 这两种情况下 syncJob 都不会移除该 finalizer。
		if hasFinalizer {
			jm.enqueueOrphanPod(pod, false)
		}
		return
	}
	jobKey, err := controller.KeyFunc(job)
	if err != nil {
		return
	}
	jm.expectations.DeletionObserved(logger, jobKey)

	// 真正的删除事件意味着 finalizer 已被移除；若只是删除时间戳更新，则需检查 finalizer。
	if final || !hasFinalizer {
		jm.finalizerExpectations.finalizerRemovalObserved(logger, jobKey, pod.UID)
	}

	jm.enqueueSyncJobBatched(logger, job)
}

// addJob 在 Job 创建时入队同步，并统计由外部控制器管理的 Job 数量。
func (jm *Controller) addJob(logger klog.Logger, obj interface{}) {
	jm.enqueueSyncJobImmediately(logger, obj)
	jobObj, ok := obj.(*batch.Job)
	if !ok {
		return
	}
	if controllerName := managedByExternalController(jobObj); controllerName != nil {
		metrics.JobByExternalControllerTotal.WithLabelValues(*controllerName).Inc()
	}
}

// updateJob 在 Job 更新时触发同步：状态更新走批量延迟，spec 变更则立即同步。
func (jm *Controller) updateJob(logger klog.Logger, old, cur interface{}) {
	oldJob := old.(*batch.Job)
	curJob := cur.(*batch.Job)

	// KeyFunc 对 Job 对象不会返回错误。
	key, err := controller.KeyFunc(curJob)
	if err != nil {
		return
	}
	// Job 只有在所有 Pod finalizer 移除后才应标记为完成，这里作为兜底再触发一次清理。
	if util.IsJobFinished(curJob) {
		jm.enqueueLabelSelector(curJob)
		return
	}

	if curJob.Generation == oldJob.Generation {
		// generation 未变说明只是状态更新（通常由 Pod 事件触发），延迟同步以便批量处理。
		jm.enqueueSyncJobBatched(logger, curJob)
	} else {
		// spec 发生变化时立即同步。
		jm.enqueueSyncJobImmediately(logger, curJob)
	}

	// ActiveDeadlineSeconds 被修改时，需要按新的截止时间重新安排一次同步。
	if curJob.Status.StartTime != nil {
		curADS := curJob.Spec.ActiveDeadlineSeconds
		if curADS == nil {
			return
		}
		oldADS := oldJob.Spec.ActiveDeadlineSeconds
		if oldADS == nil || *oldADS != *curADS {
			passed := jm.clock.Since(curJob.Status.StartTime.Time)
			total := time.Duration(*curADS) * time.Second
			// total < passed 时 AddAfter 会立即执行，无需特殊处理负数延迟。
			jm.queue.AddAfter(key, total-passed)
			logger.V(4).Info("job's ActiveDeadlineSeconds updated, will rsync", "key", key, "interval", total-passed)
		}
	}
}

// deleteJob 在 Job 删除时入队同步，并把仍带有其 finalizer 的 Pod 一并交给孤立 Pod 清理流程。
func (jm *Controller) deleteJob(logger klog.Logger, obj interface{}) {
	jm.enqueueSyncJobImmediately(logger, obj)
	jobObj, ok := obj.(*batch.Job)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		jobObj, ok = tombstone.Obj.(*batch.Job)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a job %+v", obj))
			return
		}
	}
	jm.consistencyStore.Clear(types.NamespacedName{
		Namespace: jobObj.Namespace,
		Name:      jobObj.Name,
	}, jobObj.UID)
	jm.finishedJobExpectations.Delete(jobObj.UID)
	jm.enqueueLabelSelector(jobObj)

	key := cache.MetaObjectToName(jobObj).String()
	// 清除该 Job 的预期计数，使同名的新 Job 能从干净状态开始。
	jm.expectations.DeleteExpectations(logger, key)
	err := jm.podBackoffStore.removeBackoffRecord(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error removing backoff record %w", err))
	}
}

// enqueueLabelSelector 按 Job 的标签选择器入队一次孤立 Pod 清理，覆盖那些已失去归属的 Pod。
func (jm *Controller) enqueueLabelSelector(jobObj *batch.Job) {
	selector, err := metav1.LabelSelectorAsSelector(jobObj.Spec.Selector)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("job %s/%s has invalid label selector: %w", jobObj.Namespace, jobObj.Name, err))
		return
	}
	orphanPodKey := orphanPodKey{
		kind:      OrphanPodKeyKindSelector,
		namespace: jobObj.Namespace,
		value:     selector.String(),
	}
	jm.orphanQueue.Add(orphanPodKey)
}

// enqueueSyncJobImmediately 让 Job 控制器立即调用 syncJob。
// 仅用于 Job 事件（创建、删除、spec 更新）。
// obj 可以是 *batch.Job，也可以是 DeletionFinalStateUnknown 标记对象。
func (jm *Controller) enqueueSyncJobImmediately(logger klog.Logger, obj interface{}) {
	jm.enqueueSyncJobInternal(logger, obj, 0)
}

// enqueueSyncJobBatched 让控制器延迟一个固定批量周期后调用 syncJob。
// 用于以下场景：
// - Pod 事件（创建、删除、更新）
// - Job 状态更新
// obj 可以是 *batch.Job，也可以是 DeletionFinalStateUnknown 标记对象。
func (jm *Controller) enqueueSyncJobBatched(logger klog.Logger, obj interface{}) {
	jm.enqueueSyncJobInternal(logger, obj, SyncJobBatchPeriod)
}

// enqueueSyncJobWithDelay 让控制器按自定义延迟调用 syncJob，但不会小于批量延迟。
// 用于 Pod 失败后需要延迟重建的场景。
// obj 可以是 *batch.Job，也可以是 DeletionFinalStateUnknown 标记对象。
func (jm *Controller) enqueueSyncJobWithDelay(logger klog.Logger, obj interface{}, delay time.Duration) {
	if delay < SyncJobBatchPeriod {
		delay = SyncJobBatchPeriod
	}
	jm.enqueueSyncJobInternal(logger, obj, delay)
}

// enqueueSyncJobInternal 按指定延迟把 Job 的键加入工作队列。
func (jm *Controller) enqueueSyncJobInternal(logger klog.Logger, obj interface{}, delay time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	// TODO: Handle overlapping controllers better. Either disallow them at admission time or
	// deterministically avoid syncing controllers that fight over pods. Currently, we only
	// ensure that the same controller is synced for a given pod. When we periodically relist
	// all controllers there will still be some replica instability. One way to handle this is
	// by querying the store for all controllers that this rc overlaps, as well as all
	// controllers that overlap this rc, and sorting them.
	logger.V(2).Info("enqueueing job", "key", key, "delay", delay)
	jm.queue.AddAfter(key, delay)
}

// enqueueOrphanPod 把一个孤立 Pod 加入清理队列，backoff 指定是否使用限速重试。
func (jm *Controller) enqueueOrphanPod(obj *v1.Pod, backoff bool) {
	orphanPodKey := orphanPodKey{
		kind:      OrphanPodKeyKindName,
		namespace: obj.Namespace,
		value:     obj.Name,
	}
	if backoff {
		jm.orphanQueue.AddRateLimited(orphanPodKey)
	} else {
		jm.orphanQueue.Add(orphanPodKey)
	}
}

// worker 运行工作线程，不断出队、处理并标记任务完成。
// 它保证同一个键不会被并发调用 syncHandler。
func (jm *Controller) worker(ctx context.Context) {
	for jm.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 Job 键，失败时按限速重新入队。
func (jm *Controller) processNextWorkItem(ctx context.Context) bool {
	key, quit := jm.queue.Get()
	if quit {
		return false
	}
	defer jm.queue.Done(key)

	err := jm.syncHandler(ctx, key)
	if err == nil {
		jm.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("syncing job: %w", err))
	jm.queue.AddRateLimited(key)

	return true
}

// orphanWorker 运行处理孤立 Pod 清理队列的工作线程。
func (jm *Controller) orphanWorker(ctx context.Context) {
	for jm.processNextOrphanPod(ctx) {
	}
}

// processNextOrphanPod 处理队列中的一个孤立 Pod 键，失败时按限速重试。
func (jm *Controller) processNextOrphanPod(ctx context.Context) bool {
	key, quit := jm.orphanQueue.Get()
	if quit {
		return false
	}
	defer jm.orphanQueue.Done(key)
	err := jm.syncOrphanPod(ctx, key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Error syncing orphan pod: %v", err))
		jm.orphanQueue.AddRateLimited(key)
	} else {
		jm.orphanQueue.Forget(key)
	}

	return true
}

// syncOrphanPod 按队列键找到孤立 Pod 并移除其跟踪 finalizer。
func (jm *Controller) syncOrphanPod(ctx context.Context, key orphanPodKey) error {
	startTime := jm.clock.Now()
	logger := klog.FromContext(ctx)
	defer func() {
		logger.V(4).Info("Finished syncing orphan pod", "pod", key, "elapsed", jm.clock.Since(startTime))
	}()

	switch key.kind {
	case OrphanPodKeyKindName:
		pod, err := jm.podStore.Pods(key.namespace).Get(key.value)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.V(4).Info("Orphan pod has been deleted", "pod", klog.KRef(key.namespace, key.value))
				return nil
			}
			return err
		}
		return jm.handleSingleOrphanPod(ctx, pod)
	case OrphanPodKeyKindSelector:
		logger.V(8).Info("syncing all pods matching the label selector", "namespace", key.namespace, "labelSelector", key.value)
		return jm.syncOrphanPodsBySelector(ctx, key.namespace, key.value)
	default:
		return fmt.Errorf("unknown key type: %d", key.kind)
	}
}

// syncOrphanPodsBySelector 获取并处理所有匹配给定标签选择器的 Pod。
func (jm *Controller) syncOrphanPodsBySelector(ctx context.Context, namespace string, labelSelector string) error {
	logger := klog.FromContext(ctx)
	selector, err := labels.Parse(labelSelector)
	if err != nil {
		return fmt.Errorf("invalid label selector: %w", err)
	}

	// 列出所有匹配标签选择器的 Pod：该操作相对昂贵，但仅由孤立 Pod 清理流程调用。
	pods, err := jm.podStore.Pods(namespace).List(selector)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if err := jm.handleSingleOrphanPod(ctx, pod); err != nil {
			logger.Error(err, "syncing orphan pod failed", "pod", klog.KObj(pod))
			jm.enqueueOrphanPod(pod, true)
		}
	}
	return nil
}

// handleSingleOrphanPod 处理单个孤立 Pod：确认其未被 Job 占用后移除跟踪 finalizer。
func (jm *Controller) handleSingleOrphanPod(ctx context.Context, sharedPod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	ns := sharedPod.Namespace
	name := sharedPod.Name
	// 先确认 Pod 仍处于孤立状态。
	if controllerRef := metav1.GetControllerOf(sharedPod); controllerRef != nil {
		if controllerRef.Kind != controllerKind.Kind || controllerRef.APIVersion != batch.SchemeGroupVersion.String() {
			// Pod 的所有者不是 batch/v1 Job，不能移除其 finalizer。
			return nil
		}
		job := jm.resolveControllerRef(ns, controllerRef)
		if job != nil {
			// 外部控制器管理的 Job 所拥有的 Pod，其 finalizer 由该外部控制器负责，跳过清理。
			if controllerName := managedByExternalController(job); controllerName != nil {
				logger.V(2).Info("Skip cleanup of the job finalizer for a pod owned by a job that is managed by an external controller", "namespace", ns, "name", name, "podUID", sharedPod.UID, "jobUID", job.UID, "controllerName", controllerName)
				return nil
			}
		}
		if job != nil && !util.IsJobFinished(job) {
			// Pod 已被 Job 收养，不能移除 finalizer。
			return nil
		}
	}
	if patch := removeTrackingFinalizerPatch(sharedPod); patch != nil {
		if err := jm.podControl.PatchPod(ctx, ns, name, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// getPodsForJob 返回该 Job 应管理的 Pod 集合。
// 它同时通过收养/孤立 Pod、添加跟踪 finalizer 来协调 ControllerRef。
// 注意返回的 Pod 是指向缓存对象的指针。
func (jm *Controller) getPodsForJob(ctx context.Context, j *batch.Job) ([]*v1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(j.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}

	// 通过 Pod 索引器列出所有由该 Job 管理的 Pod。
	pods, err := controller.FilterPodsByOwner(jm.podIndexer, &j.ObjectMeta, "Job", true)
	if err != nil {
		return nil, err
	}
	// 若可能执行收养操作，需在列出 Pod 之后用非缓存的一致性读再次确认 Job 未被删除（见 #42639）。
	canAdoptFunc := controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := jm.kubeClient.BatchV1().Jobs(j.Namespace).Get(ctx, j.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != j.UID {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v", j.Namespace, j.Name, fresh.UID, j.UID)
		}
		return fresh, nil
	})
	cm := controller.NewPodControllerRefManager(jm.podControl, j, selector, controllerKind, canAdoptFunc, batch.JobTrackingFinalizer)
	// 收养 Pod 时，该操作会添加 ownerRef 和 finalizer。
	pods, err = cm.ClaimPods(ctx, pods)
	if err != nil {
		return pods, err
	}
	// 为刚收养的 Pod 补上 finalizer，以便后续计数时将其视为受管理对象。
	for i, p := range pods {
		adopted := true
		for _, r := range p.OwnerReferences {
			if r.UID == j.UID {
				adopted = false
				break
			}
		}
		if adopted && !hasJobTrackingFinalizer(p) {
			pods[i] = p.DeepCopy()
			pods[i].Finalizers = append(p.Finalizers, batch.JobTrackingFinalizer)
		}
	}
	return pods, err
}

// syncJob 在预期计数已满足（即不再期待更多 Pod 创建或删除事件）时同步给定键的 Job。
// 同一个键不应并发调用该函数。
func (jm *Controller) syncJob(ctx context.Context, key string) (rErr error) {
	startTime := jm.clock.Now()
	logger := klog.FromContext(ctx)
	defer func() {
		logger.V(4).Info("Finished syncing job", "key", key, "elapsed", jm.clock.Since(startTime))
	}()

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if len(ns) == 0 || len(name) == 0 {
		return fmt.Errorf("invalid job key %q: either namespace or name is missing", key)
	}
	jobNamespacedName := types.NamespacedName{Namespace: ns, Name: name}
	// 若本控制器先前的写入尚未被 informer 观察到，则当前读到的状态与已执行的操作不一致，
	// 此时继续同步可能基于过期状态做出错误决策。
	if err := jm.consistencyStore.EnsureReady(jobNamespacedName); err != nil {
		var consistencyErr *consistencyutil.ConsistencyError
		if errors.As(err, &consistencyErr) {
			metrics.JobRequeueSkips.WithLabelValues(
				consistencyErr.GroupResource.Group,
				consistencyErr.GroupResource.Resource,
			).Inc()
		}
		return err
	}

	sharedJob, err := jm.jobLister.Jobs(ns).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(4).Info("Job has been deleted", "key", key)
			jm.expectations.DeleteExpectations(logger, key)
			jm.finalizerExpectations.deleteExpectations(logger, key)
			jm.consistencyStore.Clear(jobNamespacedName, "")

			err := jm.podBackoffStore.removeBackoffRecord(key)
			if err != nil {
				// 已结束/已删除的 Job 必须清除退避记录，因此返回错误以触发重新同步。
				return fmt.Errorf("error removing backoff record %w", err)
			}
			return nil
		}
		return err
	}

	// 跳过由其他控制器管理的 Job。
	// 不能只依赖入队时过滤：任务入队时 Job 可能还没有 managedBy 字段，
	// 随后被带该字段的同名 Job 替换，导致 syncJob 仍可能处理到这样的 Job。
	if controllerName := managedByExternalController(sharedJob); controllerName != nil {
		logger.V(2).Info("Skip syncing the job as it is managed by an external controller", "key", key, "uid", sharedJob.UID, "controllerName", controllerName)
		return nil
	}

	// 深拷贝一份，避免修改 informer 的共享缓存。
	job := *sharedJob.DeepCopy()

	// 已结束的 Job 不应重复执行终止流程。
	if util.IsJobFinished(&job) {
		err := jm.podBackoffStore.removeBackoffRecord(key)
		if err != nil {
			// 已结束的 Job 必须清除退避记录，返回错误以触发重新同步。
			return fmt.Errorf("error removing backoff record %w", err)
		}
		jm.consistencyStore.Clear(types.NamespacedName{
			Namespace: job.Namespace,
			Name:      job.Name,
		}, job.UID)
		jm.finishedJobExpectations.Delete(job.UID)
		return nil
	}
	if _, ok := jm.finishedJobExpectations.Load(job.UID); ok {
		logger.V(2).Info("Skip syncing the job as its marked finished but the corresponding update event is not yet received", "uid", job.UID, "key", key)
		return nil
	}

	if job.Spec.CompletionMode != nil && *job.Spec.CompletionMode != batch.NonIndexedCompletion && *job.Spec.CompletionMode != batch.IndexedCompletion {
		jm.recorder.Event(&job, v1.EventTypeWarning, "UnknownCompletionMode", "Skipped Job sync because completion mode is unknown")
		return nil
	}

	completionMode := getCompletionMode(&job)
	action := metrics.JobSyncActionReconciling

	defer func() {
		result := "success"
		if rErr != nil {
			result = "error"
		}

		metrics.JobSyncDurationSeconds.WithLabelValues(completionMode, result, action).Observe(jm.clock.Since(startTime).Seconds())
		metrics.JobSyncNum.WithLabelValues(completionMode, result, action).Inc()
	}()

	if job.Status.UncountedTerminatedPods == nil {
		job.Status.UncountedTerminatedPods = &batch.UncountedTerminatedPods{}
	}

	// 必须在统计活跃 Pod 之前检查预期计数：否则新 Pod 可能在读取之后才进入存储并更新预期，
	// 导致本轮基于不完整快照做决策。若检查后才有新 Pod 进入，则把同步推迟到下一次 relist。
	satisfiedExpectations := jm.expectations.SatisfiedExpectations(logger, key)

	pods, err := jm.getPodsForJob(ctx, &job)
	if err != nil {
		return err
	}

	// 为符合条件的 Job 确保 Workload 和 PodGroup 存在。
	// 必须在管理 Pod 之前完成，这样新建的 Pod 才能引用 PodGroup。
	var podGroup *schedulingv1beta1.PodGroup
	if shouldManageWorkloadForJob(&job) {
		_, podGroup, err = jm.ensureWorkloadAndPodGroup(ctx, &job, pods)
		if err != nil {
			return fmt.Errorf("ensuring Workload and PodGroup for Job %s/%s: %w", job.Namespace, job.Name, err)
		}
	}

	activePods := controller.FilterActivePods(logger, pods)
	jobCtx := &syncJobCtx{
		job:                  &job,
		pods:                 pods,
		activePods:           activePods,
		ready:                countReadyPods(activePods),
		uncounted:            newUncountedTerminatedPods(*job.Status.UncountedTerminatedPods),
		expectedRmFinalizers: jm.finalizerExpectations.getExpectedUIDs(key),
		podGroup:             podGroup,
		terminating:          ptr.To(controller.CountTerminatingPods(pods)),
	}
	active := int32(len(jobCtx.activePods))
	newSucceededPods, newFailedPods := getNewFinishedPods(jobCtx)
	jobCtx.succeeded = job.Status.Succeeded + int32(len(newSucceededPods)) + int32(len(jobCtx.uncounted.succeeded))
	jobCtx.failed = job.Status.Failed + int32(nonIgnoredFailedPodsCount(jobCtx, newFailedPods)) + int32(len(jobCtx.uncounted.failed))

	// Job 首次启动时设置 StartTime；处于挂起状态的 Job 不计时。
	if job.Status.StartTime == nil && !jobSuspended(&job) {
		now := metav1.NewTime(jm.clock.Now())
		job.Status.StartTime = &now
	}

	jobCtx.newBackoffRecord = jm.podBackoffStore.newBackoffRecord(key, newSucceededPods, newFailedPods)

	var manageJobErr error

	// 这是判定 Job 终态的起点。
	// 判定必须按序进行，因为某些情况下 Job 会同时满足多个条件：
	// 1. 先检查既有的 SuccessCriteriaMet 和 FailureTarget 以尊重上一轮协调结果，再将 FailureTarget 转为 Failed。
	// 2. 判定失败场景。
	// 3. 判定成功场景。
	// 4. 判定 jobCtx.finishedCondition（见 trackJobStatusAndRemoveFinalizers），Job 结束后把
	//    FailureTarget 转为 Failed、SuccessCriteriaMet 转为 Complete。

	exceedsBackoffLimit := jobCtx.failed > *job.Spec.BackoffLimit
	// 优先沿用已有的 SuccessCriteriaMet，尊重上一轮的判定结果。
	jobCtx.finishedCondition = hasSuccessCriteriaMetCondition(&job)

	// Job 一旦带有 SuccessCriteriaMet，说明终止条件在之前的轮次已确认；
	// 因此只有在没有该条件时才需要重新评估 podFailurePolicy。
	if jobCtx.finishedCondition == nil {
		// 沿用已有的 FailureTarget，避免重复生成失败条件。
		failureTargetCondition := findConditionByType(job.Status.Conditions, batch.JobFailureTarget)
		if failureTargetCondition != nil && failureTargetCondition.Status == v1.ConditionTrue {
			jobCtx.finishedCondition = newFailedConditionForFailureTarget(failureTargetCondition, jm.clock.Now())
			// 判定 PodFailurePolicy 触发的失败。
		} else if failJobMessage := getFailJobMessage(&job, pods); failJobMessage != nil {
			// 先写入临时的 FailureTarget 条件保存失败原因，之后再移除 finalizer 以允许删除 Pod。
			jobCtx.finishedCondition = newCondition(batch.JobFailureTarget, v1.ConditionTrue, batch.JobReasonPodFailurePolicy, *failJobMessage, jm.clock.Now())
		}
	}
	// 判定 BackoffLimit 和 ActiveDeadlineSeconds 触发的失败。
	if jobCtx.finishedCondition == nil {
		if exceedsBackoffLimit || pastBackoffLimitOnFailure(&job, pods) {
			// 检查 Pod 重启次数是否超过 backoff（仅 restartPolicy=OnFailure 适用），
			// 或自上次同步以来失败 Pod 数是否增加。
			jobCtx.finishedCondition = jm.newFailureCondition(batch.JobReasonBackoffLimitExceeded, "Job has reached the specified backoff limit")
		} else if jm.pastActiveDeadline(&job) {
			jobCtx.finishedCondition = jm.newFailureCondition(batch.JobReasonDeadlineExceeded, "Job was active longer than specified deadline")
		} else if job.Spec.ActiveDeadlineSeconds != nil && !jobSuspended(&job) {
			syncDuration := time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second - jm.clock.Since(job.Status.StartTime.Time)
			logger.V(2).Info("Job has activeDeadlineSeconds configuration. Will sync this job again", "key", key, "nextSyncIn", syncDuration)
			jm.queue.AddAfter(key, syncDuration)
		}
	}

	if isIndexedJob(&job) {
		jobCtx.prevSucceededIndexes, jobCtx.succeededIndexes = calculateSucceededIndexes(logger, &job, pods)
		jobCtx.succeeded = int32(jobCtx.succeededIndexes.total())
		// 判定 backoffLimitPerIndex 相关的失败场景。
		if hasBackoffLimitPerIndex(&job) {
			jobCtx.failedIndexes = calculateFailedIndexes(logger, &job, pods)
			if jobCtx.finishedCondition == nil {
				if job.Spec.MaxFailedIndexes != nil && jobCtx.failedIndexes.total() > int(*job.Spec.MaxFailedIndexes) {
					jobCtx.finishedCondition = jm.newFailureCondition(batch.JobReasonMaxFailedIndexesExceeded, "Job has exceeded the specified maximal number of failed indexes")
				} else if jobCtx.failedIndexes.total() > 0 && jobCtx.failedIndexes.total()+jobCtx.succeededIndexes.total() >= int(*job.Spec.Completions) {
					jobCtx.finishedCondition = jm.newFailureCondition(batch.JobReasonFailedIndexes, "Job has failed indexes")
				}
			}
			jobCtx.podsWithDelayedDeletionPerIndex = getPodsWithDelayedDeletionPerIndex(logger, jobCtx)
		}
		// 判定 SuccessPolicy 是否满足成功条件。
		if jobCtx.finishedCondition == nil {
			if msg, met := matchSuccessPolicy(logger, job.Spec.SuccessPolicy, *job.Spec.Completions, jobCtx.succeededIndexes); met {
				jobCtx.finishedCondition = newCondition(batch.JobSuccessCriteriaMet, v1.ConditionTrue, batch.JobReasonSuccessPolicy, msg, jm.clock.Now())
			}
		}
	}
	suspendCondChanged := false
	// Job 已判定失败时删除所有活跃 Pod。
	if jobCtx.finishedCondition != nil {
		deletedReady, deleted, err := jm.deleteActivePods(ctx, &job, jobCtx.activePods)
		if deleted != active || !satisfiedExpectations {
			// 仍有 Pod 带 finalizer 或未进入 informer 缓存，暂时不能宣布 Job 已结束。
			jobCtx.finishedCondition = nil
		}
		active -= deleted
		*jobCtx.terminating += deleted
		jobCtx.ready -= deletedReady
		manageJobErr = err
	} else {
		manageJobCalled := false
		if satisfiedExpectations && job.DeletionTimestamp == nil {
			active, action, manageJobErr = jm.manageJob(ctx, &job, jobCtx)
			manageJobCalled = true
		}
		// 根据 completions 判定成功。
		complete := false
		if job.Spec.Completions == nil {
			// 未指定 completions 时，任意 Pod 成功即视为完成。
			// 每个 Pod 都能决定整个 Job 是否完成；后续 Pod 的失败会被忽略。
			// 一旦有 Pod 成功，控制器会等待其余 Pod 结束后将 Job 标记为完成。
			complete = jobCtx.succeeded > 0 && active == 0
		} else {
			// 指定了 completions 时，达到该成功数即视为成功。
			// 由于创建 Pod 时不会超过剩余完成数，达到该计数时不应再有活跃 Pod。
			complete = jobCtx.succeeded >= *job.Spec.Completions && active == 0
		}
		if complete {
			jobCtx.finishedCondition = jm.newSuccessCondition()
		} else if manageJobCalled {
			// 只有本轮调用了 manageJob 才更新条件/发事件，否则留给后续的同步轮次处理。
			if jobSuspended(&job) {
				// 只有未完成的 Job 才可能处于挂起状态。
				var isUpdated bool
				job.Status.Conditions, isUpdated = ensureJobConditionStatus(job.Status.Conditions, batch.JobSuspended, v1.ConditionTrue, "JobSuspended", "Job suspended", jm.clock.Now())
				if isUpdated {
					suspendCondChanged = true
					jm.recorder.Event(&job, v1.EventTypeNormal, "Suspended", "Job suspended")
					if feature.DefaultFeatureGate.Enabled(features.MutableSchedulingDirectivesForSuspendedJobs) {
						job.Status.StartTime = nil
					}
				}
			} else {
				// Job 当前未被挂起。
				var isUpdated bool
				job.Status.Conditions, isUpdated = ensureJobConditionStatus(job.Status.Conditions, batch.JobSuspended, v1.ConditionFalse, "JobResumed", "Job resumed", jm.clock.Now())
				if isUpdated {
					suspendCondChanged = true
					jm.recorder.Event(&job, v1.EventTypeNormal, "Resumed", "Job resumed")
					// 恢复时总是把 StartTime 重置为当前时间：ActiveDeadlineSeconds 计时器
					// 不应在 Job 挂起期间流逝，且这样与"恢复一个以挂起状态创建的 Job"保持一致。
					// （ActiveDeadlineSeconds 表示 Job 连续活跃的秒数。）
					now := metav1.NewTime(jm.clock.Now())
					job.Status.StartTime = &now
				}
			}
		}
	}

	needsStatusUpdate := suspendCondChanged || active != job.Status.Active || !ptr.Equal(&jobCtx.ready, job.Status.Ready)
	needsStatusUpdate = needsStatusUpdate || !ptr.Equal(job.Status.Terminating, jobCtx.terminating)
	job.Status.Active = active
	job.Status.Ready = &jobCtx.ready
	job.Status.Terminating = jobCtx.terminating
	err = jm.trackJobStatusAndRemoveFinalizers(ctx, jobCtx, needsStatusUpdate)
	if err != nil {
		return fmt.Errorf("tracking status: %w", err)
	}

	return manageJobErr
}

// newFailureCondition 构造一个 JobFailureTarget 条件，作为失败前的中间状态。
func (jm *Controller) newFailureCondition(reason, message string) *batch.JobCondition {
	return newCondition(batch.JobFailureTarget, v1.ConditionTrue, reason, message, jm.clock.Now())
}

// newSuccessCondition 构造一个 SuccessCriteriaMet 条件，表示已达到预期的成功 Pod 数。
func (jm *Controller) newSuccessCondition() *batch.JobCondition {
	return newCondition(batch.JobSuccessCriteriaMet, v1.ConditionTrue, batch.JobReasonCompletionsReached,
		"Reached expected number of succeeded pods", jm.clock.Now())
}

// deleteActivePods 发起对所有活跃 Pod 的删除，但保留 finalizer。
// 删除通过设置删除时间戳发起；trackJobStatusAndRemoveFinalizers 随后移除 finalizer，
// 之后对象才会真正被删除。
// 返回成功删除的就绪 Pod 数和成功删除的 Pod 总数。
func (jm *Controller) deleteActivePods(ctx context.Context, job *batch.Job, pods []*v1.Pod) (int32, int32, error) {
	errCh := make(chan error, len(pods))
	successfulDeletes := int32(len(pods))
	var deletedReady int32 = 0
	wg := sync.WaitGroup{}
	wg.Add(len(pods))
	for i := range pods {
		go func(pod *v1.Pod) {
			defer wg.Done()
			if err := jm.podControl.DeletePod(ctx, job.Namespace, pod.Name, job); err != nil && !apierrors.IsNotFound(err) {
				atomic.AddInt32(&successfulDeletes, -1)
				errCh <- err
				utilruntime.HandleError(err)
			}
			if podutil.IsPodReady(pod) {
				atomic.AddInt32(&deletedReady, 1)
			}
		}(pods[i])
	}
	wg.Wait()
	return deletedReady, successfulDeletes, errorFromChannel(errCh)
}

// nonIgnoredFailedPodsCount 统计失败 Pod 中未被 podFailurePolicy 忽略的数量。
func nonIgnoredFailedPodsCount(jobCtx *syncJobCtx, failedPods []*v1.Pod) int {
	result := len(failedPods)
	if jobCtx.job.Spec.PodFailurePolicy != nil {
		for _, p := range failedPods {
			_, countFailed, _ := matchPodFailurePolicy(jobCtx.job.Spec.PodFailurePolicy, p)
			if !countFailed {
				result--
			}
		}
	}
	return result
}

// deleteJobPods 删除给定的 Pod，先移除跟踪 finalizer 再发起删除。
// 返回成功删除的就绪 Pod 数、成功删除的 Pod 总数以及遇到的错误。
func (jm *Controller) deleteJobPods(ctx context.Context, job *batch.Job, jobKey string, pods []*v1.Pod) (int32, int32, error) {
	errCh := make(chan error, len(pods))
	successfulDeletes := int32(len(pods))
	var deletedReady int32 = 0
	logger := klog.FromContext(ctx)

	failDelete := func(pod *v1.Pod, err error) {
		// 删除失败时 informer 不会观察到删除事件，因此手动递减预期计数。
		jm.expectations.DeletionObserved(logger, jobKey)
		if !apierrors.IsNotFound(err) {
			logger.V(2).Info("Failed to delete Pod", "job", klog.KObj(job), "pod", klog.KObj(pod), "err", err)
			atomic.AddInt32(&successfulDeletes, -1)
			errCh <- err
			utilruntime.HandleError(err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(pods))
	for i := range pods {
		go func(pod *v1.Pod) {
			defer wg.Done()
			if patch := removeTrackingFinalizerPatch(pod); patch != nil {
				if err := jm.podControl.PatchPod(ctx, pod.Namespace, pod.Name, patch); err != nil {
					failDelete(pod, fmt.Errorf("removing completion finalizer: %w", err))
					return
				}
			}
			if err := jm.podControl.DeletePod(ctx, job.Namespace, pod.Name, job); err != nil {
				failDelete(pod, err)
			}
			if podutil.IsPodReady(pod) {
				atomic.AddInt32(&deletedReady, 1)
			}
		}(pods[i])
	}
	wg.Wait()
	return deletedReady, successfulDeletes, errorFromChannel(errCh)
}

// trackJobStatusAndRemoveFinalizers 依次执行：
//  1. 把已结束的 Pod 记入 .status.uncountedTerminatedPods。
//  2. 在 Pod 完成、被删除或 Job 被删除时，移除其 finalizer。
//  3. 为 finalizer 已移除的 Pod 递增 Job 计数。
//  4. 当前计数满足条件时添加 Complete 条件。
//
// 每轮只处理有限数量的 Pod，避免 .status 过大，也避免单个大 Job 的同步饿死其他 Job。
func (jm *Controller) trackJobStatusAndRemoveFinalizers(ctx context.Context, jobCtx *syncJobCtx, needsFlush bool) error {
	logger := klog.FromContext(ctx)

	isIndexed := isIndexedJob(jobCtx.job)
	var podsToRemoveFinalizer []*v1.Pod
	uncountedStatus := jobCtx.job.Status.UncountedTerminatedPods
	var newSucceededIndexes []int
	if isIndexed {
		// 按完成索引排序，使成功索引按序加入状态，便于区间压缩表示。
		sort.Sort(byCompletionIndex(jobCtx.pods))
	}
	uidsWithFinalizer := make(sets.Set[types.UID], len(jobCtx.pods))
	for _, p := range jobCtx.pods {
		if hasJobTrackingFinalizer(p) && !jobCtx.expectedRmFinalizers.Has(p.UID) {
			uidsWithFinalizer.Insert(p.UID)
		}
	}

	// 浅拷贝即可，这里只用于检测计数器变化。
	oldCounters := jobCtx.job.Status
	if cleanUncountedPodsWithoutFinalizers(&jobCtx.job.Status, uidsWithFinalizer) {
		needsFlush = true
	}
	podFailureCountByPolicyAction := map[string]int{}
	reachedMaxUncountedPods := false
	for _, pod := range jobCtx.pods {
		if !hasJobTrackingFinalizer(pod) || jobCtx.expectedRmFinalizers.Has(pod.UID) {
			// 该 Pod 在之前的同步中已处理过。
			continue
		}
		considerPodFailed := isPodFailed(pod, jobCtx.job)
		if !canRemoveFinalizer(logger, jobCtx, pod, considerPodFailed) {
			continue
		}
		podsToRemoveFinalizer = append(podsToRemoveFinalizer, pod)
		if pod.Status.Phase == v1.PodSucceeded && !jobCtx.uncounted.failed.Has(pod.UID) {
			if isIndexed {
				// 索引化 Job 用完成索引即可避免重复计数，无需跟踪 UID。
				ix := getCompletionIndex(pod.Annotations)
				if ix != unknownCompletionIndex && ix < int(*jobCtx.job.Spec.Completions) && !jobCtx.prevSucceededIndexes.has(ix) {
					newSucceededIndexes = append(newSucceededIndexes, ix)
					needsFlush = true
				}
			} else if !jobCtx.uncounted.succeeded.Has(pod.UID) {
				needsFlush = true
				uncountedStatus.Succeeded = append(uncountedStatus.Succeeded, pod.UID)
			}
		} else if considerPodFailed || (jobCtx.finishedCondition != nil && !isSuccessCriteriaMetCondition(jobCtx.finishedCondition)) {
			// Job 一旦被判定为结束，所有未终结的 Pod 都按失败处理。
			ix := getCompletionIndex(pod.Annotations)
			if !jobCtx.uncounted.failed.Has(pod.UID) && (!isIndexed || (ix != unknownCompletionIndex && ix < int(*jobCtx.job.Spec.Completions))) {
				if jobCtx.job.Spec.PodFailurePolicy != nil {
					_, countFailed, action := matchPodFailurePolicy(jobCtx.job.Spec.PodFailurePolicy, pod)
					if action != nil {
						podFailureCountByPolicyAction[string(*action)] += 1
					}
					if countFailed {
						needsFlush = true
						uncountedStatus.Failed = append(uncountedStatus.Failed, pod.UID)
					}
				} else {
					needsFlush = true
					uncountedStatus.Failed = append(uncountedStatus.Failed, pod.UID)
				}
			}
		}
		if len(newSucceededIndexes)+len(uncountedStatus.Succeeded)+len(uncountedStatus.Failed) >= MaxUncountedPods {
			// .status.uncountedTerminatedPods 已记录足够多的 Pod，这里停止计数和移除 finalizer：
			// 1. 保证 UID 列表序列化后小于 20 KB；
			// 2. 限制单轮移除数量，避免大 Job 的同步饿死小 Job。
			//
			// Job 状态和 Pod 更新会把它重新放回工作队列，后续还会再次同步。
			reachedMaxUncountedPods = true
			break
		}
	}
	if isIndexed {
		jobCtx.succeededIndexes = jobCtx.succeededIndexes.withOrderedIndexes(newSucceededIndexes)
		succeededIndexesStr := jobCtx.succeededIndexes.String()
		if succeededIndexesStr != jobCtx.job.Status.CompletedIndexes {
			needsFlush = true
		}
		jobCtx.job.Status.Succeeded = int32(jobCtx.succeededIndexes.total())
		jobCtx.job.Status.CompletedIndexes = succeededIndexesStr
		var failedIndexesStr *string
		if jobCtx.failedIndexes != nil {
			failedIndexesStr = ptr.To(jobCtx.failedIndexes.String())
		}
		if !ptr.Equal(jobCtx.job.Status.FailedIndexes, failedIndexesStr) {
			jobCtx.job.Status.FailedIndexes = failedIndexesStr
			needsFlush = true
		}
	}
	// 处理 finishedCondition：把 FailureTarget 转换为 Failed。
	if jobCtx.finishedCondition != nil && jobCtx.finishedCondition.Type == batch.JobFailureTarget {

		// 先把临时 FailureTarget 条件写入状态，再移除 finalizer。
		jobCtx.job.Status.Conditions = append(jobCtx.job.Status.Conditions, *jobCtx.finishedCondition)
		needsFlush = true

		// 准备 finalizer 移除后再写入的最终 Failed 条件，enactJobFinished 上报时也使用它。
		jobCtx.finishedCondition = newFailedConditionForFailureTarget(jobCtx.finishedCondition, jm.clock.Now())
	}
	// 处理 finishedCondition：把 SuccessCriteriaMet 转换为 Complete。
	if isSuccessCriteriaMetCondition(jobCtx.finishedCondition) {
		// 先把临时 SuccessCriteriaMet 条件写入状态，再移除 finalizer。
		if hasSuccessCriteriaMetCondition(jobCtx.job) == nil {
			jobCtx.job.Status.Conditions = append(jobCtx.job.Status.Conditions, *jobCtx.finishedCondition)
			needsFlush = true
		}

		// 准备 finalizer 移除后再写入的最终 Complete 条件，enactJobFinished 上报时也使用它。
		jobCtx.finishedCondition = newCondition(batch.JobComplete, v1.ConditionTrue, jobCtx.finishedCondition.Reason, jobCtx.finishedCondition.Message, jm.clock.Now())
	}
	var err error
	if jobCtx.job, needsFlush, err = jm.flushUncountedAndRemoveFinalizers(ctx, jobCtx, podsToRemoveFinalizer, uidsWithFinalizer, &oldCounters, podFailureCountByPolicyAction, needsFlush); err != nil {
		return err
	}
	jobFinished := !reachedMaxUncountedPods && jm.enactJobFinished(logger, jobCtx)
	if jobFinished {
		needsFlush = true
	}
	if needsFlush {
		if _, err := jm.updateStatusHandler(ctx, jobCtx.job); err != nil {
			return fmt.Errorf("removing uncounted pods from status: %w", err)
		}
		if jobFinished {
			jm.recordJobFinished(jobCtx.job, jobCtx.finishedCondition)
			jm.finishedJobExpectations.Store(jobCtx.job.UID, struct{}{})
		}
		recordJobPodFinished(logger, jobCtx.job, oldCounters)
	}
	return nil
}

// canRemoveFinalizer 判断 Pod 的跟踪 finalizer 是否可以安全移除。
// 以下情况允许移除：
//   - 整个 Job 正在终止；
//   - Pod 所在索引已成功；
//   - Pod 被视为失败，但为了把 JobIndexFailureCount 注解交给替代 Pod 而延迟删除的情况除外；
//     Job 整体终止时可以无条件移除；
//   - Job 已满足 successPolicy。
func canRemoveFinalizer(logger klog.Logger, jobCtx *syncJobCtx, pod *v1.Pod, considerPodFailed bool) bool {
	if jobCtx.job.DeletionTimestamp != nil || jobCtx.finishedCondition != nil || pod.Status.Phase == v1.PodSucceeded {
		return true
	}
	if !considerPodFailed {
		return false
	}
	if hasBackoffLimitPerIndex(jobCtx.job) {
		if index := getCompletionIndex(pod.Annotations); index != unknownCompletionIndex {
			if p, ok := jobCtx.podsWithDelayedDeletionPerIndex[index]; ok && p.UID == pod.UID {
				logger.V(3).Info("Delaying pod finalizer removal to await for pod recreation within the index", "pod", klog.KObj(pod))
				return false
			}
		}
	}
	return true
}

// flushUncountedAndRemoveFinalizers 依次执行：
//  1. 提交可能包含新 uncounted Pod UID 的 Job 状态，同时写入临时的 FailureTarget 和
//     SuccessCriteriaMet 条件（如有）。
//  2. 移除 uncounted 列表中 Pod 的 finalizer。
//  3. 根据 finalizer 移除成功的 Pod 更新计数器。
//  4. 若移除未全部成功，再次提交 Job 状态。
//
// 返回是否还有待提交的状态变更，需要后续调用继续处理。
func (jm *Controller) flushUncountedAndRemoveFinalizers(ctx context.Context, jobCtx *syncJobCtx, podsToRemoveFinalizer []*v1.Pod, uidsWithFinalizer sets.Set[types.UID], oldCounters *batch.JobStatus, podFailureCountByPolicyAction map[string]int, needsFlush bool) (*batch.Job, bool, error) {
	logger := klog.FromContext(ctx)
	var err error
	if needsFlush {
		if jobCtx.job, err = jm.updateStatusHandler(ctx, jobCtx.job); err != nil {
			return jobCtx.job, needsFlush, fmt.Errorf("adding uncounted pods to status: %w", err)
		}

		err = jm.podBackoffStore.updateBackoffRecord(jobCtx.newBackoffRecord)

		if err != nil {
			// 该错误可能导致退避计数偏低，且从当前状态重新同步也未必能恢复丢失的退避信息。
			logger.Error(err, "Backoff update failed")
		}

		recordJobPodFinished(logger, jobCtx.job, *oldCounters)
		// 浅拷贝即可，这里只用于检测计数器变化。
		*oldCounters = jobCtx.job.Status
		needsFlush = false
	}
	recordJobPodFailurePolicyActions(podFailureCountByPolicyAction)

	jobKey, err := controller.KeyFunc(jobCtx.job)
	if err != nil {
		return jobCtx.job, needsFlush, fmt.Errorf("getting job key: %w", err)
	}
	var rmErr error
	if len(podsToRemoveFinalizer) > 0 {
		var rmSucceded []bool
		rmSucceded, rmErr = jm.removeTrackingFinalizerFromPods(ctx, jobKey, podsToRemoveFinalizer)
		for i, p := range podsToRemoveFinalizer {
			if rmSucceded[i] {
				uidsWithFinalizer.Delete(p.UID)
			}
		}
	}
	// 部分 finalizer 移除失败，仍尽量把已完成的进度提交到状态中。
	if cleanUncountedPodsWithoutFinalizers(&jobCtx.job.Status, uidsWithFinalizer) {
		needsFlush = true
	}
	if rmErr != nil && needsFlush {
		if job, err := jm.updateStatusHandler(ctx, jobCtx.job); err != nil {
			return job, needsFlush, fmt.Errorf("removing uncounted pods from status: %w", err)
		}
	}
	return jobCtx.job, needsFlush, rmErr
}

// cleanUncountedPodsWithoutFinalizers 从 .status.uncountedTerminatedPods 中移除
// finalizer 已成功删除的 Pod UID，并递增对应的状态计数器。
// 返回状态是否发生变化。
func cleanUncountedPodsWithoutFinalizers(status *batch.JobStatus, uidsWithFinalizer sets.Set[types.UID]) bool {
	updated := false
	uncountedStatus := status.UncountedTerminatedPods
	newUncounted := filterInUncountedUIDs(uncountedStatus.Succeeded, uidsWithFinalizer)
	if len(newUncounted) != len(uncountedStatus.Succeeded) {
		updated = true
		status.Succeeded += int32(len(uncountedStatus.Succeeded) - len(newUncounted))
		uncountedStatus.Succeeded = newUncounted
	}
	newUncounted = filterInUncountedUIDs(uncountedStatus.Failed, uidsWithFinalizer)
	if len(newUncounted) != len(uncountedStatus.Failed) {
		updated = true
		status.Failed += int32(len(uncountedStatus.Failed) - len(newUncounted))
		uncountedStatus.Failed = newUncounted
	}
	return updated
}

// removeTrackingFinalizerFromPods 移除 Pod 上的跟踪 finalizer，
// 返回布尔数组，第 i 项表示第 i 个 Pod 的 finalizer 是否移除成功
// （调用时 Pod 已被删除也视为移除成功）。
func (jm *Controller) removeTrackingFinalizerFromPods(ctx context.Context, jobKey string, pods []*v1.Pod) ([]bool, error) {
	logger := klog.FromContext(ctx)
	errCh := make(chan error, len(pods))
	succeeded := make([]bool, len(pods))
	uids := make([]types.UID, len(pods))
	for i, p := range pods {
		uids[i] = p.UID
	}
	if jobKey != "" {
		err := jm.finalizerExpectations.expectFinalizersRemoved(logger, jobKey, uids)
		if err != nil {
			return succeeded, fmt.Errorf("setting expected removed finalizers: %w", err)
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(len(pods))
	for i := range pods {
		go func(i int) {
			pod := pods[i]
			defer wg.Done()
			if patch := removeTrackingFinalizerPatch(pod); patch != nil {
				if err := jm.podControl.PatchPod(ctx, pod.Namespace, pod.Name, patch); err != nil {
					// 移除失败时 informer 不会收到 finalizer 被移除的更新，需立即清除对应预期。
					if jobKey != "" {
						jm.finalizerExpectations.finalizerRemovalObserved(logger, jobKey, pod.UID)
					}
					if !apierrors.IsNotFound(err) {
						errCh <- err
						utilruntime.HandleError(fmt.Errorf("removing tracking finalizer: %w", err))
						return
					}
				}
				succeeded[i] = true
			}
		}(i)
	}
	wg.Wait()

	return succeeded, errorFromChannel(errCh)
}

// enactJobFinished 在条件满足时写入 Complete 或 Failed 条件。
// 返回 Job 是否已被判定为结束。
func (jm *Controller) enactJobFinished(logger klog.Logger, jobCtx *syncJobCtx) bool {
	if jobCtx.finishedCondition == nil {
		return false
	}
	job := jobCtx.job
	if uncounted := job.Status.UncountedTerminatedPods; uncounted != nil {
		if count := len(uncounted.Succeeded) + len(uncounted.Failed); count > 0 {
			logger.V(4).Info("Delaying marking the Job as finished, because there are still uncounted pod(s)", "job", klog.KObj(job), "condition", jobCtx.finishedCondition.Type, "count", count)
			return false
		}
	}
	if *jobCtx.terminating > 0 {
		logger.V(4).Info("Delaying marking the Job as finished, because there are still terminating pod(s)", "job", klog.KObj(job), "condition", jobCtx.finishedCondition.Type, "count", *jobCtx.terminating)
		return false
	}
	finishedCond := jobCtx.finishedCondition
	job.Status.Conditions, _ = ensureJobConditionStatus(job.Status.Conditions, finishedCond.Type, finishedCond.Status, finishedCond.Reason, finishedCond.Message, jm.clock.Now())
	if finishedCond.Type == batch.JobComplete {
		job.Status.CompletionTime = &finishedCond.LastTransitionTime
	}
	return true
}

// recordJobFinished 为已结束的 Job 记录事件和 job_finished_total 指标。
func (jm *Controller) recordJobFinished(job *batch.Job, finishedCond *batch.JobCondition) bool {
	completionMode := getCompletionMode(job)
	if finishedCond.Type == batch.JobComplete {
		if job.Spec.Completions != nil && job.Status.Succeeded > *job.Spec.Completions {
			jm.recorder.Event(job, v1.EventTypeWarning, "TooManySucceededPods", "Too many succeeded pods running after completion count reached")
		}
		jm.recorder.Event(job, v1.EventTypeNormal, "Completed", "Job completed")
		metrics.JobFinishedNum.WithLabelValues(completionMode, "succeeded", finishedCond.Reason).Inc()
	} else {
		jm.recorder.Event(job, v1.EventTypeWarning, finishedCond.Reason, finishedCond.Message)
		metrics.JobFinishedNum.WithLabelValues(completionMode, "failed", finishedCond.Reason).Inc()
	}
	return true
}

// filterInUncountedUIDs 过滤出仍在 include 集合中的 Pod UID。
func filterInUncountedUIDs(uncounted []types.UID, include sets.Set[types.UID]) []types.UID {
	var newUncounted []types.UID
	for _, uid := range uncounted {
		if include.Has(uid) {
			newUncounted = append(newUncounted, uid)
		}
	}
	return newUncounted
}

// newFailedConditionForFailureTarget 根据临时 FailureTarget 条件生成最终的 Failed 条件。
func newFailedConditionForFailureTarget(condition *batch.JobCondition, now time.Time) *batch.JobCondition {
	return newCondition(batch.JobFailed, v1.ConditionTrue, condition.Reason, condition.Message, now)
}

// pastBackoffLimitOnFailure 检查容器重启次数之和是否超过 BackoffLimit。
// 该方法仅适用于 restartPolicy == OnFailure 的 Pod。
func pastBackoffLimitOnFailure(job *batch.Job, pods []*v1.Pod) bool {
	if job.Spec.Template.Spec.RestartPolicy != v1.RestartPolicyOnFailure {
		return false
	}
	result := int32(0)
	for i := range pods {
		po := pods[i]
		if po.Status.Phase == v1.PodRunning || po.Status.Phase == v1.PodPending {
			for j := range po.Status.InitContainerStatuses {
				stat := po.Status.InitContainerStatuses[j]
				result += stat.RestartCount
			}
			for j := range po.Status.ContainerStatuses {
				stat := po.Status.ContainerStatuses[j]
				result += stat.RestartCount
			}
		}
	}
	if *job.Spec.BackoffLimit == 0 {
		return result > 0
	}
	return result >= *job.Spec.BackoffLimit
}

// pastActiveDeadline 检查 Job 是否设置了 ActiveDeadlineSeconds 且已超时。
// 处于挂起状态的 Job 总是返回 false。
func (jm *Controller) pastActiveDeadline(job *batch.Job) bool {
	if job.Spec.ActiveDeadlineSeconds == nil || job.Status.StartTime == nil || jobSuspended(job) {
		return false
	}
	duration := jm.clock.Since(job.Status.StartTime.Time)
	allowedDuration := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

// newCondition 按给定类型、状态、原因和消息构造一个 Job 条件。
func newCondition(conditionType batch.JobConditionType, status v1.ConditionStatus, reason, message string, now time.Time) *batch.JobCondition {
	return &batch.JobCondition{
		Type:               conditionType,
		Status:             status,
		LastProbeTime:      metav1.NewTime(now),
		LastTransitionTime: metav1.NewTime(now),
		Reason:             reason,
		Message:            message,
	}
}

// getFailJobMessage 根据当前状态判断 Job 是否应失败，并返回对应的失败消息。
func getFailJobMessage(job *batch.Job, pods []*v1.Pod) *string {
	if job.Spec.PodFailurePolicy == nil {
		return nil
	}
	for _, p := range pods {
		if isPodFailed(p, job) {
			jobFailureMessage, _, _ := matchPodFailurePolicy(job.Spec.PodFailurePolicy, p)
			if jobFailureMessage != nil {
				return jobFailureMessage
			}
		}
	}
	return nil
}

// getNewFinishedPods 返回尚未计入 Job 状态的新成功和失败 Pod 列表。
// 失败 Pod 列表可能受 podFailurePolicy 影响。
func getNewFinishedPods(jobCtx *syncJobCtx) (succeededPods, failedPods []*v1.Pod) {
	succeededPods = getValidPodsWithFilter(jobCtx, jobCtx.uncounted.Succeeded(), func(p *v1.Pod) bool {
		return p.Status.Phase == v1.PodSucceeded
	})
	failedPods = getValidPodsWithFilter(jobCtx, jobCtx.uncounted.Failed(), func(p *v1.Pod) bool {
		return isPodFailed(p, jobCtx.job)
	})
	return succeededPods, failedPods
}

// jobSuspended 返回 Job 是否处于挂起状态（已考虑特性门控）。
func jobSuspended(job *batch.Job) bool {
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

// manageJob 是核心方法，负责按 job.Spec 管理运行中的 Pod 数量。
// 遵守退避策略：退避时间未到时不创建新 Pod；不会修改 activePods 切片本身。
func (jm *Controller) manageJob(ctx context.Context, job *batch.Job, jobCtx *syncJobCtx) (int32, string, error) {
	logger := klog.FromContext(ctx)
	active := int32(len(jobCtx.activePods))
	parallelism := *job.Spec.Parallelism
	jobKey, err := controller.KeyFunc(job)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for job %#v: %v", job, err))
		return 0, metrics.JobSyncActionTracking, nil
	}

	if jobSuspended(job) {
		logger.V(4).Info("Deleting all active pods in suspended job", "job", klog.KObj(job), "active", active)
		podsToDelete := activePodsForRemoval(job, jobCtx.activePods, int(active))
		jm.expectations.ExpectDeletions(logger, jobKey, len(podsToDelete))
		removedReady, removed, err := jm.deleteJobPods(ctx, job, jobKey, podsToDelete)
		active -= removed
		*jobCtx.terminating += removed
		jobCtx.ready -= removedReady
		return active, metrics.JobSyncActionPodsDeleted, err
	}

	wantActive := int32(0)
	if job.Spec.Completions == nil {
		// 未指定 completions 时，活跃数应等于 parallelism；
		// 但如果已有 Pod 成功，则保持当前运行数量不变。
		if jobCtx.succeeded > 0 {
			wantActive = active
		} else {
			wantActive = parallelism
		}
	} else {
		// 指定了 completions 时，活跃数不得超过剩余完成数。
		wantActive = *job.Spec.Completions - jobCtx.succeeded
		if wantActive > parallelism {
			wantActive = parallelism
		}
		if wantActive < 0 {
			wantActive = 0
		}
	}

	rmAtLeast := active - wantActive
	if rmAtLeast < 0 {
		rmAtLeast = 0
	}
	podsToDelete := activePodsForRemoval(job, jobCtx.activePods, int(rmAtLeast))
	if len(podsToDelete) > MaxPodCreateDeletePerSync {
		podsToDelete = podsToDelete[:MaxPodCreateDeletePerSync]
	}
	if len(podsToDelete) > 0 {
		jm.expectations.ExpectDeletions(logger, jobKey, len(podsToDelete))
		logger.V(4).Info("Too many pods running for job", "job", klog.KObj(job), "deleted", len(podsToDelete), "target", wantActive)
		removedReady, removed, err := jm.deleteJobPods(ctx, job, jobKey, podsToDelete)
		active -= removed
		*jobCtx.terminating += removed
		jobCtx.ready -= removedReady
		// Job 可能同时需要创建和删除 Pod（例如有重复索引的索引化 Job），
		// 但每轮同步只做其中一种；两者同时需要时优先删除。
		return active, metrics.JobSyncActionPodsDeleted, err
	}

	var terminating int32 = 0
	if onlyReplaceFailedPods(jobCtx.job) {
		terminating = *jobCtx.terminating
	}
	if diff := wantActive - terminating - active; diff > 0 {
		var remainingTime time.Duration
		if !hasBackoffLimitPerIndex(job) {
			// 未使用 backoffLimitPerIndex 时，按全局退避记录计算剩余等待时间。
			remainingTime = jobCtx.newBackoffRecord.getRemainingTime(jm.clock, DefaultJobPodFailureBackOff, MaxJobPodFailureBackOff)
		}
		if remainingTime > 0 {
			jm.enqueueSyncJobWithDelay(logger, job, remainingTime)
			// 没有创建或删除 Pod，应返回当前活跃数而不是 0。返回 0 会让状态更新
			// 把 Active 置为 0 而 Ready 仍反映运行中的 Pod，API Server 会拒绝
			// （"ready pods 不能多于 active"），进而阻塞 finalizer 移除和状态提交。
			return active, metrics.JobSyncActionPodsCreated, nil
		}
		if diff > int32(MaxPodCreateDeletePerSync) {
			diff = int32(MaxPodCreateDeletePerSync)
		}

		var indexesToAdd []int
		if isIndexedJob(job) {
			indexesToAdd = firstPendingIndexes(jobCtx, int(diff), int(*job.Spec.Completions))
			if hasBackoffLimitPerIndex(job) {
				indexesToAdd, remainingTime = jm.getPodCreationInfoForIndependentIndexes(logger, indexesToAdd, jobCtx.podsWithDelayedDeletionPerIndex)
				if remainingTime > 0 {
					jm.enqueueSyncJobWithDelay(logger, job, remainingTime)
					// 没有创建或删除 Pod，返回当前活跃数而不是 0（原因见上方注释）。
					return active, metrics.JobSyncActionPodsCreated, nil
				}
			}
			diff = int32(len(indexesToAdd))
		}

		jm.expectations.ExpectCreations(logger, jobKey, int(diff))
		errCh := make(chan error, diff)
		logger.V(4).Info("Too few pods running", "key", jobKey, "need", wantActive, "creating", diff)

		wait := sync.WaitGroup{}

		active += diff

		podTemplate := job.Spec.Template.DeepCopy()
		if isIndexedJob(job) {
			addCompletionIndexEnvVariables(podTemplate)
		}
		podTemplate.Finalizers = appendJobCompletionFinalizerIfNotFound(podTemplate.Finalizers)

		if jobCtx.podGroup != nil {
			pg := jobCtx.podGroup
			podTemplate.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
				PodGroupName: &pg.Name,
			}
			podTemplate.OwnerReferences = append(podTemplate.OwnerReferences,
				// PodGroup 使用非控制器 ownerRef（控制器 ownerRef 指向 Job，由
				// CreatePodsWithGenerateName 设置）。BlockOwnerDeletion 有意留空（false），
				// 避免残留 Pod 阻塞 PodGroup 删除；Job 的级联 GC 已能保证正确的清理顺序。
				metav1.OwnerReference{
					APIVersion: schedulingv1beta1.SchemeGroupVersion.String(),
					Kind:       "PodGroup",
					Name:       pg.Name,
					UID:        pg.UID,
				})
		}

		// Pod 创建成败计数器，用于 job_pods_creation_total 指标。
		var creationsSucceeded, creationsFailed int32 = 0, 0

		// 分批创建 Pod：批次大小从 SlowStartInitialBatchSize 开始，每轮成功后翻倍，
		// 形成"慢启动"。这样可避免一次性创建大量 Pod 时全部以相同错误失败——例如配额较低
		// 的项目在第一个 Pod 失败后就停止向 API Server 刷创建请求，同时也避免了失败事件刷屏。
		for batchSize := min(diff, int32(controller.SlowStartInitialBatchSize)); diff > 0; batchSize = min(2*batchSize, diff) {
			errorCount := len(errCh)
			wait.Add(int(batchSize))
			for i := int32(0); i < batchSize; i++ {
				completionIndex := unknownCompletionIndex
				if len(indexesToAdd) > 0 {
					completionIndex = indexesToAdd[0]
					indexesToAdd = indexesToAdd[1:]
				}
				go func() {
					template := podTemplate
					generateName := ""
					if completionIndex != unknownCompletionIndex {
						template = podTemplate.DeepCopy()
						addCompletionIndexAnnotation(template, completionIndex)
						addCompletionIndexLabel(template, completionIndex)
						template.Spec.Hostname = fmt.Sprintf("%s-%d", job.Name, completionIndex)
						generateName = podGenerateNameWithIndex(job.Name, completionIndex)
						if hasBackoffLimitPerIndex(job) {
							addIndexFailureCountAnnotation(logger, template, job, jobCtx.podsWithDelayedDeletionPerIndex[completionIndex])
						}
					}
					defer wait.Done()
					err := jm.podControl.CreatePodsWithGenerateName(ctx, job.Namespace, template, job, metav1.NewControllerRef(job, controllerKind), generateName)
					if err != nil {
						if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
							// 命名空间正在销毁时后续创建必然失败，可以忽略该错误。
							return
						}
					}
					if err != nil {
						defer utilruntime.HandleError(err)
						// 创建失败时 informer 不会观察到该 Pod，需递减创建预期计数。
						logger.V(2).Info("Failed creation, decrementing expectations", "job", klog.KObj(job))
						jm.expectations.CreationObserved(logger, jobKey)
						atomic.AddInt32(&active, -1)
						errCh <- err
						atomic.AddInt32(&creationsFailed, 1)
					}
					atomic.AddInt32(&creationsSucceeded, 1)
				}()
			}
			wait.Wait()
			// 慢启动失败时未尝试创建的 Pod 不应计入预期。
			skippedPods := diff - batchSize
			if errorCount < len(errCh) && skippedPods > 0 {
				logger.V(2).Info("Slow-start failure. Skipping creating pods, decrementing expectations", "skippedCount", skippedPods, "job", klog.KObj(job))
				active -= skippedPods
				for i := int32(0); i < skippedPods; i++ {
					// 这些 Pod 未真正创建，informer 不会观察到，需递减创建预期计数。
					jm.expectations.CreationObserved(logger, jobKey)
				}
				// 跳过的 Pod 留待下次同步重试慢启动流程。
				break
			}
			diff -= batchSize
		}
		recordJobPodsCreationTotal(job, jobCtx, creationsSucceeded, creationsFailed)
		return active, metrics.JobSyncActionPodsCreated, errorFromChannel(errCh)
	}

	return active, metrics.JobSyncActionTracking, nil
}

// getPodCreationInfoForIndependentIndexes 从待创建索引中筛出当前即可创建的子集；
// 若没有可创建的索引，则返回所有索引中最短的剩余等待时间。
func (jm *Controller) getPodCreationInfoForIndependentIndexes(logger klog.Logger, indexesToAdd []int, podsWithDelayedDeletionPerIndex map[int]*v1.Pod) ([]int, time.Duration) {
	var indexesToAddNow []int
	var minRemainingTimePerIndex *time.Duration
	for _, indexToAdd := range indexesToAdd {
		if remainingTimePerIndex := getRemainingTimePerIndex(logger, jm.clock, DefaultJobPodFailureBackOff, MaxJobPodFailureBackOff, podsWithDelayedDeletionPerIndex[indexToAdd]); remainingTimePerIndex == 0 {
			indexesToAddNow = append(indexesToAddNow, indexToAdd)
		} else if minRemainingTimePerIndex == nil || remainingTimePerIndex < *minRemainingTimePerIndex {
			minRemainingTimePerIndex = &remainingTimePerIndex
		}
	}
	if len(indexesToAddNow) > 0 {
		return indexesToAddNow, 0
	}
	return indexesToAddNow, ptr.Deref(minRemainingTimePerIndex, 0)
}

// activePodsForRemoval 返回应删除的 Pod：活跃 Pod 过多，或索引化 Job 中存在重复/
// 无效索引以及无索引的 Pod。
// 候选 Pod 按"未就绪 < 已就绪、未调度 < 已调度、Pending < Running"排序，
// 保证优先删除处于较早生命周期阶段的 Pod。
func activePodsForRemoval(job *batch.Job, pods []*v1.Pod, rmAtLeast int) []*v1.Pod {
	var rm, left []*v1.Pod

	if isIndexedJob(job) {
		rm = make([]*v1.Pod, 0, rmAtLeast)
		left = make([]*v1.Pod, 0, len(pods)-rmAtLeast)
		rm, left = appendDuplicatedIndexPodsForRemoval(rm, left, pods, int(*job.Spec.Completions))
	} else {
		left = pods
	}

	if len(rm) < rmAtLeast {
		sort.Sort(controller.ActivePods(left))
		rm = append(rm, left[:rmAtLeast-len(rm)]...)
	}
	return rm
}

// updateJobStatus 调用 API 提交 Job 状态，并记录写入的资源版本以保证一致性。
func (jm *Controller) updateJobStatus(ctx context.Context, job *batch.Job) (*batch.Job, error) {
	job, err := jm.kubeClient.BatchV1().Jobs(job.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	jm.consistencyStore.WroteAt(
		types.NamespacedName{Name: job.Name, Namespace: job.Namespace},
		job.UID,
		jobGroupResource,
		job.ResourceVersion,
	)
	return job, err
}

// getValidPodsWithFilter 返回通过过滤条件的有效 Pod。
// 有效 Pod 需带跟踪 finalizer 或位于 uncounted 集合中；索引化 Job 还要求有有效的完成索引。
func getValidPodsWithFilter(jobCtx *syncJobCtx, uncounted sets.Set[types.UID], filter func(*v1.Pod) bool) []*v1.Pod {
	var result []*v1.Pod
	for _, p := range jobCtx.pods {
		// 没有完成 finalizer 的 Pod 要么已在 uncounted 集合中，要么已计入状态，跳过。
		if !hasJobTrackingFinalizer(p) || uncounted.Has(p.UID) || jobCtx.expectedRmFinalizers.Has(p.UID) {
			continue
		}
		if isIndexedJob(jobCtx.job) {
			idx := getCompletionIndex(p.Annotations)
			if idx == unknownCompletionIndex || idx >= int(*jobCtx.job.Spec.Completions) {
				continue
			}
		}
		if filter(p) {
			result = append(result, p)
		}
	}
	return result
}

// getCompletionMode 返回完成模式的字符串表示，用作指标标签值。
func getCompletionMode(job *batch.Job) string {
	if isIndexedJob(job) {
		return string(batch.IndexedCompletion)
	}
	return string(batch.NonIndexedCompletion)
}

// appendJobCompletionFinalizerIfNotFound 在 finalizer 列表中不存在时追加 Job 跟踪 finalizer。
func appendJobCompletionFinalizerIfNotFound(finalizers []string) []string {
	if slices.Contains(finalizers, batch.JobTrackingFinalizer) {
		return finalizers
	}
	return append(finalizers, batch.JobTrackingFinalizer)
}

// removeTrackingFinalizerPatch 生成删除 Job 跟踪 finalizer 的 JSON Patch；Pod 无该 finalizer 时返回 nil。
func removeTrackingFinalizerPatch(pod *v1.Pod) []byte {
	if !hasJobTrackingFinalizer(pod) {
		return nil
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"$deleteFromPrimitiveList/finalizers": []string{batch.JobTrackingFinalizer},
		},
	}
	patchBytes, _ := json.Marshal(patch)
	return patchBytes
}

// uncountedTerminatedPods 保存尚未计入 Job 状态的已结束 Pod UID 集合。
type uncountedTerminatedPods struct {
	succeeded sets.Set[types.UID]
	failed    sets.Set[types.UID]
}

// newUncountedTerminatedPods 从 API 状态构造 uncountedTerminatedPods。
func newUncountedTerminatedPods(in batch.UncountedTerminatedPods) *uncountedTerminatedPods {
	return &uncountedTerminatedPods{
		succeeded: sets.New(in.Succeeded...),
		failed:    sets.New(in.Failed...),
	}
}

// Succeeded 返回未计数的成功 Pod UID 集合，接收者为 nil 时返回 nil。
func (u *uncountedTerminatedPods) Succeeded() sets.Set[types.UID] {
	if u == nil {
		return nil
	}
	return u.succeeded
}

// Failed 返回未计数的失败 Pod UID 集合，接收者为 nil 时返回 nil。
func (u *uncountedTerminatedPods) Failed() sets.Set[types.UID] {
	if u == nil {
		return nil
	}
	return u.failed
}

// errorFromChannel 非阻塞地取出错误通道中的第一个错误，没有错误则返回 nil。
func errorFromChannel(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}

// ensureJobConditionStatus 追加或更新指定类型的 Job 条件。
// 注意：新条件状态为 false 时不会追加（从无到 false 没有意义），但可以把已有条件更新为 false。
// 返回的布尔值表示条件列表是否发生了追加或更新。
func ensureJobConditionStatus(list []batch.JobCondition, cType batch.JobConditionType, status v1.ConditionStatus, reason, message string, now time.Time) ([]batch.JobCondition, bool) {
	if condition := findConditionByType(list, cType); condition != nil {
		if condition.Status != status || condition.Reason != reason || condition.Message != message {
			*condition = *newCondition(cType, status, reason, message, now)
			return list, true
		}
		return list, false
	}
	// 列表中不存在该类型的条件。
	if status != v1.ConditionFalse {
		return append(list, *newCondition(cType, status, reason, message, now)), true
	}
	return list, false
}

// isPodFailed 判断 Pod 是否应按失败处理，包括被删除但从未进入 Failed 阶段的孤立 Pod。
func isPodFailed(p *v1.Pod, job *batch.Job) bool {
	if p.Status.Phase == v1.PodFailed {
		return true
	}
	if onlyReplaceFailedPods(job) {
		return false
	}
	// 把正在删除的 Pod 计为失败，以覆盖那些从未有机会进入 Failed 阶段的孤立 Pod。
	return p.DeletionTimestamp != nil && p.Status.Phase != v1.PodSucceeded
}

// findConditionByType 返回列表中指定类型的条件，不存在则返回 nil。
func findConditionByType(list []batch.JobCondition, cType batch.JobConditionType) *batch.JobCondition {
	for i := range list {
		if list[i].Type == cType {
			return &list[i]
		}
	}
	return nil
}

// recordJobPodFinished 根据本轮与上一轮计数器的差值更新 Pod 完成数指标。
func recordJobPodFinished(logger klog.Logger, job *batch.Job, oldCounters batch.JobStatus) {
	completionMode := completionModeStr(job)
	var diff int

	// 索引化 Job 的成功指标需特殊处理：completions 和 parallelism 同时缩容后，
	// 之前已完成的索引可能超出范围（index >= spec.Completions），不能直接相减。
	if isIndexedJob(job) {
		completions := int(*job.Spec.Completions)
		if job.Status.CompletedIndexes != oldCounters.CompletedIndexes {
			diff = indexesCount(logger, &job.Status.CompletedIndexes, completions) - indexesCount(logger, &oldCounters.CompletedIndexes, completions)
		}
		backoffLimitLabel := backoffLimitMetricsLabel(job)
		metrics.JobFinishedIndexesTotal.WithLabelValues(metrics.Succeeded, backoffLimitLabel).Add(float64(diff))
		if hasBackoffLimitPerIndex(job) && job.Status.FailedIndexes != oldCounters.FailedIndexes {
			if failedDiff := indexesCount(logger, job.Status.FailedIndexes, completions) - indexesCount(logger, oldCounters.FailedIndexes, completions); failedDiff > 0 {
				metrics.JobFinishedIndexesTotal.WithLabelValues(metrics.Failed, backoffLimitLabel).Add(float64(failedDiff))
			}
		}
	} else {
		diff = int(job.Status.Succeeded) - int(oldCounters.Succeeded)
	}
	metrics.JobPodsFinished.WithLabelValues(completionMode, metrics.Succeeded).Add(float64(diff))

	// 更新失败数指标。
	diff = int(job.Status.Failed - oldCounters.Failed)
	metrics.JobPodsFinished.WithLabelValues(completionMode, metrics.Failed).Add(float64(diff))
}

// indexesCount 统计索引字符串中小于 completions 的索引数量。
func indexesCount(logger klog.Logger, indexesStr *string, completions int) int {
	if indexesStr == nil {
		return 0
	}
	return parseIndexesFromString(logger, *indexesStr, completions).total()
}

// backoffLimitMetricsLabel 返回指标中区分全局/按索引退避的标签值。
func backoffLimitMetricsLabel(job *batch.Job) string {
	if hasBackoffLimitPerIndex(job) {
		return "perIndex"
	}
	return "global"
}

// recordJobPodFailurePolicyActions 按策略动作累加 Pod 失败处理数指标。
func recordJobPodFailurePolicyActions(podFailureCountByPolicyAction map[string]int) {
	for action, count := range podFailureCountByPolicyAction {
		metrics.PodFailuresHandledByFailurePolicy.WithLabelValues(action).Add(float64(count))
	}
}

// countReadyPods 统计处于 Ready 状态的 Pod 数量。
func countReadyPods(pods []*v1.Pod) int32 {
	cnt := int32(0)
	for _, p := range pods {
		if podutil.IsPodReady(p) {
			cnt++
		}
	}
	return cnt
}

// onlyReplaceFailedPods 判断是否按 PodReplacementPolicy=Failed 执行替换。
// 该策略决定 Pod 被标记为 terminating 后何时重建：Failed 表示必须等 Pod 真正终止才重建。
func onlyReplaceFailedPods(job *batch.Job) bool {
	return job.Spec.PodReplacementPolicy != nil && *job.Spec.PodReplacementPolicy == batch.Failed
}

// recordJobPodsCreationTotal 按创建原因（新建/重建）和结果累加 Pod 创建数指标。
func recordJobPodsCreationTotal(job *batch.Job, jobCtx *syncJobCtx, succeeded, failed int32) {
	reason := metrics.PodCreateNew
	if ptr.Deref(job.Spec.PodReplacementPolicy, batch.TerminatingOrFailed) == batch.Failed && jobCtx.failed > 0 {
		reason = metrics.PodRecreateFailed
	} else if jobCtx.failed > 0 || ptr.Deref(jobCtx.terminating, 0) > 0 {
		reason = metrics.PodRecreateTerminatingOrFailed
	}
	if succeeded > 0 {
		metrics.JobPodsCreationTotal.WithLabelValues(reason, metrics.Succeeded).Add(float64(succeeded))
	}
	if failed > 0 {
		metrics.JobPodsCreationTotal.WithLabelValues(reason, metrics.Failed).Add(float64(failed))
	}
}

// managedByExternalController 返回管理该 Job 的外部控制器名称；由本控制器或无人管理时返回 nil。
func managedByExternalController(jobObj *batch.Job) *string {
	if feature.DefaultFeatureGate.Enabled(features.JobManagedBy) {
		if controllerName := jobObj.Spec.ManagedBy; controllerName != nil && *controllerName != batch.JobControllerName {
			return controllerName
		}
	}
	return nil
}
