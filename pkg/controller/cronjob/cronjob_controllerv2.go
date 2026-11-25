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

package cronjob

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchv1informers "k8s.io/client-go/informers/batch/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	batchv1listers "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/cronjob/metrics"
	jobutil "k8s.io/kubernetes/pkg/controller/job/util"
	"k8s.io/kubernetes/pkg/util/parsers"
	"k8s.io/utils/ptr"
)

const (
	jobControllerUIDIndex = "jobControllerUID"
)

var (
	// controllerKind 保存该控制器类型对应的 schema.GroupVersionKind。
	controllerKind = batchv1.SchemeGroupVersion.WithKind("CronJob")

	nextScheduleDelta = 100 * time.Millisecond
)

// ControllerV2 是 CronJob 的控制器。
// 这是重构后的 CronJob 控制器，使用延迟队列（DelayingQueue）和 informer 实现。
type ControllerV2 struct {
	// queue 是需要同步的 CronJob 键的限速工作队列。
	queue workqueue.TypedRateLimitingInterface[string]

	// kubeClient 用于访问 API Server，recorder 与 broadcaster 负责事件记录与广播。
	kubeClient  clientset.Interface
	recorder    record.EventRecorder
	broadcaster record.EventBroadcaster

	// jobControl 与 cronJobControl 封装对 Job 和 CronJob 的实际操作，便于测试时注入。
	jobControl     jobControlInterface
	cronJobControl cjControlInterface

	// cronJobLister 提供从 informer 缓存中读取 CronJob 的能力。
	cronJobLister batchv1listers.CronJobLister

	// jobIndexer 支持按 ControllerRef 的 UID 查找 Job。
	jobIndexer cache.Indexer

	// jobListerSynced 与 cronJobListerSynced 表示对应 informer 是否至少完成过一次同步。
	jobListerSynced     cache.InformerSynced
	cronJobListerSynced cache.InformerSynced

	// now 返回当前时间；抽成函数是为了在单元测试中注入可控的时间。
	now func() time.Time
}

// NewControllerV2 创建并初始化一个新的 ControllerV2。
// 它注册 Job 与 CronJob 的事件处理器，并建立按控制器 UID 查询 Job 的索引。
func NewControllerV2(ctx context.Context, jobInformer batchv1informers.JobInformer, cronJobsInformer batchv1informers.CronJobInformer, kubeClient clientset.Interface) (*ControllerV2, error) {
	logger := klog.FromContext(ctx)
	// 统一的事件广播器，供控制器向 API Server 记录事件。
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))

	jm := &ControllerV2{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "cronjob",
			},
		),
		kubeClient:  kubeClient,
		broadcaster: eventBroadcaster,
		recorder:    eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "cronjob-controller"}),

		jobControl:     realJobControl{KubeClient: kubeClient},
		cronJobControl: &realCJControl{KubeClient: kubeClient},

		cronJobLister: cronJobsInformer.Lister(),

		jobIndexer: jobInformer.Informer().GetIndexer(),

		jobListerSynced:     jobInformer.Informer().HasSynced,
		cronJobListerSynced: cronJobsInformer.Informer().HasSynced,

		now: time.Now,
	}

	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jm.addJob,
		UpdateFunc: jm.updateJob,
		DeleteFunc: jm.deleteJob,
	})

	cronJobsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			jm.enqueueController(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			jm.updateCronJob(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			jm.enqueueController(obj)
		},
	})

	// 为 Job 建立按所属控制器 UID 的索引，避免每次同步时遍历全部 Job。
	err := jobInformer.Informer().AddIndexers(cache.Indexers{
		jobControllerUIDIndex: func(obj interface{}) ([]string, error) {
			job, ok := obj.(*batchv1.Job)
			if !ok {
				return nil, nil
			}
			if controllerRef := metav1.GetControllerOf(job); controllerRef != nil {
				return []string{string(controllerRef.UID)}, nil
			}
			return nil, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("adding job controller UID indexer: %w", err)
	}

	metrics.Register()

	return jm, nil
}

// Run 启动主 goroutine，负责监听并同步 CronJob。
// 它会等待缓存同步后启动 workers 个 worker，直到 ctx 结束才返回。
func (jm *ControllerV2) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线：结构化日志与写入 API Server 的事件接收端。
	jm.broadcaster.StartStructuredLogging(3)
	jm.broadcaster.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: jm.kubeClient.CoreV1().Events("")})
	defer jm.broadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting cronjob controller v2")

	var wg sync.WaitGroup
	// 关闭时先关闭队列，再等待所有 worker 退出，避免遗留处理中的任务。
	defer func() {
		logger.Info("Shutting down cronjob controller v2")
		jm.queue.ShutDown()
		wg.Wait()
	}()

	// 必须等 informer 缓存同步完成后再启动 worker，否则读到的数据不完整。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, jm.jobListerSynced, jm.cronJobListerSynced) {
		return
	}

	// 启动多个 worker 并发处理队列；workqueue 保证同一个键不会被并发处理。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, jm.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// worker 持续从队列中取出条目进行处理，直到队列关闭。
func (jm *ControllerV2) worker(ctx context.Context) {
	for jm.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 CronJob 键，并根据同步结果决定重试或延迟重入队。
// 返回 false 表示队列已关闭，worker 应退出。
func (jm *ControllerV2) processNextWorkItem(ctx context.Context) bool {
	key, quit := jm.queue.Get()
	if quit {
		return false
	}
	// Done 必须在所有返回路径上调用，否则队列中的条目会永远处于处理中状态。
	defer jm.queue.Done(key)

	requeueAfter, err := jm.sync(ctx, key)
	switch {
	case err != nil:
		// 同步出错时按限速退避重试，避免频繁冲击 API Server。
		utilruntime.HandleError(fmt.Errorf("error syncing CronJobController %v, requeuing: %w", key, err))
		jm.queue.AddRateLimited(key)
	case requeueAfter != nil:
		// 下次调度时间已知，用延迟队列在该时间点精确重入队。
		jm.queue.Forget(key)
		jm.queue.AddAfter(key, *requeueAfter)
	default:
		// 无需重入队，清除该键的失败计数。
		jm.queue.Forget(key)
	}
	return true
}

// sync 对一个 CronJob 执行一次完整协调：清理已完成 Job、同步调度状态并按需更新 status。
// 返回的 requeueAfter 非空时，表示应在指定时长后再次同步。
func (jm *ControllerV2) sync(ctx context.Context, cronJobKey string) (*time.Duration, error) {
	ns, name, err := cache.SplitMetaNamespaceKey(cronJobKey)
	if err != nil {
		return nil, err
	}
	logger := klog.FromContext(ctx)
	cronJob, err := jm.cronJobLister.CronJobs(ns).Get(name)
	switch {
	case errors.IsNotFound(err):
		// CronJob 很可能已被删除，无需再为该键安排重试。
		logger.V(4).Info("CronJob not found, may be it is deleted", "cronjob", klog.KObj(cronJob), "err", err)
		return nil, nil
	case err != nil:
		// 其他暂时性 API Server 错误交由调用方按指数退避重试。
		return nil, err
	}

	jobsToBeReconciled, err := jm.getCronJobJobsByIndexer(cronJob)
	if err != nil {
		return nil, err
	}

	// 在副本上累积所有修改，最后只向 API Server 提交一次 status 更新。
	// 这样既能合并清理与同步两处改动，也避免污染 informer 缓存中的对象。
	cronJobCopy := cronJob.DeepCopy()

	updateStatusAfterCleanup := jm.cleanupFinishedJobs(ctx, cronJobCopy, jobsToBeReconciled)

	requeueAfter, updateStatusAfterSync, syncErr := jm.syncCronJob(ctx, cronJobCopy, jobsToBeReconciled)
	if syncErr != nil {
		logger.V(2).Info("Error reconciling cronjob", "cronjob", klog.KObj(cronJob), "err", syncErr)
	}

	// 清理和调度任一环节产生了状态变更，就提交一次 status 更新。
	if updateStatusAfterCleanup || updateStatusAfterSync {
		if _, err := jm.cronJobControl.UpdateStatus(ctx, cronJobCopy); err != nil {
			logger.V(2).Info("Unable to update status for cronjob", "cronjob", klog.KObj(cronJob), "resourceVersion", cronJob.ResourceVersion, "err", err)
			return nil, err
		}
	}

	if requeueAfter != nil {
		logger.V(4).Info("Re-queuing cronjob", "cronjob", klog.KObj(cronJob), "requeueAfter", requeueAfter)
		return requeueAfter, nil
	}
	// 走到这里表示无需重入队：通常发生在 CronJob 被暂停或 schedule 格式非法时。
	return nil, syncErr
}

// getCronJobJobsByIndexer 通过 ControllerRef UID 索引列出某个 CronJob 创建的所有 Job。
// 相比遍历整个 Job 缓存，索引能显著减少同步时的查找开销。
func (jm *ControllerV2) getCronJobJobsByIndexer(cronJob *batchv1.CronJob) ([]*batchv1.Job, error) {
	var jobsForCronJob []*batchv1.Job
	jobs, err := jm.jobIndexer.ByIndex(jobControllerUIDIndex, string(cronJob.UID))
	if err != nil {
		return nil, err
	}
	// 索引中理论上只有 Job；若出现其他类型则记录错误并跳过，避免污染后续协调。
	for _, obj := range jobs {
		job, ok := obj.(*batchv1.Job)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type in job indexer: %v", obj))
			continue
		}
		jobsForCronJob = append(jobsForCronJob, job)
	}
	return jobsForCronJob, nil
}

// resolveControllerRef 解析 ControllerRef 指向的 CronJob。
// 若类型不匹配、找不到对象或 UID 不一致，则返回 nil。
func (jm *ControllerV2) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *batchv1.CronJob {
	// informer 缓存不支持按 UID 查询，因此先按名称查找，再校验 UID 是否一致。
	// 如果 Kind 都不对，连按名称查找都没必要。
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	cronJob, err := jm.cronJobLister.CronJobs(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if cronJob.UID != controllerRef.UID {
		// 同名对象已被重建，UID 不同说明它并非 ControllerRef 指向的那个控制器。
		return nil
	}
	return cronJob
}

// addJob 在 Job 创建时把管理它的 CronJob 加入队列。
func (jm *ControllerV2) addJob(obj interface{}) {
	job := obj.(*batchv1.Job)
	if job.DeletionTimestamp != nil {
		// 控制器重启后，首次看到的 Job 可能已处于删除中状态。
		// 此时应视作删除而非创建，避免把待删除对象误当作新 Job。
		jm.deleteJob(job)
		return
	}

	// 只关心有 ControllerRef 的 Job：它明确归属某个 CronJob。
	if controllerRef := metav1.GetControllerOf(job); controllerRef != nil {
		cronJob := jm.resolveControllerRef(job.Namespace, controllerRef)
		if cronJob == nil {
			return
		}
		jm.enqueueController(cronJob)
		return
	}
}

// updateJob 在 Job 更新时找出管理它的 CronJob 并唤醒它们。
// 如果 ControllerRef 发生了变化，新旧两个 CronJob 都需要同步。
// old 和 cur 必须都是 *batchv1.Job 类型。
func (jm *ControllerV2) updateJob(old, cur interface{}) {
	curJob := cur.(*batchv1.Job)
	oldJob := old.(*batchv1.Job)
	if curJob.ResourceVersion == oldJob.ResourceVersion {
		// 周期性 resync 会为所有 Job 产生更新事件，但内容并未变化。
		// 同一 Job 的不同版本 ResourceVersion 必然不同，据此即可过滤掉假更新。
		return
	}

	curControllerRef := metav1.GetControllerOf(curJob)
	oldControllerRef := metav1.GetControllerOf(oldJob)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// ControllerRef 已改变，需要同步旧控制器以清理其状态。
		if cronJob := jm.resolveControllerRef(oldJob.Namespace, oldControllerRef); cronJob != nil {
			jm.enqueueController(cronJob)
		}
	}

	// 当前 ControllerRef 指向的 CronJob 同样需要同步。
	if curControllerRef != nil {
		cronJob := jm.resolveControllerRef(curJob.Namespace, curControllerRef)
		if cronJob == nil {
			return
		}
		jm.enqueueController(cronJob)
		return
	}
}

// deleteJob 在 Job 删除时把管理它的 CronJob 加入队列。
func (jm *ControllerV2) deleteJob(obj interface{}) {
	job, ok := obj.(*batchv1.Job)

	// 删除事件可能丢失：relist 会发现缓存中的对象已不在 API 列表中，
	// 从而插入一个 tombstone 对象。注意其中的对象信息可能已经过期。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		job, ok = tombstone.Obj.(*batchv1.Job)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Job %#v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(job)
	if controllerRef == nil {
		// 孤立对象没有所属控制器，没人需要关心它的删除。
		return
	}
	cronJob := jm.resolveControllerRef(job.Namespace, controllerRef)
	if cronJob == nil {
		return
	}
	jm.enqueueController(cronJob)
}

// enqueueController 把对象对应的命名空间/名称键立即加入工作队列。
func (jm *ControllerV2) enqueueController(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}

	jm.queue.Add(key)
}

// enqueueControllerAfter 把对象对应的键延迟 t 后加入工作队列。
// 用于 schedule 变更时按新的下次调度时间重新排队。
func (jm *ControllerV2) enqueueControllerAfter(obj interface{}, t time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}

	jm.queue.AddAfter(key, t)
}

// updateCronJob 在 spec.schedule 或时区变化时按新的下次调度时间重新排队；
// 其他字段变化则立即重新排队。
func (jm *ControllerV2) updateCronJob(logger klog.Logger, old interface{}, curr interface{}) {
	oldCJ, okOld := old.(*batchv1.CronJob)
	newCJ, okNew := curr.(*batchv1.CronJob)

	if !okOld || !okNew {
		// 类型断言失败属于异常情况，直接返回；此处无需处理正常业务对象。
		return
	}
	// 若新 schedule 使下次调度更早，队列会按新时间提前处理；
	// 若更晚，则已排队键的同步基本是空操作，下一次调度时再按新时间处理。
	if oldCJ.Spec.Schedule != newCJ.Spec.Schedule || !ptr.Equal(oldCJ.Spec.TimeZone, newCJ.Spec.TimeZone) {
		// schedule 变了：重新计算下次调度时间。这里传 nil recorder，避免与 syncCronJob 重复告警。
		sched, err := parsers.ParseCronScheduleWithPanicRecovery(formatSchedule(newCJ, nil))
		if err != nil {
			// schedule 解析失败通常是用户配置错误：记录日志和事件，且在 spec 更新前不再协调该 CronJob。
			logger.V(2).Info("Unparseable schedule for cronjob", "cronjob", klog.KObj(newCJ), "schedule", newCJ.Spec.Schedule, "err", err)
			jm.recorder.Eventf(newCJ, corev1.EventTypeWarning, "UnParseableCronJobSchedule", "unparseable schedule for cronjob: %s", newCJ.Spec.Schedule)
			return
		}
		now := jm.now()
		t := nextScheduleTimeDuration(newCJ, now, sched)

		jm.enqueueControllerAfter(curr, *t)
		return
	}

	// 其他参数变更则立即入队：若尚在截止时间内，同步循环会处理；
	// 否则变更会在下一次调度时生效。
	// TODO: 需要显式处理 spec.JobTemplate.metadata.labels 的变化，以清理带有旧标签的 Job。
	jm.enqueueController(curr)
}

// syncCronJob 根据 CronJob 创建的 Job 列表对其进行协调。
// jobs 应包含该 CronJob 已知的所有 Job；当前时间由 jm.now() 提供，便于测试。
// 第二个返回值表示是否需要向 API Server 更新 status。
func (jm *ControllerV2) syncCronJob(
	ctx context.Context,
	cronJob *batchv1.CronJob,
	jobs []*batchv1.Job) (*time.Duration, bool, error) {

	now := jm.now()
	updateStatus := false

	// 先记录当前实际存在的子 Job 集合，后续据此清理 status.active 中的陈旧引用。
	childrenJobs := make(map[types.UID]bool)
	for _, j := range jobs {
		childrenJobs[j.ObjectMeta.UID] = true
		found := inActiveList(cronJob, j.ObjectMeta.UID)
		// 未完成却不在 active 列表中的 Job：可能是控制器重启前创建、状态未更新所致。
		if !found && !jobutil.IsJobFinished(j) {
			// 缓存中的 CronJob 状态可能滞后：向 API Server 再取一次确认，避免误报 UnexpectedJob。
			cjCopy, err := jm.cronJobControl.GetCronJob(ctx, cronJob.Namespace, cronJob.Name)
			if err != nil {
				return nil, updateStatus, err
			}
			if inActiveList(cjCopy, j.ObjectMeta.UID) {
				cronJob = cjCopy
				continue
			}
			jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnexpectedJob", "Saw a job that the controller did not create or forgot: %s", j.Name)
			// 发现一个归属自己但未出现在 active 列表的未完成 Job。
			// 可能是创建 Job 后、更新状态前控制器崩溃；也可能是 relist 后 Job 列表比 CronJob 状态新；
			// 还可能有人故意创建了希望被本控制器接纳的 Job。
		} else if jobutil.IsJobFinished(j) {
			// 已完成的 Job 若仍在 active 列表中，需要移除并记录事件。
			if found {
				_, condition := jobutil.FinishedCondition(j)
				deleteFromActiveList(cronJob, j.ObjectMeta.UID)
				jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "SawCompletedJob", "Saw completed job: %s, condition: %v", j.Name, condition)
				updateStatus = true
			}
			if jobutil.IsJobSucceeded(j) {
				// 成功的 Job 即使不在 active 列表中，也要据此更新 LastSuccessfulTime。
				if cronJob.Status.LastSuccessfulTime == nil {
					cronJob.Status.LastSuccessfulTime = j.Status.CompletionTime
					updateStatus = true
				}
				if j.Status.CompletionTime != nil && j.Status.CompletionTime.After(cronJob.Status.LastSuccessfulTime.Time) {
					cronJob.Status.LastSuccessfulTime = j.Status.CompletionTime
					updateStatus = true
				}
			}

		}
	}

	// 清理 active 列表中已不存在的 Job 引用，否则 CronJob 会一直停留在 active 状态。
	for _, j := range cronJob.Status.Active {
		_, found := childrenJobs[j.UID]
		if found {
			continue
		}
		// watch 可能有延迟，lister 中的缺失并不代表 Job 真被删除。
		// 因此再向 API Server 确认一次，避免误判。
		_, err := jm.jobControl.GetJob(j.Namespace, j.Name)
		switch {
		case errors.IsNotFound(err):
			// Job 确实已不存在，从 active 列表移除；若仍在截止时间内则会调度新 Job。
			jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "MissingJob", "Active job went missing: %v", j.Name)
			deleteFromActiveList(cronJob, j.UID)
			updateStatus = true
		case err != nil:
			return nil, updateStatus, err
		}
		// Job 只是未及时出现在 lister 中，但 API Server 确认存在，无需处理。
	}

	if cronJob.DeletionTimestamp != nil {
		// CronJob 正在删除：除了更新 status 外不做任何调度操作。
		return nil, updateStatus, nil
	}

	logger := klog.FromContext(ctx)
	// 时区非法时直接跳过调度，避免按错误时区触发 Job。
	if cronJob.Spec.TimeZone != nil {
		timeZone := ptr.Deref(cronJob.Spec.TimeZone, "")
		if _, err := time.LoadLocation(timeZone); err != nil {
			logger.V(4).Info("Not starting job because timeZone is invalid", "cronjob", klog.KObj(cronJob), "timeZone", timeZone, "err", err)
			jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnknownTimeZone", "invalid timeZone: %q: %s", timeZone, err)
			return nil, updateStatus, nil
		}
	}

	// 被暂停的 CronJob 不创建新 Job，但仍会继续同步已有 Job 的状态。
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		logger.V(4).Info("Not starting job because the cron is suspended", "cronjob", klog.KObj(cronJob))
		return nil, updateStatus, nil
	}

	sched, err := parsers.ParseCronScheduleWithPanicRecovery(formatSchedule(cronJob, jm.recorder))
	if err != nil {
		// schedule 解析失败通常是用户配置错误：记录日志和事件，且在 spec 更新前不再协调。
		logger.V(2).Info("Unparseable schedule", "cronjob", klog.KObj(cronJob), "schedule", cronJob.Spec.Schedule, "err", err)
		jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "UnparseableSchedule", "unparseable schedule: %q : %s", cronJob.Spec.Schedule, err)
		return nil, updateStatus, nil
	}

	scheduledTime, err := nextScheduleTime(logger, cronJob, now, sched, jm.recorder)
	if err != nil {
		// 调度时间计算失败通常也是配置问题，记录后停止协调直到 spec 被修正。
		logger.V(2).Info("Invalid schedule", "cronjob", klog.KObj(cronJob), "schedule", cronJob.Spec.Schedule, "err", err)
		jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "InvalidSchedule", "invalid schedule: %s : %s", cronJob.Spec.Schedule, err)
		return nil, updateStatus, nil
	}
	if scheduledTime == nil {
		// 没有未满足的调度时间：通常只在控制器重启后队列被填满时发生。
		// 正常情况下队列会在调度时刻触发同步，从而至少有一个待处理的时间点。
		logger.V(4).Info("No unmet start times", "cronjob", klog.KObj(cronJob))
		t := nextScheduleTimeDuration(cronJob, now, sched)
		return t, updateStatus, nil
	}

	// 超过 startingDeadlineSeconds 的调度点视为错过，不再补跑。
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = scheduledTime.Add(time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds)).Before(now)
	}
	if tooLate {
		logger.V(4).Info("Missed starting window", "cronjob", klog.KObj(cronJob))
		jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "MissSchedule", "Missed scheduled time to start a job: %s", scheduledTime.UTC().Format(time.RFC1123Z))

		// TODO: 不调度时不会设置 LastScheduleTime，因此每个周期都会重复发现漏跑。
		// 为避免重复发送事件和反复处理该 CronJob，可以在发现漏跑时设置 Status.LastMissedTime。
		// 随后调用 getRecentUnmetScheduleTimes 时，取 creationTimestamp、Status.LastScheduleTime
		// 和 Status.LastMissedTime 的最大值，这样下次处理就不会再次生成事件，用户也能从状态中
		// 直接看出曾有一次执行被漏掉。
		t := nextScheduleTimeDuration(cronJob, now, sched)
		return t, updateStatus, nil
	}
	// 用调度时间哈希生成的 Job 名做幂等锁，防止同一调度点被重复创建。
	if inActiveListByName(cronJob, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getJobName(cronJob, *scheduledTime),
			Namespace: cronJob.Namespace,
		}}) || cronJob.Status.LastScheduleTime.Equal(&metav1.Time{Time: *scheduledTime}) {
		logger.V(4).Info("Not starting job because the scheduled time is already processed", "cronjob", klog.KObj(cronJob), "schedule", scheduledTime)
		t := nextScheduleTimeDuration(cronJob, now, sched)
		return t, updateStatus, nil
	}
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(cronJob.Status.Active) > 0 {
		// 无论 active Job 集合来自何处，都可能漏掉正在运行的 Job
		//（例如尚未观察到状态更新或新建的 Pod），因此 Forbid 理论上仍可能并发。
		// 只要两次执行在时间上相隔足够远，通常不会触发这个问题。
		//
		// TODO: 对于 Forbid，可以每次执行都使用同一个名称作为锁。
		// 对于 Replace，可以按执行时间生成确定性的名称。
		// 但这样一来，就无法再查看 Forbid Job 过去的成功或失败记录。
		logger.V(4).Info("Not starting job because prior execution is still running and concurrency policy is Forbid", "cronjob", klog.KObj(cronJob))
		jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "JobAlreadyActive", "Not starting job because prior execution is running and concurrency policy is Forbid")
		t := nextScheduleTimeDuration(cronJob, now, sched)
		return t, updateStatus, nil
	}
	// Replace 策略下，先删除所有仍在运行的旧 Job，再创建新 Job。
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, j := range cronJob.Status.Active {
			logger.V(4).Info("Deleting job that was still running at next scheduled start time", "job", klog.KRef(j.Namespace, j.Name))
			job, err := jm.jobControl.GetJob(j.Namespace, j.Name)
			if err != nil {
				jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "FailedGet", "Get job: %v", err)
				return nil, updateStatus, err
			}
			if !deleteJob(logger, cronJob, job, jm.jobControl, jm.recorder) {
				return nil, updateStatus, fmt.Errorf("could not replace job %s/%s", job.Namespace, job.Name)
			}
			updateStatus = true
		}
	}

	jobAlreadyExists := false
	jobReq, err := getJobFromTemplate2(cronJob, *scheduledTime)
	if err != nil {
		logger.Error(err, "Unable to make Job from template", "cronjob", klog.KObj(cronJob))
		return nil, updateStatus, err
	}
	jobResp, err := jm.jobControl.CreateJob(cronJob.Namespace, jobReq)
	switch {
	case errors.HasStatusCause(err, corev1.NamespaceTerminatingCause):
		// 命名空间正在终止，任何创建都会失败，直接返回。
		return nil, updateStatus, err
	case errors.IsAlreadyExists(err):
		// 若 Job 由外部创建，假定对方已更新 CronJob 状态。
		// 若确属本控制器创建，说明之前创建成功但更新 active 列表失败，
		// 此时需要重新把该 Job 加入 active 列表并更新状态。
		jobAlreadyExists = true
		job, err := jm.jobControl.GetJob(cronJob.Namespace, jobReq.GetName())
		if err != nil {
			return nil, updateStatus, err
		}
		jobResp = job

		// 确认 Job 确实归属本控制器，否则假定由外部控制器更新状态，不做处理。
		if !metav1.IsControlledBy(job, cronJob) {
			return nil, updateStatus, nil
		}

		// 再次确认 Job 不在 active 列表中，避免重复追加。
		found := inActiveList(cronJob, job.ObjectMeta.UID)
		if found {
			return nil, updateStatus, nil
		}
	case err != nil:
		// 其他创建错误按通用失败处理。
		jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, "FailedCreate", "Error creating job: %v", err)
		return nil, updateStatus, err
	}

	if jobAlreadyExists {
		logger.Info("Job already exists", "cronjob", klog.KObj(cronJob), "job", klog.KObj(jobReq))
	} else {
		metrics.CronJobCreationSkew.Observe(jobResp.ObjectMeta.GetCreationTimestamp().Sub(*scheduledTime).Seconds())
		logger.V(4).Info("Created Job", "job", klog.KObj(jobResp), "cronjob", klog.KObj(cronJob))
		jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, "SuccessfulCreate", "Created job %v", jobResp.Name)
	}

	// ------------------------------------------------------------------ //

	// 如果进程恰好在创建 Job 之后、更新状态之前重启，下一次同步可能再次创建同一个 Job。
	// 因此用包含调度时间哈希的 Job 名作为锁，确保同一调度点只会创建一个 Job。

	// 把刚启动的 Job 记录到 active 列表，并更新 LastScheduleTime。
	jobRef, err := getRef(jobResp)
	if err != nil {
		logger.V(2).Info("Unable to make object reference", "cronjob", klog.KObj(cronJob), "err", err)
		return nil, updateStatus, fmt.Errorf("unable to make object reference for job for %s", klog.KObj(cronJob))
	}
	cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	cronJob.Status.LastScheduleTime = &metav1.Time{Time: *scheduledTime}
	updateStatus = true

	t := nextScheduleTimeDuration(cronJob, now, sched)
	return t, updateStatus, nil
}

// getJobName 根据 CronJob 名称和调度时间（按分钟哈希）生成 Job 名称。
// 该名称在同一调度点上保持稳定，可作为防止重复创建的幂等锁。
func getJobName(cj *batchv1.CronJob, scheduledTime time.Time) string {
	return fmt.Sprintf("%s-%d", cj.Name, getTimeHashInMinutes(scheduledTime))
}

// cleanupFinishedJobs 清理某个 CronJob 创建的、已完成的 Job。
// 返回值表示是否需要向 API Server 更新 status。
func (jm *ControllerV2) cleanupFinishedJobs(ctx context.Context, cj *batchv1.CronJob, js []*batchv1.Job) bool {
	// 两个历史记录上限都未设置时，无需清理，直接返回。
	if cj.Spec.FailedJobsHistoryLimit == nil && cj.Spec.SuccessfulJobsHistoryLimit == nil {
		return false
	}

	updateStatus := false
	failedJobs := []*batchv1.Job{}
	successfulJobs := []*batchv1.Job{}

	// 按成功/失败状态把已完成的 Job 分开，分别应用对应的历史记录上限。
	for _, job := range js {
		isFinished, finishedStatus := jm.getFinishedStatus(job)
		if isFinished && finishedStatus == batchv1.JobComplete {
			successfulJobs = append(successfulJobs, job)
		} else if isFinished && finishedStatus == batchv1.JobFailed {
			failedJobs = append(failedJobs, job)
		}
	}

	if cj.Spec.SuccessfulJobsHistoryLimit != nil &&
		jm.removeOldestJobs(ctx, cj,
			successfulJobs,
			*cj.Spec.SuccessfulJobsHistoryLimit) {
		updateStatus = true
	}

	if cj.Spec.FailedJobsHistoryLimit != nil &&
		jm.removeOldestJobs(ctx, cj,
			failedJobs,
			*cj.Spec.FailedJobsHistoryLimit) {
		updateStatus = true
	}

	return updateStatus
}

// getFinishedStatus 检查 Job 是否已完成，并返回其完成状态类型（Complete 或 Failed）。
func (jm *ControllerV2) getFinishedStatus(j *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// removeOldestJobs 从 Job 列表中删除最旧的若干个 Job，使数量不超过 maxJobs。
// 返回值表示是否有 Job 被成功删除并需要更新状态。
func (jm *ControllerV2) removeOldestJobs(ctx context.Context, cj *batchv1.CronJob, js []*batchv1.Job, maxJobs int32) bool {
	updateStatus := false
	numToDelete := len(js) - int(maxJobs)
	if numToDelete <= 0 {
		return updateStatus
	}
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Cleaning up jobs from CronJob list", "deletejobnum", numToDelete, "jobnum", len(js), "cronjob", klog.KObj(cj))

	// 按开始时间排序，优先删除最早的 Job。
	sort.Sort(byJobStartTime(js))
	for i := 0; i < numToDelete; i++ {
		logger.V(4).Info("Removing job from CronJob list", "job", js[i].Name, "cronjob", klog.KObj(cj))
		if deleteJob(logger, cj, js[i], jm.jobControl, jm.recorder) {
			updateStatus = true
		}
	}
	return updateStatus
}

// deleteJob 回收一个 Job：删除 Job 对象本身，并清除其在 active 列表中的引用。
// Pod 的清理由 Job 控制器负责，这里不需要处理。
func deleteJob(logger klog.Logger, cj *batchv1.CronJob, job *batchv1.Job, jc jobControlInterface, recorder record.EventRecorder) bool {
	// 先删除 Job 对象；失败则返回 false，表示状态不应更新。
	if err := jc.DeleteJob(job.Namespace, job.Name); err != nil {
		recorder.Eventf(cj, corev1.EventTypeWarning, "FailedDelete", "Deleted job: %v", err)
		logger.Error(err, "Error deleting job from cronjob", "job", klog.KObj(job), "cronjob", klog.KObj(cj))
		return false
	}
	// Job 删除成功后，再移除其在 CronJob active 列表中的引用。
	deleteFromActiveList(cj, job.ObjectMeta.UID)
	recorder.Eventf(cj, corev1.EventTypeNormal, "SuccessfulDelete", "Deleted job %v", job.Name)

	return true
}

// getRef 从运行时对象生成可用于 status 的 ObjectReference。
func getRef(object runtime.Object) (*corev1.ObjectReference, error) {
	return ref.GetReference(scheme.Scheme, object)
}

// formatSchedule 将 CronJob 的时区设置转换为 cron 解析器可识别的调度表达式。
// 若 schedule 中直接包含 TZ/CRON_TZ，则发出不支持告警并原样返回。
func formatSchedule(cj *batchv1.CronJob, recorder record.EventRecorder) string {
	if strings.Contains(cj.Spec.Schedule, "TZ") {
		if recorder != nil {
			recorder.Eventf(cj, corev1.EventTypeWarning, "UnsupportedSchedule", "CRON_TZ or TZ used in schedule %q is not officially supported, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details", cj.Spec.Schedule)
		}

		return cj.Spec.Schedule
	}

	// 时区必须能被系统识别，否则回退到原始 schedule，避免解析器因无效时区报错。
	if cj.Spec.TimeZone != nil {
		if _, err := time.LoadLocation(*cj.Spec.TimeZone); err != nil {
			return cj.Spec.Schedule
		}

		// 以 TZ= 前缀把时区信息传给底层 cron 解析器。
		return fmt.Sprintf("TZ=%s %s", *cj.Spec.TimeZone, cj.Spec.Schedule)
	}

	return cj.Spec.Schedule
}
