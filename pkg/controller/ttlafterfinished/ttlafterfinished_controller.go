/*
Copyright 2018 The Kubernetes Authors.

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

package ttlafterfinished

import (
	"context"
	"fmt"
	"sync"
	"time"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	jobutil "k8s.io/kubernetes/pkg/controller/job/util"
	"k8s.io/kubernetes/pkg/controller/ttlafterfinished/metrics"
	"k8s.io/utils/clock"
)

// Controller 监听 Job API 对象的变更。当 Job 被创建或更新时，
// 若其设置了 `.spec.ttlSecondsAfterFinished`，就将其放入 `queue`。
// Controller 的 worker 消费 `queue` 并检查 TTL 是否到期：未到期则按预期到期时间
// 延迟重新入队；已到期则向 API Server 发起删除请求。
// 该逻辑独立于 Job 控制器实现，既出于关注点分离，
// 也因为后续会扩展以支持其他可结束的资源类型。
type Controller struct {
	client   clientset.Interface
	recorder record.EventRecorder

	// jLister 可从共享 informer 的存储中 list/get Job。
	jLister batchlisters.JobLister

	// jListerSynced 在 Job 存储至少同步过一次后返回 true。
	// 将其作为结构体成员，便于测试时注入。
	jListerSynced cache.InformerSynced

	// 存放待检查 TTL 的 Job，到期后会尝试删除。
	queue workqueue.TypedRateLimitingInterface[string]

	// 用于计时与判断到期的时钟。
	clock clock.Clock
}

// New 创建 Controller 实例。
func New(ctx context.Context, jobInformer batchinformers.JobInformer, client clientset.Interface) *Controller {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(3)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})

	metrics.Register()

	tc := &Controller{
		client:   client,
		recorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "ttl-after-finished-controller"}),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ttl_jobs_to_delete"},
		),
	}

	logger := klog.FromContext(ctx)
	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			tc.addJob(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			tc.updateJob(logger, oldObj, newObj)
		},
	})

	tc.jLister = jobInformer.Lister()
	tc.jListerSynced = jobInformer.Informer().HasSynced

	tc.clock = clock.RealClock{}

	return tc
}

// Run 启动用于清理 Job 的 worker。
func (tc *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting TTL after finished controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down TTL after finished controller")
		// 先关闭队列，再等待 worker 退出，确保没有任务在关闭后继续处理。
		tc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, tc.jListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, tc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// addJob 处理 Job 的新增事件：只对未进入删除流程且需要清理的 Job 入队。
func (tc *Controller) addJob(logger klog.Logger, obj interface{}) {
	job := obj.(*batch.Job)
	logger.V(4).Info("Adding job", "job", klog.KObj(job))

	if job.DeletionTimestamp == nil && needsCleanup(job) {
		tc.enqueue(logger, job)
	}

}

// updateJob 处理 Job 的更新事件：状态变化可能使 Job 变为已完成，
// 因此同样检查是否需要清理并入队。
func (tc *Controller) updateJob(logger klog.Logger, old, cur interface{}) {
	job := cur.(*batch.Job)
	logger.V(4).Info("Updating job", "job", klog.KObj(job))

	if job.DeletionTimestamp == nil && needsCleanup(job) {
		tc.enqueue(logger, job)
	}
}

// enqueue 将 Job 的命名空间键加入工作队列，供 worker 后续处理。
func (tc *Controller) enqueue(logger klog.Logger, job *batch.Job) {
	logger.V(4).Info("Add job to cleanup", "job", klog.KObj(job))
	key, err := controller.KeyFunc(job)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to get key for job", "job", klog.KObj(job))
		return
	}

	tc.queue.Add(key)
}

// enqueueAfter 在指定延迟后将 Job 重新入队，避免为未到期的 Job 反复轮询。
func (tc *Controller) enqueueAfter(logger klog.Logger, job *batch.Job, after time.Duration) {
	key, err := controller.KeyFunc(job)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Failed to get key for job", "job", klog.KObj(job))
		return
	}

	tc.queue.AddAfter(key, after)
}

// worker 持续处理工作队列，直到队列关闭。
func (tc *Controller) worker(ctx context.Context) {
	for tc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出并处理一个工作项；返回 false 表示队列已关闭，应停止 worker。
// 无论成功与否都会调用 Done，并通过 handleErr 决定重试还是 Forget。
func (tc *Controller) processNextWorkItem(ctx context.Context) bool {
	key, quit := tc.queue.Get()
	if quit {
		return false
	}
	defer tc.queue.Done(key)

	err := tc.processJob(ctx, key)
	tc.handleErr(ctx, err, key)

	return true
}

// handleErr 根据处理结果更新队列状态：成功则 Forget，失败则限速重试。
func (tc *Controller) handleErr(ctx context.Context, err error, key string) {
	if err == nil {
		tc.queue.Forget(key)
		return
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Failed to clean up job, will retry", "key", key)
	tc.queue.AddRateLimited(key)
}

// processJob 检查 Job 的状态与 TTL：当 Job 已完成且 TTL 到期时将其删除；
// 若尚未完成或 TTL 未到期，则按预期到期时间重新入队。
// 同一 key 不应并发调用该函数。
func (tc *Controller) processJob(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	// 从 lister 读取缓存中的 Job；已删除或无需清理的 Job 会在后续判断中忽略。
	job, err := tc.jLister.Jobs(namespace).Get(name)

	logger := klog.FromContext(ctx)
	logger.V(4).Info("Checking if Job is ready for cleanup", "job", klog.KRef(namespace, name))

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	if expiredAt, err := tc.processTTL(logger, job); err != nil {
		return err
	} else if expiredAt == nil {
		return nil
	}

	// 缓存中的 TTL 可能已过期，删除前再次从 API Server 读取最新 Job 做最终校验。
	// 若校验前 TTL 被修改，就无法确认是否真的到期；此时以最新对象为准重新判断。
	// 最新 Job 的 UID 可能不同，但无妨——后续事件会让检查再次执行。
	fresh, err := tc.client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// 用最新 Job 的 TTL 再判一次，避免依据陈旧缓存误删。
	expiredAt, err := tc.processTTL(logger, fresh)
	if err != nil {
		return err
	} else if expiredAt == nil {
		return nil
	}
	// 确认到期后以前台级联方式删除 Job，并带上 UID 前置条件防止误删重建的同名对象。
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{
		PropagationPolicy: &policy,
		Preconditions:     &metav1.Preconditions{UID: &fresh.UID},
	}
	logger.V(4).Info("Cleaning up Job", "job", klog.KObj(fresh))
	if err := tc.client.BatchV1().Jobs(fresh.Namespace).Delete(ctx, fresh.Name, options); err != nil {
		return err
	}
	metrics.JobDeletionDurationSeconds.Observe(time.Since(*expiredAt).Seconds())
	return nil
}

// processTTL 检查 Job 的 TTL 是否到期；若尚未到期，
// 则按预期到期时间延迟重新入队。
func (tc *Controller) processTTL(logger klog.Logger, job *batch.Job) (expiredAt *time.Time, err error) {

	// 正在删除或无需清理的 Job 直接忽略。
	if job.DeletionTimestamp != nil || !needsCleanup(job) {
		return nil, nil
	}

	now := tc.clock.Now()
	t, e, err := timeLeft(logger, job, &now)
	if err != nil {
		return nil, err
	}

	// TTL 已到期，返回到期时刻供删除和指标统计使用。
	if *t <= 0 {
		return e, nil
	}

	tc.enqueueAfter(logger, job, *t)
	return nil, nil
}

// needsCleanup 判断 Job 是否已完成且设置了 TTL。
func needsCleanup(j *batch.Job) bool {
	return j.Spec.TTLSecondsAfterFinished != nil && jobutil.IsJobFinished(j)
}

// getFinishAndExpireTime 计算已完成 Job 的结束时刻与 TTL 到期时刻。
func getFinishAndExpireTime(j *batch.Job) (*time.Time, *time.Time, error) {
	if !needsCleanup(j) {
		return nil, nil, fmt.Errorf("job %s/%s should not be cleaned up", j.Namespace, j.Name)
	}
	t, err := jobFinishTime(j)
	if err != nil {
		return nil, nil, err
	}
	finishAt := t.Time
	expireAt := finishAt.Add(time.Duration(*j.Spec.TTLSecondsAfterFinished) * time.Second)
	return &finishAt, &expireAt, nil
}

// timeLeft 计算从 since 到 TTL 到期的剩余时长，并返回到期时刻。
// 若 Job 的结束时间晚于 since，说明集群可能存在时钟偏差，此时记录警告并延后清理。
func timeLeft(logger klog.Logger, j *batch.Job, since *time.Time) (*time.Duration, *time.Time, error) {
	finishAt, expireAt, err := getFinishAndExpireTime(j)
	if err != nil {
		return nil, nil, err
	}

	if finishAt.After(*since) {
		logger.Info("Warning: Found Job finished in the future. This is likely due to time skew in the cluster. Job cleanup will be deferred.", "job", klog.KObj(j))
	}
	remaining := expireAt.Sub(*since)
	logger.V(4).Info("Found Job finished", "job", klog.KObj(j), "finishTime", finishAt.UTC(), "remainingTTL", remaining, "startTime", since.UTC(), "deadlineTTL", expireAt.UTC())
	return &remaining, expireAt, nil
}

// jobFinishTime 从已完成 Job 的状态条件中提取其结束时刻。
func jobFinishTime(finishedJob *batch.Job) (metav1.Time, error) {
	for _, c := range finishedJob.Status.Conditions {
		if (c.Type == batch.JobComplete || c.Type == batch.JobFailed) && c.Status == v1.ConditionTrue {
			finishAt := c.LastTransitionTime
			if finishAt.IsZero() {
				return metav1.Time{}, fmt.Errorf("unable to find the time when the Job %s/%s finished", finishedJob.Namespace, finishedJob.Name)
			}
			return c.LastTransitionTime, nil
		}
	}

	// 若 Job 确实已完成，理论上不会走到这里。
	return metav1.Time{}, fmt.Errorf("unable to find the status of the finished Job %s/%s", finishedJob.Namespace, finishedJob.Name)
}
