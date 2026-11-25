/*
Copyright 2014 The Kubernetes Authors.

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

package resourcequota

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/client-go/discovery"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
)

// NamespacedResourcesFunc 用于发现命名空间级别的资源。
type NamespacedResourcesFunc func() ([]*metav1.APIResourceList, error)

// ReplenishmentFunc 是一个信号，表示指定命名空间中的某个资源发生变化，
// 可能需要重新计算配额。
type ReplenishmentFunc func(ctx context.Context, groupResource schema.GroupResource, namespace string)

// ControllerOptions 保存创建配额控制器所需的选项。
type ControllerOptions struct {
	// 必须有列出所有配额并更新配额状态的权限。
	QuotaClient corev1client.ResourceQuotasGetter
	// ResourceQuota 的共享 informer。
	ResourceQuotaInformer coreinformers.ResourceQuotaInformer
	// 控制配额使用量的完整重新计算周期。
	ResyncPeriod controller.ResyncPeriodFunc
	// 维护了解如何计算各 group resource 使用量的评估器。
	Registry quota.Registry
	// 发现服务器上支持的资源列表。
	DiscoveryFunc NamespacedResourcesFunc
	// 返回需要忽略的资源列表的函数。
	IgnoredResourcesFunc func() map[schema.GroupResource]struct{}
	// 用于判断 informer 是否已启动。
	InformersStarted <-chan struct{}
	// 与 informer 交互的接口。
	InformerFactory informerfactory.InformerFactory
	// 控制对被监控对象进行完整 resync 以触发配额补充的周期。
	ReplenishmentResyncPeriod controller.ResyncPeriodFunc
	// 过滤更新事件，只将已知会引起配额变化的更新入队。
	UpdateFilter UpdateFilter
}

// Controller 负责跟踪系统中的配额使用状态。
type Controller struct {
	// 必须有列出系统中所有资源并更新配额状态的权限。
	rqClient corev1client.ResourceQuotasGetter
	// ResourceQuota 对象的 lister/getter。
	rqLister corelisters.ResourceQuotaLister
	// 一组函数，各自返回其缓存是否已完成同步。
	informerSyncedFuncs []cache.InformerSynced
	// 需要同步的 ResourceQuota 对象队列。
	queue workqueue.TypedRateLimitingInterface[string]
	// missingUsageQueue 保存缺少初始使用量信息的对象，优先处理。
	missingUsageQueue workqueue.TypedRateLimitingInterface[string]
	// 允许在测试中注入 syncUsage 处理函数。
	syncHandler func(ctx context.Context, key string) error
	// 控制配额使用量的完整重新计算周期。
	resyncPeriod controller.ResyncPeriodFunc
	// 了解如何计算使用量。
	registry quota.Registry
	// 监控配额跟踪的所有资源并触发配额补充。
	quotaMonitor *QuotaMonitor
	// 控制处理配额的 worker。
	// 获取该写锁可控制对 monitors 的写访问，并确保在所有 monitor 完成同步之前
	// 控制器不会处理配额。
	workerLock sync.RWMutex
}

// NewController 根据指定选项创建配额控制器。
func NewController(ctx context.Context, options *ControllerOptions) (*Controller, error) {
	// 构造资源配额控制器，两个队列分别处理常规重算和缺失使用量的优先重算。
	rq := &Controller{
		rqClient:            options.QuotaClient,
		rqLister:            options.ResourceQuotaInformer.Lister(),
		informerSyncedFuncs: []cache.InformerSynced{options.ResourceQuotaInformer.Informer().HasSynced},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "resourcequota_primary"},
		),
		missingUsageQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "resourcequota_priority"},
		),
		resyncPeriod: options.ResyncPeriod,
		registry:     options.Registry,
	}
	// 设置同步处理函数，worker 出队后统一走这里。
	rq.syncHandler = rq.syncResourceQuotaFromKey

	logger := klog.FromContext(ctx)

	options.ResourceQuotaInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				rq.addQuota(logger, obj)
			},
			UpdateFunc: func(old, cur interface{}) {
				// 这里只关心 quota.spec 的变化，用它来驱动 quota.status 的更新。
				// quota.Status 的更新都由本控制器产生，因此忽略，避免自我触发循环。
				// 重要：
				// 不要在此处触发完整的配额重算。那样需要把所有 quota.Status 更新都入队，
				// 而状态更新涉及无法由缓存支撑的额外查询（会完整查询命名空间内容），
				// 对于无意义的状态更新不值得付出这个代价。因此完整 resync 时的入队
				// 交由单独的 enqueueAll 协程负责。
				oldResourceQuota := old.(*v1.ResourceQuota)
				curResourceQuota := cur.(*v1.ResourceQuota)
				if quota.Equals(oldResourceQuota.Spec.Hard, curResourceQuota.Spec.Hard) {
					return
				}
				rq.addQuota(logger, curResourceQuota)
			},
			// 删除事件会进入同步循环但为 no-op，因为对象已从 store 中移除。
			// 注意：缩容到 0 后立即删除控制器不会生效；推荐做法是对控制器执行 stop 操作。
			DeleteFunc: func(obj interface{}) {
				rq.enqueueResourceQuota(logger, obj)
			},
		},
		rq.resyncPeriod(),
	)

	if options.DiscoveryFunc != nil {
		qm := NewMonitor(
			options.InformersStarted,
			options.InformerFactory,
			options.IgnoredResourcesFunc(),
			options.ReplenishmentResyncPeriod,
			rq.replenishQuota,
			rq.registry,
			options.UpdateFilter,
		)

		rq.quotaMonitor = qm

		// 做一次初始的配额 monitor 设置。此处发现失败也无妨，后续同步时还会重新发现资源。
		resources, err := GetQuotableResources(options.DiscoveryFunc)
		if discovery.IsGroupDiscoveryFailedError(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "Initial discovery check failure, continuing and counting on future sync update")
		} else if err != nil {
			return nil, err
		}

		if err = qm.SyncMonitors(ctx, resources); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Initial monitor sync has error")
		}

		// 只有所有 informer 完成同步后才启动配额处理。
		rq.informerSyncedFuncs = append(rq.informerSyncedFuncs, func() bool {
			return qm.IsSynced(ctx)
		})
	}

	return rq, nil
}

// enqueueAll 在 fullResyncPeriod 周期被调用，强制对所有配额使用量统计做完整重算。
func (rq *Controller) enqueueAll(ctx context.Context) {
	logger := klog.FromContext(ctx)
	defer logger.V(4).Info("Resource quota controller queued all resource quota for full calculation of usage")
	rqs, err := rq.rqLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to enqueue all - error listing resource quotas")
		return
	}
	for i := range rqs {
		key, err := controller.KeyFunc(rqs[i])
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Couldn't get key for object", "object", klog.KObj(rqs[i]))
			continue
		}
		rq.queue.Add(key)
	}
}

// enqueueResourceQuota 将对象对应的 key 入队。
// obj 可能是 *v1.ResourceQuota，也可能是 DeletionFinalStateUnknown 标记项。
func (rq *Controller) enqueueResourceQuota(logger klog.Logger, obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		logger.Error(err, "Couldn't get key", "object", obj)
		return
	}
	rq.queue.Add(key)
}

// addQuota 根据配额对象的状态决定将其放入优先队列还是普通队列。
func (rq *Controller) addQuota(logger klog.Logger, obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		logger.Error(err, "Couldn't get key", "object", obj)
		return
	}

	resourceQuota := obj.(*v1.ResourceQuota)

	// spec 中声明的硬限制尚未反映到 status，说明状态落后，放入优先队列。
	if !apiequality.Semantic.DeepEqual(resourceQuota.Spec.Hard, resourceQuota.Status.Hard) {
		rq.missingUsageQueue.Add(key)
		return
	}

	// 某个硬限制没有对应的使用量，而本控制器能计算它，同样优先处理。
	for constraint := range resourceQuota.Status.Hard {
		if _, usageFound := resourceQuota.Status.Used[constraint]; !usageFound {
			matchedResources := []v1.ResourceName{constraint}
			for _, evaluator := range rq.registry.List() {
				if intersection := evaluator.MatchingResources(matchedResources); len(intersection) > 0 {
					rq.missingUsageQueue.Add(key)
					return
				}
			}
		}
	}

	// 没有特殊优先需求，进入普通重算队列。
	rq.queue.Add(key)
}

// worker 返回一个工作循环：不断从队列中取出条目、处理并标记完成。
func (rq *Controller) worker(queue workqueue.TypedRateLimitingInterface[string]) func(context.Context) {
	workFunc := func(ctx context.Context) bool {
		key, quit := queue.Get()
		if quit {
			return true
		}
		defer queue.Done(key)

		// 持有读锁，避免 monitor 正在 resync（写锁）时处理配额。
		rq.workerLock.RLock()
		defer rq.workerLock.RUnlock()

		logger := klog.FromContext(ctx)
		logger = klog.LoggerWithValues(logger, "queueKey", key)
		ctx = klog.NewContext(ctx, logger)

		err := rq.syncHandler(ctx, key)
		if err == nil {
			queue.Forget(key)
			return false
		}

		utilruntime.HandleErrorWithContext(ctx, err, "Error processing resource quota work item")
		queue.AddRateLimited(key)

		return false
	}

	return func(ctx context.Context) {
		for {
			if quit := workFunc(ctx); quit {
				klog.FromContext(ctx).Info("resource quota controller worker shutting down")
				return
			}
		}
	}
}

// Run 使用指定数量的 worker 启动配额控制器。
func (rq *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Starting resource quota controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down resource quota controller")
		rq.missingUsageQueue.ShutDown()
		rq.queue.ShutDown()
		wg.Wait()
	}()

	if rq.quotaMonitor != nil {
		wg.Go(func() {
			rq.quotaMonitor.Run(ctx)
		})
	}

	if !cache.WaitForNamedCacheSyncWithContext(ctx, rq.informerSyncedFuncs...) {
		return
	}

	// worker 持续消化积压的配额计算任务，普通队列与优先队列各占一半 worker。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, rq.worker(rq.queue), time.Second)
		})
		wg.Go(func() {
			wait.UntilWithContext(ctx, rq.worker(rq.missingUsageQueue), time.Second)
		})
	}

	// 定时器控制对所有配额做完整重算的频率。
	if rq.resyncPeriod() > 0 {
		wg.Go(func() {
			wait.UntilWithContext(ctx, rq.enqueueAll, rq.resyncPeriod())
		})
	} else {
		logger.Info("periodic quota controller resync disabled")
	}

	<-ctx.Done()
}

// syncResourceQuotaFromKey 根据 key 定位配额对象并同步。
func (rq *Controller) syncResourceQuotaFromKey(ctx context.Context, key string) (err error) {
	startTime := time.Now()

	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithValues(logger, "key", key)

	defer func() {
		logger.V(4).Info("Finished syncing resource quota", "key", key, "duration", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	resourceQuota, err := rq.rqLister.ResourceQuotas(namespace).Get(name)
	// 对象已被删除属正常情况：事件入队与实际处理之间存在延迟，直接跳过即可。
	if errors.IsNotFound(err) {
		logger.Info("Resource quota has been deleted", "key", key)
		return nil
	}
	if err != nil {
		logger.Error(err, "Unable to retrieve resource quota from store", "key", key)
		return err
	}
	return rq.syncResourceQuota(ctx, resourceQuota)
}

// syncResourceQuota 跨所有已知资源类型完整地同步配额状态。
func (rq *Controller) syncResourceQuota(ctx context.Context, resourceQuota *v1.ResourceQuota) (err error) {
	// spec 的硬限制与 status 的硬限制有任何差异，都说明状态过期。
	statusLimitsDirty := !apiequality.Semantic.DeepEqual(resourceQuota.Spec.Hard, resourceQuota.Status.Hard)

	// dirty 表示使用量状态是否与上次同步不同；若不同则写回最新状态。
	// 首次同步时默认视为 dirty，因为需要记录使用量。
	dirty := statusLimitsDirty || resourceQuota.Status.Hard == nil || resourceQuota.Status.Used == nil

	used := v1.ResourceList{}
	if resourceQuota.Status.Used != nil {
		used = quota.Add(v1.ResourceList{}, resourceQuota.Status.Used)
	}
	hardLimits := quota.Add(v1.ResourceList{}, resourceQuota.Spec.Hard)

	var errs []error

	newUsage, err := quota.CalculateUsage(resourceQuota.Namespace, resourceQuota.Spec.Scopes, hardLimits, rq.registry, resourceQuota.Spec.ScopeSelector)
	if err != nil {
		// 即使计算出错，也记住错误待返回，同时继续用 newUsage 中已算出的部分更新状态。
		errs = append(errs, err)
	}
	for key, value := range newUsage {
		used[key] = value
	}

	// 确保 used 中的项与设置了硬限制的项一致，裁剪掉多余的统计项。
	hardResources := quota.ResourceNames(hardLimits)
	used = quota.Mask(used, hardResources)

	// 基于当前配额对象（携带 resourceVersion）构造用于更新的 usage 对象；
	// 默认保留旧的使用量观测值，并把 hard 设为最新的 spec。
	usage := resourceQuota.DeepCopy()
	usage.Status = v1.ResourceQuotaStatus{
		Hard: hardLimits,
		Used: used,
	}

	dirty = dirty || !quota.Equals(usage.Status.Used, resourceQuota.Status.Used)

	// 本控制器观测到了变化，需要写回配额状态。
	if dirty {
		_, err = rq.rqClient.ResourceQuotas(usage.Namespace).UpdateStatus(ctx, usage, metav1.UpdateOptions{})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

// replenishQuota 是补充回调函数，由控制器调用以通知某个配额需要重新计算。
func (rq *Controller) replenishQuota(ctx context.Context, groupResource schema.GroupResource, namespace string) {
	// 若本控制器无法评估该 groupResource，则直接忽略。
	evaluator := rq.registry.Get(groupResource)
	if evaluator == nil {
		return
	}

	// 检查该命名空间是否配置了配额。
	resourceQuotas, err := rq.rqLister.ResourceQuotas(namespace).List(labels.Everything())
	if errors.IsNotFound(err) {
		utilruntime.HandleErrorWithContext(ctx, nil, "Quota controller could not find ResourceQuota associated with namespace", "namespace", namespace, "resyncPeriod", rq.resyncPeriod())
		return
	}
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error checking to see if namespace has any ResourceQuota associated with it", "namespace", namespace)
		return
	}
	if len(resourceQuotas) == 0 {
		return
	}

	logger := klog.FromContext(ctx)

	// 只把跟踪了与该类型相关资源的配额入队，避免无谓重算。
	for i := range resourceQuotas {
		resourceQuota := resourceQuotas[i]
		resourceQuotaResources := quota.ResourceNames(resourceQuota.Status.Hard)
		if intersection := evaluator.MatchingResources(resourceQuotaResources); len(intersection) > 0 {
			// TODO: 支持仅补充特定类型的资源；目前会对该配额触发全量重算。
			rq.enqueueResourceQuota(logger, resourceQuota)
		}
	}
}

// Sync 在发现新资源时定期对控制器做 resync。
func (rq *Controller) Sync(ctx context.Context, discoveryFunc NamespacedResourcesFunc, period time.Duration) {
	// 记录上一次发现的资源状态，用于和当前状态比较。
	oldResources := make(map[schema.GroupVersionResource]struct{})
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		// 从发现接口获取当前资源列表。
		newResources, err := GetQuotableResources(discoveryFunc)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Error during resource discovery")

			if groupLookupFailures, isLookupFailure := discovery.GroupDiscoveryFailedErrorGroups(err); isLookupFailure && len(newResources) > 0 {
				// 部分发现失败时，保留失败 group 中已有的 informer，
				// 这样 resyncMonitors 只会为新发现的资源创建 informer。
				for k, v := range oldResources {
					if _, failed := groupLookupFailures[k.GroupVersion()]; failed {
						newResources[k] = v
					}
				}
			} else {
				// 非发现类错误或发现结果为空时直接返回，不做后续处理。
				return
			}
		}

		logger := klog.FromContext(ctx)

		// 判断发现结果是否真的发生了变化。
		if reflect.DeepEqual(oldResources, newResources) {
			logger.V(4).Info("no resource updates from discovery, skipping resource quota sync")
			return
		}

		// 加写锁暂停 worker，避免 informer 尚未完成 resync 时就处理事件。
		rq.workerLock.Lock()
		defer rq.workerLock.Unlock()

		// 发现结果有变化，记录差异日志并执行同步。
		if loggerV := logger.V(2); loggerV.Enabled() {
			loggerV.Info("syncing resource quota controller with updated resources from discovery", "diff", printDiff(oldResources, newResources))
		}

		// 重新同步 monitor，并等待各控制器报告缓存已同步。
		if err := rq.resyncMonitors(ctx, newResources); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Failed to sync resource monitors")
			return
		}

		// 至此新资源已同步到 monitor，记录当前状态供下次比较。
		oldResources = newResources

		// 等待缓存填充一段时间（一个同步周期）。
		// 这样可避免死锁：可用资源发生变化后某个 informer 缓存永远无法填满。
		// informer 会在后台持续尝试同步，重试不会打断它们；
		// 重试时对仍然存在的资源调用 resyncMonitors 是 no-op。
		if rq.quotaMonitor != nil {
			syncCtx, cancel := context.WithTimeout(ctx, period)
			defer cancel()

			if !cache.WaitForNamedCacheSyncWithContext(
				syncCtx,
				func() bool { return rq.quotaMonitor.IsSynced(ctx) },
			) {
				utilruntime.HandleErrorWithContext(ctx, nil, "Timed out waiting for quota monitor sync")
				return
			}
		}

		logger.V(2).Info("synced quota controller")
	}, period)
}

// printDiff 返回新增和移除资源的可读摘要，便于日志记录。
func printDiff(oldResources, newResources map[schema.GroupVersionResource]struct{}) string {
	removed := sets.NewString()
	for oldResource := range oldResources {
		if _, ok := newResources[oldResource]; !ok {
			removed.Insert(fmt.Sprintf("%+v", oldResource))
		}
	}
	added := sets.NewString()
	for newResource := range newResources {
		if _, ok := oldResources[newResource]; !ok {
			added.Insert(fmt.Sprintf("%+v", newResource))
		}
	}
	return fmt.Sprintf("added: %v, removed: %v", added.List(), removed.List())
}

// resyncMonitors 按需启动或停止配额 monitor，确保只监控 map 中出现的资源。
func (rq *Controller) resyncMonitors(ctx context.Context, resources map[schema.GroupVersionResource]struct{}) error {
	if rq.quotaMonitor == nil {
		return nil
	}

	if err := rq.quotaMonitor.SyncMonitors(ctx, resources); err != nil {
		return err
	}
	rq.quotaMonitor.StartMonitors(ctx)
	return nil
}

// GetQuotableResources 返回配额系统应识别的所有资源。
// 资源必须支持 'create'、'list'、'watch'、'delete' 这些 verb。
// 该函数可能同时返回结果和错误，此时表示发现调用仅部分成功，
// 是否继续使用结果由调用方决定。
func GetQuotableResources(discoveryFunc NamespacedResourcesFunc) (map[schema.GroupVersionResource]struct{}, error) {
	possibleResources, discoveryErr := discoveryFunc()
	if discoveryErr != nil && len(possibleResources) == 0 {
		return nil, fmt.Errorf("failed to discover resources: %v", discoveryErr)
	}
	quotableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"create", "list", "watch", "delete"}}, possibleResources)
	quotableGroupVersionResources, err := discovery.GroupVersionResources(quotableResources)
	if err != nil {
		return nil, fmt.Errorf("failed to parse resources: %v", err)
	}
	// 连同资源列表一起返回原始的发现错误（若有），由调用方决定是否继续。
	return quotableGroupVersionResources, discoveryErr
}
