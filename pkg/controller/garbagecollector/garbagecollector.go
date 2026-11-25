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

package garbagecollector

import (
	"context"
	goerrors "errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes" // 导入已知版本
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/controller"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/klog/v2"
	c "k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metrics"
)

// ResourceResyncTime 定义垃圾回收器 informer 的重新同步周期（0 表示不做周期性重同步）。
const ResourceResyncTime time.Duration = 0

// GarbageCollector 运行 reflector 监听受管理 API 对象的变化，把结果汇入单线程的
// dependencyGraphBuilder，后者构建并维护缓存对象间依赖关系的有向图。
// 图发生变化时，dependencyGraphBuilder 把可能被回收的对象放入 attemptToDelete 队列，
// 把需要孤立其 dependents 的对象放入 attemptToOrphan 队列。
// GarbageCollector 的 worker 消费这两个队列，向 API Server 发送删除/更新请求。
// 由 dependencyGraphBuilder 主动通知 GC，可保证 GC 处理时使用的图至少与通知一样新。
type GarbageCollector struct {
	restMapper     meta.ResettableRESTMapper
	metadataClient metadata.Interface
	// 时机成熟时，GC 会尝试删除 attemptToDelete 队列中的对象。
	attemptToDelete workqueue.TypedRateLimitingInterface[*node]
	// GC 会先孤立 attemptToOrphan 队列中对象的 dependents，再删除这些对象。
	attemptToOrphan        workqueue.TypedRateLimitingInterface[*node]
	dependencyGraphBuilder *GraphBuilder
	// GC 缓存那些 API Server 已确认不存在的 owner，避免重复查询。
	absentOwnerCache *ReferenceCache

	kubeClient       clientset.Interface
	eventBroadcaster record.EventBroadcaster
}

var _ controller.Interface = (*GarbageCollector)(nil)
var _ controller.Debuggable = (*GarbageCollector)(nil)

// NewGarbageCollector 创建一个新的 GarbageCollector，内部先构造依赖图构建器再组装 GC。
func NewGarbageCollector(
	ctx context.Context,
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	mapper meta.ResettableRESTMapper,
	ignoredResources map[schema.GroupResource]struct{},
	sharedInformers informerfactory.InformerFactory,
	informersStarted <-chan struct{},
) (*GarbageCollector, error) {
	graphBuilder := NewDependencyGraphBuilder(ctx, metadataClient, mapper, ignoredResources, sharedInformers, informersStarted)
	return NewComposedGarbageCollector(ctx, kubeClient, metadataClient, mapper, graphBuilder)
}

// NewComposedGarbageCollector 构造并返回测试或控制器所需的对象。
func NewComposedGarbageCollector(
	ctx context.Context,
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	mapper meta.ResettableRESTMapper,
	graphBuilder *GraphBuilder,
) (*GarbageCollector, error) {
	attemptToDelete, attemptToOrphan, absentOwnerCache := graphBuilder.GetGraphResources()

	gc := &GarbageCollector{
		metadataClient:         metadataClient,
		restMapper:             mapper,
		attemptToDelete:        attemptToDelete,
		attemptToOrphan:        attemptToOrphan,
		absentOwnerCache:       absentOwnerCache,
		kubeClient:             kubeClient,
		eventBroadcaster:       graphBuilder.eventBroadcaster,
		dependencyGraphBuilder: graphBuilder,
	}

	metrics.Register()

	return gc, nil
}

// resyncMonitors 按需启动或停止资源 monitor，确保恰好监控 map 中列出的资源。
func (gc *GarbageCollector) resyncMonitors(logger klog.Logger, deletableResources map[schema.GroupVersionResource]struct{}) error {
	if err := gc.dependencyGraphBuilder.syncMonitors(logger, deletableResources); err != nil {
		return err
	}
	gc.dependencyGraphBuilder.startMonitors(logger)
	return nil
}

// Run 启动垃圾回收器的图构建器与各类 worker，直到 ctx 结束。
func (gc *GarbageCollector) Run(ctx context.Context, workers int, initialSyncTimeout time.Duration) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线。
	gc.eventBroadcaster.StartStructuredLogging(3)
	gc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: gc.kubeClient.CoreV1().Events("")})
	defer gc.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting controller", "controller", "garbagecollector")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down controller", "controller", "garbagecollector")
		gc.dependencyGraphBuilder.graphChanges.ShutDown()
		gc.attemptToOrphan.ShutDown()
		gc.attemptToDelete.ShutDown()
		wg.Wait()
	}()

	// 在独立 goroutine 中启动依赖图构建器。
	wg.Go(func() {
		gc.dependencyGraphBuilder.Run(ctx)
	})

	// 创建一个带超时的 context，用于限制初始缓存同步的等待时间。
	syncCtx, cancel := context.WithTimeout(ctx, initialSyncTimeout)
	defer cancel()

	if !cache.WaitForNamedCacheSyncWithContext(syncCtx, func() bool {
		return gc.dependencyGraphBuilder.IsSynced(logger)
	}) {
		logger.Info("Garbage collector: not all resource monitors could be synced, proceeding anyways")
	} else {
		logger.Info("Garbage collector: all resource monitors have synced")
	}

	logger.Info("Proceeding to collect garbage")

	// 启动 gc worker：分别处理删除队列与孤立队列。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, gc.runAttemptToDeleteWorker, 1*time.Second)
		})
		wg.Go(func() {
			wait.Until(func() { gc.runAttemptToOrphanWorker(logger) }, 1*time.Second, ctx.Done())
		})
	}
	<-ctx.Done()
}

// Sync 定期根据发现结果重新同步垃圾回收器：检测到新资源时重置 restMapper 并重新同步 monitor。
// 注意 discoveryClient 不应与 gc.restMapper 共享，否则在检测新资源的过程中，
// mapper 底层的 discovery client 会被不必要地重置。
func (gc *GarbageCollector) Sync(ctx context.Context, discoveryClient discovery.ServerResourcesInterface, period time.Duration) {
	oldResources := make(map[schema.GroupVersionResource]struct{})
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		logger := klog.FromContext(ctx)

		// 从发现接口获取当前资源列表。
		newResources, err := GetDeletableResources(ctx, discovery.ToServerResourcesInterfaceWithContext(discoveryClient))

		if len(newResources) == 0 {
			logger.V(2).Info("no resources reported by discovery, skipping garbage collector sync")
			metrics.GarbageCollectorResourcesSyncError.Inc()
			return
		}
		if groupLookupFailures, isLookupFailure := discovery.GroupDiscoveryFailedErrorGroups(err); isLookupFailure {
			// 部分发现失败时，保留失败组中已同步的现有 informer，这样 resyncMonitors 只会为新发现的资源创建 informer。
			for k, v := range oldResources {
				if _, failed := groupLookupFailures[k.GroupVersion()]; failed && gc.dependencyGraphBuilder.IsResourceSynced(k) {
					newResources[k] = v
				}
			}
		}

		// 比较新旧资源集合，判断发现结果是否真的发生变化。
		if reflect.DeepEqual(oldResources, newResources) {
			logger.V(5).Info("no resource updates from discovery, skipping garbage collector sync")
			return
		}

		logger.V(2).Info(
			"syncing garbage collector with updated resources from discovery",
			"diff", printDiff(oldResources, newResources),
		)

		// 重置 REST mapper 也会使其底层 discovery client 失效。
		// 这是一种抽象泄漏，依赖了 REST mapper 的具体行为，目前只能接受。
		gc.restMapper.Reset()
		logger.V(4).Info("reset restmapper")

		// 重新同步 monitor，并等待各 controller 报告缓存已同步。
		//
		// 注意：newResources 可能与 restMapper 在 Reset 时发现的资源不一致，
		// 因为二者使用不同的 discovery client 且失效时间不同。例如若资源在两次调用之间出现，
		// newResources 可能包含 restMapper 发现调用未返回的资源，此时 restMapper 直到下一次尝试前
		// 都无法映射这些资源。
		if err := gc.resyncMonitors(logger, newResources); err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to sync resource monitors: %w", err))
			metrics.GarbageCollectorResourcesSyncError.Inc()
			return
		}
		logger.V(4).Info("resynced monitors")

		syncCtx, cancel := context.WithTimeout(ctx, period)
		defer cancel()

		// gc worker 不再等待缓存同步，但保留周期性检查以提供日志和指标。
		cacheSynced := cache.WaitForNamedCacheSyncWithContext(syncCtx, func() bool {
			return gc.dependencyGraphBuilder.IsSynced(logger)
		})
		if cacheSynced {
			logger.V(2).Info("synced garbage collector")
		} else {
			utilruntime.HandleError(fmt.Errorf("timed out waiting for dependency graph builder sync during GC sync"))
			metrics.GarbageCollectorResourcesSyncError.Inc()
		}

		// 最后更新记录的资源 monitor 状态。即使某些 monitor 缓存同步超时也仍记录，
		// 以便后续发现其资源被移除时能停止这些 monitor。
		oldResources = newResources
	}, period)
}

// printDiff 返回新增与移除资源的可读摘要，便于日志排查。
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

// IsSynced 在依赖图构建器已完成同步时返回 true。
func (gc *GarbageCollector) IsSynced(logger klog.Logger) bool {
	return gc.dependencyGraphBuilder.IsSynced(logger)
}

// runAttemptToDeleteWorker 持续消费 attemptToDelete 队列，直到队列关闭。
// 每次调用 processAttemptToDeleteWorker 处理一个节点；返回 false 表示队列已关闭，循环退出。
func (gc *GarbageCollector) runAttemptToDeleteWorker(ctx context.Context) {
	for gc.processAttemptToDeleteWorker(ctx) {
	}
}

// enqueuedVirtualDeleteEventErr 是标记错误：已入队虚拟删除事件，无需重试。
var enqueuedVirtualDeleteEventErr = goerrors.New("enqueued virtual delete event")

// namespacedOwnerOfClusterScopedObjectErr 是标记错误：集群级对象不能引用命名空间级 owner。
var namespacedOwnerOfClusterScopedObjectErr = goerrors.New("cluster-scoped objects cannot refer to namespaced owners")

// processAttemptToDeleteWorker 从 attemptToDelete 队列取一个节点并尝试回收它。
// 根据 attemptToDeleteWorker 返回的动作决定遗忘还是限速重试；队列关闭时返回 false。
func (gc *GarbageCollector) processAttemptToDeleteWorker(ctx context.Context) bool {
	item, quit := gc.attemptToDelete.Get()
	if quit {
		return false
	}
	// 必须标记 Done，否则工作队列会认为该节点仍在处理中，
	// 后续 AddRateLimited 的重试入队会被阻塞。
	defer gc.attemptToDelete.Done(item)

	action := gc.attemptToDeleteWorker(ctx, item)
	switch action {
	case forgetItem:
		gc.attemptToDelete.Forget(item)
	case requeueItem:
		gc.attemptToDelete.AddRateLimited(item)
	}

	return true
}

// workQueueItemAction 表示工作队列项处理后的动作：重试或遗忘。
type workQueueItemAction int

// 工作队列项处理动作的枚举值。
const (
	requeueItem = iota
	forgetItem
)

// attemptToDeleteWorker 判断队首节点是否已过期或已被真实对象取代，
// 再交给 attemptToDeleteItem 执行回收；根据结果返回重试或遗忘的动作。
func (gc *GarbageCollector) attemptToDeleteWorker(ctx context.Context, item interface{}) workQueueItemAction {
	n, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return forgetItem
	}

	logger := klog.FromContext(ctx)

	if !n.isObserved() {
		nodeFromGraph, existsInGraph := gc.dependencyGraphBuilder.uidToNode.Read(n.identity.UID)
		if !existsInGraph {
			// 当 attemptToDeleteItem 返回错误导致虚拟节点被重新入队并循环时会出现这种情况：
			// 期间又观测到该 UID 对应的真实对象被删除。
			logger.V(5).Info("item no longer in the graph, skipping attemptToDeleteItem", "item", n.identity)
			return forgetItem
		}
		if nodeFromGraph.isObserved() {
			// 当 attemptToDeleteItem 返回错误导致虚拟节点被重新入队并循环时会出现这种情况：
			// 期间又观测到该 UID 对应的真实对象。
			logger.V(5).Info("item no longer virtual in the graph, skipping attemptToDeleteItem on virtual node", "item", n.identity)
			return forgetItem
		}
	}

	err := gc.attemptToDeleteItem(ctx, n)
	if err == enqueuedVirtualDeleteEventErr {
		// 已生成虚拟事件并交由 processGraphChanges 处理，无需重新入队该节点。
		return forgetItem
	} else if err == namespacedOwnerOfClusterScopedObjectErr {
		// 集群级对象引用命名空间级 owner 是重试也无法修复的错误，无需重新入队。
		return forgetItem
	} else if err != nil {
		if _, ok := err.(*restMappingError); ok {
			// 至少有两类可能：
			// 1. 引用了 gc.restMapper 尚未识别的自定义类型对象（暂时性错误）。
			// 2. 引用了非法的 group/version，目前无法与下次发现同步后才会识别的合法类型区分。
			// 目前先记录错误并重试。
			logger.V(5).Info("error syncing item", "item", n.identity, "err", err)
		} else {
			utilruntime.HandleError(fmt.Errorf("error syncing item %s: %v", n, err))
		}
		// 对象回收失败时重试。
		return requeueItem
	} else if !n.isObserved() {
		// 若节点尚未通过 informer 事件被观测到则重新入队；
		// 否则在 watch 重建期间被添加又被删除的对象，其虚拟节点可能永远滞留在图中无法清除。
		// 见 https://issue.k8s.io/56121
		logger.V(5).Info("item hasn't been observed via informer yet", "item", n.identity)
		return requeueItem
	}

	return forgetItem
}

// isDangling 检查 owner 引用是否指向不存在的对象。
// 若向 API Server 查询了该对象，还会返回它的最新状态。
func (gc *GarbageCollector) isDangling(ctx context.Context, reference metav1.OwnerReference, item *node) (
	dangling bool, owner *metav1.PartialObjectMetadata, err error) {

	logger := klog.FromContext(ctx)
	// 先查缓存中是否记录了该集群级 owner 不存在。
	absentOwnerCacheKey := objectReference{OwnerReference: ownerReferenceCoordinates(reference)}
	if gc.absentOwnerCache.Has(absentOwnerCacheKey) {
		logger.V(5).Info("according to the absentOwnerCache, item's owner does not exist",
			"item", item.identity,
			"owner", reference,
		)
		return true, nil, nil
	}

	// 再查缓存中是否记录了该命名空间级 owner 不存在。
	absentOwnerCacheKey.Namespace = item.identity.Namespace
	if gc.absentOwnerCache.Has(absentOwnerCacheKey) {
		logger.V(5).Info("according to the absentOwnerCache, item's owner does not exist in namespace",
			"item", item.identity,
			"owner", reference,
		)
		return true, nil, nil
	}

	// TODO：验证 reference 对应的资源是否受系统支持。若资源无效，垃圾回收器应：
	// 1. 判断对象是否删除时忽略该引用；
	// 2. 更新对象并移除该引用。
	// 以防集群升级后，对象因引用旧资源而被删除。
	// 将引用解析为可查询的资源；集群级与命名空间级的命名空间参数不同。
	resource, namespaced, err := gc.apiResource(reference.APIVersion, reference.Kind)
	if err != nil {
		return false, nil, err
	}
	if !namespaced {
		absentOwnerCacheKey.Namespace = ""
	}

	if len(item.identity.Namespace) == 0 && namespaced {
		// 集群级对象引用命名空间级 owner 是非法的，返回标记错误，
		// 避免对注定失败的查询无限重试。
		logger.V(2).Info("item is cluster-scoped, but refers to a namespaced owner",
			"item", item.identity,
			"owner", reference,
		)
		return false, nil, namespacedOwnerOfClusterScopedObjectErr
	}

	// TODO：仅当 owner 节点为虚拟节点时才需查询 API Server。
	// 本地图可能落后于真实状态，但实际差异很小。
	// 向 API Server 获取 owner 最新状态，以 UID 是否匹配判定引用是否悬空。
	owner, err = gc.metadataClient.Resource(resource).Namespace(resourceDefaultNamespace(namespaced, item.identity.Namespace)).Get(ctx, reference.Name, metav1.GetOptions{})
	switch {
	case errors.IsNotFound(err):
		gc.absentOwnerCache.Add(absentOwnerCacheKey)
		logger.V(5).Info("item's owner is not found",
			"item", item.identity,
			"owner", reference,
		)
		return true, nil, nil
	case err != nil:
		return false, nil, err
	}

	if owner.GetUID() != reference.UID {
		logger.V(5).Info("item's owner is not found, UID mismatch",
			"item", item.identity,
			"owner", reference,
		)
		gc.absentOwnerCache.Add(absentOwnerCacheKey)
		return true, nil, nil
	}
	return false, owner, nil
}

// classifyReferences 把最新 ownerRef 分为三类：
// solid：owner 存在且未处于等待 dependents 删除状态；
// dangling：owner 不存在；
// waitingForDependentsDeletion：owner 存在、已标记删除且带有 FinalizerDeletingDependents。
// 本函数会与 API Server 通信。
func (gc *GarbageCollector) classifyReferences(ctx context.Context, item *node, latestReferences []metav1.OwnerReference) (
	solid, dangling, waitingForDependentsDeletion []metav1.OwnerReference, err error) {
	for _, reference := range latestReferences {
		isDangling, owner, err := gc.isDangling(ctx, reference, item)
		if err != nil {
			return nil, nil, nil, err
		}
		if isDangling {
			dangling = append(dangling, reference)
			continue
		}

		ownerAccessor, err := meta.Accessor(owner)
		if err != nil {
			return nil, nil, nil, err
		}
		if ownerAccessor.GetDeletionTimestamp() != nil && hasDeleteDependentsFinalizer(ownerAccessor) {
			waitingForDependentsDeletion = append(waitingForDependentsDeletion, reference)
		} else {
			solid = append(solid, reference)
		}
	}
	return solid, dangling, waitingForDependentsDeletion, nil
}

// ownerRefsToUIDs 从 ownerRef 列表中提取 UID 列表。
func ownerRefsToUIDs(refs []metav1.OwnerReference) []types.UID {
	var ret []types.UID
	for _, ref := range refs {
		ret = append(ret, ref.UID)
	}
	return ret
}

// attemptToDeleteItem 查询节点对应的实际 API 对象，仅当 UID 匹配、未被阻塞于删除 dependents
// 且所有 ownerRef 均为悬空引用时才执行删除。
// 若查询返回 NotFound 或取回对象的 UID 不匹配，会为该节点入队一个虚拟删除事件并返回
// enqueuedVirtualDeleteEventErr。
func (gc *GarbageCollector) attemptToDeleteItem(ctx context.Context, item *node) error {
	logger := klog.FromContext(ctx)

	logger.V(2).Info("Processing item",
		"item", item.identity,
		"virtual", !item.isObserved(),
	)

	// 进入删除流程是单向的，最终必然被删除；因此只需等待最终删除，再处理其 dependents。
	if item.isBeingDeleted() && !item.isDeletingDependents() {
		logger.V(5).Info("processing item returned at once, because its DeletionTimestamp is non-nil",
			"item", item.identity,
		)
		return nil
	}
	// TODO：仅当节点为虚拟节点时才需查询 API Server。
	// 本地图可能落后于真实状态，但实际差异很小。
	// 获取对象最新版本，NotFound 或 UID 不匹配都说明对象已消失。
	latest, err := gc.getObject(item.identity)
	switch {
	case errors.IsNotFound(err):
		// GraphBuilder 可能为尚不存在的 owner 添加虚拟节点，
		// 因此需入队虚拟删除事件，把它从 uidToNode 中清除。
		logger.V(5).Info("item not found, generating a virtual delete event",
			"item", item.identity,
		)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	case err != nil:
		return err
	}

	if latest.GetUID() != item.identity.UID {
		logger.V(5).Info("UID doesn't match, item not found, generating a virtual delete event",
			"item", item.identity,
		)
		gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
		return enqueuedVirtualDeleteEventErr
	}

	// TODO：attemptToOrphanWorker() 逻辑与此类似，考虑将其也合并到 attemptToDeleteItem()。
	// 处于 foreground 删除流程时交给专门的处理逻辑。
	if item.isDeletingDependents() {
		return gc.processDeletingDependentsItem(logger, item)
	}

	// 根据 ownerRef 分类决定是否回收该对象。
	ownerReferences := latest.GetOwnerReferences()
	if len(ownerReferences) == 0 {
		logger.V(2).Info("item doesn't have an owner, continue on next item",
			"item", item.identity,
		)
		return nil
	}

	solid, dangling, waitingForDependentsDeletion, err := gc.classifyReferences(ctx, item, ownerReferences)
	if err != nil {
		return err
	}
	logger.V(5).Info("classify item's references",
		"item", item.identity,
		"solid", solid,
		"dangling", dangling,
		"waitingForDependentsDeletion", waitingForDependentsDeletion,
	)

	switch {
	case len(solid) != 0:
		logger.V(2).Info("item has at least one existing owner, will not garbage collect",
			"item", item.identity,
			"owner", solid,
		)
		if len(dangling) == 0 && len(waitingForDependentsDeletion) == 0 {
			return nil
		}
		logger.V(2).Info("remove dangling references and waiting references for item",
			"item", item.identity,
			"dangling", dangling,
			"waitingForDependentsDeletion", waitingForDependentsDeletion,
		)
		// 必须从 ownerReferences 中移除处于等待删除状态的引用，
		// 否则被引用的对象会一直带有 FinalizerDeletingDependents 而无法删除。
		ownerUIDs := append(ownerRefsToUIDs(dangling), ownerRefsToUIDs(waitingForDependentsDeletion)...)
		p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes(item.identity.UID, ownerUIDs)
		if err != nil {
			return err
		}
		_, err = gc.patch(item, p, func(n *node) ([]byte, error) {
			return gc.deleteOwnerRefJSONMergePatch(n, ownerUIDs...)
		})
		return err
	case len(waitingForDependentsDeletion) != 0 && item.dependentsLength() != 0:
		deps := item.getDependents()
		for _, dep := range deps {
			if dep.isDeletingDependents() {
				// 这种环检测存在误判；若出现问题需采用更严格的检测。
				// 多个 worker 并行执行 attemptToDeleteItem，竞态下环检测可能失效。
				logger.V(2).Info("processing item, some of its owners and its dependent have FinalizerDeletingDependents, to prevent potential cycle, its ownerReferences are going to be modified to be non-blocking, then the item is going to be deleted with Foreground",
					"item", item.identity,
					"dependent", dep.identity,
				)
				patch, err := item.unblockOwnerReferencesStrategicMergePatch()
				if err != nil {
					return err
				}
				if _, err := gc.patch(item, patch, gc.unblockOwnerReferencesJSONMergePatch); err != nil {
					return err
				}
				break
			}
		}
		logger.V(2).Info("at least one owner of item has FinalizerDeletingDependents, and the item itself has dependents, so it is going to be deleted in Foreground",
			"item", item.identity,
		)
		// 删除事件会被 graphBuilder 观测到，对象将在 processDeletingDependentsItem 中再次处理；
		// 若届时已无 dependents，该函数会移除 FinalizerDeletingDependents，完成最终删除。
		policy := metav1.DeletePropagationForeground
		err := gc.deleteObject(item.identity, latest.ResourceVersion, latest.OwnerReferences, &policy)
		if errors.IsNotFound(err) {
			gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
			return enqueuedVirtualDeleteEventErr
		}
		return err
	default:
		// 对象没有任何有效的 solid owner，需要被回收；且没有 owner 在等待 dependents 删除，
		// 因此根据对象现有的 finalizer 决定传播策略。
		var policy metav1.DeletionPropagation
		switch {
		case hasOrphanFinalizer(latest):
			// 对象已有 orphan finalizer，遵循它选择 Orphan 传播。
			policy = metav1.DeletePropagationOrphan
		case hasDeleteDependentsFinalizer(latest):
			// 对象已有 foreground finalizer，遵循它选择 Foreground 传播。
			policy = metav1.DeletePropagationForeground
		default:
			// 否则默认使用 Background 传播。
			policy = metav1.DeletePropagationBackground
		}
		logger.V(2).Info("Deleting item",
			"item", item.identity,
			"propagationPolicy", policy,
		)
		err := gc.deleteObject(item.identity, latest.ResourceVersion, latest.OwnerReferences, &policy)
		if errors.IsNotFound(err) {
			gc.dependencyGraphBuilder.enqueueVirtualDeleteEvent(item.identity)
			return enqueuedVirtualDeleteEventErr
		}
		return err
	}
}

// processDeletingDependentsItem 处理正在等待其 dependents 被删除的对象。
func (gc *GarbageCollector) processDeletingDependentsItem(logger klog.Logger, item *node) error {
	blockingDependents := item.blockingDependents()
	if len(blockingDependents) == 0 {
		logger.V(2).Info("remove DeleteDependents finalizer for item", "item", item.identity)
		return gc.removeFinalizer(logger, item, metav1.FinalizerDeleteDependents)
	}
	for _, dep := range blockingDependents {
		if !dep.isDeletingDependents() {
			logger.V(2).Info("adding dependent to attemptToDelete, because its owner is deletingDependents",
				"item", item.identity,
				"dependent", dep.identity,
			)
			gc.attemptToDelete.Add(dep)
		}
	}
	return nil
}

// orphanDependents 并发地把 owner 从每个 dependent 的 ownerRef 中移除，实现“孤立”。
// 传入的 dependents 是 owner dependents 指针的副本，无需加锁。
func (gc *GarbageCollector) orphanDependents(logger klog.Logger, owner objectReference, dependents []*node) error {
	errCh := make(chan error, len(dependents))
	wg := sync.WaitGroup{}
	wg.Add(len(dependents))
	for i := range dependents {
		go func(dependent *node) {
			defer wg.Done()
			// 用 dependent 的 UID 作为 patch 的前置条件，避免误改已变化的对象。
			p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes(dependent.identity.UID, []types.UID{owner.UID})
			if err != nil {
				errCh <- fmt.Errorf("orphaning %s failed, %v", dependent.identity, err)
				return
			}
			_, err = gc.patch(dependent, p, func(n *node) ([]byte, error) {
				return gc.deleteOwnerRefJSONMergePatch(n, owner.UID)
			})
			// 注意：若目标 ownerRef 已不在 dependent 中，strategic merge patch 不会报错。
			if err != nil && !errors.IsNotFound(err) {
				errCh <- fmt.Errorf("orphaning %s failed, %v", dependent.identity, err)
			}
		}(dependents[i])
	}
	wg.Wait()
	close(errCh)

	var errorsSlice []error
	for e := range errCh {
		errorsSlice = append(errorsSlice, e)
	}

	if len(errorsSlice) != 0 {
		return fmt.Errorf("failed to orphan dependents of owner %s, got errors: %s", owner, utilerrors.NewAggregate(errorsSlice).Error())
	}
	logger.V(5).Info("successfully updated all dependents", "owner", owner)
	return nil
}

// runAttemptToOrphanWorker 持续消费 attemptToOrphan 队列，直到队列关闭。
// 与删除队列不同，这里使用普通 wait.Until 驱动，因此通过 ctx.Done() 感知停止信号。
func (gc *GarbageCollector) runAttemptToOrphanWorker(logger klog.Logger) {
	for gc.processAttemptToOrphanWorker(logger) {
	}
}

// processAttemptToOrphanWorker 从 attemptToOrphan 取出节点，根据 GC 维护的图找到其 dependents，
// 把自己从这些 dependents 的 OwnerReferences 中移除，最后更新 owner 以移除 Orphan finalizer。
// 任一步失败都会把节点重新加入 attemptToOrphan。
func (gc *GarbageCollector) processAttemptToOrphanWorker(logger klog.Logger) bool {
	item, quit := gc.attemptToOrphan.Get()
	if quit {
		return false
	}
	// 与删除队列同理：未 Done 会阻塞后续的重试入队。
	defer gc.attemptToOrphan.Done(item)

	action := gc.attemptToOrphanWorker(logger, item)
	switch action {
	case forgetItem:
		gc.attemptToOrphan.Forget(item)
	case requeueItem:
		gc.attemptToOrphan.AddRateLimited(item)
	}

	return true
}

// attemptToOrphanWorker 取出 owner 节点，复制其 dependents 快照后并发孤立它们，
// 再移除 owner 的 Orphan finalizer；任一步失败都返回重试动作。
// 复制快照是为了避免在遍历时长期持有 dependentsLock。
func (gc *GarbageCollector) attemptToOrphanWorker(logger klog.Logger, item interface{}) workQueueItemAction {
	owner, ok := item.(*node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expect *node, got %#v", item))
		return forgetItem
	}
	// 不需要逐元素加锁，因为这些元素从不会被更新。
	owner.dependentsLock.RLock()
	dependents := make([]*node, 0, len(owner.dependents))
	for dependent := range owner.dependents {
		dependents = append(dependents, dependent)
	}
	owner.dependentsLock.RUnlock()

	err := gc.orphanDependents(logger, owner.identity, dependents)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("orphanDependents for %s failed with %v", owner.identity, err))
		return requeueItem
	}
	// 更新 owner，从其 finalizer 列表中移除 Orphan finalizer。
	err = gc.removeFinalizer(logger, owner, metav1.FinalizerOrphanDependents)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("removeOrphanFinalizer for %s failed with %v", owner.identity, err))
		return requeueItem
	}
	return forgetItem
}

// *仅供测试使用*
// GraphHasUID 返回依赖图构建器的 uidToNode 中是否存有指定 UID，便于调试。
// 该方法主要由集成测试使用。
func (gc *GarbageCollector) GraphHasUID(u types.UID) bool {
	_, ok := gc.dependencyGraphBuilder.uidToNode.Read(u)
	return ok
}

// GetDeletableResources 返回 discoveryClient 发现的资源中 GC 需要处理的部分，
// 即所有支持 delete/list/watch 操作的 preferred 资源。
// 若发现过程中遇到错误，会连同已成功解析的资源一并作为结果返回。
// 所有发现错误都视为暂时性的：遇到错误时记录日志，并返回已能处理的资源（可能为空）。
func GetDeletableResources(ctx context.Context, discoveryClient discovery.ServerResourcesInterfaceWithContext) (map[schema.GroupVersionResource]struct{}, error) {
	logger := klog.FromContext(ctx)
	preferredResources, lookupErr := discoveryClient.ServerPreferredResourcesWithContext(ctx)
	if lookupErr != nil {
		if groupLookupFailures, isLookupFailure := discovery.GroupDiscoveryFailedErrorGroups(lookupErr); isLookupFailure {
			// 这里先把 groupLookupFailures 序列化成字符串，因为 map[schema.GroupVersion]error 无法 JSON 编码，
			// 否则日志框架会抛出内部错误。
			logger.Info("failed to discover some groups", "groups", fmt.Sprintf("%q", groupLookupFailures))
		} else {
			logger.Info("failed to discover preferred resources", "error", lookupErr)
		}
	}
	if preferredResources == nil {
		return map[schema.GroupVersionResource]struct{}{}, lookupErr
	}

	// 这里没有直接使用 discovery.GroupVersionResources，以便按资源粒度容忍发现失败。
	deletableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"delete", "list", "watch"}}, preferredResources)
	deletableGroupVersionResources := map[schema.GroupVersionResource]struct{}{}
	for _, rl := range deletableResources {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			logger.Info("ignoring invalid discovered resource", "groupversion", rl.GroupVersion, "error", err)
			continue
		}
		for i := range rl.APIResources {
			deletableGroupVersionResources[schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: rl.APIResources[i].Name}] = struct{}{}
		}
	}

	return deletableGroupVersionResources, lookupErr
}

// Name 返回垃圾回收器的控制器名称。
func (gc *GarbageCollector) Name() string {
	return "garbagecollector"
}

// GetDependencyGraphBuilder 返回依赖图构建器，在无法获取 controllerContext 的测试场景中尤其有用。
func (gc *GarbageCollector) GetDependencyGraphBuilder() *GraphBuilder {
	return gc.dependencyGraphBuilder
}
